# coding:utf-8
# coded by Shull Xu

import gevent
from gevent import monkey, Greenlet, pool, queue, event, Timeout, threadpool

monkey.patch_all()
import os,re
import uuid
import logging

import requests
from urllib import parse

from plugin import *
from buffer import UrlData,BloomFilter
from utils import HtmlAnalyzer, WebKit,allow_url


class Fetcher(Greenlet):
    '''下载器'''

    def __init__(self, spider):
        Greenlet.__init__(self)
        self.fetcher_id = str(uuid.uuid1())[:8]
        self.TOO_LONG = 1048576  # 1M
        self.spider = spider
        self.fetcher_bf = self.spider.fetcher_bf
        self.fetcher_queue = self.spider.fetcher_queue
        self.crawler_queue = self.spider.crawler_queue
        self.plugin_handler=self.spider.plugin_handler
        self.logger = self.spider.logger

    def _fetcher(self):
        self.logger.info('fertcher %s starting....' % (self.fetcher_id,))
        while not self.spider.stopped.isSet():
            try:
                url_data = self.fetcher_queue.get(block=False)
            except queue.Empty as e:
                if self.spider.crawler_stopped.isSet() and self.fetcher_queue.unfinished_tasks == 0:
                    self.spider.stop()
                elif self.crawler_queue.unfinished_tasks == 0 and self.fetcher_queue.unfinished_tasks == 0:
                    self.spider.stop()
                else:
                    gevent.sleep()
            else:
                if not url_data.html:
                    try:
                        html = ''
                        with gevent.Timeout(self.spider.internal_timeout, False) as timeout:
                            html = self._open(url_data)
                        if not html.strip():
                            self.spider.fetcher_queue.task_done()
                            continue
                        self.logger.info('fetcher %s accept %s' % (self.fetcher_id, url_data))
                        url_data.html = html
                        for plugin_name in self.plugin_handler:  # 循环动态调用初始化时注册的插件
                            try:
                                plugin_obj = eval(plugin_name)()
                                plugin_obj.start(url_data)
                            except Exception as e:
                                import traceback
                                traceback.print_exc()

                        if not self.spider.crawler_stopped.isSet():
                            self.crawler_queue.put(url_data, block=True)

                    except Exception as e:
                        import traceback
                        traceback.print_exc()

                    self.spider.fetcher_queue.task_done()


    def _open(self, url_data):
        iheaders = {}
        if self.spider.custom_headers:
            iheaders.update(self.spider.custom_headers)
        try:
            req = requests.get(url_data, headers=iheaders, stream=True)
        except Exception as e:
            self.logger.warn('%s %s' % (url_data, str(e)))
        else:
            if req.headers.get('content-type', '').find('text/html') < 0:
                req.close()
                return ''
            if int(req.headers.get('content-length', self.TOO_LONG)) > self.TOO_LONG:
                req.close()
                return ''
            try:
                html = req.content.decode('utf-8', 'ignore')
            except Exception as e:
                self.logger.warn('%s %s' % (url_data, str(e)))
            finally:
                req.close()
                if vars().get('html'):
                    return html
                else:
                    return ''

    def _run(self):
        self._fetcher()


class Spider(object):
    """爬虫主类"""
    logger = logging.getLogger("spider")

    def __init__(self, concurrent_num=20, crawl_tags=[], custom_headers={}, plugin=[], depth=3,
                 max_url_num=300, internal_timeout=60, spider_timeout=6 * 3600,
                 crawler_mode=0, same_origin=True, dynamic_parse=False):
        '''
        concurrent_num    : 并行crawler和fetcher数量
        crawl_tags        : 爬行时收集URL所属标签列表
        custom_headers    : 自定义HTTP请求头
        plugin            : 自定义插件列表
        depth             : 爬行深度限制
        max_url_num       : 最大收集URL数量
        internal_timeout  : 内部调用超时时间
        spider_timeout    : 爬虫超时时间
        crawler_mode      : 爬取器模型(0:多线程模型,1:gevent模型)
        same_origin       : 是否限制相同域下
        dynamic_parse     : 是否使用WebKit动态解析
        '''

        self.logger.setLevel(logging.DEBUG)
        hd = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        hd.setFormatter(formatter)
        self.logger.addHandler(hd)

        self.stopped = event.Event()
        self.internal_timeout = internal_timeout
        self.internal_timer = Timeout(internal_timeout)

        self.crawler_mode = crawler_mode  # 爬取器模型
        self.concurrent_num = concurrent_num
        self.fetcher_pool = pool.Pool(self.concurrent_num)
        if self.crawler_mode == 0:
            self.crawler_pool = threadpool.ThreadPool(min(50, self.concurrent_num))
        else:
            self.crawler_pool = pool.Pool(self.concurrent_num)

        # self.fetcher_queue = queue.JoinableQueue(maxsize=self.concurrent_num*100)
        self.fetcher_queue = threadpool.Queue(maxsize=self.concurrent_num * 10000)
        self.crawler_queue = threadpool.Queue(maxsize=self.concurrent_num * 10000)

        self.fetcher_bf=BloomFilter(0.0001,1000000)

        self.default_crawl_tags = ['a', 'base', 'iframe', 'frame', 'object']
        self.ignore_ext = ['js', 'css', 'png', 'jpg', 'gif', 'bmp', 'svg', 'exif', 'jpeg', 'exe', 'rar', 'zip', 'pdf']
        self.crawl_tags = list(set(self.default_crawl_tags) | set(crawl_tags))
        self.same_origin = same_origin
        self.depth = depth
        self.max_url_num = max_url_num
        self.fetched_url=0
        self.dynamic_parse = dynamic_parse
        if self.dynamic_parse:
            self.webkit = WebKit()
        self.crawler_stopped = event.Event()

        self.plugin_handler = plugin  # 注册Crawler中使用的插件
        self.custom_headers = custom_headers

    def _start_fetcher(self):
        '''启动下载器'''
        for i in range(self.concurrent_num):
            fetcher = Fetcher(self)
            self.fetcher_pool.start(fetcher)

    def _start_crawler(self):

        '''启动爬取器'''
        for _ in range(self.concurrent_num):
            self.crawler_pool.spawn(self.crawler)

    def start(self):
        '''启动入口'''
        self.logger.info('spider starting...')

        if self.crawler_mode == 0:
            self.logger.info('crawler run in multi-thread mode.')
        elif self.crawler_mode == 1:
            self.logger.info('crawler run in gevent mode.')

        self._start_fetcher()
        self._start_crawler()
        self.stopped.wait()
        try:
            self.internal_timer.start()
            self.fetcher_pool.join(timeout=self.internal_timer)
            if self.crawler_mode == 1:
                self.crawler_pool.join(timeout=self.internal_timer)
            else:
                self.crawler_pool.join()
        except Timeout:
            self.logger.error('internal timeout triggered')
        finally:
            self.internal_timer.cancel()
        self.stopped.clear()
        # if self.dynamic_parse:
        #     self.webkit.close()
        self.logger.info("Fetched %s urls" % self.fetched_url)
        self.logger.info("spider process quit.")

    def crawler(self, _dep=None):
        '''爬行器主函数'''
        while not self.stopped.isSet() and not self.crawler_stopped.isSet():
            try:
                self._maintain_spider()
                url_data = self.crawler_queue.get(block=False)
            except queue.Empty as e:
                if self.crawler_queue.unfinished_tasks == 0 and self.fetcher_queue.unfinished_tasks == 0:
                    self.stop()
                else:
                    if self.crawler_mode == 1:
                        gevent.sleep()
            else:
                curr_depth = len(str(url_data).split('/'))-2
                link_generator = HtmlAnalyzer.extract_links(url_data.html, url_data.url, self.crawl_tags)
                link_list = list(link_generator)
                if self.dynamic_parse:
                    link_generator = self.webkit.extract_links(url_data.url)
                    link_list.extend([ url for url in link_generator])
                link_list = list(set(link_list))
                for index, link in enumerate(link_list):
                    if not self.check_url_usable(link):
                        continue
                    if curr_depth > self.depth:
                        if self.crawler_stopped.isSet():
                            break
                        else:
                            self.crawler_stopped.set()
                            break
                    if self.fetched_url == self.max_url_num:
                        if self.crawler_stopped.isSet():
                            break
                        else:
                            self.crawler_stopped.set()
                            break
                    url = UrlData(link, depth=curr_depth)
                    self.fetcher_bf.insert_element(str(url))
                    self.fetched_url+=1
                    self.fetcher_queue.put(url, block=True)

                self.crawler_queue.task_done()

    def check_url_usable(self, link):
        '''检查链接是否可以'''
        if self.fetcher_bf.is_element_exist(link):
            return False

        if not link.startswith('http'):
            return False

        if self.same_origin:
            if not self._check_same_origin(link):
                return False

        link_ext = os.path.splitext(parse.urlsplit(link).path)[-1][1:]
        if link_ext in self.ignore_ext:
            return False

        allow_check=allow_url(link)
        if not allow_check.isallowed():
            return False

        return True

    def feed(self, url):
        '''设置起始url'''
        if isinstance(url, str):
            url = UrlData(url)

        if self.same_origin:
            url_part = parse.urlparse(str(url))
            self.origin = (url_part.scheme, url_part.netloc)

        self.fetcher_queue.put(url, block=True)

    def stop(self):
        '''终止爬虫'''
        self.stopped.set()

    def _maintain_spider(self):
        '''
        维护爬虫池:
        1)从池中剔除死掉的crawler和fetcher
        2)根据剩余任务数量及池的大小补充crawler和fetcher
        维持爬虫池饱满
        '''

        if self.crawler_mode == 1:
            for greenlet in list(self.crawler_pool):
                if greenlet.dead:
                    self.crawler_pool.discard(greenlet)
            for i in range(min(self.crawler_queue.qsize(), self.crawler_pool.free_count())):
                self.crawler_pool.spawn(self.crawler)

        for greenlet in list(self.fetcher_pool):
            if greenlet.dead:
                self.fetcher_pool.discard(greenlet)
        for i in range(min(self.fetcher_queue.qsize(), self.fetcher_pool.free_count())):
            fetcher = Fetcher(self)
            self.fetcher_pool.start(fetcher)

    def _check_same_origin(self, current_url):
        '''检查url是否同站'''
        url_part = parse.urlparse(current_url)
        url_origin = (url_part.scheme, url_part.netloc)
        return url_origin == self.origin


if __name__ == '__main__':
    spider = Spider(concurrent_num=10, depth=5, max_url_num=1000, crawler_mode=1, dynamic_parse=False)
    bk_list=['beicai', 'caolu', 'chuansha', 'hangtou', 'huamu', 'huinan', 'jinqiao', 'kangqiao', 'lianyang', 'lingangxincheng', 'liuli', 'lujiazui', 'pudongwaihuan', 'sanlin', 'shangnan', 'shibobinjianga', 'tangzhen', 'tangqiao', 'waigaoqiao', 'yangjing', 'zhangjiang', 'zhoupu', 'chunshen', 'gumeiluoyang', 'hanghua', 'jinhongqiao', 'longbojinhui', 'meilong', 'pujiang', 'qibao', 'xinzhuang', 'wujing', 'zhuanqiao', 'huajing', 'huaihaixilu', 'kangjian', 'longhua', 'shanghainanzhan', 'tianlin', 'wantiguan', 'xujiahui', 'changqiao', 'caoyang', 'ganquanyichuan', 'guangxin', 'taopu', 'wanli', 'wuning', 'changshoulu', 'zhenru', 'beixinjing', 'dongwuyuan', 'gubei', 'hongqiaolu', 'tianshan', 'xianxia', 'xinhualu', 'zhongshangongyuan', 'caojiadu', 'jiangninglu', 'jingansi', 'nanjingxilu', 'huangpubinjiang', 'laoximen', 'nanjingdonglu', 'penglaigongyuan', 'renminguangchang', 'yuyuan', 'dapuqiao', 'fuxinggongyuan', 'huaihaizhonglu', 'shibobinjiang', 'wuliqiao', 'xintiandi', 'beiwaitan', 'jiangwanzhen', 'liangvhen', 'linpinglu', 'luxungongyuan', 'quyang', 'sichuanbeilu', 'daninglvdi', 'pengpu', 'xizangbeilu', 'xinkezhan', 'zhabeigongyuan', 'anshan', 'dongwaitan', 'huangxinggongyuan', 'kongjianglu', 'wujiaochang', 'xinjiangwancheng', 'zhongyuanshequ', 'dachang', 'dahua', 'gongfu', 'gucun', 'luodian', 'shangda', 'songnan', 'tonghe', 'jiuting', 'sheshan', 'xinmin', 'sijing', 'songjiangchengqu', 'songjiangdaxuecheng', 'xiaokunshan', 'xinqiao', 'anting', 'fengzhuang', 'jiadingchengqu', 'jiangqiaoxincheng', 'nanxiang', 'huaxin', 'qingpuxincheng', 'xujing', 'zhaoxiang', 'zhonggu', 'zhujiajiao', 'fengcheng', 'haiwan', 'nanqiao', 'fengjing', 'jinshanxincheng', 'tinglin', 'zhujing', 'baozhen', 'chenjiazhen', 'changxingdao']
    for bk in bk_list:
        url='http://shanghai.anjuke.com/sale/'+bk+'/p1/#filtersort'
        spider.feed(url)
    spider.start()
