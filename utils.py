# coding:utf-8
# coded by Shull Xu

from urllib import parse
import lxml.html as H
from splinter import Browser
import requests
import re
import cmath
from BitVector import BitVector

class WebKit(object):
    '''WebKit引擎'''
    def __init__(self):
        self.tag_attr_dict={'*':'href',
                            'embed':'src',
                            'frame':'src',
                            'object':'data'}

    def extract_links(self,url):
        '''提取页面中链接'''
        self.browser=Browser("phantomjs")
        try:
            self.browser.visit(url)
        except Exception as e:
            return
        for tag,attr in self.tag_attr_dict.items():
            link_list=self.browser.find_by_xpath('//%s[@%s]' % (tag,attr))
            if not link_list:
                continue
            for link in link_list:
                link=link.__getitem__(attr)
                if not link:
                    continue
                link=link.strip()
                if link=='about:blank' or link.startwith('javascript:'):
                    continue
                if not link.startwith('http'):
                    link=parse.urljoin(url,link)
                yield link

class HtmlAnalyzer(object):
    '''页面链接解析'''
    @staticmethod
    def extract_links(html,base_ref,tags=[]):
        '''提取页面中链接
        base_ref    ：用于把相对链接转成绝对链接
        tags        :用于匹配链接的标签'''
        if not html.strip():
            return

        try:
            doc=H.document_fromstring(html)
        except Exception as e:
            return

        default_tags=['a']
        default_tags.extend(tags)
        default_tags = list(set(default_tags))
        doc.make_links_absolute(base_ref)
        links_in_doc = doc.iterlinks()
        for link in links_in_doc:
            if link[0].tag in default_tags:
                yield link[2]

class allow_url(object):
    '''限定目标url'''
    def __init__(self,url):
        self.url=url
        self.allow=False

    def isallowed(self):
        pattern=re.compile('http://shanghai.anjuke.com/sale/\w+/p\d+/#filtersort')
        match=pattern.findall(self.url)
        if match:
            self.allow=True
        return self.allow

class BloomFilter(object):
    '''布隆过滤器'''
    def __init__(self,error_rate,element_num):
        self.bit_num=


