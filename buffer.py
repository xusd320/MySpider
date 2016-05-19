# coding:utf-8
# coded by Shull Xu

import math
from BitVector import BitVector

class UrlData(object):
    '''url对象类'''
    def __init__(self,url,html=None,depth=0):
        self.url=url
        self.html=html
        self.depth=depth
        self.params={}
        self.fragments={}
        self.post_data={}

    def __str__(self):
        return self.url

    def __repr__(self):
        return '<Url data: %s>' % (self.url,)

    def __hash__(self):
        return hash(self.url)

class UrlCache(object):
    '''URL缓存类'''
    def __init__(self):
        self.__url_cache = {}

    def __len__(self):
        return len(self.__url_cache)

    def __contains__(self,url):
        return hash(url) in self.__url_cache.keys()

    def __iter__(self):
        for url in self.__url_cache:
            yield url


    def insert(self,url):
        if isinstance(url,str):
            url = UrlData(url)
        if url not in self.__url_cache:
            self.__url_cache.setdefault(hash(url),url)

class BloomFilter(object):
    '''布隆过滤器'''
    def __init__(self,error_rate,element_num):
        #计算所需bit数量
        self.bit_num=-1*element_num*math.log(error_rate)/(math.log(2)*math.log(2))
        #4字节对齐
        self.bit_num=self.align_4byte(round(self.bit_num))
        #分配内存
        self.bit_array = BitVector(size=self.bit_num)
        #计算hash函数个数
        self.hash_num=math.log(2)*self.bit_num/element_num
        self.hash_num=round(self.hash_num)+1
        #生成hash函数种子
        self.hash_seeds=self.generate_hashseeds(self.hash_num)

    def insert_element(self,element):
        #插入元素
        for seed in self.hash_seeds:
            hash_val=abs(self.hash_element(element,seed))
            #取模防越界
            hash_val=hash_val % self.bit_num
            self.bit_array[hash_val]=1

    def is_element_exist(self,element):
        for seed in self.hash_seeds:
            hash_val=abs(self.hash_element(element,seed))
            hash_val=hash_val % self.bit_num
            if self.bit_array[hash_val]==0:
                return False
            return True

    def align_4byte(self,bit_num):
        num=int(bit_num/32)
        num=32*(num+1)
        return num

    def generate_hashseeds(self,hash_num):
        count=0
        gap=50
        hash_seeds=[]
        for index in range(hash_num):
            hash_seeds.append(0)
        for index in range(10,10000):
            max_num=int(math.sqrt(index))
            flag=1
            for num in range(2,max_num):
                if max_num % num==0:
                    flag=0
                    break
            if flag==1:
                if count>0 and index-hash_seeds[count-1]<gap:
                    continue
                hash_seeds[count]=index

            if count==hash_num:
                break

            return hash_seeds

    def hash_element(self,element,seed):
        hash_val=1
        for ch in element:
            hash_val=hash_val*seed + ord(ch)
        return hash_val


