# coding:utf-8
# coded by Shull Xu
from redis import Redis

class SavePage(object):
    '''a customed fetch plugin'''
    @classmethod
    def start(cls,urldata):
        '''"start" func is the start point of plugin'''
        conn=Redis(host='localhost',port=6379,db=0,password=None)
        hash_id=hash(urldata)
        conn.hmset(hash_id,{'url':urldata.url,
                            'html':urldata.html,})





