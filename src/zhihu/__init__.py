# -*- coding: utf-8 -*-
import requests
import urllib
import re
import random
from time import sleep
def main():
    url='https://www.zhihu.com/'
    #感觉这个话题下面美女多
    headers={}
    i=1
    #  x in xrange(20,3600,20): 
    for x in xrange(20,60,20): # x in (20 40 60)
        print x
        data={'start':'0',
              'offset':str(x),
              '_xsrf':'a128464ef225a69348cef94c38f4e428'}
        #知乎用offset控制加载的个数，每次响应加载20
        content=requests.post(url,headers=headers,data=data,timeout=10).text
        print 'content:',content
        #用post提交form data
        imgs=re.findall('<img src=\\\\\"(.*?)_m.jpg',content) 
        #在爬下来的json上用正则提取图片地址，去掉_m为大图 
        print 'imgs:',imgs
    for img in imgs:
        print i
        try:
            img=img.replace('\\','')
            #去掉\字符这个干扰成分
            pic=img+'.jpg'
            path='d:\\bs4\\zhihu\\jpg\\'+str(i)+'.jpg'
            #声明存储地址及图片名称
            urllib.urlretrieve(pic,path)
            #下载图片
            print u'下载了第'+str(i)+u'张图片'
            i+=1
            sleep(random.uniform(0.5,1))
        #睡眠函数用于防止爬取过快被封IP
        except:
            print u'抓漏1张'
            pass
        sleep(random.uniform(0.5,1))

if __name__=='__main__':
    print "start" 
    main()
    print "end"