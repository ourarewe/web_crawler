#coding:utf-8
from searchengine import *

print 'start>>>'


#crawler = crawler('searchindex')
# 创建数据库
#crawler.createindextables()

# 爬取网页
#crawler.crawl(['https://en.wikipedia.org/wiki/Main_Page'])
# 据算PageRank值
#crawler.calculatepagerank(20)

# 搜索
e = searcher('searchindex')
e.query('war wome')


print 'finished'