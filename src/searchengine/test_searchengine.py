#coding:utf-8
import urllib2
#import BeautifulSoup
from BeautifulSoup import *
from urlparse import urljoin
import mysql.connector
import codecs  


print 'start>>>'
# Create a list of words to ignore
#ignorewords={'the':1,'of':1,'to':1,'and':1,'a':1,'in':1,'is':1,'it':1}
ignorewords=set(['the','of','to','and','a','in','is','it'])

class crawler:
    # Initialize the crawler with the name of database
    def __init__(self,dbname):
        self.cnx = mysql.connector.connect(user='root', password='',
                                           host='127.0.0.1',
                                           database=dbname)
        self.cursor = self.cnx.cursor()
  
    def __del__(self): 
        self.cnx.close()
    
    def dbcommit(self): 
        self.cnx.commit()
    
    # 辅助函数，用于获取条目的id，并且如果条目不存在，就将其加入数据库中
    # Auxilliary function for getting an entry id and adding it if it's not present
    def getentryid(self,table,field,value,createnew=True):
        self.cursor.execute("select id from %s where %s='%s'" % (table,field,value))
        res=self.cursor.fetchone()
        if res==None:
            self.cursor.execute("insert into %s (%s) values ('%s')" % (table,field,value))
            return self.cursor.lastrowid
        else:
            return res[0] 
    
    # Index an individual page
    def addtoindex(self,url,soup):
        if self.isindexed(url): return
        print 'Indexing '+url
        # Get the individual words
        text=self.gettextonly(soup)
        words=self.separatewords(text)
        # Get the URL id
        urlid=self.getentryid('urllist','url',url)
        # Link each word to this url
        for i in range(len(words)):
            word=words[i]
            if word in ignorewords: continue
            wordid=self.getentryid('wordlist','word',word)
            self.cursor.execute("insert into wordlocation(urlid,wordid,location) values (%d,%d,%d)" % (urlid,wordid,i))
  
    
    # Extract the text from an HTML page (no tags)
    def gettextonly(self,soup):
        # If a tag contains more than one thing, then it’s not clear what .string should refer to, 
        # so .string is defined to be None:
        v=soup.string
        print 'soup(link).string:',v
        if v==None:   
            c=soup.contents
            print 'soup(link).contents:',c
            resulttext=''
            for t in c:
                subtext=self.gettextonly(t)
                resulttext+=subtext+'\n'
            return resulttext
        else:
            return v.strip()

    # Seperate the words by any non-whitespace character
    def separatewords(self,text):
        splitter=re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s!='']
    
    # Return true if this url is already indexed
    def isindexed(self,url):
        # 判断网页是否已存入数据库
        self.cursor.execute("select id from urllist where url='%s'" % url)
        u = self.cursor.fetchone()
        if u!= None:
            # 如果存在，判断是否有单词与之关联
            self.cursor.execute("select * from wordlocation where urlid=%d" % u[0])
            v = self.cursor.fetchone()
            if v!=None: 
                try:
                    remaining_rows = self.cursor.fetchall()  # 防止cursor还有剩余结果导致报错
                    return True
                except: return True
        return False
               
    
    # Add a link between two pages
    def addlinkref(self,urlFrom,urlTo,linkText):
        words=self.separatewords(linkText)
        fromid=self.getentryid('urllist','url',urlFrom)
        toid=self.getentryid('urllist','url',urlTo)
        if fromid==toid: return
        self.cursor.execute("insert into link(fromid,toid) values (%d,%d)" % (fromid,toid))
        linkid=self.cursor.lastrowid
        for word in words:
            if word in ignorewords: continue
            wordid=self.getentryid('wordlist','word',word)
            self.cursor.execute("insert into linkwords(linkid,wordid) values (%d,%d)" % (linkid,wordid))

    
    # Starting with a list of pages, do a breadth
    # first search to the given depth, indexing pages
    # as we go
    def crawl(self,pages,depth=2):
        f1 = open('C:\Users\Administrator\Desktop\openurl.txt','w')
        f2 = codecs.open('C:\Users\Administrator\Desktop\soup.txt','w',"utf8")
        f3 = codecs.open('C:\Users\Administrator\Desktop\soup_tex.txt','a',"ascii")
        for i in range(depth):
            newpages=set()
            j = 0  # 限制爬取的网页个数
            for page in pages:
                if j>=4 : break  
                j+=1
                c=urllib2.urlopen(page)
                #print 'urlopen(page):',c
                c_read = c.read()
                f1.write(c_read+'\n')
                soup=BeautifulSoup(c_read)
                #print 'soup:',soup
                #self.addtoindex(page,soup)
                links=soup('a')
                print 'links',links
                for link in links:
                    print 'link:',link
                    if ('href' in dict(link.attrs)):
                        url=urljoin(page,link['href'])
                        if url.find("'")!=-1: continue
                        url=url.split('#')[0]  # remove location portion
                        #print 'url:',url
                        #print 'link:',link
                        if url[0:4]=='http': #and not self.isindexed(url):
                            newpages.add(url)
                        linkText=self.gettextonly(link)
                        print 'linkText:\n',linkText
                        #f3.write(linkText+'\n')
                        #self.addlinkref(page,url,linkText)
                #self.dbcommit()
            f1.close();f2.close();f3.close()
            pages=newpages

  
            
    # 计算PageRank值
    def calculatepagerank(self,iterations=20):
        # clear out the current page rank tables
        self.cursor.execute('drop table if exists pagerank')
        self.cursor.execute('create table pagerank(urlid int primary key,score double)') 
        # initialize every url with a page rank of 1
        self.cursor.execute('insert into pagerank select id,1.0 from urllist')
        self.dbcommit() 
        for i in range(iterations):
            print "Iteration %d" % (i)
            self.cursor.execute('select id from urllist')
            urlidlist = self.cursor.fetchall()
            for (urlid,) in urlidlist:
                pr=0.15  
                # Loop through all the pages that link to this one
                self.cursor.execute('select distinct fromid from link where toid=%d' % urlid)
                for (linker,) in self.cursor:
                    # Get the page rank of the linker
                    self.cursor.execute('select score from pagerank where urlid=%d' % linker)
                    linkingpr=self.cursor.fetchone()[0]
                    # Get the total number of links from the linker
                    self.cursor.execute('select count(*) from link where fromid=%d' % linker)
                    linkingcount=self.cursor.fetchone()[0]
                    pr+=0.85*(linkingpr/linkingcount)
                self.cursor.execute('update pagerank set score=%f where urlid=%d' % (pr,urlid))
            self.dbcommit()


class searcher:
    def __init__(self,dbname):
        self.cnx = mysql.connector.connect(user='root', password='555321',
                                           host='127.0.0.1',
                                           database=dbname)
        self.cursor = self.cnx.cursor()

    def __del__(self):
        self.cnx.close()
        
    
    def getmatchrows(self,q):
        # Strings to build the query
        fieldlist='w0.urlid'
        tablelist=''  
        clauselist=''
        wordids=[]
        # Split the words by spaces
        words=q.split(' ')  
        tablenumber=0  
        for word in words:
            # Get the word ID
            self.cursor.execute("select id from wordlist where word='%s'" % word)
            wordrow = self.cursor.fetchone()
            try:remaining_rows = self.cursor.fetchall()  # 防止cursor还有剩余结果导致报错
            except:pass
            if wordrow!=None:
                wordid=wordrow[0]
                wordids.append(wordid)
                if tablenumber>0:
                    tablelist+=','
                    clauselist+=' and '
                    clauselist+='w%d.urlid=w%d.urlid and ' % (tablenumber-1,tablenumber)
                fieldlist+=',w%d.location' % tablenumber
                tablelist+='wordlocation w%d' % tablenumber      
                clauselist+='w%d.wordid=%d' % (tablenumber,wordid)
                tablenumber+=1
        # Create the query from the separate parts
        fullquery='select %s from %s where %s' % (fieldlist,tablelist,clauselist)
        print fullquery
        #cur=self.cursor.execute(fullquery)
        #rows=[row for row in cur]
        #return rows,wordids



crawler = crawler('searchindex')
'http://www.bilibili.com/' 'http://www.gdut.edu.cn/' 'https://en.wikipedia.org/wiki/Main_Page' 'http://www.lawtime.cn/gongan/city/p1/shenzhen'
'http://www.junranlaw.com/news1-cylxfs-371.html'
crawler.crawl(['https://en.wikipedia.org/wiki/Main_Page'])

#searcher = searcher('searchindex')
#searcher.getmatchrows('thanks main hello')



print 'finished'
