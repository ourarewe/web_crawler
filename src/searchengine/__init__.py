#coding:utf-8
import urllib2
from BeautifulSoup import *
from urlparse import urljoin
import mysql.connector
import nn
mynet=nn.searchnet('searchindex')

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
        v=soup.string
        if v==None:   
            c=soup.contents
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
        for i in range(depth):
            newpages=set()
            j = 0  # 限制爬取的网页个数
            for page in pages:
                if j>=4 : break  
                j+=1
                try:
                    c=urllib2.urlopen(page)
                except:
                    print "Could not open %s" % page
                    continue
                try:
                    soup=BeautifulSoup(c.read())
                    self.addtoindex(page,soup)
                    links=soup('a')
                    for link in links:
                        if ('href' in dict(link.attrs)):
                            url=urljoin(page,link['href'])
                            if url.find("'")!=-1: continue
                            url=url.split('#')[0]  # remove location portion
                            if url[0:4]=='http' and not self.isindexed(url):
                                newpages.add(url)
                            linkText=self.gettextonly(link)
                            self.addlinkref(page,url,linkText)
                    self.dbcommit()
                except:
                    print "Could not parse page %s" % page
            pages=newpages

    
    # Create the database tables
    def createindextables(self):
            self.cursor.execute('create table urllist(id INT NOT NULL AUTO_INCREMENT,url varchar(250),PRIMARY KEY(id))')
            self.cursor.execute('create table wordlist(id INT NOT NULL AUTO_INCREMENT,word varchar(50),PRIMARY KEY(id))')
            self.cursor.execute('create table wordlocation(id INT NOT NULL AUTO_INCREMENT,urlid int,wordid int,location bigint,PRIMARY KEY(id))')
            self.cursor.execute('create table link(id INT NOT NULL AUTO_INCREMENT,fromid int,toid int,PRIMARY KEY(id))')
            self.cursor.execute('create table linkwords(id INT NOT NULL AUTO_INCREMENT,wordid int,linkid int,PRIMARY KEY(id))')
            self.cursor.execute('create index wordidx on wordlist(word)')
            self.cursor.execute('create index urlidx on urllist(url)')
            self.cursor.execute('create index wordurlidx on wordlocation(wordid)')
            self.cursor.execute('create index urltoidx on link(toid)')
            self.cursor.execute('create index urlfromidx on link(fromid)')
            self.dbcommit()

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
                linkerlist = self.cursor.fetchall()
                for (linker,) in linkerlist:
                    # Get the page rank of the linker
                    self.cursor.execute('select score from pagerank where urlid=%d' % linker)
                    linkingpr=self.cursor.fetchone()[0]
                    try:
                        remaining_rows = self.cursor.fetchall()  # 防止cursor还有剩余结果导致报错
                    except: pass
                    # Get the total number of links from the linker
                    self.cursor.execute('select count(*) from link where fromid=%d' % linker)
                    linkingcount=self.cursor.fetchone()[0]
                    pr+=0.85*(linkingpr/linkingcount)
                self.cursor.execute('update pagerank set score=%f where urlid=%d' % (pr,urlid))
            self.dbcommit()



class searcher:
    def __init__(self,dbname):
        self.cnx = mysql.connector.connect(user='root', password='',
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
            else: return None,word
        # Create the query from the separate parts
        fullquery='select %s from %s where %s' % (fieldlist,tablelist,clauselist)
        #print fullquery
        self.cursor.execute(fullquery)
        rows=[row for row in self.cursor]
        return rows,wordids
    
    def getscoredlist(self,rows,wordids):
        totalscores=dict([(row[0],0) for row in rows])
        # This is where we'll put our scoring functions
        weights=[(1.0,self.locationscore(rows)), 
                 (1.0,self.frequencyscore(rows)),
                 (1.0,self.pagerankscore(rows)),
                 (1.0,self.linktextscore(rows,wordids))]
        # (5.0,self.nnscore(rows,wordids))
        for (weight,scores) in weights:
            for url in totalscores:
                totalscores[url]+=weight*scores[url]
        return totalscores

    def geturlname(self,id):
        self.cursor.execute("select url from urllist where id=%d" % id)
        return self.cursor.fetchone()[0]

    def query(self,q):
        rows,wordids=self.getmatchrows(q)
        if rows==None:
            print 'not found',wordids
            return None,None
        else:
            scores=self.getscoredlist(rows,wordids)
            rankedscores=sorted([(score,url) for (url,score) in scores.items()],reverse=1)
            for (score,urlid) in rankedscores[0:10]:
                print '%f\t%s' % (score,self.geturlname(urlid))
            return wordids,[r[1] for r in rankedscores[0:10]]

    def normalizescores(self,scores,smallIsBetter=0):
        vsmall=0.00001 # Avoid division by zero errors
        if smallIsBetter:
            minscore=min(scores.values())
            return dict([(u,float(minscore)/max(vsmall,l)) for (u,l) in scores.items()])
        else:
            maxscore=max(scores.values())
            if maxscore==0: maxscore=vsmall
            return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])

    # 网页评价方法
    # 1、单词频度
    def frequencyscore(self,rows):
        counts=dict([(row[0],0) for row in rows])
        for row in rows: counts[row[0]]+=1
        return self.normalizescores(counts)

    # 2、文档位置
    def locationscore(self,rows):
        locations=dict([(row[0],1000000) for row in rows])
        for row in rows:
            loc=sum(row[1:])
            if loc<locations[row[0]]: locations[row[0]]=loc
        return self.normalizescores(locations,smallIsBetter=1)

    # 3、单词距离
    def distancescore(self,rows):
        # If there's only one word, everyone wins!
        if len(rows[0])<=2: return dict([(row[0],1.0) for row in rows])
        # Initialize the dictionary with large values
        mindistance=dict([(row[0],1000000) for row in rows])
        for row in rows:
            dist=sum([abs(row[i]-row[i-1]) for i in range(2,len(row))])
            if dist<mindistance[row[0]]: mindistance[row[0]]=dist
        return self.normalizescores(mindistance,smallIsBetter=1)

    # 4、利用外部回指链接
    # 4.1、简单计数
    def inboundlinkscore(self,rows):
        uniqueurls=set([row[0] for row in rows])
        inboundcount = {}
        for u in uniqueurls:
            self.cursor.execute('select count(*) from link where toid=%d' % u)
            inboundcount[u] = self.cursor.fetchone()[0]
        return self.normalizescores(inboundcount)

    # 4.2、PageRank分数
    def pagerankscore(self,rows):
        pageranks = {}
        row_0_set = set([row[0] for row in rows])
        for row0 in row_0_set:
            self.cursor.execute('select score from pagerank where urlid=%d' % row0)
            pageranks[row0] = self.cursor.fetchone()[0]
        maxrank=max(pageranks.values())
        normalizedscores=dict([(u,float(l)/maxrank) for (u,l) in pageranks.items()])
        return normalizedscores

    # 5、利用链接文本
    def linktextscore(self,rows,wordids):
        linkscores=dict([(row[0],0) for row in rows])
        for wordid in wordids:
            self.cursor.execute('select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.id' % wordid)
            curlist = self.cursor.fetchall()
            for (fromid,toid) in curlist:
                if toid in linkscores:
                    self.cursor.execute('select score from pagerank where urlid=%d' % fromid)
                    pr=self.cursor.fetchone()[0]
                    linkscores[toid]+=pr
        maxscore=max(linkscores.values())
        normalizedscores=dict([(u,float(l)/max(maxscore,0.00001)) for (u,l) in linkscores.items()])
        return normalizedscores
    
    # 6、利用神经网络
    def nnscore(self,rows,wordids):
        # Get unique URL IDs as an ordered list
        urlids=[urlid for urlid in dict([(row[0],1) for row in rows])]
        nnres=mynet.getresult(wordids,urlids)
        scores=dict([(urlids[i],nnres[i]) for i in range(len(urlids))])
        return self.normalizescores(scores)

    
    
    


