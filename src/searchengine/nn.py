#coding:utf-8
from math import tanh
import mysql.connector

def dtanh(y):
    return 1.0-y*y

class searchnet:
    def __init__(self,dbname):
        self.cnx = mysql.connector.connect(user='root', password='',
                                           host='127.0.0.1',
                                           database=dbname)
        self.cursor = self.cnx.cursor()
  
    def __del__(self):
        self.cnx.close()

    def maketables(self):
        self.cursor.execute('create table hiddennode(rowid INT NOT NULL AUTO_INCREMENT,create_key varchar(250),PRIMARY KEY(id))')
        self.cursor.execute('create table wordhidden(rowid INT NOT NULL AUTO_INCREMENT,fromid int,toid int,strength double,PRIMARY KEY(id))')
        self.cursor.execute('create table hiddenurl(rowid INT NOT NULL AUTO_INCREMENT,fromid int,toid int,strength double,PRIMARY KEY(id))')
        self.cursor.execute()

    def getstrength(self,fromid,toid,layer):
        if layer==0: table='wordhidden'
        else: table='hiddenurl'
        self.cursor.execute('select strength from %s where fromid=%d and toid=%d' % (table,fromid,toid))
        res = self.cursor.fetchone()
        if res==None: 
            if layer==0: return -0.2
            if layer==1: return 0
        return res[0]

    def setstrength(self,fromid,toid,layer,strength):
        if layer==0: table='wordhidden'
        else: table='hiddenurl'
        self.cursor.execute('select rowid from %s where fromid=%d and toid=%d' % (table,fromid,toid))
        res = self.cursor.fetchone()
        if res==None: 
            self.cursor.execute('insert into %s (fromid,toid,strength) values (%d,%d,%f)' % (table,fromid,toid,strength))
        else:
            rowid=res[0]
            self.cursor.execute('update %s set strength=%f where rowid=%d' % (table,strength,rowid))

    def generatehiddennode(self,wordids,urls):
        if len(wordids)>3: return None
        # Check if we already created a node for this set of words
        sorted_words=[str(id_) for id_ in wordids]
        sorted_words.sort()
        createkey='_'.join(sorted_words)
        self.cursor.execute("select rowid from hiddennode where create_key='%s'" % createkey)
        res = self.cursor.fetchall() #-----------------fetchone 改为 fetchall-----------------------------------------------------
        # If not, create it
        if res==None:
            self.cursor.execute("insert into hiddennode (create_key) values ('%s')" % createkey)
            hiddenid=self.cursor.lastrowid
            # Put in some default weights
            for wordid in wordids:
                self.setstrength(wordid,hiddenid,0,1.0/len(wordids))
            for urlid in urls:
                self.setstrength(hiddenid,urlid,1,0.1)
            self.cursor.commit()


    def getallhiddenids(self,wordids,urlids):
        l1={}
        for wordid in wordids:
            self.cursor.execute('select toid from wordhidden where fromid=%d' % wordid)
            for row in self.cursor: l1[row[0]]=1
        for urlid in urlids:
            self.cursor.execute('select fromid from hiddenurl where toid=%d' % urlid)
            for row in self.cursor: l1[row[0]]=1
        return l1.keys()

    def setupnetwork(self,wordids,urlids):
        # value lists
        self.wordids=wordids
        self.hiddenids=self.getallhiddenids(wordids,urlids)
        self.urlids=urlids
        # node outputs
        self.ai = [1.0]*len(self.wordids)
        self.ah = [1.0]*len(self.hiddenids)
        self.ao = [1.0]*len(self.urlids)
        # create weights matrix
        self.wi = [[self.getstrength(wordid,hiddenid,0) 
                    for hiddenid in self.hiddenids] 
                   for wordid in self.wordids]
        self.wo = [[self.getstrength(hiddenid,urlid,1) 
                    for urlid in self.urlids] 
                   for hiddenid in self.hiddenids]

    
    def feedforward(self):
        # the only inputs are the query words
        for i in range(len(self.wordids)):
            self.ai[i] = 1.0
        # hidden activations
        for j in range(len(self.hiddenids)):
            sum_ = 0.0
            for i in range(len(self.wordids)):
                sum_ += self.ai[i] * self.wi[i][j]
            self.ah[j] = tanh(sum_)
        # output activations
        for k in range(len(self.urlids)):
            sum_ = 0.0
            for j in range(len(self.hiddenids)):
                sum_ += self.ah[j] * self.wo[j][k]
            self.ao[k] = tanh(sum_)
        return self.ao[:]

    def getresult(self,wordids,urlids):
        self.setupnetwork(wordids,urlids)
        return self.feedforward()

    
    def backPropagate(self, targets, N=0.5):
        # calculate errors for output
        output_deltas = [0.0] * len(self.urlids)
        for k in range(len(self.urlids)):
            error = targets[k]-self.ao[k]
            output_deltas[k] = dtanh(self.ao[k]) * error
        # calculate errors for hidden layer
        hidden_deltas = [0.0] * len(self.hiddenids)
        for j in range(len(self.hiddenids)):
            error = 0.0
            for k in range(len(self.urlids)):
                error = error + output_deltas[k]*self.wo[j][k]
            hidden_deltas[j] = dtanh(self.ah[j]) * error
        # update output weights
        for j in range(len(self.hiddenids)):
            for k in range(len(self.urlids)):
                change = output_deltas[k]*self.ah[j]
                self.wo[j][k] = self.wo[j][k] + N*change
        # update input weights
        for i in range(len(self.wordids)):
            for j in range(len(self.hiddenids)):
                change = hidden_deltas[j]*self.ai[i]
                self.wi[i][j] = self.wi[i][j] + N*change

    def trainquery(self,wordids,urlids,selectedurl): 
        # generate a hidden node if necessary
        self.generatehiddennode(wordids,urlids)
        self.setupnetwork(wordids,urlids)      
        self.feedforward()
        targets=[0.0]*len(urlids)
        targets[urlids.index(selectedurl)]=1.0
        self.backPropagate(targets)
        self.updatedatabase()

    def updatedatabase(self):
        # set them to database values
        for i in range(len(self.wordids)):
            for j in range(len(self.hiddenids)):
                self.setstrength(self.wordids[i],self. hiddenids[j],0,self.wi[i][j])
        for j in range(len(self.hiddenids)):
            for k in range(len(self.urlids)):
                self.setstrength(self.hiddenids[j],self.urlids[k],1,self.wo[j][k])
        self.cursor.commit()
