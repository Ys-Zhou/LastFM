# -*- coding: utf-8 -*-
import numpy
import scipy.sparse as sparse
import math
import threading
import time
import warnings
warnings.filterwarnings('error')
from DBConnector import DBConnector
inDBConnector = DBConnector()

threadNowStep = [0, 0, 0, 0, 0, 0]
nowStep = 0
rawlist = [0, 0, 0, 0, 0, 0]
iflist = [0, 0, 0, 0, 0, 0]
feature = 20

class SvdThread(threading.Thread):
    def __init__ (self, mat, matIn):
        threading.Thread.__init__(self)
        self.mat = mat
        self.matIn = matIn
        self.steps = 10
        self.gama = numpy.float64(0.02)
        self.lamda = numpy.float64(0.3)
        
    def run(self):
        global threadStep, iflist  # 声明需要写入的全局变量
        slowRate = 0.99  
#         preRmse = 1000000000.0  
        nowRmse = 0.0
        user_feature = numpy.matrix(numpy.random.rand(10005, feature), dtype=numpy.float64)
        item_feature = numpy.matrix([[]], dtype=numpy.float64)
      
        for step in range(self.steps):  
            rmse = numpy.float64(0.0)
            n = 0 
            
            while True:  # 读取共享变量
                if con.acquire():
                    item_feature = iflist[self.matIn]
                    print 'Got item feature matrix %d' % self.matIn
                    con.release()
                    break
                time.sleep(1)

            for u in range(self.mat.shape[0]):  
                for i in range(self.mat.shape[1]):  
                    if not numpy.isnan(self.mat[u, i]):
                        pui = numpy.float64(numpy.dot(user_feature[u, :], item_feature[i, :].T))  
                        eui = numpy.float64(self.mat[u, i]) - pui  
                        rmse += eui ** 2
                        n += 1   
                        for k in range(feature):  
                            user_feature[u, k] += self.gama * (eui * item_feature[i, k] - self.lamda * user_feature[u, k])  
                            item_feature[i, k] += self.gama * (eui * user_feature[u, k] - self.lamda * item_feature[i, k])
                        print user_feature[u, :]
                        print item_feature[i, :]
            nowRmse = math.sqrt(rmse * 1.0 / n)  
            print 'step: %d      Rmse: %s' % ((step + 1), nowRmse) 
#             if (nowRmse < preRmse):    
#                 preRmse = nowRmse  
#             else:  
#                 break
            self.gama *= slowRate    
            
            while True:  # 写入共享变量
                if con.acquire():
#                     print self.matIn
#                     print user_feature
                    if threadNowStep[self.matIn] < step:
                        iflist[self.matIn] = item_feature
                        threadNowStep[self.matIn] = step
                    if nowStep == step:
                        con.release()
                        break
                    con.release()
                time.sleep(1)    
        
        fname = 'user_feature%d.txt' % self.matIn
        numpy.savetxt(fname, user_feature)
        
class AvgThread(threading.Thread):
    def __init__ (self):
        threading.Thread.__init__(self)
    
    def run(self):
        global nowStep, iflist
        while True:
            if con.acquire():
                if threadNowStep[0] > nowStep:
#                  and threadNowStep[1] > nowStep and threadNowStep[2] > nowStep and threadNowStep[3] > nowStep and threadNowStep[4] > nowStep and threadNowStep[5] > nowStep:
                    # avgmat = (iflist[0] + iflist[1] + iflist[2] + iflist[3] + iflist[4] + iflist[5]) / 6.0
#                     iflist[0] = avgmat
#                     iflist[1] = avgmat
#                     iflist[2] = avgmat
#                     iflist[3] = avgmat
#                     iflist[4] = avgmat
#                     iflist[5] = avgmat
                    nowStep += 1
                con.release()
            time.sleep(1)

def makeMatrix(p, row, col):
    query = 'SELECT tg%d*LOG(total+1) FROM norm_as' % p
    _list = inDBConnector.runQuery(query)
    d_list = map(list, zip(*r_list))[0]
    data = numpy.array(d_list)

    mtx = sparse.csc_matrix((data, (row, col)), shape=(10005, 87458))
    return mtx

con = threading.Condition()

query = 'SELECT users.uid-1 FROM norm_as JOIN users ON norm_as.uname = users.uname'
r_list = inDBConnector.runQuery(query)
d_list = map(list, zip(*r_list))[0]
row = numpy.array(d_list)

query = 'SELECT artists.aid-1 FROM norm_as JOIN artists ON norm_as.aname = artists.aname'
r_list = inDBConnector.runQuery(query)
d_list = map(list, zip(*r_list))[0]
col = numpy.array(d_list)

rawlist[0] = makeMatrix(1, row, col)
# rawlist[1] = makeMatrix(2)
# rawlist[2] = makeMatrix(3)
# rawlist[3] = makeMatrix(4)
# rawlist[4] = makeMatrix(5)
# rawlist[5] = makeMatrix(6)

for i in range(0, 6):
    iflist[i] = numpy.matrix(numpy.random.rand(87458, feature), dtype='float64') 

subsvd0 = SvdThread(rawlist[0], 0)
subsvd0.start()
# subsvd1 = SvdThread(rawlist[1], 1)
# subsvd1.start()
# subsvd2 = SvdThread(rawlist[2], 2)
# subsvd2.start()
# subsvd3 = SvdThread(rawlist[3], 3)
# subsvd3.start()
# subsvd4 = SvdThread(rawlist[4], 4)
# subsvd4.start()
# subsvd5 = SvdThread(rawlist[5], 5)
# subsvd5.start()
avg = AvgThread()
avg.start()
