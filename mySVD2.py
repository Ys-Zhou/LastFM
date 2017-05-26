import multiprocessing as mp
import math 
import random  
import time
from DBConnector import DBConnector

class SvdProc(mp.Process):
    def __init__ (self, inTg, inRaw, inLock, inSvdNowStep, inAvgNowStep, inSvdItemFeature, inAvgItemFeature):
        mp.Process.__init__(self)
        self.train = {}
        self.user_feature = {}
        self.item_feature = {}
        self.tg = inTg
        self.raw = inRaw
        # Load server values
        self.lock = inLock
        self.svdNowStep = inSvdNowStep
        self.avgNowStep = inAvgNowStep
        self.svdItemFeature = inSvdItemFeature
        self.avgItemFeature = inAvgItemFeature
        # Local attributes
        self.feature = 20
        self.userNum = 10005
        self.movieNum = 87458
        
    def load_data(self):
        for item in self.raw:
            self.train.setdefault(str(item[0]), {})
            self.train[str(item[0])][str(item[1])] = float(item[2])

    def initialFeature(self):  
        random.seed(0)    
        for i in range(1, self.userNum + 1):  
            si = str(i)  
            self.user_feature.setdefault(si, {})   
            for j in range(1, self.feature + 1):  
                sj = str(j)  
                self.user_feature[si].setdefault(sj, random.uniform(0, 1))   
 
        for i in range(1, self.movieNum + 1):  
            si = str(i)  
            self.item_feature.setdefault(si, {})  
            for j in range(1, self.feature + 1):  
                sj = str(j)  
                self.item_feature[si].setdefault(sj, random.uniform(0, 1))  

    def savetxt(self):
        fileName = 'user_feature_tg%d.txt' % self.tg
        f = open(fileName, 'w+')
        for i in range(1, self.userNum + 1):
            si = str(i)
            f.write('%s' % si)
            for j in range(1, self.feature + 1):
                sj = str(j)
                f.write('\t%f' % self.user_feature[si][sj])
            f.write('\n')    
        f.close()
        
    def run(self):  
        self.load_data()
        print 'Process-%d: load data success' % self.tg
        
        self.initialFeature()
        print 'Process-%d: initial user and item feature, respectly success' % self.tg
        
        # SVD parameter
        gama = 0.02
        lamda = 0.3
        slowRate = 0.99
        step = 0
        preRmse = 1000000000.0
        nowRmse = 0.0

        while step < 10:
            rmse = 0.0
            n = 0
            for u in self.train.keys():  
                for i in self.train[u].keys():  
                    pui = 0  
                    for k in range(1, self.feature + 1):
                        sk = str(k)  
                        pui += self.user_feature[u][sk] * self.item_feature[i][sk]  
                    eui = self.train[u][i] - pui  
                    rmse += pow(eui, 2)  
                    n += 1  
                    for k in range(1, self.feature + 1):  
                        sk = str(k)  
                        self.user_feature[u][sk] += gama * (eui * self.item_feature[i][sk] - lamda * self.user_feature[u][sk])  
                        self.item_feature[i][sk] += gama * (eui * self.user_feature[u][sk] - lamda * self.item_feature[i][sk])  
                  
            nowRmse = math.sqrt(rmse * 1.0 / n)  
            print 'Process-%d: step: %d      Rmse: %s' % (self.tg, (step + 1), nowRmse)  
            if (nowRmse < preRmse):  
                preRmse = nowRmse  
                  
            gama *= slowRate  
            step += 1

            while True:
                if self.lock.acquire():
                    if self.svdNowStep[self.tg - 1] < step:
                        self.svdItemFeature[self.tg - 1] = self.item_feature
                        self.svdNowStep[self.tg - 1] = step
                    if self.avgNowStep.value == step:
                        self.item_feature = self.avgItemFeature
                        self.lock.release()
                        break
                    self.lock.release()
                    time.sleep(1)

        self.savetxt()
        print 'Process-%d: svd + stochastic gradient descent success' % self.tg

class AvgProc(mp.Process):
    def __init__ (self, inLock, inSvdNowStep, inAvgNowStep, inSvdItemFeature, inAvgItemFeature):
        mp.Process.__init__(self)
        # Load server values
        self.lock = inLock
        self.svdNowStep = inSvdNowStep
        self.avgNowStep = inAvgNowStep
        self.svdItemFeature = inSvdItemFeature
        self.avgItemFeature = inAvgItemFeature
        # Local attributes
        self.feature = 20
        self.movieNum = 87458
        self.processNum = 6
    
    def run(self):
        while True:
            if self.lock.acquire():
                if list(self.svdNowStep) == [self.avgNowStep.value + 1] * self.processNum:
                    lSvdItemFeature = [0] * self.processNum
                    for t in range(self.processNum):
                        lSvdItemFeature[t] = self.svdItemFeature[t]
                    lAvgItemFeature = {}
                    for i in range(1, self.movieNum + 1):
                        si = str(i)
                        lAvgItemFeature.setdefault(si, {})
                        for j in range(1, self.feature + 1):
                            sj = str(j)
                            value = 0.0
                            for t in range(self.processNum):
                                value += lSvdItemFeature[t][si][sj]
                            lAvgItemFeature[si].setdefault(sj, value / float(self.processNum)) 
                    self.avgItemFeature.update(lAvgItemFeature)
                    self.avgNowStep.value += 1
                    print 'Process-avg: done'
                self.lock.release()
            time.sleep(10)

if __name__ == '__main__':
    # Main process values
    feature = 20
    movieNum = 87458
    processNum = 6
    
    # Process locker
    lock = mp.Lock()
    
    # Server Process
    manager = mp.Manager()
    svdNowStep = manager.list([0] * processNum)
    avgNowStep = manager.Value('i', 0)
    svdItemFeature = manager.list([0] * processNum)
    avgItemFeature = manager.dict()
    
    # Do queries at main process due to avoid database block
    inDBConnector = DBConnector()
    
    query = 'SELECT users.uid, artists.aid, norm_as.tg1*LOG(norm_as.total+1) FROM norm_as JOIN users ON norm_as.uname = users.uname JOIN artists ON norm_as.aname = artists.aname'
    r_list = inDBConnector.runQuery(query)
    svd1 = SvdProc(1, r_list, lock, svdNowStep, avgNowStep, svdItemFeature, avgItemFeature)
    svd1.daemon = True
    svd1.start()

    query = 'SELECT users.uid, artists.aid, norm_as.tg2*LOG(norm_as.total+1) FROM norm_as JOIN users ON norm_as.uname = users.uname JOIN artists ON norm_as.aname = artists.aname'
    r_list = inDBConnector.runQuery(query)
    svd2 = SvdProc(2, r_list, lock, svdNowStep, avgNowStep, svdItemFeature, avgItemFeature)
    svd2.daemon = True
    svd2.start()
    
    query = 'SELECT users.uid, artists.aid, norm_as.tg3*LOG(norm_as.total+1) FROM norm_as JOIN users ON norm_as.uname = users.uname JOIN artists ON norm_as.aname = artists.aname'
    r_list = inDBConnector.runQuery(query)
    svd3 = SvdProc(3, r_list, lock, svdNowStep, avgNowStep, svdItemFeature, avgItemFeature)
    svd3.daemon = True
    svd3.start()
    
    query = 'SELECT users.uid, artists.aid, norm_as.tg4*LOG(norm_as.total+1) FROM norm_as JOIN users ON norm_as.uname = users.uname JOIN artists ON norm_as.aname = artists.aname'
    r_list = inDBConnector.runQuery(query)
    svd4 = SvdProc(4, r_list, lock, svdNowStep, avgNowStep, svdItemFeature, avgItemFeature)
    svd4.daemon = True
    svd4.start()
    
    query = 'SELECT users.uid, artists.aid, norm_as.tg5*LOG(norm_as.total+1) FROM norm_as JOIN users ON norm_as.uname = users.uname JOIN artists ON norm_as.aname = artists.aname'
    r_list = inDBConnector.runQuery(query)
    svd5 = SvdProc(5, r_list, lock, svdNowStep, avgNowStep, svdItemFeature, avgItemFeature)
    svd5.daemon = True
    svd5.start()
    
    query = 'SELECT users.uid, artists.aid, norm_as.tg6*LOG(norm_as.total+1) FROM norm_as JOIN users ON norm_as.uname = users.uname JOIN artists ON norm_as.aname = artists.aname'
    r_list = inDBConnector.runQuery(query)
    svd6 = SvdProc(6, r_list, lock, svdNowStep, avgNowStep, svdItemFeature, avgItemFeature)
    svd6.daemon = True
    svd6.start()
    
    avg = AvgProc(lock, svdNowStep, avgNowStep, svdItemFeature, avgItemFeature)
    avg.daemon = True
    avg.start()