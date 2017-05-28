import random  
import math
from DBConnector import DBConnector
 
def load_data():
    
    train = {} 
    inDBConnector = DBConnector()
    query = 'SELECT uid, aid, LOG(total+1) FROM norm_as WHERE tg = 1'
    for item in inDBConnector.runQuery(query):
        train.setdefault(str(item[0]), {})
        train[str(item[0])][str(item[1])] = float(item[2])
    return train

def initialFeature(feature, userNum, movieNum):  

    random.seed(0)
    user_feature = {}
    item_feature = {}
    
    for i in range(1, userNum + 1):
        si = str(i)
        user_feature.setdefault(si, {})
        for j in range(1, feature + 1):
            sj = str(j)
            user_feature[si].setdefault(sj, random.uniform(0, 1))

    for i in range(1, movieNum + 1):  
        si = str(i)  
        item_feature.setdefault(si, {})  
        for j in range(1, feature + 1):  
            sj = str(j)  
            item_feature[si].setdefault(sj, random.uniform(0, 1))  

    return user_feature, item_feature  

def svd(train, feature, user_feature, item_feature):  

    gama = 0.02
    lamda = 0.1
    slowRate = 0.01
    step = 0
    preRmse = 1000000000.0
    nowRmse = 0.0
    
    while step < 100:  
        rmse = 0.0
        n = 0
        for u in train.keys():  
            for i in train[u].keys():  
                pui = 0  
                for k in range(1, feature + 1):  
                    sk = str(k)  
                    pui += user_feature[u][sk] * item_feature[i][sk]  
                eui = train[u][i] - pui  
                rmse += pow(eui, 2)  
                n += 1   
                for k in range(1, feature + 1):  
                    sk = str(k)  
                    user_feature[u][sk] += gama * (eui * item_feature[i][sk] - lamda * user_feature[u][sk])  
                    item_feature[i][sk] += gama * (eui * user_feature[u][sk] - lamda * item_feature[i][sk])  

        nowRmse = math.sqrt(rmse * 1.0 / n)  
        print 'step: %d      Rmse: %s' % ((step + 1), nowRmse)  
        if (nowRmse < preRmse):  
            preRmse = nowRmse  
              
        gama *= (1 - slowRate)
        step += 1

    return user_feature, item_feature

def saveFeature(feature, userNum, user_feature):
    fileName = 'user_feature_global.txt'
    f = open(fileName, 'w+')
    for i in range(1, userNum + 1):
        si = str(i)
        f.write(si)
        for j in range(1, feature + 1):
            sj = str(j)
            f.write('\t%f' % user_feature[si][sj])
        f.write('\n')
    f.close()

def measure(feature, userNum, movieNum, train, user_feature, item_feature):
    
    whn = 0.0
    for u in range (1, userNum + 1):
        su = str(u)
        res = []
        for i in range(1, movieNum + 1):
            si = str(i)
            if not train[su].has_key(si):  # exclude the known items
                pre = 0.0
                for k in range(1, feature + 1):
                    sk = str(k)
                    pre += user_feature[su][sk] * item_feature[si][sk]
                res.append([i, pre])
        topn = sorted(res, key=lambda l:l[1], reverse=True)[0:40]  # Top-N
        
        inDBConnector = DBConnector()
        query = 'SELECT aid, LOG(pc+1) FROM testdata WHERE uid = %d' % u
        rd = dict(inDBConnector.runQuery(query))

        for i in topn:
            if rd.has_key(i[0]):
                whn += rd[i[0]]
        print u, whn

if __name__ == "__main__":
    
    train = load_data()  
    print 'load data success'
    
    user_feature, item_feature = initialFeature(20, 10005, 87458)
    print 'initial user and item feature success'
    
    user_feature, item_feature = svd(train, 20, user_feature, item_feature)
    print 'svd with stochastic gradient descent success'
    
    saveFeature(20, 10005, user_feature)
    print 'dump data success'
    
    measure(20, 1100, 87458, train, user_feature, item_feature)