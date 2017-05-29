# -*- coding: utf-8 -*-
import random  
import math
from DBConnector import DBConnector
 
def load_data():
    
    inDBConnector = DBConnector()
    
    train = {}
    query = 'SELECT * FROM traindata_g'
    for item in inDBConnector.runQuery(query):
        train.setdefault(str(item[0]), {})
        train[str(item[0])][str(item[1])] = float(item[2])
    
    query = 'SELECT DISTINCT userid FROM traindata_g'
    rList = inDBConnector.runQuery(query)
    userList = map(list, zip(*rList))[0]
    userList = map(str, userList)
    
    query = 'SELECT DISTINCT gameid FROM traindata_g'
    rList = inDBConnector.runQuery(query)
    gameList = map(list, zip(*rList))[0]
    gameList = map(str, gameList)
    
    return train, userList, gameList

def initialFeature(feature, userList, gameList):  

    random.seed(0)
    user_feature = {}
    item_feature = {}
    
    for u in userList:
        user_feature.setdefault(u, {})
        for f in range(1, feature + 1):
            user_feature[u].setdefault(f, random.uniform(0, 1))

    for g in gameList:
        item_feature.setdefault(g, {})
        for f in range(1, feature + 1):
            item_feature[g].setdefault(f, random.uniform(0, 1))

    return user_feature, item_feature  

def svd(train, feature, user_feature, item_feature):  

    # SVD parameters
    gama = 0.02
    lamda = 0.1
    slowRate = 0.01
    stopAt = 0.001
    
    step = 0
    preRmse = 1000000000.0
    nowRmse = 0.0
    
    while step < 200:  # max step
        rmse = 0.0
        n = 0
        for u in train.keys():  
            for i in train[u].keys():  
                pui = 0.0 
                for k in range(1, feature + 1):  
                    pui += user_feature[u][k] * item_feature[i][k]    
                eui = train[u][i] - pui 
                rmse += pow(eui, 2)
                n += 1   
                for k in range(1, feature + 1):
                    user_feature[u][k] += gama * (eui * item_feature[i][k] - lamda * user_feature[u][k])
                    item_feature[i][k] += gama * (eui * user_feature[u][k] - lamda * item_feature[i][k])

        nowRmse = math.sqrt(rmse * 1.0 / n)  
        print 'step: %d      Rmse: %s' % ((step + 1), nowRmse)
        
        if nowRmse < preRmse * (1 - stopAt):  
            preRmse = nowRmse  
        else:
            break
        gama *= (1 - slowRate)
        step += 1

    return user_feature, item_feature

def saveFeature(feature, user_feature):
    fileName = 'user_feature_global.txt'
    f = open(fileName, 'w+')
    for u in user_feature.keys():
        f.write(u)
        for k in range(1, feature + 1):
            f.write('\t%f' % user_feature[u][k])
        f.write('\n')
    f.close()

def measure(feature, testNum, train, user_feature, item_feature):
    
    whn = 0.0

    inDBConnector = DBConnector()
    query = 'SELECT userid, COUNT(gameid) AS cr FROM date170528 WHERE userid IN (SELECT userid FROM traindata_g) GROUP BY userid ORDER BY cr DESC LIMIT 300'
    rList = inDBConnector.runQuery(query)
    test_user = map(list, zip(*rList))[0]
    
    for u in test_user:
        res = []
        for i in item_feature.keys():
            if not train[u].has_key(i):  # exclude the known items
                pre = 0.0
                for k in range(1, feature + 1):
                    pre += user_feature[u][k] * item_feature[i][k]
                res.append([i, pre])
        topn = sorted(res, key=lambda l:l[1], reverse=True)[0:50]  # Top-N
        
        inDBConnector = DBConnector()
        query = 'SELECT gameid, LOG(twoweeks+1) FROM date170528 WHERE userid = %d' % int(u)
        cd = dict(inDBConnector.runQuery(query))

        for i in range(50):  # Top-N
            if cd.has_key(topn[i][0]):
                whn += cd[topn[i][0]] / (math.log(i + 1) + 1)
        print u, whn

if __name__ == "__main__":
    
    train, userList, gameList = load_data()  
    print 'load data success'
    
    user_feature, item_feature = initialFeature(20, userList, gameList)
    print 'initial user and item feature success'
    
    user_feature, item_feature = svd(train, 20, user_feature, item_feature)
    print 'svd with stochastic gradient descent success'
    
    saveFeature(20, user_feature)
    print 'dump data success'
    
    measure(20, 200, train, user_feature, item_feature)
