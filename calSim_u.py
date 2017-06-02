# -*- coding: utf-8 -*-
from DBConnector import DBConnector
inDBConnector = DBConnector()

def calSimG(uList):
    for user in uList:
        query = ('INSERT INTO sim_g_u '
                 'SELECT s1.u1, s1.u2, s1.num / (s2.dem * s3.dem) FROM ('
                 'SELECT t1.userid AS u1, t2.userid AS u2, SUM(t1.rating * t2.rating) AS num '
                 'FROM traindata_g AS t1 JOIN traindata_g AS t2 '
                 'ON t1.userid <> t2.userid AND t1.gameid = t2.gameid AND t1.userid = \'%s\' '
                 'GROUP BY t1.userid, t2.userid) s1 '
                 'JOIN ('
                 'SELECT userid, POWER(SUM(POWER(rating, 2)), 0.5) AS dem FROM traindata_g GROUP BY userid) s2 '
                 'ON s1.u1 = s2.userid '
                 'JOIN ('
                 'SELECT userid, POWER(SUM(POWER(rating, 2)), 0.5) AS dem FROM traindata_g GROUP BY userid) s3 '
                 'ON s1.u2 = s3.userid') % user
        
        inDBConnector.runQuery(query)
        inDBConnector.commit()
        print user
    
def calSimL(uList, db):
    for user in uList:
        query = ('INSERT INTO sim_yep_u '
                 'SELECT s1.u1, s1.u2, s1.tg, s1.num / (s2.dem * s3.dem) FROM ('
                 'SELECT t1.userid AS u1, t2.userid AS u2, t2.tg, SUM(t1.rating * t2.rating) AS num '
                 'FROM %s AS t1 JOIN %s AS t2 '
                 'ON t1.userid <> t2.userid AND t1.gameid = t2.gameid AND t1.userid = \'%s\' AND t1.tg = 3 '
                 'GROUP BY t1.userid, t2.userid, t2.tg) s1 '
                 'JOIN ('
                 'SELECT userid, tg, POWER(SUM(POWER(rating, 2)), 0.5) AS dem FROM %s GROUP BY userid, tg) s2 '
                 'ON s1.u1 = s2.userid AND s2.tg = 3 '
                 'JOIN ('
                 'SELECT userid, tg, POWER(SUM(POWER(rating, 2)), 0.5) AS dem FROM %s GROUP BY userid, tg) s3 '
                 'ON s1.u2 = s3.userid AND s1.tg = s3.tg') % (db, db, user, db, db)

        inDBConnector.runQuery(query)
        inDBConnector.commit()
        print user

query = 'SELECT userid, COUNT(gameid) AS cr FROM date170528 WHERE userid IN (SELECT userid FROM traindata_g) GROUP BY userid ORDER BY cr DESC LIMIT 300'
rList = inDBConnector.runQuery(query)
uList = map(list, zip(*rList))[0]
calSimL(uList, 'traindata_yep')