# -*- coding: utf-8 -*-
import math
from DBConnector import DBConnector
inDBConnector = DBConnector()

def getUsers():
    query = 'SELECT userid, COUNT(gameid) AS cr FROM date170528 WHERE userid IN (SELECT userid FROM traindata_g) GROUP BY userid ORDER BY cr DESC LIMIT 300'
    rList = inDBConnector.runQuery(query)
    return map(list, zip(*rList))[0]

def getTopN(uid, limit, held=False):
    query = ''
    if held:
        query = ''
    else:
        query = ('SELECT t.gameid, SUM(s.sim*t.rating) AS pri '
                 'FROM (SELECT userb, tg, sim FROM sim_yes WHERE usera = %s ORDER BY sim DESC LIMIT 300) AS s JOIN traindata_yes AS t ON s.userb = t.userid AND s.tg = t.tg '
                 'AND NOT EXISTS (SELECT * FROM traindata_g AS e WHERE e.userid = %s AND e.gameid = t.gameid) '
#                  'AND NOT EXISTS (SELECT * FROM pop AS p WHERE p.gameid = t.gameid) '
                 'GROUP BY t.gameid ORDER BY pri DESC LIMIT %d') % (uid, uid, limit)
    rl = inDBConnector.runQuery(query)
    if rl:
        return map(list, zip(*rl))[0]
    else:
        return rl

def getTopN_g(uid, limit, held=False):
    query = ''
    if held:
        query = ('')
    else:
        query = ('SELECT t.gameid, SUM(s.sim*t.rating) AS pri '
                 'FROM (SELECT userb, sim FROM sim_g WHERE usera = %s ORDER BY sim DESC LIMIT 100) AS s JOIN traindata_g AS t ON s.userb = t.userid '
                 'AND NOT EXISTS (SELECT * FROM traindata_g AS e WHERE e.userid = %s AND e.gameid = t.gameid) '
#                  'AND NOT EXISTS (SELECT * FROM pop AS p WHERE p.gameid = t.gameid) '
                 'GROUP BY t.gameid ORDER BY pri DESC LIMIT %d') % (uid, uid, limit)
    rl = inDBConnector.runQuery(query)
    if rl:
        return map(list, zip(*rl))[0]
    else:
        return rl
    
def getCorrect(uid):
    query = 'SELECT gameid, LOG(twoweeks+1) FROM date170528 WHERE userid = %s' % uid
    rl = inDBConnector.runQuery(query)
    return dict(rl)

# whn_l = 0.0
whn_g = 0.0
limit = 50
userList = getUsers()

for u in range(len(userList)):
    userid = userList[u]
    cd = getCorrect(userid)
    
#     prl = getTopN(userid, limit)
#     for i in range(len(prl)):
#         if cd.has_key(prl[i]):
#             whn_l += cd[prl[i]]
    
    prg = getTopN_g(userid, limit)
    for i in range(len(prg)):
        if cd.has_key(prg[i]):
            whn_g += cd[prg[i]]

    print u, whn_g
