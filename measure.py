# -*- coding: utf-8 -*-
import math
from DBConnector import DBConnector
from _ctypes import set_last_error
inDBConnector = DBConnector()

def formatTest():
    for uid in range(1, 1101):
        print uid
        query = 'INSERT INTO testdata1 SELECT users.uid, artists.aid, testdata.pc FROM testdata JOIN users ON testdata.uname = users.uname AND users.uid = %d JOIN artists ON testdata.aname = artists.aname' % uid
        inDBConnector.runQuery(query)
        inDBConnector.commit()

def getTopN(uid, limit, held=False):
    query = ''
    if held:
        query = ('SELECT n.aid, SUM(s.sim*n.pro*LOG(n.total+1)) AS pri '
                 'FROM (SELECT uid2, tg, sim FROM sim_as WHERE uid1 = %d ORDER BY sim DESC LIMIT 600) AS s JOIN norm_as AS n ON s.uid2 = n.uid AND s.tg = n.tg '
                 'GROUP BY n.aid ORDER BY pri DESC LIMIT %d') % (uid, limit)
    else:
        query = ('SELECT n.aid, SUM(s.sim*n.pro*LOG(n.total+1)) AS pri '
                 'FROM (SELECT uid2, tg, sim FROM sim_as WHERE uid1 = %d ORDER BY sim DESC LIMIT 600) AS s JOIN norm_as AS n ON s.uid2 = n.uid AND s.tg = n.tg '
                 'AND NOT EXISTS (SELECT * FROM norm_as AS e WHERE e.uid = %d AND e.tg = 1 AND e.aid = n.aid) '
                 'GROUP BY n.aid ORDER BY pri DESC LIMIT %d') % (uid, uid, limit)
    rl = inDBConnector.runQuery(query)
    return map(list, zip(*rl))[0]

def getTopN_g(uid, limit, held=False):
    query = ''
    if held:
        query = ('SELECT n.aid, SUM(s.sim*LOG(n.total+1)) AS pri '
                 'FROM (SELECT uid2, sim FROM sim_global WHERE uid1 = %d ORDER BY sim DESC LIMIT 100) AS s JOIN norm_as AS n ON s.uid2 = n.uid AND n.tg = 1 '
                 'GROUP BY n.aid ORDER BY pri DESC LIMIT %d') % (uid, limit)
    else:
        query = ('SELECT n.aid, SUM(s.sim*LOG(n.total+1)) AS pri '
                 'FROM (SELECT uid2, sim FROM sim_global WHERE uid1 = %d ORDER BY sim DESC LIMIT 100) AS s JOIN norm_as AS n ON s.uid2 = n.uid AND n.tg = 1 '
                 'AND NOT EXISTS (SELECT * FROM norm_as AS e WHERE e.uid = %d AND e.tg = 1 AND e.aid = n.aid) '
                 'GROUP BY n.aid ORDER BY pri DESC LIMIT %d') % (uid, uid, limit)
    rl = inDBConnector.runQuery(query)
    return map(list, zip(*rl))[0]

def getCorrect(uid):
    query = 'SELECT aid, LOG(pc+1) FROM testdata WHERE uid = %d' % uid
    rl = inDBConnector.runQuery(query)
    return dict(rl)

whn_l = 0.0
whn_g = 0.0
set_l = set()
set_g = set()
limit = 40
for uid in range(1, 300 + 1):
    cd = getCorrect(uid)
    pr_l = getTopN(uid, 40)
    pr_g = getTopN_g(uid, 40)

    for i in range(limit):
        set_l.add(pr_l[i])
        set_g.add(pr_g[i])
        if cd.has_key(pr_l[i]):
            whn_l += cd[pr_l[i]] / (math.log(i + 1) + 1)
        if cd.has_key(pr_g[i]):
            whn_g += cd[pr_g[i]] / (math.log(i + 1) + 1)

    print uid, len(set_l), len(set_g)
    print uid, whn_l, whn_g