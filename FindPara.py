# -*- coding: utf-8 -*-
import math
from DBConnector import DBConnector

inDBConnector = DBConnector()

query = ('SELECT users.uid, artists.aid, p.tg1, p.tg2, p.tg3, p.tg4, p.tg5, p.tg6 FROM pretreat_as AS p '
         'JOIN users FORCE INDEX(idx_users_uid) ON p.uname = users.uname '
         'JOIN artists ON p.aname = artists.aname ORDER BY users.uid')
r_list = inDBConnector.runQuery(query)

for item in r_list:
    total = float(item[2]) + float(item[3]) + float(item[4]) + float(item[5]) + float(item[6]) + float(item[7])
    q_exp = 0.0
    exp = 0.0
    for i in range(2, 8):
        q_exp += (i - 1) ** 2 * float(item[i]) / total
        exp += (i - 1) * float(item[i]) / total
    var = q_exp - exp ** 2
    
    p = [0, 0, 0, 0, 0, 0, 0]
    tg = [0, 0, 0, 0, 0, 0, 0]
    
    if var > 0.01:
        p[1] = math.exp(-(1.0 - exp) ** 2 / (2.0 * var ** 2))
        p[2] = math.exp(-(2.0 - exp) ** 2 / (2.0 * var ** 2))
        p[3] = math.exp(-(3.0 - exp) ** 2 / (2.0 * var ** 2))
        p[4] = math.exp(-(4.0 - exp) ** 2 / (2.0 * var ** 2))
        p[5] = math.exp(-(5.0 - exp) ** 2 / (2.0 * var ** 2))
        p[6] = math.exp(-(6.0 - exp) ** 2 / (2.0 * var ** 2))
        p_all = p[1] + p[2] + p[3] + p[4] + p[5] + p[6]
    
        tg[1] = 0.05 + 0.7 * p[1] / p_all
        tg[2] = 0.05 + 0.7 * p[2] / p_all
        tg[3] = 0.05 + 0.7 * p[3] / p_all
        tg[4] = 0.05 + 0.7 * p[4] / p_all
        tg[5] = 0.05 + 0.7 * p[5] / p_all
        tg[6] = 0.05 + 0.7 * p[6] / p_all
        
    else:
        exp = int(exp + 0.01)
        for i in range(1, 7):
            tg[i] = 0.05
        tg[exp] = 0.75
    
    for tg in range(1, 7):
        insert = ('INSERT INTO norm_as ' 'VALUES (%s, %s, %s, %s, %s)')
        data = (item[0], item[1], i, tg[i], total)
        inDBConnector.runInsert(insert, data)
    
    print item[0]