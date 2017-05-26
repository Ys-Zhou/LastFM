# -*- coding: utf-8 -*-
from DBConnector import DBConnector
inDBConnector = DBConnector()

def result_g(username, alp, glonum):
    query = (
             "SELECT n.aname, SUM(u.sim * LOG(n.total+1)) AS pri "
             "FROM norm_as_tg1 AS n "
             "JOIN (SELECT st.userb, %f * st.sim + (1 - %f) * sa.sim AS sim "
             "FROM sim_total AS st JOIN sim_var_as AS sa "
             "ON st.usera = \'%s\' AND st.usera = sa.usera AND st.userb = sa.userb "
             "ORDER BY sim DESC LIMIT 100) u "
             "ON n.uname = u.userb "
             "AND NOT EXISTS (SELECT p.aname FROM norm_as_tg1 AS p WHERE p.uname = \'%s\' AND n.aname = p.aname) "
             "GROUP BY n.aname ORDER BY pri DESC LIMIT %d"
             ) % (alp, alp, username, username, glonum)
    
    rl = inDBConnector.runQuery(query)
    if rl:
        return map(list, zip(*rl))[0]
    else :
        return rl

def result_l(username, locnum):  
    query = (
            'SELECT n.aname, SUM(u.sim * n.pro * LOG(n.total + 1)) AS pri '
            'FROM norm_as_all AS n JOIN ('
            'SELECT userb, tgb, sim FROM sim_tg_as WHERE usera=\'%s\' ORDER BY sim DESC LIMIT 600) u '
            'ON n.uname = u.userb AND n.tg = u.tgb '
            'AND NOT EXISTS (SELECT p.aname FROM norm_as_tg1 AS p WHERE p.uname = \'%s\' AND n.aname = p.aname)'
            'GROUP BY aname ORDER BY pri DESC LIMIT %d'
            ) % (username, username, locnum)
    
    rl = inDBConnector.runQuery(query)
    if rl:
        return map(list, zip(*rl))[0]
    else :
        return rl

def correct(username):
    query = (
             'SELECT aname, LOG(pc+1) FROM('
             'SELECT * FROM testdata1492948800 WHERE uname = \'%s\' UNION ALL '
             'SELECT * FROM testdata1493553600 WHERE uname = \'%s\' UNION ALL '
             'SELECT * FROM testdata1494158400 WHERE uname = \'%s\' UNION ALL '
             'SELECT * FROM testdata1494763200 WHERE uname = \'%s\') temp'
             ) % (username, username)
    rl = inDBConnector.runQuery(query)
    return dict(rl)
        
def getTestUser():
    query = (
             'SELECT uname FROM user LIMIT 1000'
             )
    return inDBConnector.runQuery(query)

def check():
    index = 0
    query = ('INSERT INTO result_u1000_n100_k20(uname, glo, loc)' 'VALUES (%s, %s, %s)')
    for item in getTestUser():
        pc_sum0 = 0
        pc_sum1 = 0
        index += 1
        print index, item[0],
        co = correct(item[0])
        result = set(result_g(item[0], 0.66, 20))
        result_n = result_l(item[0], 20)

#         i = 0
#         while len(result) < 40:
#             result.add(result_n[i])
#             i += 1

        for aname in result:
            if co.has_key(aname):
                pc_sum0 += float(co[aname])

        for aname in result_n:
            if co.has_key(aname):
                pc_sum1 += float(co[aname])
                
        inDBConnector.runInsert(query, (item[0], pc_sum0, pc_sum1))
        inDBConnector.commit()
check()