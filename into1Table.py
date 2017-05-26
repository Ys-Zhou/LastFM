# -*- coding: utf-8 -*-
from DBConnector import DBConnector

inDBConnector = DBConnector()

for i in range(2, 25):
    start = 1478433600 + 604800 * (i - 1)
    query = 'SELECT uname, aname, pc FROM data%d' % start
    r_list = inDBConnector.runQuery(query)
    
    cl = 'w%d' % i
    for item in r_list:
        update = 'UPDATE rawdata SET ' + cl + ' = %s WHERE uname = %s AND aname = %s'
        data = (item[2], item[0], item[1])
        inDBConnector.runInsert(update, data)
        print i, item[0]
    inDBConnector.commit()