# -*- coding: utf-8 -*-
from DBConnector import DBConnector

inDBConnector = DBConnector()
query = 'SELECT users.uid, artists.aid, LOG(norm_as.total+1) FROM norm_as join users ON norm_as.uname = users.uname join artists on norm_as.aname = artists.aname'
r_list = inDBConnector.runQuery(query)

f = open('rawdata.txt', 'w+')
for item in r_list:
    print item[0],
    f.write('%s\t%s\t%s\n' % (item[0], item[1], item[2])) 
    print 'down'
f.close()