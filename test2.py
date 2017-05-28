import random  
import math
from DBConnector import DBConnector

inDBConnector = DBConnector()
query = 'SELECT aid, LOG(pc+1) FROM testdata WHERE uid = %d' % 1
rd = dict(inDBConnector.runQuery(query))
res = [[65152, 1.0], [0, 2.0]]

topn = sorted(res, key=lambda l:l[1], reverse=True)[0:2]
print topn
print rd.has_key(topn[1][0])
