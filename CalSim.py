# -*- coding: utf-8 -*-
import numpy as np
from DBConnector import DBConnector

# Local
inDBConnector = DBConnector()
query = 'SELECT userid, COUNT(gameid) AS cr FROM date170528 WHERE userid IN (SELECT userid FROM traindata_g) GROUP BY userid ORDER BY cr DESC LIMIT 300'
rList = inDBConnector.runQuery(query)
test_user = map(list, zip(*rList))[0]

user_list = []
all_user_feature = []
for tg in range(1, 4):
    user_feature = []
    fileName = 'user_feature_tg%d_yep.txt' % tg
    for line in open(fileName):
        line_trump = line.strip().split('\t')
        if tg == 1:    
            user_list.append(line_trump[0])
        user_feature.append(map(float, line_trump[1:22]))
    all_user_feature.append(np.mat(user_feature))
    
insert = ('INSERT INTO sim_yep ' 'VALUES (%s, %s, %s, %s)')

for aim in range(len(user_list)):
    if user_list[aim] in test_user:
        for tg in range(3):
            for cop in range(len(user_list)):
                if aim != cop:
                    aim_f = all_user_feature[2][aim, :]
                    cop_f = all_user_feature[tg][cop, :]
                    la = float(np.sqrt(aim_f.dot(aim_f.T)))
                    lc = float(np.sqrt(cop_f.dot(cop_f.T)))
                    sim = float(aim_f.dot(cop_f.T)) / (la * lc)
                    print aim, cop, tg + 1
                    inDBConnector.runInsert(insert, (user_list[aim], user_list[cop], tg + 1, sim))
        inDBConnector.commit()