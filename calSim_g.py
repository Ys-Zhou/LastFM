# -*- coding: utf-8 -*-
import numpy as np
from DBConnector import DBConnector

# Global
inDBConnector = DBConnector()
query = 'SELECT userid, COUNT(gameid) AS cr FROM date170528 WHERE userid IN (SELECT userid FROM traindata_g) GROUP BY userid ORDER BY cr DESC LIMIT 300'
rList = inDBConnector.runQuery(query)
test_user = map(list, zip(*rList))[0]

user_list = []
user_featureL = []

fileName = 'user_feature_global.txt'
for line in open(fileName):
    line_trump = line.strip().split('\t')
    user_list.append(line_trump[0])
    user_featureL.append(map(float, line_trump[1:22]))
user_feature = np.mat(user_featureL)

insert = ('INSERT INTO sim_g ' 'VALUES (%s, %s, %s)')

for aim in range(len(user_list)):
    if user_list[aim] in test_user:
        for cop in range(len(user_list)):
            if aim != cop:
                aim_f = user_feature[aim, :]
                cop_f = user_feature[cop, :]
                la = float(np.sqrt(aim_f.dot(aim_f.T)))
                lc = float(np.sqrt(cop_f.dot(cop_f.T)))
                sim = float(aim_f.dot(cop_f.T)) / (la * lc)
                inDBConnector.runInsert(insert, (user_list[aim], user_list[cop], sim))
                print aim, cop
        inDBConnector.commit()