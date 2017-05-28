# -*- coding: utf-8 -*-
import numpy as np
from DBConnector import DBConnector

# Global
user_featureL = []
fileName = 'user_feature_global.txt'
for line in open(fileName):
    uf = map(float, line.strip().split('\t')[1:22])
    user_featureL.append(uf)
    user_feature = np.mat(user_featureL)

inDBConnector = DBConnector()
insert = ('INSERT INTO sim_global ' 'VALUES (%s, %s, %s)')

for aim in range(300):
    for cop in range(10005):
        if aim != cop:
            aim_f = user_feature[aim, :]
            cop_f = user_feature[cop, :]
            la = float(np.sqrt(aim_f.dot(aim_f.T)))
            lc = float(np.sqrt(cop_f.dot(cop_f.T)))
            sim = float(aim_f.dot(cop_f.T)) / (la * lc)
            print aim + 1, cop + 1
            inDBConnector.runInsert(insert, (aim + 1, cop + 1, sim))
    inDBConnector.commit()