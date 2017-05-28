# -*- coding: utf-8 -*-
import numpy as np
from DBConnector import DBConnector

# Local
all_user_feature = []
for tg in range(1, 7):
    user_feature = []
    fileName = 'user_feature_tg%d.txt' % tg
    for line in open(fileName):
        uf = map(float, line.strip().split('\t')[1:22])
        user_feature.append(uf)
    all_user_feature.append(np.mat(user_feature))
    
inDBConnector = DBConnector()
insert = ('INSERT INTO sim_as ' 'VALUES (%s, %s, %s, %s)')

for aim in range(1100):
    for tg in range(6):
        for cop in range(10005):
            if aim != cop:
                aim_f = all_user_feature[5][aim, :]
                cop_f = all_user_feature[tg][cop, :]
                la = float(np.sqrt(aim_f.dot(aim_f.T)))
                lc = float(np.sqrt(cop_f.dot(cop_f.T)))
                sim = float(aim_f.dot(cop_f.T)) / (la * lc)
                print aim + 1, cop + 1, tg + 1
                inDBConnector.runInsert(insert, (aim + 1, cop + 1, tg + 1, sim))
    inDBConnector.commit()