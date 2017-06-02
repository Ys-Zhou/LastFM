# -*- coding: utf-8 -*-
import math
from DBConnector import DBConnector

inDBConnector = DBConnector()

switch = True
k = 3  # 目标列数

def listadd(listA, listB, num):
    res = [0] * num
    for n in range(len(listA)):
        res[n] = listA[n] + listB[n]
    return res

def listpro(listA, pro, num):
    res = [0] * num
    for n in range(len(listA)):
        res[n] = listA[n] * pro
    return res

def avg(uname):
    query = ("SELECT c1, c2, c3 "
             "FROM traindata_full WHERE userid = \"%s\"") % uname
    result_list = inDBConnector.runQuery(query)
    table = map(list, zip(*result_list))

    query = "SELECT gameid FROM traindata_full WHERE userid = \"%s\"" % uname
    result_list = inDBConnector.runQuery(query)    
    lent = len(result_list)

    a = 0
    for line in table:
        a += sum(line)
    groupTime = float(a) / k

    remind = [0] * lent
    aim = []
    for line in table:
        new = line
        while sum(remind) + sum(new) > groupTime * 0.99999:
            proportion = (groupTime - sum(remind)) / sum(new)
            aim.append(listadd(remind, listpro(new, proportion, lent), lent))
            remind = [0] * lent
            new = listpro(new, 1 - proportion, lent)
        remind = listadd(remind, new, lent)

    for index in range(lent):
        r1 = (aim[0][index] + 0.5 * aim[1][index] + 0.25 * aim[2][index]) / 1.75
        r2 = (0.5 * aim[0][index] + aim[1][index] + 0.5 * aim[2][index]) / 2
        r3 = (0.25 * aim[0][index] + 0.5 * aim[1][index] + aim[2][index]) / 1.75
#         r1 = aim[0][index]
#         r2 = aim[1][index]
#         r3 = aim[2][index]

        insert = ('INSERT INTO traindata_yep (userid, gameid, tg, rating)' 'VALUES (%s, %s, %s, %s)')
        data = (uname, result_list[index][0], 1, math.log(r1 + 1))
        inDBConnector.runInsert(insert, data)
        data = (uname, result_list[index][0], 2, math.log(r2 + 1))
        inDBConnector.runInsert(insert, data)
        data = (uname, result_list[index][0], 3, math.log(r3 + 1))
        inDBConnector.runInsert(insert, data)

if switch:
    query = 'SELECT DISTINCT userid FROM traindata_full'
    result_list = inDBConnector.runQuery(query)
    count = 1
    for item in result_list:
        avg(item[0])
        print count, item[0]
        count += 1
