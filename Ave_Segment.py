# -*- coding: utf-8 -*-
from DBConnector import DBConnector

inDBConnector = DBConnector()

switch = False
k = 6  # 目标列数

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
    query = ("SELECT w1, w2, w3, w4, w5, w6, w7, w8, w9, w10, w11, w12, w13, w14, w15, w16, w17, w18, w19, w20, w21, w22, w23, w24 "
             "FROM rawdata_nonoise WHERE uname = \"%s\"") % uname
    result_list = inDBConnector.runQuery(query)
    table = map(list, zip(*result_list))

    query = "SELECT aname FROM rawdata_nonoise WHERE uname = \"%s\"" % uname
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
        insert = ('INSERT INTO pretreat_as (uname, aname, tg1, tg2, tg3, tg4, tg5, tg6)' 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)')
        data = (uname, result_list[index][0], aim[0][index], aim[1][index], aim[2][index], aim[3][index], aim[4][index], aim[5][index])
        inDBConnector.runInsert(insert, data)

if switch:
    query = 'SELECT uname FROM user'
    result_list = inDBConnector.runQuery(query)
    count = 1
    for item in result_list:
        avg(item[0])
        print count, item[0]
        count += 1