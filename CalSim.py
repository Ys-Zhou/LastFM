# -*- coding: utf-8 -*-
from DBConnector import DBConnector

inDBConnector = DBConnector()

def cal_sim_total(usera):
    query = (
             "INSERT INTO sim_total(usera, userb, sim) "  # insert table
             "SELECT s2.uname, s3.uname, s1.fz / s2.fm / s3.fm "
             "FROM("
             "SELECT n1.uname AS un1, n2.uname AS un2, SUM(LOG(n1.total+1)*LOG(n2.total+1)) AS fz "  # cal fz
             "FROM norm_as AS n1, norm_as AS n2 "  # from table x2
             "WHERE n1.aname = n2.aname AND n1.uname = \"%s\" AND n1.uname <> n2.uname "
             "GROUP BY un1, un2"
             ") s1 "
             "JOIN("
             "SELECT uname, POWER(SUM(POWER(LOG(total+1),2)),0.5) AS fm "  # cal fm
             "FROM norm_as "  # from table
             "GROUP BY uname"
             ") s2 "
             "ON s1.un1 = s2.uname "
             "JOIN("
             "SELECT uname, POWER(SUM(POWER(LOG(total+1),2)),0.5) AS fm "  # cal fm
             "FROM norm_as "  # from table
             "GROUP BY uname"
             ") s3 "
             "ON s1.un2 = s3.uname"
             ) % (usera)

    inDBConnector.runQuery(query)

def cal_sim_jaccard(usera):
    query = (
             "INSERT INTO sim_jaccard(usera, userb, sim) "  # insert table
             "SELECT s2.uname, s3.uname, s1.fz / (s2.fm + s3.fm - s1.fz) "
             "FROM("
             "SELECT n1.uname AS un1, n2.uname AS un2, COUNT(n1.aname) AS fz "  # cal fz
             "FROM norm_as AS n1, norm_as AS n2 "  # from table x2
             "WHERE n1.aname = n2.aname AND n1.uname = \"%s\" AND n1.uname <> n2.uname "
             "GROUP BY un1, un2"
             ") s1 "
             "JOIN("
             "SELECT uname, COUNT(aname) AS fm "  # cal fm
             "FROM norm_as "  # from table
             "GROUP BY uname"
             ") s2 "
             "ON s1.un1 = s2.uname "
             "JOIN("
             "SELECT uname, COUNT(aname) AS fm "  # cal fm
             "FROM norm_as "  # from table
             "GROUP BY uname"
             ") s3 "
             "ON s1.un2 = s3.uname"
             ) % (usera)

    inDBConnector.runQuery(query)

def cal_sim_group(usera):
    for tg in range(1, 7):
        query = (
                 "INSERT INTO sim_tg_as(usera, userb, tgb, sim) "  # insert table
                 "SELECT s2.uname, s3.uname, %d, s1.fz / s2.fm / s3.fm "
                 "FROM("
                 "SELECT n1.uname AS un1, n2.uname AS un2, SUM(LOG(n1.total+1)*n1.pro*LOG(n2.total+1)*n2.pro) AS fz "  # cal fz
                 "FROM norm_as_tg6 AS n1, norm_as_tg%d AS n2 "  # from table x2
                 "WHERE n1.aname = n2.aname AND n1.uname = \"%s\" AND n1.uname <> n2.uname "
                 "GROUP BY un1, un2"
                 ") s1 "
                 "JOIN("
                 "SELECT uname, POWER(SUM(POWER(LOG(total+1)*pro,2)),0.5) AS fm "  # cal fm
                 "FROM norm_as_tg6 "  # from table
                 "GROUP BY uname"
                 ") s2 "
                 "ON s1.un1 = s2.uname "
                 "JOIN("
                 "SELECT uname, POWER(SUM(POWER(LOG(total+1)*pro,2)),0.5) AS fm "  # cal fm
                 "FROM norm_as_tg%d "  # from table
                 "GROUP BY uname"
                 ") s3 "
                 "ON s1.un2 = s3.uname"
                 ) % (tg, tg, usera, tg)
    
        inDBConnector.runQuery(query)

query = 'SELECT uname FROM user WHERE uname <= \'Biudi\''
result_list = inDBConnector.runQuery(query)

index = 0
for usera in result_list:
    index += 1
    print index, usera[0],
    cal_sim_group(usera[0])
    inDBConnector.commit()
    print 'done'