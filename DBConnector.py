# -*- coding: utf-8 -*-
import mysql.connector

class DBConnector:

    def __init__(self):
        user = "root"
        password = "zhou"
        host = "127.0.0.1"
        database = "steam"
        self.__cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)

    def __del__(self):
        self.__cnx.commit()
        self.__cnx.close()

    def runInsert(self, insert, data):
        cursor = self.__cnx.cursor(buffered=True)
        cursor.execute(insert, data)
        cursor.close()
        
    def runQuery(self, query):
        cursor = self.__cnx.cursor(buffered=True)
        cursor.execute(query)
        result_list = list(cursor)
        cursor.close()
        return result_list
    
    def runQueryWithPara(self, query, data):
        cursor = self.__cnx.cursor(buffered=True)
        cursor.execute(query, data)
        result_list = list(cursor)
        cursor.close()
        return result_list
    
    def commit(self):
        self.__cnx.commit()