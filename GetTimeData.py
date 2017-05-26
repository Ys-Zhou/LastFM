# -*- coding: utf-8 -*-
import urllib2
import xml.etree.cElementTree as ET
from DBConnector import DBConnector

switch = True

key = '34d4248c173dd4e7efa3c5d2a1d52be6'
inDBConnector = DBConnector()
res = ''

def setRes(url, times):
    global res
    try:
        req = urllib2.Request(url)
        res = urllib2.urlopen(req)
    except Exception, e:
        if times < 5:
            print 'Try again'
            setRes(url, times + 1)
        else:
            print e
 
def getWeeklyArtistChart(uname, start, end):
    insert = ('INSERT INTO data' + str(start) + ' (uname, aname, pc)' 'VALUES (%s, %s, %s)')
    url = 'http://ws.audioscrobbler.com/2.0/?method=user.getweeklyartistchart&user=%s&from=%d&to=%d&api_key=%s' % (uname, start, end, key)
    
    setRes(url, 0)
    if res.code == 200:
        tree = ET.parse(res)
        root = tree.getroot()
        for artist in root.find('weeklyartistchart').findall('artist'):
            aname = artist.find('name').text
            pc = artist.find('playcount').text
            data = (uname, aname, pc)
            try:
                inDBConnector.runInsert(insert, data)
            except Exception, e:
                print e
    else:
        print 'HTTP error'

if switch:
    basic = 1492948800
    pace = 604800
    # one week[1]
    for i in range(-3, -1):
        start = basic - pace * i
        end = start + pace
        
        try:
            query = ('CREATE TABLE `data%s` ('
                     '`uname` varchar(45) NOT NULL,'
                     '`aname` varchar(200) NOT NULL,'
                     '`pc` int(11) NOT NULL,'
                     'PRIMARY KEY (`uname`,`aname`)'
                     ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;') % start
            result_list = inDBConnector.runQuery(query)
        except Exception:
            print "Table exists"
    
        query = 'SELECT uname FROM user'
        result_list = inDBConnector.runQuery(query)
        for line in result_list:
            getWeeklyArtistChart(line[0], start, end)
            print i,
            print line[0]
        inDBConnector.commit()