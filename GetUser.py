# -*- coding: utf-8 -*-
import urllib2
from bs4 import BeautifulSoup
from DBConnector import DBConnector

switch = False

def getNeighbours(userName):
    userSet = set()
    try:
        url = 'http://www.last.fm/zh/user/%s/neighbours' % userName
        page = urllib2.urlopen(url).read()
        soup = BeautifulSoup(page, 'html.parser')
        neighbourSet = soup.find('ul', class_='user-list')
        for user in neighbourSet.find_all('a', class_=' user-list-link link-block-target '):
            userSet.add(user.string.encode('UTF-8'))
    except Exception:
        print soup
    return userSet

uid = 1
inDBConnector = DBConnector()

def store(userName):
    global uid
    insert = ('INSERT INTO user (id, name)' 'VALUES (%s, %s)')
    data = (uid, userName)
    inDBConnector.runInsert(insert, data)
    uid += 1

if switch:
    nodes = set(['eartle'])
    old = set(['eartle'])
    new = set()
    
    store('eartle')
    while len(old) <= 20000:
        for node in nodes:
            new |= getNeighbours(node)
            if len(new) + len(old) >= 20000:
                break
            print len(new) + len(old)
        nodes = new - old
        for node in nodes:
            store(node)
        old |= nodes
        new.clear()