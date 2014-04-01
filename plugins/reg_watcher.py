#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import time

tabun_feed = None
last_list = None
anon = None
db = None
i = 14

def load(urls):
    global i, last_list
    if not anon: return
    i += 1
    if i < 15: return
    i = 0
    peoples = anon.get_people_list(url='/people/new/')
    peoples.reverse()
    for people in peoples:
        if not people.username in last_list:
            ppl = anon.get_profile(people.username)
            console.stdprint(time.strftime("%H:%M:%S", ppl.registered), "New user", people.username)
            last_list.append(people.username)
            last_list = last_list[-50:]
            db.execute("replace into tabun_people values(%s, %s, %s, %s)", (ppl.username, time.mktime(ppl.registered),
                time.mktime(ppl.birthday) if ppl.birthday else None, time.strftime('%d.%m.%Y', ppl.birthday) if ppl.birthday else None,))
    
def mysql_connect():
    global db
    db = tabun_feed.require("mysql").db

def init_db():
    global db
    tables = db.execute("show tables").fetchall()
    tables = map(lambda x:x[0], tables)
    
    if not u"tabun_people" in tables:
        db.execute("create table tabun_people(username char(128) primary key, time int, birthday int default NULL sbd char(48) default NULL, key(time), key(sbd)) engine innodb character set utf8")

def init_tabun_plugin(tf):
    global tabun_feed, db, console, db_conn, last_list
    tabun_feed = tf
    console = tabun_feed.console
    try:
        mysql_connect()
    except:
        console.stdprint("Cannot connect to mysql, regwatcher is disabled")
        return
    init_db()
    
    last_list = map(lambda x:str(x[0]), db.execute("select username from tabun_people order by time desc limit 0,50").fetchall())
    
    tabun_feed.add_handler("load", load)
    tabun_feed.add_handler("set_user", start)

def start(user, anon):
    globals()['anon'] = anon
