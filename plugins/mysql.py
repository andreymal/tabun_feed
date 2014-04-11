#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import MySQLdb
from threading import Lock, RLock

db = None
tabun_feed = None

class DB:
    def __init__(self, user, password, database):
        self.user = str(user)
        self.password = str(password)
        self.database = str(database)
        self.db_conn = None
        self.lock = RLock()
        self.counter_lock = Lock()
        self.counter = None
        
        if __debug__:
            self.debug = False
        
        self.connect()

    def counter_start(self, trace=False):
        self.counter_lock.acquire()
        self.counter = [] if trace else 0
    
    def counter_end(self):
        n = self.counter
        self.counter = None
        self.counter_lock.release()
        return n
            
    def execute(self, s, args=[]):
        if not isinstance(s,(str,unicode)):raise DbExc("Query is not string")
        if __debug__:
            if self.debug:
                try: tabun_feed.console.stdprint('  MySQL QUERY:',s)
                except: tabun_feed.console.srdprint('  MySQL QUERY.')
        
        with self.lock:
            cursor = self.db_conn.cursor()
            cursor.execute(s, args)
            if self.counter is not None:
                if isinstance(self.counter, list):
                    self.counter.append(cursor._last_executed)
                else: self.counter += 1
            return cursor

    def execute_in(self, s, in_args, args=[], binary=False):
        # select * from sometable where somecolumn = %s and somecolumn2 in (%s)
        if not isinstance(s,(str,unicode)):raise DbExc("Query is not string")
        if isinstance(s, unicode): s = s.encode('utf-8')
        s = s.replace( "(%s)", "("+("binary " if binary else "") + (", "+"binary " if binary else ", ").join(map(e, in_args)) + ")" )
        return self.execute(s, args)
            

    def connect(self):
        with self.lock:
            self.db_conn = MySQLdb.connect("localhost", self.user, self.password, self.database, charset='utf8')
            self.db_conn.ping(True)
            self.execute('set autocommit=1')

    def disconnect(self):
        with self.lock:
            self.db_conn.close()

def connect():
    c = tabun_feed.config
    return DB(c.get("mysql_username"), c.get("mysql_password"), c.get("mysql_database"))

def init_tabun_plugin(tf):
    global db, tabun_feed
    tabun_feed = tf
    try:
        db = connect()
    except Exception as exc:
        tf.console.stdprint("Cannot connect to mysql:", exc)