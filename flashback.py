#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import sqlite3
import sys
try: num = int(sys.argv[-1])
except: num = 4
if num < 0: val = "+" + str(-num)
else: val = '-' + str(num)
print val
db=sqlite3.connect("tabun_feed.db")
db.execute("update lasts set value=value"+val+" where type='time'")
db.commit()
