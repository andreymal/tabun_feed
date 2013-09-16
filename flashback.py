#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import sqlite3
db=sqlite3.connect("tabun_feed.db")
db.execute("update lasts set value=value-4 where type='time'")
db.commit()
