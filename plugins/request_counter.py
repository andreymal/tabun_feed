#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import time
import tabun_api as api

tabun_feed = None
default_urlopen = None

last = {60: [], 300: [], 600: []}

def urlopen(self, *args, **kwargs):
    try:
        tm = int(time.time())
        for key, value in last.items():
            value.append(tm)
            last[key] = list(x for x in value if tm-x <= key)
        tabun_feed.console.set( "req_stat", ",".join( map(lambda x:str(len(x[1])), sorted(last.items())) ) )
    except:
        import traceback
        traceback.print_exc()
    return default_urlopen(self, *args, **kwargs)

def init_tabun_plugin(tf):
    global tabun_feed, default_urlopen
    tabun_feed = tf
    default_urlopen = api.User.urlopen
    api.User.urlopen = urlopen
