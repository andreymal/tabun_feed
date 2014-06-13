#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import time
import tabun_api as api

tabun_feed = None
user = None

last_act = None

def load_activity(data=None):
    global last_act
    full = False
    new = []
    last_id = None
    old_last_id = tabun_feed.get_db_last('activity_id')
    old_tic = tabun_feed.console.get('get_tic')[0]
    
    i = 0
    while not full:
        i += 1
        tabun_feed.console.set('get_tic', ' a:'+str(i))
        new_last_id, acts = user.get_more_activity(last_id) if last_id is not None and user.username else user.get_activity()
        if last_id is None:
            last_id = new_last_id
            tabun_feed.set_db_last('activity_id', last_id)
        else:
            time.sleep(2)
        
        if old_last_id == 0:
            old_last_id = last_id

        if not user.username: full = True

        for act in acts:
            if act == last_act or (act.id is not None and act.id <= old_last_id):
                full = True
                break
            new.insert(0, act)

    tabun_feed.console.set('get_tic', old_tic)

    if new:
        last_act = new[-1]
        tabun_feed.call_handlers("activity", new)

def init_tabun_plugin(tf):
    global tabun_feed
    tabun_feed = tf
    tabun_feed.add_handler("set_user", set_user)

def set_user(u,a):
    global user
    user = u
    tabun_feed.add_handler("post_load", load_activity)