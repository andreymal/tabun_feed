#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import sys
import traceback

tabun_feed = None

def init_tabun_plugin(tf):
    global tabun_feed
    tabun_feed = tf
    tabun_feed.add_handler("set_user", start)

def start(user, anon):
    posts = []
    for arg in sys.argv[1:]:
        if arg.startswith('-p=') or arg.startswith('--posts='):
            posts.extend(map(int, arg[arg.find("=")+1:].split(",")))
    
    posts.sort()
    for post_id in posts:
        try: post = user.get_post(post_id)
        except: traceback.print_exc(); continue
        tabun_feed.call_handlers("post", post, post)
