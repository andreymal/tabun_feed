#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import time
import json
import urllib2
import tabun_api as api
import lxml.etree

je = json.JSONEncoder(ensure_ascii=False)

tabun_feed = None
console = None
user = None
db = None
c = None # config

def post2json(post):
    post_dict = {
        'time': time.mktime(post.time),
        'blog': post.blog,
        'post_id': post.post_id,
        'author': post.author,
        'title': post.title,
        'body': lxml.etree.tostring(post.body, method="html", encoding="utf-8").decode("utf-8", "replace"),
        'tags': post.tags,
        'blog_name': post.blog_name
    }
    
    return je.encode(post_dict)

def init_db():
    global db
    tables = db.execute("show tables").fetchall()
    tables = map(lambda x:x[0], tables)
    
    if not u"tabun_backup" in tables:
        db.execute("create table tabun_backup(post_id int primary key, time int, post mediumtext) engine innodb character set utf8")

def backup_post(post, full_post=None):
    if full_post: post = full_post
    if post.private and not post.blog in api.halfclosed: return
    db.execute("replace into tabun_backup values(%s, %s, %s)", (post.post_id, time.mktime(post.time), post2json(post)) )
    
    #disabled in robots.txt
    try:urllib2.urlopen("http://web.archive.org/save/http://tabun.everypony.ru/blog/" + ((post.blog+"/") if post.blog else "") + str(post.post_id) + ".html").read()
    except IOError as e: console.stdprint("Cannot web.archive.org", post.post_id, e)
    
def mysql_connect():
    global db
    db = tabun_feed.require("mysql").db

def init_tabun_plugin(tf):
    global tabun_feed, db, console,  c
    tabun_feed = tf
    console = tabun_feed.console
    c = tabun_feed.config
    try:
        mysql_connect()
    except:
        console.stdprint("Cannot connect to mysql, backuper is disabled")
        return
    init_db()

    tabun_feed.request_full_posts()
    tabun_feed.add_handler("set_user", start)
    tabun_feed.add_handler("post", backup_post, priority=2)

def start(user, anon):
    globals()['user'] = user
    if not db.execute("select * from tabun_backup limit 0,1"):
        console.stdprint("Backuping...")
        posts = user.get_posts("/rss/new/")
        
        for post in posts:
            post = user.get_post(post.post_id, post.blog)
            backup_post(post, post)
            console.stdprint(post.post_id)
        console.stdprint("Backuped.")
