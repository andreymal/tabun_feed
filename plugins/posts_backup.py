#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import time
import json
import MySQLdb
import tabun_api as api
import lxml.etree

je = json.JSONEncoder(ensure_ascii=False)

user = None
db = None
console = None

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
    
    return je.encode(post_dict).encode("utf-8")

def init_db():
    global db
    db.execute("show tables")
    tables = db.fetchall()
    tables = map(lambda x:x[0], tables)
    
    if not u"tabun_backup" in tables:
        db.execute("create table tabun_backup(post_id int primary key, time int, post mediumtext) engine innodb character set utf8")

def backup_post(post, full_post=None):
    if full_post: post = full_post
    if post.private and not post.blog in api.halfclosed and not post.blog in ("ty_nyasha", "NSFW"): return
    db.execute("replace into tabun_backup values(%s, %s, %s)", (post.post_id, time.mktime(post.time), post2json(post)) )
    db.execute("commit")

def init_tabun_plugin(env, register_handler):
    global user, db, console, db_conn
    user = env['user']
    console = env['console']
    c=env['config']
    try:
        db_conn = MySQLdb.connect("localhost", c['mysql_username'], c['mysql_password'], c['mysql_database'])
        db = db_conn.cursor()
    except:
        console.stdprint("Cannot connect to mysql, backuper is disabled")
        return
    init_db()
    
    if not db.execute("select * from tabun_backup limit 0,1"):
        console.stdprint("Backuping...")
        posts = user.get_posts("/rss/new/")
        
        for post in posts:
            post = user.get_post(post.post_id, post.blog)
            backup_post(post, post)
            console.stdprint(post.post_id)
        console.stdprint("Backuped.")
    
    env['request_full_posts']()
    register_handler("post", backup_post, priority=0)
