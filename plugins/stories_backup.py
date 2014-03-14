#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import stories_api as sapi
from threading import RLock, Thread
import MySQLdb
import time
import traceback
import lxml.etree

user = None
db = None
console = None
quit = None
sdb = None

def init_db():
    global db
    db.execute("show tables")
    tables = db.fetchall()
    tables = map(lambda x:x[0], tables)
    
    if not u"stories_main" in tables:
        db.execute("create table stories_main(story_id int primary key, title text, author_id int, upvotes int, downvotes int, description text, key(author_id)) engine innodb character set utf8")
    if not u"stories_categories" in tables:
        db.execute("create table stories_categories(category_id int primary key, name text) engine innodb character set utf8")
    if not u"stories_characters" in tables:
        db.execute("create table stories_characters(character_id int primary key, name text) engine innodb character set utf8")
    if not u"stories_authors" in tables:
        db.execute("create table stories_authors(author_id int primary key, name text) engine innodb character set utf8")
    if not u"stories_chapters" in tables:
        db.execute("create table stories_chapters(story_id int, chapter_id int, name text, body mediumtext, primary key(story_id, chapter_id)) engine innodb character set utf8")

    if not u"stories_text_characters" in tables:
        db.execute("create table stories_text_characters(story_id int, character_id int, key(story_id), primary key(story_id, character_id)) engine innodb character set utf8")
    if not u"stories_text_categories" in tables:
        db.execute("create table stories_text_categories(story_id int, category_id int, key(story_id), primary key(story_id, category_id)) engine innodb character set utf8")

symbols = (':', '.')
symb_tic = 0
last_s = ''

def cset(s=None, req=False):
    global symbols, symb_tic, last_s
    if not s: s = last_s
    else: last_s = s
    s = 's' + (symbols[symb_tic] if req else ' ') + str(s)
    console.set('stories', s)
    if req:
        symb_tic += 1
        if symb_tic >= len(symbols): symb_tic = 0
    else:
        symb_tic = 0

def stories_thread():
    global user, quit, sdb
    last_upd_id = sdb.execute('select value from lasts where type="stories_upd_id"')
    if not last_upd_id: last_upd_id = 0
    else: last_upd_id = last_upd_id[0][0]
    
    last_upd_chapter = sdb.execute('select value from lasts where type="stories_upd_chapter"')
    if not last_upd_chapter: last_upd_chapter = 0
    else: last_upd_chapter = last_upd_chapter[0][0]
    
    db.execute("select max(story_id) from stories_main")
    last_story = db.fetchall()[0][0]
    if not last_story: last_story = 0
    
    console.stdprint("Stories thread started")
    cset(' ')
    errored = False
    err = 0
    while 1:
        try:
            cset('n', req=True)
            stories = user.get_new()
            cset(req=False)
            quit.wait(5)
            if quit.isSet(): break
            cset('u', req=True)
            try: upd_stories = user.get_updates()
            except sapi.StoriesError as exc:
                cset(req=False)
                if not errored: console.stdprint("Stories error:", exc)
                errored = True
                quit.wait(60)
                if quit.isSet(): break
                continue
            cset(req=False)
            if quit.isSet(): break
            if errored:
                console.stdprint("Stories alive")
                errored = False
            
            updated = []
            
            for ustory in upd_stories:
                if ustory[0] == last_upd_id and ustory[1] == last_upd_chapter: break
                if ustory[0] in updated: continue
                backup_story(user.get_story(ustory[0]))
                if quit.isSet(): break
                updated.append(ustory[0])
                quit.wait(10)
                if quit.isSet(): break
            
            last_upd_id, last_upd_chapter = upd_stories[0]
            sdb.execute('replace into lasts values(?, ?)', ('stories_upd_id', last_upd_id))
            sdb.execute('replace into lasts values(?, ?)', ('stories_upd_chapter', last_upd_chapter))
            
            if quit.isSet(): break
            stories.sort(key=lambda x:-x.story_id)
            for story in stories:
                if story.story_id in updated: continue
                if story.story_id <= last_story: break
                try:
                    cset(story.story_id, req=True)
                    try: backup_story(user.get_story(story.story_id))
                    except MySQLdb.OperationalError as e:
                        if e.args[0] not in (2006,): raise
                        mysql_connect()
                        backup_story(user.get_story(story.story_id))
                    cset(req=False)
                    quit.wait(10)
                except:
                    traceback.print_exc()
                    console.stdprint(story.story_id, "not backuped")
                    quit.wait(10)
                if quit.isSet(): break
            last_story = stories[0].story_id
        except sapi.StoriesError as exc:
            console.stdprint("stories error:", exc)
            err += 1
            if err >= 3:
                err = 0
                quit.wait(120)
        except:
            traceback.print_exc()
        finally:
            cset(' ')
            quit.wait(60)
            if quit.isSet(): break
    console.stdprint("Quit stories")

def backup_story(story):
    global user, quit
    if story.chapters:
        # cool story, bro
        chapters = []
        for c in xrange(1, len(story.chapters) + 1):
            cset(str(story.story_id) + '/' + str(c), req=True)
            chapters.append((story.chapters[c-1].encode("utf-8"), user.get_chapter(story.story_id, c)))
            cset(req=False)
            quit.wait(6)
            if quit.isSet(): return
    else:
        chapters = [(None, story.chapter)]
    
    db.execute("start transaction")
    
    for c in range(len(chapters)):
        chapter = chapters[c]
        db.execute("replace into stories_chapters values(%s, %s, %s, %s)", (story.story_id, c+1, chapter[0], lxml.etree.tostring(chapter[1], method="html", encoding="utf-8").replace("&#13;", ""),) )
    
    db.execute("replace into stories_authors values(%s, %s)", (story.author_id, story.author.encode("utf-8")))
    for char in story.characters.items():
        db.execute("replace into stories_characters values(%s, %s)", (char[0], char[1].encode("utf-8")))
        db.execute("replace into stories_text_characters values(%s, %s)", (story.story_id, char[0]))
    
    for cat in story.categories.items():
        db.execute("replace into stories_categories values(%s, %s)", (cat[0], cat[1].encode("utf-8")))
        db.execute("replace into stories_text_categories values(%s, %s)", (story.story_id, cat[0]))
    
    db.execute("replace into stories_main values(%s, %s, %s, %s, %s, %s)", (story.story_id, story.title.encode("utf-8"), story.author_id, story.upvotes, story.downvotes, story.description.encode("utf-8")))
    db.execute("commit")
    
    console.stdprint("story", story.story_id, story.title)

def mysql_connect():
    global db, db_conn
    db_conn = MySQLdb.connect("localhost", c['mysql_username'], c['mysql_password'], c['mysql_database'], charset="utf8")
    db_conn.ping(True)
    db = db_conn.cursor()

def init_tabun_plugin(env, register_handler):
    global user, db, sdb, console, db_conn, c, quit
    user = sapi.User()
    quit = env['quit_event']
    sdb = env['db']
    console = env['console']
    c=env['config']
    try:
        mysql_connect()
    except:
        console.stdprint("Cannot connect to mysql, stories backuper is disabled")
        return
    
    init_db()
    Thread(target=stories_thread).start()
