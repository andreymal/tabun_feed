#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import time
import traceback
import lxml.etree
import stories_api as sapi
from threading import Thread

tabun_feed = None
user = None
db = None
console = None
quit = None

def init_db():
    global db
    tables = db.execute("show tables").fetchall()
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
    global user, quit
    last_upd_id = tabun_feed.get_db_last("stories_upd_id")
    last_upd_chapter = tabun_feed.get_db_last("stories_upd_chapter")
    
    last_story = db.execute("select max(story_id) from stories_main").fetchall()[0][0]
    if not last_story: last_story = 0
    
    console.stdprint("Stories thread started")
    cset(' ')
    errored = False
    err = 0
    while 1:
        try:
            try:
                cset('n', req=True)
                stories = user.get_new()
                cset(req=False)
                quit.wait(5)
                if quit.isSet(): break
                cset('u', req=True)
                upd_stories = user.get_updates()
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
            
            ###
            
            new = []
            for ustory in upd_stories:
                if ustory[0] == last_upd_id and ustory[1] == last_upd_chapter: break
                if not ustory[0] in new: new.append(ustory[0])
            
            for story in stories:
                if story.story_id <= last_story: break
                if not story.story_id in new: new.append(story.story_id)
            
            new.sort()
            if quit.isSet(): break
            
            for story_id in new:
                try:
                    cset(story.story_id, req=True)
                    backup_story(user.get_story(story_id))
                    cset(req=False)
                    quit.wait(10)
                except Exception as exc:
                    if isinstance(exc, sapi.StoriesError): console.stdprint("stories error on story %s:"%str(story_id), exc)
                    else: traceback.print_exc()
                    console.stdprint(story_id, "not backuped")
                    quit.wait(10)
                if quit.isSet(): break

            if upd_stories:
                last_upd_id, last_upd_chapter = upd_stories[0]
                tabun_feed.set_db_last('stories_upd_id', last_upd_id)
                tabun_feed.set_db_last('stories_upd_chapter', last_upd_chapter)
            
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
        inc = 0
        for c in xrange(1, len(story.chapters) + 1):
            cset(str(story.story_id) + '/' + str(c+inc), req=True)
            j = 10
            while j > 0:
                j -= 1
                try:
                    chapters.append((story.chapters[c-1].encode("utf-8"), user.get_chapter(story.story_id, c+inc)))
                    break
                except sapi.StoriesError as exc:
                    if exc.code != 404: raise
                    console.stdprint("Stories warning:",str(story.story_id) + '/' + str(c+inc), '— 404 Not Found!')
                    inc += 1
            cset(req=False)
            quit.wait(6)
            if quit.isSet(): return
    else:
        chapters = [(None, story.chapter)]
    
    with db.lock:
        db.execute("start transaction")
        
        for c in range(len(chapters)):
            chapter = chapters[c]
            db.execute("replace into stories_chapters values(%s, %s, %s, %s)", (story.story_id, c+1, chapter[0], lxml.etree.tostring(chapter[1], method="html", encoding="utf-8").replace("&#13;", ""),) )
        
        db.execute("update stories_chapters set name = concat(%s, name) where story_id=%s and chapter_id>%s", ("[удалено] ", story.story_id, len(chapters),))
        
        db.execute("replace into stories_authors values(%s, %s)", (story.author_id, story.author.encode("utf-8")))
        db.execute("delete from stories_text_characters where story_id=%s", (story.story_id,))
        db.execute("delete from stories_text_categories where story_id=%s", (story.story_id,))
        for char in story.characters.items():
            db.execute("replace into stories_characters values(%s, %s)", (char[0], char[1].encode("utf-8")))
            db.execute("replace into stories_text_characters values(%s, %s)", (story.story_id, char[0]))
        
        for cat in story.categories.items():
            db.execute("replace into stories_categories values(%s, %s)", (cat[0], cat[1].encode("utf-8")))
            db.execute("replace into stories_text_categories values(%s, %s)", (story.story_id, cat[0]))
        
        db.execute("replace into stories_main values(%s, %s, %s, %s, %s, %s)", (story.story_id, story.title.encode("utf-8"), story.author_id, story.upvotes, story.downvotes, story.description.encode("utf-8")))
        db.execute("commit")
    
    console.stdprint(time.strftime("%H:%M:%S"), "story", story.story_id, story.title)

def mysql_connect():
    global db
    db = tabun_feed.require("mysql").db

def init_tabun_plugin(tf):
    global tabun_feed, user, console, quit
    tabun_feed = tf
    user = sapi.User()
    quit = tabun_feed.quit_event
    console = tabun_feed.console
    try:
        mysql_connect()
    except:
        console.stdprint("Cannot connect to mysql, stories backuper is disabled")
        return
    
    init_db()
    Thread(target=stories_thread).start()
