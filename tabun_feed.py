#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import imp
import sys
import time
import socket
import sqlite3
import traceback
import tabun_api as api
from threading import RLock, Event

api.headers_example["user-agent"] = 'tabun_feed/0.3; Linux/2.6'

#sqlite3.threadsafety = 0

config = {
    "phpsessid": "",
    "urls": "/blog/newall/,/personal_blog/newall/",
    "db": "tabun_feed.db",
    "sleep_time": "10",
    "plugins_dir": "plugins",
    "security_ls_key":"",
    "key":"",
    "username": "",
    "password": ""
}

plugins = {}
handlers = {}

user = None
user_tic = 0
anon = None
db = None

quit_event = Event()

class ThreadDB:
    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = RLock()
    
    def execute(self, *args):
        with self.lock:
            return self.db.execute(*args).fetchall()
    
    def commit(self):
        with self.lock:
            self.db.commit()

class Console:
    def __init__(self):
        self.keys = []
        self.data = {}
        self.last_len = 0
        self.lock = RLock()

    def set(self, key, value, position=-1):
        with self.lock:
            if isinstance(key, unicode): key = key.encode("utf-8")
            else: key = str(key)
            if isinstance(value, str): value = value.decode("utf-8", "replace")
            else: value = unicode(value)
            position = int(position)
            
            data = self.data.get(key)
            if not data:
                data = [None, 0]
                if position >= 0: self.keys.insert(position, key)
                else: self.keys.append(key)
            data[1] = max(data[1], len(value))
            data[0] = value
            self.data[key] = data
            
            self.print_all()
        
    def get(self, key):
        return self.data.get(key)
    
    def pop(self, key):
        with self.lock:
            if not key in self.keys: return
            self.data.pop(key)
            self.keys.remove(key)
            self.print_all()
    
    def clear(self):
        with self.lock:
            sys.stdout.write("\r" + " "*self.last_len + "\r")
            
    def stdprint(self, *args, **kwargs):
        with self.lock:
            self.clear()
            for x in args: print x,
            if kwargs.get("end", True):
                print
                self.print_all()
            else: sys.stdout.flush()
    
    def print_all(self):
        with self.lock:
            sys.stdout.write("\r" + " "*self.last_len + "\r")
            self.last_len = 0
            for key in self.keys:
                data = self.data[key]
                sys.stdout.write(data[0])
                if len(data[0]) < data[1]:
                    sys.stdout.write(" " * (data[1] - len(data[0])) )
                sys.stdout.write(" | ")
                self.last_len += data[1] + 3
            sys.stdout.flush()

    def bprint(self, *args):
        with self.lock:
            for x in args: print x,
            sys.stdout.write("\r")
            sys.stdout.flush()

    def rprint(self, *args):
        with self.lock:
            sys.stdout.write("\r")
            for x in args: print x,
            sys.stdout.flush()

    def nprint(self, *args):
        with self.lock:
            for x in args: print x,
            sys.stdout.flush()
   
console = Console() 
  
  
get_full_posts = False

def request_full_posts():
    global get_full_posts
    get_full_posts = True

def notify(data):
    if isinstance(data, unicode): data = data.encode("utf-8")
    if not config.get("notify_group"): return
    data = config["notify_group"] + ":" + data
    os.system('notify "'+data.replace('"','\\"').replace("`","'") + '"')
  
def load_config(f='config.cfg', config=None):
    """Читает файл как конфиг в формате ключ=значение в словарь config. Лишние пробелы удаляются, \
пустые и неполные строки игнорируются, строки с первым символом # игнорируются."""
    if config==None: config=globals()['config']
    try: fp=open(f,'r')
    except:
        print("Предупреждение: файл конфигурации "+f+" не найден\n")
        return
    l=' '
    while len(l)>0:
        l=fp.readline()
        if not l or not '=' in l: continue
        if l[0]=='#': continue
        key,data=l.split('=',1)
        key=key.strip().lower()
        data=data.strip()
        if not key or not data: continue
        config[key]=data
    fp.close()
    
def init_db():
    global db
    db_path = config["db"]
    create = not os.path.exists(db_path)
    #db = sqlite3.connect(db_path)
    db = ThreadDB(db_path)
    
    if create:
        #db.execute("create table log(time int, state varchar(1), post_id int, descr text)")
        db.execute("create table lasts(type text primary key, value int)")
        #db.execute("create table videos(link text primary key, video_id text)")

def load_plugins():
    plugins_dir = config['plugins_dir']
    files = sorted(os.listdir(plugins_dir))
    for f in files:
        if f[-3:] != ".py": continue
        fp = open(plugins_dir + "/" + f, "U")
        try:
            name = "tabun_plugin_" + f[:-3].replace(".","_").replace(" ", "_")
            plugin_file = plugins_dir + "/" + f
            description = ('.py', 'U', 1)
            
            plug = imp.load_module(name, fp, plugin_file, description)
            plugins[name] = plug
            env = {
                'config': config,
                'db': db,
                'notify': notify,
                'request_full_posts': request_full_posts,
                'console': console,
                'quit_event': quit_event,
            }
            
            plug.init_tabun_plugin(env, register_handler)
        except KeyboardInterrupt: raise
        except:
            traceback.print_exc()
        finally:
            fp.close()

def register_handler(name, func, priority=1):
    if isinstance(name, unicode): name = name.encode("utf-8")
    elif not isinstance(name, str): return
    funcs = handlers.get(name)
    if not funcs:
        funcs = ([], [], [])
        handlers[name] = funcs
    funcs[priority].append(func)

def call_handlers(name, *args, **kwargs):
    error = False
    for pr in handlers.get(name, []):
        for func in pr:
            try:
                func(*args, **kwargs)
            except KeyboardInterrupt: raise
            except:
                traceback.print_exc()
                error = True
    return error

r = 0
relogin = False
def go():
    global r
    global relogin
    #global user_tic
    
    """user_tic += 1
    if user.username and user_tic >= 5 and config.get("password"):
        user_tic = 0
        try:
            if not user.parse_userinfo(user.urlopen("/").read(1024*25)):
                user.login(config.get("username", user.username), config["password"])
                console.stdprint("Relogined as", user.username)
        except api.TabunError as exc:
            console.stdprint(str(exc))
        time.sleep(1)
    print user_tic"""
    
    if relogin:
        try:
            user.login(config.get("username", user.username), config["password"])
            console.stdprint("Relogined as", user.username)
        except api.TabunError as exc:
            console.stdprint(str(exc))
        time.sleep(1)
    
    urls = config['urls'].split(",")
    posts = []
    
    last_time = db.execute("select value from lasts where type='time'")
    if last_time: last_time = last_time[0][0]
    else:
        last_time = 0
        db.execute("insert into lasts values('time', 0)")
    last_time = time.localtime(last_time)
    
    last_comment = db.execute("select value from lasts where type='comment_id'")
    if last_comment: last_comment = last_comment[0][0]
    else:
        db.execute("insert into lasts values('comment_id', 0)")
        last_comment = 0
    
    last_bid = db.execute("select value from lasts where type='blog'")
    if last_bid: last_bid = last_bid[0][0]
    else:
        last_bid = 0
        db.execute("insert into lasts values('blog', 0)")
    
    data = {}
    comments = None
    blogs_list = None
    
    call_handlers("load", urls)
    
    for i in range(len(urls)):
        #nprint(" r"+(":" if i%2==0 else "."))
        console.set("get_tic", " r" + (":" if i%2==0 else ".") + str(r), position=0)
        try:
            raw_data = user.urlopen(urls[i]).read()
            if not relogin and user.username and config.get("password"):
                if not user.parse_userinfo(raw_data):
                    relogin = True
                    return go()
            data[urls[i]] = raw_data
            
            if "/comments/" in urls[i]:
                comments = user.get_comments(raw_data=raw_data)
            else:
                ps = user.get_posts(urls[i], raw_data=raw_data)
                posts.extend(ps)
            
            if "/index/newall/" in urls[i]:
                blogs_list = user.get_short_blogs_list(raw_data=raw_data)
        
        except KeyboardInterrupt: raise
        except api.TabunError as exc:
            console.set("get_tic", " r " + str(r) + " " + str(exc))
        except socket.timeout:
            console.set("get_tic", " r " + str(r) + " timeout")
        except socket.error:
            console.set("get_tic", " r " + str(r) + " sock err")
        else:
            console.set("get_tic", " r " + str(r))
    r += 1
    
    relogin = False
    call_handlers("post_load", data)
    
    if blogs_list:
        # блоги
        blogs_list.sort(lambda a,b:cmp(b.blog_id, a.blog_id)) # reversed list
        new_blogs = []
        
        for blog in blogs_list:
            if blog.blog_id <= last_bid: break
            new_blogs.append(blog)
        
        new_blogs.reverse()
        for blog in new_blogs:
            try:
                last_bid = blog.blog_id
                db.execute("update lasts set value=? where type='blog'", (last_bid,) )
                error = call_handlers("blog", blog)
                if error:
                    call_handlers("blog_error", blog)
            except KeyboardInterrupt: raise
            except:
                traceback.print_exc()
            finally:
                db.commit()

    if comments:
        # новые комментарии
        new_comments = []
        comments.sort(key=lambda x:-x.comment_id)
        
        for comment in comments:
            if comment.comment_id <= last_comment: break
            new_comments.append(comment)
        
        new_comments.reverse()
        for comment in new_comments:
            try:
                last_comment = comment.comment_id
                db.execute("update lasts set value=? where type='comment_id'", (last_comment,) )
                error = call_handlers("comment", comment, blogs_list)
                if error:
                    call_handlers("comment_error", blog)
            except KeyboardInterrupt: raise
            except:
                traceback.print_exc()
            finally:
                db.commit()
    
    # посты
    
    posts.sort(lambda a,b: cmp(a.time, b.time))
    
    last_pid = 0
    for post in posts:
        try:
            if post.time <= last_time or post.post_id == last_pid: continue
            if get_full_posts:
                if not post.short: full_post = post
                else:
                    # запрашиваем через анонимуса, чтобы просмотр поста не засчитали на сервере
                    try: full_post = (user if post.private else anon).get_post(post.post_id, post.blog)
                    except api.TabunError: time.sleep(0.25); return
            else:
                full_post = None
            last_pid = post.post_id
            last_time = post.time
            db.execute("update lasts set value=? where type='time'", (time.mktime(last_time),))
            r = 0
            if full_post: error = call_handlers("post", post, full_post)
            else: error = call_handlers("post", post)
            if error:
                call_handlers("post_error", full_post if full_post else post)
        except KeyboardInterrupt: raise
        except:
            traceback.print_exc()
        finally:
            db.commit()

def main():
    global user, anon
    load_config()
    
    sleep_time = int(config["sleep_time"])
    
    init_db()
    load_plugins()
    
    errors = 0
    while 1:
        try:
            user = api.User(
                phpsessid=(config['phpsessid'] if config['phpsessid'] else None),
                security_ls_key=(config['security_ls_key'] if config['security_ls_key'] else None),
                key=(config['key'] if config['key'] else None),
                login=(config['username'] if config['username'] else None),
                passwd=(config['password'] if config['password'] else None),
            )
            if not user.phpsessid:
                anon = user
            else:
                anon = api.User()
                console.stdprint("Logined as", user.username)
            break
        except Exception as exc:
            if isinstance(exc, api.TabunError):
                console.stdprint(str(exc))
            else:
                traceback.print_exc()
            errors += 1
            if errors % 3 == 0: time.sleep(60)
            else: time.sleep(5)
    
    call_handlers("set_user", user, anon)
    
    try:
        while 1:
            try:
                go()
                time.sleep(sleep_time)
            except KeyboardInterrupt: raise
            except:
                traceback.print_exc()
                time.sleep(sleep_time)
    finally:
        quit_event.set()
        call_handlers("quit")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print
