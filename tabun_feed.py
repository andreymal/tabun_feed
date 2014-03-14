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

alivetime = 0

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
        self.expander = None

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
            #sys.stdout.write("\r" + " "*self.last_len + "\r")
            sys.stdout.write("\r\x1b[2K")
            
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
            begin = ""
            center = ""
            end = ""
            self.last_len = 0
            for key in self.keys:
                if key == self.expander:
                    end = " | "
                    continue
                data = self.data[key]
                tmp = data[0]
                if len(data[0]) < data[1]:
                    tmp += " " * (data[1] - len(data[0]))
                tmp += " | "
                if end: end += tmp
                else: begin += tmp
            exp = self.data.get(self.expander)
            if exp:
                w, h = self.get_term_size()
                free = w - len(begin) - len(end) - 1
                if free > 0:
                    if free > len(exp[0]):
                        center = exp[0] + u" " * (free - len(exp[0]))
                    else:
                        center = exp[0][:free]
            #sys.stdout.write("\r" + " "*self.last_len + "\r")
            sys.stdout.write("\r\x1b[2K")
            out = begin + center + end
            print out,
            self.last_len += len(out)
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

    def set_expanded(self, key):
        self.expander = key

    def get_term_size(self):
        env = os.environ
        def ioctl_GWINSZ(fd):
            try:
                import fcntl, termios, struct
                cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
            '1234'))
            except:
                return
            return cr
        cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
        if not cr:
            try:
                fd = os.open(os.ctermid(), os.O_RDONLY)
                cr = ioctl_GWINSZ(fd)
                os.close(fd)
            except:
                pass
        if not cr:
            cr = (env.get('LINES', 25), env.get('COLUMNS', 80))
        return int(cr[1]), int(cr[0])

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
                'reset_unread': reset_unread,
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

def get_db_last(typ, default=0):
    last = db.execute("select value from lasts where type=?", (typ,))
    if last: return last[0][0]
    db.execute("insert into lasts values(?, ?)", (typ, default))
    return default

r = 0
relogin = False
old_unread = 0
def reset_unread(value=-1):
    global old_unread
    if value <= 0: old_unread = 0
    else: old_unread -= value

def go():
    global r, relogin, alivetime, old_unread
    
    if relogin:
        try:
            user.login(config.get("username", user.username), config["password"])
            console.stdprint(time.strftime("%H:%M:%S"), "Relogined as", user.username)
        except api.TabunError as exc:
            console.stdprint(str(exc))
        time.sleep(1)
    
    urls = config['urls'].split(",")
    posts = []
    
    last_time = time.localtime(get_db_last('time'))
    last_post = get_db_last('post_id')
    last_comment = get_db_last('comment_id')
    last_bid = get_db_last('blog')
    
    data = {}
    comments = []
    blogs_list = None
    
    call_handlers("load", urls)
    
    if time.time() - alivetime >= 15 and config.get("alivefile"):
        try:
            alivetime = time.time()
            with open(config["alivefile"], "wb") as fp:
                fp.write(str(int(alivetime)) + "\n")
        except:
            traceback.print_exc()
    
    for i in range(len(urls)):
        console.set("get_tic", " r" + (":" if i%2==0 else ".") + str(r), position=0)
        try:
            raw_data = user.urlopen(urls[i]).read()
            if not relogin and user.username and config.get("password"):
                if not user.update_userinfo(raw_data):
                    relogin = True
                    return go()
            data[urls[i]] = raw_data
            
            if urls[i] == '/comments/':
                comments.extend(user.get_comments(raw_data=raw_data))
                comments.sort(key=lambda x:-x.comment_id)
                
                cpage = 1
                while last_comment > 0 and comments and comments[-1].comment_id > last_comment:
                    cpage += 1
                    console.stdprint("Load comments, page", cpage)
                    raw_data2 = user.urlopen("/comments/page" + str(cpage) + "/").read()
                    time.sleep(1)
                    comments.extend(user.get_comments(raw_data=raw_data2))
                    comments.sort(key=lambda x:-x.comment_id)
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
    
    if user.talk_unread > old_unread:
        call_handlers("talk_unread")
    old_unread = user.talk_unread
    
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
    
    posts.sort(key=lambda x:x.time)
    
    last_pid = 0 # исключает дубликаты из цикла (они могут возникать при запросе постов с нескольких адресов)
    for post in posts:
        try:
            if post.time < last_time or (post.time == last_time and post.post_id <= last_post) or post.post_id == last_pid:
                continue
            
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
            db.execute("update lasts set value=? where type='post_id'", (post.post_id,))
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
    
    pidfile = config.get("pidfile")
    
    init_db()
    load_plugins()
    
    if pidfile:
        with open(pidfile, "wb") as fp: fp.write(str(os.getpid()) + "\n")
    
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
            if config.get('timeout'): user.timeout = int(config['timeout'])
            if not user.phpsessid:
                anon = user
            else:
                anon = api.User()
                if config.get('timeout'): anon.timeout = int(config['timeout'])
                console.stdprint("Logined as", user.username)
            break
        except Exception as exc:
            if isinstance(exc, api.TabunError):
                console.stdprint("init error:", str(exc))
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
        try:
            quit_event.set()
            call_handlers("quit")
        finally:
            if pidfile:
                if os.path.isfile(pidfile): os.remove(pidfile)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise#print
