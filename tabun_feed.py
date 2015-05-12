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

api.http_headers["user-agent"] = 'tabun_feed/0.5.1; Linux/2.6'

config = {
    "phpsessid": "",
    "urls": "/blog/newall/,/personal_blog/newall/",
    "db": "tabun_feed.db",
    "sleep_time": "10",
    "plugins_dir": "plugins",
    "security_ls_key":"",
    "key":"",
    "username": "",
    "password": "",
    "get_comments_max_pages": "0",
    "get_comments_min_pages": "1",

}

debug = False
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
        if debug: console.stdprint("-DB loaded")
    
    def execute(self, *args):
        with self.lock:
            if debug: console.stdprint("-DB", args)
            return self.db.execute(*args).fetchall()
    
    def commit(self):
        with self.lock:
            if debug: console.stdprint("-DB commit")
            try: self.db.commit()
            except KeyboardInterrupt:
                console.stdprint("commit break!")
                raise 
            
class Console(object):
    replacements = (
        ('K', '\x1b[01;30m'),
        ('R', '\x1b[01;31m'),
        ('G', '\x1b[01;32m'),
        ('Y', '\x1b[01;33m'),
        ('B', '\x1b[01;34m'),
        ('M', '\x1b[01;35m'),
        ('C', '\x1b[01;36m'),
        ('W', '\x1b[01;37m'),
        ('D', '\x1b[01;39m'),
        ('k', '\x1b[01;40m'),
        ('r', '\x1b[01;41m'),
        ('g', '\x1b[01;42m'),
        ('y', '\x1b[01;43m'),
        ('b', '\x1b[01;44m'),
        ('m', '\x1b[01;45m'),
        ('c', '\x1b[01;46m'),
        ('w', '\x1b[01;47m'),
        ('d', '\x1b[01;49m'),
        ('0', '\x1b[0m')
    )
    
    def __init__(self, colored=True, simple_clear=False):
        self.colored = bool(colored)
        self.simple_clear = bool(simple_clear)

        self.order = []
        self.data = {}
        self.last_len = 0
        self.lock = RLock()
        self.expander_key = None
        self.noflush = False

    def __enter__(self):
        self.lock.acquire()
        self.noflush = True

    def __exit__(self, typ, value, tb):
        self.noflush = False
        try:
            self.print_all()
        except:
            traceback.print_exc()
        self.lock.release()

    def set(self, key, value, position=-1, colored=False):
        key = key.decode("utf-8", "replace") if isinstance(key, str) else unicode(key)
        value = value.decode("utf-8", "replace") if isinstance(value, str) else unicode(value)
        position = int(position)

        with self.lock:
            data = self.data.get(key)
            if not data:
                # [unicode data, raw (colored) unicode data, output len (without
                # escape sequences), is colored]
                data = [None, None, 0, False]
                if position >= 0:
                    self.order.insert(position, key)
                else:
                    self.order.append(key)

            data[0] = value
            data[1] = None
            data[3] = bool(colored)

            self.data[key] = data

            if not self.noflush:
                try:
                    self.print_all()
                except:
                    traceback.print_exc()

    def format_field(self, key, setlen=None):
        with self.lock:
            data = self.data.get(key)
            if not data:
                return u"", 0

            data[1] = data[0].\
                replace(u'\x1b', u'^[').\
                replace(u'\x00', u'').\
                replace(u'\r', u' ').replace(u'\n', u' ')
            length = len(data[1])
            
            if not data[3]:  # not colored
                if setlen is not None:
                    if setlen < length:
                        data[1] = data[1][:setlen]
                    else:
                        data[1] = data[1] + u" " * (setlen - length)
                    length = setlen
                
                elif data[2] > length:
                    data[1] = data[1] + u" " * (len(data[1]) - length)
                    length = len(data[1])
            
            else: # colored
                rawdata = u""
                length = 0

                i = -2
                while True:
                    ni = data[1].find('%', i + 2, len(data[1]) - 1)
                    if ni < 0:
                        break

                    part = data[1][i + 2:ni + 2]  # ignore if key is unknown
                    partesc = u""
                    i = ni
                    
                    if data[1][i+1] == '%':
                        part = part[:-1]  # %% -> %
                    else:
                        for key, esc in self.replacements:
                            if data[1][i+1] == key:
                                part = part[:-2]  # crop key
                                partesc = esc if self.colored else ""
                                break

                    if setlen is not None and length + len(part) > setlen:
                        part = part[:-(length + len(part) - setlen)]
                        partesc = ""

                    rawdata += part + partesc
                    length += len(part)
                    if setlen is not None and length >= setlen:
                        break

                if i >= 0 and (setlen is None or length < setlen) and i < len(data[1]) - 2:
                    part = data[1][i + 2:]
                    if setlen is not None and length + len(part) > setlen:
                        part = part[:-(length + len(part) - setlen)]
                    rawdata += part
                    length += len(part)

                if setlen is not None and setlen > length:
                    rawdata = rawdata + u" " * (setlen - length)
                    length = setlen
                elif data[2] > length:
                    rawdata = rawdata + u" " * (data[2] - length)
                    length += data[2] - length

                data[1] = rawdata + '\x1b[0m'

            data[2] = length
            return data[1], data[2]

    def get(self, key):
        return self.data.get(key)[0]

    def pop(self, key):
        with self.lock:
            if key not in self.order:
                return False
            self.data.pop(key)
            self.order.remove(key)
            if not self.noflush:
                try:
                    self.print_all()
                except:
                    traceback.print_exc()
            return True

    def clear(self):
        with self.lock:
            if self.simple_clear:
                sys.stdout.write("\r" + " "*self.last_len + "\r")
            else:
                sys.stdout.write("\r\x1b[2K")

    def stdprint(self, *args, **kwargs):
        with self.lock:
            self.clear()
            for x in args:
                print x,
            if kwargs.get("end", True):
                print
                if not self.noflush:
                    try:
                        self.print_all()
                    except:
                        traceback.print_exc()
            else:
                sys.stdout.flush()

    def print_all(self):
        with self.lock:
            begin = u""
            center = u""
            end = u""
            self.last_len = 0

            for key in self.order:
                if key == self.expander_key:
                    end = u" | "
                    self.last_len += 3
                    continue

                raw, rawlen = self.data[key][1:3]
                if raw is None:
                    raw, rawlen = self.format_field(key)

                raw += u" | "
                if end:
                    end += raw
                else:
                    begin += raw

                self.last_len += rawlen + 3

            exp = self.data.get(self.expander_key)
            if exp:
                # raw, rawlen = exp[1:2]
                # if raw is None:
                #     raw, rawlen = self.format_field(self.expander_key)
                # center = raw
                w = self.get_term_size()[0]
                free = w - self.last_len - 1

                if free > 0:
                    raw, rawlen = self.format_field(self.expander_key, setlen=free)
                    center = raw
                    self.last_len += rawlen

                # if free > 0:
                #     if free > len(rawlen):
                #         center = raw + u" " * (free - len(text))
                #     else:
                #         center = text[:free]

            self.clear()
            print begin + center + end,
            sys.stdout.flush()

    def bprint(self, *args):
        with self.lock:
            for x in args:
                print x,
            sys.stdout.write("\r")
            sys.stdout.flush()

    def rprint(self, *args):
        with self.lock:
            sys.stdout.write("\r")
            for x in args:
                print x,
            sys.stdout.flush()

    def nprint(self, *args):
        with self.lock:
            for x in args:
                print x,
            sys.stdout.flush()

    def set_expanded(self, key):
        self.expander_key = key

    @staticmethod
    def get_term_size():
        def ioctl_GWINSZ(fd):
            try:
                import fcntl
                import termios
                import struct
                cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
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
            cr = (os.environ.get('LINES', 25), os.environ.get('COLUMNS', 80))
        return int(cr[1]), int(cr[0])

console = Console()

get_full_posts = False

def request_full_posts():
    global get_full_posts
    get_full_posts = True

def notify(data):
    if isinstance(data, unicode): data = data.encode("utf-8")
    if not config.get("notify_group"):
        console.stdprint(data)
        return
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

class TabunFeed:
    def __init__(self, plugin_name):
        self.plugin_name = plugin_name
        self.db = db
        self.notify = notify
        self.config = config
        self.request_full_posts = request_full_posts
        self.console = console
        self.quit_event = quit_event
        self.reset_unread = reset_unread
        self.require = require
        self.PluginError = PluginError
        self.add_handler = add_handler
        self.call_handlers = call_handlers
        self.get_db_last = get_db_last
        self.set_db_last = set_db_last

class PluginError(Exception): pass

def require(plugin_name):
    global plugins
    plugin_name = str(plugin_name).replace('/', '').replace(' ', '_')
    try: return plugins[plugin_name]
    except KeyError: pass

    plugin_file = config['plugins_dir'] + '/' + plugin_name
    if os.path.isdir(plugin_file):
        if os.path.exists(plugin_file + '/' + plugin_name + '.py'):
            plugin_file +=  '/' + plugin_name + '.py'
        else:
            plugin_file += '/plugin.py'
    else: plugin_file += '.py'
    try: fp = open(plugin_file, "U")
    except: raise PluginError

    try:
        name = "tabun_plugin_" + plugin_name
        description = ('.py', 'U', 1)
        try:
            plug = imp.load_module(name, fp, plugin_file, description)
        except KeyboardInterrupt: raise
        except:
            traceback.print_exc()
            raise PluginError
    
        plugins[plugin_name] = plug
        try: plug.init_tabun_plugin(TabunFeed(plugin_name))
        except: traceback.print_exc() #TODO: как-нить обработать
        return plug
    finally:
        fp.close()

def load_plugins():
    plugins_dir = config['plugins_dir']
    if not os.path.exists(plugins_dir):
        console.stdprint("Plugins dir not found! :(")
        return
    files = sorted(os.listdir(plugins_dir))
    for f in files:
        if f[-3:] == ".py": f = f[:-3]
        elif not os.path.isdir(plugins_dir + '/' + f): continue
        try:
            require(f)
        except PluginError:
            console.stdprint(f, "plugin failed")

def add_handler(name, func, priority=1):
    if isinstance(name, unicode): name = name.encode("utf-8")
    elif not isinstance(name, str): return
    funcs = handlers.get(name)
    if not funcs:
        funcs = ([], [], [])
        handlers[name] = funcs
    funcs[priority].append(func)

def call_handlers(name, *args, **kwargs):
    error = 0
    for pr in handlers.get(name, []):
        for func in pr:
            try:
                func(*args, **kwargs)
            except KeyboardInterrupt: raise
            except:
                traceback.print_exc()
                error += 1
    return error

def get_db_last(typ, default=0):
    last = db.execute("select value from lasts where type=?", (typ,))
    if last: return last[0][0]
    db.execute("insert into lasts values(?, ?)", (typ, default))
    return default

def set_db_last(typ, value):
    db.execute("replace into lasts values(?, ?)", (typ, value))

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
    
    updated = False
    for i in range(len(urls)):
        console.set("get_tic", " r" + (":" if i % 2 == 0 else ".") + str(r), position=0)
        try:
            raw_data = user.urlopen(urls[i]).read()
            if not updated:
                old_user = user.username
                if not user.update_userinfo(raw_data) and old_user and not relogin and config.get("password"):
                    relogin = True
                    return go()
            data[urls[i]] = raw_data
            
            if urls[i] == '/comments/': # TODO: переделать по-нормальному
                comments.extend(user.get_comments(raw_data=raw_data).values())
                comments.sort(key=lambda x:-x.comment_id)
                
                cpage = 1
                cpage_max = int(config.get("get_comments_max_pages", 0))
                cpage_min = int(config.get("get_comments_min_pages", 1))
                while cpage < cpage_min or ((not cpage_max or cpage < cpage_max) and last_comment > 0 and comments and comments[-1].comment_id > last_comment):
                    cpage += 1
                    if cpage > cpage_min:
                        console.stdprint("Load comments, page", cpage)
                    console.set("get_tic", " r" + (":" if (i + cpage - 1) % 2 == 0 else ".") + str(r), position=0)
                    raw_data2 = user.urlopen("/comments/page" + str(cpage) + "/").read()
                    time.sleep(1)
                    comments.extend(user.get_comments(raw_data=raw_data2).values())
                    comments.sort(key=lambda x: -x.comment_id)
            else:
                ps = user.get_posts(urls[i], raw_data=raw_data)
                posts.extend(ps)
            
            if "/index/newall/" in urls[i]:
                blogs_list = user.get_short_blogs_list(raw_data=raw_data)
        
        except KeyboardInterrupt: raise
        except api.TabunError as exc:
            console.set("get_tic", " r-" + str(r) + " " + str(exc))
        except socket.timeout:
            console.set("get_tic", " r-" + str(r) + " timeout")
        except socket.error:
            console.set("get_tic", " r-" + str(r) + " sock err")
        else:
            console.set("get_tic", " r-" + str(r))
    r += 1
    
    tmp = old_unread
    old_unread = user.talk_unread
    if user.talk_unread > tmp:
        call_handlers("talk_unread")
    del tmp
    
    relogin = False
    call_handlers("post_load", data)
    
    if blogs_list:
        # блоги
        blogs_list.sort(lambda a,b:cmp(b.blog_id, a.blog_id)) # reversed list
        new_blogs = []

        if blogs_list and not last_bid:
            db.execute("update lasts set value=? where type='blog'", (blogs_list[0].blog_id,) )
        else:
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
                if comment.comment_id == last_comment: # фильтруем дубликаты
                    continue
                last_comment = comment.comment_id
                db.execute("update lasts set value=? where type='comment_id'", (last_comment,) )
                error = call_handlers("comment", comment, blogs_list)
                if error:
                    call_handlers("comment_error", comment)
            except KeyboardInterrupt: raise
            except:
                traceback.print_exc()
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

    console.set("get_tic", " r " + str(r - 1))

def main():
    global user, anon, debug
    if "-d" in sys.argv: debug = True
    load_config()
    
    sleep_time = int(config["sleep_time"])
    
    pidfile = config.get("pidfile")
    
    init_db()
    load_plugins()
    
    if pidfile:
        with open(pidfile, "wb") as fp: fp.write(str(os.getpid()) + "\n")
    
    try:
        call_handlers("loaded")

        errors = 0
        while 1:
            try:
                user = None
                if config.get('phpsessid') and config.get('password'):
                    tmpuser = api.User(phpsessid=config['phpsessid'])
                    if tmpuser.username:
                        if config.get('username') and tmpuser.username == config['username']:
                            console.stdprint('Fast login!')
                            user = tmpuser
                    del tmpuser

                if user is None:
                    user = api.User(
                        phpsessid=(config['phpsessid'] if config['phpsessid'] else None),
                        security_ls_key=(config['security_ls_key'] if config['security_ls_key'] else None),
                        key=(config['key'] if config['key'] else None),
                        login=(config['username'] if config['username'] else None),
                        passwd=(config['password'] if config['password'] else None),
                    )
                if config.get('timeout'):
                    user.timeout = int(config['timeout'])

                if not user.username:
                    anon = user
                else:
                    console.stdprint("Logined as", user.username)
                    anon = api.User()
                    if config.get('timeout'):
                        anon.timeout = int(config['timeout'])
                break
            except Exception as exc:
                if isinstance(exc, api.TabunError):
                    console.stdprint("init error:", str(exc))
                else:
                    traceback.print_exc()
                errors += 1
                if errors % 3 == 0:
                    time.sleep(90)
                else:
                    time.sleep(15)

        call_handlers("set_user", user, anon)

        while 1:
            try:
                try:
                    go()
                finally:
                    db.commit()
                try:
                    time.sleep(sleep_time)
                except KeyboardInterrupt:
                    break
            except KeyboardInterrupt: raise
            except:
                traceback.print_exc()
                time.sleep(sleep_time)
    finally:
        try: db.commit()
        except: traceback.print_exc()
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
        raise
