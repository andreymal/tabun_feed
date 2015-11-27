#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from . import core

import os
import sqlite3
from threading import RLock


db = None


class Database(object):
    def __init__(self, path):
        self.created = not os.path.exists(path)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = RLock()
        self.allow_commit = True
        self._cur = None
        self._tables = None

    def __enter__(self):
        self.lock.acquire()
        self._cur = self.db.cursor()
        self.allow_commit = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.allow_commit = True
        if exc_type is None:
            self.commit()
        else:
            self.db.rollback()
        self._cur = None
        self.lock.release()

    @property
    def tables(self):
        if self._tables is not None:
            return self._tables
        self._tables = tuple(x[0] for x in self.query("select name from sqlite_master where type = ?", ("table",)))
        return self._tables

    def create_table(self, name, data):
        with self.lock:
            self._tables = None
            return (self._cur or self.db).execute('create table `{}` {}'.format(name, data))

    def init_table(self, name, data):
        if name in self.tables:
            return False
        self.create_table(name, data)
        return True

    def execute(self, *args):
        with self.lock:
            return (self._cur or self.db).execute(*args)

    def execute_unsafe(self, *args):
        # no locking
        return (self._cur or self.db).execute(*args)

    def executemany(self, *args):
        with self.lock:
            return (self._cur or self.db).executemany(*args)

    def query(self, *args):
        with self.lock:
            return (self._cur or self.db).execute(*args).fetchall()

    def commit(self):
        with self.lock:
            if not self.allow_commit:
                return
            try:
                self.db.commit()
            except (KeyboardInterrupt, SystemExit):
                print("commit break!")
                raise


def load_page_cache(page):
    """Возвращает список упорядоченных айдишников каких-то элементов (постов или комментариев, например)."""
    return [x[0] for x in db.query('select item_id from page_dumps where page = ? order by order_id', (page,))]


def save_page_cache(page, items):
    """Сохраняет список упорядоченных айдишников."""
    with db:
        db.execute('delete from page_dumps where page = ?', (page,))
        for index, item in enumerate(items):
            db.execute('insert into page_dumps values(?, ?, ?)', (page, index, item))


def get_db_last(name, default=0):
    last = db.query("select value from lasts where name = ?", (name,))
    if last:
        return last[0][0]
    db.query("insert into lasts values(?, ?)", (name, default))
    return default


def set_db_last(name, value):
    db.execute("replace into lasts values(?, ?)", (name, value))


def init():
    global db
    db = Database(core.config.get('tabun_feed', 'db'))

    db.init_table('lasts', "(name text not null primary key, value int not null default 0)")

    if db.init_table('page_dumps', "(page char(16) not null, order_id int not null default 0, item_id int not null, primary key(page, item_id))"):
        db.execute("create index page_dump_key on page_dumps(page)")

    if db.init_table('failures', '''(
        id integer primary key autoincrement not null,
        hash text not null,
        first_time int not null,
        last_time int not null,
        occurrences int not null default 1,
        solved int not null default 0,
        error text not null,
        desc text default null,
        status_json text not null default "{}"
    )'''):
        db.execute("create index failures_hash on failures(hash)")
