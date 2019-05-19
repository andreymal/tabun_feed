#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time
import logging
from threading import RLock

import MySQLdb
from tabun_api.compat import PY2, text, binary

if PY2:
    from urlparse import urlparse
else:
    from urllib.parse import urlparse

from .. import core


db = None
debug_by_default = False


class DB(object):
    def __init__(self, path, user=None, password=None, database=None, autocommit=True):
        self.path = text(path)
        self.user = text(user) if user else None
        self.password = text(password) if password else None
        self.database = text(database) if database else None
        self.autocommit = bool(autocommit)

        self._lock = RLock()
        self._conn = None
        self._with_count = 0  # число захватов блокировки в текущем потоке
        self._transaction_started = False  # дабы не запускать транзакцию когда не требуется

        self.debug = debug_by_default

        self.connect()

    def __enter__(self):
        self._lock.acquire()
        self._with_count += 1
        if self._with_count == 1:
            self._transaction_started = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if not self.autocommit and self._with_count == 1 and self._transaction_started:
                if exc_type is None:
                    if self.debug:
                        logging.debug('MySQL commit')
                    self._conn.commit()
                else:
                    if self.debug:
                        logging.debug('MySQL rollback')
                    self._conn.rollback()
        finally:
            self._with_count -= 1
            self._lock.release()

    @property
    def lock(self):
        return self._lock

    @property
    def connection(self):
        return self._conn

    def set_debug(self, debug):
        self.debug = bool(debug)

    def connect(self):
        data = urlparse(self.path)

        kwargs = {
            'user': data.username or self.user,
            'passwd': data.password or self.password,
            'charset': 'utf8'
        }
        if self.database:
            kwargs['db'] = self.database

        if data.scheme == 'unix':
            if data.hostname:
                raise ValueError(
                    'hostname must be empty for unix socket; '
                    'use unix:/path/to/socket or unix:///path/to/socket '
                    'or unix://username:password@/path/to/socket'
                )
            kwargs['host'] = 'localhost'
            kwargs['unix_socket'] = data.path
        else:
            kwargs['host'] = data.hostname
            if data.port is not None:
                kwargs['port'] = data.port

        with self._lock:
            self._conn = MySQLdb.connect(**kwargs)
            self._conn.ping(True)
            self._conn.cursor().execute('set autocommit=%d' % (1 if self.autocommit else 0))

    def disconnect(self):
        if not self._conn:
            return False
        with self._lock:
            self._conn.close()
            self._conn = None
        return True

    def escape(self, obj):
        # _conn.escape method is shit
        if isinstance(obj, text):
            result = self._conn.escape(obj.encode('utf-8'))
        else:
            result = self._conn.escape(obj)
        return result.decode('utf-8') if isinstance(result, binary) else text(result)

    def execute(self, sql, args=(), tries=15, _start_transaction=True):
        if _start_transaction and not self.autocommit and self._with_count > 0 and not self._transaction_started:
            # При выключенном автокоммите в конструкции `with` запускаем транзакцию
            self.execute('start transaction', _start_transaction=False)
            self._transaction_started = True

        if isinstance(sql, binary):
            sql = sql.decode('utf-8')

        if self.debug:
            logging.debug('MySQL Query: %s %s', sql, args)

        for i in range(tries):
            try:
                if i > 0:
                    self.connect()
                c = self._conn.cursor()
                c.execute(sql, args)
                return c
            except MySQLdb.OperationalError as exc:
                if self._transaction_started or i >= tries or exc.args[0] not in (2013, 2002, 2006):
                    raise
                c = None
                time.sleep(0.3)

    def execute_in(self, sql, in_args, args=(), binary_args=False):
        # select * from sometable where somecolumn = %s and somecolumn2 in (%s)
        if isinstance(sql, binary):
            sql = sql.decode('utf-8')
        # FIXME: binary_args?
        in_args = ((('binary ' if binary else '') + self.escape(x)) for x in in_args)
        in_args = ', '.join(in_args)
        sql = sql.replace('(%s)', '(' + in_args.replace('%', '%%') + ')')

        return self.execute(sql, args)

    def query(self, sql, args=()):
        with self._lock:
            return self.execute(sql, args).fetchall()

    def query_in(self, sql, in_args, args=(), binary_args=False):
        with self._lock:
            return self.execute_in(sql, in_args, args, binary_args).fetchall()


def connection_from_config(section='mysql', prefix=''):
    return DB(
        core.config.get(section, prefix + 'uri'),
        core.config.get(section, prefix + 'username') if core.config.has_option(section, prefix + 'username') else None,
        core.config.get(section, prefix + 'password') if core.config.has_option(section, prefix + 'password') else None,
        core.config.get(section, prefix + 'database') if core.config.has_option(section, prefix + 'database') else None,
        autocommit=False,
    )


def init_tabun_plugin():
    global db, debug_by_default
    if not core.config.has_section('mysql'):
        return
    if core.config.has_option('mysql', 'debug'):
        debug_by_default = core.config.getboolean('mysql', 'debug')
    db = connection_from_config()
