#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import os
import sys
import json
import time
import inspect
import traceback
from hashlib import md5
from socket import timeout as socket_timeout
from threading import Thread, RLock, Event, local, current_thread

import tabun_api as api
from tabun_api.compat import text, binary, PY2

from . import core, db

if PY2:
    from Queue import PriorityQueue, Empty as QueueEmpty
else:
    from queue import PriorityQueue, Empty as QueueEmpty


threads = []
readers = []
handlers = {}
events = PriorityQueue()
handlers_thread = None
quit_event = Event()


class Status(object):
    def __init__(self, state=None, onupdate=None, onupdate_ignore_for=(), debug=False):
        """Объект, хрнящий информацию о текущем состоянии tabun_feed (часть используется в работе, часть только для отладки)."""
        self._state = dict(state) if state else {}
        self._state['workers'] = 0
        self._lock = RLock()
        self._editables = []  # for remote control
        self._subscriptions = {}

        self._local = local()
        if debug:
            self._debug = True
            self._state['workers_list'] = []
        else:
            self._debug = False
            self._state['workers_list'] = None

        self.onupdate = onupdate
        self.onupdate_ignore_for = onupdate_ignore_for

    @property
    def state(self):
        return dict(self._state)

    @property
    def editables(self):
        return tuple(self._editables)

    @property
    def debug(self):
        return self._debug

    @property
    def lock(self):
        return self._lock

    def __enter__(self):
        if self._debug:
            if not hasattr(self._local, 'workers_queue'):
                self._local.workers_queue = []
            st = inspect.stack()
            st = '{1}:{2} ({3})'.format(*st[1]) if len(st) > 1 else None
            self._local.workers_queue.append(st)

        with self._lock:
            if self._debug:
                self._state['workers_list'].append(st)
            old = self._state['workers']
            self._state['workers'] = old + 1

        if self.onupdate and 'workers' not in self.onupdate_ignore_for:
            self.onupdate('workers', old, old + 1)

    def __exit__(self, typ, value, tb):
        if self._debug:
            st = self._local.workers_queue.pop() if self._local.workers_queue else None

        with self._lock:
            if self._debug and st in self._state['workers_list']:
                self._state['workers_list'].remove(st)
            old = self._state['workers']
            self._state['workers'] = old - 1

        if self.onupdate and 'workers' not in self.onupdate_ignore_for:
            self.onupdate('workers', old, old - 1)

    def __getitem__(self, key):
        return self._state.get(key)

    def __setitem__(self, key, value):
        with self._lock:
            old = self._state.get(key, None)
            self._state[key] = value
        if self._subscriptions.get(key):
            for x in self._subscriptions[key]:
                try:
                    x(key, old, value)
                except Exception:
                    fail()
        if self.onupdate and 'workers' not in self.onupdate_ignore_for:
            try:
                self.onupdate(key, old, value)
            except Exception:
                fail()

    def add(self, key, value=1, loop_on=None):
        with self._lock:
            old = self._state.get(key, 0)
            if loop_on is not None and old >= loop_on:
                new = 0
            else:
                new = old + value
            self[key] = new
        return new

    def append(self, key, item):
        with self._lock:
            value = self._state.get(key)
            if value is None or item in value:
                return False
            value.append(item)
            self[key] = value  # call onupdate
            return True

    def remove(self, key, item):
        with self._lock:
            value = self._state.get(key)
            if value is None or item not in value:
                return False
            value.remove(item)
            self[key] = value  # call onupdate
            return True

    def add_editable_item(self, key):
        if key not in self._editables:
            self._editables.append(key)

    def subscribe(self, key, func):
        if key not in self._subscriptions:
            self._subscriptions[key] = []
        if func not in self._subscriptions[key]:
            self._subscriptions[key].append(func)

    def get_json_key(self, key):
        with self._lock:
            value = self._state.get(key)
        if value is not None and not isinstance(value, (text, int, bool, float)):
            try:
                json.dumps(value, ensure_ascii=False)  # checking
                return value
            except Exception:
                return text(value)
        return value

    def get_json_status(self):
        with self._lock:
            state = dict(self._state)

        for key in tuple(state.keys()):
            value = state[key]
            if value is not None and not isinstance(value, (text, int, bool, float)):
                try:
                    json.dumps(value, ensure_ascii=False)  # checking
                    state[key] = value
                except Exception:
                    state[key] = text(value)
        return state


status = Status(
    {
        'started_at': time.time(),
        'counter': 0,
        'iter': 0,
        'request_error': None,
        'alivetime': 0,
        'event_id': 0,
        'threads': [],
        'readers_count': 0,
        'reader_current': 0,
        'iterations_interval': 0,
    },
    onupdate=lambda key, old_value, new_value: call_handlers_here("update_status", key, old_value, new_value),
    onupdate_ignore_for=('event_id', 'alivetime', 'last_requests', 'workers_list'),
    debug=True
)


def add_reader(func):
    """Добавляет читалку Табуна. Она будет вызываться в цикле в основном потоке."""
    if func not in readers:
        readers.append(func)


def add_handler(name, func, priority=1):
    """Добавляет обработчик в группу с указанным названием."""
    funcs = handlers.get(name)
    if not funcs:
        funcs = ([], [], [])
        handlers[name] = funcs
    if priority < 0 or priority > 2:
        raise ValueError('Invalid priority %d' % priority)
    funcs[priority].append(func)


def call_handlers(name, *args):
    """Вызывает группу обработчиков. Выполняются в отдельном потоке."""
    if current_thread() is handlers_thread:
        call_handlers_here(name, *args)
    else:
        events.put((10, status.add('event_id', loop_on=1000000), name, args))


def call_handlers_now(name, *args):
    """Вызывает группу обработчиков с повышенным приоритетом. Выполняются в отдельном потоке."""
    if current_thread() is handlers_thread:
        call_handlers_here(name, *args)
    else:
        events.put((0, status.add('event_id', loop_on=1000000), name, args))


def call_handlers_here(name, *args):
    """Вызывает группу обработчиков в текущем потоке. Аккуратнее с использованием!
    Возвращает число всего вызванных обработчиков и число упавших из них.
    """
    name = text(name)

    called = 0
    errors = 0
    prs = handlers.get(name, ())
    for pr in prs:
        for func in pr:
            try:
                if name != 'update_status':  # избегаем рекурсии из-за следующей строки
                    with status:
                        called += 1
                        func(*args)
                else:
                    func(*args)
            except Exception:
                core.logger.error('Handler %s (%s) failed:', name, func)
                fail()
                errors += 1
            finally:
                if name != 'update_status':
                    touch_alivefile()

    if not prs:
        if name != 'update_status':
            touch_alivefile()

    return called, errors


def touch_alivefile():
    tm = time.time()
    if tm - status['alivetime'] < 1:
        return
    status['alivetime'] = tm

    path = core.config.get('tabun_feed', 'alivefile')
    if not path:
        return

    try:
        with open(path, 'wb') as fp:
            fp.write((text(int(status['alivetime'])) + '\n').encode('utf-8'))
    except Exception as exc:
        core.logger.error('Cannot touch alive file %s: %s', path, exc)


def touch_pidfile():
    status['pid'] = os.getpid()

    path = core.config.get('tabun_feed', 'pidfile')
    if not path:
        return

    try:
        with open(path, 'wb') as fp:
            fp.write(text(status['pid']).encode('utf-8') + b'\n')
    except Exception as exc:
        core.logger.error('Cannot write pid file: %s', exc)


def touch_started_at_file():
    path = core.config.get('tabun_feed', 'started_at_file')
    if not path:
        return

    try:
        with open(path, 'wb') as fp:
            fp.write(text(status['started_at']).encode('utf-8') + b'\n')
    except Exception as exc:
        core.logger.error('Cannot write started_at file: %s', exc)


def clear_runfiles():
    for path in (core.config.get('tabun_feed', 'pidfile'), core.config.get('tabun_feed', 'started_at_file')):
        if not path or not os.path.isfile(path):
            continue
        try:
            os.remove(path)
        except Exception as exc:
            core.logger.error("Cannot remove %s: %s", path, exc)


def run_handlers_thread():
    while not events.empty() or not quit_event.is_set():
        try:
            priority, event_id, name, args = events.get(timeout=1)
        except QueueEmpty:
            continue

        if not name:
            continue

        try:
            call_handlers_here(name, *args)
        except Exception:
            fail()
            quit_event.wait(5)


def run_reader():
    status['iterations_interval'] = core.config.getfloat('tabun_feed', 'iterations_interval')

    while not quit_event.is_set():
        with status:
            core.logger.debug('Watcher iteration start')
            status['iter'] += 1

            rs = tuple(readers)
            status['readers_count'] = len(rs)

            for i, func in enumerate(tuple(readers)):
                if quit_event.is_set():
                    break
                status['reader_current'] = i

                try:
                    func()
                except api.TabunError as exc:
                    core.logger.warning('Tabun error: %s', exc.message)
                    status['error'] = exc.message
                except socket_timeout as exc:
                    core.logger.warning('Tabun result read error: timeout')
                    status['error'] = 'timeout'
                except Exception:
                    fail()
                    quit_event.wait(5)

                if events.empty():
                    touch_alivefile()

            db.db.commit()
            status['readers_count'] = 0
            status['reader_current'] = 0
            core.logger.debug('Watcher iteration ok')

        quit_event.wait(status['iterations_interval'])


def format_failure_email(data):
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    def e(x):
        return x.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

    plain_text = 'tabun_feed worker failed at {}:\n\n{}\n\nStatus:'.format(
        time.strftime('%Y-%m-%d %H:%M:%S'),
        data
    )

    for key, value in sorted(status.state.items()):
        plain_text += '\n- {}: {}'.format(key, text(value))
    plain_text += '\n'

    html_text = '<strong>tabun_feed worker failed at {}:</strong><br/>\n<pre style="font-family: \'DejaVu Mono\', monospace; background-color: #f5f5f5;">{}</pre>\n<hr/>\n<em>Status:</em><br/>\n<ul>'.format(
        time.strftime('%Y-%m-%d %H:%M:%S'),
        e(data)
    )

    for key, value in sorted(status.state.items()):
        html_text += '\n <li><strong>{}:</strong> <span style="white-space: pre-wrap">{}</span></li>'.format(key, e(text(value)))
    html_text += '\n</ul>'

    html_text = '<html><head></head><body>{}</body></html>'.format(html_text)

    mpart = MIMEMultipart('alternative')
    mpart.attach(MIMEText(plain_text.encode('utf-8'), 'plain', 'utf-8'))
    mpart.attach(MIMEText(html_text.encode('utf-8'), 'html', 'utf-8'))
    return [mpart]


def fail(desc=None):
    # Печатаем ошибку в лог
    exc = traceback.format_exc().strip()
    if isinstance(exc, binary):
        exc = exc.decode('utf-8', 'replace')
    if desc:
        core.logger.error(desc)
    core.logger.error(exc)

    # Отправляем на почту
    if core.config.get('email', 'errors_to'):
        try:
            core.sendmail(
                core.config.get('email', 'errors_to'),
                core.config.get('email', 'errors_subject'),
                format_failure_email(exc),
                fro=core.config.get('email', 'errors_from') or None
            )
        except Exception:
            core.logger.error(traceback.format_exc())

    try:
        if not core.config.getboolean('tabun_feed', 'failures_to_db'):
            return

        # считаем какой-нибудь хэш, чтобы не завалить админку одинаковыми ошибками
        ex_type, ex, tb = sys.exc_info()
        fail_hash = text(tb.tb_frame.f_code.co_filename) + '\x00' + text(tb.tb_lineno)
        fail_hash += '\x00' + text(ex_type) + '\x00' + text(ex)
        fail_hash = md5(fail_hash.encode('utf-8')).hexdigest()
        del ex_type, ex, tb

        st = json.dumps(status.get_json_status())

        # инкрементируем число случаев, если ошибка с таким хэшем уже есть
        fail_id = db.db.query('select id from failures where hash = ? and solved = 0 order by last_time desc limit 1', (fail_hash,))
        if fail_id:
            fail_id = fail_id[0][0]
            db.db.execute('update failures set occurrences = occurrences + 1, last_time = ? where id = ?', (int(time.time()), fail_id))
            return fail_id

        # создаём новую запись, если это первая такая нерешённая ошибка
        return db.db.execute(
            'insert into failures(hash, first_time, last_time, error, desc, status_json) values (?, ?, ?, ?, ?, ?)',
            (fail_hash, int(time.time()), int(time.time()), exc, desc or None, st)
        ).lastrowid

    except Exception:
        traceback.print_exc()
        return None


def get_failures(offset=0, count=20):
    return [{
        'id': x[0],
        'first_time': x[1],
        'last_time': x[2],
        'occurrences': x[3],
        'solved': x[4],
        'error': x[5],
        'desc': x[6]
    } for x in db.db.execute('select id, first_time, last_time, occurrences, solved, error, desc from failures order by id desc limit ?, ?', (offset, count)).fetchall()]


def get_failure(fail_id):
    x = db.db.query('select id, first_time, last_time, occurrences, solved, error, desc, status_json from failures where id = ?', (int(fail_id),))
    if not x:
        return
    x = x[0]
    return {
        'id': x[0],
        'first_time': x[1],
        'last_time': x[2],
        'occurrences': x[3],
        'solved': x[4],
        'error': x[5],
        'desc': x[6],
        'status': json.loads(x[7])
    }


def solve_failure(fail_id):
    db.db.execute('update failures set solved=1 where id=?', (fail_id,))


def start_handlers():
    global handlers_thread
    handlers_thread = start_thread(run_handlers_thread)


def start_thread(func, *args, **kwargs):
    try:
        item = (repr(func), inspect.getfile(func))
    except TypeError:
        item = (repr(func), None)

    def start_thread_func():
        threads.append(thread)
        status.append('threads', item)
        try:
            func(*args, **kwargs)
        except:  # pylint: disable=W0702
            # KeyobardInterrupt и SystemExit в неосновном потоке — тоже ошибка
            fail()
        finally:
            status.remove('threads', item)
            threads.remove(thread)

    thread = Thread(target=start_thread_func)
    thread.start()
    return thread


def stop():
    quit_event.set()
    if handlers_thread is not None:
        if status['workers'] > 0 or not events.empty():
            core.logger.info('Waiting for shutdown workers (%s)', status['workers'])
        events.put((20, status.add('event_id'), 'exit', [time.time()]))
        try:
            handlers_thread.join()
        except (KeyboardInterrupt, SystemExit):
            traceback.print_exc()

    for t in tuple(threads):
        t.join()

    try:
        db.db.commit()
    except Exception:
        pass

    core.logger.info('Exiting')
    clear_runfiles()


# entry point: #


def run():
    call_handlers('start')

    try:
        run_reader()
    except (KeyboardInterrupt, SystemExit):
        print('')
    except Exception:
        fail()
        return False
    else:
        return True
    finally:
        stop()
