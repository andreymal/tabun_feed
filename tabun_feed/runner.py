#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import sys
import signal
import inspect
import logging

import tabun_api as api


tf_user_agent = api.http_headers["user-agent"] + ' tabun_feed/0.6.2'
api.http_headers["user-agent"] = tf_user_agent

go_thread = None


def sigterm(signo, frame):
    # Получать просьбу завершиться должен получать только основной поток и только один раз
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    if go_thread:
        go_thread.kill(SystemExit, False)
    else:
        raise SystemExit


def patch_encoding():
    if sys.version_info[0] == 2:
        reload(sys).setdefaultencoding('utf-8')


def patch_iso8601():
    try:
        from iso8601.iso8601 import LOG
    except ImportError:
        pass
    else:
        LOG.setLevel(logging.INFO)


def go():
    from . import core, user, worker, remote_server, remote_commands
    remote_server.start()
    worker.start_handlers()

    core.logger.info('Starting %s', tf_user_agent)
    try:
        user.auth_global()
    except (KeyboardInterrupt, SystemExit):
        print('')
        worker.stop()
        return

    return worker.run()

def main(args=None, config_file=None):
    global go_thread

    # Запускаем костыли
    patch_encoding()
    patch_iso8601()

    # Загружаемся
    if args is None:
        args = sys.argv[1:]

    from . import core

    core.parse_arguments(args)
    core.init_config(core.arguments.get('config', config_file))

    # Всё остальное запускам только после gevent, чтобы применился monkey patching
    from . import worker, db

    # Записывам состояние для удобства отладки
    worker.status['gevent_used'] = core.gevent_used
    worker.status.append('threads', (repr(main), inspect.getfile(main)))

    db.init()
    worker.touch_pidfile()
    worker.touch_started_at_file()

    # Инициализируем плагины (здесь уже могут появляться новые потоки)
    if not core.load_plugins():
        return False

    # worker сам не выключается, мы его выключаем
    signal.signal(signal.SIGTERM, sigterm)
    signal.signal(signal.SIGINT, sigterm)

    # worker ничего не знает про gevent, разруливаем его запуск и корректное выключение
    if core.gevent_used:
        import gevent
        go_thread = gevent.spawn(go)
        go_thread.join()
        return go_thread.value
    else:
        return go()
