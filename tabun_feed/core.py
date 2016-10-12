#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import io
import os
import sys
import logging
import importlib
import traceback

from tabun_api.compat import PY2, text, binary


default_config = '''\
[tabun_feed]
loglevel = WARNING
logformat = [%(asctime)s] [%(levelname)s] %(message)s
db = tabun_feed.db
plugins_dir = plugins
plugins
failures_to_db = 0
gevent_threads = 0
pidfile
alivefile
username
password
session_id
http_host
session_cookie_name = TABUNSESSIONID
iterations_interval = 10
query_interval = 1.5
timeout = 15

[email]
from = tabun_feed@example.com
host = 127.0.0.1
port = 25
timeout = 3
notify_to
notify_subject = tabun_feed notify
notify_from
errors_to
errors_subject = tabun_feed error
errors_from

[posts]
urls = /index/newall/#3
request_full_posts = 0

[comments]
urls = /comments/#5
'''

arguments = {'--': []}

if PY2:
    from ConfigParser import RawConfigParser
    config = RawConfigParser(allow_no_value=True)
    config.readfp(io.BytesIO(default_config.encode('utf-8')))
else:
    from configparser import RawConfigParser
    config = RawConfigParser(allow_no_value=True)
    config.read_file([x for x in default_config.split('\n')])

config_files = []

logger = logging.getLogger('tabun_feed')
loglevel = logging.WARNING
plugins = {}
notify_func = None
gevent_used = False


class PluginError(Exception):
    pass


def parse_arguments(args):
    for arg in args:
        if not arg.startswith('-') or arg[1] != '-' and len(arg) != 2:
            arguments['--'].append(arg)
            continue
        if '=' in arg:
            key, value = arg.split('=', 1)
        else:
            key = arg
            value = None
        key = key.lstrip('-')
        arguments[key] = value


def load_config(config_file=None, with_includes=True):
    config_file = os.path.abspath(config_file)
    if config_file in config_files:
        raise RuntimeError('Recursive config: {}'.format(config_files))
    if not os.path.isfile(config_file):
        raise OSError('Config not found: {}'.format(config_file))
    config.read([config_file])
    config_files.append(config_file)

    # Загружаем конфиги в алфавитном порядке, а дальше в порядке загрузки конфигов
    # Пути относительно текущего конфига
    if with_includes and config.has_section('includes'):
        dirn = os.path.dirname(config_file)
        incl = sorted(config.items('includes'))
        config.remove_section('includes')
        for _, path in incl:
            load_config(os.path.join(dirn, path))


def init_config(config_file=None):
    global loglevel, gevent_used

    if not config_file:
        config_file = os.path.join(os.getcwd(), 'config.cfg')
    load_config(config_file, with_includes=True)

    # Инициализируем логгер
    loglevel = text(config.get('tabun_feed', 'loglevel')).upper()
    log_format = config.get('tabun_feed', 'logformat')

    if loglevel == 'DEBUG':
        loglevel = logging.DEBUG
    elif loglevel == 'INFO':
        loglevel = logging.INFO
    elif loglevel == 'WARNING':
        loglevel = logging.WARNING
    elif loglevel == 'ERROR':
        loglevel = logging.ERROR
    elif loglevel == 'FATAL':
        loglevel = logging.FATAL
    else:
        raise ValueError("Incorrect loglevel")

    logging.basicConfig(level=loglevel, format=log_format)

    if config.getboolean('tabun_feed', 'gevent_threads') and not gevent_used:
        import gevent.monkey
        gevent.monkey.patch_all()
        gevent_used = True

    # проверка, что это правда boolean (дабы не ловить ошибки потом в обработчике)
    config.getboolean('tabun_feed', 'failures_to_db')

    # Добавляем каталоги с плагинами в sys.path для импорта плагинов
    plugins_dir = config.get('tabun_feed', 'plugins_dir')
    if plugins_dir:
        plugins_dir = os.path.abspath(plugins_dir)
        if plugins_dir not in sys.path:
            sys.path.insert(0, plugins_dir)


def load_plugins():
    for module in text(config.get('tabun_feed', 'plugins') or '').split(','):
        if module.strip():
            try:
                load_plugin(module.strip())
            except PluginError as exc:
                logger.fatal(exc.args[0])
                return False
    return True


def load_plugin(module):
    module = get_full_module_name(module)

    if module in plugins:
        return plugins[module]

    logger.debug('load_plugin %s', module)

    try:
        if PY2:
            module = module.encode('utf-8')
        moduleobj = importlib.import_module(module)
        if hasattr(moduleobj, 'init_tabun_plugin'):
            moduleobj.init_tabun_plugin()

    except PluginError as exc:
        raise PluginError('Dependence for %s: %s' % (module, exc))

    except Exception:
        logger.error(traceback.format_exc())
        raise PluginError('Cannot load module %s' % module)

    plugins[module.decode('utf-8') if PY2 else module] = moduleobj
    return moduleobj


def get_full_module_name(module):
    module = text(module)
    if module.startswith(':'):
        module = 'tabun_feed.plugins.' + module[1:]
    elif module.startswith('r:'):
        module = 'tabun_feed.readers.' + module[2:]
    return module


def is_plugin_loaded(module):
    return get_full_module_name(module) in plugins


def notify(body):
    body = text(body)
    (notify_func or default_notify_func)(body)


def default_notify_func(body):
    if config.get('email', 'notify_to'):
        sendmail(
            config.get('email', 'notify_to'),
            config.get('email', 'notify_subject'),
            body,
            fro=config.get('email', 'notify_from') or None
        )
    else:
        logger.warning(body)


def set_notify_func(func):
    global notify_func
    if notify_func:
        raise ValueError('Conflict')
    notify_func = func


def sendmail(to, subject, items, fro=None):
    import smtplib
    from base64 import b64encode
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    if not items:
        return False
    if fro is None:
        fro = text(config.get('email', 'from'))

    if PY2:
        if isinstance(to, text):
            to = to.encode("utf-8")
    else:
        if isinstance(to, binary):
            to = to.decode("utf-8")
    if isinstance(subject, text):
        subject = subject.encode("utf-8")
    if isinstance(items, text):
        items = items.encode("utf-8")

    if isinstance(items, binary):
        msg = MIMEText(items, 'plain', 'utf-8')
    elif len(items) == 1:
        msg = items[0]
    else:
        msg = MIMEMultipart()
        for x in items:
            msg.attach(x)

    msg['From'] = fro
    msg['To'] = to

    subject_b64 = b64encode(subject)
    if isinstance(subject_b64, binary):
        subject_b64 = subject_b64.decode('ascii')
    subject_b64 = "=?UTF-8?B?" + subject_b64 + "?="
    msg['Subject'] = subject_b64.encode('ascii') if PY2 else subject_b64

    try:
        s = smtplib.SMTP(config.get('email', 'host'), config.getint('email', 'port'), timeout=config.getint('email', 'timeout'))
        s.sendmail(fro, to, msg.as_string() if PY2 else msg.as_string().encode('utf-8'))
        s.quit()
    except Exception:
        logger.error(traceback.format_exc())
        return False

    return True
