#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from .. import worker, user


talk_handler = None
process_talk = False


def update_unread_count():
    worker.status['talk_unread'] = user.user.talk_unread


def handler_raw_data(url, raw_data):
    # предполагается скачивание данных через user.open_with_check,
    # а там всегда вызывается update_userinfo
    if process_talk:
        return
    if user.user.talk_unread == worker.status['talk_unread']:
        return

    old_unread = worker.status['talk_unread']
    worker.status['talk_unread'] = user.user.talk_unread
    if talk_handler and user.user.talk_unread > old_unread:
        worker.call_handlers('_talk_new')


def set_talk_handler(func):
    # В обработчиках тоже стоит использовать open_with_check,
    # чтобы talk_unread обновлялось само
    # И не забывайте update_unread_count()
    global talk_handler
    if talk_handler:
        raise ValueError('Conflict')
    talk_handler = func

    def decorator():
        global process_talk
        process_talk = True
        try:
            func()
        finally:
            process_talk = False

    worker.add_handler('_talk_new', decorator)


def init_tabun_plugin():
    worker.status['talk_unread'] = 0
    worker.add_handler('raw_data', handler_raw_data)
