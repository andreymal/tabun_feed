#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from .. import core, user, worker
from ..db import db, get_db_last, set_db_last


tic = 9


def reader():
    global tic
    tic += 1
    if tic < 10:
        return  # Блоги появляются редко, не тратим время зазря
    tic = 0

    blogs = user.user.get_blogs_list(order_by='blog_id', order_way='desc')

    if core.loglevel == core.logging.DEBUG:
        core.logger.debug('Downloaded %d blogs, last 5: %r', len(blogs), blogs[:5])

    last_blog_id = get_db_last('blog_id')
    new_last_id = None

    for blog in blogs:
        if blog.blog_id <= last_blog_id:
            break  # сортировка по айдишнику в обратном порядке
        worker.call_handlers("new_blog", blog)
        if new_last_id is None:
            new_last_id = blog.blog_id

    if new_last_id is not None:
        set_db_last('blog_id', new_last_id)
        db.commit()

    worker.call_handlers("blogs_list", blogs)


def init_tabun_plugin():
    worker.add_reader(reader)
