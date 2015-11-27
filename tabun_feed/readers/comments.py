#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time

import tabun_api as api
from tabun_api.compat import text

from .. import core, user, worker
from ..db import db, get_db_last, set_db_last


def reader():
    last_comment_time = get_db_last('last_comment_time')

    # скачиваем комментарии
    comments, pages = load_comments(last_comment_time)

    if core.loglevel == core.logging.DEBUG:
        core.logger.debug('Downloaded %d comments, last 10: %s', len(comments), ", ".join(text(x.comment_id) for x in comments[-10:]))
    
    new_comments = []  # только для отладки

    comment_infos = get_comments_info(x.comment_id for x in comments)

    oldest_comment_time = get_db_last('oldest_comment_time')
    new_oldest_comment_time = None
    new_last_comment_time = None

    for comment in comments:
        tm = time.mktime(comment.time)
        # слишком старые комментарии игнорируем
        if tm < oldest_comment_time:
            continue

        if new_oldest_comment_time is None:
            new_oldest_comment_time = tm
        if new_last_comment_time is None or tm > new_last_comment_time:
            new_last_comment_time = tm

        comment_hash = comment_infos.get(comment.comment_id, None)
        if comment_hash:
            # комментарий уже был обработан
            continue

        set_comment_info(comment.comment_id, tm, 'hash')  # TODO: посчитать хэш

        # отправляем в другой поток на обработку
        if comment.deleted:
            worker.call_handlers("new_deleted_comment", comment)
        else:
            worker.call_handlers("new_comment", comment)
        if core.loglevel == core.logging.DEBUG:
            new_comments.append(comment.comment_id)

    if core.loglevel == core.logging.DEBUG:
        core.logger.debug('New comments: %s', ', '.join(text(x) for x in new_comments))

    # стираем слишком старые комментарии
    if new_oldest_comment_time is not None and new_oldest_comment_time != oldest_comment_time:
        set_db_last('oldest_comment_time', new_oldest_comment_time)
        clear_comment_info_older(new_oldest_comment_time)

    if new_last_comment_time is not None and new_last_comment_time != last_comment_time:
        set_db_last('last_comment_time', new_last_comment_time)

    worker.call_handlers("comments_list", comments)


def load_comments(last_comment_time=None):
    """Скачивалка комментариев согласно конфигурации."""
    urls = [x.strip() for x in core.config.get('comments', 'urls').split(',') if x.strip()]
    raw_comments = []
    pages = []

    for url in urls:
        # узнаём, сколько страниц нам разрешено качать
        if '#' in url:
            url, pages_count = url.split('#', 1)
            pages_count = max(1, int(pages_count))
        else:
            pages_count = 1

        for page_num in range(1, pages_count + 1):
            current_url = (url.rstrip('/') + ('/page%d/' % page_num)) if page_num > 1 else url
            # комменты грузятся ОЧЕНЬ долго:
            raw_data = user.open_with_check(current_url, timeout=max(120, user.user.timeout))
            worker.call_handlers('raw_data', current_url, raw_data)

            comments = sorted(user.user.get_comments(url, raw_data=raw_data).values(), key=lambda x: x.time)
            raw_comments.extend(comments)
            if page_num < 2:
                pages.append(comments)

            # не качаем то, что качать не требуется
            if last_comment_time and time.mktime(comments[0].time) < last_comment_time:
                break

    comment_ids = []
    comments = []
    for comment in sorted(raw_comments, key=lambda x: x.time):
        if comment.comment_id not in comment_ids:
            comments.append(comment)
            comment_ids.append(comment.comment_id) 

    return comments, pages


def get_comments_info(comment_ids):
    """Возвращает словарь хэшей комментариев. Хэши не могут быть None, в отличие от постов."""
    query = ', '.join(text(int(x)) for x in comment_ids)
    hashes = db.query("select comment_id, hash from comments where comment_id in (%s)" % query)
    return dict((x[0], x[1:]) for x in hashes)


def set_comment_info(comment_id, tm, comment_hash):
    """Сохраняет хэш комментария. Время нужно передавать для последующей чистки базы."""
    db.execute("replace into comments values(?, ?, ?)", (int(comment_id), int(tm), comment_hash))


def clear_comment_info_older(tm):
    """Чистит базу от слишком старых комментариев, чтобы место не забивать."""
    db.execute('delete from comments where tm < ?', (int(tm),))


def init_tabun_plugin():
    db.init_table('comments', '(comment_id int not null primary key, tm int not null, hash text not null)')
    worker.add_reader(reader)
