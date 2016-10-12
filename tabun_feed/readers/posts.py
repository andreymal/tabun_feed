#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time

from tabun_api.compat import text

from .. import core, worker, user
from ..db import db, get_db_last, set_db_last


def reader():
    last_post_time = get_db_last('last_post_time')

    # скачиваем посты
    posts, pages = load_posts(last_post_time)

    if core.loglevel == core.logging.DEBUG:
        core.logger.debug('Downloaded posts: %s', ', '.join(text(x.post_id) for x in posts))

    new_posts = []

    # подгружаем из базы информацию о последних постах
    post_infos = get_posts_info(x.post_id for x in posts)
    oldest_post_time = get_db_last('oldest_post_time')
    new_oldest_post_time = None
    new_last_post_time = None

    status_changed = False

    for post in posts:
        # слишком старые посты игнорируем
        tm = time.mktime(post.time)
        if tm < oldest_post_time:
            continue

        if new_oldest_post_time is None:
            new_oldest_post_time = tm
        if new_last_post_time is None or tm > new_last_post_time:
            new_last_post_time = tm

        # проверяем, был ли обработан этот пост
        short_hash, full_hash = post_infos.get(post.post_id, (None, None))
        if short_hash or full_hash:
            # пост уже был обработан
            new_short_hash = post.hashsum()
            if new_short_hash != short_hash:
                # Упс, пост изменили
                if not post.short:
                    full_post = post
                elif worker.status['request_full_posts'] and post.short:
                    full_post = (user.user if post.private else user.anon).get_post(post.post_id, post.blog)
                    if post.vote_total is not None and full_post.vote_total is None:
                        full_post.vote_total = post.vote_total
                        full_post.vote_count = post.vote_count
                else:
                    full_post = None
                new_full_hash = full_post.hashsum() if full_post else 'N/A'
                set_post_info(post.post_id, tm, new_short_hash, new_full_hash)
                worker.call_handlers('edit_post', post, full_post)
            continue

        if not post.short:
            full_post = post
        elif worker.status['request_full_posts'] and post.short:
            full_post = (user.user if post.private else user.anon).get_post(post.post_id, post.blog)
            if post.vote_total is not None and full_post.vote_total is None:
                full_post.vote_total = post.vote_total
                full_post.vote_count = post.vote_count
        else:
            full_post = None

        short_hash = post.hashsum()
        full_hash = full_post.hashsum() if full_post else 'N/A'

        set_post_info(post.post_id, tm, short_hash, full_hash)

        # отправляем в другой поток на обработку
        worker.call_handlers("new_post", post, full_post)
        if not status_changed:
            worker.status['iter_last_with_post'] = worker.status['iter']
            status_changed = True
        new_posts.append((post, full_post))

        if worker.status['request_full_posts'] and post.short:
            time.sleep(2)  # не DDoS'им

    # Для плагинов, желающих обработать все новые посты в одном обработчике
    worker.call_handlers("new_posts", new_posts)

    if core.loglevel == core.logging.DEBUG:
        core.logger.debug('New posts: %s', ', '.join(text(x[0].post_id) for x in new_posts))

    # стираем слишком старые посты
    if new_oldest_post_time is not None and new_oldest_post_time != oldest_post_time:
        set_db_last('oldest_post_time', new_oldest_post_time)
        clear_post_info_older(new_oldest_post_time)

    if new_last_post_time is not None and new_last_post_time != last_post_time:
        set_db_last('last_post_time', new_last_post_time)

    worker.call_handlers("posts_list", posts)

    # считалка постов, ушедших в сервис-зону и восставших из черновиков
    # old_page = load_page_cache('posts')
    # new_page = [x.post_id for x in posts]
    # posts_dict = dict(((x.post_id, x) for x in posts))

    # if old_page == new_page:
    #     return

    # added, removed, restored, displaced = calc_page_diff(old_page, new_page)
    # print old_page
    # print new_page
    # print added, removed, restored, displaced, time.strftime("%Y-%m-%d %H:%M:%S")

    # save_page_cache('posts', new_page)


def load_posts(last_post_time=None):
    """Скачивалка постов согласно конфигурации. Попутно качает список блогов, если имеется."""
    urls = [x.strip() for x in core.config.get('posts', 'urls').split(',') if x.strip()]
    raw_posts = []
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
            raw_data = user.open_with_check(current_url)
            worker.call_handlers('raw_data', current_url, raw_data)

            posts = user.user.get_posts(url, raw_data=raw_data)
            raw_posts.extend(posts)
            if page_num < 2:
                pages.append(posts)

            # не качаем то, что качать не требуется
            if last_post_time and time.mktime(posts[0].time) < last_post_time:
                # ^ посты отсортированы в API по времени в прямом порядке
                break

    post_ids = []
    posts = []
    for post in sorted(raw_posts, key=lambda x: x.time):
        if post.post_id not in post_ids:
            # ^ исключаем возможные дубликаты (ориентируемся по айдишникам, а не содержимому целиком)
            posts.append(post)
            post_ids.append(post.post_id)

    return posts, pages


def get_posts_info(post_ids):
    """Возвращает словарь хэшей постов (один хэш - пост до ката, второй хэш - пост целиком). Хэши могут быть None."""
    query = ', '.join(text(int(x)) for x in post_ids)
    hashes = db.query("select post_id, short_hash, full_hash from posts where post_id in (%s)" % query)
    return dict((x[0], x[1:]) for x in hashes)


def set_post_info(post_id, tm, short_hash, full_hash):
    """Сохраняет хэши поста. Время поста нужно передавать для последующей чистки базы."""
    db.execute("replace into posts values(?, ?, ?, ?)", (int(post_id), int(tm), short_hash, full_hash))


def clear_post_info_older(tm):
    """Чистит базу от слишком старых постов, чтобы место не забивать."""
    db.execute('delete from posts where tm < ?', (int(tm),))


def update_status(key, old, new):
    core.logger.debug('status: %s (%s -> %s)', key, old, new)


def init_tabun_plugin():
    db.init_table('posts', '(post_id int not null primary key, tm int not null, short_hash text default null, full_hash text default null)')

    if not worker.status['request_full_posts']:
        worker.status['request_full_posts'] = core.config.getboolean('posts', 'request_full_posts')
    worker.status['iter_last_with_post'] = 0
    worker.add_reader(reader)
