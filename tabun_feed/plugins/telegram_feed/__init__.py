#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import time

import tabun_api as api
from tabun_api.compat import PY2, text
from telegram import ParseMode

from tabun_feed import core, worker, user

if PY2:
    from Queue import Queue, Empty as QueueEmpty
    from urllib2 import quote
else:
    from queue import Queue, Empty as QueueEmpty
    from urllib.parse import quote


# config
default_target = None  # type: Union[int, str, None]
allowed_closed_blogs = {'NSFW'}  # TODO: настройка списка допустимых закрытых блогов

# variables
tg = core.load_plugin('tabun_feed.plugins.telegram')
posts_queue = Queue()


def tg_html_escape(s):
    s = s.replace('&', '&amp;')
    s = s.replace('<', '&lt;')
    s = s.replace('>', '&gt;')
    s = s.replace('"', '&quot;')
    return s


def build_body(post):
    # type: (api.Post) -> text
    fmt = api.utils.HTMLFormatter()

    # Заголовок
    tg_body = '<b>{}</b>\n'.format(tg_html_escape(post.title.strip()))

    # Блог и информация об авторе под заголовком
    tg_body += '#{} (<a href="{}{}">{}</a>)'.format(
        quote(post.blog or 'blog').replace('-', '_'),

        post.context['http_host'],
        '/profile/{}/'.format(quote(post.author)),
        tg_html_escape(post.author),
    )

    # Собственно текст поста (перед катом)
    post_body = fmt.format(post.body, with_cutted=False)[:8200]
    if len(post_body) >= 8200:
        post_body = post_body[:post_body.rfind(' ')] + '… ->'

    while '\n\n\n' in post_body:
        post_body = post_body.replace('\n\n\n', '\n\n')

    if post_body.endswith('\n====='):
        post_body = post_body[:-6]

    tg_body += '\n\n'
    tg_body += tg_html_escape(post_body)

    return tg_body.strip()


def process_new_post(tm, target, post, full_post=None, extra_params=None):
    # type: (float, Union[int, str], api.Post, Optional[api.Post], Optional[Dict[str, Any]]) -> None
    # Работает в отдельном потоке

    # Скачиваем профиль автора для анализа
    # TODO: спискота известных юзеров, чтоб время на скачивание не тратить
    for i in range(10):
        try:
            author = user.anon.get_profile(post.author)
            break
        except api.TabunError as exc:
            if i >= 9 or exc.code == 404:
                raise
            core.logger.warning('telegram_feed: get author profile error: %s', exc.message)
            time.sleep(3)

    tg_body = build_body(post or full_post)

    with_attachments = True  # TODO: заюзать
    with_link = True  # TODO: выпилить?

    # Защищаемся от бризюкового понева
    n = (full_post or post).body
    if author.rating < 30.0:
        with_attachments = False
    if post.private:
        with_link = False
        with_attachments = False
    elif not with_attachments and (n.xpath('//img') or n.xpath('//embed') or n.xpath('//object') or n.xpath('//iframe')):
        # Таким образом прячется картинка из превьюшки ссыки
        with_link = False

    tg_body += '\n\n' + post.url
    with_link = False  # А зачем превьюшка в телеграме-то?

    # Постим
    # (TODO: обработка ошибок)
    tg.dispatcher.bot.send_message(
        chat_id=target,
        text=tg_body,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=not with_link,
    )


def new_post(post, full_post=None):
    # type: (api.Post, Optional[api.Post]) -> None

    tm = time.time()

    if post.private and post.blog not in api.halfclosed and post.blog not in allowed_closed_blogs:
        core.logger.debug('telegram_feed: post %d is closed', post.post_id)
        return

    add_post_to_queue(post, full_post, tm=tm)


def add_post_to_queue(post=None, full_post=None, tm=None, extra_params=None):
    # type: (Optional[api.Post], Optional[api.Post], Optional[float], Optional[Dict[str, Any]]) -> None

    if tm is None:
        tm = time.time()

    post = post or full_post
    if post is None:
        core.logger.error('telegram_feed: add_post_to_queue received empty post, this is a bug')
        return

    posts_queue.put((tm, default_target, post, full_post, extra_params))
    core.logger.debug('telegram_feed: post %d added to queue', post.post_id)
    return True


# Основной поток, запускающий постилку постов из очереди


def new_post_thread():
    while True:
        # Достаём пост из очереди
        try:
            # tm: float
            # target: Union[int, str]
            # post: api.Post
            # full_post: Optional[api.Post]
            # extra_params: Optional[Dict[str, Any]]
            tm, target, post, full_post, extra_params = posts_queue.get(timeout=2)
        except QueueEmpty:
            if worker.quit_event.is_set():
                break
            continue

        # Пашем
        notify_msg = None
        try:
            with worker.status:
                process_new_post(tm, target, post, full_post, extra_params=extra_params)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            worker.fail()
            core.logger.debug('telegram_feed: post %d failed', post.post_id)
            notify_msg = 'Внутренняя ошибка сервера: {}'.format(text(exc))

        if notify_msg is not None:
            # TODO: нормальное исключение, из которого можно достать причину ошибки
            nbody = 'Не удалось запостить пост ' + post.url
            if notify_msg:
                nbody += '\n' + notify_msg
            core.notify(nbody)
            del nbody

        # Немного спим между постами, чтоб не флудить
        worker.quit_event.wait(10)


def init_tabun_plugin():
    global default_target

    if not core.config.has_option('telegram_feed', 'channel') or not core.config.get('telegram_feed', 'channel'):
        return
    default_target = core.config.get('telegram_feed', 'channel')

    core.logger.debug('telegram_feed started')
    core.logger.debug('default target: %s', default_target)

    worker.add_handler('start', start)
    worker.add_handler('new_post', new_post)


def start():
    worker.start_thread(new_post_thread)
