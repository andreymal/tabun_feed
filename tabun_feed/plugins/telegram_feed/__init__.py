#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import time

import tabun_api as api
from tabun_api.compat import PY2, text
from telegram import ParseMode
# from telegram.message import Message

from tabun_feed import core, worker, user, db
from tabun_feed.plugins.telegram_feed.queue import FeedQueueItem, queue
from tabun_feed.plugins.telegram_feed import utils as tg_utils

if PY2:
    from Queue import Empty as QueueEmpty
else:
    from queue import Empty as QueueEmpty


# config
default_target = None  # type: Union[int, str, None]
allowed_closed_blogs = {'NSFW'}  # TODO: настройка списка допустимых закрытых блогов

# variables
tg = core.load_plugin('tabun_feed.plugins.telegram')

# Здесь хранится число попыток постинга. При превышении определённого значения
# попытки прекращаются, чтобы не дудосить телеграм зазря
# Сбрасывается при перезапуске бота
post_tries = {}  # type: Dict[int, int]
max_post_tries = 5


# Функция, делающая основную работу (работает в отдельном потоке)


def process_new_post(item):
    # type: (FeedQueueItem) -> None

    target = default_target
    assert target is not None

    # Скачиваем профиль автора для анализа
    # TODO: спискота известных юзеров, чтоб время на скачивание не тратить
    worker.status['telegram_feed'] = 'Getting post author'
    author = tg_utils.get_post_author(item.post.author)

    worker.status['telegram_feed'] = 'Process post'

    with_attachments = True
    with_link = True  # TODO: выпилить?

    n = item.post.body
    if author.rating < 30.0:
        # Защищаемся от бризюкового понева
        with_attachments = False

    if item.post.private:
        # Не палим награнный контент из (полу)закрытых блогов
        with_link = False
        with_attachments = False

    if not with_attachments and with_link and (
        n.xpath('//img') or n.xpath('//embed') or
        n.xpath('//object') or n.xpath('//iframe')
    ):
        # Таким образом прячется картинка из превьюшки ссылки
        with_link = False

    with_link = False  # А зачем превьюшка в телеграме-то?

    # Крепим фоточку
    photo_url = None  # type: Optional[text]
    if with_attachments:
        photo_url = tg_utils.build_photo_attachment(item.post, item.full_post)

    # Собираем текст поста со всем оформлением
    # (его содержимое зависит от наличия или отсутствия фоточки)
    tg_body = tg_utils.build_body(item.post, short=photo_url is not None)
    tg_body += '\n\n' + item.post.url

    worker.status['telegram_feed'] = 'Sending post'

    # Постим
    result = None  # type: Optional[Message]
    if photo_url:
        try:
            # Делаем попытку оптравки с фоточкой
            result = tg.dispatcher.bot.send_photo(
                chat_id=target,
                photo=photo_url,
                caption=tg_body,
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            # Если не получилось — попробуем второй раз без фоточки
            core.logger.warning('telegram_feed: cannot send post with photo: %s', exc)
            time.sleep(1)

    if result is None:
        result = tg.dispatcher.bot.send_message(
            chat_id=target,
            text=tg_body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=not with_link,
        )

    assert result is not None


# Обработчики событий


def new_post(post, full_post=None):
    # type: (api.Post, Optional[api.Post]) -> None

    if post.private and post.blog not in api.halfclosed and post.blog not in allowed_closed_blogs:
        core.logger.debug('telegram_feed: post %d is closed', post.post_id)
        return

    if post.draft:
        core.logger.debug('telegram_feed: post %d is draft', post.post_id)
        return

    queue.add_post(post, full_post)


def new_blog(blog):
    # с блогами со всякими там очередями и надёжностями не церемонимся, потребность в этом не особо есть
    worker.status['telegram_feed'] = 'Sending blog'
    target = default_target

    if blog.status != api.Blog.OPEN:
        tg_body = 'Новый закрытый блог: ' + tg_utils.html_escape(blog.name)
    else:
        tg_body = 'Новый блог: ' + tg_utils.html_escape(blog.name)
    tg_body += '\n#' + blog.blog.replace('-', '_')
    tg_body += '\n\n' + blog.url

    notify = False
    try:
        # Постим
        # (TODO: нормальная обработка ошибок)
        tg.dispatcher.bot.send_message(
            chat_id=target,
            text=tg_body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception:
        notify = True

    if notify:
        core.notify('Не удалось запостить новый блог: ' + blog.url)

    worker.status['vk'] = None


# Основной поток, запускающий постилку постов из очереди


def new_post_thread():
    while not worker.quit_event.is_set():
        # Достаём пост из очереди
        try:
            item = queue.get()  # type: Optional[FeedQueueItem]
        except QueueEmpty:
            continue

        if item is None:
            # None обычно пихается при выключении бота, так что continue может выйти из цикла
            continue

        # Пашем
        post_id = item.post.post_id
        worker.status['telegram_feed_post'] = post_id
        notify_msg = None
        post_tries[post_id] = post_tries.get(post_id, 0) + 1

        try:
            with worker.status:
                process_new_post(item)

        except (KeyboardInterrupt, SystemExit):
            raise

        except Exception as exc:
            worker.fail()
            core.logger.debug('telegram_feed: post %d failed', item.post.post_id)
            notify_msg = 'Внутренняя ошибка сервера: {}'.format(text(exc))

        finally:
            worker.status['telegram_feed_post'] = None
            worker.status['telegram_feed'] = None

        # Если что-то сломалось или отменилось, то уведомляем админа
        if notify_msg is not None:
            # TODO: нормальное исключение, из которого можно достать причину ошибки
            nbody = 'Не удалось запостить пост ' + item.post.url
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

    worker.status['telegram_feed'] = None
    worker.status['telegram_feed_post'] = None

    worker.add_handler('start', start)
    worker.add_handler('stop', stop)
    worker.add_handler('new_post', new_post)
    worker.add_handler('new_blog', new_blog)


def start():
    worker.start_thread(new_post_thread)


def stop():
    queue.put(None)
