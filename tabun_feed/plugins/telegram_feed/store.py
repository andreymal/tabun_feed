#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import time

from tabun_api.compat import text

from tabun_feed import db


# Статусы:
# всё успешно сделано
OK = 0
# ещё в очереди лежит (храним для восстановления состояния очереди после перезапуска бота)
PENDING = 1
# закрытый блог (или черновик, но это уже не норма), ничего не сделано
CLOSED = -1
# какая-то ошибка, будет повторная попытка в будущем
FAILED = -2
# не получилось скачать пост с Табуна, ничего не сделано
NODOWNLOAD = -3
# какая-то ошибка была много раз подряд, будет повтор только после перезапуска бота
PENDING_FAILED = -4
# не запостилось из-за упора в лимит (для телеграма ещё не реализовано)
LIMIT = -5
# пост отфильтрован пользовательскими правилами, ничего не сделано (ещё не реализовано)
FILTERED = -6
# пост проигнорирован по иным причинам (тоже ещё не реализовано)
IGNORED = -7


# Значение по умолчанию для аргументов save_post_status;
# означает не обновлять значение такого-то столбца в базе
# (если его в базе нет, то будет создано значение по умолчанию)
KEEP = object()


def check_db():
    # type: () -> None

    db.db.init_table('tg_posts', '''(
        post_id int not null primary key,
        processed_at int not null,
        status int not null,
        status_text text default null,
        process_duration int not null,
        tg_chat_id int default null,
        tg_message_id int default null
    )''')


def get_rows_by_status(statuses):
    # type: (Iterable[int]) -> List[tuple]
    statuses = tuple(set(statuses))
    if not statuses:
        return []
    rows = db.db.query((
        'select post_id, processed_at, status, status_text, process_duration, tg_chat_id, tg_message_id '
        'from tg_posts where status in ({})'
    ).format(', '.join('?' for _ in statuses)), statuses)
    return rows


def save_post_status(
    post_id,
    processed_at=KEEP,
    status=KEEP,
    status_text=KEEP,
    process_duration=KEEP,
    tg_chat_id=KEEP,
    tg_message_id=KEEP,
    commit=True,
):
    post_id = int(post_id)
    exists = bool(db.db.query('select post_id from tg_posts where post_id = ?', (post_id,)))

    args = {}  # type: Dict[str, Any]

    if processed_at is not KEEP:
        args['processed_at'] = int(processed_at if processed_at is not None else int(time.time()))
    elif not exists:
        args['processed_at'] = int(time.time())

    if status is not KEEP:
        args['status'] = int(status)
    elif not exists:
        args['status'] = PENDING

    if status_text is not KEEP:
        args['status_text'] = text(status_text) if status_text else None
    elif not exists:
        args['status_text'] = None

    if process_duration is not KEEP:
        args['process_duration'] = int(process_duration)
    elif not exists:
        args['process_duration'] = 0

    if tg_chat_id is not KEEP:
        args['tg_chat_id'] = int(tg_chat_id) if tg_chat_id is not None else None
    elif not exists:
        args['tg_chat_id'] = None

    if tg_message_id is not KEEP:
        args['tg_message_id'] = int(tg_message_id) if tg_message_id is not None else None
    elif not exists:
        args['tg_message_id'] = None

    if not args:
        return

    args_fields = []  # type: List[text]
    args_values = []  # type: List[text]

    if exists:
        sql = 'update tg_posts set {0} where post_id = ?'
        for k, v in args.items():
            args_fields.append('{} = ?'.format(k))
            args_values.append(v)
        args_values.append(post_id)
        sql = sql.format(', '.join(args_fields))

    else:
        sql = 'insert into tg_posts ({0}) values ({1})'
        for k, v in args.items():
            args_fields.append(k)
            args_values.append(v)
        args_fields.append('post_id')
        args_values.append(post_id)
        sql = sql.format(', '.join(args_fields), ', '.join('?' for _ in args_values))

    db.db.execute(sql, tuple(args_values))
    if commit:
        db.db.commit()
