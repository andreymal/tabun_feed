#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time

import tabun_api as api

from .. import core, user, worker
from ..db import db, get_db_last, set_db_last


def pack_item(item):
    # надёжного красивого способа сохранить и сравнить активность нет, поэтому костыляем
    return '%d\x00%d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00' % (
        item.type, time.mktime(item.date),
        item.post_id, item.comment_id,
        item.blog, item.username,
        item.title, item.data,
    )


def reader():
    last_id = get_db_last("activity_id")
    last_loaded_id = None
    first_loaded_id = None
    items = []
    new_items = []

    # ограничиваем число загрузок на случай багов
    for i in range(50):
        if i >= 49:
            core.logger.error("Infinity activity loading! Break.")
            break

        # качаем активность
        if last_loaded_id is None:
            last_loaded_id, raw_items = user.user.get_activity()
        else:
            try:
                last_loaded_id, raw_items = user.user.get_more_activity(last_loaded_id)
            except api.TabunError as exc:
                core.logger.warning("Activity loading error: %s", exc)
                break
        items.extend(raw_items)

        # запоминаем самый свежий айдишник
        if first_loaded_id is None:
            first_loaded_id = last_loaded_id

        # выходим, если точно скачали всё новое
        # (можно и + 20, но пусть будет десяток про запас)
        if not last_id or not raw_items or last_loaded_id <= last_id + 10:
            break

    # подгружаем кэш, с которым будем сравнивать активность
    last_items = [x[0] for x in db.execute('select data from last_activity').fetchall()]

    # выбираем только новую активность
    new_items = []
    for item in items:
        if pack_item(item) in last_items:
            break
        new_items.append(item)
    if not new_items:
        return
    new_items = reversed(new_items)

    for item in new_items:
        worker.call_handlers("new_activity", item)

    worker.call_handlers("activity_list", items)

    # сохраняем кэш активности (10 штук про запас, ибо активность может пропадать, например, с удалёными постами)
    with db:
        db.execute('delete from last_activity')
        for item in items[:10]:
            db.execute('insert into last_activity values(?)', (pack_item(item),))
        set_db_last("activity_id", max(first_loaded_id, last_loaded_id, last_id))


def init_tabun_plugin():
    db.init_table('last_activity', '(data text not null)')
    worker.add_reader(reader)
