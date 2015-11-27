#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time

from .. import core, user, worker
from ..db import db


tic = 4
user_list = None


def reader():
    global tic, user_list
    tic += 1
    if tic < 5:
        return  # Пользователи появляются редко, не тратим время зазря
    tic = 0

    users = user.user.get_people_list(order_by='user_id', order_way='desc')

    if core.loglevel == core.logging.DEBUG:
        core.logger.debug('Downloaded %d new users, last 5: %r', len(users), users[:5])

    users.reverse()

    if user_list is None:
        user_list = [x[0] for x in db.query('select username from new_users')]

    for new_user in users:
        if new_user.username in user_list:
            continue
        worker.call_handlers("new_user", new_user)
        db.execute(
            'insert into new_users values(?, ?)',
            (new_user.username, int(time.time()))
        )
        user_list.append(new_user.username)
    db.commit()

    worker.call_handlers("users_list", users)


def init_tabun_plugin():
    db.init_table('new_users', '(username char(32) not null primary key, grabbed_at int not null)')
    worker.add_reader(reader)
