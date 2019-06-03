#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time

from tabun_feed import core, worker
from tabun_feed.db import db
from tabun_feed.remote_server import remote_command


vk_plug = core.load_plugin('tabun_feed.plugins.vk')
vk = None

targets = tuple(set(-x['id'] for x in vk_plug.targets.values() if x is not None and x['id'] < 0))

interval = 300
last_align_time = None
iter_current = -1


code = r"""var count = -1;
var users = [];
while((count == -1 || users.length < count - %OFFSET%) && users.length < 10000){
    var members = API.groups.getMembers( {"group_id": "%GROUP_ID%", "sort": "time_asc", "count": 1000, "offset": users.length + %OFFSET%} );
    if(members.items.length == 0) return {"count": count, "users": users};
    count = members.count;
    users = users + API.users.get( {"user_ids": members.items, "fields": "online"} );
}
return {"count": count, "users": users};"""


def reader():
    global last_align_time, iter_current

    iter_current += 1
    new_align_time = int(time.time()) // interval * interval

    if new_align_time - last_align_time < interval:
        n = (iter_current * 2) % len(targets)
        for i, group_id in enumerate(targets[n:n + 2]):
            if i > 0:
                time.sleep(0.4)
            process_if_needed(group_id)
        return

    last_align_time = new_align_time
    core.logger.debug('VK Online: %s %s', time.strftime('%H:%M:%S', time.localtime(last_align_time)), time.strftime('%H:%M:%S'))

    for i, group_id in enumerate(targets):
        if i > 0:
            time.sleep(0.5)
        worker.status['vk_online'] = 'Processing {}'.format(group_id)
        try:
            process_group(group_id)
        except Exception:
            worker.fail()
            core.logger.error('VK Online %d fail', group_id)
        worker.status['vk_online'] = ''

    worker.status['vk_online_last'] = int(time.time())
    db.commit()


def process_if_needed(group_id):
    try:
        result = vk.api('groups.getMembers', {'group_id': group_id, 'count': 0})
    except Exception as exc:
        core.logger.warning('VK Online %d fail: %s', group_id, exc)
        return

    if not result.get('response'):
        return

    old_count = db.query(
        'select count from vk_online where group_id = ? order by time desc limit 1',
        (group_id,)
    )
    old_count = old_count[0][0] if old_count else -1

    count = result['response'].get('count')
    if count is not None and count != old_count:
        time.sleep(0.3)
        worker.status['vk_online'] = 'Processing {}'.format(group_id)
        try:
            process_group(group_id)
        except Exception as exc:
            worker.fail()
            core.logger.error('VK Online %d fail', group_id)
        worker.status['vk_online'] = ''


def process_group(group_id):
    count = -1
    users = []
    usersdict = {}
    queries = 0

    # так как у метода execute ограничение в 25 запросов API, собираем-таки инфу через несколько таких запросов
    while count == -1 or len(users) < count:
        queries += 1
        if queries > 50:
            core.logger.error('VK Online: too many queries! Maybe %d members in group %d is too many for plugin', count, group_id)
            count = -1
            break

        if count != -1:
            time.sleep(0.4)

        # 10 - запас на тех, кто отписался в процессе скачивания
        # (не совсем надёжно, потому что могут отписаться и при
        # выполнении VKScript, но лучше чем ничего)
        offset = max(0, len(users) - 10)
        rcode = code.replace('%GROUP_ID%', str(group_id)).replace('%OFFSET%', str(offset))
        try:
            result = vk.api('execute', {'code': rcode})
        except Exception as exc:
            core.logger.warning('VK Online %d fail: %s', group_id, exc)
            count = -1
            break

        resp = result.get('response')
        if not resp:
            count = -1
            break

        count = resp['count']
        if not resp['users']:
            break
        for x in resp['users']:
            usersdict[x['id']] = x
            if x['id'] not in users:
                users.append(x['id'])

    if count == -1:
        return

    online = len([True for x in usersdict.values() if x['online']])

    # подгружаем сохранённый список участников
    q = db.query('select user_id, leave_time from vk_members where group_id = ?', (group_id,))
    dbusers = [x[0] for x in q]
    dbcurrent = [x[0] for x in q if x[1] is None]
    del q

    if not dbusers:
        core.logger.info('VK Online: init members for group %d', group_id)
        j = 0
        chunklen = 500
        while j * chunklen < len(users):
            chunk = [(group_id, x, None, None) for x in users[j * chunklen:j * chunklen + chunklen]]
            db.executemany('insert into vk_members(group_id, user_id, join_time, leave_time) values(?, ?, ?, ?)', chunk)
            j += 1
        joined = set()
        leaved = set()

    else:
        joined = set(users) - set(dbcurrent)
        leaved = set(dbcurrent) - set(users)
        for x in joined:
            core.logger.debug('join %d (target %d)', x, group_id)
            if x not in dbusers:
                db.execute('insert into vk_members(group_id, user_id, join_time, leave_time) values(?, ?, ?, ?)', (group_id, x, int(time.time()), None))
            elif x not in dbcurrent:
                db.execute('update vk_members set leave_time = ? where group_id = ? and user_id = ?', (None, group_id, x))

        for x in leaved:
            core.logger.debug('leave %d (target %d)', x, group_id)
            if x in dbcurrent:
                db.execute('update vk_members set leave_time = ? where group_id = ? and user_id = ?', (int(time.time()), group_id, x))

    if joined or leaved:
        diff = '|' + '|'.join(['+' + str(x) for x in joined] + ['-' + str(x) for x in leaved]) + '|'
    else:
        diff = None
    db.execute(
        'insert into vk_online(group_id, time, online, count, diff) values(?, ?, ?, ?, ?)',
        (group_id, int(time.time()), online, count, diff)
    )


@remote_command('vk_stat')
def cmd_vk_stat(packet, client):
    try:
        groups = [int(x) for x in packet.get('groups', ())]
    except Exception:
        return {'error': 'Invalid groups'}
    if not groups:
        groups = [x[0] for x in db.query('select distinct group_id from vk_online')]

    try:
        start_time = int(packet['start_time']) if packet.get('start_time') is not None else None
    except Exception:
        return {'error': 'Invalid start time'}

    try:
        end_time = int(packet['end_time']) if packet.get('end_time') is not None else int(time.time())
    except Exception:
        return {'error': 'Invalid end time'}

    result = {}
    for group_id in groups:
        if start_time is None:
            items = reversed(db.query(
                'select time, count, online from vk_online where group_id = ? and time <= ? order by time desc limit 1200',
                (group_id, end_time)
            ))
        else:
            items = db.query(
                'select time, count, online from vk_online where group_id = ? and time >= ? and time <= ? order by time limit 1200',
                (group_id, start_time, end_time)
            )
        result[str(group_id)] = [{
            'time': x[0],
            'count': x[1],
            'online': x[2]
        } for x in items]

    return {'cmd': 'vk_stat', 'groups': result}


def init_tabun_plugin():
    global vk, interval, last_align_time
    if not targets or not core.config.has_option('vk', 'access_token'):
        core.logger.warning('VK is not available; vk_online disabled')
        return
    vk = vk_plug.App()

    if db.init_table('vk_online', '''(
        group_id int not null,
        time int not null,
        online int not null,
        count int not null,
        diff text default null,
        primary key(group_id, time)
    )'''):
        db.execute('create index vk_group on vk_online(group_id)')

    if db.init_table('vk_members', '''(
        id integer primary key autoincrement not null,
        group_id int not null,
        user_id int not null,
        join_time int default null,
        leave_time int default_null
    )'''):
        db.execute('create unique index vk_member on vk_members(group_id, user_id)')

    if core.config.has_option('vk_online', 'query_interval'):
        interval = max(30, core.config.getint('vk_online', 'query_interval'))
    last_align_time = int(time.time()) // interval * interval

    worker.status['vk_online_last'] = db.query('select max(time) from vk_online')[0][0] or None
    worker.status['vk_online'] = ''
    worker.add_reader(reader)
