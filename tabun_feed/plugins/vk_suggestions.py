#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time

from tabun_feed import core, worker


vk_plug = core.load_plugin('tabun_feed.plugins.vk')
vk = None

targets = tuple(set(-x['id'] for x in vk_plug.targets.values() if x['id'] < 0))
iter_current = -1
last_posts = []


def reader():
    global iter_current
    iter_current += 1

    n = (iter_current * 2) % len(targets)
    for i, group_id in enumerate(targets[n:n + 2]):
        if i > 0:
            time.sleep(0.4)
        worker.status['vk_suggestions'] = 'Processing {}'.format(group_id)
        try:
            process_suggestions(group_id)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            worker.fail()
        worker.status['vk_suggestions'] = ''


def process_suggestions(group_id):
    global last_posts

    try:
        result = vk.api(
            'wall.get',
            {'owner_id': -group_id, 'offset': 0, 'count': 100, 'extended': 1, 'filter': 'suggests'}
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        core.logger.warning('VK Suggestions %d fail: %s', group_id, exc)
        return

    if not result.get('response'):
        return

    profiles = {x['id']: x for x in result['response'].get('profiles', [])}
    profiles.update({-x['id']: x for x in result['response'].get('groups', [])})

    if len(last_posts) > 150:
        last_posts = last_posts[-100:]

    posts = result['response'].get('items', [])
    for post in posts:
        post_id = (post['owner_id'], post['id'])
        if post_id in last_posts:
            continue
        last_posts.append(post_id)
        notify_post(post, profiles)


def notify_post(post, profiles=None):
    profiles = profiles or {}

    # Информация о паблике
    owner_id = post['owner_id']
    if owner_id in profiles:
        owner_name = profiles[owner_id].get('name') or profiles[owner_id].get('screen_name', '')
    else:
        owner_name = str(owner_id)

    if owner_id in profiles and profiles[owner_id].get('screen_name'):
        owner_link = 'https://vk.com/' + profiles[owner_id].get('screen_name')
    else:
        owner_link = 'https://vk.com/public{}'.format(-owner_id)

    # Информация об авторе предложенной новости
    user_id = post['from_id']
    if user_id in profiles:
        user_name = profiles[user_id].get('name') or '{} {}'.format(profiles[user_id].get('first_name'), profiles[user_id].get('last_name'))
    else:
        user_name = str(user_id)

    if user_id in profiles and profiles[user_id].get('screen_name'):
        user_link = 'https://vk.com/' + profiles[user_id].get('screen_name')
    else:
        user_link = 'https://vk.com/id{}'.format(user_id)

    # Собираем прикрепления к посту
    attachments = []

    for att in post.get('attachments', []):
        if att.get('type') == 'photo':
            attachments.append('Фотография {}'.format(vk_plug.get_photo_url(att['photo'])))
        else:
            attachments.append('Прикрепление {}'.format(att.get('type')))

    # Собираем и отправляем уведомление
    msg = '''{owner_name} {owner_link} — новая предложенная новость

{body}

{attachments}

{user_name} {user_link}'''.format(
        owner_id=owner_id,
        owner_name=owner_name,
        owner_link=owner_link,
        body=post.get('text', ''),
        user_id=user_id,
        user_name=user_name,
        user_link=user_link,
        attachments='\n'.join(attachments),
    )

    core.notify(msg)


def init_tabun_plugin():
    global vk
    if not targets or not core.config.has_option('vk', 'access_token'):
        core.logger.warning('VK is not available; vk_suggestions disabled')
        return
    vk = vk_plug.App()
    worker.status['vk_suggestions'] = ''
    worker.add_reader(reader)
