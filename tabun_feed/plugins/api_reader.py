#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time
from threading import RLock

import tabun_api as api
from tabun_api.compat import text, binary
from tabun_feed import worker


default_send_request = None
default_get_posts = None
default_get_comments = None
default_get_profile = None

lock = RLock()
last_requests = []


def normalize_url(user, url):
    if isinstance(url, (text, binary)):
        norm_url = url if isinstance(url, text) else url.decode('utf-8', 'replace')
    else:
        norm_url = url.get_full_url()
    if norm_url.startswith('/'):
        norm_url = (user.http_host or api.http_host) + norm_url
    return norm_url


def patched_send_request(user, request, *args, **kwargs):
    global last_requests

    url = request.get_full_url()
    if isinstance(url, binary):
        url = url.decode('utf-8', 'replace')

    http_host = user.http_host or api.http_host
    if url.startswith(http_host):
        url = url[len(http_host):]

    with lock:
        tm = time.time()
        last_requests = [x for x in last_requests if x[0] > time.time() - 60]
        last_requests.append((tm, url))
        worker.status['last_requests'] = '\n'.join(text(x[0]) + ' ' + text(x[1]) for x in last_requests)

    worker.status['request_counter'] += 1
    try:
        worker.status['request_now'] = url
        return default_send_request(user, request, *args, **kwargs)
    finally:
        if worker.status['request_now'] == url:
            worker.status['request_now'] = None


def patched_get_posts(user, url='/index/newall/', raw_data=None):
    posts = default_get_posts(user, url, raw_data)
    worker.call_handlers_here('request_posts', normalize_url(user, url), posts)
    return posts


def patched_get_comments(user, url='/comments/', raw_data=None):
    comments = default_get_comments(user, url, raw_data)
    worker.call_handlers_here('request_comments', normalize_url(user, url), comments)
    return comments


def patched_get_profile(user, username=None, raw_data=None):
    profile = default_get_profile(user, username, raw_data)
    worker.call_handlers_here('request_profile', profile)
    return profile


def init_tabun_plugin():
    global default_send_request, default_get_posts, default_get_comments, default_get_profile

    worker.status['request_counter'] = 0
    worker.status['request_now'] = None
    worker.status['last_requests'] = ''

    default_send_request = api.User.send_request
    default_get_posts = api.User.get_posts
    default_get_comments = api.User.get_comments
    default_get_profile = api.User.get_profile

    api.User.send_request = patched_send_request
    api.User.get_posts = patched_get_posts
    api.User.get_comments = patched_get_comments
    api.User.get_profile = patched_get_profile
