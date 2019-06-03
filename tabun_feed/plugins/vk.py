#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import sys
import json
import time

if sys.version_info.major == 2:
    text = unicode
    import urllib2 as urequest
    def quote(s):
        return urequest.quote(text(s).encode('utf-8'))
else:
    text = str
    import urllib.request as urequest
    quote = urequest.quote

from .. import core


targets = {}


class App(object):
    def __init__(self, access_token=None, v='5.40'):
        if access_token is None and core.config.has_option('vk', 'access_token'):
            access_token = core.config.get('vk', 'access_token')
        self.access_token = access_token
        self.v = v

    def api(self, method_name, args, method="POST", timeout=30):
        args = dict(args)
        link = "https://api.vk.com/method/" + quote(method_name)
        if self.access_token and 'access_token' not in args:
            args['access_token'] = self.access_token
        if self.v and 'v' not in args:
            args['v'] = self.v

        params = ''

        for key, data in args.items():
            if isinstance(data, (list, tuple)):
                data = ','.join(text(x) for x in data)
            else:
                data = text(data)
            params += quote(key) + '=' + quote(data) + '&'
        params = params[:-1]

        if method == "GET":
            link += "?" + params

        if sys.version_info.major == 2:
            link = link.encode('utf-8')

        req = urequest.Request(link, method)

        if method == "POST":
            req.data = params.encode('utf-8')

        for _ in range(10):
            try:
                resp = urequest.urlopen(req, timeout=timeout)
                break
            except IOError as exc:
                if 'handshake operation' not in text(exc):
                    raise
                time.sleep(2)
        data = resp.read()

        try:
            answer = json.loads(data.decode('utf-8'))
        except Exception:
            answer = {"error": {
                "error_code": 0,
                "error_msg": "Unparsed VK answer",
                "data": data
            }}

        return answer


def get_photo_url(photo, max_level='photo_2560', levels=('photo_2560', 'photo_1280', 'photo_807', 'photo_604', 'photo_130', 'photo_75')):
    try:
        pos = levels.index(max_level)
    except Exception:
        return

    for x in levels[pos:]:
        url = photo.get(x)
        if url:
            return url


def parse_vk_targets(line):
    line = [x.strip() for x in line.split(';') if x and ':' in x]
    result = {}
    for target in line:
        # blog1,blog2,blog3: owner_id, prefix
        blogs, owner = [x.split(',') for x in target.split(':')]
        blogs = [x.strip() for x in blogs if x.strip()]

        # Пустой owner_id — значит не постить посты из этих блогов
        if owner[0].strip() == '_':
            owner = ['0']
        owner_id = int(owner[0])
        prefix = None
        if len(owner) > 1:
            prefix = owner[1].strip()

        if not prefix:
            if owner_id < 0:
                prefix = "public" + str(-owner_id)
            else:
                prefix = "id" + str(owner_id)

        for blog in blogs:
            if owner_id:
                result[blog] = {'id': owner_id, 'prefix': prefix}
            else:
                result[blog] = None

    return result


def init_tabun_plugin():
    global targets
    if core.config.has_option('vk', 'targets'):
        targets = parse_vk_targets(core.config.get('vk', 'targets'))
