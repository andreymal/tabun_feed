#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import json
import vk_api

class VKApp:
    access_token = None
    owner_id = 0
    prefix = None
    targets = {}
    jd = None
    lock = vk_api.lock
    
    def __init__(self, access_token, owner_id, prefix, targets):
        self.access_token = access_token
        self.owner_id = int(owner_id)
        self.targets = targets
        
        #if not prefix:
        #    if owner_id < 0:
        #        prefix = "public" + str(-owner_id)
        #    else:
        #        prefix = "id" + str(owner_id)
        
        self.prefix = prefix
        
        self.jd = json.JSONDecoder()
    
    def api(self, method_name, args):
        return vk_api.api(method_name, args, token=self.access_token, timeout=3)


def parse_vk_targets(targets):
    targets = [x.strip() for x in targets.split(';') if x and ':' in x and ',' in x]
    def_owner_id = 0
    def_prefix = None
    result = {}
    for target in targets:
        blogs, owner = [x.split(',') for x in target.split(':')]
        blogs = [x.strip() for x in blogs if x.strip()]
        owner_id, prefix = int(owner[0]), owner[1].strip()

        if not prefix:
            if owner_id < 0:
                prefix = "public" + str(-owner_id)
            else:
                prefix = "id" + str(owner_id)

        for blog in blogs:
            if blog == '_':
                def_owner_id = owner_id
                def_prefix = prefix
            else:
                result[blog] = owner_id, prefix

    return def_owner_id, def_prefix, result

def init_tabun_plugin(tabun_feed):
    global VK
    VK = VKApp(tabun_feed.config.get("token"), *parse_vk_targets(tabun_feed.config.get("vk_targets", u"").decode("utf-8", "replace")))
