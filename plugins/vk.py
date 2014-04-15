#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import json
import vk_api

class VKApp:
    access_token = None
    owner_id = 0
    prefix = None
    jd = None
    lock = vk_api.lock
    def __init__(self, access_token, owner_id=0, prefix=None):
        self.access_token = access_token
        self.owner_id = int(owner_id)
        
        if not prefix:
            if owner_id < 0:
                prefix = "public" + str(-owner_id)
            else:
                prefix = "id" + str(owner_id)
        
        self.prefix = prefix
        
        self.jd = json.JSONDecoder()
    
    def api(self, method_name, args):
        return vk_api.api(method_name, args, token=self.access_token, timeout=2)

def init_tabun_plugin(tabun_feed):
    global VK
    VK = VKApp(tabun_feed.config.get("token"), tabun_feed.config.get("owner_id", 0), tabun_feed.config.get("prefix"))
