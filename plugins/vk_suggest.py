#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from threading import Thread

tabun_feed = None
notifies = None
VK = None

def suggest_thread():
    tabun_feed.console.stdprint("Suggest thread started")
    while 1:
        try:
            get_suggest()
        except:
            import traceback
            traceback.print_exc()
            
            tabun_feed.quit_event.wait(40)
            if tabun_feed.quit_event.isSet(): break
        else:
            tabun_feed.quit_event.wait(20)
            if tabun_feed.quit_event.isSet(): break

def get_suggest():
    global notifies
    result = VK.api("wall.get", {"owner_id": VK.owner_id, 'offset': 0, 'count': 100, 'filter': 'suggests', 'extended': 1, 'v': '3.0'})
    resp = result.get('response')
    if not resp: return
    
    wall = resp["wall"]
    
    profiles = {}
    for x in resp["profiles"]:
        profiles[x['uid']] = x
    
    newnotifies = []
    
    for x in wall[1:]:
        newnotifies.append(x['id'])
        if notifies is None: continue
        if not x['id'] in notifies:
            prof = profiles[x['from_id']]
            text = x['text'].replace(u'<br>', u'\n')
            data = "Предложенная новость:\n"
            data += prof['first_name'].encode("utf-8") + " " + prof['last_name'].encode("utf-8") + "\n"
            data += text.encode("utf-8")
            tabun_feed.notify(data)
            tabun_feed.quit_event.wait(1)
    
    notifies = newnotifies
    

def init_tabun_plugin(tf):
    global  VK, tabun_feed
    tabun_feed = tf
    VK = tabun_feed.require("vk").VK
    Thread(target=suggest_thread).start()
