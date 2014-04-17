#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import time

code = r"""var count = -1;
var users = [];
while(count == -1 || users.length < count){
    var members = API.groups.getMembers( {"group_id": "%GROUP_ID%", "sort": "time_asc", "count": 1000, "offset": users.length} );
    if(members.users.length == 0) return users;
    count = members.count;
    users = users + API.users.get( {"user_ids": members.users, "fields": "online"} );
}
return users;"""

tabun_feed = None
VK = None

req = 14
def load(urls):
    global req
    req += 1
    if req < 15: return
    req = 0
    
    users = VK.api("execute", {"code": code, "v": "3.0"})
    #print users
    users = users.get('response')
    if not users: return
    online = 0
    for user in users:
        if user['online']: online += 1
    tabun_feed.console.set("vk_online", str(online) + " / " + str(len(users)))
    tabun_feed.db.execute("insert into vk_onlines values(?, ?, ?)", (int(time.time()), online, len(users)) )  

def init_db():
    tables = tabun_feed.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = map(lambda x:x[0], tables)
    
    if not u"vk_onlines" in tables:
        tabun_feed.db.execute("create table vk_onlines(time int, online int, count int)")

def init_tabun_plugin(tf):
    global  VK, tabun_feed, code
    tabun_feed = tf
    VK = tabun_feed.require("vk").VK
    if VK.owner_id >= 0: return
    code = code.replace("%GROUP_ID%", str(abs(VK.owner_id)))
    init_db()
    tabun_feed.add_handler("load", load)
