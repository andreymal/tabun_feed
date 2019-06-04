#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import time

from tabun_api.compat import PY2, text

from tabun_feed import core, db

if PY2:
    from Queue import Queue, Empty as QueueEmpty
else:
    from queue import Queue, Empty as QueueEmpty


class FeedQueueItem(object):
    def __init__(self, post, full_post=None, extra_params=None, tm=None):
        if not post:
            raise ValueError('Post is required')
        self.tm = float(tm) if tm is not None else time.time()
        self.post = post  # type: api.Post
        self.full_post = full_post  # type: Optional[api.Post]
        self.extra_params = dict(extra_params or {})  # type: Dict[str, Any]


class FeedQueue(object):
    def __init__(self):
        self._queue = Queue()

    # low level api

    def put(self, item):
        if not isinstance(item, FeedQueueItem):
            raise TypeError
        self._queue.put(item)

    def get(self, block=True, timeout=None):
        return self._queue.get(block, timeout)

    def has_post(self, post_id):
        # Здесь никто не отменял гонку, так что это просто защита от дурака
        for item in list(self._queue):
            post = item.full_post or item.post
            if post == post_id:
                return True
        return False

    # high level api

    def add_post(self, post=None, full_post=None, tm=None, extra_params=None):
        # type: (Optional[api.Post], Optional[api.Post], Optional[float], Optional[Dict[str, Any]]) -> None

        post = post or full_post
        if post is None:
            core.logger.error('telegram_feed: add_post_to_queue received empty post, this is a bug')
            return

        item = FeedQueueItem(
            post=post,
            full_post=full_post,
            extra_params=extra_params,
            tm=tm,
        )

        self._queue.put(item)
        core.logger.debug('telegram_feed: post %d added to queue', post.post_id)
