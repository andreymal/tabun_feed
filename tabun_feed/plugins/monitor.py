#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from tabun_feed import core, worker, user


def new_post(post, full_post):
    core.logger.info('New post %d: %s', (full_post or post).post_id, (full_post or post).title)


def edit_post(post, full_post):
    core.logger.info('Edited post %d: %s', (full_post or post).post_id, (full_post or post).title)


def new_comment(comment):
    core.logger.info('New comment %d/%d', comment.post_id, comment.comment_id)


def edit_comment(comment):
    core.logger.info('Edited comment %d/%d', comment.post_id, comment.comment_id)


def new_blog(blog):
    core.logger.info('New blog %s', blog.name)


def new_user(ppl):
    core.logger.info('New user %s', ppl.username)


def init_tabun_plugin():
    worker.add_handler('new_post', new_post)
    worker.add_handler('edit_post', edit_post)
    worker.add_handler('new_comment', new_comment)
    worker.add_handler('edit_comment', edit_comment)
    worker.add_handler('new_blog', new_blog)
    worker.add_handler('new_user', new_user)
