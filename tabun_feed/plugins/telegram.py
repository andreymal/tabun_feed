#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import sys

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
# from telegram.ext.dispatcher import Dispatcher
# from telegram.message import Message
# from telegram.update import Update
# from telegram.ext.callbackcontext import CallbackContext

from tabun_feed import core, worker

if sys.version_info.major == 2:
    text = unicode
else:
    text = str

# config

read_updates = True
log_incoming_messages = True
fallback_message_text = 'Прости, я не знаю, что ответить.'  # type: Optional[str]

# variables
message_handlers = {0: [], 1: [], 2: []}
updater = None  # type: Optional[Updater]
dispatcher = None  # type: Optional[Dispatcher]


# public api


def add_message_handler(handler, priority=1):
    message_handlers[priority].append(handler)


def add_command_handler(command, handler):
    assert dispatcher is not None
    h = CommandHandler(command, handler)
    dispatcher.add_handler(h)


# worker


def default_message_handler(update, context):
    # type: (Update, CallbackContext) -> None
    if fallback_message_text:
        context.bot.send_message(chat_id=update.message.chat_id, text=fallback_message_text)


def message_handler(update, context):
    # type: (Update, CallbackContext) -> None
    try:
        message = update.message  # type: Optional[Message]
        if message is None:
            # it means update.edited_message is not None
            return

        if log_incoming_messages:
            core.logger.info(
                'Telegram message from %s (%s): %s',
                message.from_user.username if message.from_user else None,
                message.from_user.id if message.from_user else None,
                message.text,
            )

        for priority in [0, 1, 2]:
            for handler in message_handlers[priority]:
                if handler(update, context):
                    return
        default_message_handler(update, context)

    except Exception:
        worker.fail()


# init


def start_telegram():
    if updater is None:
        return
    if read_updates:
        core.logger.info('Starting Telegram updater thread')
        updater.start_polling()
    else:
        core.logger.info('Telegram started (updater thread is disabled)')


def stop_telegram():
    if updater is None:
        return
    if read_updates:
        core.logger.info('Stopping Telegram updater thread...')
        updater.stop()
    core.logger.info('Telegram stopped')


def init_tabun_plugin():
    global updater, dispatcher, fallback_message_text, read_updates, log_incoming_messages

    if not core.config.has_option('telegram', 'bot_token') or not core.config.get('telegram', 'bot_token'):
        return

    if core.config.has_option('telegram', 'read_updates'):
        read_updates = core.config.getboolean('telegram', 'read_updates')
    if core.config.has_option('telegram', 'log_incoming_messages'):
        log_incoming_messages = core.config.getboolean('telegram', 'log_incoming_messages')

    bot_token = text(core.config.get('telegram', 'bot_token'))
    updater = Updater(token=bot_token, use_context=True)
    dispatcher = updater.dispatcher

    h = MessageHandler(Filters.all, message_handler)
    dispatcher.add_handler(h)

    worker.add_handler('start', start_telegram)
    worker.add_handler('stop', stop_telegram)

    if core.config.has_option('telegram', 'fallback_message_text'):
        fallback_message_text = (
            text(core.config.get('telegram', 'fallback_message_text'))
            if core.config.get('telegram', 'fallback_message_text')
            else None
        )
