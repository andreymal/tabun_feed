#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import os
import time
import socket
import logging
import threading

from tabun_api.compat import text, PY2

if PY2:
    from Queue import Queue, Empty as QueueEmpty
else:
    from queue import Queue, Empty as QueueEmpty

from . import core, worker, remote_connection


server = None


class RemoteControlHandler(logging.Handler):
    # FIXME: it is too dirty
    _lock = None
    onemit = None

    def emit(self, record):
        if self._lock is None:
            self._lock = threading.RLock()

        with self._lock:
            msg = logging.root.handlers[0].format(record)
            if self.onemit:
                self.onemit(msg)


class RemoteClient(remote_connection.RemoteConnection):
    commands = {}

    def __init__(self, *args, **kwargs):
        super(RemoteClient, self).__init__(*args, **kwargs)
        self.authorized = False
        self.subscriptions = []

    @classmethod
    def add_command(cls, command, func):
        command = text(command)
        if command in cls.commands:
            raise RuntimeError('Conflict')
        cls.commands[command] = func

    def close(self):
        super(RemoteClient, self).close()
        server.client_onclose(self)

    def process_client(self):
        try:
            while not self.closed:
                packet = self.wait()
                if packet is None:
                    break

                result = self.process_packet(packet)
                if result is not None:
                    self.send(result)
        except:
            worker.fail()

    def process_packet(self, packet):
        if packet is None:
            return

        # Авторизация при необходимости
        if not self.authorized:
            if not server.password or packet.get('authorize') == server.password:
                self.authorized = True
            else:
                return {'error': 'Authorization required'}

        # ищем команду
        cmd = packet.get('cmd')
        if not cmd:
            return {'error': 'No command'}
        cmd = text(cmd)

        func = self.commands.get(cmd)
        if func is None:
            func = getattr(self, 'cmd_' + cmd, None)

        if func is None:
            return {'error': 'Unknown command `{}`'.format(cmd)}

        # и выполняем её
        try:
            result = func(packet, self)
        except:
            worker.fail()
            return {'error': 'Internal server error'}

        if isinstance(result, (list, tuple)) and result:
            return {'cmd': 'many', 'items': result} if len(result) > 1 else result[0]
        return result

    def cmd_subscribe(self, packet, client):
        if not isinstance(packet.get('items'), list):
            return {'error': 'Invalid items'}
        if len(self.subscriptions) + len(packet['items']) > 1000:
            return {'error': 'Too many items'}

        subs = set(text(x) for x in packet['items'] if x in ('log', 'status'))

        for x in subs:
            if x not in client.subscriptions:
                client.subscriptions.append(x)

        packets = [{'cmd': 'subscribed', 'items': client.subscriptions}]

        if 'status' in packet['items']:
            packets.append({'cmd': 'status', 'status': worker.status.get_json_status()})

        if 'log' in packet['items']:
            packets.append({'cmd': 'log', 'lines': server.get_log_buffer()})

        return packets


class RemoteServer(object):
    def __init__(self, addr, password=None, unix_mode=0o770):
        self.password = password or None
        self.typ, self.addr = remote_connection.parse_addr(addr)

        self.lock = threading.Lock()
        self.clients = []

        if self.typ == 'unix':
            if os.path.exists(self.addr):
                # Процесс после смерти может не прибрать за собой UNIX-сокет
                s = socket.socket(socket.AF_UNIX)
                try:
                    s.connect(self.addr)
                except:
                    # Не подключились — прибираем самостоятельно
                    os.remove(self.addr)
                else:
                    # Ой, процесс жив ещё — bind ниже выкинет исключение
                    s.close()
                del s

            self.sock = socket.socket(socket.AF_UNIX)
            self.sock.bind(self.addr)
            os.chmod(self.addr, unix_mode)
            # TODO: chown, chgrp
        self.sock.listen(64)

        self.log_handler = RemoteControlHandler()
        self.log_handler.onemit = self.onemit
        core.logger.addHandler(self.log_handler)
        self.nolog = False
        self._log_buffer = []

        self._pubsub_queue = Queue()

    def __del__(self):
        self.close()

    def onupdate(self, key, old_value, new_value):
        if key != 'event_id':
            self.send_pubsub('status', {key: new_value})

    def onemit(self, msg):
        if self.nolog:
            return
        try:
            self.send_pubsub('log', msg)
        except:
            self.nolog = True  # избегаем рекурсии
            try:
                worker.fail()
            finally:
                self.nolog = False

    def get_log_buffer(self, count=50):
        return self._log_buffer[-count:]

    def close(self, tm=None):
        if not self.sock:
            return

        self.close_pubsub()

        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        self.sock = None

        for c in self.clients:
            c.close()
        self.clients = []

        if self.typ == 'unix' and os.path.exists(self.addr):
            os.remove(self.addr)
        if self.log_handler:
            core.logger.removeHandler(self.log_handler)
            self.log_handler = None

    def client_onclose(self, client):
        if client.closed and client in self.clients:
            self.clients.remove(client)
            worker.status.add('clients_count', -1)

    def run(self):
        while self.sock is not None and not worker.quit_event.is_set():
            try:
                csock = self.sock.accept()[0]
            except:
                continue
            c = RemoteClient()
            c.accept(csock)
            self.clients.append(c)
            worker.status.add('clients_count', 1)
            threading.Thread(target=c.process_client).start()

    def pubsub_thread(self):
        while not worker.quit_event.is_set():
            # ожидаем новые события
            try:
                item = self._pubsub_queue.get()
            except QueueEmpty:
                continue

            if item is None:
                break

            # ожидаем новые события ещё чуть-чуть, чтобы отослать всё одним пакетом
            items = [item]
            tm = time.time()
            while time.time() - tm < 0.05:
                try:
                    item = self._pubsub_queue.get(timeout=0.03)
                except QueueEmpty:
                    break
                if item is None:
                    break
                items.append(item)

            # собираем события в один пакет
            status = {}
            log_lines = []
            for name, value in items:
                if name == 'status':
                    status.update(value)
                elif name == 'log':
                    log_lines.append(value)

            with self.lock:
                # собираем буфер для свежеподключившихся клиентов
                if self.log_handler:
                    for x in log_lines:
                        self._log_buffer.append(x)
                    if len(self._log_buffer) > 300:
                        self._log_buffer = self._log_buffer[-250:]

                # и рассылаем
                if not self.clients:
                    continue

                for x in self.clients:
                    data = []
                    if status and 'status' in x.subscriptions:
                        data.append({'cmd': 'status', 'status': status})
                    if log_lines and 'log' in x.subscriptions:
                        data.append({'cmd': 'log', 'lines': log_lines})
                    if len(data) > 1:
                        x.send({'cmd': 'many', 'items': data})
                    elif data:
                        x.send(data[0])

    def send_pubsub(self, name, data):
        self._pubsub_queue.put((name, data))

    def close_pubsub(self):
        self._pubsub_queue.put(None)


def remote_command(cmd):
    def decorator(func):
        RemoteClient.add_command(cmd, func)
        return func
    return decorator


def start():
    global server
    # читаем конфиг
    if not core.config.has_option('tabun_feed', 'remote_bind') or not core.config.get('tabun_feed', 'remote_bind'):
        core.logger.info('Remote control is disabled')
        return

    bind = core.config.get('tabun_feed', 'remote_bind')
    if core.config.has_option('tabun_feed', 'remote_password'):
        password = core.config.get('tabun_feed', 'remote_password')
    else:
        password = None

    # стартуем сервер и цепляем необходимые обработчики
    unix_mode = '770'
    if core.config.has_option('tabun_feed', 'remote_unix_mode'):
        unix_mode = core.config.get('tabun_feed', 'remote_unix_mode')
    if len(unix_mode) == 3:
        try:
            unix_mode = int(unix_mode, 8)
        except ValueError:
            unix_mode = 0o770
    else:
        unix_mode = 0o770
    server = RemoteServer(bind, password, unix_mode=unix_mode)
    worker.add_handler('exit', server.close)
    worker.add_handler("update_status", server.onupdate)
    worker.status['clients_count'] = 0

    # этот поток получает данные от клиентов
    worker.start_thread(server.run)

    # этот поток нужен, чтобы собирать кучку идущих подряд сообщений в один пакет
    # и отсылать всё разом, а не флудить кучей мелких пакетов
    worker.start_thread(server.pubsub_thread)
