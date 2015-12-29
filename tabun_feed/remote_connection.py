#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json
import socket
import threading


class RemoteConnection(object):
    def __init__(self):
        """Базовый класс для клиентов с используемым в tabun_feed протоколом."""
        self._sock = None
        self._closed = True
        self._parser = None

        self._lock = threading.Lock()
        self._handlers = []

    def __del__(self):
        self.close()

    @property
    def sock(self):
        return self._sock
    
    @property
    def closed(self):
        return self._closed
    
    @property
    def lock(self):
        return self._lock

    def fileno(self):
        return self._sock.fileno() if self._sock else None

    def connect(self, addr, timeout=None):
        """Подключается к серверу по указанному адресу."""
        with self._lock:
            if not self._closed:
                self.close()
            self._sock = open_socket_by_addr(addr, timeout=timeout)
            self._closed = False
            self._parser = start_parser()
            self._parser.send(None)

    def accept(self, sock):
        """Привязывается и переданному в аргументе сокету."""
        with self._lock:
            if not self._closed:
                self.close()
            self._sock = sock
            self._closed = False
            self._parser = start_parser()
            self._parser.send(None)

    def add_onclose_handler(self, func):
        """Добавляет обработчик отключения клиента."""
        if func not in self._handlers:
            self._handlers.append(func)

    def close(self):
        """Закрывает сокет."""
        if self._closed:
            return
        if self._sock:
            with self._lock:
                self._closed = True
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                    self._sock.close()
                except:
                    pass
                self._sock = None
                if self._parser:
                    try:
                        self._parser.send(None)
                    except StopIteration:
                        pass
                    self._parser = None
        for func in self._handlers:
            func(self)

    def get(self):
        """Возвращает следующий полученный пакет. Берёт из буфера, сокет не трогает.
        Но в случае ошибки при парсинге закрывает соединение и возвращает None.
        """
        if self._closed:
            return
        try:
            return self._parser.send(b'')
        except StopIteration:
            self.close()
            return

    def wait(self, one_pass=False):
        """Возвращает следующий пакет, ожидая его получения при необходимости.
        При one_pass=True чтение из сокета будет произведено однократно, а если
        пакет придёт не целиком, вместо дальнейшего ожидания вернётся None.
        """
        packet = self._parser.send(b'')
        if packet:
            return packet

        while packet is None:
            try:
                data = self._sock.recv(65536)
            except:
                data = b''
            if not data:
                self.close()
                break
            packet = self._parser.send(data)
            if one_pass:
                break

        return packet

    def send(self, packet):
        """Отправляет пакет и возвращает число отправленных байт (в том числе 0 при закрытии соединения или ошибке сокета)."""
        if self.closed:
            return 0
        data = json.dumps(packet).encode('utf-8')
        data = str(len(data)).encode('utf-8') + b'\n' + data + b'\n'
        with self._lock:
            try:
                self._sock.send(data)
            except:
                self.close()
                return 0


def parse_addr(addr):
    if addr.startswith('unix://'):
        return 'unix', addr[7:]
    else:
        # TODO: TCP
        raise ValueError("Invalid addr")


def open_socket_by_addr(addr, timeout=None):
    typ, addr = parse_addr(addr)
    if typ == 'unix':
        sock = socket.socket(socket.AF_UNIX)
        sock.connect(addr)
        if timeout is not None:
            sock.settimeout(timeout)
        return sock


def start_parser():
    buf = b''
    packet_size = None

    jd = json.JSONDecoder()

    while True:
        # 1) Читаем длину пакета
        while b'\n' not in buf:
            data = yield
            if data is None:
                break
            buf += data
        if data is None:
            break
        packet_size, buf = buf.split(b'\n', 1)
        if len(packet_size) > 7 or not packet_size.isdigit():
            break
        packet_size = int(packet_size)

        # 2) Качаем сам пакет
        while len(buf) <= packet_size:
            data = yield
            if data is None:
                break
            buf += data

        # 3) Проверяем целостность
        if buf[packet_size] not in (b'\n', 10):  # py2/3, ага
            break

        # 4) Декодируем пакет
        packet = buf[:packet_size]
        buf = buf[packet_size + 1:]

        try:
            packet = jd.decode(packet.decode('utf-8'))
        except:
            break

        # 5) Отдаём его
        if packet.get('cmd') == 'many' and isinstance(packet.get('items'), list):
            packets = packet['items']
        else:
            packets = [packet]

        ok = True
        for packet in packets:
            if not isinstance(packet, dict):
                ok = False
                break
            data = yield packet
            if data is None:
                break
            buf += data
        if not ok:
            break
