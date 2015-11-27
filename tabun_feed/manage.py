#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import readline  # pylint: disable=unused-import
from threading import Thread, Event

from tabun_feed.remote_connection import RemoteConnection

try:
    from ConfigParser import RawConfigParser
except ImportError:
    from configparser import RawConfigParser

PY2 = sys.version_info.major == 2

config = RawConfigParser(allow_no_value=True)

modes = {}

py_prompt = None
py_event = Event()


def print_help():
    modes_str = ', '.join(sorted(modes.keys()))
    print('Modes: {}'.format(modes_str))


def tail_log(client, args):
    client.send({'cmd': 'subscribe', 'items': ['log', 'status']})
    while True:
        packet = client.wait()
        if packet is None:
            break
        if packet.get('error'):
            print(packet)
            break
        if packet.get('cmd') == 'log':
            for x in packet.get('lines', []):
                print(x)
        elif packet.get('cmd') == 'status':
            pass  # print(packet)


def raw_connect(client, args):
    Thread(target=raw_read_thread, args=(client,)).start()
    while not client.closed:
        try:
            if PY2:
                data = raw_input().decode('utf-8')
            else:
                data = input()
        except (EOFError, KeyboardInterrupt, SystemError):
            break
        if not data:
            break
        client.send(json.loads(data))
    client.close()


def raw_read_thread(client):
    while not client.closed:
        print(client.wait())


def python(client, args):
    import getpass
    passwd = getpass.getpass('Password: ')
    client.send({'cmd': 'python', 'password': passwd})
    Thread(target=python_thread, args=(client,)).start()
    while not client.closed:
        py_event.wait()
        py_event.clear()
        if client.closed:
            break
        try:
            if PY2:
                data = raw_input(py_prompt or '>>> ').decode('utf-8')
            else:
                data = input(py_prompt or '>>> ')
        except KeyboardInterrupt:
            client.send({'cmd': 'python', 'interrupt': True})
            continue
        except EOFError:
            data = None

        client.send({'cmd': 'python', 'data': data})
    client.close()


def python_thread(client):
    global py_prompt
    while not client.closed:
        data = client.wait()
        if data is None:
            continue
        if data.get('cmd') == 'python_write' and data.get('data') is not None:
            sys.stdout.write(data['data'].encode('utf-8') if PY2 else data['data'])
        elif data.get('cmd') == 'python_input':
            py_prompt = data.get('prompt')
            py_event.set()
        elif data.get('cmd') == 'python_closed':
            client.close()
            break
        elif data.get('error'):
            print(data)
            client.close()
            break
        else:
            print(data)

    py_prompt = None
    py_event.set()


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    if not args:
        print_help()
        return

    config_path = 'config.cfg'
    for x in tuple(args):
        if x.startswith('--config='):
            config_path = x[9:]
            args.remove(x)
            break
    config.read(config_path)

    if not config.has_option('tabun_feed', 'remote_bind'):
        print('Cannot find bind in config')
        return

    client = RemoteConnection()
    client.connect(config.get('tabun_feed', 'remote_bind'))

    try:
        cmd = args.pop(0)
        if cmd in modes:
            modes[cmd](client, args)
        else:
            print('Unknown command {}'.format(cmd))
    except (KeyboardInterrupt, SystemExit):
        print()
    finally:
        client.close()


modes['log'] = tail_log
modes['raw'] = raw_connect
modes['python'] = python
