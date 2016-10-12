#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import time

from . import worker
from .remote_server import remote_command


@remote_command('get_status')
def cmd_get_status(packet, client):
    if not isinstance(packet.get('items'), list):
        return {'error': 'Invalid items'}
    items = [str(x) for x in packet['items']]

    if items == ['all']:
        result = worker.status.get_json_status()
    else:
        result = {}
        for key in items:
            result[key] = worker.status.get_json_key(key)

    return {'cmd': 'status', 'status': result}


@remote_command('set_status')
def cmd_set_status(packet, client):
    key = packet.get('key')
    if key not in worker.status.editables:
        return {'error': 'This status is not editable'}
    if 'value' not in packet:
        return {'error': 'Value is not defined'}
    worker.status[key] = packet['value']
    return {'cmd': 'set_status_ok', 'value': worker.status[key]}


@remote_command('ping')
def cmd_ping(packet, client):
    return {'cmd': 'pong', 'time': time.time()}


@remote_command('failures')
def cmd_failures(packet, client):
    try:
        offset = max(0, int(packet.get('offset', 0)))
        count = max(0, min(500, int(packet.get('count', 20))))
    except Exception:
        return {'error': 'Invalid parameters'}
    return {'cmd': 'failures', 'failures': worker.get_failures(offset, count)}


@remote_command('get_failure')
def cmd_get_failure(packet, client):
    try:
        fail_id = int(packet['id'])
    except ValueError:
        return {'error': 'Invalid id'}
    return {'cmd': 'failure', 'failure': worker.get_failure(fail_id)}


@remote_command('solve_failure')
def cmd_solve_failure(packet, client):
    try:
        fail_id = int(packet['id'])
    except ValueError:
        return {'error': 'Invalid id'}
    worker.solve_failure(fail_id)
    return {'cmd': 'ok'}
