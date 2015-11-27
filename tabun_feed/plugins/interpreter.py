#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import sys
from io import StringIO, BytesIO
from code import InteractiveConsole

from tabun_api.compat import PY2, text, binary

from tabun_feed import core, worker, db, user
from tabun_feed.remote_server import remote_command


clients = {}


class AsyncConsole(InteractiveConsole):
    def __init__(self, locals=None, filename="<console>", client=None):
        InteractiveConsole.__init__(self, locals, filename)
        self.client = client

    def interact_async(self, banner=None):
        try:
            sys.ps1
        except AttributeError:
            sys.ps1 = ">>> "
        try:
            sys.ps2
        except AttributeError:
            sys.ps2 = "... "
        cprt = 'Type "help", "copyright", "credits" or "license" for more information.'
        if banner is None:
            self.write("Python %s on %s\n%s\n(%s)\n" %
                       (sys.version, sys.platform, cprt,
                        InteractiveConsole.__name__))
        elif banner:
            self.write("%s\n" % text(banner))
        more = 0
        while 1:
            try:
                if more:
                    prompt = sys.ps2
                else:
                    prompt = sys.ps1
                try:
                    line = yield prompt
                except EOFError:
                    self.write("\n")
                    break
                else:
                    more = self.push(line)
            except KeyboardInterrupt:
                self.write("\nKeyboardInterrupt\n")
                self.resetbuffer()
                more = 0

    def write(self, data):
        if isinstance(data, binary):
            data = data.decode('utf-8', 'replace')
        elif not isinstance(data, text):
            data = text(data)
        if self.client is None:
            sys.stderr.write(data.encode('utf-8') if PY2 else data)
        else:
            self.client.send({'cmd': 'python_write', 'data': data})


def onclose(client):
    if client in clients:
        del clients[client]


@remote_command('python')
def cmd_python(packet, client):
    if client in clients:
        if packet.get('interrupt'):
            prompt = clients[client].interpreter.throw(KeyboardInterrupt())
            return {'cmd': 'python_input', 'prompt': prompt}

        data = packet.get('data')
        if not isinstance(data, text):
            try:
                clients[client].interpreter.throw(EOFError())
            except StopIteration:
                pass
            del clients[client]
            return {'cmd': 'python_closed'}

        # FIXME: this shit is not thread-safety
        old_stdout = sys.stdout
        try:
            s = BytesIO() if PY2 else StringIO
            sys.stdout = s
            prompt = clients[client].interpreter.send(data)
        finally:
            sys.stdout = old_stdout

        clients[client].write(s.getvalue())
        return {'cmd': 'python_input', 'prompt': prompt}

    passwd = None
    if core.config.has_option('tabun_feed', 'remote_console_password'):
        passwd = text(core.config.get('tabun_feed', 'remote_console_password'))
    if not passwd:
        return {'error': 'unavailable'}
    if packet.get('password') != passwd:
        return {'error': 'unauthorized'}

    new_locals = {
        'core': core,
        'worker': worker,
        'db': db,
        'user': user,
        'sys': None
    }

    client.add_onclose_handler(onclose)
    clients[client] = AsyncConsole(locals=new_locals, client=client)
    clients[client].interpreter = clients[client].interact_async()
    prompt = clients[client].interpreter.send(None)
    return {'cmd': 'python_input', 'prompt': prompt}
