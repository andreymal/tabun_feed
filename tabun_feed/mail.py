#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import smtplib
import traceback
from email.header import Header
from email.utils import formataddr
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from tabun_api.compat import text, PY2


def smtp_connect():
    from tabun_feed import core

    if core.config.getboolean('email', 'use_ssl'):
        s = smtplib.SMTP_SSL(
            core.config.get('email', 'host'),
            core.config.getint('email', 'port'),
            timeout=core.config.getint('email', 'timeout'),
            keyfile=core.config.get('email', 'ssl_keyfile'),
            certfile=core.config.get('email', 'ssl_certfile'),
        )
    else:
        s = smtplib.SMTP(
            core.config.get('email', 'host'),
            core.config.getint('email', 'port'),
            timeout=core.config.getint('email', 'timeout'),
        )

    if not core.config.getboolean('email', 'use_ssl') and core.config.getboolean('email', 'use_tls'):
        s.ehlo()
        s.starttls(keyfile=core.config.get('email', 'ssl_keyfile'), certfile=core.config.get('email', 'ssl_certfile'))
        s.ehlo()

    if core.config.get('email', 'user'):
        s.login(core.config.get('email', 'user'), core.config.get('email', 'password'))

    return s


def build_email_body(body):
    prep_body = []

    for item in body:
        if isinstance(item, text):
            item = item.encode('utf-8')

        if isinstance(item, bytes):
            # text/plain
            prep_body.append(MIMEText(item, 'plain', 'utf-8'))
            continue

        if isinstance(item, dict):
            # multipart/alternative
            item = item.copy()
            alt = []

            if 'plain' in item:
                # text/plain
                p = item.pop('plain')
                if isinstance(p, text):
                    p = p.encode('utf-8')
                alt.append(MIMEText(p, 'plain', 'utf-8'))

            if 'html' in item:
                # text/html
                p = item.pop('html')
                if isinstance(p, text):
                    p = p.encode('utf-8')
                alt.append(MIMEText(p, 'html', 'utf-8'))

            if item:
                raise NotImplementedError('non-text emails are not implemeneted')

            # build alternative
            if len(alt) == 1:
                m = alt[0]
            else:
                m = MIMEMultipart('alternative')
                for x in alt:
                    m.attach(x)

            prep_body.append(m)
            continue

        if isinstance(item, MIMEBase):
            prep_body.append(item)
            continue

        raise ValueError('Incorrect body type: {}'.format(type(item)))

    if len(prep_body) == 1:
        return prep_body[0]

    m = MIMEMultipart('mixed')
    for x in prep_body:
        m.attach(x)
    return m


def sendmail(to, subject, body, fro=None, headers=None, conn=None):
    '''Отправляет письмо по электронной почте на указанные адреса.

    В качестве отправителя ``fro`` может быть указана как просто почта, так и
    список из двух элементов: имени отправителя и почты.

    Тело письма ``body`` может быть очень произвольным:

    - str или bytes: отправляется простое text/plain письмо;
    - словарь: если элементов больше одного, то будет multipart/alternative,
      если элемент один, то только он и будет:
      - plain: простое text/plain письмо;
      - html: HTML-письмо;
    - что-то наследующееся от MIMEBase;
    - всё перечисленное в списке: будет отправлен multipart/mixed со всем
      перечисленным.

    :param to: получатели (может быть переопределено настройкой
      email.redirect_to)
    :type to: str или list
    :param str subject: тема письма
    :param body: содержимое письма
    :param fro: отправитель (по умолчанию email.from)
    :type fro: str, list, tuple
    :param dict headers: дополнительные заголовки (значения — строки
      или списки)
    :rtype: bool
    '''

    from tabun_feed import core

    if fro is None:
        fro = core.config.get('email', 'from')

    if PY2 and isinstance(fro, str):
        fro = fro.decode('utf-8')

    if not isinstance(fro, text):
        if isinstance(fro, (tuple, list)) and len(fro) == 2:
            # make From: =?utf-8?q?Name?= <e@mail>
            fro = formataddr((Header(fro[0], 'utf-8').encode(), fro[1]))
        else:
            raise ValueError('Non-string from address must be [name, email] list')

    if not core.config.get('email', 'host') or not body:
        return False

    if core.config.get('email', 'redirect_to') is not None:
        if not core.config.getboolean('email', 'dont_edit_subject_on_redirect'):
            subject = '[To: {!r}] {}'.format(to, subject or '').rstrip()
        to = core.config.get('email', 'redirect_to')

    if not isinstance(to, (tuple, list, set)):
        to = [to]

    if not isinstance(body, (list, tuple)):
        body = [body]

    msg = build_email_body(body)

    msg['From'] = fro
    msg['Subject'] = Header(subject, 'utf-8').encode()

    prep_headers = {}
    if headers:
        prep_headers.update(headers)

    for header, value in prep_headers.items():
        if not isinstance(value, (list, tuple, set)):
            value = [value]
        for x in value:
            msg[header] = x

    try:
        close_conn = False
        if not conn:
            conn = smtp_connect()
            close_conn = True

        for x in to:
            del msg['To']
            msg['To'] = x.encode('utf-8') if PY2 and isinstance(x, text) else x
            conn.sendmail(fro, x, msg.as_string() if PY2 else msg.as_string().encode('utf-8'))

        if close_conn:
            conn.quit()
    except Exception:
        core.logger.error(traceback.format_exc())
        return False

    return True
