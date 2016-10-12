#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import tabun_api as api

from . import core, worker


user = None
anon = None
last_requests = []


def auth(username=None, password=None, session_id=None, http_host=None, session_cookie_name=None, query_interval=None, timeout=None):
    """Проходит авторизацию на табуне по указанным параметрам и возвращает пользователя.
    Число попыток не ограничено: подвиснет интернет, упадёт сайт — функция дождётся, пока всё починится.
    Может вернуть None, если в процессе авторизации процесс решили остановить.
    """
    if query_interval is None:
        query_interval = core.config.getfloat('tabun_feed', 'query_interval')
    if timeout is None:
        timeout = core.config.getfloat('tabun_feed', 'timeout')

    u = None
    errors = 0

    # авторизация пользователя
    while not worker.quit_event.is_set():
        try:
            u = None
            # при наличии session_id логиниться, возможно, и не надо
            if session_id and password:
                tmpuser = api.User(session_id=session_id)
                if tmpuser.username:
                    if username == tmpuser.username:
                        core.logger.info('Fast login %s!', username)
                        u = tmpuser
                    del tmpuser

            if u is None:
                u = api.User(
                    session_id=session_id,
                    login=username,
                    passwd=password,
                    http_host=http_host,
                    session_cookie_name=session_cookie_name or 'TABUNSESSIONID',
                )
            break
        except api.TabunError as exc:
            core.logger.error("user %s auth error: %s", username, exc.message)
            errors += 1
            # избегаем удара банхаммером от fail2ban
            if errors % 3 == 0 or 'пароль' in exc.message:
                worker.quit_event.wait(60)
            else:
                worker.quit_event.wait(5)

    if not u:
        return

    if query_interval is not None:
        u.query_interval = query_interval
    if timeout is not None:
        u.timeout = timeout
    return u


def auth_global():
    """Проходит авторизацию на табуне с указанными в конфигурации параметрами и записывает результат в переменную user.
    Число попыток не ограничено: подвиснет интернет, упадёт сайт — функция дождётся, пока всё починится.
    Попутно создаёт анонима для запроса постов через него, чтобы не сбивать подсветку комментариев.
    После завершения вызывает группу обработчиков relogin_user (если процесс не решили остановить).
    """
    global user, anon
    user = auth(
        session_id=core.config.get('tabun_feed', 'session_id'),
        username=core.config.get('tabun_feed', 'username'),
        password=core.config.get('tabun_feed', 'password'),
        http_host=core.config.get('tabun_feed', 'http_host'),
        session_cookie_name=core.config.get('tabun_feed', 'session_cookie_name'),
    )
    if user is None:
        return

    if not user.username:
        anon = user
    else:
        anon = auth(
            http_host=core.config.get('tabun_feed', 'http_host'),
            session_cookie_name=core.config.get('tabun_feed', 'session_cookie_name'),
        )
    if anon is None:
        return

    core.logger.info("Logged in as %s", user.username or '[anonymous]')
    worker.call_handlers("relogin_user")


def open_with_check(url, timeout=None):
    """Загружает URL, попутно проверяя, что авторизация на месте, и перелогиниваясь при её слёте.
    Если за 10 попыток скачать не получилось, кидает исключение.
    Если за 60 попыток не получилось залогиниться, возвращает то что есть.
    """

    # Забираем таймеры из конфига (вызов тут, а не при запуске, позволяет
    # менять таймеры налету через удалённое управление)
    max_tries = max(1, core.config.getint('tabun_feed', 'tries_if_unauthorized'))
    max_error_tries = max(1, core.config.getint('tabun_feed', 'tries_if_error'))

    raw_data = None
    tries = 0
    # узнаём, можем ли мы вообще перелогиниться
    can_auth = core.config.get('tabun_feed', 'username') and core.config.get('tabun_feed', 'password')

    # Делаем вторую и последующую попытки, пока:
    # 1) tabun_feed не выключили (первую попытку всё равно качаем);
    # 2) Попыток меньше шестидеяти (по умолчанию; вроде достаточный срок,
    #    чтобы лежачий мускуль Табуна успевал проболеть);
    # 3) Мы не авторизованы, если в конфиге прописана авторизация.
    while raw_data is None or (not worker.quit_event.is_set() and tries < max_tries and can_auth and user.update_userinfo(raw_data) is None):
        if raw_data is not None:
            # если мы попали сюда, то нас разлогинило
            worker.status['request_error'] = 'need relogin'
            # перелогиниваться не торопимся, а то забанит fail2ban
            if tries > 1:
                worker.quit_event.wait(30)
            if worker.quit_event.is_set():
                break

            # перелогиниваемся
            try:
                user.login(core.config.get('tabun_feed', 'username'), core.config.get('tabun_feed', 'password'))
            except api.TabunError as exc:
                worker.status['request_error'] = exc.message
                core.logger.warning("Relogin error: %s", exc.message)
            else:
                core.logger.info("Re logged in as %s", user.username or '[anonymous]')

        tries += 1
        # скачиваем (несколько попыток)
        for i in range(max_error_tries):
            # бросаем всё, если нужно завершить работу
            if raw_data is not None and worker.quit_event.is_set():
                break

            try:
                raw_data = user.urlread(url, timeout=timeout)
                if worker.status['request_error']:
                    worker.status['request_error'] = ''
                break  # залогиненность проверяем в условии while
            except api.TabunError as exc:
                worker.status['request_error'] = exc.message
                # после последней попытки или при выходе сдаёмся
                if i >= max_error_tries - 1 or worker.quit_event.is_set():
                    raise
                worker.quit_event.wait(3)

    if raw_data:
        user.update_userinfo(raw_data)

    return raw_data
