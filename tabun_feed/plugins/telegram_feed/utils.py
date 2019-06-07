#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import time

import tabun_api as api
from tabun_api.compat import PY2, text

from tabun_feed import core, user, worker

if PY2:
    from urllib2 import quote
else:
    from urllib.parse import quote


def html_escape(s):
    s = s.replace('&', '&amp;')
    s = s.replace('<', '&lt;')
    s = s.replace('>', '&gt;')
    s = s.replace('"', '&quot;')
    return s


def build_body(post, short=False):
    # type: (api.Post) -> text
    fmt = api.utils.HTMLFormatter()

    # Заголовок
    tg_body = '<b>{}</b>\n'.format(html_escape(post.title.strip()))

    # Блог и информация об авторе под заголовком
    tg_body += '#{} (<a href="{}{}">{}</a>)'.format(
        quote(post.blog or 'blog').replace('-', '_'),

        post.context['http_host'],
        '/profile/{}/'.format(quote(post.author)),
        html_escape(post.author),
    )

    # Подпись к фоточкам у телеграма может быть не более 1024 символов
    # И ещё 120 символов резервируем про запас под ссылку и прочий хлам
    max_len = (1024 if short else 8200) - len(tg_body) - 120

    # Собственно текст поста (перед катом)
    post_body = fmt.format(post.body, with_cutted=False)[:max_len + 1]
    if len(post_body) >= max_len:
        post_body = post_body[:post_body.rfind(' ')] + '… ->'

    while '\n\n\n' in post_body:
        post_body = post_body.replace('\n\n\n', '\n\n')

    if post_body.endswith('\n====='):
        post_body = post_body[:-6]

    tg_body += '\n\n'
    # FIXME: злоупотребление html-сущностями позволяет здесь превысить
    # телеграмный лимит 1024 символа, но мне лень это фиксить
    tg_body += html_escape(post_body)

    return tg_body.strip()


def find_image(post):
    # type: (api.Post) -> Tuple[Optional[text], Optional[bytes]]

    # Для начала поищем картинку, явно заданную пользователем
    img_forced = None
    for i in post.body.xpath('.//img')[:20]:
        alt = i.get('alt')
        if not alt:
            continue
        if alt.startswith('tf:http://') or alt.startswith('tf:https://'):
            img_forced = alt[3:]
            break
        elif alt == 'tf:this' and i.get('src') and (i.get('src').startswith('http://') or i.get('src').startswith('https://')):
            img_forced = i.get('src')
            break

    if img_forced:
        return img_forced, None

    # приоритет картинок: нормальные в посте, нормальные в заголовках спойлеров, вообще какие-нибудь
    urls_clean = api.utils.find_images(post.body, spoiler_title=(post.blog == "Analiz"), no_other=True)[0]
    urls_spoilers = api.utils.find_images(post.body, spoiler_title=True, no_other=True)[0]
    urls_other = api.utils.find_images(post.body, spoiler_title=True, no_other=False)[0]

    # Если не нашлось вообще ничего, то делать нечего
    if not urls_clean and not urls_spoilers and not urls_other:
        return None, None

    for x in urls_clean:
        if x in urls_spoilers:
            urls_spoilers.remove(x)
    for x in urls_clean + urls_spoilers:
        if x in urls_other:
            urls_other.remove(x)

    # Среди найденных картинок выбираем лучшую
    url = None  # type: Optional[text]
    data = None  # type: Optional[bytes]
    for urls in (urls_clean, urls_spoilers, urls_other):
        url, data = api.utils.find_good_image(urls[:5])
        if url:
            break

    return url, data


def build_photo_attachment(post, full_post):
    # type: (api.Post, Optional[api.Post]) -> Optional[text]

    try:
        image, idata = find_image(post)
    except Exception:
        worker.fail()
        image = None
        idata = None

    if image is None:
        return None

    if image.startswith('//'):
        image = 'https:' + image
    elif image.startswith('/'):
        image = post.context['http_host'] + image
    elif not image.startswith('http://') and not image.startswith('https://'):
        image = None

    return image


def get_post_author(author):
    # type: (text) -> api.UserInfo
    for i in range(10):
        try:
            author = user.anon.get_profile(author)
            break
        except api.TabunError as exc:
            if i >= 9 or exc.code == 404 or worker.quit_event.is_set():
                raise
            core.logger.warning('telegram_feed: get author profile error: %s', exc.message)
            time.sleep(3)
    return author
