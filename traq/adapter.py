"""traQ WebSocket Bot transport for the Hermes messaging gateway."""
from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import mimetypes
import re
import uuid
from collections import OrderedDict
from pathlib import Path
from urllib.parse import urlsplit
from urllib.parse import urljoin

import aiohttp
from gateway.config import Platform
from gateway.platforms._shared import extra_or_secret, get_scoped_secret, seed_extra_from_env
from gateway.platforms.base import (BasePlatformAdapter, SendResult, cache_media_bytes_async,
                                    _TEXT_INJECT_EXTENSIONS)
from gateway.platforms.event import MessageEvent, MessageType

log = logging.getLogger(__name__)
LIMIT = 10000
MAX_ATTACHMENT_BYTES = 32 * 1024 * 1024


def csv(value):
    if isinstance(value, (list, tuple, set)):
        return {str(x).strip() for x in value if str(x).strip()}
    return {x.strip() for x in str(value or '').split(',') if x.strip()}


def setting(config, key, env, default=''):
    value = extra_or_secret(config.extra, key, env, default)
    return default if value is None else value


def enabled(value):
    return str(value).lower().strip() not in {'0', 'false', 'no', 'off'}


class TraqAdapter(BasePlatformAdapter):
    def __init__(self, config):
        super().__init__(config, Platform('traq'))
        self.token = setting(config, 'token', 'TRAQ_BOT_TOKEN')
        self.origin = str(setting(config, 'origin', 'TRAQ_ORIGIN', 'https://q.trap.jp')).rstrip('/')
        self.allowed = csv(setting(config, 'allowed_users', 'TRAQ_ALLOWED_USERS'))
        self.require_mention = enabled(setting(config, 'require_mention', 'TRAQ_REQUIRE_MENTION', 'true'))
        self.free_channels = csv(setting(config, 'free_response_channels', 'TRAQ_FREE_RESPONSE_CHANNELS'))
        self.http = None
        self.ws = None
        self.listener = None
        self.me = None
        self.stamps = {}
        self.seen = OrderedDict()

    async def request(self, method, path, **kwargs):
        async with self.http.request(method, self.origin + '/api/v3' + path, **kwargs) as response:
            response.raise_for_status()
            return await response.json() if response.content_type == 'application/json' else None

    async def connect(self, *, is_reconnect=False):
        if not self.token:
            return False
        parsed = urlsplit(self.origin)
        if parsed.scheme not in ('http', 'https') or not parsed.netloc or parsed.path not in ('', '/'):
            log.error('TRAQ_ORIGIN must be an http(s) origin')
            return False
        self.http = aiohttp.ClientSession(headers={'Authorization': 'Bearer ' + self.token}, timeout=aiohttp.ClientTimeout(total=30))
        try:
            self.me = await self.request('GET', '/users/me')
            try:
                stamps = await self.request('GET', '/stamps')
                self.stamps = {s['name']: s['id'] for s in stamps if s.get('name') in ('eyes', 'white_check_mark')}
                for name in ('eyes', 'white_check_mark'):
                    if name not in self.stamps:
                        log.warning('traQ stamp %s is unavailable', name)
            except Exception:
                log.warning('Unable to fetch traQ stamps', exc_info=True)
            await self._open_ws()
        except Exception:
            log.exception('Could not connect to traQ')
            await self.http.close()
            self.http = None
            return False
        self._mark_connected()
        self.listener = asyncio.create_task(self._listen())
        return True

    async def _open_ws(self):
        url = self.origin.replace('https://', 'wss://', 1).replace('http://', 'ws://', 1)
        self.ws = await self.http.ws_connect(url + '/api/v3/bots/ws', heartbeat=30, timeout=30)

    async def _listen(self):
        delay = 1
        while self.http and not self.http.closed:
            try:
                async for frame in self.ws:
                    if frame.type == aiohttp.WSMsgType.TEXT:
                        try:
                            await self.receive(frame.json())
                        except Exception:
                            log.exception('Failed to handle traQ event')
                if not self.http or self.http.closed:
                    return
                await asyncio.sleep(delay)
                await self._open_ws()
                delay = 1
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning('traQ websocket disconnected; retrying', exc_info=True)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def disconnect(self):
        if self.listener:
            self.listener.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.listener
            self.listener = None
        if self.ws:
            await self.ws.close()
        if self.http:
            await self.http.close()
            self.http = None
        self._mark_disconnected()

    async def receive(self, envelope):
        event_type = envelope.get('type')
        if event_type not in ('MESSAGE_CREATED', 'DIRECT_MESSAGE_CREATED'):
            return
        message = (envelope.get('body') or {}).get('message') or {}
        user = message.get('user') or {}
        uid, mid, channel = user.get('id'), message.get('id'), message.get('channelId')
        if not uid or not mid or not channel or user.get('bot') or uid == self.me.get('id'):
            return
        if self.allowed and uid not in self.allowed:
            return
        is_dm = event_type == 'DIRECT_MESSAGE_CREATED'
        text = message.get('text') or ''
        mentions = [e for e in message.get('embedded') or [] if e.get('type') == 'user' and e.get('id') == self.me.get('id')]
        # Plain @name is also useful in clients that do not embed on submission.
        pattern = r'(?<![\w@])@' + re.escape(self.me.get('name', '')) + r'(?![\w-])'
        named = bool(self.me.get('name') and re.search(pattern, text))
        if not is_dm and self.require_mention and channel not in self.free_channels and '*' not in self.free_channels and not mentions and not named:
            return
        if mid in self.seen:
            return
        def unwrap(match):
            try:
                embedded = json.loads(match.group()[1:])
                if embedded.get('type') == 'user' and embedded.get('id') == self.me['id']:
                    return ''
                return embedded.get('raw', match.group())
            except (ValueError, AttributeError):
                return match.group()
        # The event's embedded file IDs are authoritative; ordinary user-provided
        # URLs are never fetched using the bot's bearer token.
        files = [e for e in message.get('embedded') or [] if e.get('type') == 'file']
        text = re.sub(r'!\{[^{}]*\}', unwrap, text)
        if self.me.get('name'):
            text = re.sub(pattern, '', text)
        text = text.strip()
        if not text and not files:
            return
        self.seen[mid] = None
        if len(self.seen) > 10000:
            self.seen.popitem(last=False)
        chat_id = ('dm:' + uid) if is_dm else channel
        source = self.build_source(chat_id=chat_id, chat_type='dm' if is_dm else 'group', user_id=uid,
                                   user_name=user.get('name'), chat_name=chat_id, message_id=mid,
                                   role_authorized=True)
        media_urls, media_types, media_inlined, notes = await self._collect_files(files)
        if notes:
            text = '\n\n'.join([*notes, text] if text else notes)
        if not text:
            text = '(The user sent an attachment.)'
        first_type = media_types[0] if media_types else ''
        message_type = (MessageType.PHOTO if first_type.startswith('image/') else
                        MessageType.AUDIO if first_type.startswith('audio/') else
                        MessageType.VIDEO if first_type.startswith('video/') else
                        MessageType.DOCUMENT if media_urls else MessageType.TEXT)
        await self.handle_message(MessageEvent(text=text, message_type=message_type, source=source,
                                               raw_message=envelope, message_id=mid,
                                               media_urls=media_urls, media_types=media_types,
                                               media_text_inlined=media_inlined))

    async def _collect_files(self, files):
        paths, types, inlined, notes = [], [], [], []
        seen = set()
        for item in files:
            try:
                file_id = str(uuid.UUID(str(item.get('id', ''))))
                if file_id in seen:
                    continue
                seen.add(file_id)
                info = await self.request('GET', f'/files/{file_id}/meta')
                name = Path(info.get('name') or 'file.bin').name
                mime = info.get('mime') or mimetypes.guess_type(name)[0] or 'application/octet-stream'
                if info.get('size', 0) > MAX_ATTACHMENT_BYTES:
                    raise ValueError('attachment exceeds 32 MiB')
                async with self.http.get(self.origin + f'/api/v3/files/{file_id}') as response:
                    response.raise_for_status()
                    data = bytearray()
                    async for chunk in response.content.iter_chunked(65536):
                        data.extend(chunk)
                        if len(data) > MAX_ATTACHMENT_BYTES:
                            raise ValueError('attachment exceeds 32 MiB')
                cached = await cache_media_bytes_async(bytes(data), filename=name, mime_type=mime)
                if cached is None:
                    raise ValueError('invalid attachment')
                paths.append(cached.path)
                types.append(cached.media_type)
                is_inline = False
                if cached.kind == 'document' and len(data) <= 100 * 1024 and (Path(name).suffix.lower() in _TEXT_INJECT_EXTENSIONS or mime.startswith('text/')):
                    try:
                        contents = data.decode('utf-8')
                        notes.append(f"[Content of {cached.display_name}]:\n{contents}")
                        is_inline = True
                    except UnicodeDecodeError:
                        pass
                inlined.append(is_inline)
            except Exception as exc:
                log.warning('Unable to receive traQ attachment: %s', exc)
                notes.append('[An attachment could not be downloaded or exceeded the size limit.]')
        return paths, types, inlined, notes

    async def stamp(self, mid, name, method='POST'):
        stamp_id = self.stamps.get(name)
        if not stamp_id:
            return
        try:
            kwargs = {'json': {'count': 1}} if method == 'POST' else {}
            await self.request(method, f'/messages/{mid}/stamps/{stamp_id}', **kwargs)
        except Exception:
            log.warning('Could not update traQ stamp %s', name, exc_info=True)

    async def on_processing_start(self, event):
        await self.stamp(event.message_id, 'eyes')

    async def reauthorize_native_roles(self, source):
        # Hermes rechecks delegated admission when a native turn enters the
        # session authority. Never trust the event's role flag by itself.
        allowed = csv(setting(self.config, 'allowed_users', 'TRAQ_ALLOWED_USERS'))
        return bool(source.user_id and (not allowed or source.user_id in allowed))

    async def on_processing_complete(self, event, outcome):
        from gateway.platforms.base import ProcessingOutcome
        await self.stamp(event.message_id, 'eyes', 'DELETE')
        if outcome == ProcessingOutcome.SUCCESS:
            await self.stamp(event.message_id, 'white_check_mark')

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        path = '/users/' + chat_id[3:] + '/messages' if chat_id.startswith('dm:') else '/channels/' + chat_id + '/messages'
        ids = []
        try:
            for chunk in self.truncate_message(content, max_length=LIMIT):
                result = await self.request('POST', path, json={'content': chunk, 'embed': False})
                ids.append(result['id'])
            return SendResult(success=True, message_id=ids[-1] if ids else None, continuation_message_ids=tuple(ids[:-1]))
        except Exception as exc:
            log.warning('traQ send failed', exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def _send_file(self, chat_id, file_obj, filename, caption=None):
        try:
            if chat_id.startswith('dm:'):
                dm = await self.request('GET', f'/users/{chat_id[3:]}/dm-channel')
                channel_id = dm['id']
            else:
                channel_id = chat_id
            form = aiohttp.FormData()
            form.add_field('channelId', channel_id)
            form.add_field('file', file_obj, filename=filename,
                           content_type=mimetypes.guess_type(filename)[0] or 'application/octet-stream')
            info = await self.request('POST', '/files', data=form)
            embed = '!' + json.dumps({'type': 'file', 'raw': filename, 'id': info['id']}, ensure_ascii=False, separators=(',', ':'))
            content = (caption + '\n' if caption else '') + embed
            return await self.send(chat_id, content)
        except Exception as exc:
            log.warning('traQ file upload failed', exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def _send_local_file(self, chat_id, file_path, caption=None, file_name=None):
        try:
            path = Path(file_path)
            with path.open('rb') as file_obj:
                return await self._send_file(chat_id, file_obj, Path(file_name or path.name).name, caption)
        except (OSError, ValueError) as exc:
            return SendResult(success=False, error=str(exc))

    async def send_image_file(self, chat_id, image_path, caption=None, reply_to=None, metadata=None):
        return await self._send_local_file(chat_id, image_path, caption)

    async def send_document(self, chat_id, file_path, caption=None, file_name=None, reply_to=None, metadata=None):
        return await self._send_local_file(chat_id, file_path, caption, file_name)

    async def send_video(self, chat_id, video_path, caption=None, reply_to=None, metadata=None):
        return await self._send_local_file(chat_id, video_path, caption)

    async def send_voice(self, chat_id, audio_path, caption=None, reply_to=None, metadata=None, **kwargs):
        return await self._send_local_file(chat_id, audio_path, caption)

    async def _send_remote_image(self, chat_id, url, caption, filename):
        # A separate session must be used: self.http contains the traQ bearer token.
        from tools.url_safety import is_safe_url
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                current = url
                for _ in range(6):
                    if not is_safe_url(current):
                        raise ValueError('unsafe image URL')
                    async with session.get(current, allow_redirects=False) as response:
                        if response.status in (301, 302, 303, 307, 308):
                            current = urljoin(current, response.headers['Location'])
                            continue
                        response.raise_for_status()
                        data = bytearray()
                        async for chunk in response.content.iter_chunked(65536):
                            data.extend(chunk)
                            if len(data) > MAX_ATTACHMENT_BYTES:
                                raise ValueError('image exceeds 32 MiB')
                        return await self._send_file(chat_id, io.BytesIO(data), filename, caption)
                raise ValueError('too many image redirects')
        except Exception as exc:
            log.warning('Unable to attach remote image: %s', exc)
            return SendResult(success=False, error=str(exc))

    async def send_image(self, chat_id, image_url, caption=None, reply_to=None, metadata=None):
        return await self._send_remote_image(chat_id, image_url, caption, 'image' + (Path(urlsplit(image_url).path).suffix or '.png'))

    async def send_animation(self, chat_id, animation_url, caption=None, reply_to=None, metadata=None):
        return await self._send_remote_image(chat_id, animation_url, caption, 'animation.gif')

    async def edit_message(self, chat_id, message_id, content, *, finalize=False):
        if len(content) > LIMIT:
            return SendResult(success=False, error='message too long')
        try:
            await self.request('PUT', f'/messages/{message_id}', json={'content': content, 'embed': False})
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def delete_message(self, chat_id, message_id):
        try:
            await self.request('DELETE', f'/messages/{message_id}')
            return True
        except Exception:
            return False

    async def get_chat_info(self, chat_id):
        return {'name': chat_id, 'type': 'dm' if chat_id.startswith('dm:') else 'group'}


def _env_enablement():
    if not get_scoped_secret('TRAQ_BOT_TOKEN', '').strip():
        return None
    return seed_extra_from_env((('TRAQ_BOT_TOKEN', 'token', None), ('TRAQ_ORIGIN', 'origin', None),
                                ('TRAQ_ALLOWED_USERS', 'allowed_users', None),
                                ('TRAQ_REQUIRE_MENTION', 'require_mention', None),
                                ('TRAQ_FREE_RESPONSE_CHANNELS', 'free_response_channels', None)))


def register(ctx):
    ctx.register_platform(name='traq', label='traQ', adapter_factory=TraqAdapter,
                          check_fn=lambda: True, validate_config=lambda c: bool(setting(c, 'token', 'TRAQ_BOT_TOKEN')),
                          env_enablement_fn=_env_enablement, max_message_length=LIMIT)
