"""Transport tests using the real aiohttp client and a small Hermes API double."""
import importlib.util
import sys
import types
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp import web


def load_adapter(monkeypatch):
    modules = {name: types.ModuleType(name) for name in (
        'gateway', 'gateway.config', 'gateway.platforms', 'gateway.platforms._shared',
        'gateway.platforms.base', 'gateway.platforms.event')}
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    modules['gateway.config'].Platform = str
    shared = modules['gateway.platforms._shared']
    shared.extra_or_secret = lambda extra, key, env, default='': extra.get(key, default)
    shared.get_scoped_secret = lambda name, default='': default
    shared.seed_extra_from_env = lambda *args, **kwargs: {}
    class Base:
        def __init__(self, config, platform):
            self.config, self.platform = config, platform
        def build_source(self, **kwargs):
            return SimpleNamespace(**kwargs)
        @staticmethod
        def truncate_message(text, max_length):
            return [text[i:i + max_length] for i in range(0, len(text), max_length)]
        def _mark_connected(self): pass
        def _mark_disconnected(self): pass
    @dataclass
    class SendResult:
        success: bool
        message_id: str = None
        error: str = None
        continuation_message_ids: tuple = ()
    class Outcome(Enum):
        SUCCESS = 1
        FAILURE = 2
        CANCELLED = 3
    modules['gateway.platforms.base'].BasePlatformAdapter = Base
    modules['gateway.platforms.base'].SendResult = SendResult
    modules['gateway.platforms.base'].ProcessingOutcome = Outcome
    async def cache(data, *, filename, mime_type):
        return SimpleNamespace(path='/cache/' + filename, media_type=mime_type,
                               kind='document', display_name=filename)
    modules['gateway.platforms.base'].cache_media_bytes_async = cache
    modules['gateway.platforms.base']._TEXT_INJECT_EXTENSIONS = {'.txt'}
    modules['gateway.platforms.event'].MessageType = SimpleNamespace(TEXT='text', PHOTO='photo',
                                                                    AUDIO='audio', VIDEO='video', DOCUMENT='document')
    modules['gateway.platforms.event'].MessageEvent = lambda **kwargs: SimpleNamespace(**kwargs)
    spec = importlib.util.spec_from_file_location('traq_test_adapter', Path(__file__).parents[1] / 'traq/adapter.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def adapter(monkeypatch):
    module = load_adapter(monkeypatch)
    obj = module.TraqAdapter(SimpleNamespace(extra={'token': 'token'}))
    obj.me = {'id': 'bot-id', 'name': 'hermes'}
    obj.events = []
    async def handle(event): obj.events.append(event)
    obj.handle_message = handle
    return obj


def envelope(text='hello', event_type='MESSAGE_CREATED', uid='user-id', mid='msg-id', channel='channel-id', embedded=None, bot=False):
    return {'type': event_type, 'body': {'message': {'id': mid, 'channelId': channel, 'text': text,
            'embedded': embedded or [], 'user': {'id': uid, 'name': 'alice', 'bot': bot}}}}


@pytest.mark.asyncio
async def test_admission_and_dm(adapter):
    await adapter.receive(envelope())
    assert not adapter.events
    await adapter.receive(envelope('@hermes hello'))
    assert adapter.events[0].text == 'hello'
    assert adapter.events[0].source.chat_id == 'channel-id'
    await adapter.receive(envelope('@hermes hello'))
    assert len(adapter.events) == 1
    await adapter.receive(envelope('hello', event_type='DIRECT_MESSAGE_CREATED', mid='dm-id'))
    assert adapter.events[-1].source.chat_id == 'dm:user-id'
    adapter.allowed = {'another-user'}
    await adapter.receive(envelope('hello', event_type='DIRECT_MESSAGE_CREATED', mid='denied'))
    await adapter.receive(envelope('@hermes hello', uid='bot-id', mid='self'))
    assert len(adapter.events) == 2


@pytest.mark.asyncio
async def test_embedded_mention_and_free_channel(adapter):
    item = {'raw': '@hermes', 'type': 'user', 'id': 'bot-id'}
    await adapter.receive(envelope('!{"raw": "@hermes","type":"user","id":"bot-id"} hello', embedded=[item]))
    assert adapter.events[0].text == 'hello'
    adapter.free_channels = {'channel-id'}
    await adapter.receive(envelope('hello again', mid='second'))
    assert adapter.events[-1].text == 'hello again'
    adapter.require_mention = False
    await adapter.receive(envelope('third', mid='third', channel='other'))
    assert len(adapter.events) == 3


@pytest.mark.asyncio
async def test_rest_delivery_and_reactions(adapter):
    calls = []
    async def handler(request):
        body = await request.json() if request.can_read_body else None
        calls.append((request.method, request.path, body))
        if request.method == 'POST' and request.path.endswith('/messages'):
            return web.json_response({'id': 'sent'})
        return web.Response(status=204)
    app = web.Application()
    app.router.add_route('*', '/{tail:.*}', handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    import aiohttp
    adapter.origin = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
    adapter.http = aiohttp.ClientSession()
    adapter.stamps = {'eyes': 'eyes-id', 'white_check_mark': 'check-id', 'x': 'x-id'}
    try:
        assert (await adapter.send('dm:user-id', 'hello')).success
        assert (await adapter.send('channel-id', 'x' * 10001)).success
        assert (await adapter.edit_message('channel-id', 'sent', 'progress')).success
        event = SimpleNamespace(message_id='incoming')
        await adapter.on_processing_start(event)
        await adapter.on_processing_complete(event, sys.modules['gateway.platforms.base'].ProcessingOutcome.SUCCESS)
        assert calls[0][1] == '/api/v3/users/user-id/messages'
        assert [c[1] for c in calls].count('/api/v3/channels/channel-id/messages') == 2
        assert ('PUT', '/api/v3/messages/sent', {'content': 'progress', 'embed': False}) in calls
        assert ('POST', '/api/v3/messages/incoming/stamps/check-id', {'count': 1}) in calls
        assert ('DELETE', '/api/v3/messages/incoming/stamps/eyes-id', None) in calls
        calls.clear()
        await adapter.on_processing_complete(event, sys.modules['gateway.platforms.base'].ProcessingOutcome.FAILURE)
        assert calls == [('DELETE', '/api/v3/messages/incoming/stamps/eyes-id', None),
                         ('POST', '/api/v3/messages/incoming/stamps/x-id', {'count': 1})]
        calls.clear()
        await adapter.on_processing_complete(event, sys.modules['gateway.platforms.base'].ProcessingOutcome.CANCELLED)
        assert calls == [('DELETE', '/api/v3/messages/incoming/stamps/eyes-id', None)]
    finally:
        await adapter.http.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_attachment_filtering_and_errors(adapter):
    import uuid
    file_id = str(uuid.uuid4())
    calls = []
    async def request(method, path, **kwargs):
        calls.append(path)
        return {'name': 'large.txt', 'mime': 'text/plain', 'size': 33 * 1024 * 1024}
    adapter.request = request
    file_ref = {'type': 'file', 'id': file_id, 'raw': ''}
    await adapter.receive(envelope('no mention', embedded=[file_ref]))
    assert not calls and not adapter.events
    adapter.allowed = {'other'}
    await adapter.receive(envelope('@hermes', embedded=[file_ref]))
    assert not calls and not adapter.events
    adapter.allowed = set()
    await adapter.receive(envelope('@hermes', embedded=[file_ref, file_ref]))
    assert len(calls) == 1
    assert adapter.events[0].media_urls == []
    assert 'could not be downloaded' in adapter.events[0].text


@pytest.mark.asyncio
async def test_upload_failures_are_returned(adapter, tmp_path):
    result = await adapter.send_document('channel', str(tmp_path / 'missing'))
    assert not result.success
    path = tmp_path / 'file.txt'
    path.write_text('hello')
    async def fail(method, path, **kwargs):
        raise RuntimeError('upload failed')
    adapter.request = fail
    result = await adapter.send_document('channel', str(path))
    assert not result.success and 'upload failed' in result.error


@pytest.mark.asyncio
async def test_gateway_rechecks_access_policy(adapter):
    adapter.config.extra['allowed_users'] = 'user-id'
    assert await adapter.reauthorize_native_roles(SimpleNamespace(user_id='user-id'))
    assert not await adapter.reauthorize_native_roles(SimpleNamespace(user_id='other'))
    adapter.config.extra['allowed_users'] = ''
    assert await adapter.reauthorize_native_roles(SimpleNamespace(user_id='other'))
    assert not await adapter.reauthorize_native_roles(SimpleNamespace(user_id=None))
