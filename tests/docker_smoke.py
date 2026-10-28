import asyncio, aiohttp, sys, json, tempfile, uuid, struct, zlib, os, shutil
from pathlib import Path
from aiohttp import web
sys.path.insert(0, '/tmp/traq')
from adapter import TraqAdapter
from gateway.config import Platform, PlatformConfig

async def main():
    base='http://hermes-traq-test-server:3000/api/v3'
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as admin:
        async def req(method, path, data=None):
            async with admin.request(method, base+path, json=data) as r:
                raw=await r.text()
                if r.status >= 400: raise RuntimeError(f'{method} {path}: {r.status} {raw[:200]}')
                return await r.json() if r.content_type=='application/json' else None
        for attempt in range(60):
            try:
                await req('POST','/login',{'name':'traq','password':'traq'})
                break
            except (aiohttp.ClientError, RuntimeError):
                if attempt == 59:
                    raise
                await asyncio.sleep(2)
        me=await req('GET','/users/me')
        channels=await req('GET','/channels')
        channel=channels['public'][0]['id']
        def chunk(kind, data):
            return struct.pack('>I',len(data))+kind+data+struct.pack('>I',zlib.crc32(kind+data))
        png=(b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',1,1,8,6,0,0,0))+
             chunk(b'IDAT',zlib.compress(b'\x00\xff\x00\x00\xff'))+chunk(b'IEND',b''))
        stamps={s['name']:s['id'] for s in await req('GET','/stamps')}
        for name in ('eyes','white_check_mark'):
            if name not in stamps:
                form=aiohttp.FormData()
                form.add_field('name',name)
                form.add_field('file',png,filename='stamp.png',content_type='image/png')
                async with admin.post(base+'/stamps',data=form) as response:
                    response.raise_for_status()
                    stamps[name]=(await response.json())['id']
        bot_name='hermes'+uuid.uuid4().hex[:8]
        bot=await req('POST','/bots',{'name':bot_name,'displayName':'Hermes Test','description':'test','mode':'WebSocket'})
        bid=bot['id']
        await req('PATCH',f'/bots/{bid}',{'subscribeEvents':['MESSAGE_CREATED','DIRECT_MESSAGE_CREATED']})
        await req('POST',f'/bots/{bid}/actions/join',{'channelId':channel})
        Platform._add_pseudo_member('traq')
        adapter=TraqAdapter(PlatformConfig(extra={'token':bot['tokens']['accessToken'],'origin':'http://hermes-traq-test-server:3000'}))
        received=asyncio.Queue()
        async def receive(event): await received.put(event)
        adapter.handle_message=receive
        assert await adapter.connect()
        try:
            sent_by_user=await req('POST',f'/channels/{channel}/messages',{'content':f'@BOT_{bot_name} hello','embed':True})
            event=await asyncio.wait_for(received.get(),10)
            assert event.text=='hello', repr(event.text)
            from gateway.platforms.event import ProcessingOutcome
            await adapter.on_processing_start(event)
            reactions=await req('GET',f'/messages/{sent_by_user["id"]}/stamps')
            assert any(s['stampId']==stamps['eyes'] for s in reactions), reactions
            await adapter.on_processing_complete(event,ProcessingOutcome.SUCCESS)
            reactions=await req('GET',f'/messages/{sent_by_user["id"]}/stamps')
            assert any(s['stampId']==stamps['white_check_mark'] for s in reactions), reactions
            assert not any(s['stampId']==stamps['eyes'] for s in reactions), reactions
            result=await adapter.send(event.source.chat_id,'adapter reply')
            assert result.success, result.error
            assert (await adapter.edit_message(channel,result.message_id,'edited reply')).success
            await req('POST',f'/users/{bot["botUserId"]}/messages',{'content':'dm hello'})
            dm=await asyncio.wait_for(received.get(),10)
            assert dm.source.chat_id == 'dm:'+me['id']
            result=await adapter.send(dm.source.chat_id,'dm reply')
            assert result.success, result.error
            async def upload(channel_id, filename, data, mime):
                form=aiohttp.FormData()
                form.add_field('channelId',channel_id)
                form.add_field('file',data,filename=filename,content_type=mime)
                async with admin.post(base+'/files',data=form) as response:
                    response.raise_for_status()
                    return await response.json()
            def embed(info):
                return '!' + json.dumps({'type':'file','raw':info['name'],'id':info['id']},separators=(',', ':'))
            info=await upload(channel,'input.txt',b'hello from file','text/plain')
            await req('POST',f'/channels/{channel}/messages',{'content':f'@BOT_{bot_name} '+embed(info),'embed':True})
            incoming=await asyncio.wait_for(received.get(),10)
            assert incoming.media_types==['text/plain'], incoming.media_types
            assert Path(incoming.media_urls[0]).read_bytes()==b'hello from file'
            assert 'hello from file' in incoming.text and incoming.media_text_inlined==[True]
            dm_channel=(await req('GET',f'/users/{bot["botUserId"]}/dm-channel'))['id']
            info=await upload(dm_channel,'dm.txt',b'direct file','text/plain')
            await req('POST',f'/users/{bot["botUserId"]}/messages',{'content':embed(info),'embed':False})
            incoming=await asyncio.wait_for(received.get(),10)
            assert incoming.source.chat_id=='dm:'+me['id']
            assert Path(incoming.media_urls[0]).read_bytes()==b'direct file'
            info=await upload(channel,'input.png',png,'image/png')
            await req('POST',f'/channels/{channel}/messages',{'content':f'@BOT_{bot_name} '+embed(info),'embed':True})
            incoming=await asyncio.wait_for(received.get(),10)
            assert incoming.media_types[0].startswith('image/'), incoming.media_types
            assert Path(incoming.media_urls[0]).read_bytes()==png
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/'output.txt'
                path.write_bytes(b'generated file')
                for target in (channel,'dm:'+me['id']):
                    result=await adapter.send_document(target,str(path),caption='result')
                    assert result.success, result.error
                    sent=await req('GET',f'/messages/{result.message_id}')
                    assert '"type":"file"' in sent['content'], sent
                    uploaded=json.loads(sent['content'].split('!',1)[1])
                    async with admin.get(base+'/files/'+uploaded['id']) as response:
                        assert response.status==200
                        assert await response.read()==b'generated file'
                image=Path(directory)/'output.png'
                image.write_bytes(png)
                for method, media in ((adapter.send_image_file, image), (adapter.send_video, path), (adapter.send_voice, path)):
                    result=await method(channel,str(media))
                    assert result.success, result.error
                    sent=await req('GET',f'/messages/{result.message_id}')
                    uploaded=json.loads(sent['content'].split('!',1)[1])
                    async with admin.get(base+'/files/'+uploaded['id']) as response:
                        assert response.status==200 and await response.read()==media.read_bytes()
            print('real traQ: channel and DM messages, inbound files and image, outbound files and editing passed')
        finally:
            await adapter.disconnect()
        await gateway_test(admin, req, bot, bot_name, channel, stamps)


async def gateway_test(admin, req, bot, bot_name, channel, stamps):
    """Exercise the real gateway and its tool loop against a deterministic model."""
    calls=[]
    async def completion(request):
        body=await request.json()
        tool_results=[m for m in body['messages'] if m.get('role')=='tool']
        calls.append(tool_results)
        n=len(tool_results)
        if n < 2:
            message={'role':'assistant','content':None,'tool_calls':[{'id':f'call_{n}','type':'function',
                'function':{'name':'terminal','arguments':json.dumps({'command':f'printf tool-result-{n}; sleep 2'})}}]}
            reason='tool_calls'
        else:
            await asyncio.sleep(3)
            message={'role':'assistant','content':'gateway test complete'}
            reason='stop'
        if body.get('stream'):
            delta={k:v for k,v in message.items() if k != 'role' and v is not None}
            if 'tool_calls' in delta:
                delta['tool_calls'][0]['index']=0
            event={'id':'test-completion','object':'chat.completion.chunk','created':1,'model':'test-model',
                   'choices':[{'index':0,'delta':delta,'finish_reason':reason}]}
            return web.Response(text='data: '+json.dumps(event)+'\n\ndata: [DONE]\n\n',content_type='text/event-stream')
        return web.json_response({'id':'test-completion','object':'chat.completion','created':1,
            'model':'test-model','choices':[{'index':0,'message':message,'finish_reason':reason}],
            'usage':{'prompt_tokens':10,'completion_tokens':10,'total_tokens':20}})
    app=web.Application()
    app.router.add_post('/v1/chat/completions',completion)
    app.router.add_get('/v1/models',lambda request:web.json_response({'object':'list','data':[]}))
    runner=web.AppRunner(app)
    await runner.setup()
    site=web.TCPSite(runner,'127.0.0.1',0)
    await site.start()
    port=site._server.sockets[0].getsockname()[1]
    with tempfile.TemporaryDirectory() as home:
        home=Path(home)
        (home/'plugins').mkdir()
        shutil.copytree('/tmp/traq',home/'plugins'/'traq')
        (home/'config.yaml').write_text(
            f'plugins:\n  enabled: [traq-platform]\nmodel:\n  default: test-model\n  provider: custom\n  base_url: http://127.0.0.1:{port}/v1\n'
            'terminal:\n  env_type: local\nstreaming:\n  enabled: false\ndisplay:\n  tool_progress: all\n  tool_progress_grouping: accumulate\n'
            'auxiliary:\n  title_generation:\n    enabled: false\n')
        env={**os.environ,'HERMES_HOME':str(home),'TRAQ_BOT_TOKEN':bot['tokens']['accessToken'],
             'TRAQ_ORIGIN':'http://hermes-traq-test-server:3000','OPENAI_API_KEY':'test-only',
             'OPENAI_BASE_URL':f'http://127.0.0.1:{port}/v1','TERMINAL_ENV':'local',
             'PYTHONUNBUFFERED':'1'}
        logfile=(home/'gateway-test.log').open('w+')
        process=await asyncio.create_subprocess_exec('/opt/hermes/.venv/bin/python3','-m','hermes_cli.main','gateway','run',
            cwd='/opt/hermes',env=env,stdout=logfile,stderr=logfile)
        try:
            for _ in range(60):
                if process.returncode is not None:
                    raise AssertionError('gateway exited on startup')
                if 'traq connected' in (home/'logs'/'gateway.log').read_text() if (home/'logs'/'gateway.log').exists() else False:
                    break
                await asyncio.sleep(1)
            else:
                raise AssertionError('gateway did not connect')
            baseline={m['id'] for m in await req('GET',f'/channels/{channel}/messages?limit=100')}
            original=await req('POST',f'/channels/{channel}/messages',{'content':f'@BOT_{bot_name} run test tools','embed':True})
            saw_eyes=False
            messages=[]
            for _ in range(90):
                reactions=await req('GET',f'/messages/{original["id"]}/stamps')
                saw_eyes |= any(s['stampId']==stamps['eyes'] for s in reactions)
                messages=await req('GET',f'/channels/{channel}/messages?limit=100')
                if any(m['id'] not in baseline and m.get('userId') == bot['botUserId'] and 'gateway test complete' in m.get('content','') for m in messages):
                    break
                if process.returncode is not None:
                    break
                await asyncio.sleep(1)
            assert any(m['id'] not in baseline and m.get('userId') == bot['botUserId'] and 'gateway test complete' in m.get('content','') for m in messages), 'gateway answer missing'
            assert len(calls)>=3 and any('tool-result-0' in str(row) for row in calls), calls
            assert saw_eyes, 'processing stamp missing'
            reactions=await req('GET',f'/messages/{original["id"]}/stamps')
            assert any(s['stampId']==stamps['white_check_mark'] for s in reactions), reactions
            bot_messages=[m for m in messages if m.get('userId')==bot['botUserId'] and m['id'] not in baseline]
            assert any(m.get('updatedAt')!=m.get('createdAt') for m in bot_messages), f'progress message was not edited: {bot_messages!r}'
            print('real gateway: tool execution, progress edit, processing and completion stamps passed')
        except Exception:
            logfile.flush()
            print((home/'gateway-test.log').read_text()[-12000:],file=sys.stderr)
            raise
        finally:
            if process.returncode is None:
                process.terminate()
                try: await asyncio.wait_for(process.wait(),10)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
            logfile.close()
    await runner.cleanup()
asyncio.run(main())
