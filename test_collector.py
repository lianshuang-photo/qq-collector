import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp
from aiohttp import web

from collector import Collector, Store, MAX_MEDIA, clean, main, log


def message(seq=2):
    return {"op": 0, "s": seq, "t": "GROUP_MESSAGE_CREATE", "d": {
        "id": "same-message", "group_openid": "group", "timestamp": "2026-09-22T00:00:00+08:00",
        "author": {"member_openid": "user", "username": "Test"}, "content": "test storage",
        "message_type": 103, "message_scene": {"ext": ["auth_token=private", "msg_idx=100"]},
        "msg_elements": [{"attachments": [{"content_type": "image/png", "size": 5,
            "url": "https://multimedia.nt.qq.com.cn/download?fileid=example&rkey=private"}]}]}}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = Store(self.root)
        self.store.save({"op": 0, "s": 1, "t": "READY", "d": {"session_id": "test-session"}})

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def test_replay_nested_media_and_restart(self):
        self.store.save(message())
        duplicate = message(3)
        duplicate["t"] = "GROUP_AT_MESSAGE_CREATE"
        self.store.save(duplicate)
        self.store.db.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages").fetchone()[0], 1)
        row = self.store.db.execute("SELECT * FROM attachments").fetchone()
        self.assertEqual(row["source"], "top.msg_elements[0].attachments[0]")
        self.assertEqual(row["status"], "pending")
        self.assertIn("rkey=private", row["download_url"])
        self.assertNotIn("private", row["metadata_json"])
        for row in self.store.db.execute("SELECT raw_json FROM events"):
            self.assertNotIn("private", row[0])
        self.assertEqual(self.store.get("seq"), 3)
        self.assertEqual(self.store.get("session_id"), "test-session")

    def test_transaction_does_not_advance_on_failed_write(self):
        self.store.db.execute("CREATE TRIGGER fail_message BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'test'); END")
        with self.assertRaises(Exception):
            self.store.save(message())
        self.assertEqual(self.store.get("seq"), 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM events").fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM groups").fetchone()[0], 0)

    def test_video_and_large_image_metadata_only(self):
        p = message()
        p["d"]["attachments"] = [{"content_type": "video/mp4", "url": "https://qpic.cn/a"},
                                   {"content_type": "image/png", "url": "https://qpic.cn/b", "size": MAX_MEDIA+1}]
        self.store.save(p)
        rows = self.store.db.execute("SELECT status,error,download_url FROM attachments WHERE source LIKE 'top.attachments%' ORDER BY id").fetchall()
        self.assertEqual([tuple(r) for r in rows], [("skipped", "metadata_only_type", None), ("skipped", "size_limit", None)])

    def test_c2c_not_stored_but_checkpoint_advances(self):
        self.store.save({"op": 0, "s": 2, "t": "C2C_MESSAGE_CREATE", "d": {"content": "private DM"}})
        self.assertEqual(self.store.get("seq"), 2)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM events").fetchone()[0], 1)

    def test_message_id_scoped_by_group(self):
        self.store.save(message())
        other = message(3)
        other["d"]["group_openid"] = "second-group"
        self.store.save(other)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages").fetchone()[0], 2)

    def test_protocol_relative_media_credentials_not_persisted(self):
        p = message()
        p["d"]["msg_elements"][0]["attachments"][0]["url"] = "//qpic.cn/image?rkey=private&fileid=example"
        self.store.save(p)
        row = self.store.db.execute("SELECT * FROM attachments").fetchone()
        self.assertNotIn("private", row["metadata_json"])
        self.assertIn("rkey=private", row["download_url"])
        self.assertNotIn("private", self.store.db.execute("SELECT raw_json FROM messages").fetchone()[0])


class StartupTests(unittest.TestCase):
    def test_first_run_creates_data_before_acquiring_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config").mkdir()
            (root / "config/credentials.json").write_text('{"app_id":"test","app_secret":"test"}')
            old_umask = os.umask(0o077)
            old_handlers = log.handlers[:]
            try:
                with patch("collector.ROOT", root), patch("sys.argv", ["collector.py", "run"]), patch.object(Collector, "run", new_callable=AsyncMock) as run:
                    main()
                    run.assert_awaited_once()
                self.assertTrue((root / "data/messages.sqlite3").exists())
            finally:
                os.umask(old_umask)
                for handler in log.handlers[:]:
                    if handler not in old_handlers:
                        log.removeHandler(handler)
                        handler.close()


class Content:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_chunked(self, _):
        for chunk in self.chunks:
            yield chunk


class Response:
    status = 200
    content_length = None

    def __init__(self, chunks):
        self.content = Content(chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class HTTP:
    def __init__(self, chunks):
        self.chunks = chunks

    def get(self, url, **kwargs):
        assert "headers" not in kwargs
        assert kwargs["allow_redirects"] is False
        return Response(self.chunks)


class MediaTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name))
        self.store.save(message())
        self.collector = Collector(self.store, {})
        self.row = self.store.db.execute("SELECT * FROM attachments").fetchone()

    async def asyncTearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    async def test_download_is_atomic_and_hashed(self):
        self.collector.http = HTTP([b"hello", b" world"])
        path, digest, size = await self.collector.download(self.row)
        self.assertEqual((self.store.root / path).read_bytes(), b"hello world")
        self.assertEqual(size, 11)
        self.assertEqual(digest, hashlib.sha256(b"hello world").hexdigest())
        self.assertEqual(list(self.store.root.rglob("*.part")), [])

    async def test_size_enforced_without_content_length(self):
        self.collector.http = HTTP([b"x" * (MAX_MEDIA+1)])
        with self.assertRaises(OverflowError):
            await self.collector.download(self.row)
        self.assertEqual([p for p in (self.store.root / "data/media").rglob("*") if p.is_file()], [])

    async def test_worker_clears_temporary_url_on_success(self):
        self.collector.http = HTTP([b"image data"])
        worker = asyncio.create_task(self.collector.media_loop())
        await asyncio.sleep(.05)
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        row = self.store.db.execute("SELECT * FROM attachments").fetchone()
        self.assertEqual(row["status"], "ok")
        self.assertIsNone(row["download_url"])
        self.assertEqual(row["actual_size"], 10)

    async def test_gateway_resume_replay_and_invalid_session(self):
        handshakes = []

        async def gateway(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json({"op": 10, "d": {"heartbeat_interval": 41250}})
            handshake = await ws.receive_json()
            handshakes.append(handshake)
            if len(handshakes) == 1:
                await ws.send_json({"op": 0, "s": 1, "t": "READY", "d": {"session_id": "gateway-session"}})
                await ws.send_json(message(2))
            elif len(handshakes) == 2:
                await ws.send_json(message(3))
                await ws.send_json({"op": 0, "s": 4, "t": "RESUMED", "d": {}})
            else:
                await ws.send_json({"op": 9, "d": False})
            await ws.send_json({"op": 7})
            await ws.close()
            return ws

        app = web.Application()
        app.router.add_get("/gateway", gateway)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.collector.get = AsyncMock(return_value={"url": f"http://127.0.0.1:{port}/gateway"})
        self.collector.access_token = AsyncMock(return_value="mock-token")
        try:
            async with aiohttp.ClientSession() as self.collector.http:
                for _ in range(3):
                    await asyncio.wait_for(self.collector.connection(), timeout=5)
            self.assertEqual(handshakes[0]["op"], 2)
            self.assertEqual(handshakes[1]["op"], 6)
            self.assertEqual(handshakes[1]["d"]["seq"], 2)
            self.assertEqual(handshakes[1]["d"]["session_id"], "gateway-session")
            self.assertEqual(handshakes[2]["d"]["seq"], 4)
            self.assertIsNone(self.store.get("session_id"))
            self.assertIsNone(self.store.get("seq"))
            self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages").fetchone()[0], 1)
        finally:
            await runner.cleanup()

    async def test_token_cached_and_refreshed_using_actual_ttl(self):
        calls = []
        class TokenResponse(Response):
            async def json(self):
                return {"access_token": f"token-{len(calls)}", "expires_in": "100"}
        class TokenHTTP:
            def post(self, url, **kwargs):
                calls.append(url)
                return TokenResponse([])
        self.collector.credentials = {"app_id": "test", "app_secret": "test"}
        self.collector.http = TokenHTTP()
        first = await self.collector.access_token()
        self.assertEqual(await self.collector.access_token(), first)
        self.assertEqual(len(calls), 1)
        self.assertGreater(self.collector.refresh_at - time.monotonic(), 70)
        self.assertLess(self.collector.refresh_at - time.monotonic(), 81)
        self.collector.refresh_at = 0
        self.assertNotEqual(await self.collector.access_token(), first)
        self.assertEqual(len(calls), 2)

    async def test_gateway_4009_preserves_resume_checkpoint(self):
        handshakes = []

        async def gateway(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json({"op": 10, "d": {"heartbeat_interval": 41250}})
            handshakes.append(await ws.receive_json())
            if len(handshakes) == 1:
                await ws.send_json({"op": 0, "s": 1, "t": "READY", "d": {"session_id": "resume-me"}})
                await ws.send_json(message(2))
            await ws.close(code=4009)
            return ws

        app = web.Application()
        app.router.add_get("/gateway", gateway)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.collector.get = AsyncMock(return_value={"url": f"http://127.0.0.1:{port}/gateway"})
        self.collector.access_token = AsyncMock(return_value="mock-token")
        try:
            async with aiohttp.ClientSession() as self.collector.http:
                await asyncio.wait_for(self.collector.connection(), timeout=5)
                self.assertEqual(self.store.get("session_id"), "resume-me")
                self.assertEqual(self.store.get("seq"), 2)
                await asyncio.wait_for(self.collector.connection(), timeout=5)
            self.assertEqual(handshakes[1]["op"], 6)
            self.assertEqual(handshakes[1]["d"]["seq"], 2)
        finally:
            await runner.cleanup()

    async def test_nonresumable_and_permanent_gateway_close_codes(self):
        close_code = 4900
        handshakes = []

        async def gateway(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json({"op": 10, "d": {"heartbeat_interval": 41250}})
            handshakes.append(await ws.receive_json())
            await ws.close(code=close_code)
            return ws

        app = web.Application()
        app.router.add_get("/gateway", gateway)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.collector.get = AsyncMock(return_value={"url": f"http://127.0.0.1:{port}/gateway"})
        self.collector.access_token = AsyncMock(return_value="mock-token")
        try:
            async with aiohttp.ClientSession() as self.collector.http:
                for close_code in (4006, 4007, *range(4900, 4914)):
                    with self.subTest(close_code=close_code):
                        self.store.status(session_id="old-session", seq=42)
                        await asyncio.wait_for(self.collector.connection(), timeout=2)
                        self.assertIsNone(self.store.get("session_id"))
                        self.assertIsNone(self.store.get("seq"))
                for close_code in (4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915):
                    with self.subTest(close_code=close_code):
                        self.store.status(session_id="old-session", seq=42)
                        before = len(handshakes)
                        await asyncio.wait_for(self.collector.gateway_loop(), timeout=.5)
                        self.assertEqual(len(handshakes), before + 1)
                        self.assertEqual(self.store.get("connection"), "blocked")
                        self.assertEqual(self.store.get("gateway_close_code"), close_code)
        finally:
            await runner.cleanup()

    async def test_missed_heartbeat_ack_closes_connection(self):
        ws = AsyncMock()
        self.collector.awaiting_ack = False
        self.collector.access_token = AsyncMock(return_value="token")
        await asyncio.wait_for(self.collector.heartbeat(ws, .01, "token"), timeout=1)
        ws.send_json.assert_awaited_once()
        ws.close.assert_awaited_once()

    async def test_refreshed_token_triggers_reconnect(self):
        ws = AsyncMock()
        self.collector.awaiting_ack = False
        self.collector.access_token = AsyncMock(return_value="new-token")
        await asyncio.wait_for(self.collector.heartbeat(ws, .01, "old-token"), timeout=1)
        ws.send_json.assert_not_awaited()
        ws.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
