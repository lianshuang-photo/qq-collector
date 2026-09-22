"""Receive-only QQ group collector. Secrets and message bodies never enter logs."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import random
import re
import signal
import sqlite3
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import aiohttp

ROOT = Path(__file__).resolve().parent
API = "https://api.bot.qq.com"
MAX_MEDIA = 5 * 1024 * 1024
log = logging.getLogger("collector")


def now():
    return datetime.now(timezone.utc).isoformat()


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def clean(value):
    """Remove transport credentials, including credentials in nested event fields."""
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()
                if k.lower() not in {"auth_token", "access_token", "clientsecret", "rkey"}}
    if isinstance(value, list):
        return [clean(v) for v in value if not (isinstance(v, str) and v.startswith("auth_token="))]
    if isinstance(value, str):
        if value.startswith(("https://", "http://", "//")):
            try:
                p = urlsplit(value)
                query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
                         if k.lower() not in {"rkey", "auth_token", "access_token", "token"}]
                return urlunsplit((p.scheme, p.netloc, p.path, urlencode(query), p.fragment))
            except ValueError:
                return "[invalid URL]"
        return re.sub(r"auth_token=[^\s,;\"'&]+", "auth_token=[removed]", value)
    return value


def attachments(data, source="top"):
    if not isinstance(data, dict):
        return
    for i, item in enumerate(data.get("attachments") or []):
        if isinstance(item, dict):
            yield f"{source}.attachments[{i}]", item
    for i, item in enumerate(data.get("msg_elements") or []):
        yield from attachments(item, f"{source}.msg_elements[{i}]")


class Store:
    def __init__(self, root):
        self.root = root
        (root / "data").mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(root / "data/messages.sqlite3", timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS groups(
              group_openid TEXT PRIMARY KEY, group_name TEXT, last_seen_at TEXT,
              active INTEGER DEFAULT 1, info_json TEXT, bot_state_json TEXT,
              refresh_after REAL DEFAULT 0, refresh_error TEXT);
            CREATE TABLE IF NOT EXISTS events(
              id INTEGER PRIMARY KEY, session_id TEXT, seq INTEGER, event_type TEXT,
              received_at TEXT NOT NULL, raw_json TEXT NOT NULL,
              UNIQUE(session_id,seq));
            CREATE TABLE IF NOT EXISTS messages(
              group_openid TEXT NOT NULL REFERENCES groups(group_openid), id TEXT NOT NULL,
              author_openid TEXT, author_name TEXT, author_role TEXT,
              timestamp TEXT, received_at TEXT NOT NULL, message_type INTEGER,
              content TEXT, raw_json TEXT NOT NULL, PRIMARY KEY(group_openid,id));
            CREATE INDEX IF NOT EXISTS idx_messages_time ON messages(group_openid,timestamp);
            CREATE TABLE IF NOT EXISTS attachments(
              id INTEGER PRIMARY KEY, group_openid TEXT NOT NULL, message_id TEXT NOT NULL,
              source TEXT NOT NULL, content_type TEXT, filename TEXT, declared_size INTEGER,
              metadata_json TEXT NOT NULL, download_url TEXT, status TEXT NOT NULL,
              attempts INTEGER DEFAULT 0, next_attempt REAL DEFAULT 0, error TEXT,
              local_path TEXT, sha256 TEXT, actual_size INTEGER,
              UNIQUE(group_openid,message_id,source),
              FOREIGN KEY(group_openid,message_id) REFERENCES messages(group_openid,id));
            CREATE INDEX IF NOT EXISTS idx_download_queue ON attachments(status,next_attempt);
        """)

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        self.db.execute("INSERT INTO state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (key, dumps(value)))

    def status(self, **values):
        with self.db:
            for key, value in values.items():
                self.set(key, value)

    def save(self, payload):
        typ = payload.get("t", "UNKNOWN")
        d = payload.get("d") or {}
        seq = payload.get("s")
        session = d.get("session_id") if typ == "READY" else self.get("session_id")
        received = now()
        with self.db:
            if typ.startswith("GROUP_") or typ in {"READY", "RESUMED"}:
                self.db.execute("INSERT OR IGNORE INTO events(session_id,seq,event_type,received_at,raw_json) VALUES(?,?,?,?,?)",
                                (session, seq, typ, received, dumps(clean(payload))))
            group = d.get("group_openid") or d.get("group_id")
            if group:
                self.db.execute("INSERT INTO groups(group_openid,last_seen_at) VALUES(?,?) ON CONFLICT(group_openid) DO UPDATE SET last_seen_at=excluded.last_seen_at", (group, received))
                if typ in {"GROUP_ADD_ROBOT", "GROUP_DEL_ROBOT", "GROUP_MSG_RECEIVE", "GROUP_MSG_REJECT"}:
                    self.db.execute("UPDATE groups SET refresh_after=0 WHERE group_openid=?", (group,))
                if typ in {"GROUP_ADD_ROBOT", "GROUP_DEL_ROBOT"} or typ.endswith("MESSAGE_CREATE"):
                    self.db.execute("UPDATE groups SET active=? WHERE group_openid=?", (int(typ != "GROUP_DEL_ROBOT"), group))
            if group and typ in {"GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"} and d.get("id"):
                author = d.get("author") or {}
                self.db.execute("INSERT OR IGNORE INTO messages VALUES(?,?,?,?,?,?,?,?,?,?)", (
                    group, d["id"], author.get("member_openid") or author.get("id"),
                    author.get("username"), author.get("member_role"), d.get("timestamp"),
                    received, d.get("message_type"), d.get("content", ""), dumps(clean(d))))
                for source, a in attachments(d):
                    mime = a.get("content_type", "")
                    url = (a.get("voice_wav_url") or a.get("url")) if mime == "voice" else a.get("url")
                    try:
                        size = int(a.get("size") or 0)
                    except (TypeError, ValueError):
                        size = 0
                    reason = None
                    if not (mime.startswith("image/") or mime.startswith("audio/") or mime == "voice"):
                        reason = "metadata_only_type"
                    elif size > MAX_MEDIA:
                        reason = "size_limit"
                    elif not url:
                        reason = "missing_url"
                    self.db.execute("""INSERT OR IGNORE INTO attachments
                      (group_openid,message_id,source,content_type,filename,declared_size,metadata_json,download_url,status,error)
                      VALUES(?,?,?,?,?,?,?,?,?,?)""", (group, d["id"], source, mime, a.get("filename"), size,
                        dumps(clean(a)), url if not reason else None, "skipped" if reason else "pending", reason))
                self.set("last_message_at", received)
            if typ == "READY":
                self.set("session_id", session)
            if seq is not None:
                self.set("seq", seq)  # Advance only in the same transaction as durable messages.
            self.set("last_event_at", received)


class APIError(Exception):
    pass


class GatewayBlocked(APIError):
    """The platform requires operator intervention before another connection."""

    def __init__(self, code):
        self.code = code
        super().__init__(f"gateway_blocked_{code}")


class Collector:
    def __init__(self, store, credentials):
        self.store = store
        self.credentials = credentials
        self.token = None
        self.refresh_at = 0
        self.token_lock = asyncio.Lock()
        self.ws = None

    async def access_token(self):
        async with self.token_lock:
            if self.token and time.monotonic() < self.refresh_at:
                return self.token
            async with self.http.post(API + "/app/getAppAccessToken", json={
                "appId": self.credentials["app_id"], "clientSecret": self.credentials["app_secret"]
            }) as response:
                data = await response.json()
                if response.status != 200 or not data.get("access_token"):
                    raise APIError(f"token_http_{response.status}_code_{data.get('code')}")
                ttl = max(1, int(data["expires_in"]))
                self.token = data["access_token"]
                self.refresh_at = time.monotonic() + max(1, ttl - min(300, ttl * .2))
                self.store.status(token_refreshed_at=now())
                return self.token

    async def get(self, path):
        for attempt in range(2):
            token = await self.access_token()
            async with self.http.get(API + path, headers={"Authorization": "QQBot " + token}) as response:
                data = await response.json()
                code = data.get("code", 0) if isinstance(data, dict) else 0
                err = data.get("err_code", 0) if isinstance(data, dict) else 0
                if response.status == 401 or code == 11244 or err == 40011027:
                    self.refresh_at = 0
                    if attempt == 0:
                        continue
                if response.status >= 300 or code or err:
                    raise APIError(f"http_{response.status}_code_{code}_err_{err}")
                return data

    async def heartbeat(self, ws, interval, socket_token):
        while True:
            await asyncio.sleep(interval * .8)
            if self.awaiting_ack:
                log.warning("heartbeat_ack_timeout")
                await ws.close()
                return
            if await self.access_token() != socket_token:
                log.info("token_changed_resuming_connection")
                await ws.close()
                return
            self.awaiting_ack = True
            await ws.send_json({"op": 1, "d": self.store.get("seq")})
            self.store.status(heartbeat_sent_at=now())

    async def connection(self):
        gateway = await self.get("/gateway/bot")
        session = self.store.get("session_id")
        if not session:
            limits = gateway.get("session_start_limit") or {}
            if limits.get("remaining", 1) <= 0:
                delay = max(60, limits.get("reset_after", 60000) / 1000)
                self.store.status(connection="identify_limit", retry_at=time.time() + delay)
                await asyncio.sleep(delay)
                return
            delay = self.store.get("identify_after", 0) - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
        token = await self.access_token()
        self.store.status(connection="connecting")
        async with self.http.ws_connect(gateway["url"], max_msg_size=16*1024*1024) as ws:
            self.ws = ws
            hello = await ws.receive_json(timeout=30)
            if hello.get("op") != 10:
                raise APIError("missing_hello")
            self.awaiting_ack = False
            if session:
                await ws.send_json({"op": 6, "d": {"token": "QQBot " + token,
                    "session_id": session, "seq": self.store.get("seq")}})
            else:
                self.store.status(identify_after=time.time() + 60)
                await ws.send_json({"op": 2, "d": {"token": "QQBot " + token,
                    "intents": 1 << 25, "shard": [0, 1],
                    "properties": {"$os": "macos", "$browser": "qq-collector", "$device": "qq-collector"}}})
            hb = asyncio.create_task(self.heartbeat(ws, hello["d"]["heartbeat_interval"] / 1000, token))
            def heartbeat_done(task):
                if not task.cancelled() and task.exception():
                    log.warning("heartbeat_error type=%s", type(task.exception()).__name__)
                    asyncio.create_task(ws.close())
            hb.add_done_callback(heartbeat_done)
            try:
                async for message in ws:
                    if message.type == aiohttp.WSMsgType.ERROR:
                        raise APIError("websocket_error")
                    if message.type != aiohttp.WSMsgType.TEXT:
                        continue
                    p = json.loads(message.data)
                    op = p.get("op")
                    if op == 0:
                        self.store.save(p)
                        if p.get("t") in {"READY", "RESUMED"}:
                            self.store.status(connection="online", connected_at=now())
                            log.info("gateway_%s", p["t"].lower())
                    elif op == 11:
                        self.awaiting_ack = False
                        self.store.status(heartbeat_ack_at=now())
                    elif op == 1:
                        await ws.send_json({"op": 1, "d": self.store.get("seq")})
                    elif op == 7:
                        break
                    elif op == 9:
                        self.store.status(session_id=None, seq=None)
                        log.warning("invalid_session_new_identify_required")
                        break
                # QQ's 4009 connection timeout permits Resume (unlike an invalid session).
                if ws.close_code in {4006, 4007, *range(4900, 4914)}:
                    self.store.status(session_id=None, seq=None)
                if ws.close_code == 4004:
                    self.refresh_at = 0
                if ws.close_code in {4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915}:
                    raise GatewayBlocked(ws.close_code)
                log.info("gateway_closed code=%s", ws.close_code)
            finally:
                hb.cancel()
                await asyncio.gather(hb, return_exceptions=True)
                self.ws = None
                self.store.status(connection="disconnected", disconnected_at=now())

    async def gateway_loop(self):
        failures = 0
        while True:
            started = time.monotonic()
            try:
                await self.connection()
            except GatewayBlocked as exc:
                self.store.status(connection="blocked", gateway_close_code=exc.code,
                                  retry_at=None, operator_action_required=True)
                log.error("gateway_blocked code=%s operator_action_required", exc.code)
                return
            except (aiohttp.ClientError, asyncio.TimeoutError, APIError, ValueError, KeyError) as exc:
                # Exception strings can include signed CDN URLs; log type only.
                log.warning("gateway_retry type=%s", type(exc).__name__)
                self.store.status(connection="disconnected", last_error_type=type(exc).__name__)
            failures = 0 if time.monotonic() - started > 60 else min(failures + 1, 6)
            delay = min(300, 5 * 2 ** failures) + random.uniform(0, 3)
            self.store.status(reconnects=self.store.get("reconnects", 0) + 1, retry_at=time.time() + delay)
            await asyncio.sleep(delay)

    async def group_loop(self):
        while True:
            row = self.store.db.execute("SELECT group_openid FROM groups WHERE active=1 AND refresh_after<? ORDER BY refresh_after LIMIT 1", (time.time(),)).fetchone()
            if row:
                group = row[0]
                try:
                    info = await self.get(f"/v2/groups/{group}/info")
                    state = await self.get(f"/v2/groups/{group}/bot_state")
                    with self.store.db:
                        self.store.db.execute("UPDATE groups SET group_name=?,info_json=?,bot_state_json=?,refresh_after=?,refresh_error=NULL WHERE group_openid=?",
                            (info.get("group_name"), dumps(clean(info)), dumps(clean(state)), time.time()+3600, group))
                except (aiohttp.ClientError, asyncio.TimeoutError, APIError, ValueError) as exc:
                    with self.store.db:
                        self.store.db.execute("UPDATE groups SET refresh_after=?,refresh_error=? WHERE group_openid=?", (time.time()+300, type(exc).__name__, group))
                    log.warning("group_refresh_retry type=%s", type(exc).__name__)
            await asyncio.sleep(3)  # At most 20 requests/minute per endpoint.

    async def download(self, row):
        url = row["download_url"]
        if url and url.startswith("//"):
            url = "https:" + url
        p = urlsplit(url or "")
        host = p.hostname or ""
        if p.scheme != "https" or not any(host == domain or host.endswith("." + domain)
                                               for domain in ("qq.com", "qq.com.cn", "qpic.cn", "gtimg.cn")):
            raise APIError("untrusted_media_url")
        digest_name = hashlib.sha256((row["group_openid"] + row["message_id"] + row["source"]).encode()).hexdigest()
        folder = self.store.root / "data/media" / digest_name[:2]
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / digest_name
        partial = target.with_suffix(".part")
        try:
            # Separate request with NO bot Authorization; never follow unchecked redirects.
            async with self.http.get(url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=60)) as response:
                if response.status != 200:
                    raise APIError(f"media_http_{response.status}")
                if response.content_length and response.content_length > MAX_MEDIA:
                    raise OverflowError("size_limit")
                size, digest = 0, hashlib.sha256()
                with partial.open("wb") as output:
                    async for chunk in response.content.iter_chunked(64*1024):
                        size += len(chunk)
                        if size > MAX_MEDIA:
                            raise OverflowError("size_limit")
                        digest.update(chunk)
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
                partial.replace(target)
                return str(target.relative_to(self.store.root)), digest.hexdigest(), size
        finally:
            partial.unlink(missing_ok=True)

    async def media_loop(self):
        while True:
            row = self.store.db.execute("SELECT * FROM attachments WHERE status='pending' AND next_attempt<=? ORDER BY id DESC LIMIT 1", (time.time(),)).fetchone()
            if not row:
                await asyncio.sleep(.5)
                continue
            try:
                path, digest, size = await self.download(row)
                with self.store.db:
                    self.store.db.execute("UPDATE attachments SET status='ok',download_url=NULL,local_path=?,sha256=?,actual_size=?,attempts=attempts+1,error=NULL WHERE id=?", (path, digest, size, row["id"]))
            except (aiohttp.ClientError, asyncio.TimeoutError, APIError, OverflowError, ValueError) as exc:
                attempts = row["attempts"] + 1
                status = "skipped" if isinstance(exc, OverflowError) else ("failed" if attempts >= 3 else "pending")
                with self.store.db:
                    self.store.db.execute("UPDATE attachments SET status=?,attempts=?,next_attempt=?,error=?,download_url=? WHERE id=?",
                        (status, attempts, time.time() + 2 ** attempts, type(exc).__name__, row["download_url"] if status == "pending" else None, row["id"]))
                log.warning("media_attempt status=%s type=%s", status, type(exc).__name__)

    async def maintenance_loop(self):
        while True:
            self.store.status(process_alive_at=now())
            date = now()[:10]
            if self.store.get("backup_date") != date:
                folder = self.store.root / "backups"
                folder.mkdir(exist_ok=True)
                target = folder / f"messages-{date}.sqlite3"
                with sqlite3.connect(target) as dest:
                    self.store.db.backup(dest)
                self.store.status(backup_date=date, last_backup_at=now())
                for old in sorted(folder.glob("messages-*.sqlite3"))[:-7]:
                    old.unlink()
            await asyncio.sleep(30)

    async def run(self):
        self.store.status(started_at=now(), connection="starting", pid=os.getpid(),
                          gateway_close_code=None, operator_action_required=False)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as self.http:
            async with asyncio.TaskGroup() as tasks:
                for coroutine in (self.gateway_loop(), self.group_loop(), self.media_loop(), self.maintenance_loop()):
                    tasks.create_task(coroutine)


def report(root):
    db = sqlite3.connect(f"file:{root / 'data/messages.sqlite3'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    result = {"state": {r[0]: json.loads(r[1]) for r in db.execute("SELECT key,value FROM state WHERE key NOT IN ('session_id','seq')")},
              "messages": db.execute("SELECT count(*) FROM messages").fetchone()[0],
              "groups": [dict(r) for r in db.execute("SELECT g.group_name,g.active,g.bot_state_json,g.refresh_error,count(m.id) AS messages,max(m.received_at) AS last_received_at FROM groups g LEFT JOIN messages m ON g.group_openid=m.group_openid GROUP BY g.group_openid")],
              "attachments": {r[0]: r[1] for r in db.execute("SELECT status,count(*) FROM attachments GROUP BY status")}}
    db.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run", "status"])
    args = parser.parse_args()
    if args.command == "status":
        report(ROOT)
        return
    (ROOT / "logs").mkdir(exist_ok=True)
    handler = RotatingFileHandler(ROOT / "logs/collector.log", maxBytes=5*1024*1024, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    (ROOT / "data").mkdir(exist_ok=True)
    with (ROOT / "data/collector.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Collector already running")
        credentials = json.loads((ROOT / "config/credentials.json").read_text())
        store = Store(ROOT)
        async def serve():
            task = asyncio.create_task(Collector(store, credentials).run())
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, task.cancel)
            try:
                await task
            except asyncio.CancelledError:
                pass
        try:
            asyncio.run(serve())
        except Exception as exc:
            log.error("service_failed type=%s", type(exc).__name__)
            raise SystemExit(1) from None
        finally:
            store.status(connection="stopped", stopped_at=now())
            store.db.close()


if __name__ == "__main__":
    main()
