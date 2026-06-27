#!/usr/bin/env python3
"""
claude-code-qq-bridge.py — Claude Code QQ 桥接网关（Stateless Router）

架构:
  QQ → bridge → subprocess claude -p "message" → stdout → QQ

设计原则（严格）:
  - 纯转发，不解析 agent 内部状态
  - 不检测关键词，不做审批推断
  - 不注入 stdin 控制
  - 异常输出返回 "AGENT REQUIRES INTERACTION (manual terminal required)"
  - 保持 stateless，不存会话上下文

启动: python3 claude-code-qq-bridge.py
依赖: pip install aiohttp httpx
"""

import asyncio
import json
import os
import sys
import time
import logging
import uuid
import re
import shlex
import subprocess
from typing import Optional, Dict, Any
from pathlib import Path

# ================= 配置 =================
APP_ID = ""
CLIENT_SECRET = ""
MASTER_OPENID = ""
CLAUDE_BIN = "claude"
TIMEOUT = 180  # claude -p 最长等 3 分钟

API_BASE = "https://api.sgroup.qq.com"
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
GATEWAY_URL_PATH = "/gateway"

CONNECT_TIMEOUT = 20
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]
MAX_RECONNECT_ATTEMPTS = 100
HEARTBEAT_INTERVAL = 15.0
# =========================================

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/claude-code-qq-bridge.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("claude_code_bridge")


# ---- 配置加载 ----

def load_env():
    """从 .env 加载配置"""
    paths = [
        Path(".env"),
        Path(__file__).parent / ".env",
        Path.cwd() / ".env",
    ]
    for p in paths:
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            key, val = line.split("=", 1)
                            os.environ[key.strip()] = val.strip().strip('"').strip("'")
                break
            except Exception:
                pass

    global APP_ID, CLIENT_SECRET, MASTER_OPENID
    APP_ID = os.environ.get("APP_ID", "")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
    MASTER_OPENID = os.environ.get("MASTER_OPENID", "")

    missing = []
    if not APP_ID:
        missing.append("APP_ID")
    if not CLIENT_SECRET:
        missing.append("CLIENT_SECRET")
    if not MASTER_OPENID:
        missing.append("MASTER_OPENID")

    if missing:
        logger.error(f"Missing required config: {', '.join(missing)}")
        logger.error("Create .env from .env.example and fill in values")
        sys.exit(1)


# ---- Claude Code 调用 ----

async def call_claude(prompt: str) -> str:
    """
    调用 claude -p 并返回 stdout。
    纯透传，不做任何状态推断。
    超时或异常 → 返回 "AGENT REQUIRES INTERACTION (manual terminal required)"
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            CLAUDE_BIN, "-p", prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            logger.warning(f"Claude -p timed out after {TIMEOUT}s")
            return "AGENT REQUIRES INTERACTION (manual terminal required)"

        output = stdout.decode("utf-8", errors="replace").strip()
        err = stderr.decode("utf-8", errors="replace").strip()

        if err:
            logger.debug(f"Claude stderr: {err[:200]}")

        if proc.returncode != 0:
            logger.warning(f"Claude exit code: {proc.returncode}, stderr: {err[:200]}")
            # exit code non-zero 但对 stdout 有输出的话仍然返回
            if output:
                return output
            return "AGENT REQUIRES INTERACTION (manual terminal required)"

        return output if output else "[empty response]"

    except FileNotFoundError:
        logger.error(f"Claude binary not found: {CLAUDE_BIN}")
        return "[ERROR: claude not found]"
    except Exception as e:
        logger.error(f"Claude subprocess error: {e}")
        return f"[ERROR: {str(e)[:100]}]"


# ---- QQ Bot 基础设施 ----
# （从 AGY 桥继承，去掉所有审批/终端解析相关逻辑）

_access_token: Optional[str] = None
_token_expires_at: float = 0.0
_session_id: Optional[str] = None
_last_seq: Optional[int] = None
_ws = None
_http_client = None
_running = False
_last_msg_id: Optional[str] = None
_bot_openid: str = ""
heartbeat_task = None
_processing = False


async def send_message_rest(user_openid: str, content: str) -> bool:
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "ClaudeCode-QQ-Bridge/1.0",
    }
    msg_seq = _next_msg_seq(user_openid)
    display_content = content[:3990] + "\n\n... (已截断)" if len(content) > 4000 else content
    body = {
        "markdown": {"content": display_content},
        "msg_type": 2,
        "msg_seq": msg_seq,
    }

    try:
        resp = await client.post(
            f"{API_BASE}/v2/users/{user_openid}/messages",
            headers=headers,
            json=body,
            timeout=30.0,
        )
        if resp.status_code >= 400:
            logger.error(f"Send failed [{resp.status_code}]: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"Send exception: {e}")
        return False


async def send_group_message_rest(group_openid: str, content: str, reply_to: Optional[str] = None) -> bool:
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "ClaudeCode-QQ-Bridge/1.0",
    }
    msg_seq = _next_msg_seq(group_openid)
    display_content = content[:3990] + "\n\n... (已截断)" if len(content) > 4000 else content
    body = {
        "markdown": {"content": display_content},
        "msg_type": 2,
        "msg_seq": msg_seq,
    }
    if reply_to:
        body["msg_id"] = reply_to

    try:
        resp = await client.post(
            f"{API_BASE}/v2/groups/{group_openid}/messages",
            headers=headers,
            json=body,
            timeout=30.0,
        )
        if resp.status_code >= 400:
            logger.error(f"Group send failed [{resp.status_code}]: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"Group send exception: {e}")
        return False


def get_http_client():
    global _http_client
    if _http_client is None:
        import httpx
        _http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    return _http_client


async def ensure_token() -> str:
    global _access_token, _token_expires_at
    if _access_token and time.time() < _token_expires_at - 60:
        return _access_token
    client = get_http_client()
    resp = await client.post(
        TOKEN_URL,
        json={"appId": APP_ID, "clientSecret": CLIENT_SECRET},
        timeout=30.0,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Failed to get token: {data}")
    expires_in = int(data.get("expires_in", 7200))
    _access_token = token
    _token_expires_at = time.time() + expires_in
    logger.info(f"Token refreshed, expires in {expires_in}s")
    return token


async def get_gateway_url() -> str:
    token = await ensure_token()
    client = get_http_client()
    resp = await client.get(
        f"{API_BASE}{GATEWAY_URL_PATH}",
        headers={"Authorization": f"QQBot {token}", "User-Agent": "ClaudeCode-QQ-Bridge/1.0"},
        timeout=30.0,
    )
    resp.raise_for_status()
    data = resp.json()
    url = data.get("url")
    if not url:
        raise RuntimeError(f"Failed to get gateway URL: {data}")
    return url


async def send_identify(ws):
    token = await ensure_token()
    payload = {
        "op": 2,
        "d": {
            "token": f"QQBot {token}",
            "intents": (1 << 25) | (1 << 30) | (1 << 12) | (1 << 26),
            "shard": [0, 1],
            "properties": {
                "$os": "Linux",
                "$browser": "claude-code-qq-bridge",
                "$device": "claude-code-qq-bridge",
            },
        },
    }
    await ws.send_json(payload)
    logger.info("Identify sent")


async def send_resume(ws):
    payload = {
        "op": 6,
        "d": {"token": f"QQBot {_access_token}", "session_id": _session_id, "seq": _last_seq},
    }
    await ws.send_json(payload)
    logger.info(f"Resume sent (session={_session_id}, seq={_last_seq})")


def _next_msg_seq(_msg_id: str = "default") -> int:
    time_part = int(time.time()) % 100000000
    rand = int(uuid.uuid4().hex[:4], 16)
    return (time_part ^ rand) % 65536


_seen_messages: Dict[str, float] = {}


def is_duplicate(msg_id: str) -> bool:
    now = time.time()
    if msg_id in _seen_messages and now - _seen_messages[msg_id] < 300:
        return True
    _seen_messages[msg_id] = now
    if len(_seen_messages) > 1000:
        for k in list(_seen_messages.keys()):
            if now - _seen_messages[k] > 600:
                del _seen_messages[k]
    return False


# ---- 消息处理 ----

async def handle_c2c_message(d: dict):
    global _last_msg_id, _processing, _bot_openid

    msg_id = str(d.get("id", ""))
    if not msg_id or is_duplicate(msg_id):
        return

    content = str(d.get("content", "")).strip()
    author = d.get("author") if isinstance(d.get("author"), dict) else {}
    user_openid = str(author.get("user_openid", ""))

    if not user_openid or not content:
        return

    _last_msg_id = msg_id
    logger.info(f"[Recv] openid={user_openid}: {content[:100]}")

    if user_openid != MASTER_OPENID:
        logger.info(f"[Skip] non-master openid: {user_openid}")
        return

    if _processing:
        logger.info("[Skip] busy, message queued")
        return

    _processing = True
    try:
        # 转发到 Claude Code
        await send_message_rest(user_openid, "⏳ Claude Code 正在思考...")
        logger.info(f"[QQ -> Claude] {content[:100]}")
        reply = await call_claude(content)
        logger.info(f"[Claude -> QQ] {reply[:200]}")
    except Exception as e:
        reply = f"[ERROR] {str(e)[:200]}"
        logger.error(f"Processing error: {e}")
    finally:
        _processing = False

    await send_message_rest(user_openid, reply)


async def handle_group_message(d: dict, event_type: str):
    global _last_msg_id, _processing, _bot_openid

    msg_id = str(d.get("id", ""))
    if not msg_id or is_duplicate(msg_id):
        return

    group_openid = str(d.get("group_openid", ""))
    content = str(d.get("content", "")).strip()
    author = d.get("author") if isinstance(d.get("author"), dict) else {}
    member_openid = str(author.get("member_openid", ""))

    if not group_openid or not content:
        return

    # 检测 @ 提及
    is_mentioned = False
    my_openid_in_group = ""

    mentions = d.get("mentions") or []
    for m in mentions:
        if m.get("is_you") is True:
            my_openid_in_group = m.get("member_openid") or m.get("id") or m.get("user_openid") or ""
            break

    if event_type == "GROUP_AT_MESSAGE_CREATE":
        is_mentioned = True
    elif event_type == "GROUP_MESSAGE_CREATE":
        if my_openid_in_group:
            is_mentioned = True
        elif _bot_openid and mentions:
            for m in mentions:
                mid = m.get("member_openid") or m.get("id") or m.get("user_openid") or ""
                if str(mid) == str(_bot_openid):
                    is_mentioned = True
                    break

    if not is_mentioned:
        return

    _last_msg_id = msg_id
    logger.info(f"[Group Recv] group={group_openid} member={member_openid}: {content[:100]}")

    if member_openid != MASTER_OPENID:
        logger.info(f"[Group Skip] non-master: {member_openid}")
        return

    # 去掉 @ 前缀
    if my_openid_in_group:
        content = re.sub(rf"<@!?{my_openid_in_group}>", "", content).strip()
    if _bot_openid:
        content = re.sub(rf"<@!?{_bot_openid}>", "", content).strip()
    if not content:
        return

    if _processing:
        await send_group_message_rest(group_openid, "⚠️ 当前已有任务正在执行中，请稍候。", reply_to=msg_id)
        return

    _processing = True
    try:
        await send_group_message_rest(group_openid, "⏳ Claude Code 正在思考...", reply_to=msg_id)
        logger.info(f"[QQ Group -> Claude] {content[:100]}")
        reply = await call_claude(content)
        logger.info(f"[Claude -> QQ Group] {reply[:200]}")
    except Exception as e:
        reply = f"[ERROR] {str(e)[:200]}"
        logger.error(f"Group processing error: {e}")
    finally:
        _processing = False

    await send_group_message_rest(group_openid, reply, reply_to=msg_id)


# ---- WS 事件循环（标准 QQ Bot） ----

async def _heartbeat_sender(ws, interval: float):
    try:
        while _running and ws and not ws.closed:
            await asyncio.sleep(interval)
            if ws and not ws.closed:
                await ws.send_json({"op": 1, "d": _last_seq})
    except asyncio.CancelledError:
        return
    except Exception as e:
        logger.debug(f"Heartbeat error: {e}")


async def event_loop(ws):
    global _session_id, _last_seq, _running, _ws, heartbeat_task
    _ws = ws
    heartbeat_interval = HEARTBEAT_INTERVAL
    heartbeat_task = asyncio.create_task(_heartbeat_sender(ws, heartbeat_interval))

    while _running:
        try:
            while _running and ws and not ws.closed:
                msg = await ws.receive()

                if msg.type == 1:
                    try:
                        payload = json.loads(msg.data)
                    except json.JSONDecodeError:
                        continue

                    op = payload.get("op")
                    t = payload.get("t")
                    s = payload.get("s")
                    d = payload.get("d")

                    if isinstance(s, int) and (_last_seq is None or s > _last_seq):
                        _last_seq = s

                    if op == 10:
                        interval_ms = d.get("heartbeat_interval", 30000) if isinstance(d, dict) else 30000
                        heartbeat_interval = interval_ms / 1000.0 * 0.8
                        logger.info(f"Hello recv, heartbeat={heartbeat_interval:.1f}s")
                        if _session_id and _last_seq is not None:
                            await send_resume(ws)
                        else:
                            await send_identify(ws)
                        continue

                    if op == 0 and t:
                        logger.info(f"[WS Dispatch] event_type={t}")
                        if t == "READY" and isinstance(d, dict):
                            global _bot_openid
                            _session_id = d.get("session_id")
                            user = d.get("user") if isinstance(d.get("user"), dict) else {}
                            _bot_openid = str(user.get("id", ""))
                            logger.info(f"READY, session_id={_session_id}, bot_openid={_bot_openid}")
                        elif t == "RESUMED":
                            logger.info("Session resumed")
                        elif t == "C2C_MESSAGE_CREATE":
                            asyncio.create_task(handle_c2c_message(d))
                        elif t in {"GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"}:
                            asyncio.create_task(handle_group_message(d, t))
                        continue

                elif msg.type == 9:
                    logger.warning("WS close received")
                    break

        except Exception as e:
            logger.error(f"Event loop error: {e}")
            if _running:
                backoff = RECONNECT_BACKOFF[min(0, len(RECONNECT_BACKOFF) - 1)]
                await asyncio.sleep(backoff)


async def main():
    global _running
    _running = True

    try:
        gateway_url = await get_gateway_url()
        logger.info(f"Gateway URL: {gateway_url}")
    except Exception as e:
        logger.error(f"Failed to get gateway: {e}")
        sys.exit(1)

    import aiohttp

    while _running:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    gateway_url,
                    timeout=aiohttp.ClientTimeout(total=CONNECT_TIMEOUT),
                    heartbeat=HEARTBEAT_INTERVAL,
                ) as ws:
                    logger.info("WS connected")
                    await event_loop(ws)
        except asyncio.CancelledError:
            break
        except Exception as e:
            if _running:
                logger.error(f"WS connection error: {e}")
                await asyncio.sleep(RECONNECT_BACKOFF[0])

    logger.info("Bridge stopped")


if __name__ == "__main__":
    load_env()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")