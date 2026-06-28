#!/usr/bin/env python3
"""
claude-code-qq-bridge.py v3.0 - Claude Code QQ Bridge

Architecture:
  QQ → bridge → tmux send-keys → Claude Code (interactive mode)
     ↑                                      ↓
     +—— session file: waitingFor → send button
     +—— JSONL: assistant text → push reply
     +—— QQ button → tmux send-keys → Claude continues

Principles:
  - Single session保活
  - Only read fixed structure fields, no content analysis
  - Two independent channels: session status + JSONL
  - Cache only for restart detection
  - No group messages
  - No Future/state machine
"""

import asyncio
import json
import os
import sys
import time
import uuid
import logging
from typing import Optional, Dict
from pathlib import Path

# ================= .env 配置加载 =================
def load_env():
    """极简 .env 解析，零依赖"""
    candidates = [
        Path(".env"),
        Path(__file__).parent / ".env",
        Path("/root/claude-code-qq-bridge/.env"),
    ]
    for p in candidates:
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        key, val = line.split("=", 1)
                        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
                break
            except Exception:
                pass

load_env()

APP_ID = os.environ.get("APP_ID", "")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
MASTER_OPENID = os.environ.get("MASTER_OPENID", "")
TMUX_SESSION = os.environ.get("TMUX_SESSION", "1")
CLAUDE_HOME = str(Path.home() / ".claude")
CLAUDE_PROJECT = "-root"
API_BASE = "https://api.sgroup.qq.com"
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
GATEWAY_URL_PATH = "/gateway"
CONNECT_TIMEOUT = 20
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]
HEARTBEAT_INTERVAL = 15.0

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "claude-code-qq-bridge.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("claude_code_bridge")

# === State: single session保活 ===
_session_id: Optional[str] = None
_log_path: Optional[str] = None
_pid: Optional[int] = None
_session_file: Optional[Path] = None
_jsonl_watermark: int = 0

_access_token: Optional[str] = None
_token_expires_at: float = 0.0
_ws = None
_http_client = None
_running = False
_last_seq: Optional[int] = None
_last_msg_id: Optional[str] = None
_bot_openid: str = ""


def find_current_session():
    """Find the current interactive session at startup or after restart."""
    sessions_dir = Path(CLAUDE_HOME) / "sessions"
    if not sessions_dir.exists():
        return None, None, None

    best_sid = None
    best_pid = None
    best_mtime = 0

    for sf in sessions_dir.glob("*.json"):
        try:
            with open(sf) as f:
                data = json.load(f)
            pid = data.get("pid")
            sid = data.get("sessionId")
            kind = data.get("kind", "")
            entrypoint = data.get("entrypoint", "")
            if pid and sid and kind == "interactive" and entrypoint in ("cli", "sdk-ts"):
                # Skip zombie: verify process is actually alive
                try:
                    os.kill(pid, 0)
                except OSError:
                    logger.debug(f"Skipping dead session: PID {pid} ({sf.name})")
                    continue
                mtime = sf.stat().st_mtime
                if mtime > best_mtime:
                    best_mtime = mtime
                    best_sid = sid
                    best_pid = pid
        except (json.JSONDecodeError, IOError):
            continue

    if not best_sid:
        return None, None, None

    log_path = str(Path(CLAUDE_HOME) / "projects" / CLAUDE_PROJECT / f"{best_sid}.jsonl")
    return best_sid, log_path, best_pid


def refresh_session():
    """Refresh session location after startup or restart."""
    global _session_id, _log_path, _pid, _session_file, _jsonl_watermark
    sid, log_path, pid = find_current_session()
    if sid:
        if _session_id is None or sid != _session_id:
            # New session or first init → skip old JSONL, only push new messages
            _jsonl_watermark = _count_jsonl_lines(log_path)
        _session_id = sid
        _log_path = log_path
        _pid = pid
        _session_file = Path(CLAUDE_HOME) / "sessions" / f"{pid}.json"
        logger.info(f"Session: {sid} (PID: {pid})")
        logger.info(f"Log: {log_path} (watermark={_jsonl_watermark})")
        return True
    return False


def _count_jsonl_lines(path: str) -> int:
    """Count lines in JSONL file without loading content."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return sum(1 for _ in f)
    except IOError:
        return 0


def maybe_refresh_session() -> bool:
    """Refresh session if the current session file disappeared (Claude Code restarted)."""
    if _session_file and not _session_file.exists():
        logger.info("Session file gone, refreshing...")
        return refresh_session()
    return True


def get_session_status() -> Optional[Dict]:
    """Read session file for waitingFor field."""
    if not _session_file or not _session_file.exists():
        return None
    try:
        with open(_session_file) as f:
            data = json.load(f)
        return {
            "status": data.get("status"),
            "waitingFor": data.get("waitingFor"),
        }
    except (json.JSONDecodeError, IOError):
        return None


async def send_to_claude(message: str):
    """Send message to Claude Code via tmux."""
    proc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, "Escape", ""
    )
    await proc.communicate()
    await asyncio.sleep(0.3)
    proc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, message, ""
    )
    await proc.communicate()
    await asyncio.sleep(0.1)
    proc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, "Enter", ""
    )
    await proc.communicate()
    logger.info(f"[Bridge -> Claude] {message[:100]}")





async def start_claude_in_tmux():
    """Start Claude Code in tmux. Always kills any existing Claude first."""
    # Ensure tmux session exists
    proc = await asyncio.create_subprocess_exec(
        "tmux", "has-session", "-t", TMUX_SESSION,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    if proc.returncode != 0:
        proc = await asyncio.create_subprocess_exec(
            "tmux", "new-session", "-d", "-s", TMUX_SESSION,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        await asyncio.sleep(1)
    # Clear any stale input on shell line before starting Claude
    for key in ["C-c", "C-c"]:
        proc = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", TMUX_SESSION, key, ""
        )
        await proc.communicate()
        await asyncio.sleep(0.2)
    # Start fresh Claude
    proc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION,
        "cd /root && script -q -c 'claude' /dev/null", "Enter"
    )
    await proc.communicate()
    # Wait for trust prompt, then press "1" to confirm
    await asyncio.sleep(5)
    proc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, "1", ""
    )
    await proc.communicate()
    await asyncio.sleep(3)
    refresh_session()


async def stop_claude_in_tmux():
    """Stop Claude Code with Ctrl+C."""
    for key in ["C-c", "Enter", "C-c"]:
        proc = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", TMUX_SESSION, key, ""
        )
        await proc.communicate()
        await asyncio.sleep(0.3)
    logger.info("Sent Ctrl+C to Claude")


async def restart_claude_in_tmux():
    """Restart Claude Code (kill and start new)."""
    logger.info("Restarting Claude Code...")
    await stop_claude_in_tmux()
    await asyncio.sleep(2)
    proc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION,
        "cd /root && script -q -c 'claude' /dev/null", "Enter"
    )
    await proc.communicate()
    # Wait for trust prompt, then press "1" to confirm
    await asyncio.sleep(5)
    proc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, "1", ""
    )
    await proc.communicate()
    await asyncio.sleep(3)
    refresh_session()
    logger.info("Claude Code restarted")


def build_approval_keyboard() -> dict:
    """Build QQ approval button keyboard in Chinese."""
    return {
        "content": {
            "rows": [
                {
                    "buttons": [
                        {
                            "id": "btn_allow",
                            "render_data": {"label": "✅ 允许一次", "visited_label": "已允许", "style": 1},
                            "action": {"type": 2, "permission": {"type": 2}, "data": "approve:default:allow"},
                        },
                        {
                            "id": "btn_always",
                            "render_data": {"label": "🛡️ 始终允许", "visited_label": "已始终允许", "style": 1},
                            "action": {"type": 2, "permission": {"type": 2}, "data": "approve:default:allow_always"},
                        },
                    ]
                },
                {
                    "buttons": [
                        {
                            "id": "btn_deny",
                            "render_data": {"label": "❌ 拒绝", "visited_label": "已拒绝", "style": 0},
                            "action": {"type": 2, "permission": {"type": 2}, "data": "approve:default:deny"},
                        },
                    ]
                },
            ]
        }
    }


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
        headers={"Authorization": f"QQBot {token}", "User-Agent": "ClaudeCode-QQ-Bridge/3.0"},
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
            "properties": {"$os": "Linux", "$browser": "claude-code-qq-bridge", "$device": "claude-code-qq-bridge"},
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


def _next_msg_seq(msg_id: str = 'default') -> int:
    time_part = int(time.time()) % 100000000
    rnd = int(uuid.uuid4().hex[:4], 16)
    return (time_part ^ rnd) % 65536


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


async def send_message_rest(user_openid: str, content: str, *, keyboard: bool = False) -> bool:
    """Send message to QQ user. If keyboard=True, append approval buttons."""
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "ClaudeCode-QQ-Bridge/3.0",
    }
    msg_seq = _next_msg_seq(user_openid)
    display_content = content[:1500] + "\n\n... (truncated)" if len(content) > 1500 else content
    body = {"markdown": {"content": display_content}, "msg_type": 2, "msg_seq": msg_seq}

    if keyboard:
        body["keyboard"] = build_approval_keyboard()

    try:
        resp = await client.post(f"{API_BASE}/v2/users/{user_openid}/messages", headers=headers, json=body, timeout=30.0)
        if resp.status_code >= 400:
            logger.error(f"Send failed [{resp.status_code}]: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"Send exception: {e}")
        return False


async def _wait_for_approval():
    """Block until approval is cleared. Returns when session leaves waiting state."""
    while True:
        status = get_session_status()
        if not status or status.get("waitingFor") != "permission prompt":
            return
        await asyncio.sleep(0.5)


async def periodic_poll():
    """Background polling: detect approval + push replies.
    Order: JSONL text first, then approval button — so user sees
    Claude's message before being asked to approve."""
    global _jsonl_watermark
    while True:
        await asyncio.sleep(3)
        try:
            # 0. Refresh session if process died
            if not maybe_refresh_session():
                continue

            # 1. Read JSONL for new assistant replies (BEFORE approval check)
            new_texts = []
            if _log_path and Path(_log_path).exists():
                with open(_log_path, "r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()
                new_lines = lines[_jsonl_watermark:]
                if new_lines:
                    _jsonl_watermark = len(lines)
                    for line in new_lines:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if obj.get("type") == "assistant":
                            content = obj.get("message", {}).get("content", [])
                            for block in content:
                                if isinstance(block, dict) and block.get("type") == "text":
                                    t = block.get("text", "").strip()
                                    if t:
                                        new_texts.append(t)

            # 2. Check approval status (session file)
            status = get_session_status()
            approval_pending = status and status.get("waitingFor") == "permission prompt"

            if approval_pending:
                # Send text first (as normal message), then approval button (separate message)
                if new_texts:
                    reply = "\n\n".join(new_texts)
                    logger.info(f"[Poll -> QQ Text] {reply[:80]}")
                    await send_message_rest(MASTER_OPENID, reply[:1500])
                    await asyncio.sleep(0.3)  # small gap to avoid QQ rate limit

                logger.info("[Poll] Approval detected, sending QQ button")
                await send_message_rest(MASTER_OPENID, "🔐 **Claude Code 需要您的确认**", keyboard=True)

                # Block until user handles approval
                await _wait_for_approval()
                logger.info("[Poll] Approval handled, resumed polling")
                continue

            # 3. No approval pending — just push text if any
            if new_texts:
                reply = "\n\n".join(new_texts)
                logger.info(f"[Poll -> QQ] {reply[:100]}")
                await send_message_rest(MASTER_OPENID, reply[:1500])
        except Exception as e:
            logger.error(f"[Poll] error: {e}")


async def handle_c2c_message(d: dict):
    """Handle C2C message from QQ user."""
    global _last_msg_id, _bot_openid
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

    # Approval button callback
    if content.startswith("approve:"):
        parts = content.split(":")
        if len(parts) >= 3:
            keystroke = {"allow": "1", "allow_always": "2", "deny": "3"}.get(parts[2])
            if keystroke:
                logger.info(f"[Approval] Sending keystroke: {keystroke}")
                proc = await asyncio.create_subprocess_exec(
                    "tmux", "send-keys", "-t", TMUX_SESSION, keystroke, ""
                )
                await proc.communicate()
        return

    # Commands
    if content.strip().lower() in ["/new", "/reset", "/qingkong", "/xin duihua"]:
        logger.info("[Recv] New session command")
        await restart_claude_in_tmux()
        await send_message_rest(user_openid, "Session restarted.")
        return
    if content.strip().lower() in ["/stop", "/tingzhi", "/kill"]:
        logger.info("[Recv] Stop command")
        await stop_claude_in_tmux()
        await send_message_rest(user_openid, "⛔ Interrupted.")
        return

    # Regular message → Claude
    logger.info(f"[QQ -> Claude] {content}")
    await send_to_claude(content)


async def handle_interaction(d: dict):
    """Handle QQ button callback - send keystroke directly to tmux."""
    interaction_id = d.get("id")
    if not interaction_id:
        return

    # ACK within 3 seconds
    token = await ensure_token()
    client = get_http_client()
    try:
        await client.put(
            f"{API_BASE}/interactions/{interaction_id}",
            headers={
                "Authorization": f"QQBot {token}",
                "Content-Type": "application/json",
                "User-Agent": "ClaudeCode-QQ-Bridge/3.0",
            },
            json={"code": 0},
            timeout=5.0,
        )
    except Exception as e:
        logger.error(f"[Interaction] ACK failed: {e}")

    # Parse button data
    author = d.get("author") or {}
    user_openid = d.get("user_openid") or author.get("user_openid")
    if not user_openid:
        user_openid = author.get("member_openid")
    if user_openid != MASTER_OPENID:
        logger.warning(f"[Interaction] Unauthorized: {user_openid}")
        return

    data_block = d.get("data", {})
    button_data = data_block.get("button_data", "")

    if button_data.startswith("approve:"):
        parts = button_data.split(":")
        if len(parts) >= 3:
            keystroke = {"allow": "1", "allow_always": "2", "deny": "3"}.get(parts[2])
            if keystroke:
                proc = await asyncio.create_subprocess_exec(
                    "tmux", "send-keys", "-t", TMUX_SESSION, keystroke, ""
                )
                await proc.communicate()
                logger.info(f"[Interaction] Sent keystroke: {keystroke}")


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
    heartbeat_task = asyncio.create_task(_heartbeat_sender(ws, HEARTBEAT_INTERVAL))
    try:
        while _running and ws and not ws.closed:
            msg = await ws.receive()
            if msg.type == 1:
                try:
                    payload = json.loads(msg.data)
                except json.JSONDecodeError:
                    logger.warning(f"JSON parse error: {msg.data[:100]}")
                    continue
                op = payload.get("op")
                t = payload.get("t")
                s = payload.get("s")
                d = payload.get("d")
                if isinstance(s, int) and (_last_seq is None or s > _last_seq):
                    _last_seq = s
                if op == 10:
                    d_data = d if isinstance(d, dict) else {}
                    interval_ms = d_data.get("heartbeat_interval", 30000)
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
                        _session_id = d.get("session_id")
                        user = d.get("user") if isinstance(d.get("user"), dict) else {}
                        _bot_openid = str(user.get("id", ""))
                        logger.info(f"READY, session_id={_session_id}, bot_openid={_bot_openid}")
                    elif t == "RESUMED":
                        logger.info("Session resumed")
                    elif t == "C2C_MESSAGE_CREATE":
                        task = asyncio.create_task(handle_c2c_message(d))
                        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                    elif t == "INTERACTION_CREATE":
                        task = asyncio.create_task(handle_interaction(d))
                        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                    continue
            elif msg.type == 9:
                logger.warning("WS close received")
                break
    except Exception as e:
        logger.error(f"Event loop error: {e}")


async def main():
    global _running
    _running = True
    logger.info("Starting Claude Code QQ Bridge v3.0...")

    # 1. Start Claude Code in tmux
    await start_claude_in_tmux()
    if not _session_id:
        logger.warning("No Claude Code session found, retrying in 10s...")
        await asyncio.sleep(10)
        refresh_session()

    # 2. Start background polling
    asyncio.create_task(periodic_poll())

    # 3. Connect QQ Bot gateway
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
                logger.error(f"WS error: {e}")
                await asyncio.sleep(RECONNECT_BACKOFF[0])
    logger.info("Bridge stopped")


if __name__ == "__main__":
    if not APP_ID or not CLIENT_SECRET or not MASTER_OPENID:
        logger.error("Missing config: APP_ID, CLIENT_SECRET, MASTER_OPENID")
        sys.exit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
