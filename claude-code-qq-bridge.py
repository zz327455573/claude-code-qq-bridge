#!/usr/bin/env python3
"""
claude-code-qq-bridge.py v2.0 - Claude Code QQ Bridge

Architecture:
  QQ User -> QQ Bot WS -> Bridge -> tmux send-keys -> claude (interactive)
     ^                                                    |
     +-------- read project/*.jsonl structured log <------+

Based on AGY QQ Bridge (7 iterations, 48h+ production)
Key lesson: Do NOT parse terminal output, read structured logs
"""

import asyncio
import json
import re
import os
import sys
import time
import uuid
import logging
import glob
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path


def load_env(env_path: str = ".env"):
    """Load .env file, no external dependencies."""
    paths = [
        Path(env_path),
        Path(__file__).parent / env_path,
        Path("/root/.env"),
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


load_env()

APP_ID = os.environ.get("APP_ID", "102888122")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "KKLNPSVZejpw4CLUep0CObp3IYo5NfyI")
MASTER_OPENID = os.environ.get("MASTER_OPENID", "FF86A54C2DFDD5A7E7B18DE4BCA2DB63")
TMUX_SESSION = "claude-code"
CLAUDE_HOME = str(Path.home() / ".claude")
CLAUDE_PROJECT = "-root"
CLAUDE_WORKDIR = "/root"
TIMEOUT = 300
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

_session_cache: Dict[str, Any] = {"sessionId": None, "log_path": None, "pid": None}

def find_claude_session() -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """Find current Claude Code interactive session."""
    sessions_dir = Path(CLAUDE_HOME) / "sessions"
    if not sessions_dir.exists():
        return None, None, None
    best_session = None
    best_pid = None
    best_mtime = 0
    for sf in sessions_dir.glob("*.json"):
        try:
            with open(sf) as fh:
                data = json.load(fh)
            pid = data.get('pid')
            sid = data.get('sessionId')
            kind = data.get('kind', '')
            if pid and sid and kind == 'interactive':
                mtime = sf.stat().st_mtime
                if mtime > best_mtime:
                    best_mtime = mtime
                    best_session = sid
                    best_pid = pid
        except (json.JSONDecodeError, IOError):
            continue
    if not best_session:
        return None, None, None
    project_log = str(Path(CLAUDE_HOME) / "projects" / CLAUDE_PROJECT / f"{best_session}.jsonl")
    return best_session, project_log, best_pid

def refresh_session() -> bool:
    """Refresh session cache."""
    sid, log_path, pid = find_claude_session()
    if sid:
        _session_cache["sessionId"] = sid
        _session_cache["log_path"] = log_path
        _session_cache["pid"] = pid
        logger.info(f"Session: {sid} (PID: {pid})")
        logger.info(f"Log: {log_path}")
        return True
    return False

def get_session_status() -> Optional[str]:
    """Get Claude Code session status."""
    pid = _session_cache.get("pid")
    if not pid:
        return None
    session_file = Path(CLAUDE_HOME) / "sessions" / f"{pid}.json"
    if not session_file.exists():
        return None
    try:
        with open(session_file) as f:
            data = json.load(f)
        return data.get("status")
    except (json.JSONDecodeError, IOError):
        return None

async def send_to_claude(message: str):
    """Send message to Claude Code via tmux send-keys."""
    proc_esc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, "Escape", ""
    )
    await proc_esc.communicate()
    await asyncio.sleep(0.3)
    proc_msg = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, message, ""
    )
    await proc_msg.communicate()
    await asyncio.sleep(0.1)
    proc_enter = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, "Enter", ""
    )
    await proc_enter.communicate()
    logger.info(f"[Bridge -> Claude] {message[:100]}")

async def start_claude_in_tmux():
    """Start/verify Claude Code in tmux session."""
    # Check if tmux session exists
    proc_check = await asyncio.create_subprocess_exec(
        "tmux", "has-session", "-t", TMUX_SESSION,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    await proc_check.communicate()
    if proc_check.returncode != 0:
        # Create new session
        proc_create = await asyncio.create_subprocess_exec(
            "tmux", "new-session", "-d", "-s", TMUX_SESSION,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await proc_create.communicate()
        logger.info(f"Created tmux session: {TMUX_SESSION}")
        await asyncio.sleep(1)
    # Check if Claude is already running via capture-pane
    proc_cap = await asyncio.create_subprocess_exec(
        "tmux", "capture-pane", "-t", TMUX_SESSION, "-p",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc_cap.communicate()
    pane_content = stdout.decode("utf-8", errors="replace") if stdout else ""
    if len(pane_content.strip()) < 20:
        cmd = f"cd {CLAUDE_WORKDIR} && script -q -c 'claude' /dev/null"
        proc_start = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", TMUX_SESSION, cmd, "Enter"
        )
        await proc_start.communicate()
        logger.info("Started Claude Code in tmux")
        await asyncio.sleep(5)
        # Close any initial dialogs with Escape
        proc_esc = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", TMUX_SESSION, "Escape", ""
        )
        await proc_esc.communicate()
        await asyncio.sleep(1)
    refresh_session()

async def stop_claude_in_tmux():
    """Send Ctrl+C to interrupt current operation."""
    for key in ["C-c", "Enter", "C-c"]:
        proc = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", TMUX_SESSION, key, ""
        )
        await proc.communicate()
        await asyncio.sleep(0.3)
    logger.info("Sent Ctrl+C to Claude")

async def restart_claude_in_tmux():
    """Restart Claude Code (/new command)."""
    logger.info("Restarting Claude Code...")
    for key in ["C-c", "Enter", "C-d", "C-c"]:
        proc = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", TMUX_SESSION, key, ""
        )
        await proc.communicate()
        await asyncio.sleep(0.5)
    proc_q = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, "q", ""
    )
    await proc_q.communicate()
    await asyncio.sleep(1)
    cmd = f"cd {CLAUDE_WORKDIR} && script -q -c 'claude' /dev/null"
    proc_start = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, cmd, "Enter"
    )
    await proc_start.communicate()
    await asyncio.sleep(5)
    proc_esc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", TMUX_SESSION, "Escape", ""
    )
    await proc_esc.communicate()
    await asyncio.sleep(1)
    refresh_session()
    logger.info("Claude Code restarted")

ANSI_ESCAPE = re.compile(r"\x1B(?:[@-Z\\-_]|\\[[0-?]*[ -/]*[@-~])")

def clean_ansi(text: str) -> str:
    """Clean ANSI control characters."""
    text = ANSI_ESCAPE.sub("", text)
    text = re.sub(r'[\x01-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    return text.strip()

def capture_pane() -> str:
    """(Fallback only) Read tmux pane for approval detection."""
    try:
        import subprocess
        result = subprocess.run(
            ["tmux", "capture-pane", "-t", TMUX_SESSION, "-p"],
            capture_output=True, text=True, timeout=5,
        )
        return clean_ansi(result.stdout)
    except Exception as e:
        logger.debug(f"capture-pane error: {e}")
        return ""

async def wait_for_claude_response(timeout=TIMEOUT, user_openid: str = '', group_openid: str = '', reply_to: str = None) -> str:
    """
    Wait for Claude Code response from JSONL structured log.
    This is the core function replacing AGY bridge's wait_for_agy_response().
    """
    log_path = _session_cache.get("log_path")
    if not log_path or not Path(log_path).exists():
        refresh_session()
        log_path = _session_cache.get("log_path")
        if not log_path:
            return "[Claude Code: session log not found]"

    try:
        watermark = Path(log_path).stat().st_size
    except FileNotFoundError:
        return "[Claude Code: log file not found]"

    async def send_status(content: str):
        if group_openid:
            await send_group_message_rest(group_openid, content, reply_to=reply_to)
        elif user_openid:
            await send_message_rest(user_openid, content)

    start = time.time()
    last_push = time.time()
    last_activity = time.time()
    last_size = watermark

    while time.time() - start < timeout:
        await asyncio.sleep(0.5)
        try:
            current_size = Path(log_path).stat().st_size
        except FileNotFoundError:
            continue
        if current_size != last_size:
            last_activity = time.time()
            last_size = current_size
        if current_size > watermark:
            with open(log_path, 'r', encoding='utf-8', errors='replace') as lf:
                lf.seek(watermark)
                new_lines = lf.read().splitlines()
            for line in new_lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if obj.get("type") == "assistant":
                    msg = obj.get("message", {})
                    content = msg.get("content", [])
                    if isinstance(content, list):
                        texts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
                        if texts:
                            combined = ("\n").join(texts).strip()
                            if combined:
                                return combined
                    elif isinstance(content, str):
                        if content.strip():
                            return content.strip()
            watermark = current_size

        # Progress notification every 60s
        now = time.time()
        if now - last_push >= 60:
            elapsed = int(now - start)
            inactive = int(now - last_activity)
            if inactive < 60:
                status_msg = f"\u23f3 Task in progress ({elapsed}s)..."
            else:
                status_msg = f"\u26a0\ufe0f Task running ({elapsed}s), no recent activity..."
            await send_status(status_msg)
            last_push = now

        # Check if stuck on approval
        if time.time() - last_activity > 15:
            status = get_session_status()
            if status and status != 'idle':
                pane = capture_pane()
                if is_permission_prompt(pane):
                    return '__APPROVAL_REQUIRED__'

    elapsed = int(time.time() - start)
    return f"\u26a0\ufe0f Task timed out after {elapsed}s. Send [/stop] to force stop."

def is_permission_prompt(text: str) -> bool:
    """Detect if text shows a Claude Code permission prompt."""
    patterns = [
        r'Allow\s+(?:this|the)\s+(?:operation|command|tool)',
        r'requires\s+permission',
        r'need\s+your\s+approval',
        r'Allow\?',
        r'\[y/N\]',
        r'\(y/N\)',
        r'Deny',
        r'Always\s+allow',
        r'Allow\s+once',
        r'Approve',
    ]
    for p in patterns:
        if re.search(p, text, re.IGNORECASE):
            return True
    return False

def build_approval_keyboard() -> dict:
    """Build QQ approval button keyboard."""
    return {
        "content": {
            "rows": [
                {
                    "buttons": [
                        {
                            "id": "btn_allow",
                            "render_data": {"label": "\u2705 Allow once", "visited_label": "Allowed", "style": 1},
                            "action": {"type": 2, "permission": {"type": 2}, "data": "approve:default:allow"},
                        },
                        {
                            "id": "btn_always",
                            "render_data": {"label": "\U0001f6e1\ufe0f Always allow", "visited_label": "Always allowed", "style": 1},
                            "action": {"type": 2, "permission": {"type": 2}, "data": "approve:default:allow_always"},
                        },
                    ]
                },
                {
                    "buttons": [
                        {
                            "id": "btn_deny",
                            "render_data": {"label": "\u274c Deny", "visited_label": "Denied", "style": 0},
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
        headers={"Authorization": f"QQBot {token}", "User-Agent": "ClaudeCode-QQ-Bridge/2.0"},
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

async def send_message_rest(user_openid: str, content: str) -> bool:
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "ClaudeCode-QQ-Bridge/2.0",
    }
    msg_seq = _next_msg_seq(user_openid)
    display_content = content[:3990] + "\n\n... (truncated)" if len(content) > 4000 else content
    body = {"markdown": {"content": display_content}, "msg_type": 2, "msg_seq": msg_seq}

    if content == '__APPROVAL_REQUIRED__':
        body['keyboard'] = build_approval_keyboard()
        body["markdown"] = {"content": "\U0001f510 **Claude Code needs your confirmation**\n\nSelect an action:"}

    try:
        resp = await client.post(f"{API_BASE}/v2/users/{user_openid}/messages", headers=headers, json=body, timeout=30.0)
        if resp.status_code >= 400:
            logger.error(f"Send failed [{resp.status_code}]: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"Send exception: {e}")
        return False

async def send_group_message_rest(group_openid: str, content: str, reply_to: str = None) -> bool:
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "ClaudeCode-QQ-Bridge/2.0",
    }
    msg_seq = _next_msg_seq(group_openid)
    display_content = content[:3990] + "\n\n... (truncated)" if len(content) > 4000 else content
    body = {"markdown": {"content": display_content}, "msg_type": 2, "msg_seq": msg_seq}
    if reply_to:
        body["msg_id"] = reply_to

    if content == '__APPROVAL_REQUIRED__':
        body['keyboard'] = build_approval_keyboard()
        body["markdown"] = {"content": "\U0001f510 **Claude Code needs your confirmation**\n\nSelect an action:"}

    try:
        resp = await client.post(f"{API_BASE}/v2/groups/{group_openid}/messages", headers=headers, json=body, timeout=30.0)
        if resp.status_code >= 400:
            logger.error(f"Group send failed [{resp.status_code}]: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"Group send exception: {e}")
        return False

GROUP_CONTEXT_CACHE: Dict[str, list] = {}

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

    # Command handling
    if content.strip().lower() in ["/new", "/reset", "/qingkong", "/xin duihua"]:
        logger.info("[Recv] New session command")
        await restart_claude_in_tmux()
        await send_message_rest(user_openid, "Session restarted.")
        return
    if content.strip().lower() in ["/stop", "/tingzhi", "/kill"]:
        logger.info("[Recv] Stop command")
        await stop_claude_in_tmux()
        await send_message_rest(user_openid, "\u26d4 Interrupted.")
        return
    if _processing:
        await send_message_rest(user_openid, "\u26a0\ufe0f Busy, please wait.")
        return

    logger.info(f"[QQ -> Claude] {content}")
    _processing = True
    try:
        await send_message_rest(user_openid, "\u23f3 Claude Code thinking...")
        await send_to_claude(content)
        reply = await wait_for_claude_response(timeout=TIMEOUT, user_openid=user_openid)
        if reply == '__APPROVAL_REQUIRED__':
            logger.info("[Approval] Sending approval buttons")
            await send_message_rest(user_openid, '__APPROVAL_REQUIRED__')
            reply = "\u23f3 Waiting for your confirmation..."
        if not reply:
            reply = "[Claude Code: no response]"
    except Exception as e:
        reply = f"[ERROR] {str(e)[:200]}"
        logger.error(f"Error: {e}")
    finally:
        _processing = False

    if reply != '\u23f3 Waiting for your confirmation...':
        logger.info(f"[Claude -> QQ] {reply[:200]}")
        await send_message_rest(user_openid, reply)

async def handle_group_message(d: dict, event_type: str):
    global _last_msg_id, _processing, _bot_openid, GROUP_CONTEXT_CACHE
    msg_id = str(d.get("id", ""))
    logger.info(f"[Group Raw] event={event_type} msg_id={msg_id} group={d.get("group_openid")}")
    if not msg_id or is_duplicate(msg_id):
        return
    group_openid = str(d.get("group_openid", ""))
    content = str(d.get("content", "")).strip()
    author = d.get("author") if isinstance(d.get("author"), dict) else {}
    member_openid = str(author.get("member_openid", ""))
    if not group_openid or not content:
        return
    sender_name = author.get("nickname") or author.get("username") or f"user_{member_openid[-6:]}" if member_openid else "User"
    msg_line = f"[{sender_name}] {content.strip()}"

    # Detect @mention
    is_mentioned = False
    my_openid_in_group = ""
    mentions = d.get("mentions") or []
    for m in mentions:
        if m.get("is_you") is True:
            my_openid_in_group = m.get("member_openid") or m.get("id") or m.get("user_openid") or ""
            break
    if event_type == 'GROUP_AT_MESSAGE_CREATE':
        is_mentioned = True
    elif event_type == 'GROUP_MESSAGE_CREATE':
        if my_openid_in_group:
            is_mentioned = True
        elif _bot_openid and mentions:
            for m in mentions:
                mid = m.get("member_openid") or m.get("id") or m.get("user_openid") or ""
                if str(mid) == str(_bot_openid):
                    is_mentioned = True
                    break

    if not is_mentioned:
        if group_openid not in GROUP_CONTEXT_CACHE:
            GROUP_CONTEXT_CACHE[group_openid] = []
        GROUP_CONTEXT_CACHE[group_openid].append(msg_line)
        GROUP_CONTEXT_CACHE[group_openid] = GROUP_CONTEXT_CACHE[group_openid][-100:]
        return

    _last_msg_id = msg_id
    logger.info(f"[Group Recv] group={group_openid} member={member_openid}: {content[:100]}")
    if member_openid != MASTER_OPENID:
        logger.info(f"[Group Skip] non-master: {member_openid}")
        return

    # Remove @ prefix
    cleaned_content = content
    if my_openid_in_group:
        cleaned_content = re.sub(rf"<@!?{my_openid_in_group}>", "", cleaned_content).strip()
    if _bot_openid:
        cleaned_content = re.sub(rf"<@!?{_bot_openid}>", "", cleaned_content).strip()
    if not cleaned_content:
        return

    # Commands
    if cleaned_content.lower() in ["/new", "/reset", "/qingkong", "/xin duihua"]:
        logger.info("[Group Recv] New session command")
        await restart_claude_in_tmux()
        await send_group_message_rest(group_openid, "Session restarted.", reply_to=msg_id)
        return
    if cleaned_content.lower() in ["/stop", "/tingzhi", "/kill"]:
        logger.info("[Group Recv] Stop command")
        await stop_claude_in_tmux()
        await send_group_message_rest(group_openid, "\u26d4 Interrupted.", reply_to=msg_id)
        return
    if _processing:
        await send_group_message_rest(group_openid, "\u26a0\ufe0f Busy, please wait.", reply_to=msg_id)
        return

    # Build prompt with context
    channel_context = ""
    if group_openid in GROUP_CONTEXT_CACHE:
        history_lines = GROUP_CONTEXT_CACHE[group_openid]
        if history_lines:
            channel_context = "[Recent group chat context]\n" + "\n".join(history_lines)
            GROUP_CONTEXT_CACHE[group_openid] = []
    prompt_to_send = cleaned_content
    if channel_context:
        prompt_to_send = f"{channel_context}\n\n[New message]\n{cleaned_content}"

    logger.info(f"[QQ Group -> Claude] {cleaned_content[:100]}")
    _processing = True
    try:
        await send_group_message_rest(group_openid, "\u23f3 Claude Code thinking...", reply_to=msg_id)
        await send_to_claude(prompt_to_send)
        reply = await wait_for_claude_response(timeout=TIMEOUT, group_openid=group_openid, reply_to=msg_id)
        if reply == '__APPROVAL_REQUIRED__':
            logger.info("[Group Approval] Sending approval buttons")
            await send_group_message_rest(group_openid, '__APPROVAL_REQUIRED__', reply_to=msg_id)
            reply = "\u23f3 Waiting for your confirmation..."
        if not reply:
            reply = "[Claude Code: no response]"
    except Exception as e:
        reply = f"[ERROR] {str(e)[:200]}"
        logger.error(f"Group error: {e}")
    finally:
        _processing = False

    if reply != '\u23f3 Waiting for your confirmation...':
        logger.info(f"[Claude -> QQ Group] {reply[:200]}")
        await send_group_message_rest(group_openid, reply, reply_to=msg_id)

async def handle_interaction(d: dict):
    """Handle QQ button callback (INTERACTION_CREATE)."""
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
                "User-Agent": "ClaudeCode-QQ-Bridge/2.0",
            },
            json={"code": 0},
            timeout=5.0,
        )
        logger.info("[Interaction] ACK ok")
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
    logger.info(f"[Interaction] button_data={button_data}")

    if button_data.startswith('approve:'):
        parts = button_data.split(':')
        if len(parts) >= 3:
            decision = parts[2]
            group_openid = d.get('group_openid')
            # Map decisions to keystrokes
            keystroke = {'allow': 'y', 'allow_always': 'p', 'deny': 'n'}.get(decision)
            if keystroke:
                proc = await asyncio.create_subprocess_exec(
                    "tmux", "send-keys", "-t", TMUX_SESSION, keystroke, ""
                )
                await proc.communicate()
                logger.info(f"[Approval] Sent keystroke: {keystroke}")

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
                    if t == 'READY' and isinstance(d, dict):
                        global _bot_openid
                        _session_id = d.get("session_id")
                        user = d.get("user") if isinstance(d.get("user"), dict) else {}
                        _bot_openid = str(user.get("id", ""))
                        logger.info(f"READY, session_id={_session_id}, bot_openid={_bot_openid}")
                    elif t == 'RESUMED':
                        logger.info("Session resumed")
                    elif t == 'C2C_MESSAGE_CREATE':
                        task = asyncio.create_task(handle_c2c_message(d))
                        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                    elif t in {'GROUP_AT_MESSAGE_CREATE', 'GROUP_MESSAGE_CREATE'}:
                        task = asyncio.create_task(handle_group_message(d, t))
                        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                    elif t == 'INTERACTION_CREATE':
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
    logger.info("Starting Claude Code QQ Bridge v2.0...")
    # 1. Start/verify Claude Code in tmux
    await start_claude_in_tmux()
    if not _session_cache.get('sessionId'):
        logger.warning("No Claude Code session found, retrying in 10s...")
        await asyncio.sleep(10)
        refresh_session()
    # 2. Connect QQ Bot gateway
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
