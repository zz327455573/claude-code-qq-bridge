# Claude Code QQ Bridge 🚀

**Claude Code QQ 桥接网关 — Stateless Agent Proxy Gateway**

将 Claude Code CLI 桥接到 QQ 消息平台，实现手机 QQ 远程调用 Claude Code 的能力。

---

## 🌟 设计原则

本系统遵守严格的 **stateless gateway** 模式：

| 原则 | 说明 |
|------|------|
| ✅ 纯转发 | QQ 消息 → `claude -p` → stdout → QQ，不做中间处理 |
| ✅ 不解析状态 | 不检测关键词、不做审批推断、不解析语义 |
| ✅ 不注入控制 | 不注入 stdin、不发送 y/n、不操作交互界面 |
| ✅ 异常透传 | 超时或阻塞时返回 `AGENT REQUIRES INTERACTION` |
| ❌ 禁止状态推理 | 不猜 agent 在想什么，不判断需不需要审批 |

---

## 📐 架构

```
┌──────────┐    ┌──────────────┐    ┌──────────────────┐
│  QQ User  │───▶│  Bridge      │───▶│  claude -p "msg"  │
│           │    │  (stateless) │    │  (subprocess)    │
│           │◀───│              │◀───│  stdout          │
└──────────┘    └──────────────┘    └──────────────────┘
```

- **输入层**：QQ WebSocket 网关 → 消息去重 → 权限隔离
- **路由层**：纯转发，不解析，不缓存，不推断
- **执行层**：`subprocess claude -p "message"` → stdout 直接回复

---

## 🚀 部署指南

### 1. 准备环境

```bash
pip install -r requirements.txt
```

### 2. 凭据配置

```bash
cp .env.example .env
```

编辑 `.env`：

```env
APP_ID=你的QQ机器人AppID
CLIENT_SECRET=你的QQ机器人密钥
MASTER_OPENID=你的OpenID
CLAUDE_BIN=claude
TIMEOUT=180
```

### 3. 运行

```bash
python3 claude-code-qq-bridge.py
```

推荐 PM2 保活：

```bash
pm2 start claude-code-qq-bridge.py --name "claude-code-qq-bridge" --interpreter python3
```

---

## 📋 安全保障

- **权限隔离**：只有 MASTER_OPENID 的消息会被响应
- **消息去重**：5 分钟内重复消息自动丢弃
- **超时保护**：默认 3 分钟超时，防止无限挂起
- **阻塞检测**：CLI 阻塞时返回警示，不继续等待

---

## 🔗 项目背景

本项目基于 AGY-QQ-Bridge 的教训重构：

- ❌ 放弃 tmux capture-pane 终端解析（7 次迭代证实不可收敛）
- ❌ 放弃关键词审批推断（假结构化）
- ❌ 放弃 stdin 注入控制
- ✅ 采用纯 subprocess stdout 透传
- ✅ 保持 stateless，不猜测 agent 状态

---

## 📄 文件结构

```
claude-code-qq-bridge/
├── claude-code-qq-bridge.py    # 主桥接脚本
├── .env.example                # 配置模板
├── requirements.txt            # Python 依赖
├── .gitignore
├── LICENSE                     # MIT
└── README.md
```

---

*Stateless router. No state inference. No terminal parsing.*