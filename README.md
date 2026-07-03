# Lexika.ai → OpenAI API 代理

将 Lexika.ai 的 HTTP + Socket.IO 混合 API 转换为标准 OpenAI API 格式，支持流式和非流式响应。

## 架构

```
客户端 (OpenAI 格式)
    ↓
FastAPI 代理 (server.py)
    ↓
LexikaAdapter (adapter.py)
    ├── HTTP POST /messages/asking-ai  (发送消息)
    ├── Socket.IO WebSocket            (接收流式响应)
    └── JWT Token 保活                 (自动刷新)
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

编辑 `.env` 文件，填入你的 Lexika.ai 凭据：

```env
MODEL_NAME=claude-sonnet-4-6
HOST=0.0.0.0
PORT=8000
API_KEY=sk-lexika-proxy
DSML_ENABLED=false
LEXIKA_BASE_URL=https://api.lexika.ai
LEXIKA_ORIGIN=https://lexika.ai
LEXIKA_LOCALE=en
LEXIKA_JWT_TOKEN=你的JWT令牌
LEXIKA_WORKSPACE_ID=你的工作区ID
```

### 3. 获取认证信息

Lexika.ai 使用双重认证：
1. **Session Cookie** (`__Secure-better-auth.session_token`) — 用于刷新 JWT
2. **JWT Token** — 用于 HTTP API 请求和 WebSocket 连接

#### 获取方法：

1. 打开浏览器访问 [lexika.ai](https://lexika.ai) 并登录
2. 打开开发者工具 (F12) → Application 标签 → Cookies
3. 找到 `api.lexika.ai` 域名下的 `__Secure-better-auth.session_token` cookie
4. 复制 cookie 值到 `.env` 文件的 `LEXIKA_COOKIES` 字段
5. JWT Token 会在启动时自动从 `/auth/token` 获取并定期刷新

或者使用配置工具：
```bash
python config_tool.py
```
选择你的 HAR 文件，自动提取配置。

**注意**：Session Cookie 有效期约 7 天，过期后需要重新获取。

### 4. 启动代理

```bash
python server.py
```

或使用启动脚本：
```bash
start_proxy.bat
```

### Docker 部署

GitHub Actions 会在每次推送到 `main` 时自动构建镜像并发布到 GHCR：

```bash
docker run -d --name lexika-proxy \
  -p 8000:8000 \
  -e LEXIKA_COOKIES='__Secure-better-auth.session_token=你的session_token' \
  -e LEXIKA_WORKSPACE_ID='你的工作区ID' \
  ghcr.io/ldsx163/lexika-2api:latest
```

或使用 `.env` 文件：

```bash
docker run -d --name lexika-proxy -p 8000:8000 --env-file .env ghcr.io/ldsx163/lexika-2api:latest
```

本地构建：

```bash
docker build -t lexika-2api .
```

### 5. 使用

代理现在在 `http://localhost:8000` 上运行，兼容 OpenAI API 格式。

#### 列出模型
```bash
curl http://localhost:8000/v1/models
```

#### 非流式对话
```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-4-6",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": false
  }'
```

#### 流式对话
```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-4-6",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": true
  }'
```

#### 在 Python 中使用
```python
import openai

client = openai.OpenAI(
    base_url="http://localhost:8000/v1",
    api_key="sk-lexika-proxy"
)

response = client.chat.completions.create(
    model="claude-sonnet-4-6",
    messages=[{"role": "user", "content": "Hello!"}],
)

print(response.choices[0].message.content)
```

## 可用模型

Lexika.ai 支持的模型包括但不限于：
- `claude-sonnet-4-6`
- `gpt-4o`
- `gpt-4o-mini`
- `gemini-pro`
- `deepseek-chat`
- 更多模型请访问 `/v1/models` 端点查看

## 环境变量说明

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `MODEL_NAME` | 默认模型 | `claude-sonnet-4-6` |
| `HOST` | 监听地址 | `0.0.0.0` |
| `PORT` | 监听端口 | `8000` |
| `API_KEY` | 代理 API 密钥 | `sk-lexika-proxy` |
| `DSML_ENABLED` | 启用 DSML 工具调用 | `false` |
| `LEXIKA_BASE_URL` | Lexika API 地址 | `https://api.lexika.ai` |
| `LEXIKA_ORIGIN` | Origin 头 | `https://lexika.ai` |
| `LEXIKA_LOCALE` | 语言设置 | `en` |
| `LEXIKA_JWT_TOKEN` | JWT 令牌 | (必填) |
| `LEXIKA_WORKSPACE_ID` | 工作区 ID | (必填) |
| `LEXIKA_COOKIES` | Cookie 字符串 | (可选) |
| `LEXIKA_PROXY` | HTTP 代理地址 | (可选) |

## JWT Token 保活

代理内置 JWT token 自动刷新机制：
- 启动时解码 JWT 的过期时间
- 每 10 分钟自动调用 `/auth/token` 刷新 token
- 如果 token 过期，会在下次请求时尝试刷新

**注意**：Token 刷新需要有效的浏览器 session。如果刷新失败，请重新从浏览器获取 JWT token。

## DSML 工具调用

启用 DSML (`DSML_ENABLED=true`) 后，代理支持 OpenAI 格式的工具调用：
- 将 `tools` 参数转换为 DSML 提示注入到消息中
- 解析 AI 响应中的 DSML 标签为 `tool_calls` 格式
- 支持流式和非流式工具调用

## 文件结构

```
lexika/
├── adapter.py       # 核心适配器（HTTP + Socket.IO + JWT）
├── server.py        # FastAPI 代理服务器
├── config_tool.py   # HAR 配置提取工具
├── tool_dsml.py     # DSML 工具调用解析
├── tool_sieve.py    # 流式内容过滤器
├── requirements.txt # Python 依赖
├── .env             # 环境配置
├── .env.example     # 配置模板
├── start_proxy.bat  # Windows 启动脚本
└── README.md        # 本文档
```

## API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/v1/models` | GET | 列出可用模型 |
| `/v1/chat/completions` | POST | 聊天补全（支持流式） |
| `/health` | GET | 健康检查 |

## 故障排查

### Socket.IO 连接失败
- 检查 JWT token 是否有效
- 确认网络能访问 `api.lexika.ai`
- 如果使用代理，检查 `LEXIKA_PROXY` 设置

### Token 刷新失败 (401)
- JWT token 已过期且无法自动刷新
- 重新从浏览器获取新的 JWT token
- 更新 `.env` 文件中的 `LEXIKA_JWT_TOKEN`

### SSL 错误
- 代理已内置 `trust_env=False` 和 `verify=False`
- 如果仍有问题，检查系统代理设置

### 模型列表为空
- 检查 JWT token 是否有效
- 尝试访问 `/v1/models` 端点查看错误信息