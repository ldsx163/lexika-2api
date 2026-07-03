"""
Lexika.ai Adapter — HTTP POST + Socket.IO v4 (EIO=4) hybrid adapter.

Architecture:
  1. HTTP POST to /messages/asking-ai sends the user message
  2. Socket.IO WebSocket receives streaming AI response via conversation:stream_chunk events
  3. JWT token auto-refresh keeps the session alive (using cookie-based session)
  4. Each request creates an independent conversation (conversationId=null) for isolation

Uses websockets library directly (instead of python-socketio) for EIO=4 compatibility.
"""

import asyncio
import json
import time
import uuid
import base64
import logging
import os
import ssl
import httpx
import websockets

from typing import AsyncGenerator, Optional, Dict, List, Any, Union
from urllib.parse import urlparse
from tool_dsml import (
    build_dsml_tool_prompt,
    has_dsml_content,
    parse_dsml_invoke,
    strip_dsml_tags,
)
from tool_sieve import StreamSieve

logger = logging.getLogger("lexika.adapter")


class LexikaAdapter:
    """Adapter that converts between OpenAI format and Lexika's hybrid HTTP+SocketIO API."""

    def __init__(
        self,
        base_url: str = "https://api.lexika.ai",
        jwt_token: str = "",
        workspace_id: str = "",
        origin: str = "https://lexika.ai",
        locale: str = "en",
        default_model: str = "claude-sonnet-4-6",
        dsml_enabled: bool = True,
        cookies: str = "",
        proxy: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.origin = origin
        self.locale = locale
        self.default_model = default_model
        self.workspace_id = workspace_id
        self.cookies = cookies
        self.proxy = proxy

        # JWT token (kept in memory, auto-refreshed)
        self._jwt_token = jwt_token
        self._token_expires_at: float = 0.0
        self._token_lock: Optional[asyncio.Lock] = None
        self._refresh_task: Optional[asyncio.Task] = None

        # HTTP headers (from HAR analysis)
        self.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "locale": self.locale,
            "origin": self.origin,
            "pragma": "no-cache",
            "referer": self.origin + "/",
            "sec-ch-ua": '"Microsoft Edge";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0",
            "x-user-location": "Shanghai (timezone: Asia/Shanghai)",
        }
        if cookies:
            self.headers["cookie"] = cookies

        # Endpoints
        self.chat_endpoint = "/messages/asking-ai"
        self.token_endpoint = "/auth/token"
        self.models_endpoint = "/models"

        # WebSocket state
        self._ws: Optional[Any] = None
        self._ws_connected: Optional[asyncio.Event] = None
        self._ws_lock: Optional[asyncio.Lock] = None
        self._ws_listen_task: Optional[asyncio.Task] = None
        self._pending: Dict[str, asyncio.Queue] = {}
        # Map conversationId -> trackingId for routing stream events
        self._conv_to_tracking: Dict[str, str] = {}

        # DSML
        self.dsml_enabled = dsml_enabled
        self.dsml_ready = False

        # Cached model list
        self._models_cache: Optional[List[dict]] = None
        self._models_cache_time: float = 0

    # ── JWT Token Management ──────────────────────────────────────

    def _decode_jwt_exp(self, token: str) -> float:
        """Decode JWT exp field."""
        try:
            parts = token.split(".")
            if len(parts) < 2:
                return 0.0
            payload_b64 = parts[1]
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            return float(payload.get("exp", 0))
        except Exception:
            return 0.0

    def _make_client(self, timeout: int = 30) -> httpx.AsyncClient:
        """Create httpx AsyncClient with proxy/SSL settings."""
        # Build headers with current JWT token (Bearer prefix required)
        headers = dict(self.headers)
        if self._jwt_token:
            headers["Authorization"] = "Bearer " + self._jwt_token
        kwargs = {"headers": headers, "timeout": timeout, "verify": False, "trust_env": False}
        if self.proxy:
            kwargs["proxy"] = self.proxy
        return httpx.AsyncClient(**kwargs)

    async def refresh_token(self) -> bool:
        """Call GET /auth/token to refresh the JWT token (uses cookie session)."""
        async with self._token_lock:
            try:
                async with self._make_client(timeout=30) as client:
                    resp = await client.get(f"{self.base_url}{self.token_endpoint}")
                    if resp.status_code == 200:
                        data = resp.json()
                        new_token = data.get("token", "")
                        if new_token:
                            self._jwt_token = new_token
                            self._token_expires_at = self._decode_jwt_exp(new_token)
                            logger.info("JWT token refreshed, expires at %s", time.ctime(self._token_expires_at))
                            return True
                    logger.warning("Token refresh failed: status=%s", resp.status_code)
                    return False
            except Exception as e:
                logger.warning("Token refresh error: %s", e)
                return False

    async def get_token(self) -> str:
        """Get current valid JWT token, refreshing if needed."""
        needs_refresh = not self._jwt_token or (
            self._token_expires_at > 0 and self._token_expires_at - time.time() < 300
        )
        if needs_refresh:
            await self.refresh_token()
        return self._jwt_token

    async def start_token_keepalive(self):
        """Start background task to refresh token every 10 minutes."""
        # Initialize locks/events in the running event loop
        if self._token_lock is None:
            self._token_lock = asyncio.Lock()
        if self._ws_lock is None:
            self._ws_lock = asyncio.Lock()
        if self._ws_connected is None:
            self._ws_connected = asyncio.Event()

        if self._jwt_token:
            self._token_expires_at = self._decode_jwt_exp(self._jwt_token)
            logger.info("Initial JWT expires at %s", time.ctime(self._token_expires_at))
        else:
            # No JWT token provided, try to refresh immediately using cookie
            logger.info("No JWT token provided, attempting auto-refresh via session cookie...")
            await self.refresh_token()

        async def _keepalive_loop():
            while True:
                await asyncio.sleep(600)
                await self.refresh_token()

        self._refresh_task = asyncio.create_task(_keepalive_loop())

    # ── Model List ────────────────────────────────────────────────

    async def fetch_models(self, force: bool = False) -> List[dict]:
        """Fetch available models from /models endpoint. Cached for 1 hour."""
        now = time.time()
        if not force and self._models_cache and (now - self._models_cache_time < 3600):
            return self._models_cache

        try:
            async with self._make_client(timeout=30) as client:
                resp = await client.get(f"{self.base_url}{self.models_endpoint}")
                if resp.status_code == 200:
                    data = resp.json()
                    models = data.get("data", [])
                    self._models_cache = models
                    self._models_cache_time = now
                    logger.info("Fetched %d models from /models", len(models))
                    return models
        except Exception as e:
            logger.warning("Failed to fetch models: %s", e)

        return self._models_cache or []

    # ── WebSocket (Socket.IO v4 / EIO=4) Connection ───────────────

    def _build_ws_url(self) -> str:
        """Build WebSocket URL for Socket.IO v4 (EIO=4) connection."""
        parsed = urlparse(self.base_url)
        host = parsed.hostname
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        return f"{ws_scheme}://{host}/socket.io/?EIO=4&transport=websocket"

    def _build_ws_headers(self) -> dict:
        """Build headers for WebSocket connection."""
        token = self._jwt_token or ""
        headers = {
            "Origin": self.origin,
            "User-Agent": self.headers["user-agent"],
            "Authorization": token,
        }
        if self.cookies:
            headers["Cookie"] = self.cookies
        return headers

    async def _ensure_ws_connected(self):
        """Ensure WebSocket connection is established with EIO=4 handshake."""
        async with self._ws_lock:
            if self._ws and self._ws_connected.is_set():
                return

            # Refresh token before connecting
            await self.get_token()

            ws_url = self._build_ws_url()
            ws_headers = self._build_ws_headers()

            # Create SSL context that doesn't verify
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE

            try:
                logger.info("Connecting to WebSocket: %s", ws_url)
                self._ws = await asyncio.wait_for(
                    websockets.connect(
                        ws_url,
                        additional_headers=ws_headers,
                        ssl=ssl_ctx,
                        ping_interval=None,  # We handle ping/pong manually
                        ping_timeout=None,
                        close_timeout=5,
                    ),
                    timeout=15,
                )

                # Start listening task
                self._ws_listen_task = asyncio.create_task(self._ws_listen_loop())

                # Wait for connection to be fully established
                try:
                    await asyncio.wait_for(self._ws_connected.wait(), timeout=15)
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        "WebSocket handshake timed out (JWT token invalid or expired? "
                        "Check session cookie / LEXIKA_JWT_TOKEN)"
                    )
                logger.info("WebSocket connected successfully")

            except Exception as e:
                logger.error("WebSocket connection failed: %s", e)
                self._ws_connected.clear()
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None
                raise

    async def _ws_listen_loop(self):
        """Listen for WebSocket messages and handle EIO=4 protocol."""
        ws = self._ws
        if ws is None:
            return

        try:
            async for raw_msg in ws:
                try:
                    # EIO=4 message format: <packet_type><data>
                    # 0: open, 1: close, 2: ping, 3: pong
                    # 40: connect, 41: disconnect
                    # 42: event, 43: ack

                    if isinstance(raw_msg, bytes):
                        raw_msg = raw_msg.decode("utf-8")

                    # Handle Engine.IO control packets
                    if raw_msg == "2":
                        # Ping from server, respond with pong
                        await ws.send("3")
                        continue

                    if raw_msg == "1":
                        # Close from server
                        logger.warning("WebSocket closed by server")
                        self._ws_connected.clear()
                        break

                    if raw_msg.startswith("0"):
                        # Open packet — contains sid and settings
                        logger.info("EIO open packet received")
                        # Send Socket.IO connect: "40"
                        await ws.send("40")
                        continue

                    if raw_msg.startswith("40"):
                        # Socket.IO connected
                        logger.info("Socket.IO namespace connected")
                        self._ws_connected.set()
                        continue

                    if raw_msg.startswith("42"):
                        # Event packet: 42["event_name", data]
                        json_str = raw_msg[2:]
                        try:
                            event_data = json.loads(json_str)
                            if isinstance(event_data, list) and len(event_data) >= 2:
                                event_name = event_data[0]
                                event_payload = event_data[1]
                                logger.info("WS event: %s, keys: %s", event_name,
                                           list(event_payload.keys()) if isinstance(event_payload, dict) else type(event_payload))
                                await self._route_ws_message(event_name, event_payload)
                            else:
                                logger.warning("WS event format unexpected: %s", json_str[:200])
                        except json.JSONDecodeError:
                            logger.warning("Failed to parse event: %s", json_str[:100])
                        continue

                    # Other packets — log them for debugging
                    if not raw_msg.startswith("3"):
                        logger.info("Unhandled WS message: %s", raw_msg[:200])

                except Exception as e:
                    logger.error("Error processing WS message: %s", e)

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
        except Exception as e:
            logger.error("WebSocket listen loop error: %s", e)
        finally:
            self._ws_connected.clear()

    async def _route_ws_message(self, event: str, data: Any):
        """Route WebSocket messages to the correct pending request.

        Stream events (stream_chunk, stream_end, status) use conversationId.
        Created/message events use trackingId.
        We maintain a mapping: conversationId -> trackingId
        """
        if not isinstance(data, dict):
            return

        # Events with trackingId (conversation:created, conversation:message)
        tracking_id = data.get("trackingId", "")

        # If this is a conversation:created event, map conversationId -> trackingId
        if event == "conversation:created" and tracking_id:
            conv = data.get("conversation", {})
            conv_id = conv.get("id", "") if isinstance(conv, dict) else ""
            if conv_id:
                self._conv_to_tracking[conv_id] = tracking_id
                logger.info("Mapped conversation %s -> tracking %s", conv_id, tracking_id)

        # For stream events, try trackingId first, then conversationId
        if tracking_id and tracking_id in self._pending:
            queue = self._pending[tracking_id]
            await queue.put({"event": event, "data": data})
            return

        # Try conversationId for stream events
        conv_id = data.get("conversationId", "")
        if conv_id and conv_id in self._conv_to_tracking:
            mapped_tracking_id = self._conv_to_tracking[conv_id]
            queue = self._pending.get(mapped_tracking_id)
            if queue:
                await queue.put({"event": event, "data": data})
                return

        logger.debug("No pending request for event %s (tracking=%s, conv=%s)",
                     event, tracking_id, conv_id)

    # ── Request Conversion ────────────────────────────────────────

    def _messages_to_prompt(self, messages: List[dict]) -> str:
        """Convert OpenAI messages array to a single prompt string."""
        parts = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if isinstance(content, list):
                content = " ".join(
                    p.get("text", "") for p in content if p.get("type") == "text"
                )
            if not content:
                continue
            if role == "system":
                parts.append(f"[System Instructions]\n{content}")
            elif role == "user":
                parts.append(f"[User]\n{content}")
            elif role == "assistant":
                parts.append(f"[Assistant]\n{content}")
        return "\n\n".join(parts)

    def _inject_dsml_prompt(self, messages: List[dict], tools: List[dict],
                            tool_choice: Optional[Union[str, dict]] = None) -> List[dict]:
        """Inject DSML tool calling instructions into the messages array."""
        if not self.dsml_enabled or not self.dsml_ready:
            return messages
        if tool_choice == "none":
            return messages
        dsml_prompt = build_dsml_tool_prompt(tools, tool_choice)
        if not dsml_prompt:
            return messages
        result = list(messages)
        for i, msg in enumerate(result):
            if msg.get("role") == "system":
                result[i] = {**msg, "content": msg["content"] + "\n\n" + dsml_prompt}
                return result
        result.insert(0, {"role": "system", "content": dsml_prompt})
        return result

    def convert_request(
        self,
        messages: List[dict],
        stream: bool = False,
        model: Optional[str] = None,
        tools: Optional[List[dict]] = None,
        tool_choice: Optional[Union[str, dict]] = None,
        **kwargs,
    ) -> dict:
        """Convert OpenAI-format messages to Lexika API request format."""
        if tools:
            messages = self._inject_dsml_prompt(messages, tools, tool_choice)

        prompt = self._messages_to_prompt(messages)
        tracking_id = str(uuid.uuid4())

        return {
            "trackingId": tracking_id,
            "content": prompt,
            "conversationId": None,
            "workspaceId": self.workspace_id,
            "askFrom": model or self.default_model,
            "useWebSearch": False,
            "useLongTermMemory": False,
            "attachments": [],
            "reasoning": "medium",
            "folderId": None,
        }

    # ── HTTP Send Message ─────────────────────────────────────────

    async def _send_message(self, payload: dict) -> dict:
        """Send POST to /messages/asking-ai.

        Returns the JSON body even on 4xx/5xx — Lexika error bodies carry
        success=false + message (e.g. SUBSCRIPTION_NOT_SUPPORT_IT), which the
        callers surface to the client instead of a bare HTTP error.
        """
        async with self._make_client(timeout=120) as client:
            resp = await client.post(
                f"{self.base_url}{self.chat_endpoint}", json=payload
            )
            try:
                return resp.json()
            except Exception:
                resp.raise_for_status()
                raise RuntimeError(
                    f"Lexika API returned non-JSON response (status {resp.status_code})"
                )

    # ── Non-streaming Request ─────────────────────────────────────

    async def send_request(self, payload: dict) -> dict:
        """Send a message and wait for the complete response."""
        tracking_id = payload["trackingId"]
        queue: asyncio.Queue = asyncio.Queue()
        self._pending[tracking_id] = queue

        try:
            await self._ensure_ws_connected()
            resp_data = await self._send_message(payload)
            if not resp_data.get("success"):
                raise RuntimeError(f"Lexika API error: {resp_data.get('message', 'unknown')}")

            full_text = ""
            full_reasoning = ""
            usage_info: dict = {}
            timeout = 300

            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    logger.warning("Request %s timed out", tracking_id)
                    break

                event = msg["event"]
                data = msg["data"]

                if event == "conversation:stream_chunk":
                    part = data.get("part", {})
                    part_type = part.get("type", "")
                    if part_type == "text-delta":
                        full_text += part.get("text", "")
                    elif part_type == "reasoning-delta":
                        full_reasoning += part.get("text", "")
                    elif part_type == "finish":
                        usage_info = part.get("totalUsage", {})
                elif event == "conversation:stream_end":
                    buffer = data.get("buffer", [])
                    if buffer and not full_text:
                        for item in buffer:
                            if item.get("type") == "text-delta":
                                full_text += item.get("text", "")
                            elif item.get("type") == "reasoning-delta":
                                full_reasoning += item.get("text", "")
                    break

            return self.convert_response(
                {"text": full_text, "reasoning": full_reasoning, "usage": usage_info}
            )

        finally:
            self._pending.pop(tracking_id, None)

    # ── Streaming Request ─────────────────────────────────────────

    async def stream_request(self, payload: dict) -> AsyncGenerator[bytes, None]:
        """Send a message and yield streaming OpenAI SSE chunks."""
        tracking_id = payload["trackingId"]
        queue: asyncio.Queue = asyncio.Queue()
        self._pending[tracking_id] = queue

        use_sieve = self.dsml_enabled and self.dsml_ready
        sieve = StreamSieve() if use_sieve else None

        try:
            await self._ensure_ws_connected()
            resp_data = await self._send_message(payload)
            if not resp_data.get("success"):
                error_msg = resp_data.get("message", "unknown error")
                yield self._build_error_chunk(error_msg)
                yield b"data: [DONE]\n\n"
                return

            timeout = 300

            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    logger.warning("Stream %s timed out", tracking_id)
                    break

                event = msg["event"]
                data = msg["data"]

                if event == "conversation:stream_chunk":
                    part = data.get("part", {})
                    part_type = part.get("type", "")

                    if part_type == "reasoning-delta":
                        text = part.get("text", "")
                        if text:
                            yield self._build_reasoning_chunk(text)

                    elif part_type == "text-delta":
                        text = part.get("text", "")
                        if not text:
                            continue

                        if sieve:
                            result = sieve.feed(text)
                            for t in result.text_parts:
                                if t:
                                    yield self._build_content_chunk(t)
                            for tc in result.tool_calls:
                                async for chunk in self._emit_tool_call(tc):
                                    yield chunk
                            if result.pending:
                                continue
                        else:
                            if text:
                                yield self._build_content_chunk(text)

                elif event == "conversation:stream_end":
                    break

            if sieve:
                flush_result = sieve.flush()
                for t in flush_result.text_parts:
                    if t:
                        yield self._build_content_chunk(t)
                for tc in flush_result.tool_calls:
                    async for chunk in self._emit_tool_call(tc):
                        yield chunk

            yield b"data: [DONE]\n\n"

        finally:
            self._pending.pop(tracking_id, None)

    # ── Response Formatting ───────────────────────────────────────

    def convert_response(self, response: dict) -> dict:
        """Convert to OpenAI non-streaming response format."""
        full_text = response.get("text", "")
        reasoning = response.get("reasoning", "")
        usage = response.get("usage", {})

        if self.dsml_enabled and self.dsml_ready and has_dsml_content(full_text):
            return self._convert_with_dsml(full_text, usage, reasoning)

        message = {"role": "assistant", "content": full_text}
        if reasoning:
            message["reasoning_content"] = reasoning

        return {
            "id": f"chatcmpl-{int(time.time())}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": self.default_model,
            "choices": [{
                "index": 0,
                "message": message,
                "finish_reason": "stop",
            }],
            "usage": self._convert_usage(usage),
        }

    def _convert_usage(self, usage: dict) -> dict:
        result = {
            "prompt_tokens": usage.get("inputTokens", 0),
            "completion_tokens": usage.get("outputTokens", 0),
            "total_tokens": usage.get("totalTokens", 0),
        }
        reasoning_tokens = usage.get("reasoningTokens") or usage.get(
            "outputTokenDetails", {}).get("reasoningTokens", 0)
        if reasoning_tokens:
            result["completion_tokens_details"] = {"reasoning_tokens": reasoning_tokens}
        return result

    def _convert_with_dsml(self, full_text: str, usage: Optional[dict] = None,
                           reasoning: str = "") -> dict:
        """Convert response with DSML tags to OpenAI tool_calls format."""
        tool_calls = parse_dsml_invoke(full_text)
        cleaned_text = strip_dsml_tags(full_text)

        message = {"role": "assistant", "content": cleaned_text}
        if reasoning:
            message["reasoning_content"] = reasoning

        response = {
            "id": f"chatcmpl-{int(time.time())}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": self.default_model,
            "choices": [{
                "index": 0,
                "message": message,
                "finish_reason": "tool_calls" if tool_calls else "stop",
            }],
            "usage": self._convert_usage(usage or {}),
        }

        if tool_calls:
            response["choices"][0]["message"]["tool_calls"] = tool_calls

        return response

    def _build_content_chunk(self, text: str) -> bytes:
        """Build a text delta as an OpenAI SSE chunk."""
        chunk = {"choices": [{"delta": {"content": text}, "index": 0}]}
        return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode()

    def _build_reasoning_chunk(self, text: str) -> bytes:
        """Build a reasoning delta as an OpenAI SSE chunk (DeepSeek-style reasoning_content)."""
        chunk = {"choices": [{"delta": {"reasoning_content": text}, "index": 0}]}
        return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode()

    def _build_error_chunk(self, error_msg: str) -> bytes:
        """Build an error SSE chunk."""
        chunk = {"error": {"message": error_msg, "type": "upstream_error"}}
        return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode()

    async def _emit_tool_call(self, tc: dict) -> AsyncGenerator[bytes, None]:
        """Yield SSE chunks for a single tool call."""
        chunk1 = {"choices": [{"delta": {"tool_calls": [tc]}, "index": 0}]}
        yield f"data: {json.dumps(chunk1, ensure_ascii=False)}\n\n".encode()
        chunk2 = {"choices": [{"delta": {}, "index": 0, "finish_reason": "tool_calls"}]}
        yield f"data: {json.dumps(chunk2, ensure_ascii=False)}\n\n".encode()

    # ── Cleanup ───────────────────────────────────────────────────

    async def shutdown(self):
        """Clean shutdown of WebSocket and background tasks."""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass

        if self._ws_listen_task:
            self._ws_listen_task.cancel()
            try:
                await self._ws_listen_task
            except asyncio.CancelledError:
                pass

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._ws_connected.clear()