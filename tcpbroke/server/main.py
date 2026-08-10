"""tcpbroke relay server.

Public half of the tunnel. Runs as a SINGLE replica (see state.TunnelState) behind an HTTPS
ingress. Three WebSocket endpoints:

    /control       the agent registers here and holds one long-lived socket (one agent at a time)
    /listen        a viewer connects here per TCP connection it has accepted locally
    /stream/{id}   the agent dials back in here, once per stream, to complete the pair

Everything on /listen and /stream is opaque binary relayed verbatim. The server never inspects,
frames, or buffers the payload - it does not know or care that it is carrying RDP.
"""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from . import settings
from .state import AuthLimiter, PendingStream, tunnel
from ..protocol import (
    AUTH_HEADER,
    CLOSE_NO_AGENT,
    CLOSE_OCCUPIED,
    CLOSE_RATE_LIMITED,
    CLOSE_UNAUTHORIZED,
    CLOSE_UNKNOWN_STREAM,
    MSG_OPEN_FAILED,
    MSG_PING,
    MSG_PONG,
    make_open,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

limiter = AuthLimiter(
    settings.AUTH_FAIL_LIMIT,
    settings.AUTH_FAIL_WINDOW_SECONDS,
    settings.AUTH_FAIL_BLOCK_SECONDS,
)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    # Refuse to run keyless. An unauthenticated tunnel to a desktop is not a degraded mode worth
    # supporting, so this is a hard startup failure rather than a warning nobody reads.
    if not settings.TUNNEL_KEY:
        raise RuntimeError("TUNNEL_KEY is not set - refusing to start an unauthenticated tunnel")
    task = asyncio.create_task(
        tunnel.idle_check(
            settings.CONTROL_IDLE_TIMEOUT_SECONDS,
            settings.IDLE_CHECK_INTERVAL_SECONDS,
        )
    )
    try:
        yield
    finally:
        task.cancel()


# No docs/openapi: this host serves one purpose and needs no discoverable surface.
app = FastAPI(
    title="tcpbroke-server",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


def _client_ip(ws: WebSocket) -> str:
    if settings.TRUST_FORWARDED_FOR:
        forwarded = ws.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return ws.client.host if ws.client else "unknown"


async def _accept_authorized(ws: WebSocket, endpoint: str) -> bool:
    """Complete the handshake and verify the shared key. False means the socket is already closed.

    The handshake is accepted BEFORE the key is checked, deliberately: an ASGI app that closes
    before accepting makes uvicorn reject the upgrade with a plain HTTP 403, and the application
    close code (4001 and friends) never reaches the client - it would just see a failed handshake
    and could not tell "wrong key" from "server down". Accepting first costs nothing, since a
    rejected peer is closed immediately without exchanging any data.
    """
    ip = _client_ip(ws)
    await ws.accept()

    blocked = limiter.blocked_for(ip)
    if blocked > 0:
        logger.warning("auth_blocked endpoint=%s ip=%s remaining=%.0fs", endpoint, ip, blocked)
        await ws.close(CLOSE_RATE_LIMITED, "rate_limited")
        return False

    provided = ws.headers.get(AUTH_HEADER, "")
    if not secrets.compare_digest(provided, settings.TUNNEL_KEY or ""):
        tripped = limiter.record_failure(ip)
        logger.warning("auth_failed endpoint=%s ip=%s locked_out=%s", endpoint, ip, tripped)
        # Slow down credential stuffing before answering.
        await asyncio.sleep(settings.AUTH_FAIL_DELAY_SECONDS)
        await ws.close(CLOSE_UNAUTHORIZED, "unauthorized")
        return False

    limiter.record_success(ip)
    return True


@app.get("/healthz")
async def healthz() -> dict[str, object]:
    return {"status": "ok", "agent_connected": tunnel.occupied}


@app.websocket("/control")
async def control(ws: WebSocket) -> None:
    if not await _accept_authorized(ws, "control"):
        return

    async with tunnel.lock:
        if tunnel.occupied:
            logger.warning("agent_rejected reason=occupied ip=%s", _client_ip(ws))
            await ws.close(CLOSE_OCCUPIED, "occupied")
            return
        tunnel.agent_ws = ws
        tunnel.touch()

    logger.info("agent_connected ip=%s", _client_ip(ws))

    try:
        while True:
            msg = json.loads(await ws.receive_text())
            async with tunnel.lock:
                tunnel.touch()
            msg_type = msg.get("type")

            if msg_type == MSG_PING:
                await ws.send_text(json.dumps({"type": MSG_PONG}))

            elif msg_type == MSG_OPEN_FAILED:
                # The agent could not reach the local port. Fail the waiting viewer now instead of
                # letting it burn the full open timeout.
                stream_id = msg.get("stream_id", "")
                detail = msg.get("detail", "dial_failed")
                async with tunnel.lock:
                    pending = tunnel.pending.pop(stream_id, None)
                if pending is not None and not pending.agent_ws.done():
                    pending.agent_ws.set_exception(RuntimeError(detail))
                    pending.done.set()
                logger.warning("stream_dial_failed id=%s detail=%s", stream_id, detail)

            else:
                logger.warning("unknown_control_message type=%s", msg_type)

    except WebSocketDisconnect:
        logger.info("agent_disconnected ip=%s", _client_ip(ws))
    except Exception as exc:
        logger.exception("control_error: %s", exc)
    finally:
        await tunnel.clear_if_current(ws, "agent_disconnect")


@app.websocket("/listen")
async def listen(ws: WebSocket) -> None:
    if not await _accept_authorized(ws, "listen"):
        return

    async with tunnel.lock:
        if not tunnel.occupied:
            await ws.close(CLOSE_NO_AGENT, "agent_not_connected")
            return
        stream_id = str(uuid.uuid4())
        pending = PendingStream()
        tunnel.pending[stream_id] = pending
        agent_ctrl = tunnel.agent_ws
        tunnel.touch()

    async def _abort(code: int, reason: str) -> None:
        async with tunnel.lock:
            tunnel.pending.pop(stream_id, None)
        pending.done.set()
        await ws.close(code, reason)

    try:
        await agent_ctrl.send_text(json.dumps(make_open(stream_id)))
    except Exception as exc:
        logger.error("open_send_failed id=%s: %s", stream_id, exc)
        await _abort(1011, "tunnel_send_failed")
        return

    try:
        agent_ws = await asyncio.wait_for(
            pending.agent_ws, timeout=settings.STREAM_OPEN_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        logger.warning("stream_open_timeout id=%s", stream_id)
        await _abort(1011, "upstream_timeout")
        return
    except RuntimeError as exc:
        await _abort(1011, str(exc)[:120])
        return

    logger.info("stream_opened id=%s ip=%s", stream_id, _client_ip(ws))
    try:
        await _relay(ws, agent_ws)
    finally:
        async with tunnel.lock:
            tunnel.pending.pop(stream_id, None)
            tunnel.touch()
        # Releases the /stream/{id} handler, which closes the agent-side socket by returning.
        pending.done.set()
        logger.info("stream_closed id=%s", stream_id)


@app.websocket("/stream/{stream_id}")
async def stream(ws: WebSocket, stream_id: str) -> None:
    if not await _accept_authorized(ws, "stream"):
        return

    async with tunnel.lock:
        pending = tunnel.pending.get(stream_id)
    if pending is None or pending.agent_ws.done():
        logger.warning("stream_unknown id=%s", stream_id)
        await ws.close(CLOSE_UNKNOWN_STREAM, "unknown_stream")
        return

    try:
        pending.agent_ws.set_result(ws)
    except asyncio.InvalidStateError:
        await ws.close(CLOSE_UNKNOWN_STREAM, "unknown_stream")
        return

    # Block for the stream's whole life. A Starlette WebSocket endpoint closes its socket as soon
    # as the handler returns, so returning here would tear down the very connection /listen is
    # relaying through.
    await pending.done.wait()


async def _pump(src: WebSocket, dst: WebSocket) -> None:
    while True:
        message = await src.receive()
        if message["type"] == "websocket.disconnect":
            return
        data = message.get("bytes")
        if data is None:
            text = message.get("text")
            if text is None:
                continue
            data = text.encode()
        await dst.send_bytes(data)


async def _relay(a: WebSocket, b: WebSocket) -> None:
    """Pump bytes both ways until either side goes away, then stop the other."""
    tasks = {asyncio.create_task(_pump(a, b)), asyncio.create_task(_pump(b, a))}
    done, still_running = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in still_running:
        task.cancel()
    await asyncio.gather(*still_running, return_exceptions=True)
    for task in done:
        exc = task.exception()
        if exc is not None:
            logger.debug("relay_pump_ended: %s", exc)
