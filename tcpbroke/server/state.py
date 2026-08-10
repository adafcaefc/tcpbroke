from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class PendingStream:
    """One half-built stream, waiting for the agent to dial back in.

    A viewer connects, the server asks the agent to open a matching socket, and the two halves are
    joined here. `agent_ws` is resolved by the /stream/{id} handler; `done` is set by the /listen
    handler once relaying has finished, which is what lets /stream/{id} return - a Starlette
    WebSocket endpoint closes its connection the moment the handler returns, so the agent side must
    block for the whole life of the stream.
    """

    __slots__ = ("agent_ws", "done")

    def __init__(self) -> None:
        self.agent_ws: asyncio.Future[WebSocket] = asyncio.get_event_loop().create_future()
        self.done: asyncio.Event = asyncio.Event()


class TunnelState:
    """Single-agent tunnel state.

    Like nbroke, this lives in module-level globals, so the container MUST run a single replica -
    a second replica would have its own empty state and silently serve a tunnel that is not there.
    """

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.agent_ws: Optional[WebSocket] = None
        self.pending: dict[str, PendingStream] = {}
        self.last_activity: float = 0.0

    @property
    def occupied(self) -> bool:
        return self.agent_ws is not None

    def touch(self) -> None:
        self.last_activity = time.monotonic()

    async def clear(self, reason: str = "disconnect") -> None:
        async with self.lock:
            await self._clear_locked(reason)

    async def clear_if_current(self, ws: WebSocket, reason: str = "disconnect") -> None:
        """Clear only if `ws` still holds the agent slot.

        A control handler whose socket was already reaped by idle_check can finish long after a
        fresh agent has reconnected and taken the slot. An unconditional clear there would kill the
        healthy tunnel, so the loser of that race must be a no-op.
        """
        async with self.lock:
            if self.agent_ws is ws:
                await self._clear_locked(reason)

    async def _clear_locked(self, reason: str) -> None:
        logger.info("tunnel_cleared reason=%s pending=%d", reason, len(self.pending))
        ws = self.agent_ws
        self.agent_ws = None

        # Fail every waiter and release every blocked agent-side handler. Missing either one leaks
        # a coroutine that never wakes: the viewer would sit until its open timeout, and the agent
        # handler would hold a dead socket open until the process restarts.
        pending = dict(self.pending)
        self.pending.clear()
        for stream in pending.values():
            if not stream.agent_ws.done():
                stream.agent_ws.set_exception(RuntimeError(reason))
            stream.done.set()

        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    async def idle_check(self, idle_timeout: float, check_interval: float) -> None:
        while True:
            await asyncio.sleep(check_interval)
            async with self.lock:
                if self.agent_ws is None:
                    continue
                if time.monotonic() - self.last_activity > idle_timeout:
                    logger.warning("control_idle_timeout clearing agent slot")
                    await self._clear_locked("idle_timeout")


class AuthLimiter:
    """Per-IP failed-auth throttle: a delay on every failure, a lockout after too many.

    In-process and therefore per-replica, which is fine because the tunnel is single-replica by
    construction. It is a speed bump against credential stuffing, not a substitute for a long key.
    """

    def __init__(self, limit: int, window: float, block: float) -> None:
        self._limit = limit
        self._window = window
        self._block = block
        self._failures: dict[str, list[float]] = {}
        self._blocked: dict[str, float] = {}

    def blocked_for(self, ip: str) -> float:
        """Seconds remaining on this IP's lockout, or 0.0 if it may try."""
        until = self._blocked.get(ip)
        if until is None:
            return 0.0
        remaining = until - time.monotonic()
        if remaining <= 0:
            self._blocked.pop(ip, None)
            self._failures.pop(ip, None)
            return 0.0
        return remaining

    def record_failure(self, ip: str) -> bool:
        """Record a bad key. Returns True if this failure tripped the lockout."""
        now = time.monotonic()
        recent = [t for t in self._failures.get(ip, []) if now - t < self._window]
        recent.append(now)
        self._failures[ip] = recent
        if len(recent) >= self._limit:
            self._blocked[ip] = now + self._block
            return True
        return False

    def record_success(self, ip: str) -> None:
        self._failures.pop(ip, None)
        self._blocked.pop(ip, None)


tunnel = TunnelState()
