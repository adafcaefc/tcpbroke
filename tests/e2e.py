"""End-to-end test for the whole tunnel, in one process.

    client -> listener :19389  ==WSS==>  relay :18080  ==WSS==>  agent  ->  echo :19000

Everything is loopback, so this proves the protocol and the teardown paths without needing the
relay deployed. Run it inside the dev container:

    docker run --rm -v "$PWD:/app" -w /app python:3.11-slim \
        sh -c "pip install -q -r requirements-dev.txt && python -m tests.e2e"

Exits non-zero on the first failure.
"""
from __future__ import annotations

import asyncio
import os
import secrets
import sys
import urllib.error
import urllib.request

ECHO_PORT = 19000
RELAY_PORT = 18080
LISTEN_PORT = 19389
KEY = "test-key-" + secrets.token_hex(16)
RELAY_URL = f"http://127.0.0.1:{RELAY_PORT}"

# settings.py reads the environment at import time, so this must happen before the server package
# is imported anywhere below.
os.environ["TUNNEL_KEY"] = KEY
os.environ["AUTH_FAIL_DELAY_SECONDS"] = "0"
os.environ["STREAM_OPEN_TIMEOUT_SECONDS"] = "5"

import uvicorn  # noqa: E402

from tcpbroke.cli.main import run_agent, run_listener  # noqa: E402
from tcpbroke.server.main import app  # noqa: E402
from tcpbroke.server.state import tunnel  # noqa: E402

_failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    if ok:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name} {detail}")
        _failures.append(name)


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------

async def _echo_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Echo everything back. b'QUIT' makes it hang up, to test close propagation."""
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                return
            if b"QUIT" in data:
                writer.close()
                return
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        return
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def _healthz() -> dict:
    def _get() -> bytes:
        with urllib.request.urlopen(f"{RELAY_URL}/healthz", timeout=2) as resp:
            return resp.read()

    import json

    return json.loads(await asyncio.to_thread(_get))


async def _wait_for(predicate, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            if await predicate():
                return
        except (urllib.error.URLError, OSError, ConnectionError):
            pass
        await asyncio.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {what}")


# ---------------------------------------------------------------------------
# Tests.
# ---------------------------------------------------------------------------

async def test_round_trip() -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", LISTEN_PORT)
    try:
        payload = b"hello tunnel"
        writer.write(payload)
        await writer.drain()
        got = await asyncio.wait_for(reader.readexactly(len(payload)), timeout=5)
        check("small round trip", got == payload, f"got {got!r}")
    finally:
        writer.close()


async def test_large_transfer() -> None:
    """4 MiB in both directions - well past the 64 KiB read chunk, so this exercises flow control."""
    reader, writer = await asyncio.open_connection("127.0.0.1", LISTEN_PORT)
    try:
        payload = secrets.token_bytes(4 * 1024 * 1024)

        async def _send() -> None:
            writer.write(payload)
            await writer.drain()

        send_task = asyncio.create_task(_send())
        got = await asyncio.wait_for(reader.readexactly(len(payload)), timeout=60)
        await send_task
        check("4 MiB round trip", got == payload, f"got {len(got)} bytes")
    finally:
        writer.close()


async def test_interleaved() -> None:
    """Many small writes with reads in between - the interactive pattern RDP actually produces."""
    reader, writer = await asyncio.open_connection("127.0.0.1", LISTEN_PORT)
    try:
        ok = True
        for i in range(200):
            msg = f"ping-{i:04d}".encode()
            writer.write(msg)
            await writer.drain()
            got = await asyncio.wait_for(reader.readexactly(len(msg)), timeout=5)
            if got != msg:
                ok = False
                break
        check("200 interleaved round trips", ok)
    finally:
        writer.close()


async def test_remote_close_propagates() -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", LISTEN_PORT)
    try:
        writer.write(b"QUIT")
        await writer.drain()
        got = await asyncio.wait_for(reader.read(), timeout=5)
        check("remote close reaches the local client", got == b"", f"got {got!r}")
    except asyncio.TimeoutError:
        check("remote close reaches the local client", False, "timed out")
    finally:
        writer.close()


async def test_no_stream_leak() -> None:
    for _ in range(25):
        reader, writer = await asyncio.open_connection("127.0.0.1", LISTEN_PORT)
        writer.write(b"x")
        await writer.drain()
        await asyncio.wait_for(reader.readexactly(1), timeout=5)
        writer.close()
        await writer.wait_closed()
    # Give the relay a moment to run the teardown paths.
    await asyncio.sleep(1.0)
    check("no pending streams leaked", len(tunnel.pending) == 0, f"pending={len(tunnel.pending)}")


async def test_wrong_key_rejected() -> None:
    from tcpbroke.cli.main import _connect_kwargs, server_url_to_ws
    from tcpbroke.protocol import CLOSE_UNAUTHORIZED
    from websockets.asyncio.client import connect as ws_connect
    from websockets.exceptions import ConnectionClosed

    ws_url = server_url_to_ws(RELAY_URL, "/listen")
    code = None
    try:
        ws = await ws_connect(ws_url, **_connect_kwargs("wrong-key", ws_url))
        try:
            await ws.recv()
        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd else None
    except ConnectionClosed as exc:
        code = exc.rcvd.code if exc.rcvd else None
    check("wrong key closed with 4001", code == CLOSE_UNAUTHORIZED, f"code={code}")


async def test_second_agent_rejected() -> None:
    from tcpbroke.cli.main import _connect_kwargs, server_url_to_ws
    from tcpbroke.protocol import CLOSE_OCCUPIED
    from websockets.asyncio.client import connect as ws_connect
    from websockets.exceptions import ConnectionClosed

    ws_url = server_url_to_ws(RELAY_URL, "/control")
    code = None
    try:
        ws = await ws_connect(ws_url, **_connect_kwargs(KEY, ws_url))
        try:
            await ws.recv()
        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd else None
    except ConnectionClosed as exc:
        code = exc.rcvd.code if exc.rcvd else None
    check("second agent closed with 4000", code == CLOSE_OCCUPIED, f"code={code}")


# ---------------------------------------------------------------------------
# Driver.
# ---------------------------------------------------------------------------

async def main() -> int:
    echo = await asyncio.start_server(_echo_client, "127.0.0.1", ECHO_PORT)

    config = uvicorn.Config(
        app, host="127.0.0.1", port=RELAY_PORT, log_level="warning", ws_ping_interval=None
    )
    relay = uvicorn.Server(config)
    relay_task = asyncio.create_task(relay.serve())

    await _wait_for(_is_up, 15, "relay healthz")

    agent_task = asyncio.create_task(run_agent("127.0.0.1", ECHO_PORT, RELAY_URL, KEY))
    await _wait_for(_agent_connected, 15, "agent registration")

    listener_task = asyncio.create_task(run_listener("127.0.0.1", LISTEN_PORT, RELAY_URL, KEY))
    await _wait_for(_listener_up, 10, "listener")

    print("\nrunning tests")
    for test in (
        test_round_trip,
        test_large_transfer,
        test_interleaved,
        test_remote_close_propagates,
        test_no_stream_leak,
        test_wrong_key_rejected,
        test_second_agent_rejected,
    ):
        try:
            await test()
        except Exception as exc:  # a raised exception is a failed test, not a crashed run
            check(test.__name__, False, f"raised {exc!r}")

    for task in (listener_task, agent_task):
        task.cancel()
    relay.should_exit = True
    await asyncio.gather(listener_task, agent_task, relay_task, return_exceptions=True)
    echo.close()
    await echo.wait_closed()

    print()
    if _failures:
        print(f"FAILED: {len(_failures)} - {', '.join(_failures)}")
        return 1
    print("all tests passed")
    return 0


async def _is_up() -> bool:
    body = await _healthz()
    return body.get("status") == "ok"


async def _agent_connected() -> bool:
    body = await _healthz()
    return bool(body.get("agent_connected"))


async def _listener_up() -> bool:
    try:
        _r, w = await asyncio.open_connection("127.0.0.1", LISTEN_PORT)
    except OSError:
        return False
    w.close()
    return True


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
