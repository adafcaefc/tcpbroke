#!/usr/bin/env python3
"""tcpbroke client - the two host-side roles of the tunnel.

    tcpbroke agent  -p 3389  -s https://rdp.example.dev -k KEY
        Runs on the machine being reached. Holds one outbound control socket; when the server asks,
        dials the local port and brings up a matching stream socket.

    tcpbroke listen -l 13389 -s https://rdp.example.dev -k KEY
        Runs on the machine doing the reaching. Listens on 127.0.0.1 and turns each accepted TCP
        connection into its own stream. Point mstsc at 127.0.0.1:13389.

Neither mode knows anything about RDP. This is a byte pipe.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import socket
import ssl
import sys
from typing import Any

import certifi
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK

from ..protocol import (
    AUTH_HEADER,
    CLOSE_NO_AGENT,
    CLOSE_OCCUPIED,
    CLOSE_RATE_LIMITED,
    CLOSE_UNAUTHORIZED,
    MSG_OPEN,
    MSG_PING,
    MSG_PONG,
    PING_INTERVAL_SECONDS,
    READ_CHUNK_SIZE,
    make_open_failed,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RECONNECT_DELAY_BASE = 2.0
RECONNECT_DELAY_MAX = 30.0

# Close codes that will not fix themselves by trying again.
FATAL_CLOSE_CODES = {
    CLOSE_OCCUPIED: "Tunnel is occupied by another agent",
    CLOSE_UNAUTHORIZED: "Wrong key",
    CLOSE_RATE_LIMITED: "Rate limited by the server (too many failed keys)",
}


def server_url_to_ws(url: str, path: str) -> str:
    url = url.rstrip("/")
    if url.startswith("https://"):
        return "wss://" + url[len("https://"):] + path
    if url.startswith("http://"):
        return "ws://" + url[len("http://"):] + path
    raise ValueError(f"Unknown URL scheme: {url}")


def _ssl_context(ws_url: str) -> ssl.SSLContext | None:
    # certifi rather than the OS trust store: the frozen single-file build has no other CA bundle.
    if not ws_url.startswith("wss://"):
        return None
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(cafile=certifi.where())
    return ctx


def _connect_kwargs(key: str, ws_url: str) -> dict[str, Any]:
    return {
        "additional_headers": {AUTH_HEADER: key},
        "ssl": _ssl_context(ws_url),
        "open_timeout": 15,
        "max_size": None,
        # The payload is already-compressed RDP; deflate would burn CPU for nothing and add latency.
        "compression": None,
    }


def _close_code(exc: BaseException) -> int | None:
    rcvd = getattr(exc, "rcvd", None)
    if rcvd is not None:
        return rcvd.code
    return getattr(exc, "code", None)


def _set_nodelay(writer: asyncio.StreamWriter) -> None:
    """Disable Nagle. RDP is a stream of small interactive writes; coalescing them adds visible lag."""
    sock = writer.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Byte pumps shared by both modes.
# ---------------------------------------------------------------------------

async def _tcp_to_ws(reader: asyncio.StreamReader, ws: Any) -> None:
    while True:
        data = await reader.read(READ_CHUNK_SIZE)
        if not data:
            return
        await ws.send(data)


async def _ws_to_tcp(ws: Any, writer: asyncio.StreamWriter) -> None:
    async for data in ws:
        if isinstance(data, str):
            data = data.encode()
        writer.write(data)
        await writer.drain()


async def _splice(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, ws: Any) -> None:
    """Join a local TCP socket to a tunnel socket until either end goes away."""
    tasks = {
        asyncio.create_task(_tcp_to_ws(reader, ws)),
        asyncio.create_task(_ws_to_tcp(ws, writer)),
    }
    try:
        done, still_running = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in still_running:
            task.cancel()
        await asyncio.gather(*still_running, return_exceptions=True)
        # Retrieve the finished task's exception even though we do nothing with it. A peer that
        # vanishes without a close frame is the NORMAL way a stream ends here, and an unretrieved
        # exception makes asyncio dump a full traceback at GC time - once per connection, into the
        # agent's log file, which on a headless SYSTEM task is the only diagnostic there is.
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.debug("splice_ended: %s", exc)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        try:
            await ws.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Agent mode.
# ---------------------------------------------------------------------------

async def _agent_open_stream(
    stream_id: str, host: str, port: int, server_url: str, key: str, ctrl_ws: Any
) -> None:
    """Dial the local port, then bring up the matching stream socket to the server."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=10
        )
        _set_nodelay(writer)
    except Exception as exc:
        logger.error("local_dial_failed id=%s %s:%d error=%s", stream_id, host, port, exc)
        try:
            await ctrl_ws.send(json.dumps(make_open_failed(stream_id, str(exc))))
        except Exception:
            pass
        return

    ws_url = server_url_to_ws(server_url, f"/stream/{stream_id}")
    try:
        stream_ws = await ws_connect(ws_url, **_connect_kwargs(key, ws_url))
    except Exception as exc:
        logger.error("stream_connect_failed id=%s error=%s", stream_id, exc)
        writer.close()
        try:
            await ctrl_ws.send(json.dumps(make_open_failed(stream_id, str(exc))))
        except Exception:
            pass
        return

    logger.info("stream_open id=%s -> %s:%d", stream_id, host, port)
    try:
        await _splice(reader, writer, stream_ws)
    finally:
        logger.info("stream_close id=%s", stream_id)


async def _ping_loop(ws: Any, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            if stop.is_set():
                return
            await ws.send(json.dumps({"type": MSG_PING}))
        except Exception:
            return


async def _run_agent_once(host: str, port: int, server_url: str, key: str) -> bool:
    """One control-channel session. Returns True if a reconnect is worth attempting."""
    ws_url = server_url_to_ws(server_url, "/control")
    logger.info("connecting %s", ws_url)
    try:
        ctrl_ws = await ws_connect(ws_url, **_connect_kwargs(key, ws_url))
    except ConnectionClosed as exc:
        fatal = FATAL_CLOSE_CODES.get(_close_code(exc))
        if fatal:
            print(fatal, flush=True)
            return False
        logger.error("connect_failed: %s", exc)
        return True
    except Exception as exc:
        logger.error("connect_failed: %s", exc)
        return True

    print(f"Serving {host}:{port} at {server_url}", flush=True)
    logger.info("agent_connected")

    stop = asyncio.Event()
    ping_task = asyncio.create_task(_ping_loop(ctrl_ws, stop))
    stream_tasks: set[asyncio.Task] = set()

    try:
        async for raw in ctrl_ws:
            msg = json.loads(raw)
            msg_type = msg.get("type")

            if msg_type == MSG_PONG:
                continue

            if msg_type == MSG_OPEN:
                task = asyncio.create_task(
                    _agent_open_stream(
                        msg["stream_id"], host, port, server_url, key, ctrl_ws
                    )
                )
                stream_tasks.add(task)
                task.add_done_callback(stream_tasks.discard)
            else:
                logger.warning("unknown_control_message type=%s", msg_type)

    except ConnectionClosedError as exc:
        fatal = FATAL_CLOSE_CODES.get(_close_code(exc))
        if fatal:
            print(fatal, flush=True)
            return False
        logger.info("control_closed code=%s", _close_code(exc))
    except ConnectionClosedOK:
        logger.info("control_closed cleanly")
    except Exception as exc:
        logger.error("control_error: %s", exc)
    finally:
        stop.set()
        ping_task.cancel()
        await asyncio.gather(ping_task, return_exceptions=True)
        # Existing streams die with the control channel: the server drops their pending state on
        # agent disconnect, so leaving them running would only strand sockets.
        for task in list(stream_tasks):
            task.cancel()
        await asyncio.gather(*stream_tasks, return_exceptions=True)

    return True


async def run_agent(host: str, port: int, server_url: str, key: str) -> None:
    delay = RECONNECT_DELAY_BASE
    while True:
        if not await _run_agent_once(host, port, server_url, key):
            sys.exit(1)
        logger.info("reconnecting in %.1fs", delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, RECONNECT_DELAY_MAX)


# ---------------------------------------------------------------------------
# Listen mode.
# ---------------------------------------------------------------------------

async def _handle_local_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    server_url: str,
    key: str,
) -> None:
    peer = writer.get_extra_info("peername")
    _set_nodelay(writer)

    ws_url = server_url_to_ws(server_url, "/listen")
    try:
        ws = await ws_connect(ws_url, **_connect_kwargs(key, ws_url))
    except ConnectionClosed as exc:
        code = _close_code(exc)
        reason = FATAL_CLOSE_CODES.get(code)
        if code == CLOSE_NO_AGENT:
            reason = "No agent is connected to the tunnel"
        logger.error("tunnel_connect_failed peer=%s: %s", peer, reason or exc)
        writer.close()
        return
    except Exception as exc:
        logger.error("tunnel_connect_failed peer=%s: %s", peer, exc)
        writer.close()
        return

    logger.info("connection_open peer=%s", peer)
    try:
        await _splice(reader, writer, ws)
    finally:
        logger.info("connection_close peer=%s", peer)


async def run_listener(bind: str, lport: int, server_url: str, key: str) -> None:
    async def _client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _handle_local_client(reader, writer, server_url, key)

    server = await asyncio.start_server(_client, bind, lport)
    print(f"Listening on {bind}:{lport} -> {server_url}", flush=True)
    async with server:
        await server.serve_forever()


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(prog="tcpbroke", description="Raw TCP reverse tunnel")
    sub = parser.add_subparsers(dest="mode", required=True)

    agent = sub.add_parser("agent", help="Expose a local TCP port through the tunnel")
    agent.add_argument("-p", "--port", type=int, required=True, help="Local TCP port to expose")
    agent.add_argument("--host", default="127.0.0.1", help="Local host to dial (default 127.0.0.1)")
    agent.add_argument("-s", "--server", required=True, help="Server URL (http:// or https://)")
    agent.add_argument("-k", "--key", required=True, help="Shared tunnel key")

    listen = sub.add_parser("listen", help="Accept local connections and forward them")
    listen.add_argument("-l", "--lport", type=int, required=True, help="Local port to listen on")
    # Loopback by default so the listening machine never relays the tunnel onto its own LAN.
    listen.add_argument("--bind", default="127.0.0.1", help="Bind address (default 127.0.0.1)")
    listen.add_argument("-s", "--server", required=True, help="Server URL (http:// or https://)")
    listen.add_argument("-k", "--key", required=True, help="Shared tunnel key")

    args = parser.parse_args()
    server_url = args.server.rstrip("/")

    try:
        if args.mode == "agent":
            asyncio.run(run_agent(args.host, args.port, server_url, args.key))
        else:
            asyncio.run(run_listener(args.bind, args.lport, server_url, args.key))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
