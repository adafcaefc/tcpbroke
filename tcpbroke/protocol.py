"""Shared constants and control-channel messages for tcpbroke.

tcpbroke tunnels a RAW TCP port (RDP, 3389) out of a host with no inbound ports, over a single
outbound WSS/443 connection. It is a sibling of nbroke, NOT an extension of it: nbroke is
HTTP-shaped (it carries method/path/headers and replays them with httpx), which cannot represent an
opaque byte stream.

The architecture deliberately differs from nbroke's too. nbroke multiplexes every request over one
control socket; tcpbroke gives each TCP connection its OWN socket end to end:

    mstsc -> 127.0.0.1:13389
               listen mode  --WSS /listen-->  server  --WSS /stream/<id>--  agent  -> 127.0.0.1:3389

The control channel therefore carries almost nothing - just `open`, `open_failed`, and a keepalive
ping. Once a stream is paired, both halves are pure binary WebSocket frames with no header, no
stream id, no base64 and no JSON, so an interactive desktop is not paying nbroke's ~33% base64
inflation plus a JSON round trip on every display update, and one stream can never head-of-line
block another.
"""
from __future__ import annotations

from typing import Any

# The shared key travels in a HEADER, never the query string. nbroke appends `?password=...` to the
# WebSocket URL, which lands the secret in ingress access logs - tolerable for a git endpoint, not
# for one that fronts an interactive desktop.
AUTH_HEADER = "x-tunnel-key"

# Control-channel message types (text JSON frames; the data path is binary and unframed).
MSG_OPEN = "open"                # server -> agent: dial the local port and bring me a stream
MSG_OPEN_FAILED = "open_failed"  # agent -> server: could not dial; fail the waiter now
MSG_PING = "ping"                # agent -> server
MSG_PONG = "pong"                # server -> agent

# Application close codes (>= 4000 is the private range). The client distinguishes these to decide
# whether reconnecting is pointless - a wrong key or an occupied slot will not fix itself.
CLOSE_OCCUPIED = 4000
CLOSE_UNAUTHORIZED = 4001
CLOSE_RATE_LIMITED = 4002
CLOSE_NO_AGENT = 4003
CLOSE_UNKNOWN_STREAM = 4004

# Bytes read from a local socket per relay hop. 64 KiB keeps RDP's many small writes cheap while
# staying far below any WebSocket message cap.
READ_CHUNK_SIZE = 64 * 1024

# Keepalive on the control channel, so an idle tunnel is not reaped by proxy/ingress idle timeouts.
PING_INTERVAL_SECONDS = 30.0


def make_open(stream_id: str) -> dict[str, Any]:
    return {"type": MSG_OPEN, "stream_id": stream_id}


def make_open_failed(stream_id: str, detail: str) -> dict[str, Any]:
    return {"type": MSG_OPEN_FAILED, "stream_id": stream_id, "detail": detail}
