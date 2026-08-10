import os

# Shared secret required on EVERY endpoint (agent, viewer and stream). Unset means the tunnel is
# open to anyone who knows the hostname - refused at startup by main.py rather than silently
# serving an unauthenticated path to a desktop.
TUNNEL_KEY: str | None = os.environ.get("TUNNEL_KEY") or None

# How long a viewer waits for the agent to dial back in with its half of the stream. The agent only
# has to open one local TCP connection and one WSS, so this is generous.
STREAM_OPEN_TIMEOUT_SECONDS: float = float(os.environ.get("STREAM_OPEN_TIMEOUT_SECONDS", 15))

# Drop a control channel that has gone quiet for this long. The agent pings every 30s, so silence
# means the socket is dead in a way TCP has not noticed yet (a half-open NAT/proxy state), and the
# single agent slot must be released or a reconnect gets rejected as occupied forever.
CONTROL_IDLE_TIMEOUT_SECONDS: float = float(os.environ.get("CONTROL_IDLE_TIMEOUT_SECONDS", 180))
IDLE_CHECK_INTERVAL_SECONDS: float = 30.0

# Failed-auth throttling, per client IP. nbroke just closes with 4001 and lets you retry instantly
# and forever; this endpoint fronts RDP, so brute force gets a delay, then a lockout.
AUTH_FAIL_DELAY_SECONDS: float = float(os.environ.get("AUTH_FAIL_DELAY_SECONDS", 1.0))
AUTH_FAIL_LIMIT: int = int(os.environ.get("AUTH_FAIL_LIMIT", 5))
AUTH_FAIL_WINDOW_SECONDS: float = float(os.environ.get("AUTH_FAIL_WINDOW_SECONDS", 300))
AUTH_FAIL_BLOCK_SECONDS: float = float(os.environ.get("AUTH_FAIL_BLOCK_SECONDS", 900))

# Trust X-Forwarded-For for the client IP used above. True behind a single managed ingress (Azure
# Container Apps); set false if the server is ever exposed directly, or the header is trivially
# spoofable and the lockout becomes useless.
TRUST_FORWARDED_FOR: bool = os.environ.get("TRUST_FORWARDED_FOR", "true").lower() == "true"
