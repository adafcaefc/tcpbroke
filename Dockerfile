# Public relay server. Build and push this, then run it as a SINGLE replica - tunnel state lives in
# process memory, so a second replica would serve a tunnel it does not have (see server/state.py).
FROM python:3.11-slim

WORKDIR /app

COPY requirements-server.txt ./
RUN pip install --no-cache-dir -r requirements-server.txt

COPY tcpbroke ./tcpbroke

# TUNNEL_KEY is required; the app refuses to start without it (server/main.py lifespan).
ENV PORT=8000
EXPOSE 8000

# --ws-per-message-deflate false: the payload is already-compressed RDP, so deflate is pure CPU and
# added latency. It takes a VALUE - there is no --no-ws-per-message-deflate form, unlike
# --no-server-header next to it. --proxy-headers so X-Forwarded-For reaches the per-IP auth throttle.
CMD ["sh", "-c", "uvicorn tcpbroke.server.main:app --host 0.0.0.0 --port ${PORT} --proxy-headers --ws-per-message-deflate false --no-server-header"]
