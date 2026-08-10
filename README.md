# tcpbroke

A raw **TCP** reverse tunnel over a single outbound **WSS/443** connection. Built to reach RDP on a
host with no inbound ports - the deploy VM behind `git.eyds.adaf.dev` - with a **native client**
(`mstsc`), not a browser.

Sibling of **nbroke**, not a fork of it. nbroke is HTTP-shaped: its protocol carries
`method`/`path`/`headers` and its agent replays each message through httpx to
`http://127.0.0.1:<port>`. There is no way to express an opaque byte stream in that, and reworking
it to carry one would mean shipping a new build of the binary that currently serves git.

## How it works

```
mstsc --> 127.0.0.1:13389
            tcpbroke listen (operator machine)
                 |  WSS /listen                     one per TCP connection
                 v
            tcpbroke server  (public, SINGLE replica)
                 |  WSS /control   "open <id>"      control only: open, open_failed, ping
                 |  WSS /stream/<id>                one per TCP connection
                 v
            tcpbroke agent (target host)  -->  127.0.0.1:3389
```

**Each TCP connection gets its own socket end to end.** The control channel carries nothing but
`open`, `open_failed` and a keepalive ping. Once a stream is paired, both halves are pure binary
WebSocket frames - no frame header, no stream id in the data path, no base64, no JSON.

That is the main departure from nbroke, which multiplexes every request over one control socket.
Correct for HTTP; wrong for an interactive desktop, where it would mean head-of-line blocking plus
~33% base64 inflation and a JSON round trip on every display update.

| Endpoint | Who connects | Purpose |
| --- | --- | --- |
| `WS /control` | agent | registers the single agent slot; carries `open` / `open_failed` / `ping` |
| `WS /listen` | operator | one per accepted local connection |
| `WS /stream/{id}` | agent | the matching half, dialled on demand |
| `GET /healthz` | probes | `{"status","agent_connected"}` |

## Usage

On the target host:

```
tcpbroke-windows.exe agent -p 3389 -s https://rdp.example.dev -k <KEY>
```

On the operator machine:

```
tcpbroke-windows.exe listen -l 13389 -s https://rdp.example.dev -k <KEY>
mstsc /v:127.0.0.1:13389
```

`listen` binds **127.0.0.1** by default, so the operator machine never relays the tunnel onto its
own LAN. `--bind` overrides it; think before you do.

Neither mode knows anything about RDP - point it at any TCP port.

## Server deployment

Container, **one replica, no scale to zero**. Tunnel state is in process memory
(`tcpbroke/server/state.py`), so a second replica serves a tunnel it does not have, and a cold
start silently drops the agent.

```
docker build -t tcpbroke-server .
docker run -e TUNNEL_KEY=<KEY> -p 8000:8000 tcpbroke-server
```

`TUNNEL_KEY` is required - the app **refuses to start** without it rather than quietly serving an
unauthenticated path to a desktop.

| Env | Default | Notes |
| --- | --- | --- |
| `TUNNEL_KEY` | *(required)* | shared secret for all three endpoints |
| `STREAM_OPEN_TIMEOUT_SECONDS` | 15 | how long a viewer waits for the agent to dial back |
| `CONTROL_IDLE_TIMEOUT_SECONDS` | 180 | releases the agent slot when a socket dies silently |
| `AUTH_FAIL_LIMIT` / `_WINDOW_SECONDS` / `_BLOCK_SECONDS` | 5 / 300 / 900 | per-IP lockout |
| `TRUST_FORWARDED_FOR` | true | set false if not behind exactly one managed ingress |

## Security

This publishes a path to a desktop. The key is the only thing in front of it:

- **Long random key, not shared with any other tunnel.** Rotate it in both places together.
- The key travels in the **`X-Tunnel-Key` header**, never the query string - nbroke's
  `?password=...` puts the secret into ingress access logs.
- Failed auth gets a delay, then a per-IP lockout, and is logged with the source IP.
- Set an **Account Lockout Policy** on the target host and use a strong account password. The
  tunnel key stops discovery and scanning; it is not a substitute for the OS gate behind it.
- `TRUST_FORWARDED_FOR` must be false if anything other than a single trusted ingress can reach the
  server, or the per-IP lockout is trivially spoofed.

## CI

Two workflows, mirroring nbroke's:

| Workflow | Trigger | What it does |
| --- | --- | --- |
| `.github/workflows/deploy.yml` | any push, or manual | builds the server image, pushes it to ACR as `tcpbroke-server:<Jakarta timestamp>`, then POSTs a Logic App to roll the Container App onto that tag |
| `.github/workflows/release.yml` | push to `main`, or manual | PyInstaller builds for Windows and Linux, republished as the `nightly` prerelease |

Both need repository secrets: `ACR_DEV_LOGIN_SERVER`, `ACR_DEV_USERNAME`, `ACR_DEV_PASSWORD`,
`ACA_DEPLOY_LOGIC_APP_URL`, `ACA_DEPLOY_PASSWORD`. `deploy.yml` also hardcodes the resource group
`AI-Dev-JV_RG`, copied from nbroke - **check it, and check that the rollout Logic App knows about a
Container App for `tcpbroke-server`**, before trusting the first run.

The release workflow freezes **`entrypoint_cli.py`**, not `tcpbroke/cli/main.py`. This CLI imports
from its own package (`from ..protocol import ...`); handing PyInstaller the module file directly
makes it a top-level script with no parent package and the binary dies on startup.

### Building locally

Only needed to test a build without pushing - CI is the real path. Requires a **real** Python on
Windows; the Microsoft Store alias in `WindowsApps` is a stub:

```
py -m pip install -r requirements-dev.txt
powershell -ExecutionPolicy Bypass -File packaging\build-windows.ps1
```

The script prints the SHA256 and the commit.

## Test

Full end-to-end (relay + agent + listener + echo server, all in one process):

```
docker run --rm -v "$PWD:/app" -w /app python:3.11-slim \
    sh -c "pip install -q -r requirements-dev.txt && python -m tests.e2e"
```

Covers round trips, a 4 MiB transfer, 200 interleaved small writes, close propagation, stream-leak
checks, wrong-key rejection, and second-agent rejection.
