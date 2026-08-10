#!/usr/bin/env python3
"""One-shot RDP-over-tcpbroke connect helper, for the operator machine.

Starts the listener, waits for it to bind, stores the credential, opens mstsc, and tears the
listener down when the session ends.

    python rdp-connect.py

Secrets come from the environment, never the source - this file is committed:

    set TCPBROKE_KEY=<the tunnel key>
    set RDP_PASSWORD=<the VM account password>     (optional; prompts if unset)
"""
from __future__ import annotations

import getpass
import os
import socket
import subprocess
import sys
import time

# --- config ------------------------------------------------------------------
EXE = r"C:\Users\PE841SK\Downloads\tcpbroke-windows.exe"
RELAY = "https://rdp.eyds.adaf.dev"
LPORT = 13389
# The VM is WORKGROUP, so the account MUST be machine-qualified. A bare "eydstestrepo" is resolved
# against the CLIENT's domain, which is a different account that does not exist on the far end -
# and mstsc, handed a credential it cannot use, falls back to the smart-card path.
RDP_USER = r"eydslocalrepoVM\eydstestrepo"
# What mstsc is pointed at. Any name that resolves to 127.0.0.1 works; a real name is preferable to
# the bare loopback address, because CredSSP derives its target SPN from this string and
# TERMSRV/127.0.0.1 means "this machine" - the client then tries to authenticate to itself.
RDP_HOST = "127.0.0.1"

LISTENER_LOG = "tcpbroke-listen.log"


def wait_for_port(port: int, timeout: float = 15.0) -> bool:
    """Block until something is accepting on the port. Racing mstsc against the listener's bind is
    the difference between a working script and an intermittent 'cannot connect'."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.25)
    return False


def main() -> int:
    key = os.environ.get("TCPBROKE_KEY") or getpass.getpass("tunnel key: ")
    if not key:
        print("no tunnel key", file=sys.stderr)
        return 1
    password = os.environ.get("RDP_PASSWORD") or getpass.getpass(f"password for {RDP_USER}: ")

    if not os.path.isfile(EXE):
        print(f"tcpbroke not found: {EXE}", file=sys.stderr)
        return 1

    # Argument LISTS, not shell strings: the key and password go straight to the process without
    # cmd.exe parsing them, so &, ^, | and quotes in a secret can never mangle the command.
    print(f"starting listener on 127.0.0.1:{LPORT} -> {RELAY}")
    with open(LISTENER_LOG, "wb") as log:
        listener = subprocess.Popen(
            [EXE, "listen", "-l", str(LPORT), "-s", RELAY, "-k", key],
            stdout=log, stderr=subprocess.STDOUT,
        )

        try:
            if not wait_for_port(LPORT):
                print(f"listener never bound {LPORT} - see {LISTENER_LOG}", file=sys.stderr)
                return 1
            print("listener up")

            # Register the credential under BOTH forms. mstsc is inconsistent about whether the
            # non-default port is part of the lookup key, so covering both avoids a silent
            # fall-through to an interactive prompt.
            for target in (f"TERMSRV/{RDP_HOST}", f"TERMSRV/{RDP_HOST}:{LPORT}"):
                subprocess.run(
                    ["cmdkey", f"/generic:{target}", f"/user:{RDP_USER}", f"/pass:{password}"],
                    check=False, stdout=subprocess.DEVNULL,
                )

            print(f"opening mstsc -> {RDP_HOST}:{LPORT} as {RDP_USER}")
            # Blocks until the RDP window closes, which is what keeps the listener alive for the
            # whole session.
            subprocess.run(["mstsc", f"/v:{RDP_HOST}:{LPORT}"], check=False)
        finally:
            print("closing listener")
            listener.terminate()
            try:
                listener.wait(timeout=10)
            except subprocess.TimeoutExpired:
                listener.kill()
            # Leave no stored password behind on a shared or managed machine.
            for target in (f"TERMSRV/{RDP_HOST}", f"TERMSRV/{RDP_HOST}:{LPORT}"):
                subprocess.run(["cmdkey", f"/delete:{target}"],
                               check=False, stdout=subprocess.DEVNULL)

    return 0


if __name__ == "__main__":
    sys.exit(main())
