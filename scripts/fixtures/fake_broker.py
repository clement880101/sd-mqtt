#!/usr/bin/env python3
"""A stand-in for `sdmqtt serve`, for `scripts/test_idle_memory.py`.

Takes the arguments the idle memory check passes, answers
`/health/ready` on the admin address, and holds `FAKE_HOLD_MB`
megabytes of memory it has written to, so the resident figure is known
to be at least that. `FAKE_READY=0` makes it never answer, which is
the broker that does not come up.
"""
import http.server
import os
import sys
import time

admin = sys.argv[sys.argv.index("--admin-address") + 1]
host, port = admin.rsplit(":", 1)
held = bytearray(int(os.environ.get("FAKE_HOLD_MB", "0")) * 1024 * 1024)
for at in range(0, len(held), 4096):
    held[at] = 1  # touched, so it is resident rather than reserved

if os.environ.get("FAKE_READY", "1") == "0":
    time.sleep(3600)


class Ready(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - the standard library's name
        body = b'{"status":"ready"}'
        self.send_response(200)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


http.server.HTTPServer((host, int(port)), Ready).serve_forever()
