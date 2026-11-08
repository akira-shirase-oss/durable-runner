#!/usr/bin/env python3
"""Pilot workflow step handler — the smallest honest allowlisted handler.

POST /step  {input, idem_key}  -> 200 {"ok": true, "step_input_len": N}
   Records the idem_key; a repeated key returns the recorded answer —
   the handler is idempotent because the runner is at-least-once.
GET  /step?idem_key=K          -> 200 if K was seen (the reconcile probe),
                                404 otherwise.
"""
import http.server
import json
import urllib.parse

SEEN = {}


class H(http.server.BaseHTTPRequestHandler):
    def _reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        req = json.loads(self.rfile.read(n) or b"{}")
        key = req.get("idem_key", "")
        if key not in SEEN:
            SEEN[key] = {"ok": True, "step_input_len": len(req.get("input", []))}
        self._reply(200, json.dumps(SEEN[key]).encode())

    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        key = q.get("idem_key", [""])[0]
        self._reply(200 if key in SEEN else 404)

    def log_message(self, *a):
        pass


http.server.HTTPServer(("127.0.0.1", 58320), H).serve_forever()
