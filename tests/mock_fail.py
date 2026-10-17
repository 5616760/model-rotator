#!/usr/bin/env python3
"""最小 mock 上游：端口3067，/v1/chat/completions 永远返回 429。用于验证失败转移。"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        ln = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(ln)
        body = json.dumps({"error": {"message": "Too Many Requests", "type": "rate_limit_error"}}).encode()
        self.send_response(429)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


HTTPServer(("127.0.0.1", 3067), H).serve_forever()
