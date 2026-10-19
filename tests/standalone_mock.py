#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""独立 mock 上游（给 exe 端到端验收用，不带测试框架）。

用法: python tests/standalone_mock.py 8787
输出首行打印实际端口，供外部脚本读取。
"""
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODELS = ["exe-alpha", "exe-beta", "exe-gamma"]


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        ln = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(ln) if ln else b"{}"
        try:
            mid = json.loads(raw.decode()).get("model")
        except Exception:
            mid = None
        body = json.dumps({
            "id": "chatcmpl-exe", "object": "chat.completion", "model": mid,
            "choices": [{"index": 0, "message": {"role": "assistant",
                                                 "content": "exe-ok-%s" % mid},
                          "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    httpd = ThreadingHTTPServer(("127.0.0.1", port), H)
    real = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print("MOCK_PORT=%d" % real, flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
