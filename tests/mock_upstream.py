#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""可编程 mock 上游，用来精确验证看门狗行为。

每个「模型」独立配置延迟与响应码，端口由调用方传入（测试里起在随机端口）。
路径 /_stats 返回每个模型被实际请求的次数，用来判断是否真的发生了对冲
（次数 > 1 说明第二路被发起了）、以及落败路是否被放弃。
"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class MockUpstream:
    def __init__(self, plan):
        """plan: {model_id: {"delay": 秒, "status": 200, "body": str|None}}"""
        self.plan = plan
        self.hits = {}
        self.lock = threading.Lock()
        self.httpd = None
        self.port = None

    # ---- 内部 ----
    def _record(self, mid):
        with self.lock:
            self.hits[mid] = self.hits.get(mid, 0) + 1

    def hits_of(self, mid):
        with self.lock:
            return self.hits.get(mid, 0)

    def reset_hits(self):
        with self.lock:
            self.hits.clear()

    def _handler(self):
        outer = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                ln = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(ln) if ln else b"{}"
                try:
                    mid = json.loads(raw.decode()).get("model")
                except Exception:
                    mid = None
                outer._record(mid)
                conf = outer.plan.get(mid) or {"delay": 0, "status": 200}
                d = conf.get("delay", 0)
                if d:
                    time.sleep(d)
                code = conf.get("status", 200)
                if code >= 400:
                    body = json.dumps({"error": {"message": "mock %d" % code,
                                                 "type": "mock"}}).encode()
                else:
                    body = conf.get("body") or json.dumps({
                        "id": "chatcmpl-mock", "object": "chat.completion",
                        "model": mid,
                        "choices": [{"index": 0, "message": {
                            "role": "assistant",
                            "content": "mock-ok-%s" % mid},
                            "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                                  "total_tokens": 2},
                    }).encode()
                try:
                    self.send_response(code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def do_GET(self):
                if self.path.startswith("/_stats"):
                    body = json.dumps(outer.hits).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *a):
                pass

        return H

    def start(self):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        return self

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()

    @property
    def base(self):
        return "http://127.0.0.1:%d/v1" % self.port
