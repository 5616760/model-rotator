#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""端到端验证 /v1/responses 经 rotator 可用：非流式 + 流式 + 路由日志 + 未知端点。"""
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:3166"
LOG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rotator.log")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
ROUTED = re.compile(r"routed sid=\S+ model=(\S+) acct=(\S+).*?ep=(\S+) OK")


def post_json(path, body):
    """非流式：把 body 读完再返回（不能在 with 外 read，流已关闭）。"""
    data = json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer rotator-local")
    t0 = time.time()
    with OPENER.open(req, timeout=120) as r:
        status = r.status
        raw = r.read()
    return status, json.loads(raw.decode()), time.time() - t0


def post_stream(path, body):
    """流式：在 with 内消费完 SSE，返回 (status, 事件数, 是否见到 response 数据, 耗时)。"""
    data = json.dumps(dict(body, stream=True)).encode()
    req = urllib.request.Request(BASE + path, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer rotator-local")
    req.add_header("Accept", "text/event-stream")
    t0 = time.time()
    events = 0
    saw = False
    with OPENER.open(req, timeout=120) as r:
        status = r.status
        for raw in r:
            line = raw.strip()
            if line.startswith(b"event:"):
                events += 1
            if line.startswith(b"data:") and b"response" in line:
                saw = True
    return status, events, saw, time.time() - t0


def log_size():
    try:
        return os.path.getsize(LOG)
    except OSError:
        return 0


def new_route(off):
    with open(LOG, "r", encoding="utf-8", errors="replace") as f:
        f.seek(off)
        for line in f:
            m = ROUTED.search(line)
            if m:
                return m.groups()
    return None


def main():
    body = {
        "model": "AutoRotate-30m",
        "input": "只回复两个字：收到",
        "max_output_tokens": 16,
    }

    # 1) 非流式
    off = log_size()
    status, j, dur = post_json("/v1/responses", body)
    got = j.get("object")
    txt = j.get("output_text") or ""
    time.sleep(0.3)
    route = new_route(off)
    print("1) 非流式 /v1/responses")
    print("   status=%d object=%s model=%s output_text=%r %.1fs"
          % (status, got, j.get("model"), txt[:20], dur))
    print("   路由日志 model=%s acct=%s ep=%s" % route if route else "   !! 无路由日志")
    ok1 = status == 200 and got == "response" and route and route[2] == "responses"

    # 2) 流式
    off = log_size()
    status, events, saw_data, dur = post_stream("/v1/responses", body)
    time.sleep(0.3)
    route2 = new_route(off)
    print("\n2) 流式 /v1/responses")
    print("   status=%d SSE 事件数=%d 含 response 数据=%s %.1fs"
          % (status, events, saw_data, dur))
    print("   路由日志 model=%s acct=%s ep=%s" % route2 if route2 else "   !! 无路由日志")
    ok2 = status == 200 and events > 0 and saw_data and route2 and route2[2] == "responses"

    # 3) 旧端点 chat/completions 不受影响
    off = log_size()
    cb = {"model": "AutoRotate-30m",
          "messages": [{"role": "user", "content": "只回复两个字：收到"}],
          "max_tokens": 16}
    status, j, dur = post_json("/v1/chat/completions", cb)
    time.sleep(0.3)
    route3 = new_route(off)
    print("\n3) 回归 /v1/chat/completions")
    print("   status=%d object=%s 路由 ep=%s"
          % (status, j.get("object"), route3[2] if route3 else None))
    ok3 = status == 200 and route3 and route3[2] == "completions"

    # 4) 未知端点仍 404；带 query string 的合法端点仍放行
    print("\n4) 未知端点 vs query string")
    req = urllib.request.Request(BASE + "/v1/embeddings", data=b"{}", method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        OPENER.open(req, timeout=10)
        print("   /v1/embeddings -> 未预期的 2xx（应 404）")
        ok4 = False
    except urllib.error.HTTPError as e:
        print("   /v1/embeddings -> %d %s" % (e.code, "OK" if e.code == 404 else "异常"))
        ok4 = e.code == 404

    qstatus, qj, _ = post_json("/v1/responses?x=1", body)
    qok = qstatus == 200 and qj.get("object") == "response"
    print("   /v1/responses?x=1 -> %d %s" % (qstatus, "OK（query 被正确忽略）" if qok else "异常"))
    ok4 = ok4 and qok

    print("\n结论: %s" % ("ALL PASS" if (ok1 and ok2 and ok3 and ok4) else "FAILED"))
    return 0 if (ok1 and ok2 and ok3 and ok4) else 1


if __name__ == "__main__":
    sys.exit(main())
