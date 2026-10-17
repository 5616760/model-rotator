#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""实时并发验证：同时发 N 个请求（全部默认 session），确认落到不同账号。"""
import json
import sys
import threading
import time
import urllib.request

PROXY = "http://127.0.0.1:3166/v1/chat/completions"
N = 3

# 必须绕过环境里的 HTTP(S)_PROXY：否则对 127.0.0.1 的请求也会被外部代理接管
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def run(k, out):
    body = {
        "model": "AutoRotate-30m",
        "messages": [{"role": "user", "content": "只回复两个字：收到"}],
        "max_tokens": 16,
        "stream": False,
    }
    req = urllib.request.Request(PROXY, data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer rotator-local")
    t0 = time.time()
    try:
        with _OPENER.open(req, timeout=180) as r:
            data = json.loads(r.read().decode())
        out[k] = (data.get("model"), round(time.time() - t0, 1), "OK")
    except Exception as e:
        out[k] = (None, round(time.time() - t0, 1), "ERR %s" % str(e)[:80])


def main():
    out = {}
    ts = [threading.Thread(target=run, args=(k, out)) for k in range(N)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    print("=== 并发结果（请求 -> 上游模型 / 耗时 / 状态）===")
    for k in range(N):
        print("  #%d -> %s" % (k, out.get(k)))
    models = [v[0] for v in out.values() if v[0]]
    print("distinct models: %d / %d" % (len(set(models)), len(models)))
    return 0 if len(set(models)) == len(models) else 1


if __name__ == "__main__":
    sys.exit(main())
