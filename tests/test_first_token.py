#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证「首 token 超时判死」：响应头到手但首 token 迟迟不来 → 放弃当前路，
走现有 fallback 链换下一路，而不是把卡死的流原样吐给客户端。

关键不变量：
  1. 首 token 正常 → 只 1 次上游请求，客户端拿到完整流；
  2. 首 token 超时 → 首选被判死冷却，fallback 路被真正派发，客户端拿到第二路结果；
  3. 所有路径结束时 inflight=0（名额不泄漏）。
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rotator
from mock_upstream import MockUpstream

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

FAST = "fast-model"
SLOW = "slow-model"
FAIL = "fail-model"

# 用小阈值让测试秒级跑完：0.5s 内没首 token 就判死
FT_TO = 0.5


def start_rotator(base_url, hedge_after=1.0, hedge_max=2, first_token_to=FT_TO):
    pool = [
        {"id": FAST, "url": base_url + "/chat/completions", "key": "k",
         "account": "acct-fast", "quality": 99.0, "meta": {}},
        {"id": SLOW, "url": base_url + "/chat/completions", "key": "k",
         "account": "acct-slow", "quality": 90.0, "meta": {}},
        {"id": FAIL, "url": base_url + "/chat/completions", "key": "k",
         "account": "acct-fail", "quality": 80.0, "meta": {}},
    ]
    rotator.CFG = {
        "listen_host": "127.0.0.1",
        "listen_port": 0,
        "rotate_minutes": 30,
        "cooldown_minutes": 10,
        "fail_status": [408, 409, 425, 429, 500, 502, 503, 504],
        "fail_on_exception": True,
        "rotate_on_first_request": True,
        "hedge_after_seconds": hedge_after,
        "hedge_max": hedge_max,
        "first_token_timeout_seconds": first_token_to,
        "quality_priority": True,
        "expose_as": "AutoRotate-Test",
        "log_file": None,
        "_pool": pool,
    }
    rotator.ST = rotator.State(
        rotator.CFG["_pool"], rotate_s=1800, cooldown_s=600, rotate_on_first=True)

    class Quiet(rotator.Handler):
        def log_message(self, fmt, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Quiet)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, port


def post_stream(port, timeout=30):
    body = {"model": "AutoRotate-Test", "stream": True,
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 8}
    req = urllib.request.Request("http://127.0.0.1:%d/v1/chat/completions" % port,
                                data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer x")
    t0 = time.time()
    with OPENER.open(req, timeout=timeout) as r:
        raw = r.read().decode()
    return raw, time.time() - t0


def inflight(port):
    with OPENER.open("http://127.0.0.1:%d/status" % port, timeout=10) as r:
        j = json.loads(r.read().decode())
    return j["inflight_total"]


def main():
    failures = []

    # ---------- 场景 1：首 token 正常 → 只 1 次请求，客户端拿到完整流 ----------
    up = MockUpstream({FAST: {"stream": True, "ttft": 0.0},
                       SLOW: {"stream": True, "ttft": 0.0},
                       FAIL: {"status": 200}}).start()
    httpd, port = start_rotator(up.base, hedge_after=1.0, hedge_max=2)
    try:
        up.reset_hits()
        raw, dur = post_stream(port)
        hits = {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}
        print("场景1 首 token 正常（无判死）")
        print("   耗时=%.2fs 收到字节=%d 含 DONE=%s 各模型次数=%s"
              % (dur, len(raw), "[DONE]" in raw, hits))
        if up.hits_of(SLOW) or up.hits_of(FAIL):
            failures.append("场景1: 多余派发（%s）" % hits)
        elif "[DONE]" not in raw:
            failures.append("场景1: 流未完整（DONE 缺失）")
        else:
            print("   PASS（1 次请求，流完整）")
        if inflight(port) != 0:
            failures.append("场景1: inflight 未归零 = %d" % inflight(port))
    finally:
        httpd.shutdown()
        up.stop()

    # ---------- 场景 2：首选首 token 沉默 → 判死并走 fallback ----------
    # FAST 响应头立即、但首 token 拖 3s（> FT_TO）；SLOW 快。
    # 首选落在 FAST（质量高）。0.5s 判死后应派发 SLOW 并返回它的流。
    up = MockUpstream({FAST: {"stream": True, "ttft": 3.0},
                       SLOW: {"stream": True, "ttft": 0.0},
                       FAIL: {"status": 200}}).start()
    httpd, port = start_rotator(up.base, hedge_after=1.0, hedge_max=2)
    try:
        up.reset_hits()
        raw, dur = post_stream(port, timeout=30)
        hits = {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}
        print("场景2 首 token 沉默 → 判死 fallback")
        print("   耗时=%.2fs 含 DONE=%s 各模型次数=%s"
              % (dur, "[DONE]" in raw, hits))
        # FAST 被真正请求过一次（判死前确实建连了），SLOW 被作为 fallback 派发
        if up.hits_of(SLOW) == 0:
            failures.append("场景2: fallback 路未派发（%s）" % hits)
        elif "[DONE]" not in raw:
            failures.append("场景2: 客户端未收到完整流")
        elif dur >= 2.0:
            failures.append("场景2: 判死耗时异常 %.2fs（应约 %.1fs 后换路）" % (dur, FT_TO))
        else:
            print("   PASS（%.1fs 判死，fallback 路接管，客户端拿到完整流）" % FT_TO)
        if inflight(port) != 0:
            failures.append("场景2: inflight 未归零 = %d" % inflight(port))
    finally:
        httpd.shutdown()
        up.stop()

    # ---------- 场景 3：看门狗关闭 hedge + 判死开启（独立验证） ----------
    up = MockUpstream({FAST: {"stream": True, "ttft": 3.0},
                       SLOW: {"stream": True, "ttft": 0.0},
                       FAIL: {"status": 200}}).start()
    httpd, port = start_rotator(up.base, hedge_after=0, hedge_max=1)
    try:
        up.reset_hits()
        raw, dur = post_stream(port, timeout=30)
        hits = {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}
        print("场景3 hedge=0 + 判死开启")
        print("   耗时=%.2fs 含 DONE=%s 各模型次数=%s" % (dur, "[DONE]" in raw, hits))
        if up.hits_of(SLOW) == 0:
            failures.append("场景3: fallback 路未派发（%s）" % hits)
        elif "[DONE]" not in raw:
            failures.append("场景3: 客户端未收到完整流")
        else:
            print("   PASS（不并发，判死后走 fallback）")
        if inflight(port) != 0:
            failures.append("场景3: inflight 未归零 = %d" % inflight(port))
    finally:
        httpd.shutdown()
        up.stop()

    print("=" * 60)
    if failures:
        print("FAILURES:")
        for f in failures:
            print("  - " + f)
        sys.exit(1)
    print("ALL FIRST-TOKEN TESTS PASSED")


if __name__ == "__main__":
    main()