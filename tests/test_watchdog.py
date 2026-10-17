#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""看门狗（条件对冲）行为验证。

判定标准只有三条，全部用「实际发出的请求次数」和「墙钟时间」说话：
1. 快模型（< hedge_after）不触发对冲 —— 上游只收到 1 次请求，零额外成本；
2. 慢模型（> hedge_after）触发对冲 —— 第二路被真正派出，且响应来自更快的那个；
3. 名额不泄漏 —— 每轮结束后 /status 的 inflight_total 必须回 0。

为什么用真实 HTTP 而不是 mock 函数：看门狗的核心是把 open() 放旁路线程，
「等多久」和「并发是否真发出去了」只有走真实 socket 才能验证。
"""
import json
import os
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rotator  # noqa: E402
from mock_upstream import MockUpstream  # noqa: E402
from http.server import ThreadingHTTPServer  # noqa: E402

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

FAST = "fast-model"
SLOW = "slow-model"
FAIL = "fail-model"


def start_rotator(base_url, hedge_after=1.0, hedge_max=2, pool_filter=None):
    """在进程内起一个 rotator 实例，池子指向 mock 上游。"""
    pool = [
        {"id": FAST, "url": base_url + "/chat/completions", "key": "k",
         "account": "acct-fast", "quality": 99.0, "meta": {}},
        {"id": SLOW, "url": base_url + "/chat/completions", "key": "k",
         "account": "acct-slow", "quality": 90.0, "meta": {}},
        {"id": FAIL, "url": base_url + "/chat/completions", "key": "k",
         "account": "acct-fail", "quality": 80.0, "meta": {}},
    ]
    if pool_filter:
        pool = [p for p in pool if pool_filter(p["id"])]
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


def post(port, timeout=60):
    body = {"model": "AutoRotate-Test",
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 8}
    req = urllib.request.Request("http://127.0.0.1:%d/v1/chat/completions" % port,
                                data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer x")
    t0 = time.time()
    with OPENER.open(req, timeout=timeout) as r:
        data = json.loads(r.read().decode())
    return data, time.time() - t0


def inflight(port):
    with OPENER.open("http://127.0.0.1:%d/status" % port, timeout=10) as r:
        j = json.loads(r.read().decode())
    return j["inflight_total"]


def main():
    failures = []

    # ---------- 场景 1：快模型不应触发对冲 ----------
    up = MockUpstream({FAST: {"delay": 0.05},
                       SLOW: {"delay": 5.0},
                       FAIL: {"status": 200}}).start()
    httpd, port = start_rotator(up.base, hedge_after=1.0, hedge_max=2)
    try:
        up.reset_hits()
        data, dur = post(port)
        got = data["choices"][0]["message"]["content"]
        hits = {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}
        print("场景1 快模型不触发对冲")
        print("   耗时=%.2fs 返回=%s 各模型请求次数=%s" % (dur, got, hits))
        if up.hits_of(SLOW) != 0 or up.hits_of(FAIL) != 0:
            failures.append("场景1: 快路径却派出了对冲路（%s）" % hits)
        elif dur >= 1.0:
            failures.append("场景1: 触发时间异常 %.2fs" % dur)
        else:
            print("   PASS（只有 1 次上游请求，零额外成本）")
        if inflight(port) != 0:
            failures.append("场景1: inflight 未归零 = %d" % inflight(port))
    finally:
        httpd.shutdown()
        up.stop()

    # ---------- 场景 2：慢模型应触发对冲，取先到的快路 ----------
    # 让 FAST 慢(3s)、SLOW 快(0.1s)：首选会落在 FAST（质量高），
    # 1s 后看门狗应补一路 SLOW 并立刻返回它。
    up = MockUpstream({FAST: {"delay": 3.0},
                       SLOW: {"delay": 0.1},
                       FAIL: {"status": 200}}).start()
    httpd, port = start_rotator(up.base, hedge_after=1.0, hedge_max=2)
    try:
        # 先把会话钉在 FAST 上：首发请求会走质量最高的 FAST
        up.reset_hits()
        t0 = time.time()
        data, dur = post(port)
        got = data["choices"][0]["message"]["content"]
        hits = {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}
        print("\n场景2 慢模型触发对冲（FAST=3s, SLOW=0.1s）")
        print("   耗时=%.2fs 返回=%s 各模型请求次数=%s" % (dur, got, hits))
        if up.hits_of(SLOW) == 0:
            failures.append("场景2: 看门狗未派出第二路（%s）" % hits)
        if got != "mock-ok-%s" % SLOW:
            failures.append("场景2: 应返回快的 SLOW，实际 %s" % got)
        if dur > 2.0:
            failures.append("场景2: 对冲没起作用，耗时 %.2fs" % dur)
        else:
            print("   PASS（%.2fs 拿到 SLOW 的响应，而非干等 FAST 的 3s）" % dur)
        time.sleep(0.4)
        if inflight(port) != 0:
            failures.append("场景2: inflight 未归零 = %d" % inflight(port))
        else:
            print("   名额泄漏检查: inflight=0 PASS")
    finally:
        httpd.shutdown()
        up.stop()

    # ---------- 场景 3：首选 429，对冲路成功 ----------
    up = MockUpstream({FAST: {"delay": 2.0, "status": 429},
                       SLOW: {"delay": 0.1},
                       FAIL: {"delay": 3.0, "status": 500}}).start()
    httpd, port = start_rotator(up.base, hedge_after=0.8, hedge_max=2)
    try:
        up.reset_hits()
        data, dur = post(port)
        got = data["choices"][0]["message"]["content"]
        hits = {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}
        print("\n场景3 首选 429、对冲路成功")
        print("   耗时=%.2fs 返回=%s 各模型请求次数=%s" % (dur, got, hits))
        if got != "mock-ok-%s" % SLOW:
            failures.append("场景3: 应返回 SLOW，实际 %s" % got)
        else:
            print("   PASS")
        time.sleep(0.4)
        if inflight(port) != 0:
            failures.append("场景3: inflight 未归零 = %d" % inflight(port))
    finally:
        httpd.shutdown()
        up.stop()

    # ---------- 场景 4：对冲全失败 -> 走失败转移并返回 429 ----------
    up = MockUpstream({FAST: {"delay": 1.5, "status": 500},
                       SLOW: {"delay": 1.5, "status": 500},
                       FAIL: {"delay": 0.1, "status": 429}}).start()
    httpd, port = start_rotator(up.base, hedge_after=0.5, hedge_max=2)
    try:
        up.reset_hits()
        code = None
        try:
            post(port)
        except urllib.error.HTTPError as e:
            code = e.code
        print("\n场景4 全部失败")
        print("   返回码=%s 各模型请求次数=%s"
              % (code, {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}))
        if code not in (429, 500, 502, 503):
            failures.append("场景4: 返回码异常 %s" % code)
        else:
            print("   PASS（错误被正确归类返回）")
        time.sleep(0.4)
        if inflight(port) != 0:
            failures.append("场景4: inflight 未归零 = %d" % inflight(port))
    finally:
        httpd.shutdown()
        up.stop()

    # ---------- 场景 5：看门狗关闭时行为不变 ----------
    up = MockUpstream({FAST: {"delay": 2.0},
                       SLOW: {"delay": 0.1},
                       FAIL: {"status": 200}}).start()
    httpd, port = start_rotator(up.base, hedge_after=0, hedge_max=0)
    try:
        up.reset_hits()
        data, dur = post(port)
        got = data["choices"][0]["message"]["content"]
        print("\n场景5 看门狗关闭（hedge_after=0）")
        print("   耗时=%.2fs 返回=%s 各模型次数=%s"
              % (dur, got, {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}))
        if up.hits_of(SLOW) != 0:
            failures.append("场景5: 关闭状态仍派出了对冲")
        if got != "mock-ok-%s" % FAST:
            failures.append("场景5: 应返回 FAST，实际 %s" % got)
        else:
            print("   PASS（不并发，原样返回 FAST）")
    finally:
        httpd.shutdown()
        up.stop()

    # ---------- 场景 6：hedge_max=1 必须等同关闭，不能空转挂死 ----------
    # 这是实现时真实踩到的缺陷：hedge_max=1 会进入对冲分支，但
    # len(racers) < hedge_max 恒假，内层循环空转，请求永久挂起。
    up = MockUpstream({FAST: {"delay": 1.5},
                       SLOW: {"delay": 0.1},
                       FAIL: {"status": 200}}).start()
    httpd, port = start_rotator(up.base, hedge_after=0.5, hedge_max=1)
    try:
        up.reset_hits()
        started = time.time()
        ok6 = True
        try:
            data, dur = post(port, timeout=15)
            got = data["choices"][0]["message"]["content"]
        except Exception as e:
            ok6 = False
            got = "ERR %s" % str(e)[:60]
            dur = time.time() - started
        print("\n场景6 hedge_max=1（应等同关闭，不空转）")
        print("   耗时=%.2fs 返回=%s 各模型次数=%s"
              % (dur, got, {m: up.hits_of(m) for m in (FAST, SLOW, FAIL)}))
        if not ok6 or dur > 5:
            failures.append("场景6: hedge_max=1 时挂死或超时（%.2fs）" % dur)
        elif up.hits_of(SLOW) != 0:
            failures.append("场景6: hedge_max=1 不应另开一路")
        else:
            print("   PASS（正常等到首选返回，未补路）")
    finally:
        httpd.shutdown()
        up.stop()

    # ---------- 场景 7：池内只有一个模型时看门狗不能空转 ----------
    # pick_alternate 无候选可补，必须退回「等首选」而不是死循环。
    up = MockUpstream({FAST: {"delay": 1.5}}).start()
    httpd, port = start_rotator(up.base, hedge_after=0.4, hedge_max=3,
                                pool_filter=lambda mid: mid == FAST)
    try:
        up.reset_hits()
        started = time.time()
        ok7 = True
        try:
            data, dur = post(port, timeout=15)
            got = data["choices"][0]["message"]["content"]
        except Exception as e:
            ok7 = False
            got = "ERR %s" % str(e)[:60]
            dur = time.time() - started
        print("\n场景7 池内仅 1 个模型（无备胎可补）")
        print("   耗时=%.2fs 返回=%s" % (dur, got))
        if not ok7 or dur > 6:
            failures.append("场景7: 无备胎时挂死或超时（%.2fs）" % dur)
        elif got != "mock-ok-%s" % FAST:
            failures.append("场景7: 返回异常 %s" % got)
        else:
            print("   PASS（退回等首选，正常返回）")
    finally:
        httpd.shutdown()
        up.stop()

    print("\n" + "=" * 60)
    if failures:
        print("FAILURES:")
        for f in failures:
            print("  - " + f)
        return 1
    print("ALL WATCHDOG TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
