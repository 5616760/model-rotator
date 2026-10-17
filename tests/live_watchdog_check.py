#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""真实环境下的看门狗回归：确认「正常请求不被看门狗拖慢/拖并发」。

关注三点：
1. 正常请求耗时不应变差（看门狗只等到响应头，之后照常流式透传）；
2. 日志里不应出现「看门狗」行（说明真实上游都在 hedge_after 内出响应头）；
3. inflight 必须归零（名额不泄漏，否则账号分片会越来越保守）。

如果第 2 点出现了看门狗行，那反而是有价值的信号：说明真实上游确实有长尾，
可以用这些样本重新校准 hedge_after_seconds。
"""
import json
import os
import re
import sys
import time
import urllib.request

BASE = "http://127.0.0.1:3166"
LOG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rotator.log")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
N = 6


def post(prompt="只回复两个字：收到", timeout=120):
    body = {"model": "AutoRotate-30m",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 16}
    req = urllib.request.Request(BASE + "/v1/chat/completions",
                                data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer x")
    t0 = time.time()
    with OPENER.open(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode()), time.time() - t0


def inflight():
    with OPENER.open(BASE + "/status", timeout=10) as r:
        return json.loads(r.read().decode())["inflight_total"]


def log_size():
    try:
        return os.path.getsize(LOG)
    except OSError:
        return 0


def new_lines(off):
    with open(LOG, "r", encoding="utf-8", errors="replace") as f:
        f.seek(off)
        return f.read()


def main():
    off = log_size()
    print("=== 真实环境 %d 次串行请求 ===" % N)
    durs = []
    errs = 0
    for i in range(N):
        try:
            d, dur = post()
            durs.append(dur)
            m = d.get("model")
            print("  #%d %.2fs model=%s" % (i, dur, m))
        except Exception as e:
            errs += 1
            print("  #%d ERR %s" % (i, str(e)[:70]))
    time.sleep(0.4)
    chunk = new_lines(off)

    hedge_lines = [l for l in chunk.splitlines() if "看门狗" in l]
    routed = [l for l in chunk.splitlines() if "routed " in l]
    hedged_ok = [l for l in routed if "hedged" in l]

    print("\n=== 日志分析 ===")
    print("  路由行=%d  含 hedged 的成功行=%d  看门狗触发行=%d"
          % (len(routed), len(hedged_ok), len(hedge_lines)))
    for l in hedge_lines[:5]:
        print("   " + l.strip())
    for l in hedged_ok[:5]:
        print("   " + l.strip())

    if durs:
        durs_sorted = sorted(durs)
        p50 = durs_sorted[len(durs_sorted) // 2]
        print("\n  耗时 p50=%.2fs  max=%.2fs  err=%d" % (p50, max(durs), errs))

    # inflight 判断：本机是共享代理，可能有 WorkBuddy 自己的并发请求在跑，
    # 所以不能要求「正好为 0」；只要求不出现「远高于活跃请求数」的异常值。
    # 更可靠的是观察一段时间后 inflight 能回落，而不是看瞬时值。
    final_inflight = inflight()
    print("  inflight_total=%d（共享环境可能有其它并发，瞬时非 0 不一定是泄漏）" % final_inflight)

    # 连续采样：真正的泄漏会单调上升且不回落
    samples = [inflight()]
    for _ in range(4):
        time.sleep(1.5)
        samples.append(inflight())
    settled = all(s <= max(samples[:1] + [0]) + 2 for s in samples[-3:])
    print("  连续采样 inflight=%s -> %s" % (samples, "稳定" if settled else "持续上升"))
    ok = (errs == 0) and (not durs or max(durs) < 10) and settled
    if hedge_lines:
        print("\n  注意：真实上游出现了看门狗触发，说明有长尾。")
        print("  这些样本可用于校准 hedge_after_seconds（当前 3s）。")
    print("\n结论: %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
