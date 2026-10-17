#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""实时验证：质量优先 + 并发分片 同时成立。

数据源说明：上游回包里的 `model` 字段是**上游自己的名字**（例如池子里的
`wb-glm-5.3-flash` 回包写成 `glm-5.3-flash`），不能用来反查池成员。
真实路由结果只信任 rotator 自己的 `routed sid=... model=... acct=...` 日志行。

期望行为：
1. 顺序请求（无并发）粘在全局最高分模型上；
2. 并发 N 路（全默认 session）落到 N 个不同账号，而不是压一个账号；
3. 同一账号内挑质量最高的可用成员。
"""
import json
import os
import re
import sys
import threading
import time
import urllib.request

BASE = "http://127.0.0.1:3166"
PROXY = BASE + "/v1/chat/completions"
LOG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rotator.log")
N = 4
ROUTED = re.compile(r"routed sid=\S+ model=(\S+) acct=(\S+) inflight=(\d+)")


_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get_status():
    with _OPENER.open(BASE + "/status", timeout=10) as r:
        return json.loads(r.read().decode())


def log_size():
    try:
        return os.path.getsize(LOG)
    except OSError:
        return 0


def new_routes(offset):
    """读出 offset 之后新增的 routed 行。"""
    try:
        with open(LOG, "r", encoding="utf-8", errors="replace") as f:
            f.seek(offset)
            chunk = f.read()
    except OSError:
        return []
    out = []
    for line in chunk.splitlines():
        m = ROUTED.search(line)
        if m:
            out.append((m.group(1), m.group(2), int(m.group(3))))
    return out


def one_request():
    body = {
        "model": "AutoRotate-30m",
        "messages": [{"role": "user", "content": "答复一个字：好"}],
        "max_tokens": 16,
        "stream": False,
    }
    req = urllib.request.Request(PROXY, data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer rotator-local")
    with _OPENER.open(req, timeout=180) as r:
        return json.loads(r.read().decode())


def seq_once():
    before = log_size()
    try:
        one_request()
        ok = "OK"
    except Exception as e:
        ok = "ERR %s" % str(e)[:60]
    # 日志落盘有几十毫秒延迟
    time.sleep(0.3)
    hits = new_routes(before)
    return hits[0] if hits else (None, None, 0), ok


def main():
    st = get_status()
    ranked = sorted(st["models"], key=lambda m: -m["quality"])
    best = ranked[0]
    print("=== 池内质量分 Top8 ===")
    for m in ranked[:8]:
        print("  %6.1f  %-32s acct=%s" % (m["quality"], m["id"], m["account"]))
    print("  ...")
    print("  最低: %6.1f  %s" % (ranked[-1]["quality"], ranked[-1]["id"]))
    print("  池大小=%d  账号数=%d" % (st["pool_size"], len(st["accounts"])))

    # --- 1) 顺序请求应粘在全局最高分模型 ---
    print("\n=== 顺序 3 次（期望 -> %s / acct=%s）===" % (best["id"], best["account"]))
    seq_ok = True
    for i in range(3):
        (mid, acct, _), ok = seq_once()
        hit = mid == best["id"]
        seq_ok = seq_ok and hit
        print("  #%d -> %-30s acct=%-8s %s" % (i, mid, acct, ok))
    print("  -> %s" % ("PASS" if seq_ok else "FAIL"))

    # --- 2) 并发 N 路应分散到 N 个账号 ---
    print("\n=== 并发 %d 路（期望 %d 个不同账号）===" % (N, N))
    before = log_size()
    errs = []
    hold = threading.Event()

    def worker():
        try:
            one_request()
        except Exception as e:
            errs.append(str(e)[:60])
        finally:
            hold.wait(30)

    ts = [threading.Thread(target=worker) for _ in range(N)]
    for t in ts:
        t.start()
    time.sleep(2.5)
    # 抓并发中的实时 inflight
    mid_st = get_status()
    held = [(m["id"], m["inflight"]) for m in mid_st["models"] if m["inflight"] > 0]
    print("  并发中 inflight: %s" % held)
    hold.set()
    for t in ts:
        t.join()
    time.sleep(0.4)

    routes = new_routes(before)
    for i, (mid, acct, infl) in enumerate(routes):
        q = next((m["quality"] for m in st["models"] if m["id"] == mid), None)
        print("  #%d -> %-30s q=%-5s acct=%-8s inflight=%d" % (i, mid, q, acct, infl))
    if errs:
        print("  errors: %s" % errs)

    got_accts = [a for _, a, _ in routes]
    got_models = [m for m, _, _ in routes]
    ok_spread = len(routes) == N and len(set(got_accts)) == N
    ok_distinct_model = len(set(got_models)) == N
    print("\n  路由数 %d/%d  不同账号 %d/%d  不同模型 %d/%d"
          % (len(routes), N, len(set(got_accts)), N, len(set(got_models)), N))
    print("  账号分散 -> %s" % ("PASS" if ok_spread else "FAIL"))
    print("  模型分散 -> %s" % ("PASS" if ok_distinct_model else "FAIL"))
    print("\n结论: %s" % ("ALL PASS" if (seq_ok and ok_spread) else "FAILED"))
    return 0 if (seq_ok and ok_spread) else 1


if __name__ == "__main__":
    sys.exit(main())
