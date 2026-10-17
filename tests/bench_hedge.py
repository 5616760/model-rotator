#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""量化「同对话多模型并发」到底能省多少。

两种玩法分别测：
  A) 单模型串行      —— 现状基线
  B) 双模型对冲      —— 同时打到两个不同账号，取先返回的（hedged request）
  C) 三模型对冲      —— 同上，三路

为什么必须实测：对冲只在「延迟尾部很肥」时才有收益。如果所有模型都在 2s 内
回包，对冲等于白烧 2 倍额度；如果 P90 是 P50 的 3 倍，对冲收益明显。

注意：本脚本直接打上游（127.0.0.1:3065），绕过 rotator，这样能精确指定
模型对照，不受轮换策略干扰。
"""
import json
import os
import statistics as st
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import rotator  # noqa: E402

# 直接复用代理自己的配置解析：池成员的 url/key 有一部分是 key_from 共享来的，
# 手写查找会漏（例如 DeepSeek-Flash 本身不在 models.json 里）。
rotator.CFG = rotator.build_config()
MEMBERS = {m["id"]: m for m in rotator.CFG["_pool"]}

# 取三个不同账号的质量最高成员，模拟真实对冲场景
CAND = [
    ("DeepSeek-Flash", "qoder", 94.3),
    ("Doubao-Seed-2.1-Pro-solo", "trae", 91.9),
    ("sn-sensenova-6-8-flash", "raccoon", 88.2),
]

ROUNDS = 8
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def key_for(model_id):
    """取该池成员的 key（走 rotator 的解析结果，含 key_from 共享逻辑）。"""
    return MEMBERS[model_id]["key"]


def call(model_id, prompt="只回复两个字：收到", timeout=120):
    body = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 16,
        "stream": False,
    }
    req = urllib.request.Request(MEMBERS[model_id]["url"],
                                 data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer " + key_for(model_id))
    t0 = time.time()
    try:
        with _OPENER.open(req, timeout=timeout) as r:
            raw = r.read().decode()
        json.loads(raw)
        return time.time() - t0, None
    except Exception as e:
        return time.time() - t0, str(e)[:70]


def race(models):
    """同时发 N 路，返回「最先成功那一路」的耗时 + 是哪一路赢的。"""
    res = {}

    def one(i, mid):
        dur, err = call(mid)
        res[i] = (dur, err)

    ts = [threading.Thread(target=one, args=(i, m)) for i, m in enumerate(models)]
    t0 = time.time()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    wall = time.time() - t0
    oks = [(d, i) for i, (d, e) in res.items() if e is None]
    if not oks:
        return wall, None, [e for _, e in res.values()]
    d, i = min(oks)
    return d, models[i], None


def pct(xs, p):
    if not xs:
        return 0.0
    s = sorted(xs)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def main():
    print("=== 目标模型 ===")
    for mid, acct, q in CAND:
        print("  %-32s acct=%-8s q=%.1f" % (mid, acct, q))

    # A) 单模型串行：分别测三个模型各自的分布
    print("\n=== A) 单模型串行（每模型 %d 次）===" % ROUNDS)
    single = {}
    for mid, _, _ in CAND:
        ds = []
        errs = 0
        for _ in range(ROUNDS):
            d, e = call(mid)
            if e:
                errs += 1
            else:
                ds.append(d)
        single[mid] = ds
        if ds:
            print("  %-32s p50=%.2fs p90=%.2fs max=%.2fs err=%d"
                  % (mid, pct(ds, .5), pct(ds, .9), max(ds), errs))
        else:
            print("  %-32s 全部失败 err=%d" % (mid, errs))

    base = single[CAND[0][0]]
    if not base:
        print("基线模型全部失败，无法比较")
        return 1

    # B) 双模型对冲
    print("\n=== B) 双模型对冲（2 路并发取先到，%d 次）===" % ROUNDS)
    d2 = []
    wins = {}
    for _ in range(ROUNDS):
        dur, win, errs = race([m for m, _, _ in CAND[:2]])
        if win:
            d2.append(dur)
            wins[win] = wins.get(win, 0) + 1
    if d2:
        print("  p50=%.2fs p90=%.2fs max=%.2fs  胜者: %s"
              % (pct(d2, .5), pct(d2, .9), max(d2), wins))

    # C) 三模型对冲
    print("\n=== C) 三模型对冲（3 路并发取先到，%d 次）===" % ROUNDS)
    d3 = []
    wins3 = {}
    for _ in range(ROUNDS):
        dur, win, errs = race([m for m, _, _ in CAND])
        if win:
            d3.append(dur)
            wins3[win] = wins3.get(win, 0) + 1
    if d3:
        print("  p50=%.2fs p90=%.2fs max=%.2fs  胜者: %s"
              % (pct(d3, .5), pct(d3, .9), max(d3), wins3))

    # 结论
    print("\n=== 收益对比（相对单模型 %s）===" % CAND[0][0])
    b50, b90 = pct(base, .5), pct(base, .9)
    rows = [("单模型", base, 1.0), ("双路对冲", d2, 2.0), ("三路对冲", d3, 3.0)]
    for name, ds, mult in rows:
        if not ds:
            continue
        m50, m90 = pct(ds, .5), pct(ds, .9)
        print("  %-10s p50=%5.2fs (%+5.1f%%)  p90=%5.2fs (%+5.1f%%)  额度消耗 x%.0f"
              % (name, m50, (m50 / b50 - 1) * 100, m90, (m90 / b90 - 1) * 100, mult))
    return 0


if __name__ == "__main__":
    sys.exit(main())
