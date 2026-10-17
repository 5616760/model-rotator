#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""长输出场景下的对冲收益测量。

为什么需要单独测：短请求（max_tokens=16）测到的几乎全是「建连 + 首 token」延迟，
对冲只会增加并发开销。真实任务（写代码、写文档）是长输出，延迟由 token 生成
速度决定，模型间的差距会被放大，这时对冲的价值才看得出来。
"""
import json
import os
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import rotator  # noqa: E402

rotator.CFG = rotator.build_config()
MEMBERS = {m["id"]: m for m in rotator.CFG["_pool"]}

CAND = [
    ("DeepSeek-Flash", 94.3),
    ("Doubao-Seed-2.1-Pro-solo", 91.9),
    ("sn-sensenova-6-8-flash", 88.2),
    ("Qwen3.7-Plus", 91.2),
]

# 一个需要真实推理 + 长输出的任务，接近「让模型写一段可用代码」
TASK = (
    "用 Python 写一个带过期时间的 LRU 缓存类，要求：\n"
    "1) 支持 capacity 和 ttl 两个参数；\n"
    "2) get/put 都是 O(1)；\n"
    "3) 过期条目在 get 时惰性删除，并有一个 purge 方法主动清理；\n"
    "4) 线程安全。\n"
    "只输出代码，不要解释。"
)

ROUNDS = 4
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def call(model_id, timeout=180):
    body = {
        "model": model_id,
        "messages": [{"role": "user", "content": TASK}],
        "max_tokens": 1200,
        "stream": False,
    }
    m = MEMBERS[model_id]
    req = urllib.request.Request(m["url"], data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer " + m["key"])
    t0 = time.time()
    try:
        with _OPENER.open(req, timeout=timeout) as r:
            raw = r.read().decode()
        d = json.loads(raw)
        dur = time.time() - t0
        usage = d.get("usage") or {}
        out_tok = usage.get("completion_tokens") or 0
        # 推理模型把答案放 reasoning_content，也算有效产出
        msg = (d.get("choices") or [{}])[0].get("message") or {}
        text = msg.get("content") or msg.get("reasoning_content") or ""
        return dur, out_tok, len(text), None
    except Exception as e:
        return time.time() - t0, 0, 0, str(e)[:70]


def main():
    print("=== 长输出基准（同一任务，max_tokens=1200，每模型 %d 次）===" % ROUNDS)
    print("任务: 写一个带 TTL 的线程安全 LRU 缓存\n")
    stats = {}
    for mid, q in CAND:
        rows = []
        for _ in range(ROUNDS):
            dur, tok, chars, err = call(mid)
            if err:
                print("  %-32s ERR %s" % (mid, err))
                continue
            rows.append((dur, tok, chars))
        if not rows:
            continue
        dur = sum(r[0] for r in rows) / len(rows)
        tok = sum(r[1] for r in rows) / len(rows)
        chars = sum(r[2] for r in rows) / len(rows)
        tps = tok / dur if dur else 0
        stats[mid] = (dur, tok, chars, tps, q)
        print("  %-30s q=%.1f  avg=%5.1fs  out_tok=%4.0f  chars=%5.0f  %.1f tok/s"
              % (mid, q, dur, tok, chars, tps))

    if not stats:
        print("全部失败")
        return 1

    fastest = min(stats.items(), key=lambda kv: kv[1][0])
    print("\n  最快: %s (%.1fs)   质量最高: %s"
          % (fastest[0], fastest[1][0],
             max(stats.items(), key=lambda kv: kv[1][4])[0]))

    # 串行两个模型 vs 对冲
    print("\n=== 若要「既要质量又要速度」：串行 vs 对冲 ===")
    seq_total = stats[CAND[0][0]][0] + stats[CAND[1][0]][0]
    print("  串行 A 失败再跑 B : 最坏 %.1fs（A 超时/限流后才换 B）"
          % seq_total)

    hedge_times = []
    for _ in range(ROUNDS):
        res = {}

        def one(mid):
            d, tok, ch, err = call(mid)
            res[mid] = (d, err)

        ts = [threading.Thread(target=one, args=(m,)) for m, _ in CAND[:2]]
        t0 = time.time()
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        wall = time.time() - t0
        oks = [(v[0], k) for k, v in res.items() if not v[1]]
        if oks:
            hedge_times.append((wall, min(oks)[1]))
    if hedge_times:
        avg_wall = sum(t[0] for t in hedge_times) / len(hedge_times)
        wins = {}
        for _, w in hedge_times:
            wins[w] = wins.get(w, 0) + 1
        print("  双路对冲         : 实际墙钟 %.1fs（要等两路都结束才能收尾，胜者: %s）"
              % (avg_wall, wins))
        print("                     真实可用延迟是「先到那路」= %.1fs"
              % min(stats[CAND[0][0]][0], stats[CAND[1][0]][0]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
