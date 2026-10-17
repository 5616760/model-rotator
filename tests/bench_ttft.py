#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测首 token 延迟（TTFT）的尾部形态，判断「延迟对冲」是否值得做。

决策依据：
- 若 TTFT 稳定（p90 接近 p50），对冲永远不会被触发 -> 纯死代码，不做；
- 若 TTFT 有长尾（p90 远大于 p50，偶发卡十几秒），「等 N 秒没首 token 就
  并发第二路」才真正有效，且只在长尾时付费。

必须用 stream=True 才能测到 TTFT：非流式只能拿到总耗时，无法区分「慢」和
「卡住不动」。
"""
import json
import os
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import rotator  # noqa: E402

rotator.CFG = rotator.build_config()
MEMBERS = {m["id"]: m for m in rotator.CFG["_pool"]}

TARGETS = ["DeepSeek-Flash", "Qwen3.7-Plus", "sn-sensenova-6-8-flash"]
ROUNDS = 8
TASK = ("用 Python 实现一个线程安全的 LRU 缓存，支持 TTL 过期，"
        "get/put O(1)，含惰性删除与 purge 方法。只输出代码。")
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def stream_call(mid, timeout=180):
    """返回 (ttft, total, chunks, err)。ttft 是首个 SSE data 块的耗时。"""
    m = MEMBERS[mid]
    body = {
        "model": mid,
        "messages": [{"role": "user", "content": TASK}],
        "max_tokens": 800,
        "stream": True,
    }
    req = urllib.request.Request(m["url"], data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer " + m["key"])
    req.add_header("Accept", "text/event-stream")
    t0 = time.time()
    ttft = None
    chunks = 0
    try:
        with _OPENER.open(req, timeout=timeout) as r:
            for line in r:
                s = line.strip()
                if not s or not s.startswith(b"data:"):
                    continue
                payload = s[5:].strip()
                if payload == b"[DONE]":
                    break
                if ttft is None:
                    ttft = time.time() - t0
                chunks += 1
        return ttft, time.time() - t0, chunks, None
    except Exception as e:
        return ttft, time.time() - t0, chunks, str(e)[:70]


def pct(xs, p):
    if not xs:
        return 0.0
    s = sorted(xs)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def main():
    print("=== 首 token 延迟（TTFT）分布，每模型 %d 次流式 ===" % ROUNDS)
    print("判断标准: p90/p50 比值 < 1.5 说明尾不肥，对冲无意义\n")
    summary = {}
    for mid in TARGETS:
        ttfts, totals = [], []
        errs = 0
        for _ in range(ROUNDS):
            ttft, total, ch, err = stream_call(mid)
            if err:
                errs += 1
                continue
            if ttft:
                ttfts.append(ttft)
            totals.append(total)
        if not ttfts:
            print("  %-30s 无有效样本 err=%d" % (mid, errs))
            continue
        ratio = pct(ttfts, .9) / pct(ttfts, .5) if pct(ttfts, .5) else 0
        summary[mid] = (pct(ttfts, .5), pct(ttfts, .9), max(ttfts), ratio)
        print("  %-30s TTFT p50=%5.2fs p90=%5.2fs max=%5.2fs  p90/p50=%.2f  "
              "总耗时p50=%5.1fs err=%d"
              % (mid, pct(ttfts, .5), pct(ttfts, .9), max(ttfts), ratio, pct(totals, .5), errs))

    print("\n=== 结论 ===")
    fat = [(m, v) for m, v in summary.items() if v[3] >= 1.5]
    if not fat:
        print("  所有模型 TTFT 尾都不肥（p90/p50 < 1.5）")
        print("  -> 延迟对冲不会带来收益，只会翻倍烧额度")
        print("  -> 建议：不做同对话多模型并发")
    else:
        for m, v in fat:
            print("  %s 尾较肥: p50=%.2fs -> p90=%.2fs（%.1fx）" % (m, v[0], v[1], v[3]))
        print("  -> 可考虑「TTFT 超过 p90 才触发第二路」的条件对冲")
    return 0


if __name__ == "__main__":
    sys.exit(main())
