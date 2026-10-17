#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 quality_eval.py 的实测结果写回 config.json。

为什么单独一个脚本：质量分来自实测（quality_result.json），不该手填；同时要把
分数按「家族」传播到该家族的**所有**入口变体（wb-/wai-/catpaw-/sn- 前缀、
-solo/-lite 后缀），否则同家族里没被测到的入口会因缺省 50 分被排到后面。

用法:
  python apply_quality.py                 # 用 quality_result.json 覆盖 quality_scores
  python apply_quality.py --dry-run       # 只打印将要写入的内容
  python apply_quality.py --smooth        # 正确率相同时按延迟再拉开一点差距
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = os.path.join(HERE, "config.json")
RESULT = os.path.join(HERE, "quality_result.json")

PREFIXES = ("wb-", "wai-", "catpaw-", "sn-")
SUFFIXES = ("-solo", "-lite")


def family(mid):
    x = mid
    for p in PREFIXES:
        if x.startswith(p):
            x = x[len(p):]
            break
    for s in SUFFIXES:
        if x.endswith(s):
            x = x[: -len(s)]
            break
    return x


def build_scores(results, smooth=False):
    """正确率 -> 0-100 分；smooth 时把同分模型按延迟再排一次。

    分数映射刻意保留区分度：
      100% -> 90 分起
      否则按 correct_pct 线性映射到 0-89
    纯失败（fail_calls 且 0 正确）-> 0 分
    """
    scores = {}
    # 先按 (正确率降序, 延迟升序) 排名，便于 smooth 时给微小偏移
    ranked = sorted(results, key=lambda r: (-r["correct_pct"], r["avg_latency"]))
    for rank, r in enumerate(ranked):
        if r.get("fail_calls") and r["correct_pct"] == 0:
            base = 0
        elif r["correct_pct"] >= 100:
            base = 90 - min(rank, 20) * 0.2   # 100% 的模型之间按延迟微调
        else:
            base = round(r["correct_pct"] * 0.89, 1)
        if smooth and r["avg_latency"]:
            # 延迟越低加分越多（最多 +5）
            base += max(0, 5 - r["avg_latency"] / 4.0)
        scores[r["family"]] = round(min(100.0, max(0.0, base)), 1)
    return scores


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--smooth", action="store_true")
    ap.add_argument("--config", default=CFG)
    ap.add_argument("--result", default=RESULT)
    args = ap.parse_args()

    if not os.path.exists(args.result):
        print("缺少 %s，先跑 quality_eval.py" % args.result)
        return 1
    results = json.load(open(args.result, encoding="utf-8"))
    fam_scores = build_scores(results, smooth=args.smooth)

    cfg = json.load(open(args.config, encoding="utf-8"))
    scores = {}
    seen = set()
    for e in cfg["pool"]:
        mid = e["id"] if isinstance(e, dict) else e.split("@")[0]
        fam = family(mid)
        if fam in fam_scores:
            scores[mid] = fam_scores[fam]
            seen.add(fam)
        else:
            scores[mid] = 50.0
    missing = sorted(set(fam_scores) - seen)

    cfg["quality_scores"] = scores
    cfg["quality_priority"] = True

    print("模型质量分（按分数降序）:")
    for mid, s in sorted(scores.items(), key=lambda kv: -kv[1]):
        print("  %6.1f  %s" % (s, mid))
    if missing:
        print("\n注意：以下家族在池里找不到对应入口: %s" % missing)
    if args.dry_run:
        print("\n--dry-run：未写入 %s" % args.config)
        return 0

    json.dump(cfg, open(args.config, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("\n已写入 %s（%d 项），重启服务生效" % (args.config, len(scores)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
