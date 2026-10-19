#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 config.json 的 pool 与 agent2api 当前实际可用模型对齐。

判定口径（三类，处理方式不同）：
  1. 上游清单里已不存在           -> 移除（模型被网关下线，永久失效）
  2. 存在但 probe 返回 404 / model_not_found -> 移除（已关闭）
  3. 存在但 probe 返回 429 额度不足 -> 保留（日额度类，明天会恢复；
     rotator 自身有 600s 冷却，不会持续打满）
  4. 新增且 probe 工具调用成功    -> 加入池

quality_scores：新成员按「家族」继承已有分数（与 apply_quality.py 同口径），
无先例的给 50 分（该文件注释里的既定缺省值）。

用法:
  python sync_pool.py --dry-run     # 只看差异
  python sync_pool.py               # 写回 config.json（自动备份）
"""
import argparse
import json
import os
import re
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = os.path.join(HERE, "config.json")
PROBE = os.path.join(HERE, "probe_result.json")

KEY_FROM = "Qwen3.8-Flash"      # 网关密钥来源（models.json 中的条目 id）
URL = "http://127.0.0.1:3065/v1"

PREFIXES = ("wb-", "wai-", "catpaw-", "sn-", "qoder-")
SUFFIXES = ("-solo", "-lite", "-free", "-preview")


def bare(mid):
    """去掉 @account 后缀，取纯模型 id。"""
    return mid.rsplit("@", 1)[0] if "@" in mid else mid


def family(mid):
    """归并到「同族」：去掉入口前缀和修饰后缀。

    后缀要循环剥离（-solo 之外可能还挂着 -free），并额外吞掉单字母分片后缀
    （hy3-b / hy3-c 是 hy3 的多账号副本，质量应同分）。
    """
    x = bare(mid)
    for p in PREFIXES:
        if x.startswith(p):
            x = x[len(p):]
            break
    changed = True
    while changed:
        changed = False
        for s in SUFFIXES:
            if x.endswith(s) and len(x) > len(s):
                x = x[: -len(s)]
                changed = True
        if re.search(r"-[a-z]$", x) and len(x) > 3:
            x = x[:-2]
            changed = True
    return x.lower().replace(".", "-")


def guess_account(mid):
    """按已知前缀推断账号标签（只影响并发分片，不影响鉴权）。"""
    m = bare(mid)
    if m.startswith("wb-"):
        return "wb"
    if m.startswith("wai-"):
        return "wai"
    if m.startswith("catpaw-") or m.startswith("LongCat"):
        return "tt"
    if m.startswith("qoder-") or m.startswith("Qwen3."):
        return "qoder"
    if m.startswith("sn-") or m.startswith("sensenova-"):
        return "raccoon"
    if m.startswith("hy3"):
        # hy3-b / hy3-c 是同模型的不同账号副本，必须各自分桶，否则并发被当成同一资源串行
        return m
    if m.endswith("-solo"):
        return "trae"
    if m.endswith("-free"):
        return "free"
    return m


def entry_dict(mid):
    return {"id": mid, "url": URL, "key_from": KEY_FROM, "account": guess_account(mid)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--config", default=CFG)
    ap.add_argument("--probe", default=PROBE)
    ap.add_argument("--upstream", default="",
                    help="agent2api /v1/models 原始响应 JSON；给了就以它为上游全集，"
                         "否则退化为「probe 覆盖到的 id」——只探测了子集时会误删未探测的模型")
    ap.add_argument("--max-add", type=int, default=0,
                    help="本次最多新增多少个（0=不限）。池太大会让 30 分钟轮换里单模型样本过稀")
    args = ap.parse_args()

    cfg = json.load(open(args.config, encoding="utf-8"))
    probe = json.load(open(args.probe, encoding="utf-8"))
    pr = {r["id"]: r for r in probe}

    if args.upstream:
        raw = json.load(open(args.upstream, encoding="utf-8"))
        upstream = {m["id"] for m in raw.get("data", raw if isinstance(raw, list) else [])}
        scope = "上游清单 %s (%d 个)" % (os.path.basename(args.upstream), len(upstream))
    else:
        upstream = set(pr)
        scope = "probe 覆盖范围 (%d 个) —— 注意：非上游全集" % len(upstream)
    print("判定依据: %s" % scope)

    # 自伤检测：agent2api 对整把密钥限流（默认 120 req/min）。高并发探测打出的 429
    # 是探针自己造成的，不代表模型不可用——出现这类结果时不要据此判生死。
    sus = [r["id"] for r in probe if not r["ok"]
           and ("requests per minute" in (r.get("err") or "")
                or "rate-limited after" in (r.get("err") or ""))]
    if sus:
        print("!! 警告: %d 个结果是限流假阴性，建议先 `probe_models.py --rpm 90` 补测: %s%s"
              % (len(sus), ", ".join(sus[:5]), " ..." if len(sus) > 5 else ""))

    pool = cfg.get("pool", [])
    keep, drop = [], []
    for e in pool:
        mid = bare(e["id"] if isinstance(e, dict) else e)
        r = pr.get(mid)
        err = (r or {}).get("err") or ""
        if mid not in upstream:
            drop.append((mid, "上游清单已无此模型"))
        elif r and r.get("http") == 404:
            drop.append((mid, "404 已下线"))
        elif r and r.get("http") == 403 and "not available in your country" in err:
            drop.append((mid, "403 地区限制，永久不可用"))
        else:
            keep.append(e)

    kept_ids = {bare(e["id"] if isinstance(e, dict) else e) for e in keep}

    # 新增：探测工具调用成功、且不在池里
    kept_fams = {family(bare(e["id"] if isinstance(e, dict) else e)) for e in keep}
    add_all = []
    for mid, r in pr.items():
        if r.get("tool_calls") and mid not in kept_ids:
            add_all.append(mid)

    # 排序：先按「是否已有同族信任」（同族变体风险最低），再按延迟升序
    add_all.sort(key=lambda m: (0 if family(m) in kept_fams else 1,
                                pr[m].get("latency") or 999))

    add = add_all
    skipped = []
    if args.max_add and len(add_all) > args.max_add:
        add = add_all[: args.max_add]
        skipped = add_all[args.max_add:]

    scores = dict(cfg.get("quality_scores") or {})

    def score_for(mid):
        # 同族已有分数的，取最高者（同族同模型不同入口，质量应一致）
        fam = family(mid)
        cand = [s for k, s in scores.items() if family(k) == fam]
        return max(cand) if cand else 50.0

    print("== 移除 (%d) ==" % len(drop))
    for mid, why in drop:
        print("  - %-40s %s" % (mid, why))
    print("\n== 新增 (%d) ==" % len(add))
    for mid in add:
        print("  + %-40s score=%5.1f acct=%-10s %4.1fs"
              % (mid, score_for(mid), guess_account(mid), pr[mid].get("latency") or 0))
    if skipped:
        print("\n-- 达标但本次未加入（受 --max-add=%d 限制）%d 个 --" % (args.max_add, len(skipped)))
        for mid in skipped[:40]:
            print("    %-40s %4.1fs" % (mid, pr[mid].get("latency") or 0))
        if len(skipped) > 40:
            print("    ... 另有 %d 个" % (len(skipped) - 40))
    print("\n== 保留 %d 项 ==" % len(keep))
    kept_fail = [bare(e["id"] if isinstance(e, dict) else e)
                 for e in keep
                 if pr.get(bare(e["id"] if isinstance(e, dict) else e), {}).get("ok") is False]
    if kept_fail:
        print("  其中本次探测失败但保留（额度类，会自愈）: %s" % ", ".join(kept_fail))

    new_pool = keep + [entry_dict(m) for m in add]
    for m in add:
        scores[m] = score_for(m)

    print("\n池规模: %d -> %d" % (len(pool), len(new_pool)))

    if args.dry_run:
        print("\n--dry-run：未写入")
        return 0

    bak = "%s.bak-%s" % (args.config, time.strftime("%Y%m%d-%H%M%S"))
    shutil.copy2(args.config, bak)
    cfg["pool"] = new_pool
    cfg["quality_scores"] = scores
    cfg["quality_priority"] = True
    json.dump(cfg, open(args.config, "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print("\n已写入 %s（备份 %s）" % (args.config, os.path.basename(bak)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
