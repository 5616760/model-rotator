#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
agent2api 模型能力实测探针
==========================
agent2api /v1/models 报的 supports_tool_call 字段不可信：
  - sensenova-6-8-flash  标 tool=False，实际能正常返回 tool_calls
  - space-bunny-free     标 tool=False，实际直接 404
所以必须逐个发真实工具调用请求，用实测结果决定能否进轮换池。

用法: python probe_models.py [--only id1,id2] [--timeout 45]
输出: probe_result.json  + 终端表格
"""
import argparse
import concurrent.futures as cf
import json
import os
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
# 默认值面向本地 agent2api 网关（如 WorkBuddy 自带）；可通过参数覆盖。
AGENT2API = "http://127.0.0.1:3065/v1"
MODELS_JSON = os.path.expanduser("~/.workbuddy/models.json")

# 全局节流闸：agent2api 对整把密钥限流（默认 120 req/min）。并发跑探针时
# 如果不限速，绝大多数结果会是「429 requests per minute」——那不是模型不可用，
# 是探针自己把额度打满了。--rpm 用来把请求压到限流线以下。
_THROTTLE_LOCK = threading.Lock()
_LAST_CALL = [0.0]
_MIN_INTERVAL = [0.0]


def _throttle():
    if _MIN_INTERVAL[0] <= 0:
        return
    with _THROTTLE_LOCK:
        now = time.time()
        wait = _LAST_CALL[0] + _MIN_INTERVAL[0] - now
        if wait > 0:
            time.sleep(wait)
            now = time.time()
        _LAST_CALL[0] = now

TOOLS = [{
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get current weather for a city",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
}]

PROMPT = "What's the weather in Beijing? Call the get_weather tool."


def get_key():
    with open(MODELS_JSON, encoding="utf-8") as f:
        models = json.load(f)
    for m in models:
        if m.get("url", "").startswith("http://127.0.0.1:3065"):
            return m.get("apiKey", "")
    return ""


def probe(mid, key, timeout, want_stream):
    body = {
        "model": mid,
        "max_tokens": 96,
        "messages": [{"role": "user", "content": PROMPT}],
    }
    if want_stream:
        body["stream"] = False      # tool_call 判定用非流式更稳
    if TOOLS:
        body["tools"] = TOOLS
    req = urllib.request.Request(
        AGENT2API + "/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + key},
    )
    out = {"id": mid, "ok": False, "tool_calls": False, "http": None,
           "err": "", "latency": 0.0, "text": ""}
    _throttle()
    t0 = time.time()
    try:
        r = urllib.request.urlopen(req, timeout=timeout,
                                   context=ssl.create_default_context())
        raw = r.read()
        out["latency"] = round(time.time() - t0, 1)
        out["http"] = r.status
        d = json.loads(raw)
        out["ok"] = True
        msg = (d.get("choices") or [{}])[0].get("message") or {}
        tcs = msg.get("tool_calls")
        out["tool_calls"] = bool(tcs)
        out["finish"] = (d.get("choices") or [{}])[0].get("finish_reason")
        if tcs:
            try:
                out["called_fn"] = tcs[0]["function"]["name"]
                out["called_args"] = tcs[0]["function"]["arguments"][:80]
            except Exception:
                pass
        else:
            out["text"] = (msg.get("content") or "")[:60]
        u = d.get("usage") or {}
        out["prompt_tokens"] = u.get("prompt_tokens")
    except urllib.error.HTTPError as e:
        out["latency"] = round(time.time() - t0, 1)
        out["http"] = e.code
        try:
            out["err"] = e.read().decode("utf-8", "replace")[:160]
        except Exception:
            out["err"] = "HTTPError %s" % e.code
    except Exception as e:
        out["latency"] = round(time.time() - t0, 1)
        out["err"] = "%s: %s" % (type(e).__name__, str(e)[:120])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--timeout", type=int, default=45)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--agent2api", default="",
                    help="upstream gateway base, e.g. http://127.0.0.1:3065/v1")
    ap.add_argument("--models-json", default="",
                    help="path to a models.json that holds api keys (WorkBuddy style)")
    ap.add_argument("--rpm", type=int, default=0,
                    help="全局请求速率上限（req/min），0=不限。agent2api 默认对整把密钥限 120/min，"
                         "大批量探测务必设成 90 左右，否则结果会全是假 429")
    ap.add_argument("--out", default="",
                    help="结果输出路径，默认 probe_result.json")
    ap.add_argument("--merge", action="store_true",
                    help="与已有结果文件合并（只覆盖本轮 id），用于分批补测")
    ap.add_argument("--retry-tool", type=int, default=1,
                    help="对「200 但未调工具」的模型补测轮数，默认 1；设 0 关闭")
    args = ap.parse_args()

    if args.rpm > 0:
        _MIN_INTERVAL[0] = 60.0 / args.rpm

    if args.agent2api:
        globals()["AGENT2API"] = args.agent2api.rstrip("/")
    if args.models_json:
        globals()["MODELS_JSON"] = os.path.expanduser(args.models_json)

    with urllib.request.urlopen(AGENT2API + "/models", timeout=20) as r:
        all_models = json.loads(r.read()).get("data", [])
    ids = [m["id"] for m in all_models]
    if args.only:
        want = {s.strip() for s in args.only.split(",") if s.strip()}
        ids = [i for i in ids if i in want]

    key = get_key()
    print("探测 %d 个模型（并发 %d）...\n" % (len(ids), args.workers))

    results = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(probe, mid, key, args.timeout, False): mid for mid in ids}
        for fut in cf.as_completed(futs):
            res = fut.result()
            results.append(res)
            flag = "TOOL-OK " if res["tool_calls"] else ("ok      " if res["ok"] else "FAIL    ")
            print("  %s %-42s http=%-4s %5.1fs %s" % (
                flag, res["id"], res["http"], res["latency"],
                (res.get("err") or res.get("called_fn") or res.get("text") or "")[:60]))

    # 工具调用是「要求它调」而不是「禁止它调」，模型偶尔会直接文字作答而不调工具。
    # 单次判定会把这些模型误判成不可用，所以对「200 但没调工具」的补测几轮。
    if args.retry_tool > 0:
        unclear = [r for r in results if r["ok"] and not r["tool_calls"]]
        if unclear:
            print("\n补测 %d 个「200 但未调工具」的模型（最多 %d 轮）..."
                  % (len(unclear), args.retry_tool))
            idx = {r["id"]: i for i, r in enumerate(results)}
            for rnd in range(args.retry_tool):
                todo = [r for r in results if r["ok"] and not r["tool_calls"]]
                if not todo:
                    break
                with cf.ThreadPoolExecutor(max_workers=max(1, min(args.workers, 4))) as ex:
                    futs = {ex.submit(probe, r["id"], key, args.timeout, False): r["id"]
                            for r in todo}
                    for fut in cf.as_completed(futs):
                        res = fut.result()
                        if res["tool_calls"]:
                            print("  轮%d 恢复 TOOL-OK  %s (%.1fs)"
                                  % (rnd + 1, res["id"], res["latency"]))
                            results[idx[res["id"]]] = res

    order = {m: i for i, m in enumerate(ids)}
    results.sort(key=lambda r: order[r["id"]])
    out_path = args.out or os.path.join(HERE, "probe_result.json")
    if args.merge and os.path.exists(out_path):
        # 只覆盖本轮探测到的 id，保留上一轮其它模型的结果（分批补测用）
        merged = {r["id"]: r for r in json.load(open(out_path, encoding="utf-8"))}
        for r in results:
            merged[r["id"]] = r
        results_to_write = list(merged.values())
    else:
        results_to_write = results
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results_to_write, f, ensure_ascii=False, indent=2)

    good = [r["id"] for r in results if r["tool_calls"]]
    chat = [r["id"] for r in results if r["ok"] and not r["tool_calls"]]
    bad = [r["id"] for r in results if not r["ok"]]
    print("\n==== 汇总 ====")
    print("工具调用可用 (%d): %s" % (len(good), ", ".join(good)))
    print("仅对话可用 (%d): %s" % (len(chat), ", ".join(chat)))
    print("不可用     (%d): %s" % (len(bad), ", ".join(bad)))
    print("结果已写入 probe_result.json")


if __name__ == "__main__":
    main()