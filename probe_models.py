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
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
# 默认值面向本地 agent2api 网关（如 WorkBuddy 自带）；可通过参数覆盖。
AGENT2API = "http://127.0.0.1:3065/v1"
MODELS_JSON = os.path.expanduser("~/.workbuddy/models.json")

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
    import time
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
    args = ap.parse_args()

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

    order = {m: i for i, m in enumerate(ids)}
    results.sort(key=lambda r: order[r["id"]])
    with open(os.path.join(HERE, "probe_result.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

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