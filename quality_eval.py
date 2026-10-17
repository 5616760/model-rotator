#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模型质量评测（能力，不是可用性）
===========================

为什么需要它：名字里带 Flash / Plus / Pro 不能直接当能力排序依据，而
probe_models.py 只能验证“能不能调工具”，测不出“答得对不对”。要给用户一个
可信的质量排序，必须用一组有唯一答案的题目实测正确率。

做法：
- 从 config.json 的 pool 里把 30 个入口按家族去重（wb-/wai-/catpaw-/sn- 前缀、
  -solo/-lite 后缀归一），每个家族取第一个入口做代表。
- 对代表入口串行/并发发若干固定题，自动判分。
- 输出 quality_result.json + 终端表。

用法:
  python quality_eval.py                 # 评所有家族
  python quality_eval.py --only hy3,deepseek-v4.1-flash
  python quality_eval.py --repeat 2       # 每题重复 2 次（降噪，更慢）

注意：会真实消耗 agent2api 额度（n_family * n_q * repeat 次请求）。
必须绕过环境代理（urllib 默认会把 127.0.0.1 也绕出去）。
"""
import argparse
import concurrent.futures as cf
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT2API = "http://127.0.0.1:3065/v1"
MODELS_JSON = os.path.expanduser("~/.workbuddy/models.json")
# 绕过环境代理：127.0.0.1 请求不能走外部代理
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

SYSTEM = "你是答题机器人。按要求格式输出答案。"
FORMAT_TAIL = "\n\n要求：不要解释，最后一行必须单独写 `ANSWER: <答案>`。数字题只写数字，选择题只写字母，星期题只写星期几。"

TASKS = [
    {
        "id": "arith_profit",
        "prompt": "一个商店以 80 元进货、以 100 元卖出。今天卖出 7 件。今天的利润是多少元？",
        "answer": "140", "match": "number",
        "note": "一步乘法+减法",
    },
    {
        "id": "syllogism",
        "prompt": ("已知前提：\n1) 所有玫瑰都是花。\n2) 有些花很快凋谢。\n"
                   "下面哪一项必然正确？\nA) 有些玫瑰很快凋谢\n"
                   "B) 所有玫瑰都很快凋谢\nC) 以上都不必然正确"),
        "answer": "C", "match": "choice",
        "note": "三段论陷阱",
    },
    {
        "id": "code_exec",
        "prompt": ("这段 Python 最终打印什么数字？\n"
                   "def f(n):\n    return n if n < 2 else f(n-1) + f(n-2)\n"
                   "print(f(6))"),
        "answer": "8", "match": "number",
        "note": "斐波那契 f(6)",
    },
    {
        "id": "exact_count",
        "prompt": "字符串 'mississippi' 中字母 s 一共出现几次？",
        "answer": "4", "match": "number",
        "note": "精确计数",
    },
    {
        "id": "machines",
        "prompt": "如果 3 台机器 3 分钟生产 3 个零件，那么 100 台机器生产 100 个零件需要多少分钟？",
        "answer": "3", "match": "number",
        "note": "反直觉比例",
    },
    {
        "id": "weekday100",
        "prompt": "今天是星期三。100 天之后是星期几？只回答星期几（如“周五”）。",
        "answer": "周五", "match": "weekday",
        "note": "模 7 运算",
    },
    # ---- 以下为提高区分度的难题（基础题对 Flash 级模型太简单，全员满分）----
    {
        "id": "constraint",
        "prompt": ("4 个人 A、B、C、D 坐在一排 4 个座位上（从左到右编号 1-4）。\n"
                   "条件：A 不在 1 号位；B 紧挨着 C 坐；D 坐在 B 右边（不一定相邻）。\n"
                   "问：A 一定坐在哪个位置？\n"
                   "A) 1 号  B) 2 号  C) 3 号  D) 无法确定"),
        "answer": "D", "match": "choice4",
        "note": "多约束排列，B/C 可互换导致 A 位置不唯一",
    },
    {
        "id": "str_trace",
        "prompt": ("执行下面代码后 s 的值是什么？\n"
                   "s = 'abcde'\nfor i in range(0, len(s), 2):\n"
                   "    s = s[:i] + s[i].upper() + s[i+1:]\nprint(s)\n"
                   "只输出最终字符串。"),
        "answer": "AbCdE", "match": "text",
        "note": "循环中修改被迭代对象",
    },
    {
        "id": "negation",
        "prompt": ("某公司规定：“所有加班超过 3 小时的员工，都可以申请调休。”\n"
                   "以下哪项如果为真，能说明该规定被执行时出现了违规？\n"
                   "A) 有员工加班 5 小时但没申请调休\n"
                   "B) 有员工加班 4 小时，申请调休被拒绝\n"
                   "C) 有员工没加班却申请了调休\n"
                   "D) 有员工加班 2 小时且申请了调休"),
        "answer": "B", "match": "choice4",
        "note": "“可以申请”与“必须批准”的区别",
    },
    {
        "id": "mod_arith",
        "prompt": ("求 7 的 2026 次方除以 5 的余数。只输出这个余数。"),
        "answer": "4", "match": "number",
        "note": "模幂循环，需周期推理",
    },
    {
        "id": "long_count",
        "prompt": ("在下面这串字符中，字母 q 一共出现了多少次？\n"
                   "qweqrwqqeqtqyuqioqpqqasqdfqghqjkqlqzqxqcqvqbq"
                   "nqmqwqeqrqtqyquqiqoqpqaqsqdqfqgqhqjqkqlqzqxqc"),
        "answer": "42", "match": "number",
        "note": "长串精确计数，易漏",
    },
    {
        "id": "bool_logic",
        "prompt": ("设 p 为真、q 为假、r 为真。表达式 `(p or q) and (not r or q)` 的值是什么？\n"
                   "只输出 True 或 False。"),
        "answer": "False", "match": "bool",
        "note": "布尔运算需严格按优先级",
    },
]


def get_key():
    try:
        with open(MODELS_JSON, encoding="utf-8") as f:
            models = json.load(f)
        for m in models:
            if m.get("url", "").startswith("http://127.0.0.1:3065"):
                return m.get("apiKey", "") or ""
    except Exception:
        pass
    return ""


def family(mid):
    x = mid
    for p in ("wb-", "wai-", "catpaw-", "sn-"):
        if x.startswith(p):
            x = x[len(p):]
            break
    for s in ("-solo", "-lite"):
        if x.endswith(s):
            x = x[: -len(s)]
            break
    return x


def dedup_pool(path):
    cfg = json.load(open(path, encoding="utf-8"))
    seen = {}
    for e in cfg["pool"]:
        if isinstance(e, dict) and "url" in e:
            mid = e["id"]
        else:
            mid = e.split("@")[0]
        fam = family(mid)
        if fam not in seen:
            seen[fam] = mid          # 每个家族取第一个入口做代表
    return seen


def ask(mid, key, task, timeout):
    body = {
        "model": mid,
        "max_tokens": 2048,          # 推理模型(如 hy3)会先烧 token 在 reasoning 上，
                                     # 上限太小会导致 content 直接为空、答案被截断
        "stream": False,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": task["prompt"] + FORMAT_TAIL},
        ],
    }
    req = urllib.request.Request(
        AGENT2API + "/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + key},
    )
    t0 = time.time()
    try:
        with _OPENER.open(req, timeout=timeout) as r:
            d = json.loads(r.read().decode("utf-8"))
        msg = ((d.get("choices") or [{}])[0].get("message") or {})
        # 推理模型的答案可能在 reasoning_content 里（content 为空或只有一句结论）
        content = msg.get("content") or ""
        reasoning = msg.get("reasoning_content") or ""
        text = content if content.strip() else reasoning
        return {"ok": True, "text": text, "content": content,
                "reasoning": reasoning, "latency": round(time.time() - t0, 1)}
    except urllib.error.HTTPError as e:
        return {"ok": False, "text": "", "latency": round(time.time() - t0, 1),
                "err": "HTTP %s" % e.code}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "text": "", "latency": round(time.time() - t0, 1),
                "err": "%s" % str(e)[:120]}


ANSWER_RE = re.compile(r"ANSWER\s*[:：]\s*(.+)", re.IGNORECASE)


def _extract(blob):
    """抽最终答案。

    优先级：1) 最后一个 ANSWER: 行（这是要求的格式，也是抗干扰的关键——模型正文
    里的中间数字不会被当成答案）；2) 没有 ANSWER 标记时取最后一个非空行。
    注意：不能取正文里的任意数字——被评模型很话痨，解释里满是干扰数字；而且
    WorkBuddy 渠道会把「砺行」人设提示注入进模型，模型会坚持写"结论/证据"段落。
    """
    if not blob:
        return ""
    ms = ANSWER_RE.findall(blob)
    if ms:
        return ms[-1].strip().strip("`*_ ").rstrip(".。").strip()
    lines = [ln.strip() for ln in blob.splitlines() if ln.strip()]
    if lines:
        return lines[-1].strip().strip("`*_ ").rstrip(".。").strip()
    return blob.strip()


def grade(task, res):
    ans = task["answer"]

    def parse(s):
        if not s:
            return False
        m = task["match"]
        if m == "number":
            nums = re.findall(r"-?\d+", s)
            return bool(nums) and nums[-1] == ans
        if m in ("choice", "choice4"):
            # choice 只要 A/B/C；choice4 还要 D
            letters = re.findall(r"\b[A-D]\b", s.upper()) or re.findall(r"[A-D]", s.upper())
            return bool(letters) and letters[-1].strip() == ans
        if m == "weekday":
            return "五" in s
        if m == "bool":
            low = s.lower()
            if "true" in low and "false" not in low:
                return ans == "True"
            if "false" in low and "true" not in low:
                return ans == "False"
            return False
        if m == "text":
            # 大小写敏感：这类题考的正是大小写变换是否算对了
            return s.strip().strip("`'\" ") == ans
        return ans in s

    content = (res.get("content") or "") if isinstance(res, dict) else (res or "")
    reasoning = (res.get("reasoning") or "") if isinstance(res, dict) else ""
    # content 与 reasoning 都找 ANSWER 行；任一处对就算对（推理模型常把结论写在思考里）
    return parse(_extract(content)) or parse(_extract(reasoning))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--config", default=os.path.join(HERE, "config.json"))
    args = ap.parse_args()

    reps = dedup_pool(args.config)
    if args.only:
        want = {s.strip() for s in args.only.split(",") if s.strip()}
        reps = {k: v for k, v in reps.items() if k in want or v in want}
    if not reps:
        print("没有可测的模型家族")
        return 1

    key = get_key()
    print("评测 %d 个家族，每题重复 %d 次，并发 %d\n" % (len(reps), args.repeat, args.workers))

    jobs = [(fam, mid, task, rep)
            for fam, mid in reps.items()
            for task in TASKS
            for rep in range(args.repeat)]

    def run(j):
        fam, mid, task, rep = j
        res = ask(mid, key, task, args.timeout)
        return (fam, mid, task["id"], rep, res)

    results = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for out in ex.map(run, jobs):
            results.append(out)
            fam, mid, tid, rep, res = out
            ok = res.get("ok")
            print("  %-26s %-14s %s %.1fs" % (
                fam, tid, "ok" if ok else ("ERR " + res.get("err", "")), res["latency"]))

    # 汇总：按 (家族, 题目) 逐题累计，repeat 多次时是「该题的正确次数 / 尝试次数」。
    # 注意不能直接覆盖 per_task —— 重复跑时后一次会把前一次的结果冲掉，导致
    # 分数与明细对不上（这个 bug 在第一版出现过：12 个 ✓ 却算出 91.7%）。
    summary = {}
    for fam, mid, tid, rep, res in results:
        s = summary.setdefault(fam, {"model": mid, "task": {}, "n": 0,
                                     "ok": 0, "fails": 0, "lat": []})
        t = s["task"].setdefault(tid, {"hit": 0, "try": 0})
        if res.get("ok"):
            correct = bool(grade(next(x for x in TASKS if x["id"] == tid), res))
            t["try"] += 1
            t["hit"] += 1 if correct else 0
            s["n"] += 1
            s["ok"] += 1 if correct else 0
            s["lat"].append(res["latency"])
        else:
            t["try"] += 1          # 失败也计入该题分母：调用不通同样算没答对
            s["fails"] += 1

    out = []
    for fam, s in summary.items():
        # 分母用总尝试次数（含失败调用），避免「调用失败反而不扣分」
        total_try = sum(v["try"] for v in s["task"].values())
        score = round(100.0 * s["ok"] / (total_try or 1), 1)
        avg_lat = round(sum(s["lat"]) / len(s["lat"]), 1) if s["lat"] else 0
        tp = {k: ("✓" if v["hit"] and v["hit"] == v["try"]
                  else ("△" if v["hit"] else "✗"))
              for k, v in sorted(s["task"].items())}
        out.append({"family": fam, "model": s["model"], "correct_pct": score,
                    "avg_latency": avg_lat, "fail_calls": s["fails"],
                    "attempts": total_try, "per_task": tp})
        print("  %-22s %s 正确率=%4.1f%% 平均延迟=%5.1fs 失败调用=%d" % (
            fam, s["model"], score, avg_lat, s["fails"]))
    out.sort(key=lambda r: (-r["correct_pct"], r["avg_latency"]))
    with open(os.path.join(HERE, "quality_result.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n排序（质量降序）:")
    for i, r in enumerate(out, 1):
        print("  %2d. %-22s 正确率=%4.1f%%  延迟=%5.1fs" % (
            i, r["family"], r["correct_pct"], r["avg_latency"]))
    print("写入 quality_result.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
