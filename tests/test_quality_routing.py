#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
质量优先路由回归测试。

约定：
- 账号在途数是第一优先级（并发必须摊开，否则多任务压一个账号会被限流打爆）；
- 同等空闲时才比质量分（quality 越大越好）；
- 时间窗距离只做最后的 tie-break。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import rotator  # noqa: E402
from rotator import State  # noqa: E402


def setup(priority=True):
    rotator.CFG = {"quality_priority": priority}


def pool_of(specs):
    # specs: [(id, account, quality), ...]
    return [{"id": i, "account": a, "quality": q} for i, a, q in specs]


def test_prefers_higher_quality_when_idle():
    setup()
    # 全部不同账号、空闲：应选质量最高的
    pool = pool_of([("low", "A", 40), ("high", "B", 95), ("mid", "C", 70)])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    idx, _ = st.resolve("s1")
    assert pool[idx]["id"] == "high", pool[idx]["id"]
    print("OK idle -> highest quality:", pool[idx]["id"])


def test_account_load_beats_quality():
    setup()
    # high 质量最高，但它的账号已有在途请求 → 本次必须让给空闲账号
    pool = pool_of([("high", "A", 95), ("low", "B", 40)])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    i1, _ = st.resolve("s1")
    assert pool[i1]["id"] == "high"
    i2, _ = st.resolve("s2")          # high 的账号 A 已占用 1
    assert pool[i2]["id"] == "low", pool[i2]["id"]
    print("OK load beats quality ->", pool[i1]["id"], "then", pool[i2]["id"])


def test_quality_priority_off_uses_window_order():
    setup(priority=False)
    pool = pool_of([("a", "A", 10), ("b", "B", 99), ("c", "C", 50)])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    idx, _ = st.resolve("s1")
    assert idx == 0, idx     # 关掉质量优先后回到「从当前窗开始」的顺序
    print("OK priority off -> window order index", idx)


def test_quality_after_cooldown():
    setup()
    pool = pool_of([("best", "A", 99), ("ok2", "B", 60)])
    st = State(pool, rotate_s=600, cooldown_s=60, rotate_on_first=True)
    i1, _ = st.resolve("s1")
    assert pool[i1]["id"] == "best"
    st.mark_fail(i1, "http_429")
    st.release(i1)
    i2, _ = st.resolve("s1")
    assert pool[i2]["id"] == "ok2", pool[i2]["id"]
    print("OK cooldown falls to next quality ->", pool[i2]["id"])


def test_mid_quality_in_same_account_competes():
    setup()
    # 同账号两个模型质量不同、账号都空闲：B 账号里高分那个胜出
    pool = pool_of([("b_lo", "B", 50), ("b_hi", "B", 90), ("a_lo", "A", 30)])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    idx, _ = st.resolve("s1")
    assert pool[idx]["id"] == "b_hi", pool[idx]["id"]
    print("OK best within account ->", pool[idx]["id"])


def test_missing_quality_defaults_50():
    setup()
    pool = [{"id": "x", "account": "A"}, {"id": "y", "account": "B", "quality": 80}]
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    idx, _ = st.resolve("s1")
    assert pool[idx]["id"] == "y", pool[idx]["id"]
    print("OK missing quality treated as 50 ->", pool[idx]["id"])


def test_concurrency_still_spreads_with_quality():
    setup()
    # 质量优先不能破坏并发摊开：4 个账号应摊 4 路
    pool = pool_of([("m%d" % i, "AC%d" % i, 90 - i * 5) for i in range(4)])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    got = [st.resolve("default")[0] for _ in range(4)]
    accts = [pool[i]["account"] for i in got]
    assert len(set(accts)) == 4, accts
    # 首路应是质量最高的
    assert pool[got[0]]["id"] == "m0", pool[got[0]]["id"]
    print("OK concurrency spreads + best first ->", accts)


if __name__ == "__main__":
    test_prefers_higher_quality_when_idle()
    test_account_load_beats_quality()
    test_quality_priority_off_uses_window_order()
    test_quality_after_cooldown()
    test_mid_quality_in_same_account_competes()
    test_missing_quality_defaults_50()
    test_concurrency_still_spreads_with_quality()
    print("\nALL QUALITY ROUTING TESTS PASSED")
