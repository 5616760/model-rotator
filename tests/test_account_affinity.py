#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
账号感知路由回归测试：
- 并发的不同会话应尽量落到不同账号（不同任务用不同账号的模型）。
- 单会话仍跟随时间窗轮换（不会永久钉死）。
- 账号标签缺失时回退为「按模型 id 分桶」，仍能分片。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rotator import State  # noqa: E402


def make_pool(n, accounts):
    # accounts 长度可与 n 不同，用来模拟多个模型共享同一账号
    return [{"id": "m%d" % i, "account": accounts[i % len(accounts)]} for i in range(n)]


def test_concurrent_sessions_get_distinct_accounts():
    # 3 个账号、9 个模型（每账号 3 模型），同一时刻 3 个并发会话应各占不同账号
    pool = make_pool(9, ["A", "B", "C"])
    st = State(pool, rotate_s=10, cooldown_s=0, rotate_on_first=True)
    got = {st.resolve("s%d" % k)[0] for k in range(3)}
    accts = {pool[i]["account"] for i in got}
    assert len(got) == 3, got                       # 三个会话路由到三个不同模型
    assert len(accts) == 3, accts                   # 且分属三个不同账号
    print("OK concurrent: distinct accounts ->", accts)


def test_lone_session_follows_window():
    pool = [{"id": "m%d" % i, "account": "A%d" % i} for i in range(4)]
    st = State(pool, rotate_s=1, cooldown_s=0, rotate_on_first=True)
    i0, _ = st.resolve("a")
    time.sleep(1.1)
    i1, _ = st.resolve("a")
    time.sleep(1.1)
    i2, _ = st.resolve("a")
    assert (i0, i1, i2) == (0, 1, 2), (i0, i1, i2)
    print("OK lone session follows window:", (i0, i1, i2))


def test_fallback_to_model_id_when_no_account():
    # 故意不给 account（build_config 里会回退为模型 id），并发仍应分片到不同模型
    pool = [{"id": "m%d" % i} for i in range(6)]
    st = State(pool, rotate_s=10, cooldown_s=0, rotate_on_first=True)
    got = {st.resolve("s%d" % k)[0] for k in range(4)}
    assert len(got) == 4, got
    print("OK fallback(model-id) sharding ->", got)


def test_overflow_least_used():
    # 账号数(2)少于并发会话数(3)：最多一个账号被复用，且是占用最少者
    pool = make_pool(6, ["A", "B"])
    st = State(pool, rotate_s=10, cooldown_s=0, rotate_on_first=True)
    idxs = [st.resolve("s%d" % k)[0] for k in range(3)]
    accts = [pool[i]["account"] for i in idxs]
    assert accts.count("A") <= 2 and accts.count("B") <= 2, accts
    assert set(accts) <= {"A", "B"}
    print("OK overflow least-used ->", accts)


if __name__ == "__main__":
    test_concurrent_sessions_get_distinct_accounts()
    test_lone_session_follows_window()
    test_fallback_to_model_id_when_no_account()
    test_overflow_least_used()
    print("\nALL ACCOUNT-AFFINITY TESTS PASSED")
