#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
账号分片路由回归测试（按「在途请求数」分片）：
- 同一时刻的多个并发请求应落到不同账号（关键：客户端通常都发同一个默认会话，
  所以不能按会话分片，必须按在途数）。
- 顺序请求（无并发）保持粘性，不会被摊开。
- 单会话仍跟随时间窗轮换（不会永久钉死）。
- 未标 account 时回退为「一个模型一个账号」，仍能分片。
- 请求结束 release 后名额归零，不泄漏。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rotator import State  # noqa: E402


def make_pool(n, accounts):
    # accounts 长度可与 n 不同，用来模拟多个模型共享同一账号
    return [{"id": "m%d" % i, "account": accounts[i % len(accounts)]} for i in range(n)]


def test_default_session_concurrency_spreads():
    """客户端全用同一个 sid 时也必须摊开——这是修复前失效的场景。"""
    pool = make_pool(9, ["A", "B", "C"])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    idxs = [st.resolve("default")[0] for _ in range(3)]   # 3 个并发，sid 全相同
    accts = [pool[i]["account"] for i in idxs]
    assert len(set(accts)) == 3, accts
    assert len(set(idxs)) == 3, idxs
    print("OK same-sid concurrency spreads ->", accts)


def test_sequential_requests_stay_sticky():
    """没有并发时，同一个会话应一直用同一个模型（上下文不跳变）。"""
    pool = make_pool(9, ["A", "B", "C"])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    i0 = st.resolve("default")[0]
    st.release(i0)                                        # 请求结束
    for _ in range(3):
        i = st.resolve("default")[0]
        assert i == i0, (i, i0)
        st.release(i)
    print("OK sequential sticky ->", i0)


def test_concurrent_overflow_uses_least_loaded():
    # 账号数(2) < 并发数(4)：两账号各 2 个在途，不允许 3:1 的倾斜
    pool = make_pool(6, ["A", "B"])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    accts = [pool[st.resolve("default")[0]]["account"] for _ in range(4)]
    assert accts.count("A") == 2 and accts.count("B") == 2, accts
    print("OK overflow least-loaded ->", accts)


def test_lone_session_follows_window():
    pool = [{"id": "m%d" % i, "account": "A%d" % i} for i in range(8)]
    st = State(pool, rotate_s=1, cooldown_s=0, rotate_on_first=True)
    got = []
    for _ in range(3):
        i = st.resolve("a")[0]
        got.append(i)
        st.release(i)
        time.sleep(1.1)
    assert got == [0, 1, 2], got
    print("OK lone session follows window:", got)


def test_fallback_to_model_id_when_no_account():
    # 不给 account：一个模型自成一组，并发仍应分片到不同模型
    pool = [{"id": "m%d" % i} for i in range(6)]
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    got = [st.resolve("default")[0] for _ in range(4)]
    assert len(set(got)) == 4, got
    print("OK fallback(model-id) sharding ->", got)


def test_release_does_not_leak():
    pool = make_pool(4, ["A", "B"])
    st = State(pool, rotate_s=600, cooldown_s=0, rotate_on_first=True)
    for _ in range(50):
        i = st.resolve("default")[0]
        st.release(i)
    assert st.inflight_count() == 0, st.inflight_count()
    assert st.inflight == {}, st.inflight
    print("OK release no leak -> inflight=0")


def test_cooldown_member_skipped():
    pool = make_pool(3, ["A", "B", "C"])
    st = State(pool, rotate_s=600, cooldown_s=60, rotate_on_first=True)
    i0 = st.resolve("default")[0]
    st.mark_fail(i0, "http_429")
    st.release(i0)
    i1 = st.resolve("default")[0]
    assert i1 != i0, (i0, i1)
    print("OK cooldown skipped ->", i0, "->", i1)


if __name__ == "__main__":
    test_default_session_concurrency_spreads()
    test_sequential_requests_stay_sticky()
    test_concurrent_overflow_uses_least_loaded()
    test_lone_session_follows_window()
    test_fallback_to_model_id_when_no_account()
    test_release_does_not_leak()
    test_cooldown_member_skipped()
    print("\nALL ACCOUNT-SHARDING TESTS PASSED")
