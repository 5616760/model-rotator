#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""控制面回归测试：手动切换 / 快速刷新 / 权重赋值 / 池成员启停。

覆盖两条最容易坏的路径，都是 v1.1 新加的：

1) **热重载的 runtime 原子性**：reload 换的是全局 (cfg, st) 引用，单个请求
   全程只用进来时取到的那一对。若实现成「先换 cfg 再换 st」，重载瞬间的请求
   就会拿旧下标访问新池 → 越界或路由到不存在的模型。
2) **overrides.json 的分层语义**：quality_scores 必须按模型 id **逐项合并**，
   否则改一个模型的权重会把其余模型冲回 config.json 的旧值。

上游全部用 MockUpstream，用「实际收到请求的模型」和「墙钟时间」判定，不猜。
"""
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rotator  # noqa: E402
from mock_upstream import MockUpstream  # noqa: E402

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

MODELS = ["m-alpha", "m-beta", "m-gamma"]
TMP = None          # 测试专属的 config.json / overrides.json 所在目录


# ------------------------------------------------------------------ helpers  #
def write_cfg(pool_ids, scores=None, priority=True):
    """写一份 config.json 到临时目录，池全部指向 mock 上游。"""
    global TMP
    cfg = {
        "listen_host": "127.0.0.1",
        "listen_port": 3166,
        "rotate_minutes": 30,
        "cooldown_minutes": 10,
        "fail_status": [408, 409, 425, 429, 500, 502, 503, 504],
        "fail_on_exception": True,
        "rotate_on_first_request": True,
        "expose_as": "AutoRotate-Test",
        "quality_priority": priority,
        "pool": [{"id": m, "url": UPSTREAM + "/chat/completions", "key": "k",
                  "account": "acct-" + m} for m in pool_ids],
    }
    if scores:
        cfg["quality_scores"] = scores
    with open(rotator.DEFAULT_CFG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def call(port, path, method="GET", payload=None, timeout=20):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (port, path),
                                 data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with OPENER.open(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        # 控制面用 4xx/5xx 表达「操作被拒绝」，urllib 会抛 HTTPError；
        # 读掉 body 再解析，测试才能断言到 error 文案。
        raw = e.read().decode("utf-8", "replace")
        code = e.code
        try:
            obj = json.loads(raw) if raw else {}
        except ValueError:
            obj = {"ok": False, "raw": raw}
        obj["_http_code"] = code
        return obj
    try:
        return json.loads(raw) if raw else {}
    except ValueError:
        return {"ok": True, "raw": raw}


def start_rotator():
    """起 rotator；DEFAULT_CFG/OVERRIDE_CFG 已被测试改指向临时目录。"""
    cfg = rotator.build_config()
    st = rotator.State(
        cfg["_pool"], rotate_s=1800, cooldown_s=600, rotate_on_first=True,
        quality_priority=bool(cfg.get("quality_priority", True)))
    rotator.install_runtime(cfg, st)
    # 测试里不让 rotator 的日志刷屏（也不写真实 rotator.log）
    rotator.log.setLevel(100)
    rotator.log.propagate = False

    class Quiet(rotator.Handler):
        def log_message(self, fmt, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Quiet)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, port


def chat(port, model="AutoRotate-Test", sid=None, timeout=30):
    body = {"model": model, "stream": False,
            "messages": [{"role": "user", "content": "ping"}]}
    req = urllib.request.Request(
        "http://127.0.0.1:%d/v1/chat/completions" % port,
        data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    if sid:
        req.add_header("X-Rotate-Session", sid)
    with OPENER.open(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


# ------------------------------------------------------------------- setup   #
UPSTREAM = None
_MOCK = None


def setup_module():
    global TMP, UPSTREAM, _MOCK
    TMP = tempfile.mkdtemp(prefix="mr-ctl-")
    rotator.DEFAULT_CFG = os.path.join(TMP, "config.json")
    rotator.OVERRIDE_CFG = os.path.join(TMP, "overrides.json")
    _MOCK = MockUpstream({m: {"delay": 0, "status": 200} for m in MODELS}).start()
    UPSTREAM = _MOCK.base


def teardown_module():
    if _MOCK:
        _MOCK.stop()
    if TMP and os.path.isdir(TMP):
        shutil.rmtree(TMP, ignore_errors=True)


def fresh(pool_ids=None, scores=None, priority=True):
    """每个用例都从干净的 config/overrides 起，避免互相污染。"""
    global TMP
    for p in (rotator.DEFAULT_CFG, rotator.OVERRIDE_CFG):
        if os.path.exists(p):
            os.remove(p)
    _MOCK.reset_hits()      # mock 是跨用例共享的，命中计数必须归零
    write_cfg(pool_ids or MODELS, scores=scores, priority=priority)
    return start_rotator()


# ------------------------------------------------------------------- tests   #
def test_console_serves_html():
    httpd, port = fresh()
    try:
        req = urllib.request.Request("http://127.0.0.1:%d/console" % port)
        with OPENER.open(req, timeout=10) as r:
            html = r.read().decode("utf-8")
        assert "<title>ModelRotator" in html, "console html missing"
        assert "control/switch" in html, "console has no switch action"
        assert "control/weights" in html, "console has no weight action"
        print("OK /console serves self-contained html (%d bytes)" % len(html))
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_switch_forces_model_and_breaks_stickiness():
    httpd, port = fresh(scores={"m-alpha": 99, "m-beta": 10, "m-gamma": 10})
    try:
        # 质量优先 + 空闲 => 首请求必然落在 m-alpha
        first = chat(port, sid="s1")["model"]
        assert first == "m-alpha", first
        assert _MOCK.hits_of("m-beta") == 0

        r = call(port, "/control/switch", "POST", {"model": "m-beta"})
        assert r["ok"], r
        assert r["to"] == "m-beta", r
        # 关键：粘性会话必须一起释放，否则切换看起来没生效
        got = chat(port, sid="s1")["model"]
        assert got == "m-beta", got
        # 再发一次仍应在 m-beta（新窗内粘住）
        assert chat(port, sid="s1")["model"] == "m-beta"
        print("OK switch: %s -> m-beta, sticky session released + re-bound" % first)
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_switch_unknown_model_rejected():
    httpd, port = fresh()
    try:
        r = call(port, "/control/switch", "POST", {"model": "no-such-model"})
        assert not r["ok"] and "不在池内" in r["error"], r
        print("OK switch unknown model -> 404 with reason")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_switch_to_disabled_model_rejected():
    httpd, port = fresh()
    try:
        call(port, "/control/disable", "POST", {"model": "m-gamma"})
        r = call(port, "/control/switch", "POST", {"model": "m-gamma"})
        assert not r["ok"] and "停用" in r["error"], r
        print("OK switch to disabled model -> rejected")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_next_advances_one_window():
    httpd, port = fresh(scores={"m-alpha": 99, "m-beta": 10, "m-gamma": 10})
    try:
        chat(port)  # -> m-alpha
        r = call(port, "/control/next", "POST", {})
        assert r["ok"] and r["from"] == "m-alpha", r
        assert r["to"] == "m-beta", r
        assert chat(port)["model"] == "m-beta"
        print("OK next: m-alpha -> m-beta")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_weights_change_routing_and_persist():
    httpd, port = fresh(scores={"m-alpha": 99, "m-beta": 10, "m-gamma": 10})
    try:
        assert chat(port)["model"] == "m-alpha"
        r = call(port, "/control/weights", "POST",
                 {"scores": {"m-alpha": 5, "m-gamma": 99}})
        assert r["ok"] and sorted(r["updated"]) == ["m-alpha", "m-gamma"], r
        assert r["persisted"] is True, r
        assert r["weights"]["m-gamma"] == 99.0, r
        # 持久化文件必须真的落盘
        ov = json.load(open(rotator.OVERRIDE_CFG, encoding="utf-8"))
        assert ov["quality_scores"] == {"m-alpha": 5.0, "m-gamma": 99.0}, ov
        # 下一请求应改投 m-gamma
        assert chat(port)["model"] == "m-gamma"
        print("OK weights: m-alpha 99->5, m-gamma 10->99, routing follows, file saved")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_weights_bad_values_ignored_unknown_reported():
    httpd, port = fresh()
    try:
        r = call(port, "/control/weights", "POST",
                 {"scores": {"m-alpha": 500, "m-beta": "abc", "ghost": 60}})
        assert r["ok"], r
        assert r["bad"] == ["m-alpha", "m-beta"] or set(r["bad"]) == {"m-alpha", "m-beta"}, r
        assert r["unknown"] == ["ghost"], r
        # 坏值不能污染已持久化内容
        ov = json.load(open(rotator.OVERRIDE_CFG, encoding="utf-8"))
        assert "m-alpha" not in ov["quality_scores"], ov
        print("OK weights validation: bad=%s unknown=%s" % (r["bad"], r["unknown"]))
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_overrides_merge_per_model_not_replace():
    """改一个模型的权重，不能把其余模型冲回 config.json 的旧值。"""
    httpd, port = fresh(scores={"m-alpha": 99, "m-beta": 10, "m-gamma": 10})
    try:
        call(port, "/control/weights", "POST", {"model": "m-beta", "score": 77})
        r = call(port, "/control/reload", "POST", {})
        assert r["ok"], r
        w = call(port, "/control/weights")["weights"]
        assert w["m-beta"] == 77.0 and w["m-alpha"] == 99.0 and w["m-gamma"] == 10.0, w
        print("OK overrides merge: m-beta=77 kept, m-alpha/gamma inherited from cfg")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_reload_picks_up_pool_change_atomically():
    """改 config.json 的池 -> reload 后新池生效，且不出现下标错配。"""
    httpd, port = fresh(scores={"m-alpha": 99})
    try:
        snap = call(port, "/status")
        assert snap["pool_size"] == 3, snap["pool_size"]
        # 只留一个成员：旧 runtime 的池下标 2 在新 runtime 里已越界
        write_cfg(["m-alpha"], scores={"m-alpha": 99})
        r = call(port, "/control/reload", "POST", {})
        assert r["ok"] and r["pool_size"] == 1 and r["removed"], r
        # 重载后连发若干次，全部命中唯一成员，无异常
        for _ in range(3):
            assert chat(port)["model"] == "m-alpha"
        snap = call(port, "/status")
        assert snap["pool_size"] == 1 and snap["inflight_total"] == 0, snap
        print("OK reload: pool 3 -> 1, removed=%s, no index mismatch" % r["removed"])
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_reload_survives_broken_config():
    """config.json 写坏不能把代理搞挂——沿用旧配置继续服务。"""
    httpd, port = fresh()
    try:
        with open(rotator.DEFAULT_CFG, "w", encoding="utf-8") as f:
            f.write("{ this is not json")
        r = call(port, "/control/reload", "POST", {})
        assert not r["ok"] and r.get("error"), r
        assert call(port, "/status")["pool_size"] == 3
        assert chat(port)["model"] in MODELS
        print("OK broken config rejected, old runtime keeps serving")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_enable_disable_removes_from_rotation():
    httpd, port = fresh(scores={"m-alpha": 99, "m-beta": 98, "m-gamma": 97})
    try:
        r = call(port, "/control/disable", "POST", {"model": "m-alpha"})
        assert r["ok"] and r["disabled"] == ["m-alpha"], r
        ov = json.load(open(rotator.OVERRIDE_CFG, encoding="utf-8"))
        assert ov["disabled_models"] == ["m-alpha"], ov
        snap = call(port, "/status")
        alpha = [m for m in snap["models"] if m["id"] == "m-alpha"][0]
        assert alpha["enabled"] is False, alpha
        # 停用后不再被路由到
        for _ in range(4):
            assert chat(port)["model"] != "m-alpha"
        r = call(port, "/control/enable", "POST", {"model": "m-alpha"})
        assert r["ok"] and r["disabled"] == [], r
        # 启用会释放会话粘性，新权重/新成员立刻参选
        assert chat(port)["model"] == "m-alpha"
        print("OK disable -> excluded from rotation, enable -> back, both persisted")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_switch_beats_quality_priority():
    """质量优先不能把手动切换抢回去——这是「切换看起来无效」的经典 bug。"""
    httpd, port = fresh(scores={"m-alpha": 99, "m-beta": 10, "m-gamma": 10})
    try:
        assert chat(port)["model"] == "m-alpha"
        r = call(port, "/control/switch", "POST", {"model": "m-gamma"})
        assert r["ok"], r
        snap = call(port, "/status")
        assert snap["pinned"] == "m-gamma", snap
        # 连发多次都必须停在 m-gamma（pin 压制质量分）
        for _ in range(3):
            assert chat(port)["model"] == "m-gamma"
        print("OK manual switch beats quality priority (pinned=%s)" % snap["pinned"])
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_pin_expires_when_window_advances():
    """手动指定只维持一个窗口；窗口到点交还轮换，不能永久钉死。"""
    httpd, port = fresh(scores={"m-alpha": 10, "m-beta": 10, "m-gamma": 99})
    try:
        call(port, "/control/switch", "POST", {"model": "m-alpha"})
        st = rotator.current_runtime()[1]
        assert st.pin is not None
        # 把窗口时间推到过去 => 下一次查询触发 _window_locked 推进一步
        with st.lock:
            st.win_start -= st.rotate_s + 1
        snap = call(port, "/status")
        assert snap["pinned"] is None, snap
        # 从 m-alpha(下标0) 推进一步 => 下标1 = m-beta
        assert snap["active"] == "m-beta", snap
        print("OK pin cleared on window advance, rotation resumes -> m-beta")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_clear_cooldown():
    httpd, port = fresh()
    try:
        st = rotator.current_runtime()[1]
        with st.lock:
            st.cooldown[0] = time.time() + 3600
        r = call(port, "/control/clear-cooldown", "POST", {})
        assert r["ok"] and r["cleared"] == 1, r
        with st.lock:
            assert not st.cooldown
        print("OK clear-cooldown: 1 cleared")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_settings_written_and_applied_after_reload():
    httpd, port = fresh()
    try:
        r = call(port, "/control/settings", "POST",
                 {"rotate_minutes": 7, "hedge_after_seconds": 3,
                  "quality_priority": False, "bogus_key": 1})
        assert r["ok"] and r["applied"]["rotate_minutes"] == 7, r
        assert "bogus_key" not in r["applied"], r
        ov = json.load(open(rotator.OVERRIDE_CFG, encoding="utf-8"))
        assert ov["rotate_minutes"] == 7 and "bogus_key" not in ov, ov
        call(port, "/control/reload", "POST", {})
        snap = call(port, "/status")
        assert snap["rotate_minutes"] == 7, snap
        # 关掉质量优先后按「从当前窗开始」的顺序取，即池内第一个可用成员
        st = rotator.current_runtime()[1]
        assert st.quality_priority is False
        assert chat(port)["model"] == "m-alpha"
        print("OK settings: rotate_minutes=7 + priority off applied after reload")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_reset_settings_restores_base_config():
    httpd, port = fresh(scores={"m-alpha": 99})
    try:
        call(port, "/control/weights", "POST", {"model": "m-alpha", "score": 1})
        assert os.path.exists(rotator.OVERRIDE_CFG)
        r = call(port, "/control/reset-settings", "POST", {})
        assert r["ok"], r
        assert not os.path.exists(rotator.OVERRIDE_CFG)
        w = call(port, "/control/weights")["weights"]
        assert w["m-alpha"] == 99.0, w
        print("OK reset-settings: overrides deleted, base config restored")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_runtime_snapshot_is_consistent_under_reload():
    """重载瞬间在跑的请求不能踩到「新旧 runtime 混搭」。

    构造 3 成员池 -> reload 成 1 成员池，同时并发发请求；若实现先换 cfg 后换
    st，必然有人拿旧 st 的下标 2 去访问只剩 1 个成员的池 -> IndexError/KeyError。
    """
    httpd, port = fresh(scores={"m-alpha": 99})
    errors = []
    stop = threading.Event()

    def hammer():
        while not stop.is_set():
            try:
                chat(port, timeout=30)
            except Exception as e:      # noqa: BLE001
                errors.append("%s: %s" % (type(e).__name__, e))
                return

    ts = [threading.Thread(target=hammer, daemon=True) for _ in range(4)]
    for t in ts:
        t.start()
    try:
        for i in range(6):
            write_cfg(MODELS if i % 2 == 0 else ["m-alpha"], scores={"m-alpha": 99})
            r = call(port, "/control/reload", "POST", {})
            assert r["ok"], r
            time.sleep(0.05)
    finally:
        stop.set()
        for t in ts:
            t.join(timeout=10)
    assert not errors, "requests hit torn runtime: %s" % errors[:3]
    assert rotator.current_runtime()[1].n == 1
    print("OK 4 concurrent requests across 6 reloads, zero torn-runtime errors")


def test_bootstrap_recovers_from_backup_not_example():
    """config.json 丢失时必须从备份恢复，不能静默生成示例配置。

    真实事故：config.json 被误删后，bootstrap 照 config.example.json 生成了
    一份 3 个假模型的演示配置——服务「正常启动」、日志无 ERROR，但池子整个
    变了。config.json 在 .gitignore 里，没有版本控制兜底，只能靠这个 guard。
    """
    keep = rotator.DEFAULT_CFG
    tmp = tempfile.mkdtemp(prefix="mr-boot-")
    try:
        rotator.DEFAULT_CFG = os.path.join(tmp, "config.json")
        # 造两个备份，确保选的是「最新」那个
        for name, pool in (("config.json.bak-old", ["old-a"]),
                           ("config.json.bak-new", ["new-a", "new-b"])):
            json.dump({"pool": [{"id": m, "url": "http://x/v1", "key": "k"}
                                for m in pool]},
                      open(os.path.join(tmp, name), "w", encoding="utf-8"))
        os.utime(os.path.join(tmp, "config.json.bak-old"), (1, 1))
        rotator.bootstrap_config()
        got = [m["id"] for m in json.load(open(rotator.DEFAULT_CFG,
                                               encoding="utf-8"))["pool"]]
        assert got == ["new-a", "new-b"], got
        print("OK missing config.json -> restored from NEWEST backup (%s)" % got)

        # 没有备份时才允许用示例模板
        for f in os.listdir(tmp):
            os.remove(os.path.join(tmp, f))
        rotator.bootstrap_config()
        cfg = json.load(open(rotator.DEFAULT_CFG, encoding="utf-8"))
        assert cfg["expose_as"] == "AutoRotate", cfg
        print("OK no backup -> example template used")
    finally:
        rotator.DEFAULT_CFG = keep
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    setup_module()
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:      # noqa: BLE001
            failed += 1
            print("FAIL %s: %s: %s" % (t.__name__, type(e).__name__, e))
            import traceback
            traceback.print_exc()
    teardown_module()
    print("-" * 60)
    print("控制面测试: %s（%d 个用例）"
          % ("ALL PASS" if not failed else "FAIL %d" % failed, len(tests)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
