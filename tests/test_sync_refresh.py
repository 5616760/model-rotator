#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""快速刷新的上游同步回归测试（v1.2 新增）。

修的 bug：v1.1 的「快速刷新」只重读 config.json，从不联网，agent2api 上了
新模型池子里永远看不到。修复后 reload 先拉上游 /v1/models 对齐池子（增删 +
写回 config.json + 自动备份），再重建 runtime。

测试覆盖：
1) 上游有新增/下线 -> 池同步更新、config.json 写回、留备份、家族分继承；
2) 已一致时再刷新 -> 不产生新备份（幂等，防止保活场景堆文件）；
3) 上游不可达 -> reload 整体仍成功，退化为纯配置重载（sync.ok=false）；
4) 家族/账号推断口径与 sync_pool.py 完全一致（防止两处实现漂移）。
"""
import glob
import json
import os
import shutil
import sys
import tempfile
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rotator  # noqa: E402

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
TMP = None
HTTPD = None
PORT = None

# 模拟 agent2api 的上游清单，用例可改
UPSTREAM_MODELS = []


class MockGateway(BaseHTTPRequestHandler):
    """最小网关：GET /v1/models 返回 UPSTREAM_MODELS；POST chat 回固定 JSON。"""

    def log_message(self, fmt, *args):       # 静音（签名必须可变参数）
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.split("?")[0].rstrip("/") == "/v1/models":
            self._json(200, {"object": "list",
                             "data": [{"id": m, "object": "model"}
                                      for m in UPSTREAM_MODELS]})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        # 同步路径不调 chat，但保底回一个合法补全，避免意外挂死
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        self._json(200, {"id": "x", "model": "mock",
                         "choices": [{"message": {"content": "ok"}}]})


ROT_PORT = None      # rotator 实例端口（与 mock 网关分开，避免互相打错）


def call(path, method="GET", payload=None, timeout=30):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (ROT_PORT, path),
                                 data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    with OPENER.open(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", "replace")
    return json.loads(raw) if raw else {}


def start_rotator():
    cfg = rotator.build_config()
    st = rotator.State(cfg["_pool"], rotate_s=1800, cooldown_s=600,
                       rotate_on_first=True, quality_priority=True)
    rotator.install_runtime(cfg, st)
    rotator.log.setLevel(100)

    global ROT_PORT
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), rotator.Handler)
    ROT_PORT = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, ROT_PORT


def setup_module():
    global TMP, HTTPD, PORT
    TMP = tempfile.mkdtemp()
    rotator.DEFAULT_CFG = os.path.join(TMP, "config.json")
    rotator.OVERRIDE_CFG = os.path.join(TMP, "overrides.json")
    gw = ThreadingHTTPServer(("127.0.0.1", 0), MockGateway)
    global GW_PORT
    GW_PORT = gw.server_address[1]
    threading.Thread(target=gw.serve_forever, daemon=True).start()
    HTTPD = gw
    PORT = GW_PORT          # 池直接指向 mock 网关


def teardown_module():
    HTTPD.shutdown()
    HTTPD.server_close()
    shutil.rmtree(TMP, ignore_errors=True)


def write_cfg(pool, scores=None):
    cfg = {
        "listen_host": "127.0.0.1", "listen_port": 3166,
        "rotate_minutes": 30, "cooldown_minutes": 10,
        "fail_status": [429], "fail_on_exception": True,
        "rotate_on_first_request": True, "expose_as": "AutoRotate-Test",
        "quality_priority": True,
        "pool": [{"id": m, "url": "http://127.0.0.1:%d/v1/chat/completions"
                  % GW_PORT, "key": "k", "account": "acct"} for m in pool],
    }
    if scores:
        cfg["quality_scores"] = scores
    with open(rotator.DEFAULT_CFG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def backups():
    return sorted(glob.glob(os.path.join(TMP, "config.json.bak-*")))


# --------------------------------------------------------------------- tests  #
def test_sync_adds_and_removes_with_backup_and_family_score():
    global UPSTREAM_MODELS
    write_cfg(["old-a", "keep-1", "gone-1"], scores={"old-a": 80, "keep-1": 77})
    # 上游：old-a/keep-1 还在，gone-1 下线；新增 new-1、keep-1-free（同族）
    UPSTREAM_MODELS = ["old-a", "keep-1", "new-1", "keep-1-free"]
    httpd, port = start_rotator()
    try:
        r = call("/control/reload", "POST", {})
        assert r["ok"], r
        s = r["sync"]
        assert s["ok"], s
        assert sorted(s["added"]) == ["keep-1-free", "new-1"], s
        assert s["removed"] == ["gone-1"], s
        assert s["upstream_n"] == 4, s
        assert s["backup"], "必须留下备份文件"

        # config.json 被写回：gone-1 没了，新成员带 url/key/account
        raw = json.load(open(rotator.DEFAULT_CFG, encoding="utf-8"))
        ids = {e["id"] if isinstance(e, dict) else e for e in raw["pool"]}
        assert ids == {"old-a", "keep-1", "new-1", "keep-1-free"}, ids
        new_e = [e for e in raw["pool"] if isinstance(e, dict)
                 and e["id"] == "keep-1-free"][0]
        assert new_e["url"].endswith("/v1/chat/completions"), new_e
        assert new_e["key"] == "k", "鉴权写法必须抄自同组现有成员"
        assert raw["quality_scores"]["new-1"] == 50.0, "无先例给 50"
        assert raw["quality_scores"]["keep-1-free"] == 77.0, "同族继承最高分"

        # runtime 已换新：/status 能看到新成员
        snap = call("/status")
        snap_ids = {m["id"] for m in snap["models"]}
        assert "keep-1-free" in snap_ids and "gone-1" not in snap_ids, snap_ids

        assert len(backups()) == 1, backups()
        print("OK 增删同步 + 写回 + 备份 + 家族分继承")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_sync_idempotent_no_extra_backup():
    global UPSTREAM_MODELS
    UPSTREAM_MODELS = ["old-a", "keep-1", "new-1", "keep-1-free"]
    write_cfg(["old-a", "keep-1", "new-1", "keep-1-free"])
    n_before = len(backups())
    httpd, port = start_rotator()
    try:
        r = call("/control/reload", "POST", {})
        assert r["ok"], r
        assert "已与上游一致" in r["sync"]["message"], r["sync"]
        assert len(backups()) == n_before, "无变化不得新增备份文件"
        print("OK 池一致时幂等，不产生新备份")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_upstream_down_falls_back_to_plain_reload():
    global UPSTREAM_MODELS
    write_cfg(["solo-1"])
    UPSTREAM_MODELS = ["solo-1"]
    httpd, port = start_rotator()
    try:
        # 模拟 agent2api 挂掉：直接关掉 mock 网关的监听
        HTTPD.shutdown()
        r = call("/control/reload", "POST", {})
        assert r["ok"], "上游挂了不该让刷新整体失败: %s" % r
        assert not r["sync"]["ok"], r["sync"]
        assert r["sync"]["error"], r["sync"]
        # 池保持原状继续服务
        snap = call("/status")
        assert [m["id"] for m in snap["models"]] == ["solo-1"], snap
        print("OK 上游不可达 -> 退化纯重载，池保持原状")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_family_and_account_parity_with_sync_pool():
    """rotator 内嵌的家族/账号推断必须与 sync_pool.py 逐 id 一致。"""
    sys.path.insert(0, HERE)
    import sync_pool
    ids = ["wb-hy3", "wai-hy3", "hy3", "hy3-b", "hy3-c", "hy4-preview",
           "catpaw-glm-5.3-flash", "LongCat-2.0", "longcat-2.5-preview-free",
           "longcat-2.5-preview-free-solo", "sn-sensenova-6-8-flash",
           "qwen3-30b-a3b", "Qwen3.8-Flash", "glm-5.3-flash-free",
           "deepseek-v4.1-flash-0930", "mimo-v2.6-flash-free",
           "nemotron-3.5-super-vl-free", "space-bunny-free",
           "kimi-k2.6", "auto", "orcaterm-glm-5.3",
           "wb-step-5-preview", "fledge-alpha-free-solo"]
    for mid in ids:
        assert rotator._sync_family(mid) == sync_pool.family(mid), \
            "family 漂移: %s: %r vs %r" % (mid, rotator._sync_family(mid),
                                          sync_pool.family(mid))
        assert rotator._sync_account(mid) == sync_pool.guess_account(mid), \
            "account 漂移: %s" % mid
    print("OK 家族/账号推断与 sync_pool.py 逐 id 一致（%d 个样本）" % len(ids))


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
    print("-" * 60)
    print("上游同步测试: %s（%d 个用例）"
          % ("ALL PASS" if not failed else "FAIL %d" % failed, len(tests)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
