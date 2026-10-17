#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WorkBuddy 模型自动轮换代理 (Model Rotator Proxy)
================================================

背景：WorkBuddy 里多个自定义模型各自会被供应商限流(429)。本代理在它们之上
再放一层：客户端(WorkBuddy)只选一个 "AutoRotate-30m" 模型，请求由本代理按
「时间窗轮换 + 失败自动跳过并冷却」路由到池内的真实模型。

设计要点（对齐 WorkBuddy 现有自定义模型条目，已实测其 models.json 结构）：
- 上游即普通 OpenAI 兼容端点；WorkBuddy 走标准协议 (useCustomProtocol=false)。
- 逐条模型携带各自的 base_url 与 api_key；转发时替换 body 的 model 字段并带
  对应 Authorization。
- 完整流式透传 SSE / chunked；非流式透传 JSON。
- 只读 workbuddy 的 models.json，绝不写它；集成靠新增一条模型条目。

路由策略：
1) 时间窗：每 rotate_minutes 切到池内下一个模型；同一会话在窗内粘住同一模型，
   避免上下文跨模型跳变。
2) 失败转移：命中 429/5xx 或异常 → 该模型进入 cooldown_minutes 冷却，立刻改投
   池内下一个可用模型。
3) 池内全部冷却 → 清除冷却重试一轮；仍不行则返回 429/502 并带 X-Rotate-Reason。

无第三方依赖，仅用 Python 标准库。
"""

import base64
import json
import logging
import os
import re
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CFG = os.path.join(HERE, "config.json")
SESSION_TTL = 6 * 3600          # 会话 ID 多久不活动就重绑模型（秒）
MAX_REQ_BYTES = 64 * 1024 * 1024  # 请求体上限，超出直接拒（防内存爆）

log = logging.getLogger("rotator")
START_TS = time.time()
HOP_BY_HOP = {"content-length", "transfer-encoding", "connection", "keep-alive",
              "proxy-authenticate", "proxy-authorization", "te", "trailer",
              "upgrade", "content-encoding"}


def setup_log(path):
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    hs = []
    try:
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setFormatter(fmt)
        hs.append(fh)
    except Exception as e:
        print("[rotator] 日志文件不可写 %s: %s" % (path, e), file=sys.stderr)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    hs.append(sh)
    for h in hs:
        log.addHandler(h)
    log.propagate = False


# --------------------------------------------------------------------------- config  #
def load_raw(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_config():
    cfg = load_raw(DEFAULT_CFG)
    src = cfg.get("source_models_json",
                  os.path.expanduser("~/.workbuddy/models.json"))
    models = load_raw(src)
    by_id = {m["id"]: m for m in models}
    pool = []
    for entry in cfg["pool"]:
        # 内联成员: {"id":..., "url":..., "key_from":"<models.json 中的 id>"}
        # key_from 让密钥继续只存在 models.json，不复制到 config.json。
        if isinstance(entry, dict) and "url" in entry:
            src_key = ""
            ref = entry.get("key_from")
            if ref:
                if ref not in by_id:
                    raise SystemExit("key_from 指向的模型不在 models.json: %s" % ref)
                src_key = by_id[ref].get("apiKey") or ""
            pool.append({
                "id": entry["id"],
                "url": entry["url"].rstrip("/") + "/chat/completions"
                      if "/chat/completions" not in entry["url"] else entry["url"],
                "key": entry.get("key", "") or src_key,
                "meta": entry.get("meta", {}),
            })
            continue
        m = by_id.get(entry)
        if not m:
            raise SystemExit("models.json 里找不到池成员: %s" % entry)
        pool.append({
            "id": entry,
            "url": m["url"].rstrip("/") + "/chat/completions",
            "key": m.get("apiKey") or "",
            "meta": {
                "vendor": m.get("vendor"),
                "supportsToolCall": m.get("supportsToolCall"),
                "supportsImages": m.get("supportsImages"),
                "supportsReasoning": m.get("supportsReasoning"),
                "useCustomProtocol": m.get("useCustomProtocol"),
                "reasoning": m.get("reasoning"),
            },
        })
    if not pool:
        raise SystemExit("池为空，检查 config.json 的 pool")
    cfg["_pool"] = pool
    cfg["_src"] = src
    return cfg


CFG = None  # main 里填充


# --------------------------------------------------------------------------- state   #
class State:
    """时间窗轮换 + 会话粘性 + 失败冷却。全部用锁保证线程安全。"""

    def __init__(self, pool, rotate_s, cooldown_s, rotate_on_first):
        self.lock = threading.Lock()
        self.pool = pool
        self.n = len(pool)
        self.rotate_s = rotate_s
        self.cooldown_s = cooldown_s
        self.rotate_on_first = rotate_on_first
        self.cur = 0
        self.win_start = time.time()
        self.cooldown = {}          # idx -> until_ts
        self.sessions = OrderedDict()  # sid -> idx
        self.stats = {}              # idx -> {ok, fail, fail_types:{}}
        for i in range(self.n):
            self.stats[i] = {"ok": 0, "fail": 0, "fail_types": {}}

    # ---- 当前应使用的池下标（按时间窗，空闲跨多个窗口会一次性补齐） ----
    def _window_locked(self):
        now = time.time()
        elapsed = now - self.win_start
        if elapsed >= self.rotate_s:
            steps = int(elapsed // self.rotate_s)
            self.cur = (self.cur + steps) % self.n
            self.win_start += steps * self.rotate_s
            log.info("时间窗推进 %d 步，轮换 -> %s", steps, self.pool[self.cur]["id"])
        return self.cur

    def _avail_locked(self, idx):
        cd = self.cooldown.get(idx, 0)
        return cd == 0 or time.time() >= cd

    def _pick_locked(self, target):
        """从 target 起，沿池顺序找第一个可用且不在冷却的下标。"""
        now = time.time()
        expired = [i for i, until in self.cooldown.items() if now >= until]
        for i in expired:
            self.cooldown.pop(i, None)
            log.info("冷却结束，恢复 %s", self.pool[i]["id"])
        for off in range(self.n):
            i = (target + off) % self.n
            if self._avail_locked(i):
                return i
        return None

    def current(self):
        with self.lock:
            return self.pool[self._window_locked()]["id"]

    # ---- 决定本次请求路由到下标 ----
    def resolve(self, sid):
        with self.lock:
            self._window_locked()
            target = self.cur
            sticky = self.sessions.get(sid)
            if sticky is not None:
                if self._avail_locked(sticky):
                    self.sessions.move_to_end(sid)
                    return sticky, False
                target = sticky   # 从原绑定继续往后找，尽量贴近会话上下文
            elif not self.rotate_on_first:
                target = 0
            idx = self._pick_locked(target)
            cleared = False
            if idx is None:        # 池内全冷却 -> 清冷却重试一轮
                log.warning("池内全部处于冷却，清除冷却后重试一轮")
                self.cooldown.clear()
                idx = self._pick_locked(target)
                cleared = True
            if idx is None:
                idx = self.cur
            if sid is not None:
                self.sessions[sid] = idx
                while len(self.sessions) > 4096:
                    self.sessions.popitem(last=False)
            return idx, cleared

    # ---- 失败：置冷却并给出下一候选 ----
    def mark_fail(self, idx, kind):
        with self.lock:
            st = self.stats[idx]
            st["fail"] += 1
            st["fail_types"][kind] = st["fail_types"].get(kind, 0) + 1
            self.cooldown[idx] = time.time() + self.cooldown_s
            log.info("模型 %s 失败(%s)，冷却 %ds",
                     self.pool[idx]["id"], kind, int(self.cooldown_s))
            expired = [i for i, until in self.cooldown.items()
                       if time.time() >= until]
            for i in expired:
                self.cooldown.pop(i, None)
            order = []
            for off in range(self.n):
                i = (idx + 1 + off) % self.n
                if self._avail_locked(i):
                    order.append(i)
            return order

    def mark_ok(self, idx):
        with self.lock:
            self.stats[idx]["ok"] += 1

    def evict(self, sid):
        """上游返回 404 model_not_found 等不可恢复错误：把该模型移出池。"""
        with self.lock:
            if 0 <= sid < self.n:
                # 冷却设成一个很大值，等同于本轮内彻底停用
                self.cooldown[sid] = time.time() + 3650 * 86400
                log.warning("模型 %s 返回不可恢复错误，已从路由池移除(本次运行内)",
                            self.pool[sid]["id"])

    def snapshot(self):
        now = time.time()
        with self.lock:
            self._window_locked()   # 状态查询也驱动时间窗，保证读数准确
            out = []
            for i, m in enumerate(self.pool):
                cd = self.cooldown.get(i, 0)
                out.append({
                    "id": m["id"],
                    "active": i == self.cur,
                    "cooldown_remaining": max(0, round(cd - now)) if cd else 0,
                    "ok": self.stats[i]["ok"],
                    "fail": self.stats[i]["fail"],
                    "fail_types": self.stats[i]["fail_types"],
                })
            nxt = self.win_start + self.rotate_s - now
            return {
                "active": self.pool[self.cur]["id"],
                "index": self.cur,
                "pool_size": self.n,
                "rotate_minutes": round(self.rotate_s / 60),
                "cooldown_minutes": round(self.cooldown_s / 60),
                "seconds_to_next_rotate": max(0, round(nxt)),
                "session_stickiness": len(self.sessions),
                "uptime_seconds": round(now - START_TS),
                "models": out,
            }


ST = None  # main 里填充


# --------------------------------------------------------------------------- helpers #
def load_dotenv(path):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_dotenv_path = os.path.join(HERE, ".env")
if os.path.exists(_dotenv_path):
    load_dotenv(_dotenv_path)


def is_secret_header(h):
    return h.lower() in ("authorization", "x-api-key", "api-key", "proxy-authorization")


def scrub(text):
    """打日志前去掉 api key，防泄露。"""
    if not text:
        return text
    text = re.sub(r"(sk-[A-Za-z0-9_\-]{4})[A-Za-z0-9_\-]+", r"\1****", text)
    text = re.sub(r"(Bearer\s+)[A-Za-z0-9._\-]{6,}", r"\1****", text, flags=re.I)
    return text


def build_upstream_request(url, body_bytes, auth_key, extra_headers):
    headers = {}
    for k, v in extra_headers.items():
        lk = k.lower()
        if lk in HOP_BY_HOP or is_secret_header(k) or lk == "accept-encoding":
            continue
        headers[k] = v
    headers["Content-Type"] = "application/json"
    headers["Accept"] = "application/json"
    # 强制 identity：urllib 不解压，若上游回 gzip 会破坏透传，这里统一要明文
    headers["Accept-Encoding"] = "identity"
    if auth_key:
        headers["Authorization"] = "Bearer " + auth_key
    req = urllib.request.Request(url, data=body_bytes, headers=headers, method="POST")
    return req


# urllib 默认读 HTTP(S)_PROXY 环境变量，会把发往 127.0.0.1:3065 的请求也绕出去，
# 导致上游连接被拒。统一用 no-proxy opener，且 SSL context 必须装进 opener
# （OpenerDirector.open() 不接受 context 关键字参数）。
_SSL_CTX = ssl.create_default_context()
_OPENER = urllib.request.build_opener(
    urllib.request.ProxyHandler({}),
    urllib.request.HTTPSHandler(context=_SSL_CTX),
)


def urlopen_direct(req, timeout):
    """忽略环境代理直连上游（127.0.0.1 的 agent2api 必须直连）。"""
    return _OPENER.open(req, timeout=timeout)


# --------------------------------------------------------------------------- handler #
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "ModelRotator/1.0"

    def log_message(self, fmt, *args):
        log.info("http " + fmt % args)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ---- OpenAI 模型列表：只暴露一个 AutoRotate ----
    def do_GET(self):
        if self.path.startswith("/v1/models"):
            now = int(time.time())
            data = [{
                "id": CFG["expose_as"], "object": "model", "created": now,
                "owned_by": "model-rotator",
            }]
            payload = json.dumps({"object": "list", "data": data}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path in ("/", "/health", "/healthz"):
            body = {"ok": True, "active_model": ST.current(),
                    "uptime_seconds": round(time.time() - START_TS)}
            self._json(200, body)
            return
        if self.path in ("/status", "/v1/rotator/status"):
            self._json(200, ST.snapshot())
            return
        self._json(404, {"error": {"message": "not found", "type": "http_error"}})

    def _json(self, code, obj):
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        if not self.path.startswith("/v1/chat/completions"):
            self._json(404, {"error": {"message": "unsupported endpoint: %s" % self.path,
                                       "type": "http_error"}})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length > MAX_REQ_BYTES:
            self._json(413, {"error": {"message": "request body too large",
                                       "type": "invalid_request_error"}})
            return
        raw = self.rfile.read(length) if length else b""

        # 解析并改写 model
        try:
            parsed = json.loads(raw.decode("utf-8")) if raw else {}
            if not isinstance(parsed, dict):
                raise ValueError("body is not a JSON object")
        except Exception as e:
            self._json(400, {"error": {"message": "invalid JSON body: %s" % e,
                                       "type": "invalid_request_error"}})
            return

        sid = (self.headers.get("X-Rotate-Session")
               or self.headers.get("X-Session-Id") or parsed.get("user") or "default")
        stream = bool(parsed.get("stream"))

        # 路由：先取候选顺序（首个 + 失败转移链）
        idx, cleared = ST.resolve(sid)
        tried = []
        attempt_chain = [idx]

        last_err = None
        for probe_i in range(len(ST.pool) + 2):
            if not attempt_chain:
                break
            cur_idx = attempt_chain.pop(0)
            member = ST.pool[cur_idx]
            parsed["model"] = member["id"]
            body_bytes = json.dumps(parsed, ensure_ascii=False).encode("utf-8")

            extra = {k: v for k, v in self.headers.items()}
            req = build_upstream_request(
                member["url"], body_bytes, member["key"], extra)

            t0 = time.time()
            try:
                resp = urlopen_direct(req, timeout=600)
            except urllib.error.HTTPError as e:
                tried.append((member["id"], "http_%s" % e.code))
                log.warning("upstream %s HTTP %s (%.1fs): %s",
                            member["id"], e.code, time.time() - t0,
                            scrub(e.read(400).decode("utf-8", "replace"))[:400])
                if e.code in CFG["fail_status"]:
                    nxt = ST.mark_fail(cur_idx, "http_%s" % e.code)
                    attempt_chain = nxt
                    continue
                # 不可恢复错误：404 model_not_found 等，移出池
                if e.code == 404:
                    ST.evict(cur_idx)
                self._relay_error(e)
                return
            except (urllib.error.URLError, TimeoutError, ssl.SSLError) as e:
                tried.append((member["id"], "exception"))
                reason = getattr(e, "reason", e)
                log.warning("upstream %s 连接异常 (%.1fs): %s",
                            member["id"], time.time() - t0, scrub(str(reason))[:300])
                if CFG["fail_on_exception"]:
                    nxt = ST.mark_fail(cur_idx, "exception")
                    attempt_chain = nxt
                    continue
                self._json(502, {"error": {"message": "upstream connect error: %s" % reason,
                                           "type": "upstream_error"}})
                return

            # 成功：透传
            ST.mark_ok(cur_idx)
            self._relay(resp, stream)
            with ST.lock:
                bound = ST.sessions.get(sid)
            log.info("routed sid=%s model=%s stream=%s tools=%s attempts=%s %s",
                     (sid or "")[:18], member["id"], stream,
                     bool(parsed.get("tools")), len(tried) + 1,
                     "OK %.1fs" % (time.time() - t0))
            return

        # 全部候选失败
        self._json(429, {"error": {
            "message": "所有池模型均失败或冷却中，已尝试: %s" %
                       ", ".join("%s(%s)" % t for t in tried),
            "type": "rate_limit_error",
            "code": "all_models_unavailable"}})

    # ---- 透传上游响应（流式逐块，非流式读满） ----
    def _relay(self, resp, stream):
        # 上游的 Content-Length / Transfer-Encoding 必须丢弃，由本层按实际转发方式
        # 重新计算；沿用上游头会和下面手动设置的长度冲突，导致客户端收到畸形响应。
        SKIP = HOP_BY_HOP | {"set-cookie", "content-length"}
        try:
            self.send_response(resp.status)
            for k, v in resp.getheaders():
                if k.lower() in SKIP:
                    continue
                self.send_header(k, v)
            if stream:
                self.send_header("Content-Type",
                                 resp.getheader("Content-Type",
                                                "text/event-stream") or "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Accel-Buffering", "no")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                while True:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    self.wfile.write(b"%x\r\n" % len(chunk))
                    self.wfile.write(chunk)
                    self.wfile.write(b"\r\n")
                    self.wfile.flush()
                self.wfile.write(b"0\r\n\r\n")
            else:
                data = resp.read()
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            self.close_connection = True
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            try:
                resp.close()
            except Exception:
                pass

    def _relay_error(self, e):
        try:
            body = e.read()
        except Exception:
            body = b'{"error":{"message":"upstream error","type":"upstream_error"}}'
        try:
            self.send_response(e.code)
            for k, v in (e.headers.items() if e.headers else []):
                if k.lower() in HOP_BY_HOP or k.lower() in ("set-cookie", "content-length"):
                    continue
                self.send_header(k, v)
            # 长度由本层统一计算，避免与上游头冲突
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


# --------------------------------------------------------------------------- main    #
def probe_health(host, port, timeout=2):
    """探测本机端口是否已有健康的 rotator 实例在服务。"""
    try:
        with _OPENER.open("http://%s:%d/health" % (host, port), timeout=timeout) as r:
            if r.status != 200:
                return False
            data = json.loads(r.read().decode("utf-8") or "{}")
            return bool(data.get("ok"))
    except Exception:
        return False


def main():
    global CFG, ST
    cfg = build_config()
    CFG = cfg
    setup_log(cfg.get("log_file") or os.path.join(HERE, "rotator.log"))
    ST = State(
        pool=cfg["_pool"],
        rotate_s=float(cfg.get("rotate_seconds") or cfg["rotate_minutes"] * 60),
        cooldown_s=float(cfg.get("cooldown_seconds") or cfg["cooldown_minutes"] * 60),
        rotate_on_first=bool(cfg.get("rotate_on_first", True)),
    )
    host, port = cfg["listen_host"], int(cfg["listen_port"])
    rot_s = ST.rotate_s
    log.info("模型轮换代理启动 http://%s:%d  池=%s  窗口=%.1f分钟  冷却=%.1f分钟  对外=%s",
             host, port, [m["id"] for m in cfg["_pool"]],
             rot_s / 60, ST.cooldown_s / 60, cfg["expose_as"])
    log.info("模型清单源(只读): %s", cfg["_src"])

    # 单实例保护：计划任务每 5 分钟触发一次，若上一实例仍在跑则直接退出，
    # 避免端口互抢。allow_reuse_address 交给父类，此处先做一次健康探测。
    if probe_health(host, port):
        log.info("检测到 %s:%d 已有健康实例在运行，本次启动放弃", host, port)
        return

    try:
        srv = ThreadingHTTPServer((host, port), Handler)
    except OSError as e:
        log.error("无法绑定 %s:%d (%s)；可能已有实例占用", host, port, e)
        return
    log.info("监听已就绪 http://%s:%d", host, port)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log.info("收到中断信号，退出")
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
