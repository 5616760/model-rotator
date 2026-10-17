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
import faulthandler
import json
import logging
import os
import re
import socket
import ssl
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

if getattr(sys, "frozen", False):
    # PyInstaller onefile/onedir 打包运行：__file__ 指向 _MEIPASS 临时解压目录，
    # 真正的用户数据目录是 exe 所在目录。
    HERE = os.path.dirname(os.path.abspath(sys.executable))
else:
    HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CFG = os.path.join(HERE, "config.json")
SESSION_TTL = 6 * 3600          # 会话 ID 多久不活动就重绑模型（秒）
MAX_REQ_BYTES = 64 * 1024 * 1024  # 请求体上限，超出直接拒（防内存爆）

# 允许转发的 OpenAI 兼容端点。除 /v1/chat/completions 外，WorkBuddy 的部分
# agent/工具走新的 Responses API（/v1/responses）；上游 agent2api 同样支持，
# 两者请求体都用 model 字段、都支持 stream，故共用同一套路由与失败转移。
SUPPORTED_POST_PATHS = ("/v1/chat/completions", "/v1/responses")

UPSTREAM_TIMEOUT = 600          # 单次上游请求的 socket 超时（秒）
HEDGE_POLL = 0.02               # 看门狗轮询间隔（秒），只影响判定粒度，不影响延迟

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


def _resource_path(name):
    """返回捆绑数据文件（config.example.json 等）的实际路径。

    PyInstaller onefile 模式下捆绑数据在 sys._MEIPASS，onedir 在 exe 同目录的
    _internal；源码运行就在脚本目录。exe 打包时用 --add-data 指定。
    """
    base = getattr(sys, "_MEIPASS", HERE)
    p = os.path.join(base, name)
    if os.path.isfile(p):
        return p
    # 回退：源码目录 / exe 目录下的同名文件（onedir / 开发态）
    for d in (HERE, os.path.dirname(os.path.abspath(__file__))):
        p2 = os.path.join(d, name)
        if os.path.isfile(p2):
            return p2
    return None


def bootstrap_config():
    """确保 config.json 存在；不存在则用捆绑的 config.example.json 生成一份。"""
    if os.path.exists(DEFAULT_CFG):
        return DEFAULT_CFG
    example = _resource_path("config.example.json")
    if not example:
        raise SystemExit(
            "未找到 config.json，且无 config.example.json 模板可复制。\n"
            "请参照 README 手动创建 config.json。")
    try:
        with open(example, "r", encoding="utf-8") as f:
            data = json.load(f)
        with open(DEFAULT_CFG, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(
            "[rotator] 首次运行：已用 config.example.json 生成 %s\n"
            "           请编辑该文件，把 pool 里的 url/key 换成你自己的上游模型；\n"
            "           source_models_json 指向你的 models.json（WorkBuddy 默认）。"
            % DEFAULT_CFG,
            file=sys.stderr)
    except OSError as e:
        raise SystemExit("无法写入配置文件 %s: %s" % (DEFAULT_CFG, e))
    return DEFAULT_CFG


def build_config():
    bootstrap_config()
    cfg = load_raw(DEFAULT_CFG)
    src = cfg.get("source_models_json",
                  os.path.expanduser("~/.workbuddy/models.json"))
    scores = cfg.get("quality_scores") or {}

    # 按需加载 models.json：仅当池里有「字符串形式的模型 id」或「key_from 引用」
    # 时才需要它。全内联 url/key 的配置不需要外部文件（exe 分发场景友好）。
    needs_src = False
    for entry in cfg["pool"]:
        if isinstance(entry, str):
            needs_src = True
            break
        if isinstance(entry, dict) and entry.get("key_from"):
            needs_src = True
            break

    by_id = {}
    if needs_src:
        if not os.path.exists(src):
            raise SystemExit(
                "池里用了字符串模型 id 或 key_from，但找不到模型清单文件：%s\n"
                "请把 source_models_json 指向正确的 models.json，或改用内联写法\n"
                '{"id":"...","url":"...","key":"..."}。' % src)
        models = load_raw(src)
        by_id = {m["id"]: m for m in models}

    pool = []
    for entry in cfg["pool"]:
        # 轻量写法 "model-id@account"：给池成员标上游账号，并发时按账号分片。
        # 不写 @ 时账号默认等于模型 id（每个模型自成一组）。
        acc = None
        if isinstance(entry, str) and "@" in entry:
            entry, acc = entry.rsplit("@", 1)
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
                "account": entry.get("account") or acc or entry["id"],
                "quality": float(entry.get("quality", scores.get(entry["id"], 50))),
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
            "account": acc or entry,
            "quality": float(scores.get(entry, 50)),
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
        self.win_idx = 0            # 时间窗序号，每推进一个窗口 +1
        self.win_start = time.time()
        self.cooldown = {}          # idx -> until_ts
        self.sessions = OrderedDict()  # sid -> (idx, win_idx)
        self.inflight = {}           # idx -> 正在处理中的请求数
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
            self.win_idx += steps
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

    def _account_of(self, idx):
        return self.pool[idx].get("account") or self.pool[idx]["id"]

    def _account_load_locked(self):
        """账号 -> 在途请求数。并发任务是靠这个分散到不同账号的。"""
        load = {}
        for i, c in self.inflight.items():
            if c:
                a = self._account_of(i)
                load[a] = load.get(a, 0) + c
        return load

    def _pick_by_load_locked(self, start):
        """选一个池下标。双重优先：先保并发摊开，再在同等空闲里挑质量最高的。

        排序键（越小越好）:
          1) 账号在途数 —— 这是并发分片，必须最优先，否则多个任务全压一个账号；
          2) -质量分    —— 同等空闲时优先用更好的模型；
          3) 离 start 的距离 —— 时间窗轮换的体现，也是稳定 tie-break。

        为什么不能只按会话分片：客户端通常不带 X-Rotate-Session，所有请求都落
        在同一个默认会话上，按会话分片等于没分片。
        """
        load = self._account_load_locked()
        quality_first = bool(CFG and CFG.get("quality_priority", True))
        best, best_score = None, None
        for off in range(self.n):
            i = (start + off) % self.n
            if not self._avail_locked(i):
                continue
            acct_load = load.get(self._account_of(i), 0)
            q = self.pool[i].get("quality", 50)
            score = ((acct_load, -q if quality_first else 0, off) if quality_first
                     else (acct_load, off))
            if best_score is None or score < best_score:
                best, best_score = i, score
        return best

    def _acquire_locked(self, idx):
        self.inflight[idx] = self.inflight.get(idx, 0) + 1

    def acquire(self, idx):
        with self.lock:
            self._acquire_locked(idx)

    def release(self, idx):
        """请求结束（成功/失败/中断）必须调用，否则在途计数只增不减。"""
        with self.lock:
            c = self.inflight.get(idx, 0) - 1
            if c > 0:
                self.inflight[idx] = c
            else:
                self.inflight.pop(idx, None)

    def inflight_count(self):
        with self.lock:
            return sum(self.inflight.values())

    def current(self):
        with self.lock:
            return self.pool[self._window_locked()]["id"]

    def best(self):
        """质量最高且可用的模型（供 /status 与启动日志展示）。"""
        with self.lock:
            avail = [i for i in range(self.n) if self._avail_locked(i)]
            if not avail:
                return None
            i = max(avail, key=lambda k: (self.pool[k].get("quality", 50), -k))
            return self.pool[i]["id"]

    def pick_alternate(self, exclude):
        """看门狗对冲用：在 exclude 之外挑一路，优先「别的账号 + 质量高」。

        为什么强调换账号：对冲的目的是绕开某一个账号被限流/卡住，如果第二路
        落在同一账号上，大概率一起卡。当前请求已占用的账号在途数 > 0，所以按
        在途数升序排序天然会把其它账号排在前面。
        """
        with self.lock:
            load = self._account_load_locked()
            quality_first = bool(CFG and CFG.get("quality_priority", True))
            best, best_score = None, None
            for i in range(self.n):
                if i in exclude or not self._avail_locked(i):
                    continue
                acct_load = load.get(self._account_of(i), 0)
                q = self.pool[i].get("quality", 50)
                score = ((acct_load, -q if quality_first else 0, i) if quality_first
                         else (acct_load, i))
                if best_score is None or score < best_score:
                    best, best_score = i, score
            return best

    # ---- 决定本次请求路由到下标（原子地登记在途） ----
    def resolve(self, sid):
        """选模型并原子地把它记为在途；调用方结束时必须 release(idx)。"""
        with self.lock:
            self._window_locked()
            win_idx = self.win_idx
            entry = self.sessions.get(sid) if sid is not None else None
            if entry is not None:
                bound_idx, bound_win = entry
                # 粘性只在「同一时间窗」内生效：跨窗则释放，跟随轮换到当前窗模型；
                # 绑定模型若在冷却也释放，从当前窗重新选。避免长会话被永久钉死。
                if bound_win == win_idx and self._avail_locked(bound_idx):
                    if self._account_load_locked().get(self._account_of(bound_idx), 0) == 0:
                        self.sessions.move_to_end(sid)
                        self._acquire_locked(bound_idx)
                        return bound_idx, False
                    # 该账号已有请求在跑：本次让给别的账号，但不重绑会话，
                    # 顺序请求结束后仍会回到原模型，只有并发请求被摊开。
                    idx = self._pick_by_load_locked(self.cur)
                    if idx is None:
                        idx = bound_idx
                    self._acquire_locked(idx)
                    return idx, False
            target = self.cur
            if entry is None and not self.rotate_on_first:
                target = 0
            idx = self._pick_by_load_locked(target)
            cleared = False
            if idx is None:        # 池内全冷却 -> 清冷却重试一轮
                log.warning("池内全部处于冷却，清除冷却后重试一轮")
                self.cooldown.clear()
                idx = self._pick_by_load_locked(target)
                cleared = True
            if idx is None:
                idx = self.cur
            if sid is not None:
                self.sessions[sid] = (idx, win_idx)
                while len(self.sessions) > 4096:
                    self.sessions.popitem(last=False)
            self._acquire_locked(idx)
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
                    "account": self._account_of(i),
                    "quality": m.get("quality", 50),
                    "active": i == self.cur,
                    "inflight": self.inflight.get(i, 0),
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
                "accounts": sorted({self._account_of(i) for i in range(self.n)}),
                "inflight_total": sum(self.inflight.values()),
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


def upstream_url(member_url, client_path):
    """把池成员保存的端点换算成客户端实际请求的那个端点（同 base 下）。

    池里的 url 在建池时就固定成了 ".../v1/chat/completions"。客户端如果请求
    /v1/responses，必须把 "/v1/..." 整段换掉，否则会把 Responses 请求体发给
    chat/completions —— 上游会按对话补全解析 input 字段并报 400。

    注意不能只替换最后一段：chat/completions 有两级路径，只换末段会得到
    /v1/chat/responses（错误）。这里以 "/v1/" 为锚点整体重建。
    """
    idx = member_url.find("/v1/")
    base = member_url[:idx] if idx >= 0 else member_url.rsplit("/", 2)[0]
    return base + "/v1/" + client_path.split("/v1/", 1)[-1]


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


def resp_socket(resp):
    """拿到 urllib response 的底层 socket（读超时要用）。拿不到就返回 None。"""
    fp = getattr(resp, "fp", None)
    raw = getattr(fp, "raw", None)
    return getattr(raw, "_sock", None)


def read_first_chunk(resp, timeout):
    """带超时读流式响应的第一个数据块（= 首 token）。

    上游 socket 的 timeout 语义是「无数据多久断开」，正好等价于
    「首 token 迟迟不来就放弃」：把 timeout 临时设到 socket 上读一次，
    读到（或对端正常关闭，返回 b''）后立刻恢复原值。

    超时抛 socket.timeout，由调用方判死走 fallback。
    """
    if not timeout or timeout <= 0:
        return resp.read(8192)
    sock = resp_socket(resp)
    if sock is None:
        return resp.read(8192)
    old = sock.gettimeout()
    sock.settimeout(timeout)
    try:
        return resp.read(8192)
    finally:
        try:
            sock.settimeout(old)
        except Exception:
            pass


# ------------------------------------------------------------------- watchdog  #
class Attempt:
    """一次上游尝试，跑在独立线程里，外部可以「只等 N 秒就先走」。

    为什么需要它：urllib 的 timeout 是 socket 级超时（无数据多久断），无法表达
    「3 秒内没等到响应头就放弃」。而黑名单式的卡死（上游算完超长上下文才报错、
    网关 504 前先沉默 20-30s）恰恰是「连接正常但没有响应」，socket 超时抓不到。

    线程里 open() 是阻塞的，主线程只等 Event，所以等待超时不会泄漏线程 ——
    后台线程最终仍会自己结束（超时/报错/读到数据），只是结果被丢弃。
    """

    __slots__ = ("member", "req", "box", "done")

    def __init__(self, member, req):
        self.member = member
        self.req = req
        self.box = {}          # {"resp": response} 或 {"error": exc}
        self.done = threading.Event()

    def start(self):
        t = threading.Thread(target=self._run, name="up-%s" % self.member["id"],
                             daemon=True)
        t.start()
        return self

    def _run(self):
        try:
            resp = _OPENER.open(self.req, timeout=UPSTREAM_TIMEOUT)
            self.box["resp"] = resp
        except BaseException as e:      # noqa: BLE001 - 任何异常都要让主线程知道
            self.box["error"] = e
        finally:
            self.done.set()

    # -- 主线程侧 --
    def wait_headers(self, seconds):
        """最多等 seconds 秒拿响应头；返回 True 表示已有结果（成功或报错）。"""
        return self.done.wait(seconds)

    def value(self):
        """取结果：成功返回 response，失败抛原异常。"""
        self.done.wait()
        err = self.box.get("error")
        if err is not None:
            raise err
        return self.box["resp"]

    def abandon(self):
        """不再使用这一路。后台线程自己收尾，这里只关掉可能已建立的连接。"""
        def _close():
            self.done.wait(UPSTREAM_TIMEOUT)
            resp = self.box.get("resp")
            if resp is not None:
                try:
                    resp.close()
                except Exception:
                    pass
        threading.Thread(target=_close, daemon=True).start()


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
        # 去掉 query string，只按路径判断（有些客户端会带 ?api-version=）
        path = self.path.split("?", 1)[0].rstrip("/")
        if path not in SUPPORTED_POST_PATHS:
            self._json(404, {"error": {"message": "unsupported endpoint: %s" % self.path,
                                       "type": "http_error",
                                       "supported": list(SUPPORTED_POST_PATHS)}})
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
        idx, cleared = ST.resolve(sid)      # resolve 已原子地把 idx 记为在途
        tried = []
        attempt_chain = [idx]
        held = idx                          # 当前占用在途名额的池下标
        _hedge_extra = set()                # 对冲额外名额，供 finally 兜底释放
        hedge_after = float(CFG.get("hedge_after_seconds") or 0)
        hedge_max = int(CFG.get("hedge_max") or 0)
        first_token_to = float(CFG.get("first_token_timeout_seconds") or 0)

        def make_attempt(i):
            """按池下标构造一次上游尝试（body 里的 model 已改写）。"""
            m = ST.pool[i]
            body = dict(parsed)
            body["model"] = m["id"]
            bb = json.dumps(body, ensure_ascii=False).encode("utf-8")
            extra = {k: v for k, v in self.headers.items()}
            req = build_upstream_request(
                upstream_url(m["url"], path), bb, m["key"], extra)
            return Attempt(m, req).start()

        def classify(e, cur_idx, member, t0):
            """把一次失败翻译成 (是否可继续, 下一批候选)。

            返回 ("retry", chain) 表示可换路重试；("return", None) 表示已回包。
            """
            if isinstance(e, urllib.error.HTTPError):
                tried.append((member["id"], "http_%s" % e.code))
                log.warning("upstream %s HTTP %s (%.1fs): %s",
                            member["id"], e.code, time.time() - t0,
                            scrub(e.read(400).decode("utf-8", "replace"))[:400])
                if e.code in CFG["fail_status"]:
                    return "retry", ST.mark_fail(cur_idx, "http_%s" % e.code)
                if e.code == 404:
                    ST.evict(cur_idx)
                self._relay_error(e)
                return "return", None
            tried.append((member["id"], "exception"))
            reason = getattr(e, "reason", e)
            log.warning("upstream %s 连接异常 (%.1fs): %s",
                        member["id"], time.time() - t0, scrub(str(reason))[:300])
            if CFG["fail_on_exception"]:
                return "retry", ST.mark_fail(cur_idx, "exception")
            self._json(502, {"error": {"message": "upstream connect error: %s" % reason,
                                       "type": "upstream_error"}})
            return "return", None

        def succeed(cur_idx, resp, t0, tag):
            """响应头到手后的最后一道关：流式先等首 token，超时判死换路。

            返回 (True, None) 表示已开始正常透传（含客户端中途断开）；
            返回 (False, chain) 表示首 token 超时，chain 是下一条 fallback 链。
            判死只可能发生在给客户端写第一个字节之前 —— 一旦开始写就不能换。
            """
            member = ST.pool[cur_idx]
            head = None
            if stream and first_token_to > 0:
                try:
                    head = read_first_chunk(resp, first_token_to)
                except (socket.timeout, TimeoutError):
                    log.warning("看门狗: %s 响应头后 %.0fs 无首 token，判死走 fallback",
                                member["id"], first_token_to)
                    try:
                        resp.close()
                    except Exception:
                        pass
                    return False, ST.mark_fail(cur_idx, "first_token_timeout")
            ST.mark_ok(cur_idx)
            self._relay(resp, stream, head=head)
            log.info("routed sid=%s model=%s acct=%s inflight=%s stream=%s tools=%s "
                     "attempts=%s ep=%s %s",
                     (sid or "")[:18], ST.pool[cur_idx]["id"],
                     ST.pool[cur_idx].get("account") or "-",
                     ST.inflight_count(), stream,
                     bool(parsed.get("tools")), len(tried) + 1,
                     path.rsplit("/", 1)[-1],
                     "%s %.1fs" % (tag, time.time() - t0))
            return True, None

        try:
            for probe_i in range(len(ST.pool) + 2):
                if not attempt_chain:
                    break
                cur_idx = attempt_chain.pop(0)
                if cur_idx != held:         # 失败转移：名额从旧模型迁到新模型
                    ST.release(held)
                    ST.acquire(cur_idx)
                    held = cur_idx
                member = ST.pool[cur_idx]
                t0 = time.time()
                first = make_attempt(cur_idx)

                # ---- 看门狗：先只等 hedge_after 秒的响应头 ----
                # hedge_max < 2 等于没有备胎可补，必须当作关闭处理；否则
                # 进对冲分支后 len(racers) < hedge_max 恒假，内层会空转挂死。
                if hedge_after > 0 and hedge_max >= 2:
                    ready = first.wait_headers(hedge_after)
                else:
                    # 看门狗关闭：老行为，一直等到有响应头（或报错）为止
                    first.done.wait()
                    ready = True

                if ready:
                    # 正常路径：立刻拿到响应头（实测 p90 < 1.3s，绝大多数请求走这里）
                    try:
                        resp = first.value()
                    except (urllib.error.HTTPError, urllib.error.URLError,
                            TimeoutError, ssl.SSLError) as e:
                        kind, chain = classify(e, cur_idx, member, t0)
                        if kind == "return":
                            return
                        attempt_chain = chain
                        continue
                    ok, fchain = succeed(cur_idx, resp, t0, "OK")
                    if ok:
                        return
                    attempt_chain = fchain
                    continue

                # ---- 慢：进入条件对冲 ----
                # racers 里的每一路都在独立线程里等响应头；谁先拿到就用谁，
                # 其余立刻放弃并归还账号名额 —— 一旦有人能出响应，再让其它路
                # 继续跑只是白占并发、白烧额度。
                #
                # 不变量：held 是 finally 唯一负责释放的名额；extra 里是额外
                # 登记的对冲路名额，必须且只能释放一次。搞错会让在途计数虚高，
                # 进而污染账号分片判断。
                racers = [(cur_idx, first)]
                extra = _hedge_extra   # 与 finally 共享的兜底集合
                used = {cur_idx}    # 已经用过的池下标，防止同一模型开两路
                warned = False
                last_chain = []

                def drop(ci):
                    """归还某一路的额外名额；held 由 finally 统一释放。"""
                    if ci in extra:
                        extra.discard(ci)
                        ST.release(ci)

                while True:
                    # 补充选手：换账号优先，最多 hedge_max 路
                    if len(racers) < hedge_max:
                        alt = ST.pick_alternate(used)
                        if alt is not None:
                            ST.acquire(alt)
                            extra.add(alt)
                            used.add(alt)
                            racers.append((alt, make_attempt(alt)))
                            if not warned:
                                log.info("看门狗: %s %.1fs 无响应头，并发第 %d 路 -> %s",
                                         member["id"], hedge_after, len(racers),
                                         ST.pool[alt]["id"])
                                warned = True

                    done_now = [r for r in racers if r[1].done.is_set()]
                    if not done_now:
                        # 没有可补的备胎（其余模型全在冷却）：退回「等首选」，
                        # 否则这里会空转挂死。
                        if len(racers) == 1 and ST.pick_alternate(used) is None:
                            remaining = racers[0][1]
                            remaining.done.wait()
                            done_now = [racers[0]]
                        else:
                            time.sleep(HEDGE_POLL)
                            continue

                    win_idx, win_at = done_now[0]
                    racers = [r for r in racers if r[1] is not win_at]

                    try:
                        resp = win_at.value()
                    except (urllib.error.HTTPError, urllib.error.URLError,
                            TimeoutError, ssl.SSLError) as e:
                        drop(win_idx)
                        kind, chain = classify(e, win_idx, ST.pool[win_idx], t0)
                        last_chain = chain or []
                        if kind == "return":
                            for ci, at in racers:
                                at.abandon()
                                drop(ci)
                            return
                        # 这一路已冷却。还有人在跑就先等它们；否则看能否再补
                        # 一路；都不行就交给外层失败转移。
                        if not racers and len(used) >= hedge_max:
                            attempt_chain = last_chain
                            break
                        if not racers and ST.pick_alternate(used) is None:
                            attempt_chain = last_chain
                            break
                        continue

                    # 赢家产生：其余各路全部放弃并归还名额
                    for ci, at in racers:
                        at.abandon()
                        drop(ci)
                        log.info("看门狗: 放弃落败路 %s", ST.pool[ci]["id"])
                    # 名额迁移：赢家若属对冲路，已在 extra 里占一格，把它提升
                    # 为 held 即可 —— 千万不能再 acquire 一次，否则重复计数。
                    if win_idx != held:
                        ST.release(held)
                        extra.discard(win_idx)
                        held = win_idx
                    ok, fchain = succeed(win_idx, resp, t0,
                                         "OK(hedged x%d)" % len(used))
                    if ok:
                        return
                    # 赢家首 token 超时：落败路已全部放弃，交给外层 fallback 链
                    attempt_chain = fchain
                    break
                continue

            # 全部候选失败
            self._json(429, {"error": {
                "message": "所有池模型均失败或冷却中，已尝试: %s" %
                           ", ".join("%s(%s)" % t for t in tried),
                "type": "rate_limit_error",
                "code": "all_models_unavailable"}})
        finally:
            if held is not None:
                ST.release(held)
            # 兜底：异常/提前返回时对冲路可能还占着名额
            for ci in list(_hedge_extra):
                ST.release(ci)
            _hedge_extra.clear()

    # ---- 透传上游响应（流式逐块，非流式读满） ----
    def _relay(self, resp, stream, head=None):
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
                pending = head   # 首 token 判死阶段预读出来的第一块，先吐给客户端
                while True:
                    chunk = pending if pending is not None else resp.read(8192)
                    pending = None
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


def setup_crash_log():
    """把崩溃证据落到 crash.log。

    计划任务用 pythonw.exe 启动，没有 stderr——进程一旦原生崩溃或主线程异常退出，
    rotator.log 里不会留任何痕迹（这一现象在真实使用中复现过两次）。这里挂
    faulthandler + 线程/主线程异常钩子，让下一次崩溃有据可查。
    """
    try:
        f = open(os.path.join(HERE, "crash.log"), "a", encoding="utf-8", buffering=1)
    except OSError:
        return
    faulthandler.enable(file=f, all_threads=True)

    def _thread_hook(args):
        f.write("\n[%s] 线程未捕获异常 thread=%s\n"
                % (time.strftime("%Y-%m-%d %H:%M:%S"),
                   getattr(args.thread, "name", "?")))
        traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback, file=f)
        f.flush()

    def _main_hook(exc_type, exc, tb):
        f.write("\n[%s] 主线程未捕获异常\n" % time.strftime("%Y-%m-%d %H:%M:%S"))
        traceback.print_exception(exc_type, exc, tb, file=f)
        f.flush()

    threading.excepthook = _thread_hook
    sys.excepthook = _main_hook


def main():
    global CFG, ST
    setup_crash_log()
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
    except BaseException:
        log.exception("主循环异常退出")
        raise
    finally:
        srv.server_close()
        log.warning("主循环结束，进程即将退出（若不是主动停止，请查 crash.log）")


if __name__ == "__main__":
    main()
