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

=========================== 一体化 EXE：内置控制面 ===========================
v1.1 起，单文件 ModelRotator.exe 同时承载两件事：

  1) 代理本身（/v1/chat/completions、/v1/responses，行为与源码版完全一致）；
  2) 一套**控制面**：手动切换模型、快速刷新、给模型赋权重、启停池成员、
     清除冷却、重载配置——全部免重启生效，不需要再起 python / 计划任务。

控制面的三个入口，按「自动化的」到「交互的」排列：

  A. HTTP API   POST /control/switch | /control/next | /control/reload
                 POST /control/weights | /control/enable | /control/disable
                 POST /control/clear-cooldown | /control/shutdown
  B. Web 控制台 GET /console      （自包含 HTML，无外部依赖，双击即用）
  C. 命令行开关  ModelRotator.exe --switch <model> --weight <model>=<score> ...

运行时「开关与设置」落在 exe 同目录的 **overrides.json**（exe 自己的文件，
由控制台/CLI 生成），与「基础配置」config.json 分层：
    effective = config.json  被  overrides.json  逐键覆盖
    quality_scores 例外：按模型 id 逐项合并（改一个模型不会冲掉其余）。
改坏了执行 `--reset-settings` 删掉 overrides.json 即回到 config.json 原状。
"""

import argparse
import base64
import faulthandler
import glob
import json
import logging
import os
import re
import shutil
import socket
import ssl
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import webbrowser
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------------------
# PyInstaller --noconsole 下 sys.stdout / sys.stderr 都是 None，
# print() 与 logging.StreamHandler 都会 AttributeError。源码版没人踩到，因为
# 源码跑时 stdout 一定存在；v1.0.1 的 exe 只有「config.json 已存在」才不触发
# bootstrap_config 里的 print(..., file=sys.stderr)——这是首运行的隐形炸弹。
# 统一换成静默 sink：有控制台时仍然正常输出，没有时也不崩。
# ---------------------------------------------------------------------------
if getattr(sys, "frozen", False) and (sys.stdout is None or sys.stderr is None):
    class _SilentSink(object):
        """吞掉写入；len() 返回字节数以兼容 print 的返回值约定。"""

        def write(self, b):
            return len(b) if isinstance(b, (bytes, bytearray)) else len(str(b))

        def flush(self):
            pass

        def isatty(self):
            return False

    _sink = _SilentSink()
    if sys.stdout is None:
        sys.stdout = _sink
    if sys.stderr is None:
        sys.stderr = _sink


if getattr(sys, "frozen", False):
    # PyInstaller onefile/onedir 打包运行：__file__ 指向 _MEIPASS 临时解压目录，
    # 真正的用户数据目录是 exe 所在目录。
    HERE = os.path.dirname(os.path.abspath(sys.executable))
else:
    HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CFG = os.path.join(HERE, "config.json")
# 运行时「开关与设置」层：exe 自己的文件，控制台/CLI 写它，改坏了删掉即复原。
OVERRIDE_CFG = os.path.join(HERE, "overrides.json")
SESSION_TTL = 6 * 3600          # 会话 ID 多久不活动就重绑模型（秒）
MAX_REQ_BYTES = 64 * 1024 * 1024  # 请求体上限，超出直接拒（防内存爆）

# 可被 overrides.json 覆盖的标量开关。其余字段（pool 结构、listen_*、
# expose_as、source_models_json）只在 config.json 里改，控制台不碰。
OVERRIDE_KEYS = (
    "rotate_minutes", "cooldown_minutes",
    "hedge_after_seconds", "hedge_max", "first_token_timeout_seconds",
    "fail_status", "fail_on_exception", "rotate_on_first_request",
    "quality_priority", "quality_scores", "expose_as",
)

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


def _newest_backup():
    """找同目录下最新的 config.json.bak*，没有则 None。

    真实事故：exe 首次运行的 bootstrap 逻辑发现 config.json 不在，就照
    config.example.json 生成一份**示例配置**（3 个假模型）。而 config.json
    是 .gitignore 的、没有版本控制兜底，一旦被误删，服务会「正常启动」但
    池子变成演示数据——不报错、不告警，用户完全看不出哪里错了。
    所以缺文件时先找备份，找到就恢复并大声告警，而不是静默生成示例。
    """
    d = os.path.dirname(DEFAULT_CFG) or "."
    cands = [p for p in glob.glob(os.path.join(d, "config.json.bak*"))
             if os.path.isfile(p)]
    if not cands:
        return None
    return max(cands, key=os.path.getmtime)


def ensure_config_backup():
    """启动时若 config.json 与最新备份不同，就落一份带时间戳的备份。

    config.json 在 .gitignore 里、没有版本控制兜底，又是整个服务唯一的真相来源。
    误删/误改之后，最近的备份可能是几小时前的——池子已经变过好几轮，恢复回去
    等于丢进度。所以在启动这个「一定读到了最新 config.json」的时点补一份备份，
    误删时最多只丢一个启动周期内的改动。
    内容相同就不写，避免计划任务每次保活重启都堆一个新文件。
    """
    try:
        if not os.path.exists(DEFAULT_CFG):
            return None
        with open(DEFAULT_CFG, "rb") as f:
            cur = f.read()
        bak = _newest_backup()
        if bak:
            try:
                with open(bak, "rb") as f:
                    if f.read() == cur:
                        return None          # 内容一致，无需备份
            except OSError:
                pass
        d = os.path.dirname(DEFAULT_CFG) or "."
        stamp = time.strftime("%Y%m%d-%H%M%S")
        dest = os.path.join(d, "config.json.bak-%s" % stamp)
        # 极端情况下同一秒内撞名就退一步，绝不覆盖已有备份
        n = 1
        while os.path.exists(dest):
            dest = os.path.join(d, "config.json.bak-%s-%d" % (stamp, n))
            n += 1
        with open(dest, "wb") as f:
            f.write(cur)
        return dest
    except OSError:
        return None


def bootstrap_config():
    """确保 config.json 存在；不存在则按 备份 -> 示例模板 的顺序恢复。"""
    if os.path.exists(DEFAULT_CFG):
        return DEFAULT_CFG
    # 优先从最近的备份恢复，避免把用户跑了几个小时的池子静默换成演示数据
    bak = _newest_backup()
    if bak:
        try:
            shutil.copyfile(bak, DEFAULT_CFG)
            print(
                "[rotator] 未找到 %s，已从最近备份 %s 恢复。\n"
                "           若这是误删，请检查是什么删掉了它；确认无误后可删除该备份。"
                % (DEFAULT_CFG, bak),
                file=sys.stderr)
            return DEFAULT_CFG
        except OSError as e:
            print("[rotator] 从备份恢复失败(%s)，改用示例模板: %s" % (bak, e),
                  file=sys.stderr)
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
    cfg = apply_overrides(cfg, load_overrides())
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


# ----------------------------------------------------------------- overrides #
def load_overrides():
    """读 exe 同目录的 overrides.json；不存在/损坏都返回 {}（不阻塞启动）。

    损坏（手改出语法错）时保留坏文件不动，只告警——直接删会丢用户刚写的设置。
    """
    if not os.path.exists(OVERRIDE_CFG):
        return {}
    try:
        data = load_raw(OVERRIDE_CFG)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        try:
            print("[rotator] overrides.json 解析失败（已忽略，保留原文件）: %s" % e,
                  file=sys.stderr)
        except Exception:
            pass
        return {}


def save_overrides(data):
    """原子写 overrides.json：先写临时文件再替换，避免半截文件。"""
    tmp = OVERRIDE_CFG + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, OVERRIDE_CFG)
        return True
    except OSError as e:
        try:
            print("[rotator] 无法写入 %s: %s" % (OVERRIDE_CFG, e), file=sys.stderr)
        except Exception:
            pass
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        return False


def apply_overrides(cfg, ov):
    """effective = config.json 被 overrides.json 逐键覆盖。

    - 普通键：overrides 有就替换（含开关类标量）；
    - quality_scores：**按模型 id 逐项合并**——改一个模型的权重不该把其余
      模型的权重冲掉成 config.json 的旧值；
    - disabled_models / order：结构性开关，在这里就作用到 pool 上，
      改完立即生效，不必动 config.json。
    """
    if not ov:
        return cfg
    merged = dict(cfg)
    for k in OVERRIDE_KEYS:
        if k not in ov:
            continue
        if k == "quality_scores":
            base = dict(cfg.get("quality_scores") or {})
            extra = ov.get("quality_scores") or {}
            if isinstance(extra, dict):
                base.update(extra)
            merged["quality_scores"] = base
        else:
            merged[k] = ov[k]
    # 池成员启停：按 id 过滤，保持原顺序
    disabled = ov.get("disabled_models") or []
    if isinstance(disabled, list) and disabled:
        drop = {str(x) for x in disabled}
        merged["pool"] = [e for e in (cfg.get("pool") or [])
                          if str(_entry_id(e)) not in drop]
    # 池成员排序：order 里没提到的按原顺序排在后面
    order = ov.get("order") or []
    if isinstance(order, list) and order:
        pos = {str(x): i for i, x in enumerate(order)}
        merged["pool"] = sorted(
            merged.get("pool") or [],
            key=lambda e: pos.get(str(_entry_id(e)), len(pos) + 1))
    return merged


def _entry_id(entry):
    if isinstance(entry, str):
        return entry.rsplit("@", 1)[0] if "@" in entry else entry
    if isinstance(entry, dict):
        return entry.get("id") or ""
    return ""


CFG = None  # main 里填充


# ----------------------------------------------------------------- runtime  #
# 热重载的核心：CFG/ST 这对 (cfg, state) 必须原子地一起换。
# 请求进来时先取快照 (cfg, st)，之后全程只碰这对局部引用；重载换的是全局引用，
# 不影响已在跑的请求——它们继续用旧 runtime 跑完，新请求自然用上新 runtime。
# 没有这层，reload 会把「池下标」和「cfg」错配到不同版本，出现越界或假路由。
RUNTIME_LOCK = threading.Lock()


def current_runtime():
    """原子取当前 (cfg, st) 快照。单个请求全程只用这一对。"""
    with RUNTIME_LOCK:
        return CFG, ST


def install_runtime(cfg, st):
    """原子替换全局 runtime。调用前必须已各自构造完毕。"""
    global CFG, ST
    with RUNTIME_LOCK:
        CFG = cfg
        ST = st


# --------------------------------------------------------------------------- state   #
class State:
    """时间窗轮换 + 会话粘性 + 失败冷却。全部用锁保证线程安全。"""

    def __init__(self, pool, rotate_s, cooldown_s, rotate_on_first,
                 quality_priority=True):
        self.lock = threading.Lock()
        self.pool = pool
        self.n = len(pool)
        self.rotate_s = rotate_s
        self.cooldown_s = cooldown_s
        self.rotate_on_first = rotate_on_first
        # 质量优先开关存在 State 上而不是现读 CFG：热重载换 runtime 时 State
        # 整体重建，读自己构造时的值才不会和 cfg 版本错配。
        self.quality_priority = bool(quality_priority)
        self.cur = 0
        self.pin = None             # 手动指定的池下标，只在本时间窗内有效
        self.win_idx = 0            # 时间窗序号，每推进一个窗口 +1
        self.win_start = time.time()
        self.cooldown = {}          # idx -> until_ts
        self.sessions = OrderedDict()  # sid -> (idx, win_idx)
        self.inflight = {}           # idx -> 正在处理中的请求数
        self.stats = {}              # idx -> {ok, fail, fail_types:{}}
        self.disabled = set()        # 被 --disable 停用的 idx（本次运行内）
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
            # 手动指定只维持一个窗口：窗口自然到点就交还给轮换/质量策略，
            # 否则一次手动切换会把后面所有窗口都钉死。
            self.pin = None
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

    def _pick_by_load_locked(self, start, quality_first=None):
        """选一个池下标。双重优先：先保并发摊开，再在同等空闲里挑质量最高的。

        排序键（越小越好):
          1) 账号在途数 —— 这是并发分片，必须最优先，否则多个任务全压一个账号；
          2) -质量分    —— 同等空闲时优先用更好的模型；
          3) 离 start 的距离 —— 时间窗轮换的体现，也是稳定 tie-break。

        为什么不能只按会话分片：客户端通常不带 X-Rotate-Session，所有请求都落
        在同一个默认会话上，按会话分片等于没分片。

        quality_first=None 表示跟随配置；显式传 False 用于「手动指定模型」——
        否则质量分会立刻把用户刚切过去的模型抢回高分那个，切换看起来无效。
        """
        load = self._account_load_locked()
        if quality_first is None:
            quality_first = self.quality_priority
        best, best_score = None, None
        for off in range(self.n):
            i = (start + off) % self.n
            if not self._avail_locked(i) or i in self.disabled:
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
            avail = [i for i in range(self.n)
                     if self._avail_locked(i) and i not in self.disabled]
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
            quality_first = self.quality_priority
            best, best_score = None, None
            for i in range(self.n):
                if i in exclude or not self._avail_locked(i) or i in self.disabled:
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
                if bound_win == win_idx and self._avail_locked(bound_idx) \
                        and bound_idx not in self.disabled:
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
            # 手动指定（pin）优先于质量分：否则高分模型会立刻把用户刚切过去
            # 的模型抢回去，控制台上「切换」看起来毫无效果。并发摊开仍然保留
            # （账号在途数仍是第一排序键），只是不再按质量重排。
            target = self.pin if self.pin is not None else self.cur
            qf = False if self.pin is not None else None
            if entry is None and not self.rotate_on_first and self.pin is None:
                target = 0
            idx = self._pick_by_load_locked(target, qf)
            cleared = False
            if idx is None:        # 池内全冷却 -> 清冷却重试一轮
                log.warning("池内全部处于冷却，清除冷却后重试一轮")
                self.cooldown.clear()
                idx = self._pick_by_load_locked(target, qf)
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
                if self._avail_locked(i) and i not in self.disabled:
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

    # ---- 控制面：手动切换 / 快速刷新 / 权重赋值 ----
    def index_of(self, model_id):
        """按模型 id 找池下标。找不到返回 None。"""
        for i, m in enumerate(self.pool):
            if m["id"] == model_id:
                return i
        return None

    def switch_to(self, model_id):
        """手动切换：把当前窗模型钉到 model_id，并让已绑定的会话全部释放。

        为什么必须清 sessions：粘性只认「同一时间窗内」的绑定，手动切换不推进
        win_idx，老会话会凭旧绑定继续路由到旧模型——看起来「切换没生效」。
        为什么必须设 pin：质量优先会把高分模型立刻抢回来，手动选择被覆盖。
        """
        with self.lock:
            idx = self.index_of(model_id)
            if idx is None:
                return None, "模型不在池内: %s" % model_id
            if idx in self.disabled:
                return None, "模型已被停用，请先启用: %s" % model_id
            self.cooldown.pop(idx, None)     # 手动指定优先于历史冷却
            prev = self.pool[self.cur]["id"]
            self.cur = idx
            self.pin = idx
            self.win_start = time.time()     # 开一个新窗，锁住手动选择
            self.win_idx += 1                # 递窗号让所有旧会话绑定失效
            self.sessions.clear()
            log.info("手动切换: %s -> %s（会话粘性已重置，本窗内钉住）", prev, model_id)
            return prev, None

    def next_window(self):
        """推进一个时间窗：等价于窗口自然到点轮换一步，可用来跳过不满意的模型。"""
        with self.lock:
            prev = self.pool[self.cur]["id"]
            self.cur = (self.cur + 1) % self.n
            self.pin = self.cur
            self.win_start = time.time()
            self.win_idx += 1
            self.sessions.clear()
            now = self.pool[self.cur]["id"]
            log.info("手动轮换: %s -> %s（本窗内钉住）", prev, now)
            return prev, now

    def clear_cooldown(self):
        """清掉全部冷却：429 冷却中的模型立刻恢复参选。"""
        with self.lock:
            n = len(self.cooldown)
            self.cooldown.clear()
            self.sessions.clear()      # 会话可能绑在冷却中的模型上，一并释放
            log.info("已清除 %d 个模型的冷却", n)
            return n

    def set_weight(self, model_id, score):
        """给单个模型赋权重（质量分）。返回是否命中池成员。

        同时清会话粘性：用户改权重就是想让新权重立刻影响路由，若粘性把请求
        继续钉在旧模型上，改动看起来毫无效果（最长要等一个窗口）。
        """
        with self.lock:
            idx = self.index_of(model_id)
            if idx is None:
                return False
            self.pool[idx]["quality"] = float(score)
            self.sessions.clear()
            return True

    def set_enabled(self, model_id, on):
        """启用/停用池成员（本次运行内立即生效，已持久化到 overrides.json）。"""
        with self.lock:
            idx = self.index_of(model_id)
            if idx is None:
                return False
            if on:
                self.disabled.discard(idx)
                self.cooldown.pop(idx, None)
            else:
                self.disabled.add(idx)
            self.sessions.clear()
            return True

    def weights(self):
        """{模型id: 权重} 全量快照，供控制台渲染权重输入框。"""
        with self.lock:
            return {m["id"]: round(float(m.get("quality", 50)), 2)
                    for m in self.pool}

    def enabled_ids(self):
        with self.lock:
            return [m["id"] for i, m in enumerate(self.pool)
                    if i not in self.disabled]

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
                    "enabled": i not in self.disabled,
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
                "pinned": (self.pool[self.pin]["id"]
                           if self.pin is not None else None),
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


# 部分上游对 reasoning_effort 的取值有硬限制，超出白名单直接返回 400
# invalid_request_error（例如 GLM 只接受 low / high / max）。客户端常发 medium、
# auto 之类的值，命中这类模型时整请求必然失败。这里在转发前统一归一。
# 白名单按模型 id 小写子串匹配（命中即生效）；池成员可用 "reasoning_effort_allowed"
# 显式覆盖（数组形式），不配则默认不限制、原样透传。
REASONING_DEFAULT_ALLOWED = {
    "glm": {"low", "high", "max"},
}


def reasoning_allowed(member):
    """返回该池成员允许的 reasoning_effort 取值集合；None 表示不限制。"""
    explicit = member.get("reasoning_effort_allowed")
    if explicit:
        return set(explicit)
    mid = (member.get("id") or "").lower()
    for key, vals in REASONING_DEFAULT_ALLOWED.items():
        if key in mid:
            return set(vals)
    return None


def sanitize_reasoning(body, member):
    """归一 reasoning_effort：值不在上游白名单里就丢弃，避免整请求 400。

    选择丢弃而非硬映射：值无效说明客户端对该模型取值不了解，上游会走自身默认，
    丢弃比猜一个"最接近"的值更可预期，也不会引入新的语义偏差。
    """
    val = body.get("reasoning_effort")
    if val is None:
        return body
    allowed = reasoning_allowed(member)
    if allowed is None or val in allowed:
        return body
    log.warning("reasoning_effort=%r 不在 %s 白名单 %s，已丢弃",
                val, member["id"], sorted(allowed))
    del body["reasoning_effort"]
    return body


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
CONSOLE_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ModelRotator 控制台</title>
<style>
  :root {
    --bg:#f5f6f8; --card:#ffffff; --ink:#1a1d21; --sub:#5b6470; --line:#e3e6ea;
    --accent:#1f6feb; --ok:#1a7f37; --warn:#bf8700; --bad:#cf222e; --chip:#eef1f5;
  }
  * { box-sizing:border-box; }
  body { margin:0; padding:20px; background:var(--bg); color:var(--ink);
         font:14px/1.5 "Segoe UI","Microsoft YaHei",system-ui,sans-serif; }
  h1 { font-size:18px; margin:0 0 4px; }
  .sub { color:var(--sub); font-size:12px; margin-bottom:16px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px;
          padding:14px 16px; margin-bottom:14px; }
  .row { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
  button { font:inherit; font-size:13px; padding:7px 14px; border-radius:7px;
           border:1px solid var(--line); background:#fff; color:var(--ink);
           cursor:pointer; }
  button:hover { background:var(--chip); }
  button.primary { background:var(--accent); border-color:var(--accent); color:#fff; }
  button.primary:hover { filter:brightness(1.08); }
  button.danger { color:var(--bad); border-color:#f0b8bd; }
  button:disabled { opacity:.5; cursor:not-allowed; }
  select, input { font:inherit; font-size:13px; padding:6px 9px; border-radius:7px;
                  border:1px solid var(--line); background:#fff; color:var(--ink); }
  input[type=number] { width:74px; text-align:right; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { padding:7px 8px; border-bottom:1px solid var(--line); text-align:left; }
  th { color:var(--sub); font-weight:600; font-size:12px; white-space:nowrap; }
  td.num, th.num { text-align:right; }
  tr.off { opacity:.45; }
  tr.active td { background:#eaf2ff; }
  .tag { display:inline-block; padding:1px 7px; border-radius:20px; font-size:11px;
         background:var(--chip); color:var(--sub); margin-right:4px; }
  .tag.cur { background:#dcebff; color:var(--accent); font-weight:600; }
  .tag.cool { background:#ffe9c9; color:var(--warn); }
  .tag.off { background:#ffdcdd; color:var(--bad); }
  .stat { font-size:13px; color:var(--sub); }
  .stat b { color:var(--ink); font-size:16px; margin-right:3px; }
  .msg { margin-top:10px; padding:9px 12px; border-radius:7px; font-size:13px;
         display:none; }
  .msg.ok { display:block; background:#eafaee; color:var(--ok); border:1px solid #b6e2c1; }
  .msg.err { display:block; background:#ffeef0; color:var(--bad); border:1px solid #f5b8bd; }
  .hint { color:var(--sub); font-size:12px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; }
  label.k { font-size:12px; color:var(--sub); display:block; margin-bottom:3px; }
</style>
</head>
<body>
<h1>ModelRotator 控制台</h1>
<div class="sub">免重启切换模型 / 快速刷新 / 赋权重。设置写入 exe 同目录 <code>overrides.json</code>，
删掉它即回到 <code>config.json</code> 原状。</div>

<div class="card">
  <div class="grid" id="stats"></div>
</div>

<div class="card">
  <div class="row" style="margin-bottom:12px">
    <span style="font-weight:600">路由池</span>
    <span class="hint" id="poolinfo"></span>
    <span style="flex:1"></span>
    <button id="btnNext">下一个模型 &#8594;</button>
    <button id="btnReload">快速刷新</button>
    <button id="btnCool">清除冷却</button>
    <button id="btnWeights" class="primary">保存权重</button>
  </div>
  <table>
    <thead><tr>
      <th>模型</th><th>账号</th><th class="num">权重</th>
      <th class="num">在途</th><th class="num">成功</th><th class="num">失败</th>
      <th>状态</th><th>操作</th>
    </tr></thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<div class="card">
  <div style="font-weight:600;margin-bottom:10px">运行时设置（开关）</div>
  <div class="row" id="settings"></div>
  <div class="row" style="margin-top:12px">
    <button id="btnSettings" class="primary">保存设置</button>
    <button id="btnReset" class="danger">恢复默认设置</button>
    <button id="btnShutdown" class="danger">停止服务</button>
    <span class="hint">保存设置后点「快速刷新」生效。</span>
  </div>
</div>

<div id="msg" class="msg"></div>

<script>
"use strict";
var timer = null;
function say(text, isErr) {
  var m = document.getElementById("msg");
  m.textContent = text;
  m.className = "msg " + (isErr ? "err" : "ok");
  clearTimeout(timer);
  timer = setTimeout(function(){ m.className = "msg"; }, 6000);
}
function call(path, method, payload) {
  var opt = { method: method || "GET", headers: {} };
  if (payload) { opt.headers["Content-Type"] = "application/json";
                 opt.body = JSON.stringify(payload); }
  return fetch(path, opt).then(function(r) { return r.json(); })
    .catch(function(e){ throw new Error("请求失败: " + e); });
}
function esc(s) {
  return String(s).replace(/[&<>"']/g, function(c) {
    return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]; });
}
function uptime(s) {
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s/60) + "m" + (s%60) + "s";
  return Math.floor(s/3600) + "h" + Math.floor((s%3600)/60) + "m";
}

var SETTING_FIELDS = [
  ["rotate_minutes", "轮换窗口(分钟)", "number"],
  ["cooldown_minutes", "失败冷却(分钟)", "number"],
  ["hedge_after_seconds", "首字节对冲(s)", "number"],
  ["hedge_max", "对冲路数", "number"],
  ["first_token_timeout_seconds", "首token判死(s)", "number"],
  ["fail_on_exception", "异常也冷却", "checkbox"],
  ["quality_priority", "质量优先", "checkbox"],
  ["expose_as", "对外模型名", "text"]
];

function renderSettings(eff) {
  var box = document.getElementById("settings");
  box.innerHTML = "";
  SETTING_FIELDS.forEach(function(f) {
    var k = f[0], type = f[2];
    var wrap = document.createElement("div");
    var lab = document.createElement("label");
    lab.className = "k"; lab.textContent = f[1];
    var inp;
    if (type === "checkbox") {
      inp = document.createElement("input"); inp.type = "checkbox";
      inp.checked = !!eff[k]; inp.dataset.kind = "bool";
    } else {
      inp = document.createElement("input"); inp.type = type; inp.value = eff[k];
    }
    inp.id = "set_" + k; inp.dataset.key = k;
    wrap.appendChild(lab); wrap.appendChild(inp); box.appendChild(wrap);
  });
}

function collectSettings() {
  var out = {};
  document.querySelectorAll("#settings input").forEach(function(inp) {
    var v = inp.dataset.kind === "bool" ? inp.checked : inp.value;
    if (inp.type === "number") {
      v = parseFloat(v);
      if (isNaN(v)) return;
    }
    out[inp.dataset.key] = v;
  });
  return out;
}

function render(snap, weights) {
  var s = snap;
  document.getElementById("stats").innerHTML =
    '<div><label class="k">当前模型</label><span class="stat"><b>' + esc(s.active) +
    '</b></span></div>' +
    '<div><label class="k">池规模</label><span class="stat"><b>' + s.pool_size +
    '</b> 个 / ' + s.accounts.length + ' 账号</span></div>' +
    '<div><label class="k">在途请求</label><span class="stat"><b>' + s.inflight_total +
    '</b></span></div>' +
    '<div><label class="k">距下次轮换</label><span class="stat"><b>' +
    s.seconds_to_next_rotate + '</b> 秒（窗口 ' + s.rotate_minutes + ' 分钟）</span></div>' +
    '<div><label class="k">已运行</label><span class="stat"><b>' +
    uptime(s.uptime_seconds) + '</b></span></div>' +
    '<div><label class="k">会话粘性</label><span class="stat"><b>' +
    s.session_stickiness + '</b></span></div>';

  document.getElementById("poolinfo").textContent =
    "共 " + s.pool_size + " 个；* 为当前时间窗模型";

  var tb = document.getElementById("tbody");
  tb.innerHTML = "";
  s.models.slice().sort(function(a,b){
      if (a.active !== b.active) return a.active ? -1 : 1;
      return b.quality - a.quality;
    }).forEach(function(m) {
    var tr = document.createElement("tr");
    if (!m.enabled) tr.className = "off";
    if (m.active) tr.className = "active";
    var tags = [];
    if (m.active) tags.push('<span class="tag cur">当前</span>');
    if (!m.enabled) tags.push('<span class="tag off">停用</span>');
    if (m.cooldown_remaining > 0)
      tags.push('<span class="tag cool">冷却 ' + m.cooldown_remaining + 's</span>');
    tr.innerHTML =
      '<td>' + (m.active ? "* " : "") + esc(m.id) + '</td>' +
      '<td><span class="tag">' + esc(m.account) + '</span></td>' +
      '<td class="num"><input type="number" min="0" max="100" step="0.1" ' +
        'value="' + m.quality + '" data-model="' + esc(m.id) + '"></td>' +
      '<td class="num">' + m.inflight + '</td>' +
      '<td class="num">' + m.ok + '</td>' +
      '<td class="num">' + m.fail + '</td>' +
      '<td>' + (tags.join("") || '<span class="hint">-</span>') + '</td>' +
      '<td>' +
        '<button data-act="switch" data-model="' + esc(m.id) + '">切换</button> ' +
        '<button data-act="toggle" data-model="' + esc(m.id) + '" ' +
          'data-on="' + (m.enabled ? 1 : 0) + '">' +
          (m.enabled ? "停用" : "启用") + '</button>' +
      '</td>';
    tb.appendChild(tr);
  });
}

function refresh() {
  return Promise.all([
    call("/status"), call("/control/weights"), call("/control/settings")
  ]).then(function(rs) {
    render(rs[0], rs[1].weights || {});
    renderSettings(rs[2].effective || {});
  });
}

function act(path, payload, okText) {
  return call(path, "POST", payload).then(function(r) {
    say(r.ok ? (okText || r.message) : ("失败: " + (r.error || JSON.stringify(r))), !r.ok);
    return refresh();
  });
}

document.getElementById("tbody").addEventListener("click", function(e) {
  var b = e.target.closest("button");
  if (!b) return;
  var model = b.dataset.model;
  if (b.dataset.act === "switch") {
    act("/control/switch", {model: model}, "已切换到 " + model);
  } else if (b.dataset.act === "toggle") {
    var on = b.dataset.on !== "1";
    act("/control/" + (on ? "enable" : "disable"), {model: model},
        model + (on ? " 已启用" : " 已停用"));
  }
});

document.getElementById("btnNext").onclick = function() {
  act("/control/next", {}, "已推进一个时间窗");
};
document.getElementById("btnReload").onclick = function() {
  act("/control/reload", {}, "已重载配置");
};
document.getElementById("btnCool").onclick = function() {
  act("/control/clear-cooldown", {}, "已清除全部冷却");
};
document.getElementById("btnWeights").onclick = function() {
  var scores = {};
  document.querySelectorAll('#tbody input[type=number]').forEach(function(i) {
    var v = parseFloat(i.value);
    if (!isNaN(v) && v >= 0 && v <= 100) scores[i.dataset.model] = v;
  });
  if (!Object.keys(scores).length) { say("没有可保存的权重", true); return; }
  act("/control/weights", {scores: scores},
      "已保存 " + Object.keys(scores).length + " 个模型权重");
};
document.getElementById("btnSettings").onclick = function() {
  act("/control/settings", collectSettings(), "设置已写入 overrides.json");
};
document.getElementById("btnReset").onclick = function() {
  if (!confirm("删除 overrides.json，所有运行时设置回到 config.json 原状？")) return;
  act("/control/reset-settings", {}, "已恢复默认设置");
};
document.getElementById("btnShutdown").onclick = function() {
  if (!confirm("确认停止 ModelRotator 服务？")) return;
  call("/control/shutdown", "POST", {}).then(function() {
    say("服务已停止", false);
  });
};

refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>
"""


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
        cfg, st = current_runtime()
        path = self.path.split("?", 1)[0]
        if path.startswith("/v1/models"):
            now = int(time.time())
            data = [{
                "id": cfg["expose_as"], "object": "model", "created": now,
                "owned_by": "model-rotator",
            }]
            payload = json.dumps({"object": "list", "data": data}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if path == "/console":
            self._send_html(CONSOLE_HTML)
            return
        if path in ("/", "/health", "/healthz"):
            body = {"ok": True, "active_model": st.current(),
                    "uptime_seconds": round(time.time() - START_TS)}
            self._json(200, body)
            return
        if path in ("/status", "/v1/rotator/status"):
            self._json(200, st.snapshot())
            return
        if path in ("/control/weights",):
            self._json(200, {"ok": True, "weights": st.weights(),
                             "enabled": st.enabled_ids()})
            return
        if path == "/control/settings":
            self._json(200, {"ok": True, "overrides": load_overrides(),
                             "effective": effective_settings(cfg),
                             "override_file": OVERRIDE_CFG})
            return
        self._json(404, {"error": {"message": "not found", "type": "http_error"}})

    def _send_html(self, html):
        try:
            payload = html.encode("utf-8")
        except Exception:
            payload = html
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

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

        # ---- 控制面：/control/* 全部走这里，不进入 OpenAI 转发链路 ----
        if path.startswith("/control"):
            return self._handle_control(path)

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

        # 单个请求全程只认这一对 (cfg, st)：热重载换的是全局引用，
        # 已进来的请求继续用旧版 runtime 跑完，不会出现「cfg 和 st 不同版本」。
        cfg, st = current_runtime()

        sid = (self.headers.get("X-Rotate-Session")
               or self.headers.get("X-Session-Id") or parsed.get("user") or "default")
        stream = bool(parsed.get("stream"))

        # 路由：先取候选顺序（首个 + 失败转移链）
        idx, cleared = st.resolve(sid)      # resolve 已原子地把 idx 记为在途
        tried = []
        attempt_chain = [idx]
        held = idx                          # 当前占用在途名额的池下标
        _hedge_extra = set()                # 对冲额外名额，供 finally 兜底释放
        hedge_after = float(cfg.get("hedge_after_seconds") or 0)
        hedge_max = int(cfg.get("hedge_max") or 0)
        first_token_to = float(cfg.get("first_token_timeout_seconds") or 0)

        def make_attempt(i):
            """按池下标构造一次上游尝试（body 里的 model 已改写）。"""
            m = st.pool[i]
            body = dict(parsed)
            body["model"] = m["id"]
            sanitize_reasoning(body, m)
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
                if e.code in cfg["fail_status"]:
                    return "retry", st.mark_fail(cur_idx, "http_%s" % e.code)
                if e.code == 404:
                    st.evict(cur_idx)
                self._relay_error(e)
                return "return", None
            tried.append((member["id"], "exception"))
            reason = getattr(e, "reason", e)
            log.warning("upstream %s 连接异常 (%.1fs): %s",
                        member["id"], time.time() - t0, scrub(str(reason))[:300])
            if cfg["fail_on_exception"]:
                return "retry", st.mark_fail(cur_idx, "exception")
            self._json(502, {"error": {"message": "upstream connect error: %s" % reason,
                                       "type": "upstream_error"}})
            return "return", None

        def succeed(cur_idx, resp, t0, tag):
            """响应头到手后的最后一道关：流式先等首 token，超时判死换路。

            返回 (True, None) 表示已开始正常透传（含客户端中途断开）；
            返回 (False, chain) 表示首 token 超时，chain 是下一条 fallback 链。
            判死只可能发生在给客户端写第一个字节之前 —— 一旦开始写就不能换。
            """
            member = st.pool[cur_idx]
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
                    return False, st.mark_fail(cur_idx, "first_token_timeout")
            st.mark_ok(cur_idx)
            self._relay(resp, stream, head=head)
            log.info("routed sid=%s model=%s acct=%s inflight=%s stream=%s tools=%s "
                     "attempts=%s ep=%s %s",
                     (sid or "")[:18], st.pool[cur_idx]["id"],
                     st.pool[cur_idx].get("account") or "-",
                     st.inflight_count(), stream,
                     bool(parsed.get("tools")), len(tried) + 1,
                     path.rsplit("/", 1)[-1],
                     "%s %.1fs" % (tag, time.time() - t0))
            return True, None

        try:
            for probe_i in range(len(st.pool) + 2):
                if not attempt_chain:
                    break
                cur_idx = attempt_chain.pop(0)
                if cur_idx != held:         # 失败转移：名额从旧模型迁到新模型
                    st.release(held)
                    st.acquire(cur_idx)
                    held = cur_idx
                member = st.pool[cur_idx]
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
                        st.release(ci)

                while True:
                    # 补充选手：换账号优先，最多 hedge_max 路
                    if len(racers) < hedge_max:
                        alt = st.pick_alternate(used)
                        if alt is not None:
                            st.acquire(alt)
                            extra.add(alt)
                            used.add(alt)
                            racers.append((alt, make_attempt(alt)))
                            if not warned:
                                log.info("看门狗: %s %.1fs 无响应头，并发第 %d 路 -> %s",
                                         member["id"], hedge_after, len(racers),
                                         st.pool[alt]["id"])
                                warned = True

                    done_now = [r for r in racers if r[1].done.is_set()]
                    if not done_now:
                        # 没有可补的备胎（其余模型全在冷却）：退回「等首选」，
                        # 否则这里会空转挂死。
                        if len(racers) == 1 and st.pick_alternate(used) is None:
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
                        kind, chain = classify(e, win_idx, st.pool[win_idx], t0)
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
                        if not racers and st.pick_alternate(used) is None:
                            attempt_chain = last_chain
                            break
                        continue

                    # 赢家产生：其余各路全部放弃并归还名额
                    for ci, at in racers:
                        at.abandon()
                        drop(ci)
                        log.info("看门狗: 放弃落败路 %s", st.pool[ci]["id"])
                    # 名额迁移：赢家若属对冲路，已在 extra 里占一格，把它提升
                    # 为 held 即可 —— 千万不能再 acquire 一次，否则重复计数。
                    if win_idx != held:
                        st.release(held)
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
                st.release(held)
            # 兜底：异常/提前返回时对冲路可能还占着名额
            for ci in list(_hedge_extra):
                st.release(ci)
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

    # ---- 控制面 ------------------------------------------------------------
    def _handle_control(self, path):
        """POST /control/<action>：手动切换 / 快速刷新 / 权重赋值 / 池成员启停。

        全部免重启生效，并持久化到 overrides.json（exe 自己的文件）。
        改坏了删掉 overrides.json 即回到 config.json 原状。
        """
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
            if not isinstance(body, dict):
                raise ValueError("body is not a JSON object")
        except Exception as e:
            self._json(400, {"ok": False, "error": "invalid JSON body: %s" % e})
            return

        action = path.rstrip("/").split("/control/", 1)[-1].lower()
        cfg, st = current_runtime()

        # ---- 手动切换模型 ----
        if action == "switch":
            model = str(body.get("model") or "").strip()
            if not model:
                self._json(400, {"ok": False, "error": "缺少 model 参数"})
                return
            prev, err = st.switch_to(model)
            if err:
                self._json(404, {"ok": False, "error": err})
                return
            self._json(200, {"ok": True, "action": "switch",
                             "from": prev, "to": model,
                             "message": "已切换: %s -> %s" % (prev, model)})
            return

        # ---- 推进一个时间窗（等价于窗口自然轮换一步） ----
        if action == "next":
            prev, now = st.next_window()
            self._json(200, {"ok": True, "action": "next",
                             "from": prev, "to": now,
                             "message": "已轮换: %s -> %s" % (prev, now)})
            return

        # ---- 给模型权重赋值 ----
        if action == "weights":
            # 支持两种：{"model":"x","score":90} 或 {"scores":{"x":90,"y":80}}
            updates = {}
            if body.get("model"):
                updates[str(body["model"])] = body.get("score")
            extra = body.get("scores") or {}
            if isinstance(extra, dict):
                updates.update(extra)
            if not updates:
                self._json(400, {"ok": False, "error": "缺少 model/score 或 scores"})
                return
            unknown, bad = [], []
            for mid, sc in updates.items():
                try:
                    val = float(sc)
                except (TypeError, ValueError):
                    bad.append(str(mid))
                    continue
                if not (0 <= val <= 100):
                    bad.append(str(mid))
                    continue
                if not st.set_weight(mid, val):
                    unknown.append(str(mid))
            # 持久化：写进 overrides.json 的 quality_scores（按 id 合并）
            ov = load_overrides()
            ov["quality_scores"] = dict(ov.get("quality_scores") or {})
            for mid, sc in updates.items():
                if str(mid) in bad:
                    continue
                ov["quality_scores"][str(mid)] = float(sc)
            saved = save_overrides(ov)
            msg = "已更新 %d 个模型权重" % (len(updates) - len(bad))
            if unknown:
                msg += "；不在池内已忽略: %s" % ", ".join(unknown)
            if bad:
                msg += "；数值非法已忽略: %s" % ", ".join(bad)
            self._json(200, {"ok": True, "action": "weights",
                             "updated": sorted(updates), "unknown": unknown,
                             "bad": bad, "persisted": saved,
                             "weights": st.weights(), "message": msg})
            return

        # ---- 快速刷新：重读 config.json + overrides.json，重建路由池 ----
        if action == "reload":
            result = do_reload()
            self._json(200 if result.get("ok") else 500, result)
            return

        # ---- 清除全部冷却 ----
        if action == "clear-cooldown":
            n = st.clear_cooldown()
            self._json(200, {"ok": True, "action": "clear-cooldown", "cleared": n,
                             "message": "已清除 %d 个模型的冷却" % n})
            return

        # ---- 池成员启停 ----
        if action in ("enable", "disable"):
            on = action == "enable"
            model = str(body.get("model") or "").strip()
            if not model:
                self._json(400, {"ok": False, "error": "缺少 model 参数"})
                return
            if not st.set_enabled(model, on):
                self._json(404, {"ok": False, "error": "模型不在池内: %s" % model})
                return
            ov = load_overrides()
            cur = set(ov.get("disabled_models") or [])
            if on:
                cur.discard(model)
            else:
                cur.add(model)
            ov["disabled_models"] = sorted(cur)
            save_overrides(ov)
            self._json(200, {"ok": True, "action": action, "model": model,
                             "enabled": on, "disabled": sorted(cur),
                             "message": "%s 已%s" % (model, "启用" if on else "停用")})
            return

        # ---- 运行时设置开关（窗口/冷却/看门狗等） ----
        if action == "settings":
            allowed = {k: v for k, v in body.items()
                       if k in OVERRIDE_KEYS and k != "quality_scores"}
            if not allowed:
                self._json(400, {"ok": False,
                                 "error": "无可写字段，可用: %s" % list(OVERRIDE_KEYS)})
                return
            ov = load_overrides()
            ov.update(allowed)
            saved = save_overrides(ov)
            self._json(200, {"ok": True, "action": "settings",
                             "applied": allowed, "persisted": saved,
                             "message": "设置已写入 overrides.json，执行「重载」后生效"})
            return

        # ---- 恢复默认：删 overrides.json 并重载，回到 config.json 原状 ----
        if action == "reset-settings":
            existed = os.path.exists(OVERRIDE_CFG)
            if existed:
                try:
                    os.remove(OVERRIDE_CFG)
                except OSError as e:
                    self._json(500, {"ok": False, "error": "删除失败: %s" % e})
                    return
            result = do_reload()
            result["message"] = ("已删除 %s 并重载，回到 config.json 原状"
                                 % OVERRIDE_CFG) if existed else \
                                ("无 %s，已按当前配置重载" % OVERRIDE_CFG)
            self._json(200 if result.get("ok") else 500, result)
            return

        # ---- 关闭自身（--stop / 控制台按钮） ----
        if action == "shutdown":
            self._json(200, {"ok": True, "action": "shutdown",
                             "message": "正在停止服务"})
            threading.Thread(target=stop_server, daemon=True).start()
            return

        self._json(404, {"ok": False,
                         "error": "unknown control action: %s" % action,
                         "supported": ["switch", "next", "weights", "reload",
                                       "reset-settings", "clear-cooldown",
                                       "enable", "disable",
                                       "settings", "shutdown"]})

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
SERVER = None          # serve 实例，供 /control/shutdown 与 --stop 使用


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


def stop_server():
    """停掉本进程的 HTTP 服务。serve_forever 不能在自身线程里调，必须另起线程。"""
    srv = SERVER
    if srv is not None:
        threading.Thread(target=srv.shutdown, daemon=True).start()


def effective_settings(cfg):
    """当前生效的运行时开关，供控制台「设置」面板渲染。"""
    keys = [k for k in OVERRIDE_KEYS if k != "quality_scores"]
    out = {}
    for k in keys:
        if k in cfg:
            out[k] = cfg[k]
    return out


def do_reload():
    """快速刷新：重读 config.json + overrides.json，原子换 runtime。

    失败时保留旧 runtime 继续服务——配置文件写坏不该让代理挂掉。
    在途请求不受影响：它们手里是旧 (cfg, st) 引用，跑完自己释放。
    """
    try:
        cfg = build_config()
        st = State(
            pool=cfg["_pool"],
            rotate_s=float(cfg.get("rotate_seconds")
                           or cfg["rotate_minutes"] * 60),
            cooldown_s=float(cfg.get("cooldown_seconds")
                             or cfg["cooldown_minutes"] * 60),
            rotate_on_first=bool(cfg.get("rotate_on_first", True)),
            quality_priority=bool(cfg.get("quality_priority", True)),
        )
    except SystemExit as e:
        log.error("重载失败，沿用旧配置: %s", e)
        return {"ok": False, "error": str(e)}
    except Exception as e:
        log.error("重载异常，沿用旧配置: %s", e)
        return {"ok": False, "error": str(e)}
    _, old_st = current_runtime()
    old_pool = [m["id"] for m in old_st.pool] if old_st else []
    new_pool = [m["id"] for m in cfg["_pool"]]
    added = [m for m in new_pool if m not in old_pool]
    removed = [m for m in old_pool if m not in new_pool]
    install_runtime(cfg, st)
    log.info("快速刷新完成: 池 %d -> %d（新增 %d / 移除 %d）",
             len(old_pool), len(new_pool), len(added), len(removed))
    return {"ok": True, "action": "reload", "pool_size": len(new_pool),
            "added": added, "removed": removed, "active": st.current(),
            "message": "已重载配置，池 %d -> %d（+%d / -%d）"
                       % (len(old_pool), len(new_pool), len(added), len(removed))}


def setup_crash_log():
    """把崩溃证据落到 crash.log。

    计划任务用 pythonw.exe 启动，没有 stderr——进程一旦原生崩溃或主线程异常退出，
    rotator.log 里不会留任何痕迹（这一现象在真实使用中复现过两次）。这里挂
    faulthandler + 线程/主线程异常钩子，让下一次崩溃有据可查。
    """
    try:
        f = open(os.path.join(os.path.dirname(DEFAULT_CFG), "crash.log"), "a",
                 encoding="utf-8", buffering=1)
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


# ============================================================================
#  命令行开关（CLI）
#
#  exe 的双身份：
#    1) 无开关 / 只有 --console --port 等 → 跑成服务（占端口）；
#    2) 其余开关 → 一次性命令，**不起服务**，通过 HTTP 打给已在跑的实例。
#       这样 `--switch hy3` 不会因为抢端口而失败，也不会拉起第二个实例。
# ============================================================================
def _eprint(msg):
    try:
        print(msg, file=sys.stderr)
    except Exception:
        pass


def _force_utf8_stdio():
    """frozen exe 的 stdout/stderr 默认走 ANSI 代码页（中文 Windows 是 cp936），
    CLI 表格和提示里的中文会输出成乱码——日志文件一直是 UTF-8，只有控制台不是。
    能被 reconfigure 的流统一改成 UTF-8；--noconsole 下那些是静默 sink，跳过。
    """
    for name in ("stdout", "stderr"):
        s = getattr(sys, name, None)
        if s is None:
            continue
        try:
            s.reconfigure(encoding="utf-8")
        except Exception:
            pass


def http_call(base_url, method, path, payload=None, timeout=30):
    """直连本机 rotator 实例；绕过环境代理（127.0.0.1 走代理必失败）。"""
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base_url + path, data=data, headers=headers,
                                 method=method)
    with _OPENER.open(req, timeout=timeout) as r:
        text = r.read().decode("utf-8", "replace")
    try:
        return json.loads(text or "{}")
    except ValueError:
        return {"ok": True, "raw": text}


def _find_rotator_pids():
    """列出本机所有 rotator 进程（pythonw+rotator.py 或 ModelRotator.exe）。

    用 CIM 按 CommandLine 精确匹配，避免 taskkill /FI 对命令行过滤不可靠的坑。
    注意两点：
    - 必须同时限定进程名。查询自身的命令行里就含 "rotator.py" / "ModelRotator.exe"
      字面量，只按 CommandLine 匹配会把执行查询的 powershell.exe 也算上；
    - Python 侧只认 python*（源码跑法），exe 侧只认 ModelRotator.exe。
    """
    ps = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { ($_.Name -eq 'ModelRotator.exe') -or "
        "($_.Name -like 'python*' -and $_.CommandLine -match 'rotator\\.py') } | "
        "Select-Object -ExpandProperty ProcessId"
    )
    try:
        out = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True, timeout=30,
            encoding="utf-8", errors="replace",
            env=dict(os.environ, PYTHONIOENCODING="utf-8"),
        )
    except Exception as e:
        return None, str(e)
    pids = []
    for tok in (out.stdout or "").split():
        if tok.strip().isdigit():
            pids.append(int(tok))
    return pids, (out.stderr or "").strip()


def cmd_kill(include_task):
    """杀掉所有 rotator 进程；include_task=True 时连计划任务一起注销。"""
    # PyInstaller onefile 一次启动 = 父引导进程 + 子运行时进程，两个都叫
    # ModelRotator.exe。只排除自己会把同胞兄弟（父进程）也杀掉，表现为
    # `--kill` 自己退出码变 127。父进程一起排除。
    me = os.getpid()
    try:
        ppid = os.getppid()
    except (AttributeError, OSError):
        ppid = None
    keep = {me} | ({ppid} if ppid else set())
    pids, err = _find_rotator_pids()
    if pids is None:
        _eprint("[kill] 无法枚举进程: %s" % err)
        return 1
    pids = [p for p in pids if p not in keep]
    if not pids:
        print("[kill] 没有发现运行中的 rotator 进程")
    else:
        ps = (
            "foreach ($id in @(%s)) { "
            "try { Stop-Process -Id $id -Force -ErrorAction Stop; "
            "Write-Host \"killed PID $id\" } "
            "catch { Write-Host \"already gone PID $id\" } }"
            % ",".join(str(p) for p in pids)
        )
        subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive",
                        "-Command", ps], capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=30)
        print("[kill] 已处理 %d 个进程: %s" % (len(pids), ", ".join(map(str, pids))))
    if include_task:
        r = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
             "Unregister-ScheduledTask -TaskName 'ModelRotator' -Confirm:$false "
             "-ErrorAction SilentlyContinue; "
             "if (Get-ScheduledTask -TaskName 'ModelRotator' -ErrorAction SilentlyContinue) "
             "{ 'task-still-present' } else { 'task-removed' }"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=30)
        print("[kill] 计划任务: %s" % (r.stdout or "").strip())
    return 0


def cmd_install_task(exe_path):
    """注册开机自启 + 每 5 分钟保活，指向 exe 自身（不再依赖 pythonw）。

    exe 有单实例保护，保活重复拉起会主动退出，不会端口互抢。
    """
    me = os.getpid()
    esc = lambda s: s.replace("'", "''")
    ps = (
        "$exe = '%s'; "
        "$act = New-ScheduledTaskAction -Execute $exe -WorkingDirectory '%s'; "
        "$t1 = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME; "
        "$t2 = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) "
        "-RepetitionInterval (New-TimeSpan -Minutes 5) "
        "-RepetitionDuration (New-TimeSpan -Days 3650); "
        "$set = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries "
        "-DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 "
        "-RestartInterval (New-TimeSpan -Minutes 1) "
        "-ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew; "
        "$prin = New-ScheduledTaskPrincipal -UserId $env:USERNAME "
        "-LogonType Interactive -RunLevel Limited; "
        "Register-ScheduledTask -TaskName 'ModelRotator' -Action $act "
        "-Trigger @($t1,$t2) -Settings $set -Principal $prin -Force "
        "-ErrorAction Stop -Description 'ModelRotator exe: model auto-rotate "
        "proxy, at-logon + 5min keepalive.' | Out-Null; "
        "if (Get-ScheduledTask -TaskName 'ModelRotator' -ErrorAction SilentlyContinue) "
        "{ 'installed' } else { 'failed' }"
        % (esc(exe_path), esc(os.path.dirname(exe_path)))
    )
    r = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive",
                        "-Command", ps], capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=60)
    ok = "installed" in (r.stdout or "")
    print("[install-task] %s -> %s" % (exe_path, "已注册" if ok else "失败"))
    if not ok:
        _eprint(r.stderr or r.stdout or "")
        return 1
    print("[install-task] 开机自启 + 每 5 分钟保活已就绪；"
          "exe 有单实例保护，重复拉起会自动退出。")
    return 0


def _run_service(host, port, console_mode):
    """跑成服务：占端口、起控制面。"""
    global CFG, ST, SERVER
    setup_crash_log()
    bak = ensure_config_backup()
    cfg = build_config()
    install_runtime(cfg, None)
    if bak:
        log.info("config.json 已自动备份 -> %s", bak)
    # 日志默认跟着 config.json 走：--config 指到别处时，日志/overrides 都在
    # 同一个目录里，不会一半在 exe 目录一半在配置目录。
    setup_log(cfg.get("log_file")
              or os.path.join(os.path.dirname(DEFAULT_CFG), "rotator.log"))
    st = State(
        pool=cfg["_pool"],
        rotate_s=float(cfg.get("rotate_seconds") or cfg["rotate_minutes"] * 60),
        cooldown_s=float(cfg.get("cooldown_seconds") or cfg["cooldown_minutes"] * 60),
        rotate_on_first=bool(cfg.get("rotate_on_first", True)),
        quality_priority=bool(cfg.get("quality_priority", True)),
    )
    install_runtime(cfg, st)
    if host:
        cfg["listen_host"] = host
    if port:
        cfg["listen_port"] = port
    host = cfg["listen_host"]
    port = int(cfg["listen_port"])
    log.info("模型轮换代理启动 http://%s:%d  池=%s  窗口=%.1f分钟  冷却=%.1f分钟  对外=%s",
             host, port, [m["id"] for m in cfg["_pool"]],
             st.rotate_s / 60, st.cooldown_s / 60, cfg["expose_as"])
    log.info("模型清单源(只读): %s", cfg["_src"])
    if os.path.exists(OVERRIDE_CFG):
        log.info("运行时设置层(可改): %s", OVERRIDE_CFG)
    else:
        log.info("运行时设置层: %s（尚未创建，控制台/CLI 首次写入时生成）",
                 OVERRIDE_CFG)
    log.info("控制台: http://%s:%d/console", host, port)

    # 单实例保护：计划任务每 5 分钟触发一次，若上一实例仍在跑则直接退出，
    # 避免端口互抢。allow_reuse_address 交给父类，此处先做一次健康探测。
    if probe_health(host, port):
        log.info("检测到 %s:%d 已有健康实例在运行，本次启动放弃", host, port)
        return 0

    try:
        srv = ThreadingHTTPServer((host, port), Handler)
    except OSError as e:
        log.error("无法绑定 %s:%d (%s)；可能已有实例占用", host, port, e)
        return 1
    SERVER = srv
    log.info("监听已就绪 http://%s:%d", host, port)
    if not console_mode:
        try:
            print("[rotator] 服务已启动 http://%s:%d  控制台 http://%s:%d/console"
                  % (host, port, host, port), file=sys.stderr)
        except Exception:
            pass
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
    return 0


def _print_status(snap):
    m = snap["models"]
    print("状态: 池 %d 个模型 / %d 个账号 / 在途 %d / 已运行 %ds / 距下次轮换 %ds"
          % (snap["pool_size"], len(snap["accounts"]), snap["inflight_total"],
             snap["uptime_seconds"], snap["seconds_to_next_rotate"]))
    print("当前模型: %s   窗口 %.0f 分钟   冷却 %.0f 分钟"
          % (snap["active"], snap["rotate_minutes"], snap["cooldown_minutes"]))
    print("-" * 78)
    print("%-34s %-12s %7s %6s %5s %5s %7s" %
          ("模型", "账号", "权重", "在途", "成功", "失败", "冷却(s)"))
    print("-" * 78)
    for x in sorted(m, key=lambda z: (-z["quality"], z["id"])):
        flag = "*" if x["active"] else " "
        en = "" if x.get("enabled", True) else " [停用]"
        print("%s%-33s %-12s %7.1f %6d %5d %5d %7d%s"
              % (flag, x["id"][:33], x["account"][:12], x["quality"],
                 x["inflight"], x["ok"], x["fail"], x["cooldown_remaining"], en))
    print("-" * 78)
    print("* = 当前时间窗使用的模型")


def _resolve_base(host, port):
    """一次性命令要打到哪个地址：优先 CLI 显式值，否则用默认配置。"""
    if host and port:
        return "http://%s:%d" % (host, port)
    try:
        cfg = load_raw(DEFAULT_CFG)
        h = host or cfg.get("listen_host") or "127.0.0.1"
        p = int(port or cfg.get("listen_port") or 3166)
    except Exception:
        h, p = host or "127.0.0.1", int(port or 3166)
    return "http://%s:%d" % (h, p)


def _apply_config_path(path):
    """把 --config 指到的路径设成配置源，overrides.json 跟随它所在目录。

    这样 exe 可以放在任何地方，配置和运行时设置都留在用户原来的目录里，
    不必为了用 exe 而搬迁 config.json。
    """
    global DEFAULT_CFG, OVERRIDE_CFG
    if not path:
        return
    p = os.path.abspath(path)
    DEFAULT_CFG = p
    OVERRIDE_CFG = os.path.join(os.path.dirname(p), "overrides.json")


def cli_main(argv):
    """解析开关并分派。返回进程退出码。"""
    ap = argparse.ArgumentParser(
        prog="ModelRotator",
        description="模型自动轮换代理（一体化 exe：代理 + 控制面）",
        epilog="示例: ModelRotator.exe --switch hy3  |  "
               "ModelRotator.exe --weight hy3=90  |  ModelRotator.exe --refresh",
        add_help=True)
    ap.add_argument("--version", action="version", version="ModelRotator 1.1.0")
    ap.add_argument("--console", action="store_true",
                    help="保留控制台窗口（默认 exe 无窗口，日志写 rotator.log）")
    ap.add_argument("--host", default=None, help="监听地址（覆盖 config.json）")
    ap.add_argument("--port", type=int, default=None,
                    help="监听端口（覆盖 config.json）")
    ap.add_argument("--config", default=None, metavar="PATH",
                    help="配置文件路径（默认 exe 同目录的 config.json；"
                         "overrides.json 跟随它所在目录）")

    # ---- 一次性命令：不起服务，打给已在跑的实例 ----
    ap.add_argument("--status", action="store_true", help="查看运行状态")
    ap.add_argument("--list", action="store_true", help="列出池内模型与权重")
    ap.add_argument("--switch", metavar="MODEL", default=None,
                    help="手动切换当前模型（免重启）")
    ap.add_argument("--next", action="store_true",
                    help="手动推进一个时间窗（跳到下一个模型）")
    ap.add_argument("--refresh", action="store_true",
                    help="快速刷新：重读 config.json + overrides.json 重建路由池")
    ap.add_argument("--weight", action="append", metavar="MODEL=SCORE", default=None,
                    help="给模型赋权重，可重复：--weight hy3=90 --weight deepseek=80")
    ap.add_argument("--enable", action="append", metavar="MODEL", default=None,
                    help="启用池成员（可重复）")
    ap.add_argument("--disable", action="append", metavar="MODEL", default=None,
                    help="停用池成员（可重复）")
    ap.add_argument("--clear-cooldown", action="store_true", help="清除全部冷却")
    ap.add_argument("--stop", action="store_true",
                    help="停止正在运行的服务（保留计划任务）")
    ap.add_argument("--open-console", action="store_true",
                    help="在浏览器打开控制台")

    # ---- 本地进程/任务管理 ----
    ap.add_argument("--kill", action="store_true",
                    help="杀掉所有 rotator 后台进程（含 pythonw 旧实例）")
    ap.add_argument("--kill-all", action="store_true",
                    help="杀掉所有后台进程，并注销开机自启计划任务")
    ap.add_argument("--install-task", action="store_true",
                    help="注册开机自启 + 每 5 分钟保活（指向本 exe）")
    ap.add_argument("--uninstall-task", action="store_true",
                    help="注销开机自启计划任务")
    ap.add_argument("--reset-settings", action="store_true",
                    help="删除 overrides.json，回到 config.json 原状")
    args = ap.parse_args(argv)
    _apply_config_path(args.config)

    # ---------- 本地进程管理 ----------
    if args.kill or args.kill_all:
        return cmd_kill(args.kill_all)
    if args.install_task:
        return cmd_install_task(os.path.abspath(sys.executable))
    if args.uninstall_task:
        r = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
             "Unregister-ScheduledTask -TaskName 'ModelRotator' -Confirm:$false "
             "-ErrorAction SilentlyContinue; 'done'"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=30)
        print("[uninstall-task] 计划任务已注销")
        return 0
    if args.reset_settings:
        if os.path.exists(OVERRIDE_CFG):
            try:
                os.remove(OVERRIDE_CFG)
                print("[reset-settings] 已删除 %s，下次重载回到 config.json 原状"
                      % OVERRIDE_CFG)
            except OSError as e:
                _eprint("[reset-settings] 删除失败: %s" % e)
                return 1
        else:
            print("[reset-settings] 无 %s，无需处理" % OVERRIDE_CFG)
        return 0

    # ---------- 一次性命令：走 HTTP ----------
    base = _resolve_base(args.host, args.port)
    if args.open_console:
        url = base + "/console"
        try:
            webbrowser.open(url)
            print("[console] 已在浏览器打开 %s" % url)
        except Exception:
            print("[console] 请手动访问 %s" % url)
        return 0

    one_shot = (args.status or args.list or args.switch or args.next
                or args.refresh or args.weight or args.enable or args.disable
                or args.clear_cooldown or args.stop)
    if one_shot:
        try:
            if args.status or args.list:
                snap = http_call(base, "GET", "/status")
                if not snap.get("models"):
                    raise RuntimeError("无响应")
                _print_status(snap)
                if args.status:
                    w = http_call(base, "GET", "/control/weights")
                    print("权重(完整): %s" % json.dumps(w.get("weights", {}),
                                                       ensure_ascii=False))
                return 0
            if args.switch:
                r = http_call(base, "POST", "/control/switch",
                              {"model": args.switch})
                print(("[switch] " + r.get("message", "")) if r.get("ok")
                      else ("[switch] 失败: " + r.get("error", str(r))))
                return 0 if r.get("ok") else 1
            if args.next:
                r = http_call(base, "POST", "/control/next", {})
                print(("[next] " + r.get("message", "")) if r.get("ok")
                      else ("[next] 失败: " + r.get("error", str(r))))
                return 0 if r.get("ok") else 1
            if args.refresh:
                r = http_call(base, "POST", "/control/reload", {})
                print(("[refresh] " + r.get("message", "")) if r.get("ok")
                      else ("[refresh] 失败: " + r.get("error", str(r))))
                return 0 if r.get("ok") else 1
            if args.weight:
                scores, bad = {}, []
                for item in args.weight:
                    if "=" not in item:
                        bad.append(item)
                        continue
                    k, v = item.split("=", 1)
                    try:
                        scores[k.strip()] = float(v)
                    except ValueError:
                        bad.append(item)
                if bad:
                    _eprint("[weight] 无法解析（应为 MODEL=SCORE）: %s" % ", ".join(bad))
                if not scores:
                    _eprint("[weight] 没有可用的权重赋值")
                    return 1
                r = http_call(base, "POST", "/control/weights",
                              {"scores": scores})
                print(("[weight] " + r.get("message", "")) if r.get("ok")
                      else ("[weight] 失败: " + r.get("error", str(r))))
                if r.get("unknown"):
                    print("[weight] 不在池内: %s" % ", ".join(r["unknown"]))
                return 0 if r.get("ok") else 1
            for flag, action in ((args.enable, "enable"), (args.disable, "disable")):
                if not flag:
                    continue
                for model in flag:
                    r = http_call(base, "POST", "/control/" + action,
                                  {"model": model})
                    print(("[%s] %s" % (action, r.get("message", ""))) if r.get("ok")
                          else ("[%s] 失败: %s" % (action, r.get("error", str(r)))))
                    if not r.get("ok"):
                        return 1
                return 0
            if args.clear_cooldown:
                r = http_call(base, "POST", "/control/clear-cooldown", {})
                print(("[clear-cooldown] " + r.get("message", "")) if r.get("ok")
                      else ("[clear-cooldown] 失败: " + r.get("error", str(r))))
                return 0 if r.get("ok") else 1
            if args.stop:
                http_call(base, "POST", "/control/shutdown", {})
                print("[stop] 已发送停止指令")
                return 0
        except Exception as e:
            _eprint("[cli] 无法连接 %s：%s" % (base, e))
            _eprint("[cli] 服务未运行？先执行 ModelRotator.exe 启动，"
                    "或加 --console 看日志。")
            return 2

    # ---------- 服务模式 ----------
    return _run_service(args.host, args.port, args.console)


def main():
    _force_utf8_stdio()
    sys.exit(cli_main(sys.argv[1:]))


if __name__ == "__main__":
    main()
