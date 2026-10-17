# ModelRotator

> 本地 OpenAI 兼容的「模型定时轮换代理」。在多个上游模型之间按时间窗轮换，命中限流/5xx 时自动跳过并冷却，对客户端只暴露一个统一的 `AutoRotate` 模型。零第三方依赖，仅用 Python 标准库。

**ModelRotator** is a local, OpenAI-compatible proxy that rotates upstream models on a timer and fails over on rate-limits. Clients talk to a single endpoint/model; the proxy fans requests out across a pool of real models. No third-party dependencies — Python standard library only.

---

## 它解决什么问题

把多个供应商/自定义模型接进一个 OpenAI 兼容客户端（如各类 IDE 插件、CLI、自研 Agent）时，单个模型很容易被限流（429）或偶发 5xx。直接固定用一个模型，高峰就卡住；手动换模型又打断工作流。

ModelRotator 在它们之上再放一层：

- 客户端只配置一个 `AutoRotate` 模型，指向本代理；
- 代理按「时间窗轮换 + 失败自动跳过并冷却」把请求路由到池内真实模型；
- 对客户端完全透明（标准 `/v1/chat/completions` + SSE 流式透传）。

## 特性

- **零依赖**：仅 Python 标准库。
- **时间窗轮换**：每 `rotate_minutes` 切到池内下一个模型；同一会话在窗内粘住同一模型，避免上下文跨模型跳变。
- **失败转移**：命中 `fail_status`（429/5xx 等）或异常 → 该模型进入 `cooldown_minutes` 冷却，立刻改投下一个可用模型。
- **池内全冷却**：清除冷却重试一轮；仍不行则返回 429 并带 `X-Rotate-Reason`。
- **完整透传**：流式 SSE / chunked 与非流式 JSON 都原样转发；逐条携带各自的 `base_url` 与 `api_key`。
- **会话粘性**：同 `X-Rotate-Session` / `user` 在窗内恒定路由到同一模型。
- **单实例保护**：启动前先探测本机端口健康，已有实例在跑则主动退出，避免端口互抢。
- **只读上游清单**：从 `source_models_json` 读取密钥，绝不回写。

## 工作原理

```
客户端 ──► ModelRotator (127.0.0.1:3166)
                │
                ├─ 时间窗轮换：每 N 分钟切下一个模型
                ├─ 会话粘性：同一 session 在窗内粘住同一模型
                └─ 失败转移：429/5xx/异常 → 冷却 → 改投下一个
                          │
                          ▼
            池内真实模型 A / B / C / …（各自 base_url + key）
```

路由策略：

1. **时间窗**：每 `rotate_minutes` 切到池内下一个模型；同一会话在窗内粘住同一模型，避免上下文跨模型跳变。
2. **失败转移**：命中 `fail_status`（429/5xx）或异常 → 该模型进入 `cooldown_minutes` 冷却，立刻改投池内下一个可用模型。
3. **池内全冷却**：清除冷却重试一轮；仍不行则返回 429 并带 `X-Rotate-Reason`。

## 快速开始

```bash
# 1) 准备配置（复制示例并按需修改）
cp config.example.json config.json

# 2) 前台运行（看到日志即成功）
python rotator.py

# 3) 健康检查
curl http://127.0.0.1:3166/health
# => {"ok":true,"active_model":"...","uptime_seconds":...}
```

客户端把 `base_url` 设为 `http://127.0.0.1:3166/v1`，`model` 设为 `config.json` 里的 `expose_as`（示例为 `AutoRotate`），`api_key` 随便填（代理不校验客户端 key）。

## 配置（`config.json`）

| 字段 | 说明 | 默认 |
|------|------|------|
| `listen_host` | 监听地址 | `127.0.0.1` |
| `listen_port` | 监听端口 | `3166` |
| `rotate_minutes` | 时间窗长度（分钟） | `30` |
| `cooldown_minutes` | 失败模型冷却时长（分钟） | `10` |
| `fail_status` | 触发冷却的 HTTP 状态码列表 | `[408,409,425,429,500,502,503,504]` |
| `fail_on_exception` | 连接异常是否也触发冷却 | `true` |
| `rotate_on_first_request` | 收到首个请求才推进时间窗（避免空窗消耗） | `true` |
| `source_models_json` | 密钥源文件（WorkBuddy 风格的 `models.json`，提供 `apiKey`） | `~/.workbuddy/models.json` |
| `pool` | 上游模型池，见下方三种写法 | 必填 |
| `expose_as` | 对客户端暴露的模型名 | `AutoRotate` |
| `log_file` | 日志路径；省略则写到程序目录 `rotator.log` | 程序目录 |

### 池成员（`pool`）的三种写法

```jsonc
"pool": [
  // 1) 仅写 id：从 source_models_json 里取该模型的 url + api_key
  "gpt-4o-mini",

  // 2) 内联 url + 显式 key（自包含，不需要 source_models_json）
  { "id": "claude-3-5-haiku", "url": "https://api.anthropic.com/v1", "key": "sk-ant-..." },

  // 3) 内联 url + 借用密钥：key 从 source_models_json 里某个 id 的 apiKey 取
  { "id": "my-model", "url": "http://127.0.0.1:9000/v1", "key_from": "gpt-4o-mini" }
]
```

> 形式 1/3 依赖 `source_models_json`（典型如 WorkBuddy 的 `~/.workbuddy/models.json`）。形式 2 完全自包含，适合对接任意 OpenAI 兼容网关。

## 控制脚本（Windows）

`rotator-ctl.ps1` 用 PowerShell 管理进程，并能注册「开机自启 + 5 分钟保活」的计划任务：

```powershell
powershell -ExecutionPolicy Bypass -File rotator-ctl.ps1 status        # 查看状态
powershell -ExecutionPolicy Bypass -File rotator-ctl.ps1 start         # 启动（优先用计划任务）
powershell -ExecutionPolicy Bypass -File rotator-ctl.ps1 stop          # 停止
powershell -ExecutionPolicy Bypass -File rotator-ctl.ps1 restart       # 重启
powershell -ExecutionPolicy Bypass -File rotator-ctl.ps1 install       # 注册开机自启计划任务
powershell -ExecutionPolicy Bypass -File rotator-ctl.ps1 uninstall     # 移除计划任务
powershell -ExecutionPolicy Bypass -File rotator-ctl.ps1 testautostart # 停→用计划任务起→验证
```

脚本自动定位 `pythonw.exe`：优先 `$env:PYTHONW` → 已注册计划任务里的解释器 → PATH 上的 `pythonw` → 否则回退 `pythonw.exe`。macOS / Linux 可去掉计划任务部分，直接用 `nohup python rotator.py &` 或 systemd 保活。

## 健康检查与状态

| 端点 | 说明 |
|------|------|
| `GET /health` 或 `/healthz` | `{ok, active_model, uptime_seconds}` |
| `GET /status` 或 `/v1/rotator/status` | 完整快照：各模型冷却剩余、成功/失败计数、距下次轮换秒数、会话粘性数 |
| `GET /v1/models` | 只暴露一个 `expose_as` 模型，供客户端发现 |

## 探测模型真实能力

上游网关 `/v1/models` 报的 `supports_tool_call` 经常不可信（有的标 `false` 实际能返回 `tool_calls`，有的标可用实际 404）。`probe_models.py` 逐个发真实工具调用请求，用实测结果决定能否进池：

```bash
python probe_models.py                       # 探测默认网关（http://127.0.0.1:3065/v1）
python probe_models.py --only a,b,c         # 只测这几个
python probe_models.py --agent2api http://127.0.0.1:9000/v1 --models-json /path/to/models.json
```

输出 `probe_result.json` + 终端表格，分三类：`工具调用可用` / `仅对话可用` / `不可用`。

## 安全说明

- **密钥不落盘到本仓库**：`config.json` 已被 `.gitignore` 排除，仅本地存在；请复制 `config.example.json` 自行填写。
- **日志脱敏**：写日志前会自动把 `sk-...` / `Bearer ...` 等密钥打码（`****`）。
- **只读上游清单**：只从 `source_models_json` 读取密钥，绝不回写。
- **本地代理**：默认监听 `127.0.0.1`，不对外暴露；如需对外请自行加反代与鉴权。

## 排障要点

实现 / 部署时踩过的坑，已固化进代码，记录于此供二次开发参考：

1. **端口别撞车**：被其它本地代理抢占的端口会返回它自己的 502（文案是 `upstream connect failed`），极易误判成本代理故障。换一个空闲端口。
2. **进程必须被服务/计划任务持有**：用会话内 `Start-Process` 或后台任务启动的进程会随启动它的 shell 一起被回收，表现为「服务莫名消失」。用计划任务（Windows）或 systemd / nohup（*nix）持有。
3. **重复代理环境变量**：环境同时有 `HTTPS_PROXY` 和 `https_proxy` 时，`Start-Process` 会抛 `Item has already been added`。`rotator-ctl.ps1` 在启动前清理重复变量。
4. **计划任务必须用 `pythonw.exe`**：任务计划程序无法隐藏启动控制台程序（`python.exe`），会返回 `0x800710E0`。脚本直接跑 `pythonw.exe rotator.py`。
5. **SSL context 要装进 `HTTPSHandler`**：`OpenerDirector.open()` 不接受 `context` 参数，SSL context 必须装进 `HTTPSHandler(context=...)`，否则所有 POST 静默断开且 pythonw 无 stderr 不可见。
6. **必须绕过环境代理**：urllib 默认读 `HTTP(S)_PROXY`，会把发往 `127.0.0.1` 的上游请求也绕出去导致连接被拒。已统一用 `ProxyHandler({})` 的 opener 直连。
7. **透传时丢弃上游的 `Content-Length` / `Transfer-Encoding`**：由本层按实际转发方式重算，否则响应头冲突导致客户端解析异常。
8. **验证本机服务要绕过沙箱/公司代理**：Bash 里的 `curl` 可能被沙箱代理劫持返回假 502，误判成故障。验证用 PowerShell `Invoke-WebRequest -Proxy $null` 或 Python 的 `ProxyHandler({})` 直连。
9. **怀疑进程被回收时查端口监听**：用 `Get-NetTCPConnection -LocalPort 3166`（PowerShell）确认是否还在监听，不要只看 `curl`/`Invoke-WebRequest` 的结果。

## 许可证

[MIT](./LICENSE) © 2026 Jiayu1983
