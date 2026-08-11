# 远程审批（Telegram 回答 Claude 的提问）

**状态**：一期（是/否）**已实现，交互式真机端到端验证通过**；二期（单选）、三期（排序）待做。

## 目标

away 模式下，Claude 需要用户决策时，不只是把问题推到 Telegram，还要能**在 Telegram 上作答**，答案回到会话里让 Claude 继续。分三期：

| 期 | 形态 | 例子 |
|---|---|---|
| 1 | 是/否 | `Claude needs your permission to use Bash` → 允许 / 拒绝 |
| 2 | 单选 A/B/C/D/E/F | `用哪个数据库？` → A) Postgres B) SQLite … |
| 3 | 排序 1/2/3/4/5 | `按优先级排列这几项` → 3,1,2 |

先做 1，验证通过再做 2、3。

## 可行性调研（2026-08-11，claude 2.1.227）

### 已否决：uds 消息通道

活跃会话的 `cc-socks/<pid>.sock` 只接受两类消息：

- `type:"user"` —— 普通用户消息（tinyCTB 直投功能已在用）
- `type:"control"` —— 仅 `rename` 与 `peer_message_status` 两个 action

**没有回答权限对话框的控制消息**。`can_use_tool` / `request_user_dialog` 是 remote-bridge（云端控制面）的 `control_request` 子类型，本地 socket 不接受。注入一条普通用户消息也无法解除一个正在等待按键的模态对话框。

### 已否决：`PreToolUse` hook

能返回决定，但**对每次工具调用都触发**，运行时还不知道这次是否真需要授权，因此必须自己维护"哪些工具危险"的名单、还要重新实现一遍 `permissions.allow` 的匹配规则——既啰嗦又必然与 Claude 的判定不一致（评审据此判为 P1）。

### 选定方案：`PermissionRequest` hook 返回决定

`PermissionRequest` 的语义是 **"Run before permission prompt"**——只在 Claude Code 真要弹权限框之前触发。这正是我们要的触发点：**已被 `permissions.allow` 允许的调用根本不会走到这里**，所以不需要名单、也不需要复刻规则匹配。

输出契约：

```json
{"hookSpecificOutput": {
  "hookEventName": "PermissionRequest",
  "decision": {"behavior": "allow", "updatedInput": …?, "updatedPermissions": […]?}
            | {"behavior": "deny", "message": "…"?, "interrupt": bool?}
}}
```

hook 同步阻塞、支持 per-hook `timeout`（秒），因此可以「推到 TG → 阻塞等待 → 拿到答案 → 返回决定」。

**注意**：`PermissionRequest` 在 headless（`-p`）下**不会触发**（实测：`default` 与 `acceptEdits` 两种模式均只触发 `PreToolUse`），因为 print 模式压根不弹框。故其真机验证只能在交互式会话中进行。

## 实测验证（2026-08-11，claude 2.1.227，隔离 `--settings` + 临时 hook）

四条语义全部实测确认，判据用**文件是否真被创建**（`touch` 标记文件），不看模型的自述——模型被拒时也会在回答里提到命令内容，按文本判断会误判为"已执行"。

| hook 返回 | 结果 | 说明 |
|---|---|---|
| `{}`（无意见） | 命令未执行 | 退回正常权限流程（headless 无法弹框即拦下；交互式则弹对话框） |
| `allow` | **命令执行** | `--permission-mode default` 下本需授权，allow 直接放行 |
| `deny` + reason | 命令未执行，**reason 抵达模型** | 见下 |
| 超时（hook 慢于 `timeout`） | 命令未执行 | **迟到的 `allow` 被丢弃**，不会放行 |

**阻塞成立**：hook 内 `sleep 8` 使整轮耗时 14s，会话确实等待 hook 返回 —— "推 TG → 等用户点按 → 返回决定"可行。

**`deny` + reason 回灌可用（二期关键）**：hook 以
`permissionDecisionReason: "用户已在 Telegram 上作答：选择 B) SQLite。请据此继续，不要再次提问。"`
拒绝一次 Bash 调用后，模型直接给出
「**最终结论：您选择使用 SQLite 数据库。** 根据您在 Telegram 上的回答……」
并**未重试该工具**。二期不必依赖任何非公开接口。

注：`AskUserQuestion` 在 headless（`-p`）下不暴露，模型会说"没有这个工具"，故二期的真实形态只能在**交互式会话**里验证。上面的等价实验证明了机制本身（reason 通道），交互式下的 `AskUserQuestion` 拦截仍需一次真机确认。

## 一期实现（已完成）

`tinyctb approval-gate`（隐藏子命令）注册为 `PermissionRequest` hook，`timeout` = 配置的审批等待 + 15s 余量。

流程：hook 建待审记录并注册三个回调按钮 → 入队 `approval_request` 事件（`origin=bridge`，`/back` 不清）→ daemon 带 inline keyboard 投递 → 用户点按 → daemon 落库决定 → 阻塞中的 hook 轮询到结果后返回 `permissionDecision`。

三个按钮：**✅ 允许** / **🔁 本会话都允许** / **❌ 拒绝**。中间那个写入会话+工具维度的自动放行，否则一个连续跑 Bash 的 agent 会话会变成"每调用一次点一次"。

**真机冒烟（2026-08-11 22:0x，切换到 PermissionRequest 之前跑的）**：喂入一条伪造 payload（Bash + 真实命令文本），Telegram 收到带按钮的请求，点「允许」后阻塞中的 hook 返回了 allow 决定；随后又点「拒绝」被正确拒收（提示"这条请求已经处理过了"）——**一次一答**在真机生效。该冒烟验证了「推送→按钮→落库→阻塞 hook 取回决定」整条链路；**切换事件后仅决定 JSON 的形状变了，链路未变，但仍欠一次交互式真机确认**。

## 安全规则（不可妥协）

- **超时绝不自动允许**：等待超时返回空对象（等价 `ask`），退回原有行为——会话停在对话框，与今天一致。宁可卡住也不能替用户点同意。
- **只在 away 模式生效**：人在电脑前时一切照旧，终端弹框。
- **拒绝优先**：任何解析不出的答案、过期的回调、来源不符的 chat/user，一律按未作答处理。
- **一次一答**：回调用后即失效（`telegram_callback_routes.used_at`）。
- **超时即作废**：待审记录带 `expires_at`；hook 放弃后把记录标记为 `expired`，之后再点按钮一律拒收并如实提示"已超时，会话已回到终端"，绝不谎报成功（尤其"本会话都允许"——hook 已退出，其副作用根本无法生效）。
- **超时与作答是原子转换**：`expire_or_take_decision` 用条件更新竞争同一行——赢了就标记过期，输了就把**已落库的决定读回来并遵守**。否则会出现"Telegram 显示已允许、会话却悄悄退回终端"的两不像状态。
- **审批请求必须带内容**：不能只说「需要权限」，要带工具名与具体入参（此前已实现 `pending_tool_use` 摘要，复用）。

## 实现要点

- 新增隐藏子命令 `tinyctb approval-gate`，注册为 `PermissionRequest` hook，`timeout` = 配置的审批等待 + 15s 余量（默认 300+15）。
- 过滤只剩两层：away 开启、且 `permission_mode != bypassPermissions`；`config.claude.approvalTools` 留空即"凡要问的都问"，填了则为可选收窄。
- 流程：hook 写入 `pending_approvals` 记录 + spool 一条事件 → daemon 推送带 inline keyboard 的消息 → 用户点按 → daemon 处理 `callback_query` 落库决定 → hook 轮询到决定后返回。
- 回调基础设施**已存在但未启用**：`telegram_callback_routes` 表、`extract_telegram_callback_route`、`TelegramCallbackAction::{Approve,Deny}`、`telegram_answer_callback_query` 都是移植期留下的，本期启用即可。
- 二期/三期（`AskUserQuestion`）是「选项应答」不是权限问题：优先试 `PermissionRequest` allow 分支的 `updatedInput`（把选择直接写回工具入参），退路是已验证可用的 deny + reason 回灌。

## 待验证 / 开放问题

- ~~`permissionDecisionReason` 能否承载「用户选了 B」~~ **已验证可用**；但二期现在有更正统的路子：`PermissionRequest` 的 allow 分支支持 **`updatedInput`**，可以直接把用户的选择写回 `AskUserQuestion` 的入参，不必再用 deny+reason 绕。二期实现前应先验证这条。
- ~~`PermissionRequest` 的真机验证~~ **已完成（2026-08-11 23:00）**：用 pty 起一个 `--permission-mode default` 的交互式会话执行未授权命令，`PermissionRequest` 如期触发 → 22:59:58 建待审 → 23:00:02 投递到 Telegram → 23:00:06 点「允许」后阻塞的 hook 放行 → 23:00:46 再点一次正确回报「已经处理过了」。**注意**：普通交互会话若带大量 `permissions.allow` 规则（本机 settings.local.json 有 395 条）几乎不会弹框，验证必须用干净的 default 模式会话。
- hook 长时间阻塞（数分钟）对会话其他部分（心跳、UI）的影响。
- 同一会话并发多个工具调用时，多条审批请求的对应关系（复用 `event_uid` 那套精确配对）。
- 三期排序的 Telegram 交互形态：依次点选 vs 一条文本回复（`3,1,2`）。倾向后者更省事且不易点错。
