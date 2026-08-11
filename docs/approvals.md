# 远程审批（Telegram 回答 Claude 的提问）

**状态**：一期（是/否）与二三期（选项 / 自由文本，已合并为一个功能）**均已实现并通过交互式真机端到端验证**。

## 目标

away 模式下，Claude 需要用户决策时，不只是把问题推到 Telegram，还要能**在 Telegram 上作答**，答案回到会话里让 Claude 继续。分三期：

| 期 | 形态 | 例子 |
|---|---|---|
| 1 | 是/否 | `Claude needs your permission to use Bash` → 允许 / 拒绝 |
| 2 | 单选 A/B/C/D/E/F | `用哪个数据库？` → A) Postgres B) SQLite … |
| 3 | 排序 1/2/3/4/5 | `按优先级排列这几项` → 3,1,2（实为 2 的自由文本形态）|

1 已单独完成；**2 与 3 调研后合并为一个功能**一次做完（理由见下）。

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

**`deny` + reason 回灌可用（早期路径，现已弃用，保留作记录）**：hook 以
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

## 二期与三期：合并为一个功能（2026-08-11 定）

用户观察「2 期和 3 期没啥区别」——调研确认这是对的，而且理由来自工具自身的设计。bundle 里 `AskUserQuestion` 的说明写着：

> AskUserQuestion **always includes a Skip button and a free-text input box** for custom answers

也就是说它本来就是**一个对话框、两种作答方式**：点选项 或 自由文本。所谓「A/B/C/D 单选」与「1/2/3/4/5 排序」不是两种问题类型（`questions[].options[{label,description}]` 里根本没有排序类型），而是同一个对话框的两种答案形态——排序只是自由文本的一个特例。因此二三期合并实现，一次做完。

**触发方式**：Claude Code 的 hook 事件全表（`PermissionRequest / PreToolUse / PostToolUse / PostToolUseFailure / Notification / Stop / PreCompact / PostCompact / UserPromptSubmit / SessionStart`）里**没有任何一个专门管问题对话框**，`PermissionRequest` 也不会为它触发（提问不是权限行为）。可用的只有 `PreToolUse`——它的返回值能把答案直接填进工具入参（见下方"正式契约"）。

关键是 **PreToolUse 的 matcher 就是工具名**，因此注册为 `matcher: "AskUserQuestion"`，只对该工具触发——不会重蹈「每次工具调用都启动一个进程」的覆辙。

**作答通道两条并存**：
- 选项 → inline keyboard，一点即答；
- 自由文本（含排序 `3,1,2`、Skip、以及任何不在选项里的答案）→ 对该消息用 Telegram 的 Reply 回复文字。

拿到答案后走**工具自身的正式契约**：`permissionDecision: "allow"` + `updatedInput = { questions: <原样>, answers: { "<问题文本>": "<答案>" } }`。`AskUserQuestion` 的 schema 里 `answers` 的说明是 *"The answers provided by the user (question text -> answer string; multi-select answers are comma-separated)"*，所以答案是作为**工具入参**送达、由工具正常完成并把答案作为结果交给模型，而不是靠模型去理解一条拒绝理由。

这里的 `allow` **不是权限授权**——提问工具没有副作用；授权仍然只能来自审批按钮。（早期版本用 deny + reason 回灌，实验证明可行但依赖模型理解，已弃用。）

**答案保真规则（2026-08-12 三审 P2 修正）**：字母代号（`A` / `a,c`）只在**整条答案全是代号**时才展开成 label；除此以外答案**一字不改**地送达。早期版本无条件按逗号拆分再重组，制造了两个 bug：点了 label 含逗号的按钮（`Washington, D.C.`）会被重组成 `Washington,D.C.`；自由文本 `A, but only locally` 会把开头的 `A` 换成第一个选项——而"but only locally"才是这句话的重点。识别顺序也因此固定为**先精确匹配 label，再考虑代号展开**。

**新契约真机验证（2026-08-12 01:01）**：交互式会话调用 `AskUserQuestion`，答案经 `allow + updatedInput.answers` 送回后，**工具正常完成并把答案作为结果交给模型**（`Your questions have been answered: … = "交给 Sol 再审"`）——与旧 deny 路径返回一条错误形成明确对比。

**旧路径真机验证（2026-08-12 00:00，deny + reason，现已弃用）**：由一个真实交互式会话调用 `AskUserQuestion`（那条问题本身即验证）→ 00:00:54 带 A/B/C 按钮推送到 Telegram → 00:01:26 用户点选 →「已作答：提交给 Sol 评审」→ 答案原样回灌进会话，模型据此继续未重试。同一晚也验证了审批的 **deny** 路径：一条未被 allow 规则覆盖的 Bash 调用被远程拒绝，会话收到 `Denied from Telegram` 且命令未执行。

★验证方法教训：**不要用 pty 脚本自动起会话**——新会话首次进入某目录会弹「信任文件夹」对话框，模拟按键时机极难对上，三次尝试全部卡死空等。直接在一个已存在的交互式会话里调用该工具即可。

**按钮样式（2026-08-12 与用户逐版敲定，定稿=彩色圆点+字母）**：选项按钮形如 `🔴A 选项文本`，颜色序列 `🔴🟠🟡🟢🔵🟣🟤⚫`。两个要素缺一不可——**颜色**让整行一眼读成按钮（与审批的 ✅🔁❌ 同一类），**字母**让多个选项彼此好区分、也是文字作答时可以直接说的代号。

逐版试错记录（避免重走）：纯 `A.` 前缀→混进正文文字，看不出是按钮；深色 keycap `1️⃣`→同样偏暗；区域指示符 `🇦🇧🇨`→用户客户端根本不渲染成彩色；只有彩色圆点无字母→够亮但"没有 ABCDE 好区分"。Unicode 没有全套 A–Z 彩色字母（`🅰️🅱️` 只到 B 且同为红色），所以"圆点供色 + 字母供辨"是现有字符集下的最优解。

选项文本**只出现在按钮上**，正文不再重复列一遍——重复会让按钮读起来像又一段文字。（多选无按钮时正文才列 A/B/C。）

**排布**：按标签显示宽度贪心装箱（CJK/emoji 记 2 列，预算 32 列，每行最多 3 个）。短标签并排（两个左右分布最好看），长标签自动各占一行。

**多选**：`multiSelect: true` 的问题**不给按钮**——一次点击会立刻提交并丢掉其余选项。这类问题只列出 A/B/C 并要求回复逗号分隔（`A,C`），正是工具文档规定的多选答案形状。答案里的字母会按选项顺序还原成 label，不在范围内的原样保留（自由文本因此不受影响）。

**已知限制**：一次调用含多个问题时不接管（回退终端对话框）；选项按钮最多 8 个。

## 授权边界（用户 2026-08-12 定）

**只有"对话框内"的作答才具有效力**，其余一律当作对话：

| 来源 | 语义 | 处理 |
|---|---|---|
| 审批消息下的**按钮** | 肯定句 = 授权 | 唯一能放行工具的途径 |
| 对**问题消息**的 Reply 文本 | 在对话框内作答（含排序 `3,1,2`） | 作为该问题的答案 |
| 对**审批消息**的 Reply 文本 | 不构成授权 | 拒收并提示改用按钮，**且不注入会话** |
| 普通消息（未 Reply 任何对话框） | 对话 / 疑问句 | 走原有注入或无路由提示，问题继续等待 |

Telegram 里「对话框内」映射为「Reply 明确指向那条对话框消息」——这是唯一等价于"在对话框输入框里打字"的动作，它显式指名了在回答哪一个对话框。

**识别与状态解耦**：`dialog_messages` 表登记该对话框的**每一个分片**消息（长问题会被拆成多条，按钮在最后一片），查找只按消息定位、不看是否仍开放。已作答或已超时的对话框，其回复仍被识别并如实告知状态（"已处理过""已超时，会话已回到终端"），**绝不漏回普通会话注入**。

这条边界是被一个真实漏洞逼出来的：此前文字回复审批消息会走普通回复路由被**注入会话**，用户以为已作答，实际审批仍在空等超时。

## 安全规则（不可妥协）

- **超时绝不自动允许**：等待超时返回空对象（等价 `ask`），退回原有行为——会话停在对话框，与今天一致。宁可卡住也不能替用户点同意。
- **只在 away 模式生效**：人在电脑前时一切照旧，终端弹框。
- **拒绝优先**：任何解析不出的答案、过期的回调、来源不符的 chat/user，一律按未作答处理。
- **一次一答**：回调用后即失效（`telegram_callback_routes.used_at`）。
- **文字永不构成授权**：权限只能由按钮给出；对审批消息的文字回复被拒收且不注入会话。
- **超时即作废**：待审记录带 `expires_at`；hook 放弃后把记录标记为 `expired`，之后再点按钮一律拒收并如实提示"已超时，会话已回到终端"，绝不谎报成功（尤其"本会话都允许"——hook 已退出，其副作用根本无法生效）。
- **超时与作答是原子转换**：`expire_or_take_decision` 用条件更新竞争同一行——赢了就标记过期，输了就把**已落库的决定读回来并遵守**。否则会出现"Telegram 显示已允许、会话却悄悄退回终端"的两不像状态。
- **审批请求必须带内容**：不能只说「需要权限」，要带工具名与具体入参（此前已实现 `pending_tool_use` 摘要，复用）。

## 实现要点

- 新增隐藏子命令 `tinyctb approval-gate`，注册为 `PermissionRequest` hook，`timeout` = 配置的审批等待 + 15s 余量（默认 300+15）。
- 过滤只剩两层：away 开启、且 `permission_mode != bypassPermissions`；`config.claude.approvalTools` 留空即"凡要问的都问"，填了则为可选收窄。
- 流程：hook 写入 `pending_approvals` 记录 + spool 一条事件 → daemon 推送带 inline keyboard 的消息 → 用户点按 → daemon 处理 `callback_query` 落库决定 → hook 轮询到决定后返回。
- 回调基础设施**已存在但未启用**：`telegram_callback_routes` 表、`extract_telegram_callback_route`、`TelegramCallbackAction::{Approve,Deny}`、`telegram_answer_callback_query` 都是移植期留下的，本期启用即可。
- 二三期：`tinyctb question-gate` 注册为 `PreToolUse` hook 且 `matcher: "AskUserQuestion"`（只对该工具触发）。单选 → inline keyboard；自由文本 / 多选 → Reply 该消息（逗号分隔）。答案经 **`allow` + `updatedInput.answers`** 送回（工具正式契约），`answers` 的 key 用**原始问题文本**（不 trim，否则与 `questions[].question` 不一致，工具会认为没作答）。

## 待验证 / 开放问题

- ~~`permissionDecisionReason` 能否承载「用户选了 B」~~ 已验证可用，但**已弃用**：改走 `PreToolUse` 的 `allow + updatedInput.answers` 正式契约（`PermissionRequest` 确实不为 `AskUserQuestion` 触发，但 `updatedInput` 在 `PreToolUse` 上可用）。
- ~~`PermissionRequest` 的真机验证~~ **已完成（2026-08-11 23:00）**：用 pty 起一个 `--permission-mode default` 的交互式会话执行未授权命令，`PermissionRequest` 如期触发 → 22:59:58 建待审 → 23:00:02 投递到 Telegram → 23:00:06 点「允许」后阻塞的 hook 放行 → 23:00:46 再点一次正确回报「已经处理过了」。**注意**：普通交互会话若带大量 `permissions.allow` 规则（本机 settings.local.json 有 395 条）几乎不会弹框，验证必须用干净的 default 模式会话。
- hook 长时间阻塞（数分钟）对会话其他部分（心跳、UI）的影响。
- 同一会话并发多个工具调用时，多条审批请求的对应关系（复用 `event_uid` 那套精确配对）。
- ~~三期排序的交互形态~~ **已定**：一条文本回复（`3,1,2`），与选项按钮并存。
