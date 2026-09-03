# 远程审批（Telegram 回答 Claude 的提问）

**状态**：一期（是/否）、二三期（选项 / 自由文本，已合并为一个功能）、四期（无头 turn 的审批）**均已实现并通过真机端到端验证**。

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

### 已否决（仅限交互式会话）：`PreToolUse` hook

能返回决定，但**对每次工具调用都触发**，运行时还不知道这次是否真需要授权，因此必须自己维护"哪些工具危险"的名单、还要重新实现一遍 `permissions.allow` 的匹配规则——既啰嗦又必然与 Claude 的判定不一致（评审据此判为 P1）。

**这条否决只对交互式会话成立。** 无头（`-p`）turn 里 `PermissionRequest` 根本不触发，`PreToolUse` 是唯一可用的事件，所以四期正是用它来拦无头 turn——代价（工具名单、每次调用起进程）也如实付了：靠 matcher 把它限定在会改东西的那几个工具上。见文末「四期」。

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

`tinyctb approval-gate`（隐藏子命令）注册为 `PermissionRequest` hook，`timeout` = **任一网关可能选择的最长等待 + 15s 余量**（即 `WINDOWLESS_APPROVAL_WAIT` = 24h，故实际写入 86415）。这是天花板不是等待值本身：有终端窗口的会话仍按 `approvalTimeoutSeconds` 到期回落终端，无窗口的后台会话才用满长窗——只按配置值供给会让 harness 提前杀掉后者，把它要避免的陷阱原样重建。

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

- **超时绝不自动允许**。但"不允许"具体是什么，取决于 hook 下游还有没有人：交互式会话返回空对象（等价 `ask`），退回终端弹窗，与今天一致；**无头 turn 必须直接拒绝**——它跑在 `bypassPermissions` 下且背后没有终端，那里返回空对象等于放行。四期加的这条同样是"宁可卡住也不能替用户点同意"，只是换了个方向实现。
- **away 只门控交互式网关**：人在电脑前时终端会话一切照旧，终端弹框。无头 turn 不看 away——它是 Telegram 发起的、无论用户坐在哪里都没有终端，审批永远走 Telegram（四期二审修正，初版误将 away 检查共用导致 away 关闭时无头审批整体失效）。
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

## 四期：无头 turn 的审批（2026-08-12）

### 起因：远程审批恰好没覆盖最需要它的那些 turn

一期做的审批网关挂在 `PermissionRequest` 上，只对**用户自己开的交互式会话**生效。而你人在外面、从 Telegram 发起的那些 turn——最看不住的那批——反而一路裸奔。三个实测事实锁死了这个结论（脚本在 `~/.tinyctb/exp/headless-flow/`）：

| 实验 | 结果 |
|---|---|
| `claude -p --resume X` 连续三轮 | **不分叉**：session_id 始终不变、单个 transcript、3 个 user turn 依次累积。无头链路本身是连续的，TG 多轮和用户后来在终端打开的是同一条会话 |
| 无头里调 `AskUserQuestion` | **工具根本不存在**（两种 permission mode 都一样，模型 ToolSearch 明确返回 "No matching deferred tools found"）。三期做的问答网关对无头 turn 一次也不会触发 |
| 无头 + `--permission-mode default` | 需要授权的调用被**沙箱直接拒掉**（marker 文件没生成），无头模式没有弹窗可弹。这就是 bridge turn 用 `bypassPermissions` 的原因 |
| 无头 + `PreToolUse` hook | **触发**，payload 完整：`tool_name` / `tool_input` / `tool_use_id` / `permission_mode` / `cwd` / `session_id` |

最后一行是出路：`PreToolUse` 的 `permissionDecision: allow|deny` 和问答网关用的是同一套机制。

### 设计

**两个网关按 `bypassPermissions` 精确二分**，互不重叠也不留缝：交互式会话（非 bypass）归 `PermissionRequest`，无头 turn（bypass）归 `PreToolUse`。交互式会话即使切进 bypass 模式也不会弹权限框，所以一期网关本来就听不到它的调用——二分没有丢东西。有测试钉住这个划分（`the_two_gates_do_not_overlap`）。

**★超时语义在两侧相反，这是本期最要紧的一条**。一期的规则是"超时=不表态，回落到终端弹窗"。无头 turn 背后**没有终端**，而且跑在 `bypassPermissions` 下——在那里"不表态"等于"放行"。所以无头侧**超时必须直接拒绝**，并在拒绝理由里告诉模型"没有执行、不要重试、把你打算做的事告诉用户然后停下"。

**复审后收紧的三条（2026-08-12 二审）**：

1. **away 只门控交互式网关**。Telegram 的 `/new` 和 Reply 在 away 关闭时照样起无头 turn——那个 turn 无论用户坐在哪里都没有终端，所以无头网关完全不看 away。初版把 away 检查放在两个网关共用的最前面，等于 away 关闭时无头审批整个失效，`headlessApprovalTools` 里的工具直接执行。
2. **错误也遵循同样的不对称**：交互式网关内部出错降级为"不表态"（终端弹窗兜底，安全）；无头网关一旦确认是运行中的 bridge turn，任何内部错误（配置读不了、SQLite 失败、事件入队失败）都**直接拒绝**并把错误写进拒绝理由——那里没有任何东西兜底，"不表态"就是执行。初版所有错误都降级成 `{}`，等于 fail open。
3. **准入判定=「该会话有 status='running' 的 bridge turn」，且在建审批/推送之前最先做**。只看表成员资格会让"上周跑过一次 Telegram turn"的会话被永远纠缠；判定放在最后则会给用户自己的 `--dangerously-skip-permissions` 终端会话建审批、白等整个超时，还谎称"超时回落弹窗"（bypass 模式根本没有弹窗，超时后照样执行）。现在这类会话在任何有副作用的步骤之前就被无声放过——不建审批、不推送、不等待、也不暴露在 fail-closed 错误处理之下。首工具竞态由**spawn 前注册**根治：`bridge_turns` 行在进程存在之前就已落库（CLI 的 `tinyctb new`/`reply` 也一并走这条路，此前它们从不注册）。

**工具过滤是唯一挡住热路径的东西**。`PreToolUse` 对**每一次**工具调用触发，所以 `headlessApprovalTools` 的空列表含义与 `approvalTools` **正好相反**：后者空=全都问（`PermissionRequest` 只在真需要决定时才触发，没什么可滤的），前者空=**关闭功能**（否则连 Read/Grep 都要你掏手机）。默认值 `["Bash","Write","Edit","MultiEdit","NotebookEdit"]`——只有会改东西的工具。`WebFetch` 想加可以加，默认不加是因为它太常见。

实测开销：无头网关的快路径**不读 away 标记也不开数据库**——非 bypass 调用在模式分流处退出，bypass 但无令牌（用户自己的终端会话）在环境变量检查处退出。**200 次调用共 107ms，约 0.53ms/次**（三审后复测）。away 标记文件只属于交互式网关的快路径。

**⚠️ 已知缺口（二审后已收窄）**：matcher 在 `hooks install` 时写死进 `settings.json`，改了 `headlessApprovalTools` 不重装则新工具不会被拦。现在 `hooks status` 按 marker 分别核对四个 hook**并比对 matcher 与当前配置**，不一致即报 `installed: false`——`/away` 复用这个判定，会自动重装。剩余缺口只在"改完配置后既不跑 status 也不碰 /away"的窗口期。matcher 取"内置默认 ∪ 配置"的并集，保证内置那批无论配置怎么改都仍然被拦。

### 真机冒烟（2026-08-12 18:0x）

隔离在 `TINYCTB_STATE_DIR` 里跑（子进程导出该变量，hook 继承），不碰生产库也不发 Telegram 消息。判据是 **marker 文件是否存在**，不是模型自述——本项目早先有一次实验就是因为信了模型自述而完全跑偏。

| 场景 | 命令是否执行 | 落库 decision | 模型收到 |
|---|---|---|---|
| A 无人答复（15s 超时） | **NO** | expired | "not run — the Telegram approval timed out"，未重试 |
| B 点 ✅ 允许 | **YES** | allow | Done |
| C 点 ❌ 拒绝 | **NO** | deny | "denied … was not created" |

推送事件带 `headless: true`、三个按钮、正文是真实命令。Telegram 提示语按此分流：无头那条明说"背后没有终端可回落——超时不答复=拒绝，任务就停在这里"，交互式那条说"超时后回落到终端弹窗"。不说清楚的话，忽略这条消息看起来是免费的。

### 仍未解决

- 无头 turn **没法中途提问**（工具不存在）。需要拿主意时它只能猜，或者把问题留到 turn 结尾由用户回复继续。给模型加一条"别猜、把选择题放到结尾"的提示是候选方案，尚未做。
- ~~matcher 陈旧问题没有自动检测~~ **已解决（二审）**：`hooks status` 按 marker 分别核对并比对 matcher 与当前配置，不一致报 `installed: false`，`/away` 会自动重装。

### 三审修正（2026-08-12，Sol 复审 1P1+2P2）

**[P1] spawn 后身份补写失败会让活进程脱离审批边界**。初版 `let _ =` 丢弃 UPDATE 错误：pid 留空 → daemon 10 秒宽限后判 turn 失败 → turn 不再是 running → 该进程后续所有工具调用被网关放过，且 daemon 无法按 PID 回收。现在补写**重试 3 次并核对恰好更新 1 行**（针对 daemon 自身连接造成的瞬时 SQLITE_BUSY）；仍失败则**杀掉刚起的进程组、把 turn 结算为 failed、向调用方报错**——宁可这个 turn 没跑成，也不能留一个无人监管的活进程。注入式回归测试：`spawn_that_cannot_be_recorded_is_terminated`。

**[P2] 准入第一层改为环境令牌，数据库退居第二层**。bridge 起的每个无头进程带 `TINYCTB_BRIDGE_TURN=<turn_id>` 环境变量（hook 是 claude 的子进程，天然继承）。网关先看令牌：**无令牌 = 非 bridge 进程，立即放过，全程不碰数据库**——状态目录烧成灰也波及不到用户自己的 bypass 终端会话（测试：把 TINYCTB_STATE_DIR 指向一个普通文件仍返回 `{}`）。有令牌才开库核对该 turn 是否仍 running：已结算（如超时被杀）的 turn 的余党调用**直接拒绝**，不再新建指向已关窗口的审批。令牌只存在于该 turn 的进程树里，"上周的 Telegram turn 纠缠今天的终端会话"从结构上不可能。⚠️ 部署瞬间已在跑的存量 turn（旧二进制起的、无令牌）不受门控，属一次性窗口。

**[P2] 未知 decision 按网关分流**：交互式 `{}` 回落终端弹窗；无头侧 `{}` 即执行，故改为明确 deny 并写明原因。

三处修复均按惯例回退验证过测试确实变红。

### 四审残余（2026-08-12）

身份补写 UPDATE 限定 `AND status='running'`（daemon 抢先结算的 turn 不可被误报成功）；结算错误经 `settle_failed_turn` 重试并折进错误链，连结算都失败时的遗留契约=行保持 running/pid NULL 由 daemon 宽限期响亮兜底；两种交错各有注入测试。

### 五审修正（2026-08-12，反向竞态）

四审那条竞态还有**镜像**：daemon 用快照判死。交错=daemon 读到 `pid NULL` → 身份补写成功（调用方拿到"已启动"）→ daemon 拿旧快照判"无 pid=死亡"→ 推失败通知+无条件写 failed → 令牌指向 failed 行、后续受控调用全被拒、结果永不投递。修复=**先认领后宣判**：失败转换改为条件 UPDATE（CAS），"无 pid"这一判据在写入时重新断言（`status='running' AND pid IS NULL`）；0 行=判据已失效，**既不写 failed 也不发 thread_error**，turn 照常运行。超时判死不依赖快照证据，不带 pid 条件。测试 `identity_write_landing_after_the_snapshot_averts_the_failure_verdict` 钉住该交错并断言无 thread_error；回退成无条件写立即变红。

### 六审修正（2026-08-13，判死副作用的崩溃一致性）

CAS 之后副作用的**顺序**还有两处崩溃窗口：①claim 提交后、kill 前 daemon 退出→turn 已是 expired、被所有后续 `list_running_bridge_turns` 扫描隐藏，**活进程永远无人回收**；②kill 后通知入队失败→turn 已终态、下一轮不重试，**用户永远收不到 thread_error**。修复=重排：**kill 在任何提交之前**（此时崩溃则 turn 仍 running，下一轮重判超时并重复幂等的 kill）；**claim 与失败通知在同一事务提交**（入队失败则 claim 一起回滚，整个判决下一轮重试；不可能出现"已结算却没人被告知"）。测试 `broken_outbox_rolls_back_the_claim_and_the_kill_is_repeatable` 用 SQLite ABORT trigger 模拟 outbox 故障，断言 claim 回滚、kill 已先落地、修复后重试恰好投递一次；回退成旧顺序立即变红。155 测试全绿。

## 键盘在场检测（2026-08-15，用户否决"长窗口阻塞终端"后定案）

用户要求：**终端无论 away 与否都必须能直接响应**；同时 TG 推送和 /threads 重发在窗口未过期时有效。两侧同时可答在 Claude Code 现有接口下做不到（对话框无法远程作答、uds 无对话框控制消息、注入用户消息不解除模态框——一期调研已证），但可以做到**谁在场谁优先、切换以毫秒计**：

**信号 = 最近一次按键**（daemon 常驻 `xinput test-xi2 --root`，**只挑 RawKeyPress**，时间戳写入 `input-activity.json`；网关读该文件判 15 秒窗口）。★2026-08-17 换掉了原来的「桌面输入空闲时间」（Mutter IdleMonitor via D-Bus）：它把键盘与指针混在一起计时，而 Logitech G502 传感器漂移每秒产生 ~600 次零位移 Motion，把 idle 长时间压在 1–48ms，导致每条审批在第一次轮询（100ms）就被判「人在键盘前」转交终端——按钮在手机上只活 0.1 秒，`/threads` 事后查无活请求。gdbus introspect 确认该服务只暴露 `/Core`，**没有按设备路径**，无法拆分键鼠，故必须在事件层过滤；`/dev/input` 因用户不在 `input` 组不可读，XInput2 是零权限变更的路径。随之删除的旧机制：gdbus/xprintidle 探测、`presence-probe.json` 共享状态、flock 租约、双重检查、指数退避、时钟回拨校验及其 9 条专属测试。**判据方向**：读不到记录（无 X 会话／监听器未起／纯 SSH／时间戳来自未来）一律判**不在场**，提示留在手机上；away 是用户声明，看不见的守卫不该推翻它。写端节流用单调时钟（`Instant`），避免墙钟回拨后拒绝记录真实按键。⚠️更早还用过 pts atime，被 Sol 用真机 pty 实验证伪（raw 模式读取键入 atime 纹丝不动）——间接证据当成了机制验证。`/back` 仍是确定性释放路径。

规则（approval 与 question 两个交互式网关一致，无头网关不涉及）：

0. **会话级 auto-allow 先于一切在场检查**（已授权的绝不因人在键盘前复弹终端框）；
1. **away 开启即推送**：桌面在做什么完全不参与「发不发」的决定——2026-08-17 实测鼠标传感器漂移每秒发 ~600 次幽灵 motion，把桌面 idle 压在 0 附近，旧的「判在场就不建行、不推送」会让手机**完全收不到消息**且用户无从察觉。推送先落库再由桌面活动决定让路，最坏情况从「消息不存在」变成「消息很快被终端接管」。
2. **键盘空闲 → 远程窗口照常**（推按钮、/threads 可重发、最长可配 24h——clamp 已放宽到 86400）；
3. **等待中用户回来（**按键**苏醒 或 `/back`；指针移动不算）→ 原子结算转终端**：已落库的 tap 仍然生效（`expire_or_take_decision` 竞争），否则标 expired、返回不表态、终端弹框；之后再点 TG 按钮收到诚实的"已超时，会话已回到终端"。释放延迟=轮询粒度 100ms（监听器实时写时间戳，无探测节流）。★注意终端侧的**可答性**并不依赖这个释放：2026-08-17 pty 实测确认 Claude Code 在 hook 阻塞期间就已渲染自己的权限对话框（对话框出现在 hook 起点而非返回点），释放只决定远程窗口何时关闭。**`/back` 在释放判定里最先检查**，不依赖监听器是否在跑——不依赖任何桌面接口，SSH 场景也确定有效，延迟即轮询粒度（100ms）。

由此长远程窗口变得无代价：在电脑前对话框始终在终端可答（hook 阻塞期间即已渲染），手机那份随桌面活动交回；不在时窗口一直开着。本机 `approvalTimeoutSeconds` 已配 3600。

测试五条：away 开启即推送（在场也建行也推送，随后才让路）、auto-allow 优先于在场、用户返回即释放（行 expired+曾推送）、`/back` 即释放（无桌面信号）、tap 抢先仍生效；回退各变红。测试覆盖读端真实生产路径（真 JSON 文件、未来时间戳、坏数据一律 fail-away）与写端行处理（RawMotion 不写／RawKeyPress 写／单调限流／EOF 结束／多候选并行监听（各自持有并回收子进程、各自重连、按候选增删；发布经共享锁全局限流并原子 rename，任一按键即落盘）），不再只靠环境接缝。

配套（同轮 Sol 复审的另三条）：终端等待会话 union 进 /threads 候选池（真实库 50/51 边界测试）；等待层 tier-1 要求 **presence == Window**（幽灵会话的陈旧 prompt 行、以及本就无窗口的 Background 会话都不得置顶）；状态行措辞中性化"终端有对话框在等你（需要在终端处理）"（不再断言"远程窗口已过"——prompt 可能因让路而从未有过远程窗口）。

## 投递语义（2026-08-17 定）

**at-least-once（可能重复），不是 at-most-once。** 发送与 transport log 落库无法原子（一个跨网络、一个本地），daemon 若崩在两者之间，就进入不可判定窗口：Telegram 已接受但本地无记录，恢复时会重发，用户可能看到同一请求两次。

选它的理由：反过来（先记日志再发）用可见的重复换静默丢失，而这个 outbox 存在的全部意义就是需要用户作答的请求不能凭空消失。重复是显眼且可处理的（一答一行，点第二条会收到已经处理过了）；丢失则和会话卡死长得一模一样。

## 后台 fork 的原生对话框：attach 双向作答（2026-09-02 定，`src/fork_dialog.rs`）

**问题的由来**：后台 fork（`claude --bg` / daemon fork）在一块没人看的隐藏 pty 上渲染 AskUserQuestion。此前 windowless 单选走「扣住工具调用 + 手机填 `updatedInput`」：手机能答，但本地看不见、答不了。用户要「双向、谁先抢答算谁的」，且要用**原生对话框**（自造的横幅/zenity 都被否）。

**承重事实**：`claude attach <id>` 不只只读——用 pty 起 attach 客户端、向其 master 写字节，键真的送达 fork 的对话框。attach 是纯查看客户端，**零模型 token**；fork 本就带上下文在等，答完继续同一回合，与手机答开销相同。**全程不停、不发信号给任何 TUI**——只开新窗口、像人一样敲键，这是它安全、而 SIGSTOP 接管致命（bash 作业控制抢前台 + SIGTTIN 冻死，见 `terminal-takeover-impossible` 记忆）的根本区别。

**如何选中一项（2026-09-03 真机实证，推翻早前的源码猜测）**：真交互 AskUserQuestion 框给选项**编号**（`1. RED`、`2. GREEN`…），**打选项的 1-based 数字即选中**——实测对 index 2 注入数字"3"+回车，fork 答出第 3 项 `BLUE`。方向键也能导航（真框底部提示 `Enter to select · ↑/↓ to navigate · Esc to cancel`），但**打数字是绝对选择**（不依赖别的 attach 客户端把高亮停在哪），正是两面抢答需要的。故手机注入 = **打 `index+1` 数字 + 回车**（`option_digits`）。（早前从二进制 `FLt` 组件读到的 `Select with numbers [1-N]` 是**另一个非交互渲染**、不是真交互框——7 轮静态审都没发现，正是真机验挖出来的。）数字与回车**分两步写、之间再确认活框**：回车是唯一会"提交"的键，只在框仍在时才发；本地此刻抢答、框已消失时，那颗孤零数字未提交地落在主框（低危、不误发），不发回车、判 `Unreachable`。回车后再强制重绘看当前屏：`Injected` 要**正证据＝新帧非空且 chrome 没了**（fork 关框后重绘了下一状态）。`claude attach` 镜像仍在跑的会话，成功后 fork 继续工作、attach 持续产出，故正常成功是**非空帧**；**空帧＝attach 没产出＝它死了（崩溃）**、**框还在＝没吃下**——两者都判 `Unreachable`。空帧的两向歧义按**不对称危害**定案（见⑤）：空帧判 Injected 会记下 fork 可能没收到的答案、**不可恢复**（行已终结、钩子的 `answer IS NULL` 再也修不了、问题被静默丢弃）；判 Unreachable 只是把"真成功但不可见"报成可重试、**可恢复**（钩子仍会在 fork 真答时结算行、/threads 重推）——取可恢复的一侧。

**在场判定（三审 P1-1）**：不刮问题/选项文字（进历史回显判不准）；也不信连接窗口里累计的原始流。而是 `repaint_and_capture`＝**先 `flush_pending` 清掉已排队的旧字节、再强制重绘（`TIOCSWINSZ`→SIGWINCH）、只看这一帧**（清了帧边界，三审 #2）。签名用选择器专属 chrome `Enter to select`（`SELECTOR_CHROME`，真框底部提示 `Enter to select · ↑/↓ to navigate · Esc to cancel`；答后折叠成 `User answered…` 就没了、不进 scrollback、英文固定；2026-09-03 真机实证——早前的 `Select with numbers` 永远匹配不上真框）。

**新流程（仅 windowless 单选、非空选项；多选与 free-text 走扣住+填/comma；审批留 v0.2.10）**：
- **gate 放手**：windowless 单选非空选项分支建行、推手机按钮、`pop_attach_window` 弹 `claude attach` 窗口、`no_opinion` 让原生框渲染、建行后写 `native_attach`。空选项（free-text）不放手，落回扣住+填（Sol P1-4）。
- **本地**：窗口里原生答（打数字或方向键皆可）。
- **手机（先投递后记账，Sol P1-3；只认正证据，三审收敛为两态）**：daemon（`inject_native_attach_answer`）先 `pending_question_status` 查在开/已答/过期（含 deadline），仍开放；再**自己查 label→选项号**（区分"不是选项"＝坏答案不可重试，与投递失败＝可重试，三审 #6），再 `inject_option`：
  - `Injected`（回车前活框在、回车后**非空帧且框消失**，见上）→ 用 `record_native_answer` 记真答案、toast「已作答」。**该记账能覆盖钩子的 `\0elsewhere` 软结算**——若 fork 答完后 PostToolUse 在这空档先把行软结算了，手机的真选项仍写进去，审计/`/threads` 显示真答案而非 sentinel（四审 P2）。
  - `Unreachable`（回车前 chrome 不在／回车后框**还在或空帧**／attach 无帧／pty 错误——**pty 分不清"本地已答/还在连/报错"，一律不认作本地已答**，三审 #1）→ 不记不结算、行留开、toast「没能连上…未记录，可重试」、回 `retryable:true` 让按钮不被消费可再点。
- **本地已答由 PostToolUse 钩子权威结算（三审 #1/#5）**：装 `PostToolUse(AskUserQuestion)` 钩子 `question-answered-gate`，AskUserQuestion 一答完（本地或注入）就触发，把该 session **所有开放 native 行**关掉（`settle_open_native_questions_for_session` 写 `\0elsewhere`——fork 同时只卡一个问题，故其余开放 native 行是漏了答题事件的旧残留，一并关掉，四审 P2）。于是本地先答、手机从没探过也不再被 `/threads` 重推；手机成功注入的真答案由上面的 `record_native_answer` 覆盖之。**唯一未真机验的假设＝PostToolUse 对 attach 答题会不会触发**（建期 API 限速验不了）；若不触发则此钩子惰性，行退回按窗口过期。钩子**只读探优先**：`create_state_db` 初始化本身会写（WAL 切换、迁移），故先用**只读连接** `session_has_open_native_question_readonly` 查有没有 native 行，有才开可写连接走 settle——真正避免常态 no-op 上 SQLite 写锁；只读打开失败即降级 no-op（六审 P3）。install/uninstall/status 对称，status 用 `marker_installed_with_matcher` **校验 matcher 是 `AskUserQuestion`**（四审 P3）。
- **终端会话（非后台 fork）不变**：`native_attach` 假，走 v0.2.10 扣住+填。

**注入进程安全（Sol P2-1）**：`with_attach_pty` fork **前**分配 CString/argv，子进程只 `execv`+`_exit`。`write_all` 处理部分写/EINTR，`waitpid` 处理 EINTR。**程序名走 `CLAUDE_BIN` 权威解析（`resolve_program`/`pop_attach_window`）；无效 `CLAUDE_BIN` 即报错、绝不回退裸 claude**（三审 #7）——注入的 attach 与本地弹的窗口都用 daemon 同一个 claude。

**已知固有极限（pty 注入本质，非可封死的 bug，已如实记档）**：① check→回车之间的微竞态无原子保证，只能收窄（三审 #3）；② 不写整套 ANSI 终端模拟器就没有 100% 可靠的"当前屏"，`flush_pending`+重绘是最佳努力（三审 #2 内核）；③ `Injected` 靠"回车后框消失"作正证据，非应用层 ack（三审 #4），最终由 PostToolUse 钩子兜底核对；④ **数字键上游是否接受＝2026-09-03 真机已验（对 index 2 注入"3"→fork 答出 BLUE）**，不再是未验项；仍未真机验的只剩 PostToolUse 对 attach 答题是否触发、双 surface 并发时序（建期 API 限速）；⑤ **回车后空帧的处置**：`claude attach` 镜像仍在跑的会话，成功后 fork 继续工作、attach 持续产出，故空帧＝attach 没产出＝它死了（崩溃）→判 `Unreachable`（"attach 答完即退"的前提已否决）。即便退一步当歧义看，也按**不对称危害**取可恢复侧＝`Unreachable`：判 Injected 会记下 fork 可能没收到的答案、不可恢复；判 Unreachable 只是可重试、钩子仍会在真答时结算行（四→六审反复围绕此点，六审 P2 证 Injected 侧不可恢复）。

**测试**：`fork_dialog` 8（数字序列、窗口 argv、chrome 判定、present→`Injected`、非选择器屏→`Unreachable`、无帧→`Unreachable`、**答后空帧→`Unreachable`（不可恢复侧的对立）**、**答后框还在→`Unreachable`**）；daemon 6（注入并事后记账、非选择器屏→`Unreachable` 行留开、attach 失败→`Unreachable`+`retryable`、终端会话不注入、**PostToolUse 钩子关全部开放行含旧残留**、**真答案覆盖软结算**）；hooks 幂等含 PostToolUse 组（removed +5）。stub＝**后台连续 printf 模拟活屏重绘 + `head -c` 收键 + 答后画 `after`**（`after` 空＝模拟 attach 死/答后没产出→空帧、`after`=chrome＝框卡住不消失）；死 stub 模拟无帧。全量三跑 423/423 全绿、clippy `-D warnings` 退 0、fmt 净。
