# Shadow-period operations protocol — empty-output retry graduation (pre-committed)

> Canonical copy of the pre-committed shadow ops protocol. Source of truth for
> the graduation window, buckets, criteria, and exit rules; mirrored from the
> engineering workspace trellis task `09-14-empty-output-retry-graduation`
> (`research/ops-protocol.md`). The shadow header comment in
> `internal/relay/empty_output.go` points here; when the two diverge, the first
> revision wins and must be written back to the other explicitly.

> Status: **pre-committed (locked before data collection)**. Window, buckets,
> criteria, and exit rules are fixed before any data is seen. Nobody should
> make a graduation decision from memory three months later — this file and
> the shadow header comment in `empty_output.go` are the only basis. When the
> code header comment and this file drift apart, the first revision wins and
> must be written back to the other explicitly.

## Background and observation surface

- Incident (2026-09-14, log.txt): an upstream bridge (LiteLLM) laundered a 429
  into a 200 SSE shell stream (created -> completed, zero output events, zero
  usage, 194K input billed, success=true).
- Observation surface (shipped: `3c32c00e` / `6554eb35` / `c60c0b36` /
  `002962ef`):
  - `relay.empty_stream` (Warnw): `empty_stream_kind` in {empty, error_event,
    truncated, terminal_no_output}, carries channel/group/model/
    stream_end_reason.
  - `relay.empty_stream_shadow` (Infow): shadow discriminator hit, `usage_form`
    in {usage_zero, usage_absent}, carries probe sampling flag.
  - `relay.include_usage_rejected` (Warnw): upstream 400 rejecting the injected
    include_usage.
  - `stream_end_reason`: done/empty/client_gone/first_token_timeout/read_error/
    transform_error/write_error/heartbeat_error.

## Shadow predicate (current code state)

```
zero    form: terminal AND zero-visible AND usage present AND output_tokens == 0  -> failure (shadow flag)
healthy form: usage present AND output_tokens > 0                                 -> exempt (not flagged)
absent  form: usage missing                                                       -> unknown (flagged, no trigger)
```

- The predicate is the pure function `evaluateEmptyStreamFailure`
  (empty_output.go), shared by shadow / R2 gate / future passthrough hold —
  no local rewrites allowed.
- Contract relativity ("missing = breach within a mandatory-usage contract")
  is **not enabled**; flip criteria are in the exit section below.

## Graduation window (first to arrive wins)

> Round-5 write-back record (batch-1 #6, 2026-09-14): the amendments, cost
> column, ruling template, and in-window reconciliation points below are
> round-5 audit residue (P2), persisted **before the window start point**
> (Deming: criteria are operational definitions of measurement and must exist
> before data; exception — the extractor-asymmetry record may be written in
> parallel). Window start point = after the lights-off drill passes, declared
> explicitly by ops.

| # | Window | Meaning |
|---|--------|---------|
| 1 | 60 manually adjudicated true triggers (zero form) | data-volume window |
| 2 | any single channel exposure >= 30k requests | exposure window (prevents low-traffic channels from stalling forever) |
| 3 | six weeks | hard cap (prevents zombie shadow) |

When a window is reached, freeze the data and run the exit ruling. If six
weeks expire with fewer than 60 zero-form samples: rule on the samples already
collected and record "insufficient samples" as part of the decision (do not
defer by default).

## Analysis buckets

- Primary bucket: `channel_id x usage_form x empty_stream_kind`.
- `model` is an **annotation dimension, not a gate dimension** (avoid
  fragmenting sample sizes per model).
- Watch cell: `channel x model` crossover — a new model on a bridge is the
  top candidate for systematic false positives (different bridges may branch
  reasoning/usage passthrough behavior per model).
- Weekly scan: channels with zero primary-bucket rows do not enter the
  graduation sample pool; a sudden increase in crossover-cell hit rate
  (>2x week-over-week) triggers manual review.

## Ground truth (three complementary tracks)

1. **Manual adjudication** (primary): for every zero-form event, check the
   raw payload and rule "did the upstream really owe output?". 60 clean ->
   95% confidence upper bound 5% FP. Adjudications are recorded in the ops
   ledger (PR-3 panel).
2. **client_gone free label**: events with
   `stream_end_reason=client_gone` do not count as zero-form samples
   (truncated != completed-empty) — the discriminator predicate collects only
   on done/empty terminal paths.
3. **Canary triple** (low-frequency blind-spot filler): inject three
   known-answer requests weekly into every `channel x model` cell:
   safety-blocked-empty (should trigger) / normal (should not trigger) /
   reasoning-only (should not trigger). A wrong answer means the predicate
   has a blind spot on that bridge.
4. **Reconciliation** (Hightower): overlap analysis of shadow rows
   (`relay.empty_stream_shadow` usage_zero) against error_event alerts
   (`relay.empty_stream` kind=error_event) gives a free precision estimate —
   high overlap means the predicate is trustworthy on that channel; zero
   overlap means the two observations are looking at different defect
   families, recheck the predicate assumptions.

## Probe and replay (log-only; executors in PR-3 ops panel)

- **Probe** (Kleppmann): for `usage_absent`-form events, deterministically
  sampled (FNV-1a, code variable `shadowProbeSamplePct`, 1-5% during the
  protocol window), issue a **real retry once**, log-only. "Retry produced
  visible content" = true positive -> direct evidence for the confusion
  matrix.
  - Shipped this round: sampling decision + `probe=true` log flag
    (`shadow_probe_test.go`).
  - Executor (PR-3): rebuild the request from RelayLog RequestContent, resend
    once on the same channel, write results to the
    `relay.empty_stream_probe` log line (upstream_status / visible_output /
    latency).
- **Replay** (Feathers, benefit side): for `terminal_no_output` events,
  capture the request and replay once on a **backup channel**, log-only.
  Shadow can only prove "the upstream owed output"; replay is what proves
  "resending helps" — the other half of the graduation argument.
  - Executor (PR-3): `relay.empty_stream_replay` log line (backup_channel /
    outcome_changed bool / new_kind).

## Exit rules (pre-committed)

> Round-5 amendments (three one-line rulings, written back before the window
> start point; Deming: criteria are operational definitions of measurement
> and must exist before data):

1. **Fallback graduation path gets a minimum n**: when window 1 (60 samples)
   is unreachable, the fallback applies ("natural triggers under the 30k
   exposure window fall short of 60, zero FPs on samples already collected"),
   and the fallback path itself requires a minimum n = 30 — below 30 samples,
   the ruling is "insufficient samples" regardless of the FP count, handled
   per the window-3 disposition (recorded as part of the decision, no default
   deferral).
2. **5% rollback line metric made explicit**: rollback rule 1's "FP rate
   >= 5%" is measured as the **observation frequency** (FPs / total
   adjudications in the ops ledger), not a statistical confidence upper
   bound — 5% observed means revert, no interval correction. The "95%
   confidence upper bound 5%" phrasing elsewhere in this file belongs to the
   graduation criterion; the two are different metrics and must not be mixed.
3. **Shadow/gate extractor asymmetry on record**: the shadow discriminator
   and the R2 gate currently extract usage through different paths (shadow:
   `observeEmptyStreamUsage`'s sse.Read scan; gate:
   `observeStreamEvents`/`observeStreamChunk` decoded observation).
   Converging them into a single pure function with a contract parameter is
   an open G4 P2 (Feathers: the seam most likely to rot). Until converged,
   the two extractors may disagree on the same payload — every graduation
   number must cite which extractor produced it.

Graduation of the zero form (discriminator takes over behavior) requires
**all** of:

1. 60 manually adjudicated samples with FP rate < 1% (or, when natural
   triggers under the 30k exposure window fall short of 60, zero FPs on the
   samples already collected);
2. absent form characterized: probe data showing "retry produced visible
   content" > 50% -> the bridge strips usage and the upstream truly owed
   output, contract relativity may flip (missing = breach); < 20% -> absent
   form is mostly legitimate empty turns, keep unknown (no trigger);
3. no systematic `channel x model` false positives (canary and crossover-cell
   evidence).

Rollback rules (any single trigger reverts):

1. FP rate >= 5%;
2. a production incident attributed to shadow-discriminator-driven behavior
   (should be impossible while default OFF; if it appears, revert);
3. upstream protocol change (usage semantics drift).

If the absent form does not enter "flip": the predicate stays zero-only, and
the contract-relativity clause is deleted from the code comment (no ghost
promises).

## Cost dimension (round-5 addition)

The exit criteria previously lacked a cost dimension: retries have real cost
(same-channel resend latency and billing, double input tokens under false
positives), and a graduation argument with only benefit-side evidence
("retry produced visible content") is incomplete.

- The ruling table gains a cost column: **expected total cost per request** =
  one normal-request cost + P(trigger) x expected retry cost. Measure first
  (the shadow ledger records trigger frequency and actual billing of retry
  samples), calibrate thresholds later — the first ruling table records
  measured values only; thresholds are pre-committed at the first in-window
  reconciliation point.
- Cost measurement sources: RelayLog (Attempts / InputTokens / Cost) plus
  the latency fields of probe/replay log lines.

## Ruling template and reconciliation points (round-5 addition)

Ruling template (pre-committed numbers, no in-the-moment discretion):

```
Ruling #N (date: YYYY-MM-DD, week W of the window)
- Cumulative trigger samples: __ (zero form, extractor source annotated)
- Manual adjudication: clean __ / FP __ (observed FP frequency __%)
- Absent characterization (if probe data): visible-content rate __% (n=__)
- Crossover watch cell: channel x model spike __ (none/yes, detail)
- Rollback-line check: observed frequency >= 5%? (yes -> execute rollback rules immediately)
- Cost measurement: P(trigger)=__, expected retry cost=__/request
- Conclusion: keep observing / enter graduation ruling / rollback triggered
```

**Reconciliation happens at every in-window ruling point** (the template IS
the reconciliation action), not once at the six-week end — criteria drift or
predicate blind spots surface inside the window instead of being ratified
after the fact.

**Citation restriction for absent-form rulings**: absent-form graduation
rulings must **not cite shadow rows from G6-ON channels** — the G6 hold
starves the passthrough path's usage-form data (behavior before precondition
1's fix), so those shadow rows are always empty or biased for the absent
form, and citing them pollutes the confusion matrix. Until G6 status is
written into the graduation dashboard as a track-splitting condition,
absent rulings may only cite shadow rows from G6-OFF (or G6-less) channels.

## Relationship to existing constraints

- `EmptyRetryEnabled` stays default OFF; shadow-period output **changes
  observation only, never behavior**.
- passthrough hold-until-output-evidence (G6) is independent of this
  protocol: its shadow data reuses this file's buckets and window, but its
  graduation criteria are ruled separately (different protocols: envelope
  level vs usage level).
- Existing constraints (force-push to origin, web build, gemini WIP) are
  unchanged.

## Appendix: lights-off drill (window zero)

演练目的：验证 G6 开关全生命周期可由**非修复者**操作，演练通过后由运维宣布
窗口起点。此前积累的影子数据一律按垃圾时间排除，不得进入裁决。

> round-5 走查修订（Nottingham/Kleppmann/Deming/Feathers/Hightower 合裁）：
> 第一版手册的操作面按记忆书写（路由写错）且步骤③预期与实现相反，均在
> 走查中裁定后按代码修订。本版操作面逐条对照 router/handler 原文。

**前置锁定（代码级，已入测试）**

1. 开关动态生效：`TestPassthroughHoldSwitchTakesEffectWithoutRestart`
   （不重启翻 ON 拦截、翻 OFF 恢复直通）。
2. 拼错布尔值大声拒绝：`TestSettingBooleanValidation`
   （model 层校验；API 层 400 依赖 `setSetting` → `Validate()` 链）。
3. 在飞流不受中途翻转影响：`TestPassthroughHoldInFlightStreamUnaffectedByMidStreamFlip`
   （建立点每流恰读一次,首读 ON 后中途翻 OFF 不放行）。
4. 关联键等式：`TestShadowLogJoinKeyMatchesRelayLogTime`
   （hold_failure/shadow 行 `start_time_unix` == `metrics.StartTime.Unix()`,
   与 RelayLog `Time` 字段同源;四元组消歧字段在场）。

**操作面（对照 handlers/setting.go 原文）**

- 写入:`POST /api/v1/setting/set`,JSON 体 `{"key":"empty_passthrough_hold_enabled","value":"true"}`,
  需 admin JWT(`middleware.Auth()` 组级挂载)与 `Content-Type: application/json`
  （`RequireJSON()`）。
- 读回:`GET /api/v1/setting/list`(同鉴权),在返回列表中查找该键;终验以数据库
  `settings` 表直查为准(面板读的是驱动开关的同一进程缓存,半循环验证)。

**演练步骤（生产/预发,按序,全部通过才宣布窗口起点）**

1. 翻开关不重启:按操作面把 `empty_passthrough_hold_enabled` 置 `true`,
   预期 200,服务不重启（观测面:进程无重启,API 连续可用）。
2. 拼错值大声拒绝:同一接口把 value 改为 `"1"`,预期 400,报文体**逐字含**
   `setting value must be true or false`（截取报文原文回贴）。
3. hold 行为随开关停止(先正控再负控):
   - 正控(ON 期):注入一条空壳流（如轻量请求经无输出通道）,亲见拦截证据——
     客户端零字节 + `relay.empty_stream_hold_failure` 新增一行;
   - 负控(OFF 期):置 `false`,等在飞流收尾（以最后一条在飞流的 RelayLog
     终态行落库为准,兜底最长流 + 2 分钟）,注入同款壳流,亲见直通——
     payload 原样到达客户端,`relay.empty_stream_hold_failure` 不再新增。
   - 无对照流量的通过视为空洞,判 FAIL（「停止新增」与「本无空流」不可区分,
     NULL≠0）。注:`relay.empty_stream_shadow` 是无条件观测仪,不随开关停止,
     不作本步判据。
4. 日志关联键:取第 3 步正控行的 `start_time_unix`,关联 RelayLog 明细
   （`Time` 字段同值）,并用四元组 `(api_key_id, channel_id, start_time_unix, model)`
   消歧同秒并发;断言命中唯一同流记录。注意落库经异步 flush,立即关联可能
   扑空,等待落库后重查。
5. 翻回:按操作面读回,确认 `empty_passthrough_hold_enabled` 恢复 `false`。

**执行人与记录**：非修复者执行,一名见证复核。每次置值记录时间戳;五步各
PASS/FAIL **附命令与原始输出**回贴 graduation dashboard 窗口零条目。

**回炉路径**：任一步 FAIL → 立即翻回 `false`（全程翻设置不重启,回滚安全）→
失败区间连同此前影子数据一并计入垃圾时间 → 修订规程 → 同规程重跑;两次
重跑仍 FAIL → 升级为设计问题,回到毕业判据修订,不进入窗口。

**遗留（不阻塞演练,阻塞毕业）**：①的端到端开关测试（经 `SettingGetBool`
→ settingCache 真接缝,变量替换测试绕开了该缝）列为 P2 补齐项。
