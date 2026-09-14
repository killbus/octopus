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

## Relationship to existing constraints

- `EmptyRetryEnabled` stays default OFF; shadow-period output **changes
  observation only, never behavior**.
- passthrough hold-until-output-evidence (G6) is independent of this
  protocol: its shadow data reuses this file's buckets and window, but its
  graduation criteria are ruled separately (different protocols: envelope
  level vs usage level).
- Existing constraints (force-push to origin, web build, gemini WIP) are
  unchanged.
