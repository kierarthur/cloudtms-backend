# DY-10 Worker design review — sequence 97

Independent read-only review, 11 October 2026. **DESIGN PASS with the concrete conditions below; implementation and joined acceptance are not certified.** Root alone authors product. No product/checklist edits, SQL/database calls, tests, business fixtures, rendering, hosted calls or commits were performed.

## Authority and exact scope

Full approved card PLAN `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1119–1141`, checklist D01–D05 `IMPLEMENTATION_CHECKLIST.md:2684–2707`, master `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:67,181`, Rule B owner `09_OWNER_ANSWERS_BINDING.md:11–15`, and saved `DY-10-current-investigation.md` were read. The current owner E2 direction in the root task is controlling: the named selected-Work/Source/Case refusal codes use the existing DR05 system-release mechanics, retaining original Rule B inputs and native holds/locks. The investigation's description of historical PIN REVIEW must not be treated as a new owner decision or reverted over E2.

This review covers the current TSFIN Worker/helper failure, duplicate-cleanup and same-attempt checkpoint seams. It recommends no new SQL object, money owner, command, protocol, retry identity, failure store, Daily calculator or producer framework. D03's actual Office visibility and future D05 journeys remain separate required evidence.

## Concrete failure paths

Current `broker/src/index.js:167753–167759` calls `tsfin_work_fail` for a damaged Daily's primary and duplicates, then continues. It omits the existing primary-failure marker. Later `:167806–167813` clears duplicates if the primary is absent from the two failure sets, so those failed Daily duplicates can disappear.

The same defect exists in the Daily no-op catch `:167740–167743`: failed `tsfin_work_success(primary)` leads to `tsfin_work_fail(primary)` without recording that primary. Add the existing marker in both catches, and retain exact primary failure in the Daily context catch `:167121–167126`. No additional outbox identity is needed.

The smallest reliable shared correction is a bounded `completedPrimaryOutboxSet` populated by the existing `markTimesheetComplete(effectiveId)` through `effToPrimary` (`:166955–166995`). Require positive completed-primary membership as well as absence from the existing failure sets before duplicate cleanup. This closes failure paths that never reach the write batch without spreading new failure bookkeeping across every weekly branch.

Preserve existing completion points: genuine missing-context revocation and locked-current cleanup mark complete only after their existing required calls succeed; weekly native self-heal and identical-snapshot success already call the same completion helper. Neither locked nor revoked cleanup means a newly approved zero. A failed no-op, failed native heal, calculation exception or unconfirmed write never contributes completed-primary membership. Sets are confined to the current bounded leased page.

## Closed native write receipt and mixed checkpoints

The helper `rpcTsfinWriteSnapshotsAndComplete` currently coerces missing/malformed fields into zero and defaults errors to an empty array (`:165613–165623`). The Worker then resets malformed errors to an empty failure set and relies on global fail_count=0 for completion (`:167771–167789`). Absence of an error is insufficient success evidence.

Reuse the existing native receipt: `public.tsfin_write_snapshots_and_complete(jsonb)` returns exactly `ok_count,fail_count,errors` (`21072026_1235_38_tsfin_write_snapshots_and_complete.sql:4–5`). Each actual input row produces exactly one native ok or fail outcome (`:153–167,903–955`). Every failed row includes its exact `outbox_id,timesheet_id,error` (`:947–953`); the CONTINUE at504 belongs only to the inner segment loop.

Validate the raw singleton receipt inside the existing helper before adding no-op counts. Require finite nonnegative integer counts, sum equal to the actual written input length, an array of exactly fail_count entries, distinct known written-primary IDs, matching original Timesheet IDs and the existing error tuple. Reject missing, extra or malformed membership/count evidence. Strings may be decoded only through the existing documented scalar/JSON transport representation; do not use Number(null), empty text, booleans or default zero as authority.

Validate helper-local no-op completion separately. `tsfin_work_success_bulk` swallows per-ID failures and returns the number of successful calls (`05012026_tsfin_work_success_bulk.sql:15–22`), so a partial count does not identify which no-op rows completed. Require each no-op chunk's confirmed integer count to equal that chunk's length before declaring its members complete. Preserve the existing normal-write fallback when no-op clearing is unconfirmed; alternatively stop conservatively with no claimed checkpoint. Do not accept a short no-op count or allow an inflated no-op count to conceal a short write receipt.

The combined helper result must conserve its original input count. Error membership belongs only to actual written rows, never helper-skipped no-ops. With that proof, checkpoint every actual nonfailed primary independently, including successful rows from a valid mixed batch. Keep the damaged row's native last_error/backoff; one damaged Daily must not erase the successful checkpoint of another.

An unknown RPC outcome or invalid receipt contributes no new completion or duplicate-success cleanup. It is an unavailable/unconfirmed attempt, not certified zero. Preserve already acknowledged earlier per-item completions and the native leased retry; do not automatically recalculate accepted Draft inputs.

## Bounded native retry and preserved economics

`tsfin_work_fail` stores last_error and capped 1–60 minute backoff (`19012026_extras.sql:233–251`). Specific dequeue accepts at most5000 target IDs, seeks at most50 target Timesheets and leases at most100 due outbox rows with SKIP LOCKED/five-minute lease (`05012026_tsfin_batch_rpcs_part1.sql:34–86`). Preserve those owners. Generic dequeue still uses its existing caller page limit; no new inline drain loop is required for this repair.

The existing targeted drain uses native pending summary, bounded normal defaults60 loops/50 items, pending next-attempt facts, and done=false on a caught exception (`index.js:172437–172705`). Its absent/malformed pending summary must never become authoritative zero. If that wrapper is claimed as DY10's completed checkpoint, strictly validate its native total/ready facts and use finite integer bounds on configured loop values; current positive-only config parsing at172479–172489 is not itself a universal finite cap. This does not justify broad Source-route rewiring or changing established defaults.

Current1001 retains one real Daily aggregate line even at approved zero (`11102026_1001_banking_pay_next_daily_frozen_approval_context_v1.sql:151–155`) and stores certified_zero from its actual approved total (`:306–321`). Current1129 separately enforces genuine current authorised DAILY/HOURS evidence, exact physical/version/client/time/rate authority and hours-only integrity (`11102026_1129_banking_pay_next_daily_hours_only_integrity_v1.sql:5–144`; the investigation's end166 anchor is stale). Preserve both complete bodies.

Canonical Daily status selection remains in `daily-canonical-authority.js:200–224`; null/missing used rates remain RATE_MISSING or UNPROCESSED, not approval. Its numerical intermediate zero is not a certificate. Preserve original WITHDRAWN/removed publication and native paid-excess reconciliation. Unpaid pre-click approved decrease creates no debt; actual realised excess continues through the single existing recovery owner. Accepted click inputs remain frozen; later inputs belong to the next Workbench/Draft under Rule B, with E2's existing system release where required. No money formula or capture owner change is justified by this Worker defect.

D03 must display genuine native failure even when no approved NEXT Work exists. A Work-based JOB_PROGRESS reader cannot manufacture that identity. Root's narrow original Timesheet/Office reader mapping remains necessary; this review does not mark it implemented.

## Saved pre-edit source evidence

Hashes identify the source read for design, not final root implementation:

| File | SHA-256 |
| --- | --- |
| broker/src/index.js | 020ba5667127beb0abfafa8f57b2e78bb2801d62878858e55b608c1452c45639 |
| broker/src/daily-canonical-authority.js | 64cd2109da90f9ee277c2829a734683e7e06649ebebe14b1e2d296ac1c04d00b |
| DY-10-current-investigation.md | 5cca599ef465a6595f365937e659744ccce4803888fdb3a62df312a8c8fc6f4b |
| Current1001 publisher | b5f2f9ec742a11a88ede05ae0eb433725b410907827bf10300b3540824a92a72 |
| Current1129 evidence | 8d9dfa131740391903d8aa68c6ce8a478f9a646b0f431ad43d30987a37b65911 |
| Native write receipt21072026_1235_38 | 175903f7dd7b6f48af6209cf29173a5e05372d05ba8c59d87399833e1d008693 |
| Native no-op bulk05012026 | 509ade551e9ed4113e278de6684f253312605505a752c098f49d14e2d7696704 |
| Native success/fail19012026 | 8c8dadbaaf5e45e377f4cee4586080677627236bda91aea423bac743c30a0c36 |

The index raw bytes have32 CRLFs and201765 bare LFs, no bare CR. The named165339–167827 TSFIN range has only LF terminators. Root should use exact narrow byte-preserving replacement; normalising the whole file would create unrelated churn.

**Verdict:** the proposed positive-primary completion and strict original receipt validation meet the narrow D02 design when all conditions above hold. No compulsory SQL successor is demonstrated. D01 reuse, D03 source completion and all D05 joined/rehearsal checks must be recorded separately; no runtime acceptance is asserted.

