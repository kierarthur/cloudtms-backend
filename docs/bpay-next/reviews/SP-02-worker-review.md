# SP-02 Worker caller review

Status: **PASS, including the final post-finalisation retirement catches and corrected actual-start progress projection.** Initial source receipts and the intermediate resolved finding remain below for provenance. Reviewed 10 October 2026 against full SP-02 card/checklist and `design/SP-02-build.md`. No product edit, tests, SQL, runtime, fixtures or hosted operations were performed. Source acceptance does not claim joined/installed/runtime acceptance or the whole SP-02 card complete.

## Findings

No remaining concrete defect found in the reviewed caller delta.

| Required behavior | Evidence in current source | Result |
|---|---|---|
| Recover admission before shared open/task writes | `recoverWeeklySourceFinalisationPayProjection`: actor/request validation through1035; fixed service database admission1039–1053; only then existing pay_open1055. Genuine NEXT refusal ends at1046; it cannot reach original open/recover, calculation, retry rearm or the original later sourceFinalisation construction. | PASS |
| Generic finalisation and actual facts retained | `orchestrateWeeklySourceFinalisation`963–1017 unchanged. Original finalise RPC/receipt stays first, original pay_open and ordinary TSFIN/proposed Source task pipeline continue. Only the separate RECOVER function gained admission; no shared SQL fact owner was replaced. | PASS |
| Database chooses ownership; finite response | Admission calls zeroarg `bpay_next_source_legacy_finance_admit_v1` with `{}`; only actual boolean true (including established scalar RPC unwrap) allows continuation. No active_owner/epoch is sent or locally interpreted. Helper0715 service JWT check and0555 module SHARE owner validation precede LEGACY true / PT410 NEXT / PT503 DISABLED. | PASS |
| Exact received refusal is typed | Recover1043–1050 normalizes exact allowed database code+message pair (or already typed trusted error); all other malformed/unavailable/failed admission paths503. It does not turn an arbitrary55000 or free-text message into retirement. | PASS |
| C1 admission loss proves no requested C1 action | Raw transport128–137 performs the fixed admission before original HTTP146. Loss/timeout/oversize/malformed/nontrue admission is converted to `WeeklySourceC1RawRpcKnownOutcomeError`, code OWNER_STATUS_UNAVAILABLE503, outcomeKnown=true / unknownOutcome=false, with the admission rpcName. The requested external C1 action has not started. | PASS |
| Actual C1 loss still ambiguous | The original requested call retains its original timeout/transport/read/oversize ambiguous branches156–183. No retry loop or fresh auto-replay was added. Admission's known-nonexecution conversion does not cover losses from the actual C1 call. | PASS |
| Genuine NEXT refusal never becomes UNKNOWN | Raw known refusal parsing24–37 preserves known flags22–23; preflight catches/rethrows exact retirement/disabled133. Adapter `isKnownOutcomeError`656–657 and#invokePrepared874–882 rethrow known outcomes without attaching recoveryCall. Publication94–107 calls on_unknown only when recoveryCall exists; durable273–281 only maps C1_OUTCOME_UNKNOWN. Therefore these admission refusals cannot write a durable UNKNOWN checkpoint. | PASS |
| Original raw business bytes preserved | Raw RPC still sends original `parametersJsonText` directly153 and returns original `responseText`188. Only bounded received non-OK database error text is parsed in KnownOutcomeError; successful signed-bigint business JSON is not parsed/stringified. Original request shape/options/size checks remain before admission. Fixed admission has literal `'{}'`, never re-encodes the C1 payload. | PASS |
| Actual outer routes retain typed status/code | Recover error class carries code/status; C1 known error does likewise. Original protected action owner has no catch replacing the typed exception; adapter/publication/durable chain rethrows it. `routes.js:953–976` keeps provided HTTP410/503 and error_code, rather than replacing them with WEEKLY_SOURCE_CONFLICT. Existing routing is unchanged. | PASS |
| No fake receipt or retirement state | No new task state, receipt, command, source completion, C1 success/checkpoint or retired-generation update occurs in the caller delta. All local evidence remains original stage/read/complete-local authority. Admission rejection throws; it never supplies a fake PUBLISHED/RETIRED reply. | PASS |

## Scope and concurrent-owner limit

The admission is a separate database transaction before a separate later HTTP request. Its row SHARE validates/holds owner within the admission transaction only; it is not atomic with the subsequent C1 call. Existing0550 database financial-table fences remain the final enforcement against a concurrently changed owner. This review confirms callers do not remove/bypass those fences, invent an owner token, cache LEGACY or treat admission as a financial receipt. It does not claim absent saved C1 core definitions were reviewed or prove current hosted C1 completion. F18 still owns the genuine unresolved/retired C1 census, including the known previously cited staged request; no operation was retried/cancelled/deleted here.

Raw transport internally uses the same validated no-retry function for the fixed admission, exempting only that exact zeroarg admission name from recursion. The external adapter's fixed18 C1 names still receive admission before each call. No broad Weekly Source route fence was added, so factual import/finalise/authorise/query/read operations keep their original behavior.

Static diff confirms insertion-only caller deltas: Recover18 added lines, raw transport27 added lines; outer Source routes unchanged. Existing ordinary unit/mock adaptation is root-owned; no result from that work is asserted by this review.

## Fresh source hashes

| Physical source | Raw SHA-256 |
|---|---|
| `broker/src/weekly-source/finalisation-pay-orchestrator.mjs` | `d883c8c007121f08c92d023f190641787d6ee97fadc01fffd659496f76f90729` |
| `broker/src/banking-pay/weekly-source-c1-raw-rpc.mjs` | `fb6c9a55414821c6a17effeb1a5f2e0dfe2fa10c8743f72dfb050ed62e51eb8f` |
| `broker/src/weekly-source/routes.js` | `606615afec53a10c6507f86cb0c39605c056d06e2a322a2addcf28645bacfa81` |
| `broker/src/banking-pay/weekly-source-c1-adapter.mjs` | `0af84a1c0e190de3813160cfb05a870488d511446818a89a3518bb65da5abc59` |
| `broker/src/banking-pay/weekly-source-c1-publication.mjs` | `2a9a1f6d06eed3637b1de2f11de3023e87cd3d71ab686a847fc616a0d405be7e` |
| `broker/src/banking-pay/weekly-source-c1-durable-publication.mjs` | `cb39107459dd0e002234db92316ecb8a91a0b80f6874f1e2ee7275ab233d5e18` |
| `broker/src/weekly-source/protected-action-orchestrator.mjs` | `5352ef8bcc66e87b090d5a1d4f40ba872cf7a542d38f60aaf690ec1eea80f3c3` |
| `supabase/repeatable/10102026_0715_banking_pay_next_source_retired_finance_entry_v1.sql` | `67e511e8672cb8d5ae6e91b0022d4c15557861f19bc0e78a7c2a180ea6886e3b` |

## Fresh post-finalisation tail review (supersedes initial tail coverage)

Read full current `rpc`, `processReadyTasks`, `markUnknown`, generic finalise and recovery areas after root's new finite retirement branches. New normalizer accepts only typed retired/disabled codes or exact finite database code/message pairs; all other errors rethrow. Five new catches retain the genuine Source finalisation and invoice receipt and return existing FINALISED_PAY_ACTION_REQUIRED before marking UNKNOWN. Raw C1 comment now truthfully says separate admission and no cross-transaction lease. These aspects PASS. Root reports23 existing caller controls passing; this independent review did not execute them.

**One remaining source finding:** successful `task_start` commits actual SUBMISSION_STARTED task/state/version/attempt_count and run RUNNING (original2336:545–551). Current caller915 accepts `task=started.task` but leaves the local `run` and its tasks at the pre-start snapshot. New apply/finish retirement catches945–946/967–968 return that stale run, potentially showing READY/oldversion after the genuine start was committed. These new known-refusal exits avoid markUnknown (correct) but therefore need to preserve the actual start receipt in their returned progress.

Smallest fix: after validated start with nonnull projection_request (after existing no-projection replay handling), update the bounded in-memory `run` projection with the exact returned `started.task` and the genuine native start's RUNNING state; keep other tasks/counts/identities unchanged. Do not manufacture a DB task transition, UNKNOWN row, terminal receipt, or whole-run currentness; no extra DB read/write is needed. A later successful apply plus refused finish should truthfully retain SUBMISSION_STARTED because finish did not commit, while preserving the genuine Source receipt.

Pending final fresh reread of that bounded correction. Current reviewed finalisation source SHA-256: `d883c8c007121f08c92d023f190641787d6ee97fadc01fffd659496f76f90729`.

## Final fresh closure: PASS

The single intermediate stale-progress finding is **closed**. Current933–937 freezes the actual validated start receipt into `run.tasks` and uses RUNNING after the no-projection replay branch and before apply. `validateStart:627–628` proves status and returned task SUBMISSION_STARTED; original2336:545–551 establishes that this exact successful native start also commits run RUNNING. Other task entries, immutable identities and counts remain unchanged. Thus apply/finish retirement951–952/973–974 now retain actual committed task state/version/attempt_count rather than the pre-start READY projection. No new database update, UNKNOWN row, guessed completion or fabricated financial receipt is involved.

Fresh full scoped diff and surrounding current bodies re-reviewed: the five exact known-retirement/disabled catches preserve genuine Source/invoice finalisation receipts, return existing FINALISED_PAY_ACTION_REQUIRED with finite `pay_projection_error_code` and genuine run/null, and exit before markUnknown. All unrecognized/ambiguous errors retain their original uncertainty path. A refusal before an actual finalisation receipt still propagates as an error; the caller never invents a committed Source receipt. Explicit Recover admission still occurs before open/recover writes. Raw C1 known/error-byte handling remains as initially reviewed; revised comment correctly describes one separate admission and one original C1 attempt, not a cross-transaction lease.

No remaining concrete source finding in this bounded Worker review. No execution, product edit, SQL, tests, fixtures or hosted observation was performed. Joined/runtime receipt remains outside this review.

Final current raw SHA-256:

- `broker/src/weekly-source/finalisation-pay-orchestrator.mjs`: `549e63d14c160eb1ad932089dc4febced99854f843bec7d36edb81f649dcfd0b`.
- `broker/src/banking-pay/weekly-source-c1-raw-rpc.mjs`: `fb6c9a55414821c6a17effeb1a5f2e0dfe2fa10c8743f72dfb050ed62e51eb8f`.
