# F-11 runtime wiring — independent source review

10 October 2026. **PASS for the completed F-11 source scope; no open concrete defect found.** This verdict applies to the final `broker/src/index.js` SHA-256 `29cfa90d56aa10b6d86c50c687445a4649c876d3799d64a7287b8a6162d623ec`, including the corrected lazy nudge registration. It does not apply to the earlier `52ecb6f6…` checkpoint.

Read the full F-11 card, checklist D01–D06, current scope/design, relevant current Worker seams and HEAD diff, reused background/runtime/expiry/financial queue transports, and 0560 SQL service source. Reviewed against BD019/BD020: accepted binding means the saved expected revision/selection Candidate/raw query snapshot and existing PIN v2. The superseded producer/resolver protocol is not a prerequisite or an additional F-11 deliverable. Product scope is only `broker/src/index.js`.

This review performed source reads, hashes and comparisons only. No product or SQL edits, runtime, database calls, tests, fixtures, Docker, hosted inspection, credential access or external effects. No checklist acceptance box was changed.

## Required objects

| Object | Independent source result |
| --- | --- |
| D01 — 0560 metadata recovery services | Reused unchanged. Enrollment is one ordered metadata operation. READY/LEASE_EXPIRED pages and expiry discovery are bounded to 100 and retain SQL NEXT/module authority, server time and durable cursors. Wake publication changes neither financial job state nor lease authority. |
| D02 — continuation queue | Fresh closed owner read before dispatch, actual platform message retained, NEXT-only dispatch to the existing background consumer. Non-NEXT/unavailable authority retries. Batch drift retries every actual message; damaged message interfaces reject the handler rather than successfully returning an undisposed tail. |
| D03 — minute recovery | Exact minute cron predicate precedes admission. One existing recovery tick under NEXT; inactive/unavailable authority has no work. The five-minute schedule cannot duplicate this tick. Existing unrelated scheduled tasks and their aggregate lifetime are preserved. |
| D04 — ENROLL wakes | Existing preparation/command handlers retain their durable accepted response before starting wake publication. Preparation requires the exact `ENROLL` response header. Failed wake cannot become a false command refusal. Old-name cron/wake entry points now await one NEXT ENROLL send or return inactive; neither falls through into retired financial reconstruction. |
| D05 — PREPARE mutable-read binding | Reuses completed rule B: expected work revision, saved Candidate settings, raw scoped query snapshot, and existing PIN v2. Original revision-change REVIEW and live money rules remain the owners. No additional producer/cutoff implementation is inferred from historical text. |
| D06 — existing CLAIM/PIN/order owners | Retained. Queue wiring supplies the existing typed financial adapter and exact job/kind/nonce/epoch/cursor; it does not adopt leases, invent commands, alter preparation locks, or reconstruct financial state. |

## Queue disposition and recovery

`handleBankingPayContinuationQueue` (index lines 41036–41066) preserves the actual message's `ack`/`retry` closures. Fresh owner failure, pause and inactive legacy ownership retry at 30 seconds. The actual NEXT consumer is awaited by the exported queue handler. Its refusal callback logs only closed code/recovery fields and returns `RETRY`; a log is never claimed as a durable `ACK_RECORDED` receipt.

The reused consumer ACKs ENROLL only after successful metadata enrollment and settled continuation publication. EXPIRY ACK requires a definite known not-eligible result or successful RECEIVE plus settled ENROLL publication. Financial ACK requires a definite terminal/completed result; an unfinished step first awaits publication of its exact continuation. An uncertain CLAIM waits for lease-expiry recovery, and an uncertain STEP retries its original authority. Invalid/unknown envelopes are retained for retry. No local timestamp becomes lease or expiry authority.

The four recovery lanes each get a turn despite another lane failing: one ENROLL, READY ≤100, LEASE_EXPIRED ≤100, EXPIRY ≤100. At most 302 small wakes follow from the static code. This is bounded source accounting, not measured throughput or a queue publication deadline. SQL wake rotation and expiry sweep persistence make missed sends discoverable again; neither publication nor DLQ placement means a financial obligation completed.

F-13 still owns qualified old-envelope retirement/reconciliation. F-26 still owns pause/resume epoch validity. Their remaining work does not justify ACKing a paused/old/unknown delivery here. SQL remains the actual owner/epoch authority after Worker admission.

## Lifetime and receipt-first behavior

The synchronous nudge at index lines 18028–18048 retains existing caller expectations. Missing `waitUntil` returns unscheduled before starting work. Its lazy promise invokes `bpayNextEnrollmentWake` only when the synchronous `registered` flag is set after successful `ctx.waitUntil(wake)`. Registration rejection leaves the owner RPC/send unstarted; successful registration retains the complete promise, whose closed catch leaves minute recovery available. The returned `scheduled` flag describes registration, not financial execution or successful publication.

The awaited old-name helpers return actual `enqueued`/`skipped` receipts with financial progress/counters false or zero. `bpayNextEnrollmentWake` returns one of these outcomes or throws, so the retained old bodies cannot execute through a fallthrough branch. No job identity or financial result is synthesized.

Existing HTTP preparation/command wakes preserve the accepted DB response, real `waitUntil` retention and awaited fallback. A missed send leaves that receipt intact. The scheduled handler retains `Promise.allSettled` and real `ctx.waitUntil(allScheduledTasks)`/await fallback (index lines 201606–201618). There is no detached background financial drain introduced by these changes.

Existing closed typed Source/CASE preparation issues and exact lease/retry envelopes are preserved. A durable REVIEW outcome is not converted to READY. 72-hour expiry eligibility remains the existing SQL deadline/receive authority, rather than a Worker clock calculation.

## Evidence and acceptance boundary

| Source | SHA-256 |
| --- | --- |
| `broker/src/index.js` | `29cfa90d56aa10b6d86c50c687445a4649c876d3799d64a7287b8a6162d623ec` |
| `broker/src/bpay-next-background.js` | `77faa6d64335f9c54a5508f09df30fa02763861c9b89b3959b8f43349ad0edf8` |
| `broker/src/bpay-next-runtime-rpc.js` | `9c8c94e9bb1a3a42c126c815d6f896f59de20bf75ed34d776c089c5628e55a91` |
| `broker/src/bpay-next-preparation-expiry-rpc.js` | `190aed494b72c87ac9b3579c3cc752d2620e585c2de24a8bad20c2dd86d3ea9c` |
| `broker/src/bpay-next-job-rpc.js` | `d00d086d44baaee414ca8509fbe9c30b91015e55982d84c2035e32a3dfba7fb7` |
| `broker/src/bpay-next-job-step.js` | `a103b8e4d6551ebe879392c3fbe2087ea2b8aeea41b47c8a0e1c0cefabcfa0c5` |
| `broker/src/bpay-next-financial-queue.js` | `de92b5604e8bddd24efedd1dfeab3a09d052613484e457785412a4e9e72b9c19` |
| `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` | `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa` |
| `wrangler.toml` | `abe27f2dff0d9c0c07a4af699d2125b8c6bd0c60948e5d2f7a5d18632455903d` |

Existing TEST queue binding, batch size one, DLQ and schedules are reused without configuration changes. This source PASS does not establish hosted installation, browser-closed READY/REVIEW, full 72-hour expiry acceptance, resume epochs, external quietness, or throughput. Those runtime/app acceptance observations remain unperformed by this reviewer.
