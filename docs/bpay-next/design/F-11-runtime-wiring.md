# F-11 exact runtime wiring

Read-only source design, 9 October 2026. Re-read F-11, checklist F-11-D01–D06, master file 08, owner start sheet §§2–3, BUILD-BE AGENTS, the ported background/transport/financial consumer modules, current shared Worker entry points and `F-13-retirement-and-external-barrier.md`. This supplements `F-11-background-wiring.md` with the current wiring checkpoint. No product, SQL or test edits; no execution, fixtures, Docker, hosted calls or secrets.

## Minimal bounded Worker hunks

All line references below are the current `broker/src/index.js` checkpoint; re-read saved source immediately before editing.

| Hunk | Exact current seam | Required source behavior |
|---|---|---|
| Imports | Existing NEXT imports near lines 40–50 | Import `createBpayNextRuntimeRpc` from `bpay-next-runtime-rpc.js`, `createBpayNextPreparationExpiryRpc` from `bpay-next-preparation-expiry-rpc.js`, `createBpayNextFinancialJobRpc` from `bpay-next-job-rpc.js`, and `handleBpayNextBackgroundMessage` / `runBpayNextRecoveryTick` from `bpay-next-background.js`. Reuse the already ported files without copying RESET's whole index. |
| Dependency factory | Beside `bpayNextOwnerStatusRpc(env)`, line 42464 | Construct the three fixed transports with `{baseUrl:env.SUPABASE_URL,headers:sbHeaders(env)}` and pass `{runtimeRpc,preparationExpiryRpc,financialJobRpc,queue:env.BANKING_PAY_CONTINUATION_QUEUE,onFailure}`. Construct per invocation; no cached owner or job state. The required `onFailure` callback logs only closed code/recovery fields and returns `RETRY`; it cannot return `ACK_RECORDED` without real durable idempotent failure recording. |
| Shared PAY consumer | `async function handleBankingPayContinuationQueue(batch,env,ctx)`, line 42482 | Preserve one-message admission. Await a fresh `bpayNextOwnerStatusRpc(env)()` **before** old certificate/Workbench/operation parsing or any old call. NEXT awaits `handleBpayNextBackgroundMessage(message,deps)` and returns. DISABLED, LEGACY, malformed/unavailable owner explicitly retry and return safely. No old financial execution or flag-false ACK. F-13 later supplies qualified old-envelope dispositions. |
| Minute recovery | `bankingPayWorkbenchTask` inside `scheduled`, line 202958 | Retain `bankingPayWorkbenchScheduledCronEligible(cronExpr)` first, then fresh owner admission **before** `getScheduledSettings()` and the old drain. NEXT awaits `runBpayNextRecoveryTick(deps)` once; other owners return an explicit static paused/retired result, owner failure an unavailable result. Do not run NEXT recovery again in the five-minute Banking cron block. |
| Existing old-name tick | `async function bankingPayWorkbenchCronTick(env,options={})`, line 16950 | Fresh owner check at entry; NEXT awaits exactly one metadata ENROLL send and returns a small wake receipt. Set progress/continuation counters false/zero so the retained caller cannot treat this as an old burst drain. Other owners refuse/return a static inactive result before old reconstruction. |
| Existing old-name wake | `async function enqueueBankingPayWorkbenchDrainWake(env,options={})`, line 42220 | Fresh owner check; NEXT awaits the same ENROLL send and returns the existing small enqueued/wake-state shape. Other owners refuse/return inactive before constructing the old 13-field wake. Preserve genuine shared caller response expectations; no in-request financial fallback. |
| Existing nudge caller | `function nudgeBankingPayWorkbenchDrain(env,ctx,options={})`, line 18022 | This synchronous caller registers an async task, uses isolate-local single-flight state and can invoke multiple old tick passes. Gating the two async core helpers above prevents its old finance calls. A small NEXT shortcut may replace its old registration, but it must retain the actual send promise through `ctx.waitUntil` or an awaited caller; do not simply change its signature to async while existing callers ignore the returned promise. Old nudge single-flight/coalescing is not NEXT durable authority. |
| Exported hooks | `async queue(batch,env,ctx)`, line 202738; scheduled aggregate near 203241 | Preserve `await handleBankingPayContinuationQueue(...)`. Preserve the existing unrelated task aggregate and real `ctx.waitUntil(allScheduledTasks)`/await fallback. Do not detach queue work or ACK methods. |

The existing malformed-batch branch retries only `messages.slice(0,100)`. If retaining a defensive batch-drift branch, every actual message must receive retry or the handler must reject the whole batch; returning successfully with undisposed messages risks implicit acknowledgement. The configured platform batch size remains one.

## Exact module and RPC contract

`handleBpayNextBackgroundMessage(message,deps)` requires the actual platform message's `ack()` and `retry()` closures, a `runtimeRpc` function and queue `sendBatch`. Its exact metadata envelopes are `{version:1,runtime_action:'ENROLL'}` and `{version:1,runtime_action:'EXPIRY',run_id:<uuid>}`. Other bodies go through the closed financial consumer. Do not synthesize or normalize a message into a different operation.

| Factory / action | Existing fixed public RPC | Scope |
|---|---|---|
| Runtime ENROLL | `bpay_next_enroll_service_v1` | One metadata enrollment; possibly one READY CLAIM wake and another ENROLL wake |
| Runtime WAKE_PAGE | `bpay_next_job_wake_page_v1` | READY or LEASE_EXPIRED page, limit 1–100; no adoption of old lease tokens |
| Runtime EXPIRY_PAGE | `bpay_next_expiry_dispatch_page_v1` | Maintained expiry sweep cursor; page of due run IDs |
| Expiry DUE / RECEIVE | `bpay_next_preparation_expiry_due_v1` / `bpay_next_preparation_expiry_receive_v1` | SQL server clock and SYSTEM actor; RECEIVE of the same run is idempotent |
| Financial CLAIM / STEP | `bpay_next_financial_job_v1` | Existing closed 13-kind financial job adapter; supplied nonce/epoch/cursor stays exact |

The runtime/expiry adapters bound requests to 1024 bytes, responses to 32 KiB, deadline to 8000 ms and pages to 100. The financial adapter bounds requests to 2048 bytes, responses to 128 KiB and deadline to 8000 ms. These are source limits, not measured latency. All factories receive the existing configured service headers and forward AbortSignal; never inspect or print the header values.

`runBpayNextRecoveryTick(deps)` gives four independent bounded lanes a turn: one ENROLL, READY ≤100, LEASE_EXPIRED ≤100, EXPIRY ≤100. Each lane awaits its queue sends; a failing lane does not suppress the other lanes. Maximum static output is 302 small wakes. There is no recursive financial drain, whole-history rebuild, browser lifetime dependency or provider action. Queue send completion is awaited; the code does not establish an overall queue-publish deadline.

## Durable acknowledgement, wakes and expiry

- ENROLL ACK follows successful metadata response and settled continuation publication. EXPIRY ACK follows successful RECEIVE plus settled ENROLL publication. A definite `BPAY_NEXT_EXPIRY_NOT_ELIGIBLE` can ACK only when the adapter says the outcome is known; damaged deadline/state or other refusal retries.
- Financial ACK follows a definite terminal CLAIM or completed STEP, or an explicitly durable recorded failure. An unfinished STEP publishes and awaits its precise continuation before ACK. Lost CLAIM/send waits for the lease-expiry recovery direction (120 seconds); lost STEP/send retries the original nonce/epoch/cursor. No local-clock lease adoption and no guessed completed state.
- Background transport/envelope/send failures retry at 30 seconds. Retrying through a pause may exhaust delivery retries into the existing DLQ; that does not mean the durable job completed. NEXT READY/LEASE_EXPIRED recovery remains the discovery path.
- F-10 already supplies accepted preparation and command ENROLL wakes at current lines 200314/200343. Preserve the durable handler response before attempting send, the exact `x-bpay-next-preparation-wake: ENROLL` condition, and real `ctx.waitUntil(wake)` with awaited fallback. A missed send keeps the accepted receipt and is recovered by the minute lanes.
- `SOURCE_PAY_QUERY_OPEN` and `SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE` remain explicit stored PREPARE issue values. A REVIEW outcome is visible, durable and non-executable; retain per-Candidate command ordering and bounded retry for transient failures. Wiring must not turn a typed blocker into READY, silent success or an unbounded global retry loop.
- 72-hour eligibility stays in the installed SQL/header deadline and expiry RECEIVE contract. Wire discovery/receive/enrollment only; do not invent a Worker clock expiry, revise confirmed Drafts, or claim DR-08's later full expiry successor is complete.

## Infrastructure and dependency boundary

Reuse `wrangler.toml` unchanged: TEST binding `BANKING_PAY_CONTINUATION_QUEUE` / `test-cloudtms-banking-pay-continuation`, consumer batch size 1, timeout 1, max retries 10, retry delay 5, concurrency 2, existing DLQ. Keep TEST minute and five-minute schedules and root crons empty. `bankingPayWorkbenchScheduledCronEligible`, line 1307, selects exact `* * * * *`; do not double the recovery tick every fifth minute. No new queue/configuration/deployment or provider/email/bank effect is part of this wiring. Preserve unrelated Source/invoice/Candidate/MyTMS and scheduled tasks.

Checklist D01 reuses installed `04102026_0560_banking_pay_next_queue_recovery_service_v1.sql`: service authority, NEXT owner FOR SHARE, bounded wake metadata and own server clock. D02–D04 are the bounded Worker wiring above. D06 retains the installed manifest/claim/PIN lock and epoch contract. Master 08 rows 99–103 place **F-25 → F-11 → F-08 → F-13**, with F-11 depending on F-09/F-10/F-12/F-25 and F-13 also on F-18.

F-25's accepted-input/effective-WORK cut is not yet complete. D05 cannot be closed by queue wiring: use `F-25-accepted-cutoff.md` and `F-25-nonqueued-producers.md` for the exact live-input/version-receipt successors. The initial Draft must remain fixed to its accepted Enqueue, including the exact earlier-work resolution; a wake is not that freeze. F-26 separately owns safe epoch resume.

F-13 owns strict old-envelope classification and ACK only after a real qualified terminal/reconciled disposition plus idempotent retirement receipt. Until then preserve/retry old, unknown and cross-owner messages; do not copy RESET's LEGACY operational fallthrough. F-12-D05/F-17/F-18/F-19 own the real external quiet barrier and admitted-result reconciliation; none is implied by a fresh Worker owner check. OR-45/start-sheet overrides require safe inactive legacy admission, not an alternative old financial engine.

The owner start sheet §3 allows SQL loading, db:check, node --check, contract seal/coupling and normal source gates. This investigator ran none. No new harness, mock/runtime scenario, fixture or timing programme follows from F-11. Source/compile success cannot tick browser-closed READY, full expiry, F-25 cutoff, external quietness or hosted activation acceptance.

## Read checkpoint SHA-256

| Source | SHA-256 |
|---|---|
| `broker/src/index.js` | `91f6327ed3ba135c1055fd06da94c5f5dec64df9bad8612277e5581ed603af34` |
| `broker/src/bpay-next-background.js` | `77faa6d64335f9c54a5508f09df30fa02763861c9b89b3959b8f43349ad0edf8` |
| `broker/src/bpay-next-runtime-rpc.js` | `9c8c94e9bb1a3a42c126c815d6f896f59de20bf75ed34d776c089c5628e55a91` |
| `broker/src/bpay-next-preparation-expiry-rpc.js` | `190aed494b72c87ac9b3579c3cc752d2620e585c2de24a8bad20c2dd86d3ea9c` |
| `broker/src/bpay-next-job-rpc.js` | `d00d086d44baaee414ca8509fbe9c30b91015e55982d84c2035e32a3dfba7fb7` |
| `broker/src/bpay-next-financial-queue.js` | `de92b5604e8bddd24efedd1dfeab3a09d052613484e457785412a4e9e72b9c19` |
| `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` | `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa` |
| `wrangler.toml` | `abe27f2dff0d9c0c07a4af699d2125b8c6bd0c60948e5d2f7a5d18632455903d` |
