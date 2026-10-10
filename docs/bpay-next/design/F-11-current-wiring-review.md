# F-11 current wiring review — 10 October 2026

Read-only source investigation for sequence 16. No product edits, execution, SQL, tests, fixtures, network or secret operations. This is an implementation map, not a completed-batch verdict or runtime acceptance receipt.

## Binding scope

Read the complete F-11 card (`01_BUILDER_RULES_AND_FOUNDATION.md:701–721`), all six current checklist rows (`IMPLEMENTATION_CHECKLIST.md:392–409`), the master sequence and existing `design/F-11-runtime-wiring.md`. BD019 in `BUILD_LOG.md` supersedes the former F-25 producer/cutoff protocol. Reuse the current rule-B selection architecture and original revision-change REVIEW/live money checks. Do not change producer, PIN, enroller, 1610/1620/0280 SQL, Worker routes or parked F-25 repeatables. The old design's missing-cutoff dependency paragraph is historical, not an implementation instruction.

F-11 follows F-25 and precedes F-08. F-13 owns safe retirement/reconciliation of old envelopes and the final external quiet barrier; F-26 owns paused execution epoch changes. The historical NX-169 reference to a LEGACY branch does not override the card's OR-45 rule: no alternative legacy engine needs to be maintained or tested.

## Current state and exact module contracts

The ported modules are present and unchanged. `broker/src/index.js` has no background/runtime/job/expiry imports or calls to `handleBpayNextBackgroundMessage`/`runBpayNextRecoveryTick`. Its queue still parses old certificates/wakes before any NEXT dispatch, and its minute task still loads old settings and calls the old Workbench drain.

Import only these existing public APIs:

```js
import { handleBpayNextBackgroundMessage, runBpayNextRecoveryTick } from './bpay-next-background.js';
import { createBpayNextRuntimeRpc } from './bpay-next-runtime-rpc.js';
import { createBpayNextFinancialJobRpc } from './bpay-next-job-rpc.js';
import { createBpayNextPreparationExpiryRpc } from './bpay-next-preparation-expiry-rpc.js';
```

Beside `bpayNextOwnerStatusRpc` at index line 42464, construct dependencies per invocation from `{ baseUrl: env.SUPABASE_URL, headers: sbHeaders(env) }`, yielding `runtimeRpc`, `financialJobRpc`, `preparationExpiryRpc`, `queue: env.BANKING_PAY_CONTINUATION_QUEUE`, and `onFailure`. These adapters return raw transports; the existing parsed callers own request/reply limits and uncertainty. Do not wrap them in generic `sbRpc`, unwrap their raw replies, add discovery, or log headers. Preserve the existing logical configuration names and effective gateway route.

`handleBpayNextBackgroundMessage(message, deps)` requires actual platform `ack` and `retry` methods. Metadata envelopes are exactly `{version:1,runtime_action:'ENROLL'}` and `{version:1,runtime_action:'EXPIRY',run_id}`. Financial envelopes are delegated to the existing financial consumer with exact CLAIM/STEP authority. `queue.sendBatch` is required even if entry wakes use `queue.send`. Internal publication is awaited and uses `{body,contentType:'json'}`.

`runBpayNextRecoveryTick(deps)` runs four sequential, independently caught lanes: one ENROLL, READY page <=100, LEASE_EXPIRED page <=100, EXPIRY page <=100. It returns `{ok,lanes}`; it has no legacy drain counters. The first lane publishes at most two messages, giving a static maximum of 302 publications per tick. This is a source bound, not a throughput/fairness claim. Runtime/expiry RPC calls have an 8-second deadline; queue publication has no extra module-owned deadline. Do not add a second expiry scanner via the unused DUE export.

`onFailure` must return `RETRY` unless an actual durable, idempotent failure/handoff owner has successfully recorded the failure. Logging is not `ACK_RECORDED`. Log only the existing closed, token-free summary; never body, nonce, SQL text or exception messages. `SOURCE_PAY_QUERY_OPEN` and the finite case issues are already recognized by `bpay-next-job-step.js`; preserve their typed refusal/recovery semantics and existing Candidate isolation. Do not manufacture a success or introduce a new receipt owner.

## Necessary bounded index seams

| Seam, current lines | Required change |
| --- | --- |
| Imports, 40–50; dependencies, 42464 | Add the four imports above and a small per-invocation dependency factory. Use `await bpayNextOwnerStatusRpc(env)()` for fresh, already normalized `{active_owner,owner_epoch}`. No global owner cache. |
| `handleBankingPayContinuationQueue`, 42482 | Keep the one-message contract. Before `parseBankingPayWorkbenchCertificateContinuationV8` at 42491 or any old envelope parsing/action, perform the fresh owner read. NEXT: `await handleBpayNextBackgroundMessage(message, deps)` and return. DISABLED, LEGACY, unavailable/malformed owner: explicit safe retry (30 seconds) and return; no old financial work. Do not reinterpret an old envelope as a NEXT job or ACK it as retired here; F-13 supplies its qualified retirement/reconciliation. |
| Invalid batch, 42484–42487 | The current `messages.slice(0,100)` can leave delivered messages with implicit ACK. Explicitly retry every actual delivered message, or throw so the platform retries the batch if a real message interface cannot be safely disposed. Never return successfully with an undisposed delivered message. This does not change configured batch size or introduce multi-message work. |
| `bankingPayWorkbenchTask`, 202958 | Keep the existing minute-only eligibility predicate (1307–1311) first. Then fresh owner check before `getScheduledSettings()` at 202980 or old drain entry. NEXT: await one recovery tick and return its bounded lane summary. Other owners: explicit inactive/paused result with zero work; unavailable owner: bounded failure result. No five-minute NEXT tick, old settings gate, old drain, or continuation burst. Other scheduled tasks remain intact. |
| `bankingPayWorkbenchCronTick`, 16950 | At entry, before legacy reconstruction/RPCs, obtain fresh owner. NEXT: await a single exact ENROLL send and return a small wake receipt compatible with callers, with actual financial counters zero and `made_progress:false`, `more_due:false`. Other owners return inactive/paused without entering the old engine. Do not claim the queued hint performed a financial step. |
| `enqueueBankingPayWorkbenchDrainWake`, 42220 | Obtain fresh owner before constructing the old 13-field wake at 42235. NEXT: await `queue.send({version:1,runtime_action:'ENROLL'})`; return truthful `enqueued:true`, `wake_state:'DURABLY_QUEUED'` and the existing caller-compatible state fields. Do not invent a job/command/lease UUID. Inactive/unavailable owners cause no send. Existing old wake retirement is F-13. |
| `nudgeBankingPayWorkbenchDrain`, 18022 | Preserve its synchronous public signature: existing callers ignore an async return. Register a small, observed promise using the real `ctx.waitUntil`, with fresh owner admission and one awaited NEXT ENROLL send via the async wake helper. Do not enter the old single-flight/coalesced/burst machinery (18891–19528) under NEXT. If no usable execution context exists, return an explicit unscheduled result before starting background work; minute recovery is the fallback. If registration throws, prevent the queued promise from starting unobserved work. Do not silently convert the function into an unawaited async API. |

The existing exported queue at 202738 already awaits the shared handler. Scheduled aggregation at 203240–203252 already owns `bankingPayWorkbenchTask` through `Promise.allSettled` and real `ctx.waitUntil`/await; preserve those lifetime boundaries.

Admission is fresh NEXT status, not an epoch adoption mechanism. Do not append owner epoch to closed module envelopes or rewrite lease/cursor authority. CLAIM/STEP enforce current installed SQL authority, and uncertainty retains the original nonce/epoch/cursor. F-26 separately addresses resumed stale epochs. A Worker owner read is not a cross-service lock and cannot prove the F-13 external quiet barrier.

## Already correct wake and retry behavior

Preparation at index 200316–200339 sends only after the closed handler's successful accepted `SEAL_ENQUEUE` reply with `x-bpay-next-preparation-wake: ENROLL`. Command at 200342–200364 sends after its durable command reply. Each uses the real `ctx.waitUntil`, with await fallback, and awaits `queue.send`; a failed hint leaves the accepted DB receipt intact for minute recovery. These routes need no F-11 edit. Selection/page/history reads do not invoke recovery.

The background module ACKs ENROLL only after enrollment and required sends settle. EXPIRY is ACKed only after definite not-eligible refusal or successful receive plus awaited enrollment wake; uncertainty retries the same run. Financial continuation ACK follows awaited continuation publication. Unknown CLAIM waits for lease expiry; unknown STEP retries the same authority. Terminal ACK is based on a definite installed job result, not a catch/log path. Keep these APIs and provider-free dispatch intact.

`wrangler.toml:250–266` already provides the TEST queue, max batch 1, timeout 1, max retries 10, retry delay 5, concurrency 2, DLQ and the minute/five-minute cron expressions. No configuration change, queue drain, deployment or provider action is part of this source batch.

## Checklist disposition and SQL authority

| Row | Source conclusion |
| --- | --- |
| D01 | Reuse 0560 metadata enrollment/wake/expiry service unchanged. No rule-B SQL integration required. |
| D02 | Implement fresh NEXT admission and one-message module dispatch; safe retry pending F-13 old-envelope retirement. |
| D03 | Implement one minute-only four-lane tick; no epoch rewrite. |
| D04 | Existing HTTP wake ordering already correct; preserve it and add core Workbench wake routing. |
| D05 | Rule B supersedes the old broad accepted-cut repair wording. Preserve its exact selected revision and existing REVIEW behavior; no parked F-25 protocol. |
| D06 | Preserve installed claim/PIN/order owners; no SQL changes. |

Current registry final authorities are `public.bpay_next_enroll_service_v1()`, `public.bpay_next_job_wake_page_v1(text,integer)`, `public.bpay_next_expiry_dispatch_page_v1(integer)` → `04102026_0560_banking_pay_next_queue_recovery_service_v1.sql`; `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` → `04102026_0280_banking_pay_next_financial_job_service_v1.sql`; expiry due/receive → `04102026_0520_banking_pay_next_preparation_expiry_service_v1.sql`. These are existing installed/local source reuse, not a fresh installation assertion. No checklist Built/Reviewed/Installed/App-tested boxes were changed by this investigation. Browser-closed READY, REVIEW completion, 72-hour expiry and throughput remain later authorized acceptance, not source-review claims.

## Fresh saved-source SHA-256 receipts

| File | SHA-256 |
| --- | --- |
| `broker/src/index.js` | `91f6327ed3ba135c1055fd06da94c5f5dec64df9bad8612277e5581ed603af34` |
| `broker/src/bpay-next-background.js` | `77faa6d64335f9c54a5508f09df30fa02763861c9b89b3959b8f43349ad0edf8` |
| `broker/src/bpay-next-runtime-rpc.js` | `9c8c94e9bb1a3a42c126c815d6f896f59de20bf75ed34d776c089c5628e55a91` |
| `broker/src/bpay-next-job-rpc.js` | `d00d086d44baaee414ca8509fbe9c30b91015e55982d84c2035e32a3dfba7fb7` |
| `broker/src/bpay-next-job-step.js` | `a103b8e4d6551ebe879392c3fbe2087ea2b8aeea41b47c8a0e1c0cefabcfa0c5` |
| `broker/src/bpay-next-financial-queue.js` | `de92b5604e8bddd24efedd1dfeab3a09d052613484e457785412a4e9e72b9c19` |
| `broker/src/bpay-next-preparation-expiry-rpc.js` | `190aed494b72c87ac9b3579c3cc752d2620e585c2de24a8bad20c2dd86d3ea9c` |
| `broker/src/bpay-next-legacy-fence.js` | `afdbdda4ac440ada0a6267523a4a60398ae47feec85fcf8772f0fb2a2c6dfef8` |
| `wrangler.toml` | `abe27f2dff0d9c0c07a4af699d2125b8c6bd0c60948e5d2f7a5d18632455903d` |
| prior `design/F-11-runtime-wiring.md` | `ac058b434d4418e5f9893296acd73ecf5c08fa7ff2489511cbc105199a2f6e6d` |
| `IMPLEMENTATION_CHECKLIST.md` | `c762463b79788aa40d6c3e85293e3a9f0768dd7138cb9488b941f5aecf7ca6a5` |
| `BUILD_LOG.md` | `0fc743c35ec92ee3fabe98a63b35113897260e19dc547d1f2d0ea4f60a2e4616` |
| `final-definer-closures.json` | `875f3b481cdd9e617a408ff49ff66556f14714ae7b5d1de421351858d149c28a` |
