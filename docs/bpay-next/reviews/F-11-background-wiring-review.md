# F-11 background wiring — independent source review

**PASS for the bounded Worker source batch**, 10 October 2026. Reviewed latest saved `broker/src/index.js` SHA-256 `29cfa90d56aa10b6d86c50c687445a4649c876d3799d64a7287b8a6162d623ec`. No actionable source defect remains in the reviewed hunks. No product changes or tests, SQL, DB, runtime, fixtures, network or secret operations were performed by this reviewer.

Read the full F-11 card, all checklist D01–D06 rows, current master sequence, `design/F-11-runtime-wiring.md`, the fresh `design/F-11-current-wiring-review.md`, BD019 and the existing module APIs. Rule B is binding: expected selected revision, captured Candidate terms/snapshot and existing PIN architecture remain authority. No producer/resolver/2153 accepted-cut protocol, PIN, enroller, 1610/1620/0280 SQL or Worker route change is required or included.

## Full bounded-hunk findings

The current index diff contains 87 added and 1,721 removed lines. Its seven hunk groups are the background imports, old-name cron entry, replacement synchronous nudge, old-name wake entry, per-invocation dependency/wake helpers, shared queue dispatch, and minute Workbench task. The large deletion is the old nudge/continuation engine, not unrelated business code. Product diff scope is index only; BUILD_LOG is documentation. Existing module and wrangler source remain unchanged.

| Group | Reviewed result |
| --- | --- |
| Imports/dependencies | Exact existing runtime, financial-job, preparation-expiry and background APIs are used with `{baseUrl:env.SUPABASE_URL,headers:sbHeaders(env)}` per invocation. The queue binding is passed directly. Fixed adapters and parsed wrappers retain byte/deadline/schema/uncertainty contracts. `onFailure` logs only closed code/recovery and returns RETRY, never a fabricated ACK_RECORDED. |
| Shared consumer, 41036–41064 | Each real delivery must provide ACK/retry methods. A non-singleton batch retries every actual message; a damaged retry interface throws and no undisposed tail is returned as success. Fresh database owner precedes body parsing or adapter construction. NEXT awaits the existing background handler. LEGACY/DISABLED or failed owner lookup explicitly retry 30 seconds. No old certificate, Workbench drain or operation branch remains reachable through this consumer. Exact message nonce/epoch/cursor and bound disposition methods are passed unchanged. |
| Minute task, 201379–201396 | Exact minute-only predicate runs first; five-minute schedule does not start NEXT recovery. Fresh owner admission precedes any old settings/drain work. NEXT awaits one existing four-lane recovery tick; inactive owners return static zero-work status, unavailable authority returns bounded failure. Unrelated scheduled tasks/aggregate lifetime remain intact. |
| Old-name cron/wake entry | Both call `bpayNextEnrollmentWake` before their retained old bodies. That helper either returns enqueued, returns skipped, or throws, so no owner path falls through into old financial reconstruction. NEXT awaits one exact ENROLL send; return fields truthfully state entered:false, financial counters zero, made_progress:false and more_due:false. Other owners are inactive. No job/command UUID or financial result is invented. |
| Synchronous nudge, 18028–18048 | Public function stays synchronous for existing callers. Missing waitUntil returns unscheduled before starting work. The lazy promise checks `registered`; the flag becomes true only after real waitUntil succeeds. A thrown registration leaves owner RPC/send unstarted. Successful registration retains the complete owner/read/send promise; closed catch leaves durable minute recovery available. Scheduled is a registration result, not an enqueue/financial completion assertion. |
| Existing preparation/command wakes | Unchanged: accepted DB response precedes hint send; preparation requires the exact ENROLL header, command acceptance uses its existing durable receipt. Real waitUntil with await fallback retains sends; failed wake does not become false command refusal. Reads do not run recovery. |

The earlier `52ecb6f6…` snapshot started the nudge promise before registration. The final lazy registered-flag implementation resolves that lifetime concern; this verdict applies only to `29cfa90d…`.

## Module and database reuse

`handleBpayNextBackgroundMessage` retains exact ENROLL/EXPIRY envelopes. ENROLL ACK follows settled enrollment/publication; expiry ACK follows definite not-eligible refusal or successful receive plus awaited enrollment publication. Financial CLAIM/STEP use the existing consumer: continuation publication settles before ACK, uncertain CLAIM waits for lease expiry, uncertain STEP retries original authority, and definite terminal/completed results alone support successful ACK. No caller timestamp, lease adoption, provider call, generic RPC or response reinterpretation was added.

Recovery remains one ENROLL plus READY/LEASE_EXPIRED/EXPIRY pages of at most 100 rows each. Independent lanes get a turn despite another lane failing; at most 302 small wakes are published per tick. This is static source accounting, not performance evidence. Existing `SOURCE_PAY_QUERY_OPEN`/Source evidence and case issue parsing is preserved; installed PREPARE returns durable REVIEW worker/issue identities rather than treating them as READY. The new callback retains refused deliveries instead of labeling logs as durable records.

Current final registry keeps 0560 enrollment/wake/expiry discovery, 0280 financial job service and 0520 expiry intake. No source change to these authorities or original claim/PIN/order owners occurred. No epoch is appended to closed metadata envelopes or adopted from Worker owner status. SQL remains the actual authority; F-26 separately handles resume epochs.

| Checklist row | Independent source disposition |
| --- | --- |
| D01 | Existing 0560 metadata service reused unchanged. |
| D02 | NEXT-only, fresh-owner one-message dispatch is wired; malformed/paused/old deliveries retained. F-13 qualified retirement/reconciliation is separate. |
| D03 | One minute-only bounded recovery tick wired, preserving installed epoch authority. |
| D04 | Durable receipt-before-wake and missed-wake recovery preserved; old-name factual helpers now send NEXT metadata. |
| D05 | No additional mutable-binding implementation inferred from historical wording. Built rule-B F-25 choice/capture/snapshot/PIN and original revision-change REVIEW remain unchanged. |
| D06 | Existing claim/PIN/order architecture preserved, with no SQL successor. |

No wrangler/configuration change: existing TEST queue, batch size 1, DLQ and both schedules remain. Retried old/wrong-owner envelopes are not claimed retired or financially reconciled; DLQ state is not completion. F-13/F-18 retained obligations and the external quiet barrier remain separate. Hosted installation, browser-closed READY/REVIEW, 72-hour expiry, F-26 resume and throughput/app acceptance are **NOT VERIFIED by this source review**. This reviewer changed no checklist completion boxes.

## Retained source receipts

| File | SHA-256 |
| --- | --- |
| `broker/src/index.js` | `29cfa90d56aa10b6d86c50c687445a4649c876d3799d64a7287b8a6162d623ec` |
| `broker/src/bpay-next-background.js` | `77faa6d64335f9c54a5508f09df30fa02763861c9b89b3959b8f43349ad0edf8` |
| `broker/src/bpay-next-runtime-rpc.js` | `9c8c94e9bb1a3a42c126c815d6f896f59de20bf75ed34d776c089c5628e55a91` |
| `broker/src/bpay-next-job-rpc.js` | `d00d086d44baaee414ca8509fbe9c30b91015e55982d84c2035e32a3dfba7fb7` |
| `broker/src/bpay-next-job-step.js` | `a103b8e4d6551ebe879392c3fbe2087ea2b8aeea41b47c8a0e1c0cefabcfa0c5` |
| `broker/src/bpay-next-financial-queue.js` | `de92b5604e8bddd24efedd1dfeab3a09d052613484e457785412a4e9e72b9c19` |
| `broker/src/bpay-next-preparation-expiry-rpc.js` | `190aed494b72c87ac9b3579c3cc752d2620e585c2de24a8bad20c2dd86d3ea9c` |
| `broker/src/bpay-next-legacy-fence.js` | `afdbdda4ac440ada0a6267523a4a60398ae47feec85fcf8772f0fb2a2c6dfef8` |
| `wrangler.toml` | `abe27f2dff0d9c0c07a4af699d2125b8c6bd0c60948e5d2f7a5d18632455903d` |
