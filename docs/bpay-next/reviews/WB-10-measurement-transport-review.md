# WB-10 measurement transport review

Verdict: SOURCE PASS for the scoped Worker/FE deadline and measurement additions. One finite byte-count finding was corrected and independently reread. No open source finding remains in this subset. Actual app/RPC/network/DOM latency,2s acceptance, heavy-Candidate and1/17/100-page timing: NOT_RUN by this reviewer.

Read full `02_WORKBENCH.md:422-462`, every WB10 ITEM/D01-D05 checklist row, current BD066 performance scope and timeout/group scope, exact current diffs and complete changed helper areas, actual route/CORS helpers and existing Office durable state/retry code. No product edits, tests, SQL/runtime, fixtures, hosted/config/provider or network actions executed.

## Reviewed contracts

| Clause | Source finding |
| --- | --- |
| Fixed4s Worker read deadline | `broker/src/bpay-next-workbench.js:3,489-495` uses timeoutMs4000. One Promise.race surrounds actual fixed RPC fetch, whole streamed body read, closed response validation and serialization. Timeout aborts the shared controller and emits typed BPAY_NEXT_WORKBENCH_UNAVAILABLE with retryable=true. A late fetch response sees aborted and cancels its body; body reads check abort before/after each read and cancel through their listener. No read timeout creates or retries financial work. |
| FE whole-read deadline | `js/bpay-next-workbench.js:5,491-507` fixes4000ms around authFetch plus complete body and validation. Slow headers and a slow body share one timer; abort cancels the body. Actor equality is checked before dispatch and after body read. Timeout remains a typed unavailable refusal and emits no late successful result to its caller. Existing mount generation/owner/actor adoption guards remain. |
| Factual response-step identity | Worker474-478 serializes the exact response string once. The header carries only closed kind, actual validated row count (zero on refusal), and TextEncoder UTF-8 byte length of that response string. No IDs, filters, search, cursors, labels, amounts, bank data or raw payload appear in the header. Closed normalized kind is set only after trusted Office admin and request validation. Header does not add fields to the strict JSON interface. |
| Actual CORS preservation | Header adds access-control-expose-headers:x-bpay-next-step. Existing index withCORS136406-136411 begins with the response headers, then merges buildCORSHeaders135636-135646. That merge contains no expose-header entry, so the factual header and its exposure survive; existing allowed-origin/credentials policy remains. No index edit was necessary. |
| Local bounded measurements | FE480-507 takes performance.now around each read (existing nonbrowser clock fallback retained). It records exactly kind/rows/bytes/status/elapsedMs and freezes each entry. Fixed kinds only, max100 entries, rounded nonnegative elapsed time; successful row counts follow validation. Refusal/timeout rows are0 rather than guessed from partial JSON. Nothing is sent, logged or cached remotely. |
| Actor and late-completion boundary | Debug append and onReadFailure are guarded by the captured actor still matching currentActorId. The single outer finally appends only once at the Promise outcome; it freezes scalar values. Any later stream cleanup only changes the read-local byte variable and cannot append or mutate the already-frozen debug entry. Existing issue-list adoption guards refuse late/stale results. |
| Separate from durable journal | Office createController424 defines debug independently of state428, snapshot456 and save457. It returns debug beside snapshot at487 and passes the same object into the issue client561. Journal version/keys/migrations/limits, pending request bytes, mutation identities, receipt adoption and exact retry are unchanged. Debug contains no actor or financial payload and is never serialized by save. |
| Preserved source scope | Fixed GET and fixed service-only Workbench RPC, READ classification, auth/environment headers, strict shapes, native sort/cursor checks,32KiB request and128KiB response bounds remain. No new mutation, queue wake, background retry, financial calculator, producer or fixture programme was added. |

## Closed finding

Initial FE snapshot `6a2cf100108f3548e81ebb551b6b6b48ab433ca688bfa490d611f4e6f03817d9` reported bytes only when `readWorkbenchJson` completed its finally. A partial stream whose cancellation cleanup outlived the4s refusal could therefore freeze debug bytes0 although chunks had already arrived. Fresh line445 invokes the running count callback immediately after every byteLength increment, including an oversized chunk before refusal; line447 retains cleanup reporting. The outer frozen observation now contains the bytes received by its outcome. No second late observation is introduced. Correction independently reread at FE SHA below.

## Limits of this review

D01-D03 DB keysets/indexes and5s settings belong to their separate source/SQL review; a4s HTTP abort alone is not proof of the server's effective statement limit. Effective hosted PostgREST setting/cache and all real-data2s goals remain release/acceptance evidence to obtain later. D04's historical300/30/twoDraft result stays owner-reported DB-only with its original receipt unlocated; this review creates no substitute receipt. The measurement hooks prepare for later observations and do not prove those timings.

## Final SHA-256 snapshot

- `broker/src/bpay-next-workbench.js`: `3234c8640877dd5d1dac8ffdddee675f8d9e3e2003f73bc52f191b3150f64c31`
- `broker/src/index.js`: `b9872851195f757a57575b90cfade390d47ffeccc1991cbd702c01dd1dc8742b`
- `js/bpay-next-workbench.js`: `beea3265774d368d8b3332014235eb152cd48573997ca6775d24fc38e18fc14c`
- `js/bpay-next-office.js`: `48c9f4c014fc62f5d1b3c4f633d138f31d43aeb91124c66b41e10054b67505f2`
- `docs/bpay-next/design/WB-10-read-performance-scope.md`: `1d451791988839ab5ff80bdbe5b9966cb5cc2588010fa03c599f731cc48d8998`
- `docs/bpay-next/design/WB-10-timeout-and-group-scope.md`: `66d7ba13e69c2b8390e952612b8c4a582eb2cacc6cf276c2058a143ae1bdf2b1`
