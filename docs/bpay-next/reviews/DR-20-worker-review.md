# DR-20 Worker source review

12 October 2026. Independent read-only review of the current saved Worker batch against the complete DR20 card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:521–541`, both DR20 checklist rows, full PF02 card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1693–1716`, BD140/root-settled `design/DR-20.md` and `DR-20-build-investigation.md`. **SOURCE PASS for this Worker slice; no remaining essential finding.** The proposed design was reviewer-authored and independently settled by root before product authoring. This source review does not substitute for the separate SQL review or future joined acceptance.

## Closed enrollment receipt and publication

`broker/src/bpay-next-runtime-rpc.js:223–247` requires the additive jobs array, maximum20 entries, exact three-field entry schemas, unique native UUIDs and closed job-kind/status values. The existing last scalar tuple must equal the final entry exactly, or all three scalars are NULL for an empty array. Multiple entries require PREPARE/READY. Blocked/error receipts cannot contain jobs or claim progress. Genuine zero-job advancement retains its original meaning. Native bigint sequence parsing, duplicate JSON-key rejection, service-only fixed RPC mapping,1024-byte request/32KiB response/8s deadlines and unknown-result recovery remain unchanged.

`bpay-next-background.js:34–49` publishes every READY entry plus the original progressed ENROLL wake and awaits the batch before returning. A lost enrollment reply or uncertain publication is not interpreted as completion; the original native wake/recovery lane remains authoritative. No last-job-only omission remains. The strict transport depends on the matching public enrollment successor being installed before runtime publication; this review did not execute that successor.

## Fresh claims, finite turns and cleanup

`bpay-next-financial-queue.js:20–21,151–205` retains the closed `{id,kind}` wake and derives every STEP only from a fresh native CLAIM. Economic PREPARE alone allows at most16 STEP pages under a10s soft RPC-start budget; each started successful CLAIM still reaches its one original bounded STEP/failure path. The previous nonce is never reused after the service's READY yield. Original continuation validation compares the complete phase-specific cursor tuple; the original job-step adapter preserves100-row page bounds,2048-byte request/128KiB response/8s deadline and native row/byte/time measurement IDs.

After a validated nonterminal page, extra looping requires both claimed and resulting phases in the seven named economic/projection phases and a worker status other than CANCELLED. WORKER_RELEASE phases receive only their original first page; release projection through shared WORKBENCH phase names stops on the CANCELLED receipt. Other financial kinds retain one STEP. Terminal, durable-deferred, uncertain, invalid-continuation and recorded-failure outcomes retain their original dispositions and PF08 evidence. At a finite turn boundary, continuation publication settles before the actual platform message is acknowledged. No active foreign lease is adopted or abandoned to obey the timer.

## Optional accepted-SEAL support

`bpay-next-background.js:56–80` permits at most two native enrollment pages and two distinct jobs concurrently, sharing the original10s start deadline. Every READY identity is already durably published before local attempts. The inline candidates come only from multi-entry PREPARE/READY pages, which the matching public owner reserves for original preparation; one-entry actions, reprepare/cleanup and one-Candidate original preparation use normal queue handling. This is a bounded local list, not a Run/agency inventory. A queued claimant can legitimately win; the inline synthetic message acknowledges no real delivery. Its returned task status is not a financial completion receipt. Original durable wakes and native recovery cover unknown or interrupted attempts.

`index.js:199067–199095` publishes the original ENROLL wake first, then qualifies the already closed handler response's cloned JSON by `ok:true` and action SEAL_ENQUEUE. The header alone does not authorize inline work. `inlineAllowed` becomes true only after the native ctx.waitUntil registration succeeds; its existing await fallback publishes the wake without starting the financial turn. Manual reprepare/release, discard and WAKE retain their original accepted wake behavior. Failure is caught and logged token-free without replacing the accepted HTTP response. The existing dependency factory supplies native service transports, original PREPARE failure recording and measurement callback; no route or Office command schema changes.

## Preserved scope and limits

The saved diff is limited to the four named Worker files. Native money/page owners, private enrollment, original selection/cut, Rule B/Source producers, queue identities, actor/NEXT admission, other recovery lanes and queue configuration remain outside the change. The unchanged job-step adapter and wrangler files have no saved diff. Timing limits are soft starts, not a10s completion guarantee; every in-flight RPC retains its original deadline. Per-task two-job concurrency is not a global HTTP-plus-queue concurrency theorem. No measured latency or300-Timesheet60s PASS is claimed. Owner C3's eventual timing journey is Create Draft only; confirmation/approval is excluded. Joined producer mix, concurrency, browser close and uncertainty journeys remain AUTHORED_NOT_RUN after authorized installation on existing TEST.

## Current raw SHA256

| Saved source | SHA256 |
| --- | --- |
| broker/src/bpay-next-runtime-rpc.js | c4339ae6c3e579eabb30abc748f4e87c337a29ff343520d5db7ad5066c8fc480 |
| broker/src/bpay-next-background.js | 7858985806b52d045ec88300946c47ebc2fa4b1da01c7e23b57e16dbbdde7edd |
| broker/src/bpay-next-financial-queue.js | 4ba5c8ecc3d69c55d6ce0ceef3257f3848be80955d58ed634618e67b5d0b976a |
| broker/src/index.js | 20da273aff3058378153c7e4e541325c196f1e1b96979dbb0b3127f2cd917e45 |

Only source/docs/Git comparisons and hashing were performed. No product edits, test/syntax/build execution, SQL/RPC/business invocation, fixtures, rendered captures, hosted calls or commits were made by this reviewer. Workers review guidance was read locally; the owner's narrower source-only scope controls this review, so it creates no separate runtime/type/config proof programme or external dependency claim.
