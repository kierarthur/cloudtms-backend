# WB-06 runtime/native reuse review

**PASS — scoped source reuse for D03–D09, D12–D14 and D17–D23; no confirmed source gap requiring a Worker or accepted-protocol rewrite.** Read-only inspection of current saved files and final registry, compared with the full card/all23 rows in `design/WB-06-capture-retry-investigation.md`. No SQL, DB, business, runtime, fixture, app or test execution. Actual app journeys, queued/minute execution and timing remain **NOT_RUN**. New1346 position projection money/body review is separate; this review verifies its finite 100-row call compatibility, not a duplicate whole D02 certification.

## Required row closure

| Row | Exact current source / conclusion |
|---|---|
| D03 capture | `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` final1236:2984. Immutable captured work/revision, lease/module/Candidate/job/cursor and native A/C/H/source-book/segment evidence remain unchanged from HEAD. Bounded100 typed service calls; missing/changed exact captured position produces actual REVIEW, never omitted money or latest revision guess. Later DR38/SP/CM capture treatment remains its explicit master successor. |
| D04 hold | `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` final1236:3403. Captured positions, exact original equality of all money fields3555–3575, real residual-derived line/hold/position and worker totals3618–3669 stay unchanged. Single existing valuer and actual segment-origin shares, no full saved quote or NET ratio. Later WB45/DR38 named changes remain later. |
| D05 PIN | `private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` finalRule-B0151:181. Actual sealed work_choice.expected_revision_id, Source origin proof, retained query snapshot and query_pin_v2. Post-click current revision mismatch307→SELECTED_WORK_REVISION_CHANGED REVIEW318 is deliberate current owner Rule B and must remain. No parked accepted-input resolver/protocol revival. |
| D06 position claim | `private.bpay_next_claim_position_job_v1(uuid,integer)` final1430:397–451 unchanged. Exact NEXT/module epoch, Candidate control, job kind, READY/expiredLEASED and earlier same-Candidate unfinished command431–435; fresh owner epoch/nonce. Definite rolled-back STEP never automatically persists FAILED. No fake reclaim or lease adoption. |
| D07 enroll service | Public0560:7–36 unchanged. Actual service auth/NEXT and one enroller call25. `progressed = job_id IS NOT NULL OR after_last > before_last`32 correctly handles zero-job metadata progression. No Workbench/history rebuild. |
| D08 wake page | Public0560:38–86 unchanged; READY includes READY/BLOCKED due, LEASE_EXPIRED exact expired lease. <=100 rows with genuine0502 indexed lanes; rotates queue_wake_after only. CLAIM decides predecessor/owner eligibility. No current native FAILED setter exists, so speculative reset/new FAILED protocol is not justified. Other finite snooze/week/producer lanes remain their specific WB36/WB37/WB44 owning scope. |
| D09 financial job service | Public0280 unchanged, fixed CLAIM/STEP typed dispatch to actual current private functions. Known actual lease-busy/predecessor/reclaim-state refusals293–304 preserve original decisions. Its limit1..100 and exact JSON types/keys agree with new1346 private apply default100 and explicit NULL/out-of-range refusal256–257. No service contract/allowlist/API change is needed. |
| D12 background/recovery | `handleBpayNextBackgroundMessage` and `runBpayNextRecoveryTick` active functions in background.js:53/102. Exact ENROLL/EXPIRY metadata and financial calls, awaited continuation before ACK; recovery112–124 ENROLL→READY/LEASE_EXPIRED→EXPIRY with separate lane catches. No source only/dead-module inference. |
| D13 actual dispatch | Actual imports/index factories40997–41013, current NEXT queue branch41065–41090, scheduled current owner path201487–201497. NEXT legacy-named cron16957 and drain-wake40747 first return through the actual ENROLL wake; genuine legacy finance is fenced. Exact queue binding/config remains unchanged. |
| D14 preparation.js | Existing active handlers imported atindex47 and called by preparation route198821/198841 with actual preparation page/command RPC factories. Current accepted response/refusal/progress behavior unchanged; no economics recomputation or QUEUE proof inferred. |
| D17 runtime RPC | `createBpayNextRuntimeRpc` runtime-rpc.js:93 supplies fixed service ENROLL/WAKE_PAGE/EXPIRY/cleanup endpoints, exact bounded args/response schemas and actual AbortSignal transport. Factory is instantiated in actualindex41000. Ready/expired wake inputs match public0560. |
| D18 financial RPC | `createBpayNextFinancialJobRpc` job-rpc.js:5 fixed `/rest/v1/rpc/bpay_next_financial_job_v1`, finite13 kinds, exact action/job/args, signal, no redirects, OPERATION_NUDGE route class. Actualindex41002 instantiates it with current service headers. |
| D19 job step | `stepBpayNextFinancialJob` current job-step.js, actual queue call185. Finite exact schemas for position/PREPARE claims and steps, owned nonce/epoch and cursor, bounded streamed bytes/deadline, unknown-outcome retention. No hidden owner redispatch. |
| D20 financial queue | `handleBpayNextFinancialQueueMessage` current financial-queue.js:169. Exactly one adapter call; terminal CLAIM known receipt and done STEP ACK; continuation128–134 retains original or newly owner-issued nonce/epoch; await send203 beforeACK208. Lost send retains/retries exact call205–206, never declares SQL/financial success. |
| D21 expiry RPC | `createBpayNextPreparationExpiryRpc` current expiry module, actualindex41001 factory and background73 call. Uses bounded named request transport and genuine eligibility refusal, does not reimplement release/financial ownership. Later full release/projection expiry scope remains named owner/WB44. |
| D22 PREPARE claim | Rule-B0151:79–178 unchanged. Exact sealed PREPARE/full enrollment/epoch119–131; any earlier same-Candidate unfinished job133–138 blocks. New worker target143–157 comes from retained selection_candidate channel/Umbrella/VAT, not delayed live Candidate. Original worker/lease authority retained. |
| D23 command.js | Imported actualindex45; actual command route198867 calls `handleBpayNextCommand` with original service transport/actor/request and queue wake. Typed result/known refusal/uncertainty and four-field accepted identities remain unchanged from HEAD; no broad new command producer or caller pricing. |

D01/D10/D11/D15/D16 remain reused context, not extra duplicate edits. Registry actual winners agree with this table: claim/PIN0151, capture/hold1236, claim_position1430, public financial0280, public enroll/wake0560; only apply_position advances to1346.

## Failure, retry and due-index source contract

Current native jobs are durable authority; queue messages are wakes. The declared FAILED enum is not a current first-party automatic failure transition: no inspected current registered final native writer sets `private.bpay_next_job.status='FAILED'`. Public financial wrapper supports only CLAIM/STEP; Worker `onFailure` index41007–41011 logs and returns RETRY, not a database failure receipt or ACK_RECORDED. Do not convert old uncertain/permanent rows to READY or infer a hosted FAILED population from source.

Definite transaction errors roll back the actual step; prior committed lease/job state remains. job-step349–350 classifies40001/40P01/55P03/57014 as known transaction retry. Unknown CLAIM uses WAIT_FOR_LEASE_EXPIRY; unknown STEP uses RETRY_SAME_STEP, with original nonce/epoch/cursors. Native1430/new1346 exact terminal and already-advanced cursor readbacks remain receipt-preserving. Due lane0560 discovers expiredLEASED from physical0502 index `greatest(queue_wake_after_utc,lease_until_utc)`; actual worker creates a fresh CLAIM envelope, never adopts a lost nonce. Finite transport retries/config max10 and DLQ remain independent of database due discovery.

`wrangler.toml:251–261` has actual BANKING_PAY_CONTINUATION_QUEUE binding, max_batch_size1, max_retries10, max_concurrency2 and TEST DLQ. Real index queue path explicitly uses current NEXT owner and one actual message; uncertain failures retry instead of falling into old Workbench drain. One Candidate error cannot create another Candidate's lease or skip its agency order. Recovery lanes catch failures independently. No deployment, current hosted binding, lost-message/failure scenario or measured performance is asserted here.

## Finite compatibility and preservation conclusion

Current public service args reject absent/NULL/non-number/noninteger/out-of-range limit, preserve typed opaque component cursor, and constrain pages100. New1346's default100/NULL refusal is compatible with existing queue pageRows100 and service clamp; no runtime allowlist/shape change is required. Original immutable Source current/protected/query pin semantics, Rule-B REVIEW, exact captured chosen revision and native residual/hold totals remain preserved. This review does not certify future producer/seed, snooze/week, DR38, WB45 or later CM functionality. It does not convert NOT_RUN app/runtime/timing acceptance into PASS.

## Final saved-source hashes

- `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql`: `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf`
- `supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql`: `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d`
- `supabase/repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql`: `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa`
- `supabase/repeatable/10102026_1346_banking_pay_next_position_projection_boundary_v1.sql`: `88e129008b29922790832e361e74bdb9bf4ccb7dc3893d7af0c961f59a0f6c70`
- `supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql`: `6fdd30c7b53b4e2d18d048aca46fbaadc7733a3cd5edefe4773f11b50288334d`
- `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql`: `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa`
- `supabase/migrations/04102026_0502_banking_pay_next_queue_recovery.sql`: `ad151f934eaa1585a849cf965d6b0d9d9672f46dfb5dd9226db8c8cf335167bb`
- `broker/src/bpay-next-background.js`: `bf485315e29a6180337ff04ee38d97fa4e9afb4f35dc4c69294f69ccc151cf14`
- `broker/src/bpay-next-runtime-rpc.js`: `7fd7a0cecb2f27d87f5b78ca7453b1684e9c5ee2b4e956d9e30c1da59262d88b`
- `broker/src/bpay-next-job-rpc.js`: `d00d086d44baaee414ca8509fbe9c30b91015e55982d84c2035e32a3dfba7fb7`
- `broker/src/bpay-next-job-step.js`: `53e2f466929e6640de7be4823aebf4979b81510c3de38931d198897e2de2c810`
- `broker/src/bpay-next-financial-queue.js`: `59bb0fc90f37d3e211f9bcdbf9a17ee3f1ecbfbd6e6d3b94197cf2369ba38e20`
- `broker/src/bpay-next-preparation-expiry-rpc.js`: `190aed494b72c87ac9b3579c3cc752d2620e585c2de24a8bad20c2dd86d3ea9c`
- `broker/src/bpay-next-preparation.js`: `adef1957995903841072a7731bbae8131334330feafb09da835628ba52b01da9`
- `broker/src/bpay-next-command.js`: `95f082f6fef61932b0498ea007d47966692ec15094d954fc9cbbf4bd1d7ed059`
- `broker/src/index.js`: `eb507f1fa44ae63b75f3c86a86dc5217753ad6fea5fd9321dcce55d70fad2c14`
- `wrangler.toml`: `abe27f2dff0d9c0c07a4af699d2125b8c6bd0c60948e5d2f7a5d18632455903d`
- `docs/bpay-next/final-definer-closures.json`: `27e6e603bfc6de290db463652ea341b48b59431e7fe74610491363b31f27b119`
