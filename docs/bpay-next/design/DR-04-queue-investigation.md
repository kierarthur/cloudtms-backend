# DR-04 queue-shape investigation

Read-only source investigation, 11 October 2026. No product, schema, checklist, database, tests, fixtures, hosted resources or commits changed. Full Worker/queue/minute paths remain **NOT_RUN**. This note complements `DR-04-current-investigation.md`; it addresses the actual `{id, kind}` gap only.

## Binding boundary

The full card `PLAN/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:121–153` says database jobs are durable authority and the queue only wakes (127); a ready/due index and minute tick recover loss/duplication (127,135); one job-step RPC applies one bounded page and commits its cursor/effects with original order/epoch locks (129). Line147 requires every NEXT queue body to be exactly **`{id, kind}`** and a static check. No screen, retained database connection, queue configuration change, new financial framework or provider action is assigned here. All current checklist rows D01–D21 were read (`BE/docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:2398–2415`).

F-11 source notes preserve actual fresh owner, bounded dispatch/recovery and original native financial owners. Their historical full-token queue shapes are a source implementation to correct under DR-04, not an exemption from card147. Their parked2153 producer references are superseded by BD019/BD020 in `F-11-SCOPE.md` and current Rule B. Shared queue batch1/concurrency2/bindings/DLQ/crons remain unchanged.

## Actual current owners and gap

| Existing seam | Current source / exact role |
| --- | --- |
| Financial service | `supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql`; `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)`; service-only exact CLAIM/STEP dispatch |
| Claim/PIN PREPARE | `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql`; run → Candidate control → job, sealed command and predecessor proof |
| Financial durable state | migration `03102026_1324_banking_pay_next_core.sql:967–992`; native identity, phase, cursor, position cursor pair, epoch, nonce, lease, availability and attempts |
| Financial wake recovery | `04102026_0560_banking_pay_next_queue_recovery_service_v1.sql:38–85`; indexed bounded READY and LEASE_EXPIRED IDs/kinds, rotates only wake metadata |
| Recovery indexes | migration `04102026_0502_banking_pay_next_queue_recovery.sql:10–15`; READY/BLOCKED and expired-lease partial seeks |
| Ordered enrollment | `11102026_0316_banking_pay_next_snooze_many_enrollment_v1.sql:20–30,123–164`; exact next agency sequence, one sealed member, all members enrolled before clock advances |
| Workbench task runtime | `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql:282–305,308–550`; native fresh CLAIM and exact generation/step/phase/cursor STEP |
| Existing bounded transports | `broker/src/bpay-next-job-step.js`, `bpay-next-job-rpc.js`, `bpay-next-runtime-rpc.js`; fixed service RPC targets, closed typed inputs/results, byte/deadline bounds |

Current `bpay-next-background.js:16,26–31` emits version/action/runtime_action plus lease/cursor envelopes. `bpay-next-financial-queue.js:114–164` sends those full STEP continuations. `index.js:41029,198871,198891,198913` emits the same old ENROLL/CLAIM shapes. All must use the card's two-field body; compressing tokens into `id` would preserve the defect.

The current financial service intentionally does not lock the job before its original owner (`0280:147–149`). CLAIM refuses terminal/busy jobs, calls exactly one native owner, and reveals **only the newly returned claim nonce** (`193–206,293–325`). A lost CLAIM must wait for native expiry. No service reader exposes an arbitrary active token. PREPARE CLAIM already returns phase plus full original integer cursor or Work/component cursor pair (`317–325`), so no new cursor table/reader is needed.

## Smallest safe implementation proposal

Treat `{id,kind}` solely as a native identity notification. Financial `id` is existing job UUID and `kind` is its closed native job_kind; validate the stored kind through the existing service. Workbench `id` is native task UUID and kind identifies that existing lane. Expiry uses existing run UUID and the current receive-expiry owner. ENROLL identifies the native enrollment-clock singleton (`id='1'`, strictly closed for that kind) or an already retained command ID; it must not invent a UUID or encode command inputs. The same existing ordered enroller runs once.

**Prefer two existing calls in one financial delivery: metadata CLAIM, then one job STEP.** The Worker obtains a fresh lease with the unchanged claim adapter, creates one exact STEP from that successful fresh reply, and executes no more than that bounded page. Tokens remain invocation-local and never enter queue JSON. This is one job-step RPC/page, plus the existing metadata claim. Build the same PREPARE phase/cursor shapes already produced by the current continuation helper; keep limits ≤100 and exact string epochs/cursors. Simple financial kinds remain dispatched only to their existing money owners.

A small successor of the **existing financial-job service** is indispensable for prompt ID-only continuation: current nonterminal owners keep status LEASED, and another ID cannot claim that active lease. After the original owner finishes and its complete result is validated (`0280:415–446`), yield **only that STEP's exact still-owned nonterminal nonce/owner_epoch** to READY, clear its nonce/deadline, and keep its committed phase, cursor pair, financial effects and availability. Preserve DONE and original immutable replay; do not reset cursors/epochs/attempts or release some other lease. The original owner already holds its job/control locks at this point. The output/adapter must explicitly represent the yielded lease as null for a nonterminal successful page; it must not falsely report an active lease. No separate unlock RPC, new job, receipt table or generic claim framework is needed.

Unknown CLAIM/STEP is **not ACK and not evidence of completion**. Retry the same identity notification; while its native lease remains active, fresh CLAIM refuses and the queue waits. On expiry, original CLAIM creates a new nonce/epoch and returns the current durable cursor. The already committed page cannot be repeated as fresh work; an uncommitted page rolls back, and a delayed old token is fenced by original epoch/nonce checks. Exact owner DONE replay remains retained when original inputs exist. Update the old adapter comments/continuation assumptions explicitly: its current `job-step.js:11–20` requires persisted CLAIM/exact unknown STEP bytes, which ID-only queue bodies no longer carry. Durable committed job state and native expiration/claim govern lost invocation recovery; never describe this as adoption of an active lease.

**Do not combine CLAIM and STEP under an outer job lock.** Their established parent/control/job ordering differs across owners. The service explicitly prohibits that inversion (`0280:147–149`); split calls preserve original transaction boundaries. A single added WAKE service action would require a separate complete per-kind parent-lock proof and is unnecessary for the card's one-step rule.

Workbench requires no financial lease rewrite: existing task STEP already returns READY/DONE and clears its own nonce/deadline atomically (`1429:533–541`). An ID delivery can fresh-CLAIM then perform one exact STEP from the returned generation/step/phase/cursor. Stale/busy/error outcomes remain bounded retry; its current generation/step fences and failure status are retained. Expiry and enrollment keep their current original owners and one-call metadata semantics.

Publish another `{id,kind}` only after a known nonterminal successful page, await that send before ACK, and let existing indexed recovery rediscover a failed/unknown send. Terminal claims/known terminal pages may ACK under existing authority. Current minute lanes and per-lane caps remain unchanged (`background.js:127–174`), including WB-37 snooze/week lanes. Failed or blocked Candidates do not suppress another lane.

## Named supporting edits and scope guard

Only the actual queue builders/consumer in `bpay-next-background.js` and `bpay-next-financial-queue.js`, fixed transport/type/result handling in `bpay-next-job-rpc.js`/`bpay-next-job-step.js` if needed for the yielded lease, the four NEXT send sites in `index.js`, and the existing0280 service successor are needed for the financial shape correction. Use existing Workbench runtime adapters, not a new RPC endpoint. The existing F-13 classifier (`bpay-next-legacy-retirement.js:40–71`) should recognize the exact two-field NEXT family at its actual shared seam; preserve its unrelated native old-obligation disposition logic. Do not ACK unknown/old nonterminal obligations merely because the new shape is different.

Add the card147 static assertion to the existing source-check path for each actual NEXT send/sendBatch/continuation builder and its closed body keys. It must reject extra fields and hidden encoded envelopes; it is not a new runtime harness, fixture or test programme. Keep existing request/response limits, module/financial admission, accepted Rule B objects, PIN/query binding, case allocation plan and money locks unchanged. `0270` and0316 enrollment/intake should receive no speculative producer amendment: the ordered native proof already exists (`0316:123–164`, `0560:23–34`).

Other DR-04 issues, including explicit terminal Workbench refresh and owner-required REVIEW cleanup, are recorded in `DR-04-current-investigation.md` and belong to their named current owners. This queue note does not widen those branches or declare any path installed/app-tested.
