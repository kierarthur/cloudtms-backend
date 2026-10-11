# DR-10 official-week financial-facts investigation

11 October 2026. Read-only source/schema investigation for sequence99. **Final finding: classify the actual original PAYE membership and matching financial receipts; root/Worker/transfer status alone cannot prove execution.** No business data, SQL/function invocation, database/hosted request, test/fixture, secret, product edit or commit was used. Root alone authors product.

Read DR-10 full card/checklist context, BD111, F18 query map/provenance, current final-definer registry and saved current-contract relations/indices. This note does not assert observed cutover counts or installed/runtime acceptance.

## Official week and native membership

Use the accepted official `bpay_next_pay_run.pay_date`: Monday `date_trunc('week', pay_date::timestamp)::date` through +6. Original server official-date helper owns that date. Do not use Work week-ending, snooze date, transfer-created date or CSV-issued date. The current run is excluded by its exact original ID; no broader actor-only exemption.

NEXT frozen PAYE identity is `run_worker.target_pay_channel='PAYE'`, joined by exact `run_id` and Candidate. Before Worker admission, the accepted cut already stores `selection_candidate.target_pay_channel` (0426:608–617), so PREPARING/REVIEW cannot disappear from the guard merely because no Worker exists yet. Those are accepted preparations, not a falsely labelled confirmed Draft. Preserve the native race/serialization seam described in BD111.

A confirmed channel Draft is the root's `confirmed_at_utc IS NOT NULL` plus its actual PAYE membership. Channel-reader0236:17–34 intentionally retains cancelled/zero workers in historical membership; its existence alone is not a live unexecuted blocker. Exclude an actually released/cancelled member only when its original native disposition is coherent; contradictory issued/paid/active-hold facts remain integrity/pending, not silence. Never filter by gross/cash >0: an accepted genuine zero payroll has its own internal completion receipt.

Inspect original transfer membership only: `transfer.run_worker_id=worker.id`, `candidate_id=worker.candidate_id`, and `original_transfer_id IS NULL`. CASH_REISSUE/returned-cash successor transfers do not create a second PAYE payroll or a new official week. For split destinations, use exact `destination_group_leg(anchor_transfer_id,transfer_id)` and `destination_group_member`/`transfer_member` FKs; don't judge just the anchor's state. Do not infer payroll membership from beneficiary_kind alone or today's Candidate pay method.

## Exact NEXT financial evidence

Current authoritative source is C15 `original_reservation_v1` in `10102026_0639…`:333–377, supported by final CSV issue0316 and settlement2018.

| Native fact | Exact evidence | Classification/consequence |
| --- | --- | --- |
| Confirmed PAYE member with no accepted projection/transfer or no execution receipt | Actual confirmed original PAYE Worker; absent matching execution receipt, including a Draft still waiting for NET input | Unexecuted PAYE Draft; hard block. |
| Original reserved/building/member-ready/scheduled/refused transfer | No exact settlement/return/internal receipt; native active reservation/member basis where required | Unexecuted/pending. REFUSED is not executed merely because its enum is non-Draft. |
| Issued CSV | Exact immutable `csv_instruction.transfer_id=t.id`, `run_worker_id=w.id`, matching cash/bank snapshot, and `t.status='ISSUED_CSV'` | Real issued instruction, **Not paid yet**; native C15 says ISSUED_AWAITING_RECEIPT and retains reservation. Issuance is not settlement or permission to infer paid. |
| SUBMITTED / UNKNOWN | No matching terminal native receipt | EXECUTION_UNCERTAIN; hard-block/unavailable classification, not a paid override. |
| Actual bank execution | `transfer_outcome o` joined to `outcome_request q` by `q.outcome_id=o.id AND q.transfer_id=o.transfer_id`; `o.transfer_id=t.id`, `o.outcome_kind='SETTLED'`, `o.whole_transfer_amount=t.cash_amount`, exact q Worker/Candidate, and `t.status='SETTLED'` | Actual executed receipt. q.posting_complete distinguishes POSTED from EXECUTED_PENDING_POSTING; preserve pending owner work rather than claiming a fully closed Draft. |
| Returned original | The exact prior SETTLED receipt above plus exact RETURNED outcome/return_request binding and `t.status='RETURNED'` | Genuine executed-and-returned original; not new payroll. Reissue amount/status cannot replace this original proof. |
| Genuine zero cash | Exact `internal_receipt` joined through `outcome_request.internal_receipt_id`, same original transfer/Worker/Candidate/projection and cash=0; INTERNAL_ZERO transfer with INTERNAL_PROCESSING/INTERNAL_SETTLED | Native INTERNAL_PENDING_POSTING or CLOSED_INTERNAL. Only completed native posting is final; never substitute absence of a bank transfer for execution. |
| Split original destinations | All exact original legs and their receipt/posting facts, not only worker.status or one paid leg | Any remaining unexecuted/uncertain/pending original PAYE member/leg keeps the hard block. Actual paid history may be named separately. |

Final issue0316:320–331 inserts the immutable CSV instruction and sets only transfer.status=ISSUED_CSV; its fresh receipt returns `payment_recorded=false`. Parent run remains DRAFT and Worker can remain READY. Therefore `run.status='DRAFT'` is not sufficient to say none was issued/executed, while Worker READY is not sufficient to say none was issued.

Final2018:2278–2285 writes the exact SETTLED outcome/request and transfer status before member posting. C15:342–353 deliberately calls unposted receipt EXECUTED_PENDING_POSTING. C15:99–159 binds completed destination legs to original outcome/request/whole cash and sets Worker COMPLETE only after all expected original legs post. Internal0490:202–241 completes the zero owner and its original weekly contribution without inventing bank payment. Reuse these existing receipt facts; do not create a new payment-state authority.

**BD111 correction reported immediately and accepted by root:** the broad list “issued/submitted/unknown/settled/returned/refused/internal-final” cannot itself be the override predicate. Status-only is unavailable/hard-block; actual issued can be named Not paid yet. A same-week executed override requires genuine matching native receipts and no remaining unexecuted/pending original PAYE obligation. No early financial action, reader, money calculation, history engine or additional reason column is introduced by this finding.

## Conditional retained legacy facts

F18 `09102026_2109…` is the current final owner. `readiness_unused_batch_v1`:110–134 provides a positive unused-shell proof. Exclude a batch only when that result is **TRUE**; failed/unknown/inconsistent proof is not “unused”.

Its exact unused condition requires DRAFT/DRAFT_CREATED, NOT_SUBMITTED, no CSV export/schedule/executing/commit/Monzo timestamp, no AUTHORISED auth request, RESERVED/COMMITTED reservation, settled/PARTIAL/FAILED Candidate or non-null NET, captured PAYE NET input, non-pristine transfer, provider scope/attempt/event, pending/failed settlement, live correction or retained typed advice. A genuine captured zero NET is retained because `IS NOT NULL`/input existence is used, not >0.

Do not equate `NOT unused` with paid. F18:142–164 separates GENUINE_OBLIGATION, OBSOLETE_BOOKKEEPING and INTEGRITY_REVIEW. PAYE-input branch217–231 separates actual input from unsupported awaiting/net/status assertions. Read actual PAYE ownership: native batch_kind_fixed PAYE or exact Candidate/item/transfer PAYE facts; exclude explicit LOANS, avoid live Candidate pay-method inference and voided/superseded item fiction. Every conditional legacy row needs its own retained native identity/disposition, not every old preview shell.

| Retained legacy class | Actual source facts | Guard use |
| --- | --- | --- |
| Genuine unexecuted/admitted owner | Real captured PAYE NET input (including zero), AUTHORISED intent, active reservation or exact genuine unresolved Candidate/payroll owner; no committed execution proof | Existing unexecuted PAYE obligation hard-blocks; merely scheduling or authorising is not paid. |
| Actual issued/committed instruction | Native bank_csv_export_json/execution_commit_state/execution_committed_at_utc and exact original PAYE membership, reconciled with transfer/provider/settlement state | Name only the actual issued/admitted truth; unresolved execution is not paid. No preview_refresh/draft_create event alone establishes it. |
| Genuine paid member | Native settled Candidate/item/transfer disposition bound to exact batch/PAYE member; provider transfer COMPLETED plus completed_at and trusted matched COMPLETED event where provider route supplies that witness | True financial history in the official week; preserve partial membership and no-money/returned distinctions. |
| Unknown provider admission | Original scope REQUEST_SENDING/REQUEST_SENT_LOCAL/PROVIDER_ACCEPTED/PROVIDER_UNKNOWN or exact sending/sent/transaction evidence, joined to its original transfer/batch/attempt | Real unresolved external obligation; never silently downgrade to unused or call it paid. |
| No-money/returned terminal history | F18 exact matched/signature-qualified terminal event and scope/request/transaction identity, no later uncertain event | Terminal helper TRUE includes no-money failures as well as completion; split on outcome/movement, so “terminal” alone is not “paid”. |

Legacy same-week date is `coalesce(authoritative_payment_date,pay_date)` in the same accepted Monday–Sunday interval (native legacy guard95330–95331). The old helper's Draft branch95317–95320 is globally dated; do not copy that broad branch into DR10. A genuinely retained obligation lacking a usable official date remains an integrity/reconciliation fact; do not assign this week's date from created_at.

F18 scope/event helpers:34–107 bind exact transfer/batch, MATCHED, provider webhook signature, original request/transaction IDs and no later uncertain event. Their terminal set also includes PRE_BANK_CANCEL/NO_MONEY_UNWIND/PROVIDER_CANCELLED_NO_MONEY/PROVIDER_FAILED_NO_MONEY. Thus helper terminal=true cannot be used as “payment executed” without the actual COMPLETED/RETURNED/REVERTED branch.

`F-18-readiness-provenance.json` records hostedCounts=NOT_OBSERVED, externalQuietWindow=UNPROVEN, activationReady=false. Actual cutover census/disposition remains required; no clean empty result, legacy adoption or fixture is asserted here.

## Stored human references and bounded source access

- NEXT pay_run/run_worker have **no stored human Draft/run reference** in current saved relations. Use a truthful generic PAYE Draft label plus actual status/context; do not display a shortened UUID or invent PB-0412-P.
- Issued NEXT `csv_instruction.payment_reference` is a real stored bank-facing code. Final0316:303–305 produces BP plus the first18 native transfer-UUID hex characters and persists it; `file_name` is an actual file name, not a Draft reference. Only display the stored reference for that exact issued instruction. It can be named as CSV/reference while Not paid yet; never imply paid.
- Retained legacy `pay_batches.bulk_reference` is the existing stored batch reference (unique when present). `pay_bank_transfers.payment_reference`, `banking_pay_operation_transfer_scope.payment_reference` and `pay_batch_items.bank_reference` are separate actual instruction references; never substitute a provider request UUID or null batch reference with an invented human label.
- Saved current-contract supports root pay_date/created_at/id seek, run/channel/Candidate Worker index, Worker transfer-no/id, exact instruction transfer unique index, exact outcome_request transfer unique index and destination leg anchor/transfer index. Legacy has separate authoritative_payment_date and pay_date indices; a conditional official-date branch can preserve those range seeks rather than scan all historical batches.
- Bounded output must preserve “more/unresolved” truth. Do not let LIMIT on displayed references truncate the EXISTS blocker predicate or allow Verify. No additional index or maintained summary is proved necessary by this source note.

## Source provenance

Current BE worktree: C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009.

| Saved source | SHA-256 |
| --- | --- |
| docs/bpay-next/final-definer-closures.json | eaf5fcb66dbd61f382f9c7596b31d3a9fdf629482bdbdf884b8c67a70dcc9375 |
| supabase/release/current-contract.json | 2d018d0cf3f84428f1b739eeaadaa34a991c9ad12b1307ab92cbdc635736d63a |
| core1324 migration | f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c |
| destination1827 migration | af9849f6e83784b0064ee58179aabc740d396764c3639ef7132967f394c9512d |
| readiness2109 final | 980a79151e8cc49f3db8c5c4e1f8cd99aac6039600405e25ec1803d3b7c91add |
| channel Draft0236 final | a90b3b2fe01e079e7177bedde583d5e0b66dd037b2344272a6a8fb288d008719 |
| CSV issue0316 final | 98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6 |
| C15 reservation/destination0639 final | 15c88f4c45051087976ed9187a741da494c69a052611c2a41892417f9665a22d |
| internal zero0490 | 88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c |
| settlement2018 final | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |
| accepted Create0426 final | aec4c904cee22e748ea1ab3006e8cfdbf50bcdb9dd567b1fb1b2bc9da7391b92 |
| original monolith official-date/legacy guard guide | 8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f |
| BD111 reviewed before root classification amendment | 02cce93b5c43d1357257c83af57826844e931a5dd66b34d099d8aa7a8a4dd051 |

All conclusions are source design. Joined same-week/concurrency/reauth/payment/cutover acceptance remains NOT_RUN.
