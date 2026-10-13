# PS-02 current transition investigation

Read-only source investigation, 10 October 2026. Full PS-02 card 06:1353–1375, current D01/D02/D04/D05 and shared D03 evidence reread; dependency F-05. No SQL, business function, test, fixture, database, hosted call or runtime invoked. No product change. Source presence, historical installed receipts and future actual acceptance remain separate.

**Current settlement/return fact writing and internal receipt ownership can be reused. A separate calendar-date column is absent. CANCELLED/SCHEDULED/SUBMITTED/UNKNOWN/REFUSED fact delivery must not be manufactured by PS-02.**

## Actual final owners

| Exact routine | Current final defining root |
| --- | --- |
| `private.bpay_next_receive_simple_csv_settlement_v1(uuid,uuid,text,numeric,timestamp with time zone,uuid)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:2191` |
| `private.bpay_next_claim_simple_outcome_job_v1(uuid,integer)` | `04102026_0200_banking_pay_next_simple_csv_settlement_v1.sql:99` |
| `private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:2298` |
| `private.bpay_next_receive_simple_csv_return_v1(uuid,uuid,text,numeric,timestamp with time zone,uuid)` / `private.bpay_next_post_simple_return_v1(uuid,uuid,bigint)` | `04102026_0230_banking_pay_next_simple_csv_return_v1.sql:5/83` |
| `private.bpay_next_receive_internal_settlement_v1(uuid,uuid,uuid)` / `private.bpay_next_complete_internal_zero_v1(uuid,bigint)` / `private.bpay_next_internal_receipt_guard_v1()` | `04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql` |
| `private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid)` | `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql` |
| `private.bpay_next_build_simple_reissue_v1(uuid,uuid,bigint)` | `04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql` |
| `private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)` | `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql` |
| `private.bpay_next_cancel_simple_worker_page_v1(uuid,uuid,bigint,bigint,integer)` | `04102026_0260_banking_pay_next_simple_cancel_v1.sql` |
| `private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer)` | `04102026_0460_banking_pay_next_case_cancel_v1.sql` |
| `private.bpay_next_apply_reissue_cancel_v1(uuid,uuid,bigint)` | `04102026_0355_banking_pay_next_reissue_cancel_v1.sql` |
| `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:3044` |

Do not copy the historical 0200 receive/post bodies over their F05 final successors. CLAIM0200 is already reusable unchanged. The existing private financial owners and public intake have fixed permissions; no generic RPC or new Worker/config/screen is required merely to add stored date metadata.

## Current fact and clock evidence

| Existing producer | Durable evidence and clock semantics | Replay / transaction conclusion |
| --- | --- | --- |
| CSV settlement, including supported ordinary Umbrella and reissued cash | Core `private.bpay_next_transfer_outcome` has native UUID, unique receipt_id, exact transfer/amount/kind, supplied `occurred_at_utc`, and server-default `received_at_utc = transaction_timestamp()`. Final receive2018 inserts SETTLED outcome and request, then changes transfer SETTLED in the same transaction. | Exact receipt replay compares transfer, SETTLED kind, whole amount and occurrence instant; it returns the original request/command, with no new fact. Wrong receipt tuple or competing request refuses. The 0200 `bpay_next_outcome_immutable_v1` UPDATE/DELETE trigger invokes final1342 `bpay_next_effect_immutable_v1`. |
| CSV return | Receiver0230 inserts RETURNED outcome/request and changes transfer RETURNED in the same transaction. It requires the real SETTLED predecessor and predecessor occurrence not later than return occurrence. The same outcome clocks and immutable trigger apply. | Exact receipt replay returns the original request; return-cash posting is separate and ordered. `post_simple_return_v1` creates the original obligation once or reopens the same reissued-cash obligation. DONE/posting_complete replay does not duplicate cash or a fact. |
| Internal zero settlement | Migration0480 `private.bpay_next_internal_receipt` has unique command/transfer, actual typed FK tuple, server occurrence and ingestion timestamps, both finite and received≥occurred. Guard0490 requires INSERT only and both exact timestamps equal the current transaction timestamp; UPDATE/DELETE refuse. Receiver inserts this receipt and request then sets INTERNAL_PROCESSING in the same transaction. | Command replay checks exact transfer/worker/Candidate/actor and returns the original immutable receipt. Bounded final member posting calls `complete_internal_zero_v1` before DONE; that helper validates the original receipt/complete members and sets INTERNAL_SETTLED. It creates no fictitious bank outcome, CSV or return balance. Initial receipt time must not be relabelled as later completion time. |
| Cash reissue construction | Final receive0316 accepts the exact existing return-cash obligation; build0240 creates one transfer with original_transfer_id/return_cash_id, one CASH_REISSUE member and the exact cash hold. Transfer has server `created_at_utc`; request receives its transfer_id. | DONE replay returns the same transfer. Building/holding a reissue is not another payroll posting or a bank payment fact. Its eventual settlement and return use the existing outcome receivers above. Do not invent a new SETTLED fact at construction. |
| CSV issue | Final0316 retains one immutable `bpay_next_csv_instruction` (unique transfer, exact bytes/SHA/reference), with server `issued_at_utc`, in the transaction that changes ISSUED_CSV. 0145 protects the instruction from UPDATE/DELETE. | Exact instruction replay reuses that instruction. Current source does **not** insert a CSV_ISSUED transfer_outcome; report the actual immutable instruction evidence rather than claiming such a producer exists. Later Execution owners own their required outcome facts. |
| Simple / Case / reissue cancellation | 0260/0460/0355 retain typed requests/bindings and real state transitions. Simple cancellation changes the transfer at the initial admitted release stage; request.finished_at may be recorded several pages later. Case cancellation similarly separates CHECK/RELEASE/completion. Reissue cancellation changes only the admitted real transfer and return-cash hold. | DONE terminal replay returns the original disposition. BLOCKED closes the request/job without cancelling cash; it is not a payment transition. None of these current final bodies inserts CANCELLED into transfer_outcome. Request finished_at/completed_at is not a substitute for the actual transfer event time. EX-27 owns the missing append-only CANCELLED fact producer. |

`financial_effect.occurred_at_utc` defaults to the financial posting transaction for simple WORK posting; Case posting can retain the original outcome/internal occurrence in its own event. Neither posting timestamp nor hold/request/job completion timestamp may become a bank date. Posting and external fact receipt are deliberately separate ordered transactions; PS-02 must preserve that distinction.

## The exact missing date boundary

No `business_date` or `bank_date` column exists in the current NEXT schema sources. Existing outcome occurrence is a timestamptz, not a native calendar-date column. Core outcome clocks are NOT NULL; only received has a server default. The current public intake and Worker exact allowlist for CSV_SETTLEMENT / CSV_RETURN carry `occurred_at` as an explicit-offset ISO timestamp and carry **no separate date argument**. The intake casts that timestamp once and passes the six-argument private receiver. Its amount, receipt and occurrence checks/replay must remain intact.

Execution card 04:488/494 explicitly defines SETTLED occurred time as the payment date and future file settlement as receipt occurrence derived from its admitted payment_date. That proves which existing operand is business occurrence; it does not authorise deriving a bank date from ingestion, run.pay_date or economic-effect time. A stored calendar projection of that occurrence needs an explicit stable calendar convention; raw `occurred_at_utc::date` would depend on session TimeZone. For a British business-calendar convention, the finite expression is `(occurred_at_utc AT TIME ZONE 'Europe/London')::date`. This is an explicitly derived calendar day of the admitted occurrence, **not proof of an independently supplied date** if a later owner admits a separate payment_date.

Minimum bounded supporting schema is a finite native `date` on the actual outcome fact, and the equivalent internal-receipt business day if the completion note calls that a business-date fact. Keep historical absence NULL rather than synthesising a bank date from unrelated clocks. New supported inserts should retain their business occurrence day together with server ingestion; no financial owner, counter, member, hold or posting calculation needs to change. An insert-only metadata guard can stamp/validate these finite metadata fields without rewriting return/internal money owners; if the named settlement receive owner supplies it directly, use the full current2018 successor and keep CLAIM/post reusable. Do not add an ambiguous new overload or broaden the public args merely to manufacture a second independent date within this no-Worker card.

Current received_at is server-owned through the existing receive bodies because callers never supply it and INSERT omits it. The outcome immutable trigger protects updates/deletes; it does not enforce INSERT timestamp equality. If stronger owner-write enforcement is part of the small metadata support, an insert guard must enforce the current transaction ingestion timestamp rather than treating DEFAULT alone as that stronger guarantee. Internal0490 already enforces the exact server timestamps.

## Scope that remains explicitly pending

- EX-16 owns the single combined SCHEDULED/CANCELLED outcome_kind CHECK and cancel-origin columns/check/index. Do not install a standalone older PS_C02 cancellation schema after it or overwrite its combined catalogue.
- EX-27 owns the actual CANCELLED fact at the real admitted transition, including simple/Case/reissue/destination cancellation. PS-02 can record current absence and future required ownership; it cannot claim request completion is that fact or introduce a new cancellation producer.
- SCHEDULED, SUBMITTED, UNKNOWN and REFUSED are currently enum/guard possibilities, not available source producers. Their future Execution/Outcomes producers must provide immutable replay-safe fact + server ingestion + distinct business occurrence in the same transaction. Additional facts must not create an outcome_request: settlement retains the existing unique-per-transfer outcome request.
- The future joined D05 check remains NOT_RUN: yesterday's admitted settlement business day versus today's received timestamp, BLOCKED cancellation with no cancelled cash, atomic rollback with no current-transaction fact, and repeated receipt replay with one immutable fact. No full Worker/queue/minute or installed/business result is proved here.

## Source hashes at investigation

| Source | SHA-256 |
| --- | --- |
| core1324 | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| internal schema0480 | `032fcf4d3456b66c67c49a6399ee6a13207e223cb37b5d5fff0de3439e70f70b` |
| historical0200 / current CLAIM and outcome trigger attachment | `fa6bac8c799387f91717375219af885f65b334274bdb139a048f46e90e3a98fc` |
| final2018 receive/post/intake | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| return0230 | `0b4085def3cfcc9c6b8f7d1aae05716dea85578bd6433ed28f70038f98cc145d` |
| internal0490 | `88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c` |
| current0316 issue/reissue receive | `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6` |
| reissue0240 | `5f8365702daf773fae696a4c0861892f5fd19be701c517a00bbbbee07e50e714` |
| simple cancel0260 | `d10f3468e3fa6f39bc20b9e792f1d228259e5d234d433397cf5f552b9588135f` |
| reissue cancel0355 | `3f31e48aef1629bac6dea37cd2a40a1e2dd0b5806031971b0c5e8334f4873c55` |
| Case cancel0460 | `4417b25f96d6caefb627eae013c0c956268356104be15e2411dead56cc60bcf5` |
| registry | `83d8a27dc37d7193feaf651ca026b612344995d508281bbd839ec14b17d1d3cc` |
