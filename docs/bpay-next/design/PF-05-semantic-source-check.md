# PF-05 sequence 20: finite semantic source check

10 October 2026. Read-only inventory/design for root implementation. No product edits, gate/unit/SQL/runtime execution, fixtures, hosted calls or checklist changes. Full controlling normal R3 card read at `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1764–1780`; current BUILD-BE checklist MD 493–509 and exact CSV PF-05-D01/D02 read. F-09 is the sole dependency; no design approval is required. D01 owns the source check. D02 is the future joined application checklist, with Worker/queue/minute-tick acceptance NOT_RUN.

The card forbids a theoretical R/C/Q/W/B/E admission allowance (including renamed forms), another round-trip to price a processing step, and a processing-payer ancestor graph. It explicitly permits legitimate financial money helpers and operational page/byte/time bounds. The current F23 gate supplies the existing module/discovery/unit seam; a second harness or general call-graph parser is unnecessary.

## Actual current transport inventory

All paths below are repository-relative within BUILD-BE. `broker/src/index.js:40985–41020` binds fresh owner status and the three background adapters; its NEXT route block at 198719–198800 binds history/frozen/prepare/command adapters and only wakes enrollment after the original accepted command. The wake is metadata, not a pricing preflight.

| Actual Worker transport | Fixed SQL endpoint(s) | Current physical final definition |
| --- | --- | --- |
| bpay-next-command-rpc.js:7–31 | bpay_next_financial_intake_v1; BIND_STORED_CREDIT alone bpay_next_bind_stored_credit_v1 | 09102026_2018 foundation_pending_successors; bind 04102026_1827 candidate_destination_group |
| bpay-next-preparation-rpc.js:3–16 | bpay_next_preparation_intake_v1 / bpay_next_preparation_page_v1 | 09102026_2018 foundation_pending_successors |
| bpay-next-job-rpc.js:5–24 | bpay_next_financial_job_v1 | 04102026_0280 financial_job_service |
| bpay-next-runtime-rpc.js:17–18,95–128 | bpay_next_enroll_service_v1 / bpay_next_job_wake_page_v1 / bpay_next_expiry_dispatch_page_v1 | 04102026_0560 queue_recovery_service |
| bpay-next-preparation-expiry-rpc.js:14–17 | bpay_next_preparation_expiry_due_v1 / bpay_next_preparation_expiry_receive_v1 | 04102026_0520 preparation_expiry_service |
| bpay-next-legacy-fence.js:4,70 | bpay_next_owner_status_v1 | 04102026_0550 legacy_writer_fence |
| bpay-next-history.js:215 | bpay_next_payment_history_page_v1 | registry-controlled immutable history reader |
| bpay-next-frozen-rpc.js:4; bpay-next-frozen-advice-rpc.js:15 | bpay_next_frozen_detail_page_v1 / bpay_next_original_csv_advice_page_v1 | registry-controlled frozen readers |

The exact public financial wrapper signature is `(uuid,text,jsonb)`, preparation intake/page `(uuid,text,jsonb)`, financial job `(text,uuid,text,jsonb)`, enrollment `()`, job wake `(text,integer)` and expiry dispatch `(integer)`. The registry, rather than filename date inference, supplied each final physical winner above. Public wrapper dispatches remain financial intake/private financial owners, preparation selection/review/confirm, or lease/phase/cursor job work. No pricing-preflight RPC, tariff allowance argument or fee-ancestor authority was found in these current NEXT paths.

`bpay-next-command.js` has closed action INPUTS. Money fields `entered_paye_net` and settlement/return `amount` are actual financial inputs; they are permitted. `bpay-next-preparation.js` has closed CREATE/WORK_PAGE/COMPONENT_PAGE/SEAL/REVIEW/CONFIRM fields. Choices retain actual `expected_revision_id`; this card does not reopen F25's parked protocol. `bpay-next-job-step.js` has exact lease/epoch/phase/cursor fields and finite limits. Background handles one existing adapter per wake and bounded recovery lanes; it contains no earnings pricing call.

## Small implementation recommendation

Add one exported finite check to `scripts/bpay-next-source-gates.mjs` and invoke it through the existing inventory paths. Apply it to **actual NEXT routine declarations, calls, DDL column/relation identities and request fields**, and actual NEXT Worker endpoint/call/request identifiers. Normalize quoted SQL identifiers and underscore/camel-case components. Existing SQL comment/body/token handling and current source file discovery are sufficient. No general call graph or newly invented RPC inventory is needed.

Use composite semantic identifiers, not unrestricted words. Reject the finite combinations identifying (a) admission allowance/tariff/budget, (b) processing or step pricing/fee/cost/budget, and (c) processing payer/fee ancestor/fee graph, when they occupy those executable declaration/call/DDL/argument roles. Include G01/C1 only when tied to the abandoned processing-fee/ancestor/admission architecture; a bare marker is insufficient. The normal source unit should visibly list the rejected vocabulary. Illustrative negative probe names are test fixtures for this policy, **not claims that those endpoints currently exist**.

A compact collector can obtain SQL callable/declaration names, DDL relation/column names and JSON request-key accesses, plus Worker RPC-target/name/argument-key identifiers. The same composite matcher can then serve each role; roughly thirty lines of policy code plus ordinary finite source assertions should be enough. Preserve source literals used for request fields, but do not feed comments, error prose, docs, arbitrary text values or all unclassified identifiers into a vocabulary ban. A renamed composite such as processing_credit_allowance must still be classified by its semantic components. This is a finite source boundary, not proof that arbitrary concealed semantics can be inferred statically.

Required finite negatives: actual SQL call/declaration, NEXT admission request field or DDL authority with composite tariff/budget semantics; Worker RPC pre-pricing target/argument; processing payer ancestor/fee graph. Include one underscore-to-camel rename control and one quoted SQL identifier. Required positives: bare G01/C1 unrelated markers, Source C1 replay/publication, financial rate/amount/conversion helpers, page/byte/millisecond/lease controls, and the real financial inputs above. Test only this named source check through the existing source-unit file; no business fixture, pricing service or extra programme.

## Important false-positive controls from saved source

- `broker/src/index.js` contains old `budgetProfile` NUDGE/CRON scheduling options and finite claim/max-jobs/lease/time limits. They are operational controls and must not be rejected because of the word budget.
- Actual `public.banking_pay_hot_path_budget_apply(text)` at `26052026_2100HRS_NEW_FUNCTIONS.sql:132073–132164` returns void and only applies `statement_timeout`, `lock_timeout` and `idle_in_transaction_session_timeout`, using finite values/settings. It neither computes a processing price nor consumes an admission allowance. Its name alone is not forbidden architecture. No such call was found in the current NEXT wrappers inventoried above.
- `26092026_0202_banking_pay_stage2_source_authorisation_v1.sql:6` uses G01 to label Source/expense separation, and line 1828 invokes that legitimate timeout helper. A repository-wide G01/budget word ban would incorrectly reject shared Source.
- `private.weekly_source_pay_query_admit_v2` is transaction contention/security admission, not paid processing allowance. `private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)` has final `06102026_1157_banking_pay_shared_selected_quote_v1.sql` and is a genuine money conversion helper. Both remain permitted. No Source/Candidate/invoice owner rewrite is implied.

## Current source receipts

- Existing gate: `7cbdba5eccca9c83191f872f4fc60f9199e42287d8fbbb2524772c34fa10c55c`.
- command RPC: `060ce9bdab50576aec731d0b9b3c6499a608e3cd71c17a3c9bdf54483db1b68e`.
- preparation RPC: `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34`.
- runtime RPC: `9c8c94e9bb1a3a42c126c815d6f896f59de20bf75ed34d776c089c5628e55a91`.
- financial job RPC: `d00d086d44baaee414ca8509fbe9c30b91015e55982d84c2035e32a3dfba7fb7`.
- checklist CSV read: `ceb82eddbcc3fc446e1d048f25f098fec9cf2e426b6008db53c14bb798e38dca`.

Root alone implements D01 and records execution; root's prepared D02 checklist remains a later joined acceptance boundary. This inventory grants no runtime, SQL, deployment or application-test authority.