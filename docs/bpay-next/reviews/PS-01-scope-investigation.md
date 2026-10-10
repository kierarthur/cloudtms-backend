# PS-01 scope investigation

Source-only review, 10 October 2026. No product edits, SQL/runtime/testing, hosted reads or data writes. Binding source: normal R3 `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1327–1349`; build checklist PS-01-D01/D02/D05/D06 and shared-context D03/D04.

## Required scope

PS-C01 is accepted ledger/schema work independent of AUTH-PS. The required current change is a scaffolded new migration on only `private.bpay_next_net_projection` and `private.bpay_next_transfer`, plus the current simple-transfer builder successor. Historical D01 lists Case schema context; it does not require rebuilding its unrelated tables. Stored payment/deduction Case TARGET INC cannot reuse ex-VAT accepted gross additions/deductions or be derived in a CHECK. Two dedicated nonnegative penny-amount columns are appropriate. Preserve both PAYE cash branches and CASE_PAYOUT branch exactly. Replace only the formerly unconditional UMBRELLA branch after an exact named predecessor assertion. Retain the original gross ex+VAT=inc and NET-nullability constraints.

The actual current simple builder is `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`, not historical 0130. Its ordinary Umbrella path already binds each transfer to one run-worker/Candidate and its immutable projection. `private.bpay_next_claim_simple_transfer_job_v1(uuid,integer)` remains final in 0130 and requires no target change. Preserve module/epoch/lease/cursor/replay checks, destination and Case dispatch, original terminal member reconciliation, PAYE adjustment behavior and command/job completion. Populate stored bridge values only on genuine terminal ordinary Umbrella completion from that exact projection.

## Enforced storage and immutability

The original transfer has no stored ex/VAT/inc/Case TARGET INC bridge. A nullable five-amount bridge is appropriate during BUILDING, with an all-NULL-or-complete shape CHECK and a required complete tuple before a financially ready original Umbrella transfer. A stored UMBRELLA discriminator and an exact composite FK to projection `(id,run_worker_id,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,payment_case_target_inc_vat,deduction_case_target_inc_vat,cash_amount)` close type and identity ambiguity without a new financial calculator. Existing 0390 whole-row projection immutability includes new fields automatically. Existing 0145 pins transfer projection/worker/cash/beneficiary identity.

A terminal-cancellation exemption must permit an unassembled cancellation without manufacturing a ready bridge; once any complete bridge is captured it must never be cleared or changed, including on ready→CANCELLED. A minimal successor to the existing 0145 callback for that exact freeze is justified support; do not modify the immutable historical 0145 file or attach a new generic guard. Reissues retain original cash lineage and need no fabricated earning bridge.

The current F06 `bpay_next_projection_umbrella_work_cash_bridge_v1` remains a stricter WORK-only constraint. CM-13a owns its explicit later replacement and ordinary Case producer population through0410; CM-14 owns moved origins. Current ordinary builder should refuse nonzero dedicated Case terms, including balanced terms, so an equal payment/deduction pair cannot masquerade as current WORK support. PS01 must not pretend these future producers are built.

## Member-sum confirmation, precise evidence

0145 `bpay_next_transfer_member_guard_v1()` is channel-neutral: only INSERT is allowed and its actual parent must still be BUILDING. It freezes Umbrella members as well as PAYE members. It does **not** itself compute SUM or COUNT. The actual member reconciliation is supplied by the current 2018 builder: accumulate each actual frozen run-line member, require captured line count with no omitted later line, require frozen gross equality, apply the unchanged PAYE-only NET member, require final sum=projection cash, then publish MEMBERS_READY. The 1324 transfer CHECK independently requires stored positive member_count/member_cash_sum=cash at MEMBERS_READY. Completion wording should cite this combined owner+schema+immutable-member proof, rather than claiming 0145 computes an SQL SUM. No new proof framework is required.

## Acceptance and release limits

D05 preserves the installed 0300 baseline receipt; D03/D04 are context only, not a reinstall instruction. D06 must record ordinary WORK100/VAT20→120, rejected mismatch/partial or foreign bridge, per-Candidate separation, exact PAYE/CASE_PAYOUT preservation, immutable members, and deferred CM-13a/14 120−36→84 Case example. All real app/Worker/queue/minute checks remain NOT_RUN. A migration that adds required bridge fields without a backfill will deliberately refuse an existing financially ready original Umbrella row lacking retained bridge fields; current hosted contents are unknown here. This is an installation precondition to check under the later release workflow, not permission to invent historical facts.

## Source checkpoints

| Current source | Raw SHA-256 |
|---|---|
| migrations/03102026_1324_banking_pay_next_core.sql | f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c |
| migrations/04102026_0300_banking_pay_next_case_instruction_core.sql | 04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e |
| migrations/09102026_2011_banking_pay_next_initial_tsfin_umbrella_work_shape_v1.sql | 8dd6232ccda068022e9ab8faef163d89eeaa883720a0e95e441db56177a7aeef |
| repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |
| repeatable/04102026_0130_banking_pay_next_simple_transfer_members_v1.sql | 763176b54482954adf8c07e7415a86e005baae71848d03bba3245ee8c385907a |
| repeatable/04102026_0145_banking_pay_next_transfer_guards_v1.sql | 2006007829398c9d849ddad959ea7e665f07b37a49f08dddd3e56385c8623a2c |
| repeatable/04102026_0390_banking_pay_next_case_paye_net_v1.sql | 9622c5b9259f80e69a4fb661425c0016a4f3ee69ee8b02fcb3a7baa7ba389fbb |
| repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql | f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8 |

Paths are relative to this build repository's `supabase/`. Investigation does not claim installation or runtime success. Root's subsequently saved 0533 implementation is independently reviewed in `PS-01-scope-review.md`.
