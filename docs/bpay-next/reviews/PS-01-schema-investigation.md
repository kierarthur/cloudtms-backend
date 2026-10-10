# PS-01 stored Umbrella bridge investigation

10 October 2026. Read-only source investigation; no SQL, tests, fixtures, runtime, network, secrets or business actions. Root is the sole product writer. Scope is PS-C01's data/ledger compatibility, not the owner-gated Payment Summary screen or future Case execution owners.

## Exact card and checklist

Read the complete PS-01 card at plan06:1327–1356 and read `.codex-local/checklist-records.cjs` to identify its exact CSV selection (`Item ID` equals `PS-01`). The CSV was read as records without executing that script.

| Row | Required object |
|---|---|
| D01 | New scaffolded schema change for the named 0300 constraint and stored projection/transfer bridge |
| D02 | Scoped 0130 successor, claim/build functions; actual current authorities below |
| D03 | CONTEXT only: existing installed 1324 receipt, not an implementation task |
| D04 | CONTEXT only: existing installed 0130 receipt, not an implementation task |
| D05 | Existing installed 0300 migration receipt: confirm it against the new schema change; do not edit/reinstall the old migration blindly |
| D06 | Future joined in-app checklist; execution remains NOT_RUN |

PS-01 is position 1 of 5 in the 0130 editor chain and depends on F-05. Accepted PS-C01 does not depend on AUTH-PS. The card explicitly leaves ordinary Umbrella Case population to CM-13a and moved-origin population to CM-14. No future Case writer is changed now.

## Current stored schema

`03102026_1324_banking_pay_next_core.sql:631` creates `private.bpay_next_net_projection` with stored `gross_ex_vat`, `gross_vat`, `gross_inc_vat`, `entered_paye_net`, `accepted_recoveries`, `cash_amount`, request/projection identity and clocks. Its original CHECK already requires `gross_inc_vat=gross_ex_vat+gross_vat`; cash is nonnegative. The 0300 migration adds nonnegative penny-domain `accepted_net_additions`, `accepted_gross_additions` and `accepted_gross_deductions`, extends input kinds/net nullability, and adds the exact predecessor cash constraint:

```sql
(input_kind='UMBRELLA')
or (input_kind in ('PAYE_MANUAL','PAYE_IMPORT')
    and cash_amount=entered_paye_net+accepted_net_additions-accepted_recoveries)
or (input_kind='CASE_PAYOUT' and gross_ex_vat=0 and gross_vat=0 and gross_inc_vat=0
    and accepted_gross_additions=0 and accepted_gross_deductions=0
    and accepted_recoveries=0 and cash_amount=accepted_net_additions)
```

The named `bpay_next_projection_case_cash` predecessor is at 0300:565–569. Assert its exact audited catalog expression before replacing it; retain the PAYE_MANUAL/PAYE_IMPORT and CASE_PAYOUT branches exactly.

There is also an existing F-06 constraint at `09102026_2011...:63`, `bpay_next_projection_umbrella_work_cash_bridge_v1`. Every UMBRELLA row currently requires nonnegative ex/VAT, ex+VAT=inc, NET/request command NULL, all four accepted adjustment/recovery fields zero, and cash=inc. It is a real additional restriction; replacing the 0300 constraint alone cannot enable Case cash. Root's narrow choice is to retain it now because current supported Umbrella execution is ordinary WORK only. CM-13a must explicitly supersede that restriction when its named supported Case producer is implemented. This fact must appear in PS-01's completion receipt; no current Case support is implied.

No projection column has an established, universal Umbrella payment-Case/deduction-Case TARGET INC meaning. The current 0390 producer is PAYE/CASE_PAYOUT only; 0410 accumulates `allocated_target_inc_vat` by payroll stage into build totals and compares those totals with the existing accepted_* fields. Its supported PAYE scope has zero VAT. Reusing those stage-specific fields as universal Umbrella cash terms would introduce an unsupported interpretation. Root's chosen additions are nonnegative stored `payment_case_target_inc_vat` and `deduction_case_target_inc_vat`, default zero. These are actual target-inclusive totals, not source/ex-VAT amounts and not a derived CHECK expression.

The new UMBRELLA cash branch can enforce, entirely on stored fields:

```sql
input_kind='UMBRELLA'
and entered_paye_net is null
and gross_inc_vat=gross_ex_vat+gross_vat
and cash_amount=gross_inc_vat
    +payment_case_target_inc_vat-deduction_case_target_inc_vat
```

The existing `cash_amount >= 0` and penny/range constraints remain. For current ordinary WORK the two new totals are zero. Future CM-13a/CM-14 own their population from actual immutable target-inc allocations and the explicit supersession of the WORK-only restriction; PS-01 does not calculate those allocations.

## Transfer bridge and minimum support

The transfer table at 1324:651 currently stores Candidate/run-worker/projection identity, beneficiary, cash, member count/sum, status and frozen destination details. It has **no stored ex/VAT/inc/Case bridge columns**. The 0400 Case-transfer migration adds typed member/build relationships, not a transfer-level bridge.

Root's bounded choice is five nullable transfer copies: Umbrella ex-VAT, VAT, inclusive WORK, payment-Case target-inclusive and deduction-Case target-inclusive, with the exact arithmetic against existing `cash_amount`. Use the original Umbrella transfer identity (`beneficiary_kind='UMBRELLA'`, original transfer/projection binding), not an umbrella-company aggregation. Add an exact composite FK to the immutable projection including projection ID, run-worker ID, all copied bridge values and cash, supported by an exact unique projection key. This prevents fabricated copies and does not derive money in a CHECK.

Require either the permitted all-NULL shape while an original Umbrella transfer is BUILDING, or the complete all-non-NULL bound bridge. After BUILDING an original Umbrella transfer must have the full bridge. Non-Umbrella/reissue rows retain their existing scope without fabricated earning fields. Explicit all-null/all-nonnull tests are necessary: a default MATCH SIMPLE composite FK alone silently exempts any partially NULL key. The current simple reissue owner only permits original Candidate transfers, so this addition does not create an Umbrella reissue workflow.

The existing 0390 `private.bpay_next_net_projection_guard_v1()` uses `to_jsonb(new)-'retired_at_utc'` equality and freezes all new projection fields automatically. Existing 0145 freezes transfer projection/run-worker/Candidate/cash identity during BUILDING and MEMBERS_READY, and whole-row non-status fields after issuance. The exact FK makes copied bridge values immutable even though the two early-state manual field lists do not name the additions. No new trigger or 0145 rewrite is needed for this scope.

## Actual final simple-transfer owners

Registry source, rather than historical filenames, controls the edits:

| Signature | Current final defining file |
|---|---|
| `private.bpay_next_claim_simple_transfer_job_v1(uuid,integer)` | `04102026_0130_banking_pay_next_simple_transfer_members_v1.sql` |
| `private.bpay_next_begin_simple_transfer_v1(uuid,uuid)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:1524` |
| `private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:1655` |
| `private.bpay_next_transfer_member_guard_v1()` and `private.bpay_next_transfer_guard_v1()` | `04102026_0145_banking_pay_next_transfer_guards_v1.sql` |

The claim function owns the existing module/Candidate/job lease and command-order checks. It needs no bridge change. Begin creates one projection/transfer for one run worker, copies that Candidate ID even when the beneficiary is its umbrella, requires no prior transfer for that worker and accepts only the supported ordinary WORK Umbrella shape. It also needs no edit if bridge fields may be absent only while BUILDING.

The **2018 final build body**, not the earlier PAYE-only 0130 body, is the scoped successor source. Preserve destination-group/Case dispatch, module/lease/run-worker scope, cursor replay, fixed 1–100 pages and PAYE NET_ADJUSTMENT behavior. Its ordinary Umbrella lines must be qualified TAXABLE WORK, same source/target channel, exact captured revision/line and frozen ex equal to consumed source ex. It refuses Case/conversion paths with `BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED`.

At the existing terminal branch around 2018:1800–1835, after frozen-line count/gross and cash checks and before the single MEMBERS_READY update, copy the five bridge values from `v_projection`. **Assert both new target-inclusive totals are zero for this current simple WORK-only path before copying/sealing.** Otherwise a fabricated balanced payment=deduction pair could satisfy both cash CHECKs while there are no Case members. Preserve the single job/command completion and replay result. No new money owner, calculator, claim or begin rewrite is required.

## What 0145 actually guarantees

The member trigger is channel-neutral: it rejects UPDATE/DELETE and allows INSERT only when the exact transfer is BUILDING. Its compound FKs bind each member to the same transfer/run-worker and each WORK line to that run-worker. The unique WORK member index prohibits duplicate line membership. It does **not** itself calculate a member sum; the 2018 builder accumulates frozen member cash and refuses mismatch, while the transfer's `MEMBERS_READY` CHECK requires `member_count>0` and `member_cash_sum=cash_amount`. Together these cover current ordinary Umbrella as well as PAYE. Do not describe the trigger alone as an aggregate proof.

One Candidate leg remains bound through `(run_worker_id,candidate_id)` and one transfer per current simple run worker, with beneficiary umbrella ID separate from Candidate ID. Two Candidates sharing an umbrella do not become one company transfer. Preserve this exact ownership; no new grouping query is needed.

## Future acceptance and honest completion

D06 should save, without executing, the card's 100/20/120/0/120 ordinary bridge, mismatched projection/transfer refusal, exact frozen member sum and two-Candidate legs; unchanged PAYE/CASE_PAYOUT branches; known ordinary Case example 120−36=84 assigned to CM-13a and moved-origin variant to CM-14; PAYE earnings basis 70 is not its recorded payroll NET. Full Worker/queue/minute/in-app acceptance remains NOT_RUN. No fixtures, scenarios, hash programme or screen is introduced.

PS-01's completion records PS-C01 and the exact stored column mapping. Current ordinary supported closure and stored future Case terms must be distinguished from later Case producer delivery. Historical migrations stay immutable; use scaffolded additive migration and scoped repeatable successor with updated final-definer authority. All original ACL/RLS/private ownership remains.

## Source receipt (SHA-256)

| File | SHA-256 |
|---|---|
| 1324 core migration | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| 0300 Case core migration | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| 2011 F-06 shape migration | `8dd6232ccda068022e9ab8faef163d89eeaa883720a0e95e441db56177a7aeef` |
| 0130 predecessor/claim | `763176b54482954adf8c07e7415a86e005baae71848d03bba3245ee8c385907a` |
| 0145 transfer/member guards | `2006007829398c9d849ddad959ea7e665f07b37a49f08dddd3e56385c8623a2c` |
| 2018 current begin/build | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| 0410 current Case transfer | `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8` |
| Final-definer registry | `06b12aca8673b4d5b113684e21c744ba92d29618de43b70e585b71042cab3309` |
| Exact checklist CSV | `f429852aef1cebaa5ec562b0832478b728f9fbf3d0a23b2cac16ba100b0ae3e4` |

Only this investigation document was written by the reviewer.
