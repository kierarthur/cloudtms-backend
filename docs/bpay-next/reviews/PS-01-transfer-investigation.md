# PS-01 current transfer-owner investigation

Read the full PS-01 card (`06` lines 1327–1352), canonical D01/D02/D05/D06 and shared-evidence rows, master F04 chain and saved BUILD-BE final-definer registry on 10 October 2026. Source investigation only: no product writes, tests, SQL/database/business/browser/fixtures/network/secrets execution. The existing release receipts do not identify the current successor or prove app behavior.

## Actual defining owners

| Exact signature | Current final physical/root | Relevant source |
| --- | --- | --- |
| `private.bpay_next_claim_simple_transfer_job_v1(uuid,integer)` | `04102026_0130_banking_pay_next_simple_transfer_members_v1.sql` | Claim lines 7–65: current NEXT epoch, Candidate control, exact job kind, lease and earlier unfinished Candidate jobs. No bridge money is created here. |
| `private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | Lines 1655–1863: F05 successor owns the current PAYE/Umbrella behavior. Historical `0130` is not the final build body. |
| `private.bpay_next_begin_simple_transfer_v1(uuid,uuid)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | Lines 1524–1652: existing per-worker/Candidate original transfer, Umbrella beneficiary and ordinary WORK projection. No PS01 edit needed. |
| `private.bpay_next_transfer_guard_v1()` / `private.bpay_next_transfer_member_guard_v1()` | `04102026_0145_banking_pay_next_transfer_guards_v1.sql` | Channel-neutral transfer money/identity/status protection and append-only members while BUILDING. |
| `private.bpay_next_net_projection_guard_v1()` | `04102026_0390_banking_pay_next_case_paye_net_v1.sql` | Lines 183–197: accepted projection permits only its first finite retirement timestamp; every other field is pinned by full-row comparison. New projection columns inherit that comparison. |
| `private.bpay_next_case_transfer_member_guard_v1()` | `04102026_0410_banking_pay_next_case_transfer_post_v1.sql` | Real Case/destination bindings and typed members remain separate. No PS01 widening. |

PS01 is editor **1 of 5** for logical `0130`: **PS01 → DY09 → DR38 → DR22 → CM13a**. Select the latest F05 build body as predecessor, retain historical roots, then register the new exact build signature/root/physical/closure. CLAIM remains its current owner unless the new source demonstrably needs a claim change; none is needed for bridge population.

## Existing schema is stronger than the historical card snapshot

- Core `03102026_1324`: `net_projection` stores `gross_ex_vat`, `gross_vat`, `gross_inc_vat`, `entered_paye_net`, `accepted_recoveries` and `cash_amount` as penny money; `gross_inc_vat=gross_ex_vat+gross_vat`, unique `(id,run_worker_id)`. `transfer` has cash and maintained member count/sum, but **no stored ex/VAT/inc/Case bridge**. `(run_worker_id,candidate_id)` and `(projection_id,run_worker_id)` FKs preserve the Candidate/worker leg. Unique `(run_worker_id,transfer_no)` supplies per-worker addresses; there is no grouping by Umbrella company.
- `04102026_0300:565–569` still defines `bpay_next_projection_case_cash` with the unconstrained `(input_kind='UMBRELLA')` branch. Its PAYE_MANUAL/PAYE_IMPORT formula and CASE_PAYOUT zero-work formula must be kept literally in the successor.
- F06 additive migration `09102026_2011:63` already adds `bpay_next_projection_umbrella_work_cash_bridge_v1`: UMBRELLA gross ex/VAT nonnegative, ex+VAT=inc, NET/request NULL, all existing adjustment/recovery fields zero and cash=inc. Preserve this ordinary-WORK refusal. Future CM13a/CM14 must deliberately supersede the WORK-only constraint as part of their genuine Case authority; PS01 does not enable Case shapes now.
- Existing `accepted_gross_additions/deductions`, `accepted_net_additions` and recoveries have their present PAYE/Case semantics. They are not a proven generic Umbrella payment-Case/deduction-Case TARGET INC pair. Dedicated stored TARGET INC operands are therefore the clean finite extension; default zero supports the existing ordinary WORK insert. Do not derive Case amounts from current rules or an aggregate in a CHECK.

## Minimum ordinary WORK successor

Add only a full copied successor of the current **2018 build function**. Preserve all current exact branches and locks, then at the terminal UMBRELLA branch copy the immutable projection bridge into the new transfer columns in the same update that sets `MEMBERS_READY`.

1. Preserve NEXT/module check, exact job→transfer→worker Candidate scope, run/control/job/transfer/worker locking, lease nonce/owner epoch/deadline, expected cursor and DONE/cursor replay paths.
2. Preserve destination-group and Case-transfer delegation before ordinary branch locks. Existing Case paths stay on `0410`; no new Case calculator, Case allocation, PAYE NET or conversion branch is introduced.
3. Preserve each bounded line's `qualified_line_id`/`qualified_revision_id`, WORK/TAXABLE, UMBRELLA source and target, nonnegative source consumed, frozen ex=consumed, projection UMBRELLA, no Case selection or payroll NET/request. Keep `BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED` and its refusal shape.
4. Keep the bounded WORK member insert, monotone member/cursor counts and cash accumulator. On completion still require no unseen captured line, count exactly captured count and member sum equal worker gross inc and projection cash. Assert both **new dedicated payment-Case and deduction-Case TARGET INC operands are individually zero**; equality/balanced net zero is insufficient to prove ordinary WORK.
5. Keep PAYE's existing single `NET_ADJUSTMENT` member and cash behavior byte-for-byte. UMBRELLA adds no NET_ADJUSTMENT. Copy ex/VAT/inc and the two zero Case operands directly from this exact immutable projection, with the discriminator `UMBRELLA`; actual transfer cash already exists. Do not reprice, sum history, copy mutable Candidate settings or alter the begin owner.
6. Preserve terminal job DONE/phase/cursor/lease clearing, command COMPLETE and existing output JSON/rows_visited. Partial-page updates retain an absent bridge until the terminal copy.

## Minimum schema closure: exact immutable composite FK

Root's proposed exact composite-FK design is sufficient without rewriting `0145` or adding another trigger:

- Add the dedicated nonnegative finite penny TARGET INC projection operands with ordinary zero defaults, and replace only the exact asserted predecessor `bpay_next_projection_case_cash` UMBRELLA branch with stored inc+payment−deduction=cash / NET NULL. Keep PAYE and CASE_PAYOUT branches exactly. The separate F06 WORK-only CHECK remains intentionally active.
- Add a nullable transfer `UMBRELLA` discriminator plus five nullable ex/VAT/inc/payment/deduction copies. Use an explicit all-null or all-non-null CHECK, finite/domain money and local arithmetic. Require the complete tuple for an **original Umbrella-beneficiary** transfer outside BUILDING/CANCELLED. Cancellation before member assembly and non-Umbrella/cash-reissue forms retain their prior shape.
- Add a unique exact immutable projection tuple over `(id,run_worker_id,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,payment_target_inc,deduction_target_inc,cash_amount)`. Add the corresponding transfer composite FK containing projection identity, discriminator, all five copied operands and **actual transfer cash**. Explicit all-or-none/required CHECKs close MATCH SIMPLE's NULL escape.
- `0145` pins the projection identity, `0390` pins the entire projection row including new columns, and the required transfer CHECK prevents clearing a completed original Umbrella bridge. The composite FK prevents changing any bridge value to a different earning/VAT/Case tuple with the same cash. Its explicit column-list guards do not independently freeze new fields, but the immutable exact FK closes that gap. An extra trigger is unnecessary.
- Existing original ready Umbrella rows without newly added copies are a release precondition to reconcile explicitly. This source investigation makes no count claim and proposes no current mutable reconstruction, fake bridge, blanket historical exemption or blind reinstall/backfill. Preserve the immutable baseline migrations and create additive new source.

## What the existing member guard actually proves

`0145` prevents INSERT after BUILDING and any member UPDATE/DELETE. Core `1324:694` requires `member_count>0` and stored `member_cash_sum=cash_amount` at MEMBERS_READY, independently of beneficiary channel. The final 2018 builder maintains that sum alongside each bounded member insert and checks exact frozen gross/cash before sealing, so its same-channel Umbrella path is covered. **Do not claim `0145` performs an independent aggregate SUM over actual member rows.** No new full-transfer/history scan or duplicate financial counter is needed for this successor. Current PK/FKs and unique `(transfer_id,run_line_id)` prevent duplicate/foreign WORK membership; `0400`'s exact subject shape and `0410`'s bound Case guard remain intact.

## Installation/security ownership

Use the normal new additive migration then new repeatable after schema availability. The build successor retains `SECURITY DEFINER`, `search_path=pg_catalog,private,public`, logical owner `postgres` and REVOKE ALL from PUBLIC/anon/authenticated/service_role, matching its current private runtime entry. Existing public service-only runtime/financial wrappers remain its only API path; no new Worker/Office endpoint or grant is needed. New columns inherit existing owner/private RLS/table privileges; do not add browser policies or grants. Keep the release's audited logical-owner mapping and final-definer closure selection. Commit/source integrity/security evidence is separate from installation and app testing.

Future D06 acceptance remains **NOT_RUN**: ordinary Umbrella 100/20/120/0/120; bad bridge rejected; two Candidates sharing an Umbrella retain two distinct transfers; PAYE and CASE_PAYOUT unchanged. The Case 120−36=84 example belongs to later CM13a/CM14 authority and is not enabled by this PS01 ordinary-WORK successor.

## Read source hashes

| File | SHA-256 |
| --- | --- |
| `09102026_2018` final build/begin | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| `04102026_0130` final claim / historical build | `763176b54482954adf8c07e7415a86e005baae71848d03bba3245ee8c385907a` |
| `04102026_0145` guards | `2006007829398c9d849ddad959ea7e665f07b37a49f08dddd3e56385c8623a2c` |
| `04102026_0300` projection cash predecessor | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| `03102026_1324` core schema | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| `09102026_2011` ordinary Umbrella WORK constraint | `8dd6232ccda068022e9ab8faef163d89eeaa883720a0e95e441db56177a7aeef` |
| `04102026_0390` immutable projection guard | `9622c5b9259f80e69a4fb661425c0016a4f3ee69ee8b02fcb3a7baa7ba389fbb` |
| `04102026_0410` Case owner | `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8` |
| `final-definer-closures.json` | `06b12aca8673b4d5b113684e21c744ba92d29618de43b70e585b71042cab3309` |

This is the completed source investigation and minimum successor recommendation, not a review of product source still being authored or a whole PS01 completion claim.
