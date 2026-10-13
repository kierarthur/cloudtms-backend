# FC-16 current investigation

Date: 11 October 2026. Sequence: **100**. Scope: read-only inspection of current saved product, card, checklist, owner directions and final-definer registry. This is an investigation, not implementation or acceptance. No product, database, runtime, fixture, screen-render, deployment or financial action was performed.

## Required boundary

The full FC-16 card (03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md, lines 1372–1395) requires protection of both accepted PREPARING case inputs before a hold exists and active READY/DRAFT reserved slices. A case edit, pause, restructure or write-off may change future unreserved authority; it must not rewrite a protected accepted input or frozen confirmed Draft slice. An eligible whole Candidate release uses the original DR-05 WORKER_RELEASE route. A confirmed Draft requires the later EX-27 Candidate cancellation; FC-16 must neither depend on EX-27 nor call an unbuilt EX-27 object.

The checklist's remaining owned work is D04 (banking_pay_next_case_change_guard_v1 repeatable), D08 (private.bpay_next_case_reserved_slices_v1), and the shared assertion represented by D09/private.bpay_next_case_change_assert_v1. D05 is the documented, typed reuse of existing WORKER_RELEASE, not a new command. D01/D02 preserve the installed 0310 guards; D03 preserves the current prepare-case/one-plan chain. D06 is the card output and D07 the existing immutable case instruction foundation.

Master ordering is FC-16/100, FC-03/101, EX-08/102, FC-09/107, FC-10/108, FC-11/132, FC-12/133 and FC-13/134. FC-16 is not an additional allocation-plan chain link and must not implement those later case surfaces early. Direct dependencies are DR-05 and F-05.

## Current owners and finite writer census

The F04 registry selects these physical final sources, rather than historical ancestors:

| Current function / responsibility | Final physical source | FC-16 treatment |
| --- | --- | --- |
| case_hold_guard_v1; case_capacity_use_guard_v1 | 04102026_0310_banking_pay_next_case_guards_v1.sql | Preserve exact immutable hold/capacity transition rules. They are not a complete case-change guard. |
| apply_case_create_v1 | 04102026_0330_banking_pay_next_case_creation_selection_v1.sql | Genuine case creation owner; do not turn creation into a protected-slice rewrite. |
| post_case_transfer_member_v1 | 04102026_0410_banking_pay_next_case_transfer_post_v1.sql | Original realisation owner; retain exact hold/event money tuple checks. |
| apply_write_off_v1 | 04102026_1647_banking_pay_next_work_collection_write_off_v1.sql | Current generic amendment seam needing accepted-before-hold protection. |
| apply_case_paye_net_page_v1; cancel_case_worker_page_v1 | 11102026_0559_banking_pay_next_worker_reprepare_v1.sql | Original projection/cancellation owners. Do not indiscriminately block their frozen ordered authority. |
| release_prepare_case_hold_v1; reconcile_work_collection_v1 | 11102026_0814_banking_pay_next_preparation_discard_v1.sql | Original release/reconciliation owners. Protect amendment authority while preserving legitimate release, expiry, discard and realisation paths. |
| prepare_case_page_v1 | 11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql | Current allocation/reservation owner. Retain the existing accepted-selection and allocation plan flow. |
| case_allocation_plan_v1 | 11102026_0204_banking_pay_next_case_allocation_plan_v1.sql | The one existing pure plan; no replacement or second money owner. |

The current-function census found eight final writers of finance_case, case_rule, case_period, case_component, case_hold or case_capacity_use: apply_case_create, post_case_transfer_member, apply_write_off, apply_case_paye_net_page, cancel_case_worker_page, release_prepare_case_hold, reconcile_work_collection and prepare_case_page. It scanned registry-selected function bodies. This identifies actual mutation owners; it does not justify inserting the same refusal into every allocation, settlement, cancellation and release path.

0310 permits a typed ACTIVE hold insertion and only exact ACTIVE→RELEASED/REALISED transitions. Capacity-use transitions must match the hold and, for realisation, a matching financial event. Neither trigger accounts for aggregate case capacity, protects accepted PREPARING inputs before holds, or intercepts every case-authority writer.

## Accepted-before-hold source gap

The saved metadata includes private.bpay_next_accepted_case_input, with captured case/component/rule revisions and an accepted_input_id key. However, the current repeatable-source search finds only its 2256 guard/trigger: **no current producer or consumer was found**. This is a source fact, not a claim about installed or business rows. Its existence alone cannot satisfy FC-16.

Current accepted Create instead freezes CASE_COMPONENT membership in private.bpay_next_run_choice_member (current final 1358 guarded Create, carrying the earlier 0426 authority); selected=false components are excluded. Current 1358 takes the component SHARE locks at lines 220–225 and inserts the native case members at lines 375–377. The current prepare-case page consumes the native sealed case selection. The original F-25 Rule B boundary deliberately did not add an accepted_case_input producer. The implementation must derive protection from the actual accepted run/choice/selection owners and exact component identity. It must not invent a second capture protocol or rely on an unused table.

An accepted selected component before allocation has a known protected binding, but no allocated hold money tuple in run_choice_member. Do not report fabricated £0 or calculate a new estimated slice in this reader. The final design must state how its fixed summary represents a protected, not-yet-allocated input, using current factual authority. Active holds provide exact source_reserved_ex_vat in the original source basis; do not sum unrelated target-channel amounts as one cash figure.

Current write-off checks remaining principal and active_recovery_hold_amount and refuses a whole remaining write-off that exceeds unreserved active-hold capacity. It does not cover an accepted selected PREPARING component with no hold. The guard must run before the financial mutation/event and must not produce a whole-case COMPLETE receipt while protected authority remains.

## Smallest reader / assertion contract

Use a bounded, independently paged protected-reference reader, with a small exact summary containing the card's unreserved_capacity, protected_count, protected_amount and next_cursor. Keep protected references out of an unbounded JSON array. Qualify every reference by actual case/component, Candidate, run/worker, channel and native state; include accepted-before-hold and active READY/DRAFT references once each. Exclude released holds and genuinely excluded accepted members.

Existing native storage already supplies case/component authority, immutable run choices/sealed case selection, active case holds/capacity use, run-worker and pay-run identity. Current active hold indexes include (case_id,id) WHERE ACTIVE and (run_worker_id,id) WHERE ACTIVE. Selection has component/run and selected-page indexes; run choices have run/Candidate/kind/member keys. Do not add a new table, global counter ledger, reason column, history scan, framework or money formula. Verify the exact chosen accepted-case lookup has a bounded native index; add only a required narrow index if the current key cannot serve that predicate.

The assertion must consume the same predicate and exact current authority used by the reader, including a stale/revision refusal. It must not treat a browser summary as authority. Record lock order explicitly: retain the owning command's run/header-before-Candidate order, then its existing job/request/case/component order. Before any eligible READY hold is released, the run leaves confirmable readiness under that same header lock. Do not acquire a different run header while already holding a Candidate lock.

## Existing Worker / Office reuse

The current preparation transport already accepts WORKER_RELEASE with command_id, run_worker_id and expected_preparation_revision. Its finite receipt carries command/run/worker identity, action, expected/current preparation revision, review revision, agency sequence, phase and replay. It uses the original PREPARE job/release phases and ENROLL handoff; no new route, command kind or job kind is needed.

The existing Office run-scoped controller retains the original command bytes and frozen worker_action receipt. Unknown outcomes replay the same original request; a higher revision alone does not unlock another intent. Settlement requires the original COMPLETE replay and matching current worker/Candidate/parent/revision facts. Existing Review Draft / Remove from Draft is the eligible whole-Candidate release surface. FC-16 callers must reuse this original action and display the guard's actual protected reference and required next step.

There is **no direct FC-16 screen or Worker handler deliverable**. Do not fabricate human names, references, cancellation controls or a new case UI. Later FC-09/10/11/12/13 callers must consume these exact results. A protected confirmed Draft reports the amount and the later cancellation requirement without changing the Draft.

## Completion evidence to collect

After the narrow implementation, inspect exact final registry owners, bounded/indexed reader predicates, strict transport contract reuse, grants/private exposure, and compile/source gates. Those checks belong to the author. Future joined scenarios remain NOT_RUN: accepted-before-hold case change, READY release/re-review, confirmed Draft pause retaining its frozen deduction, concurrent/stale intent and protected whole-case write-off.

No A4 screenshot approval is applicable to FC-16 alone. No runtime, payment, provider, database or whole-item acceptance is claimed here.

## Source provenance

Final source follow-up: the card's shared summary should return **protected_amount=NULL and unreserved_capacity=NULL while any accepted selected binding remains unallocated**. Its exact protected binding count/reference remains visible; a whole-case write-off is blocked until original release. This is a truthful unknown, not a zero amount or a new estimate. Future unreserved authority may still advance through the later case cards' own immutable intent/version support. Genuine completed zero allocation must be distinguished from merely finding no hold.

There is no registered standalone current NEXT case balance/capacity reader. The current case reader (0585) returns stored principal/active-hold facts. The current component reader (2026:557–619) returns factual outstanding_source_ex_vat and the official week's amount_due, but not generic unreserved capacity; it requires OPEN/PAUSED and current official-date context. Do not call that page as a generic mutation authoriser. The one pure plan (0204:181–289) owns purpose-specific availability, including funded loan/advance recovery, weekly due, realised recovery and disjoint active holds. Do not invoke or copy its whole allocation logic into the new protection reader.

Frozen run_case_instruction.nominal_source_ex_vat is scheduled/requested nominal; capacity_source_ex_vat is its captured ceiling before allocation. Neither is the actual protected money slice. A real case_hold.source_reserved_ex_vat/result taken tuple supplies the latter. Before that exists, the native selected binding supplies identity and count only.

The current write-off result guard (1647:171–175) requires write_off_request.protected_amount to equal the stored active_recovery_hold_amount. Its immutable migration also requires non-null protected_amount for BLOCKED/REVIEW. Preserve that original allocated-hold field's meaning; FC-16's nullable pending protection summary must not silently replace it or present its possible £0 as the full pending protection total. Any original receipt carrying the pending refusal must use truthful reason/guard evidence before the financial mutation. The ordinary write-off capacity remains the original source-qualified remaining-principal/active-recovery-hold expression; it is not a generic funding or cash-capacity formula for all case kinds.

Current WORKER_RELEASE source is 0814:3590–3725. It locks pay_run FOR UPDATE before Candidate worker_control, rejects confirmation/expiry/discard and invalid revisions, then invalidates parent readiness/review revision and maintained counters before hold mutation. It already permits eligible PREPARING/REVIEW/READY workers and exact native pending enrolment release. Reuse that owner; do not create a cross-run release path from the case guard.

F-05 was read as the baseline dependency: it reconciles exact older NEXT definitions while preserving upstream changes, owner/epoch admission and current final source. It adds no direct screen. Owner OR-44/OR-45 and F-25 Rule B govern the current captured selection, rather than reviving historical operational engines or dormant accepted-case tables.

Hashes below are saved-file SHA-256 at this investigation boundary. Product is still owned by the root author; a later changed file needs a fresh implementation review. SQL paths are relative to BUILD-BE unless marked FE.

| Source | SHA-256 |
| --- | --- |
| docs/bpay-next/final-definer-closures.json | 60ac256660798807f2ad55fd3db990b1bbade96f5cafdb495c1dca0d4bf12d26 |
| supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql | 7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe |
| supabase/repeatable/04102026_0330_banking_pay_next_case_creation_selection_v1.sql | 9dc328ef5fb4e172622f72a97ce3cb84184740c5fe2575eb67f8bf1dac5fe903 |
| supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql | f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8 |
| supabase/repeatable/04102026_1647_banking_pay_next_work_collection_write_off_v1.sql | 77019761e500193ddd5d8c4147aa0db5bf389c2d1deb02d576407548c0ed4a87 |
| supabase/repeatable/04102026_0585_banking_pay_next_current_case_pages_v1.sql | ec0ab1ec0ce95b690f50c711023a6dda431e562ed075a9716361d1ede2aac436 |
| supabase/repeatable/09102026_2256_banking_pay_next_accepted_case_week_guards_v1.sql | 5226a8c39e53f7900a1dfd6a466b8991b51cdff2c8095c52c300825d759dfd6f |
| supabase/repeatable/10102026_2026_banking_pay_next_current_group_details_v1.sql | 278bd4d3dccc1e7453a69d1d2e0e4bdcde7075e5fe5181c5fe090c4a9dfc7e8a |
| supabase/repeatable/11102026_0204_banking_pay_next_case_allocation_plan_v1.sql | 2eeaafb8f14444715c30e13dc898f9b35614de1f53920344913f2986e09d6b0c |
| supabase/repeatable/11102026_0426_banking_pay_next_pay_hold_accepted_choice_v1.sql | aec4c904cee22e748ea1ab3006e8cfdbf50bcdb9dd567b1fb1b2bc9da7391b92 |
| supabase/repeatable/11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql | d40cc95cb6675f09b1fe06a87801e42a6a28ca1f02a563534ae5e1a65263b5cb |
| supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql | 514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6 |
| supabase/repeatable/11102026_0814_banking_pay_next_preparation_discard_v1.sql | 29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c |
| supabase/repeatable/11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql | fa3c73a83075f67246e62adea7ba1467c019bada601d3db880ec51b48cdd1a9e |
| supabase/repeatable/11102026_1358_banking_pay_next_guarded_create_v1.sql | 6d13cd0a482be586e49aaeb2fb67bca82613aa092657cbcc208e6f012acbc0a6 |
| broker/src/bpay-next-preparation.js | d8e678c400a4ea39a63cd67a75f16562116b70babbcf748435aa2b7ac2e072b7 |
| FE/js/bpay-next-office.js | 9791dbe4268ecdb63f0de7dacaf0dce929ead441b6cedd0bab181b450f2e6b1f |
