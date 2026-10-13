# FC-12 sequence 133 — schedule semantics and exact future-period seam

12 October 2026. Read-only design challenge for the full FC-12 card (`03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1267–1289`) and owning D02/D03/D04/D06/D07/D09 rows; all FC-12 rows were read. Complements `FC-12-investigation.md`. Root alone writes product. No SQL/RPC, business data, tests, fixtures, hosted calls, product/checklist edits or screenshots were performed. **Design evidence only; all future joined acceptance NOT_RUN.**

## What the actual schedule currently means

Current registered `private.bpay_next_case_allocation_plan_v1(jsonb)` and `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` live in `12102026_0408_banking_pay_next_other_draft_preparation_v1.sql`, not the original 0360 or the copied 1939 plan.

For a manual RECOVERY, current DERIVE reads funded component/Case principal for LOAN/ADVANCE and approved principal otherwise, subtracting real recovered/written-off values (0408:191–194). Once `pay_week_start >= next_due_monday`, nominal recovery is `least(weekly_due_source_ex_vat, component outstanding, Case outstanding)` (203–214). The actual capacity is additionally capped by the period's opening due less realised/active recovery and disjoint component/Case active holds (303–310). Threshold/floor and the sole allocation row kernel remain later allocation authority.

**There is no duration/end-week gate.** `schedule_week_count` appears in the exact old/new rule-identity tuple at 275/278, but is not consulted to stop repayment after that many weeks. `schedule_start_monday` likewise is retained identity; allocation eligibility uses `next_due_monday`. The code does not decrement week count or advance next due after each deduction. No amount becomes zero merely because a nominal last-due Monday has passed.

The existing `private.bpay_next_case_create_terms_v1(jsonb)` in `11102026_1536_banking_pay_next_case_create_intake_v1.sql:81–97` derives:

- By weeks: weekly amount = penny ceiling of total / entered weeks.
- By weekly amount: required weeks = ceiling of total / weekly amount.
- Last due = first Monday + 7 × (weeks − 1), checked against the supported date range.
- The returned total remains the input principal. The coverage guard prevents a supplied week count being too short (83–84).

These are schedule terms and a nominal last-due date, not a guarantee of N successful deductions. Shortfalls, floors, unticked weeks and protected deductions affect actual progress. FC-12 can reuse this arithmetic through a finite schedule-input adapter while keeping original principal authority separate; it should not turn week count into a new allocation stop rule.

## Manual total remaining: safe interpretation and hard limits

The proposed narrow interpretation is compatible with the existing kernel **only if manual total is an immutable scheduling operand**: it derives weekly amount/week count but does not replace the liability. Keep original funded/approved principal, recovered R, written-off W, paid credit, active holds, funding event/date and Case age unchanged. Limit a positive entered schedule total to the genuine source outstanding; keep it separate from original approved principal in the immutable restructure receipt. Current live outstanding and unreserved capacity still cap every allocation.

An explicit eligibility nuance remains: that positive funded-outstanding limit would prevent an entirely unfunded Advance from restructuring, because its recovery outstanding is zero. The card does not state a funded-only restriction; SCR125's supplied unfunded Advance row includes Restructure. Root must reconcile that before adopting the limit universally. A genuinely permitted unfunded scheduling-only path can use the unchanged original approved amount as prospective schedule basis while native recovery stays zero until real funding (0408:210–211); it must not display that approval as an already funded repayment debt. This is a design boundary, not a recommendation to manufacture funding or expand selection.

If entered total is less than actual outstanding, preview must show both facts: amount used to calculate the arrangement and genuine outstanding. The remainder stays owed; repayment can continue beyond the nominal number of weeks under the existing kernel. Calling the entered amount the new financial “total still to repay”, or claiming the liability ends at the derived last Monday, would be false without an independently authorized financial adjustment. FC-12 names no such write-off/approval/funding rewrite. Do not silently add one or reset counters to fit the form.

The card's Done example (1286) does not require a new formula: actual outstanding £150 and a five-week scheduling input yields £30 × 5, while the old £50/week corresponds to three nominal weeks. But if the same £150 includes a £50 active protected deduction, only £100 is currently unreserved. The protected £50 remains governed by its captured old rule and period. A preview may truthfully show the proposed **nominal** £30 × 5 arrangement and unchanged protected £50; it cannot promise five additional £30 collections plus that protected £50, because native conservation permits only £150 total. Similarly, using £100 instead would derive £20 × 5 and would not match the card's stated £30 example.

**Hard conflict to flag, rather than choose silently:** if “manual total remaining” is intended to replace debt, or Done is interpreted as five guaranteed future £30 deductions in addition to the same protected £50, that conflicts with the explicitly preserved real effects/holds and actual outstanding cap (1271,1274–1275). Source proves no existing authority for either interpretation. Root's design must state the scheduling-only meaning and before/after consequences explicitly, or record the unresolved financial interpretation before building. The supplied SCR125 overlay also displays “Total still to repay” and “Last due”; those captions must not imply a new liability or guaranteed execution date. No screenshot alone authorizes extra money behavior.

## Minimum immutable approval association

Current DERIVE qualifies manual Case approval through `bpay_next_case_edit_origin_v1(rule_id)` (0408:157–185). The existing helper is in `11102026_1939_banking_pay_next_case_edit_v1.sql:6–53`. For a funded rule it first follows the exact FUNDED event → payout instruction → approved rule and requires schedule/threshold/floor equality with that approved payout rule (15–31). A post-funding restructure with genuinely changed terms therefore fails this original branch before it could return NULL. A naive `current_rule_id` update is insufficient.

Small required support is an immutable native restructure request/terminal amendment association, uniquely point-addressable by its new rule UUID and exact Case/Candidate/rule revision. It retains the original approved-principal source, previous/new terms, manual scheduling input, reason/actor, effective next-due Monday, expected versions and original funding/creation identity. A constant-size new-rule receipt qualification must precede the original create/edit/funding-only qualification; no generic exception fallback or history-latest lookup.

Successive restructures need direct retained approval provenance, not recursive traversal of all prior rules. Keep the true approved principal independent of each scheduling total. If an unfunded advance is restructured and funded later, the existing funding owner copies that rule's terms into its funding successor (`04102026_0410_banking_pay_next_case_transfer_post_v1.sql:451–454`). Qualify that successor through its exact FUNDED event and payout instruction's rule UUID → restructure receipt point, retaining the original funding-only tuple checks. A lookup only on the funded successor's own UUID would miss the actual approved restructure rule. This is the existing native funding bridge, not a new funding event or calculator.

Manual approval must still match component original approved principal, native source/tax/Case/component identity and original create or preactivity edit approval (0408:159–182). A scheduling amount must never be substituted for `v_approved_principal`. Resolution provenance should name the exact restructure receipt when it supplies the new schedule, preserving original immutable funding age (`0408:361–377`). Existing Rule-B selection memory supplies the accepted old rule/revision (`0408:58–86`); do not overwrite that with the later current rule.

## Minimum future-period consumption branch

The original period key is `(case_component_id, pay_week_start)` (0408:236–249). Current differing-rule handling accepts either an unused preactivity CASE_EDIT opening or the exact unchanged-schedule funding successor (254–301). The existing FC09 period-amendment table has an FK specifically to `case_edit_request` (`11102026_1939_banking_pay_next_case_edit_request.sql:43–64`); its helper explicitly forbids a funded predecessor and requires FC09 approval (`1939:254–271`). It cannot receive a fabricated CASE_EDIT row for a restructure.

Add only the explicitly typed restructure association and its exact period-amendment receipt, with immutable OLD/NEW rule and period revisions/opening values. Consume it at the existing bounded point when the named prepare writer encounters that week, using the current Case → component → period locks (`0408:740–745`). The STABLE shared plan returns the prospective amendment; the writer performs it and verifies the resulting exact period row (747–787). No whole-history period rewrite or acceptance-time scan is needed.

The allowed point must be RECOVERY, at/after the recorded effective Monday, have zero realised recovery and zero active recovery/payout counters, and be genuinely unreserved under FC-16. Zero counters alone do not establish that an already captured/accepted slice is unprotected: the card explicitly preserves captured/held rule/period facts (1271), not just positive holds. Retain protected and earlier period openings; never rewrite an old instruction/result. For such a retained opening, the design must explicitly KEEP/refuse according to the approved new-rule binding instead of falling through to the old funding equality arm. New instructions may use approved new nominal terms while capacity remains limited by the exact retained opening and live original counters; old captured instructions continue using their own immutable rule/capacity.

Use the existing Candidate ordering, exact own job/lease and DR-37 financial fence before mutation. R2 `addendum/R2_EXACT_CONNECTIONS.md:38–40` explicitly includes FC-12 in the Candidate-wide fence: another live NET owner defers promptly, never financial REVIEW. FC-16 FUTURE_ONLY permits the future arrangement while reporting protection; it is not permission to alter a protected slice. WB-08 refresh follows the actual rule/Case update and financial-view invalidation as the existing chain requires. No new queue/action beyond the named CASE_RESTRUCTURE, producer, channel-conversion or money owner is justified.

## Source checkpoint and delivery boundary

| Physical source | SHA-256 |
|---|---|
| `12102026_0408_banking_pay_next_other_draft_preparation_v1.sql` | `278096454907768263320ba6d53eaa28bce367c79b30fac08597e86bc2845698` |
| `11102026_1939_banking_pay_next_case_edit_v1.sql` | `7b723a61096ac50853dbc609acbba6437b403f11aa796a6f77e39a78b8308f66` |
| `11102026_1536_banking_pay_next_case_create_intake_v1.sql` | `853ddce45455defed9f715a2593267794f3b224e469a9eaccde2aeca6e44a470` |

D02/D03/D04 own the typed native receipt, read-only before/after and ordered mutation. D09 owns the necessary current prepare/plan period-consumption connection. D06 closes finite Worker mappings; D07 implements truthful defaults/preview/help and retained exact retry. Shared/context rows do not license another money formula, FC-13 channel restructuring or parked F25 storage. The £150 example can be displayed with the existing penny terms; exact duration and replacement liability interpretations remain the explicit design boundary above.
