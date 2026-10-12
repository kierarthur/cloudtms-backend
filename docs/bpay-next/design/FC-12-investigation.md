# FC-12 sequence133 — current-source investigation

12 October 2026. Read-only investigation of full card03:1267–1289, every FC-12 checklist row (`IMPLEMENTATION_CHECKLIST.md:3931–3957`), current final-definer registry, FC03/FC09/FC16/DR37 owners and supplied SCR125 specification. Root alone authors products. No SQL/RPC/business data, tests, fixtures, browser/captures, hosted actions, product/checklist edits or commits were performed. **Design required before build; this is source evidence, not product or joined acceptance.**

## Exact remaining scope

D02 is the new named restructure repeatable; D03/D04 its write-free CASE_RESTRUCTURE_PREVIEW and ordered CASE_RESTRUCTURE contracts; D06 the existing command transport mapping; D07 the native restructure window; D09 the exact current prepare-chain successor. D01/D05/D08/D10 are shared-owner/context evidence, not second implementations. The card requires LOAN/ADVANCE/MANUAL_DEBT repayment terms only, weeks or weekly amount, next due Monday, manual total remaining, threshold/floor, required reason, before/after, expected Case version, FC16 protection and targeted WB08 refresh. Preserve WB26's independent per-run tick. FC13 channel restructure, automatic Work-origin debt producers, new money formulas and later surfaces are excluded.

## Current owners and genuine gaps

| Signature / object | Latest physical parent and source evidence |
|---|---|
| `private.bpay_next_case_allocation_plan_v1(jsonb)` | `supabase/repeatable/12102026_0408_banking_pay_next_other_draft_preparation_v1.sql:5` (DR14), not historical0360 or FC09's copied plan |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | same0408:484; original Case→component→period locks at740–747; DERIVE and exact period writes at750–783 |
| `private.bpay_next_case_allocation_row_v1(text,numeric,numeric,numeric,numeric,numeric,numeric,numeric)` | unchanged0290; sole row allocation formula |
| `private.bpay_next_case_edit_origin_v1(uuid)` | `11102026_1939_banking_pay_next_case_edit_v1.sql:6`; exact create/edit/funding point qualification, no history-latest fallback |
| `private.bpay_next_apply_case_period_amendment_v1(uuid,jsonb,jsonb)` | same1939:233; exact OLD/NEW + one revision, zero used/active counters, original immutable edit FK |
| `private.bpay_next_case_change_assert_v1(uuid,bigint,text,numeric,text)` and protected bindings | `11102026_1509_banking_pay_next_case_change_guard_v1.sql:238` and10; FUTURE_ONLY explicitly does not rewrite accepted rules/periods (235–237) |
| `private.bpay_next_assert_candidate_financial_fence_v1(uuid,uuid,uuid,bigint)` | `12102026_1215_banking_pay_next_case_net_replacement_v1.sql`; real current NET owner/lease fence, reuse before Case money locks in apply |
| `private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)` / pause state | `11102026_2041_banking_pay_next_case_pause_v1.sql`; current exact schedule/outstanding/protected facts and RECOVERY-purpose summary; reuse for form defaults |
| `private.bpay_next_workbench_refresh_case_v1(uuid)` | `12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql`; existing targeted refresh |

The original0270 public financial intake/enroller is historical architecture evidence only. Resolve current intake/enroller/job/page bodies from the final registry before composing successors; do not reinstall0270 or restore0360. Current rules are immutable (`0310:286–289`), native `(Case,rule_revision)` is unique (`0300` migration:37), and original creation/funding age remains separate from new terms. Native rule INSERT guard0310:104 only checks real funding-event evidence, not restructure receipt authority.

The exact blocker is current0408:158–176: a manual rule must qualify its original CASE_CREATE or FC09 preactivity CASE_EDIT approval and original approved-principal/component identity. New current_rule_id alone is not accepted. At254–301, an existing period admits only the exact unconsumed/unfunded CASE_EDIT_OPENING or original funding successor, with exact funding tuple/unchanged terms. A funded new schedule is deliberately refused by that funding-only branch. FC12 must add its own finite immutable approved-restructure point, not weaken the existing arms.

**Funded-rule lookup ordering matters:** current `case_edit_origin_v1`1939:12–31 follows a rule's original funding event back to the payout instruction and requires the rule's schedule/threshold/floor tuple to equal that original approved payout rule. A funded restructure inheriting the true funding event but changing its schedule fails here before the helper can return NULL. Positively qualify the new restructure rule by its own immutable indexed receipt before this original funding/edit-only path; do not catch arbitrary INPUT_UNBOUND and call it a restructure, remove the original equality guard, erase funding evidence, or invent a second funding event. The approved-principal basis must remain the original real approval, not the entered scheduling total.

Registry-qualified service parents are financial intake `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` and financial job `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` in1215; private enroller `private.bpay_next_enroll_one_v1()` in2041; Workbench public page `public.bpay_next_workbench_page_v1(jsonb)` in0235. Original unrelated branches/security/ACL/replay must survive their complete successor copies.

## Smallest safe future-period boundary

Append a new rule and exact immutable request/amendment receipt with original Case/Candidate/component, actor, expected version, previous/new rule revisions, typed inputs, derived terms and effective next-due Monday. Original request replay must be checked before new eligibility; a changed Case after preview is refused. Keep Candidate ordering/own job lease and DR37 fence before Case→component→period mutation locks. FC16 FUTURE_ONLY allows future authority but is not permission to change a captured old slice.

Do not enumerate all period history at acceptance. A bounded native component/week point when later prepare actually consumes that future week can append the exact OLD→NEW period-opening receipt and increment the period revision only if the period is eligible, unused/unreserved, at/after the new effective week and bound to a genuinely approved predecessor. Protected earlier or future periods, old accepted selections, existing instructions/results/holds and original opening history remain unchanged. Explicitly define the KEEP/REFUSE behavior for an already used/protected period whose rule differs; do not route a restructure through the funding-only equality test or silently substitute the newest rule for an accepted cut.

The existing FC09 `case_period_amendment` cannot blindly store this approval: migration1939:59 binds `edit_command_id` to `case_edit_request`, and helper1939:258–265 rejects funded predecessors and requires preactivity edit provenance. Any reuse needs an exact separately typed restructure association and finite authority branch; no fabricated CASE_EDIT receipt, relaxed whole history or kernel rewrite. Preserve original funding date/event and original approved principal for the plan's origin checks.

**Design decision still needed:** the card's manual total remaining must have explicit server-owned meaning. Current0408:194–209 computes real outstanding from original funded/approved minus recovered/written-off/paid credit, then takes the weekly term against live outstanding; it does not have an independent replacement liability balance. Do not reset principal, recovered R, funded amounts, written-off W or a whole-Case snapshot to make entered total fit. Specify whether the approved manual total is a scheduling operand with unchanged liability (and truthful preview), or whether another exact recorded adjustment authority is genuinely card-required. The supplied screenshot alone cannot authorize a new money owner. Current legacy `pay_finance_case_restructure` (`26052026_2100HRS_NEW_FUNCTIONS.sql:102047`) has no manual-total argument and derives schedule from stored remaining amount; the full current card is therefore the controlling requirement for this gap.

The existing native create-terms helper (`11102026_1536_banking_pay_next_case_create_intake_v1.sql:5,81–95`) already owns penny ceiling/week-count derivation and Monday checks. Reuse its correct arithmetic through a finite schedule-specific input adapter where compatible; do not implement browser pricing or reclassify tax/source channel. No future tax/channel conversion is needed by this card.

## Screen, transport and history

SCR125 is `SCREEN_DESIGN_POLICY_20261010/batches/B9/png/SCR-125_restructure_dark.png` and `batches/B9/specs/SCR-125_restructure.json` (DESIGN_INDEX:140, inventory:201). Its overlay names the card's fields, before/after weekly/weeks/next/last/total and the WB26 tick help. Native FC02 currently delegates its Restructure More action through `options.caseActions.restructure`; connect that real seam and the existing NEXT qualified native caller. Reuse FC03 Preview/Apply invalidation/journal and native window lifecycle; pending retry retains identical original bytes/UUID and does not reinterpret later live terms. No extra credential helper is card-required.

Preview is genuinely read-only, has no command/wake and returns bounded typed before/after/control/protection facts; Apply is a finite action on the existing financial route with native expected-version/replay/order. Own reader/schema validation must preserve nullable floor and unknown capacity. Use a native Case-point re-read after success, not agency/Candidate inventories. Date here is the next due week, not an execution pay-date promise.

FC14's current audit2129 explicitly understands CASE_CREATE and CASE_EDIT receipts, but not a future restructure request. When FC12 authors its immutable amendment, ensure the existing native audit can display that actual event's actor/reason/old/new terms through its bounded exact source branch; do not label it as an invented principal approval or fabricate an activity timestamp. Keep original rules and period-amendment history addressable.

## Source checkpoint

| Physical file | SHA-256 |
|---|---|
| current0408 plan/prepare | `278096454907768263320ba6d53eaa28bce367c79b30fac08597e86bc2845698` |
| FC09 current1939 | `7b723a61096ac50853dbc609acbba6437b403f11aa796a6f77e39a78b8308f66` |
| FC16 current1509 | `087c3c5d3ae9cfaa94b66ea58b338358d0236f2b4953be5ee6adf7e3dfc60fd4` |
| DR37 current1215 | `23ac3d72354eac877d66e61e249de0db94f1f820bb7f96a90373c5d5425d72bb` |
| component/pause2041 | `80675b1f77af852977da427d0ce0702bfb2a2da6fafcea2a5e05b731810de44b` |

Final design, exact new storage/owner count and conditional protected-period/manual-total behavior must be resolved before product implementation. All product/installation/A4/business journeys remain NOT_RUN here.
