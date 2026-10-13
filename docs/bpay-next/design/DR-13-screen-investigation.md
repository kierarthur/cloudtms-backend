# DR-13 — screen investigation, read-ahead only

11 October 2026. **Investigation only; DR13 is sequence122, not88.** Parent corrected order against IMPLEMENTATION_SEQUENCE/file08: DY02 is next after DR12. No DR13 product/spec/design build or render is authorised by this note. No screen approval, database/runtime/test claim or checklist tick. Reviewer read product/design sources and supplied PNGs only, then wrote this file.

## Authority and exact supplied design

Read full card `PLAN/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:357–380`, current checklist `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3513–3544` (D01–D06, including shared kernel and reviewed-output-only rows), dependencies DR11/DR16/WB41/F16, Part E OD04/RQ037, binding owner answers and latest owner A2/A4/11 October directions. PLAN is `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\cloudtms-backend\codex_outputs\claude-stage2-continuation-20260925\BPAY_NEXT_BUILD_PLAN_20261009_codex_r3`. Generic checklist prose does not authorise unrelated financial owners or replacing correct existing mechanics.

Pack inventory/index maps DR13 to **SCR44**, jointly owned with DR16/DR14/DR08. Exact read-only B4 references are `batches/B4/specs/SCR-44_draft_review.json`, `SCR-44b_draft_review_ready.json`, `SCR-44c_draft_review_loading.json`, `SCR-44d_draft_review_refresh_failed.json`, and their PNGs under `batches/B4/png/`. Independently viewed SCR44 desktop, SCR44b ready with expanded deduction rows, and SCR44 phone. Pack root: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\cloudtms-backend\codex_outputs\claude-stage2-continuation-20260925\SCREEN_DESIGN_POLICY_20261010`. No pack copy, generation or edit.

SCR44b's DR13 part is one native expanded Candidate detail: Deduction / Due / This Draft / Outstanding after / Note. Its examples include partial £60/£40 Manual Debt, paused £0/£60 overpayment, another-Draft £0/£50 instalment, and an unticked em-dash line. Supplied whole/channel totals, human identity, full review/reason/exclusion lists and other controls belong to their named cards; DR13 does not fabricate their reader facts or implement them early just to match a picture.

## Established current gaps and fields

| Current exact owner / reader | Available fact and limit |
| --- | --- |
| `private.bpay_next_case_allocation_row_v1(text,numeric,numeric,numeric,numeric,numeric,numeric,numeric)`, unchanged0290 | Computes allocated amount plus typed cap/affordability/capacity reason. Preserve this sole kernel; no UI recomputation. |
| `private.bpay_next_case_allocation_plan_v1(jsonb)`, final0204 | Captures immutable accepted rule choice, derives nominal and usable capacity under original money locks; current DERIVE is PAYE. Other-channel illustration is not authority to add conversion/financial scope. |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)`, registry final0904 | Inserts original DRAFT allocation result with exact preparation/selection identity and original instruction order. Current result fields: nominal_target_ex_vat, usable_capacity_target_ex_vat, allocated_source_ex_vat, allocated_target_ex_vat/VAT/incVAT, shortfall_target_ex_vat, cap_reason, affordability_reason, capacity_reason, headroom/take-home before/after. |
| `private.bpay_next_run_case_instruction`, original0300 schema | Frozen kind/subtype/tax/stage, nominal source/target, capacity source/target, rule/revision/week and identity. It does not itself expose four named review balances. is_selected constraint is true: do not infer unticked rows from absence of an instruction. Original choice reader is a separate owner. |
| Existing current Case/component pages | Live principal/recovered/write-off/active-hold and outstanding/amount_due exist before Draft; CASE_COMPONENTS recoverable_this_run is deliberately null. They cannot replace the frozen accepted allocation explanation after Draft. |
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)`, registry final0814; Worker `broker/src/bpay-next-preparation.js`; Office `js/bpay-next-office.js` | Current WORKERS/STATUS supply worker/parent state and frozen gross, not REVIEW_CASES. **REVIEW_CASES is absent from current Office/Worker page/row maps.** Current generic Candidate has no factual human name/reference; DR15 remains required. |
| Original allocation result/period stores | Nominal-shortfall is this instruction's unallocated due, not automatically total principal or global unreserved balance. Mutable period/Case counters may support allocation at that point but cannot be casually re-read as frozen facts. |

The four card-required concepts must remain distinct: principal outstanding; already active reserved amount; this Draft's recoverable allocation; remaining unreserved balance. For an £80 instalment from £240 principal with no other reservation, allocated80/shortfall0 does **not** mean principal outstanding0; £160 remains after reservation. Another Draft's active reservation also means shortfall is not a general unreserved balance. The reader contract must identify exact frozen supporting fields/identities for all four, or record the missing snapshot as the card's genuine gap. Do not rename nominal due/usable capacity into principal or subtract browser arrays. Do not sum source and converted target amounts.

WAIT is separately typed, not zero entitlement. Current0204:300–306 tests native WAIT only for the no-run estimate and explicitly leaves accepted-Draft WAIT branches to WB41. Do not manufacture ORIGIN_UNAUTHORISED_WAIT from a zero allocation, or run a live origin lookup from the UI. CASE_NO_PAYABLE_AMOUNT is the whole-worker REVIEW reason, not a Case-row cap reason; its plain reason/removal remains DR15/DR05.

## Narrow eventual UI contract

At its approved sequence, add the owning card's bounded REVIEW_CASES receipt to existing preparation read route and exact Worker/Office closed maps, with original accepted run/worker/preparation/selection identity, money strings, stable tuple cursor/order and closed typed reason. DR16 owns the native full review/totals integration; reuse F16's existing host rather than reviving the internal Sheet or adding a second review screen. This is a recommendation for the named missing card contract, not an invented implemented function signature.

Use the supplied native Deduction/Due/This Draft/Outstanding after/Note detail with no tick/mutation controls. Card-required principal/reserved/unreserved facts must remain separately named when exposed, despite the illustration's shorter columns. Money is right/tabular; actual zero differs from unavailable em dash. No Show estimate amount, no pay date before execute, no floor/threshold editor, no extra fake label/date. Use an in-place caret only where real detail exists, never Expand text or a redundant one-line child. Full human Candidate/detail/caret placement must be completed with DR15/DR16; do not approve wrong-person context while it is absent.

| Typed receipt state | Plain presentation / next step |
| --- | --- |
| Partial PAY_HEADROOM / TAKE_HOME_FLOOR | “£60.00 this Draft – not enough pay. £40.00 outstanding.” Keep the reason and retained balance; not Blocked. |
| Zero PAY_HEADROOM / TAKE_HOME_FLOOR | “£0.00 this Draft – not enough pay. £50.00 outstanding.” Zero repayment remains an ordinary included opportunity, not a permanent refusal. |
| EARNINGS_THRESHOLD | “£0.00 this Draft – below the minimum earnings threshold.” Outstanding remains separately factual. |
| CASE_CAPACITY | “£0.00 this Draft – this week's instalment is already in another Draft.” Existing active reservation is not recovered principal. |
| ORIGIN_UNAUTHORISED_WAIT | “£0.00 this Draft – recovery paused, waiting for re-authorisation.” Balance remains outstanding; only native WB41-owned typed result admits this wording. |
| CASE_NO_PAYABLE_AMOUNT worker | “Nothing to pay in this Draft. Repayments stay outstanding.” Existing Remove from Draft action; this is not a row cap or invented £0 gross. |
| Unknown reason/read not available | Plain inability-to-read/explain plus Refresh/Try again; never raw code, silent omission, invented identity or unavailable £0.00. |

These phrases apply policy wording to card terminology (this Draft, £ with pence, not enough pay), following owner A2 and policy.json manual wordlists. Current cap_reason precedence is already floor/threshold/pay versus tighter capacity; UI must consume it, not recalculate or override it. Review remains frozen and read only; returning to original Banking Pay ticks is prior owner scope.

## A4 evidence plan — no captures performed here

When the named reader/UI is built at sequence122, root's actual local native captures should show completed/full allocation, partial headroom, zero headroom/floor, threshold, another-Draft capacity, genuine WB41 WAIT, explicit excluded/missing when native receipt supports it, no-pay worker handoff, empty/loading and failed/readback states. Include mixed principal/reservation values proving shortfall is not principal, and exact money/row totals without browser sums. Compare every actual image beside supplied SCR44/SCR44b/c/d in dark, device-light and390px phone; record every difference and every control's payroll purpose. Full person context is DR15-dependent. Any absent later owner must remain named DEFERRED, not whole-screen PASS.

| All42 policy rows to assess later | Required evidence |
| --- | --- |
|1–4 | Full owning card/checklist, typed scope and frozen/zero/WAIT behavior; no late eligibility substitution. |
|5–11 | Existing frame/header, one tab layer, native body/detail with no box-in-box, table/tabular money, state tones, footer roles, scoped tokens/no !important. |
|12–15 | Bounded continuous scroll/stable server order; no page controls/client sums; read-only detail has no new ticks; totals from owning server receipt. |
|16–21 | Manual policy.json wordlists and sentence limits; plain inline reason/remedy, only necessary existing confirmations, empty/read failure, no internal IDs, UK dates/money/em dash. |
|22–24 | Source roles/focus/keyboard guards plus actual phone44px/cards/no overflow and unchanged device-light palette; executed keyboard claim only if actually performed. |
|25–27 |25 and spec readerAdditions26 N/A per owner; independently map every displayed field to own actual receipt; no pre-execution pay date. |
|28–35 | Understandability, subject-adjacent actions, next step, who/how much/why, reason/remedy, visible feedback, no redundant content, names consistent. |
|36–42 | Safe scope/confirmation/back-out, fewest clicks, principal/reserved/allocated/unreserved reconciliation, pre-Create owner boundary, busy/failure/keyboard coverage and return context. DR15 identity and unperformed joined acceptance explicitly DEFERRED/NOT_RUN. |

## Read provenance

Snapshot is read-ahead source evidence only, not installed-runtime or final DR13 implementation evidence.

| Root | Read file | SHA256 |
| --- | --- | --- |
| FE | js/bpay-next-office.js | `8b9e437170c1cb015d13391447d3fb20aa39b37718de9040dc0e5051097abdf3` |
| BE | broker/src/bpay-next-preparation.js | `572dee1fba45c21c01ae01cd60f5f3675e0b42363450625363bb7473f71eed20` |
| BE | docs/bpay-next/final-definer-closures.json | `56c8f726194c1f4ab353430c3e632a23275967ff221d44cca7976947a7398bab` |
| BE | supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| BE | supabase/repeatable/04102026_0290_banking_pay_next_case_allocation_row_v1.sql | `400e854bfd5619f7bd004e9e922d4681a673216ee001dd1a7cbd81ecf6d20ac6` |
| BE | supabase/repeatable/11102026_0204_banking_pay_next_case_allocation_plan_v1.sql | `2eeaafb8f14444715c30e13dc898f9b35614de1f53920344913f2986e09d6b0c` |
| BE | supabase/repeatable/11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql | `fa3c73a83075f67246e62adea7ba1467c019bada601d3db880ec51b48cdd1a9e` |
| Read-only pack | batches/B4/specs/SCR-44b_draft_review_ready.json | `263b16732dc2012927131b8c72bf862044d8c4f7565f399caa95bc61bed70f96` |
| Read-only pack | batches/B4/png/SCR-44b_draft_review_ready_dark.png | `78fbf039385de2f8307bd77a2db03fb65b899c22557549170b21fbef26c9ce8d` |
| Read-only pack | batches/B4/png/SCR-44_draft_review_dark.png | `da3f8cab213332c6dac526aeacaeca469f140e8e1a70a59777089359f2612fdf` |
| Read-only pack | batches/B4/png/SCR-44_draft_review_phone_dark.png | `80a7c271e5799c9fa2395885701530eb59050f67a3037fd716d7e8a9140177cc` |

Only this investigation file changed. No product/spec/pack/checklist write, render, database/hosted call, test/fixture, commit or deploy. Stop this read-ahead here; parent proceeds DY02 in the approved sequence.

Sequence122 update12October: this dated read-ahead is retained provenance only. Current0552 implementation and BD135 supersede its old owner/gap/manual-release recommendations; see DR-13-build-scope.md/read-contracts.md/completion-partial.md. D03/WAIT-specificD05/ITEM explicitly blocked; no genuine accepted WAIT is claimed.
