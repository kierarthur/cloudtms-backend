# FC-09 sequence107 — exact amendment and kernel investigation

11 October2026. Read-only design findings for root; no product/checklist edits, SQL/DB execution, tests, business fixtures, hosted actions or rendering. Full owning card03:1196–1215 and all working FC09 rows3158–3186 were read. This is a focused supplement to the primary FC09 design investigation, not a build receipt. Every future check is NOT_RUN.

## Current exact owners

| Native owner | Current registered saved source |
|---|---|
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | `11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql` |
| `private.bpay_next_case_allocation_plan_v1(jsonb)` | `11102026_0204_banking_pay_next_case_allocation_plan_v1.sql` |
| Common create terms/context/apply and14-argument compatibility accept | `11102026_1536_banking_pay_next_case_create_intake_v1.sql` |
| Create Preview/17-argument accept/current components/Case-list row | `11102026_1910_banking_pay_next_manual_debt_reference_v1.sql` |
| FC16 change assert `(uuid,bigint,text,numeric,text)` | `11102026_1509_banking_pay_next_case_change_guard_v1.sql` |
| Rule/event/result/hold/selection guards | `04102026_0310_banking_pay_next_case_guards_v1.sql` |
| Current instruction insert guard | `11102026_0559_banking_pay_next_worker_reprepare_v1.sql:4398–4425` |
| Original funded-rule successor and transfer posting | `04102026_0410_banking_pay_next_case_transfer_post_v1.sql` |
| Read-only estimate using the same plan | `11102026_0207_banking_pay_next_candidate_estimate_reader_v1.sql` |

The card/checklist reference0360 is historical. Current0904 no longer implements the creation-origin check itself:333–345 locks exact Case, component and pay-week period and calls the shared DERIVE plan. That plan, not a second writer-only formula, is the actual origin-check owner. D08 therefore needs the appropriate small shared-plan successor as well as any essential prepare write operand extension. Original live balance locks and immutable instruction insertion remain.

## Proved origin gap

0204:50–88 reads the exact native Case/component and the accepted memory-cut rule ID/revision when present; ordinary current estimate reads the current rule. Accepted identity includes kind/subtype/tax/instruction/direction/stage/source-channel/ordinal; balances remain live under the original writable locks. Rule B is unchanged.

0204:89 and152–174 reads immutable original `case_create_request` by exact Case/Candidate. It checks original component identity, instruction role, source channel and principal equality. **Principal equality at164 will reject a legitimate principal amendment.** It does not compare the selected rule ID with original create.rule_id; changing a current rule pointer alone is therefore neither complete amendment provenance nor permission to alter original approval facts. Ordinary automatic Work-collection OVERPAYMENT has its separate qualified branch and must remain untouched.

The smallest honest authority is an immutable CASE_EDIT request containing the original creation/Case/Candidate identity, expected original Case revision/current rule, complete approved resulting terms, resulting rule ID/revision and same native component IDs. Give its new_rule_id an exact unique native point relation to the selected unfunded/amended rule. After funding, use the existing typed funding-event/instruction chain described below to reach that same rule; no new rule column or money-owner rewrite is required. Do not use `latest edit`, MAX revision, history search or current Contract/Client inference. The original creation path remains when there is no amendment. An amended rule requires an exact matching approved edit receipt/event and exact resulting principal/tax/schedule/threshold/floor/component projection. Keep original creation source channel, valuation policy, creator timestamp/order and component identities; later current metadata cannot become approval authority.

0204:173,288–300 uses original creation policy and `CASE_CREATE:<command>:PAYE` resolution reference. A proven amendment needs its exact CASE_EDIT reference while retaining the original valuation authority. The existing native resolution-ref byte bound is256 (0300:98); no new signing/digest/proof framework is needed. ALLOCATE0204:358–395 and row kernel remain unchanged, including age tuple/order, capacity, threshold/floor/headroom and disjoint holds.

## Approved event and successive edits

Core1324:373–386 permits signed/zero `approved_delta` and uniquely binds operation/item/event.0310:127–154 adds result evidence only to FUNDED/RECOVERED/PAID/SHORTFALL, not APPROVED.1342:259–262 makes event UPDATE/DELETE immutable. Consequently an exact CASE_EDIT approval event with `event_kind='APPROVED'`, command/native component identity and delta new-minus-old (zero for schedule/metadata only) fits the card's literal no-event-beyond-APPROVED rule and permits successive no-financial-activity edits. CORRECTED would make the next edit fail that literal admission unless an extra exception were invented. Do not update/delete the original creation request or original APPROVED event;1536:314–319 uses the original event for creation replay.

FC16 WHOLE_CASE1509:256–291 checks exact Case revision and native protected bindings, including accepted PREPARING input before ACTIVE money exists. Fresh edit must call it under the same original Candidate/control/Case/component lock order, in addition to explicit zero funded/recovered/paid/written-off/active counters and no non-APPROVED event. Map the card's activity/held refusal through the named existing transport rather than weaken FC16.1324:1021–1022 already supplies the ACTIVE Case-hold point index.2013 payment-history index `(case_id,occurred_at_utc,id)` supports Case history; if literal no-non-APPROVED existence must remain a physical point seek after many successive APPROVED edits, a narrow partial `(case_id)` index for non-APPROVED events is the specific missing support, not an all-history count.

## Open periods are real before financial activity

Creator1536:344–359 opens periods immediately: ManualDebt's primary period at input_start_monday has nonzero original due; Credit/payout primary opening uses opening_pay_week_start; unfunded Loan/Advance recovery at input_start_monday has zero outstanding/due. Therefore a legitimate no-activity edit cannot assume no period exists.

0300:135–155 has one mutable current period per `(case_component_id,pay_week_start)`, with native rule FK, revision, original opening outstanding/due and disjoint realised/active counters.0310's immutable fact list286–289 excludes period; this legal mutability is not authority to reprice every old opening. Prepare0904:352–374 accepts only INSERT or the exact original FUNDING_ZERO update, requires expected period revision and five zero operands, then compares the entire actual period with the pure plan's prospective row.

Plan0204:242–276 admits a differing rule only for genuine Loan/Advance funding: same Case/Candidate/kind/subtype/tax/creator age/schedule/threshold/floor, exact FUNDED event/time and consistent original funding identity. Recovery promotion requires five exact zero operands; consumed openings retain old recorded rule/counters. **It does not admit an economic CASE_EDIT.** A general `old rule exists`, current-rule equality, or unchanged counter shortcut would remove this protection. Keeping old opening_due for a newly changed future instalment would under-deliver the edit.

The FC09 design must explicitly identify which unreserved future opening can receive a versioned exact amendment and how the old opening facts remain recorded. It must preserve captured instructions/results, old period revision/rule/opening history and all age/funding/holds. The current schema has no general immutable period-opening revision history. A single slot cannot represent both old and new opening merely by changing current_rule_id. Root's narrow proposal is a preserved exact period-amendment receipt under the named CASE_EDIT provenance: old/new native rule/revision/opening operands, unchanged financial counters and exact component/week identity, with optimistic revision validation. This gives the amended current opening an explicit version while the original opening remains recorded. No delete/reset/new age or general period framework is proposed. In particular preserve the original creation start-Monday zero recovery anchor rather than move/delete it when the schedule start changes. Prior completed zero capture and released reservations must not be silently equated with a never-captured opening; the card's explicit no-activity predicates do not themselves prove that equivalence. The complete bounded treatment of any additional unreserved opened future periods remains a design/source-review requirement.

## Funding lineage must survive after the edit

Original transfer0410:438–454 reads original create only for payout/recovery identities, reads the genuine current rule and, at first funding, appends another immutable rule copying its schedule/threshold/floor/creator age with the FUNDED event. Thus an amendment relation keyed **only** by current rule ID stops matching after normal funding. However the required exact provenance already exists: current rule.original_funding_event_id → typed immutable FUNDED event.instruction_id → immutable payout instruction.rule_id → unique CASE_EDIT.new_rule_id. The rule funding-event composite FK is0300:548–549; event instruction/scope FKs and typed FUNDED shape are512–535; the instruction binds the exact Case/Candidate/rule revision and family/tax at278–280. This is a finite native PK/unique-point chain, not history selection.

DERIVE must positively validate that exact original funding event/time, Case/Candidate/payout-component identity, instruction family/role, predecessor rule ID/revision and complete funded-rule copied operands against the approved amended predecessor. The immutable edit event/request must match that point. Retain the existing original funding evidence guard. FC16 and no-activity admission prevent a new edit from changing the approved Case while that funding slice remains held. **No new rule provenance column, carrying change or0410 successor is required by this path.** This supersedes the preliminary suggestion to add/copy a rule edit-command pointer.

0410:464–483 must keep its original create.input_start_monday point-seek and exact zero/tuple promotion unchanged. During the preactivity edit, version that original zero recovery anchor to the edited unfunded rule through the preserved exact period-amendment receipt; outstanding/due and all financial counters remain exactly zero. Then unchanged0410 sees the copied edited schedule/threshold/floor/age tuple at that original native point and promotes it normally. Actual recovery eligibility still uses the amended rule.next_due_monday (0204:196–202); other pay-week openings are derived with the genuine funded rule. The old anchor date is not used as the amended due date. This closes the demonstrated start-Monday compatibility seam without a transfer/provider/settlement change. Source review must verify exact old/new period evidence and changed-start cases; this investigation is not runtime proof.

## Reader scope and revision closure

0207:605–613 calls the same DERIVE/ALLOCATE plan for current estimate; changing only writable prepare would leave Preview/estimate inconsistent. Its native version list already includes exact rule/period/Case/component versions (325–328) and original creation request (383). A direct immutable amendment reached from the selected rule, or its exact typed original funding event/instruction, stays fixed; Case/component/control revision changes must continue to invalidate the existing context. No new public selector/cursor or money estimate is needed.

1910 Case-list row274–277 reads a due period only when its rule_id equals current component.rule_id; mismatched missing due stays unknown, not zero. Its policy lookup250 remains original create authority and its original-liability reference334–337 remains the immutable creation reference. Component page similarly reads current immutable rule/period. If edit mode permits Comment/Note/reference changes, their effective current readback must use the exact approved amendment pointer, preserving original create history; do not silently display old fields as the amended result or select a latest event. The primary design must state the permitted field set. No full FC02 Case page is built early.

## Source identities and current boundary

| Reviewed native source | SHA256 |
|---|---|
| plan0204 | `2EEAAFB8F14444715C30E13DC898F9B35614DE1F53920344913F2986E09D6B0C` |
| prepare0904 | `FA3C73A83075F67246E62ADEA7BA1467C019BADA601D3DB880EC51B48CDD1A9E` |
| transfer0410 | `F56898DC7E992FC271D2A90C0C7FDB85D77DDE4BAC5E8B852E008C0DA90793D8` |
| FC16 guard1509 | `087C3C5D3AE9CFAA94B66EA58B338358D0236F2B4953BE5EE6ADF7E3DFC60FD4` |

Findings are sufficient to identify the exact kernel gaps. The exact native funding-event/instruction point chain closes the lineage design without another money owner. Root/primary designer must still make the bounded period-amendment receipt and protection predicates concrete before source acceptance. No compile, runtime, application, replay, Draft, funding, screen or timing PASS is claimed.
