# DR-13 minimal frozen reservation support

12 October 2026. Supplement to `DR-13-build-investigation.md`, source only. Root requested the **before-this-Draft** snapshot basis; it is not a current NET-time/current Case balance. Root alone writes product. No SQL/business calls, tests, fixtures or hosted actions; all joined acceptance NOT_RUN.

## Existing exact facts and capture position

- Component source counters: `private.bpay_next_case_component.active_recovery_source_ex_vat` and `.active_payout_source_ex_vat`, penny domain, nonnull/nonnegative(core0300:89–90). Case source counters: `private.bpay_next_finance_case.active_recovery_hold_amount` and `.active_payout_hold_amount`, same domain0300:473–474. Do not use `.active_hold_amount`: it combines purposes.
- Parent0408 INSTRUCTION_CAPTURE734–745 locks the exact Case, exact component, then pay-week period before calling the existing DERIVE plan and inserting an immutable instruction. ALLOCATE occurs only after instruction capture. Existing DR16 `private.bpay_next_case_review_outstanding_capture_v1()` in trigger0442:102–132 already reads those exact rows/revisions before INSERT under the parent's locks. This is the smallest supporting owner; no prepare/plan/NET/kernel modification, new business lock or index is needed.
- Instruction already freezes `captured_component_outstanding_source_ex_vat`, `captured_case_outstanding_source_ex_vat`, its source/target channels, captured component/Case revisions and captured_at_utc(migration0442:48–54, current trigger107–124). Existing immutability preserves them, including retained Retry. Historical missing snapshots remain NULL; no history backfill.
- Excluded rows have no instruction/own allocation. `private.bpay_next_case_review_excluded_capture_v1()`0442:133–176 already observes exact native component/Case plus the original/inherited LEAF Rule/control once, stores effective outstanding/Due/observation time on the existing immutable Case selection item, and acquires no extra money lock. Original CASE_PAGE replay0454:372–381 returns before INSERT, so it cannot refresh that observation.

## Purpose and scope

Use the **instruction_kind**, not gross/NET stage, to choose the matching reservation:

| Purpose | Component principal outstanding | Case principal outstanding | Native active reservation |
|---|---|---|---|
| RECOVERY | funded for LOAN/ADVANCE, otherwise approved; minus recovered and written-off | funded for LOAN/ADVANCE, otherwise approved; minus recovered, written-off and paid-credit | component active_recovery / Case active_recovery_hold |
| PAYOUT or CREDIT | approved minus funded, paid-credit and written-off | approved minus funded, paid-credit and written-off | component active_payout / Case active_payout_hold |

These are the existing plan0408:191–222 and metadata0442:107–114 expressions, not another calculator. A NON_TAXABLE credit is still payment/payout purpose despite NET_ADD; a GROSS_ADD credit also uses payout reservation. Loan repayment and its original funding/payout component must not combine both reservation counters.

Let frozen source observations be component principal **CP**, Case principal **FP**, component reserved **CR**, Case reserved **FR**; let **A** be the exact displayed accepted result's `allocated_source_ex_vat`. The honest component principal is CP; component Active reserved before this Draft is CR. Case FR is a separate aggregate constraint across the Case's components, not another amount to add to CR or repeat as per-component principal. Preserve both raw scope operands privately because their limiting side can differ.

For display, the component's unreserved balance before this Draft is `greatest(CP - CR, 0)`; the Case's separate available balance is `greatest(FP - FR, 0)`. Their minimum is the effective balance available to this component. When the Case side is strictly smaller, explain that the remaining-unreserved figure is **limited by the Case's balance**; keep CP and CR visible as component facts. A tie needs no claim about a unique limiting cause. This is balance scope clarification, not the native result's affordability/cap reason or the DR14 proven another-run causal explanation.

Remaining unreserved after this Draft's reservation is the same source-balance capacity arithmetic already used at plan0408:303–312, excluding weekly due/pay affordability:

`least(greatest(CP - CR - A, 0), greatest(FP - FR - A, 0))`.

This is **read-only display metadata**. It changes no principal/recovery/hold and never claims A was settled. Existing Outstanding after stays `least(CP, FP) - A` for its existing source-basis contract; that deliberately differs from remaining unreserved because prior reservations are not paid recovery. Do not replace full principal with weekly nominal, shortfall or capacity. Recoverable this run is the actual target allocation on recovery stages; source A must be used for source balances when target differs. Keep source/target qualification visible.

The existing NET owner already limits surviving recovery capacity to the original DRAFT result's `allocated_source_ex_vat` (2129:1191–1197); the shared row then allocates within that capacity (1199–1202). Thus accepted NET A can reduce this original-basis remainder without claiming that the original CR/FR observations describe later NET-time reservations. There is no new reservation formula or economic owner.

## Smallest storage/reader delta

Selected rows need two nullable source observations on the existing immutable instruction: **component reserved before** and **Case reserved before**. Extend the existing outstanding-capture SELECT to read the matching native purpose counters at the same exact Case/component revisions; reuse captured_at_utc. Pair/null/finite penny constraints must preserve historical all-NULL and no fake0. The existing PK points suffice. Then the existing REVIEW_CASES helper can emit genuine component principal, component active-reserved-before, actual target recoverable, source remaining-unreserved and original result/capture identities without a live Case lookup.

Excluded observation currently stores only `least(CP,FP)`, which loses both individual operands. To keep full component principal truthful, the smallest capture extension can store **component principal**, **component reserved before** and **effective unreserved before** as nullable metadata on the same existing selection item, calculated from that one native point SELECT; no allocation A exists (known unticked, no result). Its original outstanding field/observation timestamp/Rule basis remain intact. Alternatively store the four raw operands, but that is larger than the fields actually needed for this row's display. Do not derive component CR from a min-cap difference or from the AVAILABLE LEAF's0. Historical absent observations remain unknown. The UI must label this excluded observation's time rather than call it the same capture instant as a selected instruction.

The chosen excluded shape is exactly those **three** fields: CP, CR and `least(greatest(CP-CR,0),greatest(FP-FR,0))`, with the existing observation timestamp. The Case-limit clarification follows when the stored effective figure is smaller than `greatest(CP-CR,0)`; no additional FP/FR storage or current Case read is required for this display. Selected support remains exactly **two** matching-purpose reservation fields alongside its existing captured CP/FP/time.

The selected remaining-unreserved figure combines original before-this-Draft CP/FP/CR/FR with the native currently accepted result A. That is an explicitly frozen original-basis explanation, **not** a reconstruction of later NET-time foreign holds or current Case capacity. Later disjoint balance mutations cannot be silently substituted into the readonly reader. No maintained projection, reservation history scan, actor guess, new protocol or public trigger is needed.

## Final implementation boundary

Root's BD135 decision retains the existing instruction capture and excluded-item observation owners above. The subsequent result-insertion exploration is abandoned before product work: no allocation-result capture trigger, old-hold subtraction, transition proof, new result columns or NET owner edit is recommended. Two selected instruction reservation observations plus three excluded observation fields, the named REVIEW_CASES reader/closed contracts and the explanatory UI are the complete proposed supporting delta. All verification and joined acceptance remain NOT_RUN by this investigator.

## WAIT boundary

The genuine accepted WAIT source remains **BLOCKED** as set out in the main investigation: native WB41 informational WAIT is outside accepted LEAF/Create membership, and accepted Rule B rows must not be repriced after withdrawal. Retain typed compatibility interpretation for a genuinely retained marker, but do not claim the current normal capture can produce one. No live lookup, producer or selection expansion is proposed.
