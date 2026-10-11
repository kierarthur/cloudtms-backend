# SP-20 sequence115 current investigation

Read-only investigation, 11 October 2026, Sol6.1 Extra High. Product, checklist and installed SQL were not changed; no database, business call, test, fixture, browser, hosted or release action was performed. This is an object/scope investigation, not delivery or joined acceptance.

## Controlling scope and explicit conflicts

Read the complete approved card `05_SOURCE_AND_PROTECTED_PAY.md:655–685`, all current checklist rows `IMPLEMENTATION_CHECKLIST.md:3378–3403` (D01–D03 owning, D04 DR38 evidence only), the shared successor/dependency chain, and `09_OWNER_ANSWERS_BINDING.md:11–38`. Dependencies are F05, F06 and completed SP14; this is design-first wiring. Worker/Screen: none. D01 stages existing tax classification without changing Source amounts; D02 removes only answered Source component/channel refusals; D03 keeps one existing target valuation owner and exact native Source-book checks. D04 belongs to scheduled DR38, not an additional SP20 editor.

Master `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:65–66,199,215` preserves F25→WB36→SP14→SP20→DR38→CM13a→CM03→CM10 for1610 and F25→WB36→SP20→DR38→WB29→CM03→CM19 for1620. R5.md:142 / R5.json SP20 R5-D085–D088 require no reimbursement conversion, no invented history/rate fallback and no new Worker/Screen; D088 is `buildTask:false`, a DR38 dependency. These corroborate the bounded owner interpretation and separate future joined acceptance.

The explicit owner instruction of 11 October 2026, supplied by root in this task, controls **SOURCE_FIXED_EXPENSE = NON_TAXABLE reimbursement, no channel conversion, no whole-Candidate REVIEW merely because of that kind**. It is consistent with original economic mapping: `04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql:489–534` maps SOURCE_FIXED_EXPENSE to EXPENSE_CODE/EXPENSES, separately retains pay/charge and aggregates only that original comparison reader's genuine members. SP20 must carry each existing NEXT component separately; do not call that legacy reader, aggregate new Banking identities or add another expense carrier. Its tax treatment follows existing OD03 EXPENSE_CODE→REIMBURSEMENT_GROSS_FIXED→NON_TAXABLE. WORK/Additional remain TAXABLE; Mileage is the existing EXPENSE_CODE MILEAGE, including a Source EXPENSE component already representing it.

**Owner clarification C1, not an unresolved tax-policy conflict:** the card leaves fixed expense to whichever existing classifier supplies it; the actual EXPENSE_CODE mapping above and the latest owner agree on NON_TAXABLE. Log that explicit resolved mapping and latest no-conversion/no-Candidate-REVIEW direction. The old blanket non-WORK refusal is an implementation gap, not a contrary binding tax answer. This does not waive genuinely absent book/evidence, signed/mixed basis, currentness or unknown-kind safety.

**BLOCKED scope/acceptance dependency C2:** the card explicitly assigns NON_TAXABLE valuer bypass in1620 and downstream0130/0200 guards to DR38, yet its own Done requires unchanged mileage/expense and settled cross-channel payment. Current hold calls the taxable channel valuer for every positive non-collection line. These cannot all be claimed complete at SP20 with three guard relaxations alone. Root must record the precise narrow SP20 support decision or keep the delegated DR38 acceptance OPEN/NOT_RUN. In particular, newly permitting a cross-channel reimbursement to reach that unconditional call is not a safe interim implementation. Do not silently advance DR38's transfer/settlement owners.

Earlier WB03 documented Source tax completion as a DR38 handoff (`WB-03-COMPLETION.md:15`); current SP20 D01 explicitly owns Source staging now. Record that concrete owner-chain advancement, while ordinary Additional/expense publisher tax completion remains DR38. F25 producer/resolver/version/latch/certificate protocol rework remains parked. Preserve OR44 original Create cut and SP14's retained individual shift exclusion; no new whole-Candidate query hold, repricing or post-click classification.

## Exact current parents and smallest deltas

The saved final-definer registry, rather than old card filenames, supplies these current owners. Each successor must copy the complete current named definition with its signature/configuration/ACL, preserving all unrelated current behavior.

| Required current signature | Final physical file / exact seam |
| --- | --- |
| `private.bpay_next_stage_source_current_v1(uuid,uuid,uuid)` | `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:1274`; supported native Source inventory kinds1568–1575; approved INSERT1702–1718 |
| `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | `11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql:1134`; Umbrella guard1267–1272; exact binding1281–1299; snapshot comparison1330–1341 |
| `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | `11102026_0559_banking_pay_next_worker_reprepare_v1.sql:1480`; native book1640–1660; unconditional valuation1676–1684 |
| Necessary supporting `private.bpay_next_initial_tsfin_certificate_v1(uuid)` | `07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql:10`; exact staged line/classification comparison100–101 |

Stage already accepts WORKED_TIME, ADDITIONAL_UNIT, EXPENSE and SOURCE_FIXED_EXPENSE and keeps their actual component UUID/key, ordinal, amount, exclusion, unit/rate, schedule and evidence once. It maps WORKED_TIME→WORK, ADDITIONAL_UNIT→ADDITIONAL, other admitted Source expense kinds→EXPENSE. **Actual gap:** only WORKED_TIME writes TAXABLE; all others currently write NULL1708. Reuse the existing finite classification expression (no callable component-classification owner was found in the inspected authority chain); assign TAXABLE to the two taxable kinds and NON_TAXABLE to the admitted expense kinds. Preserve that identity/kind mapping, pay/charge separation, native zero/excluded lines, signed correction semantics and initial/common-HEAD authority. Do not invent a distinct Source Mileage identity or revalue gross.

Capture must remain Source-specific: ordinary unsupported kinds/Umbrella guards are not SP20's relaxation. Remove the source/target-equality restriction only for supported answered Source kinds; independently require exact own kind/tax qualification, not a blanket all-kind pass. Replace literal TAXABLE in the book comparison with the line's own positively qualified treatment. Keep exact Candidate/work/component/revision/line, one publication/no comparison pair, origin, initial root authorisation/physical version/snapshot or common HEAD, approved amount, seeded book revision and both native source-channel R/H counters. An absent or stale genuine binding still refuses. Capture computes no converted money. The SP14 query pin/segment exclusions and every DR05 retry/checkpoint/economic-versus-execution fence remain intact.

Hold keeps the immutable captured run-position and all current position R/H/target comparisons, completed segment attribution, original managed collection and fresh/zero handling. Only the answered Source book comparison changes from hardcoded TAXABLE/same-channel to exact qualified own tax with the permitted target. Taxable conversion remains the **single** current1157 valuer call; never convert again in capture. Reimbursement handling must satisfy C2 before admitting it to that call.

## Concrete supporting assertion and binding evidence

The initial certificate currently compares line tax to “WORKED_TIME→TAXABLE; else NULL” at0018:101. A stage-only correction therefore breaks genuine initial I1 publication/binding with `BPAY_NEXT_INITIAL_SEALED_COMPONENTS_MISMATCH`. Amend this existing certificate's exact classification comparison consistently; keep its real inventory/detail/policy/TSFIN signature/generation/digest and immutable original IDs unchanged. This is a necessary assertion successor, not a new certificate protocol or schema.

Both binding paths already cover **all actual** approved components: common HEAD `bpay_next_source_component_bind_v1(uuid,uuid)`1246:181–196 reads the exact staged line and copies tax; initial `bpay_next_initial_tsfin_bind_v1(uuid)`0018:150–180 iterates the actual sealed components and copies line tax. `bpay_next_bound_source_snapshot_v1(uuid,bigint)`1246:666–687 returns CLASSIFICATION_UNBOUND for NULL tax, then exposes the native stored tax. Thus schema/binder rewiring is not proved necessary; proper stage+certificate classification enables the existing path. Missing actual binding cannot be guessed or bypassed.

Preserve exact stored stage/bind replay before fresh discovery. Do not rewrite previously sealed NULL-tax lines/binding versions or reclassify an accepted Draft. Existing unclassified retained origins, if genuinely present, require an explicit disposition; no assumed absence, backfill, fake history or automatic new origin/event may be introduced to make acceptance pass.

## Shared calculator and later-owner boundaries

`private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)` stays at `06102026_1157_banking_pay_shared_selected_quote_v1.sql:932–1002`. It selects the existing immutable policy window and calls shared selected quote; it has no tax operand, so it cannot independently know an expense is exempt from channel conversion. Preserve its signature and shared calculators. Merely passing manufactured same-channel arguments would disguise the captured source/target identity and is not a documented substitute for the explicit reimbursement branch.

DR38's genuine current simple transfer and settlement parents are `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`: `bpay_next_begin_simple_transfer_v1(uuid,uuid)` guard1778–1787 and `bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)` guard2418–2424 still require WORK/TAXABLE/sourceUMBRELLA/equal source-target money for Umbrella. Those owners must later compare the frozen line with exact hold target amounts for converted taxable lines, and preserve reimbursement fixed source units. SP20 does not edit them or claim their completed transfer acceptance.

The actual Case chain is pure `bpay_next_case_allocation_plan_v1(jsonb)` at1939 (FC09 successor of0204), writer `bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` at2129 (FC14 successor), NET `bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer)` at2129, and post `bpay_next_post_case_transfer_member_v1(uuid,uuid,bigint)` at2129. Original0290 remains the only row kernel. Current prepare still admits PAYE/noVAT and current gross basis2129:589–646; pure plan's retained source-origin checks do not authorise a general mixed-channel rewrite. Record these actual parents for later CM13a/DR38; do not reinstall0360/0549 or broaden SP20 into Case formulas/net/instructions.

Original paid-hours qualification `05102026_1112_bpay_next_source_paid_evidence_page_v1.sql:357` intentionally requires original/source/target PAYE and rejects channel-converted evidence as QUANTITY_MAPPING_UNSUPPORTED420–423; retain truthful unavailable paid-hours wording, not calculated converted hours. Worker/Screen additions, new commands/kinds/routes/tables/triggers/proof envelopes and Source producers are not demonstrated requirements here.

## Fresh saved-source hashes

| File (under supabase/repeatable unless stated) | SHA-256 |
| --- | --- |
| 10102026_1236_banking_pay_next_segment_authority_final_v1.sql | f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d |
| 11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql | b3d7b39a31d35f573dbcc4ea21e9fc0015c801d09a02f1508fe8c9b3ed4535dd |
| 11102026_0559_banking_pay_next_worker_reprepare_v1.sql | 514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6 |
| 07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql | fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2 |
| 06102026_1246_banking_pay_next_source_book_binding_v1.sql | 2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340 |
| 06102026_1157_banking_pay_shared_selected_quote_v1.sql | ad1c7a92754f2937edb642dd5ee387f62761ef241cef77d3b792185b2eb22f3c |
| 09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |
| 11102026_2129_banking_pay_next_case_audit_v1.sql | cdfd284bf1026875c30b8b472719dc31650e5a4f783e50ce9e241024407c84a8 |
| 11102026_1939_banking_pay_next_case_edit_v1.sql | 7b723a61096ac50853dbc609acbba6437b403f11aa796a6f77e39a78b8308f66 |
| docs/bpay-next/final-definer-closures.json | 6d804afb96533069147fe3b4ff3cb2a6b2a50d5157d94c85153d8d2676c66efa |

Root owns implementation design, normal permitted static/schema checks and precise conflict disposition. All Source/Draft/transfer/settlement/app/contention/timing acceptance remains NOT_RUN; source existence and historic ledger evidence are not completed journey proof.

