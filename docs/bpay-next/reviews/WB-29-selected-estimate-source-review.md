# WB-29 selected estimate source review

SOURCE PASS — current selected estimate slice; 0 open findings. E01 closed in final saved source.

Read-only comparison of authored, not installed `13102026_0055_banking_pay_next_selected_treatment_estimate_v1.sql` with the public workbench reader body in 2241 and approved `WB-29-estimate-native-selected-join.md`. No product change, SQL/RPC, database, fixture, test, browser or business action by this reviewer.

## E01 — closed: retain actual current/applied equality for every bound Source publication Work

At 0055:617–626, both original publication currentness predicates have become canonical-root comparisons. A companion Source Work can have a sealed treatment current revision and a different still-applied prior revision, with both mapping to the same original approval; this replacement counts the original APPLIED/DONE publication anyway. The treatment approval helper at 2135 native-publication:17–30 proves a genuine sealed POSITION_APPLY publication/command and unchanged approval root, but does not require its publication to be APPLIED/DONE. The selected component's actual Work/Position checks at 0055:469–473 protect that selected Work, not every companion in the one/two-member Source binding.

Final saved 0055:620 adds `w.current_revision_id IS NOT DISTINCT FROM w.applied_revision_id` before both canonical original publication comparisons. The roots still have to equal the exact bound original publication revision. NULL roots cannot satisfy that publication proof. This closes E01 without replacing the original binding/publication identities or relaxing selected expected/current/Position/FVR predicates.

## Final source checks

The full public function was compared with its 2241 parent. The successor defines only that existing public function; private definitions preceding it in the parent file remain with their existing owners. Unrelated request kinds, Source/Case/descriptor qualification and public ACL/error envelope are unchanged.

The original descriptor choices-to-LEAF join at273–281 is unchanged. Exact actual leaf keys are retained at465 and passed to the fresh 0045 facade at678–679, grouped by native Work/revision/component/approved-line. Existing expected/current/applied Work and Position, WAIT/hold, FVR and projection checks remain before quotation. No synthetic RunPosition, accepted ref, browser money, new route or response kind is added.

Original Source binding/approval-line/receipt-publication operands now use only the genuine original approval root. Actual selected revisions/leaf identities/current-applied Position remain current; E01 restores equality for companion publication Work as well. Exact initial root authorisation/financial snapshot/native command-job phases and one/two original publication-member proof remain. Original Source basis/seed/native PAYE-or-Umbrella scalar equality and READY book checks remain; target Candidate channel is separately qualified instead of incorrectly equated with Source basis. Classification is joined to the original line and must equal the actual current component kind/tax treatment. No Source producer/binder/seed or financial principal formula is changed.

The new fixed-reimbursement branch is limited to positively qualified ORDINARY/SOURCE or native EXPENSE-carrier EXPENSE/MILEAGE with NON_TAXABLE classification. It retains Source ex once, selects the same winning immutable policy window/ambiguity guard as0005, uses the existing Umbrella VAT calculator, and supplies PAYE VAT0. WorkRevision's Source channel is constrained non-null PAYE/UMBRELLA by native core schema. Taxable WORK/PROTECTED_WORK/ADDITIONAL/ADJUSTMENT alone enters the fresh0045 quote; a proven absent saved quote retains the original valuer. Returned Included and policy must equal native grouped Source and revision policy. Other classification is unavailable. No per-bucket target calculator or inference is introduced.

Consumed RATE_DECISION tuples from the actual quote are collected into bounded `jsonb[]` using array_cat. The final native merge uses UNION ALL/unnest, distinct and original text order; no opaque monetary JSON concatenation or gate waiver is required. Early comparison removes only RATE_DECISION tuples so base dependencies are checked first. The complete merged tuple set is then compared at709–710 before native Case allocation and any monetary response; change/absence returns OUTDATED with original null money. Initial, continuation and verify-only requests share this check. Both the consumed tuple accumulator and merged pre-dedup count retain the8,000 limit; no dependency is truncated.

Source and target are summed once per component. Original Case ALLOCATE/order/floor/weekly/WAIT owners, original1,000 membership/leaf and contribution bounds, URL/context32768 and response120000 budgets, and PENDING exception paths remain. The updated estimate integration adds no financial write, execution or historical recount. Shared0045 quote economics/census correctness remains covered by its separate source receipt; this review covers its actual selected-reader call and native returned operands.

Final source review only. Joined/runtime/business acceptance and future selected Amount/sort joins remain NOT_RUN/unapproved here. No test or source-gate execution was performed by this reviewer.

| Source | SHA-256 final checkpoint |
| --- | --- |
| supabase/repeatable/13102026_0055_banking_pay_next_selected_treatment_estimate_v1.sql | 373FC87826C62B88524504B9F3C673A79E75CAC1C55D4746BC6CE3D3B39C4D6A |
| supabase/repeatable/12102026_2241_banking_pay_next_resolution_context_v1.sql | 098EF7EE8E20B889B85D9702A374CB381A172D07CB47692E56FE20F7CD9D650E |
| docs/bpay-next/design/WB-29-estimate-native-selected-join.md | E94C6A4F491440118B6741810F4512D13CA87CCF348DB35D414BDA6DB18187B2 |
| supabase/repeatable/13102026_0045_banking_pay_next_current_selected_resolution_quote_v1.sql | 6488C85F18850119D0A64812E3420C7AAC765A4801924AF737E08D9BEA39960B |
| supabase/repeatable/13102026_0005_banking_pay_next_captured_resolution_hold_v1.sql | 5DD92083243DDE0BD41418DA6436D28D1566C32C1B0A0A540D9A8D4AE742F66A |
| supabase/repeatable/12102026_2135_banking_pay_next_treatment_native_publication_v1.sql | 56D91E37EDE376018C72245EE02FD1D30237F987C656754B2FD0EC668582DDC1 |
