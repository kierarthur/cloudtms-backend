# SP-20 independent boundary and source review

**SOURCE PASS — no actionable remaining finding.** Reviewed 12 October 2026, read-only, against the saved BD127 design and the four actual successor definitions. This approves the current Source-only implementation boundary; it does not claim joined financial acceptance or completion of DR-38/WB-29.

## Reviewed scope

Read the complete SP-20 card (05_SOURCE_AND_PROTECTED_PAY.md:655–685), all four checklist rows, OD03, OR44, and docs/bpay-next/design/SP-20-tax-classification.md. Compared the actual 12102026_0005 successor with the complete four original/current bodies in .codex-local/SP20-parents.json. All four recorded parent hashes match disk; each original body occurs in its named parent and each successor body occurs in the current SQL file.

| Owner | Verdict | Exact supported change |
| --- | --- | --- |
| D01 — stage_source_current_v1 | PASS | Native WORKED_TIME/ADDITIONAL_UNIT become TAXABLE; EXPENSE/SOURCE_FIXED_EXPENSE become NON_TAXABLE. Amounts, component identities, approved vector, exclusions, signs, pay/charge separation and original staging refusals are retained. |
| Necessary initial certificate support | PASS | initial_tsfin_certificate_v1 applies that same finite classification expression to its original sealed-line assertion. Leaving the old NULL expectation would reject the newly classified native components. Its other authority/evidence assertions are retained. |
| D02 — capture_position_page_v1 | PASS | Only genuine SOURCE with common HEAD or the existing initial authorised financial snapshot, known source channel and the closed kind/tax pair bypasses the old blanket Umbrella/same-channel restriction. NULL classification fails closed. The bound book compares the actual captured line's tax. Original canonical identity, binding, current revision, source seed, book balances and SP-14 segment exclusions remain required. No valuation happens here. |
| D03 — hold_fresh_position_page_v1 | PASS | Captured approved-line kind/tax is read by exact line ID/revision/component; a LEFT JOIN retains missing evidence for refusal instead of dropping the position. Bound-book qualification is retained. Taxable Source uses the unchanged single 1157 valuer call; bound NON_TAXABLE Source EXPENSE/MILEAGE retains its original included source amount. |
| D04 — later transfer/settlement context | OUTSIDE CURRENT PRODUCT | General ordinary non-taxable handling and later transfer/settlement guards belong to DR-38; active component resolutions belong to WB-29. No such owner is redefined by this four-function successor. Their joined acceptance remains NOT_RUN/BLOCKED_ON_DR-38. |

## Native classification and valuation boundary

The canonical 2314 mapper identifies EXPENSE and SOURCE_FIXED_EXPENSE as EXPENSE_CODE (fixed expense uses EXPENSES); mileage retains its existing EXPENSE/MILEAGE identity. The successor classifies those existing components; it introduces no component, amount key, aggregate money formula or producer. Source mileage need not be renamed to a new NEXT component kind.

The 1246 common binding writer already stores the actual approved line's tax and classification-line identity. The 0018 initial binder and bound assertion likewise consume and compare the actual line. They require no rewrite. Existing unavailable/NULL classification is not fabricated or repaired in historical sealed lines.

The reimbursement branch is restricted to a non-NULL original Source binding, SOURCE work and captured EXPENSE/MILEAGE NON_TAXABLE. It selects the same immutable winning valuation-policy window as 1157, retains missing/ambiguous-window refusals and checks channel, date, policy and two-decimal inputs. PAYE receives original ex-VAT with zero VAT; Umbrella uses the existing public._pay_umbrella_vat_calc and the captured target VAT-chargeable setting. It never invokes ERNI conversion for that branch. This narrow support is necessary to make the card's Source reimbursement behavior real now; it does not relax generic ordinary classification or implement DR-38.

The line difference census contains only these classification/assertion, finite Source eligibility/book-tax, captured-line lookup and reimbursement-window/helper changes. Original module/epoch, parent/Candidate/job/lease/cursor lock sequence, position witnesses, hold writes, frozen run-line provenance, count checks, zero-line behavior, projection and idempotence statements are retained. This accords with the PostgreSQL review skill's lock-deadlock-prevention rule: no new lock owner or changed acquisition order.

Policy X remains captured classification, and OR44 remains the original Create click cut. No accepted Draft is reclassified from later live tax/settings. Direct no-HEAD PROTECTED_WORK keeps its original restrictions; unsupported Source and ordinary paths receive no blanket exemption. The file defines exactly the four existing private functions with original signatures and closes their browser/service-role ACLs; no schema, public reader, command kind, queue, calculator or screen is added.

## Evidence hashes

SHA-256 of the actual reviewed files:

| File | SHA-256 |
| --- | --- |
| supabase/repeatable/12102026_0005_banking_pay_next_source_tax_classification_v1.sql | df49b9fc467ae9806effb19ec3b39820059fa46980a2f761f62c3f2ef8a96111 |
| .codex-local/SP20-parents.json | 09b9952a93877cbd0b722b04f6876b0efcce2fddcb8038d3d904bfbe39975293 |
| design/SP-20-tax-classification.md | 8e37b38a27fdb022d1e61bb64aac8dc5f8bfc3d57b58c2ff9cc799f3e103c04e |
| repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql | ad1c7a92754f2937edb642dd5ee387f62761ef241cef77d3b792185b2eb22f3c |
| repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql | ad9af3abdac58406d11cddcef287c58a072151b7c4a231f8f363ec134c32216c |
| repeatable/06102026_1246_banking_pay_next_source_book_binding_v1.sql | 2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340 |

Exact original parent hashes: stage1236 f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d; certificate0018 fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2; capture2223 b3d7b39a31d35f573dbcc4ea21e9fc0015c801d09a02f1508fe8c9b3ed4535dd; hold0559 514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6.

Reviewer performed saved-source comparison and hashing only. No SQL/RPC execution, business data, fixtures, tests, hosted calls, screenshots or product edits. Compilation, installation, contract/source gates, financial outcomes and joined application journeys are not independently claimed by this receipt.
