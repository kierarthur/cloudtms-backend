# DR-38 independent design review

12 October 2026. Independent read-only review of BD145, the complete R3 card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:969–1006`, all current DR-38 checklist rows (`IMPLEMENTATION_CHECKLIST.md:3858–3883`), the investigation, binding classification/Rule B directions and current registered parent sources. Root alone authors product. No SQL, business data, tests, fixtures, runtime, hosted calls or product edits were performed. This is a **design/source review**, not installation or joined acceptance.

## Initial verdict and essential correction

The proposed classification, single fresh-hold valuation, reimbursement VAT and independently conserved source/target settlement are consistent with the card. **One required downstream owner is missing from the initial design; complete the amendment below before treating the design as PASS.**

**D38-01 — destination-group WORK admission must receive the same narrow correction.** `private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer)` is current in `11102026_0559_banking_pay_next_worker_reprepare_v1.sql:3592`. Its WORK branch at 3654–3655 requires source PAYE, target PAYE and VAT zero. The current simple builder dispatches this existing destination group at `12102026_1215_banking_pay_next_case_net_replacement_v1.sql:2726`, before its Case-build dispatch at 2733. Therefore a once-converted UMBRELLA-to-PAYE Work line in a Case projection with an external destination still fails even after the proposed ordinary simple and Case WORK-arm corrections. Card 973 explicitly requires both simple and Case downstream admission. Add only the existing destination WORK-arm equivalent, using the exact captured approved-line classification and frozen original line/hold source/target tuple. Preserve target PAYE/VAT zero, original destination routing, Case results, own-leg membership, cursor/count/cash conservation, replay, economic preparation identity, pending-pointer guards and original Run → Candidate → job lock order. No new destination feature or formula is required.

**D38-02 — explicitly qualify the native expense-carrier boundary.** Publisher1001 creates `work_kind='EXPENSE'` and revision source kind EXPENSE for its genuine `EXPENSE_CARRIER` route at 285–310; ordinary hours uses ORDINARY. If the new reimbursement arm tests only ORDINARY, this existing publisher route remains rejected by Umbrella capture and uses taxable conversion in PAYE hold. The design/investigation currently preserve carrier distinctions without stating whether this route is admitted. Resolve the exact card boundary explicitly: either positively qualify this genuine publisher route within ordinary reimbursement support, or document its separate owning-card exclusion. Do not admit unrelated Work kinds merely because a component is named EXPENSE. No new producer or route is needed to make this choice explicit.

## Required construction conditions

- Ordinary publisher1001:700–745 adds only ADDITIONAL TAXABLE and EXPENSE/MILEAGE NON_TAXABLE. Preserve amount/quantity/rate/evidence/count/revision publication, Daily immutable context, nullable Contract and expense-carrier integrity. Historical NULL tax is not silently inferred or backfilled.
- Capture0005:865–912 retains the exact SP20 bound Source arm and Direct no-HEAD PROTECTED_WORK restrictions. Add a positively qualified ordinary four-class arm; unsupported/null classifications remain refused. Rule B chosen revisions/query/channel/Umbrella cuts remain unchanged.
- Fresh hold0005:1311–1382 preserves native current position/book equality. Extend only the genuine reimbursement qualification around the existing immutable policy-window/VAT branch. Reimbursement target ex equals source consumption; unique captured Policy X window supplies VAT, with zero PAYE/non-chargeable VAT. Taxable lines call unchanged1157 once; all native line/hold/position/worker assignments consume that one result.
- Simple transfer1215:2791–2839 and outcome2018:2410–2484 retain original line identities, frozen target equality, source consumption and actual inc-VAT cash/count conservation. Positive lines require their own exact hold; genuine zero lines retain zero/no-hold semantics. No reconversion or source R posting from target money.
- Case transfer0559:3500–3515 and destination group0559:3650–3672 need only the corresponding WORK admission changes. Preserve existing PAYE-source Direct/no-HEAD PROTECTED_WORK acceptance: adding four-class conditions to the new cross-channel arm must not remove that original supported route. Both remain PAYE-target/VAT-zero; finance Case allocations are unchanged.

The existing downstream member guards require original member/worker/cash identity, not WORK source PAYE: `bpay_next_transfer_member_guard_v1()` in0554:23–43, `bpay_next_case_transfer_member_guard_v1()` in0410:159–165 and `bpay_next_record_destination_member_v1(...)` in1827:350–354. No successor for these guards is indicated by this admission change. Current Case posting2129:1618–1625 is deliberately PAYE-to-PAYE/zero-VAT for **CASE** members; outcome2018 handles ordinary **WORK** through the exact ordinary hold. Do not relax the Case-slice currency rule to fix ordinary Work conversion.

Core1324:560–618 already stores independent source/target run-line/hold fields, positive hold amounts, exact original tuple foreign keys and inc=ex+VAT checks. No schema, new money authority, command, public trigger, API or screen is proved necessary. Shared1157 is unchanged. The recorded NON_TAXABLE Workbench estimate mismatch stays the explicit next-touch WB29 handoff; this review does not claim estimate parity. Genuine Source book, OR47, signed/zero and currentness guards remain native.

## Read checkpoint hashes

| Saved file | SHA-256 |
| --- | --- |
| design/DR-38.md | `d841b7c87883014c1786b9aeade9586de9266b2219d8cce85d42a2e4c3d0be7e` |
| design/DR-38-investigation.md | `fd9eca69c5fed26fb008e50346ec197748683dcf15fa0bb9de38962146af7635` |
| Publisher1001 | `b5f2f9ec742a11a88ede05ae0eb433725b410907827bf10300b3540824a92a72` |
| Capture/hold0005 | `df49b9fc467ae9806effb19ec3b39820059fa46980a2f761f62c3f2ef8a96111` |
| Simple/current DR37 owner1215 | `23ac3d72354eac877d66e61e249de0db94f1f820bb7f96a90373c5d5425d72bb` |
| Outcome2018 | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| Case/destination WORK owners0559 | `514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6` |
| Unchanged1157 | `ad1c7a92754f2937edb642dd5ee387f62761ef241cef77d3b792185b2eb22f3c` |
| Case member guard0410 | `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8` |
| Case posting2129 | `cdfd284bf1026875c30b8b472719dc31650e5a4f783e50ce9e241024407c84a8` |

The card's 665/133/798, non-chargeable, opposite-channel, Source-credit, Case-bearing, replay and genuine-zero journeys remain **AUTHORED_NOT_RUN**. Subsequent source review must verify complete current-parent inverse, finite body deltas and unchanged configuration/ACL before Root's permitted normal checks; this note runs none of those checks.

## Reviewed boundary closure

Root's 12 October review response adopts both concrete amendments above. **D38-01 and D38-02 are closed at the design level; amended boundary DESIGN PASS.** The exact construction is seven current function successors: ordinary publisher1001, capture/fresh hold0005, simple builder1215, outcome2018 and the Case/destination WORK arms0559. Shared1157 remains unchanged; no Case allocation, destination feature, schema or new producer is added.

Expense-carrier support is positively qualified by the already captured own revision `source_kind='EXPENSE'` plus non-NULL `financial_snapshot_id`, own `work_kind='EXPENSE'` and component EXPENSE/MILEAGE with NON_TAXABLE. It joins the genuine ORDINARY reimbursement arm; this is not blanket EXPENSE admission. Existing Source qualification stays separate and unchanged. This resolves the named publisher's actual route without inventing provenance or fetching new input.

Both Case/destination WORK guards preserve original PAYE-source acceptance exactly. Only the newly admitted UMBRELLA-source branch requires exact original approved revision/component kind/tax, PAYE target/VAT zero, reimbursement frozen ex=source consumption where applicable, and its own exact ACTIVE hold/source/target tuple. Genuine zero retains zero/no-hold semantics. Existing Direct/no-HEAD PROTECTED_WORK and native destination/Case processing therefore keep their original contract.

This closure records the amended instruction Root sent after the initial saved note. The design file hash in the checkpoint table is the initially read version; it must be refreshed when Root saves these agreed amendments. Product implementation/source/installation and business acceptance remain separate and unreviewed here.

Final saved-note amendment: Root saved and this reviewer reread the agreed seven-owner/positive expense-carrier/retained PAYE branch boundary in `design/DR-38.md`, SHA-256 `0c4a2abd58d66ade91cc0e42dfd46835cb04ab70af2bf5080c1f49edcdab1ee1`. **DESIGN PASS** remains current. Separate actual1419 source review is saved in `reviews/DR-38-sql-review.md`; no runtime/installation claim is added to this design receipt.
