# FC-15 first SQL source review

Status: OPEN — independent read-only review of the uninstalled working migration/repeatable `12102026_1822`; no business SQL, compiler, tests, data, fixtures, hosted calls or product edits. Root is still authoring visibility and ACL/footer. This is not a whole-card PASS. Manual collection is expressly permitted by the owner; all-origin automatic collection is not a completion requirement.

## Essential finding F15-S01 — segmented observation references nonexistent result columns

`12102026_1822_banking_pay_next_classified_work_collection_v1.sql:76–80` selects `g.realised_target_ex_vat`, `g.realised_target_vat`, `g.realised_target_inc_vat`, and `g.active_held_target_*` from `private.bpay_next_work_segment_gross_balance_v1`. Its current installed parent (`10102026_1236_banking_pay_next_segment_authority_final_v1.sql:2678–2688`) returns only realised source and active-held source. A segmented native observation therefore fails when that query runs.

Smallest correction: read the already-native target effect-share totals and ACTIVE origin-share target totals for the exact Work/component (the existing members helper demonstrates these inputs at `10102026_1321_banking_pay_next_workbench_removed_source_classification_v1.sql:75–81`) and compare those sums with Position alongside the existing source debt-basis totals. Do not extend the balance helper's signature or calculate gross-to-net proportions.

## Pending source work

## Essential finding F15-S02 — unintended alias predicate escaped the origin lookup (closed)

The working transformer initially inserted `rule.tax_treatment=b.original_tax_treatment` into the unrelated allocation-plan `IF v_rule.weekly_due_source_ex_vat IS NULL` branch, where neither SQL alias exists. This was reported before installation. Fresh saved source now restores the original `IF v_rule.weekly_due_source_ex_vat IS NULL THEN` at line 1074. The tax comparison remains confined to the intended origin lookup.

Fresh-source closure of F15-S01: lines 76–87 now read actual native target effect-share and ACTIVE origin-share target totals per segment, rather than nonexistent balance-helper columns. The actual source and target totals remain compared with the conserved Position tuple. These two reported defects are closed in the current working bytes; remaining review is still OPEN.

## Essential finding F15-S03 — existing Expense carrier cannot retain its manual obligation

The collection guard at current lines 594–603 has ordinary and Source arms, but no actual native Expense carrier arm. The current publisher `12102026_1419_banking_pay_next_ordinary_umbrella_components_v1.sql:287,309` creates `work_kind=EXPENSE`, captured `source_kind=EXPENSE`, and the genuine financial snapshot; its capture at lines 1377–1379 already admits the classified NON_TAXABLE EXPENSE/MILEAGE route. The new reconciler attempts a MANUAL insert for that existing route, which the guard rejects with `BPAY_NEXT_WORK_COLLECTION_ORIGINAL_BASIS_INVALID`. This aborts the owner transaction instead of retaining the obligation. Add only the positively qualified existing carrier identity/kind/tax/snapshot arm needed for manual retention; this finding does not require broader automatic collection.

## Essential finding F15-S04 — non-taxable Origin WAIT fails the retained allocation annotation

The Case classifier now admits a NON_TAXABLE automatic OVERPAYMENT with `NET_DEDUCT` (current lines 1507–1508), but the allocation-plan zero-WAIT annotation at lines 1350–1354 still requires only `GROSS_DEDUCT/GROSS_RECOVERY`. The new reimbursement instruction therefore fails its annotation after the original allocation kernel. Admit the exact tax-qualified `NET_DEDUCT/NET_RECOVERY_CAPACITY` tuple alongside the retained taxable tuple. Preserve all zero source/target/taken and unchanged headroom checks; no allocation formula change is required.

Fresh correction: the saved annotation now explicitly requires a non-NULL TAXABLE/NON_TAXABLE treatment and its exact stage/purpose pair, with the original zero and headroom checks retained. F15-S04 is closed. F15-S03 paid admission is corrected, but both origin lookups must also include Expense in their ordinary `source_component_id` exception: native Expense carrier approved lines have no Source component ID, so ordinary `NULL=NULL` equality cannot qualify them. The reconciler already has that correct two-kind exception.

## Essential finding F15-S05 — exact existing Source automatic authority is demoted

The original `11102026_0814` reconciler admits its strict Source/protected WORK paid origin (lines 987 onward) and proceeds with its READY collection (line 1134). The new mode expression additionally requires ordinary/Expense Work, so that same already-qualified Source origin becomes MANUAL. BD149 explicitly retains this existing Source authority. Preserve that original strict READY branch; this is not a request to implement the later broader Source automatic owner.

## Essential finding F15-S06 — withdrawal hides or erases retained manual observation

The observation helper refuses WITHDRAWN approval. A subsequent native reconciliation can therefore replace a previously known manual observation with NULL/AMOUNT_UNBOUND, although withdrawal has not changed its authorised Position basis. The new Work display also restricts its independent manual rows to APPROVED Work, while the withdrawal branch shows only withdrawal/reauthorisation. A known unlinked manual liability consequently disappears from this presentation on withdrawal. Retain the prior qualified liability and present it once as informational manual collection, keeping withdrawal Evidence and all positive-offer prohibitions. For a linked Case, changes to actual recovery/write-off/held amounts must continue to use its exact native R/W/H owner.

## Required migration audit

Card D06 requires replacing the two tax CHECKs by audited expression, rather than relying on generated constraint names. The current migration drops `bpay_next_work_collection_check1/check2` directly. The original `04102026_1519` definitions at lines 48–49 prove the intended two expressions. Bind the replacement to those audited definitions (with a fail-closed exact two-expression census); do not broaden the drop or guess another generated suffix.

### Locked migration boundary — D06 remains BLOCKED

Root reports that migration 1822 was hash-locked before this review and remains uninstalled. `docs/DATABASE_RELEASE_BIBLE.md:15,48–49` expressly prohibits editing or re-locking that existing migration. The prior correction advice must not be read as permission to edit locked bytes.

The full card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1357` requires the old constraints to be “found by their audited expression as 0380:62–81 does (never by a generated name).” Root's fresh local metadata observation reportedly proves that the two named targets still have the exact original expressions, and that the new helper/column is absent. An exact source-gate comparison plus that evidence can prove the correctness of these targets on this catalogue, but cannot turn the locked migration's literal name-based drops into expression discovery. These authorities cannot both be satisfied by editing or silently declaring the existing method equivalent. Preserve the locked migration and mark D06 explicitly BLOCKED; retain the metadata/source audit as evidence, without an invented release wrapper, ledger alteration, or immutable-file bypass. No independent database observation was performed by this reviewer. Any acceptance of the audited-name method requires an explicit owner scope amendment rather than an inferred exemption.

## Essential finding F15-S07 — manual informational portion borrows accepted money

The recovery portion inherits `is_selected/selected_source_ex_vat` from the original accepted Work lookup. Its new MANUAL annotation does not clear those informational-row flags. Group display gives selected money precedence over nominal money, then sums that value for MANUAL_COLLECTION. For example C=100, A=150, a captured next Draft=50 with H=50, followed by a genuinely authorised A=80, leaves a manual liability of 20 but makes the manual portion display/sum the accepted 50; the RESERVED portion still retains 50 separately. Clear selected flags/selected amount only on the new manual informational recovery/reference portion, preserving the original saved `v_base` and RESERVED/accepted offer. This is display identity repair, not repricing or membership mutation.

## Visibility still being authored

An unlinked MANUAL/AMOUNT_UNBOUND record also needs explicit manual-unavailable wording on its existing component Evidence; the current independent fallback requires a positive known manual amount, while a native unavailable member remains generic Evidence. Do not manufacture an amount or certify its positive offer. Root is still writing this supporting presentation slice, so this remains an explicit incomplete seam rather than a claim that the whole-card implementation is final.

## Source facts retained by this draft

Static whole-statement comparison against the exact named current parents confirms that expiry scope and worker-release scope retain `11102026_0559`, and discard scope retains `11102026_0814`, with only the two explicit tax and stage/purpose predicate extensions. All native command/request/run/worker/lease/epoch/cursor/hold/capacity/released-state qualifiers remain byte-identical after newline normalization.

The principal update continues to add recovered and written-off history to the remaining outstanding, floors that outstanding at the native active recovery hold, and updates the same Case/component identity. The original same-job return precedes observation/refresh. The added positive paid-anchor seek is identity/provenance, not a historical amount sum; Position and a complete native segment basis remain the amount operands. The original shared allocation row function and the existing `10102026_0316` source=target/VAT-zero recovery writer are reused. The latter has no taxable-only stage restriction requiring another monetary owner.

The current pure plan retains its FC12 period/funding branches and FC13 accepted-resolution dependency fence; the gross-only NOT_AFFECTED exemption remains gross-only. The helper and all successors have the original private owner-only revoke pattern; no public RPC or Source producer was added. Native Work-before-Case locks are acquired under the original Candidate owner transaction. These source findings do not prove business execution or acceptance.

## Working source snapshot

- Repeatable `12102026_1822_banking_pay_next_classified_work_collection_v1.sql`: `8a7710e45be369eaa1b529784360885d79866b7cc107d5ff9537278a6179d73d`.
- Migration `12102026_1822_banking_pay_next_work_collection_non_work.sql`: `a7563bc0c03e983a0de2b502946bedfe1c874331805bd5e94f8aebb872cbc00c`.
- Design `FC-15.md`: `8f7c97ac4553912efdd5773f6da04e54b8a4071f8173f0b3868fc190c66a1fd7`.
- Exact-parent manifest `FC15-parents.json`: `11b77adfec59033abb6f68f5d30ac59978d29ae6fb836fae033620984f743c42`.

This is an interim snapshot while root is still changing its own product. Findings S05–S07 and the migration audit need closure; S03 has its remaining lookup exception noted above, and S01/S02/S04 are closed. No build, install, business runtime or app-test verdict is claimed.

## Fresh working-source amendment

At repeatable SHA `f759af4f6cd52740c4fe2a02debf3044d7dc8dc27e6fff88056b37b714e1cec4`, S03 is closed: both exact origin lookups now have the ordinary/Expense exception for genuine non-Source component identity, beside the independently qualified native carrier arm. S05 is closed: AUTOMATIC mode again follows original READY/PAYE authority without the extra Work-kind restriction. S07 is closed: only the manual informational recovery/reference row clears its selected flags, selected amount and accepted address; the original `v_base` remains the source for RESERVED/accepted presentation. S06 and the unbound-Evidence visibility seam remain under root's active correction. Migration SHA remains `a7563bc0c03e983a0de2b502946bedfe1c874331805bd5e94f8aebb872cbc00c`; preserve its locked bytes and the explicit D06 barrier above.

Reconciler admission/replay/manual transitions, original R/W/H conservation, migration expression audit, named cleanup/plan gates and retained FC12/FC13/frozen consumers remain under inspection. Missing product footer/visibility is unfinished integration rather than a finding about an otherwise completed card.
