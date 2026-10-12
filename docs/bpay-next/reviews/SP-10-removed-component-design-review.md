# SP-10 removed-component design review

12 October2026. Independent read-only check of the actual saved `design/SP-10-partial-removal-design.md` (the requested removed-component design is saved under that filename), current observation1849, Work core1858, immutable Source binding1246 and segment/source authority1236. Root alone writes product. No execution, SQL/RPC, data, fixtures, tests, renders or hosted calls. Design/source scope only; joined acceptance NOT_RUN.

## Initial essential finding — R10-D01 OPEN

The proposed predicate2 requires current and original `detail_kind` equality, and the note preserves observation1849:93 unchanged. Native Source staging in `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:1605–1606` sets SHIFT when at least one dated WORKED_TIME component exists and FIXED when none exists. A genuine accepted singleton removal of the last WORKED_TIME component, with an Additional/Expense sibling still present, therefore changes SHIFT to FIXED and is not a certified whole-zero revision. The design's representation equality rejects the positive REMOVED proof; if that equality alone were removed, observation1849:93's `(current detail SHIFT OR certified_zero)` would still reject the retained segmented paid balance.

Smallest required amendment: use the same fully qualified REMOVED Source singleton fact as an exception to only those two whole-revision representation tests. Preserve the original paid WORK/SHIFT classification, exact original Source UUID/key/channel/tax, exact current binding/version/accepted-head/publication/currentness and native Position approved0. Preserve every existing segment source/target/Case conservation test and amount formula. This is a removed-component proof, not arbitrary cross-representation approval, generic missing-key zero, Source producer or reconciler change. Reported immediately to root and investigator.

## Scope findings already supported by native source

The helper-only correction is necessary and appropriately narrow. Binding1246:197–221 explicitly produces immutable REMOVED only for accepted membership with the exact current approved line absent and retained original classification; :267–281 stores the complete receipt/approval/publication identity. Bound snapshot1246:669–676 supplies zero specifically from that REMOVED fact, rather than generic absence. Existing observation1849:54–72 already requires the exact singleton COMMITTED bundle / COMMITTED_CURRENT head / live root authorization / receipt digest / own current publication. These native facts can qualify a read-only completeness alternative without invoking the locking bound snapshot from a STABLE helper.

The retained classification UUID should not be forced to equal the first paid anchor. Binding1246:206 retains the prior accepted classification, and :215–216 can bind the immediate predecessor classification for first adoption. A later same-component line is legitimate; require its immutable approved-line/revision ownership, original Source UUID/key, native component kind, null-safe matching tax and source channel instead. Current original paid-basis identity must still qualify independently. Nullable native tax may remain NULL for a known manual observation; no automatic classification or payroll stage is inferred. Unmatched/moved/two-publication/cross-Work/unbound classifications remain incomplete.

No new table, command, producer, public trigger, history scan, financial formula or future collection feature is justified. Existing native manual reconciliation and1858 display remain the consumers. Exact actual successor source review is pending after the design amendment; this initial receipt is not a product PASS.

**Native APPLYING timing clarification.** Predicate1's “current/applied revision equal p_revision_id” must preserve Work.current plus Position.applied equality, rather than introduce Work.applied equality. Current `12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql:200/249–253` calls reconciliation after the Position's new revision is written, while Work.applied remains the predecessor until :305. Observation1849:26–28 deliberately supports this step. Requiring the Work's terminal applied pointer too would miss the new observation at its existing reconciler call. Keep the current singleton publication QUEUED/APPLYING/APPLIED states and all existing owner/job protections; no new release or re-observation call is justified. Reported as a concrete design clarification before authoring.

## Final independent design check — PASS

Fresh saved design closes R10-D01 explicitly: predicate2 removes whole-revision detail equality only for the fully qualified `v_removed` proof, and the paragraph after predicate6 permits that same proof in the segmented representation gate only when the original paid line is WORK and original detail is SHIFT. All other completeness alternatives, n>0, nonnegative values, exact source/actual-target/Case conservation and formulas remain unchanged. The initial OPEN finding above is superseded. The timing clarification above is a required preservation of the existing helper prerequisite, not an additional Work.applied terminal gate.

Predicate4's null-safe classification treatment is supported by native binding schema `09102026_2010_banking_pay_next_source_book_binding.sql:112` and original observation1849:46–49. Matching NULL tax does not prevent display of a proved manual nominal amount; it continues to prevent automatic tax/stage qualification, and current1858:349/:354 retains NULL rather than inventing a stage or tax. The final design's “unqualified tax” caveat means unmatched or unbound classification, not the deliberately retained matching NULL case.

The correction is one new complete successor of `private.bpay_next_work_collection_observation_v1(uuid,text,uuid)` with its original STABLE SECURITY INVOKER configuration and ACL. No successor of1858 core,1246 binding,1236 segment money basis, reconciler or Source producer is required. Positively qualified singleton REMOVED authority can establish complete approved0 for its original typed component; it cannot establish zero from generic absence, old history, withdrawn authority, moved/two-book membership or an unaccepted later Final. Known manual liability reuses the same native ledger and existing visible informational row; there is no second debt or automatic ownership expansion.

This verdict checks design only. The root-authored successor needs its own independent actual-source review. S3's separate factual-enqueue conflict remains BLOCKED, and every joined/business first-use acceptance is NOT_RUN.

### Reviewed SHA-256

| Saved file | SHA-256 |
|---|---|
| `design/SP-10-partial-removal-design.md` | `274e403ce62709994bffb09b0b1bccf13bc3dfb91e9f83a9bb5769626284cb58` |
| Observation parent1849 | `de67314666b76892ef6b4ac25554fa44e7f7a89a96068eabe6b7ea9a3b79a830` |
| Work core1858 | `455e61cb1df71f04dd3394a4b03e3320f9ccdaecf1c4d32b096e6185a24d20ff` |
| Native binding1246 | `2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340` |
| Segment/source authority1236 | `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d` |

Final prose refinement independently re-read: predicate1 now explicitly binds Work.current plus Position.applied while preserving predecessor Work.applied during native APPLYING. The final tax caveat explicitly distinguishes unmatched/unbound classification from genuine matching NULL tax. These close the wording clarifications above; design PASS remains against the current hash in this table.
