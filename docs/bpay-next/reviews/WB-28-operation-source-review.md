# WB-28 frozen operation SOURCE review — OPEN

13 October2026. **OPEN — concrete source findings below require correction before a source PASS.** Root alone authors product. Review covers the new uninstalled0721 repeatable/0725 migration against WB28 card02:1046–1067, R2:252–254, BD175, current0537 CLEAR and current1646 enrollment. This is source inspection only; no SQL/RPC, data, tests, fixtures, capture, network or runtime action.

Initial reviewed complete0721 was3490338838d55b28c6e04c9ca49641423cf7e43b52d233f447a12be937204d65 (includes public W2 and enroller); migration3a67e8b6bb124d6a5890818d16fa5280185786521d43b39792275a9999697838. Root is actively correcting the first findings, so these are historical exact anchors until the revised complete files are reviewed.

## R28-S01 — discovery is not a bounded native physical prefix

0721:265–276 globally SELECT DISTINCTs ACTIVE Work/family rows after Work/current-item joins and semantic filter, then applies outer LIMIT1/order. The new0725:52 (work_id,rate_family,id) partial index does not match CASE-derived family ordering and cannot itself bound pre-filter physical visits. A filter matching no eligible owner can scan/sort all ACTIVE agency history. Current-revision-only predicate269 also skips active dormant/withdrawn roots before recording the boundary's promised exclusions.

Use a statically indexed ACTIVE raw Work/family prefix, advance the raw cursor even for out-of-filter/unavailable groups, and qualify current scope after that finite physical step. Keep exhaustion separate from no qualifying member. With skipped discovery steps, member_no cannot remain freeze_step+1 while APPLY319 assumes member_no=step+1; a maintained contiguous member_count / exact member cursor is required. Root has proposed this exact correction.

## R28-S02 — frozen family exclusion drops permissible unticked owners

283–290 marks a whole Work/family IN_DRAFT when any of its saved owners is selected. A SUBSET with Day protected and Night unselected would omit the otherwise native-qualifiable Night owner. The same predicate omits the original accepted-descriptor selected-component fallback from0537:69, so early accepted SUBSET can instead reach the helper's frozen-root STALE and be labelled CHANGED.

Qualify each actual root owner separately through unchanged0537(owner_id), retain excluded/frozen IDs with per-owner reason, and apply only the complete disclosed permissible same-family closure. Copy the exact positive ALL/SUBSET/accepted-descriptor/current-projection evidence; never turn all STALE into IN_DRAFT or expand a protected owner. Root has proposed this correction.

## R28-S03 — rate-bearing A/R/H evidence is missing

346–358 compares signature/quoted amount, while342 ignores financial_view_revision. In native0537:131–135, position_basis in the signature is populated only for AMOUNT_LED. A rate-bearing manualzero can retain the same quote0 and signature after a realised/reserved change committed by an earlier accepted job, whose execution does not create a new command-clock number. Original frozen economic identity is therefore not proved for every owner.

Freeze the native context.works[].rows[] economic facts for every owner and require unchanged A/R/H/source channel and original units/rates/amount/authority at first use. Do not solve this by re-enabling global FVR equality, which would stale own legitimate publication.

Exact own carry normalization: basis2100:53–61 has no revision_id/approved_line_id and its full tuple must remain equal. It does include source_event_id, which legitimately becomesNULL on an original SOURCE→treatment clone. Normalize that field only through the exact same canonical original approval root on both revisions; do not delete it. Outer approved_line_id and position_applied_revision_id may follow only the exact accepted publication/carried owner tuple. Source UNBOUND→READY/book revision is separate seed bookkeeping: retain an exact page-owned before/after tuple under native locks, or positively reuse original native seed qualification with unchanged frozen A/R/H/channel, canonical root/binding identity and amount. Never blindly ignore Source book/binding fields or reset READY.

## R28-S04 — VIEW parent traversal is unbounded before flattening

155–176 expands all operation members through lateral jsonb_array_elements, applies the tuple cursor only after expansion, sorts and then LIMIT11. The PK(request_id,member_no) and <=100owners per member do not establish the promised bounded pre-expansion parent window. Large immutable operations need a bounded indexed member prefix before lateral expansion, with bounded owner ordinal traversal and a last-emitted exact cursor. No new global operation cap or response drop is acceptable.

The existing110000row-byte /120000outer-response checks prevent an oversized successful reply, but response budgeting must still preserve the last emitted tuple; any known truncation cannot report complete.

## R28-S05 — partial outcomes and confirmation counts are not typed consistently

Result167–176 contains only the original member.exclusion_code / frozen owner data. APPLY372–382 may store CHANGED and refuse owners, yet VIEW never joins page.outcome_code/native_receipt. Users can see PARTIAL/refused_count without identifying which frozen group cleared or refused. Add only an exact bounded PK page outcome join per displayed member; preserve the original disclosure separately from accepted/refused result.

excluded_count304–310 mixes number of NOT_CLEARABLE owners with one per excluded Work/family. Those are different units. scope_summary.candidate_count238–248 comes from HEADER sectionREADY, while discovery271–275 has no corresponding section limit and can include ACTION/WAITING saved roots. It is not the exact affected Candidate count. Maintain/label native distinct Candidate/Work and owner/exclusion counts honestly (existing member indexes permit exact first-occurrence checks), or label the READY header count as such; never present it as the complete Clear scope. Amount totals must include each permitted owner exactly once, never alternative variants or unavailable0.

## R28-S06 — new evidence guard does not bind stage/counter/reset proof

0725:57–67 permits mutable state/counters/amounts/cursors/reset fields but checks only identity immutability and nondecreasing steps/expected_sequence. READY→DISCOVERING, count/amount decreases, arbitrary progression and terminal reset fields without a matching native page are not refused. Operation INSERT and page INSERT lack the corresponding native progression/receipt check. FKs/checks preserve basic shapes but do not establish the claimed monotone recorded operation.

Strengthen only these new private evidence guards with exact stage/checkpoint/counter and page/reset binding, using bounded exact member/page/command/receipt lookups. Terminal result/state/zero-job command/sequence must agree and remain immutable. Do not add a GUC/protocol, unrelated public trigger or historical aggregate.

## Inspected retained guarantees / closed boundary finding

The original outer request/scope/phase/step/cursor shape is closed; unknown absent page readback returns receivedfalse and an accepted exact page body replays before later live locks. NOWAIT operation serialization prevents a second concurrent financial page. One Work/family per financial step preserves the native single-family owner and original32768 input; accepted native CLEAR and operation page commit in the same outer transaction. Frozen members are not rediscovered in APPLY. Exact native receipt sequence expected+1 refuses an external clock interleaving and rolls back the child transaction.

New public W2:405–470 retains the original2320 body except the named action/delegation. New enroll_one:531–547 now handles the exact terminal DECISIONS_CLEAR COMPLETE/zero-member/zero-job native operation+terminalAPPLY receipt, then advances exactly the next enrollment sequence. This **closes R28-D01's missing integration** structurally; every older native branch remains the actual complete1646 predecessor. Security/role/owner/search_path/ACL are retained; new tables have forced owner-only RLS and no browser/service table grants.

No current source PASS or item/runtime acceptance is claimed. Subsequent revised-byte review must close S01–S06 and retain this finding history.

### Complete-parent inverse source comparison

The public W2 successor minus its DECISIONS_CLEAR enum literal/three-line delegate equals the complete2320 parent after LF normalization. The new enroller minus its exact WB28 terminal zero-job branch equals the complete1646 parent after LF normalization. Both comparisons matched nonempty complete function bodies. No blanket historical/source waiver or old branch change is needed. This closes only the parent-preservation/enrollment support point; S01–S06 remain the current operation SOURCE findings.

## Corrected-byte reread — initial S01–S06 partly closed; SOURCE remains OPEN

Re-read the complete new operation result/command and migration guard in corrected drafts0721 SHA100f08806c234c7855443ebf82148f882a27a8b57a4111a815c68c1de7afef81 /0725 SHA203f4b33e3febba5e8fe88efffd708537c13fcf53baaa79c69df63f8aa77e794. Product was still uninstalled and Root was editing; these hashes are the reread checkpoint, not final claims.

S01's global DISTINCT has been replaced with one raw ACTIVE expression-index Work/family seek before filtering; contiguous member_count now separates empty discovery steps from APPLY numbering. S02 now examines every owner, includes the exact accepted-descriptor fallback and retains frozen owners separately. S03 now freezes every native position/basis row, canonicalizes only Source source_event_id through the exact stored approval root, and retains page-owned post-Source book state/revision/binding observations under the original native CLEAR locks. S04 now materializes <=11 indexed parents before bounded lateral expansion. S05 now has distinct Candidate/Work counters and uniform owner exclusions plus a PK page-outcome join. S06 adds operation/member/page INSERT and monotone-stage guards. Those changes are the correct narrow support. The additional failures below remain.

### R28-S07 — first successful READY page fails its own guard; dormant raw group aborts discovery

Corrected0721:453 first updates expected_sequence/cleared counters while the operation is still READY;464 changes state to APPLYING later. Corrected migration73 permits READY→APPLYING/COMPLETE/PARTIAL but rejects READY→READY. Thus the first ordinary successful financial page raises BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE and rolls back Native CLEAR. Combine the state transition/counters with the successful page update or otherwise permit only the exact intermediate recorded transition; do not loosen the guard generically.

Raw discovery now reaches withdrawn/dormant ACTIVE owners, but287 SELECTs current_revision_id INTOSTRICT before any per-owner filter. A legitimate withdrawn Work with current_revision_idNULL causes P0002 and prevents all later scope traversal, including when it is outside the requested Candidate. Positively skip known out-of-filter Work groups first; qualify native last-applied/saved revision labels for in-scope dormant exclusions without treating them as current approved financial owners. The raw cursor must progress, and absence of current approval must never become a zero or disappear from an ALL scope.

### R28-S08 — group receipt falsely marks a protected owner cleared

Result165/174 emits cleared=true whenever its whole member has a native receipt. A member now legitimately contains protected NOT_CLEARABLE and eligible owners together; clearing the eligible owner consequently marks the excluded owner clearedtrue too. Gate this field by the exact owner being in the native page's cleared closure / original RESOLVED_AND_CLEARABLE set. A carried protected sibling stays excluded/unchanged, even though the old future owner version is archived during publication.

### R28-S09 — subject4096 imposes a new smaller native label ceiling

Migration32 limits the whole subject JSON to4096. Existing core1324:174–175 allows candidate_display_name and client_display_name8192bytes each, and0537's existing reply budget supports valid long labels. A single valid long label now fails FREEZE before disclosure. Preserve native label bounds in stored evidence and project/pack rows using the existing overall response budget. VIEW180 must stop before an overflowing next row and retain the last emitted cursor when at least one row fits; it must not make a valid multirow disclosure impossible merely because10 long subjects exceed110000. No semantic truncation or fabricated shortened identity is appropriate.

### R28-S10 — nullable JSON comparisons do not establish exact receipt evidence

New page/operation guard92–94/108–125 uses several nullable JSON<> expressions without required-key/type proof. A malformed object with absent fields can make the IF UNKNOWN and pass. Use required-key/type checks or ISDISTINCTFROM on mandatory fields; terminal command expected_member_count must be null-safe as well.

For a page with native_receipt, additionally point-join the already existing immutable workbench_command_receipt at subrequest_id. Require exact actor/action/module epoch/native_body/result/candidate/Work agreement; a JSON receivedtrue/action/sequence assertion alone does not prove that Native CLEAR accepted that body. This is one existing PK lookup in the new guard, not a new protocol or money-owner rewrite.

Source remainsOPEN pending these exact corrections. The public W2/enroller complete-parent inverse preservation and boundary DESIGN PASS remain valid; no runtime/business approval is implied.

## Final independent reread checkpoint — S01–S10 closed; saved page result remains OPEN

13 October 2026. Independent source reread of the complete saved definitions against the full approved WB-28 card (02_WORKBENCH.md:1046–1067), shared W12–W14 (49–51), R2 WB-28 (252–254), and WB-28-build-boundary.md. This is a saved-source review only. No SQL, business RPC, tests, fixtures, data, rehearsal, network, provision, deployment, public trigger, Git mutation or runtime claim was used. Only this review document was edited by this reviewer. Historical findings above remain the evidence of earlier bytes.

Reviewed checkpoint byte hashes:

| Source | SHA-256 |
| --- | --- |
| New uninstalled 0721 frozen decisions clear repeatable | 2731325502C411CAD74E4B61C3D2B0BB0F31A6D44C44A88225399AD25AE4B83F |
| New uninstalled 0725 frozen decisions clear migration | 6059407B8C3DE01A15EEAEB8EF158391B5AB47AFE0971879E070F112DF600381 |
| Retained native 0537 saved resolution clear owner | 09AEFF09B7E0CA6E1874920E917C987B1F75467CF8E6312D09BF8D8C60D878C8 |
| Retained complete W2 parent 2320 | 1DD72479BA41C1997439F80B8A2546D285F29CD1F791BBDF28DC2FC615385CE2 |
| Retained complete enrollment parent 1646 | FEC5DD533F37C45F26E46011A551009AE8D62FB93A18862E082B2ACF74197456 |

| Finding | Current source status and evidence |
| --- | --- |
| R28-S01 | CLOSED. Raw ACTIVE Work/family is selected before semantic qualification. Initial and later cursor statements are separate; the later tuple predicate is unconditional and matches the new partial expression index. The raw cursor advances on skipped groups; contiguous member_count separately owns APPLY numbering. The 101st owner is a refusal sentinel, never a truncated closure. This is source access-path qualification, not an executed query-plan/performance claim. |
| R28-S02 | CLOSED. Every saved root owner is examined separately. Exact current accepted ALL/SUBSET/component-descriptor protection is copied from native 0537; a protected sibling is retained as an exclusion while permissible unticked siblings remain eligible. Only original root owners in the precise filter are retained; linked native review rows do not widen the operation. |
| R28-S03 | CLOSED. Frozen row facts retain complete native A/R/H/source-channel, basis, available rates and binding authority for every eligible owner. Own revision/carry permits only exact immutable prior page publication plus old ID/version to new ID/version mapping. Canonical original Source approval root retains source_event_id identity; only exact own page-recorded source book state/revision is normalized, with binding and remaining facts unchanged. The first financial-page full native offer context is retained, subsequent owner reviews must match it exactly, and that same context enters unchanged native CLEAR. This closes the cross-owner review-cut race without imposing global frozen FVR equality between pages. |
| R28-S04 | CLOSED. VIEW first materializes at most eleven indexed parents, then bounded owner ordinals, and emits at most ten rows. Byte overflow after an emitted row returns that last exact tuple cursor. No global closure cap, OFFSET or all-history flatten/sort remains. |
| R28-S05 | CLOSED for original discovery/VIEW finding. Exact distinct Candidate/Work counters and uniform owner exclusion counts are maintained; amount sums only original permissible owners. VIEW joins its exact page outcome by PK and retains original disclosure separately. The additional immediate saved-result issue below is R28-S11. |
| R28-S06 | CLOSED for the original stage/reset evidence finding. Only the new private operation/member/page tables have the new guards. Identity/evidence is immutable; checkpoint increments, stages, nondecreasing counters, sealed zero-job reset command and terminal result identity are qualified. Terminal operation and all member/page evidence cannot be rewritten/deleted/truncated. Native accepted pages require the original immutable workbench receipt PK join. No new GUC, certificate, public trigger or money owner was introduced. |
| R28-S07 | CLOSED. First success sets APPLYING together with sequence/cleared counters. Missing current Work approval uses existing applied/saved revision only for retained exclusion labels. Unsupported inactive/channel/Umbrella authority is positively inspected through existing Candidate/Umbrella PK rows and stored NOT_CLEARABLE/UNAVAILABLE; it never invokes financial review or fabricates context. True metadata/admission unavailability still propagates as retryable rather than being falsely finalized. Raw scope traversal continues. |
| R28-S08 | CLOSED. Displayed cleared is true only for original RESOLVED_AND_CLEARABLE owners in an accepted same-family member; protected siblings stay excluded. |
| R28-S09 | CLOSED. Stored subject budget is 40000, preserving the native 8192-byte Candidate and Client labels. Overall disclosure remains row/byte-bounded and resumes at the last emitted tuple. No shortened or invented identity is used. |
| R28-S10 | CLOSED. Required identities and terminal counts use required-key/null-safe comparisons. A native page points to the exact immutable workbench_command_receipt at subrequest_id and matches actor/action/module epoch/Work/Candidate/body/result. Missing JSON is not accepted as proof. |
| R28-D01 | CLOSED. The complete W2 successor minus only the DECISIONS_CLEAR enum/delegate equals nonempty complete 2320 after LF normalization. The complete enroller minus only the terminal DECISIONS_CLEAR zero-job branch equals nonempty complete 1646. The exact next enrollment sequence advances only after COMPLETE/sealed/zero-members/zero-jobs plus operation/reset/terminal APPLY receipt qualification; all older branches remain unchanged. |

### R28-S11 — current page's saved rows lag its accepted counts (OPEN)

At checkpoint 0721:519–523, result_v1 is called before the current APPLY page is inserted. Its exact PK outcome join at 168 therefore sees no current-page receipt/outcome. The immediate immutable result_body can show an increased cleared_count while that same frozen member's visible owners still have cleared=false; a refused member similarly lacks its just-recorded outcome_code. An exact page replay returns those saved rows unchanged. A later VIEW corrects them, but it does not make the originally saved result internally accurate.

Fold only this current bounded member's accepted/refused outcome into the at-most-ten returned rows before saving the response, retaining exact member identity and original clear_class. Alternatively, a narrow pending-page argument to this private disclosure helper can supply the same exact current-page evidence before insertion. Do not mark excluded owners cleared, alter original disclosure, rewrite a saved page, add a new public route or rediscover the scope.

Current whole-operation SOURCE verdict remains OPEN solely for R28-S11 at these bytes. All original S01–S10 and D01 findings are closed as above. A further byte-bound reread must confirm the correction; source review does not authorize installation or assert compiled/runtime/business acceptance. Policy X, immutable original Create/PIN/offer evidence, other actors' memory, snoozes, native Source/money and post-Draft owners remain preserved by the inspected source boundary.

## Final corrected-byte SOURCE PASS — all findings closed

13 October 2026. Re-read the saved R28-S11 correction and its complete containing operation definition, retained disclosure helper, migration guards, native Source/Position qualification and terminal enrollment support. The final source receipt is:

| Source | SHA-256 |
| --- | --- |
| Complete new uninstalled 0721 repeatable | 86216A77B69388669077B294C764BDBB4922A46AEF4286CCF18C5A14833A2CD2 |
| Complete new uninstalled 0725 migration | 6059407B8C3DE01A15EEAEB8EF158391B5AB47AFE0971879E070F112DF600381 |
| Unchanged native 0537 money/clear owner | 09AEFF09B7E0CA6E1874920E917C987B1F75467CF8E6312D09BF8D8C60D878C8 |

**R28-S11 CLOSED.** Final 0721:520–530 folds the current APPLY member's real outcome into only its already bounded returned rows before inserting the immutable page. It preserves ordinal order and every other member's original result. `cleared` is true only with the accepted native receipt and that original owner's RESOLVED_AND_CLEARABLE class. Refused rows receive the current exact outcome_code. Excluded owners remain uncleared; original frozen owner IDs/versions/disclosure stay unchanged. The <=120000 outer response check still precedes page insertion, and the existing real native receipt guard qualifies the accepted journal atomically. No page rewrite, rediscovery, new helper signature, route or money owner was added.

Re-ran both complete nonempty normalized-source inverse comparisons after this correction: public W2 minus only its named DECISIONS_CLEAR enum/delegate equals the full current 2320 parent; enroller minus only its qualified terminal DECISIONS_CLEAR zero-job branch equals the full current 1646 parent. Both matched. The native 0537 file hash remains identical to the investigation/review anchor. Function ACL/owner/search_path boundaries and final PostgREST reload notification are retained.

**Whole frozen-operation SOURCE PASS, zero open source findings at these exact saved bytes.** R28-S01 through R28-S11 and R28-D01 are CLOSED. Earlier OPEN sections remain historical checkpoint evidence and are superseded only by this byte-bound verdict. The operation is still authored/uninstalled, and joined acceptance remains AUTHORED_NOT_RUN. This reviewer executed no SQL/RPC, data/business tests, fixtures, rehearsal, network, provisioning, deployment, Git mutation or runtime/performance proof; only this review document was changed. Compilation, installation, query plans and actual app acceptance were not inferred from source inspection. Policy X and the approved WB-28 boundary remain preserved.
