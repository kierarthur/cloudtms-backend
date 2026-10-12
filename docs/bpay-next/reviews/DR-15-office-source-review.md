# DR-15 Office and transport source review

12 October2026. Independent read-only review of D02/D04/D05/D07, BD132, current0307 preparation reader/manifest, BE preparation transport and native FE Office/main/CSS/index integration. No product edits, tests, SQL/database/business/hosted calls, fixtures or screen generation. Joined/runtime/timing/A4 acceptance NOT_RUN. Root owns product and all permitted checks.

## Initial source finding — OPEN

**O01 — null-cursor backward paging can merge different native contexts.** Office `preparationIssuePages.read` initially1723 compares old/new context only when `after!==null`, but1724 retains old pages when `direction==='back'`. After four pages, the first page is evicted with its original null input cursor in history. Restoring it with `read(null,'back')` skips the equality guard; if the native run/selection/Workbench context changed, a fresh first page can be mixed with older retained pages. This affects both Candidate review and Not-in-Draft because they share this owner.

Minimum correction: compare contexts whenever a read preserves old pages, including backward reads with null cursor; an explicit ordinary refresh may still reset to the new context. Reported to root immediately. No code was changed by this reviewer. Current product is still being refined; this initial receipt is not SOURCE PASS.

## Source-supported behavior already inspected

- BE preparation:14–54 and FE Office:205–246 have the same closed request/context/row/cursor/count shapes, including selection_pages/selected_candidates, nullable native worker/revision/physical identity, unknown-original label provenance, finite syntactic issue codes and the exact six SYSTEM-release reasons. The Worker parser/result path uses PREPARE_ISSUES only at the existing GET preparation-page route; cursor is JSON, unknown keys/reflected actor/run/view/sort/row identity and malformed nullable tuples refuse. No financial command or wake is introduced by this reader.
- Current0307:16–56 uses plain service-role/admin/NEXT checks before the locking general authority; native original epoch and exact context qualify the read. Rows/counts come from retained accepted skip/page/request/current worker facts.0307:165–191 verifies actual native tail/value, checks the fresh context and admits only the finite byte prefix/cursor. This review does not supersede the separate SQL-owner review or infer a runtime bound from those source checks.
- Office1719–1728 limits resident page rows to three100-row pages; the bounded history contains only native cursors. Known reasons come from the finite manifest; well-shaped unknowns remain visible with the Contact support fallback. Whole-scope server skipped/removed counts are used directly, distinct from resident row count.
- Exact Timesheet navigation at Office1740–1749 re-reads the native row by row_key, verifies original IDs/revision/issue tuple and actor/epoch, then calls the existing reviewTimesheet adapter. Main7182–7204 validates native provenance and passes exact physical ID/version to the existing native opener. It does not search CURRENT_WORK or substitute a newer Work approval. The existing physical-version refusal remains authoritative.
- Candidate REVIEW actions retain the original DR05 controller/mutation journal and expected revision; PF08_NEEDS_ATTENTION navigates to the existing qualified progress owner rather than issuing WORKER_REPREPARE. No-worker PENDING/FAILED rows are displayed without invented workers/actions. Completed omissions are informational; the progress omission-read error is separate from economic preparation/confirmation errors.

Further focused review and final source hashes will be appended after root's correction/current bytes stabilize. Historical findings remain below a later explicit final verdict.

## Current final verdict — SOURCE PASS

Fresh current bytes below supersede the initial OPEN checkpoint. **O01 is closed; no remaining essential transport/native-flow defect was identified in this source review.** Office1723 now compares context for `after!==null OR direction==='back'`, while ordinary explicit refresh resets its bounded pages. Null-cursor first-page recovery cannot combine different native contexts. The original finding remains above as history.

Current BE/FE finite contracts match semantically, including the two added native context counters, strict nullable worker/physical tuples and captured/current/unknown-original label origins. Static comparison of all33 manifest definitions against the Office table found zero review/omitted copy differences. Unknown codes are admitted by finite syntax and display the visible one-sentence support fallback; neither spelling of an old hold diagnostic was added as a new capture code.

The active native Draft reads PREPARE_ISSUES directly for real PREPARING/PENDING/FAILED rows instead of delegating to restrictive2359 WORKERS. Rows without workers get no economic action. Current REVIEW workers keep original controller-bound DR05 retry bytes/revision; PF08 still opens the qualified existing progress retry. The omission information line uses server whole-scope skipped-Timesheet/removed-Candidate counts. Its separate progress read failure never becomes a preparation error or changes Confirm eligibility; Not-in-Draft actions only read/open an exact existing Timesheet.

Navigation is qualified again at Office1740–1749 before main's exact captured-origin branch. Actual main7231–7242 provides both native adapters and `ownerReady` bound to the original NEXT epoch. Existing main148926–148932 refuses a different physical Timesheet ID/version instead of following a newer row; same-frame/child closure ownership remains intact. Candidate review retains current actor/namespace/owner invalidation and original mutation journals. No new route, command, financial step, pricing authority, projection table or all-page browser inventory was introduced by this batch.

The same reader endpoint/current0307 interface has finite input/row/cursor/reply packing and coherent native context, including the mutable legacy OPEN-page counters. Public service-only execute and read-only early branch remain source-visible. Separate `DR-15-reader-review.md` controls the full SQL-owner body/inverse/source-gate assessment; this receipt does not invent database execution evidence. CSS/index changes are scoped native presentation/cache-version support. Actual pixel/policy comparison belongs the screen reviewer.

| Current saved input | SHA256 |
|---|---|
| BE preparation transport | `00F21FD6DB791E3F2990344EDE3FD0EEF2A417FA60120792F67A99F196CB7A48` |
| Current0307 SQL interface | `7C83ED8EBEDC42F63F97320DB565904FDD33E4F26E298ED96592076D0EAA9274` |
| D02 manifest | `EAB3266BC7E92B353028853DFE2718AF1AEAE40CA24F339612E331D393C0B23C` |
| FE Office | `AE15C01E492C0BC6B9F9985213F055B78AAF78F333C9BEED552A85D038172F13` |
| FE main native adapter | `69D84496624EF0F3AEBFB204E1E81CD7EBD824A793BC130A8379D4E7A0992F5B` |
| FE native CSS | `74C6D84CB16163EDC8227BBDA6B18654E055C38CF83F6098EC9750CE9E547AD8` |
| FE index | `9252D255DBA51987FC978FB097F8ED59A571BF69537D3C93D0616ECCF35C64CD` |

SOURCE PASS applies to these reviewed bytes and the exact D02/D04/D05/D07 integration. No tests, compile checks, runtime/joined/queue/business/timing/hosted actions or A4 capture were run by this reviewer; those remain NOT_RUN here. It is not whole-item completion or release authority.

## Final0332 / presentation amendment — SOURCE PASS

Fresh source re-read supersedes the prior Office/CSS/SQL-interface hashes below. **No remaining essential source defect was found in these final reviewed bytes.** The finite typed Worker/FE contract and main's captured navigation adapter remain unchanged. Registry now names `12102026_0332_banking_pay_next_preparation_issue_payment_sort_v1.sql` for the public preparation reader. Static0307→0332 diff contains only its header comment and one Payment sort operand: captured reference displays/sorts as `Timesheet ` plus the reference; no-original-Work displays/sorts as Payments and unknown-reference Work as Timesheet. All other reader context/identity/byte behavior is preserved.

The original null-back-cursor equality check remains at1723. Presentation now keeps native Close only, hides the empty owned footer, shows omission Refresh only after failed/stale read, exposes its bounded server sort selector/body scope count, and uses CSS sort arrows rather than copying symbols into phone labels. These changes add no data fields, mutation or new route. The distinct Candidate-review warning2032 uses actual whole-run PROGRESS.review, not resident page count.

**O02 historical omitted-row error bypass is closed:** the initial fresh omitted renderer disabled buttons on error but its row double-click/Enter `known` predicate lacked that gate. Root's current1769 adds `!error`, so both native row navigation and Open controls cannot clear a retained read error; explicit Refresh remains available. Fresh2011–2025 already had `rowLocked=locked||Boolean(readError)` on all Candidate remedies and `if(locked||readError)return` in openRow. Those Candidate gates are confirmed present, not an outstanding defect or a newly required fix.

Current1939 keeps native reader rows directly;2018 tests native issue_code for PF08 attention. The prior version's explicit local review_issue_code alias was valid and already made that same branch run; the cleanup does not fix a broken PF08 dispatch. Original DR05 identical mutation replay, PF08 qualified progress handoff, current actor/epoch guards, exact physical ID/version opening and informational omissions retain their reviewed behavior.

| Fresh saved source | SHA256 |
|---|---|
| FE Office | `44BD5ACEEC83DCDD7A5683929FB671AB0DF93BAC5FAB5CC6DE9DED2DA37FFADA` |
| FE native CSS | `EA6285B395C8373E03BF5711AA46F7BC5D499261F0A32D50FF29D9200FA70DBB` |
| FE main | `69D84496624EF0F3AEBFB204E1E81CD7EBD824A793BC130A8379D4E7A0992F5B` |
| FE index | `9252D255DBA51987FC978FB097F8ED59A571BF69537D3C93D0616ECCF35C64CD` |
| Current0332 SQL | `D424565E4301D8F7B8FFBFC64C82CF40F85F20D4D7C046C627FDD084F828E751` |
| Current registry | `1F9D271ED332985AEE4043E14E3916B7FA1A77A3CAEDA74823B53FF1DC27069E` |
| BE preparation | `00F21FD6DB791E3F2990344EDE3FD0EEF2A417FA60120792F67A99F196CB7A48` |
| D02 manifest | `EAB3266BC7E92B353028853DFE2718AF1AEAE40CA24F339612E331D393C0B23C` |

This amendment is SOURCE-only. No database/runtime/tests/business/hosted/queue/A4 actions were run by this reviewer. Root's current captures are separately reviewed; no pixel or joined-acceptance claim is inferred here. Later saved-byte changes need a scoped receipt update.

## Final0341 / native accessibility amendment — SOURCE PASS

Fresh saved bytes below supersede the preceding0332 checkpoint. **No remaining essential source defect was identified in the final shape, transport, row actions or accessibility integration.** This is a scoped static source verdict; historical O01/O02 findings and their closures remain above.

The registry now names `12102026_0341_banking_pay_next_preparation_issue_review_order_v1.sql`. Its complete0332→0341 diff changes only the header comment and STATE sort operand at130. REVIEW/FAILED share rank0 (To review first), followed by PREPARING/PENDING/READY and the remaining native states. The existing native row-key tie, ascending/descending seek and tail validation at134–165 are unchanged. Old cursors containing the previous State label cannot silently resume under the new rank operand: exact tail matching refuses them through the existing context-changed response. No extra field, command, table or pricing calculation was added.

Office1720 selects STATE_ASC only for the Candidate view; Not-in-Draft still starts Candidate A–Z. Candidate1997/2038 labels the state ordering explicitly and displays server counts.scope_count for the complete Candidate scope, distinct from the resident three100-row pages. Native reader/Worker/FE finite row/context/count/cursor shapes remain identical to the reviewed prior interface (BE14–54; FE205–245), including nullable worker identities, PENDING/FAILED, unknown-original labels, exact SYSTEM reason enumeration and visible well-shaped unknown codes. The backward-null-cursor equality check1723 remains present.

Both lists now expose labelled focusable regions (1765/2001), unique row focus keys and row-qualified action keys (1769/1775;2006/2029). ArrowUp/ArrowDown/Home/End operate only when the row itself owns the event, update the selected row and focus/scroll that existing resident row (1770/2007); they do not submit an action. Existing UI continuity15–28 restores the same available keyed row/control after replacement, with the native fallback when it is absent. Enter/double-click still use the qualified existing opener. Omitted1771 gates known navigation on !error; Candidate2015–2029 keeps rowLocked and openRow's readError guard on every remedy. Actor/NEXT/epoch invalidation and main's exact physical-ID/version refusal remain intact (main7182–7204,7231–7242,148926–148932).

Current1764/1998/2035 provides native live read-status copy during initial/refresh/paging reads.2011 gives every accepted finite worker state an explicit existing badge tone; BLOCKED overrides to danger at2014. These presentation changes retain native state/reason authority and the existing DR05 identical-request retry/PF08 qualified-progress handoff. CSS939–968 remains scoped to native lists, phone controls, existing sort indicators and notice layout; source inspection is not a rendered accessibility/pixel acceptance claim.

| Final saved input | SHA256 |
|---|---|
| FE Office | `4049207AED657E1857F18C6922A104DDA6FFBA3FF15150A2BFAA2019AEDB9B86` |
| FE native CSS | `7BAF62658A63AD218A55F7806E4AB507E7569B6AD636AAED8ED6F484AFA6594F` |
| FE main | `69D84496624EF0F3AEBFB204E1E81CD7EBD824A793BC130A8379D4E7A0992F5B` |
| FE index | `9252D255DBA51987FC978FB097F8ED59A571BF69537D3C93D0616ECCF35C64CD` |
| Current0341 SQL | `3526263068AEFA3C351E68B6ECA3C6EE3CC30C40474027C14C97DD6D13007CA5` |
| Current registry | `35B57B2E8EDBD3A1196281DD743F817A19753F313A2D71DF931D42D15728692C` |
| BE preparation | `00F21FD6DB791E3F2990344EDE3FD0EEF2A417FA60120792F67A99F196CB7A48` |
| D02 manifest | `EAB3266BC7E92B353028853DFE2718AF1AEAE40CA24F339612E331D393C0B23C` |

SOURCE PASS is limited to these bytes and exact DR15 D02/D04/D05/D07 seams. Runtime/joined/queue/business/database/hosted/tests/timing and A4/pixel acceptance remain NOT_RUN by this reviewer. Root's compile receipts and the independent screen review remain separate evidence; no whole-item completion or release authority is claimed here.
