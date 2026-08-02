# CloudTMS correction-pair lifecycle and HealthRoster validation completion

Generated: 2 August 2026 (Europe/London)

## Independent-review instruction

Review the committed source, the installed TEST definitions, and the deployed TEST frontend/Worker against this report. Do not use this handover to broaden the implementation. If anything remains wrong, return a function-by-function and file-by-file implementation plan containing only the functions/files that demonstrably still require work, the exact defect, the smallest correction, and an executable regression test. Do not propose changes to Banking Pay, Policy X, payment execution, settlement, remittances, invoice economics, general TSFIN calculation, HealthRoster Daily mutation, or HealthRoster Weekly validation-only mutation unless a concrete defect in this brief proves such a change unavoidable.

## Outcome

The requested correction-pair lifecycle controls and HealthRoster validation-review UX have been implemented, committed, pushed, deployed to normal TEST, and tested. Sarah Dumbuya's duplicate shift was a real UI aggregation defect: one validation blocker and one historical email row represented the same source shift. The frontend now merges the historical email delivery state into the owning validation row. The shift is displayed once, while `Previously sent 1 time` remains visible.

No policy decision was changed beyond the rules explicitly agreed with the user.

## Locked business policy implemented

### Correction pairs

- A changed-hours reversal and replacement sharing the correction identity form one lifecycle unit.
- Authorising either member authorises both atomically.
- Unauthorising either member unauthorises both atomically.
- Simple Timesheet and Bulk Authorise use the friendly CloudTMS confirmation modal before the joint transition.
- Cancelling the confirmation makes no mutation.
- Both members may be deleted or archived together when ordinary financial/lifecycle rules allow it.
- Neither member may be deleted or archived alone.
- A pair may be placed on one invoice or split across two compatible invoices.
- During an invoice edit, one member may be temporarily unplaced so it can be moved to another compatible invoice. The bounded move gap must not be treated as a completed correction state.
- If one member is invoice-placed and the other is not, only the missing member shows the Timesheet Summary issue `Paired needs invoicing`.
- Its exact help text is: `This paired timesheet needs invoicing. The other timesheet is attached to an invoice, this timesheet needs attaching as soon as possible`.
- Import Review still blocks malformed or economically unprovable correction history. It does not assume that two members must be on the same invoice.

### HealthRoster validation-only review

- HealthRoster Weekly validation-only does not amend financial timesheets.
- HealthRoster Daily remains validation-only and is not made import-authoritative.
- Matching shifts are `Passed checks`.
- A differing shift appears once in the appropriate review section with the candidate, week, evidence, difference and row-level proposed outcome.
- A Weekly group carries `Validation incomplete · N shift(s) differ` while any row still prevents full validation.
- A specific HealthRoster shift missing from an existing submitted Weekly timesheet can be resolved as `Candidate did not work this shift`; an entirely missing/unsubmitted timesheet cannot use that exception.
- A Daily missing timesheet remains an independent blocker because Daily records do not hold other days in a Weekly record.
- An invoice-attached timesheet is not eligible for validation and is shown as `Timesheet present but invoiced` until removed from the invoice (and the invoice unissued first where required).
- Import-level `SUCCESS` is shown only after every validation target has settled successfully with no outstanding blocker/email/document work.
- Query-email rows use a time-sensitive greeting, concise non-duplicated references, the configured company name, and the relevant complete timesheet evidence.
- Evidence completeness means: a fully generated and signed electronic timesheet, a fully signed QR timesheet, or an attached manual timesheet. Missing evidence remains Pending Action.
- Required document generation is queued asynchronously and pushed for immediate processing. The modal polls automatically; the user does not need to press Recheck. Review and Apply remains available as a review surface but cannot commit email sending until required documents are ready.
- Sent/delivery history is preserved on a subsequent recheck without rendering a duplicate shift row.

## Source commits

### Backend `cloudtms-backend`, branch `test`

Runtime commits in this combined workflow:

- `d3524c9` Fix validation-only Import Review workflow
- `bc8d960` Fix Import Review UI state persistence
- `16c9143` Accept the Email review section in UI state
- `ace0102` Complete HealthRoster validation evidence workflow
- `c809678` Complete correction pair lifecycle and validation review

Current pushed runtime head at verification: `c8096785adf46f7570c3d7a1c86215337ec4556d`.

The final correction-pair commit changes only:

- `broker/src/index.js`
- `supabase/repeatable/02082026_2014_timesheet_correction_pair_lifecycle_preview_v1.sql`
- `supabase/repeatable/21072026_1235_00_import_correction_policy_helpers.sql`
- `supabase/repeatable/21072026_1235_00b_import_correction_runtime_guards.sql`
- `supabase/repeatable/21072026_1235_10_invoice_correction_pair_scope_v1.sql`
- `supabase/repeatable/21072026_1235_32_timesheet_authorise_bulk_atomic.sql`
- `supabase/repeatable/21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql`
- `supabase/repeatable/21072026_1820_00_import_review_internal_core.sql`
- `supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_apply_edits.sql`
- `supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_correction_validate_batch.sql`
- `supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql`
- `tests/correction-pair-lifecycle-placement.test.cjs`
- `tests/import-authoritative-effective-balance-helper.test.cjs`

Although `26052026_2100HRS_NEW_FUNCTIONS.sql` is a large canonical repeatable file, the runtime change in this brief is confined to existing Timesheet archive/delete helpers near the end of that file. No `pay_*`, Banking Pay, settlement, provider, remittance or Policy X definition was changed.

### Frontend `TEST-Frontend`, branch `main`

Relevant commits:

- `0132013` Improve validation-only Import Review UI
- `a074f92` Persist opaque Import Review expansion state
- `186ef70` Clear resolved Import Review save errors
- `3e31b21` Clarify Daily validation email summaries
- `cb40533` Show incomplete validation on week headers
- `08382f3` Show settled import validation success
- `65f30f5` Complete correction pair lifecycle UX
- `540a490` Show preserved import query delivery history
- `ff63f97` Respect correction pair confirmation cancellation
- `9a86603` Keep pair confirmation above loading overlay
- `dfcc915` Refresh completed pair lifecycle immediately
- `08cf641` Align Import Review test with row-level outcomes

Current pushed head at verification: `08cf64119299d43d16629d89649d86a883624157`.

Changed runtime/test files in the final pair UX range:

- `js/import-review-v1.js`
- `js/main.js`
- `tests/correction-pair-lifecycle-ui.static.test.mjs`
- `tests/e2e/import-review-v2.spec.ts`

## Function-by-function implementation map

### `timesheet_correction_pair_lifecycle_preview_v1`

New bounded read/validation authority for lifecycle selection. Expands a selected member into one exact two-role active pair, carries signed row identities/fingerprints, reports affected members and fails closed on malformed membership.

### `_ctms_expand_correction_member_ids_v1` and `_ctms_expand_lifecycle_items_v1`

Expand a selected lifecycle member to the exact correction pair. Preserve canonical expected timesheet IDs and row signatures so downstream atomic functions retain stale-row protection.

### `timesheet_authorise_bulk_atomic` / `timesheet_unauthorise_bulk_atomic`

Consume expanded pair membership before locks and mutations. Both roles enter the same transaction and either both succeed or neither succeeds.

### `timesheet_archive_state_v1`, `timesheet_archive_transition_v1`, `timesheet_standard_delete_preview_v1` and existing delete application helpers

Apply the agreed all-or-nothing removal rule to active correction pairs. A single-leg archive/delete is not offered as a valid completion. Existing financial-history retention and archive protections remain authoritative.

### `invoice_correction_pair_scope_v1`

Classifies pair invoice placement without requiring both members to share one invoice. It distinguishes complete compatible placement, compatible split invoices, the bounded invoice-edit move gap, and malformed/unsafe placement.

### `_ctms_assert_invoice_correction_lines_v1`, `private._invoice_correction_validate_batch` and `invoice_apply_edits`

Validate and apply invoice edits against the pair placement policy atomically. A user can remove one member during an edit and place it on another compatible invoice without corrupting later import mathematics; unsupported partial economics still fail closed.

### `_import_review_effective_invoice_balance_core_v1` and `_import_review_action_catalog_core_v1`

Retain immutable economic truth and surface the correct validation blockers. Invoiced validation targets do not proceed to TSFIN follow-up. Redundant email/action rows are removed only when they refer to the same underlying validation row, while delivery history is preserved for the UI.

### Worker handlers in `broker/src/index.js`

- Simple and bulk authorise/unauthorise call the pair preview and require explicit confirmation.
- Successful atomic pair responses return signed affected rows for immediate UI refresh.
- Timesheet Summary enrichment marks only the unplaced member with `Paired needs invoicing`.
- HealthRoster document/email follow-up routes preserve asynchronous queueing and validation-only behaviour.

### Frontend `main.js`

- Friendly pair confirmations for simple and bulk lifecycle operations.
- Confirmation cancellation is handled as cancellation, not consent.
- Confirmation displays above the global loading overlay.
- Successful pair mutations consume signed affected-row patches immediately instead of entering the uncertain-outcome reconciler.
- Pair archive/delete language is explicit.
- The Timesheet Summary issue and exact hover guidance are shown on the missing invoice member only.

### Frontend `import-review-v1.js`

- Candidate/week grouping and large group expand/collapse controls.
- Functional sorting in all sections.
- Row-level validation reason/outcome presentation.
- Weekly incomplete badge.
- Passed checks and final SUCCESS semantics.
- Automatic document-preparation polling.
- Delivery history is merged into the matching validation blocker rather than rendered as a duplicate row.

## Installed TEST function evidence

Fresh `pg_get_functiondef` MD5 values recorded after installation:

| Function | MD5 |
| --- | --- |
| `private._invoice_correction_validate_batch(jsonb,date)` | `7e94e13f8cd730fdd8d1956aaa2846a6` |
| `_ctms_assert_invoice_correction_lines_v1` | `b75e9046d6fb1b2577801fafce624b2b` |
| `_ctms_expand_lifecycle_items_v1` | `369b16dca3de1c744ccad04c09131835` |
| `_import_review_action_catalog_core_v1` | `b0e57debd99db45149679921167ab15a` |
| `_import_review_effective_invoice_balance_core_v1` | `3da4f5254190266741f448561da7c35f` |
| `invoice_apply_edits` | `569497c0f3831b0b0a2f237c4220c398` |
| `invoice_correction_pair_scope_v1` | `c7686d26e0f2b3e1188d2484f80d3ca7` |
| `timesheet_archive_state_v1` | `70c5b38fb6783c2dd5c7987fa40fa17a` |
| `timesheet_archive_transition_v1` | `b153ae8b844781326c92c7cb07e631df` |
| `timesheet_authorise_bulk_atomic` | `fa4191e06a2031e04025863197211007` |
| `timesheet_correction_pair_lifecycle_preview_v1` | `bf4615061961e5dc3a1efb91879b73fe` |
| `timesheet_standard_delete_preview_v1` | `b1931fff52322b163d924776f9af6313` |
| `timesheet_unauthorise_bulk_atomic` | `bd940f973094c4b1250cf648043af534` |

## Deployment evidence

- Normal TEST Worker deployed: `test-cloudtms-backend`.
- Deployed Worker version ID: `dd84b0e2-5de4-449e-859e-08fa22dd9af3`.
- TEST `/healthz`: HTTP 200, `ok`.
- TEST `/version`: `1.2.0` (harmless runtime marker confirmed).
- Frontend Pages workflow for `08cf641` completed successfully.
- Saved/deployed `js/main.js` SHA-256 both equal `694D766ED1049A3D9C9A14B32E76E5BBFC0A06DAEC2DDF091F0E8694BA1CE4D3`.
- Production deployment: none.

## Verification results

### Automated

- Frontend JavaScript syntax: passed for `main.js` and `import-review-v1.js`.
- Frontend pair lifecycle static suite: 8/8 passed.
- Backend focused correction/import suite: 48/48 passed.
- Backend standard suite: 182/182 passed during the implementation run.
- Deployed Import Review contract Playwright test: 2/2 passed.
- Patched Import Review desktop/narrow Playwright contract: 2/2 passed after updating the obsolete expectation that differences belonged in group-header badges; the current policy correctly presents them on the row and week summary.
- A broader all-CJS run exposed three pre-existing Banking Pay source-pattern mismatches unrelated to this change. They were not changed or folded into this work.

### Live Sarah regression

The deployed modal was checked on Sarah's stored HealthRoster Weekly import:

- candidate group: 1 item;
- week group: 1 item;
- source reference displayed once;
- week badge: `Validation incomplete · 1 shift differs`;
- outcome: `Timesheet present but invoiced`;
- preserved history: `Previously sent 1 time`;
- no duplicate Email-section row.

Root cause: the same source row existed in both the validation catalogue and query-email history. Fix: deterministic identity matching merges delivery metadata into the validation row and suppresses only the redundant visual row.

### Live correction-pair lifecycle

- Friendly confirmation was displayed for joint pair lifecycle actions.
- Cancel produced no database mutation.
- Joint unauthorise completed for both members.
- Joint authorise completed for both members.
- Worker tail was started before the actions and captured the normal TEST `/unauthorise` and `/authorise` requests without an application exception.
- A later diagnostic timed out after another unauthorise. The sole affected pair was restored through the same atomic pair-aware authorisation function.
- Final TEST integrity: 9 active pair groups; 9 jointly authorised; 0 split-authorisation groups; 0 fully unauthorised groups.

### Live database health relevant to this work

- Import apply operations: 38 total; 37 COMPLETE; one known non-complete Sarah operation.
- The remaining Sarah operation is `SOURCE_COMMITTED_TSFIN_PENDING` from the earlier attempt to validate an already invoice-locked timesheet. This implementation prevents that invoiced validation target from being eligible. The old operation was not retried or rewritten.

## Explicitly unproven live permutations

These contracts are implemented and covered by source/database-focused tests, but were not forced through destructive UI fixtures because no safe eligible live specimen was available:

1. Moving one member of a pair between two compatible draft invoices through the live invoice modal, including the temporary one-leg move gap.
2. Archive/delete round-trip of a financially eligible correction pair; the available live pair was financialised and correctly did not offer an unsafe removal.
3. End-to-end generation of a missing electronic/QR/manual evidence document through the live queue and subsequent email attachment. The available mismatches were either invoice-blocked or lacked a safe complete evidence fixture. Queue/poll behaviour and deployed UI contracts passed.

These limitations must not be represented as failures, but an independent reviewer should require real reversible fixtures before claiming those three UI permutations are live-proven.

## Rollback

`02082026_2100_correction_pair_lifecycle_rollback.sql` contains the exact pre-install TEST definitions for the 12 replaced SQL functions plus the drop for the new preview function. It is rollback evidence, not an instruction to run it. It must not be applied without rechecking current live hashes and explicit TEST rollback authority.

Frontend rollback is by reverting the listed frontend commits and redeploying Pages. Worker rollback is by restoring the prior backend source/deployed version only with explicit TEST deployment authority.

## Required independent verification checklist

1. Fetch backend `test` and frontend `main`; confirm the stated heads or classify any later commits before reviewing.
2. Diff each listed runtime commit and verify no Banking Pay/Policy X or general invoice/TSFIN economics change is present.
3. Re-query all 13 installed function definitions and compare exact source-normalised hashes.
4. Verify the pair preview, expansion, authorise, unauthorise, archive/delete and invoice placement contracts function-by-function.
5. Verify simple and bulk UI confirmation, cancellation, overlay ordering and immediate signed-row refresh.
6. Verify the single-leg invoice move gap is accepted only inside the bounded edit workflow and malformed effective economics still block Import Review.
7. Verify Weekly validation-only and Daily validation-only remain non-mutating.
8. Verify invoiced validation rows cannot enqueue TSFIN follow-up.
9. Verify async evidence generation is queued immediately, automatically polled, and email commit waits for complete evidence.
10. Verify Sarah's validation/email identity renders one row with preserved delivery history.
11. Run the provided focused tests and real reversible fixtures for the three explicitly unproven live permutations.
12. Report only demonstrated remaining defects. For each, name the exact function/file and provide the smallest function-by-function correction and executable regression test.

## Safety statement

- Secrets printed or committed: no.
- Raw cookies, session state, Worker tail logs or DB result dumps committed: no.
- Production access/deployment: no.
- Banking Pay or Policy X runtime change: no.
- Payment execution/provider submission: no.
- Invoice economics or general TSFIN-calculation change: no.
- TEST mutations: the explicitly authorised pair lifecycle test only; final pair state fully restored.
- Raw tail logs remained under `C:\tmp` and were not committed.
