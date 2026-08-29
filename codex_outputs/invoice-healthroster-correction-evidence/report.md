# West London HealthRoster invoice evidence repair

## Outcome

The missing HealthRoster support page for West London correction invoices has been fixed in the canonical backend source, committed, pushed to the backend `test` branch, installed in TEST Supabase, and deployed to the normal TEST Worker.

The repair is prospective. Issued invoice `1003101` and its stored one-page PDF remain immutable and were not regenerated or altered. A newly generated invoice document using the same correction evidence now receives both HealthRoster source occurrences and renders the HealthRoster support section.

## Root cause

Three independent gates omitted the evidence:

1. Invoice generation reconstructed correction ancestry only for `NHSP_ADJUSTMENT`, not `HEALTHROSTER_ADJUSTMENT`.
2. Direct HealthRoster evidence attachment required both `requires_hr` and `hr_attach_to_invoice`. `requires_hr` is the validation-only setting and is correctly false for West London's import-authoritative contract; attachment should be controlled independently by `hr_attach_to_invoice`.
3. The presentation model and PDF template did not carry/display the source occurrence's correction role, so a HealthRoster reversal could not be clearly identified even when evidence existed.

The issue-time evidence guard repeated the same incorrect `requires_hr` dependency.

## Implemented behavior

- Both NHSP and HealthRoster correction ancestry are eligible for source reconstruction.
- HealthRoster evidence is attached when the frozen `hr_attach_to_invoice` setting is enabled, independently of validation-only behavior.
- Correction source occurrences are labelled:
  - `HealthRoster Corrected Hours`
  - `HealthRoster Reversal`
  - `HealthRoster Shift` for ordinary source evidence
- Reversal source rows also carry `reversal_state = REVERSED`.
- The HealthRoster support table renders a dedicated `Entry` column.
- Issue validation now requires HealthRoster evidence whenever the frozen attachment policy requires it.

## Files changed

- `broker/src/invoice-document-templates.js`
- `supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_issue_validate_batch.sql`
- `supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql`
- `supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_12_private_invoice_generation_advance_core_v8.sql`
- `tests/invoice-async-v8-sql-contract.test.js`
- `tests/invoice-healthroster-correction-support.test.js`

## Source control

- Implementation commit: `e6a2350399afab26250a67bfc43c067359f18a79`
- Commit message: `Attach HealthRoster correction evidence to invoices`
- Backend branch: `test`
- Push: non-forced fast-forward; `origin/test` matched the implementation commit after publishing.

## TEST database deployment

- Project: `test-cloudtms`
- Applied migration record: `02082026_0320_healthroster_invoice_correction_evidence`
- Deployed function hashes:
  - generation core: `7bfecba1d0d1960ac895de982e22260d`
  - issue validation: `a840c68e8cda0e3ec3fd03e78dfcce5d`
  - presentation snapshot: `6d53f6e7ab189b36ac74c3e6c60e8cc1`
- Function owner/security/search-path checks passed.
- Normal runtime roles did not receive new direct execution permissions.

## Worker deployment

- Worker: `test-cloudtms-backend`
- Version: `4a17d652-c5e0-4fbd-8641-67029ba82b9f`
- `/healthz`: HTTP 200, `ok`
- Production deployment: none
- Frontend deployment: none; no frontend source changed

## Verification

### Automated source and rendering tests

- Focused affected tests: `62/62` passed.
- Full local dirty-worktree suite: `182/182` passed.
- Clean committed-source suite: `178/178` passed.
- Clean committed Worker build/dry-run passed.
- JavaScript syntax checks passed.

The clean committed-source run is the authoritative commit proof; the larger dirty-worktree run includes unrelated tests belonging to other ongoing work.

### Real West London evidence reconstruction

A bounded read-only TEST query followed the actual invoice-line timesheets through their parent ancestry to the authoritative HealthRoster source shift. It returned exactly:

| Evidence role | Occurrences | Distinct source shifts | Distinct imports |
| --- | ---: | ---: | ---: |
| HealthRoster Corrected Hours | 1 | 1 | 1 |
| HealthRoster Reversal | 1 | 1 | 1 |

This proves the issued invoice's real correction pair has complete, resolvable HealthRoster source evidence under the deployed rule.

### Existing issued invoice integrity

After all tests, invoice `1003101` remained:

- status `ISSUED`;
- two invoice lines;
- two current TSFIN records locked to the invoice;
- zero stored HealthRoster evidence rows, as expected for its already-frozen pre-fix document.

No invoice, timesheet, TSFIN, contract week, or source evidence mutation persisted from verification.

An attempted replay of the historical completed generation chunk was safely superseded by the existing invoice/source lifecycle guards. That is correct behavior: an issued and locked source cannot be replayed as a new draft-generation operation. This is not evidence that the new generation path fails; the deployed ancestry reconstruction was verified directly and the clean build/render contract tests exercised the new generation, presentation, and template behavior.

## Policy and scope confirmation

- Policy decisions changed: **none**.
- HealthRoster Weekly validation-only behavior changed: **no**.
- HealthRoster Weekly import-authoritative behavior changed: only invoice evidence attachment for generated correction documents.
- HealthRoster Daily changed: **no**.
- Invoice amounts, VAT, totals, issue/unissue semantics, TSFIN calculations, and correction mathematics changed: **no**.
- Banking Pay, payment execution, CSV/provider processing, settlement, remittances, and Policy X changed: **no**.
- Production touched: **no**.
- Secrets printed or committed: **no**.

## Operational note

The existing PDF for issued invoice `1003101` cannot acquire a new page merely from a code deployment because issued document artifacts are immutable. Proof in the UI requires a newly generated draft/issued document containing a HealthRoster correction pair, or an explicitly approved unissue/regenerate/reissue lifecycle for the old invoice. The fix will apply automatically to new documents.

