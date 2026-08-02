# HealthRoster validation evidence workflow — implementation and independent-verification handover

## Reviewer mandate

Independently verify the deployed implementation described below against current GitHub source and the TEST runtime. Review it function by function and file by file. If anything remains to satisfy the brief, return a highly detailed function-by-function implementation plan containing **only** the functions/files that demonstrably still require work. Do not use this handover to broaden scope into Banking Pay, invoice economics, import-authoritative financial correction logic, TSFIN calculation, unrelated email workflows, or other application areas.

## Final implementation status

The requested HealthRoster validation-only workflow has been implemented, committed, pushed, installed on TEST Supabase, deployed to the normal TEST Worker, published to the TEST frontend, and tested after deployment.

No production resource was accessed or deployed.

No policy decision was changed during implementation. The implemented policy is the policy explicitly agreed with the user:

- HealthRoster Weekly validation-only is atomic per candidate/client/contract/week.
- One mismatch, query email, missing evidence, invoice blocker, or unresolved shift holds the entire Weekly timesheet. No sibling shift may be validated and no TSFIN follow-up may start for that held week.
- HealthRoster Daily remains validation-only and is atomic only per separate Daily timesheet. One Daily timesheet does not hold another.
- An invoiced timesheet is not eligible for validation. It appears in Pending Action with the friendly reason `Timesheet present but invoiced`, is non-selectable, and cannot cause validation, email dispatch, TSFIN work, or SUCCESS.
- Weekly `Candidate did not work this shift` remains available only for a proved shift omitted from an otherwise submitted Weekly timesheet. It is not offered where the whole Weekly timesheet is missing or unsubmitted.
- Query emails require exact, complete, current timesheet evidence before Apply is allowed to send them.
- Electronic evidence requires hours and both nurse and authorised/manager signature evidence.
- QR evidence requires a completed signed QR lifecycle and its signed source PDF.
- Manual evidence requires the real registered manual document.
- Complete evidence without a current generated timesheet PDF is prepared asynchronously. Import Review remains usable, but Review and Apply cannot commit the email until preparation has completed and re-attestation succeeds.
- A green `SUCCESS` status is server-derived and appears only after the import is APPLIED, follow-up is COMPLETE or NOT_REQUIRED, there are no current blockers/email/query actions, and all validation targets have reached `VALIDATION_OK` for that import.
- Non-authoritative Weekly and Daily processing remains validation-only.

## Root causes corrected

### 1. Sarah's historical TSFIN follow-up

The previous Weekly apply path allowed a selected email/query action to establish the candidate/week group, then treated sibling validation rows as eligible. That meant a held Weekly timesheet could still enter validation/TSFIN follow-up even though one shift was unresolved.

The deployed fix creates `tmp_mode_a_eligible_groups` and admits a Weekly group only when every current validation action in that exact candidate/client/contract/week is eligible. A selected email row can no longer make sibling rows eligible. A held Weekly group produces no validation target and no TSFIN target.

The old `TSFIN_FOLLOW_UP_INCOMPLETE` operation remains historical TEST state and was not retried or altered. It pre-dates this correction.

### 2. Evidence was not a server-enforced precondition

Previously, the email UI could present a selectable query without proving that the exact current timesheet PDF existed and represented complete source evidence. The new evidence helper re-attests the source, invoice protection and exact document revision on the server. Both catalog materialisation and email enqueue consume this authority.

### 3. SUCCESS was not a settled server status

The import list previously exposed the underlying lifecycle state only. It now derives `display_status=SUCCESS` only from settled database evidence. The UI renders that server status and does not invent success locally.

## Published source identities

### Backend

- Repository branch: `cloudtms-backend/test`
- Commit: `ace010237f70932b8f121b73a3ffa6ab5fcdcf66`
- Commit message: `Complete HealthRoster validation evidence workflow`
- This is the runtime implementation commit. The branch also contains a later documentation-only handover commit; no runtime source changed in that documentation commit.

Changed files:

1. `broker/src/import-review.js`
2. `broker/src/index.js`
3. `supabase/repeatable/21072026_1820_00_import_review_internal_core.sql`
4. `supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql`
5. `supabase/repeatable/21072026_1820_04_timesheet_query_email_rpcs.sql`
6. `supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql`
7. `tests/import-review-validation-evidence-integrity.test.cjs`

Diff size: 577 insertions and 21 deletions across those seven files.

### Frontend

- Repository branch: `TEST-Frontend/main`
- Commit: `08382f3a38371cf1820c9e664e177192f90ebac4`
- Commit message: `Show settled import validation success`
- Remote parity: local `HEAD` and `origin/main` both equal this SHA at handover time.

Changed files:

1. `css/import-review-v1.css`
2. `index.html`
3. `js/import-review-v1.js`

## Function-by-function implementation

### `public._import_review_query_evidence_core_v1(uuid)` — new internal authority

Canonical source: `supabase/repeatable/21072026_1820_00_import_review_internal_core.sql`

Responsibilities:

- Requires an exact current, non-archived timesheet row.
- Rejects invoice-protected timesheets with `TIMESHEET_PRESENT_BUT_INVOICED`.
- Proves complete Weekly schedules by requiring every schedule member to contain start and end values; Daily requires worked start/end.
- Proves electronic evidence from hours, nurse signature object/hash, manager/authoriser signature object/hash/name and server authorisation time.
- Treats any QR lifecycle marker as QR and accepts it only when the completed signed-QR contract is satisfied.
- Proves manual evidence from the timesheet's manual PDF key or exact READY registered manual document asset.
- Proves the current generated attachment using `current_document_version_id`, exact `document_revision`, `TIMESHEET` entity/purpose, READY state, unsuperseded row, R2 key, SHA-256, non-zero bytes/pages, and ready/verified timestamps.
- Returns source/document readiness, preparation requirement, exact attachment metadata, source type, invoice protection and a deterministic evidence fingerprint.
- Has no runtime-role direct EXECUTE grant; it is an internal server authority.

Installed TEST MD5: `95854d40deeb64d26c4556f356cab236`.

### `public._import_review_action_catalog_core_v1(uuid,integer,integer)`

Canonical source: `supabase/repeatable/21072026_1820_00_import_review_internal_core.sql`

Responsibilities added:

- Re-attests query evidence for current EMAIL actions.
- Converts incomplete/preparing/invoiced email rows to non-selectable Pending/Blocked results with the evidence reason.
- Holds Daily `NO_ACTION` validation rows only when their exact timesheet has an unresolved email/evidence/invoice condition.
- Holds Weekly `NO_ACTION` validation rows atomically when any action in the same candidate/client/contract/week is unresolved.
- Adds a Weekly hold summary/badge without misclassifying the held row as passed.
- Preserves existing validation-only `Candidate did not work` resolution behaviour.

Installed TEST MD5: `1e36c2a336d5a925c479fde70a0aa942`.

### `public.import_review_attachment_preparation_targets_v1(uuid,uuid,integer)` — new service RPC

Canonical source: `supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql`

Responsibilities:

- Returns at most 100 exact timesheet-document preparation commands for current email actions whose source evidence is complete but whose exact current PDF is not READY.
- Uses deterministic command tokens based on import, timesheet and evidence fingerprint.
- Does not send email synchronously and does not make Review and Apply wait on a Worker request.
- Direct EXECUTE is service-role only.

Installed TEST MD5: `4701b34a8bef8698a2c4487a0a7ec67b`.

### `public.import_review_list_v1(...)`

Canonical source: `supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql`

Responsibilities added:

- Counts unresolved current actions, applied email/query outcomes, applied financial changes, validation targets, and incomplete validations.
- Returns `display_status=SUCCESS` only when the lifecycle and settled evidence contract are all true.
- Does not use a browser-local inference for success.

Installed TEST MD5: `60aaa40bd3455000c7d958c4e4d51786`.

### `public.timesheet_query_email_enqueue_v1(...)`

Canonical source: `supabase/repeatable/21072026_1820_04_timesheet_query_email_rpcs.sql`

Responsibilities added:

- Re-attests every selected timesheet immediately before enqueue.
- Rejects incomplete, preparing or invoiced evidence.
- Attaches each exact current PDF using its proven R2 key and filename.
- Uses a time-sensitive London greeting.
- Uses the agreed request wording and company sign-off from settings.
- Does not repeat an identical reference as `x -> x`.
- Keeps Weekly and Daily validation email behaviour aligned.

Installed TEST MD5: `315acd41eb8824dd854f9087ebea9e59`.

### `public.hr_weekly_apply_transactional(uuid,jsonb,uuid)`

Canonical source: `supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql`

Responsibilities added:

- Builds exact validation-only Weekly eligible groups before mutation.
- Requires the whole candidate/client/contract/week group to be free of unresolved/blocking/email/evidence conditions.
- Prevents a selected email row from admitting sibling validation rows.
- Restricts the validation target and TSFIN follow-up target set to those wholly eligible groups.
- Leaves import-authoritative financial amendment routes unchanged.

Installed TEST MD5: `19e39b9918f512abfc3cbb77f54018ed`.

### Worker `broker/src/import-review.js` and `broker/src/index.js`

Responsibilities added:

- Requests bounded asynchronous attachment-preparation targets after materialisation/recheck.
- Schedules the existing `VIEW_TIMESHEET_DOCUMENT` commands in `waitUntil` and nudges existing document queues.
- Does not block ordinary import review rendering while evidence generation is underway.
- Does not send an email before Review and Apply re-attests a READY attachment.

### Frontend `js/import-review-v1.js`, CSS and `index.html`

Responsibilities added:

- Renders server-provided `SUCCESS` with a green status style.
- Shows friendly evidence states including invoiced, incomplete and preparing.
- Uses a new deployed asset version so browsers load the corrected JS/CSS.

## TEST database installation

Target project was explicitly confirmed as `test-cloudtms` (`yakevhtttcsljosbdpov`) before database operations.

Applied migration records:

1. `healthroster_validation_evidence_workflow_20260802`
   - Atomically installed the six definitions above after an actual TEST-schema transaction dry-run.
2. `restore_import_review_contract_gate_20260802`
   - Restored the exact existing canonical GitHub definition of `import_review_contract_version_get_v1` because the installed TEST copy was stale and prevented the current Worker from starting Import Review.
   - This did not introduce new runtime policy or Banking Pay behaviour. It restored source/runtime parity for two already-canonical contract flags.
   - Installed MD5: `1be00633e7235b94519a65e1afacf94b`.

Function owners, search paths and grants were checked after installation. Internal helpers remain inaccessible to normal runtime roles; public runtime entry points retain service-role-only execution.

## Normal TEST deployments

### Worker

- Worker: `test-cloudtms-backend`
- Deployed version ID: `20b2bfc2-c658-4efc-84cf-40186968434c`
- Wrangler version number: 2125
- Created: 2 August 2026 18:13:25 UTC
- `/healthz`: HTTP 200 after deployment.
- Wrangler Tail was connected before live browser actions and observed the deployed version. It has since been stopped and its raw logs removed.

### Frontend

- GitHub Pages was published from `TEST-Frontend/main` commit `08382f3`.
- Live TEST HTML references asset version `20260802-validation-policy-r4`.
- Live JS contains `display_status` and the invoice-evidence blocker handling.
- Live CSS contains the green SUCCESS status style.

## Post-deployment runtime verification

### Weekly invoiced/held fixture

TEST import: `ba1f8f09-0aac-4913-ac12-f24ff451e4cd`

TEST timesheet: `afd31117-1f8b-4193-8c9d-782971bdd8d8`

Database helper result:

- `reason_code=TIMESHEET_PRESENT_BUT_INVOICED`
- `source_complete=true`
- `document_ready=true`
- `preparation_required=false`

Catalog/browser result after deployed Recheck:

- View status: PENDING.
- Pending Action: 2.
- Ready: 0.
- Emails: 0.
- Passed checks: 0.
- Both rows non-selectable and default-excluded for `TIMESHEET_PRESENT_BUT_INVOICED`.
- Week header displays `Validation incomplete · 1 shift differs`.
- Review and Apply is disabled.
- Friendly `Timesheet present but invoiced` text is visible.

TSFIN proof:

- Before and after repeated real Rechecks, current/historical TSFIN row count remained 4.
- Digest remained `b319d490edf2c6f039cd1b619ad31a19`.
- This proves the corrected Recheck/catalog workflow did not mutate Sarah's TSFIN.

### Daily validation fixture

TEST import: `756ff897-1ef9-42b9-8fde-ab117b036a00`

Deployed catalog result:

- 20 separate missing Daily timesheets remain individual `DAILY_TIMESHEET_NOT_SUBMITTED` blockers.
- One existing invoiced Daily timesheet has both its email and validation row held with `TIMESHEET_PRESENT_BUT_INVOICED`.
- Both held rows are non-selectable.
- The hold is scoped to that one Daily timesheet and does not create a Weekly-style cross-timesheet hold.

### SUCCESS runtime proof

A live call to the deployed `import_review_list_v1` returned these current TEST display-status counts:

- ABANDONED: 17
- BLOCKED: 4
- READY: 1
- SUCCESS: 4
- SUPERSEDED: 30

This proves SUCCESS is operational in the installed database contract rather than merely present in source.

## Automated verification after deployment

- Focused validation-policy suite: 14/14 passed.
- Full backend test suite: 182/182 passed.
- Frontend `node --check` for `js/import-review-v1.js` and `js/main.js`: passed.
- Git whitespace check: passed before commit.
- Backend and frontend branches are clean and at their pushed remotes before this handover artifact is added.

The focused suite verifies:

- exact complete current document evidence;
- Daily isolation and Weekly atomic hold;
- exact email attachment re-attestation;
- asynchronous/idempotent document preparation;
- settled server SUCCESS;
- no Banking Pay/invoice writer/TSFIN writer scope drift;
- candidate-did-not-work constraints;
- Weekly hold badge and UI grouping contracts.

## Verification limitations

- No real query email was sent during the final post-deployment check. The exact email/attachment contract is covered by installed-source inspection and automated tests, while live TEST data available at verification time correctly blocked the current email examples because their timesheets were invoiced.
- No new incomplete electronic, signed QR or manual timesheet was fabricated in persistent TEST data. Their evidence contracts are enforced by the installed helper and focused automated source-contract tests.
- The old Sarah operation that previously reached `TSFIN_FOLLOW_UP_INCOMPLETE` was not retried. It remains historical evidence of the pre-fix defect, not a new post-fix failure.

## Hard scope boundary for the reviewer

The implementation deliberately did **not** modify:

- Banking Pay or any `pay_*` function;
- Policy X;
- payment drafts, CSV/provider execution, settlement or remittances;
- NHSP/HealthRoster import-authoritative effective-balance mathematics;
- invoice creation, issue/unissue, credit-note generation, totals, VAT or documents;
- TSFIN calculation/rates/rounding or general TSFIN Workers;
- Bulk Authorise;
- unrelated frontend modal infrastructure;
- production resources.

The reviewer must not recommend changes in those areas unless it demonstrates a direct, reproducible defect caused by this exact deployment. General hardening or architecture rewrites are out of scope.

## Required independent-review output

The reviewing chat must:

1. Fetch the current backend `test` and frontend `main` branches.
2. Verify the two commit SHAs and inspect their exact diffs.
3. Query the seven installed function definitions/hashes, including the restored contract gate, from `test-cloudtms`.
4. Re-run the 14 focused tests, 182 full backend tests and frontend syntax checks.
5. Recheck the Weekly atomic-hold, Daily isolation, evidence completeness, attachment re-attestation, asynchronous preparation and server SUCCESS contracts.
6. Confirm HealthRoster Daily and Weekly validation-only remain non-authoritative.
7. Confirm import-authoritative financial logic and Banking Pay are untouched.
8. State one of:
   - `FULLY IMPLEMENTED`, with evidence; or
   - `FURTHER TARGETED WORK REQUIRED`, followed by a function-by-function plan listing only functions/files that demonstrably still require changes.

The review must not treat limitations in available live fixtures as permission to rewrite unrelated systems. Any alleged defect must identify the exact function/file, failing state, current observed behaviour, required behaviour, and smallest compatible correction.

## Safety record

- Secrets printed: no.
- Production access/deploy: no.
- Banking/payment/provider execution: no.
- Destructive data deletion: no.
- Persistent TEST fixture fabrication: no.
- Normal TEST DB definitions changed: yes, explicitly authorised and listed above.
- Normal TEST Worker deployed: yes, explicitly authorised.
- TEST frontend published: yes, explicitly authorised.
- Raw Wrangler logs committed: no; temporary logs removed.
- Policy decision changes: none.
