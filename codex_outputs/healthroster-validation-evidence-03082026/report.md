# CloudTMS HealthRoster validation evidence — implementation and independent-verification handover

Generated: 03/08/2026 (Europe/London)

## Verification instruction

Independently verify the committed code, installed TEST definitions and the evidence below. Report a function-by-function verdict. If anything remains necessary to meet this brief, provide a tightly scoped function-by-function implementation plan containing only the functions/files that demonstrably require further work. Do not use this handover to broaden the work into Banking Pay, invoice economics, TSFIN calculation, HealthRoster authoritative mutation policy, or unrelated modal architecture.

## Outcome and source identity

- Backend branch: `test`
- Implementation commit: `5aadf9039db5375f16c06989e50f38955c2cdacc`
- Commit message: `Complete HealthRoster validation email evidence`
- Remote `origin/test` was confirmed at the same SHA after push.
- Frontend branch: `main`, unchanged by this implementation; working tree was clean at `4aaafca`.
- Target database: Supabase project `test-cloudtms` (`yakevhtttcsljosbdpov`), confirmed `ACTIVE_HEALTHY`.
- Normal TEST Worker JavaScript did not change and was not redeployed.
- Production was not accessed or deployed.

## Locked policy implemented

- HealthRoster Weekly validation-only and HealthRoster Daily are validation workflows. They do not silently amend candidate timesheet hours.
- A mismatch can produce a grouped query email only when a complete timesheet document is available as evidence.
- A stale or missing electronic document is queued for immediate asynchronous generation; the Import Review modal remains usable and polls automatically until the document becomes ready. The user does not need to press Recheck.
- Review and Apply cannot commit the email send while required document evidence is unavailable or stale.
- The query email uses the requested greeting and wording, the configured agency name, one deduplicated reference, and the generated/signed timesheet attachment.
- The HTML table has separate `Unit / ward` and `Request grade` columns for both Daily and Weekly HealthRoster evidence.
- A successful Daily validation writes the chosen HealthRoster request ID to the active Daily timesheet's `reference_number` during validation Apply, subject to the existing paid/invoice-lock protections. It is not inserted by invoicing.
- Weekly validation writes matched HealthRoster references to the relevant shift evidence during successful validation Apply.
- Changing the timesheet reference is included in the existing timesheet-document invalidation trigger. An electronic document must therefore be regenerated with the validated reference before subsequent attachment/use.
- Sending a mismatch/query email does not itself validate the timesheet and does not add a new reference.
- Partial reviews fingerprint only actions that remain open; already committed outcomes are not reintroduced during final Apply re-attestation.
- No business-policy decision was changed during the final implementation/testing cycle.

## Files changed

### `supabase/repeatable/21072026_1820_00_import_review_internal_core.sql`

Two existing functions are amended in the canonical repeatable file.

#### `public._import_review_action_catalog_core_v1`

- Builds Daily query-email comparison evidence rather than emitting an under-specified issue row.
- Carries Daily timesheet and HealthRoster start/end/break values.
- Carries Daily `Unit`, hospital/trust and `Request Grade` evidence from the imported payload and established normalised fields.
- Moves Daily query evidence to `issue-evidence-v2`, making the displayed/emailed comparison part of the immutable evidence fingerprint.
- Enriches Weekly email comparison rows from the exact `hr_rows.id` with Unit/ward, hospital/trust and Request Grade.
- Retains the validation-only financial boundary.

#### `public._import_review_refresh_core_v1`

- Uses a stable `daily-resolution-evidence-v1` fingerprint based on source identity, row/timesheet ownership and material imported/current/mapping evidence.
- Uses the same calculation when retiring stale Daily resolutions.
- Prevents refreshed UI-only action-envelope details from needlessly invalidating an otherwise identical Daily association.

### `supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql`

#### `public.import_review_apply_guard_v1`

- After regenerating the current action catalogue, removes action IDs already represented by committed `import_review_action_outcomes` for that import before calculating the final fresh fingerprint.
- Aligns Apply re-attestation with refresh behaviour for partial reviews.
- Prevents a successful earlier portion of a review from being reintroduced as stale/open work when the remaining email action is applied later.

### `supabase/repeatable/21072026_1820_04_timesheet_query_email_rpcs.sql`

#### `public.timesheet_query_email_enqueue_v1`

- Uses: `Good morning/afternoon, Please can you kindly make amendments on HealthRoster for the below shifts. The relevant timesheets have been attached to this email.`
- Keeps the configured agency name in the sign-off.
- Renders separate HTML columns for `Unit / ward` and `Request grade`.
- Renders the same source detail in plain text.
- Keeps one reference where before/after reference values are identical.
- Does not change the existing document-readiness, evidence-fingerprint, recipient-route, idempotency or attachment controls.

### Tests

- `tests/import-review-validation-evidence-integrity.test.cjs`
- `tests/import-review-validation-only-policy.test.cjs`
- `broker/test/import-review-ui-sql-contract.test.mjs` (updated only for the deliberately revised email wording)

## TEST database installation proof

Fresh read-only `pg_get_functiondef` inspection after installation showed:

| Function | Installed MD5 | Expected implementation marker |
|---|---:|---|
| `_import_review_action_catalog_core_v1` | `077d0ab764d67f015261ab55c414591a` | `issue-evidence-v2` present |
| `_import_review_refresh_core_v1` | `1d82ea822c8d34c2f7090fd380e47df1` | `daily-resolution-evidence-v1` present |
| `import_review_apply_guard_v1` | `0392c8e8799285f3b773667d6c7477db` | committed-outcome filtering present |
| `timesheet_query_email_enqueue_v1` | `e3be3be6cbe67293c9f9ca8fff2227b5` | email wording and Unit/grade columns present |

All inspected definitions are owned by `postgres`, are `SECURITY DEFINER`, and use `search_path=public, extensions, pg_temp`.

## Executed verification

### Source tests

- Focused validation suites: **19/19 passed**.
- `git diff --check`: passed.
- Four broader Import Review suites passed individually:
  - `import-review-index-integration.test.mjs`
  - `import-review-follow-up.test.mjs`
  - `import-review-follow-up-sql-contract.test.mjs`
  - `import-review-contract.test.mjs`
- `import-review-ui-sql-contract.test.mjs` still contains three pre-existing stale assertions unrelated to this delta (a missing legacy contract-version definition, an old transition-items formatting assertion, and an old candidate binding assertion). The deliberately changed email-introduction assertion now matches the approved wording. These unrelated failures were not concealed or altered.

### Daily email and forced asynchronous regeneration

- TEST timesheet: `dc19abfa-8da2-404a-804f-aa122b66f3cf` (Electronic, Daily, fully signed).
- Its existing ready document revision was deliberately invalidated under the user's explicit TEST authority.
- Document state moved from revision 27 `READY` to revision 28 `STALE`; the current document-version pointer was cleared.
- TEST import: `375a662b-2347-4948-a2a6-937e076b6619`.
- Async document operation: `d864ccdb-1731-430e-81f9-2920ef2c6c17`, completed successfully.
- New document version: `7409d0c1-a93f-42e1-8c35-d33116628e6f`, revision 28 `READY`.
- The open Import Review modal polled automatically several times without Recheck and enabled Apply once the document became ready.
- Mail outbox: `100916ee-b4ac-4f32-b353-c4d16a68be30`, `SENT/ACCEPTED`.
- Read-only DB proof showed exactly one attachment and that its R2 key equalled the new current document version's R2 key.
- Email comparison contained the populated timesheet/HealthRoster values, reference, Unit/ward and Request grade.
- Import and follow-up completed.

This proves the complete missing/stale electronic-document path: immediate asynchronous queueing, regeneration, automatic modal polling, Apply availability, and attachment of the newly generated PDF.

### Additional Daily email proof

- Import: `2aec2ef5-436c-4ac0-938e-ddaf9d5e9983`.
- Operation: `365a7216-3f18-4dca-8b8e-0daac78a9a74`.
- Mail outbox: `8b774c9d-6aae-43ea-bf04-60b426b05e52`, `SENT/ACCEPTED`.
- One PDF attachment.
- Daily values were populated (`07:30–20:00` versus `07:30–19:00`) with reference `0726049648` shown once.
- Unit/ward and Request grade were populated from source evidence.

### Weekly email proof

- Import: `72c78d35-30d5-4fcf-8069-7055c93751bf`.
- Mail outbox: `1188d0ad-7ba8-4e97-9f0a-b9877b21dd09`, `SENT/ACCEPTED`.
- One timesheet attachment.
- Source-specific `Unit / ward` (`Health Vis Hornsey Cent`) and `Request grade` (`BANK - Band 6 HV`) were present.
- Reference `726621746` appeared once.
- Import reached `APPLIED`; follow-up reached `COMPLETE`.

### Safety and boundaries

- No payment, provider, Banking Pay, settlement or remittance operation was run.
- No invoice writer or TSFIN-calculation function was changed.
- Emails were confined to the user-authorised TEST recipient domain.
- No secrets were printed or committed.
- No production resource was accessed.

## What the verifier must check

1. Confirm commit `5aadf9039db5375f16c06989e50f38955c2cdacc` contains exactly the six files above and no Banking Pay/invoice/TSFIN writer code.
2. Compare the four installed TEST definitions with the canonical definitions in the commit.
3. Confirm Daily and Weekly email comparison evidence is source-specific and fingerprinted.
4. Confirm reference deduplication does not discard differing before/after values.
5. Confirm attachment evidence is still re-attested immediately before enqueue and one document is deduplicated per timesheet.
6. Confirm the partial-Apply outcome exclusion is identical in meaning to refresh's open-work fingerprint policy and does not suppress genuinely open work.
7. Confirm Daily resolution evidence does not become stable across a material change in source/timesheet ownership, schedule, mapping or authority.
8. Independently reproduce the Daily forced-regeneration path and inspect the resulting attachment.
9. Independently reproduce one Weekly validation query email.
10. Verify HealthRoster Daily and Weekly validation-only remain non-authoritative and do not amend hours.
11. Report every inspected function individually. If a defect is demonstrated, give the smallest exact function/file change needed. Do not propose a broad rewrite.

## Known limitations / not claimed

- A freshly created signed QR document and a newly uploaded manual document were not destructively re-created during this final run. Their existing readiness/evidence resolver is unchanged and remains covered by source-contract tests, but those two document modes should be included in independent regression verification.
- The broad `import-review-ui-sql-contract.test.mjs` suite has the three unrelated stale assertions listed above; they predate this delta and require a separate test-maintenance decision rather than production changes.
- No production deployment was performed.

## Separate performance finding: invoice Batch Generate / Batch Issue modal

This was diagnosed only; no invoice function was changed.

- Live UI first-page timings were approximately 4.15 seconds for Batch Generate (17 returned rows) and 4.14 seconds for Batch Issue (32 returned rows).
- Read-only `EXPLAIN ANALYZE` measured approximately 3.66 seconds / 84,421 buffer hits for the Generate candidate-row query and 4.00 seconds / 169,862 buffer hits for the Issue candidate-row query.
- The principal problem is server-side, not rendering the returned rows. PAGE mode computes the system-wide classification to produce candidate keys and then computes the authoritative classification again before filtering to the first page.
- `page_size=100` therefore bounds the returned rows, but does not bound the expensive classification work. This will scale poorly as the total eligible population approaches 1,000.
- Any future fix should remove the duplicate full-scope classification while preserving the existing snapshot, filter, total and selection semantics. That is a separate invoice-performance brief and must not be mixed into this HealthRoster validation change.

## Reference-number lifecycle answer

The validated reference is stored during successful validation Apply. It is not inserted by invoicing. For an electronic timesheet, the existing document invalidation trigger treats `reference_number` as document-bearing truth, so the current electronic PDF is made stale and regenerated asynchronously. Invoice generation then consumes the already reference-bearing current timesheet/document; it is not the step that establishes the reference.
