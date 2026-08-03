# Import Review contract and invoice batch single-pass handover

## Outcome

Both requested defects are fixed, pushed to backend `test`, installed in `test-cloudtms`, and verified against deployed TEST.

- Import Review again proves the full `IMPORT_REVIEW_UI_V6` database contract. The obsolete short manifest can no longer overwrite the canonical manifest.
- Batch Generate and Batch Issue PAGE-family requests classify candidates once per request and reuse materialised candidate JSON for page rows, totals, selections, filters, blockers and ordering.
- Fixed-snapshot response JSON is unchanged before versus after the performance repair.
- Invoice creation/issue economics, totals, VAT, documents, emails, Banking Pay economics, frontend, Worker JavaScript, NHSP, HealthRoster and timesheet business logic were not changed.

## Root causes

### Import Review contract mismatch

Two repeatables owned `public.import_review_contract_version_get_v1()`:

1. the canonical rich definition in `25072026_1615_banking_pay_canonical_correction_carrier.sql`; and
2. an obsolete short definition in `21072026_1820_01_import_review_lifecycle_rpcs.sql`.

The lifecycle repeatable was installed later and removed these required proof fields:

```text
canonical_correction_carrier_version
targeted_family_materialisation_version
```

The Worker therefore failed closed even though the underlying objects existed.

### Batch Generate / Batch Issue latency

For PAGE, EXPAND_SELECTION and EXPLICIT_KEYS, each candidate-row function called a key helper that classified the complete candidate population, then called the same full classifier again before filtering to the requested page. Page size limited response JSON, not the duplicated work.

## Implemented changes

### Canonical Import Review manifest

- Removed the obsolete manifest and grants from `21072026_1820_01_import_review_lifecycle_rpcs.sql`.
- Marked `25072026_1615_banking_pay_canonical_correction_carrier.sql` as the sole canonical owner.
- Added tests proving exactly one repeatable owner and both required manifest fields.

### Single-pass Batch Generate / Issue

- Added private Generate and Issue key-row cores returning existing key data plus classified `candidate_json`.
- Preserved the original key helper signatures and return contracts as compatibility wrappers.
- PAGE-family candidate-row requests consume the key-row core once and reuse candidate JSON.
- FACETS and SUMMARY retain one direct classifier path. The paths are mutually exclusive by mode.
- Added guards against a PAGE-family second classifier call or a call to the old key wrapper.
- Changed the invoice async runtime-authority repeatable so the changed-repeatable workflow reinstalls the nested corrected definitions.

## Files changed

Runtime SQL:

- `supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql`
- `supabase/repeatable/25072026_1615_banking_pay_canonical_correction_carrier.sql`
- `supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_09_private_invoice_batch_generate_candidate_rows_v2.sql`
- `supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_10_private_invoice_batch_issue_candidate_rows_v2.sql`
- `supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1501_private_invoice_batch_generate_candidate_keys_v2.sql`
- `supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1501_private_invoice_batch_issue_candidate_keys_v2.sql`
- `supabase/repeatable/28072026_1609_invoice_async_v8_runtime_authority.sql`

Focused tests:

- `broker/test/import-review-follow-up-sql-contract.test.mjs`
- `broker/test/import-review-ui-sql-contract.test.mjs`
- `tests/banking-pay-canonical-correction-carrier.test.cjs`
- `tests/invoice-async-v8-sql-contract.test.js`

## Verification

### Automated tests

```text
npm test                                      183/183 passed
invoice-async-v8 focused SQL contracts         55/55 passed
combined invoice + carrier focused contracts   81/81 passed
git diff --check                               passed
```

The newly modified Import Review broker assertions pass. The whole standalone broker UI SQL-contract file retains two unrelated stale assertions that pre-date this work and are not part of repository `npm test`; neither concerns the manifest owner or invoice batch call graph.

### Fixed-snapshot output equivalence

The same PAGE request fixed at `2026-08-03 12:00:00+00` was evaluated before and after installation. Volatile `snapshot` and `query_hash` were removed before hashing.

| Action | Stable MD5 before | Stable MD5 after | Rows | Totals |
|---|---|---|---:|---|
| Generate | `0cf2f2f9079d41f34dc0e70e8241ff30` | `0cf2f2f9079d41f34dc0e70e8241ff30` | 17 | all 17, ready 4, blocked 13 |
| Issue | `8d340a89c7535f83709fb7a9281ed556` | `8d340a89c7535f83709fb7a9281ed556` | 32 | all 32, ready 1, blocked 31 |

Selection keys, rows, snapshots, blockers, totals and ordering therefore remain contractually identical for the fixed snapshot.

### Live performance evidence

| Action | Before | Initial after | Change |
|---|---:|---:|---:|
| Generate | 4612.75 ms | 2225.73 ms | 51.8% faster |
| Issue | 4405.97 ms | 3895.39 ms | 11.6% faster |

Subsequent `EXPLAIN (ANALYZE, TIMING OFF)` on the same 100-row PAGE query shape measured:

```text
Generate execution: 1678.540 ms
Issue execution:    3004.743 ms
```

Issue remains heavier because its one remaining classifier is intrinsically more expensive; the duplicate population-wide pass is removed without changing eligibility or economics.

### Live call-graph proof

Installed TEST definitions show:

- one Generate classifier call in the Generate key-row core;
- one Issue classifier call in the Issue key-row core;
- one direct classifier in each candidate-row function, gated to FACETS/SUMMARY;
- PAGE-family requests use the key-row core and carry `candidate_json`.

Because the mode gates are mutually exclusive, a PAGE request cannot execute both classifier sources.

### Import Review browser proof

The authenticated deployed TEST UI was opened after installation:

- the old “Worker cannot prove the required database contract” blocker was absent;
- “Retry contract check” was absent;
- the modal displayed `✓ Approved contract IMPORT_REVIEW_UI_V6`;
- NHSP Weekly, HealthRoster Weekly, HealthRoster Daily and saved reviews were available.

The live manifest again returns:

```text
canonical_correction_carrier_version = BANKING_PAY_CANONICAL_CORRECTION_CARRIER_V1
targeted_family_materialisation_version = BANKING_PAY_TARGETED_FAMILY_MATERIALISATION_V1
```

## Deployment

```text
fc36fecbc0f90d67cc4d70e2bd78895eb1a200de
  Fix import contract and batch candidate performance

89d8b8bd2ac366190a84b1cd1ecbf1b7d6515b52
  Reinstall invoice batch runtime authority
```

Both were pushed to `origin/test`. Supabase Migrate Actions runs `30797429462` and `30797661091` succeeded.

The second commit was necessary because deployment tracks changed top-level repeatables; changing runtime authority forced installation of the corrected nested functions. Live hashes were checked after that run.

No frontend or Worker JavaScript changed, so no frontend or Cloudflare Worker deployment was required.

## Safety and boundaries

- Target: TEST only (`test-cloudtms`).
- Production deployment: no.
- Business-row mutation: no.
- Invoice generation/issue operation: no.
- Payment/CSV/provider/remittance action: no.
- Email/document generation: no.
- Secrets printed or committed: no.
- Policy X drift: none.
- Unrelated worktree changes overwritten or committed: no.

## Independent-review instruction

Review the exact commits and deployed definitions above. If any defect remains, provide a function-by-function implementation plan only for functions demonstrably requiring work. Do not expand into invoice economics, VAT, documents, email delivery, Banking Pay, payments, frontend, Workers, timesheets, NHSP, HealthRoster or unrelated database functions.
