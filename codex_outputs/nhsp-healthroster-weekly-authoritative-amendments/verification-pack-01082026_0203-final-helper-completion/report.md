# NHSP/HealthRoster Weekly Authoritative Amendments - final helper completion handover

## Executive verdict

The targeted final completion is implemented, committed, pushed, and deployed to TEST.

Production code changed in exactly one function:

    public._import_review_effective_invoice_balance_core_v1(
      uuid,
      jsonb,
      integer,
      integer,
      integer,
      integer
    )

Canonical source:

    supabase/repeatable/21072026_1820_00_import_review_internal_core.sql

GitHub TEST commit:

    fbac04ca040af0d63f002996b6e5c2a15a1c113b
    Complete authoritative balance edge evidence

TEST Supabase deployment record:

    20260801010059
    complete_authoritative_balance_edge_evidence_01082026_0158

Live TEST helper MD5 before installation:

    270da78518d8c18a10f260ec7eb4f623

Live TEST helper MD5 after installation:

    d2fa598794684bbde7176919eb209942

The installed hash exactly equals the definition compiled from the committed source in disposable PostgreSQL 18.

No Worker or frontend deployment was required or performed. No Banking Pay, invoice-writing, credit-note-writing, TSFIN-calculation, phase-3, caller, Daily, or validation-only function was changed.

## Locked scope

This remains solely an Import Review evidence-and-routing correction for:

1. NHSP Weekly import-authoritative processing.
2. HealthRoster Weekly import-authoritative processing.

It does not apply to:

- HealthRoster Daily.
- HealthRoster Weekly validation-only.
- ordinary non-authoritative imports.
- Banking Pay.

Banking Pay remains a downstream consumer of valid outstanding financial records. It does not decide which correction timesheets an authoritative import requires.

## Final defects addressed

### 1. Historical operation discovery across later imports

Prior defect:

A later import replaces the source shift's latest_import_id. If an older invoiced correction member had then been physically deleted together with its TSFIN and audit evidence, the helper could miss the older completed operation and omit its surviving invoice line from B.

Implemented correction:

- Candidate operations now come from the existing bounded import-ID path and the exact immutable shift/action/outcome path.
- The additional path is:

      import_review_decisions.shift_id
      -> import_review_action_outcomes.action_id
      -> operation_id
      -> import_apply_operations.id

- Source system, source identity, shift, candidate, client, contract, request unit, applied result, policy unit, and all relevant fingerprints still have to agree.
- The existing per-source maximum operation bound remains enforced.
- Operations prove ownership only. They never contribute money directly.
- Validated member IDs are added before invoice-line scope is constructed.

Result:

Older completed generations remain discoverable after a later import becomes latest, while invalid, conflicting, failed, cancelled, or uncommitted operation evidence still fails closed or is excluded as appropriate.

### 2. Legitimate archived-sibling repair supersession

Prior defect:

A valid archived-member repair can deliberately re-key a surviving physical member from an old correction ID to a fresh correction ID. A later review could see the same member in old and new completed-operation evidence and treat that approved transition as contradictory ownership.

Implemented correction:

- The helper constructs a narrow supersession edge only where the later completed operation proves all of:

      route = AMEND_EXISTING_REPLACEMENT
      repair_identity_mode = FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED
      reviewed existing correction ID = old correction ID
      applied correction ID = new correction ID
      same source
      same physical member
      same correction role
      valid request/applied/policy fingerprints
      member was among the reviewed existing members
      no invoice line existed for that member at or before repair finalisation

- The old assignment remains audit history.
- The new assignment becomes canonical ownership.
- Role changes, source changes, unsupported dual economics, and unexplained re-keying remain conflicts.
- The supersession map participates in evidence and reconciliation fingerprints.

Result:

A valid archived/live repair survives later review without weakening fail-closed rules for genuinely contradictory ownership.

### 3. Multi-source aggregate credit allocation

Prior defect:

For a credit of a Weekly-hours invoice line containing several frozen source segments, the helper could use the exact source segment's hours but allocate the entire credit line's money to that one source.

Implemented correction:

- The credit must be a complete mirror of the original physical invoice line under the established writer contract.
- Its stored pay, charge, and margin must match the negative original totals to currency precision.
- Its hour buckets must match the original writer shape.
- Exactly one frozen source segment must match the source.
- A multi-segment component must contain exact segment pay and charge.
- For a multi-segment original:

      credit source hours  = negative exact original segment hours
      credit source pay    = negative exact original segment pay
      credit source charge = negative exact original segment charge
      credit source margin = credit source charge - credit source pay

- Whole-line signed credit totals remain valid only for a proved single-source line.
- Ambiguous or non-mirroring credits fail closed.

Result:

Every source receives only its own frozen signed hours and financial values. No source can receive the full aggregate credit economics.

### 4. Zero hours with non-zero money

Prior defect:

A signed Weekly-hours ledger could net all hour buckets to zero while retaining a non-zero pay, charge, or margin residual. That state could be considered standard-representable and fall through to an ordinary amendment route.

Implemented correction:

- Hours are zero only if every hour bucket is zero.
- Money is zero only if pay, charge, and margin each round to zero at two decimal places.
- A zero-hour/non-zero-money position is not standard-representable.
- It returns the existing friendly blocker:

      IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE

- The hours-zero and money-zero state participates in reconciliation fingerprinting.
- The helper exposes effective_money_net_is_zero for evidence and diagnostics.

Result:

A hidden monetary residual can no longer be treated as an ordinary zero balance.

## Function-by-function implementation verdict

| Function | Current verdict | Code change in this completion |
| --- | --- | --- |
| _import_review_effective_invoice_balance_core_v1 | Final four evidence/ledger corrections implemented | Yes |
| _import_review_action_catalog_core_v1 | Existing blocker precedence and routes are compatible | No |
| _import_review_apply_envelope_core_v1 | Existing reviewed evidence envelope remains compatible | No |
| import_review_correction_generation_transition_v1 | Existing request/applied/policy transition remains compatible | No |
| timesheet_paid_uninvoiced_rollover_v1 | Existing paid-uninvoiced route remains compatible | No |
| hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) | Existing zero/one/two-member repair remains compatible | No |
| nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) | Existing repair contract remains compatible | No |
| hr_weekly_apply_transactional(uuid,jsonb,uuid) | Existing orchestration remains compatible | No |
| nhsp_weekly_apply_transactional(uuid,jsonb,uuid) | Existing orchestration remains compatible | No |

## Protected hash verification

The eight adjacent TEST functions were recorded immediately before installation and re-read immediately afterwards. Their hashes did not change.

| Function | Pre-install MD5 | Post-install MD5 |
| --- | --- | --- |
| _import_review_action_catalog_core_v1 | 1623934a80bd09d263588271addd9ffd | 1623934a80bd09d263588271addd9ffd |
| _import_review_apply_envelope_core_v1 | 3fad3522f7d3d0aaa464d8a32b20ebd9 | 3fad3522f7d3d0aaa464d8a32b20ebd9 |
| import_review_correction_generation_transition_v1 | 1d3481867a4326c471812ef7c6326f76 | 1d3481867a4326c471812ef7c6326f76 |
| timesheet_paid_uninvoiced_rollover_v1 | 2632b3b506dcd2bd06a77dddad01e76d | 2632b3b506dcd2bd06a77dddad01e76d |
| hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) | 788e9d8926c91cf654a9d36634944d94 | 788e9d8926c91cf654a9d36634944d94 |
| nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) | 5a35c7cd476e580841eb8c8a060d3992 | 5a35c7cd476e580841eb8c8a060d3992 |
| hr_weekly_apply_transactional | d57dea07edca42d3ae41b56b37ded723 | d57dea07edca42d3ae41b56b37ded723 |
| nhsp_weekly_apply_transactional | 505bfc530f49e8e79233d1e468271e49 | 505bfc530f49e8e79233d1e468271e49 |

Installed helper metadata:

- Owner: postgres.
- Security mode: SECURITY INVOKER.
- search_path: public, extensions, pg_temp.
- Direct EXECUTE for public, anon, authenticated, and service_role: false.
- Exact installed signature count: one.

## Verification performed

### Focused source-contract suite

Command:

    node --test tests/import-authoritative-effective-balance-helper.test.cjs

Result:

    23 tests
    23 passed
    0 failed

Complete output: source-contract-test-output.txt.

### Backend regression suite

Equivalent command:

    node --test tests/*.test.js

Result:

    162 tests
    162 passed
    0 failed

Complete output: backend-test-output.txt.

### Executable PostgreSQL fixtures

A fresh disposable PostgreSQL 18 database was created from the included minimal runtime schema. The fixture SQL ran inside a transaction and rolled back.

The four final cases ran independently for NHSP and HealthRoster:

1. Cross-import deleted-member reconstruction.
2. Archived-member repair supersession.
3. Multi-source aggregate credit allocation.
4. Zero-hours/non-zero-money blocking.

Result:

    8 scenario executions
    8 passed
    0 failed
    transaction rolled back

Included files:

- 01082026_0150_import_authoritative_effective_balance_runtime_schema.sql
- 01082026_0150_import_authoritative_effective_balance_runtime_fixtures.sql
- runtime-fixture-output.txt

The fixture file explicitly warns that it is destructive only to its disposable fixture tables and must not be run against CloudTMS TEST or production.

### TEST deployment gate

Immediately before installation:

    import_apply_operations total = 29
    unsafe/non-complete operations = 0

The live helper remained at the recorded pre-install hash before atomic deployment.

### Post-install live smoke

Read-only action-catalog calls completed successfully for recent authoritative source imports.

HealthRoster:

    action rows = 11
    amendment rows = 0
    blocked rows = 10
    selectable rows = 1

NHSP:

    action rows = 26
    amendment rows = 2
    blocked rows = 23
    selectable rows = 3

These calls proved the deployed helper compiles and operates through the unchanged catalog for both source families. No action was selected or applied and no business row was mutated.

## Deployment method

The exact committed helper definition was installed atomically in TEST Supabase with:

- CREATE OR REPLACE FUNCTION for the exact signature.
- owner restored/verified as postgres.
- direct execution revoked from public, anon, authenticated, and service_role.
- no drop and no signature change.

No normal TEST Worker deployment was performed because the changed runtime is a PostgreSQL function, not Worker code.

No frontend deployment was performed.

No production resource was accessed or changed.

## Rollback

The pack includes:

    01082026_0203_effective_balance_helper_rollback.sql

It restores the exact pre-install helper whose live pg_get_functiondef MD5 was:

    270da78518d8c18a10f260ec7eb4f623

It was not executed.

It is emergency rollback only. Applying it intentionally removes the four final corrections. The older nine-function rollback must not be used for this completion.

## Files in the GitHub runtime commit

Changed:

- supabase/repeatable/21072026_1820_00_import_review_internal_core.sql
- tests/import-authoritative-effective-balance-helper.test.cjs
- tests/01082026_0150_import_authoritative_effective_balance_runtime_schema.sql
- tests/01082026_0150_import_authoritative_effective_balance_runtime_fixtures.sql

Deliberately untouched:

- all Banking Pay files and pay_* functions;
- all invoice writers, issue/unissue and credit-note functions;
- TSFIN calculation and Worker code;
- action catalog and apply envelope;
- phase-3 functions;
- Weekly transactional callers;
- paid rollover and lifecycle transition;
- frontend and Bulk Authorise;
- HealthRoster Daily;
- HealthRoster Weekly validation-only.

Unrelated concurrent working-tree files were excluded from both commits.

## Evidence limitations stated precisely

Current live TEST data does not contain a completed real operation carrying the new top-level reconciliation-unit evidence model. Therefore no destructive live Import Review apply was manufactured solely for verification.

Instead:

- the exact installed function was exercised through current read-only NHSP and HealthRoster catalog calls;
- destructive edge cases were executed against a fresh disposable PostgreSQL database using included schema and fixtures;
- immutable artifact and role/economic assertions are executable and independently reproducible from this pack.

This is not a claim that live TEST happened to contain every destructive anomaly. It is a claim that the committed/deployed helper passed the explicit disposable database cases and live compile/catalog smoke.

## Independent verifier instruction

The independent verifier must inspect commit fbac04ca040af0d63f002996b6e5c2a15a1c113b and the current deployed TEST definition.

The verifier must:

1. Confirm the pack helper and GitHub commit match.
2. Confirm current live helper hash d2fa598794684bbde7176919eb209942.
3. Re-run included source-contract and disposable database fixtures.
4. Review the four exact repairs described above.
5. Confirm the eight protected functions remain unchanged and compatible.
6. Confirm no Banking Pay, invoice-writer, TSFIN-calculation, frontend, Worker, Daily, or validation-only code entered the implementation.
7. Confirm rollback restores only the pre-change helper.

If further work is demonstrated, the verifier must provide a highly detailed function-by-function implementation plan only for functions with a specific evidenced defect needed to fulfil this brief.

The verifier must not:

- treat this handover as authority for a broad rewrite;
- open Banking Pay or Policy X;
- change invoice generation, issue/unissue, credits, VAT, documents, or totals;
- change TSFIN calculations, rates, rounding, queues, or Workers;
- change frontend, phase 3, Weekly callers, Daily, or validation-only routes without a separately demonstrated defect directly required by this brief;
- propose speculative infrastructure work.

If no remaining defect is demonstrated, the correct verdict is fully implemented.

## Safety and release summary

- TEST Supabase changed: yes, exact one-function DDL explicitly authorised.
- TEST business rows mutated: no.
- Production accessed or deployed: no.
- Worker deployed: no.
- Frontend deployed: no.
- Banking Pay changed: no.
- Policy X changed: no.
- Invoice/credit writer changed: no.
- TSFIN calculation changed: no.
- Destructive live Import Review action run: no.
- Disposable local fixture data created and rolled back: yes.
- Secrets printed or committed: no.
- Raw sensitive diagnostic data committed: no.
- GitHub pushed: yes.
- Runtime commit: fbac04ca040af0d63f002996b6e5c2a15a1c113b.
- TEST live helper hash: d2fa598794684bbde7176919eb209942.
