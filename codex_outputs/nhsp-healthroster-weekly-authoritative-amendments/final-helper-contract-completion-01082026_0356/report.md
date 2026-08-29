# NHSP/HealthRoster Weekly Authoritative Amendments

## Final helper-contract completion handover

Generated: 01/08/2026 03:56 Europe/London

Implementation commit:

```text
cffab09d284a690c5130b6821bef53a50f1dbcdc
Complete authoritative balance evidence contracts
```

Remote branch:

```text
kierarthur/cloudtms-backend test
```

Deployment:

```text
GitHub Actions run 30680863468
Supabase Migrate (safe)
conclusion: success
target: TEST Supabase only
```

## Executive result

The final targeted correction has been implemented, committed, pushed and installed on TEST.

The only production function changed by this implementation is:

```text
public._import_review_effective_invoice_balance_core_v1(
  uuid,jsonb,integer,integer,integer,integer
)
```

Its canonical definition remains in:

```text
supabase/repeatable/21072026_1820_00_import_review_internal_core.sql
```

No Banking Pay function, invoice writer, credit writer, TSFIN calculator, Worker, frontend, phase-3 function, Weekly caller, HealthRoster Daily route or HealthRoster validation-only route was changed.

## Why this final correction was required

The preceding helper could already reconstruct most effective invoice history, archived/mutable generations and full-credit source safety. Independent verification found four remaining defects, followed by a final producer-contract clarification:

1. An older completed correction operation could be missed after a later import replaced `nhsp_shifts.latest_import_id`.
2. A legitimate archived-sibling repair could later look like contradictory ownership because the surviving physical member had moved to a fresh correction ID.
3. A credit against a multi-source Weekly invoice line used source-specific hours but could allocate the whole invoice line's money to one source.
4. A ledger with zero net hours but non-zero pay/charge/margin could fall through to an ordinary source amendment.
5. A fresh request produced by the real apply envelope has no repair mode, while phase 3 records `CREATE_NEW_GENERATION`; the historical evidence validator had to accept that exact route-aware pair without weakening mutable-repair validation.

## Implemented production logic

### 1. Cross-import historical operation discovery

The helper now builds a bounded set of candidate operation IDs from both:

```text
known import IDs

UNION

import_review_decisions.shift_id
  -> import_review_action_outcomes.action_id/operation_id
  -> import_apply_operations.id
```

Decision and outcome evidence must agree on source identity, source shift, candidate, client, contract and action kind. The existing `p_max_operations_per_source` bound remains authoritative.

This permits a completed operation from Import A to remain discoverable when Import B has become the source shift's latest import.

### 2. Durable operation re-attestation

Every historical operation is validated independently. Historical authority requires:

```text
state = COMPLETE
committed_at_utc is not null
finalised_at_utc is not null
exactly one matching request unit
exactly one matching applied unit
exactly one matching policy unit
exactly one matching action outcome
```

The helper cross-checks:

```text
action ID
source system and source identity
source shift and root/source timesheet
candidate, client, contract and week
invoice stream
source-scope fingerprint
exact action-outcome evidence fingerprint
unit-v2 fingerprint
reviewed-unit fingerprint
reconciliation fingerprint
policy-envelope fingerprint
operation-contract fingerprint
applied-result fingerprint
```

Incomplete or invalid operations do not become immutable historical authority. Active committed operations remain in-progress evidence. Contradictory completed evidence fails closed.

### 3. Route-aware repair identity validation

For `CREATE_REVERSAL_REPLACEMENT`:

```text
request repair mode = null/blank (or explicit CREATE_NEW_GENERATION)
applied repair mode = CREATE_NEW_GENERATION
reviewed existing correction ID = null
```

For `AMEND_EXISTING_REPLACEMENT`:

```text
request repair mode = RETAIN_EXISTING_CORRECTION_ID
  or FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED
applied repair mode = exact request mode
reviewed existing correction ID is required
```

Retain mode must retain the correction ID. Fresh archived-role repair must change it and must prove that a reviewed active member survives into the applied pair.

The focused source test reads the real apply-envelope source and both real phase-3 source files. The PostgreSQL fixture also executes the real `_import_review_apply_envelope_core_v1` for NHSP and HealthRoster and validates its exact `unit-v2` output.

### 4. Transitive archived-member supersession

A valid archived-sibling repair records a correction-ID edge such as:

```text
C1 -> C2
```

Successive valid repairs may form:

```text
C1 -> C2 -> C3
```

The helper now computes a bounded recursive closure by physical member and correction role. Earlier assignments remain audit evidence; the terminal assignment becomes canonical.

It fails closed on:

```text
branches
cycles
multiple terminals
depth-cap continuation
role changes
source changes
unsupported dual economic ownership
```

An archived-role supersession edge is accepted only where no effective or pending Weekly economic component existed before that repair. Known separable non-hours lines do not become financial authority.

### 5. Exact credit provenance and source-segment allocation

A credit is admitted only where the helper proves:

```text
credit header.original_invoice_id = original line.invoice_id
credit line original_invoice_line_id = original physical line ID
credit invoice client = original invoice client
credit/original source identity is exact
credit is a complete signed monetary mirror of the original physical line
```

For a multi-source frozen Weekly line, the credit component for one source uses:

```text
hours  = negative exact original segment hours
pay    = negative exact original segment pay
charge = negative exact original segment charge
margin = charge - pay
```

Whole credit-line totals are used only for a proven single-source original. Ambiguous, partial or contradictory credits fail closed; invoice and credit artifacts are never mutated.

### 6. Zero-hours monetary residual

The helper separately establishes:

```text
hours_zero
money_zero (pay, charge and margin rounded to stored 2dp precision)
```

If hours net to zero but money does not, the result is:

```text
B_standard_representable = false
blocking_code = IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE
```

The existing catalog blocker precedence and existing friendly frontend message handle the result. No catalog or frontend change was needed.

### 7. Fingerprints and deterministic evidence

The existing output signature is unchanged. Evidence and reconciliation fingerprints now include the expanded operation evidence, transitive correction lineage, exact role ownership, credit allocation and effective-zero classification.

Identical durable evidence returns identical output. Invoice amounts are counted once by physical invoice-line ID; operations prove identity and never contribute money.

## Function-by-function implementation verdict

| Function | Result | Change |
| --- | --- | --- |
| `_import_review_effective_invoice_balance_core_v1` | Final evidence/ledger correction implemented and deployed | Yes |
| `_import_review_action_catalog_core_v1` | Existing blocker precedence and route contract remain compatible | No |
| `_import_review_apply_envelope_core_v1` | Existing request producer retained; exercised by real runtime fixture | No |
| `import_review_correction_generation_transition_v1` | Existing request/applied/policy transition retained | No |
| `timesheet_paid_uninvoiced_rollover_v1` | Existing paid-uninvoiced path retained | No |
| `hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)` | Existing repair/result producer retained | No |
| `nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)` | Existing repair/result producer retained | No |
| `hr_weekly_apply_transactional` | Existing orchestration retained | No |
| `nhsp_weekly_apply_transactional` | Existing orchestration retained | No |

## Verification performed

### Focused Node source/contract suite

```text
31/31 passed
0 failed
```

This includes 28 helper/producer/scope tests and 3 existing correction PAID_DATE policy tests.

### Executable PostgreSQL 18 fixture suite

The canonical helper in the runtime schema is byte-identical after newline normalization to the committed canonical helper.

All fixtures run inside `BEGIN ... ROLLBACK` on an isolated disposable local PostgreSQL database.

```text
18 NHSP/HealthRoster group executions passed
30 distinct source/case scenarios passed
```

Covered scenarios include:

```text
cross-import deleted reversal/replacement reconstruction
archived/live fresh-ID repair
transitive C1 -> C2 -> C3 supersession
invalid route/mode combinations
wrong invoice stream
wrong source-scope fingerprint
wrong action-outcome evidence fingerprint
malformed credit header and source provenance
multi-source credit source allocation
zero-hours/non-zero-money blocking
repeated +10, -10 +11, -11 +12, -12 +10 arithmetic
real apply-envelope production request generation for NHSP and HealthRoster
```

### Repository-wide suite

```text
326 tests
301 passed
8 failed
17 skipped
```

The eight failures are pre-existing tests in unrelated Banking Pay or invoice/PDF areas. No file or runtime function implicated by those failures is changed by `cffab09`. The exact failure names are included in `outputs/repository-wide-tests-summary.txt` and must not be treated as authority to broaden this Import Review correction.

### TEST deployment and parity

Pre-deploy helper MD5:

```text
d2fa598794684bbde7176919eb209942
```

Post-deploy helper MD5:

```text
feaf3ed9910f2dc0f8132b406f8f5cd0
```

Committed versus live normalized PL/pgSQL body:

```text
exact match: yes
SHA-256: fac36d9aa4af0078d8760bbb00573d495019c5f2fc4119a87a442dae354d9e35
```

Repeatable Git blob versus TEST deployment ledger:

```text
exact match: yes
SHA-256: 93e6850be5b9c87ae5f1905279fde98df33ba5dcfd3c57d4270c023c70e9de80
```

Installed helper metadata:

```text
owner: postgres
security: SECURITY INVOKER
search_path: public, extensions, pg_temp
ACL: postgres only
exact signature count: 1
```

The other eight protected function MD5 values are unchanged from the pre-deployment baseline.

### Live safe catalog smoke

The latest currently materialised imports contain no amendment rows, so destructive correction routes cannot be exercised safely against current business-like TEST data. Both catalog calls nevertheless compile and complete:

```text
HealthRoster: 11 action rows; 0 APPLY_AMENDMENT rows
NHSP:         26 action rows; 0 APPLY_AMENDMENT rows
```

The executable local database fixtures are the acceptance evidence for the destructive edge cases.

## Rollback

The rollback file restores only the exact pre-deployment helper definition whose live MD5 was observed as `d2fa598794684bbde7176919eb209942`:

```text
01082026_0356_authoritative_effective_balance_helper_rollback.sql
```

It was compiled successfully in disposable PostgreSQL. It was not run on TEST.

Do not use an older nine-function rollback for this deployment.

## Hard scope boundary

This implementation does not change:

```text
Banking Pay or pay_* functions
Policy X
payment execution, CSV/provider processing, settlement or remittances
invoice creation, issuing, credit creation, totals, VAT or documents
TSFIN calculation, rates, rounding or Workers
frontend or Bulk Authorise
HealthRoster Daily
HealthRoster Weekly validation-only
phase-3 functions or Weekly callers
```

The helper remains read-only. The GitHub deployment workflow changed the helper definition and its normal repeatable deployment-ledger record only. No application timesheet, invoice, operation, payment or user row was mutated during deployment verification.

## Independent verifier instructions

The verifier must review the exact committed patch and deployed helper against the requirements in this report.

The verifier must:

1. Confirm the commit is a fast-forward descendant of `e3df88f` and contains only the four reported files.
2. Confirm only `_import_review_effective_invoice_balance_core_v1` changes production runtime logic.
3. Re-run the focused Node tests and the disposable PostgreSQL fixtures from this pack.
4. Re-query the live helper body/hash and confirm the other eight protected functions remain unchanged.
5. Review each of the seven implemented logic sections above, especially real request/applied mode compatibility, transitive repair lineage, credit provenance and zero-hour monetary residual handling.
6. Confirm HealthRoster Daily, HealthRoster Weekly validation-only, Banking Pay, invoicing and TSFIN remain outside the implementation.

If any further defect is demonstrated, the response must provide a highly detailed function-by-function implementation plan only for functions with direct evidence of a remaining defect. It must state the exact failing fixture, source line/contract, required amendment and regression test.

The verifier must not use this handover as authority for a broad rewrite or for changes to Banking Pay, invoicing, TSFIN, frontend, Workers, phase 3, Weekly callers, Daily or validation-only processing.

## Safety summary

```text
TEST database targeted: yes
Production database targeted: no
Normal TEST Worker deployed: no
Frontend deployed: no
Worker code changed: no
Banking Pay code changed: no
Application financial rows mutated: no
Payment/provider/remittance action run: no
Secrets printed or committed: no
Raw sensitive diagnostics committed: no
```

