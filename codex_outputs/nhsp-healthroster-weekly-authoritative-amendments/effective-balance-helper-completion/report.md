# NHSP/HealthRoster Weekly Authoritative Amendments

## Effective-balance helper completion: implementation and independent-verification handover

Date: 01 August 2026 (Europe/London)

## Executive result

The final tightly scoped helper-only repair has been implemented, committed, pushed, and installed on TEST.

Production function changed:

```text
public._import_review_effective_invoice_balance_core_v1(
  uuid,
  jsonb,
  integer,
  integer,
  integer,
  integer
)
```

Canonical source:

```text
supabase/repeatable/21072026_1820_00_import_review_internal_core.sql
```

Implementation commit:

```text
411eff20d64e8ded44ca1563403e82e20a88922c
Complete authoritative effective balance evidence
```

Commit parent / editing baseline:

```text
dacc55410f27fd9800d00d5afe26c38928c9e076
```

Branch and remote state:

```text
branch: test
implementation commit present on origin/test: 411eff20d64e8ded44ca1563403e82e20a88922c
push: successful
```

TEST deployment proof:

```text
project: test-cloudtms
live helper pg_get_functiondef MD5: 270da78518d8c18a10f260ec7eb4f623
live stored body equals commit 411eff2 after normalising PostgreSQL's
stored terminator and CRLF/LF line endings: true
exact helper signature count: 1
owner: postgres
search_path: public, extensions, pg_temp
```

No Worker or frontend deployment is required for this database-only change. No frontend, Worker, Banking Pay, invoice mutation, TSFIN calculation, Daily, or validation-only production code was changed.

## Locked scope

The implementation applies only to changed-hours reconciliation for:

```text
NHSP Weekly import-authoritative processing
HealthRoster Weekly import-authoritative processing
```

It does not apply to:

```text
HealthRoster Weekly validation-only processing
HealthRoster Daily
ordinary non-authoritative imports
Banking Pay
payment drafting or execution
payment CSV generation
Policy X
invoice creation, issue, unissue, credit creation, totals, VAT, or documents
general TSFIN calculation, rates, rounding, or Workers
frontend or Bulk Authorise
```

Banking Pay remains a downstream consumer of whatever genuine outstanding timesheet position exists. This implementation does not make Banking Pay decide or repair authoritative import economics.

## The defects repaired

The previous helper could not reliably calculate the true authoritative position where historical correction members were archived or physically absent. Its role-state classification also used a second raw invoice-line existence test, which could disagree with the signed Weekly-hours ledger used for `B`.

Specifically, it could:

1. discover completed-operation member IDs after invoice scope had already been fixed;
2. omit a surviving invoice line when the historical member and cascaded TSFIN were physically absent;
3. let archived-only roles establish a mutable generation;
4. classify expenses or other non-hours lines as role invoice evidence;
5. classify fully credited roles from physical line existence rather than their signed economic result;
6. inadequately distinguish valid successive operations from contradictory duplicate evidence;
7. advertise an unsafe ordinary source amendment after a full credit where the current row remained paid or invoice-lined.

## Locked implementation plan

The approved implementation plan was:

1. Use the current committed helper as the sole editing baseline.
2. Load all bounded operation evidence for each exact source before constructing historical timesheet or invoice scope.
3. Accept historical applied-result authority only from complete, committed, finalised operations containing exactly one matching request unit, applied-result unit, and correction-policy unit.
4. Cross-check action, source, source shift, root timesheet, candidate, client, contract, week, correction identity, member identity, policy envelope, operation contract, reviewed-unit fingerprint, reconciliation fingerprint, and applied-result fingerprint.
5. Accumulate every valid completed correction generation; never treat more than one valid operation for the same source as a duplicate merely because the source repeats.
6. Add every validated applied member ID to historical timesheet and invoice scope before `B` is calculated.
7. Build one canonical member/source/role evidence map from completed operation evidence, correction audit evidence, frozen TSFIN evidence, and a surviving active row. Fill missing fields but fail closed on contradictions.
8. Permit an exact `HOURS_WEEKLY` invoice line for a validated applied member to provide frozen economics when the member row, TSFIN, and audit row are absent.
9. Build one canonical signed Weekly-hours component ledger. Count economics once by physical invoice-line identity. Use that same ledger for `B` and correction-role economic state.
10. Keep separable expense, travel, mileage, accommodation, reimbursement, and other non-hours lines outside `B` and role invoice state.
11. Apply credits once: negate the resolved original component's hours and use the credit line's stored signed financial amounts once.
12. Classify role states explicitly, including effective history, settled-zero history, pending invoice, active mutable, operation-proved physically missing mutable, archived audit-only, and unprovable.
13. Ensure both archived roles are audit-only and cannot establish `M`; allow archived/live repair only through the existing fresh-correction-ID mode.
14. Treat a generation as genuinely partial only when one economically proved Weekly-hours role is effective and the other expected role is proved but not economically effective.
15. Preserve repeated correction arithmetic across every valid physical component, for example `+10 -10 +11 -11 +12 -12 +10 = +10`.
16. For a fully credited `B = 0`, distinguish a genuinely safe current ordinary source from a current row that remains paid or invoice-lined. Block the unsafe state with `IMPORT_REVIEW_EFFECTIVE_ZERO_NO_ACTIVE_SOURCE`.
17. Preserve the helper signature and all existing downstream output keys. Add source-safety and validated-operation evidence to the reconciliation fingerprint so Review becomes stale if lifecycle truth changes before Apply.
18. Leave the other eight previously reviewed production functions unchanged unless executable regression testing demonstrates an independent defect.
19. Verify compile/runtime behaviour for NHSP and HealthRoster independently, capture a one-function rollback, install only the helper on TEST, and prove live source parity.

## Implementation delivered

### Completed operation evidence

Operation evidence is now loaded before `v_hist_ids` and invoice-line scoping. Historical applied-result authority requires:

```text
state = COMPLETE
committed_at_utc is not null
finalised_at_utc is not null
one matching request unit
one matching applied-result unit
one matching policy unit
matching action and source identities
matching reviewed/reconciliation fingerprints
valid policy-envelope fingerprint
valid operation-contract fingerprint
valid applied-result fingerprint
valid correction/member/parent identities
```

Committed in-progress states are treated as processing, not immutable history. Invalid, failed, cancelled, prepared, uncommitted, duplicate, or contradictory operation evidence does not seed historical authority.

All valid completed operations are retained in deterministic finalisation order. Same-operation replay and repeated operations amending one mutable generation do not create economic duplication because money is never counted from operations.

### Historical member reconstruction

Validated reversal, replacement, and applied-member IDs enter `v_hist_ids` before invoice scanning. A missing live timesheet row is reported in `historical_missing_timesheet_ids`.

The canonical role map records correction ID, correction kind, source ownership, source shift, member ID, and authority source. Contradictory source or role assignments fail closed with the existing scope-unprovable blocker.

### One signed Weekly-hours economic ledger

Each admitted physical invoice component records:

```text
invoice line identity
effective or pending state
timesheet/member identity
correction generation and role
signed day/night/Saturday/Sunday/bank-holiday hours
signed pay, charge, and margin
```

Only this ledger contributes to `B` or makes a correction role economically invoiced. Raw invoice-line existence is no longer a second financial authority.

An exact `HOURS_WEEKLY` line whose member ownership is proved by a validated completed operation remains usable when the historical member, TSFIN, and audit row have been physically removed. Ambiguous aggregate or contradictory lines remain blocked.

### Archived, deleted, mutable, and invoiced generations

Archived-only generations now return `ARCHIVED_AUDIT_ONLY`, contribute zero to `M`, and cannot win mutable-generation selection.

An archived role with a surviving active sibling remains repairable using:

```text
FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED
```

A fully invoiced generation is determined from the signed Weekly-hours ledger. A genuinely one-sided economic Weekly-hours generation remains safely blocked. Multiple active roles or multiple unsupported economically effective physical members for one logical role fail closed.

### Repeated correction arithmetic

Operations prove source/member ownership; invoice lines prove money. Every distinct physical invoice component is counted once.

The implemented ledger supports:

```text
T0:  +10
R1:  -10   P1: +11
R2:  -11   P2: +12
R3:  -12   P3: +10

effective total = +10
```

It never assumes that the latest surviving positive row is complete or unchanged, and it does not repeatedly reverse the original while ignoring later economic history.

### Effective-zero source safety

Where historical invoice and credit components net fully to zero, the helper now proves the exact current ordinary source is safe before allowing the existing catalogue to fall through to `AMEND_SOURCE`.

The safe source must be current, non-archived, ordinary, source-matched, unpaid, unlocked, fresh, free of rate/pay-channel and segment-lock issues, unreferenced by invoice lines, free of an active invoice operation or pay draft, and have one safe direct contract week.

Unsafe paid or invoice-lined effective-zero states receive:

```text
IMPORT_REVIEW_EFFECTIVE_ZERO_NO_ACTIVE_SOURCE
```

with a diagnostic such as:

```text
CURRENT_SOURCE_PAID_AND_INVOICE_LINED
CURRENT_SOURCE_INVOICE_LINED_AFTER_EFFECTIVE_ZERO
```

A genuinely paid-and-uninvoiced ordinary source remains eligible for the existing `AMEND_PAID_UNINVOICED_SOURCE` route because it has no invoice-line history.

### Output and staleness contract

The helper signature and existing downstream keys are unchanged. It additionally reports validated-operation evidence and effective-zero source-safety facts. Those facts enter `reconciliation-v2`, so a lifecycle change between Review and Apply invalidates the reviewed decision rather than silently changing the route.

## Function-by-function production verdict

| Function | Implementation decision | Verification status |
| --- | --- | --- |
| `_import_review_effective_invoice_balance_core_v1` | Changed. Implements the plan above. | Source tests, isolated PostgreSQL runtime fixtures, rollback compile, live source parity, and live catalogue smoke passed. |
| `_import_review_action_catalog_core_v1` | Deliberately unchanged. Existing blocker precedence and route order consume the corrected helper contract. | Live MD5 unchanged; live catalogue executed for both source systems. |
| `_import_review_apply_envelope_core_v1` | Deliberately unchanged. Existing request envelope freezes the required helper evidence. | Live MD5 unchanged. |
| `import_review_correction_generation_transition_v1` | Deliberately unchanged. Request/applied/policy separation remains compatible. | Live MD5 unchanged. |
| `timesheet_paid_uninvoiced_rollover_v1` | Deliberately unchanged. Genuine paid-and-uninvoiced route remains separate. | Live MD5 unchanged; existing paid-date regression tests passed. |
| `hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)` | Deliberately unchanged. Existing zero/one/two-member repair consumes corrected helper evidence. | Live MD5 unchanged. |
| `nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)` | Deliberately unchanged for the same reason. | Live MD5 unchanged. |
| `hr_weekly_apply_transactional` | Deliberately unchanged. HealthRoster authoritative Weekly caller remains compatible. | Live MD5 unchanged. |
| `nhsp_weekly_apply_transactional` | Deliberately unchanged. NHSP authoritative Weekly caller remains compatible. | Live MD5 unchanged. |

The separate four-argument HealthRoster phase-3 overload also remained unchanged.

## Verification evidence

### Source and regression tests

```text
tests/import-authoritative-effective-balance-helper.test.cjs: 21/21 passed
tests/import-correction-policy-paid-date.test.cjs:             3/3 passed
combined focused run:                                          24/24 passed
git diff --check:                                              passed
```

The previous verification pack's 11-test source-pattern suite was also evaluated against the final source. Ten assertions passed. Its sole failure is a stale literal regex that requires invoice status filtering to appear specifically as `i.status in (...)`; the refactored helper filters the same issued statuses through the canonical ledger variable. This is verification-metadata/test maintenance, not a runtime defect. The new focused suite asserts ledger state and issued-status behaviour directly.

### Isolated PostgreSQL runtime fixtures

The actual helper definition was compiled and executed inside a disposable local PostgreSQL 18 cluster using bounded schema-compatible fixtures. The cluster and logs were removed after testing.

Passed scenarios:

1. ordinary safe `B = 0` source;
2. full credit plus paid/invoice-lined current source blocks with the correct code and reason;
3. genuine paid-and-uninvoiced source remains unblocked;
4. both roles archived produce `ARCHIVED_AUDIT_ONLY` and no mutable generation;
5. archived reversal plus live replacement selects the fresh correction-ID repair mode;
6. deleted invoiced reversal with no row, TSFIN, or audit is reconstructed from completed operation plus physical `HOURS_WEEKLY` line;
7. three completed NHSP generations calculate the repeated `+10 → +11 → +12 → +10` chain correctly;
8. the identical three-generation HealthRoster authoritative Weekly chain calculates correctly;
9. a corrupted applied-result fingerprint fails closed;
10. fully credited history plus a distinct safe current ordinary source remains eligible for ordinary amendment;
11. the one-function rollback definition compiles and restores successfully.

### Live TEST verification

Fresh post-install hashes:

```text
helper       270da78518d8c18a10f260ec7eb4f623
catalog      1623934a80bd09d263588271addd9ffd
envelope     3fad3522f7d3d0aaa464d8a32b20ebd9
transition   1d3481867a4326c471812ef7c6326f76
rollover     2632b3b506dcd2bd06a77dddad01e76d
hr_phase3_3  788e9d8926c91cf654a9d36634944d94
hr_phase3_4  8d6ea15820345929163ffc7dedf7b29b
nhsp_phase3  5a35c7cd476e580841eb8c8a060d3992
hr_apply     d57dea07edca42d3ae41b56b37ded723
nhsp_apply   505bfc530f49e8e79233d1e468271e49
```

The helper changed. The other inspected definitions remained at their established current hashes.

Install/follow-up gates after deployment:

```text
total import operations: 29
non-complete operations: 0
complete operations missing commit/finalisation timestamps: 0
active review follow-ups: 0
exact helper signature count: 1
```

Read-only live catalogue smoke:

```text
latest HealthRoster import: 11 catalogue rows returned
latest NHSP import:         26 catalogue rows returned
result: both live catalogue calls completed successfully
```

Those live calls exercised the deployed helper through the unchanged catalogue against current TEST data. They did not approve or apply an import and caused no business-data mutation.

### Verification limitation

Destructive edge cases were not manufactured in shared TEST data. They were executed against the actual compiled function in rollback-only isolated PostgreSQL fixtures. Live TEST verification proves committed-source parity, successful compilation/installation, unchanged dependent definitions, safe current-state gates, and read-only execution for both source systems.

## Rollback

Rollback file:

```text
01082026_0025_effective_invoice_balance_helper_rollback.sql
```

The rollback restores only the exact pre-change helper definition from the commit parent. It does not touch any other function, table, data row, Worker, frontend asset, invoice, payment, or Banking Pay pathway.

Rollback was compile-tested locally. It was not run on TEST.

## Repository and deployment safety

```text
Secrets printed: no
Production accessed: no
Production deployment: no
Normal TEST Worker deployed: no
Frontend deployed: no
TEST database function installed: yes, one helper only
Destructive TEST data mutation: no
Payment/provider/remittance action: no
Invoice creation/credit/issue action: no
Raw diagnostic artifacts committed: no
Policy X drift: none; Banking Pay code and contracts unchanged
```

Unrelated pre-existing local changes and historical verification-pack artifacts were preserved and excluded from the implementation commit.

## Mandatory independent-review instruction

The receiving reviewer must independently inspect commit `411eff20d64e8ded44ca1563403e82e20a88922c`, compare the committed helper with the installed TEST definition, and evaluate the implementation against the locked plan in this report.

The reviewer must return:

1. an overall verdict of `FULLY IMPLEMENTED` or `FURTHER WORK REQUIRED`;
2. a function-by-function verdict for the nine named production functions, plus the unchanged four-argument HealthRoster overload where relevant;
3. executable evidence for every claimed defect;
4. if further work is genuinely required, a highly detailed function-by-function implementation plan containing **only** functions with a demonstrated remaining defect;
5. the exact failing scenario, evidence, intended postcondition, test, deployment consequence, and rollback consequence for each proposed function change.

The reviewer must not use this handover as permission to broaden implementation into:

```text
Banking Pay or Policy X
payment preview, draft, CSV, execution, settlement, provider, or remittance
invoice creation, issue/unissue, credit creation, VAT, totals, or documents
general TSFIN calculation or Workers
frontend or Bulk Authorise
HealthRoster Daily
HealthRoster Weekly validation-only
ordinary non-authoritative imports
the eight unchanged production functions without a reproducible independent defect
```

Historical hash metadata, stylistic preferences, speculative hardening, broad redesign, or a desire to rewrite adjacent architecture are not evidence of a remaining defect.

If no remaining defect is demonstrated, the reviewer must state that no further production function requires amendment.
