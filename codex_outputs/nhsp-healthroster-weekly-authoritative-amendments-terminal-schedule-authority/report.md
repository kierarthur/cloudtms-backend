# NHSP/HealthRoster Weekly Authoritative Amendments — terminal schedule authority handover

## Executive verdict

The locked terminal-schedule-authority completion is implemented, committed, pushed and installed on TEST.

Production logic changed in exactly one function:

```text
public._import_review_effective_invoice_balance_core_v1(
  uuid,jsonb,integer,integer,integer,integer
)
```

No Banking Pay, invoice writer, credit-note writer, TSFIN calculator, Worker, frontend, action-catalog, apply-envelope, lifecycle, rollover, phase-3, Weekly caller, Daily or validation-only production function changed.

## Source and deployment identity

```text
Repository: kierarthur/cloudtms-backend
Branch: test
Pre-change commit: 1952d506d2fd06efef29721e3b845928bb14f850
Implementation commit: 648ca6c1584a233739d7829cd321476cd4cd496d
Commit message: Bind correction schedules to terminal evidence
TEST Supabase project: test-cloudtms
Supabase migration label: terminal_schedule_authority_evidence
```

The normal TEST Worker and frontend were not deployed because neither changed. Production was not accessed or deployed.

## Root defect completed here

The previous helper correctly accumulated the signed Weekly-hours economic ledger, but its reversal schedule fallback could still be governed by whichever positive frozen schedule happened to survive. An older positive could therefore win if it had the same hour buckets as the current effective balance, even though it represented different work times.

That was unsafe because bucket equality proves only arithmetic. It does not prove schedule provenance.

The fixed rule is:

```text
economic B = every admitted effective physical Weekly-hours component

schedule authority = exact terminal effective positive member only
```

## Implementation details

### Terminal member proof

The helper now selects the final fully invoiced correction generation using its existing deterministic evidence order. It then requires exactly one canonical `CHANGED_HOURS_REPLACEMENT` physical member for that terminal generation.

If no correction generation exists and the ordinary source has a positive effective position, the exact ordinary source timesheet is the terminal positive member.

### Frozen candidates carry provenance

Positive frozen source segments are retained with:

```text
physical timesheet ID
correction ID
correction kind
invoice-line ID
exact normalized schedule
exact hour buckets
canonical policy fingerprint
```

Only a candidate belonging to the exact terminal member is eligible. Older positives are ignored even when their buckets equal `B`.

### Canonical correction policy validation

For correction members, a frozen policy is accepted only when:

- the correction policy envelope is an object;
- the embedded and top-level envelope fingerprints agree;
- hashing the envelope without its fingerprint re-attests the recorded fingerprint.

The helper no longer carries forward an unrelated older TSFIN policy fingerprint.

### Completed-operation fallback

If the terminal positive row or historical TSFIN has been physically removed, the helper selects only the validated completed operation that proves the same:

```text
terminal correction ID
terminal replacement timesheet ID
source identity and source shift
reviewed unit and reconciliation fingerprints
```

It then uses that operation's reviewed `A_schedule_json`, `A_hours`, operation ID and policy-envelope fingerprint.

### Dual-authority agreement

When terminal frozen and terminal completed-operation evidence both exist, they must agree on:

```text
date/work date
start
end
break
shift ID
external row key
canonical policy fingerprint
```

A material schedule conflict, policy conflict, unprovable terminal member or missing terminal policy fails closed through the existing scope-unprovable blocker.

### Output and staleness contract

The helper now emits and fingerprints:

```text
B_standard_schedule_authority
B_standard_schedule_authority_timesheet_id
B_standard_schedule_authority_correction_id
B_standard_schedule_authority_operation_id
B_standard_schedule_authority_policy_fingerprint
B_standard_schedule_authority_fingerprint
B_standard_schedule_authority_diagnostic
```

Authority values are limited to:

```text
ORIGINAL_SOURCE_FROZEN_SEGMENT
TERMINAL_REPLACEMENT_FROZEN_SEGMENT
TERMINAL_COMPLETED_OPERATION_A_SCHEDULE
NONE
```

The authority contract is incorporated into `role-evidence-v4`, `effective-invoice-v4` and `reconciliation-v4`. A lifecycle/evidence change between Review and Apply therefore changes the reconciliation fingerprint and becomes stale.

## Production files changed

```text
supabase/repeatable/21072026_1820_00_import_review_internal_core.sql
```

The canonical file still contains one definition for the exact helper signature. No duplicate replacement definition was added.

## Verification files changed

```text
tests/01082026_0150_import_authoritative_effective_balance_runtime_schema.sql
tests/01082026_0150_import_authoritative_effective_balance_runtime_fixtures.sql
tests/import-authoritative-effective-balance-helper.test.cjs
```

The disposable schema's helper definition is an exact mechanical mirror of the canonical function body.

## Local source-contract verification

Command:

```text
node --test tests/import-authoritative-effective-balance-helper.test.cjs
```

Result:

```text
30 tests
30 passed
0 failed
```

This suite reads the real canonical apply-envelope and NHSP/HealthRoster phase-3 sources and re-attests their request/applied-result contracts. It also proves the one-function scope, terminal provenance selection, completed-operation fallback, six-field authority output, fingerprint propagation and absence of Banking Pay/invoice mutation paths.

Complete output is included as `source-contract-test-output.txt`.

## Disposable PostgreSQL runtime verification

Environment:

```text
local disposable PostgreSQL 18
fixture transaction begins and ends with ROLLBACK
no CloudTMS TEST or production rows are used or changed
```

The runtime suite passed independently for NHSP Weekly and HealthRoster Weekly import-authoritative processing:

- cross-import deleted historical-member reconstruction;
- archived-member repair supersession;
- transitive archived-member supersession;
- multi-source credit allocation;
- zero-hours/non-zero-money safe blocker;
- invalid/non-complete operation authority controls;
- malformed credit provenance controls;
- repeated correction-chain arithmetic;
- older positive schedule with matching buckets rejected as authority;
- missing terminal row/TSFIN resolved from exact completed-operation schedule and policy;
- agreeing terminal frozen segment preferred;
- terminal frozen/operation material conflict blocked;
- real apply-envelope producer contract for both source systems.

Complete output is included as `runtime-fixture-output.txt`.

The fixture directly seeds synthetic frozen invoice/TSFIN evidence as explicitly permitted. Completed applied-result payloads use the exact validated phase-3 contract; the separate source-contract suite re-attests those fields and fingerprints against both real phase-3 source definitions.

## TEST pre-install safeguards

Before installation:

```text
total Import Review operations: 29
non-COMPLETE operations: 0
operations missing committed/finalised timestamps: 0
active TSFIN follow-ups: 0
```

The previous TEST helper exactly matched the canonical pre-change source:

```text
pre-change pg_get_functiondef MD5: feaf3ed9910f2dc0f8132b406f8f5cd0
pre-change body MD5:              737b212ae796e318b72dbb5e819c0745
pre-change body length:           94044
```

## TEST post-install verification

The installed helper exactly matches the committed local definition:

```text
post-change pg_get_functiondef MD5: 4b183b4e4009189f6361223299dee287
post-change body MD5:               baa981be3bdf45e8a729fcd71cb8d7c4
post-change body length:            107526
exact body parity:                  yes
```

Runtime metadata remains:

```text
owner: postgres
security: SECURITY INVOKER
search_path: public, extensions, pg_temp
direct EXECUTE ACL: postgres only
exact signature count: 1
```

The other eight protected function MD5 values were checked after deployment and are unchanged:

| Function | MD5 |
| --- | --- |
| `_import_review_action_catalog_core_v1` | `1623934a80bd09d263588271addd9ffd` |
| `_import_review_apply_envelope_core_v1` | `3fad3522f7d3d0aaa464d8a32b20ebd9` |
| `import_review_correction_generation_transition_v1` | `1d3481867a4326c471812ef7c6326f76` |
| `timesheet_paid_uninvoiced_rollover_v1` | `2632b3b506dcd2bd06a77dddad01e76d` |
| `hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)` | `788e9d8926c91cf654a9d36634944d94` |
| `nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)` | `5a35c7cd476e580841eb8c8a060d3992` |
| `hr_weekly_apply_transactional` | `d57dea07edca42d3ae41b56b37ded723` |
| `nhsp_weekly_apply_transactional` | `505bfc530f49e8e79233d1e468271e49` |

Latest live catalog smoke calls also completed successfully:

```text
HealthRoster: 11 action rows, 0 APPLY_AMENDMENT rows
NHSP:         26 action rows, 0 APPLY_AMENDMENT rows
```

Those live imports had no amendment-route rows, so they are compile/catalog smoke evidence rather than destructive correction-route evidence. The destructive edge cases were exercised only in the rollback-only disposable database suite.

## Rollback

`01082026_1407_nhsp_hr_terminal_schedule_authority_rollback.sql` restores only the exact pre-install helper captured from commit `1952d506...` in one transaction. It does not restore, modify or redeploy any of the other eight protected functions.

## Repository state

The implementation commit was pushed directly and non-forced to backend `test`. The saved backend checkout used by GitHub Desktop was fast-forwarded to the same commit and was clean at implementation handoff.

The earlier local duplicate Banking Pay commit was not reapplied. Its published content remains represented by remote commit `e3df88f`. The safety stash created during the earlier Windows rebase recovery remains unapplied and does not affect repository status.

## Safety summary

```text
Secrets printed: no
Production accessed: no
Production deployed: no
TEST table data mutated by this deployment: no
TEST DDL: one CREATE OR REPLACE FUNCTION only
Worker deployed: no
Frontend deployed: no
Banking Pay changed: no
Invoice/credit writers changed: no
TSFIN calculation changed: no
HealthRoster Daily changed: no
HealthRoster validation-only changed: no
Policy X drift: none; Banking Pay was outside scope and untouched
```

## Independent reviewer instruction

Use `locked-implementation-and-verification-brief.md` as the governing brief and commit `648ca6c` as the implementation under review.

The reviewer must:

1. Re-read the committed helper and all three committed test files.
2. Verify the live TEST helper body and metadata against the committed definition.
3. Verify the other eight named protected functions remain unchanged.
4. Re-run or independently reconstruct the focused terminal-provenance cases for both NHSP and HealthRoster authoritative Weekly.
5. Give a function-by-function verdict for the nine named functions.
6. If further work is genuinely required, provide a highly detailed function-by-function implementation plan only for functions with a demonstrated defect necessary to satisfy the locked brief.
7. Do not expand scope into Banking Pay, invoice creation/credits, TSFIN calculation, frontend, Worker, Daily, validation-only or unrelated Import Review architecture.

The reviewer must not treat this handover as permission for a broad rewrite.
