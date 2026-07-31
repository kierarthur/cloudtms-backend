# Banking Pay Source-Build Owner Recovery Completion Plan

## Closure correction and independent-verification handover

Date: 31 July 2026  
Environment: TEST only  
Repository: `cloudtms-backend`  
Branch: `test`

## 1. Final implementation verdict

The Banking Pay Source-Build Owner Recovery Completion Plan is functionally
complete.

No Banking Pay function body required another amendment. The installed TEST
definitions remain the intended completed definitions. The remaining plan work
was implemented only as:

1. corrected runtime-manifest provenance;
2. an exact replacement rollback;
3. an explicit supersession marker on the obsolete rollback; and
4. deterministic PostgreSQL 17.6 compilation, manifest, rollback and
   transactional tests.

No TEST database function was reinstalled. No repair helper was invoked against
TEST. No Worker or frontend was changed or deployed.

## 2. Exact installed TEST manifest

The verified TEST manifest is:

| Function | Installed `pg_get_functiondef` MD5 |
| --- | --- |
| `pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamptz,text)` | `78d2a4ac9dd7b8309ed5c77112d981f0` |
| `pay_workbench_session_get_progress_light(uuid)` | `9f7489d1242697dea393fab3a1d748e3` |
| `pay_workbench_session_recompute_progress_counters(uuid,boolean,text,boolean)` | `46b1686a98b3baeb7cbbadbcdc456f75` |

The recompute default remains:

```text
AUTHORITATIVE_COUNTER_RECOMPUTE
```

The recompute function retains:

```text
search_path=public
plpgsql_check.mode=disabled
plpgsql_check.profiler=off
plpgsql_check.tracer=off
plpgsql_check.constants_tracing=off
plpgsql_check.cursors_leaks=off
plpgsql_check.strict_cursors_leaks=off
plpgsql_check.fatal_errors=off
```

The repair helper remains executable only by `postgres` and `service_role`.
Progress-light and recompute retain their existing `postgres`,
`authenticated` and `service_role` execution grants.

## 3. Corrected explanation of the historical hash discrepancy

The earlier implementation addendum recorded:

```text
progress-light  497a7be67673cae16b2d95e47290fd3c
recompute       acc358aa65a14b4466cc47919d7132e5
```

Those values were not evidence of different business logic, but they were also
not random transcription strings.

The focused PostgreSQL 17.6 reproduction established the exact cause:

1. The canonical Windows checkout contains CRLF line endings.
2. PostgreSQL preserves function-body line endings in the stored function
   source used by `pg_get_functiondef`.
3. The MD5 therefore changes when the same function body is installed with CRLF
   rather than LF.
4. Installing the canonical body with CRLF reproduces `497a...` and
   `acc358...`.
5. TEST stores the same bodies with LF and therefore returns `9f748...` and
   `46b168...`.
6. After normalising both bodies to LF, the canonical and TEST function bodies
   are identical.

There is no semantic function drift. The reproducibility harness now
normalises the extracted canonical definitions to LF before installation, which
reproduces the exact TEST hashes on PostgreSQL 17.6.

The current TEST hashes remain the correct rollback precondition because the
rollback must guard the definitions actually installed in TEST.

## 4. Files changed

### 4.1 Superseded rollback marker

```text
codex_outputs/banking-pay-pending-owner-recovery/
30072026_1754_banking_pay_source_build_owner_recovery_completion_rollback.sql
```

Only a prominent non-functional header was added:

```text
SUPERSEDED — INCORRECT POST-DELTA HASH MANIFEST. DO NOT USE.
```

The historical SQL remains retained as evidence and still fails safely against
the current TEST definitions.

### 4.2 Replacement rollback

```text
codex_outputs/banking-pay-pending-owner-recovery/
31072026_1122_banking_pay_source_build_owner_recovery_completion_rollback.sql
```

The replacement is identical to the prior narrow rollback except for:

1. its replacement/supersession header; and
2. these two post-delta guards:

```text
progress-light  9f7489d1242697dea393fab3a1d748e3
recompute       46b1686a98b3baeb7cbbadbcdc456f75
```

The helper guard remains:

```text
78d2a4ac9dd7b8309ed5c77112d981f0
```

The replacement rollback:

- begins one transaction;
- takes the existing transaction-scoped advisory lock;
- verifies all three exact current hashes before replacing anything;
- restores only the three exact pre-delta definitions;
- restores their exact owners, ACLs, settings and null comments;
- verifies the three pre-delta hashes before commit;
- leaves the repair helper present;
- does not use `CASCADE`;
- does not restore the vulnerable Worker/failure-handler implementation; and
- does not touch any unrelated Banking Pay function.

The verified pre-delta hashes are:

```text
helper          977f2aa68b33a10649c69e308cf86e16
progress-light  64a227e561acf1be8bf434b13dd253c7
recompute       0830bcf4a7895de0cfee6960120580df
```

### 4.3 Transactional and reproducibility test

```text
tests/banking-pay-pending-owner-recovery.transactional.test.cjs
```

The test now:

- pins `postgres:17.6-alpine`;
- proves the running engine is exactly PostgreSQL 17.6
  (`server_version_num = 170006`);
- waits for the final official PostgreSQL startup, rather than the temporary
  initialization server;
- normalises extracted canonical function SQL to LF;
- installs the exact three canonical definitions;
- applies and asserts the intended owners, ACLs, defaults, comments and
  `proconfig`;
- asserts the exact three TEST-compatible compiled hashes;
- injects a deterministic error after the first rollback replacement and proves
  the entire rollback is atomic;
- runs the complete replacement rollback and proves the exact pre-delta
  manifest;
- reinstalls the completion definitions and proves the exact completed
  manifest; and
- runs all fourteen focused functional and concurrency scenarios after that
  reinstall.

### 4.4 This correction handover

```text
codex_outputs/banking-pay-pending-owner-recovery/
closure-correction-and-independent-verification-handover.md
```

This document supersedes the old addendum only for the hash provenance,
replacement rollback and final closure evidence. It does not rewrite the
historical record silently.

## 5. Files deliberately unchanged

No change was made to:

```text
supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql
supabase/repeatable/30072026_1310_pay_workbench_repair_orphaned_pending_source_build.sql
broker/src/index.js
broker/src/pay-workbench-worker.js
broker/src/pay-workbench-preview.js
broker/src/pay-workbench-drafts.js
wrangler.toml
```

No frontend repository file was changed.

## 6. Verification performed

### Static contract suite

```text
node --test tests/banking-pay-pending-owner-recovery.test.cjs
```

Result:

```text
13 passed
0 failed
```

This continues to prove the tightly scoped three-function completion contract,
canonical-definition uniqueness, safe repair transitions, deterministic
successor reporting, bounded recovery and Policy X boundary.

### Exact PostgreSQL 17.6 transactional suite

```text
BANKING_PAY_OWNER_RECOVERY_TRANSACTIONAL=1
node --test tests/banking-pay-pending-owner-recovery.transactional.test.cjs
```

Result:

```text
17 passed
0 failed
```

The 17 tests comprise:

- exact PostgreSQL 17.6 manifest reproduction;
- forced mid-rollback atomicity;
- complete rollback plus exact completion reinstallation;
- T01–T14 functional, failure-isolation, deterministic-successor and
  concurrency scenarios.

### Replacement rollback comparison

The old and replacement rollback files were compared directly. The only
differences are:

- the supersession/replacement header; and
- the two corrected current-runtime hash guards.

No restored function body, owner, ACL, setting, comment or pre-delta
verification guard changed.

### TEST runtime confirmation

TEST Supabase was inspected read-only. The project was confirmed as:

```text
test-cloudtms
PostgreSQL 17.6
```

The installed hashes and metadata match section 2.

## 7. Runtime and deployment status

```text
TEST database DDL/DML executed: no
TEST helper/claim/enqueue/drain invoked: no
Normal TEST Worker deployed: no
Isolated Worker deployed: no
Frontend deployed: no
Production accessed or deployed: no
Payment draft/CSV/execution performed: no
Provider/settlement/remittance action performed: no
Secrets printed: no
```

There was deliberately no deployment. Reinstalling definitions that are already
correct would add risk without changing runtime behaviour.

## 8. Policy X and behavioural boundary

The closure work does not alter:

- pre-draft live-truth authority;
- post-draft frozen-artifact authority;
- `TS_DAY`;
- economic keys;
- preview calculations;
- persisted decisions;
- draft creation;
- CSV generation;
- payment execution;
- PAYE or umbrella treatment;
- daily, NHSP or HealthRoster timesheets;
- overpayments or correction economics;
- provider, settlement or remittance behaviour.

Policy X drift: none.

## 9. Instructions for the independent verifier

Verify this closure only. Do not use the review to reopen unrelated Banking Pay
architecture.

The verifier should independently confirm:

1. The installed TEST hashes and metadata in section 2.
2. The canonical and TEST progress-function bodies are identical after LF
   normalisation.
3. The transactional harness is pinned to PostgreSQL 17.6 and rejects another
   server version.
4. The harness normalises canonical function SQL to LF before installation.
5. The exact completed hashes, metadata and ACLs are asserted.
6. The forced mid-rollback failure leaves all three completed definitions
   unchanged.
7. The complete rollback restores exactly the three pre-delta definitions and
   metadata.
8. Completion reinstallation restores the exact completed manifest.
9. T01–T14 run successfully after completion reinstallation.
10. The replacement rollback differs from the superseded rollback only as
    documented.
11. No canonical runtime function body, Worker or frontend file changed.
12. No database or application deployment was needed or performed.
13. Policy X and all payment economics remain unchanged.

## 10. Required response from the independent verifier

Return a clear verdict:

```text
FULLY IMPLEMENTED
```

or:

```text
REMAINING DEFECTS
```

If fully implemented, identify the evidence checked.

If anything remains defective, provide a highly detailed, function-by-function
or file-by-file implementation plan limited strictly to:

```text
pay_workbench_repair_orphaned_pending_source_build
pay_workbench_session_get_progress_light
pay_workbench_session_recompute_progress_counters
the focused transactional/static tests
the correction addendum
the replacement rollback
```

For every claimed defect, state:

- exact file/function;
- exact current behaviour;
- exact evidence of non-compliance;
- exact minimal amendment;
- exact tests required;
- why the amendment is within this plan.

The verifier must not propose changes to:

```text
WAIT_FOR_PREVIEW_READY
preview or draft contracts
payment calculations
economic keys
TS_DAY
CSV generation
persisted selections or resolutions
frontend polling or reset architecture
pay_workbench_fail_job
the general Worker drain
RPC permissions
PAYE, umbrella, daily, NHSP or HealthRoster behaviour
providers, settlement or remittances
```

No broad redesign is authorised.

## 11. Final closure statement

The recovery functions are complete and healthy in TEST. The documentation,
rollback and exact-engine reproducibility gap has been corrected without
changing runtime logic or redeploying already-correct functions.
