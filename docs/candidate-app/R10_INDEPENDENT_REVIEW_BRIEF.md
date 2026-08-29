# Candidate Daily Phase 2 and Phase 1B R10 Independent Review Brief

## Requested disposition

Perform a fresh, bounded operation-level audit of the R10 first-rollback correction.

Do not reopen accepted Candidate core, Office, authentication, Phase 0, Phase 1A or Phase 1B architecture without a concrete new defect. Do not broaden into finance, Invoice, Banking Pay, Policy X, provider, settlement, remittance, production or frontend design.

The requested verdict is:

```text
GO for Candidate Daily Phase 2
```

only if the correction proves that no changed authority mode can commit with database-derived `NONE` and all preserved regression/safety gates remain green.

## Incoming finding to reproduce first

Against the R9 baseline, construct:

```text
authority_mode = SUPABASE_PRIMARY
candidate_daily_enabled = false
entitlement = false
one unresolved owner (for example PENDING projection)
requested new mode = ROLLBACK_PENDING
requested disposition = NONE
```

Confirm the R9 function could commit that first rollback stage. This establishes that the audit is exercising the same bounded defect rather than a source-presence check.

## R10 source change to inspect

The runtime correction is intentionally one semantic barrier in:

```text
supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql
```

It must occur after PostgreSQL has derived the disposition and compared it with the caller assertion, and before any transition row, entitlement update or authority-mode update can commit:

```text
prior mode differs from new mode
and derived disposition is NONE
-> CANDIDATE_DAILY_NOT_READY
```

Confirm the function's item-subtransaction semantics remove the temporary transition fence and roll back any item-local proposal/source change on this rejection.

## Mandatory direct cases

Run the supplied SQL suite on PostgreSQL 17.6 and 18.1 and independently inspect the results for:

1. PENDING projection, false DRAINED assertion;
2. PENDING projection, truthful NONE assertion;
3. CLAIMED projection, false DRAINED and truthful NONE;
4. RETRY projection, false DRAINED and truthful NONE;
5. TERMINAL projection, false DRAINED and truthful NONE;
6. IN_PROGRESS Candidate command;
7. another IN_PROGRESS Candidate Daily batch;
8. IN_PROGRESS external effect;
9. UNKNOWN external effect;
10. two concurrent different-key first rollback attempts.

For every rejection independently confirm:

- mode remains `SUPABASE_PRIMARY`;
- entitlement remains false;
- `transition_in_progress` is false;
- no transition ledger row was added.

## Preserved positive journeys

Do not accept the correction merely because negative cases pass. Re-run and confirm:

- settled `DRAINED` first rollback can still enter `ROLLBACK_PENDING`;
- exact valid `RECONCILED` overlay remains usable;
- final `ROLLBACK_PENDING -> GOOGLE_PRIMARY` retains full source/generation/cursor/reconciliation proof;
- dark forward cutover retains full R9 proof;
- exact no-op with `NONE` remains `NO_CHANGE`;
- exact replay and changed-request conflict remain durable;
- partial cohorts remain isolated;
- same-key and different-key concurrency remain deterministic.

## Installed TEST verification

Confirm:

- current backend `test` contains the declared R10 runtime commit and no later Candidate runtime drift;
- the safe-migration workflow succeeded at that commit;
- the installed repeatable ledger/source digest matches the published file digest;
- the canonical installed `pg_get_functiondef` digest matches the handover;
- the installed function contains the R10 guard;
- all Candidate feature flags are false;
- seven Candidate core and twelve Candidate Daily tables are empty;
- Candidate-bound mail remains empty;
- no real authority scope, entitlement, source link, transition, command, projection, effect or external action was created by verification.

## Stop rule

If every mandatory journey passes and no concrete supported blocker remains, issue GO and stop. Do not invent an unrelated next round.

If a genuine blocker remains, return one bounded handover that identifies the exact operation family, concrete reproduction, expected authority and smallest safe correction.
