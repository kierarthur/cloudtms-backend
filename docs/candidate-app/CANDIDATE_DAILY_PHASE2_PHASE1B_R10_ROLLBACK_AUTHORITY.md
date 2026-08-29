# Candidate Daily Phase 2 and Phase 1B R10 Rollback Authority

Date: 17 August 2026

Environment: TEST only

## Purpose

R10 closes the one bounded release blocker identified by the independent R9 review. R9 correctly moved generation, source, cursor, reconciliation, overlay and in-flight proof into the locked PostgreSQL authority. The remaining defect was narrower: the first rollback edge from `SUPABASE_PRIMARY` to `ROLLBACK_PENDING` compared the caller's disposition with the database-derived disposition, but did not reject the truthful database result `NONE`.

Because `NONE` means unresolved work exists, that edge could commit while a projection, command, batch or external effect remained unresolved. R10 makes the existing rule executable for every mode-changing edge.

R10 changes no table, RPC signature, HTTP route, Worker route, frontend, Google source, Candidate entitlement or financial authority.

## Later-controlling invariant

`NONE` is evidence that the current authority cannot change. It is never authority to move between modes.

For any item where:

```text
prior_authority_mode != new_authority_mode
```

the database-derived disposition must be either:

```text
DRAINED
```

or the already-approved exact overlay state:

```text
RECONCILED
```

If the locked database owner derives `NONE`, the item is rejected as `CANDIDATE_DAILY_NOT_READY`. The authority mode, entitlement, immutable transition ledger and transition fence remain unchanged.

## Why the caller comparison is retained

The request still carries `in_flight_disposition` as an optimistic assertion. PostgreSQL still derives the actual value under the same locks and rejects any mismatch as `SEMANTIC_REJECTION`.

R10 adds a separate semantic barrier after that equality check:

```text
caller says DRAINED, database derives NONE
-> SEMANTIC_REJECTION

caller says NONE, database derives NONE, mode changes
-> CANDIDATE_DAILY_NOT_READY

caller says NONE, database derives NONE, exact no-op
-> existing NO_CHANGE rule remains available
```

This separation preserves a stable distinction between a falsified request and a truthful request that is not ready to switch authority.

## First rollback edge

The first rollback stage remains:

```text
SUPABASE_PRIMARY
-> ROLLBACK_PENDING
```

It requires all of the following:

- the global Candidate Daily flag is already false;
- the Candidate entitlement becomes or remains false;
- the current expected mode, canonical version and entitlement match locked database truth;
- no pending, claimed, retry or terminal Google projection exists;
- no Candidate command is `IN_PROGRESS`;
- no other Candidate Daily batch for the Candidate is `IN_PROGRESS`;
- no external effect is `IN_PROGRESS` or `UNKNOWN`;
- the caller assertion equals the database-derived disposition;
- the database-derived disposition is not `NONE`.

The first stage does not require the final Google parity proof. That proof remains mandatory for the second edge:

```text
ROLLBACK_PENDING
-> GOOGLE_PRIMARY
```

## Accepted settled dispositions

`DRAINED` remains accepted when no unresolved owner and no deferred overlay exists.

`RECONCILED` remains accepted when no unresolved owner exists and every retained `DEFERRED_OVERLAY` has the exact active-generation/date/source-row proof already required by R9. This preserves the accepted overlay design and does not force the system to discard a valid, currently visible overlay before rollback can begin.

## Executable regression matrix

The direct PostgreSQL suite now proves the first rollback edge rejects:

- `PENDING` projection with caller `DRAINED`;
- `PENDING` projection with caller `NONE`;
- `CLAIMED` projection with caller `DRAINED` and `NONE`;
- `RETRY` projection with caller `DRAINED` and `NONE`;
- `TERMINAL` projection with caller `DRAINED` and `NONE`;
- an `IN_PROGRESS` Candidate command;
- another `IN_PROGRESS` Candidate Daily batch;
- an `IN_PROGRESS` external effect;
- an `UNKNOWN` external effect.

Every rejection proves:

- mode remains `SUPABASE_PRIMARY`;
- entitlement remains false;
- `transition_in_progress` is false after the item subtransaction ends;
- no immutable transition row is appended.

The real two-session Node/PostgreSQL test additionally races two different-key first rollback attempts against one pending projection and requires both to return the same stable not-ready rejection with no authority drift.

The valid R9 forward, settled first-stage rollback, final rollback, exact replay, different-key single-winner, no-op, overlay and partial-cohort journeys remain in the same suites and must stay green on PostgreSQL 17.6 and 18.1.

## Preserved boundaries

R10 preserves:

- exactly twelve Candidate Daily tables;
- exactly thirteen Candidate Daily public service RPCs;
- every RPC signature, grant, `SECURITY DEFINER` posture and closed search path;
- all Phase 1B private/public routing and response contracts;
- the Google evidence gate and minimal-change legacy boundary;
- the disabled feature and empty Candidate/Daily TEST posture;
- Candidate core and Office authority;
- finance, Invoice, Banking Pay, Policy X, provider, settlement and remittance authority;
- no production access or deployment.

## Re-audit gate

Phase 1B remains independently approved. Phase 2 remains blocked only until an independent reviewer verifies this R10 correction against source, both PostgreSQL engines and the installed TEST function.

A later GO for R10 permits the already-planned Phase 3 Google coexistence gate only. It does not enable Candidate Daily, create an entitlement, change Google, retire the legacy browser or complete the full Candidate App.
