# Candidate Daily Phase 2 and Phase 1B R9 Correction Authority

Date: 17 August 2026

Environment: TEST only

## Purpose

R9 closes the one bounded release blocker found by the independent R8 review. R8 correctly installed the additive Daily schema, thirteen service RPCs, Phase 1B broker/private mappings and disabled TEST deployment. Its authority-transition implementation was not sufficient: a caller could propose a forward mode change without PostgreSQL independently proving the complete generation, source, cursor, reconciliation and in-flight state described by the decisions.

R9 changes only the existing transition owner and its executable verification. It does not add a table, RPC, HTTP route, Worker route, Candidate feature, Google change, financial owner or production authority.

## Controlling invariant

An authority-changing result is valid only when the database transaction that writes the immutable transition has already locked and proved every current fact that could make that result false.

The request remains required to carry expected facts. They are optimistic-concurrency assertions, not authority. PostgreSQL compares them with locked current facts and derives the actual disposition itself.

## Database-owned lock and proof order

For one batch, the function:

1. validates the signed system context, actor, separate approver, batch identity, item set, reason and evidence digest;
2. acquires/locks the durable batch receipt and returns only the stored exact replay for a completed same request;
3. locks the singleton Candidate Daily feature configuration;
4. pre-locks all existing cohort scope rows in deterministic Candidate order;
5. for each item, requires and locks the exact scope and entitlement;
6. validates the closed mode edge and entitlement/global-switch relationship;
7. locks and proves the current source-link catalogue;
8. locks in-progress commands, other batches, in-progress/unknown effects and all projection rows for the Candidate;
9. derives `DRAINED`, `RECONCILED` or `NONE` from those locked rows and rejects a mismatched caller assertion;
10. for forward cutover or completed rollback, locks and proves the exact active generation, fourteen day rows and sync state;
11. verifies the exact deferred-overlay generation/date/source-row hashes;
12. verifies all three visible cursors equal the locked canonical version and the expected cursors;
13. verifies reconciliation is no older than the latest generation, availability or projection fact;
14. appends the immutable database-winner snapshot, updates entitlement and changes mode in one item subtransaction;
15. completes the durable batch receipt with the explicit per-item outcomes.

## Closed transition rules

| Prior mode | Permitted result | Mandatory facts |
| --- | --- | --- |
| `GOOGLE_PRIMARY` | `GOOGLE_PRIMARY` no-op or source-link preparation | Exact expected scope/entitlement; no authority switch |
| `GOOGLE_PRIMARY` | `SUPABASE_PRIMARY` | One current source, complete fresh generation, exact cursors/reconciliation, no unresolved work, enabled entitlement only if global feature is enabled |
| `SUPABASE_PRIMARY` | `SUPABASE_PRIMARY` no-op/entitlement update | Exact scope/entitlement and derived in-flight state; enabling still requires global switch |
| `SUPABASE_PRIMARY` | `ROLLBACK_PENDING` | Global feature already disabled, entitlement disabled, no unresolved work |
| `ROLLBACK_PENDING` | `ROLLBACK_PENDING` no-op | Exact scope/entitlement |
| `ROLLBACK_PENDING` | `GOOGLE_PRIMARY` | One current source, complete fresh generation, exact cursors/reconciliation and no unresolved work |

All other edges are rejected. `CANCELLED` is accepted as a syntactically known request value only so older callers fail deterministically; PostgreSQL never derives it, so it cannot authorise a transition.

## Source authority

A strict authority switch requires:

- exactly one time-current `PRIMARY` `GOOGLE_CREDENTIALLY_PUBLIC_ID` link;
- exactly one active link group across `PRIMARY`/`OVERLAP` rows;
- the expected environment and Candidate scope;
- row locks covering the catalogue used by the decision.

Missing, expired/disabled and ambiguous source authority fail closed. A request may propose a bounded source-link record for preparation, but the database still owns uniqueness, Candidate mapping and the resulting catalogue. A same-mode source preparation is not a cutover and does not bypass the later strict switch proof.

## Generation, cursor and freshness authority

For an authority switch, the active generation must:

- equal the caller's expected generation UUID and version;
- belong to the same environment/Candidate scope;
- be `ACTIVE`, activated and published;
- contain exactly fourteen expected, actual and persisted day rows;
- have been published within the bounded 120-second cutover proof window.

The sync state must exist and be `READY`. Accepted, required-visible and effective-visible cursors must all equal both the expected values and the locked scope canonical version. Pending, retry and terminal counts must be zero, the observed source revision must be non-empty, and reconciliation must not predate any relevant current fact.

## In-flight and overlay authority

PostgreSQL derives:

- `NONE` when a pending, claimed, retry, terminal projection, in-progress command, other in-progress batch, or in-progress/unknown external effect exists;
- `RECONCILED` when no unresolved owner exists but an exact current `DEFERRED_OVERLAY` remains;
- `DRAINED` only when neither unresolved work nor deferred overlay exists.

A deferred overlay is exact only when its generation ID/version, date and source row hash match a booked or system-blocked day in the active generation. Invalid overlay evidence produces `PROJECTION_STALE_COMPLETION`.

## Cohort, replay and concurrency authority

Each item runs in an isolated PL/pgSQL subtransaction. Every per-item scalar/record is reset before evaluation. An expected business rejection rolls back that item's source/scope changes and produces an explicit `REJECTED` outcome without contaminating another item.

The batch receipt remains the same factual idempotency owner:

- same key and identical factual request returns the stored result with the internal replay marker;
- same key with changed facts raises `IDEMPOTENCY_KEY_REUSED`;
- concurrent exact callers serialize on the batch receipt and return one result;
- concurrent different-key cutovers serialize on the deterministic scope lock, yielding one committed winner and one explicit stale-precondition rejection.

An exact no-op returns `NO_CHANGE` and does not append a transition ledger row.

## Preserved boundaries

R9 preserves:

- exactly twelve Candidate Daily tables;
- exactly thirteen Candidate Daily public service RPCs;
- all existing signatures, `SECURITY DEFINER` posture, closed search paths and grants;
- Phase 1A transport and Phase 1B response mappings;
- the disabled feature/empty-data TEST posture;
- minimal legacy Google change and all later Phase 3 requirements;
- Candidate core, Office, finance, Invoice, Banking Pay, Policy X, provider, settlement and remittance authority;
- no production access or deployment.

## Required re-audit

The independent reviewer must execute the direct R9 SQL suite and parallel Node/PostgreSQL test on PostgreSQL 17.6 and 18.1, inspect the installed TEST function definition after publication, and independently reproduce at least: valid forward cutover, rollback, missing/partial/stale/mismatched generation, missing/ambiguous/disabled source, cursor lag, invalid/valid overlay, every unresolved owner, falsified disposition, replay/conflict, parallel same/different keys, no-op and partial cohort behaviour.

