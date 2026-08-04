# Banking Pay bounded-scope Version 1.2.4 — Stage 1 implementation report

## Outcome

Stage 1 is implemented in the isolated bounded-scope worktree. It contains the
schema foundation, new private functions, the two service-role RPCs, mapped
replacement functions, invalidation triggers, legacy bootstrap artifact,
transactional rollback artifact, verification SQL and focused regression tests.

The implementation has not been applied to TEST. No Worker or frontend file was
changed, no Worker was deployed, and no commit, push or pull request was made.

## Implemented database surface

- Eight new durable tables in the existing `private` schema.
- Two existing public tables extended: `banking_pay_workbench_jobs` and
  `settings_defaults`.
- Exactly 88 active named constraints, including the PostgreSQL 17.6
  `UNIQUE NULLS NOT DISTINCT` canonical-stage identity.
- Exactly 47 active indexes, including the active-state selector and incomplete
  scope partial indexes used by bounded terminal checks.
- Nine new database function identities:
  - seven postgres-only `private` helpers;
  - two service-role-only public claim/start and exact-attempt execute RPCs.
- Fifteen planned Workbench function replacements, two candidate-deletion
  lifecycle replacements, and three additional installed source-job writer
  replacements required to preserve the one permitted typed null-build state.
- Sixteen new statement-level transition-table invalidation triggers.
- Three retained finance dirty trigger identities adapted from `AFTER ROW` to
  `BEFORE ROW` so exact OLD/NEW expected-effect evidence exists before finance
  DML; rollback restores their installed timing and definitions.

The three additional installed writer replacements are:

- `pay_workbench_repair_invalid_source_build_poison`;
- `pay_workbench_session_clone_eligible_rows_v1`;
- `pay_workbench_session_replay_replaced_queue_v1`.

Without those replacements, installed direct source-job insert paths could
create rows that violate the typed initial-job invariant.

## Core behavior implemented

- Ordinary candidate discovery starts at indexed `DIRTY`/`LIVE` state and does
  not begin with lifetime-history tables.
- Dependency closure is complete, resumable and uncapped by the former
  100-member authority.
- Facts are collected in bounded, idempotent pages with immutable cursor-chain,
  count and digest evidence.
- Candidate-wide financial reconciliation runs only after all page-composable
  facts and dependency units are sealed.
- Canonical rows are staged privately and publication switches complete
  `CURRENT` source authority atomically.
- Material delivery uses a committed claim/start transaction and a second exact
  nonce-bound execution transaction.
- Expired, cancelled, duplicate, stale and superseded attempts fail closed and
  converge through durable attempt evidence.
- Reconciliation scale outside the active measured envelope blocks
  non-terminally without finance DML or partial publication.
- Legacy data is discovered and classified in bounded streams. Complete
  dependency units are classified before only the active/uncertain remainder is
  reconciled.
- Terminal scope sealing consumes bounded cursor counters and rolling digests;
  the final completion path does not rescan the whole build.
- Candidate deletion is fenced against concurrent builds and clears typed job
  metadata only at the terminal detachment boundary.

## Repository artifacts

Auto-run repository SQL:

- `supabase/migrations/04082026_1134_banking_pay_bounded_scope_v12.sql`
- 30 timestamped files under `supabase/repeatable/04082026_*.sql`

Non-auto-run operational evidence:

- `codex_outputs/banking-pay-bounded-scope-v12/bootstrap-test.sql`
- `codex_outputs/banking-pay-bounded-scope-v12/rollback.sql`
- `codex_outputs/banking-pay-bounded-scope-v12/verification.sql`
- `codex_outputs/banking-pay-bounded-scope-v12/performance-evidence.md`

Tests:

- `tests/banking-pay-workbench-bounded-scope-v12.test.js`
- `tests/banking-pay-workbench-two-call-v12.test.js`
- bounded-scope assertions added to
  `tests/banking-pay-workbench-refresh.test.cjs`

## Validation completed

### Disposable PostgreSQL validation

The migration and all 30 repeatable SQL files compile and install in dependency
order in a disposable local PostgreSQL 18.1 cluster. The target installed TEST
baseline remains PostgreSQL 17.6.1; no TEST DDL or DML was executed.

The installed local catalog reports:

- 8 new private tables;
- 88 named constraints;
- 47 indexes;
- 9 new function identities and zero new overloads;
- 16/16 new triggers are statement-level;
- 3/3 retained expected-effect triggers are `BEFORE ROW`;
- no malformed typed source jobs;
- no duplicate active builds or active attempts;
- no dangling build/job authority;
- no private table grants to `PUBLIC`, `anon`, `authenticated` or
  `service_role`;
- private helper execution granted only to `postgres`;
- public RPC execution granted only to `postgres` and `service_role`;
- empty hardened `search_path` on all nine new functions.

`EXPLAIN` uses:

- an index-only scan on the active timesheet-state selector;
- an index-only scan on the incomplete-scope partial index for the final
  bounded completion check.

### Rollback validation

The rollback artifact was executed with `ON_ERROR_STOP` in a disposable clone.
It committed successfully and proved:

- all eight private tables removed;
- all nine new function identities removed;
- all five job metadata columns removed;
- all three settings columns removed;
- all 16 new triggers removed;
- all three retained dirty triggers restored to installed `AFTER ROW` timing;
- all mapped installed function definitions restored to their captured MD5
  values, including source build, public sync, queue, invalidation and the three
  additional source-job writers.

### Bootstrap validation

The non-auto-run TEST bootstrap artifact was parsed and executed against a
schema-aligned disposable clone with no eligible candidates. It committed with
zero registry rows and zero queued jobs. It has not been run on TEST and remains
guarded as an explicitly approved post-install operation.

### Automated tests

- Focused bounded-scope/two-call/refresh tests: **32 passed, 0 failed**.
- Configured repository `npm test`: **206 passed, 0 failed**.
- `git diff --check`: passed.

An additional broad `node --test tests/*.test.cjs` diagnostic reported 284
passes, 17 skips and 7 failures. A clean archive of the frozen implementation
baseline reports 5 of those same failures before this work. The remaining two
are legacy static assertions that require one repository source definition for
`pay_sync_overpayments_from_preview` and
`pay_workbench_repair_orphaned_pending_source_build`; Stage 1 correctly supplies
new timestamped `CREATE OR REPLACE` repeatables with the same installed
identities and introduces no database overload. These two legacy assertions
must be reconciled in the designated integration pass after the cancellation
work is rebased. The shared legacy test files were deliberately not edited here
to avoid a concurrent ownership collision.

## Required post-merge and pre-cutover evidence

The following remains blocking before TEST cutover and is intentionally not
claimed as Stage 1 local proof:

- rebaseline installed function hashes immediately before application;
- apply migration first, then repeatables, only with explicit TEST approval;
- run the read-only verification artifact against installed TEST;
- run combined bounded-scope, cancellation-refresh, catalog-validator and
  Policy X tests after merging the parallel implementations;
- reconcile the two legacy repeatable-source-count assertions against the final
  integrated source layout;
- execute financial `EXCEPT ALL`, multiplicity, ordered JSON and digest
  equivalence;
- benchmark 1/100/1,000-row trigger paths;
- prove 1 LIVE versus 10,000 CLOSED + 1 LIVE history independence;
- prove 101- and 500-relevant-timesheet correctness and admitted-envelope
  performance;
- test crash, cancellation, late nonce, expiry and attempt exhaustion;
- prove atomic publication visibility under concurrent readers;
- benchmark one lane and test four lanes separately without enabling it unless
  all contention and fairness gates pass;
- run bounded legacy bootstrap and cleanup interruption/resumption tests.

No failed measurement authorises higher timeouts, truncated scope, partial
publication or per-row Worker fanout.

## Policy X and frozen contracts

The implementation changes pre-draft freshness and orchestration only.
Post-draft authority continues to use frozen batch artifacts. It does not alter
the Policy X economic-key ladder, `TS_DAY` date identity, Banking Pay public
arguments or response shapes, source-line public identity/order, frontend
polling, Draft economics, payment, settlement, remittance, provider submission,
webhooks, PAYE/Umbrella rules, VAT or TSFIN formulas.

## Safety and delivery status

- Secrets printed: no.
- Destructive SQL/RPC/actions against TEST: no.
- TEST data changed: no.
- Normal TEST Worker deployed: no.
- Isolated Worker deployed: no.
- Production accessed or deployed: no.
- Raw diagnostic logs committed: no.
- Commit/push/PR: no.
- Policy X drift detected: no.

