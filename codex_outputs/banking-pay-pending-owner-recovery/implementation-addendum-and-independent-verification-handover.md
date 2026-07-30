# Banking Pay Source-Build Owner Recovery Completion

## Implementation addendum and independent-verification handover

Date: 30 July 2026  
Environment: TEST only  
Implementation commit: `279f1892f82423cba6f77662278f8dea03be7406`

## Correction to the historical incident report

The historical report incorrectly called the incident candidate James Terwane.
The source-build-owner incident candidate was **Eduardo Almeida**.

Original TEST incident identifiers:

- workbench session: `f3523145-c8d6-42e1-9e95-510e4da1db67`
- candidate: `6fad0e88-ab7f-4760-88fa-a9a0250b1d5e`
- original failed source-build job: `fe761ec6-cce7-4fe7-828b-64da75e07e07`
- original database failure: PostgreSQL `55P03` lock not available

The historical report remains intact. This addendum corrects its candidate name
and records the narrowly scoped completion delta.

## Outcome

The locked **Banking Pay Source-Build Owner Recovery Completion Plan** has been
implemented without implementing or modifying the separate
**NHSP/HealthRoster Weekly Authoritative Amendments Plan**.

Exactly three existing production database functions changed:

1. `public.pay_workbench_repair_orphaned_pending_source_build`
2. `public.pay_workbench_session_get_progress_light`
3. `public.pay_workbench_session_recompute_progress_counters`

No production function was added.

No table, column, constraint, enum, index, trigger, policy, RLS rule, or other
schema object was added or altered.

No backend Worker source was changed by this implementation.

No frontend source was changed by this implementation.

No Worker was deployed.

No frontend was deployed.

Only the three function definitions were installed in TEST Supabase.

The repair helper was not invoked against Eduardo or any other application
candidate as part of this completion deployment. A post-install read-only
check found zero open `SOURCE_BUILD_PENDING` scopes and zero invalid/stale open
owners, so no one-off repair was required.

## Exact source files

Canonical progress definitions:

`supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql`

Helper definition:

`supabase/repeatable/30072026_1310_pay_workbench_repair_orphaned_pending_source_build.sql`

Static tests:

`tests/banking-pay-pending-owner-recovery.test.cjs`

Disposable-database transactional tests:

`tests/banking-pay-pending-owner-recovery.transactional.test.cjs`

Narrow rollback:

`codex_outputs/banking-pay-pending-owner-recovery/30072026_1754_banking_pay_source_build_owner_recovery_completion_rollback.sql`

There remains exactly one repository definition of each affected function
across `supabase/migrations` and `supabase/repeatable`.

## Function-by-function implementation

### 1. `pay_workbench_repair_orphaned_pending_source_build`

Signature, parameter defaults, `SECURITY DEFINER`, search path, owner, ACL, and
the existing 1–25 bounded sweep limit were preserved.

The outer discovery query now joins the live candidate change counter and
selects an owner whose syntactically valid numeric `source_change_seq` is lower
than live truth. The unlocked selector remains discovery only. The helper then
locks and re-reads the session and scope, reads the current owner and live
sequence, and recalculates owner validity.

Each candidate is processed inside its own PL/pgSQL exception/subtransaction
boundary. A candidate-specific postcondition failure rolls back only that
candidate. Earlier proven repairs remain committed within the helper
invocation, and later candidates continue up to the bounded limit.

Reusable completed source-build authority now requires all of:

- `SUCCEEDED`;
- non-null `completed_at_utc`;
- null `failed_at_utc`;
- exact session and candidate;
- canonical source-build job type;
- exact current session version;
- numeric source sequence at least equal to locked live truth;
- valid source-build run UUID;
- at least one exact `CURRENT` source row matching the job's session,
  candidate, version, run ID, and source sequence;
- no current-version `DIRTY` or `ERROR` source row for the candidate.

A mixed `CURRENT`/`DIRTY` build is therefore not reused. It falls through to
the existing active-successor or canonical-enqueue recovery ladder.

Canonical reconciliation is still performed only by
`pay_workbench_reconcile_successful_source_build`, which was not changed. The
helper now passes:

`p_recompute_session_progress => false`

The reconciliation call has its own nested subtransaction. Its result must be
a JSON object with `ok=true` and `skipped=false`. The helper then locks and
proves:

- the scope moved out of `SOURCE_BUILD_PENDING`;
- it no longer points to the invalid old owner;
- it is clean;
- an exact current source authority row remains;
- no current-version dirty/error source row remains;
- any remaining downstream owner is locked and is a valid active job for the
  same session, candidate, and session version.

If reconciliation raises, is skipped, returns an invalid result, or cannot
prove the postcondition, that reconciliation subtransaction rolls back and the
helper continues the established recovery ladder.

The active-successor rebind now:

- keeps the existing validity predicate and deterministic ordering;
- captures `ROW_COUNT`;
- re-reads the scope;
- re-reads live sequence;
- revalidates the exact successor;
- counts the branch only after the exact scope/job postcondition is proven.

Both fail-close paths now:

- capture `ROW_COUNT`;
- re-read the scope;
- prove `SOURCE_BUILD_ERROR`, cleared ownership, dirty state, the existing safe
  error code, and `automatic_recovery_scheduled=false`;
- count the branch only after proof.

Canonical enqueue still calls only:

`public.pay_workbench_enqueue_candidate_refresh`

Its existing payload and `PRE_DRAFT_LIVE_TRUTH` authority are preserved. The
returned successor is validated against exact session, candidate, active
status, canonical type, version, live sequence, run ID, and scope binding. A
final locked scope re-read is required before the branch is counted.

After any proven ownership/source transition, the helper separately calls:

`public.pay_workbench_session_recompute_progress_counters`

The recompute runs in its own nested subtransaction with the locked plan's
branch-specific reason. A recompute failure rolls back only recompute writes;
it cannot undo the already-proven ownership transition.

Aggregate repair counters are updated only after the candidate subtransaction
returns. A candidate whose transition cannot be proved returns the bounded
safe result:

- action `UNRESOLVED_POSTCONDITION_NOT_PROVEN`;
- `state_transition_proven=false`;
- `repaired=false`;
- code `WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB`;
- reason `POSTCONDITION_NOT_PROVEN`;
- `retry_safe=true`.

No SQL error message, exception detail, context, job payload, scope error
payload, or banking payload is returned.

Added aggregate result fields:

- `unresolved_count`;
- `progress_recomputed_count`;
- `progress_recompute_failed_count`;
- `all_state_transitions_proven`;
- `all_progress_recomputed`;
- `partial`.

Added per-candidate fields:

- `state_transition_proven`;
- `repaired`;
- `progress_recomputed`;
- `progress_recompute_error_code`.

All previous result fields remain.

### 2. `pay_workbench_session_get_progress_light`

The owner-validity and recovery classification predicate is unchanged in
meaning.

The previous boolean-only successor `EXISTS` check was replaced by one bounded
`LEFT JOIN LATERAL` lookup that returns the exact selected successor.

Ordering is:

1. `RUNNING` before `QUEUED`;
2. highest parsed source-change sequence;
3. oldest `created_at_utc`;
4. lowest UUID.

Each bounded owner-failure sample now adds:

- `successor_job_id`;
- `successor_job_status`.

No pending/failed/recovery count, readiness field, blocker, phase, status text,
selection count, preview count, or `work_queued` rule changed.

### 3. `pay_workbench_session_recompute_progress_counters`

This function received only the same deterministic successor lookup and two
additive sample fields as progress-light.

The validity predicate and ordering are textually identical between the two
functions.

No `p_apply` behavior, read-only/write mode, count classification, draft
readiness, session readiness, phase, or progress write behavior changed.

## Explicitly unchanged production areas

The implementation did not amend:

- `pay_workbench_claim_due_jobs`;
- `pay_workbench_fail_job`;
- `pay_workbench_worker_drain_chunk`;
- `pay_workbench_worker_drain_chunk_revalidated_v1`;
- `pay_workbench_enqueue_candidate_refresh`;
- `pay_workbench_enqueue_candidate_refresh_many`;
- `pay_workbench_repair_invalid_source_build_poison`;
- `pay_workbench_reconcile_successful_source_build`;
- `pay_workbench_projection_lifecycle_repair`;
- `pay_workbench_complete_job`;
- `pay_workbench_session_get_progress`;
- `pay_workbench_session_get_preview`;
- `pay_workbench_session_seed_scope_chunk`;
- `WAIT_FOR_PREVIEW_READY`;
- preview or create-draft Worker routes;
- Worker failure handling;
- RPC permissions;
- frontend progress, reset, polling, or modal code;
- payment calculations or economic keys;
- CSV generation;
- PAYE or umbrella treatment;
- daily, NHSP, or HealthRoster treatment;
- Policy X;
- persisted selections or resolutions.

## TEST database installation

Target was confirmed as the `test-cloudtms` Supabase project. A bounded smoke
query succeeded on PostgreSQL 17.6.

The three saved definitions first compiled against the live TEST schema inside
a deliberate rollback.

Pre-delta installed `pg_get_functiondef` MD5 hashes:

- helper: `977f2aa68b33a10649c69e308cf86e16`
- progress-light: `64a227e561acf1be8bf434b13dd253c7`
- recompute: `0830bcf4a7895de0cfee6960120580df`

The installation bundle was mechanically extracted from the two canonical
repeatable files and installed the functions atomically in this order:

1. progress-light;
2. recompute;
3. helper.

Final installed `pg_get_functiondef` MD5 hashes:

- helper: `78d2a4ac9dd7b8309ed5c77112d981f0`
- progress-light: `497a7be67673cae16b2d95e47290fd3c`
- recompute: `acc358aa65a14b4466cc47919d7132e5`

Post-install metadata verification:

- all three signatures: unchanged;
- all defaults: unchanged;
- owner: `postgres`;
- `SECURITY DEFINER`: true;
- search paths and recompute PL/pgSQL settings: unchanged;
- comments: null, unchanged;
- helper execute ACL: `postgres` and `service_role` only;
- progress-light/recompute execute ACL: `postgres`, `authenticated`, and
  `service_role`, unchanged.

No helper repair call followed the installation.

## Verification

### Static/source tests

`node --test tests/banking-pay-pending-owner-recovery.test.cjs`

Result: 13 passed, 0 failed.

These tests cover:

- single canonical definition;
- numeric stale-owner discovery;
- after-lock live-sequence revalidation;
- exact completed-build authority;
- dirty/error exclusion;
- reconciliation with recompute false;
- result and postcondition validation;
- manual `ROW_COUNT` checks;
- aggregate accounting outside the candidate boundary;
- separate recompute isolation;
- safe result fields;
- deterministic successor diagnostics;
- exact progress/recompute lookup parity;
- unchanged existing claim, failure-handler, Worker drain, and draft-gate
  contracts.

### Disposable PostgreSQL transactional tests

`BANKING_PAY_OWNER_RECOVERY_TRANSACTIONAL=1 node --test tests/banking-pay-pending-owner-recovery.transactional.test.cjs`

Result: 15 passed, 0 failed.

T01–T14 prove:

- numeric stale active owner replacement;
- current active owner no-op;
- usable completed-build reconciliation;
- mixed `CURRENT`/`DIRTY` rejection;
- skipped reconciliation fallback;
- reconciliation exception rollback/fallback;
- active-successor rebind postcondition;
- maximum-attempt fail close;
- canonical enqueue identity/binding;
- candidate-local zero-row rollback while preserving an earlier repair;
- successful recompute for reconciliation, rebind, enqueue, and fail close;
- recompute-failure isolation and later convergence;
- deterministic successor ID/status parity;
- concurrent repair convergence without duplicate authority or deadlock.

The fifteenth test applies the committed rollback in the disposable database
and proves that it restores exactly the three pre-delta hashes.

### Wider Banking Pay source suite

`node --test tests/banking-pay-*.test.cjs`

Result:

- 187 passed;
- 14 transactional cases skipped because the explicit disposable-database
  environment switch was not set in that invocation;
- 3 failed.

The three failures are pre-existing/out-of-scope correction-chain assertions:

- correction-chain entry-point checker guard;
- exact durable first-page reconciliation attestation;
- canonical correction-carrier suggested-rate evidence.

None is in an amended function or file from this implementation. Concurrent
invoice/PDF/correction work present in the shared worktree was preserved and
was not staged, committed, deployed, or modified by this work.

### TEST financial-data integrity

Before and after the three-function install, exact row counts and full-row
hashes were unchanged for:

- 571 Banking Pay preview rows;
- 5,239 candidate source rows;
- 1,858 frozen pay-batch items;
- 273 pay-batch candidates.

The full `pay_batches` table hash changed during the verification window while
its row count remained 316. Read-only inspection showed the normal background
completion-notice retry timestamps advancing on settled batches. The function
installation contains DDL only and does not reference or update `pay_batches`.
Frozen batch-item and candidate authority remained byte-for-byte unchanged.

No provider, bank, payment execution, settlement, remittance, email, or
completion-notice action was invoked by this implementation or its tests.

## Rollback

The new rollback restores only the three definitions that existed immediately
before this completion delta. It does not drop the helper and does not restore
the original vulnerable pre-recovery implementation.

It includes:

- one transaction;
- a transaction-scoped advisory lock;
- guards for all three expected post-delta hashes;
- the three mechanically captured pre-delta definitions;
- exact owner, ACL, and null-comment restoration;
- guards for all three expected pre-delta hashes;
- commit only after every guard passes.

It was applied successfully only in the disposable PostgreSQL test database.
It was not run against TEST Supabase.

## Policy X and safety

This completion remains entirely inside:

`PRE_DRAFT_LIVE_TRUTH`

It does not alter post-draft frozen authority.

It does not alter:

- `TS_DAY`;
- hours, rates, gross/net, holiday pay, employer NI, VAT, margin, or expenses;
- selection;
- draft eligibility rules;
- frozen batch items;
- frozen source evidence;
- CSV content;
- payment rail or provider behavior;
- settlement or remittance behavior.

Safety record:

- TEST Supabase only;
- production access/deployment: none;
- destructive application-data SQL: none;
- payment/draft/provider/settlement/remittance action: none;
- repair invocation: none;
- secrets printed or committed: none;
- raw diagnostics committed: none;
- Worker deployment: none;
- frontend deployment: none;
- Policy X drift identified: none.

---

# Mandatory instructions for the independent reviewing chat

This is verification only. Do not write, deploy, or mutate code or data.

Review the saved backend commit, the three installed TEST database functions,
the two focused test files, this addendum, and the narrow rollback.

The review must remain within the **Banking Pay Source-Build Owner Recovery
Completion Plan**. Do not expand it into the separate NHSP/HealthRoster plan,
a Banking Pay redesign, Worker architecture, frontend polling/reset work,
preview/draft redesign, payment economics, or unrelated correction-chain work.

Return a function-by-function verdict for exactly these three production
functions:

1. `pay_workbench_repair_orphaned_pending_source_build`
2. `pay_workbench_session_get_progress_light`
3. `pay_workbench_session_recompute_progress_counters`

For each, return one of:

- `VERIFIED — NO CHANGE REQUIRED`, with concrete source/runtime/test evidence;
- `AMEND`, with a highly detailed, function-by-function implementation plan
  naming the exact existing file, exact branch/condition, locks, predicates,
  postconditions, result fields, tests, rollback effect, and deployment
  verification still required.

Do not propose a new production function or schema object unless you can prove
that the locked brief cannot be met inside the three approved existing
functions. A preference for broader architecture is not evidence.

If any gap is found, the resulting implementation plan must:

- be limited to the smallest exact correction required;
- preserve the existing canonical enqueue and reconciliation pathways;
- preserve the candidate-local transaction boundary;
- preserve separate recompute failure isolation;
- preserve progress-light/recompute semantic parity;
- preserve all existing preview, readiness, draft, selection, CSV, payment,
  provider, settlement, remittance, and Policy X behavior;
- identify which focused transactional test must be added or amended;
- not reopen functions already proved correct without concrete contradictory
  evidence.

The reviewer must explicitly answer:

1. Is numeric stale-owner discovery complete?
2. Is after-lock owner/live-sequence revalidation complete?
3. Can mixed current/dirty evidence ever be reused?
4. Can skipped, malformed, or raising reconciliation be counted as repaired?
5. Is every manual transition counted only after `ROW_COUNT` and exact
   postcondition proof?
6. Can a candidate-local failure undo an earlier candidate repair?
7. Can recompute failure undo a proven ownership transition?
8. Do unresolved candidates remain visible and retryable?
9. Are successor ID/status deterministic and additive only?
10. Do progress-light and recompute preserve their prior count/readiness/phase
    meaning?
11. Is there exactly one canonical definition of each affected function?
12. Are signatures, defaults, owners, ACLs, security mode, settings, and
    comments preserved?
13. Is the rollback exact and narrowly guarded?
14. Was any Worker/frontend/schema/timesheet-type/payment behavior changed?
15. Is there any Policy X drift?

If all requirements are satisfied, state that no further implementation is
required. If not, provide the requested tightly scoped function-by-function
delta only.
