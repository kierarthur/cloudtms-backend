# CloudTMS Candidate App - Phase 2 and Phase 1B R10 Rollback Correction Handover

Date: 17 August 2026

Environment: TEST only

## 1. Executive disposition

This package implements the one bounded blocker from the independent R9 review:

```text
SUPABASE_PRIMARY
-> ROLLBACK_PENDING
while PostgreSQL derives NONE because unresolved work exists
```

R10 prevents every authority-mode change from committing with database-derived `NONE`.

The implementation is deliberately narrow:

```text
Runtime SQL functions changed:       1 existing function definition
Tables added/changed:                0
RPC signatures added/changed:       0
HTTP routes/contracts changed:      0
Backend Worker source changed:      0
Frontend source changed:            0
Google source/deployment changed:   0
Financial/Banking Pay owners:       0
Production changes:                 0
```

Candidate Daily Phase 1B's independent GO remains in force. R10 requests the outstanding Phase 2 GO only. Phase 3 remains blocked until independent R10 acceptance.

## 2. Incoming R9 defect

R9 correctly moved authority proof into PostgreSQL. It locks the current scope, entitlement, source links, active generation, sync state, commands, other batches, effects and projection outbox, then derives:

```text
NONE        unresolved owner exists
RECONCILED  no unresolved owner; exact deferred overlay remains
DRAINED     neither unresolved work nor deferred overlay remains
```

It also rejects a caller assertion that differs from this database result.

The defect was that the first rollback edge did not use the later `v_strict_barrier` block. Therefore:

```text
caller says NONE
database derives NONE
equality check passes
first rollback commits
```

That violated AV-237 and AV-239. A truthful description of unresolved work is not authority to move ownership away from Supabase.

## 3. Corrected database sequence

For each transition item the existing locked owner now performs:

1. signed-system context, batch, actor, independent approver, reason and evidence validation;
2. durable batch receipt acquisition and exact replay/conflict handling;
3. global feature lock and deterministic Candidate scope locks;
4. exact scope/entitlement and closed-edge validation;
5. source-link, command, other-batch, effect and projection row locks;
6. database derivation of `NONE`, `RECONCILED` or `DRAINED`;
7. caller/database disposition equality check;
8. **R10 guard: changed mode plus `NONE` returns `CANDIDATE_DAILY_NOT_READY`;**
9. any later R9 strict generation/source/cursor/reconciliation/overlay proof applicable to the edge;
10. immutable transition append, entitlement update and authority-mode update in the same item subtransaction.

The R10 guard is downstream of database derivation and upstream of all persistent transition changes.

## 4. Stable outcome contract

| Request/database facts | Result |
| --- | --- |
| Caller `DRAINED`; database `NONE` | `SEMANTIC_REJECTION` |
| Caller `NONE`; database `NONE`; mode changes | `CANDIDATE_DAILY_NOT_READY` |
| Caller `NONE`; database `NONE`; exact same-mode/same-entitlement no-op | `NO_CHANGE` |
| Caller/database `DRAINED`; every other prerequisite passes | transition may commit |
| Caller/database `RECONCILED`; exact overlay and every other prerequisite passes | transition may commit |

This preserves caller-conflict diagnostics while making the database readiness barrier unambiguous.

## 5. Complete adversarial matrix

The updated direct SQL suite covers the first rollback edge for:

- PENDING, CLAIMED, RETRY and TERMINAL projections;
- false `DRAINED` and truthful `NONE` assertions for each projection state;
- IN_PROGRESS Candidate command;
- another IN_PROGRESS Candidate Daily batch;
- IN_PROGRESS external effect;
- UNKNOWN external effect.

Every case asserts no authority, entitlement, ledger or fence drift.

The real Node/PostgreSQL integration test creates one pending projection and races two different-key first rollback calls in independent sessions. Both must return `CANDIDATE_DAILY_NOT_READY`; the scope must remain `SUPABASE_PRIMARY`, the fence false and transition count zero.

The existing R9 suite remains intact for valid forward, settled rollback, final rollback, valid/invalid overlay, generation/cursor/freshness, replay/conflict, no-op, partial cohort and concurrency behaviour.

## 6. Exact changed source boundary

Runtime and tests:

```text
supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql
tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql
tests/candidate-daily-authority-transition-concurrency.integration.js
tests/candidate-daily-phase2-source-contract.test.js
```

Current authority/documentation:

```text
docs/candidate-app/AUTHORITY_MAP.md
docs/candidate-app/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md
docs/candidate-app/CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md
docs/candidate-app/CANDIDATE_DAILY_PHASE2_PHASE1B_R10_ROLLBACK_AUTHORITY.md
docs/candidate-app/IMPLEMENTATION_PLAN.md
docs/candidate-app/build_candidate_daily_r10_decisions_pdf.py
```

Release evidence and final pack documents are included in the archive and identified by its manifests.

## 7. Verification summary

The final immutable facts are recorded in `03_VERIFICATION_SUMMARY.md`, `02_CURRENT_STATE.md`, `PROVENANCE.json` and the R10 Decisions PDF.

Required gates include:

- exact source/static contract;
- direct rollback matrix on PostgreSQL 17.6 and 18.1;
- real two-session concurrency on both engines;
- complete backend JavaScript suite;
- exact Candidate DB GitHub workflow on both engines;
- safe TEST migration at the published commit;
- installed repeatable and canonical function-definition hashes;
- disabled/empty TEST snapshot;
- health/readiness smoke for the unchanged deployed Workers.

No real Candidate authority transition, external effect, Candidate email, push, R2 object, Google change or financial mutation is part of verification.

## 8. Decisions and documentation authority

The package contains the complete R9 Decisions PDF unchanged as the base and a rebuilt current R10 Decisions PDF. Sections 1-84 remain byte/text preserved. Sections 85-88 add the R10 controlling correction and decisions AV-245 through AV-249.

The later-controlling rule is:

> Database-derived `NONE` never authorises a changed Candidate Daily authority mode.

All accepted minimal-change legacy, Availability/Emergency continuity, Master Rota dual-publication, Phase 3-7, Candidate core/Office and no-financial-drift decisions remain in force.

## 9. Safety and no-change statement

R10 authorises no feature activation or user journey. The safe position remains:

```text
Candidate feature flags:             all false
Candidate core rows:                 zero
Candidate Daily rows:                zero
Candidate-bound mail rows:           zero
Candidate emails/pushes/R2 writes:   zero
Google Apps Script/Sheet writes:     zero
Financial/Banking Pay changes:       zero
Production changes:                  zero
```

The repeatable installation changes only the existing function definition. It does not create or mutate Candidate business data.

## 10. Independent review instructions

Use `01_INDEPENDENT_REVIEW_BRIEF.md` as the required protocol. Review the operation, not merely source tokens. Run both engine suites, reproduce the incoming R9 defect against the supplied baseline, inspect the installed TEST function and prove all rejection postconditions.

If every gate passes and no concrete supported blocker remains, issue GO for Candidate Daily Phase 2 and stop. That GO permits the planned Phase 3 Google coexistence gate only. It does not enable Candidate Daily or complete the app.

## 11. Remaining full-implementation phases

After independent R10 GO:

| Phase | Remaining outcome |
| --- | --- |
| Phase 3 | Minimal Availability Apps Script compatibility adapter, Master Rota signed dual publisher, projection/effect adapters and coexistence outage/recovery proof |
| Phase 4 | Complete Daily responsive web/iOS/Android UI plus shadow parity and retained specialist journeys |
| Phase 5 | Controlled TEST cutover with identity/parity/soak/error-budget/rollback evidence |
| Phase 6 | Complete Emergency, cannot-attend, leave-early, running-late, DNA, messages/content, Past Shifts and DAILY signing/EMAIL/PHONE acceptance across old/new paths |
| Phase 7 | Gradual entitled rollout, monitoring and separately authorised legacy-browser/compatibility-adapter retirement |

Availability, Emergency, Master Rota publication, projections/freshness and retained specialist services survive legacy-browser retirement until separately migrated and accepted.
