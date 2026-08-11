# Candidate rejected-resubmission idempotency closure

Date: 11 August 2026

Runtime commit: `d63bf708cdaf8de5124b0199b2b7eb79e4d13e6b`

Runtime parent: `fb3d54a3d85b3e3357d3fbbf1416e5ba6d13c2ac`

## Scope and disposition

This correction closes the independent audit finding that rejected-workflow resubmission was not atomically bound to its rejected source or to a canonical request identity. It does not implement the CloudTMS office frontend, change the Candidate feature state, alter financial authority, or modify Banking Pay.

The office-frontend implementation remains a separate stage. Its full implementation plan may be completed in parallel as a planning deliverable, but frontend coding remains deferred pending independent backend/API approval.

## Runtime correction

### Durable replacement lineage

The existing `candidate_submission_workflows` table now has a nullable `replacement_of_workflow_id` self-reference with `ON DELETE RESTRICT`, a not-self constraint and a partial unique index. There is still no eighth Candidate table.

The unique source-lineage rule gives the database one durable answer to both questions:

- which rejected workflow a replacement belongs to; and
- whether that rejected workflow already has a replacement.

### Atomic rejected resubmission

The existing `candidate_workflow_transition_atomic_v1` RPC now owns `RESUBMIT_REJECTED`. There is still no fifteenth public Candidate business RPC.

The action:

1. validates the selected Candidate, source workflow and expected generation;
2. acquires the account/idempotency-key advisory lock and rejected-source advisory lock;
3. locks and validates the immutable `REJECTED` source;
4. accepts an exact retry only where durable source lineage and canonical identity agree;
5. rejects the source's original key and cross-source or conflicting key reuse;
6. rejects a second key after another replacement has won;
7. derives kind, scope, route and current WEEKLY or DAILY identity in the database;
8. inserts at most one `WORKER_DRAFT` replacement; and
9. returns the same durable replacement receipt on exact replay.

The rejected source row remains unchanged.

### Request-aware generic creation

Generic `CREATE` now acquires the account/idempotency-key lock before deciding replay. It canonicalises and compares the requested Candidate, workflow kind, scope, route, contract/week or DAILY booking identity, dates, input snapshot and signature. An exact request returns the existing workflow; a different request returns `CANDIDATE_IDEMPOTENCY_CONFLICT`.

Omitted server-derived work/week dates remain valid on exact retry and do not create a false conflict.

### Thin private adapter

`handleWorkflowResubmit` now validates Candidate access, source generation and UUID idempotency key, then invokes the one atomic database action. It no longer performs REST replay lookup, broad shape matching, read-model actionability checking, current-record derivation or random replacement orchestration.

### Action-input correction

`NO_WORK_THIS_WEEK` now advertises only the genuine user-supplied idempotency key. The expected row signature remains server-owned.

## Files in the runtime commit

- `.github/workflows/candidate-db-runtime.yml`
- `broker/src/candidate-app-backend.js`
- `docs/candidate-app/CANDIDATE_API_OPENAPI_V1.yaml`
- `supabase/migrations/11082026_1708_candidate_workflow_replacement_lineage.sql`
- `supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql`
- `supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql`
- `tests/11082026_1715_candidate_resubmission_idempotency_verification.sql`
- `tests/candidate-app-backend.test.js`
- `tests/candidate-app-db-rpc-contract.test.cjs`

## Verification

### Local gates

- complete backend JavaScript suite: `461/461` passed;
- focused Candidate/broker suite: `64/64` passed;
- DB/RPC structural suite: `41/41` passed;
- Candidate runtime/concurrency suites on clean PostgreSQL 17.6: `29/29` passed;
- Candidate runtime/concurrency suites on clean PostgreSQL 18.1: `29/29` passed;
- repeatable reinstall: passed;
- normal backend, private Candidate API and public Candidate broker dry-run builds: passed;
- OpenAPI path/action inventory: passed through the broker boundary suite.

The new database suite proves the original-source-key conflict, exact sequential replay, cross-source key conflict, generic CREATE exact/conflicting replay, same-key concurrency, different-key concurrency for hours, expenses, combined and DAILY workflows, durable lineage, and the no-work input contract.

### GitHub gates

- Candidate DB runtime verification run `31512287233`: success on PostgreSQL 17.6 and PostgreSQL 18.1 for exact runtime `d63bf708cdaf8de5124b0199b2b7eb79e4d13e6b`;
- Supabase Migrate (safe) run `31512287254`: success for the same runtime.

### Installed TEST authority

TEST records:

- schema migration `11082026_1708_candidate_workflow_replacement_lineage.sql` installed;
- replacement column, self-FK and partial unique index installed;
- `RESUBMIT_REJECTED` installed in the existing workflow RPC;
- LF-normalised read/action repeatable hash `59e24b5a71170b77fd4f336d1aa0005cf50dedc8345e22ecaedf1bf1829369dd`;
- LF-normalised workflow repeatable hash `c171c87f2bc4a524a9586df38925e48f261f5e6a8aca547ec386369ab374c3cb`;
- exactly 14 approved public Candidate business RPCs;
- all 12 Candidate feature flags false;
- Candidate electronic auto-authorisation default false;
- all seven Candidate business tables empty;
- Candidate-bound mail empty.

### TEST Worker deployments

The exact published runtime was deployed only to the approved TEST Workers:

- normal TEST backend version `93f7e806-a204-4532-9b84-902b8ace0fd4`;
- private Candidate API version `62a9d4f8-8c9f-4a66-b59d-b96df0c49e2e`;
- public Candidate broker version `de619daf-5f08-407b-bf52-74bfdd80b0f0`.

Harmless post-deployment checks passed:

- normal TEST health/readiness: `200/200`;
- direct Candidate route on the normal backend: `404`;
- Candidate broker health/readiness: `200/200`.

No Candidate business row, Candidate-bound mail row, email, notification, push, R2 business object or financial record was created. Production was not accessed or deployed.

## Independent re-audit request

The independent reviewer should verify the whole Candidate DB/RPC/backend/API contract against the complete current-decisions document, not merely confirm the narrow assertions above. In particular, independently exercise:

- original-key, cross-source-key and request-shape conflicts;
- same-key and different-key concurrency;
- hours, expense, combined and DAILY replacement identity;
- durable replacement chains after later amendment, finalisation or rejection;
- exact action execution using only fixed server facts plus declared user inputs;
- absence of regressions in Current/History, card/detail mapping, PAPER readiness, manager mail, provider permits, route conversion and rejection lifecycle;
- the seven-table, 14-public-RPC, disabled/empty and no-financial-authority boundaries.

The reviewer is expressly invited to report any clearly wrong lifecycle, concurrency, identity, security, document-authority or API-contract issue within the decisions document's scope, including issues not listed in the originating audit. The review must remain within Candidate DB/RPC/backend/API scope and must not modify Banking Pay, financial authority, frontend code or production.
