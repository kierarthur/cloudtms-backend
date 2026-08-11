# CloudTMS Candidate manager-mail and Current/History closure handover

Date: 11 August 2026  
Environment: TEST only  
Runtime commit: `adc399455383fc60e76159042eee78dff00896a2`  
Status: published, installed and deployed while every Candidate feature flag remains false; independent API-freeze approval is still required before office-frontend implementation begins.

## Closure delivered

### Manager EMAIL authority

- Every invitation, reminder and renewal outbox row is bound to one exact `MANAGER_APPROVAL_V1` request, request generation, workflow, workflow generation, recipient and mail kind.
- Approval-request cancellation, supersession, refusal, expiry or approval centrally retires its exact queued/failed mail. Accepted sent history remains immutable.
- A live exact outbox lease blocks lifecycle mutation. The provider adapter must obtain `MANAGER_PROVIDER_SUBMIT_PERMIT` immediately before sending and prove the same request/workflow generations and recipient.
- The claimant rejects retired, stale-generation, wrong-recipient or non-current request/workflow rows.
- Withdrawal is created only where provider acceptance of earlier mail for that exact request is proved. A local timestamp is insufficient.
- The rule is centralized at the request state boundary so every caller receives the same protection.

### Timesheet Current/History read contract

- `Current` is the default view.
- Future week-ending dates are excluded.
- Current contains every unpaid timesheet without an age limit and paid timesheets whose authoritative paid timestamp is at or after the exact frozen-snapshot minus seven days.
- `History` contains only paid timesheets older than that exact cutoff and only within each contract's effective current week plus the previous 15 contract-specific weeks.
- The two views cannot overlap at the same frozen snapshot.
- Results are ordered by week ending date newest to oldest with deterministic tie-breakers.
- The API returns the exact server label, for example `Week Ending 1 January 2025`, plus `tab_bucket`, status fields, one `primary_action`, available actions and a stable `detail_target`.
- The v2 cursor is bound to candidate, view and snapshot. A cursor cannot be replayed into the other tab or another candidate session and expires after 24 hours.
- Card tap opens the server-identified detail/review screen. That detail screen is the action hub; the frontend must not invent a generic action-menu overlay or infer capabilities from status text.
- Detail may be resolved through canonical timesheet identity or the explicit contract-week/workflow aliases in the OpenAPI contract.

No frontend code was changed. The next UI phase is the CloudTMS office frontend only. Candidate responsive web, iOS, Android and the public app/broker clients remain deferred until the office frontend is complete and accepted.

## Source changes

- `supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql`
- `supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql`
- `supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql`
- `supabase/repeatable/23072026_2207_email_outbox_claim_ready_batch.sql`
- `broker/src/candidate-manager-provider-authority.js`
- `broker/src/candidate-app-backend.js`
- `broker/src/index.js`
- `docs/candidate-app/openapi.yaml`
- focused JavaScript and PostgreSQL regression suites plus the Candidate DB workflow.

No Banking Pay, payment, settlement, invoice, DAILY/WEEKLY economics, Process, Authorise, Policy X or production source was changed by this runtime commit.

## Installed TEST parity

| Repeatable | LF-normalised SHA-256 |
|---|---|
| `07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql` | `3d3795b4c305fec344d8e8c15226333893ff32723182a46c196102f218f31a5b` |
| `07082026_2120_candidate_workflow_transition_atomic_v1.sql` | `c2bc4c9c1e9e27148bb7cb5b708bdc271c6edcabca918a6e23448eb6d2c611f3` |
| `08082026_2035_timesheet_route_version_rotate.sql` | `b9656644a099a5b7d7e1163579a2dafe637ef0a8bf64608b4a805f64e78d3d90` |
| `23072026_2207_email_outbox_claim_ready_batch.sql` | `e3d817bed564f538b5089c27b9ce4d58b7e6f6b970066f4e8be2724834c0a112` |

TEST inspection after deployment proved:

- PostgreSQL 17.6, project environment `TEST`;
- all 12 Candidate flags false;
- electronic auto-authorisation default false;
- all seven Candidate business tables empty;
- Candidate-bound mail empty;
- exactly 14 approved public Candidate business RPCs, one overload each, executable by `service_role` only;
- no Candidate email, push, workflow, R2 business document or financial operation created by verification.

## Verification and deployment

| Gate | Result |
|---|---|
| Focused Candidate/backend tests | 122 passed, 0 failed |
| Complete backend suite | 459 passed, 0 failed |
| Disposable PostgreSQL 17.6 | 26 suites passed |
| Disposable PostgreSQL 18.1 | 26 suites passed |
| GitHub Candidate DB runtime workflow | `31487593920`, passed |
| GitHub safe TEST migration | `31487593931`, passed |
| OpenAPI validation | passed |
| Normal/private/broker Worker dry-run builds | passed |
| Normal TEST backend | version `c25ed724-3d11-4234-b4b4-1b08f305a3d4`, health 200 |
| Private Candidate API | version `e6b7d82e-1a89-4303-bfef-a6aca9c8b151`, service-binding only |
| Public Candidate broker | version `0e1f7188-f341-4620-b8df-4c5c8703276a`, health/readiness 200/200 |
| Direct Candidate route on normal backend | 404 as required |

Immediately before deployment, `origin/test` equalled the exact tested runtime and no GitHub workflow was queued or in progress. The runtime preserves Banking Pay-only parent commit `92f8b39f7870cdbaa993d40e036e97c35f2fe983`; no file owned by that separate lane was changed.

## Independent re-audit request

Do not restrict the audit to the two latest findings. Review the complete current-decisions PDF and every adjacent installed DB/RPC/backend/API seam needed to satisfy it. Report any clearly incorrect, unsafe, contradictory or incomplete server-owned behaviour within that scope, while respecting the financial/no-change boundary.

At minimum, independently prove:

1. every manager invitation/reminder/renewal is request- and generation-bound from enqueue through claim, permit and provider completion;
2. every request-closing caller retires the same exact mail set and cannot mutate beneath a live provider permit;
3. accepted sent history is preserved, queued/failed obsolete mail is inert, and withdrawal requires actual provider acceptance;
4. Current and History implement the exact, non-overlapping seven-day and 16-week rules at one frozen snapshot, including the exact boundary instant;
5. future weeks never appear; unpaid rows have no age limit; History uses each contract's own effective current week;
6. cursor/view/candidate/snapshot binding, newest-first ordering, exact English date label, primary action and stable detail identity are consistent across SQL, backend and OpenAPI;
7. list and detail retain the previously accepted current-version anchor, replacement workflow and plural rejection truth;
8. all earlier Candidate upload, PAPER, manager method, deterministic PDF, notification, outbox, rejection, route-conversion and provider-permit controls remain intact;
9. no accepted lifecycle caller, including an adjacent one not named by this handover, can bypass those central authorities;
10. all 26 current-decisions pages are implemented or explicitly deferred exactly as recorded, with no frontend inference required for server truth.

The receiving chat should issue GO only if the complete audited server contract is safe to freeze. If GO is issued, its next deliverable should be a highly detailed CloudTMS **office-frontend** implementation plan that follows the full decisions PDF without exception and explicitly includes:

- Current/History tabs and the server-owned membership rules;
- exact week labels and newest-first order;
- row/card tap to the detail/review action hub;
- incomplete expense-claim warnings and explicit confirmation before office conversion to MANUAL;
- all W01–W13 warning/modal behavior;
- professional, uncluttered responsive modal layouts exercised through deterministic UI states with realistic data;
- accessibility, loading, empty, error, stale-context and retry states;
- no Candidate web/iOS/Android or public app/broker implementation until the office frontend is complete and separately accepted.

## Safety boundary

Keep every Candidate feature flag false and Candidate business tables empty during re-audit. Do not enable Candidate traffic or begin frontend implementation before independent GO. Do not change DAILY/WEEKLY calculations, rate resolution, pay, charge, VAT, ERNI, margin, TSFIN economics, Process, Authorise, invoice, payment, Banking Pay, Policy X, settlement, remittance or production.
