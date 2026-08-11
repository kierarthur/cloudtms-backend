# CloudTMS Candidate manager-mail and Current/History closure handover

Date: 11 August 2026
Environment: TEST only
Runtime commit: `a14b60b734560fc5ddf0109bbef6e21eb46e4857`
Status: published, installed and deployed while every Candidate feature flag remains false; independent API-freeze approval is still required before office-frontend implementation begins.

## Closure delivered

### Manager EMAIL authority

- Every invitation, reminder and renewal outbox row is bound to one exact `MANAGER_APPROVAL_V1` request, request generation, workflow, workflow generation, recipient and mail kind.
- Approval-request cancellation, supersession, refusal, expiry or approval centrally retires its exact queued/failed mail. Accepted sent history remains immutable.
- A live exact outbox lease blocks lifecycle mutation. The provider adapter must obtain `MANAGER_PROVIDER_SUBMIT_PERMIT` immediately before sending and prove the same request/workflow generations and recipient.
- The claimant rejects retired, stale-generation, wrong-recipient or non-current request/workflow rows.
- Withdrawal is created only where provider acceptance of earlier mail for that exact request is proved. A local timestamp is insufficient.
- The rule is centralized at the request state boundary so every caller receives the same protection.
- Initial/renewal queue creation does not claim the mail was sent. The exact bound provider-accepted outbox receipt owns first/latest sent time and the 24-hour reminder clock.
- `REMIND` remains on the same pending request ID/generation, rotates that request's token, permits no more than five resends, requires 24 hours since the latest accepted send and cannot queue while an exact delivery remains pending.
- `RENEW` is never substituted for reminder. It is accepted only for an expired unchanged request and creates a new request generation, fresh token, seven-day expiry and resend allowance.
- `CANCEL` requires a non-empty plain-English reason of no more than 1,000 characters. It records that reason, retires the exact request delivery authority and queues one deterministic withdrawal only where provider acceptance is proved.

### Timesheet-detail action hub

- All three detail aliases return the same typed detail contract.
- The response contains one `primary_action`, a closed `available_actions` list and EMAIL `manager_approval` facts.
- Each action supplies its exact code, label, HTTP method/path, workflow ID/generation, approval-request identity where applicable, confirmation requirement, enabled flag and stable disabled reason.
- The server distinguishes continue-timesheet from continue-expense, whole-claim from expense-only cancellation, reminder from expired-request renewal, PAPER download/return, rejection recovery, no-work and retry-finalisation.
- `RETRY_FINALISATION` is allowed only for an owned retryable `RECEIVED` workflow and composes the existing service finalisation owner. General public Candidate `FINALISE` remains absent.
- The frontend must render these actions exactly and must not infer or translate lifecycle actions from status text.

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
- `broker/src/candidate-app-backend.js`
- `docs/candidate-app/CANDIDATE_API_OPENAPI_V1.yaml`
- focused JavaScript and PostgreSQL regression suites plus the Candidate DB workflow.

No Banking Pay, payment, settlement, invoice, DAILY/WEEKLY economics, Process, Authorise, Policy X or production source was changed by this runtime commit.

## Installed TEST parity

| Repeatable | LF-normalised SHA-256 |
|---|---|
| `07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql` | `30cfbe23762d92af40a40c4e0cb4e6085ecb4cf631a2f48c0f9f8b6ea0c1babd` |
| `07082026_2120_candidate_workflow_transition_atomic_v1.sql` | `8eb1bf2762462164569b03d1964d99700055e7ebd7895250d5dc0964a686c2d3` |

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
| Focused Candidate/backend tests | 124 passed, 0 failed |
| Complete backend suite | 460 passed, 0 failed |
| Disposable PostgreSQL 17.6 | 27 suites passed |
| Disposable PostgreSQL 18.1 | 27 suites passed |
| GitHub Candidate DB runtime workflow | `31495500205`, passed |
| GitHub safe TEST migration | `31495500073`, passed |
| OpenAPI validation | passed |
| Normal/private/broker Worker dry-run builds | passed |
| Normal TEST backend | version `14554299-dcb6-4102-8ef9-7316d34b3654`, health/readiness 200/200 |
| Private Candidate API | version `407596b9-5dd4-48fa-939a-4a9b2885ef74`, service-binding only |
| Public Candidate broker | version `7dfc34ec-b78f-48ca-8b0d-ebd847cfcebc`, health/readiness 200/200 |
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

The receiving chat should issue GO only if the complete audited server contract is safe to freeze. It must not limit itself to the latest findings or assume a seam is correct because an earlier audit accepted it. It should trace the complete current-decisions PDF through every relevant installed DB/RPC/backend/OpenAPI caller and report any material omission, contradiction, unsafe lifecycle path or client inference that remains within scope.

If backend GO is issued, the receiving chat must also produce `OFFICE_FRONTEND_IMPLEMENTATION_BLUEPRINT.md`. This must be implementation-ready rather than a high-level feature list. It must give the implementing chat enough exact evidence to edit the current office frontend directly without rediscovering the architecture or inventing product behaviour. At minimum it must contain:

1. exact frontend repository baseline, current source inventory and existing route/screen/modal/component helpers to reuse;
2. a file-by-file patch map naming every existing function to amend, every new function/component proposed, its inputs/outputs, callers, state ownership and test owner;
3. a screen-by-screen, component-by-component and modal-by-modal map for Simple Timesheet, Timesheet Summary, Bulk Process and Bulk Authorise;
4. an API-to-UI field map for every displayed or submitted field, including source endpoint/RPC, type, null handling, formatting, permissions and authoritative owner;
5. a complete backend-state × server-action × UI-state matrix, including disabled actions and stable error/recovery handling;
6. the full manager reminder/renew/cancel experience: provider-accepted sent time, expiry, resends remaining, next reminder, distinct action wording, required cancellation reason, confirmation, withdrawal result and race/error states;
7. the approved incomplete-expense-claim-to-MANUAL warning: the office is told that an unfinished ELECTRONIC or PAPER/QR expense claim exists, **No** performs zero mutation, and **Yes** discards/supersedes that incomplete claim before the normal MANUAL conversion proceeds;
8. one shared W01–W14 warning renderer using the controlling catalogue without rewriting its wording;
9. exact modal structure, information hierarchy, primary/secondary button placement, spacing, overflow, narrow-screen behaviour and rules for keeping the presentation professional and uncluttered;
10. accessibility requirements including focus entry/return, keyboard order, escape/cancel behaviour, ARIA naming/descriptions, error association, contrast and reduced-motion handling;
11. loading, empty, stale-context, conflict, retry, partial-follow-on-failure and permission-denied states for every affected screen/action;
12. deterministic UI state fixtures or a state-gallery harness that can render hard-to-reach modals without creating database conditions, plus realistic long/short data stress cases; the plan must explain why this is isolated from production logic and how browser tests prove the real component is rendered;
13. desktop and narrow viewport browser-verification matrices, exact screenshot/state coverage, patched-asset proof and visual acceptance criteria;
14. unit, contract, integration and Playwright tests with exact fixtures/assertions, including modal confirmation/no-mutation paths and server-action payloads;
15. a correctly ordered implementation sequence with safe checkpoints, feature flags, rollback boundary and completion criteria;
16. a separate list of genuine unresolved decisions or contract gaps. The plan must not silently fill them with frontend inference.

The blueprint must explicitly separate phases. The next implementation is the current CloudTMS **office frontend only**. Candidate responsive web, iOS, Android and public app-client work must not be implemented yet. The later client phase must nevertheless remain mapped from the decisions PDF, including Current/History, exact week labels/order and card-to-detail action-hub navigation, so office work does not close off the approved client design.

## Safety boundary

Keep every Candidate feature flag false and Candidate business tables empty during re-audit. Do not enable Candidate traffic or begin frontend implementation before independent GO. Do not change DAILY/WEEKLY calculations, rate resolution, pay, charge, VAT, ERNI, margin, TSFIN economics, Process, Authorise, invoice, payment, Banking Pay, Policy X, settlement, remittance or production.
