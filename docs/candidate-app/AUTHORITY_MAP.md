# CloudTMS Candidate App authority and caller map

Status: TEST broker/private-backend authority map, updated 12 August 2026 through semantic pre-enrichment replay, trigger-key-independent finalisation completion, exact PAPER claim/backoff recovery and the typed timesheet-detail action contract. Candidate features remain disabled pending independent approval.

## Canonical owner graph

```text
Candidate app/web or manager browser
        ↓
public Candidate broker
  origin/rate-limit/public-token boundary
        ↓ signed service binding
private CloudTMS Candidate API
        ↓
installed Candidate DB/RPC validation + immutable workflow
        ↓
WEEKLY → existing WEEKLY calculation/upsert authority
DAILY  → one canonical DAILY Save/Recalculate owner
        ↓
existing Process authority
        ↓
existing Authorise authority when office/policy permits
        ↓
existing invoice and payment-eligibility pipelines
```

Neither Worker alters the existing financial algorithms. The broker cannot access Supabase or R2. The private API adapts factual input to the existing WEEKLY/DAILY owners. Banking Pay is not called or changed.

## Fourteen Candidate business RPCs and HTTP callers

| Installed RPC | CloudTMS backend caller |
|---|---|
| `candidate_auth_account_transition_v1` | login, activation/password completion, refresh, logout, selection, preferences, push registration and password change |
| `candidate_auth_challenge_transition_v1` | activation/reset/recovery challenge start, resend and verify |
| `candidate_app_bootstrap_v1` | `GET /candidate-app/v1/bootstrap` |
| `candidate_app_timesheet_page_v1` | `GET /candidate-app/v1/timesheets?view=current|history`; owns the frozen-snapshot, non-overlapping Current/History partition, v2 cursor, newest-first order, exact week-ending label, one primary action and stable detail target while preserving exact anchor/rejection projection |
| `private._candidate_timesheet_action_contract_v1` | Private detail action/status authority; returns the closed typed invocation catalogue, exact PAPER readiness and provider-owned manager-approval timing/eligibility without adding a public RPC |
| `candidate_app_timesheet_detail_v1` | `GET /candidate-app/v1/timesheets/:id`, `/candidate-app/v1/contract-weeks/:id/detail` or `/candidate-app/v1/workflows/:id/timesheet-detail`; resolves one exact card/version-family identity and returns current/history membership, plural scoped rejection truth, typed actions and PAPER readiness |
| `candidate_missing_week_options_v1` | `GET /candidate-app/v1/contracts/:id/missing-weeks` |
| `candidate_contract_week_add_missing_atomic_v1` | `POST /candidate-app/v1/contracts/:id/missing-weeks` |
| `expense_placement_resolve_v1` | `POST /candidate-app/v1/timesheets/:id/expense-placement` |
| `expense_carrier_resolve_or_create_atomic_v1` | `POST /candidate-app/v1/timesheets/:id/expense-carrier`; finalisation composition |
| `timesheet_expense_apply_atomic_v1` | called only inside the installed finalisation authority |
| `candidate_workflow_transition_atomic_v1` | workflow/components/manager/phone/notification orchestration |
| `candidate_submission_finalize_atomic_v1` | final signed ELECTRONIC and complete PAPER finalisation |
| `candidate_submission_reject_atomic_v1` | office whole-record Candidate rejection; one candidate/contract/week family lock is obtained before row locks, then pre-finalisation target-or-anchor retirement, finalised exact-target rejection, separate-expense anchor isolation and `AWAITING_PAPER_RETURN`/`RECEIVED`/`FINALISED` delivery retirement with mail-receipt-owned QR invalidation occur before economic-target rotation |
| `candidate_no_work_atomic_v1` | Candidate no-work decision |

## Existing CloudTMS owners retained

| Authority | Entry points after backend implementation | Single owner |
|---|---|---|
| DAILY economics | office create, office save/edit, additional DAILY, TSFIN worker, Candidate finalisation | `buildCanonicalDailyFinancialSnapshot` and its canonical context/schedule helpers |
| WEEKLY economics | office WEEKLY, Candidate WEEKLY | existing WEEKLY schedule/TSFIN calculation and `contract_week_manual_upsert_atomic` |
| DAILY Process | office and Candidate | `timesheet_daily_manual_process_atomic` |
| Authorise | office and policy-safe Candidate auto-authorise | `timesheet_authorise_generic_atomic` / established row authority |
| Route/version | office and Candidate route orchestration | confirmed SQL route/version authority; feature-off legacy wrapper retained |
| QR pack | office and Candidate PAPER | `timesheet_qr_send_enqueue_v1` plus existing document-operation worker |
| Invoice | all record types | existing Generator/Issuer grouping, validation, render, issue and email pipeline |
| Official timesheet | QR unsigned, manager review, final signed | existing official renderer with controlled variants |
| Manager approval | email and phone | one workflow/request/manifest/signature/finalisation authority |

## Source responsibilities

### `candidate-broker/src/candidate-broker.js`

- public Candidate and manager route boundary;
- exact browser origins and declared iOS/Android native clients;
- independent general/auth/manager/upload rate limits;
- public access/refresh envelope issuance and unwrapping;
- bounded body forwarding and public-safe error mapping;
- device-provider validation and broker-key encryption of raw push tokens;
- signed Cloudflare service-binding calls only—no Supabase, R2, mail or financial access.

### `broker/src/candidate-private-worker.js`

- accepts only `/private/candidate-app/v1` and `/private/candidate-manager/v1`;
- verifies environment/body/authorisation-bound service HMAC;
- has no public workers.dev route;
- removes public browser headers and injects the `PRIVATE` Candidate route audience;
- delegates all business work to the existing Candidate backend module and canonical owners.

### `broker/src/candidate-app-backend.js`

QR/paper delivery remains one composed authority: `PAPER_PREPARE` queues the existing QR document operation and binds its exact held mail operation to the workflow generation and manifest; the private scheduler alone assembles the complete pack and releases that exact email and the idempotent Candidate notification. Candidate paper-pack status/download endpoints are read-only and only inspect/stream an already-ready immutable pack without exposing an R2 key. They cannot requeue failed mail.

Candidate rejection/read authority remains server-owned: historical workflow and carrier anchor IDs are immutable audit facts, while the current card identity is resolved through one exact current booking/version family. A newer workflow suppresses an older rejection only when it is a true replacement of that exact logical claim: the same contract-week record for hours/combined, the same week-level expense family for expenses, or the same DAILY booking/work date. Independently actionable hours and expense rejections are returned together, so the client never guesses which recovery action survives.

PAPER retirement keeps QR source, delivery owner, affected workflow set and economic target separate. The source and current-token owner come from exact immutable workflow-generation/mail receipts; the affected economic target comes from the locked workflow/route target and may be a different expense carrier. A second mail-independent source-family catalogue finds every nonterminal PAPER workflow that would lose its current anchor, including draft, submitted, review/approval, waiting-return and received states. The database locks the complete set and fails closed before delivery or route mutation when the selected delivery owner does not own the sole affected nonterminal lifecycle. A final locked postcondition covers `CONVERT_QR_TO_MANUAL`, `DISABLE_QR`, `INVALIDATE_QR` and `REISSUE_QR`, so no nonterminal workflow can remain tied exclusively to a source made historical. Cancellation, supersession, rejection and confirmed route conversion continue to compose the immutable delivery owner under the same family lock and mail permit barrier. No Candidate, frontend or broker code may infer, interchange or bypass these identities.

Manager EMAIL delivery has the same exact-authority discipline. `MANAGER_APPROVAL_V1` scope binds mail kind, workflow ID/generation and approval-request ID/generation. Approval-request state transitions centrally retire queued or failed mail, fence an active provider lease and preserve accepted sent history. The normal backend obtains `MANAGER_PROVIDER_SUBMIT_PERMIT` for that exact outbox lease immediately before external submission. Withdrawal is allowed only when provider acceptance of earlier mail for that exact request is proved.

Manager action meaning is closed. `REMIND` remains on the same pending request and rotates its token after the exact 24-hour provider-acceptance boundary, subject to five resends and no pending exact delivery. `RENEW` is permitted only for an expired unchanged request and creates a new request generation. `CANCEL` requires a recorded plain-English reason and queues one deterministic withdrawal only for provider-accepted request history. Enqueue timestamps never become send truth.

The timesheet detail authority, not the client, decides `primary_action`, `available_actions`, `manager_approval` and `paper_pack`. The action object carries typed invocation version 1: exact method/path or client editor destination, immutable fixed body, required user inputs, idempotency requirement, workflow/request identity, confirmation, enabled state and disabled reason. `ENTER_TIMESHEET` and `ADD_EXPENSES` carry complete server-owned create context. `CONTINUE_*` opens the declared editor. `REFUSED` invokes supported `AMEND`; `REJECTED` remains immutable and `POST /candidate-app/v1/workflows/:id/resubmit` creates a new server-derived workflow through the existing `CREATE` owner. Public retry-finalisation is restricted to a retryable `RECEIVED` workflow and composes the existing service finalisation owner; general public `FINALISE` remains absent.

Exact mutation replay is governed by the durable semantic receipt described in `CANDIDATE_EXECUTION_REPLAY_AND_PAPER_FAILURE_AUTHORITY.md`. Caller-owned keys are mandatory. `WORKER_SUBMIT` and manager semantic replay are probed before mutable enrichment and exclude generated presentation/mail/token facts. Finalisation completion is keyed by immutable workflow-generation and approval/PAPER identity rather than the incidental trigger key. PAPER scheduler/Office retries share the exact outbox operation/attempt lease, failure catalogue, source-document readiness and advancing backoff authority. Replayed `CLAIMED` receipts never create a second executor; Office cannot bypass backoff, can recover only after an expired lease, and retains the complete final result rather than a transient in-progress response.

Detail workflow/component/document membership is bounded by the exact card/version family, including historical-to-current expense-anchor resolution. Another `additional_seq` record or unrelated same-date claim cannot leak into the opened action hub. PAPER readiness is derived from the exact workflow generation, manifest, outbox and attachment receipt; download and signed-return upload remain disabled until `READY`, and the private backend separately proves the immutable R2 receipt.

The Timesheets list is server-partitioned. Current is the default: no future weeks, all unpaid rows regardless of age, and paid rows whose `paid_at_utc` is exactly seven days old or newer. History contains only older-paid rows inside the contract-specific 16-week window. A frozen snapshot and view/candidate-bound v2 cursor make the sets disjoint across pagination. Clients display the returned `week_ending_label`, follow `detail_target`, and render at most the returned `primary_action`; they do not recalculate membership or status.

- versioned private Candidate/manager and authenticated office routing, explicitly separated by route audience;
- private Candidate session/password boundary behind the broker;
- database-result-owned refresh-token reconstruction for activation, login and refresh, including concurrent same-key winner/loser responses;
- stable opaque public session identity plus deterministic authenticated v4 broker access/refresh/PHONE envelopes whose key version is bound into HMAC, key derivation and AES-GCM authenticated data, with frozen issue/expiry facts, explicit reader catalogues and retained v1/v2/v3 read compatibility;
- randomized versioned push-token storage encryption separated from the versioned provider/session/token HMAC used by semantic idempotency; overlapping identity proofs preserve exact replay during an approved identity-key rotation;
- a closed unauthenticated authentication-route catalogue: logout always unwraps the public access credential and forwards the exact private bearer;
- challenge start/resend preserve caller-key validation and idempotency conflicts while masking only Candidate eligibility/account state; resend timing/allowance throttles are durable public 429 results, and each receipt freezes the exact challenge-token issuing key version for retained-reader replay;
- generic unknown-account login failures are durable, mutate no account counter and conflict if their key is reused with changed factual input;
- PHONE mutation receipts bind the initiating public session and optional supplied device before any token is generated;
- post-precondition exact-receipt recovery for concurrent logout and password change;
- dedicated session, challenge and upload secrets with no general-secret fallback;
- encrypted upload envelopes plus actual PNG/JPEG/PDF validation, one-page evidence PDF enforcement and R2 byte verification;
- DB-owned `COMPONENT_PREPARE` replay identity: the upload ticket is always built from the authoritative stored key/type/size/role/category returned by SQL;
- safe RPC adapters and response filtering;
- official manager-review/final page rendering and registration;
- configured-brand professional Expense and Mileage Approval Summary and A4 repeatable-journey Mileage Claim Form rendering;
- manager email/phone orchestration and isolated follow-on recovery;
- no-work, notifications, route preview/confirm and rejection adapters.

General public Candidate workflow actions do not expose service finalisation. Manager approval, complete paper return and authenticated office retry are the only HTTP owners that can invoke the service-finalisation context.

It must not own rates, pay, charge, VAT, ERNI, margin, invoice breakdown, TSFIN, Process, Authorise, invoice grouping or QR versions.

### `broker/src/index.js`

- injects the established CloudTMS RPC and canonical financial/document dependencies;
- keeps existing office endpoints and business authorities intact;
- exposes authenticated Candidate office adapters only; direct public Candidate/manager paths are not dispatched by the normal CloudTMS Worker;
- exports dependency composition for the private Candidate Worker without duplicating financial or lifecycle logic;
- routes Candidate DAILY finalisation through the already shared canonical DAILY materialisation seam;
- routes Candidate WEEKLY through the existing WEEKLY calculation/upsert authority;
- routes PAPER pack creation through the existing QR enqueue/document worker.

### Official renderer modules

- `invoice-presentation-contract.js` accepts `ELECTRONIC_MANAGER_REVIEW` only with candidate signature and without manager signature/authorisation;
- `timesheet-official-pdf.js` displays the candidate signature in the review variant and both signatures only in the final variant;
- immutable official presentation facts are frozen at worker submission and reused for review and final derivatives.

## Consumer boundaries

- CloudTMS frontend consumes server warning/status/capability fields and performs preview → warning → confirm.
- Candidate broker consumes only the versioned private CloudTMS service API.
- Candidate app/web sends factual inputs and renders server truth.
- Google Availability/rota remains unchanged and is mediated rather than moved.
