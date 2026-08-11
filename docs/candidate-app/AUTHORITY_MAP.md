# CloudTMS Candidate App authority and caller map

Status: deployed TEST broker/private-backend authority map, updated 11 August 2026 through the pre-delivery PAPER route guard at Candidate runtime commit `7c087bc79a010944cefc4b605dcab6ea335b6c85`: immutable delivery ownership remains mail-receipt based, while every nonterminal PAPER workflow affected by source rotation is also catalogued independently of mail. A distinct draft/submitted/approval-stage claim blocks the route action before mutation rather than being stranded or silently cancelled.

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
| `candidate_app_timesheet_page_v1` | `GET /candidate-app/v1/timesheets`; resolves immutable anchors through the exact current version, suppresses a historical rejection after a same-family replacement advances, and returns every actionable hours/expense recovery scope |
| `candidate_app_timesheet_detail_v1` | `GET /candidate-app/v1/timesheets/:id`; separates immutable workflow history, current replacement lifecycle and current economic record while returning the same scoped rejection contract |
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

- versioned private Candidate/manager and authenticated office routing, explicitly separated by route audience;
- private Candidate session/password boundary behind the broker;
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
