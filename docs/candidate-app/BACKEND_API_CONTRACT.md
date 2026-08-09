# CloudTMS Candidate App backend contract

Status: TEST implementation contract, 9 August 2026. The CloudTMS Worker is the only server authority exposed to the future private broker. The broker, app and web client must not call Supabase or R2 directly.

## Boundary rules

- Base Candidate API: `/candidate-app/v1`.
- Base manager API: `/candidate-manager/v1`.
- Existing CloudTMS office adapters: `/api/candidate-app`.
- JSON responses use `cache-control: no-store` and stable `error_code` values.
- Candidate access is a short-lived HMAC token bound to one environment, active session and rotation.
- Refresh tokens are opaque, rotated on every use and stored only as hashes.
- Manager access is an opaque token from the URL fragment; only its SHA-256 reaches SQL.
- Upload tickets are short-lived encrypted envelopes. They do not disclose R2 object keys.
- Document bytes are streamed by CloudTMS after ownership/manifest checks. No response returns a raw R2 key.
- Candidate mutation payloads containing rates, pay, charge, VAT, ERNI, margin, invoice breakdown, TSFIN or canonical lifecycle state are rejected.
- All factual mutations carry an idempotency key. Stale workflow generations, route contexts and lifecycle row signatures fail closed.

## Candidate authentication

| Method | Path | Purpose |
|---|---|---|
| POST | `/candidate-app/v1/auth/challenge/start` | Enumeration-safe activation/reset/recovery challenge start. |
| POST | `/candidate-app/v1/auth/challenge/resend` | Supersede/resend an eligible challenge. |
| POST | `/candidate-app/v1/auth/challenge/verify` | Verify a single-use challenge token. |
| POST | `/candidate-app/v1/auth/password/complete` | Establish/reset password and create the first rotated session transactionally. |
| POST | `/candidate-app/v1/auth/login` | Verify password and create a session. |
| POST | `/candidate-app/v1/auth/refresh` | Verify current refresh hash, rotate family and detect replay. |
| POST | `/candidate-app/v1/auth/logout` | Revoke the current session. |
| POST | `/candidate-app/v1/account/password` | Require the current password, change it and revoke other active sessions. |

Challenge/outbox insertion is retry-safe: the backend deterministically reproduces the same challenge token for an idempotent replay and upserts the same mail-outbox identity.

## Candidate account and read API

| Method | Path | Purpose |
|---|---|---|
| POST | `/candidate-app/v1/account/select-candidate` | TEST-only selection where one email maps to several candidates. |
| PATCH | `/candidate-app/v1/account/preferences` | Persist server-owned notification preferences. |
| POST | `/candidate-app/v1/account/push-token` | Encrypt and register a provider token on the active session. |
| GET | `/candidate-app/v1/bootstrap` | Entitlements, selection and global capabilities. |
| GET | `/candidate-app/v1/timesheets` | Candidate-safe paged projection. |
| GET | `/candidate-app/v1/timesheets/:timesheetId` | Candidate-safe detail and capabilities. |
| GET | `/candidate-app/v1/timesheets/:timesheetId/paper-pack/status` | Return `PREPARING`, `READY` or `FAILED` and whether secure download is available. |
| GET | `/candidate-app/v1/timesheets/:timesheetId/paper-pack` | Stream the current WEEKLY QR/paper pack only after durable document readiness. |
| GET | `/candidate-app/v1/contracts/:contractId/missing-weeks` | Per-week effective missing-week options. |
| POST | `/candidate-app/v1/contracts/:contractId/missing-weeks` | Add one eligible missing week. |
| GET | `/candidate-app/v1/notifications` | Read the account notification feed. |
| POST | `/candidate-app/v1/notifications/:notificationId/read` | Idempotently mark only the owning account's notification read. |

The safe timesheet projection owns client name, job title, band, date/status, route family, action flags, expense overlay and exact workflow state. The broker must not fill gaps with direct database reads.

## Workflow, expense and document API

| Method | Path | Purpose |
|---|---|---|
| POST | `/candidate-app/v1/workflows` | Create a server-validated WEEKLY or DAILY workflow. |
| POST | `/candidate-app/v1/workflows/:workflowId/actions/:action` | Amend, submit, select approval, prepare/return PAPER, remind, renew, cancel, supersede or finalise. |
| POST | `/candidate-app/v1/workflows/:workflowId/components/prepare` | Prepare a category-bound source/signature/return component. |
| PUT | `/candidate-app/v1/uploads/:encryptedTicket` | Upload exact bytes; CloudTMS computes SHA-256 and completes the component. |
| GET | `/candidate-app/v1/workflows/:workflowId/components/:componentId/document` | Stream an authorised workflow document. |
| POST | `/candidate-app/v1/timesheets/:timesheetId/expense-placement` | Resolve eligibility/placement without creating financial truth. |
| POST | `/candidate-app/v1/timesheets/:timesheetId/expense-carrier` | Idempotently resolve/create the server-authorised carrier. |
| POST | `/candidate-app/v1/contract-weeks/:contractWeekId/no-work` | Apply the route/capability-guarded no-work decision. |

Explicit `no_break: true` or an explicit numeric `break_minutes: 0` is valid. Blank or null break values are not silently converted to no-break. DAILY zero break produces the existing checking issue; WEEKLY zero break does not create that issue.

Category selection may be omitted by the app only when its UI context is unambiguous. The component PREPARE/COMPLETE authority still records one exact immutable server category. One evidence object cannot satisfy several categories.

### QR/paper readiness and delivery

Candidate PAPER applies to WEEKLY only. `PAPER_PREPARE` freezes the complete return manifest and automatically calls the existing `timesheet_qr_send_enqueue_v1` authority. That authority creates a durable document operation and an idempotent candidate-email outbox row. The email is held with no attachment until the official unsigned PDF reaches `READY`; the existing document/mail authority then attaches and releases it. The candidate does not make a second email request.

The app must treat these states separately:

```text
QUEUED / RENDERING  -> show "Preparing documents"; download is unavailable
READY               -> notification feed/push announces readiness; download is available
SENT                 -> the automatic candidate email has been sent
FAILED               -> show a controlled retry/support state; never claim readiness
```

QR token creation is not document readiness. Only the current canonical `TIMESHEET` document version in `READY` state unlocks the secure paper-pack stream. The Worker verifies Candidate ownership and streams bytes from R2; the broker/app never receives an R2 key. The notification feed and paper-pack status endpoint are authoritative even where a device push is unavailable or disabled.

## Manager API

| Method | Path | Purpose |
|---|---|---|
| GET | `/candidate-manager/v1/workflows/:workflowId/start` | Resolve the immutable required manifest. |
| POST | `/candidate-manager/v1/workflows/:workflowId/progress` | Record review of one exact page/component. |
| POST | `/candidate-manager/v1/workflows/:workflowId/signature/prepare` | Prepare and upload one manager signature component. |
| POST | `/candidate-manager/v1/workflows/:workflowId/approve` | First-complete approval, then guarded final render/finalisation. |
| POST | `/candidate-manager/v1/workflows/:workflowId/refuse` | Refuse the whole active submission. |
| GET | `/candidate-manager/v1/workflows/:workflowId/components/:componentId/document` | Stream only a component in the token-bound manifest. |

Approval commits before guarded asynchronous final rendering/finalisation. Independent workflows do not share a global lock. A failed follow-on leaves durable manager approval and a retryable workflow; it does not create partial canonical finalisation.

The initial manager email is also readiness-gated. Candidate submission first renders and registers every required review component: the candidate-signed hours page where applicable, the expense summary, mileage form and each required evidence image/PDF. The workflow becomes `READY_FOR_MANAGER_APPROVAL` only when the immutable review manifest reports every component ready. `CREATE_EMAIL_APPROVAL_REQUEST` recomputes and verifies that same manifest before it can queue the email. Consequently, the manager link opens already-created pages; it never asks the manager to wait for the review pack to render. Rendering failure leaves the workflow pending/retryable and prevents the email request.

## CloudTMS office adapters

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/candidate-app/timesheets/:timesheetId/route-preview?action=...` | Server warning/action/context preflight. |
| POST | `/api/candidate-app/timesheets/:timesheetId/route-confirm` | Row-signature/context-hash confirmed route transition with reason where required. |
| POST | `/api/candidate-app/timesheets/:timesheetId/reject` | Whole-record Candidate rejection; authorised records require Unauthorise first. |
| POST | `/api/candidate-app/workflows/:workflowId/actions/phone-review` | Start the same manifest authority for phone approval. |
| POST | `/api/candidate-app/workflows/:workflowId/actions/phone-progress` | Record exact page review. |
| POST | `/api/candidate-app/workflows/:workflowId/actions/phone-approve` | Record manager approval and start guarded final rendering/finalisation. |
| POST | `/api/candidate-app/workflows/:workflowId/actions/phone-refuse` | Refuse the whole active workflow. |
| POST | `/api/candidate-app/workflows/:workflowId/actions/retry-finalisation` | Idempotently recover final rendering/finalisation without a second signature. |

All office route actions will use the approved W01–W13 warning catalogue from the living implementation plan. The frontend must preview before confirmation and must not independently derive warning state.

## Finalisation contract

- ELECTRONIC review/final PDFs use one official CloudTMS renderer. Display facts and wording are frozen into the immutable submission before manager review, so final rendering cannot drift.
- WEEKLY finalisation calls the existing WEEKLY calculation/upsert authority.
- DAILY finalisation calls the one canonical DAILY Save/Recalculate owner, then the existing Process authority.
- Candidate auto-authorisation is only a decision to call the existing Authorise authority.
- PAPER finalisation is WEEKLY-only and never auto-authorises.
- PAPER return immediately attempts canonical finalisation after the complete manifest is accepted. A canonical blocker leaves the workflow `RECEIVED` with a controlled retry state.
- Finalisation responses expose lifecycle outcome and identifiers only; internal renderer contracts and canonical financial snapshots are removed.

## Configuration and delivery gates

The Worker requires the existing TEST Supabase/R2 bindings plus token-sealing secrets. A Candidate/public frontend base URL must be configured before transactional links are enabled. Push-provider delivery remains coupled to the provider/token format selected during the broker/app implementation; database notification creation, preferences, encryption, dedupe and readiness timing remain CloudTMS-owned.

No endpoint in this contract executes Banking Pay, settlement, remittance or a production action.
