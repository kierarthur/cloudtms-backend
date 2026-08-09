# CloudTMS Candidate App backend contract

Status: versioned TEST implementation contract, updated 10 August 2026. The public Candidate broker and private CloudTMS API are separate Worker artefacts. The app, Candidate web client and manager browser call only the broker. Only the private CloudTMS API can compose Supabase, R2, mail and canonical CloudTMS authorities.

Machine-readable contract: `CANDIDATE_API_OPENAPI_V1.yaml`. Trust and deployment contract: `BROKER_PRIVATE_TOPOLOGY.md`.

## Boundary rules

- Public broker Candidate API: `/candidate-app/v1`.
- Public broker manager API: `/candidate-manager/v1`.
- Private service-bound Candidate API: `/private/candidate-app/v1`.
- Private service-bound manager API: `/private/candidate-manager/v1`.
- Existing CloudTMS office adapters: `/api/candidate-app`.
- The normal CloudTMS Worker exposes only the office adapters; it does not expose the public Candidate or manager paths.
- Public Candidate/manager paths map deterministically to their private-prefixed equivalent over a signed Cloudflare Worker service binding.
- JSON responses use `cache-control: no-store` and stable `error_code` values.
- Public Candidate access and refresh values are independently encrypted broker envelopes bound to audience, environment, expiry and a public session identity.
- The private access token and rotating refresh token are never returned to the browser/native client. The broker unwraps them only for the signed service-bound call; refresh hashes remain the database authority.
- Manager access is an opaque token from the URL fragment; only its SHA-256 reaches SQL.
- EMAIL manager access is independent of the Candidate session. Same-phone PHONE access is a separate short-lived broker envelope bound to the initiating public Candidate session and, where supplied, device identity; the raw private PHONE token is never returned by the public API.
- Upload tickets are short-lived encrypted envelopes. They do not disclose R2 object keys.
- Public requests have exact browser-origin/native-client policy, bounded bodies and independent authentication/manager/upload rate limits before private work.
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
| POST | `/candidate-app/v1/account/push-token` | Broker-validates APNs/FCM/Web Push and encrypts the raw provider token before private registration on the active session. |
| GET | `/candidate-app/v1/bootstrap` | Entitlements, selection and global capabilities. |
| GET | `/candidate-app/v1/timesheets` | Candidate-safe paged projection. |
| GET | `/candidate-app/v1/timesheets/:timesheetId` | Candidate-safe detail and capabilities. |
| GET | `/candidate-app/v1/timesheets/:timesheetId/paper-pack/status` | Read-only return of `PREPARING`, `READY` or `FAILED` and whether secure download is available. It never assembles a pack, updates mail or creates a notification. |
| GET | `/candidate-app/v1/timesheets/:timesheetId/paper-pack` | Read-only stream of an already-ready immutable WEEKLY QR/paper pack. It never assembles or releases work. |
| GET | `/candidate-app/v1/contracts/:contractId/missing-weeks` | Per-week effective missing-week options. |
| POST | `/candidate-app/v1/contracts/:contractId/missing-weeks` | Add one eligible missing week. |
| GET | `/candidate-app/v1/notifications` | Read the account notification feed. |
| POST | `/candidate-app/v1/notifications/:notificationId/read` | Idempotently mark only the owning account's notification read. |

The safe timesheet projection owns client name, job title, band, date/status, route family, action flags, expense overlay and exact workflow state. The broker must not fill gaps with direct database reads.

## Workflow, expense and document API

| Method | Path | Purpose |
|---|---|---|
| POST | `/candidate-app/v1/workflows` | Create a server-validated WEEKLY or DAILY workflow. |
| POST | `/candidate-app/v1/workflows/:workflowId/actions/:action` | Amend, submit, select approval, prepare/return PAPER, remind, renew, cancel or supersede. General public Candidate finalisation is not an action. |
| POST | `/candidate-app/v1/workflows/:workflowId/components/prepare` | Prepare a category-bound source/signature/return component. First execution and replay resolve the same DB-owned object identity and immutable upload contract. |
| PUT | `/candidate-app/v1/uploads/:encryptedTicket` | Upload exact bytes; CloudTMS computes SHA-256 and completes the component. |
| GET | `/candidate-app/v1/workflows/:workflowId/components/:componentId/document` | Stream an authorised workflow document. |
| POST | `/candidate-app/v1/timesheets/:timesheetId/expense-placement` | Resolve eligibility/placement without creating financial truth. |
| POST | `/candidate-app/v1/timesheets/:timesheetId/expense-carrier` | Idempotently resolve/create the server-authorised carrier. |
| POST | `/candidate-app/v1/contract-weeks/:contractWeekId/no-work` | Apply the route/capability-guarded no-work decision. |

Explicit `no_break: true` or an explicit numeric `break_minutes: 0` is valid. Blank or null break values are not silently converted to no-break. DAILY zero break produces the existing checking issue; WEEKLY zero break does not create that issue.

Category selection may be omitted by the app only when its UI context is unambiguous. The component PREPARE/COMPLETE authority still records one exact immutable server category. One evidence object cannot satisfy several categories.

`COMPONENT_PREPARE` idempotency is identity-preserving. SQL returns the authoritative component ID, storage identity, media type, byte size, component kind, document role, category and paper-page key on both first execution and replay. The private API creates the encrypted upload ticket from that returned contract; a retry never substitutes a newly generated object key. Reusing an idempotency key with conflicting type, role, category, media, size or paper-page identity fails closed. The storage key remains private and is never returned as a public response field.

### QR/paper readiness and delivery

Candidate PAPER applies to WEEKLY only. `PAPER_PREPARE` freezes the complete return manifest and automatically calls the existing `timesheet_qr_send_enqueue_v1` authority. That authority creates a durable document operation and an idempotent candidate-email outbox row. The email is held with no attachment until the official unsigned PDF reaches `READY`; the existing document/mail authority then attaches and releases it. The candidate does not make a second email request.

The app must treat these states separately:

```text
QUEUED / RENDERING  -> show "Preparing documents"; download is unavailable
READY               -> notification feed/push announces readiness; download is available
SENT                 -> the automatic candidate email has been sent
FAILED               -> show a controlled retry/support state; never claim readiness
```

QR token creation is not document readiness. The current canonical `TIMESHEET` document version must first be `READY`; the private scheduled worker then assembles one complete immutable pack in exact manifest order: official unsigned timesheet, Expense and Mileage Approval Summary, Mileage Claim Form, and every evidence page. The scheduled worker alone releases the exact workflow/generation/manifest-bound mail operation and idempotent Candidate notification. A `FAILED` mail row remains failed until the existing explicit mail-retry authority acts; Candidate polling cannot requeue it. The status and download GETs are strictly read-only. The Worker verifies Candidate ownership and streams bytes from R2; the broker/app never receives an R2 key.

The Expense and Mileage Approval Summary uses configured agency branding, Candidate/client/week identity and plain-English claim lines rather than internal economic keys. The A4 Mileage Claim Form uses the configured branding, the title `Mileage Claim Form for week ending dd/mm/yyyy`, repeatable `Post Code from`, `Cost Code To` and `Number of miles` rows, total mileage, manager signature/date and stable workflow/page identity. These documents format frozen canonical claim facts only; they do not calculate financial truth.

Paper-pack selection fails closed unless exactly one active `PAPER` workflow in `AWAITING_PAPER_RETURN` targets the current timesheet. More than one produces `CANDIDATE_PAPER_WORKFLOW_CONFLICT`; no arbitrary workflow is selected.

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

Manager-triggered finalisation uses a server-created `CANDIDATE_MANAGER_FINALISATION_V1` context bound to the exact workflow generation, approval method, approved request and review-manifest digest. It deliberately does not require a live Candidate session; the finalisation RPC re-locks and rechecks that complete context before canonical mutation.

Canonical finalisation is triggered only by manager EMAIL/PHONE approval, a complete PAPER return or the authenticated CloudTMS office retry adapter. It is not exposed as a general public Candidate action, so an authenticated Candidate cannot use the service-finalisation path against a workflow UUID owned by another account.

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

The deployment consists of two explicitly configured Workers:

- Candidate broker: exact TEST/LIVE origin list, native-client switch, service binding, four rate-limit bindings and dedicated access/refresh/device/service secrets;
- private CloudTMS API: no public route, explicit TEST/LIVE environment and public frontend URL, Supabase/R2 bindings and separate service/session/challenge/upload secrets.

There are no environment or general-session-secret fallbacks. TEST and LIVE secrets are unrelated. Transactional links remain disabled until the approved Candidate/public frontend URL is configured.

CloudTMS owns canonical notification events and preferences. The Candidate broker owns provider device material and eventual APNs/FCM/Web Push delivery. Provider credentials and app identities are a coordinated broker/app activation gate; in-app notification feed and app-resume refresh remain authoritative where external push is unavailable.

No endpoint in this contract executes Banking Pay, settlement, remittance or a production action.
