# CloudTMS Candidate App — broker/private-backend authority closure handover

Date: 11 August 2026
Status: Candidate DB/RPC/backend closure runtime published, installed and deployed to dormant TEST for independent API-freeze audit. Candidate runtime commit: `a14b60b734560fc5ddf0109bbef6e21eb46e4857`. This document is current through exact manager reminder/renew/cancel meaning, provider-accepted timing, required cancellation reason and withdrawal, the closed timesheet-detail action hub, the server-owned Current/History timesheet partition, and all previously accepted Candidate PAPER, source-owner, route/rejection, upload, renderer and provider-permit closures.

## Executive result

The independent P0 topology finding has been implemented as a targeted separation:

```text
Candidate iOS / Android / web + manager browser
                         ↓
              public Candidate broker
                         ↓
       signed Cloudflare Worker service binding
                         ↓
             private CloudTMS Candidate API
                         ↓
       DB/RPC + R2 + mail + canonical authorities
```

The public broker has no Supabase/R2 or CloudTMS business-authority dependency. The private Candidate API has no public Worker route. The normal CloudTMS Worker retains authenticated office adapters and now rejects public Candidate/manager paths before its global preflight handler.

No DAILY/WEEKLY financial algorithm, Process, Authorise, route/version algorithm, invoice, payment, Banking Pay, Policy X, Google rota/availability or official-timesheet-renderer economics were changed in this closure. Existing Candidate workflow, route-orchestration, QR-enqueue and mail-claim repeatables were tightened only to make one PAPER generation the complete owner of its delivery authority and to retire stale authority safely.

## Runtime namespace and anchor rejection closure retained

These earlier accepted controls are retained unchanged in current Candidate runtime source `a14b60b734560fc5ddf0109bbef6e21eb46e4857`.

- Candidate PAPER retirement and canonical QR enqueue now call `extensions.digest` explicitly with UTF-8 byte conversion, matching the installed TEST pgcrypto schema and removing reliance on an unavailable public wrapper;
- the disposable PostgreSQL fixture no longer invents `public.digest`, so clean-install verification exercises the same namespace contract as Supabase TEST;
- whole-record office rejection locks nonterminal pre-finalisation workflows whose target or anchor is the rejected current timesheet;
- a `FINALISED` workflow is captured only where its exact target is that rejected current timesheet, so finalised hours, combined and separate-expense targets remain rejectable without treating an anchor as their economic target;
- a finalised separate-expense workflow therefore remains immutable and unaffected when its hours anchor is rejected, while an active pre-finalisation expense workflow anchored to those hours is still retired as required;
- finalised approval/component artefacts are closed on the exact preceding frozen generation, while the locked finalised workflow generation moves once to `REJECTED`;
- each captured generation is retired by exact workflow ID and generation across PAPER mail/notification/token authority, approval request, component and workflow state;
- the runtime suites prove finalised hours, combined, separate expense, anchor isolation, replacement submission, replay, Unauthorise-first and protected paid history, not merely source strings.

Current acceptance evidence is 27/27 SQL runtime/concurrency suites on PostgreSQL 17.6 and 18.1, 124/124 focused Candidate/backend/broker/DB/PAPER tests, 460/460 complete backend tests, safe TEST migration and read-only installed-definition verification. All Candidate feature flags remain false and all Candidate business tables remain empty.

## Final rejection projection and finalised PAPER delivery closure

- rejected hours and combined workflows resolve through their current `contract_week_id`, rather than disappearing with the historical submitted target after version rotation;
- rejected separate-expense workflows resolve through their expense contract week and canonical expense-carrier display anchor, so the worked card receives `COMPLETE_EXPENSE_CLAIM` and `RESUBMIT_EXPENSE_CLAIM` rather than a timesheet-resubmission instruction;
- each rejected workflow projects server-owned `rejection_reason`, `rejection_scope` and `required_resubmission_action`;
- `PAID`, `AUTHORISED` and `INVOICED_NOT_PAID` remain canonical top-level precedence, followed by a genuinely active replacement workflow; only an unresolved rejection becomes top-level `REJECTED`, while immutable rejection history remains in detail;
- a finalised PAPER workflow derives its delivery artefact generation as the preceding generation, locks the exact workflow/generation-bound mail rows and blocks an active provider lease before any rejection mutation;
- non-sent obsolete PAPER mail is retained but retired, `PAPER_PACK_READY` notification/deep-link authority is dismissed and obsoleted, and already `SENT` mail remains immutable history;
- QR token/document authority is invalidated through the existing QR refusal owner, whose evidence update now qualifies its table columns so the real rejection route is executable;
- rejection replay creates no second version, no duplicate notification and no repeated generation transition.

## Rejection monotonicity, anchor-family continuity and distinct PAPER QR ownership

- a rejection is actionable only until a true later replacement advances: combined requires combined on the same `contract_week_id`, hours requires hours/combined on that same record, expenses require expense/combined for the same candidate/contract/week, and DAILY requires the same work date and stable booking family;
- a later `FINALISED` workflow suppresses the historical rejection and permits the canonical current processing state to win; a later `REFUSED` workflow exposes the current refusal rather than resurrecting the prior office rejection;
- immutable workflow history remains available, but only unresolved rejections receive a resubmission action;
- list and detail responses return deterministic scoped `rejections` arrays, allowing independent hours and expense recovery actions to coexist without one `LIMIT 1` choice hiding the other;
- historical workflow anchors and expense-carrier parents resolve by stable booking/version identity to exactly one current same-candidate/same-contract/same-week worked row. A finalised separate expense remains overlaid beneath H2 after hours H1 is rejected and rotated to H2;
- `_candidate_paper_delivery_retire_set_v1` derives each QR source from exact workflow-generation-bound mail context and composes the exact single-generation retirement owner. It does not use the workflow's mutable final expense-carrier target as the QR owner;
- all relevant waiting/received/finalised PAPER workflows and bound outbox rows on the source are locked and lease-checked before mutation. A live token must have one immutable owner, is invalidated once, and every stale delivery surface is retired even when the token was already absent;
- executable PostgreSQL tests cover `WORKER_DRAFT → FINALISED → REFUSED`, hours-only versus combined identity, multiple additional contract-week records, simultaneous hours/expense rejection scopes, H1→H2 expense visibility, both UUID orderings, queued/sent mail combinations, already-cleared-token cleanup and separate-expense PAPER where H1 owns the issued QR token but E1 is the rejected target.

No table, public Candidate RPC, HTTP route, policy or economic owner was added or changed.

## Retryable RECEIVED PAPER and rejection-lock closure

The previous source-set authority was correct for `AWAITING_PAPER_RETURN` and `FINALISED` but omitted the documented retry state created when a complete PAPER return is accepted and immediate canonical finalisation encounters a controlled blocker. This revision closes that state and the linked rejection concurrency seam:

- both `_candidate_paper_delivery_retire_v1` and `_candidate_paper_delivery_retire_set_v1` accept exactly `AWAITING_PAPER_RETURN`, `RECEIVED` and `FINALISED`;
- `RECEIVED` uses the current generation for its immutable mail/pack identity; `FINALISED` continues to use the preceding delivery generation;
- every relevant-workflow selection, workflow/outbox lock, provider-lease check, current-token-owner lookup, mail retirement, notification retirement and preserved-workflow receipt uses the same three-state set;
- `candidate_submission_reject_atomic_v1` adds a selected `PAPER / RECEIVED` workflow to the set-level retirement and requires the source receipt before timesheet rotation or workflow rejection;
- a queued/unleased generation is made inert; a live lease returns `CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS` before mutation; sent mail remains immutable; `PAPER_PACK_READY` becomes dismissed/obsolete; the exact source token is invalidated or proved already invalid;
- the provider adapter calls the service-only `PAPER_PROVIDER_SUBMIT_PERMIT` action immediately before external email submission. The database atomically locks and proves exact mail/workflow/generation/manifest/lease authority and renews the exact lease as a submit permit;
- a stable candidate/contract/week advisory lock is taken by rejection before target, contract-week, TSFIN or workflow row locks. The source-set helper uses the same family and deterministic row order, eliminating the H1/E1 workflow/source lock cycle;
- the two-session PostgreSQL suite runs simultaneous hours-source and separate-expense rejection in both start orders and rejects any uncontrolled `40P01` deadlock or partial lifecycle mutation.

No new schema object, Candidate table, public RPC, public route or product decision was introduced. The correction does not change DAILY/WEEKLY economics, Process, Authorise, invoice, payment, Banking Pay or Policy X.

## Manager EMAIL lifecycle and provider-submit closure

Manager invitation, reminder, renewal and withdrawal delivery now uses the same exact lifecycle model as the accepted PAPER boundary:

- every row is scoped as `MANAGER_APPROVAL_V1` and binds mail kind, workflow ID/generation, approval-request ID/generation and retired state;
- queue creation validates the exact EMAIL request, recipient, generation and current/terminal state appropriate to the mail kind;
- a central approval-request trigger invokes `_candidate_manager_mail_retire_v1` whenever an EMAIL request leaves its current state, so no installed or later cancellation/supersession/refusal/expiry/approval caller can bypass mail retirement;
- retirement locks the exact request and mail set, blocks a live provider lease before lifecycle mutation, makes queued mail inert, preserves `FAILED` as failed-but-retired and preserves provider-accepted `SENT` history;
- the normal mail worker calls `MANAGER_PROVIDER_SUBMIT_PERMIT` immediately before the provider request. That service-only action locks and proves the exact outbox lease, request/workflow generations, recipient, state, expiry and manifest before extending the lease as a submit permit;
- route takeover sends a withdrawal only where retirement proves provider-accepted earlier mail for that exact request. A request timestamp is never used as delivery proof.
- initial and renewal request creation leaves send timestamps unset. The exact provider-accepted outbox receipt owns initial/latest sent time, expiry presentation and the 24-hour reminder clock;
- `REMIND` rotates the token on the same pending approval-request ID/generation, enforces the five-resend maximum and cannot queue behind an exact pending delivery;
- `RENEW` is accepted only for the unchanged expired request and creates a new request generation/token/expiry/resend allowance;
- `CANCEL` requires a non-empty reason, records it in response/audit truth and creates one deterministic withdrawal only where the retired request has provider-accepted delivery history.

## Current/History read-contract closure

The Timesheets read boundary now implements the approved server-owned partition:

- `GET /candidate-app/v1/timesheets` defaults to `view=current`; `view=history` is explicit and invalid values fail before RPC work;
- Current excludes future weeks, includes all unpaid rows with no age limit and includes paid rows exactly seven days old or newer;
- History includes only paid rows older than seven days inside each contract's effective current week plus 15 preceding contract weeks;
- classification is frozen to `snapshot_utc`; view/candidate/snapshot identity is carried by a v2 cursor, so pagination cannot cross tabs or candidates;
- order is week ending newest first with deterministic contract, additional-sequence and row tie-breakers;
- every card returns `Week Ending …`, one server primary action and a stable detail target; detail aliases accept timesheet, contract-week or workflow identity and return the same membership/action truth;
- no public RPC, table or financial owner was added. Future office/client UI must consume these fields and may not recalculate them.

## Timesheet-detail action-hub closure

- all three detail aliases return the same typed detail contract;
- the private action-contract helper supplies one `primary_action`, a closed `available_actions` array and EMAIL `manager_approval` facts without becoming a public business RPC;
- action objects include code, label, exact method/path, workflow ID/generation, approval-request identity where applicable, confirmation, enabled state and stable disabled reason;
- the action catalogue distinguishes continuing hours from continuing expenses, whole-claim from expense-only cancellation, reminder from expired-request renewal, PAPER download/return, rejection recovery, no-work and retry-finalisation;
- `RETRY_FINALISATION` verifies candidate/workflow ownership and `RECEIVED` state before composing the existing service finalisation owner. A general public Candidate `FINALISE` action remains unavailable;
- no frontend may infer action eligibility from status wording or silently translate one action into another.

## Complete PAPER caller and provider-submit closure

The remaining independent findings were caused by adjacent installed callers that still accepted PAPER `RECEIVED` or finalised delivery history without composing the shared retirement/barrier owner. The complete caller matrix is retained in the published Candidate runtime lineage:

- `candidate_workflow_transition_atomic_v1` routes PAPER `RECEIVED` cancellation and supersession through `_candidate_paper_delivery_retire_set_v1` and requires both the durable retirement receipt and QR-invalidation proof before component/request/workflow mutation;
- active provider permit/lease blocks cancellation/supersession with zero mutation; queued unsent mail becomes inert, `SENT` mail remains immutable and the exact readiness notification/deep link becomes obsolete;
- route context resolves the exact linked PAPER workflow for `AWAITING_PAPER_RETURN`, `RECEIVED` and `FINALISED`, including immutable mail/version-family linkage rather than relying only on `active_workflow_id`;
- `_timesheet_route_supersede_candidate_v1` retires the current delivery generation for `AWAITING_PAPER_RETURN`/`RECEIVED` or the preceding delivery generation for `FINALISED`. A finalised workflow and signed evidence remain immutable while stale delivery authority is retired;
- `timesheet_route_version_confirmed_v1` and `candidate_submission_reject_atomic_v1` acquire the same candidate/contract/week family advisory lock before booking, target, TSFIN, workflow or source-row locks;
- the two-session suite executes W09 conversion of E1 and rejection of shared source H1 in both start orders and permits only a deterministic serial result or controlled stale-context conflict, never `40P01` or partial lifecycle mutation;
- `PAPER_PROVIDER_SUBMIT_PERMIT` is a service-only action inside the existing 14-RPC surface. It atomically locks the exact workflow and outbox row, verifies the live lease, immutable generation/manifest, complete-pack attachment and non-retired `PAPER / AWAITING_PAPER_RETURN` authority, and renews the lease as the submit permit;
- `PAPER_RETURN` locks that exact Candidate-bound outbox row before changing the workflow to `RECEIVED` and refuses while the permit/lease is live. The same barrier is respected by cancellation, supersession, rejection and route intervention;
- ordinary non-Candidate mail bypasses the Candidate permit and retains its existing provider behaviour.

This is lifecycle/orchestration closure only. It adds no table, column, public RPC, public route, financial calculation or product-policy change and does not alter Banking Pay or Policy X.

## PAPER source-owner and claim-isolation closure

The final caller-selection defect is closed without changing the seven-table/fourteen-RPC architecture:

- `private._candidate_paper_source_workflow_context_v1` resolves one stable current QR source and the exact immutable current-token workflow from workflow/generation-bound mail receipts;
- a historical finalised workflow in `timesheets.candidate_workflow_id` cannot override a later active standalone expense workflow that owns the current token;
- with no live token, exactly one nonterminal source workflow may be selected; finalised history is considered only when no nonterminal workflow remains, and ambiguity fails closed;
- `_candidate_paper_delivery_retire_set_v1` rejects before mutation if any unselected workflow is `AWAITING_PAPER_RETURN` or `RECEIVED`; only terminal immutable history may be preserved;
- W08/W09 route preview and confirmation share this identity in the signed context hash, then supersede the actual live owner before H1 rotates to MANUAL H2;
- claim-level cancellation/supersession cannot retire another claim's live pack: shared-source conflict returns with zero mail, notification, QR or workflow mutation;
- executable regression covers historical finalised hours W1 plus later expense W2 in both `AWAITING_PAPER_RETURN` and `RECEIVED`, ambiguous multiple nonterminal owners and cancellation isolation.

This is lifecycle identity closure only. It adds no table, public RPC, public route, financial calculation or product-policy change and does not touch Banking Pay or Policy X.

## Pre-delivery PAPER source-rotation guard

The latest independent audit correctly identified that immutable delivery ownership alone could not find a legitimate later PAPER claim before `PAPER_PREPARE`, because a `WORKER_DRAFT` or `WORKER_SUBMITTED` workflow has no mail receipt yet. The targeted correction deliberately fails closed rather than silently cancelling an independent expense claim:

- `private._candidate_paper_source_workflow_context_v1` retains its exact mail-backed current-token owner, then builds a separate affected-nonterminal catalogue through the current stable booking/version family without requiring `mail_outbox`;
- the catalogue includes draft, submitted, review/approval, waiting-return and received PAPER states tied by target or anchor to the source family;
- if the selected immutable delivery owner is absent from, or differs from, the sole affected nonterminal workflow—or more than one is affected—the source context returns `CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT`;
- route preview returns a controlled not-permitted context and does not retire mail, dismiss notifications, clear QR identity, transition a workflow or rotate a timesheet;
- route confirmation recomputes and locks the complete booking/version-family workflow and approval set in deterministic order rather than trusting preview identity alone;
- immediately before `timesheet_route_version_rotate`, `CONVERT_QR_TO_MANUAL`, `DISABLE_QR`, `INVALIDATE_QR` and `REISSUE_QR` prove that no nonterminal Candidate PAPER workflow remains tied exclusively to the source being made historical;
- executable PostgreSQL regression covers historical finalised hours W1 plus a later no-mail expense W2 in both `WORKER_DRAFT` and `WORKER_SUBMITTED`, all four source-rotating actions and zero-mutation confirmation failure.

This closes the lifecycle inconsistency without adding a table, public RPC, route, frontend inference, notification rule or financial authority. It does not change Banking Pay or Policy X.

## Final PAPER generation-retirement closure

Runtime source: `6ddba7f17f98a4265232b4b2ac51b1ac25d46687`.

- complete-pack release is now a service-only action in the existing workflow RPC and is atomic across workflow/current-timesheet proof, the exact held outbox row, attachment receipt, release timing and insert-once notification;
- the private scheduler no longer mutates `mail_outbox` or `candidate_notifications` directly;
- one private retirement helper is composed by PAPER amendment, cancellation, supersession, office rejection and canonical route/QR intervention;
- obsolete non-sent delivery rows are retained as inert audit history, old readiness notifications/deep links are retired and the old QR token/generated-document identity is invalidated;
- a provider-owned live lease blocks retirement before mutation; a completed retirement is independently rejected by the mail claimant's current workflow/generation/route/state/manifest fence;
- fresh PAPER generations receive fresh mail, QR token and document identities, so the previous operation cannot block or be reused by the new generation;
- sent mail and all signed/issued R2 evidence remain immutable history and are never physically purged by this correction.

That PAPER-generation revision remains covered by the single current acceptance record above; its historical partial totals are deliberately not retained as release evidence.

## Final retry, document and HTTP authority closure in this revision

- `COMPONENT_PREPARE` accepts an idempotent replay only for the workflow's current generation and only while the original component is `PENDING` or already `IMMUTABLE`; cross-generation and terminal-state reuse fails before a ticket can be created;
- `COMPONENT_COMPLETE` accepts an identical immutable digest replay, permits only `PENDING` to become `IMMUTABLE`, and guards the final update so `SUPERSEDED`, `REJECTED` or `ABANDONED` evidence cannot be revived;
- every persisted official, expense, mileage and combined-paper-pack PDF disables live-clock metadata and binds its immutable identity to frozen input, renderer version and frozen agency/logo digest;
- generated-document and paper-pack retries inspect the durable R2 receipt first and resume registration/release from exact existing bytes after a post-write failure;
- manager methods are exact at both boundaries: `GET start`, `POST progress`, `POST approve`, `POST refuse`, `POST signature/prepare` and `GET document`; a mismatch returns `405` before any RPC;
- paper notifications are insert-once and cannot reset read/dismissed or claimed/sent/failed push state;
- paper workflow multiplicity is checked before any document-state return, and outbox bind/release requires one verified expected-status compare-and-set before notification;
- the expense renderer requires the explicit frozen canonical display total and performs no fallback category summation.

## Final audit finding closure in this revision

### Public Candidate finalisation authority

- removed `FINALISE` from the public Candidate action enum, router and OpenAPI contract;
- retained manager EMAIL/PHONE, complete PAPER return and authenticated office retry as the only service-finalisation triggers;
- service finalisation remains manager/request/manifest bound and session-independent where intended;
- an authenticated Candidate can no longer present an unrelated workflow UUID to the service-finalisation path.

### Idempotent component preparation

- SQL now returns component ID, generation, original storage key, media type, byte size, kind, role, expense category, paper-page key and state on both first PREPARE and replay;
- reuse of the same idempotency key with conflicting kind, role, category, media, size or paper-page identity fails with `CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT`;
- the private API builds the encrypted upload ticket from the SQL-returned authoritative contract, never from a newly generated replay key;
- the raw storage key remains private and cannot appear in the public response.

### Read-only paper-pack delivery and exact release ownership

- status/download GETs now perform only Candidate-safe ownership reads, immutable receipt inspection and ready-byte streaming;
- all R2 pack assembly, outbox release and notification creation moved to the scheduled private worker;
- PAPER_PREPARE binds the exact QR mail-outbox operation to workflow, generation and manifest identity;
- release updates only that exact bound `QUEUED` operation and never resets a `FAILED` operation;
- repeated polling and concurrent clients cannot create a pack, requeue mail or duplicate the readiness notification;
- zero matching workflows remains not-ready; more than one active matching PAPER workflow fails closed with `CANDIDATE_PAPER_WORKFLOW_CONFLICT`.

### Approved paper documents

- the Expense and Mileage Approval Summary now uses configured agency branding, Candidate/client/week identity and plain-English claim lines instead of internal economic keys;
- the A4 Mileage Claim Form uses the configured agency branding and exact title/columns, ten repeatable journey rows, total mileage, manager signature/date and stable workflow/page identity;
- both outputs were rendered as single-page A4 PDFs and visually inspected for clipping, overlap and readability;
- the renderers only present frozen canonical facts and do not derive financial truth.

## Audit finding closure

### Public broker versus private CloudTMS authority

Implemented:

- new public broker entry point and deployment manifest;
- new private Candidate API entry point and deployment manifest;
- deterministic public-to-private path mapping;
- Worker service binding only between the two artefacts;
- HMAC service request binding of environment, method, path/query, timestamp, nonce, complete bounded body, internal authorisation and selected request headers;
- explicit `PRIVATE` and `OFFICE` route audiences in the existing Candidate backend module;
- direct public Candidate/manager route rejection by the normal CloudTMS Worker, including OPTIONS/preflight.

### Public security controls

Implemented:

- exact HTTPS origin allowlist; wildcard and malformed origins fail closed;
- manager routes require an approved browser origin;
- no-Origin Candidate requests require an enabled and declared `ios` or `android` client;
- exact CORS preflight methods/headers;
- independent Cloudflare rate-limit bindings for general, authentication, manager and upload traffic;
- IP plus account/session/token/workflow-context keys are hashed before rate-limit use;
- 1 MiB JSON and 15 MiB upload limits are enforced before signing/forwarding;
- private 5xx details are replaced by public-safe stable errors;
- challenge start/resend returns the same accepted result for eligible, unknown, disabled or already-activated account state, with a minimum response floor after public shape validation;
- login still uses the existing dummy-verifier path and generic invalid-login error.

### Environment and secret separation

Implemented:

- explicit TEST/LIVE environment; no implicit TEST fallback;
- separate broker access, refresh and device-token secrets;
- separate private service, Candidate session, challenge and upload secrets;
- no fallback to the normal CloudTMS session secret;
- no secret values in either deployment manifest;
- private configuration fails readiness if a required binding/secret is absent.

### Candidate tokens and sessions

Implemented:

- browser/native access and refresh values are broker-sealed AES-GCM envelopes;
- access envelope binds public audience, environment, public session, internal access token and expiry;
- refresh envelope binds a separate audience, environment, public session, internal session, internal rotating refresh value and absolute expiry;
- internal Candidate access/refresh values are never returned to the public client;
- private database refresh-family rotation, reuse detection and family revocation remain authoritative; every session create/rotate and account/family-wide invalidation participates in one transaction-scoped per-account lock taken after receipt ownership and before account/session row locks, so a concurrent refresh successor cannot escape reset, password change, reuse revocation, revoke, lock or disable.

### Manager approval and finalisation independence

Implemented:

- EMAIL manager requests continue to use the one hashed approval-token authority and no Candidate bearer/session is required by the manager;
- `SELECT_PHONE_APPROVAL` now creates a real short-lived PHONE approval request with only its hash stored in SQL;
- the broker wraps the raw PHONE token in a separate AES-GCM handoff envelope bound to the initiating public Candidate session, workflow/request and optional device ID;
- PHONE manager start, component access, progress, signature upload, approve and refuse all use that handoff token rather than pretending to remain the Candidate;
- `CANCEL_MANAGER_HANDOFF` retires only the unfinished PHONE request/signature and returns the workflow to approval-route selection;
- final rendering and canonical finalisation use an exact `CANDIDATE_MANAGER_FINALISATION_V1` context bound to workflow generation, method, approved request and review-manifest digest;
- the finalisation RPC locks and revalidates that service context, then continues through the existing WEEKLY/DAILY, Process and optional Authorise owners without an active Candidate session;
- the runtime suite revokes the Candidate session before finalisation and proves the canonical path still completes.

### Immutable storage and complete PAPER pack

Implemented:

- both uploaded components and server-generated review/final documents use conditional create-only R2 writes;
- exact same-digest/context replay is idempotent; different bytes at the same immutable key fail with a stable conflict and can never delete the winning object;
- SQL component completion validates media type, size and digest before returning an idempotent response, so a different-byte retry cannot be mistaken for success;
- one-page PDF expense evidence is valid end to end, matching the HTTP validator and installed database policy;
- the Candidate PAPER return manifest drives one complete pack in frozen order: official QR timesheet page(s), professional Expense and Mileage Approval Summary, approved Mileage Claim Form, then every evidence page;
- QR email attachment release and `PAPER_PACK_READY` notification occur only after the complete pack is durably stored by the private scheduler; Candidate GET polling is read-only and a failed email is never requeued by a read.

### Upload and evidence hardening

Implemented in the private CloudTMS API:

- actual PNG/JPEG/PDF byte validation, not declared MIME alone;
- malformed image rejection;
- malformed/encrypted PDF rejection;
- exactly one page per evidence PDF;
- configurable maximum bytes, image dimension and total pixels;
- private SHA-256 calculation before immutable component completion;
- existing duplicate-byte, owner, workflow, generation and component checks retained;
- no raw R2 key in public responses.

### Push ownership

Implemented now:

- the broker accepts only `APNS`, `FCM` or `WEB_PUSH` registration;
- raw provider tokens are encrypted using a broker-only device secret;
- only ciphertext, provider and key version cross the private boundary;
- round-trip encryption/decryption is unit-tested;
- CloudTMS notification rows remain canonical event/preference/dedupe/deep-link truth.

Provider transport activation remains correctly sequenced with the app stage because APNs/FCM/Web Push application identities, credentials and TEST devices do not yet exist. The broker owns that future delivery adapter, retry/dead-letter and invalid-token retirement. Until activation, the in-app notification feed and resume/refresh path remain authoritative and workflow truth never depends on an external push.

### API contract

Implemented:

- `CANDIDATE_API_OPENAPI_V1.yaml` contains every current public Candidate and manager operation;
- each operation records its deterministic private path;
- public token, manager token, upload, pagination, health/readiness and stable error contracts are versioned;
- private service-HMAC headers and mapping are recorded;
- Redocly OpenAPI 3.1 validation passes.

## Source inventory for this correction

### New runtime files

- `candidate-broker/src/candidate-broker.js`
- `candidate-broker/src/index.js`
- `candidate-broker/wrangler.jsonc`
- `candidate-private-api/wrangler.jsonc`
- `broker/src/candidate-private-worker.js`
- `broker/src/candidate-service-auth.js`

### Amended runtime files

- `broker/src/candidate-app-backend.js`
- `broker/src/index.js`
- `package.json`
- `.github/workflows/candidate-db-runtime.yml`
- `supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql`
- `supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql`
- `supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql`
- `supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql`
- `supabase/repeatable/23072026_2207_email_outbox_claim_ready_batch.sql`
- `supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_invoice_work_complete_batch.sql`

### New/amended tests

- `tests/candidate-broker-boundary.test.js`
- `tests/candidate-app-backend.test.js`
- `tests/candidate-app-db-rpc-contract.test.cjs`
- `tests/candidate-paper-mail-authority.test.js`
- `tests/07082026_2155_candidate_app_local_runtime_verification.sql`
- `tests/08082026_1040_candidate_app_policy_corrections_runtime_verification.sql`
- `tests/08082026_1200_candidate_app_expense_workflow_runtime_verification.sql`
- `tests/10082026_1113_candidate_paper_mail_authority_verification.sql`
- `tests/10082026_1817_candidate_finalised_rejection_verification.sql`
- `tests/10082026_2005_candidate_finalised_paper_rejection_verification.sql`
- `tests/fixtures/07082026_2155_candidate_app_local_compile_base.sql`

### Living contract and plan

- `docs/candidate-app/IMPLEMENTATION_PLAN.md`
- `docs/candidate-app/BACKEND_API_CONTRACT.md`
- `docs/candidate-app/AUTHORITY_MAP.md`
- `docs/candidate-app/BROKER_PRIVATE_TOPOLOGY.md`
- `docs/candidate-app/CANDIDATE_API_OPENAPI_V1.yaml`
- `docs/candidate-app/ROUTE_WARNING_CATALOGUE.md` — controlling W01–W14 copy

## Final Candidate PAPER acceptance and mail-authority closure

- `candidate_workflow_transition_atomic_v1/PAPER_PREPARE` now locks the current timesheet before the workflow, freezes the PAPER return manifest and invokes the existing `timesheet_qr_send_enqueue_v1` authority inside the same PostgreSQL transaction;
- missing, opted-out or unresolved Candidate email, a canonical QR/document queue rejection, or failure to prove one exact held outbox row raises a stable error and rolls back the workflow/QR/document/mail/audit transaction;
- the private backend consumes the SQL receipt, verifies rather than creates the workflow/outbox binding, and never returns an accepted PAPER response unless the queue, recipient and exact binding are all proven;
- the scheduled pack worker requires exactly one matching unclaimed held-or-complete outbox operation before R2 receipt lookup or pack assembly;
- zero or multiple outbox rows, failed/claimed mail, a partial binding or a lost compare-and-set cannot create `PAPER_PACK_READY`;
- the mail claim gate treats any Candidate PAPER marker as requiring the complete pack proof, so a malformed partial row cannot fall through to ordinary QR mail;
- the sole `email_outbox_claim_ready_batch` definition was moved to the top-level repeatable directory consumed by the safe installer. No duplicate qualified function definition was introduced;
- this is an orchestration and installation-authority correction only. It changes no DAILY/WEEKLY economics, rates, pay, charge, VAT, ERNI, margin, TSFIN, Process, Authorise, invoice economics, payment, Banking Pay, Policy X or frontend behaviour.

## Verification evidence

### Exact replacement identity and source-set PAPER closure

- `private._candidate_rejection_replaced_v1` is the one list/detail replacement predicate. A later hours-only workflow cannot suppress a rejected combined claim, a workflow on a different additional `contract_weeks` record cannot suppress another record, and a DAILY replacement must share both work date and stable booking family;
- expense replacement remains deliberately week-level because a separate expense carrier can be recreated while still representing the same candidate/contract/week expense claim family;
- `private._candidate_paper_delivery_retire_set_v1` freezes the complete selected workflow set, resolves immutable mail receipts to stable QR source families, locks every relevant waiting/received/finalised PAPER workflow and bound outbox row for each source, and checks every provider lease before mutation;
- the current QR token is matched to exactly one immutable delivery owner and invalidated once. Every delivery surface on the same source is then retired, including when the token was already cleared; sent mail and immutable R2 history remain untouched;
- an unrelated finalised separate-expense workflow can therefore remain finalised with unchanged economics while its obsolete source pack/mail/notification is made inert before the hours row rotates;
- the full SQL fixture proves both UUID orderings, selected active versus preserved finalised expense workflows, queued/sent permutations, active-lease rollback, already-cleared-token cleanup and idempotent replay.

This is lifecycle identity and stale-delivery closure only. It introduces no schema object, table, public RPC, public route, frontend change or financial calculation.

| Verification | Result |
|---|---:|
| Changed/new JavaScript syntax | PASS |
| Focused Candidate/backend/broker/DB-contract/PAPER tests | 124 passed, 0 failed |
| Complete backend test suite | 460 passed, 0 failed |
| PostgreSQL 17.6 runtime/concurrency suites | 27 passed, 0 failed |
| PostgreSQL 18.1 runtime/concurrency suites | 27 passed, 0 failed |
| Candidate Worker source/configuration change | Manager provider-permit adapter and Current/History HTTP adapter updated; all three Worker dry-run builds passed |
| OpenAPI 3.1 lint | PASS |
| Git whitespace/error check | PASS |
| GitHub TEST database migration workflow `31495500073` | PASS |
| GitHub exact PostgreSQL 17.6/18.1 matrix workflow `31495500205` | PASS |
| Public Candidate broker health/readiness | 200 / 200 |
| Normal TEST backend health/readiness | 200 / 200 |
| Direct public Candidate route on normal backend | 404, as required |

The deployment used repository-installed Wrangler 4.43.0 after the remote branch and active-workflow gate proved the exact tested runtime was still current and no other push/deployment was active. Active TEST identities are:

- normal TEST backend: `14554299-dcb6-4102-8ef9-7316d34b3654`;
- private Candidate API: `407596b9-5dd4-48fa-939a-4a9b2885ef74`;
- public Candidate broker: `7dfc34ec-b78f-48ca-8b0d-ebd847cfcebc`.

The private Worker secret inventory includes `SUPABASE_SERVICE_ROLE_KEY` and all four dedicated Candidate private secrets. The secret values were not displayed, logged, committed or packaged.

## Independent audit requests

Please verify, function by function, including the final authority seams:

1. `handleCandidateBrokerRequest` never gains DB/R2/business authority and enforces origin, rate, token and body boundaries before forwarding.
2. `signCandidatePrivateRequest` and `verifyCandidatePrivateRequest` bind the same complete request context and fail closed.
3. `candidate-private-worker.js` has no public path, requires service authentication and injects only the private Candidate route audience.
4. the normal `broker/src/index.js` rejects public Candidate/manager routes before preflight while preserving authenticated office adapters;
5. internal Candidate tokens cannot appear in public responses and refresh replay remains governed by the existing DB/RPC;
6. evidence validation cannot accept a multi-page PDF, malformed/encrypted PDF or malformed image and still preserves the one-component-per-page/category policy;
7. no financial/lifecycle authority has moved into the broker and no existing economic calculation changed;
8. the OpenAPI path inventory matches the actual broker/private router;
9. provider push activation can remain a coordinated broker/app gate without changing canonical notification truth;
10. manager EMAIL, same-phone PHONE, office PHONE and PAPER completion cannot be blocked by Candidate logout/session expiry, while an invented or stale service-finalisation context fails closed;
11. `SELECT_PHONE_APPROVAL` creates only a short-lived hashed request and the broker binds its public handoff envelope to the initiating public session/device;
12. TEST Candidate selection cannot expose the internal session UUID or break the existing refresh envelope;
13. upload and generated-document create-only writes accept exact replay but never allow a losing writer to overwrite or delete the winner;
14. one-page PDF expense evidence is accepted consistently by HTTP and SQL, while multi-page/malformed/encrypted PDFs fail;
15. the downloadable/email QR pack includes every frozen required page in the exact return-manifest order and readiness is released only after the full immutable bundle exists;
16. the OpenAPI path/method/query inventory matches the implemented broker/private router and notification pagination.
17. public Candidate workflow actions cannot invoke service finalisation, while manager approval, PAPER completion and office retry remain functional and idempotent;
18. PREPARE replay returns the first stored upload contract, conflict reuse fails, and no ticket can point to a second orphan object key;
19. paper-pack status/download are read-only and cannot write R2, mail or notifications, including when a bound mail row is `FAILED`;
20. the scheduler releases only one workflow/generation/manifest-bound outbox row and fails closed on multiple active paper workflows;
21. the expense summary and mileage form meet the configured-brand, plain-English, A4 table and stable-identity presentation contract without altering economics.
22. component PREPARE cannot cross a workflow generation or revive a terminal component, and COMPLETE can mutate only `PENDING` while preserving identical immutable replay;
23. every persisted Candidate PDF is byte-identical for the same frozen contract across wall-clock time and can recover after R2 success plus later registration/release failure;
24. unsupported manager HTTP methods return `405` with zero RPC calls at both public and private boundaries;
25. duplicate paper release cannot reset notification read or push state, and no notification is created after a lost outbox compare-and-set race;
26. paper multiplicity fails before PREPARING/FAILED/missing-document returns, and rendering fails closed when the explicit canonical expense total is absent.
27. `PAPER_PREPARE` and canonical QR/email enqueue share one transaction and one timesheet-before-workflow lock order;
28. missing/opted-out email and canonical enqueue failure leave the workflow in its pre-PAPER state with no bound/partial mail operation;
29. zero outbox rows, a claimed row or a partial binding prevent pack assembly/release and create no readiness notification;
30. one successful PAPER preparation returns only the public-safe queued receipt while the private outbox ID remains behind the private boundary;
31. `email_outbox_claim_ready_batch` has exactly one repository definition and its top-level repeatable ledger SHA matches the installed TEST definition;
32. all Candidate flags/tables/mail remain disabled and empty after harmless deployment verification;
33. finalised hours, combined and separate-expense workflows reject when their exact target is rejected, while a separate finalised expense workflow survives rejection of its anchor hours;
34. finalised approval/component artefacts close on the frozen pre-finalisation generation, and rejection replay cannot rotate or notify twice;
35. authorised finalised targets still require Unauthorise first and paid/invoiced/protected history still blocks rejection.
36. a replacement reaching `FINALISED` or `REFUSED` cannot make its historical office rejection actionable again;
37. an immutable H1 anchor resolves to current H2 so a preserved finalised separate expense remains visible and overlaid without an anchor conflict;
38. independent unresolved HOURS and EXPENSES rejections are returned together with their distinct server-owned recovery actions;
39. finalised `CONTRACT_EXPENSE` PAPER derives the QR source from its immutable delivery receipt, invalidates H1, rotates E1, and fails atomically if source/hash/lease proof is incomplete.
40. an hours-only workflow cannot suppress a rejected combined claim, and an additional-timesheet workflow cannot suppress rejection of another `contract_weeks` record;
41. list and detail use the same exact replacement helper for CONTRACT and DAILY identities;
42. every waiting/received/finalised PAPER workflow and bound outbox row on a shared source is locked and lease-checked before retirement mutation;
43. shared-source retirement is independent of workflow UUID ordering and queued/sent combinations;
44. a preserved finalised expense workflow retains its lifecycle/economics while every obsolete pack notification/mail generation on its hours source becomes inert;
45. a source whose QR token was already cleared still retires all stale delivery surfaces and cannot later send an obsolete queued pack.
46. a real `PAPER_RETURN → RECEIVED → controlled finalisation blocker → office rejection` path retires the exact queued generation, obsoletes `PAPER_PACK_READY`, invalidates the QR source and inserts only the office rejection;
47. a live provider lease on any `AWAITING_PAPER_RETURN`, `RECEIVED` or `FINALISED` generation blocks the entire rejection before workflow, timesheet, mail, notification or token mutation;
48. a `RECEIVED` workflow can own the current shared-source token and participates in every owner/lease/mail/notification query without a false owner conflict;
49. provider submission obtains one atomic exact Candidate outbox/workflow/generation/manifest/lease submit permit immediately before send;
50. simultaneous rejection of hours H1 and its separate-expense carrier E1 in either start order cannot deadlock or partially commit and uses one stable rejection-family lock order;
51. `CANCEL` and `SUPERSEDE` on retryable PAPER `RECEIVED` either retire the exact queued generation first or fail with zero mutation while a provider permit/lease is live;
52. confirmed W09 conversion retires the exact `RECEIVED` or preceding finalised delivery generation before route rotation, without deleting signed/sent history;
53. route conversion and office rejection share the same family lock and cannot form the E1-workflow/H1-source deadlock in either start order;
54. `PAPER_RETURN` and every retirement-causing transition lock/respect the provider-submit lease barrier, so authority cannot change between the atomic permit and external send;
55. ordinary non-Candidate mail and route conversion retain their existing behaviour.
56. a historical finalised hours workflow cannot override the later active expense workflow that owns the current source token;
57. W08/W09 route confirmation supersedes that exact owner before source rotation, leaving no waiting/received workflow bound exclusively to historical H1;
58. source-wide retirement preserves terminal history only and fails before mutation on an unselected waiting/received workflow;
59. claim-level cancellation cannot invalidate another claim's live source delivery and returns a controlled conflict with zero mutation.
60. a no-mail `WORKER_DRAFT` or `WORKER_SUBMITTED` PAPER expense workflow is included in source context through stable booking/version identity;
61. a distinct affected pre-delivery workflow makes all four source-rotating QR actions not permitted with zero mutation;
62. confirmation recomputes and locks the complete affected workflow set and cannot trust a stale preview owner;
63. no successful source rotation can leave any nonterminal Candidate PAPER workflow tied exclusively to the historical source.
64. every manager EMAIL outbox row is bound to one exact approval request, request generation, workflow, workflow generation, recipient and mail kind;
65. cancellation, supersession, refusal, expiry and approval centrally retire queued/failed manager mail, preserve accepted history and fail before mutation while the exact provider lease is live;
66. the manager provider handoff obtains `MANAGER_PROVIDER_SUBMIT_PERMIT` immediately before send, and an old invitation/reminder/renewal cannot be claimed or sent after authority changes;
67. withdrawal is created only when exact provider-accepted history exists, never from a local sent timestamp alone;
68. `CURRENT` and `HISTORY` are disjoint at one frozen snapshot: Current contains no future week, all unpaid records and paid records at or after the exact seven-day cutoff; History contains only older-paid records in each contract's effective current week plus its previous 15 weeks;
69. Current/History cursors are view-, candidate- and snapshot-bound; ordering is newest week ending first; the server returns the exact `Week Ending 1 January 2025` label, one primary action and stable detail identity;
70. card/detail routes expose the same current lifecycle, available actions and scoped rejection truth, and the future office frontend must use the detail screen as the action hub rather than infer an action-menu contract;
71. request creation/queue time never becomes manager-send truth; detail and reminder eligibility use the exact provider-accepted outbox receipt;
72. `REMIND` preserves the same request ID/generation, rotates its token, enforces the 24-hour boundary, five-resend maximum and pending-delivery fence;
73. `RENEW` is available only for an expired unchanged request and creates a new request generation/token/expiry/resend allowance;
74. `CANCEL` requires and records a reason, retires delivery before mutation and creates at most one provider-proved withdrawal;
75. all detail aliases return the same typed action hub, including exact method/path/identity/enabled/disabled truth and bounded `RETRY_FINALISATION`, without restoring general public `FINALISE`.

## Remaining delivery sequence after independent GO

1. independently audit the exact runtime/evidence commits, installed TEST repeatable hashes, deployed TEST service versions and latest-only handover manifest recorded after this revision's rollout;
2. keep Candidate feature flags false until the coordinated synthetic TEST fixtures and current CloudTMS frontend are ready;
3. after independent backend GO, have the receiving chat produce a highly detailed sequenced UI implementation plan from the complete current-decisions PDF and this authority contract: the CloudTMS office-frontend phase must cover the incomplete-claim-to-MANUAL confirmation and the shared W01–W14 renderer; the later Candidate web/iOS/Android phase must cover Current/History, exact week labels/order and detail navigation/action hub. Implement the office frontend first, and do not begin Candidate clients until the office frontend is complete and separately accepted;
4. independently verify and freeze the OpenAPI contract after frontend acceptance;
5. build the responsive Candidate web/iOS/Android clients against that frozen contract;
6. configure APNs/FCM/Web Push application identities, provider credentials and TEST devices, then activate broker provider delivery;
7. run end-to-end synthetic TEST acceptance before any wider enablement.

## Safety and provenance

- Database mutation or migration: explicitly authorised TEST safe-migration workflow `31495500073` installed the two changed repeatables from runtime commit `a14b60b734560fc5ddf0109bbef6e21eb46e4857`. Workflow `31495500205` proved all 27 Candidate suites on PostgreSQL 17.6/18.1. Installed hashes match the LF-normalised package sources. No Candidate business data was created or changed.
- Candidate/manager workflow mutation: none.
- R2 business document/evidence write/delete: none. Broker readiness used the designed one-use private-service nonce and the private scheduler may purge expired nonce objects; no Candidate workflow object was created.
- Email or push sent: none.
- Normal TEST Worker deployed: yes, version `14554299-dcb6-4102-8ef9-7316d34b3654`, explicitly authorised; TEST only; exact current remote source.
- Private/broker Workers deployed: yes, versions `407596b9-5dd4-48fa-939a-4a9b2885ef74` and `7dfc34ec-b78f-48ca-8b0d-ebd847cfcebc`, explicitly authorised; TEST only.
- Production accessed or deployed: no.
- Commit/push: Candidate runtime commit `a14b60b734560fc5ddf0109bbef6e21eb46e4857` was published non-forcibly directly to `origin/test` after clean lane checks. It is based on and preserves the then-current remote parent `ed9ce72dbcb3423e0cc20225d982301523d84377`; this work changed no Banking Pay file.
- Deployment note: immediately before deployment, `origin/test` still equalled the exact tested runtime and GitHub reported zero queued/in-progress workflows. All three TEST Workers were rebuilt from that source. No production or unrelated Worker was deployed.
- Secrets printed or packaged: no.
- Banking Pay/Policy X code changed by this correction: no.

The handover package must be treated as audit/merge evidence. It must not be copied over a later worktree without a current overlap/provenance check.
