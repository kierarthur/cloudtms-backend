# CloudTMS Candidate App — broker/private-backend authority closure handover

Date: 10 August 2026
Status: final Candidate broker/private API authority correction published and deployed to TEST for independent API-freeze audit. This document is updated through public-finalisation closure, DB-owned component replay identity, read-only paper-pack delivery, deterministic scheduled release, paper-workflow multiplicity and professional paper documents.

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

No DAILY/WEEKLY financial algorithm, Process, Authorise, route/version, invoice, payment, Banking Pay, Policy X, Google rota/availability or official-timesheet-renderer economics were changed in this pass. One existing Candidate workflow RPC definition was tightened only to return and validate the authoritative prepared-upload contract on idempotent replay.

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
- private database refresh-family rotation, reuse detection and family revocation remain unchanged and authoritative.

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
- `supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql`
- `supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql`
- `supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql`
- `supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_invoice_work_complete_batch.sql`

### New/amended tests

- `tests/candidate-broker-boundary.test.js`
- `tests/candidate-app-backend.test.js`
- `tests/candidate-app-db-rpc-contract.test.cjs`
- `tests/07082026_2155_candidate_app_local_runtime_verification.sql`
- `tests/08082026_1200_candidate_app_expense_workflow_runtime_verification.sql`

### Living contract and plan

- `docs/candidate-app/IMPLEMENTATION_PLAN.md`
- `docs/candidate-app/BACKEND_API_CONTRACT.md`
- `docs/candidate-app/AUTHORITY_MAP.md`
- `docs/candidate-app/BROKER_PRIVATE_TOPOLOGY.md`
- `docs/candidate-app/CANDIDATE_API_OPENAPI_V1.yaml`
- `docs/candidate-app/ROUTE_WARNING_CATALOGUE.md` — unchanged controlling W01–W13 copy

## Verification evidence

| Verification | Result |
|---|---:|
| Changed/new JavaScript syntax | PASS |
| Focused Candidate/backend/broker/DB-contract tests | 75 passed, 0 failed |
| Candidate DB/RPC + canonical DAILY structural tests | 39 passed, 0 failed |
| Complete backend test suite | 400 passed, 0 failed |
| PostgreSQL 17.6 runtime/concurrency suites | 13 passed, 0 failed |
| PostgreSQL 18.1 runtime/concurrency suites | 13 passed, 0 failed |
| Candidate broker Wrangler dry run | PASS |
| Private Candidate API Wrangler dry run | PASS |
| OpenAPI 3.1 lint | PASS |
| Git whitespace/error check | PASS |
| GitHub TEST database migration workflow `31341588838` | PASS |
| Public Candidate broker health/readiness | 200 / 200 |
| Normal TEST backend health/readiness | 200 / 200 |
| Direct public Candidate route on normal backend | 404, as required |

The pre-deployment dry runs and final deployments used repository-installed Wrangler 4.43.0. Active TEST deployment identities at handover generation are:

- normal TEST backend: `2ac68ff7-424d-4b35-adb6-6f9c016a6380`;
- private Candidate API: `1e7bd640-7db2-4276-9e99-e0d4fd043ef3`;
- public Candidate broker: `4c01dff4-43eb-4337-adeb-10f89b1bc090`.

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

## Remaining delivery sequence after independent GO

1. independently audit exact runtime commit `657eb607396411fd9c1f50c1f44f63afdf13824e`, its deployed TEST services, the documentation-only follow-up commit and this handover manifest;
2. keep Candidate feature flags false until the coordinated synthetic TEST fixtures and current CloudTMS frontend are ready;
3. implement the approved CloudTMS frontend changes, including one shared W01–W13 renderer;
4. independently verify and freeze the OpenAPI contract after frontend acceptance;
5. build the responsive Candidate web/iOS/Android clients against that frozen contract;
6. configure APNs/FCM/Web Push application identities, provider credentials and TEST devices, then activate broker provider delivery;
7. run end-to-end synthetic TEST acceptance before any wider enablement.

## Safety and provenance

- Database mutation or migration: the explicitly authorised TEST migration workflow `31341588838` installed the approved latest definitions; no Candidate business data was created or changed.
- Candidate/manager workflow mutation: none.
- R2 write/delete: none.
- Email or push sent: none.
- Normal TEST Worker deployed: yes, explicitly authorised; TEST only.
- Private/broker Worker deployed: yes, explicitly authorised; TEST only.
- Production accessed or deployed: no.
- Commit/push: runtime backend commit `657eb607396411fd9c1f50c1f44f63afdf13824e` pushed directly to `origin/test`; the subsequent plan/evidence update is documentation-only; no PR.
- Secrets printed or packaged: no.
- Banking Pay/Policy X code changed by this correction: no.

The handover package must be treated as audit/merge evidence. It must not be copied over a later worktree without a current overlap/provenance check.
