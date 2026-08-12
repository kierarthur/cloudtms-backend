# Candidate broker and private CloudTMS API topology

Status: TEST implementation and deployed topology contract; updated through incomplete-expense switch-to-MANUAL confirmation/removal, amendable `REFUSED` source protection, mail-independent PAPER source guarding, source-owner/claim isolation, complete caller retirement, atomic provider-submit permission and shared route/rejection lock order on 11 August 2026.

## Required trust boundary

```text
Candidate iOS / Android / responsive web
Public manager review page
                  |
                  v
       Candidate broker Worker
  public origin, abuse and token boundary
                  |
        Cloudflare service binding
       signed private service request
                  |
                  v
      CloudTMS private API Worker
       no public workers.dev route
                  |
                  v
 Candidate DB/RPC + R2 + mail outbox
 existing financial/lifecycle authorities
```

The two Workers are separate deployment artefacts in the same repository. They have different entry points, bindings, secrets and audiences:

| Artefact | Entry point | Network audience | Main responsibility |
|---|---|---|---|
| Candidate broker | `candidate-broker/src/index.js` | Candidate apps, Candidate web and manager browser | Strict origins, native-client declaration, public access/refresh envelopes, rate limits, safe errors, body limits, device-token encryption and public upload forwarding |
| CloudTMS private Candidate API | `broker/src/candidate-private-worker.js` | Candidate broker service binding only | Service authentication, Candidate DB/RPC composition, official documents, R2, mail outbox and canonical workflow/lifecycle composition |
| Existing CloudTMS Worker | `broker/src/index.js` | Existing CloudTMS frontend/API | Existing office routes, including authenticated `/api/candidate-app/*` adapters only |

The existing CloudTMS Worker no longer dispatches public `/candidate-app/v1` or `/candidate-manager/v1` routes. Its Candidate dependency injection has the `OFFICE` audience. The private Worker uses the `PRIVATE` audience. A missing or incorrect audience returns no Candidate route.

## Service authentication

The broker reaches the private API through the `CLOUDTMS_PRIVATE` service binding. Every request is HMAC-SHA256 signed with a dedicated service secret. The signature binds:

- protocol version;
- HTTP method and complete private path/query;
- TEST/LIVE environment;
- timestamp and nonce;
- SHA-256 of the complete bounded request body;
- SHA-256 of the internal authorisation header;
- SHA-256 of content type, idempotency key, request ID and declared public-client context.

The private Worker rejects an incorrect environment, signature, body, authorisation digest or a timestamp outside the five-minute service window. Mutating business calls retain their existing idempotency keys and database locks. The private Worker has `workers_dev = false` and no public route in its deployment manifest.

## Token and secret separation

The browser/native client receives only broker-sealed access and refresh envelopes. Those envelopes are audience- and environment-bound and contain the corresponding internal Candidate session material encrypted with different broker secrets. New responses use deterministic authenticated v4 envelopes. The issuing key version participates in the domain-separated HMAC identity, distinct per-message AES-GCM key derivation and AES-GCM authenticated data, so changing only the version label always invalidates the credential—even if two configured version slots share secret material. Explicit reader catalogues retain supported random-IV v1 and deterministic v2/v3 envelopes during bounded rollout and may retire a version even while its secret remains bound. Public session UUIDs are stable opaque HMAC-derived identities for the immutable internal session. Before a session or PHONE mutation reaches the private authority, the broker proves that every selected public wrapping secret exists. The broker unwraps a supported version only while constructing a signed service-bound request.

No secret has a fallback to the normal CloudTMS session secret. Required secret names are:

### Candidate broker

- `CANDIDATE_BROKER_ACCESS_TOKEN_SECRET`
- `CANDIDATE_BROKER_REFRESH_TOKEN_SECRET`
- `CANDIDATE_BROKER_DEVICE_TOKEN_SECRET`
- `CANDIDATE_PRIVATE_SERVICE_SECRET`

### CloudTMS private Candidate API

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `CANDIDATE_PRIVATE_SERVICE_SECRET`
- `CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET`
- `CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET`
- `CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET`

TEST and LIVE must use unrelated values. Missing environment or secrets fail closed. Secret values must be set through Worker secret bindings and must never be committed.

## Public browser and native policy

- Browser routes accept only exact origins from `CANDIDATE_ALLOWED_ORIGINS`; wildcard origins are rejected.
- Preflight accepts only the documented methods and headers.
- Manager routes always require an approved browser origin.
- Candidate native requests without an `Origin` header require `CANDIDATE_ALLOW_NATIVE_CLIENTS=true` and `x-cloudtms-client: ios` or `android`.
- Public responses use `no-store`, `nosniff`, `DENY` framing and `no-referrer`.
- Public errors expose a stable safe code and request ID, not private response details.
- Activation/reset challenge start remains enumeration-safe: the public success contract is always `202 accepted`; login failures remain generic.

## Abuse controls

The Candidate broker uses independent Cloudflare rate-limit bindings for:

- all public requests;
- authentication/challenge/refresh requests;
- manager-token requests;
- upload preparation and upload bytes.

Keys are SHA-256 digests of environment plus bounded IP/account/session/token context. If a required rate-limit binding is missing, traffic fails closed. Request bodies are bounded before signing or forwarding: ordinary JSON is 1 MiB; component uploads are 15 MiB.

The initial TEST limits are recorded in `candidate-broker/wrangler.jsonc`. They are configuration values, not economic or lifecycle policy, and must be tuned from TEST telemetry before LIVE.

## Upload and evidence boundary

The public broker accepts bytes only within the bounded route and forwards them through the signed service binding. The private API then:

- verifies the upload ticket, owner, workflow, generation, component and declared media type;
- validates the actual PNG/JPEG/PDF byte signature;
- rejects malformed images and malformed or encrypted PDFs;
- requires exactly one page for an evidence PDF;
- enforces byte, dimension and pixel limits;
- calculates SHA-256 itself;
- refuses duplicate source bytes through the existing database authority;
- stores bytes under a server-generated R2 identity;
- returns no R2 key.

The existing one-component-per-category/page policy remains unchanged.

## Notifications and push ownership

CloudTMS remains the canonical event ledger: event type, preference category, dedupe identity, deep link and push state are database truth. The public broker owns provider-facing device material: it validates provider type and encrypts the raw APNs/FCM/Web Push token with a broker-only versioned key before the private API persists the ciphertext. Storage encryption keeps its random IV and embeds the encryption-key version; that version is bounded to the positive `smallint` range used by the established session column. Separately, a versioned broker HMAC binds environment, provider, public session and raw token. During an approved identity-key rotation the broker supplies proofs for every configured reader version; the private durable receipt selects its originally frozen semantic version before hashing. Randomized ciphertext is excluded from semantic identity, so storage-key drift and supported identity-key drift replay the same result, while a changed token/provider/session conflicts.

Public credential rollout uses v4 envelopes with an authenticated issuing version for access, refresh and PHONE handoff. Each authority has a current writer version and an explicit reader catalogue. An approved rollback uses the same v4-capable reader build with the previous writer version restored while retaining the newer version in the reader catalogue; rollback to a pre-v4 writer build is forbidden after issuance begins. Login, activation and refresh store the chosen public access/refresh/session versions in the durable private result so a lost-response retry reissues the byte-identical public result. Refresh retains the original public-session mapping version even when access or refresh writer keys rotate.

Unauthenticated routing is a closed catalogue limited to challenge start/resend/verify, password completion, login and refresh. Public logout is never included: it unwraps the Candidate access credential and forwards the exact private bearer. Challenge start/resend validate the bounded caller key at the broker, pass through idempotency errors/conflicts and enumeration-mask only eligibility/account-state outcomes. Too-soon and exhausted-allowance resends are durable public 429 results; the same key replays the same throttle. Each accepted or throttled start/resend receipt freezes the challenge-token issuing key version, which the private reader catalogue can reconstruct across forward rotation and approved writer rollback. Unknown-account login also records a durable generic failure without mutating an account. PHONE selection binds public session and optional supplied device digests into the private semantic mutation receipt before creating or wrapping any handoff token.

The provider delivery adapter and retry/dead-letter worker belong to the Candidate broker deployment, not the CloudTMS private API. Their final provider credentials, application identifiers and TEST devices cannot be configured until the iOS/Android/web application identities exist. Until then:

- the in-app notification feed is complete and authoritative;
- push registration is encrypted and provider-labelled;
- QR readiness is released only after durable pack readiness;
- no code is permitted to claim that an external push was sent;
- lack of push never changes workflow truth and the app refresh/resume path reads the feed.

The delivery contract to implement with the app identities is: deterministic notification ID as provider collapse/dedupe key; per-device delivery; bounded exponential retry; invalid-token retirement; dead-letter after the approved attempt limit; and acknowledgement back to the canonical notification `push_state`. This provider activation is a coordinated broker/app stage, not a DB/RPC or financial-authority change.

## Candidate paper-email provider boundary

Candidate PAPER email uses the existing CloudTMS mail delivery authority. The database claim fence and the provider adapter are both required:

- the claimant proves the current `PAPER / AWAITING_PAPER_RETURN` workflow, exact workflow generation, immutable manifest, complete-pack attachment and non-retired outbox binding;
- after claim, immediately before provider submission, `candidate-paper-provider-authority.js` calls service-only `PAPER_PROVIDER_SUBMIT_PERMIT` through the existing workflow RPC;
- that action atomically locks and verifies the live lease token, `QUEUED`/not-sent state, workflow ID, generation, manifest, context timesheet, complete-pack readiness, attachment and non-retired binding;
- the workflow must still be `PAPER / AWAITING_PAPER_RETURN` for that exact generation and manifest, and the exact lease is renewed as the provider-submit permit;
- `PAPER_RETURN`, cancellation, supersession, office rejection and route intervention lock/respect the same outbox barrier, so authority cannot change after permit while the live permit/lease remains active;
- ordinary non-Candidate mail retains its existing provider path.

This permit and the transactional retirement rules are one database coordination boundary. An active permit/lease blocks every supported authority-changing PAPER transition before mutation.

## Candidate manager-email provider boundary

Manager EMAIL outbox rows use exact `MANAGER_APPROVAL_V1` scope rather than an unbound recipient/deterministic key. The claimant and `candidate-manager-provider-authority.js` require the exact request/workflow IDs and generations, mail kind, recipient, current state, non-retired marker and live outbox lease. Immediately before provider submission the adapter calls service-only `MANAGER_PROVIDER_SUBMIT_PERMIT`; cancellation, supersession, expiry, refusal and approval retire the same exact mail set under that lease barrier. Queued mail becomes inert, failed mail remains failed-but-retired, accepted sent history is immutable, and a withdrawal is created only from proved accepted provider history.

Route intervention derives the live PAPER owner from the immutable current-token delivery receipt, not from a historical timesheet workflow pointer. It separately finds every nonterminal PAPER workflow affected by rotating the source, including a draft/submitted workflow that has no mail receipt yet. A distinct or ambiguous affected workflow returns a controlled conflict before any delivery, notification, token, workflow or route change. Source-wide retirement preserves only terminal history, and claim-level cancellation cannot invalidate another claim's live pack.

## Current TEST deployment

- Public broker: `test-cloudtms-candidate-broker`, active version `0e1f7188-f341-4620-b8df-4c5c8703276a`.
- Private API: `test-cloudtms-candidate-private-api`, active version `e6b7d82e-1a89-4303-bfef-a6aca9c8b151`, with `workers_dev = false` and service-binding access only.
- Normal backend: `test-cloudtms-backend`, active version `c25ed724-3d11-4234-b4b4-1b08f305a3d4`.
- Broker health/readiness: 200/200; readiness proves the private service binding and signed private-health request.
- Normal backend health: 200.
- Direct public Candidate route on normal backend: 404.
- Candidate feature flags remain false; all seven Candidate business tables and Candidate-bound mail contain zero rows after deployment verification.

The preceding public-authentication runtime correction was published through backend commit `1df31d2f041bb2e6b9381f39dd97a0f63ae7bcd4`. GitHub workflow `31628119602` proved all 35 Candidate PostgreSQL suites on 17.6 and 18.1, and safe migration workflow `31628119591` installed exact repeatable hashes `ac1f99582254f661fec65d3d69624af0648b0618265cd1b2854c046c550623e7` (authentication) and `0ca52de3d26c069846caa7717a88cc75c6c861bf35e3279a9fd8f4752510bb35` (workflow). Its private Candidate version was `ab5ea859-f68a-41fd-98a0-4c20ed6dac2e`, normal TEST backend version `463e8856-4e78-493e-8d96-742a69963bc1` and public broker version `0f358ca2-4631-42b8-b18b-820fe82f0e69`. That record is retained only as prior rollout history; the later v4 final-correction rollout and its independently reproducible evidence supersede it as current authority. TEST and production remain strictly separate; no production resource is part of this authority.

## Deployment and verification gate

The following gate was applied to the current authorised TEST deployment and remains mandatory for every later deployment:

1. merge only the current saved files and preserve unrelated work;
2. provision the private Worker first with no public route;
3. set private secrets without displaying them;
4. provision the broker service binding and dedicated broker secrets;
5. deploy with every Candidate feature flag still false unless separately approved;
6. verify the normal CloudTMS Worker returns no public Candidate/manager route;
7. verify an unsigned private request is rejected;
8. verify exact TEST origins, rate limits, public token wrapping and no raw storage key;
9. run disabled-state and existing-system regression before any Candidate data mutation;
10. enable one synthetic TEST capability at a time only with explicit approval.

No production deployment is authorised by this document.
