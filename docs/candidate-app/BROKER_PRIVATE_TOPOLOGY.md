# Candidate broker and private CloudTMS API topology

Status: TEST implementation and deployed topology contract; updated through final runtime namespace and target-or-anchor rejection closure on 10 August 2026.

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

The browser/native client receives only broker-sealed access and refresh envelopes. Those envelopes are audience- and environment-bound and contain the corresponding internal Candidate session material encrypted with different broker secrets. The broker unwraps them only while constructing a signed service-bound request.

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

CloudTMS remains the canonical event ledger: event type, preference category, dedupe identity, deep link and push state are database truth. The public broker owns provider-facing device material: it validates provider type and encrypts the raw APNs/FCM/Web Push token with a broker-only key before the private API persists the ciphertext.

The provider delivery adapter and retry/dead-letter worker belong to the Candidate broker deployment, not the CloudTMS private API. Their final provider credentials, application identifiers and TEST devices cannot be configured until the iOS/Android/web application identities exist. Until then:

- the in-app notification feed is complete and authoritative;
- push registration is encrypted and provider-labelled;
- QR readiness is released only after durable pack readiness;
- no code is permitted to claim that an external push was sent;
- lack of push never changes workflow truth and the app refresh/resume path reads the feed.

The delivery contract to implement with the app identities is: deterministic notification ID as provider collapse/dedupe key; per-device delivery; bounded exponential retry; invalid-token retirement; dead-letter after the approved attempt limit; and acknowledgement back to the canonical notification `push_state`. This provider activation is a coordinated broker/app stage, not a DB/RPC or financial-authority change.

## Current TEST deployment

- Public broker: `test-cloudtms-candidate-broker`, active version `276c54ed-0cb6-4253-b3de-34f5a0aaec92`.
- Private API: `test-cloudtms-candidate-private-api`, active version `37a7f7f9-dcb2-417f-a6e7-5c29b0e2add4`, with `workers_dev = false` and service-binding access only.
- Normal backend: `test-cloudtms-backend`, active version `57b70f56-f76c-40a8-ba6f-e2de116cbc8a`.
- Broker health/readiness: 200/200.
- Normal backend health/readiness: 200/200.
- Direct public Candidate route on normal backend: 404.
- Candidate feature flags remain false and no Candidate accounts or workflow data were created for deployment verification.

The current Candidate runtime correction is published through backend commit `bf96c859c38367e6c12aee7daa694086ba50b104`. TEST and production remain strictly separate; no production resource was accessed or deployed.

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
