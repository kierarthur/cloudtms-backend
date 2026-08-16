# Candidate Daily Phase 1A Implementation Authority - R7

## Current disposition

Phase 1A source implementation has been corrected for all nine bounded independent R6 findings and is ready for independent R7 review.

It is deliberately **dark**:

- no Daily route is enabled;
- no Candidate is entitled;
- no Daily database table or RPC exists yet;
- no Google adapter has been edited;
- no Candidate Daily UI has been added;
- no Daily business capability is enabled by Worker publication;
- no database, Google, email, push, emergency, rota or external effect was mutated.

The accepted Candidate database/RPC/authentication/Office architecture is preserved. Phase 1A adds only the frozen Daily transport and policy seams required before Phase 2 SQL authority can be authored.

## Source baseline and isolation

| Item | Authority |
| --- | --- |
| Repository | `kierarthur/cloudtms-backend` |
| Baseline branch | `origin/test` |
| Exact baseline commit | `5386d3d2504d86a0366d66f4096a2f7a8912b2e9` |
| Isolated worktree branch | `codex/candidate-daily-phase1a-20260816` |
| Sole merged API contract | `CANDIDATE_API_OPENAPI_V1_MERGED_R5.yaml` |
| Merged API SHA-256 | `1e4362f363e02eda34405f1f7edacdf7db0da8aad2a018cf75a5cd0993f765fa` |
| R5 HMAC fixture SHA-256 | `d82d943b44876466defcb38d324bf737829b1771e36a15a78b1ef6f93f1f0c22` |
| R7 expanded HMAC fixture SHA-256 | `3aabb105d8d6d97bc0b916985e6791c5e356916ec62d07ef7fa6c90d0b805d30` |

The main local backend clone and unrelated user work were not edited. No finance, Banking Pay, Policy X, invoice, payment, settlement, remittance, provider, Office Candidate or legacy Google file is in the Phase 1A diff.

## Files added

### `broker/src/candidate-daily-contract-v1.js`

One closed runtime route and policy catalogue for the 24 new R5 Daily operations. It owns:

- exact methods, paths, operation IDs, route classes and access policies;
- route-specific rate, in-flight, body-size, deadline and idempotency metadata;
- exact path-template matching;
- Candidate correlation ULID validation/generation;
- the sole header idempotency-key rule and rejection of duplicate body keys;
- the four-input Candidate capability policy;
- separate legacy-compatibility and signed-system policies;
- additive bootstrap capability composition;
- stable Daily error envelopes;
- bounded JSON body reading and SHA-256 helpers.

### `broker/src/candidate-daily-hmac-v1.js`

Private Google-system HMAC v1 and replay authority. It owns:

- byte-exact raw target parsing and query normalization;
- strict header, content-length, content-type, body-encoding and BOM rules;
- the exact R5 signed prefix and raw-body composition;
- PRIMARY plus optional OVERLAP key rotation slots;
- ±300-second private-server clock acceptance;
- fixed-length content-digest comparison and WebCrypto HMAC verification;
- exact valid signed correlation requirement;
- route-specific idempotency rules;
- create-if-absent R2 nonce consumption;
- the durable nonce namespace `candidate-daily-google-nonces/v1/{environment}/{key_id}/{timestamp}/{nonce}`;
- ten-minute minimum replay retention and scheduled bounded cleanup;
- generic authentication/dependency failures that do not disclose key or signature detail.

The public broker never receives an HMAC secret and owns no nonce replay store.

### `broker/src/candidate-daily-phase1a.js`

Dark dispatch authority for Candidate and signed-system Daily routes. It:

- composes the disabled Daily capability additively into bootstrap;
- returns `CANDIDATE_DAILY_DISABLED` with `GLOBAL_DISABLED` for authenticated Candidate Daily calls;
- verifies signed Google-system calls in the private Worker before returning the intentionally dark dependency response;
- performs no business, Google, database or external-effect work.

### `tests/candidate-daily-phase1a-contract.test.js`

Executable Phase 1A boundary coverage against production modules rather than test-only duplicates.

### `tests/fixtures/candidate-daily-r5/canonicalization-v1-vectors.json`

The unchanged accepted R5 HMAC/raw-parser corpus, byte-identical to the source handover fixture.

## Existing files changed

### `broker/src/candidate-app-backend.js`

- preserves every existing bootstrap member;
- adds only `capabilities.daily_availability`;
- returns/propagates one Candidate correlation ID;
- recognizes Daily public routes only after normal Candidate access verification;
- sends them to the dark Phase 1A handler.

No existing Candidate operation or business RPC was changed.

### `broker/src/candidate-private-worker.js`

- adds the private `/private/candidate-system/v1` service-binding prefix;
- requires the existing private service authentication before Daily HMAC work;
- strips private service headers before the signed-system verifier;
- dispatches only the closed signed-system catalogue;
- schedules bounded nonce cleanup alongside existing scheduled work.

### `candidate-broker/src/candidate-broker.js`

- forwards signed Google-system bytes without browser credentials, cookies or CORS authority;
- rejects browser `Origin`, `Cookie` and `Authorization` on system routes;
- bounds and forwards exact method/path/query/security headers/raw body;
- applies system rate limiting by key ID;
- validates Candidate Daily content/framing, JSON object shape, 32 KiB limit and idempotency rules;
- applies Candidate rate limiting by public Candidate session;
- applies the exact route deadline to public-to-private service-binding requests;
- generates or preserves Candidate correlation IDs before authority processing;
- never replaces the signed system correlation ID;
- exposes only stable bounded Daily response envelopes and selected safe headers.

Existing auth, workflow, manager and Office routes keep their prior path.

### `candidate-broker/wrangler.jsonc`

Adds four TEST-scoped rate-limit bindings:

| Binding | Namespace | Limit |
| --- | --- | --- |
| `CANDIDATE_DAILY_READ_RATE_LIMIT` | `71005` | 60 / 60 seconds |
| `CANDIDATE_DAILY_COMMAND_RATE_LIMIT` | `71006` | 12 / 60 seconds |
| `CANDIDATE_DAILY_EFFECT_RATE_LIMIT` | `71007` | 6 / 60 seconds |
| `CANDIDATE_DAILY_SYSTEM_RATE_LIMIT` | `71008` | 120 / 60 seconds |

No secret value is committed. HMAC secret/key-ID bindings are intentionally absent from committed TEST configuration at this dark source-only stage.

## Exact route catalogue

### Candidate surface — 11 operations

| Method | Path | Operation | Class | Idempotency |
| --- | --- | --- | --- | --- |
| GET | `/candidate-app/v1/daily/tiles` | `getCandidateDailyTiles` | `CANDIDATE_DAILY_READ` | forbidden |
| PATCH | `/candidate-app/v1/daily/availability` | `applyCandidateDailyAvailability` | `CANDIDATE_DAILY_COMMAND` | required |
| GET | `/candidate-app/v1/daily/past-shifts` | `getCandidateDailyPastShifts` | `CANDIDATE_DAILY_READ` | forbidden |
| GET | `/candidate-app/v1/daily/content/{kind}` | `getCandidateDailyContent` | `CANDIDATE_DAILY_READ` | forbidden |
| GET | `/candidate-app/v1/daily/emergency-window` | `getCandidateDailyEmergencyWindow` | `CANDIDATE_DAILY_READ` | forbidden |
| POST | `/candidate-app/v1/daily/running-late/options` | `getCandidateDailyRunningLateOptions` | `CANDIDATE_DAILY_READ` | forbidden |
| POST | `/candidate-app/v1/daily/running-late/preview` | `previewCandidateDailyRunningLate` | `CANDIDATE_DAILY_READ` | forbidden |
| POST | `/candidate-app/v1/daily/running-late/send` | `sendCandidateDailyRunningLate` | `CANDIDATE_DAILY_COMMAND` | required |
| POST | `/candidate-app/v1/daily/emergencies` | `raiseCandidateDailyEmergency` | `CANDIDATE_DAILY_COMMAND` | required |
| POST | `/candidate-app/v1/daily/message-seen` | `markCandidateDailyMessageSeen` | `CANDIDATE_DAILY_COMMAND` | required |
| GET | `/candidate-app/v1/daily/effects/{effect_key}` | `getCandidateDailyEffectStatus` | `CANDIDATE_DAILY_READ` | forbidden |

### Trusted Google-system surface — 13 operations

| Method | Path | Operation | Class |
| --- | --- | --- | --- |
| POST | `/candidate-system/v1/google-availability/legacy/tiles` | `googleAvailabilityLegacyTiles` | `LEGACY_COMPAT_READ` |
| POST | `/candidate-system/v1/google-availability/legacy/availability` | `googleAvailabilityLegacyApply` | `LEGACY_COMPAT_COMMAND` |
| POST | `/candidate-system/v1/google-availability/legacy/timesheet-authorisation-status` | `googleAvailabilityLegacyTimesheetAuthorisationStatus` | `LEGACY_COMPAT_READ` |
| POST | `/candidate-system/v1/google-availability/rota-generations` | `googleAvailabilityPublishRotaGenerations` | `SIGNED_SYSTEM_COMMAND` |
| POST | `/candidate-system/v1/google-availability/sheet-edits` | `googleAvailabilityApplySheetEdits` | `SIGNED_SYSTEM_COMMAND` |
| POST | `/candidate-system/v1/google-availability/projection/claim` | `googleAvailabilityClaimProjection` | `SIGNED_SYSTEM_COMMAND` |
| POST | `/candidate-system/v1/google-availability/projection/complete` | `googleAvailabilityCompleteProjection` | `SIGNED_SYSTEM_COMMAND` |
| POST | `/candidate-system/v1/google-availability/sync-status` | `googleAvailabilityReadSyncStatus` | `SIGNED_SYSTEM_READ` |
| POST | `/candidate-system/v1/google-availability/reconciliation` | `googleAvailabilityApplyReconciliation` | `SIGNED_SYSTEM_COMMAND` |
| POST | `/candidate-system/v1/google-availability/legacy/availability-status` | `googleAvailabilityLegacyStatus` | `LEGACY_COMPAT_READ` |
| POST | `/candidate-system/v1/google-availability/effects/claim` | `googleAvailabilityEffectClaim` | `SIGNED_SYSTEM_COMMAND` |
| POST | `/candidate-system/v1/google-availability/effects/complete` | `googleAvailabilityEffectComplete` | `SIGNED_SYSTEM_COMMAND` |
| POST | `/candidate-system/v1/google-availability/effects/status` | `googleAvailabilityEffectStatus` | `SIGNED_SYSTEM_READ` |

The existing additive Candidate bootstrap is the 25th Daily-related merged-R5 operation and remains a baseline route, not a new Daily route.

## Policy authority

Four policy names exist and no other eligibility ladder is introduced:

1. `BASELINE_BOOTSTRAP` — preserves accepted bootstrap availability regardless of Daily activation.
2. `CANDIDATE_SURFACE` — requires, in this order, readable authority, global feature enablement, explicit Candidate entitlement, source identity readiness and Daily authority readiness.
3. `LEGACY_COMPAT` — independent of the global Candidate product flag; requires verified signed system transport, consumed nonce, trusted environment, stable operation identity, approved source mapping, compatible authority mode and transition readiness.
4. `SIGNED_SYSTEM_SYNC` — independent of the global Candidate product flag; requires verified signed system transport, consumed nonce, trusted environment, source-scope readiness, compatible authority mode and transition readiness.

Phase 1A hard-codes only the dark facts needed to prove safe unavailability. Phase 2 will own the durable facts. Phase 1B will read those facts; it must not create a second policy calculation.

## Limits and current enforcement

| Class | Rate | In-flight authority | Max body | Deadline |
| --- | --- | --- | --- | --- |
| Candidate read | 60/min/Candidate | 6 | 32 KiB | 12 seconds |
| Candidate command | 12/min/Candidate | 1 | 32 KiB | 10 seconds |
| Candidate external effect | 6/min/Candidate | 1/effect key | 32 KiB | 20 seconds |
| Signed system | 120/min/key ID | 8 | 256 KiB | 10/12/20 seconds by operation |

Rate, body and deadline enforcement exists in the broker source. In-flight cardinality is frozen as route metadata and is not represented as an unsafe isolate-local counter. Durable distributed enforcement belongs to the Phase 2 receipts/leases and Phase 1B integration. Because every operation is dark in Phase 1A, no business execution can currently exceed those cardinalities. Activation is prohibited until the integrated Phase 1B tests prove the durable owner.

## HMAC v1 byte authority

The signed message is exactly:

```text
CLOUDTMS-HMAC-V1\n
METHOD\n
normalized-path\n
normalized-query\n
timestamp\n
nonce\n
lowercase-content-sha256\n
idempotency-key-or-empty\n
correlation-ulid\n
\n
raw-body-bytes
```

Verification order follows R5:

1. method/path/query/header/body/content-type syntax and size;
2. configured PRIMARY/OVERLAP key ID selection;
3. ±300-second timestamp;
4. fixed-length raw-body digest comparison;
5. WebCrypto HMAC verification;
6. atomic create-if-absent nonce consumption;
7. only then JSON object and route/idempotency validation.

Business replay remains a later database receipt concern. A legitimate business retry uses a fresh nonce/signature and the same `Idempotency-Key`.

## R7 correction authority

The independent R6 review accepted the Phase 1A topology and identified nine bounded defects. R7 closes them without changing the merged R5 OpenAPI, its 25 operation/error matrices, the route catalogue, the HMAC signed prefix, any Daily business authority or any later-phase scope.

### Closed error and response boundary

`candidate-daily-contract-v1.js` owns one fixed public message for every frozen Daily error code and a closed route/status/code/retry/details catalogue for all 24 Daily routes plus bootstrap. Public errors require a valid ULID and either omit details or use exactly one typed closed variant. The public broker reconstructs those fields and safe headers; arbitrary private JSON is never forwarded. Nonconforming private failures and unexpected success schemas fail to the route-appropriate generic dependency result.

### Public policy and framing

Origin, native-client and preflight/header-policy failures remain public HTTP 403 `FORBIDDEN` / `DO_NOT_RETRY`. Candidate and signed-system paths compare any observable supplied `Content-Length` to the actual received bytes. The deployed raw-header claim is intentionally limited to the Fetch Request/Headers representation that Cloudflare Workers receives.

### Signed-system pre-auth rate ownership

Every signed-system request consumes one source-IP limiter before private work. It then consumes either a configured accepted PRIMARY/OVERLAP key-ID limiter or one shared invalid-key limiter. Unknown attacker-controlled labels cannot create independent buckets. The public Worker still has no HMAC secret.

### Nonce, correlation and canonical query

Nonce cleanup is owned by the successful server consumption epoch or R2 upload timestamp, never by the caller-signed timestamp. Cleanup retains a nonce at 599 seconds and permits deletion at 600 seconds, including both accepted clock-skew edges. Signed correlation must be a valid ULID to reach private verification; missing/invalid input is rejected with a newly generated valid response ULID. JavaScript and Python canonical query vectors use explicit ASCII/code-unit tuple ordering rather than locale collation.

### Google coexistence clarification

The user deployed the certified current NEW MASTER ROTA Head as active web version 101 on 16 August 2026. Phase 3 must still re-read and hash-check before any Google edit. Retiring the temporary legacy browser and `LEGACY_COMPAT` facade is not authority to retire the Availability Apps Script service, Emergency functions, Master Rota publication, signed synchronisation, projections/freshness or specialist services. Those continue until separately migrated, accepted and explicitly retired.

## Verification result

Final saved-source evidence:

| Gate | Result |
| --- | --- |
| Focused Phase 1A Node TAP | 21 passed, 0 failed |
| Complete backend Node TAP | 584 passed, 0 failed |
| R7 HMAC Node vectors | 3 positive, 2 route-valid, 24 negative, 8 query, 20 raw-parser - PASS |
| R7 HMAC Python vectors | same corpus - PASS |
| R5 source-identity Node vectors | 6 positive, 5 normalization, 2 malformed, 9 negative, 2 bindings — PASS |
| R5 source-identity Python vectors | same corpus — PASS |
| R5 pack validator | 154 decisions, 25 operations, 4 policies, 63 effects, 28 adapters — PASS |
| Runtime route/OpenAPI parity | 24/24 exact; merged API SHA exact — PASS |
| Candidate public Worker dry build | PASS |
| Candidate private Worker dry build | PASS |
| Normal TEST Worker dry build | PASS |
| JavaScript syntax and `git diff --check` | PASS |

The locked npm installation reported inherited dependency advisories. They were not introduced or changed by this bounded implementation, and no broad `npm audit fix` was run because that would alter unrelated dependency authority. The independent reviewer should assess those advisories separately from Phase 1A acceptance.

## Deliberately not done

- no pull request or feature-branch merge workflow;
- no Supabase schema/RPC authoring or installation;
- no TEST database read/write was required for source-only Phase 1A;
- no feature flag or Candidate entitlement change;
- no Google Apps Script edit/deploy/trigger execution;
- no Candidate/Office frontend change;
- no iOS/Android implementation;
- no email, push, WATI, emergency, rota, payment or provider effect;
- no production access.

## Next gates

1. Independent R7 source, deployed transport and handover review.
2. Phase 2 additive SQL/RPC authoring and disposable PostgreSQL 17/18 verification.
3. Separate explicit authority before any TEST SQL installation or mutation.
4. Phase 1B broker-to-RPC integration and real public/private/database concurrency/failure tests.
5. Phase 3 minimal Google coexistence adapter, only after a fresh effective-source hash check and separate Google edit/deploy approval.
6. Phase 4 Candidate Daily UI across web/iOS/Android.
7. Controlled TEST cutover, specialist acceptance, gradual rollout and eventual legacy decommissioning.
