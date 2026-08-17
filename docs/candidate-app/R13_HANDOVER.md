# CloudTMS Candidate App - Candidate Daily Phase 3 R13 handover

Date: 17 August 2026

Environment: TEST only

## 1. Executive result

R13 closes the three executable Master Rota durability/quota defects in the independent R12 review while preserving every accepted R10-R12 correction.

The independent review's proposed one-candidate/source-HMAC allowlist is **not** implemented. After reviewing that recommendation, the product owner made a later-controlling decision that the first enabled TEST exercise is population-wide. Kier Arthur is the first observational phone-app journey because the product owner controls that legacy app, but Kier is not hard-coded and is not a technical authority boundary.

The current position remains dormant:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false
```

The corrected Master helper is published in `cloudtms-backend/test` and saved in the editable head of the existing NEW MASTER ROTA Apps Script project. At the product owner's direction it is **not yet deployed** as a new Google web-app version. The active Google deployments remain Availability 216 and Master 102 until an independent R13 GO is issued.

## 2. Incoming findings and exact disposition

| R12 finding | R13 disposition | Evidence |
| --- | --- | --- |
| No technical one-candidate/cohort gate | **SUPERSEDED BY PRODUCT DECISION** | Population-wide TEST enablement is explicitly later-controlling. No raw name, candidate ID, public ID or source HMAC is hard-coded. Two eligible fixture candidates are both emitted. |
| `409 BATCH_IN_PROGRESS / STATUS_CHECK` deletes state | **CLOSED** | Closed response classifier retains the exact frozen manifest/body/key/correlation and never logs completion. |
| Uncertain publication has no recovery owner | **CLOSED** | One ordered pending index owns all frozen batches; pending recovery always runs before a new event; no TTL replacement exists. |
| One Script Property exceeds 9 KB | **CLOSED** | Immutable request bodies are split into UTF-8-safe chunks capped at 7,000 bytes, with manifest hash/size verification and whole-store capacity preflight. |

The future Phase 6 effect-completion idempotency observation remains deferred and unwired. It is not broadened into Phase 3.

## 3. New Master Rota durable owner

One accepted legacy `AVAILABILITY_UPDATE_END` event is frozen into an ordered collection:

```text
CTMS_P3_ROTA_PENDING_INDEX
  -> CTMS_P3_ROTA_MANIFEST_<operation>
       -> CTMS_P3_ROTA_BODY_<operation>_000
       -> CTMS_P3_ROTA_BODY_<operation>_001
       -> ...
```

Each manifest freezes:

- the request body SHA-256 and UTF-8 byte length;
- the exact body chunk order;
- `batch_request_id`;
- idempotency key;
- correlation ID;
- operation/event identity;
- item count and attempt metadata;
- current bounded disposition.

All chunks and manifests for the complete event are capacity-checked and persisted under Script Lock before the first POST. A partial/corrupt state is not treated as a valid command and cannot silently create a replacement identity.

## 4. Quota and route boundaries

R13 applies all three limits simultaneously:

```text
Maximum items per Worker request:       50
Maximum signed request safety ceiling: 245,760 UTF-8 bytes
Maximum Script Property body chunk:      7,000 UTF-8 bytes
Maximum bridge-owned store preflight:  480,000 UTF-8 bytes
```

The Worker route ceiling remains 256 KiB and the Google property limits remain 9 KB/value and 500 KB/store. The lower R13 ceilings deliberately preserve operating margin.

If the complete recoverable command cannot be stored, no POST occurs. The already-completed legacy Master/Availability action remains authoritative.

## 5. Recovery contract

| Exact result | R13 action |
| --- | --- |
| `2xx` and `ok:true` | Mark that exact batch successful and clear its immutable state. |
| `409 / BATCH_IN_PROGRESS / STATUS_CHECK` | Retain and exact-replay later; never log completion. |
| `409 / SOURCE_EVENT_CONFLICT / DO_NOT_RETRY` | Explicit terminal rejection; clear exact state with bounded failure status. |
| `422 / GENERATION_INCOMPLETE / DO_NOT_RETRY` | Explicit terminal rejection; clear exact state with bounded failure status. |
| `429`, `5xx`, transport failure | Retain exact state. |
| Malformed/unknown response | Retain exact state. |
| Stored body missing/hash mismatch | Fail closed; do not POST or create replacement identity. |

When a pending event exists, a later accepted legacy event invokes recovery first. While any batch remains unresolved, no new source timestamp, fingerprint, UUID, key, correlation or body is generated. Overall completion is emitted only once every frozen batch succeeds.

## 6. Population-wide TEST decision and Kier journey

There is no candidate-specific runtime allowlist. When the bridge is later enabled, all otherwise eligible TEST source rows may participate.

Kier Arthur is the named first observational journey only because:

- the TEST database contains one active Kier candidate record; and
- the product owner has Kier's legacy phone app.

Read-only TEST evidence proves that Kier's active Candidate record already contains the exact existing global Candidate key supplied by the product owner. That existing key is the established Candidate-product mapping and is not changed by Phase 3.

It is not, however, the separate Daily source authority used by the signed Google bridge. The installed resolver accepts only a non-reversible HMAC of the Google `Public ID - Credentially`, catalogued as `GOOGLE_CREDENTIALLY_PUBLIC_ID` in `private.candidate_daily_source_links`. Read-only TEST evidence currently shows zero such rows for Kier and zero active TEST rows for that source type overall. Candidate/global-key existence is therefore not proof of Daily source-link readiness. Before enabled proving, the controlled bootstrap must derive, bind and verify exact source links for every eligible row. It must map to the existing Candidate UUID; it must not create a replacement Candidate. No source link or Candidate row was created by R13.

The same rule explicitly covers candidates who never use the legacy browser. An administrator enters their global Candidate key against the canonical CloudTMS Candidate record. Controlled onboarding resolves that key to exactly one existing UUID and binds the derived Google source HMAC to that same UUID. The new app, Master Rota, Availability and Emergency journeys therefore converge on one person record without making legacy-app participation a prerequisite. Duplicate, missing or ambiguous mappings fail closed.

## 7. Source and publication authority

Implementation commit published to `cloudtms-backend/test`:

```text
ccf193eb49d4e022a971d845ce77120f53cd6bb8
Close Candidate Phase 3 generation recovery
```

Parent R12 authority:

```text
6117000635a2f287220e5d20b90ba9e74d5cd8b1
```

The implementation commit changes exactly nine files: the Master helper, one new focused recovery test, the existing Phase 3 test, and six Candidate Daily authority/runbook files. It changes no SQL, Worker runtime, frontend, finance, Banking Pay, Emergency/provider or production source.

## 8. Google authority and deployment gate

### Availability API

```text
Active web-app version: 216
Retained rollback version: 215
R13 source change: none
R13 Google save/deployment: none
```

### NEW MASTER ROTA

```text
Active web-app version: 102
Retained rollback version: 101
R13 helper saved to editable head: yes
R13 helper deployed as a new version: no
```

The product owner explicitly required Google deployment to wait for an independent R13 GO. Saving the helper to the editable head does not change the active web-app version. The bridge property remains false, so no bridge request, log or bridge-owned Script Property operation was executed.

## 9. Verification

```text
Focused Phase 1A/1B/2/3/R13: 73 passed, 0 failed, 0 skipped
R13 Master recovery/quota tests: 19 passed, 0 failed, 0 skipped
Complete backend JavaScript: 651 passed, 0 failed, 0 skipped
Candidate broker Wrangler dry build: PASS
Private Candidate Wrangler dry build: PASS
Public Candidate /healthz: HTTP 200
Public Candidate /readyz: HTTP 200
Real TEST broker invalid-authority probe: HTTP 401 SYSTEM_AUTH_FAILED / DO_NOT_RETRY
```

The R13 matrix proves closed disposition handling, exact replay across repeated uncertainty, no seven-day replacement, multi-batch partial progress, quota-safe persistence/reassembly, store-capacity no-POST, request-byte rechunking, corruption fail-closed behavior, population-wide fixture inclusion and absence of raw identity. A real non-mutating TEST request with a structurally valid route/body and deliberately invalid signing authority also proves the deployed broker parses the contract and fails closed before identity or database mutation. It is not a positive generation-publication test.

## 10. Intentionally not executed

```text
Google web-app version creation/deployment: no (awaiting independent GO)
Bridge enablement: no
Signed Google-to-Worker request: no
Candidate source-link/bootstrap mutation: no
Candidate business-data mutation: no
Projection/effect drain: no
Emergency/provider action: no
Supabase schema/RPC change: no
Worker deployment: no
Frontend deployment: no
Production action: no
```

## 11. Independent disposition requested

The reviewer should issue either:

```text
GO to deploy the R13 Master helper and begin controlled population-wide Phase 3 TEST proving
```

or one bounded NO-GO with independently reproducible evidence.

A GO does not itself enable the bridge. After GO, the sequence is: deploy the Master revision while false, prove false-path parity and trigger/source integrity, bootstrap and verify all eligible source links against the existing Candidate mappings, then separately authorize and observe population-wide TEST enablement with Kier as the first user-visible journey.

## 12. Safety

```text
Secrets printed or packaged: no
Raw candidate/source identity packaged: no
Database mutation: no
Destructive SQL/RPC/action: no
Candidate feature activation: no
Google deployment in R13: no
Finance/Banking Pay/Policy X change: no
Production access/deployment: no
Screenshots packaged: no
Machine-local paths packaged: no
```
