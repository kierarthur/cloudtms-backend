# Candidate Daily Phase 3 implementation authority

Date: 17 August 2026  
Scope: Availability API and NEW MASTER ROTA server-side coexistence source  
Environment authority: TEST preparation only

## 1. Outcome

Phase 3 implements the smallest server-side compatibility bridge required for temporary coexistence:

```text
Legacy browser/UI/login/msisdn model: unchanged
Existing Availability Sheet/cache/emergency behaviour: unchanged
Existing Master Rota publication: unchanged and still first
New authority: signed Apps Script -> CloudTMS Worker requests
CloudTMS canonical storage/read authority: TEST Supabase behind the Worker
```

The corrected R12 source is complete, unredacted and installed in both TEST Google projects. Availability API is deployed as version 216 and NEW MASTER ROTA as version 102. Installation and deployment occurred with `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false`; no signed bridge request or Candidate Daily business-data mutation was authorised. Independent R12 operation-level review and a separately controlled enablement/proving window remain required.

> R12 controlling correction: the R11 Availability write adapter was independently rejected because it mirrored deferred rows, copied rejected rows into mixed commands and deleted non-terminal `STATUS_CHECK` operations. Sections 11-13 below supersede any earlier wording that could permit those outcomes.

## 2. Absolute disabled-state invariant

Only the case-insensitive value `true` enables the bridge. With `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED` missing or false:

- no CloudTMS request occurs;
- no bridge retry or status probe occurs;
- no bridge Script Property is created, changed or deleted;
- no bridge log occurs;
- no bridge-owned Sheet write occurs;
- the existing Availability and Master Rota return objects are unchanged;
- existing cache, login, msisdn, emergency, specialist, message, timesheet and Sheet logic remains the certified legacy logic;
- existing installed triggers remain untouched;
- the orphan `ai_startDailyPings` menu/reference state is not revived, repaired or removed.

Each legacy `Code.gs` seam is also protected by `typeof` so an absent helper during a partial paste cannot break the legacy result.

## 3. Availability tile read

When disabled, the app obtains tiles exactly as before from Master Rota-backed Availability/EmailHistory data and its existing cache.

When enabled:

1. the existing legacy tile envelope is built first;
2. Apps Script resolves the candidate through the existing trusted msisdn path and reads `Public ID - Credentially` from the established Candidate List;
3. it derives a non-reversible source HMAC and sends a signed server-to-server request to the CloudTMS Worker;
4. the Worker independently validates HMAC, correlation, nonce, clock and source-link authority;
5. the Worker reads canonical tiles from TEST Supabase through the frozen Phase 2 RPC authority;
6. Apps Script merges canonical tile fields by date into the existing envelope while retaining legacy-only cohorts, welcome content and emergency presentation fields;
7. any CloudTMS error returns the unchanged legacy envelope.

The browser never calls CloudTMS or Supabase and never receives a CloudTMS session or HMAC secret.

## 4. Availability change dual write

The existing legacy Sheet queue/write remains first and its existing response remains authoritative for the temporary browser.

After that legacy outcome exists, the enabled helper submits the same factual dates/codes to the signed legacy-compatibility route. It does not transmit mobile number, email or raw Credentially ID.

Apps Script persists one operation record under Script Lock for the factual request. It freezes:

- source HMAC;
- normalized changes;
- request UUID;
- idempotency key;
- correlation ID;
- exact-retry-consumed state.

On uncertainty it probes exact status first. An authoritative not-found may consume one exact retry using the same request, body, key and correlation. Continued uncertainty is status-only; a new key is never invented. The operation is removed only after an authoritative terminal result or stable non-uncertain rejection. CloudTMS failure does not change the legacy response.

## 5. Master Rota dual publication

The existing `_postAvailabilityEvent` call remains first. Its exact result is captured, the enabled mirror is invoked only after an accepted 2xx legacy publication, and the exact legacy result is returned. A rejected/uncertain legacy publication never advances CloudTMS generation truth independently.

Only `AVAILABILITY_UPDATE_END` emits a CloudTMS generation. Other existing event calls remain legacy-only. The generation builder:

- reads the existing 14 Availability headers from columns G onward;
- resolves each candidate through `Public ID - Credentially` without transmitting it;
- treats EmailHistory as booked-shift source truth;
- uses existing shift/notes time parsing and rejects an unresolvable booked time instead of inventing one;
- derives stable booking fallback identity only when the existing booking reference is absent;
- derives system-blocked truth from the existing value/background contract;
- hashes every day row and complete source item;
- sends exactly 14 days per candidate;
- chunks at no more than 50 items;
- persists and reuses the same batch/key/body after uncertainty.

Master Rota continues feeding the Availability API during coexistence and after the temporary legacy browser retires. The CloudTMS mirror is additive; it does not replace the Availability publication.

## 6. Projection consumer

`ctmsP3_projectionDrainOnce()` is an explicit callable adapter. Phase 3 adds no trigger.

It:

1. claims up to 50 rows for target `MASTER_AVAILABILITY_SHEET`;
2. maps source HMAC to the trusted Candidate List;
3. resolves the exact Availability row/date;
4. returns `DEFERRED_OVERLAY` without writing when the legacy Sheet is booked or system-blocked;
5. otherwise applies only the existing `_mapWrite` value/background mapping;
6. completes each lease with a semantic observed revision;
7. returns `RETRY` for a missing row/date or bounded local failure.

The adapter never creates a new trigger and is not enabled by this package. Controlled scheduling and soak belong to the later TEST proving gate.

## 7. External-effect receipt adapter

The Availability helper exposes signed claim, complete and status primitives for the existing effect authority:

```text
ctmsP3_effectClaim_
ctmsP3_effectComplete_
ctmsP3_effectStatus_
```

They do not rewire or invoke any emergency/specialist provider by themselves. Concrete running-late, cannot-attend, leave-early, DNA, message and escalation execution remains Phase 6 and requires end-to-end receipt/provider acceptance. This preserves current emergency behaviour while making the bounded server receipt seam available.

## 8. HMAC and request contract

The implementation uses the frozen R5 `CLOUDTMS-HMAC-V1` canonical bytes, UTF-8 JSON, body SHA-256, HMAC-SHA-256, ten-digit timestamp, fresh nonce, ULID correlation ID and caller-owned idempotency key where required.

The Apps Script cross-language implementation passes the frozen UTF-8 vector. Secret values are property-owned, never hard-coded and never returned by configuration diagnostics.

## 9. Diagnostics

Structured logs exist only while enabled and contain only bounded control facts:

```text
system
event
status
error_code
correlation_id
operation_id
duration_ms
```

They contain no mobile number, email, public ID, raw request body, HMAC secret, signature, token or Sheet row payload.

## 10. Unchanged and later-gated boundaries

Unchanged:

- legacy browser/UI/login;
- Availability cache, Sheet and response shapes when disabled;
- all existing triggers;
- Emergency and retained specialist behaviour;
- Office, finance, rates, Process, Authorise, invoice, payments, Banking Pay, Policy X, provider, settlement and remittance;
- Candidate feature flags and entitlements;
- production.

Later-gated:

- live Google source installation/deployment is complete with the bridge false; live enabled transport/projection/effect proving remains gated;
- real TEST source-link bootstrap;
- enabled parity/latency/quota/outage/recovery soak;
- projection scheduling;
- Phase 4 Candidate Daily UI;
- Phase 5 controlled cutover;
- Phase 6 full Emergency/specialist effects;
- Phase 7 rollout and legacy-browser retirement.

## 11. R12 durable accepted-subset authority

The factual authority for an Availability mirror is the completed legacy result, never the unfiltered browser request.

- A row is eligible only when `applied === true` and `deferred !== true`.
- The accepted date must be an exact `YYYY-MM-DD` value and the accepted code must be in the closed legacy catalogue: blank, `N/A`, `LD`, `N` or `LD/N`.
- Duplicate accepted dates or malformed/contradictory legacy results fail open toward the already-established legacy response. They cannot invent a CloudTMS command.
- Zero accepted rows is a true no-op before candidate identity lookup, Script Property mutation, Script Lock, bridge logging or network activity.
- The persisted fingerprint and signed command body contain exactly the durable accepted subset.

The rota-busy branch remains deferred and non-authoritative. It preserves the exact existing queued browser response and performs no CloudTMS work. `_flushPendingWrites()` revalidates the queue, performs the existing value and background writes, records only the exact successful rows, releases the legacy write lock and only then mirrors those written rows. A failed or rejected Sheet row is never mirrored.

## 12. R12 closed response-disposition authority

HTTP status alone is not terminal authority. The adapter consumes a closed route-specific triple:

```text
HTTP status + error_code + retry_class
```

- `2xx` plus `ok:true` is terminal success.
- An approved `DO_NOT_RETRY` or `REFRESH` triple is an explicit terminal rejection, is bounded-logged and may clear the operation.
- `STATUS_CHECK`, `RETRY_SAME_KEY`, `RETRY_AFTER`, transport uncertainty and any malformed/unrecognised 4xx preserve the exact operation and enter status-first recovery.
- Only status-route `404 / NOT_FOUND / DO_NOT_RETRY` is authoritative not-found.
- The first authoritative not-found may consume one exact retry. Once consumed, every later refresh is status-only even if status again reports authoritative not-found.
- Request UUID, idempotency key, correlation ID, source HMAC and factual body never change while outcome is uncertain.

## 13. R12 source-installation and review status

The R12 correction is bounded to the Availability queue/write seam, Availability helper disposition/recovery logic, executable tests and Candidate Daily documentation. Master Rota runtime semantics, Phase 1B Worker routes and signing, Phase 2 database authority, projection/effect adapters, legacy UI/login/msisdn, manifests/scopes/triggers, Emergency, finance, Banking Pay, Policy X and production remain unchanged.

Live source installation retained `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false`. Availability API version 216 and NEW MASTER ROTA version 102 now contain the corrected sources while remaining legacy-only. Saving and versioning created no signed bridge request and no Candidate Daily data mutation. Controlled enablement remains a later, separately reviewed TEST proving gate.
