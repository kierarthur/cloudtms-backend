# Candidate Daily Phase 3 implementation authority

Date: 18 August 2026
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

The corrected R12 source is complete, unredacted and installed in both TEST Google projects. Availability API is deployed as version 216 and NEW MASTER ROTA as version 102. R14 corrected aggregate item-outcome handling and received independent GO. R15 adds automatic exact first-generation source binding in repository source only. R15 has not been installed in Google, installed in Supabase, pushed or deployed. Both Google bridge switches remain false and no TEST Candidate Daily business data was changed while this pack was built.

> R12 controlling correction: the R11 Availability write adapter was independently rejected because it mirrored deferred rows, copied rejected rows into mixed commands and deleted non-terminal `STATUS_CHECK` operations. Sections 11-13 below supersede any earlier wording that could permit those outcomes.

## 2. Absolute disabled-state invariant

The disabled invariant remains the immediate rollback proof, but it is not the product purpose of R15. R15 acceptance is the enabled coexistence path serving both the old app and the new app from one canonical generation.

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
- resolves the existing Google-generated `CID1-...` global key and the separate versioned source HMAC from `Public ID - Credentially` without transmitting the raw public ID;
- submits both safe identities in the same signed generation item so PostgreSQL can bind first use without a manual source-link bootstrap;
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
rejection_items
```

`rejection_items` is present only for a terminal Master generation aggregate and contains bounded numeric item indexes plus approved safe error codes. Logs contain no mobile number, email, public ID, raw request body, HMAC secret, signature, token or Sheet row payload.

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

- independent review of the exact R15 source, SQL, Worker contract and evidence;
- R15 repository publication, TEST SQL installation and Candidate Worker deployment after that GO;
- R15 Master source installation and a new disabled Google version only after the repository/runtime authority is current;
- live enabled transport/projection/effect proving remains gated;
- real first-generation automatic source-link proof for an existing Candidate with an exact admin-entered CID1 key;
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

## 12. R12 Availability closed response-disposition authority

HTTP status alone is not terminal authority. The adapter consumes a closed route-specific triple:

```text
HTTP status + error_code + retry_class
```

- For the Availability command/status routes covered by R12, `2xx` plus `ok:true` is terminal success. This statement does not govern the Master aggregate generation route; Sections 14 and 17 govern that route.
- An approved `DO_NOT_RETRY` or `REFRESH` triple is an explicit terminal rejection, is bounded-logged and may clear the operation.
- `STATUS_CHECK`, `RETRY_SAME_KEY`, `RETRY_AFTER`, transport uncertainty and any malformed/unrecognised 4xx preserve the exact operation and enter status-first recovery.
- Only status-route `404 / NOT_FOUND / DO_NOT_RETRY` is authoritative not-found.
- The first authoritative not-found may consume one exact retry. Once consumed, every later refresh is status-only even if status again reports authoritative not-found.
- Request UUID, idempotency key, correlation ID, source HMAC and factual body never change while outcome is uncertain.

## 13. R12 source-installation and review status

The R12 correction is bounded to the Availability queue/write seam, Availability helper disposition/recovery logic, executable tests and Candidate Daily documentation. Master Rota runtime semantics, Phase 1B Worker routes and signing, Phase 2 database authority, projection/effect adapters, legacy UI/login/msisdn, manifests/scopes/triggers, Emergency, finance, Banking Pay, Policy X and production remain unchanged.

Live source installation retained `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false`. Availability API version 216 and NEW MASTER ROTA version 102 now contain the corrected sources while remaining legacy-only. Saving and versioning created no signed bridge request and no Candidate Daily data mutation. Controlled enablement remains a later, separately reviewed TEST proving gate.

## 14. R13 Master Rota durable-generation correction

R13 replaces the unsafe single-value, seven-day Master Rota retry record with one durable event owner. Before the first signed generation request, the helper freezes every batch body, `batch_request_id`, idempotency key and correlation ID for the complete accepted legacy event.

The event is stored as:

```text
CTMS_P3_ROTA_PENDING_INDEX
  -> ordered CTMS_P3_ROTA_MANIFEST_* records
       -> numbered CTMS_P3_ROTA_BODY_* chunks
```

Each body chunk is bounded to 7,000 UTF-8 bytes, below Google's documented 9 KB per-value quota. The helper preflights the whole Script Property store at 480,000 bytes, below the documented 500 KB store quota, before it sends any batch. Each Worker request is also bounded to 50 items and 245,760 UTF-8 bytes, below the frozen 256 KiB route limit.

Every manifest freezes the exact body hash and byte count. Reassembly verifies both before any replay. A missing/corrupt index, manifest or body fails closed: no replacement event is invented and the already-completed legacy action remains unchanged.

Rota result ownership is closed:

- `2xx` plus `ok:true` is only a candidate aggregate result, not automatic batch success;
- the result must contain a structurally valid `batch_receipt_id` and exactly one outcome for every submitted item index;
- indexes must be unique, in range and complete, and every outcome status must be recognised;
- only an aggregate whose every outcome is `COMMITTED` or `REPLAYED` advances that exact frozen batch;
- a known `REJECTED` outcome is an explicit terminal item rejection, records only its bounded index and safe error code, and can never emit mirror completion;
- exact `409 / BATCH_IN_PROGRESS / STATUS_CHECK` retains the batch;
- exact `409 / SOURCE_EVENT_CONFLICT / DO_NOT_RETRY` and `422 / GENERATION_INCOMPLETE / DO_NOT_RETRY` are explicit terminal rejections and never completion;
- transport errors, `429`, `5xx`, malformed or incomplete aggregates, unknown outcome statuses/error codes and every unknown triple retain the batch.

When any pending event exists, a later accepted legacy update first replays that exact event. It cannot generate a new timestamp, body, batch ID, key or correlation ID. The seven-day replacement rule is removed. Overall completion is logged only after every frozen batch succeeds.

## 15. R13 TEST population decision

The product owner explicitly decided that the first enabled TEST exercise is not technically limited to one candidate. `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=true` applies to the normal eligible TEST population. Kier Arthur is the named first observational journey because the product owner has the legacy phone app; Kier is not hard-coded and is not an authority boundary.

Therefore the source contains no candidate allowlist property and no candidate-specific identifier. R15 removes the separate pre-enable source-link bootstrap: each first generation carries its exact Google-generated CID1 key and separate versioned source HMAC. PostgreSQL either binds them to exactly one active existing Candidate or rejects that indexed item. Apps Script never nominates a Candidate UUID.

This later-controlling product decision supersedes earlier wording that described a one-candidate or one-cohort technical gate. It does not enable the bridge, enable Candidate features, change the database, or authorise production.

## 16. Existing global key and automatic first-generation onboarding

The global Candidate key is generated by the certified Master Rota code from the normalized `Public ID - Credentially`: Crockford Base32 encodes the normalized public ID and a four-character HMAC-SHA256 checksum is appended under the `CID1-` prefix. The resulting `CID1-...` value—not the raw Credentially Public ID—is what the CloudTMS administrator enters on the Candidate record. It must resolve to exactly one existing CloudTMS Candidate UUID. That rule applies equally to a candidate who used the temporary legacy app and to a candidate who registers only in the new Candidate App after the legacy browser has been retired.

The CID1 key is a controlled mapping value, not a secret and not the signed Daily runtime identity. R15 transmits that safe key in each frozen Master generation item alongside the separate non-reversible `GOOGLE_CREDENTIALLY_PUBLIC_ID` HMAC and source-key version. The database validates the Google-originated CID1 value against `public.candidates.key_norm`, resolves exactly one active existing Candidate UUID and creates the HMAC source link only if that exact identity has no conflicting owner. It must never create or replace a Candidate row. No match, multiple matches, duplicate HMAC ownership, a different active PRIMARY source or cross-Candidate history fails closed.

This applies identically to a legacy-coexistence candidate and to a new-app-only candidate. The only admin action is entering the already-generated CID1 value on the correct existing Candidate. There is no separate source-link bootstrap or per-candidate allowlist.

## 17. R14 aggregate outcome authority

Independent R13 review proved a bounded integration mismatch: the Worker and installed TEST RPC legitimately return HTTP 200 with `ok:true` while one or more indexed generation outcomes are `REJECTED`. The R13 helper examined only the top-level envelope and could therefore delete the frozen operation and log overall completion after a partial rejection.

R14 makes the current Worker/database aggregate contract controlling. `ctmsP3_masterContractDisposition_` receives the submitted item count and validates the complete aggregate before permitting success:

1. `json.result` must be an object;
2. `batch_receipt_id` must be a structurally valid UUID;
3. `outcomes` must contain exactly the submitted item count;
4. every index must be an integer, unique, in range and collectively complete;
5. every status must be `COMMITTED`, `REPLAYED` or `REJECTED`;
6. only all-`COMMITTED`/`REPLAYED` aggregates are successful;
7. `REJECTED` is terminal only for the installed closed code catalogue: `SOURCE_EVENT_CONFLICT`, `GENERATION_INCOMPLETE`, `IDENTITY_LINK_MISSING`, `IDENTITY_LINK_AMBIGUOUS`, `IDENTITY_LINK_CONFLICT` and `CANDIDATE_DAILY_NOT_READY`;
8. malformed, incomplete, duplicate-index, unknown-status and unknown-error aggregates preserve the exact frozen operation.

Terminal item rejection clears the frozen event through the existing explicit terminal path, emits `ROTA_GENERATION_TERMINAL_REJECTION` with bounded item indexes and safe codes, and never emits `ROTA_GENERATION_MIRROR_COMPLETE`. The existing top-level 409/422 handling remains as defensive compatibility authority.

This is a Master-helper-only correction. Availability source, legacy Master seam, Worker routes, database definitions, source links, Candidate records, feature flags, entitlements, triggers, finance, Banking Pay and production are unchanged. The active Google versions remain Availability 216 and Master 102; R14 must not be copied or deployed until independent GO.

## 18. R15 automatic first-generation source-link authority

R15 makes the first valid Master generation the sole automatic source-link bootstrap. Every generation item contains the exact `candidate_global_key`, `candidate_source_hmac` and `source_hmac_key_version=1` as factual identity inputs covered by the batch request hash.

Inside one indexed item subtransaction PostgreSQL:

1. takes deterministic advisory locks for every global-key and source-HMAC identity in the batch;
2. locks and resolves exactly one active existing Candidate whose normalized `key_norm` equals the supplied CID1 value;
3. creates the missing initial `GOOGLE_PRIMARY` authority scope without enabling an entitlement;
4. creates one PRIMARY `GOOGLE_CREDENTIALLY_PUBLIC_ID` source link if and only if ownership is unambiguous and conflict-free;
5. validates and commits the complete fourteen-day generation;
6. rolls the new scope/link back if that same generation item rejects.

Exact replay returns the same generation and does not add another link. Concurrent different-batch first attempts for the same factual source converge to one link, one scope and one generation. A later Master generation reuses those owners. Missing, inactive, duplicate or conflicting Candidate/source identity returns an indexed terminal rejection and never creates a Candidate.

The resulting canonical generation has two deliberately separate consumers:

```text
Bridge-enabled legacy Availability app
  -> signed source HMAC
  -> the linked Candidate UUID
  -> canonical generation tiles

New Candidate app
  -> authenticated Candidate UUID
  -> the same canonical generation tiles
```

The new Candidate app remains additionally gated by the existing controlled authority transition, entitlement and global feature flag. R15 creates none of those activation permissions. Master continues publishing to the Availability system first even after the temporary legacy phone UI is retired, because Availability and Emergency remain retained consumers until separately replaced.

## 19. R16 identity-integrity and installation-gate authority

R16 preserves automatic first-generation binding but closes every R15 assurance finding before publication or installation.

### 19.1 Normalized active CID1 ownership is a database invariant

The database, rather than the generation RPC alone, owns uniqueness of `upper(btrim(key_norm))` for active valid CID1 Candidate keys. Installation first performs a non-disclosing duplicate preflight and then creates a functional partial unique index. Case variants, leading/trailing-space variants and activation of a normalized duplicate cannot coexist as active owners. Existing noncanonical-but-unique CID1 storage remains resolvable without rewriting Candidate data.

### 19.2 A source HMAC has one Candidate owner across all history

For the tuple `environment + source_system + hmac_key_version + identifier_hmac`, every source-link state and validity period is authoritative history. `PRIMARY`, `OVERLAP`, `RETIRED`, `REJECTED`, expired and future-valid rows all prevent assignment to another Candidate. A same-Candidate non-current historical identity is not silently reactivated by first generation; rotation or repair remains a separately controlled authority.

The invariant is enforced for every insert and identity-changing update by a database trigger using the same deterministic `SOURCE` advisory-lock namespace as first generation, plus an all-history unique lookup index. A conflict leaves no partial scope, link or generation.

### 19.3 Privileged installation is atomic and closed

The R16 first-generation repeatable is one transaction. The private `SECURITY DEFINER` binder is revoked from `PUBLIC`, `anon`, `authenticated` and `service_role` immediately after its definition, before the public RPC replacement is parsed or installed. Final authority is:

```text
private binder: no caller role execute
private history guard: no caller role execute
public generation RPC: service_role execute only
```

The nontransactional `psql -f` runner cannot expose an intermediate committed helper because the complete repeatable commits as one unit.

### 19.4 Terminal identity conflict is closed at both response layers

Master treats both indexed HTTP-200 `REJECTED / IDENTITY_LINK_CONFLICT` and top-level `409 / IDENTITY_LINK_CONFLICT / DO_NOT_RETRY` as terminal rejection. Either path clears only the exact frozen terminal operation, emits bounded safe rejection information, and never emits `ROTA_GENERATION_MIRROR_COMPLETE`.

### 19.5 Dual-consumer proof must execute the accepted gates

The qualifying runtime journey may not manufacture success through direct authority-scope or entitlement writes. It must publish first generation, prove the legacy source-HMAC read, execute the real reconciliation/readiness path, call `candidate_daily_authority_transition_atomic_v1` with independent actor/approver and exact generation/cursors, prove its durable transition and entitlement receipts, then prove the Candidate-UUID read of the same generation. The Candidate read must fail before transition and succeed only afterwards. The entire fixture rolls back.

### 19.6 Database installation is gated by both TEST engines

The Supabase migration job has an explicit dependency on the reusable Candidate database runtime matrix for the same commit. PostgreSQL 17.6 and 18.1 must both pass the exact ordered Candidate migration/repeatable/runtime and concurrency chain before TEST mutation can begin. A separate concurrently running workflow is not a gate.

R16 adds no manual bootstrap, Candidate creation, Candidate identity update, entitlement outside the controlled transition, feature enablement, authority cutover, frontend change, Google trigger change, finance, Banking Pay, payment/provider action or production authority. Availability 216 and Master 102 remain active, both Google bridge flags remain false, and R16 remains an unpublished review candidate until independent GO.

## 20. R17 authority-transition source-identity integration

Independent R16 review accepted every R16 identity-integrity and installation correction but proved that the pre-existing Office authority-transition RPC did not yet compose safely with the new all-history source-identity guard. R17 is the bounded successor authority.

### 20.1 Source-identity conflict is an item result, not a batch abort

`public.candidate_daily_authority_transition_atomic_v1` is a durable multi-item operation. R17 adds `IDENTITY_LINK_CONFLICT` to its closed per-item exception catalogue. A conflicting item now returns `REJECTED / IDENTITY_LINK_CONFLICT` under its original zero-based item index. The batch receipt completes with HTTP authority 200, successful siblings retain their committed results, and exact replay returns the stored terminal body with the same receipt. The function never converts an expected source-identity conflict into an all-or-nothing transaction abort.

This rule is order-independent. It applies to a single conflict, valid-then-conflict and conflict-then-valid batches. A conflict creates no source link, transition or entitlement side effect for the rejected item.

### 20.2 One cross-writer lock hierarchy

R16 generation binding already acquires a deterministic `SOURCE` advisory lock before it locks or creates Candidate authority scope. R17 makes the Office transition writer use the same order:

```text
1. validate the outer batch envelope and durable receipt identity;
2. derive every syntactically safe source-link lock identity;
3. deduplicate and sort those identities;
4. acquire pg_advisory_xact_lock(hashtextextended(identity,0)) for each SOURCE identity;
5. lock Candidate authority scopes in Candidate UUID order;
6. process the batch items under their existing item subtransactions.
```

The lock namespace is exactly `environment + ':SOURCE:' + hmac_key_version + ':' + identifier_hmac`. It is byte-identical to the R16 source-link history trigger namespace. Trigger reacquisition by the same transaction is therefore safe. A generation writer and Office transition can no longer form the previous `SOURCE -> scope` versus `scope -> SOURCE` wait cycle.

Multiple transitions presenting the same source identities in different item orders acquire the same sorted lock sequence. Item order remains the response-index authority; lock order does not reorder results.

### 20.3 Malformed source identities remain per-item validation

The prelock phase considers only a source-link object with a 64-character lower-case hexadecimal identifier HMAC and a positive decimal key-version string that is no longer than ten digits and no greater than the PostgreSQL integer maximum. It performs no integer cast.

Missing, malformed, out-of-range or otherwise invalid source-link values bypass prelocking and reach the existing item validation block, which records `VALIDATION_FAILED` under that item index. A malformed sibling cannot abort or erase another item.

### 20.4 Transactional replacement and caller boundary

R17 installs one complete later repeatable owning only the existing eight-argument public authority-transition function. The file is wrapped in `BEGIN/COMMIT`, retains `SECURITY DEFINER` with an empty search path, revokes `PUBLIC`, `anon` and `authenticated`, and grants execute only to `service_role` when that role exists.

The public signature, context policy, receipt/idempotency rules, Candidate transition rules, reconciliation barriers, independent-approver rule, entitlement ownership and response schema remain unchanged.

### 20.5 Required executable evidence

R17 qualification requires all of the following on PostgreSQL 17.6 and 18.1 before any TEST migration:

1. one conflicting item is durably indexed and exactly replayable;
2. valid-then-conflict preserves the valid sibling;
3. conflict-then-valid preserves the valid sibling;
4. malformed source-link data is indexed as `VALIDATION_FAILED` without a prelock cast error;
5. a transition with no source link retains its established `NO_CHANGE` behaviour;
6. an actual first-generation call and actual authority-transition call using the same Candidate/source/scope complete without SQLSTATE `40P01`;
7. two transition batches presenting the same two source identities in opposite input order complete without deadlock; and
8. the existing complete Candidate runtime and concurrency chain remains green.

AV-325 through AV-333 are later-controlling over R16 wherever authority-transition conflict containment or cross-writer lock order is concerned.

R17 does not change Worker request/response fields, Apps Script, Google projects, Candidate frontend, Office frontend, Candidate creation, ordinary Candidate identity management, Daily feature flags, entitlements outside the existing transition, Emergency, finance, Banking Pay, payments, providers or production. Availability 216 and Master 102 remain active and both Google bridge flags remain false. R17 remains an unpublished, uninstalled review candidate until independent GO.
