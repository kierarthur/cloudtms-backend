# CloudTMS Candidate Daily Phase 3 Apps Script source

Date: 18 August 2026
Environment: TEST preparation only  
Live Google status: **Availability API version 216 and NEW MASTER ROTA version 102 remain active with the bridge false. The inherited R16 Apps Script source is the current copy/paste-ready authority and is unchanged by R17; it has not been installed or deployed. Controlled enablement is not part of this package.**

This directory contains complete, unredacted and copy/paste-ready source for the two existing Google Apps Script projects. It implements the narrow server-side coexistence bridge without redesigning the temporary legacy browser.

## Projects

### Availability API

Copy these two files into the existing Availability API Apps Script project:

```text
availability-api/Code.gs
availability-api/CloudTMSCandidateBridge.gs
```

`Code.gs` is the complete certified Availability Head with the approved Phase 3 return seams plus the R12 post-write queue-flush seam. The helper is a new uniquely named source file.

Exact pre-change rollback source:

```text
availability-api/rollback/Code.gs
```

Certified rollback SHA-256:

```text
eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f
```

### NEW MASTER ROTA

Copy these two files into the existing NEW MASTER ROTA Apps Script project:

```text
master-rota/Code.gs
master-rota/CloudTMSCandidateBridge.gs
```

`Code.gs` is the complete certified Master Head with one additive post-legacy mirror seam only. The helper is a new uniquely named source file.

Exact pre-change rollback source:

```text
master-rota/rollback/Code.gs
```

Certified rollback SHA-256:

```text
c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8
```

## Candidate identity connection

The existing Master Rota `Candidate_ID` authority transforms normalized `Public ID - Credentially` into `CID1-<Crockford Base32 payload><4-character keyed checksum>`. A CloudTMS administrator enters that exact generated CID1 value on the matching Candidate record; the raw Credentially Public ID is not entered in CloudTMS. R15 sends the safe CID1 key, the separate non-reversible source HMAC and source-key version in the first signed generation. PostgreSQL automatically binds the HMAC to that one existing Candidate in the same item transaction as generation. CID1 and the source HMAC remain deliberately different authorities.

## Binding invariant

When the Google Apps Script Project Script Property is missing or false:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED = false
```

the bridge makes no CloudTMS request, performs no bridge retry, writes no bridge operation state, emits no bridge log and performs no bridge-owned Sheet write. Existing legacy return objects remain unchanged. If the new helper file is absent during a partial installation, the guarded `typeof` seams also return the existing legacy result.

These values are added in Project Settings -> Script Properties; they are not JavaScript globals pasted into either `Code.gs` file. Both projects require:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false
CLOUDTMS_CANDIDATE_BASE_URL=https://test-cloudtms-candidate-broker.kier-88a.workers.dev
CLOUDTMS_CANDIDATE_ENVIRONMENT=TEST
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID=<installed TEST key ID>
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET=<installed TEST signing secret>
CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET=<installed, different TEST source-identity secret>
```

Availability API additionally requires:

```text
CLOUDTMS_CANDIDATE_EXECUTOR_ID=availability-api-google-test
```

The operator installed the TEST transport key ID on both Candidate Worker layers and the matching signing secret on the private Candidate Worker, while installing the Google property family in both Apps Script projects. The bridge remains false. Secret values were not read back and are not embedded in this source package.

## R12 Availability correction

- busy/deferred rows produce no CloudTMS operation before durable flush;
- the signed body is derived from the exact `applied:true`, non-deferred legacy result rather than the original request;
- all-rejected results are a true identity/state/log/network no-op;
- queued rows mirror only after successful value/background writes and after the legacy write lock is released;
- response disposition uses a closed route-specific `HTTP/error_code/retry_class` catalogue;
- `STATUS_CHECK`, retryable/uncertain results and malformed 4xx retain the exact operation;
- status not-found permits one exact retry only, after which recovery is status-only.

## R13 Master Rota durability correction

- all generation batches are frozen into quota-safe Script Property chunks before the first POST;
- each manifest binds the exact request byte count and SHA-256;
- each property value is capped at 7,000 UTF-8 bytes and the complete store is preflighted below 500 KB;
- each request stays below both 50 candidates and the 256 KiB route limit;
- `BATCH_IN_PROGRESS`, malformed responses, rate limiting, server failure and transport loss retain the exact body/key/correlation;
- no seven-day expiry can create a replacement identity;
- a later accepted rota event recovers the unresolved event before building anything new;
- multi-batch completion is recorded only after every frozen batch succeeds;
- persisted corruption fails closed and cannot silently create a new event.

## R14 generation outcome contract correction

- HTTP 200 with `ok:true` is not automatically successful;
- the helper validates a UUID batch receipt, exact outcome count and a complete unique in-range index set;
- the frozen batch advances only when every outcome is `COMMITTED` or `REPLAYED`;
- an approved `REJECTED` item produces bounded terminal-rejection logging and never mirror completion;
- malformed, incomplete, duplicate-index, unknown-status and unknown-error aggregates retain the exact frozen operation;
- tests construct the aggregate through the current Worker success-envelope builder, not only top-level error approximations.

## R15 automatic first-generation source linking

- there is no separate manual source-link bootstrap;
- the Master helper uses the certified existing `buildCandidateIdFromPublicId_` authority and never transmits the raw public ID;
- each generation item carries `candidate_global_key`, `candidate_source_hmac` and `source_hmac_key_version=1`;
- PostgreSQL resolves exactly one active existing Candidate by the normalized CID1 value and creates one PRIMARY source link plus the initial `GOOGLE_PRIMARY` scope atomically with generation;
- missing, inactive, duplicate and conflicting mappings reject the indexed item without creating a Candidate or leaving a partial link/scope;
- concurrent first generation converges to one link, scope and generation;
- the bridge-enabled legacy app reads by source HMAC and the new app reads by Candidate UUID, both from the same canonical generation.

There is no candidate-specific runtime allowlist. The product owner chose population-wide TEST enablement; Kier Arthur is the first observational legacy-phone journey only. R15 remains undeployed and both currently installed Google bridges remain false.

## R16 identity-integrity correction

- automatic first-generation binding remains the normal path; there is still no manual source-link bootstrap;
- PostgreSQL owns normalized active CID1 uniqueness and all-history versioned source-HMAC ownership;
- retired, rejected, expired, future-valid and same-Candidate non-current HMAC history cannot be silently rebound or reactivated;
- the private binder is transactionally installed with immediate caller-role revocation;
- Master treats top-level `409 / IDENTITY_LINK_CONFLICT / DO_NOT_RETRY` as terminal, with no mirror-complete log;
- the qualifying new-app read runs through the real reconciliation and authority-transition RPC;
- TEST migration is gated by exact-commit PostgreSQL 17.6 and 18.1 success.

R16 is the copy/paste-ready Google source authority accepted for the R17 publication/install sequence. R17 changes no Google source. It has not been installed in either Google project. Active versions remain Availability 216 and Master 102 and both bridge flags remain false.

## Enabled coexistence behaviour

```text
Legacy browser
  -> existing Apps Script login/msisdn path
  -> existing Sheet/cache response
  -> signed Apps Script-to-CloudTMS Worker request
  -> TEST Supabase canonical authority
  -> canonical tile fields merged into the existing response
```

The legacy browser never receives a CloudTMS credential and never calls Supabase. CloudTMS failure leaves the legacy response intact.

NEW MASTER ROTA continues its existing Availability publication first. Only after that call returns does the enabled bridge mirror a complete 14-day generation to CloudTMS in batches of at most 50 candidates.

All bridge calls use POST to the TEST public Candidate broker under the exact route family `/candidate-system/v1/google-availability/*`. The browser never calls these routes and Apps Script never connects directly to Supabase.

See `SCRIPT_PROPERTIES.md`, the installation runbook and the diagnostic/rollback guide before any manual Google change.
