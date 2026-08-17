# CloudTMS Candidate Daily Phase 3 Apps Script source

Date: 17 August 2026  
Environment: TEST preparation only  
Live Google status: **R12 is installed and deployed with the bridge flag false: Availability API version 216; NEW MASTER ROTA version 102. Controlled enablement is not part of this package.**

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

There is no candidate-specific runtime allowlist. The product owner chose population-wide TEST enablement; Kier Arthur is the first observational legacy-phone journey only. Both bridges remain false in the installed state covered by this package.

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
