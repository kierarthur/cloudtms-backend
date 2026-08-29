# CloudTMS Candidate App - Phase 1A R7 Correction Handover

## Purpose and verdict requested

This is the self-contained handover for independent verification of the bounded Candidate Daily Phase 1A R7 correction. It assumes the reviewer knows nothing about the implementation history.

Phase 0 R5 remains the accepted complete Daily/Availability specification. R6 implemented its dark transport and policy seam. The independent R6 audit accepted the architecture and found nine concrete transport defects. R7 closes only those nine defects. It does not begin Phase 2, create Daily business authority, edit Google, enable Candidate features or implement the Candidate Daily UI.

The requested independent disposition is:

> GO or one bounded evidence-backed NO-GO for corrected Phase 1A only. A GO does not authorise Phase 2 SQL installation, Google editing, Candidate UI implementation, feature enablement, production rollout or retirement of the legacy/specialist systems.

## Controlling authority

1. The accepted R5 decisions and merged OpenAPI remain the sole product/API authority.
2. The R6 Google Evidence Gate remains the effective-source authority, with one later operational update: the user deployed the certified current NEW MASTER ROTA Head as active web version 101 on 16 August 2026.
3. The independent R6 report is the defect authority for this correction.
4. Sections 65-69 of the R7 Decisions PDF and AV-181 through AV-192 are later-controlling where they expressly clarify R6.
5. No R7 implementation may change the frozen 25-operation status/error/retry matrix.

## The nine accepted R6 findings and exact R7 closure

| Finding | Failure in R6 | R7 correction | Required proof |
| --- | --- | --- | --- |
| R6-ERR-001 | Daily errors could omit `message` or contain untyped details. | Fixed public message catalogue; every error is closed and has either no details or one route-approved typed variant. | Exhaustive 25-operation matrix and schema tests. |
| R6-RESP-001 | Public broker could forward arbitrary private response fields. | Broker rebuilds allowlisted status/code/retry/correlation/message/details and safe headers only; drift fails to a generic dependency result. | Stack/token/database/future-field leakage and wrong-success tests. |
| R6-ORIGIN-001 | Public policy 403 could become private/dependency 500. | Origin, native-client and preflight/header rejection is final public 403 `FORBIDDEN` / `DO_NOT_RETRY`. | All public 403 paths asserted. |
| R6-RATE-001 | Rotating unverified key IDs could obtain independent signed-system rate buckets. | Every request consumes an IP bucket plus either an accepted key-ID bucket or one shared `invalid-key` bucket. | 150 rotating invalid IDs; no more than 120 reach private HMAC work. |
| R6-NONCE-001 | Cleanup age could be derived from caller timestamp. | Successful nonce consumption stores server `consumed_epoch` and `expires_epoch`; R2 uploaded time is the safe fallback. | 599/600-second boundaries at both +/-300-second timestamp edges. |
| R6-CORR-001 | Missing/invalid signed correlation could produce an invalid empty response correlation. | Missing/invalid input is rejected at the public pre-auth edge with a newly generated valid ULID; a valid signed ULID is preserved. | Missing, malformed and valid tests. |
| R6-FRAME-001 | Candidate `Content-Length` equality was not checked. | If an observable declared length is supplied, Candidate and system paths require exact equality with actual bytes. | Declared-shorter, declared-longer and exact tests. |
| R6-HMAC-QUERY-001 | JavaScript `localeCompare` could disagree with Python/ASCII ordering. | Explicit code-unit/ASCII tuple comparator in production JavaScript and reference Python. | Eight shared Node/Python query vectors. |
| R6-RAW-001 | Raw-header byte-exactness was claimed beyond the Fetch platform boundary. | Contract now states the enforceable Fetch-observable boundary; platform-normalised ambiguous/duplicate values are rejected, while raw-pair vectors remain reference-parser evidence. | Real Request/Headers normalisation test plus documentation review. |

## Public response contract

For every Daily/bootstrap failure, the public edge emits exactly:

```json
{
  "ok": false,
  "error_code": "<closed code>",
  "correlation_id": "<valid ULID>",
  "message": "<fixed safe catalogue message>",
  "retry_class": "DO_NOT_RETRY | RETRY_SAME_KEY | RETRY_NEW_KEY"
}
```

`details` is omitted unless the exact route/status/code permits one closed typed variant: field errors, conflict identity or retry timing. `additionalProperties` is not permitted. Private stack, token, database, storage, diagnostic and unknown future fields never cross the public broker.

Phase 1A has no new Daily business success. An unexpected private success schema therefore fails closed to the route-appropriate generic dependency result rather than being forwarded.

## Rate, replay, framing and HMAC ownership

- Candidate request rates remain 60/min reads, 12/min commands and 6/min external effects.
- Signed-system pre-auth remains 120/min but cannot be partitioned by unverified attacker labels.
- Public broker has no HMAC secret and no replay store.
- Private Worker retains PRIMARY and optional OVERLAP HMAC keys and owns the versioned R2 nonce namespace.
- The signed timestamp is validated at +/-300 seconds but never owns retention age.
- Successful server consumption owns the ten-minute nonce age.
- `Idempotency-Key` remains the sole factual command key and is separate from transport nonce replay.
- Candidate/body ceilings remain 32 KiB/256 KiB.
- Canonical query ordering is explicit and identical in Node and Python.
- The deployed claim is limited to what Cloudflare Fetch Request/Headers actually exposes.

## Google and legacy coexistence boundary

R7 makes no Google edit and performs no Apps Script deployment.

The temporary legacy Availability browser must continue unchanged as far as reasonably possible. Later Phase 3 may add only the smallest server-side Apps Script-to-CloudTMS adapter. The browser must never receive a CloudTMS HMAC key, Supabase authority, Candidate credential or a capability to nominate arbitrary Candidate UUIDs.

The user confirmed that the certified current NEW MASTER ROTA Head is now active web version 101. This removes the historical R6 v100-versus-Head operational difference, but Phase 3 must still re-read and hash-check both Google projects immediately before any authorised edit.

Decommissioning the temporary legacy browser and `LEGACY_COMPAT` facade does not decommission:

- the Availability Sheet and Apps Script service;
- Emergency functions;
- NEW MASTER ROTA generation/working publication;
- signed system synchronisation;
- projection and freshness routes;
- retained specialist services and effect/receipt ownership.

Those owners continue through coexistence and after browser retirement until each is separately migrated, accepted and explicitly retired.

## Changed production boundary

The production source/config boundary is intentionally limited to:

- `broker/src/candidate-daily-contract-v1.js`;
- `broker/src/candidate-daily-hmac-v1.js`;
- `broker/src/candidate-daily-phase1a.js`;
- `broker/src/candidate-app-backend.js`;
- `broker/src/candidate-private-worker.js`;
- `candidate-broker/src/candidate-broker.js`;
- `candidate-broker/wrangler.jsonc`.

Tests, vectors and documentation are included separately. No frontend, SQL migration/repeatable, Candidate table/RPC, Google source, finance, invoice, payment, Banking Pay, Policy X, provider, settlement or remittance owner is changed.

## Publication and deployed TEST authority

| Item | Final identity |
| --- | --- |
| Backend repository/branch | `kierarthur/cloudtms-backend` / `test` |
| R7 implementation commit | `fd7c8c4eee49ccb38848f0ebaa281f81a11a4974` |
| Published backend runtime head | `fd7c8c4eee49ccb38848f0ebaa281f81a11a4974` before the later documentation-only evidence commit |
| Candidate DB runtime workflow | `31975585688` - PASS on PostgreSQL 17.6 and PostgreSQL 18.1 |
| Safe migration workflow | `31975584305` - deliberately recorded NO-GO at the post-apply catalogue verifier because TEST already contained exactly three manually installed James read/presentation definitions pending that separate task's publication; both Candidate and Banking Pay source-authority gates passed and R7 introduced no SQL |
| Candidate private Worker | `4cdbcdeb-fc06-4a22-8af0-6876f633e41d` - 100% traffic |
| Candidate public broker | `2bd9023c-b9de-4ae3-b5e2-2c91f96942f9` - 100% traffic |
| Normal TEST Worker | deliberately unchanged unless separately stated |

Candidate routes remain dark after deployment. Deployment is transport publication only; it is not feature enablement.

The safe-migration NO-GO is not a Candidate defect and did not justify reverting or reinstalling the three James functions. The verifier named only:

- `private.pay_workbench_preview_effective_section_v1`;
- `public.pay_workbench_session_get_candidate_preview`;
- `public.pay_workbench_session_get_preview_page`.

Those definitions had been installed and verified manually by the separate James task immediately before R7 publication, while their source/ledger publication was intentionally still pending. R7 did not change SQL, the workflow passed the Candidate authority check, and the independent Candidate PostgreSQL matrix passed on both supported versions. The R7 pack preserves the failed workflow as shared-state evidence instead of misreporting it as a Candidate pass.

Postdeployment public evidence:

- `/healthz` returned HTTP 200 with service `candidate-broker` and environment `TEST`;
- `/readyz` returned HTTP 200 with `ok=true`;
- missing, invalid and duplicate correlation headers returned HTTP 400 `VALIDATION_FAILED`, a valid server-generated response correlation and the fixed public message;
- duplicate signed key-ID input returned the same closed HTTP 400 response;
- rejected browser origin and forbidden preflight-header probes remained HTTP 403 `FORBIDDEN`;
- declared-shorter and exact-length unsigned system probes failed safely as HTTP 400; an over-declared raw request was retained by the HTTP client/framing layer until timeout and did not produce a business response;
- no valid signed-system success or business effect was attempted.

The read-only TEST safety snapshot at `2026-08-16 22:14:45 UTC` confirmed PostgreSQL 17.6, 0/12 enabled Candidate flags, `candidate_electronic_auto_authorise_default=false`, zero rows in all seven Candidate business tables, and zero Phase 2 `candidate_daily%` tables/functions.

## Verification summary

The final raw logs are in `evidence/` in the sealed package. The minimum gates are:

- 21/21 focused Candidate Daily R7 tests;
- 589/589 complete backend JavaScript tests after rebase to the current shared backend authority;
- Node and Python R7 HMAC/query/raw-parser vectors;
- Node and Python source-identity vectors;
- exact runtime/OpenAPI route parity;
- Candidate public, Candidate private and normal TEST Worker dry builds;
- PDF structural and full visual review;
- deployed Candidate public health/readiness and safe raw HTTP rejection probes;
- exact deployed Worker identities;
- confirmation that Candidate flags remain false and no Candidate Daily SQL/data/effect was introduced.

## Independent re-audit instructions

The reviewer must inspect actual production modules and independently reproduce:

1. all 25 operation-specific error matrices;
2. private-body leakage and unexpected-success drift;
3. every browser 403 path;
4. 150 rotating unverified key IDs at the signed-system pre-auth edge;
5. nonce cleanup at 599/600 seconds and both accepted clock-skew edges;
6. missing/invalid/valid signed correlation;
7. declared-shorter/declared-longer/exact body framing;
8. all eight Node/Python query-order vectors;
9. Fetch-normalised duplicate/ambiguous-header rejection;
10. existing R5 HMAC and source-identity vectors;
11. the dark/no-table/no-RPC/no-Google/no-effect boundary;
12. the full decisions and compliance matrix.

If all pass and no supported blocker remains, issue GO for corrected Phase 1A and stop. Do not infer authorisation for Phase 2 or any later phase. If NO-GO remains, return one bounded finding with exact reproduction, affected operation family and complete correction/proof requirement.
