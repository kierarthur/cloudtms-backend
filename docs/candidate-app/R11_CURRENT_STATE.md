# Candidate Daily Phase 3 R11 current state

Date: 17 August 2026

Environment: TEST preparation only

## Disposition

Phase 3 source implementation is complete and ready for independent review. It has not been pasted into, saved in, versioned in or deployed from either live Google Apps Script project.

The controlling disabled-state rule is:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED missing or false
-> certified legacy Availability and Master Rota behaviour only
-> no CloudTMS bridge request, retry, state, log or Sheet write
```

Only the case-insensitive value `true` enables the additive server-to-server bridge.

The Phase 3 configuration is supplied through **Google Apps Script Project Script Properties**, not JavaScript declarations in `Code.gs`. Both projects require `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED`, `CLOUDTMS_CANDIDATE_BASE_URL`, `CLOUDTMS_CANDIDATE_ENVIRONMENT`, `CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID`, `CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET` and `CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET`. Availability API additionally requires `CLOUDTMS_CANDIDATE_EXECUTOR_ID`. The installation runbook and `phase3-apps-script/SCRIPT_PROPERTIES.md` contain the exact safe TEST template; secret values are never embedded in source or evidence.

Read-only Cloudflare control-plane inspection on 17 August 2026 confirmed that deployed TEST public Candidate broker version `09ac826b-d7da-4932-b0ad-a5fe6e194779` and private Candidate Worker version `9d73bbff-5099-4f12-a58d-64cb9dbb4889` have no Candidate Daily Google primary/overlap key ID, signing secret, accepted-key catalogue or source-HMAC secret. This is the correct dark state. A new TEST key ID and two different new secrets must be provisioned only after independent Phase 3 source GO, while the bridge remains false.

## Source authority

| Project | Revised complete source SHA-256 | Helper SHA-256 | Certified rollback SHA-256 |
| --- | --- | --- | --- |
| Availability API | `4113dadcbd4044f222fafd51c78ff5bea8905cc22c8517107ba26866b269d905` | `4bc6cb8eaa77ef21ba98d90b52b0d05cc6363f9e41c800e430d95361869d85f9` | `eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f` |
| NEW MASTER ROTA | `6d742f8fac4f9b98630f2afb44d4c2d7c7dc085a0c56c26391c6b921ee70db03` | `58e8da3948f2890b42abd802485776169ff500dedcf060d27b449a60597bcb2c` | `c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8` |

Every `Code.gs` is complete, unredacted and copy/paste-ready. Each rollback file is byte-identical to the user-certified incoming source.

## Accepted Google baseline

- Availability API: active version 215 was previously proven to match Head; no installed triggers.
- NEW MASTER ROTA: the user deployed the supplied current source as active version 101; twelve triggers were recorded, eight enabled and four disabled.
- `ai_startDailyPings` has no declaration or trigger in the certified source. Its single orphan reference is deliberately untouched.
- The Phase 3 package changes no manifest, HTML file, OAuth scope or trigger.

These facts must be re-exported immediately before a later manual Google installation. If the live Head differs from the packaged rollback authority, stop and reconcile rather than overwriting it.

## Enabled behaviour

### Availability reads

The existing tile envelope is built first. Apps Script resolves the established Candidate List public identity, derives an environment-bound non-reversible source HMAC, and calls the signed CloudTMS Worker. The Worker reads the canonical Phase 2 tile authority from TEST Supabase. Canonical date facts are merged into the legacy envelope; legacy-only cohort, welcome and emergency fields remain. Any bridge failure returns the unchanged legacy envelope.

The legacy browser never calls CloudTMS or Supabase directly and receives no CloudTMS access token, candidate UUID or signing secret.

The helper calls the TEST public Candidate broker at `https://test-cloudtms-candidate-broker.kier-88a.workers.dev` using the exact public route family `/candidate-system/v1/google-availability/*`. Source inspection confirms that the public broker forwards that family through its private service binding and that the private Candidate Worker validates the closed Phase 1B request bodies before invoking the installed Phase 2 RPC owners. A real deployed Google-to-broker round trip remains a later controlled proving gate.

### Availability writes

The existing legacy Sheet queue/write completes first and its result remains the browser result. The enabled helper mirrors the same factual date/code change to CloudTMS under one frozen operation identity. Lost-response recovery is status-first, permits one exact retry only after authoritative not-found, and becomes status-only after that retry is consumed.

### Master Rota publication

The existing Availability event remains first. Only an accepted legacy `AVAILABILITY_UPDATE_END` result may be mirrored. The mirror publishes complete, hashed, exactly fourteen-day candidate generations in batches of at most fifty. The same batch, key, correlation and body survive uncertainty.

Master Rota continues feeding Availability during coexistence and after retirement of the temporary legacy browser. The new app does not make Availability or Emergency independent of Master Rota.

### Projection and effects

The package supplies an explicit projection claim/complete adapter that preserves booked and system-blocked overlays. It adds no trigger. Effect claim/complete/status primitives are present, but no provider or Emergency action is rewired or invoked in Phase 3.

## Verification state

```text
Focused Phase 1A/1B/2/3 JavaScript:  48 passed, 0 failed
Complete backend JavaScript:         625 passed, 0 failed
Google live install/deploy:           not performed
Real signed Google-to-Worker calls:   not performed
Candidate feature activation:        not performed
Candidate business-data mutation:    not performed
Production access/deployment:         none
```

## Remaining Phase 3 acceptance gates

1. Independent R11 source and operation-level review.
2. Fresh Google export and exact Head/deployment/trigger comparison.
3. Manual paste/save with the flag false.
4. False-path parity proof for both projects.
5. Controlled TEST signed-route enablement for one approved cohort.
6. Fourteen-day generation, source mapping, exact-replay and outage recovery proof.
7. Projection delivered/deferred-overlay proof.
8. Quota, lock-contention, latency and coexistence soak.
9. Evidence-backed GO before Phase 4.
