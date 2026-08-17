# Candidate Daily Phase 3 R11 independent review brief

## Required disposition

Audit the supplied Phase 3 Apps Script coexistence package at operation level. Do not infer live Google installation or Phase 3 GO merely from source presence or green unit tests.

Issue one of:

- `GO for Phase 3 source installation/proving gate`, if every source and disabled-state requirement is met and no concrete supported blocker remains; or
- one bounded `NO-GO` handover containing the complete affected operation family and reproducible evidence.

Do not broaden into unrelated legacy functions, Candidate core/Office, finance, Invoice, Banking Pay, Policy X, provider execution or production.

## Review inputs

Read in this order:

1. `00_HANDOVER.md`;
2. `01_INDEPENDENT_REVIEW_BRIEF.md`;
3. `02_CURRENT_STATE.md`;
4. `03_VERIFICATION_SUMMARY.md`;
5. the current Decisions PDF;
6. Phase 3 implementation authority and compliance matrix;
7. installation and diagnostic/rollback runbooks;
8. both complete revised Apps Script projects and both certified rollback files;
9. the Phase 3 test and raw TAP evidence;
10. incoming R10 GO artifacts and retained Phase 1A/1B/2 authorities.

Verify the archive manifests before relying on any file.

Treat `phase3-apps-script/SCRIPT_PROPERTIES.md` as the controlling configuration catalogue. Confirm that these are Google Apps Script Project Script Properties rather than source-code globals, that the bridge is initially false, and that the TEST broker origin and TEST environment are exact. The current deployed TEST Workers contain no Candidate Daily Google key family. Verify that the runbook provisions a new non-secret key ID on both broker layers, the matching new signing secret only on the private Candidate Worker and both Google projects, and a different new source-HMAC secret for both Google projects and source-link bootstrap. Confirm no secret value is embedded in packaged source or evidence.

## Mandatory source review

### Disabled path

Prove for both projects that a missing, blank, false or differently cased false flag results in:

```text
no HTTP request
no bridge retry or status probe
no bridge log
no bridge Script Property change
no bridge Sheet write
the exact certified legacy result/shape
```

Inspect every insertion seam, not only the helper entrypoint. Confirm an absent helper file cannot break the certified legacy path during a partial paste.

### Legacy containment

Confirm there is no browser UI, login, token, msisdn, manifest, OAuth-scope, HTML or trigger redesign. The browser must not receive CloudTMS credentials or nominate a Candidate UUID. The only new security boundary is trusted Apps Script to CloudTMS Worker.

### Identity and signing

Independently reproduce the frozen R5 HMAC vector. Verify source identity canonicalisation includes the environment and exact trusted public source identity, uses a separate secret, and never transmits the raw public ID, mobile number or email.

Cross-check all nine supplied helper operations against the exact public broker routes under `/candidate-system/v1/google-availability/*`, the public-to-private prefix translation, the private Phase 1B request validators and the installed Phase 2 RPC owners. Reject any invented Office/private Candidate route family or direct Apps Script-to-Supabase path.

### Availability read

Trace one tile read from legacy envelope creation through signed Worker request, Phase 2 canonical tile response and by-date merge. Confirm failure returns the unmodified legacy envelope and retained cohorts/welcome/emergency fields cannot be lost.

### Availability write

Trace one legacy-success/CloudTMS-success journey, one CloudTMS stable failure and the full uncertain sequence:

```text
execute uncertain
status found

execute uncertain
status not found
one exact retry
continued uncertainty
status-only refresh
```

Assert the request UUID, idempotency key, correlation ID, factual body and source HMAC never change. Confirm no second exact retry exists.

### Master Rota

Prove the existing Availability publication remains first and CloudTMS is not invoked after a non-2xx legacy result. Confirm only update-end mirrors, every generation contains exactly fourteen valid days, batches contain at most fifty candidates, source rows/items are hashed and unresolvable booked time fails closed.

Confirm Master Rota continues feeding Availability/Emergency during coexistence and after temporary legacy-browser retirement.

### Projection and effects

Trace claim/complete handling for one unblocked projection and one booked/system-blocked projection. Booked/system-blocked must return `DEFERRED_OVERLAY` without overwriting the Sheet.

Confirm effect helpers are receipt primitives only: source installation must not invoke running-late, cannot-attend, leave-early, DNA, messaging or any provider.

## Mandatory test reruns

Run:

```text
node --test \
  tests/candidate-daily-phase1a-contract.test.js \
  tests/candidate-daily-phase1b-contract.test.js \
  tests/candidate-daily-phase2-source-contract.test.js \
  tests/candidate-daily-phase3-apps-script.test.js

npm test
```

Required supplied baseline:

```text
Focused: 48/48 pass
Complete: 625/625 pass
```

## Live Google qualification

Source acceptance does not authorise enablement. A later controlled TEST installation must:

1. re-export current Head, deployment/version, manifest, scopes and triggers;
2. prove the pre-install source equals the packaged rollback authority or reconcile it;
3. paste/save with the flag false;
4. prove false-path legacy response, Sheet, cache and trigger parity;
5. version/deploy only after that parity passes;
6. enable one approved TEST cohort and prove signed transport, fourteen-day generation, lost-response recovery, projection deferral, quota, latency and outage recovery;
7. keep all Candidate user flags and entitlements false.

## Stop rule

If source review passes but live Google proving has not occurred, the maximum verdict is GO for the controlled Phase 3 installation/proving gate. It is not GO for Candidate Daily cutover, Phase 4, feature enablement or production.
