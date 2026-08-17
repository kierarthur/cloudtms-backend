# Candidate Daily Phase 3 Google installation runbook

This runbook records the completed disabled installation and governs the later controlled TEST enablement window. R12 installed and versioned both Google projects while the bridge remained false.

## 1. Preconditions

- Independent review has accepted the source and decisions.
- Current Google Head/deployment/trigger evidence is re-exported and compared with the certified rollback files.
- CloudTMS TEST Worker signed routes are healthy.
- Exact, unambiguous source-link rows exist for every eligible TEST source row that the enabled Master publication will include.
- Every source-link row resolves through the admin-controlled global Candidate key to exactly one pre-existing Candidate UUID. This includes candidates registered only in the new app; legacy-browser use is never a prerequisite. No-match, duplicate or ambiguous global-key mappings stop before enablement.
- Required HMAC key ID/secret and source-HMAC secret are installed in retained-reader catalogues.
- No other Google editor/deployment window is active.
- `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED` is set to `false` in both projects.
- Candidate feature flags and entitlements remain false.

## 1A. Exact Google Script Properties

These are Project Script Properties, not declarations in `Code.gs`.

Both projects:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false
CLOUDTMS_CANDIDATE_BASE_URL=https://test-cloudtms-candidate-broker.kier-88a.workers.dev
CLOUDTMS_CANDIDATE_ENVIRONMENT=TEST
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID=candidate-daily-google-test-v1
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET=<new signing secret generated after Phase 3 source GO>
CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET=<different new source-identity secret generated after Phase 3 source GO>
```

Availability API only:

```text
CLOUDTMS_CANDIDATE_EXECUTOR_ID=availability-api-google-test
```

The operator installed the TEST property family in both Google projects and the corresponding transport key names in the public/private Candidate Workers on 17 August 2026. The bridge remains false. The public broker owns only the non-secret primary key ID; the private Worker owns the matching primary ID and signing secret. Both Google projects own the same ID/signing secret and the same, different source-identity secret. Do not read back, print, commit or place either secret in evidence. The complete ownership rules are in `source/SCRIPT_PROPERTIES.md`.

## 2. Availability API installation

1. Export the live project and save the deployment/version identity. R12 recorded active version 215 before installation and version 216 after installation.
2. Prove its current `Code.gs` equals the package rollback authority after newline normalisation, or stop and reconcile the difference.
3. Add a new script file named `CloudTMSCandidateBridge` and paste the complete Availability helper.
4. Replace `Code.gs` with the complete revised Availability `Code.gs`.
5. Save without changing the manifest, HTML files, deployments or triggers.
6. Add the exact Phase 3 Script Properties above with the bridge flag still false.
7. Run `ctmsP3_configurationStatus()` and record only property presence booleans.
8. With the flag false, execute the normal read-only tiles journey and a separately approved harmless legacy write journey. Compare response shape and Sheet outcome to the pre-install baseline.
9. Confirm no `CANDIDATE_DAILY_PHASE3` log and no `CTMS_P3_OP_` property exists.
10. R12 created web-app version 216 and preserved version 215 for immediate rollback.

## 3. NEW MASTER ROTA installation

1. Export the live project and re-record enabled/disabled triggers.
2. Prove current `Code.gs` begins with the certified package rollback source and account explicitly for the operator-added property setup utility; do not remove that utility.
3. Add a new script file named `CloudTMSCandidateBridge` and paste the complete Master helper.
4. Replace `Code.gs` with the complete revised Master `Code.gs`.
5. Save with the flag false. Do not change the manifest or any trigger.
6. Add the exact common Phase 3 Script Properties above; do not add the Availability-only executor property.
7. Run `ctmsP3_masterConfigurationStatus()` and record presence booleans only.
8. Exercise an approved non-destructive legacy Availability event and prove the exact existing result and downstream Availability update remain unchanged.
9. Re-export trigger inventory and prove exact equality. Confirm there is still no `ai_startDailyPings` declaration/trigger.
10. R12 created web-app version 102 and retained version 101 as the immediate deployment rollback. The operator-added configuration helper was preserved.

## 3A. R12 correction-specific false-path checks

- A busy Availability request must retain the exact deferred browser response and create no CloudTMS operation.
- `_flushPendingWrites()` must mirror only rows whose value/background writes succeeded, and only after releasing the legacy write lock.
- An all-rejected result must perform no candidate identity lookup, state write, bridge log or request.
- Mixed accepted/rejected/deferred results must send only the accepted, non-deferred subset.
- `STATUS_CHECK`, `RETRY_AFTER`, `RETRY_SAME_KEY` and malformed 4xx results must retain the same operation; only approved terminal triples may clear it.
- Repeated authoritative status not-found after the one exact retry is consumed must remain status-only.

## 4. Controlled TEST enablement after R13

Do not enable both projects simultaneously on the first attempt.

1. The product owner has chosen a population-wide TEST bridge gate rather than a candidate-specific allowlist. Confirm all eligible source rows have exact TEST links, then enable Master Rota while Candidate entitlement remains false.
2. Prove at least one legacy-coexistence mapping and one new-app-only mapping: in each case the admin-entered global key resolves to exactly one existing Candidate UUID, the separate Google source HMAC is bound to that UUID, and no duplicate Candidate row is created.
3. Run one update-end event and verify signed HMAC, complete generation receipts, 14 days per candidate, source/day hashes and no duplicate batch. Observe Kier Arthur first using the existing phone app, but do not hard-code or technically restrict the runtime to Kier.
4. Disable Master if any mismatch occurs.
5. Enable Availability for the eligible TEST population.
6. Verify canonical tiles are read through the Worker and merged without losing cohorts/emergency fields.
7. Perform one approved Availability change, deliberately simulate a lost response in a test harness, and prove status-first/same-key recovery.
8. Verify legacy Sheet and CloudTMS parity, latency, UrlFetch quota, lock contention and no sensitive logging.
9. Run projection claim/complete manually for an unblocked row and a booked/system-blocked row. Prove `DELIVERED` versus `DEFERRED_OVERLAY`.
10. Keep effect/provider execution disabled; Phase 6 owns it.

Before enabling, confirm no `CTMS_P3_ROTA_PENDING_INDEX` exists from a prior experiment. During an uncertain Master result, do not delete or edit `CTMS_P3_ROTA_*` properties: the next accepted update must replay the exact frozen event. The bridge has no seven-day automatic replacement.

## 5. Stop conditions

Immediately set the flag false and stop if any of the following occurs:

- legacy response or Sheet behaviour changes while disabled;
- an unexpected trigger appears;
- raw public ID/mobile/email/secret appears in a request or log;
- HMAC, source mapping, generation completeness or status recovery is ambiguous;
- the same factual uncertain request receives a new key;
- booked/system-blocked projection is overwritten;
- legacy Availability publication is skipped;
- feature flag/entitlement becomes enabled;
- any production, finance, payment or provider boundary is reached.

## 6. Required evidence

- before/after Apps Script project exports;
- deployment IDs and versions;
- trigger inventory equality;
- property-name presence only;
- false-path legacy response/result comparison;
- signed request canonical vector and correlation proof;
- generation/day/batch hashes and 14-day completeness;
- exact existing-Candidate binding proof for both a legacy-coexistence candidate and a new-app-only candidate, with no duplicate Candidate creation;
- lost-response one-key recovery;
- projection delivered/deferred cases;
- quota/latency/concurrency soak;
- feature/entitlement disabled snapshot;
- explicit no-production/no-provider/no-financial-mutation statement.
