# Candidate Daily Phase 3 Google installation runbook

This runbook records the completed disabled R12 installation and governs the later R15 repository/runtime/Google installation and controlled TEST enablement windows. R15 is not yet installed, pushed or deployed.

## 1. Preconditions

- Independent review has accepted the R15 source, SQL, Worker contract and decisions.
- Current Google Head/deployment/trigger evidence is re-exported and compared with the certified rollback files.
- CloudTMS TEST Worker signed routes are healthy.
- The R15 repeatable and Worker schema are installed from the same reviewed commit before any Google enablement.
- Every eligible source row has a Google-generated `CID1-...` key entered by an administrator on exactly one active pre-existing Candidate. This includes candidates registered only in the new app; legacy-browser use and manual source-link prepopulation are never prerequisites. No-match, inactive or duplicate global-key mappings reject only the indexed generation item and stop the enablement exercise for review.
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

## 3B. R14 aggregate-outcome checks before any Master deployment

- A current Worker envelope with every indexed outcome `COMMITTED` must advance the frozen batch.
- A current Worker envelope with every indexed outcome `REPLAYED` must advance the frozen batch.
- Any approved item-level `REJECTED` outcome must emit terminal rejection and no completion.
- Missing, duplicate, out-of-range or wrong-count outcome indexes must preserve the frozen batch.
- Unknown statuses, missing rejection codes and unknown rejection codes must preserve the frozen batch.
- In a two-batch event, first-batch success followed by second-batch rejection must never emit overall completion.
- The active Master version remains 102 until this exact matrix receives independent GO.

## 3C. R15 automatic first-generation checks before Google deployment

- Prove the Master helper emits the exact Google-generated CID1 key, separate source HMAC and `source_hmac_key_version=1`, and never emits the raw public ID.
- Install the reviewed R15 repeatable before the reviewed Worker code; verify the exact function definition and service-role-only ACL.
- Prove one first generation creates exactly one source link, one initial `GOOGLE_PRIMARY` scope and one complete generation in a single item subtransaction.
- Prove invalid generation content rolls back a newly proposed link and scope.
- Prove missing, inactive, duplicate and conflicting identity returns the closed indexed error and creates no Candidate/link/generation residue.
- Prove parallel first generation converges to one source link, one scope and one generation.
- Prove no Candidate entitlement or global Candidate flag is created or enabled.

## 4. Controlled TEST enablement after R15

Do not enable both projects simultaneously on the first attempt.

1. The product owner has chosen a population-wide TEST bridge gate rather than a candidate-specific allowlist. Enable Master Rota while Candidate entitlement and the Candidate global feature remain false.
2. Run one update-end event. For each eligible item, prove Google sends its CID1 key plus separate source HMAC/version and PostgreSQL either reuses or automatically creates the exact source link against one existing Candidate. No manual source-link bootstrap is permitted.
3. Verify complete generation receipts, 14 days per candidate, source/day hashes, one source link per Candidate and no duplicate batch/generation. Observe Kier Arthur first using the existing phone app, but do not hard-code or technically restrict the runtime to Kier.
4. Disable Master if any mismatch occurs.
5. Enable Availability for the eligible TEST population.
6. Verify the enabled legacy app reads the canonical generation by source HMAC and merges it without losing cohorts/emergency fields.
7. Perform one approved Availability change, deliberately simulate a lost response in a test harness, and prove status-first/same-key recovery.
8. Verify legacy Sheet and CloudTMS parity, latency, UrlFetch quota, lock contention and no sensitive logging.
9. Run projection claim/complete manually for an unblocked row and a booked/system-blocked row. Prove `DELIVERED` versus `DEFERRED_OVERLAY`.
10. In a rolled-back or separately approved Candidate-app harness, apply the existing entitlement/global-feature/authority transition and prove the authenticated Candidate UUID reads the same generation. R15 itself must not enable those gates.
11. Keep effect/provider execution disabled; Phase 6 owns it.

Before enabling, confirm no `CTMS_P3_ROTA_PENDING_INDEX` exists from a prior experiment. During an uncertain Master result, do not delete or edit `CTMS_P3_ROTA_*` properties: the next accepted update must replay the exact frozen event. The bridge has no seven-day automatic replacement.

## 5. Stop conditions

Immediately set the flag false and stop if any of the following occurs:

- legacy response or Sheet behaviour changes while disabled;
- an unexpected trigger appears;
- raw public ID/mobile/email/secret appears in a request or log;
- HMAC, source mapping, generation completeness or status recovery is ambiguous;
- a 2xx generation aggregate is missing its receipt, exact item outcomes or complete index authority;
- a generation item is rejected but Apps Script records mirror completion;
- the same factual uncertain request receives a new key;
- booked/system-blocked projection is overwritten;
- legacy Availability publication is skipped;
- feature flag/entitlement becomes enabled;
- any production, finance, payment or provider boundary is reached.

## 5A. R16 database publication and installation gate

Do not publish or install R15. The only acceptable successor is the complete R16 identity-integrity authority.

Before any TEST publication:

1. Rebase the complete R16 change set on the current backend `test` head and repeat the focused/source gates.
2. Prove the new normalized active-CID1 migration finds no duplicate group without printing a CID1 value.
3. Prove the all-history source-HMAC preflight finds no cross-Candidate duplicate without printing an HMAC.
4. Run the exact ordered Candidate database chain on PostgreSQL 17.6 and 18.1.
5. Run first-generation, authority-transition and R16 identity concurrency suites on both engines.
6. Confirm the Supabase migration workflow has `needs: candidate-db-runtime` and that the reusable matrix executes against the exact same commit.

Only after independent GO may the complete R16 commit be pushed. The safe migration must not start until both engine jobs are successful. Stop if either engine fails, if the workflow dependency is absent, or if the migration begins before both jobs are terminal/success.

After a separately authorised TEST install, verify read-only:

```text
normalized active CID1 unique index present
all-history source-HMAC unique index present
history-ownership trigger present and enabled
anon/authenticated/service_role execute private binder: false
anon/authenticated/service_role execute private guard: false
service_role execute public generation RPC: true
Candidate Daily global flag: false
enabled Candidate Daily entitlements: 0
source-link and generation business rows: unchanged unless a later enabled proving window explicitly creates them
```

The Apps Script source remains uninstalled while this database qualification runs. Google installation and false-path qualification require a later separate authorisation after database/Worker parity is proven.

## 5B. R17 authority-transition integration gate

Do not publish or install R16. The acceptable successor is the complete R17 source-identity/authority-transition integration.

Before any repository publication or TEST SQL action:

1. Confirm the R17 later repeatable owns only `public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)`.
2. Confirm the repeatable starts with `BEGIN`, ends with `COMMIT`, retains an empty search path and exposes execute only to `service_role`.
3. Confirm all syntactically safe source-link identities are deduplicated, sorted and locked in the exact R16 `environment:SOURCE:key-version:hmac` namespace before any Candidate scope row lock.
4. Run the single-conflict, valid-then-conflict, conflict-then-valid, malformed-source and no-source runtime fixture.
5. Run the actual generation-versus-transition concurrency journey with the deliberate old-inversion delay and prove no SQLSTATE `40P01`.
6. Run two transition batches with the same two source identities in opposite input order and prove deterministic completion with durable indexed results.
7. Run the complete ordered Candidate chain on PostgreSQL 17.6 and 18.1. Both must include 46 SQL suites and the R17 concurrency test.
8. Confirm safe migration still declares `needs: candidate-db-runtime` for the same commit.

Only after an independent R17 GO may the complete rebased change be published. A successful PostgreSQL 18.1 run alone is not publication/install authority. The exact PostgreSQL 17.6 result remains mandatory because TEST runs that engine family.

After a separately authorised TEST install, read-only verification must prove:

```text
one authority-transition overload with the unchanged signature
SECURITY DEFINER with empty search_path
PUBLIC/anon/authenticated execute: false
service_role execute: true
installed definition equals the reviewed R17 source
Candidate Daily global feature remains false
enabled Candidate Daily entitlements remain zero unless separately authorised
no Google bridge was enabled
no Candidate Daily business row was created by installation
```

R17 requires no Worker, OpenAPI, frontend or Google-source change. Do not deploy or paste those surfaces merely because the SQL correction receives GO.

## 6. Required evidence

- before/after Apps Script project exports;
- deployment IDs and versions;
- trigger inventory equality;
- property-name presence only;
- false-path legacy response/result comparison;
- signed request canonical vector and correlation proof;
- generation/day/batch hashes and 14-day completeness;
- exact automatic first-generation existing-Candidate binding proof for both a legacy-coexistence candidate and a new-app-only candidate, with no duplicate Candidate creation and no manual source-link bootstrap;
- normalized CID1 insert/activation concurrency and all-history HMAC conflict evidence;
- transactional repeatable and final ACL evidence;
- same canonical generation read through both the enabled legacy source-HMAC path and the gated new-app Candidate-UUID path;
- durable transition and entitlement receipts created by the real authority-transition RPC rather than direct fixture writes;
- PostgreSQL 17.6 and 18.1 exact ordered matrix results that precede any TEST migration;
- lost-response one-key recovery;
- projection delivered/deferred cases;
- quota/latency/concurrency soak;
- feature/entitlement disabled snapshot;
- explicit no-production/no-provider/no-financial-mutation statement.
