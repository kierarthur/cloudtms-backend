# CloudTMS Candidate App - Phase 2 and Phase 1B R8 Handover

## 1. Purpose

This is the self-contained independent-review handover for Candidate Daily Phase 2 and Phase 1B R8. It is written for a reviewer with no prior chat context.

The implementation completes the next dependency-controlled package after independently accepted Phase 0 R5 and corrected Phase 1A R7:

- Phase 2: additive Daily database/schema/RPC authority;
- Phase 1B: public broker -> private Candidate Worker -> installed RPC integration.

It does not claim that the full Candidate App is finished. Google coexistence, Candidate Daily UI, controlled cutover, specialist end-to-end acceptance and gradual rollout remain Phases 3–7.

Requested verdict:

> GO or one bounded evidence-backed NO-GO for Phase 2 and Phase 1B R8. A GO may release Phase 3 implementation planning/work, but does not enable Candidate features, authorise real Candidate data/effects, complete the app UI, deploy production or retire the legacy browser.

## 2. Controlling authority order

1. Accepted Candidate core decisions/API/Office authority remains unchanged.
2. Accepted Phase 0 R5 documents, merged contract and AV-001–AV-154 remain controlling.
3. The authorised Google Evidence Gate and certified source hashes remain the source/deployment evidence authority.
4. Corrected Phase 1A R7 and AV-155–AV-192 remain the transport authority.
5. R8 source, merged R8 OpenAPI, current PDF Sections 70–79 and AV-193–AV-228 are later-controlling for installed Phase 2/1B facts.
6. Historical R6/R7 current-state documents describe those phases and do not supersede R8 current state.

## 3. What has been implemented

### 3.1 Phase 2 schema

One migration adds exactly twelve tables:

```text
private.candidate_daily_authority_scopes
private.candidate_daily_entitlements
private.candidate_daily_source_links
public.candidate_daily_command_receipts
private.candidate_daily_batch_receipts
public.candidate_daily_rota_generations
public.candidate_daily_rota_days
public.candidate_daily_availability_days
public.candidate_daily_sheet_projection_outbox
private.candidate_daily_sync_state
private.candidate_daily_authority_transitions
private.candidate_daily_external_effect_receipts
```

The same migration adds `candidate_daily_enabled=false` to the existing Candidate feature object if the key does not already exist. It creates no entitlement/source/generation/business data.

Every table is RLS-enabled and has no direct DML grant to `anon`, `authenticated` or `service_role`. Transition history is immutable. The design stores bounded hashes/identities, not raw passwords, CloudTMS tokens, legacy browser sessions or Apps Script secrets.

### 3.2 Phase 2 RPCs

One repeatable installs exactly thirteen service-role-only functions:

```text
candidate_daily_legacy_availability_apply_atomic_v1
candidate_daily_legacy_availability_status_get_v1
candidate_daily_rota_generation_publish_atomic_v1
candidate_daily_projection_claim_v1
candidate_daily_projection_complete_atomic_v1
candidate_daily_sync_status_get_v1
candidate_daily_reconciliation_apply_atomic_v1
candidate_daily_authority_transition_atomic_v1
candidate_daily_external_effect_claim_v1
candidate_daily_external_effect_complete_v1
candidate_daily_external_effect_status_get_v1
candidate_daily_tiles_get_v1
candidate_daily_availability_apply_atomic_v1
```

They own factual request hashes, exact idempotency replay/conflict, source link resolution, generation publication/completeness, Candidate/legacy availability, projection claims/completion, dual cursor state, overlay retreat, three-mode transition and external-effect receipts.

### 3.3 Bootstrap capability

The existing `candidate_app_bootstrap_v1` remains one overload and retains baseline output. It now obtains `capabilities.daily_availability` from the same database helper used by Daily business routes. No frontend or Worker independently infers capability from route type or Electronic timesheet data.

### 3.4 Phase 1B private integration

`broker/src/candidate-daily-phase1b.js` maps the 24 accepted Daily operations to their installed RPC or a closed specialist dependency seam. It:

- derives Candidate identity from the authenticated private session;
- never trusts a request-owned Candidate UUID;
- validates database response shapes;
- distinguishes factual idempotency replay from transport nonce replay;
- composes existing Candidate DAILY action targets rather than creating financial owners;
- exposes no SQL, R2 key, receipt hash, lease token or internal version publicly;
- fails missing specialist integrations as typed unavailable rather than success.

### 3.5 Phase 1B public integration

The public broker preserves the corrected R7 origin, authentication, rate, HMAC, nonce, correlation, framing and error boundary. New success validators rebuild allowlisted responses. Private extra fields, stacks, database text, hashes, internal replay markers and future schema drift are rejected or removed.

Candidate Daily routes require the disabled global flag plus exact entitlement/mode/generation/freshness. Signed Google-system continuity routes do not consult the Candidate flag, but still require service binding, HMAC, timestamp, nonce and database authority.

## 4. Important user decisions preserved

### Minimal legacy change

Do not modernise the temporary Availability browser. It is expected to exist only during proving. Preserve its UI, login and `msisdn` lookup as far as reasonably possible.

Phase 3 may add only a small server-side compatibility adapter inside the existing Apps Script boundary:

```text
legacy browser
 -> existing Apps Script handler
 -> existing legacy identity resolution
 -> approved CloudTMS source link
 -> stable event/idempotency identity
 -> signed server-to-server request
 -> CloudTMS
 -> existing legacy response shape
```

The browser must never see HMAC secrets, Supabase credentials, Candidate access tokens or a canonical Candidate UUID selector.

### Master Rota dual publication

The user deployed the certified NEW MASTER ROTA current Head as active web version 101. Phase 3 must still recheck the effective source immediately before editing.

Master Rota publication to Availability remains necessary during coexistence and after old-browser retirement because Availability/Emergency must know who is working and when. Phase 3 adds signed CloudTMS publication; it does not replace the existing Availability publication merely because the new app exists.

### Emergency and specialist continuity

Emergency, cannot-attend, leave-early, running-late, DNA, acknowledgement, messages, content and Past Shifts must work with both the legacy and new app during coexistence. R8 installs the one durable CloudTMS effect receipt seam. Concrete adapters and end-to-end effects remain Phase 3/6 and cannot be waived.

### Full app scope

This project is the full Candidate App, not only Daily availability. Existing authentication, session, workflows, WEEKLY/DAILY submission, manager approval, QR/electronic, evidence, notifications and Office compatibility remain part of the product. R8 adds the Daily authority without narrowing or rewriting those accepted owners.

## 5. Exact source boundary

Runtime/workflow/test changes from the prior shared backend authority are:

```text
.github/workflows/candidate-db-runtime.yml
.github/workflows/supabase-migrate.yml
broker/src/candidate-app-backend.js
broker/src/candidate-daily-contract-v1.js
broker/src/candidate-daily-phase1b.js
broker/src/candidate-private-worker.js
candidate-broker/src/candidate-broker.js
docs/candidate-app/CANDIDATE_API_OPENAPI_V1_MERGED_R8.yaml
supabase/migrations/17082026_0010_candidate_daily_phase2_authority_schema.sql
supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql
supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql
tests/17082026_0053_candidate_daily_phase2_runtime_verification.sql
tests/candidate-daily-phase1a-contract.test.js
tests/candidate-daily-phase1b-contract.test.js
tests/candidate-daily-phase2-source-contract.test.js
```

R8 documentation/evidence files are additional non-runtime changes. No frontend, Google source, finance, Office Candidate, Banking Pay or James runtime file is in the Candidate diff.

## 6. Publication history

| Event | Identity/result |
| --- | --- |
| Runtime commit | `fad3b82a0e6559854964a3d64b8be527d3492680` |
| Runtime Candidate DB workflow | `31981046790`, success |
| First automatic safe migration | `31981046807`, deliberately cancelled after identifying repeatable-before-new-schema ordering risk; no unsafe completion was accepted |
| Ordering correction commit/current runtime head | `1823403f33fc6e3741c435dce5b2b3a6340db1de` |
| Replacement safe migration | `31981114093`; Candidate schema/repeatable installed, then existing James catalogue mismatch stopped final verification |
| Exact-current-head Candidate DB workflow | `31981494256`, success on PostgreSQL 17.6 and 18.1 |
| Private Candidate deploy | `689bbe95-bf31-4f91-8e5a-40289558cefa` |
| Public Candidate deploy | `18f67f8e-3ca2-46ad-9599-8512894de6c3` |

The migration-order correction is intentional. This repository normally installs repeatables before one-time migrations. The new Daily repeatable depends on the new Daily tables. The workflow now applies only the exact `17082026_0010` prerequisite first, records it in the one-time ledger, then uses the existing repeatable-first runner and skips the already-recorded migration in the later one-time loop. A source-contract test locks that order.

## 7. Safe-migration qualification and shared state

The replacement run proves:

```text
APPLY prerequisite migration: 17082026_0010_candidate_daily_phase2_authority_schema.sql
APPLY new/changed repeatable: 17082026_0015_candidate_daily_phase2_rpcs_v1.sql
```

It then stopped at the Banking Pay targeted fast-route catalogue verifier because TEST deliberately contains separately coordinated James diagnostic definitions ahead of source. The exact mismatches were:

```text
public.pay_preview_candidate_build_canonical_lines
public.pay_workbench_repair_orphaned_pending_source_build
private.pay_workbench_candidate_session_version_rebase_v1
```

The Candidate task was explicitly instructed not to reinstall, revert, catalogue-normalise or classify that state as a Candidate defect. It did not do so. Read-only post-install queries independently prove Candidate ledger hashes, tables, RPC definitions, ACLs, flags and empty rows.

## 8. Installed TEST proof

Target:

```text
Project:      test-cloudtms
Project ID:   yakevhtttcsljosbdpov
Region:       eu-west-2
PostgreSQL:   17.6
Status:       ACTIVE_HEALTHY
```

Installed source hashes:

```text
17082026_0010_candidate_daily_phase2_authority_schema.sql
cdd8446c0a390b89ed324ffa89f1fda11e85f3ffbccfdf1dcf54bbc4e764b226

17082026_0015_candidate_daily_phase2_rpcs_v1.sql
d9297dd73058e71ad01fb96e9460077be2ffc2649acb1b0fadeee615302f668c

07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql
55d6aab7d5e53ea8e81e4617c4740a32b3e23fe4aad3170f2e0b6a4e3d2b4153
```

Post-install facts:

- migration ledger contains the exact schema filename;
- repeatable ledger contains both exact repeatable hashes;
- 12/12 Daily tables exist with RLS and zero permissive policies;
- all three roles have no direct DML on every Daily table;
- 13/13 Daily RPCs exist with security-definer and service-role-only execution;
- bootstrap is one service-role-only overload and contains the Daily capability binding;
- 0/13 Candidate flags are enabled; Daily is false;
- all seven Candidate core and twelve Daily tables have zero rows.

## 9. Verification performed

### Local and GitHub

```text
PostgreSQL 17.6: 42 Candidate suites PASS
PostgreSQL 18.1: 42 Candidate suites PASS
Complete backend JavaScript: 605/605 PASS
Focused Phase 1A/1B/2: 35/35 PASS
Merged OpenAPI: PASS, 62 paths
Candidate private Worker dry build: PASS
Candidate public Worker dry build: PASS
```

### Deployed runtime

```text
GET /healthz -> HTTP 200, Candidate broker, TEST
GET /readyz  -> HTTP 200
unsigned GET /candidate-app/v1/daily/tiles -> HTTP 401
```

No valid signed-system business request, Candidate business request, email, push, R2 write or external effect was attempted.

## 10. Required independent re-audit

The reviewer must run `R8_INDEPENDENT_REVIEW_BRIEF.md`, including:

- manifest verification;
- source and duplicate-owner review;
- PostgreSQL 17.6/18.1 execution;
- source link, receipt replay/conflict and concurrency;
- complete/partial/stale generation;
- projection lease, overlay, cursor retreat and requeue;
- transition/rollback state machine;
- effect claim/complete/status/lost-response;
- Candidate flag/entitlement/mode/generation/freshness conjunction;
- signed-system continuity independence;
- strict public response reconstruction;
- read-only installed TEST ledger/ACL/flags/row counts;
- legacy minimal-change and Master/Emergency lifecycle decisions.

## 11. No-change/safety record

```text
Candidate flags enabled:                 0 of 13
Candidate core business rows:            0
Candidate Daily rows:                    0
Candidate Daily entitlements/source links: 0
Candidate emails/pushes/R2 writes:       0
External effects:                        0
Google source/deployment edits:           0
Normal TEST Worker deploys:               0
Frontend/Pages deploys:                   0
Production changes:                       0
Finance/Banking Pay/Policy X changes:      0
Secrets printed or packaged:              0
```

## 12. Remaining phases to full completion

### Phase 3 - Google coexistence

Freshly re-read/hashes both effective Google projects; minimally modify Availability server Apps Script and Master Rota publication; preserve the browser; add signed CloudTMS generation/availability/projection/effect calls; prove outage/recovery and dual publication. Google writes/deployments require their own explicit authority at execution time.

### Phase 4 - Candidate App Daily UI and shadow parity

Build the complete Daily experience across responsive browser, iOS and Android from the same generated contract, including tiles, availability, booked actions, specialist journeys and accessibility/safe retry. Shadow-compare new app, CloudTMS DB, legacy facade and Sheet projection.

### Phase 5 - controlled TEST authority cutover

Meet identity, freshness, parity, soak and error budgets; reconcile/drain; rehearse rollback; then explicitly enable selected TEST flag/entitlement cohorts.

### Phase 6 - full specialist/workflow acceptance

Prove every Emergency/specialist journey through both paths and prove Candidate DAILY signing/EMAIL/PHONE against existing Office/financial authorities. No legacy capability is silently waived.

### Phase 7 - gradual rollout and retirement

Expand cohorts under monitoring. Only after successful proving, separately approve legacy-browser/compatibility-adapter retirement. Availability, Emergency, Master Rota and specialist owners continue until individually migrated and accepted.

## 13. Final R8 disposition requested

Phase 2 and Phase 1B are implemented, installed, deployed disabled and ready for independent review. They are not self-approved.

The correct next action is independent R8 review. If it returns GO, proceed to Phase 3—not directly to feature activation or frontend rollout.
