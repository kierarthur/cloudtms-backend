# CloudTMS Candidate App — Phase 1A R6 Handover

## Purpose of this package

This package is the complete independent-review handover for the Candidate Daily/Availability Phase 1A source implementation. It is self-contained for a reviewer with no prior chat context.

The package contains:

- the complete accepted Phase 0 R5 audit bundle, unchanged under `baseline_r5/`;
- the accepted merged R5 OpenAPI contract and its provenance;
- the complete R5 decision ledger and 154-row compliance matrix;
- the new R6 Google Evidence Gate;
- the complete Phase 1A implementation authority;
- the R6 AV-155–AV-180 decision extension and compliance matrix;
- the rebuilt 70-page Current Decisions PDF;
- every production source/config file changed or added for Phase 1A;
- the exact focused test and accepted HMAC fixture used by the production implementation;
- raw final verification logs;
- a package validator and two manifests.

No screenshot is required to understand or reproduce the source review. No unredacted Apps Script, Script Property value, credential, browser state, personal-data row or secret is included.

## Current verdict requested from the independent reviewer

The implementation author’s disposition is:

> **Phase 1A source authority is ready for independent GO/NO-GO review. It is not an activation, database, Google or full-app GO.**

All new Daily routes are deliberately dark. The Candidate product global flag remains conceptually disabled; no Candidate is entitled; no Daily SQL/RPC authority exists; and no Google adapter or Candidate UI has been created. This is the transport, policy, HMAC, replay-nonce, limit and failure-envelope layer that must exist before Phase 2 database authoring.

## Product context

CloudTMS is implementing the complete Candidate App, not merely a new Daily screen. The accepted phased programme is:

1. **Phase 0 — decisions and executable contract:** complete and independently accepted in R5.
2. **Phase 1A — dark transport/policy seams:** implemented in this R6 package, awaiting independent review.
3. **Phase 2 — additive Daily SQL/RPC authority:** not started; follows independent R6 review. It remains twelve additive tables and thirteen service-role-only RPC owners, with disposable PostgreSQL 17/18 verification before any separate TEST-install decision.
4. **Phase 1B — broker-to-RPC integration:** not started; blocked on Phase 2. It will read the database-owned policy, receipts, leases and authority instead of inventing a second policy layer.
5. **Phase 3 — minimal Google coexistence adapter:** not started. It must preserve the temporary legacy client and add only the smallest trusted Apps Script server-to-server compatibility seam after a new effective-source hash check and separate Google edit/deploy permission.
6. **Phase 4 — Candidate Daily UI:** not started. Web, iOS and Android remain on the same broker contract and must not access Google or Supabase directly.
7. **Later controlled rollout:** TEST-only proving, specialist acceptance, gradual cohorts, separately authorised production steps and eventual explicit legacy decommissioning.

Existing Candidate authentication, timesheet/expense submission, approval, QR, Office and backend authorities remain accepted and were not rewritten by this bounded phase.

## Binding legacy decision

The temporary legacy Availability app must remain fully functional during coexistence and then be decommissioned after the new Candidate App is proven.

The binding rule is:

> **Do not modernise the legacy app. Contain it.**

Phase 1A therefore does not change the legacy browser, login, `msisdn` lookup, Google Sheets behaviour or Apps Script. A later Phase 3 adapter may exist only behind Apps Script:

```text
legacy browser
  -> existing Apps Script behaviour
  -> narrow trusted server-side compatibility adapter
  -> signed CloudTMS system request
  -> one CloudTMS Daily authority
```

The legacy browser must never receive CloudTMS HMAC keys, Supabase authority, Candidate tokens or a capability to nominate arbitrary Candidate UUIDs. Emergency and Master Rota consumers must remain compatible with both the temporary legacy app and the new Candidate App throughout coexistence.

## Google Evidence Gate

The user authorised an authenticated, read-only evidence gate for the Availability API and NEW MASTER ROTA bound Apps Script projects. The gate established:

- each effective current Head and its SHA-256;
- exact equality between each Head `Code.gs` and the corresponding user-certified unredacted attachment;
- deployment identities and the fact that Availability deployment version 215 matches Head;
- the fact that Master Rota web deployment version 100 differs from Head while installed triggers run current Head;
- effective file order, manifest, scopes, trigger inventory, property-name inventory and relevant Sheet topology;
- `ai_startDailyPings` has no declaration or trigger and must not be revived as Candidate Daily work.

The evidence process did not edit or execute Apps Script, read property values, change a Sheet, change a trigger, alter OAuth or deploy anything. Unredacted source stays outside this package. Review `r6_documents/GOOGLE_EVIDENCE_GATE_20260816.md` for the complete record.

## Exact source baseline and working boundary

| Item | Identity |
| --- | --- |
| Repository | `kierarthur/cloudtms-backend` |
| Baseline | `origin/test` |
| Exact baseline commit | `5386d3d2504d86a0366d66f4096a2f7a8912b2e9` |
| Isolated worktree branch | `codex/candidate-daily-phase1a-20260816` |
| Sole merged API | `CANDIDATE_API_OPENAPI_V1_MERGED_R5.yaml` |
| Merged API SHA-256 | `1e4362f363e02eda34405f1f7edacdf7db0da8aad2a018cf75a5cd0993f765fa` |
| Accepted R5 HMAC fixture SHA-256 | `d82d943b44876466defcb38d324bf737829b1771e36a15a78b1ef6f93f1f0c22` |

The main backend clone and unrelated dirty/user files were not edited. This handover has not been committed or pushed.

## Phase 1A implementation

The exact implementation is described in `r6_documents/CANDIDATE_DAILY_PHASE1A_IMPLEMENTATION_AUTHORITY.md`. In summary:

- one closed runtime catalogue owns exactly 11 Candidate Daily and 13 signed Google-system routes;
- existing Candidate bootstrap remains additive and is the 25th merged-R5 Daily-related operation;
- new Candidate capability output is disabled with `GLOBAL_DISABLED` while preserving every existing capability;
- Candidate access remains behind the existing authenticated public/private boundary;
- signed Google-system traffic is rejected when it carries browser Origin, Cookie or Authorization authority;
- the public broker forwards exact signed bytes but owns no HMAC secret or replay store;
- the private Worker verifies the exact R5 HMAC v1 contract, PRIMARY/OVERLAP rotation, ±300-second clock, fixed-work digest check, WebCrypto HMAC and one atomic R2 nonce before JSON/business dispatch;
- reads forbid `Idempotency-Key`; commands require it; duplicate body keys are rejected;
- Candidate/system bodies are bounded to 32 KiB/256 KiB with strict framing, UTF-8 JSON-object and content-type rules;
- Candidate/system rate classes are 60/12/6/120 per minute as settled;
- public-to-private calls use 10/12/20-second route deadlines;
- distributed in-flight ownership is frozen in route metadata but is not falsely implemented with isolate-local counters; Phase 2 receipts/leases and Phase 1B integration must own it before activation;
- every business route remains dark and can perform no database, Google, email, push, emergency, rota or other effect.

## Files in the source change

Added:

```text
broker/src/candidate-daily-contract-v1.js
broker/src/candidate-daily-hmac-v1.js
broker/src/candidate-daily-phase1a.js
tests/candidate-daily-phase1a-contract.test.js
tests/fixtures/candidate-daily-r5/canonicalization-v1-vectors.json
```

Changed:

```text
broker/src/candidate-app-backend.js
broker/src/candidate-private-worker.js
candidate-broker/src/candidate-broker.js
candidate-broker/wrangler.jsonc
docs/candidate-app/IMPLEMENTATION_PLAN.md
docs/candidate-app/AUTHORITY_MAP.md
docs/candidate-app/BACKEND_API_CONTRACT.md
```

R6 documentation/build files are additionally included under `r6_documents/` and `tools/`.

## Verification completed

Final saved-source results:

| Gate | Result |
| --- | --- |
| Focused production-module TAP | 13 passed, 0 failed |
| Complete backend TAP | 576 passed, 0 failed |
| R5 HMAC Node vectors | PASS — 3 positive, 2 route-valid, 24 negative, 5 query, 20 raw-parser |
| R5 HMAC Python vectors | PASS — same corpus |
| Source-identity Node vectors | PASS — 6 positive, 5 normalization, 2 malformed, 9 negative, 2 bindings |
| Source-identity Python vectors | PASS — same corpus |
| R5 pack validator | PASS — 154 decisions, 25 operations, 4 policies, 63 effects, 28 adapters |
| Runtime/OpenAPI parity | PASS — exact 24/24 new routes and exact merged API SHA |
| Candidate public Worker dry build | PASS |
| Candidate private Worker dry build | PASS |
| Normal backend Worker dry build | PASS |
| JavaScript syntax | PASS |
| `git diff --check` | PASS |
| Decisions PDF content check | PASS — 70 nonblank pages |
| Decisions PDF visual check | PASS — all 70 pages, no clipping/overlap/corruption |

The locked dependency installation reported inherited repository advisories (11 high and 1 critical). They were not introduced by Phase 1A and no broad dependency rewrite or `npm audit fix` was performed. They remain a separate repository dependency-maintenance concern.

## Safety and external-state result

```text
Google data/source/configuration mutation: none
Apps Script execution/deployment:           none
Supabase SQL/RPC/schema/data mutation:       none
Candidate feature/entitlement change:       none
Worker deployment:                          none
Frontend deployment/change:                 none
Email/push/emergency/rota effect:            none
Production access/change:                   none
Commit/push/PR:                              none
Secrets printed or packaged:                none
```

## Package navigation

Start with:

1. `01_INDEPENDENT_REVIEW_BRIEF.md`
2. `02_R6_CURRENT_STATE.md`
3. `CloudTMS_Candidate_App_Current_Decisions_20260816_Phase1A_R6.pdf`
4. `r6_documents/CANDIDATE_DAILY_PHASE1A_IMPLEMENTATION_AUTHORITY.md`
5. `r6_documents/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md`
6. `r6_documents/GOOGLE_EVIDENCE_GATE_20260816.md`
7. `source/` and `tests/`
8. `evidence/`
9. `baseline_r5/` for the full preceding authority.

Run `tools/validate_candidate_daily_r6_pack.py` against the extracted package root before review.

## Required independent-review outcome

Issue either:

- **GO for Phase 1A source authority**, explicitly retaining the dark/no-activation state and permitting Phase 2 authoring only; or
- **NO-GO**, limited to reproducible defects in the supported Phase 1A route/policy/HMAC/limit/failure boundary.

Do not treat deferred Phase 2 SQL, Phase 1B integration, Phase 3 Google adapter, Phase 4 UI or later rollout as accidental omissions: each is a separate named gate. Equally, do not approve full Candidate Daily activation from this package.
