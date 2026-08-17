# Candidate Daily Phase 2 and Phase 1B R8 - Verification Summary

## Source and contract gates

| Gate | Result |
| --- | --- |
| Complete backend JavaScript | 605 passed, 0 failed |
| Focused Phase 1A/Phase 1B/Phase 2 source contracts | 35 passed, 0 failed |
| Node syntax for changed JavaScript | PASS |
| Merged R8 OpenAPI semantic validation | PASS; 62 paths |
| OpenAPI local reference closure and operation IDs | PASS |
| Candidate private Worker dry build | PASS |
| Candidate public broker dry build | PASS |
| Exact changed-file boundary | 15 runtime/workflow/test/contract files before R8 evidence documents |

## PostgreSQL gates

| Runtime | Result |
| --- | --- |
| PostgreSQL 17.6 | 42 Candidate suites plus Phase 1B/runtime chain PASS |
| PostgreSQL 18.1 | 42 Candidate suites plus Phase 1B/runtime chain PASS |
| GitHub Candidate matrix `31981046790` | PASS at implementation commit |
| GitHub Candidate matrix `31981494256` | exact-current-head PASS on PostgreSQL 17.6 and 18.1 |

The local matrices create a clean database, apply all Candidate schema/repeatables in deployment order, run all inherited Candidate suites and then execute `17082026_0053_candidate_daily_phase2_runtime_verification.sql`. They prove complete generation, request/batch receipts, replay/conflict, overlay/cursor, transition, effect lease/status and ACL behaviour on both supported PostgreSQL versions.

## TEST install evidence

| Check | Result |
| --- | --- |
| Target project | `test-cloudtms`, ACTIVE_HEALTHY, PostgreSQL 17.6 |
| Phase 2 migration ledger | present |
| Daily repeatable ledger hash | exact source match |
| Bootstrap repeatable ledger hash | exact source match |
| Daily tables | 12/12 present; RLS on; no direct role DML |
| Daily RPCs | 13/13; fixed security-definer; service-role-only |
| Bootstrap | one overload; database Daily capability bound; service-role-only |
| Candidate flags | 0 enabled of 13 |
| Candidate/Daily rows | all zero |

The broad safe-migration workflow did not finish green. Candidate installation succeeded, then the declared out-of-band James definition hashes failed a Banking Pay verifier. R8 preserves that fact and independently proves the Candidate subset instead of mislabelling the whole workflow PASS.

## Deployment evidence

| Artefact | Version/result |
| --- | --- |
| Private Candidate Worker | `689bbe95-bf31-4f91-8e5a-40289558cefa`, 100% |
| Public Candidate broker | `18f67f8e-3ca2-46ad-9599-8512894de6c3`, 100% |
| Public `/healthz` | HTTP 200; Candidate broker; TEST |
| Public `/readyz` | HTTP 200 |
| Unsigned `GET /candidate-app/v1/daily/tiles` | HTTP 401; no business effect |
| Normal TEST Worker | not deployed by R8 |
| TEST frontend/Pages | not deployed by R8 |
| Production | not accessed/deployed |

## Security/adversarial coverage

- closed route/method catalogue;
- strict Candidate/session and signed-system separation;
- accepted/invalid key-rate partitioning;
- timestamp, nonce, correlation, body framing and query canonicalisation;
- strict public success/error reconstruction and future-field rejection;
- no request-owned Candidate identity;
- source-link ambiguity/missing/disabled failure;
- exact receipt replay and changed-input conflicts;
- one complete generation requirement;
- lease fencing for projections/effects;
- durable/effective cursor ownership and overlay retreat;
- closed three-mode transition state machine;
- flag/entitlement/mode/generation/freshness Candidate gate;
- global flag independence for signed-system continuity;
- no direct table privileges;
- no Google, finance, Office or Banking Pay drift.

## Limitations correctly retained

- No valid real Google HMAC success was executed because Phase 3 has not installed a Google adapter and secret-bearing test traffic was unnecessary for this disabled deployment.
- No real Candidate Daily business row, entitlement, source link, generation, availability write, email, push, R2 write or specialist effect was created.
- No Candidate Daily UI exists yet, so there is no R8 Playwright UI claim.
- Apps Script, Sheets and Google deployments remain unchanged; fresh Phase 3 effective-source verification is still mandatory.

## Safety defaults

```text
Secrets printed:                         no
Destructive SQL/RPC/actions:             no
Candidate business-data mutation:        no
Google edit/deploy:                      no
Normal TEST Worker/frontend deployment: no
Candidate isolated Workers deployed:    yes, explicitly authorised
Production access/deploy:                no
Policy X drift:                          none
```
