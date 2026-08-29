# Candidate Daily Phase 1A R7 - Current State

Date: 16 August 2026

## Product state

| Boundary | State |
| --- | --- |
| Phase 0 R5 decisions/merged API | accepted and unchanged |
| Google Evidence Gate | complete; Master Rota current Head later deployed by user as active web v101 |
| Phase 1A transport/policy source | R7 corrected |
| Daily business tables/RPCs | absent; Phase 2 not started |
| Phase 1B broker-to-RPC integration | not started |
| Phase 3 Google coexistence adapter | not started |
| Phase 4 Candidate Daily UI | not started |
| Candidate Daily feature/entitlement | disabled/dark |
| Google source/data/triggers | unchanged by R7 |
| Production | untouched |

## R7 scope

R7 corrects only the nine independent R6 findings: closed error schema/messages, public reconstruction, 403 preservation, unverified key-ID throttling, server-consumption nonce age, signed correlation, exact framing, ASCII query ordering and Fetch-boundary wording/proof.

No Phase 2, SQL, Supabase business mutation, frontend, Google edit, Apps Script deployment, Candidate entitlement, email, push, rota, Emergency or external business effect is part of R7.

## Publication state

| Item | Identity |
| --- | --- |
| Implementation commit | `fd7c8c4eee49ccb38848f0ebaa281f81a11a4974` |
| Published backend runtime head | `fd7c8c4eee49ccb38848f0ebaa281f81a11a4974` before the later documentation-only evidence commit |
| Candidate DB runtime | workflow `31975585688` - PASS on PostgreSQL 17.6 and 18.1 |
| Safe migration | workflow `31975584305` - expected verifier NO-GO on exactly three pre-existing manually installed James reader definitions; Candidate source authority passed and R7 has no SQL |
| Candidate private Worker version | `4cdbcdeb-fc06-4a22-8af0-6876f633e41d` - 100% traffic |
| Candidate public Worker version | `2bd9023c-b9de-4ae3-b5e2-2c91f96942f9` - 100% traffic |

The safe-migration verifier did not report Candidate drift. It rejected exactly the separate James task's pending source/ledger publication boundary and those definitions were not reinstalled or reverted by R7.

## Postdeployment proof

| Check | Result |
| --- | --- |
| Public `/healthz` | HTTP 200; TEST Candidate broker |
| Public `/readyz` | HTTP 200; private binding ready |
| Missing/invalid/duplicate correlation | closed HTTP 400 `VALIDATION_FAILED`; valid response ULID; no private detail |
| Duplicate signed key ID | closed HTTP 400 `VALIDATION_FAILED` |
| Rejected origin / forbidden preflight header | HTTP 403 `FORBIDDEN` |
| Candidate feature flags | 0 enabled of 12 |
| Candidate electronic auto-authorise default | false |
| Seven Candidate business tables | 0 rows in every table |
| Phase 2 Daily tables/functions | 0 / 0 |
| Business effects attempted | none |

## Safety statement

Candidate business capability remains false. The R7 deployed source may validate, rate-limit, authenticate and reject requests, but no Daily business execution can occur because the Phase 2 database/RPC authority and Phase 1B integration do not exist. This is fail-closed by design.
