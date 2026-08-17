# Candidate Daily Phase 2 and Phase 1B R8 - Current State

Date: 17 August 2026

## Phase disposition

| Phase/boundary | Current state |
| --- | --- |
| Candidate core DB/RPC/backend/API | independently accepted; unchanged |
| Candidate Office API/frontend preparation | independently accepted; unchanged by R8 |
| Phase 0 R5 decisions/merged contract | independently accepted |
| Google Evidence Gate | complete; NEW MASTER ROTA current Head is active web v101 |
| Phase 1A R7 transport | independently accepted and preserved |
| Phase 2 Daily SQL/RPC | implemented, installed in TEST, independently awaiting R8 verdict |
| Phase 1B broker-to-RPC | implemented/deployed in TEST, independently awaiting R8 verdict |
| Phase 3 Google coexistence | not started; Apps Script unchanged |
| Phase 4 Candidate Daily UI | not started |
| Phase 5 controlled cutover | not authorised |
| Phase 6 full specialist/workflow acceptance | not started |
| Phase 7 gradual rollout/legacy retirement | not authorised |
| Production | untouched |

## Published authority

| Item | Identity/result |
| --- | --- |
| Repository/branch | `kierarthur/cloudtms-backend` / `test` |
| Runtime implementation | `fad3b82a0e6559854964a3d64b8be527d3492680` |
| Migration-order correction/current runtime head | `1823403f33fc6e3741c435dce5b2b3a6340db1de` |
| Candidate DB runtime workflow at first runtime commit | `31981046790`, success |
| Candidate DB runtime workflow at current runtime head | `31981494256`, success on PostgreSQL 17.6 and 18.1 |
| Safe migration | `31981114093`: Candidate migration/repeatable installed, then pre-existing James catalogue mismatch stopped the workflow |
| Candidate private Worker | `689bbe95-bf31-4f91-8e5a-40289558cefa`, 100% traffic |
| Candidate public broker | `18f67f8e-3ca2-46ad-9599-8512894de6c3`, 100% traffic |
| Normal TEST backend | unchanged by R8 |
| TEST frontend/Pages | unchanged by R8 |

## Installed TEST state

Project: `test-cloudtms` / `yakevhtttcsljosbdpov` / eu-west-2 / PostgreSQL 17.6.

- migration `17082026_0010_candidate_daily_phase2_authority_schema.sql` is in the installation ledger;
- repeatable `17082026_0015_candidate_daily_phase2_rpcs_v1.sql` ledger SHA-256 is `d9297dd73058e71ad01fb96e9460077be2ffc2649acb1b0fadeee615302f668c`;
- amended bootstrap repeatable ledger SHA-256 is `55d6aab7d5e53ea8e81e4617c4740a32b3e23fe4aad3170f2e0b6a4e3d2b4153`;
- 12/12 Daily tables exist with RLS and no direct role DML;
- 13/13 Daily RPCs exist, are security-definer and service-role-only;
- 0/13 Candidate flags are enabled; `candidate_daily_enabled=false`;
- all seven Candidate core tables and all twelve Daily tables contain zero rows;
- no Candidate mail, push, R2 business object or external effect was produced.

## Shared migration qualification

Safe migration `31981114093` applied the Candidate prerequisite migration and new repeatable successfully. It then failed in the Banking Pay targeted fast-route catalogue verifier on exactly:

- `public.pay_preview_candidate_build_canonical_lines` definition hash;
- `public.pay_workbench_repair_orphaned_pending_source_build` definition hash;
- `private.pay_workbench_candidate_session_version_rebase_v1` definition hash.

Those are the explicitly declared out-of-band James definitions from another coordinated task. R8 did not edit, reinstall, revert or catalogue-normalise them. The failure is retained honestly as shared-state evidence and is not reported as a successful broad safe-migration run.

## Current safety position

Although SQL and Workers are present, Candidate Daily business use is impossible because the global switch is false, entitlement/source/generation/availability catalogues are empty and Candidate routes require all prerequisites. Signed-system routes also require valid service/HMAC/nonce authority; no signing secret is exposed to the public broker or legacy browser.

No emergency rollback is required. The safe current action is independent R8 review while all Candidate flags remain false.
