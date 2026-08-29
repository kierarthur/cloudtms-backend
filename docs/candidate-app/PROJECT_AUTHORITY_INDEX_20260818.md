# CloudTMS Candidate App authority index

Date: 18 August 2026

## Whole-programme authority

| Pack path | Purpose |
| --- | --- |
| `CloudTMS_Candidate_App_Current_Decisions_20260818_Phase3_R17.pdf` | Complete decisions authority through AV-333 |
| `01_PROJECT_CONTINUATION_MASTER_HANDOVER.md` | Self-contained whole-project context, status, phase roadmap and decision tree |
| `02_PHASE_COMPLETION_MATRIX.md` | Quick disposition and remaining-work matrix |
| `decisions/IMPLEMENTATION_PLAN.md` | Living full-stack implementation plan and Office/app-ready rules |
| `decisions/AUTHORITY_MAP.md` | Canonical owners and caller graph |
| `decisions/BACKEND_API_CONTRACT.md` | Backend/API contract and Google coexistence route ownership |

## Core Candidate and Office authority

| Pack path | Boundary |
| --- | --- |
| `contract/CANDIDATE_API_OPENAPI_V1_MERGED_R8.yaml.txt` | Merged Candidate/Candidate Daily HTTP schema |

## Candidate Daily Phase 0-2/1B authority

| Pack path | Boundary |
| --- | --- |
| `decisions/GOOGLE_EVIDENCE_GATE_20260816.md` | Read-only Google project/source/deployment/trigger evidence scope |
| `decisions/CANDIDATE_DAILY_PHASE1A_IMPLEMENTATION_AUTHORITY.md` | Phase 1A signed transport |
| `decisions/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md` | Phase 1A decision matrix |
| `decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md` | Phase 2 DB/RPC and Phase 1B adapter |
| `decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md` | Phase 2/1B matrix |
| `decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md` | Later-controlling transition barrier |
| `decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R10_ROLLBACK_AUTHORITY.md` | Later-controlling rollback barrier |

## Phase 3 authority

| Pack path | Boundary |
| --- | --- |
| `decisions/CANDIDATE_DAILY_PHASE3_IMPLEMENTATION_AUTHORITY.md` | Complete Phase 3 coexistence decisions including R11-R17 |
| `decisions/CANDIDATE_DAILY_PHASE3_DECISION_COMPLIANCE_MATRIX.md` | Decisions AV-250 onward and R17 compliance |
| `runbooks/CANDIDATE_DAILY_PHASE3_INSTALLATION_RUNBOOK.md` | Safe install/qualification/proving sequence |
| `runbooks/CANDIDATE_DAILY_PHASE3_DIAGNOSTIC_AND_ROLLBACK.md` | Diagnostics and bridge-off rollback |
| `r17-current-gate/*` | Current exact R17 assignment, findings, state and evidence summary |

## Google source authority

| Pack path | Meaning |
| --- | --- |
| `source/README.md` | Installation boundaries and certified-source explanation |
| `source/SCRIPT_PROPERTIES.md` | Property names and secret-handling rules; no values |
| `source/availability-api/Code.gs.txt` | Complete certified current Availability source plus accepted seams |
| `source/availability-api/CloudTMSCandidateBridge.gs.txt` | Availability CloudTMS helper |
| `source/availability-api/rollback/Code.gs.txt` | Certified rollback source |
| `source/master-rota/Code.gs.txt` | Complete certified current Master source plus accepted seam |
| `source/master-rota/CloudTMSCandidateBridge.gs.txt` | Accepted proposed R16 Master helper; unchanged by R17 |
| `source/master-rota/rollback/Code.gs.txt` | Certified rollback source |

## R16 identity authority plus R17 effective transition integration

| Pack path | Meaning |
| --- | --- |
| `contract/18082026_0802_candidate_daily_identity_integrity.sql.txt` | One-time CID1/source-history integrity migration |
| `contract/18082026_0131_candidate_daily_first_generation_source_link_v1.sql.txt` | Transactional first-generation binder and generation RPC |
| `contract/candidate-daily-contract-v1.js.txt` | Closed Candidate Daily contract |
| `contract/candidate-daily-phase1b.js.txt` | Private Worker-to-RPC adapter |
| `workflow/candidate-db-runtime.yml.txt` | Reusable exact PG17.6/18.1 matrix |
| `workflow/supabase-migrate.yml.txt` | Dependent TEST safe migration |
| `tests/*` | Source, runtime and concurrency proof, including R16 and R17 |
| `evidence/*` | Raw R17 local JavaScript, exact PostgreSQL 17.6/18.1, workflow and PDF evidence |
| `contract/18082026_1051_candidate_daily_authority_transition_source_identity_v1.sql.txt` | Later effective authority-transition definition with SOURCE-before-scope locking and item conflict containment |

## Historical provenance

`history/` contains the retained R14-R16 handovers and current-state/provenance documents needed to understand how the current rules evolved. Historical PDFs and nested ZIP files are deliberately excluded because the current PDF supersedes them and nested archives make independent review harder.
