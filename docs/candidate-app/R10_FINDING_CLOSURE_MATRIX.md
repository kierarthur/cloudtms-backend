# R10 Finding Closure Matrix

Date: 17 August 2026

| Finding | Incoming R9 evidence | R10 correction | Executable proof | Status |
| --- | --- | --- | --- | --- |
| `R9-P2-ROLLBACK-001` | `SUPABASE_PRIMARY -> ROLLBACK_PENDING` could commit with database-derived `NONE` while unresolved work existed. | Every mode-changing item rejects derived `NONE` with `CANDIDATE_DAILY_NOT_READY` after caller/database equality. | Direct PostgreSQL matrix plus real two-session race on 17.6 and 18.1. | CLOSED FOR RE-AUDIT |

## Mandatory operation matrix

| Operation fact | Caller assertion | Required result | R10 proof |
| --- | --- | --- | --- |
| PENDING projection | DRAINED | `SEMANTIC_REJECTION` | direct SQL |
| PENDING projection | NONE | `CANDIDATE_DAILY_NOT_READY` | direct SQL |
| CLAIMED projection | DRAINED / NONE | conflict / not-ready respectively | direct SQL |
| RETRY projection | DRAINED / NONE | conflict / not-ready respectively | direct SQL |
| TERMINAL projection | DRAINED / NONE | conflict / not-ready respectively | direct SQL |
| IN_PROGRESS command | NONE | `CANDIDATE_DAILY_NOT_READY` | direct SQL |
| Other IN_PROGRESS batch | NONE | `CANDIDATE_DAILY_NOT_READY` | direct SQL |
| IN_PROGRESS effect | NONE | `CANDIDATE_DAILY_NOT_READY` | direct SQL |
| UNKNOWN effect | NONE | `CANDIDATE_DAILY_NOT_READY` | direct SQL |
| Two different-key first rollback calls with PENDING projection | NONE | both rejected; zero transition rows | two-session Node/PostgreSQL |
| Settled first rollback | DRAINED | committed when all other prerequisites hold | preserved R9 direct SQL |
| Valid deferred overlay | RECONCILED | preserved exact overlay path | preserved R9 direct SQL |
| Exact same-mode no-op | NONE | `NO_CHANGE`; no ledger append | preserved R9 direct SQL |

## Required rejection postconditions

Every R10 rejection must leave:

```text
authority_mode          = SUPABASE_PRIMARY
entitlement_enabled     = false
transition_in_progress  = false
transition ledger delta = 0
```

The direct suite asserts all four postconditions for every unresolved-owner family.

## Preserved verdicts

- Candidate core DB/RPC/backend/API GO remains in force.
- Integrated Office Candidate API GO remains in force.
- Candidate Daily Phase 0 and corrected Phase 1A GO remain in force.
- Candidate Daily Phase 1B GO from the R9 independent review remains in force.
- Only Phase 2 rollback acceptance is being re-audited in R10.
- Phase 3 remains blocked until R10 receives independent GO.

## No-change boundary

```text
New tables:                         0
New RPCs:                           0
Changed RPC signatures:             0
Changed HTTP routes/contracts:      0
Changed Worker routing:             0
Frontend changes:                   0
Google Apps Script/Sheet changes:   0
Candidate feature activation:       0
Financial/Banking Pay changes:      0
Production changes:                 0
```
