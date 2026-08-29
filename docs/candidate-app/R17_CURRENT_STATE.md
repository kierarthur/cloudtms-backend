# Candidate Daily Phase 3 R17 current state

Date: 18 August 2026

Environment: TEST-only unpublished review candidate

## Repository authority

```text
Repository: kierarthur/cloudtms-backend
Branch:     test
Base HEAD:  37b7e5d140cfb97d8b32b8b4b727b470ff545134
R17 commit: none
R17 push:   none
```

R17 exists only as saved local source, tests, workflow changes, decisions and assurance material. It has not been committed, pushed, installed into Supabase, deployed to a Worker or copied into Google Apps Script.

## Operational baseline

```text
Availability active version:   216
Availability rollback version: 215
Master active version:         102
Master rollback version:       101
R17 Google installation:       none
Availability bridge:           false
Master bridge:                 false
Triggers:                      unchanged
```

No Candidate feature flag, entitlement, source-link row, generation, Candidate record or TEST business row was changed.

## R17 correction

R17 supplies one later effective definition of `public.candidate_daily_authority_transition_atomic_v1`. It pre-acquires every syntactically valid source-identity lock in deterministic order before Candidate authority-scope locks, contains `IDENTITY_LINK_CONFLICT` as an indexed per-item rejection, preserves exact batch replay and leaves malformed source-link items on the existing `VALIDATION_FAILED` path.

## Verification state

```text
Focused Candidate Daily JavaScript: 72 passed, 0 failed
Complete backend JavaScript:        686 passed, 0 failed
PostgreSQL 18.1 ordered matrix:      46 suites passed
PostgreSQL 18.1 concurrency:         all required R17 races passed
PostgreSQL 17.6 ordered matrix:      46 suites passed
```

The exact PostgreSQL 17.6 and 18.1 local gates both pass. This pack remains an independently reviewable source candidate, not publication or installation authority; independent GO and separate publication permission are still required.
