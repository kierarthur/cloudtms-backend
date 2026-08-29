# Candidate Daily R10 Verification Summary

Date: 17 August 2026

## Local exact-source gates

```text
Source contract:                  5 passed, 0 failed
Direct PostgreSQL 17.6 matrix:   PASS
Direct PostgreSQL 18.1 matrix:   PASS
Two-session PostgreSQL 17.6:     2 passed, 0 failed
Two-session PostgreSQL 18.1:     2 passed, 0 failed
Complete backend JavaScript:     613 passed, 0 failed
git diff --check:                PASS
```

The first complete-suite invocation in the fresh isolated worktree failed only because package dependencies had not yet been installed. `npm ci --ignore-scripts --no-audit --no-fund` installed the locked dependency set; the immediately repeated complete suite produced the authoritative result above. No source was changed to make that dependency-only invocation pass.

## GitHub dual-engine and migration gates

```text
Runtime commit:                  304ff61ba3f6870caa43928fd11d8ddeb7914e9e
Candidate DB workflow:          32027528703 / success
PostgreSQL 17.6 result:         PASS
PostgreSQL 18.1 result:         PASS
Candidate SQL suite count:      43 per engine plus 2 concurrency tests
Safe migration workflow:        32027528744
Safe migration result:          success
```

## Installed TEST proof

```text
Repeatable source SHA-256:      ba297193832c9f2b9e4f3dad894bcee039deafe3d2a8096818839e5f8b518b85
Installed ledger/source match: exact
Canonical function SHA-256:    bc0da2bc78a2df454aa3d658454af42b5014401562cf9afce2d2cb5b5b03096a
Normalised definition SHA-256:  a6682078185f4d811545ed6cbff9ff8bb8585d1bc3c07d8e2592576ff43858b7 (matches local 17.6/18.1)
R10 guard present:             true
RPC signature count:           1
service_role execute:          true
anon execute:                  false
authenticated execute:         false
```

The installed verification is read-only. No transition RPC, Candidate command, reconciliation, external effect or projection mutation is invoked against TEST.

## Exact regression meaning

The R10 negative matrix proves:

```text
every unresolved owner
-> database derives NONE
-> first rollback returns CANDIDATE_DAILY_NOT_READY
-> mode remains SUPABASE_PRIMARY
-> entitlement remains false
-> transition fence is false
-> transition ledger delta is zero
```

The false-assertion matrix separately proves:

```text
caller says DRAINED
database derives NONE
-> SEMANTIC_REJECTION
```

The preserved suite proves settled DRAINED, exact RECONCILED, final rollback, forward cutover, no-op, replay/conflict and cohort/concurrency journeys did not regress.

## Runtime/deployment qualification

R10 is a database repeatable correction. No Worker or frontend source changed. Publication nevertheless produced the normal TEST Worker's repository-linked deployment shown below; private/public Candidate Workers remained unchanged. Verification therefore records the new normal Worker identity and proves all three runtime boundaries without inventing a Worker-code change.

```text
Normal Worker:                   6ce0838a-862b-4abb-9fd1-d86e0202d5f4 / health 200 / ready 200
Private Candidate Worker:       9d73bbff-5099-4f12-a58d-64cb9dbb4889 / private-only; public binding probe ready
Public Candidate broker:        09ac826b-d7da-4932-b0ad-a5fe6e194779 / health 200 / ready 200
```

## Safety

```text
Secrets printed:                         no
Destructive SQL/RPC/actions:             no
Real TEST Candidate business mutation:   no
Candidate feature activation:            no
Google mutation/deployment:               no
Financial/Banking Pay mutation:           no
Production access/deployment:             no
```
