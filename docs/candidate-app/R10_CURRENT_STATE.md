# Candidate Daily R10 Current State

Date: 17 August 2026

## Source authority

```text
Baseline backend test:             a8b94a3f6beea29c26f4011ba64a2384925ff8c5
Published R10 runtime commit:       304ff61ba3f6870caa43928fd11d8ddeb7914e9e
Backend origin/test at verification:304ff61ba3f6870caa43928fd11d8ddeb7914e9e
Candidate runtime drift after R10: none
Frontend origin/main at verification: ae17c5791a05d56039ae3ddc7157b0153f2aabc9
Frontend changes in R10:           none
```

## Workflow and installed authority

```text
Candidate DB runtime workflow:     32027528703 / success
PostgreSQL 17.6:                   43 suites + 2 concurrency tests PASS
PostgreSQL 18.1:                   43 suites + 2 concurrency tests PASS
Safe migration workflow:          32027528744 / success
Repeatable file/ledger SHA-256:    ba297193832c9f2b9e4f3dad894bcee039deafe3d2a8096818839e5f8b518b85
Installed canonical function SHA: bc0da2bc78a2df454aa3d658454af42b5014401562cf9afce2d2cb5b5b03096a
Normalised function SHA-256:       a6682078185f4d811545ed6cbff9ff8bb8585d1bc3c07d8e2592576ff43858b7
```

## Worker authority

R10 changes no Worker source or configuration. The deployed TEST Worker identities are recorded to prove the runtime boundary stayed stable:

```text
Normal TEST Worker:                6ce0838a-862b-4abb-9fd1-d86e0202d5f4 / 100%
Private Candidate Worker:          9d73bbff-5099-4f12-a58d-64cb9dbb4889 / 100%
Public Candidate broker:           09ac826b-d7da-4932-b0ad-a5fe6e194779 / 100%
Normal /healthz:                   HTTP 200 / ok
Normal /readyz:                    HTTP 200 / ready=true
Private direct health:             intentionally not internet-routable
Public /healthz:                   HTTP 200 / candidate-broker / TEST
Public /readyz:                    HTTP 200 / private service binding ready
```

## TEST safety snapshot

```text
Candidate feature flags enabled:   0
Candidate core rows:               0 across seven tables
Candidate Daily rows:              0 across twelve tables
Candidate-bound mail rows:         0
Real transition/effect execution:  none
Google changes:                    none
Production changes:                none
```

## Current gate

```text
Candidate core DB/RPC/backend/API: GO retained
Integrated Office Candidate API:  GO retained
Candidate Daily Phase 0:           GO retained
Candidate Daily Phase 1A:          GO retained
Candidate Daily Phase 1B:          GO retained
Candidate Daily Phase 2:           R10 independent re-audit required
Candidate Daily Phase 3+:          not started / not authorised
Feature activation:                not authorised
```
