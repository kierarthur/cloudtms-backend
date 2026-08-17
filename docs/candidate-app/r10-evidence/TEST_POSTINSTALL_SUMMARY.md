# Candidate Daily R10 TEST Post-install Summary

The post-install inspection was read-only against the exact TEST project.

```text
Project:                         test-cloudtms
PostgreSQL:                      17.6.1.147
Repeatable file/ledger SHA-256:  ba297193832c9f2b9e4f3dad894bcee039deafe3d2a8096818839e5f8b518b85
Installed function SHA-256:      bc0da2bc78a2df454aa3d658454af42b5014401562cf9afce2d2cb5b5b03096a
Normalised definition SHA-256:   a6682078185f4d811545ed6cbff9ff8bb8585d1bc3c07d8e2592576ff43858b7
Local 17.6/18.1 normalised match: exact
R10 guard present:               true
Exact RPC overloads:             1
SECURITY DEFINER:                true
service_role execute:            true
anon execute:                    false
authenticated execute:           false
```

The canonical installed hash is the raw `pg_get_functiondef` representation. The normalised hash removes formatting-only whitespace and exactly matches both locally compiled PostgreSQL definitions.

## Disabled and empty safety state

```text
Candidate flags enabled:        0
candidate_daily_enabled:        false
Candidate core rows:            0 across seven tables
Candidate Daily rows:           0 across twelve tables
Candidate-bound mail rows:      0
Transition/effect execution:    none
```

No transition RPC, Candidate command, reconciliation, projection, effect or external action was invoked against TEST.
