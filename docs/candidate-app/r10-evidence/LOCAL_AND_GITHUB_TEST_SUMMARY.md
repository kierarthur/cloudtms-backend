# Candidate Daily R10 Local and GitHub Test Summary

## Exact-source local gates

```text
Source contract:                5 passed, 0 failed
PostgreSQL 17.6 direct matrix: PASS
PostgreSQL 18.1 direct matrix: PASS
PostgreSQL 17.6 concurrency:   2 passed, 0 failed
PostgreSQL 18.1 concurrency:   2 passed, 0 failed
Complete backend JavaScript:   613 passed, 0 failed
git diff --check:              PASS
```

The complete suite was run after installing the repository's locked dependencies in the fresh worktree. No source correction was needed for the dependency-only first invocation.

## GitHub gates at the exact runtime commit

```text
Candidate DB runtime run: 32027528703 / success
PostgreSQL 17.6:          43 suites + 2 concurrency tests PASS
PostgreSQL 18.1:          43 suites + 2 concurrency tests PASS
Safe migration run:      32027528744 / success
```

The two workflow runs executed against runtime commit `304ff61ba3f6870caa43928fd11d8ddeb7914e9e`.

## Behaviour proved

- False `DRAINED` while the database derives `NONE` remains a semantic conflict.
- Truthful `NONE` cannot move authority while any tested unresolved owner exists.
- Every rejection preserves mode, entitlement, transition fence and transition-ledger count.
- Exact no-op `NONE`, settled `DRAINED`, exact `RECONCILED`, replay/conflict, cohort isolation and valid forward/final rollback paths remain green.
