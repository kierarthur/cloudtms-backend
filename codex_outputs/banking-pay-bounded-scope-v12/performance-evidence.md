# Banking Pay bounded-scope V1.2.4 performance evidence

Status: evidence template only. No TEST fixture, migration, RPC, Worker or data mutation has been run by this implementation worktree.

This artifact is completed only after the migration and runtime have been installed in an explicitly authorised isolated TEST environment. A missed gate blocks cutover; it does not authorise a larger timeout, truncated scope, per-row Worker fanout or Policy X change.

## Runtime identity

| Field | Required evidence |
| --- | --- |
| Backend Git commit | Exact commit containing the migration, repeatables and Worker integration |
| Worker blob/version | Exact isolated Worker source identity |
| Supabase project | `test-cloudtms` / `yakevhtttcsljosbdpov` only |
| PostgreSQL | Installed server version |
| Migration/repeatable hashes | SHA-256 for every applied V1.2.4 SQL file |
| Envelope version | Active version, limits and evidence digest |
| Lane configuration | One-lane baseline; four-lane result recorded separately |

## Hard gates

| Gate | Required result | Observed | Pass |
| --- | ---: | ---: | :---: |
| Execute RPC p95 | `< 6,000 ms` | Pending | ☐ |
| Execute RPC observed maximum | approximately `< 7,500 ms` | Pending | ☐ |
| External timeout | `0` | Pending | ☐ |
| Ordinary trigger p95 | `<= 20 ms` | Pending | ☐ |
| Ordinary trigger maximum | `<= 50 ms` | Pending | ☐ |
| Cumulative internal-trigger p95 | `<= 100 ms` | Pending | ☐ |
| Cumulative internal-trigger maximum | `<= 250 ms` | Pending | ☐ |
| Growing-table sequential scan in trigger path | `0` | Pending | ☐ |
| Financial `EXCEPT ALL` differences | `0` both directions | Pending | ☐ |
| External finance/publication partial commit | `0` | Pending | ☐ |

## Fixture definitions

Fixtures must be economically real and isolated by explicit TEST IDs. Zero-effect padding is prohibited.

| Fixture | Required composition |
| --- | --- |
| F1 | 1 `CLOSED` + 1 `LIVE`; current entitlement and canonical output |
| F2 | 10,000 `CLOSED` + the same 1 `LIVE` as F1 |
| F3 | 10,000 `CLOSED` + 5 `LIVE` |
| F4 | 101 relevant timesheets with settled components, reservations, positive/negative outstanding, cases/components, corrections and canonical output |
| F5 | 500 realistically mixed relevant timesheets; no padding |

F1 and F2 must have approximately identical active selector/helper inputs and query plans. F2 must not scan the 10,000-row closed history during ordinary refresh.

## Per-fixture evidence

Record minimum, median, p95-equivalent and maximum. Record scheduler wait separately from service time.

| Metric | F1 | F2 | F3 | F4 | F5 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Selected active scope | Pending | Pending | Pending | Pending | Pending |
| Dependency nodes | Pending | Pending | Pending | Pending | Pending |
| Dependency edges | Pending | Pending | Pending | Pending | Pending |
| Fact rows by family | Pending | Pending | Pending | Pending | Pending |
| Finance case/component DML | Pending | Pending | Pending | Pending | Pending |
| Canonical rows/bytes | Pending | Pending | Pending | Pending | Pending |
| RPC 1 calls/latency | Pending | Pending | Pending | Pending | Pending |
| RPC 2 calls by stage | Pending | Pending | Pending | Pending | Pending |
| Complete candidate-chain time | Pending | Pending | Pending | Pending | Pending |
| Scheduler wait | Pending | Pending | Pending | Pending | Pending |
| Shared/local buffers | Pending | Pending | Pending | Pending | Pending |
| WAL records/bytes | Pending | Pending | Pending | Pending | Pending |

Attach `EXPLAIN (ANALYZE, BUFFERS, WAL)` output for selector, closure pages, fact pages, trigger parent statements, atomic publication and bounded cleanup. Redact candidate/payment identities; retain plans, counts and timing.

## Trigger scale matrix

For every new I/U/D backstop and each retained finance dirty trigger, run one-row, 100-row and 1,000-row parent statements. Compare relevant changes with audit-only changes and exact internal effects.

| Relation/operation | Rows | Parent ms | Trigger ms | Buffers | WAL bytes | Locks | Seq scan | Pass |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: | :---: |
| Pending | 1 | Pending | Pending | Pending | Pending | Pending | Pending | ☐ |
| Pending | 100 | Pending | Pending | Pending | Pending | Pending | Pending | ☐ |
| Pending | 1,000 | Pending | Pending | Pending | Pending | Pending | Pending | ☐ |

## Bounded terminal paths

Prove with `EXPLAIN (ANALYZE, BUFFERS)` that successful closure `COMPLETE`, bootstrap READY and cleanup terminal checks use metadata plus indexed existence probes. They must not issue an unbounded `COUNT(*)`, aggregate digest, whole-build edge join or sequential scan.

| Path | Fixture size | Rows read | Buffers | Duration | Whole-build rescan | Pass |
| --- | ---: | ---: | ---: | ---: | :---: | :---: |
| Closure COMPLETE | Pending | Pending | Pending | Pending | No | ☐ |
| Bootstrap READY | Pending | Pending | Pending | Pending | No | ☐ |
| Cleanup terminal | Pending | Pending | Pending | Pending | No | ☐ |

## Concurrency, cancellation and recovery

Record exact outcomes for dirty changes before claim, after claim, during closure/facts/reconciliation, between publication preparation and commit, and immediately before finalisation. Same-candidate work must serialize; different candidates must progress independently.

| Scenario | Expected | Observed | Pass |
| --- | --- | --- | :---: |
| Worker crash after RPC 1 | STARTED attempt expires after lease + grace; new nonce recovers | Pending | ☐ |
| RPC 2 timeout/cancellation | Stage transaction rolls back; committed attempt remains actionable | Pending | ☐ |
| Duplicate/late/stale nonce | Safe no-work; no finance/publication mutation | Pending | ☐ |
| Concurrent dirty generation | Current attempt/build becomes obsolete; candidate remains DIRTY | Pending | ☐ |
| Publication reader race | Complete old run before commit or complete new run after commit | Pending | ☐ |
| Attempt exhaustion | Durable actionable failure; no permanent Refreshing | Pending | ☐ |

## Bootstrap and cleanup

Prove interruption, duplicate page, concurrent dirty event, 10,000-row resumption, dependency-closed unit classification, active-remainder reconciliation and READY gating. Prove cleanup protects current, retryable, blocked, attested and unpublished builds and deletes no more than 500 child rows per unit.

| Check | Required result | Observed | Pass |
| --- | --- | --- | :---: |
| Discovery page | `<= 250` flattened source rows | Pending | ☐ |
| Unit evidence/apply page | `<= 250` sealed scope rows | Pending | ☐ |
| Reset/cleanup page | `<= 250` bootstrap reset / `<= 500` cleanup rows | Pending | ☐ |
| Concurrent dirty event | Never sealed by older captured generation | Pending | ☐ |
| READY | All streams, units, active reconciliation and publication complete | Pending | ☐ |

## Queue capacity and fairness

Measure one lane first. Test four lanes separately; do not enable them unless contention and fairness pass.

| Configuration | Ordinary candidates/min | 1,000 backlog service | 10,000 backlog service | Normal-due fairness | DB contention | Pass |
| --- | ---: | ---: | ---: | --- | --- | :---: |
| One lane | Pending | Pending | Pending | Pending | Pending | ☐ |
| Four lanes | Pending | Pending | Pending | Pending | Pending | ☐ |

## Policy X and cutover decision

- Pre-draft calculation used live truth only: Pending.
- Post-draft checks used frozen batch artifacts only: Pending.
- `TS_DAY` remained `YYYY-MM-DD`: Pending.
- No economic-key ladder, public Banking Pay contract, settlement, remittance or provider behavior changed: Pending.
- Cutover decision: **BLOCKED until every mandatory row above passes.**
