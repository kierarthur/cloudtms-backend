# Candidate Daily Phase 1A R7 - Verification Summary

## Saved-source gates

| Gate | Result |
| --- | --- |
| Focused R7 production-module TAP | 21 passed, 0 failed |
| Complete backend JavaScript suite | 589 passed, 0 failed after rebase to the current shared backend authority |
| R7 Node HMAC/query/raw-parser vectors | PASS - 3 positive, 2 route-valid, 24 negative, 8 query, 20 raw-parser |
| R7 Python HMAC/query/raw-parser vectors | PASS - same corpus |
| Candidate public Worker dry build | PASS |
| Candidate private Worker dry build | PASS |
| Normal TEST Worker dry build | PASS |
| JavaScript syntax and `git diff --check` | PASS |
| Decisions PDF generation/structure | PASS - 76 pages; first 70 preserved, 6-page R7 addendum |
| Decisions PDF visual QA | PASS - every page rendered; contact sheets and each R7 page inspected |
| Decisions PDF SHA-256 | `eefdec06d06306508ae8c67d842559ed8ab622b434515d5b7cefe1e062343f3c` |
| Expanded R7 vector fixture SHA-256 | `3aabb105d8d6d97bc0b916985e6791c5e356916ec62d07ef7fa6c90d0b805d30` |

## Publication/runtime gates

| Gate | Result |
| --- | --- |
| Backend runtime push | `fd7c8c4eee49ccb38848f0ebaa281f81a11a4974` on `test` |
| Candidate DB runtime | workflow `31975585688` - PostgreSQL 17.6 PASS; PostgreSQL 18.1 PASS |
| Safe migration | workflow `31975584305` - source guards PASS, then expected catalogue NO-GO on exactly three manually installed James reader definitions pending separate publication; no Candidate failure and no R7 SQL |
| Candidate private Worker deployment | `4cdbcdeb-fc06-4a22-8af0-6876f633e41d` - 100% traffic |
| Candidate public Worker deployment | `2bd9023c-b9de-4ae3-b5e2-2c91f96942f9` - 100% traffic |
| Public health/readiness | HTTP 200 / HTTP 200 |
| Missing/invalid/duplicate correlation | HTTP 400 closed `VALIDATION_FAILED`; valid response ULID; fixed message; no leakage |
| Duplicate signed key ID | HTTP 400 closed `VALIDATION_FAILED` |
| Rejected origin and forbidden preflight header | HTTP 403 closed `FORBIDDEN` |
| Declared-shorter/exact unsigned system probes | safe HTTP 400; over-declared raw client frame timed out before a Worker business response |
| TEST safety snapshot | PostgreSQL 17.6; Candidate flags 0/12; auto-authorise default false; seven Candidate tables empty; no Phase 2 Daily tables/functions |

## Limits and exclusions

- Valid signed-system success cannot be activated or used as a business proof in Phase 1A because the Daily business owner does not exist and secrets are not exposed to the public broker.
- The raw-header guarantee is intentionally limited to Cloudflare's Fetch-observable request representation.
- Playwright is not applicable: R7 has no Candidate Daily UI or Office frontend change.
- No database business row, Google row/source/trigger, Candidate flag, entitlement, email, push, R2 business object or production resource is changed.
- Safe migration run `31975584305` is retained as an honest shared-state NO-GO rather than relabelled as a Candidate pass. Its only three mismatches were the separately declared James read/presentation definitions already installed outside the published source/ledger; R7 did not touch or revert them.
