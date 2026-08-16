# Candidate Daily Phase 1A R6 Verification Summary

## Final saved-source results

| Evidence | Result | Pack file |
| --- | --- | --- |
| Focused production-module TAP | 13 pass, 0 fail | `evidence/focused-phase1a.tap` |
| Complete backend TAP | 576 pass, 0 fail | `evidence/complete-backend.tap` |
| R5 HMAC Node | PASS | `evidence/r5-hmac-node.log` |
| R5 HMAC Python | PASS | `evidence/r5-hmac-python.log` |
| Source identity Node | PASS | `evidence/source-identity-node.log` |
| Source identity Python | PASS | `evidence/source-identity-python.log` |
| R5 pack validator | PASS | `evidence/r5-pack-validator.log` |
| Runtime/OpenAPI parity | 24/24 exact, SHA exact | `evidence/runtime-openapi-parity.log` |
| Candidate public Worker dry build | PASS | `evidence/worker-public-dry-build.log` |
| Candidate private Worker dry build | PASS | `evidence/worker-private-dry-build.log` |
| Normal backend Worker dry build | PASS | `evidence/worker-normal-dry-build.log` |
| PDF structural check | 70 nonblank pages, SHA exact | `evidence/decisions-pdf-check.log` |
| PDF visual inspection | all 70 pages pass | recorded in handover/implementation authority |
| JavaScript syntax | PASS | rerun directly against `source/` |
| Git whitespace check | PASS | implementation worktree proof |

## Focused executable cases

The 13 focused tests cover:

1. exact merged OpenAPI route catalogue;
2. additive bootstrap preservation;
3. disabled Candidate capability and stable reason;
4. unlisted/wrong-method fail-closed behaviour;
5. Candidate correlation validation/generation;
6. Candidate body, framing, JSON and idempotency constraints;
7. Candidate rate-class ownership;
8. signed-system browser-authority rejection;
9. exact signed byte forwarding to the private Worker;
10. HMAC positive, rotation, invalid configuration and negative vector cases;
11. atomic nonce/replay behaviour;
12. signed correlation preservation and safe failure envelopes;
13. exact route limits, bodies, rates, in-flight metadata and deadlines.

## Runtime build boundary

All three Worker builds were `wrangler deploy --dry-run`. No Worker was deployed. The normal Worker command emitted the repository’s existing multi-environment warning because no environment was selected for a dry run; it did not change external state.

## Dependency note

`npm ci` used the committed lock. The repository reports inherited dependency advisories (11 high, 1 critical). No dependency was changed and no `npm audit fix` was run because it would exceed the bounded Phase 1A authority.

## Explicit limitations

- no Phase 2 database authority exists, so no real Daily business mutation can be tested;
- no durable database receipt/lease exists yet, so distributed in-flight ownership remains a Phase 2/1B activation prerequisite;
- no Google adapter exists, so no Apps Script-to-CloudTMS effect was run;
- no Candidate Daily UI exists, so Playwright is not applicable to Phase 1A source-only transport;
- no TEST or production runtime deployment was performed.

These limitations are intentional phase gates, not hidden claims of completion.
