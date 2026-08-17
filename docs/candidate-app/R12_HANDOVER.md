# CloudTMS Candidate App - Candidate Daily Phase 3 R12 handover

Date: 17 August 2026

Environment: TEST only

## 1. Executive result

R12 closes every bounded Availability write/mirror/recovery finding raised by the independent R11 audit and installs the corrected, complete Apps Script source in both live TEST Google projects while the bridge remains disabled.

The delivered position is deliberately dormant:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false
```

means the legacy Availability browser, Availability Sheet/cache, NEW MASTER ROTA publication, Emergency journeys and retained specialist services continue on their established path. The new helper performs no CloudTMS request, retry, status probe, state write, bridge log or bridge-owned Sheet write.

R12 is not Candidate feature activation and is not production rollout. The maximum independent verdict now available is:

```text
GO for the controlled Phase 3 enabled TEST proving gate
```

## 2. Incoming R11 findings and exact closure

| R11 finding | Root cause | R12 correction | Executable proof |
| --- | --- | --- | --- |
| Busy/deferred rows mirrored before Google durability | The rota-busy return path called the bridge immediately although the rows had only been queued. | The busy branch no longer mirrors. `_flushPendingWrites()` records the exact accepted rows only after successful value/background writes, releases the write lock, clears the queue, then mirrors that exact subset. | Busy/deferred no-network test and post-write/post-lock flush test. |
| Mixed results forwarded rejected dates | The helper rebuilt the command from the original request rather than the completed legacy result. | The helper derives the signed body only from unique rows where `applied===true` and `deferred!==true`; every date/code is closed-catalogue validated; all-rejected and contradictory results are fail-open no-ops. | All-accepted, all-rejected and mixed-result tests. |
| `409 ... STATUS_CHECK` was treated as terminal | A broad HTTP 4xx classifier cleared operation state without using the route contract. | One route-specific classifier now requires an exact HTTP status, `error_code` and `retry_class` triple. Every `STATUS_CHECK`, retryable or malformed response retains the exact operation and enters status recovery. | Three `STATUS_CHECK` tests, terminal `REFRESH`/`DO_NOT_RETRY` tests, malformed 409 test and complete lost-response test. |
| Supplied Phase 3 test was not self-contained | The test assumed repository paths and the HMAC fixture was absent from the archive. | The test resolves either repository or packaged paths. The archive includes the exact canonicalization fixture and validates itself by executing the extracted test from the archive root. | Archive validator requires 18/18 extracted-root tests. |

No Master Rota business rule, Candidate database authority, Worker route, signing protocol, projection/effect semantic, Emergency provider, legacy UI/login, manifest, scope, trigger, finance, Banking Pay or production authority was changed to close these findings.

## 3. Corrected Availability write order

### Normal non-busy write

1. Existing legacy validation determines each accepted or rejected date.
2. Existing Availability value/background writes complete.
3. Existing cache and legacy response behavior remain unchanged.
4. The helper receives the completed per-date legacy results.
5. Only durable accepted, non-deferred rows are normalized and frozen.
6. The bridge creates one request UUID/idempotency identity and signs the exact accepted subset.
7. Any CloudTMS failure is additive and fail-open toward the already-completed legacy result.

### Busy/deferred write

1. Existing browser request is queued exactly as before.
2. Existing cache/deferred browser result is returned exactly as before.
3. No CloudTMS operation exists at this point.
4. The existing flush owner later revalidates the current window, candidate, row, booked/blocked state and code.
5. Successful Google writes are recorded in a post-write list.
6. The legacy lock is released and queue state cleared before network work.
7. Only the successful post-write subset is mirrored.

The browser never waits for CloudTMS, and a row that Google rejected, superseded or failed to write cannot become canonical merely because it appeared in an earlier browser request.

## 4. Closed recovery authority

HTTP status alone never owns operation deletion. R12 classifies the exact response triple expected by the frozen Phase 1B route:

| Response class | R12 action |
| --- | --- |
| `2xx` plus `ok:true` | Authoritative success; clear the operation. |
| Approved terminal `DO_NOT_RETRY` or `REFRESH` triple | Bounded terminal rejection; clear only for the exact approved triple. |
| `STATUS_CHECK` | Preserve the exact operation and perform exact status recovery. |
| `RETRY_SAME_KEY`, `RETRY_AFTER` or transport uncertainty | Preserve the operation; status first. |
| Malformed or unknown 4xx | Treat as uncertain; never discard the recovery identity. |
| Exact status `404 / CANDIDATE_DAILY_OPERATION_NOT_FOUND / DO_NOT_RETRY` before retry | Consume the one exact retry and resend the frozen request unchanged. |
| Exact not-found after retry consumed | Status-only; never POST again. |

The request UUID, idempotency key, correlation ID, source HMAC and factual body never change during recovery.

## 5. Google installation and rollback authority

### Availability API

```text
Spreadsheet ID: 1BSomZL0jRse5SGfTgADwswVmIjY4mCMfvDAfQxxIUA8
Apps Script project ID: 11vXbScO0TtFRCXwM0T3tkbrpjNx5BATsv4WgW9fruzN_fWyCUMbMLjAe
Active web-app version: 216
Retained rollback version: 215
Deployment ID: AKfycbw9X71BbvC55s2iJhyfGU5PoBtOxC9jvFsdKpd8BvrNghMfw8Yy9X-iSZ1Xcw9oTAPMcA
```

Installed files:

```text
Code.gs
CloudTMSCandidateBridge.gs
```

### NEW MASTER ROTA

```text
Spreadsheet ID: 1eEnrLMhLX_FzuO7sdUfAzEnvuXAwmR79fBntKe5Gp04
Apps Script project ID: 1pZVArwdKFqomofNK121LaKyiqct7G-YWCDUlIVEX-t87P0oBXQ01Yx20
Active web-app version: 102
Retained rollback version: 101
Deployment ID: AKfycbxt6w1FQ9nRuubK2_dEIjGxQl9qING-HlfRbeDBXnmRZ50aOs23BpVnnougfppppDS8aQ
```

Installed files:

```text
Code.gs
CloudTMSCandidateBridge.gs
```

The complete live Master Rota `Code.gs` authority, including the previously installed operator TEST-property setup utility, is preserved in the pack. No `Untitled.gs` file remains. No trigger, manifest, OAuth scope or Google Sheet business cell was changed.

Rollback is immediate and independent per project: select the retained previous deployment version. The bridge flag remains false, so ordinary legacy behavior does not depend on either new helper.

## 6. Configuration state

The operator installed the approved TEST Script Property names in both Google projects. Availability alone also contains the executor property. Secret values were not read, displayed, committed or packaged.

Common property names:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED
CLOUDTMS_CANDIDATE_BASE_URL
CLOUDTMS_CANDIDATE_ENVIRONMENT
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET
CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET
```

Availability-only property:

```text
CLOUDTMS_CANDIDATE_EXECUTOR_ID
```

The disabled value remains `false`, the environment remains `TEST`, and the base URL remains the TEST Candidate broker. Wrangler secret-name evidence shows the TEST Candidate broker/private verifier key family is present under the expected ownership. R12 did not read secret values or deploy Worker source.

## 7. Source and Git authority

Implementation commits on `cloudtms-backend/test`:

```text
65d2d196b1e7a7622e7a66db9580a665dd3ebbb3
Correct Candidate Daily Phase 3 coexistence

2ffabc7cf8a945ea1a5d064af9a57c691940d629
Preserve deployed Master Rota authority

4e5e2a9f7667d2866f5061ae75eadc008ea0cc26
Make Candidate Phase 3 archive test portable
```

The second commit adds no new bridge behavior; it ensures the repository's complete Master Rota source equals the effective live project authority, including the operator setup utility. The third commit makes the extracted-root audit independent of the reviewer's Git line-ending configuration; it changes no runtime source.

## 8. Exact source hashes

```text
Availability API revised Code.gs
01a737620116b387776d1bcb24992abecb9cdde523f09a936c587b17a7b55309

Availability API CloudTMSCandidateBridge.gs
2a44bce4dd72613178370c342e8dcfc45f13bdaedf16ef29b828b6a64c079bdf

Availability API certified rollback Code.gs
eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f

NEW MASTER ROTA complete effective Code.gs
e0d4ac48bffd4e9e79b1c0439cb952a298a2d11f90e3caf2a53df4b9d779d700

NEW MASTER ROTA CloudTMSCandidateBridge.gs
58e8da3948f2890b42abd802485776169ff500dedcf060d27b449a60597bcb2c

NEW MASTER ROTA certified rollback Code.gs
c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8
```

## 9. Verification performed

```text
Focused Phase 1A/1B/2/3: 54 passed, 0 failed, 0 skipped
Complete backend JavaScript: 632 passed, 0 failed, 0 skipped
Standalone Phase 3 from extracted archive root: required 18 passed, 0 failed
Candidate broker Wrangler dry build: PASS
Private Candidate Wrangler dry build: PASS
Public Candidate /healthz: 200
Public Candidate /readyz: 200
Decisions PDF: 109 pages rendered and every page visually inspected
```

The focused suite retains all accepted Phase 1A, Phase 1B and Phase 2 contracts and adds the complete R11 finding matrix. The complete suite proves no broader backend regression.

## 10. Intentionally not executed

```text
Bridge enablement: no
Signed Google-to-Worker request: no
Candidate source-link/bootstrap mutation: no
Candidate business-data mutation: no
Projection/effect drain: no
Emergency/provider action: no
Supabase schema/RPC change: no
Worker source deployment: no
Candidate frontend change: no
Production action: no
```

The absence of an enabled round trip is intentional. It is the next separately authorised Phase 3 proving gate, not missing evidence for disabled installation.

## 11. Decisions authority

The 109-page R12 Decisions PDF preserves all 105 R11 pages unchanged and adds Sections 95-97 and AV-271 through AV-280. Those later-controlling decisions freeze:

- the durable accepted-subset rule;
- post-write/post-lock flush ordering;
- all-rejected and malformed-result no-op behavior;
- the exact response disposition catalogue;
- one exact retry only;
- self-contained extracted-root tests;
- installed TEST Google versions and rollback;
- continued false-flag dormancy;
- the complete Phase 4-7 product scope.

## 12. Full implementation still remaining

| Phase | Remaining outcome |
| --- | --- |
| Phase 3 proving | Independent R12 acceptance; enable one approved TEST cohort; prove signed tiles, availability, fourteen-day Master generation, exact replay, projection deferral, quota/latency/outage behavior and rollback. |
| Phase 4 | Complete responsive Candidate web, iOS and Android journeys plus retained specialist interfaces. |
| Phase 5 | Controlled TEST cutover with identity, shadow parity, soak, error-budget and rollback proof. |
| Phase 6 | Complete Emergency, cannot-attend, leave-early, running-late, DNA, messages/content, Past Shifts, DAILY signing and EMAIL/PHONE acceptance across retained and new paths. |
| Phase 7 | Gradual entitled rollout, monitoring and separately authorised retirement of the temporary browser compatibility adapter. |

Master Rota continues publishing to Availability after temporary browser retirement because Availability and Emergency still need current work truth until each retained consumer is separately migrated and accepted.

## 13. Safety statement

```text
Secrets printed or packaged: no
Raw candidate identity packaged: no
Database mutation: no
Destructive SQL/RPC/action: no
Candidate feature activation: no
Finance/Banking Pay/Policy X change: no
Production access/deployment: no
Screenshots packaged: no
Machine-local paths packaged: no
```
