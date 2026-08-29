# Candidate Daily Phase 2/1B R9 - R8 Finding Closure Matrix

| R8 finding assertion | R8 defect | R9 correction | Executable proof | R9 result |
| --- | --- | --- | --- | --- |
| Forward mode could commit without a generation | Request JSON could omit generation proof | Strict switch requires expected generation ID/version and exact locked active generation | missing, incomplete and wrong generation cases | CLOSED |
| Caller could assert in-flight disposition | `DRAINED` was not independently derived | PostgreSQL derives disposition from locked command/batch/effect/outbox rows and requires request equality | false-DRAINED plus all unresolved-state cases | CLOSED |
| Source authority was not proved | Transition did not require one current source identity | Locked source catalogue must contain one current PRIMARY in one active group | missing, ambiguous, disabled source cases | CLOSED |
| Cursor/freshness was not proved | Empty or lagging sync state could accompany transition | Exact READY sync and three cursor equality checks plus latest-fact watermark | lagging cursor and stale generation cases | CLOSED |
| Overlay claim could be unproved | Deferred overlay was not bound to active generation row truth | Exact ID/version/date/source-row hash validation | invalid overlay rejects; exact overlay commits RECONCILED | CLOSED |
| Terminal/in-flight work could be called drained | Current work owners were not locked/classified | Pending, claimed, retry, terminal, command, other batch, IN_PROGRESS and UNKNOWN effect all block | complete in-flight matrix | CLOSED |
| Rollback could be an unproved direct flip | Caller supplied disposition and sparse snapshots | Global switch off first, ROLLBACK_PENDING stage, then strict Google parity proof | complete forward/rollback sequence | CLOSED |
| Transition audit could contain null winner facts | Ledger copied sparse execution state | Authority-changing commit freezes locked generation and full sync snapshot | immutable ledger assertions | CLOSED |
| Missing scope could be silently created | Transition was also bootstrap authority | Missing scope now rejects without creating scope/entitlement/transition | missing-scope case | CLOSED |
| Cohort failures could contaminate another item | Per-item state was insufficiently proved | All per-item variables reset and each business rejection rolls back its subtransaction | one commit/one reject partial cohort | CLOSED |
| Concurrency was not demonstrated | Inventory test did not invoke the transition owner | Two real independent PostgreSQL sessions prove exact replay and single-winner different-key cutover | parallel Node/PostgreSQL integration | CLOSED |
| R8 evidence overstated AV-214/AV-215 | Documentation said PASS without executable proof | Living matrix records the R8 qualification and later-controlling R9 proof | updated matrix/PDF/review brief | CLOSED |

## No additional finding introduced

R9 adds no table, public RPC, route, UI, feature enablement, Google change or financial authority. It preserves Phase 1B mappings and all accepted Candidate core/Office boundaries. The correction is dark until a separately approved future cutover.

