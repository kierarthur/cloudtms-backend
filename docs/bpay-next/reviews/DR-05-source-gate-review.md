# DR-05 source-gate review

Independent read-only review of the saved source on 11 October 2026. **Source-gate changes: PASS; no essential defect found in the reviewed delta.** No gates, tests, SQL, business data or hosted actions were executed by this reviewer. The root reported its localPG17 load COMPILED and12/12 source controls passing; those are root-reported results, not independently executed evidence here. Runtime, app and item acceptance remain NOT_RUN.

## Finite private closure

`scripts/bpay-next-source-gates.mjs:24–39` adds exactly16 explicit private signatures: prepare-job run/active; action receipt/accept/enroll/guard; release scope; Work hold release; released collection scope/reconciliation; Case hold release; release page; week exclusion; retained segment parent completion; completed-release proof; execution guard. They resolve through actual final routine definitions at `:1691–1699`, with exactly one matching body required. The new `DR05_ROUTINE_WORK_CALLEES` map `:684–1094` lists explicit per-signature names for those helpers and the changed existing preparation, Case, release, projection, service/intake and guard owners. It introduces no prefix, wildcard, arbitrary helper reflection or dynamically derived call closure.

The existing checker still refuses named history readers (`:1101`), every EXECUTE in a routine (`:1102`), and any call outside that signature's finite original+DR05 sets (`:1103–1115`). Its history-relation census still fails every remaining non-declaration/non-INSERT ledger occurrence (`:1142–1148`). The one new history point is the exact literal `exists(select 1 from private.bpay_next_internal_receipt where run_worker_id=v_worker.id)` solely for the named week-exclusion helper (`:1119–1120`); it does not waive the table generally. Existing current financially authoritative helpers remain reviewed through their original named closures; this map adds no alternate money owner or history reconciliation formula.

## Exact audited constraint block

`closedExpiryWorkerShapeUpgrade` (`:1479–1482`) requires the precise0559 migration path, its precise outer-part signature and exact trimmed DO body. The literal pins `private.bpay_next_preparation_expiry_worker`, CHECK-kind catalogue rows, the full original predecessor predicate, exactly one matching constraint, named refusal `BPAY_NEXT_EXPIRY_ORIGINAL_WORKER_SHAPE_UNRECOGNISED`, and only the escaped `%I` DROP CONSTRAINT on that fixed private relation. It matches migration0559 `:94–104`; the replacement `bpay_next_expiry_worker_shape_v2_ck` remains ordinary static SQL (`:105–113`). It admits neither a caller-picked object nor arbitrary catalogue-generated SQL.

Only this exact block obtains its local CONNAME scalar classification (`:1491–1492`) and exemption from the dynamic/dollar-literal hazard (`:1544`). The common classifier still refuses unclassified bodies (`:1486`), dynamic SQL outside established closed forms, invalid rowtype selects and unknown aggregate types (`:1555–1556,1577–1579`). Baseline hazard allowances remain same-signature/exact-statement/count-consumed (`:1588–1595`). No other migration, DO body or routine inherits this exception.

## Historical and current projection type evidence

The current registry `final-definer-closures.json:4720–4732` keeps0533 as predecessor and names0559 as final preparation projection owner. The script first verifies that registry (`:1687`) and resolves the actual final physical definition (`:1772–1774`); it checks both the historical0533 file and the final file. Each must contain exactly two MIN/MAX aggregates on `p.selection_no`, both native selected expressions and exactly two ordered `LIMIT p_limit` subquery endings, otherwise it refuses `BPAY_NEXT_PROJECTION_MEMBERSHIP_TYPE_UNCLASSIFIED` (`:1775–1781`).

The native schema evidence is concrete: run_selection.selection_no is BIGINT at core migration1324 `:450–461`; case_selection_item.selection_no is BIGINT at Case migration0300 `:189–205`. Saved0533 `:75–78,91–94` and0559 `:2458–2461,2474–2477` select those exact columns into aliasp and aggregate only the bounded Work/Case page. Both readers validate p_limit1..100 (0533 `:45–47`;0559 `:2422–2425`) and seek by exact run/Candidate/selection/cursor. The MAX operand is a numeric membership ordinal, never a UUID or ledger balance.

The `P . SELECTION_NO → BIGINT` allowance is passed only while scanning the exact projection routine statement in those two files (`:1783–1795`). Other statements receive no additional column map. A third same-alias aggregate fails the count; a different unresolved expression retains the aggregate hazard. General SQL write, numeric, frozen-authority and final-definer controls remain active (`:1786,1793–1795`). This is bounded native type classification, not a historical-file or whole-SQL waiver.

Saved SHA256:

| File | SHA256 |
| --- | --- |
| scripts/bpay-next-source-gates.mjs | `FD52ABA405AF43B885DCE074FE216E0AFAE7D1E97E1844C4F8C1AB7DD3429600` |
| repeatable0559 | `514FB6A069DFE860D06C76D98302554BF3ED93E8E027CDBAC460BF8CEA7DA9A6` |
| migration0559 | `1619C1D4EF5B06D0086E9757CE4B43D7C4D316ABAF757B2B1D814F8171557C20` |
| final-definer-closures.json | `1F4BEB308220B861E29E8FADA58C27E403F058E9CA3755D41728F290F21A4194` |
