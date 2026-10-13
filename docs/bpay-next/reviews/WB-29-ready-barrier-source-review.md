# WB-29 Ready public read barrier — independent source review

13 October 2026. **SOURCE PASS; zero concrete findings in this narrowly reviewed successor.** Read-only comparison of `13102026_0117_banking_pay_next_chosen_display_read_barrier_v1.sql` with the complete current0055 parent and the0113 metadata/index migration. No SQL/RPC, database, business, fixture, test, runtime or hosted action was performed; no product was edited. This does not accept the pending Kind/DTO joins, date repair, performance or whole WB-29 item.

## Exact parent retention

The only executable function differences are the three stale chosen-date EXISTS probes and `_official_pay_date` added to the internal read scope. Removing these exact additions reconstructs the complete parent function text exactly after line-ending normalization: parent102549 UTF-8 bytes, successor103577 bytes. The full-file unified comparison contains those two hunks and one new leading comment only. There is still one `public.bpay_next_workbench_page_v1(jsonb)` definition, with the same STABLE/security-definer/search-path/timeout attributes and exact original owner, revoke, service-role grant and notify tail.

This complete inverse preserves the current0055 selected-estimate path, including actual Source companion `current_revision_id IS NOT DISTINCT FROM applied_revision_id` qualification before original approval-root publication checks, native selected quote/version consumption, RATE_DECISION filtering, bounded `jsonb[]`/array_cat and distinct native version merge, late version comparison, original Case/floor/headroom model, budgets, native selection and estimate continuation. It also preserves all original actor/NEXT/agency, closed request, namespace, cursor, selection-memory and special read-branch behavior. No public input, membership, page kind or response field is added.

## Barrier, index and trusted scope

The new probes separately test NULL, older and newer chosen dates against the already resolved native official pay date. Each is a point-range EXISTS with LIMIT1 and the exact0113 partial-index predicate: WORK, date-sensitive, complete, not dirty, CURRENT, and positive Ready or Action count. The support index is `(chosen_display_pay_date NULLS FIRST, work_id)` with that same predicate. Thus these are actual index-compatible stale-date ranges, not a full inventory count/SUM or an assertion that observed row counts changed. Index use or timing was not measured.

The probes extend the existing dirty/pending/adoption refusal before selection-memory resolution or successful list/estimate reads. They return the exact original `BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT` envelope and genuine maintained dirty/pending counters; clean counters can truthfully be zero for a date-only refusal. HEADER job progress and its response-size check are unchanged. Incomplete, dirty, noncurrent, nonsensitive and informational groups do not acquire a separate date-only fence.

`_official_pay_date` is injected only into `v_read_scope` from the existing native official-date result after public input validation and context construction, before the unchanged cursor comparison. It is absent from the public accepted-key list and the published scope/context; callers cannot supply or overwrite it. The published context's original official-date identity remains unchanged. Existing early special read kinds remain byte-equivalent to0055 and do not gain unrelated barrier behavior.

## Reviewed SHA-256

| Saved source | SHA-256 |
| --- | --- |
| 13102026_0117 chosen_display_read_barrier_v1 | `e924cb0c55d2af7c85f9c37c0b6164dec2ea8532f319ead1f49b38f0c7e2ee5b` |
| Complete parent13102026_0055 selected_treatment_estimate_v1 | `373fc87826c62b88524504b9f3c673a79e75cac1c55d4746bc6ce3d3b39c4d6a` |
| 13102026_0113 chosen_display_date_support migration | `27fa42f6c21758235e1970dc1aa684570636c82822b1a8bdcbb2c249ba232f8d` |

Static source-only receipt. Installation/catalogue verification and the concrete child contracts remain root's separate checks.
