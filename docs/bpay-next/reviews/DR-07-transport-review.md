# DR-07 closed transport source review

Reviewed 12 October 2026 independently, read-only. **SOURCE PASS for the Worker/Office transport slice; no essential defect found.** Banner UI, pixels and joined acceptance are outside this verdict. DR-28's actual Payment Batches host remains the dependency for the confirmed-channel banner handoff.

Authority: approved `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:202–222`, every DR-07 checklist row at `IMPLEMENTATION_CHECKLIST.md:3674–3700`, and the saved `design/DR-07-screen-investigation.md`. The native directory is a bounded progress list, with card-defined newest-first order; no new financial or selection authority follows from its response.

## Reviewed source

| File | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-preparation.js` | `80fdcf78fdb3c7e4110d976a7b1c7c7935e4d3ac7029b14176e5c84286b4c527` |
| BUILD-FE `js/bpay-next-office.js` | `231d28c48e249e2751d7f58fe2cc558722466508e2a490f4c2dbbdd91ab1a172` |
| `supabase/repeatable/12102026_0718_banking_pay_next_active_preparations_v1.sql` (response/admission contract reference) | `5615691cd204c789855bd5c5f52387770bb5bc5c52404674e87579ad8dee1fa0` |

Worker `:77–112` and Office `:269–304` define the same closed ACTIVE_PREPARATIONS input, context, row and result helpers. Their dispatch points are Worker `:322,:642,:668` and Office `:450,:953–959`. Optional cursor handling delegates through the existing null-aware field dispatcher. Required fields cannot disappear or become undefined; extra members and malformed/native-overflow counters fail closed.

The result binds agency, actor, owner epoch and ALL/MINE scope to the actual request/current transport actor. MINE additionally binds every returned creator to that actor; ALL preserves the native creator identity rather than borrowing another user's journal. Request cursors carry the same context; each row must follow the preceding native timestamp/UUID tuple strictly. Timestamp comparison preserves all six fractional digits using BigInt, including offset normalization. Duplicate run identities, an empty unfinished page, a cursor on a completed page, or a continuation not exactly equal to the last returned row are refused.

Native `0718:130–207` supplies the same schema and fixed descending `(created_at_utc,id)` keyset from current-epoch PREPARE membership. The directory preserves native PREPARING/REVIEW and OPEN/SEALED facts. It uses stored counter equations: ready + review + pending + removed = selected, with preparing <= pending. The validators enforce those equations with nonnegative signed-bigint-range decimal operands, require a nonblank creator display name and literal safe_to_close=true, and check the exact 72-hour started/expiry interval. Empty complete pages are valid. No browser money calculation, financial steps, invented identity or inferred completion is added.

The existing bounded UTF-8 reader remains in both transports. Worker caps encoded request queries at 32 KiB, the closed cursor at 8 KiB, and replies including the wrapper at 128 KiB; native row packing reserves context/cursor space below that reply envelope (`0718:198–207`). ACTIVE_PREPARATIONS and PROGRESS use a two-second transport deadline. Their read failures preserve outcome_unknown=false; the exact native ACTIVE_PREPARATIONS_CONTEXT_CHANGED refusal is a definite 409, with no mutation retry behavior added (Worker `:953,:975–1004`; Office `:852–883,:955–959`).

The PROGRESS schema gains only required `selection_state`, matching the native scalar output (`0718:100`). Existing mutation inputs/receipts, accepted identities, journal keys, saved request bytes and protected dispatch logic are unchanged in the reviewed diffs. Directory discovery is not authority to recreate, reseal or replace an original pending command. The separate banner caller must retain the existing actor/NEXT/agency fences and original unknown-journal recovery, as recorded in the investigation.

No tests, database/business calls, hosted requests, runtime renders or captures were performed for this review. Static source comparison and file hashing only; SQL money/projection review and UI/A4 evidence remain separately owned.
