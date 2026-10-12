# SP-17 authored source review

12 October 2026; sequence137. **SOURCE FINDING OPEN: S17-01 blocks the claimed D03 ≤100-root service-read closure.** The two authored owner deltas and ordinary/targeted caller pagination pass the scoped source checks described below. No product/checklist edits, tests, SQL/RPC/business execution, fixtures, browser or hosted actions were performed. Source-only; compilation and joined acceptance are not independently claimed.

## S17-01 — outer page size does not bound native I3 evaluation

Card `05_SOURCE_AND_PROTECTED_PAY.md:584/590` requires I3/category batching at most100 roots per service read. New1946 enforces100 at its outer page admission199 and calls the original summary at209. However that original summary has an inner full-scope category call before paging:

- `19012026_extras.sql:7199–7202` materializes the complete `bulk_timesheet_workbench_row_source_v1(v_source_filters)` into `source_rows`; its final sorted LIMIT/OFFSET are8166–8167.
- Current bulk owner `14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql:1387–1390` calls `private.weekly_source_timesheet_category_v2(sr0.timesheet_id)` for its native row set. The bulk function has no outer page LIMIT. Its result exposes the resulting native category as Tools Stage; the lightweight summary uses that Stage for filtering/sorting as well as output.
- The retained classifier calls I3 at0512:170 for each qualifying current authorised Source/protected-family root. Consequently an ordinary request with no exact IDs and more than100 qualifying roots can evaluate I3 for the whole inventory despite emitting only100 rows. Splitting the Worker response into two100-row RPCs does not repair this inner evaluation bound.

This is source evidence, not a timing/EXPLAIN or observed data-count claim. It does not establish that today's database has >100 roots. The accepted design's statement that each category service read is bounded to100 roots cannot be certified from the present call graph. Do not replace the global Stage filter/order with a pre-page approximation or silently claim an index/snapshot fix. Root must either supply a genuinely bounded native call path preserving the existing whole-list category/filter/order contract, or record the exact D03 obligation as blocked for its owning later list work. No broad classifier/projection/producer rewrite is recommended by this review.

## Passed narrow source checks

Static statement inverse comparison: removing only the two SNOOZE enum additions and the new phase/cursor coherence predicate reconstructs the complete2106 I3 definition exactly after CRLF normalization. Changing the new public maximum100 back to200 reconstructs the complete0559 summary definition exactly.1946 contains precisely two CREATE OR REPLACE functions. Registry promotes exactly their existing signatures from the correct parents; no0512 classifier successor is introduced.

I3's native SNOOZE qualification (`1946:125–135`) requires original APPLYING/SEALED/predecessor/epoch authority plus both publication and enrolled job in SNOOZE with identical native cursors (including NULL). No-job SNOOZE or mismatched phase/cursor cannot become PENDING. Actual0016 writes that paired checkpoint atomically259–262 and updates the pair together on later pages; APPLIED/DONE/Work-applied finality remains300–316. Original NEW/REMOVED paths, strict inputs, all17 output keys and APPLIED predicates are unchanged. This adds no financial-completion/recovery condition. Private invoker/owner-only ACL and public STABLE definer/service-only grant remain original; owner statements and PostgREST notify are retained.

Worker `readWeeklySourceOfficeSummaryRows` (`index.js:86042–86071`) admits original limit1–200/nonnegative safe offset, uses at most two sequential native reads with original filters/order and exact offset+100, rejects each result exceeding its requested limit, stops only on a short page, and returns only after every required read succeeds. Failed second-read results are not exposed as a successful partial page. Existing page_size/total and totals-owner behavior is unchanged; no cross-RPC snapshot is claimed.

The existing targeted patch (`index.js:86165–86228`) chunks the entire already-deduplicated ID union at100, uses each chunk's offset0/native limit, and keeps original current-timesheet/timesheet/ContractWeek correlation precedence and requested patch count. It does not retain the previous aggregate min200 cutoff or per-root HTTP fan-out. Existing enrichment functions already chunk their own native reads; no new enrichment authority was added. The normal sorted ID-filter list path still supplies the complete original filter to offset pagination; targeted patch correlation is not a replacement sorting contract.

## Current reviewed hashes

| File | SHA-256 |
| --- | --- |
| 12102026_1946_banking_pay_next_source_applied_category_read_v1.sql | `2cfaaea1eee500ea40ed0a60998ab9a40235e0cc50887b6ac292a80f2a912e4f` |
| broker/src/index.js | `77673593d09145179b9855ffab5563569b8408e7bede806f1691f576642caa19` |
| design/SP-17-i3.md | `ffd0c4f51c03d42370203df2626c1015122653a8841e7f71f4bca759ba788f63` |
| final-definer-closures.json | `9a189f188ca3ce5983ef16ea89ae41b26bf7429604fdc46de8cbfef2aba6e530` |

Original parent hashes are recorded in the design review. All app journeys remain AUTHORED_NOT_RUN. This finding supersedes any earlier inference that a100-row output alone proves100 inner I3 roots.

## Final scoped source closure

Root amended BD151 to state S17-01 explicitly and keep D03 ordinary actual-I3 bounds and whole ITEM open. **SOURCE PASS for the actual1946/index delta and D01/D02/interface-shape portions; D03 remains useful response-bound/targeted-ID partial work, not complete.** No additional code defect was found at the current hashes. This scoped verdict does not remove S17-01 or independently certify installation, app acceptance or a future final implementation-commit record.

The requested existing equivalent preclassifier path was checked. Exact native IDs are admitted before category at `14082026_1310:230–241` and its ContractWeek point scope287–305. But an ordinary whole-list selection has no already-equivalent preclassifier paged reader: `public.timesheet_list_ids` in `15012026_summarytotals.sql:2252` disables paging in its source filters and calls the same `timesheet_summary_lightweight_rows_v1` at2427. It therefore reaches the same category/I3 call rather than supplying a fresh independent bounded Stage membership. The current bulk source derives Tools Stage from category1282–1283 and processing display1310–1313; original lightweight Stage filter/order is downstream. Moving a page boundary ahead of that derivation would alter the native whole-list contract. No unlisted row-source, invoice, producer, projection, membership or GUC change was made or recommended as part of the closed code delta.

Current code hashes remain SQL `2cfaaea1eee500ea40ed0a60998ab9a40235e0cc50887b6ac292a80f2a912e4f`, Worker `77673593d09145179b9855ffab5563569b8408e7bede806f1691f576642caa19`, registry `9a189f188ca3ce5983ef16ea89ae41b26bf7429604fdc46de8cbfef2aba6e530`. Current corrected design/I3 record is `9cd1881c42bdf6567112ea67830969e456afce21055c53b45673b83730a6fb5c`; investigation is `2ed3580ba9ebe21fc12e6667b6cdc8eb59e6c8815dc9160bfe202eb32c33e039`.

