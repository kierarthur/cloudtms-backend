# SP-17 independent design review

12 October 2026; sequence 137. **DESIGN PASS for BD151 at the reviewed bytes.** This is a source-backed design check, not implementation, database or joined acceptance approval. Root owns product changes. No product/checklist edits, tests, business SQL/RPC, fixtures, browser or hosted actions were performed.

Read the complete approved `05_SOURCE_AND_PROTECTED_PAY.md:574–599`, all four deliverables and ITEM (`IMPLEMENTATION_CHECKLIST.md:4060–4075`), `design/SP-17-investigation.md`, and `design/SP-17-i3.md`. D01–D04 and the NEXT/OR45 boundary are preserved. Card591 assigns no new Worker/screen deliverable; adapting the existing caller of the named bounded reader is necessary supporting scope because its current 200-row pages would otherwise skip rows. SP18 owns later list work. No new route, category system, protocol, financial owner or producer is proposed.

## Exact required changes

1. Retain both 0512 classifier functions. `weekly_source_operational_empty_v1` reads I3 at170 and preserves all eight native duty facts/NULL completeness187–198. `weekly_source_timesheet_category_v2` keeps archive precedence237, current authorised protected-family proof238–245 and exact APPLIED-only derived Withdrawn. No recovery-completion condition, Banking discovery filter or Restore action is added. Never-authorised/query/Additional/adjustment/Magnit/invoice branches remain original.
2. Successor of `private.bpay_next_source_origin_state_v1(uuid,uuid)` in 2106 adds only `SNOOZE` to the APPLYING publication phase set128 and enrolled native job phase set133. Current Position0016 enters SNOOZE259–262 and completes Work applied/publication/job/command only at300–316. Therefore SNOOZE is genuine PENDING/OK, not APPLIED or incompatible. Keep the current predecessor, revision/origin/inventory, module epoch, command/member/job authority, APPLIED117–122, strict NULL inputs and exact17-key reply138–160. No new keys/codes/count proxy are required.
3. Successor of `public.weekly_source_office_summary_rows_v1(jsonb)` in0559 changes only the page admission maximum from200 to100. Its one native paged read38 and each category/root/signature check stay in the same STABLE statement snapshot. A100-row bound also bounds distinct category roots to100, including ordinary or ContractWeek-only rows.
4. Keep the existing public Office page200/offset/response contract. Current `readWeeklySourceOfficeSummaryRows` at `broker/src/index.js:86042–86057` is the narrow adapter seam. For an ordinary requested page, use original filters/order with native `(offset, min(limit,100))`, followed when needed by `(offset+100, limit-100)`: at most two service reads. Do not clamp the outward page size or change the caller's offset progression. Each reply must independently satisfy the existing contract and its requested≤100 bound; a failed second read must reject the whole adapter result, not present a partial successful page.

The targeted patch (`index.js:86164–86215`) accepts up to200 identity records but each may contribute several distinct IDs. Chunk its **entire** deduplicated ID union into≤100-ID groups, read each with its own offset0/native bound, then retain the original identity correlation mapping. Do not retain the old `min(ids.length,200)` truncation as an aggregate cap or silently omit later groups. Ordinary sorted ID-list pages still use the full original filter plus offset pagination; targeted correlation patches do not introduce a replacement sort owner. Current normal page limit is200 (`index.js:86252–86255`), display scan pages200 (`86626`) and normal offsets are derived from the original page size (`86609–86610`), so SQL-only clamping is insufficient.

The design correctly disclaims a shared snapshot across two service calls. Card581/584 requires row/category/I3 snapshot coherence per service read; it does not specify a new cross-page snapshot protocol. Counts/totals remain their original owners. Keep calls sequential and introduce no per-root HTTP fan-out or automatic mutation retries.

## Closure conditions

No essential design defect remains in the saved BD151 note. Final source review must verify the exact parent inverses, complete targeted ID handling, ordinary offsets/order, per-call100 reply validation and unchanged17-key/ACL contract. Required final files/implementation commit belong in the provenance and I3 record; they are not yet claimed by this review. All card597 joined journeys remain AUTHORED_NOT_RUN, including pending-to-Withdrawn with live recovery and the nonempty duty cases. SP10's separate factualS3 conflict is not an authority for altering this accepted-revision classifier.

## Reviewed SHA-256

| File | SHA-256 |
| --- | --- |
| design/SP-17-i3.md | `cae7d26aa22c470566a465e07ed55bd6b91eb9f8e6362fd1ba0ae0fe0e88b5bc` |
| design/SP-17-investigation.md | `36444f65eb2c54e43672e5ee5d6c01324a819cb36317de70f0c811f6a887dcad` |
| 04102026_2106_banking_pay_next_source_origin_state_v1.sql | `1b91f4b5de40d4d18606dd89d65bfa4d93e9198c3c7a2d7bc3e56b0642d911ec` |
| 05102026_0559_weekly_source_office_category_batch_v1.sql | `7f7b89775be928ea49f001fb74b7262e7d2d6d1b1600739742bc620803d33d54` |
| 05102026_0512_weekly_source_operational_category_v2.sql | `602db4edb834be6a6b3428b5d47828b62f4c7ff3589738bc36d756b30a1b8700` |

The Worker is being authored concurrently; the anchors above identify the examined original caller seam rather than claim a final Worker byte hash.

## Authored-source amendment

The later exact call-graph review found S17-01 in `SP-17-source-review.md`: the original native summary materializes full-scope category/I3 evaluation before its final LIMIT. The narrow planned deltas remain correct, but the design's claim of at most100 inner I3 roots per service read is not closed by output pagination. The earlier DESIGN PASS does not certify that remaining bound; D03 source closure is OPEN until Root resolves or explicitly blocks it. No product changes were made by this reviewer.

Root's final BD151 amendment now records that exact ordinary-read obligation as BLOCKED and keeps whole ITEM open. Corrected design hash `9cd1881c42bdf6567112ea67830969e456afce21055c53b45673b83730a6fb5c` is reviewed. The scoped source receipt passes the actual1946/index narrow deltas without claiming the unfulfilled bound or joined acceptance. Existing timesheet_list_ids also reaches the same full unpaged category path, so no equivalent existing preclassifier bounded Stage page was found. No expansion into unlisted native row-source/projection owners is approved by this review.

