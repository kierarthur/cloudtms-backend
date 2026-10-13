# WB16 exact group/detail join design

Source-only design; no product edits, SQL/runtime/test execution or whole-WB16 acceptance. Read current common1946, core1324, maintained item schema1252/indexes1634 and current detail draft2019. The supporting change, if needed, is an inline GROUP_LINES enrichment in the named existing common reader plus exact Worker/FE field mirrors. No new helper, schema, RPC, producer or money owner is required.

## Smallest truthful join

GROUP_LINES is a maintained native **member/portion** page, not a day/shift page. Its existing keyset is `(item.id)` and cursor membership/order are UUID-based (1946:1000–1055). Keep that identity and page boundary. First select the existing scoped item identities/explicit typed columns with the original candidate/client/channel/section/group/ROW/search/seek predicates and `limit+1`. Enrich only those bounded seeds afterward. Do not place an approved-detail join or component aggregate before the seed limit, change cursor identity, aggregate by date, or claim the first100 member rows are a complete component.

For each seeded native WORK_COMPONENT/WORK_SEGMENT, join approved_line by the complete existing FK `(revision_id,approved_line_id,component_key)` and revision by `(work_id,revision_id)`; require the same native Work/Candidate scope. Return exact line_no, source_component_id, unit_label, approved_quantity/unit_rate, actual line.work_date and immutable revision.job_title/band_label. **There are no role/band columns on approved_line** (core1324:188–211); role/band are revision.job_title/band_label (161–162). Do not label a present-day contract read as approved role/band. CASE/GROUP_EVIDENCE rows retain NULL Work detail. Missing retained line is genuine missing evidence, never a best/latest line.

The already named COMPONENTS reader can be the header authority instead where only complete native parent A/C/H/residual and approved metadata are displayed. Its exact Work/revision/line_no page is sufficient and avoids a GROUP_LINES money addition. It does not provide classified per-section membership totals, so it cannot alone turn a mixed Ready/Waiting day into separate truthful amounts. For that exact requirement an inline maintained-item rollup is the smallest supporter.

## Server amount enrichment, not browser aggregation

Deduplicate at most101 seeded native `(group_key,candidate_id,work_id,revision_id,component_key,approved_line_id)` keys in the same statement. For each key aggregate only **current maintained** workbench_item rows with the exact same key and Work-member kinds, across all own portions; exclude GROUP_EVIDENCE and Case rows. This is one indexed bounded-key fan-in, not history. Join the resulting one-row scalar aggregate back to each seed without multiplying output rows. A simple per-seed LATERAL is correct but repeats work for sibling portions; a deduplicated seed-key CTE/inline aggregate is preferable for many same-day shifts. Keep the original public STABLE single-statement snapshot/context guards.

Use separate named native operands, never generic sum(nominal) or a browser rate/money calculation:

- Full component approved/realised/active-reserved/residual/included/excluded/recovery/held-overhang/written-off totals are sums of their like-named stored typed fields across own current portions. This directly follows current1946:339–348 per-native-member conservation.
- Ready display amount is the stored positive `included_source_ex_vat` of its own `section='READY'` AVAILABLE/ZERO portions, returned separately from current Waiting reserved amount. Zero is allowed only when all contributing authority is known and the native zero/member evidence is real.
- Waiting reserved amount is `active_reserved_source_ex_vat` from own RESERVED portions. Snoozed/future/in-preparation positive amounts remain separate factual named amounts/section facts from their native AVAILABLE rows; do not combine them with reservations as if all were new payable money.
- Keep individual seeded member section/reason/selectable/snooze IDs and amounts intact. The component totals are contextual facts, not permission to tick blocked/snoozed siblings.

For every exposed total, a contributing unknown amount, missing numeric operand or incomplete/noncurrent parent makes that total NULL with a factual known/completeness flag. SQL SUM ignores NULL; guard it with actual count>0 and bool_and(required known/non-NULL operands), rather than silently summing the remaining known rows or coalescing unknown to0. Unknown component evidence must not be turned into selectable or a financial zero. Do not clamp signed residual/recovery to a fabricated positive. Do not include another revision/Candidate/group or treat a Source component as a WORK:DAY parent by its date.

Full-component totals are repeated context on member seeds; the FE must render them once under exact `(work_id,revision_id,approved_line_id,component_key)` and must never add repeated copies. If the UI needs section headers, use a **server section-specific scalar** joined under `(same component,section)` and label its scope. A complete current component total can be complete even when its member-detail page is partial; detail completeness remains the actual GROUP_LINES/SHIFTS cursor, not that total.

## Exact same-date independent shifts and portions

Native stable identity comes from `(approved_segment_id,segment_key)` and genuine shift_detail_id, not detail_no/date. Existing2019 SHIFTS adds actual segment/approved_segment IDs; use that exact approved-line+revision relationship and its own detail_no cursor, and BREAKS exact shift_detail_id+break_no. Daily/aggregate detail without a genuine segment remains NULL and has no manufactured physical shift.

One physical segment may have both AVAILABLE and RESERVED native portions. Current1946:325–335 deliberately copies identity into RESERVED while splitting counters; do not display the same physical shift twice merely because two item IDs exist. Render its actual shift detail once, with distinct native portion/status facts underneath. Separate same-date sibling **segments** that are Ready and Waiting keep their own identities, section and independent controls; a date rollup cannot merge them. A bounded item page may contain one portion before its sibling portion; the separate factual SHIFTS expansion can still load one real shift by exact ID, but never implies all native portions have arrived.

## Existing index proof and limits

| Need | Existing index/constraint | Source |
|---|---|---|
| Native group ALL UUID page | bpay_next_wb10_item_group_all_page_idx `(group_key,id)` |1634 read_indexes:24|
| Native group section UUID page | bpay_next_wb10_item_group_page_idx `(group_key,section,id)` |1634:23|
| Exact group/component aggregate prefix | UNIQUE `(group_key,component_key,member_kind,member_key,portion_kind)` |1252:126|
| Exact Work/component/segment lookup | bpay_next_workbench_item_work_idx `(work_id,component_key,segment_key)` |1252:178|
| Scope/Candidate filter | bpay_next_workbench_item_candidate_idx `(candidate_id,section,group_key,component_key,member_kind,member_key,portion_kind)` |1252:177|
| Exact line | approved_line PK and UNIQUE `(revision_id,component_key)` / `(revision_id,id,component_key)` |1324:189,206–208|
| Physical shift expansion | shift_detail UNIQUE `(approved_line_id,detail_no)` |1324:231|
| Actual approved segment | approved_segment unique shift_detail_id plus exact composite parent FK |1100 segment authority schema|

No new index is evidenced for this finite inline support: group+component unique prefix and Work+component index already locate the native current component. Do not remove all-row aggregate rows using a first100 cap; current single component can legitimately contain many segments. Existing top-level5s timeout, SQL110000 array/reply guards, public120000 reply cap and Worker4s/131072 wire cap remain truthful refusal bounds. If a real component exceeds execution/byte bounds, return the existing typed failure, never a partial success or guessed total. Do not generate raw child arrays or whole-revision JSON in GROUP_LINES; detail remains independently paged.

## Reviewable scope

Keep exact ten Workbench read kinds, all existing eight issue amount fields, public shape/auth/private ACL and UUID cursor unchanged. Any additional scalar fields require the existing closed Worker/FE projections and finite source-gate maintained-template match to be updated to the actual narrow projection. No broad file waiver. No financial calculation in FE, pre-open publication, current-contract recompute, new schema/helper, Source producer, F25 parked protocol, future WB27 estimate or WB29/45 resolution is part of this support. Static design only; app/latency/mixed same-day journeys remain NOT_RUN.

## Source hashes
- `supabase/repeatable/10102026_1946_banking_pay_next_candidate_group_reader_v1.sql` — `c050f51fb7f132626bd479c87de127f78a41029ee16044d048d39af2e94b4e37`
- `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql` — `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`
- `supabase/migrations/10102026_1634_banking_pay_next_workbench_read_indexes_v1.sql` — `886cdd1d4333b05ec92ff6cf39b775bfe0a9f3225b02a8da2e237673b794b4b2`
- `supabase/migrations/03102026_1324_banking_pay_next_core.sql` — `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c`
- `supabase/repeatable/10102026_2019_banking_pay_next_current_payment_details_v1.sql` — `ee5f2d83158cb7bcb3afc331641051e2f531214782478f5ecb28d82458d6fe61`
