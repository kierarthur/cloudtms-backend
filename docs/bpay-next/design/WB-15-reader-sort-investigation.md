# WB-15 indexed reader sorts and native ROW scope

Read-only investigation, 10 October 2026. Authority: full WB-15 card/checklist, WB-16/WB-26 interfaces, R2-F09/RQ-406 and SCREEN_DESIGN_POLICY **SDP-34**. No product edits or execution. Current common reader/public wrapper is `10102026_1806_banking_pay_next_issue_reason_reader_v1.sql`; the Candidate wrapper is 1634. WB-21 1832 remains the accepted-intake owner and is not rewritten by this support.

## Finding and smallest complete support

The current group table stores week/channel and native parent identities but **does not store** Client text, deductions or amount/unknown keys. Its contribution routine calculates per-group typed deltas; the maintained Candidate/scope summaries aggregate away group identity. Current 1806 first selects ten parents, then calculates those presentation fields by a lateral member aggregate. Sorting that result would sort only the loaded ten. Moving every member aggregate ahead of LIMIT would make the reader reconstruct all Candidate groups on each read and still lacks the SDP-34 matching index.

Root's proposed **one narrow private maintained display-sort table**, keyed `(presentation_type, native_id, section)` with exact `parent_group_key` FK, is the smallest coherent support that also handles ROW. It is a read projection of already maintained items, not a new financial parent, command kind, producer protocol, money owner or ledger. Preserve existing WORK/CASE group/item checks and FKs. No read-side writes, new public triggers or financial reconstruction are needed.

Rows refer only to: Work ID for Timesheet; finance Case ID for an Overpayment group; Case-component ID for a standalone ROW. Keep `candidate_id`, native Case/Work/component IDs, Client/target channel and the existing financial/projection/metadata revisions for exact joins. Public ROW `group_key` can be null as R2 permits, while the private `parent_group_key='CASE:'||case_id` remains required. Do not turn a display key into a newly generated economic UUID.

## Finite stored keys and indexes

Retain only the required already-produced display keys per actual section:

| Column | Sort key before native tie-break | Actual source |
|---|---|---|
| Payment | null-week flag, native week, presentation type/native ID | Existing group/work week; actual Case row has null week unless a genuine existing date is explicitly used. Do not guess a week from TODAY or label text. |
| Client | null-label flag, exact Client text COLLATE C | WORK already captures `work_revision.client_display_name` into items; no browser/live-name fallback. Case currently has no populated item Client label. |
| Pay method/channel | null-channel flag, target channel COLLATE C | Existing maintained group/item target channel; retain source channel separately as provenance. |
| Deductions | exact boolean | Same actual scoped member predicate used for the displayed Yes/No; not negative-money inference. |
| Amount | unknown/null flag, exact numeric value | Same exact stored member sum/amount represented in that displayed section, with semantics kept separate below. |

Append the RQ-406 tie-break `(week_is_null, week_value, presentation_type COLLATE C, native_id)` to non-Payment keys. Native UUID is the final tie-break; do not use random `workbench_item.id` as standalone row identity (items are rebuilt). Store key values in their PostgreSQL types; cursor transports decimals/bigints as text and UUID/date/boolean as their existing closed wire types.

Add one exact matching Candidate/section-prefixed B-tree for each of the five data-column tuples. Ascending and all-component descending can use the same index backwards. Existing `bpay_next_wb10_group_week_idx` supports only the old parent week order, not Client/channel/deductions/aggregate amount or the ROW union. Preserve the existing Candidate/client/channel scope filters and assess matching filter-prefix variants only where the actual route requires them; do not claim runtime/index-plan proof from this source note.

The amount key must sort the amount actually shown. Existing group amount is READY GROSS_ADD included SOURCE; READY NET additions, gross/net debt balances and Waiting amounts are distinct facts. Never combine GROSS/NET/debt or show known debt as an estimated recoverable amount. Until WB-27, recoverable remains null; its real outstanding balance may be separately displayed/sorted only under that explicit label. Unknown key uses a deterministic numeric placeholder solely behind its explicit null/unknown discriminator; transport/display remains null, not hidden zero. A ROW payment uses its actual component/stage and maintained amount; an Overpayment parent's amount/count is the complete relevant component sum, not the first child page.

For each sort, close the same finite selection in four places: common SQL tuple/order/seek/key; public wrapper allowed sort and exact key-type/cardinality; Worker `validKey/rowKey` and kind-specific sort allowlist; FE mirror/headers. Use closed named Client/channel sort tokens scoped to CANDIDATE_GROUPS rather than abusing Candidate NAME sorting. Default Payment order follows genuine newest week with deterministic null ordering. Sorting resets the window to top per SDP-34. Bound cursor to actual kind, Candidate/client/channel/section/sort/context and verify it belongs to this exact current result before seek.

## Complete ten-parent boundary

Read indexed display rows in current exact scope with `LIMIT p_limit+1` (`p_limit<=10`). The table represents complete parent/ROW membership and retained section sums; choose the ten parents before independent `GROUP_LINES` paging. Enrich only those exact selected native identities with bounded metadata/facts. Never let a missing or corrupt enrichment silently drop a parent after it has consumed one of ten slots: return the existing typed unavailable/refusal and preserve the previous UI page. A Ready row additionally requires actual parent complete/non-dirty/current status, current matching projection versions and a genuine selectable Ready member; merely retaining an old display-sort row is insufficient.

Refresh the old page anchor under one context and adopt the replacement once. Child cursors are independent of parent paging. Continuous scrolling can retain more than one SQL page while each request remains ten complete parents; do not restore Previous/Next controls. Existing workbench/adoption/dirty barriers remain authoritative.

## Native standalone ROW proof

`04102026_0300_banking_pay_next_case_instruction_core.sql:68–130` gives immutable native component identity and compound FK `(id,case_id,candidate_id)`, unique Case/component-key and ordinal, exact kind/subtype, instruction kind, direction and stage. There is **no one-component-per-Case constraint**. The real 0330 create handler inserts a PAYOUT component and an additional RECOVERY component for LOAN/ADVANCE; CREDIT and MANUAL_DEBT have their actual typed component. Therefore one non-Overpayment Case cannot be treated as one standalone row by assuming cardinality one.

Use actual `case_component.id` as ROW ID for each non-Overpayment component, retaining its actual Case and rule identity. Overpayment uses actual `case_id` parent with complete component membership. Classification comes from constrained native `case_kind/instruction_kind`, not display words or a guessed sign. Maintain no extra summary copy of the same Case alongside its ROW components. If the same component has AVAILABLE and RESERVED portions, aggregate by its native component and requested section rather than emit a duplicate financial row. `GROUP_LINES` for ROW resolves the retained parent Case key **and exact component ID**, so it cannot return sibling payout/recovery components under one row.

Native Case FK and current projection state remain prerequisites. CASE-component amount/counters remain the already existing 1252 projection output; do not call shared target valuer or Case allocation from this new display helper. Future WB-26 default ticks/case-state fields remain their named owner, though the native identities must be correct now.

## Exact writer seams and metadata

The display helper should consume the completed actual item set while the existing Candidate/Work/Case and projection locks are already held. No additional command clock is acquired.

| Current final owner | Exact seam |
|---|---|
| `private.bpay_next_workbench_refresh_work_core_v1(uuid)` in **1438** | After the final item rebuild, counts/stale-snooze update and established contribution delta; same native Work/group. The withdrawn branch is inside this routine and flows to the same final tail. |
| `private.bpay_next_workbench_refresh_case_v1(uuid)` in **1252** | After complete actual component/portion rebuild and group count/contribution update; same native Case. It may produce several ROWs for the genuine components. |
| Core 1438 early Processing RETURN (`:30–41`) | It mutates existing items/group without rebuilding; old display rows must be invalidated or excluded through status/version equality before LIMIT. Do not keep stale Ready selectable values. |
| `private.bpay_next_workbench_mark_processing_v1(uuid)` in **1346** | Replaces items with a nonselectable GROUP_EVIDENCE and Processing group. Apply the same finite invalidation/refresh policy. This is an actual writer separate from the final core tail. |
| 1429 task `METADATA_GROUP` branch (`:471–487`) | Actual display-only mutation updates Candidate name/reference and group/item metadata/projection versions. If the new sort table copies those versions or Candidate labels, update/refresh its rows at this existing tail. If Candidate labels are joined from current metadata after LIMIT, retain that exact join and only synchronize whatever versions are stored. |

**No actual Client-label update exists in the metadata continuation.** It updates Candidate display/reference, not Client display. WORK item Client text is the captured revision label at 1438:97/126, so Candidate rename does not change its Client sort key. The Case writer 1252:480–486 obtains Client/contract via genuine `work_collection → work → contracts`, but never populates `v_item.client_display_name`. Preserve null as absent unless a genuine bounded native source is deliberately added: `work_collection.original_revision_id` is an actual compound-FK-bound revision whose `client_display_name` exists, and can label that original origin without using today's Candidate. Do not introduce a new Clients trigger or refresh all agency history as this card's support.

Existing seed/task lanes already call the real Work/Case refresh owners. They can populate the added read projection through those hooks; do not create another seeding protocol. Before reading CURRENT, missing rows must not be presented as a genuine empty Candidate: preserve existing adoption/dirty barrier or a finite explicit unavailable condition until that actual projection is present. This note does not authorize a migration data sweep or fabricate application proof.

## Current source identities

| Source | SHA-256 |
|---|---|
| 1252 group/item/summary migration | `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab` |
| 1332 final typed group-contribution function | `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391` |
| 1438 current Work/core/refresh-many | `b8d445c4a4cfc02978e8ac77a159350b0e1978aaff9a1172da8e7e157a6414f5` |
| 1429 current metadata/task continuation | `dc539089fae0cd1a8baf4765a89cea9ff1d1e8039200ecda5d80371dda2335dd` |
| 1634 matching existing WB10 indexes | `886cdd1d4333b05ec92ff6cf39b775bfe0a9f3225b02a8da2e237673b794b4b2` |
| 1806 current common reader/wrapper | `bbde6d21a6aad4eeb5bfa0741976246f58fbdebd6410c163d759fcf3321c8329` |

This is necessary WB-15 read/index support. It leaves native money owners, Source publication, Rule-B captured selection/PIN and parked F-25 protocols unchanged. Actual local checks/application/screenshot verdicts belong to root's subsequent implementation and review.

## Final root choice / narrower validity closure

Root chose **no copied Candidate/projection/metadata versions or Candidate labels** in this cache. Current `g`/`m` are joined after the bounded ten-row selection. Therefore the existing display-only 1429 METADATA_GROUP owner does **not** need a cache hook merely for Candidate rename/reference/version; its control-changed branch already calls the actual refresh owners. WORK Client remains captured item/revision text. Case Client may be copied from the actual bounded native Case→work_collection→work→contract→`public.clients.name` lookup, preserving a genuine null when absent. Do not add a Clients trigger. If that label is cached, render that same retained label, not a newer post-limit Client name under an older sort key.

The existing public reader already refuses every non-HEADER read when adoption is not CURRENT or dirty Candidate/group/pending-task counts are nonzero (**1806:584–586**). Both core's early Processing branch and 1346 mark_processing increment the real dirty-group barrier before their mutations; they do not allow a CURRENT reader to expose old cached Ready rows. Final core/Case helper refresh occurs while the same writer locks are held and before the real continuation clears its barrier. Keep the actual parent complete/current/non-dirty predicate as well. With that existing global barrier preserved, no invented cache-generation protocol or second processing owner is required. Independently verify that every actual completed native refresh tail calls the helper; display-sort metadata must never outlive its genuine current item rebuild.

Root also chose one initial `INSERT ... SELECT` from the **existing maintained group/items only** into this new private read cache in the additive migration, using the identical typed projection selection as the regular helper. This is permissible narrow read-index initialization, not a TSFIN/history/base-data backfill or another seed job. It must populate all true native rows/sections and fail atomically on incomplete/wrong identity rather than silently make a missing cache look empty. Preserve owner-only RLS/ACL and no browser/service table grant. Its actual SQL/source gates and installation remain root's work; no initialization was executed by this reviewer.

### BD071 design review

Fresh-read `WB-15-candidate-window.md` SHA-256 `84bf0ed37c95d12c32d26536738ad78ba531ca32b161f0ffca7377c7815641ba`. **Source-design scope PASS; no concrete scope contradiction found.** It correctly retains D02 factual Case owner, D03 exact Office journal, D07 Candidate child, real native ROW parent, one existing choice namespace, separate monetary provenance and future-owned controls/application acceptance. This is not a product SQL or screenshot verdict.

Three finite implementation obligations remain: (1) after-limit enrichment must preserve each of the ten selected identities, refusing incomplete/mismatched data instead of silently filtering them out; parent READY predicate and existing global dirty/adoption barrier remain strict; (2) initializer and helper must use identical section/native membership and null/unknown rules, with no duplicate CASE plus ROW contribution for non-Overpayment Cases; (3) accepted run/command fields are populated only from one genuine qualified native pair and selected amount remains null whenever exact retained provenance is unavailable. No MIN/MAX UUID selection of competing accepted runs. Those are constraints within BD071, not additional owners or protocols.
