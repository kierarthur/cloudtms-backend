# WB-39 reader/sort boundary review

12 October 2026. Independent read-only investigation, Sol6.1 Extra High. Full WB39 card `02_WORKBENCH.md:1401–1426`, all current checklist rows, R2:296–298/F05, current0454 Workbench owner, native snooze/revision schema and supplied policy SDP30/34 checked. No product/checklist edits, database/business calls, tests, fixtures, rendering, hosted actions or commits.

**SCOPED DESIGN PASS for root's revised sequential boundary:** WB39 supplies the card's created-time/ID reader, exact management row lookup and D05 handlers; FC02 immediately next supplies every actual list column's whole-scope server sort/keyset/index before its own completion. WB39 has no renderer. No shadow immutable-label projection is justified in this batch. This is not a claim that Candidate/week/What/Until/Note sorting is already implemented.

## Exact scope and prerequisite

Card1419 explicitly names SNOOZES in the existing `public.bpay_next_workbench_page_v1(jsonb)`, created-time/ID paging, native revision/component business labels and active/indefinite/dated filters. Card1409/1421 makes FC02 the sole renderer and WB39 the callable action owner. D01–D04/D06 are current context rows; D05 is the named handler deliverable. Add no interim table UI, amount/totals, liability calculator, export, new financial command or Credit/debt alias merely to match a supplied picture.

SDP34 (`SCREEN_DESIGN_POLICY.md:172`) requires each actual data column to sort the whole server list with its real keyset/index; sorting loaded rows is forbidden. Root's latest direction applies those extra keys when FC02 builds/touches the actual list, immediately after WB39. Record this as a hard FC02 completion prerequisite. Candidate/Client, week, What, Until, Note and Created need ASC/DESC support when those columns exist; action columns and expanded children are exempt. Do not defer those keys to final integration, advertise unsupported sorts in WB39's closed union, or mark them complete from a created-time index.

The actual current0454 boundary is STABLE/service-only (`11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql:682–712`), already has SNOOZE_CHECK714–723, and authenticates active admin/NEXT/current namespace in its ordinary read branch735–741. SNOOZES is absent744–745. Add its exact read branch without changing unrelated choices/estimate bodies or granting a new browser RPC. Exact point management can reuse the native snooze PK and the same actor/NEXT namespace; it does not need an agency list scan or a current-approved Work context to identify a retained control.

## Small necessary native read support now

Current snooze1100:45–50 indexes active scope, Work scope, history and due date. The A2 `(work_id,id) WHERE active` index supports Work cancellation, not agency Created paging. None has the complete created-time/ID pathkeys.

For the active created-newest list, a native index on `(created_at_utc DESC,id ASC) WHERE active` matches the complete page order. Date-type filters remain native `until_date IS NULL` / `IS NOT NULL`; if separate indefinite/dated branches must have their own seek without scanning the opposite cohort, use the same small created/ID partial index for each exact predicate. Add only the branches actually admitted by the closed reader. Existing `(until_date,id)` due index is not a substitute for a created-time order. If a later screen admits inactive history or ascending Created, its exact predicate/direction must have its own named support then.

The next seek is lexicographically `(created_at_utc < after_created) OR (created_at_utc = after_created AND id > after_id)` for DESC/ASC ties. Apply native filters before the ordered `LIMIT limit+1`; never limit a candidate prefix and then sort only those rows. Use actual finite server timestamps/UUIDs, deterministic NULL handling where allowed, and the existing byte/page/context refusal boundaries. Read full labels only for the selected bounded native controls. Point-management requests use an indexed exact ID lookup, with row Candidate/scope/version checked, not label guesses.

## Labels must stay native and coherent

R2:298 requires current/supplied business labels and **withdrawn Work's last applied revision**, not NULL current. Work has current/applied pointers/core1324:111–129; immutable revision132–176 supplies Candidate/reference, Client, Timesheet reference and week. Snooze's exact segment/expense foreign keys1100:14–15 provide retained covered-scope facts. Missing native description is an honest known category/unknown detail, not a raw evidence UUID, invented description or whole-history reconstruction. Current pointer is required for admitting Amend/SET; exact Unsnooze can clear an active retained withdrawn control without readmitting its Work.

A fixed per-control key maintained only by a snooze trigger cannot replace those current/applied joins. A charge-only publication can preserve the snooze while selecting another revision's labels; withdrawal can change the applied fallback without updating the control. If displayed labels change but the old key remains, sorting is false even if its B-tree is fast. An agency revision refusal alone cannot regenerate that key. Root has rejected this shadow projection for WB39.

## FC02 joined-sort design conditions

Before FC02 implementation closes, save its exact query/index matrix for the actual columns. Native dates/note/created support is straightforward. Joined Candidate/Timesheet/What labels require an actual ordered input path or a narrowly maintained native read key whose invalidation covers the label source. Do not claim that a PK join or an index on a different tuple orders the whole joined list.

A direct immutable revision-label prefix index plus exact current/applied Work joins can preserve semantics, but includes irrelevant historical revision cohorts; its cost must be stated honestly and it must not become an agency-history fallback/materialisation. The native Work schema currently supplies Candidate/week indexes, not global selected-revision label ordering (`core1324:994–1002`). Any required reverse current/applied pointer support or maintained current-label point must be explicit, private and limited to these read facts. A per-control projection with only snooze invalidation is insufficient; no broad public Candidate/Source producer rewiring follows from that finding.

FC01's approved prefix/full-value residual pattern (`11102026_1738_banking_pay_next_case_list_indexes.sql:24–27`, corresponding reader323–329) is a precedent for legal8KiB labels, not proof for WB39's different joins. Index only a safe prefix, compare/sort the full native value within the residual cohort, retain created/ID ties, and report the cohort cost. No lexical truncation, UUID replacement, fixed arbitrary first-N cohort or universal timing claim. Canonical cursor encoding and assembled-byte limits must handle full values when FC02 adds those sorts; a small Created/UUID cursor does not need that machinery now.

The revised ordering removes an unnecessary storage/projection decision from WB39 while leaving the full screen obligation concrete at the next owner. Source/currentness/label gaps, joined-sort performance and all application journeys remain **NOT_RUN**; this note does not tick WB39 or FC02.
