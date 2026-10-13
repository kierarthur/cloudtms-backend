# WB-39 current source investigation

12 October 2026. Read-only investigation for approved sequence116. Complete authority read: `02_WORKBENCH.md:1401–1426`, every current WB39 checklist row, R2 WB39:296–298 and R2-F05:112–118, shared Rule B/OR40/OR45, current final-definer registry and the supplied B9 SCR113/SCR115 specifications/inventory. Product/checklist edits, SQL/database/business/hosted calls, tests, fixtures, captures and commits **NOT_RUN**. Parent reports owner A2 closed97 at commit `954e23b8`; current registered0016 is retained. This note is not an implementation or acceptance tick.

## Exact scope and retained delivery

WB39 supplies **SNOOZES in the existing Workbench reader and the named action-handler module**. FC02seq117 alone renders the Payment Advances / Snoozes table. D01–D04 andD06 are explicitly CONTEXT/buildTask:false in the working checklist: current Case page/component page, public Workbench reader, native snooze schema and output card are shared provenance, not five duplicate rewrites. Their card semantics still apply to the new reader/handlers. D05 is the concrete frontend/join deliverable.

The full card1405–1424 requires business labels for Timesheet/shift/expense snoozes, indefinite management, exact allowed Case kind/version actions, confirmation before Unsnooze, created-time/ID paging, active/indefinite/dated filters and FC02 consumption. It does not authorise exports, another list renderer, a replacement selection/freeze process, Credit deferral, new financial rules or new command/job kinds.

| Current registered object | Physical final owner at this read | Retained behavior / required delta |
| --- | --- | --- |
| `private.bpay_next_current_case_page_v1(uuid,uuid,integer)` | `supabase/repeatable/04102026_0585_banking_pay_next_current_case_pages_v1.sql:6–38` | Bounded Candidate Case identity; reuse rather than rewrite. |
| `private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)` | `11102026_2041_banking_pay_next_case_pause_v1.sql:1861–1928` | Actual Case/Candidate validation, component/rule revisions and native fields. It is VOLATILE; do not put an unrestricted call to it inside the new STABLE Workbench branch. Exact point joins suffice for snooze rows. |
| `public.bpay_next_workbench_page_v1(jsonb)` | now `12102026_0041_banking_pay_next_snooze_management_seek_v1.sql:142+`; prior0454:682+ | Current registered SNOOZES dispatch retains the original public reader. Prior service-only/SNOOZE_CHECK/choice/estimate bodies remain predecessor provenance; the initial absence finding below is historical. |
| `private.bpay_next_snooze_management_row_v1(uuid)` | `12102026_0041_banking_pay_next_snooze_management_seek_v1.sql:5–57` | Current/supplied native labels through current→applied→stored expected revision; exact stored control identity remains separate. |
| `private.bpay_next_snooze_management_page_v1(uuid,jsonb)` | `12102026_0041_banking_pay_next_snooze_management_seek_v1.sql:59–140` | Closed native CREATED_ASC/DESC keyset, limit≤50, exact context and byte bounds; active management list plus exact inactive-ID recovery. Other future FC02 header sorts remain the matrix below. |
| `private.bpay_next_snooze_context_v1(uuid,jsonb)` | `11102026_0312_banking_pay_next_snooze_context_controls_v1.sql` | Current true read; exact supported Case recovery pairing73–90; current approved Work revision97–100; basis/active/version/warnings/control cut. Reuse for new/amend where current authority exists. |
| `private.bpay_next_snooze_action_v1(uuid,text,jsonb)` | `10102026_1100_banking_pay_next_snooze_v1.sql:163–328` | SET/ACK/CLEAR existing mutations, exact original replay189–194/219–224, Candidate and native scope locks, expected version/basis, immutable receipt. No replacement owner required. |
| `public.bpay_next_workbench_command_v1(jsonb)` | `11102026_0312_banking_pay_next_snooze_workbench_controls_v1.sql:799+` | Existing six command allowlist814 and readback836–850. Reuse route, request IDs, closed receipt and failure semantics. |
| Native tables/control/event | `10102026_1100_banking_pay_next_snooze.sql:7–60` | Work/segment/expense/Case exact scope, basis/version/sequence and immutable events already exist. No Credit alias or extra snooze table. |
| Current stale-cancellation guards/basis helper | registry `12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql`, A2 commit `954e23b8` | Parent reports completed A2 prerequisite. Retain its bounded existing POSITION_APPLY SNOOZE phase and per-event cancellation guards; do not rebuild it in WB39. |
| Existing refresh/due control | `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql:7–29`, due0255 | Snooze mutations already refresh exact native scope through the current dirty/ORIGIN path. Reuse; no new queue/cron/provider behavior. |

WB36 financial bases remain1236 and accepted snooze-at-cut remains1100. Case pause remains FC10/2041. FC01's current1939 Case-list owner carries Case labels and truthful totals; do not rebuild its liability calculations in a snooze reader.

## Supported actions and latest owner resolution

Card1413, native context0312:73–90 and shared window `FE/js/bpay-next-snooze-window.js:194–198` establish the exact recovery pairing:

| Actual owner/kind | New Snooze | Existing supported stored snooze |
| --- | --- | --- |
| LOAN/ADVANCE, genuine RECOVERY component | PAYMENT_ADVANCE_REPAYMENT (legacy LOAN_REPAYMENT alias remains canonicalised) | Amend/Unsnooze with exact snooze/version and current authoritative component/rule where needed |
| MANUAL_DEBT, genuine RECOVERY component | MANUAL_DEBT_RECOVERY | Same |
| OVERPAYMENT, genuine RECOVERY component | **No separate new button**; FC10 Pause recovery is the deferral | OVERPAYMENT_RECOVERY remains valid storage; carried over active controls can amend where current authority exists or be unsnoozed |
| CREDIT/UNDERPAYMENT, PAYOUT, unsupported pairing | None | Never treat as MANUAL_DEBT or invent a new repayment owner |

Supplied `SCREEN_INVENTORY.md:191` explicitly says Q-CREDIT-SNOOZE settled: no new Credit-payout snooze. SCR115.reviewNotes calls this an owner ruling and shows no Manual Credit snooze. Thus this target can proceed without another owner question; the approved card's earlier “Q” wording is historical. Preserve the actual stored supported kinds and future F19 carry-over handoff; WB39 performs no legacy business-row import.

New actions derive from the current actual recovery component, status/version and supported pair; not a Case parent's guessed UUID or browser kind-only inference. Existing snooze rows retain their actual stored kind. A stale/unsupported/closed current owner must give an honest absent/disabled new/amend action and reason, while native CLEAR remains available on an active exact old control. Do not alter allocation, schedule, pause state, holds, captured Case balances or existing accepted Drafts.

## Concrete frontend reuse and one management gap — initial checkpoint

The WB38 shared native window already exists and is included at `FE/index.html:3024`:

- `js/bpay-next-snooze-window.js` exports open/seed/validators367; seed194–204 supports exact CASE, Work, segment and expense identity.
- Its proposal288 correctly uses Case fields when WorkNULL. PRE_SAVE checks current basis/version/module292 and genuine CASE expected financial revision312.
- It reuses PRE_OPEN/PRE_SAVE warnings, inclusive London dates, note≤500, saved amend values, discard/clear confirmations283–301, exact retained unknown request journal265–281 and original readback-before-repeat. No new journal protocol is necessary.
- Whole/all-expense operations already use native MANY freeze/receipts; the supported handlers should delegate to the same saved owner, not construct every expense ID in the browser.
- Existing Workbench entry joins1242–1243/1404–1408 use this shared window and keep actual actor/NEXT/native-frame guards. They do not implement the named `bpay-next-snooze-actions` module or FC02 management entry.

**Important remaining gap:** the shared window unconditionally opens with SNOOZE_CHECK PRE_OPEN217, and its opening flow350–364 relies on that current authority. Work context0312:97–100 refuses withdrawn Work with current_revisionNULL/non-APPROVED. Therefore reusing that opener unchanged would leave an active indefinite withdrawn control impossible to unsnooze from its management row.

Native CLEAR already handles this correctly:1100:196–204 locates the exact control and Candidate;228–239 validates active/version and clears even on stale financial facts, explicitly commenting that it does not readmit the scope. Add a narrow management clear path using the fresh SNOOZES row's native snooze ID/Candidate/version, native confirmation and original retained CLEAR request/readback. It must not require a successful new/current Work PRE_OPEN check. Reuse the existing owned window/journal implementation where possible; do not weaken SET/context's current-approved gate. Amend on withdrawn/unavailable current authority can explain the refusal and offer exact Unsnooze; do not invent a new approval basis.

The named module should expose `openSnoozeFor(scope)`, `amendSnooze(id)`, `unsnooze(id)` to FC02, backed by fresh qualified native row/component data and the existing window. Make it injectable with actual document/window/actor/authFetch/api/native showReadWindow/current namespace, not hidden global actor inference. Preserve exact actor/agency/owner epoch, original pending bytes, safe close and already accepted result on reload. Its successful result must re-read only affected Case/Work/list scope through the current FC02 adapter, retain selection intent and show genuine Processing when original refresh is pending.

## Reader labels, action identity and bounded currentness

At the initial checkpoint no SNOOZES kind existed in Worker `broker/src/bpay-next-workbench.js:6` or FE Workbench equivalent, and no separate handler file existed. Worker GET handler651–675 already authenticated admin, injected actor, bounded bytes/deadline and validated the closed response. These are reuse findings, not a claim that the root's subsequent product additions are absent. Current registered0041 now owns the SNOOZES database branch on the same Workbench route.

For each native control retain internal exact snooze/version/sequence, Candidate/Work or Case/component/rule identity, scope/stored kind, active/date/note/created server timestamp and allowed actions/reason. IDs remain internal. Page scope/cursor must bind actor/agency/module epoch, exact filters/sort plus existing Workbench/adoption/currentness controls; refuse a changed context rather than continue into changed membership. Reader is write-free and does not mint warning ACKs or perform a projection rebuild.

The business source points are real:

- Revision labels at core1324:157–166 include Candidate/reference, Client, job title/band, Timesheet reference/week, approved total and timestamps.
- Work retains immutable week/origin and current/applied revision pointers. **R2:298 explicitly requires current/supplied business labels and withdrawn labels from the last applied revision.** Current0041:28–29 selects the same Work's sealed revision via `coalesce(w.current_revision_id,w.applied_revision_id,s.expected_revision_id)`: current first, last applied when currentNULL, stored expected only as the final native fallback. The earlier proposed expected-only display source was incorrect and is superseded. The stored `(work_id,expected_revision_id)` FK (snooze1100:10/29/34), stored physical/component IDs, worker-pay basis and control sequence remain the immutable control's own identity; displayed labels are not a rewrite of those operands.
- Current0041:30–35 resolves the selected display revision's exact native segment_key/component_key membership; if that member is absent, it falls back only to the control's stored approved_segment_id/expense_approved_line_id point. Thus supplied shift dates/clocks or expense category may follow genuine current/last-applied membership while stored control identity remains separate. Readable categories TRAVEL/ACCOMMODATION/OTHER/EXPENSES and Mileage are published at current1001:717–745. No history scan or today's Contract/Client inference is required.
- Approved_line stores evidence_ref, not a guaranteed free-text expense description (core188–211). The required narrow reader can show its genuine business category/Mileage and omit absent free-text detail. Do not expose the raw reference, fabricate “Train fare”, add unrelated TSFIN/source fields to match the picture, or infer labels from today's Contract. A future owner requiring a description must name its actual frozen source.
- For whole/aggregate covered membership, reuse complete existing counts/labels when available. Do not collect all approved segment IDs to decorate one control or describe a whole Timesheet as one guessed shift.

Card1419 does not require an amount, payout estimate, Case liability reconstruction or whole-list counters. Parent12October confirms the named reader provides only its control/revision/component facts: **omit unsupported Amount ex VAT and aggregate counts at FC02 under BD129**, unless FC02/FC01 explicitly owns the particular fact. Do not add money/totals to fill SCR113. No £0 substitute or browser sum.

## Sorting, supplied design and small supporting scope

Read-only supplied design viewed: B9 `SCR-113_timesheet_snoozes_dark.png` and `SCR-115_case_snooze_dark.png`; specifications/inventory113/113p/115 read. No pack file/renderer/spec was modified or run.

SCR113 is the future FC02 table: Candidate/Client labels, Timesheet week, What is snoozed (shift dates/times or expense text), existing amount, Until/Indefinitely, Note, Created and actions. Default Created newest first. Indefinite rows have Amend/Unsnooze here; expense More delegates Unsnooze all expenses; phone Open is44px, tap focuses and double-click/Enter opens native Timesheet. Those are FC02 table controls; WB39 does not add an interim table.

SCR115 reuses the shared date/indefinite/note/warning window for supported repayments. It illustrates weekly/outstanding/next due facts already owned by the Case reader; consume only validated fields actually supplied. No Credit payout deferral or early restructure/write-off feature is introduced to match a menu illustration.

Owner10October A3e/SDP34 supersedes “created only” as the eventual list sorting contract: every actual data column sorts **the whole server scope** ASC/DESC with deterministic created/id ties and correct NULL ordering. No loaded-row browser sort. SDP30 uses bounded server chunks behind continuous scrolling; action columns do not sort, nor fixed child/result rows. The actual SCR113 headers after the owner-authorised omissions are the following finite six-key future FC02 contract; Client and reference are subordinate Candidate/Timesheet text, not additional headers. Current0041 accepts only CREATED_ASC/DESC; the matrix does not claim the other five pairs are already implemented. Every future sort must use the exact same fresh current/supplied label source as the reader, not the superseded expected-only proposal.

| Actual data header / finite ASC and DESC pair | Native server sort operands, using the same source as display | Native access and required supporting scope |
| --- | --- | --- |
| Candidate | Work: selected current→applied→expected display revision `candidate_display_name COLLATE "C"`; genuine Case row: current native Case-list key's Candidate label (0041:18/51). No UUID-as-name fallback introduced. | Native Work/revision and Case-list-key PK points. Existing WB10 summary-name indexes do **not** cover this joined selected-revision/control order. Whole-scope residual label sort is needed. |
| Timesheet | The same selected display revision's `week_ending_date`, then its `timesheet_reference COLLATE "C"` with explicit NULLS LAST. A Case row has no Timesheet; NULL remains honest. | Exact current/applied/stored revision PK point. No Timesheet/Contract history scan. Joined ordering still requires residual sorting across the qualifying controls. |
| What is snoozed | The reader's supplied business scope/category label plus native work date/local start/end where displayed as the exact shift's secondary facts. Whole Timesheet and all-expense scope use their truthful scope labels; no guessed membership. | Selected display revision's segment_key/component_key point, with the exact stored native ID fallback only when that member is absent (0041:30–35). Use this same fresh expression for cursor comparison and ORDER BY; do not sort stored-old detail while displaying current detail. |
| Snoozed until | Native `until_date`; explicitly choose NULLS LAST for Indefinitely on both ASC/DESC, rather than converting it to a fabricated date. | Existing active due index1100:50 supports dated seeking but lacks the chosen created/id tie and indefinite branch. A narrow additional native `(until_date,created_at_utc,id)` read index is justified only for the actual mixed-direction key used; matching partial predicate must not silently exclude indefinite rows. |
| Note | Native `note COLLATE "C"`, unchanged; empty note is an actual empty string, not NULL. | Native length≤500 characters1100:21 makes a narrow note/created/id B-tree feasible, unlike 8KiB labels. Add only the direction/predicate actually required; no derived note store. |
| Created, default newest first | Native `created_at_utc`, then actual control `id`; direction is explicit. | New native snooze created/id index supplies the required default seek. Optional Candidate-leading variant is justified only if the actual Candidate filter requires it. The database is already agency-bound; there is no snooze agency column to invent. |

For non-Created sorts, preserve one declared deterministic tie such as `created_at_utc DESC,id ASC` and implement its exact mixed-direction seek. A backward scan of an ASC index reverses **all** directions; it is not evidence that one index covers both primary directions while keeping those ties unchanged. NULL flags/secondary values belong to the closed cursor only when they are actual sort operands. Do not invent a Client sort for a sublabel or an Amount sort for an omitted column.

The smallest honest query plan first qualifies the native active/indefinite/dated scope and exact Candidate filter, obtains one row per control through the FK/PK point joins, and performs the selected **static** whole-scope ORDER BY/keyset comparison before LIMIT≤100. Native created/date/note indexes can improve their paths; native active-scope/work indices1100:45–49 and seed1429:21 constrain membership, and A2's0016 active `(work_id,id)` index remains its separate publication seek. Label joins must not fan out to every shift/detail or historical revision. Choose exact native segment/detail binding, not LIMIT1 on an ambiguous match.

There is **no existing single covering B-tree for the selected current/applied revision label plus another table's snooze created/id**. Adding a broad revision-label index or claiming the current metadata index covers that join would be misleading. Correct joined Candidate/Timesheet/What sorts require residual whole-qualified-scope sorting with potentially nonconstant input work; output and each join remain bounded/point-based, but LIMIT100 does not prove only100 native controls were inspected. This is source-known cost, not a timing result. No cross-table projection table, public trigger, generic sort framework, monetary read key or semantic truncation is proposed. FC01's prefix/full-name technique illustrates an honest residual cohort cost but does not magically cover this different join, and 8KiB full-label B-trees must not be introduced.

Retain the existing context contract: public0454:736–741 checks admin/NEXT/agency;819–822 binds actor/agency/epoch/official date/Workbench and adoption revisions plus exact normalised scope;826–834 refuses non-CURRENT/dirty/pending projections. Snooze mutation1429:12–23 already schedules native Work ORIGIN or Case refresh; metadata currentness remains existing1252:22–27/1429. Include this finite sort/filter/last native tuple in the usual bounded cursor and compare exact context at every page. No new counter, hash/digest, protocol or browser page-array is needed. Large text cursor values need existing byte-safe closed transport, never truncation; current FC01 demonstrates canonical Base64 UTF-8 cursor-only transport if required by the existing byte bound, without altering displayed/sorted text.

## Current0041 exact-ID recovery amendment

Registered current row/page/public signatures are all `12102026_0041_banking_pay_next_snooze_management_seek_v1.sql`; observed read-only SHA256 `7AC799D177F99CF912B3021DD6BB6CE3EBC5B90B7C13C88ADD4C0100F1C0C0B8`. This records source provenance, not runtime acceptance.

0041:70–82 accepts a closed optional `snooze_id` and forbids combining it with `after`. Normal list scope remains active, but the exact supplied ID allows that one inactive control through `(s.active OR snooze_id IS NOT NULL)` together with `s.id=snooze_id` (103–105/116–118); Candidate/Client/scope/filter predicates still apply. Actor/admin/NEXT/agency and the usual raw scope are qualified; context87–88 includes Workbench/adoption revisions and native command control_cut. This is a bounded native recovery point for readback after an accepted CLEAR/cancellation has removed the control from the active list, not inactive-history enumeration or permission to unsnooze it again.

Row0041:45–55 retains stored expected revision separately, exposes current revision only when it is genuinely the selected current pointer, and supplies fresh physical/label fields. `can_unsnooze=s.active`; inactive recovery exposes no new clear capability. Work amend37–39 requires APPROVED/current and exact current worker-pay basis equality; withdrawn Work can retain supplied last-applied labels and exact active CLEAR without pretending that it is amendable. Existing immutable CLEAR receipt/readback remains the accepted-outcome authority.

## Original minimum build map and boundary — historical checkpoint

1. New SNOOZES service-only STABLE read branch in the actual public0454 owner, with bounded keyset/raw row and byte behavior, exact business labels/action versions, supported filters and currentness/refusal; preserve every unrelated original branch.
2. Necessary read indexes and complete Worker/FE closed SNOOZES request/row/cursor validation on the existing Workbench transport.
3. Named snooze-actions thin adapter to the current shared window and direct stale/withdrawn-safe native CLEAR seam, with server allowed-action pairing and confirmation.
4. Register successor provenance/contracts and record only these card changes; FC02 renderer/summary/exports, later Case features, F19 import and any undefined Credit capability remain their named owners.

Policy X: no drift. Rule B accepted scope and money remain unchanged; snooze changes affect future eligibility through original controls. A2 cancellation is retained completed prerequisite work at `954e23b8`, not a WB39 money rewrite. All future runtime/browser/database/performance checks remain **NOT_RUN**.

