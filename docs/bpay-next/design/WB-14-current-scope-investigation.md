# WB-14 current scope investigation — sequence 61

Read-only source investigation. Product, runtime, app, SQL and tests were not changed or executed. The saved source is authoritative; these hashes are the investigation snapshot, not an installed or in-app receipt.

## Exact contract and scope

Read the complete approved r3 `02_WORKBENCH.md:591–614`, all three current WB-14 checklist rows, master sequence 61, R2 issue-detail requirements (lines 196–198), owner A3/A5 and indexed design policy. Principal WB-14-D01 is the five-column reason-driven issue detail in `js/bpay-next-workbench.js`; D02 is output-only context. The exact five columns are Candidate, Payment, What needs attention / Why it is blocked, Amount, Actions. Preserve all **real applicable** retained owner controls, the correct native identity/action family, actual nominal amount when recovery is zero, Refresh for this kind, Close/Escape, and parent list position. No Rates, snooze, exclusion, hold, Case, Source or other money producer is authorised to be invented by this screen card.

Read all four current JSON specs and rendered design references: SCR-05 `T04b_action_required`, SCR-06 `T04c_blocked`, SCR-08 `SCR-08_issue_detail` and SCR-08b `SCR-08b_issue_detail_pay_query`. SCR-05/06 are the issue lists; SCR-08/b are the child detail. Mock counts, rates, times and suggested actions are design examples, not source facts. Actual visual/app acceptance remains NOT_RUN.

Owner A5 makes the old WB13 issue-list debt part of this touch: ordinary wording, server header sorts, continuous bounded scrolling, single-click highlight, double-click/Enter opening and phone Open. Actions is a sort exclusion. This is explicit A3e supporting work, not an independent feature. Previous/Next *payments* in the card must not turn into numbered pages or Previous/Next *page* controls prohibited by the current owner instruction.

## Current winners and confirmed gaps

| Object | Current physical winner | Required remaining support |
|---|---|---|
| `private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)` and `public.bpay_next_workbench_page_v1(jsonb)` | `supabase/repeatable/10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql` | Genuine native affected scope/count; retained display fields; issue reason sort. Preserve WB12 counts, eight amount fields, all ten kinds, existing bounds/ACL. |
| `private.bpay_next_workbench_issue_detail_page_v1(jsonb,jsonb,integer)` | `supabase/repeatable/10102026_1634_banking_pay_next_workbench_read_limits_v1.sql` | Existing wrapper delegates to current common helper; do not duplicate/rebuild correct wrapper unless scope signature really changes. |
| Issue screen/module | FE `js/bpay-next-workbench.js:738–809` | Actual child window absent; list currently uses technical copy, dropdown sorts and page buttons. Existing actor/context/exact retry and bounded cursor protections must survive. |
| Actual Office join | FE `js/bpay-next-office.js:560–571`, `js/main.js:7159–7173` | Current fallback closes Office, then opens Timesheet/Candidate. Replace the actual hook with a trusted current actor/client/lifetime/standard-window child and retain parent. |

Current common reader lines 166–168 filter exact `group_key`, optional `item_id`, and `reason_code`; public lines 546–548 require group/reason for ISSUE_DETAIL. Lines 440–443 compute only EXISTS and line 479 returns 0 or 1 as `affected_candidate_count`. The current native WORK/CASE group is one Candidate, so this cannot truthfully produce the card's three-Candidate shared Umbrella cause. Passing `item_id` as the entire detail scope additionally narrows it to one row; it should be an exact anchor validation, not accidentally the whole affected set.

Do not broaden by reason alone. PAY_QUERY_OPEN, hold, missing approval, recovery and rates can have unrelated causes with identical reason text. A new free-standing issue UUID or browser-selected list of Candidate IDs would also contradict the card.

## Finite native cause support: actionable boundary

There **is** genuine retained ordinary source Umbrella evidence: migration `10102026_1009_banking_pay_next_ordinary_rate_authority_v1.sql:20–26` adds `work_revision.source_umbrella_id`; the actual ordinary producer captures it from the original financial snapshot/candidate authority and rejects an unexplained missing UMBRELLA source identity. Do not infer a historical Umbrella from today's Candidate. Current maintained target identity is `workbench_candidate_metadata.target_pay_channel/target_umbrella_id/target_umbrella_vat_chargeable/target_umbrella_enabled` (1429 migration lines 28–35), with an existing partial `(target_umbrella_id,candidate_id)` index. These are the concrete source/target IDs to use, rather than a fabricated issue identity.

Final evidence / builder recommendation: `source_umbrella_id` is retained provenance, not an issue-change event or shared cause certificate. Current maintained target identity similarly proves today's adopted target, not which particular shared change created the issue. The current final origin classifier 1438 and Case classifier 1252 do not emit literal Umbrella-change / `RATE_RESOLUTION_REQUIRED` / `GROSS_RESOLUTION_REQUIRED` rows. The existing native Work is one Candidate (1324 lines 111–129), native Finance Case is one Candidate (1324 lines 355–357), and Workbench group is one Work or Case plus one Candidate (1252 migration lines 32–65). No existing Case identity can span three Candidates. No stored shared issue/cause relation was found in the current native projection, rate-resolution or metadata shapes.

Therefore **do not use the source/target Umbrella pair plus reason as a replacement shared-cause identity**. It would still aggregate unrelated changes/decisions. Do not invent a new issue UUID, group by same reason/target, borrow the parked F25 event machinery, or publish a mock three-Candidate group. Only the specific three-Candidate Umbrella Done journey has missing genuine source evidence and remains BLOCKED; this does not block the five-column window, exact group reader/count, real control joins, A5 corrections or the other source implementation work.

Smallest current support: keep exact native WORK/CASE `group_key` + reason scope, validate optional item as the precise opening anchor rather than restricting all detail rows to that item, preserve explicit parent client/channel/Candidate filters, and compute the actual distinct-Candidate count over that exact whole scoped group (not from current page). It truthfully returns zero/one today. Preserve each actual Work/Case/item ID, nominal amount and appropriate action owner. Do not add a shared cause index to make unsupported fanout appear real. If a later named rates/classifier owner supplies a genuine retained shared cause, that owner can expose its exact relation and this reusable detail reader can follow it without a new Banking identity.

## Display data and Source query support

Current ISSUE_DETAIL has the native IDs, Candidate display/reference, Timesheet reference/week, reason/hold/snooze/query state, nominal/approved/reserved/deduction/recovery/included/gross-due fields and direction/payroll stage. Current maintained item also has `client_display_name`, `component_kind`, `tax_treatment`, `work_date`, `realised_source_ex_vat` and genuine line/segment links, which the issue transport omits. Small finite explicit projections of those fields can supply actual Payment/Already paid context without calculations or a new reader kind. Client and human Timesheet reference are display; UUID/component key remain internal. Unknown money stays unavailable, zero stays a genuine server zero, and zero recovery may use the genuine server nominal/unresolved field as directed by the card.

SP-13 `private.weekly_source_pay_query_open_display_v1(uuid,integer,text,uuid)` is explicitly registered for the future WB14 consumer. Its physical 0956 reader is STABLE and owner-only; no public exposure, gate, admission, clock or mutation. It returns six keys `ok/code/scope/queries/complete/next_cursor`, with eight row fields `query_kind/query_id/reason/opened_by_user_id/opened_by_name/opened_at_utc/work_date/evidence_available`. Manual rows have the real stored reason, actor name and opening time; incidents deliberately have NULL actor fields and real created time/issue-family evidence. Missing evidence remains explicit, and real owning rows are retained. Never print actor UUID as a name or invent today's Office actor.

This is genuine supporting projection **only for an actual PAY_QUERY detail**: a correct SCR08b detail cannot invent its query reason/raised-by/time from `query_state`. Resolve Source root from the exact item's retained revision/physical Timesheet; 1438 lines 206–216 already verify canonical root/family/Candidate/client/contract/week/version equality against this retained approval. Query display is informational, not a new blocking classification or receipt exemption. Add it to existing ISSUE_DETAIL with closed typed validation/bounds if the actual detail uses those facts; do not add it to every read/header/list. Preserve the root-scoped native `(kind COLLATE C,id)` cursor. Do not present the first 100 queries as complete when the actual reader returns `complete=false`, and do not use display completion as query clearance. Do not broaden manual resolve or Source writers.

## A5 list/detail support and indexes

Current issue sorts are NAME, PAYMENT and AMOUNT only; NAME uses C-collated name + Candidate UUID + item UUID, PAYMENT reference + item UUID, AMOUNT null flag + nominal + item UUID. Current 1634 migration already has these exact issue indexes plus group/reason support. Add one finite REASON/WHY sort for Action/Blocked/detail with identical order/seek/cursor/validation semantics in SQL, Worker and FE. Sorting by stored reason priority/code must be labelled honestly; do not sort translated display sentences in the browser. A narrow `(section,reason_priority,reason_code COLLATE "C",id)` index is the natural new support only if this is the chosen actual tuple. Keep all sort directions and null/UUID semantics exact.

Reuse the existing bounded reader path and compact three-page/32KiB retained-window approach already present for Ready/pickers. Preserve exact failed request bytes and scroll on Try again; context-stale resets need explicit Refresh. Every late publication/finally/focus must require captured actor, ticket, context and actual frame liveness. A live but non-top parent may adopt child results without stealing focus from that child. A new screen cannot just call global `openIssueWindow(issue)` without the actual trusted actor/client/window adapter and parent lifetime.

Use the existing standard window adapter/real `bpay-next-read` frame stack, wide/phone semantics and Escape rules. Single-click highlights one row; double-click/Enter opens once; embedded actions stop propagation; phone exposes an actual Open control. Sortable headers are buttons with aria-sort; Actions excluded. No technical payment-engine chatter, SOURCE/frozen/component keys, mock Updated times or direct service RPCs. Render amounts using the existing UK currency formatter without changing any financial scalar.

## Retained controls and exact owner boundary

| Applicable control | Genuine owner/reuse | WB14 requirement / later handoff |
|---|---|---|
| Timesheet / Source checks | Existing Timesheet owner; current main `openWeeklySourceApprovedHoursModal` at 178888 requires its genuine owner-built view model and authorisation; do not fabricate that view model from a Banking row | Open exact retained physical Timesheet through trusted owner callback. WB17 owns its later focused Timesheet/return integration; preserve parent now. |
| Candidate / bank-warning chip | Existing Candidate owner and current actual bank warning metadata | Exact Candidate callback; EX07 owns warning-detail feature. Bank warning must not be invented as the issue's blocked reason. |
| Review rates/manual amount/clear/accept suggestion | Named WB30/WB31 and subsequent decision owners | Preserve the applicable typed callback/control seam and correct work/segment identity. Current NEXT decision window is not built: do not invoke the retired legacy action/session payload or add money writes here. |
| Snooze/clear snooze | Existing private 1100 context/action and `broker/src/bpay-next-snooze.js` service actions; WB38 UI owner | Genuine service support exists. Route to real owning UI when present; no mock Snooze button pretending an unavailable owner is complete. |
| Hold/exclusion/cancel/restore/linked scope/restructure | Named later owners / real native Case relations | Only controls justified by actual retained identity and existing owner are applicable now. Retain explicit callback interfaces; do not guess from the reason string, use legacy session identities, or pull later money owners forward. |
| Breakdown | Real retained approved line/segment/quantity/rate evidence | Read-only breakdown is valid only when exact frozen links can be read within existing bounds. No suggestion/amount calculation from component-key strings. |

Legacy `banking-pay-modal-v2-issue-detail.js:145–218` demonstrates the five-column restructuring and moves all actual interactive controls without duplicating amount explanations; lines 254–260 stop child click propagation and forward the owning action. It is presentation evidence. Its actual legacy financial session/advice identities are not NEXT authority. Copy the useful interaction pattern, not the old financial engine.

Completion boundary: source implementation and current caller review can be complete for the actual available issue-detail contract. Named future action/return joins must remain explicit; the three-Candidate Umbrella journey and visual/layout/keyboard acceptance require actual later local app evidence, not a source read or mock image. No new programme, producer protocol, parked F25 objects or ahead-of-chain money owner is recommended.

## Source snapshot SHA256

- `BE supabase/repeatable/10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql` — `727d80356ecfd7ef7e8c822c3181403602910ab4cef5bb50f4677dd32df2b760`
- `BE supabase/repeatable/10102026_1634_banking_pay_next_workbench_read_limits_v1.sql` — `391f32cbecaf4b32a311931d69ed7cbdb0dd85c0bae11a2cdf6a3a07a2e9c02d`
- `BE supabase/migrations/10102026_1634_banking_pay_next_workbench_read_indexes_v1.sql` — `886cdd1d4333b05ec92ff6cf39b775bfe0a9f3225b02a8da2e237673b794b4b2`
- `BE supabase/repeatable/10102026_0956_weekly_source_pay_query_open_display_v1.sql` — `e0c83140e923ab4d7fd856abb09ca6419c43c0c6d1c9056f03a7013e86f74a9b`
- `BE supabase/migrations/10102026_1009_banking_pay_next_ordinary_rate_authority_v1.sql` — `071cf3af4a6ae9ef1c63c93eb32d33b00c9bc93fbc51558d0598d3da5f00b0b4`
- `BE supabase/migrations/10102026_1429_banking_pay_next_workbench_current_continuation.sql` — `c89b160ebb66275b9f60a6e969b7ab15a961750d56b57d95447ed4d5c53d5978`
- `BE supabase/repeatable/10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` — `b8d445c4a4cfc02978e8ac77a159350b0e1978aaff9a1172da8e7e157a6414f5`
- `FE js/bpay-next-workbench.js` — `82df7035fc358f017a0384f62000c492312f09ab7eeada2328c0f67c14ef7ad2`
- `FE js/bpay-next-office.js` — `d377ce9c8eb7073169f045d997200c19c47a828bcf88ff5fcdf9b9df9696c189`
- `FE js/main.js` — `7cde52d8f113f9c35f19f2f0be198390e2d742118c991b98e29ca06dcc645845`
- `FE js/bpay-next-ui.js` — `dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272`
