# Payment Summary compatibility closure

PS-15 / BD039,10 October2026. Existing Banking history/frozen-detail compatibility is built now. Owner start-sheet AUTH-PS approval is recorded; approved Summary policy/layout/filter/list/export implementation remains S1 sequence287, per R2§7 and file07. No approval pause is required. Local source closure is separate from installed/runtime/application acceptance, all NOT_RUN here.

## Current authoritative paths

Final public.bpay_next_payment_history_page_v1(jsonb) root/physical0653 composes the original1206 body after0623→0614. One final0653 definer; later EX-41/CM-18 merge this closure. Exactly52 section definitions are mechanically mirrored by broker/src/bpay-next-history.js and the existing Office history modal. PS-03/PS-11 typed member/recorded-NET semantics remain.

Authenticated POST /api/banking/pay/next/history-page invokes only that fixed service RPC with a server-injected active-admin actor. SQL checks service_role and genuine Candidate or Timesheet ancestry on every page. Every parent is exact scoped Work/revision/line/shift/worker/transfer/Case/component/instruction/command identity. No READY, active-owner, current-revision or live pricing predicate selects historical facts. The route is explicitly exempt from the retired legacy engine fence; normal TEST environment/auth checks still apply.

SHIFTS/BREAKS/RATES/SCHEDULE through this history route are the state-independent frozen itemised detail path for prepared, paid, cancelled and returned records. The operational /frozen-detail endpoint is a distinct guarded Draft-review reader; its REVIEW/DRAFT/READY/module guard is retained. Whole frozen detail means following its typed ancestry/cursors to terminal pages, not collecting a new all-run JSON or reconstructing a transfer cash total from member contributions. Stored transfer, accepted projection, actual outcome and return balance remain distinct money authorities. Current/cache sections carry their explicit current amount_scope and cannot replace frozen origins.

## Bounds and consistent exports

Existing history accepts limit1–100, exact closed request/parent/cursor fields and unique typed ordered ties. SQL uses one extra row for continuation and indexed last key for upper without COUNT; accumulated data100000 bytes and final120000 bytes. Worker request12KiB/response128KiB/finite8s abort; one request/one bounded page/no automatic mutation/queue wake. Native decimal/bigint strings and NULL are retained without float or browser arithmetic. These are encoded/transport bounds and index support, not measured latency or proof that an abandoned database statement stops at8s.

LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT is a live upper/cursor convention, not a cross-request MVCC snapshot. A later commit below a cursor needs refresh. It cannot be sold as consistent filtered CSV/print membership or totals. The current closed adapter rejects unknown export/filter fields/sections before its sole RPC; it never emits a partial report export. No generic export/session/digest framework is added here.

Accepted S1 obligations: bounded approved Summary list/filter contract; whole itemised modal across ordinary/Source/both channels; one transfer counted once across multiple Clients; consistent filtered CSV/print membership and stored totals, or explicit incomplete-export refusal; PSP-81 Components scroll. Exact new service signatures and export consistency mechanism are authored with the approved screen at S1, not assigned provisional competing signatures in PS-15. No Client/Timesheet NET allocation, duplicate Client capture, second ledger, N+1 live repricing or member-page cash calculation is permitted.

## Exact source index/unique-key inventory

All paths below were checked against current migration source, including inline unique/primary constraints, not just CREATE INDEX names. **No missing exact access index was found for the current52-section contract.** Unique equality keys can serve point reads and ordered subsets; another index with identical leading scope/key columns is unnecessary. This is source support, not a measured plan guarantee.

| Section(s) | Actual scope and cursor key | Existing support/source |
| --- | --- | --- |
| WORKS | candidate_id; week_ending_date,id |1324:994 `bpay_next_work_candidate_week_idx`; id PK and original_timesheet_id/booking_id unique for scope points1324:125–128 |
| REVISIONS | work_id; revision_no |1324:167 unique(work_id,revision_no),1001 work/revision/id index; historical sealed filter retained |
| LINES | revision_id; line_no |1324:205 unique(revision_id,line_no),1003 revision/line/id index |
| SHIFTS | approved_line_id; detail_no |1324:231 unique line/detail;1005 line/detail/id index |
| BREAKS | shift_detail_id; break_no |1324:292 unique(shift_detail_id,break_no) |
| RATES | approved_line_id; bucket |1324:254 unique and1007 matching index |
| SCHEDULE | revision_id; rate_family,rate_code |1324:271 unique and1009 matching index |
| POSITIONS | work_id; component_key |1324:345 PK(work_id,component_key) |
| COLLECTIONS | work_id; component_key |1519:36 unique(work_id,component_key); unique Case identity for historical Case ancestry |
| EFFECTS | work_id plus component_key; occurred_at_utc,id |1324:1011 work/component/time/id index; component is mandatory typed request operand |
| PUBLICATIONS | work_id; revision_no |1324:947 unique and1069 work/revision index |
| RUN_WORKS | work_id; id |1324:1033 work/id index |
| RUN_POSITIONS / RUN_LINES | run_work_id; component_key |1324:547 and583 unique(run_work_id,component_key) |
| HOLD | run_line_id; id |1324:605 unique run_line_id; at most one original hold |
| WORKER | id; id |1324:471 PK |
| PROJECTIONS | run_worker_id; projection_no |1324:645 unique(run_worker_id,projection_no) |
| TRANSFERS | run_worker_id; transfer_no |1324:675 unique and1052 worker/transfer_no/id index |
| MEMBERS | transfer_id; member_no |1324:731 PK and1054 matching index; stored transfer cash/member totals are returned, not rebuilt |
| OUTCOMES | transfer_id; occurred_at_utc,id |1324:1056 matching index |
| RETURN_CASH | original_transfer_id; id |1324:708 unique original_transfer_id; current return balance labelled as such |
| CSV / INTERNAL_RECEIPT | transfer_id; id |1324 CSV transfer_id UNIQUE;0480 internal transfer_id UNIQUE, each at most one exact receipt |
| GROUP_LEGS | anchor_transfer_id; transfer_id |1827 unique(anchor_transfer_id,transfer_id) |
| EXPIRY | run_worker_id; command_id |0501 `bpay_next_preparation_expiry_worker_idx(run_worker_id,command_id)` |
| CASES / CASE | candidate_id,id / id point |1324:1019 candidate/id full index, case id PK; no status-only partial index substitutes for closed historical Cases |
| COMPONENTS | case_id; component_ordinal |0300 unique(case_id,component_ordinal) |
| RULES | case_id; rule_revision |0300 unique(case_id,rule_revision) |
| CASE_EVENTS | case_id; occurred_at_utc,id |F-06 migration2013 full `bpay_next_case_event_history_idx`; includes old untyped events, unlike component-only/typed partial indexes |
| INSTRUCTIONS | case_component_id; pay_week_start,id |0300 `bpay_next_case_instruction_component_idx` exact columns |
| CASE_HOLDS | case_component_id; pay_week_start,id |0300 `bpay_next_case_hold_period_idx` exact columns; historical untyped holds remain separately available in WORKER_CASE_HOLDS |
| RESULTS | instruction_id; state_id |0300 `bpay_next_case_result_instruction_idx(instruction_id,state_id)`; unique(state_id,instruction_id) makes the reverse scoped cursor tie unique |
| COMMANDS | candidate_id; command_id |F-06 migration2013 full `bpay_next_command_member_history_idx`; original command-first PK is not substituted |
| COMMAND | id; id |1324 command PK |
| JOBS | command_id plus candidate_id; job_kind |1324:986 unique(command_id,candidate_id,job_kind);1206:333 adds candidate_id predicate, so cursor does not collide across Candidates |
| NET_REQUEST / OUTCOME_REQUEST / RETURN_REQUEST / REISSUE_REQUEST / CANCEL_REQUEST / REISSUE_CANCEL / WRITE_OFF / CASE_CREATE / EXPIRY_REQUEST | command_id; command_id |native typed request command_id PKs; these are point reads, not broad journal pagination |
| PROTECTED_RECEIPT | revision_id; orchestration_run_id |0340 revision_id UNIQUE and orchestration_run_id PK; at most one genuine protected receipt |
| SOURCE_DETAIL | component_id plus exact head_id; component_id |1324:319 PK(head_id,component_id);1206:318–319 binds both operands to the captured approved origin |
| POLICY | policy_id plus source_window_id; source_window_id |1324:35 PK(policy_id,source_window_id);1206:320–321 binds the captured policy/window |
| CHOICE | run_id plus work_id; work_id |1241 PK(run_id,work_id);1206:322–323 supplies both equality operands |
| SELECTION | run_id plus work_id; selection_no |1241 unique(run_id,work_id,selection_no); same exact captured run/work scope |
| WORKER_CASE_HOLDS | run_worker_id; id |1324:929 full worker/id index, including historical untyped/released rows |
| COMMAND_WORKER | run_id plus candidate_id; id |1324:492 unique(run_id,candidate_id);1206:325–326 supplies both equality operands, at most one worker |

Stable component cursors have genuine unique ties; pay-week/time keysets include native UUID where needed. Do not add arbitrary synthetic ties, full-history count/hash routines or a general indexing programme. If PS-11/other earlier work changes the actual final contract before PS-15 is authored, re-check only that exact changed key/predicate against current source and add a new index only if genuinely missing.


## Ownership, FORCE-RLS and grants

Logical owner postgres remains release-mapped CURRENT_USER on local/Miget. Core1324 already revokes direct PUBLIC/anon/authenticated/service_role table privileges. Do not claim all NEXT core tables are FORCE-RLS: the existing Source binding/version owner boundary in2010 explicitly enables and forces RLS, revokes service direct access and uses owner-only policies. That Source authority remains unchanged. Private detail helpers retain no service/browser EXECUTE; public wrappers retain audited SECURITY DEFINER/search_path and service-only EXECUTE, with active-admin actor/scope checks. No new grant/policy/Source read is needed. Final isolation closures continue after controlling reader installation.

## Deliberate gaps and owning steps

Current manifest honestly reports moved-root flat paid binding, canonical multi-channel vector, move comparison receipt and uncaptured general approval actor/reason gaps; Timesheet scope also labels commands without Work/Case ancestry. Existing current financial caches are not historical captures. Later named Contract-move/EX owners, including CM-18/EX-41, own those exact changes; this card does not manufacture facts, widen current scope or copy live Client data. Rule-B F-25 capture objects remain authoritative; parked protocols stay parked.

D01 reuses positively evidenced indexes; no missing exact index and no new migration. D02 this closure; D03 controlling0653 successor projects six already captured revision display fields; D04 existing adapter mirrors exactly those fields; D06 PS-15-JOINED-ACCEPTANCE.md NOT_RUN. Local schema/contract/source checks and independent review are recorded in PS-15-COMPLETION.md/BUILD_LOG. No fixture, rehearsal, hosted installation, network side effect or performance measurement.

BD039 evidenced compatibility gap: original history REVISIONS omitted six existing immutable work_revision header fields (candidate_display_name, candidate_reference, client_display_name, job_title, band_label, timesheet_reference).0653 exposes precisely those captures; existing Worker and Office history closed allowlists mirror them. RUN_WORKS stays its genuine six-field capture-ID relation, with captured_revision_id→REVISIONS ancestry. No new table/column/index or duplicate Client capture. Captured Client display is available; this does not invent a frozen Client-ID field or the later Summary filter API.
