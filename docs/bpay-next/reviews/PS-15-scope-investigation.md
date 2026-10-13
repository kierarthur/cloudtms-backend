# PS-15 scope and indexed reader investigation

Read-only source investigation, 10 October 2026. No SQL, tests, runtime, data, hosted operations or fixtures executed; no product files edited. This is source inventory, not query-plan/timing, deployed ACL or connected acceptance proof.

## Controlling scope and exact checklist

Sequence35, card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1636–1660`, dependency F-06. The card's **R2 contract controls** the older broader wording: `addendum/R2_EXACT_CONNECTIONS.md:82–84` requires frozen origins and existing Banking history/state-independent detail; **new Payment Summary list/detail/export signatures and provisional filters belong to AUTH-PS/S1**. Do not implement a new Summary API, filter vocabulary, CSV/print consumer, Screen layout, ledger or calculator to satisfy an older checklist description. Keep the accepted underlying frozen attribution and bounded reader guarantees now.

| Row | Required current object | Minimum scope |
| --- | --- | --- |
| D01 | named1324 core schema/index paths | Inventory actual existing point/keyset paths; add a release-owned migration only for an evidenced missing exact index. Preserve immutable objects and ownership/security. |
| D02 | `docs/bpay-next/PSC_CLOSURE.md` | Add the PS-15 exact reader/index/role/route inventory, current reuse and explicit future AUTH-PS list/filter/export ownership. Reuse this one closure document, not a second status register. |
| D03 | `public.bpay_next_payment_history_page_v1(jsonb)` PS-15 successor | Compose onto the current controlling history closure, retaining scope/cursor/security/frozen attribution and state independence; no independent competing final. Existing source already implements these substantive paths. |
| D04 | `broker/src/bpay-next-history.js` | Reuse the authenticated bounded history adapter and exact52-section contract. R2 does not authorise the older proposed new list/detail/export adapters. |
| D05 | recorded TEST1324 baseline receipt | Reference only. Do not reinstall/rewrite it or infer today's installed state from that old receipt. |
| D06 | future joined checklist | Record exact prepared/paid/cancelled/returned, foreign-scope, complete itemised and export-ownership clauses as NOT_RUN. |
| D07 | unapproved Summary policy/design reference | Reference-only AUTH-PS/S1 work. No new Screen in PS-15. |

Rule-B/F25, financial producers/claims/PIN, Source certificates, public/service grants and policies remain outside this card's new product scope.

## Existing authoritative reader paths

Current history root is `supabase/repeatable/10102026_0614_banking_pay_next_ps_c08_history_authority_v1.sql`, including original1206. Original1206:134–181 requires service_role JWT, a server-supplied active admin, an exact closed request and a genuine Candidate or Timesheet scope. Timesheet resolution is a native unique original_timesheet_id or actual booking-family WORK point, not an old Timesheet history sweep. Parent guards1206:234–313 bind revision/approved line/shift/worker/transfer/Case/component/instruction/command ancestry to that scope. No active_owner/READY/current-revision predicate applies to historical selection.

The closed registry1206:74–121 gives each section an exact table, parent FK, ordered key and explicit fields. Query construction1206:315–390 fixes the parent predicate and types all cursor operands. Limit is1–100, one extra row determines continuation, a last indexed key determines upper without COUNT, data accumulation is capped100000 bytes and final response120000 bytes. Worker reads are bounded12KiB request/128KiB response, one page, finite timeout, and retain strings/NULL without monetary aggregation. Live keyset refresh is explicitly **not an across-request MVCC snapshot**. No frozen/current child scan is used to rebuild stored transfer totals.

History SHIFTS/BREAKS/RATES/SCHEDULE read immutable captured approved children via sealed revision ancestry. Current/cache sections are deliberately labelled current and cannot replace frozen money/origins. The route is the existing authenticated POST `/api/banking/pay/next/history-page`, exempt from the retired legacy route fence, with the configured TEST environment guard. Its RPC is the fixed `bpay_next_payment_history_page_v1`, with actor injected by Worker, not browser payload.

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

## Security and state boundaries

* History and public frozen service functions retain SECURITY DEFINER, fixed pg_catalog/private/public path, owner postgres as logical release owner, browser PUBLIC/anon/authenticated EXECUTE revoked, service_role wrapper EXECUTE granted. Their Worker supplies an authenticated admin actor and service claim. Private detail helper EXECUTE remains revoked from service/browser roles; wrapper runs through owner authority. No direct private-table grant is needed or authorised.
* Core1324:1072–1159 pins the owner and revokes direct PUBLIC/anon/authenticated/service_role table authority. Do not describe all core tables as FORCE-RLS if their source does not enable it; FORCE-RLS is a separate existing Source boundary. Source-binding migration2010:208–221 explicitly enables+forces RLS on binding/version, revokes even service direct access and grants only CURRENT_USER owner policies. Preserve that existing owner route. History reads retained frozen NEXT children, not live Source policy/contact/Client tables.
* **Important acceptance distinction:** operational `/frozen-detail` currently dispatches0250 with only four Draft review kinds.0250:29–31 requires active_ownerNEXT;0025:28–32 and162–171 require REVIEW/DRAFT and workerREADY. These are genuine retained operational Draft guards. The same captured historical SHIFTS/BREAKS/RATES/SCHEDULE are readable through history1206 without those predicates. PS-15 documentation/journey must name the history route for paid/cancelled/returned immutable detail and must not claim the operational0250 endpoint is state-independent. Preserve the PS-07 separation; do not silently strip Draft operational guards. If the owner requires the literal0250 endpoint itself to admit historical states, that is a concrete additional change requiring the exact0250/0025/1134 dependency contract, not accomplished by a1206 comment or route-fence exemption.
* Existing known POST history/frozen-detail paths are individually exempt from the old-engine fence; index dispatch still authenticates admin and applies PF13 TEST environment compatibility before RPC. Unknown/method-invalid routes retain their existing fence/refusal semantics. Informational history is not a financial epoch admission or a payment action.

## Smallest complete next change

Finish D02 with the above exact table/security/route evidence and accepted future list/detail/export ownership. Reuse the correct D01/D04 source; **no index migration is currently justified**. D03 can be the release-owned scoped successor/closure preserving the current physical definition with accurate compatibility provenance, composed after actual PS-07/PS-11 changes. Shared history chain remains PS-07 → EX-41 → CM-18; PS-15 has no independent numbered slot. Current final must remain singular and later chain owners must merge this card's guarantees. Do not mutate immutable1206/2013/core1324 or reopen Rule-B/producers.

Use existing source checks for closed52 allowlists, exact parent/cursor/role negative coverage, bytes/rows and route fence. Do not build a new parser, benchmark, proof programme, supplied totals/hash protocol, read session or generic export adapter. Existing live keyset cannot certify a consistent new Summary export across concurrent requests: until AUTH-PS defines that owned API, record export as **not implemented/owned by AUTH-PS/S1**, never emit partial totals or claim existing52 pages are a consistent report snapshot.

D06 future joined acceptance remains NOT_RUN, including original named whole itemised records and ordinary/Source/both-channel coverage; new Summary/multi-Client filters/export are separately owned after approval. A source index list neither proves two-second latency nor deployed FORCE-RLS/role behavior.

## Raw inspected source hashes

| Source | SHA-256 |
| --- | --- |
|1324 core |`f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
|0300 Case instruction core |`04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
|2010 Source binding schema |`d2641717de3967aa30e200d75f9e4fde801b31c7eeef9cc8db84efb36d9266ea` |
|2013 history scope indexes |`cb08a6b28d8a2eeace2e0f4ed3561c84dcda33022bb0fe54d7f46ee5e43eacc2` |
|original1206 history SQL |`d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
|current0614 history root |`abecabcaa00883d1ebd2735c4f28df313a8a6161f4dce71272a57b4bd590d340` |
|existing BE history caller |`b5bb109e546787fc65a83df1916dfd29c1b21d32ae6372fd3fd04ae97baa0d50` |

Hashes identify the inspected saved source only; root may be composing earlier cards concurrently and must re-read the final registry before implementation.
