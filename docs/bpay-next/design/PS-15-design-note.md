# PS-15 scoped design note and PSC_CLOSURE recommendation — 10 October 2026

Read-only source/design inventory. No product edits, SQL/tests/runtime/app/data/hosted operations or test programme. Root alone writes `docs/bpay-next/PSC_CLOSURE.md` and product. This note refines the earlier `reviews/PS-15-scope-investigation.md`; its0614-current-root and 'approval pending/unapproved' wording is now historical.

## Recommendation and binding approval

**D01 has no evidenced missing index in the current52-section contract.** Preserve installed1324 and F06 schema/index receipts; do not write a noop or duplicate-index migration merely to populate a checklist box. D02 should contain this exact current reader/access/security inventory. D03 can be a scoped release-owned closure after current0623, preserving the actual1206 definition. D04 reuses the existing authenticated fixed-RPC history adapter. D06 must name every accepted underlying compatibility clause and retain actual joined acceptance as NOT_RUN.

Fullcard06:1636–1657 / current checklist876–901 / R2§7 control. Existing history and captured detail build now; no new Summary signatures/filters are inferred into PS15. **Owner start sheet29–34 explicitly pre-approves AUTH-PS with recommended provisional choices and S1/PSP81.** No approval pause is needed, and PSC_CLOSURE must not repeat the older UNAPPROVED/Await approval wording. New Summary list/detail/export signatures, exact filters and screen remain the approved **later S1 owner** under R2 and file08 seq287; approval does not move that implementation into PS15 or prove it already exists.

Accepted bounded list/itemised/consistent-export requirements remain real; record their current reusable data support and their remaining S1 implementation separately. Do not call these requirements optional, complete from existing live history pages, or blocked by unanswered owner approval.

## Exact current SQL/Worker/route ownership

Registry current `public.bpay_next_payment_history_page_v1(jsonb)`:

- finalRoot `supabase/repeatable/10102026_0623_banking_pay_next_ps_c12_recorded_net_history_v1.sql`;
- includes0614 PS07 semantic root, which includes original `06102026_1206_banking_pay_payment_history_page_v1.sql`;
- finalPhysical remains that original1206; one actual physical definition,52 sections, no duplicate independent definer.

0623 preserves original captured projection/transfer NET; Candidate recorded NET is not line/Timesheet/Client NET. PS15's successor should include the **fresh current0623 closure**, register singular root/provenance, and explain retained indexed frozen/state-independent guarantees. Existing shared future editor chain remains EX41 then CM18, merging prior PS07/PS11/PS15 guarantees. Do not replace physical1206, alter its original ACL or import a historical whole shared file.

Existing route: authenticated POST `/api/banking/pay/next/history-page` -> `handleBpayNextHistoryRead` in `broker/src/bpay-next-history.js` -> fixed `/rest/v1/rpc/bpay_next_payment_history_page_v1`. Worker injects actor_user_id from actual Office user; browser request cannot supply the trusted actor. Exact normalized route is separately exempt from the old-engine fence, authenticates admin, and checks PF13 environment before RPC (current index198704–198738). No new route/list/export adapter is needed for existing history.

CurrentSQL1206:135–181 requires service claim, active admin, closed request, exactly one actual Candidate/Timesheet scope. Timesheet resolves its unique original_timesheet_id or actual booking-family WORK point; no historical Timesheet scan/current-authorised selector. Parent234–313 binds every child to actual captured ancestry. Original projection id or sealed captured revision is authority; no latest/current financial reprice or READY/active_owner gate in history selection.

## Exact52-section access inventory

Every row below is from the unchanged actual1206 registry included by0623. Scope columns are equality predicates before keyset ordering; additional required equalities are stated explicitly. Primary/unique constraints count as real access support. A duplicate same-leading-column index is not a missing deliverable. This is source evidence, not an EXPLAIN/timing or deployed-index receipt.

| Section | Native table (private schema) | Equality scope / ordered key | Existing exact index/unique support |
| --- | --- | --- | --- |
| WORKS | `bpay_next_work` | `candidate_id`; `week_ending_date,id` | 1324 bpay_next_work_candidate_week_idx(candidate_id,week_ending_date,id); original_timesheet_id / booking_id UNIQUE for Timesheet point |
| REVISIONS | `bpay_next_work_revision` | `work_id`; `revision_no` | 1324 UNIQUE(work_id,revision_no); bpay_next_revision_work_idx(work_id,revision_no,id) |
| LINES | `bpay_next_approved_line` | `revision_id`; `line_no` | 1324 UNIQUE(revision_id,line_no); bpay_next_line_revision_idx(revision_id,line_no,id) |
| SHIFTS | `bpay_next_shift_detail` | `approved_line_id`; `detail_no` | 1324 UNIQUE(approved_line_id,detail_no); bpay_next_detail_line_idx(approved_line_id,detail_no,id) |
| BREAKS | `bpay_next_break_detail` | `shift_detail_id`; `break_no` | 1324 UNIQUE(shift_detail_id,break_no) |
| RATES | `bpay_next_rate_detail` | `approved_line_id`; `bucket` | 1324 UNIQUE(approved_line_id,bucket); bpay_next_rate_detail_line_idx(approved_line_id,bucket) |
| SCHEDULE | `bpay_next_rate_schedule` | `revision_id`; `rate_family,rate_code` | 1324 UNIQUE(revision_id,rate_family,rate_code); bpay_next_rate_schedule_revision_idx(revision_id,rate_family,rate_code) |
| POSITIONS | `bpay_next_position` | `work_id`; `component_key` | 1324 PK(work_id,component_key) |
| COLLECTIONS | `bpay_next_work_collection` | `work_id`; `component_key` | 1519 UNIQUE(work_id,component_key); Case identity UNIQUE supplies ancestry point |
| EFFECTS | `bpay_next_financial_effect` | `work_id`+component_key; `occurred_at_utc,id` | 1324 bpay_next_effect_work_history_idx(work_id,component_key,occurred_at_utc,id); component_key mandatory equality |
| PUBLICATIONS | `bpay_next_publication` | `work_id`; `revision_no` | 1324 UNIQUE(work_id,revision_no); bpay_next_publication_work_order_idx(work_id,revision_no) |
| RUN_WORKS | `bpay_next_run_work` | `work_id`; `id` | 1324 bpay_next_run_work_work_idx(work_id,id) |
| RUN_POSITIONS | `bpay_next_run_position` | `run_work_id`; `component_key` | 1324 UNIQUE(run_work_id,component_key) |
| RUN_LINES | `bpay_next_run_line` | `run_work_id`; `component_key` | 1324 UNIQUE(run_work_id,component_key) |
| HOLD | `bpay_next_hold` | `run_line_id`; `id` | 1324 UNIQUE(run_line_id); at most one original hold |
| WORKER | `bpay_next_run_worker` | `id`; `id` | 1324 PK(id) |
| PROJECTIONS | `bpay_next_net_projection` | `run_worker_id`; `projection_no` | 1324 UNIQUE(run_worker_id,projection_no) |
| TRANSFERS | `bpay_next_transfer` | `run_worker_id`; `transfer_no` | 1324 UNIQUE(run_worker_id,transfer_no); bpay_next_transfer_worker_idx(run_worker_id,transfer_no,id) |
| MEMBERS | `bpay_next_transfer_member` | `transfer_id`; `member_no` | 1324 PK(transfer_id,member_no); bpay_next_transfer_member_page_idx(transfer_id,member_no) |
| OUTCOMES | `bpay_next_transfer_outcome` | `transfer_id`; `occurred_at_utc,id` | 1324 bpay_next_transfer_outcome_idx(transfer_id,occurred_at_utc,id) |
| RETURN_CASH | `bpay_next_return_cash` | `original_transfer_id`; `id` | 1324 UNIQUE(original_transfer_id); at most one exact original cash owner |
| CSV | `bpay_next_csv_instruction` | `transfer_id`; `id` | 1324 UNIQUE(transfer_id); at most one exact instruction receipt |
| INTERNAL_RECEIPT | `bpay_next_internal_receipt` | `transfer_id`; `id` | 0480 UNIQUE(transfer_id); at most one exact internal receipt |
| GROUP_LEGS | `bpay_next_destination_group_leg` | `anchor_transfer_id`; `transfer_id` | 1827 UNIQUE(anchor_transfer_id,transfer_id); bpay_next_destination_group_leg_page_idx(anchor_transfer_id,transfer_id) |
| EXPIRY | `bpay_next_preparation_expiry_worker` | `run_worker_id`; `command_id` | 0501 bpay_next_preparation_expiry_worker_idx(run_worker_id,command_id) |
| CASES | `bpay_next_finance_case` | `candidate_id`; `id` | 1324 bpay_next_case_candidate_exists_idx(candidate_id,id); full index includes closed Cases |
| COMPONENTS | `bpay_next_case_component` | `case_id`; `component_ordinal` | 0300 UNIQUE(case_id,component_ordinal) |
| RULES | `bpay_next_case_rule` | `case_id`; `rule_revision` | 0300 UNIQUE(case_id,rule_revision) |
| CASE_EVENTS | `bpay_next_case_event` | `case_id`; `occurred_at_utc,id` | 2013 bpay_next_case_event_history_idx(case_id,occurred_at_utc,id); full index includes old untyped events |
| INSTRUCTIONS | `bpay_next_run_case_instruction` | `case_component_id`; `pay_week_start,id` | 0300 bpay_next_case_instruction_component_idx(case_component_id,pay_week_start,id) |
| CASE_HOLDS | `bpay_next_case_hold` | `case_component_id`; `pay_week_start,id` | 0300 bpay_next_case_hold_period_idx(case_component_id,pay_week_start,id) |
| RESULTS | `bpay_next_case_allocation_result` | `instruction_id`; `state_id` | 0300 bpay_next_case_result_instruction_idx(instruction_id,state_id); UNIQUE(state_id,instruction_id) makes scoped state tie unique |
| COMMANDS | `bpay_next_command_member` | `candidate_id`; `command_id` | 2013 bpay_next_command_member_history_idx(candidate_id,command_id) |
| COMMAND | `bpay_next_command` | `id`; `id` | 1324 PK(id) |
| JOBS | `bpay_next_job` | `command_id`+candidate_id; `job_kind` | 1324 UNIQUE(command_id,candidate_id,job_kind); mandatory candidate_id equality supplies full prefix |
| NET_REQUEST | `bpay_next_paye_net_request` | `command_id`; `command_id` | 1324 PK(command_id) |
| OUTCOME_REQUEST | `bpay_next_outcome_request` | `command_id`; `command_id` | 1324 PK(command_id) |
| RETURN_REQUEST | `bpay_next_return_request` | `command_id`; `command_id` | 1324 PK(command_id) |
| REISSUE_REQUEST | `bpay_next_reissue_request` | `command_id`; `command_id` | 1324 PK(command_id) |
| CANCEL_REQUEST | `bpay_next_cancel_request` | `command_id`; `command_id` | 1324 PK(command_id) |
| REISSUE_CANCEL | `bpay_next_reissue_cancel_request` | `command_id`; `command_id` | 0350 PK(command_id) |
| WRITE_OFF | `bpay_next_write_off_request` | `command_id`; `command_id` | 1647 PK(command_id) |
| CASE_CREATE | `bpay_next_case_create_request` | `command_id`; `command_id` | 0320 PK(command_id) |
| EXPIRY_REQUEST | `bpay_next_preparation_expiry_request` | `command_id`; `command_id` | 0501 PK(command_id) |
| CASE | `bpay_next_finance_case` | `id`; `id` | 1324 PK(id) |
| PROTECTED_RECEIPT | `bpay_next_protected_source_receipt` | `revision_id`; `orchestration_run_id` | 0340 UNIQUE(revision_id), PK(orchestration_run_id); at most one exact protected receipt |
| SOURCE_DETAIL | `bpay_next_source_chosen_detail` | `component_id`+captured head_id; `component_id` | 1324 PK(head_id,component_id); exact captured head_id equality required |
| POLICY | `bpay_next_valuation_policy_window` | `policy_id`+captured source_window_id; `source_window_id` | 1324 PK(policy_id,source_window_id); exact captured source_window_id equality required |
| CHOICE | `bpay_next_work_choice` | `work_id`+captured run_id; `work_id` | 1241 PK(run_id,work_id); actual captured run_id equality required |
| SELECTION | `bpay_next_selected_component` | `work_id`+captured run_id; `selection_no` | 1241 UNIQUE(run_id,work_id,selection_no); actual captured run_id equality required |
| WORKER_CASE_HOLDS | `bpay_next_case_hold` | `run_worker_id`; `id` | 1324 bpay_next_case_hold_worker_idx(run_worker_id,id); full historical released/untyped path |
| COMMAND_WORKER | `bpay_next_run_worker` | `run_id`+candidate_id; `id` | 1324 UNIQUE(run_id,candidate_id); actual Candidate equality, at most one scoped worker |

Source IDs above refer to actual immutable migration files listed below; these existing definitions, including inline constraints, were inspected. JOBS1206:330 requires Candidate equality before its job_kind key; SOURCE_DETAIL318–319 binds captured component/head, POLICY320–321 captured policy/window, CHOICE/SELECTION322–323 captured work/run. COMMAND_WORKER324–326 binds actual run/Candidate, discovering zero-WORK Case-only workers. Only EFFECTS permits/requires component_key. REVISIONS retains sealed filter but indexes still lead actual work/revision.

Critical full-history distinctions: CASE_EVENTS uses F062013 full Case/time/id, not a typed-component/partial-event index that omits older untyped records. COMMANDS uses F062013 Candidate/command, not command-first PK. CASES uses full Candidate/id, not statusOPEN partial due index. WORKER_CASE_HOLDS uses full worker/id, not ACTIVE-only or component-typed paths. No new synthetic tie, whole-history count/hash, indexing programme or calculator is justified.

## Frozen origins and whole itemised compatibility

A payment/transfer's immutable native amount and original projection remain whole-transfer/whole-Candidate facts. TRANSFERS -> MEMBERS -> actual WORK run_line/captured revision, CASE instruction/result/hold, NET_ADJUSTMENT original projection and CASH_REISSUE original/return links preserve PS03 origin semantics. PS11 current0623 keeps entered PAYE NET vs not-entered/not-applicable/genuine0 distinct and does not create line NET. MEMBERS signed contributions identify constituents, never arbitrary Timesheet or Client cash shares.

Existing historical itemised traversal is explicit: MANIFEST then every declared applicable child section **for each actual emitted parent**, to terminal. Captured RUN_WORKS -> revisions/approved lines -> SHIFTS/RATES/SCHEDULE and SHIFTS -> BREAKS are immutable. Case-only paths originate from COMMAND_WORKER or genuine INSTRUCTIONS worker/context; related Case ancestry is actual WORK_COLLECTION uniqueCase/Work, not guessed allocation. Requests, failed/DONE jobs, released/untyped holds, past projections and outcomes remain retained. Current POSITION/CASE/COMPONENT/WORKER/return caches carry explicit current-state labels and cannot replace immutable figures or attribution.

Current fields do not add a duplicate Client/contact/Contract capture. Preserve the exact frozen attribution already available. If a future S1 requested display label lacks genuine retained source, show its approved missing-source wording and record that exact owned gap rather than live Client lookup or fabricated captured label. PS15 doesn't certify late moved-origin flat vectors/comparison receipts that original1206 explicitly reports unavailable; their actual CM/SP owners retain that work.

## SQL role, FORCE-RLS and ordinary ACL distinctions

| Boundary | Actual current source fact | PS15 action |
| --- | --- | --- |
| Public history RPC | STABLE SECURITY DEFINER; fixed pg_catalog/private/public path; timezoneUTC; logical ownerpostgres; PUBLIC/anon/authenticated EXECUTE revoked; service_role EXECUTE granted; service JWT claim **and** active admin independently checked1206:135–163,411–414 | Reuse unchanged; actor injected by Worker; no new grants/policies |
| NEXT private tables | Core1324 owner/revoke boundary includes direct PUBLIC/anon/authenticated/service_role table revocation. PK/index existence is not a role grant. Not every core table is FORCE-RLS | Do not falsely claim every52 source table is FORCE-RLS or grant direct service/browser SELECT |
| Existing Source binding/version | F062010:208–221 logical ownerpostgres; ENABLE+FORCE RLS on `bpay_next_source_component_binding` and `_version`; direct all roles including service_role revoked; actual CURRENT_USER owner-only USING/WITH CHECKtrue policies | Preserve exact existing owner route; no new service bypass/policy or broad grant |
| Release owner adapter | Repository logicalpostgres maps only to audited actual generated ownerCURRENT_USER at release | Source mapping is not permission to SET ROLEpostgres or prove runtime ACL here |
| Worker/route | Real Office authentication/admin + PF13 TEST environment; fixed serviceRPC; no browser service credential, rawSQL/free section/table/type | Reuse existing boundary |

FORCE-RLS, role privilege and SECURITY DEFINER are separate mechanisms. The generated physical owner still needs the existing audited policy/owner adapter; a serviceRole JWT alone is not a direct Source table SELECT grant. Existing history consumes retained NEXT captured records and owner routines, not live Source contact/Client/pricing tables. No new policy/grant is necessary to claim indexed reader reuse.

## State-independent history versus operational frozen-detail

The existing historical SHIFTS/BREAKS/RATES/SCHEDULE sections resolve immutable approved children through actual captured parent ancestry without workerREADY/runDRAFT/current revision/active_owner predicates. This is the historical detail path for prepared/paid/cancelled/returned records.

The different operational POST `/api/banking/pay/next/frozen-detail` remains service wrapper `04102026_0250_banking_pay_next_frozen_detail_service_v1.sql`; it selects only SHIFTS/BREAKS/RATE_DETAILS/RATE_SCHEDULE for current Draft review, requires active_ownerNEXT at30–31 and calls private0025 readers whose actual REVIEW/DRAFT and workerREADY guards remain deliberate. Route-fence exemption does not remove those DB guards. Do not say that0250 endpoint is already state-independent, strip its operational guards or broaden original advice to close PS15.

PSC_CLOSURE should explicitly map **historical detail to history-page** and **operational Draft review to frozen-detail**. Paid/cancelled/returned historical acceptance uses the former. If a caller literally sends historical records through operational0250, the concrete fix is selecting the existing historical reader/captured parent contract; a wholly new summary or mutation API is unnecessary. FE F16's UI owner readiness gate remains separate from SQL history's financial-lifecycle state independence; this note doesn't claim owner-paused UI behavior.

## Bounds, consistency and fixed-export ownership

| Accepted contract | Existing current support | Remaining owner / truthful status |
| --- | --- | --- |
| Bounded list access | Existing history Candidate/Timesheet parent pages over native52sections; actual keys/upper/cursor and permission checks | PS15 preserves generic existing history compatibility. New Summary payment-row list, whole-filter totals, exact provisional filters/count semantics and UI are approved S1 work, not built merely by these52 pages |
| Whole itemised payment detail | Native immutable transfer cash/projection and explicit complete captured child traversal, Case-only provenance, PS03 typed origins, actual0/NULL semantics | PS15 retains data compatibility and existing bounded history/details. Approved S1 modal component aggregation/pinned full server totals/PSP81 rendering remains S1; no current browser-summed incomplete totals |
| Consistent filtered CSV/print | Immutable frozen constituent data and native stored amounts are reusable inputs; current reader explicitly LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT | Exact fixed-export API/membership/totals/filters, every-component export and formula-safe CSV/print belong to approved S1 (PSP66–69,77). **Not implemented by repeated live history pages**; incomplete export must fail clearly, never partial totals/file |

SQL request limit1..100, exact cursor binds scope/section/parent/context/component/upper/key. First upper uses indexed reverse last key rather than COUNT. Each page visits at mostlimit+1 data rows; row accumulation100000bytes/finalresponse120000bytes. Worker request12KiB/response128KiB/pageRows100/timeout8s; one HTTP request fetches one page only. Exact scalars remain text/null; native decimal/bigint precision preserved. Header metadata and fields are closed, no user-controlled SQL/table/order.

Upper keys do **not** provide a cross-request MVCC/export snapshot: late/backdated commits can appear below an earlier cursor; current caches can change between requests. Refresh the section from null for a new live session; retain explicit consistency label. Do not introduce a stored read-session/hash/count/proof framework for PS15. No measured2s/timing, deployed grants/FORCE-RLS or connected export success is asserted by this static inventory.

PSP35–39 accepted totals/cash lifecycle and multiClient filtering remain whole native transfer once, parent reservation never doubled with legs; no NET share by Client. PS14 reservation facts will be reused by S1, not rebuilt in browser. Missing original Client is explicit missing provenance, not live lookup; executed cash/reservations/unknown NET remain distinct. Original PAYE advice reader stays restricted to its real original Candidate BANK/CSV case, not enlarged for generic exports.

## Exact PSC_CLOSURE structure and per-deliverable recommendation

Root's single `docs/bpay-next/PSC_CLOSURE.md` should contain: authority/preapproval; current singular history closure;52section key/index table; immutable/current fact categories and exact attribution traversal; role/FORCE-RLS distinctions; historical versus operational routes; bounds/live consistency; accepted list/itemised/export requirement-to-current-support-to-S1 ownership table; precise NOT_RUN joined clauses and source hashes. This is the existing requested closure target, not a new status/proof register.

- **D01:** source-supported reuse, no missing exact index found; preserve original schema and F062013's two genuinely missing baseline read indices. No new migration justified.
- **D02:** write the requested closure with the above facts; fix stale approval/current-root language.
- **D03:** scoped successor include of actual0623 + narrow guarantee/provenance wording, correct singular registry and future EX41/CM18 merge. No needless copied full function, new SQL fields or money/actor hunk.
- **D04:** existing fixed history route/Worker52contract correct reuse. Do not create the older row's proposed Summary adapters inside PS15; explicitly map their new signatures to approved later S1 under R2.
- **D05:** installed1324 historical release receipt is baseline evidence only; not current hosted/state readback or reason to reinstall.
- **D06:** finite future joined checklist is required and NOT_RUN. Whole itemised ordinary/Source bothchannels, originalmoney0/NULL, Case-only/cancelled/returned/native receipts, captured detail after current source change, foreign parent/cursor refusal, route-fence/admin/environment, larger keyset and explicit incomplete export/S1 ownership clauses all remain named.
- **D07:** earlier design's 'unapproved' heading is superseded by ownerstartAUTHPS; record approved recommended choices and remaining laterS1 implementation, no permission pause.

Use ordinary existing source/caller gates only when root implements its closure; no tests/fixtures/scenarios/measurements/tooling programme added by this investigation. Do not claim source-review completion means app-tested or installed. Preserve RuleB/F25, frozen rows, Source/private finance ownership, existing correct role policy, shared Case/payment writers and future cards.

## Source IDs / exact inspected paths

| ID | BUILD-BE relative file | Raw SHA256 |
| --- | --- | --- |
| 1324 | `supabase/migrations/03102026_1324_banking_pay_next_core.sql` | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| 0300 | `supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql` | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| 1519 | `supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql` | `924fe1d8feb9b3767bae3261ba711b7837b4c2479cf689c780f3e651e28875d3` |
| 1241 | `supabase/migrations/04102026_1241_bpay_next_component_selection.sql` | `21716604ef859fea23dd1f15d0e5f642528b2f9457e9e96c7acf32615164fe3d` |
| 0480 | `supabase/migrations/04102026_0480_banking_pay_next_internal_zero_execution.sql` | `032fcf4d3456b66c67c49a6399ee6a13207e223cb37b5d5fff0de3439e70f70b` |
| 1827 | `supabase/migrations/04102026_1827_banking_pay_next_candidate_destination_group.sql` | `af9849f6e83784b0064ee58179aabc740d396764c3639ef7132967f394c9512d` |
| 0501 | `supabase/migrations/04102026_0501_banking_pay_next_preparation_expiry.sql` | `c9758121675a070ae40f1be352a40247f357309ae94efca59baf6de002cac845` |
| 0340 | `supabase/migrations/04102026_0340_banking_pay_next_protected_source_receipt.sql` | `3b2f62d68c8138f871dd5736dfd3176d7b762b90154eb3c44b771e0dc3b9deab` |
| 0350 | `supabase/migrations/04102026_0350_banking_pay_next_reissue_cancel_request.sql` | `1cf953a9bc2c4e9610827e652c2ad0abe730ae5e02ff5b86bf481a6de5def6c4` |
| 1647 | `supabase/migrations/04102026_1647_banking_pay_next_work_collection_write_off.sql` | `eb17aa10fbcc81d50210f0e1ed8998e9c80b0c35d621a4a85ebe59e8b5d484e2` |
| 0320 | `supabase/migrations/04102026_0320_banking_pay_next_case_create_request.sql` | `870ff18f1d7d9af464c19f0138da2bc4438ec1953dc82800653069cdd53e042b` |
| F06 2013 | `supabase/migrations/09102026_2013_banking_pay_payment_history_scope_indexes_v1.sql` | `cb08a6b28d8a2eeace2e0f4ed3561c84dcda33022bb0fe54d7f46ee5e43eacc2` |
| F06 2010 | `supabase/migrations/09102026_2010_banking_pay_next_source_book_binding.sql` | `d2641717de3967aa30e200d75f9e4fde801b31c7eeef9cc8db84efb36d9266ea` |
| physical1206 | `supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql` | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| PS07 0614 | `supabase/repeatable/10102026_0614_banking_pay_next_ps_c08_history_authority_v1.sql` | `abecabcaa00883d1ebd2735c4f28df313a8a6161f4dce71272a57b4bd590d340` |
| PS11 0623 | `supabase/repeatable/10102026_0623_banking_pay_next_ps_c12_recorded_net_history_v1.sql` | `f53ca9dae5e297c628b5e1abcf491ceec1dd3bbea05e99aa2fea61fec108573e` |
| operational0250 | `supabase/repeatable/04102026_0250_banking_pay_next_frozen_detail_service_v1.sql` | `ca6e50fcb6324e9e3a39e840a664ddfa53a7a7d96d9d0fb9859d65743614e091` |
| Worker | `broker/src/bpay-next-history.js` | `a402fa5f72baf341235b0c35a513bed6cb47c5df17e3b7f461918e86a7a29cb9` |
| registry | `docs/bpay-next/final-definer-closures.json` | `a917f11d77faca02dd4ad17b4e5dba85eaa4d6126274d60383f8327f31eff068` |

FE history consumer inspected: `js/bpay-next-payment-history.js`, raw SHA256 `1923e375f1583d03deb00c1a020e565c2ff908e785e9e3a48628915717778ed9`. It is the existing history consumer, not a complete filtered Summary export. Ownerstart29–34, card06:1636–1657, R2:82–84, current checklist876–901 and file08 S1seq287 control the scope.

## Fresh PS-15 gap correction and implementation reconciliation

Root subsequently identified a concrete transport gap: core work_revision already retains candidate_display_name, candidate_reference, client_display_name, job_title, band_label and timesheet_reference, but the old REVISIONS projection/Worker/FE allowlists omitted them. This supersedes the earlier include-only D03 recommendation and the assertion that no transport hunk was needed. D01 no-missing-index finding remains valid: all six are fields of the already indexed captured revision, with no new schema/index/live Client lookup.

The current finalRoot **and finalPhysical** are now `supabase/repeatable/10102026_0653_banking_pay_next_ps_c16_indexed_history_v1.sql`, a single-transaction full physical1206 successor. Static comparison confirms the only function-body delta is these six REVISIONS projection fields; the52 sections, parent/cursor semantics, state independence, bounds and role/grants remain the original behavior. BE/FE52-section field mirrors must use this current projection. Preserve original1206 and0623 history as predecessors; EX41/CM18 re-read current0653.

The original table/index inventory remains applicable. Whole stored frozen display attribution now reaches the existing history route, without a duplicate capture or live display fallback. Approved S1/new-filter/fixed-export ownership remains as stated above. This is source reconciliation only, no execution/installed/app claim.

Fresh0653 raw SHA256 `a6c76d789c3ffae22ed7b7749fa0943807a16c0477cf8cbe2cd56e878c33cec3`. BEhistory raw SHA256 `1262a7961045a44b294939257db93e2ef54a3cf0754f76f90e8ebc52982335e0`; FEhistory raw SHA256 `467a78cd820f61cb0ee0e461f02f78d6c06f468024e7121f629770351f074d4e`.
