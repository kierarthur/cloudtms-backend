# F-08 contract build investigation - 9 October 2026

Current conclusion: **M1-M4 already align in saved product source** after F-05/F-06/F-09/F-10/F-16. No proven contract mismatch currently justifies another SQL successor or product rewrite. F-08 still needs canonical source-gate alignment, the final contract completion table and linked F-25/F-11 cut-input completion. Source inspection does not prove installed/app acceptance.

This note was written by the read-only investigation agent. Root alone writes product; no tests, runtime, fixtures, SQL, hosted reads or network operations were executed. All compared exact paths, raw hashes, eight final routine definitions and the 18-kind/52-section contract data are recorded in `F-08-contract-build.json` beside this note.

## Authority and dependency position

Controlling scope is R3 foundation F-08 Rules/Build/Done plus checklist F-08-D01-D04. Dependencies are F-05, F-06, F-09, F-10, F-11 and F-16. Generated build sequence places F-08 at Seq17, after F-25 and F-11; that sequence controls over stale prose order. F-08 has no assigned editor-chain successor. F-04's master chain controls actual declarations: intake edits append only owned action/page kinds in dependency order; history changes remain PS-07 then EX-41 then CM-18. F-25 owns accepted_input/effective_work_cut and the 1600/1610/1620/0360 chain. Do not duplicate those owners in F-08.

Owner start sheet overrides old approval/STOP text: continue with recorded sensible builder decisions; preserve OR-44 accepted-click freeze, OR-45 clean launch/no old engine compatibility, OR-46 captured pay method/creditor and OR-47 snooze scope. Only ordinary changed-source/compile/seal/coupling gates are allowed. No SQL/payment scenarios, fixtures, test tooling, legacy-runtime rounds or browser acceptance are introduced here. Later genuine app acceptance belongs to the separate NEXT TEST agency.

## Final contract table (completion-note basis)

| Row | SQL final authority | Worker | Office | Current conclusion |
| --- | --- | --- | --- | --- |
| M1 / D01 | 2018 `private.bpay_next_confirm_simple_run_v1(uuid,bigint)` and `public.bpay_next_preparation_intake_v1(uuid,text,jsonb)`, plus0236 Draft reader | preparation.js INPUT/OUTPUTS.CONFIRM and result validator | office.js INPUT/RECEIPT.CONFIRM, receipt/adopt/setDrafts/openDraft | Same run, exact1-2 Draft addresses, PAYE then UMBRELLA; aligned |
| M2 / D02 | 2018 `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` plus0236 channel readers | 18 exact BPAY_NEXT_PREPARATION_PAGE_INPUTS with closed per-kind replies | thin factual PAGES subset and channel open/loadWorkers/loadStatus | All18 SQL kind/key lists equal Worker; thin UI channel contracts match |
| M3 / D03 | 2018 `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` CANCEL_REQUEST branch | command.js dynamic closed CANCEL_REQUEST schema | office.js dynamic requestBody/bindDraft/cancel control | Paired run/channel keys and exact whole-worker guard; aligned |
| M4 / D04 | 1206 `public.bpay_next_payment_history_page_v1(jsonb)` | history.js fixed RPC, request/cursor/section validators and authenticated route | payment-history.js same52 allowlists and bounded explicit pages | SQL registry, BE and FE52 section metadata equal exactly; informational reader activation-independent |

### M1: CONFIRM and Draft addresses

Browser/Worker input is `{action:'CONFIRM',run_id:UUID,review_revision:positive decimal bigint text}`. Fixed service RPC receives verified actor separately; browser cannot choose actor/function. Public output is `{run_id,phase:'DRAFT',review_revision:positive text,replay:boolean,drafts:[{run_id,pay_channel}]}`. Draft list has1 or2 exact objects, only PAYE/UMBRELLA, same accepted run; two rows are PAYE then UMBRELLA. Empty/duplicate/reversed/foreign-run/extra keys refuse. SQL private result keeps review_revision numeric internally; public intake stringifies it. Both first confirmation and exact replay include Draft rows from0236. Zero/cancelled workers still establish channel membership. One preparation is not split into Source versus ordinary runs.

Office keeps the parent run, immutable address list and separately selected pay_channel in journal v3; channel switch clears worker/detail scope only. Visible controls are Open PAYE Draft / Open Umbrella Draft. A prepared READY worker under confirmed DRAFT remains actionable; do not require a nonexistent post-confirmation worker transition.

### M2: channel pages and the18-kind closed service

Exact channel input/output:

- CHANNEL_DRAFTS args `{run_id}`. SQL raw reply `{rows:[{run_id,pay_channel}],complete:true}`. Worker adds `ok:true,kind:'CHANNEL_DRAFTS',next_cursor:null`; Office accepts that exact5-key envelope.
- CHANNEL_WORKERS args `{run_id,pay_channel,after_candidate_id:UUID|null,limit:1..100 integer}`. SQL raw `{rows,complete}`; Worker emits `{ok,kind,rows,complete,next_cursor}` with last emitted Candidate UUID cursor. Rows are exact run_worker_id/candidate_id/pay_channel/worker_status/frozen_ex_vat/frozen_vat/frozen_inc_vat/review_issue_code; money is exact text including zero and issue may be null. SQL orders and limits within the exact run/channel before packing. Worker verifies row channel equals request; cross-channel cannot pass.
- CHANNEL_STATUS args `{run_id,pay_channel,run_worker_id,after_transfer_no:positive int32 decimal text|null,limit}`. SQL verifies exact worker/run/channel then delegates existing status reader. Worker verifies header target_pay_channel then the existing STATUS contract and adds the exact cursor. Header has25 closed fields; net_pending equals request/projection revision inequality, original projection/null facts retained. Transfer number stays positive <=2147483647 decimal text.

All18 SQL kind/key lists were compared directly against Worker source, not a saved installed snapshot. They are:

- `COMPONENTS`: `work_id:uuid`, `expected_revision_id:uuid`, `after_line_no:integer?`, `limit:limit`.
- `WORK_CHOICES`: `run_id:uuid`, `after_work_id:uuid?`, `limit:limit`.
- `SELECTED_COMPONENTS`: `run_id:uuid`, `work_id:uuid`, `after_selection_no:integer?`, `limit:limit`.
- `CHANNEL_DRAFTS`: `run_id:uuid`.
- `CHANNEL_WORKERS`: `run_id:uuid`, `pay_channel:channel`, `after_candidate_id:uuid?`, `limit:limit`.
- `CHANNEL_STATUS`: `run_id:uuid`, `pay_channel:channel`, `run_worker_id:uuid`, `after_transfer_no:transferNo?`, `limit:limit`.
- `WORKERS`: `run_id:uuid`, `after_candidate_id:uuid?`, `limit:limit`.
- `REVISION_COMPARISON`: `run_worker_id:uuid`, `after_work_id:uuid?`, `limit:limit`.
- `LINES`: `run_worker_id:uuid`, `after_line_no:integer?`, `limit:limit`.
- `SHIFTS`: `work_id:uuid`, `expected_revision_id:uuid`, `approved_line_id:uuid`, `after_detail_no:integer?`, `limit:limit`.
- `BREAKS`: `work_id:uuid`, `expected_revision_id:uuid`, `approved_line_id:uuid`, `shift_detail_id:uuid`, `after_break_no:integer?`, `limit:limit`.
- `RATES`: `work_id:uuid`, `expected_revision_id:uuid`, `approved_line_id:uuid`, `after_bucket:bucket?`, `limit:limit`.
- `CURRENT_WORK`: `candidate_id:uuid`, `after_week:date?`, `after_work_id:uuid?`, `limit:limit`.
- `CURRENT_CASES`: `candidate_id:uuid`, `after_case_id:uuid?`, `limit:limit`.
- `CURRENT_STORED_CREDITS`: `candidate_id:uuid`, `after_legacy_case_id:uuid?`, `limit:limit`.
- `CASE_COMPONENTS`: `candidate_id:uuid`, `case_id:uuid`, `after_component_ordinal:integer?`, `limit:limit`.
- `STATUS`: `run_id:uuid`, `run_worker_id:uuid`, `after_transfer_no:transferNo?`, `limit:limit`.
- `RETURN_CASH`: `run_id:uuid`, `run_worker_id:uuid`, `after_transfer_no:transferNo?`, `limit:limit`.

Thin Office does not expose every page capability as a screen. It already implements CURRENT_WORK/CURRENT_CASES/CASE_COMPONENTS plus channel Draft/workers/status, worker list/lines/revision comparison/return cash; frozen detail has its own exact route. Missing later screen actions are owned by their later feature cards and do not justify inventing another page kind here.

### M3: channel cancellation

Base accepted body is `{action:'CANCEL_REQUEST',command_id,run_worker_id}`. If either run_id or pay_channel is present, both become mandatory: exact extended body has those two additional fields and no extras. SQL applies the same closed key selection and string/UUID/channel validation, then verifies worker membership in the exact run/channel before calling the existing whole-Candidate cancellation owner. Office includes both fields from selected frozen Draft and validates them locally against journal run/channel/worker before dispatch. No selected-line/subset cancellation, caller amounts or Candidate-global cancel is introduced.

Reply is exactly command_id,run_worker_id,sequence (positive bigint text),phase REQUESTED/CANCELLING/CANCELLED/BLOCKED,cursor positive bigint text|null,released_line_count nonnegative bigint text,blocked_code:string|null,replay:boolean. A BLOCKED phase iff blocked_code is nonnull is enforced. Missing extended key/wrong channel is request-invalid, actual SQL scope mismatch translates the existing 22023 error to COMMAND_REQUEST_INVALID; no unrelated run_id is echoed into this reply.

### M4:52-section informational history

Browser request exact keys: version,candidate_id,timesheet_id,section,parent_id,context_id,component_key,after,upper,limit. Exactly one scope UUID is nonnull, version PAYMENT_HISTORY_V1, limit integer1..100. Server injects actor_user_id into p_request for the sole fixed RPC. MANIFEST has null parent/context/component/cursors; section requests bind their actual captured parent/context links. Cursor exact keys: version PAYMENT_HISTORY_CURSOR_V1,scope_kind,scope_id,section,parent_id,context_id,component_key,upper,key. Continue with whole returned after/upper; refresh explicitly resets them for late commits.

All52 section names, parents, key types/field order and amount_scope equal across SQL/Worker/Office. POSITIONS adds current_residual_source_ex_vat only to actual row data in all layers; it is deliberately absent from the registry field list. No append to the manifest is needed for that established exception. Scalars remain strings/null, current cache/status distinct from immutable source/frozen target/whole-worker NET/whole-transfer cash. No approval-ready/current-revision dependency, all-history scan/count, cash allocation or moved-root fabrication is added.

Success history is `{ok:true,page}`; error is `{ok:false,error_code,retryable}` with no outcome_unknown. Page and manifest have their own exact closed keys. History uses existing service JWT+active-admin admission and a single exact fence exemption independently of NEXT activation. Financial preparation/command replies instead include outcome_unknown and their own MODULE_NOT_ACTIVE ownership gates.

**Gate interpretation to record as BUILDER DECISION if needed:** F-16 explicitly hides the Office history entry and requires ownerReady at UI open; F-08's joined correction and1206 explicitly leave the authenticated informational DB/Worker history read activation-independent. Retain both boundaries: no financial module-control read/gate is added to1206 or its Worker route, and no broadened anonymous/nonadmin access is added. This does not create a hidden financial preparation bypass. If later wording changes the UI visibility itself, its exact F-16 gate must be revisited rather than weakening history service permissions or claiming activation-dependent SQL.

## Remaining source work and source-gate changes

1. **Reread current final registry after F-25/F-11.** Current declarations already agree; preserve them unless a new precise contract mismatch is evidenced. Product edits currently warranted are only F-25's separately owned cutoff bindings and F-11 wiring. F-08 should not regenerate0236/1206 or append another intake successor merely to acknowledge agreement. Record M1-M4 final roots/signatures/hashes in completion note and link the checked source schemas.
2. **Fix canonical gate targets without weakening assertions.** tests/bpay-next-command.test.js:68 and:413 read obsolete0270 while final financial intake is2018. tests/bpay-next-preparation.test.js:331 reads obsolete0575 while final public intake/page is2018. Select the precise current signature's designated finalPhysical from final-definer-closures.json and inspect its CREATE statement. Keep unchanged0575 authority/args helpers from their actual final roots; required owner/ACL statements come from the designated closure. Do not concatenate an old and new duplicate function then let a stale matching regex certify the new body. Existing source scanner/routineDefinitions can select statement slices without execution; extend normal source gates, not a test-tooling programme. Historic-root assertions may remain separately labelled history preservation.
3. **Retain/add precise existing source-unit contracts.** Preparation units already cover CONFIRM1/2 addresses, wrong ordering/scope, channel Drafts/workers and zero amounts. Add CHANNEL_STATUS positive and wrong header channel to that existing unit if missing; normal state/header validation already exists. Command unit currently has no explicit run_id/pay_channel CANCEL_REQUEST branch coverage: add paired-key positive, one-key-only refusal, unsupported channel/extra subset rejection and fixed actual args forwarding; no DB rows or fixtures. FE existing office unit already proves two Draft addresses across resume and exact channel-bound whole-Candidate cancel. FE history unit proves BE/FE registry equality; BE history unit proves52 SQL equality and activation independence. Bind canonical source checks to the actual final1206 (later PS-07/EX-41/CM-18 successor when applicable), not a permanently frozen predecessor path.
4. **Preserve existing strong gates.** Keep request/auth-before-parse, fixed actor injection, decimal/bigint precision, closed keys, cursor/scope binding, body/UTF-8/deadline budgets, malformed receipt unknown mutation, identical retry, frozen Draft immutable inputs and historical reader independence. Keep18/52 exact count assertions here; later owners may change a contract only through their explicit ordered addition and corresponding consumers. Existing F-04 final-root gate remains controlling.
5. **Cutoff binding completion belongs to F-25.** Fresh source search found no accepted_input/effective_work_cut/seal_effective_work_cut definitions in live migration/repeatable folders at this note. addendum/R2_EXACT_CONNECTIONS.md section4 requires per-Candidate unique prepare-command receipt using existing agency_sequence, immutable selected-work child receipt and one leased resolver. Extend final0575 intake/seal-enqueue acceptance +1590 and1600, not0570's sealed manifest.1600 captured channel/query/revision and0360 captured week-floor/rule consume that record;1610/1620 captured Source origin and live money conservation remain separate. Same-key replay consumes the same record. No new browser actor/cutoff timestamp/amount fields or separate clock are authorized by F-08. Re-read and reconcile the current later successor as F-25 lands.

The master intake chain requires every later writer to preserve all existing actions/page kinds; record F-08 as contract reconciliation, not a competing intake owner. Frozen-boundary adjustments occur only under the named F-25 card and logged authority. Final-definer registry/contract coupling must be updated only when actual SQL changes, with original installed bodies left immutable.

## Error table and verification limits

Preparation/command known mappings:401 UNAUTHORIZED,403 FORBIDDEN,400 REQUEST_INVALID (22023/22003/date/UUID),404 SCOPE_NOT_FOUND(P0002),409 CONFLICT(check/unique) or NOT_ELIGIBLE(55000 other than module),503 MODULE_NOT_ACTIVE(55000 exact BPAY_NEXT_MODULE_NOT_ACTIVE),503 dependency unavailable(PGRST202/transport),503 timeout and response-invalid/too-large. Prefix is BPAY_NEXT_PREPARATION_ or BPAY_NEXT_COMMAND_. Both error envelopes include retryable/outcome_unknown; a typed known refusal does not prove the outcome of an earlier lost reply. History's separate prefix/maps retain403 permission,400 invalid,404 exact scope-not-found,503 dependency/timeout/page-too-large and502 malformed response; its reads have no unknown-mutation state. Frontend continues safe visible copy and exact stale-code invalidation established in F-16.

Root may run only owner-authorized normal source/compile/seal/coupling gates for actual changes. Do not generate fixtures, seed sets, SQL/payment rehearsals, performance rounds, browser fixtures or exposed LSV-109 diagnostic. Later accepted mixed Draft/cancel/history app journeys remain NOT_RUN here. No deployment/activation or hosted observation is implied by source hashes.

## Current source hashes

| Exact relative path | Raw SHA256 |
| --- | --- |
| `BUILD-BE/supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| `BUILD-BE/supabase/repeatable/06102026_0236_banking_pay_next_channel_draft_readers_v1.sql` | `a90b3b2fe01e079e7177bedde583d5e0b66dd037b2344272a6a8fb288d008719` |
| `BUILD-BE/supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql` | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| `BUILD-BE/supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql` | `4c651558f56b85c29cf10f21a1d3168ce259f75a66001a24a7f53dbc76216a87` |
| `BUILD-BE/broker/src/bpay-next-preparation.js` | `defd708e500ea645ee272d326a7b0ed15f03c8b07dc280d3c9590933548c8ed6` |
| `BUILD-BE/broker/src/bpay-next-command.js` | `95f082f6fef61932b0498ea007d47966692ec15094d954fc9cbbf4bd1d7ed059` |
| `BUILD-BE/broker/src/bpay-next-preparation-rpc.js` | `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34` |
| `BUILD-BE/broker/src/bpay-next-command-rpc.js` | `060ce9bdab50576aec731d0b9b3c6499a608e3cd71c17a3c9bdf54483db1b68e` |
| `BUILD-BE/broker/src/bpay-next-history.js` | `81d4ca2fb90b1bbec7dea3a41698933f70b306833079484d45a89e4e77738451` |
| `BUILD-BE/tests/bpay-next-preparation.test.js` | `c1526acd3b6f57f4c60ccf39e2d59d6b21f012f9e258cf9025f224a45fdddeb7` |
| `BUILD-BE/tests/bpay-next-command.test.js` | `0d847330d80e8c8e3fad9225e00726e26afd364e7903c810d71ca66cb58fca3d` |
| `BUILD-BE/tests/bpay-next-payment-history-caller.test.js` | `7ea8f4ef53e3bc4fc7c02188fc0ffca629f17bd370ddcd41685b40503b5ef28e` |
| `BUILD-BE/tests/bpay-next-preparation-rpc.test.js` | `dc65cf3e1b0d0d8934c57226357e70fcc00324809d5f537f42667eb4725676e7` |
| `BUILD-BE/tests/bpay-next-command-rpc.test.js` | `60ce0eecd5da3c5ad6be7cca3d65ac3e274b4fb0f489d3288cd8d57e397ae9a9` |
| `BUILD-FE/js/bpay-next-office.js` | `a4ba8ea0b63f3723a062dea7f915e91ed193c3c20c03e9dfae7765e8de25a356` |
| `BUILD-FE/js/bpay-next-payment-history.js` | `715a1035f88204b39771932a479f733b98ef7c971656c77767bd396861a24e9b` |
| `BUILD-FE/js/bpay-next-owner.js` | `4947d2e61cd4fa734cbaa99b05c415bad8552865d286233123cc48f34e1ca922` |
| `BUILD-FE/tests/unit/bpay-next-office.test.cjs` | `6e905cab92c36eafbea0425524038f7e3157fba2014d4711fa1164dcfeca6048` |
| `BUILD-FE/tests/unit/bpay-next-office-frozen-detail.test.cjs` | `3ac53acadc4251376f2ffcdc703892414bcabaee8447a2e8837848a1df1e6b89` |
| `BUILD-FE/tests/unit/bpay-next-payment-history.test.cjs` | `48b496e942232ae15e7bedebe7da61aa89e6fe65bc5fd71e09c348caf3ce7cdf` |

JSON beside this note contains the full final CREATE definitions and contract data for source review. It contains no fixture/data/credential material.
