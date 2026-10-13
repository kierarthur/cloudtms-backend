# F-08 current contract scope

Saved 10 October 2026. Read-only source reconciliation; no product change, test/runtime/SQL execution, data, network, deployment or checklist write. Observed BUILD-BE HEAD `fcad793c83b3213bde507561bbca1f71e4bf761f`; BUILD-FE HEAD `c85a3290061541ce2901191fde384e3554da6671`. F-25 is closed at BE `cadf7b8e4b834059b81f76435996c9123555b945` / FE `c85a329`. This note applies the surviving Rule B authority and does not reopen parked protocol work.

Authority: full F-08 card `01_BUILDER_RULES_AND_FOUNDATION.md:619–647`, checklist `IMPLEMENTATION_CHECKLIST.md:411–430` D01–D04, owner start sheet OR44–47, saved `F-08-contract-build.md/.json`, final-definer registry and current source. F-08 depends on F-05/F-06/F-09/F-10/F-11/F-16. Existing saved definition evidence remains current: all eight F-08 SQL CREATE definitions are exact unchanged strings in their registered final physical winners; the three principal SQL roots below and the history Worker/Office files are byte-identical to that evidence. The old note’s proposed accepted-input/resolver work is superseded by Rule B.

## Final four contracts

| Deliverable | Exact caller and reply contract | Registered SQL authority and actual callers | Current result |
|---|---|---|---|
| D01 / M1 CONFIRM | Request `{action:"CONFIRM",run_id:UUID,review_revision:positive bigint text}`. Result exactly `{run_id,phase:"DRAFT",review_revision,replay,drafts}`. `drafts` has one or two exact `{run_id,pay_channel}` rows, same run, PAYE then UMBRELLA when both; no duplicates, extra keys or Source/ordinary partition. Prepared worker can remain READY inside a DRAFT. Worker envelope `{ok:true,action,result}`. | `private.bpay_next_confirm_simple_run_v1(uuid,bigint)` and `public.bpay_next_preparation_intake_v1(uuid,text,jsonb)` → **2018**. `broker/src/bpay-next-preparation.js` OUTPUTS/result validator; `bpay-next-preparation-rpc.js` fixed RPC. Office `js/bpay-next-office.js` RECEIPT, receipt validation/controller CONFIRM, `loadDrafts/openDraft`. | Aligned. Normal source units already cover one/both addresses and wrong run/channel/order/extra keys. |
| D02 / M2 channel reads and all 18 page kinds | CHANNEL_DRAFTS `{run_id}` → `{rows:[{run_id,pay_channel}],complete:true}`. CHANNEL_WORKERS `{run_id,pay_channel,after_candidate_id:UUID|null,limit:1..100}` → eight fields `run_worker_id,candidate_id,pay_channel,worker_status,frozen_ex_vat,frozen_vat,frozen_inc_vat,review_issue_code`; zero amounts and cancelled membership remain valid. CHANNEL_STATUS `{run_id,pay_channel,run_worker_id,after_transfer_no:positive int32 text|null,limit:1..100}` → exact shared 25-field STATUS header plus transfer rows; header run/worker/channel must match; `net_pending` equals request/projection revision inequality. Worker envelope adds `ok,kind,next_cursor`; header only where defined. | `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` → **2018**; `private.bpay_next_channel_drafts_page_v1(uuid)`, `private.bpay_next_channel_worker_page_v1(uuid,text,uuid,integer)`, `private.bpay_next_channel_status_page_v1(uuid,text,uuid,integer,integer)` → **0236**. Status delegate `private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)` → **0580**. Worker PAGE_INPUTS/ROWS/STATUS_HEADER and fixed page RPC; Office PAGES, `loadWorkers/loadStatus`. | 18 active kinds aligned. Two retained ACCEPTED_* decoder kinds are dormant and unsupported by surviving SQL; actual Office controller refuses them before transport. Do not certify them as installed or revive them. Add one bounded CHANNEL_STATUS normal unit if still absent. |
| D03 / M3 whole-Candidate CANCEL_REQUEST | Base request `{action:"CANCEL_REQUEST",command_id,run_worker_id}`; extended request requires **both** `run_id` and `pay_channel`. No candidate/line subset or amount. Result exactly `{command_id,run_worker_id,sequence,phase,cursor,released_line_count,blocked_code,replay}`; phase REQUESTED/CANCELLING/CANCELLED/BLOCKED, blocked iff nonnull blocked_code; integer values are bounded decimal text. | `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` → **2018**; retains whole-Candidate cancel owner. `broker/src/bpay-next-command.js` closed input/result and command-rpc fixed intake; Office requestBody paired schema, controller scope bind, cancellation UI includes selected run/channel. | Aligned. Normal units reject subsets already. Add paired run/channel positive and single-key refusal checks if still absent; wrong scope is authoritative SQL/Office validation, not a fabricated Worker lookup. |
| D04 / M4 payment history, all 52 sections | Browser exact ten keys `version,candidate_id,timesheet_id,section,parent_id,context_id,component_key,after,upper,limit`; version PAYMENT_HISTORY_V1, exactly one Candidate/Timesheet scope, limit numeric 1..100. Server injects actor. Cursor PAYMENT_HISTORY_CURSOR_V1 binds scope, section, parent/context/component, upper and key; preserve exact after/upper together. Success `{ok:true,page}`; failure `{ok:false,error_code,retryable}`, no mutation outcome_unknown. Section parent/key/field order/amount scope remains exact across SQL/Worker/Office; POSITIONS’ extra current_residual_source_ex_vat is intentional. | `public.bpay_next_payment_history_page_v1(jsonb)` → **1206**. `broker/src/bpay-next-history.js` 52 definitions/validation/fixed RPC; Office `js/bpay-next-payment-history.js`. Trusted service JWT plus active admin auth; route’s exact history fence exemption and absence of NEXT activation gate are F-11 coupling requirements. Office ownerReady opening guard from F-16 stays qualified. | SQL/Worker/Office registries aligned and unchanged. Fresh normal unit reads registry-selected final physical source and checks all 52 definitions. Do not add NEXT module gate to the database/history route. |

Root abbreviations are full physical release roots (also the final physical file here):

- **2018** `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`.
- **0236** `supabase/repeatable/06102026_0236_banking_pay_next_channel_draft_readers_v1.sql`.
- **0580** `supabase/repeatable/04102026_0580_banking_pay_next_office_status_v1.sql`.
- **1206** `supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql`.
- Shared auth `private.bpay_next_preparation_authority_v1(uuid)` remains **0575** `supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql`; closed args `private.bpay_next_preparation_args_v1(jsonb,text[])` is **2018**. Use each actual statement separately; do not certify a predecessor by combining it with the successor.

Preparation and command refusal envelopes retain their existing prefixes (`BPAY_NEXT_PREPARATION_*`, `BPAY_NEXT_COMMAND_*`), status and retryability: invalid input 400; unauthorized/forbidden 401/403; missing scope 404; conflict/not-eligible 409; actual MODULE_NOT_ACTIVE 503; schema/dependency/response/timeout bounds 503. Preparation read has `outcome_unknown:false`; uncertain mutation requires exact original bytes/IDs on retry. History retains BPAY_NEXT_HISTORY_* (invalid 400, auth 401/403, missing scope 404, bounded dependency/page/timeout 503). Preserve exact error maps, journal retention, actor-before-parse, no fallback, one RPC, and original SEAL_ENQUEUE-only wake, including successful exact replay. No invented query protocol refusals are required.

## Closed page and section inventory

The SQL’s exact 18 active kind names match Worker after excluding the two explicit dormant decoders. All required keys/type declarations remain recorded in `F-08-contract-build.json` pageContracts; fresh SQL bytes are identical. Full active list: `COMPONENTS`, `WORK_CHOICES`, `SELECTED_COMPONENTS`, `CHANNEL_DRAFTS`, `CHANNEL_WORKERS`, `CHANNEL_STATUS`, `WORKERS`, `REVISION_COMPARISON`, `LINES`, `SHIFTS`, `BREAKS`, `RATES`, `CURRENT_WORK`, `CURRENT_CASES`, `CURRENT_STORED_CREDITS`, `CASE_COMPONENTS`, `STATUS`, `RETURN_CASH`.

Dormant Worker/Office decoder declarations: `ACCEPTED_CANDIDATES`, `ACCEPTED_WORK`. Their SQL successor was parked under Rule B. Office `createController.page` explicitly fails REQUEST_INVALID for these names before `options.transport.page`; they are not a progress dependency or a basis for creating a resolver. Preserve the closed F-25 decision.

Full 52 section list (metadata unchanged; full fields/parents/types/amount scopes in saved JSON historySections and matching current registries): `WORKS`, `REVISIONS`, `LINES`, `SHIFTS`, `BREAKS`, `RATES`, `SCHEDULE`, `POSITIONS`, `COLLECTIONS`, `EFFECTS`, `PUBLICATIONS`, `RUN_WORKS`, `RUN_POSITIONS`, `RUN_LINES`, `HOLD`, `WORKER`, `PROJECTIONS`, `TRANSFERS`, `MEMBERS`, `OUTCOMES`, `RETURN_CASH`, `CSV`, `INTERNAL_RECEIPT`, `GROUP_LEGS`, `EXPIRY`, `CASES`, `COMPONENTS`, `RULES`, `CASE_EVENTS`, `INSTRUCTIONS`, `CASE_HOLDS`, `RESULTS`, `COMMANDS`, `COMMAND`, `JOBS`, `NET_REQUEST`, `OUTCOME_REQUEST`, `RETURN_REQUEST`, `REISSUE_REQUEST`, `CANCEL_REQUEST`, `REISSUE_CANCEL`, `WRITE_OFF`, `CASE_CREATE`, `EXPIRY_REQUEST`, `CASE`, `PROTECTED_RECEIPT`, `SOURCE_DETAIL`, `POLICY`, `CHOICE`, `SELECTION`, `WORKER_CASE_HOLDS`, `COMMAND_WORKER`.

## Rule B join and only necessary follow-up

Later F-08 selection/capture references must use original choice `expected_revision_id`, the captured `selection_candidate` channel/Umbrella/VAT/floor, exact run/work/revision `bpay_next_run_work_query_snapshot`, and existing `pin_v2` path. Registered seal/claim/pin consumers now resolve to `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql`. Keep revision-changed REVIEW, captured query evidence, live money counters under original locks, and original PREPARE seal/enqueue ordering. Do not reference retained-but-unused 2153/resolver or any parked accepted input/projection progress authority. No producer, invoice, Candidate or finance-origin change belongs to F-08.

The earlier source-gate mismatches are already fixed in current saved `tests/bpay-next-command.test.js` and `tests/bpay-next-preparation.test.js`: finalRoutine resolves the registry, extracts exactly one matching CREATE statement with the source scanner/signature parser, and checks ACLs in that actual closure. The history unit similarly selects final physical 1206. Historical schema/owner conservation checks remain separately labelled. These fixes must be retained, not repeated.

Only remaining bounded work identified in current saved source:

1. Add CHANNEL_STATUS normal source-unit success with exact run/worker/channel/header and wrong-channel reply refusal; preserve int32 cursor cap. Existing STATUS and CHANNEL_DRAFTS/CHANNEL_WORKERS cases remain.
2. Add CANCEL_REQUEST paired run/channel positive, exact args forwarding and run-only/channel-only refusal with zero dependency calls; preserve existing subset/extra-key/receipt tests. A foreign channel within the same valid shape is rejected by actual SQL scope, while Office rejects a channel outside its selected Draft; the generic Worker intentionally does not invent database membership.
3. Preserve/verify F-11 wiring of trusted history auth and its exact read-only fence exemption. Record this table as F-08 final contract evidence; normal source gates are root’s task. The checklist’s later whole-app mixed-run observation remains separate and was not executed here.

No new SQL or Worker/Office product contract change is justified by this reconciliation.

## Fresh raw-byte evidence

| File | SHA-256 | Same as original F-08 evidence |
|---|---|---|
| `BE/supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` | yes |
| `BE/supabase/repeatable/06102026_0236_banking_pay_next_channel_draft_readers_v1.sql` | `a90b3b2fe01e079e7177bedde583d5e0b66dd037b2344272a6a8fb288d008719` | yes |
| `BE/supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql` | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` | yes |
| `BE/supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql` | `4c651558f56b85c29cf10f21a1d3168ce259f75a66001a24a7f53dbc76216a87` | yes |
| `BE/broker/src/bpay-next-preparation.js` | `9271ec0086bffdd9779707bf44407eba0923b1392317eb142e46c8f8e0338749` | no; current saved read |
| `BE/broker/src/bpay-next-command.js` | `95f082f6fef61932b0498ea007d47966692ec15094d954fc9cbbf4bd1d7ed059` | yes |
| `BE/broker/src/bpay-next-preparation-rpc.js` | `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34` | yes |
| `BE/broker/src/bpay-next-command-rpc.js` | `060ce9bdab50576aec731d0b9b3c6499a608e3cd71c17a3c9bdf54483db1b68e` | yes |
| `BE/broker/src/bpay-next-history.js` | `81d4ca2fb90b1bbec7dea3a41698933f70b306833079484d45a89e4e77738451` | yes |
| `BE/tests/bpay-next-preparation.test.js` | `5ceb752d39b24061a39d0d8c6e4c69db4e1acfd2c9f27836ba8884c46670d1a7` | no; current saved read |
| `BE/tests/bpay-next-command.test.js` | `b1051f3dcf536c0616d8699ee7bf98c4fe4c4658af8a57113e93e0645ee8bade` | no; current saved read |
| `BE/tests/bpay-next-payment-history-caller.test.js` | `8a09560333f72c485564777980d0c82af4451d0f4e73f9e02f55cc01f54fc98c` | no; current saved read |
| `BE/tests/bpay-next-preparation-rpc.test.js` | `dc65cf3e1b0d0d8934c57226357e70fcc00324809d5f537f42667eb4725676e7` | yes |
| `BE/tests/bpay-next-command-rpc.test.js` | `60ce0eecd5da3c5ad6be7cca3d65ac3e274b4fb0f489d3288cd8d57e397ae9a9` | yes |
| `FE/js/bpay-next-office.js` | `ad62e30d80660757529b402e0f260f263b57184d56da2dd4dc9f89595acadd9c` | no; current saved read |
| `FE/js/bpay-next-payment-history.js` | `715a1035f88204b39771932a479f733b98ef7c971656c77767bd396861a24e9b` | yes |
| `FE/js/bpay-next-owner.js` | `4947d2e61cd4fa734cbaa99b05c415bad8552865d286233123cc48f34e1ca922` | yes |
| `FE/tests/unit/bpay-next-office.test.cjs` | `b59979ae82548952da91de39d1e4c3e24d2798564708becfe1f253a6d5bc9b79` | no; current saved read |
| `FE/tests/unit/bpay-next-office-frozen-detail.test.cjs` | `a9cb950db10f77791bb239a64f2526a41bbbbd0355f70c901db75e8ea165a2ec` | no; current saved read |
| `FE/tests/unit/bpay-next-payment-history.test.cjs` | `48b496e942232ae15e7bedebe7da61aa89e6fe65bc5fd71e09c348caf3ce7cdf` | yes |

All hashes are source bytes, not installation/runtime proof. No test was executed by this investigator. Relevant product files were only read.
