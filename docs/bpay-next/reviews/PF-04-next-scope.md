# PF-04 sequence 22: current scope and cleanup census

10 October 2026. Read-only source inventory/design; no product edits, gate/unit/SQL/runtime/fixture/hosted execution or checklist writes. Full normal R3 card 06:1741–1762, BUILD checklist 538–559 and all five exact CSV deliverables read. This is **No history scans in routine work**, dependencies F-06/F-11/PF-06. The requested processing/reply-bounds census is useful reuse context, but those transport concerns are PF-01/PF-02 and B7; they do not replace PF-04's five deliverables. Historical R2/lane clauses do not override current Rules/Build/Done or OR-45. F25 Rule B remains closed.

## Exact remaining delivery

| Row | Actual required object | Current saved-source state / smallest work |
| --- | --- | --- |
| D01 | private.bpay_next_terminal_cleanup_v1 | No current CREATE definition found. Bounded service-only terminal temporary-row cleanup, exact retained-reference/replay protection below. No new financial command kind or protocol. A fixed public service wrapper is supporting transport if needed. |
| D02 | private.bpay_next_operational_health_v1 | No current CREATE definition found. Read-only queue age, error counts, oldest active hold age, PF06 step latency and table sizes. No tariff/budget/fee figure or authority mutation. |
| D03 | broker/src/bpay-next-background.js cleanup dispatch | Existing bounded NEXT recovery tick has ENROLL, READY/LEASE_EXPIRED wake pages and expiry dispatch. Add one awaited cleanup lane through a fixed wrapper, active_owner NEXT only; no cron/wrangler change or whole drain. |
| D04 | future joined application checklist | Record exact clauses and NOT_RUN, reuse later actual joined receipt. No new scenario/harness. |
| D05 | targeted history-point source/transport check | Reuse existing gate module/normal source units and final-definer registry. Gate design is required; cleanup/health need no separate architecture. |

B7:221–225 requires row+byte bounds, one bounded RPC page rather than the 120-second database timeout, real measurement identities, and these specific monitoring/cleanup owners. PF04 additionally forbids routine history-driven work, whole-population final transactions, G01 processing architecture, bank-edit earnings pricing and retry money changes. CM09 later owns repeated-move/audit-reader closure; this gate does not wait for that chain.

## Current routine/history gate seed

Use the registry's actual physical finals, not every predecessor body. Exact initial routine set: capture_position_page, apply_position_page, hold_fresh_position_page, pin_selected_work, prepare_case_page, complete_no_case_worker, confirm_simple_run and build_simple_transfer_page (all private.bpay_next_*_v1). Current final families are 2018, 1430, 1630 and Rule-B0151. The source reader/history modal is on-demand, not in this routine set.

Canonical history seed: private.bpay_next_financial_effect, bpay_next_case_event, bpay_next_transfer_outcome, immutable CSV instructions/NET projections when read as historical authority, and public.bpay_next_payment_history_page_v1 plus its reader calls. Do not reject financial writes or every private point-key row lock. The two actual positive point reads found in the current initial set are:

- 2018:1717 transfer-build reads net_projection by exact `id=v_transfer.projection_id`.
- Rule-B0151:913 case-prepare validates original FUNDED case_event by exact `e.id=v_rule.original_funding_event_id`, with case/kind/time tuple predicates.

The other six selected current bodies did not directly read the five historical relation names in this finite source census. This is a seed for D05, not a claim of a transitive whole-finance graph proof. Enumerate these exact positive patterns and reject broad history scans/joins/reader calls inside the named routine set. Keep each body bound to its final registry entry and inspect actual SQL clauses, not a bare history keyword. No new proof framework.

## Precise temporary-row / FK census

Core1324:439–448 selection_page has `(run_id,page_no)` primary key, original ordered work-ID JSON, first_selection_no and item_count, with outgoing RESTRICT FK to pay_run. **No inbound FK to selection_page was found** in current managed migration/repeatable source. Core comments436–438 explicitly say its exact bytes distinguish identical retry from changed input. The actual append owner0570:148–165 reads that page and original work_choice fields before checking whether the run is still appendable; deleting it changes existing exact replay behavior. Retain run_selection, selection_candidate, work_choice, selected components, Rule-B query snapshot and pin_v2. None is disposable merely because its run ended.

Seven exact inbound job FK edges exist; all ON DELETE RESTRICT:

| Retained relation / column | Target | Physical source / line |
| --- | --- | --- |
| bpay_next_case_allocation_state(job_id,command_id,candidate_id,module_epoch) | job(id,command_id,candidate_id,module_epoch) | 04102026_0300 case_instruction_core:339 |
| bpay_next_preparation_expiry_worker.job_id | job(id) | 04102026_0501 preparation_expiry:55 |
| bpay_next_preparation_expiry_worker.original_job_id | job(id) | same:57 |
| bpay_next_work_collection.last_job_id | job(id) | 04102026_1519 work_collection:32 |
| bpay_next_destination_posted_leg.job_id | job(id) | 04102026_2025 destination_lifecycle:19 |
| bpay_next_run_work_query_pin_v2.prepare_job_id | job(id) | 05102026_0044 run_work_query_pin_v2:15 |
| bpay_next_effective_work_cut.prepare_job_id | job(id) | retained installed-unused 09102026_2153 accepted_input:190 |

Job's own outgoing FKs to original command/sequence, command_member and Candidate do not by themselves prevent deleting a job; the seven inbound edges do. Retain the corresponding records rather than deleting dependencies to make an otherwise ineligible job deletable. Retained unused2153 still has real schema authority and is not a cleanup target.

No direct immutable/delete trigger attachment on job or selection_page was found, including dynamic literal table-name census. New0352 job_lifecycle_clock is BEFORE INSERT OR UPDATE only. This **does not establish deletion eligibility**: original financial_job service0280:150/311/415 uses strict job lookup, including DONE CLAIM/STEP replay. Deleting an unreferenced DONE job can destroy the original retry receipt. Retain command/member/request/effect/outcome/hold/case history, frozen run rows and archived Timesheets. Preserve existing immutable guards; no trigger disable/drop, DELETE cascade or repair permission.

## Strict eligibility recommendation

- Candidate page scope: only original temporary selection_page, with terminal pay_run status COMPLETE or CANCELLED. PREPARING/REVIEW/DRAFT/EXECUTING remain nonterminal. Require selected-origin records and original exact page replay authority retained. Current source has no alternate immutable page replay receipt; therefore terminal status and no inbound FK alone are insufficient. Keep such pages until an exact existing retained origin is proven adequate; do not manufacture a baseline/replay journal.
- Candidate job scope: status DONE, no live lease, original command COMPLETE, no pending worker/enrollment/continuation obligation, **none** of the seven inbound references, and exact original replay output retained by an already valid authority. FAILED is a recoverable state and remains retained. Unknown kinds/states remain retained. The present public wrapper requires the job itself for terminal replay, so there is no unconditional positive deletion case proved by this census.
- Take one bounded keyset/limited candidate page and locked revalidation before DELETE. FKs remain the last line of defense against concurrent references. Report visited/deleted/retained counts honestly. A no-op page caused by genuine retention constraints is valid; do not claim that every terminal job is ephemeral or add another cleanup command protocol. Any bounded successor needed to preserve an original existing replay must be explicit builder work, not assumed in this inventory.

## Existing transport and normal unit reuse

Current limits: financial command 2,048 request / 131,072 response bytes / 8s; preparation 32,768 / 131,072 / 8s; job 2,048 / 131,072 / 8s with <=100 rows; metadata runtime and expiry 1,024 / 32,768 / 8s with <=100 rows. Background pageRows100/retry30s/lease120s are operational controls. Fixed transports set X-CloudTMS-Banking-Pay-Route-Class READ/PREVIEW_PAGE/OPERATION_NUDGE as appropriate. Exact allowed fields prevent caller pricing/population payloads. Actual bytes are checked before parsing; closed receipt decoders preserve original identities and uncertainty.

Existing normal source-unit files worth reusing: bpay-next-command.test.js (request341, closed replies351, oversized stream401, timeout413, late response455), runtime-rpc.test.js (one call114, exact bounded page161, cap196/312/319, duplicate JSON333), job-step/preparation/expiry-rpc/background tests, and source-gates/final-definers tests. These files are sources, not evidence of execution by this reviewer. The new cleanup transport should use the same raw-response/stream/deadline contract, not a generic unbounded request wrapper.

Current runtime timeout reports BPAY_NEXT_RUNTIME_TIMED_OUT with outcome_unknown/recovery direction after an attempted mutation; financial/preparation unknown outcomes preserve exact caller-owned identities and prohibit automatic re-dispatch. The shared index sbFetch wrapper at164804–164838 reports SUPABASE_FETCH_TIMEOUT408 when its own controller expires, but buffers res.text(); it is not substitute proof for a bounded NEXT response reader. Measured timeout versus definite SQL refusal must stay distinct. No hosted measurement has run.

Source receipts: background `d1d73c0559dda63c9b25b2799e1c2e0bb1055bf569bce82a25bb86ac81886855`; new0352 clocks `3ef9116ca77508cc1f419a2b7e209ed4a5e729e00f3b66de98d5bd40305ba0bc`; registry `a2699dead66e4c2601ad7b41822555c587a9524e086c4cd41709468724b61f0d`. Root is writing concurrently; these identify the inspected snapshot. No PF04 completion/installation/application acceptance is inferred.