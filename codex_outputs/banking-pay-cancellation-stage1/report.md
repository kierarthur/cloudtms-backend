# CloudTMS Banking Pay cancellation — Stage 1 implementation report

## Outcome

Stage 1 is complete in the isolated cancellation worktree. The implementation contains the locked schema change, eight new SQL functions, fifteen replacement SQL functions, four compatibility ACL retirements, and a focused SQL-contract regression suite.

The implementation remains disabled. Nothing was installed in TEST, deployed, committed, pushed, or merged.

## Authority and isolation

- Backend authority: `51862116f470f7cba2332c4b310cb92af52c0a5b`.
- Implementation worktree: `C:\tmp\cloudtms-banking-pay-cancellation-stage1`.
- Main backend clone remained clean and unchanged.
- The separate bounded-scope v124 worktree was not edited.
- The cancellation implementation is isolated for the later designated integration pass.

## Implemented schema

One one-time migration was added:

- `supabase/migrations/04082026_1126_banking_pay_cancellation_schema.sql`

It adds exactly:

- one immutable eleven-column `pay_payment_correction_request_candidates` membership table;
- forced RLS, no policies, and no direct application-role table access;
- three request-proof columns;
- the `PLANNING` and `PLANNED` request states;
- the `PAYMENT_CORRECTION` operation type;
- five disabled operation-phase configuration rows, all with minimum chunk size 1 and maximum chunk size no greater than 100;
- five settings columns with the locked 10,000 candidate, 250,000 active-item, 128 item-per-candidate, and 512 source-row-per-candidate limits;
- no new trigger.

## Implemented SQL functions

Eight new function files were added:

1. `private.pay_payment_correction_sha256_v1`
2. `private.pay_payment_mutation_guard_v1`
3. `public.pay_payment_correction_reauth_bind_v1`
4. `public.pay_payment_correction_expire_due_v1`
5. `public.pay_payment_correction_status_get_v1`
6. `public.pay_batch_payment_status_page_v1`
7. `public.pay_payment_correction_selection_prepare_chunk_v1`
8. `public.pay_payment_correction_integrity_check_v1`

Fifteen current functions were replaced at their exact installed identities:

1. `banking_pay_operation_start`
2. `pay_bank_event_ingest`
3. `pay_bank_transfers_claim_provider_submit_chunk`
4. `pay_batch_auth_apply_action`
5. `pay_batch_auth_start`
6. `pay_batch_cancel`
7. `pay_batch_schedule`
8. `pay_batches_claim_due_scheduled`
9. `pay_no_money_unwind_apply_work_item`
10. `pay_payment_correction_authorise`
11. `pay_payment_correction_expand_work`
12. `pay_payment_correction_process_chunk`
13. `pay_payment_correction_request_start`
14. `pay_pre_bank_cancel_apply_work_item`
15. `pay_settle_rail`

Four unchanged compatibility bodies were made owner-only with no application execute grant:

1. `pay_payment_cancel_not_sent_and_recalculate`
2. `pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr`
3. `pay_payment_cancel_not_sent_and_recalculate_complete_v1`
4. `pay_payment_confirm_no_money_and_unwind`

## Locked behavioural outcomes

- One asynchronous operation lifecycle is used for every selection size.
- The browser and Worker do not perform per-candidate mutation RPC fan-out.
- Each Worker advance will call one bounded database phase RPC.
- The SQL phase owner performs at most one Workbench call per refresh group, with no more than 100 candidate IDs.
- No loop-to-completion or timeout-driven capacity discovery exists.
- Every competing payment mutation uses the shared mode-aware advisory-lock guard.
- Provider evidence remains ingestible in `AUTHORITATIVE_EVENT` mode.
- Manual `CONFIRMED_NOT_PAID` records evidence only; release remains a separate approved asynchronous action.
- `PAID_EVIDENCE_AFTER_RELEASE` is classified before the completed-event idempotency return, blocks new payment actions, retains both histories, and does not reverse settlement automatically.
- Policy X is preserved: post-draft cancellation reads and mutates frozen batch artifacts only.

## Overview and PAYE behaviour

Both financial candidate helpers now:

- void only the selected frozen active items;
- recalculate each affected candidate from its remaining unvoided frozen items;
- set a fully removed candidate's net bank amount to zero;
- preserve the stored PAYE net input as audit history while excluding the candidate from active PAYE schedule scope;
- recalculate `pay_batches.total_bank_out` and the remaining transfer amounts;
- preserve unselected candidates unchanged.

Finalisation refreshes `pay_batch_display_summary`, touches the Overview and Current Payment Status signals, and records that the active PAYE schedule is derived from the remaining unvoided frozen items.

## Verification completed

- PostgreSQL parser: 28/28 SQL files passed.
- Disposable PostgreSQL installation: 27/27 repeatable files installed successfully.
- Exact signed-off function identity comparison: 27 expected, 27 present, 0 missing, 0 unexpected.
- Function catalogue: 27 target names, 27 functions, 0 missing or extra overloads.
- Owner/security/ACL checks: 0 wrong owners, 0 untrusted execute grants, 0 private service grants, 0 missing public service grants, 0 compatibility service grants.
- Schema catalogue: 11 membership columns, 3 proof columns, 5 settings columns, 5 disabled operation rows, 0 new triggers, forced RLS, 0 policies, and 0 direct application-role table privileges.
- Required mode-aware guard callers: 11/11 present.
- Compatibility references in the new normal call graph: 0.
- Membership deletion paths: 0.
- Workbench call sites: exactly 2 alternatives, both inside the single SQL phase owner.
- JSON response contract audit: 22 JSON-returning functions checked, 0 missing required fields.
- New Stage 1 SQL-contract tests: 7 passed, 0 failed.
- Existing relevant Banking Pay regression tests plus the new suite: 27 passed, 0 failed.
- Trailing-whitespace check: 29 files checked, 0 findings.

## Not performed

- No TEST database migration or function installation.
- No TEST data mutation or payment/provider/settlement action.
- No Worker or frontend change in Stage 1.
- No Worker deployment, frontend deployment, or production access.
- No commit, push, pull request, or merge.
- No browser test, because Stage 1 contains database source only and is not installed in a runtime.

## Bounded-scope integration collision audit

Compared with `C:\tmp\cloudtms-bounded-scope-v124-stage1` at the same base HEAD:

- Direct SQL function-definition collisions: 0.
- Direct trigger-name collisions: 0; cancellation Stage 1 creates no trigger.
- Direct test-file collisions: 0.
- Current `broker/src/index.js` collisions: 0; neither Stage 1 worktree changes it.
- Schema-name collisions: 0. Both migrations alter `settings_defaults`, but add disjoint columns and constraints.

The following are required integration interactions, even though they are not same-file definitions:

1. Cancellation `REFRESH_WORKBENCH` calls `pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1` or `pay_workbench_enqueue_candidate_refresh_many`. Both existing functions call `pay_workbench_enqueue_candidate_refresh`, which bounded-scope replaces. Combined cancellation-refresh and Workbench queue tests are mandatory after merge.
2. Cancellation updates on `pay_advances`, `pay_finance_case_components`, and inserts on `pay_finance_case_events` invoke bounded-scope replacements of `pay_workbench_mark_candidate_dirty` and `pay_workbench_mark_finance_case_dirty` through existing triggers.
3. Cancellation updates on `pay_batch_items`, `pay_batch_candidates`, `pay_batches`, and `pay_bank_transfers` invoke the bounded-scope replacement of `pay_timesheet_summary_pay_state_refresh_trigger` through existing summary/pay-state triggers.
4. Cancellation insert/update on `pay_bank_transfer_events` invokes the new bounded-scope transfer-event invalidation triggers.
5. Cancellation update on `pay_advance_reservations` invokes the new bounded-scope reservation invalidation trigger.
6. Bounded-scope owns the future Workbench lane edits in `broker/src/index.js`; cancellation Stage 2 will also edit that file for routes, correction advancement, expiry, due scheduling, cron ownership, and provider receipt processing. Bounded-scope must be integrated first, then cancellation Stage 2 must be rebased and merged by symbol in one designated integration pass.

Trigger objects that require combined cancellation verification include:

- `trg_pay_workbench_mark_candidate_dirty__pay_advances`;
- `trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_com`;
- `trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_eve`;
- `trg_ts_summary_pay_cache_advances_au`;
- `trg_ts_summary_pay_cache_finance_components_au`;
- `trg_ts_summary_pay_cache_items_au`;
- `trg_ts_summary_pay_cache_candidates_au`;
- `trg_ts_summary_pay_cache_batches_au`;
- `trg_ts_summary_pay_cache_transfers_au`;
- `trg_bpay_wb_transfer_events_insert_dirty_v1`;
- `trg_bpay_wb_transfer_events_update_dirty_v1`;
- `trg_bpay_wb_reservations_update_dirty_v1`.

There is no cancellation DELETE path, so the bounded-scope DELETE-only backstops do not collide with this Stage 1 implementation.

## Safety confirmation

- Secrets printed: no.
- Destructive SQL/RPC/actions run: no.
- Normal TEST deploy: no.
- Production deploy: no.
- Policy X drift: no.
- Raw diagnostic artifacts committed: no.
