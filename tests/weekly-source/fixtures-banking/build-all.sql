-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
-- Builds every named evidence state into a DISPOSABLE LOCAL clone.
--
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f install.sql
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f build-all.sql
--
-- IMPORTANT: run this WITHOUT wrapping it in an explicit transaction.  psql's
-- default autocommit gives each statement its own transaction, which is what the
-- real pre-bank cancellation chain needs: `pay_payment_correction_expand_work`
-- sets `run_after_utc = clock_timestamp()` while
-- `banking_pay_operation_claim_next` compares against `now()`, so a later phase
-- can only be claimed from a later transaction.

\set ON_ERROR_STOP on
set statement_timeout = '300s';

select ws_banking_fixture.base_world_v1() ->> 'actor_user_id' as base_world_actor;

-- BUILD ORDER MATTERS FOR THIS ONE STATE.
-- `public.pay_set_paye_net_manual` runs `public.pay_batch_validate_freshness`,
-- whose installed PAYE guardrail refuses BATCH_STALE / PAYE_GUARDRAILS_CHANGED
-- with "A PAYE draft batch already exists. Cancel or delete the existing PAYE
-- draft before creating another PAYE draft." as soon as any other PAYE Draft is
-- open.  The Binding C state is therefore built first, while no other Draft
-- exists.  Its batch stays DRAFT afterwards, which is exactly `R30`'s "while the
-- Draft is alive the root stays FROZEN".
select ws_banking_fixture.state_paye_net_manual_void_v1() ->> 'state_key' as built;

-- Cancellation states (real installed owners)
select ws_banking_fixture.state_batch_cancelled_whole_binding_a_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_candidate_cancelled_out_of_multi_draft_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_applied_with_blockers_v1('OUTSIDE_FAMILY') ->> 'state_key' as built;
select ws_banking_fixture.state_applied_with_blockers_v1('INSIDE_FAMILY') ->> 'state_key' as built;
select ws_banking_fixture.state_applied_with_blockers_v1('NO_ITEM_IDENTITY') ->> 'state_key' as built;
select ws_banking_fixture.state_batch_aborted_failed_draft_create_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_batch_cancelled_whole_binding_a_complete_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_same_family_draft_remainder_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_multi_draft_remainder_settled_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_pre_bank_cancel_voided_transfer_v1() ->> 'state_key' as built;

-- Advance every pending correction operation through PROCESS_CHUNKS and FINALISE.
-- Each call is its own transaction; five calls is more than the chain needs.
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_1;
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_2;
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_3;
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_4;
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_5;

-- Only now may a request be promoted to APPLIED_WITH_BLOCKERS: while it is
-- promoted it no longer owns the installed batch gate.
select ws_banking_fixture.finish_applied_with_blockers_v1('OUTSIDE_FAMILY') ->> 'state_key' as finished;
select ws_banking_fixture.finish_applied_with_blockers_v1('INSIDE_FAMILY') ->> 'state_key' as finished;
select ws_banking_fixture.finish_applied_with_blockers_v1('NO_ITEM_IDENTITY') ->> 'state_key' as finished;

-- The terminal Binding A operation, the same-family DRAFT remainder's
-- re-projected item, and the settled remainder of the R41 pair.  All three run
-- after the chain has finished, each in its own transaction.
select ws_banking_fixture.finish_batch_cancelled_whole_binding_a_complete_v1() ->> 'state_key' as finished;
select ws_banking_fixture.finish_same_family_draft_remainder_v1() ->> 'state_key' as finished;
select ws_banking_fixture.finish_multi_draft_remainder_settled_v1() ->> 'state_key' as finished;

-- Settlement states (named seeds)
select ws_banking_fixture.state_batch_settled_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_settled_batch_retains_schedule_kind_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_batch_failed_completed_mixed_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_partial_settlement_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_settlement_history_conflict_v1('DUPLICATE_DIFFERENT_SIGNATURE') ->> 'state_key' as built;
select ws_banking_fixture.state_settlement_history_conflict_v1('DUPLICATE_IDENTICAL') ->> 'state_key' as built;
select ws_banking_fixture.state_settlement_snapshot_conflict_v1('MISSING') ->> 'state_key' as built;
select ws_banking_fixture.state_settlement_snapshot_conflict_v1('EMPTY_SIGNATURE') ->> 'state_key' as built;
select ws_banking_fixture.state_settlement_snapshot_conflict_v1('CONFLICTING_SECOND') ->> 'state_key' as built;
select ws_banking_fixture.state_settlement_snapshot_conflict_v1('SIGNATURE_MISMATCH') ->> 'state_key' as built;
select ws_banking_fixture.state_root_settled_in_two_batches_v1() ->> 'state_key' as built;

-- WP-11a review F1 second model case: the downward recovery restated on the
-- demoted version of the rotated family (UI-012 on a fixture, not a seed).
select ws_banking_fixture.state_rotated_root_restated_downward_v1() ->> 'state_key' as built;

-- Transfer, operation, reservation and void states
select ws_banking_fixture.state_transfers_unknown_and_pending_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_transfers_returned_and_reversed_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_operation_history_forms_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_umbrella_batch_level_transfers_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_reservation_lifecycle_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_synthetic_unbound_void_v1() ->> 'state_key' as built;
select ws_banking_fixture.state_born_voided_item_v1() ->> 'state_key' as built;

select state_key, build_method, proofs
from ws_banking_fixture.state_register
order by state_key;
