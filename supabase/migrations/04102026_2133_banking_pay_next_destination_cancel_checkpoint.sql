-- L22: bounded all-original-leg CHECK pages retain REQUESTED/INTENT until the
-- complete certificate exists. Permit their nonnegative checkpoint without
-- starting/releasing cancellation. 0460 still admits only checkpoint0 INSERT
-- and certificate-backed +1 transitions;2102 certifies the actual leased page.
-- Replace only the exact audited0450 lifecycle CHECK, preserving its other
-- branches and every other CHECK/FK. No business update, new money or grant.

\set ON_ERROR_STOP on

begin;

do $audited_predecessor$
declare v_actual text;v_expected text;
begin
  select pg_catalog.pg_get_expr(c.conbin,c.conrelid) into strict v_actual
    from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_case_cancel_binding'::regclass
      and c.conname='bpay_next_case_cancel_binding_check2' and c.contype='c' and c.convalidated;
  -- Exact installedPG17 expression; normalise whitespace only. Preserve
  -- parentheses/grouping, case, casts, operators and every condition.
  v_expected:=$check$
    (((status = 'REQUESTED'::text) AND (stage = 'INTENT'::text) AND (checkpoint = 0)
      AND (started_at_utc IS NULL) AND (finished_at_utc IS NULL) AND (expected_case_hold_count IS NULL)
      AND (accepted_projection_revision IS NULL) AND (projection_id IS NULL) AND (net_state_id IS NULL) AND (transfer_id IS NULL))
    OR ((status = 'CANCELLING'::text) AND (stage = ANY (ARRAY['WORK'::text, 'CASE'::text, 'FINAL'::text]))
      AND (started_at_utc IS NOT NULL) AND (finished_at_utc IS NULL)
      AND (expected_case_hold_count IS NOT NULL) AND (accepted_projection_revision IS NOT NULL))
    OR ((status = 'CANCELLED'::text) AND (stage = 'COMPLETE'::text) AND (started_at_utc IS NOT NULL)
      AND (finished_at_utc IS NOT NULL) AND (expected_case_hold_count IS NOT NULL) AND (accepted_projection_revision IS NOT NULL)
      AND (released_work_count = expected_work_count) AND (released_case_hold_count = expected_case_hold_count))
    OR ((status = 'BLOCKED'::text) AND (stage = 'BLOCKED'::text) AND (finished_at_utc IS NOT NULL)
      AND (released_work_count = 0) AND (released_case_hold_count = 0)))
  $check$;
  if pg_catalog.regexp_replace(v_actual,'[[:space:]]','','g')
    is distinct from pg_catalog.regexp_replace(v_expected,'[[:space:]]','','g') then
    raise exception using errcode='55000',message='BPAY_NEXT_DESTINATION_CANCEL_PREDECESSOR_CHECK_MISMATCH';
  end if;
end $audited_predecessor$;

alter table private.bpay_next_case_cancel_binding
  drop constraint bpay_next_case_cancel_binding_check2;
alter table private.bpay_next_case_cancel_binding
  add constraint bpay_next_case_cancel_binding_check2 check (
    (status='REQUESTED' and stage='INTENT' and released_work_count=0 and released_case_hold_count=0
      and work_cursor is null and case_hold_cursor is null and started_at_utc is null and finished_at_utc is null
      and expected_case_hold_count is null and accepted_projection_revision is null and projection_id is null and net_state_id is null and transfer_id is null)
    or (status='CANCELLING' and stage in ('WORK','CASE','FINAL') and started_at_utc is not null and finished_at_utc is null
      and expected_case_hold_count is not null and accepted_projection_revision is not null)
    or (status='CANCELLED' and stage='COMPLETE' and started_at_utc is not null and finished_at_utc is not null
      and expected_case_hold_count is not null and accepted_projection_revision is not null
      and released_work_count=expected_work_count and released_case_hold_count=expected_case_hold_count)
    or (status='BLOCKED' and stage='BLOCKED' and finished_at_utc is not null and released_work_count=0 and released_case_hold_count=0));

-- The existing checkpoint>=0 column CHECK remains intact. Install current0460
-- and2102 before application use; no partially checked group may release a
-- financial hold. This one-time predecessor assertion must not be reapplied.

commit;
