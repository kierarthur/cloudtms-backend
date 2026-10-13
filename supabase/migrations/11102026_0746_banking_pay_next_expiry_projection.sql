-- DR08: bounded projection continuation on the existing private expiry binding.
-- No business data, new table, public trigger, producer or money formula.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_preparation_expiry_worker
 add column projection_work_cursor bigint check(projection_work_cursor>0),
 add column projection_case_cursor bigint check(projection_case_cursor>0),
 add column pending_work_ids uuid[] check(pending_work_ids is null or
   (cardinality(pending_work_ids) between 1 and 100 and array_ndims(pending_work_ids)=1
    and array_position(pending_work_ids,null::uuid) is null and pg_column_size(pending_work_ids)<=16384));
alter table private.bpay_next_preparation_expiry_worker
 drop constraint bpay_next_preparation_expiry_worker_stage_check,
 drop constraint bpay_next_preparation_expiry_worker_check3;
alter table private.bpay_next_preparation_expiry_worker
 add constraint bpay_next_expiry_projection_stage_ck check(stage in ('NEW','WORK','CASE','PROJECT_WORK','PROJECT_CASE','FINAL','COMPLETE')),
 add constraint bpay_next_expiry_projection_shape_ck check(
  (status='READY' and stage='NEW' and checkpoint=0 and started_at_utc is null and finished_at_utc is null
   and projection_work_cursor is null and projection_case_cursor is null and pending_work_ids is null)
  or(status='RELEASING' and stage in ('WORK','CASE','PROJECT_WORK','PROJECT_CASE','FINAL')
   and started_at_utc is not null and finished_at_utc is null)
  or(status='EXPIRED' and stage='COMPLETE' and started_at_utc is not null and finished_at_utc is not null
   and released_work_count=expected_work_count and released_case_hold_count=expected_case_hold_count and pending_work_ids is null));
commit;
