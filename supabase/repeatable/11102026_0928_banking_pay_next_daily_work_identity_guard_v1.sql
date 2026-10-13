-- DY-03: extend only the existing NULL-safe immutable Work identity tuple.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_work_pointer_guard_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_sealed_at timestamptz;
  v_revision_no bigint;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514', message='BPAY_NEXT_WORK_DELETE_FORBIDDEN';
  end if;
  if (new.candidate_id,new.contract_id,new.original_timesheet_id,new.booking_id,
      new.work_kind,new.week_ending_date,new.origin_kind,new.origin_work_date)
     is distinct from
     (old.candidate_id,old.contract_id,old.original_timesheet_id,old.booking_id,
      old.work_kind,old.week_ending_date,old.origin_kind,old.origin_work_date) then
    raise exception using errcode='23514', message='BPAY_NEXT_WORK_IDENTITY_IMMUTABLE';
  end if;
  if new.current_revision_id is not null then
    select r.sealed_at_utc,r.revision_no into v_sealed_at,v_revision_no
      from private.bpay_next_work_revision r
      where r.id=new.current_revision_id and r.work_id=new.id for share;
    if not found or v_sealed_at is null then
      raise exception using errcode='23514', message='BPAY_NEXT_CURRENT_REVISION_NOT_SEALED';
    end if;
    if new.current_revision_no <> v_revision_no then
      raise exception using errcode='23514', message='BPAY_NEXT_CURRENT_REVISION_NUMBER_MISMATCH';
    end if;
    if new.current_revision_id is distinct from old.current_revision_id
       and v_revision_no <= old.current_revision_no then
      raise exception using errcode='23514', message='BPAY_NEXT_CURRENT_REVISION_NOT_NEWER';
    end if;
  elsif new.current_revision_no <> old.current_revision_no then
    raise exception using errcode='23514', message='BPAY_NEXT_WITHDRAWAL_REVISION_NUMBER_CHANGED';
  end if;
  if new.applied_revision_id is not null then
    select r.sealed_at_utc into v_sealed_at from private.bpay_next_work_revision r
      where r.id=new.applied_revision_id and r.work_id=new.id for share;
    if not found or v_sealed_at is null then
      raise exception using errcode='23514', message='BPAY_NEXT_APPLIED_REVISION_NOT_SEALED';
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_work_pointer_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_pointer_guard_v1() from public,anon,authenticated,service_role;
commit;
