-- Source withdrawal is a different event from certified zero. Only the
-- successful Source owner may mark its live root-authorisation withdrawn;
-- this exact-row trigger makes the corresponding new work unpayable in the
-- same transaction. Approved D1: a captured offer/active hold remains frozen
-- and does not prevent current approval withdrawal. The independent paid-work
-- Source safety contract is retained; this is not an accounting correction.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_source_withdrawal_guard_v1()
returns trigger
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_booking_id text;
  v_contract_id uuid;
  v_candidate_id uuid;
  v_work_id uuid;
  v_work private.bpay_next_work%rowtype;
begin
  if old.withdrawn_at_utc is not null or new.withdrawn_at_utc is null
     or (select active_owner from private.bpay_next_module_control
          where id=1 for share)<>'NEXT' then
    return new;
  end if;
  select t.booking_id,t.contract_id,c.candidate_id
    into strict v_booking_id,v_contract_id,v_candidate_id
    from public.timesheets t
    join public.contracts c on c.id=t.contract_id
    where t.timesheet_id=new.root_timesheet_id;
  -- Preserve the publication/worker lock order: Candidate control first,
  -- exact work second. These are the only new rows this event protects.
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select id into v_work_id from private.bpay_next_work
    where booking_id=v_booking_id;
  if v_work_id is null then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_WITHDRAW_WORK_MISSING';
  end if;
  select * into strict v_work from private.bpay_next_work
    where id=v_work_id for update;
  if v_work.work_kind<>'SOURCE'
     or v_work.candidate_id<>v_candidate_id
     or v_work.contract_id<>v_contract_id
     or v_work.approval_state<>'APPROVED' then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_WITHDRAW_WORK_MISMATCH';
  end if;
  -- A later withdrawal changes CURRENT eligibility, not captured finances.
  -- Never release/reprice frozen selections or active holds here. Retain the
  -- separate Source paid-work restriction: one indexed exact-work EXISTS,
  -- no traversal or reconstruction of old Timesheet versions.
  if exists(select 1 from private.bpay_next_financial_effect e
      where e.work_id=v_work_id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SOURCE_WITHDRAW_FINANCIAL_DEPENDENCY';
  end if;
  update private.bpay_next_work
    set approval_state='WITHDRAWN',current_revision_id=null,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_work_id;
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate_id;
  return new;
end
$function$;

alter function private.bpay_next_source_withdrawal_guard_v1() owner to postgres;
revoke all on function private.bpay_next_source_withdrawal_guard_v1()
  from public,anon,authenticated,service_role;

drop trigger if exists bpay_next_source_withdrawal_guard_v1
  on public.weekly_source_root_authorisations;
create trigger bpay_next_source_withdrawal_guard_v1
before update of withdrawn_at_utc on public.weekly_source_root_authorisations
for each row when (old.withdrawn_at_utc is null and new.withdrawn_at_utc is not null)
execute function private.bpay_next_source_withdrawal_guard_v1();

commit;
