-- Repeatable CloudTMS function/view authority: banking_pay_next_guards_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- No user-facing route points here until the single-writer cutover is proved.
-- These guards make an approved revision and its financial/display children
-- immutable, including under concurrent insert/seal attempts.

create or replace function private.bpay_next_revision_guard_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_missing_detail boolean;
  v_line_count bigint;
  v_total_source_ex numeric;
begin
  if tg_op = 'INSERT' then
    if new.approved_at_utc is not null or new.sealed_at_utc is not null then
      raise exception using errcode='23514', message='BPAY_NEXT_DIRECT_SEALED_INSERT_FORBIDDEN';
    end if;
    return new;
  end if;
  if tg_op = 'DELETE' then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_DELETE_FORBIDDEN';
  end if;
  if old.sealed_at_utc is not null then
    raise exception using errcode='23514', message='BPAY_NEXT_SEALED_REVISION_IMMUTABLE';
  end if;
  if (to_jsonb(new) - 'approved_at_utc' - 'sealed_at_utc')
     is distinct from (to_jsonb(old) - 'approved_at_utc' - 'sealed_at_utc')
     or new.approved_at_utc is null or new.sealed_at_utc is null then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_SEAL_ONLY';
  end if;
  select count(*),coalesce(sum(source_pay_ex_vat),0)
    into v_line_count,v_total_source_ex
    from private.bpay_next_approved_line l where l.revision_id=old.id;
  if v_line_count<>new.expected_line_count
     or v_total_source_ex<>new.approved_source_ex_vat then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_INCOMPLETE_OR_TOTAL_MISMATCH';
  end if;
  if new.financial_snapshot_id is not null and not exists (
    select 1 from public.timesheets_financials tf
     where tf.id=new.financial_snapshot_id
       and tf.timesheet_id=new.physical_timesheet_id
       and tf.timesheet_version=new.physical_timesheet_version
  ) then
    raise exception using errcode='23514', message='BPAY_NEXT_FINANCIAL_SNAPSHOT_IDENTITY_MISMATCH';
  end if;
  if exists (
    select 1 from private.bpay_next_approved_line l
    left join private.bpay_next_rate_detail rd on rd.approved_line_id=l.id
    where l.revision_id=old.id
    group by l.id,l.expected_rate_detail_count
    having count(rd.id)<>l.expected_rate_detail_count
  ) then
    raise exception using errcode='23514', message='BPAY_NEXT_RATE_DETAIL_COUNT_MISMATCH';
  end if;
  if (select count(*) from private.bpay_next_rate_schedule s
      where s.revision_id=old.id)<>new.expected_rate_schedule_count then
    raise exception using errcode='23514', message='BPAY_NEXT_RATE_SCHEDULE_COUNT_MISMATCH';
  end if;
  if new.detail_kind='SHIFT' then
    select exists (
      select 1 from private.bpay_next_approved_line l
      where l.revision_id=old.id
        and l.component_kind in ('WORK','PROTECTED_WORK')
        and not exists (
          select 1 from private.bpay_next_shift_detail d
          where d.approved_line_id=l.id
            and d.work_date is not null
            and (d.approved_minutes is not null or d.approved_hours is not null
                 or d.shift_start_at is not null)
        )
    ) into v_missing_detail;
    if v_missing_detail then
      raise exception using errcode='23514', message='BPAY_NEXT_REVISION_WITHOUT_SHIFT_DETAIL';
    end if;
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_revision_child_guard_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_sealed_at timestamptz;
begin
  if tg_op <> 'INSERT' then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_CHILD_IMMUTABLE';
  end if;
  if tg_table_name in ('bpay_next_approved_line','bpay_next_rate_schedule') then
    select r.sealed_at_utc into v_sealed_at
      from private.bpay_next_work_revision r
      where r.id=new.revision_id for share;
  elsif tg_table_name='bpay_next_break_detail' then
    select r.sealed_at_utc into v_sealed_at
      from private.bpay_next_shift_detail d
      join private.bpay_next_approved_line l on l.id=d.approved_line_id
      join private.bpay_next_work_revision r on r.id=l.revision_id
      where d.id=new.shift_detail_id for share of r;
  else
    select r.sealed_at_utc into v_sealed_at
      from private.bpay_next_approved_line l
      join private.bpay_next_work_revision r on r.id=l.revision_id
      where l.id=new.approved_line_id for share of r;
  end if;
  if not found or v_sealed_at is not null then
    raise exception using errcode='23514', message='BPAY_NEXT_CHILD_AFTER_SEAL';
  end if;
  return new;
end
$function$;

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
      new.work_kind,new.week_ending_date)
     is distinct from
     (old.candidate_id,old.contract_id,old.original_timesheet_id,old.booking_id,
      old.work_kind,old.week_ending_date) then
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

-- Bind the stable work key to its original physical Timesheet once. The
-- original row may later become historical; its booking identity cannot.
create or replace function private.bpay_next_work_insert_guard_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_booking_id text;
begin
  select t.booking_id into v_booking_id
    from public.timesheets t where t.timesheet_id=new.original_timesheet_id;
  if v_booking_id is distinct from new.booking_id then
    raise exception using errcode='23514', message='BPAY_NEXT_WORK_ANCHOR_BOOKING_MISMATCH';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_effect_immutable_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
begin
  if tg_op <> 'INSERT' then
    raise exception using errcode='23514', message='BPAY_NEXT_FINANCIAL_EFFECT_IMMUTABLE';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_policy_immutable_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
begin
  if tg_op = 'INSERT'
     and tg_table_name = 'bpay_next_valuation_policy_window'
     and exists (
       select 1
         from private.bpay_next_valuation_policy p
         cross join private.bpay_next_valuation_policy_control c
        where p.id = new.policy_id
          and c.id = 1
          and p.policy_no > c.current_policy_no
     ) then
    return new;
  end if;
  raise exception using errcode='23514', message='BPAY_NEXT_VALUATION_POLICY_IMMUTABLE';
end
$function$;

drop trigger if exists bpay_next_revision_guard_v1 on private.bpay_next_work_revision;
create trigger bpay_next_revision_guard_v1
before insert or update or delete on private.bpay_next_work_revision
for each row execute function private.bpay_next_revision_guard_v1();
drop trigger if exists bpay_next_line_guard_v1 on private.bpay_next_approved_line;
create trigger bpay_next_line_guard_v1
before insert or update or delete on private.bpay_next_approved_line
for each row execute function private.bpay_next_revision_child_guard_v1();
drop trigger if exists bpay_next_detail_guard_v1 on private.bpay_next_shift_detail;
create trigger bpay_next_detail_guard_v1
before insert or update or delete on private.bpay_next_shift_detail
for each row execute function private.bpay_next_revision_child_guard_v1();
drop trigger if exists bpay_next_rate_detail_guard_v1 on private.bpay_next_rate_detail;
create trigger bpay_next_rate_detail_guard_v1
before insert or update or delete on private.bpay_next_rate_detail
for each row execute function private.bpay_next_revision_child_guard_v1();
drop trigger if exists bpay_next_rate_schedule_guard_v1 on private.bpay_next_rate_schedule;
create trigger bpay_next_rate_schedule_guard_v1
before insert or update or delete on private.bpay_next_rate_schedule
for each row execute function private.bpay_next_revision_child_guard_v1();
drop trigger if exists bpay_next_break_guard_v1 on private.bpay_next_break_detail;
create trigger bpay_next_break_guard_v1
before insert or update or delete on private.bpay_next_break_detail
for each row execute function private.bpay_next_revision_child_guard_v1();
drop trigger if exists bpay_next_work_pointer_guard_v1 on private.bpay_next_work;
create trigger bpay_next_work_pointer_guard_v1
before update or delete on private.bpay_next_work
for each row execute function private.bpay_next_work_pointer_guard_v1();
drop trigger if exists bpay_next_work_insert_guard_v1 on private.bpay_next_work;
create trigger bpay_next_work_insert_guard_v1
before insert on private.bpay_next_work
for each row execute function private.bpay_next_work_insert_guard_v1();
drop trigger if exists bpay_next_financial_effect_immutable_v1 on private.bpay_next_financial_effect;
create trigger bpay_next_financial_effect_immutable_v1
before update or delete on private.bpay_next_financial_effect
for each row execute function private.bpay_next_effect_immutable_v1();
drop trigger if exists bpay_next_case_event_immutable_v1 on private.bpay_next_case_event;
create trigger bpay_next_case_event_immutable_v1
before update or delete on private.bpay_next_case_event
for each row execute function private.bpay_next_effect_immutable_v1();
drop trigger if exists bpay_next_policy_immutable_v1 on private.bpay_next_valuation_policy;
create trigger bpay_next_policy_immutable_v1
before update or delete on private.bpay_next_valuation_policy
for each row execute function private.bpay_next_policy_immutable_v1();
drop trigger if exists bpay_next_policy_no_truncate_v1 on private.bpay_next_valuation_policy;
create trigger bpay_next_policy_no_truncate_v1
before truncate on private.bpay_next_valuation_policy
for each statement execute function private.bpay_next_policy_immutable_v1();
drop trigger if exists bpay_next_policy_window_immutable_v1
  on private.bpay_next_valuation_policy_window;
create trigger bpay_next_policy_window_immutable_v1
before insert or update or delete on private.bpay_next_valuation_policy_window
for each row execute function private.bpay_next_policy_immutable_v1();
drop trigger if exists bpay_next_policy_window_no_truncate_v1
  on private.bpay_next_valuation_policy_window;
create trigger bpay_next_policy_window_no_truncate_v1
before truncate on private.bpay_next_valuation_policy_window
for each statement execute function private.bpay_next_policy_immutable_v1();

alter function private.bpay_next_revision_guard_v1() owner to postgres;
alter function private.bpay_next_policy_immutable_v1() owner to postgres;
alter function private.bpay_next_revision_child_guard_v1() owner to postgres;
alter function private.bpay_next_work_pointer_guard_v1() owner to postgres;
alter function private.bpay_next_work_insert_guard_v1() owner to postgres;
alter function private.bpay_next_effect_immutable_v1() owner to postgres;
revoke all on function private.bpay_next_revision_guard_v1(),
  private.bpay_next_revision_child_guard_v1(),
  private.bpay_next_work_pointer_guard_v1(),
  private.bpay_next_work_insert_guard_v1(),
  private.bpay_next_effect_immutable_v1(),
  private.bpay_next_policy_immutable_v1()
  from public, anon, authenticated, service_role;

commit;
