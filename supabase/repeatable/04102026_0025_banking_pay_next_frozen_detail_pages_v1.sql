-- Owner-only frozen detail pages for internal Review/Draft proof. These read
-- the selected immutable revision, never a current Timesheet or live rates.
-- No browser/service grant is made until the complete Draft route is proved.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_simple_frozen_line_guard_v1(
  p_run_line_id uuid
) returns table (approved_line_id uuid,captured_revision_id uuid)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_status text;
  v_worker_status text;
begin
  if p_run_line_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_FROZEN_LINE_ID_REQUIRED';
  end if;
  select l.approved_line_id,l.captured_revision_id,r.status,w.status
    into strict approved_line_id,captured_revision_id,
      v_run_status,v_worker_status
    from private.bpay_next_run_line l
    join private.bpay_next_run_worker w on w.id=l.run_worker_id
    join private.bpay_next_pay_run r on r.id=w.run_id
    where l.id=p_run_line_id;
  if v_run_status not in ('REVIEW','DRAFT')
     or v_worker_status<>'READY' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_FROZEN_DETAIL_NOT_AVAILABLE';
  end if;
  return next;
end
$function$;

create or replace function private.bpay_next_simple_shift_page_v1(
  p_run_line_id uuid,p_after_detail_no integer,p_limit integer
) returns table (
  shift_detail_id uuid,detail_no integer,work_date date,
  shift_start_at timestamptz,shift_end_at timestamptz,
  shift_start_local text,shift_end_local text,shift_overnight boolean,
  submitted_minutes integer,approved_minutes integer,
  approved_hours numeric,detail_label text
)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_line record;
begin
  if p_limit is null or p_limit not between 1 and 100
     or (p_after_detail_no is not null and p_after_detail_no<0) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_SHIFT_PAGE_INPUT_INVALID';
  end if;
  select * into strict v_line
    from private.bpay_next_simple_frozen_line_guard_v1(p_run_line_id);
  return query
    select s.id,s.detail_no,s.work_date,s.shift_start_at,s.shift_end_at,
      s.shift_start_local,s.shift_end_local,s.shift_overnight,
      s.submitted_minutes,s.approved_minutes,s.approved_hours,s.detail_label
    from private.bpay_next_shift_detail s
    where s.approved_line_id=v_line.approved_line_id
      and s.detail_no>coalesce(p_after_detail_no,0)
    order by s.detail_no,s.id
    limit p_limit;
end
$function$;

create or replace function private.bpay_next_simple_break_page_v1(
  p_run_line_id uuid,p_shift_detail_id uuid,p_after_break_no integer,
  p_limit integer
) returns table (
  break_detail_id uuid,break_no integer,break_start_at timestamptz,
  break_end_at timestamptz,break_start_local text,break_end_local text,
  break_minutes integer
)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_line record;
begin
  if p_shift_detail_id is null or p_limit is null
     or p_limit not between 1 and 100
     or (p_after_break_no is not null and p_after_break_no<0) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_BREAK_PAGE_INPUT_INVALID';
  end if;
  select * into strict v_line
    from private.bpay_next_simple_frozen_line_guard_v1(p_run_line_id);
  if not exists (
    select 1 from private.bpay_next_shift_detail s
    where s.id=p_shift_detail_id
      and s.approved_line_id=v_line.approved_line_id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_BREAK_SHIFT_NOT_IN_FROZEN_LINE';
  end if;
  return query
    select b.id,b.break_no,b.break_start_at,b.break_end_at,
      b.break_start_local,b.break_end_local,b.break_minutes
    from private.bpay_next_break_detail b
    where b.shift_detail_id=p_shift_detail_id
      and b.break_no>coalesce(p_after_break_no,0)
    order by b.break_no,b.id
    limit p_limit;
end
$function$;

create or replace function private.bpay_next_simple_rate_detail_page_v1(
  p_run_line_id uuid,p_after_bucket text,p_limit integer
) returns table (
  rate_detail_id uuid,bucket text,approved_hours numeric,
  source_pay_rate numeric
)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_line record;
begin
  if p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_RATE_DETAIL_PAGE_INPUT_INVALID';
  end if;
  select * into strict v_line
    from private.bpay_next_simple_frozen_line_guard_v1(p_run_line_id);
  return query
    select d.id,d.bucket,d.approved_hours,d.source_pay_rate
    from private.bpay_next_rate_detail d
    where d.approved_line_id=v_line.approved_line_id
      and (p_after_bucket is null or d.bucket>p_after_bucket)
    order by d.bucket,d.id
    limit p_limit;
end
$function$;

-- Available rate options are frozen once per selected work revision. Read
-- them once per run_work, not once per component or from today's contract.
create or replace function private.bpay_next_simple_rate_schedule_page_v1(
  p_run_work_id uuid,p_after_rate_family text,p_after_rate_code text,
  p_limit integer
) returns table (
  rate_schedule_id uuid,rate_family text,rate_code text,unit_label text,
  paye_rate numeric,umbrella_rate numeric,charge_rate numeric
)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_revision_id uuid;
  v_run_status text;
  v_worker_status text;
begin
  if p_run_work_id is null or p_limit is null
     or p_limit not between 1 and 100
     or ((p_after_rate_family is null)<>(p_after_rate_code is null)) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_RATE_SCHEDULE_PAGE_INPUT_INVALID';
  end if;
  select rw.captured_revision_id,r.status,w.status
    into strict v_revision_id,v_run_status,v_worker_status
    from private.bpay_next_run_work rw
    join private.bpay_next_run_worker w on w.id=rw.run_worker_id
    join private.bpay_next_pay_run r on r.id=w.run_id
    where rw.id=p_run_work_id;
  if v_run_status not in ('REVIEW','DRAFT')
     or v_worker_status<>'READY' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_RATE_SCHEDULE_NOT_AVAILABLE';
  end if;
  return query
    select s.id,s.rate_family,s.rate_code,s.unit_label,
      s.paye_rate,s.umbrella_rate,s.charge_rate
    from private.bpay_next_rate_schedule s
    where s.revision_id=v_revision_id
      and (p_after_rate_family is null or
           (s.rate_family,s.rate_code)>(p_after_rate_family,p_after_rate_code))
    order by s.rate_family,s.rate_code,s.id
    limit p_limit;
end
$function$;

alter function private.bpay_next_simple_frozen_line_guard_v1(uuid)
  owner to postgres;
alter function private.bpay_next_simple_shift_page_v1(uuid,integer,integer)
  owner to postgres;
alter function private.bpay_next_simple_break_page_v1(uuid,uuid,integer,integer)
  owner to postgres;
alter function private.bpay_next_simple_rate_detail_page_v1(uuid,text,integer)
  owner to postgres;
alter function private.bpay_next_simple_rate_schedule_page_v1(
  uuid,text,text,integer) owner to postgres;
revoke all on function private.bpay_next_simple_frozen_line_guard_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_simple_shift_page_v1(uuid,integer,integer)
  from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_simple_break_page_v1(uuid,uuid,integer,integer)
  from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_simple_rate_detail_page_v1(uuid,text,integer)
  from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_simple_rate_schedule_page_v1(
  uuid,text,text,integer) from public,anon,authenticated,service_role;

commit;
