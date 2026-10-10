-- WB-36 exact approved / selected / captured stable shift attribution.
-- Money parents and Rule-B query admission remain their existing owners.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_segment_pay_basis_v1(p_segment_id uuid)
returns bytea language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array(r.work_id,s.segment_key,
    s.component_key,r.source_pay_channel,r.source_umbrella_id,
    a.tax_treatment,s.pay_excluded,s.approved_hours,s.hours_day,s.hours_night,s.hours_sat,s.hours_sun,s.hours_bh,
    case when not s.pay_excluded and s.hours_day<>0 then s.pay_day end,
    case when not s.pay_excluded and s.hours_night<>0 then s.pay_night end,
    case when not s.pay_excluded and s.hours_sat<>0 then s.pay_sat end,
    case when not s.pay_excluded and s.hours_sun<>0 then s.pay_sun end,
    case when not s.pay_excluded and s.hours_bh<>0 then s.pay_bh end,
    s.approved_source_ex_vat)::text,'UTF8'))
  from private.bpay_next_approved_segment s
  join private.bpay_next_approved_line a on a.id=s.approved_line_id
  join private.bpay_next_work_revision r on r.id=s.revision_id where s.id=p_segment_id
$function$;
alter function private.bpay_next_segment_pay_basis_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_segment_pay_basis_v1(uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_line_pay_basis_v1(p_line_id uuid)
returns bytea language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array(r.work_id,a.component_key,
    r.source_pay_channel,r.source_umbrella_id,a.component_kind,
    a.tax_treatment,a.approved_quantity,case when a.approved_quantity<>0 then a.approved_unit_rate end,a.source_pay_ex_vat,
    coalesce((select jsonb_agg(jsonb_build_array(d.bucket,d.approved_hours,
      case when d.approved_hours<>0 then d.source_pay_rate end) order by d.bucket)
      from private.bpay_next_rate_detail d where d.approved_line_id=a.id),'[]'::jsonb))::text,'UTF8'))
  from private.bpay_next_approved_line a join private.bpay_next_work_revision r on r.id=a.revision_id
  where a.id=p_line_id
$function$;
alter function private.bpay_next_line_pay_basis_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_line_pay_basis_v1(uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_segment_revision_seal_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_row record; v_whole jsonb; v_expenses jsonb;
begin
  if old.sealed_at_utc is not null or new.sealed_at_utc is null then return new; end if;
  -- Genuine stable keys were persisted by the named approval producers.
  -- Existing producer traversal remains; read CHECK is a point lookup below.
  insert into private.bpay_next_approved_segment(revision_id,approved_line_id,component_key,segment_key,
    shift_detail_id,work_date,shift_start_at,shift_end_at,shift_start_local,shift_end_local,shift_overnight,
    approved_hours,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
    pay_day,pay_night,pay_sat,pay_sun,pay_bh,approved_source_ex_vat,pay_excluded)
  select a.revision_id,a.id,a.component_key,d.segment_key,d.id,d.work_date,d.shift_start_at,d.shift_end_at,
    d.shift_start_local,d.shift_end_local,d.shift_overnight,d.approved_hours,
    d.hours_day,d.hours_night,d.hours_sat,d.hours_sun,d.hours_bh,
    rd.pay_day,rd.pay_night,rd.pay_sat,rd.pay_sun,rd.pay_bh,d.segment_pay_ex_vat,d.pay_excluded
  from private.bpay_next_approved_line a join private.bpay_next_shift_detail d on d.approved_line_id=a.id
  left join lateral(select max(x.source_pay_rate::numeric) filter(where x.bucket='DAY') pay_day,
    max(x.source_pay_rate::numeric) filter(where x.bucket='NIGHT') pay_night,
    max(x.source_pay_rate::numeric) filter(where x.bucket='SAT') pay_sat,
    max(x.source_pay_rate::numeric) filter(where x.bucket='SUN') pay_sun,
    max(x.source_pay_rate::numeric) filter(where x.bucket='BH') pay_bh
    from private.bpay_next_rate_detail x where x.approved_line_id=a.id) rd on true
  where a.revision_id=new.id and d.segment_key is not null;
  for v_row in select a.id,a.component_key,a.source_pay_ex_vat,a.approved_quantity,
      count(s.id) segment_count,sum(s.approved_source_ex_vat) segment_gross,
      sum(case when new.source_kind='ORDINARY' and s.pay_excluded then 0 else s.approved_hours end) segment_hours
    from private.bpay_next_approved_line a left join private.bpay_next_approved_segment s on s.approved_line_id=a.id
    where a.revision_id=new.id and a.component_kind in ('WORK','PROTECTED_WORK')
    group by a.id,a.component_key,a.source_pay_ex_vat,a.approved_quantity
  loop
    if v_row.segment_count>0 and (v_row.segment_gross,v_row.segment_hours)
      is distinct from (v_row.source_pay_ex_vat,v_row.approved_quantity) then
      raise exception using errcode='23514',message='BPAY_NEXT_APPROVED_SEGMENT_PARENT_TOTAL_MISMATCH';
    end if;
    if exists(select 1 from private.bpay_next_shift_detail d where d.approved_line_id=v_row.id
      and d.segment_key is not null) and exists(select 1 from private.bpay_next_shift_detail d
      where d.approved_line_id=v_row.id and d.segment_key is null) then
      raise exception using errcode='23514',message='BPAY_NEXT_APPROVED_SEGMENT_IDENTITY_INCOMPLETE';
    end if;
  end loop;
  select coalesce(jsonb_agg(x.basis order by x.identity),'[]'::jsonb) into v_whole from (
    select 'SEGMENT:'||s.segment_key identity,encode(private.bpay_next_segment_pay_basis_v1(s.id),'hex') basis
      from private.bpay_next_approved_segment s where s.revision_id=new.id
    union all select 'LINE:'||a.component_key,encode(private.bpay_next_line_pay_basis_v1(a.id),'hex')
      from private.bpay_next_approved_line a where a.revision_id=new.id
        and not exists(select 1 from private.bpay_next_approved_segment s where s.approved_line_id=a.id)
  ) x;
  select coalesce(jsonb_agg(encode(private.bpay_next_line_pay_basis_v1(a.id),'hex') order by a.component_key),'[]'::jsonb)
    into v_expenses from private.bpay_next_approved_line a where a.revision_id=new.id and a.component_kind in ('EXPENSE','MILEAGE');
  insert into private.bpay_next_revision_worker_basis(revision_id,whole_basis,expense_basis)
    values(new.id,pg_catalog.sha256(pg_catalog.convert_to(v_whole::text,'UTF8')),
      pg_catalog.sha256(pg_catalog.convert_to(v_expenses::text,'UTF8')));
  return new;
end
$function$;
alter function private.bpay_next_segment_revision_seal_v1() owner to postgres;
revoke all on function private.bpay_next_segment_revision_seal_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_revision_seal on private.bpay_next_work_revision;
create trigger bpay_next_segment_revision_seal before update on private.bpay_next_work_revision
  for each row execute function private.bpay_next_segment_revision_seal_v1();

create or replace function private.bpay_next_segment_child_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_revision private.bpay_next_work_revision%rowtype; v_shift private.bpay_next_shift_detail%rowtype;
  v_rates record;v_whole jsonb;v_expenses jsonb;
begin
  if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_FACT_IMMUTABLE'; end if;
  if tg_table_name in ('bpay_next_approved_segment','bpay_next_revision_worker_basis') then
    select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=new.revision_id for share;
    if v_revision.sealed_at_utc is not null then
      raise exception using errcode='55000',message='BPAY_NEXT_APPROVED_SEGMENT_REVISION_SEALED';
    end if;
    if tg_table_name='bpay_next_approved_segment' then
      select d.* into strict v_shift from private.bpay_next_shift_detail d where d.id=new.shift_detail_id;
      select max(x.source_pay_rate::numeric) filter(where x.bucket='DAY') pay_day,
        max(x.source_pay_rate::numeric) filter(where x.bucket='NIGHT') pay_night,
        max(x.source_pay_rate::numeric) filter(where x.bucket='SAT') pay_sat,
        max(x.source_pay_rate::numeric) filter(where x.bucket='SUN') pay_sun,
        max(x.source_pay_rate::numeric) filter(where x.bucket='BH') pay_bh into v_rates
        from private.bpay_next_rate_detail x where x.approved_line_id=v_shift.approved_line_id;
      if (new.approved_line_id,new.segment_key,new.work_date,new.shift_start_at,new.shift_end_at,
          new.shift_start_local,new.shift_end_local,new.shift_overnight,new.approved_hours,
          new.hours_day,new.hours_night,new.hours_sat,new.hours_sun,new.hours_bh,new.approved_source_ex_vat,new.pay_excluded)
        is distinct from (v_shift.approved_line_id,v_shift.segment_key,v_shift.work_date,v_shift.shift_start_at,v_shift.shift_end_at,
          v_shift.shift_start_local,v_shift.shift_end_local,v_shift.shift_overnight,v_shift.approved_hours,
          v_shift.hours_day,v_shift.hours_night,v_shift.hours_sat,v_shift.hours_sun,v_shift.hours_bh,v_shift.segment_pay_ex_vat,v_shift.pay_excluded)
        or (new.pay_day,new.pay_night,new.pay_sat,new.pay_sun,new.pay_bh)
          is distinct from (v_rates.pay_day,v_rates.pay_night,v_rates.pay_sat,v_rates.pay_sun,v_rates.pay_bh)
        or not exists(select 1 from private.bpay_next_approved_line a where a.id=new.approved_line_id
          and a.revision_id=new.revision_id and a.component_key=new.component_key and a.component_kind in ('WORK','PROTECTED_WORK')) then
        raise exception using errcode='23514',message='BPAY_NEXT_APPROVED_SEGMENT_NATIVE_IDENTITY_MISMATCH';
      end if;
    else
      select coalesce(jsonb_agg(x.basis order by x.identity),'[]'::jsonb) into v_whole from (
        select 'SEGMENT:'||s.segment_key identity,encode(private.bpay_next_segment_pay_basis_v1(s.id),'hex') basis
          from private.bpay_next_approved_segment s where s.revision_id=new.revision_id
        union all select 'LINE:'||a.component_key,encode(private.bpay_next_line_pay_basis_v1(a.id),'hex')
          from private.bpay_next_approved_line a where a.revision_id=new.revision_id
            and not exists(select 1 from private.bpay_next_approved_segment s where s.approved_line_id=a.id)
      ) x;
      select coalesce(jsonb_agg(encode(private.bpay_next_line_pay_basis_v1(a.id),'hex') order by a.component_key),'[]'::jsonb)
        into v_expenses from private.bpay_next_approved_line a where a.revision_id=new.revision_id and a.component_kind in ('EXPENSE','MILEAGE');
      if (new.whole_basis,new.expense_basis) is distinct from
        (pg_catalog.sha256(pg_catalog.convert_to(v_whole::text,'UTF8')),pg_catalog.sha256(pg_catalog.convert_to(v_expenses::text,'UTF8'))) then
        raise exception using errcode='23514',message='BPAY_NEXT_REVISION_WORKER_BASIS_MISMATCH';
      end if;
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_child_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_child_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_approved_segment_guard on private.bpay_next_approved_segment;
create trigger bpay_next_approved_segment_guard before insert or update or delete on private.bpay_next_approved_segment
  for each row execute function private.bpay_next_segment_child_guard_v1();
drop trigger if exists bpay_next_approved_segment_truncate_guard on private.bpay_next_approved_segment;
create trigger bpay_next_approved_segment_truncate_guard before truncate on private.bpay_next_approved_segment
  for each statement execute function private.bpay_next_segment_child_guard_v1();
drop trigger if exists bpay_next_revision_worker_basis_guard on private.bpay_next_revision_worker_basis;
create trigger bpay_next_revision_worker_basis_guard before insert or update or delete on private.bpay_next_revision_worker_basis
  for each row execute function private.bpay_next_segment_child_guard_v1();
drop trigger if exists bpay_next_revision_worker_basis_truncate_guard on private.bpay_next_revision_worker_basis;
create trigger bpay_next_revision_worker_basis_truncate_guard before truncate on private.bpay_next_revision_worker_basis
  for each statement execute function private.bpay_next_segment_child_guard_v1();

-- Create records the existing ordered metadata cut once. No producer protocol.
create or replace function private.bpay_next_segment_run_boundary_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
  if tg_op='INSERT' then
    select c.last_sequence into strict new.snooze_control_sequence from private.bpay_next_command_clock c where c.id=1 for share;
    new.snooze_business_date:=(clock_timestamp() at time zone 'Europe/London')::date;
  else
    if (new.snooze_control_sequence,new.snooze_business_date) is distinct from (old.snooze_control_sequence,old.snooze_business_date) then
      raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_CREATE_BOUNDARY_IMMUTABLE';
    end if;
    if old.selection_state='OPEN' and new.selection_state='SEALED'
      and (new.snooze_control_sequence is null or new.segment_sealed_choice_count<>new.work_choice_count) then
      raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_SEGMENTS_NOT_SEALED';
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_run_boundary_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_run_boundary_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_run_boundary_guard on private.bpay_next_pay_run;
create trigger bpay_next_segment_run_boundary_guard before insert or update on private.bpay_next_pay_run
  for each row execute function private.bpay_next_segment_run_boundary_guard_v1();

create or replace function private.bpay_next_segment_choice_boundary_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;
begin
  if tg_op='INSERT' then
    select r.* into strict v_run from private.bpay_next_pay_run r where r.id=new.run_id for update;
    new.segment_control_sequence:=v_run.snooze_control_sequence;
    if not exists(select 1 from private.bpay_next_approved_segment s where s.revision_id=new.expected_revision_id) then
      new.segment_sealed_count:=0;
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_choice_boundary_v1() owner to postgres;
revoke all on function private.bpay_next_segment_choice_boundary_v1() from public,anon,authenticated,service_role;
drop trigger if exists a_bpay_next_segment_choice_boundary on private.bpay_next_work_choice;
create trigger a_bpay_next_segment_choice_boundary before insert on private.bpay_next_work_choice
  for each row execute function private.bpay_next_segment_choice_boundary_v1();

create or replace function private.bpay_next_segment_choice_sealed_count_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
  if (tg_op='INSERT' and new.segment_sealed_count is not null)
    or (tg_op='UPDATE' and old.segment_sealed_count is null and new.segment_sealed_count is not null) then
    update private.bpay_next_pay_run set segment_sealed_choice_count=segment_sealed_choice_count+1 where id=new.run_id;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_choice_sealed_count_v1() owner to postgres;
revoke all on function private.bpay_next_segment_choice_sealed_count_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_choice_sealed_count on private.bpay_next_work_choice;
create trigger bpay_next_segment_choice_sealed_count after insert or update on private.bpay_next_work_choice
  for each row execute function private.bpay_next_segment_choice_sealed_count_v1();

create or replace function private.bpay_next_append_segment_selection_page_v1(
  p_run_id uuid,p_work_id uuid,p_expected_revision_id uuid,p_page_no bigint,p_request_id uuid,p_segment_keys text[])
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype; v_choice private.bpay_next_work_choice%rowtype;
  v_page private.bpay_next_segment_selection_page%rowtype; v_segment private.bpay_next_approved_segment%rowtype;
  v_n integer; v_i integer:=0; v_key text; v_body jsonb; v_snooze jsonb;
begin
  v_n:=cardinality(p_segment_keys); v_body:=to_jsonb(p_segment_keys);
  if p_run_id is null or p_work_id is null or p_expected_revision_id is null or p_request_id is null
    or p_page_no is null or p_page_no<1 or v_n is null or v_n not between 1 and 100
    or exists(select 1 from unnest(p_segment_keys) k where k is null or length(k) not between 1 and 245 or btrim(k)<>k)
    or (select count(distinct k) from unnest(p_segment_keys) k)<>v_n then
    raise exception using errcode='22023',message='BPAY_NEXT_SEGMENT_SELECTION_INPUT_INVALID';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select c.* into strict v_choice from private.bpay_next_work_choice c where c.run_id=p_run_id and c.work_id=p_work_id;
  select p.* into v_page from private.bpay_next_segment_selection_page p where p.run_id=p_run_id and p.work_id=p_work_id and p.page_no=p_page_no;
  if found then
    if (v_page.request_id,v_page.request_body,v_choice.expected_revision_id)
      is distinct from (p_request_id,v_body,p_expected_revision_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_SELECTION_REPLAY_MISMATCH';
    end if;
    return jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
      'page_no',p_page_no::text,'item_count',v_n::text,'segment_count',v_choice.segment_count::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or v_choice.segment_sealed_count is not null
    or v_choice.expected_revision_id is distinct from p_expected_revision_id or p_page_no<>v_choice.segment_page_count+1
    or v_choice.selection_state<>'SEALED' then
    raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_SELECTION_NOT_OPEN';
  end if;
  insert into private.bpay_next_segment_selection_page(run_id,work_id,page_no,request_id,request_body,item_count,first_member_no)
    values(p_run_id,p_work_id,p_page_no,p_request_id,v_body,v_n,v_choice.segment_count+1);
  foreach v_key in array p_segment_keys loop
    select s.* into v_segment from private.bpay_next_approved_segment s where s.revision_id=p_expected_revision_id and s.segment_key=v_key
      and (v_choice.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component c
        where c.run_id=p_run_id and c.work_id=p_work_id and c.component_key=s.component_key));
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_NOT_IN_SELECTED_APPROVED_INVENTORY'; end if;
    v_i:=v_i+1;
    v_snooze:=private.bpay_next_snooze_at_cut_v1(p_work_id,p_expected_revision_id,v_key,v_segment.component_key,
      v_choice.segment_control_sequence,v_run.snooze_business_date);
    insert into private.bpay_next_selected_segment(run_id,work_id,expected_revision_id,segment_key,component_key,
      approved_segment_id,selection_no,disposition,snooze_id,snooze_version,control_sequence)
    values(p_run_id,p_work_id,p_expected_revision_id,v_key,v_segment.component_key,v_segment.id,
      v_choice.segment_count+v_i,case when v_snooze is null then 'INCLUDED' else 'SNOOZED' end,
      nullif(v_snooze->>'id','')::uuid,nullif(v_snooze->>'version','')::bigint,v_choice.segment_control_sequence);
  end loop;
  update private.bpay_next_work_choice set segment_page_count=p_page_no,segment_count=segment_count+v_n
    where run_id=p_run_id and work_id=p_work_id;
  return jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
    'page_no',p_page_no::text,'item_count',v_n::text,'segment_count',(v_choice.segment_count+v_n)::text,'replay',false);
end
$function$;
alter function private.bpay_next_append_segment_selection_page_v1(uuid,uuid,uuid,bigint,uuid,text[]) owner to postgres;
revoke all on function private.bpay_next_append_segment_selection_page_v1(uuid,uuid,uuid,bigint,uuid,text[]) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_seal_segment_selection_v1(
  p_run_id uuid,p_work_id uuid,p_expected_revision_id uuid,p_expected_pages bigint,p_expected_count bigint)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype; v_choice private.bpay_next_work_choice%rowtype; v_count bigint;
begin
  if p_expected_pages is null or p_expected_count is null or p_expected_pages<0 or p_expected_count<0 then
    raise exception using errcode='22023',message='BPAY_NEXT_SEGMENT_SELECTION_INPUT_INVALID';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select c.* into strict v_choice from private.bpay_next_work_choice c where c.run_id=p_run_id and c.work_id=p_work_id;
  if (v_choice.expected_revision_id,v_choice.segment_page_count,v_choice.segment_count)
    is distinct from (p_expected_revision_id,p_expected_pages,p_expected_count) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_SELECTION_SEAL_COUNT_MISMATCH';
  end if;
  if v_choice.segment_sealed_count is not null then
    return jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
      'sealed',true,'segment_count',v_choice.segment_count::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or v_choice.selection_state<>'SEALED' then
    raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_SELECTION_NOT_OPEN';
  end if;
  select count(*) into v_count from private.bpay_next_approved_segment s where s.revision_id=p_expected_revision_id
    and (v_choice.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component c
      where c.run_id=p_run_id and c.work_id=p_work_id and c.component_key=s.component_key));
  if v_count<>p_expected_count or exists(select 1 from private.bpay_next_approved_segment s
    where s.revision_id=p_expected_revision_id and (v_choice.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component c
      where c.run_id=p_run_id and c.work_id=p_work_id and c.component_key=s.component_key))
      and not exists(select 1 from private.bpay_next_selected_segment m where m.run_id=p_run_id and m.work_id=p_work_id and m.segment_key=s.segment_key)) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_SELECTION_INVENTORY_INCOMPLETE';
  end if;
  update private.bpay_next_work_choice set segment_sealed_count=segment_count where run_id=p_run_id and work_id=p_work_id;
  return jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
    'sealed',true,'segment_count',v_choice.segment_count::text,'replay',false);
end
$function$;
alter function private.bpay_next_seal_segment_selection_v1(uuid,uuid,uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_seal_segment_selection_v1(uuid,uuid,uuid,bigint,bigint) from public,anon,authenticated,service_role;

-- WB-36 successor of1009: persist upstream segment identity; money unchanged.
create or replace function private.bpay_next_publish_ordinary_from_financial_v1(
  p_financial_id uuid
) returns uuid
language plpgsql security definer
set search_path = pg_catalog, public, private
as $function$
declare
  v_tf public.timesheets_financials%rowtype;
  v_ts public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_revision_id uuid;
  v_line_id uuid;
  v_shift_id uuid;
  v_command_id uuid:=pg_catalog.gen_random_uuid();
  v_mode text;
  v_route text;
  v_detail_kind text;
  v_segment jsonb;
  v_segment_row record;
  v_schedule jsonb;
  v_shift_entry jsonb;
  v_extra record;
  v_bucket record;
  v_break jsonb;
  v_segment_count integer:=0;
  v_additional_count integer:=0;
  v_expense_count integer:=0;
  v_mileage_count integer:=0;
  v_base_count integer:=0;
  v_expected_count integer;
  v_schedule_count integer;
  v_resolved_daily_rates boolean;
  v_available_daily_evidence jsonb;
  v_available_rows jsonb;
  v_source_umbrella_id uuid;
  v_source_candidate_channel text;
  v_extra_code text;
  v_extra_quantity numeric;
  v_extra_pay_rate numeric;
  v_line_no integer:=0;
  v_rate_detail_count integer;
  v_break_no integer;
  v_shift_no integer;
  v_break_total integer;
  v_break_mins integer;
  v_begin_mins integer;
  v_end_mins integer;
  v_break_start text;
  v_break_end text;
  v_shift_start_local text;
  v_shift_end_local text;
  v_hours numeric;
  v_amount numeric;
  v_base_amount numeric;
  v_expense_total numeric;
  v_expected_total numeric;
  v_written_total numeric:=0;
  v_work_date date;
  v_component_key text;
  v_excluded boolean;
  v_candidate_name text;
  v_candidate_ref text;
  v_candidate_mileage_pay_rate numeric;
  v_client_name text;
  v_client_mileage_charge_rate numeric;
  v_mileage_pay_rate numeric;
  v_mileage_charge_rate numeric;
begin
  if p_financial_id is null then
    raise exception using errcode='22023', message='BPAY_NEXT_ORDINARY_FINANCIAL_ID_REQUIRED';
  end if;
  if (select active_owner from private.bpay_next_module_control
        where id=1 for share)<>'NEXT' then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_tf from public.timesheets_financials
    where id=p_financial_id;
  select * into strict v_ts from public.timesheets
    where timesheet_id=v_tf.timesheet_id;
  select * into strict v_contract from public.contracts
    where id=v_ts.contract_id;
  v_route:=private.bpay_next_approval_route_v1(v_ts.timesheet_id);
  if v_route not in ('ORDINARY_HOURS','EXPENSE_CARRIER')
     or v_tf.is_current is distinct from true
     or v_tf.is_stale is distinct from false
     or v_tf.authorised_at_utc is null
     or v_ts.authorised_at_server is null
     or v_tf.timesheet_version<>v_ts.version
     or v_tf.candidate_id is distinct from v_contract.candidate_id
     or v_tf.client_id is distinct from v_contract.client_id
     or upper(coalesce(v_tf.pay_method,'')) not in ('PAYE','UMBRELLA') then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_APPROVAL_NOT_CURRENT';
  end if;
  select coalesce(nullif(btrim(c.display_name),''),
                  nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),
                  c.id::text),c.tms_ref,c.mileage_pay_rate,c.pay_method,c.umbrella_id
    into strict v_candidate_name,v_candidate_ref,v_candidate_mileage_pay_rate,
      v_source_candidate_channel,v_source_umbrella_id
    from public.candidates c where c.id=v_contract.candidate_id for share;
  if upper(v_tf.pay_method)='PAYE' then
    v_source_umbrella_id:=null;
  elsif upper(coalesce(v_source_candidate_channel,''))<>'UMBRELLA'
     or v_source_umbrella_id is null then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SOURCE_UMBRELLA_UNAVAILABLE';
  end if;
  select name,mileage_charge_rate into strict
    v_client_name,v_client_mileage_charge_rate from public.clients
    where id=v_contract.client_id;
  v_mode:=upper(coalesce(v_tf.invoice_breakdown_json->>'mode',''));
  if v_mode='SEGMENTS' then
    if jsonb_typeof(v_tf.invoice_breakdown_json->'segments')<>'array' then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SEGMENTS_NOT_ARRAY';
    end if;
    v_segment_count:=jsonb_array_length(v_tf.invoice_breakdown_json->'segments');
    -- Validate identities before grouping. Physical segment IDs are retained
    -- as original evidence, not continuing financial obligations.
    for v_segment in select value from jsonb_array_elements(
      v_tf.invoice_breakdown_json->'segments')
    loop
      if jsonb_typeof(v_segment)<>'object'
         or nullif(btrim(v_segment->>'segment_id'),'') is null
         or coalesce(v_segment->>'date','') !~ '^\d{4}-\d{2}-\d{2}$' then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SEGMENT_ID_OR_DATE_INVALID';
      end if;
    end loop;
    select count(distinct (value->>'date')::date) into v_segment_count
      from jsonb_array_elements(v_tf.invoice_breakdown_json->'segments');
    v_detail_kind:='SHIFT';
  elsif v_mode='AGGREGATE' then
    v_detail_kind:='AGGREGATE';
    v_base_amount:=v_tf.total_pay_ex_vat-v_tf.additional_pay_ex_vat
      -coalesce(v_tf.expenses_pay_ex_vat,0)-coalesce(v_tf.mileage_pay_ex_vat,0);
    v_base_count:=case when v_base_amount<>0 or v_tf.total_hours<>0 then 1 else 0 end;
  elsif v_mode='EXPENSES_ONLY' and v_route='EXPENSE_CARRIER' then
    v_detail_kind:='FIXED';
  else
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_FINANCIAL_MODE_UNSUPPORTED';
  end if;
  if jsonb_typeof(v_tf.additional_units_json)<>'object' then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_NOT_OBJECT';
  end if;
  select count(*),count(distinct upper(btrim(key)))
    into v_additional_count,v_rate_detail_count from jsonb_each(v_tf.additional_units_json);
  if v_additional_count<>v_rate_detail_count then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_CODE_CONFLICT';
  end if;
  if v_route='EXPENSE_CARRIER' then
    if v_segment_count<>0 or v_base_count<>0 or v_additional_count<>0
       or v_tf.total_hours<>0 or v_tf.additional_pay_ex_vat<>0
       or v_tf.hours_day<>0 or v_tf.hours_night<>0
       or v_tf.hours_sat<>0 or v_tf.hours_sun<>0 or v_tf.hours_bh<>0
       or v_tf.total_pay_ex_vat is distinct from
          coalesce(v_tf.expenses_pay_ex_vat,0)+coalesce(v_tf.mileage_pay_ex_vat,0) then
      raise exception using errcode='23514', message='BPAY_NEXT_EXPENSE_CARRIER_MONEY_SHAPE_INVALID';
    end if;
    v_detail_kind:='FIXED';
  end if;
  v_expense_total:=coalesce(v_tf.travel_pay_ex_vat,0)
    +coalesce(v_tf.accommodation_pay_ex_vat,0)+coalesce(v_tf.other_pay_ex_vat,0);
  if v_expense_total<>0 then
    if coalesce(v_tf.expenses_pay_ex_vat,0) not in (0,v_expense_total) then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_EXPENSE_TOTAL_CONFLICT';
    end if;
    v_expense_count:=(case when v_tf.travel_pay_ex_vat<>0 then 1 else 0 end)
      +(case when v_tf.accommodation_pay_ex_vat<>0 then 1 else 0 end)
      +(case when v_tf.other_pay_ex_vat<>0 then 1 else 0 end);
  else
    v_expense_total:=coalesce(v_tf.expenses_pay_ex_vat,0);
    v_expense_count:=case when v_expense_total<>0 then 1 else 0 end;
  end if;
  v_mileage_count:=case when v_tf.mileage_pay_ex_vat<>0
    or v_tf.mileage_units<>0 then 1 else 0 end;
  v_expected_count:=v_segment_count+v_base_count+v_additional_count
    +v_expense_count+v_mileage_count;
  -- Both production snapshot writers include additional pay, expenses and
  -- mileage in total_pay_ex_vat. They are components of this total, not extras.
  v_expected_total:=v_tf.total_pay_ex_vat;
  if v_expected_total<>round(v_expected_total,2)
     or v_tf.total_pay_ex_vat<>round(v_tf.total_pay_ex_vat,2)
     or v_tf.additional_pay_ex_vat<>round(v_tf.additional_pay_ex_vat,2)
     or v_tf.expenses_pay_ex_vat<>round(v_tf.expenses_pay_ex_vat,2)
     or v_tf.travel_pay_ex_vat<>round(v_tf.travel_pay_ex_vat,2)
     or v_tf.accommodation_pay_ex_vat<>round(v_tf.accommodation_pay_ex_vat,2)
     or v_tf.other_pay_ex_vat<>round(v_tf.other_pay_ex_vat,2)
     or v_tf.mileage_pay_ex_vat<>round(v_tf.mileage_pay_ex_vat,2) then
    raise exception using errcode='22003', message='BPAY_NEXT_ORDINARY_MONEY_PRECISION_INVALID';
  end if;
  v_mileage_pay_rate:=coalesce(v_tf.mileage_pay_rate,
                               v_contract.mileage_pay_rate,
                               v_candidate_mileage_pay_rate);
  v_mileage_charge_rate:=coalesce(v_tf.mileage_charge_rate,
                                  v_contract.mileage_charge_rate,
                                  v_client_mileage_charge_rate);
  if v_mileage_count>0 and v_mileage_pay_rate is null then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
      detail=jsonb_build_object('bucket','MILE','contract_id',v_contract.id)::text;
  end if;
  v_resolved_daily_rates:=v_route in ('ORDINARY_HOURS','DAILY_HOURS')
    and v_ts.sheet_scope='DAILY'::public.timesheet_scope_enum;
  if v_resolved_daily_rates then
    v_available_daily_evidence:=private.bpay_next_daily_approval_evidence_v1(v_tf.id);
    select (v_available_daily_evidence->'rates')
      ||coalesce(jsonb_agg(to_jsonb(r) order by r.rate_family,r.rate_code),'[]'::jsonb)
      into v_available_rows
      from private.bpay_next_contract_rate_rows_v1(
        '{}'::jsonb,v_contract.bucket_labels_json,v_contract.additional_rates_json,
        v_mileage_pay_rate,v_mileage_charge_rate) r
      where r.rate_family<>'STANDARD' or not exists (
        select 1 from jsonb_array_elements(v_available_daily_evidence->'rates') d
        where d->>'rate_family'='STANDARD' and d->>'rate_code'=r.rate_code);
  else
    select jsonb_agg(to_jsonb(r) order by r.rate_family,r.rate_code)
      into v_available_rows
      from private.bpay_next_contract_rate_rows_v1(
        v_contract.rates_json,v_contract.bucket_labels_json,v_contract.additional_rates_json,
        v_mileage_pay_rate,v_mileage_charge_rate) r;
  end if;
  -- Actual used-only Additional identities retain unknown Contract availability.
  for v_extra in select key,value from jsonb_each(v_tf.additional_units_json) order by key
  loop
    v_extra_code:=upper(btrim(v_extra.key));
    if jsonb_typeof(v_extra.value)<>'object' or v_extra_code=''
       or char_length(v_extra_code)>245 then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_SHAPE_INVALID';
    end if;
    if not exists (select 1 from jsonb_array_elements(v_available_rows) x
       where x->>'rate_family'='ADDITIONAL' and x->>'rate_code'=v_extra_code) then
      v_available_rows:=v_available_rows||jsonb_build_array(jsonb_build_object(
        'rate_family','ADDITIONAL','rate_code',v_extra_code,
        'unit_label',nullif(v_extra.value->>'unit_name',''),
        'paye_rate',null,'umbrella_rate',null,'charge_rate',null));
    end if;
  end loop;
  v_schedule_count:=jsonb_array_length(v_available_rows);

  insert into private.bpay_next_worker_control(candidate_id)
    values (v_contract.candidate_id) on conflict (candidate_id) do nothing;
  select * into v_work from private.bpay_next_work
    where booking_id=v_ts.booking_id for update;
  if not found then
    insert into private.bpay_next_work
      (candidate_id,contract_id,original_timesheet_id,booking_id,
       work_kind,week_ending_date)
      values (v_contract.candidate_id,v_contract.id,v_ts.timesheet_id,
              v_ts.booking_id,
              case when v_route='EXPENSE_CARRIER' then 'EXPENSE' else 'ORDINARY' end,
              v_ts.week_ending_date)
      returning * into v_work;
  elsif v_work.candidate_id<>v_contract.candidate_id
     or v_work.contract_id<>v_contract.id
     or v_work.week_ending_date<>v_ts.week_ending_date
     or v_work.work_kind<>(case when v_route='EXPENSE_CARRIER'
       then 'EXPENSE' else 'ORDINARY' end) then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_WORK_IDENTITY_CONFLICT';
  end if;
  insert into private.bpay_next_work_revision
    (work_id,revision_no,source_kind,physical_timesheet_id,
     financial_snapshot_id,physical_timesheet_version,source_umbrella_id,
     source_rate_authority_kind,source_rate_override_id,source_rate_default_id,
     source_pay_channel,week_ending_date,detail_kind,
     expected_line_count,expected_rate_schedule_count,certified_zero,
     approved_source_ex_vat,candidate_display_name,candidate_reference,
     client_display_name,job_title,band_label,timesheet_reference)
    values (v_work.id,v_work.current_revision_no+1,
            case when v_route='EXPENSE_CARRIER' then 'EXPENSE' else 'ORDINARY' end,
            v_ts.timesheet_id,v_tf.id,v_ts.version,v_source_umbrella_id,
            coalesce(nullif(v_tf.rate_source_refs_json->>'mode',''),
              nullif(v_tf.rate_source_refs_json->>'kind',''),
              nullif(v_tf.rate_source_refs_json->>'source_kind','')),
            nullif(v_tf.rate_source_refs_json->>'override_id','')::uuid,
            nullif(v_tf.rate_source_refs_json->>'default_id','')::uuid,
            upper(v_tf.pay_method),
            v_ts.week_ending_date,v_detail_kind,v_expected_count,
            v_schedule_count,v_expected_total=0,v_expected_total,
            v_candidate_name,v_candidate_ref,v_client_name,
            v_contract.role,v_contract.band,v_ts.reference_number)
    returning id into v_revision_id;
  insert into private.bpay_next_rate_schedule
    (revision_id,rate_family,rate_code,unit_label,paye_rate,umbrella_rate,charge_rate,
     approved_pay_rate,approved_charge_rate)
    select v_revision_id,r.rate_family,r.rate_code,r.unit_label,
      r.paye_rate,r.umbrella_rate,r.charge_rate,
      private.bpay_next_exact_rate_v1(case r.rate_family
        when 'STANDARD' then case r.rate_code when 'DAY' then v_tf.pay_day
          when 'NIGHT' then v_tf.pay_night when 'SAT' then v_tf.pay_sat
          when 'SUN' then v_tf.pay_sun when 'BH' then v_tf.pay_bh end::text
        when 'MILEAGE' then v_mileage_pay_rate::text
        when 'ADDITIONAL' then coalesce(a.value->>'pay_rate',a.value->>'rate') end),
      private.bpay_next_exact_rate_v1(case r.rate_family
        when 'STANDARD' then case r.rate_code when 'DAY' then v_tf.charge_day
          when 'NIGHT' then v_tf.charge_night when 'SAT' then v_tf.charge_sat
          when 'SUN' then v_tf.charge_sun when 'BH' then v_tf.charge_bh end::text
        when 'MILEAGE' then v_mileage_charge_rate::text
        when 'ADDITIONAL' then a.value->>'charge_rate' end)
    from jsonb_to_recordset(v_available_rows)
      r(rate_family text,rate_code text,unit_label text,paye_rate numeric,
        umbrella_rate numeric,charge_rate numeric)
    left join lateral (select value from jsonb_each(v_tf.additional_units_json)
      where upper(btrim(key))=r.rate_code and r.rate_family='ADDITIONAL') a on true;

  if v_mode='SEGMENTS' then
    -- One pass ordered by stable business date and original array ordinal.
    -- Window sums use exact stored amounts, not a new hours/rates calculator.
    -- Child INSERTs retain every original shift, including zero/excluded ones.
    for v_segment_row in
      select value,ordinality,
        row_number() over day_order as detail_no,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
            then 0 else (value->>'pay_amount')::numeric end) over day_set as day_pay,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_day')::numeric,0)
          +coalesce((value->>'hours_night')::numeric,0)
          +coalesce((value->>'hours_sat')::numeric,0)
          +coalesce((value->>'hours_sun')::numeric,0)
          +coalesce((value->>'hours_bh')::numeric,0) end) over day_set as day_hours,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_day')::numeric,0) end) over day_set as day_day,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_night')::numeric,0) end) over day_set as day_night,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_sat')::numeric,0) end) over day_set as day_sat,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_sun')::numeric,0) end) over day_set as day_sun,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_bh')::numeric,0) end) over day_set as day_bh
      from jsonb_array_elements(v_tf.invoice_breakdown_json->'segments') with ordinality
      window day_set as (partition by (value->>'date')::date),
        day_order as (partition by (value->>'date')::date order by ordinality)
      order by (value->>'date')::date,ordinality
    loop
      v_segment:=v_segment_row.value;
      if jsonb_typeof(v_segment)<>'object'
         or nullif(btrim(v_segment->>'segment_id'),'') is null
         or coalesce(v_segment->>'date','') !~ '^\d{4}-\d{2}-\d{2}$' then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SEGMENT_ID_OR_DATE_INVALID';
      end if;
      v_work_date:=(v_segment->>'date')::date;
      -- TS_DAY is the supported payment choice. A new physical UUID, edited
      -- clock/break or array order cannot create another DAY obligation.
      -- Per-segment authorisation exclusions remain independent below.
      v_component_key:='WORK:DAY:'||v_work_date::text;
      v_excluded:=coalesce((v_segment->>'exclude_from_pay')::boolean,false);
      v_amount:=case when v_excluded then 0
        else (v_segment->>'pay_amount')::numeric end;
      if v_amount is null or v_amount<>round(v_amount,2) then
        raise exception using errcode='22003', message='BPAY_NEXT_ORDINARY_SEGMENT_AMOUNT_INVALID';
      end if;
      v_hours:=0; v_rate_detail_count:=0;
      for v_bucket in select * from (values
        ('DAY'::text,'hours_day'::text,v_tf.pay_day),
        ('NIGHT','hours_night',v_tf.pay_night),
        ('SAT','hours_sat',v_tf.pay_sat),
        ('SUN','hours_sun',v_tf.pay_sun),
        ('BH','hours_bh',v_tf.pay_bh)) b(code,hours_key,pay_rate)
      loop
        if coalesce((v_segment->>v_bucket.hours_key)::numeric,0)<0
           or coalesce((v_segment->>v_bucket.hours_key)::numeric,0)
                <>round(coalesce((v_segment->>v_bucket.hours_key)::numeric,0),6) then
          raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SEGMENT_HOURS_INVALID';
        end if;
        v_hours:=v_hours+coalesce((v_segment->>v_bucket.hours_key)::numeric,0);
        if not v_excluded and coalesce((v_segment->>v_bucket.hours_key)::numeric,0)>0 then
          if v_bucket.pay_rate is null then
            raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
              detail=jsonb_build_object('bucket',v_bucket.code,'contract_id',v_contract.id)::text;
          end if;
          if v_bucket.pay_rate<>round(v_bucket.pay_rate,6)
             or abs(v_bucket.pay_rate)>=1000000000000 then
            raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_INVALID';
          end if;
          v_rate_detail_count:=v_rate_detail_count+1;
        end if;
      end loop;
      if v_segment_row.detail_no=1 then
        v_rate_detail_count:=(case when v_segment_row.day_day>0 then 1 else 0 end)
          +(case when v_segment_row.day_night>0 then 1 else 0 end)
          +(case when v_segment_row.day_sat>0 then 1 else 0 end)
          +(case when v_segment_row.day_sun>0 then 1 else 0 end)
          +(case when v_segment_row.day_bh>0 then 1 else 0 end);
        v_line_no:=v_line_no+1;
        insert into private.bpay_next_approved_line
          (revision_id,line_no,component_key,component_kind,work_date,
           approved_quantity,expected_rate_detail_count,source_pay_ex_vat,tax_treatment,evidence_ref)
          values (v_revision_id,v_line_no,v_component_key,'WORK',v_work_date,
            v_segment_row.day_hours,v_rate_detail_count,v_segment_row.day_pay,'TAXABLE',
            'timesheets_financials:'||v_tf.id::text||'#day:'||v_work_date::text)
          returning id into v_line_id;
        insert into private.bpay_next_rate_detail
          (approved_line_id,bucket,approved_hours,source_pay_rate)
          select v_line_id,b.code,b.hours,b.pay_rate from (values
            ('DAY'::text,v_segment_row.day_day,v_tf.pay_day),
            ('NIGHT',v_segment_row.day_night,v_tf.pay_night),
            ('SAT',v_segment_row.day_sat,v_tf.pay_sat),
            ('SUN',v_segment_row.day_sun,v_tf.pay_sun),
            ('BH',v_segment_row.day_bh,v_tf.pay_bh)) b(code,hours,pay_rate)
          where b.hours>0;
      end if;
      v_shift_start_local:=nullif(btrim(coalesce(v_segment->>'start_local',v_segment->>'start','')),'');
      v_shift_end_local:=nullif(btrim(coalesce(v_segment->>'end_local',v_segment->>'end','')),'');
      if (v_shift_start_local is null)<>(v_shift_end_local is null)
         or (v_shift_start_local is not null and
           (v_shift_start_local !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
            or v_shift_end_local !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$')) then
        raise exception using errcode='23514',message='BPAY_NEXT_ORDINARY_SCHEDULE_CLOCK_INVALID';
      end if;
      insert into private.bpay_next_shift_detail
        (approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,
         shift_start_local,shift_end_local,shift_overnight,
         approved_minutes,approved_hours,immutable_evidence_ref,
         segment_pay_ex_vat,pay_excluded,hours_day,hours_night,hours_sat,hours_sun,hours_bh,segment_key)
        values (v_line_id,v_segment_row.detail_no::integer,v_work_date,
          nullif(v_segment->>'start_utc','')::timestamptz,
          nullif(v_segment->>'end_utc','')::timestamptz,
          v_shift_start_local,v_shift_end_local,
          nullif(v_segment->>'overnight','')::boolean,
          case when v_hours*60=trunc(v_hours*60) then (v_hours*60)::integer end,
          v_hours,'timesheets_financials:'||v_tf.id::text||'#segment:'||v_segment_row.ordinality::text,
          v_amount,v_excluded,
          coalesce((v_segment->>'hours_day')::numeric,0),
          coalesce((v_segment->>'hours_night')::numeric,0),
          coalesce((v_segment->>'hours_sat')::numeric,0),
          coalesce((v_segment->>'hours_sun')::numeric,0),
          coalesce((v_segment->>'hours_bh')::numeric,0),v_segment->>'segment_id') returning id into v_shift_id;
      v_break_total:=0; v_break_no:=0;
      if v_segment ? 'breaks' and jsonb_typeof(v_segment->'breaks')<>'array' then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_BREAK_SHAPE_INVALID';
      end if;
      for v_break in select value from jsonb_array_elements(
        coalesce(v_segment->'breaks','[]'::jsonb))
      loop
        v_break_start:=v_break->>'start';
        v_break_end:=v_break->>'end';
        if jsonb_typeof(v_break)<>'object'
           or coalesce(v_break_start,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or coalesce(v_break_end,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
          raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_BREAK_CLOCK_INVALID';
        end if;
        v_begin_mins:=substring(v_break_start,1,2)::integer*60
          +substring(v_break_start,4,2)::integer;
        v_end_mins:=substring(v_break_end,1,2)::integer*60
          +substring(v_break_end,4,2)::integer;
        v_break_mins:=(v_end_mins-v_begin_mins+1440)%1440;
        if v_break_mins=0 then
          raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_BREAK_ZERO_WINDOW';
        end if;
        v_break_no:=v_break_no+1;
        v_break_total:=v_break_total+v_break_mins;
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_start_local,break_end_local,break_minutes)
          values (v_shift_id,v_break_no,v_break_start,v_break_end,v_break_mins);
      end loop;
      if v_break_no=0 and coalesce((v_segment->>'break_mins')::integer,
                                   (v_segment->>'break_minutes')::integer,0)>0 then
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_minutes)
          values (v_shift_id,1,coalesce((v_segment->>'break_mins')::integer,
                                        (v_segment->>'break_minutes')::integer));
      elsif v_break_no>0 and (v_segment ? 'break_mins' or v_segment ? 'break_minutes')
         and v_break_total<>coalesce((v_segment->>'break_mins')::integer,
                                    (v_segment->>'break_minutes')::integer) then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_BREAK_DURATION_CONFLICT';
      end if;
      v_written_total:=v_written_total+v_amount;
    end loop;
  elsif v_base_count=1 then
    for v_bucket in select * from (values
      ('DAY'::text,v_tf.hours_day,v_tf.pay_day),('NIGHT',v_tf.hours_night,v_tf.pay_night),
      ('SAT',v_tf.hours_sat,v_tf.pay_sat),('SUN',v_tf.hours_sun,v_tf.pay_sun),
      ('BH',v_tf.hours_bh,v_tf.pay_bh)) b(code,hours,pay_rate)
    loop
      if v_bucket.hours>0 and v_bucket.pay_rate is null then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
          detail=jsonb_build_object('bucket',v_bucket.code,'contract_id',v_contract.id)::text;
      end if;
      if v_bucket.hours<0 or v_bucket.hours<>round(v_bucket.hours,6)
         or (v_bucket.hours>0 and
             (v_bucket.pay_rate<>round(v_bucket.pay_rate,6)
              or abs(v_bucket.pay_rate)>=1000000000000)) then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_AGGREGATE_RATE_INVALID';
      end if;
    end loop;
    v_rate_detail_count:=
      (case when v_tf.hours_day>0 then 1 else 0 end)
      +(case when v_tf.hours_night>0 then 1 else 0 end)
      +(case when v_tf.hours_sat>0 then 1 else 0 end)
      +(case when v_tf.hours_sun>0 then 1 else 0 end)
      +(case when v_tf.hours_bh>0 then 1 else 0 end);
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,component_kind,approved_quantity,
       expected_rate_detail_count,source_pay_ex_vat,tax_treatment,evidence_ref)
      values (v_revision_id,v_line_no,'WORK:AGGREGATE','WORK',v_tf.total_hours,
              v_rate_detail_count,v_base_amount,'TAXABLE',
              'timesheets_financials:'||v_tf.id::text||'#aggregate')
      returning id into v_line_id;
    insert into private.bpay_next_rate_detail
      (approved_line_id,bucket,approved_hours,source_pay_rate)
      select v_line_id,b.code,b.hours,b.pay_rate
      from (values ('DAY'::text,v_tf.hours_day,v_tf.pay_day),
                   ('NIGHT',v_tf.hours_night,v_tf.pay_night),
                   ('SAT',v_tf.hours_sat,v_tf.pay_sat),
                   ('SUN',v_tf.hours_sun,v_tf.pay_sun),
                   ('BH',v_tf.hours_bh,v_tf.pay_bh)) b(code,hours,pay_rate)
      where b.hours>0;
    -- An AGGREGATE money line can still originate from an entered weekly
    -- schedule. Keep the entered date/clock/break evidence as small typed
    -- children, without inventing a money split across those shifts.
    if v_tf.actual_schedule_json is not null
       and jsonb_typeof(v_tf.actual_schedule_json)<>'array' then
      raise exception using errcode='23514',
        message='BPAY_NEXT_ORDINARY_SCHEDULE_SHAPE_INVALID';
    end if;
    v_schedule:=case
      when jsonb_typeof(v_tf.actual_schedule_json)='array'
       and jsonb_array_length(v_tf.actual_schedule_json)>0
        then v_tf.actual_schedule_json
      else v_ts.actual_schedule_json end;
    if v_schedule is not null and jsonb_typeof(v_schedule)<>'array' then
      raise exception using errcode='23514',
        message='BPAY_NEXT_ORDINARY_SCHEDULE_SHAPE_INVALID';
    end if;
    v_shift_no:=0;
    for v_shift_entry in select value from jsonb_array_elements(
      coalesce(v_schedule,'[]'::jsonb))
    loop
      if jsonb_typeof(v_shift_entry)<>'object'
         or coalesce(v_shift_entry->>'date',v_shift_entry->>'work_date','')
              !~ '^\d{4}-\d{2}-\d{2}$' then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_SCHEDULE_DATE_INVALID';
      end if;
      v_shift_start_local:=nullif(btrim(coalesce(v_shift_entry->>'start','')),'');
      v_shift_end_local:=nullif(btrim(coalesce(v_shift_entry->>'end','')),'');
      if (v_shift_start_local is null)<>(v_shift_end_local is null)
         or (v_shift_start_local is not null and
             (v_shift_start_local !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
              or v_shift_end_local !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$')) then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_SCHEDULE_CLOCK_INVALID';
      end if;
      v_shift_no:=v_shift_no+1;
      insert into private.bpay_next_shift_detail
        (approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,
         shift_start_local,shift_end_local,approved_minutes,approved_hours)
        values (v_line_id,v_shift_no,
          coalesce(v_shift_entry->>'date',v_shift_entry->>'work_date')::date,
          nullif(v_shift_entry->>'start_utc','')::timestamptz,
          nullif(v_shift_entry->>'end_utc','')::timestamptz,
          v_shift_start_local,v_shift_end_local,
          nullif(v_shift_entry->>'worked_minutes','')::integer,
          nullif(v_shift_entry->>'approved_hours','')::numeric)
        returning id into v_shift_id;
      if v_shift_entry ? 'breaks'
         and jsonb_typeof(v_shift_entry->'breaks') not in ('array','null') then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_BREAK_SHAPE_INVALID';
      end if;
      v_break_no:=0; v_break_total:=0;
      for v_break in select value from jsonb_array_elements(
        case when jsonb_typeof(v_shift_entry->'breaks')='array'
          then v_shift_entry->'breaks' else '[]'::jsonb end)
      loop
        v_break_start:=v_break->>'start';
        v_break_end:=v_break->>'end';
        if jsonb_typeof(v_break)<>'object'
           or coalesce(v_break_start,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or coalesce(v_break_end,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
          raise exception using errcode='23514',
            message='BPAY_NEXT_ORDINARY_BREAK_CLOCK_INVALID';
        end if;
        v_begin_mins:=substring(v_break_start,1,2)::integer*60
          +substring(v_break_start,4,2)::integer;
        v_end_mins:=substring(v_break_end,1,2)::integer*60
          +substring(v_break_end,4,2)::integer;
        v_break_mins:=(v_end_mins-v_begin_mins+1440)%1440;
        if v_break_mins=0 then
          raise exception using errcode='23514',
            message='BPAY_NEXT_ORDINARY_BREAK_ZERO_WINDOW';
        end if;
        v_break_no:=v_break_no+1;
        v_break_total:=v_break_total+v_break_mins;
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_start_local,break_end_local,break_minutes)
          values (v_shift_id,v_break_no,v_break_start,v_break_end,v_break_mins);
      end loop;
      if v_break_no=0
         and coalesce((v_shift_entry->>'break_minutes')::integer,
                      (v_shift_entry->>'break_mins')::integer,0)>0 then
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_minutes)
          values (v_shift_id,1,
            coalesce((v_shift_entry->>'break_minutes')::integer,
                     (v_shift_entry->>'break_mins')::integer));
      elsif v_break_no>0
         and (v_shift_entry ? 'break_minutes' or v_shift_entry ? 'break_mins')
         and v_break_total<>coalesce((v_shift_entry->>'break_minutes')::integer,
                                    (v_shift_entry->>'break_mins')::integer) then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_BREAK_DURATION_CONFLICT';
      end if;
    end loop;
    v_written_total:=v_written_total+v_base_amount;
  end if;

  for v_extra in select key,value from jsonb_each(v_tf.additional_units_json)
                 order by key
  loop
    if jsonb_typeof(v_extra.value)<>'object'
       or nullif(btrim(v_extra.key),'') is null
       or length('ADDITIONAL:'||upper(btrim(v_extra.key)))>256 then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_SHAPE_INVALID';
    end if;
    v_extra_quantity:=coalesce((v_extra.value->>'unit_count')::numeric,
                              (v_extra.value->>'units_week')::numeric);
    v_extra_pay_rate:=coalesce((v_extra.value->>'pay_rate')::numeric,
                              (v_extra.value->>'rate')::numeric);
    v_amount:=coalesce((v_extra.value->>'pay_ex_vat')::numeric,
      (v_extra.value->>'amount_ex_vat')::numeric,v_extra_quantity*v_extra_pay_rate);
    if v_amount is null or (v_extra_pay_rate is null and
        (coalesce(v_extra_quantity,0)<>0 or v_amount<>0)) then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
        detail=jsonb_build_object('bucket',upper(btrim(v_extra.key)),
          'contract_id',v_contract.id)::text;
    end if;
    if v_amount<>round(v_amount,2) then
      raise exception using errcode='22003', message='BPAY_NEXT_ORDINARY_ADDITIONAL_PRECISION_INVALID';
    end if;
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,component_kind,unit_label,
       approved_quantity,approved_unit_rate,source_pay_ex_vat,evidence_ref)
      values (v_revision_id,v_line_no,'ADDITIONAL:'||upper(btrim(v_extra.key)),
        'ADDITIONAL',nullif(v_extra.value->>'unit_name',''),
        coalesce((v_extra.value->>'unit_count')::numeric,
                 (v_extra.value->>'units_week')::numeric),
        coalesce((v_extra.value->>'pay_rate')::numeric,
                 (v_extra.value->>'rate')::numeric),v_amount,
        'timesheets_financials:'||v_tf.id::text||'#additional:'||v_extra.key);
    v_written_total:=v_written_total+v_amount;
  end loop;
  if v_written_total <> v_tf.total_pay_ex_vat-v_expense_total
      -v_tf.mileage_pay_ex_vat then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_WORK_TOTAL_MISMATCH';
  end if;

  for v_extra in select * from (values
      ('TRAVEL'::text,v_tf.travel_pay_ex_vat),
      ('ACCOMMODATION',v_tf.accommodation_pay_ex_vat),
      ('OTHER',v_tf.other_pay_ex_vat),
      ('EXPENSES',case when v_expense_count=1
        and v_tf.travel_pay_ex_vat=0 and v_tf.accommodation_pay_ex_vat=0
        and v_tf.other_pay_ex_vat=0 then v_expense_total else 0 end)) x(code,amount)
  loop
    if v_extra.amount<>0 then
      if v_extra.amount<>round(v_extra.amount,2) then
        raise exception using errcode='22003', message='BPAY_NEXT_ORDINARY_EXPENSE_PRECISION_INVALID';
      end if;
      v_line_no:=v_line_no+1;
      insert into private.bpay_next_approved_line
        (revision_id,line_no,component_key,component_kind,
         source_pay_ex_vat,evidence_ref)
        values (v_revision_id,v_line_no,'EXPENSE:'||v_extra.code,'EXPENSE',
          v_extra.amount,'timesheets_financials:'||v_tf.id::text||'#expense:'||v_extra.code);
      v_written_total:=v_written_total+v_extra.amount;
    end if;
  end loop;
  if v_mileage_count=1 then
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,component_kind,unit_label,
       approved_quantity,approved_unit_rate,source_pay_ex_vat,evidence_ref)
      values (v_revision_id,v_line_no,'MILEAGE:MILE','MILEAGE','mile',
        v_tf.mileage_units,v_mileage_pay_rate,v_tf.mileage_pay_ex_vat,
        'timesheets_financials:'||v_tf.id::text||'#mileage');
    v_written_total:=v_written_total+v_tf.mileage_pay_ex_vat;
  end if;
  if v_line_no<>v_expected_count or v_written_total<>v_expected_total then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_COMPONENT_TOTAL_MISMATCH';
  end if;
  perform private.bpay_next_publish_staged_revision_v1(
    v_work.id,v_revision_id,v_command_id);
  return v_revision_id;
end
$function$;
alter function private.bpay_next_publish_ordinary_from_financial_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_publish_ordinary_from_financial_v1(uuid) from public,anon,authenticated,service_role;

-- Exact current successor: supabase/repeatable/04102026_0570_banking_pay_next_component_selection_v1.sql
create or replace function private.bpay_next_work_choice_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;v_choice private.bpay_next_work_choice%rowtype;
  v_page private.bpay_next_component_selection_page%rowtype;v_count integer;
begin
  if tg_op='DELETE' then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';
  end if;
  if tg_op='UPDATE' and tg_table_name<>'bpay_next_work_choice' then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r
    where r.id=new.run_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
  end if;
  if tg_op='UPDATE' then
    if (old.selection_state='SEALED' and (to_jsonb(new)-'segment_page_count'-'segment_count'-'segment_sealed_count')
        is distinct from (to_jsonb(old)-'segment_page_count'-'segment_count'-'segment_sealed_count'))
      or new.run_id<>old.run_id or new.work_id<>old.work_id
      or new.expected_revision_id is distinct from old.expected_revision_id
      or new.selection_mode<>old.selection_mode
      or new.page_count<old.page_count or new.component_count<old.component_count then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';
    end if;
    if (old.segment_sealed_count is not null and (new.segment_page_count,new.segment_count,new.segment_sealed_count)
      is distinct from (old.segment_page_count,old.segment_count,old.segment_sealed_count)) or new.segment_control_sequence is distinct from old.segment_control_sequence
      or new.segment_page_count<old.segment_page_count or new.segment_count<old.segment_count then
      raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_SELECTION_IMMUTABLE';
    end if;
    if new.segment_page_count<>old.segment_page_count or new.segment_count<>old.segment_count then
      if new.segment_page_count<>old.segment_page_count+1 or new.segment_count<=old.segment_count
        or new.segment_count-old.segment_count>100 or not exists(select 1 from private.bpay_next_segment_selection_page p
          where p.run_id=new.run_id and p.work_id=new.work_id and p.page_no=new.segment_page_count
            and p.first_member_no=old.segment_count+1 and p.item_count=new.segment_count-old.segment_count)
        or (select count(*) from private.bpay_next_selected_segment s where s.run_id=new.run_id and s.work_id=new.work_id
          and s.selection_no>old.segment_count and s.selection_no<=new.segment_count)<>new.segment_count-old.segment_count then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_PAGE_COUNT_MISMATCH';
      end if;
    end if;
    if new.page_count<>old.page_count or new.component_count<>old.component_count then
      select p.* into strict v_page from private.bpay_next_component_selection_page p
        where p.run_id=new.run_id and p.work_id=new.work_id and p.page_no=new.page_count;
      select count(*)::integer into v_count from private.bpay_next_selected_component c
        where c.run_id=new.run_id and c.work_id=new.work_id and c.page_no=new.page_count;
      if new.page_count<>old.page_count+1 or v_page.first_selection_no<>old.component_count+1
        or new.component_count<>old.component_count+v_page.item_count or v_count<>v_page.item_count
        or new.selection_state<>old.selection_state then
        raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_PAGE_COUNT_MISMATCH';
      end if;
    end if;
  elsif tg_table_name='bpay_next_work_choice' then
    if new.page_count<>0 or new.component_count<>0
      or (new.selection_mode='SUBSET' and new.selection_state<>'OPEN') then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_PAGE_COUNT_MISMATCH';
    end if;
  else
    select c.* into strict v_choice from private.bpay_next_work_choice c
      where c.run_id=new.run_id and c.work_id=new.work_id;
    if v_choice.selection_mode<>'SUBSET' or v_choice.selection_state<>'OPEN'
      or new.page_no<>v_choice.page_count+1 then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
    end if;
    if tg_table_name='bpay_next_component_selection_page' then
      if new.first_selection_no<>v_choice.component_count+1 then
        raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_PAGE_COUNT_MISMATCH';
      end if;
    else
      select p.* into strict v_page from private.bpay_next_component_selection_page p
        where p.run_id=new.run_id and p.work_id=new.work_id and p.page_no=new.page_no;
      if new.expected_revision_id<>v_choice.expected_revision_id or new.page_item_no>v_page.item_count
        or new.selection_no<>v_page.first_selection_no+new.page_item_no-1 then
        raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_PAGE_COUNT_MISMATCH';
      end if;
    end if;
  end if;
  return new;
end
$function$;

alter function private.bpay_next_work_choice_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_choice_guard_v1() from public,anon,authenticated,service_role;
-- Exact current successor: supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql
create or replace function private.bpay_next_preparation_args_v1(p_args jsonb,p_keys text[])
returns void language plpgsql security invoker
set search_path=pg_catalog
as $function$
declare v_key text;v_type text;v_text text;
begin
  if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
    or pg_catalog.octet_length(p_args::text)>32768
    or (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.cardinality(p_keys)
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(p_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  foreach v_key in array p_keys loop
    v_type:=pg_catalog.jsonb_typeof(p_args->v_key);v_text:=p_args->>v_key;
    if v_key in ('choices','approved_line_ids','component_ids','selected','segment_keys') then
      if v_type is distinct from 'array' or pg_catalog.jsonb_array_length(p_args->v_key) not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='limit' then
      if v_type is distinct from 'number' or v_text!~'^[0-9]{1,3}$' or v_text::integer not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key like 'after_%' and v_type='null' then
      continue;
    elsif v_type is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    elsif v_key like '%_id' then
      if v_text!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key in ('pay_date','after_week') then
      if v_text!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or not pg_catalog.isfinite(v_text::date)
        or v_text::date::text<>v_text then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='pay_channel' then
      if v_text not in ('PAYE','UMBRELLA') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='after_bucket' then
      if v_text not in ('DAY','NIGHT','SAT','SUN','BH') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    else
      if v_text!~'^(0|[1-9][0-9]{0,18})$' or v_text::numeric>9223372036854775807
        or (v_key in ('page_no','selection_revision','review_revision') and v_text='0') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    end if;
  end loop;
end
$function$;

alter function private.bpay_next_preparation_args_v1(jsonb,text[]) owner to postgres;
revoke all on function private.bpay_next_preparation_args_v1(jsonb,text[]) from public,anon,authenticated,service_role;
-- Exact current successor: supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql
create or replace function public.bpay_next_preparation_intake_v1(p_actor_user_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_run uuid;v_work uuid;v_revision uuid;v_candidate uuid;v_result jsonb;v_output jsonb;
  v_ids uuid[];v_selected boolean[];v_entry jsonb;v_header private.bpay_next_pay_run%rowtype;
begin
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_action
    when 'CREATE' then array['run_id','pay_date']
    when 'WORK_PAGE' then array['run_id','page_no','choices']
    when 'COMPONENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','approved_line_ids']
    when 'SEGMENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','request_id','segment_keys']
    when 'SEGMENT_SEAL' then array['run_id','work_id','expected_revision_id','expected_pages','expected_count']
    when 'COMPONENT_SEAL' then array['run_id','work_id','expected_revision_id','expected_pages','expected_count']
    when 'CASE_PAGE' then array['run_id','candidate_id','selection_revision','page_no','request_id','component_ids','selected']
    when 'CASE_SEAL' then array['run_id','candidate_id','selection_revision','expected_pages','expected_items']
    when 'SEAL_ENQUEUE' then array['run_id','command_id','expected_pages','expected_count']
    when 'REVIEW' then array['run_id']
    when 'CONFIRM' then array['run_id','review_revision'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(p_args,v_keys);
  v_run:=(p_args->>'run_id')::uuid;v_work:=(p_args->>'work_id')::uuid;
  v_revision:=(p_args->>'expected_revision_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
  if p_action in ('COMPONENT_PAGE','CASE_PAGE') then
    for v_entry in select value from pg_catalog.jsonb_array_elements(
      p_args->case when p_action='CASE_PAGE' then 'component_ids' else 'approved_line_ids' end) loop
      if pg_catalog.jsonb_typeof(v_entry) is distinct from 'string'
        or (v_entry#>>'{}')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      v_ids:=pg_catalog.array_append(v_ids,(v_entry#>>'{}')::uuid);
    end loop;
    if p_action='CASE_PAGE' then
      if pg_catalog.jsonb_array_length(p_args->'selected')<>pg_catalog.cardinality(v_ids) then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      for v_entry in select value from pg_catalog.jsonb_array_elements(p_args->'selected') loop
        if pg_catalog.jsonb_typeof(v_entry) is distinct from 'boolean' then
          raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
        end if;
        v_selected:=pg_catalog.array_append(v_selected,(v_entry#>>'{}')::boolean);
      end loop;
    end if;
  end if;
  case p_action
  when 'CREATE' then
    v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,(p_args->>'pay_date')::date);
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'status',v_result->>'status',
      'selection_state',v_result->>'selection_state','selection_count',v_header.selection_count::text,
      'selected_candidate_count',v_header.selected_candidate_count::text);
  when 'WORK_PAGE' then
    v_output:=private.bpay_next_append_work_choices_v1(v_run,(p_args->>'page_no')::bigint,p_args->'choices');
  when 'COMPONENT_PAGE' then
    v_output:=private.bpay_next_append_component_selection_page_v1(v_run,v_work,v_revision,(p_args->>'page_no')::bigint,v_ids);
  when 'SEGMENT_PAGE' then
    if exists(select 1 from jsonb_array_elements(p_args->'segment_keys') item where jsonb_typeof(item)<>'string') then
      raise exception using errcode='22023',message='BPAY_NEXT_SEGMENT_SELECTION_KEY_INVALID';
    end if;
    v_output:=private.bpay_next_append_segment_selection_page_v1(v_run,v_work,v_revision,(p_args->>'page_no')::bigint,
      (p_args->>'request_id')::uuid,array(select value#>>'{}' from jsonb_array_elements(p_args->'segment_keys')));
  when 'SEGMENT_SEAL' then
    v_output:=private.bpay_next_seal_segment_selection_v1(v_run,v_work,v_revision,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
  when 'COMPONENT_SEAL' then
    v_output:=private.bpay_next_seal_component_selection_v1(v_run,v_work,v_revision,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
  when 'CASE_PAGE' then
    v_result:=private.bpay_next_append_case_selection_page_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'page_no')::bigint,(p_args->>'request_id')::uuid,v_ids,v_selected);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',p_args->>'selection_revision','page_no',v_result->>'page_no',
      'item_count',v_result->>'item_count','replay',v_result->'replay');
  when 'CASE_SEAL' then
    v_result:=private.bpay_next_seal_case_selection_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_items')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',v_result->>'selection_revision','sealed',v_result->'sealed',
      'selected_count',v_result->>'selected_count','excluded_count',v_result->>'excluded_count','replay',v_result->'replay');
  when 'SEAL_ENQUEUE' then
    perform private.bpay_next_seal_selection_v1(v_run,(p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
    v_result:=private.bpay_next_enqueue_preparation_v1(v_run,(p_args->>'command_id')::uuid);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'command_id',v_result->>'command_id',
      'agency_sequence',v_result->>'agency_sequence','replay',v_result->'replay');
  when 'REVIEW' then
    v_result:=private.bpay_next_review_simple_run_v1(v_run);
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase','reason',v_result->>'reason',
      'review_revision',v_header.review_revision::text,'all_ready',v_result->'all_ready',
      'replay',coalesce(v_result->'replay','false'::jsonb));
  when 'CONFIRM' then
    v_result:=private.bpay_next_confirm_simple_run_v1(v_run,(p_args->>'review_revision')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase',
      'review_revision',v_result->>'review_revision','replay',v_result->'replay','drafts',v_result->'drafts');
  end case;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';
  end if;
  return v_output;
end
$function$;

alter function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) to service_role;

-- Named stable detail producer successor: supabase/repeatable/03102026_1550_banking_pay_next_source_stage_v1.sql
create or replace function private.bpay_next_stage_source_current_v1(
  p_root_timesheet_id uuid, p_source_event_id uuid, p_expected_head_id uuid
) returns table(work_id uuid,revision_id uuid,source_event_id uuid)
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_segment_key text;
  v_existing record;
  v_ts public.timesheets%rowtype;
  v_tf public.timesheets_financials%rowtype;
  v_contract public.contracts%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_inventory jsonb;
  v_basis jsonb;
  v_scope jsonb;
  v_origin jsonb;
  v_initial_details jsonb;
  v_inventory_digest bytea;
  v_component jsonb;
  v_segment jsonb;
  v_chosen_component_sha bytea;
  v_chosen_detail_sha bytea;
  v_head_bundle_id uuid;
  v_head_bundle_revision bigint;
  v_segment_match_count integer;
  v_break jsonb;
  v_head_count integer;
  v_auth_id uuid;
  v_auth_generation integer;
  v_auth_signature text;
  v_auth_booking text;
  v_auth_version integer;
  v_head_authority text;
  v_head_certified_zero boolean;
  v_head_component_count integer;
  v_head_revision bigint;
  v_head_inventory_digest bytea;
  v_head_entitlement_digest bytea;
  v_head_generation_digest bytea;
  v_head_origin jsonb;
  v_candidate_name text;
  v_candidate_ref text;
  v_candidate_mileage_rate numeric;
  v_client_name text;
  v_client_mileage_rate numeric;
  v_component_id uuid;
  v_component_kind text;
  v_component_key text;
  v_component_amount numeric;
  v_total numeric:=0;
  v_hours numeric;
  v_rate_count integer;
  v_schedule_count integer;
  v_line_id uuid;
  v_shift_id uuid;
  v_break_no integer;
  v_break_minutes integer;
  v_break_total integer;
  v_break_start text;
  v_break_end text;
  v_use_segment boolean;
  v_line_no integer:=0;
  v_work_count integer:=0;
  v_missing_work_date boolean:=false;
  v_detail_kind text;
  v_revision_id uuid;
  v_first_snapshot boolean;
  v_source_pay_method text;
  v_bucket record;
  v_source_rate numeric;
begin
  if p_root_timesheet_id is null or p_source_event_id is null then
    raise exception using errcode='22023', message='BPAY_NEXT_SOURCE_STAGE_INPUT_INVALID';
  end if;
  if (select active_owner from private.bpay_next_module_control
      where id=1 for share)<>'NEXT' then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- Exact replay precedes current-state discovery: the Source head may have
  -- advanced since this already accepted event, but its revision cannot.
  select r.id as revision_id,w.id as work_id,r.physical_timesheet_id,
         r.source_head_id,r.source_kind
    into v_existing
    from private.bpay_next_work_revision r
    join private.bpay_next_work w on w.id=r.work_id
    where r.source_event_id=p_source_event_id;
  if found then
    if v_existing.physical_timesheet_id is distinct from p_root_timesheet_id
       or v_existing.source_head_id is distinct from p_expected_head_id
       or v_existing.source_kind not in ('SOURCE','PROTECTED') then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_STAGE_REPLAY_CONFLICT';
    end if;
    work_id:=v_existing.work_id;
    revision_id:=v_existing.revision_id;
    source_event_id:=p_source_event_id;
    return next;
    return;
  end if;

  if private.bpay_next_approval_route_v1(p_root_timesheet_id)<>'SOURCE_HOURS' then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_STAGE_ROUTE_INVALID';
  end if;
  select * into strict v_ts from public.timesheets
    where timesheet_id=p_root_timesheet_id for key share;
  select * into strict v_contract from public.contracts
    where id=v_ts.contract_id for key share;
  v_first_snapshot:=p_expected_head_id is null;
  -- A first authorisation is the sole case in which the financial snapshot
  -- supplies Source money. A committed head supplies its own complete money
  -- and may have no current TSFIN row (the Source publisher does not create
  -- one). Never require or silently substitute an older snapshot for a head.
  select * into v_tf from public.timesheets_financials
    where timesheet_id=p_root_timesheet_id and is_current=true for share;
  -- The saved contract channel is available even when the first-authorisation
  -- owner leaves TSFIN.pay_method NULL, or a head has no TSFIN at all.
  v_source_pay_method:=upper(coalesce(
    case when v_first_snapshot then v_tf.pay_method end,
    v_contract.pay_method_snapshot,''));
  if v_ts.is_current is distinct from true
     or (v_first_snapshot and (
       v_tf.id is null or v_ts.authorised_at_server is null
       or v_tf.authorised_at_utc is null
       or v_tf.is_stale is distinct from false
       or v_tf.timesheet_version<>v_ts.version
       or v_tf.candidate_id is distinct from v_contract.candidate_id
       or v_tf.client_id is distinct from v_contract.client_id))
     or v_source_pay_method not in ('PAYE','UMBRELLA') then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_STAGE_FINANCIAL_NOT_CURRENT',
      detail=pg_catalog.jsonb_build_object(
        'timesheet_current',v_ts.is_current is true,
        'timesheet_authorised',v_ts.authorised_at_server is not null,
        'financial_authorised',v_tf.authorised_at_utc is not null,
        'financial_fresh',v_tf.is_stale is false,
        'version_matches',v_tf.timesheet_version=v_ts.version,
        'candidate_matches',v_tf.candidate_id is not distinct from v_contract.candidate_id,
        'client_matches',v_tf.client_id is not distinct from v_contract.client_id,
        'pay_method',v_source_pay_method)::text;
  end if;
  select a.id,a.authorisation_generation,a.authorised_row_signature,
         a.family_booking_id,a.timesheet_version
    into v_auth_id,v_auth_generation,v_auth_signature,v_auth_booking,v_auth_version
    from public.weekly_source_root_authorisations a
    where a.root_timesheet_id=p_root_timesheet_id
      and a.withdrawn_at_utc is null
      and a.current_entitlement_head_id is not distinct from p_expected_head_id
    for share;
  if v_auth_id is null then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_STAGE_AUTHORITY_MISMATCH';
  end if;
  if v_first_snapshot then
    if v_auth_id<>p_source_event_id
       or exists(select 1 from public.weekly_source_entitlement_heads h
                 where pg_catalog.btrim(h.root_family_booking_id)
                       =pg_catalog.btrim(v_ts.booking_id)) then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_FIRST_SNAPSHOT_NOT_PERMITTED';
    end if;
    v_head_authority:='LOCKED_FINAL_SOURCE';
  else
    if p_source_event_id<>p_expected_head_id then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_HEAD_EVENT_MISMATCH';
    end if;
    select h.authority_kind,h.certified_zero,h.component_count,
           h.decision_bundle_id,h.bundle_revision,h.head_revision,
           h.inventory_digest,h.entitlement_digest,h.source_generation_digest,
           h.source_origin_json
      into v_head_authority,v_head_certified_zero,v_head_component_count,
           v_head_bundle_id,v_head_bundle_revision,v_head_revision,
           v_head_inventory_digest,v_head_entitlement_digest,v_head_generation_digest,
           v_head_origin
      from public.weekly_source_entitlement_heads h
      where h.id=p_expected_head_id and h.root_timesheet_id=p_root_timesheet_id
        and h.root_family_booking_id=v_ts.booking_id
        and h.root_timesheet_version=v_ts.version
        and h.candidate_id=v_contract.candidate_id
        and h.week_ending_date=v_ts.week_ending_date
        and h.contract_id=v_contract.id and h.state='COMMITTED_CURRENT'
      for share;
    if not found then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_HEAD_NOT_CURRENT';
    end if;
  end if;
  v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
  if coalesce((v_inventory->>'ok')::boolean,false) is not true
     or (v_inventory->>'authority') is distinct from
        (case when v_first_snapshot then 'TSFIN' else 'HEAD' end)
     or (v_inventory->>'head_id')::uuid is distinct from p_expected_head_id
     or pg_catalog.jsonb_typeof(v_inventory->'components')<>'array'
     or (v_inventory->>'component_count')::integer is distinct from
        pg_catalog.jsonb_array_length(v_inventory->'components')
     or (not v_first_snapshot and v_head_component_count is distinct from
        pg_catalog.jsonb_array_length(v_inventory->'components')) then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_INVENTORY_NOT_EXACT';
  end if;
  -- JD01: I7 calls the genuine Source I1 once for this complete vector. Missing
  -- basis is unavailable, never a zero approval or a second live-money route.
  -- This qualification precedes every Banking financial INSERT. The actual
  -- Source owner must capture all chosen HEAD detail before invoking this mapper.
  v_basis:=v_inventory->'approval_basis';
  v_scope:=v_basis->'scope';
  v_origin:=v_basis->'origin';
  if pg_catalog.jsonb_typeof(v_basis) is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_scope) is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_origin) is distinct from 'object'
     or v_basis->'coverage_complete' is distinct from 'true'::jsonb
     or v_basis->'component_count' is distinct from
        pg_catalog.to_jsonb(pg_catalog.jsonb_array_length(v_inventory->'components'))
     or coalesce(v_basis->>'inventory_digest','') !~ '^[0-9a-f]{64}$'
     or coalesce(v_basis->>'entitlement_digest','') !~ '^[0-9a-f]{64}$'
     or coalesce(v_basis->>'detail_digest','') !~ '^[0-9a-f]{64}$'
     or coalesce(v_basis->>'policy_fingerprint','') !~ '^[0-9a-f]{64}$'
     or coalesce(v_basis->>'approved_pay_ex_vat','') !~ '^-?[0-9]+[.][0-9]{2}$'
     or v_basis->>'inventory_digest' is distinct from v_inventory->>'inventory_digest'
     or v_scope->'root_timesheet_id' is distinct from pg_catalog.to_jsonb(v_ts.timesheet_id)
     or v_scope->'family_booking_id' is distinct from pg_catalog.to_jsonb(v_ts.booking_id)
     or v_scope->'root_version' is distinct from pg_catalog.to_jsonb(v_ts.version::text)
     or v_scope->'candidate_id' is distinct from pg_catalog.to_jsonb(v_contract.candidate_id)
     or v_scope->'client_id' is distinct from pg_catalog.to_jsonb(v_contract.client_id)
     or v_scope->'contract_id' is distinct from pg_catalog.to_jsonb(v_contract.id)
     or v_scope->'week_ending_date' is distinct from pg_catalog.to_jsonb(v_ts.week_ending_date)
     or v_auth_booking is distinct from v_ts.booking_id
     or v_auth_version is distinct from v_ts.version then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_APPROVAL_BASIS_NOT_EXACT';
  end if;
  -- A genuine ordinary HEAD can have no exceptional family. A declared family
  -- instead binds its exact PK/current root and bound version; neither lane
  -- trims or remaps the immutable raw WORK booking identity.
  if (v_scope->'target_family_id'='null'::jsonb
      and v_scope->'family_bound_version'='null'::jsonb) is not true then
    if pg_catalog.jsonb_typeof(v_scope->'target_family_id') is distinct from 'string'
       or pg_catalog.jsonb_typeof(v_scope->'family_bound_version') is distinct from 'string'
       or coalesce(v_scope->>'target_family_id','') !~
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_scope->>'family_bound_version','') !~ '^[1-9][0-9]*$' then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_APPROVAL_SCOPE_NOT_EXACT';
    end if;
    if not exists(select 1 from public.weekly_exceptional_pay_target_families f
      where f.id=(v_scope->>'target_family_id')::uuid
        and f.bound_version::text=v_scope->>'family_bound_version'
        and f.root_timesheet_id=v_ts.timesheet_id
        and f.candidate_id=v_contract.candidate_id and f.contract_id=v_contract.id
        and f.week_ending_date=v_ts.week_ending_date) then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_APPROVAL_SCOPE_NOT_EXACT';
    end if;
  end if;
  v_inventory_digest:=pg_catalog.decode(v_basis->>'inventory_digest','hex');
  if v_first_snapshot then
    v_initial_details:=pg_catalog.jsonb_build_object(
      'segments',v_tf.invoice_breakdown_json->'segments',
      'additional_units',v_tf.additional_units_json,
      'actual_schedule',v_tf.actual_schedule_json,
      'rate_source_refs',v_tf.rate_source_refs_json);
    if v_origin is distinct from pg_catalog.jsonb_build_object(
        'kind','INITIAL_AUTHORISED_TSFIN_V1','root_authorisation_id',v_auth_id,
        'financial_snapshot_id',v_tf.id,'authorisation_generation',v_auth_generation,
        'root_timesheet_id',v_ts.timesheet_id,'root_version',v_ts.version::text,
        'authorised_row_signature',v_auth_signature,
        'financial_snapshot_digest',pg_catalog.encode(
          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_APPROVED_FINANCIAL_SNAPSHOT_V2',pg_catalog.jsonb_build_object(
            'policy',v_tf.policy_snapshot_json,'details',v_initial_details,
            'components',v_inventory->'components')),'hex'),
        'inventory_digest',v_basis->>'inventory_digest',
        'entitlement_digest',v_basis->>'entitlement_digest')
       or v_basis->>'detail_digest' is distinct from pg_catalog.encode(
         private.weekly_source_sha256_jsonb_v1(
           'WEEKLY_SOURCE_APPROVED_DETAIL_V2',v_initial_details),'hex')
       or v_basis->>'policy_fingerprint' is distinct from pg_catalog.encode(
         private.weekly_source_sha256_jsonb_v1(
           'WEEKLY_SOURCE_APPROVED_POLICY_V2',v_tf.policy_snapshot_json),'hex') then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_INITIAL_BASIS_ORIGIN_MISMATCH';
    end if;
  elsif v_origin is distinct from pg_catalog.jsonb_build_object(
      'kind','COMMITTED_SOURCE_HEAD_V1','head_id',p_expected_head_id,
      'head_revision',v_head_revision::text,'decision_bundle_id',v_head_bundle_id,
      'bundle_revision',v_head_bundle_revision::text,'root_authorisation_id',v_auth_id,
      'authorisation_generation',v_auth_generation,'root_timesheet_id',v_ts.timesheet_id,
      'root_version',v_ts.version::text,
      'source_generation_digest',pg_catalog.encode(v_head_generation_digest,'hex'),
      'source_revision',v_head_origin,'inventory_digest',v_basis->>'inventory_digest',
      'entitlement_digest',v_basis->>'entitlement_digest')
     or v_inventory_digest is distinct from v_head_inventory_digest
     or pg_catalog.decode(v_basis->>'entitlement_digest','hex') is distinct from
        v_head_entitlement_digest
     or v_basis->>'policy_fingerprint' is distinct from v_head_origin->>'policy_fingerprint'
     or pg_catalog.jsonb_typeof(v_head_origin) is distinct from 'object'
     or private.weekly_source_publication_request_digest_v1(v_head_origin) is distinct from
        v_head_generation_digest then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_HEAD_BASIS_ORIGIN_MISMATCH';
  end if;
  for v_component in select value from pg_catalog.jsonb_array_elements(
    v_inventory->'components')
  loop
    v_component_kind:=v_component->>'component_kind';
    if v_component_kind not in
       ('WORKED_TIME','ADDITIONAL_UNIT','EXPENSE','SOURCE_FIXED_EXPENSE')
       or nullif(v_component->>'component_id','') is null
       or (v_component->>'pay_ex_vat')::numeric is null
       or (v_component->>'pay_ex_vat')::numeric<>
          pg_catalog.round((v_component->>'pay_ex_vat')::numeric,2)
       or (v_component->>'exclude_from_pay')::boolean is null then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_COMPONENT_INVALID';
    end if;
    if v_component_kind='WORKED_TIME' then
      v_work_count:=v_work_count+1;
      if nullif(v_component->>'work_date','') is null then
        v_missing_work_date:=true;
      end if;
    end if;
    v_total:=v_total+case when (v_component->>'exclude_from_pay')::boolean
      then 0 else (v_component->>'pay_ex_vat')::numeric end;
  end loop;
  if v_total<>pg_catalog.round(v_total,2) then
    raise exception using errcode='22003', message='BPAY_NEXT_SOURCE_TOTAL_PRECISION_INVALID';
  end if;
  if (v_basis->>'approved_pay_ex_vat')::numeric is distinct from v_total then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_APPROVAL_MONEY_MISMATCH';
  end if;
  -- With no committed head, the authorised financial snapshot is the first
  -- Source amount. An empty or incomplete component inventory must never
  -- turn a positive authorised amount into a certified zero payment.
  if v_first_snapshot and (v_tf.total_pay_ex_vat is null
       or v_total is distinct from pg_catalog.round(v_tf.total_pay_ex_vat,2)) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_FIRST_TOTAL_MISMATCH';
  end if;
  v_head_count:=pg_catalog.jsonb_array_length(v_inventory->'components');
  if v_head_count=0 and v_total<>0 then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_EMPTY_MONEY_INVALID';
  end if;
  v_detail_kind:=case when v_work_count>0 and not v_missing_work_date
    then 'SHIFT' when v_work_count>0 then 'AGGREGATE' else 'FIXED' end;
  select coalesce(nullif(btrim(c.display_name),''),
                  nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),
                  c.id::text),c.tms_ref,c.mileage_pay_rate
    into strict v_candidate_name,v_candidate_ref,v_candidate_mileage_rate
    from public.candidates c where c.id=v_contract.candidate_id;
  select cl.name,cl.mileage_charge_rate
    into strict v_client_name,v_client_mileage_rate
    from public.clients cl where cl.id=v_contract.client_id;
  select count(*) into v_schedule_count
    from private.bpay_next_contract_rate_rows_v1(
      v_contract.rates_json,v_contract.bucket_labels_json,
      v_contract.additional_rates_json,
      coalesce(v_tf.mileage_pay_rate,v_contract.mileage_pay_rate,
               v_candidate_mileage_rate),
      coalesce(v_tf.mileage_charge_rate,v_contract.mileage_charge_rate,
               v_client_mileage_rate));
  -- Source head publication can cover two roots. The worker-control lock is
  -- taken before either work lock, matching the pair publisher and worker.
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_contract.candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_contract.candidate_id for update;
  select * into v_work from private.bpay_next_work
    where booking_id=v_ts.booking_id for update;
  if not found then
    insert into private.bpay_next_work
      (candidate_id,contract_id,original_timesheet_id,booking_id,
       work_kind,week_ending_date)
      values(v_contract.candidate_id,v_contract.id,v_ts.timesheet_id,
             v_ts.booking_id,'SOURCE',v_ts.week_ending_date)
      returning * into v_work;
  elsif v_work.candidate_id<>v_contract.candidate_id
     or v_work.contract_id<>v_contract.id
     or v_work.week_ending_date<>v_ts.week_ending_date
     or v_work.work_kind<>'SOURCE' then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_WORK_IDENTITY_CONFLICT';
  end if;
  insert into private.bpay_next_work_revision
    (work_id,revision_no,source_kind,source_head_id,source_event_id,
     physical_timesheet_id,financial_snapshot_id,physical_timesheet_version,
     source_pay_channel,week_ending_date,detail_kind,expected_line_count,
     expected_rate_schedule_count,certified_zero,approved_source_ex_vat,
     candidate_display_name,candidate_reference,client_display_name,
     job_title,band_label,timesheet_reference,source_inventory_digest)
    values(v_work.id,v_work.current_revision_no+1,
      case when v_head_authority='PROTECTED' then 'PROTECTED' else 'SOURCE' end,
      p_expected_head_id,p_source_event_id,v_ts.timesheet_id,v_tf.id,v_ts.version,
      v_source_pay_method,v_ts.week_ending_date,v_detail_kind,v_head_count,
      v_schedule_count,case when v_first_snapshot then v_head_count=0
        else v_head_certified_zero end,v_total,
      v_candidate_name,v_candidate_ref,v_client_name,
      v_contract.role,v_contract.band,v_ts.reference_number,v_inventory_digest)
    returning id into v_revision_id;
  insert into private.bpay_next_rate_schedule
    (revision_id,rate_family,rate_code,unit_label,paye_rate,umbrella_rate,charge_rate)
    select v_revision_id,r.rate_family,r.rate_code,r.unit_label,
           r.paye_rate,r.umbrella_rate,r.charge_rate
      from private.bpay_next_contract_rate_rows_v1(
        v_contract.rates_json,v_contract.bucket_labels_json,
        v_contract.additional_rates_json,
        coalesce(v_tf.mileage_pay_rate,v_contract.mileage_pay_rate,
                 v_candidate_mileage_rate),
        coalesce(v_tf.mileage_charge_rate,v_contract.mileage_charge_rate,
                 v_client_mileage_rate)) r;

  for v_component in select value from pg_catalog.jsonb_array_elements(
    v_inventory->'components') order by (value->>'component_ordinal')::integer
  loop
    v_component_id:=(v_component->>'component_id')::uuid;
    v_component_kind:=v_component->>'component_kind';
    v_component_key:='SOURCE:'||v_component_id::text;
    v_component_amount:=case when (v_component->>'exclude_from_pay')::boolean
      then 0 else (v_component->>'pay_ex_vat')::numeric end;
    v_hours:=coalesce((v_component->>'hours_day')::numeric,0)
      +coalesce((v_component->>'hours_night')::numeric,0)
      +coalesce((v_component->>'hours_sat')::numeric,0)
      +coalesce((v_component->>'hours_sun')::numeric,0)
      +coalesce((v_component->>'hours_bh')::numeric,0);
    if (v_first_snapshot and v_hours<0)
       or v_hours<>pg_catalog.round(v_hours,6) then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_HOURS_INVALID';
    end if;
    v_rate_count:=0;
    if v_component_kind='WORKED_TIME' then
      -- A committed head can contain signed correction hours. Freeze every
      -- non-zero bucket exactly, but never invent a Source hourly rate from
      -- its independently authoritative component amount.
      v_rate_count:=(case when coalesce((v_component->>'hours_day')::numeric,0)<>0 then 1 else 0 end)
        +(case when coalesce((v_component->>'hours_night')::numeric,0)<>0 then 1 else 0 end)
        +(case when coalesce((v_component->>'hours_sat')::numeric,0)<>0 then 1 else 0 end)
        +(case when coalesce((v_component->>'hours_sun')::numeric,0)<>0 then 1 else 0 end)
        +(case when coalesce((v_component->>'hours_bh')::numeric,0)<>0 then 1 else 0 end);
    end if;
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,source_component_id,component_kind,tax_treatment,
       work_date,approved_quantity,approved_unit_rate,
       expected_rate_detail_count,source_pay_ex_vat,evidence_ref)
      values(v_revision_id,v_line_no,v_component_key,v_component_id,
        case v_component_kind when 'WORKED_TIME' then 'WORK'
          when 'ADDITIONAL_UNIT' then 'ADDITIONAL' else 'EXPENSE' end,
        case when v_component_kind='WORKED_TIME' then 'TAXABLE' else null end,
        nullif(v_component->>'work_date','')::date,
        case when v_component_kind='WORKED_TIME' then v_hours
          when v_component_kind='ADDITIONAL_UNIT'
            then (v_component->>'unit_count')::numeric else null end,
        case when v_component_kind='ADDITIONAL_UNIT'
          then (v_component->>'unit_pay_rate')::numeric else null end,
        v_rate_count,v_component_amount,
        'source-component:'||v_component_id::text||'#'||
          coalesce(v_component->>'component_sha256','no-hash'))
      returning id into v_line_id;
    if v_component_kind='WORKED_TIME'
       and nullif(v_component->>'work_date','') is not null then
      v_segment:=null; v_segment_match_count:=0;
      if v_first_snapshot then
        select pg_catalog.count(*)::integer,
               pg_catalog.jsonb_agg(segment.value)->0
          into v_segment_match_count,v_segment
          from pg_catalog.jsonb_array_elements(
            case when pg_catalog.jsonb_typeof(v_tf.invoice_breakdown_json->'segments')='array'
              then v_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end)
            segment(value)
          where segment.value->>'segment_id'=v_component->>'segment_id';
      else
        -- A published head stores amount and bucket hours but not accepted
        -- clocks/rates. Only its exact chosen-detail key may add those facts.
        select d.detail_json,d.component_sha256,d.detail_sha256
          into v_segment,v_chosen_component_sha,v_chosen_detail_sha
          from private.bpay_next_source_chosen_detail d
          where d.head_id=p_expected_head_id
            and d.component_id=v_component_id
            and d.decision_bundle_id=v_head_bundle_id
            and d.bundle_revision=v_head_bundle_revision;
        if not found or v_segment is null or (
           v_chosen_component_sha is distinct from
             pg_catalog.decode(v_component->>'component_sha256','hex')
           or v_chosen_detail_sha is distinct from
             pg_catalog.sha256(pg_catalog.convert_to(v_segment::text,'UTF8'))
           or v_segment->>'work_date' is distinct from v_component->>'work_date') then
          raise exception using errcode='23514',
            message='BPAY_NEXT_SOURCE_CHOSEN_DETAIL_MISMATCH';
        end if;
      end if;
      if v_segment_match_count>1 then
        raise exception using errcode='23514',
          message='BPAY_NEXT_SOURCE_SEGMENT_ID_AMBIGUOUS';
      end if;
      v_use_segment:=v_segment is not null
        and coalesce(v_segment->>'date',v_segment->>'work_date')
            =v_component->>'work_date'
        and (not v_first_snapshot or (
        coalesce((v_segment->>'hours_day')::numeric,0)
            =coalesce((v_component->>'hours_day')::numeric,0)
        and coalesce((v_segment->>'hours_night')::numeric,0)
            =coalesce((v_component->>'hours_night')::numeric,0)
        and coalesce((v_segment->>'hours_sat')::numeric,0)
            =coalesce((v_component->>'hours_sat')::numeric,0)
        and coalesce((v_segment->>'hours_sun')::numeric,0)
            =coalesce((v_component->>'hours_sun')::numeric,0)
        and coalesce((v_segment->>'hours_bh')::numeric,0)
            =coalesce((v_component->>'hours_bh')::numeric,0)));
      if v_first_snapshot and not v_use_segment then
        raise exception using errcode='23514',
          message='BPAY_NEXT_SOURCE_FIRST_SEGMENT_EVIDENCE_MISMATCH';
      end if;
      v_segment_key:=case when v_first_snapshot then nullif(v_segment#>>'{weekly_source,work_event_id}','')
        when v_segment->'from_frozen_pair'='true'::jsonb then nullif(v_segment->>'segment_key','')
        when v_segment->'from_reviewed_target_tsfin'='true'::jsonb then nullif(v_segment->>'before_segment_id','')
        else nullif(v_segment->>'work_event_id','') end;
      if v_segment_key is not null and v_segment->'from_frozen_pair' is distinct from 'true'::jsonb
        and v_segment->'from_reviewed_target_tsfin' is distinct from 'true'::jsonb then
        v_segment_key:=v_segment_key::uuid::text;
      end if;
      insert into private.bpay_next_shift_detail
        (approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,
         shift_start_local,shift_end_local,shift_overnight,
         approved_hours,
         detail_label,immutable_evidence_ref,segment_key,segment_pay_ex_vat,pay_excluded,
         hours_day,hours_night,hours_sat,hours_sun,hours_bh)
        values(v_line_id,1,(v_component->>'work_date')::date,
          case when v_use_segment then nullif(v_segment->>'start_utc','')::timestamptz end,
          case when v_use_segment then nullif(v_segment->>'end_utc','')::timestamptz end,
          case when v_use_segment then nullif(v_segment->>'start','') end,
          case when v_use_segment then nullif(v_segment->>'end','') end,
          case when v_use_segment then (v_segment->>'overnight')::boolean end,
          v_hours,
          nullif(v_component->>'reference_number',''),
          'source-component:'||v_component_id::text,v_segment_key,v_component_amount,
          (v_component->>'exclude_from_pay')::boolean,
          coalesce((v_component->>'hours_day')::numeric,0),coalesce((v_component->>'hours_night')::numeric,0),
          coalesce((v_component->>'hours_sat')::numeric,0),coalesce((v_component->>'hours_sun')::numeric,0),
          coalesce((v_component->>'hours_bh')::numeric,0))
        returning id into v_shift_id;
      if v_use_segment then
        if v_segment ? 'breaks'
           and pg_catalog.jsonb_typeof(v_segment->'breaks') not in ('array','null') then
          raise exception using errcode='23514',
            message='BPAY_NEXT_SOURCE_BREAK_SHAPE_INVALID';
        end if;
        v_break_no:=0; v_break_total:=0;
        for v_break in select value from pg_catalog.jsonb_array_elements(
          case when pg_catalog.jsonb_typeof(v_segment->'breaks')='array'
            then v_segment->'breaks' else '[]'::jsonb end)
        loop
          v_break_start:=v_break->>'start';
          v_break_end:=v_break->>'end';
          if pg_catalog.jsonb_typeof(v_break)<>'object'
             or coalesce(v_break_start,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
             or coalesce(v_break_end,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
            raise exception using errcode='23514',
              message='BPAY_NEXT_SOURCE_BREAK_CLOCK_INVALID';
          end if;
          v_break_minutes:=(
            (pg_catalog.substring(v_break_end,1,2)::integer*60
              +pg_catalog.substring(v_break_end,4,2)::integer)
            -(pg_catalog.substring(v_break_start,1,2)::integer*60
              +pg_catalog.substring(v_break_start,4,2)::integer)+1440)%1440;
          if v_break_minutes=0 then
            raise exception using errcode='23514',
              message='BPAY_NEXT_SOURCE_BREAK_ZERO_WINDOW';
          end if;
          v_break_no:=v_break_no+1;
          v_break_total:=v_break_total+v_break_minutes;
          insert into private.bpay_next_break_detail
            (shift_detail_id,break_no,break_start_local,break_end_local,break_minutes)
            values(v_shift_id,v_break_no,v_break_start,v_break_end,v_break_minutes);
        end loop;
        if v_break_no=0
           and coalesce((v_segment->>'break_mins')::integer,
                        (v_segment->>'break_minutes')::integer,0)>0 then
          insert into private.bpay_next_break_detail
            (shift_detail_id,break_no,break_minutes)
            values(v_shift_id,1,coalesce((v_segment->>'break_mins')::integer,
                (v_segment->>'break_minutes')::integer));
        elsif v_break_no>0
           and (v_segment ? 'break_mins' or v_segment ? 'break_minutes')
           and v_break_total is distinct from
               coalesce((v_segment->>'break_mins')::integer,
                        (v_segment->>'break_minutes')::integer) then
          raise exception using errcode='23514',
            message='BPAY_NEXT_SOURCE_BREAK_DURATION_CONFLICT';
        end if;
      end if;
    end if;
    if v_rate_count>0 then
      for v_bucket in select * from (values
        ('DAY'::text,'hours_day'::text,v_tf.pay_day),
        ('NIGHT','hours_night',v_tf.pay_night),
        ('SAT','hours_sat',v_tf.pay_sat),
        ('SUN','hours_sun',v_tf.pay_sun),
        ('BH','hours_bh',v_tf.pay_bh)) b(code,hours_key,pay_rate)
      loop
        if coalesce((v_component->>v_bucket.hours_key)::numeric,0)<>0 then
          -- TSFIN's pay_day/... are explicitly FIRST-SEGMENT display rates.
          -- A Source first authorisation may contain several shifts with
          -- different rates in the same bucket, so use each exact matched
          -- segment's immutable Source pay vector when it exists.
          v_source_rate:=case
            when not v_use_segment then null
            when v_first_snapshot
              and pg_catalog.jsonb_typeof(v_segment->'weekly_source')='object'
              then (v_segment#>>array['weekly_source','pay_vector','rates',
                                    pg_catalog.lower(v_bucket.code)])::numeric
            when v_first_snapshot and v_work_count=1 then v_bucket.pay_rate
            when v_first_snapshot then null
            else (v_segment->'rates'->>pg_catalog.lower(v_bucket.code))::numeric
          end;
           if v_use_segment
              and not (v_source_rate is null and (
                coalesce(v_segment->>'from_frozen_pair','false')='true'
                or (v_first_snapshot
                    and (v_component->>'exclude_from_pay')::boolean)))
             and (v_source_rate is null
             or v_source_rate<>pg_catalog.round(v_source_rate,6)) then
            raise exception using errcode='23514',
              message='BPAY_NEXT_SOURCE_USED_RATE_MISSING';
          end if;
          insert into private.bpay_next_rate_detail
            (approved_line_id,bucket,approved_hours,source_pay_rate)
            values(v_line_id,v_bucket.code,
              (v_component->>v_bucket.hours_key)::numeric,v_source_rate);
        end if;
      end loop;
    end if;
  end loop;
  work_id:=v_work.id;
  revision_id:=v_revision_id;
  source_event_id:=p_source_event_id;
  return next;
end
$function$;

alter function private.bpay_next_stage_source_current_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_stage_source_current_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
-- Named stable detail producer successor: supabase/repeatable/03102026_1548_banking_pay_next_source_chosen_detail_v1.sql
create or replace function private.bpay_next_capture_source_pair_before_detail_v1(
  p_decision_bundle_id uuid,p_bundle_revision bigint,
  p_before_source_event_ids uuid[],p_before_financial_snapshot_ids uuid[]
) returns integer
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_bundle public.weekly_source_entitlement_decision_bundles%rowtype;
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_auth public.weekly_source_root_authorisations%rowtype;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_before_head public.weekly_source_entitlement_heads%rowtype;
  v_tf public.timesheets_financials%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_line private.bpay_next_approved_line%rowtype;
  v_shift private.bpay_next_shift_detail%rowtype;
  v_existing private.bpay_next_source_chosen_detail%rowtype;
  v_roots uuid[];
  v_bookings text[];
  v_contracts uuid[];
  v_before_heads uuid[]:=array[null,null]::uuid[];
  v_before_revisions uuid[]:=array[null,null]::uuid[];
  v_tf_components jsonb[]:=array[null,null]::jsonb[];
  v_i integer;
  v_origin integer;
  v_role integer;
  v_count integer:=0;
  v_seen integer;
  v_matches integer;
  v_before_count integer;
  v_expected_rates integer;
  v_actual_rates integer;
  v_hours numeric;
  v_bucket record;
  v_row record;
  v_component public.weekly_source_entitlement_head_components%rowtype;
  v_before_component public.weekly_source_entitlement_head_components%rowtype;
  v_canonical jsonb;
  v_before_canonical jsonb;
  v_component_sha bytea;
  v_composed jsonb;
  v_segment jsonb;
  v_rates jsonb;
  v_rate numeric;
  v_stored_hours numeric;
  v_detail jsonb;
  v_detail_sha bytea;
  v_break jsonb;
  v_breaks jsonb;
  v_break_total integer;
  v_break_no integer;
  v_break_minutes integer;
  v_all_clocked boolean;
  v_start text;
  v_end text;
  v_start_utc timestamptz;
  v_end_utc timestamptz;
  v_blank jsonb;
begin
  if p_decision_bundle_id is null or p_bundle_revision is null or p_bundle_revision<1
     or p_before_source_event_ids is null or p_before_financial_snapshot_ids is null
     or pg_catalog.array_ndims(p_before_source_event_ids) is distinct from 1
     or pg_catalog.array_ndims(p_before_financial_snapshot_ids) is distinct from 1
     or pg_catalog.array_lower(p_before_source_event_ids,1) is distinct from 1
     or pg_catalog.array_lower(p_before_financial_snapshot_ids,1) is distinct from 1
     or pg_catalog.cardinality(p_before_source_event_ids)<>2
     or pg_catalog.cardinality(p_before_financial_snapshot_ids)<>2
     or p_before_source_event_ids[1] is null then
    raise exception using errcode='22023',message='BPAY_NEXT_PAIR_DETAIL_INPUT_INVALID';
  end if;
  perform 1 from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if not found then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into v_bundle from public.weekly_source_entitlement_decision_bundles
    where decision_bundle_id=p_decision_bundle_id and bundle_revision=p_bundle_revision
    for share;
  if not found or v_bundle.bundle_kind is distinct from 'CROSS_CONTRACT_A_B'
     or v_bundle.state not in ('PROPOSED','COMMITTED')
     or v_bundle.source_root_timesheet_id is null or v_bundle.target_root_timesheet_id is null
     or v_bundle.source_root_timesheet_id=v_bundle.target_root_timesheet_id
     or v_bundle.source_contract_id is null or v_bundle.target_contract_id is null
     or v_bundle.source_contract_id=v_bundle.target_contract_id
     or pg_catalog.array_ndims(v_bundle.proposed_head_ids) is distinct from 1
     or pg_catalog.array_lower(v_bundle.proposed_head_ids,1) is distinct from 1
     or pg_catalog.cardinality(v_bundle.proposed_head_ids)<>2
     or v_bundle.proposed_head_ids[1] is null or v_bundle.proposed_head_ids[2] is null
     or v_bundle.proposed_head_ids[1]=v_bundle.proposed_head_ids[2] then
    raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BUNDLE_NOT_EXACT';
  end if;
  v_roots:=array[v_bundle.source_root_timesheet_id,v_bundle.target_root_timesheet_id];
  v_bookings:=array[v_bundle.source_root_family_booking_id,v_bundle.target_root_family_booking_id];
  v_contracts:=array[v_bundle.source_contract_id,v_bundle.target_contract_id];

  -- Source family/root locks already precede this call. Reacquire exact rows
  -- consistently, without resolving old physical versions or choosing history.
  for v_row in select t.timesheet_id from public.timesheets t
    where t.timesheet_id=any(v_roots)
    order by pg_catalog.btrim(t.booking_id),t.timesheet_id for key share
  loop null; end loop;
  for v_row in select c.id from public.contracts c
    where c.id=any(v_contracts) order by c.id for key share
  loop null; end loop;
  for v_role in select i from pg_catalog.generate_series(1,2) r(i)
    order by pg_catalog.btrim(v_bookings[i]),v_roots[i]
  loop
    v_i:=v_role;
    select * into strict v_root from public.timesheets where timesheet_id=v_roots[v_i];
    select * into strict v_contract from public.contracts where id=v_contracts[v_i];
    if v_root.is_current is distinct from true or v_root.archived_at_utc is not null
       or v_root.revoked_at is not null or v_root.booking_id is distinct from v_bookings[v_i]
       or v_root.contract_id is distinct from v_contracts[v_i]
       or v_root.week_ending_date is distinct from v_bundle.week_ending_date
       or v_contract.candidate_id is distinct from v_bundle.candidate_id then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_ROOT_NOT_EXACT';
    end if;
    -- A canonical current collision is a refusal, never an identity remap.
    select pg_catalog.count(*)::integer into v_seen from (
      select t.timesheet_id from public.timesheets t
      where t.is_current=true and t.booking_id is not null and pg_catalog.btrim(t.booking_id)<>''
        and pg_catalog.btrim(t.booking_id)=pg_catalog.btrim(v_bookings[v_i])
      limit 2) actual_current;
    if v_seen<>1 then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_ROOT_NOT_EXACT';
    end if;
    select * into v_head from public.weekly_source_entitlement_heads
      where id=v_bundle.proposed_head_ids[v_i] for share;
    if not found or v_head.state is distinct from 'STAGED'
       or v_head.decision_bundle_id is distinct from p_decision_bundle_id
       or v_head.bundle_revision is distinct from p_bundle_revision
       or v_head.decision_id is distinct from v_bundle.decision_id
       or v_head.decided_by_user_id is distinct from v_bundle.decided_by_user_id
       or v_head.agency_id is distinct from v_bundle.agency_id
       or v_head.root_timesheet_id is distinct from v_roots[v_i]
       or v_head.root_family_booking_id is distinct from v_bookings[v_i]
       or v_head.root_timesheet_version is distinct from v_root.version
       or v_head.candidate_id is distinct from v_bundle.candidate_id
       or v_head.contract_id is distinct from v_contracts[v_i]
       or v_head.week_ending_date is distinct from v_bundle.week_ending_date
       or (select pg_catalog.count(*) from public.weekly_source_entitlement_head_components c
           where c.head_id=v_head.id) is distinct from v_head.component_count::bigint then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_STAGED_NOT_EXACT';
    end if;
    select pg_catalog.count(*)::integer into v_seen from (
      select a.id from public.weekly_source_root_authorisations a
      where a.withdrawn_at_utc is null and (a.root_timesheet_id=v_roots[v_i]
        or pg_catalog.btrim(a.family_booking_id)=pg_catalog.btrim(v_bookings[v_i]))
      order by a.id limit 2 for update) actual_authorisation;
    select * into v_auth from public.weekly_source_root_authorisations a
      where a.root_timesheet_id=v_roots[v_i] and a.withdrawn_at_utc is null;
    select pg_catalog.count(*)::integer into v_before_count from (
      select h.id from public.weekly_source_entitlement_heads h
      where h.state='COMMITTED_CURRENT'
        and (h.root_timesheet_id=v_roots[v_i]
          or pg_catalog.btrim(h.root_family_booking_id)=pg_catalog.btrim(v_bookings[v_i]))
      order by h.id limit 2 for share) actual_before;
    if v_seen>1 or v_before_count>1 then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BEFORE_NOT_EXACT';
    end if;
    if v_before_count=1 then
      select * into strict v_before_head from public.weekly_source_entitlement_heads h
        where h.state='COMMITTED_CURRENT' and (h.root_timesheet_id=v_roots[v_i]
          or pg_catalog.btrim(h.root_family_booking_id)=pg_catalog.btrim(v_bookings[v_i]));
      if v_seen<>1 or v_auth.id is null
         or v_auth.family_booking_id is distinct from v_bookings[v_i]
         or v_auth.timesheet_version is distinct from v_root.version
         or v_auth.current_entitlement_head_id is distinct from v_before_head.id
         or p_before_source_event_ids[v_i] is distinct from v_before_head.id
         or p_before_financial_snapshot_ids[v_i] is not null
         or v_before_head.root_timesheet_id is distinct from v_roots[v_i]
         or v_before_head.root_family_booking_id is distinct from v_bookings[v_i]
         or v_before_head.root_timesheet_version is distinct from v_root.version
         or v_before_head.candidate_id is distinct from v_bundle.candidate_id
         or v_before_head.contract_id is distinct from v_contracts[v_i]
         or v_before_head.week_ending_date is distinct from v_bundle.week_ending_date then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_HEAD_BEFORE_NOT_EXACT';
      end if;
      v_before_heads[v_i]:=v_before_head.id;
    else
      if v_seen=1 then
        if v_auth.id is null or v_auth.id is distinct from p_before_source_event_ids[v_i]
           or v_auth.family_booking_id is distinct from v_bookings[v_i]
           or v_auth.timesheet_version is distinct from v_root.version
           or v_auth.current_entitlement_head_id is not null
           or v_root.authorised_at_server is null then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_INITIAL_BEFORE_NOT_EXACT';
        end if;
      elsif v_i<>2 or p_before_source_event_ids[v_i] is not null then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_NULL_EVENT_NOT_PERMITTED';
      end if;
      if p_before_financial_snapshot_ids[v_i] is null then
        -- Only an actually blank new B has no own before evidence. The caller's
        -- accepted target_auth/signature checks still dominate this helper;
        -- genuine I6 must succeed after capture, before activation.
        v_blank:=private.weekly_source_publication_target_root_blank_v1(v_roots[v_i],v_bookings[v_i]);
        if v_i<>2 or v_seen<>0 or v_blank->'blank' is distinct from 'true'::jsonb then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FINANCIAL_REQUIRED';
        end if;
        v_tf_components[v_i]:='[]'::jsonb;
        continue;
      end if;
      select * into v_tf from public.timesheets_financials
        where id=p_before_financial_snapshot_ids[v_i] for share;
      select pg_catalog.count(*)::integer into v_matches from (
        select f.id from public.timesheets_financials f
        where f.timesheet_id=v_roots[v_i] and f.is_current=true limit 2) actual_financial;
      if v_tf.id is null or v_matches<>1 or v_tf.timesheet_id is distinct from v_roots[v_i]
         or v_tf.timesheet_version is distinct from v_root.version
         or v_tf.is_current is distinct from true or v_tf.is_stale is distinct from false
         or v_tf.candidate_id is distinct from v_bundle.candidate_id
         or v_tf.client_id is distinct from v_contract.client_id
         or pg_catalog.jsonb_typeof(v_tf.invoice_breakdown_json->'segments') is distinct from 'array'
         or (v_seen=1 and v_tf.authorised_at_utc is null) then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FINANCIAL_NOT_EXACT';
      end if;
      if v_seen=0 then
        v_blank:=private.weekly_source_publication_target_root_blank_v1(v_roots[v_i],v_bookings[v_i]);
        if v_blank->'blank' is distinct from 'true'::jsonb and (
          v_bundle.whole_root_review_required is distinct from true
          or v_bundle.whole_root_reviewed_by_user_id is null
          or v_bundle.whole_root_reviewed_at_utc is null
          or not pg_catalog.isfinite(v_bundle.whole_root_reviewed_at_utc)) then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_TARGET_REVIEW_REQUIRED';
        end if;
      end if;
      -- One composer over each exact named snapshot's existing segment vector.
      -- It selects no current upload, movement ladder, revision history or I7.
      v_tf_components[v_i]:=private.weekly_source_entitlement_components_v1(
        v_tf.invoice_breakdown_json->'segments','[]'::jsonb);
    end if;
  end loop;

  perform 1 from public.candidates where id=v_bundle.candidate_id for key share;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_bundle.candidate_id for update;
  if not found then
    raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_CANDIDATE_OWNER_MISSING';
  end if;
  -- Unique immutable Source event, never current_revision_id/applied_revision_id.
  for v_i in 1..2 loop
    if v_before_heads[v_i] is not null or p_before_source_event_ids[v_i] is null then
      continue;
    end if;
    select * into v_revision from private.bpay_next_work_revision
      where source_event_id=p_before_source_event_ids[v_i] for share;
    if not found or v_revision.source_kind is distinct from 'SOURCE'
       or v_revision.source_head_id is not null
       or v_revision.financial_snapshot_id is distinct from p_before_financial_snapshot_ids[v_i]
       or v_revision.physical_timesheet_id is distinct from v_roots[v_i]
       or v_revision.physical_timesheet_version is distinct from
         (select t.version from public.timesheets t where t.timesheet_id=v_roots[v_i])
       or v_revision.week_ending_date is distinct from v_bundle.week_ending_date
       or v_revision.currency is distinct from 'GBP'
       or v_revision.approved_at_utc is null or v_revision.sealed_at_utc is null
       or not pg_catalog.isfinite(v_revision.approved_at_utc)
       or not pg_catalog.isfinite(v_revision.sealed_at_utc) then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_INITIAL_REVISION_NOT_EXACT';
    end if;
    select * into strict v_work from private.bpay_next_work where id=v_revision.work_id;
    if v_work.work_kind is distinct from 'SOURCE'
       or v_work.booking_id is distinct from v_bookings[v_i]
       or v_work.candidate_id is distinct from v_bundle.candidate_id
       or v_work.contract_id is distinct from v_contracts[v_i]
       or v_work.week_ending_date is distinct from v_bundle.week_ending_date
       or not exists (
         select 1 from private.bpay_next_publication p
         join private.bpay_next_command c on c.id=p.command_id
         join private.bpay_next_command_member m
           on m.command_id=c.id and m.candidate_id=v_bundle.candidate_id
         where p.work_id=v_work.id and p.revision_id=v_revision.id
           and p.candidate_id=v_bundle.candidate_id and p.revision_no=v_revision.revision_no
           and c.id=private.bpay_next_source_command_id_v1(p_before_source_event_ids[v_i])
           and c.command_kind='POSITION_APPLY' and c.status in ('SEALED','COMPLETE')
           and c.expected_member_count=1 and c.sealed_at_utc is not null
           and pg_catalog.isfinite(c.sealed_at_utc) and m.member_no=1) then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_INITIAL_PUBLICATION_NOT_EXACT';
    end if;
    v_before_revisions[v_i]:=v_revision.id;
  end loop;

  for v_component in select c.* from public.weekly_source_entitlement_head_components c
    where c.head_id=any(v_bundle.proposed_head_ids) and c.component_kind='WORKED_TIME'
    order by c.head_id,c.component_ordinal
  loop
    -- The retained canonical owner requires DEC JSON strings, not the JSON
    -- numbers produced by to_jsonb(row). Its own scale/padding/magnitude guards
    -- remain authoritative; nullable numeric::text preserves NULL, and the
    -- explicit ISO date is independent of the session's DateStyle.
    v_canonical:=private.weekly_source_publication_component_canonical_v1(
      (pg_catalog.to_jsonb(v_component)-array['id','head_id','component_sha256',
        'decision_bundle_id','bundle_revision','created_at_utc'])||pg_catalog.jsonb_build_object(
        'work_date',pg_catalog.to_char(v_component.work_date,'YYYY-MM-DD'),
        'hours_day',v_component.hours_day::text,'hours_night',v_component.hours_night::text,
        'hours_sat',v_component.hours_sat::text,'hours_sun',v_component.hours_sun::text,
        'hours_bh',v_component.hours_bh::text,'unit_count',v_component.unit_count::text,
        'unit_pay_rate',v_component.unit_pay_rate::text,'unit_charge_rate',v_component.unit_charge_rate::text,
        'pay_ex_vat',v_component.pay_ex_vat::text,'charge_ex_vat',v_component.charge_ex_vat::text),
      'bpay_next_pair_detail.successor');
    v_component_sha:=private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_component_content_v1(v_canonical));
    if v_component.decision_bundle_id is distinct from p_decision_bundle_id
       or v_component.bundle_revision is distinct from p_bundle_revision
       or v_component.component_sha256 is distinct from v_component_sha then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_COMPONENT_NOT_EXACT';
    end if;
    v_matches:=0; v_origin:=null; v_segment:=null;
    for v_i in 1..2 loop
      if v_before_heads[v_i] is not null then
        if exists(select 1 from public.weekly_source_entitlement_head_components c
          where c.head_id=v_before_heads[v_i] and c.component_id=v_component.component_id) then
          v_matches:=v_matches+1; v_origin:=v_i;
        end if;
      else
        select pg_catalog.count(*)::integer into v_seen
          from pg_catalog.jsonb_array_elements(v_tf_components[v_i]) c(value)
          where c.value->>'component_id'=v_component.component_id::text;
        if v_seen>1 then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_COMPONENT_AMBIGUOUS';
        elsif v_seen=1 then v_matches:=v_matches+1; v_origin:=v_i;
        end if;
      end if;
    end loop;
    if v_matches<>1 then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_COMPONENT_ORIGIN_NOT_EXACT';
    end if;
    v_i:=v_origin;
    if v_before_heads[v_i] is not null then
      select * into strict v_before_component from public.weekly_source_entitlement_head_components c
        where c.head_id=v_before_heads[v_i] and c.component_id=v_component.component_id;
      v_before_canonical:=private.weekly_source_publication_component_canonical_v1(
        (pg_catalog.to_jsonb(v_before_component)-array['id','head_id','component_sha256',
          'decision_bundle_id','bundle_revision','created_at_utc'])||pg_catalog.jsonb_build_object(
          'work_date',pg_catalog.to_char(v_before_component.work_date,'YYYY-MM-DD'),
          'hours_day',v_before_component.hours_day::text,'hours_night',v_before_component.hours_night::text,
          'hours_sat',v_before_component.hours_sat::text,'hours_sun',v_before_component.hours_sun::text,
          'hours_bh',v_before_component.hours_bh::text,'unit_count',v_before_component.unit_count::text,
          'unit_pay_rate',v_before_component.unit_pay_rate::text,'unit_charge_rate',v_before_component.unit_charge_rate::text,
          'pay_ex_vat',v_before_component.pay_ex_vat::text,'charge_ex_vat',v_before_component.charge_ex_vat::text),
        'bpay_next_pair_detail.before');
      select * into strict v_before_head from public.weekly_source_entitlement_heads
        where id=v_before_heads[v_i];
      select * into v_existing from private.bpay_next_source_chosen_detail d
        where d.head_id=v_before_heads[v_i] and d.component_id=v_component.component_id;
      if not found or v_before_component.component_sha256 is distinct from v_component_sha
         or private.weekly_source_publication_request_digest_v1(
           private.weekly_source_publication_component_content_v1(v_before_canonical))
              is distinct from v_component_sha
         or v_existing.decision_bundle_id is distinct from v_before_head.decision_bundle_id
         or v_existing.bundle_revision is distinct from v_before_head.bundle_revision
         or v_existing.component_sha256 is distinct from v_component_sha
         or v_existing.detail_sha256 is distinct from
           pg_catalog.sha256(pg_catalog.convert_to(v_existing.detail_json::text,'UTF8')) then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_HEAD_COPY_NOT_EXACT';
      end if;
      v_detail:=v_existing.detail_json; -- exact old JSON, no UTC/rate enrichment
    else
      select c.value into strict v_composed from pg_catalog.jsonb_array_elements(v_tf_components[v_i]) c(value)
        where c.value->>'component_id'=v_component.component_id::text;
      v_before_canonical:=private.weekly_source_publication_component_canonical_v1(
        v_composed,'bpay_next_pair_detail.snapshot');
      if private.weekly_source_publication_request_digest_v1(
           private.weekly_source_publication_component_content_v1(v_before_canonical))
             is distinct from v_component_sha then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_SNAPSHOT_COMPONENT_NOT_EXACT';
      end if;
      select * into strict v_tf from public.timesheets_financials
        where id=p_before_financial_snapshot_ids[v_i];
      if v_before_revisions[v_i] is not null then
        select * into v_line from private.bpay_next_approved_line
          where revision_id=v_before_revisions[v_i]
            and component_key='SOURCE:'||v_component.component_id::text;
        v_hours:=coalesce(v_component.hours_day,0)+coalesce(v_component.hours_night,0)
          +coalesce(v_component.hours_sat,0)+coalesce(v_component.hours_sun,0)+coalesce(v_component.hours_bh,0);
        if not found or v_line.source_component_id is distinct from v_component.component_id
           or v_line.component_kind is distinct from 'WORK'
           or v_line.evidence_ref is distinct from 'source-component:'||v_component.component_id::text
             ||'#'||pg_catalog.encode(v_component_sha,'hex')
           or v_line.source_pay_ex_vat is distinct from
             (case when v_component.exclude_from_pay then 0 else v_component.pay_ex_vat end)
           or v_line.work_date is distinct from v_component.work_date
           or v_line.approved_quantity is distinct from v_hours then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_LINE_NOT_EXACT';
        end if;
        v_rates:='{}'::jsonb; v_expected_rates:=0;
        for v_bucket in select * from (values
          ('DAY'::text,'day'::text,coalesce(v_component.hours_day,0)),
          ('NIGHT','night',coalesce(v_component.hours_night,0)),
          ('SAT','sat',coalesce(v_component.hours_sat,0)),
          ('SUN','sun',coalesce(v_component.hours_sun,0)),
          ('BH','bh',coalesce(v_component.hours_bh,0))) b(code,key,hours)
        loop
          select rd.approved_hours,rd.source_pay_rate into v_stored_hours,v_rate
            from private.bpay_next_rate_detail rd
            where rd.approved_line_id=v_line.id and rd.bucket=v_bucket.code;
          if coalesce(v_stored_hours,0) is distinct from v_bucket.hours
             or (v_bucket.hours<>0 and not found)
             or (v_bucket.hours=0 and found) then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_BUCKET_NOT_EXACT';
          end if;
          if v_bucket.hours<>0 then
            v_expected_rates:=v_expected_rates+1;
            v_rates:=v_rates||pg_catalog.jsonb_build_object(v_bucket.key,v_rate);
          end if;
        end loop;
        select pg_catalog.count(*)::integer into v_actual_rates from private.bpay_next_rate_detail rd
          where rd.approved_line_id=v_line.id;
        if v_actual_rates<>v_expected_rates or v_line.expected_rate_detail_count<>v_expected_rates then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_BUCKET_NOT_EXACT';
        end if;
        select pg_catalog.count(*)::integer into v_seen from private.bpay_next_shift_detail s
          where s.approved_line_id=v_line.id;
        if v_seen<>1 then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_SHIFT_NOT_EXACT';
        end if;
        select * into strict v_shift from private.bpay_next_shift_detail s where s.approved_line_id=v_line.id;
        if v_shift.work_date is distinct from v_component.work_date
           or v_shift.approved_hours is distinct from v_hours then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_FROZEN_SHIFT_NOT_EXACT';
        end if;
        v_break_total:=0; v_break_no:=0; v_all_clocked:=true; v_breaks:='[]'::jsonb;
        for v_row in select b.* from private.bpay_next_break_detail b
          where b.shift_detail_id=v_shift.id order by b.break_no
        loop
          v_break_no:=v_break_no+1;
          if v_row.break_no<>v_break_no or v_row.break_minutes<0 then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
          end if;
          if v_row.break_start_local is not null then
            v_break_minutes:=((pg_catalog.substring(v_row.break_end_local,1,2)::integer*60
              +pg_catalog.substring(v_row.break_end_local,4,2)::integer)
              -(pg_catalog.substring(v_row.break_start_local,1,2)::integer*60
              +pg_catalog.substring(v_row.break_start_local,4,2)::integer)+1440)%1440;
            if v_row.break_end_local is null or v_break_minutes=0
               or v_break_minutes<>v_row.break_minutes then
              raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
            end if;
          else v_all_clocked:=false;
          end if;
          v_break_total:=v_break_total+v_row.break_minutes;
          v_breaks:=v_breaks||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'start',v_row.break_start_local,'end',v_row.break_end_local));
        end loop;
        select * into strict v_revision from private.bpay_next_work_revision where id=v_before_revisions[v_i];
        select * into strict v_work from private.bpay_next_work where id=v_revision.work_id;
        v_detail:=pg_catalog.jsonb_build_object(
          'from_frozen_pair',true,'segment_key',v_shift.segment_key,'prior_approved_line_id',v_line.id,
          'prior_revision_id',v_revision.id,'prior_work_id',v_work.id,
          'prior_source_event_id',v_revision.source_event_id,
          'prior_financial_snapshot_id',v_revision.financial_snapshot_id,
          'prior_root_timesheet_id',v_revision.physical_timesheet_id,
          'prior_root_version',v_revision.physical_timesheet_version,
          'prior_family_booking_id',v_work.booking_id,
          'source_component_id',v_component.component_id,
          'source_component_sha256',pg_catalog.encode(v_component_sha,'hex'),
          'work_date',pg_catalog.to_char(v_shift.work_date,'YYYY-MM-DD'),'start',v_shift.shift_start_local,
          'end',v_shift.shift_end_local,'overnight',v_shift.shift_overnight,
          'break_minutes',v_break_total,'rates',v_rates);
        if v_all_clocked then v_detail:=v_detail||pg_catalog.jsonb_build_object('breaks',v_breaks); end if;
        if (v_shift.shift_start_at is null)<>(v_shift.shift_end_at is null)
           or (v_shift.shift_start_at is not null and (
             not pg_catalog.isfinite(v_shift.shift_start_at) or not pg_catalog.isfinite(v_shift.shift_end_at)
             or v_shift.shift_end_at<=v_shift.shift_start_at)) then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_UTC_INVALID';
        end if;
        if v_shift.shift_start_at is not null then
          v_detail:=v_detail||pg_catalog.jsonb_build_object(
            'start_utc',pg_catalog.to_char(v_shift.shift_start_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'end_utc',pg_catalog.to_char(v_shift.shift_end_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
        end if;
      else
        -- Reviewed, not-yet-authorised B: ordinary segment identity is real;
        -- no Final/movement/work-event/Bank approval identity is manufactured.
        select pg_catalog.count(*)::integer,pg_catalog.jsonb_agg(s.value)->0 into v_seen,v_segment
          from pg_catalog.jsonb_array_elements(v_tf.invoice_breakdown_json->'segments') s(value)
          where s.value->>'segment_id'=v_component.segment_id;
        if v_i<>2 or p_before_source_event_ids[v_i] is not null or v_seen<>1 then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_TARGET_SEGMENT_NOT_EXACT';
        end if;
        if (v_segment->>'start_local' is not null and v_segment->>'start' is not null
              and v_segment->>'start_local' is distinct from v_segment->>'start')
           or (v_segment->>'end_local' is not null and v_segment->>'end' is not null
              and v_segment->>'end_local' is distinct from v_segment->>'end') then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_CLOCK_ALIAS_CONFLICT';
        end if;
        v_start:=nullif(pg_catalog.btrim(coalesce(v_segment->>'start_local',v_segment->>'start','')),'');
        v_end:=nullif(pg_catalog.btrim(coalesce(v_segment->>'end_local',v_segment->>'end','')),'');
        v_rates:='{}'::jsonb;
        for v_bucket in select * from (values
          ('day'::text,v_tf.pay_day),('night',v_tf.pay_night),('sat',v_tf.pay_sat),
          ('sun',v_tf.pay_sun),('bh',v_tf.pay_bh)) b(key,rate)
        loop
          if pg_catalog.jsonb_typeof(v_segment->'weekly_source')='object' then
            v_rate:=(v_segment#>>array['weekly_source','pay_vector','rates',v_bucket.key])::numeric;
          else v_rate:=v_bucket.rate;
          end if;
          v_rates:=v_rates||pg_catalog.jsonb_build_object(v_bucket.key,v_rate);
        end loop;
        v_break_total:=0; v_break_no:=0; v_breaks:='[]'::jsonb;
        if v_segment ? 'breaks' and pg_catalog.jsonb_typeof(v_segment->'breaks') not in ('array','null') then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
        end if;
        for v_break in select value from pg_catalog.jsonb_array_elements(
          case when pg_catalog.jsonb_typeof(v_segment->'breaks')='array' then v_segment->'breaks' else '[]'::jsonb end)
        loop
          if pg_catalog.jsonb_typeof(v_break) is distinct from 'object'
             or coalesce(v_break->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
             or coalesce(v_break->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
          end if;
          v_break_minutes:=((pg_catalog.substring(v_break->>'end',1,2)::integer*60
            +pg_catalog.substring(v_break->>'end',4,2)::integer)
            -(pg_catalog.substring(v_break->>'start',1,2)::integer*60
            +pg_catalog.substring(v_break->>'start',4,2)::integer)+1440)%1440;
          if v_break_minutes=0 then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
          end if;
          v_break_no:=v_break_no+1; v_break_total:=v_break_total+v_break_minutes;
          v_breaks:=v_breaks||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'start',v_break->>'start','end',v_break->>'end'));
        end loop;
        if v_segment ? 'break_mins' and v_segment ? 'break_minutes'
           and (v_segment->>'break_mins')::integer is distinct from (v_segment->>'break_minutes')::integer then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
        end if;
        if v_break_no=0 then
          v_break_total:=coalesce((v_segment->>'break_mins')::integer,(v_segment->>'break_minutes')::integer,0);
        elsif (v_segment ? 'break_mins' or v_segment ? 'break_minutes') and v_break_total is distinct from
          coalesce((v_segment->>'break_mins')::integer,(v_segment->>'break_minutes')::integer) then
          raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_BREAK_NOT_EXACT';
        end if;
        v_detail:=pg_catalog.jsonb_build_object(
          'from_reviewed_target_tsfin',true,'before_financial_snapshot_id',v_tf.id,
          'before_root_timesheet_id',v_tf.timesheet_id,'before_root_version',v_tf.timesheet_version,
          'before_segment_id',v_component.segment_id,'source_component_id',v_component.component_id,
          'source_component_sha256',pg_catalog.encode(v_component_sha,'hex'),
          'decision_bundle_id',p_decision_bundle_id,'bundle_revision',p_bundle_revision,
          'snapshot_detail_sha256',pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            pg_catalog.jsonb_build_object('segments',v_tf.invoice_breakdown_json->'segments',
              'actual_schedule',v_tf.actual_schedule_json,'rate_source_refs',v_tf.rate_source_refs_json)::text,'UTF8')),'hex'),
          'work_date',v_segment->>'date','start',v_start,'end',v_end,
          'overnight',(v_segment->>'overnight')::boolean,'break_minutes',v_break_total,'rates',v_rates);
        if v_break_no>0 then v_detail:=v_detail||pg_catalog.jsonb_build_object('breaks',v_breaks); end if;
        if (v_segment->'start_utc' is not null and v_segment->'start_utc'<>'null'::jsonb)
           or (v_segment->'end_utc' is not null and v_segment->'end_utc'<>'null'::jsonb) then
          if pg_catalog.jsonb_typeof(v_segment->'start_utc') is distinct from 'string'
             or pg_catalog.jsonb_typeof(v_segment->'end_utc') is distinct from 'string'
             or coalesce(v_segment->>'start_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$'
             or coalesce(v_segment->>'end_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$' then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_UTC_INVALID';
          end if;
          begin
            v_start_utc:=(v_segment->>'start_utc')::timestamptz;
            v_end_utc:=(v_segment->>'end_utc')::timestamptz;
          exception when invalid_datetime_format or datetime_field_overflow or invalid_time_zone_displacement_value then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_UTC_INVALID';
          end;
          if not pg_catalog.isfinite(v_start_utc) or not pg_catalog.isfinite(v_end_utc)
             or v_end_utc<=v_start_utc
             or pg_catalog.to_char(v_start_utc at time zone 'Europe/London','YYYY-MM-DD')<>v_segment->>'date'
             or pg_catalog.to_char(v_start_utc at time zone 'Europe/London','HH24:MI')<>v_start
             or pg_catalog.to_char(v_end_utc at time zone 'Europe/London','HH24:MI')<>v_end
             or (v_end_utc at time zone 'Europe/London')::date<>(v_segment->>'date')::date
                +(case when (v_segment->>'overnight')::boolean then 1 else 0 end) then
            raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_UTC_INVALID';
          end if;
          v_detail:=v_detail||pg_catalog.jsonb_build_object('start_utc',v_segment->'start_utc','end_utc',v_segment->'end_utc');
        end if;
      end if;
    end if;
    -- Shared presentation qualification does not turn clocks into money.
    if pg_catalog.jsonb_typeof(v_detail) is distinct from 'object'
       or v_detail->>'work_date' is distinct from pg_catalog.to_char(v_component.work_date,'YYYY-MM-DD')
       or coalesce(v_detail->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
       or coalesce(v_detail->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
       or pg_catalog.jsonb_typeof(v_detail->'overnight') is distinct from 'boolean'
       or pg_catalog.jsonb_typeof(v_detail->'rates') is distinct from 'object'
       or coalesce(v_detail->>'break_minutes','') !~ '^[0-9]+$'
       or pg_catalog.octet_length(v_detail::text)>4096 then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_PRESENTATION_NOT_EXACT';
    end if;
    for v_bucket in select * from (values
      ('day'::text,coalesce(v_component.hours_day,0)),('night',coalesce(v_component.hours_night,0)),
      ('sat',coalesce(v_component.hours_sat,0)),('sun',coalesce(v_component.hours_sun,0)),
      ('bh',coalesce(v_component.hours_bh,0))) b(key,hours)
    loop
      v_rate:=(v_detail->'rates'->>v_bucket.key)::numeric;
      if (v_bucket.hours<>0 and not v_component.exclude_from_pay and v_rate is null)
         or (v_rate is not null and (v_rate::text in ('NaN','Infinity','-Infinity')
           or v_rate<>pg_catalog.round(v_rate,6) or pg_catalog.abs(v_rate)>=1000000000000)) then
        raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_USED_RATE_NOT_EXACT';
      end if;
    end loop;
    v_detail_sha:=pg_catalog.sha256(pg_catalog.convert_to(v_detail::text,'UTF8'));
    insert into private.bpay_next_source_chosen_detail
      (head_id,component_id,decision_bundle_id,bundle_revision,component_sha256,detail_json,detail_sha256)
      values(v_component.head_id,v_component.component_id,p_decision_bundle_id,p_bundle_revision,
        v_component_sha,v_detail,v_detail_sha)
      on conflict(head_id,component_id) do nothing;
    select * into strict v_existing from private.bpay_next_source_chosen_detail d
      where d.head_id=v_component.head_id and d.component_id=v_component.component_id;
    if v_existing.decision_bundle_id is distinct from p_decision_bundle_id
       or v_existing.bundle_revision is distinct from p_bundle_revision
       or v_existing.component_sha256 is distinct from v_component_sha
       or v_existing.detail_json is distinct from v_detail
       or v_existing.detail_sha256 is distinct from v_detail_sha then
      raise exception using errcode='23514',message='BPAY_NEXT_PAIR_DETAIL_REPLAY_CONFLICT';
    end if;
    v_count:=v_count+1;
  end loop;
  return v_count;
end
$function$;

alter function private.bpay_next_capture_source_pair_before_detail_v1(uuid,bigint,uuid[],uuid[]) owner to postgres;
revoke all on function private.bpay_next_capture_source_pair_before_detail_v1(uuid,bigint,uuid[],uuid[]) from public,anon,authenticated,service_role;
-- Named stable detail producer successor: supabase/repeatable/04102026_0345_banking_pay_next_protected_source_v1.sql
create or replace function private.bpay_next_stage_protected_source_v1(
  p_approval_id uuid,p_generation_id uuid,p_root_timesheet_id uuid,p_target_components jsonb
) returns table(work_id uuid,revision_id uuid,source_event_id uuid)
language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_ts public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_existing private.bpay_next_work_revision%rowtype;
  v_revision uuid;
  v_component jsonb;
  v_component_id uuid;
  v_line uuid;
  v_shift uuid;
  v_n integer:=0;
  v_rate_n integer;
  v_hours numeric;
  v_amount numeric;
  v_candidate_name text;
  v_candidate_reference text;
  v_client_name text;
  v_schedule_count integer;
  v_bucket text;
  v_break jsonb;
  v_break_n integer;
begin
  select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a where a.id=p_approval_id;
  select g.* into strict v_generation from public.weekly_exceptional_pay_generations g
    where g.id=p_generation_id and g.family_id=v_approval.pay_target_family_id;
  if v_generation.complete_next_vector_json->'components' is distinct from p_target_components
     or pg_catalog.jsonb_typeof(p_target_components)<>'array'
     or pg_catalog.jsonb_array_length(p_target_components)>100 then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_TARGET_NOT_APPROVED';
  end if;
  select r.* into v_existing from private.bpay_next_work_revision r where r.source_event_id=p_approval_id;
  if found then
    if v_existing.source_kind<>'PROTECTED' or v_existing.physical_timesheet_id<>p_root_timesheet_id
       or v_existing.source_head_id is not null or v_existing.financial_snapshot_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_REVISION_REPLAY_CONFLICT';
    end if;
    work_id:=v_existing.work_id; revision_id:=v_existing.id; source_event_id:=p_approval_id;
    return next; return;
  end if;
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE'; end if;
  select t.* into strict v_ts from public.timesheets t where t.timesheet_id=p_root_timesheet_id for key share;
  select c.* into strict v_contract from public.contracts c where c.id=v_ts.contract_id for key share;
  if private.bpay_next_approval_route_v1(v_ts.timesheet_id)<>'SOURCE_HOURS'
     or v_ts.contract_id<>v_approval.contract_id or v_contract.candidate_id<>v_approval.candidate_id
     or v_ts.week_ending_date<>v_approval.week_ending or v_ts.is_current is distinct from true
     or v_ts.revoked_at is not null or v_ts.archived_at_utc is not null or v_ts.is_adjustment
     or upper(v_contract.pay_method_snapshot) not in ('PAYE','UMBRELLA') then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_PHYSICAL_SCOPE_INVALID';
  end if;
  -- Immutable revision replay above precedes these fresh owner/auth gates.
  if private.bpay_next_protected_owner_route_v1(v_approval.pay_target_family_id)<>'NEXT' then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_LOCAL_OWNER_REQUIRED';
  end if;
  perform private.bpay_next_protected_require_authorised_root_v1(v_ts.timesheet_id);
  insert into private.bpay_next_worker_control(candidate_id) values(v_approval.candidate_id) on conflict do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_approval.candidate_id for update;
  select w.* into v_work from private.bpay_next_work w where w.booking_id=v_ts.booking_id for update;
  if not found then
    insert into private.bpay_next_work(candidate_id,contract_id,original_timesheet_id,booking_id,work_kind,week_ending_date)
      values(v_approval.candidate_id,v_approval.contract_id,v_ts.timesheet_id,v_ts.booking_id,'SOURCE',v_ts.week_ending_date)
      returning * into v_work;
  elsif v_work.candidate_id<>v_approval.candidate_id or v_work.contract_id<>v_approval.contract_id
      or v_work.work_kind<>'SOURCE' or v_work.week_ending_date<>v_ts.week_ending_date then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_WORK_IDENTITY_CONFLICT';
  end if;
  select coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),c.id::text),c.tms_ref
    into strict v_candidate_name,v_candidate_reference from public.candidates c where c.id=v_approval.candidate_id;
  select c.name into strict v_client_name from public.clients c where c.id=v_approval.client_id;
  select pg_catalog.count(*)::integer into v_schedule_count from private.bpay_next_contract_rate_rows_v1(
    v_contract.rates_json,v_contract.bucket_labels_json,v_contract.additional_rates_json,
    v_contract.mileage_pay_rate,v_contract.mileage_charge_rate);
  insert into private.bpay_next_work_revision(work_id,revision_no,source_kind,source_head_id,source_event_id,
    physical_timesheet_id,financial_snapshot_id,physical_timesheet_version,source_pay_channel,week_ending_date,
    detail_kind,expected_line_count,expected_rate_schedule_count,certified_zero,approved_source_ex_vat,
    candidate_display_name,candidate_reference,client_display_name,job_title,band_label,timesheet_reference)
  values(v_work.id,v_work.current_revision_no+1,'PROTECTED',null,p_approval_id,v_ts.timesheet_id,null,v_ts.version,
    upper(v_contract.pay_method_snapshot),v_ts.week_ending_date,
    case when exists(select 1 from pg_catalog.jsonb_array_elements(p_target_components) x(value)
      where x.value->>'kind'='WORK') then 'SHIFT' else 'FIXED' end,
    pg_catalog.jsonb_array_length(p_target_components),v_schedule_count,v_approval.approved_target_gross=0,
    v_approval.approved_target_gross,v_candidate_name,v_candidate_reference,v_client_name,
    v_contract.role,v_contract.band,v_ts.reference_number) returning id into v_revision;
  insert into private.bpay_next_rate_schedule(revision_id,rate_family,rate_code,unit_label,paye_rate,umbrella_rate,charge_rate)
    select v_revision,r.rate_family,r.rate_code,r.unit_label,r.paye_rate,r.umbrella_rate,r.charge_rate
    from private.bpay_next_contract_rate_rows_v1(v_contract.rates_json,v_contract.bucket_labels_json,
      v_contract.additional_rates_json,v_contract.mileage_pay_rate,v_contract.mileage_charge_rate) r;
  for v_component in select value from pg_catalog.jsonb_array_elements(p_target_components)
  loop
    v_n:=v_n+1; v_hours:=0; v_rate_n:=0;
    if v_component->>'kind'='WORK' then
      v_component_id:=private.weekly_source_entitlement_component_id_v1('WORKED_TIME','SEGMENT',
        'weekly-source-event:'||(v_component->>'work_event_id'),v_component->>'work_event_id');
      foreach v_bucket in array array['day','night','sat','sun','bh'] loop
        v_hours:=v_hours+(v_component#>>array['hours',v_bucket])::numeric;
        if (v_component#>>array['hours',v_bucket])::numeric<>0 then v_rate_n:=v_rate_n+1; end if;
      end loop;
    else
      v_component_id:=private.weekly_source_entitlement_component_id_v1('SOURCE_FIXED_EXPENSE','EXPENSE_AUTHORITY',
        'weekly-source-expense:'||(v_component->>'work_event_id'),v_component->>'work_event_id');
    end if;
    v_amount:=(v_component->>'pay_ex_vat')::numeric;
    insert into private.bpay_next_approved_line(revision_id,line_no,component_key,source_component_id,component_kind,
      work_date,approved_quantity,expected_rate_detail_count,source_pay_ex_vat,evidence_ref)
    values(v_revision,v_n,'SOURCE:'||v_component_id::text,v_component_id,
      case when v_component->>'kind'='EXPENSE' then 'EXPENSE'
        when (v_component->>'protected')::boolean then 'PROTECTED_WORK' else 'WORK' end,
      nullif(v_component->>'date','')::date,case when v_component->>'kind'='WORK' then v_hours else null end,
      v_rate_n,v_amount,'protected-approval:'||p_approval_id::text||'#'||p_generation_id::text)
    returning id into v_line;
    if v_component->>'kind'='WORK' then
      insert into private.bpay_next_shift_detail(approved_line_id,detail_no,work_date,
        shift_start_at,shift_end_at,shift_start_local,shift_end_local,shift_overnight,
        approved_hours,detail_label,immutable_evidence_ref,segment_key,segment_pay_ex_vat,pay_excluded,
        hours_day,hours_night,hours_sat,hours_sun,hours_bh)
      values(v_line,1,(v_component->>'date')::date,nullif(v_component->>'start_at_utc','')::timestamptz,
        nullif(v_component->>'end_at_utc','')::timestamptz,v_component->>'start',v_component->>'end',
        (v_component->>'overnight')::boolean,v_hours,'weekly-source-event:'||(v_component->>'work_event_id'),
        'protected-approval:'||p_approval_id::text,(v_component->>'work_event_id')::uuid::text,v_amount,false,
        (v_component#>>'{hours,day}')::numeric,(v_component#>>'{hours,night}')::numeric,
        (v_component#>>'{hours,sat}')::numeric,(v_component#>>'{hours,sun}')::numeric,
        (v_component#>>'{hours,bh}')::numeric) returning id into v_shift;
      foreach v_bucket in array array['day','night','sat','sun','bh'] loop
        if (v_component#>>array['hours',v_bucket])::numeric<>0 then
          insert into private.bpay_next_rate_detail(approved_line_id,bucket,approved_hours,source_pay_rate)
          values(v_line,upper(v_bucket),(v_component#>>array['hours',v_bucket])::numeric,
            (v_component#>>array['rates',v_bucket])::numeric);
        end if;
      end loop;
      v_break_n:=0;
      for v_break in select value from pg_catalog.jsonb_array_elements(v_component->'breaks') loop
        v_break_n:=v_break_n+1;
        insert into private.bpay_next_break_detail(shift_detail_id,break_no,break_start_local,break_end_local,break_minutes)
          values(v_shift,v_break_n,v_break->>'start',v_break->>'end',(v_break->>'minutes')::integer);
      end loop;
      if v_break_n=0 and (v_component->>'break_minutes')::integer>0 then
        insert into private.bpay_next_break_detail(shift_detail_id,break_no,break_minutes)
          values(v_shift,1,(v_component->>'break_minutes')::integer);
      end if;
    end if;
  end loop;
  work_id:=v_work.id; revision_id:=v_revision; source_event_id:=p_approval_id; return next;
end
$function$;

alter function private.bpay_next_stage_protected_source_v1(uuid,uuid,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_stage_protected_source_v1(uuid,uuid,uuid,jsonb) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_work_segment_gross_balance_v1(
  p_work_id uuid,p_component_key text,p_segment_key text)
returns table(realised_source_ex_vat numeric,active_held_source_ex_vat numeric)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select coalesce((select sum(e.source_disposition_ex_vat) from private.bpay_next_work_segment_effect_share e
      where e.work_id=p_work_id and e.component_key=p_component_key and e.segment_key=p_segment_key),0),
    coalesce((select sum(s.source_reserved_ex_vat) from private.bpay_next_work_segment_origin_share s
      join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
      where s.work_id=p_work_id and s.component_key=p_component_key and s.segment_key=p_segment_key),0)
$function$;
alter function private.bpay_next_work_segment_gross_balance_v1(uuid,text,text) owner to postgres;
revoke all on function private.bpay_next_work_segment_gross_balance_v1(uuid,text,text) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_record_work_segment_origin_shares_v1(p_run_line_id uuid,p_hold_id uuid)
returns void language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare
  v_line private.bpay_next_run_line%rowtype; v_hold private.bpay_next_hold%rowtype;
  v_position private.bpay_next_run_position%rowtype; v_worker private.bpay_next_run_worker%rowtype;
  v_run private.bpay_next_pay_run%rowtype; v_total record; v_expected bigint;
begin
  select l.* into strict v_line from private.bpay_next_run_line l where l.id=p_run_line_id;
  select h.* into strict v_hold from private.bpay_next_hold h where h.id=p_hold_id;
  select p.* into strict v_position from private.bpay_next_run_position p
    where p.run_work_id=v_line.run_work_id and p.component_key=v_line.component_key;
  if (v_hold.run_line_id,v_hold.work_id,v_hold.component_key,v_hold.source_reserved_ex_vat,
      v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat,v_hold.status)
    is distinct from (v_line.id,v_line.work_id,v_line.component_key,v_line.source_consumed_ex_vat,
      v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat,'ACTIVE'::text) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_HOLD_PARENT_MISMATCH';
  end if;
  if v_position.segment_member_count=0 then return; end if;
  if not v_position.segment_attribution_complete or v_position.segment_included_positive_ex_vat<>v_hold.source_reserved_ex_vat then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_HOLD_CAPTURE_INCOMPLETE';
  end if;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_line.run_worker_id;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_worker.run_id;
  -- One completed captured parent; no live approval/history or NET allocation.
  -- Shared gross valuation of cumulative prefixes conserves the parent's pennies.
  with members as materialized (
    select p.*,sum(p.included_positive_source_ex_vat) over(order by p.segment_key) cumulative_source
    from private.bpay_next_run_work_segment_parent p where p.run_work_id=v_line.run_work_id
      and p.component_key=v_line.component_key and p.included_positive_source_ex_vat>0
  ), quoted as materialized (
    select p.*,q.target_ex_vat,q.target_vat,q.target_inc_vat from members p
    cross join lateral private.bpay_next_value_source_for_target_v1(p.cumulative_source,v_line.source_pay_channel,
      v_line.target_pay_channel,case when v_line.target_pay_channel='UMBRELLA'
        then v_worker.target_umbrella_vat_chargeable else false end,v_line.valuation_policy_id,v_run.pay_date) q
  ), shares as (
    select p.*,p.target_ex_vat-coalesce(lag(p.target_ex_vat) over(order by p.segment_key),0) share_ex,
      p.target_vat-coalesce(lag(p.target_vat) over(order by p.segment_key),0) share_vat
    from quoted p
  )
  insert into private.bpay_next_work_segment_origin_share(hold_id,run_line_id,run_work_segment_id,
    work_id,component_key,segment_key,source_reserved_ex_vat,target_reserved_ex_vat,target_reserved_vat,target_reserved_inc_vat)
  select v_hold.id,v_line.id,p.run_work_segment_id,v_line.work_id,v_line.component_key,p.segment_key,
    p.included_positive_source_ex_vat,p.share_ex,p.share_vat,p.share_ex+p.share_vat from shares p;
  select count(*),sum(s.source_reserved_ex_vat) source_total,sum(s.target_reserved_ex_vat) target_ex,
    sum(s.target_reserved_vat) target_vat,sum(s.target_reserved_inc_vat) target_inc into v_total
    from private.bpay_next_work_segment_origin_share s where s.hold_id=v_hold.id;
  select count(*) into v_expected from private.bpay_next_run_work_segment_parent p
    where p.run_work_id=v_line.run_work_id and p.component_key=v_line.component_key and p.included_positive_source_ex_vat>0;
  if (v_total.count,v_total.source_total,v_total.target_ex,v_total.target_vat,v_total.target_inc)
    is distinct from (v_expected,v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_HOLD_SHARE_TOTAL_MISMATCH';
  end if;
end
$function$;
alter function private.bpay_next_record_work_segment_origin_shares_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_record_work_segment_origin_shares_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- The actual PAYROLL_SETTLED insertion covers BANK and INTERNAL_ZERO.
-- Attach immutable shares at that exact native insertion, never on return/reissue.
create or replace function private.bpay_next_segment_payroll_effect_membership_v1()
returns trigger language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare v_hold private.bpay_next_hold%rowtype; v_line private.bpay_next_run_line%rowtype; v_total record;
begin
  if new.effect_kind<>'PAYROLL_SETTLED' then return new; end if;
  select h.* into strict v_hold from private.bpay_next_hold h where h.id=new.operation_item_id;
  select l.* into strict v_line from private.bpay_next_run_line l where l.id=v_hold.run_line_id;
  if not exists(select 1 from private.bpay_next_run_work_segment_parent p
    where p.run_work_id=v_line.run_work_id and p.component_key=v_line.component_key) then return new; end if;
  if (new.work_id,new.component_key,new.source_disposition_ex_vat,new.target_amount_ex_vat,
      new.target_amount_vat,new.target_amount_inc_vat)
    is distinct from (v_hold.work_id,v_hold.component_key,v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,
      v_hold.target_amount_vat,v_hold.target_amount_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_EFFECT_PARENT_MISMATCH';
  end if;
  insert into private.bpay_next_work_segment_effect_share(effect_id,work_id,component_key,segment_key,
    source_disposition_ex_vat,target_ex_vat,target_vat,target_inc_vat)
  select new.id,s.work_id,s.component_key,s.segment_key,s.source_reserved_ex_vat,
    s.target_reserved_ex_vat,s.target_reserved_vat,s.target_reserved_inc_vat
    from private.bpay_next_work_segment_origin_share s where s.hold_id=v_hold.id;
  insert into private.bpay_next_work_segment_effect_origin(effect_share_id,origin_share_id,source_disposition_ex_vat)
  select e.id,s.id,s.source_reserved_ex_vat from private.bpay_next_work_segment_effect_share e
    join private.bpay_next_work_segment_origin_share s on s.hold_id=v_hold.id and s.segment_key=e.segment_key
    where e.effect_id=new.id;
  select sum(e.source_disposition_ex_vat) source_total,sum(e.target_ex_vat) target_ex,
    sum(e.target_vat) target_vat,sum(e.target_inc_vat) target_inc into v_total
    from private.bpay_next_work_segment_effect_share e where e.effect_id=new.id;
  if (v_total.source_total,v_total.target_ex,v_total.target_vat,v_total.target_inc)
    is distinct from (new.source_disposition_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_EFFECT_SHARE_TOTAL_MISMATCH';
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_payroll_effect_membership_v1() owner to postgres;
revoke all on function private.bpay_next_segment_payroll_effect_membership_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_payroll_effect_membership on private.bpay_next_financial_effect;
create trigger bpay_next_segment_payroll_effect_membership after insert on private.bpay_next_financial_effect
  for each row execute function private.bpay_next_segment_payroll_effect_membership_v1();

-- Current native parent membership, plus original parents of the exact selected
-- stable segments. SUBSET never sweeps the Candidate's other work/history.
create or replace function private.bpay_next_selected_position_keys_v1(p_run_worker_id uuid,p_work_cursor uuid,p_component_cursor text,p_limit integer)
returns table(run_work_id uuid,work_id uuid,captured_revision_id uuid,component_key text,selection_mode text)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
  with all_page as materialized (
    select rw.id,rw.work_id,rw.captured_revision_id,p.component_key,'ALL'::text mode
    from private.bpay_next_run_work rw join private.bpay_next_run_worker w on w.id=rw.run_worker_id
    join private.bpay_next_work_choice c on c.run_id=w.run_id and c.work_id=rw.work_id
      and c.expected_revision_id=rw.captured_revision_id and c.selection_mode='ALL' and c.selection_state='SEALED'
    join private.bpay_next_position p on p.work_id=rw.work_id and p.applied_revision_id=rw.captured_revision_id
    where rw.run_worker_id=p_run_worker_id and (p_work_cursor is null or (rw.work_id,p.component_key)>(p_work_cursor,p_component_cursor))
    order by rw.work_id,p.component_key limit p_limit
  ), selected_page as materialized (
    select rw.id,rw.work_id,rw.captured_revision_id,s.component_key,'SUBSET'::text mode
    from private.bpay_next_run_work rw join private.bpay_next_run_worker w on w.id=rw.run_worker_id
    join private.bpay_next_work_choice c on c.run_id=w.run_id and c.work_id=rw.work_id
      and c.expected_revision_id=rw.captured_revision_id and c.selection_mode='SUBSET' and c.selection_state='SEALED'
    join private.bpay_next_selected_component s on s.run_id=w.run_id and s.work_id=rw.work_id and s.expected_revision_id=rw.captured_revision_id
    where rw.run_worker_id=p_run_worker_id and (p_work_cursor is null or (rw.work_id,s.component_key)>(p_work_cursor,p_component_cursor))
    order by rw.work_id,s.component_key limit p_limit
  ), origin_page as materialized (
    select distinct rw.id,rw.work_id,rw.captured_revision_id,o.component_key,'ORIGIN'::text mode
    from private.bpay_next_run_work rw join private.bpay_next_run_worker w on w.id=rw.run_worker_id
    join private.bpay_next_work_choice c on c.run_id=w.run_id and c.work_id=rw.work_id
      and c.expected_revision_id=rw.captured_revision_id and c.selection_mode='SUBSET' and c.selection_state='SEALED'
    join private.bpay_next_selected_segment s on s.run_id=w.run_id and s.work_id=rw.work_id and s.expected_revision_id=rw.captured_revision_id
    join private.bpay_next_work_segment_origin_share o on o.work_id=rw.work_id and o.segment_key=s.segment_key
    where rw.run_worker_id=p_run_worker_id and (p_work_cursor is null or (rw.work_id,o.component_key)>(p_work_cursor,p_component_cursor))
      and not exists(select 1 from private.bpay_next_selected_component a where a.run_id=w.run_id and a.work_id=rw.work_id and a.component_key=o.component_key)
    order by rw.work_id,o.component_key,rw.id,rw.captured_revision_id limit p_limit
  ) select * from (select * from all_page union all select * from selected_page union all select * from origin_page) page
    order by work_id,component_key limit p_limit
$function$;alter function private.bpay_next_selected_position_keys_v1(uuid,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_selected_position_keys_v1(uuid,uuid,text,integer) from public,anon,authenticated,service_role;

-- Complete attribution identities for each captured real parent. An unselected
-- sibling needed for conservation stays NOT_SELECTED; it never becomes payable.
create or replace function private.bpay_next_run_segment_members_v1(p_run_worker_id uuid,p_work_cursor uuid,p_component_cursor text,p_segment_cursor text,p_limit integer)
returns table(run_work_id uuid,work_id uuid,captured_revision_id uuid,component_key text,segment_key text)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
  with approved_page as materialized (
    select p.run_work_id,p.work_id,p.captured_revision_id,p.component_key,s.segment_key
    from private.bpay_next_run_position p join private.bpay_next_approved_segment s on s.revision_id=p.captured_revision_id and s.component_key=p.component_key
    where p.run_worker_id=p_run_worker_id and (p_work_cursor is null or (p.work_id,p.component_key,s.segment_key)>(p_work_cursor,p_component_cursor,p_segment_cursor))
    order by p.work_id,p.component_key,s.segment_key limit p_limit
  ), origin_page as materialized (
    select distinct p.run_work_id,p.work_id,p.captured_revision_id,p.component_key,s.segment_key
    from private.bpay_next_run_position p join private.bpay_next_work_segment_origin_share s on s.work_id=p.work_id and s.component_key=p.component_key
    where p.run_worker_id=p_run_worker_id and (p_work_cursor is null or (p.work_id,p.component_key,s.segment_key)>(p_work_cursor,p_component_cursor,p_segment_cursor))
      and not exists(select 1 from private.bpay_next_approved_segment a where a.revision_id=p.captured_revision_id and a.component_key=p.component_key and a.segment_key=s.segment_key)
    order by p.work_id,p.component_key,s.segment_key,p.run_work_id,p.captured_revision_id limit p_limit
  ) select * from (select * from approved_page union all select * from origin_page) page order by work_id,component_key,segment_key limit p_limit
$function$;alter function private.bpay_next_run_segment_members_v1(uuid,uuid,text,text,integer) owner to postgres;
revoke all on function private.bpay_next_run_segment_members_v1(uuid,uuid,text,text,integer) from public,anon,authenticated,service_role;

-- One bounded subordinate page per real RESERVE call. Parent gross stays intact.
create or replace function private.bpay_next_capture_segment_page_v1(p_run_worker_id uuid,p_limit integer)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare
  v_state private.bpay_next_run_segment_capture%rowtype;
  v_member record; v_current private.bpay_next_approved_segment%rowtype;
  v_parent private.bpay_next_run_position%rowtype; v_selected private.bpay_next_selected_segment%rowtype;
  v_worker private.bpay_next_run_worker%rowtype; v_segment_id uuid; v_disposition text;
  v_balance record; v_totals record;v_target_totals record;v_collection private.bpay_next_work_collection%rowtype;
  v_needs_resolution boolean;v_seen integer:=0; v_a numeric;v_p numeric;v_r numeric;v_h numeric;
begin
  if p_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_SEGMENT_CAPTURE_LIMIT_INVALID';end if;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=p_run_worker_id for update;
  if not exists(select 1 from private.bpay_next_job j join private.bpay_next_run_command rc on rc.command_id=j.command_id
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    where rc.run_id=v_worker.run_id and j.candidate_id=v_worker.candidate_id and j.job_kind='PREPARE'
      and j.phase='RESERVE' and j.status='LEASED' and j.lease_until_utc>clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_CAPTURE_OWNER_STALE';
  end if;
  select s.* into v_state from private.bpay_next_run_segment_capture s where s.run_worker_id=p_run_worker_id for update;
  if not found then
    insert into private.bpay_next_run_segment_capture(run_worker_id,expected_count)
      select p_run_worker_id,coalesce(sum(p.segment_expected_member_count),0)
      from private.bpay_next_run_position p where p.run_worker_id=p_run_worker_id;
    select s.* into strict v_state from private.bpay_next_run_segment_capture s where s.run_worker_id=p_run_worker_id for update;
  end if;
  if v_state.complete then return jsonb_build_object('done',true,'rows_visited',0);end if;
  for v_member in select s.* from private.bpay_next_run_segment_members_v1(p_run_worker_id,
    v_state.cursor_work_id,v_state.cursor_component_key,v_state.cursor_segment_key,p_limit) s
  loop
    select p.* into strict v_parent from private.bpay_next_run_position p
      where p.run_work_id=v_member.run_work_id and p.component_key=v_member.component_key for update;
    select s.* into v_current from private.bpay_next_approved_segment s
      where s.revision_id=v_member.captured_revision_id and s.segment_key=v_member.segment_key;
    select s.* into v_selected from private.bpay_next_selected_segment s
      where s.run_id=v_worker.run_id and s.work_id=v_member.work_id and s.segment_key=v_member.segment_key;
    v_disposition:=case when v_current.id is null then 'REMOVED'
      when v_selected.approved_segment_id=v_current.id then v_selected.disposition else 'NOT_SELECTED' end;
    insert into private.bpay_next_run_work_segment(run_work_id,run_worker_id,work_id,captured_revision_id,
      segment_key,approved_segment_id,disposition)
    values(v_member.run_work_id,p_run_worker_id,v_member.work_id,v_member.captured_revision_id,
      v_member.segment_key,v_current.id,v_disposition) on conflict(run_work_id,segment_key) do nothing;
    select s.id into strict v_segment_id from private.bpay_next_run_work_segment s
      where s.run_work_id=v_member.run_work_id and s.segment_key=v_member.segment_key
        and s.approved_segment_id is not distinct from v_current.id and s.disposition=v_disposition;
    v_a:=case when v_current.component_key=v_member.component_key then v_current.approved_source_ex_vat else 0 end;
    select b.realised_source_ex_vat,b.active_held_source_ex_vat as held_source_ex_vat into strict v_balance
      from private.bpay_next_work_segment_gross_balance_v1(
      v_member.work_id,v_member.component_key,v_member.segment_key) b;
    v_p:=greatest(v_a-v_balance.realised_source_ex_vat-v_balance.held_source_ex_vat,0);
    v_r:=greatest(v_balance.realised_source_ex_vat-v_a,0);
    v_h:=greatest(v_balance.held_source_ex_vat-greatest(v_a-v_balance.realised_source_ex_vat,0),0);
    insert into private.bpay_next_run_work_segment_parent(run_work_segment_id,run_work_id,work_id,captured_revision_id,
      segment_key,component_key,approved_segment_id,approved_line_id,work_date,approved_source_ex_vat,
      realised_source_ex_vat,held_source_ex_vat,residual_source_ex_vat,included_positive_source_ex_vat,
      excluded_positive_source_ex_vat,recovery_source_ex_vat,held_overhang_source_ex_vat)
    values(v_segment_id,v_member.run_work_id,v_member.work_id,v_member.captured_revision_id,v_member.segment_key,
      v_member.component_key,case when v_current.component_key=v_member.component_key then v_current.id end,
      case when v_current.component_key=v_member.component_key then v_current.approved_line_id end,
      case when v_current.component_key=v_member.component_key then v_current.work_date end,v_a,
      v_balance.realised_source_ex_vat,v_balance.held_source_ex_vat,v_a-v_balance.realised_source_ex_vat-v_balance.held_source_ex_vat,
      case when v_disposition='INCLUDED' then v_p else 0 end,
      case when v_disposition='INCLUDED' then 0 else v_p end,v_r,v_h);
    update private.bpay_next_run_position set segment_member_count=segment_member_count+1,
      segment_included_positive_ex_vat=segment_included_positive_ex_vat+case when v_disposition='INCLUDED' then v_p else 0 end,
      segment_excluded_positive_ex_vat=segment_excluded_positive_ex_vat+case when v_disposition='INCLUDED' then 0 else v_p end,
      segment_recovery_ex_vat=segment_recovery_ex_vat+v_r,segment_held_overhang_ex_vat=segment_held_overhang_ex_vat+v_h
      where run_work_id=v_member.run_work_id and component_key=v_member.component_key;
    if v_parent.segment_member_count+1=v_parent.segment_expected_member_count then
      select sum(p.approved_source_ex_vat) a,sum(p.realised_source_ex_vat) c,sum(p.held_source_ex_vat) h
        into v_totals from private.bpay_next_run_work_segment_parent p
        where p.run_work_id=v_member.run_work_id and p.component_key=v_member.component_key;
      select (select row(coalesce(sum(e.target_ex_vat),0),coalesce(sum(e.target_vat),0),coalesce(sum(e.target_inc_vat),0))
          from private.bpay_next_work_segment_effect_share e where e.work_id=v_member.work_id and e.component_key=v_member.component_key) c,
        (select row(coalesce(sum(s.target_reserved_ex_vat),0),coalesce(sum(s.target_reserved_vat),0),coalesce(sum(s.target_reserved_inc_vat),0))
          from private.bpay_next_work_segment_origin_share s join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
          where s.work_id=v_member.work_id and s.component_key=v_member.component_key) h into v_target_totals;
      if (v_totals.a,v_totals.c,v_totals.h) is distinct from
        (v_parent.approved_source_ex_vat,v_parent.realised_source_ex_vat,v_parent.held_source_ex_vat)
        or v_target_totals.c is distinct from row(v_parent.realised_target_ex_vat,v_parent.realised_target_vat,v_parent.realised_target_inc_vat)
        or v_target_totals.h is distinct from row(v_parent.held_target_ex_vat,v_parent.held_target_vat,v_parent.held_target_inc_vat) then
        update private.bpay_next_run_segment_capture set cursor_work_id=v_member.work_id,
          cursor_component_key=v_member.component_key,cursor_segment_key=v_member.segment_key,
          processed_count=v_state.processed_count+v_seen+1 where run_worker_id=p_run_worker_id;
        update private.bpay_next_run_worker set status='REVIEW',review_issue_code='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE',
          review_issue_work_id=v_member.work_id where id=p_run_worker_id;
        return jsonb_build_object('done',false,'rows_visited',v_seen+1);
      end if;
      select p.* into strict v_parent from private.bpay_next_run_position p
        where p.run_work_id=v_member.run_work_id and p.component_key=v_member.component_key;
      v_collection:=null;v_needs_resolution:=v_parent.needs_financial_resolution;
      if v_parent.segment_recovery_ex_vat>0 and v_worker.target_pay_channel='PAYE' then
        select b.* into v_collection from private.bpay_next_work_collection b
          where b.work_id=v_member.work_id and b.component_key=v_member.component_key and b.segment_attribution_complete;
        if v_collection.case_id is not null then
          v_collection:=private.bpay_next_work_collection_origin_v1(v_collection.case_id,v_collection.case_component_id,v_worker.candidate_id);
        else v_collection:=null;end if;
        if v_collection.id is not null and v_parent.segment_held_overhang_ex_vat=0 then v_needs_resolution:=false;
        else v_needs_resolution:=true;end if;
      elsif v_parent.segment_recovery_ex_vat>0 or v_parent.segment_held_overhang_ex_vat>0 then v_needs_resolution:=true;
      elsif v_parent.approved_source_ex_vat=0 and v_parent.realised_source_ex_vat=0 and v_parent.held_source_ex_vat=0 then
        v_needs_resolution:=false;
      end if;
      update private.bpay_next_run_position set segment_attribution_complete=true,
        needs_financial_resolution=v_needs_resolution,work_collection_id=v_collection.id,work_collection_revision=v_collection.reconcile_revision
        where run_work_id=v_member.run_work_id and component_key=v_member.component_key;
      update private.bpay_next_run_worker set financial_resolution_count=financial_resolution_count
        +case when v_needs_resolution then 1 else 0 end-case when v_parent.needs_financial_resolution then 1 else 0 end,
        expected_managed_position_count=expected_managed_position_count+case when v_collection.id is not null then 1 else 0 end
          -case when v_parent.work_collection_id is not null then 1 else 0 end
        where id=p_run_worker_id;
    end if;
    v_state.cursor_work_id:=v_member.work_id;v_state.cursor_component_key:=v_member.component_key;
    v_state.cursor_segment_key:=v_member.segment_key;v_seen:=v_seen+1;
  end loop;
  v_state.processed_count:=v_state.processed_count+v_seen;
  v_state.complete:=v_state.processed_count=v_state.expected_count;
  if v_state.complete and exists(select 1 from private.bpay_next_run_position p
    where p.run_worker_id=p_run_worker_id and not p.segment_attribution_complete) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';
  end if;
  update private.bpay_next_run_segment_capture set cursor_work_id=v_state.cursor_work_id,
    cursor_component_key=v_state.cursor_component_key,cursor_segment_key=v_state.cursor_segment_key,
    processed_count=v_state.processed_count,complete=v_state.complete where run_worker_id=p_run_worker_id;
  return jsonb_build_object('done',v_state.complete,'rows_visited',v_seen);
end
$function$;
alter function private.bpay_next_capture_segment_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_capture_segment_page_v1(uuid,integer) from public,anon,authenticated,service_role;

-- Exact current1610 successor from2018.
create or replace function private.bpay_next_capture_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_row record;
  v_member record;
  v_seen integer:=0;
  v_selected_seen integer:=0;
  v_resolution_count integer:=0;
  v_managed_seen integer:=0;
  v_needs_resolution boolean;
  v_collection private.bpay_next_work_collection%rowtype;
  v_work_cursor uuid;
  v_component_cursor text;
  v_issue text;
  v_issue_work_id uuid;
  v_query_pin private.bpay_next_run_work_query_pin_v2%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_book record;
  v_segments bigint;v_snooze jsonb;v_segment_result jsonb;v_segment_visited integer:=0;
  v_seed_effect uuid;v_seed_hold uuid;v_seed_holds uuid[];v_seed record;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CAPTURE_POSITION_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('RESERVE','ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_LEASE_STALE';
  end if;
  if v_job.phase='ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'replay',true,'rows_visited',0);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
      'replay',true,'rows_visited',0,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_WORKER_NOT_PREPARING';
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  for v_member in
    select s.* from private.bpay_next_selected_position_keys_v1(v_worker.id,v_work_cursor,v_component_cursor,p_limit) s
  loop
    -- The Candidate control is already exclusively held. Lock this exact
    -- selected WORK before its position, matching the reentrant seed order.
    perform 1 from private.bpay_next_work w where w.id=v_member.work_id
      and w.candidate_id=v_worker.candidate_id for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_POSITION_WORK_SCOPE_INVALID';end if;
    -- Read and lock the exact position after choosing the bounded member.
    -- Do not FOR SHARE the nullable side of an outer join (PostgreSQL refuses
    -- that), and never turn a missing selected position into an omitted line.
    select v_member.run_work_id as run_work_id,v_member.work_id as work_id,
           v_member.captured_revision_id as captured_revision_id,
           p.component_key,p.applied_revision_id,
           p.approved_source_ex_vat,p.realised_source_ex_vat,
           p.held_source_ex_vat,p.realised_target_ex_vat,
           p.realised_target_vat,p.realised_target_inc_vat,
           p.held_target_ex_vat,p.held_target_vat,
           p.held_target_inc_vat,p.source_basis_channel,
           rev.source_pay_channel,rev.source_kind,rev.source_event_id,
           rev.source_head_id,rev.financial_snapshot_id,
           rev.physical_timesheet_id,rev.physical_timesheet_version,
           w.work_kind,w.candidate_id as origin_candidate_id,
           l.component_kind,l.id as approved_line_id,
           l.source_pay_ex_vat as approved_line_source_ex_vat,l.tax_treatment,
           p.source_book_state,p.source_book_revision
      into v_row
      from private.bpay_next_position p
      join private.bpay_next_work_revision rev
        on rev.id=v_member.captured_revision_id and rev.work_id=v_member.work_id
      join private.bpay_next_work w on w.id=rev.work_id
      left join private.bpay_next_approved_line l
        on l.revision_id=v_member.captured_revision_id
       and l.component_key=v_member.component_key
      where p.work_id=v_member.work_id and p.component_key=v_member.component_key
      for update of p;
    v_issue:=null;
    if not found
       or v_row.applied_revision_id is distinct from v_member.captured_revision_id then
      v_issue:='CAPTURE_POSITION_CHANGED';
    elsif v_row.approved_line_id is null
       and v_row.approved_source_ex_vat<>0 then
      v_issue:='POSITION_COMPONENT_EVIDENCE_MISSING';
    elsif v_row.approved_line_id is not null
       and v_row.approved_line_source_ex_vat is distinct from
         v_row.approved_source_ex_vat then
      v_issue:='POSITION_APPROVAL_AMOUNT_MISMATCH';
    end if;
    if v_worker.target_pay_channel='UMBRELLA' and v_issue is null and
       (v_row.source_pay_channel is distinct from 'UMBRELLA'
        or v_row.work_kind not in ('ORDINARY','SOURCE')
        or v_row.component_kind is distinct from 'WORK'
        or v_row.tax_treatment is distinct from 'TAXABLE') then
      v_issue:='UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    if v_issue is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code=v_issue,
            review_issue_work_id=v_member.work_id
        where id=v_worker.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code',v_issue,'work_id',v_member.work_id,'replay',false);
    end if;
    -- Typed initial I-1 is the ONLY new no-HEAD binding arm. Initial and
    -- common-HEAD captures require self-canonical same-channel TAXABLE WORK.
    v_binding:=null;v_book:=null;
    if v_row.work_kind='SOURCE' and (v_row.source_head_id is not null
      or (v_row.source_kind='SOURCE' and v_row.source_event_id is not null
        and v_row.financial_snapshot_id is not null)) then
      select v.* into v_binding from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
        where b.canonical_work_id=v_row.work_id and b.canonical_component_key=v_row.component_key
          and v.approval_work_id=v_row.work_id and v.approval_revision_id=v_row.captured_revision_id
          and v.approval_component_key=v_row.component_key
          and ((v_row.source_head_id is not null and v.origin_kind='COMMON_HEAD_RECEIPT_V1'
              and v.head_id=v_row.source_head_id)
            or (v_row.source_head_id is null and v.origin_kind='INITIAL_AUTHORISED_TSFIN_V1'
              and v.initial_root_authorisation_id=v_row.source_event_id
              and v.initial_financial_snapshot_id=v_row.financial_snapshot_id
              and v.initial_root_timesheet_id=v_row.physical_timesheet_id
              and v.initial_root_version=v_row.physical_timesheet_version));
      if v_binding.id is null or v_binding.publication_command_id_2 is not null
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.component_kind is distinct from 'WORK' or v_row.tax_treatment is distinct from 'TAXABLE' then
        v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
      else
        if v_row.source_book_state='UNBOUND' then
          -- Capture is already a bounded selected point with real APPLIED/DONE
          -- authority. Adoption never borrows a queued writer's lease here.
          v_seed_effect:=null;v_seed_hold:=null;v_seed_holds:=null;
          if v_row.realised_source_ex_vat>0 then
            select b.original_effect_id into v_seed_effect from private.bpay_next_work_collection b
              where b.work_id=v_row.work_id and b.component_key=v_row.component_key
                and b.candidate_id=v_worker.candidate_id and b.paid_basis_qualified;
          end if;
          if v_row.held_source_ex_vat>0 then
            select pg_catalog.array_agg(h.id order by h.id) into v_seed_holds from (
              select id from private.bpay_next_hold where work_id=v_row.work_id
                and component_key=v_row.component_key and status='ACTIVE' order by id limit 2) h;
            if pg_catalog.cardinality(v_seed_holds) is distinct from 1 then
              raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ACTIVE_HOLD_WITNESS_AMBIGUOUS';
            end if;
            v_seed_hold:=v_seed_holds[1];
          end if;
          select seed.* into strict v_seed from private.bpay_next_source_disposition_seed_v1(
            v_row.work_id,v_row.component_key,v_row.captured_revision_id,v_seed_effect,v_seed_hold) seed;
          if v_seed.qualification_state is distinct from 'READY' then
            raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SELECTED_SEED_NOT_READY';
          end if;
          select p.source_book_state,p.source_book_revision into v_row.source_book_state,v_row.source_book_revision
            from private.bpay_next_position p where p.work_id=v_row.work_id and p.component_key=v_row.component_key;
        end if;
        select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
          v_binding.id,v_row.source_book_revision) s;
        if v_book.qualification_state is distinct from 'READY'
           or (v_book.candidate_id,v_book.canonical_work_id,v_book.canonical_component_key,
               v_book.approval_work_id,v_book.approval_revision_id,v_book.approved_line_id,
               v_book.source_channel,v_book.tax_treatment,v_book.approved_source_ex_vat,
               v_book.native_realised_paye,v_book.native_held_paye,v_book.native_realised_umbrella,v_book.native_held_umbrella)
             is distinct from (v_worker.candidate_id,v_row.work_id,v_row.component_key,
               v_row.work_id,v_row.captured_revision_id,v_row.approved_line_id,
               v_row.source_pay_channel,'TAXABLE'::text,v_row.approved_source_ex_vat,
               case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end) then
          v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
        end if;
      end if;
      if v_issue is not null then
        update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,
          review_issue_work_id=v_row.work_id where id=v_worker.id;
        update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,
          lease_nonce=null,lease_until_utc=null where id=p_job_id;
        return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','issue_code',v_issue,
          'work_id',v_row.work_id,'rows_visited',v_seen,'replay',false);
      end if;
    end if;
    v_collection:=null;
    -- Only the exact typed original ordinary PAYE liability can own a
    -- negative residual. Unknown signed/cross-basis/held shapes still REVIEW.
    if v_worker.target_pay_channel='PAYE' and v_row.source_pay_channel='PAYE'
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat<0
       and (v_row.held_source_ex_vat,v_row.held_target_ex_vat,v_row.held_target_vat,v_row.held_target_inc_vat)
         is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      select b.* into v_collection from private.bpay_next_work_collection b
        where b.work_id=v_row.work_id and b.component_key=v_row.component_key;
      if v_collection.case_id is not null then
        v_collection:=private.bpay_next_work_collection_origin_v1(
          v_collection.case_id,v_collection.case_component_id,v_worker.candidate_id);
      else v_collection:=null;end if;
      if v_collection.id is not null and
         (v_collection.observed_revision_id,v_collection.observed_approved_source_ex_vat,v_collection.observed_realised_source_ex_vat)
           is distinct from (v_row.captured_revision_id,v_row.approved_source_ex_vat,v_row.realised_source_ex_vat) then
        v_collection:=null;
      end if;
    end if;
    v_needs_resolution:=v_collection.id is null and (
       v_row.approved_line_id is null
       or v_row.approved_source_ex_vat<0
       or ((v_row.realised_source_ex_vat<>0
          or v_row.held_source_ex_vat<>0
          or v_row.realised_target_inc_vat<>0
          or v_row.held_target_inc_vat<>0)
         and v_row.source_basis_channel is distinct from v_row.source_pay_channel)
       or v_row.realised_source_ex_vat<0
       or v_row.realised_target_ex_vat<0 or v_row.realised_target_vat<0 or v_row.realised_target_inc_vat<0
       or (v_row.held_source_ex_vat>0 and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<=0)
       or (v_row.held_source_ex_vat=0 and v_row.held_target_inc_vat<>0)
       or v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<0);
    -- Only genuine positive Source WORK consumes the retained disposition.
    -- Expense/additional/ordinary, zero, realised/held-away, managed negative
    -- and already-unresolved financial shapes do not become query holds.
    -- The actual pin validated the typed origin before this immutable row;
    -- missing evidence is REVIEW, never a lazy live Source reader fallback.
    if v_row.work_kind='SOURCE' and v_row.source_kind in ('SOURCE','PROTECTED')
       and v_row.source_event_id is not null
       and v_row.component_kind in ('WORK','PROTECTED_WORK')
       and not v_needs_resolution
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat>0 then
      select qp.* into v_query_pin from private.bpay_next_run_work_query_pin_v2 qp
        where qp.run_work_id=v_row.run_work_id;
      if found and (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.prepare_job_id,v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,p_job_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        v_issue:='SOURCE_PAY_QUERY_OPEN';
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        update private.bpay_next_run_worker
          set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_row.work_id
          where id=v_worker.id;
        update private.bpay_next_job
          set status='DONE',phase='DONE',cursor_key=null,
              lease_nonce=null,lease_until_utc=null
          where id=p_job_id;
        return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
          'issue_code',v_issue,'work_id',v_row.work_id,'rows_visited',v_seen,'replay',false);
      end if;
    end if;
    select count(*) into v_segments from (
      select s.segment_key from private.bpay_next_approved_segment s
        where s.revision_id=v_row.captured_revision_id and s.component_key=v_row.component_key
      union select s.segment_key from private.bpay_next_work_segment_origin_share s
        where s.work_id=v_row.work_id and s.component_key=v_row.component_key) members;
    if v_segments>0 and v_row.work_kind='ORDINARY' then
      -- Exact segment negative ownership is sealed after complete child capture.
      -- Initial native net-negative classification cannot bypass that census.
      if v_collection.id is not null then v_needs_resolution:=true;end if;
      v_collection:=null;
    end if;
    v_snooze:=null;
    if v_segments=0 then
      v_snooze:=private.bpay_next_snooze_at_cut_v1(v_row.work_id,v_row.captured_revision_id,
        null,v_row.component_key,v_run.snooze_control_sequence,v_run.snooze_business_date);
    end if;
    insert into private.bpay_next_run_position
      (run_worker_id,run_work_id,work_id,captured_revision_id,
       component_key,approved_line_id,source_pay_channel,
       source_basis_channel,
       approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,
       residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,
       realised_target_inc_vat,held_target_ex_vat,held_target_vat,
       held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,
       source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision,
       segment_expected_member_count,segment_attribution_complete,snooze_id,snooze_version,snooze_excluded_positive_ex_vat)
      values(v_worker.id,v_row.run_work_id,v_row.work_id,
       v_row.captured_revision_id,v_row.component_key,v_row.approved_line_id,
       v_row.source_pay_channel,v_row.source_basis_channel,
       v_row.approved_source_ex_vat,
       v_row.realised_source_ex_vat,v_row.held_source_ex_vat,
       v_row.approved_source_ex_vat-v_row.realised_source_ex_vat
         -v_row.held_source_ex_vat,
       v_row.realised_target_ex_vat,v_row.realised_target_vat,
       v_row.realised_target_inc_vat,v_row.held_target_ex_vat,
       v_row.held_target_vat,v_row.held_target_inc_vat,
       v_needs_resolution,v_collection.id,v_collection.reconcile_revision,
       v_binding.id,v_binding.canonical_work_id,v_binding.canonical_component_key,
       case when v_binding.id is null then null else v_row.source_book_revision end,
       v_segments,v_segments=0,(v_snooze->>'id')::uuid,(v_snooze->>'version')::bigint,
       case when v_snooze is not null then greatest(v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat,0) else 0 end);
    if v_needs_resolution then
      v_resolution_count:=v_resolution_count+1;
    end if;
    if v_collection.id is not null then v_managed_seen:=v_managed_seen+1;end if;
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
    if v_member.selection_mode='SUBSET' then
      v_selected_seen:=v_selected_seen+1;
    end if;
  end loop;
  if v_resolution_count>0 then
    update private.bpay_next_run_worker
      set financial_resolution_count=financial_resolution_count+v_resolution_count
      where id=v_worker.id;
  end if;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_position_count=captured_position_count+v_seen,
          expected_managed_position_count=expected_managed_position_count+v_managed_seen,
          captured_selected_component_count=
            captured_selected_component_count+v_selected_seen
      where id=v_worker.id;
  end if;
  if v_seen<p_limit then
    if v_worker.captured_selected_component_count+v_selected_seen
       <>v_worker.expected_selected_component_count then
      select rw.work_id into strict v_issue_work_id
        from private.bpay_next_run_work rw
        join private.bpay_next_work_choice choice
          on choice.run_id=v_run_id and choice.work_id=rw.work_id
         and choice.selection_mode='SUBSET'
        where rw.run_worker_id=v_worker.id
        order by rw.work_id limit 1;
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='CAPTURE_POSITION_CHANGED',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code','CAPTURE_POSITION_CHANGED','work_id',v_issue_work_id,'replay',false);
    end if;
    v_segment_result:=private.bpay_next_capture_segment_page_v1(v_worker.id,p_limit-v_seen);
    v_segment_visited:=(v_segment_result->>'rows_visited')::integer;
    select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker.id;
    if v_worker.status='REVIEW' then
      update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
      return jsonb_build_object('done',true,'phase','REVIEW','issue_code',v_worker.review_issue_code,
        'work_id',v_worker.review_issue_work_id,'rows_visited',v_seen+v_segment_visited,'replay',false);
    end if;
    if not (v_segment_result->>'done')::boolean then
      update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
      return jsonb_build_object('done',false,'phase','RESERVE','rows_visited',v_seen+v_segment_visited,
        'work_cursor',v_work_cursor,'component_cursor',v_component_cursor,'replay',false);
    end if;
    update private.bpay_next_job
      set phase='ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'rows_visited',v_seen+v_segment_visited,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
    'rows_visited',v_seen+v_segment_visited,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

alter function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;

-- Exact current1620 successor from2018.
create or replace function private.bpay_next_hold_fresh_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_issue_work_id uuid;
  v_row record;
  v_position private.bpay_next_position%rowtype;
  v_value record;v_book record;
  v_run_line_id uuid;v_hold_id uuid;v_included_source numeric(18,2);v_freeze_zero boolean;
  v_seen integer:=0;
  v_work_cursor uuid;
  v_component_cursor text;
  v_line_count bigint;
  v_managed_count bigint;
  v_total_ex numeric(18,2);
  v_total_vat numeric(18,2);
  v_total_inc numeric(18,2);
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_HOLD_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('ALLOCATE','FINANCE_ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_HOLD_LEASE_STALE';
  end if;
  if v_job.phase='FINANCE_ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',0,'replay',true);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
      'rows_visited',0,'replay',true,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_HOLD_WORKER_NOT_PREPARING';
  end if;
  if v_worker.financial_resolution_count>0 then
    select rp.work_id into strict v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id and rp.needs_financial_resolution
      order by rp.work_id,rp.component_key limit 1;
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code='FINANCIAL_RESOLUTION_REQUIRED',
          review_issue_work_id=v_issue_work_id
      where id=v_worker.id;
    update private.bpay_next_job
      set status='DONE',phase='DONE',cursor_key=null,
          lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
      'issue_code','FINANCIAL_RESOLUTION_REQUIRED',
      'work_id',v_issue_work_id,'replay',false);
  end if;
  if v_worker.target_pay_channel='UMBRELLA'
     and (v_worker.target_umbrella_id is null
       or v_worker.target_umbrella_vat_chargeable is null) then
    select rp.work_id into v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id
      order by rp.work_id,rp.component_key limit 1;
    if v_issue_work_id is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='TARGET_UMBRELLA_MISSING',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code','TARGET_UMBRELLA_MISSING',
        'work_id',v_issue_work_id,'replay',false);
    end if;
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  v_line_count:=v_worker.captured_line_count;
  v_managed_count:=v_worker.captured_managed_position_count;
  v_total_ex:=v_worker.gross_ex_vat;
  v_total_vat:=v_worker.gross_vat;
  v_total_inc:=v_worker.gross_inc_vat;
  for v_row in
    select rp.*,rw.id as pinned_work_id,rev.valuation_policy_id
      from private.bpay_next_run_position rp
      join private.bpay_next_run_work rw on rw.id=rp.run_work_id
      join private.bpay_next_work_revision rev
        on rev.id=rp.captured_revision_id and rev.work_id=rp.work_id
      where rp.run_worker_id=v_worker.id
        and (v_work_cursor is null or rp.work_id>v_work_cursor
          or (rp.work_id=v_work_cursor
            and rp.component_key>v_component_cursor))
      order by rp.work_id,rp.component_key limit p_limit
  loop
    if not v_row.segment_attribution_complete then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';
    end if;
    v_included_source:=case when v_row.segment_member_count>0 then v_row.segment_included_positive_ex_vat
      else greatest(v_row.residual_source_ex_vat,0)-v_row.snooze_excluded_positive_ex_vat end;
    v_freeze_zero:=v_row.approved_line_id is not null and v_row.approved_source_ex_vat=0 and v_row.realised_source_ex_vat=0 and v_row.held_source_ex_vat=0
      and v_row.snooze_id is null and (v_row.segment_member_count=0 or exists(
        select 1 from private.bpay_next_run_work_segment_parent p join private.bpay_next_run_work_segment s on s.id=p.run_work_segment_id
          where p.run_work_id=v_row.run_work_id and p.component_key=v_row.component_key and s.disposition='INCLUDED'));
    if v_row.needs_financial_resolution
       or (v_row.work_collection_id is null and
         ((v_row.approved_line_id is null and (v_row.segment_member_count=0 or v_included_source>0)) or v_row.residual_source_ex_vat<0)) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_HOLD_UNEXPECTED_POSITION_SHAPE';
    end if;
    select * into strict v_position from private.bpay_next_position
      where work_id=v_row.work_id and component_key=v_row.component_key
      for update;
    if v_position.applied_revision_id is distinct from v_row.captured_revision_id
       or v_position.approved_source_ex_vat is distinct from
         v_row.approved_source_ex_vat
       or v_position.realised_source_ex_vat is distinct from
         v_row.realised_source_ex_vat
       or v_position.held_source_ex_vat is distinct from
         v_row.held_source_ex_vat
       or v_position.source_basis_channel is distinct from
         v_row.source_basis_channel
       or v_position.realised_target_ex_vat is distinct from
         v_row.realised_target_ex_vat
       or v_position.realised_target_vat is distinct from
         v_row.realised_target_vat
       or v_position.realised_target_inc_vat is distinct from
         v_row.realised_target_inc_vat
       or v_position.held_target_ex_vat is distinct from
         v_row.held_target_ex_vat
       or v_position.held_target_vat is distinct from
         v_row.held_target_vat
       or v_position.held_target_inc_vat is distinct from
         v_row.held_target_inc_vat then
      raise exception using errcode='55000',
        message='BPAY_NEXT_HOLD_CURRENT_POSITION_CHANGED';
    end if;
    if v_row.source_binding_version_id is not null then
      select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
        v_row.source_binding_version_id,v_row.source_book_revision) s;
      if v_book.qualification_state is distinct from 'READY'
         or (v_book.canonical_work_id,v_book.canonical_component_key,v_book.approval_work_id,
             v_book.approval_revision_id,v_book.approved_line_id,v_book.source_channel,v_book.tax_treatment,
             v_book.approved_source_ex_vat,v_book.native_realised_paye,v_book.native_held_paye,
             v_book.native_realised_umbrella,v_book.native_held_umbrella)
           is distinct from (v_row.work_id,v_row.component_key,v_row.work_id,
             v_row.captured_revision_id,v_row.approved_line_id,v_row.source_pay_channel,'TAXABLE'::text,
             v_row.approved_source_ex_vat,
             case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end)
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.canonical_source_work_id is distinct from v_row.work_id
         or v_row.canonical_source_component_key is distinct from v_row.component_key then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CAPTURE_CHANGED';
      end if;
    end if;
    if v_row.work_collection_id is not null then
      -- Classification and original provenance were frozen by RESERVE. Never
      -- reread a newer WORK approval or create an ordinary negative/zero hold.
      update private.bpay_next_run_position set managed_capture_completed=true
        where id=v_row.id and not managed_capture_completed;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ALREADY_SPENT';end if;
      v_managed_count:=v_managed_count+1;
    end if;
    if v_included_source>0 or v_freeze_zero then
    select * into strict v_value
      from private.bpay_next_value_source_for_target_v1(
        v_included_source,v_row.source_pay_channel,
        v_worker.target_pay_channel,
        case when v_worker.target_pay_channel='UMBRELLA'
          then v_worker.target_umbrella_vat_chargeable else false end,
        v_row.valuation_policy_id,v_run.pay_date);
    v_line_count:=v_line_count+1;
    insert into private.bpay_next_run_line
      (run_worker_id,run_work_id,line_no,approved_line_id,
       captured_revision_id,work_id,component_key,source_consumed_ex_vat,
       source_pay_channel,target_pay_channel,valuation_policy_id,
       policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat,source_binding_version_id,
       canonical_source_work_id,canonical_source_component_key,source_book_revision,updates_original_source_scalar)
      values(v_worker.id,v_row.run_work_id,v_line_count,
       v_row.approved_line_id,v_row.captured_revision_id,v_row.work_id,
       v_row.component_key,v_included_source,
       v_row.source_pay_channel,v_worker.target_pay_channel,
       v_row.valuation_policy_id,v_value.policy_window_id,
       v_value.target_ex_vat,v_value.target_vat,v_value.target_inc_vat,
       v_row.source_binding_version_id,v_row.canonical_source_work_id,v_row.canonical_source_component_key,
       v_row.source_book_revision,case when v_row.source_binding_version_id is null then null else true end)
      returning id into v_run_line_id;
    -- An authorised zero still has immutable shift/rate advice detail. It
    -- becomes a frozen zero line, never a zero-valued financial hold.
    if v_included_source>0 then
      insert into private.bpay_next_hold
        (run_line_id,work_id,component_key,source_reserved_ex_vat,
         target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status)
        values(v_run_line_id,v_row.work_id,v_row.component_key,
         v_included_source,v_value.target_ex_vat,
         v_value.target_vat,v_value.target_inc_vat,'ACTIVE') returning id into v_hold_id;
      perform private.bpay_next_record_work_segment_origin_shares_v1(v_run_line_id,v_hold_id);
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat+v_included_source,
            held_target_ex_vat=held_target_ex_vat+v_value.target_ex_vat,
            held_target_vat=held_target_vat+v_value.target_vat,
            held_target_inc_vat=held_target_inc_vat+v_value.target_inc_vat,
            updated_at_utc=pg_catalog.transaction_timestamp()
        where work_id=v_row.work_id and component_key=v_row.component_key;
    end if;
    v_total_ex:=v_total_ex+v_value.target_ex_vat;
    v_total_vat:=v_total_vat+v_value.target_vat;
    v_total_inc:=v_total_inc+v_value.target_inc_vat;
    end if;
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
  end loop;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_line_count=v_line_count,
          captured_managed_position_count=v_managed_count,
          gross_ex_vat=v_total_ex,gross_vat=v_total_vat,
          gross_inc_vat=v_total_inc
      where id=v_worker.id;
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_job.candidate_id;
  end if;
  if v_seen<p_limit then
    if v_managed_count<>v_worker.expected_managed_position_count or exists(
      select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
        and work_collection_id is not null and not managed_capture_completed) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_COUNT_MISMATCH';
    end if;
    update private.bpay_next_job
      set phase='FINANCE_ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',v_seen,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
    'rows_visited',v_seen,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

alter function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;

-- Exact current1519 guard successor, required subordinate attribution only.
create or replace function private.bpay_next_work_collection_capture_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;v_totals record;
begin
  if tg_table_name='bpay_next_case_period' then
    if tg_op='UPDATE' then
      if new.work_collection_id is distinct from old.work_collection_id then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_IMMUTABLE';
      end if;
      if new.work_collection_id is not null and
         ((new.case_component_id,new.case_id,new.candidate_id,new.pay_week_start,new.rule_id,
           new.opening_outstanding_source_ex_vat,new.opening_due_source_ex_vat)
           is distinct from (old.case_component_id,old.case_id,old.candidate_id,old.pay_week_start,old.rule_id,
             old.opening_outstanding_source_ex_vat,old.opening_due_source_ex_vat)
          or new.period_revision<>old.period_revision+1
          or new.realised_recovery_source_ex_vat<old.realised_recovery_source_ex_vat
          or new.active_payout_source_ex_vat<>0
          or not exists(select 1 from private.bpay_next_case_component x
            join private.bpay_next_finance_case c on c.id=x.case_id and c.candidate_id=x.candidate_id
            where x.id=new.case_component_id and x.id=new.work_collection_id
              and x.case_id=new.case_id and x.candidate_id=new.candidate_id
              and x.recovered_source_ex_vat>=new.realised_recovery_source_ex_vat
              and x.active_recovery_source_ex_vat>=new.active_unrealised_recovery_source_ex_vat
              and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
                is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat))) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_COUNTER_INVALID';
      end if;
      return new;
    end if;
    if new.work_collection_id is null then
      if exists(select 1 from private.bpay_next_work_collection b where b.case_id=new.case_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_REQUIRED';
      end if;
      return new;
    end if;
    v_basis:=private.bpay_next_work_collection_origin_v1(new.case_id,new.case_component_id,new.candidate_id);
    if v_basis.id is distinct from new.work_collection_id then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_run_case_instruction' then
    if new.work_collection_id is null then
      if exists(select 1 from private.bpay_next_work_collection b where b.case_id=new.case_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_INSTRUCTION_ORIGIN_REQUIRED';
      end if;
      return new;
    end if;
    v_basis:=private.bpay_next_work_collection_origin_v1(new.case_id,new.case_component_id,new.candidate_id);
    if v_basis.id is distinct from new.work_collection_id
       or new.valuation_policy_id is distinct from v_basis.original_valuation_policy_id then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_INSTRUCTION_ORIGIN_INVALID';
    end if;
    return new;
  end if;
  -- Required WB36 attribution counters are subordinate immutable observations.
  -- The old managed-capture transition remains exactly the ALLOCATE arm below.
  if tg_op='UPDATE' and (to_jsonb(new)-array['segment_member_count','segment_included_positive_ex_vat',
      'segment_excluded_positive_ex_vat','segment_recovery_ex_vat','segment_held_overhang_ex_vat','segment_attribution_complete',
      'needs_financial_resolution','work_collection_id','work_collection_revision'])
    is not distinct from (to_jsonb(old)-array['segment_member_count','segment_included_positive_ex_vat',
      'segment_excluded_positive_ex_vat','segment_recovery_ex_vat','segment_held_overhang_ex_vat','segment_attribution_complete',
      'needs_financial_resolution','work_collection_id','work_collection_revision']) then
    select count(*) n,coalesce(sum(p.included_positive_source_ex_vat),0) inc,
      coalesce(sum(p.excluded_positive_source_ex_vat),0) exc,coalesce(sum(p.recovery_source_ex_vat),0) rec,
      coalesce(sum(p.held_overhang_source_ex_vat),0) overhang,sum(p.approved_source_ex_vat) a,
      sum(p.realised_source_ex_vat) c,sum(p.held_source_ex_vat) h into v_totals
      from private.bpay_next_run_work_segment_parent p where p.run_work_id=new.run_work_id and p.component_key=new.component_key;
    if old.segment_attribution_complete or new.segment_member_count not between old.segment_member_count and old.segment_member_count+1
      or not private.bpay_next_segment_prepare_owner_v1(new.run_worker_id,'RESERVE')
      or (new.segment_member_count,new.segment_included_positive_ex_vat,new.segment_excluded_positive_ex_vat,
          new.segment_recovery_ex_vat,new.segment_held_overhang_ex_vat) is distinct from
        (v_totals.n,v_totals.inc,v_totals.exc,v_totals.rec,v_totals.overhang)
      or (new.segment_attribution_complete and (new.segment_member_count<>new.segment_expected_member_count or
        (new.approved_source_ex_vat,new.realised_source_ex_vat,new.held_source_ex_vat) is distinct from
          (v_totals.a,v_totals.c,v_totals.h))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_COUNTER_INVALID';
    end if;
    if (new.needs_financial_resolution,new.work_collection_id,new.work_collection_revision)
      is distinct from (old.needs_financial_resolution,old.work_collection_id,old.work_collection_revision) then
      if not new.segment_attribution_complete or new.segment_member_count<>old.segment_member_count then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CLASSIFICATION_BEFORE_SEAL';
      end if;
      if new.work_collection_id is not null then
        select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=new.work_collection_id;
        v_basis:=private.bpay_next_work_collection_origin_v1(v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id);
        if v_basis.id is distinct from new.work_collection_id or not v_basis.segment_attribution_complete
          or new.work_collection_revision is distinct from v_basis.reconcile_revision or new.needs_financial_resolution
          or new.segment_recovery_ex_vat<>v_basis.observed_segment_negative_ex_vat or new.segment_recovery_ex_vat<=0
          or new.segment_held_overhang_ex_vat<>0 or new.source_pay_channel<>'PAYE' or new.source_basis_channel is distinct from 'PAYE'
          or (new.work_id,new.component_key,new.captured_revision_id,new.approved_source_ex_vat,new.realised_source_ex_vat)
            is distinct from (v_basis.work_id,v_basis.component_key,v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_COLLECTION_CLASSIFICATION_INVALID';
        end if;
      elsif not new.needs_financial_resolution and not (new.approved_source_ex_vat=0 and new.realised_source_ex_vat=0 and new.held_source_ex_vat=0) then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_FINANCIAL_RESOLUTION_UNEXPLAINED';
      end if;
    end if;
    return new;
  end if;
  if tg_op<>'INSERT' then
    if tg_op<>'UPDATE' or old.work_collection_id is null or old.managed_capture_completed
       or not new.managed_capture_completed
       or (pg_catalog.to_jsonb(new)-'managed_capture_completed')
         is distinct from (pg_catalog.to_jsonb(old)-'managed_capture_completed') then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_IMMUTABLE';
    end if;
  elsif new.work_collection_id is null then
    return new;
  else
    select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=new.work_collection_id;
    v_basis:=private.bpay_next_work_collection_origin_v1(v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id);
    if v_basis.id is distinct from new.work_collection_id or new.managed_capture_completed
       or new.work_collection_revision is distinct from v_basis.reconcile_revision
       or (new.captured_revision_id,new.approved_source_ex_vat,new.realised_source_ex_vat)
         is distinct from (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat)
       or new.source_pay_channel<>'PAYE' or new.source_basis_channel is distinct from 'PAYE'
       or (new.held_source_ex_vat,new.held_target_ex_vat,new.held_target_vat,new.held_target_inc_vat)
         is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ORIGIN_INVALID';
    end if;
  end if;
  if not exists(select 1 from private.bpay_next_run_work rw
    join private.bpay_next_run_worker w on w.id=rw.run_worker_id and w.status='PREPARING'
    join private.bpay_next_run_command rc on rc.run_id=w.run_id
    join private.bpay_next_job j on j.command_id=rc.command_id and j.candidate_id=w.candidate_id
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    where rw.id=new.run_work_id and rw.run_worker_id=new.run_worker_id and rw.work_id=new.work_id
      and rw.captured_revision_id=new.captured_revision_id and j.job_kind='PREPARE' and j.status='LEASED'
      and j.phase=case when tg_op='INSERT' then 'RESERVE' else 'ALLOCATE' end
      and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_COLLECTION_CAPTURE_OWNER_STALE';
  end if;
  return new;
end
$function$;

alter function private.bpay_next_work_collection_capture_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_collection_capture_guard_v1() from public,anon,authenticated,service_role;

create or replace function private.bpay_next_segment_prepare_owner_v1(p_run_worker_id uuid,p_phase text)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_run_worker w
    join private.bpay_next_run_command rc on rc.run_id=w.run_id
    join private.bpay_next_job j on j.command_id=rc.command_id and j.candidate_id=w.candidate_id
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    join private.bpay_next_pay_run r on r.id=w.run_id and r.status='PREPARING' and r.selection_state='SEALED'
    where w.id=p_run_worker_id and w.status='PREPARING' and j.job_kind='PREPARE' and j.phase=p_phase
      and j.status='LEASED' and j.lease_until_utc>clock_timestamp())
$function$;
alter function private.bpay_next_segment_prepare_owner_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_segment_prepare_owner_v1(uuid,text) from public,anon,authenticated,service_role;

-- Finite native scope checks; none of these children is an independent money owner.
create or replace function private.bpay_next_segment_fact_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_choice private.bpay_next_work_choice%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_segment private.bpay_next_approved_segment%rowtype;v_captured private.bpay_next_run_work_segment%rowtype;
  v_parent private.bpay_next_run_position%rowtype;v_origin private.bpay_next_work_segment_origin_share%rowtype;
  v_effect private.bpay_next_financial_effect%rowtype;v_effect_share private.bpay_next_work_segment_effect_share%rowtype;
  v_hold private.bpay_next_hold%rowtype;v_line private.bpay_next_run_line%rowtype;
  v_case_event private.bpay_next_case_event%rowtype;v_case_hold private.bpay_next_case_hold%rowtype;
  v_snapshot jsonb;v_balance record;v_count bigint;v_disposition text;v_amount numeric;v_recovery numeric;v_overhang numeric;
begin
  if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_FACT_IMMUTABLE';end if;
  if tg_table_name in ('bpay_next_segment_selection_page','bpay_next_selected_segment') then
    select c.* into strict v_choice from private.bpay_next_work_choice c where c.run_id=new.run_id and c.work_id=new.work_id;
    select r.* into strict v_run from private.bpay_next_pay_run r where r.id=new.run_id;
    if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or v_choice.selection_state<>'SEALED'
      or v_choice.segment_sealed_count is not null then
      raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_SELECTION_CLOSED';
    end if;
    if tg_table_name='bpay_next_segment_selection_page' then
      if new.page_no<>v_choice.segment_page_count+1 or new.first_member_no<>v_choice.segment_count+1
        or jsonb_typeof(new.request_body) is distinct from 'array'
        or jsonb_array_length(new.request_body)<>new.item_count
        or exists(select 1 from jsonb_array_elements(new.request_body) item where jsonb_typeof(item)<>'string') then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_PAGE_SCOPE_INVALID';
      end if;
    else
      select s.* into strict v_segment from private.bpay_next_approved_segment s where s.id=new.approved_segment_id;
      v_snapshot:=private.bpay_next_snooze_at_cut_v1(new.work_id,new.expected_revision_id,new.segment_key,
        new.component_key,v_choice.segment_control_sequence,v_run.snooze_business_date);
      if (new.expected_revision_id,new.component_key,new.segment_key,new.control_sequence)
        is distinct from (v_choice.expected_revision_id,v_segment.component_key,v_segment.segment_key,v_choice.segment_control_sequence)
        or v_segment.revision_id<>new.expected_revision_id
        or (new.disposition,new.snooze_id,new.snooze_version) is distinct from
          (case when v_snapshot is null then 'INCLUDED'::text else 'SNOOZED'::text end,(v_snapshot->>'id')::uuid,(v_snapshot->>'version')::bigint)
        or not exists(select 1 from private.bpay_next_segment_selection_page p where p.run_id=new.run_id and p.work_id=new.work_id
          and p.page_no=v_choice.segment_page_count+1 and new.selection_no between p.first_member_no and p.first_member_no+p.item_count-1
          and p.request_body ? new.segment_key)
        or (v_choice.selection_mode='SUBSET' and not exists(select 1 from private.bpay_next_selected_component c
          where c.run_id=new.run_id and c.work_id=new.work_id and c.component_key=new.component_key)) then
        raise exception using errcode='23514',message='BPAY_NEXT_SELECTED_SEGMENT_SCOPE_INVALID';
      end if;
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_run_work_segment' then
    if not private.bpay_next_segment_prepare_owner_v1(new.run_worker_id,'RESERVE') then
      raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_CAPTURE_OWNER_STALE';
    end if;
    select s.* into v_segment from private.bpay_next_approved_segment s
      where s.revision_id=new.captured_revision_id and s.segment_key=new.segment_key;
    select s.disposition into v_disposition from private.bpay_next_selected_segment s
      join private.bpay_next_run_worker w on w.run_id=s.run_id and w.id=new.run_worker_id
      where s.work_id=new.work_id and s.expected_revision_id=new.captured_revision_id and s.approved_segment_id=v_segment.id;
    v_disposition:=case when v_segment.id is null then 'REMOVED' else coalesce(v_disposition,'NOT_SELECTED') end;
    if new.approved_segment_id is distinct from v_segment.id or new.disposition<>v_disposition
      or (v_segment.id is null and not exists(select 1 from private.bpay_next_work_segment_origin_share o
        where o.work_id=new.work_id and o.segment_key=new.segment_key)) then
      raise exception using errcode='23514',message='BPAY_NEXT_CAPTURED_SEGMENT_SCOPE_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_run_work_segment_parent' then
    select p.* into strict v_parent from private.bpay_next_run_position p where p.run_work_id=new.run_work_id and p.component_key=new.component_key;
    select s.* into strict v_captured from private.bpay_next_run_work_segment s where s.id=new.run_work_segment_id;
    select s.* into v_segment from private.bpay_next_approved_segment s where s.revision_id=new.captured_revision_id
      and s.segment_key=new.segment_key and s.component_key=new.component_key;
    select b.* into strict v_balance from private.bpay_next_work_segment_gross_balance_v1(new.work_id,new.component_key,new.segment_key) b;
    v_amount:=greatest(coalesce(v_segment.approved_source_ex_vat,0)-v_balance.realised_source_ex_vat-v_balance.active_held_source_ex_vat,0);
    v_recovery:=greatest(v_balance.realised_source_ex_vat-coalesce(v_segment.approved_source_ex_vat,0),0);
    v_overhang:=greatest(v_balance.active_held_source_ex_vat-greatest(coalesce(v_segment.approved_source_ex_vat,0)-v_balance.realised_source_ex_vat,0),0);
    if not private.bpay_next_segment_prepare_owner_v1(v_parent.run_worker_id,'RESERVE') or v_parent.segment_attribution_complete
      or (new.work_id,new.captured_revision_id,new.segment_key) is distinct from (v_captured.work_id,v_captured.captured_revision_id,v_captured.segment_key)
      or (new.approved_segment_id,new.approved_line_id,new.work_date,new.approved_source_ex_vat,new.realised_source_ex_vat,new.held_source_ex_vat)
        is distinct from (v_segment.id,v_segment.approved_line_id,v_segment.work_date,coalesce(v_segment.approved_source_ex_vat,0),
          v_balance.realised_source_ex_vat,v_balance.active_held_source_ex_vat)
      or (new.included_positive_source_ex_vat,new.excluded_positive_source_ex_vat,new.recovery_source_ex_vat,new.held_overhang_source_ex_vat)
        is distinct from (case when v_captured.disposition='INCLUDED' then v_amount else 0::numeric end,
          case when v_captured.disposition='INCLUDED' then 0::numeric else v_amount end,v_recovery,v_overhang)
      or (v_segment.id is null and not exists(select 1 from private.bpay_next_work_segment_origin_share o
        where o.work_id=new.work_id and o.component_key=new.component_key and o.segment_key=new.segment_key)) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_PARENT_ATTRIBUTION_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_work_segment_origin_share' then
    select h.* into strict v_hold from private.bpay_next_hold h where h.id=new.hold_id;
    select l.* into strict v_line from private.bpay_next_run_line l where l.id=new.run_line_id;
    select s.* into strict v_captured from private.bpay_next_run_work_segment s where s.id=new.run_work_segment_id;
    select p.* into strict v_parent from private.bpay_next_run_position p where p.run_work_id=v_captured.run_work_id and p.component_key=new.component_key;
    if not private.bpay_next_segment_prepare_owner_v1(v_line.run_worker_id,'ALLOCATE') or v_hold.status<>'ACTIVE'
      or v_hold.run_line_id<>v_line.id or not v_parent.segment_attribution_complete or v_captured.disposition<>'INCLUDED'
      or (new.work_id,new.component_key,new.segment_key) is distinct from (v_line.work_id,v_line.component_key,v_captured.segment_key)
      or v_captured.run_work_id<>v_line.run_work_id or not exists(select 1 from private.bpay_next_run_work_segment_parent p
        where p.run_work_segment_id=new.run_work_segment_id and p.component_key=new.component_key
          and p.included_positive_source_ex_vat=new.source_reserved_ex_vat)
      or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
        is distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_ORIGIN_SHARE_SCOPE_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_work_segment_effect_share' then
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=new.effect_id;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      select e.* into strict v_case_event from private.bpay_next_case_event e where e.id=v_effect.operation_item_id;
      select h.* into strict v_case_hold from private.bpay_next_case_hold h where h.id=v_case_event.operation_item_id and h.status='REALISED';
      select sum(s.reserved_source_ex_vat) into v_amount from private.bpay_next_work_segment_recovery_share s
        where s.case_hold_id=v_case_hold.id and s.work_id=new.work_id and s.component_key=new.component_key and s.segment_key=new.segment_key;
      if (new.work_id,new.component_key) is distinct from (v_effect.work_id,v_effect.component_key)
        or (new.source_disposition_ex_vat,new.target_ex_vat,new.target_vat,new.target_inc_vat)
          is distinct from (-v_amount,-v_amount,0::numeric,-v_amount) or v_amount is null
        or not exists(select 1 from private.bpay_next_job j
          join private.bpay_next_outcome_request r on r.command_id=j.command_id and r.transfer_id=v_case_event.original_transfer_id
          join private.bpay_next_transfer_member m on m.transfer_id=r.transfer_id and m.case_hold_id=v_case_hold.id
            and m.case_instruction_id=v_case_event.instruction_id and m.case_allocation_result_id=v_case_event.allocation_result_id and m.subject_kind='CASE'
          join private.bpay_next_case_capacity_use u on u.case_hold_id=v_case_hold.id and u.status='REALISED' and u.realisation_event_id=v_case_event.id
          join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
          join private.bpay_next_module_control mod on mod.id=1 and mod.active_owner='NEXT' and mod.owner_epoch=j.module_epoch
          where j.command_id=v_effect.operation_id and j.candidate_id=v_effect.candidate_id and j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')
            and j.phase='MEMBERS' and j.status='LEASED' and j.lease_until_utc>clock_timestamp()
            and v_case_event.event_kind='RECOVERED' and v_case_event.operation_id=j.command_id
            and v_case_event.case_id=v_effect.case_id and v_case_event.source_amount_ex_vat=v_case_hold.source_reserved_ex_vat
            and v_case_event.candidate_id=j.candidate_id and v_case_hold.run_worker_id=r.run_worker_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_EFFECT_SHARE_INVALID';
      end if;
      return new;
    end if;
    if (new.work_id,new.component_key) is distinct from (v_effect.work_id,v_effect.component_key)
      or v_effect.effect_kind<>'PAYROLL_SETTLED' then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_EFFECT_SHARE_SCOPE_INVALID';
    end if;
    select h.* into strict v_hold from private.bpay_next_hold h where h.id=v_effect.operation_item_id and h.status='ACTIVE';
    select s.* into strict v_origin from private.bpay_next_work_segment_origin_share s where s.hold_id=v_hold.id and s.segment_key=new.segment_key;
    if (new.work_id,new.component_key) is distinct from (v_origin.work_id,v_origin.component_key)
      or (v_hold.work_id,v_hold.component_key) is distinct from (v_effect.work_id,v_effect.component_key)
      or (new.source_disposition_ex_vat,new.target_ex_vat,new.target_vat,new.target_inc_vat)
      is distinct from (v_origin.source_reserved_ex_vat,v_origin.target_reserved_ex_vat,v_origin.target_reserved_vat,v_origin.target_reserved_inc_vat)
      or not exists(select 1 from private.bpay_next_job j
        join private.bpay_next_outcome_request r on r.command_id=j.command_id and r.transfer_id=v_effect.original_transfer_id
        join private.bpay_next_transfer t on t.id=r.transfer_id and t.original_transfer_id is null and t.return_cash_id is null
        join private.bpay_next_transfer_member m on m.transfer_id=t.id and m.run_line_id=v_hold.run_line_id and m.subject_kind='WORK'
        join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
        join private.bpay_next_module_control mod on mod.id=1 and mod.active_owner='NEXT' and mod.owner_epoch=j.module_epoch
        where j.command_id=v_effect.operation_id and j.candidate_id=v_effect.candidate_id and j.status='LEASED' and j.phase='MEMBERS'
          and j.lease_until_utc>clock_timestamp() and ((j.job_kind='CSV_SETTLEMENT' and t.execution_kind='BANK' and t.status in ('SETTLED','RETURNED'))
            or (j.job_kind='INTERNAL_SETTLEMENT' and t.execution_kind='INTERNAL_ZERO' and t.status='INTERNAL_PROCESSING'))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_EFFECT_SHARE_OWNER_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_work_segment_effect_origin' then
    select e.* into strict v_effect_share from private.bpay_next_work_segment_effect_share e where e.id=new.effect_share_id;
    select s.* into strict v_origin from private.bpay_next_work_segment_origin_share s where s.id=new.origin_share_id;
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=v_effect_share.effect_id;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      select e.* into strict v_case_event from private.bpay_next_case_event e where e.id=v_effect.operation_item_id;
      if (v_effect_share.work_id,v_effect_share.component_key,v_effect_share.segment_key)
        is distinct from (v_origin.work_id,v_origin.component_key,v_origin.segment_key)
        or new.source_disposition_ex_vat>=0 or not exists(select 1 from private.bpay_next_work_segment_recovery_share s
          where s.case_hold_id=v_case_event.operation_item_id and s.origin_share_id=v_origin.id
            and s.work_id=v_effect.work_id and s.component_key=v_effect.component_key
            and s.reserved_source_ex_vat=-new.source_disposition_ex_vat)
        or -new.source_disposition_ex_vat>coalesce((select sum(e.source_disposition_ex_vat)
          from private.bpay_next_work_segment_effect_origin e where e.origin_share_id=v_origin.id),0) then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_ORIGIN_INVALID';
      end if;
      return new;
    end if;
    if (v_effect_share.work_id,v_effect_share.component_key,v_effect_share.segment_key)
      is distinct from (v_origin.work_id,v_origin.component_key,v_origin.segment_key)
      or v_effect.operation_item_id<>v_origin.hold_id or v_effect.effect_kind<>'PAYROLL_SETTLED'
      or new.source_disposition_ex_vat<>v_origin.source_reserved_ex_vat
      or new.source_disposition_ex_vat<>v_effect_share.source_disposition_ex_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_EFFECT_ORIGIN_SCOPE_INVALID';
    end if;
    return new;
  end if;
  raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_FACT_RELATION_UNSUPPORTED';
end
$function$;
alter function private.bpay_next_segment_fact_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_fact_guard_v1() from public,anon,authenticated,service_role;

drop trigger if exists bpay_next_segment_fact_guard on private.bpay_next_segment_selection_page;
create trigger bpay_next_segment_fact_guard before insert or update or delete on private.bpay_next_segment_selection_page for each row execute function private.bpay_next_segment_fact_guard_v1();
drop trigger if exists bpay_next_segment_fact_truncate_guard on private.bpay_next_segment_selection_page;
create trigger bpay_next_segment_fact_truncate_guard before truncate on private.bpay_next_segment_selection_page for each statement execute function private.bpay_next_segment_fact_guard_v1();

drop trigger if exists bpay_next_segment_fact_guard on private.bpay_next_selected_segment;
create trigger bpay_next_segment_fact_guard before insert or update or delete on private.bpay_next_selected_segment for each row execute function private.bpay_next_segment_fact_guard_v1();
drop trigger if exists bpay_next_segment_fact_truncate_guard on private.bpay_next_selected_segment;
create trigger bpay_next_segment_fact_truncate_guard before truncate on private.bpay_next_selected_segment for each statement execute function private.bpay_next_segment_fact_guard_v1();

drop trigger if exists bpay_next_segment_fact_guard on private.bpay_next_run_work_segment;
create trigger bpay_next_segment_fact_guard before insert or update or delete on private.bpay_next_run_work_segment for each row execute function private.bpay_next_segment_fact_guard_v1();
drop trigger if exists bpay_next_segment_fact_truncate_guard on private.bpay_next_run_work_segment;
create trigger bpay_next_segment_fact_truncate_guard before truncate on private.bpay_next_run_work_segment for each statement execute function private.bpay_next_segment_fact_guard_v1();

drop trigger if exists bpay_next_segment_fact_guard on private.bpay_next_run_work_segment_parent;
create trigger bpay_next_segment_fact_guard before insert or update or delete on private.bpay_next_run_work_segment_parent for each row execute function private.bpay_next_segment_fact_guard_v1();
drop trigger if exists bpay_next_segment_fact_truncate_guard on private.bpay_next_run_work_segment_parent;
create trigger bpay_next_segment_fact_truncate_guard before truncate on private.bpay_next_run_work_segment_parent for each statement execute function private.bpay_next_segment_fact_guard_v1();

drop trigger if exists bpay_next_segment_fact_guard on private.bpay_next_work_segment_origin_share;
create trigger bpay_next_segment_fact_guard before insert or update or delete on private.bpay_next_work_segment_origin_share for each row execute function private.bpay_next_segment_fact_guard_v1();
drop trigger if exists bpay_next_segment_fact_truncate_guard on private.bpay_next_work_segment_origin_share;
create trigger bpay_next_segment_fact_truncate_guard before truncate on private.bpay_next_work_segment_origin_share for each statement execute function private.bpay_next_segment_fact_guard_v1();

drop trigger if exists bpay_next_segment_fact_guard on private.bpay_next_work_segment_effect_share;
create trigger bpay_next_segment_fact_guard before insert or update or delete on private.bpay_next_work_segment_effect_share for each row execute function private.bpay_next_segment_fact_guard_v1();
drop trigger if exists bpay_next_segment_fact_truncate_guard on private.bpay_next_work_segment_effect_share;
create trigger bpay_next_segment_fact_truncate_guard before truncate on private.bpay_next_work_segment_effect_share for each statement execute function private.bpay_next_segment_fact_guard_v1();

drop trigger if exists bpay_next_segment_fact_guard on private.bpay_next_work_segment_effect_origin;
create trigger bpay_next_segment_fact_guard before insert or update or delete on private.bpay_next_work_segment_effect_origin for each row execute function private.bpay_next_segment_fact_guard_v1();
drop trigger if exists bpay_next_segment_fact_truncate_guard on private.bpay_next_work_segment_effect_origin;
create trigger bpay_next_segment_fact_truncate_guard before truncate on private.bpay_next_work_segment_effect_origin for each statement execute function private.bpay_next_segment_fact_guard_v1();

create or replace function private.bpay_next_work_segment_debt_basis_v1(p_work_id uuid,p_component_key text,p_revision_id uuid)
returns table(segment_key text,approved_source_ex_vat numeric,realised_source_ex_vat numeric,held_source_ex_vat numeric,
  written_off_source_ex_vat numeric,active_recovery_source_ex_vat numeric,negative_source_ex_vat numeric,
  free_recovery_source_ex_vat numeric,outstanding_source_ex_vat numeric)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
  with keys as (
    select s.segment_key from private.bpay_next_approved_segment s where s.revision_id=p_revision_id and s.component_key=p_component_key
    union select s.segment_key from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id and s.component_key=p_component_key
  ), amounts as (
    select k.segment_key,coalesce(a.approved_source_ex_vat,0) a,b.realised_source_ex_vat c,b.active_held_source_ex_vat h,
      coalesce((select sum(s.written_off_source_ex_vat) from private.bpay_next_work_segment_write_off_share s
        where s.work_id=p_work_id and s.component_key=p_component_key and s.segment_key=k.segment_key),0) w,
      coalesce((select sum(s.reserved_source_ex_vat) from private.bpay_next_work_segment_recovery_share s
        join private.bpay_next_case_hold h on h.id=s.case_hold_id and h.status='ACTIVE'
        where s.work_id=p_work_id and s.component_key=p_component_key and s.segment_key=k.segment_key),0) hc
    from keys k left join private.bpay_next_approved_segment a on a.revision_id=p_revision_id
      and a.component_key=p_component_key and a.segment_key=k.segment_key
    cross join lateral private.bpay_next_work_segment_gross_balance_v1(p_work_id,p_component_key,k.segment_key) b
  ) select segment_key,a,c,h,w,hc,greatest(c-a,0),greatest(c-a-w-hc,0),greatest(c-a-w,hc,0) from amounts
$function$;
alter function private.bpay_next_work_segment_debt_basis_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_work_segment_debt_basis_v1(uuid,text,uuid) from public,anon,authenticated,service_role;

-- Full same-parent gross qualification; absence is from a genuine sealed
-- segment revision. Missing historic paid/held attribution is never zero.
create or replace function private.bpay_next_segment_collection_basis_v1(p_work_id uuid,p_component_key text,p_revision_id uuid)
returns jsonb language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select jsonb_build_object('complete',count(d.segment_key)>0 and bool_and(d.approved_source_ex_vat>=0
      and d.realised_source_ex_vat>=0) and w.work_kind='ORDINARY' and r.source_kind='ORDINARY'
      and r.source_pay_channel='PAYE' and p.source_basis_channel='PAYE' and r.sealed_at_utc is not null
      and (r.detail_kind='SHIFT' or r.certified_zero) and p.applied_revision_id=r.id
      and coalesce(sum(d.approved_source_ex_vat),0)=p.approved_source_ex_vat
      and coalesce(sum(d.realised_source_ex_vat),0)=p.realised_source_ex_vat
      and coalesce(sum(d.held_source_ex_vat),0)=p.held_source_ex_vat
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and (p.held_target_ex_vat,p.held_target_vat,p.held_target_inc_vat)
        is not distinct from (p.held_source_ex_vat,0::numeric,p.held_source_ex_vat)
      and coalesce(sum(d.written_off_source_ex_vat),0)=coalesce(c.principal_written_off,0)
      and coalesce(sum(d.active_recovery_source_ex_vat),0)=coalesce(c.active_recovery_hold_amount,0)
      and coalesce((select -sum(s.source_disposition_ex_vat)
        from private.bpay_next_work_segment_effect_share s
        where s.work_id=p_work_id and s.component_key=p_component_key and s.source_disposition_ex_vat<0),0)=coalesce(c.principal_recovered,0)
      and (coalesce(c.principal_written_off,0),coalesce(c.active_recovery_hold_amount,0),coalesce(c.principal_recovered,0))
        is not distinct from (coalesce(cx.written_off_source_ex_vat,0),coalesce(cx.active_recovery_source_ex_vat,0),
          coalesce(cx.recovered_source_ex_vat,0))
      and (a.id is null or (a.component_kind='WORK' and a.tax_treatment='TAXABLE')),
    'negative_source_ex_vat',coalesce(sum(d.negative_source_ex_vat),0),
    'outstanding_source_ex_vat',coalesce(sum(d.outstanding_source_ex_vat),0),
    'free_recovery_source_ex_vat',coalesce(sum(d.free_recovery_source_ex_vat),0))
  from private.bpay_next_position p join private.bpay_next_work w on w.id=p.work_id
  join private.bpay_next_work_revision r on r.id=p_revision_id and r.work_id=p.work_id
  left join private.bpay_next_approved_line a on a.revision_id=r.id and a.component_key=p.component_key
  left join private.bpay_next_work_collection collection on collection.work_id=p.work_id and collection.component_key=p.component_key
  left join private.bpay_next_finance_case c on c.id=collection.case_id
  left join private.bpay_next_case_component cx on cx.id=collection.case_component_id and cx.case_id=c.id
  left join lateral private.bpay_next_work_segment_debt_basis_v1(p_work_id,p_component_key,p_revision_id) d on true
  where p.work_id=p_work_id and p.component_key=p_component_key
  group by w.work_kind,r.id,r.source_kind,r.source_pay_channel,r.sealed_at_utc,r.detail_kind,r.certified_zero,
    p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat,p.held_source_ex_vat,p.source_basis_channel,
    p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat,p.held_target_ex_vat,p.held_target_vat,p.held_target_inc_vat,
    c.id,c.principal_written_off,c.active_recovery_hold_amount,c.principal_recovered,
    cx.written_off_source_ex_vat,cx.active_recovery_source_ex_vat,cx.recovered_source_ex_vat,a.id,a.component_kind,a.tax_treatment
$function$;
alter function private.bpay_next_segment_collection_basis_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_segment_collection_basis_v1(uuid,text,uuid) from public,anon,authenticated,service_role;

-- Narrow R2 exact-negative successor of supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql.
create or replace function private.bpay_next_reconcile_work_collection_v1(
  p_job_id uuid,p_work_id uuid,p_component_key text,p_effect_id uuid default null
) returns void
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;v_work private.bpay_next_work%rowtype;
  v_position private.bpay_next_position%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_original_revision private.bpay_next_work_revision%rowtype;
  v_original_approved private.bpay_next_approved_line%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_approved private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_state text;v_qualified boolean;v_capturable boolean;v_supported_paid boolean;
  v_original_detail text;v_whole_zero boolean;v_paired_origin boolean;
  v_segment_basis jsonb;v_segment_complete boolean;
  v_principal numeric;v_delta numeric;v_event text;
begin
  if p_job_id is null or p_work_id is null or p_component_key is null
     or pg_catalog.char_length(p_component_key) not between 1 and 256 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORK_COLLECTION_INPUT_INVALID';
  end if;
  -- Parent owners already hold run (when applicable), Candidate control and
  -- job, in that order. These reentrant locks never acquire another run.
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.status<>'LEASED' or v_job.module_epoch<>v_epoch
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id and w.candidate_id=v_candidate;
  select p.* into strict v_position from private.bpay_next_position p
    where p.work_id=p_work_id and p.component_key=p_component_key for update;
  if v_job.job_kind='POSITION_APPLY' then
    if p_effect_id is not null or v_job.phase not in ('NEW','REMOVED') or not exists(
      select 1 from private.bpay_next_publication p where p.command_id=v_job.command_id
        and p.work_id=p_work_id and p.candidate_id=v_candidate and p.status in ('QUEUED','APPLYING')
        and p.revision_id=v_position.applied_revision_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_POSITION_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
    if p_effect_id is null or v_job.phase<>'MEMBERS' then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SETTLEMENT_SCOPE_INVALID';
    end if;
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_effect_id;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      -- The negative WORK disposition is exactly the genuine posted CASE
      -- recovery, never a caller supplied negative or a cash-return inverse.
      if not exists(select 1 from private.bpay_next_work_collection b
        join private.bpay_next_case_event ce on ce.id=v_effect.operation_item_id and ce.case_id=b.case_id
        join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=b.id
        join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
        join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
        join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=ce.original_transfer_id
        where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
          and ce.operation_id=v_job.command_id and ce.event_kind='RECOVERED'
          and ce.case_component_id=b.case_component_id and ce.source_amount_ex_vat>0
          and v_effect.operation_id=v_job.command_id and v_effect.work_id=b.work_id
          and v_effect.component_key=b.component_key and v_effect.candidate_id=b.candidate_id
          and v_effect.case_id=b.case_id and v_effect.original_transfer_id=ce.original_transfer_id
          and (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
            is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
      end if;
      v_capturable:=false;v_supported_paid:=true;
    else
    select l.* into strict v_line from private.bpay_next_hold h
      join private.bpay_next_run_line l on l.id=h.run_line_id
      join private.bpay_next_outcome_request r on r.command_id=v_job.command_id
      join private.bpay_next_transfer t on t.id=r.transfer_id
      where h.id=v_effect.operation_item_id and h.status='REALISED'
        and l.run_worker_id=r.run_worker_id and l.work_id=p_work_id and l.component_key=p_component_key
        and t.id=v_effect.original_transfer_id and t.candidate_id=v_candidate
        and t.original_transfer_id is null and t.return_cash_id is null
        and ((v_job.job_kind='CSV_SETTLEMENT' and t.execution_kind='BANK' and t.status in ('SETTLED','RETURNED'))
          or (v_job.job_kind='INTERNAL_SETTLEMENT' and t.execution_kind='INTERNAL_ZERO' and t.status='INTERNAL_PROCESSING'));
    if v_effect.operation_id<>v_job.command_id or v_effect.effect_kind<>'PAYROLL_SETTLED'
       or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id)
         is distinct from (p_work_id,p_component_key,v_candidate)
       or v_effect.source_disposition_ex_vat<>v_line.source_consumed_ex_vat
       or v_effect.source_disposition_ex_vat<=0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EFFECT_SCOPE_INVALID';
    end if;
    select a.* into strict v_approved from private.bpay_next_approved_line a where a.id=v_line.approved_line_id;
    -- Original ordinary or genuine Source PAYE -> PAYE WORK only. NULL
    -- tax is capturable as exact UNBOUND evidence, never inferred taxable.
    -- Do not return before checking an existing link: a later unsupported
    -- actual posting invalidates its paid qualification, not the payment.
    v_capturable:=v_work.work_kind='ORDINARY' and v_approved.component_kind='WORK'
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
    -- PROTECTED is admitted only for 1550's actual committed-head branch;
    -- 0345's independent/headless protected target is not this authority.
    v_capturable:=v_capturable or (v_work.work_kind='SOURCE' and v_approved.component_kind='WORK'
      and v_approved.source_component_id is not null
      and v_approved.component_key='SOURCE:'||v_approved.source_component_id::text
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.work_id=v_work.id
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))));
    v_supported_paid:=v_capturable and v_approved.tax_treatment is not distinct from 'TAXABLE';
    end if;
  elsif v_job.job_kind='SIMPLE_CANCEL' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      join private.bpay_next_case_cancel_binding cb on cb.command_id=v_job.command_id and cb.candidate_id=b.candidate_id
      join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
      join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
      join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
      join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=b.id
      join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and cb.status='CANCELLING' and cb.stage='CASE'
        and i.case_id=b.case_id and i.case_component_id=b.case_component_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CANCEL_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARATION_EXPIRY' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_expired_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  else
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_JOB_KIND_INVALID';
  end if;
  select b.* into v_basis from private.bpay_next_work_collection b
    where b.work_id=p_work_id and b.component_key=p_component_key for update;
  if not found then
    -- An unpaid hold is not realised money. Existing pre-install paid rows
    -- have no invented basis/backfill; future actual settlement captures it.
    if p_effect_id is null or not v_capturable then return;end if;
    insert into private.bpay_next_work_collection
      (work_id,component_key,candidate_id,original_effect_id,original_run_line_id,
       original_revision_id,original_approved_line_id,original_transfer_id,
       original_source_pay_channel,original_tax_treatment,paid_basis_qualified,original_valuation_policy_id,
       qualification_state,observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat,last_job_id,last_effect_id)
      values(p_work_id,p_component_key,v_candidate,p_effect_id,v_line.id,v_line.captured_revision_id,
        v_line.approved_line_id,v_effect.original_transfer_id,v_line.source_pay_channel,v_approved.tax_treatment,
        v_approved.tax_treatment is not distinct from 'TAXABLE'
          and v_position.realised_source_ex_vat=v_effect.source_disposition_ex_vat,v_line.valuation_policy_id,
        'CURRENT_APPROVAL_UNBOUND',v_position.applied_revision_id,v_position.approved_source_ex_vat,
        v_position.realised_source_ex_vat,p_job_id,p_effect_id) returning * into v_basis;
  elsif v_basis.last_job_id=p_job_id and
      not (p_effect_id is not null and v_effect.effect_kind='WORK_COLLECTION_RECOVERED'
        and v_basis.last_effect_id is distinct from p_effect_id) then
    if (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat,v_basis.last_effect_id)
       is distinct from (v_position.applied_revision_id,v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,p_effect_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SAME_JOB_CHANGED';
    end if;
    return;
  end if;
  v_qualified:=v_basis.paid_basis_qualified;
  if p_effect_id is not null and not v_supported_paid then v_qualified:=false;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_position.applied_revision_id;
  select r.detail_kind into strict v_original_detail from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select r.* into strict v_original_revision from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select a.* into strict v_original_approved from private.bpay_next_approved_line a
    where a.id=v_basis.original_approved_line_id and a.revision_id=v_original_revision.id;
  v_paired_origin:=(v_work.work_kind='ORDINARY' and v_original_revision.source_kind='ORDINARY'
      and v_revision.source_kind='ORDINARY')
    or (v_work.work_kind='SOURCE' and v_original_approved.component_kind='WORK'
      and v_original_approved.source_component_id is not null
      and p_component_key='SOURCE:'||v_original_approved.source_component_id::text
      and ((v_original_revision.source_kind='SOURCE' and v_original_revision.source_event_id is not null
        and (v_original_revision.source_head_id is null
        or v_original_revision.source_event_id=v_original_revision.source_head_id)) or (v_original_revision.source_kind='PROTECTED'
        and v_original_revision.source_head_id is not null
        and v_original_revision.source_event_id=v_original_revision.source_head_id))
      and ((v_revision.source_kind='SOURCE' and v_revision.source_event_id is not null
        and (v_revision.source_head_id is null
        or v_revision.source_event_id=v_revision.source_head_id)) or (v_revision.source_kind='PROTECTED'
        and v_revision.source_head_id is not null and v_revision.source_event_id=v_revision.source_head_id)));
  -- Absence of a DAY key does not prove removal: an AGGREGATE or another
  -- business-date component may retain the entitlement. This first slice
  -- qualifies unchanged typed identity only. A genuinely sealed, certified
  -- whole-base empty zero is the sole missing-key exception; reconciliation
  -- across representations remains explicitly UNBOUND, not discarded.
  v_segment_basis:=private.bpay_next_segment_collection_basis_v1(p_work_id,p_component_key,v_position.applied_revision_id);
  v_segment_complete:=coalesce((v_segment_basis->>'complete')::boolean,false);
  v_whole_zero:=v_revision.sealed_at_utc is not null and v_revision.certified_zero
    and v_revision.approved_source_ex_vat=0 and v_revision.expected_line_count=0;
  if v_basis.original_tax_treatment is distinct from 'TAXABLE' then v_state:='ORIGINAL_TAX_UNBOUND';
  elsif not v_qualified then v_state:='PAID_BASIS_UNBOUND';
  elsif v_job.job_kind not in ('SIMPLE_CANCEL','PREPARATION_EXPIRY') and v_effect.effect_kind is distinct from 'WORK_COLLECTION_RECOVERED'
      and (v_work.approval_state<>'APPROVED'
        or v_work.current_revision_id is distinct from v_position.applied_revision_id) then
    v_state:='CURRENT_APPROVAL_UNBOUND';
  elsif v_paired_origin is not true or v_revision.source_pay_channel<>'PAYE'
      or v_position.source_basis_channel is distinct from 'PAYE'
      or (not v_whole_zero and not v_segment_complete and (v_revision.detail_kind<>v_original_detail
        or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_key=p_component_key
            and a.component_kind='WORK' and a.tax_treatment='TAXABLE'
            and (v_work.work_kind='ORDINARY'
              or a.source_component_id=v_original_approved.source_component_id)))) then
    v_state:='CURRENT_SOURCE_UNBOUND';
  elsif v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then v_state:='SIGNED_SOURCE_UNBOUND';
  else v_state:='READY';end if;
  if v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists (
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=v.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=v.decision_bundle_id
        and bundle.bundle_revision=v.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=v.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=v.publication_command_id_1
        and publication.work_id=v.approval_work_id and publication.revision_id=v.approval_revision_id
        and publication.candidate_id=v_candidate
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and v.approval_work_id=p_work_id and v.approval_component_key=p_component_key
        and v.approval_revision_id=v_position.applied_revision_id
        and v.head_id=v_revision.source_head_id and v_revision.source_event_id=v.head_id
        and v.publication_command_id_2 is null and v.candidate_id=v_candidate
        and v.approval_source_channel='PAYE' and v.tax_treatment='TAXABLE'
        and receipt.decision_bundle_id=v.decision_bundle_id and receipt.bundle_revision=v.bundle_revision
        and receipt.request_digest=v.request_digest and pg_catalog.cardinality(receipt.head_ids)=1
        and receipt.head_ids[1]=v.head_id and v_work.current_revision_id=v.approval_revision_id
        and ((v_job.job_kind='POSITION_APPLY' and publication.command_id=v_job.command_id
              and publication.status in ('QUEUED','APPLYING'))
          or (v_job.job_kind<>'POSITION_APPLY' and publication.status='APPLIED' and publication.phase='DONE'))
  ) then v_state:='CURRENT_SOURCE_UNBOUND';end if;
  if v_state='READY' then
    if v_basis.case_id is null then
      v_principal:=case when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric
        else greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0) end;
      if v_principal>0 then
        insert into private.bpay_next_finance_case
          (id,candidate_id,case_kind,tax_treatment,status,principal_approved,order_key,case_revision)
          values(v_basis.id,v_candidate,'OVERPAYMENT','TAXABLE','OPEN',v_principal,v_job.command_sequence,1);
        insert into private.bpay_next_case_rule
          (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc)
          values(v_basis.id,v_basis.id,v_candidate,1,'OVERPAYMENT','OVERPAYMENT','TAXABLE',pg_catalog.transaction_timestamp());
        update private.bpay_next_finance_case set current_rule_id=v_basis.id,current_rule_revision=1 where id=v_basis.id;
        insert into private.bpay_next_case_component
          (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,
           source_pay_channel,currency,approved_source_ex_vat,resolution_state)
          values(v_basis.id,v_basis.id,v_candidate,'RECOVERY',1,1,v_basis.id,
            'OVERPAYMENT','OVERPAYMENT','TAXABLE','RECOVERY','DEDUCTION','GROSS_DEDUCT','PAYE','GBP',v_principal,'REVIEW');
        v_basis.case_id:=v_basis.id;v_basis.case_component_id:=v_basis.id;v_delta:=v_principal;v_event:='APPROVED';
      end if;
    else
      select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_basis.case_id for update;
      select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_basis.case_component_id for update;
      if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
           v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BALANCE_MISMATCH';
      end if;
      -- R is actual SOURCE disposition, never held money/bank cash. Genuine
      -- future recovery must lower R with its own signed effect. Forgiveness
      -- W is separate; protected H is not repriced or released by an approval.
      v_principal:=v_case.principal_recovered+v_case.principal_written_off+
        case when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric else
        greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-v_case.principal_written_off,
          v_case.active_recovery_hold_amount,0) end;
      v_delta:=v_principal-v_case.principal_approved;
      if v_delta<>0 then
        update private.bpay_next_finance_case set principal_approved=v_principal,
          status=case when v_principal=principal_recovered+principal_written_off then 'CLOSED'
            when status='PAUSED' then 'PAUSED' else 'OPEN' end,
          case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
        update private.bpay_next_case_component set approved_source_ex_vat=v_principal,
          component_revision=component_revision+1,resolution_state='REVIEW',
          target_pay_channel=null,target_ex_vat=null,target_vat=null,target_inc_vat=null,
          valuation_policy_id=null,valuation_window_id=null,resolution_ref=null,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_component_id;
        v_event:='CORRECTED';
      elsif v_principal=v_case.principal_recovered+v_case.principal_written_off and v_case.status<>'CLOSED' then
        update private.bpay_next_finance_case set status='CLOSED',case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
      end if;
    end if;
    if v_event is not null then
      insert into private.bpay_next_case_event
        (case_id,operation_id,operation_item_id,event_kind,approved_delta,original_transfer_id,original_effect_id)
        values(v_basis.case_id,v_job.command_id,v_basis.id,v_event,v_delta,
          v_basis.original_transfer_id,v_basis.original_effect_id);
    end if;
  end if;
  -- First insert records exact basis; this single follow-up records the
  -- observation/case under the same actual owner transaction. No view counter
  -- is independently incremented: existing position/posting owners do that.
  update private.bpay_next_work_collection set case_id=v_basis.case_id,case_component_id=v_basis.case_component_id,
    paid_basis_qualified=v_qualified,qualification_state=v_state,observed_revision_id=v_position.applied_revision_id,
    observed_approved_source_ex_vat=v_position.approved_source_ex_vat,
    observed_realised_source_ex_vat=v_position.realised_source_ex_vat,
    segment_attribution_complete=v_segment_complete,
    observed_segment_negative_ex_vat=case when v_segment_complete then (v_segment_basis->>'negative_source_ex_vat')::numeric else 0 end,
    reconcile_revision=reconcile_revision+1,last_job_id=p_job_id,last_effect_id=p_effect_id,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_basis.id;
end
$function$;

alter function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) from public,anon,authenticated,service_role;

-- Narrow R2 exact-negative successor of supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql.
create or replace function private.bpay_next_work_collection_origin_v1(
  p_case_id uuid,p_component_id uuid,p_candidate_id uuid
) returns private.bpay_next_work_collection
language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
begin
  select b.* into v_basis from private.bpay_next_work_collection b
    join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
    join private.bpay_next_work_revision r on r.id=p.applied_revision_id and r.work_id=b.work_id
    join private.bpay_next_work_revision original on original.id=b.original_revision_id and original.work_id=b.work_id
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id
    join private.bpay_next_approved_line original_line on original_line.id=b.original_approved_line_id
      and original_line.revision_id=original.id and original_line.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=b.case_id and c.candidate_id=b.candidate_id
    join private.bpay_next_case_component x on x.id=b.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_case_rule rule on rule.id=x.rule_id and rule.case_id=c.id and rule.candidate_id=c.candidate_id
    where b.case_id=p_case_id and b.case_component_id=p_component_id and b.candidate_id=p_candidate_id
      and b.id=b.case_id and b.id=b.case_component_id and b.paid_basis_qualified
      and b.qualification_state='READY' and b.original_source_pay_channel='PAYE' and b.original_tax_treatment='TAXABLE'
      and (b.observed_revision_id,b.observed_approved_source_ex_vat,b.observed_realised_source_ex_vat)
        is not distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
      and p.source_basis_channel='PAYE' and p.approved_source_ex_vat>=0 and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and ((work.work_kind='ORDINARY' and original.source_kind='ORDINARY' and r.source_kind='ORDINARY')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK'
          and original_line.source_component_id is not null
          and b.component_key='SOURCE:'||original_line.source_component_id::text
          and ((original.source_kind='SOURCE' and original.source_event_id is not null
            and (original.source_head_id is null or original.source_event_id=original.source_head_id)) or (original.source_kind='PROTECTED'
            and original.source_head_id is not null and original.source_event_id=original.source_head_id))
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))))
      and r.source_pay_channel='PAYE' and r.sealed_at_utc is not null
      and ((r.detail_kind=original.detail_kind and exists(select 1 from private.bpay_next_approved_line a
        where a.revision_id=r.id and a.component_key=b.component_key and a.component_kind='WORK'
          and a.tax_treatment='TAXABLE' and a.source_pay_ex_vat=p.approved_source_ex_vat
          and (work.work_kind='ORDINARY' or a.source_component_id=original_line.source_component_id)))
        or (r.certified_zero and r.approved_source_ex_vat=0 and r.expected_line_count=0 and p.approved_source_ex_vat=0)
        or (b.segment_attribution_complete and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and (not b.segment_attribution_complete or (b.observed_segment_negative_ex_vat is not distinct from
        (private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'negative_source_ex_vat')::numeric
        and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
      and x.case_kind='OVERPAYMENT' and x.case_subtype='OVERPAYMENT' and x.tax_treatment='TAXABLE'
      and x.source_pay_channel='PAYE' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION'
      and x.payroll_stage='GROSS_DEDUCT' and x.component_key='RECOVERY' and x.component_ordinal=1
      and c.current_rule_id=rule.id and c.current_rule_revision=rule.rule_revision and rule.id=b.id
      and rule.weekly_due_source_ex_vat is null
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);
  return v_basis;
end
$function$;

alter function private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

-- Narrow R2 exact-negative successor of supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql.
create or replace function private.bpay_next_work_collection_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_DELETE_FORBIDDEN';
  end if;
  if tg_op='UPDATE' then
    if (new.id,new.work_id,new.component_key,new.candidate_id,new.original_effect_id,
        new.original_run_line_id,new.original_revision_id,new.original_approved_line_id,
        new.original_transfer_id,new.original_source_pay_channel,new.original_tax_treatment,
        new.original_valuation_policy_id,new.created_at_utc)
       is distinct from
       (old.id,old.work_id,old.component_key,old.candidate_id,old.original_effect_id,
        old.original_run_line_id,old.original_revision_id,old.original_approved_line_id,
        old.original_transfer_id,old.original_source_pay_channel,old.original_tax_treatment,
        old.original_valuation_policy_id,old.created_at_utc)
       or (old.case_id is not null and (new.case_id,new.case_component_id)
           is distinct from (old.case_id,old.case_component_id))
       or (not old.paid_basis_qualified and new.paid_basis_qualified)
       or new.reconcile_revision<>old.reconcile_revision+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BASIS_IMMUTABLE';
    end if;
  elsif new.reconcile_revision<>1 or new.case_id is not null
      or new.last_effect_id is distinct from new.original_effect_id then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_STATE_INVALID';
  end if;
  select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=new.original_effect_id;
  select l.* into strict v_line from private.bpay_next_run_line l where l.id=new.original_run_line_id;
  if tg_op='INSERT' and not exists(select 1 from private.bpay_next_job j
      where j.id=new.last_job_id and j.command_id=v_effect.operation_id
        and j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_OWNER_INVALID';
  end if;
  if v_effect.effect_kind<>'PAYROLL_SETTLED' or v_effect.source_disposition_ex_vat<=0
     or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id,v_effect.original_transfer_id)
       is distinct from (new.work_id,new.component_key,new.candidate_id,new.original_transfer_id)
     or (v_line.work_id,v_line.component_key,v_line.captured_revision_id,v_line.approved_line_id,
         v_line.source_pay_channel,v_line.valuation_policy_id)
       is distinct from (new.work_id,new.component_key,new.original_revision_id,new.original_approved_line_id,
         new.original_source_pay_channel,new.original_valuation_policy_id)
     or v_line.target_pay_channel<>'PAYE'
     or (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
       is distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
     or not exists(select 1 from private.bpay_next_hold h
       where h.id=v_effect.operation_item_id and h.run_line_id=v_line.id and h.status='REALISED'
         and h.source_reserved_ex_vat=v_effect.source_disposition_ex_vat
         and h.source_reserved_ex_vat=v_line.source_consumed_ex_vat)
     or not exists(select 1 from private.bpay_next_approved_line a
       join private.bpay_next_work_revision r on r.id=a.revision_id
       join private.bpay_next_work w on w.id=r.work_id
       where a.id=new.original_approved_line_id and a.component_kind='WORK'
         and a.tax_treatment is not distinct from new.original_tax_treatment
         and w.id=new.work_id and a.component_key=new.component_key
         and ((r.source_kind='ORDINARY' and w.work_kind='ORDINARY')
           or (w.work_kind='SOURCE' and a.source_component_id is not null
             and a.component_key='SOURCE:'||a.source_component_id::text
             and ((r.source_kind='SOURCE' and r.source_event_id is not null
               and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
               and r.source_head_id is not null and r.source_event_id=r.source_head_id)))))
     or not exists(select 1 from private.bpay_next_transfer_member m
       where m.transfer_id=new.original_transfer_id and m.run_line_id=v_line.id and m.subject_kind='WORK'
         and m.signed_cash_contribution=v_line.frozen_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_ORIGINAL_BASIS_INVALID';
  end if;
  if new.case_id is not null and
     (new.case_id<>new.id or new.case_component_id<>new.id or not exists(
       select 1 from private.bpay_next_finance_case c
       join private.bpay_next_case_component x on x.id=new.case_component_id and x.case_id=c.id
       join private.bpay_next_case_rule r on r.id=x.rule_id and r.case_id=c.id
       where c.id=new.case_id and c.candidate_id=new.candidate_id
         and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
         and c.principal_approved=x.approved_source_ex_vat
         and c.principal_recovered=x.recovered_source_ex_vat
         and c.principal_written_off=x.written_off_source_ex_vat
         and c.active_recovery_hold_amount=x.active_recovery_source_ex_vat
         and x.candidate_id=c.candidate_id and x.component_key='RECOVERY' and x.component_ordinal=1
         and x.case_kind='OVERPAYMENT' and x.tax_treatment='TAXABLE' and x.source_pay_channel='PAYE'
         and x.instruction_kind='RECOVERY' and x.payroll_stage='GROSS_DEDUCT'
         and c.current_rule_id=r.id and c.current_rule_revision=r.rule_revision
         and r.id=new.id and r.case_kind='OVERPAYMENT' and r.weekly_due_source_ex_vat is null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CASE_OWNER_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_job j
     join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
     join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT'
     where j.id=new.last_job_id and j.candidate_id=new.candidate_id
       and j.job_kind in ('POSITION_APPLY','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY')
       and j.status='LEASED' and j.module_epoch=m.owner_epoch
       and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()
       and ((j.job_kind='POSITION_APPLY' and j.phase in ('NEW','REMOVED') and new.last_effect_id is null
         and exists(select 1 from private.bpay_next_publication p where p.command_id=j.command_id
           and p.work_id=new.work_id and p.candidate_id=new.candidate_id
           and p.revision_id=new.observed_revision_id and p.status in ('QUEUED','APPLYING')))
         or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
           and exists(select 1 from private.bpay_next_financial_effect e
             join private.bpay_next_outcome_request r on r.command_id=j.command_id
             where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='PAYROLL_SETTLED'
               and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id
               and e.original_transfer_id=r.transfer_id and e.source_disposition_ex_vat>0))
         or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
           and exists(select 1 from private.bpay_next_financial_effect e
             join private.bpay_next_case_event ce on ce.id=e.operation_item_id and ce.operation_id=j.command_id
             join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=new.id
             join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
             join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
             join private.bpay_next_outcome_request r on r.command_id=j.command_id and r.transfer_id=ce.original_transfer_id
             where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='WORK_COLLECTION_RECOVERED'
               and ce.event_kind='RECOVERED' and ce.case_id=new.case_id and ce.case_component_id=new.case_component_id
               and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id
               and e.original_transfer_id=ce.original_transfer_id
               and (e.source_disposition_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)
                 is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)))
         or (j.job_kind='PREPARATION_EXPIRY' and j.phase='RELEASE' and new.last_effect_id is null
           and private.bpay_next_expired_work_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='SIMPLE_CANCEL' and j.phase='RELEASE' and new.last_effect_id is null
           and exists(select 1 from private.bpay_next_case_cancel_binding cb
             join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
             join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
             join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
             join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=new.id
             join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
             where cb.command_id=j.command_id and cb.candidate_id=new.candidate_id
               and cb.status='CANCELLING' and cb.stage='CASE'
               and i.case_id=new.case_id and i.case_component_id=new.case_component_id)))) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  if not exists(select 1 from private.bpay_next_position p
      where p.work_id=new.work_id and p.component_key=new.component_key
        and (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
          is not distinct from (new.observed_revision_id,new.observed_approved_source_ex_vat,new.observed_realised_source_ex_vat)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_OBSERVATION_INVALID';
  end if;
  if new.segment_attribution_complete is distinct from coalesce((private.bpay_next_segment_collection_basis_v1(
      new.work_id,new.component_key,new.observed_revision_id)->>'complete')::boolean,false)
    or new.observed_segment_negative_ex_vat is distinct from (case when new.segment_attribution_complete then
      (private.bpay_next_segment_collection_basis_v1(new.work_id,new.component_key,new.observed_revision_id)->>'negative_source_ex_vat')::numeric else 0::numeric end) then
    -- The original INSERT records only its native paid anchor; the same-owner
    -- follow-up supplies the qualified observation, exactly as the predecessor.
    if tg_op<>'INSERT' or new.segment_attribution_complete or new.observed_segment_negative_ex_vat<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_SEGMENT_OBSERVATION_INVALID';
    end if;
  end if;
  return new;
end
$function$;

alter function private.bpay_next_work_collection_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_collection_guard_v1() from public,anon,authenticated,service_role;

-- Exact native paid origins within one component. Whole current approval
-- limits the key's debt; retained paid shares supply deterministic attribution.
create or replace function private.bpay_next_available_segment_recovery_origins_v1(p_work_id uuid,p_component_key text,p_revision_id uuid)
returns table(origin_share_id uuid,segment_key text,available_source_ex_vat numeric)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
  with credit as (
    select o.id,o.segment_key,
      greatest(coalesce((select sum(e.source_disposition_ex_vat) from private.bpay_next_work_segment_effect_origin e where e.origin_share_id=o.id),0)
        -coalesce((select sum(w.written_off_source_ex_vat) from private.bpay_next_work_segment_write_off_share w where w.origin_share_id=o.id),0)
        -coalesce((select sum(s.reserved_source_ex_vat) from private.bpay_next_work_segment_recovery_share s
          join private.bpay_next_case_hold h on h.id=s.case_hold_id and h.status='ACTIVE' where s.origin_share_id=o.id),0),0) available
    from private.bpay_next_work_segment_origin_share o join private.bpay_next_hold h on h.id=o.hold_id and h.status='REALISED'
    where o.work_id=p_work_id and o.component_key=p_component_key
  ), prefixes as (
    select c.*,coalesce(sum(c.available) over(partition by c.segment_key order by c.id rows between unbounded preceding and 1 preceding),0) earlier
      from credit c where c.available>0
  ) select c.id,c.segment_key,least(c.available,greatest(d.free_recovery_source_ex_vat-c.earlier,0))
    from prefixes c join private.bpay_next_work_segment_debt_basis_v1(p_work_id,p_component_key,p_revision_id) d on d.segment_key=c.segment_key
    where least(c.available,greatest(d.free_recovery_source_ex_vat-c.earlier,0))>0
$function$;
alter function private.bpay_next_available_segment_recovery_origins_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_available_segment_recovery_origins_v1(uuid,text,uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_segment_recovery_reservation_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_instruction private.bpay_next_run_case_instruction%rowtype;v_basis private.bpay_next_work_collection%rowtype;
  v_result private.bpay_next_case_allocation_result%rowtype;v_total numeric;
begin
  if new.instruction_id is null then return new;end if;
  select i.* into strict v_instruction from private.bpay_next_run_case_instruction i where i.id=new.instruction_id;
  if v_instruction.work_collection_id is null then return new;end if;
  select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=v_instruction.work_collection_id;
  if not v_basis.segment_attribution_complete then return new;end if;
  v_basis:=private.bpay_next_work_collection_origin_v1(new.case_id,new.case_component_id,new.candidate_id);
  select r.* into strict v_result from private.bpay_next_case_allocation_result r where r.id=new.allocation_result_id;
  if v_basis.id is distinct from v_instruction.work_collection_id or new.status<>'ACTIVE' or new.purpose<>'GROSS_RECOVERY'
    or new.allocation_pass_kind<>'DRAFT' or v_result.pass_kind<>'DRAFT'
    or not private.bpay_next_segment_prepare_owner_v1(new.run_worker_id,'FINANCE_ALLOCATE')
    or (new.run_worker_id,new.candidate_id,new.instruction_id,new.case_id,new.case_component_id)
      is distinct from (v_instruction.run_worker_id,v_instruction.candidate_id,v_instruction.id,v_instruction.case_id,v_instruction.case_component_id)
    or (new.source_reserved_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat)
      is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat)
    or (new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat)
      is distinct from (new.source_reserved_ex_vat,0::numeric,new.source_reserved_ex_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_RESERVATION_SCOPE_INVALID';
  end if;
  insert into private.bpay_next_work_segment_recovery_share(case_hold_id,origin_share_id,work_collection_id,work_id,
    component_key,segment_key,captured_revision_id,reserved_source_ex_vat)
  with origins as (
    select o.*,coalesce(sum(o.available_source_ex_vat) over(order by o.segment_key,o.origin_share_id
      rows between unbounded preceding and 1 preceding),0) earlier
    from private.bpay_next_available_segment_recovery_origins_v1(v_basis.work_id,v_basis.component_key,v_basis.observed_revision_id) o
  ) select new.id,o.origin_share_id,v_basis.id,v_basis.work_id,v_basis.component_key,o.segment_key,v_basis.observed_revision_id,
    least(o.available_source_ex_vat,greatest(new.source_reserved_ex_vat-o.earlier,0)) from origins o
    where least(o.available_source_ex_vat,greatest(new.source_reserved_ex_vat-o.earlier,0))>0;
  select sum(s.reserved_source_ex_vat) into v_total from private.bpay_next_work_segment_recovery_share s where s.case_hold_id=new.id;
  if v_total is distinct from new.source_reserved_ex_vat then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_RESERVATION_INCOMPLETE';
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_recovery_reservation_v1() owner to postgres;
revoke all on function private.bpay_next_segment_recovery_reservation_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_recovery_reservation on private.bpay_next_case_hold;
create trigger bpay_next_segment_recovery_reservation after insert on private.bpay_next_case_hold
  for each row execute function private.bpay_next_segment_recovery_reservation_v1();

-- The native0316 negative effect is posted after genuine Case realisation.
create or replace function private.bpay_next_segment_recovery_effect_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_event private.bpay_next_case_event%rowtype;v_basis private.bpay_next_work_collection%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;v_total numeric;
begin
  if new.effect_kind<>'WORK_COLLECTION_RECOVERED' then return new;end if;
  select b.* into strict v_basis from private.bpay_next_work_collection b where b.work_id=new.work_id and b.component_key=new.component_key;
  select e.* into strict v_event from private.bpay_next_case_event e where e.id=new.operation_item_id;
  if not exists(select 1 from private.bpay_next_work_segment_recovery_share s
    where s.case_hold_id=v_event.operation_item_id) then return new;end if;
  select h.* into strict v_hold from private.bpay_next_case_hold h where h.id=v_event.operation_item_id and h.status='REALISED';
  if v_event.event_kind<>'RECOVERED' or (v_event.case_id,v_event.case_component_id,v_event.operation_id,v_event.original_transfer_id)
      is distinct from (v_basis.case_id,v_basis.case_component_id,new.operation_id,new.original_transfer_id)
    or (new.source_disposition_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat)
      is distinct from (-v_event.source_amount_ex_vat,-v_event.target_amount_ex_vat,-v_event.target_amount_vat,-v_event.target_amount_inc_vat)
    or not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=v_hold.id
      and u.status='REALISED' and u.realisation_event_id=v_event.id) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_EFFECT_SCOPE_INVALID';
  end if;
  insert into private.bpay_next_work_segment_effect_share(effect_id,work_id,component_key,segment_key,
    source_disposition_ex_vat,target_ex_vat,target_vat,target_inc_vat)
  select new.id,s.work_id,s.component_key,s.segment_key,-sum(s.reserved_source_ex_vat),-sum(s.reserved_source_ex_vat),0,-sum(s.reserved_source_ex_vat)
    from private.bpay_next_work_segment_recovery_share s where s.case_hold_id=v_hold.id group by s.work_id,s.component_key,s.segment_key;
  insert into private.bpay_next_work_segment_effect_origin(effect_share_id,origin_share_id,source_disposition_ex_vat)
  select e.id,s.origin_share_id,-s.reserved_source_ex_vat from private.bpay_next_work_segment_recovery_share s
    join private.bpay_next_work_segment_effect_share e on e.effect_id=new.id and e.segment_key=s.segment_key
    where s.case_hold_id=v_hold.id;
  select sum(e.source_disposition_ex_vat) into v_total from private.bpay_next_work_segment_effect_share e where e.effect_id=new.id;
  if (v_total,v_total,0::numeric,v_total) is distinct from
    (new.source_disposition_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_EFFECT_INCOMPLETE';
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_recovery_effect_v1() owner to postgres;
revoke all on function private.bpay_next_segment_recovery_effect_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_recovery_effect on private.bpay_next_financial_effect;
create trigger bpay_next_segment_recovery_effect after insert on private.bpay_next_financial_effect
  for each row execute function private.bpay_next_segment_recovery_effect_v1();

create or replace function private.bpay_next_segment_write_off_membership_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_request private.bpay_next_write_off_request%rowtype;v_basis private.bpay_next_work_collection%rowtype;v_total numeric;
begin
  if new.event_kind<>'WRITTEN_OFF' then return new;end if;
  select r.* into v_request from private.bpay_next_write_off_request r where r.command_id=new.operation_id;
  if not found then return new;end if;
  select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=v_request.work_collection_id;
  if not v_basis.segment_attribution_complete then return new;end if;
  if (new.id,new.case_id,new.operation_item_id) is distinct from (v_request.command_id,v_request.case_id,v_basis.id)
    or new.written_off_delta<=0 or v_request.status<>'REQUESTED' or not exists(
      select 1 from private.bpay_next_job j join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
      join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
      join private.bpay_next_finance_case fc on fc.id=v_basis.case_id and fc.case_revision=v_request.basis_case_revision
      join private.bpay_next_case_component x on x.id=v_basis.case_component_id and x.component_revision=v_request.expected_component_revision
      where j.command_id=new.operation_id and j.candidate_id=v_basis.candidate_id and j.job_kind='CASE_WRITE_OFF'
        and j.status='LEASED' and j.lease_until_utc>clock_timestamp()) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_WRITE_OFF_OWNER_INVALID';
  end if;
  insert into private.bpay_next_work_segment_write_off_share(case_event_id,origin_share_id,work_collection_id,work_id,
    component_key,segment_key,written_off_source_ex_vat)
  with origins as (
    select o.*,coalesce(sum(o.available_source_ex_vat) over(order by o.segment_key,o.origin_share_id
      rows between unbounded preceding and 1 preceding),0) earlier
    from private.bpay_next_available_segment_recovery_origins_v1(v_basis.work_id,v_basis.component_key,v_basis.observed_revision_id) o
  ) select new.id,o.origin_share_id,v_basis.id,v_basis.work_id,v_basis.component_key,o.segment_key,
    least(o.available_source_ex_vat,greatest(new.written_off_delta-o.earlier,0)) from origins o
    where least(o.available_source_ex_vat,greatest(new.written_off_delta-o.earlier,0))>0;
  select sum(s.written_off_source_ex_vat) into v_total from private.bpay_next_work_segment_write_off_share s where s.case_event_id=new.id;
  if v_total is distinct from new.written_off_delta then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_WRITE_OFF_ORIGINS_INCOMPLETE';
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_write_off_membership_v1() owner to postgres;
revoke all on function private.bpay_next_segment_write_off_membership_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_write_off_membership on private.bpay_next_case_event;
create trigger bpay_next_segment_write_off_membership after insert on private.bpay_next_case_event
  for each row execute function private.bpay_next_segment_write_off_membership_v1();

create or replace function private.bpay_next_segment_capture_progress_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_total record;v_cursor record;
begin
  if tg_op not in ('INSERT','UPDATE') then raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_CAPTURE_PROGRESS_IMMUTABLE';end if;
  if not private.bpay_next_segment_prepare_owner_v1(new.run_worker_id,'RESERVE') then
    raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_CAPTURE_OWNER_STALE';
  end if;
  select coalesce(sum(p.segment_expected_member_count),0) expected,coalesce(sum(p.segment_member_count),0) processed
    into v_total from private.bpay_next_run_position p where p.run_worker_id=new.run_worker_id;
  select p.work_id,p.component_key,p.segment_key into v_cursor from private.bpay_next_run_work_segment_parent p
    join private.bpay_next_run_work rw on rw.id=p.run_work_id where rw.run_worker_id=new.run_worker_id
    order by p.work_id desc,p.component_key desc,p.segment_key desc limit 1;
  if new.expected_count<>v_total.expected or new.processed_count<>v_total.processed
    or (new.cursor_work_id,new.cursor_component_key,new.cursor_segment_key)
      is distinct from (v_cursor.work_id,v_cursor.component_key,v_cursor.segment_key)
    or (new.complete and exists(select 1 from private.bpay_next_run_position p
      where p.run_worker_id=new.run_worker_id and not p.segment_attribution_complete))
    or (tg_op='INSERT' and (new.processed_count<>0 or new.complete)) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_PROGRESS_INVALID';
  end if;
  if tg_op='UPDATE' and (old.complete or new.run_worker_id<>old.run_worker_id or new.expected_count<>old.expected_count
    or new.processed_count<old.processed_count or new.processed_count>old.processed_count+100) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_PROGRESS_INVALID';
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_capture_progress_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_capture_progress_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_capture_progress_guard on private.bpay_next_run_segment_capture;
create trigger bpay_next_segment_capture_progress_guard before insert or update or delete on private.bpay_next_run_segment_capture
  for each row execute function private.bpay_next_segment_capture_progress_guard_v1();
drop trigger if exists bpay_next_segment_capture_progress_truncate_guard on private.bpay_next_run_segment_capture;
create trigger bpay_next_segment_capture_progress_truncate_guard before truncate on private.bpay_next_run_segment_capture
  for each statement execute function private.bpay_next_segment_capture_progress_guard_v1();

create or replace function private.bpay_next_segment_position_capture_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_expected bigint;v_snapshot jsonb;v_run private.bpay_next_pay_run%rowtype;
begin
  if tg_op='UPDATE' then
    if (new.segment_expected_member_count,new.snooze_id,new.snooze_version,new.snooze_excluded_positive_ex_vat)
      is distinct from (old.segment_expected_member_count,old.snooze_id,old.snooze_version,old.snooze_excluded_positive_ex_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_BOUNDARY_IMMUTABLE';
    end if;
    return new;
  end if;
  if not private.bpay_next_segment_prepare_owner_v1(new.run_worker_id,'RESERVE') then
    raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_CAPTURE_OWNER_STALE';
  end if;
  select count(*) into v_expected from (
    select s.segment_key from private.bpay_next_approved_segment s where s.revision_id=new.captured_revision_id and s.component_key=new.component_key
    union select s.segment_key from private.bpay_next_work_segment_origin_share s where s.work_id=new.work_id and s.component_key=new.component_key) members;
  select r.* into strict v_run from private.bpay_next_pay_run r join private.bpay_next_run_worker w on w.run_id=r.id where w.id=new.run_worker_id;
  if v_expected=0 then v_snapshot:=private.bpay_next_snooze_at_cut_v1(new.work_id,new.captured_revision_id,null,new.component_key,
    v_run.snooze_control_sequence,v_run.snooze_business_date);end if;
  if new.segment_expected_member_count<>v_expected or new.segment_member_count<>0
    or new.segment_attribution_complete is distinct from (v_expected=0)
    or (new.segment_included_positive_ex_vat,new.segment_excluded_positive_ex_vat,new.segment_recovery_ex_vat,new.segment_held_overhang_ex_vat)
      is distinct from (0::numeric,0::numeric,0::numeric,0::numeric)
    or (new.snooze_id,new.snooze_version,new.snooze_excluded_positive_ex_vat)
      is distinct from ((v_snapshot->>'id')::uuid,(v_snapshot->>'version')::bigint,
        case when v_snapshot is null then 0::numeric else greatest(new.residual_source_ex_vat,0) end) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_POSITION_CAPTURE_SCOPE_INVALID';
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_position_capture_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_position_capture_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists a_bpay_next_segment_position_capture_guard on private.bpay_next_run_position;
create trigger a_bpay_next_segment_position_capture_guard before insert or update on private.bpay_next_run_position
  for each row execute function private.bpay_next_segment_position_capture_guard_v1();

-- Attribute genuine native Case facts without adding a financial owner.
create or replace function private.bpay_next_segment_negative_fact_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_origin private.bpay_next_work_segment_origin_share%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_result private.bpay_next_case_allocation_result%rowtype;
  v_event private.bpay_next_case_event%rowtype;
  v_request private.bpay_next_write_off_request%rowtype;
  v_available numeric;v_amount numeric;
begin
  if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_FACT_IMMUTABLE';end if;
  select o.* into strict v_origin from private.bpay_next_work_segment_origin_share o where o.id=new.origin_share_id;
  select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=new.work_collection_id;
  if not v_basis.segment_attribution_complete or v_basis.qualification_state<>'READY'
    or (new.work_id,new.component_key,new.segment_key) is distinct from (v_origin.work_id,v_origin.component_key,v_origin.segment_key)
    or (new.work_id,new.component_key) is distinct from (v_basis.work_id,v_basis.component_key) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_NEGATIVE_SCOPE_INVALID';
  end if;
  if tg_table_name='bpay_next_work_segment_recovery_share' then
    select h.* into strict v_hold from private.bpay_next_case_hold h where h.id=new.case_hold_id;
    select i.* into strict v_instruction from private.bpay_next_run_case_instruction i where i.id=v_hold.instruction_id;
    select r.* into strict v_result from private.bpay_next_case_allocation_result r where r.id=v_hold.allocation_result_id;
    v_amount:=new.reserved_source_ex_vat;
    if v_hold.status<>'ACTIVE' or v_hold.purpose<>'GROSS_RECOVERY' or v_hold.allocation_pass_kind<>'DRAFT'
      or not private.bpay_next_segment_prepare_owner_v1(v_hold.run_worker_id,'FINANCE_ALLOCATE')
      or new.captured_revision_id<>v_basis.observed_revision_id
      or (v_hold.case_id,v_hold.case_component_id,v_hold.candidate_id,v_instruction.work_collection_id)
        is distinct from (v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id,v_basis.id)
      or (v_result.instruction_id,v_result.run_worker_id,v_result.candidate_id,v_result.pass_kind,v_result.hold_purpose)
        is distinct from (v_instruction.id,v_hold.run_worker_id,v_basis.candidate_id,'DRAFT'::text,'GROSS_RECOVERY'::text)
      or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
        is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_source_ex_vat,0::numeric,v_result.allocated_source_ex_vat)
      or v_amount>v_hold.source_reserved_ex_vat-coalesce((select sum(s.reserved_source_ex_vat)
        from private.bpay_next_work_segment_recovery_share s where s.case_hold_id=v_hold.id),0) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_SHARE_OWNER_INVALID';
    end if;
  elsif tg_table_name='bpay_next_work_segment_write_off_share' then
    select e.* into strict v_event from private.bpay_next_case_event e where e.id=new.case_event_id;
    select r.* into strict v_request from private.bpay_next_write_off_request r where r.command_id=v_event.operation_id;
    v_amount:=new.written_off_source_ex_vat;
    if v_event.event_kind<>'WRITTEN_OFF' or v_request.status<>'REQUESTED'
      or (v_event.id,v_event.case_id,v_event.operation_item_id,v_request.work_collection_id,v_request.case_component_id)
        is distinct from (v_request.command_id,v_basis.case_id,v_basis.id,v_basis.id,v_basis.case_component_id)
      or not private.bpay_next_write_off_owner_v1(v_request.command_id,v_basis.candidate_id)
      or not exists(select 1 from private.bpay_next_finance_case fc
        join private.bpay_next_case_component x on x.id=v_basis.case_component_id and x.case_id=fc.id
        where fc.id=v_basis.case_id and fc.case_revision=v_request.basis_case_revision
          and x.component_revision=v_request.expected_component_revision)
      or v_amount>v_event.written_off_delta-coalesce((select sum(s.written_off_source_ex_vat)
        from private.bpay_next_work_segment_write_off_share s where s.case_event_id=v_event.id),0) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_WRITE_OFF_SHARE_OWNER_INVALID';
    end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_NEGATIVE_RELATION_UNSUPPORTED';end if;
  select o.available_source_ex_vat into v_available
    from private.bpay_next_available_segment_recovery_origins_v1(v_basis.work_id,v_basis.component_key,v_basis.observed_revision_id) o
    where o.origin_share_id=v_origin.id;
  if v_available is null or v_amount>v_available then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_ORIGIN_CAPACITY_EXCEEDED';
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_negative_fact_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_negative_fact_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_negative_fact_guard on private.bpay_next_work_segment_recovery_share;
create trigger bpay_next_segment_negative_fact_guard before insert or update or delete on private.bpay_next_work_segment_recovery_share for each row execute function private.bpay_next_segment_negative_fact_guard_v1();
drop trigger if exists bpay_next_segment_negative_truncate_guard on private.bpay_next_work_segment_recovery_share;
create trigger bpay_next_segment_negative_truncate_guard before truncate on private.bpay_next_work_segment_recovery_share for each statement execute function private.bpay_next_segment_negative_fact_guard_v1();

drop trigger if exists bpay_next_segment_negative_fact_guard on private.bpay_next_work_segment_write_off_share;
create trigger bpay_next_segment_negative_fact_guard before insert or update or delete on private.bpay_next_work_segment_write_off_share for each row execute function private.bpay_next_segment_negative_fact_guard_v1();
drop trigger if exists bpay_next_segment_negative_truncate_guard on private.bpay_next_work_segment_write_off_share;
create trigger bpay_next_segment_negative_truncate_guard before truncate on private.bpay_next_work_segment_write_off_share for each statement execute function private.bpay_next_segment_negative_fact_guard_v1();

-- Native owners finish counters after their INSERT callbacks. Check the
-- completed membership at commit without reacquiring money-owner locks.
create or replace function private.bpay_next_segment_membership_commit_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_origin_id uuid;v_effect_id uuid;v_effect_share_id uuid;v_collection_id uuid;
  v_origin private.bpay_next_work_segment_origin_share%rowtype;
  v_hold private.bpay_next_hold%rowtype;v_case_hold private.bpay_next_case_hold%rowtype;
  v_effect private.bpay_next_financial_effect%rowtype;
  v_share private.bpay_next_work_segment_effect_share%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;
  v_event private.bpay_next_case_event%rowtype;v_request private.bpay_next_write_off_request%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_totals record;v_credit numeric;v_written_off numeric;v_active numeric;v_recovered numeric;
begin
  if tg_table_name='bpay_next_work_segment_origin_share' then
    v_origin_id:=new.id;
  elsif tg_table_name='bpay_next_work_segment_effect_share' then
    v_effect_id:=new.effect_id;v_effect_share_id:=new.id;
  elsif tg_table_name='bpay_next_work_segment_effect_origin' then
    v_origin_id:=new.origin_share_id;v_effect_share_id:=new.effect_share_id;
    select s.effect_id into strict v_effect_id from private.bpay_next_work_segment_effect_share s where s.id=v_effect_share_id;
  elsif tg_table_name='bpay_next_work_segment_recovery_share' then
    v_origin_id:=new.origin_share_id;v_collection_id:=new.work_collection_id;
    select h.* into strict v_case_hold from private.bpay_next_case_hold h where h.id=new.case_hold_id;
  elsif tg_table_name='bpay_next_work_segment_write_off_share' then
    v_origin_id:=new.origin_share_id;v_collection_id:=new.work_collection_id;
    select e.* into strict v_event from private.bpay_next_case_event e where e.id=new.case_event_id;
    select r.* into strict v_request from private.bpay_next_write_off_request r where r.command_id=v_event.operation_id;
    select sum(s.written_off_source_ex_vat) amount into v_totals
      from private.bpay_next_work_segment_write_off_share s where s.case_event_id=v_event.id;
    if (v_totals.amount,v_request.applied_amount,v_request.event_id,v_request.status)
      is distinct from (v_event.written_off_delta,v_event.written_off_delta,v_event.id,'WRITTEN_OFF'::text)
      or not exists(select 1 from private.bpay_next_command c join private.bpay_next_job j on j.command_id=c.id
        where c.id=v_request.command_id and c.status='COMPLETE' and j.status='DONE' and j.phase='DONE') then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_WRITE_OFF_MEMBERSHIP_INCOMPLETE';
    end if;
  elsif tg_table_name='bpay_next_case_hold' then
    select s.work_collection_id into v_collection_id from private.bpay_next_work_segment_recovery_share s
      where s.case_hold_id=new.id limit 1;
    if v_collection_id is null then return new;end if;
    select h.* into strict v_case_hold from private.bpay_next_case_hold h where h.id=new.id;
  else raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_MEMBERSHIP_RELATION_UNSUPPORTED';end if;
  if v_case_hold.id is not null then
    select sum(s.reserved_source_ex_vat) amount into v_totals
      from private.bpay_next_work_segment_recovery_share s where s.case_hold_id=v_case_hold.id;
    if v_totals.amount is distinct from v_case_hold.source_reserved_ex_vat
      or not exists(select 1 from private.bpay_next_case_capacity_use u
        join private.bpay_next_case_allocation_result r on r.id=v_case_hold.allocation_result_id
        where u.case_hold_id=v_case_hold.id and u.status=v_case_hold.status
          and u.source_amount_ex_vat=v_case_hold.source_reserved_ex_vat
          and r.instruction_id=v_case_hold.instruction_id and r.pass_kind='DRAFT'
          and (r.allocated_source_ex_vat,r.allocated_target_ex_vat,r.allocated_target_vat,r.allocated_target_inc_vat)
            is not distinct from (v_case_hold.source_reserved_ex_vat,v_case_hold.target_amount_ex_vat,
              v_case_hold.target_amount_vat,v_case_hold.target_amount_inc_vat)) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_MEMBERSHIP_INCOMPLETE';
    end if;
    if v_case_hold.status='REALISED' and not exists(select 1 from private.bpay_next_case_capacity_use u
      join private.bpay_next_case_event e on e.id=u.realisation_event_id and e.event_kind='RECOVERED'
      join private.bpay_next_financial_effect f on f.operation_item_id=e.id and f.effect_kind='WORK_COLLECTION_RECOVERED'
      where u.case_hold_id=v_case_hold.id and e.operation_item_id=v_case_hold.id
        and f.source_disposition_ex_vat=-v_case_hold.source_reserved_ex_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_RECOVERY_EFFECT_MISSING';
    end if;
  end if;
  if v_origin_id is not null then
    select o.* into strict v_origin from private.bpay_next_work_segment_origin_share o where o.id=v_origin_id;
    select h.* into strict v_hold from private.bpay_next_hold h where h.id=v_origin.hold_id;
    select sum(s.source_reserved_ex_vat) source,sum(s.target_reserved_ex_vat) ex,
      sum(s.target_reserved_vat) vat,sum(s.target_reserved_inc_vat) inc into v_totals
      from private.bpay_next_work_segment_origin_share s where s.hold_id=v_hold.id;
    if (v_totals.source,v_totals.ex,v_totals.vat,v_totals.inc) is distinct from
      (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_ORIGIN_MEMBERSHIP_INCOMPLETE';
    end if;
    if v_hold.status='REALISED' then
      select coalesce(sum(e.source_disposition_ex_vat),0) into v_credit
        from private.bpay_next_work_segment_effect_origin e where e.origin_share_id=v_origin.id;
      select coalesce(sum(s.written_off_source_ex_vat),0) into v_written_off
        from private.bpay_next_work_segment_write_off_share s where s.origin_share_id=v_origin.id;
      select coalesce(sum(s.reserved_source_ex_vat),0) into v_active
        from private.bpay_next_work_segment_recovery_share s join private.bpay_next_case_hold h on h.id=s.case_hold_id and h.status='ACTIVE'
        where s.origin_share_id=v_origin.id;
      if v_credit<v_written_off+v_active or v_credit>v_origin.source_reserved_ex_vat then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_ORIGIN_OVERCONSUMED';
      end if;
    end if;
  end if;
  if v_effect_share_id is not null then
    select s.* into strict v_share from private.bpay_next_work_segment_effect_share s where s.id=v_effect_share_id;
    select sum(o.source_disposition_ex_vat) source into v_totals
      from private.bpay_next_work_segment_effect_origin o where o.effect_share_id=v_share.id;
    if v_totals.source is distinct from v_share.source_disposition_ex_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_EFFECT_ORIGIN_INCOMPLETE';
    end if;
  end if;
  if v_effect_id is not null then
    select f.* into strict v_effect from private.bpay_next_financial_effect f where f.id=v_effect_id;
    select sum(s.source_disposition_ex_vat) source,sum(s.target_ex_vat) ex,sum(s.target_vat) vat,sum(s.target_inc_vat) inc into v_totals
      from private.bpay_next_work_segment_effect_share s where s.effect_id=v_effect.id;
    if (v_totals.source,v_totals.ex,v_totals.vat,v_totals.inc) is distinct from
      (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_EFFECT_MEMBERSHIP_INCOMPLETE';
    end if;
    if v_effect.effect_kind='PAYROLL_SETTLED' and not exists(select 1 from private.bpay_next_hold h
      where h.id=v_effect.operation_item_id and h.status='REALISED') then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_PAYROLL_HOLD_NOT_REALISED';
    end if;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      select b.id into strict v_collection_id from private.bpay_next_work_collection b
        where b.work_id=v_effect.work_id and b.component_key=v_effect.component_key;
    end if;
  end if;
  if v_collection_id is not null then
    select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=v_collection_id;
    select x.* into strict v_component from private.bpay_next_case_component x where x.id=v_basis.case_component_id;
    select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_basis.case_id;
    select coalesce(sum(s.written_off_source_ex_vat),0) into v_written_off
      from private.bpay_next_work_segment_write_off_share s where s.work_collection_id=v_basis.id;
    select coalesce(sum(s.reserved_source_ex_vat),0) into v_active
      from private.bpay_next_work_segment_recovery_share s join private.bpay_next_case_hold h on h.id=s.case_hold_id and h.status='ACTIVE'
      where s.work_collection_id=v_basis.id;
    select -coalesce(sum(s.source_disposition_ex_vat),0) into v_recovered
      from private.bpay_next_work_segment_effect_share s
      where s.work_id=v_basis.work_id and s.component_key=v_basis.component_key and s.source_disposition_ex_vat<0;
    if (v_written_off,v_active,v_recovered) is distinct from
      (v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat,v_component.recovered_source_ex_vat)
      or (v_written_off,v_active,v_recovered) is distinct from
      (v_case.principal_written_off,v_case.active_recovery_hold_amount,v_case.principal_recovered) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CASE_MEMBERSHIP_INCOMPLETE';
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_segment_membership_commit_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_membership_commit_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_segment_membership_commit_guard on private.bpay_next_work_segment_origin_share;
create constraint trigger bpay_next_segment_membership_commit_guard after insert on private.bpay_next_work_segment_origin_share deferrable initially deferred for each row execute function private.bpay_next_segment_membership_commit_guard_v1();

drop trigger if exists bpay_next_segment_membership_commit_guard on private.bpay_next_work_segment_effect_share;
create constraint trigger bpay_next_segment_membership_commit_guard after insert on private.bpay_next_work_segment_effect_share deferrable initially deferred for each row execute function private.bpay_next_segment_membership_commit_guard_v1();

drop trigger if exists bpay_next_segment_membership_commit_guard on private.bpay_next_work_segment_effect_origin;
create constraint trigger bpay_next_segment_membership_commit_guard after insert on private.bpay_next_work_segment_effect_origin deferrable initially deferred for each row execute function private.bpay_next_segment_membership_commit_guard_v1();

drop trigger if exists bpay_next_segment_membership_commit_guard on private.bpay_next_work_segment_recovery_share;
create constraint trigger bpay_next_segment_membership_commit_guard after insert on private.bpay_next_work_segment_recovery_share deferrable initially deferred for each row execute function private.bpay_next_segment_membership_commit_guard_v1();

drop trigger if exists bpay_next_segment_membership_commit_guard on private.bpay_next_work_segment_write_off_share;
create constraint trigger bpay_next_segment_membership_commit_guard after insert on private.bpay_next_work_segment_write_off_share deferrable initially deferred for each row execute function private.bpay_next_segment_membership_commit_guard_v1();
drop trigger if exists bpay_next_segment_case_hold_commit_guard on private.bpay_next_case_hold;
create constraint trigger bpay_next_segment_case_hold_commit_guard after insert or update on private.bpay_next_case_hold
  deferrable initially deferred for each row execute function private.bpay_next_segment_membership_commit_guard_v1();

commit;
