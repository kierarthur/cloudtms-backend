-- Captured case facts are immutable; mutable capacity is a separate fact.
-- Owner-only additive guards. This file is not the allocation/posting owner.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_case_selection_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_run_status text;v_page private.bpay_next_case_selection_page%rowtype;
  v_count integer;v_selected integer;v_min_item integer;v_max_item integer;
  v_min_selection bigint;v_max_selection bigint;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_DELETE_FORBIDDEN';
  end if;
  select status into strict v_run_status from private.bpay_next_pay_run
    where id=new.run_id for share;
  if v_run_status<>'PREPARING' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_RUN_NOT_PREPARING';
  end if;
  if tg_op='INSERT' then
    if new.status<>'OPEN' or new.page_count<>0 or new.item_count<>0
       or new.selected_count<>0 or new.excluded_count<>0 or new.sealed_at_utc is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_DIRECT_SEAL_FORBIDDEN';
    end if;
    return new;
  end if;
  if old.status='SEALED' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_SEALED_IMMUTABLE';
  end if;
  if (new.run_id,new.candidate_id,new.selection_revision)
     is distinct from (old.run_id,old.candidate_id,old.selection_revision)
     or new.page_count<old.page_count or new.item_count<old.item_count
     or new.selected_count<old.selected_count or new.excluded_count<old.excluded_count then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_IDENTITY_OR_COUNT_CHANGED';
  end if;
  if new.status='SEALED' and
     (new.page_count,new.item_count,new.selected_count,new.excluded_count)
       is distinct from (old.page_count,old.item_count,old.selected_count,old.excluded_count) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_SEAL_COUNT_CHANGED';
  end if;
  if new.status='SEALED' and exists(select 1 from private.bpay_next_case_selection_page
      where run_id=old.run_id and candidate_id=old.candidate_id
        and selection_revision=old.selection_revision and page_no=old.page_count+1) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_UNCOUNTED_PAGE';
  end if;
  if new.status='OPEN' then
    if new.page_count<>old.page_count+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_PAGE_COUNT_INVALID';
    end if;
    select * into strict v_page from private.bpay_next_case_selection_page
      where run_id=old.run_id and candidate_id=old.candidate_id
        and selection_revision=old.selection_revision and page_no=new.page_count;
    -- Only this exact <=100-row page is checked. Final seal trusts its
    -- transactionally maintained counters, never recounts the entire run.
    select count(*)::integer,count(*) filter(where is_selected)::integer,
      min(item_no),max(item_no),min(selection_no),max(selection_no)
      into v_count,v_selected,v_min_item,v_max_item,v_min_selection,v_max_selection
      from private.bpay_next_case_selection_item
      where run_id=old.run_id and candidate_id=old.candidate_id
        and selection_revision=old.selection_revision and page_no=new.page_count;
    if v_count<>v_page.item_count or v_selected<>v_page.selected_count
       or v_min_item<>1 or v_max_item<>v_count
       or v_min_selection<>old.item_count+1 or v_max_selection<>old.item_count+v_count
       or v_page.first_selection_no<>old.item_count+1
       or (new.item_count,new.selected_count,new.excluded_count) is distinct from
         (old.item_count+v_page.item_count,old.selected_count+v_page.selected_count,
           old.excluded_count+v_page.excluded_count) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_PAGE_CONTENT_MISMATCH';
    end if;
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_selection_child_insert_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_selection private.bpay_next_case_selection%rowtype;v_items integer;
begin
  select s.* into strict v_selection from private.bpay_next_case_selection s
    where (s.run_id,s.candidate_id,s.selection_revision)=
      (new.run_id,new.candidate_id,new.selection_revision) for share;
  if v_selection.status<>'OPEN' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_LATE_CHILD_FORBIDDEN';
  end if;
  if new.page_no<>v_selection.page_count+1 then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_CHILD_PAGE_OUT_OF_ORDER';
  end if;
  if tg_table_name='bpay_next_case_selection_item' then
    select item_count into strict v_items from private.bpay_next_case_selection_page
      where run_id=new.run_id and candidate_id=new.candidate_id
        and selection_revision=new.selection_revision and page_no=new.page_no for share;
    if new.item_no>v_items then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_CHILD_ITEM_OUT_OF_RANGE';
    end if;
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_rule_insert_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
begin
  if new.original_funding_event_id is not null and
     (new.case_kind not in ('LOAN','ADVANCE') or not exists(
       select 1 from private.bpay_next_case_event e
       where e.id=new.original_funding_event_id and e.case_id=new.case_id
         and e.event_kind='FUNDED' and e.occurred_at_utc=new.original_funded_at_utc)) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_RULE_FUNDING_EVIDENCE_INVALID';
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_event_insert_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_result private.bpay_next_case_allocation_result%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;
begin
  if new.event_kind in ('FUNDED','RECOVERED','PAID','SHORTFALL') then
    if new.instruction_id is null or new.allocation_result_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_EVENT_TYPED_RESULT_REQUIRED';
    end if;
    select * into strict v_result from private.bpay_next_case_allocation_result
      where id=new.allocation_result_id for share;
    select * into strict v_instruction from private.bpay_next_run_case_instruction
      where id=new.instruction_id for share;
    if v_result.pass_kind not in ('DRAFT','NET') then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_EVENT_PREVIEW_NOT_FINANCIAL';
    end if;
    if new.event_kind in ('RECOVERED','SHORTFALL') and
       v_result.pass_kind is distinct from (case when new.hold_purpose='NET_RECOVERY_CAPACITY'
         then 'NET' else 'DRAFT' end) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_EVENT_RECOVERY_STAGE_INVALID';
    end if;
    if new.event_kind='SHORTFALL' then
      if new.shortfall_source_ex_vat is distinct from
         v_instruction.nominal_source_ex_vat-v_result.allocated_source_ex_vat
         or new.cap_reason is distinct from v_result.cap_reason then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_EVENT_SHORTFALL_MISMATCH';
      end if;
    elsif (new.source_amount_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat)
       is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,
         v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_EVENT_RESULT_AMOUNT_MISMATCH';
    end if;
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_instruction_insert_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_worker_status text;v_run_status text;v_selection_status text;
  v_preparation_revision bigint;v_selection_revision bigint;
begin
  -- Serialize insertion with confirmation/cancellation and exact selection
  -- seal; no latest Timesheet/rate/history lookup or post-Draft enrichment.
  select status into strict v_run_status from private.bpay_next_pay_run
    where id=new.run_id for share;
  select status,preparation_revision,case_selection_revision
    into strict v_worker_status,v_preparation_revision,v_selection_revision
    from private.bpay_next_run_worker
    where id=new.run_worker_id and candidate_id=new.candidate_id
      and run_id=new.run_id for share;
  select status into strict v_selection_status from private.bpay_next_case_selection
    where run_id=new.run_id and candidate_id=new.candidate_id
      and selection_revision=new.selection_revision for share;
  if v_worker_status<>'PREPARING' or v_run_status<>'PREPARING'
     or v_selection_status<>'SEALED' or v_preparation_revision<>new.preparation_revision
     or v_selection_revision<>new.selection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_INSTRUCTION_CAPTURE_STATE_INVALID';
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_result_insert_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_status text;v_instruction private.bpay_next_run_case_instruction%rowtype;
begin
  select s.status into strict v_status from private.bpay_next_case_allocation_state s
    where s.id=new.state_id for share;
  if v_status<>'BUILDING' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_RESULT_LATE_INSERT_FORBIDDEN';
  end if;
  select * into strict v_instruction from private.bpay_next_run_case_instruction
    where id=new.instruction_id for share;
  if new.nominal_target_ex_vat is distinct from v_instruction.nominal_target_ex_vat
     or new.allocated_source_ex_vat>v_instruction.nominal_source_ex_vat
     or new.allocated_target_ex_vat>v_instruction.nominal_target_ex_vat
     or new.allocated_target_vat>v_instruction.nominal_target_vat
     or new.allocated_target_inc_vat>v_instruction.nominal_target_inc_vat then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_RESULT_FROZEN_AMOUNT_EXCEEDED';
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_hold_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_result private.bpay_next_case_allocation_result%rowtype;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_DELETE_FORBIDDEN';
  end if;
  if tg_op='INSERT' then
    if new.status<>'ACTIVE' or new.instruction_id is null or new.allocation_result_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_TYPED_ACTIVE_REQUIRED';
    end if;
    select * into strict v_result from private.bpay_next_case_allocation_result
      where id=new.allocation_result_id for share;
    if v_result.pass_kind not in ('DRAFT','NET')
       or (new.source_reserved_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat)
         is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,
           v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_RESULT_AMOUNT_MISMATCH';
    end if;
    return new;
  end if;
  -- Resizing a NET capacity uses a new exact result/hold after releasing its
  -- own prior hold. Never rewrite an old captured allocation to hide money.
  if old.status<>'ACTIVE' or new.status not in ('RELEASED','REALISED')
     or old.finished_at_utc is not null or new.finished_at_utc is null
     or not pg_catalog.isfinite(new.finished_at_utc)
     or (pg_catalog.to_jsonb(new)-'status'-'projection_id'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'status'-'projection_id'-'finished_at_utc')
     or (old.projection_id is not null and new.projection_id is distinct from old.projection_id)
     or (new.status='RELEASED' and new.projection_id is distinct from old.projection_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_TRANSITION_INVALID';
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_capacity_use_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_hold private.bpay_next_case_hold%rowtype;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CAPACITY_DELETE_FORBIDDEN';
  end if;
  select * into strict v_hold from private.bpay_next_case_hold
    where id=new.case_hold_id for share;
  if tg_op='INSERT' then
    if new.status<>'ACTIVE' or v_hold.status<>'ACTIVE'
       or new.source_amount_ex_vat is distinct from v_hold.source_reserved_ex_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CAPACITY_INITIAL_STATE_INVALID';
    end if;
  elsif old.status<>'ACTIVE' or new.status not in ('RELEASED','REALISED')
     or (pg_catalog.to_jsonb(new)-'status'-'realisation_event_id')
       is distinct from (pg_catalog.to_jsonb(old)-'status'-'realisation_event_id')
     or v_hold.status<>new.status then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CAPACITY_TRANSITION_INVALID';
  end if;
  if new.status='REALISED' and not exists(select 1 from private.bpay_next_case_event e
      where e.id=new.realisation_event_id and e.instruction_id=v_hold.instruction_id
        and e.allocation_result_id=v_hold.allocation_result_id
        and e.event_kind in ('FUNDED','RECOVERED','PAID')
        and (e.source_amount_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)=
          (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CAPACITY_EVENT_MISMATCH';
  end if;
  return new;
end;
$function$;

do $install$
declare v_table text;
begin
  foreach v_table in array array['bpay_next_case_rule','bpay_next_run_case_instruction',
    'bpay_next_case_selection_page','bpay_next_case_selection_item','bpay_next_case_allocation_result'] loop
    execute pg_catalog.format('drop trigger if exists bpay_next_case_fact_immutable_v1 on private.%I',v_table);
    execute pg_catalog.format('create trigger bpay_next_case_fact_immutable_v1 before update or delete on private.%I for each row execute function private.bpay_next_effect_immutable_v1()',v_table);
  end loop;
  foreach v_table in array array['bpay_next_case_selection_page','bpay_next_case_selection_item'] loop
    execute pg_catalog.format('drop trigger if exists bpay_next_case_child_insert_v1 on private.%I',v_table);
    execute pg_catalog.format('create trigger bpay_next_case_child_insert_v1 before insert on private.%I for each row execute function private.bpay_next_case_selection_child_insert_guard_v1()',v_table);
  end loop;
end;
$install$;

drop trigger if exists bpay_next_case_selection_guard_v1 on private.bpay_next_case_selection;
create trigger bpay_next_case_selection_guard_v1 before insert or update or delete
  on private.bpay_next_case_selection for each row execute function private.bpay_next_case_selection_guard_v1();
drop trigger if exists bpay_next_case_instruction_insert_v1 on private.bpay_next_run_case_instruction;
create trigger bpay_next_case_instruction_insert_v1 before insert
  on private.bpay_next_run_case_instruction for each row execute function private.bpay_next_case_instruction_insert_guard_v1();
drop trigger if exists bpay_next_case_rule_insert_v1 on private.bpay_next_case_rule;
create trigger bpay_next_case_rule_insert_v1 before insert
  on private.bpay_next_case_rule for each row execute function private.bpay_next_case_rule_insert_guard_v1();
drop trigger if exists bpay_next_case_event_insert_v1 on private.bpay_next_case_event;
create trigger bpay_next_case_event_insert_v1 before insert
  on private.bpay_next_case_event for each row execute function private.bpay_next_case_event_insert_guard_v1();
drop trigger if exists bpay_next_case_result_insert_v1 on private.bpay_next_case_allocation_result;
create trigger bpay_next_case_result_insert_v1 before insert
  on private.bpay_next_case_allocation_result for each row execute function private.bpay_next_case_result_insert_guard_v1();
drop trigger if exists bpay_next_case_hold_guard_v1 on private.bpay_next_case_hold;
create trigger bpay_next_case_hold_guard_v1 before insert or update or delete
  on private.bpay_next_case_hold for each row execute function private.bpay_next_case_hold_guard_v1();
drop trigger if exists bpay_next_case_capacity_use_guard_v1 on private.bpay_next_case_capacity_use;
create trigger bpay_next_case_capacity_use_guard_v1 before insert or update or delete
  on private.bpay_next_case_capacity_use for each row execute function private.bpay_next_case_capacity_use_guard_v1();

alter function private.bpay_next_case_selection_guard_v1() owner to postgres;
alter function private.bpay_next_case_selection_child_insert_guard_v1() owner to postgres;
alter function private.bpay_next_case_instruction_insert_guard_v1() owner to postgres;
alter function private.bpay_next_case_result_insert_guard_v1() owner to postgres;
alter function private.bpay_next_case_hold_guard_v1() owner to postgres;
alter function private.bpay_next_case_capacity_use_guard_v1() owner to postgres;
alter function private.bpay_next_case_rule_insert_guard_v1() owner to postgres;
alter function private.bpay_next_case_event_insert_guard_v1() owner to postgres;
revoke all on function private.bpay_next_case_selection_guard_v1(),
  private.bpay_next_case_selection_child_insert_guard_v1(),
  private.bpay_next_case_instruction_insert_guard_v1(),
  private.bpay_next_case_result_insert_guard_v1(),
  private.bpay_next_case_hold_guard_v1(),private.bpay_next_case_capacity_use_guard_v1(),
  private.bpay_next_case_rule_insert_guard_v1(),private.bpay_next_case_event_insert_guard_v1()
  from public,anon,authenticated,service_role;
commit;
