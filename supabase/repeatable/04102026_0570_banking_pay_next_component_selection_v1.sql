-- Bounded typed pre-Draft choices; financial capture/holds remain1600/1610/1620.
-- ALL retains whole-WORK positions, SUBSET names immutable approved lines.
-- No old session, date remapping, current financial calculation or money input.
\set ON_ERROR_STOP on
begin;

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
    if old.selection_state='SEALED'
      or new.run_id<>old.run_id or new.work_id<>old.work_id
      or new.expected_revision_id is distinct from old.expected_revision_id
      or new.selection_mode<>old.selection_mode
      or new.page_count<old.page_count or new.component_count<old.component_count then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';
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
drop trigger if exists bpay_next_work_choice_guard on private.bpay_next_work_choice;
create trigger bpay_next_work_choice_guard before insert or update or delete
on private.bpay_next_work_choice for each row execute function private.bpay_next_work_choice_guard_v1();
drop trigger if exists bpay_next_component_page_guard on private.bpay_next_component_selection_page;
create trigger bpay_next_component_page_guard before insert or update or delete
on private.bpay_next_component_selection_page for each row execute function private.bpay_next_work_choice_guard_v1();
drop trigger if exists bpay_next_selected_component_guard on private.bpay_next_selected_component;
create trigger bpay_next_selected_component_guard before insert or update or delete
on private.bpay_next_selected_component for each row execute function private.bpay_next_work_choice_guard_v1();

--1590 calls this ONLY after a genuine new run_selection INSERT. Pending and
--withdrawn WORK are retained explicitly for1600 REVIEW, not silently omitted.
create or replace function private.bpay_next_record_all_work_choice_v1(p_run_id uuid,p_work_id uuid)
returns void language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_revision uuid;v_choice private.bpay_next_work_choice%rowtype;
begin
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select c.* into v_choice from private.bpay_next_work_choice c where c.run_id=p_run_id and c.work_id=p_work_id;
  if found then
    if v_choice.selection_mode<>'ALL' then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REPLAY_CONFLICT';
    end if;
    return; -- original evidence, never re-read a newer revision on replay
  end if;
  select w.current_revision_id into strict v_revision from private.bpay_next_work w
    join private.bpay_next_run_selection s on s.work_id=w.id and s.candidate_id=w.candidate_id
    where s.run_id=p_run_id and s.work_id=p_work_id;
  insert into private.bpay_next_work_choice(run_id,work_id,expected_revision_id,selection_mode,selection_state)
    values(p_run_id,p_work_id,v_revision,'ALL','SEALED');
  update private.bpay_next_pay_run set work_choice_count=work_choice_count+1,
    sealed_work_choice_count=sealed_work_choice_count+1 where id=p_run_id;
end
$function$;

create or replace function private.bpay_next_append_work_choices_v1(p_run_id uuid,p_page_no bigint,p_choices jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;v_page private.bpay_next_selection_page%rowtype;
  v_work private.bpay_next_work%rowtype;v_choice private.bpay_next_work_choice%rowtype;
  v_item jsonb;v_ids jsonb:='[]'::jsonb;v_n integer;v_i integer:=0;
  v_work_id uuid;v_revision uuid;v_mode text;v_member bigint;v_inserted integer;v_all integer:=0;
begin
  if p_run_id is null or p_page_no is null or p_page_no<1
    or pg_catalog.jsonb_typeof(p_choices) is distinct from 'array'
    or pg_catalog.octet_length(p_choices::text)>32768 then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_INPUT_INVALID';
  end if;
  v_n:=pg_catalog.jsonb_array_length(p_choices);
  if v_n not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_INPUT_INVALID';end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- Closed tuples are validated before casting, including replay bodies.
  for v_item in select value from pg_catalog.jsonb_array_elements(p_choices) loop
    if pg_catalog.jsonb_typeof(v_item) is distinct from 'object'
      or (select count(*) from pg_catalog.jsonb_object_keys(v_item))<>3
      or not(v_item ?& array['work_id','expected_revision_id','mode'])
      or pg_catalog.jsonb_typeof(v_item->'work_id') is distinct from 'string'
      or pg_catalog.jsonb_typeof(v_item->'expected_revision_id') is distinct from 'string'
      or pg_catalog.jsonb_typeof(v_item->'mode') is distinct from 'string'
      or (v_item->>'work_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or (v_item->>'expected_revision_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or v_item->>'mode' not in ('ALL','SUBSET') then
      raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_INPUT_INVALID';
    end if;
    v_ids:=v_ids||pg_catalog.jsonb_build_array(((v_item->>'work_id')::uuid)::text);
  end loop;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select p.* into v_page from private.bpay_next_selection_page p where p.run_id=p_run_id and p.page_no=p_page_no;
  if found then
    if v_page.work_ids_json is distinct from v_ids or v_page.item_count<>v_n then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REPLAY_CONFLICT';
    end if;
    for v_item in select value from pg_catalog.jsonb_array_elements(p_choices) loop
      select c.* into strict v_choice from private.bpay_next_work_choice c
        where c.run_id=p_run_id and c.work_id=(v_item->>'work_id')::uuid;
      if v_choice.expected_revision_id is distinct from (v_item->>'expected_revision_id')::uuid
        or v_choice.selection_mode<>v_item->>'mode' then
        raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REPLAY_CONFLICT';
      end if;
    end loop;
    return pg_catalog.jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,
      'item_count',v_n::text,'selection_count',v_run.selection_count::text,
      'selected_candidate_count',v_run.selected_candidate_count::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or p_page_no<>v_run.selection_page_count+1 then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
  end if;
  v_member:=v_run.selected_candidate_count;
  -- Same membership rows as1590, explicit choice headers in the SAME tx.
  -- Do not call the ID-only default ALL helper on this explicit path.
  for v_item in select value from pg_catalog.jsonb_array_elements(p_choices) loop
    v_work_id:=(v_item->>'work_id')::uuid;v_revision:=(v_item->>'expected_revision_id')::uuid;v_mode:=v_item->>'mode';
    select w.* into strict v_work from private.bpay_next_work w where w.id=v_work_id;
    if v_work.approval_state<>'APPROVED' or v_work.current_revision_id is distinct from v_revision
      or v_work.applied_revision_id is distinct from v_revision
      or not exists(select 1 from private.bpay_next_work_revision r where r.id=v_revision and r.work_id=v_work_id
        and r.approved_at_utc is not null and r.sealed_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    v_i:=v_i+1;
    insert into private.bpay_next_run_selection(run_id,selection_no,work_id,candidate_id)
      values(p_run_id,v_run.selection_count+v_i,v_work_id,v_work.candidate_id);
    insert into private.bpay_next_work_choice(run_id,work_id,expected_revision_id,selection_mode,selection_state)
      values(p_run_id,v_work_id,v_revision,v_mode,case when v_mode='ALL' then 'SEALED' else 'OPEN' end);
    if v_mode='ALL' then v_all:=v_all+1;end if;
    insert into private.bpay_next_selection_candidate(run_id,candidate_id,member_no)
      values(p_run_id,v_work.candidate_id,v_member+1) on conflict(run_id,candidate_id) do nothing;
    get diagnostics v_inserted=row_count;
    if v_inserted=1 then v_member:=v_member+1;end if;
  end loop;
  insert into private.bpay_next_selection_page(run_id,page_no,work_ids_json,first_selection_no,item_count)
    values(p_run_id,p_page_no,v_ids,v_run.selection_count+1,v_n);
  update private.bpay_next_pay_run set selection_page_count=p_page_no,selection_count=selection_count+v_n,
    selected_candidate_count=v_member,work_choice_count=work_choice_count+v_n,
    sealed_work_choice_count=sealed_work_choice_count+v_all where id=p_run_id;
  return pg_catalog.jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,'item_count',v_n::text,
    'selection_count',(v_run.selection_count+v_n)::text,'selected_candidate_count',v_member::text,'replay',false);
end
$function$;

create or replace function private.bpay_next_append_component_selection_page_v1(
  p_run_id uuid,p_work_id uuid,p_expected_revision_id uuid,p_page_no bigint,p_approved_line_ids uuid[]
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;v_choice private.bpay_next_work_choice%rowtype;
  v_page private.bpay_next_component_selection_page%rowtype;v_key text;v_n integer;v_i integer;
begin
  v_n:=pg_catalog.cardinality(p_approved_line_ids);
  if p_run_id is null or p_work_id is null or p_expected_revision_id is null or p_page_no is null or p_page_no<1
    or p_approved_line_ids is null or v_n not between 1 and 100
    or pg_catalog.array_ndims(p_approved_line_ids)<>1 or pg_catalog.array_lower(p_approved_line_ids,1)<>1
    or pg_catalog.array_position(p_approved_line_ids,null) is not null then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select c.* into strict v_choice from private.bpay_next_work_choice c where c.run_id=p_run_id and c.work_id=p_work_id;
  if v_choice.expected_revision_id is distinct from p_expected_revision_id or v_choice.selection_mode<>'SUBSET' then
    raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REVISION_CONFLICT';
  end if;
  select p.* into v_page from private.bpay_next_component_selection_page p
    where p.run_id=p_run_id and p.work_id=p_work_id and p.page_no=p_page_no;
  if found then
    if v_page.item_count<>v_n or exists(select 1 from private.bpay_next_selected_component c
      where c.run_id=p_run_id and c.work_id=p_work_id and c.page_no=p_page_no
        and c.approved_line_id is distinct from p_approved_line_ids[c.page_item_no]) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REPLAY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
      'page_no',p_page_no::text,'item_count',v_n::text,'component_count',v_choice.component_count::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or v_choice.selection_state<>'OPEN'
    or p_page_no<>v_choice.page_count+1 then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
  end if;
  if not exists(select 1 from private.bpay_next_work w where w.id=p_work_id and w.approval_state='APPROVED'
    and w.current_revision_id=p_expected_revision_id and w.applied_revision_id=p_expected_revision_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
  end if;
  insert into private.bpay_next_component_selection_page(run_id,work_id,page_no,first_selection_no,item_count)
    values(p_run_id,p_work_id,p_page_no,v_choice.component_count+1,v_n);
  for v_i in 1..v_n loop
    select l.component_key into v_key from private.bpay_next_approved_line l
      where l.id=p_approved_line_ids[v_i] and l.revision_id=p_expected_revision_id;
    if not found then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
    insert into private.bpay_next_selected_component(run_id,work_id,expected_revision_id,approved_line_id,
      component_key,selection_no,page_no,page_item_no)
      values(p_run_id,p_work_id,p_expected_revision_id,p_approved_line_ids[v_i],v_key,
        v_choice.component_count+v_i,p_page_no,v_i);
  end loop;
  update private.bpay_next_work_choice set page_count=p_page_no,component_count=component_count+v_n
    where run_id=p_run_id and work_id=p_work_id;
  return pg_catalog.jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
    'page_no',p_page_no::text,'item_count',v_n::text,'component_count',(v_choice.component_count+v_n)::text,'replay',false);
end
$function$;

create or replace function private.bpay_next_seal_component_selection_v1(
  p_run_id uuid,p_work_id uuid,p_expected_revision_id uuid,p_expected_pages bigint,p_expected_count bigint
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_choice private.bpay_next_work_choice%rowtype;
begin
  if p_run_id is null or p_work_id is null or p_expected_revision_id is null
    or p_expected_pages is null or p_expected_pages<1 or p_expected_count is null or p_expected_count<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select c.* into strict v_choice from private.bpay_next_work_choice c where c.run_id=p_run_id and c.work_id=p_work_id;
  if v_choice.selection_mode<>'SUBSET' or v_choice.expected_revision_id is distinct from p_expected_revision_id
    or v_choice.page_count<>p_expected_pages or v_choice.component_count<>p_expected_count then
    raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_SEAL_CONFLICT';
  end if;
  if v_choice.selection_state='SEALED' then
    return pg_catalog.jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
      'sealed',true,'component_count',v_choice.component_count::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
  end if;
  update private.bpay_next_work_choice set selection_state='SEALED' where run_id=p_run_id and work_id=p_work_id;
  update private.bpay_next_pay_run set sealed_work_choice_count=sealed_work_choice_count+1 where id=p_run_id;
  return pg_catalog.jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
    'sealed',true,'component_count',v_choice.component_count::text,'replay',false);
end
$function$;

alter function private.bpay_next_work_choice_guard_v1() owner to postgres;
alter function private.bpay_next_record_all_work_choice_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_append_work_choices_v1(uuid,bigint,jsonb) owner to postgres;
alter function private.bpay_next_append_component_selection_page_v1(uuid,uuid,uuid,bigint,uuid[]) owner to postgres;
alter function private.bpay_next_seal_component_selection_v1(uuid,uuid,uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_work_choice_guard_v1(),
  private.bpay_next_record_all_work_choice_v1(uuid,uuid),
  private.bpay_next_append_work_choices_v1(uuid,bigint,jsonb),
  private.bpay_next_append_component_selection_page_v1(uuid,uuid,uuid,bigint,uuid[]),
  private.bpay_next_seal_component_selection_v1(uuid,uuid,uuid,bigint,bigint)
  from public,anon,authenticated,service_role;
commit;
