-- Owner-only, ID-only preparation selection. No live API grant or Draft
-- activation is conferred by this file. A later job consumes SEALED pages.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_create_preparation_v1(
  p_run_id uuid,p_actor_user_id uuid,p_pay_date date
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
begin
  if p_run_id is null or p_actor_user_id is null or p_pay_date is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PREPARATION_INPUT_INVALID';
  end if;
  if (select active_owner from private.bpay_next_module_control
      where id=1 for share)<>'NEXT' then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if not exists(select 1 from public.tms_users u
      where u.id=p_actor_user_id and u.is_active=true) then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_ACTOR_INVALID';
  end if;
  -- First accepted preparation owns one elapsed-UTC 72-hour deadline.
  -- Reads, exact retries and worker lease renewal never extend it.
  insert into private.bpay_next_pay_run(id,actor_user_id,pay_date,status,
    created_at_utc,preparation_expires_at_utc)
    values(p_run_id,p_actor_user_id,p_pay_date,'PREPARING',
      pg_catalog.transaction_timestamp(),
      pg_catalog.transaction_timestamp()+interval '72 hours')
    on conflict(id) do nothing;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.actor_user_id<>p_actor_user_id or v_run.pay_date<>p_pay_date then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARATION_REPLAY_CONFLICT';
  end if;
  return jsonb_build_object('run_id',v_run.id,'status',v_run.status,
    'selection_state',v_run.selection_state,
    'selection_count',v_run.selection_count);
end
$function$;

create or replace function private.bpay_next_append_selection_page_v1(
  p_run_id uuid,p_page_no bigint,p_work_ids jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_page private.bpay_next_selection_page%rowtype;
  v_item jsonb;
  v_work_id uuid;
  v_candidate_id uuid;
  v_item_count integer;
  v_ordinal bigint;
  v_member_count bigint;
  v_inserted integer;
begin
  if p_run_id is null or p_page_no is null or p_page_no<1
     or pg_catalog.jsonb_typeof(p_work_ids) is distinct from 'array' then
    raise exception using errcode='22023',
      message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';
  end if;
  v_item_count:=pg_catalog.jsonb_array_length(p_work_ids);
  if v_item_count not between 1 and 100
     or pg_catalog.octet_length(p_work_ids::text)>262144 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_SELECTION_PAGE_TOO_LARGE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  select * into v_page from private.bpay_next_selection_page
    where run_id=p_run_id and page_no=p_page_no;
  if found then
    if v_page.work_ids_json is distinct from p_work_ids then
      raise exception using errcode='23514',
        message='BPAY_NEXT_SELECTION_PAGE_REPLAY_CONFLICT';
    end if;
    return jsonb_build_object('run_id',p_run_id,'page_no',p_page_no,
      'item_count',v_page.item_count,'replay',true,
      'selection_count',v_run.selection_count);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN'
     or p_page_no<>v_run.selection_page_count+1 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_PAGE_NOT_APPENDABLE';
  end if;
  v_ordinal:=v_run.selection_count;
  v_member_count:=v_run.selected_candidate_count;
  for v_item in select value from pg_catalog.jsonb_array_elements(p_work_ids)
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'string' then
      raise exception using errcode='22023',
        message='BPAY_NEXT_SELECTION_WORK_ID_NOT_STRING';
    end if;
    v_work_id:=(v_item #>> '{}')::uuid;
    select candidate_id into strict v_candidate_id
      from private.bpay_next_work where id=v_work_id;
    v_ordinal:=v_ordinal+1;
    insert into private.bpay_next_run_selection
      (run_id,selection_no,work_id,candidate_id)
      values(p_run_id,v_ordinal,v_work_id,v_candidate_id);
    -- Existing whole-WORK callers still make an explicit revision-bound ALL
    -- choice. No missing selection metadata may become an implicit fallback.
    perform private.bpay_next_record_all_work_choice_v1(p_run_id,v_work_id);
    insert into private.bpay_next_selection_candidate
      (run_id,candidate_id,member_no)
      values(p_run_id,v_candidate_id,v_member_count+1)
      on conflict(run_id,candidate_id) do nothing;
    get diagnostics v_inserted=row_count;
    if v_inserted=1 then
      v_member_count:=v_member_count+1;
    end if;
  end loop;
  insert into private.bpay_next_selection_page
    (run_id,page_no,work_ids_json,first_selection_no,item_count)
    values(p_run_id,p_page_no,p_work_ids,v_run.selection_count+1,v_item_count);
  update private.bpay_next_pay_run
    set selection_page_count=p_page_no,selection_count=v_ordinal,
        selected_candidate_count=v_member_count
    where id=p_run_id;
  return jsonb_build_object('run_id',p_run_id,'page_no',p_page_no,
    'item_count',v_item_count,'replay',false,'selection_count',v_ordinal,
    'selected_candidate_count',v_member_count);
end
$function$;

create or replace function private.bpay_next_enqueue_preparation_v1(
  p_run_id uuid,p_command_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_existing_command uuid;
  v_sequence bigint;
begin
  if p_run_id is null or p_command_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PREPARE_COMMAND_INPUT_INVALID';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  select command_id into v_existing_command
    from private.bpay_next_run_command where run_id=p_run_id;
  if found then
    if v_existing_command<>p_command_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_COMMAND_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence
      from private.bpay_next_command where id=p_command_id;
    return jsonb_build_object('run_id',p_run_id,
      'command_id',p_command_id,'agency_sequence',v_sequence,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count
     or v_run.selected_candidate_count<1
     or (v_run.selection_count=0 and (v_run.selection_page_count<>0
       or v_run.sealed_case_candidate_count<>v_run.selected_candidate_count)) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_SELECTION_NOT_SEALED';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARE');
  insert into private.bpay_next_run_command(run_id,command_id)
    values(p_run_id,p_command_id);
  update private.bpay_next_command
    set expected_member_count=v_run.selected_candidate_count,
        status='ENROLLING'
    where id=p_command_id and status='RECEIVED';
  if not found then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
  end if;
  return jsonb_build_object('run_id',p_run_id,
    'command_id',p_command_id,'agency_sequence',v_sequence,'replay',false);
end
$function$;

create or replace function private.bpay_next_seal_selection_v1(
  p_run_id uuid,p_expected_pages bigint,p_expected_count bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;
begin
  if p_run_id is null or p_expected_pages is null or p_expected_count is null
     or p_expected_pages<0 or p_expected_count<0
     or (p_expected_pages=0) is distinct from (p_expected_count=0) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_SELECTION_SEAL_INPUT_INVALID';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.selection_page_count<>p_expected_pages
     or v_run.selection_count<>p_expected_count then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SELECTION_SEAL_COUNT_MISMATCH';
  end if;
  if v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_COMPONENT_CHOICES_NOT_SEALED';
  end if;
  if v_run.selection_state='SEALED' then
    return jsonb_build_object('run_id',p_run_id,'sealed',true,
      'selection_count',v_run.selection_count,'replay',true);
  end if;
  -- A case-only run contains genuine selected case components, not fictitious
  -- WORK rows. The root-locked seal counter is maintained once per Candidate
  -- by0330; checking it does not scan every Candidate at final confirmation.
  if p_expected_count=0 and (v_run.selected_candidate_count<1
     or v_run.sealed_case_candidate_count<>v_run.selected_candidate_count) then
    raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_CASE_ONLY_NOT_SEALED';
  end if;
  if v_run.status<>'PREPARING' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_SEAL_STATUS_INVALID';
  end if;
  update private.bpay_next_pay_run
    set selection_state='SEALED',review_revision=review_revision+1
    where id=p_run_id;
  return jsonb_build_object('run_id',p_run_id,'sealed',true,
    'selection_count',v_run.selection_count,'replay',false);
end
$function$;

alter function private.bpay_next_create_preparation_v1(uuid,uuid,date)
  owner to postgres;
alter function private.bpay_next_append_selection_page_v1(uuid,bigint,jsonb)
  owner to postgres;
alter function private.bpay_next_enqueue_preparation_v1(uuid,uuid)
  owner to postgres;
alter function private.bpay_next_seal_selection_v1(uuid,bigint,bigint)
  owner to postgres;
revoke all on function
  private.bpay_next_create_preparation_v1(uuid,uuid,date),
  private.bpay_next_append_selection_page_v1(uuid,bigint,jsonb),
  private.bpay_next_enqueue_preparation_v1(uuid,uuid),
  private.bpay_next_seal_selection_v1(uuid,bigint,bigint)
  from public,anon,authenticated,service_role;

commit;
