-- DR-01 exact current successors:1590/0570/1236; RuleB/F25 owners remain untouched.
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
  v_official_date date;
begin
  if p_run_id is null or p_actor_user_id is null then
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
  -- Retained identity is authoritative across London rollover/settings edits.
  select r.* into v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  if found then
    if v_run.actor_user_id<>p_actor_user_id then
      raise exception using errcode='23514',message='BPAY_NEXT_PREPARATION_REPLAY_CONFLICT';
    end if;
    if p_pay_date is not null and p_pay_date<>v_run.pay_date then
      raise exception using errcode='22023',message='BPAY_NEXT_PAY_DATE_NOT_OFFICIAL';
    end if;
    return jsonb_build_object('run_id',v_run.id,'status',v_run.status,
      'selection_state',v_run.selection_state,'selection_count',v_run.selection_count,
      'official_pay_date',v_run.pay_date::text);
  end if;
  v_official_date:=(public.pay_banking_official_date_context_v1(
    pg_catalog.transaction_timestamp())->>'current_official_pay_date')::date;
  if v_official_date is null or not pg_catalog.isfinite(v_official_date) then
    raise exception using errcode='55000',message='BPAY_NEXT_OFFICIAL_PAY_DATE_UNAVAILABLE';
  end if;
  -- First accepted preparation owns one elapsed-UTC 72-hour deadline.
  -- Reads, exact retries and worker lease renewal never extend it.
  insert into private.bpay_next_pay_run(id,actor_user_id,pay_date,status,
    created_at_utc,preparation_expires_at_utc)
    values(p_run_id,p_actor_user_id,v_official_date,'PREPARING',
      pg_catalog.transaction_timestamp(),
      pg_catalog.transaction_timestamp()+interval '72 hours')
    on conflict(id) do nothing;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.actor_user_id<>p_actor_user_id then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARATION_REPLAY_CONFLICT';
  end if;
  if p_pay_date is not null and p_pay_date<>v_run.pay_date then
    raise exception using errcode='22023',message='BPAY_NEXT_PAY_DATE_NOT_OFFICIAL';
  end if;
  return jsonb_build_object('run_id',v_run.id,'status',v_run.status,
    'selection_state',v_run.selection_state,'selection_count',v_run.selection_count,
    'official_pay_date',v_run.pay_date::text);
end
$function$;
alter function private.bpay_next_create_preparation_v1(uuid,uuid,date) owner to postgres;
revoke all on function private.bpay_next_create_preparation_v1(uuid,uuid,date) from public,anon,authenticated,service_role;

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
  v_work private.bpay_next_work%rowtype;
  v_skipped jsonb:='[]'::jsonb;
  v_week_start date;
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
    if v_page.work_ids_json is distinct from p_work_ids or v_page.choices_request_json is not null then
      raise exception using errcode='23514',
        message='BPAY_NEXT_SELECTION_PAGE_REPLAY_CONFLICT';
    end if;
    return jsonb_build_object('run_id',p_run_id,'page_no',p_page_no,
      'item_count',v_page.item_count,'replay',true,
      'selection_count',v_run.selection_count,
      'selected_candidate_count',v_run.selected_candidate_count,'skipped',v_page.skipped_json);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN'
     or p_page_no<>v_run.selection_page_count+1 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_PAGE_NOT_APPENDABLE';
  end if;
  if (select count(distinct (x.value#>>'{}')::uuid) from pg_catalog.jsonb_array_elements(p_work_ids) x)<>v_item_count then
    raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';
  end if;
  if v_run.snooze_business_date is null or v_run.snooze_control_sequence is null then
    raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_CUT_UNAVAILABLE';
  end if;
  v_week_start:=pg_catalog.date_trunc('week',v_run.snooze_business_date::timestamp)::date;
  v_ordinal:=v_run.selection_count;
  v_member_count:=v_run.selected_candidate_count;
  for v_item in select value from pg_catalog.jsonb_array_elements(p_work_ids)
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'string' then
      raise exception using errcode='22023',
        message='BPAY_NEXT_SELECTION_WORK_ID_NOT_STRING';
    end if;
    v_work_id:=(v_item #>> '{}')::uuid;
    select w.* into strict v_work from private.bpay_next_work w where w.id=v_work_id;
    v_candidate_id:=v_work.candidate_id;
    if v_work.week_ending_date-6>v_week_start then
      v_skipped:=v_skipped||jsonb_build_array(jsonb_build_object('work_id',v_work_id,
        'code','BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE'));
      continue;
    end if;
    -- Preserve the inherited ID-only pending/withdrawn/missing-approval REVIEW
    -- path. Only an actually approved sealed revision can qualify for a whole
    -- snooze; absence of required basis on that payable path fails closed.
    if v_work.approval_state='APPROVED' and v_work.current_revision_id is not null
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_work.current_revision_id and r.work_id=v_work.id
          and r.approved_at_utc is not null and r.sealed_at_utc is not null) then
    if not exists(select 1 from private.bpay_next_revision_worker_basis b where b.revision_id=v_work.current_revision_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_BASIS_UNAVAILABLE';
    end if;
    -- Only historical WHOLE_TIMESHEET authority skips the parent WORK.
    -- Segment/expense exclusions retain their existing selected-segment owner.
    if exists(select 1 from private.bpay_next_snooze sn
      join lateral(select ev.after_state from private.bpay_next_snooze_event ev
        where ev.snooze_id=sn.id and ev.control_sequence<=v_run.snooze_control_sequence
        order by ev.control_sequence desc limit 1) e on true
      join private.bpay_next_revision_worker_basis b on b.revision_id=v_work.current_revision_id
      where sn.work_id=v_work.id and sn.scope_kind='WHOLE_TIMESHEET'
        and (e.after_state->>'active')::boolean
        and ((e.after_state->>'until_date') is null or (e.after_state->>'until_date')::date>=v_run.snooze_business_date)
        and (e.after_state->>'worker_pay_basis')::bytea=b.whole_basis) then
      v_skipped:=v_skipped||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('work_id',v_work_id,
        'code','BPAY_NEXT_SELECTION_SNOOZED'));
      continue;
    end if;
    end if;
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
    (run_id,page_no,work_ids_json,first_selection_no,item_count,skipped_json)
    values(p_run_id,p_page_no,p_work_ids,v_run.selection_count+1,v_item_count,v_skipped);
  update private.bpay_next_pay_run
    set selection_page_count=p_page_no,selection_count=v_ordinal,
        selected_candidate_count=v_member_count
    where id=p_run_id;
  return jsonb_build_object('run_id',p_run_id,'page_no',p_page_no,
    'item_count',v_item_count,'replay',false,'selection_count',v_ordinal,
    'selected_candidate_count',v_member_count,'skipped',v_skipped);
end
$function$;
alter function private.bpay_next_append_selection_page_v1(uuid,bigint,jsonb) owner to postgres;
revoke all on function private.bpay_next_append_selection_page_v1(uuid,bigint,jsonb) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_append_work_choices_v1(p_run_id uuid,p_page_no bigint,p_choices jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;v_page private.bpay_next_selection_page%rowtype;
  v_work private.bpay_next_work%rowtype;v_choice private.bpay_next_work_choice%rowtype;
  v_item jsonb;v_ids jsonb:='[]'::jsonb;v_n integer;v_i integer:=0;
  v_work_id uuid;v_revision uuid;v_mode text;v_member bigint;v_inserted integer;v_all integer:=0;
  v_skipped jsonb:='[]'::jsonb;v_week_start date;
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
  if (select count(distinct x.value) from pg_catalog.jsonb_array_elements(v_ids) x)<>v_n then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_INPUT_INVALID';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select p.* into v_page from private.bpay_next_selection_page p where p.run_id=p_run_id and p.page_no=p_page_no;
  if found then
    if v_page.work_ids_json is distinct from v_ids or v_page.item_count<>v_n
      or (v_page.choices_request_json is not null and v_page.choices_request_json is distinct from p_choices) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REPLAY_CONFLICT';
    end if;
    if v_page.choices_request_json is null then
    for v_item in select value from pg_catalog.jsonb_array_elements(p_choices) loop
      select c.* into strict v_choice from private.bpay_next_work_choice c
        where c.run_id=p_run_id and c.work_id=(v_item->>'work_id')::uuid;
      if v_choice.expected_revision_id is distinct from (v_item->>'expected_revision_id')::uuid
        or v_choice.selection_mode<>v_item->>'mode' then
        raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REPLAY_CONFLICT';
      end if;
    end loop;
    end if;
    return pg_catalog.jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,
      'item_count',v_n::text,'selection_count',v_run.selection_count::text,
      'selected_candidate_count',v_run.selected_candidate_count::text,'replay',true,'skipped',v_page.skipped_json);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or p_page_no<>v_run.selection_page_count+1 then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
  end if;
  if v_run.snooze_business_date is null or v_run.snooze_control_sequence is null then
    raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_CUT_UNAVAILABLE';
  end if;
  v_week_start:=pg_catalog.date_trunc('week',v_run.snooze_business_date::timestamp)::date;
  v_member:=v_run.selected_candidate_count;
  -- Same membership rows as1590, explicit choice headers in the SAME tx.
  -- Do not call the ID-only default ALL helper on this explicit path.
  for v_item in select value from pg_catalog.jsonb_array_elements(p_choices) loop
    v_work_id:=(v_item->>'work_id')::uuid;v_revision:=(v_item->>'expected_revision_id')::uuid;v_mode:=v_item->>'mode';
    select w.* into strict v_work from private.bpay_next_work w where w.id=v_work_id;
    if not exists(select 1 from private.bpay_next_work_revision r where r.id=v_revision and r.work_id=v_work_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REVISION_CONFLICT';
    end if;
    if v_work.week_ending_date-6>v_week_start then
      v_skipped:=v_skipped||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('work_id',v_work_id,
        'code','BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE'));
      continue;
    end if;
    if v_work.approval_state<>'APPROVED' or v_work.current_revision_id is distinct from v_revision
      or v_work.applied_revision_id is distinct from v_revision
      or not exists(select 1 from private.bpay_next_work_revision r where r.id=v_revision and r.work_id=v_work_id
        and r.approved_at_utc is not null and r.sealed_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    if not exists(select 1 from private.bpay_next_revision_worker_basis b where b.revision_id=v_revision) then
      raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_BASIS_UNAVAILABLE';
    end if;
    -- Only historical WHOLE_TIMESHEET authority skips the parent WORK.
    -- Segment/expense exclusions retain their existing selected-segment owner.
    if exists(select 1 from private.bpay_next_snooze sn
      join lateral(select ev.after_state from private.bpay_next_snooze_event ev
        where ev.snooze_id=sn.id and ev.control_sequence<=v_run.snooze_control_sequence
        order by ev.control_sequence desc limit 1) e on true
      join private.bpay_next_revision_worker_basis b on b.revision_id=v_revision
      where sn.work_id=v_work.id and sn.scope_kind='WHOLE_TIMESHEET'
        and (e.after_state->>'active')::boolean
        and ((e.after_state->>'until_date') is null or (e.after_state->>'until_date')::date>=v_run.snooze_business_date)
        and (e.after_state->>'worker_pay_basis')::bytea=b.whole_basis) then
      v_skipped:=v_skipped||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('work_id',v_work_id,
        'code','BPAY_NEXT_SELECTION_SNOOZED'));
      continue;
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
  insert into private.bpay_next_selection_page(run_id,page_no,work_ids_json,first_selection_no,item_count,choices_request_json,skipped_json)
    values(p_run_id,p_page_no,v_ids,v_run.selection_count+1,v_n,p_choices,v_skipped);
  update private.bpay_next_pay_run set selection_page_count=p_page_no,selection_count=selection_count+v_i,
    selected_candidate_count=v_member,work_choice_count=work_choice_count+v_i,
    sealed_work_choice_count=sealed_work_choice_count+v_all where id=p_run_id;
  return pg_catalog.jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,'item_count',v_n::text,
    'selection_count',(v_run.selection_count+v_i)::text,'selected_candidate_count',v_member::text,'replay',false,'skipped',v_skipped);
end
$function$;
alter function private.bpay_next_append_work_choices_v1(uuid,bigint,jsonb) owner to postgres;
revoke all on function private.bpay_next_append_work_choices_v1(uuid,bigint,jsonb) from public,anon,authenticated,service_role;

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
    when 'CREATE' then case when p_args ? 'pay_date' then array['run_id','pay_date'] else array['run_id'] end
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
      'selected_candidate_count',v_header.selected_candidate_count::text,'official_pay_date',v_header.pay_date::text);
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
notify pgrst, 'reload schema';
commit;
