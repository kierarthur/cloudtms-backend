-- F-25 fresh acceptance refuses prior terminal clock before business locks.
-- Exact immutable replay retains its original cut and existing response.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_enqueue_preparation_v1(p_run_id uuid,p_command_id uuid)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_existing_command uuid;v_sequence bigint;
  v_stage private.bpay_next_acceptance_stage%rowtype;v_input private.bpay_next_accepted_input%rowtype;
begin
  if p_run_id is null or p_command_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARE_COMMAND_INPUT_INVALID';
  end if;
  -- Exact immutable replay before fresh admission/locks/capture.
  select rc.command_id into v_existing_command from private.bpay_next_run_command rc where rc.run_id=p_run_id;
  if found then
    if v_existing_command<>p_command_id then
      raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_COMMAND_REPLAY_CONFLICT';
    end if;
    select c.agency_sequence into strict v_sequence from private.bpay_next_command c where c.id=p_command_id;
    return pg_catalog.jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,
      'agency_sequence',v_sequence,'replay',true);
  end if;
  if exists(select 1 from private.bpay_next_input_transaction t
    where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() and t.phase<>'OPEN') then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_TRANSACTION_NOT_FRESH';
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  perform 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  -- Recheck after waiting: a concurrent exact accepted call retains its cut.
  select rc.command_id into v_existing_command from private.bpay_next_run_command rc where rc.run_id=p_run_id;
  if found then
    if v_existing_command<>p_command_id then
      raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_COMMAND_REPLAY_CONFLICT';
    end if;
    select c.agency_sequence into strict v_sequence from private.bpay_next_command c where c.id=p_command_id;
    return pg_catalog.jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,
      'agency_sequence',v_sequence,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
    or v_run.work_choice_count<>v_run.selection_count or v_run.sealed_work_choice_count<>v_run.selection_count
    or v_run.selected_candidate_count<1 or (v_run.selection_count=0 and
      (v_run.selection_page_count<>0 or v_run.sealed_case_candidate_count<>v_run.selected_candidate_count)) then
    raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_SELECTION_NOT_SEALED';
  end if;
  perform private.bpay_next_stage_acceptance_v1(p_run_id,p_command_id);
  -- No new business/FK target lock follows this sole existing clock receipt.
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARE');
  insert into private.bpay_next_run_command(run_id,command_id) values(p_run_id,p_command_id);
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    select p_command_id,s.candidate_id,s.member_no from private.bpay_next_selection_candidate s
    where s.run_id=p_run_id order by s.member_no;
  update private.bpay_next_command set expected_member_count=v_run.selected_candidate_count,status='ENROLLING'
    where id=p_command_id and status='RECEIVED';
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';end if;
  for v_stage in select s.* from private.bpay_next_acceptance_stage s
    where s.prepare_command_id=p_command_id and s.preparing_transaction_id=pg_catalog.pg_current_xact_id()
      and s.record_kind='CANDIDATE' order by s.entity_id
  loop
    v_input:=pg_catalog.jsonb_populate_record(null::private.bpay_next_accepted_input,v_stage.record_json);
    v_input.accepted_sequence:=v_sequence;
    v_input.evidence_sha256:=pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(
      'BPAY_NEXT_ACCEPTED_INPUT_V1',v_input.run_id,v_input.prepare_command_id,v_input.candidate_id,
      v_input.module_epoch,v_input.accepted_sequence,v_input.target_pay_channel,v_input.target_umbrella_id,
      v_input.umbrella_vat_chargeable,v_input.umbrella_enabled,v_input.candidate_floor_raw,v_input.floor_state,
      v_input.candidate_floor,v_input.pay_date,v_input.pay_week_start,v_input.valuation_policy_id,v_input.case_scope,
      v_input.case_selection_revision,v_input.candidate_version_event_id,v_input.umbrella_version_event_id,
      v_input.policy_version_event_id,v_input.settings_witness_kind)::text,'UTF8'));
    insert into private.bpay_next_accepted_input select v_input.*;
  end loop;
  insert into private.bpay_next_accepted_work_input
    select r.* from private.bpay_next_acceptance_stage s
      cross join lateral pg_catalog.jsonb_populate_record(null::private.bpay_next_accepted_work_input,s.record_json) r
    where s.prepare_command_id=p_command_id and s.preparing_transaction_id=pg_catalog.pg_current_xact_id()
      and s.record_kind='WORK' order by s.entity_id;
  insert into private.bpay_next_accepted_work_member
    select r.* from private.bpay_next_acceptance_stage s
      cross join lateral pg_catalog.jsonb_populate_record(null::private.bpay_next_accepted_work_member,s.record_json) r
    where s.prepare_command_id=p_command_id and s.preparing_transaction_id=pg_catalog.pg_current_xact_id()
      and s.record_kind='MEMBER' order by r.accepted_work_input_id,r.member_no;
  insert into private.bpay_next_accepted_case_input
    select r.* from private.bpay_next_acceptance_stage s
      cross join lateral pg_catalog.jsonb_populate_record(null::private.bpay_next_accepted_case_input,s.record_json) r
    where s.prepare_command_id=p_command_id and s.preparing_transaction_id=pg_catalog.pg_current_xact_id()
      and s.record_kind='CASE' order by r.accepted_input_id,r.selection_no;
  insert into private.bpay_next_accepted_week_input
    select r.* from private.bpay_next_acceptance_stage s
      cross join lateral pg_catalog.jsonb_populate_record(null::private.bpay_next_accepted_week_input,s.record_json) r
    where s.prepare_command_id=p_command_id and s.preparing_transaction_id=pg_catalog.pg_current_xact_id()
      and s.record_kind='WEEK' order by r.accepted_input_id,r.original_pay_date,r.original_created_at_utc,r.original_run_worker_id;
  delete from private.bpay_next_acceptance_stage s where s.prepare_command_id=p_command_id
    and s.preparing_transaction_id=pg_catalog.pg_current_xact_id();
  return pg_catalog.jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,
    'agency_sequence',v_sequence,'replay',false);
end
$function$;

create or replace function public.bpay_next_preparation_intake_v1(p_actor_user_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_run uuid;v_work uuid;v_revision uuid;v_candidate uuid;v_result jsonb;v_output jsonb;
  v_ids uuid[];v_selected boolean[];v_entry jsonb;v_header private.bpay_next_pay_run%rowtype;
begin
  -- Pure authority probe preserves auth-before-arguments and admits Source
  -- before the original locked authority check. Recorded acceptance skips it.
  if p_action='SEAL_ENQUEUE' then
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users u
        where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';
    end if;
    if pg_catalog.jsonb_typeof(p_args)='object' and p_args->>'run_id' ~* '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      and not exists(select 1 from private.bpay_next_run_command rc where rc.run_id=(p_args->>'run_id')::uuid) then
      if exists(select 1 from private.bpay_next_input_transaction t
        where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() and t.phase<>'OPEN') then
        raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_TRANSACTION_NOT_FRESH';
      end if;
      perform private.weekly_source_pay_query_admit_v2();
    end if;
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_action
    when 'CREATE' then array['run_id','pay_date']
    when 'WORK_PAGE' then array['run_id','page_no','choices']
    when 'COMPONENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','approved_line_ids']
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

alter function private.bpay_next_enqueue_preparation_v1(uuid,uuid) owner to postgres;
alter function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_enqueue_preparation_v1(uuid,uuid),public.bpay_next_preparation_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) to service_role;
notify pgrst,'reload schema';
commit;
