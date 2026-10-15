-- SP14 / BD122 owner11Oct shift-only query exclusion. No Source producer changed.
\set ON_ERROR_STOP on
begin;

-- Pre-cut read only. NULL preserves the existing unknown/unavailable refusal;
-- [] is genuinely clear; otherwise exact native segment reasons, no money.
create or replace function private.bpay_next_source_query_segment_exclusions_v1(p_revision_id uuid,p_gate jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare v_revision private.bpay_next_work_revision%rowtype;v_work private.bpay_next_work%rowtype;
 v_facts jsonb;v_item jsonb;v_result jsonb:='[]'::jsonb;v_key text;v_reason text;
begin
 select revision.* into v_revision from private.bpay_next_work_revision revision where id=p_revision_id;
 if not found or v_revision.source_kind not in ('SOURCE','PROTECTED') then return null;end if;
 select work.* into strict v_work from private.bpay_next_work work where id=v_revision.work_id;
 if p_gate->'ok' is distinct from 'true'::jsonb or p_gate->>'code' is distinct from 'OK'
  or jsonb_typeof(p_gate->'blocked') is distinct from 'boolean' then return null;end if;
 v_facts:=private.weekly_source_pay_query_facts_v1(v_revision.physical_timesheet_id);
 if v_facts->'ok' is distinct from 'true'::jsonb or v_facts->>'code' is distinct from 'OK'
  or (v_facts->'scope',v_facts->'blocked',v_facts->'query_state_sha256')
     is distinct from (p_gate->'scope',p_gate->'blocked',p_gate->'query_state_sha256')
  or jsonb_typeof(v_facts->'hours_incidents') is distinct from 'array'
  or jsonb_typeof(v_facts->'manual_reviews') is distinct from 'array'
  or v_facts#>>'{scope,root_timesheet_id}' is distinct from v_revision.physical_timesheet_id::text
  or v_facts#>>'{scope,root_version}' is distinct from v_revision.physical_timesheet_version::text
  or v_facts#>>'{scope,candidate_id}' is distinct from v_work.candidate_id::text
  or v_facts#>>'{scope,contract_id}' is distinct from v_work.contract_id::text
  or v_facts#>>'{scope,week_ending_date}' is distinct from v_work.week_ending_date::text
  or v_facts#>>'{scope,family_booking_id}' is distinct from v_work.booking_id then return null;end if;
 -- A free-text manual reason cannot be guessed to mean hours differ.
 -- It must have the actual same-event current typed hours-difference incident.
 for v_item in select value from jsonb_array_elements(v_facts->'manual_reviews') loop
  if not exists(select 1 from jsonb_array_elements(v_facts->'hours_incidents') i(value)
   where i.value->>'work_event_id'=v_item->>'work_event_id'
    and i.value->>'source_group_id'=v_item->>'source_group_id'
    and i.value->>'candidate_id'=v_item->>'candidate_id'
    and i.value->>'client_id'=v_item->>'client_id'
    and i.value->>'comparison_contract_id'=v_item->>'contract_id'
    and i.value->>'issue_family'='SOURCE_HOURS_DIFFER') then return null;end if;
 end loop;
 for v_item in select value from jsonb_array_elements(v_facts->'hours_incidents') loop
  v_reason:=v_item->>'issue_family';
  if v_reason='CANDIDATE_TIMESHEET_MISSING' then continue;end if;
  if v_item->'latest_qualified_exception' is distinct from 'null'::jsonb
    and not exists(select 1 from jsonb_array_elements(v_facts->'manual_reviews') m(value)
      where m.value->>'work_event_id'=v_item->>'work_event_id') then continue;end if;
  if v_reason not in ('HEALTHROSTER_NOT_FINALISED','SOURCE_MISSING_OR_NOT_AUTHORISED','SOURCE_HOURS_DIFFER')
    or v_reason is null then return null;end if;
  v_key:=v_item->>'work_event_id';
  if v_key is null or not exists(select 1 from private.bpay_next_approved_segment s
    where s.revision_id=p_revision_id and s.segment_key=v_key) then return null;end if;
  v_result:=v_result||jsonb_build_array(jsonb_build_object('segment_key',v_key,'reason',v_reason));
 end loop;
 if p_gate->'blocked'='true'::jsonb and jsonb_array_length(v_result)=0 then return null;end if;
 return (select coalesce(jsonb_agg(x.item order by x.item->>'segment_key',x.item->>'reason'),'[]'::jsonb)
  from (select distinct value item from jsonb_array_elements(v_result)) x);
end
$function$;

-- Current owner: supabase/repeatable/11102026_1358_banking_pay_next_guarded_create_v1.sql
create or replace function private.bpay_next_create_preparation_v1(
 p_run_id uuid,p_actor_user_id uuid,p_pay_date date,p_descriptor jsonb,p_command_id uuid,p_guard jsonb
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_result jsonb;v_ns jsonb;v_intent jsonb;
 v_row jsonb;v_array text;v_keys text[];v_field text;v_id uuid;v_header jsonb;
 v_query_admitted boolean:=true;v_policy text:='NONE';v_channel text:='ALL';
 v_reason text;v_proof uuid;v_guard jsonb;v_scope jsonb;v_policy_args jsonb;
 v_locked_candidates uuid[];v_locked_works uuid[];v_locked_revisions uuid[];v_locked_cases uuid[];
 v_candidate uuid;v_work uuid;v_revision uuid;v_sequence bigint;v_gate jsonb;v_query_exclusions jsonb;v_member record;
 v_memory boolean:=p_descriptor ? 'memory_baseline_run_id';v_baseline uuid;v_page_no bigint;v_lines uuid[];v_subset record;
 v_mode text;v_selected boolean;v_count bigint;v_code text;v_week_start date;v_work_count bigint:=0;
begin
 if p_guard is not null then
  if jsonb_typeof(p_guard) is distinct from 'object' or not(p_guard ?& array['decision','reason','reauth_proof_id'])
   or p_guard-array['decision','reason','reauth_proof_id']<>'{}'::jsonb
   or jsonb_typeof(p_guard->'decision') is distinct from 'string'
   or p_guard->>'decision' not in ('UMBRELLA_ONLY','VERIFY_BOTH','VERIFY_PAYE') then
   raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  v_policy:=p_guard->>'decision';v_channel:=case v_policy when 'UMBRELLA_ONLY' then 'UMBRELLA' when 'VERIFY_PAYE' then 'PAYE' else 'ALL' end;
  if v_policy='UMBRELLA_ONLY' then
   if p_guard->'reason'<>'null'::jsonb or p_guard->'reauth_proof_id'<>'null'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  else
   if jsonb_typeof(p_guard->'reason') is distinct from 'string' or btrim(p_guard->>'reason')=''
    or octet_length(p_guard->>'reason') not between 1 and 500
    or jsonb_typeof(p_guard->'reauth_proof_id') is distinct from 'string'
    or p_guard->>'reauth_proof_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
   v_reason:=p_guard->>'reason';v_proof:=(p_guard->>'reauth_proof_id')::uuid;
  end if;
 end if;
 if p_command_id is null or jsonb_typeof(p_descriptor) is distinct from 'object'
   or octet_length(p_descriptor::text)>32768
   or not(p_descriptor ?& array['namespace','intent','revision','workbench_revision','adoption_revision'])
   or p_descriptor-array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id']<>'{}'::jsonb then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';
 end if;
 if v_memory and p_descriptor->'memory_baseline_run_id'<>'null'::jsonb and
  (jsonb_typeof(p_descriptor->'memory_baseline_run_id')<>'string' or p_descriptor->>'memory_baseline_run_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 v_ns:=p_descriptor->'namespace';v_intent:=p_descriptor->'intent';
 if jsonb_typeof(v_ns) is distinct from 'object'
   or not(v_ns ?& array['agency_id','owner_epoch','actor_user_id','official_pay_date','scope'])
   or v_ns-array['agency_id','owner_epoch','actor_user_id','official_pay_date','scope']<>'{}'::jsonb
   or jsonb_typeof(v_ns->'scope') is distinct from 'object'
   or not(v_ns->'scope' ?& array['candidate_id','client_id','channel'])
   or (v_ns->'scope')-array['candidate_id','client_id','channel']<>'{}'::jsonb
   or jsonb_typeof(v_intent) is distinct from 'object'
   or not(v_intent ?& array['mode','candidates','works','components','cases'])
   or v_intent-array['mode','candidates','works','components','cases']<>'{}'::jsonb
   or jsonb_typeof(v_intent->'mode') is distinct from 'string'
   or v_intent->>'mode' not in ('ALL_MATCHING','NONE') then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';
 end if;
 foreach v_array in array array['candidates','works','components','cases'] loop
  if jsonb_typeof(v_intent->v_array) is distinct from 'array'
    or jsonb_array_length(v_intent->v_array)>1000 then
   raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
  v_keys:=case v_array when 'candidates' then array['candidate_id','selected']
    when 'works' then array['candidate_id','work_id','expected_revision_id','selected','mode']
    when 'components' then array['candidate_id','work_id','expected_revision_id','approved_line_id','selected']
    else array['candidate_id','case_component_id','selected'] end;
  for v_row in select value from jsonb_array_elements(v_intent->v_array) loop
   if jsonb_typeof(v_row) is distinct from 'object' or not(v_row ?& v_keys)
     or v_row-v_keys<>'{}'::jsonb or jsonb_typeof(v_row->'selected') is distinct from 'boolean' then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
   foreach v_field in array v_keys loop
    if v_field in ('selected','mode') then continue;end if;
    if jsonb_typeof(v_row->v_field) is distinct from 'string'
      or v_row->>v_field!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
     raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
   end loop;
   if v_array='works' and (jsonb_typeof(v_row->'mode') is distinct from 'string'
      or v_row->>'mode' not in ('ALL','SUBSET')) then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
  end loop;
  if exists(select 1 from jsonb_array_elements(v_intent->v_array) a(value)
    group by case v_array when 'candidates' then a.value->>'candidate_id'
      when 'works' then a.value->>'work_id' when 'components' then a.value->>'approved_line_id'
      else a.value->>'case_component_id' end having count(*)>1) then
   raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 end loop;
 if jsonb_typeof(v_ns->'agency_id') is distinct from 'string'
   or v_ns->>'agency_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or jsonb_typeof(v_ns->'actor_user_id') is distinct from 'string'
   or v_ns->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or jsonb_typeof(v_ns->'owner_epoch') is distinct from 'string'
   or v_ns->>'owner_epoch'!~'^[1-9][0-9]{0,18}$'
   or jsonb_typeof(v_ns->'official_pay_date') is distinct from 'string'
   or v_ns->>'official_pay_date'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
   or jsonb_typeof(v_ns->'scope'->'channel') is distinct from 'string'
   or v_ns->'scope'->>'channel' not in ('ALL','PAYE','UMBRELLA') then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 foreach v_field in array array['candidate_id','client_id'] loop
  if v_ns->'scope'->v_field<>'null'::jsonb and (jsonb_typeof(v_ns->'scope'->v_field) is distinct from 'string'
    or v_ns->'scope'->>v_field!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
   raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 end loop;
 if jsonb_typeof(p_descriptor->'workbench_revision') is distinct from 'string'
   or p_descriptor->>'workbench_revision'!~'^(0|[1-9][0-9]{0,18})$'
   or jsonb_typeof(p_descriptor->'adoption_revision') is distinct from 'string'
   or p_descriptor->>'adoption_revision'!~'^(0|[1-9][0-9]{0,18})$' then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 -- An exact receipt replay acquires no new source/cut/FK-parent locks.
 select src.* into v_run from private.bpay_next_pay_run src where id=p_run_id;
 if found then
  if v_run.accepted_choice_descriptor is null then
   raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_RUN_NOT_EMPTY';end if;
  v_result:=private.bpay_next_create_preparation_v1(p_run_id,p_actor_user_id,p_pay_date);
  select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id;
  if v_run.accepted_choice_descriptor is distinct from p_descriptor
    or v_run.accepted_choice_command_id is distinct from p_command_id then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  if (v_run.paye_guard_decision,v_run.channel_scope,v_run.paye_override_reason,v_run.reauth_proof_id,
    v_run.paye_override_actor) is distinct from (v_policy,v_channel,v_reason,v_proof,
      case when v_policy='NONE' then null::uuid else p_actor_user_id end) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  if v_proof is not null then
   perform private.bpay_next_reauth_consume_v1(v_proof,p_actor_user_id,'PAYE_SAME_WEEK_OVERRIDE',v_run.paye_guard_scope_sha256,p_command_id);
  end if;
  select agency_sequence into v_sequence from private.bpay_next_command where id=p_command_id;
  return v_result||jsonb_build_object('command_id',p_command_id,'agency_sequence',v_sequence::text,
    'choice_work_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'),
    'choice_case_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='CASE_COMPONENT'),
    'selected_candidate_count',v_run.selected_candidate_count::text,'empty',v_run.selected_candidate_count=0,'replay',true);
 end if;
 -- Source admission precedes the original module/run/snooze cut locks.
 begin perform private.weekly_source_pay_query_admit_v2();
 exception when lock_not_available then v_query_admitted:=false;end;
 perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 -- FK parent locks precede the inherited snooze command-clock SHARE.
 -- No revision-meta lock: Snooze writers already take clock before meta.
 select coalesce(array_agg(parent.id),'{}'::uuid[]) into v_locked_candidates from (select candidate.id from public.candidates candidate where exists(select 1 from private.bpay_next_workbench_item i
   where i.candidate_id=candidate.id and i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   or exists(select 1 from unnest(array['candidates','works','components','cases']) kind
     cross join lateral jsonb_array_elements(v_intent->kind) a(value) where a.value->>'candidate_id'=candidate.id::text)
   order by candidate.id for share) parent;
 if exists(select 1 from jsonb_array_elements(v_intent->'candidates') requested
   where not ((requested->>'candidate_id')::uuid=any(v_locked_candidates))) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 perform 1 from public.umbrellas umbrella where exists(select 1 from public.candidates candidate
   join private.bpay_next_workbench_item i on i.candidate_id=candidate.id and i.section='READY' and i.selectable
   where candidate.umbrella_id=umbrella.id
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   order by umbrella.id for share;
 select coalesce(array_agg(parent.id),'{}'::uuid[]) into v_locked_works from (select w.id from private.bpay_next_work w where exists(select 1 from private.bpay_next_workbench_item i
   where i.work_id=w.id and i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   or exists(select 1 from jsonb_array_elements(v_intent->'works') a(value) where a.value->>'work_id'=w.id::text)
   order by w.id for share) parent;
 select coalesce(array_agg(parent.id),'{}'::uuid[]) into v_locked_revisions from (select r.id from private.bpay_next_work_revision r where exists(select 1 from private.bpay_next_workbench_item i
   where i.revision_id=r.id and i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   or exists(select 1 from jsonb_array_elements(v_intent->'works') a(value) where a.value->>'expected_revision_id'=r.id::text)
   order by r.work_id,r.id for share) parent;
 select coalesce(array_agg(parent.id),'{}'::uuid[]) into v_locked_cases from (select component.id from private.bpay_next_case_component component where exists(select 1 from private.bpay_next_workbench_item i
   where i.case_component_id=component.id and i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   or exists(select 1 from jsonb_array_elements(v_intent->'cases') a(value) where a.value->>'case_component_id'=component.id::text)
   order by component.candidate_id,component.id for share) parent;
 if exists(select 1 from private.bpay_next_command c where c.status in ('RECEIVED','ENROLLING'))
   or exists(select 1 from private.bpay_next_job j join private.bpay_next_command c on c.id=j.command_id
   where j.status<>'DONE' and j.candidate_id=any(v_locked_candidates)) then
  raise exception using errcode='55000',message='BPAY_NEXT_ACCEPT_INPUT_EARLIER_ECONOMIC_COMMAND_PENDING';end if;
 -- Acquire UPDATE once before the inherited SHARE, avoiding concurrent upgrades.
 perform 1 from private.bpay_next_command_clock where id=1 for update;
 if exists(select 1 from private.bpay_next_command c where c.status in ('RECEIVED','ENROLLING'))
   or exists(select 1 from private.bpay_next_job j join private.bpay_next_command c on c.id=j.command_id
   where j.status<>'DONE' and j.candidate_id=any(v_locked_candidates)) then
  raise exception using errcode='55000',message='BPAY_NEXT_ACCEPT_INPUT_EARLIER_ECONOMIC_COMMAND_PENDING';end if;
 if exists(select 1 from private.bpay_next_command where id=p_command_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_COMMAND_REPLAY_CONFLICT';end if;
 -- A same-run concurrent legacy writer must never make clock->run wait.
 perform 1 from private.bpay_next_pay_run where id=p_run_id for update nowait;
 if found and exists(select 1 from private.bpay_next_pay_run where id=p_run_id and accepted_choice_descriptor is null) then
  raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_RUN_NOT_EMPTY';end if;
 -- Original UUID/actor/date/deadline owner is preserved, including replay.
 v_result:=private.bpay_next_create_preparation_v1(p_run_id,p_actor_user_id,p_pay_date);
 select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
 if v_run.accepted_choice_descriptor is not null then
  if v_run.accepted_choice_descriptor is distinct from p_descriptor
    or v_run.accepted_choice_command_id is distinct from p_command_id then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  if (v_run.paye_guard_decision,v_run.channel_scope,v_run.paye_override_reason,v_run.reauth_proof_id,
    v_run.paye_override_actor) is distinct from (v_policy,v_channel,v_reason,v_proof,
      case when v_policy='NONE' then null::uuid else p_actor_user_id end) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  if v_proof is not null then
   perform private.bpay_next_reauth_consume_v1(v_proof,p_actor_user_id,'PAYE_SAME_WEEK_OVERRIDE',v_run.paye_guard_scope_sha256,p_command_id);
  end if;
  select agency_sequence into v_sequence from private.bpay_next_command where id=p_command_id;
  return v_result||jsonb_build_object('command_id',p_command_id,'agency_sequence',v_sequence::text,
    'choice_work_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'),
    'choice_case_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='CASE_COMPONENT'),
    'selected_candidate_count',v_run.selected_candidate_count::text,'empty',v_run.selected_candidate_count=0,'replay',true);
 end if;
 if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or v_run.selection_page_count<>0
   or v_run.selection_count<>0 or v_run.selected_candidate_count<>0
   or exists(select 1 from private.bpay_next_run_selection where run_id=p_run_id)
   or exists(select 1 from private.bpay_next_selection_candidate where run_id=p_run_id)
   or exists(select 1 from private.bpay_next_case_selection where run_id=p_run_id)
   or exists(select 1 from private.bpay_next_run_command where run_id=p_run_id)
   or v_run.preparation_expires_at_utc<=clock_timestamp() then
  raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_RUN_NOT_EMPTY';end if;
 -- Compare real authenticated identity/context; caller fields are never authority.
 v_header:=public.bpay_next_workbench_page_v1(jsonb_build_object('actor_user_id',p_actor_user_id,
   'kind','HEADER','candidate_id',v_ns->'scope'->'candidate_id','client_id',v_ns->'scope'->'client_id',
   'channel',v_ns->'scope'->>'channel','section','READY','search','','sort','NAME_ASC',
   'group_key',null,'item_id',null,'reason_code',null,'count_selection',null,'after',null,'limit',100));
 if v_ns->>'agency_id' is distinct from v_header->'context'->>'agency_id'
   or v_ns->>'actor_user_id' is distinct from p_actor_user_id::text
   or v_ns->>'owner_epoch' is distinct from v_header->'context'->>'owner_epoch'
   or v_ns->>'official_pay_date' is distinct from v_run.pay_date::text
   or p_descriptor->>'workbench_revision' is distinct from v_header->'context'->>'workbench_revision'
   or p_descriptor->>'adoption_revision' is distinct from v_header->'context'->>'adoption_revision'
   or jsonb_typeof(p_descriptor->'revision') is distinct from 'number'
   or p_descriptor->>'revision'!~'^(0|[1-9][0-9]{0,15})$' then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
 -- Validate every deliberate subordinate identity before any accepted cut.
 for v_array,v_row in select k,value from unnest(array['works','components']) k
   cross join lateral jsonb_array_elements(v_intent->k) a(value) loop
  v_work:=(v_row->>'work_id')::uuid;v_candidate:=(v_row->>'candidate_id')::uuid;
  v_revision:=(v_row->>'expected_revision_id')::uuid;
  if not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision r
    on r.work_id=w.id and r.id=v_revision where w.id=v_work and w.candidate_id=v_candidate)
    or (v_array='components' and not exists(select 1 from private.bpay_next_approved_line l
      where l.id=(v_row->>'approved_line_id')::uuid and l.revision_id=v_revision)) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 end loop;
 -- WB25: component-only memory overlays retain untouched native defaults.
 -- A supplied parent must match; an absent parent must be current, scoped and
 -- eligible under the original held Work/revision locks. Legacy stays SUBSET.
 if exists(select 1 from jsonb_array_elements(v_intent->'components') c(value)
   where case when exists(select 1 from jsonb_array_elements(v_intent->'works') w(value)
      where w.value->>'work_id'=c.value->>'work_id') then
     not exists(select 1 from jsonb_array_elements(v_intent->'works') w(value)
      where w.value->>'work_id'=c.value->>'work_id' and w.value->>'candidate_id'=c.value->>'candidate_id'
       and w.value->>'expected_revision_id'=c.value->>'expected_revision_id'
       and (v_memory or w.value->>'mode'='SUBSET' and w.value->'selected'='true'::jsonb))
    else not v_memory or not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision r on r.work_id=w.id
       join private.bpay_next_workbench_item i on i.work_id=w.id and i.revision_id=r.id
       join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
       where w.id=(c.value->>'work_id')::uuid and w.candidate_id=(c.value->>'candidate_id')::uuid
        and r.id=(c.value->>'expected_revision_id')::uuid and w.current_revision_id=r.id and w.applied_revision_id=r.id
        and i.approved_line_id=(c.value->>'approved_line_id')::uuid and i.candidate_id=w.candidate_id
        and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
        and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
        and g.progress_state='CURRENT' and i.progress_state='CURRENT'
        and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
        and (v_ns->'scope'->>'candidate_id' is null or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
        and (v_ns->'scope'->>'client_id' is null or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
        and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel')) end) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 if exists(select 1 from jsonb_array_elements(v_intent->'cases') a(value) where not exists(
   select 1 from private.bpay_next_case_component c where c.id=(a.value->>'case_component_id')::uuid
     and c.candidate_id=(a.value->>'candidate_id')::uuid)) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 -- DR-10: fresh serialized policy admission, before the original member cut.
 v_guard:=private.bpay_next_paye_guard_page_v1(p_actor_user_id,p_run_id,p_descriptor);
 if v_policy='NONE' and (v_guard->>'guard_applies')::boolean then
  raise exception using errcode='55000',message='BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNRESOLVED';end if;
 if v_policy<>'NONE' and not(v_guard->>'guard_applies')::boolean then
  raise exception using errcode='23514',message='BPAY_NEXT_PAYE_GUARD_CHANGED';end if;
 if v_policy in ('VERIFY_BOTH','VERIFY_PAYE') then
  if (v_guard->>'unexecuted')::boolean then
   raise exception using errcode='55000',message='BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNEXECUTED';end if;
  v_policy_args:=jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,'decision',v_policy,'reason',v_reason,
    'context',p_descriptor-array['intent','revision','memory_baseline_run_id']);
  v_scope:=private.bpay_next_reauth_scope_v1(p_actor_user_id,'PAYE_SAME_WEEK_OVERRIDE',v_policy_args,false);
  perform private.bpay_next_reauth_consume_v1(v_proof,p_actor_user_id,'PAYE_SAME_WEEK_OVERRIDE',v_scope->>'scope_sha256',p_command_id);
 end if;
 update private.bpay_next_pay_run set paye_guard_decision=v_policy,channel_scope=v_channel,
  paye_override_actor=case when v_policy='NONE' then null else p_actor_user_id end,
  paye_override_reason=v_reason,reauth_proof_id=v_proof,paye_guard_scope_sha256=v_scope->>'scope_sha256' where id=p_run_id;
 -- The original run boundary retained its snooze cut; no second clock or kind.
 insert into private.bpay_next_run_selection_exclusion(run_id,page_no,candidate_id,actor_user_id)
   select p_run_id,(row_number() over(order by a.value->>'candidate_id')+99)/100,
     (a.value->>'candidate_id')::uuid,p_actor_user_id
   from jsonb_array_elements(v_intent->'candidates') a(value) where a.value->'selected'='false'::jsonb;
 insert into private.bpay_next_run_selection_exclusion(run_id,page_no,work_id,actor_user_id)
   select p_run_id,((select count(*) from private.bpay_next_run_selection_exclusion where run_id=p_run_id)
     +row_number() over(order by a.value->>'work_id')+99)/100,(a.value->>'work_id')::uuid,p_actor_user_id
   from jsonb_array_elements(v_intent->'works') a(value) where a.value->'selected'='false'::jsonb;
 -- No membership may acquire an unheld FK parent after the command clock.
 if exists(select 1 from private.bpay_next_workbench_item i
   where i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel')
     and (not(i.candidate_id=any(v_locked_candidates))
       or (i.work_id is not null and (not(i.work_id=any(v_locked_works)) or not(i.revision_id=any(v_locked_revisions))))
       or (i.case_component_id is not null and not(i.case_component_id=any(v_locked_cases))))) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
 -- Memory is captured under the original parent locks and final context check.
 if v_memory then
  v_baseline:=private.bpay_next_selection_memory_resolve_v1(v_ns,jsonb_build_object('run_id',p_descriptor->'memory_baseline_run_id','scope',v_ns->'scope'));
  perform private.bpay_next_selection_memory_capture_v1(p_run_id,p_descriptor);
  with choices as materialized(select * from private.bpay_next_selection_memory_choices_v1(v_ns->'scope',v_baseline,v_intent)),
  works as (select l.work_id,l.candidate_id,
    coalesce((ov.value->>'expected_revision_id')::uuid,l.revision_id) expected_revision_id,
    case when bool_and(l.selected) then 'ALL' else 'SUBSET' end selection_mode
   from choices l left join lateral(select value from jsonb_array_elements(v_intent->'works') where value->>'work_id'=l.work_id::text) ov on true
   where l.work_id is not null and l.direction='PAYMENT'
    and (v_policy<>'VERIFY_PAYE' or exists(select 1 from public.candidates c where c.id=l.candidate_id and upper(c.pay_method)='PAYE'))
   group by l.work_id,l.candidate_id,coalesce((ov.value->>'expected_revision_id')::uuid,l.revision_id)
   having bool_or(l.selected)),
  work_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,work_id,expected_revision_id,selection_mode,selected,member_no)
   select p_run_id,'WORK',w.work_id,w.candidate_id,w.work_id,w.expected_revision_id,w.selection_mode,true,
    row_number() over(order by (w.selection_mode='SUBSET') desc,w.work_id) from works w returning candidate_id),
  case_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,case_component_id,selected,member_no)
   select p_run_id,'CASE_COMPONENT',l.case_component_id,l.candidate_id,l.case_component_id,l.selected,
    row_number() over(order by l.candidate_id,l.case_component_id) from choices l
   where l.case_component_id is not null
    and (v_channel='ALL' or exists(select 1 from public.candidates c where c.id=l.candidate_id and upper(c.pay_method)=v_channel)) and
    (exists(select 1 from work_members w where w.candidate_id=l.candidate_id)
     or exists(select 1 from choices payment where payment.candidate_id=l.candidate_id and payment.case_component_id is not null and payment.direction='PAYMENT' and payment.selected))
   returning candidate_id)
  select (select count(*) from work_members)+(select count(*) from case_members) into v_count;
 else
 -- Retain Work and Case membership in one statement snapshot.
 with work_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,
   work_id,expected_revision_id,selection_mode,selected,member_no)
 select p_run_id,'WORK',i.work_id,i.candidate_id,i.work_id,
   coalesce((ov.value->>'expected_revision_id')::uuid,i.revision_id),coalesce(ov.value->>'mode','ALL'),true,
   row_number() over(order by (coalesce(ov.value->>'mode','ALL')='SUBSET') desc,i.work_id)
 from private.bpay_next_workbench_item i join private.bpay_next_workbench_group g
   on g.group_key=i.group_key and g.candidate_id=i.candidate_id
 left join lateral(select a.value from jsonb_array_elements(v_intent->'works') a(value)
   where a.value->>'work_id'=i.work_id::text) ov on true
 where i.section='READY' and i.selectable and i.work_id is not null and i.amount_known
   and i.direction='PAYMENT' and i.portion_kind in ('AVAILABLE','ZERO')
   and (v_policy<>'VERIFY_PAYE' or exists(select 1 from public.candidates c where c.id=i.candidate_id and upper(c.pay_method)='PAYE'))
   and g.complete and not g.dirty and g.progress_state='CURRENT' and i.progress_state='CURRENT'
   and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
   and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
   and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
   and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel')
   and not exists(select 1 from private.bpay_next_run_selection_exclusion e where e.run_id=p_run_id
     and (e.candidate_id=i.candidate_id or e.work_id=i.work_id))
   and (v_intent->>'mode'='ALL_MATCHING' or ov.value->'selected'='true'::jsonb or exists(
     select 1 from jsonb_array_elements(v_intent->'candidates') a(value)
       where a.value->>'candidate_id'=i.candidate_id::text and a.value->'selected'='true'::jsonb))
 group by i.work_id,i.candidate_id,coalesce((ov.value->>'expected_revision_id')::uuid,i.revision_id),coalesce(ov.value->>'mode','ALL') returning candidate_id),
 case_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,case_component_id,selected,member_no)
 select p_run_id,'CASE_COMPONENT',i.case_component_id,i.candidate_id,i.case_component_id,
   coalesce((ov.value->>'selected')::boolean,true),row_number() over(order by i.candidate_id,i.case_component_id)
 from private.bpay_next_workbench_item i join private.bpay_next_workbench_group g
   on g.group_key=i.group_key and g.candidate_id=i.candidate_id
 left join lateral(select a.value from jsonb_array_elements(v_intent->'cases') a(value)
   where a.value->>'case_component_id'=i.case_component_id::text) ov on true
 where i.section='READY' and i.selectable and i.case_component_id is not null
   and (v_channel='ALL' or exists(select 1 from public.candidates c where c.id=i.candidate_id and upper(c.pay_method)=v_channel))
   and g.complete and not g.dirty and g.progress_state='CURRENT' and i.progress_state='CURRENT'
   and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
   and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
   and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
   and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel')
   and not exists(select 1 from private.bpay_next_run_selection_exclusion e where e.run_id=p_run_id
     and e.candidate_id=i.candidate_id)
   and (exists(select 1 from work_members m where m.candidate_id=i.candidate_id)
     or exists(select 1 from private.bpay_next_workbench_item payment
       join private.bpay_next_workbench_group payment_group
         on payment_group.group_key=payment.group_key and payment_group.candidate_id=payment.candidate_id
       left join lateral(select a.value from jsonb_array_elements(v_intent->'cases') a(value)
         where a.value->>'case_component_id'=payment.case_component_id::text) chosen on true
       where payment.candidate_id=i.candidate_id and payment.case_component_id is not null
         and payment.section='READY' and payment.selectable and payment.direction='PAYMENT'
         and payment.amount_known and payment.portion_kind in ('AVAILABLE','ZERO')
         and payment_group.complete and not payment_group.dirty
         and payment_group.progress_state='CURRENT' and payment.progress_state='CURRENT'
         and payment.projection_revision=payment_group.projection_revision
         and payment.financial_view_revision=payment_group.financial_view_revision
         and coalesce((chosen.value->>'selected')::boolean,true)
         and (v_ns->'scope'->'client_id'='null'::jsonb or payment.client_id=(v_ns->'scope'->>'client_id')::uuid)
         and (v_ns->'scope'->>'channel'='ALL' or payment.target_pay_channel=v_ns->'scope'->>'channel')
         and (v_intent->>'mode'='ALL_MATCHING' or chosen.value->'selected'='true'::jsonb
           or exists(select 1 from jsonb_array_elements(v_intent->'candidates') a(value)
             where a.value->>'candidate_id'=i.candidate_id::text and a.value->'selected'='true'::jsonb))))
 group by i.case_component_id,i.candidate_id,coalesce((ov.value->>'selected')::boolean,true) returning candidate_id)
 select (select count(*) from work_members)+(select count(*) from case_members) into v_count;
 end if;
 v_week_start:=date_trunc('week',v_run.snooze_business_date::timestamp)::date;
 for v_member in select m.*,w.approval_state,w.current_revision_id,w.applied_revision_id,w.week_ending_date,
   r.physical_timesheet_id,r.source_kind,r.approved_at_utc,r.sealed_at_utc,upper(c.pay_method) as pay_channel
   from private.bpay_next_run_choice_member m join private.bpay_next_work w on w.id=m.work_id
   join private.bpay_next_work_revision r on r.work_id=w.id and r.id=m.expected_revision_id
   join public.candidates c on c.id=m.candidate_id
   where m.run_id=p_run_id and m.member_kind='WORK' order by m.member_no loop
  v_code:=null;
  if v_policy='UMBRELLA_ONLY' and v_member.pay_channel='PAYE' then
   v_code:='PAYE_EXCLUDED_BY_SAME_WEEK_GUARD';
  elsif v_member.approval_state<>'APPROVED' or v_member.approved_at_utc is null or v_member.sealed_at_utc is null then
   v_code:='BPAY_NEXT_SELECTION_WORK_NOT_APPROVED';
  elsif v_member.current_revision_id is distinct from v_member.expected_revision_id
    or v_member.applied_revision_id is distinct from v_member.expected_revision_id then
   v_code:='BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';
  elsif v_member.week_ending_date-6>v_week_start then v_code:='BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE';
  elsif not exists(select 1 from private.bpay_next_work w
    join public.timesheets t on t.booking_id=w.booking_id and t.is_current
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where w.id=v_member.work_id) then v_code:='BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';
  elsif exists(select 1 from private.bpay_next_work w
    join public.timesheets t on t.booking_id=w.booking_id and t.is_current=true
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
    where w.id=v_member.work_id and tf.pay_on_hold is true) then
   v_code:='BPAY_NEXT_SELECTION_ON_HOLD';
  elsif not exists(select 1 from private.bpay_next_revision_worker_basis where revision_id=v_member.expected_revision_id) then
   raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_BASIS_UNAVAILABLE';
  elsif exists(select 1 from private.bpay_next_snooze sn
    join lateral(select ev.after_state from private.bpay_next_snooze_event ev
      where ev.snooze_id=sn.id and ev.control_sequence<=v_run.snooze_control_sequence
      order by ev.control_sequence desc limit 1) e on true
    join private.bpay_next_revision_worker_basis b on b.revision_id=v_member.expected_revision_id
    where sn.work_id=v_member.work_id and sn.scope_kind='WHOLE_TIMESHEET'
      and (e.after_state->>'active')::boolean
      and ((e.after_state->>'until_date') is null or (e.after_state->>'until_date')::date>=v_run.snooze_business_date)
      and (e.after_state->>'worker_pay_basis')::bytea=b.whole_basis) then
   v_code:='BPAY_NEXT_SELECTION_SNOOZED';
  end if;
  v_gate:=null;v_query_exclusions:=null;
  if v_code is null and v_member.source_kind in ('SOURCE','PROTECTED') then
   if not v_query_admitted then v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
   else begin v_gate:=private.weekly_source_pay_query_gate_v1(v_member.physical_timesheet_id);
    if v_gate->'ok' is distinct from 'true'::jsonb or v_gate->>'code' is distinct from 'OK'
      or jsonb_typeof(v_gate->'blocked') is distinct from 'boolean' then
     v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
    else
     v_query_exclusions:=private.bpay_next_source_query_segment_exclusions_v1(v_member.expected_revision_id,v_gate);
     if v_gate->'blocked'='true'::jsonb and v_query_exclusions is null then v_code:='SOURCE_PAY_QUERY_OPEN';end if;
    end if;
   exception when others then v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end;end if;
  end if;
  if v_code is null and v_member.selection_mode='SUBSET' and v_memory then
   if not exists(select 1 from private.bpay_next_selection_memory_choices_v1(v_ns->'scope',v_baseline,v_intent) l
     where l.work_id=v_member.work_id and l.revision_id=v_member.expected_revision_id and l.selected and l.direction='PAYMENT') then
    v_code:='BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE';end if;
  elsif not v_memory then
  if v_code is null and v_member.selection_mode='SUBSET' and (not exists(
    select 1 from private.bpay_next_workbench_item i
    join jsonb_array_elements(v_intent->'components') a(value)
      on a.value->>'approved_line_id'=i.approved_line_id::text and a.value->'selected'='true'::jsonb
    where i.work_id=v_member.work_id and i.revision_id=v_member.expected_revision_id
      and i.section='READY' and i.selectable and i.amount_known and i.direction='PAYMENT'
      and i.portion_kind in ('AVAILABLE','ZERO'))
    or exists(select 1 from jsonb_array_elements(v_intent->'components') chosen
      where chosen->>'work_id'=v_member.work_id::text and chosen->'selected'='true'::jsonb
        and not exists(select 1 from private.bpay_next_workbench_item i
          where i.work_id=v_member.work_id and i.revision_id=v_member.expected_revision_id
            and i.approved_line_id=(chosen->>'approved_line_id')::uuid and i.section='READY'
            and i.selectable and i.amount_known and i.direction='PAYMENT'
            and i.portion_kind in ('AVAILABLE','ZERO')))) then
   v_code:='BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE';end if;
  end if;
  if v_code is not null then
   update private.bpay_next_run_choice_member set skip_code=v_code
     where run_id=p_run_id and member_kind='WORK' and member_id=v_member.member_id;
  else
   -- Use the existing Rule B objects as the only captured financial authority.
   v_work_count:=v_work_count+1;
   insert into private.bpay_next_run_selection(run_id,selection_no,work_id,candidate_id)
     values(p_run_id,v_work_count,v_member.work_id,v_member.candidate_id);
   insert into private.bpay_next_work_choice(run_id,work_id,expected_revision_id,selection_mode,selection_state)
     values(p_run_id,v_member.work_id,v_member.expected_revision_id,v_member.selection_mode,
       case when v_member.selection_mode='ALL' then 'SEALED' else 'OPEN' end);
   if v_gate is not null then
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc,segment_query_exclusions)
      values(p_run_id,v_member.work_id,v_member.expected_revision_id,v_member.physical_timesheet_id,v_gate,transaction_timestamp(),v_query_exclusions);
   end if;
  end if;
 end loop;
 -- Publish genuine native counters before the unchanged component seal.
 if v_memory then
  update private.bpay_next_pay_run set work_choice_count=v_work_count,
   sealed_work_choice_count=(select count(*) from private.bpay_next_work_choice where run_id=p_run_id and selection_state='SEALED') where id=p_run_id;
  for v_subset in select c.* from private.bpay_next_work_choice c where c.run_id=p_run_id and c.selection_mode='SUBSET' order by c.work_id loop
   v_page_no:=0;
   for v_lines in select array_agg(q.approved_line_id order by q.approved_line_id) from
    (select l.approved_line_id,(row_number() over(order by l.approved_line_id)-1)/100 page_no
     from private.bpay_next_selection_memory_choices_v1(v_ns->'scope',v_baseline,v_intent) l
     where l.work_id=v_subset.work_id and l.revision_id=v_subset.expected_revision_id and l.selected and l.direction='PAYMENT') q
    group by q.page_no order by q.page_no loop
    v_page_no:=v_page_no+1;
    perform private.bpay_next_append_component_selection_page_v1(p_run_id,v_subset.work_id,v_subset.expected_revision_id,v_page_no,v_lines);
   end loop;
   select c.* into strict v_subset from private.bpay_next_work_choice c where c.run_id=p_run_id and c.work_id=v_subset.work_id;
   perform private.bpay_next_seal_component_selection_v1(p_run_id,v_subset.work_id,v_subset.expected_revision_id,v_subset.page_count,v_subset.component_count);
  end loop;
 end if;
 -- Capture the existing Rule B Candidate settings exactly once in this cut.
 insert into private.bpay_next_selection_candidate(run_id,candidate_id,member_no,
   target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,min_take_home_wtd)
 select p_run_id,c.id,row_number() over(order by c.id),upper(c.pay_method),c.umbrella_id,u.vat_chargeable,c.min_take_home_wtd
 from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id
 where exists(select 1 from private.bpay_next_run_choice_member m where m.run_id=p_run_id
   and m.candidate_id=c.id and m.selected and m.skip_code is null
   and (m.member_kind='WORK' or exists(select 1 from private.bpay_next_workbench_item i
     where i.case_component_id=m.case_component_id and i.direction='PAYMENT' and i.section='READY')));
 get diagnostics v_count=row_count;
 -- Optimistic context check, with no meta row lock or downstream recalculation.
 if not exists(select 1 from private.bpay_next_workbench_revision r where r.id=1
   and r.workbench_revision::text=p_descriptor->>'workbench_revision'
   and r.adoption_revision::text=p_descriptor->>'adoption_revision' and r.adoption_state='CURRENT') then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
 if v_count>0 then
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARE');
  insert into private.bpay_next_run_command(run_id,command_id) values(p_run_id,p_command_id);
 end if;
 update private.bpay_next_pay_run set accepted_choice_descriptor=p_descriptor,
   accepted_choice_command_id=p_command_id,selected_candidate_count=v_count,
   selection_count=v_work_count,work_choice_count=v_work_count,
   sealed_work_choice_count=(select count(*) from private.bpay_next_work_choice where run_id=p_run_id and selection_state='SEALED') where id=p_run_id;
 if v_memory then
  update private.bpay_next_selection_mode_memory set command_id=case when v_sequence is not null then p_command_id end,
   agency_sequence=v_sequence where run_id=p_run_id and row_kind='HEADER' and v_sequence is not null;
  for v_subset in select c.work_id from private.bpay_next_work_choice c where c.run_id=p_run_id and c.selection_mode='SUBSET' and c.selection_state='SEALED' order by c.work_id loop
   perform private.bpay_next_materialise_choice_segments_v1(p_run_id,v_subset.work_id);
  end loop;
 end if;
 return v_result||jsonb_build_object('command_id',p_command_id,'agency_sequence',v_sequence::text,
   'selection_count',v_work_count::text,
   'selected_candidate_count',v_count::text,'empty',v_count=0,
   'choice_work_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'),
   'choice_case_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='CASE_COMPONENT'),'replay',false);
end
$function$;

-- Current owner: supabase/repeatable/11102026_0426_banking_pay_next_pay_hold_accepted_choice_v1.sql
create or replace function private.bpay_next_append_work_skip_v1(p_run_id uuid,p_work_id uuid,p_revision_id uuid)
returns text language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_work private.bpay_next_work%rowtype;
 v_revision private.bpay_next_work_revision%rowtype;v_gate jsonb;v_skip text;
begin
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id;
 -- Accepted acknowledgements use the original per-Work skip, including a
 -- held Work with no work_choice. Never read newer hold to amend that offer.
 if v_run.accepted_choice_descriptor is not null then
  select skip_code into v_skip from private.bpay_next_run_choice_member
   where run_id=p_run_id and work_id=p_work_id and member_kind='WORK';
  if found then return v_skip;end if;
  if exists(select 1 from private.bpay_next_work_choice where run_id=p_run_id and work_id=p_work_id
    and expected_revision_id=p_revision_id) then return null;end if;
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_MISSING';
 end if;
 select src.* into strict v_work from private.bpay_next_work src where id=p_work_id for share;
 if p_revision_id is not null and not exists(select 1 from private.bpay_next_work_revision
   where work_id=p_work_id and id=p_revision_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REVISION_CONFLICT';end if;
 select src.* into v_revision from private.bpay_next_work_revision src
   where work_id=p_work_id and id=p_revision_id;
 if v_work.approval_state is distinct from 'APPROVED' or v_revision.approved_at_utc is null
   or v_revision.sealed_at_utc is null then return 'BPAY_NEXT_SELECTION_WORK_NOT_APPROVED';end if;
 if v_work.current_revision_id is distinct from p_revision_id or v_work.applied_revision_id is distinct from p_revision_id then
  return 'BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';end if;
 if private.bpay_next_work_period_proof_v1(v_work.id,p_revision_id,v_run.snooze_business_date)->'valid' is distinct from 'true'::jsonb then
  return 'BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';end if;
 if v_work.week_ending_date-6>v_run.snooze_business_date then
  return 'BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE';end if;
 if not exists(select 1 from public.timesheets t join public.timesheets_financials tf
   on tf.timesheet_id=t.timesheet_id and tf.is_current
   where t.booking_id=v_work.booking_id and t.is_current) then
  return 'BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';end if;
 if exists(select 1 from public.timesheets t join public.timesheets_financials tf
   on tf.timesheet_id=t.timesheet_id and tf.is_current=true
   where t.booking_id=v_work.booking_id and t.is_current=true and tf.pay_on_hold is true) then
  return 'BPAY_NEXT_SELECTION_ON_HOLD';end if;
 if not exists(select 1 from private.bpay_next_revision_worker_basis where revision_id=p_revision_id) then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_BASIS_UNAVAILABLE';end if;
 if exists(select 1 from private.bpay_next_snooze sn
   join lateral(select ev.after_state from private.bpay_next_snooze_event ev
     where ev.snooze_id=sn.id and ev.control_sequence<=v_run.snooze_control_sequence
     order by ev.control_sequence desc limit 1) e on true
   join private.bpay_next_revision_worker_basis b on b.revision_id=p_revision_id
   where sn.work_id=p_work_id and sn.scope_kind='WHOLE_TIMESHEET' and (e.after_state->>'active')::boolean
     and ((e.after_state->>'until_date') is null or (e.after_state->>'until_date')::date>=v_run.snooze_business_date)
     and (e.after_state->>'worker_pay_basis')::bytea=b.whole_basis) then
  return 'BPAY_NEXT_SELECTION_SNOOZED';end if;
 if v_revision.source_kind in ('SOURCE','PROTECTED') then
  begin
   perform private.weekly_source_pay_query_admit_v2();
   v_gate:=private.weekly_source_pay_query_gate_v1(v_revision.physical_timesheet_id);
   if v_gate->'ok' is distinct from 'true'::jsonb or v_gate->>'code' is distinct from 'OK'
     or jsonb_typeof(v_gate->'blocked') is distinct from 'boolean' then
    return 'SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end if;
   if v_gate->'blocked'='true'::jsonb and
     private.bpay_next_source_query_segment_exclusions_v1(p_revision_id,v_gate) is null then return 'SOURCE_PAY_QUERY_OPEN';end if;
  exception when others then return 'SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end;
 end if;
 return null;
end
$function$;

-- Current owner: supabase/repeatable/11102026_1358_banking_pay_next_guarded_create_v1.sql
create or replace function private.bpay_next_seal_selection_v1(
  p_run_id uuid,p_expected_pages bigint,p_expected_count bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_selected record;v_guard jsonb;
begin
  if p_run_id is null or p_expected_pages is null or p_expected_count is null
     or p_expected_pages<0 or p_expected_count<0
     or (p_expected_count>0 and p_expected_pages=0) then
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
  if v_run.accepted_choice_descriptor is not null then
    if exists(select 1 from private.bpay_next_work_choice where run_id=p_run_id and segment_sealed_count is null)
      or v_run.uploaded_selection_count<>v_run.selection_count
      or v_run.uploaded_choice_work_count<>(select count(*) from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK')
      or v_run.exclusion_page_count<>(select greatest(1,(count(*)+99)/100) from private.bpay_next_run_selection_exclusion where run_id=p_run_id)
      or exists(select 1 from private.bpay_next_run_choice_member member
        join private.bpay_next_selection_candidate selected on selected.run_id=member.run_id and selected.candidate_id=member.candidate_id
        where member.run_id=p_run_id and member.member_kind='CASE_COMPONENT' and not exists(
          select 1 from private.bpay_next_case_selection_item item
          join private.bpay_next_case_selection header on header.run_id=item.run_id and header.candidate_id=item.candidate_id and header.selection_revision=item.selection_revision
          where item.run_id=member.run_id and item.candidate_id=member.candidate_id and header.status='SEALED'
            and item.case_component_id=member.case_component_id and item.is_selected=member.selected))
      or exists(select 1 from private.bpay_next_run_selection selected
        join private.bpay_next_work_choice choice on choice.run_id=selected.run_id and choice.work_id=selected.work_id
        join private.bpay_next_work_revision revision on revision.work_id=choice.work_id and revision.id=choice.expected_revision_id
        where selected.run_id=p_run_id and revision.source_kind in ('SOURCE','PROTECTED') and not exists(
          select 1 from private.bpay_next_run_work_query_snapshot snapshot where snapshot.run_id=p_run_id and snapshot.work_id=choice.work_id
            and snapshot.expected_revision_id=choice.expected_revision_id and snapshot.physical_timesheet_id=revision.physical_timesheet_id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_UPLOAD_INCOMPLETE';
    end if;
  else
  -- Owner rule B: capture only the existing ticked expected revisions.
  -- Exact sealed replay above never refreshes settings or query state.
  perform private.weekly_source_pay_query_admit_v2();
  update private.bpay_next_selection_candidate selected
    set target_pay_channel=upper(candidate.pay_method),
        target_umbrella_id=candidate.umbrella_id,
        target_umbrella_vat_chargeable=umbrella.vat_chargeable,
        min_take_home_wtd=candidate.min_take_home_wtd
    from public.candidates candidate left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id
    where selected.run_id=p_run_id and selected.candidate_id=candidate.id;
  for v_selected in
    select choice.work_id,choice.expected_revision_id,revision.physical_timesheet_id
      from private.bpay_next_run_selection selected
      join private.bpay_next_work_choice choice on choice.run_id=selected.run_id and choice.work_id=selected.work_id
      join private.bpay_next_work work on work.id=selected.work_id
      join private.bpay_next_work_revision revision on revision.work_id=choice.work_id and revision.id=choice.expected_revision_id
      where selected.run_id=p_run_id and work.work_kind='SOURCE' and revision.source_kind in ('SOURCE','PROTECTED')
      order by selected.selection_no
  loop
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc,segment_query_exclusions)
      select p_run_id,v_selected.work_id,v_selected.expected_revision_id,v_selected.physical_timesheet_id,
        gate.value,pg_catalog.transaction_timestamp(),
        private.bpay_next_source_query_segment_exclusions_v1(v_selected.expected_revision_id,gate.value)
      from (select private.weekly_source_pay_query_gate_v1(v_selected.physical_timesheet_id) value) gate;
  end loop;
  end if;
  -- DR-10: plain fresh same-week read; no new clock, proof or foreign run locks.
  v_guard:=private.bpay_next_paye_guard_page_v1(v_run.actor_user_id,p_run_id,null);
  if (v_guard->>'guard_applies')::boolean and v_run.paye_guard_decision='NONE' then
   raise exception using errcode='55000',message='BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNRESOLVED';end if;
  if v_run.paye_guard_decision in ('VERIFY_BOTH','VERIFY_PAYE') and (v_guard->>'unexecuted')::boolean then
   raise exception using errcode='55000',message='BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNEXECUTED';end if;
  -- Acknowledge only the original captured boundary, never current inputs.
  update private.bpay_next_selection_mode_memory set acknowledged_at_utc=transaction_timestamp()
   where run_id=p_run_id and row_kind='HEADER' and acknowledged_at_utc is null
    and command_id=v_run.accepted_choice_command_id and agency_sequence is not null;
  update private.bpay_next_pay_run
    set selection_state='SEALED',review_revision=review_revision+1
    where id=p_run_id;
  return jsonb_build_object('run_id',p_run_id,'sealed',true,
    'selection_count',v_run.selection_count,'replay',false);
end
$function$;

-- Current owner: supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql
create or replace function private.bpay_next_append_segment_selection_page_v1(
  p_run_id uuid,p_work_id uuid,p_expected_revision_id uuid,p_page_no bigint,p_request_id uuid,p_segment_keys text[])
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype; v_choice private.bpay_next_work_choice%rowtype;
  v_page private.bpay_next_segment_selection_page%rowtype; v_segment private.bpay_next_approved_segment%rowtype;
  v_n integer; v_i integer:=0; v_key text; v_body jsonb; v_snooze jsonb;v_query_reason text;
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
    v_query_reason:=null;
    select reason.value->>'reason' into v_query_reason
      from private.bpay_next_run_work_query_snapshot snapshot
      cross join lateral jsonb_array_elements(snapshot.segment_query_exclusions) reason(value)
      where snapshot.run_id=p_run_id and snapshot.work_id=p_work_id
        and snapshot.expected_revision_id=p_expected_revision_id and reason.value->>'segment_key'=v_key
      order by reason.value->>'reason' limit 1;
    insert into private.bpay_next_selected_segment(run_id,work_id,expected_revision_id,segment_key,component_key,
      approved_segment_id,selection_no,disposition,snooze_id,snooze_version,control_sequence,query_hold_reason)
    values(p_run_id,p_work_id,p_expected_revision_id,v_key,v_segment.component_key,v_segment.id,
      v_choice.segment_count+v_i,case when v_snooze is null then 'INCLUDED' else 'SNOOZED' end,
      nullif(v_snooze->>'id','')::uuid,nullif(v_snooze->>'version','')::bigint,v_choice.segment_control_sequence,v_query_reason);
  end loop;
  update private.bpay_next_work_choice set segment_page_count=p_page_no,segment_count=segment_count+v_n
    where run_id=p_run_id and work_id=p_work_id;
  return jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
    'page_no',p_page_no::text,'item_count',v_n::text,'segment_count',(v_choice.segment_count+v_n)::text,'replay',false);
end
$function$;

-- Current owner: supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql
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
  v_snapshot jsonb;v_query_reason text;v_balance record;v_count bigint;v_disposition text;v_amount numeric;v_recovery numeric;v_overhang numeric;
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
      v_query_reason:=null;
      select reason.value->>'reason' into v_query_reason
        from private.bpay_next_run_work_query_snapshot snapshot
        cross join lateral jsonb_array_elements(snapshot.segment_query_exclusions) reason(value)
        where snapshot.run_id=new.run_id and snapshot.work_id=new.work_id
          and snapshot.expected_revision_id=new.expected_revision_id and reason.value->>'segment_key'=new.segment_key
        order by reason.value->>'reason' limit 1;
      if new.query_hold_reason is distinct from v_query_reason then
        raise exception using errcode='23514',message='BPAY_NEXT_SELECTED_SEGMENT_QUERY_SCOPE_INVALID';end if;
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
    select case when s.query_hold_reason is not null then 'NOT_SELECTED' else s.disposition end into v_disposition from private.bpay_next_selected_segment s
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

-- Current owner: supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql
create or replace function private.bpay_next_capture_segment_page_v1(p_run_worker_id uuid,p_limit integer)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare
  v_state private.bpay_next_run_segment_capture%rowtype;
  v_member record; v_current private.bpay_next_approved_segment%rowtype;
  v_parent private.bpay_next_run_position%rowtype; v_selected private.bpay_next_selected_segment%rowtype;
  v_worker private.bpay_next_run_worker%rowtype; v_segment_id uuid; v_disposition text;
  v_balance record; v_totals record;v_target_totals record;v_collection private.bpay_next_work_collection%rowtype;
  v_needs_resolution boolean;v_retry_seen integer:=0;v_retry record;v_seen integer:=0; v_a numeric;v_p numeric;v_r numeric;v_h numeric;
begin
  if p_limit is null or p_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_SEGMENT_CAPTURE_LIMIT_INVALID';end if;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=p_run_worker_id for update;
  if not private.bpay_next_segment_prepare_owner_v1(p_run_worker_id,'RESERVE') then
    raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_CAPTURE_OWNER_STALE';end if;
  select s.* into v_state from private.bpay_next_run_segment_capture s where s.run_worker_id=p_run_worker_id for update;
  if not found then
    insert into private.bpay_next_run_segment_capture(run_worker_id,expected_count)
      select p_run_worker_id,coalesce(sum(p.segment_expected_member_count),0)
      from private.bpay_next_run_position p where p.run_worker_id=p_run_worker_id;
    select s.* into strict v_state from private.bpay_next_run_segment_capture s where s.run_worker_id=p_run_worker_id for update;
  end if;
  if v_state.complete then return jsonb_build_object('done',true,'rows_visited',0);end if;
  -- Recheck only spent incomplete parents, before advancing the original seek.
  for v_retry in select run_work_id,component_key,work_id from private.bpay_next_run_position
    where run_worker_id=p_run_worker_id and not segment_attribution_complete
      and segment_member_count=segment_expected_member_count and segment_member_count>0
    order by work_id,component_key limit p_limit loop
    v_retry_seen:=v_retry_seen+1;
    if not private.bpay_next_complete_retained_segment_parent_v1(p_run_worker_id,v_retry.run_work_id,v_retry.component_key) then
      update private.bpay_next_run_worker set status='REVIEW',review_issue_code='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE',
        review_issue_work_id=v_retry.work_id where id=p_run_worker_id;
      return jsonb_build_object('done',false,'rows_visited',v_retry_seen);
    end if;
  end loop;
  if v_retry_seen=p_limit then return jsonb_build_object('done',false,'rows_visited',v_retry_seen);end if;

  for v_member in select s.* from private.bpay_next_run_segment_members_v1(p_run_worker_id,
    v_state.cursor_work_id,v_state.cursor_component_key,v_state.cursor_segment_key,p_limit-v_retry_seen) s
  loop
    select p.* into strict v_parent from private.bpay_next_run_position p
      where p.run_work_id=v_member.run_work_id and p.component_key=v_member.component_key for update;
    select s.* into v_current from private.bpay_next_approved_segment s
      where s.revision_id=v_member.captured_revision_id and s.segment_key=v_member.segment_key;
    select s.* into v_selected from private.bpay_next_selected_segment s
      where s.run_id=v_worker.run_id and s.work_id=v_member.work_id and s.segment_key=v_member.segment_key;
    v_disposition:=case when v_current.id is null then 'REMOVED'
      when v_selected.approved_segment_id=v_current.id and v_selected.query_hold_reason is null then v_selected.disposition else 'NOT_SELECTED' end;
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
      if not private.bpay_next_complete_retained_segment_parent_v1(p_run_worker_id,v_member.run_work_id,v_member.component_key) then
        update private.bpay_next_run_segment_capture set cursor_work_id=v_member.work_id,
          cursor_component_key=v_member.component_key,cursor_segment_key=v_member.segment_key,
          processed_count=v_state.processed_count+v_seen+1 where run_worker_id=p_run_worker_id;
        update private.bpay_next_run_worker set status='REVIEW',review_issue_code='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE',
          review_issue_work_id=v_member.work_id where id=p_run_worker_id;
        return jsonb_build_object('done',false,'rows_visited',v_retry_seen+v_seen+1);
      end if;
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
  return jsonb_build_object('done',v_state.complete,'rows_visited',v_retry_seen+v_seen);
end
$function$;

-- Current owner: supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql
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
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
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
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
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
      v_issue_work_id:=v_member.work_id;exit;
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
        v_issue_work_id:=v_row.work_id;exit;
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
      if found and (private.bpay_next_prepare_job_run_v1(v_query_pin.prepare_job_id) is distinct from v_run_id
        or (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text)) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        -- Only the original cut's complete, exact named shift exclusions
        -- can bypass a whole-Work blocker. Never read today's query here.
        if not exists(select 1 from private.bpay_next_run_work_query_snapshot snapshot
          where snapshot.run_id=v_run_id and snapshot.work_id=v_row.work_id
            and snapshot.expected_revision_id=v_row.captured_revision_id
            and snapshot.physical_timesheet_id::text=(v_query_pin.scope->>'root_timesheet_id')
            and jsonb_typeof(snapshot.segment_query_exclusions)='array'
            and jsonb_array_length(snapshot.segment_query_exclusions)>0)
        then v_issue:='SOURCE_PAY_QUERY_OPEN';end if;
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
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
  update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
  if v_issue is not null then
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_issue_work_id where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
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
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
    end if;
    v_segment_result:=private.bpay_next_capture_segment_page_v1(v_worker.id,p_limit-v_seen);
    v_segment_visited:=(v_segment_result->>'rows_visited')::integer;
    select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker.id;
    if v_worker.status='REVIEW' then
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen+v_segment_visited);
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

-- Current owner: supabase/repeatable/11102026_0426_banking_pay_next_pay_hold_classification_v1.sql
create or replace function private.bpay_next_workbench_refresh_work_core_v1(p_work_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_item private.bpay_next_workbench_item%rowtype;v_base private.bpay_next_workbench_item%rowtype;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_member record;v_accepted record;v_totals record;v_candidate uuid;v_client uuid;v_target text;v_financial bigint;v_projection bigint;
  v_group text:='WORK:'||p_work_id::text;v_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
  v_sunday date;v_sequence bigint;v_reason text;v_priority smallint;v_section text;v_complete boolean;
  v_pay_hold record;v_hold_canonical boolean;
  v_query_gate jsonb;v_query_scope jsonb;v_query_loaded boolean:=false;v_query_state text:='UNAVAILABLE';v_query_exclusions jsonb;v_segment_query_state text;
  v_wait_before boolean;v_wait_after boolean;v_case_id uuid;v_withdrawn_complete boolean;v_was_dirty boolean;
  v_positive numeric;v_recovery numeric;v_overhang numeric;v_snooze jsonb;v_count bigint:=0;v_portion text;v_zero boolean;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id for update;
  -- Exact current booking authority; no historical/original-row fallback.
  select t.timesheet_id,tf.id financial_id,coalesce(tf.pay_on_hold,false) pay_on_hold,
    case when tf.pay_on_hold then tf.pay_on_hold_reason end pay_on_hold_reason into v_pay_hold
    from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.booking_id=v_work.booking_id and t.is_current;
  v_hold_canonical:=v_pay_hold.timesheet_id is not null and v_pay_hold.financial_id is not null;
  -- Read stored WAIT before replacing it; the withdrawal owner already changed work.
  select exists(select 1 from private.bpay_next_workbench_item i where i.group_key=v_group
    and i.reason_code='AWAITING_REAUTHORISATION') into v_wait_before;
  v_wait_after:=private.bpay_next_work_wait_state_v1(v_work.id);
  select exists(select 1 from private.bpay_next_workbench_group g where g.group_key=v_group and g.dirty) into v_was_dirty;
  -- Reauthorisation becomes effective only after final position application.
  -- Keep its stored WAIT marker during Processing for the final boundary hook.
  if v_wait_before and v_work.approval_state='APPROVED'
    and v_work.current_revision_id is distinct from v_work.applied_revision_id then
    update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
      dirty_group_count=dirty_group_count+case when v_was_dirty then 0 else 1 end,
      updated_at_utc=transaction_timestamp() where id=1 returning workbench_revision into v_projection;
    update private.bpay_next_workbench_group set complete=false,dirty=true,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    update private.bpay_next_workbench_item set selectable=false,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    return;
  end if;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select c.client_id into v_client from public.contracts c where c.id=v_work.contract_id;
  select r.* into v_revision from private.bpay_next_work_revision r
    where r.id=case when v_work.approval_state='WITHDRAWN' then v_work.applied_revision_id else v_work.current_revision_id end
      and r.work_id=v_work.id;
  v_complete:=v_work.approval_state='APPROVED' and v_revision.id is not null and v_revision.sealed_at_utc is not null
    and v_work.applied_revision_id=v_work.current_revision_id
    and (select count(*) from private.bpay_next_approved_line a where a.revision_id=v_revision.id)=v_revision.expected_line_count;
  v_withdrawn_complete:=v_work.approval_state='WITHDRAWN' and v_revision.id is not null
    and v_revision.sealed_at_utc is not null and not exists(select 1 from private.bpay_next_position p
      where p.work_id=v_work.id and p.applied_revision_id is distinct from v_work.applied_revision_id
        and (p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
          or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
          or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0));
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    dirty_group_count=dirty_group_count-case when v_was_dirty then 1 else 0 end,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,work_id,candidate_id,client_id,contract_id,
    physical_timesheet_id,current_revision_id,source_pay_channel,target_pay_channel,week_ending_date,
    complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'WORK',v_work.id,v_candidate,v_client,v_work.contract_id,v_revision.physical_timesheet_id,v_work.current_revision_id,
    v_revision.source_pay_channel,v_target,v_work.week_ending_date,coalesce(v_complete,false),v_projection,v_financial,v_metadata.metadata_version,
    case when v_complete then 'CURRENT' when v_work.approval_state='APPROVED' then 'PROCESSING' else 'UNAVAILABLE' end)
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    physical_timesheet_id=excluded.physical_timesheet_id,current_revision_id=excluded.current_revision_id,
    source_pay_channel=excluded.source_pay_channel,target_pay_channel=excluded.target_pay_channel,week_ending_date=excluded.week_ending_date,
    complete=excluded.complete,dirty=false,projection_revision=excluded.projection_revision,financial_view_revision=excluded.financial_view_revision,
    metadata_version=excluded.metadata_version,progress_state=excluded.progress_state,error_code=null,updated_at_utc=transaction_timestamp();
  v_sunday:=v_today+(7-extract(isodow from v_today)::integer);
  select last_sequence into strict v_sequence from private.bpay_next_command_clock where id=1;
  if v_work.approval_state='WITHDRAWN' then
    -- Native retained positions supply A/C/H directly. No zero revision, job,
    -- debt, current residual offer, segment repricing or Case mutation here.
    for v_member in select p.component_key,p.approved_source_ex_vat approved_amount,
      p.realised_source_ex_vat realised_amount,p.held_source_ex_vat held_amount,p.source_basis_channel,
      a.id approved_line_id,a.component_kind,a.tax_treatment,a.work_date,a.approved_quantity,a.unit_label
      from private.bpay_next_position p left join private.bpay_next_approved_line a
        on a.revision_id=v_revision.id and a.component_key=p.component_key
      where p.work_id=v_work.id and v_withdrawn_complete and (p.applied_revision_id=v_work.applied_revision_id
        or p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
        or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
        or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0) order by p.component_key loop
      v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
      v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
      v_item.component_key:=v_member.component_key;v_item.member_kind:='WORK_COMPONENT';v_item.member_key:=v_member.component_key;
      v_item.portion_kind:='WITHDRAWN';v_item.direction:='INFORMATIONAL';v_item.payroll_stage:='GROSS_ADD';
      v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_item.week_ending_date:=v_work.week_ending_date;v_item.work_date:=v_member.work_date;
      v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
      v_item.client_display_name:=v_revision.client_display_name;v_item.timesheet_reference:=v_revision.timesheet_reference;
      v_item.component_kind:=v_member.component_kind;v_item.tax_treatment:=v_member.tax_treatment;
      v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
      v_item.source_pay_channel:=v_member.source_basis_channel;v_item.target_pay_channel:=v_target;
      v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_member.approved_amount;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=v_member.held_amount;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;v_item.written_off_source_ex_vat:=0;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.reason_priority:=case when v_wait_after then 70 else 20 end;v_item.selectable:=false;
      v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
      v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
      insert into private.bpay_next_workbench_item select (v_item).*;
    end loop;
    update private.bpay_next_workbench_group set complete=v_count>0,certified_zero=false,
      progress_state=case when v_withdrawn_complete then 'CURRENT' when v_work.applied_revision_id is not null then 'PROCESSING' else 'UNAVAILABLE' end
      where group_key=v_group;
  else
  for v_member in select m.component_key,m.approved_line_id,m.approved_segment_id,m.segment_key,m.component_kind,m.tax_treatment,m.work_date,m.approved_quantity,m.unit_label,m.approved_amount,m.realised_amount,m.held_amount,m.source_pay_channel,m.parent_complete,m.segment_attribution_complete from private.bpay_next_workbench_work_members_v1(v_work.id,v_revision.id) m order by m.component_key,m.segment_key nulls first loop
    v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
    v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
    v_item.approved_segment_id:=v_member.approved_segment_id;v_item.segment_key:=v_member.segment_key;
    v_item.component_key:=v_member.component_key;v_item.member_kind:=case when v_member.segment_key is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end;
    v_item.member_key:=coalesce(v_member.segment_key,v_member.component_key);v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;
    v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;v_item.work_date:=v_member.work_date;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;
    v_item.candidate_reference:=v_metadata.tms_ref;v_item.client_display_name:=v_revision.client_display_name;
    v_item.timesheet_reference:=v_revision.timesheet_reference;v_item.component_kind:=v_member.component_kind;
    v_item.tax_treatment:=v_member.tax_treatment;v_item.source_pay_channel:=v_member.source_pay_channel;v_item.target_pay_channel:=v_target;
    v_item.direction:='PAYMENT';v_item.payroll_stage:='GROSS_ADD';v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
    v_item.amount_known:=coalesce(v_complete,false) and v_member.parent_complete and v_member.segment_attribution_complete;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_complete then 'CURRENT' else 'PROCESSING' end;v_item.updated_at_utc:=transaction_timestamp();
    v_reason:='READY';v_priority:=200;v_section:='READY';
    if v_revision.id is null or v_target is null or v_work.week_ending_date is null
      or extract(isodow from v_work.week_ending_date)<>7 then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif v_work.approval_state<>'APPROVED' then v_reason:='MISSING_APPROVAL';v_priority:=21;v_section:='BLOCKED';
    elsif not v_complete then v_reason:='APPLY_PENDING';v_priority:=30;v_section:='BLOCKED';
    elsif not v_member.parent_complete then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif not v_member.segment_attribution_complete then v_reason:='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE';v_priority:=12;v_section:='BLOCKED';end if;
    if v_item.amount_known then
      v_positive:=greatest(v_member.approved_amount-v_member.realised_amount-v_member.held_amount,0);
      v_recovery:=greatest(v_member.realised_amount-v_member.approved_amount,0);
      v_overhang:=greatest(v_member.held_amount-greatest(v_member.approved_amount-v_member.realised_amount,0),0);
      v_item.nominal_source_ex_vat:=v_positive;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=0;
      v_item.written_off_source_ex_vat:=0;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=v_positive;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=v_recovery;v_item.held_overhang_source_ex_vat:=v_overhang;
    else v_positive:=null;v_recovery:=null;v_overhang:=null;end if;
    select a.run_id,a.command_id,a.module_epoch,a.selected_source_ex_vat into v_accepted from private.bpay_next_workbench_accepted_work_v1(v_work.id,v_member.component_key,v_member.segment_key) a;
    if found then
      v_item.is_selected:=true;v_item.accepted_run_id:=v_accepted.run_id;v_item.accepted_command_id:=v_accepted.command_id;
      v_item.accepted_module_epoch:=v_accepted.module_epoch;v_item.selected_source_ex_vat:=v_accepted.selected_source_ex_vat;
      v_item.selected_amount_known:=v_accepted.selected_source_ex_vat is not null;
      if v_priority>50 then v_reason:='IN_PREPARATION';v_priority:=50;v_section:='WAITING';end if;
    end if;
    v_snooze:=private.bpay_next_snooze_at_cut_v1(v_work.id,v_revision.id,v_member.segment_key,v_member.component_key,v_sequence,v_today);
    if v_snooze is not null then
      v_item.snooze_id:=(v_snooze->>'id')::uuid;v_item.snooze_version:=(v_snooze->>'version')::bigint;
      v_item.snooze_scope:=v_snooze->>'scope_kind';v_item.snooze_until_date:=(v_snooze->>'until_date')::date;
      if v_positive>0 then v_item.excluded_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=0;end if;
    elsif exists(select 1 from private.bpay_next_snooze s where s.work_id=v_work.id and s.active
      and (s.until_date is null or s.until_date>=v_today) and ((s.scope_kind='SEGMENT' and s.segment_key=v_member.segment_key)
        or s.scope_kind='WHOLE_TIMESHEET' or (s.scope_kind='EXPENSE_LINE' and s.component_key=v_member.component_key)
        or (s.scope_kind='EXPENSES_ALL' and v_member.component_kind in ('EXPENSE','MILEAGE')))) then
      v_item.stale_snooze_code:=case when v_member.segment_key is not null and v_member.approved_segment_id is null
        then 'SNOOZE_SEGMENT_STALE' else 'SNOOZE_BASIS_STALE' end;
    end if;
    if v_priority>100 and v_work.week_ending_date>v_sunday then
      v_reason:='FUTURE_WEEK';v_priority:=100;v_section:='WAITING';v_item.eligible_from_date:=v_work.week_ending_date-6;
    elsif v_priority>110 and v_snooze is not null and v_positive>0 then v_reason:='SNOOZED';v_priority:=110;v_section:='WAITING';end if;
    v_item.query_qualified:=coalesce(v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED')
      and v_member.component_kind in ('WORK','PROTECTED_WORK') and v_revision.source_event_id is not null
      and v_revision.physical_timesheet_id is not null and v_item.amount_known and v_item.included_source_ex_vat>0,false);
    if v_item.query_qualified then v_item.query_state:='UNAVAILABLE';end if;
    v_item.section:=v_section;v_item.reason_code:=v_reason;v_item.reason_priority:=v_priority;
    v_item.pay_hold:=coalesce(v_pay_hold.pay_on_hold,false);v_item.pay_hold_reason:=v_pay_hold.pay_on_hold_reason;
    v_item.selectable:=v_section='READY' and v_item.amount_known;v_base:=v_item;
    if not v_item.amount_known or v_positive>0 or (v_member.approved_amount=0 and v_member.realised_amount=0
      and v_member.held_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null) then
      v_item.portion_kind:=case when not v_item.amount_known then 'EVIDENCE' when v_positive=0 then 'ZERO' else 'AVAILABLE' end;
      -- WB40: only a new positive offer is blocked. The saved v_base keeps
      -- native H/R and accepted Draft selection untouched (owner E1/E3).
      if not v_item.is_selected and not v_hold_canonical and v_item.reason_priority>15 then
        v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=15;v_item.section:='BLOCKED';
      elsif not v_item.is_selected and v_item.pay_hold and v_item.reason_priority>40 then
        v_item.reason_code:='PAY_ON_HOLD';v_item.reason_priority:=40;v_item.section:='BLOCKED';
      end if;
      v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      -- WB-07: query classification belongs only to this new positive portion.
      -- The original pre-query v_base/v_priority remain unchanged for H/R.
      if v_item.query_qualified and v_item.portion_kind='AVAILABLE' then
        if not v_query_loaded then
          v_query_gate:=null;v_query_state:='UNAVAILABLE';
          begin
            v_query_gate:=private.weekly_source_pay_query_gate_v1(v_revision.physical_timesheet_id);
          exception when others then
            v_query_gate:=null;
          end;
          v_query_loaded:=true;
          -- Guard object types separately: malformed scalar replies cannot
          -- reach the object-minus-keys operators or escape the unavailable path.
          if jsonb_typeof(v_query_gate)='object' then
            if (v_query_gate ?& array['ok','code','scope','blocked','query_state_sha256']
                and v_query_gate-array['ok','code','scope','blocked','query_state_sha256']='{}'::jsonb
                and v_query_gate->'ok'='true'::jsonb and v_query_gate->>'code'='OK'
                and jsonb_typeof(v_query_gate->'code')='string'
                and jsonb_typeof(v_query_gate->'blocked')='boolean'
                and jsonb_typeof(v_query_gate->'query_state_sha256')='string'
                and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$') is true then
              v_query_scope:=v_query_gate->'scope';
              if jsonb_typeof(v_query_scope)='object' then
                if (v_query_scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
                    'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
                    and v_query_scope-array['root_timesheet_id','family_booking_id','target_family_id',
                      'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
                    and v_query_scope->'root_timesheet_id'=to_jsonb(v_revision.physical_timesheet_id::text)
                    and v_query_scope->'family_booking_id'=to_jsonb(v_work.booking_id)
                    and v_query_scope->'candidate_id'=to_jsonb(v_work.candidate_id::text)
                    and v_query_scope->'client_id'=to_jsonb(v_client::text)
                    and v_query_scope->'contract_id'=to_jsonb(v_work.contract_id::text)
                    and v_query_scope->'week_ending_date'=to_jsonb(v_work.week_ending_date::text)
                    and v_query_scope->'root_version'=to_jsonb(v_revision.physical_timesheet_version::text)
                    and ((v_query_scope->'target_family_id'='null'::jsonb
                        and v_query_scope->'family_bound_version'='null'::jsonb)
                      or (jsonb_typeof(v_query_scope->'target_family_id')='string'
                        and v_query_scope->>'target_family_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        and jsonb_typeof(v_query_scope->'family_bound_version')='string'
                        and v_query_scope->>'family_bound_version' ~ '^[1-9][0-9]{0,18}$'
                        and (char_length(v_query_scope->>'family_bound_version')<19
                          or (v_query_scope->>'family_bound_version') collate "C"<='9223372036854775807' collate "C")))) is true then
                  v_query_state:=case when v_query_gate->'blocked'='true'::jsonb then 'OPEN' else 'CLEAR' end;
                  v_query_exclusions:=private.bpay_next_source_query_segment_exclusions_v1(v_revision.id,v_query_gate);
                end if;
              end if;
            end if;
          end if;
        end if;
        v_segment_query_state:=v_query_state;
        if v_query_state='OPEN' and v_query_exclusions is not null and v_member.segment_key is not null then
          v_segment_query_state:=case when exists(select 1 from jsonb_array_elements(v_query_exclusions) reason(value)
            where reason.value->>'segment_key'=v_member.segment_key) then 'OPEN' else 'CLEAR' end;
        end if;
        v_item.query_state:=v_segment_query_state;
        if v_segment_query_state='UNAVAILABLE' and v_item.reason_priority>60 then
          v_item.reason_code:='PAY_QUERY_UNAVAILABLE';v_item.reason_priority:=60;v_item.section:='BLOCKED';
        elsif v_segment_query_state='OPEN' and v_item.reason_priority>61 then
          v_item.reason_code:='PAY_QUERY_OPEN';v_item.reason_priority:=61;v_item.section:='ACTION';
        end if;
        v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    elsif v_recovery>0 then
      v_item.portion_kind:='RECOVERY';v_item.direction:='INFORMATIONAL';v_item.selectable:=false;
      if v_priority>83 then v_item.reason_code:='RECOVERY_MANAGED';v_item.reason_priority:=83;v_item.section:='ACTION';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_item.amount_known and v_member.held_amount>0 then
      v_item:=v_base;v_item.id:=gen_random_uuid();v_item.portion_kind:='RESERVED';v_item.selectable:=false;
      v_item.active_reserved_source_ex_vat:=v_member.held_amount;v_item.nominal_source_ex_vat:=v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      if v_positive>0 or v_recovery>0 then
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.residual_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
      end if;
      if v_priority>111 then v_item.reason_code:='RESERVED';v_item.reason_priority:=111;v_item.section:='WAITING';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_base.amount_known and (v_positive>0 or v_recovery>0 or v_member.held_amount>0
      or (v_member.approved_amount=0 and v_member.realised_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null)) then
      select sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
        sum(i.residual_source_ex_vat) residual,
        sum(i.included_source_ex_vat+i.excluded_source_ex_vat-i.recovery_source_ex_vat-i.held_overhang_source_ex_vat) split_residual
        into v_totals from private.bpay_next_workbench_item i where i.group_key=v_group
          and i.component_key=v_member.component_key and i.member_kind=v_base.member_kind and i.member_key=v_base.member_key;
      if (v_totals.a,v_totals.c,v_totals.h,v_totals.residual,v_totals.split_residual)
        is distinct from (v_member.approved_amount,v_member.realised_amount,v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_NATIVE_MEMBER_NOT_CONSERVED';end if;
    end if;
  end loop;
  end if;
  if v_count=0 then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.work_id:=v_work.id;
    v_item.revision_id:=v_revision.id;v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_work.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;v_item.selected_amount_known:=false;v_item.is_selected:=false;v_item.selectable:=false;
    v_item.section:='BLOCKED';v_item.reason_code:=case when v_work.approval_state<>'APPROVED' then 'MISSING_APPROVAL' else 'DATA_UNAVAILABLE' end;
    if v_work.approval_state='WITHDRAWN' then
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
    end if;
    v_item.reason_priority:=case when v_work.approval_state='WITHDRAWN' then case when v_wait_after then 70 else 20 end
      when v_work.approval_state<>'APPROVED' then 21 else 10 end;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_work.approval_state='WITHDRAWN' and v_work.applied_revision_id is not null
      and not v_withdrawn_complete then 'PROCESSING' else 'UNAVAILABLE' end;
    v_item.updated_at_utc:=transaction_timestamp();v_item.target_pay_channel:=v_target;
    v_zero:=coalesce(v_complete,false) and v_revision.certified_zero and v_revision.approved_source_ex_vat=0
      and v_revision.expected_line_count=0 and v_target is not null and v_work.week_ending_date is not null
      and extract(isodow from v_work.week_ending_date)=7
      and not exists(select 1 from private.bpay_next_position p where p.work_id=v_work.id);
    if v_zero then
      v_item.section:='READY';v_item.reason_code:='CERTIFIED_ZERO';v_item.reason_priority:=200;v_item.progress_state:='CURRENT';
      if v_work.week_ending_date>v_sunday then
        v_item.section:='WAITING';v_item.reason_code:='FUTURE_WEEK';v_item.reason_priority:=100;v_item.eligible_from_date:=v_work.week_ending_date-6;
      end if;
    end if;
    insert into private.bpay_next_workbench_item select (v_item).*;
  end if;
  update private.bpay_next_workbench_group g set ready_count=x.ready,action_count=x.action,blocked_count=x.blocked,waiting_count=x.waiting,
    complete=g.complete and x.unknown=0,certified_zero=coalesce(v_complete,false) and coalesce(v_revision.certified_zero,false)
    from(select count(*) filter(where section='READY') ready,count(*) filter(where section='ACTION') action,
      count(*) filter(where section='BLOCKED') blocked,count(*) filter(where section='WAITING') waiting,
      count(*) filter(where not amount_known and not (member_kind='GROUP_EVIDENCE' and coalesce(v_zero,false))) unknown
      from private.bpay_next_workbench_item where group_key=v_group) x where g.group_key=v_group;
  update private.bpay_next_workbench_group g set stale_snooze_count=x.n,
    stale_snooze_code=case when x.n>0 then 'SNOOZE_SEGMENT_STALE' end
    from(select count(*) n from private.bpay_next_snooze s where s.work_id=v_work.id and s.scope_kind='SEGMENT'
      and s.active and (s.until_date is null or s.until_date>=v_today) and not exists(select 1
        from private.bpay_next_approved_segment a where a.revision_id=v_revision.id and a.segment_key=s.segment_key)) x
    where g.group_key=v_group;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
  perform private.bpay_next_workbench_refresh_group_display_v1(v_group);
end
$function$;

-- Current owner: supabase/repeatable/10102026_0806_banking_pay_next_first_authorise_covered_hours_gate_v1.sql
CREATE OR REPLACE FUNCTION private.weekly_source_first_authorise_core_v1(p_timesheet_id uuid, p_expected_row_signature text, p_actor_user_id uuid, p_lock_result jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_family jsonb;

  v_elsewhere uuid;
  v_actor public.tms_users%rowtype;
  v_authorise jsonb;
  v_root public.timesheets%rowtype;
  v_contract_week_id uuid;
  v_signature text;
  v_signature_json jsonb;
  v_generation integer;
  v_agency_id uuid;
  v_protected_hashes text[];
  v_evidence_reference jsonb;
  v_decision_digest bytea;
  v_id uuid;
begin
  if p_timesheet_id is null or p_actor_user_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_REQUEST_INVALID',
      'retryable',false,'reason','TIMESHEET_AND_ACTOR_REQUIRED');
  end if;

  select * into v_actor from public.tms_users where id=p_actor_user_id;
  if not found or not coalesce(v_actor.is_active,false) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_ACTOR_INVALID',
      'retryable',false,'reason','ACTOR_NOT_FOUND_OR_INACTIVE');
  end if;

  -- proof/34 section 5 steps 2 and 3: the lock result is the proof that the
  -- deadlock-free lock set is held and the only source of the canonical answer.
  if coalesce((p_lock_result->>'ok')::boolean,false) is not true
     or coalesce(p_lock_result->>'gate','')<>'GRANTED' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_LOCK_RESULT_INVALID',
      'retryable',false,'reason','LOCK_RESULT_NOT_GRANTED');
  end if;

  select family_element.value
    into v_family
  from pg_catalog.jsonb_array_elements(coalesce(p_lock_result->'families','[]'::jsonb))
    as family_element(value)
  where (family_element.value->>'requested_timesheet_id')::uuid=p_timesheet_id;

  if v_family is null
     or coalesce((v_family->>'requested_is_canonical')::boolean,false) is not true
     or coalesce((v_family->>'family_is_current')::boolean,false) is not true
     or (v_family->>'canonical_timesheet_id')::uuid is distinct from p_timesheet_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
      'retryable',false,'timesheet_id',p_timesheet_id,
      'canonical_timesheet_id',v_family->>'canonical_timesheet_id');
  end if;

  -- The outer owner still holds its accepted family locks. This core
  -- addresses the live financial authorisation directly by frozen work key;
  -- it never treats obsolete Timesheet hours as current entitlement.
  -- WP-07b finding F2: a live generation anywhere ELSE in the family means the
  -- authorised root was rotated after its first authorisation.  proof/34
  -- section 6: "the operation stops with WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE,
  -- no financial change is made, and Office review is required."  Refused
  -- before the ordinary Authorise owner is called, so nothing is written.
  select live_row.root_timesheet_id
    into v_elsewhere
  from public.weekly_source_root_authorisations as live_row
  where live_row.family_booking_id=v_family->>'family_booking_id'
    and live_row.root_timesheet_id is distinct from p_timesheet_id
    and live_row.withdrawn_at_utc is null
  limit 1;
  if v_elsewhere is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'retryable',false,'reason','LIVE_AUTHORISATION_NOT_ON_CANONICAL_ROW',
      'timesheet_id',p_timesheet_id,
      'live_authorisation_root_timesheet_id',v_elsewhere,
      'family_booking_id',v_family->>'family_booking_id');
  end if;

  -- A live generation on the requested row means the root is already an
  -- authorised managed root; re-authorisation is only ever the path after a
  -- withdrawal.
  if exists (select 1 from public.weekly_source_root_authorisations as live_row
             where live_row.root_timesheet_id=p_timesheet_id
               and live_row.withdrawn_at_utc is null) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_ALREADY_AUTHORISED',
      'retryable',false,'timesheet_id',p_timesheet_id);
  end if;

  -- The Office projection has always described this as a first-authorisation
  -- hold. Enforce it in the command while the canonical family locks are held,
  -- using the Timesheet's work week rather than the source cutoff period.
  select * into v_root from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then
    return pg_catalog.jsonb_build_object('ok',false,
      'code','WEEKLY_SOURCE_FIRST_AUTHORISE_TIMESHEET_MISSING','retryable',false);
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_QUERY_FAMILY:'||v_root.contract_id::text||':'||v_root.week_ending_date::text,0));
  -- SP-03: retain ordinary/new-B scope while qualifying actual query exceptions.
  perform private.weekly_source_office_authority_v1(p_actor_user_id,'APPROVE_PROTECTED_PAY');
  if exists (
    select 1 from private.weekly_source_manual_reviews manual_review
    where manual_review.contract_id=v_root.contract_id
      and manual_review.work_date between v_root.week_ending_date-6 and v_root.week_ending_date
      and manual_review.state='OPEN'
      and not exists(select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
        where incident.state='OPEN' and incident.work_event_id=manual_review.work_event_id
          and incident.candidate_id=manual_review.candidate_id and incident.client_id=manual_review.client_id
          and incident.source_group_id=manual_review.source_group_id
          and comparison.incident_id=incident.id and comparison.contract_id=manual_review.contract_id
          and comparison.issue_family='SOURCE_HOURS_DIFFER'
          and not exists(select 1 from public.weekly_issue_comparison_revisions newer
            where newer.incident_id=incident.id and newer.revision_number>comparison.revision_number))
  ) then
    return pg_catalog.jsonb_build_object('ok',false,
      'code','WEEKLY_SOURCE_FIRST_AUTHORISE_QUERY_OPEN','retryable',false,
      'reason','RESOLVE_MANUAL_QUERY');
  end if;
  if exists (
    select 1 from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    where incident.state='OPEN'
      and comparison.contract_id=v_root.contract_id
      and work_event.work_date between v_root.week_ending_date-6 and v_root.week_ending_date
      -- Owner11Oct: these hold their exact shift, never the whole root.
      -- Republish the existing missing-Candidate-Timesheet exemption.
      and comparison.issue_family is distinct from 'CANDIDATE_TIMESHEET_MISSING'
      and (comparison.issue_family in ('HEALTHROSTER_NOT_FINALISED',
        'SOURCE_MISSING_OR_NOT_AUTHORISED','SOURCE_HOURS_DIFFER')
        and comparison.incident_id=incident.id
        and work_event.candidate_id=incident.candidate_id and work_event.client_id=incident.client_id
        and not exists(select 1 from public.weekly_issue_comparison_revisions newer
          where newer.incident_id=incident.id and newer.revision_number>comparison.revision_number)) is not true
      and private.weekly_source_covered_hours_incident_v1(incident.id) is not true
  ) then
    return pg_catalog.jsonb_build_object('ok',false,
      'code','WEEKLY_SOURCE_FIRST_AUTHORISE_QUERY_OPEN','retryable',false,
      'reason','RESOLVE_OPEN_HOURS_QUERY');
  end if;

  -- The UNCHANGED ordinary Authorise owner, exactly once, through its current
  -- contract.  No timestamp is passed: the owner keeps its own default.
  v_evidence_reference:=private.bpay_protected_reference_for_root_v1(p_timesheet_id);

  v_authorise:=public.timesheet_authorise_generic_atomic(
    p_timesheet_id=>p_timesheet_id,
    p_expected_timesheet_id=>p_timesheet_id,
    p_actor_user_id=>p_actor_user_id,
    p_expected_row_signature=>nullif(pg_catalog.btrim(coalesce(p_expected_row_signature,'')),''));

  if coalesce((v_authorise->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_FIRST_AUTHORISE_OWNER_REFUSED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_FIRST_AUTHORISE_OWNER_REFUSED',
              'timesheet_id',p_timesheet_id,
              'authorise_result',v_authorise)::text;
  end if;

  select * into v_root from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
              'reason','CANONICAL_ROW_MISSING_AFTER_AUTHORISE',
              'timesheet_id',p_timesheet_id)::text;
  end if;

  -- proof/34 section 4: the row signature AT authorisation.  Read from the
  -- owner's own return where it gives one, else recomputed; blank fails closed.
  v_signature:=nullif(pg_catalog.btrim(coalesce(
    v_authorise->>'backend_row_signature',v_authorise->>'row_signature','')),'');
  if v_signature is null then
    select contract_week_row.id into v_contract_week_id
    from public.contract_weeks as contract_week_row
    where contract_week_row.timesheet_id=p_timesheet_id;
    v_signature_json:=public.timesheet_lifecycle_guard_signature_v1(
      p_timesheet_id,v_contract_week_id,false);
    v_signature:=nullif(pg_catalog.btrim(coalesce(
      v_signature_json->>'backend_row_signature',
      v_signature_json->>'row_signature','')),'');
  end if;
  if v_signature is null or pg_catalog.char_length(v_signature)>512 then
    raise exception 'WEEKLY_SOURCE_ROOT_SIGNATURE_UNAVAILABLE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_ROOT_SIGNATURE_UNAVAILABLE',
              'timesheet_id',p_timesheet_id)::text;
  end if;

  select coalesce(pg_catalog.max(prior_row.authorisation_generation),0)+1
    into v_generation
  from public.weekly_source_root_authorisations as prior_row
  where prior_row.root_timesheet_id=p_timesheet_id;

  -- ROUND 5 Part E, the rotation/signature ruling: the authorisation row binds
  -- all five required fields, and the canonical digest of them is stored on the
  -- row so that a later reader can prove the row is the one that was written
  -- rather than trusting the columns individually.  Bound over the WHOLE
  -- family: the protected decisions and the agency evidence belong to the
  -- family, not to whichever physical row is current.
  SELECT agency_id INTO v_agency_id
  FROM private.bpay_protected_family_for_current_v1(p_timesheet_id);
  v_protected_hashes:=NULL; -- V2 uses the frozen reference; never pretend this is a V1 empty set.
  v_decision_digest:=private.weekly_source_publication_request_digest_v1(
    private.bpay_protected_reference_authorisation_signature_v1(
      v_agency_id,p_timesheet_id,v_root.booking_id,v_root.version,v_generation,
      v_signature,v_protected_hashes,2::smallint,v_evidence_reference));

  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,current_entitlement_head_id,authorised_by_user_id,
    agency_id,protected_decision_hashes,decision_digest,bpay_evidence_codec_v1,bpay_evidence_reference_v1
  ) values (
    p_timesheet_id,v_root.booking_id,v_root.version,v_generation,
    v_signature,null,p_actor_user_id,
    v_agency_id,v_protected_hashes,v_decision_digest,2,v_evidence_reference
  ) returning id into v_id;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'timesheet_id',p_timesheet_id,
    'root_authorisation_id',v_id,
    'authorisation_generation',v_generation,
    'family_booking_id',v_root.booking_id,
    'timesheet_version',v_root.version,
    'authorised_row_signature',v_signature,
    'agency_id',v_agency_id,
    'decision_digest',pg_catalog.encode(v_decision_digest,'hex'),
    'authorise_result',pg_catalog.jsonb_build_object(
      'ok',true,
      'operation',v_authorise->>'operation',
      'timesheet_id',v_authorise->>'timesheet_id',
      'contract_week_id',v_authorise->>'contract_week_id',
      'processing_status',v_authorise->>'processing_status'));
end;
$function$;

-- Current owner: supabase/repeatable/11102026_0939_banking_pay_next_daily_authorise_rate_refusal_v1.sql
CREATE OR REPLACE FUNCTION public.timesheet_authorise_generic_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_has_segment_invoice_lock boolean := false;
  v_validation_status text := NULL;
  v_validation_pre_validated boolean := false;
  v_validation_ok boolean := false;
  v_contract_requires_hr boolean := false;
  v_client_requires_hr boolean := false;
  v_hr_validation_required_for_invoice boolean := false;
  v_must_hold_for_hr_validation boolean := false;
  v_prevalidated_fast_track boolean := false;
  v_force_ready_for_invoice boolean := false;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_diag_started_at timestamptz := clock_timestamp();
  v_temp_log_enabled boolean := false;
  v_advance_state_refresh_json jsonb := '{}'::jsonb;
  v_has_uncleared_advance_override boolean := false;
  v_duplicate_expense_review jsonb := '{}'::jsonb;
  v_daily_missing_bucket record;
  v_query_pre_root public.timesheets%rowtype;v_query_pre_contract public.contracts%rowtype;
  v_query_pre_count bigint;v_query_key_required boolean:=true;
BEGIN

  if coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    return jsonb_build_object(
      'ok', false,
      'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_BULK_LIFECYCLE',
      'timesheet_id', p_timesheet_id,
      'required_rpc', case when 'AUTHORISE' = 'AUTHORISE'
        then 'timesheet_authorise_bulk_atomic' else 'timesheet_unauthorise_bulk_atomic' end
    );
  end if;
  PERFORM set_config('lock_timeout', '300ms', true);

  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  -- SP-12: revoked_at restoration changes the qualified root scope.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    IF p_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
    END IF;
    IF p_expected_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
    END IF;
    IF p_actor_user_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
    END IF;

    -- Owner11Oct A3: lock-free native Source applicability before business locks.
    -- Revoked restoration or undecidable scope keeps the existing admission.
    begin
      select t.* into v_query_pre_root from public.timesheets t where t.timesheet_id=p_timesheet_id;
      if found and v_query_pre_root.is_current is not true then
        select count(*) into v_query_pre_count from public.timesheets t
          where t.booking_id=v_query_pre_root.booking_id and t.is_current;
        if v_query_pre_count=1 then
          select t.* into v_query_pre_root from public.timesheets t
            where t.booking_id=v_query_pre_root.booking_id and t.is_current;
        else v_query_pre_root:=null;end if;
      end if;
      if v_query_pre_root.timesheet_id is not null and v_query_pre_root.revoked_at is null
        and v_query_pre_root.archived_at_utc is null then
        if v_query_pre_root.sheet_scope is not null and v_query_pre_root.line_type is not null
          and (v_query_pre_root.sheet_scope<>'WEEKLY' or v_query_pre_root.line_type<>'HOURS') then
          v_query_key_required:=false;
        elsif v_query_pre_root.sheet_scope is not null and v_query_pre_root.line_type is not null
          and v_query_pre_root.contract_id is not null and v_query_pre_root.week_ending_date is not null then
          select c.* into v_query_pre_contract from public.contracts c where c.id=v_query_pre_root.contract_id;
          if found and v_query_pre_contract.client_id is not null then
            select count(*) into v_query_pre_count from public.weekly_source_group_clients membership
              join public.weekly_source_groups g on g.id=membership.source_group_id
              where membership.client_id=v_query_pre_contract.client_id and g.active
                and v_query_pre_root.week_ending_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date);
            v_query_key_required:=v_query_pre_count<>0;
          end if;
        end if;
      end if;
      if not v_query_key_required then
        -- Native Source ownership includes resolved import/self-bill lineage,
        -- independently of today's Office group applicability.
        if v_query_pre_root.sheet_scope='WEEKLY' and v_query_pre_root.line_type='HOURS'
          and (v_query_pre_root.settings_authority_version is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
            or v_query_pre_root.settings_authority_resolved_at is null
            or jsonb_typeof(v_query_pre_root.settings_authority_json) is distinct from 'object') then
          v_query_key_required:=true;
        end if;
        select c.* into v_query_pre_contract from public.contracts c where c.id=v_query_pre_root.contract_id;
        if not found or v_query_pre_contract.self_bill is distinct from false
          or private._candidate_expense_source_family_v1(v_query_pre_root.timesheet_id) is distinct from false
          or exists(select 1 from public.weekly_source_root_authorisations a
            where a.root_timesheet_id=v_query_pre_root.timesheet_id and a.withdrawn_at_utc is null) then
          v_query_key_required:=true;
        end if;
      end if;
    exception when others then v_query_key_required:=true;end;
    if v_query_key_required then perform private.weekly_source_pay_query_admit_v2();end if;
  end if;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'entry',
      'timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'actor_user_id_present', p_actor_user_id IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_requested_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'requested_timesheet_resolved',
      'timesheet_id', v_requested_ts.timesheet_id,
      'is_current', COALESCE(v_requested_ts.is_current, false),
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;
    IF v_current_ts.timesheet_id IS NULL THEN
      v_current_ts := v_requested_ts;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH', DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  v_duplicate_expense_review:=private._timesheet_duplicate_expense_review_v1(
    v_current_ts.timesheet_id
  );
  IF COALESCE((v_duplicate_expense_review->>'required')::boolean,false)
     AND COALESCE(current_setting('cloudtms.duplicate_expense_reviewed',true),'')<>'true' THEN
    RAISE EXCEPTION 'DUPLICATE_EXPENSE_REVIEW_REQUIRED'
      USING ERRCODE='PT409',DETAIL=jsonb_build_object(
        'code','DUPLICATE_EXPENSE_REVIEW_REQUIRED',
        'timesheet_id',v_current_ts.timesheet_id,
        'categories',COALESCE(v_duplicate_expense_review->'categories','[]'::jsonb)
      )::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'current_timesheet_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'NO_TSFIN')::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_current_tsfin.processing_status::text,
      'old_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_current_ts.contract_id IS NOT NULL THEN
    SELECT COALESCE(c.requires_hr, false)
      INTO v_contract_requires_hr
    FROM public.contracts AS c
    WHERE c.id = v_current_ts.contract_id
    LIMIT 1;
  END IF;

  IF private._candidate_expense_source_family_v1(v_current_ts.timesheet_id) THEN
SELECT
  COALESCE((authority.settings_json#>>'{values,requires_hr}')::boolean,false) AS client_requires_hr,
  (ts.timesheet_id IS NOT NULL
    AND COALESCE((authority.settings_json#>>'{values,hr_validation_required_for_invoice}')::boolean,false)
    AND NOT COALESCE((authority.settings_json#>>'{values,no_timesheet_required}')::boolean,false)
    AND COALESCE(tf.total_hours,0)>0) AS hr_validation_required_for_invoice,
  UPPER(vl.status::text) AS validation_status
INTO v_client_requires_hr, v_hr_validation_required_for_invoice, v_validation_status
FROM public.timesheets ts
LEFT JOIN public.contract_weeks cw ON cw.timesheet_id=ts.timesheet_id
LEFT JOIN public.contracts ct ON ct.id=COALESCE(ts.contract_id,cw.contract_id)
LEFT JOIN LATERAL (
  SELECT f.client_id,f.total_hours FROM public.timesheets_financials f
  WHERE f.timesheet_id=ts.timesheet_id AND f.is_current=true
  ORDER BY f.created_at DESC LIMIT 1
) tf ON true
CROSS JOIN LATERAL (
  SELECT private._contract_settings_effective_core_v1(
    COALESCE(tf.client_id,ct.client_id),COALESCE(ts.contract_id,cw.contract_id),
    COALESCE((ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
      (ts.scheduled_start_iso AT TIME ZONE 'Europe/London')::date,ts.week_ending_date),
    CASE WHEN ts.sheet_scope='DAILY'::public.timesheet_scope_enum THEN 'DAILY' ELSE 'FINANCE' END,
    ts.timesheet_id) AS settings_json
) authority
LEFT JOIN LATERAL (
  SELECT tv.status FROM public.timesheet_validations tv
  WHERE tv.timesheet_id=ts.timesheet_id ORDER BY tv.created_at DESC LIMIT 1
) vl ON true
WHERE ts.timesheet_id=v_current_ts.timesheet_id AND ts.is_current=true
LIMIT 1
;
  ELSE
  -- Exact current-root flags only. Do not rebuild the summary of every
  -- Timesheet to authorise one root; retain the same dated/frozen settings,
  -- current-financial selection and latest-validation semantics.
  WITH bpay_authorisation_point_flags AS MATERIALIZED (
    SELECT ts.timesheet_id, COALESCE(tf.total_hours,0) AS total_hours,
      public.contract_settings_effective_get_v1(
        COALESCE(tf.client_id,ct.client_id),COALESCE(ts.contract_id,cw.contract_id),
        COALESCE((ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
          (ts.scheduled_start_iso AT TIME ZONE 'Europe/London')::date,ts.week_ending_date),
        CASE WHEN ts.sheet_scope='DAILY'::public.timesheet_scope_enum THEN 'DAILY' ELSE 'FINANCE' END,
        ts.timesheet_id) AS settings_json,
      UPPER(vl.status::text) AS validation_status
    FROM public.timesheets ts
    LEFT JOIN public.contract_weeks cw ON cw.timesheet_id=ts.timesheet_id
    LEFT JOIN public.contracts ct ON ct.id=COALESCE(ts.contract_id,cw.contract_id)
    LEFT JOIN LATERAL (
      SELECT f.client_id,f.total_hours FROM public.timesheets_financials f
      WHERE f.timesheet_id=ts.timesheet_id AND f.is_current=true
      ORDER BY f.created_at DESC LIMIT 1
    ) tf ON true
    LEFT JOIN LATERAL (
      SELECT tv.status FROM public.timesheet_validations tv
      WHERE tv.timesheet_id=ts.timesheet_id ORDER BY tv.created_at DESC LIMIT 1
    ) vl ON true
    WHERE ts.timesheet_id=v_current_ts.timesheet_id AND ts.is_current=true
    LIMIT 1
  )
  SELECT COALESCE((point.settings_json#>>'{values,requires_hr}')::boolean,false),
    (point.timesheet_id IS NOT NULL
      AND COALESCE((point.settings_json#>>'{values,hr_validation_required_for_invoice}')::boolean,false)
      AND NOT COALESCE((point.settings_json#>>'{values,no_timesheet_required}')::boolean,false)
      AND point.total_hours>0),
    point.validation_status
  INTO v_client_requires_hr,v_hr_validation_required_for_invoice,v_validation_status
  FROM bpay_authorisation_point_flags point;
  END IF;

  v_client_requires_hr := COALESCE(v_client_requires_hr, v_contract_requires_hr, false);
  v_hr_validation_required_for_invoice := COALESCE(v_hr_validation_required_for_invoice, v_contract_requires_hr, false);

  IF private._candidate_expense_source_family_v1(v_current_ts.timesheet_id) THEN
    SELECT cw.* INTO v_contract_week FROM public.contract_weeks cw
    WHERE cw.timesheet_id=v_current_ts.timesheet_id FOR UPDATE;
  ELSE
  SELECT cw.*
    INTO v_contract_week
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_ts.timesheet_id
     OR EXISTS (
       SELECT 1
       FROM public.timesheets AS cw_ts
       WHERE cw_ts.timesheet_id = cw.timesheet_id
         AND cw_ts.booking_id = v_current_ts.booking_id
     )
  ORDER BY CASE WHEN cw.timesheet_id = v_current_ts.timesheet_id THEN 0 ELSE 1 END,
           cw.updated_at DESC NULLS LAST,
           cw.id DESC
  LIMIT 1
  FOR UPDATE;
  END IF;

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND v_contract_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TARGET_NOT_FOUND',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'CANCELLED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_AUTHORISABLE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text)::text;
  END IF;

  v_prev_status := v_current_tsfin.processing_status;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_current_tsfin.authorised_at_utc IS NOT NULL OR v_contract_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_current_ts.timesheet_id::text,
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'timesheet_authorise_generic_atomic',
          'stage', 'row_signature_mismatch_before_authorise',
          'action', 'authorise',
          'route_family', 'timesheet_lifecycle',
          'timesheet_id', p_timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json,
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
        ))
      );
    END IF;
    RAISE EXCEPTION USING MESSAGE = 'ROW_SIGNATURE_MISMATCH', DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  SELECT COALESCE(tv.pre_validated, false)
    INTO v_validation_pre_validated
  FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = v_current_ts.timesheet_id
  ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    v_validation_pre_validated := false;
  END IF;

  v_validation_ok := COALESCE(v_validation_status, '') IN ('VALIDATION_OK', 'OVERRIDDEN');
  v_force_ready_for_invoice := v_current_tsfin.basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum);
  v_must_hold_for_hr_validation := COALESCE(v_hr_validation_required_for_invoice, false) AND NOT v_validation_ok;
  v_prevalidated_fast_track := COALESCE(v_validation_pre_validated, false) AND v_validation_ok;

  -- DY04: report actual missing Daily authority without making it authorisable.
  IF (SELECT active_owner FROM private.bpay_next_module_control WHERE id=1)='NEXT'
     AND v_current_ts.sheet_scope='DAILY'::public.timesheet_scope_enum
     AND v_current_ts.line_type::text='HOURS'
     AND v_current_ts.is_current IS TRUE
     AND v_current_ts.is_adjustment IS DISTINCT FROM TRUE
     AND v_current_ts.parent_timesheet_id IS NULL AND v_current_ts.correction_id IS NULL
     AND NULLIF(BTRIM(v_current_ts.adjustment_origin),'') IS NULL
     AND NULLIF(BTRIM(v_current_ts.correction_kind),'') IS NULL
     AND v_current_ts.worked_start_iso IS NOT NULL
     AND v_current_tsfin.is_current IS TRUE AND v_current_tsfin.is_stale IS FALSE
     AND v_current_tsfin.timesheet_version=v_current_ts.version
     AND v_current_tsfin.basis::text='SELF_REPORTED'
     AND v_current_tsfin.candidate_id IS NOT NULL AND v_current_tsfin.client_id IS NOT NULL
     AND UPPER(v_current_tsfin.pay_method) IN ('PAYE','UMBRELLA')
     AND v_current_tsfin.has_rate_issue IS TRUE
     AND v_prev_status::text IN ('RATE_MISSING','UNPROCESSED')
     AND NOT EXISTS (SELECT 1 FROM public.contracts dc
       WHERE dc.id=v_current_ts.contract_id AND dc.self_bill IS TRUE)
     AND NOT private._candidate_expense_source_family_v1(v_current_ts.timesheet_id) THEN
    FOR v_daily_missing_bucket IN
      SELECT d.* FROM (VALUES
        (1,'DAY'::text,v_current_tsfin.hours_day,v_current_tsfin.pay_day,v_current_tsfin.charge_day),
        (2,'NIGHT',v_current_tsfin.hours_night,v_current_tsfin.pay_night,v_current_tsfin.charge_night),
        (3,'SAT',v_current_tsfin.hours_sat,v_current_tsfin.pay_sat,v_current_tsfin.charge_sat),
        (4,'SUN',v_current_tsfin.hours_sun,v_current_tsfin.pay_sun,v_current_tsfin.charge_sun),
        (5,'BH',v_current_tsfin.hours_bh,v_current_tsfin.pay_bh,v_current_tsfin.charge_bh)
      ) d(ordinal,bucket,hours,pay_rate,charge_rate)
      WHERE d.hours>0 AND (d.pay_rate IS NULL OR d.charge_rate IS NULL) ORDER BY d.ordinal
    LOOP
      RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
        DETAIL=jsonb_build_object('bucket',v_daily_missing_bucket.bucket,
          'rate_field',CASE WHEN v_daily_missing_bucket.pay_rate IS NULL THEN 'PAY' ELSE 'CHARGE' END,
          'rate_authority','DAILY_DATED','sheet_scope','DAILY','line_type','HOURS',
          'timesheet_id',v_current_ts.timesheet_id,'contract_id',v_current_ts.contract_id,
          'candidate_id',v_current_tsfin.candidate_id,'client_id',v_current_tsfin.client_id,
          'work_date',(v_current_ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
          'pay_channel',UPPER(v_current_tsfin.pay_method))::text;
    END LOOP;
  END IF;

  IF v_prev_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RAISE EXCEPTION USING MESSAGE = 'AUTHORISE_NOT_ALLOWED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'processing_status', v_prev_status::text)::text;
  END IF;

  v_new_status := CASE
    WHEN v_force_ready_for_invoice THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN v_must_hold_for_hr_validation THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    WHEN v_prevalidated_fast_track THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN COALESCE(v_client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
  END;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'before_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'tsfin_id', v_current_tsfin.id,
      'current_row_signature_present', v_current_row_signature IS NOT NULL,
      'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'timesheet_authorise', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);

  UPDATE public.timesheets AS ts
     SET authorised_at_server = v_now,
         revoked_at = NULL,
         revoked_reason = NULL,
         revoked_by = NULL,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'timesheets_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'new_authorised_present', v_current_ts.authorised_at_server IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         authorised_by_user_id = p_actor_user_id,
         authorised_at_utc = v_now,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_prev_status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_contract_week.id IS NOT NULL THEN
    UPDATE public.contract_weeks AS cw
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
     WHERE cw.id = v_contract_week.id
     RETURNING * INTO v_contract_week;
  END IF;

  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'off', true);

  v_has_uncleared_advance_override := private.bpay_has_uncleared_advance_v1(v_current_ts.timesheet_id);

  IF COALESCE(v_has_uncleared_advance_override, false) THEN
    v_advance_state_refresh_json := public.pay_timesheet_summary_advance_state_refresh(
      p_timesheet_id => v_current_ts.timesheet_id,
      p_actor_user_id => p_actor_user_id
    );

    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_done',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'refresh_result', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  ELSE
    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_skipped',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'reason', 'NO_ACTIVE_ADVANCE_THIS_PAYMENT_OVERRIDE',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'after_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'row_signature_present', v_after_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_AUTHORISED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'previous_processing_status', v_prev_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'new_processing_status', v_new_status::text, 'authorised_at_utc', v_now, 'authorised_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'TIMESHEET_AUTHORISE',
    p_actor_user_id
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'audit_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'return',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'new_processing_status', v_new_status::text,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'timesheet_authorise',
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'requested_timesheet_id', v_requested_ts.timesheet_id,
    'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
    'booking_id', v_current_ts.booking_id,
    'candidate_id', v_current_tsfin.candidate_id,
    'authorised_at_server', v_current_ts.authorised_at_server,
    'revoked_at', v_current_ts.revoked_at,
    'authorised_at_utc', v_current_tsfin.authorised_at_utc,
    'advance_state_refresh', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_processing_status', v_prev_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'validation_status', v_validation_status,
    'validation_pre_validated', COALESCE(v_validation_pre_validated, false),
    'hr_validation_required_for_invoice', COALESCE(v_hr_validation_required_for_invoice, false),
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'booking_id', v_current_ts.booking_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', CASE WHEN COALESCE(v_has_uncleared_advance_override, false) THEN jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_summary_pay_state_cache') ELSE jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks') END, 'timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  -- Exact busy must escape before diagnostic writes or legacy lock translation.
  IF v_error_state = '55P03' AND v_error_message = 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY' THEN
    RAISE;
  END IF;
  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'exception',
      'timesheet_id', p_timesheet_id,
      'sqlstate', v_error_state,
      'error_message', v_error_message,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );
  IF v_error_state = '55P03' THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;
  RAISE;
END;
$function$;
alter function private.bpay_next_source_query_segment_exclusions_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_source_query_segment_exclusions_v1(uuid,jsonb) from public,anon,authenticated,service_role;

alter function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb) from public,anon,authenticated,service_role;

alter function private.bpay_next_append_work_skip_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_append_work_skip_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) from public,anon,authenticated,service_role;

alter function private.bpay_next_append_segment_selection_page_v1(uuid,uuid,uuid,bigint,uuid,text[]) owner to postgres;
revoke all on function private.bpay_next_append_segment_selection_page_v1(uuid,uuid,uuid,bigint,uuid,text[]) from public,anon,authenticated,service_role;

alter function private.bpay_next_segment_fact_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_fact_guard_v1() from public,anon,authenticated,service_role;

alter function private.bpay_next_capture_segment_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_capture_segment_page_v1(uuid,integer) from public,anon,authenticated,service_role;

alter function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_refresh_work_core_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_work_core_v1(uuid) from public,anon,authenticated,service_role;

alter function private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb) from public,anon,authenticated,service_role;

alter function public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text) owner to postgres;
revoke all on function public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text) to service_role;

notify pgrst,'reload schema';
commit;
