-- WB29: original eligible leaf cut; complete A/R/H parents retained.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_choice_leaf_eligible_v1(
 p_run_id uuid,p_work_id uuid,p_revision_id uuid,p_line_id uuid,p_segment_id uuid
) returns boolean language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 r private.bpay_next_pay_run%rowtype;h private.bpay_next_selection_mode_memory%rowtype;
 c private.bpay_next_work_choice%rowtype;a private.bpay_next_approved_line%rowtype;
 s private.bpay_next_approved_segment%rowtype;v_candidate uuid;
begin
 select x.* into r from private.bpay_next_pay_run x where x.id=p_run_id;
 select x.* into h from private.bpay_next_selection_mode_memory x where x.run_id=p_run_id and x.row_kind='HEADER';
 if h.run_id is null then
  if r.id is null or r.accepted_choice_descriptor is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ELIGIBLE_CAPTURE_MISSING';end if;
  -- Original non-descriptor selection owners retain their existing admission.
  return true;
 end if;
 if r.id is null or (h.actor_user_id,h.official_pay_date) is distinct from (r.actor_user_id,r.pay_date)
  or (r.accepted_choice_descriptor is not null and
    (h.accepted_context is distinct from r.accepted_choice_descriptor-'intent'
      or h.raw_intent is distinct from r.accepted_choice_descriptor->'intent')) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ELIGIBLE_CAPTURE_MISSING';end if;
 select x.* into c from private.bpay_next_work_choice x where x.run_id=p_run_id and x.work_id=p_work_id;
 select x.* into a from private.bpay_next_approved_line x where x.id=p_line_id and x.revision_id=p_revision_id;
 select x.candidate_id into v_candidate from private.bpay_next_work x where x.id=p_work_id;
 if c.work_id is null or c.expected_revision_id is distinct from p_revision_id
  or c.selection_state is distinct from 'SEALED' or a.id is null or v_candidate is null
  or not exists(select 1 from private.bpay_next_work_revision x where x.id=p_revision_id and x.work_id=p_work_id)
  or (c.selection_mode='SUBSET' and not exists(select 1 from private.bpay_next_selected_component x
    where x.run_id=p_run_id and x.work_id=p_work_id and x.expected_revision_id=p_revision_id
      and x.approved_line_id=p_line_id and x.component_key=a.component_key)) then return false;end if;
 if p_segment_id is not null then
  select x.* into s from private.bpay_next_approved_segment x where x.id=p_segment_id;
  if s.id is null or (s.revision_id,s.approved_line_id,s.component_key)
    is distinct from (p_revision_id,p_line_id,a.component_key) then return false;end if;
 end if;
 return exists(select 1 from private.bpay_next_selection_mode_memory m
  where m.run_id=p_run_id and m.row_kind='LEAF' and m.candidate_id=v_candidate
    and m.work_id=p_work_id and m.approved_line_id=p_line_id
    and m.leaf_key='WORK:'||p_work_id::text||':'||a.component_key||':'||
      case when p_segment_id is null then 'WORK_COMPONENT:'||a.component_key else 'WORK_SEGMENT:'||s.segment_key end
    and m.basis->>'revision_id'=p_revision_id::text and m.basis->>'approved_line_id'=p_line_id::text
    and m.basis->>'component_key'=a.component_key and m.basis->>'direction'='PAYMENT'
    and ((p_segment_id is null and m.basis->>'member_kind'='WORK_COMPONENT'
      and m.basis->'approved_segment_id'='null'::jsonb and m.basis->'segment_key'='null'::jsonb)
     or (p_segment_id is not null and m.basis->>'member_kind'='WORK_SEGMENT'
      and m.basis->>'approved_segment_id'=p_segment_id::text and m.basis->>'segment_key'=s.segment_key)));
end
$function$;

create or replace function private.bpay_next_selection_memory_capture_v1(p_run uuid,p_descriptor jsonb)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_ns jsonb:=p_descriptor->'namespace';v_baseline uuid:=(p_descriptor->>'memory_baseline_run_id')::uuid;v_kind text;v_row jsonb;v_id uuid;v_leaf record;v_group text;v_next_group text;v_rows bigint:=0;v_bytes bigint:=0;v_encoded jsonb;
begin
 insert into private.bpay_next_selection_mode_memory(run_id,row_kind,leaf_key,agency_id,owner_epoch,actor_user_id,official_pay_date,scope,mode,intent_revision,baseline_run_id,basis,raw_intent,accepted_context)
 values(p_run,'HEADER','HEADER',(v_ns->>'agency_id')::uuid,(v_ns->>'owner_epoch')::bigint,(v_ns->>'actor_user_id')::uuid,(v_ns->>'official_pay_date')::date,v_ns->'scope',p_descriptor->'intent'->>'mode',(p_descriptor->>'revision')::bigint,v_baseline,'{}',p_descriptor->'intent',p_descriptor-'intent');
 -- Finite per-component proof, streamed without a global Draft cap.
 for v_leaf in select l.* from private.bpay_next_selection_memory_leaves_v1(v_ns->'scope') l
   order by l.candidate_id,l.work_id,l.approved_line_id,l.case_component_id,l.leaf_key loop
  v_next_group:=jsonb_build_array(v_leaf.candidate_id,v_leaf.work_id,v_leaf.approved_line_id,v_leaf.case_component_id)::text;
  if v_group is distinct from v_next_group then v_group:=v_next_group;v_rows:=0;v_bytes:=0;end if;
  v_encoded:=jsonb_build_object('run_id',p_run,'row_kind','LEAF','leaf_key',v_leaf.leaf_key,
   'agency_id',v_ns->'agency_id','owner_epoch',v_ns->'owner_epoch','actor_user_id',v_ns->'actor_user_id',
   'official_pay_date',v_ns->'official_pay_date','scope',v_ns->'scope','mode',p_descriptor->'intent'->'mode',
   'intent_revision',p_descriptor->'revision','baseline_run_id',v_baseline,
   'candidate_id',v_leaf.candidate_id,'work_id',v_leaf.work_id,'approved_line_id',v_leaf.approved_line_id,
   'case_component_id',v_leaf.case_component_id,'basis',v_leaf.basis);
  v_rows:=v_rows+1;v_bytes:=v_bytes+octet_length(v_encoded::text);
  if v_rows>10000 or v_bytes>8388608 then
   raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
  insert into private.bpay_next_selection_mode_memory(run_id,row_kind,leaf_key,agency_id,owner_epoch,actor_user_id,official_pay_date,scope,mode,intent_revision,baseline_run_id,candidate_id,work_id,approved_line_id,case_component_id,basis)
  values(p_run,'LEAF',v_leaf.leaf_key,(v_ns->>'agency_id')::uuid,(v_ns->>'owner_epoch')::bigint,(v_ns->>'actor_user_id')::uuid,(v_ns->>'official_pay_date')::date,v_ns->'scope',p_descriptor->'intent'->>'mode',(p_descriptor->>'revision')::bigint,v_baseline,v_leaf.candidate_id,v_leaf.work_id,v_leaf.approved_line_id,v_leaf.case_component_id,v_leaf.basis);
 end loop;
 -- Carry the original basis reference; never requalify inherited unticks.
 insert into private.bpay_next_selection_exclusion_memory
 select p_run,e.agency_id,e.owner_epoch,e.actor_user_id,e.official_pay_date,e.scope,e.intent_kind,e.native_id,e.candidate_id,e.work_id,e.expected_revision_id,e.approved_line_id,e.case_component_id,e.selected,e.basis_ref_run_id,e.intent_json,transaction_timestamp()
 from private.bpay_next_selection_exclusion_memory e where e.run_id=v_baseline
  and not exists(select 1 from jsonb_array_elements(p_descriptor->'intent'->e.intent_kind) raw
   where coalesce(raw->>'case_component_id',raw->>'approved_line_id',raw->>'work_id',raw->>'candidate_id')=e.native_id::text)
  and not exists(select 1 from jsonb_array_elements(p_descriptor->'intent'->'candidates') raw
   where raw->>'candidate_id'=e.candidate_id::text and raw->'selected'='true'::jsonb)
  and not (e.intent_kind='components' and exists(select 1 from jsonb_array_elements(p_descriptor->'intent'->'works') raw
   where raw->>'work_id'=e.work_id::text and raw->'selected'='true'::jsonb and raw->>'mode'='ALL'));
 foreach v_kind in array array['candidates','works','components','cases'] loop
  for v_row in select value from jsonb_array_elements(p_descriptor->'intent'->v_kind) loop
   v_id:=(coalesce(v_row->>'case_component_id',v_row->>'approved_line_id',v_row->>'work_id',v_row->>'candidate_id'))::uuid;
   insert into private.bpay_next_selection_exclusion_memory(run_id,agency_id,owner_epoch,actor_user_id,official_pay_date,scope,intent_kind,native_id,candidate_id,work_id,expected_revision_id,approved_line_id,case_component_id,selected,basis_ref_run_id,intent_json)
   values(p_run,(v_ns->>'agency_id')::uuid,(v_ns->>'owner_epoch')::bigint,(v_ns->>'actor_user_id')::uuid,(v_ns->>'official_pay_date')::date,v_ns->'scope',v_kind,v_id,(v_row->>'candidate_id')::uuid,(v_row->>'work_id')::uuid,(v_row->>'expected_revision_id')::uuid,(v_row->>'approved_line_id')::uuid,(v_row->>'case_component_id')::uuid,(v_row->>'selected')::boolean,p_run,v_row);
  end loop;
 end loop;
end
$function$;
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
 -- The eligible leaf cut is universal; optional remembered-choice semantics remain original.
 perform private.bpay_next_selection_memory_capture_v1(p_run_id,p_descriptor);
 if v_memory then
  v_baseline:=private.bpay_next_selection_memory_resolve_v1(v_ns,jsonb_build_object('run_id',p_descriptor->'memory_baseline_run_id','scope',v_ns->'scope'));
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
  elsif private.bpay_next_work_period_proof_v1(v_member.work_id,v_member.expected_revision_id,v_run.snooze_business_date)->'valid' is distinct from 'true'::jsonb then
   v_code:='BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';
  elsif v_member.week_ending_date-6>v_run.snooze_business_date then v_code:='BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE';
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
   insert into private.bpay_next_work_choice(run_id,work_id,expected_revision_id,selection_mode,selection_state,accepted_resolution_refs)
     values(p_run_id,v_member.work_id,v_member.expected_revision_id,v_member.selection_mode,
       case when v_member.selection_mode='ALL' then 'SEALED' else 'OPEN' end,
       private.bpay_next_accepted_resolution_refs_v1(p_run_id,v_member.work_id,v_member.expected_revision_id));
   if v_gate is not null then
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc,segment_query_exclusions)
      values(p_run_id,v_member.work_id,v_member.expected_revision_id,v_member.physical_timesheet_id,v_gate,transaction_timestamp(),v_query_exclusions);
   end if;
  end if;
 end loop;
 -- Seal both original descriptor paths at the accepted cut with native pages.
 update private.bpay_next_pay_run set work_choice_count=v_work_count,
  sealed_work_choice_count=(select count(*) from private.bpay_next_work_choice where run_id=p_run_id and selection_state='SEALED') where id=p_run_id;
 for v_subset in select c.* from private.bpay_next_work_choice c where c.run_id=p_run_id and c.selection_mode='SUBSET' order by c.work_id loop
  v_page_no:=0;
  for v_lines in select array_agg(q.approved_line_id order by q.approved_line_id) from
   (select chosen.approved_line_id,(row_number() over(order by chosen.approved_line_id)-1)/100 page_no from (
     select l.approved_line_id from private.bpay_next_selection_memory_choices_v1(v_ns->'scope',v_baseline,v_intent) l
      where v_memory and l.work_id=v_subset.work_id and l.revision_id=v_subset.expected_revision_id and l.selected and l.direction='PAYMENT'
     union all select (l.value->>'approved_line_id')::uuid from jsonb_array_elements(v_intent->'components') l
      where not v_memory and l.value->>'work_id'=v_subset.work_id::text
       and l.value->>'expected_revision_id'=v_subset.expected_revision_id::text and l.value->'selected'='true'::jsonb
   ) chosen) q group by q.page_no order by q.page_no loop
   v_page_no:=v_page_no+1;
   perform private.bpay_next_append_component_selection_page_v1(p_run_id,v_subset.work_id,v_subset.expected_revision_id,v_page_no,v_lines);
  end loop;
  select c.* into strict v_subset from private.bpay_next_work_choice c where c.run_id=p_run_id and c.work_id=v_subset.work_id;
  perform private.bpay_next_seal_component_selection_v1(p_run_id,v_subset.work_id,v_subset.expected_revision_id,v_subset.page_count,v_subset.component_count);
 end loop;
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
 end if;
 -- ACK can only acknowledge these already frozen segments; no later READY reread.
 for v_subset in select c.work_id from private.bpay_next_work_choice c where c.run_id=p_run_id and c.selection_state='SEALED' order by c.work_id loop
  perform private.bpay_next_materialise_choice_segments_v1(p_run_id,v_subset.work_id);
 end loop;
 return v_result||jsonb_build_object('command_id',p_command_id,'agency_sequence',v_sequence::text,
   'selection_count',v_work_count::text,
   'selected_candidate_count',v_count::text,'empty',v_count=0,
   'choice_work_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'),
   'choice_case_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='CASE_COMPONENT'),'replay',false);
end
$function$;
create or replace function private.bpay_next_materialise_choice_segments_v1(p_run_id uuid,p_work_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_choice private.bpay_next_work_choice%rowtype;v_run private.bpay_next_pay_run%rowtype;v_keys text[];
 v_after text;v_pages bigint:=0;v_count bigint:=0;
begin
 select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id;
 select src.* into strict v_choice from private.bpay_next_work_choice src
   where run_id=p_run_id and work_id=p_work_id;
 if v_choice.segment_sealed_count is not null then return;end if;
 if v_choice.selection_state<>'SEALED' or v_choice.segment_page_count<>0 or v_choice.segment_count<>0 then
  raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_SELECTION_NOT_OPEN';end if;
 loop
  select array_agg(page.segment_key order by page.segment_key collate "C") into v_keys from (
    select s.segment_key from private.bpay_next_approved_segment s
    where s.revision_id=v_choice.expected_revision_id
      and (private.bpay_next_choice_leaf_eligible_v1(p_run_id,p_work_id,v_choice.expected_revision_id,s.approved_line_id,s.id)
       or private.bpay_next_snooze_at_cut_v1(p_work_id,v_choice.expected_revision_id,s.segment_key,s.component_key,
         v_choice.segment_control_sequence,v_run.snooze_business_date) is not null
       or exists(select 1 from private.bpay_next_run_work_query_snapshot snapshot
         cross join lateral jsonb_array_elements(snapshot.segment_query_exclusions) exclusion(value)
         where snapshot.run_id=p_run_id and snapshot.work_id=p_work_id and snapshot.expected_revision_id=v_choice.expected_revision_id
           and exclusion.value->>'segment_key'=s.segment_key))
      and (v_after is null or s.segment_key collate "C">v_after collate "C")
      and (v_choice.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component c
        where c.run_id=p_run_id and c.work_id=p_work_id and c.component_key=s.component_key))
    order by s.segment_key collate "C" limit 100) page;
  exit when v_keys is null;
  v_pages:=v_pages+1;v_count:=v_count+cardinality(v_keys);
  perform private.bpay_next_append_segment_selection_page_v1(p_run_id,p_work_id,v_choice.expected_revision_id,
    v_pages,gen_random_uuid(),v_keys);
  v_after:=v_keys[cardinality(v_keys)];
 end loop;
 perform private.bpay_next_seal_segment_selection_v1(p_run_id,p_work_id,v_choice.expected_revision_id,v_pages,v_count);
end
$function$;
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
    and (private.bpay_next_choice_leaf_eligible_v1(p_run_id,p_work_id,p_expected_revision_id,s.approved_line_id,s.id)
       or private.bpay_next_snooze_at_cut_v1(p_work_id,p_expected_revision_id,s.segment_key,s.component_key,
         v_choice.segment_control_sequence,v_run.snooze_business_date) is not null
       or exists(select 1 from private.bpay_next_run_work_query_snapshot snapshot
         cross join lateral jsonb_array_elements(snapshot.segment_query_exclusions) exclusion(value)
         where snapshot.run_id=p_run_id and snapshot.work_id=p_work_id and snapshot.expected_revision_id=p_expected_revision_id
           and exclusion.value->>'segment_key'=s.segment_key))
    and (v_choice.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component c
      where c.run_id=p_run_id and c.work_id=p_work_id and c.component_key=s.component_key));
  if v_count<>p_expected_count or exists(select 1 from private.bpay_next_approved_segment s
    where s.revision_id=p_expected_revision_id
    and (private.bpay_next_choice_leaf_eligible_v1(p_run_id,p_work_id,p_expected_revision_id,s.approved_line_id,s.id)
       or private.bpay_next_snooze_at_cut_v1(p_work_id,p_expected_revision_id,s.segment_key,s.component_key,
         v_choice.segment_control_sequence,v_run.snooze_business_date) is not null
       or exists(select 1 from private.bpay_next_run_work_query_snapshot snapshot
         cross join lateral jsonb_array_elements(snapshot.segment_query_exclusions) exclusion(value)
         where snapshot.run_id=p_run_id and snapshot.work_id=p_work_id and snapshot.expected_revision_id=p_expected_revision_id
           and exclusion.value->>'segment_key'=s.segment_key)) and (v_choice.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component c
      where c.run_id=p_run_id and c.work_id=p_work_id and c.component_key=s.component_key))
      and not exists(select 1 from private.bpay_next_selected_segment m where m.run_id=p_run_id and m.work_id=p_work_id and m.segment_key=s.segment_key)) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_SELECTION_INVENTORY_INCOMPLETE';
  end if;
  update private.bpay_next_work_choice set segment_sealed_count=segment_count where run_id=p_run_id and work_id=p_work_id;
  return jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
    'sealed',true,'segment_count',v_choice.segment_count::text,'replay',false);
end
$function$;
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
        or (new.disposition='INCLUDED' and new.query_hold_reason is null
          and not private.bpay_next_choice_leaf_eligible_v1(new.run_id,new.work_id,new.expected_revision_id,v_segment.approved_line_id,v_segment.id))
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
alter function private.bpay_next_choice_leaf_eligible_v1(uuid,uuid,uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_choice_leaf_eligible_v1(uuid,uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_selection_memory_capture_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_capture_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_materialise_choice_segments_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_materialise_choice_segments_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_seal_segment_selection_v1(uuid,uuid,uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_seal_segment_selection_v1(uuid,uuid,uuid,bigint,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_segment_fact_guard_v1() owner to postgres;
revoke all on function private.bpay_next_segment_fact_guard_v1() from public,anon,authenticated,service_role;
commit;
