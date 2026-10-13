-- WB21 exact retained choice membership; no producer or money-owner rewrite.
\set ON_ERROR_STOP on
begin;

-- Only the new WB21 input identities and retained descriptor are guarded here.
create or replace function private.bpay_next_accepted_choice_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;
begin
 if tg_table_name='bpay_next_pay_run' then
  if tg_op='UPDATE' and old.accepted_choice_descriptor is not null
    and (new.accepted_choice_descriptor,new.accepted_choice_command_id)
      is distinct from (old.accepted_choice_descriptor,old.accepted_choice_command_id) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_IMMUTABLE';end if;
  if new.accepted_choice_descriptor is not null and new.accepted_choice_command_id is null then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
  return new;
 end if;
 if tg_op='DELETE' then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_INPUT_IMMUTABLE';end if;
 select src.* into strict v_run from private.bpay_next_pay_run src where id=new.run_id for update;
 if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_ALREADY_SEALED';end if;
 if tg_op='UPDATE' then
  if tg_table_name<>'bpay_next_run_choice_member' then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_INPUT_IMMUTABLE';end if;
  if v_run.accepted_choice_descriptor is not null
    or old.skip_code is not null or new.skip_code is null
    or (to_jsonb(new)-'skip_code') is distinct from (to_jsonb(old)-'skip_code') then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_INPUT_IMMUTABLE';end if;
 elsif tg_table_name='bpay_next_selection_exclusion_page' then
  if v_run.accepted_choice_descriptor is null or new.page_no<>v_run.exclusion_page_count+1 then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
 else
  if v_run.accepted_choice_descriptor is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_INPUT_IMMUTABLE';end if;
  if tg_table_name='bpay_next_run_choice_member' then
   if new.member_kind='CASE_COMPONENT' and not exists(select 1 from private.bpay_next_case_component
     where id=new.case_component_id and candidate_id=new.candidate_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
  elsif new.actor_user_id is distinct from v_run.actor_user_id then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 end if;
 return new;
end
$function$;
alter function private.bpay_next_accepted_choice_guard_v1() owner to postgres;
revoke all on function private.bpay_next_accepted_choice_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_accepted_choice_guard on private.bpay_next_pay_run;
create trigger bpay_next_accepted_choice_guard before insert or update on private.bpay_next_pay_run
 for each row execute function private.bpay_next_accepted_choice_guard_v1();
drop trigger if exists bpay_next_accepted_choice_member_guard on private.bpay_next_run_choice_member;
create trigger bpay_next_accepted_choice_member_guard before insert or update or delete on private.bpay_next_run_choice_member
 for each row execute function private.bpay_next_accepted_choice_guard_v1();
drop trigger if exists bpay_next_accepted_choice_exclusion_guard on private.bpay_next_run_selection_exclusion;
create trigger bpay_next_accepted_choice_exclusion_guard before insert or update or delete on private.bpay_next_run_selection_exclusion
 for each row execute function private.bpay_next_accepted_choice_guard_v1();
drop trigger if exists bpay_next_accepted_choice_exclusion_page_guard on private.bpay_next_selection_exclusion_page;
create trigger bpay_next_accepted_choice_exclusion_page_guard before insert or update or delete on private.bpay_next_selection_exclusion_page
 for each row execute function private.bpay_next_accepted_choice_guard_v1();

-- E1 eligibility on legacy manifest append; accepted WB21 pages replay their cut.
create or replace function private.bpay_next_append_work_skip_v1(p_run_id uuid,p_work_id uuid,p_revision_id uuid)
returns text language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_work private.bpay_next_work%rowtype;
 v_revision private.bpay_next_work_revision%rowtype;v_gate jsonb;
begin
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id;
 select src.* into strict v_work from private.bpay_next_work src where id=p_work_id;
 if p_revision_id is not null and not exists(select 1 from private.bpay_next_work_revision
   where work_id=p_work_id and id=p_revision_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REVISION_CONFLICT';end if;
 select src.* into v_revision from private.bpay_next_work_revision src
   where work_id=p_work_id and id=p_revision_id;
 if v_work.approval_state is distinct from 'APPROVED' or v_revision.approved_at_utc is null
   or v_revision.sealed_at_utc is null then return 'BPAY_NEXT_SELECTION_WORK_NOT_APPROVED';end if;
 if v_work.current_revision_id is distinct from p_revision_id or v_work.applied_revision_id is distinct from p_revision_id then
  return 'BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';end if;
 if v_work.week_ending_date-6>date_trunc('week',v_run.snooze_business_date::timestamp)::date then
  return 'BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE';end if;
 if exists(select 1 from public.timesheets t join public.timesheets_financials tf
   on tf.timesheet_id=t.timesheet_id and tf.is_current=true
   where t.booking_id=v_work.booking_id and t.is_current=true and tf.pay_on_hold is true) then
  return 'BPAY_NEXT_SELECTION_PAY_ON_HOLD';end if;
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
   if v_gate->'blocked'='true'::jsonb then return 'SOURCE_PAY_QUERY_OPEN';end if;
  exception when others then return 'SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end;
 end if;
 return null;
end
$function$;
alter function private.bpay_next_append_work_skip_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_append_work_skip_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_create_preparation_v1(
 p_run_id uuid,p_actor_user_id uuid,p_pay_date date,p_descriptor jsonb,p_command_id uuid
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_result jsonb;v_ns jsonb;v_intent jsonb;
 v_row jsonb;v_array text;v_keys text[];v_field text;v_id uuid;v_header jsonb;
 v_query_admitted boolean:=true;
 v_locked_candidates uuid[];v_locked_works uuid[];v_locked_revisions uuid[];v_locked_cases uuid[];
 v_candidate uuid;v_work uuid;v_revision uuid;v_sequence bigint;v_gate jsonb;v_member record;
 v_mode text;v_selected boolean;v_count bigint;v_code text;v_week_start date;v_work_count bigint:=0;
begin
 if p_command_id is null or jsonb_typeof(p_descriptor) is distinct from 'object'
   or octet_length(p_descriptor::text)>32768
   or not(p_descriptor ?& array['namespace','intent','revision','workbench_revision','adoption_revision'])
   or p_descriptor-array['namespace','intent','revision','workbench_revision','adoption_revision']<>'{}'::jsonb then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';
 end if;
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
 if exists(select 1 from jsonb_array_elements(v_intent->'components') c(value)
   where not exists(select 1 from jsonb_array_elements(v_intent->'works') w(value)
     where w.value->>'work_id'=c.value->>'work_id' and w.value->>'candidate_id'=c.value->>'candidate_id'
       and w.value->>'expected_revision_id'=c.value->>'expected_revision_id'
       and w.value->>'mode'='SUBSET' and w.value->'selected'='true'::jsonb)) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 if exists(select 1 from jsonb_array_elements(v_intent->'cases') a(value) where not exists(
   select 1 from private.bpay_next_case_component c where c.id=(a.value->>'case_component_id')::uuid
     and c.candidate_id=(a.value->>'candidate_id')::uuid)) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
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
 v_week_start:=date_trunc('week',v_run.snooze_business_date::timestamp)::date;
 for v_member in select m.*,w.approval_state,w.current_revision_id,w.applied_revision_id,w.week_ending_date,
   r.physical_timesheet_id,r.source_kind,r.approved_at_utc,r.sealed_at_utc
   from private.bpay_next_run_choice_member m join private.bpay_next_work w on w.id=m.work_id
   join private.bpay_next_work_revision r on r.work_id=w.id and r.id=m.expected_revision_id
   where m.run_id=p_run_id and m.member_kind='WORK' order by m.member_no loop
  v_code:=null;
  if v_member.approval_state<>'APPROVED' or v_member.approved_at_utc is null or v_member.sealed_at_utc is null then
   v_code:='BPAY_NEXT_SELECTION_WORK_NOT_APPROVED';
  elsif v_member.current_revision_id is distinct from v_member.expected_revision_id
    or v_member.applied_revision_id is distinct from v_member.expected_revision_id then
   v_code:='BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';
  elsif v_member.week_ending_date-6>v_week_start then v_code:='BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE';
  elsif exists(select 1 from private.bpay_next_work w
    join public.timesheets t on t.booking_id=w.booking_id and t.is_current=true
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
    where w.id=v_member.work_id and tf.pay_on_hold is true) then
   v_code:='BPAY_NEXT_SELECTION_PAY_ON_HOLD';
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
  v_gate:=null;
  if v_code is null and v_member.source_kind in ('SOURCE','PROTECTED') then
   if not v_query_admitted then v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
   else begin v_gate:=private.weekly_source_pay_query_gate_v1(v_member.physical_timesheet_id);
    if v_gate->'ok' is distinct from 'true'::jsonb or v_gate->>'code' is distinct from 'OK'
      or jsonb_typeof(v_gate->'blocked') is distinct from 'boolean' then
     v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
    elsif v_gate->'blocked'='true'::jsonb then v_code:='SOURCE_PAY_QUERY_OPEN';end if;
   exception when others then v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end;end if;
  end if;
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
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc)
      values(p_run_id,v_member.work_id,v_member.expected_revision_id,v_member.physical_timesheet_id,v_gate,transaction_timestamp());
   end if;
  end if;
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
 return v_result||jsonb_build_object('command_id',p_command_id,'agency_sequence',v_sequence::text,
   'selection_count',v_work_count::text,
   'selected_candidate_count',v_count::text,'empty',v_count=0,
   'choice_work_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'),
   'choice_case_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='CASE_COMPONENT'),'replay',false);
end
$function$;
alter function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid) owner to postgres;
revoke all on function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid) from public,anon,authenticated,service_role;

-- Existing WB36 inventory owner derives INCLUDED/SNOOZED at the retained cut.
-- WB21 has no independent segment tick or new segment-selection behaviour.
create or replace function private.bpay_next_materialise_choice_segments_v1(p_run_id uuid,p_work_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_choice private.bpay_next_work_choice%rowtype;v_keys text[];
 v_after text;v_pages bigint:=0;v_count bigint:=0;
begin
 select src.* into strict v_choice from private.bpay_next_work_choice src
   where run_id=p_run_id and work_id=p_work_id;
 if v_choice.segment_sealed_count is not null then return;end if;
 if v_choice.selection_state<>'SEALED' or v_choice.segment_page_count<>0 or v_choice.segment_count<>0 then
  raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_SELECTION_NOT_OPEN';end if;
 loop
  select array_agg(page.segment_key order by page.segment_key collate "C") into v_keys from (
    select s.segment_key from private.bpay_next_approved_segment s
    where s.revision_id=v_choice.expected_revision_id
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
alter function private.bpay_next_materialise_choice_segments_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_materialise_choice_segments_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- Accepted pages acknowledge the exact retained native cut, including skips.
create or replace function private.bpay_next_ack_choice_work_page_v1(
 p_run_id uuid,p_page_no bigint,p_body jsonb,p_explicit boolean
) returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_page private.bpay_next_selection_page%rowtype;
 v_member private.bpay_next_run_choice_member%rowtype;v_row jsonb;v_work uuid;v_n integer;
 v_index integer:=0;v_accepted bigint:=0;v_skipped jsonb:='[]';v_ids jsonb:='[]';
begin
 if p_run_id is null or p_page_no is null or p_page_no<1 or p_explicit is null
   or jsonb_typeof(p_body) is distinct from 'array' or jsonb_array_length(p_body) not between 1 and 100
   or octet_length(p_body::text)>32768 then
  raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';end if;
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id for update;
 if v_run.accepted_choice_descriptor is null then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_MISSING';end if;
 v_n:=jsonb_array_length(p_body);
 for v_row in select value from jsonb_array_elements(p_body) loop
  if p_explicit then
   if jsonb_typeof(v_row) is distinct from 'object'
     or not(v_row ?& array['work_id','expected_revision_id','mode'])
     or v_row-array['work_id','expected_revision_id','mode']<>'{}'::jsonb
     or jsonb_typeof(v_row->'work_id') is distinct from 'string'
     or jsonb_typeof(v_row->'expected_revision_id') is distinct from 'string'
     or v_row->>'mode' not in ('ALL','SUBSET') then
    raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';end if;
   v_work:=(v_row->>'work_id')::uuid;
  else
   if jsonb_typeof(v_row) is distinct from 'string' then
    raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_WORK_ID_NOT_STRING';end if;
   v_work:=(v_row#>>'{}')::uuid;
  end if;
  if v_ids ? v_work::text then
   raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';end if;
  v_ids:=v_ids||jsonb_build_array(v_work);
 end loop;
 select src.* into v_page from private.bpay_next_selection_page src where run_id=p_run_id and page_no=p_page_no;
 if found then
  if v_page.work_ids_json is distinct from v_ids
    or (p_explicit and v_page.choices_request_json is distinct from p_body)
    or (not p_explicit and v_page.choices_request_json is not null) then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_PAGE_REPLAY_CONFLICT';end if;
  return jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,'item_count',v_n::text,
    'selection_count',v_run.selection_count::text,'selected_candidate_count',v_run.selected_candidate_count::text,
    'skipped',v_page.skipped_json,'replay',true);
 end if;
 if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN'
   or v_run.preparation_expires_at_utc<=clock_timestamp() then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_ALREADY_SEALED';end if;
 if p_page_no<>v_run.selection_page_count+1 then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_PAGE_NOT_APPENDABLE';end if;
 for v_row in select value from jsonb_array_elements(p_body) loop
  v_index:=v_index+1;
  v_work:=case when p_explicit then (v_row->>'work_id')::uuid else (v_row#>>'{}')::uuid end;
  select src.* into v_member from private.bpay_next_run_choice_member src
    where run_id=p_run_id and member_kind='WORK' and member_no=v_run.uploaded_choice_work_count+v_index;
  if not found or v_member.work_id is distinct from v_work
    or (p_explicit and ((v_row->>'expected_revision_id')::uuid is distinct from v_member.expected_revision_id
      or v_row->>'mode' is distinct from v_member.selection_mode))
    or (not p_explicit and v_member.selection_mode<>'ALL') then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
  if v_member.skip_code is not null then
   v_skipped:=v_skipped||jsonb_build_array(jsonb_build_object('work_id',v_member.work_id,'code',v_member.skip_code));
  else
   if not exists(select 1 from private.bpay_next_run_selection s
     join private.bpay_next_work_choice choice on choice.run_id=s.run_id and choice.work_id=s.work_id
     where s.run_id=p_run_id and s.work_id=v_work and s.candidate_id=v_member.candidate_id
       and s.selection_no=v_run.uploaded_selection_count+v_accepted+1
       and choice.expected_revision_id=v_member.expected_revision_id and choice.selection_mode=v_member.selection_mode) then
    raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
   if v_member.selection_mode='ALL' then perform private.bpay_next_materialise_choice_segments_v1(p_run_id,v_work);end if;
   v_accepted:=v_accepted+1;
  end if;
 end loop;
 insert into private.bpay_next_selection_page(run_id,page_no,work_ids_json,first_selection_no,item_count,
   choices_request_json,skipped_json) values(p_run_id,p_page_no,v_ids,v_run.uploaded_selection_count+1,v_n,
     case when p_explicit then p_body else null end,v_skipped);
 update private.bpay_next_pay_run set selection_page_count=p_page_no,
   uploaded_choice_work_count=uploaded_choice_work_count+v_n,
   uploaded_selection_count=uploaded_selection_count+v_accepted where id=p_run_id;
 return jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,'item_count',v_n::text,
   'selection_count',v_run.selection_count::text,'selected_candidate_count',v_run.selected_candidate_count::text,
   'skipped',v_skipped,'replay',false);
end
$function$;
alter function private.bpay_next_ack_choice_work_page_v1(uuid,bigint,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_ack_choice_work_page_v1(uuid,bigint,jsonb,boolean) from public,anon,authenticated,service_role;


create or replace function private.bpay_next_append_selection_page_v1(
  p_run_id uuid,p_page_no bigint,p_work_ids jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_skip_code text;
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
  if exists(select 1 from private.bpay_next_pay_run where id=p_run_id and accepted_choice_descriptor is not null) then
    return private.bpay_next_ack_choice_work_page_v1(p_run_id,p_page_no,p_work_ids,false);
  end if;
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
    v_skip_code:=private.bpay_next_append_work_skip_v1(p_run_id,v_work_id,v_work.current_revision_id);
    if v_skip_code is not null then
      v_skipped:=v_skipped||jsonb_build_array(jsonb_build_object('work_id',v_work_id,'code',v_skip_code));continue;end if;
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

create or replace function private.bpay_next_append_work_choices_v1(p_run_id uuid,p_page_no bigint,p_choices jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_skip_code text;
  v_run private.bpay_next_pay_run%rowtype;v_page private.bpay_next_selection_page%rowtype;
  v_work private.bpay_next_work%rowtype;v_choice private.bpay_next_work_choice%rowtype;
  v_item jsonb;v_ids jsonb:='[]'::jsonb;v_n integer;v_i integer:=0;
  v_work_id uuid;v_revision uuid;v_mode text;v_member bigint;v_inserted integer;v_all integer:=0;
  v_skipped jsonb:='[]'::jsonb;v_week_start date;
begin
  if exists(select 1 from private.bpay_next_pay_run where id=p_run_id and accepted_choice_descriptor is not null) then
    return private.bpay_next_ack_choice_work_page_v1(p_run_id,p_page_no,p_choices,true);
  end if;
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
    v_skip_code:=private.bpay_next_append_work_skip_v1(p_run_id,v_work_id,v_revision);
    if v_skip_code is not null then
      v_skipped:=v_skipped||jsonb_build_array(jsonb_build_object('work_id',v_work_id,'code',v_skip_code));continue;end if;
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

create or replace function private.bpay_next_seal_selection_v1(
  p_run_id uuid,p_expected_pages bigint,p_expected_count bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_selected record;
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
      or v_run.exclusion_page_count<>(select (count(*)+99)/100 from private.bpay_next_run_selection_exclusion where run_id=p_run_id)
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
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc)
      values(p_run_id,v_selected.work_id,v_selected.expected_revision_id,v_selected.physical_timesheet_id,
        private.weekly_source_pay_query_gate_v1(v_selected.physical_timesheet_id),pg_catalog.transaction_timestamp());
  end loop;
  end if;
  update private.bpay_next_pay_run
    set selection_state='SEALED',review_revision=review_revision+1
    where id=p_run_id;
  return jsonb_build_object('run_id',p_run_id,'sealed',true,
    'selection_count',v_run.selection_count,'replay',false);
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
  if v_run.accepted_choice_descriptor is not null then
    if exists(select 1 from unnest(p_approved_line_ids) line_id where not exists(
      select 1 from jsonb_array_elements(v_run.accepted_choice_descriptor->'intent'->'components') choice
      where choice->>'work_id'=p_work_id::text and choice->>'expected_revision_id'=p_expected_revision_id::text
        and choice->>'approved_line_id'=line_id::text and choice->'selected'='true'::jsonb)) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
  elsif not exists(select 1 from private.bpay_next_work w where w.id=p_work_id and w.approval_state='APPROVED'
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
  if v_run.accepted_choice_descriptor is not null and
    (select count(*) from jsonb_array_elements(v_run.accepted_choice_descriptor->'intent'->'components') choice
      where choice->>'work_id'=p_work_id::text and choice->>'expected_revision_id'=p_expected_revision_id::text
        and choice->'selected'='true'::jsonb)<>v_choice.component_count then
    raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_UPLOAD_INCOMPLETE';end if;
  update private.bpay_next_work_choice set selection_state='SEALED' where run_id=p_run_id and work_id=p_work_id;
  update private.bpay_next_pay_run set sealed_work_choice_count=sealed_work_choice_count+1 where id=p_run_id;
  if v_run.accepted_choice_descriptor is not null then
    perform private.bpay_next_materialise_choice_segments_v1(p_run_id,p_work_id);end if;
  return pg_catalog.jsonb_build_object('run_id',p_run_id,'work_id',p_work_id,'expected_revision_id',p_expected_revision_id,
    'sealed',true,'component_count',v_choice.component_count::text,'replay',false);
end
$function$;

create or replace function private.bpay_next_append_case_selection_page_v1(
  p_run_id uuid,p_candidate_id uuid,p_selection_revision bigint,p_page_no bigint,p_request_id uuid,
  p_component_ids uuid[],p_selected boolean[]
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;v_header private.bpay_next_case_selection%rowtype;
  v_page private.bpay_next_case_selection_page%rowtype;v_n integer;v_yes integer;v_i integer;v_case uuid;v_inserted integer;
begin
  v_n:=pg_catalog.cardinality(p_component_ids);
  if p_run_id is null or p_candidate_id is null or p_selection_revision is null or p_selection_revision<1
     or p_page_no is null or p_page_no<1 or p_request_id is null
     or p_component_ids is null or p_selected is null or v_n not between 1 and 100
     or pg_catalog.cardinality(p_selected)<>v_n or pg_catalog.array_ndims(p_component_ids)<>1 or pg_catalog.array_ndims(p_selected)<>1
     or pg_catalog.array_lower(p_component_ids,1)<>1 or pg_catalog.array_lower(p_selected,1)<>1 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_SELECTION_INPUT_INVALID';
  end if;
  if pg_catalog.array_position(p_component_ids,null) is not null or pg_catalog.array_position(p_selected,null) is not null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_SELECTION_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select p.* into v_page from private.bpay_next_case_selection_page p
    where p.run_id=p_run_id and p.candidate_id=p_candidate_id and p.selection_revision=p_selection_revision and p.page_no=p_page_no;
  if found then
    if v_page.request_id<>p_request_id or v_page.item_count<>v_n
       or exists(select 1 from private.bpay_next_case_selection_item i where i.run_id=p_run_id and i.candidate_id=p_candidate_id
         and i.selection_revision=p_selection_revision and i.page_no=p_page_no
         and (i.case_component_id is distinct from p_component_ids[i.item_no] or i.is_selected is distinct from p_selected[i.item_no])) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_REPLAY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object('page_no',p_page_no::text,'item_count',v_n::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_SELECTION_NOT_OPEN';
  end if;
  if v_run.accepted_choice_descriptor is not null then
    if p_selection_revision<>1 or not exists(select 1 from private.bpay_next_selection_candidate
      where run_id=p_run_id and candidate_id=p_candidate_id) or exists(select 1 from unnest(p_component_ids,p_selected) requested(component_id,selected)
      where not exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=p_run_id
        and member.member_kind='CASE_COMPONENT' and member.candidate_id=p_candidate_id
        and member.case_component_id=requested.component_id and member.selected=requested.selected)) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
  end if;
  select h.* into v_header from private.bpay_next_case_selection h
    where h.run_id=p_run_id and h.candidate_id=p_candidate_id and h.selection_revision=p_selection_revision for update;
  if not found then
    if p_page_no<>1 or exists(select 1 from private.bpay_next_case_selection h where h.run_id=p_run_id and h.candidate_id=p_candidate_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_SELECTION_REVISION_NOT_INITIAL';
    end if;
    -- Exclusion-only manifests may accompany an already selected WORK worker,
    -- but must not invent a case-only selected Candidate with no selected case.
    if not (true=any(p_selected)) and not exists(select 1 from private.bpay_next_selection_candidate
      where run_id=p_run_id and candidate_id=p_candidate_id) then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_SELECTION_EMPTY_CANDIDATE';
    end if;
    insert into private.bpay_next_selection_candidate(run_id,candidate_id,member_no)
      values(p_run_id,p_candidate_id,v_run.selected_candidate_count+1) on conflict(run_id,candidate_id) do nothing;
    get diagnostics v_inserted=row_count;
    if v_inserted=1 then update private.bpay_next_pay_run set selected_candidate_count=selected_candidate_count+1 where id=p_run_id;end if;
    insert into private.bpay_next_case_selection(run_id,candidate_id,selection_revision,status)
      values(p_run_id,p_candidate_id,p_selection_revision,'OPEN') returning * into v_header;
  end if;
  if v_header.status<>'OPEN' or p_page_no<>v_header.page_count+1 then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_SELECTION_PAGE_NOT_APPENDABLE';
  end if;
  select count(*)::integer into v_yes from pg_catalog.unnest(p_selected) s(value) where s.value;
  insert into private.bpay_next_case_selection_page
    (run_id,candidate_id,selection_revision,page_no,request_id,first_selection_no,item_count,selected_count,excluded_count)
    values(p_run_id,p_candidate_id,p_selection_revision,p_page_no,p_request_id,v_header.item_count+1,v_n,v_yes,v_n-v_yes);
  for v_i in 1..v_n loop
    select c.case_id into strict v_case from private.bpay_next_case_component c
      where c.id=p_component_ids[v_i] and c.candidate_id=p_candidate_id;
    insert into private.bpay_next_case_selection_item
      (run_id,candidate_id,selection_revision,page_no,item_no,selection_no,case_component_id,case_id,is_selected)
      values(p_run_id,p_candidate_id,p_selection_revision,p_page_no,v_i,v_header.item_count+v_i,p_component_ids[v_i],v_case,p_selected[v_i]);
  end loop;
  update private.bpay_next_case_selection set page_count=p_page_no,item_count=item_count+v_n,
    selected_count=selected_count+v_yes,excluded_count=excluded_count+(v_n-v_yes)
    where run_id=p_run_id and candidate_id=p_candidate_id and selection_revision=p_selection_revision;
  return pg_catalog.jsonb_build_object('page_no',p_page_no::text,'item_count',v_n::text,
    'selected_count',v_yes::text,'excluded_count',(v_n-v_yes)::text,'replay',false);
end
$function$;

create or replace function private.bpay_next_seal_case_selection_v1(
  p_run_id uuid,p_candidate_id uuid,p_selection_revision bigint,p_expected_pages bigint,p_expected_items bigint
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_header private.bpay_next_case_selection%rowtype;
begin
  if p_run_id is null or p_candidate_id is null or p_selection_revision is null or p_selection_revision<1
     or p_expected_pages is null or p_expected_pages<1 or p_expected_items is null or p_expected_items<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_SELECTION_SEAL_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select h.* into strict v_header from private.bpay_next_case_selection h
    where h.run_id=p_run_id and h.candidate_id=p_candidate_id and h.selection_revision=p_selection_revision for update;
  if v_header.page_count<>p_expected_pages or v_header.item_count<>p_expected_items then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_SEAL_COUNT_MISMATCH';
  end if;
  if v_header.status='SEALED' then
    return pg_catalog.jsonb_build_object('sealed',true,'selection_revision',p_selection_revision::text,
      'selected_count',v_header.selected_count::text,'excluded_count',v_header.excluded_count::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_SELECTION_SEAL_NOT_OPEN';
  end if;
  if v_run.accepted_choice_descriptor is not null and
    (select count(*) from private.bpay_next_run_choice_member where run_id=p_run_id
      and member_kind='CASE_COMPONENT' and candidate_id=p_candidate_id)<>v_header.item_count then
    raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_UPLOAD_INCOMPLETE';end if;
  -- Counts were advanced by each complete bounded page. 0310 seal guards verify
  -- that append-only children match them; no growing final all-case serialisation.
  update private.bpay_next_case_selection set status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where run_id=p_run_id and candidate_id=p_candidate_id and selection_revision=p_selection_revision;
  -- Exact first-seal receipt only: replay returned above. This gives the
  -- all-case/no-WORK root a constant-time admission proof, not a final scan.
  if v_header.selected_count>0 then
    update private.bpay_next_pay_run set sealed_case_candidate_count=sealed_case_candidate_count+1
      where id=p_run_id;
  end if;
  return pg_catalog.jsonb_build_object('sealed',true,'selection_revision',p_selection_revision::text,
    'selected_count',v_header.selected_count::text,'excluded_count',v_header.excluded_count::text,'replay',false);
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
v_command private.bpay_next_command%rowtype;
v_epoch bigint;
begin
  if p_run_id is null or p_command_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PREPARE_COMMAND_INPUT_INVALID';
  end if;
select m.owner_epoch into v_epoch
  from private.bpay_next_module_control m
  where m.id=1 and m.active_owner='NEXT' for share;
if v_epoch is null then
  raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
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
  -- Plain read: replay must not take command UPDATE while holding run UPDATE.
  select c.* into strict v_command
    from private.bpay_next_command c where c.id=p_command_id;
  if v_command.command_kind is distinct from 'PREPARE'
     or v_command.module_epoch is distinct from v_epoch then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
  end if;
  v_sequence:=v_command.agency_sequence;
  if v_command.status='RECEIVED' then
    if v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from p_command_id
       or v_command.expected_member_count is not null
       or v_command.enrolled_member_count<>0
       or v_command.sealed_at_utc is not null
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=p_command_id)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=p_command_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
    end if;
    -- Identical original1590 sealed/count/Candidate predicate.
    if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
       or v_run.work_choice_count<>v_run.selection_count
       or v_run.sealed_work_choice_count<>v_run.selection_count
       or v_run.selected_candidate_count<1
       or (v_run.selection_count=0 and ((v_run.accepted_choice_descriptor is null and v_run.selection_page_count<>0)
         or v_run.sealed_case_candidate_count<>v_run.selected_candidate_count)) then
      raise exception using errcode='55000',
        message='BPAY_NEXT_PREPARE_SELECTION_NOT_SEALED';
    end if;
    update private.bpay_next_command
      set expected_member_count=v_run.selected_candidate_count,status='ENROLLING'
      where id=p_command_id and status='RECEIVED'
        and module_epoch=v_epoch and expected_member_count is null
        and enrolled_member_count=0 and sealed_at_utc is null;
    if not found then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
    end if;
    return jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,
      'agency_sequence',v_sequence,'replay',false);
  end if;
  -- Do not return an abandoned zero-job receipt as successful enqueue.
  if v_command.status not in ('ENROLLING','SEALED','COMPLETE')
     or (v_command.status='COMPLETE' and v_command.expected_member_count=0) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
  end if;
  return jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,
    'agency_sequence',v_sequence,'replay',true);
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

create or replace function private.bpay_next_enroll_one_v1()
returns uuid
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_last bigint;
  v_command private.bpay_next_command%rowtype;
  v_member private.bpay_next_command_member%rowtype;
  v_job_id uuid;
  v_epoch bigint;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_selected_candidate uuid;
  v_expired_preparation boolean;
begin
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select last_enrolled_sequence into strict v_last
    from private.bpay_next_enrollment_clock where id=1 for update;
  select * into v_command from private.bpay_next_command
    where agency_sequence=v_last+1 for update;
  if not found then
    return null;
  end if;
  if v_command.command_kind='PREPARE' then
    select rc.run_id into strict v_run_id
      from private.bpay_next_run_command rc
      where rc.command_id=v_command.id;
if v_command.status in ('RECEIVED','COMPLETE') then
  -- NO FOR SHARE/UPDATE here. Seal/expiry acquire run then command;
  -- waiting/retired metadata must not add command -> run locking.
  select r.* into strict v_run from private.bpay_next_pay_run r
    where r.id=v_run_id;
  if v_command.status='RECEIVED' then
    if v_command.module_epoch is distinct from v_epoch
       or v_run.status is distinct from 'PREPARING'
       or v_run.selection_state is distinct from 'OPEN'
       or v_run.confirmed_at_utc is not null
       or v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from v_command.id
       or v_command.expected_member_count is not null
       or v_command.enrolled_member_count<>0
       or v_command.sealed_at_utc is not null
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=v_command.id)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=v_command.id)
       or exists(select 1 from private.bpay_next_run_worker rw
         where rw.run_id=v_run_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    return null;
  end if;
  -- Only an exact expired, never-enrolled positive CREATE may retire0.
  if v_command.module_epoch is distinct from v_epoch
     or v_command.expected_member_count is distinct from 0::bigint
     or v_command.enrolled_member_count<>0
     or v_command.sealed_at_utc is null
     or v_run.selection_state is distinct from 'OPEN'
     or v_run.status not in ('CANCELLING','CANCELLED')
     or v_run.confirmed_at_utc is not null
     or v_run.accepted_choice_descriptor is null
     or v_run.accepted_choice_command_id is distinct from v_command.id
     or not private.bpay_next_preparation_expiry_fenced_v1(v_run_id,v_command.id)
     or exists(select 1 from private.bpay_next_command_member cm
       where cm.command_id=v_command.id)
     or exists(select 1 from private.bpay_next_job j
       where j.command_id=v_command.id)
     or exists(select 1 from private.bpay_next_run_worker rw
       where rw.run_id=v_run_id) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
  end if;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence
    where id=1 and last_enrolled_sequence=v_last
      and v_command.agency_sequence=v_last+1;
  if not found then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
  end if;
  return null;
end if;
    select * into strict v_run from private.bpay_next_pay_run
      where id=v_run_id for share;
    v_expired_preparation:=private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_command.id);
    if v_command.module_epoch<>v_epoch
       or v_command.status<>'ENROLLING'
       or (v_run.status<>'PREPARING' and not
           (v_run.status='CANCELLING' and v_expired_preparation))
       or v_run.selection_state<>'SEALED'
       or v_command.expected_member_count<>v_run.selected_candidate_count
       or v_command.enrolled_member_count>=v_command.expected_member_count then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    select sc.candidate_id into strict v_selected_candidate
      from private.bpay_next_selection_candidate sc
      where sc.run_id=v_run_id
        and sc.member_no=v_command.enrolled_member_count+1;
    insert into private.bpay_next_command_member
      (command_id,candidate_id,member_no)
      values(v_command.id,v_selected_candidate,
             v_command.enrolled_member_count+1);
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_selected_candidate,'PREPARE',
             case when v_expired_preparation then 'DONE' else 'READY' end,
             case when v_expired_preparation then 'EXPIRED' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command
      set enrolled_member_count=enrolled_member_count+1,
          status=case when enrolled_member_count+1=expected_member_count
            then 'SEALED' else 'ENROLLING' end,
          sealed_at_utc=case when enrolled_member_count+1=expected_member_count
            then pg_catalog.transaction_timestamp() else null end
      where id=v_command.id;
    if v_command.enrolled_member_count+1=v_command.expected_member_count then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARATION_EXPIRY' then
    -- Exact header fence permits only metadata enrollment here. The helper
    -- retires unfinished original PREPARE jobs and binds ONE cleanup member;
    -- financial holds are released later by its normally ordered worker.
    -- The helper, not this caller, owns member counts and command sealing.
    v_job_id:=private.bpay_next_enroll_preparation_expiry_one_v1(v_command.id);
    if exists(select 1 from private.bpay_next_command c
        where c.id=v_command.id
          and c.enrolled_member_count=c.expected_member_count
          and c.status in ('SEALED','COMPLETE')) then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.status <> 'SEALED' then
    return null;
  end if;
  if v_command.command_kind='CASE_WRITE_OFF' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_write_off_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_WRITE_OFF','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_CREATE' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_create_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_CREATE','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='REISSUE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_reissue_cancel_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'REISSUE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind in ('CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','INTERNAL_SETTLEMENT') then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if v_command.command_kind='INTERNAL_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is null and r.internal_receipt_id is not null)
       or v_command.command_kind='CSV_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is not null and r.internal_receipt_id is null)
       or v_command.command_kind='CSV_RETURN' and not exists(select 1 from private.bpay_next_return_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id)
       or v_command.command_kind='CASH_REISSUE' and not exists(select 1 from private.bpay_next_reissue_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,v_command.command_kind,'READY','MEMBERS',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='SIMPLE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_cancel_request r
      where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
        and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'SIMPLE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='PAYE_NET_ENTRY' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_paye_net_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'PAYE_NET_ENTRY','READY',
             case when exists(select 1 from private.bpay_next_paye_net_request r
               where r.command_id=v_command.id and r.case_draft_state_id is not null)
               then 'FINANCE_ALLOCATE' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='TRANSFER_BUILD' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_transfer t
                  where t.build_command_id=v_command.id
                    and t.candidate_id=v_member.candidate_id
                    and t.status='BUILDING') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BUILD_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'TRANSFER_BUILD','READY','WORK',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.module_epoch<>v_epoch
     or v_command.command_kind<>'POSITION_APPLY'
     or v_command.expected_member_count<>1
     or v_command.enrolled_member_count<>0 then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_COMMAND_INVALID';
  end if;
  select * into strict v_member from private.bpay_next_command_member
    where command_id=v_command.id and member_no=1;
  if not exists (select 1 from private.bpay_next_publication p
                 where p.command_id=v_command.id and p.candidate_id=v_member.candidate_id) then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_PUBLICATION_MISSING';
  end if;
  insert into private.bpay_next_job
    (command_id,command_sequence,module_epoch,candidate_id,job_kind,
     status,phase,owner_epoch)
    values (v_command.id,v_command.agency_sequence,v_epoch,
            v_member.candidate_id,'POSITION_APPLY','READY','NEW',1)
    returning id into v_job_id;
  update private.bpay_next_command set enrolled_member_count=1
    where id=v_command.id;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence where id=1;
  return v_job_id;
end
$function$;

create or replace function private.bpay_next_accept_preparation_expiry_v1(
  p_command_id uuid,p_run_id uuid,p_actor_user_id uuid,p_expected_deadline timestamptz,
  p_observed_now timestamptz default null)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_prepare_command private.bpay_next_command%rowtype;
 v_epoch bigint;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_expiry_request%rowtype;v_sequence bigint;v_prepare uuid;v_now timestamptz;
begin
  if p_command_id is null or p_run_id is null or p_expected_deadline is null
     or not pg_catalog.isfinite(p_expected_deadline)
     or (p_observed_now is not null and not pg_catalog.isfinite(p_observed_now)) then
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-preparation-expiry:'||p_command_id::text,0));
  select * into strict v_run from private.bpay_next_pay_run where id=p_run_id for update;
  select * into v_request from private.bpay_next_preparation_expiry_request where command_id=p_command_id;
  if found then
    if v_request.run_id<>p_run_id or v_request.actor_user_id is distinct from p_actor_user_id
       or v_request.captured_deadline<>p_expected_deadline
       or not exists(select 1 from private.bpay_next_command where id=p_command_id
         and command_kind='PREPARATION_EXPIRY' and module_epoch=v_epoch) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REPLAY_CONFLICT';end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command where id=p_command_id;
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',v_sequence::text,
      'phase',v_request.status,'expected_candidates',v_request.expected_candidate_count::text,
      'completed_candidates',v_request.completed_candidate_count::text,'replay',true);
  end if;
  -- Public wrappers MUST pass NULL, never a browser/request-supplied clock.
  -- Obtain real time after waiting for this same header as confirmation.
  -- The private ungranted operand permits labelled native 72h clock vectors.
  v_now:=coalesce(p_observed_now,pg_catalog.clock_timestamp());
  if p_actor_user_id is not null and not exists(select 1 from public.tms_users
      where id=p_actor_user_id and is_active is true and role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_EXPIRY_ACTOR_FORBIDDEN';end if;
  if v_run.confirmed_at_utc is not null or v_run.status not in ('PREPARING','REVIEW') then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_NOT_UNCONFIRMED';end if;
  if v_run.preparation_expires_at_utc is null then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_DEADLINE_UNBOUND';end if;
  if v_run.preparation_expires_at_utc<>p_expected_deadline then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_DEADLINE_CONFLICT';end if;
  if v_now<v_run.preparation_expires_at_utc then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_NOT_DUE';end if;
  if exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=p_run_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_RUN_ALREADY_BOUND';end if;
  select command_id into v_prepare from private.bpay_next_run_command where run_id=p_run_id;
if v_prepare is not null then
  select c.* into strict v_prepare_command
    from private.bpay_next_command c where c.id=v_prepare;
  if v_prepare_command.status='RECEIVED' then
    -- Original run UPDATE is already held. Enroller RECEIVED takes no run lock.
    select c.* into strict v_prepare_command
      from private.bpay_next_command c where c.id=v_prepare for update;
    if v_prepare_command.command_kind is distinct from 'PREPARE'
       or v_prepare_command.module_epoch is distinct from v_epoch
       or v_prepare_command.status is distinct from 'RECEIVED'
       or v_prepare_command.expected_member_count is not null
       or v_prepare_command.enrolled_member_count<>0
       or v_prepare_command.sealed_at_utc is not null
       or v_run.status is distinct from 'PREPARING'
       or v_run.selection_state is distinct from 'OPEN'
       or v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from v_prepare
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=v_prepare)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=v_prepare)
       or exists(select 1 from private.bpay_next_run_worker rw
         where rw.run_id=p_run_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
    end if;
    update private.bpay_next_command
      set expected_member_count=0,status='COMPLETE',sealed_at_utc=v_now
      where id=v_prepare and status='RECEIVED';
    if not found then
      raise exception using errcode='23514',
        message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
    end if;
  elsif not exists(select 1 from private.bpay_next_command
      where id=v_prepare and command_kind='PREPARE' and module_epoch=v_epoch
        and status in ('ENROLLING','SEALED')
        and expected_member_count=v_run.selected_candidate_count) then
    -- Original normal binding predicate, unchanged.
    raise exception using errcode='23514',
      message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
  end if;
end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARATION_EXPIRY');
  insert into private.bpay_next_preparation_expiry_request(command_id,run_id,original_prepare_command_id,
    captured_deadline,selection_count,selection_page_count,selection_review_revision,expected_candidate_count,
    initiator_kind,actor_user_id,accepted_at_utc,status,finished_at_utc)
    values(p_command_id,p_run_id,v_prepare,p_expected_deadline,v_run.selection_count,v_run.selection_page_count,v_run.review_revision,
      v_run.selected_candidate_count,case when p_actor_user_id is null then 'SYSTEM' else 'OFFICE' end,p_actor_user_id,v_now,
      case when v_run.selected_candidate_count=0 then 'EXPIRED' else 'CANCELLING' end,
      case when v_run.selected_candidate_count=0 then v_now else null end);
  update private.bpay_next_pay_run set status=case when selected_candidate_count=0 then 'CANCELLED' else 'CANCELLING' end,
    review_revision=review_revision+1 where id=p_run_id;
  update private.bpay_next_command set expected_member_count=v_run.selected_candidate_count,
    status=case when v_run.selected_candidate_count=0 then 'COMPLETE' else 'ENROLLING' end,
    sealed_at_utc=case when v_run.selected_candidate_count=0 then v_now else null end where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',v_sequence::text,
    'phase',case when v_run.selected_candidate_count=0 then 'EXPIRED' else 'CANCELLING' end,
    'expected_candidates',v_run.selected_candidate_count::text,'completed_candidates','0','replay',false);
end
$function$;

create or replace function private.bpay_next_enroll_preparation_expiry_one_v1(p_command_id uuid)
returns uuid language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_original_unenrolled boolean:=false;
 v_run_id uuid;v_epoch bigint;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_expiry_request%rowtype;v_command private.bpay_next_command%rowtype;
  v_original private.bpay_next_job%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_candidate uuid;v_member bigint;v_job uuid;v_state uuid;
begin
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id into strict v_run_id from private.bpay_next_preparation_expiry_request where command_id=p_command_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for share;
  select * into strict v_request from private.bpay_next_preparation_expiry_request where command_id=p_command_id for update;
  select * into strict v_command from private.bpay_next_command where id=p_command_id for update;
  if v_command.command_kind<>'PREPARATION_EXPIRY' or v_command.module_epoch<>v_epoch
     or v_command.expected_member_count<>v_request.expected_candidate_count
     or v_command.enrolled_member_count<>v_request.enrolled_candidate_count
     or v_run.confirmed_at_utc is not null or v_run.preparation_expires_at_utc<>v_request.captured_deadline
     or v_run.selection_count<>v_request.selection_count or v_run.selection_page_count<>v_request.selection_page_count
     or v_run.selected_candidate_count<>v_request.expected_candidate_count
     or v_run.review_revision<>v_request.selection_review_revision+1 then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ENROLLMENT_SCOPE_INVALID';end if;
  if v_request.expected_candidate_count=0 then
    if v_run.status<>'CANCELLED' or v_request.status<>'EXPIRED' or v_command.status<>'COMPLETE' then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_EMPTY_NOT_COMPLETE';end if;
    return null;
  end if;
  if v_run.status<>'CANCELLING' or v_request.status<>'CANCELLING' or v_command.status<>'ENROLLING'
     or v_request.enrolled_candidate_count>=v_request.expected_candidate_count then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_NOT_ENROLLABLE';end if;
  v_member:=v_request.enrolled_candidate_count+1;
  select candidate_id into strict v_candidate from private.bpay_next_selection_candidate where run_id=v_run_id and member_no=v_member;
if v_request.original_prepare_command_id is not null then
  v_original_unenrolled:=
    v_run.selection_state='OPEN'
    and v_run.accepted_choice_descriptor is not null
    and v_run.accepted_choice_command_id=v_request.original_prepare_command_id
    and private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_request.original_prepare_command_id)
    and exists(select 1 from private.bpay_next_command c
      where c.id=v_request.original_prepare_command_id
        and c.command_kind='PREPARE' and c.module_epoch=v_epoch
        and c.status='COMPLETE' and c.expected_member_count=0
        and c.enrolled_member_count=0 and c.sealed_at_utc is not null)
    and not exists(select 1 from private.bpay_next_command_member cm
      where cm.command_id=v_request.original_prepare_command_id)
    and not exists(select 1 from private.bpay_next_job j
      where j.command_id=v_request.original_prepare_command_id)
    and not exists(select 1 from private.bpay_next_run_worker rw
      where rw.run_id=v_run_id);
  if not coalesce(v_original_unenrolled,false) then
    -- Complete original ordinary-job retirement block follows unchanged.
    select original_job.* into strict v_original from private.bpay_next_job original_job
      where command_id=v_request.original_prepare_command_id
        and candidate_id=v_candidate and job_kind='PREPARE' for update;
    if v_original.module_epoch<>v_epoch then
      raise exception using errcode='23514',
        message='BPAY_NEXT_EXPIRY_ORIGINAL_JOB_INVALID';
    end if;
    if v_original.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='EXPIRED',
        lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_PREPARATION_EXPIRED' where id=v_original.id;
    end if;
  end if;
end if;
  select * into v_worker from private.bpay_next_run_worker where run_id=v_run_id and candidate_id=v_candidate;
  if v_worker.id is not null then
    if v_original.id is null or v_worker.status not in ('PREPARING','REVIEW','READY') then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_NOT_PREPARATION';end if;
    select id into v_state from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
      and preparation_revision=v_worker.preparation_revision and selection_revision=v_worker.case_selection_revision
      and pass_kind='DRAFT' and projection_no=0;
  end if;
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_candidate,v_member);
  insert into private.bpay_next_job(command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
    values(p_command_id,v_command.agency_sequence,v_epoch,v_candidate,'PREPARATION_EXPIRY','READY','NEW',1) returning id into v_job;
  insert into private.bpay_next_preparation_expiry_worker(command_id,candidate_id,run_id,member_no,job_id,run_worker_id,
    original_job_id,original_job_status,original_job_phase,original_job_cursor,original_position_work_cursor,
    original_position_component_cursor,original_applied_line_count,original_worker_status,original_review_issue_code,
    original_review_issue_work_id,preparation_revision,selection_revision,draft_state_id,expected_work_count,expected_case_hold_count,status,stage)
    values(p_command_id,v_candidate,v_run_id,v_member,v_job,v_worker.id,v_original.id,v_original.status,v_original.phase,v_original.cursor_key,
      v_original.position_work_cursor,v_original.position_component_cursor,v_original.applied_line_count,v_worker.status,
      v_worker.review_issue_code,v_worker.review_issue_work_id,v_worker.preparation_revision,v_worker.case_selection_revision,v_state,
      coalesce(v_worker.captured_line_count,0),coalesce(v_worker.active_case_hold_count,0),'READY','NEW');
  update private.bpay_next_preparation_expiry_request set enrolled_candidate_count=v_member where command_id=p_command_id;
  update private.bpay_next_command set enrolled_member_count=v_member,
    status=case when v_member=expected_member_count then 'SEALED' else 'ENROLLING' end,
    sealed_at_utc=case when v_member=expected_member_count then pg_catalog.clock_timestamp() else null end where id=p_command_id;
  return v_job;
end
$function$;

create or replace function private.bpay_next_preparation_expiry_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_original_unenrolled boolean:=false;
 v_run private.bpay_next_pay_run%rowtype;v_request private.bpay_next_preparation_expiry_request%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_EVIDENCE_DELETE_FORBIDDEN';end if;
  if tg_table_name='bpay_next_pay_run' then
    if tg_op='INSERT' then
      if new.preparation_expires_at_utc is null or new.preparation_expires_at_utc<>new.created_at_utc+interval '72 hours' then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CAPTURED_DEADLINE_REQUIRED';end if;
    elsif (new.created_at_utc,new.preparation_expires_at_utc) is distinct from (old.created_at_utc,old.preparation_expires_at_utc) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_DEADLINE_IMMUTABLE';end if;
    if new.status='CANCELLING' or (tg_op='UPDATE' and old.status='CANCELLING') then
      if new.confirmed_at_utc is not null or new.status not in ('CANCELLING','CANCELLED')
         or not exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=new.id
           and captured_deadline=new.preparation_expires_at_utc and selection_count=new.selection_count
           and selection_page_count=new.selection_page_count and expected_candidate_count=new.selected_candidate_count
           and new.review_revision=selection_review_revision+1) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_HEADER_FENCE_INVALID';end if;
    end if;
    return new;
  end if;
  if tg_op='INSERT' then
    if tg_table_name='bpay_next_preparation_expiry_request' then
      select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;
      if v_run.status not in ('PREPARING','REVIEW') or v_run.confirmed_at_utc is not null
         or new.captured_deadline is distinct from v_run.preparation_expires_at_utc
         or (new.selection_count,new.selection_page_count,new.selection_review_revision,new.expected_candidate_count)
           is distinct from (v_run.selection_count,v_run.selection_page_count,v_run.review_revision,v_run.selected_candidate_count)
         or new.enrolled_candidate_count<>0 or new.completed_candidate_count<>0
         or not exists(select 1 from private.bpay_next_command where id=new.command_id and command_kind='PREPARATION_EXPIRY' and status='RECEIVED')
         or new.original_prepare_command_id is distinct from (select command_id from private.bpay_next_run_command where run_id=new.run_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REQUEST_INITIAL_INVALID';end if;
    else
      select * into strict v_request from private.bpay_next_preparation_expiry_request where command_id=new.command_id;
      select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;
v_original_unenrolled:=
  v_request.original_prepare_command_id is not null
  and new.original_job_id is null and new.run_worker_id is null
  and new.expected_work_count=0 and new.expected_case_hold_count=0
  and v_run.selection_state='OPEN'
  and v_run.accepted_choice_descriptor is not null
  and v_run.accepted_choice_command_id=v_request.original_prepare_command_id
  and private.bpay_next_preparation_expiry_fenced_v1(
    new.run_id,v_request.original_prepare_command_id)
  and exists(select 1 from private.bpay_next_command c
    join private.bpay_next_job expiry on expiry.id=new.job_id
      and expiry.module_epoch=c.module_epoch
    where c.id=v_request.original_prepare_command_id
      and c.command_kind='PREPARE' and c.status='COMPLETE'
      and c.expected_member_count=0 and c.enrolled_member_count=0
      and c.sealed_at_utc is not null)
  and not exists(select 1 from private.bpay_next_command_member cm
    where cm.command_id=v_request.original_prepare_command_id)
  and not exists(select 1 from private.bpay_next_job j
    where j.command_id=v_request.original_prepare_command_id)
  and not exists(select 1 from private.bpay_next_run_worker rw
    where rw.run_id=new.run_id);
      if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_request.status<>'CANCELLING'
         or new.run_id<>v_request.run_id or new.member_no<>v_request.enrolled_candidate_count+1
         or new.status<>'READY' or new.stage<>'NEW' or new.checkpoint<>0
         or new.released_work_count<>0 or new.released_case_hold_count<>0 or new.work_cursor is not null or new.case_hold_cursor is not null
         or not exists(select 1 from private.bpay_next_selection_candidate where run_id=new.run_id and candidate_id=new.candidate_id and member_no=new.member_no)
         or not exists(select 1 from private.bpay_next_job where id=new.job_id and command_id=new.command_id and candidate_id=new.candidate_id
           and job_kind='PREPARATION_EXPIRY' and status='READY' and phase='NEW')
         or (((v_request.original_prepare_command_id is null)
       is distinct from (new.original_job_id is null))
    and not coalesce(v_original_unenrolled,false))
         or (new.original_job_id is not null and not exists(select 1 from private.bpay_next_job where id=new.original_job_id
           and command_id=v_request.original_prepare_command_id and candidate_id=new.candidate_id and job_kind='PREPARE' and status='DONE'))
         or (new.run_worker_id is not null and not exists(select 1 from private.bpay_next_run_worker where id=new.run_worker_id
           and run_id=new.run_id and candidate_id=new.candidate_id and status=new.original_worker_status
           and preparation_revision=new.preparation_revision and case_selection_revision=new.selection_revision
           and captured_line_count=new.expected_work_count and active_case_hold_count=new.expected_case_hold_count
           and review_issue_code is not distinct from new.original_review_issue_code and review_issue_work_id is not distinct from new.original_review_issue_work_id)) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_INITIAL_INVALID';end if;
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_preparation_expiry_request' then
    if old.status='EXPIRED' or (pg_catalog.to_jsonb(new)-'enrolled_candidate_count'-'completed_candidate_count'-'status'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'enrolled_candidate_count'-'completed_candidate_count'-'status'-'finished_at_utc')
       or new.enrolled_candidate_count not between old.enrolled_candidate_count and old.enrolled_candidate_count+1
       or new.completed_candidate_count not between old.completed_candidate_count and old.completed_candidate_count+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REQUEST_TRANSITION_INVALID';end if;
  else
    if old.status='EXPIRED' or (pg_catalog.to_jsonb(new)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'started_at_utc'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'started_at_utc'-'finished_at_utc')
       or new.checkpoint<>old.checkpoint+1 or new.released_work_count<old.released_work_count or new.released_case_hold_count<old.released_case_hold_count
       or (old.work_cursor is not null and (new.work_cursor is null or new.work_cursor<old.work_cursor))
       or (old.case_hold_cursor is not null and (new.case_hold_cursor is null or new.case_hold_cursor<old.case_hold_cursor))
       or (old.started_at_utc is not null and new.started_at_utc is distinct from old.started_at_utc)
       or (old.stage='NEW' and new.stage not in ('WORK','CASE')) or (old.stage='WORK' and new.stage not in ('WORK','CASE'))
       or (old.stage='CASE' and new.stage not in ('CASE','FINAL')) or (old.stage='FINAL' and new.stage<>'COMPLETE') then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_TRANSITION_INVALID';end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_append_selection_page_v1(uuid,bigint,jsonb) owner to postgres;
alter function private.bpay_next_append_work_choices_v1(uuid,bigint,jsonb) owner to postgres;
alter function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) owner to postgres;
alter function private.bpay_next_append_component_selection_page_v1(uuid,uuid,uuid,bigint,uuid[]) owner to postgres;
alter function private.bpay_next_seal_component_selection_v1(uuid,uuid,uuid,bigint,bigint) owner to postgres;
alter function private.bpay_next_append_case_selection_page_v1(uuid,uuid,bigint,bigint,uuid,uuid[],boolean[]) owner to postgres;
alter function private.bpay_next_seal_case_selection_v1(uuid,uuid,bigint,bigint,bigint) owner to postgres;
alter function private.bpay_next_enqueue_preparation_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_enroll_one_v1() owner to postgres;
alter function private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamptz,timestamptz) owner to postgres;
alter function private.bpay_next_enroll_preparation_expiry_one_v1(uuid) owner to postgres;
alter function private.bpay_next_preparation_expiry_guard_v1() owner to postgres;

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
    if v_key='descriptor' then
      if v_type is distinct from 'object' or octet_length((p_args->v_key)::text)>32768 then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    elsif v_key in ('choices','approved_line_ids','component_ids','selected','segment_keys') then
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

create or replace function public.bpay_next_preparation_intake_v1(p_actor_user_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_run uuid;v_work uuid;v_revision uuid;v_candidate uuid;v_result jsonb;v_output jsonb;
  v_ids uuid[];v_selected boolean[];v_entry jsonb;v_header private.bpay_next_pay_run%rowtype;
begin
  if p_action='CREATE' and p_args ? 'descriptor' then
    -- Authenticate before admission; preserve Source -> module lock order.
    if coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users u
        where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin' for share) then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    perform private.bpay_next_preparation_args_v1(p_args,array['run_id','command_id','descriptor']);
    if not exists(select 1 from private.bpay_next_pay_run where id=(p_args->>'run_id')::uuid) then
      begin perform private.weekly_source_pay_query_admit_v2();
      exception when lock_not_available then null;end;end if;
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_action
    when 'CREATE' then case when p_args ? 'descriptor' then array['run_id','command_id','descriptor'] when p_args ? 'pay_date' then array['run_id','pay_date'] else array['run_id'] end
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
    if p_args ? 'descriptor' then
      v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,null,p_args->'descriptor',(p_args->>'command_id')::uuid);
    else
      v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,(p_args->>'pay_date')::date);
    end if;
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'status',v_result->>'status',
      'selection_state',v_result->>'selection_state','selection_count',v_header.selection_count::text,
      'selected_candidate_count',v_header.selected_candidate_count::text,'official_pay_date',v_header.pay_date::text);
    if p_args ? 'descriptor' then
      v_output:=v_output||jsonb_build_object('command_id',v_result->>'command_id',
        'agency_sequence',v_result->'agency_sequence','choice_work_count',v_result->>'choice_work_count',
        'choice_case_count',v_result->>'choice_case_count','empty',v_result->'empty','replay',v_result->'replay');
    end if;
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
alter function private.bpay_next_preparation_args_v1(jsonb,text[]) owner to postgres;
alter function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) to service_role;

notify pgrst, 'reload schema';
commit;
