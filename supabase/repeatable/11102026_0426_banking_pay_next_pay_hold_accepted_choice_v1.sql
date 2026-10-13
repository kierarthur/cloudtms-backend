-- WB40 narrow successor; complete latest bodies, unrelated branches retained.
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
  if tg_table_name='bpay_next_work_choice' and tg_op='INSERT' then
    -- Original choice construction only, under the exact Work lock. This also
    -- covers retained manifest callers; no delayed capture can refill it.
    perform 1 from private.bpay_next_work where id=new.work_id for share;
    select w.pay_hold_control_version,w.pay_hold_changed_at_utc,t.timesheet_id,tf.id,
      coalesce(tf.pay_on_hold,false),case when tf.pay_on_hold then tf.pay_on_hold_reason end
      into new.pay_hold_control_version,new.pay_hold_changed_at_utc,new.pay_hold_physical_timesheet_id,
        new.pay_hold_financial_id,new.pay_hold,new.pay_hold_reason
      from private.bpay_next_work w left join public.timesheets t on t.booking_id=w.booking_id and t.is_current
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current where w.id=new.work_id;
  end if;
  if tg_op='UPDATE' then
    if tg_table_name='bpay_next_work_choice' and
      (new.pay_hold_control_version,new.pay_hold_changed_at_utc,new.pay_hold_physical_timesheet_id,
        new.pay_hold_financial_id,new.pay_hold,new.pay_hold_reason) is distinct from
      (old.pay_hold_control_version,old.pay_hold_changed_at_utc,old.pay_hold_physical_timesheet_id,
        old.pay_hold_financial_id,old.pay_hold,old.pay_hold_reason) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';end if;
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
 if v_work.week_ending_date-6>date_trunc('week',v_run.snooze_business_date::timestamp)::date then
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
   if v_gate->'blocked'='true'::jsonb then return 'SOURCE_PAY_QUERY_OPEN';end if;
  exception when others then return 'SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end;
 end if;
 return null;
end
$function$;

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
 v_memory boolean:=p_descriptor ? 'memory_baseline_run_id';v_baseline uuid;v_page_no bigint;v_lines uuid[];v_subset record;
 v_mode text;v_selected boolean;v_count bigint;v_code text;v_week_start date;v_work_count bigint:=0;
begin
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
   group by l.work_id,l.candidate_id,coalesce((ov.value->>'expected_revision_id')::uuid,l.revision_id)
   having bool_or(l.selected)),
  work_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,work_id,expected_revision_id,selection_mode,selected,member_no)
   select p_run_id,'WORK',w.work_id,w.candidate_id,w.work_id,w.expected_revision_id,w.selection_mode,true,
    row_number() over(order by (w.selection_mode='SUBSET') desc,w.work_id) from works w returning candidate_id),
  case_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,case_component_id,selected,member_no)
   select p_run_id,'CASE_COMPONENT',l.case_component_id,l.candidate_id,l.case_component_id,l.selected,
    row_number() over(order by l.candidate_id,l.case_component_id) from choices l
   where l.case_component_id is not null and
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
 end if;
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
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc)
      values(p_run_id,v_member.work_id,v_member.expected_revision_id,v_member.physical_timesheet_id,v_gate,transaction_timestamp());
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
alter function private.bpay_next_work_choice_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_choice_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_append_work_skip_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_append_work_skip_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid) owner to postgres;
revoke all on function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
