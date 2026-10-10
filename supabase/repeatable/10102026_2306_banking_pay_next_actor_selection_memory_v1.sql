-- WB22 two-table native choice memory; existing intake/reader/segment owners.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_selection_memory_leaves_v1(p_scope jsonb)
returns table(leaf_key text,candidate_id uuid,work_id uuid,revision_id uuid,approved_line_id uuid,case_component_id uuid,direction text,basis jsonb)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select case when i.work_id is not null then 'WORK:'||i.work_id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key
  else 'CASE:'||i.case_component_id::text end,i.candidate_id,i.work_id,i.revision_id,i.approved_line_id,i.case_component_id,i.direction,
 jsonb_build_object('revision_id',i.revision_id,'approved_line_id',i.approved_line_id,
  'approved_segment_id',i.approved_segment_id,'component_key',i.component_key,'segment_key',i.segment_key,
  'member_kind',i.member_kind,'source_pay_channel',i.source_pay_channel,'target_pay_channel',i.target_pay_channel,
  'direction',i.direction,'payroll_stage',i.payroll_stage,'approved_quantity',i.approved_quantity,'unit_label',i.unit_label,
  'approved_source_ex_vat',i.approved_source_ex_vat,'realised_source_ex_vat',i.realised_source_ex_vat,
  'active_reserved_source_ex_vat',i.active_reserved_source_ex_vat,'included_source_ex_vat',i.included_source_ex_vat,
  'deduction_balance_source_ex_vat',i.deduction_balance_source_ex_vat,'snooze_id',i.snooze_id,
  'snooze_version',i.snooze_version,'snooze_until_date',i.snooze_until_date,'pay_hold',
  exists(select 1 from private.bpay_next_work hold_work join public.timesheets hold_sheet on hold_sheet.booking_id=hold_work.booking_id and hold_sheet.is_current=true
   join public.timesheets_financials hold_financial on hold_financial.timesheet_id=hold_sheet.timesheet_id and hold_financial.is_current=true
   where hold_work.id=i.work_id and hold_financial.pay_on_hold is true),
  'query_state',i.query_state,'query_qualified',i.query_qualified,'rule_id',i.rule_id,
  'case_component_revision',cc.component_revision,'case_rule_revision',cr.rule_revision,
  'case_threshold',cr.minimum_earnings_threshold,'case_floor',cr.take_home_floor_override,
  'case_weekly_due',cr.weekly_due_source_ex_vat,'case_next_due',cr.next_due_monday,
  'week_ending_date',i.week_ending_date,'work_date',i.work_date,
  'target_umbrella_id',candidate.umbrella_id,'target_umbrella_vat_chargeable',umbrella.vat_chargeable,
  'min_take_home_wtd',candidate.min_take_home_wtd)
 from private.bpay_next_workbench_item i join private.bpay_next_workbench_group g
  on g.group_key=i.group_key and g.candidate_id=i.candidate_id
 join public.candidates candidate on candidate.id=i.candidate_id
 left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id
 left join private.bpay_next_case_component cc on cc.id=i.case_component_id
 left join private.bpay_next_case_rule cr on cr.id=cc.rule_id
 where i.section='READY' and i.selectable and i.amount_known and not i.is_selected
  and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
  and g.progress_state='CURRENT' and i.progress_state='CURRENT'
  and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
  and ((i.work_id is not null and i.approved_line_id is not null) or i.case_component_id is not null)
  and (p_scope->>'candidate_id' is null or i.candidate_id=(p_scope->>'candidate_id')::uuid)
  and (p_scope->>'client_id' is null or i.client_id=(p_scope->>'client_id')::uuid)
  and (p_scope->>'channel'='ALL' or i.target_pay_channel=p_scope->>'channel')
;
$function$;

create or replace function private.bpay_next_selection_memory_resolve_v1(p_namespace jsonb,p_reference jsonb)
returns uuid language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_run uuid;v_scope jsonb:=p_namespace->'scope';v_ns jsonb:=p_namespace;
begin
 if p_reference is not null and p_reference<>'null'::jsonb then
  if jsonb_typeof(p_reference)<>'object' or not(p_reference ?& array['run_id','scope'])
   or p_reference-array['run_id','scope']<>'{}'::jsonb or jsonb_typeof(p_reference->'scope')<>'object'
   or not(p_reference->'scope' ?& array['candidate_id','client_id','channel'])
   or (p_reference->'scope')-array['candidate_id','client_id','channel']<>'{}'::jsonb then
   raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_scope:=p_reference->'scope';
  if jsonb_typeof(v_scope->'channel') is distinct from 'string' or v_scope->>'channel' not in ('ALL','PAYE','UMBRELLA')
   or (v_scope->>'candidate_id' is not null and (jsonb_typeof(v_scope->'candidate_id')<>'string' or v_scope->>'candidate_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
   or (v_scope->>'client_id' is not null and (jsonb_typeof(v_scope->'client_id')<>'string' or v_scope->>'client_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')) then
   raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if v_scope->'client_id' is distinct from p_namespace->'scope'->'client_id'
   or v_scope->'channel' is distinct from p_namespace->'scope'->'channel'
   or (v_scope->>'candidate_id' is not null and v_scope->'candidate_id' is distinct from p_namespace->'scope'->'candidate_id') then
   raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_reference->'run_id'='null'::jsonb then return null;end if;
  if jsonb_typeof(p_reference->'run_id')<>'string' or p_reference->>'run_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_run:=(p_reference->>'run_id')::uuid;
 end if;
 select m.run_id into v_run from private.bpay_next_selection_mode_memory m
 where m.row_kind='HEADER' and m.acknowledged_at_utc is not null
  and m.agency_id=(v_ns->>'agency_id')::uuid and m.owner_epoch=(v_ns->>'owner_epoch')::bigint
  and m.actor_user_id=(v_ns->>'actor_user_id')::uuid and m.official_pay_date=(v_ns->>'official_pay_date')::date
  and m.scope=v_scope and (v_run is null or m.run_id=v_run)
 order by m.agency_sequence desc,m.run_id limit 1;
 if not found and p_reference is not null and p_reference<>'null'::jsonb then
  raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
 return v_run;
end
$function$;

create or replace function private.bpay_next_selection_memory_choices_v1(p_scope jsonb,p_baseline uuid,p_intent jsonb)
returns table(leaf_key text,candidate_id uuid,work_id uuid,revision_id uuid,approved_line_id uuid,case_component_id uuid,direction text,basis jsonb,selected boolean,new_changed boolean)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 with current_leaves as materialized(select * from private.bpay_next_selection_memory_leaves_v1(p_scope)),
 units as (select l.candidate_id,l.work_id,l.revision_id,l.approved_line_id,l.case_component_id,l.direction,
  case when l.work_id is not null then 'WORK:'||l.work_id::text||':'||(l.basis->>'component_key') else 'CASE:'||l.case_component_id::text end unit_key,
  bool_or(b.basis is distinct from l.basis) or
   exists(select 1 from private.bpay_next_selection_mode_memory prior where prior.run_id=p_baseline and prior.row_kind='LEAF'
    and prior.candidate_id=l.candidate_id and (prior.work_id=l.work_id and prior.approved_line_id=l.approved_line_id or prior.case_component_id=l.case_component_id)
    and not exists(select 1 from current_leaves now_leaf where now_leaf.leaf_key=prior.leaf_key and now_leaf.basis=prior.basis)) changed
 from current_leaves l left join private.bpay_next_selection_mode_memory b on b.run_id=p_baseline and b.row_kind='LEAF' and b.leaf_key=l.leaf_key
 group by l.candidate_id,l.work_id,l.revision_id,l.approved_line_id,l.case_component_id,l.direction,l.basis->>'component_key'),
 defaults as (
 select l.*,'{}'::jsonb basis,
  case when p_baseline is null then coalesce(p_intent->>'mode','ALL_MATCHING')='ALL_MATCHING'
   when l.changed then true
   when coalesce(p_intent->>'mode',h.mode)='NONE' then false
   else not exists(select 1 from private.bpay_next_selection_exclusion_memory e
     join private.bpay_next_selection_mode_memory original on original.run_id=e.basis_ref_run_id
       and original.row_kind='LEAF' and original.candidate_id=l.candidate_id
       and (original.work_id=l.work_id and original.approved_line_id=l.approved_line_id or original.case_component_id=l.case_component_id)
     where e.run_id=p_baseline and not e.selected and e.candidate_id=l.candidate_id
      and not exists(select 1 from current_leaves current_leaf where current_leaf.candidate_id=l.candidate_id
       and (current_leaf.approved_line_id=l.approved_line_id or current_leaf.case_component_id=l.case_component_id)
       and not exists(select 1 from private.bpay_next_selection_mode_memory original_leaf where original_leaf.run_id=e.basis_ref_run_id
        and original_leaf.row_kind='LEAF' and original_leaf.leaf_key=current_leaf.leaf_key and original_leaf.basis=current_leaf.basis))
      and not exists(select 1 from private.bpay_next_selection_mode_memory original_leaf where original_leaf.run_id=e.basis_ref_run_id and original_leaf.row_kind='LEAF'
       and (original_leaf.approved_line_id=l.approved_line_id or original_leaf.case_component_id=l.case_component_id)
       and not exists(select 1 from current_leaves current_leaf where current_leaf.leaf_key=original_leaf.leaf_key and current_leaf.basis=original_leaf.basis))
      and (e.intent_kind='candidates' or e.intent_kind='works' and e.work_id=l.work_id
       or e.intent_kind='components' and e.approved_line_id=original.approved_line_id
       or e.intent_kind='cases' and e.case_component_id=l.case_component_id)) end default_selected
 from units l left join private.bpay_next_selection_mode_memory h on h.run_id=p_baseline and h.row_kind='HEADER'
 ), chosen as (
 select l.*,candidate.value candidate_choice,work.value work_choice,component.value component_choice,case_choice.value case_choice
 from defaults l
 left join lateral(select value from jsonb_array_elements(coalesce(p_intent->'candidates','[]'::jsonb)) where value->>'candidate_id'=l.candidate_id::text) candidate on true
 left join lateral(select value from jsonb_array_elements(coalesce(p_intent->'works','[]'::jsonb)) where value->>'work_id'=l.work_id::text) work on true
 left join lateral(select value from jsonb_array_elements(coalesce(p_intent->'components','[]'::jsonb)) where value->>'approved_line_id'=l.approved_line_id::text) component on true
 left join lateral(select value from jsonb_array_elements(coalesce(p_intent->'cases','[]'::jsonb)) where value->>'case_component_id'=l.case_component_id::text) case_choice on true
 ) select l.unit_key,l.candidate_id,l.work_id,l.revision_id,l.approved_line_id,l.case_component_id,l.direction,l.basis,
 case when l.candidate_choice->'selected'='false'::jsonb or l.work_choice->'selected'='false'::jsonb then false
  when l.case_component_id is not null then coalesce((l.case_choice->>'selected')::boolean,(l.candidate_choice->>'selected')::boolean,l.default_selected)
  when l.work_choice->>'mode'='SUBSET' and l.work_choice->'selected'='true'::jsonb then coalesce((l.component_choice->>'selected')::boolean,false)
  else coalesce((l.component_choice->>'selected')::boolean,
   case when l.work_choice->>'mode'='ALL' then (l.work_choice->>'selected')::boolean end,
   (l.candidate_choice->>'selected')::boolean,l.default_selected) end,
 p_baseline is not null and l.changed from chosen l;
$function$;

create or replace function private.bpay_next_selection_memory_capture_v1(p_run uuid,p_descriptor jsonb)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_ns jsonb:=p_descriptor->'namespace';v_baseline uuid:=(p_descriptor->>'memory_baseline_run_id')::uuid;v_kind text;v_row jsonb;v_id uuid;
begin
 insert into private.bpay_next_selection_mode_memory(run_id,row_kind,leaf_key,agency_id,owner_epoch,actor_user_id,official_pay_date,scope,mode,intent_revision,baseline_run_id,basis,raw_intent,accepted_context)
 values(p_run,'HEADER','HEADER',(v_ns->>'agency_id')::uuid,(v_ns->>'owner_epoch')::bigint,(v_ns->>'actor_user_id')::uuid,(v_ns->>'official_pay_date')::date,v_ns->'scope',p_descriptor->'intent'->>'mode',(p_descriptor->>'revision')::bigint,v_baseline,'{}',p_descriptor->'intent',p_descriptor-'intent');
 insert into private.bpay_next_selection_mode_memory(run_id,row_kind,leaf_key,agency_id,owner_epoch,actor_user_id,official_pay_date,scope,mode,intent_revision,baseline_run_id,candidate_id,work_id,approved_line_id,case_component_id,basis)
 select p_run,'LEAF',l.leaf_key,(v_ns->>'agency_id')::uuid,(v_ns->>'owner_epoch')::bigint,(v_ns->>'actor_user_id')::uuid,(v_ns->>'official_pay_date')::date,v_ns->'scope',p_descriptor->'intent'->>'mode',(p_descriptor->>'revision')::bigint,v_baseline,l.candidate_id,l.work_id,l.approved_line_id,l.case_component_id,l.basis
 from private.bpay_next_selection_memory_leaves_v1(v_ns->'scope') l;
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
 if exists(select 1 from jsonb_array_elements(v_intent->'components') c(value)
   where not exists(select 1 from jsonb_array_elements(v_intent->'works') w(value)
     where w.value->>'work_id'=c.value->>'work_id' and w.value->>'candidate_id'=c.value->>'candidate_id'
       and w.value->>'expected_revision_id'=c.value->>'expected_revision_id'
       and (v_memory or w.value->>'mode'='SUBSET' and w.value->'selected'='true'::jsonb))) then
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
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc)
      values(p_run_id,v_selected.work_id,v_selected.expected_revision_id,v_selected.physical_timesheet_id,
        private.weekly_source_pay_query_gate_v1(v_selected.physical_timesheet_id),pg_catalog.transaction_timestamp());
  end loop;
  end if;
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
alter function private.bpay_next_selection_memory_leaves_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_leaves_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_selection_memory_resolve_v1(jsonb,jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_resolve_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_selection_memory_capture_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_capture_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid) owner to postgres;
revoke all on function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_selection_memory_intent_valid_v1(p_intent jsonb)
returns boolean language plpgsql immutable security invoker set search_path=pg_catalog
as $function$
declare k text;r jsonb;keys text[];f text;
begin
 if p_intent is null or jsonb_typeof(p_intent)<>'object' or octet_length(p_intent::text)>32768
  or not(p_intent ?& array['mode','candidates','works','components','cases'])
  or p_intent-array['mode','candidates','works','components','cases']<>'{}'::jsonb
  or coalesce(p_intent->>'mode','') not in ('ALL_MATCHING','NONE') then return false;end if;
 foreach k in array array['candidates','works','components','cases'] loop
  if jsonb_typeof(p_intent->k)<>'array' or jsonb_array_length(p_intent->k)>1000 then return false;end if;
  keys:=case k when 'candidates' then array['candidate_id','selected']
   when 'works' then array['candidate_id','work_id','expected_revision_id','selected','mode']
   when 'components' then array['candidate_id','work_id','expected_revision_id','approved_line_id','selected']
   else array['candidate_id','case_component_id','selected'] end;
  for r in select value from jsonb_array_elements(p_intent->k) loop
   if jsonb_typeof(r)<>'object' or not(r ?& keys) or r-keys<>'{}'::jsonb or jsonb_typeof(r->'selected')<>'boolean' then return false;end if;
   foreach f in array keys loop
    if f in ('selected','mode') then continue;end if;
    if jsonb_typeof(r->f)<>'string' or r->>f!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then return false;end if;
   end loop;
   if k='works' and coalesce(r->>'mode','') not in ('ALL','SUBSET') then return false;end if;
  end loop;
  if exists(select 1 from jsonb_array_elements(p_intent->k) a(value) group by
    coalesce(a.value->>'case_component_id',a.value->>'approved_line_id',a.value->>'work_id',a.value->>'candidate_id') having count(*)>1) then return false;end if;
 end loop;
 return true;
end
$function$;

create or replace function private.bpay_next_selection_memory_row_v1(p_kind text,p_row jsonb,p_scope jsonb,p_baseline uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_scope jsonb:=p_scope;v_work uuid;v_case uuid;v_component uuid;v_total bigint;v_selected bigint;v_changed bigint;v_basis jsonb;
begin
 v_scope:=jsonb_set(v_scope,'{candidate_id}',p_row->'candidate_id');
 if p_kind='CANDIDATE_GROUPS' then
  if p_row->>'presentation_kind'='WORK' or p_row->>'group_kind'='WORK' then
   v_work:=coalesce(p_row->>'presentation_id',p_row->>'work_id',split_part(p_row->>'group_key',':',2))::uuid;
  elsif p_row->>'presentation_kind'='ROW' or p_row->>'group_kind'='ROW' then
   v_component:=coalesce(p_row->>'case_component_id',p_row->>'presentation_id',split_part(p_row->>'group_key',':',2))::uuid;
  else v_case:=coalesce(p_row->>'case_id',p_row->>'presentation_id',split_part(p_row->>'group_key',':',2))::uuid;end if;
 elsif p_kind='GROUP_LINES' then
  v_work:=(p_row->>'work_id')::uuid;v_component:=(p_row->>'case_component_id')::uuid;
  if v_work is null and v_component is null then return p_row||jsonb_build_object('default_selected',false,'selection_eligible',false,
   'new_changed',false,'choice_member_count','0','choice_selected_count','0','selection_basis',null);end if;
 end if;
 select count(*),count(*) filter(where l.selected),count(*) filter(where l.new_changed)
 into v_total,v_selected,v_changed
 from private.bpay_next_selection_memory_choices_v1(v_scope,p_baseline,null) l
 where (v_work is null or l.work_id=v_work) and (v_component is null or l.case_component_id=v_component)
  and (v_case is null or exists(select 1 from private.bpay_next_case_component cc where cc.id=l.case_component_id and cc.case_id=v_case))
  and (p_kind<>'GROUP_LINES' or l.work_id is null or l.approved_line_id=(p_row->>'approved_line_id')::uuid);
 -- Fixed native version/position/control operands for local Keep. Detailed
 -- per-segment equality remains in the private baseline and set comparison.
 if p_kind='CANDIDATES' then
  select jsonb_build_object('kind','PARENT','financial_view_revision',c.financial_view_revision::text) into v_basis
   from private.bpay_next_worker_control c where c.candidate_id=(p_row->>'candidate_id')::uuid;
 elsif p_kind='CANDIDATE_GROUPS' and v_work is not null then
  select jsonb_build_object('kind','WORK','work_id',w.id,'revision_id',w.current_revision_id,'applied_revision_id',w.applied_revision_id,
   'position_count',(select count(*)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'position_updated_at_utc',(select max(pos.updated_at_utc) from private.bpay_next_position pos where pos.work_id=w.id),
   'approved_source_ex_vat',(select sum(pos.approved_source_ex_vat)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'realised_source_ex_vat',(select sum(pos.realised_source_ex_vat)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'held_source_ex_vat',(select sum(pos.held_source_ex_vat)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'source_book_revision',(select coalesce(sum(pos.source_book_revision),0)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'target_pay_channel',candidate.pay_method,'target_umbrella_id',candidate.umbrella_id,
   'target_umbrella_vat_chargeable',umbrella.vat_chargeable,'min_take_home_wtd',candidate.min_take_home_wtd::text,
   'control_sequence',coalesce((select max(sn.control_sequence) from private.bpay_next_snooze sn where sn.work_id=w.id),0)::text) into v_basis
  from private.bpay_next_work w join public.candidates candidate on candidate.id=w.candidate_id
  left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id where w.id=v_work;
 elsif v_component is not null then
  select jsonb_build_object('kind','CASE','case_component_id',cc.id,'component_revision',cc.component_revision::text,
   'rule_id',cc.rule_id,'approved_source_ex_vat',cc.approved_source_ex_vat::text,
   'funded_source_ex_vat',cc.funded_source_ex_vat::text,'recovered_source_ex_vat',cc.recovered_source_ex_vat::text,
   'paid_credit_source_ex_vat',cc.paid_credit_source_ex_vat::text,'written_off_source_ex_vat',cc.written_off_source_ex_vat::text,
   'active_payout_source_ex_vat',cc.active_payout_source_ex_vat::text,'active_recovery_source_ex_vat',cc.active_recovery_source_ex_vat::text,
   'resolution_state',cc.resolution_state,'resolved_channel',cc.target_pay_channel,
   'target_ex_vat',cc.target_ex_vat::text,'target_vat',cc.target_vat::text,'valuation_policy_id',cc.valuation_policy_id,
   'valuation_window_id',cc.valuation_window_id,'target_pay_channel',candidate.pay_method,'target_umbrella_id',candidate.umbrella_id,
   'target_umbrella_vat_chargeable',umbrella.vat_chargeable,'min_take_home_wtd',candidate.min_take_home_wtd::text,
   'control_sequence',coalesce((select max(sn.control_sequence) from private.bpay_next_snooze sn where sn.case_component_id=cc.id),0)::text) into v_basis
  from private.bpay_next_case_component cc join public.candidates candidate on candidate.id=cc.candidate_id
  left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id where cc.id=v_component;
 elsif v_work is not null then
  select jsonb_build_object('kind','COMPONENT','work_id',w.id,'revision_id',w.current_revision_id,
   'approved_line_id',l.id,'component_key',l.component_key,'applied_revision_id',position.applied_revision_id,
   'approved_source_ex_vat',position.approved_source_ex_vat::text,'realised_source_ex_vat',position.realised_source_ex_vat::text,
   'held_source_ex_vat',position.held_source_ex_vat::text,'realised_target_ex_vat',position.realised_target_ex_vat::text,
   'realised_target_vat',position.realised_target_vat::text,'held_target_ex_vat',position.held_target_ex_vat::text,
   'held_target_vat',position.held_target_vat::text,'target_pay_channel',candidate.pay_method,
   'target_umbrella_id',candidate.umbrella_id,'target_umbrella_vat_chargeable',umbrella.vat_chargeable,
   'min_take_home_wtd',candidate.min_take_home_wtd::text,
   'control_sequence',coalesce((select max(sn.control_sequence) from private.bpay_next_snooze sn
     where sn.work_id=w.id and (sn.component_key is null or sn.component_key=l.component_key)),0)::text) into v_basis
  from private.bpay_next_work w join private.bpay_next_approved_line l on l.revision_id=w.current_revision_id and l.id=(p_row->>'approved_line_id')::uuid
  join public.candidates candidate on candidate.id=w.candidate_id left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id
  left join private.bpay_next_position position on position.work_id=w.id and position.component_key=l.component_key where w.id=v_work;
 end if;
 if octet_length(coalesce(v_basis,'null')::text)>32768 then
  raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
 return p_row||jsonb_build_object('default_selected',v_total>0 and v_selected=v_total,
  'selection_eligible',v_total>0 and (p_kind<>'CANDIDATES' or (p_row->>'payable_count')::bigint>0),
  'new_changed',v_changed>0,'choice_member_count',v_total::text,'choice_selected_count',v_selected::text,
  'selection_basis',v_basis);
end
$function$;

create or replace function private.bpay_next_workbench_kind_page_v1(
  p_kind text,p_scope jsonb,p_after jsonb,p_limit integer
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare v_sql text;v_where text;v_order text;v_tuple text;v_seek text;v_key text;v_projection text;
  v_row record;v_rows jsonb:='[]'::jsonb;v_last jsonb;v_n integer:=0;v_complete boolean:=true;v_member boolean;v_affected bigint;v_anchor_page boolean:=false;
  v_client_scope text:=coalesce(p_scope->>'client_id','ALL');v_channel text:=p_scope->>'channel';
  v_section text:=p_scope->>'section';v_candidate uuid:=(p_scope->>'candidate_id')::uuid;
  v_client uuid:=(p_scope->>'client_id')::uuid;v_search text:=p_scope->>'search';
  v_group text:=p_scope->>'group_key';v_item uuid:=(p_scope->>'item_id')::uuid;v_reason text:=p_scope->>'reason_code';
  v_sort text:=p_scope->>'sort';v_desc boolean;v_op text;
  v_candidate_count bigint;v_members bigint;v_selected_candidates bigint;v_selected_members bigint;
  v_count_selection jsonb:=coalesce(p_scope->'count_selection','null'::jsonb);
  v_baseline uuid:=(p_scope->'_selection_memory'->>'run_id')::uuid;v_memory jsonb:=p_scope->'_selection_memory';
  v_mode text;v_intent jsonb;v_value jsonb;v_remembered jsonb;

begin
  if p_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS') or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_desc:=v_sort like '%_DESC';v_op:=case when v_desc then '<' else '>' end;
  if p_kind='EXPORT_ROWS' then
    -- WB18: native list sort only, exact bounded item seed before immutable facts.
    -- Export controls supply the exact normalized filter, not free text.
    if v_search<>'' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_sort in ('C_NAME_ASC','C_NAME_DESC') then
      v_tuple:='(coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C",(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null),coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,'''') collate "C",i.candidate_id,i.id)';v_seek:='(($10->>0) collate "C",($10->>1)::boolean,($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C" @,(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null) @,coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,'''') collate "C" @,i.candidate_id @,i.id @';v_key:='jsonb_build_array(coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,''''),(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null),coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,''''),i.candidate_id,i.id)';
    elsif v_sort in ('C_AMOUNT_ASC','C_AMOUNT_DESC') then
      v_tuple:='((coalesce(cs.unknown_amount_count,0)>0),(case when coalesce(cs.unknown_amount_count,0)=0 then coalesce(cs.amount,0) else 0 end),i.candidate_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2)::uuid,($10->>3)::uuid)';v_order:='(coalesce(cs.unknown_amount_count,0)>0) @,(case when coalesce(cs.unknown_amount_count,0)=0 then coalesce(cs.amount,0) else 0 end) @,i.candidate_id @,i.id @';v_key:='jsonb_build_array((coalesce(cs.unknown_amount_count,0)>0),(case when coalesce(cs.unknown_amount_count,0)=0 then coalesce(cs.amount,0) else 0 end)::text,i.candidate_id,i.id)';
    elsif v_sort in ('C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC') then
      v_tuple:='((coalesce(cs.deduction_item_count,0)>0),coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C",(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null),coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,'''') collate "C",i.candidate_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2)::boolean,($10->>3) collate "C",($10->>4)::uuid,($10->>5)::uuid)';v_order:='(coalesce(cs.deduction_item_count,0)>0) @,coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C" @,(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null) @,coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,'''') collate "C" @,i.candidate_id @,i.id @';v_key:='jsonb_build_array((coalesce(cs.deduction_item_count,0)>0),coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,''''),(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null),coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,''''),i.candidate_id,i.id)';
    elsif v_sort in ('C_STATUS_ASC','C_STATUS_DESC') then
      v_tuple:='((coalesce(cs.payable_count,0)=0),(coalesce(cs.bank_warning_code,m.bank_warning_code) is null),coalesce(cs.bank_warning_code,m.bank_warning_code,'''') collate "C",coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C",i.candidate_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1)::boolean,($10->>2) collate "C",($10->>3) collate "C",($10->>4)::uuid,($10->>5)::uuid)';v_order:='(coalesce(cs.payable_count,0)=0) @,(coalesce(cs.bank_warning_code,m.bank_warning_code) is null) @,coalesce(cs.bank_warning_code,m.bank_warning_code,'''') collate "C" @,coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C" @,i.candidate_id @,i.id @';v_key:='jsonb_build_array((coalesce(cs.payable_count,0)=0),(coalesce(cs.bank_warning_code,m.bank_warning_code) is null),coalesce(cs.bank_warning_code,m.bank_warning_code,''''),coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,''''),i.candidate_id,i.id)';
    elsif v_sort in ('G_PAYMENT_ASC','G_PAYMENT_DESC') then
      v_tuple:='((gd.week_ending_date is null),coalesce(gd.week_ending_date,date ''0001-01-01''),gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1)::date,($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='(gd.week_ending_date is null) @,coalesce(gd.week_ending_date,date ''0001-01-01'') @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array((gd.week_ending_date is null),coalesce(gd.week_ending_date,date ''0001-01-01'')::text,gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_CLIENT_ASC','G_CLIENT_DESC') then
      v_tuple:='((gd.client_display_name is null),coalesce(gd.client_display_name,'''') collate "C",gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='(gd.client_display_name is null) @,coalesce(gd.client_display_name,'''') collate "C" @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array((gd.client_display_name is null),coalesce(gd.client_display_name,''''),gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_CHANNEL_ASC','G_CHANNEL_DESC') then
      v_tuple:='((gd.target_pay_channel is null),coalesce(gd.target_pay_channel,'''') collate "C",gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='(gd.target_pay_channel is null) @,coalesce(gd.target_pay_channel,'''') collate "C" @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array((gd.target_pay_channel is null),coalesce(gd.target_pay_channel,''''),gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_AMOUNT_ASC','G_AMOUNT_DESC') then
      v_tuple:='((gd.group_amount is null),coalesce(gd.group_amount,0),gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='(gd.group_amount is null) @,coalesce(gd.group_amount,0) @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array((gd.group_amount is null),coalesce(gd.group_amount,0)::text,gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC') then
      v_tuple:='(gd.has_deduction,gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2)::uuid,($10->>3)::uuid)';v_order:='gd.has_deduction @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array(gd.has_deduction,gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_REASON_ASC','G_REASON_DESC') then
      v_tuple:='(gd.reason_priority,gd.reason_code collate "C",gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::smallint,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='gd.reason_priority @,gd.reason_code collate "C" @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array(gd.reason_priority,gd.reason_code,gd.presentation_kind,gd.presentation_id,i.id)';
    else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    v_order:=replace(v_order,'@',case when v_desc then 'desc' else 'asc' end);
    v_sql:=' from private.bpay_next_workbench_item i
      join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
      join private.bpay_next_workbench_group_display gd on gd.parent_group_key=i.group_key and gd.candidate_id=i.candidate_id
        and gd.section=''ALL'' and (gd.presentation_kind<>''ROW'' or gd.case_component_id=i.case_component_id)
      left join private.bpay_next_workbench_candidate_summary cs on cs.candidate_id=i.candidate_id
        and cs.client_scope=$1 and cs.pay_channel=$2 and cs.section=''READY''
      left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=i.candidate_id where ';
    v_where:='($4 is null or i.candidate_id=$4) and ($5 is null or i.client_id=$5)
      and ($2=''ALL'' or i.target_pay_channel=$2) and ($3=''ALL'' or i.section=$3)';
    if p_after is not null then
      execute 'select exists(select 1'||v_sql||v_where||' and '||v_tuple||'='||v_seek||')' into v_member
        using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
      if not v_member then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
      v_where:=v_where||' and '||v_tuple||v_op||v_seek;
    end if;
    v_sql:='with seed as materialized(select i.id,'||v_key||' row_key,gd.payment_label,
        (coalesce(cs.payable_count,0)>0) candidate_has_pay'||v_sql||v_where||' order by '||v_order||' limit $11)
      select '||'pg_catalog.jsonb_build_object(''item_id'',i.id,
      ''group_key'',i.group_key,
      ''candidate_id'',i.candidate_id,
      ''work_id'',i.work_id,
      ''case_id'',i.case_id,
      ''case_component_id'',i.case_component_id,
      ''revision_id'',i.revision_id,
      ''client_id'',i.client_id,
      ''component_key'',i.component_key,
      ''member_kind'',i.member_kind,
      ''member_key'',i.member_key,
      ''portion_kind'',i.portion_kind,
      ''candidate_display_name'',i.candidate_display_name,
      ''candidate_reference'',i.candidate_reference,
      ''client_display_name'',i.client_display_name,
      ''timesheet_reference'',i.timesheet_reference,
      ''work_date'',i.work_date,
      ''week_ending_date'',i.week_ending_date,
      ''component_kind'',i.component_kind,
      ''tax_treatment'',i.tax_treatment,
      ''source_pay_channel'',i.source_pay_channel,
      ''target_pay_channel'',i.target_pay_channel,
      ''direction'',i.direction,
      ''payroll_stage'',i.payroll_stage,
      ''approved_quantity'',(i.approved_quantity)::text,
      ''unit_label'',i.unit_label,
      ''nominal_source_ex_vat'',(i.nominal_source_ex_vat)::text,
      ''approved_source_ex_vat'',(i.approved_source_ex_vat)::text,
      ''realised_source_ex_vat'',(i.realised_source_ex_vat)::text,
      ''active_reserved_source_ex_vat'',(i.active_reserved_source_ex_vat)::text,
      ''residual_source_ex_vat'',(i.residual_source_ex_vat)::text,
      ''included_source_ex_vat'',(i.included_source_ex_vat)::text,
      ''excluded_source_ex_vat'',(i.excluded_source_ex_vat)::text,
      ''deduction_balance_source_ex_vat'',(i.deduction_balance_source_ex_vat)::text,
      ''amount_known'',i.amount_known,
      ''section'',i.section,
      ''reason_code'',i.reason_code,
      ''selectable'',i.selectable,
      ''financial_view_revision'',(i.financial_view_revision)::text,
      ''projection_revision'',(i.projection_revision)::text)||pg_catalog.jsonb_build_object(''metadata_version'',(i.metadata_version)::text)||jsonb_build_object(
      ''export_key'',seed.row_key,''approved_line_id'',i.approved_line_id,
      ''physical_timesheet_id'',r.physical_timesheet_id,''job_title'',r.job_title,''band_label'',r.band_label,
      ''shift_start'',coalesce(sd.shift_start_local,to_char(sd.shift_start_at at time zone ''Europe/London'',''HH24:MI'')),
      ''shift_finish'',coalesce(sd.shift_end_local,to_char(sd.shift_end_at at time zone ''Europe/London'',''HH24:MI'')),
      ''detail_label'',coalesce(sd.detail_label,l.evidence_ref),
      ''candidate_has_pay'',seed.candidate_has_pay,
      ''export_amount'',(case when not i.amount_known then null
        when i.is_selected then case when i.selected_amount_known then i.selected_source_ex_vat end
        when i.portion_kind=''RESERVED'' then i.active_reserved_source_ex_vat
        when i.direction=''PAYMENT'' and i.section=''READY'' then i.included_source_ex_vat
        when i.direction=''PAYMENT'' and i.section=''WAITING'' and i.payroll_stage=''GROSS_ADD'' then i.gross_due_source_ex_vat
        else i.nominal_source_ex_vat end)::text,
      ''export_label'',seed.payment_label,
      ''choice_eligible'',i.selectable and i.section=''READY'' and not i.is_selected and g.complete and not g.dirty and g.progress_state=''CURRENT'')'||' as row_value,seed.row_key from seed
      join private.bpay_next_workbench_item i on i.id=seed.id
      join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
      left join private.bpay_next_work_revision r on r.id=i.revision_id and r.work_id=i.work_id
      left join private.bpay_next_approved_line l on l.id=i.approved_line_id and l.revision_id=i.revision_id and l.component_key=i.component_key
      left join private.bpay_next_approved_segment sg on sg.id=i.approved_segment_id and sg.revision_id=i.revision_id
        and sg.approved_line_id=i.approved_line_id and sg.component_key=i.component_key and sg.segment_key=i.segment_key
      left join private.bpay_next_shift_detail sd on sd.id=sg.shift_detail_id and sd.approved_line_id=i.approved_line_id
        and sd.segment_key=i.segment_key order by '||replace(v_seek,'$10','seed.row_key')||case when v_desc then ' desc' else ' asc' end;
    -- Only the <=101 seeded identities reach this typed final presentation sort.
    -- No unbounded ranking, JSON numeric-string ordering or factual scan.
    for v_row in execute v_sql using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1 loop
      if v_n=p_limit then v_complete:=false;exit;end if;
      v_value:=private.bpay_next_selection_memory_row_v1('GROUP_LINES',v_row.row_value,p_scope,v_baseline);
      v_rows:=v_rows||jsonb_build_array(v_value);v_last:=v_row.row_key;v_n:=v_n+1;
      if octet_length(v_rows::text)>110000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    end loop;
    return jsonb_build_object('rows',v_rows,'complete',v_complete,'last_key',v_last);
  end if;
  if p_kind='HEADER' then
    -- Exact natural summary lookup; missing CURRENT scope has no contributions.
    select pg_catalog.jsonb_build_object('candidate_count',coalesce(s.candidate_count,0)::text,'item_count',coalesce(s.item_count,0)::text,'contribution_count',coalesce(s.contribution_count,0)::text,'segment_count',coalesce(s.segment_count,0)::text,'payable_count',coalesce(s.payable_count,0)::text,'selected_count',coalesce(s.selected_count,0)::text,'snoozed_count',coalesce(s.snoozed_count,0)::text,'held_count',coalesce(s.held_count,0)::text,'queried_count',coalesce(s.queried_count,0)::text,'blocked_count',coalesce(s.blocked_count,0)::text,'deduction_item_count',coalesce(s.deduction_item_count,0)::text,'unknown_amount_count',coalesce(s.unknown_amount_count,0)::text,'unknown_selected_amount_count',coalesce(s.unknown_selected_amount_count,0)::text,'amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.amount,0)::text else null end,'waiting_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.waiting_amount,0)::text else null end,'ready_net_add_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.ready_net_add_amount,0)::text else null end,'waiting_net_add_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.waiting_net_add_amount,0)::text else null end,'gross_deduction_balance',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.gross_deduction_balance,0)::text else null end,'net_deduction_balance',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.net_deduction_balance,0)::text else null end,'approved_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.approved_amount,0)::text else null end,'paid_credit_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.paid_credit_amount,0)::text else null end,'reserved_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.reserved_amount,0)::text else null end,'excluded_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.excluded_amount,0)::text else null end,'recovery_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.recovery_amount,0)::text else null end,'selected_amount',case when coalesce(s.unknown_selected_amount_count,0)=0 then coalesce(s.selected_amount,0)::text else null end,'has_deduction',coalesce(s.deduction_item_count,0)>0) into v_rows
      from (values(1)) anchor(n) left join private.bpay_next_workbench_scope_summary s
        on s.client_scope=v_client_scope and s.pay_channel=v_channel and s.section=v_section;
    if v_candidate is not null then
      select pg_catalog.jsonb_build_object('candidate_count',case when s.candidate_id is null then '0' else '1' end,'item_count',coalesce(s.item_count,0)::text,'contribution_count',coalesce(s.contribution_count,0)::text,'segment_count',coalesce(s.segment_count,0)::text,'payable_count',coalesce(s.payable_count,0)::text,'selected_count',coalesce(s.selected_count,0)::text,'snoozed_count',coalesce(s.snoozed_count,0)::text,'held_count',coalesce(s.held_count,0)::text,'queried_count',coalesce(s.queried_count,0)::text,'blocked_count',coalesce(s.blocked_count,0)::text,'deduction_item_count',coalesce(s.deduction_item_count,0)::text,'unknown_amount_count',coalesce(s.unknown_amount_count,0)::text,'unknown_selected_amount_count',coalesce(s.unknown_selected_amount_count,0)::text,'amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.amount,0)::text else null end,'waiting_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.waiting_amount,0)::text else null end,'ready_net_add_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.ready_net_add_amount,0)::text else null end,'waiting_net_add_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.waiting_net_add_amount,0)::text else null end,'gross_deduction_balance',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.gross_deduction_balance,0)::text else null end,'net_deduction_balance',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.net_deduction_balance,0)::text else null end,'approved_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.approved_amount,0)::text else null end,'paid_credit_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.paid_credit_amount,0)::text else null end,'reserved_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.reserved_amount,0)::text else null end,'excluded_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.excluded_amount,0)::text else null end,'recovery_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.recovery_amount,0)::text else null end,'selected_amount',case when coalesce(s.unknown_selected_amount_count,0)=0 then coalesce(s.selected_amount,0)::text else null end,'has_deduction',coalesce(s.deduction_item_count,0)>0) into v_rows
        from (values(1)) anchor(n) left join private.bpay_next_workbench_candidate_summary s
          on s.candidate_id=v_candidate and s.client_scope=v_client_scope and s.pay_channel=v_channel and s.section=v_section;
    end if;
    -- Owner E6: deductions-only remains visible, but does not enter Create counts.
    -- Native ALL/client/channel summary universe; no work/publisher/money reads.
    if v_section='READY' then
      select count(*),coalesce(sum(s.payable_count+s.deduction_item_count),0)::bigint,
        count(*) filter(where v_count_selection<>'null'::jsonb and
          ((v_count_selection->>'mode'='ALL') <> ((v_count_selection->'candidate_ids') ? s.candidate_id::text))),
        coalesce(sum(s.payable_count+s.deduction_item_count) filter(where v_count_selection<>'null'::jsonb and
          ((v_count_selection->>'mode'='ALL') <> ((v_count_selection->'candidate_ids') ? s.candidate_id::text))),0)::bigint
      into v_candidate_count,v_members,v_selected_candidates,v_selected_members
      from private.bpay_next_workbench_candidate_summary s
      where s.section='READY' and s.client_scope=v_client_scope and s.pay_channel=v_channel
        and s.payable_count>0 and (v_candidate is null or s.candidate_id=v_candidate);
      v_rows:=v_rows||jsonb_build_object('candidate_count',v_candidate_count::text);
    end if;
    v_rows:=v_rows||jsonb_build_object('selectable_member_count',v_members::text,
      'selected_candidate_count',case when v_count_selection<>'null'::jsonb then v_selected_candidates::text else null end,
      'selected_member_count',case when v_count_selection<>'null'::jsonb then v_selected_members::text else null end);
    select m.mode,m.raw_intent into v_mode,v_remembered from private.bpay_next_selection_mode_memory m where m.run_id=v_baseline and m.row_kind='HEADER';
    if v_count_selection ? 'intent' then
     v_intent:=v_count_selection->'intent';
     with choices as materialized(select * from private.bpay_next_selection_memory_choices_v1(p_scope,v_baseline,v_intent)),
     payable as (select distinct candidate_id from choices where selected and direction='PAYMENT')
     select (select count(*) from payable),count(*) into v_selected_candidates,v_selected_members
      from private.bpay_next_workbench_item i join choices l on l.approved_line_id=i.approved_line_id or l.case_component_id=i.case_component_id
      where i.section='READY' and i.selectable and i.amount_known and not i.is_selected and i.portion_kind in ('AVAILABLE','ZERO')
       and l.selected and exists(select 1 from payable p where p.candidate_id=l.candidate_id);
     v_rows:=v_rows||jsonb_build_object('selected_candidate_count',v_selected_candidates::text,'selected_member_count',v_selected_members::text);
    end if;
    v_rows:=v_rows||jsonb_build_object('selection_memory',jsonb_build_object('run_id',v_baseline,'scope',v_memory->'scope'),
     'selection_mode',coalesce(v_mode,'ALL_MATCHING'),'remembered_intent',v_remembered);
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(v_rows),'complete',true,'last_key',null);
  elsif p_kind in ('CANDIDATES','PICK_CANDIDATES') then
    v_where:=$query$s.client_scope=$1 and s.pay_channel=$2 and s.section=$3$query$;
    if p_kind='CANDIDATES' and v_section='READY' then
      v_where:=v_where||' and (s.payable_count>0 or s.deduction_item_count>0)';end if;
    if v_candidate is not null then v_where:=v_where||' and s.candidate_id=$4';end if;
    if v_search<>'' then v_where:=v_where||' and (s.candidate_sort_name like $6 escape ''!'' or s.tms_ref like $6 escape ''!'')';end if;
    v_projection:=case p_kind when 'CANDIDATES' then $query$pg_catalog.jsonb_build_object('candidate_id',s.candidate_id,
      'candidate_sort_name',s.candidate_sort_name,
      'tms_ref',s.tms_ref,
      'bank_warning_code',s.bank_warning_code,
      'metadata_version',(s.metadata_version)::text,
      'projection_revision',(s.projection_revision)::text,
      'item_count',(s.item_count)::text,
      'contribution_count',(s.contribution_count)::text,
      'segment_count',(s.segment_count)::text,
      'payable_count',(s.payable_count)::text,
      'selected_count',(s.selected_count)::text,
      'snoozed_count',(s.snoozed_count)::text,
      'held_count',(s.held_count)::text,
      'queried_count',(s.queried_count)::text,
      'blocked_count',(s.blocked_count)::text,
      'deduction_item_count',(s.deduction_item_count)::text,
      'unknown_amount_count',(s.unknown_amount_count)::text,
      'unknown_selected_amount_count',(s.unknown_selected_amount_count)::text,
      'amount',(case when s.unknown_amount_count=0 then s.amount else null end)::text,
      'waiting_amount',(case when s.unknown_amount_count=0 then s.waiting_amount else null end)::text,
      'ready_net_add_amount',(case when s.unknown_amount_count=0 then s.ready_net_add_amount else null end)::text,
      'waiting_net_add_amount',(case when s.unknown_amount_count=0 then s.waiting_net_add_amount else null end)::text,
      'gross_deduction_balance',(case when s.unknown_amount_count=0 then s.gross_deduction_balance else null end)::text,
      'net_deduction_balance',(case when s.unknown_amount_count=0 then s.net_deduction_balance else null end)::text,
      'approved_amount',(case when s.unknown_amount_count=0 then s.approved_amount else null end)::text,
      'paid_credit_amount',(case when s.unknown_amount_count=0 then s.paid_credit_amount else null end)::text,
      'reserved_amount',(case when s.unknown_amount_count=0 then s.reserved_amount else null end)::text,
      'excluded_amount',(case when s.unknown_amount_count=0 then s.excluded_amount else null end)::text,
      'recovery_amount',(case when s.unknown_amount_count=0 then s.recovery_amount else null end)::text,
      'selected_amount',(case when s.unknown_selected_amount_count=0 then s.selected_amount else null end)::text,
      'has_deduction',s.deduction_item_count>0)$query$ else $query$pg_catalog.jsonb_build_object('candidate_id',s.candidate_id,
      'candidate_sort_name',s.candidate_sort_name,
      'tms_ref',s.tms_ref)$query$ end;
    if v_sort like 'AMOUNT_%' then
      v_tuple:='((s.unknown_amount_count>0),(case when s.unknown_amount_count=0 then s.amount else 0 end),s.candidate_id)';
      v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2)::uuid)';v_key:='pg_catalog.jsonb_build_array(s.unknown_amount_count>0,(case when s.unknown_amount_count=0 then s.amount else 0 end)::text,s.candidate_id)';
    elsif v_sort like 'STATUS_%' then
      v_tuple:='((s.payable_count=0),(s.bank_warning_code is null),coalesce(s.bank_warning_code,'''') collate "C",s.candidate_sort_name collate "C",s.candidate_id)';
      v_seek:='(($10->>0)::boolean,($10->>1)::boolean,($10->>2) collate "C",($10->>3) collate "C",($10->>4)::uuid)';
      v_key:='pg_catalog.jsonb_build_array(s.payable_count=0,s.bank_warning_code is null,coalesce(s.bank_warning_code,''''),s.candidate_sort_name,s.candidate_id)';
    elsif v_sort like 'DEDUCTIONS_%' then
      v_tuple:='((s.deduction_item_count>0),s.candidate_sort_name collate "C",(s.tms_ref is null),coalesce(s.tms_ref,'''') collate "C",s.candidate_id)';
      v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2)::boolean,($10->>3) collate "C",($10->>4)::uuid)';v_key:='pg_catalog.jsonb_build_array(s.deduction_item_count>0,s.candidate_sort_name,s.tms_ref is null,coalesce(s.tms_ref,''''),s.candidate_id)';
    else
      v_tuple:='(s.candidate_sort_name collate "C",(s.tms_ref is null),coalesce(s.tms_ref,'''') collate "C",s.candidate_id)';
      v_seek:='(($10->>0) collate "C",($10->>1)::boolean,($10->>2) collate "C",($10->>3)::uuid)';v_key:='pg_catalog.jsonb_build_array(s.candidate_sort_name,s.tms_ref is null,coalesce(s.tms_ref,''''),s.candidate_id)';end if;
    v_sql:=' from private.bpay_next_workbench_candidate_summary s where ';
  elsif p_kind='PICK_CLIENTS' then
    v_where:='s.client_id is not null and s.pay_channel=$2 and s.section=$3 and s.item_count>0';
    if v_client is not null then v_where:=v_where||' and s.client_id=$5';end if;
    if v_candidate is not null then v_where:=v_where||' and exists(select 1 from private.bpay_next_workbench_candidate_summary cs where cs.candidate_id=$4 and cs.client_scope=s.client_scope and cs.pay_channel=$2 and cs.section=$3 and cs.item_count>0)';end if;
    if v_search<>'' then v_where:=v_where||' and exists(select 1 from public.clients c where c.id=s.client_id and c.name like $6 escape ''!'')';end if;
    -- Client IDs seed the exact maintained scope before one factual name lookup.
    v_projection:=$query$pg_catalog.jsonb_build_object('client_id',s.client_id,
      'client_display_name',c.name)$query$;
    v_tuple:='(s.client_scope collate "C")';v_seek:='(($10->>0) collate "C")';v_key:='pg_catalog.jsonb_build_array(s.client_scope)';
    v_sql:=' from private.bpay_next_workbench_scope_summary s where ';
  elsif p_kind='CANDIDATE_GROUPS' then
    v_where:='s.candidate_id=$4 and s.section=$3';
    if v_client is not null then v_where:=v_where||' and s.client_id=$5';end if;
    if v_channel<>'ALL' then v_where:=v_where||' and s.target_pay_channel=$2';end if;
    if v_section='READY' then v_where:=v_where||' and g.complete and not g.dirty and g.progress_state=''CURRENT'' and s.selectable';end if;
    v_projection:=$query$pg_catalog.jsonb_build_object('group_key',s.presentation_key,
      'group_kind',s.presentation_kind,
      'candidate_id',g.candidate_id,
      'work_id',g.work_id,
      'case_id',g.case_id,
      'client_id',g.client_id,
      'contract_id',g.contract_id,
      'physical_timesheet_id',r.physical_timesheet_id,
      'physical_revision_id',r.id,'physical_timesheet_version',r.physical_timesheet_version::text,
      'source_kind',r.source_kind,'source_head_id',r.source_head_id,
      'source_root_timesheet_id',case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_id else h.root_timesheet_id end end,
      'source_root_timesheet_version',case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_version else h.root_timesheet_version end end::text,
      'source_week_ending_date',case when r.source_kind in ('SOURCE','PROTECTED') then r.week_ending_date end,

      'current_revision_id',g.current_revision_id,
      'source_pay_channel',g.source_pay_channel,
      'target_pay_channel',g.target_pay_channel,
      'week_ending_date',g.week_ending_date,
      'complete',g.complete,
      'dirty',g.dirty,
      'certified_zero',g.certified_zero,
      'ready_count',(g.ready_count)::text,
      'action_count',(g.action_count)::text,
      'blocked_count',(g.blocked_count)::text,
      'waiting_count',(g.waiting_count)::text,
      'stale_snooze_count',(g.stale_snooze_count)::text,
      'financial_view_revision',(g.financial_view_revision)::text,
      'projection_revision',(g.projection_revision)::text,
      'metadata_version',(g.metadata_version)::text,
      'stale_snooze_code',g.stale_snooze_code,
      'error_code',g.error_code,
      'progress_state',g.progress_state,
      'client_display_name',s.client_display_name,
      'bank_warning_code',m.bank_warning_code,
      'section',$3,
      'reason_code',s.reason_code,
      'has_deduction',s.has_deduction,
      'selectable',g.complete and not g.dirty and g.progress_state='CURRENT' and s.selectable,
      'group_amount',s.group_amount::text,
      'ready_net_add_amount',s.ready_net_add_amount::text,
      'gross_deduction_balance',s.gross_deduction_balance::text,
      'net_deduction_balance',s.net_deduction_balance::text,
      'presentation_id',s.presentation_id,'parent_group_key',s.parent_group_key,'case_component_id',s.case_component_id,
      'payment_label',s.payment_label,'timesheet_reference',s.timesheet_reference,
      'waiting_amount',s.waiting_amount::text,'waiting_net_add_amount',s.waiting_net_add_amount::text,
      'accepted_run_id',s.accepted_run_id,'accepted_command_id',s.accepted_command_id,'accepted_module_epoch',s.accepted_module_epoch::text,
      'snooze_until_date',s.snooze_until_date,'eligible_from_date',s.eligible_from_date,'pay_hold_reason',s.pay_hold_reason,
      'reason_priority',s.reason_priority)$query$;
    if v_sort like 'PAYMENT_%' or v_sort='NAME_ASC' then
      v_tuple:='((s.week_ending_date is null),coalesce(s.week_ending_date,date ''0001-01-01''),s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,coalesce(nullif($10->>1,'''')::date,date ''0001-01-01''),($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.week_ending_date is null,coalesce(s.week_ending_date::text,''''),s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'CLIENT_%' then
      v_tuple:='((s.client_display_name is null),coalesce(s.client_display_name,'''') collate "C",s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.client_display_name is null,coalesce(s.client_display_name,''''),s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'CHANNEL_%' then
      v_tuple:='((s.target_pay_channel is null),coalesce(s.target_pay_channel,'''') collate "C",s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.target_pay_channel is null,coalesce(s.target_pay_channel,''''),s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'DEDUCTIONS_%' then
      v_tuple:='(s.has_deduction,s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2)::uuid)';v_key:='jsonb_build_array(s.has_deduction,s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'AMOUNT_%' then
      v_tuple:='((s.group_amount is null),coalesce(s.group_amount,0),s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.group_amount is null,coalesce(s.group_amount,0)::text,s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'REASON_%' then
      v_tuple:='(s.reason_priority,s.reason_code collate "C",s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::smallint,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.reason_priority,s.reason_code,s.presentation_kind,s.presentation_id)';
    else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    v_sql:=' from private.bpay_next_workbench_group_display s join private.bpay_next_workbench_group g on g.group_key=s.parent_group_key and g.candidate_id=s.candidate_id where ';
  else
    v_where:='true';
    if v_candidate is not null then v_where:=v_where||' and i.candidate_id=$4';end if;
    if v_client is not null then v_where:=v_where||' and i.client_id=$5';end if;
    if v_channel<>'ALL' then v_where:=v_where||' and i.target_pay_channel=$2';end if;
    if v_section<>'ALL' then v_where:=v_where||' and i.section=$3';end if;
    if v_group is not null then
      if p_kind='GROUP_LINES' and v_group like 'ROW:%' then
        v_where:=v_where||' and i.case_component_id=substring($7 from 5)::uuid and exists(select 1 from private.bpay_next_workbench_group_display d where d.presentation_key=$7 and d.parent_group_key=i.group_key and d.candidate_id=i.candidate_id)';
      else v_where:=v_where||' and i.group_key=$7';end if;
    end if;
    if v_item is not null then v_where:=v_where||' and i.id=$8';end if;
    if v_reason is not null then v_where:=v_where||' and i.reason_code=$9';end if;
    if v_search<>'' then v_where:=v_where||' and (i.candidate_display_name like $6 escape ''!'' or i.candidate_reference like $6 escape ''!'' or i.timesheet_reference like $6 escape ''!'')';end if;
    v_projection:=case p_kind when 'ACTION_REQUIRED' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',i.group_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'client_id',i.client_id,
      'physical_timesheet_id',i.physical_timesheet_id,
      'revision_id',i.revision_id,
      'component_key',i.component_key,
      'member_kind',i.member_kind,
      'member_key',i.member_key,
      'portion_kind',i.portion_kind,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'timesheet_reference',i.timesheet_reference,
      'week_ending_date',i.week_ending_date,
      'reason_code',i.reason_code,
      'reason_priority',i.reason_priority,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'amount_known',i.amount_known,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'section',i.section,
      'query_state',i.query_state,
      'query_qualified',i.query_qualified,
      'pay_hold',i.pay_hold,
      'pay_hold_reason',i.pay_hold_reason,
      'eligible_from_date',i.eligible_from_date,
      'snooze_id',i.snooze_id,
      'snooze_version',(i.snooze_version)::text,
      'snooze_until_date',i.snooze_until_date,
      'stale_snooze_code',i.stale_snooze_code,
      'progress_state',i.progress_state,
      'error_code',i.error_code,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text,
      'metadata_version',(i.metadata_version)::text)||pg_catalog.jsonb_build_object(
      'direction',i.direction,'payroll_stage',i.payroll_stage,
      'approved_source_ex_vat',i.approved_source_ex_vat::text,
      'active_reserved_source_ex_vat',i.active_reserved_source_ex_vat::text,
      'deduction_balance_source_ex_vat',i.deduction_balance_source_ex_vat::text,
      'recovery_source_ex_vat',i.recovery_source_ex_vat::text,
      'included_source_ex_vat',i.included_source_ex_vat::text,
      'gross_due_source_ex_vat',i.gross_due_source_ex_vat::text)$query$ when 'BLOCKED' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',i.group_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'client_id',i.client_id,
      'physical_timesheet_id',i.physical_timesheet_id,
      'revision_id',i.revision_id,
      'component_key',i.component_key,
      'member_kind',i.member_kind,
      'member_key',i.member_key,
      'portion_kind',i.portion_kind,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'timesheet_reference',i.timesheet_reference,
      'week_ending_date',i.week_ending_date,
      'reason_code',i.reason_code,
      'reason_priority',i.reason_priority,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'amount_known',i.amount_known,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'section',i.section,
      'query_state',i.query_state,
      'query_qualified',i.query_qualified,
      'pay_hold',i.pay_hold,
      'pay_hold_reason',i.pay_hold_reason,
      'eligible_from_date',i.eligible_from_date,
      'snooze_id',i.snooze_id,
      'snooze_version',(i.snooze_version)::text,
      'snooze_until_date',i.snooze_until_date,
      'stale_snooze_code',i.stale_snooze_code,
      'progress_state',i.progress_state,
      'error_code',i.error_code,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text,
      'metadata_version',(i.metadata_version)::text)||pg_catalog.jsonb_build_object(
      'direction',i.direction,'payroll_stage',i.payroll_stage,
      'approved_source_ex_vat',i.approved_source_ex_vat::text,
      'active_reserved_source_ex_vat',i.active_reserved_source_ex_vat::text,
      'deduction_balance_source_ex_vat',i.deduction_balance_source_ex_vat::text,
      'recovery_source_ex_vat',i.recovery_source_ex_vat::text,
      'included_source_ex_vat',i.included_source_ex_vat::text,
      'gross_due_source_ex_vat',i.gross_due_source_ex_vat::text)$query$ when 'ISSUE_DETAIL' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',i.group_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'client_id',i.client_id,
      'physical_timesheet_id',r.physical_timesheet_id,
      'physical_revision_id',r.id,'physical_timesheet_version',r.physical_timesheet_version::text,
      'source_kind',r.source_kind,'source_head_id',r.source_head_id,
      'source_root_timesheet_id',case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_id else h.root_timesheet_id end end,
      'source_root_timesheet_version',case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_version else h.root_timesheet_version end end::text,
      'source_week_ending_date',case when r.source_kind in ('SOURCE','PROTECTED') then r.week_ending_date end,

      'revision_id',i.revision_id,
      'component_key',i.component_key,
      'member_kind',i.member_kind,
      'member_key',i.member_key,
      'portion_kind',i.portion_kind,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'timesheet_reference',i.timesheet_reference,
      'week_ending_date',i.week_ending_date,
      'reason_code',i.reason_code,
      'reason_priority',i.reason_priority,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'amount_known',i.amount_known,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'section',i.section,
      'query_state',i.query_state,
      'query_qualified',i.query_qualified,
      'pay_hold',i.pay_hold,
      'pay_hold_reason',i.pay_hold_reason,
      'eligible_from_date',i.eligible_from_date,
      'snooze_id',i.snooze_id,
      'snooze_version',(i.snooze_version)::text,
      'snooze_until_date',i.snooze_until_date,
      'stale_snooze_code',i.stale_snooze_code,
      'progress_state',i.progress_state,
      'error_code',i.error_code,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text,
      'metadata_version',(i.metadata_version)::text)||pg_catalog.jsonb_build_object(
      'direction',i.direction,'payroll_stage',i.payroll_stage,
      'approved_source_ex_vat',i.approved_source_ex_vat::text,
      'active_reserved_source_ex_vat',i.active_reserved_source_ex_vat::text,
      'deduction_balance_source_ex_vat',i.deduction_balance_source_ex_vat::text,
      'recovery_source_ex_vat',i.recovery_source_ex_vat::text,
      'included_source_ex_vat',i.included_source_ex_vat::text,
      'gross_due_source_ex_vat',i.gross_due_source_ex_vat::text)$query$ when 'GROUP_LINES' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',coalesce($7,i.group_key),
      'component_key',i.component_key,
      'member_key',i.member_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'revision_id',i.revision_id,
      'approved_line_id',i.approved_line_id,
      'approved_segment_id',i.approved_segment_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'rule_id',i.rule_id,
      'client_id',i.client_id,
      'contract_id',i.contract_id,
      'physical_timesheet_id',i.physical_timesheet_id,
      'snooze_id',i.snooze_id,
      'accepted_run_id',i.accepted_run_id,
      'accepted_command_id',i.accepted_command_id,
      'segment_key',i.segment_key,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'client_display_name',i.client_display_name,
      'timesheet_reference',i.timesheet_reference,
      'component_kind',i.component_kind,
      'tax_treatment',i.tax_treatment,
      'unit_label',i.unit_label,
      'pay_hold_reason',i.pay_hold_reason,
      'channel_note_code',i.channel_note_code,
      'snooze_scope',i.snooze_scope,
      'stale_snooze_code',i.stale_snooze_code,
      'error_code',i.error_code,
      'member_kind',i.member_kind,
      'portion_kind',i.portion_kind,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'direction',i.direction,
      'payroll_stage',i.payroll_stage,
      'work_date',i.work_date,
      'week_ending_date',i.week_ending_date,
      'eligible_from_date',i.eligible_from_date)||pg_catalog.jsonb_build_object('snooze_until_date',i.snooze_until_date,
      'approved_quantity',(i.approved_quantity)::text,
      'amount_known',i.amount_known,
      'selected_amount_known',i.selected_amount_known,
      'selectable',i.selectable,
      'is_selected',i.is_selected,
      'query_qualified',i.query_qualified,
      'pay_hold',i.pay_hold,
      'section',i.section,
      'reason_code',i.reason_code,
      'reason_priority',i.reason_priority,
      'query_state',i.query_state,
      'progress_state',i.progress_state,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text,
      'metadata_version',(i.metadata_version)::text,
      'snooze_version',(i.snooze_version)::text,
      'accepted_module_epoch',(i.accepted_module_epoch)::text,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'approved_source_ex_vat',(i.approved_source_ex_vat)::text,
      'realised_source_ex_vat',(i.realised_source_ex_vat)::text,
      'active_reserved_source_ex_vat',(i.active_reserved_source_ex_vat)::text,
      'residual_source_ex_vat',(i.residual_source_ex_vat)::text,
      'gross_due_source_ex_vat',(i.gross_due_source_ex_vat)::text,
      'included_source_ex_vat',(i.included_source_ex_vat)::text,
      'excluded_source_ex_vat',(i.excluded_source_ex_vat)::text,
      'recovery_source_ex_vat',(i.recovery_source_ex_vat)::text,
      'held_overhang_source_ex_vat',(i.held_overhang_source_ex_vat)::text,
      'deduction_balance_source_ex_vat',(i.deduction_balance_source_ex_vat)::text,
      'selected_source_ex_vat',(i.selected_source_ex_vat)::text,
      'written_off_source_ex_vat',(i.written_off_source_ex_vat)::text,
      'approved_line_no',l.line_no::text,'shift_detail_id',sd.id,'shift_detail_no',sd.detail_no::text,
      'job_title',r.job_title,'band_label',r.band_label,
      'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id,
      'is_adjustment',ts.is_adjustment,
      'component_amount_known',a.amount_known,'component_ready_amount',a.ready_amount::text,
      'component_waiting_amount',a.waiting_amount::text,'component_reserved_amount',a.reserved_amount::text)$query$ when 'EXPORT_ROWS' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',i.group_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'revision_id',i.revision_id,
      'client_id',i.client_id,
      'component_key',i.component_key,
      'member_kind',i.member_kind,
      'member_key',i.member_key,
      'portion_kind',i.portion_kind,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'client_display_name',i.client_display_name,
      'timesheet_reference',i.timesheet_reference,
      'work_date',i.work_date,
      'week_ending_date',i.week_ending_date,
      'component_kind',i.component_kind,
      'tax_treatment',i.tax_treatment,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'direction',i.direction,
      'payroll_stage',i.payroll_stage,
      'approved_quantity',(i.approved_quantity)::text,
      'unit_label',i.unit_label,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'approved_source_ex_vat',(i.approved_source_ex_vat)::text,
      'realised_source_ex_vat',(i.realised_source_ex_vat)::text,
      'active_reserved_source_ex_vat',(i.active_reserved_source_ex_vat)::text,
      'residual_source_ex_vat',(i.residual_source_ex_vat)::text,
      'included_source_ex_vat',(i.included_source_ex_vat)::text,
      'excluded_source_ex_vat',(i.excluded_source_ex_vat)::text,
      'deduction_balance_source_ex_vat',(i.deduction_balance_source_ex_vat)::text,
      'amount_known',i.amount_known,
      'section',i.section,
      'reason_code',i.reason_code,
      'selectable',i.selectable,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text)||pg_catalog.jsonb_build_object('metadata_version',(i.metadata_version)::text)$query$ end;
    -- Non-null native UUID closes every issue sort including CASE portions.
    if p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then
      if v_sort like 'PAYMENT_%' then
        v_tuple:='(coalesce(i.timesheet_reference,'''') collate "C",i.id)';
        v_seek:='(($10->>0) collate "C",($10->>1)::uuid)';v_key:='jsonb_build_array(coalesce(i.timesheet_reference,''''),i.id)';
      elsif v_sort like 'REASON_%' then
        v_tuple:='(i.reason_priority,i.reason_code collate "C",i.id)';
        v_seek:='(($10->>0)::smallint,($10->>1) collate "C",($10->>2)::uuid)';
        v_key:='jsonb_build_array(i.reason_priority,i.reason_code,i.id)';
      elsif v_sort like 'AMOUNT_%' then
        v_tuple:='((i.nominal_source_ex_vat is null),coalesce(i.nominal_source_ex_vat,0),i.id)';
        v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2)::uuid)';v_key:='jsonb_build_array(i.nominal_source_ex_vat is null,coalesce(i.nominal_source_ex_vat,0)::text,i.id)';
      else
        v_tuple:='(coalesce(i.candidate_display_name,'''') collate "C",i.candidate_id,i.id)';
        v_seek:='(($10->>0) collate "C",($10->>1)::uuid,($10->>2)::uuid)';v_key:='jsonb_build_array(coalesce(i.candidate_display_name,''''),i.candidate_id,i.id)';end if;
    else v_tuple:='(i.id)';v_seek:='(($10->>0)::uuid)';v_key:='pg_catalog.jsonb_build_array(i.id)';end if;
    v_sql:=' from private.bpay_next_workbench_item i where ';
  end if;
  -- Explicit scalar ORDER BY; never ORDER BY a composite row (which prevents
  -- ordinary multicolumn index use). Commas inside COALESCE stay intact.
  v_order:=case
    when p_kind in ('CANDIDATES','PICK_CANDIDATES') and v_sort like 'AMOUNT_%' then '(s.unknown_amount_count>0) @,(case when s.unknown_amount_count=0 then s.amount else 0 end) @,s.candidate_id @'
    when p_kind='CANDIDATES' and v_sort like 'STATUS_%' then '(s.payable_count=0) @,(s.bank_warning_code is null) @,coalesce(s.bank_warning_code,'''') collate "C" @,s.candidate_sort_name collate "C" @,s.candidate_id @'
    when p_kind in ('CANDIDATES','PICK_CANDIDATES') and v_sort like 'DEDUCTIONS_%' then '(s.deduction_item_count>0) @,s.candidate_sort_name collate "C" @,(s.tms_ref is null) @,coalesce(s.tms_ref,'''') collate "C" @,s.candidate_id @'
    when p_kind in ('CANDIDATES','PICK_CANDIDATES') then 's.candidate_sort_name collate "C" @,(s.tms_ref is null) @,coalesce(s.tms_ref,'''') collate "C" @,s.candidate_id @'
    when p_kind='PICK_CLIENTS' then 's.client_scope collate "C" @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'PAYMENT_%' or v_sort='NAME_ASC') then '(s.week_ending_date is null) @,coalesce(s.week_ending_date,date ''0001-01-01'') @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'CLIENT_%') then '(s.client_display_name is null) @,coalesce(s.client_display_name,'''') collate "C" @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'CHANNEL_%') then '(s.target_pay_channel is null) @,coalesce(s.target_pay_channel,'''') collate "C" @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'DEDUCTIONS_%') then 's.has_deduction @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'AMOUNT_%') then '(s.group_amount is null) @,coalesce(s.group_amount,0) @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'REASON_%') then 's.reason_priority @,s.reason_code collate "C" @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'PAYMENT_%' then 'coalesce(i.timesheet_reference,'''') collate "C" @,i.id @'
    when p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'REASON_%' then 'i.reason_priority @,i.reason_code collate "C" @,i.id @'
    when p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'AMOUNT_%' then '(i.nominal_source_ex_vat is null) @,coalesce(i.nominal_source_ex_vat,0) @,i.id @'
    when p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then 'coalesce(i.candidate_display_name,'''') collate "C" @,i.candidate_id @,i.id @'
    else 'i.id @' end;
  v_order:=replace(v_order,'@',case when v_desc then 'desc' else 'asc' end);
  v_search:=replace(replace(replace(v_search,'!','!!'),'%','!%'),'_','!_')||'%';
  if p_kind='ISSUE_DETAIL' then
    execute 'select count(distinct i.candidate_id)'||v_sql||v_where into v_affected
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
  end if;
  -- A refresh anchor is a native identity, resolved to today's exact sort key here.
  -- It is never a browser-retagged historical cursor.
  if p_kind='CANDIDATE_GROUPS' and v_group is not null and p_after is null then
    execute 'select '||v_key||v_sql||v_where||' and s.presentation_key=$7' into p_after
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if p_after is null then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_anchor_page:=true;
  end if;
  if p_after is not null then
    execute 'select exists(select 1'||v_sql||v_where||' and '||v_tuple||'='||v_seek||')' into v_member
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if not v_member then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_where:=v_where||' and '||v_tuple||case when v_anchor_page then case when v_desc then '<=' else '>=' end else v_op end||v_seek;
  end if;
  if p_kind='PICK_CLIENTS' then
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select s.client_id,s.client_scope'||v_sql||v_where||' order by '||v_order||' limit $11) s join public.clients c on c.id=s.client_id order by '||v_order;
  elsif p_kind='CANDIDATE_GROUPS' then
    -- Ten indexed native presentation identities before only PK factual enrichment.
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select s.presentation_key,s.presentation_kind,s.presentation_id,s.parent_group_key,s.candidate_id,s.case_component_id,s.section,s.client_id,s.week_ending_date,s.client_display_name,s.payment_label,s.timesheet_reference,s.target_pay_channel,s.has_deduction,s.selectable,s.reason_code,s.reason_priority,s.group_amount,s.ready_net_add_amount,s.gross_deduction_balance,s.net_deduction_balance,s.waiting_amount,s.waiting_net_add_amount,s.accepted_run_id,s.accepted_command_id,s.accepted_module_epoch,s.snooze_until_date,s.eligible_from_date,s.pay_hold_reason'||v_sql||v_where||' order by '||v_order||' limit $11) s join private.bpay_next_workbench_group g on g.group_key=s.parent_group_key and g.candidate_id=s.candidate_id left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=s.candidate_id left join private.bpay_next_work w on w.id=g.work_id and w.candidate_id=g.candidate_id left join private.bpay_next_work_revision r on r.work_id=w.id and r.id=case when w.approval_state=''WITHDRAWN'' then w.applied_revision_id else w.current_revision_id end left join public.weekly_source_entitlement_heads h on h.id=r.source_head_id and h.candidate_id=w.candidate_id and h.contract_id=w.contract_id and h.week_ending_date=r.week_ending_date order by '||v_order;
  elsif p_kind='ISSUE_DETAIL' then
    -- WB17: native issue identities first, exact Work current/withdrawn-applied
    -- revision only; no first-booking fallback or current Source pointer.
    v_sql:='with seed as materialized(select i.id,'||v_key||' row_key'||v_sql||v_where||' order by '||v_order||' limit $11)
      select '||v_projection||' as row_value,seed.row_key from seed
      join private.bpay_next_workbench_item i on i.id=seed.id
      left join private.bpay_next_work w on w.id=i.work_id and w.candidate_id=i.candidate_id
      left join private.bpay_next_work_revision r on r.work_id=w.id
        and r.id=case when w.approval_state=''WITHDRAWN'' then w.applied_revision_id else w.current_revision_id end
      left join public.weekly_source_entitlement_heads h on h.id=r.source_head_id
        and h.candidate_id=w.candidate_id and h.contract_id=w.contract_id and h.week_ending_date=r.week_ending_date
      order by '||v_order;
  elsif p_kind='GROUP_LINES' then
    -- WB16: limit the original scoped UUID identities first. Deduplicate only
    -- their exact component keys, then enrich from current indexed facts.
    -- No history, quote, producer or browser aggregation on opening.
    v_sql:='with seed as materialized(select i.id,'||v_key||' row_key'||v_sql||v_where||' order by '||v_order||' limit $11),
      component_keys as materialized(select distinct i.group_key,i.candidate_id,i.work_id,i.revision_id,i.approved_line_id,i.component_key
        from seed join private.bpay_next_workbench_item i on i.id=seed.id where i.work_id is not null and i.approved_line_id is not null),
      component_amounts as materialized(select k.group_key,k.candidate_id,k.work_id,k.revision_id,k.approved_line_id,k.component_key,
        count(*)>0 and bool_and(m.amount_known and (not m.is_selected or m.selected_amount_known)
          and m.included_source_ex_vat is not null and m.gross_due_source_ex_vat is not null
          and m.active_reserved_source_ex_vat is not null and g.complete and not g.dirty and g.current_revision_id=m.revision_id) amount_known,
        case when count(*)>0 and bool_and(m.amount_known and (not m.is_selected or m.selected_amount_known)
          and m.included_source_ex_vat is not null and m.gross_due_source_ex_vat is not null
          and m.active_reserved_source_ex_vat is not null and g.complete and not g.dirty and g.current_revision_id=m.revision_id)
          then coalesce(sum(case when m.section=''READY'' then case when m.is_selected then m.selected_source_ex_vat else m.included_source_ex_vat end else 0 end),0) end ready_amount,
        case when count(*)>0 and bool_and(m.amount_known and (not m.is_selected or m.selected_amount_known)
          and m.included_source_ex_vat is not null and m.gross_due_source_ex_vat is not null
          and m.active_reserved_source_ex_vat is not null and g.complete and not g.dirty and g.current_revision_id=m.revision_id)
          then coalesce(sum(m.gross_due_source_ex_vat) filter(where m.section=''WAITING'' and m.portion_kind<>''RESERVED''),0) end waiting_amount,
        case when count(*)>0 and bool_and(m.amount_known and (not m.is_selected or m.selected_amount_known)
          and m.included_source_ex_vat is not null and m.gross_due_source_ex_vat is not null
          and m.active_reserved_source_ex_vat is not null and g.complete and not g.dirty and g.current_revision_id=m.revision_id)
          then coalesce(sum(m.active_reserved_source_ex_vat) filter(where m.portion_kind=''RESERVED''),0) end reserved_amount
        from component_keys k join private.bpay_next_workbench_item m on m.group_key=k.group_key and m.candidate_id=k.candidate_id
          and m.work_id=k.work_id and m.revision_id=k.revision_id and m.approved_line_id=k.approved_line_id and m.component_key=k.component_key
          and m.member_kind in (''WORK_COMPONENT'',''WORK_SEGMENT'')
        join private.bpay_next_workbench_group g on g.group_key=k.group_key and g.candidate_id=k.candidate_id
        group by k.group_key,k.candidate_id,k.work_id,k.revision_id,k.approved_line_id,k.component_key)
      select '||v_projection||' as row_value,seed.row_key from seed join private.bpay_next_workbench_item i on i.id=seed.id
      left join private.bpay_next_approved_line l on l.id=i.approved_line_id and l.revision_id=i.revision_id and l.component_key=i.component_key
      left join private.bpay_next_work_revision r on r.id=i.revision_id and r.work_id=i.work_id
      left join private.bpay_next_work w on w.id=i.work_id and w.candidate_id=i.candidate_id
      left join private.bpay_next_approved_segment sg on sg.id=i.approved_segment_id and sg.revision_id=i.revision_id
        and sg.approved_line_id=i.approved_line_id and sg.component_key=i.component_key and sg.segment_key=i.segment_key
      left join private.bpay_next_shift_detail sd on sd.id=sg.shift_detail_id and sd.approved_line_id=i.approved_line_id and sd.segment_key=i.segment_key
      left join public.timesheets ts on ts.timesheet_id=r.physical_timesheet_id and ts.contract_id=w.contract_id
      left join public.timesheets parent on parent.timesheet_id=ts.parent_timesheet_id
        and exists(select 1 from public.contracts pc where pc.id=parent.contract_id and pc.candidate_id=i.candidate_id)
      left join component_amounts a on a.group_key=i.group_key and a.candidate_id=i.candidate_id and a.work_id=i.work_id
        and a.revision_id=i.revision_id and a.approved_line_id=i.approved_line_id and a.component_key=i.component_key
      order by '||v_order;
  else
    -- No joins or arbitrary row transport: the page is the indexed typed seed.
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key'||v_sql||v_where||' order by '||v_order||' limit $11';
  end if;
  for v_row in execute v_sql using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1 loop
    if v_n=p_limit then v_complete:=false;exit;end if;
    if pg_catalog.octet_length(v_row.row_value::text)>110000 then
      raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    v_value:=v_row.row_value;
    if p_kind in ('CANDIDATES','CANDIDATE_GROUPS','GROUP_LINES') then
     v_value:=private.bpay_next_selection_memory_row_v1(p_kind,v_value,p_scope,v_baseline);end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_value);v_last:=v_row.row_key;v_n:=v_n+1;
    if pg_catalog.octet_length(v_rows::text)>110000 then
      raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  end loop;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete,'last_key',v_last,
    'affected_candidate_count',case when p_kind='ISSUE_DETAIL' then v_affected::text else null end);
end
$function$;
create or replace function public.bpay_next_workbench_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare
  v_actor uuid;v_kind text;v_scope jsonb;v_context jsonb;v_after jsonb;v_cursor jsonb;
  v_page jsonb;v_response jsonb;v_read_scope jsonb;v_ns jsonb;v_memory jsonb;v_baseline uuid;v_revision private.bpay_next_workbench_revision%rowtype;
  v_epoch bigint;v_agency uuid;v_date_context jsonb;v_limit integer;v_error text;
  v_key_count integer;v_key jsonb;v_sort text;v_text text;v_keys text[];v_count_selection jsonb;v_progress jsonb;
begin
  -- STABLE reads share a single statement snapshot. No volatile authority or
  -- FOR SHARE helper; these are the existing service/active-admin predicates.
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  p_request:=p_request||jsonb_build_object('count_selection',coalesce(p_request->'count_selection','null'::jsonb),
    'progress_after',coalesce(p_request->'progress_after','null'::jsonb),'selection_memory',coalesce(p_request->'selection_memory','null'::jsonb));
  v_keys:=array['actor_user_id','kind','candidate_id','client_id','channel','section',
    'search','sort','group_key','item_id','reason_code','after','limit','count_selection','progress_after','selection_memory'];
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>32768
    or not(p_request ?& v_keys) or p_request-v_keys<>'{}'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if jsonb_typeof(p_request->'actor_user_id')<>'string'
    or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select r.* into strict v_revision from private.bpay_next_workbench_revision r where r.id=1;
  select d.installation_id into strict v_agency from private.cloudtms_database_identity d where d.singleton;
  v_kind:=p_request->>'kind';v_sort:=p_request->>'sort';
  if jsonb_typeof(p_request->'kind')<>'string'
    or v_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL',
      'CANDIDATE_GROUPS','GROUP_LINES','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS')
    or jsonb_typeof(p_request->'channel')<>'string' or p_request->>'channel' not in ('ALL','PAYE','UMBRELLA')
    or jsonb_typeof(p_request->'section')<>'string' or p_request->>'section' not in ('ALL','READY','ACTION','BLOCKED','WAITING')
    or jsonb_typeof(p_request->'sort')<>'string' or (v_kind='EXPORT_ROWS' and v_sort not in ('C_NAME_ASC','C_NAME_DESC','C_AMOUNT_ASC','C_AMOUNT_DESC','C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC','C_STATUS_ASC','C_STATUS_DESC','G_PAYMENT_ASC','G_PAYMENT_DESC','G_CLIENT_ASC','G_CLIENT_DESC','G_CHANNEL_ASC','G_CHANNEL_DESC','G_AMOUNT_ASC','G_AMOUNT_DESC','G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC','G_REASON_ASC','G_REASON_DESC'))
    or (v_kind<>'EXPORT_ROWS' and (false or v_sort not in ('NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC','CLIENT_ASC','CLIENT_DESC','CHANNEL_ASC','CHANNEL_DESC')
    or (v_kind='CANDIDATES' and v_sort like 'PAYMENT_%')
    or (v_kind<>'CANDIDATES' and v_sort like 'STATUS_%')
    or (v_kind not in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort like 'REASON_%')
    or (v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'DEDUCTIONS_%')
    or (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort<>'NAME_ASC')
    or (v_sort like 'CLIENT_%' or v_sort like 'CHANNEL_%') and v_kind<>'CANDIDATE_GROUPS'
    or (v_kind='CANDIDATE_GROUPS' and v_sort='NAME_DESC')
))
    or jsonb_typeof(p_request->'search')<>'string' or length(p_request->>'search')>200
    or octet_length(p_request->>'search')>800
    or btrim(p_request->>'search')<>p_request->>'search'
    or jsonb_typeof(p_request->'limit')<>'number' or p_request->>'limit'!~'^[1-9][0-9]{0,2}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_limit:=(p_request->>'limit')::integer;
  if v_limit not between 1 and 100 or (v_kind='CANDIDATE_GROUPS' and v_limit>10) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  foreach v_text in array array['candidate_id','client_id','item_id'] loop
    if p_request->v_text<>'null'::jsonb and (jsonb_typeof(p_request->v_text)<>'string'
      or p_request->>v_text!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end loop;
  if p_request->'group_key'<>'null'::jsonb and (jsonb_typeof(p_request->'group_key')<>'string'
    or p_request->>'group_key'!~'^(WORK|CASE|ROW):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->>'group_key' like 'ROW:%' and v_kind not in ('GROUP_LINES','CANDIDATE_GROUPS') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->'reason_code'<>'null'::jsonb and (jsonb_typeof(p_request->'reason_code')<>'string'
    or p_request->>'reason_code'!~'^[A-Z][A-Z0-9_]{0,255}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if (v_kind in ('CANDIDATE_GROUPS','GROUP_LINES') and p_request->'candidate_id'='null'::jsonb)
    or (v_kind in ('GROUP_LINES','ISSUE_DETAIL') and p_request->'group_key'='null'::jsonb)
    or (v_kind='ISSUE_DETAIL' and p_request->'reason_code'='null'::jsonb)
    or (v_kind not in ('GROUP_LINES','ISSUE_DETAIL','CANDIDATE_GROUPS') and (p_request->'group_key'<>'null'::jsonb or p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)
    or (v_kind='CANDIDATE_GROUPS' and (p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)))
    or (v_kind not in ('CANDIDATES','PICK_CANDIDATES','PICK_CLIENTS','ACTION_REQUIRED','BLOCKED','EXPORT_ROWS') and p_request->>'search'<>'')
    or (v_kind='HEADER' and p_request->'after'<>'null'::jsonb) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_count_selection:=p_request->'count_selection';
  if v_count_selection ? 'intent' then
   if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
    or not(v_count_selection ?& array['intent','memory_baseline_run_id'])
    or v_count_selection-array['intent','memory_baseline_run_id']<>'{}'::jsonb
    or not private.bpay_next_selection_memory_intent_valid_v1(v_count_selection->'intent') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  else
  if v_count_selection<>'null'::jsonb then
    if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
      or not(v_count_selection ?& array['mode','candidate_ids'])
      or v_count_selection-array['mode','candidate_ids']<>'{}'::jsonb
      or jsonb_typeof(v_count_selection->'mode')<>'string' or v_count_selection->>'mode' not in ('ALL','NONE')
      or jsonb_typeof(v_count_selection->'candidate_ids')<>'array' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if exists(select 1 from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)
      where jsonb_typeof(a.value)<>'string' or (a.value#>>'{}')!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (select count(*)<>count(distinct a.value) from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)) then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end if;
  end if;
  if p_request->'progress_after'<>'null'::jsonb and v_kind<>'HEADER' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_scope:=p_request-array['actor_user_id','kind','after','limit','progress_after'];
  -- Fixed effective sections, explicit normalization; client cannot silently
  -- continue a cursor issued for a different list.
  if v_kind='CANDIDATES' then v_scope:=jsonb_set(v_scope,'{section}','"READY"'::jsonb);
  elsif v_kind='ACTION_REQUIRED' then v_scope:=jsonb_set(v_scope,'{section}','"ACTION"'::jsonb);
  elsif v_kind='BLOCKED' then v_scope:=jsonb_set(v_scope,'{section}','"BLOCKED"'::jsonb);end if;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  if v_date_context->>'ok'<>'true' or v_date_context->>'current_official_pay_date' is null then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE';end if;
  v_context:=jsonb_build_object('kind',v_kind,'agency_id',v_agency,'actor_user_id',v_actor,
    'owner_epoch',v_epoch::text,'official_pay_date',v_date_context->>'current_official_pay_date',
    'workbench_revision',v_revision.workbench_revision::text,'adoption_revision',v_revision.adoption_revision::text,
    'scope',v_scope);
  if v_kind='HEADER' then
    v_progress:=private.bpay_next_workbench_job_progress_v1(v_context,p_request->'progress_after',v_limit);
  end if;
  if v_revision.adoption_state<>'CURRENT' or v_revision.dirty_candidate_count<>0
    or v_revision.dirty_group_count<>0 or v_revision.pending_task_count<>0 then
    v_response:=jsonb_build_object('ok',false,'error_code','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT',
      'retryable',true,'reload_required',false,'workbench_revision',v_revision.workbench_revision::text,
      'complete',false,'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
      'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
    if v_kind='HEADER' then v_response:=v_response||jsonb_build_object('job_progress',v_progress);end if;
    if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    return v_response;
  end if;
  v_ns:=jsonb_build_object('agency_id',v_agency,'owner_epoch',v_epoch::text,'actor_user_id',v_actor,
   'official_pay_date',v_date_context->>'current_official_pay_date','scope',jsonb_build_object(
    'candidate_id',v_scope->'candidate_id','client_id',v_scope->'client_id','channel',v_scope->'channel'));
  v_baseline:=private.bpay_next_selection_memory_resolve_v1(v_ns,p_request->'selection_memory');
  v_memory:=jsonb_build_object('run_id',v_baseline,'scope',coalesce(nullif(p_request->'selection_memory'->'scope','null'::jsonb),v_ns->'scope'));
  if v_count_selection ? 'intent' and v_count_selection->'memory_baseline_run_id' is distinct from v_memory->'run_id' then
   raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  if v_count_selection ? 'intent' then
   if exists(select 1 from unnest(array['works','components']) kind
    cross join lateral jsonb_array_elements(v_count_selection->'intent'->kind) chosen
    where not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision wr on wr.work_id=w.id
     where w.id=(chosen->>'work_id')::uuid and w.candidate_id=(chosen->>'candidate_id')::uuid
      and wr.id=(chosen->>'expected_revision_id')::uuid and w.current_revision_id=wr.id and w.applied_revision_id=wr.id
      and (kind='works' or exists(select 1 from private.bpay_next_approved_line l where l.id=(chosen->>'approved_line_id')::uuid and l.revision_id=wr.id))))
    or exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'cases') chosen
     where not exists(select 1 from private.bpay_next_case_component cc where cc.id=(chosen->>'case_component_id')::uuid and cc.candidate_id=(chosen->>'candidate_id')::uuid)) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  end if;
  v_context:=v_context||jsonb_build_object('selection_memory',v_memory);
  v_read_scope:=v_scope||jsonb_build_object('_selection_memory',v_memory);
  v_cursor:=p_request->'after';
  if v_cursor<>'null'::jsonb then
    if jsonb_typeof(v_cursor)<>'object' or not(v_cursor ?& array['context','key'])
      or v_cursor-array['context','key']<>'{}'::jsonb or v_cursor->'context' is distinct from v_context then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_after:=v_cursor->'key';
    if v_kind='EXPORT_ROWS' then
      if jsonb_typeof(v_after)<>'array' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      if v_sort in ('C_NAME_ASC','C_NAME_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'string' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_AMOUNT_ASC','C_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'boolean' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_STATUS_ASC','C_STATUS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_PAYMENT_ASC','G_PAYMENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CLIENT_ASC','G_CLIENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CHANNEL_ASC','G_CHANNEL_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_AMOUNT_ASC','G_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_REASON_ASC','G_REASON_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    else
    v_key_count:=case when v_kind in ('CANDIDATES','PICK_CANDIDATES') then case when v_sort like 'DEDUCTIONS_%' or v_sort like 'STATUS_%' then 5 when v_sort like 'AMOUNT_%' then 3 else 4 end
      when v_kind='CANDIDATE_GROUPS' then case when v_sort like 'DEDUCTIONS_%' then 3 else 4 end when v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then case when v_sort like 'PAYMENT_%' then 2 else 3 end else 1 end;
    if jsonb_typeof(v_after)<>'array' or jsonb_array_length(v_after)<>v_key_count then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_kind in ('CANDIDATES','PICK_CANDIDATES') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string'
        or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'DEDUCTIONS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or jsonb_typeof(v_after->2)<>'boolean' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'STATUS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string')) then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind='CANDIDATE_GROUPS' then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(v_after->(v_key_count-2))<>'string' or v_after->>(v_key_count-2) not in ('WORK','CASE','ROW')
        or (v_sort like 'DEDUCTIONS_%' and jsonb_typeof(v_after->0)<>'boolean')
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort not like 'DEDUCTIONS_%' and v_sort not like 'REASON_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string'))
        or ((v_sort='NAME_ASC' or v_sort like 'PAYMENT_%') and ((v_after->>0='true' and v_after->>1<>'') or (v_after->>0='false' and v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')))
        or (v_sort like 'AMOUNT_%' and v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^(0|[1-9][0-9]*)$'
          or (v_after->>0)::numeric>32767 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort like 'PAYMENT_%' and jsonb_typeof(v_after->0)<>'string') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif jsonb_typeof(v_after->0)<>'string' or (v_kind='CANDIDATE_GROUPS'
      and v_after->>0!~'^(WORK|CASE):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (v_kind<>'CANDIDATE_GROUPS' and v_after->>0!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    end if;
  end if;
  case v_kind
    when 'HEADER' then v_page:=private.bpay_next_workbench_header_page_v1(v_read_scope,v_after,v_limit);
    when 'CANDIDATES' then v_page:=private.bpay_next_workbench_candidates_page_v1(v_read_scope,v_after,v_limit);
    when 'ACTION_REQUIRED' then v_page:=private.bpay_next_workbench_action_required_page_v1(v_read_scope,v_after,v_limit);
    when 'BLOCKED' then v_page:=private.bpay_next_workbench_blocked_page_v1(v_read_scope,v_after,v_limit);
    when 'ISSUE_DETAIL' then v_page:=private.bpay_next_workbench_issue_detail_page_v1(v_read_scope,v_after,v_limit);
    when 'CANDIDATE_GROUPS' then v_page:=private.bpay_next_workbench_candidate_groups_page_v1(v_read_scope,v_after,v_limit);
    when 'GROUP_LINES' then v_page:=private.bpay_next_workbench_group_lines_page_v1(v_read_scope,v_after,v_limit);
    when 'PICK_CANDIDATES' then v_page:=private.bpay_next_workbench_pick_candidates_page_v1(v_read_scope,v_after,v_limit);
    when 'PICK_CLIENTS' then v_page:=private.bpay_next_workbench_pick_clients_page_v1(v_read_scope,v_after,v_limit);
    when 'EXPORT_ROWS' then v_page:=private.bpay_next_workbench_export_rows_page_v1(v_read_scope,v_after,v_limit);
  end case;
  v_response:=jsonb_build_object('ok',true,'kind',v_kind,'workbench_revision',v_revision.workbench_revision::text,
    'complete',v_page->'complete','context',v_context,'rows',v_page->'rows',
    'affected_candidate_count',v_page->'affected_candidate_count',
    'next_cursor',case when v_page->>'complete'='true' then null else jsonb_build_object('context',v_context,'key',v_page->'last_key') end,
    'page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_page->'last_key',
      'row_count',jsonb_array_length(v_page->'rows')::text));
  if v_kind='HEADER' then
    v_response:=v_response||jsonb_build_object('job_progress',v_progress);
    v_response:=jsonb_set(v_response,'{rows,0}',(v_response#>'{rows,0}')||
      jsonb_build_object('oldest_pending_age_seconds',v_progress->'oldest_pending_age_seconds'));
  end if;
  if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  return v_response;
exception when others then
  v_error:=case
    when sqlstate='42501' then 'BPAY_NEXT_WORKBENCH_FORBIDDEN'
    when sqlstate in ('22023','22P02','22003','22007','22008') then 'BPAY_NEXT_WORKBENCH_REQUEST_INVALID'
    when sqlstate='54000' then 'BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE'
    when sqlerrm in ('BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_WORKBENCH_CURSOR_STALE',
      'BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT','BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE') then sqlerrm
    else 'BPAY_NEXT_WORKBENCH_UNAVAILABLE' end;
  return jsonb_build_object('ok',false,'error_code',v_error,'retryable',v_error in
    ('BPAY_NEXT_WORKBENCH_UNAVAILABLE','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT'),
    'reload_required',v_error='BPAY_NEXT_WORKBENCH_CURSOR_STALE',
    'workbench_revision',v_revision.workbench_revision::text,'complete',false,
    'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
    'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
end
$function$;
alter function private.bpay_next_selection_memory_intent_valid_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_intent_valid_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid) owner to postgres;
revoke all on function private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer) owner to postgres;
revoke all on function private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_workbench_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_page_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_workbench_page_v1(jsonb) to service_role;

create or replace function private.bpay_next_selection_memory_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare r private.bpay_next_pay_run%rowtype;h private.bpay_next_selection_mode_memory%rowtype;v_header boolean:=false;
begin
 if tg_op in ('DELETE','TRUNCATE') then raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_IMMUTABLE';end if;
 select pr.* into strict r from private.bpay_next_pay_run pr where pr.id=new.run_id for update;
 if tg_op='UPDATE' then
  if tg_table_name<>'bpay_next_selection_mode_memory' then raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_IMMUTABLE';end if;
  if old.row_kind<>'HEADER' or r.selection_state<>'OPEN' or r.status<>'PREPARING'
   or (to_jsonb(new)-array['command_id','agency_sequence','acknowledged_at_utc']) is distinct from (to_jsonb(old)-array['command_id','agency_sequence','acknowledged_at_utc']) then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_IMMUTABLE';end if;
  if (new.command_id,new.agency_sequence) is distinct from (old.command_id,old.agency_sequence) then
   if old.command_id is not null or old.agency_sequence is not null or new.command_id is null or new.agency_sequence is null
    or new.acknowledged_at_utc is distinct from old.acknowledged_at_utc then
    raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_IMMUTABLE';end if;
  elsif old.acknowledged_at_utc is not null or new.acknowledged_at_utc is null then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_IMMUTABLE';end if;
  if new.command_id is distinct from r.accepted_choice_command_id or not exists(
   select 1 from private.bpay_next_run_command rc join private.bpay_next_command c on c.id=rc.command_id
   where rc.run_id=new.run_id and rc.command_id=new.command_id and c.command_kind='PREPARE'
    and c.agency_sequence=new.agency_sequence and c.module_epoch=new.owner_epoch) then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_NATIVE_IDENTITY_INVALID';end if;
  return new;
 end if;
 if r.status<>'PREPARING' or r.selection_state<>'OPEN' or r.accepted_choice_descriptor is not null
  or new.actor_user_id is distinct from r.actor_user_id or new.official_pay_date is distinct from r.pay_date
  or not exists(select 1 from private.cloudtms_database_identity d where d.singleton and d.installation_id=new.agency_id)
  or not exists(select 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' and c.owner_epoch=new.owner_epoch) then
  raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_NATIVE_IDENTITY_INVALID';end if;
 if tg_table_name='bpay_next_selection_mode_memory' then v_header:=new.row_kind='HEADER';end if;
 if v_header then
  if new.command_id is not null or new.agency_sequence is not null or new.acknowledged_at_utc is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_NATIVE_IDENTITY_INVALID';end if;
  if new.baseline_run_id is not null and not exists(select 1 from private.bpay_next_selection_mode_memory b
   where b.run_id=new.baseline_run_id and b.row_kind='HEADER' and b.acknowledged_at_utc is not null
    and (b.agency_id,b.owner_epoch,b.actor_user_id,b.official_pay_date,b.scope)=(new.agency_id,new.owner_epoch,new.actor_user_id,new.official_pay_date,new.scope)) then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_NATIVE_IDENTITY_INVALID';end if;
 else
  select m.* into strict h from private.bpay_next_selection_mode_memory m where m.run_id=new.run_id and m.row_kind='HEADER';
  if (h.agency_id,h.owner_epoch,h.actor_user_id,h.official_pay_date,h.scope) is distinct from (new.agency_id,new.owner_epoch,new.actor_user_id,new.official_pay_date,new.scope) then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_NATIVE_IDENTITY_INVALID';end if;
  if tg_table_name='bpay_next_selection_mode_memory' then
   if new.work_id is not null and not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision wr on wr.work_id=w.id join private.bpay_next_approved_line l on l.revision_id=wr.id
     where w.id=new.work_id and w.candidate_id=new.candidate_id and l.id=new.approved_line_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_NATIVE_IDENTITY_INVALID';end if;
   if new.case_component_id is not null and not exists(select 1 from private.bpay_next_case_component c where c.id=new.case_component_id and c.candidate_id=new.candidate_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_NATIVE_IDENTITY_INVALID';end if;
  elsif not exists(select 1 from private.bpay_next_selection_mode_memory b where b.run_id=new.basis_ref_run_id and b.row_kind='HEADER'
    and (b.agency_id,b.owner_epoch,b.actor_user_id,b.official_pay_date,b.scope)=(new.agency_id,new.owner_epoch,new.actor_user_id,new.official_pay_date,new.scope)) then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_NATIVE_IDENTITY_INVALID';end if;
 end if;
 return new;
end
$function$;
alter function private.bpay_next_selection_memory_guard_v1() owner to postgres;
revoke all on function private.bpay_next_selection_memory_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_selection_mode_memory_guard on private.bpay_next_selection_mode_memory;
create trigger bpay_next_selection_mode_memory_guard before insert or update or delete on private.bpay_next_selection_mode_memory for each row execute function private.bpay_next_selection_memory_guard_v1();
drop trigger if exists bpay_next_selection_mode_memory_truncate on private.bpay_next_selection_mode_memory;
create trigger bpay_next_selection_mode_memory_truncate before truncate on private.bpay_next_selection_mode_memory for each statement execute function private.bpay_next_selection_memory_guard_v1();
drop trigger if exists bpay_next_selection_exclusion_memory_guard on private.bpay_next_selection_exclusion_memory;
create trigger bpay_next_selection_exclusion_memory_guard before insert or update or delete on private.bpay_next_selection_exclusion_memory for each row execute function private.bpay_next_selection_memory_guard_v1();
drop trigger if exists bpay_next_selection_exclusion_memory_truncate on private.bpay_next_selection_exclusion_memory;
create trigger bpay_next_selection_exclusion_memory_truncate before truncate on private.bpay_next_selection_exclusion_memory for each statement execute function private.bpay_next_selection_memory_guard_v1();
commit;
