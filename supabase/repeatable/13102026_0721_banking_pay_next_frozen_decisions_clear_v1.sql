-- WB28/BD175: precise frozen future clear and actor defaults.
\set ON_ERROR_STOP on
begin;

-- WB28/BD175: immutable unit defaults, no accepted-record deletion.
create or replace function private.bpay_next_selection_remembered_unit_v1(p_leaves bigint,p_recorded bigint,p_low boolean,p_high boolean)
returns boolean language plpgsql immutable security invoker set search_path=pg_catalog,private
as $function$
begin
 if p_recorded=0 then return null;end if;
 if p_leaves<>p_recorded or p_low is null or p_high is null or p_low<>p_high then
  raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_UNIT_MISSING';end if;
 return p_low;
end
$function$;
create or replace function private.bpay_next_selection_reset_matches_v1(p_baseline uuid,p_candidate uuid,p_work uuid,p_revision uuid,p_line uuid,p_case uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select exists(select 1 from private.bpay_next_selection_mode_memory h
  join private.bpay_next_decisions_clear_operation o on o.agency_id=h.agency_id and o.owner_epoch=h.owner_epoch
   and o.actor_user_id=h.actor_user_id and o.official_pay_date=h.official_pay_date
   and o.reset_sequence>h.agency_sequence
  where h.run_id=p_baseline and h.row_kind='HEADER' and h.acknowledged_at_utc is not null
   and(o.scope->>'candidate_id' is null or o.scope->>'candidate_id'=p_candidate::text)
   and exists(select 1 from private.bpay_next_workbench_item i where i.candidate_id=p_candidate
    and i.work_id is not distinct from p_work and i.revision_id is not distinct from p_revision
    and i.approved_line_id is not distinct from p_line and i.case_component_id is not distinct from p_case
    and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
    and i.portion_kind in('AVAILABLE','ZERO') and i.progress_state='CURRENT'
    and(o.scope->>'client_id' is null or o.scope->>'client_id'=i.client_id::text)
    and(o.scope->>'channel'='ALL' or o.scope->>'channel'=i.target_pay_channel)))
$function$;

create or replace function private.bpay_next_selection_memory_choices_v1(p_scope jsonb,p_baseline uuid,p_intent jsonb)
returns table(leaf_key text,candidate_id uuid,work_id uuid,revision_id uuid,approved_line_id uuid,case_component_id uuid,direction text,basis jsonb,selected boolean,new_changed boolean)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 with current_leaves as materialized(select * from private.bpay_next_selection_memory_leaves_v1(p_scope)),
 unit_facts as (select l.candidate_id,l.work_id,l.revision_id,l.approved_line_id,l.case_component_id,l.direction,
  case when l.work_id is not null then 'WORK:'||l.work_id::text||':'||(l.basis->>'component_key') else 'CASE:'||l.case_component_id::text end unit_key,
  bool_or(b.basis is distinct from l.basis) or
   exists(select 1 from private.bpay_next_selection_mode_memory prior where prior.run_id=p_baseline and prior.row_kind='LEAF'
    and prior.candidate_id=l.candidate_id and (prior.work_id=l.work_id and prior.approved_line_id=l.approved_line_id or prior.case_component_id=l.case_component_id)
    and not exists(select 1 from current_leaves now_leaf where now_leaf.leaf_key=prior.leaf_key and now_leaf.basis=prior.basis)) changed,
  count(*) leaf_count,count(b.remembered_selected) remembered_count,
  bool_and(b.remembered_selected) remembered_min,bool_or(b.remembered_selected) remembered_max
 from current_leaves l left join private.bpay_next_selection_mode_memory b on b.run_id=p_baseline and b.row_kind='LEAF' and b.leaf_key=l.leaf_key
 group by l.candidate_id,l.work_id,l.revision_id,l.approved_line_id,l.case_component_id,l.direction,l.basis->>'component_key'),
 units as (select u.*,case when u.changed then null else
  private.bpay_next_selection_remembered_unit_v1(u.leaf_count,u.remembered_count,u.remembered_min,u.remembered_max) end remembered_selected from unit_facts u),
 defaults as (
 select l.*,'{}'::jsonb basis,
  case when p_baseline is null then coalesce(p_intent->>'mode','ALL_MATCHING')='ALL_MATCHING'
   when l.changed then true
   when private.bpay_next_selection_reset_matches_v1(p_baseline,l.candidate_id,l.work_id,l.revision_id,l.approved_line_id,l.case_component_id)
    then case when p_intent->>'mode'='NONE' and h.mode<>'NONE' then false else true end
   when l.remembered_selected is not null and(p_intent->>'mode' is null or p_intent->>'mode'=h.mode) then l.remembered_selected
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
declare v_ns jsonb:=p_descriptor->'namespace';v_baseline uuid:=(p_descriptor->>'memory_baseline_run_id')::uuid;v_kind text;v_row jsonb;v_id uuid;v_leaf record;v_group text;v_next_group text;v_rows bigint:=0;v_bytes bigint:=0;v_encoded jsonb;
begin
 insert into private.bpay_next_selection_mode_memory(run_id,row_kind,leaf_key,agency_id,owner_epoch,actor_user_id,official_pay_date,scope,mode,intent_revision,baseline_run_id,basis,raw_intent,accepted_context)
 values(p_run,'HEADER','HEADER',(v_ns->>'agency_id')::uuid,(v_ns->>'owner_epoch')::bigint,(v_ns->>'actor_user_id')::uuid,(v_ns->>'official_pay_date')::date,v_ns->'scope',p_descriptor->'intent'->>'mode',(p_descriptor->>'revision')::bigint,v_baseline,'{}',p_descriptor->'intent',p_descriptor-'intent');
 -- Finite per-component proof, streamed without a global Draft cap.
 for v_leaf in with choices as materialized(
   select * from private.bpay_next_selection_memory_choices_v1(v_ns->'scope',v_baseline,p_descriptor->'intent'))
  select l.*,c.selected remembered_selected from private.bpay_next_selection_memory_leaves_v1(v_ns->'scope') l
  left join choices c on c.candidate_id=l.candidate_id and c.work_id is not distinct from l.work_id
   and c.revision_id is not distinct from l.revision_id and c.approved_line_id is not distinct from l.approved_line_id
   and c.case_component_id is not distinct from l.case_component_id and c.direction=l.direction
  order by l.candidate_id,l.work_id,l.approved_line_id,l.case_component_id,l.leaf_key loop
  if v_leaf.remembered_selected is null then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_MEMORY_UNIT_MISSING';end if;
  v_next_group:=jsonb_build_array(v_leaf.candidate_id,v_leaf.work_id,v_leaf.approved_line_id,v_leaf.case_component_id)::text;
  if v_group is distinct from v_next_group then v_group:=v_next_group;v_rows:=0;v_bytes:=0;end if;
  v_encoded:=jsonb_build_object('run_id',p_run,'row_kind','LEAF','leaf_key',v_leaf.leaf_key,
   'agency_id',v_ns->'agency_id','owner_epoch',v_ns->'owner_epoch','actor_user_id',v_ns->'actor_user_id',
   'official_pay_date',v_ns->'official_pay_date','scope',v_ns->'scope','mode',p_descriptor->'intent'->'mode',
   'intent_revision',p_descriptor->'revision','baseline_run_id',v_baseline,
   'candidate_id',v_leaf.candidate_id,'work_id',v_leaf.work_id,'approved_line_id',v_leaf.approved_line_id,
   'case_component_id',v_leaf.case_component_id,'basis',v_leaf.basis,'remembered_selected',v_leaf.remembered_selected);
  v_rows:=v_rows+1;v_bytes:=v_bytes+octet_length(v_encoded::text);
  if v_rows>10000 or v_bytes>8388608 then
   raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
  insert into private.bpay_next_selection_mode_memory(run_id,row_kind,leaf_key,agency_id,owner_epoch,actor_user_id,official_pay_date,scope,mode,intent_revision,baseline_run_id,candidate_id,work_id,approved_line_id,case_component_id,basis,remembered_selected)
  values(p_run,'LEAF',v_leaf.leaf_key,(v_ns->>'agency_id')::uuid,(v_ns->>'owner_epoch')::bigint,(v_ns->>'actor_user_id')::uuid,(v_ns->>'official_pay_date')::date,v_ns->'scope',p_descriptor->'intent'->>'mode',(p_descriptor->>'revision')::bigint,v_baseline,v_leaf.candidate_id,v_leaf.work_id,v_leaf.approved_line_id,v_leaf.case_component_id,v_leaf.basis,v_leaf.remembered_selected);
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


alter function private.bpay_next_selection_remembered_unit_v1(bigint,bigint,boolean,boolean) owner to postgres;
revoke all on function private.bpay_next_selection_remembered_unit_v1(bigint,bigint,boolean,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_selection_reset_matches_v1(uuid,uuid,uuid,uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_selection_reset_matches_v1(uuid,uuid,uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_selection_memory_capture_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_capture_v1(uuid,jsonb) from public,anon,authenticated,service_role;

-- WB28 bounded immutable disclosure over the frozen closure; never live rediscovery.
create or replace function private.bpay_next_decisions_clear_result_v1(p_request uuid,p_cursor jsonb default null)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare o private.bpay_next_decisions_clear_operation%rowtype;v_rows jsonb:='[]';v_next jsonb;n record;v_count integer:=0;v_bytes integer:=0;v_value jsonb;
begin
 select x.* into strict o from private.bpay_next_decisions_clear_operation x where x.request_id=p_request;
 for n in with parents as materialized(
   select x.* from private.bpay_next_decisions_clear_member x where x.request_id=p_request
    and x.member_no>=coalesce((p_cursor->>'member_no')::bigint,1) order by x.member_no limit 11
  ) select m.member_no,ord.ordinality owner_no,m.work_id,m.candidate_id,m.owner_family,m.exclusion_code,m.subject,
   pg.outcome_code,pg.native_receipt is not null cleared,ord.value owner
  from parents m
  cross join lateral jsonb_array_elements(case when jsonb_array_length(m.owners)=0 then '[null]'::jsonb else m.owners end) with ordinality ord(value,ordinality)
  left join private.bpay_next_decisions_clear_page pg on pg.request_id=p_request and pg.phase='APPLY' and pg.step_no=m.member_no-1
  where(p_cursor is null or(m.member_no,ord.ordinality)>((p_cursor->>'member_no')::bigint,(p_cursor->>'owner_no')::bigint))
  order by m.member_no,ord.ordinality limit 11 loop
  if v_count=10 then v_next:=jsonb_build_object('member_no',(v_rows->(v_count-1)->>'member_no'),'owner_no',(v_rows->(v_count-1)->>'owner_no'));exit;end if;
  v_value:=jsonb_build_object('member_no',n.member_no::text,'owner_no',n.owner_no::text,'candidate_id',n.candidate_id,'work_id',n.work_id,
   'owner_family',n.owner_family,'exclusion_code',n.exclusion_code,'subject',n.subject,
   'outcome_code',n.outcome_code,'cleared',n.cleared and n.owner->>'clear_class'='RESOLVED_AND_CLEARABLE','owner',case when n.owner is null or n.owner='null'::jsonb then null else jsonb_build_object(
    'owner_id',n.owner->'owner_id','owner_version',n.owner->'owner_version','approved_line_id',n.owner->'approved_line_id',
    'rate_family',n.owner->'rate_family','rate_code',n.owner->'rate_code','amount_ex_vat',n.owner->'amount_ex_vat',
    'clear_class',n.owner->'clear_class','exclusion_code',n.owner->'exclusion_code','target_pay_channel',n.owner->'target_pay_channel',
    'resolution_mode',n.owner#>'{signature,resolution_mode}','manual_replacement_rate',n.owner#>'{signature,manual_replacement_rate}',
    'manual_amount_ex_vat',n.owner#>'{signature,manual_amount_ex_vat}') end);
  if v_bytes+octet_length(v_value::text)>110000 then
   if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_DECISIONS_CLEAR_SCOPE_TOO_LARGE';end if;
   v_next:=jsonb_build_object('member_no',(v_rows->(v_count-1)->>'member_no'),'owner_no',(v_rows->(v_count-1)->>'owner_no'));exit;end if;
  v_bytes:=v_bytes+octet_length(v_value::text);
  v_rows:=v_rows||jsonb_build_array(v_value);v_count:=v_count+1;
 end loop;
 return jsonb_build_object('request_id',o.request_id,'agency_id',o.agency_id,'actor_user_id',o.actor_user_id,'owner_epoch',o.owner_epoch::text,
  'official_pay_date',o.official_pay_date::text,'scope',o.scope,'expected_workbench_revision',o.expected_workbench_revision::text,'state',o.state,
  'scope_summary',o.scope_summary,'candidate_count',o.candidate_count::text,'work_count',o.work_count::text,'owner_count',o.owner_count::text,'excluded_count',o.excluded_count::text,
  'affected_amount',o.affected_amount::text,'cleared_count',o.cleared_count::text,'refused_count',o.refused_count::text,'cleared_amount',o.cleared_amount::text,
  'freeze_step',o.freeze_step::text,'apply_step',o.apply_step::text,'reset_sequence',o.reset_sequence::text,'rows',v_rows,'next_cursor',v_next);
end
$function$;

create or replace function private.bpay_next_decisions_clear_command_v1(p_actor uuid,p_args jsonb,p_readback boolean)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare
 o private.bpay_next_decisions_clear_operation%rowtype;pg private.bpay_next_decisions_clear_page%rowtype;
 m private.bpay_next_decisions_clear_member%rowtype;prev private.bpay_next_decisions_clear_page%rowtype;
 w private.bpay_next_work%rowtype;d private.bpay_next_component_resolution%rowtype;v_pair record;
 v_request uuid;v_phase text;v_step bigint;v_epoch bigint;v_header jsonb;v_scope jsonb;v_read jsonb;v_base jsonb;v_result jsonb;v_response jsonb;
 v_owners jsonb;v_rows jsonb;v_native jsonb;v_receipt jsonb;v_owner jsonb;v_current jsonb;v_carry jsonb;
 v_revision uuid;v_line uuid;v_effective uuid;v_effective_version bigint;v_subrequest uuid;v_code text;v_sequence bigint;v_count bigint;v_excluded bigint;
 v_sum numeric;v_clear_count bigint:=0;v_clear_sum numeric:=0;v_reason text;v_ready boolean;v_context jsonb;v_after jsonb;
 v_resolved boolean;v_member_count bigint;v_bytes bigint:=0;v_physical integer:=0;
 v_frozen boolean;v_facts jsonb;v_subject jsonb;v_one jsonb;i private.bpay_next_workbench_item%rowtype;r private.bpay_next_work_revision%rowtype;
 v_approval uuid;v_source_event uuid;v_after_book jsonb;v_book_after jsonb;v_compare jsonb;v_original jsonb;v_page_base jsonb;
 v_unsupported boolean;v_scope_channel text;
begin
 if p_readback is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>4096
  or not(p_args?&array['request_id','scope','owner_epoch','official_pay_date','expected_workbench_revision','phase','step','cursor','confirmed'])
  or p_args-array['request_id','scope','owner_epoch','official_pay_date','expected_workbench_revision','phase','step','cursor','confirmed']<>'{}'::jsonb
  or jsonb_typeof(p_args->'request_id') is distinct from 'string' or p_args->>'request_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  or jsonb_typeof(p_args->'expected_workbench_revision') is distinct from 'string' or p_args->>'expected_workbench_revision'!~'^(0|[1-9][0-9]{0,18})$'
  or jsonb_typeof(p_args->'owner_epoch') is distinct from 'string' or p_args->>'owner_epoch'!~'^[1-9][0-9]{0,18}$'
  or (p_args->>'owner_epoch')::numeric>9223372036854775807
  or jsonb_typeof(p_args->'official_pay_date') is distinct from 'string' or p_args->>'official_pay_date'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  or (p_args->>'official_pay_date')::date::text is distinct from p_args->>'official_pay_date'
  or jsonb_typeof(p_args->'step') is distinct from 'string' or p_args->>'step'!~'^(0|[1-9][0-9]{0,18})$'
  or (p_args->>'expected_workbench_revision')::numeric>9223372036854775807 or (p_args->>'step')::numeric>9223372036854775807
  or jsonb_typeof(p_args->'phase') is distinct from 'string' or p_args->>'phase' not in('FREEZE','VIEW','APPLY')
  or jsonb_typeof(p_args->'confirmed') is distinct from 'boolean'
  or ((p_args->>'confirmed')::boolean is distinct from (p_args->>'phase'='APPLY')) then
  raise exception using errcode='22023',message='BPAY_NEXT_DECISIONS_CLEAR_REQUEST_INVALID';end if;
 v_scope:=p_args->'scope';
 if jsonb_typeof(v_scope) is distinct from 'object' or not(v_scope?&array['candidate_id','client_id','channel'])
  or v_scope-array['candidate_id','client_id','channel']<>'{}'::jsonb or jsonb_typeof(v_scope->'channel') is distinct from 'string'
  or v_scope->>'channel' not in('ALL','PAYE','UMBRELLA')
  or exists(select 1 from unnest(array['candidate_id','client_id']) k where jsonb_typeof(v_scope->k) not in('null','string')
   or(v_scope->k<>'null'::jsonb and v_scope->>k!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')) then
  raise exception using errcode='22023',message='BPAY_NEXT_DECISIONS_CLEAR_REQUEST_INVALID';end if;
 if p_args->'cursor'<>'null'::jsonb and(p_args->>'phase'<>'VIEW' or jsonb_typeof(p_args->'cursor')<>'object'
  or not(p_args->'cursor'?&array['member_no','owner_no']) or(p_args->'cursor')-array['member_no','owner_no']<>'{}'::jsonb
  or (p_args->'cursor'->>'owner_no')::numeric>100
  or exists(select 1 from unnest(array['member_no','owner_no']) k where jsonb_typeof(p_args->'cursor'->k)<>'string'
   or p_args->'cursor'->>k!~'^[1-9][0-9]{0,18}$' or(p_args->'cursor'->>k)::numeric>9223372036854775807)) then
  raise exception using errcode='22023',message='BPAY_NEXT_DECISIONS_CLEAR_REQUEST_INVALID';end if;
 v_request:=(p_args->>'request_id')::uuid;v_phase:=p_args->>'phase';v_step:=(p_args->>'step')::bigint;
 -- Original operation/page identity and immutable receipt replay before all live fences.
 select x.* into o from private.bpay_next_decisions_clear_operation x where x.request_id=v_request;
 if o.request_id is not null and(o.actor_user_id,o.scope,o.owner_epoch::text,o.official_pay_date::text,o.expected_workbench_revision::text)
  is distinct from(p_actor,v_scope,p_args->>'owner_epoch',p_args->>'official_pay_date',p_args->>'expected_workbench_revision') then
  raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_REPLAY_CONFLICT';end if;
 if v_phase='VIEW' then
  if not p_readback then raise exception using errcode='22023',message='BPAY_NEXT_DECISIONS_CLEAR_REQUEST_INVALID';end if;
  if o.request_id is null then return jsonb_build_object('request_id',v_request,'phase',v_phase,'step',v_step::text,'received',false,'replay',false,'processing',false,'result',null);end if;
  return jsonb_build_object('request_id',v_request,'phase',v_phase,'step',v_step::text,'received',true,'replay',true,'processing',false,
   'result',private.bpay_next_decisions_clear_result_v1(v_request,nullif(p_args->'cursor','null'::jsonb)));
 end if;
 select x.* into pg from private.bpay_next_decisions_clear_page x where x.request_id=v_request and x.phase=v_phase and x.step_no=v_step;
 if pg.request_id is not null then
  if pg.request_body is distinct from p_args then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_REPLAY_CONFLICT';end if;
  return pg.result_body||jsonb_build_object('replay',true);
 end if;
 if p_readback then return jsonb_build_object('request_id',v_request,'phase',v_phase,'step',v_step::text,'received',false,'replay',false,'processing',false,'result',null);end if;
 select x.owner_epoch into v_epoch from private.bpay_next_module_control x where x.id=1 and x.active_owner='NEXT' for share nowait;
 if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if o.request_id is null then
  if v_phase<>'FREEZE' or v_step<>0 then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_REPLAY_CONFLICT';end if;
  v_header:=public.bpay_next_workbench_page_v1(jsonb_build_object('actor_user_id',p_actor,'kind','HEADER',
   'candidate_id',v_scope->'candidate_id','client_id',v_scope->'client_id','channel',v_scope->>'channel','section','READY','search','','sort','NAME_ASC',
   'group_key',null,'item_id',null,'reason_code',null,'count_selection',null,'after',null,'limit',100));
  if v_header->>'ok' is distinct from 'true' or v_header#>>'{context,workbench_revision}' is distinct from p_args->>'expected_workbench_revision'
   or v_header#>>'{context,owner_epoch}' is distinct from p_args->>'owner_epoch'
   or v_header#>>'{context,official_pay_date}' is distinct from p_args->>'official_pay_date' then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
  select last_sequence into strict v_sequence from private.bpay_next_command_clock where id=1;
  insert into private.bpay_next_decisions_clear_operation(request_id,agency_id,actor_user_id,owner_epoch,official_pay_date,scope,
   expected_workbench_revision,initial_sequence,initial_args,scope_summary,state,expected_sequence)
  values(v_request,(v_header#>>'{context,agency_id}')::uuid,p_actor,v_epoch,(v_header#>>'{context,official_pay_date}')::date,v_scope,
   (p_args->>'expected_workbench_revision')::bigint,v_sequence,p_args,
   jsonb_build_object('candidate_count',v_header#>'{rows,0,candidate_count}'),'DISCOVERING',v_sequence)
  on conflict(request_id) do nothing;
 end if;
 select x.* into strict o from private.bpay_next_decisions_clear_operation x where x.request_id=v_request for update nowait;
 if(o.actor_user_id,o.scope,o.owner_epoch::text,o.official_pay_date::text,o.expected_workbench_revision::text) is distinct from(p_actor,v_scope,p_args->>'owner_epoch',p_args->>'official_pay_date',p_args->>'expected_workbench_revision') then
  raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_REPLAY_CONFLICT';end if;
 select x.* into pg from private.bpay_next_decisions_clear_page x where x.request_id=v_request and x.phase=v_phase and x.step_no=v_step;
 if pg.request_id is not null then
  if pg.request_body is distinct from p_args then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_REPLAY_CONFLICT';end if;
  return pg.result_body||jsonb_build_object('replay',true);
 end if;
 if o.owner_epoch<>v_epoch then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
 if v_phase='FREEZE' then
  if o.state<>'DISCOVERING' or o.freeze_step<>v_step then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_REPLAY_CONFLICT';end if;
  if (select workbench_revision from private.bpay_next_workbench_revision where id=1)<>o.expected_workbench_revision
   or(select last_sequence from private.bpay_next_command_clock where id=1)<>o.initial_sequence then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
  -- Bound the physical indexed Work/family prefix before current/filter qualification.
  if o.after_work_id is null then
   select d.work_id,d.based_on_revision_id basis_revision_id,case when d.rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end owner_family into v_pair
    from private.bpay_next_component_resolution d where d.state='ACTIVE'
    order by d.work_id,case when d.rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end,d.id limit 1;
  else
   select d.work_id,d.based_on_revision_id basis_revision_id,case when d.rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end owner_family into v_pair
    from private.bpay_next_component_resolution d where d.state='ACTIVE'
     and(d.work_id,case when d.rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end)>(o.after_work_id,o.after_family)
    order by d.work_id,case when d.rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end,d.id limit 1;
  end if;
  if v_pair.work_id is null then
   update private.bpay_next_decisions_clear_operation set state='READY',freeze_step=freeze_step+1 where request_id=v_request;
  else
   select x.* into strict w from private.bpay_next_work x where x.id=v_pair.work_id;
   select x.* into strict r from private.bpay_next_work_revision x where x.id=coalesce(w.current_revision_id,w.applied_revision_id,v_pair.basis_revision_id) and x.work_id=w.id;
   v_owners:='[]';v_code:=null;v_physical:=0;
   if(o.scope->>'candidate_id' is null or w.candidate_id=(o.scope->>'candidate_id')::uuid)
    and(o.scope->>'client_id' is null or exists(select 1 from private.bpay_next_workbench_group g where g.work_id=w.id and g.client_id=(o.scope->>'client_id')::uuid)) then
   -- Positively unsupported saved roots are disclosed, never submitted to CLEAR.
   select upper(c.pay_method),not(c.active is true and upper(c.pay_method) in('PAYE','UMBRELLA')
     and(upper(c.pay_method)<>'UMBRELLA' or(u.id is not null and u.enabled is true and u.vat_chargeable is not null)))
    into v_scope_channel,v_unsupported from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id where c.id=w.candidate_id;
   v_unsupported:=coalesce(v_unsupported,true);
   if v_unsupported then v_base:='{}';v_code:='UNAVAILABLE';
   else
   v_base:=private.bpay_next_resolution_offer_context_v1(p_actor,w.candidate_id);
   if v_base->>'agency_id' is distinct from o.agency_id::text or v_base->>'actor_user_id' is distinct from o.actor_user_id::text
    or v_base->>'owner_epoch' is distinct from o.owner_epoch::text or v_base->>'official_pay_date' is distinct from o.official_pay_date::text then
    raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
   end if;
   if o.scope->>'channel'='ALL' or v_scope_channel=o.scope->>'channel' then
   v_subject:=jsonb_build_object('candidate_display_name',r.candidate_display_name,'candidate_reference',r.candidate_reference,
    'client_display_name',r.client_display_name,'timesheet_reference',r.timesheet_reference,'week_ending_date',w.week_ending_date);
   for d in select x.* from private.bpay_next_component_resolution x where x.state='ACTIVE' and x.work_id=w.id
     and(case when x.rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end)=v_pair.owner_family order by x.id limit 101 loop
    v_physical:=v_physical+1;
    if v_physical>100 then raise exception using errcode='54000',message='BPAY_NEXT_DECISIONS_CLEAR_SCOPE_TOO_LARGE';end if;
    -- Scope comes only from this owner's current mapped native projection.
    select x.* into i from private.bpay_next_workbench_item x where x.work_id=w.id and x.revision_id=w.current_revision_id
      and x.approved_line_id=case when d.based_on_revision_id=w.current_revision_id then d.approved_line_id
        when d.published_revision_id=w.current_revision_id then d.published_line_id end
      and(o.scope->>'candidate_id' is null or x.candidate_id=(o.scope->>'candidate_id')::uuid)
      and(o.scope->>'client_id' is null or x.client_id=(o.scope->>'client_id')::uuid)
      and(o.scope->>'channel'='ALL' or x.target_pay_channel=o.scope->>'channel') order by x.section,x.id limit 1;
    v_line:=coalesce(i.approved_line_id,case when d.published_revision_id=r.id then d.published_line_id else d.approved_line_id end);
    v_one:=jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'approved_line_id',v_line,
      'rate_family',d.rate_family,'rate_code',d.rate_code,'owner_family',v_pair.owner_family,'owner_id',d.id,'owner_version',d.decision_version::text,
      'linked',false,'signature',null,'amount_ex_vat',null,'amount_unavailable_code',null,'clear_class','NOT_CLEARABLE',
      'client_display_name',r.client_display_name,'timesheet_reference',r.timesheet_reference,'week_ending_date',w.week_ending_date,
      'target_pay_channel',d.target_pay_channel,'target_umbrella_id',d.target_umbrella_id,'exclusion_code',null,'frozen_facts',null);
    if v_unsupported then v_one:=v_one||jsonb_build_object('exclusion_code','UNAVAILABLE');
    elsif i.id is null or w.current_revision_id is null or w.applied_revision_id is distinct from w.current_revision_id then
     v_one:=v_one||jsonb_build_object('exclusion_code','CHANGED');
    else
    v_frozen:=i.accepted_run_id is not null or i.is_selected or exists(
      select 1 from private.bpay_next_run_choice_member cm join private.bpay_next_pay_run pr on pr.id=cm.run_id and pr.accepted_choice_descriptor is not null
      left join private.bpay_next_run_worker rw on rw.run_id=cm.run_id and rw.candidate_id=cm.candidate_id
      where cm.candidate_id=i.candidate_id and cm.work_id=i.work_id and cm.expected_revision_id=i.revision_id and cm.member_kind='WORK'
       and cm.selected and cm.skip_code is null and pr.status in('PREPARING','REVIEW','DRAFT','EXECUTING') and(rw.id is null or rw.status not in('CANCELLED','COMPLETE'))
       and(cm.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component sc where sc.run_id=cm.run_id and sc.work_id=cm.work_id and sc.approved_line_id=i.approved_line_id)
        or exists(select 1 from jsonb_array_elements(pr.accepted_choice_descriptor#>'{intent,components}') dl
         where dl->>'work_id'=cm.work_id::text and dl->>'approved_line_id'=i.approved_line_id::text and dl->'selected'='true'::jsonb)));
    if v_frozen then v_one:=v_one||jsonb_build_object('exclusion_code','IN_DRAFT');
    else
     begin
      v_read:=private.bpay_next_saved_resolution_clear_review_v1(p_actor,jsonb_build_object('candidate_id',w.candidate_id,'work_id',w.id,
       'expected_revision_id',w.current_revision_id,'owner_family',v_pair.owner_family,'owner_id',d.id));
      select x into v_one from jsonb_array_elements(v_read#>'{clear_review,owners}') x where x->>'work_id'=w.id::text and x->>'owner_id'=d.id::text;
      if v_one is null then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
      select f into v_facts from jsonb_array_elements(v_read#>'{context,works}') cw cross join lateral jsonb_array_elements(cw->'rows') f
       where cw->>'work_id'=w.id::text and f->>'approved_line_id'=i.approved_line_id::text and f->>'rate_family'=d.rate_family and f->>'rate_code'=d.rate_code;
      if v_facts is null then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
      if w.work_kind='SOURCE' then
       v_approval:=private.bpay_next_treatment_approval_revision_v1(w.current_revision_id);
       select ar.source_event_id into strict v_source_event from private.bpay_next_work_revision ar where ar.id=v_approval and ar.work_id=w.id;
       v_facts:=jsonb_set(v_facts,'{original_basis,source_event_id}',coalesce(to_jsonb(v_source_event),'null'::jsonb))
        ||jsonb_build_object('approval_revision_id',v_approval);end if;
      v_one:=v_one||jsonb_build_object('exclusion_code',case when v_one->>'clear_class'<>'RESOLVED_AND_CLEARABLE' then 'UNAVAILABLE' end,
       'frozen_facts',v_facts-array['approved_line_id','decisions']);
     exception when check_violation or invalid_parameter_value then
      get stacked diagnostics v_reason=MESSAGE_TEXT;
      if v_reason not in('BPAY_NEXT_RESOLUTION_STALE','BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED','BPAY_NEXT_RESOLUTION_BASIS_CHANGED',
       'BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE','BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE') then raise;end if;
      v_one:=jsonb_build_object('work_id',w.id,'expected_revision_id',w.current_revision_id,'approved_line_id',i.approved_line_id,
       'rate_family',d.rate_family,'rate_code',d.rate_code,'owner_family',v_pair.owner_family,'owner_id',d.id,'owner_version',d.decision_version::text,
       'linked',false,'signature',null,'amount_ex_vat',null,'amount_unavailable_code',v_reason,'clear_class','NOT_CLEARABLE',
       'client_display_name',r.client_display_name,'timesheet_reference',r.timesheet_reference,'week_ending_date',w.week_ending_date,
       'target_pay_channel',d.target_pay_channel,'target_umbrella_id',d.target_umbrella_id,
       'exclusion_code',case when v_reason='BPAY_NEXT_RESOLUTION_STALE' then 'CHANGED' else 'UNAVAILABLE' end,'frozen_facts',null);
     end;
    end if;
    end if;
    v_owners:=v_owners||jsonb_build_array(v_one);
    if octet_length(v_owners::text)>100000 then raise exception using errcode='54000',message='BPAY_NEXT_DECISIONS_CLEAR_SCOPE_TOO_LARGE';end if;
   end loop;
   if v_owners<>'[]'::jsonb then
    select count(*) filter(where x->>'clear_class'='RESOLVED_AND_CLEARABLE'),coalesce(sum((x->>'amount_ex_vat')::numeric) filter(where x->>'clear_class'='RESOLVED_AND_CLEARABLE'),0),
     count(*) filter(where x->>'clear_class'<>'RESOLVED_AND_CLEARABLE') into v_count,v_sum,v_excluded from jsonb_array_elements(v_owners) x;
    update private.bpay_next_decisions_clear_operation set
     candidate_count=candidate_count+case when not exists(select 1 from private.bpay_next_decisions_clear_member mm where mm.request_id=v_request and mm.candidate_id=w.candidate_id) then 1 else 0 end,
     work_count=work_count+case when not exists(select 1 from private.bpay_next_decisions_clear_member mm where mm.request_id=v_request and mm.work_id=w.id) then 1 else 0 end where request_id=v_request;
    insert into private.bpay_next_decisions_clear_member(request_id,member_no,candidate_id,work_id,expected_revision_id,owner_family,native_context,subject,owners,exclusion_code)
     values(v_request,o.member_count+1,w.candidate_id,w.id,r.id,v_pair.owner_family,v_base,v_subject,v_owners,null);
    update private.bpay_next_decisions_clear_operation set member_count=member_count+1,owner_count=owner_count+v_count,
     affected_amount=affected_amount+v_sum,excluded_count=excluded_count+v_excluded where request_id=v_request;
   end if;
   end if;end if;
   update private.bpay_next_decisions_clear_operation set freeze_step=freeze_step+1,after_work_id=w.id,after_family=v_pair.owner_family where request_id=v_request;
  end if;
  -- One complete discovery cut: even a concurrent non-clock metadata refresh cannot split it.
  if (select workbench_revision from private.bpay_next_workbench_revision where id=1)<>o.expected_workbench_revision
   or(select last_sequence from private.bpay_next_command_clock where id=1)<>o.initial_sequence then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
 else
  if o.state not in('READY','APPLYING') or o.apply_step<>v_step then
   raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_REPLAY_CONFLICT';end if;
  select x.* into m from private.bpay_next_decisions_clear_member x where x.request_id=v_request and x.member_no=v_step+1;
  v_code:=null;
  if m.request_id is not null then
   v_code:=m.exclusion_code;
   select count(*),coalesce(sum((x->>'amount_ex_vat')::numeric),0) into v_clear_count,v_clear_sum from jsonb_array_elements(m.owners) x where x->>'clear_class'='RESOLVED_AND_CLEARABLE';
   if v_clear_count=0 then v_code:=coalesce(v_code,'UNAVAILABLE');end if;
   if v_code is null then
    if (select last_sequence from private.bpay_next_command_clock where id=1)<>o.expected_sequence
     or(o.expected_sequence=o.initial_sequence and(select workbench_revision from private.bpay_next_workbench_revision where id=1)<>o.expected_workbench_revision) then v_code:='CHANGED';
    else
     select x.* into strict w from private.bpay_next_work x where x.id=m.work_id;
     v_revision:=m.expected_revision_id;
     select x.* into prev from private.bpay_next_decisions_clear_page x where x.request_id=v_request and x.phase='APPLY' and x.work_id=m.work_id
      and x.native_receipt is not null order by x.step_no desc limit 1;
     if prev.request_id is not null then v_revision:=(prev.native_receipt#>>'{result,works,0,published_revision_id}')::uuid;end if;
     if w.current_revision_id is distinct from v_revision then v_code:='CHANGED';
     elsif w.applied_revision_id is distinct from v_revision then
      return jsonb_build_object('request_id',v_request,'phase',v_phase,'step',v_step::text,'received',false,'replay',false,'processing',true,
       'result',private.bpay_next_decisions_clear_result_v1(v_request));
     else
      begin
       v_rows:='[]';v_physical:=0;v_bytes:=0;
       for v_owner in select value from jsonb_array_elements(m.owners) where value->>'clear_class'='RESOLVED_AND_CLEARABLE' loop
        v_effective:=(v_owner->>'owner_id')::uuid;v_effective_version:=(v_owner->>'owner_version')::bigint;
        if prev.request_id is not null then
         select x into v_carry from jsonb_array_elements(prev.native_receipt#>'{result,works,0,carried_decisions}') x
          where x->>'previous_decision_id'=v_effective::text and x->>'previous_decision_version'=v_effective_version::text;
         if v_carry is null then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
         v_effective:=(v_carry->>'decision_id')::uuid;v_effective_version:=(v_carry->>'decision_version')::bigint;
        end if;
        v_read:=private.bpay_next_saved_resolution_clear_review_v1(p_actor,jsonb_build_object('candidate_id',m.candidate_id,'work_id',m.work_id,
         'expected_revision_id',v_revision,'owner_family',m.owner_family,'owner_id',v_effective));
        v_base:=v_read->'context'-'works';
        if v_page_base is null then v_page_base:=v_base;
        elsif v_base is distinct from v_page_base then
         raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
        if v_base-array['workbench_revision','adoption_revision','last_command_sequence','financial_view_revision']
         is distinct from m.native_context-array['workbench_revision','adoption_revision','last_command_sequence','financial_view_revision'] then
         raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
        select x into v_current from jsonb_array_elements(v_read#>'{clear_review,owners}') x where x->>'work_id'=m.work_id::text
         and x->>'owner_id'=v_effective::text and x->>'owner_version'=v_effective_version::text;
        if v_current is null or v_current->>'clear_class'<>'RESOLVED_AND_CLEARABLE'
         or v_current->'signature' is distinct from v_owner->'signature' or v_current->'amount_ex_vat' is distinct from v_owner->'amount_ex_vat' then
         raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
        select f into v_facts from jsonb_array_elements(v_read#>'{context,works}') cw cross join lateral jsonb_array_elements(cw->'rows') f
         where cw->>'work_id'=m.work_id::text and f->>'approved_line_id'=v_current->>'approved_line_id'
          and f->>'rate_family'=v_current->>'rate_family' and f->>'rate_code'=v_current->>'rate_code';
        if v_facts is null then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
        if w.work_kind='SOURCE' then
         v_approval:=private.bpay_next_treatment_approval_revision_v1(v_revision);
         select ar.source_event_id into strict v_source_event from private.bpay_next_work_revision ar where ar.id=v_approval and ar.work_id=w.id;
         v_facts:=jsonb_set(v_facts,'{original_basis,source_event_id}',coalesce(to_jsonb(v_source_event),'null'::jsonb))
          ||jsonb_build_object('approval_revision_id',v_approval);end if;
        v_compare:=v_facts-array['approved_line_id','decisions','position_applied_revision_id'];
        v_original:=v_owner->'frozen_facts'-'position_applied_revision_id';
        if w.work_kind='SOURCE' and prev.request_id is not null then
         select x into v_after_book from jsonb_array_elements(prev.owned_source_after) x
          where x->>'component_key'=v_facts#>>'{original_basis,component_key}';
         if v_after_book is null or(v_facts->'source_book_state',v_facts->'source_book_revision',v_facts->'source_binding_version_id')
          is distinct from(v_after_book->'source_book_state',v_after_book->'source_book_revision',v_after_book->'source_binding_version_id') then
          raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
         v_compare:=v_compare-array['source_book_state','source_book_revision'];v_original:=v_original-array['source_book_state','source_book_revision'];end if;
        if v_facts->>'position_applied_revision_id' is distinct from v_revision::text
         or v_compare is distinct from v_original
         or(prev.request_id is null and v_owner#>>'{frozen_facts,position_applied_revision_id}' is distinct from v_revision::text) then
         raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
        v_rows:=v_rows||jsonb_build_array(jsonb_build_object('approved_line_id',v_current->'approved_line_id','rate_family',v_current->'rate_family','rate_code',v_current->'rate_code',
         'intent',null,'expected_decision_id',v_effective,'expected_decision_version',v_effective_version::text));
       end loop;
       v_subrequest:=gen_random_uuid();v_native:=jsonb_build_object('request_id',v_subrequest,'candidate_id',m.candidate_id,'context',v_page_base,
        'works',jsonb_build_array(jsonb_build_object('work_id',m.work_id,'expected_revision_id',v_revision,'rows',v_rows)),'reason','Saved decisions cleared','zero_confirmed',false);
       if octet_length(v_native::text)>32768 then raise exception using errcode='54000',message='BPAY_NEXT_DECISIONS_CLEAR_SCOPE_TOO_LARGE';end if;
       v_receipt:=private.bpay_next_resolution_command_v1(p_actor,'RESOLUTION_CLEAR',v_native,false);
       if v_receipt->>'received' is distinct from 'true' or jsonb_array_length(v_receipt#>'{result,works}')<>1 then
        raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_RECEIPT_INVALID';end if;
       v_sequence:=(v_receipt#>>'{result,works,0,agency_sequence}')::bigint;
       if v_sequence<>o.expected_sequence+1 then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_STALE';end if;
       if w.work_kind='SOURCE' then
        -- Native CLEAR still holds its original Position/Source locks. Preserve only
        -- this accepted page's exact bookkeeping result for its own next family.
        with keys as materialized(
         select distinct fo#>>'{frozen_facts,original_basis,component_key}' component_key from private.bpay_next_decisions_clear_member mm
          cross join lateral jsonb_array_elements(mm.owners) fo where mm.request_id=v_request and mm.work_id=w.id
           and fo#>>'{frozen_facts,original_basis,component_key}' is not null order by component_key limit 201
        ) select coalesce(jsonb_agg(jsonb_build_object('component_key',pos.component_key,'source_book_state',pos.source_book_state,
          'source_book_revision',pos.source_book_revision::text,'source_binding_version_id',b.current_binding_version_id) order by pos.component_key),'[]'::jsonb)
         into v_book_after from keys k join private.bpay_next_position pos on pos.work_id=w.id and pos.component_key=k.component_key
          left join private.bpay_next_source_component_binding b
          on b.canonical_work_id=pos.work_id and b.canonical_component_key=pos.component_key
         ;
        if jsonb_array_length(v_book_after)>200 or octet_length(v_book_after::text)>32768 then
         raise exception using errcode='54000',message='BPAY_NEXT_DECISIONS_CLEAR_SCOPE_TOO_LARGE';end if;end if;
       update private.bpay_next_decisions_clear_operation set state='APPLYING',expected_sequence=v_sequence,cleared_count=cleared_count+v_clear_count,
        cleared_amount=cleared_amount+v_clear_sum where request_id=v_request;
      exception when check_violation or invalid_parameter_value then
       get stacked diagnostics v_reason=MESSAGE_TEXT;
       if v_reason not in('BPAY_NEXT_DECISIONS_CLEAR_STALE','BPAY_NEXT_RESOLUTION_STALE','BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED','BPAY_NEXT_RESOLUTION_BASIS_CHANGED',
        'BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE','BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE') then raise;end if;
       v_code:='CHANGED';v_receipt:=null;v_native:=null;v_subrequest:=null;v_book_after:=null;
      end;
     end if;
    end if;
   end if;
   update private.bpay_next_decisions_clear_operation set state='APPLYING',apply_step=apply_step+1,
    refused_count=refused_count+case when v_code is not null then v_clear_count else 0 end where request_id=v_request;
  else
   -- Original accepted order serialises this future-default reset with Create capture.
   v_sequence:=private.bpay_next_receive_command_v1(v_request,'DECISIONS_CLEAR');
   update private.bpay_next_command set expected_member_count=0,enrolled_member_count=0,status='COMPLETE',sealed_at_utc=transaction_timestamp() where id=v_request;
   update private.bpay_next_decisions_clear_operation set state=case when refused_count>0 then 'PARTIAL' else 'COMPLETE' end,
    apply_step=apply_step+1,reset_sequence=v_sequence,completed_at_utc=transaction_timestamp() where request_id=v_request;
   update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,updated_at_utc=transaction_timestamp() where id=1;
  end if;
 end if;
 v_result:=private.bpay_next_decisions_clear_result_v1(v_request);
 -- The original page receipt must include its just-accepted bounded outcome.
 if v_phase='APPLY' and m.request_id is not null then
  v_result:=jsonb_set(v_result,'{rows}',(select coalesce(jsonb_agg(case when x.value->>'member_no'=m.member_no::text
    then x.value||jsonb_build_object('outcome_code',v_code,'cleared',v_receipt is not null and x.value#>>'{owner,clear_class}'='RESOLVED_AND_CLEARABLE')
    else x.value end order by x.ordinality),'[]'::jsonb) from jsonb_array_elements(v_result->'rows') with ordinality x(value,ordinality)));
 end if;
 v_response:=jsonb_build_object('request_id',v_request,'phase',v_phase,'step',v_step::text,'received',true,'replay',false,'processing',false,'result',v_result);
 if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_DECISIONS_CLEAR_SCOPE_TOO_LARGE';end if;
 insert into private.bpay_next_decisions_clear_page(request_id,phase,step_no,work_id,subrequest_id,request_body,native_body,native_receipt,owned_source_after,outcome_code,result_body)
 values(v_request,v_phase,v_step,m.work_id,v_subrequest,p_args,v_native,v_receipt,v_book_after,v_code,v_response);
 return v_response;
end
$function$;
alter function private.bpay_next_decisions_clear_result_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_decisions_clear_result_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_decisions_clear_command_v1(uuid,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_decisions_clear_command_v1(uuid,jsonb,boolean) from public,anon,authenticated,service_role;

create or replace function public.bpay_next_workbench_command_v1(p_request jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v_actor uuid;v_epoch bigint;v_receipt private.bpay_next_snooze_request%rowtype;v_result jsonb;v_request uuid;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>32768
    or (select count(*) from jsonb_object_keys(p_request))<>4
    or exists(select 1 from jsonb_object_keys(p_request) k where k not in ('actor_user_id','action','args','readback'))
    or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
    or (p_request->>'actor_user_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
    or p_request->>'action' is null or p_request->>'action' not in ('DECISIONS_CLEAR','RESOLUTION_SAVE','RESOLUTION_CLEAR','RETRY_WORK_JOB','SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR','SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY')
    or jsonb_typeof(p_request->'readback') is distinct from 'boolean' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
  if p_request->>'action'='DECISIONS_CLEAR' then
    return private.bpay_next_decisions_clear_command_v1(v_actor,p_request->'args',(p_request->>'readback')::boolean);
  end if;
  if p_request->>'action' in ('RESOLUTION_SAVE','RESOLUTION_CLEAR') then
    return private.bpay_next_resolution_command_v1(v_actor,p_request->>'action',p_request->'args',(p_request->>'readback')::boolean);
  end if;
  if p_request->>'action'<>'RETRY_WORK_JOB' then
    if p_request->>'action' in ('SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY') then
      return private.bpay_next_snooze_many_v1(v_actor,p_request->>'action',p_request->'args',(p_request->>'readback')::boolean);
    end if;
    if jsonb_typeof(p_request->'args') is distinct from 'object' or octet_length((p_request->'args')::text)>4096
      or p_request#>>'{args,request_id}' is null
      or p_request#>>'{args,request_id}'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_REQUEST_INVALID';end if;
    select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
    if v_epoch is null then
      raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
    v_request:=(p_request#>>'{args,request_id}')::uuid;
    select r.* into v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
    if found and not exists(select 1 from private.bpay_next_command c where c.id=v_request and c.module_epoch=v_epoch
      and c.command_kind=p_request->>'action') then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_FINANCIAL_VERSION_STALE';end if;
    if (p_request->>'readback')::boolean then
      select r.* into v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
      if found and (v_receipt.actor_id,v_receipt.action,v_receipt.request_body)
        is distinct from (v_actor,p_request->>'action',p_request->'args') then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';end if;
      v_result:=v_receipt.result_body;
    else
      v_result:=private.bpay_next_snooze_action_v1(v_actor,p_request->>'action',p_request->'args');
    end if;
    if v_result is not null then
      if v_result ? 'version' then v_result:=jsonb_set(v_result,'{version}',to_jsonb(v_result->>'version'));end if;
      if v_result ? 'control_sequence' then v_result:=jsonb_set(v_result,'{control_sequence}',to_jsonb(v_result->>'control_sequence'));end if;
    end if;
    return jsonb_build_object('request_id',v_request,'candidate_id',p_request#>>'{args,candidate_id}',
      'received',v_result is not null,'replay',(p_request->>'readback')::boolean and v_result is not null,'result',v_result);
  end if;
  if octet_length(p_request::text)>4096 then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  if (p_request->>'readback')::boolean then
    return private.bpay_next_retry_work_job_v1(v_actor,p_request->'args',true);
  end if;
  return public.bpay_next_financial_intake_v1(v_actor,'RETRY_WORK_JOB',p_request->'args')->'result';
end
$function$;
alter function public.bpay_next_workbench_command_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_command_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_workbench_command_v1(jsonb) to service_role;

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
  v_discarded_preparation boolean;
begin
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select last_enrolled_sequence into strict v_last
    from private.bpay_next_enrollment_clock where id=1 for update;
  select dr09_row.* into v_command from private.bpay_next_command dr09_row where agency_sequence=v_last+1 for update;
  if not found then
    return null;
  end if;
  -- Native completed snooze controls own no financial job. Advance exactly
  -- the next accepted sequence only after its original immutable receipt.
  if v_command.command_kind in ('SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR')
    and v_command.status='COMPLETE' then
    if v_command.module_epoch is distinct from v_epoch or v_command.expected_member_count is distinct from 0
      or v_command.enrolled_member_count<>0 or v_command.sealed_at_utc is null
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id)
      or not (exists(select 1 from private.bpay_next_snooze_request r
        where r.request_id=v_command.id and r.action=v_command.command_kind
          and (r.request_body->>'request_id')::uuid=r.request_id)
        or private.bpay_next_snooze_due_receipt_valid_v1(v_command.id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ENROLLMENT_RECEIPT_INVALID';
    end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return null;
  end if;
  -- WB-38 parent records frozen native scope; original single child commands own all controls.
  if v_command.command_kind in ('SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY') and v_command.status='COMPLETE' then
    if v_command.module_epoch is distinct from v_epoch or v_command.expected_member_count is distinct from 0
      or v_command.enrolled_member_count<>0 or v_command.sealed_at_utc is null
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id)
      or not exists(select 1 from private.bpay_next_snooze_many_request r
        join private.bpay_next_snooze_many_page p on p.request_id=r.request_id and p.page_no=1
        where r.request_id=v_command.id and r.action=v_command.command_kind and r.module_epoch=v_epoch
          and (r.request_body->>'request_id')::uuid=r.request_id and p.result_body->'received'='true'::jsonb) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ENROLLMENT_RECEIPT_INVALID';end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return null;
  end if;
  -- WB28 terminal future-default reset owns no financial job. Its exact original
  -- immutable terminal page qualifies the next sequence; all older branches stay.
  if v_command.command_kind='DECISIONS_CLEAR' and v_command.status='COMPLETE' then
    if v_command.module_epoch is distinct from v_epoch or v_command.expected_member_count is distinct from 0
      or v_command.enrolled_member_count<>0 or v_command.sealed_at_utc is null
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id)
      or not exists(select 1 from private.bpay_next_decisions_clear_operation o
        join private.bpay_next_decisions_clear_page p on p.request_id=o.request_id and p.phase='APPLY' and p.step_no=o.apply_step-1
        where o.request_id=v_command.id and o.owner_epoch=v_epoch and o.reset_sequence=v_command.agency_sequence
          and o.state in('COMPLETE','PARTIAL') and o.completed_at_utc is not null
          and o.initial_args->>'request_id'=o.request_id::text and p.request_body->>'request_id'=o.request_id::text
          and p.result_body->'received'='true'::jsonb and p.result_body#>>'{result,reset_sequence}'=v_command.agency_sequence::text
          and p.result_body#>>'{result,state}'=o.state) then
      raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_ENROLLMENT_RECEIPT_INVALID';end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return null;
  end if;
  if v_command.command_kind='PREPARE' and exists(select 1 from private.bpay_next_worker_reprepare_request x
      where x.command_id=v_command.id and x.initiator_kind='OFFICE') then
    v_job_id:=private.bpay_next_enroll_worker_action_v1(v_command.id);
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
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
     or not(private.bpay_next_preparation_expiry_fenced_v1(v_run_id,v_command.id)
       or private.bpay_next_preparation_discard_fenced_v1(v_run_id,v_command.id))
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
    select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=v_run_id for share nowait;
    v_expired_preparation:=private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_command.id);
    v_discarded_preparation:=private.bpay_next_preparation_discard_fenced_v1(v_run_id,v_command.id);
    if v_command.module_epoch<>v_epoch
       or v_command.status<>'ENROLLING'
       or (v_run.status<>'PREPARING' and not
           (v_run.status='CANCELLING' and(v_expired_preparation or v_discarded_preparation)))
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
             case when v_expired_preparation or v_discarded_preparation then 'DONE' else 'READY' end,
             case when v_expired_preparation then 'EXPIRED' when v_discarded_preparation then 'DISCARDED' else 'NEW' end,1)
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
  if v_command.command_kind='PREPARATION_DISCARD' then
    -- Exact header fence permits only metadata enrollment here. The helper
    -- retires unfinished original PREPARE jobs and binds ONE cleanup member;
    -- financial holds are released later by its normally ordered worker.
    -- The helper, not this caller, owns member counts and command sealing.
    v_job_id:=private.bpay_next_enroll_preparation_discard_one_v1(v_command.id);
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
  if v_command.command_kind='CASE_CHANNEL_RESOLVE' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CHANNEL_RESOLVE_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_channel_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id 
         and r.module_epoch=v_epoch and r.command_sequence=v_command.agency_sequence) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CHANNEL_RESOLVE_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        v_command.command_kind,'READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_RESTRUCTURE' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_RESTRUCTURE_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_restructure_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id 
         and r.module_epoch=v_epoch and r.command_sequence=v_command.agency_sequence) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_RESTRUCTURE_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        v_command.command_kind,'READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind in ('CASE_PAUSE','CASE_RESUME') then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PAUSE_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_pause_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.action=v_command.command_kind
         and r.module_epoch=v_epoch and r.command_sequence=v_command.agency_sequence) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PAUSE_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        v_command.command_kind,'READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_EDIT' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_edit_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_EDIT','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_CREATE' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
  select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
alter function private.bpay_next_enroll_one_v1() owner to postgres;
revoke all on function private.bpay_next_enroll_one_v1() from public,anon,authenticated,service_role;
notify pgrst, 'reload schema';
commit;
