-- WB29 / BD161/162: closed native W2 SAVE/CLEAR, complete treatment publication.
-- Original immutable request replay precedes fresh context/currentness.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_resolution_command_v1(
 p_actor_id uuid,p_action text,p_args jsonb,p_readback boolean
) returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_request uuid;v_candidate uuid;v_epoch bigint;v_scope jsonb;v_base jsonb;v_read jsonb;
 v_read_works jsonb:='[]';v_read_rows jsonb;v_seen text[];v_key text;
 v_work_ids uuid[];v_revisions uuid[]:='{}';v_cases uuid[]:='{}';v_physical uuid[]:='{}';
 v_umbrellas uuid[]:='{}';v_policies uuid[]:='{}';v_contracts uuid[]:='{}';v_financials uuid[]:='{}';
 v_clients uuid[]:='{}';
 v_origins uuid[]:='{}';v_roots uuid[]:='{}';v_id uuid;v_root uuid;
 v_receipt private.bpay_next_workbench_command_receipt%rowtype;
 w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
 d private.bpay_next_component_resolution%rowtype;pos private.bpay_next_position%rowtype;
 v_new private.bpay_next_component_resolution;v_active record;v_snapshot record;v_seed record;v_native record;
 v_work jsonb;v_row jsonb;v_basis jsonb;v_position jsonb;v_quote jsonb;v_plan jsonb;
 v_carry jsonb:='[]';v_intents jsonb:='[]';v_source jsonb:='[]';v_stage jsonb:='[]';
 v_changes jsonb;v_carried jsonb;v_outputs jsonb:='[]';v_result jsonb;
 v_count integer:=0;v_bytes bigint:=0;v_mapped_count integer:=0;v_mapped_keys text[]:='{}';
 v_map_line uuid;v_new_line uuid;v_revision uuid;v_command uuid;v_sequence bigint;
 v_clear_ids uuid[]:='{}';v_replace_ids uuid[]:='{}';v_zero boolean:=false;v_post jsonb;
begin
 if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
  or p_actor_id is null or not exists(select 1 from public.tms_users u where u.id=p_actor_id and u.is_active and u.role::text='admin') then
  raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
 if p_action is null or p_action not in ('RESOLUTION_SAVE','RESOLUTION_CLEAR') or p_readback is null
  or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
  or not(p_args ?& array['request_id','candidate_id','context','works','reason','zero_confirmed'])
  or p_args-array['request_id','candidate_id','context','works','reason','zero_confirmed']<>'{}'::jsonb
  or jsonb_typeof(p_args->'request_id') is distinct from 'string'
  or p_args->>'request_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  or jsonb_typeof(p_args->'context') is distinct from 'object'
  or jsonb_typeof(p_args->'zero_confirmed') is distinct from 'boolean' then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_REQUEST_INVALID';end if;
 v_scope:=jsonb_build_object('candidate_id',p_args->'candidate_id','works',p_args->'works','reason',p_args->'reason');
 perform private.bpay_next_resolution_scope_validate_v1(v_scope,p_action);
 v_request:=(p_args->>'request_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
 -- No later metadata, epoch, date, book or revision may reinterpret a receipt.
 select x.* into v_receipt from private.bpay_next_workbench_command_receipt x where x.request_id=v_request;
 if v_receipt.request_id is not null then
  if (v_receipt.actor_user_id,v_receipt.action,v_receipt.candidate_id,v_receipt.request_body,v_receipt.module_epoch::text)
   is distinct from (p_actor_id,p_action,v_candidate,p_args,p_args#>>'{context,owner_epoch}') then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_REPLAY_CONFLICT';end if;
  return jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'received',true,'replay',true,'result',v_receipt.result);
 end if;
 if p_readback then return jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'received',false,'replay',false,'result',null);end if;
 select x.owner_epoch into v_epoch from private.bpay_next_module_control x where x.id=1 and x.active_owner='NEXT' for share nowait;
 if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 perform 1 from private.bpay_next_worker_control x where x.candidate_id=v_candidate for update nowait;
 if not found then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,null,null,null);
 -- Receipt may have arrived while this original operation waited for control.
 select x.* into v_receipt from private.bpay_next_workbench_command_receipt x where x.request_id=v_request;
 if v_receipt.request_id is not null then
  if (v_receipt.actor_user_id,v_receipt.action,v_receipt.candidate_id,v_receipt.request_body,v_receipt.module_epoch::text)
   is distinct from (p_actor_id,p_action,v_candidate,p_args,p_args#>>'{context,owner_epoch}') then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_REPLAY_CONFLICT';end if;
  return jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'received',true,'replay',true,'result',v_receipt.result);
 end if;
 select array_agg((x->>'work_id')::uuid order by (x->>'work_id')::uuid) into v_work_ids from jsonb_array_elements(p_args->'works') x;
 perform 1 from private.bpay_next_work x where x.id=any(v_work_ids) order by x.id for update;
 for v_work in select value from jsonb_array_elements(p_args->'works') loop
  select x.* into w from private.bpay_next_work x where x.id=(v_work->>'work_id')::uuid and x.candidate_id=v_candidate;
  select x.* into r from private.bpay_next_work_revision x where x.id=(v_work->>'expected_revision_id')::uuid and x.work_id=w.id;
  if w.id is null or w.approval_state is distinct from 'APPROVED' or r.id is null
   or w.current_revision_id is distinct from r.id or w.applied_revision_id is distinct from r.id
   or w.current_revision_no is distinct from r.revision_no or r.approved_at_utc is null or r.sealed_at_utc is null then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
  v_root:=private.bpay_next_treatment_approval_revision_v1(r.id);
  v_revisions:=v_revisions||array[r.id,v_root];v_roots:=array_append(v_roots,v_root);
  v_origins:=array_append(v_origins,w.original_timesheet_id);
  if w.contract_id is not null then v_contracts:=array_append(v_contracts,w.contract_id);end if;
  v_read_rows:='[]';v_seen:='{}';
  for v_row in select value from jsonb_array_elements(v_work->'rows') loop
   v_key:=jsonb_build_array(v_row->'approved_line_id',v_row->'rate_family',v_row->'rate_code')::text;
   if not(v_key=any(v_seen)) then
    v_seen:=array_append(v_seen,v_key);v_read_rows:=v_read_rows||jsonb_build_array(v_row-array['expected_decision_id','expected_decision_version']);
   end if;
  end loop;
  v_read_works:=v_read_works||jsonb_build_array(v_work||jsonb_build_object('rows',v_read_rows));
 end loop;
 perform 1 from private.bpay_next_work_revision x where x.id=any(v_revisions) order by x.work_id,x.id for share;
 perform private.bpay_next_resolution_revision_census_v1(v_revisions);
 -- Full physical Positions/collection membership, before any semantic filter.
 for v_id in select x from unnest(v_work_ids) x order by x loop
  for pos in select x.* from private.bpay_next_position x where x.work_id=v_id order by x.component_key collate "C" limit 10001 loop
   v_count:=v_count+1;v_bytes:=v_bytes+octet_length(to_jsonb(pos)::text);
   if v_count>10000 or v_bytes>8388608 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  end loop;
  for v_native in select x.* from private.bpay_next_work_collection x where x.work_id=v_id order by x.component_key collate "C" limit 10001 loop
   v_count:=v_count+1;v_bytes:=v_bytes+octet_length(to_jsonb(v_native)::text);
   if v_count>10000 or v_bytes>8388608 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
   if v_native.case_id is not null and not(v_native.case_id=any(v_cases)) then v_cases:=array_append(v_cases,v_native.case_id);end if;
   if cardinality(v_cases)>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  end loop;
 end loop;
 perform 1 from private.bpay_next_position x where x.work_id=any(v_work_ids) order by x.work_id,x.component_key collate "C" for update;
 perform 1 from private.bpay_next_finance_case x where x.id=any(v_cases) order by x.id for update;
 if exists(select 1 from unnest(v_cases) id where not exists(select 1 from private.bpay_next_finance_case c where c.id=id and c.candidate_id=v_candidate)) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 -- The native collection guard fixes Case/Component IDs to collection.id.
 -- Each related Case therefore uses this exact already-censused Work origin.
 if exists(select 1 from unnest(v_cases) id where not exists(select 1 from private.bpay_next_work_collection c
  where c.id=id and c.case_id=id and c.case_component_id=id and c.work_id=any(v_work_ids) and c.candidate_id=v_candidate)) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 perform 1 from private.bpay_next_workbench_task x where x.work_id=any(v_work_ids) and x.task_type='ORIGIN' order by x.task_key collate "C" for update;
 perform private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
 -- Preserve all mapped ACTIVE variants; today's compatibility filters only SAVE's owner.
 for w in select x.* from private.bpay_next_work x where x.id=any(v_work_ids) order by x.id loop
  for d in select x.* from private.bpay_next_component_resolution x where x.work_id=w.id and x.state='ACTIVE'
   and (x.based_on_revision_id=w.current_revision_id or x.published_revision_id=w.current_revision_id) order by x.id limit 101 for update loop
   v_mapped_count:=v_mapped_count+1;
   if v_mapped_count>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
   v_map_line:=case when d.based_on_revision_id=w.current_revision_id then d.approved_line_id else d.published_line_id end;
   v_basis:=private.bpay_next_component_resolution_basis_v1(w.current_revision_id,v_map_line,d.rate_family,d.rate_code);
   if d.candidate_id is distinct from v_candidate or (case when d.based_on_revision_id=w.current_revision_id then d.original_basis else d.published_basis end) is distinct from v_basis then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;
   v_key:=jsonb_build_array(v_map_line,d.rate_family,d.rate_code,d.target_pay_channel,d.target_umbrella_id,d.target_umbrella_vat_chargeable,d.valuation_policy_id,d.valuation_date)::text;
   if v_key=any(v_mapped_keys) or d.decision_version>=9223372036854775806 then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE';end if;
   v_mapped_keys:=array_append(v_mapped_keys,v_key);
   v_carry:=v_carry||jsonb_build_array(jsonb_build_object('decision_id',d.id,'decision_version',d.decision_version::text,
    'work_id',w.id,'approved_line_id',v_map_line,'rate_family',d.rate_family,'rate_code',d.rate_code));
   if d.target_umbrella_id is not null then v_umbrellas:=array_append(v_umbrellas,d.target_umbrella_id);end if;
   v_policies:=array_append(v_policies,d.valuation_policy_id);
   if octet_length(v_carry::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  end loop;
 end loop;
 for r in select x.* from private.bpay_next_work_revision x where x.id=any(v_revisions) loop
  v_physical:=array_append(v_physical,r.physical_timesheet_id);v_policies:=array_append(v_policies,r.valuation_policy_id);
  if r.source_umbrella_id is not null then v_umbrellas:=array_append(v_umbrellas,r.source_umbrella_id);end if;
  if r.financial_snapshot_id is not null then v_financials:=array_append(v_financials,r.financial_snapshot_id);end if;
 end loop;
 -- Strong live qualification under native private order never waits on a public trigger.
 perform 1 from public.tms_users x where x.id=p_actor_id and x.is_active and x.role::text='admin' for share nowait;
 if not found then raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
 perform 1 from public.candidates x where x.id=v_candidate for share nowait;
 select x.umbrella_id into v_id from public.candidates x where x.id=v_candidate and upper(x.pay_method)='UMBRELLA';
 if v_id is not null then v_umbrellas:=array_append(v_umbrellas,v_id);end if;
 perform 1 from public.timesheets x where x.timesheet_id=any(v_physical||v_origins) order by x.timesheet_id for share nowait;
 perform 1 from public.timesheets_financials x where x.id=any(v_financials) order by x.id for share nowait;
 perform 1 from public.contracts x where x.id=any(v_contracts) order by x.id for share nowait;
 select coalesce(array_agg(distinct x.client_id),'{}'::uuid[]) into v_clients from public.contracts x where x.id=any(v_contracts) and x.client_id is not null;
 perform 1 from public.clients x where x.id=any(v_clients) order by x.id for key share nowait;
 perform 1 from public.umbrellas x where x.id=any(v_umbrellas) order by x.id for share nowait;
 perform 1 from private.bpay_next_valuation_policy x where x.id=any(v_policies) order by x.id for key share;
 v_base:=private.bpay_next_resolution_offer_context_v1(p_actor_id,v_candidate);
 if p_args->'context' is distinct from v_base or v_epoch::text is distinct from v_base->>'owner_epoch' then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 v_read:=private.bpay_next_resolution_context_v1(p_actor_id,jsonb_build_object('candidate_id',v_candidate,'works',v_read_works,'reason',p_args->'reason'));
 -- Every requested owner/quote is qualified before seed, stage or publication.
 for v_work in select value from jsonb_array_elements(p_args->'works') loop
  select x.* into w from private.bpay_next_work x where x.id=(v_work->>'work_id')::uuid;
  select x.* into r from private.bpay_next_work_revision x where x.id=w.current_revision_id;
  for v_row in select value from jsonb_array_elements(v_work->'rows') loop
   v_map_line:=(v_row->>'approved_line_id')::uuid;
   v_basis:=private.bpay_next_component_resolution_basis_v1(r.id,v_map_line,v_row->>'rate_family',v_row->>'rate_code');
   select x.* into pos from private.bpay_next_position x where x.work_id=w.id and x.component_key=v_basis->>'component_key';
   if pos.applied_revision_id is distinct from r.id or pos.approved_source_ex_vat is distinct from (v_basis->>'component_source_ex_vat')::numeric then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED';end if;
   if p_action='RESOLUTION_CLEAR' then
    select x.* into d from private.bpay_next_component_resolution x where x.id=(v_row->>'expected_decision_id')::uuid and x.decision_version=(v_row->>'expected_decision_version')::bigint;
    if d.id is null or not exists(select 1 from jsonb_array_elements(v_carry) x where x->>'decision_id'=d.id::text
     and x->>'approved_line_id'=v_map_line::text and x->>'work_id'=w.id::text and x->>'rate_family'=v_row->>'rate_family' and x->>'rate_code'=v_row->>'rate_code') then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
    v_clear_ids:=array_append(v_clear_ids,d.id);
   else
    select x.* into v_active from private.bpay_next_component_resolution_active_v1(w.id,r.id,v_map_line,v_row->>'rate_family',v_row->>'rate_code',
     v_base->>'target_pay_channel',(v_base->>'target_umbrella_id')::uuid,(v_base->>'target_umbrella_vat_chargeable')::boolean,r.valuation_policy_id,(v_base->>'official_pay_date')::date) x;
    if (v_active.id::text,v_active.decision_version::text) is distinct from (v_row->>'expected_decision_id',v_row->>'expected_decision_version') then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
    if v_active.id is not null then v_replace_ids:=array_append(v_replace_ids,v_active.id);end if;
    if private.bpay_next_resolution_live_snooze_v1(w.id,r.id,v_basis->>'component_key') then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
    v_position:=jsonb_build_object('approved_source_ex_vat',pos.approved_source_ex_vat,'realised_source_ex_vat',pos.realised_source_ex_vat,
     'held_source_ex_vat',pos.held_source_ex_vat,'source_basis_channel',pos.source_basis_channel);
    v_new:=null;v_new.work_id:=w.id;v_new.candidate_id:=v_candidate;v_new.based_on_revision_id:=r.id;v_new.approved_line_id:=v_map_line;
    v_new.physical_timesheet_id:=r.physical_timesheet_id;v_new.component_key:=v_basis->>'component_key';
    v_new.rate_family:=v_row->>'rate_family';v_new.rate_code:=v_row->>'rate_code';v_new.original_basis:=v_basis;v_new.original_position_basis:=v_position;
    v_new.target_pay_channel:=v_base->>'target_pay_channel';v_new.target_umbrella_id:=(v_base->>'target_umbrella_id')::uuid;
    v_new.target_umbrella_vat_chargeable:=(v_base->>'target_umbrella_vat_chargeable')::boolean;v_new.valuation_policy_id:=r.valuation_policy_id;
    v_new.valuation_date:=(v_base->>'official_pay_date')::date;v_new.resolution_mode:=(v_row#>>'{intent,resolution_mode}')::public.pay_finance_component_resolution_mode_enum;
    v_new.manual_replacement_rate:=(v_row#>>'{intent,manual_replacement_rate}')::numeric;v_new.manual_amount_ex_vat:=(v_row#>>'{intent,manual_amount_ex_vat}')::numeric;
    v_new.reason:=coalesce(nullif(btrim(p_args->>'reason'),''),'Suggested rate accepted');
    v_new.decision_version:=coalesce(v_active.decision_version+1,1);
    v_quote:=private.bpay_next_component_resolution_input_quote_v1(v_new,r.id,v_position,greatest(pos.approved_source_ex_vat-pos.realised_source_ex_vat-pos.held_source_ex_vat,0));
    v_zero:=v_zero or coalesce(v_new.manual_replacement_rate=0 or v_new.manual_amount_ex_vat=0 or (v_quote#>>'{quote,ex}')::numeric=0,false);
    v_intents:=v_intents||jsonb_build_array(to_jsonb(v_new));
   end if;
  end loop;
 end loop;
 if p_action='RESOLUTION_SAVE' and v_zero and not(p_args->>'zero_confirmed')::boolean then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ZERO_CONFIRMATION_REQUIRED';end if;
 -- Preflight seed every required SOURCE component once, including carried siblings.
 for v_native in select n.* from (select distinct x.work_id,x.component_key,x.current_revision_id from (
  select w.id work_id,l.component_key,w.current_revision_id from private.bpay_next_work w
   join jsonb_array_elements(p_args->'works') wi on wi->>'work_id'=w.id::text
   cross join lateral jsonb_array_elements(wi->'rows') ri
   join private.bpay_next_approved_line l on l.id=(ri->>'approved_line_id')::uuid and l.revision_id=w.current_revision_id
   where w.work_kind='SOURCE'
  union select w.id,d.component_key,w.current_revision_id from private.bpay_next_work w
   join jsonb_array_elements(v_carry) ca on ca->>'work_id'=w.id::text
   join private.bpay_next_component_resolution d on d.id=(ca->>'decision_id')::uuid where w.work_kind='SOURCE'
 ) x) n order by n.work_id,n.component_key collate "C" loop
  v_source:=v_source||jsonb_build_array(private.bpay_next_resolution_source_seed_plan_v1(v_native.work_id,v_native.component_key,v_native.current_revision_id));
 end loop;
 -- Closed result budget before any mutation. Future UUIDs have the same encoded
 -- width as the request UUID; native sequence/revision strings use bigint's
 -- maximum width. These are byte-budget operands, never persisted identities.
 for v_work in select value from jsonb_array_elements(p_args->'works') loop
  v_id:=(v_work->>'work_id')::uuid;v_changes:='[]';v_carried:='[]';
  for v_row in select value from jsonb_array_elements(v_intents) x where x->>'work_id'=v_id::text loop
   v_changes:=v_changes||jsonb_build_array(jsonb_build_object('approved_line_id',v_row->'approved_line_id','rate_family',v_row->'rate_family','rate_code',v_row->'rate_code',
    'decision_id',v_request,'decision_version',v_row->>'decision_version'));
  end loop;
  if p_action='RESOLUTION_CLEAR' then
   for v_row in select value from jsonb_array_elements(v_work->'rows') loop
    v_changes:=v_changes||jsonb_build_array(jsonb_build_object('approved_line_id',v_row->'approved_line_id','rate_family',v_row->'rate_family','rate_code',v_row->'rate_code',
     'decision_id',v_row->'expected_decision_id','decision_version',v_row->'expected_decision_version'));
   end loop;
  end if;
  for v_row in select value from jsonb_array_elements(v_carry) x where x->>'work_id'=v_id::text loop
   if not((v_row->>'decision_id')::uuid=any(v_clear_ids||v_replace_ids)) then
    v_carried:=v_carried||jsonb_build_array(jsonb_build_object('previous_decision_id',v_row->'decision_id','previous_decision_version',v_row->'decision_version',
     'decision_id',v_request,'decision_version',((v_row->>'decision_version')::bigint+1)::text));
   end if;
  end loop;
  v_outputs:=v_outputs||jsonb_build_array(jsonb_build_object('work_id',v_id,'based_on_revision_id',v_work->'expected_revision_id',
   'published_revision_id',v_request,'command_id',v_request,'agency_sequence','9223372036854775807','changes',v_changes,'carried_decisions',v_carried));
  if octet_length(v_outputs::text)>60000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
 end loop;
 v_result:=jsonb_build_object('action',p_action,'candidate_id',v_candidate,'owner_epoch',v_epoch::text,'works',v_outputs,'processing',true,
  'adoption',jsonb_build_object('workbench_revision','9223372036854775807','adoption_revision','9223372036854775807','adoption_state',repeat('x',64)));
 if octet_length(v_result::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
 v_outputs:='[]';v_result:=null;
 -- Retention/FK locks for all Works precede Source's first global refresh.
 perform public.timesheet_financial_retention_mark_v1(v_physical);
 for v_plan in select value from jsonb_array_elements(v_source) loop
  if (v_plan->>'seed_required')::boolean then
   select x.* into strict v_seed from private.bpay_next_source_disposition_seed_v1((v_plan->>'work_id')::uuid,v_plan->>'component_key',
    (v_plan->>'expected_revision_id')::uuid,(v_plan->>'effect_id')::uuid,(v_plan->>'hold_id')::uuid) x;
   if v_seed.qualification_state is distinct from 'READY' then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';end if;
   select x.* into strict v_snapshot from private.bpay_next_bound_source_snapshot_v1((v_plan->>'binding_version_id')::uuid,v_seed.source_book_revision) x;
   if v_snapshot.qualification_state is distinct from 'READY'
    or (v_snapshot.candidate_id,v_snapshot.canonical_work_id,v_snapshot.canonical_component_key) is distinct from (v_candidate,(v_plan->>'work_id')::uuid,v_plan->>'component_key') then
    raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';end if;
  end if;
 end loop;
 -- Complete immutable staging for every Work before publisher1's agency clock.
 for w in select x.* from private.bpay_next_work x where x.id=any(v_work_ids) order by x.id loop
  v_revision:=private.bpay_next_stage_treatment_revision_v1(w.id,w.current_revision_id);
  v_stage:=v_stage||jsonb_build_array(jsonb_build_object('work_id',w.id,'based_on_revision_id',w.current_revision_id,'published_revision_id',v_revision,'command_id',gen_random_uuid()));
 end loop;
 for v_plan in select value from jsonb_array_elements(v_stage) loop
  v_id:=(v_plan->>'work_id')::uuid;v_revision:=(v_plan->>'published_revision_id')::uuid;v_command:=(v_plan->>'command_id')::uuid;
  v_sequence:=private.bpay_next_publish_staged_revision_v1(v_id,v_revision,v_command);v_changes:='[]';v_carried:='[]';
  for v_row in select value from jsonb_array_elements(v_carry) x where x->>'work_id'=v_id::text loop
   select x.* into strict d from private.bpay_next_component_resolution x where x.id=(v_row->>'decision_id')::uuid and x.decision_version=(v_row->>'decision_version')::bigint;
   insert into private.bpay_next_component_resolution_event(event_kind,work_id,candidate_id,revision_id,approved_line_id,rate_family,rate_code,decision_id,decision_version,command_id,actor_user_id,reason)
    values('CLEARED',d.work_id,d.candidate_id,d.based_on_revision_id,d.approved_line_id,d.rate_family,d.rate_code,d.id,d.decision_version,v_command,p_actor_id,
     case when d.id=any(v_clear_ids) then 'Saved rate removed' when d.id=any(v_replace_ids) then 'Saved rate replaced' else 'Saved rate retained on revised payment treatment' end);
   update private.bpay_next_component_resolution set state='CLEARED' where id=d.id;
   if d.id=any(v_clear_ids) or d.id=any(v_replace_ids) then continue;end if;
   v_map_line:=(v_row->>'approved_line_id')::uuid;
   select l.id into strict v_new_line from private.bpay_next_approved_line l where l.revision_id=v_revision and l.component_key=d.component_key;
   v_new:=d;v_new.id:=gen_random_uuid();v_new.decision_version:=d.decision_version+1;v_new.state:='ACTIVE';
   v_new.based_on_revision_id:=(v_plan->>'based_on_revision_id')::uuid;v_new.approved_line_id:=v_map_line;
   v_new.original_basis:=private.bpay_next_component_resolution_basis_v1(v_new.based_on_revision_id,v_map_line,d.rate_family,d.rate_code);
   v_new.published_revision_id:=v_revision;v_new.published_line_id:=v_new_line;
   v_new.published_basis:=private.bpay_next_component_resolution_basis_v1(v_revision,v_new_line,d.rate_family,d.rate_code);
   v_new.command_id:=v_command;v_new.actor_user_id:=p_actor_id;v_new.saved_at_utc:=transaction_timestamp();
   v_new.request_body:=jsonb_build_object('carry',jsonb_build_object('decision_id',d.id,'decision_version',d.decision_version::text,'request_id',v_request));
   insert into private.bpay_next_component_resolution select (v_new).*;
   insert into private.bpay_next_component_resolution_event(event_kind,work_id,candidate_id,revision_id,approved_line_id,rate_family,rate_code,decision_id,decision_version,command_id,actor_user_id,reason)
    values('SAVED',v_new.work_id,v_new.candidate_id,v_new.based_on_revision_id,v_new.approved_line_id,v_new.rate_family,v_new.rate_code,v_new.id,v_new.decision_version,v_command,p_actor_id,'Saved rate retained on revised payment treatment');
   v_carried:=v_carried||jsonb_build_array(jsonb_build_object('previous_decision_id',d.id,'previous_decision_version',d.decision_version::text,'decision_id',v_new.id,'decision_version',v_new.decision_version::text));
  end loop;
  for v_row in select value from jsonb_array_elements(v_intents) x where x->>'work_id'=v_id::text loop
   select * into v_new from jsonb_populate_record(null::private.bpay_next_component_resolution,v_row);
   select l.id into strict v_new_line from private.bpay_next_approved_line l where l.revision_id=v_revision and l.component_key=v_new.component_key;
   v_new.id:=gen_random_uuid();v_new.published_revision_id:=v_revision;v_new.published_line_id:=v_new_line;
   v_new.published_basis:=private.bpay_next_component_resolution_basis_v1(v_revision,v_new_line,v_new.rate_family,v_new.rate_code);
   v_new.command_id:=v_command;v_new.actor_user_id:=p_actor_id;v_new.saved_at_utc:=transaction_timestamp();v_new.state:='ACTIVE';
   v_new.request_body:=jsonb_build_object('request_id',v_request,'action',p_action,'intent',jsonb_build_object('resolution_mode',v_new.resolution_mode,
    'manual_replacement_rate',v_new.manual_replacement_rate::text,'manual_amount_ex_vat',v_new.manual_amount_ex_vat::text));
   insert into private.bpay_next_component_resolution select (v_new).*;
   insert into private.bpay_next_component_resolution_event(event_kind,work_id,candidate_id,revision_id,approved_line_id,rate_family,rate_code,decision_id,decision_version,command_id,actor_user_id,reason)
    values('SAVED',v_new.work_id,v_new.candidate_id,v_new.based_on_revision_id,v_new.approved_line_id,v_new.rate_family,v_new.rate_code,v_new.id,v_new.decision_version,v_command,p_actor_id,v_new.reason);
   v_changes:=v_changes||jsonb_build_array(jsonb_build_object('approved_line_id',v_new.approved_line_id,'rate_family',v_new.rate_family,'rate_code',v_new.rate_code,
    'decision_id',v_new.id,'decision_version',v_new.decision_version::text));
  end loop;
  if p_action='RESOLUTION_CLEAR' then
   for v_row in select value from jsonb_array_elements(p_args->'works') x where x->>'work_id'=v_id::text loop
    for v_work in select value from jsonb_array_elements(v_row->'rows') loop
     v_changes:=v_changes||jsonb_build_array(jsonb_build_object('approved_line_id',v_work->'approved_line_id','rate_family',v_work->'rate_family','rate_code',v_work->'rate_code',
      'decision_id',v_work->'expected_decision_id','decision_version',v_work->'expected_decision_version'));
    end loop;
   end loop;
  end if;
  v_outputs:=v_outputs||jsonb_build_array(v_plan||jsonb_build_object('agency_sequence',v_sequence::text,'changes',v_changes,'carried_decisions',v_carried));
  if octet_length(v_outputs::text)>60000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
 end loop;
 perform private.bpay_next_workbench_refresh_many_v1(v_work_ids,'{}'::uuid[]);
 select jsonb_build_object('workbench_revision',x.workbench_revision::text,'adoption_revision',x.adoption_revision::text,'adoption_state',x.adoption_state)
  into strict v_post from private.bpay_next_workbench_revision x where x.id=1;
 v_result:=jsonb_build_object('action',p_action,'candidate_id',v_candidate,'owner_epoch',v_epoch::text,'works',v_outputs,'processing',true,'adoption',v_post);
 if octet_length(v_result::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
 insert into private.bpay_next_workbench_command_receipt(request_id,actor_user_id,candidate_id,work_id,job_id,module_epoch,request_body,result,action)
  values(v_request,p_actor_id,v_candidate,case when cardinality(v_work_ids)=1 then v_work_ids[1] end,null,v_epoch,p_args,v_result,p_action);
 return jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'received',true,'replay',false,'result',v_result);
end
$function$;

alter function private.bpay_next_resolution_command_v1(uuid,text,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_resolution_command_v1(uuid,text,jsonb,boolean) from public,anon,authenticated,service_role;

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
    or p_request->>'action' is null or p_request->>'action' not in ('RESOLUTION_SAVE','RESOLUTION_CLEAR','RETRY_WORK_JOB','SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR','SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY')
    or jsonb_typeof(p_request->'readback') is distinct from 'boolean' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
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
notify pgrst,'reload schema';
commit;
