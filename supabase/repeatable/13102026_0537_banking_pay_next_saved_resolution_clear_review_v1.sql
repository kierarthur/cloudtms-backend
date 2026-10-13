-- Repeatable CloudTMS function/view authority: banking_pay_next_saved_resolution_clear_review_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- WB33: saved-owner discovery only. Original W1 quotes and W2 publication own money.
create or replace function private.bpay_next_saved_resolution_clear_review_v1(p_actor_id uuid,p_scope jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare
 i private.bpay_next_workbench_item%rowtype;g private.bpay_next_workbench_group%rowtype;
 w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
 root_w private.bpay_next_work%rowtype;root_r private.bpay_next_work_revision%rowtype;
 d private.bpay_next_component_resolution%rowtype;n record;
 v_base jsonb;v_items uuid[]:='{}';v_seen uuid[]:='{}';v_root_signatures jsonb:='[]';
 v_scope jsonb;v_works jsonb:='[]';v_owners jsonb:='[]';v_excluded jsonb:='[]';v_excluded_ids uuid[]:='{}';
 v_one jsonb;v_read jsonb;v_row jsonb;v_facts jsonb;v_native jsonb;v_signature jsonb;v_quote jsonb;
 v_amount text;v_code text;v_line uuid;v_phase integer;v_index integer;v_key text;
 v_count integer:=0;v_rows integer:=0;v_physical integer:=0;v_bytes bigint:=0;v_root_count integer:=0;
 v_frozen boolean;v_eligible boolean;v_reason text;v_owner_physical integer;
 v_projection_current boolean;v_immutable_basis jsonb;v_immutable_signature jsonb;
begin
 if jsonb_typeof(p_scope) is distinct from 'object' or octet_length(p_scope::text)>32768
  or (select count(*) from jsonb_object_keys(p_scope))<>5
  or not(p_scope ?& array['candidate_id','work_id','expected_revision_id','owner_family','owner_id'])
  or jsonb_typeof(p_scope->'candidate_id') is distinct from 'string' or jsonb_typeof(p_scope->'work_id') is distinct from 'string'
  or jsonb_typeof(p_scope->'expected_revision_id') is distinct from 'string'
  or jsonb_typeof(p_scope->'owner_family') is distinct from 'string' or p_scope->>'owner_family' not in('WORK_BUCKET','GROSS')
  or jsonb_typeof(p_scope->'owner_id') not in('null','string') then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_REQUEST_INVALID';end if;
 v_base:=private.bpay_next_resolution_offer_context_v1(p_actor_id,(p_scope->>'candidate_id')::uuid);
 select x.* into root_w from private.bpay_next_work x where x.id=(p_scope->>'work_id')::uuid and x.candidate_id=(p_scope->>'candidate_id')::uuid;
 select x.* into root_r from private.bpay_next_work_revision x where x.id=(p_scope->>'expected_revision_id')::uuid and x.work_id=root_w.id;
 if root_w.id is null or root_r.id is null or(root_w.current_revision_id,root_w.applied_revision_id) is distinct from(root_r.id,root_r.id) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 -- Bound the indexed physical prefix before any semantic filtering or root-first ordering.
 for i in select x.* from private.bpay_next_workbench_item x where x.candidate_id=root_w.candidate_id order by x.section,x.id limit 1001 loop
  v_physical:=v_physical+1;v_bytes:=v_bytes+octet_length(to_jsonb(i)::text);
  if v_physical>1000 or v_bytes>1048576 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  v_items:=array_append(v_items,i.id);
 end loop;
 -- Root signatures are complete before linked owners are considered.
 for v_phase in 0..1 loop
  for i in select x.* from private.bpay_next_workbench_item x where x.id=any(v_items)
   and((v_phase=0 and x.work_id=root_w.id) or(v_phase=1 and x.work_id<>root_w.id)) order by x.section,x.id loop
   if i.work_id is null or i.approved_line_id is null or i.portion_kind not in('AVAILABLE','RESERVED') or i.direction<>'PAYMENT'
    or not i.amount_known or i.included_source_ex_vat<=0 or i.tax_treatment is distinct from 'TAXABLE'
    or i.section not in('ACTION','READY','WAITING') or i.progress_state<>'CURRENT' then continue;end if;
   select x.* into w from private.bpay_next_work x where x.id=i.work_id and x.candidate_id=root_w.candidate_id;
   select x.* into r from private.bpay_next_work_revision x where x.id=i.revision_id and x.work_id=w.id;
   select x.* into g from private.bpay_next_workbench_group x where x.group_key=i.group_key and x.work_id=w.id;
   v_reason:=null;
   if w.id is null or r.id is null or w.contract_id is distinct from root_w.contract_id then
    if v_phase=0 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;continue;
   end if;
   v_projection_current:=g.group_key is not null and g.complete and not g.dirty and g.progress_state='CURRENT'
    and(w.current_revision_id,w.applied_revision_id,g.current_revision_id) is not distinct from(r.id,r.id,r.id)
    and(g.financial_view_revision,g.metadata_version) is not distinct from((v_base->>'financial_view_revision')::bigint,(v_base->>'metadata_version')::bigint)
    and(i.projection_revision,i.financial_view_revision,i.metadata_version) is not distinct from(g.projection_revision,g.financial_view_revision,g.metadata_version);
   if v_phase=0 and not v_projection_current then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
   v_frozen:=i.accepted_run_id is not null or i.is_selected or exists(
    select 1 from private.bpay_next_run_choice_member cm join private.bpay_next_pay_run pr on pr.id=cm.run_id and pr.accepted_choice_descriptor is not null
    left join private.bpay_next_run_worker rw on rw.run_id=cm.run_id and rw.candidate_id=cm.candidate_id
    where cm.candidate_id=i.candidate_id and cm.work_id=i.work_id and cm.expected_revision_id=i.revision_id and cm.member_kind='WORK' and cm.selected
     and cm.skip_code is null and pr.status in('PREPARING','REVIEW','DRAFT','EXECUTING') and(rw.id is null or rw.status not in('CANCELLED','COMPLETE'))
     and(cm.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component sc where sc.run_id=cm.run_id and sc.work_id=cm.work_id and sc.approved_line_id=i.approved_line_id)
      or exists(select 1 from jsonb_array_elements(pr.accepted_choice_descriptor#>'{intent,components}') dl where dl->>'work_id'=cm.work_id::text and dl->>'approved_line_id'=i.approved_line_id::text and dl->'selected'='true'::jsonb)));
   v_owner_physical:=0;
   for d in select x.* from private.bpay_next_component_resolution x where x.work_id=w.id and x.state='ACTIVE'
    and(case when x.rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end)=p_scope->>'owner_family'
    and((x.based_on_revision_id=r.id and x.approved_line_id=i.approved_line_id) or(x.published_revision_id=r.id and x.published_line_id=i.approved_line_id))
    and(v_phase=1 or p_scope->'owner_id'='null'::jsonb or x.id=(p_scope->>'owner_id')::uuid) order by x.id limit 101 loop
    v_owner_physical:=v_owner_physical+1;
    if v_owner_physical>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
    if d.id=any(v_seen) then continue;end if;v_seen:=array_append(v_seen,d.id);v_reason:=null;
    if cardinality(v_seen)>1000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
    if v_phase=1 then
     -- Positively match this saved owner's immutable mapped tuple before reporting exclusions.
     -- Current quote/Position facts are still required separately for every eligible linked member.
     v_immutable_basis:=case when d.based_on_revision_id=r.id then d.original_basis else d.published_basis end;
     v_immutable_signature:=jsonb_build_object('contract_id',w.contract_id,'rate_family',d.rate_family,'rate_code',d.rate_code,
      'component_kind',v_immutable_basis->>'component_kind','tax_treatment',v_immutable_basis->>'tax_treatment',
      'source_kind',v_immutable_basis->>'source_kind','source_pay_channel',v_immutable_basis->>'source_pay_channel',
      'source_umbrella_id',v_immutable_basis->>'source_umbrella_id','valuation_policy_id',v_immutable_basis->>'valuation_policy_id',
      'unit_label',v_immutable_basis->>'unit_label','semantics',v_immutable_basis->>'semantics',
      'source_rate',v_immutable_basis->>'source_rate','source_charge_rate',v_immutable_basis->>'source_charge_rate',
      'target_pay_channel',d.target_pay_channel,'target_umbrella_id',d.target_umbrella_id,'target_umbrella_vat_chargeable',d.target_umbrella_vat_chargeable,
      'owner_policy_id',d.valuation_policy_id,'owner_valuation_date',d.valuation_date,'resolution_mode',d.resolution_mode,
      'manual_replacement_rate',d.manual_replacement_rate::text,'manual_amount_ex_vat',d.manual_amount_ex_vat::text,
      'source_amount',case when v_immutable_basis->>'semantics'='AMOUNT_LED' then v_immutable_basis->>'component_source_ex_vat' end,
      'amount_quantity',case when v_immutable_basis->>'semantics'='AMOUNT_LED' then v_immutable_basis->>'source_units' end);
     if not exists(select 1 from jsonb_array_elements(v_root_signatures) x
      where x-array['available_rates','valuation_window_id','suggested_rate','amount_position','amount_basis']=v_immutable_signature) then continue;end if;
    end if;
    if not v_projection_current then
     if not w.id=any(v_excluded_ids) then v_excluded_ids:=array_append(v_excluded_ids,w.id);
      v_excluded:=v_excluded||jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'reason','CHANGED'));end if;continue;
    end if;
    if v_frozen and v_phase=0 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
    if not v_frozen and(i.portion_kind<>'AVAILABLE' or i.section not in('ACTION','READY')) then continue;end if;
     v_line:=i.approved_line_id;
     v_native:=jsonb_build_object('approved_line_id',v_line,'rate_family',d.rate_family,'rate_code',d.rate_code,'intent',null);
     v_scope:=jsonb_build_object('candidate_id',root_w.candidate_id,'works',jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'rows',jsonb_build_array(v_native))),'reason',null);
     begin
      v_one:=private.bpay_next_resolution_context_v1(p_actor_id,v_scope);
      v_row:=v_one#>'{works,0,rows,0}';v_facts:=v_one#>'{context,works,0,rows,0}';
      if not exists(select 1 from jsonb_array_elements(v_facts->'decisions') x where x->>'decision_id'=d.id::text and x->>'decision_version'=d.decision_version::text) then
       raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
      v_quote:=null;v_code:=null;v_amount:=null;
      begin
       v_quote:=private.bpay_next_component_resolution_quote_v1(d.id,d.decision_version,r.id,null);
       v_amount:=v_quote#>>'{quote,ex}';
       if v_amount is null then v_code:='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
      exception when check_violation then
       get stacked diagnostics v_code=MESSAGE_TEXT;
       if v_code not in('BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE') then raise;end if;
      end;
      -- Same literal rate semantics as WB30; amount-led members also retain WB31's full position/quantity basis.
      v_signature:=jsonb_build_object('contract_id',w.contract_id,'rate_family',d.rate_family,'rate_code',d.rate_code,
       'component_kind',v_row#>'{original_basis,component_kind}','tax_treatment',v_row#>'{original_basis,tax_treatment}',
       'source_kind',v_row#>'{original_basis,source_kind}','source_pay_channel',v_row#>'{original_basis,source_pay_channel}',
       'source_umbrella_id',v_row#>'{original_basis,source_umbrella_id}','valuation_policy_id',v_row#>'{original_basis,valuation_policy_id}',
       'unit_label',v_row#>'{original_basis,unit_label}','semantics',v_row#>'{original_basis,semantics}',
       'source_rate',v_row#>'{original_basis,source_rate}','source_charge_rate',v_row#>'{original_basis,source_charge_rate}',
       'available_rates',v_row->'available_rates','valuation_window_id',v_row#>'{suggestion,valuation_window_id}',
       'suggested_rate',v_row#>'{suggestion,quote,rate}','target_pay_channel',d.target_pay_channel,'target_umbrella_id',d.target_umbrella_id,
       'target_umbrella_vat_chargeable',d.target_umbrella_vat_chargeable,'owner_policy_id',d.valuation_policy_id,'owner_valuation_date',d.valuation_date,
       'resolution_mode',d.resolution_mode,'manual_replacement_rate',d.manual_replacement_rate::text,'manual_amount_ex_vat',d.manual_amount_ex_vat::text,
       'source_amount',case when v_row#>>'{original_basis,semantics}'='AMOUNT_LED' then v_row#>'{original_basis,component_source_ex_vat}' end,
       'amount_position',case when v_row#>>'{original_basis,semantics}'='AMOUNT_LED' then v_facts->'position_basis' end,
       'amount_quantity',case when v_row#>>'{original_basis,semantics}'='AMOUNT_LED' then v_row#>'{original_basis,source_units}' end,
       'amount_basis',case when v_row#>>'{original_basis,semantics}'='AMOUNT_LED' then jsonb_build_array(v_row#>'{original_basis,component_source_ex_vat}',
        v_row#>'{suggestion,source_component_residual_ex_vat}',v_row#>'{suggestion,included_source_component_ex_vat}',v_row#>'{suggestion,quote,ex}',v_row#>'{effect,suggestion,charge_ex_vat}') end);
      if v_frozen then
       -- Frozen exclusions match immutable native semantics; reservation changes R/H, not membership.
       -- This relaxed comparison is used only for exclusions, never to admit a linked Clear owner.
       if not exists(select 1 from jsonb_array_elements(v_root_signatures) x
        where x-array['amount_position','amount_basis','valuation_window_id','suggested_rate']
         =v_signature-array['amount_position','amount_basis','valuation_window_id','suggested_rate']) then continue;end if;
       v_reason:='IN_DRAFT';
      else
      if v_phase=1 and not exists(select 1 from jsonb_array_elements(v_root_signatures) x where x=v_signature) then continue;end if;
      v_eligible:=v_amount is not null and v_code is null;
      if v_phase=1 and not v_eligible then v_reason:='UNAVAILABLE';
      else
       if v_phase=0 then v_root_count:=v_root_count+1;
        if v_eligible then v_root_signatures:=v_root_signatures||jsonb_build_array(v_signature);end if;
       end if;
       v_count:=v_count+1;if v_count>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
       select ordinality::integer-1 into v_index from jsonb_array_elements(v_works) with ordinality where value->>'work_id'=w.id::text;
       if v_index is null then v_works:=v_works||jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'rows',jsonb_build_array(v_native)));
       elsif not exists(select 1 from jsonb_array_elements(v_works->v_index->'rows') x where x=v_native) then
        v_works:=jsonb_set(v_works,array[v_index::text,'rows'],(v_works->v_index->'rows')||jsonb_build_array(v_native));
       elsif exists(select 1 from jsonb_array_elements(v_owners) x where x->>'work_id'=w.id::text and x->>'approved_line_id'=v_line::text and x->>'rate_family'=d.rate_family and x->>'rate_code'=d.rate_code) then
        raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE';
       end if;
       v_owners:=v_owners||jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'approved_line_id',v_line,
        'rate_family',d.rate_family,'rate_code',d.rate_code,'owner_family',p_scope->'owner_family','owner_id',d.id,'owner_version',d.decision_version::text,
        'linked',v_phase=1,'signature',v_signature,'amount_ex_vat',v_amount,'amount_unavailable_code',v_code,
        'clear_class',case when v_eligible then 'RESOLVED_AND_CLEARABLE' else 'NOT_CLEARABLE' end,
        'client_display_name',r.client_display_name,'timesheet_reference',r.timesheet_reference,'week_ending_date',w.week_ending_date,
        'target_pay_channel',d.target_pay_channel,'target_umbrella_id',d.target_umbrella_id));
       if octet_length(v_works::text)>30000 or octet_length(v_owners::text)>100000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
      end if;
      end if;
     exception when check_violation then
      get stacked diagnostics v_code=MESSAGE_TEXT;
      if v_phase=0 or v_code not in('BPAY_NEXT_RESOLUTION_STALE','BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED','BPAY_NEXT_RESOLUTION_BASIS_CHANGED','BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE') then raise;end if;
      v_reason:='CHANGED';
     end;
    if v_reason is not null and not w.id=any(v_excluded_ids) then
     v_excluded_ids:=array_append(v_excluded_ids,w.id);v_excluded:=v_excluded||jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'reason',v_reason));
    end if;
   end loop;
  end loop;
 end loop;
 if v_root_count=0 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 v_read:=private.bpay_next_resolution_context_v1(p_actor_id,jsonb_build_object('candidate_id',root_w.candidate_id,'works',v_works,'reason',null));
 v_read:=v_read||jsonb_build_object('clear_review',p_scope||jsonb_build_object('complete',true,
  'header',jsonb_build_object('candidate_display_name',root_r.candidate_display_name,'candidate_reference',root_r.candidate_reference),
  'owners',v_owners,'excluded',v_excluded));
 if octet_length(v_read::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
 return v_read;
end
$function$;
alter function private.bpay_next_saved_resolution_clear_review_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_saved_resolution_clear_review_v1(uuid,jsonb) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_resolution_context_v1(p_actor_id uuid,p_scope jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare
 w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;pos private.bpay_next_position%rowtype;
 c public.contracts%rowtype;t public.timesheets%rowtype;s private.bpay_next_rate_schedule%rowtype;
 d private.bpay_next_component_resolution%rowtype;v_compatible record;v_rate record;
 v_intent private.bpay_next_component_resolution;v_current_basis jsonb;v_position jsonb;v_base jsonb;
 v_works jsonb:='[]';v_contexts jsonb:='[]';v_rows jsonb;v_row_contexts jsonb;
 v_work jsonb;v_row jsonb;v_decisions jsonb;v_incompatible jsonb;v_expected jsonb;v_available jsonb;
 v_suggestion jsonb;v_preview jsonb;v_saved jsonb;v_ref jsonb;v_work_context jsonb;v_response jsonb;
 v_code text;v_preview_code text;v_saved_code text;v_date date;v_owner_count integer:=0;v_row_count integer;
 v_pos_residual numeric;v_zero boolean;v_daily boolean;v_basis_ok boolean;v_saved_quote jsonb;v_quote_ok boolean;
 v_decimal_objects jsonb;v_decimal_object jsonb;v_decimal_quote jsonb;v_decimal_member record;
 v_effect jsonb;
begin
 if jsonb_typeof(p_scope)='object' and (select count(*) from jsonb_object_keys(p_scope))=1 and p_scope ? 'clear_review' then
  return private.bpay_next_saved_resolution_clear_review_v1(p_actor_id,p_scope->'clear_review');
 end if;
 if jsonb_typeof(p_scope)='object' and (select count(*) from jsonb_object_keys(p_scope))=1 and p_scope ? 'channel_review' then
  return private.bpay_next_channel_resolution_review_v1(p_actor_id,p_scope->'channel_review');
 end if;
 if jsonb_typeof(p_scope)='object' and (select count(*) from jsonb_object_keys(p_scope))=1 and p_scope ? 'component_review' then
  return private.bpay_next_component_resolution_review_v1(p_actor_id,p_scope->'component_review');
 end if;
 if jsonb_typeof(p_scope)='object' and (select count(*) from jsonb_object_keys(p_scope))=1 and p_scope ? 'bucket_review' then
  return private.bpay_next_bucket_resolution_review_v1(p_actor_id,p_scope->'bucket_review');
 elsif jsonb_typeof(p_scope)='object' and (select count(*) from jsonb_object_keys(p_scope))=1 and p_scope ? 'bucket_evidence' then
  return private.bpay_next_bucket_resolution_evidence_v1(p_actor_id,p_scope->'bucket_evidence');
 end if;
 perform private.bpay_next_resolution_scope_validate_v1(p_scope,'READ');
 v_base:=private.bpay_next_resolution_offer_context_v1(p_actor_id,(p_scope->>'candidate_id')::uuid);
 v_date:=(v_base->>'official_pay_date')::date;
 for v_work in select value from jsonb_array_elements(p_scope->'works') loop
  select x.* into w from private.bpay_next_work x where x.id=(v_work->>'work_id')::uuid and x.candidate_id=(p_scope->>'candidate_id')::uuid;
  select x.* into r from private.bpay_next_work_revision x where x.id=(v_work->>'expected_revision_id')::uuid and x.work_id=w.id;
  select x.* into t from public.timesheets x where x.timesheet_id=r.physical_timesheet_id;
  if w.id is null or r.id is null or w.approval_state is distinct from 'APPROVED' or r.approved_at_utc is null or r.sealed_at_utc is null
   or w.current_revision_id is distinct from r.id or w.applied_revision_id is distinct from r.id
   or t.is_current is distinct from true or t.version is distinct from r.physical_timesheet_version or t.booking_id is distinct from w.booking_id
   or t.contract_id is distinct from w.contract_id then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
  select x.* into c from public.contracts x where x.id=w.contract_id;
  v_daily:=w.origin_kind='DAILY' or (r.daily_approval_evidence_json->>'contract_version'='BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
    and r.daily_approval_evidence_json->>'sheet_scope'='DAILY');
  v_work_context:=jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'applied_revision_id',w.applied_revision_id,
   'revision_no',r.revision_no::text,'physical_timesheet_id',r.physical_timesheet_id,'physical_timesheet_version',r.physical_timesheet_version::text,
   'original_timesheet_id',w.original_timesheet_id,'contract_id',w.contract_id,'week_ending_date',w.week_ending_date,
   'valuation_policy_id',r.valuation_policy_id,'source_pay_channel',r.source_pay_channel,'source_umbrella_id',r.source_umbrella_id);
  v_rows:='[]';v_row_contexts:='[]';
  for v_row in select value from jsonb_array_elements(v_work->'rows') loop
   v_current_basis:=private.bpay_next_component_resolution_basis_v1(r.id,(v_row->>'approved_line_id')::uuid,v_row->>'rate_family',v_row->>'rate_code');
   select x.* into pos from private.bpay_next_position x where x.work_id=w.id and x.component_key=v_current_basis->>'component_key';
   if pos.applied_revision_id is distinct from r.id or pos.approved_source_ex_vat is distinct from (v_current_basis->>'component_source_ex_vat')::numeric then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED';end if;
   v_position:=jsonb_build_object('approved_source_ex_vat',pos.approved_source_ex_vat,'realised_source_ex_vat',pos.realised_source_ex_vat,
    'held_source_ex_vat',pos.held_source_ex_vat,'source_basis_channel',pos.source_basis_channel);
   v_pos_residual:=greatest(pos.approved_source_ex_vat-pos.realised_source_ex_vat-pos.held_source_ex_vat,0);
   select x.* into s from private.bpay_next_rate_schedule x where x.revision_id=r.id and x.rate_family=v_row->>'rate_family' and x.rate_code=v_row->>'rate_code';
   v_available:=jsonb_build_object('provenance','UNAVAILABLE','paye_rate',null,'umbrella_rate',null,'charge_rate',null,'unit_label',null);
   if v_daily then
    if r.daily_approval_evidence_json->>'contract_version' is distinct from 'BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
     or r.daily_approval_evidence_json->>'sheet_scope' is distinct from 'DAILY' then
     raise exception using errcode='23514',message='BPAY_NEXT_DAILY_REVISION_EVIDENCE_REQUIRED';end if;
    v_available:=jsonb_build_object('provenance','APPROVAL_FROZEN_DAILY','paye_rate',s.paye_rate,'umbrella_rate',s.umbrella_rate,
     'charge_rate',s.charge_rate,'unit_label',s.unit_label);
   elsif c.id is not null and v_row->>'rate_family' in ('STANDARD','ADDITIONAL') then
    if octet_length(jsonb_build_array(c.rates_json,c.bucket_labels_json,c.additional_rates_json)::text)>65536 then
     raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
    for v_rate in select x.* from private.bpay_next_contract_rate_rows_v1(coalesce(c.rates_json,'{}'::jsonb),c.bucket_labels_json,c.additional_rates_json,c.mileage_pay_rate,c.mileage_charge_rate) x
      where x.rate_family=v_row->>'rate_family' and x.rate_code=v_row->>'rate_code' loop
     v_available:=jsonb_build_object('provenance','CURRENT_CONTRACT','paye_rate',v_rate.paye_rate,'umbrella_rate',v_rate.umbrella_rate,
      'charge_rate',v_rate.charge_rate,'unit_label',v_rate.unit_label);
    end loop;
   end if;
   select x.* into v_compatible from private.bpay_next_component_resolution_active_v1(w.id,r.id,(v_row->>'approved_line_id')::uuid,
    v_row->>'rate_family',v_row->>'rate_code',v_base->>'target_pay_channel',(v_base->>'target_umbrella_id')::uuid,
    (v_base->>'target_umbrella_vat_chargeable')::boolean,r.valuation_policy_id,v_date) x;
   v_expected:=case when v_compatible.id is null then 'null'::jsonb else jsonb_build_object('decision_id',v_compatible.id,'decision_version',v_compatible.decision_version::text) end;
   v_decisions:='[]';v_incompatible:='[]';v_saved:=null;v_row_count:=0;
   for d in select x.* from private.bpay_next_component_resolution x where x.work_id=w.id and x.state='ACTIVE'
    and x.rate_family=v_row->>'rate_family' and x.rate_code=v_row->>'rate_code'
    and ((x.based_on_revision_id=r.id and x.approved_line_id=(v_row->>'approved_line_id')::uuid)
     or (x.published_revision_id=r.id and x.published_line_id=(v_row->>'approved_line_id')::uuid)) order by x.id limit 101 loop
    v_owner_count:=v_owner_count+1;v_row_count:=v_row_count+1;
    if v_row_count>100 or v_owner_count>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
    v_basis_ok:=(case when d.based_on_revision_id=r.id then d.original_basis else d.published_basis end) is not distinct from v_current_basis;
    if d.candidate_id is distinct from w.candidate_id or not v_basis_ok then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;
    v_ref:=jsonb_build_object('decision_id',d.id,'decision_version',d.decision_version::text,'target_pay_channel',d.target_pay_channel,
     'target_umbrella_id',d.target_umbrella_id,'target_umbrella_vat_chargeable',d.target_umbrella_vat_chargeable,
     'valuation_policy_id',d.valuation_policy_id,'valuation_date',d.valuation_date,'resolution_mode',d.resolution_mode,
     'manual_replacement_rate',d.manual_replacement_rate::text,'manual_amount_ex_vat',d.manual_amount_ex_vat::text,'reason',d.reason,'clearable',true);
    v_decisions:=v_decisions||jsonb_build_array(v_ref);
    if d.id is not distinct from v_compatible.id then v_saved:=v_ref;
    else v_incompatible:=v_incompatible||jsonb_build_array(v_ref);end if;
   end loop;
   v_intent:=null;v_intent.work_id:=w.id;v_intent.candidate_id:=w.candidate_id;v_intent.based_on_revision_id:=r.id;
   v_intent.approved_line_id:=(v_row->>'approved_line_id')::uuid;v_intent.physical_timesheet_id:=r.physical_timesheet_id;
   v_intent.component_key:=v_current_basis->>'component_key';v_intent.rate_family:=v_row->>'rate_family';v_intent.rate_code:=v_row->>'rate_code';
   v_intent.original_basis:=v_current_basis;v_intent.original_position_basis:=v_position;v_intent.valuation_policy_id:=r.valuation_policy_id;
   v_intent.valuation_date:=v_date;v_intent.target_pay_channel:=v_base->>'target_pay_channel';
   v_intent.target_umbrella_id:=(v_base->>'target_umbrella_id')::uuid;v_intent.target_umbrella_vat_chargeable:=(v_base->>'target_umbrella_vat_chargeable')::boolean;
   v_intent.resolution_mode:='SUGGESTED_EQUIVALENT_BASIS';v_suggestion:=null;v_preview:=null;v_saved_quote:=null;
   v_code:=null;v_preview_code:=null;v_saved_code:=null;v_zero:=false;
   v_quote_ok:=not private.bpay_next_resolution_live_snooze_v1(w.id,r.id,v_intent.component_key);
   if v_quote_ok then
    begin v_suggestion:=private.bpay_next_component_resolution_input_quote_v1(v_intent,r.id,v_position,v_pos_residual);
    exception when check_violation then
     get stacked diagnostics v_code=MESSAGE_TEXT;
     if v_code not in ('BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE') then raise;end if;
    end;
    if v_compatible.id is not null then
     begin v_saved_quote:=private.bpay_next_component_resolution_quote_v1(v_compatible.id,v_compatible.decision_version,r.id,null);
     exception when check_violation then
      get stacked diagnostics v_saved_code=MESSAGE_TEXT;
      if v_saved_code not in ('BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE') then raise;end if;
     end;
    end if;
    if v_row->'intent'<>'null'::jsonb then
     v_intent.resolution_mode:=(v_row->'intent'->>'resolution_mode')::public.pay_finance_component_resolution_mode_enum;
     v_intent.manual_replacement_rate:=(v_row->'intent'->>'manual_replacement_rate')::numeric;
     v_intent.manual_amount_ex_vat:=(v_row->'intent'->>'manual_amount_ex_vat')::numeric;v_intent.reason:=p_scope->>'reason';
     begin v_preview:=private.bpay_next_component_resolution_input_quote_v1(v_intent,r.id,v_position,v_pos_residual);
     exception when check_violation then
      get stacked diagnostics v_preview_code=MESSAGE_TEXT;
      if v_preview_code not in ('BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE') then raise;end if;
     end;
     v_zero:=v_preview is not null and (v_intent.manual_replacement_rate=0 or v_intent.manual_amount_ex_vat=0 or (v_preview->'quote'->>'ex')::numeric=0);
     v_zero:=coalesce(v_zero,false);
    end if;
   else v_code:='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';v_preview_code:=case when v_row->'intent'<>'null'::jsonb then v_code end;
    v_saved_code:=case when v_saved is not null then v_code end;
   end if;
   v_effect:=jsonb_build_object('suggestion',private.bpay_next_component_resolution_effect_facts_v1(r.id,v_intent.approved_line_id,v_current_basis,v_suggestion),
    'saved',private.bpay_next_component_resolution_effect_facts_v1(r.id,v_intent.approved_line_id,v_current_basis,v_saved_quote),
    'preview',private.bpay_next_component_resolution_effect_facts_v1(r.id,v_intent.approved_line_id,v_current_basis,v_preview));
   -- BD160: finite DTO projection only, after all native numeric qualification.
   -- JavaScript never rounds native pennies, units, rates or decision versions.
   v_decimal_objects:='[]';
   for v_decimal_object in select value from jsonb_array_elements(jsonb_build_array(v_current_basis,v_position,v_available,v_suggestion,v_preview,v_saved_quote)) loop
    if jsonb_typeof(v_decimal_object)='object' then
     for v_decimal_member in select key,value from jsonb_each(v_decimal_object) loop
      if jsonb_typeof(v_decimal_member.value)='number' then
       v_decimal_object:=jsonb_set(v_decimal_object,array[v_decimal_member.key],to_jsonb(v_decimal_member.value#>>'{}'));
      end if;
     end loop;
     if jsonb_typeof(v_decimal_object->'quote')='object' then
      v_decimal_quote:=v_decimal_object->'quote';
      for v_decimal_member in select key,value from jsonb_each(v_decimal_quote) loop
       if jsonb_typeof(v_decimal_member.value)='number' then
        v_decimal_quote:=jsonb_set(v_decimal_quote,array[v_decimal_member.key],to_jsonb(v_decimal_member.value#>>'{}'));
       end if;
      end loop;
      v_decimal_object:=jsonb_set(v_decimal_object,'{quote}',v_decimal_quote);
     end if;
    end if;
    v_decimal_objects:=v_decimal_objects||jsonb_build_array(v_decimal_object);
   end loop;
   v_current_basis:=v_decimal_objects->0;v_position:=v_decimal_objects->1;v_available:=v_decimal_objects->2;
   v_suggestion:=nullif(v_decimal_objects->3,'null'::jsonb);v_preview:=nullif(v_decimal_objects->4,'null'::jsonb);v_saved_quote:=nullif(v_decimal_objects->5,'null'::jsonb);
   v_row_contexts:=v_row_contexts||jsonb_build_array(jsonb_build_object('approved_line_id',v_intent.approved_line_id,
    'rate_family',v_intent.rate_family,'rate_code',v_intent.rate_code,'original_basis',v_current_basis,'position_basis',v_position,
    'position_applied_revision_id',pos.applied_revision_id,'source_book_state',pos.source_book_state,'source_book_revision',pos.source_book_revision::text,
    'source_binding_version_id',(select b.current_binding_version_id from private.bpay_next_source_component_binding b where b.canonical_work_id=w.id and b.canonical_component_key=pos.component_key),
    'available_rates',v_available,'decisions',v_decisions));
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('approved_line_id',v_intent.approved_line_id,'rate_family',v_intent.rate_family,'rate_code',v_intent.rate_code,
    'original_basis',v_current_basis,'available_rates',v_available,'suggestion',v_suggestion,'suggestion_unavailable_code',v_code,
    'expected_decision',v_expected,'saved_decision',v_saved,'saved_quote',v_saved_quote,'saved_unavailable_code',v_saved_code,
    'incompatible_decisions',v_incompatible,'preview',v_preview,'preview_unavailable_code',v_preview_code,'requires_zero_confirmation',v_zero,'effect',v_effect));
   if octet_length(jsonb_build_array(v_contexts,v_works,v_row_contexts,v_rows)::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  end loop;
  v_contexts:=v_contexts||jsonb_build_array(v_work_context||jsonb_build_object('rows',v_row_contexts));
  v_works:=v_works||jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'rows',v_rows));
 end loop;
 v_response:=jsonb_build_object('ok',true,'kind','RESOLUTION_CONTEXT','context',v_base||jsonb_build_object('works',v_contexts),'works',v_works);
 if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
 return v_response;
end
$function$;

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
 v_clear_family text;v_row_family text;
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
    -- Actual native family, after both original-receipt replay checks and owner qualification.
    v_row_family:=case when d.rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end;
    if v_clear_family is not null and v_clear_family<>v_row_family then
     raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_REQUEST_INVALID';end if;
    v_clear_family:=v_row_family;
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
 if p_action='RESOLUTION_CLEAR' then v_result:=v_result||jsonb_build_object('clear_class','RESOLVED_AND_CLEARABLE');end if;
 if octet_length(v_result::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
 insert into private.bpay_next_workbench_command_receipt(request_id,actor_user_id,candidate_id,work_id,job_id,module_epoch,request_body,result,action)
  values(v_request,p_actor_id,v_candidate,case when cardinality(v_work_ids)=1 then v_work_ids[1] end,null,v_epoch,p_args,v_result,p_action);
 return jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'received',true,'replay',false,'result',v_result);
end
$function$;
alter function private.bpay_next_resolution_context_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_resolution_context_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_resolution_command_v1(uuid,text,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_resolution_command_v1(uuid,text,jsonb,boolean) from public,anon,authenticated,service_role;

commit;
