-- WB31 / BD172: native single-component review and informational effect facts.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_component_resolution_effect_facts_v1(
 p_revision_id uuid,p_line_id uuid,p_basis jsonb,p_quote jsonb
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare r private.bpay_next_work_revision%rowtype;a private.bpay_next_approved_line%rowtype;
 v_charge numeric;v_pay numeric;v_units numeric;v_rate numeric;v_native jsonb;v_whole boolean;
begin
 if p_quote is null or p_quote='null'::jsonb then return null;end if;
 select q.* into r from private.bpay_next_work_revision q where q.id=p_revision_id and q.work_id=(p_basis->>'work_id')::uuid;
 select q.* into a from private.bpay_next_approved_line q where q.id=p_line_id and q.revision_id=r.id and q.component_key=p_basis->>'component_key';
 if r.id is null or a.id is null or r.approved_at_utc is null or r.sealed_at_utc is null or a.tax_treatment<>'TAXABLE' then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 v_pay:=(p_quote->'quote'->>'ex')::numeric;v_rate:=(p_basis->>'source_charge_rate')::numeric;
 if p_basis->>'semantics'='RATE_BEARING' then
  -- Existing0045 already qualified the exact selected residual units.
  v_units:=(p_quote->>'remaining_units')::numeric;
 elsif p_basis->>'semantics'='AMOUNT_LED' then
  v_whole:=(p_quote->>'source_component_residual_ex_vat')::numeric=a.source_pay_ex_vat
   and (p_quote->>'included_source_component_ex_vat')::numeric=a.source_pay_ex_vat;
  if v_whole then
   if a.component_kind='ADDITIONAL' and a.approved_quantity is not null and v_rate is not null then
    v_units:=a.approved_quantity;
   elsif r.source_head_id is not null and a.source_component_id is not null then
    -- A unique original head/component point, never today's head or TF JSON.
    select h.charge_ex_vat into v_charge from public.weekly_source_entitlement_head_components h
     where h.head_id=r.source_head_id and h.component_id=a.source_component_id
      and h.pay_ex_vat=a.source_pay_ex_vat and not h.exclude_from_pay
      and ((a.component_kind='ADDITIONAL' and h.component_kind='ADDITIONAL_UNIT')
       or (a.component_kind in ('WORK','PROTECTED_WORK') and h.component_kind='WORKED_TIME'));
   end if;
  end if;
 end if;
 if v_units is not null and v_rate is not null then
  if v_units<0 or v_units::text in ('NaN','Infinity','-Infinity') or v_rate<0 or v_rate::text in ('NaN','Infinity','-Infinity') then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
  v_native:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',a.source_pay_ex_vat,'PAYE','PAYE',v_units,v_rate,null,null,null,false);
  v_charge:=(v_native->>'ex')::numeric;
 end if;
 if v_charge is not null and (v_charge<0 or v_charge>=10000000000000000 or v_charge::text in ('NaN','Infinity','-Infinity')) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_QUOTE_RANGE_INVALID';end if;
 return jsonb_build_object('pay_ex_vat',v_pay::text,'charge_ex_vat',v_charge::text,
  'margin_ex_vat',(v_charge-v_pay)::text,'charge_state',case when v_charge is null then 'UNAVAILABLE' else 'APPROVED' end);
end
$function$;

create or replace function private.bpay_next_component_resolution_review_v1(p_actor_id uuid,p_scope jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
 receipt private.bpay_next_pay_method_change_receipt%rowtype;n record;x record;
 v_scope jsonb;v_read jsonb;v_base jsonb;v_one jsonb;v_row jsonb;v_link jsonb;v_signature jsonb;v_need jsonb;
 v_excluded jsonb:='[]';v_seen text[]:='{}';v_count integer:=0;v_events integer;v_members integer:=0;v_root_position jsonb;v_match_basis jsonb;
 v_reason text;v_code text;v_key text;v_index integer;v_response jsonb;
begin
 if jsonb_typeof(p_scope) is distinct from 'object' or octet_length(p_scope::text)>32768
  or (select count(*) from jsonb_object_keys(p_scope))<>6
  or not(p_scope ?& array['candidate_id','work_id','expected_revision_id','approved_line_id','rate_family','rate_code']) then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_REQUEST_INVALID';end if;
 v_scope:=jsonb_build_object('candidate_id',p_scope->'candidate_id','works',jsonb_build_array(jsonb_build_object(
  'work_id',p_scope->'work_id','expected_revision_id',p_scope->'expected_revision_id','rows',jsonb_build_array(jsonb_build_object(
   'approved_line_id',p_scope->'approved_line_id','rate_family',p_scope->'rate_family','rate_code',p_scope->'rate_code','intent',null)))),'reason',null);
 -- Original reader owns typed identity/current/applied/physical/Position/snooze qualification.
 v_read:=private.bpay_next_resolution_context_v1(p_actor_id,v_scope);v_base:=v_read->'context';v_row:=v_read->'works'->0->'rows'->0;
 select q.* into w from private.bpay_next_work q where q.id=(p_scope->>'work_id')::uuid and q.candidate_id=(p_scope->>'candidate_id')::uuid;
 select q.* into r from private.bpay_next_work_revision q where q.id=(p_scope->>'expected_revision_id')::uuid and q.work_id=w.id;
 if not exists(select 1 from private.bpay_next_workbench_item i join private.bpay_next_workbench_group g on g.group_key=i.group_key
 where i.work_id=w.id and i.revision_id=r.id and i.approved_line_id=(p_scope->>'approved_line_id')::uuid
 and i.candidate_id=w.candidate_id and i.portion_kind='AVAILABLE' and i.direction='PAYMENT' and i.amount_known and i.included_source_ex_vat>0
 and ((i.section='ACTION' and i.reason_code in ('RATE_RESOLUTION_REQUIRED','GROSS_RESOLUTION_REQUIRED')) or (i.section='READY' and v_row->'saved_decision'<>'null'::jsonb))
 and i.progress_state='CURRENT' and i.accepted_run_id is null and not i.is_selected and not exists(select 1 from private.bpay_next_run_choice_member cm
 join private.bpay_next_pay_run pr on pr.id=cm.run_id and pr.accepted_choice_descriptor is not null
 left join private.bpay_next_run_worker rw on rw.run_id=cm.run_id and rw.candidate_id=cm.candidate_id
 where cm.candidate_id=w.candidate_id and cm.work_id=i.work_id and cm.expected_revision_id=i.revision_id
  and cm.member_kind='WORK' and cm.selected and cm.skip_code is null
  and pr.status in ('PREPARING','REVIEW','DRAFT','EXECUTING') and(rw.id is null or rw.status not in ('CANCELLED','COMPLETE'))
  and(cm.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component sc where sc.run_id=cm.run_id and sc.work_id=cm.work_id and sc.approved_line_id=i.approved_line_id)
   or exists(select 1 from jsonb_array_elements(pr.accepted_choice_descriptor#>'{intent,components}') dl where dl->>'work_id'=i.work_id::text and dl->>'approved_line_id'=i.approved_line_id::text and dl->'selected'='true'::jsonb)))
 and g.complete and not g.dirty and g.progress_state='CURRENT' and g.current_revision_id=r.id
 and g.financial_view_revision=(v_base->>'financial_view_revision')::bigint and g.metadata_version=(v_base->>'metadata_version')::bigint
 and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision and i.metadata_version=g.metadata_version) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 v_reason:=case when p_scope->>'rate_family'='STANDARD' then 'RATE_RESOLUTION_REQUIRED' else 'GROSS_RESOLUTION_REQUIRED' end;
 v_root_position:=v_read#>'{context,works,0,rows,0,position_basis}';
 v_signature:=jsonb_build_object('rate_family',v_row->>'rate_family','rate_code',v_row->>'rate_code',
 'component_kind',v_row#>'{original_basis,component_kind}','tax_treatment',v_row#>'{original_basis,tax_treatment}',
 'source_kind',v_row#>'{original_basis,source_kind}','source_pay_channel',v_row#>'{original_basis,source_pay_channel}',
 'source_umbrella_id',v_row#>'{original_basis,source_umbrella_id}','valuation_policy_id',v_row#>'{original_basis,valuation_policy_id}',
 'unit_label',v_row#>'{original_basis,unit_label}','semantics',v_row#>'{original_basis,semantics}',
 'source_rate',v_row#>'{original_basis,source_rate}','source_charge_rate',v_row#>'{original_basis,source_charge_rate}',
 'available_rates',v_row->'available_rates','valuation_window_id',v_row#>'{suggestion,valuation_window_id}',
 'suggested_rate',v_row#>'{suggestion,quote,rate}',
 'amount_position',case when v_row#>>'{original_basis,semantics}'='AMOUNT_LED' then v_root_position end,
 'amount_quantity',case when v_row#>>'{original_basis,semantics}'='AMOUNT_LED' then v_row#>'{original_basis,source_units}' end,
 'amount_basis',case when v_row#>>'{original_basis,semantics}'='AMOUNT_LED' then jsonb_build_array(
  v_row#>'{original_basis,component_source_ex_vat}',v_row#>'{suggestion,source_component_residual_ex_vat}',
  v_row#>'{suggestion,included_source_component_ex_vat}',v_row#>'{suggestion,quote,ex}',v_row#>'{effect,suggestion,charge_ex_vat}') end);v_count:=1;
 select q.* into receipt from private.bpay_next_pay_method_change_receipt q where q.candidate_id=w.candidate_id order by q.post_metadata_version desc limit 1;
 select count(*) into v_events from(select e.id from private.bpay_next_component_resolution_event e where e.need_operation_id=receipt.operation_id and e.event_kind='NEED'
  order by e.need_member_key,e.approved_line_id,e.rate_family,e.rate_code,e.need_version limit 5001) finite;
 if v_events>5000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
 for n in select m.* from private.bpay_next_pay_method_change_member m where m.operation_id=receipt.operation_id order by m.member_key limit 1001 loop
  v_members:=v_members+1;if v_members>1000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  if n.work_id is null or n.work_id=w.id or n.member_kind not in ('WORK','PROTECTED_WORK') or n.classification not in ('NEEDS_DECISION','PROTECTED') then continue;end if;
  if not exists(select 1 from private.bpay_next_work lw where lw.id=n.work_id and lw.candidate_id=w.candidate_id and lw.contract_id is not distinct from w.contract_id) then continue;end if;
  if n.classification='PROTECTED' then
   -- Protected members have no NEED event. Classify only their actual accepted exact component.
   if not exists(select 1 from private.bpay_next_run_choice_member cm
    join private.bpay_next_pay_run pr on pr.id=cm.run_id and pr.accepted_choice_descriptor is not null
    left join private.bpay_next_run_worker rw on rw.run_id=cm.run_id and rw.candidate_id=cm.candidate_id
    join private.bpay_next_approved_line fl on fl.revision_id=cm.expected_revision_id and fl.id=n.approved_line_id
    where cm.candidate_id=w.candidate_id and cm.work_id=n.work_id and cm.expected_revision_id=n.revision_id and cm.run_id=n.run_id
     and cm.member_kind='WORK' and cm.selected and cm.skip_code is null and pr.status in ('PREPARING','REVIEW','DRAFT','EXECUTING')
     and(rw.id is null or rw.status not in ('CANCELLED','COMPLETE'))
     and(cm.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component sc where sc.run_id=cm.run_id and sc.work_id=cm.work_id and sc.approved_line_id=fl.id)
      or exists(select 1 from jsonb_array_elements(pr.accepted_choice_descriptor#>'{intent,components}') dl where dl->>'work_id'=cm.work_id::text and dl->>'approved_line_id'=fl.id::text and dl->'selected'='true'::jsonb))
     and fl.tax_treatment='TAXABLE' and((p_scope->>'rate_family'='STANDARD' and fl.component_kind in ('WORK','PROTECTED_WORK')
      and exists(select 1 from private.bpay_next_rate_detail fd where fd.approved_line_id=fl.id and fd.bucket=p_scope->>'rate_code' and fd.approved_hours>0))
      or(p_scope->>'rate_family'='ADDITIONAL' and fl.component_kind='ADDITIONAL' and fl.component_key='ADDITIONAL:'||(p_scope->>'rate_code'))
      or(p_scope->>'rate_family'='ADJUSTMENT' and fl.component_kind='ADJUSTMENT' and fl.component_key=p_scope->>'rate_code'))) then continue;end if;
   begin v_match_basis:=private.bpay_next_component_resolution_basis_v1(n.revision_id,n.approved_line_id,p_scope->>'rate_family',p_scope->>'rate_code');
   exception when check_violation then get stacked diagnostics v_code=MESSAGE_TEXT;
    if v_code<>'BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE' then raise;end if;continue;end;
   if v_match_basis->>'rate_family'=p_scope->>'rate_family' and v_match_basis->>'rate_code'=p_scope->>'rate_code'
 and(v_match_basis->>'component_kind',v_match_basis->>'tax_treatment',v_match_basis->>'source_kind',v_match_basis->>'source_pay_channel',v_match_basis->>'source_umbrella_id',v_match_basis->>'valuation_policy_id',v_match_basis->>'unit_label',v_match_basis->>'semantics')
 is not distinct from(v_row#>>'{original_basis,component_kind}',v_row#>>'{original_basis,tax_treatment}',v_row#>>'{original_basis,source_kind}',v_row#>>'{original_basis,source_pay_channel}',v_row#>>'{original_basis,source_umbrella_id}',v_row#>>'{original_basis,valuation_policy_id}',v_row#>>'{original_basis,unit_label}',v_row#>>'{original_basis,semantics}')
 and (v_match_basis->>'source_rate')::numeric is not distinct from(v_row#>>'{original_basis,source_rate}')::numeric
 and (v_match_basis->>'source_charge_rate')::numeric is not distinct from(v_row#>>'{original_basis,source_charge_rate}')::numeric
 and(v_match_basis->>'semantics'<>'AMOUNT_LED' or((v_match_basis->>'component_source_ex_vat')::numeric is not distinct from(v_row#>>'{original_basis,component_source_ex_vat}')::numeric
  and(v_match_basis->>'source_units')::numeric is not distinct from(v_row#>>'{original_basis,source_units}')::numeric)) and not n.work_id::text=any(v_seen) then
    v_excluded:=v_excluded||jsonb_build_array(jsonb_build_object('work_id',n.work_id,'expected_revision_id',n.revision_id,'reason','IN_DRAFT'));v_seen:=array_append(v_seen,n.work_id::text);end if;
   continue;
  end if;
  select count(*) into v_events from(select e.id from private.bpay_next_component_resolution_event e where e.need_operation_id=n.operation_id and e.need_member_key=n.member_key and e.event_kind='NEED' limit 6) finite;
  if v_events>5 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  if exists(select 1 from private.bpay_next_component_resolution_event e where e.need_operation_id=n.operation_id and e.need_member_key=n.member_key and e.event_kind='NEED'
   and(e.need_version<>receipt.post_metadata_version or e.candidate_id<>n.candidate_id or e.work_id<>n.work_id or e.revision_id<>n.revision_id or e.approved_line_id<>n.approved_line_id)) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
  for x in select lw.current_revision_id as revision_id,a.id as line_id,seg.id as segment_id,e.rate_family,e.rate_code
   from private.bpay_next_work lw join private.bpay_next_approved_line old_a on old_a.id=n.approved_line_id and old_a.revision_id=n.revision_id
   join private.bpay_next_approved_line a on a.revision_id=lw.current_revision_id and a.component_key=old_a.component_key and a.tax_treatment='TAXABLE'
   left join private.bpay_next_approved_segment old_seg on old_seg.id=n.approved_segment_id and old_seg.revision_id=n.revision_id and old_seg.approved_line_id=old_a.id
   left join private.bpay_next_approved_segment seg on seg.revision_id=lw.current_revision_id and seg.approved_line_id=a.id and seg.component_key=old_seg.component_key and seg.segment_key=old_seg.segment_key
   join private.bpay_next_component_resolution_event e on e.need_operation_id=n.operation_id and e.need_member_key=n.member_key and e.event_kind='NEED'
    and e.need_version=receipt.post_metadata_version and e.candidate_id=n.candidate_id and e.work_id=n.work_id and e.revision_id=n.revision_id and e.approved_line_id=n.approved_line_id
    and e.rate_family=p_scope->>'rate_family' and e.rate_code=p_scope->>'rate_code'
   where lw.id=n.work_id and(n.approved_segment_id is null or seg.id is not null)
    and(e.rate_family<>'STANDARD' or exists(select 1 from private.bpay_next_rate_detail rd where rd.approved_line_id=a.id and rd.bucket=e.rate_code and rd.approved_hours>0))
    and(n.approved_segment_id is null or e.rate_family<>'STANDARD' or case e.rate_code when 'DAY' then seg.hours_day when 'NIGHT' then seg.hours_night when 'SAT' then seg.hours_sat when 'SUN' then seg.hours_sun when 'BH' then seg.hours_bh end>0)
   order by a.id,e.rate_family,e.rate_code,seg.id limit 6 loop
   v_key:=jsonb_build_array(n.work_id,x.line_id,x.rate_family,x.rate_code)::text;if v_key=any(v_seen) then continue;end if;
   v_need:=private.bpay_next_component_resolution_need_v1(n.work_id,x.revision_id,x.line_id,x.segment_id,v_base->>'target_pay_channel',
    (v_base->>'target_umbrella_id')::uuid,(v_base->>'target_umbrella_vat_chargeable')::boolean,(v_base->>'official_pay_date')::date);
   if v_need->>'reason_code' is distinct from v_reason then continue;end if;
   v_one:=jsonb_build_object('candidate_id',w.candidate_id,'works',jsonb_build_array(jsonb_build_object('work_id',n.work_id,'expected_revision_id',x.revision_id,
    'rows',jsonb_build_array(jsonb_build_object('approved_line_id',x.line_id,'rate_family',x.rate_family,'rate_code',x.rate_code,'intent',null)))),'reason',null);
   v_code:=null;begin v_one:=private.bpay_next_resolution_context_v1(p_actor_id,v_one);
   exception when check_violation then get stacked diagnostics v_code=MESSAGE_TEXT;
    if v_code not in ('BPAY_NEXT_RESOLUTION_STALE','BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED') then raise;end if;end;
   if v_code is not null then
    begin v_match_basis:=private.bpay_next_component_resolution_basis_v1(x.revision_id,x.line_id,x.rate_family,x.rate_code);
    exception when check_violation then get stacked diagnostics v_code=MESSAGE_TEXT;
     if v_code<>'BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE' then raise;end if;continue;end;
    if v_match_basis->>'rate_family'=p_scope->>'rate_family' and v_match_basis->>'rate_code'=p_scope->>'rate_code'
 and(v_match_basis->>'component_kind',v_match_basis->>'tax_treatment',v_match_basis->>'source_kind',v_match_basis->>'source_pay_channel',v_match_basis->>'source_umbrella_id',v_match_basis->>'valuation_policy_id',v_match_basis->>'unit_label',v_match_basis->>'semantics')
 is not distinct from(v_row#>>'{original_basis,component_kind}',v_row#>>'{original_basis,tax_treatment}',v_row#>>'{original_basis,source_kind}',v_row#>>'{original_basis,source_pay_channel}',v_row#>>'{original_basis,source_umbrella_id}',v_row#>>'{original_basis,valuation_policy_id}',v_row#>>'{original_basis,unit_label}',v_row#>>'{original_basis,semantics}')
 and (v_match_basis->>'source_rate')::numeric is not distinct from(v_row#>>'{original_basis,source_rate}')::numeric
 and (v_match_basis->>'source_charge_rate')::numeric is not distinct from(v_row#>>'{original_basis,source_charge_rate}')::numeric
 and(v_match_basis->>'semantics'<>'AMOUNT_LED' or((v_match_basis->>'component_source_ex_vat')::numeric is not distinct from(v_row#>>'{original_basis,component_source_ex_vat}')::numeric
  and(v_match_basis->>'source_units')::numeric is not distinct from(v_row#>>'{original_basis,source_units}')::numeric)) and not n.work_id::text=any(v_seen) then
     v_excluded:=v_excluded||jsonb_build_array(jsonb_build_object('work_id',n.work_id,'expected_revision_id',x.revision_id,'reason','CHANGED'));v_seen:=array_append(v_seen,n.work_id::text);end if;continue;end if;
   v_link:=v_one->'works'->0->'rows'->0;
   if jsonb_build_object('rate_family',v_link->>'rate_family','rate_code',v_link->>'rate_code',
 'component_kind',v_link#>'{original_basis,component_kind}','tax_treatment',v_link#>'{original_basis,tax_treatment}',
 'source_kind',v_link#>'{original_basis,source_kind}','source_pay_channel',v_link#>'{original_basis,source_pay_channel}',
 'source_umbrella_id',v_link#>'{original_basis,source_umbrella_id}','valuation_policy_id',v_link#>'{original_basis,valuation_policy_id}',
 'unit_label',v_link#>'{original_basis,unit_label}','semantics',v_link#>'{original_basis,semantics}',
 'source_rate',v_link#>'{original_basis,source_rate}','source_charge_rate',v_link#>'{original_basis,source_charge_rate}',
 'available_rates',v_link->'available_rates','valuation_window_id',v_link#>'{suggestion,valuation_window_id}',
 'suggested_rate',v_link#>'{suggestion,quote,rate}',
 'amount_position',case when v_link#>>'{original_basis,semantics}'='AMOUNT_LED' then v_one#>'{context,works,0,rows,0,position_basis}' end,
 'amount_quantity',case when v_link#>>'{original_basis,semantics}'='AMOUNT_LED' then v_link#>'{original_basis,source_units}' end,
 'amount_basis',case when v_link#>>'{original_basis,semantics}'='AMOUNT_LED' then jsonb_build_array(
  v_link#>'{original_basis,component_source_ex_vat}',v_link#>'{suggestion,source_component_residual_ex_vat}',
  v_link#>'{suggestion,included_source_component_ex_vat}',v_link#>'{suggestion,quote,ex}',v_link#>'{effect,suggestion,charge_ex_vat}') end) is distinct from v_signature or v_link->'saved_decision'<>'null'::jsonb then continue;end if;
   if not exists(select 1 from private.bpay_next_workbench_item i join private.bpay_next_workbench_group g on g.group_key=i.group_key
 where i.work_id=n.work_id and i.revision_id=x.revision_id and i.approved_line_id=x.line_id and i.approved_segment_id is not distinct from x.segment_id
 and i.candidate_id=w.candidate_id and i.portion_kind='AVAILABLE' and i.direction='PAYMENT' and i.amount_known and i.included_source_ex_vat>0
 and i.section='ACTION' and i.reason_code=v_reason
 and i.progress_state='CURRENT' and i.accepted_run_id is null and not i.is_selected and not exists(select 1 from private.bpay_next_run_choice_member cm
 join private.bpay_next_pay_run pr on pr.id=cm.run_id and pr.accepted_choice_descriptor is not null
 left join private.bpay_next_run_worker rw on rw.run_id=cm.run_id and rw.candidate_id=cm.candidate_id
 where cm.candidate_id=w.candidate_id and cm.work_id=i.work_id and cm.expected_revision_id=i.revision_id
  and cm.member_kind='WORK' and cm.selected and cm.skip_code is null
  and pr.status in ('PREPARING','REVIEW','DRAFT','EXECUTING') and(rw.id is null or rw.status not in ('CANCELLED','COMPLETE'))
  and(cm.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component sc where sc.run_id=cm.run_id and sc.work_id=cm.work_id and sc.approved_line_id=i.approved_line_id)
   or exists(select 1 from jsonb_array_elements(pr.accepted_choice_descriptor#>'{intent,components}') dl where dl->>'work_id'=i.work_id::text and dl->>'approved_line_id'=i.approved_line_id::text and dl->'selected'='true'::jsonb)))
 and g.complete and not g.dirty and g.progress_state='CURRENT' and g.current_revision_id=x.revision_id
 and g.financial_view_revision=(v_base->>'financial_view_revision')::bigint and g.metadata_version=(v_base->>'metadata_version')::bigint
 and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision and i.metadata_version=g.metadata_version) then
    if not n.work_id::text=any(v_seen) then v_excluded:=v_excluded||jsonb_build_array(jsonb_build_object('work_id',n.work_id,'expected_revision_id',x.revision_id,'reason','UNAVAILABLE'));v_seen:=array_append(v_seen,n.work_id::text);end if;continue;end if;
   v_seen:=array_append(v_seen,v_key);v_count:=v_count+1;if v_count>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
   select ordinality::integer-1 into v_index from jsonb_array_elements(v_scope->'works') with ordinality where value->>'work_id'=n.work_id::text;
   if v_index is null then v_scope:=jsonb_set(v_scope,'{works}',(v_scope->'works')||jsonb_build_array(jsonb_build_object('work_id',n.work_id,'expected_revision_id',x.revision_id,
    'rows',jsonb_build_array(jsonb_build_object('approved_line_id',x.line_id,'rate_family',x.rate_family,'rate_code',x.rate_code,'intent',null)))));
   else v_scope:=jsonb_set(v_scope,array['works',v_index::text,'rows'],(v_scope->'works'->v_index->'rows')||jsonb_build_array(jsonb_build_object('approved_line_id',x.line_id,'rate_family',x.rate_family,'rate_code',x.rate_code,'intent',null)));end if;
  end loop;
 end loop;
 if receipt.operation_id is not null and v_members<>receipt.affected_member_count then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 -- Root must own this exact need, rather than another bucket's line-level issue.
 if v_row->'saved_decision'='null'::jsonb and not exists(
  select 1 from private.bpay_next_pay_method_change_member m
  join private.bpay_next_work_revision mr on mr.id=m.revision_id and mr.work_id=m.work_id and mr.sealed_at_utc is not null
  join private.bpay_next_approved_line old_a on old_a.id=m.approved_line_id and old_a.revision_id=m.revision_id
  join private.bpay_next_approved_line a on a.id=(p_scope->>'approved_line_id')::uuid and a.revision_id=r.id and a.component_key=old_a.component_key
  left join private.bpay_next_approved_segment old_s on old_s.id=m.approved_segment_id and old_s.revision_id=m.revision_id and old_s.approved_line_id=old_a.id
  left join private.bpay_next_approved_segment actual_s on actual_s.revision_id=r.id and actual_s.approved_line_id=a.id and actual_s.component_key=old_s.component_key and actual_s.segment_key=old_s.segment_key
  join private.bpay_next_component_resolution_event e on e.need_operation_id=m.operation_id and e.need_member_key=m.member_key and e.event_kind='NEED'
   and e.need_version=receipt.post_metadata_version and e.candidate_id=m.candidate_id and e.work_id=m.work_id and e.revision_id=m.revision_id and e.approved_line_id=m.approved_line_id
   and e.rate_family=p_scope->>'rate_family' and e.rate_code=p_scope->>'rate_code'
  join private.bpay_next_workbench_item i on i.work_id=w.id and i.revision_id=r.id and i.approved_line_id=a.id
   and ((m.approved_segment_id is null and i.approved_segment_id is null) or(actual_s.id is not null and i.approved_segment_id=actual_s.id))
  join private.bpay_next_workbench_group g on g.group_key=i.group_key
  where m.operation_id=receipt.operation_id and m.candidate_id=w.candidate_id and m.work_id=w.id and m.member_kind='WORK' and m.classification='NEEDS_DECISION'
   and(receipt.target_pay_channel,receipt.target_umbrella_id) is not distinct from(v_base->>'target_pay_channel',(v_base->>'target_umbrella_id')::uuid)
   and(case when mr.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(mr.id) else mr.id end)=private.bpay_next_treatment_approval_revision_v1(r.id)
   and i.candidate_id=w.candidate_id and i.section='ACTION' and i.reason_code=v_reason and i.portion_kind='AVAILABLE' and i.direction='PAYMENT'
   and i.amount_known and i.included_source_ex_vat>0 and i.progress_state='CURRENT' and i.accepted_run_id is null and not i.is_selected
   and g.complete and not g.dirty and g.progress_state='CURRENT' and g.current_revision_id=r.id
   and g.financial_view_revision=(v_base->>'financial_view_revision')::bigint and g.metadata_version=(v_base->>'metadata_version')::bigint
   and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision and i.metadata_version=g.metadata_version
   and(p_scope->>'rate_family'<>'STANDARD' or exists(select 1 from private.bpay_next_rate_detail rd where rd.approved_line_id=a.id and rd.bucket=p_scope->>'rate_code' and rd.approved_hours>0))
   and(m.approved_segment_id is null or p_scope->>'rate_family'<>'STANDARD' or case p_scope->>'rate_code' when 'DAY' then actual_s.hours_day when 'NIGHT' then actual_s.hours_night when 'SAT' then actual_s.hours_sat when 'SUN' then actual_s.hours_sun when 'BH' then actual_s.hours_bh end>0)
 ) then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 if octet_length(v_scope::text)>30000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
 v_read:=private.bpay_next_resolution_context_v1(p_actor_id,v_scope);
 select coalesce(jsonb_agg(value||jsonb_build_object('partly_included',exists(select 1 from jsonb_array_elements(v_scope->'works') sw where sw->>'work_id'=value->>'work_id'))),'[]'::jsonb) into v_excluded from jsonb_array_elements(v_excluded);
 v_response:=v_read||jsonb_build_object('component_review',jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'approved_line_id',p_scope->'approved_line_id',
  'rate_family',p_scope->'rate_family','rate_code',p_scope->'rate_code','linked_count',(jsonb_array_length(v_scope->'works')-1)::text,'excluded',v_excluded,'complete',true,
  'header',jsonb_build_object('candidate_display_name',r.candidate_display_name,'candidate_reference',r.candidate_reference,'client_display_name',r.client_display_name,
   'timesheet_reference',r.timesheet_reference,'week_ending_date',w.week_ending_date,'job_title',r.job_title,'band_label',r.band_label)));
 if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;return v_response;
end
$function$;

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

alter function private.bpay_next_component_resolution_effect_facts_v1(uuid,uuid,jsonb,jsonb) owner to postgres;
revoke all on function private.bpay_next_component_resolution_effect_facts_v1(uuid,uuid,jsonb,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_component_resolution_review_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_component_resolution_review_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_resolution_context_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_resolution_context_v1(uuid,jsonb) from public,anon,authenticated,service_role;
commit;
