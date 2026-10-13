-- WB29: current native selected operands and unchanged captured/scalar authority; one1157 body.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_component_resolution_input_quote_v1(
 p_intent private.bpay_next_component_resolution,p_boundary_revision_id uuid,
 p_position_basis jsonb,p_included_source_ex_vat numeric,p_run_position_id uuid,p_through_segment_key text,p_current_working jsonb
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 d private.bpay_next_component_resolution:=p_intent;
 b jsonb;v_line uuid;win private.bpay_next_valuation_policy_window%rowtype;
 v_approved numeric;v_realised numeric;v_held numeric;v_residual numeric;
 v_units numeric;v_full_units numeric;v_rate numeric;v_source numeric;
 v_full boolean;v_buckets integer;v_source_sum numeric:=0;v_bucket_source numeric;
 rd private.bpay_next_rate_detail%rowtype;v_native jsonb;v_quote jsonb;v_window_count bigint;
 rp private.bpay_next_run_position%rowtype;worker private.bpay_next_run_worker%rowtype;
 v_current boolean:=p_current_working is not null;
 pos private.bpay_next_position%rowtype;cw private.bpay_next_work%rowtype;cr private.bpay_next_work_revision%rowtype;
 ca private.bpay_next_approved_line%rowtype;cc public.candidates%rowtype;cu public.umbrellas%rowtype;
 v_scope_work uuid;v_scope_candidate uuid;v_scope_revision uuid;v_scope_line uuid;v_scope_component text;
 v_scope_target text;v_scope_umbrella uuid;v_scope_vat boolean;v_scope_source text;v_scope_basis text;
 v_scope_a numeric;v_scope_r numeric;v_scope_h numeric;v_scope_residual numeric;v_scope_members bigint;
 v_saved private.bpay_next_component_resolution%rowtype;v_window private.bpay_next_valuation_policy_window%rowtype;
 v_working jsonb;v_plan jsonb:='[]';v_entry jsonb;v_ref jsonb;v_required_basis jsonb;
 v_required_included numeric;v_capture_included numeric;v_captured_segments boolean:=false;
 v_prepared jsonb[]:=array[]::jsonb[];v_qualified jsonb[]:=array[]::jsonb[];
 v_quote_members jsonb[];v_bucket_facts jsonb[]:=array[]::jsonb[];v_prefixes jsonb[]:=array[]::jsonb[];
 v_member jsonb;v_member_units numeric[];v_member_source numeric[];v_member_included numeric;
 v_member_full boolean;v_member_source_known boolean;v_member_buckets integer;v_single_index integer;
 v_i integer;v_h numeric;v_r numeric;v_equivalent numeric;v_native_sum numeric;v_cell_source numeric;
 v_cum_units numeric[]:=array[0::numeric,0,0,0,0];v_cum_source numeric[]:=array[0::numeric,0,0,0,0];
 v_cum_known boolean[]:=array[true,true,true,true,true];v_zero boolean;
 v_prefix_a numeric:=0;v_prefix_r numeric:=0;v_prefix_h numeric:=0;v_prefix_source numeric:=0;
 v_component_ex numeric;v_component_vat numeric;v_component_inc numeric;v_component jsonb;
 v_requested jsonb;v_selected_requested jsonb;v_selected_component jsonb;v_selected_bucket_facts jsonb[];v_result jsonb;

begin
 -- Complete native capture and pinned scope precede the shared quote plan.
 if v_current and p_run_position_id is not null then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 if p_run_position_id is null and not v_current then
  if p_through_segment_key is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  v_plan:=jsonb_build_array(jsonb_build_object('intent',to_jsonb(p_intent)));
 else
  if not v_current then
   v_working:=private.bpay_next_captured_resolution_operands_v1(p_run_position_id);
   rp:=jsonb_populate_record(null::private.bpay_next_run_position,v_working->'position');
   worker:=jsonb_populate_record(null::private.bpay_next_run_worker,v_working->'worker');
   v_scope_work:=rp.work_id;v_scope_candidate:=worker.candidate_id;v_scope_revision:=rp.captured_revision_id;
   v_scope_line:=rp.approved_line_id;v_scope_component:=rp.component_key;v_scope_target:=worker.target_pay_channel;
   v_scope_umbrella:=worker.target_umbrella_id;v_scope_vat:=worker.target_umbrella_vat_chargeable;
   v_scope_source:=rp.source_pay_channel;v_scope_basis:=rp.source_basis_channel;
   v_scope_a:=rp.approved_source_ex_vat;v_scope_r:=rp.realised_source_ex_vat;v_scope_h:=rp.held_source_ex_vat;
   v_scope_residual:=rp.residual_source_ex_vat;v_scope_members:=rp.segment_member_count;
  else
   v_working:=p_current_working;
   select x.* into cw from private.bpay_next_work x where x.id=p_intent.work_id;
   select x.* into cr from private.bpay_next_work_revision x where x.id=p_boundary_revision_id and x.work_id=cw.id;
   select x.* into ca from private.bpay_next_approved_line x where x.id=(v_working->>'approved_line_id')::uuid
    and x.revision_id=cr.id and x.component_key=p_intent.component_key;
   select x.* into pos from private.bpay_next_position x where x.work_id=cw.id and x.component_key=ca.component_key;
   select x.* into cc from public.candidates x where x.id=cw.candidate_id;
   select x.* into cu from public.umbrellas x where x.id=cc.umbrella_id;
   v_scope_work:=cw.id;v_scope_candidate:=cc.id;v_scope_revision:=cr.id;v_scope_line:=ca.id;
   v_scope_component:=ca.component_key;v_scope_target:=upper(cc.pay_method);
   v_scope_umbrella:=case when v_scope_target='UMBRELLA' then cu.id end;
   v_scope_vat:=case when v_scope_target='UMBRELLA' then cu.vat_chargeable end;
   v_scope_source:=cr.source_pay_channel;v_scope_basis:=pos.source_basis_channel;
   v_scope_a:=pos.approved_source_ex_vat;v_scope_r:=pos.realised_source_ex_vat;v_scope_h:=pos.held_source_ex_vat;
   v_scope_residual:=v_scope_a-v_scope_r-v_scope_h;v_scope_members:=jsonb_array_length(v_working->'members');
   if cw.id is null or cr.id is null or ca.id is null or cc.id is null or pos.work_id is null
    or cw.approval_state is distinct from 'APPROVED' or cw.current_revision_id is distinct from cr.id
    or cw.applied_revision_id is distinct from cr.id or pos.applied_revision_id is distinct from cr.id
    or cr.approved_at_utc is null or cr.sealed_at_utc is null or ca.tax_treatment is distinct from 'TAXABLE'
    or cc.active is not true or v_scope_target not in ('PAYE','UMBRELLA') or v_scope_target is null
    or (v_scope_target='UMBRELLA' and (cu.id is null or cu.enabled is not true or cu.vat_chargeable is null))
    or (v_working->>'work_id')::uuid is distinct from cw.id or (v_working->>'revision_id')::uuid is distinct from cr.id
    or (v_working->>'valuation_policy_id')::uuid is distinct from cr.valuation_policy_id
    or (v_working->>'pay_date')::date is distinct from
       (public.pay_banking_official_date_context_v1(null::timestamptz)->>'current_official_pay_date')::date
    or v_scope_a is distinct from ca.source_pay_ex_vat or v_scope_members>10000
    or jsonb_typeof(v_working->'refs') is distinct from 'array' or jsonb_array_length(v_working->'refs') not between 1 and 5
    or octet_length(v_working::text)>8388608 then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  end if;
  v_capture_included:=(v_working->>'included_source_ex_vat')::numeric;
  if p_boundary_revision_id is distinct from v_scope_revision
   or p_intent.work_id is distinct from v_scope_work or p_intent.component_key is distinct from v_scope_component
   or not exists(select 1 from jsonb_array_elements(v_working->'refs') x
     where (x.value->>'decision_id')::uuid=p_intent.id and (x.value->>'decision_version')::bigint=p_intent.decision_version) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
  select x.* into v_saved from private.bpay_next_component_resolution x
   where x.id=p_intent.id and x.decision_version=p_intent.decision_version;
  if v_saved.id is null or to_jsonb(v_saved) is distinct from to_jsonb(p_intent)
   or (v_current and v_saved.state is distinct from 'ACTIVE') then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_VERSION_MISSING';end if;
  if p_through_segment_key is null then
   v_required_basis:=jsonb_build_object('approved_source_ex_vat',v_scope_a,'realised_source_ex_vat',v_scope_r,
    'held_source_ex_vat',v_scope_h,'source_basis_channel',v_scope_basis);
   v_required_included:=v_capture_included;
  else
   if not exists(select 1 from jsonb_array_elements(v_working->'members') x where x.value->>'segment_key'=p_through_segment_key
      and (x.value->>'included_source_ex_vat')::numeric>0) then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
   select jsonb_build_object('approved_source_ex_vat',sum((x.value->>'approved_source_ex_vat')::numeric),
      'realised_source_ex_vat',sum((x.value->>'realised_source_ex_vat')::numeric),
      'held_source_ex_vat',sum((x.value->>'held_source_ex_vat')::numeric),'source_basis_channel',v_scope_basis),
     sum((x.value->>'included_source_ex_vat')::numeric) into v_required_basis,v_required_included
    from jsonb_array_elements(v_working->'members') x where x.value->>'segment_key'<=p_through_segment_key;
  end if;
  if p_position_basis is distinct from v_required_basis or p_included_source_ex_vat is distinct from v_required_included then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  v_captured_segments:=v_scope_members>0;
  if v_captured_segments then
   for rd in select x.* from private.bpay_next_rate_detail x where x.approved_line_id=v_scope_line and x.approved_hours>0
      order by case x.bucket when 'DAY' then 1 when 'NIGHT' then 2 when 'SAT' then 3 when 'SUN' then 4 when 'BH' then 5 else 6 end loop
    if rd.bucket not in ('DAY','NIGHT','SAT','SUN','BH') or jsonb_array_length(v_plan)>=5 then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BUCKET_MISSING';end if;
    v_ref:=null;
    select x.value into v_ref from jsonb_array_elements(v_working->'refs') x
     where x.value->>'rate_family'='STANDARD' and x.value->>'rate_code'=rd.bucket;
    if v_ref is not null then
     select x.* into d from private.bpay_next_component_resolution x
      where x.id=(v_ref->>'decision_id')::uuid and x.decision_version=(v_ref->>'decision_version')::bigint;
     if d.id is null then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_VERSION_MISSING';end if;
    else
     -- Native equivalent-basis treatment, no fabricated stored decision or CM receipt.
     d:=jsonb_populate_record(null::private.bpay_next_component_resolution,jsonb_build_object('work_id',v_scope_work,
      'candidate_id',v_scope_candidate,'based_on_revision_id',v_scope_revision,'approved_line_id',v_scope_line,
      'physical_timesheet_id',p_intent.physical_timesheet_id,'component_key',v_scope_component,'rate_family','STANDARD','rate_code',rd.bucket,
      'target_pay_channel',v_scope_target,'target_umbrella_id',v_scope_umbrella,
      'target_umbrella_vat_chargeable',v_scope_vat,'valuation_policy_id',v_working->'valuation_policy_id',
      'valuation_date',v_working->'pay_date','resolution_mode','SUGGESTED_EQUIVALENT_BASIS',
      'original_basis',private.bpay_next_component_resolution_basis_v1(v_scope_revision,v_scope_line,'STANDARD',rd.bucket)));
    end if;
    v_plan:=v_plan||jsonb_build_array(jsonb_build_object('intent',to_jsonb(d),'ref',v_ref));
   end loop;
   if jsonb_array_length(v_plan)=0 or exists(select 1 from jsonb_array_elements(v_working->'refs') x
      where x.value->>'rate_family'<>'STANDARD' or not exists(select 1 from jsonb_array_elements(v_plan) y
       where y.value#>>'{intent,rate_code}'=x.value->>'rate_code')) then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
  else
   if p_through_segment_key is not null or jsonb_array_length(v_working->'refs')<>1 then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
   v_plan:=jsonb_build_array(jsonb_build_object('intent',to_jsonb(p_intent),'ref',(v_working->'refs')->0));
  end if;
 end if;
 -- One mode/basis/window admission shared by live scalar and captured entries.
 for v_entry in select x.value from jsonb_array_elements(v_plan) x loop
 d:=jsonb_populate_record(null::private.bpay_next_component_resolution,v_entry->'intent');
 v_source_sum:=0;v_bucket_source:=null;v_units:=null;v_quote:=null;
 if p_boundary_revision_id is null or d.work_id is null or d.candidate_id is null
  or d.rate_family is null or d.rate_code is null or d.component_key is null
  or d.target_pay_channel is null or d.target_pay_channel not in ('PAYE','UMBRELLA')
  or (d.target_pay_channel='PAYE' and (d.target_umbrella_id is not null or d.target_umbrella_vat_chargeable is not null))
  or (d.target_pay_channel='UMBRELLA' and (d.target_umbrella_id is null or d.target_umbrella_vat_chargeable is null))
  or d.valuation_policy_id is null or d.valuation_date is null or not isfinite(d.valuation_date)
  or d.valuation_date not between date '0001-01-01' and date '9999-12-31'
  or d.resolution_mode is null then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
 if not exists(select 1 from private.bpay_next_work w where w.id=d.work_id and w.candidate_id=d.candidate_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
 if p_boundary_revision_id=d.based_on_revision_id then v_line:=d.approved_line_id;
 elsif p_boundary_revision_id=d.published_revision_id then v_line:=d.published_line_id;
 else raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_PUBLICATION_INCOMPATIBLE';end if;
 b:=private.bpay_next_component_resolution_basis_v1(p_boundary_revision_id,v_line,d.rate_family,d.rate_code);
 if b is distinct from (case when p_boundary_revision_id=d.based_on_revision_id then d.original_basis else d.published_basis end)
  or (b->>'work_id')::uuid is distinct from d.work_id
  or (b->>'physical_timesheet_id')::uuid is distinct from d.physical_timesheet_id
  or b->>'component_key' is distinct from d.component_key
  or (b->>'valuation_policy_id')::uuid is distinct from d.valuation_policy_id then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;

 if p_run_position_id is not null or v_current then
  if (d.work_id,d.candidate_id,d.component_key,v_line,d.target_pay_channel,d.target_umbrella_id,d.target_umbrella_vat_chargeable,d.valuation_policy_id,d.valuation_date)
    is distinct from (v_scope_work,v_scope_candidate,v_scope_component,v_scope_line,v_scope_target,
      v_scope_umbrella,v_scope_vat,(v_working->>'valuation_policy_id')::uuid,(v_working->>'pay_date')::date)
    or (d.id is not null and not exists(select 1 from jsonb_array_elements(v_working->'refs') x
      where (x.value->>'decision_id')::uuid=d.id and (x.value->>'decision_version')::bigint=d.decision_version
       and x.value->>'rate_family'=d.rate_family and x.value->>'rate_code'=d.rate_code
       and (x.value->>'approved_line_id')::uuid=v_line)) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
 end if;
 if not v_captured_segments then
 if jsonb_typeof(p_position_basis) is distinct from 'object' then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 if (select count(*) from jsonb_object_keys(p_position_basis))<>4
  or not(p_position_basis ?& array['approved_source_ex_vat','realised_source_ex_vat','held_source_ex_vat','source_basis_channel'])
  or exists(select 1 from jsonb_each(p_position_basis) x where x.key<>'source_basis_channel'
   and jsonb_typeof(x.value)<>'number') then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 if exists(select 1 from jsonb_each_text(p_position_basis) x where x.key<>'source_basis_channel'
   and (x.value in ('NaN','Infinity','-Infinity') or x.value::numeric<0 or x.value::numeric>=10000000000000000
    or x.value::numeric<>trunc(x.value::numeric,2))) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 v_approved:=(p_position_basis->>'approved_source_ex_vat')::numeric;
 v_realised:=(p_position_basis->>'realised_source_ex_vat')::numeric;
 v_held:=(p_position_basis->>'held_source_ex_vat')::numeric;
 v_residual:=v_approved-v_realised-v_held;
 if v_approved is distinct from (b->>'component_source_ex_vat')::numeric or v_residual<0
  or p_included_source_ex_vat is null or p_included_source_ex_vat::text in ('NaN','Infinity','-Infinity')
  or p_included_source_ex_vat<0 or p_included_source_ex_vat>v_residual
  or p_included_source_ex_vat<>trunc(p_included_source_ex_vat,2)
  or ((v_realised<>0 or v_held<>0) and p_position_basis->>'source_basis_channel' is distinct from b->>'source_pay_channel') then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 else
  -- Signed A/R and positive Included above net residual are admitted only by
  -- the complete native segment census; never by a scalar/caller basis.
  v_approved:=v_scope_a;v_realised:=v_scope_r;v_held:=v_scope_h;
  v_residual:=v_scope_residual;
  if v_approved is distinct from (b->>'component_source_ex_vat')::numeric
   or ((v_realised<>0 or v_held<>0) and v_scope_basis is distinct from b->>'source_pay_channel') then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 end if;
 -- Raw request precision is checked before constructing this native composite.
 -- Repeat the exact semantic admission here for every qualified caller.
 if (d.resolution_mode='SUGGESTED_EQUIVALENT_BASIS' and (d.manual_replacement_rate is not null or d.manual_amount_ex_vat is not null))
  or (d.resolution_mode='MANUAL_REPLACEMENT_RATE' and (b->>'semantics'<>'RATE_BEARING'
   or d.manual_replacement_rate is null or d.manual_replacement_rate::text in ('NaN','Infinity','-Infinity')
   or d.manual_replacement_rate<0 or d.manual_replacement_rate>=1000000000000
   or d.manual_replacement_rate<>trunc(d.manual_replacement_rate,6) or d.manual_amount_ex_vat is not null
   or nullif(btrim(d.reason),'') is null))
  or (d.resolution_mode='MANUAL_AMOUNT' and (b->>'semantics'<>'AMOUNT_LED'
   or d.manual_amount_ex_vat is null or d.manual_amount_ex_vat::text in ('NaN','Infinity','-Infinity')
   or d.manual_amount_ex_vat<0 or d.manual_amount_ex_vat>=10000000000000000
   or d.manual_amount_ex_vat<>trunc(d.manual_amount_ex_vat,2) or d.manual_replacement_rate is not null
   or nullif(btrim(d.reason),'') is null)) or octet_length(d.reason)>2000 then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_MODE_INVALID';end if;
 if d.resolution_mode='MANUAL_AMOUNT' and (d.original_position_basis is distinct from p_position_basis
  or p_included_source_ex_vat is distinct from v_residual) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 select x.* into win
  from private.bpay_next_valuation_policy_window x where x.policy_id=d.valuation_policy_id
   and d.valuation_date>=x.date_from and (x.date_to is null or d.valuation_date<=x.date_to)
  order by x.date_from desc,x.source_window_id desc limit 1;
 if win.source_window_id is null then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POLICY_WINDOW_MISSING';end if;
 select count(*) into v_window_count from private.bpay_next_valuation_policy_window x
  where x.policy_id=d.valuation_policy_id and x.date_from=win.date_from
   and d.valuation_date>=x.date_from and (x.date_to is null or d.valuation_date<=x.date_to);
 if v_window_count<>1 then
  raise exception using errcode='23514',message='BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS';end if;

 if not v_captured_segments then
  if p_included_source_ex_vat=0 then v_source:=0;v_units:=0;v_zero:=true;
  else
 v_full:=v_realised=0 and v_held=0 and p_included_source_ex_vat=v_approved;
 v_full_units:=(b->>'source_units')::numeric;v_rate:=(b->>'source_rate')::numeric;
 v_units:=case when v_full then v_full_units end;v_source:=p_included_source_ex_vat;
 if d.rate_family='STANDARD' then
  select count(*) into v_buckets from private.bpay_next_rate_detail where approved_line_id=v_line and approved_hours>0;
  if v_buckets>1 then
   if not v_full then
    -- Aggregate A/R/H cannot attribute paid units or SOURCE money by bucket.
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
   for rd in select x.* from private.bpay_next_rate_detail x where x.approved_line_id=v_line and x.approved_hours>0 loop
    if rd.approved_hours::text in ('NaN','Infinity','-Infinity') or rd.approved_hours>=1000000000000
     or rd.approved_hours<>trunc(rd.approved_hours,6) or rd.source_pay_rate is null
     or rd.source_pay_rate::text in ('NaN','Infinity','-Infinity') or rd.source_pay_rate<0
     or rd.source_pay_rate>=1000000000000 or rd.source_pay_rate<>trunc(rd.source_pay_rate,6) then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
    v_native:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',null,
     b->>'source_pay_channel',b->>'source_pay_channel',rd.approved_hours,rd.source_pay_rate,null,win.erni_pct,win.vat_rate_pct,false);
    v_source_sum:=v_source_sum+(v_native->>'ex')::numeric;
    if rd.bucket=d.rate_code then v_bucket_source:=(v_native->>'ex')::numeric;end if;
   end loop;
   if v_source_sum=v_approved and v_bucket_source is not null then v_source:=v_bucket_source;
   elsif d.resolution_mode='MANUAL_REPLACEMENT_RATE' then v_source:=null;
   else raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
  elsif v_buckets<>1 then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 end if;
 if not v_full and b->>'semantics'='RATE_BEARING' then
  -- One immutable unit-price basis only. Numeric division itself is not proof:
  -- exact <=6-decimal quotient, multiplication equality and native roundtrip.
  if v_rate>0 and v_full_units>0 then
   v_native:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',null,
    b->>'source_pay_channel',b->>'source_pay_channel',v_full_units,v_rate,null,win.erni_pct,win.vat_rate_pct,false);
   v_units:=p_included_source_ex_vat/v_rate;
   if (v_native->>'ex')::numeric is distinct from v_approved
    or v_units::text in ('NaN','Infinity','-Infinity') or v_units<0 or v_units>=1000000000000
    or v_units>v_full_units or v_units<>trunc(v_units,6)
    or v_units*v_rate is distinct from p_included_source_ex_vat then v_units:=null;
   else
    v_native:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',null,
     b->>'source_pay_channel',b->>'source_pay_channel',v_units,v_rate,null,win.erni_pct,win.vat_rate_pct,false);
    if (v_native->>'ex')::numeric is distinct from p_included_source_ex_vat then v_units:=null;end if;
   end if;
  end if;
 end if;
 if d.resolution_mode='MANUAL_REPLACEMENT_RATE' and (v_units is null or v_units<=0) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 v_zero:=false;
  end if;
 end if;
 v_prepared:=array_append(v_prepared,v_entry||jsonb_build_object('basis',b,'window',to_jsonb(win),
  'units',v_units,'source',v_source,'residual',v_residual,'zero',v_zero));
 end loop;

 -- Qualify each real Included member once, before any target quote/hold.
 if v_captured_segments then
  v_window:=jsonb_populate_record(null::private.bpay_next_valuation_policy_window,v_prepared[1]->'window');
  for v_member in select x.value from jsonb_array_elements(v_working->'members') x loop
   v_member_included:=(v_member->>'included_source_ex_vat')::numeric;
   v_member_units:=array[0::numeric,0,0,0,0];v_member_source:=array[0::numeric,0,0,0,0];
   if v_member_included>0 then
    v_member_full:=(v_member->>'realised_source_ex_vat')::numeric=0 and (v_member->>'held_source_ex_vat')::numeric=0
      and v_member_included=(v_member->>'approved_source_ex_vat')::numeric;
    v_member_buckets:=0;v_native_sum:=0;v_member_source_known:=true;v_single_index:=null;
    for v_i in 1..5 loop
     v_h:=(v_member->'hours'->>(v_i-1))::numeric;v_r:=(v_member->'rates'->>(v_i-1))::numeric;
     if v_h is null or v_h::text in ('NaN','Infinity','-Infinity') or v_h<0 or v_h>=1000000000000 or v_h<>trunc(v_h,6) then
      raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
     if v_h>0 then
      v_member_buckets:=v_member_buckets+1;v_single_index:=v_i;
      if not exists(select 1 from unnest(v_prepared) x where x#>>'{basis,rate_code}'=(array['DAY','NIGHT','SAT','SUN','BH'])[v_i]) then
       raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BUCKET_MISSING';end if;
      v_member_units[v_i]:=v_h;
      if v_r is null or v_r::text in ('NaN','Infinity','-Infinity') or v_r<0 or v_r>=1000000000000 or v_r<>trunc(v_r,6) then
       v_member_source_known:=false;
      else
       v_native:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',null,v_scope_source,v_scope_source,
        v_h,v_r,null,v_window.erni_pct,v_window.vat_rate_pct,false);
       v_member_source[v_i]:=(v_native->>'ex')::numeric;v_native_sum:=v_native_sum+v_member_source[v_i];
      end if;
     end if;
    end loop;
    if v_member_buckets=0 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
    v_member_source_known:=v_member_source_known and v_native_sum=(v_member->>'approved_source_ex_vat')::numeric;
    if not v_member_full then
     if v_member_buckets<>1 or not v_member_source_known then
      raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
     v_i:=v_single_index;v_r:=(v_member->'rates'->>(v_i-1))::numeric;v_h:=v_member_units[v_i];
     if v_r is null or v_r<=0 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
     v_equivalent:=v_member_included/v_r;
     if v_equivalent::text in ('NaN','Infinity','-Infinity') or v_equivalent<=0 or v_equivalent>=1000000000000
       or v_equivalent>v_h or v_equivalent<>trunc(v_equivalent,6) or v_equivalent*v_r is distinct from v_member_included then
      raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
     v_native:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',null,v_scope_source,v_scope_source,
      v_equivalent,v_r,null,v_window.erni_pct,v_window.vat_rate_pct,false);
     if (v_native->>'ex')::numeric is distinct from v_member_included then
      raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
     v_member_units[v_i]:=v_equivalent;v_member_source[v_i]:=v_member_included;
    end if;
    if not v_member_source_known then
     for v_i in 1..5 loop if v_member_units[v_i]>0 then v_member_source[v_i]:=null;end if;end loop;
    end if;
   end if;
   v_qualified:=array_append(v_qualified,v_member||jsonb_build_object('included_units',to_jsonb(v_member_units),'bucket_source',to_jsonb(v_member_source)));
  end loop;
  if octet_length((v_working||jsonb_build_object('members',to_jsonb(v_qualified),'plan',to_jsonb(v_prepared)))::text)>8388608 then
   raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 end if;

 if v_captured_segments and v_capture_included>0 then v_quote_members:=v_qualified;
 else v_quote_members:=array[jsonb_build_object('zero',v_captured_segments)];end if;
 foreach v_member in array v_quote_members loop
  if v_captured_segments and v_capture_included>0 then
   v_prefix_a:=v_prefix_a+(v_member->>'approved_source_ex_vat')::numeric;
   v_prefix_r:=v_prefix_r+(v_member->>'realised_source_ex_vat')::numeric;
   v_prefix_h:=v_prefix_h+(v_member->>'held_source_ex_vat')::numeric;
   v_prefix_source:=v_prefix_source+(v_member->>'included_source_ex_vat')::numeric;
   if (v_member->>'included_source_ex_vat')::numeric=0 then continue;end if;
   for v_i in 1..5 loop
    v_cum_units[v_i]:=v_cum_units[v_i]+(v_member->'included_units'->>(v_i-1))::numeric;
    v_cell_source:=(v_member->'bucket_source'->>(v_i-1))::numeric;
    if v_cell_source is null then v_cum_known[v_i]:=false;end if;
    v_cum_source[v_i]:=case when v_cum_known[v_i] then v_cum_source[v_i]+v_cell_source end;
   end loop;
  end if;
  v_component_ex:=0;v_component_vat:=0;v_component_inc:=0;v_bucket_facts:=array[]::jsonb[];
  foreach v_entry in array v_prepared loop
   d:=jsonb_populate_record(null::private.bpay_next_component_resolution,v_entry->'intent');
   b:=v_entry->'basis';win:=jsonb_populate_record(null::private.bpay_next_valuation_policy_window,v_entry->'window');
   if v_captured_segments then
    v_i:=array_position(array['DAY','NIGHT','SAT','SUN','BH'],d.rate_code);
    v_units:=v_cum_units[v_i];v_source:=v_cum_source[v_i];
    if v_units is null or v_units<0 or v_units>=1000000000000 or v_units<>trunc(v_units,6)
      or v_units>(b->>'source_units')::numeric then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
    if d.resolution_mode='SUGGESTED_EQUIVALENT_BASIS' and v_source is null then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
    v_zero:=v_units=0 and v_source=0;
   else
    v_units:=(v_entry->>'units')::numeric;v_source:=(v_entry->>'source')::numeric;v_zero:=(v_entry->>'zero')::boolean;
   end if;
   if v_zero then v_quote:=jsonb_build_object('ex',0,'vat',0,'inc',0,'rate',null);
   else
   -- The original2218 target arithmetic call and result admission, one shared body.
   v_quote:=private.pay_finance_selected_quote_v1(d.resolution_mode,v_source,b->>'source_pay_channel',
    d.target_pay_channel,v_units,d.manual_replacement_rate,d.manual_amount_ex_vat,
    win.erni_pct,win.vat_rate_pct,coalesce(d.target_umbrella_vat_chargeable,false));
   end if;
   if not(v_quote ?& array['ex','vat','inc']) or exists(select 1 from jsonb_each_text(v_quote) q where q.key in ('ex','vat','inc')
     and(q.value is null or q.value in ('NaN','Infinity','-Infinity') or q.value::numeric<0
      or q.value::numeric>=10000000000000000 or q.value::numeric<>trunc(q.value::numeric,2)))
    or (v_quote->>'inc')::numeric is distinct from (v_quote->>'ex')::numeric+(v_quote->>'vat')::numeric then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_QUOTE_RANGE_INVALID';end if;
   v_component_ex:=v_component_ex+(v_quote->>'ex')::numeric;
   v_component_vat:=v_component_vat+(v_quote->>'vat')::numeric;v_component_inc:=v_component_inc+(v_quote->>'inc')::numeric;
   v_bucket_facts:=array_append(v_bucket_facts,jsonb_build_object('rate_family',d.rate_family,'rate_code',d.rate_code,
     'decision_id',d.id,'decision_version',case when d.id is not null then d.decision_version::text end,
     'treatment',case when d.id is null then 'NATIVE_DEFAULT' else 'PINNED' end,'resolution_mode',d.resolution_mode,
     'approved_units',b->'source_units','included_units',v_units,'source_rate',b->'source_rate','source_ex_vat',v_source,
     'manual_replacement_rate',d.manual_replacement_rate,'manual_amount_ex_vat',d.manual_amount_ex_vat,'quote',v_quote,
     'valuation_policy_id',d.valuation_policy_id,'valuation_window_id',win.source_window_id));
   if (d.rate_family,d.rate_code) is not distinct from (p_intent.rate_family,p_intent.rate_code) then
    v_requested:=jsonb_build_object('source_ex_vat',v_source,
      'source_component_residual_ex_vat',case when v_captured_segments then (p_position_basis->>'approved_source_ex_vat')::numeric
         -(p_position_basis->>'realised_source_ex_vat')::numeric-(p_position_basis->>'held_source_ex_vat')::numeric else (v_entry->>'residual')::numeric end,
      'included_source_component_ex_vat',p_included_source_ex_vat,'remaining_units',v_units,'quote',v_quote,
      'valuation_policy_id',d.valuation_policy_id,'valuation_window_id',win.source_window_id);
   end if;
  end loop;
  if v_component_ex>=10000000000000000 or v_component_vat>=10000000000000000 or v_component_inc>=10000000000000000
   or v_component_inc is distinct from v_component_ex+v_component_vat then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_QUOTE_RANGE_INVALID';end if;
  v_component:=jsonb_build_object('ex',v_component_ex,'vat',v_component_vat,'inc',v_component_inc);
  if v_captured_segments and v_capture_included>0 then
   v_prefixes:=array_append(v_prefixes,jsonb_build_object('segment_key',v_member->>'segment_key',
    case when v_current then 'approved_segment_id' else 'run_work_segment_id' end,
      case when v_current then v_member->'approved_segment_id' else v_member->'run_work_segment_id' end,
    'source_ex_vat',v_member->'included_source_ex_vat',
    'quote',v_component));
  end if;
  if p_through_segment_key is null or v_member->>'segment_key'=p_through_segment_key then
   v_selected_requested:=v_requested;v_selected_component:=v_component;v_selected_bucket_facts:=v_bucket_facts;
  end if;
 end loop;
 if p_run_position_id is null and not v_current then return v_selected_requested;end if;
 if v_selected_requested is null or v_selected_component is null then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 if p_through_segment_key is not null then
  select coalesce(array_agg(x order by x->>'segment_key'),array[]::jsonb[]) into v_prefixes
   from unnest(v_prefixes) x where x->>'segment_key'<=p_through_segment_key;
 end if;
 v_result:=v_selected_requested||jsonb_build_object('component_quote',v_selected_component,
  'bucket_details',to_jsonb(v_selected_bucket_facts),'prefix_targets',to_jsonb(v_prefixes))
   ||case when v_current then jsonb_build_object('current_decision_versions',v_working->'current_decision_versions')
     else jsonb_build_object('accepted_resolution_refs',v_working->'refs') end;
 if octet_length((v_result||jsonb_build_object('working',v_working,'plan',v_plan,'prepared',to_jsonb(v_prepared),'qualified',to_jsonb(v_qualified)))::text)>8388608 then
  raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 return v_result;

end
$function$;
create or replace function private.bpay_next_component_resolution_input_quote_v1(
 p_intent private.bpay_next_component_resolution,p_boundary_revision_id uuid,
 p_position_basis jsonb,p_included_source_ex_vat numeric,p_run_position_id uuid,p_through_segment_key text
) returns jsonb language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select private.bpay_next_component_resolution_input_quote_v1(p_intent,p_boundary_revision_id,p_position_basis,
  p_included_source_ex_vat,p_run_position_id,p_through_segment_key,null)
$function$;
create or replace function private.bpay_next_current_component_resolution_quote_v1(
 p_work_id uuid,p_revision_id uuid,p_approved_line_id uuid,p_pay_date date,
 p_selected_leaf_keys text[],p_through_segment_key text default null
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
 a private.bpay_next_approved_line%rowtype;pos private.bpay_next_position%rowtype;
 g private.bpay_next_workbench_group%rowtype;c public.candidates%rowtype;u public.umbrellas%rowtype;
 md private.bpay_next_workbench_candidate_metadata%rowtype;v_fvr bigint;v_date jsonb;
 d private.bpay_next_component_resolution%rowtype;first_d private.bpay_next_component_resolution%rowtype;active record;
 v_target text;v_umbrella uuid;v_vat boolean;v_line uuid;v_seen integer:=0;
 v_refs jsonb:='[]';v_versions jsonb:='[]';v_decision_keys text[]:='{}';v_key text;
 v_count bigint;v_bad boolean;v_approval_count bigint;v_approval_a numeric;v_selected_count bigint;
 v_segmented boolean;v_member jsonb;v_members jsonb[]:=array[]::jsonb[];v_bytes bigint:=0;
 v_a numeric:=0;v_r numeric:=0;v_h numeric:=0;v_included numeric:=0;v_excluded numeric:=0;
 v_recovery numeric:=0;v_overhang numeric:=0;v_basis jsonb;v_required_included numeric;
 v_working jsonb;v_result jsonb;leaf record;v_native_version text;
begin
 if p_work_id is null or p_revision_id is null or p_approved_line_id is null or p_pay_date is null
  or not isfinite(p_pay_date) or p_selected_leaf_keys is null
  or cardinality(p_selected_leaf_keys) not between 1 and 10000
  or array_ndims(p_selected_leaf_keys) is distinct from 1
  or exists(select 1 from unnest(p_selected_leaf_keys) x where x is null or octet_length(x) not between 1 and 8192)
  or (select count(distinct x) from unnest(p_selected_leaf_keys) x)<>cardinality(p_selected_leaf_keys)
  or octet_length(to_jsonb(p_selected_leaf_keys)::text)>8388608 then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
 select x.* into w from private.bpay_next_work x where x.id=p_work_id;
 select x.* into r from private.bpay_next_work_revision x where x.id=p_revision_id and x.work_id=w.id;
 select x.* into a from private.bpay_next_approved_line x where x.id=p_approved_line_id and x.revision_id=r.id;
 select x.* into pos from private.bpay_next_position x where x.work_id=w.id and x.component_key=a.component_key;
 select x.* into c from public.candidates x where x.id=w.candidate_id;
 select x.* into u from public.umbrellas x where x.id=c.umbrella_id;
 select x.* into g from private.bpay_next_workbench_group x where x.work_id=w.id;
 select x.* into md from private.bpay_next_workbench_candidate_metadata x where x.candidate_id=w.candidate_id;
 select x.financial_view_revision into v_fvr from private.bpay_next_worker_control x where x.candidate_id=w.candidate_id;
 v_date:=public.pay_banking_official_date_context_v1(null::timestamptz);
 v_target:=upper(c.pay_method);v_umbrella:=case when v_target='UMBRELLA' then u.id end;
 v_vat:=case when v_target='UMBRELLA' then u.vat_chargeable end;
 if w.id is null or r.id is null or a.id is null or pos.work_id is null or c.id is null or g.work_id is null
  or md.candidate_id is null or v_fvr is null or c.active is not true
  or v_target is null or v_target not in ('PAYE','UMBRELLA')
  or (v_target='UMBRELLA' and (u.id is null or u.enabled is not true or u.vat_chargeable is null))
  or w.approval_state is distinct from 'APPROVED' or w.current_revision_id is distinct from r.id
  or w.applied_revision_id is distinct from r.id or pos.applied_revision_id is distinct from r.id
  or r.approved_at_utc is null or r.sealed_at_utc is null or a.tax_treatment is distinct from 'TAXABLE'
  or pos.approved_source_ex_vat is distinct from a.source_pay_ex_vat
  or g.group_kind is distinct from 'WORK' or g.candidate_id is distinct from c.id
  or g.current_revision_id is distinct from r.id or g.target_pay_channel is distinct from v_target
  or g.source_pay_channel is distinct from r.source_pay_channel or g.complete is not true or g.dirty is not false
  or g.progress_state is distinct from 'CURRENT' or g.financial_view_revision is distinct from v_fvr
  or g.metadata_version is distinct from md.metadata_version or md.continuation_pending is not false
  or md.adopted_metadata_version is distinct from md.metadata_version
  or v_date->>'ok' is distinct from 'true' or (v_date->>'current_official_pay_date')::date is distinct from p_pay_date then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Physical inputs are bounded before filtering any eligibility or selected rows.
 with native as materialized(select x.* from private.bpay_next_approved_segment x
   where x.revision_id=r.id and x.component_key=a.component_key order by x.segment_key limit 10001)
 select count(*),coalesce(sum(case when pay_excluded then 0 else approved_source_ex_vat end),0),
  coalesce(bool_or(approved_line_id is distinct from a.id),false)
 into v_approval_count,v_approval_a,v_bad from native;
 if v_approval_count>10000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 if v_bad then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 with native as materialized(select x.* from private.bpay_next_workbench_item x
   where x.work_id=w.id and x.component_key=a.component_key order by x.segment_key,x.portion_kind,x.id limit 50001)
 select count(*),coalesce(bool_or(group_key is distinct from g.group_key or candidate_id is distinct from c.id
   or revision_id is distinct from r.id or approved_line_id is distinct from a.id
   or member_kind not in ('WORK_COMPONENT','WORK_SEGMENT') or amount_known is not true or progress_state is distinct from 'CURRENT'
   or financial_view_revision is distinct from v_fvr or projection_revision is distinct from g.projection_revision
   or metadata_version is distinct from md.metadata_version or source_pay_channel is distinct from r.source_pay_channel
   or target_pay_channel is distinct from v_target),false),coalesce(bool_or(member_kind='WORK_SEGMENT'),false)
 into v_count,v_bad,v_segmented from native;
 if v_count>50000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 v_segmented:=v_segmented or v_approval_count>0;
 if v_bad or (v_segmented and v_approval_a is distinct from pos.approved_source_ex_vat)
  or exists(select 1 from private.bpay_next_workbench_item x where x.work_id=w.id and x.component_key=a.component_key
    and x.member_kind is distinct from case when v_segmented then 'WORK_SEGMENT' else 'WORK_COMPONENT' end) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Exact native READY keys only. Need checks precede the no-saved-decision return.
 v_selected_count:=0;
 for leaf in select i.* from private.bpay_next_workbench_item i where i.work_id=w.id and i.component_key=a.component_key
   and ('WORK:'||w.id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key)=any(p_selected_leaf_keys)
   and i.portion_kind in ('AVAILABLE','ZERO') order by i.member_key loop
  v_selected_count:=v_selected_count+1;
  if iSfinite(p_pay_date) is not true or leaf.section is distinct from 'READY' or leaf.selectable is not true
   or leaf.is_selected is not false or leaf.amount_known is not true or leaf.direction is distinct from 'PAYMENT'
   or coalesce(leaf.pay_hold,false) or leaf.snooze_id is not null
   or (v_segmented and not exists(select 1 from private.bpay_next_approved_segment s where s.id=leaf.approved_segment_id
     and s.revision_id=r.id and s.approved_line_id=a.id and s.component_key=a.component_key and s.segment_key=leaf.segment_key
     and not s.pay_excluded)) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  if leaf.included_source_ex_vat>0 and private.bpay_next_component_resolution_need_v1(w.id,r.id,a.id,
    leaf.approved_segment_id,v_target,v_umbrella,v_vat,p_pay_date) is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 end loop;
 if v_selected_count<>cardinality(p_selected_leaf_keys) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Complete native A/R/H coverage: all portions plus exhausted current approvals.
 with portions as materialized(select i.segment_key,count(*) n,
   sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) r,sum(i.active_reserved_source_ex_vat) h,
   sum(i.residual_source_ex_vat) residual,sum(i.included_source_ex_vat+i.excluded_source_ex_vat) positive,
   sum(i.recovery_source_ex_vat) recovery,sum(i.held_overhang_source_ex_vat) overhang,
   sum(case when ('WORK:'||w.id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key)=any(p_selected_leaf_keys)
     and i.portion_kind='AVAILABLE' then i.included_source_ex_vat else 0 end) included
   from private.bpay_next_workbench_item i where i.work_id=w.id and i.component_key=a.component_key group by i.segment_key),
 native as materialized(select s.* from private.bpay_next_approved_segment s
   where s.revision_id=r.id and s.component_key=a.component_key),
 keys as materialized(select segment_key from portions union select segment_key from native)
 select count(*) into v_count from (select segment_key from keys limit 10001) bounded;
 if v_count>10000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 for v_member in
  with portions as materialized(select i.segment_key,count(*) n,
    sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) r,sum(i.active_reserved_source_ex_vat) h,
    sum(i.residual_source_ex_vat) residual,sum(i.included_source_ex_vat+i.excluded_source_ex_vat) positive,
    sum(i.recovery_source_ex_vat) recovery,sum(i.held_overhang_source_ex_vat) overhang,
    sum(case when ('WORK:'||w.id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key)=any(p_selected_leaf_keys)
      and i.portion_kind='AVAILABLE' then i.included_source_ex_vat else 0 end) included
    from private.bpay_next_workbench_item i where i.work_id=w.id and i.component_key=a.component_key group by i.segment_key),
  native as materialized(select s.* from private.bpay_next_approved_segment s
    where s.revision_id=r.id and s.component_key=a.component_key),
  keys as materialized(select segment_key from portions union select segment_key from native)
  select jsonb_build_object('segment_key',k.segment_key,'approved_segment_id',s.id,
    'approved_source_ex_vat',case when p.n is null then case when s.pay_excluded then 0 else s.approved_source_ex_vat end else p.a end,
    'realised_source_ex_vat',case when p.n is null then case when s.pay_excluded then 0 else s.approved_source_ex_vat end else p.r end,
    'held_source_ex_vat',coalesce(p.h,0),'included_source_ex_vat',coalesce(p.included,0),
    'excluded_source_ex_vat',coalesce(p.positive-p.included,0),'recovery_source_ex_vat',coalesce(p.recovery,0),
    'held_overhang_source_ex_vat',coalesce(p.overhang,0),'native_source_ex_vat',case when s.pay_excluded then 0 else s.approved_source_ex_vat end,
    'projected_residual',coalesce(p.residual,0),
    'hours',jsonb_build_array(s.hours_day,s.hours_night,s.hours_sat,s.hours_sun,s.hours_bh),
    'rates',jsonb_build_array(s.pay_day,s.pay_night,s.pay_sat,s.pay_sun,s.pay_bh))
  from keys k left join portions p on p.segment_key is not distinct from k.segment_key
   left join native s on s.segment_key=k.segment_key order by k.segment_key nulls first
 loop
  if ((v_member->>'approved_segment_id') is not null
      and (v_member->>'approved_source_ex_vat')::numeric is distinct from (v_member->>'native_source_ex_vat')::numeric)
   or (v_segmented and v_member->>'approved_segment_id' is null and (v_member->>'approved_source_ex_vat')::numeric<>0)
   or (v_member->>'approved_source_ex_vat')::numeric-(v_member->>'realised_source_ex_vat')::numeric
      -(v_member->>'held_source_ex_vat')::numeric is distinct from (v_member->>'projected_residual')::numeric
   or (v_member->>'included_source_ex_vat')::numeric+(v_member->>'excluded_source_ex_vat')::numeric
      -(v_member->>'recovery_source_ex_vat')::numeric-(v_member->>'held_overhang_source_ex_vat')::numeric
      is distinct from (v_member->>'projected_residual')::numeric then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  v_a:=v_a+(v_member->>'approved_source_ex_vat')::numeric;v_r:=v_r+(v_member->>'realised_source_ex_vat')::numeric;
  v_h:=v_h+(v_member->>'held_source_ex_vat')::numeric;v_included:=v_included+(v_member->>'included_source_ex_vat')::numeric;
  v_excluded:=v_excluded+(v_member->>'excluded_source_ex_vat')::numeric;v_recovery:=v_recovery+(v_member->>'recovery_source_ex_vat')::numeric;
  v_overhang:=v_overhang+(v_member->>'held_overhang_source_ex_vat')::numeric;
  v_bytes:=v_bytes+octet_length(v_member::text)+2;
  if v_bytes>8388608 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
  if v_segmented then v_members:=array_append(v_members,v_member);end if;
 end loop;
 if (v_a,v_r,v_h) is distinct from (pos.approved_source_ex_vat,pos.realised_source_ex_vat,pos.held_source_ex_vat)
  or v_a-v_r-v_h is distinct from v_included+v_excluded-v_recovery-v_overhang then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Same101 physical mapped-boundary discovery as the accepted owner, without a Run.
 for d in select x.* from private.bpay_next_component_resolution x where x.work_id=w.id and x.state='ACTIVE'
   and (x.based_on_revision_id=r.id or x.published_revision_id=r.id)
   order by case when x.based_on_revision_id=r.id then x.approved_line_id else x.published_line_id end,
     x.rate_family,x.rate_code,x.id limit 101 loop
  v_seen:=v_seen+1;
  if v_seen>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
  v_line:=case when d.based_on_revision_id=r.id then d.approved_line_id else d.published_line_id end;
  if v_line is distinct from a.id or (d.candidate_id,d.target_pay_channel,d.target_umbrella_id,d.target_umbrella_vat_chargeable,
    d.valuation_policy_id,d.valuation_date) is distinct from(c.id,v_target,v_umbrella,v_vat,r.valuation_policy_id,p_pay_date) then continue;end if;
  select x.* into active from private.bpay_next_component_resolution_active_v1(w.id,r.id,a.id,d.rate_family,d.rate_code,
    v_target,v_umbrella,v_vat,r.valuation_policy_id,p_pay_date) x;
  if active.id is distinct from d.id or active.decision_version is distinct from d.decision_version then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;
  v_key:=jsonb_build_array(d.rate_family,d.rate_code)::text;
  if v_key=any(v_decision_keys) then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_COMPATIBILITY_AMBIGUOUS';end if;
  v_decision_keys:=array_append(v_decision_keys,v_key);
  if cardinality(v_decision_keys)>5 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BUCKET_MISSING';end if;
  if first_d.id is null then first_d:=d;end if;
  v_refs:=v_refs||jsonb_build_array(jsonb_build_object('approved_line_id',a.id,'rate_family',d.rate_family,
    'rate_code',d.rate_code,'decision_id',d.id,'decision_version',d.decision_version::text));
  select x.xmin::text into v_native_version from private.bpay_next_component_resolution x where x.id=d.id;
  v_versions:=v_versions||jsonb_build_array(jsonb_build_array('RATE_DECISION',d.id::text,d.decision_version::text,v_native_version));
 end loop;
 if first_d.id is null then return null;end if;
 v_basis:=jsonb_build_object('approved_source_ex_vat',v_a,'realised_source_ex_vat',v_r,
   'held_source_ex_vat',v_h,'source_basis_channel',pos.source_basis_channel);
 v_required_included:=v_included;
 if p_through_segment_key is not null then
  if not v_segmented or not exists(select 1 from unnest(v_members) x where x->>'segment_key'=p_through_segment_key
    and (x->>'included_source_ex_vat')::numeric>0 and x->>'approved_segment_id' is not null) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  select jsonb_build_object('approved_source_ex_vat',sum((x->>'approved_source_ex_vat')::numeric),
    'realised_source_ex_vat',sum((x->>'realised_source_ex_vat')::numeric),'held_source_ex_vat',sum((x->>'held_source_ex_vat')::numeric),
    'source_basis_channel',pos.source_basis_channel),sum((x->>'included_source_ex_vat')::numeric)
   into v_basis,v_required_included from unnest(v_members) x where x->>'segment_key'<=p_through_segment_key;
 end if;
 v_working:=jsonb_build_object('work_id',w.id,'revision_id',r.id,'approved_line_id',a.id,'pay_date',p_pay_date,
  'valuation_policy_id',r.valuation_policy_id,'refs',v_refs,'members',to_jsonb(v_members),
  'included_source_ex_vat',v_included,'current_decision_versions',v_versions);
 if octet_length(v_working::text)>8388608 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 v_result:=private.bpay_next_component_resolution_input_quote_v1(first_d,r.id,v_basis,v_required_included,null,p_through_segment_key,v_working);
 return v_result;
end
$function$;

alter function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text) owner to postgres;
revoke all on function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text) owner to postgres;
revoke all on function private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text) from public,anon,authenticated,service_role;
commit;
