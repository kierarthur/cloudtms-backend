-- WB29/BD164: exact native captured operands; unchanged1157 owns arithmetic.
-- One linear complete census/bucket-prefix plan per captured invocation.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_captured_resolution_operands_v1(p_run_position_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 rp private.bpay_next_run_position%rowtype;rw private.bpay_next_run_work%rowtype;
 worker private.bpay_next_run_worker%rowtype;run private.bpay_next_pay_run%rowtype;
 choice private.bpay_next_work_choice%rowtype;r private.bpay_next_work_revision%rowtype;
 a private.bpay_next_approved_line%rowtype;v_refs jsonb;v_members jsonb;v_result jsonb;
 v_count bigint;v_bad boolean;v_totals jsonb;
begin
 select x.* into rp from private.bpay_next_run_position x where x.id=p_run_position_id;
 select x.* into rw from private.bpay_next_run_work x where x.id=rp.run_work_id;
 select x.* into worker from private.bpay_next_run_worker x where x.id=rp.run_worker_id;
 select x.* into run from private.bpay_next_pay_run x where x.id=worker.run_id;
 select x.* into choice from private.bpay_next_work_choice x where x.run_id=run.id and x.work_id=rp.work_id;
 select x.* into r from private.bpay_next_work_revision x where x.id=rp.captured_revision_id and x.work_id=rp.work_id;
 select x.* into a from private.bpay_next_approved_line x where x.id=rp.approved_line_id
  and x.revision_id=rp.captured_revision_id and x.component_key=rp.component_key;
 if rp.id is null or rw.id is null or worker.id is null or run.id is null or choice.work_id is null
  or r.id is null or a.id is null or r.approved_at_utc is null or r.sealed_at_utc is null
  or run.accepted_choice_descriptor is null
  or a.tax_treatment is distinct from 'TAXABLE' or choice.accepted_resolution_refs is null
  or choice.selection_state is distinct from 'SEALED' or choice.expected_revision_id is distinct from r.id
  or (rp.run_worker_id,rp.work_id,rp.captured_revision_id) is distinct from (rw.run_worker_id,rw.work_id,rw.captured_revision_id)
  or rw.candidate_id is distinct from worker.candidate_id
  or not exists(select 1 from private.bpay_next_work w where w.id=rp.work_id and w.candidate_id=worker.candidate_id)
  or not rp.segment_attribution_complete or rp.segment_member_count<>rp.segment_expected_member_count
  or rp.approved_source_ex_vat is distinct from a.source_pay_ex_vat
  or rp.source_pay_channel is distinct from r.source_pay_channel or rp.needs_financial_resolution then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Validate the immutable original Header even when all native segments are excluded.
 perform private.bpay_next_choice_leaf_eligible_v1(run.id,rp.work_id,r.id,a.id,null);
 if rp.segment_expected_member_count>10000 then
  raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 select coalesce(jsonb_agg(x.value order by x.value->>'rate_family',x.value->>'rate_code'),'[]') into v_refs
  from jsonb_array_elements(choice.accepted_resolution_refs) x where (x.value->>'approved_line_id')::uuid=a.id;
 if jsonb_array_length(v_refs) not between 1 and 5 or octet_length(v_refs::text)>8192
  or (select count(distinct jsonb_build_array(x.value->>'rate_family',x.value->>'rate_code'))
    from jsonb_array_elements(v_refs) x)<>jsonb_array_length(v_refs) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
 -- The maintained count is not an excuse to omit the physical overflow sentinel.
 with parents as materialized (
  select p.* from private.bpay_next_run_work_segment_parent p where p.run_work_id=rw.id and p.component_key=rp.component_key
   order by p.segment_key limit 10001
 ), members as materialized (
  select p.*,s.run_worker_id,s.work_id as segment_work_id,s.captured_revision_id as segment_revision,
   s.segment_key as actual_key,s.approved_segment_id as actual_approved_segment,s.disposition,
   g.id as native_segment_id,g.revision_id as native_revision,g.approved_line_id as native_line,
   g.component_key as native_component,g.segment_key as native_key,g.approved_source_ex_vat as native_source,
   jsonb_build_array(g.hours_day,g.hours_night,g.hours_sat,g.hours_sun,g.hours_bh) as hours,
   jsonb_build_array(g.pay_day,g.pay_night,g.pay_sat,g.pay_sun,g.pay_bh) as rates
  from parents p left join private.bpay_next_run_work_segment s on s.id=p.run_work_segment_id
  left join private.bpay_next_approved_segment g on g.id=p.approved_segment_id
 )
 select count(*),coalesce(bool_or(
    (work_id,captured_revision_id,segment_key) is distinct from (segment_work_id,segment_revision,actual_key)
    or run_worker_id is distinct from worker.id or captured_revision_id is distinct from r.id
    or approved_segment_id is distinct from actual_approved_segment
    or (approved_segment_id is not null and (native_segment_id,native_revision,native_line,native_component,native_key,native_source)
      is distinct from (approved_segment_id,r.id,a.id,rp.component_key,segment_key,approved_source_ex_vat))
    or (included_positive_source_ex_vat>0 and (disposition is distinct from 'INCLUDED' or native_segment_id is null
      or not private.bpay_next_choice_leaf_eligible_v1(run.id,rp.work_id,r.id,a.id,native_segment_id)))
    or residual_source_ex_vat<>approved_source_ex_vat-realised_source_ex_vat-held_source_ex_vat
    or included_positive_source_ex_vat+excluded_positive_source_ex_vat-recovery_source_ex_vat-held_overhang_source_ex_vat<>residual_source_ex_vat
   ),false),coalesce(jsonb_agg(jsonb_build_object('segment_key',segment_key,'run_work_segment_id',run_work_segment_id,
    'approved_segment_id',approved_segment_id,'approved_source_ex_vat',approved_source_ex_vat,
    'realised_source_ex_vat',realised_source_ex_vat,'held_source_ex_vat',held_source_ex_vat,
    'included_source_ex_vat',included_positive_source_ex_vat,'excluded_source_ex_vat',excluded_positive_source_ex_vat,
    'recovery_source_ex_vat',recovery_source_ex_vat,'held_overhang_source_ex_vat',held_overhang_source_ex_vat,
    'hours',hours,'rates',rates) order by segment_key),'[]'),
   jsonb_build_object('a',coalesce(sum(approved_source_ex_vat),0),'r',coalesce(sum(realised_source_ex_vat),0),
    'h',coalesce(sum(held_source_ex_vat),0),'included',coalesce(sum(included_positive_source_ex_vat),0),
    'excluded',coalesce(sum(excluded_positive_source_ex_vat),0),'recovery',coalesce(sum(recovery_source_ex_vat),0),
    'overhang',coalesce(sum(held_overhang_source_ex_vat),0))
 into v_count,v_bad,v_members,v_totals
 from members;
 if v_count>10000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 if v_count=0 and not private.bpay_next_choice_leaf_eligible_v1(run.id,rp.work_id,r.id,a.id,null) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 if v_bad or v_count<>rp.segment_member_count
  or (v_count>0 and ((v_totals->>'a')::numeric,(v_totals->>'r')::numeric,(v_totals->>'h')::numeric,(v_totals->>'included')::numeric,
     (v_totals->>'excluded')::numeric,(v_totals->>'recovery')::numeric,(v_totals->>'overhang')::numeric)
    is distinct from (rp.approved_source_ex_vat,rp.realised_source_ex_vat,rp.held_source_ex_vat,
      rp.segment_included_positive_ex_vat,rp.segment_excluded_positive_ex_vat,rp.segment_recovery_ex_vat,rp.segment_held_overhang_ex_vat)) then
  raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_HOLD_CAPTURE_INCOMPLETE';end if;
 v_result:=jsonb_build_object('position',to_jsonb(rp),'work',to_jsonb(rw),'worker',to_jsonb(worker),
  'pay_date',run.pay_date,'valuation_policy_id',r.valuation_policy_id,'refs',v_refs,'members',v_members,
  'included_source_ex_vat',case when v_count>0 then rp.segment_included_positive_ex_vat
    else greatest(rp.residual_source_ex_vat,0)-rp.snooze_excluded_positive_ex_vat end);
 if octet_length(v_result::text)>8388608 then
  raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 return v_result;
end
$function$;

create or replace function private.bpay_next_component_resolution_input_quote_v1(
 p_intent private.bpay_next_component_resolution,p_boundary_revision_id uuid,
 p_position_basis jsonb,p_included_source_ex_vat numeric,p_run_position_id uuid,p_through_segment_key text
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
 if p_run_position_id is null then
  if p_through_segment_key is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  v_plan:=jsonb_build_array(jsonb_build_object('intent',to_jsonb(p_intent)));
 else
  v_working:=private.bpay_next_captured_resolution_operands_v1(p_run_position_id);
  rp:=jsonb_populate_record(null::private.bpay_next_run_position,v_working->'position');
  worker:=jsonb_populate_record(null::private.bpay_next_run_worker,v_working->'worker');
  v_capture_included:=(v_working->>'included_source_ex_vat')::numeric;
  if p_boundary_revision_id is distinct from rp.captured_revision_id
   or p_intent.work_id is distinct from rp.work_id or p_intent.component_key is distinct from rp.component_key
   or not exists(select 1 from jsonb_array_elements(v_working->'refs') x
     where (x.value->>'decision_id')::uuid=p_intent.id and (x.value->>'decision_version')::bigint=p_intent.decision_version) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
  select x.* into v_saved from private.bpay_next_component_resolution x
   where x.id=p_intent.id and x.decision_version=p_intent.decision_version;
  if v_saved.id is null or to_jsonb(v_saved) is distinct from to_jsonb(p_intent) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_VERSION_MISSING';end if;
  if p_through_segment_key is null then
   v_required_basis:=jsonb_build_object('approved_source_ex_vat',rp.approved_source_ex_vat,'realised_source_ex_vat',rp.realised_source_ex_vat,
    'held_source_ex_vat',rp.held_source_ex_vat,'source_basis_channel',rp.source_basis_channel);
   v_required_included:=v_capture_included;
  else
   if not exists(select 1 from jsonb_array_elements(v_working->'members') x where x.value->>'segment_key'=p_through_segment_key
      and (x.value->>'included_source_ex_vat')::numeric>0) then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
   select jsonb_build_object('approved_source_ex_vat',sum((x.value->>'approved_source_ex_vat')::numeric),
      'realised_source_ex_vat',sum((x.value->>'realised_source_ex_vat')::numeric),
      'held_source_ex_vat',sum((x.value->>'held_source_ex_vat')::numeric),'source_basis_channel',rp.source_basis_channel),
     sum((x.value->>'included_source_ex_vat')::numeric) into v_required_basis,v_required_included
    from jsonb_array_elements(v_working->'members') x where x.value->>'segment_key'<=p_through_segment_key;
  end if;
  if p_position_basis is distinct from v_required_basis or p_included_source_ex_vat is distinct from v_required_included then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  v_captured_segments:=rp.segment_member_count>0;
  if v_captured_segments then
   for rd in select x.* from private.bpay_next_rate_detail x where x.approved_line_id=rp.approved_line_id and x.approved_hours>0
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
     d:=jsonb_populate_record(null::private.bpay_next_component_resolution,jsonb_build_object('work_id',rp.work_id,
      'candidate_id',worker.candidate_id,'based_on_revision_id',rp.captured_revision_id,'approved_line_id',rp.approved_line_id,
      'physical_timesheet_id',p_intent.physical_timesheet_id,'component_key',rp.component_key,'rate_family','STANDARD','rate_code',rd.bucket,
      'target_pay_channel',worker.target_pay_channel,'target_umbrella_id',worker.target_umbrella_id,
      'target_umbrella_vat_chargeable',worker.target_umbrella_vat_chargeable,'valuation_policy_id',v_working->'valuation_policy_id',
      'valuation_date',v_working->'pay_date','resolution_mode','SUGGESTED_EQUIVALENT_BASIS',
      'original_basis',private.bpay_next_component_resolution_basis_v1(rp.captured_revision_id,rp.approved_line_id,'STANDARD',rd.bucket)));
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

 if p_run_position_id is not null then
  if (d.work_id,d.candidate_id,d.component_key,v_line,d.target_pay_channel,d.target_umbrella_id,d.target_umbrella_vat_chargeable,d.valuation_policy_id,d.valuation_date)
    is distinct from (rp.work_id,worker.candidate_id,rp.component_key,rp.approved_line_id,worker.target_pay_channel,
      worker.target_umbrella_id,worker.target_umbrella_vat_chargeable,(v_working->>'valuation_policy_id')::uuid,(v_working->>'pay_date')::date)
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
  v_approved:=rp.approved_source_ex_vat;v_realised:=rp.realised_source_ex_vat;v_held:=rp.held_source_ex_vat;
  v_residual:=rp.residual_source_ex_vat;
  if v_approved is distinct from (b->>'component_source_ex_vat')::numeric
   or ((v_realised<>0 or v_held<>0) and rp.source_basis_channel is distinct from b->>'source_pay_channel') then
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
       v_native:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',null,rp.source_pay_channel,rp.source_pay_channel,
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
     v_native:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',null,rp.source_pay_channel,rp.source_pay_channel,
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
    'run_work_segment_id',v_member->'run_work_segment_id','source_ex_vat',v_member->'included_source_ex_vat',
    'quote',v_component));
  end if;
  if p_through_segment_key is null or v_member->>'segment_key'=p_through_segment_key then
   v_selected_requested:=v_requested;v_selected_component:=v_component;v_selected_bucket_facts:=v_bucket_facts;
  end if;
 end loop;
 if p_run_position_id is null then return v_selected_requested;end if;
 if v_selected_requested is null or v_selected_component is null then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 if p_through_segment_key is not null then
  select coalesce(array_agg(x order by x->>'segment_key'),array[]::jsonb[]) into v_prefixes
   from unnest(v_prefixes) x where x->>'segment_key'<=p_through_segment_key;
 end if;
 v_result:=v_selected_requested||jsonb_build_object('component_quote',v_selected_component,
  'bucket_details',to_jsonb(v_selected_bucket_facts),'prefix_targets',to_jsonb(v_prefixes),'accepted_resolution_refs',v_working->'refs');
 if octet_length((v_result||jsonb_build_object('working',v_working,'plan',v_plan,'prepared',to_jsonb(v_prepared),'qualified',to_jsonb(v_qualified)))::text)>8388608 then
  raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 return v_result;

end
$function$;
create or replace function private.bpay_next_component_resolution_input_quote_v1(
 p_intent private.bpay_next_component_resolution,p_boundary_revision_id uuid,p_position_basis jsonb,p_included_source_ex_vat numeric
) returns jsonb language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select private.bpay_next_component_resolution_input_quote_v1(p_intent,p_boundary_revision_id,p_position_basis,p_included_source_ex_vat,null,null)
$function$;
create or replace function private.bpay_next_captured_component_resolution_quote_v1(p_run_position_id uuid,p_through_segment_key text default null)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare rp private.bpay_next_run_position%rowtype;d private.bpay_next_component_resolution%rowtype;
 v_ref jsonb;v_basis jsonb;v_included numeric;v_count bigint;v_positive boolean;
begin
 select x.* into rp from private.bpay_next_run_position x where x.id=p_run_position_id;
 select x.value into v_ref from private.bpay_next_run_worker worker
  join private.bpay_next_work_choice choice on choice.run_id=worker.run_id and choice.work_id=rp.work_id
  cross join lateral jsonb_array_elements(choice.accepted_resolution_refs) x
  where worker.id=rp.run_worker_id and (x.value->>'approved_line_id')::uuid=rp.approved_line_id
  order by x.value->>'rate_family',x.value->>'rate_code' limit 1;
 select x.* into d from private.bpay_next_component_resolution x
  where x.id=(v_ref->>'decision_id')::uuid and x.decision_version=(v_ref->>'decision_version')::bigint;
 if d.id is null then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
 if p_through_segment_key is null then
  v_basis:=jsonb_build_object('approved_source_ex_vat',rp.approved_source_ex_vat,'realised_source_ex_vat',rp.realised_source_ex_vat,
   'held_source_ex_vat',rp.held_source_ex_vat,'source_basis_channel',rp.source_basis_channel);
  v_included:=case when rp.segment_member_count>0 then rp.segment_included_positive_ex_vat
    else greatest(rp.residual_source_ex_vat,0)-rp.snooze_excluded_positive_ex_vat end;
 else
  if rp.segment_expected_member_count>10000 then
   raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
  with parents as materialized (
   select p.* from private.bpay_next_run_work_segment_parent p where p.run_work_id=rp.run_work_id and p.component_key=rp.component_key
    order by p.segment_key limit 10001
  )
  select count(*),coalesce(bool_or(p.segment_key=p_through_segment_key and p.included_positive_source_ex_vat>0),false),
   jsonb_build_object('approved_source_ex_vat',sum(p.approved_source_ex_vat) filter(where p.segment_key<=p_through_segment_key),
    'realised_source_ex_vat',sum(p.realised_source_ex_vat) filter(where p.segment_key<=p_through_segment_key),
    'held_source_ex_vat',sum(p.held_source_ex_vat) filter(where p.segment_key<=p_through_segment_key),'source_basis_channel',rp.source_basis_channel),
   sum(p.included_positive_source_ex_vat) filter(where p.segment_key<=p_through_segment_key)
   into v_count,v_positive,v_basis,v_included from parents p;
  if v_count>10000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
  if v_count<>rp.segment_member_count or v_count<>rp.segment_expected_member_count or not v_positive then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 end if;
 return private.bpay_next_component_resolution_input_quote_v1(d,rp.captured_revision_id,v_basis,v_included,rp.id,p_through_segment_key);
end
$function$;
create or replace function private.bpay_next_component_resolution_quote_v1(
 p_decision_id uuid,p_version bigint,p_boundary_revision_id uuid,p_run_position_id uuid default null
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare d private.bpay_next_component_resolution%rowtype;rp private.bpay_next_run_position%rowtype;
 pos private.bpay_next_position%rowtype;w private.bpay_next_work%rowtype;
 rw private.bpay_next_run_worker%rowtype;run private.bpay_next_pay_run%rowtype;
 b jsonb;v_line uuid;v_approved numeric;v_realised numeric;v_held numeric;v_residual numeric;v_included numeric;
begin
 select wb45src.* into d from private.bpay_next_component_resolution wb45src where id=p_decision_id and decision_version=p_version;
 if d.id is null or p_boundary_revision_id is null then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_VERSION_MISSING';end if;
 if p_boundary_revision_id=d.based_on_revision_id then v_line:=d.approved_line_id;
 elsif p_boundary_revision_id=d.published_revision_id then v_line:=d.published_line_id;
 else raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_PUBLICATION_INCOMPATIBLE';end if;
 b:=private.bpay_next_component_resolution_basis_v1(p_boundary_revision_id,v_line,d.rate_family,d.rate_code);
 if b is distinct from (case when p_boundary_revision_id=d.based_on_revision_id then d.original_basis else d.published_basis end) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;
 if p_run_position_id is null then
  select wb45src.* into w from private.bpay_next_work wb45src where id=d.work_id;
  select wb45src.* into pos from private.bpay_next_position wb45src where work_id=d.work_id and component_key=d.component_key;
  if w.approval_state is distinct from 'APPROVED' or w.current_revision_id is distinct from p_boundary_revision_id
   or w.applied_revision_id is distinct from p_boundary_revision_id or pos.applied_revision_id is distinct from p_boundary_revision_id then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
  if private.bpay_next_resolution_live_snooze_v1(d.work_id,p_boundary_revision_id,d.component_key) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
  v_approved:=pos.approved_source_ex_vat;v_realised:=pos.realised_source_ex_vat;v_held:=pos.held_source_ex_vat;
  if (v_realised<>0 or v_held<>0) and pos.source_basis_channel is distinct from d.original_basis->>'source_pay_channel' then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
  v_residual:=v_approved-v_realised-v_held;v_included:=greatest(v_residual,0);
 else
  select wb45src.* into rp from private.bpay_next_run_position wb45src where id=p_run_position_id;
  select wb45src.* into rw from private.bpay_next_run_worker wb45src where id=rp.run_worker_id;
  select wb45src.* into run from private.bpay_next_pay_run wb45src where id=rw.run_id;
  if (rp.work_id,rp.captured_revision_id,rp.component_key,rp.approved_line_id)
   is distinct from (d.work_id,p_boundary_revision_id,d.component_key,v_line) or not rp.segment_attribution_complete
   or (rw.candidate_id,rw.target_pay_channel,rw.target_umbrella_id,rw.target_umbrella_vat_chargeable,run.pay_date)
   is distinct from (d.candidate_id,d.target_pay_channel,d.target_umbrella_id,d.target_umbrella_vat_chargeable,d.valuation_date) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  v_approved:=rp.approved_source_ex_vat;v_realised:=rp.realised_source_ex_vat;v_held:=rp.held_source_ex_vat;
  v_residual:=rp.residual_source_ex_vat;
  v_included:=case when rp.segment_member_count>0 then rp.segment_included_positive_ex_vat
   else greatest(v_residual,0)-rp.snooze_excluded_positive_ex_vat end;
  if (v_realised<>0 or v_held<>0) and rp.source_basis_channel is distinct from d.original_basis->>'source_pay_channel' then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 end if;
 if p_run_position_id is null and (v_approved is distinct from (d.original_basis->>'component_source_ex_vat')::numeric
  or v_residual is distinct from v_approved-v_realised-v_held or v_residual<0
  or v_included is null or v_included<0 or v_included>greatest(v_residual,0)) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 return private.bpay_next_component_resolution_input_quote_v1(d,p_boundary_revision_id,
  jsonb_build_object('approved_source_ex_vat',v_approved,'realised_source_ex_vat',v_realised,
   'held_source_ex_vat',v_held,'source_basis_channel',case when p_run_position_id is null then pos.source_basis_channel else rp.source_basis_channel end),
  v_included,p_run_position_id,null)||jsonb_build_object('decision_id',d.id,'decision_version',d.decision_version);
end
$function$;
alter function private.bpay_next_captured_resolution_operands_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_captured_resolution_operands_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text) owner to postgres;
revoke all on function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric) owner to postgres;
revoke all on function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric) from public,anon,authenticated,service_role;
alter function private.bpay_next_captured_component_resolution_quote_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_captured_component_resolution_quote_v1(uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_component_resolution_quote_v1(uuid,bigint,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_component_resolution_quote_v1(uuid,bigint,uuid,uuid) from public,anon,authenticated,service_role;
commit;
