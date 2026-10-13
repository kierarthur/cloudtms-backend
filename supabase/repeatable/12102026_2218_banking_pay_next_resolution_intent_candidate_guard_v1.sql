-- WB29 SOURCE review Q01: native Candidate FK qualification for in-memory intent.
-- Installed2213 stays immutable; every quote/money guard otherwise unchanged.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_component_resolution_input_quote_v1(
 p_intent private.bpay_next_component_resolution,p_boundary_revision_id uuid,
 p_position_basis jsonb,p_included_source_ex_vat numeric
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 d private.bpay_next_component_resolution:=p_intent;
 b jsonb;v_line uuid;win private.bpay_next_valuation_policy_window%rowtype;
 v_approved numeric;v_realised numeric;v_held numeric;v_residual numeric;
 v_units numeric;v_full_units numeric;v_rate numeric;v_source numeric;
 v_full boolean;v_buckets integer;v_source_sum numeric:=0;v_bucket_source numeric;
 rd private.bpay_next_rate_detail%rowtype;v_native jsonb;v_quote jsonb;v_window_count bigint;
begin
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
 if p_included_source_ex_vat=0 then
  return jsonb_build_object('source_ex_vat',0,'source_component_residual_ex_vat',v_residual,
   'included_source_component_ex_vat',0,'remaining_units',0,
   'quote',jsonb_build_object('ex',0,'vat',0,'inc',0,'rate',null),
   'valuation_policy_id',d.valuation_policy_id,'valuation_window_id',win.source_window_id);end if;
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
 v_quote:=private.pay_finance_selected_quote_v1(d.resolution_mode,v_source,b->>'source_pay_channel',
  d.target_pay_channel,v_units,d.manual_replacement_rate,d.manual_amount_ex_vat,
  win.erni_pct,win.vat_rate_pct,coalesce(d.target_umbrella_vat_chargeable,false));
 if not(v_quote ?& array['ex','vat','inc']) or exists(select 1 from jsonb_each_text(v_quote) q where q.key in ('ex','vat','inc')
   and(q.value is null or q.value in ('NaN','Infinity','-Infinity') or q.value::numeric<0
    or q.value::numeric>=10000000000000000 or q.value::numeric<>trunc(q.value::numeric,2)))
  or (v_quote->>'inc')::numeric is distinct from (v_quote->>'ex')::numeric+(v_quote->>'vat')::numeric then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_QUOTE_RANGE_INVALID';end if;
 return jsonb_build_object('source_ex_vat',v_source,'source_component_residual_ex_vat',v_residual,
  'included_source_component_ex_vat',p_included_source_ex_vat,'remaining_units',v_units,
  'quote',v_quote,'valuation_policy_id',d.valuation_policy_id,'valuation_window_id',win.source_window_id);
end
$function$;


alter function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric) owner to postgres;
revoke all on function private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric) from public,anon,authenticated,service_role;

commit;
