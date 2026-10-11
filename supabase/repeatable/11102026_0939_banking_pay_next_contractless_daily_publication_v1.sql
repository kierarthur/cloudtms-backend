-- DY04 full joint Contractless Daily merge onto current1236/1332/1342.
-- Corrected0925 evidence and0928 pointer remain their existing final owners.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_approval_route_v1(p_timesheet_id uuid)
returns text
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private
as $function$
declare
  v_self_bill boolean;
  v_contract_id uuid;
  v_sheet_scope text;
  v_source_root boolean;
  v_line_type text;
  v_is_current boolean;
  v_archived_at timestamptz;
  v_revoked_at timestamptz;
  v_adjustment_origin text;
  v_correction_kind text;
  v_is_adjustment boolean;
  v_parent_timesheet_id uuid;
  v_correction_id text;
  v_import_classification jsonb;
begin
  if p_timesheet_id is null then
    raise exception using errcode='22023', message='BPAY_NEXT_APPROVAL_ROUTE_TIMESHEET_REQUIRED';
  end if;
  select t.contract_id,t.sheet_scope::text,c.self_bill,t.line_type::text,t.is_current,t.archived_at_utc,t.revoked_at,
         upper(btrim(coalesce(t.adjustment_origin,''))),
         upper(btrim(coalesce(t.correction_kind,''))),
         t.is_adjustment,t.parent_timesheet_id,t.correction_id
    into v_contract_id,v_sheet_scope,v_self_bill,v_line_type,v_is_current,v_archived_at,v_revoked_at,
         v_adjustment_origin,v_correction_kind,v_is_adjustment,
         v_parent_timesheet_id,v_correction_id
    from public.timesheets t
    left join public.contracts c on c.id=t.contract_id
    where t.timesheet_id=p_timesheet_id;
  if not found or v_is_current is distinct from true
     or v_archived_at is not null or v_revoked_at is not null then
    raise exception using errcode='23514', message='BPAY_NEXT_APPROVAL_ROUTE_NOT_CURRENT';
  end if;
  if v_contract_id is null then
    if v_sheet_scope is distinct from 'DAILY' or v_line_type is distinct from 'HOURS'
       or v_is_adjustment is true or v_parent_timesheet_id is not null or v_correction_id is not null
       or v_adjustment_origin<>'' or v_correction_kind<>'' then
      raise exception using errcode='23514',message='BPAY_NEXT_CONTRACTLESS_DAILY_ROUTE_REQUIRED';
    end if;
    return 'DAILY_HOURS';
  end if;
  if v_line_type in ('EXPENSES','MILEAGE') then
    return 'EXPENSE_CARRIER';
  end if;
  if v_line_type='HOURS' then
    -- Imported reversal/replacement children remain physical/invoice facts;
    -- NEXT realises the approved complete Source base, never the retired pair.
    -- Either typed marker requires genuine provenance: a malformed child must
    -- not fall through to the ordinary or self-bill Source financial producer.
    if v_adjustment_origin in (
         'IMPORT_CORRECTION','IMPORT_CANCELLATION',
         'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
         'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
       ) or v_correction_kind in (
         'CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT',
         'CANCELLATION_REVERSAL','CANCELLATION_REPLACEMENT'
       ) then
      v_import_classification:=public._ctms_import_correction_classify_v1(p_timesheet_id);
      if v_is_adjustment is distinct from true
         or v_parent_timesheet_id is null or v_correction_id is null
         or jsonb_typeof(v_import_classification) is distinct from 'object'
         or (v_import_classification->>'timesheet_id') is distinct from p_timesheet_id::text
         or (v_import_classification->>'correction_id') is distinct from v_correction_id
         or (v_import_classification->>'adjustment_origin') is distinct from v_adjustment_origin
         or (v_import_classification->>'correction_kind') is distinct from v_correction_kind
         or (v_import_classification->'is_import_authoritative_correction')
              is distinct from 'true'::jsonb then
        raise exception using errcode='23514', message='BPAY_NEXT_IMPORT_CORRECTION_PROVENANCE_INVALID';
      end if;
      return 'FACTUAL_IMPORT_CORRECTION';
    end if;
    -- Self-bill is import-authoritative by the product decision. During the
    -- real first-authorisation owner the financial row is stamped BEFORE its
    -- Source authorisation record is inserted. Do not misroute that temporary
    -- state to the ordinary producer or refuse the legitimate Source call.
    -- The Source stage still requires the completed authorisation and exact
    -- inventory before it can publish any pay revision.
    if v_self_bill is true then
      return 'SOURCE_HOURS';
    end if;
    -- Non-self-bill imported HOURS (including Magnit) use the installed
    -- resolved family classifier, not an untrusted import label.
    v_source_root:=private._candidate_expense_source_family_v1(p_timesheet_id);
    if v_source_root then
      return 'SOURCE_HOURS';
    end if;
    return 'ORDINARY_HOURS';
  end if;
  raise exception using errcode='23514', message='BPAY_NEXT_APPROVAL_ROUTE_UNSUPPORTED';
end
$function$;

create or replace function private.bpay_next_publish_ordinary_from_financial_v1(
  p_financial_id uuid
) returns uuid
language plpgsql security definer
set search_path = pg_catalog, public, private
as $function$
declare
  v_tf public.timesheets_financials%rowtype;
  v_ts public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_candidate_id uuid;
  v_client_id uuid;
  v_contract_id uuid;
  v_role text;
  v_band text;
  v_daily boolean;
  v_daily_evidence jsonb;
  v_work private.bpay_next_work%rowtype;
  v_revision_id uuid;
  v_line_id uuid;
  v_shift_id uuid;
  v_command_id uuid:=pg_catalog.gen_random_uuid();
  v_mode text;
  v_route text;
  v_detail_kind text;
  v_segment jsonb;
  v_segment_row record;
  v_schedule jsonb;
  v_shift_entry jsonb;
  v_extra record;
  v_bucket record;
  v_break jsonb;
  v_segment_count integer:=0;
  v_additional_count integer:=0;
  v_expense_count integer:=0;
  v_mileage_count integer:=0;
  v_base_count integer:=0;
  v_expected_count integer;
  v_schedule_count integer;
  v_resolved_daily_rates boolean;
  v_available_daily_evidence jsonb;
  v_available_rows jsonb;
  v_source_umbrella_id uuid;
  v_source_candidate_channel text;
  v_extra_code text;
  v_extra_quantity numeric;
  v_extra_pay_rate numeric;
  v_line_no integer:=0;
  v_rate_detail_count integer;
  v_break_no integer;
  v_shift_no integer;
  v_break_total integer;
  v_break_mins integer;
  v_begin_mins integer;
  v_end_mins integer;
  v_break_start text;
  v_break_end text;
  v_shift_start_local text;
  v_shift_end_local text;
  v_hours numeric;
  v_amount numeric;
  v_base_amount numeric;
  v_expense_total numeric;
  v_expected_total numeric;
  v_written_total numeric:=0;
  v_work_date date;
  v_component_key text;
  v_excluded boolean;
  v_candidate_name text;
  v_candidate_ref text;
  v_candidate_mileage_pay_rate numeric;
  v_client_name text;
  v_client_mileage_charge_rate numeric;
  v_mileage_pay_rate numeric;
  v_mileage_charge_rate numeric;
begin
  if p_financial_id is null then
    raise exception using errcode='22023', message='BPAY_NEXT_ORDINARY_FINANCIAL_ID_REQUIRED';
  end if;
  if (select active_owner from private.bpay_next_module_control
        where id=1 for share)<>'NEXT' then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_tf from public.timesheets_financials
    where id=p_financial_id;
  select * into strict v_ts from public.timesheets
    where timesheet_id=v_tf.timesheet_id;
  v_route:=private.bpay_next_approval_route_v1(v_ts.timesheet_id);
  v_daily:=v_route='DAILY_HOURS';
  if v_daily then
    v_daily_evidence:=private.bpay_next_daily_approval_evidence_v1(v_tf.id);
    v_candidate_id:=v_tf.candidate_id; v_client_id:=v_tf.client_id;
    v_contract_id:=null; v_role:=v_tf.role; v_band:=v_tf.band;
    v_work_date:=(v_daily_evidence->>'work_date')::date;
  else
    select c.* into strict v_contract from public.contracts c where c.id=v_ts.contract_id;
    v_candidate_id:=v_contract.candidate_id; v_client_id:=v_contract.client_id;
    v_contract_id:=v_contract.id; v_role:=v_contract.role; v_band:=v_contract.band;
  end if;
  if v_route not in ('ORDINARY_HOURS','DAILY_HOURS','EXPENSE_CARRIER')
     or v_tf.is_current is distinct from true
     or v_tf.is_stale is distinct from false
     or v_tf.authorised_at_utc is null
     or v_ts.authorised_at_server is null
     or v_tf.timesheet_version<>v_ts.version
     or v_tf.candidate_id is distinct from v_candidate_id
     or v_tf.client_id is distinct from v_client_id
     or upper(coalesce(v_tf.pay_method,'')) not in ('PAYE','UMBRELLA') then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_APPROVAL_NOT_CURRENT';
  end if;
  select coalesce(nullif(btrim(c.display_name),''),
                  nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),
                  c.id::text),c.tms_ref,c.mileage_pay_rate,c.pay_method,c.umbrella_id
    into strict v_candidate_name,v_candidate_ref,v_candidate_mileage_pay_rate,
      v_source_candidate_channel,v_source_umbrella_id
    from public.candidates c where c.id=v_candidate_id for share;
  if upper(v_tf.pay_method)='PAYE' then
    v_source_umbrella_id:=null;
  elsif upper(coalesce(v_source_candidate_channel,''))<>'UMBRELLA'
     or v_source_umbrella_id is null then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SOURCE_UMBRELLA_UNAVAILABLE';
  end if;
  select name,mileage_charge_rate into strict
    v_client_name,v_client_mileage_charge_rate from public.clients
    where id=v_client_id;
  v_mode:=upper(coalesce(v_tf.invoice_breakdown_json->>'mode',''));
  if v_mode='SEGMENTS' then
    if jsonb_typeof(v_tf.invoice_breakdown_json->'segments')<>'array' then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SEGMENTS_NOT_ARRAY';
    end if;
    v_segment_count:=jsonb_array_length(v_tf.invoice_breakdown_json->'segments');
    -- Validate identities before grouping. Physical segment IDs are retained
    -- as original evidence, not continuing financial obligations.
    for v_segment in select value from jsonb_array_elements(
      v_tf.invoice_breakdown_json->'segments')
    loop
      if jsonb_typeof(v_segment)<>'object'
         or nullif(btrim(v_segment->>'segment_id'),'') is null
         or coalesce(v_segment->>'date','') !~ '^\d{4}-\d{2}-\d{2}$' then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SEGMENT_ID_OR_DATE_INVALID';
      end if;
    end loop;
    select count(distinct (value->>'date')::date) into v_segment_count
      from jsonb_array_elements(v_tf.invoice_breakdown_json->'segments');
    v_detail_kind:='SHIFT';
  elsif v_mode='AGGREGATE' then
    v_detail_kind:='AGGREGATE';
    v_base_amount:=v_tf.total_pay_ex_vat-v_tf.additional_pay_ex_vat
      -coalesce(v_tf.expenses_pay_ex_vat,0)-coalesce(v_tf.mileage_pay_ex_vat,0);
    v_base_count:=case when v_daily or v_base_amount<>0 or v_tf.total_hours<>0 then 1 else 0 end;
  elsif v_mode='EXPENSES_ONLY' and v_route='EXPENSE_CARRIER' then
    v_detail_kind:='FIXED';
  else
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_FINANCIAL_MODE_UNSUPPORTED';
  end if;
  if jsonb_typeof(v_tf.additional_units_json)<>'object' then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_NOT_OBJECT';
  end if;
  select count(*),count(distinct upper(btrim(key)))
    into v_additional_count,v_rate_detail_count from jsonb_each(v_tf.additional_units_json);
  if v_additional_count<>v_rate_detail_count then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_CODE_CONFLICT';
  end if;
  if v_route='EXPENSE_CARRIER' then
    if v_segment_count<>0 or v_base_count<>0 or v_additional_count<>0
       or v_tf.total_hours<>0 or v_tf.additional_pay_ex_vat<>0
       or v_tf.hours_day<>0 or v_tf.hours_night<>0
       or v_tf.hours_sat<>0 or v_tf.hours_sun<>0 or v_tf.hours_bh<>0
       or v_tf.total_pay_ex_vat is distinct from
          coalesce(v_tf.expenses_pay_ex_vat,0)+coalesce(v_tf.mileage_pay_ex_vat,0) then
      raise exception using errcode='23514', message='BPAY_NEXT_EXPENSE_CARRIER_MONEY_SHAPE_INVALID';
    end if;
    v_detail_kind:='FIXED';
  end if;
  v_expense_total:=coalesce(v_tf.travel_pay_ex_vat,0)
    +coalesce(v_tf.accommodation_pay_ex_vat,0)+coalesce(v_tf.other_pay_ex_vat,0);
  if v_expense_total<>0 then
    if coalesce(v_tf.expenses_pay_ex_vat,0) not in (0,v_expense_total) then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_EXPENSE_TOTAL_CONFLICT';
    end if;
    v_expense_count:=(case when v_tf.travel_pay_ex_vat<>0 then 1 else 0 end)
      +(case when v_tf.accommodation_pay_ex_vat<>0 then 1 else 0 end)
      +(case when v_tf.other_pay_ex_vat<>0 then 1 else 0 end);
  else
    v_expense_total:=coalesce(v_tf.expenses_pay_ex_vat,0);
    v_expense_count:=case when v_expense_total<>0 then 1 else 0 end;
  end if;
  v_mileage_count:=case when v_tf.mileage_pay_ex_vat<>0
    or v_tf.mileage_units<>0 then 1 else 0 end;
  v_expected_count:=v_segment_count+v_base_count+v_additional_count
    +v_expense_count+v_mileage_count;
  -- Both production snapshot writers include additional pay, expenses and
  -- mileage in total_pay_ex_vat. They are components of this total, not extras.
  v_expected_total:=v_tf.total_pay_ex_vat;
  if v_expected_total<>round(v_expected_total,2)
     or v_tf.total_pay_ex_vat<>round(v_tf.total_pay_ex_vat,2)
     or v_tf.additional_pay_ex_vat<>round(v_tf.additional_pay_ex_vat,2)
     or v_tf.expenses_pay_ex_vat<>round(v_tf.expenses_pay_ex_vat,2)
     or v_tf.travel_pay_ex_vat<>round(v_tf.travel_pay_ex_vat,2)
     or v_tf.accommodation_pay_ex_vat<>round(v_tf.accommodation_pay_ex_vat,2)
     or v_tf.other_pay_ex_vat<>round(v_tf.other_pay_ex_vat,2)
     or v_tf.mileage_pay_ex_vat<>round(v_tf.mileage_pay_ex_vat,2) then
    raise exception using errcode='22003', message='BPAY_NEXT_ORDINARY_MONEY_PRECISION_INVALID';
  end if;
  v_mileage_pay_rate:=coalesce(v_tf.mileage_pay_rate,
                               v_contract.mileage_pay_rate,
                               v_candidate_mileage_pay_rate);
  v_mileage_charge_rate:=coalesce(v_tf.mileage_charge_rate,
                                  v_contract.mileage_charge_rate,
                                  v_client_mileage_charge_rate);
  if v_mileage_count>0 and v_mileage_pay_rate is null then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
      detail=jsonb_build_object('bucket','MILE','contract_id',v_contract_id)::text;
  end if;
  v_resolved_daily_rates:=v_route in ('ORDINARY_HOURS','DAILY_HOURS')
    and v_ts.sheet_scope='DAILY'::public.timesheet_scope_enum;
  if v_resolved_daily_rates then
    if v_daily then
      v_available_daily_evidence:=v_daily_evidence;
    else
      v_available_daily_evidence:=private.bpay_next_daily_approval_evidence_v1(v_tf.id);
    end if;
    select (v_available_daily_evidence->'rates')
      ||coalesce(jsonb_agg(to_jsonb(r) order by r.rate_family,r.rate_code),'[]'::jsonb)
      into v_available_rows
      from private.bpay_next_contract_rate_rows_v1(
        '{}'::jsonb,v_contract.bucket_labels_json,v_contract.additional_rates_json,
        v_mileage_pay_rate,v_mileage_charge_rate) r
      where r.rate_family<>'STANDARD' or not exists (
        select 1 from jsonb_array_elements(v_available_daily_evidence->'rates') d
        where d->>'rate_family'='STANDARD' and d->>'rate_code'=r.rate_code);
  else
    select jsonb_agg(to_jsonb(r) order by r.rate_family,r.rate_code)
      into v_available_rows
      from private.bpay_next_contract_rate_rows_v1(
        v_contract.rates_json,v_contract.bucket_labels_json,v_contract.additional_rates_json,
        v_mileage_pay_rate,v_mileage_charge_rate) r;
  end if;
  -- Actual used-only Additional identities retain unknown Contract availability.
  for v_extra in select key,value from jsonb_each(v_tf.additional_units_json) order by key
  loop
    v_extra_code:=upper(btrim(v_extra.key));
    if jsonb_typeof(v_extra.value)<>'object' or v_extra_code=''
       or char_length(v_extra_code)>245 then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_SHAPE_INVALID';
    end if;
    if not exists (select 1 from jsonb_array_elements(v_available_rows) x
       where x->>'rate_family'='ADDITIONAL' and x->>'rate_code'=v_extra_code) then
      v_available_rows:=v_available_rows||jsonb_build_array(jsonb_build_object(
        'rate_family','ADDITIONAL','rate_code',v_extra_code,
        'unit_label',nullif(v_extra.value->>'unit_name',''),
        'paye_rate',null,'umbrella_rate',null,'charge_rate',null));
    end if;
  end loop;
  v_schedule_count:=jsonb_array_length(v_available_rows);

  insert into private.bpay_next_worker_control(candidate_id)
    values (v_candidate_id) on conflict (candidate_id) do nothing;
  select * into v_work from private.bpay_next_work
    where booking_id=v_ts.booking_id for update;
  if not found then
    insert into private.bpay_next_work
      (candidate_id,contract_id,original_timesheet_id,booking_id,
       work_kind,week_ending_date,origin_kind,origin_work_date)
      values (v_candidate_id,v_contract_id,v_ts.timesheet_id,v_ts.booking_id,
              case when v_route='EXPENSE_CARRIER' then 'EXPENSE' else 'ORDINARY' end,
              v_ts.week_ending_date,case when v_daily then 'DAILY' else 'CONTRACT' end,
              case when v_daily then v_work_date end)
      returning * into v_work;
  elsif v_work.candidate_id is distinct from v_candidate_id
     or v_work.contract_id is distinct from v_contract_id
     or v_work.origin_kind is distinct from (case when v_daily then 'DAILY' else 'CONTRACT' end)
     or v_work.origin_work_date is distinct from (case when v_daily then v_work_date end)
     or v_work.week_ending_date is distinct from v_ts.week_ending_date
     or v_work.work_kind<>(case when v_route='EXPENSE_CARRIER'
       then 'EXPENSE' else 'ORDINARY' end) then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_WORK_IDENTITY_CONFLICT';
  end if;
  insert into private.bpay_next_work_revision
    (work_id,revision_no,source_kind,physical_timesheet_id,
     financial_snapshot_id,physical_timesheet_version,source_umbrella_id,
     source_rate_authority_kind,source_rate_override_id,source_rate_default_id,
     source_pay_channel,week_ending_date,detail_kind,
     expected_line_count,expected_rate_schedule_count,certified_zero,
     approved_source_ex_vat,candidate_display_name,candidate_reference,
     client_display_name,job_title,band_label,timesheet_reference,daily_approval_evidence_json)
    values (v_work.id,v_work.current_revision_no+1,
            case when v_route='EXPENSE_CARRIER' then 'EXPENSE' else 'ORDINARY' end,
            v_ts.timesheet_id,v_tf.id,v_ts.version,v_source_umbrella_id,
            coalesce(nullif(v_tf.rate_source_refs_json->>'mode',''),
              nullif(v_tf.rate_source_refs_json->>'kind',''),
              nullif(v_tf.rate_source_refs_json->>'source_kind','')),
            nullif(v_tf.rate_source_refs_json->>'override_id','')::uuid,
            nullif(v_tf.rate_source_refs_json->>'default_id','')::uuid,
            upper(v_tf.pay_method),
            v_ts.week_ending_date,v_detail_kind,v_expected_count,
            v_schedule_count,v_expected_total=0,v_expected_total,
            v_candidate_name,v_candidate_ref,v_client_name,
            v_role,v_band,v_ts.reference_number,v_daily_evidence)
    returning id into v_revision_id;
  insert into private.bpay_next_rate_schedule
    (revision_id,rate_family,rate_code,unit_label,paye_rate,umbrella_rate,charge_rate,
     approved_pay_rate,approved_charge_rate)
    select v_revision_id,r.rate_family,r.rate_code,r.unit_label,
      r.paye_rate,r.umbrella_rate,r.charge_rate,
      private.bpay_next_exact_rate_v1(case r.rate_family
        when 'STANDARD' then case r.rate_code when 'DAY' then v_tf.pay_day
          when 'NIGHT' then v_tf.pay_night when 'SAT' then v_tf.pay_sat
          when 'SUN' then v_tf.pay_sun when 'BH' then v_tf.pay_bh end::text
        when 'MILEAGE' then v_mileage_pay_rate::text
        when 'ADDITIONAL' then coalesce(a.value->>'pay_rate',a.value->>'rate') end),
      private.bpay_next_exact_rate_v1(case r.rate_family
        when 'STANDARD' then case r.rate_code when 'DAY' then v_tf.charge_day
          when 'NIGHT' then v_tf.charge_night when 'SAT' then v_tf.charge_sat
          when 'SUN' then v_tf.charge_sun when 'BH' then v_tf.charge_bh end::text
        when 'MILEAGE' then v_mileage_charge_rate::text
        when 'ADDITIONAL' then a.value->>'charge_rate' end)
    from jsonb_to_recordset(v_available_rows)
      r(rate_family text,rate_code text,unit_label text,paye_rate numeric,
        umbrella_rate numeric,charge_rate numeric)
    left join lateral (select value from jsonb_each(v_tf.additional_units_json)
      where upper(btrim(key))=r.rate_code and r.rate_family='ADDITIONAL') a on true;

  if v_mode='SEGMENTS' then
    -- One pass ordered by stable business date and original array ordinal.
    -- Window sums use exact stored amounts, not a new hours/rates calculator.
    -- Child INSERTs retain every original shift, including zero/excluded ones.
    for v_segment_row in
      select value,ordinality,
        row_number() over day_order as detail_no,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
            then 0 else (value->>'pay_amount')::numeric end) over day_set as day_pay,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_day')::numeric,0)
          +coalesce((value->>'hours_night')::numeric,0)
          +coalesce((value->>'hours_sat')::numeric,0)
          +coalesce((value->>'hours_sun')::numeric,0)
          +coalesce((value->>'hours_bh')::numeric,0) end) over day_set as day_hours,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_day')::numeric,0) end) over day_set as day_day,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_night')::numeric,0) end) over day_set as day_night,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_sat')::numeric,0) end) over day_set as day_sat,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_sun')::numeric,0) end) over day_set as day_sun,
        sum(case when coalesce((value->>'exclude_from_pay')::boolean,false)
          then 0 else coalesce((value->>'hours_bh')::numeric,0) end) over day_set as day_bh
      from jsonb_array_elements(v_tf.invoice_breakdown_json->'segments') with ordinality
      window day_set as (partition by (value->>'date')::date),
        day_order as (partition by (value->>'date')::date order by ordinality)
      order by (value->>'date')::date,ordinality
    loop
      v_segment:=v_segment_row.value;
      if jsonb_typeof(v_segment)<>'object'
         or nullif(btrim(v_segment->>'segment_id'),'') is null
         or coalesce(v_segment->>'date','') !~ '^\d{4}-\d{2}-\d{2}$' then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SEGMENT_ID_OR_DATE_INVALID';
      end if;
      v_work_date:=(v_segment->>'date')::date;
      -- TS_DAY is the supported payment choice. A new physical UUID, edited
      -- clock/break or array order cannot create another DAY obligation.
      -- Per-segment authorisation exclusions remain independent below.
      v_component_key:='WORK:DAY:'||v_work_date::text;
      v_excluded:=coalesce((v_segment->>'exclude_from_pay')::boolean,false);
      v_amount:=case when v_excluded then 0
        else (v_segment->>'pay_amount')::numeric end;
      if v_amount is null or v_amount<>round(v_amount,2) then
        raise exception using errcode='22003', message='BPAY_NEXT_ORDINARY_SEGMENT_AMOUNT_INVALID';
      end if;
      v_hours:=0; v_rate_detail_count:=0;
      for v_bucket in select * from (values
        ('DAY'::text,'hours_day'::text,v_tf.pay_day),
        ('NIGHT','hours_night',v_tf.pay_night),
        ('SAT','hours_sat',v_tf.pay_sat),
        ('SUN','hours_sun',v_tf.pay_sun),
        ('BH','hours_bh',v_tf.pay_bh)) b(code,hours_key,pay_rate)
      loop
        if coalesce((v_segment->>v_bucket.hours_key)::numeric,0)<0
           or coalesce((v_segment->>v_bucket.hours_key)::numeric,0)
                <>round(coalesce((v_segment->>v_bucket.hours_key)::numeric,0),6) then
          raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_SEGMENT_HOURS_INVALID';
        end if;
        v_hours:=v_hours+coalesce((v_segment->>v_bucket.hours_key)::numeric,0);
        if not v_excluded and coalesce((v_segment->>v_bucket.hours_key)::numeric,0)>0 then
          if v_bucket.pay_rate is null then
            raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
              detail=jsonb_build_object('bucket',v_bucket.code,'contract_id',v_contract_id)::text;
          end if;
          if v_bucket.pay_rate<>round(v_bucket.pay_rate,6)
             or abs(v_bucket.pay_rate)>=1000000000000 then
            raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_INVALID';
          end if;
          v_rate_detail_count:=v_rate_detail_count+1;
        end if;
      end loop;
      if v_segment_row.detail_no=1 then
        v_rate_detail_count:=(case when v_segment_row.day_day>0 then 1 else 0 end)
          +(case when v_segment_row.day_night>0 then 1 else 0 end)
          +(case when v_segment_row.day_sat>0 then 1 else 0 end)
          +(case when v_segment_row.day_sun>0 then 1 else 0 end)
          +(case when v_segment_row.day_bh>0 then 1 else 0 end);
        v_line_no:=v_line_no+1;
        insert into private.bpay_next_approved_line
          (revision_id,line_no,component_key,component_kind,work_date,
           approved_quantity,expected_rate_detail_count,source_pay_ex_vat,tax_treatment,evidence_ref)
          values (v_revision_id,v_line_no,v_component_key,'WORK',v_work_date,
            v_segment_row.day_hours,v_rate_detail_count,v_segment_row.day_pay,'TAXABLE',
            'timesheets_financials:'||v_tf.id::text||'#day:'||v_work_date::text)
          returning id into v_line_id;
        insert into private.bpay_next_rate_detail
          (approved_line_id,bucket,approved_hours,source_pay_rate)
          select v_line_id,b.code,b.hours,b.pay_rate from (values
            ('DAY'::text,v_segment_row.day_day,v_tf.pay_day),
            ('NIGHT',v_segment_row.day_night,v_tf.pay_night),
            ('SAT',v_segment_row.day_sat,v_tf.pay_sat),
            ('SUN',v_segment_row.day_sun,v_tf.pay_sun),
            ('BH',v_segment_row.day_bh,v_tf.pay_bh)) b(code,hours,pay_rate)
          where b.hours>0;
      end if;
      v_shift_start_local:=nullif(btrim(coalesce(v_segment->>'start_local',v_segment->>'start','')),'');
      v_shift_end_local:=nullif(btrim(coalesce(v_segment->>'end_local',v_segment->>'end','')),'');
      if (v_shift_start_local is null)<>(v_shift_end_local is null)
         or (v_shift_start_local is not null and
           (v_shift_start_local !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
            or v_shift_end_local !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$')) then
        raise exception using errcode='23514',message='BPAY_NEXT_ORDINARY_SCHEDULE_CLOCK_INVALID';
      end if;
      insert into private.bpay_next_shift_detail
        (approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,
         shift_start_local,shift_end_local,shift_overnight,
         approved_minutes,approved_hours,immutable_evidence_ref,
         segment_pay_ex_vat,pay_excluded,hours_day,hours_night,hours_sat,hours_sun,hours_bh,segment_key)
        values (v_line_id,v_segment_row.detail_no::integer,v_work_date,
          nullif(v_segment->>'start_utc','')::timestamptz,
          nullif(v_segment->>'end_utc','')::timestamptz,
          v_shift_start_local,v_shift_end_local,
          nullif(v_segment->>'overnight','')::boolean,
          case when v_hours*60=trunc(v_hours*60) then (v_hours*60)::integer end,
          v_hours,'timesheets_financials:'||v_tf.id::text||'#segment:'||v_segment_row.ordinality::text,
          v_amount,v_excluded,
          coalesce((v_segment->>'hours_day')::numeric,0),
          coalesce((v_segment->>'hours_night')::numeric,0),
          coalesce((v_segment->>'hours_sat')::numeric,0),
          coalesce((v_segment->>'hours_sun')::numeric,0),
          coalesce((v_segment->>'hours_bh')::numeric,0),v_segment->>'segment_id') returning id into v_shift_id;
      v_break_total:=0; v_break_no:=0;
      if v_segment ? 'breaks' and jsonb_typeof(v_segment->'breaks')<>'array' then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_BREAK_SHAPE_INVALID';
      end if;
      for v_break in select value from jsonb_array_elements(
        coalesce(v_segment->'breaks','[]'::jsonb))
      loop
        v_break_start:=v_break->>'start';
        v_break_end:=v_break->>'end';
        if jsonb_typeof(v_break)<>'object'
           or coalesce(v_break_start,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or coalesce(v_break_end,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
          raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_BREAK_CLOCK_INVALID';
        end if;
        v_begin_mins:=substring(v_break_start,1,2)::integer*60
          +substring(v_break_start,4,2)::integer;
        v_end_mins:=substring(v_break_end,1,2)::integer*60
          +substring(v_break_end,4,2)::integer;
        v_break_mins:=(v_end_mins-v_begin_mins+1440)%1440;
        if v_break_mins=0 then
          raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_BREAK_ZERO_WINDOW';
        end if;
        v_break_no:=v_break_no+1;
        v_break_total:=v_break_total+v_break_mins;
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_start_local,break_end_local,break_minutes)
          values (v_shift_id,v_break_no,v_break_start,v_break_end,v_break_mins);
      end loop;
      if v_break_no=0 and coalesce((v_segment->>'break_mins')::integer,
                                   (v_segment->>'break_minutes')::integer,0)>0 then
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_minutes)
          values (v_shift_id,1,coalesce((v_segment->>'break_mins')::integer,
                                        (v_segment->>'break_minutes')::integer));
      elsif v_break_no>0 and (v_segment ? 'break_mins' or v_segment ? 'break_minutes')
         and v_break_total<>coalesce((v_segment->>'break_mins')::integer,
                                    (v_segment->>'break_minutes')::integer) then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_BREAK_DURATION_CONFLICT';
      end if;
      v_written_total:=v_written_total+v_amount;
    end loop;
  elsif v_base_count=1 then
    for v_bucket in select * from (values
      ('DAY'::text,v_tf.hours_day,v_tf.pay_day),('NIGHT',v_tf.hours_night,v_tf.pay_night),
      ('SAT',v_tf.hours_sat,v_tf.pay_sat),('SUN',v_tf.hours_sun,v_tf.pay_sun),
      ('BH',v_tf.hours_bh,v_tf.pay_bh)) b(code,hours,pay_rate)
    loop
      if v_bucket.hours>0 and v_bucket.pay_rate is null then
        raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
          detail=jsonb_build_object('bucket',v_bucket.code,'contract_id',v_contract_id)::text;
      end if;
      if v_bucket.hours<0 or v_bucket.hours<>round(v_bucket.hours,6)
         or (v_bucket.hours>0 and
             (v_bucket.pay_rate<>round(v_bucket.pay_rate,6)
              or abs(v_bucket.pay_rate)>=1000000000000)) then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_AGGREGATE_RATE_INVALID';
      end if;
    end loop;
    v_rate_detail_count:=
      (case when v_tf.hours_day>0 then 1 else 0 end)
      +(case when v_tf.hours_night>0 then 1 else 0 end)
      +(case when v_tf.hours_sat>0 then 1 else 0 end)
      +(case when v_tf.hours_sun>0 then 1 else 0 end)
      +(case when v_tf.hours_bh>0 then 1 else 0 end);
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,component_kind,work_date,approved_quantity,
       expected_rate_detail_count,source_pay_ex_vat,tax_treatment,evidence_ref)
      values (v_revision_id,v_line_no,
              case when v_daily then 'WORK:DAY:'||v_work_date::text else 'WORK:AGGREGATE' end,
              'WORK',case when v_daily then v_work_date end,v_tf.total_hours,
              v_rate_detail_count,v_base_amount,'TAXABLE',
              'timesheets_financials:'||v_tf.id::text||'#aggregate')
      returning id into v_line_id;
    insert into private.bpay_next_rate_detail
      (approved_line_id,bucket,approved_hours,source_pay_rate)
      select v_line_id,b.code,b.hours,b.pay_rate
      from (values ('DAY'::text,v_tf.hours_day,v_tf.pay_day),
                   ('NIGHT',v_tf.hours_night,v_tf.pay_night),
                   ('SAT',v_tf.hours_sat,v_tf.pay_sat),
                   ('SUN',v_tf.hours_sun,v_tf.pay_sun),
                   ('BH',v_tf.hours_bh,v_tf.pay_bh)) b(code,hours,pay_rate)
      where b.hours>0;
    if v_daily then
      insert into private.bpay_next_shift_detail
        (approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,
         shift_start_local,shift_end_local,shift_overnight,approved_minutes,approved_hours,
         immutable_evidence_ref,segment_pay_ex_vat,pay_excluded,hours_day,hours_night,hours_sat,hours_sun,hours_bh)
        values (v_line_id,1,v_work_date,v_tf.worked_start_iso,v_tf.worked_end_iso,
          to_char(v_tf.worked_start_iso at time zone 'Europe/London','HH24:MI'),
          to_char(v_tf.worked_end_iso at time zone 'Europe/London','HH24:MI'),
          (v_tf.worked_end_iso at time zone 'Europe/London')::date>v_work_date,
          case when v_tf.total_hours*60=trunc(v_tf.total_hours*60) then (v_tf.total_hours*60)::integer end,
          v_tf.total_hours,'timesheets_financials:'||v_tf.id::text||'#daily',v_base_amount,false,
          v_tf.hours_day,v_tf.hours_night,v_tf.hours_sat,v_tf.hours_sun,v_tf.hours_bh)
        returning id into v_shift_id;
      if v_tf.break_start_iso is not null or coalesce(v_tf.break_minutes,0)>0 then
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_start_at,break_end_at,break_minutes)
          values (v_shift_id,1,v_tf.break_start_iso,v_tf.break_end_iso,
            coalesce(v_tf.break_minutes,extract(epoch from (v_tf.break_end_iso-v_tf.break_start_iso))/60)::integer);
      end if;
    else
    -- An AGGREGATE money line can still originate from an entered weekly
    -- schedule. Keep the entered date/clock/break evidence as small typed
    -- children, without inventing a money split across those shifts.
    if v_tf.actual_schedule_json is not null
       and jsonb_typeof(v_tf.actual_schedule_json)<>'array' then
      raise exception using errcode='23514',
        message='BPAY_NEXT_ORDINARY_SCHEDULE_SHAPE_INVALID';
    end if;
    v_schedule:=case
      when jsonb_typeof(v_tf.actual_schedule_json)='array'
       and jsonb_array_length(v_tf.actual_schedule_json)>0
        then v_tf.actual_schedule_json
      else v_ts.actual_schedule_json end;
    if v_schedule is not null and jsonb_typeof(v_schedule)<>'array' then
      raise exception using errcode='23514',
        message='BPAY_NEXT_ORDINARY_SCHEDULE_SHAPE_INVALID';
    end if;
    v_shift_no:=0;
    for v_shift_entry in select value from jsonb_array_elements(
      coalesce(v_schedule,'[]'::jsonb))
    loop
      if jsonb_typeof(v_shift_entry)<>'object'
         or coalesce(v_shift_entry->>'date',v_shift_entry->>'work_date','')
              !~ '^\d{4}-\d{2}-\d{2}$' then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_SCHEDULE_DATE_INVALID';
      end if;
      v_shift_start_local:=nullif(btrim(coalesce(v_shift_entry->>'start','')),'');
      v_shift_end_local:=nullif(btrim(coalesce(v_shift_entry->>'end','')),'');
      if (v_shift_start_local is null)<>(v_shift_end_local is null)
         or (v_shift_start_local is not null and
             (v_shift_start_local !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
              or v_shift_end_local !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$')) then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_SCHEDULE_CLOCK_INVALID';
      end if;
      v_shift_no:=v_shift_no+1;
      insert into private.bpay_next_shift_detail
        (approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,
         shift_start_local,shift_end_local,approved_minutes,approved_hours)
        values (v_line_id,v_shift_no,
          coalesce(v_shift_entry->>'date',v_shift_entry->>'work_date')::date,
          nullif(v_shift_entry->>'start_utc','')::timestamptz,
          nullif(v_shift_entry->>'end_utc','')::timestamptz,
          v_shift_start_local,v_shift_end_local,
          nullif(v_shift_entry->>'worked_minutes','')::integer,
          nullif(v_shift_entry->>'approved_hours','')::numeric)
        returning id into v_shift_id;
      if v_shift_entry ? 'breaks'
         and jsonb_typeof(v_shift_entry->'breaks') not in ('array','null') then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_BREAK_SHAPE_INVALID';
      end if;
      v_break_no:=0; v_break_total:=0;
      for v_break in select value from jsonb_array_elements(
        case when jsonb_typeof(v_shift_entry->'breaks')='array'
          then v_shift_entry->'breaks' else '[]'::jsonb end)
      loop
        v_break_start:=v_break->>'start';
        v_break_end:=v_break->>'end';
        if jsonb_typeof(v_break)<>'object'
           or coalesce(v_break_start,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or coalesce(v_break_end,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
          raise exception using errcode='23514',
            message='BPAY_NEXT_ORDINARY_BREAK_CLOCK_INVALID';
        end if;
        v_begin_mins:=substring(v_break_start,1,2)::integer*60
          +substring(v_break_start,4,2)::integer;
        v_end_mins:=substring(v_break_end,1,2)::integer*60
          +substring(v_break_end,4,2)::integer;
        v_break_mins:=(v_end_mins-v_begin_mins+1440)%1440;
        if v_break_mins=0 then
          raise exception using errcode='23514',
            message='BPAY_NEXT_ORDINARY_BREAK_ZERO_WINDOW';
        end if;
        v_break_no:=v_break_no+1;
        v_break_total:=v_break_total+v_break_mins;
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_start_local,break_end_local,break_minutes)
          values (v_shift_id,v_break_no,v_break_start,v_break_end,v_break_mins);
      end loop;
      if v_break_no=0
         and coalesce((v_shift_entry->>'break_minutes')::integer,
                      (v_shift_entry->>'break_mins')::integer,0)>0 then
        insert into private.bpay_next_break_detail
          (shift_detail_id,break_no,break_minutes)
          values (v_shift_id,1,
            coalesce((v_shift_entry->>'break_minutes')::integer,
                     (v_shift_entry->>'break_mins')::integer));
      elsif v_break_no>0
         and (v_shift_entry ? 'break_minutes' or v_shift_entry ? 'break_mins')
         and v_break_total<>coalesce((v_shift_entry->>'break_minutes')::integer,
                                    (v_shift_entry->>'break_mins')::integer) then
        raise exception using errcode='23514',
          message='BPAY_NEXT_ORDINARY_BREAK_DURATION_CONFLICT';
      end if;
    end loop;
    end if;
    v_written_total:=v_written_total+v_base_amount;
  end if;

  for v_extra in select key,value from jsonb_each(v_tf.additional_units_json)
                 order by key
  loop
    if jsonb_typeof(v_extra.value)<>'object'
       or nullif(btrim(v_extra.key),'') is null
       or length('ADDITIONAL:'||upper(btrim(v_extra.key)))>256 then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_SHAPE_INVALID';
    end if;
    v_extra_quantity:=coalesce((v_extra.value->>'unit_count')::numeric,
                              (v_extra.value->>'units_week')::numeric);
    v_extra_pay_rate:=coalesce((v_extra.value->>'pay_rate')::numeric,
                              (v_extra.value->>'rate')::numeric);
    v_amount:=coalesce((v_extra.value->>'pay_ex_vat')::numeric,
      (v_extra.value->>'amount_ex_vat')::numeric,v_extra_quantity*v_extra_pay_rate);
    if v_amount is null or (v_extra_pay_rate is null and
        (coalesce(v_extra_quantity,0)<>0 or v_amount<>0)) then
      raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
        detail=jsonb_build_object('bucket',upper(btrim(v_extra.key)),
          'contract_id',v_contract_id)::text;
    end if;
    if v_amount<>round(v_amount,2) then
      raise exception using errcode='22003', message='BPAY_NEXT_ORDINARY_ADDITIONAL_PRECISION_INVALID';
    end if;
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,component_kind,unit_label,
       approved_quantity,approved_unit_rate,source_pay_ex_vat,evidence_ref)
      values (v_revision_id,v_line_no,'ADDITIONAL:'||upper(btrim(v_extra.key)),
        'ADDITIONAL',nullif(v_extra.value->>'unit_name',''),
        coalesce((v_extra.value->>'unit_count')::numeric,
                 (v_extra.value->>'units_week')::numeric),
        coalesce((v_extra.value->>'pay_rate')::numeric,
                 (v_extra.value->>'rate')::numeric),v_amount,
        'timesheets_financials:'||v_tf.id::text||'#additional:'||v_extra.key);
    v_written_total:=v_written_total+v_amount;
  end loop;
  if v_written_total <> v_tf.total_pay_ex_vat-v_expense_total
      -v_tf.mileage_pay_ex_vat then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_WORK_TOTAL_MISMATCH';
  end if;

  for v_extra in select * from (values
      ('TRAVEL'::text,v_tf.travel_pay_ex_vat),
      ('ACCOMMODATION',v_tf.accommodation_pay_ex_vat),
      ('OTHER',v_tf.other_pay_ex_vat),
      ('EXPENSES',case when v_expense_count=1
        and v_tf.travel_pay_ex_vat=0 and v_tf.accommodation_pay_ex_vat=0
        and v_tf.other_pay_ex_vat=0 then v_expense_total else 0 end)) x(code,amount)
  loop
    if v_extra.amount<>0 then
      if v_extra.amount<>round(v_extra.amount,2) then
        raise exception using errcode='22003', message='BPAY_NEXT_ORDINARY_EXPENSE_PRECISION_INVALID';
      end if;
      v_line_no:=v_line_no+1;
      insert into private.bpay_next_approved_line
        (revision_id,line_no,component_key,component_kind,
         source_pay_ex_vat,evidence_ref)
        values (v_revision_id,v_line_no,'EXPENSE:'||v_extra.code,'EXPENSE',
          v_extra.amount,'timesheets_financials:'||v_tf.id::text||'#expense:'||v_extra.code);
      v_written_total:=v_written_total+v_extra.amount;
    end if;
  end loop;
  if v_mileage_count=1 then
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,component_kind,unit_label,
       approved_quantity,approved_unit_rate,source_pay_ex_vat,evidence_ref)
      values (v_revision_id,v_line_no,'MILEAGE:MILE','MILEAGE','mile',
        v_tf.mileage_units,v_mileage_pay_rate,v_tf.mileage_pay_ex_vat,
        'timesheets_financials:'||v_tf.id::text||'#mileage');
    v_written_total:=v_written_total+v_tf.mileage_pay_ex_vat;
  end if;
  if v_line_no<>v_expected_count or v_written_total<>v_expected_total then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_COMPONENT_TOTAL_MISMATCH';
  end if;
  perform private.bpay_next_publish_staged_revision_v1(
    v_work.id,v_revision_id,v_command_id);
  return v_revision_id;
end
$function$;

create or replace function private.bpay_next_ordinary_authorisation_trigger_v1()
returns trigger
language plpgsql security definer
set search_path = pg_catalog, public, private
as $function$
declare
  v_new_authorisation boolean;
  v_removed_authorisation boolean;
  v_route text;
  v_booking_id text;
  v_contract_id uuid;
  v_candidate_id uuid;
  v_work private.bpay_next_work%rowtype;
  v_physical_timesheet_id uuid;
  v_physical_version integer;
begin
  if tg_op='INSERT' then
    v_new_authorisation:=new.authorised_at_utc is not null;
    v_removed_authorisation:=false;
  else
    v_new_authorisation:=old.authorised_at_utc is null
      and new.authorised_at_utc is not null;
    v_removed_authorisation:=old.authorised_at_utc is not null
      and new.authorised_at_utc is null;
  end if;
  if not (v_new_authorisation or v_removed_authorisation)
     or (select active_owner from private.bpay_next_module_control
          where id=1)<>'NEXT' then
    return new;
  end if;
  if v_new_authorisation then
    v_route:=private.bpay_next_approval_route_v1(new.timesheet_id);
    if v_route in ('ORDINARY_HOURS','DAILY_HOURS','EXPENSE_CARRIER') then
      perform private.bpay_next_publish_ordinary_from_financial_v1(new.id);
    end if;
  else
    -- Unauthorisation changes what may be offered next; it does not erase a
    -- revision already captured into a frozen offer or rewrite paid history.
    select t.booking_id,t.contract_id,
           case when t.contract_id is null then old.candidate_id else c.candidate_id end
      into strict v_booking_id,v_contract_id,v_candidate_id
      from public.timesheets t
      left join public.contracts c on c.id=t.contract_id
      where t.timesheet_id=old.timesheet_id;
    if v_candidate_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_ORDINARY_UNAUTHORISE_CANDIDATE_REQUIRED';
    end if;
    insert into private.bpay_next_worker_control(candidate_id)
      values (v_candidate_id) on conflict(candidate_id) do nothing;
    perform 1 from private.bpay_next_worker_control
      where candidate_id=v_candidate_id for update;
    select * into v_work from private.bpay_next_work
      where booking_id=v_booking_id for update;
    -- The installed unauthorisation owner stamps revoked_at BEFORE clearing
    -- the financial row. Its approval route rightly rejects revoked rows;
    -- use the already-published exact work identity for this transition.
    if not found or v_work.work_kind='SOURCE' then
      return new;
    end if;
    select r.physical_timesheet_id,r.physical_timesheet_version into strict v_physical_timesheet_id,v_physical_version
      from private.bpay_next_work_revision r
      where r.id=v_work.current_revision_id and r.work_id=v_work.id;
    if v_work.candidate_id is distinct from v_candidate_id
       or v_work.contract_id is distinct from v_contract_id
       or v_work.work_kind not in ('EXPENSE','ORDINARY')
       or v_work.approval_state<>'APPROVED'
       or v_physical_timesheet_id is distinct from old.timesheet_id
       or v_physical_version is distinct from old.timesheet_version then
      raise exception using errcode='23514',
        message='BPAY_NEXT_ORDINARY_UNAUTHORISE_WORK_MISMATCH';
    end if;
    update private.bpay_next_work
      set approval_state='WITHDRAWN',current_revision_id=null,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_work.id;
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_candidate_id;
    perform private.bpay_next_workbench_refresh_work_v1(v_work.id);
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_revision_guard_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_missing_detail boolean;
  v_line_count bigint;
  v_total_source_ex numeric;
  v_daily_work private.bpay_next_work%rowtype;
begin
  if tg_op = 'INSERT' then
    if new.approved_at_utc is not null or new.sealed_at_utc is not null then
      raise exception using errcode='23514', message='BPAY_NEXT_DIRECT_SEALED_INSERT_FORBIDDEN';
    end if;
    return new;
  end if;
  if tg_op = 'DELETE' then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_DELETE_FORBIDDEN';
  end if;
  if old.sealed_at_utc is not null then
    raise exception using errcode='23514', message='BPAY_NEXT_SEALED_REVISION_IMMUTABLE';
  end if;
  if (to_jsonb(new) - 'approved_at_utc' - 'sealed_at_utc')
     is distinct from (to_jsonb(old) - 'approved_at_utc' - 'sealed_at_utc')
     or new.approved_at_utc is null or new.sealed_at_utc is null then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_SEAL_ONLY';
  end if;
  select count(*),coalesce(sum(source_pay_ex_vat),0)
    into v_line_count,v_total_source_ex
    from private.bpay_next_approved_line l where l.revision_id=old.id;
  if v_line_count<>new.expected_line_count
     or v_total_source_ex<>new.approved_source_ex_vat then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_INCOMPLETE_OR_TOTAL_MISMATCH';
  end if;
  if new.financial_snapshot_id is not null and not exists (
    select 1 from public.timesheets_financials tf
     where tf.id=new.financial_snapshot_id
       and tf.timesheet_id=new.physical_timesheet_id
       and tf.timesheet_version=new.physical_timesheet_version
  ) then
    raise exception using errcode='23514', message='BPAY_NEXT_FINANCIAL_SNAPSHOT_IDENTITY_MISMATCH';
  end if;
  if exists (
    select 1 from private.bpay_next_approved_line l
    left join private.bpay_next_rate_detail rd on rd.approved_line_id=l.id
    where l.revision_id=old.id
    group by l.id,l.expected_rate_detail_count
    having count(rd.id)<>l.expected_rate_detail_count
  ) then
    raise exception using errcode='23514', message='BPAY_NEXT_RATE_DETAIL_COUNT_MISMATCH';
  end if;
  if (select count(*) from private.bpay_next_rate_schedule s
      where s.revision_id=old.id)<>new.expected_rate_schedule_count then
    raise exception using errcode='23514', message='BPAY_NEXT_RATE_SCHEDULE_COUNT_MISMATCH';
  end if;
  select w.* into strict v_daily_work from private.bpay_next_work w where w.id=new.work_id;
  if v_daily_work.origin_kind='DAILY' then
    if new.source_kind is distinct from 'ORDINARY' or new.financial_snapshot_id is null
       or jsonb_typeof(new.daily_approval_evidence_json) is distinct from 'object'
       or new.daily_approval_evidence_json->>'contract_version' is distinct from 'BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
       or new.daily_approval_evidence_json->>'timesheet_id' is distinct from new.physical_timesheet_id::text
       or new.daily_approval_evidence_json->>'timesheet_version' is distinct from new.physical_timesheet_version::text
       or new.daily_approval_evidence_json->>'financial_id' is distinct from new.financial_snapshot_id::text
       or new.daily_approval_evidence_json->>'candidate_id' is distinct from v_daily_work.candidate_id::text
       or new.daily_approval_evidence_json->>'booking_id' is distinct from v_daily_work.booking_id
       or new.daily_approval_evidence_json->>'work_date' is distinct from v_daily_work.origin_work_date::text then
      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_REVISION_EVIDENCE_REQUIRED';
    end if;
  elsif new.daily_approval_evidence_json is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_CONTRACT_REVISION_DAILY_EVIDENCE_FORBIDDEN';
  end if;
  if new.detail_kind='SHIFT' then
    select exists (
      select 1 from private.bpay_next_approved_line l
      where l.revision_id=old.id
        and l.component_kind in ('WORK','PROTECTED_WORK')
        and not exists (
          select 1 from private.bpay_next_shift_detail d
          where d.approved_line_id=l.id
            and d.work_date is not null
            and (d.approved_minutes is not null or d.approved_hours is not null
                 or d.shift_start_at is not null)
        )
    ) into v_missing_detail;
    if v_missing_detail then
      raise exception using errcode='23514', message='BPAY_NEXT_REVISION_WITHOUT_SHIFT_DETAIL';
    end if;
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_work_insert_guard_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_booking_id text;
  v_original public.timesheets%rowtype;
begin
  select t.booking_id into v_booking_id
    from public.timesheets t where t.timesheet_id=new.original_timesheet_id;
  if v_booking_id is distinct from new.booking_id then
    raise exception using errcode='23514', message='BPAY_NEXT_WORK_ANCHOR_BOOKING_MISMATCH';
  end if;
  if new.origin_kind='DAILY' then
    select t.* into strict v_original from public.timesheets t where t.timesheet_id=new.original_timesheet_id;
    if v_original.contract_id is not null or v_original.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum
       or v_original.line_type::text is distinct from 'HOURS'
       or (v_original.worked_start_iso at time zone 'Europe/London')::date is distinct from new.origin_work_date
       or not exists (select 1 from public.timesheets_financials tf
         where tf.timesheet_id=v_original.timesheet_id and tf.timesheet_version=v_original.version
           and tf.candidate_id=new.candidate_id and tf.is_current is true and tf.is_stale is false
           and tf.authorised_at_utc is not null and tf.basis::text='SELF_REPORTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_ORIGINAL_IDENTITY_REQUIRED';
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_approval_route_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_approval_route_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_publish_ordinary_from_financial_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_publish_ordinary_from_financial_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_ordinary_authorisation_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_ordinary_authorisation_trigger_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_revision_guard_v1() owner to postgres;
revoke all on function private.bpay_next_revision_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_work_insert_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_insert_guard_v1() from public,anon,authenticated,service_role;
commit;
