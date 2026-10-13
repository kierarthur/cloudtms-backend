-- DR-38 / BD145: ordinary classified lines; one native hold valuation and exact downstream tuples.
\set ON_ERROR_STOP on
begin;
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
  v_frozen_settings jsonb;
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
  select dr38_point_1.* into strict v_tf from public.timesheets_financials dr38_point_1
    where id=p_financial_id;
  select dr38_point_2.* into strict v_ts from public.timesheets dr38_point_2
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
    v_base_count:=case when v_ts.sheet_scope='DAILY'::public.timesheet_scope_enum or v_base_amount<>0 or v_tf.total_hours<>0 then 1 else 0 end;
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
    -- DY05: preserve native approval/settings and labels for every genuine Daily.
    v_frozen_settings:=private._timesheet_settings_authority_frozen_v1(v_ts.timesheet_id);
    if jsonb_typeof(v_frozen_settings) is distinct from 'object'
       or v_frozen_settings->>'authority_version' is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
       or v_frozen_settings->>'workflow' is distinct from 'DAILY'
       or v_frozen_settings->>'client_id' is distinct from v_tf.client_id::text
       or v_frozen_settings->'contract_id' is distinct from coalesce(to_jsonb(v_contract_id),'null'::jsonb)
       or v_frozen_settings->>'relevant_date' is distinct from v_available_daily_evidence->>'work_date' then
      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_FROZEN_SETTINGS_IDENTITY_REQUIRED';
    end if;
    v_daily_evidence:=v_available_daily_evidence||jsonb_build_object(
      'sheet_scope','DAILY','contract_id',v_contract_id,
      'settings_authority_json',v_frozen_settings,
      'financial_authorised_at_utc',v_tf.authorised_at_utc,
      'timesheet_authorised_at_utc',v_ts.authorised_at_server);
    v_role:=v_tf.role;v_band:=v_tf.band;
    v_work_date:=(v_available_daily_evidence->>'work_date')::date;
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
  select dr38_point_3.* into v_work from private.bpay_next_work dr38_point_3
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
              case when v_resolved_daily_rates then 'WORK:DAY:'||v_work_date::text else 'WORK:AGGREGATE' end,
              'WORK',case when v_resolved_daily_rates then v_work_date end,v_tf.total_hours,
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
    if v_resolved_daily_rates then
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
          (shift_detail_id,break_no,break_start_at,break_end_at,break_start_local,break_end_local,break_minutes)
          values (v_shift_id,1,v_tf.break_start_iso,v_tf.break_end_iso,
            to_char(v_tf.break_start_iso at time zone 'Europe/London','HH24:MI'),
            to_char(v_tf.break_end_iso at time zone 'Europe/London','HH24:MI'),
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
       approved_quantity,approved_unit_rate,source_pay_ex_vat,tax_treatment,evidence_ref)
      values (v_revision_id,v_line_no,'ADDITIONAL:'||upper(btrim(v_extra.key)),
        'ADDITIONAL',nullif(v_extra.value->>'unit_name',''),
        coalesce((v_extra.value->>'unit_count')::numeric,
                 (v_extra.value->>'units_week')::numeric),
        coalesce((v_extra.value->>'pay_rate')::numeric,
                 (v_extra.value->>'rate')::numeric),v_amount,'TAXABLE',
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
         source_pay_ex_vat,tax_treatment,evidence_ref)
        values (v_revision_id,v_line_no,'EXPENSE:'||v_extra.code,'EXPENSE',
          v_extra.amount,'NON_TAXABLE','timesheets_financials:'||v_tf.id::text||'#expense:'||v_extra.code);
      v_written_total:=v_written_total+v_extra.amount;
    end if;
  end loop;
  if v_mileage_count=1 then
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,component_kind,unit_label,
       approved_quantity,approved_unit_rate,source_pay_ex_vat,tax_treatment,evidence_ref)
      values (v_revision_id,v_line_no,'MILEAGE:MILE','MILEAGE','mile',
        v_tf.mileage_units,v_mileage_pay_rate,v_tf.mileage_pay_ex_vat,'NON_TAXABLE',
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

create or replace function private.bpay_next_capture_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_row record;
  v_member record;
  v_seen integer:=0;
  v_selected_seen integer:=0;
  v_resolution_count integer:=0;
  v_managed_seen integer:=0;
  v_needs_resolution boolean;
  v_collection private.bpay_next_work_collection%rowtype;
  v_work_cursor uuid;
  v_component_cursor text;
  v_issue text;v_answered_source boolean;
  v_issue_work_id uuid;
  v_query_pin private.bpay_next_run_work_query_pin_v2%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_book record;
  v_segments bigint;v_snooze jsonb;v_segment_result jsonb;v_segment_visited integer:=0;
  v_seed_effect uuid;v_seed_hold uuid;v_seed_holds uuid[];v_seed record;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CAPTURE_POSITION_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select dr38_point_4.* into strict v_run from private.bpay_next_pay_run dr38_point_4
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select dr38_point_5.* into strict v_job from private.bpay_next_job dr38_point_5
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('RESERVE','ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_LEASE_STALE';
  end if;
  if v_job.phase='ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'replay',true,'rows_visited',0);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
      'replay',true,'rows_visited',0,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select dr38_point_6.* into strict v_worker from private.bpay_next_run_worker dr38_point_6
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_WORKER_NOT_PREPARING';
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  for v_member in
    select s.* from private.bpay_next_selected_position_keys_v1(v_worker.id,v_work_cursor,v_component_cursor,p_limit) s
  loop
    -- The Candidate control is already exclusively held. Lock this exact
    -- selected WORK before its position, matching the reentrant seed order.
    perform 1 from private.bpay_next_work w where w.id=v_member.work_id
      and w.candidate_id=v_worker.candidate_id for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_POSITION_WORK_SCOPE_INVALID';end if;
    -- Read and lock the exact position after choosing the bounded member.
    -- Do not FOR SHARE the nullable side of an outer join (PostgreSQL refuses
    -- that), and never turn a missing selected position into an omitted line.
    select v_member.run_work_id as run_work_id,v_member.work_id as work_id,
           v_member.captured_revision_id as captured_revision_id,
           p.component_key,p.applied_revision_id,
           p.approved_source_ex_vat,p.realised_source_ex_vat,
           p.held_source_ex_vat,p.realised_target_ex_vat,
           p.realised_target_vat,p.realised_target_inc_vat,
           p.held_target_ex_vat,p.held_target_vat,
           p.held_target_inc_vat,p.source_basis_channel,
           rev.source_pay_channel,rev.source_kind,rev.source_event_id,
           rev.source_head_id,rev.financial_snapshot_id,
           rev.physical_timesheet_id,rev.physical_timesheet_version,
           w.work_kind,w.candidate_id as origin_candidate_id,
           l.component_kind,l.id as approved_line_id,
           l.source_pay_ex_vat as approved_line_source_ex_vat,l.tax_treatment,
           p.source_book_state,p.source_book_revision
      into v_row
      from private.bpay_next_position p
      join private.bpay_next_work_revision rev
        on rev.id=v_member.captured_revision_id and rev.work_id=v_member.work_id
      join private.bpay_next_work w on w.id=rev.work_id
      left join private.bpay_next_approved_line l
        on l.revision_id=v_member.captured_revision_id
       and l.component_key=v_member.component_key
      where p.work_id=v_member.work_id and p.component_key=v_member.component_key
      for update of p;
    v_issue:=null;
    if not found
       or v_row.applied_revision_id is distinct from v_member.captured_revision_id then
      v_issue:='CAPTURE_POSITION_CHANGED';
    elsif v_row.approved_line_id is null
       and v_row.approved_source_ex_vat<>0 then
      v_issue:='POSITION_COMPONENT_EVIDENCE_MISSING';
    elsif v_row.approved_line_id is not null
       and v_row.approved_line_source_ex_vat is distinct from
         v_row.approved_source_ex_vat then
      v_issue:='POSITION_APPROVAL_AMOUNT_MISMATCH';
    end if;
    -- The Direct no-HEAD PROTECTED_WORK route retains its original restrictions.
    v_answered_source:=coalesce(v_row.work_kind='SOURCE'
      and (v_row.source_head_id is not null
        or (v_row.source_kind='SOURCE' and v_row.source_event_id is not null
          and v_row.financial_snapshot_id is not null))
      and v_row.source_pay_channel in ('PAYE','UMBRELLA')
      and ((v_row.component_kind in ('WORK','ADDITIONAL') and v_row.tax_treatment='TAXABLE')
        or (v_row.component_kind in ('EXPENSE','MILEAGE') and v_row.tax_treatment='NON_TAXABLE')),false);
    if v_worker.target_pay_channel='UMBRELLA' and v_issue is null and not v_answered_source
      and not coalesce(v_row.source_pay_channel in ('PAYE','UMBRELLA') and (
        (v_row.work_kind='ORDINARY' and (
          (v_row.component_kind in ('WORK','ADDITIONAL') and v_row.tax_treatment='TAXABLE')
          or (v_row.component_kind in ('EXPENSE','MILEAGE') and v_row.tax_treatment='NON_TAXABLE')))
        or (v_row.work_kind='EXPENSE' and v_row.source_kind='EXPENSE' and v_row.financial_snapshot_id is not null
          and v_row.component_kind in ('EXPENSE','MILEAGE') and v_row.tax_treatment='NON_TAXABLE')),false) and
       (v_row.source_pay_channel is distinct from 'UMBRELLA'
        or v_row.work_kind not in ('ORDINARY','SOURCE')
        or v_row.component_kind is distinct from 'WORK'
        or v_row.tax_treatment is distinct from 'TAXABLE') then
      v_issue:='UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    if v_issue is not null then
      v_issue_work_id:=v_member.work_id;exit;
    end if;
    -- Typed initial I-1 is the ONLY new no-HEAD binding arm. Initial and
    -- common-HEAD captures require self-canonical, exactly classified Source components.
    v_binding:=null;v_book:=null;
    if v_row.work_kind='SOURCE' and (v_row.source_head_id is not null
      or (v_row.source_kind='SOURCE' and v_row.source_event_id is not null
        and v_row.financial_snapshot_id is not null)) then
      select v.* into v_binding from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
        where b.canonical_work_id=v_row.work_id and b.canonical_component_key=v_row.component_key
          and v.approval_work_id=v_row.work_id and v.approval_revision_id=v_row.captured_revision_id
          and v.approval_component_key=v_row.component_key
          and ((v_row.source_head_id is not null and v.origin_kind='COMMON_HEAD_RECEIPT_V1'
              and v.head_id=v_row.source_head_id)
            or (v_row.source_head_id is null and v.origin_kind='INITIAL_AUTHORISED_TSFIN_V1'
              and v.initial_root_authorisation_id=v_row.source_event_id
              and v.initial_financial_snapshot_id=v_row.financial_snapshot_id
              and v.initial_root_timesheet_id=v_row.physical_timesheet_id
              and v.initial_root_version=v_row.physical_timesheet_version));
      if v_binding.id is null or v_binding.publication_command_id_2 is not null
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or not v_answered_source then
        v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
      else
        if v_row.source_book_state='UNBOUND' then
          -- Capture is already a bounded selected point with real APPLIED/DONE
          -- authority. Adoption never borrows a queued writer's lease here.
          v_seed_effect:=null;v_seed_hold:=null;v_seed_holds:=null;
          if v_row.realised_source_ex_vat>0 then
            select b.original_effect_id into v_seed_effect from private.bpay_next_work_collection b
              where b.work_id=v_row.work_id and b.component_key=v_row.component_key
                and b.candidate_id=v_worker.candidate_id and b.paid_basis_qualified;
          end if;
          if v_row.held_source_ex_vat>0 then
            select pg_catalog.array_agg(h.id order by h.id) into v_seed_holds from (
              select id from private.bpay_next_hold where work_id=v_row.work_id
                and component_key=v_row.component_key and status='ACTIVE' order by id limit 2) h;
            if pg_catalog.cardinality(v_seed_holds) is distinct from 1 then
              raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ACTIVE_HOLD_WITNESS_AMBIGUOUS';
            end if;
            v_seed_hold:=v_seed_holds[1];
          end if;
          select seed.* into strict v_seed from private.bpay_next_source_disposition_seed_v1(
            v_row.work_id,v_row.component_key,v_row.captured_revision_id,v_seed_effect,v_seed_hold) seed;
          if v_seed.qualification_state is distinct from 'READY' then
            raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SELECTED_SEED_NOT_READY';
          end if;
          select p.source_book_state,p.source_book_revision into v_row.source_book_state,v_row.source_book_revision
            from private.bpay_next_position p where p.work_id=v_row.work_id and p.component_key=v_row.component_key;
        end if;
        select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
          v_binding.id,v_row.source_book_revision) s;
        if v_book.qualification_state is distinct from 'READY'
           or (v_book.candidate_id,v_book.canonical_work_id,v_book.canonical_component_key,
               v_book.approval_work_id,v_book.approval_revision_id,v_book.approved_line_id,
               v_book.source_channel,v_book.tax_treatment,v_book.approved_source_ex_vat,
               v_book.native_realised_paye,v_book.native_held_paye,v_book.native_realised_umbrella,v_book.native_held_umbrella)
             is distinct from (v_worker.candidate_id,v_row.work_id,v_row.component_key,
               v_row.work_id,v_row.captured_revision_id,v_row.approved_line_id,
               v_row.source_pay_channel,v_row.tax_treatment,v_row.approved_source_ex_vat,
               case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end) then
          v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
        end if;
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
      end if;
    end if;
    v_collection:=null;
    -- Only the exact typed original ordinary PAYE liability can own a
    -- negative residual. Unknown signed/cross-basis/held shapes still REVIEW.
    if v_worker.target_pay_channel='PAYE' and v_row.source_pay_channel='PAYE'
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat<0
       and (v_row.held_source_ex_vat,v_row.held_target_ex_vat,v_row.held_target_vat,v_row.held_target_inc_vat)
         is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      select b.* into v_collection from private.bpay_next_work_collection b
        where b.work_id=v_row.work_id and b.component_key=v_row.component_key;
      if v_collection.case_id is not null then
        v_collection:=private.bpay_next_work_collection_origin_v1(
          v_collection.case_id,v_collection.case_component_id,v_worker.candidate_id);
      else v_collection:=null;end if;
      if v_collection.id is not null and
         (v_collection.observed_revision_id,v_collection.observed_approved_source_ex_vat,v_collection.observed_realised_source_ex_vat)
           is distinct from (v_row.captured_revision_id,v_row.approved_source_ex_vat,v_row.realised_source_ex_vat) then
        v_collection:=null;
      end if;
    end if;
    v_needs_resolution:=v_collection.id is null and (
       v_row.approved_line_id is null
       or v_row.approved_source_ex_vat<0
       or ((v_row.realised_source_ex_vat<>0
          or v_row.held_source_ex_vat<>0
          or v_row.realised_target_inc_vat<>0
          or v_row.held_target_inc_vat<>0)
         and v_row.source_basis_channel is distinct from v_row.source_pay_channel)
       or v_row.realised_source_ex_vat<0
       or v_row.realised_target_ex_vat<0 or v_row.realised_target_vat<0 or v_row.realised_target_inc_vat<0
       or (v_row.held_source_ex_vat>0 and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<=0)
       or (v_row.held_source_ex_vat=0 and v_row.held_target_inc_vat<>0)
       or v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<0);
    -- Only genuine positive Source WORK consumes the retained disposition.
    -- Expense/additional/ordinary, zero, realised/held-away, managed negative
    -- and already-unresolved financial shapes do not become query holds.
    -- The actual pin validated the typed origin before this immutable row;
    -- missing evidence is REVIEW, never a lazy live Source reader fallback.
    if v_row.work_kind='SOURCE' and v_row.source_kind in ('SOURCE','PROTECTED')
       and v_row.source_event_id is not null
       and v_row.component_kind in ('WORK','PROTECTED_WORK')
       and not v_needs_resolution
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat>0 then
      select qp.* into v_query_pin from private.bpay_next_run_work_query_pin_v2 qp
        where qp.run_work_id=v_row.run_work_id;
      if found and (private.bpay_next_prepare_job_run_v1(v_query_pin.prepare_job_id) is distinct from v_run_id
        or (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text)) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        -- Only the original cut's complete, exact named shift exclusions
        -- can bypass a whole-Work blocker. Never read today's query here.
        if not exists(select 1 from private.bpay_next_run_work_query_snapshot snapshot
          where snapshot.run_id=v_run_id and snapshot.work_id=v_row.work_id
            and snapshot.expected_revision_id=v_row.captured_revision_id
            and snapshot.physical_timesheet_id::text=(v_query_pin.scope->>'root_timesheet_id')
            and jsonb_typeof(snapshot.segment_query_exclusions)='array'
            and jsonb_array_length(snapshot.segment_query_exclusions)>0)
        then v_issue:='SOURCE_PAY_QUERY_OPEN';end if;
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
      end if;
    end if;
    select count(*) into v_segments from (
      select s.segment_key from private.bpay_next_approved_segment s
        where s.revision_id=v_row.captured_revision_id and s.component_key=v_row.component_key
      union select s.segment_key from private.bpay_next_work_segment_origin_share s
        where s.work_id=v_row.work_id and s.component_key=v_row.component_key) members;
    if v_segments>0 and v_row.work_kind='ORDINARY' then
      -- Exact segment negative ownership is sealed after complete child capture.
      -- Initial native net-negative classification cannot bypass that census.
      if v_collection.id is not null then v_needs_resolution:=true;end if;
      v_collection:=null;
    end if;
    v_snooze:=null;
    if v_segments=0 then
      v_snooze:=private.bpay_next_snooze_at_cut_v1(v_row.work_id,v_row.captured_revision_id,
        null,v_row.component_key,v_run.snooze_control_sequence,v_run.snooze_business_date);
    end if;
    insert into private.bpay_next_run_position
      (run_worker_id,run_work_id,work_id,captured_revision_id,
       component_key,approved_line_id,source_pay_channel,
       source_basis_channel,
       approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,
       residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,
       realised_target_inc_vat,held_target_ex_vat,held_target_vat,
       held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,
       source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision,
       segment_expected_member_count,segment_attribution_complete,snooze_id,snooze_version,snooze_excluded_positive_ex_vat)
      values(v_worker.id,v_row.run_work_id,v_row.work_id,
       v_row.captured_revision_id,v_row.component_key,v_row.approved_line_id,
       v_row.source_pay_channel,v_row.source_basis_channel,
       v_row.approved_source_ex_vat,
       v_row.realised_source_ex_vat,v_row.held_source_ex_vat,
       v_row.approved_source_ex_vat-v_row.realised_source_ex_vat
         -v_row.held_source_ex_vat,
       v_row.realised_target_ex_vat,v_row.realised_target_vat,
       v_row.realised_target_inc_vat,v_row.held_target_ex_vat,
       v_row.held_target_vat,v_row.held_target_inc_vat,
       v_needs_resolution,v_collection.id,v_collection.reconcile_revision,
       v_binding.id,v_binding.canonical_work_id,v_binding.canonical_component_key,
       case when v_binding.id is null then null else v_row.source_book_revision end,
       v_segments,v_segments=0,(v_snooze->>'id')::uuid,(v_snooze->>'version')::bigint,
       case when v_snooze is not null then greatest(v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat,0) else 0 end);
    if v_needs_resolution then
      v_resolution_count:=v_resolution_count+1;
    end if;
    if v_collection.id is not null then v_managed_seen:=v_managed_seen+1;end if;
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
    if v_member.selection_mode='SUBSET' then
      v_selected_seen:=v_selected_seen+1;
    end if;
  end loop;
  if v_resolution_count>0 then
    update private.bpay_next_run_worker
      set financial_resolution_count=financial_resolution_count+v_resolution_count
      where id=v_worker.id;
  end if;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_position_count=captured_position_count+v_seen,
          expected_managed_position_count=expected_managed_position_count+v_managed_seen,
          captured_selected_component_count=
            captured_selected_component_count+v_selected_seen
      where id=v_worker.id;
  end if;
  update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
  if v_issue is not null then
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_issue_work_id where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
  end if;
  if v_seen<p_limit then
    if v_worker.captured_selected_component_count+v_selected_seen
       <>v_worker.expected_selected_component_count then
      select rw.work_id into strict v_issue_work_id
        from private.bpay_next_run_work rw
        join private.bpay_next_work_choice choice
          on choice.run_id=v_run_id and choice.work_id=rw.work_id
         and choice.selection_mode='SUBSET'
        where rw.run_worker_id=v_worker.id
        order by rw.work_id limit 1;
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='CAPTURE_POSITION_CHANGED',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
    end if;
    v_segment_result:=private.bpay_next_capture_segment_page_v1(v_worker.id,p_limit-v_seen);
    v_segment_visited:=(v_segment_result->>'rows_visited')::integer;
    select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker.id;
    if v_worker.status='REVIEW' then
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen+v_segment_visited);
    end if;
    if not (v_segment_result->>'done')::boolean then
      update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
      return jsonb_build_object('done',false,'phase','RESERVE','rows_visited',v_seen+v_segment_visited,
        'work_cursor',v_work_cursor,'component_cursor',v_component_cursor,'replay',false);
    end if;
    update private.bpay_next_job
      set phase='ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'rows_visited',v_seen+v_segment_visited,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
    'rows_visited',v_seen+v_segment_visited,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

create or replace function private.bpay_next_hold_fresh_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_issue_work_id uuid;
  v_row record;
  v_position private.bpay_next_position%rowtype;
  v_value record;v_book record;
  v_reimbursement_window uuid;v_reimbursement_vat_pct numeric;v_reimbursement_window_count bigint;v_reimbursement_amounts jsonb;
  v_run_line_id uuid;v_hold_id uuid;v_included_source numeric(18,2);v_freeze_zero boolean;
  v_seen integer:=0;
  v_affected_work_ids uuid[]:=array[]::uuid[];
  v_work_cursor uuid;
  v_component_cursor text;
  v_line_count bigint;
  v_managed_count bigint;
  v_total_ex numeric(18,2);
  v_total_vat numeric(18,2);
  v_total_inc numeric(18,2);
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_HOLD_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select dr38_point_7.* into strict v_run from private.bpay_next_pay_run dr38_point_7
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select dr38_point_8.* into strict v_job from private.bpay_next_job dr38_point_8
    where id=p_job_id for update;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('ALLOCATE','FINANCE_ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_HOLD_LEASE_STALE';
  end if;
  if v_job.phase='FINANCE_ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',0,'replay',true);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
      'rows_visited',0,'replay',true,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select dr38_point_9.* into strict v_worker from private.bpay_next_run_worker dr38_point_9
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_HOLD_WORKER_NOT_PREPARING';
  end if;
  if v_worker.financial_resolution_count>0 then
    select rp.work_id into strict v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id and rp.needs_financial_resolution
      order by rp.work_id,rp.component_key limit 1;
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code='FINANCIAL_RESOLUTION_REQUIRED',
          review_issue_work_id=v_issue_work_id
      where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
  end if;
  if v_worker.target_pay_channel='UMBRELLA'
     and (v_worker.target_umbrella_id is null
       or v_worker.target_umbrella_vat_chargeable is null) then
    select rp.work_id into v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id
      order by rp.work_id,rp.component_key limit 1;
    if v_issue_work_id is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='TARGET_UMBRELLA_MISSING',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
    end if;
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  v_line_count:=v_worker.captured_line_count;
  v_managed_count:=v_worker.captured_managed_position_count;
  v_total_ex:=v_worker.gross_ex_vat;
  v_total_vat:=v_worker.gross_vat;
  v_total_inc:=v_worker.gross_inc_vat;
  for v_row in
    select rp.*,rw.id as pinned_work_id,rev.valuation_policy_id,rev.source_kind as captured_source_kind,
      rev.financial_snapshot_id as captured_financial_snapshot_id,
      w.work_kind as captured_work_kind,l.component_kind as captured_component_kind,l.tax_treatment as captured_tax_treatment
      from private.bpay_next_run_position rp
      join private.bpay_next_run_work rw on rw.id=rp.run_work_id
      join private.bpay_next_work_revision rev
        on rev.id=rp.captured_revision_id and rev.work_id=rp.work_id
      join private.bpay_next_work w on w.id=rp.work_id
      left join private.bpay_next_approved_line l on l.id=rp.approved_line_id
        and l.revision_id=rp.captured_revision_id and l.component_key=rp.component_key
      where rp.run_worker_id=v_worker.id
        and (v_work_cursor is null or rp.work_id>v_work_cursor
          or (rp.work_id=v_work_cursor
            and rp.component_key>v_component_cursor))
      order by rp.work_id,rp.component_key limit p_limit
  loop
    if not v_row.segment_attribution_complete then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';
    end if;
    v_included_source:=case when v_row.segment_member_count>0 then v_row.segment_included_positive_ex_vat
      else greatest(v_row.residual_source_ex_vat,0)-v_row.snooze_excluded_positive_ex_vat end;
    v_freeze_zero:=v_row.approved_line_id is not null and v_row.approved_source_ex_vat=0 and v_row.realised_source_ex_vat=0 and v_row.held_source_ex_vat=0
      and v_row.snooze_id is null and (v_row.segment_member_count=0 or exists(
        select 1 from private.bpay_next_run_work_segment_parent p join private.bpay_next_run_work_segment s on s.id=p.run_work_segment_id
          where p.run_work_id=v_row.run_work_id and p.component_key=v_row.component_key and s.disposition='INCLUDED'));
    if v_row.needs_financial_resolution
       or (v_row.work_collection_id is null and
         ((v_row.approved_line_id is null and (v_row.segment_member_count=0 or v_included_source>0)) or v_row.residual_source_ex_vat<0)) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_HOLD_UNEXPECTED_POSITION_SHAPE';
    end if;
    select dr38_point_10.* into strict v_position from private.bpay_next_position dr38_point_10
      where work_id=v_row.work_id and component_key=v_row.component_key
      for update;
    if v_position.applied_revision_id is distinct from v_row.captured_revision_id
       or v_position.approved_source_ex_vat is distinct from
         v_row.approved_source_ex_vat
       or v_position.realised_source_ex_vat is distinct from
         v_row.realised_source_ex_vat
       or v_position.held_source_ex_vat is distinct from
         v_row.held_source_ex_vat
       or v_position.source_basis_channel is distinct from
         v_row.source_basis_channel
       or v_position.realised_target_ex_vat is distinct from
         v_row.realised_target_ex_vat
       or v_position.realised_target_vat is distinct from
         v_row.realised_target_vat
       or v_position.realised_target_inc_vat is distinct from
         v_row.realised_target_inc_vat
       or v_position.held_target_ex_vat is distinct from
         v_row.held_target_ex_vat
       or v_position.held_target_vat is distinct from
         v_row.held_target_vat
       or v_position.held_target_inc_vat is distinct from
         v_row.held_target_inc_vat then
      raise exception using errcode='55000',
        message='BPAY_NEXT_HOLD_CURRENT_POSITION_CHANGED';
    end if;
    if v_row.source_binding_version_id is not null then
      select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
        v_row.source_binding_version_id,v_row.source_book_revision) s;
      if v_book.qualification_state is distinct from 'READY'
         or (v_book.canonical_work_id,v_book.canonical_component_key,v_book.approval_work_id,
             v_book.approval_revision_id,v_book.approved_line_id,v_book.source_channel,v_book.tax_treatment,
             v_book.approved_source_ex_vat,v_book.native_realised_paye,v_book.native_held_paye,
             v_book.native_realised_umbrella,v_book.native_held_umbrella)
           is distinct from (v_row.work_id,v_row.component_key,v_row.work_id,
             v_row.captured_revision_id,v_row.approved_line_id,v_row.source_pay_channel,v_row.captured_tax_treatment,
             v_row.approved_source_ex_vat,
             case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end)
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or coalesce(v_row.captured_work_kind='SOURCE'
           and ((v_row.captured_component_kind in ('WORK','ADDITIONAL') and v_row.captured_tax_treatment='TAXABLE')
             or (v_row.captured_component_kind in ('EXPENSE','MILEAGE') and v_row.captured_tax_treatment='NON_TAXABLE')),false) is not true
         or v_row.canonical_source_work_id is distinct from v_row.work_id
         or v_row.canonical_source_component_key is distinct from v_row.component_key then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CAPTURE_CHANGED';
      end if;
    end if;
    if v_row.work_collection_id is not null then
      -- Classification and original provenance were frozen by RESERVE. Never
      -- reread a newer WORK approval or create an ordinary negative/zero hold.
      update private.bpay_next_run_position set managed_capture_completed=true
        where id=v_row.id and not managed_capture_completed;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ALREADY_SPENT';end if;
      v_managed_count:=v_managed_count+1;
    end if;
    if v_included_source>0 or v_freeze_zero then
    if ((v_row.source_binding_version_id is not null and v_row.captured_work_kind='SOURCE')
        or v_row.captured_work_kind='ORDINARY'
        or (v_row.captured_work_kind='EXPENSE' and v_row.captured_source_kind='EXPENSE'
          and v_row.captured_financial_snapshot_id is not null))
      and v_row.captured_component_kind in ('EXPENSE','MILEAGE') and v_row.captured_tax_treatment='NON_TAXABLE' then
      -- A classified ordinary/Source reimbursement keeps source ex; only ordinary line VAT applies.
      -- Use the same immutable winning policy window and existing VAT calculator.
      if v_included_source is null or v_row.valuation_policy_id is null or v_run.pay_date is null
        or v_row.source_pay_channel is null or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
        or v_worker.target_pay_channel is null or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
        or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_vat_chargeable is null)
        or v_included_source<>pg_catalog.round(v_included_source,2) then
        raise exception using errcode='22023',message='BPAY_NEXT_TARGET_VALUATION_INPUT_INVALID';end if;
      v_reimbursement_window:=null;v_reimbursement_vat_pct:=null;v_reimbursement_window_count:=null;
      select w.source_window_id,w.vat_rate_pct,count(*) over(partition by w.date_from)
        into v_reimbursement_window,v_reimbursement_vat_pct,v_reimbursement_window_count
        from private.bpay_next_valuation_policy_window w
        where w.policy_id=v_row.valuation_policy_id and v_run.pay_date>=w.date_from
          and v_run.pay_date<=coalesce(w.date_to,'infinity'::date)
        order by w.date_from desc,w.source_window_id desc limit 1;
      if v_reimbursement_window is null then
        raise exception using errcode='23514',message='BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING';end if;
      if v_reimbursement_window_count<>1 then
        raise exception using errcode='23514',message='BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS';end if;
      if v_worker.target_pay_channel='UMBRELLA' then
        v_reimbursement_amounts:=public._pay_umbrella_vat_calc(v_included_source,
          v_reimbursement_vat_pct,v_worker.target_umbrella_vat_chargeable);
        select v_reimbursement_window as policy_window_id,v_included_source as target_ex_vat,
          (v_reimbursement_amounts->>'vat')::numeric as target_vat,
          (v_reimbursement_amounts->>'inc')::numeric as target_inc_vat into strict v_value;
      else
        select v_reimbursement_window as policy_window_id,v_included_source as target_ex_vat,
          0::numeric as target_vat,v_included_source as target_inc_vat into strict v_value;
      end if;
    else
    select * into strict v_value
      from private.bpay_next_value_source_for_target_v1(
        v_included_source,v_row.source_pay_channel,
        v_worker.target_pay_channel,
        case when v_worker.target_pay_channel='UMBRELLA'
          then v_worker.target_umbrella_vat_chargeable else false end,
        v_row.valuation_policy_id,v_run.pay_date);
    end if;
    v_line_count:=v_line_count+1;
    insert into private.bpay_next_run_line
      (run_worker_id,run_work_id,line_no,approved_line_id,
       captured_revision_id,work_id,component_key,source_consumed_ex_vat,
       source_pay_channel,target_pay_channel,valuation_policy_id,
       policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat,source_binding_version_id,
       canonical_source_work_id,canonical_source_component_key,source_book_revision,updates_original_source_scalar)
      values(v_worker.id,v_row.run_work_id,v_line_count,
       v_row.approved_line_id,v_row.captured_revision_id,v_row.work_id,
       v_row.component_key,v_included_source,
       v_row.source_pay_channel,v_worker.target_pay_channel,
       v_row.valuation_policy_id,v_value.policy_window_id,
       v_value.target_ex_vat,v_value.target_vat,v_value.target_inc_vat,
       v_row.source_binding_version_id,v_row.canonical_source_work_id,v_row.canonical_source_component_key,
       v_row.source_book_revision,case when v_row.source_binding_version_id is null then null else true end)
      returning id into v_run_line_id;
    -- An authorised zero still has immutable shift/rate advice detail. It
    -- becomes a frozen zero line, never a zero-valued financial hold.
    if v_included_source>0 then
      insert into private.bpay_next_hold
        (run_line_id,work_id,component_key,source_reserved_ex_vat,
         target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status)
        values(v_run_line_id,v_row.work_id,v_row.component_key,
         v_included_source,v_value.target_ex_vat,
         v_value.target_vat,v_value.target_inc_vat,'ACTIVE') returning id into v_hold_id;
      perform private.bpay_next_record_work_segment_origin_shares_v1(v_run_line_id,v_hold_id);
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat+v_included_source,
            held_target_ex_vat=held_target_ex_vat+v_value.target_ex_vat,
            held_target_vat=held_target_vat+v_value.target_vat,
            held_target_inc_vat=held_target_inc_vat+v_value.target_inc_vat,
            updated_at_utc=pg_catalog.transaction_timestamp()
        where work_id=v_row.work_id and component_key=v_row.component_key;
    end if;
    v_total_ex:=v_total_ex+v_value.target_ex_vat;
    v_total_vat:=v_total_vat+v_value.target_vat;
    v_total_inc:=v_total_inc+v_value.target_inc_vat;
    end if;
    v_affected_work_ids:=array_append(v_affected_work_ids,v_row.work_id);
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
  end loop;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_line_count=v_line_count,
          captured_managed_position_count=v_managed_count,
          gross_ex_vat=v_total_ex,gross_vat=v_total_vat,
          gross_inc_vat=v_total_inc
      where id=v_worker.id;
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_job.candidate_id;
    perform private.bpay_next_workbench_refresh_many_v1(v_affected_work_ids,null);
  end if;
  if v_seen<p_limit then
    if v_managed_count<>v_worker.expected_managed_position_count or exists(
      select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
        and work_collection_id is not null and not managed_capture_completed) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_COUNT_MISMATCH';
    end if;
    update private.bpay_next_job
      set phase='FINANCE_ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',v_seen,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
    'rows_visited',v_seen,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

create or replace function private.bpay_next_build_simple_transfer_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_candidate_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_line record;
  v_seen integer:=0;
  v_member_count bigint;
  v_cash_sum numeric(18,2);
  v_cursor bigint;
  v_adjustment numeric(18,2);
  v_epoch bigint;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit is null or p_limit not between 1 and 100
     or p_expected_cursor is not null and p_expected_cursor<1 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_TRANSFER_PAGE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate_id
    from private.bpay_next_job j
    join private.bpay_next_transfer t on t.build_command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where j.id=p_job_id and j.candidate_id=w.candidate_id;
  if exists(select 1 from private.bpay_next_destination_group b
     join private.bpay_next_transfer t on t.id=b.anchor_transfer_id
     join private.bpay_next_job j on j.command_id=t.build_command_id where j.id=p_job_id) then
    return private.bpay_next_build_destination_group_page_v1(
      p_job_id,p_lease_nonce,p_owner_epoch,p_expected_cursor,p_limit);
  end if;
  if exists(select 1 from private.bpay_next_case_transfer_build b
     join private.bpay_next_transfer t on t.id=b.transfer_id
     join private.bpay_next_job j on j.command_id=t.build_command_id
     where j.id=p_job_id) then
    return private.bpay_next_build_case_transfer_page_v1(
      p_job_id,p_lease_nonce,p_owner_epoch,p_expected_cursor,p_limit);
  end if;
  select dr37_point_135.* into strict v_run from private.bpay_next_pay_run dr37_point_135
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select dr37_point_136.* into strict v_job from private.bpay_next_job dr37_point_136
    where id=p_job_id for update;
  select dr37_point_137.* into strict v_transfer from private.bpay_next_transfer dr37_point_137
    where build_command_id=v_job.command_id for update;
  select dr37_point_138.* into strict v_worker from private.bpay_next_run_worker dr37_point_138
    where id=v_transfer.run_worker_id for update;
  select dr37_point_139.* into strict v_projection from private.bpay_next_net_projection dr37_point_139
    where id=v_transfer.projection_id;
  if v_job.job_kind<>'TRANSFER_BUILD'
     or v_job.module_epoch<>v_epoch
     or v_job.candidate_id<>v_worker.candidate_id
     or v_transfer.candidate_id<>v_worker.candidate_id
     or not ((v_worker.target_pay_channel='PAYE' and
          v_transfer.beneficiary_kind='CANDIDATE' and v_transfer.beneficiary_id=v_worker.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_id is not null and
          v_transfer.beneficiary_kind='UMBRELLA' and v_transfer.beneficiary_id=v_worker.target_umbrella_id))
     or v_run.id<>v_worker.run_id
     or v_projection.run_worker_id<>v_worker.id then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_JOB_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_job.phase='MEMBERS_READY'
     and v_transfer.status<>'BUILDING' then
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY',
      'transfer_status',v_transfer.status,
      'transfer_id',v_transfer.id,'cursor',v_job.cursor_key,
      'member_count',v_transfer.member_count,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'WORK'
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or v_worker.status<>'READY'
     or v_worker.net_projection_revision<1 or v_worker.net_replacement_pending_id is not null
     or (v_worker.target_pay_channel='PAYE' and v_worker.net_request_revision<>v_worker.net_projection_revision)
     or v_projection.projection_no<>v_worker.net_projection_revision or v_projection.retired_at_utc is not null
     or v_projection.cash_amount<>v_transfer.cash_amount
     or v_transfer.status<>'BUILDING'
     or v_transfer.account_approval_ref is not null
     or v_transfer.original_transfer_id is not null
     or v_transfer.member_count>v_worker.captured_line_count then
    raise exception using errcode='55000',
      message='BPAY_NEXT_TRANSFER_BUILD_NOT_ELIGIBLE';
  end if;
  if v_job.cursor_key is distinct from p_expected_cursor::text then
    return pg_catalog.jsonb_build_object('phase','BUILDING',
      'transfer_id',v_transfer.id,'cursor',v_job.cursor_key,
      'member_count',v_transfer.member_count,'replay',true);
  end if;
  v_member_count:=v_transfer.member_count;
  v_cash_sum:=v_transfer.member_cash_sum;
  v_cursor:=p_expected_cursor;
  for v_line in
    select l.id,l.line_no,l.frozen_inc_vat,l.frozen_ex_vat,l.frozen_vat,l.source_consumed_ex_vat,
      l.source_pay_channel,l.target_pay_channel,l.component_key,l.work_id,
      a.id as qualified_line_id,a.component_kind,a.tax_treatment,
      r.id as qualified_revision_id,
      h.id as qualified_hold_id,h.work_id as hold_work_id,h.component_key as hold_component_key,
      h.source_reserved_ex_vat as hold_source_ex_vat,h.target_amount_ex_vat as hold_target_ex_vat,
      h.target_amount_vat as hold_target_vat,h.target_amount_inc_vat as hold_target_inc_vat
    from private.bpay_next_run_line l
    left join private.bpay_next_approved_line a on a.id=l.approved_line_id
      and a.revision_id=l.captured_revision_id and a.component_key=l.component_key
    left join private.bpay_next_work_revision r on r.id=l.captured_revision_id and r.work_id=l.work_id
    left join private.bpay_next_hold h on h.run_line_id=l.id and h.status='ACTIVE'
    where l.run_worker_id=v_worker.id
      and (v_cursor is null or l.line_no>v_cursor)
    order by l.line_no,l.id limit p_limit
  loop
    if v_worker.target_pay_channel='UMBRELLA' and
       (v_projection.input_kind is distinct from 'UMBRELLA'
        or v_worker.case_selection_revision<>0 or v_worker.net_request_revision<>0
        or v_projection.entered_paye_net is not null or v_projection.request_command_id is not null
        or v_projection.cash_amount is distinct from v_worker.gross_inc_vat
        or v_line.qualified_line_id is null or v_line.qualified_revision_id is null
        or ((v_line.component_kind in ('WORK','ADDITIONAL') and v_line.tax_treatment='TAXABLE')
          or (v_line.component_kind in ('EXPENSE','MILEAGE') and v_line.tax_treatment='NON_TAXABLE'
            and v_line.frozen_ex_vat=v_line.source_consumed_ex_vat)) is not true
        or v_line.source_pay_channel not in ('PAYE','UMBRELLA')
        or v_line.target_pay_channel is distinct from 'UMBRELLA'
        or v_line.source_consumed_ex_vat<0
        or (v_line.source_consumed_ex_vat>0 and (v_line.qualified_hold_id is null
          or (v_line.hold_work_id,v_line.hold_component_key,v_line.hold_source_ex_vat,
              v_line.hold_target_ex_vat,v_line.hold_target_vat,v_line.hold_target_inc_vat)
             is distinct from (v_line.work_id,v_line.component_key,v_line.source_consumed_ex_vat,
              v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)))
        or (v_line.source_consumed_ex_vat=0 and (v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
          is distinct from (0::numeric,0::numeric,0::numeric))) then
      raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    v_seen:=v_seen+1;
    v_member_count:=v_member_count+1;
    v_cash_sum:=v_cash_sum+v_line.frozen_inc_vat;
    insert into private.bpay_next_transfer_member
      (transfer_id,run_worker_id,member_no,subject_kind,
       run_line_id,signed_cash_contribution)
      values(v_transfer.id,v_worker.id,v_member_count,'WORK',
             v_line.id,v_line.frozen_inc_vat);
    v_cursor:=v_line.line_no;
  end loop;
  if v_member_count>v_worker.captured_line_count then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_LINE_COUNT_EXCEEDED';
  end if;
  if v_member_count=v_worker.captured_line_count then
    if exists(select 1 from private.bpay_next_run_line l
              where l.run_worker_id=v_worker.id
                and (v_cursor is null or l.line_no>v_cursor))
       or v_cash_sum<>v_worker.gross_inc_vat then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_FROZEN_GROSS_MISMATCH';
    end if;
    if v_worker.target_pay_channel='PAYE' then
      v_adjustment:=v_projection.cash_amount-v_worker.gross_inc_vat;
      insert into private.bpay_next_transfer_member
        (transfer_id,run_worker_id,member_no,subject_kind,
         signed_cash_contribution)
        values(v_transfer.id,v_worker.id,v_member_count+1,
               'NET_ADJUSTMENT',v_adjustment);
      v_member_count:=v_member_count+1;
      v_cash_sum:=v_cash_sum+v_adjustment;
    end if;
    if v_cash_sum<>v_projection.cash_amount then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_CASH_MISMATCH';
    end if;
    if v_worker.target_pay_channel='UMBRELLA' then
      if v_projection.payment_case_target_inc_vat<>0
         or v_projection.deduction_case_target_inc_vat<>0 then
        raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
      end if;
      update private.bpay_next_transfer
        set umbrella_bridge_kind=v_projection.input_kind,
            umbrella_gross_ex_vat=v_projection.gross_ex_vat,
            umbrella_gross_vat=v_projection.gross_vat,
            umbrella_gross_inc_vat=v_projection.gross_inc_vat,
            umbrella_payment_case_target_inc_vat=v_projection.payment_case_target_inc_vat,
            umbrella_deduction_case_target_inc_vat=v_projection.deduction_case_target_inc_vat
        where id=v_transfer.id;
    end if;
    update private.bpay_next_transfer
      set member_count=v_member_count,member_cash_sum=v_cash_sum,
          status='MEMBERS_READY'
      where id=v_transfer.id;
    update private.bpay_next_job
      set status='DONE',phase='MEMBERS_READY',
          cursor_key=v_cursor::text,
          lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE'
      where id=v_job.command_id;
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY',
      'transfer_id',v_transfer.id,'cursor',v_cursor,
      'member_count',v_member_count,'rows_visited',v_seen,'replay',false);
  end if;
  if v_seen=0 then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_FROZEN_LINE_MISSING';
  end if;
  update private.bpay_next_transfer
    set member_count=v_member_count,member_cash_sum=v_cash_sum
    where id=v_transfer.id;
  update private.bpay_next_job set cursor_key=v_cursor::text
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('phase','BUILDING',
    'transfer_id',v_transfer.id,'cursor',v_cursor,
    'member_count',v_member_count,'rows_visited',v_seen,'replay',false);
end
$function$;

create or replace function private.bpay_next_post_simple_outcome_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_run uuid;
  v_candidate uuid;
  v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_outcome_request%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_line private.bpay_next_run_line%rowtype;
  v_hold private.bpay_next_hold%rowtype;
  v_member record;
  v_cursor bigint;
  v_count bigint;
  v_seen integer:=0;
  v_original uuid;
  v_effect uuid;v_seed_effect uuid;
  v_return_cash private.bpay_next_return_cash%rowtype;
  v_internal private.bpay_next_internal_receipt%rowtype;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit is null or p_limit not between 1 and 100
     or p_expected_cursor is not null and p_expected_cursor<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_OUTCOME_PAGE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,j.candidate_id into strict v_run,v_candidate
    from private.bpay_next_job j
    join private.bpay_next_outcome_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id
    where j.id=p_job_id and r.candidate_id=j.candidate_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select dr38_point_11.* into strict v_job from private.bpay_next_job dr38_point_11 where id=p_job_id for update;
  select dr38_point_12.* into strict v_request from private.bpay_next_outcome_request dr38_point_12
    where command_id=v_job.command_id for update;
  perform 1 from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select dr38_point_13.* into strict v_transfer from private.bpay_next_transfer dr38_point_13
    where id=v_request.transfer_id for update;
  if exists(select 1 from private.bpay_next_run_worker w
            where w.id=v_request.run_worker_id and w.status in ('CANCELLING','CANCELLED')) then
    raise exception using errcode='55000',message='BPAY_NEXT_OUTCOME_CANCELLED_GROUP';
  end if;
  if v_job.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or v_job.module_epoch<>v_epoch
     or v_transfer.candidate_id<>v_candidate
     or v_transfer.run_worker_id<>v_request.run_worker_id then
    raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_SCOPE_INVALID';
  end if;
  if v_job.job_kind='INTERNAL_SETTLEMENT' then
    select dr38_point_14.* into strict v_internal from private.bpay_next_internal_receipt dr38_point_14 where id=v_request.internal_receipt_id;
    if v_request.outcome_id is not null or v_internal.command_id<>v_job.command_id
       or v_internal.transfer_id<>v_transfer.id or v_internal.run_worker_id<>v_request.run_worker_id
       or v_internal.candidate_id<>v_candidate or v_internal.projection_id<>v_transfer.projection_id
       or v_transfer.execution_kind<>'INTERNAL_ZERO' or v_transfer.cash_amount<>0
       or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_OUTCOME_SCOPE_INVALID';
    end if;
  elsif v_transfer.execution_kind<>'BANK' or v_request.outcome_id is null or v_request.internal_receipt_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_BANK_OUTCOME_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_request.posting_complete then
    return pg_catalog.jsonb_build_object('done',true,'replay',true,
      'posted_members',v_request.posted_member_count);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'MEMBERS'
     or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or (v_job.job_kind='CSV_SETTLEMENT' and v_transfer.status not in ('SETTLED','RETURNED'))
     or (v_job.job_kind='INTERNAL_SETTLEMENT' and v_transfer.status<>'INTERNAL_PROCESSING')
     or (v_job.job_kind='INTERNAL_SETTLEMENT' and not exists(select 1 from private.bpay_next_worker_control
       where candidate_id=v_candidate and active_owner_epoch=p_owner_epoch))
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
               and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_OUTCOME_POST_NOT_ELIGIBLE';
  end if;
  if v_job.cursor_key is distinct from p_expected_cursor::text then
    return pg_catalog.jsonb_build_object('done',false,'replay',true,
      'cursor',v_job.cursor_key,'posted_members',v_request.posted_member_count);
  end if;
  v_cursor:=p_expected_cursor; v_count:=v_request.posted_member_count;
  if v_transfer.return_cash_id is not null then
    select dr38_point_15.* into strict v_return_cash from private.bpay_next_return_cash dr38_point_15
      where id=v_transfer.return_cash_id for update;
    if v_transfer.member_count<>1 or v_return_cash.candidate_id<>v_candidate
       or v_return_cash.original_transfer_id<>v_transfer.original_transfer_id
       or v_return_cash.amount_held<v_transfer.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CASH_HOLD_MISMATCH';
    end if;
  end if;
  for v_member in select * from private.bpay_next_transfer_member
    where transfer_id=v_transfer.id and (v_cursor is null or member_no>v_cursor)
    order by member_no limit p_limit
  loop
    if v_member.subject_kind='CASH_REISSUE' then
      if v_transfer.return_cash_id is null
         or v_member.signed_cash_contribution<>v_transfer.cash_amount then
        raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_MEMBER_MISMATCH';
      end if;
      update private.bpay_next_return_cash
        set amount_held=amount_held-v_transfer.cash_amount,
            amount_reissued_paid=amount_reissued_paid+v_transfer.cash_amount
        where id=v_return_cash.id;
    elsif v_transfer.return_cash_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_PAYROLL_MEMBER_FORBIDDEN';
    elsif v_member.subject_kind='WORK' then
      select dr38_point_16.* into strict v_line from private.bpay_next_run_line dr38_point_16 where id=v_member.run_line_id;
      if v_line.run_worker_id<>v_request.run_worker_id
         or v_line.target_pay_channel not in ('PAYE','UMBRELLA')
         or v_line.frozen_inc_vat<>v_member.signed_cash_contribution then
        raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_FROZEN_MEMBER_MISMATCH';
      end if;
      if v_line.target_pay_channel='UMBRELLA' and
         (v_line.source_pay_channel not in ('PAYE','UMBRELLA')
          or not exists(select 1 from private.bpay_next_approved_line a
            where a.id=v_line.approved_line_id and a.revision_id=v_line.captured_revision_id
              and a.component_key=v_line.component_key and (
                (a.component_kind in ('WORK','ADDITIONAL') and a.tax_treatment='TAXABLE')
                or (a.component_kind in ('EXPENSE','MILEAGE') and a.tax_treatment='NON_TAXABLE'
                  and v_line.frozen_ex_vat=v_line.source_consumed_ex_vat)))) then
        raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
      end if;
      if v_line.source_consumed_ex_vat>0 then
        select dr38_point_17.* into strict v_hold from private.bpay_next_hold dr38_point_17 where run_line_id=v_line.id for update;
        if v_hold.status<>'ACTIVE' or v_hold.work_id<>v_line.work_id
           or v_hold.component_key<>v_line.component_key
           or v_hold.source_reserved_ex_vat<>v_line.source_consumed_ex_vat
           or v_hold.target_amount_ex_vat<>v_line.frozen_ex_vat
           or v_hold.target_amount_vat<>v_line.frozen_vat
           or v_hold.target_amount_inc_vat<>v_line.frozen_inc_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_HOLD_MISMATCH';
        end if;
        -- A retained unbound frozen child may settle after common-HEAD adoption.
        -- Seed from the real ACTIVE hold/current APPLIED position only; never
        -- require its old capture to point at a newly-created binding version.
        if exists(select 1 from private.bpay_next_position p
          join private.bpay_next_source_component_binding b on b.canonical_work_id=p.work_id
            and b.canonical_component_key=p.component_key
          join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
          where p.work_id=v_line.work_id and p.component_key=v_line.component_key
            and p.source_book_state='UNBOUND' and v.approval_work_id=p.work_id
            and v.approval_revision_id=p.applied_revision_id and v.publication_command_id_2 is null
            and v.approval_source_channel=v_line.source_pay_channel
            and v.approval_source_channel in ('PAYE','UMBRELLA') and v.tax_treatment='TAXABLE') then
          select b.original_effect_id into v_seed_effect from private.bpay_next_work_collection b
            join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
            where b.work_id=v_line.work_id and b.component_key=v_line.component_key and p.realised_source_ex_vat>0;
          perform * from private.bpay_next_source_disposition_seed_v1(v_line.work_id,v_line.component_key,
            (select applied_revision_id from private.bpay_next_position where work_id=v_line.work_id
              and component_key=v_line.component_key),v_seed_effect,v_hold.id);
        end if;
        select original_timesheet_id into strict v_original from private.bpay_next_work where id=v_line.work_id;
        insert into private.bpay_next_financial_effect
          (operation_id,operation_item_id,effect_kind,candidate_id,work_id,component_key,
           original_timesheet_id,original_transfer_id,source_disposition_ex_vat,
           target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,source_run_line_id,
           source_binding_version_id,canonical_source_work_id,canonical_source_component_key,native_source_channel)
          values(v_job.command_id,v_hold.id,'PAYROLL_SETTLED',v_candidate,
            v_line.work_id,v_line.component_key,v_original,v_transfer.id,
            v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,
            v_hold.target_amount_vat,v_hold.target_amount_inc_vat,
            case when v_line.source_binding_version_id is null then null else v_line.id end,
            v_line.source_binding_version_id,v_line.canonical_source_work_id,v_line.canonical_source_component_key,
            case when v_line.source_binding_version_id is null then null else v_line.source_pay_channel end)
          returning id into v_effect;
        update private.bpay_next_run_worker
          set realised_effect_count=realised_effect_count+1
          where id=v_request.run_worker_id;
        update private.bpay_next_position
          set held_source_ex_vat=held_source_ex_vat-v_hold.source_reserved_ex_vat,
              held_target_ex_vat=held_target_ex_vat-v_hold.target_amount_ex_vat,
              held_target_vat=held_target_vat-v_hold.target_amount_vat,
              held_target_inc_vat=held_target_inc_vat-v_hold.target_amount_inc_vat,
              realised_source_ex_vat=realised_source_ex_vat+v_hold.source_reserved_ex_vat,
              realised_target_ex_vat=realised_target_ex_vat+v_hold.target_amount_ex_vat,
              realised_target_vat=realised_target_vat+v_hold.target_amount_vat,
              realised_target_inc_vat=realised_target_inc_vat+v_hold.target_amount_inc_vat,
              updated_at_utc=pg_catalog.transaction_timestamp()
          where work_id=v_line.work_id and component_key=v_line.component_key
            and source_basis_channel=v_line.source_pay_channel
            and held_source_ex_vat>=v_hold.source_reserved_ex_vat;
        if not found then
          raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_POSITION_MISMATCH';
        end if;
        update private.bpay_next_hold set status='REALISED',finished_at_utc=pg_catalog.transaction_timestamp()
          where id=v_hold.id;
        perform private.bpay_next_reconcile_work_collection_v1(
          p_job_id,v_line.work_id,v_line.component_key,v_effect);
      end if;
    elsif v_member.subject_kind='CASE' then
      perform private.bpay_next_post_case_transfer_member_v1(p_job_id,v_transfer.id,v_member.member_no);
    elsif v_member.subject_kind<>'NET_ADJUSTMENT' then
      raise exception using errcode='55000',message='BPAY_NEXT_SIMPLE_OUTCOME_CASE_NOT_SUPPORTED';
    end if;
    v_cursor:=v_member.member_no; v_count:=v_count+1; v_seen:=v_seen+1;
  end loop;
  if v_seen=0 or v_count>v_transfer.member_count then
    raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_MEMBER_COUNT_INVALID';
  end if;
  update private.bpay_next_outcome_request
    set posted_member_count=v_count,posting_complete=(v_count=v_transfer.member_count)
    where command_id=v_job.command_id;
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate;
  if v_count=v_transfer.member_count then
    if exists(select 1 from private.bpay_next_transfer_member
              where transfer_id=v_transfer.id and member_no>v_cursor) then
      raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_EXTRA_MEMBER';
    end if;
    if v_job.job_kind='INTERNAL_SETTLEMENT' then
      perform private.bpay_next_complete_internal_zero_v1(p_job_id,v_cursor);
    else
      perform private.bpay_next_apply_week_outcome_v1(p_job_id,null);
    end if;
    perform private.bpay_next_complete_destination_leg_v1(p_job_id);
    update private.bpay_next_job set status='DONE',phase='COMPLETE',cursor_key=v_cursor::text,
      lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  else
    update private.bpay_next_job set cursor_key=v_cursor::text where id=p_job_id;
  end if;
  return pg_catalog.jsonb_build_object('done',v_count=v_transfer.member_count,
    'replay',false,'cursor',v_cursor,'posted_members',v_count,'rows_visited',v_seen);
end
$function$;

create or replace function private.bpay_next_build_case_transfer_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_checkpoint bigint;v_cursor bigint;
  v_run private.bpay_next_pay_run%rowtype;v_job private.bpay_next_job%rowtype;v_control private.bpay_next_worker_control%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_transfer private.bpay_next_transfer%rowtype;
  v_build private.bpay_next_case_transfer_build%rowtype;v_projection private.bpay_next_net_projection%rowtype;
  v_draft private.bpay_next_case_allocation_state%rowtype;v_line private.bpay_next_run_line%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;v_seen integer:=0;v_more boolean;v_signed numeric;v_count bigint;v_sum numeric;v_adjustment numeric;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_limit is null or p_limit not between 1 and 100 or (p_expected_cursor is not null and p_expected_cursor<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_TRANSFER_PAGE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_transfer t on t.build_command_id=j.command_id
    join private.bpay_next_case_transfer_build b on b.transfer_id=t.id
    join private.bpay_next_run_worker w on w.id=b.run_worker_id where j.id=p_job_id and j.candidate_id=w.candidate_id;
  select dr38_point_18.* into strict v_run from private.bpay_next_pay_run dr38_point_18 where id=v_run_id for update;
  select dr38_point_19.* into strict v_control from private.bpay_next_worker_control dr38_point_19 where candidate_id=v_candidate for update;
  select dr38_point_20.* into strict v_job from private.bpay_next_job dr38_point_20 where id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id where t.build_command_id=v_job.command_id for update of w;
  select dr38_point_21.* into strict v_transfer from private.bpay_next_transfer dr38_point_21 where build_command_id=v_job.command_id for update;
  select dr38_point_22.* into strict v_build from private.bpay_next_case_transfer_build dr38_point_22 where transfer_id=v_transfer.id for update;
  select dr38_point_23.* into strict v_projection from private.bpay_next_net_projection dr38_point_23 where id=v_build.projection_id;
  select dr38_point_24.* into strict v_draft from private.bpay_next_case_allocation_state dr38_point_24 where id=v_build.draft_state_id;
  if v_job.job_kind<>'TRANSFER_BUILD' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_build.candidate_id
     or v_transfer.run_worker_id<>v_build.run_worker_id or v_transfer.projection_id<>v_build.projection_id
     or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
     or v_transfer.beneficiary_kind<>'CANDIDATE' or v_transfer.beneficiary_id<>v_candidate then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_JOB_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase='MEMBERS_READY' and v_build.stage='COMPLETE' and v_transfer.status<>'BUILDING' then
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY','transfer_status',v_transfer.status,'transfer_id',v_transfer.id,
      'cursor',v_job.cursor_key,'member_count',v_transfer.member_count::text,'rows_visited','0','replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'WORK' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.net_projection_revision<>v_projection.projection_no or v_worker.net_request_revision<>v_projection.projection_no
     or v_projection.retired_at_utc is not null or v_transfer.status<>'BUILDING' or v_transfer.account_approval_ref is not null
     or v_worker.captured_preparation_revision<>v_build.preparation_revision or v_worker.case_selection_revision<>v_build.selection_revision
     or v_transfer.cash_amount<>v_projection.cash_amount
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_TRANSFER_BUILD_NOT_ELIGIBLE';end if;
  v_checkpoint:=nullif(v_build.checkpoint,0);v_cursor:=nullif(v_job.cursor_key,'')::bigint;
  if v_cursor is distinct from v_checkpoint then raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CHECKPOINT_MISMATCH';end if;
  if p_expected_cursor is distinct from v_checkpoint then
    if coalesce(p_expected_cursor,0)>coalesce(v_checkpoint,0) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_TRANSFER_CURSOR_STALE';end if;
    return pg_catalog.jsonb_build_object('phase','BUILDING','transfer_id',v_transfer.id,'cursor',v_job.cursor_key,
      'member_count',v_transfer.member_count::text,'rows_visited','0','replay',true);
  end if;
  v_count:=v_transfer.member_count;v_sum:=v_transfer.member_cash_sum;
  if v_build.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_worker.id
      and (v_build.work_cursor is null or line_no>v_build.work_cursor) order by line_no,id limit p_limit loop
      if v_line.target_pay_channel<>'PAYE' or v_line.frozen_vat<>0
        or (v_line.source_pay_channel='PAYE' or (v_line.source_pay_channel='UMBRELLA'
          and exists(select 1 from private.bpay_next_approved_line a
            join private.bpay_next_work_revision r on r.id=a.revision_id and r.work_id=v_line.work_id
            where a.id=v_line.approved_line_id and a.revision_id=v_line.captured_revision_id
              and a.component_key=v_line.component_key and r.source_pay_channel='UMBRELLA'
              and ((a.component_kind in ('WORK','ADDITIONAL') and a.tax_treatment='TAXABLE')
                or (a.component_kind in ('EXPENSE','MILEAGE') and a.tax_treatment='NON_TAXABLE'
                  and v_line.frozen_ex_vat=v_line.source_consumed_ex_vat)))
          and ((v_line.source_consumed_ex_vat>0 and exists(select 1 from private.bpay_next_hold h
              where h.run_line_id=v_line.id and h.status='ACTIVE' and h.work_id=v_line.work_id
                and h.component_key=v_line.component_key
                and (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
                  is not distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)))
            or (v_line.source_consumed_ex_vat=0 and (v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
              is not distinct from (0::numeric,0::numeric,0::numeric))))) is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_WORK_BASIS_INVALID';end if;
      v_count:=v_count+1;v_sum:=v_sum+v_line.frozen_inc_vat;v_seen:=v_seen+1;
      insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,run_line_id,signed_cash_contribution)
        values(v_transfer.id,v_worker.id,v_count,'WORK',v_line.id,v_line.frozen_inc_vat);
      v_build.processed_work_count:=v_build.processed_work_count+1;v_build.work_cash_total:=v_build.work_cash_total+v_line.frozen_inc_vat;
      v_build.work_cursor:=v_line.line_no;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker.id
      and (v_build.work_cursor is null or line_no>v_build.work_cursor)) into v_more;
    if not v_more then
      if v_build.processed_work_count<>v_build.expected_work_count or v_build.work_cash_total<>v_draft.captured_work_gross then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_WORK_TOTAL_INVALID';end if;
      v_build.stage:='CASE';
    end if;
  elsif v_build.stage='CASE' then
    for v_instruction in select * from private.bpay_next_run_case_instruction where run_worker_id=v_worker.id
      and preparation_revision=v_build.preparation_revision and selection_revision=v_build.selection_revision
      and (v_build.cursor_case_component_id is null or (age_key,case_id,component_ordinal,case_component_id)
        >(v_build.cursor_age_key,v_build.cursor_case_id,v_build.cursor_component_ordinal,v_build.cursor_case_component_id))
      order by age_key,case_id,component_ordinal,case_component_id limit p_limit loop
      if v_instruction.source_pay_channel<>'PAYE' or v_instruction.target_pay_channel<>'PAYE' or v_instruction.nominal_target_vat<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CASE_BASIS_INVALID';end if;
      select dr38_point_25.* into strict v_result from private.bpay_next_case_allocation_result dr38_point_25 where instruction_id=v_instruction.id
        and state_id=case when v_instruction.payroll_stage='NET_DEDUCT' then v_build.net_state_id else v_build.draft_state_id end;
      v_hold:=null;
      if v_result.allocated_source_ex_vat>0 then
        select dr38_point_26.* into strict v_hold from private.bpay_next_case_hold dr38_point_26 where allocation_result_id=v_result.id and status='ACTIVE' for share;
        if not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=v_hold.id and u.status='ACTIVE'
            and u.source_amount_ex_vat=v_result.allocated_source_ex_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CAPACITY_MISMATCH';end if;
      end if;
      v_signed:=case when v_instruction.direction='DEDUCTION' then -v_result.allocated_target_inc_vat else v_result.allocated_target_inc_vat end;
      v_count:=v_count+1;v_sum:=v_sum+v_signed;v_seen:=v_seen+1;
      insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,case_hold_id,
        case_instruction_id,case_allocation_result_id,signed_cash_contribution)
        values(v_transfer.id,v_worker.id,v_count,'CASE',v_hold.id,v_instruction.id,v_result.id,v_signed);
      v_build.processed_case_count:=v_build.processed_case_count+1;
      if v_instruction.payroll_stage='GROSS_ADD' then v_build.gross_additions_total:=v_build.gross_additions_total+v_result.allocated_target_inc_vat;
      elsif v_instruction.payroll_stage='GROSS_DEDUCT' then v_build.gross_deductions_total:=v_build.gross_deductions_total+v_result.allocated_target_inc_vat;
      elsif v_instruction.payroll_stage='NET_ADD' then v_build.net_additions_total:=v_build.net_additions_total+v_result.allocated_target_inc_vat;
      else v_build.net_recoveries_total:=v_build.net_recoveries_total+v_result.allocated_target_inc_vat;end if;
      v_build.cursor_age_key:=v_instruction.age_key;v_build.cursor_case_id:=v_instruction.case_id;
      v_build.cursor_component_ordinal:=v_instruction.component_ordinal;v_build.cursor_case_component_id:=v_instruction.case_component_id;
    end loop;
    select exists(select 1 from private.bpay_next_run_case_instruction where run_worker_id=v_worker.id
      and preparation_revision=v_build.preparation_revision and selection_revision=v_build.selection_revision
      and (v_build.cursor_case_component_id is null or (age_key,case_id,component_ordinal,case_component_id)
        >(v_build.cursor_age_key,v_build.cursor_case_id,v_build.cursor_component_ordinal,v_build.cursor_case_component_id))) into v_more;
    if not v_more then
      if v_build.processed_case_count<>v_build.expected_case_count
         or (v_build.gross_additions_total,v_build.gross_deductions_total,v_build.net_additions_total,v_build.net_recoveries_total)
           is distinct from (v_projection.accepted_gross_additions,v_projection.accepted_gross_deductions,
             v_projection.accepted_net_additions,v_projection.accepted_recoveries) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CASE_TOTAL_INVALID';end if;
      v_build.stage:='FINAL';
    end if;
  elsif v_build.stage='FINAL' then
    if v_build.work_cash_total+v_build.gross_additions_total-v_build.gross_deductions_total<>v_projection.gross_inc_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_PAYROLL_GROSS_MISMATCH';end if;
    v_adjustment:=coalesce(v_projection.entered_paye_net,0)-v_projection.gross_inc_vat;
    v_count:=v_count+1;v_sum:=v_sum+v_adjustment;v_seen:=1;
    insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,signed_cash_contribution)
      values(v_transfer.id,v_worker.id,v_count,'NET_ADJUSTMENT',v_adjustment);
    if v_count<>v_build.expected_work_count+v_build.expected_case_count+1 or v_sum<>v_projection.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CASH_MISMATCH';end if;
    v_build.stage:='COMPLETE';v_build.completed_at_utc:=pg_catalog.transaction_timestamp();
  else raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_STAGE_INVALID';end if;
  update private.bpay_next_case_transfer_build set stage=v_build.stage,checkpoint=checkpoint+1,
    processed_work_count=v_build.processed_work_count,processed_case_count=v_build.processed_case_count,work_cursor=v_build.work_cursor,
    cursor_age_key=v_build.cursor_age_key,cursor_case_id=v_build.cursor_case_id,cursor_component_ordinal=v_build.cursor_component_ordinal,
    cursor_case_component_id=v_build.cursor_case_component_id,work_cash_total=v_build.work_cash_total,
    gross_additions_total=v_build.gross_additions_total,gross_deductions_total=v_build.gross_deductions_total,
    net_additions_total=v_build.net_additions_total,net_recoveries_total=v_build.net_recoveries_total,completed_at_utc=v_build.completed_at_utc
    where transfer_id=v_transfer.id returning checkpoint into v_cursor;
  update private.bpay_next_transfer set member_count=v_count,member_cash_sum=v_sum,
    status=case when v_build.stage='COMPLETE' then 'MEMBERS_READY' else 'BUILDING' end where id=v_transfer.id;
  update private.bpay_next_job set cursor_key=v_cursor::text,
    status=case when v_build.stage='COMPLETE' then 'DONE' else 'LEASED' end,
    phase=case when v_build.stage='COMPLETE' then 'MEMBERS_READY' else 'WORK' end,
    lease_nonce=case when v_build.stage='COMPLETE' then null else lease_nonce end,
    lease_until_utc=case when v_build.stage='COMPLETE' then null else lease_until_utc end where id=p_job_id;
  if v_build.stage='COMPLETE' then update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;end if;
  return pg_catalog.jsonb_build_object('phase',case when v_build.stage='COMPLETE' then 'MEMBERS_READY' else 'BUILDING' end,
    'transfer_id',v_transfer.id,'cursor',v_cursor::text,'member_count',v_count::text,'rows_visited',v_seen::text,'replay',false);
end
$function$;

create or replace function private.bpay_next_build_destination_group_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_cursor bigint;v_checkpoint bigint;
  v_run private.bpay_next_pay_run%rowtype;v_job private.bpay_next_job%rowtype;v_control private.bpay_next_worker_control%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_anchor private.bpay_next_transfer%rowtype;v_leg private.bpay_next_transfer%rowtype;
  v_g private.bpay_next_destination_group%rowtype;v_p private.bpay_next_net_projection%rowtype;
  v_n private.bpay_next_net_destination_state%rowtype;v_draft private.bpay_next_case_allocation_state%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_i private.bpay_next_run_case_instruction%rowtype;
  v_r private.bpay_next_case_allocation_result%rowtype;v_h private.bpay_next_case_hold%rowtype;
  v_c private.bpay_next_net_destination_credit%rowtype;v_d private.bpay_next_net_destination_amount%rowtype;
  v_seen integer:=0;v_more boolean;v_signed numeric;v_leg_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_limit is null or p_limit not between 1 and 100 or p_expected_cursor<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_DESTINATION_PAGE_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_transfer t on t.build_command_id=j.command_id
    join private.bpay_next_destination_group g on g.anchor_transfer_id=t.id
    join private.bpay_next_run_worker w on w.id=g.run_worker_id where j.id=p_job_id and j.candidate_id=w.candidate_id;
  select dr38_point_27.* into strict v_run from private.bpay_next_pay_run dr38_point_27 where id=v_run_id for update;
  select dr38_point_28.* into strict v_control from private.bpay_next_worker_control dr38_point_28 where candidate_id=v_candidate for update;
  select dr38_point_29.* into strict v_job from private.bpay_next_job dr38_point_29 where id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id where t.build_command_id=v_job.command_id for update of w;
  select dr38_point_30.* into strict v_anchor from private.bpay_next_transfer dr38_point_30 where build_command_id=v_job.command_id for update;
  select dr38_point_31.* into strict v_g from private.bpay_next_destination_group dr38_point_31 where anchor_transfer_id=v_anchor.id for update;
  select dr38_point_32.* into strict v_p from private.bpay_next_net_projection dr38_point_32 where id=v_g.projection_id;
  select dr38_point_33.* into strict v_n from private.bpay_next_net_destination_state dr38_point_33 where state_id=v_g.net_state_id;
  select dr38_point_34.* into strict v_draft from private.bpay_next_case_allocation_state dr38_point_34 where id=v_g.draft_state_id;
  if v_job.job_kind<>'TRANSFER_BUILD' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_g.candidate_id
     or v_anchor.run_worker_id<>v_g.run_worker_id or v_anchor.cash_amount<>v_n.own_amount
     or v_n.projection_id<>v_p.id or v_n.cash_amount<>v_p.cash_amount
     or v_anchor.beneficiary_kind<>'CANDIDATE' or v_anchor.beneficiary_id<>v_candidate then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_JOB_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase='MEMBERS_READY' and v_g.stage='COMPLETE' then
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY','transfer_status',v_anchor.status,'transfer_id',v_anchor.id,
      'cursor',v_job.cursor_key,'member_count',v_anchor.member_count::text,'rows_visited','0','replay',true);end if;
  if v_job.status<>'LEASED' or v_job.phase<>'WORK' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.net_projection_revision<>v_p.projection_no or v_worker.net_request_revision<>v_p.projection_no
     or v_p.retired_at_utc is not null or v_anchor.account_approval_ref is not null
     or v_worker.captured_preparation_revision<>v_g.preparation_revision or v_worker.case_selection_revision<>v_g.selection_revision
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_DESTINATION_BUILD_NOT_ELIGIBLE';end if;
  v_checkpoint:=nullif(v_g.checkpoint,0);v_cursor:=nullif(v_job.cursor_key,'')::bigint;
  if v_cursor is distinct from v_checkpoint then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CHECKPOINT_MISMATCH';end if;
  if p_expected_cursor is distinct from v_checkpoint then
    if coalesce(p_expected_cursor,0)>coalesce(v_checkpoint,0) then
      raise exception using errcode='55000',message='BPAY_NEXT_DESTINATION_CURSOR_STALE';end if;
    return pg_catalog.jsonb_build_object('phase','BUILDING','transfer_id',v_anchor.id,'cursor',v_job.cursor_key,
      'member_count',v_anchor.member_count::text,'rows_visited','0','replay',true);end if;
  if v_g.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_worker.id
      and (v_g.work_cursor is null or line_no>v_g.work_cursor) order by line_no limit p_limit loop
      v_anchor.member_count:=v_anchor.member_count+1;v_anchor.member_cash_sum:=v_anchor.member_cash_sum+v_line.frozen_inc_vat;
      if v_line.target_pay_channel<>'PAYE' or v_line.frozen_vat<>0
        or (v_line.source_pay_channel='PAYE' or (v_line.source_pay_channel='UMBRELLA'
          and exists(select 1 from private.bpay_next_approved_line a
            join private.bpay_next_work_revision r on r.id=a.revision_id and r.work_id=v_line.work_id
            where a.id=v_line.approved_line_id and a.revision_id=v_line.captured_revision_id
              and a.component_key=v_line.component_key and r.source_pay_channel='UMBRELLA'
              and ((a.component_kind in ('WORK','ADDITIONAL') and a.tax_treatment='TAXABLE')
                or (a.component_kind in ('EXPENSE','MILEAGE') and a.tax_treatment='NON_TAXABLE'
                  and v_line.frozen_ex_vat=v_line.source_consumed_ex_vat)))
          and ((v_line.source_consumed_ex_vat>0 and exists(select 1 from private.bpay_next_hold h
              where h.run_line_id=v_line.id and h.status='ACTIVE' and h.work_id=v_line.work_id
                and h.component_key=v_line.component_key
                and (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
                  is not distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)))
            or (v_line.source_consumed_ex_vat=0 and (v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
              is not distinct from (0::numeric,0::numeric,0::numeric))))) is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_WORK_BASIS_INVALID';end if;
      insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,run_line_id,signed_cash_contribution)
        values(v_anchor.id,v_worker.id,v_anchor.member_count,'WORK',v_line.id,v_line.frozen_inc_vat);
      v_g.processed_work_count:=v_g.processed_work_count+1;v_g.work_cash_total:=v_g.work_cash_total+v_line.frozen_inc_vat;
      v_g.member_cash_total:=v_g.member_cash_total+v_line.frozen_inc_vat;v_g.work_cursor:=v_line.line_no;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker.id
      and (v_g.work_cursor is null or line_no>v_g.work_cursor)) into v_more;
    if not v_more then
      if v_g.processed_work_count<>v_g.expected_work_count or v_g.work_cash_total<>v_draft.captured_work_gross then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_WORK_TOTAL_MISMATCH';end if;
      v_g.stage:='CASE';end if;
  elsif v_g.stage='CASE' then
    for v_i in select * from private.bpay_next_run_case_instruction where run_worker_id=v_worker.id
      and preparation_revision=v_g.preparation_revision and selection_revision=v_g.selection_revision
      and (v_g.cursor_case_component_id is null or (age_key,case_id,component_ordinal,case_component_id)
        >(v_g.cursor_age_key,v_g.cursor_case_id,v_g.cursor_component_ordinal,v_g.cursor_case_component_id))
      order by age_key,case_id,component_ordinal,case_component_id limit p_limit loop
      select dr38_point_35.* into strict v_r from private.bpay_next_case_allocation_result dr38_point_35 where instruction_id=v_i.id
        and state_id=case when v_i.payroll_stage='NET_DEDUCT' then v_g.net_state_id else v_g.draft_state_id end;
      v_h:=null;
      if v_r.allocated_source_ex_vat>0 then
        select dr38_point_36.* into strict v_h from private.bpay_next_case_hold dr38_point_36 where allocation_result_id=v_r.id and status='ACTIVE' for share;
        if not exists(select 1 from private.bpay_next_case_capacity_use where case_hold_id=v_h.id and status='ACTIVE'
          and source_amount_ex_vat=v_r.allocated_source_ex_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CAPACITY_MISMATCH';end if;
      end if;
      select dr38_point_37.* into v_c from private.bpay_next_net_destination_credit dr38_point_37 where state_id=v_g.net_state_id and instruction_id=v_i.id;
      if found then
        select dr38_point_38.* into strict v_d from private.bpay_next_net_destination_amount dr38_point_38 where destination_id=v_c.destination_id;
        select l.transfer_id into v_leg_id from private.bpay_next_destination_group_leg l where l.destination_id=v_d.destination_id;
        if not found then
          if v_g.created_leg_count>=2147483647 then raise exception using errcode='22023',message='BPAY_NEXT_DESTINATION_LEG_NUMBER_RANGE';end if;
          insert into private.bpay_next_transfer(run_worker_id,candidate_id,projection_id,transfer_no,beneficiary_kind,beneficiary_id,cash_amount,status,execution_kind)
            values(v_worker.id,v_candidate,v_p.id,(v_g.created_leg_count+1)::integer,'CANDIDATE',v_candidate,v_d.amount,'BUILDING','BANK')
            returning id into v_leg_id;
          insert into private.bpay_next_destination_group_leg(transfer_id,anchor_transfer_id,destination_id,leg_kind)
            values(v_leg_id,v_anchor.id,v_d.destination_id,'ONEOFF');
          v_g.created_leg_count:=v_g.created_leg_count+1;
        end if;
        select dr38_point_39.* into strict v_leg from private.bpay_next_transfer dr38_point_39 where id=v_leg_id for update;
      else v_leg:=v_anchor;end if;
      v_signed:=case when v_i.direction='DEDUCTION' then -v_r.allocated_target_inc_vat else v_r.allocated_target_inc_vat end;
      v_leg.member_count:=v_leg.member_count+1;v_leg.member_cash_sum:=v_leg.member_cash_sum+v_signed;
      insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,case_hold_id,
        case_instruction_id,case_allocation_result_id,signed_cash_contribution)
        values(v_leg.id,v_worker.id,v_leg.member_count,'CASE',v_h.id,v_i.id,v_r.id,v_signed);
      if v_leg.id=v_anchor.id then v_anchor:=v_leg;
      else update private.bpay_next_transfer set member_count=v_leg.member_count,member_cash_sum=v_leg.member_cash_sum where id=v_leg.id;end if;
      v_g.processed_case_count:=v_g.processed_case_count+1;v_g.member_cash_total:=v_g.member_cash_total+v_signed;
      if v_i.payroll_stage='GROSS_ADD' then v_g.gross_additions_total:=v_g.gross_additions_total+v_r.allocated_target_inc_vat;
      elsif v_i.payroll_stage='GROSS_DEDUCT' then v_g.gross_deductions_total:=v_g.gross_deductions_total+v_r.allocated_target_inc_vat;
      elsif v_i.payroll_stage='NET_ADD' then v_g.net_additions_total:=v_g.net_additions_total+v_r.allocated_target_inc_vat;
      else v_g.net_recoveries_total:=v_g.net_recoveries_total+v_r.allocated_target_inc_vat;end if;
      v_g.cursor_age_key:=v_i.age_key;v_g.cursor_case_id:=v_i.case_id;v_g.cursor_component_ordinal:=v_i.component_ordinal;
      v_g.cursor_case_component_id:=v_i.case_component_id;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_case_instruction where run_worker_id=v_worker.id
      and preparation_revision=v_g.preparation_revision and selection_revision=v_g.selection_revision
      and (v_g.cursor_case_component_id is null or (age_key,case_id,component_ordinal,case_component_id)
        >(v_g.cursor_age_key,v_g.cursor_case_id,v_g.cursor_component_ordinal,v_g.cursor_case_component_id))) into v_more;
    if not v_more then
      if v_g.processed_case_count<>v_g.expected_case_count or v_g.created_leg_count<>v_g.expected_leg_count
        or (v_g.gross_additions_total,v_g.gross_deductions_total,v_g.net_additions_total,v_g.net_recoveries_total)
          is distinct from (v_p.accepted_gross_additions,v_p.accepted_gross_deductions,v_p.accepted_net_additions,v_p.accepted_recoveries) then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CASE_TOTAL_MISMATCH';end if;
      v_g.stage:='ADJUST';end if;
  elsif v_g.stage='ADJUST' then
    if v_g.work_cash_total+v_g.gross_additions_total-v_g.gross_deductions_total<>v_p.gross_inc_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_GROSS_MISMATCH';end if;
    v_signed:=coalesce(v_p.entered_paye_net,0)-v_p.gross_inc_vat;
    v_anchor.member_count:=v_anchor.member_count+1;v_anchor.member_cash_sum:=v_anchor.member_cash_sum+v_signed;
    insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,signed_cash_contribution)
      values(v_anchor.id,v_worker.id,v_anchor.member_count,'NET_ADJUSTMENT',v_signed);
    v_g.member_cash_total:=v_g.member_cash_total+v_signed;
    if v_g.member_cash_total<>v_p.cash_amount or v_anchor.member_cash_sum<>v_n.own_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CASH_MISMATCH';end if;
    v_g.stage:='SEAL';v_seen:=1;
  elsif v_g.stage='SEAL' then
    for v_leg in select t.* from private.bpay_next_transfer t where t.run_worker_id=v_worker.id
      and (v_g.seal_cursor is null or t.transfer_no>v_g.seal_cursor) order by t.transfer_no limit p_limit loop
      if v_leg.status<>'BUILDING' or v_leg.member_cash_sum<>v_leg.cash_amount
        or not exists(select 1 from private.bpay_next_destination_group_leg where transfer_id=v_leg.id and anchor_transfer_id=v_anchor.id) then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_LEG_SEAL_MISMATCH';end if;
      update private.bpay_next_transfer set status='MEMBERS_READY' where id=v_leg.id;
      v_g.seal_cursor:=v_leg.transfer_no;v_g.sealed_leg_count:=v_g.sealed_leg_count+1;
      v_g.sealed_cash_total:=v_g.sealed_cash_total+v_leg.cash_amount;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_transfer where run_worker_id=v_worker.id
      and (v_g.seal_cursor is null or transfer_no>v_g.seal_cursor)) into v_more;
    if not v_more then
      if v_g.sealed_leg_count<>v_g.expected_leg_count or v_g.sealed_cash_total<>v_p.cash_amount then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_GROUP_SEAL_MISMATCH';end if;
      v_g.stage:='COMPLETE';v_g.completed_at_utc:=pg_catalog.transaction_timestamp();end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STAGE_INVALID';end if;
  -- Never turn a already sealed anchor back into BUILDING during leg sealing.
  if v_g.seal_cursor is null then
    update private.bpay_next_transfer set member_count=v_anchor.member_count,member_cash_sum=v_anchor.member_cash_sum where id=v_anchor.id;
  end if;
  v_cursor:=v_g.checkpoint+1;
  update private.bpay_next_destination_group set stage=v_g.stage,checkpoint=v_cursor,
    processed_work_count=v_g.processed_work_count,processed_case_count=v_g.processed_case_count,created_leg_count=v_g.created_leg_count,
    sealed_leg_count=v_g.sealed_leg_count,work_cursor=v_g.work_cursor,cursor_age_key=v_g.cursor_age_key,cursor_case_id=v_g.cursor_case_id,
    cursor_component_ordinal=v_g.cursor_component_ordinal,cursor_case_component_id=v_g.cursor_case_component_id,seal_cursor=v_g.seal_cursor,
    work_cash_total=v_g.work_cash_total,gross_additions_total=v_g.gross_additions_total,gross_deductions_total=v_g.gross_deductions_total,
    net_additions_total=v_g.net_additions_total,net_recoveries_total=v_g.net_recoveries_total,member_cash_total=v_g.member_cash_total,
    sealed_cash_total=v_g.sealed_cash_total,completed_at_utc=v_g.completed_at_utc where anchor_transfer_id=v_anchor.id;
  update private.bpay_next_job set cursor_key=v_cursor::text,status=case when v_g.stage='COMPLETE' then 'DONE' else 'LEASED' end,
    phase=case when v_g.stage='COMPLETE' then 'MEMBERS_READY' else 'WORK' end,
    lease_nonce=case when v_g.stage='COMPLETE' then null else lease_nonce end,
    lease_until_utc=case when v_g.stage='COMPLETE' then null else lease_until_utc end where id=p_job_id;
  if v_g.stage='COMPLETE' then update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;end if;
  return pg_catalog.jsonb_build_object('phase',case when v_g.stage='COMPLETE' then 'MEMBERS_READY' else 'BUILDING' end,
    'transfer_id',v_anchor.id,'cursor',v_cursor::text,'member_count',v_anchor.member_count::text,'rows_visited',v_seen::text,'replay',false);
end $function$;
alter function private.bpay_next_publish_ordinary_from_financial_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_publish_ordinary_from_financial_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
