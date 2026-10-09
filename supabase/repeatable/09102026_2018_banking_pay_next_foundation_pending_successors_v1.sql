-- F-05 complete current successors of the22 changed functions in18 reviewed RESET deltas.
-- Reuse every unchanged function/body and installed migration. F-06 binding/evidence/readers
-- ship in the same release. F-04 sole target valuer is1157, never this file.
-- Owner/epoch admission and frozen input identity remain exactly the saved authorities.
\set ON_ERROR_STOP on
begin;

-- F-05-D02 successor of supabase/repeatable/03102026_1530_banking_pay_next_ordinary_approval_v1.sql
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
  v_available_daily_rows jsonb;
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
  select * into strict v_contract from public.contracts
    where id=v_ts.contract_id;
  v_route:=private.bpay_next_approval_route_v1(v_ts.timesheet_id);
  if v_route not in ('ORDINARY_HOURS','EXPENSE_CARRIER')
     or v_tf.is_current is distinct from true
     or v_tf.is_stale is distinct from false
     or v_tf.authorised_at_utc is null
     or v_ts.authorised_at_server is null
     or v_tf.timesheet_version<>v_ts.version
     or v_tf.candidate_id is distinct from v_contract.candidate_id
     or v_tf.client_id is distinct from v_contract.client_id
     or upper(coalesce(v_tf.pay_method,'')) not in ('PAYE','UMBRELLA') then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_APPROVAL_NOT_CURRENT';
  end if;
  select coalesce(nullif(btrim(c.display_name),''),
                  nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),
                  c.id::text),c.tms_ref,c.mileage_pay_rate
    into strict v_candidate_name,v_candidate_ref,v_candidate_mileage_pay_rate
    from public.candidates c where c.id=v_contract.candidate_id;
  select name,mileage_charge_rate into strict
    v_client_name,v_client_mileage_charge_rate from public.clients
    where id=v_contract.client_id;
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
    v_base_count:=case when v_base_amount<>0 or v_tf.total_hours<>0 then 1 else 0 end;
  elsif v_mode='EXPENSES_ONLY' and v_route='EXPENSE_CARRIER' then
    v_detail_kind:='FIXED';
  else
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_FINANCIAL_MODE_UNSUPPORTED';
  end if;
  if jsonb_typeof(v_tf.additional_units_json)<>'object' then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_ADDITIONAL_NOT_OBJECT';
  end if;
  select count(*) into v_additional_count from jsonb_each(v_tf.additional_units_json);
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
  v_resolved_daily_rates:=v_route in ('ORDINARY_HOURS','DAILY_HOURS')
    and v_ts.sheet_scope='DAILY'::public.timesheet_scope_enum;
  if v_resolved_daily_rates then
    v_available_daily_evidence:=private.bpay_next_daily_approval_evidence_v1(v_tf.id);
    select (v_available_daily_evidence->'rates')
      ||coalesce(jsonb_agg(to_jsonb(r) order by r.rate_family,r.rate_code),'[]'::jsonb)
      into v_available_daily_rows
      from private.bpay_next_contract_rate_rows_v1(
        '{}'::jsonb,v_contract.bucket_labels_json,v_contract.additional_rates_json,
        v_mileage_pay_rate,v_mileage_charge_rate) r;
    v_schedule_count:=jsonb_array_length(v_available_daily_rows);
  else
    select count(*) into v_schedule_count
      from private.bpay_next_contract_rate_rows_v1(
        v_contract.rates_json,v_contract.bucket_labels_json,v_contract.additional_rates_json,
        v_mileage_pay_rate,v_mileage_charge_rate);
  end if;

  insert into private.bpay_next_worker_control(candidate_id)
    values (v_contract.candidate_id) on conflict (candidate_id) do nothing;
  select * into v_work from private.bpay_next_work
    where booking_id=v_ts.booking_id for update;
  if not found then
    insert into private.bpay_next_work
      (candidate_id,contract_id,original_timesheet_id,booking_id,
       work_kind,week_ending_date)
      values (v_contract.candidate_id,v_contract.id,v_ts.timesheet_id,
              v_ts.booking_id,
              case when v_route='EXPENSE_CARRIER' then 'EXPENSE' else 'ORDINARY' end,
              v_ts.week_ending_date)
      returning * into v_work;
  elsif v_work.candidate_id<>v_contract.candidate_id
     or v_work.contract_id<>v_contract.id
     or v_work.week_ending_date<>v_ts.week_ending_date
     or v_work.work_kind<>(case when v_route='EXPENSE_CARRIER'
       then 'EXPENSE' else 'ORDINARY' end) then
    raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_WORK_IDENTITY_CONFLICT';
  end if;
  insert into private.bpay_next_work_revision
    (work_id,revision_no,source_kind,physical_timesheet_id,
     financial_snapshot_id,physical_timesheet_version,
     source_pay_channel,week_ending_date,detail_kind,
     expected_line_count,expected_rate_schedule_count,certified_zero,
     approved_source_ex_vat,candidate_display_name,candidate_reference,
     client_display_name,job_title,band_label,timesheet_reference)
    values (v_work.id,v_work.current_revision_no+1,
            case when v_route='EXPENSE_CARRIER' then 'EXPENSE' else 'ORDINARY' end,
            v_ts.timesheet_id,v_tf.id,v_ts.version,upper(v_tf.pay_method),
            v_ts.week_ending_date,v_detail_kind,v_expected_count,
            v_schedule_count,v_expected_total=0,v_expected_total,
            v_candidate_name,v_candidate_ref,v_client_name,
            v_contract.role,v_contract.band,v_ts.reference_number)
    returning id into v_revision_id;
  if v_resolved_daily_rates then
    insert into private.bpay_next_rate_schedule
      (revision_id,rate_family,rate_code,unit_label,paye_rate,umbrella_rate,charge_rate)
      select v_revision_id,r.rate_family,r.rate_code,r.unit_label,r.paye_rate,r.umbrella_rate,r.charge_rate
      from jsonb_to_recordset(v_available_daily_rows)
        r(rate_family text,rate_code text,unit_label text,paye_rate numeric,umbrella_rate numeric,charge_rate numeric);
  else
    insert into private.bpay_next_rate_schedule
      (revision_id,rate_family,rate_code,unit_label,paye_rate,umbrella_rate,charge_rate)
      select v_revision_id,r.rate_family,r.rate_code,r.unit_label,r.paye_rate,r.umbrella_rate,r.charge_rate
      from private.bpay_next_contract_rate_rows_v1(
        v_contract.rates_json,v_contract.bucket_labels_json,v_contract.additional_rates_json,
        v_mileage_pay_rate,v_mileage_charge_rate) r;
  end if;

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
          if v_bucket.pay_rate is null
             or v_bucket.pay_rate<>round(v_bucket.pay_rate,6)
             or abs(v_bucket.pay_rate)>=1000000000000 then
            raise exception using errcode='23514', message='BPAY_NEXT_ORDINARY_USED_RATE_MISSING';
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
         segment_pay_ex_vat,pay_excluded,hours_day,hours_night,hours_sat,hours_sun,hours_bh)
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
          coalesce((v_segment->>'hours_bh')::numeric,0)) returning id into v_shift_id;
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
      (v_tf.hours_day,v_tf.pay_day),(v_tf.hours_night,v_tf.pay_night),
      (v_tf.hours_sat,v_tf.pay_sat),(v_tf.hours_sun,v_tf.pay_sun),
      (v_tf.hours_bh,v_tf.pay_bh)) b(hours,pay_rate)
    loop
      if v_bucket.hours<0 or v_bucket.hours<>round(v_bucket.hours,6)
         or (v_bucket.hours>0 and
             (v_bucket.pay_rate is null
              or v_bucket.pay_rate<>round(v_bucket.pay_rate,6)
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
      (revision_id,line_no,component_key,component_kind,approved_quantity,
       expected_rate_detail_count,source_pay_ex_vat,tax_treatment,evidence_ref)
      values (v_revision_id,v_line_no,'WORK:AGGREGATE','WORK',v_tf.total_hours,
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
    v_amount:=coalesce((v_extra.value->>'pay_ex_vat')::numeric,
      (v_extra.value->>'amount_ex_vat')::numeric,
      coalesce((v_extra.value->>'unit_count')::numeric,
               (v_extra.value->>'units_week')::numeric,0)
       *coalesce((v_extra.value->>'pay_rate')::numeric,
                 (v_extra.value->>'rate')::numeric,0));
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

ALTER FUNCTION private.bpay_next_publish_ordinary_from_financial_v1(uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_publish_ordinary_from_financial_v1(uuid) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D01 successor of supabase/repeatable/03102026_1600_banking_pay_next_prepare_pin_v1.sql
create or replace function private.bpay_next_claim_prepare_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_run_id uuid;
  v_module_epoch bigint;
  v_channel text;
  v_umbrella_id uuid;
  v_umbrella_vat_chargeable boolean;
  v_owner_epoch bigint;
  v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PREPARE_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select j.candidate_id,rc.run_id into strict v_job.candidate_id,v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_job.candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.module_epoch<>v_module_epoch
     or not exists(select 1 from private.bpay_next_command command
       where command.id=v_job.command_id and command.agency_sequence=v_job.command_sequence
         and command.module_epoch=v_module_epoch and command.command_kind='PREPARE'
         and command.status='SEALED' and command.sealed_at_utc is not null
         and command.expected_member_count=v_run.selected_candidate_count
         and command.enrolled_member_count=command.expected_member_count)
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED'
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE';
  end if;
  if exists(select 1 from private.bpay_next_job earlier
      where earlier.candidate_id=v_job.candidate_id
        and earlier.command_sequence<v_job.command_sequence
        and earlier.status<>'DONE') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';
  end if;
  select * into v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if not found then
    select upper(c.pay_method),c.umbrella_id,u.vat_chargeable
      into strict v_channel,v_umbrella_id,v_umbrella_vat_chargeable
      from public.candidates c
      left join public.umbrellas u on u.id=c.umbrella_id
      where c.id=v_job.candidate_id for share of c;
    if v_channel not in ('PAYE','UMBRELLA') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_PAY_CHANNEL_UNSUPPORTED';
    end if;
    insert into private.bpay_next_run_worker
      (run_id,candidate_id,target_pay_channel,target_umbrella_id,
       target_umbrella_vat_chargeable,status)
      values(v_run_id,v_job.candidate_id,v_channel,
        case when v_channel='UMBRELLA' then v_umbrella_id else null end,
        case when v_channel='UMBRELLA' then v_umbrella_vat_chargeable
          else null end,'PREPARING')
      returning * into v_worker;
  elsif v_worker.status<>'PREPARING' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_WORKER_STATE_CHANGED';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_job.candidate_id
    returning active_owner_epoch into v_owner_epoch;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()
          +pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner_epoch;
  return next;
end
$function$;

ALTER FUNCTION private.bpay_next_claim_prepare_job_v1(uuid,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_claim_prepare_job_v1(uuid,integer) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D06 successor of supabase/repeatable/03102026_1610_banking_pay_next_position_capture_v1.sql
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
  v_issue text;
  v_issue_work_id uuid;
  v_query_pin private.bpay_next_run_work_query_pin_v2%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_book record;
  v_seed_effect uuid;v_seed_hold uuid;v_seed_holds uuid[];v_seed record;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CAPTURE_POSITION_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
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
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_WORKER_NOT_PREPARING';
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  for v_member in
    -- Each lane emits at most one page before the bounded merge. SUBSET is
    -- driven by selected membership, not an EXISTS filter over all positions.
    -- A selected missing position remains a member and is refused below.
    with all_page as materialized (
      select rw.id as run_work_id,rw.work_id,rw.captured_revision_id,
             p.component_key,'ALL'::text as selection_mode
        from private.bpay_next_run_work rw
        join private.bpay_next_work_choice choice
          on choice.run_id=v_run_id and choice.work_id=rw.work_id
         and choice.selection_mode='ALL' and choice.selection_state='SEALED'
         and choice.expected_revision_id=rw.captured_revision_id
        join private.bpay_next_position p
          on p.work_id=rw.work_id
         and p.applied_revision_id=rw.captured_revision_id
        where rw.run_worker_id=v_worker.id
          and (v_work_cursor is null or rw.work_id>v_work_cursor
            or (rw.work_id=v_work_cursor and p.component_key>v_component_cursor))
        order by rw.work_id,p.component_key limit p_limit
    ), selected_page as materialized (
      select rw.id as run_work_id,rw.work_id,rw.captured_revision_id,
             member.component_key,'SUBSET'::text as selection_mode
        from private.bpay_next_selected_component member
        join private.bpay_next_run_work rw
          on rw.work_id=member.work_id
         and rw.captured_revision_id=member.expected_revision_id
        join private.bpay_next_work_choice choice
          on choice.run_id=member.run_id and choice.work_id=member.work_id
         and choice.selection_mode='SUBSET' and choice.selection_state='SEALED'
         and choice.expected_revision_id=rw.captured_revision_id
        where member.run_id=v_run_id and rw.run_worker_id=v_worker.id
          and (v_work_cursor is null or member.work_id>v_work_cursor
            or (member.work_id=v_work_cursor and member.component_key>v_component_cursor))
        order by member.work_id,member.component_key limit p_limit
    )
    select * from (
      select * from all_page union all select * from selected_page
    ) page order by work_id,component_key limit p_limit
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
    if v_worker.target_pay_channel='UMBRELLA' and v_issue is null and
       (v_row.source_pay_channel is distinct from 'UMBRELLA'
        or v_row.work_kind not in ('ORDINARY','SOURCE')
        or v_row.component_kind is distinct from 'WORK'
        or v_row.tax_treatment is distinct from 'TAXABLE') then
      v_issue:='UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    if v_issue is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code=v_issue,
            review_issue_work_id=v_member.work_id
        where id=v_worker.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code',v_issue,'work_id',v_member.work_id,'replay',false);
    end if;
    -- Typed initial I-1 is the ONLY new no-HEAD binding arm. Initial and
    -- common-HEAD captures require self-canonical same-channel TAXABLE WORK.
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
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.component_kind is distinct from 'WORK' or v_row.tax_treatment is distinct from 'TAXABLE' then
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
               v_row.source_pay_channel,'TAXABLE'::text,v_row.approved_source_ex_vat,
               case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end) then
          v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
        end if;
      end if;
      if v_issue is not null then
        update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,
          review_issue_work_id=v_row.work_id where id=v_worker.id;
        update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,
          lease_nonce=null,lease_until_utc=null where id=p_job_id;
        return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','issue_code',v_issue,
          'work_id',v_row.work_id,'rows_visited',v_seen,'replay',false);
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
      if found and (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.prepare_job_id,v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,p_job_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        v_issue:='SOURCE_PAY_QUERY_OPEN';
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        update private.bpay_next_run_worker
          set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_row.work_id
          where id=v_worker.id;
        update private.bpay_next_job
          set status='DONE',phase='DONE',cursor_key=null,
              lease_nonce=null,lease_until_utc=null
          where id=p_job_id;
        return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
          'issue_code',v_issue,'work_id',v_row.work_id,'rows_visited',v_seen,'replay',false);
      end if;
    end if;
    insert into private.bpay_next_run_position
      (run_worker_id,run_work_id,work_id,captured_revision_id,
       component_key,approved_line_id,source_pay_channel,
       source_basis_channel,
       approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,
       residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,
       realised_target_inc_vat,held_target_ex_vat,held_target_vat,
       held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,
       source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision)
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
       case when v_binding.id is null then null else v_row.source_book_revision end);
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
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code','CAPTURE_POSITION_CHANGED','work_id',v_issue_work_id,'replay',false);
    end if;
    update private.bpay_next_job
      set phase='ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'rows_visited',v_seen,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
    'rows_visited',v_seen,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

ALTER FUNCTION private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D05 successor of supabase/repeatable/03102026_1620_banking_pay_next_fresh_positive_hold_v1.sql
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
  v_run_line_id uuid;
  v_seen integer:=0;
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
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
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
  select * into strict v_worker from private.bpay_next_run_worker
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
    update private.bpay_next_job
      set status='DONE',phase='DONE',cursor_key=null,
          lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
      'issue_code','FINANCIAL_RESOLUTION_REQUIRED',
      'work_id',v_issue_work_id,'replay',false);
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
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code','TARGET_UMBRELLA_MISSING',
        'work_id',v_issue_work_id,'replay',false);
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
    select rp.*,rw.id as pinned_work_id,rev.valuation_policy_id
      from private.bpay_next_run_position rp
      join private.bpay_next_run_work rw on rw.id=rp.run_work_id
      join private.bpay_next_work_revision rev
        on rev.id=rp.captured_revision_id and rev.work_id=rp.work_id
      where rp.run_worker_id=v_worker.id
        and (v_work_cursor is null or rp.work_id>v_work_cursor
          or (rp.work_id=v_work_cursor
            and rp.component_key>v_component_cursor))
      order by rp.work_id,rp.component_key limit p_limit
  loop
    if v_row.needs_financial_resolution
       or (v_row.work_collection_id is null and
         (v_row.approved_line_id is null or v_row.residual_source_ex_vat<0)) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_HOLD_UNEXPECTED_POSITION_SHAPE';
    end if;
    select * into strict v_position from private.bpay_next_position
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
             v_row.captured_revision_id,v_row.approved_line_id,v_row.source_pay_channel,'TAXABLE'::text,
             v_row.approved_source_ex_vat,
             case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end)
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
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
    else
    select * into strict v_value
      from private.bpay_next_value_source_for_target_v1(
        v_row.residual_source_ex_vat,v_row.source_pay_channel,
        v_worker.target_pay_channel,
        case when v_worker.target_pay_channel='UMBRELLA'
          then v_worker.target_umbrella_vat_chargeable else false end,
        v_row.valuation_policy_id,v_run.pay_date);
    v_line_count:=v_line_count+1;
    insert into private.bpay_next_run_line
      (run_worker_id,run_work_id,line_no,approved_line_id,
       captured_revision_id,work_id,component_key,source_consumed_ex_vat,
       source_pay_channel,target_pay_channel,valuation_policy_id,
       policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat,source_binding_version_id,
       canonical_source_work_id,canonical_source_component_key,source_book_revision,updates_original_source_scalar)
      values(v_worker.id,v_row.run_work_id,v_line_count,
       v_row.approved_line_id,v_row.captured_revision_id,v_row.work_id,
       v_row.component_key,v_row.residual_source_ex_vat,
       v_row.source_pay_channel,v_worker.target_pay_channel,
       v_row.valuation_policy_id,v_value.policy_window_id,
       v_value.target_ex_vat,v_value.target_vat,v_value.target_inc_vat,
       v_row.source_binding_version_id,v_row.canonical_source_work_id,v_row.canonical_source_component_key,
       v_row.source_book_revision,case when v_row.source_binding_version_id is null then null else true end)
      returning id into v_run_line_id;
    -- An authorised zero still has immutable shift/rate advice detail. It
    -- becomes a frozen zero line, never a zero-valued financial hold.
    if v_row.residual_source_ex_vat>0 then
      insert into private.bpay_next_hold
        (run_line_id,work_id,component_key,source_reserved_ex_vat,
         target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status)
        values(v_run_line_id,v_row.work_id,v_row.component_key,
         v_row.residual_source_ex_vat,v_value.target_ex_vat,
         v_value.target_vat,v_value.target_inc_vat,'ACTIVE');
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat+v_row.residual_source_ex_vat,
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

ALTER FUNCTION private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D04 successor of supabase/repeatable/03102026_2359_banking_pay_next_simple_review_confirm_v1.sql
create or replace function private.bpay_next_confirm_simple_run_v1(
  p_run_id uuid,p_review_revision bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_module_epoch bigint;
  v_command private.bpay_next_command%rowtype;
begin
  if p_run_id is null or p_review_revision is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CONFIRM_INPUT_INVALID';
  end if;
  select owner_epoch into v_module_epoch
    from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.status='DRAFT' and v_run.review_revision=p_review_revision
     and v_run.confirmed_at_utc is not null then
    return pg_catalog.jsonb_build_object('phase','DRAFT','replay',true,
      'review_revision',v_run.review_revision,
      'drafts',private.bpay_next_channel_drafts_page_v1(p_run_id)->'rows');
  end if;
  if v_run.status<>'REVIEW' or v_run.selection_state<>'SEALED'
     or v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count
     or v_run.review_revision<>p_review_revision
     or v_run.confirmed_at_utc is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_REVIEW_STATE_INVALID';
  end if;
  -- The real clock is read AFTER the header lock: a transaction started
  -- before the deadline cannot confirm late after waiting for that lock.
  -- The completed DRAFT replay above is permanent and never expires.
  if v_run.preparation_expires_at_utc is null
     or v_run.preparation_expires_at_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_PREPARATION_EXPIRED_OR_UNBOUND';
  end if;
  select c.* into strict v_command
    from private.bpay_next_run_command rc
    join private.bpay_next_command c on c.id=rc.command_id
    where rc.run_id=p_run_id;
  if v_command.status<>'SEALED' or v_command.module_epoch<>v_module_epoch
     or v_command.expected_member_count<>v_run.selected_candidate_count
     or v_command.enrolled_member_count<>v_run.selected_candidate_count
     or exists (select 1 from private.bpay_next_job j
        where j.command_id=v_command.id and j.job_kind='PREPARE'
          and j.status<>'DONE')
     or exists (
       select 1 from private.bpay_next_run_worker
       where run_id=p_run_id and status<>'READY') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_UNRESOLVED_WORKER';
  end if;
  -- Each bounded holder transaction wrote its line, position and ACTIVE hold
  -- atomically before its worker became READY. Future cancellation/expiry
  -- owners must lock this parent and make a worker non-ready before release;
  -- no such owner is granted or activated in this narrow proof.
  update private.bpay_next_pay_run
    set status='DRAFT',confirmed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_run_id;
  return pg_catalog.jsonb_build_object('phase','DRAFT','replay',false,
    'review_revision',p_review_revision,
    'drafts',private.bpay_next_channel_drafts_page_v1(p_run_id)->'rows');
end
$function$;

ALTER FUNCTION private.bpay_next_confirm_simple_run_v1(uuid,bigint) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_confirm_simple_run_v1(uuid,bigint) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D07 successor of supabase/repeatable/04102026_0120_banking_pay_next_simple_transfer_begin_v1.sql
create or replace function private.bpay_next_begin_simple_transfer_v1(
  p_command_id uuid,p_run_worker_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_sequence bigint;
  v_candidate_id uuid;
  v_case_selection_revision bigint;
begin
  if p_command_id is null or p_run_worker_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_TRANSFER_BEGIN_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select run_id,candidate_id,case_selection_revision
    into strict v_run_id,v_candidate_id,v_case_selection_revision from private.bpay_next_run_worker
    where id=p_run_worker_id;
  if v_case_selection_revision>0 then
    return private.bpay_next_begin_case_transfer_v1(p_command_id,p_run_worker_id);
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where id=p_run_worker_id for update;
  select * into v_transfer from private.bpay_next_transfer
    where build_command_id=p_command_id;
  if found then
    if v_transfer.run_worker_id<>p_run_worker_id
       or v_transfer.candidate_id<>v_worker.candidate_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BEGIN_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence
      from private.bpay_next_command
      where id=p_command_id and command_kind='TRANSFER_BUILD';
    return pg_catalog.jsonb_build_object('sequence',v_sequence,
      'transfer_id',v_transfer.id,'phase',v_transfer.status,'replay',true);
  end if;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or v_worker.status<>'READY'
     or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
     or (v_worker.target_pay_channel='PAYE' and
         (v_worker.net_projection_revision<>1 or v_worker.net_request_revision<>1))
     or (v_worker.target_pay_channel='UMBRELLA' and
         (v_worker.net_projection_revision<>0 or v_worker.net_request_revision<>0
          or v_worker.entered_paye_net is not null
          or v_worker.target_umbrella_id is null or v_worker.target_umbrella_vat_chargeable is null
          or v_worker.gross_inc_vat<=0 or v_worker.captured_line_count<1
          or v_worker.financial_resolution_count<>0 or v_worker.expected_managed_position_count<>0
          or exists(select 1 from private.bpay_next_paye_net_request where run_worker_id=v_worker.id)))
     or v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_transfer t
               where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SIMPLE_TRANSFER_NOT_ELIGIBLE';
  end if;
  if v_worker.target_pay_channel='UMBRELLA' then
    -- No payroll NET request/value, CASE adjustment or live financial lookup.
    -- Bounded member pages must certify every included line as same-channel
    -- TAXABLE WORK before the transfer can become issuable.
    insert into private.bpay_next_net_projection
      (run_worker_id,request_command_id,projection_no,input_kind,
       gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,cash_amount)
      values(v_worker.id,null,1,'UMBRELLA',v_worker.gross_ex_vat,v_worker.gross_vat,
        v_worker.gross_inc_vat,null,0,v_worker.gross_inc_vat);
    update private.bpay_next_run_worker set net_projection_revision=1 where id=v_worker.id;
  end if;
  select * into strict v_projection from private.bpay_next_net_projection
    where run_worker_id=v_worker.id and projection_no=1;
  if v_worker.target_pay_channel='PAYE' and (v_projection.input_kind<>'PAYE_MANUAL'
     or v_projection.gross_ex_vat<>v_worker.gross_ex_vat
     or v_projection.gross_vat<>0
     or v_projection.gross_inc_vat<>v_worker.gross_inc_vat
     or v_projection.entered_paye_net is distinct from
        v_worker.entered_paye_net
     or v_projection.accepted_recoveries<>0
     or v_projection.cash_amount<0
     or v_projection.cash_amount<>v_worker.entered_paye_net
     or v_projection.request_command_id is null) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SIMPLE_TRANSFER_PROJECTION_INVALID';
  end if;
  if v_worker.target_pay_channel='UMBRELLA' and
     ((v_projection.input_kind,v_projection.gross_ex_vat,v_projection.gross_vat,
       v_projection.gross_inc_vat,v_projection.cash_amount,v_projection.accepted_recoveries,
       v_projection.accepted_gross_additions,v_projection.accepted_gross_deductions,v_projection.accepted_net_additions)
       is distinct from ('UMBRELLA'::text,v_worker.gross_ex_vat,v_worker.gross_vat,
         v_worker.gross_inc_vat,v_worker.gross_inc_vat,0::numeric,0::numeric,0::numeric,0::numeric)
      or v_projection.entered_paye_net is not null or v_projection.request_command_id is not null) then
    raise exception using errcode='23514',message='BPAY_NEXT_SIMPLE_TRANSFER_PROJECTION_INVALID';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(
    p_command_id,'TRANSFER_BUILD');
  insert into private.bpay_next_command_member
    (command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_transfer
    (run_worker_id,candidate_id,projection_id,build_command_id,
     transfer_no,beneficiary_kind,beneficiary_id,cash_amount,status,execution_kind)
    values(v_worker.id,v_worker.candidate_id,v_projection.id,p_command_id,
      1,case when v_worker.target_pay_channel='UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end,
      case when v_worker.target_pay_channel='UMBRELLA' then v_worker.target_umbrella_id else v_worker.candidate_id end,
      v_projection.cash_amount,'BUILDING',
      case when v_projection.cash_amount=0 then 'INTERNAL_ZERO' else 'BANK' end)
    returning * into v_transfer;
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,
    'transfer_id',v_transfer.id,'phase','BUILDING','replay',false);
end
$function$;

ALTER FUNCTION private.bpay_next_begin_simple_transfer_v1(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_begin_simple_transfer_v1(uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D16 successor of supabase/repeatable/04102026_0130_banking_pay_next_simple_transfer_members_v1.sql
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
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  select * into strict v_transfer from private.bpay_next_transfer
    where build_command_id=v_job.command_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where id=v_transfer.run_worker_id for update;
  select * into strict v_projection from private.bpay_next_net_projection
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
     or v_worker.net_projection_revision<>1
     or v_projection.projection_no<>1
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
    select l.id,l.line_no,l.frozen_inc_vat,l.frozen_ex_vat,l.source_consumed_ex_vat,
      l.source_pay_channel,l.target_pay_channel,l.component_key,l.work_id,
      a.id as qualified_line_id,a.component_kind,a.tax_treatment,
      r.id as qualified_revision_id
    from private.bpay_next_run_line l
    left join private.bpay_next_approved_line a on a.id=l.approved_line_id
      and a.revision_id=l.captured_revision_id and a.component_key=l.component_key
    left join private.bpay_next_work_revision r on r.id=l.captured_revision_id and r.work_id=l.work_id
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
        or v_line.component_kind is distinct from 'WORK' or v_line.tax_treatment is distinct from 'TAXABLE'
        or v_line.source_pay_channel is distinct from 'UMBRELLA'
        or v_line.target_pay_channel is distinct from 'UMBRELLA'
        or v_line.source_consumed_ex_vat<0
        or v_line.frozen_ex_vat is distinct from v_line.source_consumed_ex_vat) then
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

ALTER FUNCTION private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D15 successor of supabase/repeatable/04102026_0140_banking_pay_next_simple_csv_destination_v1.sql
create or replace function private.bpay_next_bind_simple_csv_destination_v1(
  p_transfer_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_candidate public.candidates%rowtype;
  v_umbrella public.umbrellas%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_name text;
  v_sort text;
  v_account text;
  v_hash text;
  v_replay boolean;
begin
  if p_transfer_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CSV_DESTINATION_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run_id
    from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where t.id=p_transfer_id;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id
    where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=p_transfer_id for update;
  v_leg:=private.bpay_next_destination_execution_leg_v1(p_transfer_id);
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or (v_worker.status<>'READY' and not (v_worker.status='COMPLETE'
       and v_leg.transfer_id is not null and v_transfer.original_transfer_id is not null))
     or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
     or v_transfer.status<>'MEMBERS_READY'
     or v_transfer.candidate_id<>v_worker.candidate_id
     or not ((v_worker.target_pay_channel='PAYE' and
          v_transfer.beneficiary_kind='CANDIDATE' and v_transfer.beneficiary_id=v_worker.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_id is not null and
          v_transfer.beneficiary_kind='UMBRELLA' and v_transfer.beneficiary_id=v_worker.target_umbrella_id))
     or (v_transfer.projection_id is null and v_transfer.return_cash_id is null)
     or v_transfer.cash_amount<=0
     or v_transfer.member_count<=0
     or v_transfer.member_cash_sum<>v_transfer.cash_amount
     or (not exists(select 1 from private.bpay_next_command c
                   where c.id=v_transfer.build_command_id
                     and c.command_kind in ('TRANSFER_BUILD','CASH_REISSUE')
                     and c.status='COMPLETE')
       and not (v_leg.transfer_id is not null and v_transfer.original_transfer_id is null)) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_DESTINATION_NOT_ELIGIBLE';
  end if;
  select * into strict v_candidate from public.candidates
    where id=v_worker.candidate_id;
  v_name:=nullif(pg_catalog.btrim(v_candidate.account_holder),'');
  v_sort:=pg_catalog.regexp_replace(
    coalesce(v_candidate.sort_code,''),'[^0-9]','','g');
  v_account:=pg_catalog.regexp_replace(
    coalesce(v_candidate.account_number,''),'[^0-9]','','g');
  v_hash:=public._bank_hash(v_candidate.sort_code,
    v_candidate.account_number,v_candidate.account_holder);
  if v_worker.target_pay_channel='UMBRELLA' then
    if v_leg.transfer_id is not null or v_worker.case_selection_revision<>0
       or v_candidate.active is not true
       or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>'UMBRELLA'
       or v_candidate.umbrella_id is distinct from v_worker.target_umbrella_id then
      raise exception using errcode='55000',message='BPAY_NEXT_CSV_UMBRELLA_DESTINATION_NOT_READY';
    end if;
    select * into strict v_umbrella from public.umbrellas where id=v_worker.target_umbrella_id for share;
    v_name:=nullif(pg_catalog.btrim(v_umbrella.name),'');
    v_sort:=pg_catalog.regexp_replace(coalesce(v_umbrella.sort_code,''),'[^0-9]','','g');
    v_account:=pg_catalog.regexp_replace(coalesce(v_umbrella.account_number,''),'[^0-9]','','g');
    v_hash:=public._bank_hash(v_umbrella.sort_code,v_umbrella.account_number,v_umbrella.name);
    if v_umbrella.enabled is not true or v_hash is distinct from v_umbrella.bank_details_hash then
      raise exception using errcode='55000',message='BPAY_NEXT_CSV_UMBRELLA_DESTINATION_NOT_READY';
    end if;
  end if;
  if v_leg.leg_kind='ONEOFF' then
    select o.* into strict v_origin from private.bpay_next_net_destination_amount d
      join private.bpay_next_stored_credit_origin o on o.command_id=d.first_origin_command_id
      where d.destination_id=v_leg.destination_id;
    v_name:=nullif(pg_catalog.btrim(v_origin.beneficiary_name),'');
    v_sort:=v_origin.sort_code;v_account:=v_origin.account_number;v_hash:=v_origin.bank_details_hash;
  end if;
  if v_candidate.active is not true
     or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>v_worker.target_pay_channel
     or v_name is null
     or v_sort !~ '^[0-9]{6}$'
     or v_account !~ '^[0-9]{8}$'
     or v_hash is null
     or (v_worker.target_pay_channel='PAYE' and
         v_leg.leg_kind is distinct from 'ONEOFF' and v_hash is distinct from v_candidate.bank_details_hash) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_BANK_DETAILS_NOT_READY';
  end if;
  v_replay:=coalesce(v_transfer.destination_rail='CSV'
    and v_transfer.beneficiary_name_snapshot=v_name
    and v_transfer.sort_code_snapshot=v_sort
    and v_transfer.account_number_snapshot=v_account
    and v_transfer.bank_details_hash_snapshot=v_hash,false);
  if not v_replay then
    update private.bpay_next_transfer
      set destination_rail='CSV',beneficiary_name_snapshot=v_name,
          sort_code_snapshot=v_sort,account_number_snapshot=v_account,
          bank_details_hash_snapshot=v_hash
      where id=p_transfer_id;
  end if;
  return pg_catalog.jsonb_build_object('transfer_id',p_transfer_id,
    'phase','MEMBERS_READY','destination_rail','CSV',
    'bank_hash',v_hash,'replay',v_replay,
    'instruction_issued',false);
end
$function$;

ALTER FUNCTION private.bpay_next_bind_simple_csv_destination_v1(uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_bind_simple_csv_destination_v1(uuid) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D14 successor of supabase/repeatable/04102026_0150_banking_pay_next_simple_csv_issue_v1.sql
create or replace function private.bpay_next_issue_simple_csv_v1(
  p_instruction_id uuid,p_transfer_id uuid,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_candidate public.candidates%rowtype;
  v_umbrella public.umbrellas%rowtype;
  v_actor public.tms_users%rowtype;
  v_existing private.bpay_next_csv_instruction%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_columns jsonb;
  v_options jsonb;
  v_with_account_type boolean;
  v_name text;
  v_sort text;
  v_account text;
  v_hash text;
  v_reference text;
  v_file_name text;
  v_csv text;
  v_sha text;
begin
  if p_instruction_id is null or p_transfer_id is null
     or p_actor_user_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CSV_ISSUE_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run_id
    from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where t.id=p_transfer_id;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id
    where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=p_transfer_id for update;
  select * into strict v_actor from public.tms_users
    where id=p_actor_user_id for share;
  if v_actor.is_active is not true
     or (v_actor.payment_authoriser is not true
         and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',
      message='BPAY_NEXT_CSV_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_existing from private.bpay_next_csv_instruction
    where id=p_instruction_id;
  if found then
    if v_existing.transfer_id<>p_transfer_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_CSV_ISSUE_REPLAY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object(
      'instruction_id',v_existing.id,'transfer_id',p_transfer_id,
      'file_name',v_existing.file_name,'csv_sha256',v_existing.csv_sha256,
      'csv_text',v_existing.csv_text,'replay',true,
      'payment_recorded',exists(select 1 from private.bpay_next_outcome_request r
        where r.transfer_id=p_transfer_id and r.posting_complete));
  end if;
  if exists(select 1 from private.bpay_next_csv_instruction i
            where i.transfer_id=p_transfer_id) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_CSV_TRANSFER_ALREADY_ISSUED';
  end if;
  -- Exact issued receipt readback above precedes current bank safety checks.
  v_leg:=private.bpay_next_destination_execution_leg_v1(p_transfer_id);
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or (v_worker.status<>'READY' and not (v_worker.status='COMPLETE'
       and v_leg.transfer_id is not null and v_transfer.original_transfer_id is not null))
     or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
     or v_transfer.status<>'MEMBERS_READY'
     or v_transfer.candidate_id<>v_worker.candidate_id
     or not ((v_worker.target_pay_channel='PAYE' and
          v_transfer.beneficiary_kind='CANDIDATE' and v_transfer.beneficiary_id=v_worker.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_id is not null and
          v_transfer.beneficiary_kind='UMBRELLA' and v_transfer.beneficiary_id=v_worker.target_umbrella_id))
     or v_transfer.destination_rail<>'CSV'
     or v_transfer.account_approval_ref is not null
     or v_transfer.member_count<=0
     or v_transfer.member_cash_sum<>v_transfer.cash_amount
     or v_transfer.cash_amount<=0 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_ISSUE_NOT_ELIGIBLE';
  end if;
  -- The Candidate row lock makes the actual issue and a simultaneous bank
  -- change resolve in one order. No historical account or Timesheet scan.
  select * into strict v_candidate from public.candidates
    where id=v_worker.candidate_id for share;
  v_name:=nullif(pg_catalog.btrim(v_candidate.account_holder),'');
  v_sort:=pg_catalog.regexp_replace(
    coalesce(v_candidate.sort_code,''),'[^0-9]','','g');
  v_account:=pg_catalog.regexp_replace(
    coalesce(v_candidate.account_number,''),'[^0-9]','','g');
  v_hash:=public._bank_hash(v_candidate.sort_code,
    v_candidate.account_number,v_candidate.account_holder);
  if v_worker.target_pay_channel='UMBRELLA' then
    if v_leg.transfer_id is not null or v_worker.case_selection_revision<>0
       or v_candidate.active is not true
       or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>'UMBRELLA'
       or v_candidate.umbrella_id is distinct from v_worker.target_umbrella_id then
      raise exception using errcode='55000',message='BPAY_NEXT_CSV_UMBRELLA_DESTINATION_NOT_READY';
    end if;
    select * into strict v_umbrella from public.umbrellas where id=v_worker.target_umbrella_id for share;
    v_name:=nullif(pg_catalog.btrim(v_umbrella.name),'');
    v_sort:=pg_catalog.regexp_replace(coalesce(v_umbrella.sort_code,''),'[^0-9]','','g');
    v_account:=pg_catalog.regexp_replace(coalesce(v_umbrella.account_number,''),'[^0-9]','','g');
    v_hash:=public._bank_hash(v_umbrella.sort_code,v_umbrella.account_number,v_umbrella.name);
    if v_umbrella.enabled is not true or v_hash is distinct from v_umbrella.bank_details_hash then
      raise exception using errcode='55000',message='BPAY_NEXT_CSV_UMBRELLA_DESTINATION_NOT_READY';
    end if;
  end if;
  if v_leg.leg_kind='ONEOFF' then
    select o.* into strict v_origin from private.bpay_next_net_destination_amount d
      join private.bpay_next_stored_credit_origin o on o.command_id=d.first_origin_command_id
      where d.destination_id=v_leg.destination_id;
    v_name:=nullif(pg_catalog.btrim(v_origin.beneficiary_name),'');
    v_sort:=v_origin.sort_code;v_account:=v_origin.account_number;v_hash:=v_origin.bank_details_hash;
  end if;
  if v_candidate.active is not true
     or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>v_worker.target_pay_channel
     or v_name is null
     or pg_catalog.octet_length(v_name)>70
     or position(E'\r' in v_name)>0
     or position(E'\n' in v_name)>0
     or v_sort !~ '^[0-9]{6}$'
     or v_account !~ '^[0-9]{8}$'
     or v_hash is null
     or (v_worker.target_pay_channel='PAYE' and
         v_leg.leg_kind is distinct from 'ONEOFF' and v_hash is distinct from v_candidate.bank_details_hash)
     or v_name is distinct from v_transfer.beneficiary_name_snapshot
     or v_sort is distinct from v_transfer.sort_code_snapshot
     or v_account is distinct from v_transfer.account_number_snapshot
     or v_hash is distinct from v_transfer.bank_details_hash_snapshot then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_DESTINATION_CHANGED_BEFORE_ISSUE';
  end if;
  select sd.pay_export_csv_columns_json,
         coalesce(sd.pay_export_csv_format_json,'{}'::jsonb)
    into strict v_columns,v_options
    from public.settings_defaults sd where sd.id=1;
  if v_options<>'{}'::jsonb then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_CONFIGURED_FORMAT_NOT_YET_SUPPORTED';
  end if;
  v_with_account_type:=coalesce(v_columns=
    '["payment_reference","payee_name","sort_code","account_number","account_type","amount"]'::jsonb,false);
  if not v_with_account_type and v_columns is distinct from
      '["payment_reference","payee_name","sort_code","account_number","amount"]'::jsonb then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_CONFIGURED_COLUMNS_NOT_YET_SUPPORTED';
  end if;
  v_reference:='BP'||substring(
    pg_catalog.replace(p_transfer_id::text,'-','') from 1 for 18);
  v_file_name:='bpay-next-'||p_instruction_id::text||'.csv';
  v_csv:='Payment reference,Payee name,Sort code,Bank account number,';
  if v_with_account_type then
    v_csv:=v_csv||'Bank account type,';
  end if;
  v_csv:=v_csv||'Amount'||E'\n'||v_reference||',"'||
    pg_catalog.replace(v_name,'"','""')||'",'||v_sort||','||v_account||',';
  if v_with_account_type then
    v_csv:=v_csv||',';
  end if;
  v_csv:=v_csv||pg_catalog.to_char(v_transfer.cash_amount,
    'FM9999999999999990.00')||E'\n';
  v_sha:=pg_catalog.encode(pg_catalog.sha256(
    pg_catalog.convert_to(v_csv,'UTF8')),'hex');
  insert into private.bpay_next_csv_instruction
    (id,transfer_id,run_worker_id,actor_user_id,payment_reference,
     bank_details_hash_snapshot,cash_amount,file_name,csv_text,csv_sha256)
    values(p_instruction_id,p_transfer_id,v_worker.id,p_actor_user_id,
      v_reference,v_hash,v_transfer.cash_amount,v_file_name,v_csv,v_sha);
  update private.bpay_next_transfer
    set account_approval_ref='CSV_INSTRUCTION:'||p_instruction_id::text,
        status='ISSUED_CSV'
    where id=p_transfer_id;
  return pg_catalog.jsonb_build_object(
    'instruction_id',p_instruction_id,'transfer_id',p_transfer_id,
    'file_name',v_file_name,'csv_sha256',v_sha,'csv_text',v_csv,
    'replay',false,'payment_recorded',false);
end
$function$;

ALTER FUNCTION private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D13 successor of supabase/repeatable/04102026_0200_banking_pay_next_simple_csv_settlement_v1.sql
create or replace function private.bpay_next_receive_simple_csv_settlement_v1(
  p_command_id uuid,p_transfer_id uuid,p_receipt_id text,
  p_amount numeric,p_occurred_at timestamptz,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_candidate_id uuid;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_actor public.tms_users%rowtype;
  v_receipt private.bpay_next_transfer_outcome%rowtype;
  v_request private.bpay_next_outcome_request%rowtype;
  v_sequence bigint;
begin
  if p_command_id is null or p_transfer_id is null or p_receipt_id is null
     or pg_catalog.octet_length(p_receipt_id) not between 1 and 256
     or p_amount is null or p_amount<=0
     or p_amount<>pg_catalog.round(p_amount,2)
     or p_occurred_at is null or not pg_catalog.isfinite(p_occurred_at)
     or p_actor_user_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_SETTLEMENT_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
                where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate_id
    from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where t.id=p_transfer_id;
  perform 1 from private.bpay_next_pay_run where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id
    where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=p_transfer_id for update;
  select * into strict v_actor from public.tms_users
    where id=p_actor_user_id for share;
  if v_actor.is_active is not true
     or (v_actor.payment_authoriser is not true
         and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',message='BPAY_NEXT_SETTLEMENT_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_receipt from private.bpay_next_transfer_outcome
    where receipt_id=p_receipt_id;
  if found then
    if v_receipt.transfer_id<>p_transfer_id or v_receipt.outcome_kind<>'SETTLED'
       or v_receipt.whole_transfer_amount<>p_amount
       or v_receipt.occurred_at_utc<>p_occurred_at then
      raise exception using errcode='23514',message='BPAY_NEXT_SETTLEMENT_RECEIPT_CONFLICT';
    end if;
    select * into strict v_request from private.bpay_next_outcome_request
      where outcome_id=v_receipt.id;
    return pg_catalog.jsonb_build_object('command_id',v_request.command_id,
      'outcome_id',v_receipt.id,'replay',true,
      'posting_complete',v_request.posting_complete);
  end if;
  if exists(select 1 from private.bpay_next_outcome_request
            where command_id=p_command_id or transfer_id=p_transfer_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_SETTLEMENT_COMMAND_CONFLICT';
  end if;
  if v_transfer.status not in ('ISSUED_CSV','UNKNOWN')
     or v_transfer.destination_rail<>'CSV'
     or v_transfer.cash_amount<>p_amount
     or not exists(select 1 from private.bpay_next_csv_instruction i
                   where i.transfer_id=p_transfer_id and i.cash_amount=p_amount)
     or not (v_worker.target_pay_channel='PAYE' or
       (v_worker.target_pay_channel='UMBRELLA' and v_worker.case_selection_revision=0
        and v_worker.target_umbrella_id is not null
        and v_transfer.beneficiary_kind='UMBRELLA'
        and v_transfer.beneficiary_id=v_worker.target_umbrella_id
        and exists(select 1 from private.bpay_next_net_projection p
          where p.id=coalesce(v_transfer.projection_id,
            (select projection_id from private.bpay_next_transfer where id=v_transfer.original_transfer_id))
            and p.run_worker_id=v_worker.id and p.input_kind='UMBRELLA'
            and p.entered_paye_net is null and p.request_command_id is null
            and p.cash_amount=v_worker.gross_inc_vat))) then
    raise exception using errcode='55000',message='BPAY_NEXT_SETTLEMENT_NOT_ELIGIBLE';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CSV_SETTLEMENT');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_transfer_outcome
    (transfer_id,receipt_id,outcome_kind,whole_transfer_amount,occurred_at_utc)
    values(p_transfer_id,p_receipt_id,'SETTLED',p_amount,p_occurred_at)
    returning * into v_receipt;
  insert into private.bpay_next_outcome_request
    (command_id,outcome_id,transfer_id,run_worker_id,candidate_id,actor_user_id)
    values(p_command_id,v_receipt.id,p_transfer_id,v_worker.id,
      v_worker.candidate_id,p_actor_user_id);
  update private.bpay_next_transfer set status='SETTLED' where id=p_transfer_id;
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,
    'outcome_id',v_receipt.id,'sequence',v_sequence,'replay',false,'posting_complete',false);
end
$function$;

ALTER FUNCTION private.bpay_next_receive_simple_csv_settlement_v1(uuid,uuid,text,numeric,timestamp with time zone,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_receive_simple_csv_settlement_v1(uuid,uuid,text,numeric,timestamp with time zone,uuid) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D13 successor of supabase/repeatable/04102026_0200_banking_pay_next_simple_csv_settlement_v1.sql
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
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_outcome_request
    where command_id=v_job.command_id for update;
  perform 1 from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_transfer from private.bpay_next_transfer
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
    select * into strict v_internal from private.bpay_next_internal_receipt where id=v_request.internal_receipt_id;
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
    select * into strict v_return_cash from private.bpay_next_return_cash
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
      select * into strict v_line from private.bpay_next_run_line where id=v_member.run_line_id;
      if v_line.run_worker_id<>v_request.run_worker_id
         or v_line.target_pay_channel not in ('PAYE','UMBRELLA')
         or v_line.frozen_inc_vat<>v_member.signed_cash_contribution then
        raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_FROZEN_MEMBER_MISMATCH';
      end if;
      if v_line.target_pay_channel='UMBRELLA' and
         (v_line.source_pay_channel is distinct from 'UMBRELLA'
          or v_line.frozen_ex_vat is distinct from v_line.source_consumed_ex_vat
          or not exists(select 1 from private.bpay_next_approved_line a
            where a.id=v_line.approved_line_id and a.revision_id=v_line.captured_revision_id
              and a.component_key=v_line.component_key and a.component_kind='WORK' and a.tax_treatment='TAXABLE')) then
        raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
      end if;
      if v_line.source_consumed_ex_vat>0 then
        select * into strict v_hold from private.bpay_next_hold where run_line_id=v_line.id for update;
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

ALTER FUNCTION private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D12 successor of supabase/repeatable/04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql
create or replace function private.bpay_next_receive_simple_reissue_v1(
  p_command_id uuid,p_return_cash_id uuid,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run uuid;
  v_original private.bpay_next_transfer%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_actor public.tms_users%rowtype;
  v_request private.bpay_next_reissue_request%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;
  v_sequence bigint;
begin
  if p_command_id is null or p_return_cash_id is null or p_actor_user_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_REISSUE_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run from private.bpay_next_return_cash c
    join private.bpay_next_transfer t on t.id=c.original_transfer_id
    join private.bpay_next_run_worker w on w.id=t.run_worker_id where c.id=p_return_cash_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  select t.* into strict v_original from private.bpay_next_return_cash c
    join private.bpay_next_transfer t on t.id=c.original_transfer_id where c.id=p_return_cash_id;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_original.run_worker_id for update;
  select * into strict v_actor from public.tms_users where id=p_actor_user_id for share;
  if v_actor.is_active is not true or
     (v_actor.payment_authoriser is not true and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',message='BPAY_NEXT_REISSUE_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_request from private.bpay_next_reissue_request where command_id=p_command_id;
  if found then
    if v_request.return_cash_id<>p_return_cash_id then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_REPLAY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'transfer_id',v_request.transfer_id,'replay',true);
  end if;
  if v_original.status<>'RETURNED'
     or not ((v_original.beneficiary_kind='CANDIDATE' and v_original.beneficiary_id=v_original.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.case_selection_revision=0
         and v_worker.target_umbrella_id is not null and v_original.beneficiary_kind='UMBRELLA'
         and v_original.beneficiary_id=v_worker.target_umbrella_id))
     or not exists(select 1 from private.bpay_next_return_request r
                   where r.transfer_id=v_original.id and r.posting_complete) then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_RETURN_NOT_COMPLETE';
  end if;
  -- Reject duplicate NEW requests before receiving a command. Otherwise an
  -- unfulfillable earlier job could block the actual settlement/return.
  -- Run lock serialises all supported cash writers; the pending index also
  -- prevents two accepted requests before either one builds its transfer.
  select * into strict v_cash from private.bpay_next_return_cash where id=p_return_cash_id for update;
  if v_cash.amount_owed-v_cash.amount_held-v_cash.amount_reissued_paid<=0
     or exists(select 1 from private.bpay_next_reissue_request
               where return_cash_id=p_return_cash_id and transfer_id is null) then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_ALREADY_REQUESTED_OR_RESERVED';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CASH_REISSUE');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_original.candidate_id,1);
  insert into private.bpay_next_reissue_request(command_id,return_cash_id,run_worker_id,candidate_id,actor_user_id)
    values(p_command_id,p_return_cash_id,v_original.run_worker_id,v_original.candidate_id,p_actor_user_id);
  update private.bpay_next_command set status='SEALED',expected_member_count=1,
    sealed_at_utc=pg_catalog.transaction_timestamp() where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'sequence',v_sequence,'replay',false);
end
$function$;

ALTER FUNCTION private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D11 successor of supabase/repeatable/04102026_0255_banking_pay_next_original_csv_advice_page_v1.sql
create or replace function public.bpay_next_original_csv_advice_page_v1(
  p_actor_user_id uuid,p_instruction_id uuid,p_kind text,
  p_run_work_id uuid default null,p_run_line_id uuid default null,p_shift_detail_id uuid default null,
  p_after_work_id uuid default null,p_after_member_no bigint default null,
  p_after_detail_no integer default null,p_after_break_no integer default null,
  p_after_bucket text default null,p_after_rate_family text default null,p_after_rate_code text default null,
  p_limit integer default 50
) returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set timezone='UTC'
as $function$
declare
  v_instruction private.bpay_next_csv_instruction%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_group private.bpay_next_destination_group%rowtype;
  v_net private.bpay_next_net_destination_state%rowtype;
  v_destination private.bpay_next_net_destination_amount%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_destination_kind text:='OWN';
  v_approved_line uuid;
  v_revision uuid;
  v_sql text;
  v_row record;
  v_rows jsonb:='[]'::jsonb;
  v_bytes integer:=512;
  v_count integer:=0;
  v_more boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or not exists(select 1 from public.tms_users u where u.id=p_actor_user_id
      and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_ADVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_instruction_id is null or p_kind is null
    or p_kind not in ('HEADER','WORKS','MEMBERS','SHIFTS','BREAKS','RATE_DETAILS','RATE_SCHEDULE')
    or p_limit is null or p_limit not between 1 and 100
    or (p_after_member_no is not null and p_after_member_no<0)
    or (p_after_detail_no is not null and p_after_detail_no<0)
    or (p_after_break_no is not null and p_after_break_no<0)
    or (p_after_bucket is not null and p_after_bucket not in ('BH','DAY','NIGHT','SAT','SUN'))
    or ((p_after_rate_family is null)<>(p_after_rate_code is null))
    or (p_after_rate_family is not null and (p_after_rate_family not in ('ADDITIONAL','MILEAGE','STANDARD')
      or char_length(p_after_rate_code) not between 1 and 128)) then
    raise exception using errcode='22023',message='BPAY_NEXT_ADVICE_REQUEST_INVALID';
  end if;
  -- Closed shape: no cursor or child ID is silently ignored by a different kind.
  if (p_run_work_id is not null and p_kind<>'RATE_SCHEDULE')
    or (p_run_line_id is not null and p_kind not in ('SHIFTS','BREAKS','RATE_DETAILS'))
    or (p_shift_detail_id is not null and p_kind<>'BREAKS')
    or (p_after_work_id is not null and p_kind<>'WORKS')
    or (p_after_member_no is not null and p_kind<>'MEMBERS')
    or (p_after_detail_no is not null and p_kind<>'SHIFTS')
    or (p_after_break_no is not null and p_kind<>'BREAKS')
    or (p_after_bucket is not null and p_kind<>'RATE_DETAILS')
    or (p_after_rate_family is not null and p_kind<>'RATE_SCHEDULE')
    or (p_kind='RATE_SCHEDULE' and p_run_work_id is null)
    or (p_kind in ('SHIFTS','BREAKS','RATE_DETAILS') and p_run_line_id is null)
    or (p_kind='BREAKS' and p_shift_detail_id is null) then
    raise exception using errcode='22023',message='BPAY_NEXT_ADVICE_REQUEST_INVALID';
  end if;
  select * into v_instruction from private.bpay_next_csv_instruction where id=p_instruction_id;
  if not found then
    raise exception using errcode='P0002',message='BPAY_NEXT_ADVICE_INSTRUCTION_NOT_FOUND';
  end if;
  select * into strict v_transfer from private.bpay_next_transfer where id=v_instruction.transfer_id;
  if v_transfer.original_transfer_id is not null or v_transfer.projection_id is null then
    raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_ORIGINAL_CSV_REQUIRED';
  end if;
  select * into strict v_projection from private.bpay_next_net_projection where id=v_transfer.projection_id;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_instruction.run_worker_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_worker.run_id;
  if v_transfer.original_transfer_id is not null or v_transfer.execution_kind<>'BANK'
    or not ((v_worker.target_pay_channel='PAYE' and v_transfer.beneficiary_kind='CANDIDATE')
      or (v_worker.target_pay_channel='UMBRELLA' and v_transfer.beneficiary_kind='UMBRELLA'
        and v_worker.target_umbrella_id is not null and v_projection.input_kind='UMBRELLA'
        and v_worker.case_selection_revision=0))
    or v_transfer.run_worker_id<>v_worker.id or v_transfer.candidate_id<>v_worker.candidate_id
    or v_projection.run_worker_id<>v_worker.id
    or v_transfer.beneficiary_id is distinct from
      (case when v_worker.target_pay_channel='UMBRELLA' then v_worker.target_umbrella_id else v_worker.candidate_id end)
    or v_transfer.return_cash_id is not null
    or v_instruction.cash_amount<>v_transfer.cash_amount
    or v_instruction.bank_details_hash_snapshot is distinct from v_transfer.bank_details_hash_snapshot
    or v_transfer.member_count<1 or v_transfer.member_cash_sum<>v_transfer.cash_amount
    or (v_worker.target_pay_channel='PAYE' and v_projection.input_kind not in ('PAYE_MANUAL','PAYE_IMPORT','CASE_PAYOUT'))
    or (v_worker.target_pay_channel='UMBRELLA' and
      (v_projection.entered_paye_net is not null or v_projection.request_command_id is not null
       or v_projection.cash_amount is distinct from v_projection.gross_inc_vat)) then
    raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_ORIGINAL_CSV_REQUIRED';
  end if;
  -- A frozen leg may be smaller than its accepted Candidate projection. This
  -- is a point read of immutable sealed facts, NOT the execution qualifier:
  -- that qualifier checks today's bank version and must not invalidate advice
  -- for an already-issued original instruction after factual account changes.
  select * into v_leg from private.bpay_next_destination_group_leg where transfer_id=v_transfer.id;
  if not found then
    if v_projection.cash_amount<>v_transfer.cash_amount
      or exists(select 1 from private.bpay_next_destination_group where run_worker_id=v_worker.id) then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_FROZEN_DESTINATION_INVALID';end if;
  else
    select * into strict v_group from private.bpay_next_destination_group where anchor_transfer_id=v_leg.anchor_transfer_id;
    select * into strict v_net from private.bpay_next_net_destination_state where state_id=v_group.net_state_id;
    if v_group.stage<>'COMPLETE' or v_group.completed_at_utc is null
      or v_group.run_worker_id<>v_worker.id or v_group.candidate_id<>v_worker.candidate_id
      or v_group.projection_id<>v_projection.id or v_net.run_worker_id<>v_worker.id
      or v_net.projection_id is distinct from v_projection.id or v_net.completed_at_utc is null
      or v_group.processed_work_count<>v_group.expected_work_count or v_group.processed_case_count<>v_group.expected_case_count
      or v_group.created_leg_count<>v_group.expected_leg_count or v_group.sealed_leg_count<>v_group.expected_leg_count
      or v_group.expected_leg_count<>v_net.external_leg_count+1
      or v_group.member_cash_total<>v_projection.cash_amount or v_group.sealed_cash_total<>v_projection.cash_amount
      or v_net.cash_amount is distinct from v_projection.cash_amount
      or v_net.own_amount+v_net.external_amount is distinct from v_projection.cash_amount then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_FROZEN_DESTINATION_INVALID';end if;
    v_destination_kind:=v_leg.leg_kind;
    if v_leg.leg_kind='OWN' then
      if v_transfer.id<>v_group.anchor_transfer_id or v_leg.destination_id is not null
        or v_transfer.cash_amount is distinct from v_net.own_amount then
        raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_FROZEN_DESTINATION_INVALID';end if;
    else
      select * into strict v_destination from private.bpay_next_net_destination_amount where destination_id=v_leg.destination_id;
      select * into strict v_origin from private.bpay_next_stored_credit_origin where command_id=v_destination.first_origin_command_id;
      if v_destination.state_id<>v_net.state_id or v_destination.amount<>v_transfer.cash_amount
        or v_origin.candidate_id<>v_worker.candidate_id or v_destination.bank_details_hash<>v_origin.bank_details_hash
        or v_origin.original_source_pay_channel<>'UMBRELLA' or v_origin.original_tax_treatment<>'NON_TAXABLE'
        or v_origin.original_routing_kind<>'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
        or (v_transfer.bank_details_hash_snapshot,v_transfer.beneficiary_name_snapshot,
            v_transfer.sort_code_snapshot,v_transfer.account_number_snapshot)
          is distinct from (v_origin.bank_details_hash,pg_catalog.btrim(v_origin.beneficiary_name),
            v_origin.sort_code,v_origin.account_number) then
        raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_FROZEN_DESTINATION_INVALID';end if;
    end if;
  end if;
  -- CSV issuance is the immutable read gate. Current worker/transfer/outcome
  -- status and retirement of this NET projection cannot re-price this issue.
  if p_kind='HEADER' then
    v_rows:=jsonb_build_array(jsonb_build_object(
      'instruction_id',v_instruction.id,'transfer_id',v_transfer.id,'projection_id',v_projection.id,
      'projection_no',v_projection.projection_no::text,'input_kind',v_projection.input_kind,
      'run_id',v_run.id,'run_worker_id',v_worker.id,'candidate_id',v_worker.candidate_id,
      'pay_date',v_run.pay_date,'target_pay_channel',v_worker.target_pay_channel,
      'beneficiary_name_snapshot',v_transfer.beneficiary_name_snapshot,
      'payment_reference',v_instruction.payment_reference,'issued_at_utc',v_instruction.issued_at_utc,
      'member_count',v_transfer.member_count::text,
      'gross_ex_vat',v_projection.gross_ex_vat::text,'gross_vat',v_projection.gross_vat::text,
      'gross_inc_vat',v_projection.gross_inc_vat::text,'entered_paye_net',v_projection.entered_paye_net::text,
      'accepted_gross_additions',v_projection.accepted_gross_additions::text,
      'accepted_gross_deductions',v_projection.accepted_gross_deductions::text,
      'accepted_net_additions',v_projection.accepted_net_additions::text,
      'accepted_recoveries',v_projection.accepted_recoveries::text,'cash_amount',v_instruction.cash_amount::text,
      'projection_cash_amount',v_projection.cash_amount::text,'destination_kind',v_destination_kind));
    if octet_length(v_rows::text)>120000 then
      raise exception using errcode='54000',message='BPAY_NEXT_ADVICE_ROW_TOO_LARGE';
    end if;
    return jsonb_build_object('rows',v_rows,'complete',true);
  end if;
  if p_kind in ('SHIFTS','BREAKS','RATE_DETAILS') then
    select l.approved_line_id into v_approved_line
    from private.bpay_next_transfer_member m join private.bpay_next_run_line l on l.id=m.run_line_id
    where m.transfer_id=v_transfer.id and m.subject_kind='WORK' and l.id=p_run_line_id
      and l.run_worker_id=v_worker.id;
    if not found then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_LINE_NOT_IN_INSTRUCTION';
    end if;
  elsif p_kind='RATE_SCHEDULE' then
    select rw.captured_revision_id into v_revision from private.bpay_next_run_work rw
    where rw.id=p_run_work_id and rw.run_worker_id=v_worker.id and exists(
      select 1 from private.bpay_next_run_line l join private.bpay_next_transfer_member m on m.run_line_id=l.id
      where l.run_work_id=rw.id and m.transfer_id=v_transfer.id and m.subject_kind='WORK');
    if not found then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_WORK_NOT_IN_INSTRUCTION';
    end if;
  end if;
  if p_kind='WORKS' then
    v_sql:=$query$select private.bpay_next_advice_work_v1(rw.id) row_value
      from private.bpay_next_run_work rw where rw.run_worker_id=$2
        and ($6::uuid is null or rw.work_id>$6) and exists(
          select 1 from private.bpay_next_run_line l join private.bpay_next_transfer_member m on m.run_line_id=l.id
          where l.run_work_id=rw.id and m.transfer_id=$1 and m.subject_kind='WORK')
      order by rw.work_id limit $12$query$;
  elsif p_kind='MEMBERS' then
    v_sql:=$query$select jsonb_build_object(
      'member_no',m.member_no::text,'subject_kind',m.subject_kind,'signed_cash_contribution',m.signed_cash_contribution::text,
      'work',case when m.subject_kind='WORK' then private.bpay_next_advice_work_v1(l.run_work_id) end,
      'line',case when m.subject_kind='WORK' then jsonb_build_object(
        'run_line_id',l.id,'run_work_id',l.run_work_id,'captured_revision_id',l.captured_revision_id,
        'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',a.component_kind,
        'work_date',a.work_date,'unit_label',a.unit_label,'approved_quantity',a.approved_quantity::text,
        'approved_unit_rate',a.approved_unit_rate::text,'expected_rate_detail_count',a.expected_rate_detail_count,
        'evidence_ref',a.evidence_ref,'approved_source_ex_vat',a.source_pay_ex_vat::text,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,
        'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text) end,
      'case',case when m.subject_kind='CASE' then jsonb_build_object(
        'case_instruction_id',ci.id,'allocation_result_id',cr.id,'case_id',ci.case_id,
        'case_kind',ci.case_kind,'case_subtype',ci.case_subtype,'tax_treatment',ci.tax_treatment,
        'instruction_kind',ci.instruction_kind,'direction',ci.direction,'payroll_stage',ci.payroll_stage,
        'resolution_ref',ci.resolution_ref,'explanation',ci.explanation,
        'allocated_target_ex_vat',cr.allocated_target_ex_vat::text,'allocated_target_vat',cr.allocated_target_vat::text,
        'allocated_target_inc_vat',cr.allocated_target_inc_vat::text) end) row_value
      from private.bpay_next_transfer_member m
      left join private.bpay_next_run_line l on l.id=m.run_line_id and l.run_worker_id=m.run_worker_id
      left join private.bpay_next_approved_line a on a.id=l.approved_line_id and a.revision_id=l.captured_revision_id
      left join private.bpay_next_run_case_instruction ci on ci.id=m.case_instruction_id and ci.run_worker_id=m.run_worker_id
      left join private.bpay_next_case_allocation_result cr on cr.id=m.case_allocation_result_id and cr.instruction_id=ci.id
      where m.transfer_id=$1 and m.member_no>coalesce($7::bigint,0)
      order by m.member_no limit $12$query$;
  elsif p_kind='SHIFTS' then
    v_sql:=$query$select to_jsonb(page) row_value from (
      select s.id shift_detail_id,s.detail_no,s.work_date,s.shift_start_at,s.shift_end_at,s.shift_start_local,
        s.shift_end_local,s.shift_overnight,s.submitted_minutes,s.approved_minutes,s.approved_hours::text,s.detail_label,
        s.segment_pay_ex_vat::text,s.pay_excluded,s.hours_day::text,s.hours_night::text,s.hours_sat::text,s.hours_sun::text,s.hours_bh::text
      from private.bpay_next_shift_detail s where s.approved_line_id=$4 and s.detail_no>coalesce($8::integer,0)
      order by s.detail_no limit $12) page$query$;
  elsif p_kind='BREAKS' then
    if not exists(select 1 from private.bpay_next_shift_detail where id=p_shift_detail_id and approved_line_id=v_approved_line) then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_BREAK_NOT_IN_LINE';
    end if;
    v_sql:=$query$select to_jsonb(page) row_value from (
      select b.id break_detail_id,b.break_no,b.break_start_at,b.break_end_at,b.break_start_local,b.break_end_local,b.break_minutes
      from private.bpay_next_break_detail b where b.shift_detail_id=$5 and b.break_no>coalesce($8::integer,0)
      order by b.break_no limit $12) page$query$;
  elsif p_kind='RATE_DETAILS' then
    v_sql:=$query$select to_jsonb(page) row_value from (
      select d.id rate_detail_id,d.bucket,d.approved_hours::text,d.source_pay_rate::text
      from private.bpay_next_rate_detail d where d.approved_line_id=$4 and ($9::text is null or d.bucket>$9)
      order by d.bucket limit $12) page$query$;
  else
    v_sql:=$query$select to_jsonb(page) row_value from (
      select s.id rate_schedule_id,s.rate_family,s.rate_code,s.unit_label,s.paye_rate::text,s.umbrella_rate::text,s.charge_rate::text
      from private.bpay_next_rate_schedule s where s.revision_id=$3
        and ($10::text is null or (s.rate_family,s.rate_code)>($10,$11))
      order by s.rate_family,s.rate_code limit $12) page$query$;
  end if;
  for v_row in execute v_sql using v_transfer.id,v_worker.id,v_revision,v_approved_line,p_shift_detail_id,
    p_after_work_id,p_after_member_no,coalesce(p_after_detail_no,p_after_break_no),p_after_bucket,p_after_rate_family,p_after_rate_code,p_limit
  loop
    if v_bytes+octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_ADVICE_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;

ALTER FUNCTION public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer) TO service_role;

-- F-05-D10 successor of supabase/repeatable/04102026_0260_banking_pay_next_simple_cancel_v1.sql
create or replace function private.bpay_next_simple_cancel_block_v1(
  p_run_worker_id uuid,p_allow_building boolean default false
) returns text
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_count integer;
begin
  select * into strict v_worker from private.bpay_next_run_worker
    where id=p_run_worker_id;
  -- Only a no-case, never-executed frozen UMB worker may use SIMPLE_CANCEL.
  -- No current Candidate/rate/Source lookup and no UMB NET/CSV admission.
  if v_worker.target_pay_channel='UMBRELLA' then
    if v_worker.status<>'READY' or v_worker.target_umbrella_id is null
       or v_worker.target_umbrella_vat_chargeable is null
       or v_worker.gross_ex_vat<0 or v_worker.gross_vat<0 or v_worker.gross_inc_vat<0
       or v_worker.gross_inc_vat<>v_worker.gross_ex_vat+v_worker.gross_vat
       or (v_worker.target_umbrella_vat_chargeable is false and v_worker.gross_vat<>0) then
      return 'BPAY_NEXT_CANCEL_GROUP_NOT_SIMPLE_DRAFT';
    end if;
    if v_worker.realised_effect_count<>0 or v_worker.financial_resolution_count<>0
       or not exists(select 1 from private.bpay_next_worker_control c
         where c.candidate_id=v_worker.candidate_id and c.pending_outcome_count=0) then
      return 'BPAY_NEXT_CANCEL_REALISED_OR_PENDING';
    end if;
    if v_worker.case_selection_revision<>0 or v_worker.active_case_hold_count<>0
       or v_worker.case_instruction_count<>0 or v_worker.case_allocated_count<>0
       or v_worker.case_pending_binding_count<>0
       or exists(select 1 from private.bpay_next_case_hold h where h.run_worker_id=v_worker.id) then
      return 'BPAY_NEXT_CANCEL_CASE_GROUP_UNSUPPORTED';
    end if;
    -- UMB uses frozen gross-inc cash, never a payroll NET request/value.
    if v_worker.net_request_revision is distinct from 0
       or v_worker.net_projection_revision not in (0,1)
       or v_worker.net_projection_revision is null
       or v_worker.entered_paye_net is not null
       or exists(select 1 from private.bpay_next_paye_net_request n where n.run_worker_id=v_worker.id) then
      return 'BPAY_NEXT_CANCEL_PROJECTION_UNSUPPORTED';
    end if;
    select count(*) into v_count from
      (select 1 from private.bpay_next_net_projection p
       where p.run_worker_id=v_worker.id order by p.projection_no limit 2) p;
    if v_count>1 or v_count is distinct from v_worker.net_projection_revision then
      return 'BPAY_NEXT_CANCEL_PROJECTION_UNSUPPORTED';
    end if;
    if v_count=1 then
      select p.* into strict v_projection from private.bpay_next_net_projection p
        where p.run_worker_id=v_worker.id;
      if (v_projection.run_worker_id,v_projection.projection_no,v_projection.input_kind,
          v_projection.gross_ex_vat,v_projection.gross_vat,v_projection.gross_inc_vat,
          v_projection.cash_amount,v_projection.accepted_recoveries,
          v_projection.accepted_gross_additions,v_projection.accepted_gross_deductions,
          v_projection.accepted_net_additions)
         is distinct from
         (v_worker.id,1::bigint,'UMBRELLA'::text,
          v_worker.gross_ex_vat,v_worker.gross_vat,v_worker.gross_inc_vat,
          v_worker.gross_inc_vat,0::numeric,0::numeric,0::numeric,0::numeric)
         or v_projection.entered_paye_net is not null
         or v_projection.request_command_id is not null
         or v_projection.retired_at_utc is not null then
        return 'BPAY_NEXT_CANCEL_PROJECTION_UNSUPPORTED';
      end if;
    end if;
    -- One original transfer only. Intake may precede its earlier builder;
    -- the unchanged claimant waits, and first ordered page disallows BUILDING.
    select count(*) into v_count from
      (select 1 from private.bpay_next_transfer t
       where t.run_worker_id=v_worker.id order by t.transfer_no limit 2) t;
    if v_count>1 then return 'BPAY_NEXT_CANCEL_MULTI_TRANSFER_UNSUPPORTED';end if;
    if v_count=1 then
      select t.* into strict v_transfer from private.bpay_next_transfer t
        where t.run_worker_id=v_worker.id;
      if v_projection.id is null
         or (v_transfer.run_worker_id,v_transfer.candidate_id,v_transfer.transfer_no,
             v_transfer.beneficiary_kind,v_transfer.beneficiary_id,
             v_transfer.projection_id,v_transfer.cash_amount,v_transfer.execution_kind)
            is distinct from
            (v_worker.id,v_worker.candidate_id,1::bigint,
             'UMBRELLA'::text,v_worker.target_umbrella_id,
             v_projection.id,v_projection.cash_amount,'BANK'::text)
         or v_transfer.cash_amount<=0
         or v_transfer.original_transfer_id is not null
         or v_transfer.return_cash_id is not null then
        return 'BPAY_NEXT_CANCEL_REISSUE_OR_NON_CANDIDATE_UNSUPPORTED';
      end if;
      if v_transfer.status not in ('MEMBERS_READY','DRAFT','SCHEDULED')
         and not (p_allow_building is true and v_transfer.status='BUILDING') then
        return 'BPAY_NEXT_CANCEL_TRANSFER_PROTECTED';
      end if;
      if v_transfer.account_approval_ref is not null
         or exists(select 1 from private.bpay_next_csv_instruction i where i.transfer_id=v_transfer.id)
         or exists(select 1 from private.bpay_next_transfer_outcome o where o.transfer_id=v_transfer.id)
         or exists(select 1 from private.bpay_next_internal_receipt r where r.transfer_id=v_transfer.id) then
        return 'BPAY_NEXT_CANCEL_INSTRUCTION_OR_OUTCOME_PROTECTED';
      end if;
    end if;
    return null;
  end if;
  if v_worker.case_selection_revision>0 then
    return private.bpay_next_case_cancel_block_v1(p_run_worker_id,p_allow_building);
  end if;
  if v_worker.status<>'READY' or v_worker.target_pay_channel<>'PAYE'
     or v_worker.target_umbrella_id is not null or v_worker.gross_vat<>0
     or v_worker.gross_inc_vat<0
     or v_worker.net_request_revision not between 0 and 1
     or v_worker.net_projection_revision not between 0 and 1
     or v_worker.net_projection_revision>v_worker.net_request_revision then
    return 'BPAY_NEXT_CANCEL_GROUP_NOT_SIMPLE_DRAFT';
  end if;
  if v_worker.realised_effect_count<>0
     or v_worker.financial_resolution_count<>0
     or exists(select 1 from private.bpay_next_worker_control c
               where c.candidate_id=v_worker.candidate_id
                 and c.pending_outcome_count<>0) then
    return 'BPAY_NEXT_CANCEL_GROUP_REALISED_OR_PENDING_OUTCOME';
  end if;
  if v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_case_hold h
               where h.run_worker_id=v_worker.id) then
    return 'BPAY_NEXT_CANCEL_CASE_GROUP_UNSUPPORTED';
  end if;
  if exists(select 1 from private.bpay_next_net_projection p
            where p.run_worker_id=v_worker.id
              and (p.projection_no<>1 or p.input_kind<>'PAYE_MANUAL'
                   or p.accepted_recoveries<>0
                   or p.retired_at_utc is not null)) then
    return 'BPAY_NEXT_CANCEL_PROJECTION_UNSUPPORTED';
  end if;
  if p_allow_building is not true
     and v_worker.net_request_revision<>v_worker.net_projection_revision then
    return 'BPAY_NEXT_CANCEL_PROJECTION_PENDING';
  end if;
  -- The existing original builder admits at most one original transfer.
  -- LIMIT 2 is deliberate: a broader/reissue group is not this owner's slice.
  select count(*) into v_count from
    (select 1 from private.bpay_next_transfer t
     where t.run_worker_id=v_worker.id order by t.transfer_no limit 2) t;
  if v_count>1 then return 'BPAY_NEXT_CANCEL_MULTI_TRANSFER_UNSUPPORTED'; end if;
  if v_count=1 then
    select * into strict v_transfer from private.bpay_next_transfer
      where run_worker_id=v_worker.id;
    if v_transfer.original_transfer_id is not null
       or v_transfer.return_cash_id is not null
       or v_transfer.projection_id is null
       or v_transfer.candidate_id<>v_worker.candidate_id
       or v_transfer.beneficiary_kind<>'CANDIDATE'
       or v_transfer.beneficiary_id<>v_worker.candidate_id then
      return 'BPAY_NEXT_CANCEL_REISSUE_OR_NON_CANDIDATE_UNSUPPORTED';
    end if;
    if v_transfer.status not in ('MEMBERS_READY','DRAFT','SCHEDULED')
       and not (p_allow_building is true and v_transfer.status='BUILDING') then
      return 'BPAY_NEXT_CANCEL_TRANSFER_PROTECTED';
    end if;
    if v_transfer.account_approval_ref is not null
       or exists(select 1 from private.bpay_next_csv_instruction i
                 where i.transfer_id=v_transfer.id)
       or exists(select 1 from private.bpay_next_transfer_outcome o
                 where o.transfer_id=v_transfer.id)
       or exists(select 1 from private.bpay_next_internal_receipt r
                 where r.transfer_id=v_transfer.id) then
      return 'BPAY_NEXT_CANCEL_INSTRUCTION_OR_OUTCOME_PROTECTED';
    end if;
  end if;
  return null;
end
$function$;

ALTER FUNCTION private.bpay_next_simple_cancel_block_v1(uuid,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_simple_cancel_block_v1(uuid,boolean) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D09 successor of supabase/repeatable/04102026_0270_banking_pay_next_financial_intake_service_v1.sql
create or replace function public.bpay_next_financial_intake_v1(
  p_actor_user_id uuid,p_action text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];
  v_key text;
  v_command_id uuid;
  v_worker_id uuid;
  v_transfer_id uuid;
  v_instruction_id uuid;
  v_return_cash_id uuid;
  v_run_id uuid;
  v_amount numeric;
  v_occurred_at timestamptz;
  v_result jsonb;
  v_output jsonb;
  v_sequence text;
  v_projection_revision bigint;
  v_case_selection_revision bigint;
  v_case_component_id uuid;
  v_component_revision bigint;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
     or p_actor_user_id is null
     or not exists(select 1 from public.tms_users u
                   where u.id=p_actor_user_id and u.is_active is true
                     and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_action is null or p_action not in
      ('PAYE_NET','CASE_PAYOUT','TRANSFER_BEGIN','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL',
       'CSV_ISSUE','CSV_SETTLEMENT','CSV_RETURN','INTERNAL_SETTLE','PREPARATION_EXPIRE','WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  v_keys:=case p_action
    when 'WRITE_OFF' then array['command_id','case_component_id','expected_component_revision','scope','reason']
      || case when p_args->>'scope'='AMOUNT' then array['amount'] else array[]::text[] end
    when 'PAYE_NET' then array['command_id','run_worker_id','entered_paye_net']
      || case when p_args ? 'expected_projection_revision' then array['expected_projection_revision'] else array[]::text[] end
    when 'CASE_PAYOUT' then array['command_id','run_worker_id','expected_projection_revision']
    when 'TRANSFER_BEGIN' then array['command_id','run_worker_id']
    when 'CANCEL_REQUEST' then array['command_id','run_worker_id']
      || case when p_args ? 'run_id' or p_args ? 'pay_channel'
        then array['run_id','pay_channel'] else array[]::text[] end
    when 'CASH_REISSUE' then array['command_id','return_cash_id']
    when 'REISSUE_CANCEL' then array['command_id','transfer_id']
    when 'CSV_ISSUE' then array['instruction_id','transfer_id']
    when 'INTERNAL_SETTLE' then array['command_id','transfer_id']
    when 'PREPARATION_EXPIRE' then array['command_id','run_id','expected_deadline']
    else array['command_id','transfer_id','receipt_id','amount','occurred_at'] end;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k
               where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  foreach v_key in array v_keys loop
    if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key in ('command_id','run_worker_id','transfer_id','instruction_id','return_cash_id','run_id','case_component_id')
       and (p_args->>v_key)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key='pay_channel' and (p_args->>v_key) not in ('PAYE','UMBRELLA') then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end loop;
  v_command_id:=(p_args->>'command_id')::uuid;
  v_worker_id:=(p_args->>'run_worker_id')::uuid;
  v_transfer_id:=(p_args->>'transfer_id')::uuid;
  v_instruction_id:=(p_args->>'instruction_id')::uuid;
  v_return_cash_id:=(p_args->>'return_cash_id')::uuid;
  if p_action='WRITE_OFF' then
    v_case_component_id:=(p_args->>'case_component_id')::uuid;
    if (p_args->>'expected_component_revision')!~'^[1-9][0-9]{0,18}$'
       or (p_args->>'expected_component_revision')::numeric>9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_component_revision:=(p_args->>'expected_component_revision')::bigint;
  end if;
  if p_action='PREPARATION_EXPIRE' then
    v_run_id:=(p_args->>'run_id')::uuid;
    if (p_args->>'expected_deadline')!~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'expected_deadline')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_args ? 'expected_projection_revision' then
    if (p_args->>'expected_projection_revision')!~'^(0|[1-9][0-9]{0,18})$'
       or (p_args->>'expected_projection_revision')::numeric>=9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_projection_revision:=(p_args->>'expected_projection_revision')::bigint;
  end if;
  if p_action in ('PAYE_NET','CSV_SETTLEMENT','CSV_RETURN') then
    v_key:=case when p_action='PAYE_NET' then 'entered_paye_net' else 'amount' end;
    -- Current money columns are numeric(18,2). Explicit finite, ordinary
    -- decimal syntax prevents exponent/NaN, narrowing and JSON-number coercion.
    if (p_args->>v_key)!~'^(0|[1-9][0-9]{0,15})([.][0-9]{1,2})?$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_amount:=(p_args->>v_key)::numeric;
    if p_action<>'PAYE_NET' and v_amount<=0 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_action in ('CSV_SETTLEMENT','CSV_RETURN') then
    if pg_catalog.octet_length(p_args->>'receipt_id') not between 1 and 256
       or (p_args->>'occurred_at')!~
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'occurred_at')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;

  -- Fixed branches and exact typed IDs only. No dynamic identifier, query,
  -- caller actor, arbitrary RPC, live finance/history reconstruction or drain.
  case p_action
    when 'WRITE_OFF' then
      v_result:=private.bpay_next_accept_write_off_v1(v_command_id,v_case_component_id,p_actor_user_id,
        v_component_revision,p_args->>'scope',p_args->>'amount',p_args->>'reason');
      v_output:=pg_catalog.jsonb_build_object('command_id',v_result->>'command_id',
        'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase','requested_scope',v_result->>'requested_scope',
        'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
        'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id',
        'issue_code',v_result->>'issue_code','replay',v_result->'replay');
    when 'PREPARATION_EXPIRE' then
      -- NULL clock selects the real server clock AFTER acquiring the header.
      -- Office cannot supply SYSTEM authority, now, or a renewal duration.
      v_result:=private.bpay_next_accept_preparation_expiry_v1(
        v_command_id,v_run_id,p_actor_user_id,v_occurred_at,null);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','run_id',v_result->>'run_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'expected_candidates',v_result->>'expected_candidates',
        'completed_candidates',v_result->>'completed_candidates',
        'replay',v_result->'replay');
    when 'PAYE_NET' then
      -- Route by the worker's FROZEN selection, never by live Candidate cases.
      -- Explicit prior projection protects resaves from overwriting a newer one.
      select w.case_selection_revision into strict v_case_selection_revision
        from private.bpay_next_run_worker w where w.id=v_worker_id;
      if v_case_selection_revision>0 then
        if v_projection_revision is null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_case_paye_net_v1(
          v_command_id,v_worker_id,p_actor_user_id,v_amount,v_projection_revision);
      else
        if v_projection_revision is not null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_simple_paye_net_v1(v_command_id,v_worker_id,v_amount);
      end if;
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CASE_PAYOUT' then
      v_result:=private.bpay_next_accept_case_payout_projection_v1(
        v_command_id,v_worker_id,p_actor_user_id,v_projection_revision);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'TRANSFER_BEGIN' then
      v_result:=private.bpay_next_begin_simple_transfer_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','transfer_id',v_result->>'transfer_id',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CANCEL_REQUEST' then
      -- Optional address is strictly paired and independently bound before accepting intent.
      if p_args ? 'run_id' and not exists(select 1 from private.bpay_next_run_worker w
        where w.id=v_worker_id and w.run_id=(p_args->>'run_id')::uuid
          and w.target_pay_channel=p_args->>'pay_channel') then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_CHANNEL_SCOPE_INVALID';
      end if;
      v_result:=private.bpay_next_accept_simple_cancel_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'cursor',v_result->>'cursor','released_line_count',v_result->>'released_line_count',
        'blocked_code',v_result->>'blocked_code','replay',v_result->'replay');
    when 'CASH_REISSUE' then
      v_result:=private.bpay_next_receive_simple_reissue_v1(v_command_id,v_return_cash_id,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c where c.id=v_command_id and c.command_kind='CASH_REISSUE';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'return_cash_id',v_return_cash_id,
        'sequence',v_sequence,'transfer_id',v_result->>'transfer_id',
        'phase',case when v_result->>'transfer_id' is null
          then 'ACCEPTED_PENDING_BUILD' else 'TRANSFER_CREATED' end,
        'replay',v_result->'replay');
    when 'REISSUE_CANCEL' then
      v_result:=private.bpay_next_accept_reissue_cancel_v1(v_command_id,v_transfer_id,p_actor_user_id);
      select r.blocked_code into v_key from private.bpay_next_reissue_cancel_request r where r.command_id=v_command_id;
      v_output:=pg_catalog.jsonb_build_object('command_id',v_command_id,'transfer_id',v_transfer_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase','blocked_code',v_key,'replay',v_result->'replay');
    when 'CSV_ISSUE' then
      -- A new CSV issue first binds its current approved destination using the
      -- existing owner. Already-issued replay never rebinds or changes bytes.
      -- Both calls use the same transaction and parent run mutex; any issue
      -- permission/state failure rolls the destination preparation back too.
      -- Acquire that mutex BEFORE checking instruction existence: concurrent
      -- same-ID retry must see the first committed issue and skip rebinding.
      select w.run_id into strict v_run_id
        from private.bpay_next_transfer t
        join private.bpay_next_run_worker w on w.id=t.run_worker_id
        where t.id=v_transfer_id;
      perform 1 from private.bpay_next_pay_run where id=v_run_id for update;
      if not exists(select 1 from private.bpay_next_csv_instruction i where i.id=v_instruction_id) then
        perform private.bpay_next_bind_simple_csv_destination_v1(v_transfer_id);
      end if;
      v_result:=private.bpay_next_issue_simple_csv_v1(v_instruction_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'instruction_id',v_result->>'instruction_id','transfer_id',v_result->>'transfer_id',
        'file_name',v_result->>'file_name','csv_sha256',v_result->>'csv_sha256',
        'csv_text',v_result->>'csv_text','phase','ISSUED_CSV',
        'payment_recorded',v_result->'payment_recorded','replay',v_result->'replay');
    when 'CSV_SETTLEMENT' then
      v_result:=private.bpay_next_receive_simple_csv_settlement_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_SETTLEMENT';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
    when 'INTERNAL_SETTLE' then
      -- The server creates an internal receipt for an exact zero-cash frozen
      -- envelope. There is no caller amount, bank receipt or CSV instruction.
      v_result:=private.bpay_next_receive_internal_settlement_v1(
        v_command_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_result->>'transfer_id',
        'internal_receipt_id',v_result->>'internal_receipt_id','sequence',v_result->>'sequence',
        'posting_complete',v_result->'posting_complete','phase',v_result->>'phase','replay',v_result->'replay');
    when 'CSV_RETURN' then
      v_result:=private.bpay_next_receive_simple_csv_return_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_RETURN';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
  end case;
  if pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean'
     or pg_catalog.octet_length(v_output::text)>120000 then
    -- Exception aborts the whole request transaction; no partial issue or
    -- accepted financial request is committed without its bounded receipt.
    raise exception using errcode='54000',message='BPAY_NEXT_COMMAND_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;

ALTER FUNCTION public.bpay_next_financial_intake_v1(uuid,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bpay_next_financial_intake_v1(uuid,text,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bpay_next_financial_intake_v1(uuid,text,jsonb) TO service_role;

-- F-05-D08 successor of supabase/repeatable/04102026_0430_banking_pay_next_week_lifecycle_v1.sql
create or replace function private.bpay_next_apply_week_outcome_v1(
  p_job_id uuid,p_return_cash_id uuid default null
) returns void
language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_control private.bpay_next_worker_control%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_original private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_outcome private.bpay_next_transfer_outcome%rowtype;
  v_settlement private.bpay_next_outcome_request%rowtype;
  v_return private.bpay_next_return_request%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;
  v_week private.bpay_next_worker_week%rowtype;
  v_prior private.bpay_next_worker_week_contribution%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_epoch bigint;v_worker_id uuid;v_candidate uuid;v_transfer_id uuid;
  v_outcome_id uuid;v_posted boolean;v_next_state text;v_payout boolean;
  v_old_resolved numeric;v_unresolved_delta integer:=0;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_destination private.bpay_next_net_destination_state%rowtype;
begin
  if p_job_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_WEEK_OUTCOME_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind='CSV_SETTLEMENT' then
    select r.* into strict v_settlement from private.bpay_next_outcome_request r where r.command_id=v_job.command_id;
    v_worker_id:=v_settlement.run_worker_id;v_candidate:=v_settlement.candidate_id;
    v_transfer_id:=v_settlement.transfer_id;v_outcome_id:=v_settlement.outcome_id;
    if p_return_cash_id is not null then
      raise exception using errcode='22023',message='BPAY_NEXT_WEEK_SETTLEMENT_CASH_ARGUMENT_INVALID';
    end if;
  elsif v_job.job_kind='CSV_RETURN' then
    select r.* into strict v_return from private.bpay_next_return_request r where r.command_id=v_job.command_id;
    v_worker_id:=v_return.run_worker_id;v_candidate:=v_return.candidate_id;
    v_transfer_id:=v_return.transfer_id;v_outcome_id:=v_return.outcome_id;
    if p_return_cash_id is null then
      raise exception using errcode='22023',message='BPAY_NEXT_WEEK_RETURN_CASH_REQUIRED';
    end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_WEEK_OUTCOME_JOB_KIND_INVALID';end if;
  -- Same order as the posting caller, also safe for an owner-only first-use
  -- probe: header -> Candidate control -> job -> worker -> transfer/cash -> week.
  select r.* into strict v_run from private.bpay_next_pay_run r
    join private.bpay_next_run_worker w on w.run_id=r.id where w.id=v_worker_id for update of r;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker_id for update;
  select t.* into strict v_transfer from private.bpay_next_transfer t where t.id=v_transfer_id for update;
  select o.* into strict v_outcome from private.bpay_next_transfer_outcome o where o.id=v_outcome_id;
  if v_job.job_kind='CSV_SETTLEMENT' then
    select r.* into strict v_settlement from private.bpay_next_outcome_request r where r.command_id=v_job.command_id;
    v_posted:=v_settlement.posting_complete and v_settlement.posted_member_count=v_transfer.member_count;
  else
    select r.* into strict v_return from private.bpay_next_return_request r where r.command_id=v_job.command_id;
    v_posted:=v_return.posting_complete;
  end if;
  if v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_candidate
     or v_worker.candidate_id<>v_candidate or v_worker.run_id<>v_run.id
     or v_transfer.candidate_id<>v_candidate or v_transfer.run_worker_id<>v_worker.id
     or not ((v_worker.target_pay_channel='PAYE' and v_transfer.beneficiary_kind='CANDIDATE'
         and v_transfer.beneficiary_id=v_candidate)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.case_selection_revision=0
         and v_worker.target_umbrella_id is not null and v_transfer.beneficiary_kind='UMBRELLA'
         and v_transfer.beneficiary_id=v_worker.target_umbrella_id))
     or v_outcome.transfer_id<>v_transfer.id or v_outcome.whole_transfer_amount<>v_transfer.cash_amount
     or v_outcome.outcome_kind<>(case when v_job.job_kind='CSV_SETTLEMENT' then 'SETTLED' else 'RETURNED' end)
     or v_posted is not true then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_OUTCOME_SCOPE_INVALID';
  end if;
  select t.* into strict v_original from private.bpay_next_transfer t
    where t.id=coalesce(v_transfer.original_transfer_id,v_transfer.id) for update;
  if v_original.original_transfer_id is not null or v_original.return_cash_id is not null
     or v_original.run_worker_id<>v_worker.id or v_original.candidate_id<>v_candidate
     or v_original.beneficiary_kind is distinct from v_transfer.beneficiary_kind
     or v_original.beneficiary_id is distinct from v_transfer.beneficiary_id then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_ORIGINAL_TRANSFER_INVALID';
  end if;
  if v_job.job_kind='CSV_RETURN' or v_transfer.return_cash_id is not null then
    select c.* into strict v_cash from private.bpay_next_return_cash c
      where c.id=coalesce(p_return_cash_id,v_transfer.return_cash_id) for update;
    if v_cash.original_transfer_id<>v_original.id or v_cash.candidate_id<>v_candidate
       or v_cash.amount_owed<>v_original.cash_amount
       or (v_transfer.return_cash_id is not null and v_transfer.return_cash_id<>v_cash.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_RETURN_CASH_NOT_EXACT';
    end if;
  end if;
  -- The posting transition and this helper commit atomically. A delayed old
  -- DONE helper is readback, not adoption/backfill of its old reissue pointer.
  if v_job.status='DONE' then return;end if;
  if v_job.status<>'LEASED' or v_job.lease_nonce is null
     or v_job.owner_epoch<>v_control.active_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_transfer.status not in ('SETTLED','RETURNED')
     or (v_job.job_kind='CSV_RETURN' and v_transfer.status<>'RETURNED') then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_OUTCOME_OWNER_INVALID';
  end if;
  if v_worker.target_pay_channel<>'PAYE' then return;end if;
  v_leg:=private.bpay_next_destination_execution_leg_v1(v_original.id);
  if v_leg.transfer_id is not null then
    -- One-off cash is not the Candidate's own weekly take-home contribution.
    -- Its exact receipt still posts its CASE members through the normal owner.
    if v_leg.leg_kind='ONEOFF' then return;end if;
    select n.* into strict v_destination from private.bpay_next_destination_group g
      join private.bpay_next_net_destination_state n on n.state_id=g.net_state_id
      where g.anchor_transfer_id=v_leg.anchor_transfer_id;
  end if;
  select w.* into v_week from private.bpay_next_worker_week w
    where w.candidate_id=v_candidate
      and w.pay_week_start=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1) for update;
  select x.* into v_prior from private.bpay_next_worker_week_contribution x
    where x.original_run_worker_id=v_worker.id for update;
  if v_prior.id is null then
    if v_worker.case_selection_revision<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CASE_CONTRIBUTION_MISSING';
    end if;
    -- An explicitly unbound no-case READY floor has no contribution. Do not
    -- reconstruct one from a now-paid transfer or guess its earlier earnings.
    raise notice 'BPAY_NEXT_WEEK_ORIGINAL_CONTRIBUTION_UNBOUND';return;
  end if;
  select p.* into strict v_projection from private.bpay_next_net_projection p where p.id=v_original.projection_id;
  if v_week.candidate_id is null or v_prior.candidate_id<>v_candidate
     or v_prior.pay_week_start<>v_week.pay_week_start
     or v_prior.original_pay_date<>v_run.pay_date or v_prior.original_created_at_utc<>v_run.created_at_utc
     or v_prior.original_gross_amount<>v_projection.gross_inc_vat
     or v_prior.accepted_projection_id is distinct from v_projection.id
     or v_prior.accepted_bank_cash is distinct from v_projection.cash_amount
     or v_projection.run_worker_id<>v_worker.id
     or v_original.cash_amount<>(case when v_leg.transfer_id is null then v_projection.cash_amount else v_destination.own_amount end)
     or (v_prior.original_transfer_id is not null and v_prior.original_transfer_id<>v_original.id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_ORIGINAL_CONTRIBUTION_NOT_EXACT';
  end if;
  v_payout:=v_projection.input_kind='CASE_PAYOUT';
  if v_payout and (v_prior.basis_kind<>'GROSS_FALLBACK' or v_prior.original_gross_amount<>0
     or v_prior.original_payroll_net is not null or v_prior.eligibility_state<>'EXCLUDED'
     or v_prior.eligible_arranged_amount is distinct from 0::numeric) then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NON_EARNINGS_BASIS_INVALID';
  end if;
  if v_job.job_kind='CSV_SETTLEMENT' and v_transfer.original_transfer_id is null then
    -- RETURNED may already be RECEIVED: original payroll posting still comes
    -- first. The ordered return owner will later publish the actual cash FK.
    if v_prior.payment_state='PAID' and v_prior.original_transfer_id=v_original.id then return;end if;
    if v_prior.payment_state<>'ARRANGED' or v_prior.eligibility_state='UNBOUND' then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_ORIGINAL_SETTLEMENT_STATE_INVALID';
    end if;
    update private.bpay_next_worker_week_contribution set payment_state='PAID',original_transfer_id=v_original.id,
      contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
    update private.bpay_next_worker_week set period_revision=period_revision+1
      where candidate_id=v_candidate and pay_week_start=v_week.pay_week_start;
    return;
  end if;
  if v_prior.original_transfer_id is distinct from v_original.id
     or (v_prior.returned_cash_id is not null and v_prior.returned_cash_id<>v_cash.id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_RETURN_ORIGINAL_NOT_PAID';
  end if;
  if v_job.job_kind='CSV_SETTLEMENT' then
    v_next_state:='REISSUED_PAID';
    if v_prior.payment_state='REISSUED_PAID' and v_prior.reissue_transfer_id=v_transfer.id then return;end if;
    if v_prior.payment_state<>'RETURNED_OWED' or v_prior.returned_cash_id is distinct from v_cash.id
       or v_cash.amount_reissued_paid<v_transfer.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_REISSUE_SETTLEMENT_STATE_INVALID';
    end if;
  else
    v_next_state:='RETURNED_OWED';
    if v_prior.payment_state='RETURNED_OWED' and v_prior.returned_cash_id=v_cash.id
       and (v_transfer.original_transfer_id is null or v_prior.reissue_transfer_id=v_transfer.id) then return;end if;
    if (v_transfer.original_transfer_id is null and v_prior.payment_state<>'PAID')
       or (v_transfer.original_transfer_id is not null and
         (v_prior.payment_state<>'REISSUED_PAID' or v_prior.reissue_transfer_id is distinct from v_transfer.id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_RETURN_PAYMENT_STATE_INVALID';
    end if;
  end if;
  v_old_resolved:=coalesce(v_prior.eligible_arranged_amount,0);
  if not v_payout and v_prior.eligibility_state<>'UNBOUND' then v_unresolved_delta:=1;end if;
  update private.bpay_next_worker_week_contribution set payment_state=v_next_state,returned_cash_id=v_cash.id,
    reissue_transfer_id=case when v_transfer.original_transfer_id is not null then v_transfer.id else reissue_transfer_id end,
    eligibility_state=case when v_payout then 'EXCLUDED' else 'UNBOUND' end,
    eligible_arranged_amount=case when v_payout then 0 else null end,
    contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
  update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home-v_old_resolved,
    unresolved_contribution_count=unresolved_contribution_count+v_unresolved_delta,period_revision=period_revision+1
    where candidate_id=v_candidate and pay_week_start=v_week.pay_week_start
      and resolved_arranged_take_home>=v_old_resolved;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_RESOLVED_DELTA_INVALID';end if;
end
$function$;

ALTER FUNCTION private.bpay_next_apply_week_outcome_v1(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_apply_week_outcome_v1(uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D19 successor of supabase/repeatable/04102026_0557_banking_pay_next_source_owner_completion_v1.sql
CREATE OR REPLACE FUNCTION public.weekly_source_first_authorise_v1(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_expected_row_signature text, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_context jsonb;
  v_lock jsonb;
  v_core jsonb;
  v_canonical uuid;
  -- BEGIN A8 SOURCE FIRST AUTHORISE DECLARATIONS
  v_work uuid;
  v_revision uuid;
  v_event uuid;
  -- END A8 SOURCE FIRST AUTHORISE DECLARATIONS
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  if p_timesheet_id is null or p_actor_user_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_REQUEST_INVALID',
      'retryable',false,'reason','TIMESHEET_AND_ACTOR_REQUIRED');
  end if;

  -- One plain read before the gate, because the gate is pinned to a Candidate.
  -- Nothing is written by it and the identity is re-resolved under the locks.
  v_context:=private.bpay_source_authorise_identity_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    return v_context;
  end if;

  -- BEGIN QUERY V2 FIRST AUTHORISE ADMISSION
  -- Admit before the first Source/Candidate lock; contention remains exact
  -- 55P03, not a financial REVIEW or an automatic owner retry.
  perform private.weekly_source_pay_query_admit_v2();
  -- END QUERY V2 FIRST AUTHORISE ADMISSION

  v_lock:=private.bpay_source_authorise_lock_v1(
    (v_context->>'candidate_id')::uuid,p_timesheet_id,pg_catalog.gen_random_uuid());
  if coalesce((v_lock->>'ok')::boolean,false) is not true then
    return v_lock;
  end if;

  select (family_element.value->>'canonical_timesheet_id')::uuid
    into v_canonical
  from pg_catalog.jsonb_array_elements(coalesce(v_lock->'families','[]'::jsonb))
    as family_element(value)
  where (family_element.value->>'requested_timesheet_id')::uuid=p_timesheet_id;

  if p_expected_timesheet_id is not null
     and p_expected_timesheet_id is distinct from v_canonical then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
      'retryable',false,'timesheet_id',p_timesheet_id,
      'canonical_timesheet_id',v_canonical);
  end if;

  v_core:=private.weekly_source_first_authorise_core_v1(
    p_timesheet_id,p_expected_row_signature,p_actor_user_id,v_lock);
  -- BEGIN A8 SOURCE FIRST AUTHORISE NEXT TAIL
  -- Capture only after the real outer first-authorise owner succeeds. The
  -- unchanged private core also runs inside two-root head publication and must
  -- never publish an intermediate B revision.
  if coalesce((v_core->>'ok')::boolean,false) is true
     and (select active_owner from private.bpay_next_module_control
          where id=1)='NEXT' then
    v_event:=(v_core->>'root_authorisation_id')::uuid;
    select s.work_id,s.revision_id into strict v_work,v_revision
      from private.bpay_next_stage_source_current_v1(
        p_timesheet_id,v_event,null) s;
    perform * from private.bpay_next_publish_source_staged_pair_v1(
      array[v_work],array[v_revision],array[v_event]);
    -- Metadata only, after the real outer stage/publication; no position or
    -- financial seed while the queued POSITION_APPLY is still incomplete.
    perform * from private.bpay_next_initial_tsfin_bind_v1(v_event);
  end if;
  -- END A8 SOURCE FIRST AUTHORISE NEXT TAIL
  return v_core||pg_catalog.jsonb_build_object('gate',v_lock->>'gate');
end;
$function$;

ALTER FUNCTION public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid) TO service_role;

-- F-05-D18 successor of supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql
create or replace function private.bpay_next_preparation_args_v1(p_args jsonb,p_keys text[])
returns void language plpgsql security invoker
set search_path=pg_catalog
as $function$
declare v_key text;v_type text;v_text text;
begin
  if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
    or pg_catalog.octet_length(p_args::text)>32768
    or (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.cardinality(p_keys)
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(p_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  foreach v_key in array p_keys loop
    v_type:=pg_catalog.jsonb_typeof(p_args->v_key);v_text:=p_args->>v_key;
    if v_key in ('choices','approved_line_ids','component_ids','selected') then
      if v_type is distinct from 'array' or pg_catalog.jsonb_array_length(p_args->v_key) not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='limit' then
      if v_type is distinct from 'number' or v_text!~'^[0-9]{1,3}$' or v_text::integer not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key like 'after_%' and v_type='null' then
      continue;
    elsif v_type is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    elsif v_key like '%_id' then
      if v_text!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key in ('pay_date','after_week') then
      if v_text!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or not pg_catalog.isfinite(v_text::date)
        or v_text::date::text<>v_text then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='pay_channel' then
      if v_text not in ('PAYE','UMBRELLA') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='after_bucket' then
      if v_text not in ('DAY','NIGHT','SAT','SUN','BH') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    else
      if v_text!~'^(0|[1-9][0-9]{0,18})$' or v_text::numeric>9223372036854775807
        or (v_key in ('page_no','selection_revision','review_revision') and v_text='0') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    end if;
  end loop;
end
$function$;

ALTER FUNCTION private.bpay_next_preparation_args_v1(jsonb,text[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_preparation_args_v1(jsonb,text[]) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D18 successor of supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql
create or replace function public.bpay_next_preparation_intake_v1(p_actor_user_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_run uuid;v_work uuid;v_revision uuid;v_candidate uuid;v_result jsonb;v_output jsonb;
  v_ids uuid[];v_selected boolean[];v_entry jsonb;v_header private.bpay_next_pay_run%rowtype;
begin
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_action
    when 'CREATE' then array['run_id','pay_date']
    when 'WORK_PAGE' then array['run_id','page_no','choices']
    when 'COMPONENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','approved_line_ids']
    when 'COMPONENT_SEAL' then array['run_id','work_id','expected_revision_id','expected_pages','expected_count']
    when 'CASE_PAGE' then array['run_id','candidate_id','selection_revision','page_no','request_id','component_ids','selected']
    when 'CASE_SEAL' then array['run_id','candidate_id','selection_revision','expected_pages','expected_items']
    when 'SEAL_ENQUEUE' then array['run_id','command_id','expected_pages','expected_count']
    when 'REVIEW' then array['run_id']
    when 'CONFIRM' then array['run_id','review_revision'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(p_args,v_keys);
  v_run:=(p_args->>'run_id')::uuid;v_work:=(p_args->>'work_id')::uuid;
  v_revision:=(p_args->>'expected_revision_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
  if p_action in ('COMPONENT_PAGE','CASE_PAGE') then
    for v_entry in select value from pg_catalog.jsonb_array_elements(
      p_args->case when p_action='CASE_PAGE' then 'component_ids' else 'approved_line_ids' end) loop
      if pg_catalog.jsonb_typeof(v_entry) is distinct from 'string'
        or (v_entry#>>'{}')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      v_ids:=pg_catalog.array_append(v_ids,(v_entry#>>'{}')::uuid);
    end loop;
    if p_action='CASE_PAGE' then
      if pg_catalog.jsonb_array_length(p_args->'selected')<>pg_catalog.cardinality(v_ids) then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      for v_entry in select value from pg_catalog.jsonb_array_elements(p_args->'selected') loop
        if pg_catalog.jsonb_typeof(v_entry) is distinct from 'boolean' then
          raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
        end if;
        v_selected:=pg_catalog.array_append(v_selected,(v_entry#>>'{}')::boolean);
      end loop;
    end if;
  end if;
  case p_action
  when 'CREATE' then
    v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,(p_args->>'pay_date')::date);
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'status',v_result->>'status',
      'selection_state',v_result->>'selection_state','selection_count',v_header.selection_count::text,
      'selected_candidate_count',v_header.selected_candidate_count::text);
  when 'WORK_PAGE' then
    v_output:=private.bpay_next_append_work_choices_v1(v_run,(p_args->>'page_no')::bigint,p_args->'choices');
  when 'COMPONENT_PAGE' then
    v_output:=private.bpay_next_append_component_selection_page_v1(v_run,v_work,v_revision,(p_args->>'page_no')::bigint,v_ids);
  when 'COMPONENT_SEAL' then
    v_output:=private.bpay_next_seal_component_selection_v1(v_run,v_work,v_revision,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
  when 'CASE_PAGE' then
    v_result:=private.bpay_next_append_case_selection_page_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'page_no')::bigint,(p_args->>'request_id')::uuid,v_ids,v_selected);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',p_args->>'selection_revision','page_no',v_result->>'page_no',
      'item_count',v_result->>'item_count','replay',v_result->'replay');
  when 'CASE_SEAL' then
    v_result:=private.bpay_next_seal_case_selection_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_items')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',v_result->>'selection_revision','sealed',v_result->'sealed',
      'selected_count',v_result->>'selected_count','excluded_count',v_result->>'excluded_count','replay',v_result->'replay');
  when 'SEAL_ENQUEUE' then
    perform private.bpay_next_seal_selection_v1(v_run,(p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
    v_result:=private.bpay_next_enqueue_preparation_v1(v_run,(p_args->>'command_id')::uuid);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'command_id',v_result->>'command_id',
      'agency_sequence',v_result->>'agency_sequence','replay',v_result->'replay');
  when 'REVIEW' then
    v_result:=private.bpay_next_review_simple_run_v1(v_run);
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase','reason',v_result->>'reason',
      'review_revision',v_header.review_revision::text,'all_ready',v_result->'all_ready',
      'replay',coalesce(v_result->'replay','false'::jsonb));
  when 'CONFIRM' then
    v_result:=private.bpay_next_confirm_simple_run_v1(v_run,(p_args->>'review_revision')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase',
      'review_revision',v_result->>'review_revision','replay',v_result->'replay','drafts',v_result->'drafts');
  end case;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';
  end if;
  return v_output;
end
$function$;

ALTER FUNCTION public.bpay_next_preparation_intake_v1(uuid,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bpay_next_preparation_intake_v1(uuid,text,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bpay_next_preparation_intake_v1(uuid,text,jsonb) TO service_role;

-- F-05-D18 successor of supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql
create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
begin
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then array['run_id','after_work_id','limit']
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(p_args,v_keys);
  v_work:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_line:=(p_args->>'approved_line_id')::uuid;v_shift:=(p_args->>'shift_detail_id')::uuid;v_limit:=(p_args->>'limit')::integer;
  if p_kind='CHANNEL_DRAFTS' then
    return private.bpay_next_channel_drafts_page_v1((p_args->>'run_id')::uuid);
  elsif p_kind='CHANNEL_WORKERS' then
    return private.bpay_next_channel_worker_page_v1((p_args->>'run_id')::uuid,
      p_args->>'pay_channel',(p_args->>'after_candidate_id')::uuid,v_limit);
  elsif p_kind='CHANNEL_STATUS' then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    return private.bpay_next_channel_status_page_v1((p_args->>'run_id')::uuid,p_args->>'pay_channel',
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  -- Only the two exact factual0580 owners; no competing reader or financial
  -- history/calculation path. CURRENT_WORK does not certify payability.
  if p_kind='CURRENT_WORK' then
    return private.bpay_next_current_work_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_week')::date,(p_args->>'after_work_id')::uuid,v_limit);
  elsif p_kind='CURRENT_CASES' then
    return private.bpay_next_current_case_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_case_id')::uuid,v_limit);
  elsif p_kind='CURRENT_STORED_CREDITS' then
    -- Factual, exact-Candidate possible origins only; binding revalidates the
    -- retained creator/component/account facts. This is not pay eligibility.
    -- Closed raw reply includes scan_cursor from the bounded raw window;
    -- filtering can produce an empty nonterminal page without restarting it.
    return private.bpay_next_current_stored_credit_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_legacy_case_id')::uuid,v_limit);
  elsif p_kind='CASE_COMPONENTS' then
    return private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
  elsif p_kind in ('STATUS','RETURN_CASH') then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    if p_kind='RETURN_CASH' then
      return private.bpay_next_return_cash_page_v1((p_args->>'run_id')::uuid,
        (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
    end if;
    return private.bpay_next_worker_status_page_v1((p_args->>'run_id')::uuid,
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  if p_kind in ('COMPONENTS','SHIFTS','BREAKS','RATES') then
    if not exists(select 1 from private.bpay_next_work w
      join private.bpay_next_work_revision r on r.work_id=w.id and r.id=v_revision
      where w.id=v_work and w.approval_state='APPROVED' and w.current_revision_id=v_revision
        and w.applied_revision_id=v_revision and r.sealed_at_utc is not null and r.approved_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    if p_kind<>'COMPONENTS' and not exists(select 1 from private.bpay_next_approved_line l where l.id=v_line and l.revision_id=v_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
    if p_kind='BREAKS' and not exists(select 1 from private.bpay_next_shift_detail s where s.id=v_shift and s.approved_line_id=v_line) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
  elsif p_kind in ('WORK_CHOICES','SELECTED_COMPONENTS') then
    perform 1 from private.bpay_next_pay_run r where r.id=(p_args->>'run_id')::uuid;
    if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    if p_kind='SELECTED_COMPONENTS' then
      perform 1 from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work;
      if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    end if;
  end if;
  -- Each branch is a <=100 indexed relational page. A byte boundary ends
  -- before an entire row, never truncates facts; callers resume its last key.
  -- CASE evaluates only its chosen subquery, so unrelated review owners are
  -- never called with NULL scope. Each subquery orders/limits BEFORE packing.
  select case p_kind
    when 'COMPONENTS' then array(
      select pg_catalog.jsonb_build_object(
        'approved_line_id',l.id,'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',l.component_kind,
        'work_date',l.work_date,'source_pay_channel',r.source_pay_channel,'approved_source_ex_vat',l.source_pay_ex_vat::text,
        'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text,
        'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text,
        'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false)) as value
        from private.bpay_next_approved_line l join private.bpay_next_work_revision r on r.id=l.revision_id
        left join private.bpay_next_position p on p.work_id=v_work and p.component_key=l.component_key
        where l.revision_id=v_revision and l.line_no>coalesce((p_args->>'after_line_no')::bigint,0)
        order by l.line_no,l.id limit v_limit)
    when 'WORK_CHOICES' then array(
      select pg_catalog.jsonb_build_object('work_id',c.work_id,'expected_revision_id',c.expected_revision_id,
        'selection_mode',c.selection_mode,'selection_state',c.selection_state,'page_count',c.page_count::text,'component_count',c.component_count::text)
        from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid
          and ((p_args->>'after_work_id') is null or c.work_id>(p_args->>'after_work_id')::uuid)
        order by c.work_id limit v_limit)
    when 'SELECTED_COMPONENTS' then array(
      select pg_catalog.jsonb_build_object('selection_no',c.selection_no::text,
        'approved_line_id',c.approved_line_id,'expected_revision_id',c.expected_revision_id,'component_key',c.component_key)
        from private.bpay_next_selected_component c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work
          and c.selection_no>coalesce((p_args->>'after_selection_no')::bigint,0)
        order by c.selection_no limit v_limit)
    when 'WORKERS' then array(
      select pg_catalog.jsonb_build_object('run_worker_id',w.run_worker_id,'candidate_id',w.candidate_id,
        'worker_status',w.worker_status,'frozen_ex_vat',w.frozen_ex_vat::text,'frozen_vat',w.frozen_vat::text,
        'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code)
        from private.bpay_next_simple_review_worker_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit) w
        order by w.candidate_id limit v_limit)
    when 'REVISION_COMPARISON' then array(
      select pg_catalog.jsonb_build_object('run_work_id',w.run_work_id,'work_id',w.work_id,'original_timesheet_id',w.original_timesheet_id,
        'captured_revision_id',w.captured_revision_id,'current_revision_id',w.current_revision_id,'newer_approval_available',w.newer_approval_available)
        from private.bpay_next_simple_revision_comparison_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_work_id')::uuid,v_limit) w
        order by w.work_id limit v_limit)
    when 'LINES' then array(
      select pg_catalog.jsonb_build_object('run_line_id',l.run_line_id,'line_no',l.line_no::text,
        'original_timesheet_id',l.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key,
        'component_kind',l.component_kind,'work_date',l.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)
        from private.bpay_next_simple_review_line_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_line_no')::bigint,v_limit) l
        order by l.line_no limit v_limit)
    when 'SHIFTS' then array(
      select pg_catalog.jsonb_build_object('shift_detail_id',s.id,'detail_no',s.detail_no::text,'work_date',s.work_date,
        'shift_start_at',s.shift_start_at,'shift_end_at',s.shift_end_at,'shift_start_local',s.shift_start_local,'shift_end_local',s.shift_end_local,
        'shift_overnight',s.shift_overnight,'submitted_minutes',s.submitted_minutes::text,'approved_minutes',s.approved_minutes::text,
        'approved_hours',s.approved_hours::text,'detail_label',s.detail_label,'segment_pay_ex_vat',s.segment_pay_ex_vat::text,'pay_excluded',s.pay_excluded,
        'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text)
        from private.bpay_next_shift_detail s where s.approved_line_id=v_line and s.detail_no>coalesce((p_args->>'after_detail_no')::bigint,0)
        order by s.detail_no limit v_limit)
    when 'BREAKS' then array(
      select pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at,
        'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)
        from private.bpay_next_break_detail b where b.shift_detail_id=v_shift and b.break_no>coalesce((p_args->>'after_break_no')::bigint,0)
        order by b.break_no limit v_limit)
    when 'RATES' then array(
      select pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text)
        from private.bpay_next_rate_detail r where r.approved_line_id=v_line
          and ((p_args->>'after_bucket') is null or r.bucket>p_args->>'after_bucket')
        order by r.bucket limit v_limit)
  end into v_page;
  foreach v_row in array v_page
  loop
    if pg_catalog.octet_length((v_rows||pg_catalog.jsonb_build_array(v_row))::text)>120000 then
      if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_complete:=false;exit;
    end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);v_n:=v_n+1;
  end loop;
  if v_n=v_limit then v_complete:=false;end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete);
end
$function$;

ALTER FUNCTION public.bpay_next_preparation_page_v1(uuid,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bpay_next_preparation_page_v1(uuid,text,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bpay_next_preparation_page_v1(uuid,text,jsonb) TO service_role;

-- F-05-D17 successor of supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql
create or replace function private.bpay_next_reconcile_work_collection_v1(
  p_job_id uuid,p_work_id uuid,p_component_key text,p_effect_id uuid default null
) returns void
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;v_work private.bpay_next_work%rowtype;
  v_position private.bpay_next_position%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_original_revision private.bpay_next_work_revision%rowtype;
  v_original_approved private.bpay_next_approved_line%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_approved private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_state text;v_qualified boolean;v_capturable boolean;v_supported_paid boolean;
  v_original_detail text;v_whole_zero boolean;v_paired_origin boolean;
  v_principal numeric;v_delta numeric;v_event text;
begin
  if p_job_id is null or p_work_id is null or p_component_key is null
     or pg_catalog.char_length(p_component_key) not between 1 and 256 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORK_COLLECTION_INPUT_INVALID';
  end if;
  -- Parent owners already hold run (when applicable), Candidate control and
  -- job, in that order. These reentrant locks never acquire another run.
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.status<>'LEASED' or v_job.module_epoch<>v_epoch
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id and w.candidate_id=v_candidate;
  select p.* into strict v_position from private.bpay_next_position p
    where p.work_id=p_work_id and p.component_key=p_component_key for update;
  if v_job.job_kind='POSITION_APPLY' then
    if p_effect_id is not null or v_job.phase not in ('NEW','REMOVED') or not exists(
      select 1 from private.bpay_next_publication p where p.command_id=v_job.command_id
        and p.work_id=p_work_id and p.candidate_id=v_candidate and p.status in ('QUEUED','APPLYING')
        and p.revision_id=v_position.applied_revision_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_POSITION_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
    if p_effect_id is null or v_job.phase<>'MEMBERS' then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SETTLEMENT_SCOPE_INVALID';
    end if;
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_effect_id;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      -- The negative WORK disposition is exactly the genuine posted CASE
      -- recovery, never a caller supplied negative or a cash-return inverse.
      if not exists(select 1 from private.bpay_next_work_collection b
        join private.bpay_next_case_event ce on ce.id=v_effect.operation_item_id and ce.case_id=b.case_id
        join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=b.id
        join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
        join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
        join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=ce.original_transfer_id
        where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
          and ce.operation_id=v_job.command_id and ce.event_kind='RECOVERED'
          and ce.case_component_id=b.case_component_id and ce.source_amount_ex_vat>0
          and v_effect.operation_id=v_job.command_id and v_effect.work_id=b.work_id
          and v_effect.component_key=b.component_key and v_effect.candidate_id=b.candidate_id
          and v_effect.case_id=b.case_id and v_effect.original_transfer_id=ce.original_transfer_id
          and (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
            is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
      end if;
      v_capturable:=false;v_supported_paid:=true;
    else
    select l.* into strict v_line from private.bpay_next_hold h
      join private.bpay_next_run_line l on l.id=h.run_line_id
      join private.bpay_next_outcome_request r on r.command_id=v_job.command_id
      join private.bpay_next_transfer t on t.id=r.transfer_id
      where h.id=v_effect.operation_item_id and h.status='REALISED'
        and l.run_worker_id=r.run_worker_id and l.work_id=p_work_id and l.component_key=p_component_key
        and t.id=v_effect.original_transfer_id and t.candidate_id=v_candidate
        and t.original_transfer_id is null and t.return_cash_id is null
        and ((v_job.job_kind='CSV_SETTLEMENT' and t.execution_kind='BANK' and t.status in ('SETTLED','RETURNED'))
          or (v_job.job_kind='INTERNAL_SETTLEMENT' and t.execution_kind='INTERNAL_ZERO' and t.status='INTERNAL_PROCESSING'));
    if v_effect.operation_id<>v_job.command_id or v_effect.effect_kind<>'PAYROLL_SETTLED'
       or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id)
         is distinct from (p_work_id,p_component_key,v_candidate)
       or v_effect.source_disposition_ex_vat<>v_line.source_consumed_ex_vat
       or v_effect.source_disposition_ex_vat<=0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EFFECT_SCOPE_INVALID';
    end if;
    select a.* into strict v_approved from private.bpay_next_approved_line a where a.id=v_line.approved_line_id;
    -- Original ordinary or genuine Source PAYE -> PAYE WORK only. NULL
    -- tax is capturable as exact UNBOUND evidence, never inferred taxable.
    -- Do not return before checking an existing link: a later unsupported
    -- actual posting invalidates its paid qualification, not the payment.
    v_capturable:=v_work.work_kind='ORDINARY' and v_approved.component_kind='WORK'
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
    -- PROTECTED is admitted only for 1550's actual committed-head branch;
    -- 0345's independent/headless protected target is not this authority.
    v_capturable:=v_capturable or (v_work.work_kind='SOURCE' and v_approved.component_kind='WORK'
      and v_approved.source_component_id is not null
      and v_approved.component_key='SOURCE:'||v_approved.source_component_id::text
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.work_id=v_work.id
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))));
    v_supported_paid:=v_capturable and v_approved.tax_treatment is not distinct from 'TAXABLE';
    end if;
  elsif v_job.job_kind='SIMPLE_CANCEL' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      join private.bpay_next_case_cancel_binding cb on cb.command_id=v_job.command_id and cb.candidate_id=b.candidate_id
      join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
      join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
      join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
      join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=b.id
      join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and cb.status='CANCELLING' and cb.stage='CASE'
        and i.case_id=b.case_id and i.case_component_id=b.case_component_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CANCEL_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARATION_EXPIRY' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_expired_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  else
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_JOB_KIND_INVALID';
  end if;
  select b.* into v_basis from private.bpay_next_work_collection b
    where b.work_id=p_work_id and b.component_key=p_component_key for update;
  if not found then
    -- An unpaid hold is not realised money. Existing pre-install paid rows
    -- have no invented basis/backfill; future actual settlement captures it.
    if p_effect_id is null or not v_capturable then return;end if;
    insert into private.bpay_next_work_collection
      (work_id,component_key,candidate_id,original_effect_id,original_run_line_id,
       original_revision_id,original_approved_line_id,original_transfer_id,
       original_source_pay_channel,original_tax_treatment,paid_basis_qualified,original_valuation_policy_id,
       qualification_state,observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat,last_job_id,last_effect_id)
      values(p_work_id,p_component_key,v_candidate,p_effect_id,v_line.id,v_line.captured_revision_id,
        v_line.approved_line_id,v_effect.original_transfer_id,v_line.source_pay_channel,v_approved.tax_treatment,
        v_approved.tax_treatment is not distinct from 'TAXABLE'
          and v_position.realised_source_ex_vat=v_effect.source_disposition_ex_vat,v_line.valuation_policy_id,
        'CURRENT_APPROVAL_UNBOUND',v_position.applied_revision_id,v_position.approved_source_ex_vat,
        v_position.realised_source_ex_vat,p_job_id,p_effect_id) returning * into v_basis;
  elsif v_basis.last_job_id=p_job_id and
      not (p_effect_id is not null and v_effect.effect_kind='WORK_COLLECTION_RECOVERED'
        and v_basis.last_effect_id is distinct from p_effect_id) then
    if (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat,v_basis.last_effect_id)
       is distinct from (v_position.applied_revision_id,v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,p_effect_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SAME_JOB_CHANGED';
    end if;
    return;
  end if;
  v_qualified:=v_basis.paid_basis_qualified;
  if p_effect_id is not null and not v_supported_paid then v_qualified:=false;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_position.applied_revision_id;
  select r.detail_kind into strict v_original_detail from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select r.* into strict v_original_revision from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select a.* into strict v_original_approved from private.bpay_next_approved_line a
    where a.id=v_basis.original_approved_line_id and a.revision_id=v_original_revision.id;
  v_paired_origin:=(v_work.work_kind='ORDINARY' and v_original_revision.source_kind='ORDINARY'
      and v_revision.source_kind='ORDINARY')
    or (v_work.work_kind='SOURCE' and v_original_approved.component_kind='WORK'
      and v_original_approved.source_component_id is not null
      and p_component_key='SOURCE:'||v_original_approved.source_component_id::text
      and ((v_original_revision.source_kind='SOURCE' and v_original_revision.source_event_id is not null
        and (v_original_revision.source_head_id is null
        or v_original_revision.source_event_id=v_original_revision.source_head_id)) or (v_original_revision.source_kind='PROTECTED'
        and v_original_revision.source_head_id is not null
        and v_original_revision.source_event_id=v_original_revision.source_head_id))
      and ((v_revision.source_kind='SOURCE' and v_revision.source_event_id is not null
        and (v_revision.source_head_id is null
        or v_revision.source_event_id=v_revision.source_head_id)) or (v_revision.source_kind='PROTECTED'
        and v_revision.source_head_id is not null and v_revision.source_event_id=v_revision.source_head_id)));
  -- Absence of a DAY key does not prove removal: an AGGREGATE or another
  -- business-date component may retain the entitlement. This first slice
  -- qualifies unchanged typed identity only. A genuinely sealed, certified
  -- whole-base empty zero is the sole missing-key exception; reconciliation
  -- across representations remains explicitly UNBOUND, not discarded.
  v_whole_zero:=v_revision.sealed_at_utc is not null and v_revision.certified_zero
    and v_revision.approved_source_ex_vat=0 and v_revision.expected_line_count=0;
  if v_basis.original_tax_treatment is distinct from 'TAXABLE' then v_state:='ORIGINAL_TAX_UNBOUND';
  elsif not v_qualified then v_state:='PAID_BASIS_UNBOUND';
  elsif v_job.job_kind not in ('SIMPLE_CANCEL','PREPARATION_EXPIRY') and v_effect.effect_kind is distinct from 'WORK_COLLECTION_RECOVERED'
      and (v_work.approval_state<>'APPROVED'
        or v_work.current_revision_id is distinct from v_position.applied_revision_id) then
    v_state:='CURRENT_APPROVAL_UNBOUND';
  elsif v_paired_origin is not true or v_revision.source_pay_channel<>'PAYE'
      or v_position.source_basis_channel is distinct from 'PAYE'
      or (not v_whole_zero and (v_revision.detail_kind<>v_original_detail
        or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_key=p_component_key
            and a.component_kind='WORK' and a.tax_treatment='TAXABLE'
            and (v_work.work_kind='ORDINARY'
              or a.source_component_id=v_original_approved.source_component_id)))) then
    v_state:='CURRENT_SOURCE_UNBOUND';
  elsif v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then v_state:='SIGNED_SOURCE_UNBOUND';
  else v_state:='READY';end if;
  if v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists (
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=v.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=v.decision_bundle_id
        and bundle.bundle_revision=v.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=v.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=v.publication_command_id_1
        and publication.work_id=v.approval_work_id and publication.revision_id=v.approval_revision_id
        and publication.candidate_id=v_candidate
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and v.approval_work_id=p_work_id and v.approval_component_key=p_component_key
        and v.approval_revision_id=v_position.applied_revision_id
        and v.head_id=v_revision.source_head_id and v_revision.source_event_id=v.head_id
        and v.publication_command_id_2 is null and v.candidate_id=v_candidate
        and v.approval_source_channel='PAYE' and v.tax_treatment='TAXABLE'
        and receipt.decision_bundle_id=v.decision_bundle_id and receipt.bundle_revision=v.bundle_revision
        and receipt.request_digest=v.request_digest and pg_catalog.cardinality(receipt.head_ids)=1
        and receipt.head_ids[1]=v.head_id and v_work.current_revision_id=v.approval_revision_id
        and ((v_job.job_kind='POSITION_APPLY' and publication.command_id=v_job.command_id
              and publication.status in ('QUEUED','APPLYING'))
          or (v_job.job_kind<>'POSITION_APPLY' and publication.status='APPLIED' and publication.phase='DONE'))
  ) then v_state:='CURRENT_SOURCE_UNBOUND';end if;
  if v_state='READY' then
    if v_basis.case_id is null then
      v_principal:=greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0);
      if v_principal>0 then
        insert into private.bpay_next_finance_case
          (id,candidate_id,case_kind,tax_treatment,status,principal_approved,order_key,case_revision)
          values(v_basis.id,v_candidate,'OVERPAYMENT','TAXABLE','OPEN',v_principal,v_job.command_sequence,1);
        insert into private.bpay_next_case_rule
          (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc)
          values(v_basis.id,v_basis.id,v_candidate,1,'OVERPAYMENT','OVERPAYMENT','TAXABLE',pg_catalog.transaction_timestamp());
        update private.bpay_next_finance_case set current_rule_id=v_basis.id,current_rule_revision=1 where id=v_basis.id;
        insert into private.bpay_next_case_component
          (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,
           source_pay_channel,currency,approved_source_ex_vat,resolution_state)
          values(v_basis.id,v_basis.id,v_candidate,'RECOVERY',1,1,v_basis.id,
            'OVERPAYMENT','OVERPAYMENT','TAXABLE','RECOVERY','DEDUCTION','GROSS_DEDUCT','PAYE','GBP',v_principal,'REVIEW');
        v_basis.case_id:=v_basis.id;v_basis.case_component_id:=v_basis.id;v_delta:=v_principal;v_event:='APPROVED';
      end if;
    else
      select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_basis.case_id for update;
      select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_basis.case_component_id for update;
      if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
           v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BALANCE_MISMATCH';
      end if;
      -- R is actual SOURCE disposition, never held money/bank cash. Genuine
      -- future recovery must lower R with its own signed effect. Forgiveness
      -- W is separate; protected H is not repriced or released by an approval.
      v_principal:=v_case.principal_recovered+v_case.principal_written_off+
        greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-v_case.principal_written_off,
          v_case.active_recovery_hold_amount,0);
      v_delta:=v_principal-v_case.principal_approved;
      if v_delta<>0 then
        update private.bpay_next_finance_case set principal_approved=v_principal,
          status=case when v_principal=principal_recovered+principal_written_off then 'CLOSED'
            when status='PAUSED' then 'PAUSED' else 'OPEN' end,
          case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
        update private.bpay_next_case_component set approved_source_ex_vat=v_principal,
          component_revision=component_revision+1,resolution_state='REVIEW',
          target_pay_channel=null,target_ex_vat=null,target_vat=null,target_inc_vat=null,
          valuation_policy_id=null,valuation_window_id=null,resolution_ref=null,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_component_id;
        v_event:='CORRECTED';
      elsif v_principal=v_case.principal_recovered+v_case.principal_written_off and v_case.status<>'CLOSED' then
        update private.bpay_next_finance_case set status='CLOSED',case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
      end if;
    end if;
    if v_event is not null then
      insert into private.bpay_next_case_event
        (case_id,operation_id,operation_item_id,event_kind,approved_delta,original_transfer_id,original_effect_id)
        values(v_basis.case_id,v_job.command_id,v_basis.id,v_event,v_delta,
          v_basis.original_transfer_id,v_basis.original_effect_id);
    end if;
  end if;
  -- First insert records exact basis; this single follow-up records the
  -- observation/case under the same actual owner transaction. No view counter
  -- is independently incremented: existing position/posting owners do that.
  update private.bpay_next_work_collection set case_id=v_basis.case_id,case_component_id=v_basis.case_component_id,
    paid_basis_qualified=v_qualified,qualification_state=v_state,observed_revision_id=v_position.applied_revision_id,
    observed_approved_source_ex_vat=v_position.approved_source_ex_vat,
    observed_realised_source_ex_vat=v_position.realised_source_ex_vat,
    reconcile_revision=reconcile_revision+1,last_job_id=p_job_id,last_effect_id=p_effect_id,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_basis.id;
end
$function$;

ALTER FUNCTION private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) FROM PUBLIC, anon, authenticated, service_role;

-- F-05-D17 successor of supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql
create or replace function private.bpay_next_apply_work_collection_recovery_v1(p_job_id uuid,p_case_event_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_event private.bpay_next_case_event%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_effect private.bpay_next_financial_effect%rowtype;v_effect_id uuid;
  v_source_line private.bpay_next_run_line%rowtype;
begin
  select * into strict v_job from private.bpay_next_job where id=p_job_id;
  select * into strict v_event from private.bpay_next_case_event where id=p_case_event_id;
  select * into strict v_instruction from private.bpay_next_run_case_instruction where id=v_event.instruction_id;
  select * into strict v_basis from private.bpay_next_work_collection where id=v_instruction.work_collection_id;
  if v_job.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or v_job.phase<>'MEMBERS' or v_job.status<>'LEASED'
     or v_job.candidate_id<>v_basis.candidate_id or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or (v_event.operation_id,v_event.case_id,v_event.case_component_id,v_event.candidate_id)
       is distinct from (v_job.command_id,v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id)
     or v_event.event_kind<>'RECOVERED' or v_event.source_amount_ex_vat<=0
     or v_instruction.valuation_policy_id<>v_basis.original_valuation_policy_id
     or (v_event.source_amount_ex_vat,v_event.target_amount_ex_vat,v_event.target_amount_vat,v_event.target_amount_inc_vat)
       is distinct from (v_event.recovered_delta,v_event.recovered_delta,0::numeric,v_event.recovered_delta)
     or not exists(select 1 from private.bpay_next_module_control m
       join private.bpay_next_worker_control c on c.candidate_id=v_basis.candidate_id
       where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_job.module_epoch and c.active_owner_epoch=v_job.owner_epoch)
     or not exists(select 1 from private.bpay_next_case_hold h
       join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=v_event.id
       join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=v_event.original_transfer_id
       join private.bpay_next_transfer_member member on member.transfer_id=r.transfer_id and member.case_hold_id=h.id
         and member.case_instruction_id=v_instruction.id and member.case_allocation_result_id=v_event.allocation_result_id
       where h.id=v_event.operation_item_id and h.status='REALISED' and h.instruction_id=v_instruction.id
         and h.source_reserved_ex_vat=v_event.source_amount_ex_vat and h.run_worker_id=r.run_worker_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
  end if;
  select * into strict v_source_line from private.bpay_next_run_line where id=v_basis.original_run_line_id;
  insert into private.bpay_next_financial_effect(operation_id,operation_item_id,effect_kind,candidate_id,
    work_id,component_key,case_id,original_timesheet_id,original_transfer_id,source_disposition_ex_vat,
    target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,occurred_at_utc,source_run_line_id,
    source_binding_version_id,canonical_source_work_id,canonical_source_component_key,native_source_channel,source_case_event_id)
    select v_job.command_id,v_event.id,'WORK_COLLECTION_RECOVERED',v_basis.candidate_id,
      v_basis.work_id,v_basis.component_key,v_basis.case_id,w.original_timesheet_id,v_event.original_transfer_id,
      -v_event.source_amount_ex_vat,-v_event.target_amount_ex_vat,-v_event.target_amount_vat,-v_event.target_amount_inc_vat,v_event.occurred_at_utc,
      case when v_source_line.source_binding_version_id is null then null else v_source_line.id end,
      v_source_line.source_binding_version_id,v_source_line.canonical_source_work_id,v_source_line.canonical_source_component_key,
      case when v_source_line.source_binding_version_id is null then null else v_source_line.source_pay_channel end,
      case when v_source_line.source_binding_version_id is null then null else v_event.id end
      from private.bpay_next_work w where w.id=v_basis.work_id
    on conflict(operation_id,operation_item_id,effect_kind) do nothing returning id into v_effect_id;
  if v_effect_id is null then
    select * into strict v_effect from private.bpay_next_financial_effect where operation_id=v_job.command_id
      and operation_item_id=v_event.id and effect_kind='WORK_COLLECTION_RECOVERED';
    if (v_effect.candidate_id,v_effect.work_id,v_effect.component_key,v_effect.case_id,v_effect.original_transfer_id,
        v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
      is distinct from (v_basis.candidate_id,v_basis.work_id,v_basis.component_key,v_basis.case_id,v_event.original_transfer_id,
        -v_event.source_amount_ex_vat,-v_event.target_amount_ex_vat,-v_event.target_amount_vat,-v_event.target_amount_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_REPLAY_CONFLICT';
    end if;
    if (v_effect.source_run_line_id,v_effect.source_binding_version_id,v_effect.canonical_source_work_id,
        v_effect.canonical_source_component_key,v_effect.native_source_channel,v_effect.source_case_event_id)
      is distinct from (case when v_source_line.source_binding_version_id is null then null else v_source_line.id end,
        v_source_line.source_binding_version_id,v_source_line.canonical_source_work_id,v_source_line.canonical_source_component_key,
        case when v_source_line.source_binding_version_id is null then null else v_source_line.source_pay_channel end,
        case when v_source_line.source_binding_version_id is null then null else v_event.id end) then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_EFFECT_REPLAY_CONFLICT';
    end if;
    return;
  end if;
  update private.bpay_next_position set realised_source_ex_vat=realised_source_ex_vat-v_event.source_amount_ex_vat,
    realised_target_ex_vat=realised_target_ex_vat-v_event.target_amount_ex_vat,
    realised_target_vat=realised_target_vat-v_event.target_amount_vat,
    realised_target_inc_vat=realised_target_inc_vat-v_event.target_amount_inc_vat,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where work_id=v_basis.work_id and component_key=v_basis.component_key and source_basis_channel='PAYE'
      and realised_source_ex_vat>=v_event.source_amount_ex_vat
      and (realised_target_ex_vat,realised_target_vat,realised_target_inc_vat)
        is not distinct from (realised_source_ex_vat,0::numeric,realised_source_ex_vat);
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_POSITION_INVALID';end if;
  perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_basis.work_id,v_basis.component_key,v_effect_id);
end
$function$;

ALTER FUNCTION private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;

notify pgrst, 'reload schema';
commit;
