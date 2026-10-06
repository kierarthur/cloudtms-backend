-- Capture the exact newly authorised ordinary or independent expense-carrier
-- financial row. The trigger is dormant until the replacement is the exclusive
-- financial owner. Source HOURS have a separate Source-owned producer.
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
  select count(*) into v_schedule_count
    from private.bpay_next_contract_rate_rows_v1(
      v_contract.rates_json,v_contract.bucket_labels_json,
      v_contract.additional_rates_json,
      v_mileage_pay_rate,v_mileage_charge_rate);

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
  insert into private.bpay_next_rate_schedule
    (revision_id,rate_family,rate_code,unit_label,
     paye_rate,umbrella_rate,charge_rate)
    select v_revision_id,r.rate_family,r.rate_code,r.unit_label,
           r.paye_rate,r.umbrella_rate,r.charge_rate
    from private.bpay_next_contract_rate_rows_v1(
      v_contract.rates_json,v_contract.bucket_labels_json,
      v_contract.additional_rates_json,
      v_mileage_pay_rate,v_mileage_charge_rate) r;

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
    if v_route in ('ORDINARY_HOURS','EXPENSE_CARRIER') then
      perform private.bpay_next_publish_ordinary_from_financial_v1(new.id);
    end if;
  else
    -- Unauthorisation changes what may be offered next; it does not erase a
    -- revision already captured into a frozen offer or rewrite paid history.
    select t.booking_id,t.contract_id,c.candidate_id
      into strict v_booking_id,v_contract_id,v_candidate_id
      from public.timesheets t
      join public.contracts c on c.id=t.contract_id
      where t.timesheet_id=new.timesheet_id;
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
    select r.physical_timesheet_id into strict v_physical_timesheet_id
      from private.bpay_next_work_revision r
      where r.id=v_work.current_revision_id and r.work_id=v_work.id;
    if v_work.candidate_id<>v_candidate_id
       or v_work.contract_id<>v_contract_id
       or v_work.work_kind not in ('EXPENSE','ORDINARY')
       or v_work.approval_state<>'APPROVED'
       or v_physical_timesheet_id<>new.timesheet_id then
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
  end if;
  return new;
end
$function$;

drop trigger if exists bpay_next_ordinary_authorisation_v1
  on public.timesheets_financials;
create trigger bpay_next_ordinary_authorisation_v1
  after insert or update of authorised_at_utc on public.timesheets_financials
  for each row execute function private.bpay_next_ordinary_authorisation_trigger_v1();

alter function private.bpay_next_publish_ordinary_from_financial_v1(uuid)
  owner to postgres;
alter function private.bpay_next_ordinary_authorisation_trigger_v1()
  owner to postgres;
revoke all on function private.bpay_next_publish_ordinary_from_financial_v1(uuid)
  from public, anon, authenticated, service_role;
revoke all on function private.bpay_next_ordinary_authorisation_trigger_v1()
  from public, anon, authenticated, service_role;

commit;
