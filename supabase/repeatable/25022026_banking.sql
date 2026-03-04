CREATE OR REPLACE FUNCTION public.banking_get_capabilities()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text;
  v_env text;

  v_supports_scheduling boolean;
  v_supports_name_check boolean;
  v_supports_auto_execute boolean;

  v_supports_csv_confirm boolean;

  -- ✅ C: include default funding account in capabilities output
  v_rail_default_funding_account_ref text;

  -- ✅ NEW: payroll testing flag (simulate payments; no real bank payments)
  v_payroll_testing boolean;

  -- ✅ NEW: PAYE remittances gate
  v_paye_remittances_enabled boolean;
begin
  -- settings_defaults is expected to have a single row; do not assume an id column.
  select
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_scheduling,
    sd.rail_supports_name_check,
    sd.rail_supports_auto_execute,
    sd.rail_default_funding_account_ref,
    sd.payroll_testing,
    sd.paye_remittances_enabled
  into
    v_provider,
    v_env,
    v_supports_scheduling,
    v_supports_name_check,
    v_supports_auto_execute,
    v_rail_default_funding_account_ref,
    v_payroll_testing,
    v_paye_remittances_enabled
  from public.settings_defaults sd
  limit 1;

  v_provider := upper(btrim(coalesce(v_provider, 'CSV')));
  v_env := upper(btrim(coalesce(v_env, 'PROD')));

  v_supports_scheduling := coalesce(v_supports_scheduling, false);
  v_supports_name_check := coalesce(v_supports_name_check, false);
  v_supports_auto_execute := coalesce(v_supports_auto_execute, false);

  v_payroll_testing := coalesce(v_payroll_testing, false);
  v_paye_remittances_enabled := coalesce(v_paye_remittances_enabled, false);

  -- CSV rail implies manual bank confirmation (upload + confirm).
  v_supports_csv_confirm := (v_provider = 'CSV');

  return jsonb_build_object(
    'rail_provider', v_provider,
    'rail_env', v_env,
    'supports_scheduling', v_supports_scheduling,
    'supports_name_check', v_supports_name_check,
    'supports_auto_execute', v_supports_auto_execute,
    'supports_csv_confirm', v_supports_csv_confirm,
    'requires_manual_bank_confirm', v_supports_csv_confirm,

    -- ✅ NEW: surface test-mode switch for UI/backend consistency
    'payroll_testing', v_payroll_testing,

    -- ✅ NEW: PAYE remittance gate (UI can block PAYE remittance send)
    'paye_remittances_enabled', v_paye_remittances_enabled,

    -- ✅ C: surface saved default so UI can preselect consistently
    'rail_default_funding_account_ref', v_rail_default_funding_account_ref
  );
end;
$function$;

CREATE OR REPLACE FUNCTION public._pay_timesheet_components(p_snapshot_json jsonb)
RETURNS TABLE (
  key_type text,
  key_value text,
  amount_ex_vat numeric,
  amount_inc_vat numeric
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $$
with
seg_raw as (
  select
    nullif(btrim(coalesce(seg->>'segment_id','')), '') as segment_id,
    nullif(btrim(coalesce(seg->>'date','')), '') as seg_date_raw,
    coalesce(nullif(seg->>'exclude_from_pay','')::boolean, false) as exclude_from_pay,
    case
      when coalesce(seg->>'pay_amount','') ~ '^-?\d+(\.\d+)?$' then (seg->>'pay_amount')::numeric
      else 0::numeric
    end as pay_amount_ex
  from jsonb_array_elements(coalesce(p_snapshot_json->'segments','[]'::jsonb)) as seg
  where seg is not null and jsonb_typeof(seg) = 'object'
),
seg_norm as (
  select
    case
      when sr.seg_date_raw ~ '^\d{4}-\d{2}-\d{2}$' then 'TS_DAY'
      else 'TS_TOTAL'
    end as key_type,
    case
      when sr.seg_date_raw ~ '^\d{4}-\d{2}-\d{2}$' then sr.seg_date_raw
      else 'TOTAL'
    end as key_value,
    round(
      sum(
        case
          when sr.exclude_from_pay then 0::numeric
          else coalesce(sr.pay_amount_ex,0)
        end
      ),
      2
    ) as amount_ex_vat
  from seg_raw sr
  where sr.segment_id is not null
  group by 1,2
),
add_kv as (
  select
    upper(btrim(e.key)) as code,
    e.value as obj
  from jsonb_each(coalesce(p_snapshot_json->'additional_units_json','{}'::jsonb)) as e
  where e.key is not null
    and btrim(e.key) <> ''
    and e.value is not null
    and jsonb_typeof(e.value) = 'object'
),
add_by_code as (
  select
    'ADDITIONAL_CODE'::text as key_type,
    ak.code as key_value,
    round(
      coalesce(
        case when coalesce(ak.obj->>'pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'pay_ex_vat')::numeric end,
        case when coalesce(ak.obj->>'amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'amount_ex_vat')::numeric end,
        (
          coalesce(
            case when coalesce(ak.obj->>'unit_count','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'unit_count')::numeric end,
            case when coalesce(ak.obj->>'units_week','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'units_week')::numeric end,
            0::numeric
          )
          *
          coalesce(
            case when coalesce(ak.obj->>'pay_rate','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'pay_rate')::numeric end,
            case when coalesce(ak.obj->>'rate','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'rate')::numeric end,
            0::numeric
          )
        ),
        0::numeric
      ),
      2
    ) as amount_ex_vat
  from add_kv ak
),
add_sum as (
  select
    round(coalesce(sum(abc.amount_ex_vat),0),2) as sum_ex
  from add_by_code abc
),
add_total_fallback as (
  select
    'ADDITIONAL_CODE'::text as key_type,
    'TOTAL'::text as key_value,
    round(
      case
        when coalesce(p_snapshot_json->>'additional_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (p_snapshot_json->>'additional_pay_ex_vat')::numeric
        else 0::numeric
      end,
      2
    ) as amount_ex_vat
  where (select coalesce(a.sum_ex,0) from add_sum a) = 0
    and (
      coalesce(p_snapshot_json->>'additional_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
      and (p_snapshot_json->>'additional_pay_ex_vat')::numeric <> 0
    )
),
exp_vals as (
  select
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,travel_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,travel_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as travel_ex,
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,accommodation_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,accommodation_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as accom_ex,
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,other_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,other_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as other_ex,
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,mileage_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,mileage_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as mileage_ex,
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,expenses_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,expenses_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as expenses_rollup_ex
),
exp_has_cats as (
  select
    (coalesce(ev.travel_ex,0) <> 0)
    or (coalesce(ev.accom_ex,0) <> 0)
    or (coalesce(ev.other_ex,0) <> 0)
    or (coalesce(ev.mileage_ex,0) <> 0) as has_any_cat
  from exp_vals ev
),
exp_components as (
  select 'EXPENSE_CODE'::text as key_type, 'TRAVEL'::text as key_value, ev.travel_ex as amount_ex_vat
  from exp_vals ev
  where coalesce(ev.travel_ex,0) <> 0
  union all
  select 'EXPENSE_CODE'::text, 'ACCOMMODATION'::text, ev.accom_ex
  from exp_vals ev
  where coalesce(ev.accom_ex,0) <> 0
  union all
  select 'EXPENSE_CODE'::text, 'OTHER'::text, ev.other_ex
  from exp_vals ev
  where coalesce(ev.other_ex,0) <> 0
  union all
  select 'EXPENSE_CODE'::text, 'MILEAGE'::text, ev.mileage_ex
  from exp_vals ev
  where coalesce(ev.mileage_ex,0) <> 0
  union all
  select 'EXPENSE_CODE'::text, 'EXPENSES'::text, ev.expenses_rollup_ex
  from exp_vals ev
  where (select eh.has_any_cat from exp_has_cats eh) = false
    and coalesce(ev.expenses_rollup_ex,0) <> 0
),
adj_components as (
  select
    'EXPENSE_CODE'::text as key_type,
    upper('ADJ:' || nullif(btrim(coalesce(adj->>'id','')), '')) as key_value,
    round(
      case
        when coalesce(adj->>'delta_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (adj->>'delta_pay_ex_vat')::numeric
        else 0::numeric
      end,
      2
    ) as amount_ex_vat
  from jsonb_array_elements(coalesce(p_snapshot_json->'adjustments','[]'::jsonb)) as adj
  where adj is not null
    and jsonb_typeof(adj)='object'
    and nullif(btrim(coalesce(adj->>'id','')), '') is not null
    and (
      coalesce(adj->>'delta_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
      and (adj->>'delta_pay_ex_vat')::numeric <> 0
    )
)
select
  s.key_type,
  s.key_value,
  s.amount_ex_vat,
  s.amount_ex_vat as amount_inc_vat
from seg_norm s

union all
select
  abc.key_type,
  abc.key_value,
  abc.amount_ex_vat,
  abc.amount_ex_vat
from add_by_code abc
where abc.amount_ex_vat <> 0

union all
select
  atf.key_type,
  atf.key_value,
  atf.amount_ex_vat,
  atf.amount_ex_vat
from add_total_fallback atf

union all
select
  ec.key_type,
  ec.key_value,
  ec.amount_ex_vat,
  ec.amount_ex_vat
from exp_components ec

union all
select
  ac.key_type,
  ac.key_value,
  ac.amount_ex_vat,
  ac.amount_ex_vat
from adj_components ac;
$$;


CREATE OR REPLACE FUNCTION public._pay_reserved_components(p_timesheet_ids uuid[])
RETURNS TABLE (
  timesheet_id uuid,
  key_type text,
  key_value text,
  amount_ex_vat numeric,
  amount_inc_vat numeric
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $$
with
inp as (
  select coalesce(
    (select array_agg(distinct x) from unnest(coalesce(p_timesheet_ids, array[]::uuid[])) as t(x) where x is not null),
    array[]::uuid[]
  ) as ts_ids
),
active_items as (
  select
    pb_r.id as pay_batch_id,
    pbi.timesheet_id as timesheet_id,
    pbc_r.candidate_id as candidate_id,
    pbi.item_type as item_type,
    pbi.segment_key as segment_key,
    pbi.source_ref as source_ref,
    coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0)::numeric as amount_ex_vat,
    coalesce(pbi.amount_inc_vat, pbi.amount_ex_vat, 0)::numeric as amount_inc_vat
  from inp i
  join public.pay_batch_items pbi
    on pbi.timesheet_id = any(i.ts_ids)
  join public.pay_batch_candidates pbc_r
    on pbc_r.id = pbi.pay_batch_candidate_id
  join public.pay_batches pb_r
    on pb_r.id = pbc_r.pay_batch_id
  where pbi.timesheet_id is not null
    and pbi.pay_channel in ('PAYE','UMBRELLA')
    and upper(coalesce(pb_r.status,'')) in (
      'DRAFT',
      'DRAFT_CREATED',
      'READY',
      'WAITING_BANK_CONFIRM',
      'PARTIAL',
      'FAILED',
      'BLOCKED_FUNDS',
      'SCHEDULED',
      'EXECUTING',
      'AWAITING_AUTHORISATION',
      'AUTHORISED_FOR_PAYMENT'
    )
    and pbi.item_type not in (
      'DEBT_CREATED',
      'LOAN_REPAYMENT',
      'OVERPAYMENT_RECOVERY',
      'LOAN_PAYOUT'
    )
),
snap_choice as (
  select
    ai.pay_batch_id,
    ai.timesheet_id,
    (
      select pbs1.target_snapshot_json
      from public.pay_batch_timesheet_snapshots pbs1
      where pbs1.pay_batch_id = ai.pay_batch_id
        and pbs1.timesheet_id = ai.timesheet_id
      order by pbs1.created_at_utc desc, pbs1.id desc
      limit 1
    ) as target_snapshot_json
  from (select distinct pay_batch_id, timesheet_id from active_items) ai
),
seg_lookup as (
  select
    ai.pay_batch_id,
    ai.timesheet_id,
    coalesce(
      nullif(btrim(coalesce(ai.segment_key,'')), ''),
      case
        when ai.source_ref is not null and btrim(ai.source_ref) like 'seg:%'
          then nullif(btrim(split_part(ai.source_ref,':',2)), '')
        else null
      end
    ) as seg_id
  from active_items ai
  where ai.item_type = 'SEGMENT_DELTA'
),
seg_date_map as (
  select
    sl.pay_batch_id,
    sl.timesheet_id,
    sl.seg_id,
    nullif(btrim(coalesce(seg->>'date','')), '') as seg_date_raw
  from seg_lookup sl
  join snap_choice sc
    on sc.pay_batch_id = sl.pay_batch_id
   and sc.timesheet_id = sl.timesheet_id
  join lateral jsonb_array_elements(coalesce(sc.target_snapshot_json->'segments','[]'::jsonb)) as seg on true
  where sl.seg_id is not null
    and seg is not null
    and jsonb_typeof(seg)='object'
    and nullif(btrim(coalesce(seg->>'segment_id','')), '') = sl.seg_id
),
seg_date_final as (
  select
    sdm.pay_batch_id,
    sdm.timesheet_id,
    sdm.seg_id,
    case when sdm.seg_date_raw ~ '^\d{4}-\d{2}-\d{2}$' then sdm.seg_date_raw else null end as seg_date
  from seg_date_map sdm
),
reserved_components as (
  select
    ai.timesheet_id,
    case
      when ai.item_type = 'SEGMENT_DELTA'
        then case when sdf.seg_date is not null then 'TS_DAY' else 'TS_TOTAL' end
      when ai.item_type = 'MILEAGE_DELTA'
        then 'EXPENSE_CODE'
      when ai.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
        then case
          when ai.source_ref is not null and (btrim(ai.source_ref) like 'additional:%' or btrim(ai.source_ref) like 'add:%' or btrim(ai.source_ref) = 'additional')
            then 'ADDITIONAL_CODE'
          else 'EXPENSE_CODE'
        end
      else 'EXPENSE_CODE'
    end as key_type,
    case
      when ai.item_type = 'SEGMENT_DELTA'
        then coalesce(sdf.seg_date, 'TOTAL')
      when ai.item_type = 'MILEAGE_DELTA'
        then 'MILEAGE'
      when ai.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
        then case
          when ai.source_ref is not null and (btrim(ai.source_ref) like 'additional:%' or btrim(ai.source_ref) like 'add:%')
            then upper(nullif(btrim(split_part(ai.source_ref,':',2)), ''))
          when ai.source_ref is not null and btrim(ai.source_ref) = 'additional'
            then 'TOTAL'
          when ai.source_ref is not null and btrim(ai.source_ref) <> ''
            then upper(btrim(ai.source_ref))
          else 'UNKNOWN'
        end
      else 'UNKNOWN'
    end as key_value,
    round(sum(coalesce(ai.amount_ex_vat,0)),2) as amount_ex_vat,
    round(sum(coalesce(ai.amount_inc_vat,0)),2) as amount_inc_vat
  from active_items ai
  left join seg_date_final sdf
    on sdf.pay_batch_id = ai.pay_batch_id
   and sdf.timesheet_id = ai.timesheet_id
   and sdf.seg_id = coalesce(
     nullif(btrim(coalesce(ai.segment_key,'')), ''),
     case
       when ai.source_ref is not null and btrim(ai.source_ref) like 'seg:%'
         then nullif(btrim(split_part(ai.source_ref,':',2)), '')
       else null
     end
   )
  where ai.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
  group by
    ai.timesheet_id,
    case
      when ai.item_type = 'SEGMENT_DELTA'
        then case when sdf.seg_date is not null then 'TS_DAY' else 'TS_TOTAL' end
      when ai.item_type = 'MILEAGE_DELTA'
        then 'EXPENSE_CODE'
      when ai.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
        then case
          when ai.source_ref is not null and (btrim(ai.source_ref) like 'additional:%' or btrim(ai.source_ref) like 'add:%' or btrim(ai.source_ref) = 'additional')
            then 'ADDITIONAL_CODE'
          else 'EXPENSE_CODE'
        end
      else 'EXPENSE_CODE'
    end,
    case
      when ai.item_type = 'SEGMENT_DELTA'
        then coalesce(sdf.seg_date, 'TOTAL')
      when ai.item_type = 'MILEAGE_DELTA'
        then 'MILEAGE'
      when ai.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
        then case
          when ai.source_ref is not null and (btrim(ai.source_ref) like 'additional:%' or btrim(ai.source_ref) like 'add:%')
            then upper(nullif(btrim(split_part(ai.source_ref,':',2)), ''))
          when ai.source_ref is not null and btrim(ai.source_ref) = 'additional'
            then 'TOTAL'
          when ai.source_ref is not null and btrim(ai.source_ref) <> ''
            then upper(btrim(ai.source_ref))
          else 'UNKNOWN'
        end
      else 'UNKNOWN'
    end
)
select
  rc.timesheet_id,
  rc.key_type,
  rc.key_value,
  rc.amount_ex_vat,
  rc.amount_inc_vat
from reserved_components rc
where rc.key_value is not null and btrim(rc.key_value) <> '';
$$;


CREATE OR REPLACE FUNCTION public._pay_outstanding_components(p_timesheet_ids uuid[])
RETURNS TABLE (
  timesheet_id uuid,
  key_type text,
  key_value text,
  truth_ex_vat numeric,
  baseline_ex_vat numeric,
  reserved_ex_vat numeric,
  outstanding_ex_vat numeric,
  truth_inc_vat numeric,
  baseline_inc_vat numeric,
  reserved_inc_vat numeric,
  outstanding_inc_vat numeric,
  reservation_overrun_detected boolean
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $$
with
inp as (
  select coalesce(
    (select array_agg(distinct x) from unnest(coalesce(p_timesheet_ids, array[]::uuid[])) as t(x) where x is not null),
    array[]::uuid[]
  ) as ts_ids
),
tf as (
  select
    tfin.timesheet_id,
    tfin.total_pay_ex_vat,
    tfin.invoice_breakdown_json,
    tfin.additional_units_json,
    tfin.expenses_pay_ex_vat,
    tfin.travel_pay_ex_vat,
    tfin.accommodation_pay_ex_vat,
    tfin.other_pay_ex_vat,
    tfin.mileage_pay_ex_vat
  from inp i
  left join public.timesheets_financials tfin
    on tfin.is_current = true
   and tfin.timesheet_id = any(i.ts_ids)
),
truth_segments as (
  select
    tf0.timesheet_id,
    case
      when tf0.invoice_breakdown_json is not null
       and jsonb_typeof(tf0.invoice_breakdown_json)='object'
       and upper(coalesce(tf0.invoice_breakdown_json->>'mode',''))='SEGMENTS'
       and jsonb_typeof(tf0.invoice_breakdown_json->'segments')='array'
      then (
        select coalesce(
          jsonb_agg(seg),
          '[]'::jsonb
        )
        from jsonb_array_elements(tf0.invoice_breakdown_json->'segments') as seg
        where seg is not null and jsonb_typeof(seg)='object'
      )
      else jsonb_build_array(
        jsonb_build_object(
          'segment_id', ('ts:' || tf0.timesheet_id::text),
          'pay_amount', round(coalesce(tf0.total_pay_ex_vat,0),2),
          'exclude_from_pay', false
        )
      )
    end as segments_json
  from tf tf0
),
truth_snapshot_like as (
  select
    ts.timesheet_id,
    jsonb_build_object(
      'segments', ts.segments_json,
      'additional_units_json', coalesce(tf1.additional_units_json,'{}'::jsonb),
      'additional_pay_ex_vat', 0,
      'expenses', jsonb_build_object(
        'expenses_pay_ex_vat', round(coalesce(tf1.expenses_pay_ex_vat,0),2),
        'travel_pay_ex_vat', round(coalesce(tf1.travel_pay_ex_vat,0),2),
        'accommodation_pay_ex_vat', round(coalesce(tf1.accommodation_pay_ex_vat,0),2),
        'other_pay_ex_vat', round(coalesce(tf1.other_pay_ex_vat,0),2),
        'mileage_pay_ex_vat', round(coalesce(tf1.mileage_pay_ex_vat,0),2)
      ),
      'adjustments', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'id', a.id::text,
            'delta_pay_ex_vat', round(coalesce(a.delta_pay_ex_vat,0),2)
          )
          order by a.created_at nulls last, a.id
        )
        from public.ts_pay_adjustments a
        where a.as_advance = false
          and a.timesheet_id = ts.timesheet_id
      ), '[]'::jsonb)
    ) as snap_json
  from truth_segments ts
  join tf tf1
    on tf1.timesheet_id = ts.timesheet_id
),
truth_components as (
  select
    tsl.timesheet_id,
    tc.key_type,
    tc.key_value,
    tc.amount_ex_vat,
    tc.amount_inc_vat
  from truth_snapshot_like tsl
  join lateral public._pay_timesheet_components(tsl.snap_json) as tc on true
),
baseline_components as (
  select
    tps.timesheet_id,
    bc.key_type,
    bc.key_value,
    bc.amount_ex_vat,
    bc.amount_inc_vat
  from inp i
  join public.timesheet_pay_state tps
    on tps.timesheet_id = any(i.ts_ids)
  join lateral public._pay_timesheet_components(coalesce(tps.last_settled_snapshot_json,'{}'::jsonb)) as bc on true
),
reserved_components as (
  select
    rc.timesheet_id,
    rc.key_type,
    rc.key_value,
    rc.amount_ex_vat,
    rc.amount_inc_vat
  from public._pay_reserved_components((select ts_ids from inp)) rc
),
all_keys as (
  select distinct
    x.timesheet_id,
    x.key_type,
    x.key_value
  from (
    select tc.timesheet_id, tc.key_type, tc.key_value from truth_components tc
    union all
    select bc.timesheet_id, bc.key_type, bc.key_value from baseline_components bc
    union all
    select rc.timesheet_id, rc.key_type, rc.key_value from reserved_components rc
  ) x
),
joined as (
  select
    ak.timesheet_id,
    ak.key_type,
    ak.key_value,
    coalesce(tc.amount_ex_vat,0) as truth_ex_vat,
    coalesce(bc.amount_ex_vat,0) as baseline_ex_vat,
    coalesce(rc.amount_ex_vat,0) as reserved_ex_vat,
    coalesce(tc.amount_inc_vat,0) as truth_inc_vat,
    coalesce(bc.amount_inc_vat,0) as baseline_inc_vat,
    coalesce(rc.amount_inc_vat,0) as reserved_inc_vat
  from all_keys ak
  left join truth_components tc
    on tc.timesheet_id = ak.timesheet_id
   and tc.key_type = ak.key_type
   and tc.key_value = ak.key_value
  left join baseline_components bc
    on bc.timesheet_id = ak.timesheet_id
   and bc.key_type = ak.key_type
   and bc.key_value = ak.key_value
  left join reserved_components rc
    on rc.timesheet_id = ak.timesheet_id
   and rc.key_type = ak.key_type
   and rc.key_value = ak.key_value
)
select
  j.timesheet_id,
  j.key_type,
  j.key_value,
  round(j.truth_ex_vat,2) as truth_ex_vat,
  round(j.baseline_ex_vat,2) as baseline_ex_vat,
  round(j.reserved_ex_vat,2) as reserved_ex_vat,
  round(j.truth_ex_vat - j.baseline_ex_vat - j.reserved_ex_vat,2) as outstanding_ex_vat,
  round(j.truth_inc_vat,2) as truth_inc_vat,
  round(j.baseline_inc_vat,2) as baseline_inc_vat,
  round(j.reserved_inc_vat,2) as reserved_inc_vat,
  round(j.truth_inc_vat - j.baseline_inc_vat - j.reserved_inc_vat,2) as outstanding_inc_vat,
  (
    round(j.reserved_ex_vat,2) >
    round(greatest(j.truth_ex_vat - j.baseline_ex_vat, 0),2)
  ) as reservation_overrun_detected
from joined j
where j.timesheet_id is not null
  and j.key_type is not null
  and j.key_value is not null;
$$;
CREATE OR REPLACE FUNCTION public._pay_candidate_week_totals(p_candidate_ids uuid[], p_week_start date)
RETURNS TABLE (
  candidate_id uuid,
  week_start date,
  paid_wtd numeric,
  loan_repaid_wtd numeric,
  overpay_recovered_wtd numeric
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $$
with
inp as (
  select
    coalesce(
      (select array_agg(distinct x) from unnest(coalesce(p_candidate_ids, array[]::uuid[])) as t(x) where x is not null),
      array[]::uuid[]
    ) as cand_ids,
    p_week_start as week_start,
    (p_week_start + interval '6 days')::date as week_end
),
eligible_batches as (
  select
    pb.id as pay_batch_id
  from inp i
  join public.pay_batches pb
    on pb.pay_date >= i.week_start
   and pb.pay_date <= i.week_end
  where upper(coalesce(pb.status,'')) in (
    'DRAFT',
    'DRAFT_CREATED',
    'READY',
    'WAITING_BANK_CONFIRM',
    'PARTIAL',
    'FAILED',
    'BLOCKED_FUNDS',
    'SCHEDULED',
    'EXECUTING',
    'AWAITING_AUTHORISATION',
    'AUTHORISED_FOR_PAYMENT',
    'SETTLED'
  )
    and upper(coalesce(pb.batch_kind_fixed,'')) <> 'LOANS'
),
cand_rows as (
  select
    pbc.candidate_id,
    pbc.pay_batch_id,
    coalesce(pbc.net_bank_amount,0)::numeric as net_bank_amount
  from inp i
  join public.pay_batch_candidates pbc
    on pbc.candidate_id = any(i.cand_ids)
  join eligible_batches eb
    on eb.pay_batch_id = pbc.pay_batch_id
),
paid as (
  select
    cr.candidate_id,
    round(sum(cr.net_bank_amount),2) as paid_wtd
  from cand_rows cr
  group by cr.candidate_id
),
loan_rep as (
  select
    pbc.candidate_id,
    round(sum(abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0))),2) as loan_repaid_wtd
  from inp i
  join public.pay_batch_candidates pbc
    on pbc.candidate_id = any(i.cand_ids)
  join eligible_batches eb
    on eb.pay_batch_id = pbc.pay_batch_id
  join public.pay_batch_items pbi
    on pbi.pay_batch_candidate_id = pbc.id
  where pbi.item_type = 'LOAN_REPAYMENT'
  group by pbc.candidate_id
),
overpay_rec as (
  select
    pbc.candidate_id,
    round(sum(abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0))),2) as overpay_recovered_wtd
  from inp i
  join public.pay_batch_candidates pbc
    on pbc.candidate_id = any(i.cand_ids)
  join eligible_batches eb
    on eb.pay_batch_id = pbc.pay_batch_id
  join public.pay_batch_items pbi
    on pbi.pay_batch_candidate_id = pbc.id
  where pbi.item_type = 'OVERPAYMENT_RECOVERY'
  group by pbc.candidate_id
)
select
  c.id as candidate_id,
  (select week_start from inp) as week_start,
  coalesce(p.paid_wtd,0) as paid_wtd,
  coalesce(l.loan_repaid_wtd,0) as loan_repaid_wtd,
  coalesce(o.overpay_recovered_wtd,0) as overpay_recovered_wtd
from inp i
join public.candidates c
  on c.id = any(i.cand_ids)
left join paid p
  on p.candidate_id = c.id
left join loan_rep l
  on l.candidate_id = c.id
left join overpay_rec o
  on o.candidate_id = c.id;
$$;



CREATE OR REPLACE FUNCTION public.pay_create_draft_batches_split(
  p_pay_date date,
  p_week_ending_cutoff date,
  p_actor_user_id uuid,
  p_preview_decisions_json jsonb,
  p_candidate_id uuid default null,
  p_client_id uuid default null,
  p_force_include_timesheet_ids uuid[] default null,
  p_override_reason text default null,
  p_override_mode public.pay_override_mode_enum default 'NONE'::public.pay_override_mode_enum
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_umbrella_res jsonb;
  v_paye_res jsonb;

  v_umbrella_pay_batch_id uuid;
  v_paye_pay_batch_id uuid;

  v_err text;

  v_override_mode public.pay_override_mode_enum := coalesce(p_override_mode, 'NONE'::public.pay_override_mode_enum);
  v_is_timesheet_advance boolean := (v_override_mode = 'TIMESHEET_ADVANCE'::public.pay_override_mode_enum);

  v_force_include_timesheet_ids uuid[] := array[]::uuid[];
  v_candidate_id_effective uuid;

  v_candidate_ids_from_ts uuid[] := array[]::uuid[];

  v_cand_pay_method text;
  v_cand_umbrella_id uuid;
  v_route_scope text;
begin
  if p_pay_date is null then
    raise exception 'pay_date is required';
  end if;

  if p_week_ending_cutoff is null then
    raise exception 'week_ending_cutoff is required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id is required';
  end if;

  v_force_include_timesheet_ids := coalesce(
    (
      select array_agg(distinct t.x)
      from unnest(coalesce(p_force_include_timesheet_ids, array[]::uuid[])) as t(x)
      where t.x is not null
    ),
    array[]::uuid[]
  );

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCHES_SPLIT:INPUTS',
      jsonb_build_object(
        'pay_date', p_pay_date::text,
        'week_ending_cutoff', p_week_ending_cutoff::text,
        'actor_user_id', p_actor_user_id::text,
        'candidate_id_param', coalesce(p_candidate_id::text, null),
        'client_id_param', coalesce(p_client_id::text, null),
        'override_mode', coalesce(v_override_mode::text, null),
        'override_reason_present', (nullif(btrim(coalesce(p_override_reason,'')), '') is not null),
        'force_include_timesheet_ids', (
          select coalesce(jsonb_agg(x::text order by x::text), '[]'::jsonb)
          from unnest(v_force_include_timesheet_ids) as x
        ),
        'preview_decisions_json_keys_sample', (
          select coalesce(jsonb_agg(k.key order by k.key), '[]'::jsonb)
          from (
            select e.key
            from jsonb_each(coalesce(p_preview_decisions_json,'{}'::jsonb)) e
            order by e.key
            limit 50
          ) k
        )
      ),
      'pay_create_draft_batches_split',
      'pay_date:'||p_pay_date::text,
      null,
      null,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  if v_is_timesheet_advance then
    if coalesce(array_length(v_force_include_timesheet_ids, 1), 0) = 0 then
      raise exception 'TIMESHEET_ADVANCE requires force_include_timesheet_ids';
    end if;

    v_candidate_id_effective := p_candidate_id;

    select array_agg(distinct ct.candidate_id)
      into v_candidate_ids_from_ts
    from public.timesheets ts
    join public.contracts ct
      on ct.id = ts.contract_id
    where ts.timesheet_id = any(v_force_include_timesheet_ids);

    if v_candidate_ids_from_ts is null or coalesce(array_length(v_candidate_ids_from_ts, 1), 0) = 0 then
      raise exception 'force_include_timesheet_ids did not resolve any candidate_id (via timesheets.contract_id -> contracts.candidate_id)';
    end if;

    if coalesce(array_length(v_candidate_ids_from_ts, 1), 0) <> 1 then
      raise exception 'force_include_timesheet_ids span multiple candidate_ids (count=%)', array_length(v_candidate_ids_from_ts, 1);
    end if;

    if v_candidate_ids_from_ts[1] is null then
      raise exception 'force_include_timesheet_ids resolved a NULL candidate_id (contracts.candidate_id is NULL)';
    end if;

    if v_candidate_id_effective is null then
      v_candidate_id_effective := v_candidate_ids_from_ts[1];
    else
      if v_candidate_id_effective <> v_candidate_ids_from_ts[1] then
        raise exception 'candidate_id_param does not match force_include_timesheet_ids candidate_id (param=% vs resolved=%)',
          v_candidate_id_effective::text,
          v_candidate_ids_from_ts[1]::text;
      end if;
    end if;

    select
      upper(btrim(coalesce(ca.pay_method,''))) as cand_pay_method,
      ca.umbrella_id as cand_umbrella_id
      into v_cand_pay_method, v_cand_umbrella_id
    from public.candidates ca
    where ca.id = v_candidate_id_effective
    limit 1;

    if v_cand_pay_method is null or btrim(v_cand_pay_method) = '' then
      raise exception 'candidate pay_method missing for candidate_id=%', v_candidate_id_effective::text;
    end if;

    if v_cand_pay_method not in ('PAYE','UMBRELLA') then
      raise exception 'candidate pay_method invalid (expected PAYE/UMBRELLA) for candidate_id=% (got=%)',
        v_candidate_id_effective::text,
        v_cand_pay_method;
    end if;

    v_route_scope := v_cand_pay_method;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCHES_SPLIT:TS_ADV_ROUTE_SELECTED',
        jsonb_build_object(
          'candidate_id', v_candidate_id_effective::text,
          'cand_pay_method', v_cand_pay_method,
          'cand_umbrella_id', coalesce(v_cand_umbrella_id::text, null),
          'route_scope', v_route_scope,
          'force_include_timesheet_ids', (
            select coalesce(jsonb_agg(x::text order by x::text), '[]'::jsonb)
            from unnest(v_force_include_timesheet_ids) as x
          ),
          'override_reason_present', (nullif(btrim(coalesce(p_override_reason,'')), '') is not null)
        ),
        'pay_create_draft_batches_split',
        'pay_date:'||p_pay_date::text,
        null,
        null,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;

    if v_route_scope = 'UMBRELLA' then
      begin
        v_umbrella_res := public.pay_create_draft_batch(
          p_pay_date,
          p_week_ending_cutoff,
          'UMBRELLA',
          p_actor_user_id,
          p_preview_decisions_json,
          v_candidate_id_effective,
          p_client_id,
          v_force_include_timesheet_ids,
          p_override_reason,
          v_override_mode
        );

        v_umbrella_pay_batch_id := nullif(btrim(coalesce(v_umbrella_res->>'pay_batch_id','')), '')::uuid;
        v_paye_pay_batch_id := null;
      exception
        when others then
          v_err := coalesce(SQLERRM, '');
          if position('Nothing to pay' in v_err) = 1 then
            v_umbrella_pay_batch_id := null;
            v_paye_pay_batch_id := null;
          else
            raise;
          end if;
      end;
    else
      begin
        v_paye_res := public.pay_create_draft_batch(
          p_pay_date,
          p_week_ending_cutoff,
          'PAYE',
          p_actor_user_id,
          p_preview_decisions_json,
          v_candidate_id_effective,
          p_client_id,
          v_force_include_timesheet_ids,
          p_override_reason,
          v_override_mode
        );

        v_paye_pay_batch_id := nullif(btrim(coalesce(v_paye_res->>'pay_batch_id','')), '')::uuid;
        v_umbrella_pay_batch_id := null;
      exception
        when others then
          v_err := coalesce(SQLERRM, '');
          if position('Nothing to pay' in v_err) = 1 then
            v_umbrella_pay_batch_id := null;
            v_paye_pay_batch_id := null;
          else
            raise;
          end if;
      end;
    end if;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCHES_SPLIT:TS_ADV_RESULT',
        jsonb_build_object(
          'route_scope', v_route_scope,
          'umbrella_pay_batch_id', coalesce(v_umbrella_pay_batch_id::text, null),
          'paye_pay_batch_id', coalesce(v_paye_pay_batch_id::text, null)
        ),
        'pay_create_draft_batches_split',
        'pay_date:'||p_pay_date::text,
        null,
        null,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;

  else
    -- Create UMBRELLA draft (READY umbrella items only; enforced by pay_create_draft_batch scope logic)
    begin
      v_umbrella_res := public.pay_create_draft_batch(
        p_pay_date,
        p_week_ending_cutoff,
        'UMBRELLA',
        p_actor_user_id,
        p_preview_decisions_json,
        p_candidate_id,
        p_client_id,
        v_force_include_timesheet_ids,
        p_override_reason,
        v_override_mode
      );

      v_umbrella_pay_batch_id := nullif(btrim(coalesce(v_umbrella_res->>'pay_batch_id','')), '')::uuid;
    exception
      when others then
        v_err := coalesce(SQLERRM, '');
        -- Treat "Nothing to pay ..." as non-fatal: allow PAYE draft to still be created.
        if position('Nothing to pay' in v_err) = 1 then
          v_umbrella_pay_batch_id := null;
        else
          raise;
        end if;
    end;

    -- Create PAYE draft (worksheet batch; net can be set later; pay_create_draft_batch does not require net)
    begin
      v_paye_res := public.pay_create_draft_batch(
        p_pay_date,
        p_week_ending_cutoff,
        'PAYE',
        p_actor_user_id,
        p_preview_decisions_json,
        p_candidate_id,
        p_client_id,
        v_force_include_timesheet_ids,
        p_override_reason,
        v_override_mode
      );

      v_paye_pay_batch_id := nullif(btrim(coalesce(v_paye_res->>'pay_batch_id','')), '')::uuid;
    exception
      when others then
        v_err := coalesce(SQLERRM, '');
        -- Treat "Nothing to pay ..." as non-fatal: allow UMBRELLA draft to still be created.
        if position('Nothing to pay' in v_err) = 1 then
          v_paye_pay_batch_id := null;
        else
          raise;
        end if;
    end;
  end if;

  if v_umbrella_pay_batch_id is null and v_paye_pay_batch_id is null then
    raise exception 'Nothing to pay (no payable items for UMBRELLA or PAYE after readiness blockers)';
  end if;

  return jsonb_build_object(
    'ok', true,
    'umbrella_pay_batch_id', case when v_umbrella_pay_batch_id is null then null else v_umbrella_pay_batch_id::text end,
    'paye_pay_batch_id', case when v_paye_pay_batch_id is null then null else v_paye_pay_batch_id::text end
  );
end;
$$;





CREATE OR REPLACE FUNCTION public.bank_name_check_record_result(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_bank_details_hash text,
  p_status text,
  p_result_json jsonb,
  p_checked_at_utc timestamptz,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));
  v_status text := upper(btrim(coalesce(p_status,'')));

  v_current_hash text;
  v_now timestamptz := now();

  v_inserted boolean := false;

  v_action text := null;

  v_row_json jsonb;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;
  if v_status not in ('UNVERIFIED','PASS','NEAR_MATCH','FAIL','UNAVAILABLE') then
    raise exception '%', jsonb_build_object('error','INVALID_STATUS')::text;
  end if;
  if p_bank_details_hash is null or btrim(p_bank_details_hash) = '' then
    raise exception '%', jsonb_build_object('error','BANK_DETAILS_HASH_REQUIRED')::text;
  end if;

  -- Resolve current bank_details_hash from the entity table (must match to accept the result)
  if v_kind = 'CANDIDATE' then
    select c.bank_details_hash
    into v_current_hash
    from public.candidates c
    where c.id = p_entity_id
    limit 1;
  else
    select u.bank_details_hash
    into v_current_hash
    from public.umbrellas u
    where u.id = p_entity_id
    limit 1;
  end if;

  if v_current_hash is null then
    raise exception '%', jsonb_build_object('error','ENTITY_NOT_FOUND_OR_NO_HASH','entity_kind',v_kind)::text;
  end if;

  -- Stale-result guard (bank details changed while check in-flight)
  if v_current_hash is distinct from btrim(p_bank_details_hash) then
    return jsonb_build_object(
      'ok', true,
      'action', 'ignored_stale_hash',
      'ignored', true,
      'reason', 'STALE_HASH',
      'entity_kind', v_kind,
      'entity_id', p_entity_id::text,
      'current_bank_details_hash', v_current_hash,
      'provided_bank_details_hash', btrim(p_bank_details_hash)
    );
  end if;

  -- ✅ Approach A: single statement consumes the CTE and captures both inserted_flag + row JSON.
  -- IMPORTANT: explicitly type NULLs to avoid any implicit text typing issues for uuid columns.
  with upserted as (
    insert into public.bank_name_checks (
      rail_provider,
      rail_env,
      entity_kind,
      entity_id,
      bank_details_hash,
      status,
      checked_at_utc,
      result_json,
      created_at_utc,
      updated_at_utc,
      override_reason,
      override_by_user_id,
      override_at_utc,
      override_hash
    )
    values (
      v_provider,
      v_env,
      v_kind,
      p_entity_id,
      v_current_hash,
      v_status,
      coalesce(p_checked_at_utc, v_now),
      p_result_json,
      v_now,
      v_now,
      null::text,
      null::uuid,
      null::timestamptz,
      null::text
    )
    on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
    do update set
      status = excluded.status,
      checked_at_utc = excluded.checked_at_utc,
      result_json = excluded.result_json,
      updated_at_utc = v_now,

      override_reason = case
        when excluded.status = 'PASS' then null::text
        else public.bank_name_checks.override_reason
      end,
      override_by_user_id = case
        when excluded.status = 'PASS' then null::uuid
        else public.bank_name_checks.override_by_user_id
      end,
      override_at_utc = case
        when excluded.status = 'PASS' then null::timestamptz
        else public.bank_name_checks.override_at_utc
      end,
      override_hash = case
        when excluded.status = 'PASS' then null::text
        else public.bank_name_checks.override_hash
      end
    returning
      public.bank_name_checks.*,
      (xmax = 0) as inserted_flag
  )
  select
    u.inserted_flag,
    jsonb_build_object(
      'id', u.id::text,
      'rail_provider', u.rail_provider,
      'rail_env', u.rail_env,
      'entity_kind', u.entity_kind,
      'entity_id', u.entity_id::text,
      'bank_details_hash', u.bank_details_hash,
      'status', u.status,
      'checked_at_utc', u.checked_at_utc,
      'result_json', u.result_json,
      'override_reason', u.override_reason,
      'override_by_user_id', case when u.override_by_user_id is null then null else u.override_by_user_id::text end,
      'override_at_utc', u.override_at_utc,
      'override_hash', u.override_hash,
      'created_at_utc', u.created_at_utc,
      'updated_at_utc', u.updated_at_utc
    )
  into
    v_inserted,
    v_row_json
  from upserted u
  limit 1;

  v_action := case when v_inserted then 'inserted' else 'updated' end;

  return jsonb_build_object(
    'ok', true,
    'action', v_action,
    'ignored', false,
    'row', v_row_json
  );
end;
$function$;


CREATE OR REPLACE FUNCTION public.bank_name_check_set_override(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_reason text,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));

  v_reason text := nullif(btrim(coalesce(p_reason,'')), '');
  v_now timestamptz := now();

  v_current_hash text;

  v_before public.bank_name_checks%rowtype;
  v_row public.bank_name_checks%rowtype;

  v_before_json jsonb := null;
  v_after_json jsonb := null;

  v_inserted boolean := false;
  v_action text := null;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;
  if v_reason is null then
    raise exception '%', jsonb_build_object('error','REASON_REQUIRED')::text;
  end if;

  -- Resolve current bank_details_hash (override must apply to the current hash)
  if v_kind = 'CANDIDATE' then
    select c.bank_details_hash
    into v_current_hash
    from public.candidates c
    where c.id = p_entity_id
    limit 1;
  else
    select u.bank_details_hash
    into v_current_hash
    from public.umbrellas u
    where u.id = p_entity_id
    limit 1;
  end if;

  if v_current_hash is null then
    raise exception '%', jsonb_build_object('error','ENTITY_NOT_FOUND_OR_NO_HASH','entity_kind',v_kind)::text;
  end if;

  -- Capture before row (if any)
  select bnc.*
  into v_before
  from public.bank_name_checks bnc
  where bnc.rail_provider = v_provider
    and bnc.rail_env = v_env
    and bnc.entity_kind = v_kind
    and bnc.entity_id = p_entity_id
    and bnc.bank_details_hash = v_current_hash
  limit 1;

  if v_before.id is not null then
    v_before_json := jsonb_build_object(
      'id', v_before.id::text,
      'rail_provider', v_before.rail_provider,
      'rail_env', v_before.rail_env,
      'entity_kind', v_before.entity_kind,
      'entity_id', v_before.entity_id::text,
      'bank_details_hash', v_before.bank_details_hash,
      'status', v_before.status,
      'checked_at_utc', v_before.checked_at_utc,
      'result_json', v_before.result_json,
      'override_reason', v_before.override_reason,
      'override_by_user_id', case when v_before.override_by_user_id is null then null else v_before.override_by_user_id::text end,
      'override_at_utc', v_before.override_at_utc,
      'override_hash', v_before.override_hash,
      'created_at_utc', v_before.created_at_utc,
      'updated_at_utc', v_before.updated_at_utc
    );
  end if;

  -- Ensure row exists (insert UNVERIFIED if not), then set override fields.
  with upserted as (
    insert into public.bank_name_checks (
      rail_provider,
      rail_env,
      entity_kind,
      entity_id,
      bank_details_hash,
      status,
      checked_at_utc,
      result_json,
      override_reason,
      override_by_user_id,
      override_at_utc,
      override_hash,
      created_at_utc,
      updated_at_utc
    )
    values (
      v_provider,
      v_env,
      v_kind,
      p_entity_id,
      v_current_hash,
      'UNVERIFIED',
      null,
      null,
      v_reason,
      p_actor_user_id,
      v_now,
      v_current_hash,
      v_now,
      v_now
    )
    on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
    do update set
      override_reason = excluded.override_reason,
      override_by_user_id = excluded.override_by_user_id,
      override_at_utc = excluded.override_at_utc,
      override_hash = excluded.override_hash,
      updated_at_utc = v_now
    returning
      public.bank_name_checks.*
  )
  select
    u.*
  into v_row
  from upserted u
  limit 1;

  -- Derive inserted vs updated deterministically from timestamps set by this function.
  v_inserted := (v_row.created_at_utc is not null and v_row.updated_at_utc is not null and v_row.created_at_utc = v_row.updated_at_utc);
  v_action := case when v_inserted then 'inserted' else 'updated' end;

  v_after_json := jsonb_build_object(
    'id', v_row.id::text,
    'rail_provider', v_row.rail_provider,
    'rail_env', v_row.rail_env,
    'entity_kind', v_row.entity_kind,
    'entity_id', v_row.entity_id::text,
    'bank_details_hash', v_row.bank_details_hash,
    'status', v_row.status,
    'checked_at_utc', v_row.checked_at_utc,
    'result_json', v_row.result_json,
    'override_reason', v_row.override_reason,
    'override_by_user_id', case when v_row.override_by_user_id is null then null else v_row.override_by_user_id::text end,
    'override_at_utc', v_row.override_at_utc,
    'override_hash', v_row.override_hash,
    'created_at_utc', v_row.created_at_utc,
    'updated_at_utc', v_row.updated_at_utc
  );

  -- ✅ User-facing audit (UNGATED): bank name-check override set
  perform public._audit_insert(
    'bank_name_checks',
    v_row.id::text,
    'BANK_NAME_CHECK_OVERRIDE_SET',
    v_before_json,
    v_after_json,
    v_reason,
    p_actor_user_id
  );

  return jsonb_build_object(
    'ok', true,
    'action', v_action,
    'row', v_after_json
  );
end;
$function$;


CREATE OR REPLACE FUNCTION public.bank_name_check_clear_override(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));

  v_now timestamptz := now();
  v_current_hash text;

  v_updated int := 0;
  v_row public.bank_name_checks%rowtype;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;

  -- Resolve current hash
  if v_kind = 'CANDIDATE' then
    select c.bank_details_hash
    into v_current_hash
    from public.candidates c
    where c.id = p_entity_id
    limit 1;
  else
    select u.bank_details_hash
    into v_current_hash
    from public.umbrellas u
    where u.id = p_entity_id
    limit 1;
  end if;

  if v_current_hash is null then
    raise exception '%', jsonb_build_object('error','ENTITY_NOT_FOUND_OR_NO_HASH','entity_kind',v_kind)::text;
  end if;

  update public.bank_name_checks bnc
  set
    override_reason = null,
    override_by_user_id = null,
    override_at_utc = null,
    override_hash = null,
    updated_at_utc = v_now
  where bnc.rail_provider = v_provider
    and bnc.rail_env = v_env
    and bnc.entity_kind = v_kind
    and bnc.entity_id = p_entity_id
    and bnc.bank_details_hash = v_current_hash;

  get diagnostics v_updated = row_count;

  if v_updated = 0 then
    return jsonb_build_object(
      'ok', true,
      'did_update', false,
      'row', null
    );
  end if;

  select bnc2.*
  into v_row
  from public.bank_name_checks bnc2
  where bnc2.rail_provider = v_provider
    and bnc2.rail_env = v_env
    and bnc2.entity_kind = v_kind
    and bnc2.entity_id = p_entity_id
    and bnc2.bank_details_hash = v_current_hash
  limit 1;

  return jsonb_build_object(
    'ok', true,
    'did_update', true,
    'row', jsonb_build_object(
      'rail_provider', v_row.rail_provider,
      'rail_env', v_row.rail_env,
      'entity_kind', v_row.entity_kind,
      'entity_id', v_row.entity_id::text,
      'bank_details_hash', v_row.bank_details_hash,
      'status', v_row.status,
      'checked_at_utc', v_row.checked_at_utc,
      'result_json', v_row.result_json,
      'override_reason', v_row.override_reason,
      'override_by_user_id', case when v_row.override_by_user_id is null then null else v_row.override_by_user_id::text end,
      'override_at_utc', v_row.override_at_utc,
      'override_hash', v_row.override_hash,
      'created_at_utc', v_row.created_at_utc,
      'updated_at_utc', v_row.updated_at_utc
    )
  );
end;
$function$;

CREATE OR REPLACE FUNCTION public.bank_payee_map_upsert(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_bank_details_hash text,
  p_payee_id text,
  p_payee_account_id text,
  p_meta_json jsonb,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));

  v_hash text := nullif(btrim(coalesce(p_bank_details_hash,'')), '');
  v_payee_id text := nullif(btrim(coalesce(p_payee_id,'')), '');
  v_payee_account_id text := nullif(btrim(coalesce(p_payee_account_id,'')), '');

  v_now timestamptz := now();

  v_inserted boolean := false;
  v_action text := null;

  v_row_json jsonb;
  v_out_payee_id text;
  v_out_payee_account_id text;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;

  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;

  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;

  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;

  if v_hash is null then
    raise exception '%', jsonb_build_object('error','BANK_DETAILS_HASH_REQUIRED')::text;
  end if;

  if v_payee_id is null then
    raise exception '%', jsonb_build_object('error','PAYEE_ID_REQUIRED')::text;
  end if;

  -- ✅ Approach A: single statement consumes the CTE and captures inserted_flag + output fields.
  with upserted as (
    insert into public.bank_payee_map (
      rail_provider,
      rail_env,
      entity_kind,
      entity_id,
      bank_details_hash,
      payee_id,
      payee_account_id,
      meta_json,
      created_at_utc,
      updated_at_utc
    )
    values (
      v_provider,
      v_env,
      v_kind,
      p_entity_id,
      v_hash,
      v_payee_id,
      v_payee_account_id,
      p_meta_json,
      v_now,
      v_now
    )
    on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
    do update set
      payee_id = excluded.payee_id,
      payee_account_id = excluded.payee_account_id,
      meta_json = excluded.meta_json,
      updated_at_utc = v_now
    returning
      public.bank_payee_map.*,
      (xmax = 0) as inserted_flag
  )
  select
    u.inserted_flag,
    u.payee_id,
    u.payee_account_id,
    jsonb_build_object(
      'rail_provider', u.rail_provider,
      'rail_env', u.rail_env,
      'entity_kind', u.entity_kind,
      'entity_id', u.entity_id::text,
      'bank_details_hash', u.bank_details_hash,
      'payee_id', u.payee_id,
      'payee_account_id', u.payee_account_id,
      'meta_json', u.meta_json,
      'created_at_utc', u.created_at_utc,
      'updated_at_utc', u.updated_at_utc
    )
  into
    v_inserted,
    v_out_payee_id,
    v_out_payee_account_id,
    v_row_json
  from upserted u
  limit 1;

  v_action := case when v_inserted then 'inserted' else 'updated' end;

  return jsonb_build_object(
    'ok', true,
    'action', v_action,
    'payee_id', v_out_payee_id,
    'payee_account_id', v_out_payee_account_id,
    'row', v_row_json
  );
end;
$function$;




create or replace function public.pay_batch_schedule(
  p_pay_batch_id uuid,
  p_schedule_kind text,
  p_scheduled_at_utc timestamptz,
  p_funding_account_ref text,
  p_warning_hours_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_kind text := upper(btrim(coalesce(p_schedule_kind,'')));
  v_batch record;
  v_cfg record;

  v_provider text := upper(btrim(coalesce(nullif(btrim(coalesce(p_schedule_kind,'')),''),'')));

  v_sched_at timestamptz;
  v_warn jsonb;

  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_funding text := nullif(btrim(coalesce(p_funding_account_ref,'')), '');

  v_missing_bank int := 0;
  v_blocked_name int := 0;
  v_missing_map int := 0;
  v_pending_transfers int := 0;

  v_fresh jsonb := null;
  v_is_stale boolean := false;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;

  v_batch_kind_fixed text := null;
  v_bad_loans_payee_ct int := 0;
begin
  if p_pay_batch_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'message', 'pay_batch_schedule: pay_batch_id is required'
    )::text;
  end if;
  if p_actor_user_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_batch_schedule: actor_user_id is required',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;
  if v_kind not in ('IMMEDIATE','SCHEDULED') then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'INVALID_SCHEDULE_KIND',
      'message', 'pay_batch_schedule: invalid schedule_kind (IMMEDIATE|SCHEDULED)',
      'schedule_kind', v_kind,
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  select
    pb.id,
    pb.status,
    pb.batch_kind_fixed,
    pb.pay_date,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'PAY_BATCH_NOT_FOUND',
      'message', 'pay_batch_schedule: pay_batch not found',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  v_batch_kind_fixed := upper(btrim(coalesce(v_batch.batch_kind_fixed,'')));

  v_fresh := public.pay_batch_validate_freshness(p_pay_batch_id, p_actor_user_id);
  v_is_stale := coalesce((v_fresh->>'is_stale')::boolean, false);
  v_stale_reasons := coalesce(v_fresh->'stale_reasons', '[]'::jsonb);

  if v_is_stale = true then
    select coalesce(jsonb_agg(x.elem), '[]'::jsonb)
      into v_diff_sample
    from (
      select elem
      from jsonb_array_elements(coalesce(v_fresh->'diff','[]'::jsonb)) as elem
      limit 50
    ) x;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_SCHEDULE:STALE',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'stale_reasons', v_stale_reasons,
          'diff_sample', v_diff_sample
        ),
        'pay_batches',
        p_pay_batch_id::text
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'BATCH_STALE',
      'message', 'pay_batch_schedule: batch is stale; regenerate draft before proceeding',
      'pay_batch_id', p_pay_batch_id::text,
      'stale_reasons', v_stale_reasons,
      'diff', v_diff_sample
    )::text;
  end if;

  v_provider := upper(btrim(coalesce(v_batch.rail_provider_snapshot,'')));
  if v_provider = '' then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'RAIL_PROVIDER_MISSING_ON_BATCH',
      'message', 'pay_batch_schedule: rail_provider_snapshot missing on batch',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  select
    sd.funds_warning_hours_json,
    sd.rail_supports_name_check,
    sd.rail_supports_scheduling,
    sd.rail_default_funding_account_ref
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cfg.rail_supports_scheduling is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'SETTINGS_DEFAULTS_MISSING',
      'message', 'pay_batch_schedule: settings_defaults missing (id=1)',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  -- Manual rails (e.g. CSV) must not use scheduling
  if v_provider = 'CSV' then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'SCHEDULING_NOT_SUPPORTED_FOR_MANUAL_RAIL',
      'message', format('pay_batch_schedule: scheduling is not supported for manual rail_provider_snapshot=%s', v_batch.rail_provider_snapshot),
      'pay_batch_id', p_pay_batch_id::text,
      'rail_provider_snapshot', v_batch.rail_provider_snapshot
    )::text;
  end if;

  -- Global capability gate (current defaults)
  if coalesce(v_cfg.rail_supports_scheduling,false) = false then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'SCHEDULING_DISABLED_IN_SETTINGS',
      'message', 'pay_batch_schedule: scheduling is not enabled in settings_defaults (rail_supports_scheduling=false)',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  v_need_name_check := (coalesce(v_cfg.rail_supports_name_check,false) = true);
  v_requires_payee_map := true; -- scheduling rails are API rails and require payee mapping

  v_warn := coalesce(p_warning_hours_json, v_cfg.funds_warning_hours_json);
  if v_warn is not null and jsonb_typeof(v_warn) <> 'array' then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'WARNING_HOURS_JSON_INVALID',
      'message', 'pay_batch_schedule: warning_hours_json must be a JSON array',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  if v_kind = 'IMMEDIATE' then
    v_sched_at := now();
  else
    if p_scheduled_at_utc is null then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'SCHEDULED_AT_REQUIRED',
        'message', 'pay_batch_schedule: scheduled_at_utc is required when schedule_kind=SCHEDULED',
        'pay_batch_id', p_pay_batch_id::text,
        'schedule_kind', v_kind
      )::text;
    end if;
    v_sched_at := p_scheduled_at_utc;
  end if;

  -- Funding account must be present for scheduling rails; allow fallback to settings default
  if v_funding is null then
    v_funding := nullif(btrim(coalesce(v_cfg.rail_default_funding_account_ref,'')), '');
  end if;
  if v_funding is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'FUNDING_ACCOUNT_REQUIRED',
      'message', format('pay_batch_schedule: funding_account_ref is required for this rail (provider=%s)', v_batch.rail_provider_snapshot),
      'pay_batch_id', p_pay_batch_id::text,
      'rail_provider_snapshot', v_batch.rail_provider_snapshot
    )::text;
  end if;

  select count(*)::int
  into v_pending_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  if v_pending_transfers = 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'NO_PENDING_TRANSFERS',
      'message', 'pay_batch_schedule: no PENDING transfers exist for this batch (execute-bank required first)',
      'pay_batch_id', p_pay_batch_id::text,
      'ui_hint', 'RERUN_PREVIEW_OR_EXECUTE_BANK'
    )::text;
  end if;

  if v_batch_kind_fixed = 'LOANS' then
    select count(*)::int
    into v_bad_loans_payee_ct
    from public.pay_bank_transfers pbt2
    where pbt2.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt2.status,'')) = 'PENDING'
      and upper(
        coalesce(
          pbt2.payee_entity_kind,
          case
            when upper(coalesce(pbt2.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA'
            else 'CANDIDATE'
          end
        )
      ) <> 'CANDIDATE';

    if v_bad_loans_payee_ct > 0 then
      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_BATCH_SCHEDULE:BLOCKED_LOANS_PAYEE_KIND',
          jsonb_build_object(
            'pay_batch_id', p_pay_batch_id::text,
            'bad_transfer_count', v_bad_loans_payee_ct
          ),
          'pay_batches',
          p_pay_batch_id::text
        );
      exception when others then
        null;
      end;

      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
        'code', 'LOANS_PAYEE_MUST_BE_CANDIDATE',
        'message', 'pay_batch_schedule: LOANS batches must pay candidates (not umbrellas)',
        'pay_batch_id', p_pay_batch_id::text,
        'bad_transfer_count', v_bad_loans_payee_ct,
        'ui_hint', 'REGENERATE_TRANSFERS_AS_CANDIDATE_PAYEES'
      )::text;
    end if;
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      coalesce(nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')),''), c.bank_details_hash, u.bank_details_hash) as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(case when t.bank_hash is null or btrim(t.bank_hash) = '' then 1 else 0 end)::int
  into v_missing_bank
  from t;

  if v_missing_bank > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_SCHEDULE:BLOCKED_BANK_DETAILS',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'count', v_missing_bank
        ),
        'pay_batches',
        p_pay_batch_id::text
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
      'code', 'BLOCKED_BANK_DETAILS',
      'message', format('pay_batch_schedule: BLOCKED_BANK_DETAILS for %s payee(s)', v_missing_bank::text),
      'pay_batch_id', p_pay_batch_id::text,
      'count', v_missing_bank,
      'ui_hint', 'RERUN_PREVIEW'
    )::text;
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      coalesce(nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')),''), c.bank_details_hash, u.bank_details_hash) as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(
      case
        when v_need_name_check = true then
          case
            when coalesce(bnc.status,'UNVERIFIED') = 'PASS' then 0
            when (bnc.override_reason is not null and bnc.override_hash = t.bank_hash) then 0
            else 1
          end
        else 0
      end
    )::int
  into v_blocked_name
  from t
  left join public.bank_name_checks bnc
    on bnc.rail_provider = v_batch.rail_provider_snapshot
   and bnc.rail_env = v_batch.rail_env_snapshot
   and bnc.entity_kind = t.payee_kind
   and bnc.entity_id = t.payee_id
   and bnc.bank_details_hash = t.bank_hash;

  if v_blocked_name > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_SCHEDULE:BLOCKED_NAME_CHECK',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'count', v_blocked_name
        ),
        'pay_batches',
        p_pay_batch_id::text
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
      'code', 'BLOCKED_NAME_CHECK',
      'message', format('pay_batch_schedule: BLOCKED_NAME_CHECK for %s payee(s)', v_blocked_name::text),
      'pay_batch_id', p_pay_batch_id::text,
      'count', v_blocked_name,
      'ui_hint', 'RERUN_PREVIEW'
    )::text;
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      coalesce(nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')),''), c.bank_details_hash, u.bank_details_hash) as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(
      case
        when v_requires_payee_map = true then
          case when bpm.payee_id is null then 1 else 0 end
        else 0
      end
    )::int
  into v_missing_map
  from t
  left join public.bank_payee_map bpm
    on bpm.rail_provider = v_batch.rail_provider_snapshot
   and bpm.rail_env = v_batch.rail_env_snapshot
   and bpm.entity_kind = t.payee_kind
   and bpm.entity_id = t.payee_id
   and bpm.bank_details_hash = t.bank_hash;

  if v_missing_map > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_SCHEDULE:BLOCKED_NO_PAYEE_MAP',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'count', v_missing_map
        ),
        'pay_batches',
        p_pay_batch_id::text
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
      'code', 'BLOCKED_NO_PAYEE_MAP',
      'message', format('pay_batch_schedule: BLOCKED_NO_PAYEE_MAP for %s payee(s)', v_missing_map::text),
      'pay_batch_id', p_pay_batch_id::text,
      'count', v_missing_map,
      'ui_hint', 'RERUN_PREVIEW'
    )::text;
  end if;

  update public.pay_batches pb
  set
    schedule_kind = v_kind,
    scheduled_at_utc = v_sched_at,
    scheduled_by_user_id = p_actor_user_id,
    funding_account_ref = v_funding,
    funds_warning_hours_json = v_warn,
    status = 'SCHEDULED'
  where pb.id = p_pay_batch_id;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_SCHEDULE:OK',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'schedule_kind', v_kind,
        'scheduled_at_utc', v_sched_at::text,
        'funding_account_ref', v_funding,
        'rail_provider_snapshot', v_batch.rail_provider_snapshot,
        'rail_env_snapshot', v_batch.rail_env_snapshot,
        'pending_transfers', v_pending_transfers
      ),
      'pay_batches',
      p_pay_batch_id::text
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', (select pb2.status from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'schedule_kind', v_kind,
    'scheduled_at_utc', v_sched_at::text,
    'funding_account_ref', v_funding,
    'funds_warning_hours_json', v_warn,
    'rail_provider_snapshot', v_batch.rail_provider_snapshot,
    'rail_env_snapshot', v_batch.rail_env_snapshot
  );
end;
$$;


create or replace function public.pay_batch_prepare(
  p_pay_batch_id uuid,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch record;
  v_cfg record;

  v_provider text := null;

  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;

  v_payees jsonb := '[]'::jsonb;
  v_summary jsonb := '{}'::jsonb;
  v_has_hard_blockers boolean := false;

  v_fresh jsonb := null;
  v_is_stale boolean := false;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;

  v_batch_kind_fixed text := null;
  v_has_paye boolean := false;
  v_has_awaiting_net boolean := false;

  v_bad_loans_payee_ct int := 0;
begin
  if p_pay_batch_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'message', 'pay_batch_prepare: pay_batch_id is required'
    )::text;
  end if;
  if p_actor_user_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_batch_prepare: actor_user_id is required',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  select
    pb.id,
    pb.status,
    pb.batch_kind_fixed,
    pb.pay_date,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot,
    pb.schedule_kind,
    pb.scheduled_at_utc,
    pb.funding_account_ref,
    pb.funds_warning_hours_json
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id;

  if v_batch.id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'PAY_BATCH_NOT_FOUND',
      'message', 'pay_batch_prepare: pay_batch not found',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  v_batch_kind_fixed := upper(btrim(coalesce(v_batch.batch_kind_fixed,'')));

  v_fresh := public.pay_batch_validate_freshness(p_pay_batch_id, p_actor_user_id);
  v_is_stale := coalesce((v_fresh->>'is_stale')::boolean, false);
  v_stale_reasons := coalesce(v_fresh->'stale_reasons', '[]'::jsonb);

  if v_is_stale = true then
    select coalesce(jsonb_agg(x.elem), '[]'::jsonb)
      into v_diff_sample
    from (
      select elem
      from jsonb_array_elements(coalesce(v_fresh->'diff','[]'::jsonb)) as elem
      limit 50
    ) x;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_PREPARE:STALE',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'stale_reasons', v_stale_reasons,
          'diff_sample', v_diff_sample
        ),
        'pay_batches',
        p_pay_batch_id::text,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'BATCH_STALE',
      'message', 'pay_batch_prepare: batch is stale; regenerate draft before proceeding',
      'pay_batch_id', p_pay_batch_id::text,
      'stale_reasons', v_stale_reasons,
      'diff', v_diff_sample
    )::text;
  end if;

  select exists(
    select 1
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.pay_channel = 'PAYE'
      and pbi.is_voided = false
  )
  into v_has_paye;

  select exists(
    select 1
    from public.pay_batch_candidates pbc2
    where pbc2.pay_batch_id = p_pay_batch_id
      and coalesce(pbc2.awaiting_net_amount,false) = true
  )
  into v_has_awaiting_net;

  if v_batch_kind_fixed <> 'LOANS' and v_has_paye = true and v_has_awaiting_net = true then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_PREPARE:BLOCKED_AWAITING_NET',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'has_paye', v_has_paye,
          'has_awaiting_net', v_has_awaiting_net
        ),
        'pay_batches',
        p_pay_batch_id::text,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'PAYE_NET_REQUIRED',
      'message', 'pay_batch_prepare: PAYE net amounts are required before this batch can proceed',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  if v_batch_kind_fixed = 'LOANS' then
    select count(*)::int
      into v_bad_loans_payee_ct
    from public.pay_bank_transfers pbt
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(
        coalesce(
          pbt.payee_entity_kind,
          case
            when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA'
            else 'CANDIDATE'
          end
        )
      ) <> 'CANDIDATE';

    if v_bad_loans_payee_ct > 0 then
      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_BATCH_PREPARE:BLOCKED_LOANS_PAYEE_KIND',
          jsonb_build_object(
            'pay_batch_id', p_pay_batch_id::text,
            'bad_transfer_count', v_bad_loans_payee_ct
          ),
          'pay_batches',
          p_pay_batch_id::text,
          null,
          null,
          null,
          null
        );
      exception when others then
        null;
      end;

      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_PREPARE',
        'code', 'LOANS_PAYEE_MUST_BE_CANDIDATE',
        'message', 'pay_batch_prepare: LOANS batches must pay candidates (not umbrellas)',
        'pay_batch_id', p_pay_batch_id::text,
        'bad_transfer_count', v_bad_loans_payee_ct
      )::text;
    end if;
  end if;

  v_provider := upper(btrim(coalesce(v_batch.rail_provider_snapshot,'')));

  select
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_scheduling,
    sd.rail_supports_name_check,
    sd.rail_supports_auto_execute,
    sd.default_schedule_umbrella_local,
    sd.default_schedule_paye_local,
    sd.funds_warning_hours_json,
    sd.rail_default_funding_account_ref
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cfg.rail_provider_default is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'SETTINGS_DEFAULTS_MISSING',
      'message', 'pay_batch_prepare: settings_defaults missing (id=1)',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  -- Generic readiness semantics:
  -- - name-check only when the rail supports it (and never for manual CSV)
  -- - payee-map required for API rails (false for manual CSV)
  v_need_name_check := (coalesce(v_cfg.rail_supports_name_check,false) = true)
                       and (v_provider <> 'CSV');

  v_requires_payee_map := (v_provider <> 'CSV');

  with t as (
    select
      pbt.id as transfer_id,
      pbt.pay_batch_id,
      upper(coalesce(pbt.pay_channel,'')) as pay_channel,
      upper(coalesce(pbt.status,'')) as status,
      pbt.amount,
      pbt.currency,
      pbt.payment_reference,
      pbt.payee_name,
      pbt.sort_code,
      pbt.account_number,
      pbt.account_type,
      pbt.rail_provider,
      pbt.rail_env,
      pbt.request_id,
      pbt.rail_tx_id,
      pbt.rail_state,
      pbt.rail_meta_json,
      pbt.bank_details_hash_snapshot,
      pbt.payee_entity_kind,
      pbt.payee_entity_id,
      pbt.transfer_group_key,
      pbt.grouping_mode_used,
      pbt.week_ending_bucket,
      pbt.candidate_id,
      pbt.umbrella_id,

      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as derived_payee_kind,

      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as derived_payee_id
    from public.pay_bank_transfers pbt
    where pbt.pay_batch_id = p_pay_batch_id
  ),
  t2 as (
    select
      t.*,
      c.bank_details_hash as cand_bank_hash,
      u.bank_details_hash as umb_bank_hash
    from t
    left join public.candidates c
      on c.id = t.derived_payee_id
     and t.derived_payee_kind = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = t.derived_payee_id
     and t.derived_payee_kind = 'UMBRELLA'
  ),
  t3 as (
    select
      t2.*,
      coalesce(nullif(btrim(coalesce(t2.bank_details_hash_snapshot,'')),''), t2.cand_bank_hash, t2.umb_bank_hash) as payee_bank_hash
    from t2
  ),
  payees as (
    select
      t3.derived_payee_kind as payee_entity_kind,
      t3.derived_payee_id as payee_entity_id,
      t3.payee_bank_hash as bank_details_hash
    from t3
    group by t3.derived_payee_kind, t3.derived_payee_id, t3.payee_bank_hash
  ),
  payees_enriched as (
    select
      p.payee_entity_kind,
      p.payee_entity_id,
      p.bank_details_hash,

      coalesce(bnc.status, 'UNVERIFIED') as name_check_status,
      (bnc.override_reason is not null and bnc.override_hash = p.bank_details_hash) as name_check_has_override,

      (bpm.payee_id is not null) as payee_map_present,

      (p.bank_details_hash is null or btrim(p.bank_details_hash) = '') as is_missing_bank_details,

      (
        v_need_name_check = true
        and coalesce(bnc.status, 'UNVERIFIED') <> 'PASS'
        and not (bnc.override_reason is not null and bnc.override_hash = p.bank_details_hash)
      ) as is_name_check_blocked,

      (
        v_requires_payee_map = true
        and (bpm.payee_id is null)
      ) as is_payee_map_blocked
    from payees p
    left join public.bank_name_checks bnc
      on bnc.rail_provider = v_batch.rail_provider_snapshot
     and bnc.rail_env = v_batch.rail_env_snapshot
     and bnc.entity_kind = p.payee_entity_kind
     and bnc.entity_id = p.payee_entity_id
     and bnc.bank_details_hash = p.bank_details_hash
    left join public.bank_payee_map bpm
      on bpm.rail_provider = v_batch.rail_provider_snapshot
     and bpm.rail_env = v_batch.rail_env_snapshot
     and bpm.entity_kind = p.payee_entity_kind
     and bpm.entity_id = p.payee_entity_id
     and bpm.bank_details_hash = p.bank_details_hash
  ),
  payees_json as (
    select
      jsonb_agg(
        jsonb_build_object(
          'payee_entity_kind', pe.payee_entity_kind,
          'payee_entity_id', pe.payee_entity_id::text,
          'bank_details_hash', pe.bank_details_hash,
          'name_check', jsonb_build_object(
            'status', pe.name_check_status,
            'has_override', pe.name_check_has_override
          ),
          'payee_map', jsonb_build_object(
            'present', pe.payee_map_present
          ),
          'blockers',
            (
              (case when pe.is_missing_bank_details then jsonb_build_array('BLOCKED_BANK_DETAILS') else '[]'::jsonb end)
              ||
              (case when pe.is_name_check_blocked then jsonb_build_array('BLOCKED_NAME_CHECK') else '[]'::jsonb end)
              ||
              (case when pe.is_payee_map_blocked then jsonb_build_array('BLOCKED_NO_PAYEE_MAP') else '[]'::jsonb end)
            ),
          'transfers',
            coalesce(
              (
                select jsonb_agg(
                  jsonb_build_object(
                    'id', t3.transfer_id::text,
                    'pay_channel', t3.pay_channel,
                    'status', t3.status,
                    'amount', t3.amount,
                    'currency', t3.currency,
                    'payment_reference', t3.payment_reference,
                    'payee_name', t3.payee_name,
                    'sort_code', t3.sort_code,
                    'account_number', t3.account_number,
                    'account_type', t3.account_type,
                    'rail_provider', t3.rail_provider,
                    'rail_env', t3.rail_env,
                    'request_id', t3.request_id,
                    'rail_tx_id', t3.rail_tx_id,
                    'rail_state', t3.rail_state,
                    'rail_meta_json', t3.rail_meta_json,
                    'bank_details_hash_snapshot', t3.bank_details_hash_snapshot,
                    'transfer_group_key', t3.transfer_group_key,
                    'grouping_mode_used', t3.grouping_mode_used,
                    'week_ending_bucket', case when t3.week_ending_bucket is null then null else t3.week_ending_bucket::text end
                  )
                  order by t3.pay_channel, t3.amount desc, t3.transfer_id
                )
                from t3
                where t3.derived_payee_kind = pe.payee_entity_kind
                  and t3.derived_payee_id = pe.payee_entity_id
                  and t3.payee_bank_hash is not distinct from pe.bank_details_hash
              ),
              '[]'::jsonb
            )
        )
        order by pe.payee_entity_kind, pe.payee_entity_id
      ) as j
    from payees_enriched pe
  ),
  summary as (
    select
      jsonb_build_object(
        'total_transfers', count(*)::int,
        'pending', sum(case when upper(coalesce(t3.status,'')) = 'PENDING' then 1 else 0 end)::int,
        'blocked', sum(case when upper(coalesce(t3.status,'')) = 'BLOCKED' then 1 else 0 end)::int,
        'completed', sum(case when upper(coalesce(t3.status,'')) = 'COMPLETED' then 1 else 0 end)::int,
        'failed', sum(case when upper(coalesce(t3.status,'')) = 'FAILED' then 1 else 0 end)::int
      ) as j
    from t3
  ),
  hard_blockers as (
    select
      exists(
        select 1
        from payees_enriched pe
        where pe.is_missing_bank_details = true
           or pe.is_name_check_blocked = true
           or pe.is_payee_map_blocked = true
      ) as has_any
  )
  select
    coalesce((select pj.j from payees_json pj), '[]'::jsonb),
    coalesce((select s.j from summary s), '{}'::jsonb),
    coalesce((select hb.has_any from hard_blockers hb), false)
  into v_payees, v_summary, v_has_hard_blockers;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_PREPARE:READY',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'has_hard_blockers', v_has_hard_blockers
      ),
      'pay_batches',
      p_pay_batch_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch.id::text,
    'status', v_batch.status,
    'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
    'rail', jsonb_build_object(
      'provider_snapshot', v_batch.rail_provider_snapshot,
      'env_snapshot', v_batch.rail_env_snapshot,
      'need_name_check', v_need_name_check,
      'requires_payee_map', v_requires_payee_map
    ),
    'schedule_recommendations', jsonb_build_object(
      'default_schedule_umbrella_local', v_cfg.default_schedule_umbrella_local,
      'default_schedule_paye_local', v_cfg.default_schedule_paye_local,
      'funds_warning_hours_json', v_cfg.funds_warning_hours_json,
      'rail_default_funding_account_ref', v_cfg.rail_default_funding_account_ref
    ),
    'batch_schedule', jsonb_build_object(
      'schedule_kind', v_batch.schedule_kind,
      'scheduled_at_utc', case when v_batch.scheduled_at_utc is null then null else v_batch.scheduled_at_utc::text end,
      'funding_account_ref', v_batch.funding_account_ref,
      'funds_warning_hours_json', v_batch.funds_warning_hours_json
    ),
    'payees', v_payees,
    'summary', v_summary,
    'has_hard_blockers', v_has_hard_blockers
  );
end;
$$;



create or replace function public.pay_batch_mark_blocked_funds(
  p_pay_batch_id uuid,
  p_actor_user_id uuid,
  p_funds_check_json jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch record;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_mark_blocked_funds: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_mark_blocked_funds: actor_user_id is required';
  end if;

  select
    pb.id,
    pb.status
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch_mark_blocked_funds: pay_batch not found';
  end if;

  if v_batch.status not in ('READY','SCHEDULED','EXECUTING') then
    raise exception 'pay_batch_mark_blocked_funds: batch status must be READY, SCHEDULED or EXECUTING (current=%)', v_batch.status;
  end if;

  update public.pay_batch_items pbi
  set pay_bank_transfer_id = null
  from public.pay_batch_candidates pbc
  where pbc.id = pbi.pay_batch_candidate_id
    and pbc.pay_batch_id = p_pay_batch_id
    and pbi.pay_bank_transfer_id is not null;

  delete from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id;

  update public.pay_batches pb
  set
    status = 'BLOCKED_FUNDS',
    last_funds_check_at_utc = now(),
    last_funds_check_json = p_funds_check_json,
    schedule_kind = null,
    scheduled_at_utc = null,
    scheduled_by_user_id = null
  where pb.id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', (select pb2.status from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'last_funds_check_at_utc', (select pb3.last_funds_check_at_utc::text from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'last_funds_check_json', (select pb4.last_funds_check_json from public.pay_batches pb4 where pb4.id = p_pay_batch_id)
  );
end;
$$;

create or replace function public.pay_export_bank_csv(
  p_pay_batch_id uuid,
  p_scope text default 'ALL'
)
returns text
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(btrim(coalesce(p_scope, 'ALL')));

  v_default_cols jsonb := '["payment_reference","payee_name","sort_code","account_number","account_type","amount"]'::jsonb;
  v_cols_json jsonb;
  v_cols text[];
  v_col text;

  v_allowed_cols text[] := array[
    'transfer_id',
    'payment_reference',
    'payee_name',
    'sort_code',
    'account_number',
    'account_type',
    'amount',
    'currency',
    'pay_channel',
    'rail_provider',
    'rail_env',
    'request_id',
    'transfer_group_key',
    'grouping_mode_used',
    'week_ending_bucket',
    'candidate_id',
    'umbrella_id',
    'payee_entity_kind',
    'payee_entity_id',
    'bank_details_hash_snapshot'
  ];

  v_header text := '';
  v_body text := '';
  v_csv text := '';

  v_missing_count int := 0;
  v_row_count int := 0;

  v_hdr_part text;
  v_val text;
  v_line text;

  r record;
begin
  if v_scope not in ('ALL','PAYE','UMBRELLA') then
    raise exception 'pay_export_bank_csv: invalid scope "%". Expected ALL|PAYE|UMBRELLA.', v_scope;
  end if;

  if not exists (select 1 from public.pay_batches pb where pb.id = p_pay_batch_id) then
    raise exception 'pay_export_bank_csv: pay batch % not found.', p_pay_batch_id;
  end if;

  -- Load configured CSV column order (fallback to default if NULL/invalid/empty)
  select sd.pay_export_csv_columns_json
  into v_cols_json
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cols_json is null or jsonb_typeof(v_cols_json) <> 'array' or jsonb_array_length(v_cols_json) = 0 then
    v_cols_json := v_default_cols;
  end if;

  select coalesce(array_agg(t.col order by t.ord), array[]::text[])
  into v_cols
  from jsonb_array_elements_text(v_cols_json) with ordinality as t(col, ord);

  if array_length(v_cols, 1) is null or array_length(v_cols, 1) = 0 then
    v_cols := array['payment_reference','payee_name','sort_code','account_number','account_type','amount']::text[];
  end if;

  -- Validate column keys: must be from allowed set
  foreach v_col in array v_cols loop
    if v_col is null or btrim(v_col) = '' then
      raise exception 'pay_export_bank_csv: export columns contain an empty key';
    end if;

    if not (v_col = any(v_allowed_cols)) then
      raise exception 'pay_export_bank_csv: invalid export column "%". Allowed=%', v_col, array_to_string(v_allowed_cols, ',');
    end if;
  end loop;

  -- Reject duplicates (deterministic export)
  if (
    select count(*)::int
    from unnest(v_cols) x
  ) <> (
    select count(distinct x)::int
    from unnest(v_cols) x
  ) then
    raise exception 'pay_export_bank_csv: duplicate column keys in pay_export_csv_columns_json';
  end if;

  -- Ensure required snapshot fields exist for all *pending* transfers in scope, but only for fields actually exported.
  select count(*)::int
  into v_missing_count
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt.pay_channel = v_scope)
    and upper(coalesce(pbt.status,'')) = 'PENDING'
    and (
      (array_position(v_cols,'payment_reference') is not null and pbt.payment_reference is null)
      or (array_position(v_cols,'payee_name') is not null and pbt.payee_name is null)
      or (array_position(v_cols,'sort_code') is not null and pbt.sort_code is null)
      or (array_position(v_cols,'account_number') is not null and pbt.account_number is null)
      or (array_position(v_cols,'account_type') is not null and pbt.account_type is null)
      or (array_position(v_cols,'amount') is not null and pbt.amount is null)
      or (array_position(v_cols,'currency') is not null and pbt.currency is null)
      or (array_position(v_cols,'pay_channel') is not null and pbt.pay_channel is null)
      or (array_position(v_cols,'rail_provider') is not null and pbt.rail_provider is null)
      or (array_position(v_cols,'rail_env') is not null and pbt.rail_env is null)
      or (array_position(v_cols,'request_id') is not null and pbt.request_id is null)
      or (array_position(v_cols,'transfer_group_key') is not null and pbt.transfer_group_key is null)
      or (array_position(v_cols,'grouping_mode_used') is not null and pbt.grouping_mode_used is null)
      or (array_position(v_cols,'week_ending_bucket') is not null and pbt.week_ending_bucket is null)
      or (array_position(v_cols,'candidate_id') is not null and pbt.candidate_id is null)
      or (array_position(v_cols,'umbrella_id') is not null and pbt.umbrella_id is null)
      or (array_position(v_cols,'payee_entity_kind') is not null and pbt.payee_entity_kind is null)
      or (array_position(v_cols,'payee_entity_id') is not null and pbt.payee_entity_id is null)
      or (array_position(v_cols,'bank_details_hash_snapshot') is not null and pbt.bank_details_hash_snapshot is null)
    );

  if v_missing_count > 0 then
    raise exception 'pay_export_bank_csv: % pending transfer(s) missing one or more required fields for the configured export. Execute-bank must populate these first.', v_missing_count;
  end if;

  -- If no rows, raise to avoid silent "empty file".
  select count(*)::int
  into v_row_count
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt.pay_channel = v_scope)
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  if v_row_count = 0 then
    raise exception 'pay_export_bank_csv: no pending transfers found for batch % (scope=%).', p_pay_batch_id, v_scope;
  end if;

  -- Build header from configured columns
  v_header := '';
  foreach v_col in array v_cols loop
    v_hdr_part := case v_col
      when 'transfer_id' then 'Transfer id'
      when 'payment_reference' then 'Payment reference'
      when 'payee_name' then 'Payee name'
      when 'sort_code' then 'Sort code'
      when 'account_number' then 'Bank account number'
      when 'account_type' then 'Bank account type'
      when 'amount' then 'Amount'
      when 'currency' then 'Currency'
      when 'pay_channel' then 'Channel'
      when 'rail_provider' then 'Rail provider'
      when 'rail_env' then 'Rail env'
      when 'request_id' then 'Request id'
      when 'transfer_group_key' then 'Transfer group key'
      when 'grouping_mode_used' then 'Grouping mode'
      when 'week_ending_bucket' then 'Week ending'
      when 'candidate_id' then 'Candidate id'
      when 'umbrella_id' then 'Umbrella id'
      when 'payee_entity_kind' then 'Payee entity kind'
      when 'payee_entity_id' then 'Payee entity id'
      when 'bank_details_hash_snapshot' then 'Bank details hash'
      else v_col
    end;

    if v_header = '' then
      v_header := v_hdr_part;
    else
      v_header := v_header || ',' || v_hdr_part;
    end if;
  end loop;

  -- Build rows
  v_body := '';
  for r in
    select
      pbt.id as transfer_id,
      pbt.payment_reference,
      pbt.payee_name,
      pbt.sort_code,
      pbt.account_number,
      pbt.account_type,
      pbt.amount,
      pbt.currency,
      pbt.pay_channel,
      pbt.rail_provider,
      pbt.rail_env,
      pbt.request_id,
      pbt.transfer_group_key,
      pbt.grouping_mode_used,
      pbt.week_ending_bucket,
      pbt.candidate_id,
      pbt.umbrella_id,
      pbt.payee_entity_kind,
      pbt.payee_entity_id,
      pbt.bank_details_hash_snapshot
    from public.pay_bank_transfers pbt
    where pbt.pay_batch_id = p_pay_batch_id
      and (v_scope = 'ALL' or pbt.pay_channel = v_scope)
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    order by pbt.id
  loop
    v_line := '';

    foreach v_col in array v_cols loop
      v_val := null;

      if v_col = 'transfer_id' then
        v_val := r.transfer_id::text;
      elsif v_col = 'payment_reference' then
        v_val := r.payment_reference;
      elsif v_col = 'payee_name' then
        v_val := r.payee_name;
      elsif v_col = 'sort_code' then
        v_val := r.sort_code;
      elsif v_col = 'account_number' then
        v_val := r.account_number;
      elsif v_col = 'account_type' then
        v_val := r.account_type;
      elsif v_col = 'amount' then
        v_val := to_char(r.amount, 'FM9999999990.00');
      elsif v_col = 'currency' then
        v_val := r.currency;
      elsif v_col = 'pay_channel' then
        v_val := r.pay_channel;
      elsif v_col = 'rail_provider' then
        v_val := r.rail_provider;
      elsif v_col = 'rail_env' then
        v_val := r.rail_env;
      elsif v_col = 'request_id' then
        v_val := r.request_id;
      elsif v_col = 'transfer_group_key' then
        v_val := r.transfer_group_key;
      elsif v_col = 'grouping_mode_used' then
        v_val := r.grouping_mode_used;
      elsif v_col = 'week_ending_bucket' then
        v_val := case when r.week_ending_bucket is null then null else r.week_ending_bucket::text end;
      elsif v_col = 'candidate_id' then
        v_val := case when r.candidate_id is null then null else r.candidate_id::text end;
      elsif v_col = 'umbrella_id' then
        v_val := case when r.umbrella_id is null then null else r.umbrella_id::text end;
      elsif v_col = 'payee_entity_kind' then
        v_val := r.payee_entity_kind;
      elsif v_col = 'payee_entity_id' then
        v_val := case when r.payee_entity_id is null then null else r.payee_entity_id::text end;
      elsif v_col = 'bank_details_hash_snapshot' then
        v_val := r.bank_details_hash_snapshot;
      else
        v_val := null;
      end if;

      v_val := coalesce(v_val, '');

      -- CSV escaping: quote when contains comma/quote/newline, double quotes inside.
      if v_val ~ '[,"\r\n]' then
        v_val := '"' || replace(v_val, '"', '""') || '"';
      end if;

      if v_line = '' then
        v_line := v_val;
      else
        v_line := v_line || ',' || v_val;
      end if;
    end loop;

    if v_body = '' then
      v_body := v_line;
    else
      v_body := v_body || E'\n' || v_line;
    end if;
  end loop;

  v_csv := v_header || E'\n' || coalesce(v_body, '');
  return v_csv;
end;
$$;

create or replace function public.pay_settle_manual_confirm(
  p_pay_batch_id uuid,
  p_scope text,
  p_bank_confirm_ref text,
  p_payment_date date,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(btrim(coalesce(p_scope,'ALL')));
  v_batch record;

  v_settlement_json jsonb := '[]'::jsonb;
  v_now timestamptz := now();

  v_pending_count int := 0;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_settle_manual_confirm: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_settle_manual_confirm: actor_user_id is required';
  end if;
  if v_scope not in ('ALL','PAYE','UMBRELLA') then
    raise exception 'pay_settle_manual_confirm: invalid scope (ALL|PAYE|UMBRELLA)';
  end if;
  if nullif(btrim(coalesce(p_bank_confirm_ref,'')),'') is null then
    raise exception 'pay_settle_manual_confirm: bank_confirm_ref is required';
  end if;
  if p_payment_date is null then
    raise exception 'pay_settle_manual_confirm: payment_date is required';
  end if;

  select
    pb.id,
    pb.status,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_settle_manual_confirm: pay_batch not found';
  end if;

  if upper(coalesce(v_batch.rail_provider_snapshot,'')) <> 'CSV' then
    raise exception 'pay_settle_manual_confirm: CSV-only (rail_provider_snapshot must be CSV; current=%)', v_batch.rail_provider_snapshot;
  end if;

  select count(*)::int
  into v_pending_count
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or upper(coalesce(pbt.pay_channel,'')) = v_scope)
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  if v_pending_count = 0 then
    raise exception 'pay_settle_manual_confirm: no PENDING transfers found for batch % (scope=%)', p_pay_batch_id, v_scope;
  end if;

  update public.pay_batches pb2
  set
    monzo_confirmed_at_utc = v_now,
    monzo_confirmed_by_user_id = p_actor_user_id
  where pb2.id = p_pay_batch_id;

  update public.pay_batch_items pbi
  set bank_reference = p_bank_confirm_ref
  from public.pay_bank_transfers pbt
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
   and pbc.pay_batch_id = p_pay_batch_id
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or upper(coalesce(pbt.pay_channel,'')) = v_scope)
    and upper(coalesce(pbt.status,'')) = 'PENDING'
    and pbi.pay_bank_transfer_id = pbt.id;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'transfer_id', pbt.id::text,
        'status', 'COMPLETED',
        'rail_tx_id', null,
        'rail_state', 'MANUAL_CONFIRM',
        'rail_meta_json', jsonb_build_object(
          'bank_confirm_ref', p_bank_confirm_ref,
          'payment_date', p_payment_date::text,
          'confirmed_at_utc', v_now::text,
          'confirmed_by_user_id', p_actor_user_id::text
        )
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_settlement_json
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or upper(coalesce(pbt.pay_channel,'')) = v_scope)
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  return public.pay_settle_rail(p_pay_batch_id, v_settlement_json, p_actor_user_id);
end;
$$;

create or replace function public.pay_settle_rail(
  p_pay_batch_id uuid,
  p_settlement_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch record;

  v_now timestamptz := now();

  v_newly_settled_candidates jsonb := '[]'::jsonb;

  v_pending_transfers jsonb := '[]'::jsonb;
  v_failed_transfers  jsonb := '[]'::jsonb;
  v_blocked_transfers jsonb := '[]'::jsonb;

  v_batch_status text;

  v_missing_timesheets jsonb := '[]'::jsonb;
  v_ambig_timesheets jsonb := '[]'::jsonb;

  v_adv_id uuid;
  v_old_sched jsonb;
  v_new_sched jsonb;
  v_old_out numeric;
  v_new_out numeric;
  v_old_next date;
  v_new_next date;
  v_total_taken numeric;

  v_linked_transfer_ct int := 0;
  v_has_pos_net boolean := false;

  v_fresh jsonb := null;
  v_is_stale boolean := false;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;

  v_overpay_patched_ct int := 0;
  v_loan_payout_updated_ct int := 0;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_settle_rail: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_settle_rail: actor_user_id is required';
  end if;

  if p_settlement_json is null or jsonb_typeof(p_settlement_json) <> 'array' then
    raise exception 'pay_settle_rail: settlement_json must be a JSON array';
  end if;

  select
    pb.id,
    pb.status,
    pb.pay_date,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot,
    pb.batch_kind_fixed
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_settle_rail: pay_batch not found';
  end if;

  v_fresh := public.pay_batch_validate_freshness(p_pay_batch_id, p_actor_user_id);
  v_is_stale := coalesce((v_fresh->>'is_stale')::boolean, false);
  v_stale_reasons := coalesce(v_fresh->'stale_reasons', '[]'::jsonb);

  if v_is_stale = true then
    select coalesce(jsonb_agg(x.elem), '[]'::jsonb)
      into v_diff_sample
    from (
      select elem
      from jsonb_array_elements(coalesce(v_fresh->'diff','[]'::jsonb)) as elem
      limit 50
    ) x;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_SETTLE_RAIL:STALE_PROCEEDING',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'stale_reasons', v_stale_reasons,
          'diff_sample', v_diff_sample
        ),
        'pay_batches',
        p_pay_batch_id::text,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;
  end if;

  create temp table if not exists _tmp_settle_in (
    transfer_id uuid not null,
    status text not null,
    rail_tx_id text null,
    rail_state text null,
    rail_meta_json jsonb null
  ) on commit drop;

  truncate table _tmp_settle_in;

  insert into _tmp_settle_in(transfer_id, status, rail_tx_id, rail_state, rail_meta_json)
  select
    nullif(btrim(coalesce(e->>'transfer_id','')),'')::uuid as transfer_id,
    upper(btrim(coalesce(e->>'status',''))) as status,
    nullif(btrim(coalesce(e->>'rail_tx_id','')),'') as rail_tx_id,
    nullif(btrim(coalesce(e->>'rail_state','')),'') as rail_state,
    case
      when (e ? 'rail_meta_json') and jsonb_typeof(e->'rail_meta_json') in ('object','array','string','number','boolean','null')
        then e->'rail_meta_json'
      else null
    end as rail_meta_json
  from jsonb_array_elements(p_settlement_json) e
  where e is not null and jsonb_typeof(e) = 'object';

  if exists (select 1 from _tmp_settle_in t where t.transfer_id is null limit 1) then
    raise exception 'pay_settle_rail: settlement_json contains an invalid or missing transfer_id';
  end if;

  if exists (
    select 1
    from _tmp_settle_in t
    where t.status not in ('PENDING','COMPLETED','FAILED')
    limit 1
  ) then
    raise exception 'pay_settle_rail: invalid status in settlement_json (allowed: PENDING|COMPLETED|FAILED)';
  end if;

  if exists (
    select 1
    from _tmp_settle_in t
    left join public.pay_bank_transfers pbt
      on pbt.id = t.transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    where pbt.id is null
    limit 1
  ) then
    raise exception 'pay_settle_rail: one or more transfer_id values do not belong to the specified pay batch';
  end if;

  update public.pay_bank_transfers pbt
  set
    status = t.status,
    rail_tx_id = coalesce(t.rail_tx_id, pbt.rail_tx_id),
    rail_state = coalesce(t.rail_state, pbt.rail_state),
    rail_meta_json = case
      when t.rail_meta_json is null then pbt.rail_meta_json
      when pbt.rail_meta_json is null then t.rail_meta_json
      else (pbt.rail_meta_json || t.rail_meta_json)
    end,
    completed_at_utc = case
      when t.status = 'COMPLETED' then coalesce(pbt.completed_at_utc, v_now)
      else pbt.completed_at_utc
    end,
    failed_reason = case
      when t.status = 'FAILED' then coalesce(pbt.failed_reason, nullif(btrim(coalesce(t.rail_state,'')),''))
      else pbt.failed_reason
    end
  from _tmp_settle_in t
  where pbt.id = t.transfer_id
    and pbt.pay_batch_id = p_pay_batch_id;

  -- =========================================================
  -- A5 HARDENING (batch consistency guard):
  -- If any candidate has positive frozen net_bank_amount, there must be at least one linked payable transfer.
  -- (Covers "deleted transfers" and "cleared pay_bank_transfer_id links" bad states.)
  -- =========================================================
  select count(distinct pbi_chk.pay_bank_transfer_id)::int
  into v_linked_transfer_ct
  from public.pay_batch_items pbi_chk
  join public.pay_batch_candidates pbc_chk
    on pbc_chk.id = pbi_chk.pay_batch_candidate_id
  where pbc_chk.pay_batch_id = p_pay_batch_id
    and pbi_chk.item_type <> 'DEBT_CREATED'
    and pbi_chk.is_voided = false
    and pbi_chk.pay_bank_transfer_id is not null;

  select exists (
    select 1
    from public.pay_batch_candidates pbc_pos
    where pbc_pos.pay_batch_id = p_pay_batch_id
      and coalesce(pbc_pos.net_bank_amount,0) > 0
    limit 1
  )
  into v_has_pos_net;

  if v_linked_transfer_ct = 0 and v_has_pos_net then
    raise exception 'STATE_INCONSISTENT: positive net_bank_amount but no transfers';
  end if;

  create temp table if not exists _tmp_newly_settled_candidates (
    candidate_id uuid primary key
  ) on commit drop;

  truncate table _tmp_newly_settled_candidates;

  with cand_transfers as (
    select
      pbc.candidate_id as candidate_id,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.is_voided = false
          and pbi.pay_bank_transfer_id is not null
      ) as total_transfers,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.is_voided = false
          and pbi.pay_bank_transfer_id is not null
          and upper(coalesce(pbt.status,'')) = 'COMPLETED'
      ) as completed_transfers
    from public.pay_batch_candidates pbc
    left join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    left join public.pay_bank_transfers pbt
      on pbt.id = pbi.pay_bank_transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    where pbc.pay_batch_id = p_pay_batch_id
    group by pbc.candidate_id
  ),
  eligible as (
    select
      ct.candidate_id
    from cand_transfers ct
    join public.pay_batch_candidates pbc2
      on pbc2.pay_batch_id = p_pay_batch_id
     and pbc2.candidate_id = ct.candidate_id
    where (coalesce(pbc2.settled_at_utc, null) is null)
      and (
        (ct.total_transfers = 0 and coalesce(pbc2.net_bank_amount,0) <= 0)
        or (ct.total_transfers > 0 and ct.total_transfers = ct.completed_transfers)
      )
  )
  insert into _tmp_newly_settled_candidates(candidate_id)
  select e.candidate_id
  from eligible e;

  update public.pay_batch_candidates pbc
  set
    settlement_status = 'SETTLED',
    settled_at_utc = v_now,
    settled_via = upper(coalesce(v_batch.rail_provider_snapshot,'RAIL')),
    settled_note = null
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t);

  select coalesce(jsonb_agg(t.candidate_id::text order by t.candidate_id), '[]'::jsonb)
  into v_newly_settled_candidates
  from _tmp_newly_settled_candidates t;

  with needed_timesheets as (
    select distinct
      pbi.timesheet_id as timesheet_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
      and pbi.item_type <> 'DEBT_CREATED'
      and pbi.is_voided = false
      and pbi.timesheet_id is not null
  ),
  have_snap as (
    select distinct
      pbs.timesheet_id as timesheet_id
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
  ),
  missing as (
    select n.timesheet_id
    from needed_timesheets n
    left join have_snap h
      on h.timesheet_id = n.timesheet_id
    where h.timesheet_id is null
  )
  select coalesce(jsonb_agg(m.timesheet_id::text order by m.timesheet_id), '[]'::jsonb)
  into v_missing_timesheets
  from missing m;

  if jsonb_array_length(v_missing_timesheets) > 0 then
    raise exception 'pay_settle_rail: MISSING_FROZEN_SNAPSHOTS for timesheets %', v_missing_timesheets::text;
  end if;

  with snap as (
    select
      pbs.timesheet_id,
      pbs.target_snapshot_json
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
  ),
  ambig as (
    select
      s.timesheet_id
    from snap s
    group by s.timesheet_id
    having count(distinct s.target_snapshot_json) > 1
  )
  select coalesce(jsonb_agg(a.timesheet_id::text order by a.timesheet_id), '[]'::jsonb)
  into v_ambig_timesheets
  from ambig a;

  if jsonb_array_length(v_ambig_timesheets) > 0 then
    raise exception 'pay_settle_rail: AMBIGUOUS_TARGET_SNAPSHOT for timesheets %', v_ambig_timesheets::text;
  end if;

  with chosen as (
    select distinct on (pbs.timesheet_id)
      pbs.timesheet_id,
      pbs.target_snapshot_json,
      pbs.signature
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id
  )
  insert into public.timesheet_pay_state_history(
    timesheet_id,
    pay_batch_id,
    settled_at_utc,
    snapshot_json,
    signature
  )
  select
    c.timesheet_id,
    p_pay_batch_id,
    v_now,
    c.target_snapshot_json,
    c.signature
  from chosen c;

  with chosen as (
    select distinct on (pbs.timesheet_id)
      pbs.timesheet_id,
      pbs.target_snapshot_json,
      pbs.signature
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id
  )
  insert into public.timesheet_pay_state(
    timesheet_id,
    last_settled_snapshot_json,
    last_settled_signature,
    last_settled_pay_batch_id,
    last_settled_at_utc
  )
  select
    c.timesheet_id,
    c.target_snapshot_json,
    c.signature,
    p_pay_batch_id,
    v_now
  from chosen c
  on conflict (timesheet_id) do update
  set
    last_settled_snapshot_json = excluded.last_settled_snapshot_json,
    last_settled_signature = excluded.last_settled_signature,
    last_settled_pay_batch_id = excluded.last_settled_pay_batch_id,
    last_settled_at_utc = excluded.last_settled_at_utc;

  create temp table if not exists _tmp_loan_taken (
    advance_id uuid not null,
    week_start date not null,
    taken_amount numeric not null,
    primary key (advance_id, week_start)
  ) on commit drop;

  truncate table _tmp_loan_taken;

  insert into _tmp_loan_taken(advance_id, week_start, taken_amount)
  select
    nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid as advance_id,
    pbi.repayment_week_start as week_start,
    round(sum(abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0))),2) as taken_amount
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    and pbi.is_voided = false
    and pbi.item_type = 'LOAN_REPAYMENT'
    and pbi.repayment_week_start is not null
    and pbi.source_ref is not null
    and btrim(coalesce(pbi.source_ref,'')) like 'advance:%'
  group by
    nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid,
    pbi.repayment_week_start
  having round(sum(abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0))),2) > 0;

  for v_adv_id in
    select distinct lt.advance_id
    from _tmp_loan_taken lt
    where lt.advance_id is not null
  loop
    select
      pa.schedule_json,
      pa.outstanding_amount,
      pa.next_due_week_start
    into
      v_old_sched,
      v_old_out,
      v_old_next
    from public.pay_advances pa
    where pa.id = v_adv_id
    for update;

    if v_old_sched is null then
      v_old_sched := '[]'::jsonb;
    end if;

    select round(coalesce(sum(lt.taken_amount),0),2)
    into v_total_taken
    from _tmp_loan_taken lt
    where lt.advance_id = v_adv_id;

    v_new_out := round(greatest(coalesce(v_old_out,0) - coalesce(v_total_taken,0), 0), 2);

    with taken_map as (
      select
        lt.week_start,
        lt.taken_amount
      from _tmp_loan_taken lt
      where lt.advance_id = v_adv_id
    ),
    expanded as (
      select
        e.elem as elem,
        nullif(e.elem->>'week_start','')::date as wk,
        coalesce(nullif(e.elem->>'amount','')::numeric,0) as amt
      from jsonb_array_elements(coalesce(v_old_sched,'[]'::jsonb)) e(elem)
    ),
    rewritten as (
      select
        case
          when em.wk is not null
           and tm.week_start is not null
           and em.wk = tm.week_start
           and em.amt < 0
          then
            jsonb_set(
              em.elem,
              '{amount}',
              to_jsonb(round(em.amt + tm.taken_amount, 2)),
              true
            )
          else em.elem
        end as elem
      from expanded em
      left join taken_map tm
        on tm.week_start = em.wk
    )
    select coalesce(jsonb_agg(r.elem), '[]'::jsonb)
    into v_new_sched
    from rewritten r;

    with expanded2 as (
      select
        nullif(e2.elem->>'week_start','')::date as wk,
        coalesce(nullif(e2.elem->>'amount','')::numeric,0) as amt
      from jsonb_array_elements(coalesce(v_new_sched,'[]'::jsonb)) e2(elem)
    )
    select min(ex2.wk)
    into v_new_next
    from expanded2 ex2
    where ex2.wk is not null
      and ex2.amt < 0;

    update public.pay_advances pa2
    set
      schedule_json = coalesce(v_new_sched,'[]'::jsonb),
      outstanding_amount = v_new_out,
      next_due_week_start = v_new_next,
      status = case
        when v_new_out <= 0 or v_new_next is null then 'PAID_OFF'::pay_advance_status_enum
        else pa2.status
      end,
      updated_at = v_now
    where pa2.id = v_adv_id;

    insert into public.pay_advance_patches(
      advance_id,
      pay_batch_id,
      old_outstanding_amount,
      new_outstanding_amount,
      old_schedule_json,
      new_schedule_json,
      old_next_due_week_start,
      new_next_due_week_start
    )
    values (
      v_adv_id,
      p_pay_batch_id,
      v_old_out,
      v_new_out,
      v_old_sched,
      v_new_sched,
      v_old_next,
      v_new_next
    );
  end loop;

  create temp table if not exists _tmp_overpay_taken (
    advance_id uuid not null,
    taken_amount numeric not null,
    primary key (advance_id)
  ) on commit drop;

  truncate table _tmp_overpay_taken;

  insert into _tmp_overpay_taken(advance_id, taken_amount)
  select
    nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid as advance_id,
    round(sum(abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0))),2) as taken_amount
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    and pbi.is_voided = false
    and pbi.item_type = 'OVERPAYMENT_RECOVERY'
    and pbi.source_ref is not null
    and btrim(coalesce(pbi.source_ref,'')) like 'advance:%'
  group by
    nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid
  having round(sum(abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0))),2) > 0;

  v_overpay_patched_ct := 0;

  for v_adv_id in
    select distinct ot.advance_id
    from _tmp_overpay_taken ot
    where ot.advance_id is not null
  loop
    select
      pa.schedule_json,
      pa.outstanding_amount,
      pa.next_due_week_start
    into
      v_old_sched,
      v_old_out,
      v_old_next
    from public.pay_advances pa
    where pa.id = v_adv_id
      and pa.advance_kind = 'OVERPAYMENT'::public.pay_advance_kind_enum
    for update;

    select round(coalesce(ot.taken_amount,0),2)
    into v_total_taken
    from _tmp_overpay_taken ot
    where ot.advance_id = v_adv_id;

    v_new_out := round(greatest(coalesce(v_old_out,0) - coalesce(v_total_taken,0), 0), 2);

    v_new_sched := v_old_sched;
    v_new_next := v_old_next;

    update public.pay_advances pa2
    set
      outstanding_amount = v_new_out,
      status = case
        when v_new_out <= 0 then 'PAID_OFF'::pay_advance_status_enum
        else pa2.status
      end,
      updated_at = v_now
    where pa2.id = v_adv_id;

    insert into public.pay_advance_patches(
      advance_id,
      pay_batch_id,
      old_outstanding_amount,
      new_outstanding_amount,
      old_schedule_json,
      new_schedule_json,
      old_next_due_week_start,
      new_next_due_week_start
    )
    values (
      v_adv_id,
      p_pay_batch_id,
      v_old_out,
      v_new_out,
      v_old_sched,
      v_new_sched,
      v_old_next,
      v_new_next
    );

    v_overpay_patched_ct := v_overpay_patched_ct + 1;
  end loop;

  v_loan_payout_updated_ct := 0;

  if upper(btrim(coalesce(v_batch.batch_kind_fixed,''))) = 'LOANS' then
    with payouts as (
      select distinct
        replace(pbi.source_ref, 'advance:', '')::uuid as loan_id,
        pbi.pay_bank_transfer_id as transfer_id
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc
        on pbc.id = pbi.pay_batch_candidate_id
      join public.pay_bank_transfers pbt
        on pbt.id = pbi.pay_bank_transfer_id
       and pbt.pay_batch_id = p_pay_batch_id
      where pbc.pay_batch_id = p_pay_batch_id
        and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
        and pbi.is_voided = false
        and pbi.item_type = 'LOAN_PAYOUT'
        and pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
        and pbi.pay_bank_transfer_id is not null
        and upper(coalesce(pbt.status,'')) = 'COMPLETED'
    ),
    upd as (
      update public.pay_advances pa
      set
        payout_status = 'PAID'::public.pay_advance_payout_status_enum,
        payout_pay_batch_id = p_pay_batch_id,
        payout_transfer_id = p.transfer_id,
        updated_at = v_now
      from payouts p
      where pa.id = p.loan_id
        and pa.advance_kind = 'LOAN'::public.pay_advance_kind_enum
        and coalesce(pa.payout_status::text,'') <> 'PAID'
      returning pa.id
    )
    select count(*)::int
    into v_loan_payout_updated_ct
    from upd;
  end if;

  with payable_transfer_ids as (
    select distinct
      pbi.pay_bank_transfer_id as transfer_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.item_type <> 'DEBT_CREATED'
      and pbi.is_voided = false
      and pbi.pay_bank_transfer_id is not null
  ),
  stats as (
    select
      sum(case when upper(coalesce(pbt.status,'')) = 'PENDING' then 1 else 0 end)::int as pending_ct,
      sum(case when upper(coalesce(pbt.status,'')) = 'COMPLETED' then 1 else 0 end)::int as completed_ct,
      sum(case when upper(coalesce(pbt.status,'')) = 'FAILED' then 1 else 0 end)::int as failed_ct,
      sum(case when upper(coalesce(pbt.status,'')) = 'BLOCKED' then 1 else 0 end)::int as blocked_ct
    from payable_transfer_ids pti
    join public.pay_bank_transfers pbt
      on pbt.id = pti.transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
  )
  select
    case
      when coalesce(s.pending_ct,0) = 0 and coalesce(s.failed_ct,0) = 0 and coalesce(s.blocked_ct,0) = 0 then 'SETTLED'
      when coalesce(s.pending_ct,0) = 0 and coalesce(s.failed_ct,0) > 0 then 'FAILED'
      else 'PARTIAL'
    end
  into v_batch_status
  from stats s;

  -- ✅ Updated: always bump last_status_checked_at_utc for poll throttling (even if settlement_json was empty/no-op)
  update public.pay_batches pb2
  set
    status = v_batch_status,
    completed_at_utc = case when v_batch_status = 'SETTLED' then coalesce(pb2.completed_at_utc, v_now) else pb2.completed_at_utc end,
    last_status_checked_at_utc = v_now
  where pb2.id = p_pay_batch_id;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_pending_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json,
        'failed_reason', pbt.failed_reason
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_failed_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'FAILED';

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_blocked_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'BLOCKED';

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_SETTLE_RAIL:COUNTS',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'is_stale', v_is_stale,
        'stale_reasons', v_stale_reasons,
        'newly_settled_candidates_count', jsonb_array_length(coalesce(v_newly_settled_candidates,'[]'::jsonb)),
        'overpayment_patches_applied', v_overpay_patched_ct,
        'loan_payouts_marked_paid', v_loan_payout_updated_ct,
        'batch_status', v_batch_status
      ),
      'pay_batches',
      p_pay_batch_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'batch_status', (select pb3.status from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'newly_settled_candidates', v_newly_settled_candidates,
    'still_pending_transfers', v_pending_transfers,
    'failed_transfers', v_failed_transfers,
    'blocked_transfers', v_blocked_transfers,
    'freshness', jsonb_build_object(
      'is_stale', v_is_stale,
      'stale_reasons', v_stale_reasons,
      'diff_sample', v_diff_sample
    ),
    'patch_summary', jsonb_build_object(
      'overpayment_patches_applied', v_overpay_patched_ct,
      'loan_payouts_marked_paid', v_loan_payout_updated_ct
    )
  );
end;
$$;







create or replace function public.pay_remittance_mark_sent(
  p_pay_batch_id uuid,
  p_scope text,
  p_results_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(btrim(coalesce(p_scope, 'ALL')));
  v_exists boolean := false;
  v_audit_id uuid;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_remittance_mark_sent: pay_batch_id is required';
  end if;

  if v_scope not in ('ALL','PAYE','UMBRELLA') then
    raise exception 'pay_remittance_mark_sent: invalid scope "%". Expected ALL|PAYE|UMBRELLA.', v_scope;
  end if;

  select exists(
    select 1
    from public.pay_batches pb
    where pb.id = p_pay_batch_id
  )
  into v_exists;

  if v_exists is false then
    raise exception 'pay_remittance_mark_sent: pay batch % not found.', p_pay_batch_id;
  end if;

  insert into public.audit_events(
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  )
  values (
    p_actor_user_id,
    'pay_batch',
    p_pay_batch_id::text,
    'PAY_REMITTANCE_SENT',
    null,
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'scope', v_scope,
      'results', coalesce(p_results_json, 'null'::jsonb)
    ),
    'remittance_mark_sent'
  )
  returning id into v_audit_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'scope', v_scope,
    'audit_event_id', v_audit_id::text
  );
end;
$$;

CREATE OR REPLACE FUNCTION public.pay_remittance_build(p_pay_batch_id uuid, p_scope text DEFAULT 'ALL'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_scope text := upper(btrim(coalesce(p_scope, 'ALL')));
  v_batch record;

  v_jobs_umb jsonb := '[]'::jsonb;
  v_jobs_paye jsonb := '[]'::jsonb;
  v_jobs_cand_umb_copy jsonb := '[]'::jsonb;

  -- ✅ Remittance settings (single-row settings_defaults)
  v_remittance_includes_json jsonb := null;
  v_remittance_header_message text := null;
  v_remittance_footer_message text := null;

  -- ✅ New defaults
  v_sd_paye_remittances_enabled boolean := false;
  v_sd_remittances_detailed_breakdown boolean := false;
  v_sd_remittance_receive_when_umbrella_paid boolean := false;

  -- ✅ derived defaults for config evaluation
  v_missing_scope text := 'WEEKLY';
  v_unknown_item_type_default boolean := true;
  v_unknown_item_type_default_text text := 'true';
begin
  if p_pay_batch_id is null then
    raise exception 'pay_remittance_build: pay_batch_id is required';
  end if;

  if v_scope not in ('ALL','PAYE','UMBRELLA') then
    raise exception 'pay_remittance_build: invalid scope "%". Expected ALL|PAYE|UMBRELLA.', v_scope;
  end if;

  -- ✅ Load remittance settings + new defaults (single row, id=1 semantics)
  select
    sd.remittance_includes_json,
    sd.remittance_header_message,
    sd.remittance_footer_message,
    coalesce(sd.paye_remittances_enabled,false) as paye_remittances_enabled,
    coalesce(sd.remittances_detailed_breakdown,false) as remittances_detailed_breakdown,
    coalesce(sd.remittance_receive_when_umbrella_paid,false) as remittance_receive_when_umbrella_paid
  into
    v_remittance_includes_json,
    v_remittance_header_message,
    v_remittance_footer_message,
    v_sd_paye_remittances_enabled,
    v_sd_remittances_detailed_breakdown,
    v_sd_remittance_receive_when_umbrella_paid
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  -- ✅ Normalize include config defaults (safe when config is NULL or not an object)
  if v_remittance_includes_json is not null and jsonb_typeof(v_remittance_includes_json) = 'object' then
    v_missing_scope := upper(btrim(coalesce(v_remittance_includes_json->'defaults'->>'missing_scope', 'WEEKLY')));

    v_unknown_item_type_default_text := lower(btrim(coalesce(v_remittance_includes_json->'defaults'->>'unknown_item_type', 'true')));
    v_unknown_item_type_default := (v_unknown_item_type_default_text in ('true','1','yes','y','on'));
  else
    v_missing_scope := 'WEEKLY';
    v_unknown_item_type_default := true;
  end if;

  if v_unknown_item_type_default then
    v_unknown_item_type_default_text := 'true';
  else
    v_unknown_item_type_default_text := 'false';
  end if;

  select
    pb.id,
    pb.pay_date,
    pb.status,
    pb.bulk_reference,
    pb.banking_system_snapshot,
    pb.external_paye_system_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id;

  if v_batch.id is null then
    raise exception 'pay_remittance_build: pay batch % not found.', p_pay_batch_id;
  end if;

  -- ============================================================
  -- Umbrella remittance jobs (recipient = umbrella)
  -- HARD RULES:
  --  - generate only when umbrella transfers exist AND umbrella remittance_email exists
  --  - detailed_breakdown resolved from umbrella overrides vs settings_defaults
  --  - when detailed_breakdown=true and SEGMENT timesheet: include schedule rows (worked shifts only)
  --  - aggregate timesheets never include schedules
  --  - include canonical breakdown lines via pay_batch_item_breakdowns (no derived rates)
  -- ============================================================
  if v_scope in ('ALL','UMBRELLA') then
    with umb_transfers as (
      select
        pbt.id as transfer_id,
        pbt.pay_batch_id,
        pbt.candidate_id,
        pbt.umbrella_id,
        pbt.pay_channel,
        pbt.amount,
        pbt.currency,
        pbt.status,
        pbt.payment_reference,
        pbt.completed_at_utc,
        pbt.rail_tx_id,
        pbt.rail_state,
        pbt.rail_meta_json,
        pbt.failed_reason,
        (
          upper(coalesce(pbt.rail_state,'')) = 'PAYROLL_TESTING'
          or (
            pbt.rail_meta_json is not null
            and (pbt.rail_meta_json ? 'simulated')
            and lower(btrim(coalesce(pbt.rail_meta_json->>'simulated',''))) in ('true','1','yes','y','on')
          )
        ) as is_simulated
      from public.pay_bank_transfers pbt
      where pbt.pay_batch_id = p_pay_batch_id
        and upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA'
        and upper(coalesce(pbt.status,'')) = 'COMPLETED'
        and pbt.umbrella_id is not null
        and pbt.candidate_id is not null
    ),
    umb_roster as (
      select
        ut.umbrella_id,
        max(u.name) as umbrella_name,
        max(u.remittance_email) as remittance_email,
        max(coalesce(u.remittance_overrides_enabled,false)) as umb_override_enabled,
        max(coalesce(u.remittances_detailed_breakdown,false)) as umb_detail_enabled,
        bool_or(coalesce(ut.is_simulated,false)) as test_mode
      from umb_transfers ut
      join public.umbrellas u
        on u.id = ut.umbrella_id
      group by ut.umbrella_id
      having nullif(btrim(coalesce(max(u.remittance_email),'')),'') is not null
    ),
    umb_job_flags as (
      select
        ur.umbrella_id,
        case
          when ur.umb_override_enabled then ur.umb_detail_enabled
          else v_sd_remittances_detailed_breakdown
        end as detailed_breakdown
      from umb_roster ur
    ),
    umb_candidates as (
      select distinct
        ut.umbrella_id,
        ut.candidate_id
      from umb_transfers ut
    ),
    cand_meta as (
      select
        uc.umbrella_id,
        c.id as candidate_id,
        c.tms_ref as tms_ref,
        c.display_name as display_name,
        c.email as email
      from umb_candidates uc
      join public.candidates c
        on c.id = uc.candidate_id
    ),
    cand_items as (
      select
        uc.umbrella_id,
        pbc.candidate_id,
        pbi.id as pay_batch_item_id,
        pbi.item_type,
        pbi.description,
        pbi.timesheet_id,
        pbi.segment_key,
        pbi.source_ref,
        pbi.amount_ex_vat,
        pbi.amount_vat,
        pbi.amount_inc_vat,
        pbi.pay_channel,
        pbi.umbrella_id,
        upper(coalesce(vs.sheet_scope::text, v_missing_scope)) as sheet_scope_norm
      from umb_candidates uc
      join public.pay_batch_candidates pbc
        on pbc.pay_batch_id = p_pay_batch_id
       and pbc.candidate_id = uc.candidate_id
      join public.pay_batch_items pbi
        on pbi.pay_batch_candidate_id = pbc.id
      left join public.v_timesheets_summary vs
        on vs.timesheet_id = pbi.timesheet_id
      where upper(coalesce(pbi.pay_channel,'')) = 'UMBRELLA'
        and pbi.item_type <> 'DEBT_CREATED'
        and coalesce(pbi.is_voided,false) = false
        and (
          case
            when upper(coalesce(vs.sheet_scope::text, v_missing_scope)) = 'DAILY' then
              (case
                 when lower(coalesce(v_remittance_includes_json->'daily'->'include_item_types'->>pbi.item_type, v_unknown_item_type_default_text)) in ('true','1','yes','y','on')
                 then true else false end)
            else
              (case
                 when lower(coalesce(v_remittance_includes_json->'weekly'->'include_item_types'->>pbi.item_type, v_unknown_item_type_default_text)) in ('true','1','yes','y','on')
                 then true else false end)
          end
        )
    ),
    ts_meta as (
      select
        ci.umbrella_id,
        ci.candidate_id,
        ts.timesheet_id,
        ts.week_ending_date,
        ts.reference_number,
        ts.contract_id,
        nullif(btrim(coalesce(ts.job_title_norm,'')),'') as job_title_norm,
        nullif(btrim(coalesce(ts.band,'')),'') as band_norm
      from cand_items ci
      join public.timesheets ts
        on ts.timesheet_id = ci.timesheet_id
       and ts.is_current = true
      group by
        ci.umbrella_id, ci.candidate_id, ts.timesheet_id,
        ts.week_ending_date, ts.reference_number, ts.contract_id, ts.job_title_norm, ts.band
    ),
    ts_enrich as (
      select
        tm.umbrella_id,
        tm.candidate_id,
        tm.timesheet_id,
        tm.week_ending_date,
        tm.reference_number,
        vs.client_id as client_id,
        vs.client_name as client_name,
        upper(coalesce(vs.sheet_scope::text, v_missing_scope)) as sheet_scope,
        nullif(btrim(coalesce(ct.role, tm.job_title_norm)),'') as job_title,
        nullif(btrim(coalesce(ct.band, tm.band_norm)),'') as band
      from ts_meta tm
      left join public.contracts ct
        on ct.id = tm.contract_id
      left join public.v_timesheets_summary vs
        on vs.timesheet_id = tm.timesheet_id
    ),
    ts_snap as (
      select
        tss.pay_batch_id,
        tss.timesheet_id,
        tss.candidate_id,
        tss.pay_channel,
        tss.base_snapshot_json,
        tss.target_snapshot_json
      from public.pay_batch_timesheet_snapshots tss
      where tss.pay_batch_id = p_pay_batch_id
        and upper(coalesce(tss.pay_channel,'')) = 'UMBRELLA'
    ),
    ts_class as (
      select
        te.umbrella_id,
        te.candidate_id,
        te.timesheet_id,
        te.week_ending_date,
        te.reference_number,
        te.client_id,
        te.client_name,
        te.sheet_scope,
        te.job_title,
        te.band,
        tsn.base_snapshot_json,
        tsn.target_snapshot_json,
        case
          when tsn.target_snapshot_json is not null
           and jsonb_typeof(tsn.target_snapshot_json) = 'object'
           and jsonb_typeof(tsn.target_snapshot_json->'segments') = 'array'
           and exists (
             select 1
             from jsonb_array_elements(tsn.target_snapshot_json->'segments') s
             where s is not null
               and jsonb_typeof(s)='object'
               and nullif(btrim(coalesce(s->>'date','')),'') is not null
           )
          then 'SEGMENT'
          else 'AGGREGATE'
        end as timesheet_render_mode,
        case
          when tsn.base_snapshot_json is null then 'Standard'
          when tsn.base_snapshot_json = '{}'::jsonb then 'Standard'
          when (
            exists (
              select 1
              from jsonb_array_elements(coalesce(tsn.base_snapshot_json->'segments','[]'::jsonb)) s
              where s is not null and jsonb_typeof(s)='object'
                and round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) <> 0
            )
            or round(coalesce(nullif(tsn.base_snapshot_json->>'additional_pay_ex_vat','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),2) <> 0
            or (
              jsonb_typeof(tsn.base_snapshot_json->'adjustments')='array'
              and jsonb_array_length(tsn.base_snapshot_json->'adjustments') > 0
            )
          )
          then 'Adjustment'
          else 'Standard'
        end as timesheet_type
      from ts_enrich te
      left join ts_snap tsn
        on tsn.timesheet_id = te.timesheet_id
       and tsn.candidate_id = te.candidate_id
    ),
    breakdown_raw as (
      select
        ci.umbrella_id,
        ci.candidate_id,
        ci.timesheet_id,
        pbib.line_kind,
        pbib.bucket_code,
        pbib.unit_name,
        pbib.units,
        pbib.rate,
        pbib.amount_ex_vat,
        pbib.amount_vat,
        pbib.amount_inc_vat
      from cand_items ci
      join public.pay_batch_item_breakdowns pbib
        on pbib.pay_batch_item_id = ci.pay_batch_item_id
      where ci.timesheet_id is not null
    ),
    ts_unit_rows as (
      select
        br.umbrella_id,
        br.candidate_id,
        br.timesheet_id,
        br.line_kind,
        br.bucket_code,
        br.unit_name,
        br.rate,
        round(sum(coalesce(br.units,0)),2) as quantity,
        round(sum(coalesce(br.amount_ex_vat,0)),2) as total_ex_vat,
        round(sum(coalesce(br.amount_vat,0)),2) as total_vat,
        round(sum(coalesce(br.amount_inc_vat,0)),2) as total_inc_vat
      from breakdown_raw br
      group by br.umbrella_id, br.candidate_id, br.timesheet_id, br.line_kind, br.bucket_code, br.unit_name, br.rate
      having
        round(sum(coalesce(br.units,0)),2) <> 0
        or round(sum(coalesce(br.amount_ex_vat,0)),2) <> 0
        or round(sum(coalesce(br.amount_inc_vat,0)),2) <> 0
    ),
    ts_sections as (
      select
        tc.umbrella_id,
        tc.candidate_id,
        tc.timesheet_id,
        tc.week_ending_date,
        tc.reference_number,
        tc.client_id,
        tc.client_name,
        tc.job_title,
        tc.band,
        tc.timesheet_render_mode,
        tc.timesheet_type,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by
              case upper(coalesce(r.bucket_code,''))
                when 'DAY' then 1
                when 'NIGHT' then 2
                when 'SAT' then 3
                when 'SUN' then 4
                when 'BH' then 5
                else 99
              end,
              coalesce(r.unit_name,'') asc,
              coalesce(r.rate,0) asc
          )
          filter (where upper(coalesce(r.line_kind,'')) = 'TS_BUCKET'),
          '[]'::jsonb
        ) as unit_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by coalesce(r.unit_name,'') asc, coalesce(r.rate,0) asc
          )
          filter (where upper(coalesce(r.line_kind,'')) = 'ADDITIONAL_UNIT'),
          '[]'::jsonb
        ) as additional_units_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by coalesce(r.unit_name,'') asc
          )
          filter (where upper(coalesce(r.line_kind,'')) in ('EXPENSE','MILEAGE')),
          '[]'::jsonb
        ) as expenses_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat,
              'line_kind', r.line_kind
            )
            order by coalesce(r.unit_name,'') asc
          )
          filter (where upper(coalesce(r.line_kind,'')) in ('ADJUSTMENT','CONVERSION_ADJ','LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','LOAN_PAYOUT','DEBT_CREATED')),
          '[]'::jsonb
        ) as other_rows,
        round(coalesce(sum(r.total_ex_vat),0),2) as totals_ex_vat,
        round(coalesce(sum(r.total_vat),0),2) as totals_vat,
        round(coalesce(sum(r.total_inc_vat),0),2) as totals_inc_vat,
        tc.base_snapshot_json,
        tc.target_snapshot_json
      from ts_class tc
      left join ts_unit_rows r
        on r.umbrella_id = tc.umbrella_id
       and r.candidate_id = tc.candidate_id
       and r.timesheet_id = tc.timesheet_id
      group by
        tc.umbrella_id,
        tc.candidate_id,
        tc.timesheet_id,
        tc.week_ending_date,
        tc.reference_number,
        tc.client_id,
        tc.client_name,
        tc.job_title,
        tc.band,
        tc.timesheet_render_mode,
        tc.timesheet_type,
        tc.base_snapshot_json,
        tc.target_snapshot_json
    ),
    ts_schedule_rows_all as (
      select
        ts.umbrella_id,
        ts.candidate_id,
        ts.timesheet_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'segment_id', nullif(btrim(coalesce(s->>'segment_id','')),''),
              'date', nullif(btrim(coalesce(s->>'date','')),''),
              'start_utc', nullif(btrim(coalesce(s->>'start_utc','')),''),
              'end_utc', nullif(btrim(coalesce(s->>'end_utc','')),''),
              'break_mins', coalesce(nullif(s->>'break_mins','')::numeric,0),
              'breaks', coalesce(s->'breaks','[]'::jsonb)
            )
            order by
              nullif(btrim(coalesce(s->>'date','')),'') asc,
              nullif(btrim(coalesce(s->>'start_utc','')),'') asc,
              nullif(btrim(coalesce(s->>'segment_id','')),'') asc
          ),
          '[]'::jsonb
        ) as schedule_rows
      from ts_sections ts
      join lateral jsonb_array_elements(coalesce(ts.target_snapshot_json->'segments','[]'::jsonb)) s
        on ts.target_snapshot_json is not null
       and jsonb_typeof(ts.target_snapshot_json)='object'
      where ts.timesheet_render_mode = 'SEGMENT'
        and s is not null
        and jsonb_typeof(s)='object'
        and coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) = false
        and (
          coalesce(nullif(s->>'hours_day','')::numeric,0)
          + coalesce(nullif(s->>'hours_night','')::numeric,0)
          + coalesce(nullif(s->>'hours_sat','')::numeric,0)
          + coalesce(nullif(s->>'hours_sun','')::numeric,0)
          + coalesce(nullif(s->>'hours_bh','')::numeric,0)
        ) > 0
      group by ts.umbrella_id, ts.candidate_id, ts.timesheet_id
    ),
    ts_schedule_changes_all as (
      select
        ts.umbrella_id,
        ts.candidate_id,
        ts.timesheet_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'segment_id', seg_id,
              'date', coalesce(before_seg->>'date', after_seg->>'date'),
              'before', jsonb_build_object(
                'start_utc', before_seg->>'start_utc',
                'end_utc', before_seg->>'end_utc',
                'break_mins', coalesce(nullif(before_seg->>'break_mins','')::numeric,0),
                'breaks', coalesce(before_seg->'breaks','[]'::jsonb),
                'exclude_from_pay', coalesce(nullif(before_seg->>'exclude_from_pay','')::boolean,false)
              ),
              'after', jsonb_build_object(
                'start_utc', after_seg->>'start_utc',
                'end_utc', after_seg->>'end_utc',
                'break_mins', coalesce(nullif(after_seg->>'break_mins','')::numeric,0),
                'breaks', coalesce(after_seg->'breaks','[]'::jsonb),
                'exclude_from_pay', coalesce(nullif(after_seg->>'exclude_from_pay','')::boolean,false)
              )
            )
            order by
              coalesce(before_seg->>'date', after_seg->>'date') asc,
              coalesce(before_seg->>'start_utc', after_seg->>'start_utc') asc,
              seg_id asc
          ),
          '[]'::jsonb
        ) as schedule_changes
      from (
        select
          ts0.umbrella_id,
          ts0.candidate_id,
          ts0.timesheet_id,
          nullif(btrim(coalesce(ids->>'segment_id','')),'') as seg_id,
          bseg.seg as before_seg,
          aseg.seg as after_seg
        from ts_sections ts0
        join lateral (
          select s
          from (
            select s0 as s from jsonb_array_elements(coalesce(ts0.base_snapshot_json->'segments','[]'::jsonb)) s0
            union all
            select s1 as s from jsonb_array_elements(coalesce(ts0.target_snapshot_json->'segments','[]'::jsonb)) s1
          ) u
          where u.s is not null and jsonb_typeof(u.s)='object'
        ) ids
          on ts0.timesheet_render_mode = 'SEGMENT'
         and ts0.timesheet_type = 'Adjustment'
        left join lateral (
          select s as seg
          from jsonb_array_elements(coalesce(ts0.base_snapshot_json->'segments','[]'::jsonb)) s
          where s is not null and jsonb_typeof(s)='object'
            and nullif(btrim(coalesce(s->>'segment_id','')),'') = nullif(btrim(coalesce(ids->>'segment_id','')),'')
          limit 1
        ) bseg on true
        left join lateral (
          select s as seg
          from jsonb_array_elements(coalesce(ts0.target_snapshot_json->'segments','[]'::jsonb)) s
          where s is not null and jsonb_typeof(s)='object'
            and nullif(btrim(coalesce(s->>'segment_id','')),'') = nullif(btrim(coalesce(ids->>'segment_id','')),'')
          limit 1
        ) aseg on true
      ) x
      join ts_sections ts
        on ts.umbrella_id = x.umbrella_id
       and ts.candidate_id = x.candidate_id
       and ts.timesheet_id = x.timesheet_id
      where x.seg_id is not null
        and (
          coalesce(x.before_seg->>'start_utc','') <> coalesce(x.after_seg->>'start_utc','')
          or coalesce(x.before_seg->>'end_utc','') <> coalesce(x.after_seg->>'end_utc','')
          or coalesce(nullif(x.before_seg->>'break_mins','')::numeric,0) <> coalesce(nullif(x.after_seg->>'break_mins','')::numeric,0)
          or coalesce(x.before_seg->'breaks','[]'::jsonb) <> coalesce(x.after_seg->'breaks','[]'::jsonb)
          or coalesce(nullif(x.before_seg->>'exclude_from_pay','')::boolean,false) <> coalesce(nullif(x.after_seg->>'exclude_from_pay','')::boolean,false)
        )
      group by ts.umbrella_id, ts.candidate_id, ts.timesheet_id
    ),
    cand_timesheets as (
      select
        ts.umbrella_id,
        ts.candidate_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'timesheet_id', ts.timesheet_id::text,
              'week_ending_date', case when ts.week_ending_date is null then null else ts.week_ending_date::text end,
              'reference_number', ts.reference_number,
              'client_id', case when ts.client_id is null then null else ts.client_id::text end,
              'client_name', ts.client_name,
              'job_title', ts.job_title,
              'band', ts.band,
              'timesheet_render_mode', ts.timesheet_render_mode,
              'timesheet_type', ts.timesheet_type,
              'unit_rows', ts.unit_rows,
              'additional_units_rows', ts.additional_units_rows,
              'expenses_rows', ts.expenses_rows,
              'other_rows', ts.other_rows,
              'totals', jsonb_build_object(
                'total_ex_vat', ts.totals_ex_vat,
                'vat', ts.totals_vat,
                'total_inc_vat', ts.totals_inc_vat
              ),
              'schedule_rows', case
                when coalesce(ujf.detailed_breakdown,false) = true and ts.timesheet_render_mode = 'SEGMENT'
                  then coalesce(sr.schedule_rows,'[]'::jsonb)
                else '[]'::jsonb
              end,
              'schedule_changes', case
                when coalesce(ujf.detailed_breakdown,false) = true and ts.timesheet_render_mode = 'SEGMENT' and ts.timesheet_type = 'Adjustment'
                  then coalesce(sc.schedule_changes,'[]'::jsonb)
                else '[]'::jsonb
              end
            )
            order by ts.week_ending_date desc nulls last, ts.client_name nulls last, ts.reference_number nulls last, ts.timesheet_id
          ),
          '[]'::jsonb
        ) as timesheets_json
      from ts_sections ts
      join umb_job_flags ujf
        on ujf.umbrella_id = ts.umbrella_id
      left join ts_schedule_rows_all sr
        on sr.umbrella_id = ts.umbrella_id
       and sr.candidate_id = ts.candidate_id
       and sr.timesheet_id = ts.timesheet_id
      left join ts_schedule_changes_all sc
        on sc.umbrella_id = ts.umbrella_id
       and sc.candidate_id = ts.candidate_id
       and sc.timesheet_id = ts.timesheet_id
      group by ts.umbrella_id, ts.candidate_id
    ),
    candidate_payload as (
      select
        cm.umbrella_id,
        cm.candidate_id,
        jsonb_build_object(
          'candidate_id', cm.candidate_id::text,
          'tms_ref', cm.tms_ref,
          'display_name', cm.display_name,
          'email', cm.email,
          'timesheets', coalesce(ct.timesheets_json,'[]'::jsonb),
          'totals', coalesce((
            select jsonb_build_object(
              'gross_preview', round(coalesce(pbc_tot.gross_preview,0),2),
              'overpayment_recovery_taken', round(coalesce(pbc_tot.overpayment_recovery_taken,0),2),
              'loan_repayment_taken', round(coalesce(pbc_tot.loan_repayment_taken,0),2),
              'final_paid', round(coalesce(pbc_tot.net_bank_amount,0),2)
            )
            from public.pay_batch_candidates pbc_tot
            where pbc_tot.pay_batch_id = p_pay_batch_id
              and pbc_tot.candidate_id = cm.candidate_id
            limit 1
          ), '{}'::jsonb),
          'non_timesheet_lines', coalesce((
            select coalesce(
              jsonb_agg(
                jsonb_build_object(
                  'line_kind', pbib_nt.line_kind,
                  'bucket_code', pbib_nt.bucket_code,
                  'unit', pbib_nt.unit_name,
                  'quantity', pbib_nt.units,
                  'rate', pbib_nt.rate,
                  'total_ex_vat', pbib_nt.amount_ex_vat,
                  'total_vat', pbib_nt.amount_vat,
                  'total_inc_vat', pbib_nt.amount_inc_vat
                )
                order by coalesce(pbib_nt.line_kind,''), coalesce(pbib_nt.unit_name,''), coalesce(pbib_nt.rate,0)
              ),
              '[]'::jsonb
            )
            from public.pay_batch_candidates pbc_nt
            join public.pay_batch_items pbi_nt
              on pbi_nt.pay_batch_candidate_id = pbc_nt.id
            join public.pay_batch_item_breakdowns pbib_nt
              on pbib_nt.pay_batch_item_id = pbi_nt.id
            where pbc_nt.pay_batch_id = p_pay_batch_id
              and pbc_nt.candidate_id = cm.candidate_id
              and pbi_nt.is_voided = false
              and pbi_nt.timesheet_id is null
              and pbi_nt.item_type in ('LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','LOAN_PAYOUT')
          ), '[]'::jsonb)
        ) as candidate_json
      from cand_meta cm
      left join cand_timesheets ct
        on ct.umbrella_id = cm.umbrella_id
       and ct.candidate_id = cm.candidate_id
    ),
    umb_job as (
      select
        ur.umbrella_id,
        ur.umbrella_name,
        ur.remittance_email,
        ur.test_mode,
        ujf.detailed_breakdown,
        round(coalesce(sum(ut.amount),0),2) as umbrella_total_amount,
        coalesce(
          jsonb_agg(cp.candidate_json order by (cp.candidate_json->>'display_name') nulls last, (cp.candidate_json->>'tms_ref') nulls last),
          '[]'::jsonb
        ) as candidates_json
      from umb_roster ur
      join umb_job_flags ujf
        on ujf.umbrella_id = ur.umbrella_id
      join umb_transfers ut
        on ut.umbrella_id = ur.umbrella_id
      join candidate_payload cp
        on cp.umbrella_id = ur.umbrella_id
      group by ur.umbrella_id, ur.umbrella_name, ur.remittance_email, ur.test_mode, ujf.detailed_breakdown
    )
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'job_kind', 'UMBRELLA_REMITTANCE',
          'pay_batch_id', v_batch.id::text,
          'scope', 'UMBRELLA',
          'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
          'bulk_reference', v_batch.bulk_reference,
          'test_mode', coalesce(uj.test_mode,false),
          'detailed_breakdown', coalesce(uj.detailed_breakdown,false),
          'recipient', jsonb_build_object(
            'entity_kind', 'UMBRELLA',
            'umbrella_id', uj.umbrella_id::text,
            'name', uj.umbrella_name,
            'remittance_email', uj.remittance_email
          ),
          'summary', jsonb_build_object(
            'total_amount', uj.umbrella_total_amount,
            'currency', 'GBP'
          ),
          'candidates', coalesce(uj.candidates_json,'[]'::jsonb)
        )
        order by uj.umbrella_name nulls last, uj.umbrella_id
      ),
      '[]'::jsonb
    )
    into v_jobs_umb
    from umb_job uj;
  end if;

  -- ============================================================
  -- Candidate PAYE remittance jobs (recipient = candidate)
  -- LOCKED RULE:
  --  - generated only if effective receive is true:
  --    overrides? candidate.remittance_receive_enabled : settings_defaults.paye_remittances_enabled
  --  - detailed_breakdown resolved from candidate overrides vs settings_defaults
  -- ============================================================
  if v_scope in ('ALL','PAYE') then
    with paye_transfers as (
      select
        pbt.id as transfer_id,
        pbt.pay_batch_id,
        pbt.candidate_id,
        pbt.pay_channel,
        pbt.amount,
        pbt.currency,
        pbt.status,
        pbt.payment_reference,
        pbt.completed_at_utc,
        pbt.rail_tx_id,
        pbt.rail_state,
        pbt.rail_meta_json,
        pbt.failed_reason,
        (
          upper(coalesce(pbt.rail_state,'')) = 'PAYROLL_TESTING'
          or (
            pbt.rail_meta_json is not null
            and (pbt.rail_meta_json ? 'simulated')
            and lower(btrim(coalesce(pbt.rail_meta_json->>'simulated',''))) in ('true','1','yes','y','on')
          )
        ) as is_simulated
      from public.pay_bank_transfers pbt
      where pbt.pay_batch_id = p_pay_batch_id
        and upper(coalesce(pbt.pay_channel,'')) = 'PAYE'
        and upper(coalesce(pbt.status,'')) = 'COMPLETED'
        and pbt.candidate_id is not null
    ),
    cand_cfg as (
      select
        c.id as candidate_id,
        c.tms_ref,
        c.display_name,
        c.email,
        coalesce(c.remittance_overrides_enabled,false) as overrides_enabled,
        coalesce(c.remittance_receive_enabled,false) as paye_receive_enabled,
        coalesce(c.remittances_detailed_breakdown,false) as detailed_enabled
      from public.candidates c
      where c.id in (select distinct pt.candidate_id from paye_transfers pt)
    ),
    cand_effective as (
      select
        cc.*,
        (case when cc.overrides_enabled then cc.paye_receive_enabled else v_sd_paye_remittances_enabled end) as eff_paye_receive,
        (case when cc.overrides_enabled then cc.detailed_enabled else v_sd_remittances_detailed_breakdown end) as eff_detailed
      from cand_cfg cc
    ),
    cand_allowed as (
      select *
      from cand_effective ce
      where ce.eff_paye_receive = true
    ),
    cand_items as (
      select
        ca.candidate_id,
        pbi.id as pay_batch_item_id,
        pbi.item_type,
        pbi.description,
        pbi.timesheet_id,
        pbi.segment_key,
        pbi.source_ref,
        pbi.amount_ex_vat,
        pbi.amount_vat,
        pbi.amount_inc_vat,
        pbi.pay_channel
      from cand_allowed ca
      join public.pay_batch_candidates pbc
        on pbc.pay_batch_id = p_pay_batch_id
       and pbc.candidate_id = ca.candidate_id
      join public.pay_batch_items pbi
        on pbi.pay_batch_candidate_id = pbc.id
      left join public.v_timesheets_summary vs
        on vs.timesheet_id = pbi.timesheet_id
      where upper(coalesce(pbi.pay_channel,'')) = 'PAYE'
        and pbi.item_type <> 'DEBT_CREATED'
        and coalesce(pbi.is_voided,false) = false
        and (
          case
            when upper(coalesce(vs.sheet_scope::text, v_missing_scope)) = 'DAILY' then
              (case
                 when lower(coalesce(v_remittance_includes_json->'daily'->'include_item_types'->>pbi.item_type, v_unknown_item_type_default_text)) in ('true','1','yes','y','on')
                 then true else false end)
            else
              (case
                 when lower(coalesce(v_remittance_includes_json->'weekly'->'include_item_types'->>pbi.item_type, v_unknown_item_type_default_text)) in ('true','1','yes','y','on')
                 then true else false end)
          end
        )
    ),
    ts_meta as (
      select
        ci.candidate_id,
        ts.timesheet_id,
        ts.week_ending_date,
        ts.reference_number,
        ts.contract_id,
        nullif(btrim(coalesce(ts.job_title_norm,'')),'') as job_title_norm,
        nullif(btrim(coalesce(ts.band,'')),'') as band_norm
      from cand_items ci
      join public.timesheets ts
        on ts.timesheet_id = ci.timesheet_id
       and ts.is_current = true
      group by ci.candidate_id, ts.timesheet_id, ts.week_ending_date, ts.reference_number, ts.contract_id, ts.job_title_norm, ts.band
    ),
    ts_enrich as (
      select
        tm.candidate_id,
        tm.timesheet_id,
        tm.week_ending_date,
        tm.reference_number,
        vs.client_id as client_id,
        vs.client_name as client_name,
        upper(coalesce(vs.sheet_scope::text, v_missing_scope)) as sheet_scope,
        nullif(btrim(coalesce(ct.role, tm.job_title_norm)),'') as job_title,
        nullif(btrim(coalesce(ct.band, tm.band_norm)),'') as band
      from ts_meta tm
      left join public.contracts ct
        on ct.id = tm.contract_id
      left join public.v_timesheets_summary vs
        on vs.timesheet_id = tm.timesheet_id
    ),
    ts_snap as (
      select
        tss.timesheet_id,
        tss.candidate_id,
        tss.base_snapshot_json,
        tss.target_snapshot_json
      from public.pay_batch_timesheet_snapshots tss
      where tss.pay_batch_id = p_pay_batch_id
        and upper(coalesce(tss.pay_channel,'')) = 'PAYE'
    ),
    ts_class as (
      select
        te.candidate_id,
        te.timesheet_id,
        te.week_ending_date,
        te.reference_number,
        te.client_id,
        te.client_name,
        te.sheet_scope,
        te.job_title,
        te.band,
        tsn.base_snapshot_json,
        tsn.target_snapshot_json,
        case
          when tsn.target_snapshot_json is not null
           and jsonb_typeof(tsn.target_snapshot_json) = 'object'
           and jsonb_typeof(tsn.target_snapshot_json->'segments') = 'array'
           and exists (
             select 1
             from jsonb_array_elements(tsn.target_snapshot_json->'segments') s
             where s is not null
               and jsonb_typeof(s)='object'
               and nullif(btrim(coalesce(s->>'date','')),'') is not null
           )
          then 'SEGMENT'
          else 'AGGREGATE'
        end as timesheet_render_mode,
        case
          when tsn.base_snapshot_json is null then 'Standard'
          when tsn.base_snapshot_json = '{}'::jsonb then 'Standard'
          when (
            exists (
              select 1
              from jsonb_array_elements(coalesce(tsn.base_snapshot_json->'segments','[]'::jsonb)) s
              where s is not null and jsonb_typeof(s)='object'
                and round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) <> 0
            )
            or round(coalesce(nullif(tsn.base_snapshot_json->>'additional_pay_ex_vat','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),2) <> 0
            or (
              jsonb_typeof(tsn.base_snapshot_json->'adjustments')='array'
              and jsonb_array_length(tsn.base_snapshot_json->'adjustments') > 0
            )
          )
          then 'Adjustment'
          else 'Standard'
        end as timesheet_type
      from ts_enrich te
      left join ts_snap tsn
        on tsn.timesheet_id = te.timesheet_id
       and tsn.candidate_id = te.candidate_id
    ),
    breakdown_raw as (
      select
        ci.candidate_id,
        ci.timesheet_id,
        pbib.line_kind,
        pbib.bucket_code,
        pbib.unit_name,
        pbib.units,
        pbib.rate,
        pbib.amount_ex_vat,
        pbib.amount_vat,
        pbib.amount_inc_vat
      from cand_items ci
      join public.pay_batch_item_breakdowns pbib
        on pbib.pay_batch_item_id = ci.pay_batch_item_id
      where ci.timesheet_id is not null
    ),
    ts_unit_rows as (
      select
        br.candidate_id,
        br.timesheet_id,
        br.line_kind,
        br.bucket_code,
        br.unit_name,
        br.rate,
        round(sum(coalesce(br.units,0)),2) as quantity,
        round(sum(coalesce(br.amount_ex_vat,0)),2) as total_ex_vat,
        round(sum(coalesce(br.amount_vat,0)),2) as total_vat,
        round(sum(coalesce(br.amount_inc_vat,0)),2) as total_inc_vat
      from breakdown_raw br
      group by br.candidate_id, br.timesheet_id, br.line_kind, br.bucket_code, br.unit_name, br.rate
      having
        round(sum(coalesce(br.units,0)),2) <> 0
        or round(sum(coalesce(br.amount_ex_vat,0)),2) <> 0
        or round(sum(coalesce(br.amount_inc_vat,0)),2) <> 0
    ),
    ts_sections as (
      select
        tc.candidate_id,
        tc.timesheet_id,
        tc.week_ending_date,
        tc.reference_number,
        tc.client_id,
        tc.client_name,
        tc.job_title,
        tc.band,
        tc.timesheet_render_mode,
        tc.timesheet_type,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by
              case upper(coalesce(r.bucket_code,''))
                when 'DAY' then 1
                when 'NIGHT' then 2
                when 'SAT' then 3
                when 'SUN' then 4
                when 'BH' then 5
                else 99
              end,
              coalesce(r.unit_name,'') asc,
              coalesce(r.rate,0) asc
          )
          filter (where upper(coalesce(r.line_kind,'')) = 'TS_BUCKET'),
          '[]'::jsonb
        ) as unit_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by coalesce(r.unit_name,'') asc, coalesce(r.rate,0) asc
          )
          filter (where upper(coalesce(r.line_kind,'')) = 'ADDITIONAL_UNIT'),
          '[]'::jsonb
        ) as additional_units_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by coalesce(r.unit_name,'') asc
          )
          filter (where upper(coalesce(r.line_kind,'')) in ('EXPENSE','MILEAGE')),
          '[]'::jsonb
        ) as expenses_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat,
              'line_kind', r.line_kind
            )
            order by coalesce(r.unit_name,'') asc
          )
          filter (where upper(coalesce(r.line_kind,'')) in ('ADJUSTMENT','CONVERSION_ADJ','LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','LOAN_PAYOUT','DEBT_CREATED')),
          '[]'::jsonb
        ) as other_rows,
        round(coalesce(sum(r.total_ex_vat),0),2) as totals_ex_vat,
        round(coalesce(sum(r.total_vat),0),2) as totals_vat,
        round(coalesce(sum(r.total_inc_vat),0),2) as totals_inc_vat,
        tc.base_snapshot_json,
        tc.target_snapshot_json
      from ts_class tc
      left join ts_unit_rows r
        on r.candidate_id = tc.candidate_id
       and r.timesheet_id = tc.timesheet_id
      group by
        tc.candidate_id,
        tc.timesheet_id,
        tc.week_ending_date,
        tc.reference_number,
        tc.client_id,
        tc.client_name,
        tc.job_title,
        tc.band,
        tc.timesheet_render_mode,
        tc.timesheet_type,
        tc.base_snapshot_json,
        tc.target_snapshot_json
    ),
    ts_schedule_rows_all as (
      select
        ts.candidate_id,
        ts.timesheet_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'segment_id', nullif(btrim(coalesce(s->>'segment_id','')),''),
              'date', nullif(btrim(coalesce(s->>'date','')),''),
              'start_utc', nullif(btrim(coalesce(s->>'start_utc','')),''),
              'end_utc', nullif(btrim(coalesce(s->>'end_utc','')),''),
              'break_mins', coalesce(nullif(s->>'break_mins','')::numeric,0),
              'breaks', coalesce(s->'breaks','[]'::jsonb)
            )
            order by
              nullif(btrim(coalesce(s->>'date','')),'') asc,
              nullif(btrim(coalesce(s->>'start_utc','')),'') asc,
              nullif(btrim(coalesce(s->>'segment_id','')),'') asc
          ),
          '[]'::jsonb
        ) as schedule_rows
      from ts_sections ts
      join lateral jsonb_array_elements(coalesce(ts.target_snapshot_json->'segments','[]'::jsonb)) s
        on ts.target_snapshot_json is not null
       and jsonb_typeof(ts.target_snapshot_json)='object'
      where ts.timesheet_render_mode = 'SEGMENT'
        and s is not null
        and jsonb_typeof(s)='object'
        and coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) = false
        and (
          coalesce(nullif(s->>'hours_day','')::numeric,0)
          + coalesce(nullif(s->>'hours_night','')::numeric,0)
          + coalesce(nullif(s->>'hours_sat','')::numeric,0)
          + coalesce(nullif(s->>'hours_sun','')::numeric,0)
          + coalesce(nullif(s->>'hours_bh','')::numeric,0)
        ) > 0
      group by ts.candidate_id, ts.timesheet_id
    ),
    ts_schedule_changes_all as (
      select
        ts.candidate_id,
        ts.timesheet_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'segment_id', seg_id,
              'date', coalesce(before_seg->>'date', after_seg->>'date'),
              'before', jsonb_build_object(
                'start_utc', before_seg->>'start_utc',
                'end_utc', before_seg->>'end_utc',
                'break_mins', coalesce(nullif(before_seg->>'break_mins','')::numeric,0),
                'breaks', coalesce(before_seg->'breaks','[]'::jsonb),
                'exclude_from_pay', coalesce(nullif(before_seg->>'exclude_from_pay','')::boolean,false)
              ),
              'after', jsonb_build_object(
                'start_utc', after_seg->>'start_utc',
                'end_utc', after_seg->>'end_utc',
                'break_mins', coalesce(nullif(after_seg->>'break_mins','')::numeric,0),
                'breaks', coalesce(after_seg->'breaks','[]'::jsonb),
                'exclude_from_pay', coalesce(nullif(after_seg->>'exclude_from_pay','')::boolean,false)
              )
            )
            order by
              coalesce(before_seg->>'date', after_seg->>'date') asc,
              coalesce(before_seg->>'start_utc', after_seg->>'start_utc') asc,
              seg_id asc
          ),
          '[]'::jsonb
        ) as schedule_changes
      from (
        select
          ts0.candidate_id,
          ts0.timesheet_id,
          nullif(btrim(coalesce(ids->>'segment_id','')),'') as seg_id,
          bseg.seg as before_seg,
          aseg.seg as after_seg
        from ts_sections ts0
        join lateral (
          select s
          from (
            select s0 as s from jsonb_array_elements(coalesce(ts0.base_snapshot_json->'segments','[]'::jsonb)) s0
            union all
            select s1 as s from jsonb_array_elements(coalesce(ts0.target_snapshot_json->'segments','[]'::jsonb)) s1
          ) u
          where u.s is not null and jsonb_typeof(u.s)='object'
        ) ids
          on ts0.timesheet_render_mode = 'SEGMENT'
         and ts0.timesheet_type = 'Adjustment'
        left join lateral (
          select s as seg
          from jsonb_array_elements(coalesce(ts0.base_snapshot_json->'segments','[]'::jsonb)) s
          where s is not null and jsonb_typeof(s)='object'
            and nullif(btrim(coalesce(s->>'segment_id','')),'') = nullif(btrim(coalesce(ids->>'segment_id','')),'')
          limit 1
        ) bseg on true
        left join lateral (
          select s as seg
          from jsonb_array_elements(coalesce(ts0.target_snapshot_json->'segments','[]'::jsonb)) s
          where s is not null and jsonb_typeof(s)='object'
            and nullif(btrim(coalesce(s->>'segment_id','')),'') = nullif(btrim(coalesce(ids->>'segment_id','')),'')
          limit 1
        ) aseg on true
      ) x
      join ts_sections ts
        on ts.candidate_id = x.candidate_id
       and ts.timesheet_id = x.timesheet_id
      where x.seg_id is not null
        and (
          coalesce(x.before_seg->>'start_utc','') <> coalesce(x.after_seg->>'start_utc','')
          or coalesce(x.before_seg->>'end_utc','') <> coalesce(x.after_seg->>'end_utc','')
          or coalesce(nullif(x.before_seg->>'break_mins','')::numeric,0) <> coalesce(nullif(x.after_seg->>'break_mins','')::numeric,0)
          or coalesce(x.before_seg->'breaks','[]'::jsonb) <> coalesce(x.after_seg->'breaks','[]'::jsonb)
          or coalesce(nullif(x.before_seg->>'exclude_from_pay','')::boolean,false) <> coalesce(nullif(x.after_seg->>'exclude_from_pay','')::boolean,false)
        )
      group by ts.candidate_id, ts.timesheet_id
    ),
    cand_timesheets as (
      select
        ts.candidate_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'timesheet_id', ts.timesheet_id::text,
              'week_ending_date', case when ts.week_ending_date is null then null else ts.week_ending_date::text end,
              'reference_number', ts.reference_number,
              'client_id', case when ts.client_id is null then null else ts.client_id::text end,
              'client_name', ts.client_name,
              'job_title', ts.job_title,
              'band', ts.band,
              'timesheet_render_mode', ts.timesheet_render_mode,
              'timesheet_type', ts.timesheet_type,
              'unit_rows', ts.unit_rows,
              'additional_units_rows', ts.additional_units_rows,
              'expenses_rows', ts.expenses_rows,
              'other_rows', ts.other_rows,
              'totals', jsonb_build_object(
                'total_ex_vat', ts.totals_ex_vat,
                'vat', ts.totals_vat,
                'total_inc_vat', ts.totals_inc_vat
              ),
              'schedule_rows', case
                when coalesce(ca.eff_detailed,false) = true and ts.timesheet_render_mode = 'SEGMENT'
                  then coalesce(sr.schedule_rows,'[]'::jsonb)
                else '[]'::jsonb
              end,
              'schedule_changes', case
                when coalesce(ca.eff_detailed,false) = true and ts.timesheet_render_mode = 'SEGMENT' and ts.timesheet_type = 'Adjustment'
                  then coalesce(sc.schedule_changes,'[]'::jsonb)
                else '[]'::jsonb
              end
            )
            order by ts.week_ending_date desc nulls last, ts.client_name nulls last, ts.reference_number nulls last, ts.timesheet_id
          ),
          '[]'::jsonb
        ) as timesheets_json
      from ts_sections ts
      join cand_allowed ca
        on ca.candidate_id = ts.candidate_id
      left join ts_schedule_rows_all sr
        on sr.candidate_id = ts.candidate_id
       and sr.timesheet_id = ts.timesheet_id
      left join ts_schedule_changes_all sc
        on sc.candidate_id = ts.candidate_id
       and sc.timesheet_id = ts.timesheet_id
      group by ts.candidate_id
    ),
    cand_job as (
      select
        ca.candidate_id,
        ca.tms_ref,
        ca.display_name,
        ca.email,
        ca.eff_detailed,
        bool_or(coalesce(pt.is_simulated,false)) as test_mode,
        round(coalesce(sum(pt.amount),0),2) as total_amount,
        coalesce(ct.timesheets_json,'[]'::jsonb) as timesheets_json,
        coalesce(jsonb_agg(
          jsonb_build_object(
            'transfer_id', pt.transfer_id::text,
            'completed_at_utc', case when pt.completed_at_utc is null then null else pt.completed_at_utc::text end,
            'transfer', jsonb_build_object(
              'pay_channel', pt.pay_channel,
              'amount', pt.amount,
              'currency', pt.currency,
              'status', pt.status,
              'payment_reference', pt.payment_reference,
              'rail_tx_id', pt.rail_tx_id,
              'rail_state', pt.rail_state,
              'rail_meta_json', pt.rail_meta_json,
              'failed_reason', pt.failed_reason,
              'is_simulated', coalesce(pt.is_simulated,false)
            )
          )
          order by pt.transfer_id
        ), '[]'::jsonb) as transfers_json
      from cand_allowed ca
      join paye_transfers pt
        on pt.candidate_id = ca.candidate_id
      left join cand_timesheets ct
        on ct.candidate_id = ca.candidate_id
      group by ca.candidate_id, ca.tms_ref, ca.display_name, ca.email, ca.eff_detailed, ct.timesheets_json
    )
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'job_kind', 'PAYE_REMITTANCE',
          'pay_batch_id', v_batch.id::text,
          'scope', 'PAYE',
          'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
          'bulk_reference', v_batch.bulk_reference,
          'test_mode', coalesce(cj.test_mode,false),
          'detailed_breakdown', coalesce(cj.eff_detailed,false),
          'recipient', jsonb_build_object(
            'entity_kind', 'CANDIDATE',
            'candidate_id', cj.candidate_id::text,
            'tms_ref', cj.tms_ref,
            'display_name', cj.display_name,
            'email', cj.email
          ),
          'summary', jsonb_build_object(
            'total_amount', cj.total_amount,
            'currency', 'GBP'
          ),
          'timesheets', coalesce(cj.timesheets_json,'[]'::jsonb),
          'transfers', coalesce(cj.transfers_json,'[]'::jsonb),
          'net_input', coalesce((
            select jsonb_build_object(
              'source', pni.source,
              'net_amount', pni.net_amount,
              'imported_at_utc', case when pni.imported_at_utc is null then null else pni.imported_at_utc::text end,
              'file_name', pni.file_name
            )
            from public.pay_batch_candidates pbc
            join public.pay_batch_paye_net_inputs pni
              on pni.pay_batch_candidate_id = pbc.id
            where pbc.pay_batch_id = p_pay_batch_id
              and pbc.candidate_id = cj.candidate_id
            order by pni.imported_at_utc desc
            limit 1
          ), null),
          'pay_totals', coalesce((
            select jsonb_build_object(
              'gross_preview', round(coalesce(pbc_tot.gross_preview,0),2),
              'overpayment_recovery_taken', round(coalesce(pbc_tot.overpayment_recovery_taken,0),2),
              'loan_repayment_taken', round(coalesce(pbc_tot.loan_repayment_taken,0),2),
              'final_paid', round(coalesce(pbc_tot.net_bank_amount,0),2)
            )
            from public.pay_batch_candidates pbc_tot
            where pbc_tot.pay_batch_id = p_pay_batch_id
              and pbc_tot.candidate_id = cj.candidate_id
            limit 1
          ), '{}'::jsonb),
          'paye_net_advisory', coalesce((
            select jsonb_build_object(
              'original_net_input', (
                select pni2.net_amount
                from public.pay_batch_paye_net_inputs pni2
                where pni2.pay_batch_candidate_id = pbc_adv.id
                order by pni2.imported_at_utc desc
                limit 1
              ),
              'loan_repayment_taken', round(coalesce(pbc_adv.loan_repayment_taken,0),2),
              'overpayment_recovery_taken', round(coalesce(pbc_adv.overpayment_recovery_taken,0),2),
              'final_net_paid', round(coalesce(pbc_adv.net_bank_amount,0),2)
            )
            from public.pay_batch_candidates pbc_adv
            where pbc_adv.pay_batch_id = p_pay_batch_id
              and pbc_adv.candidate_id = cj.candidate_id
            limit 1
          ), null),
          'non_timesheet_lines', coalesce((
            select coalesce(
              jsonb_agg(
                jsonb_build_object(
                  'line_kind', pbib_nt.line_kind,
                  'bucket_code', pbib_nt.bucket_code,
                  'unit', pbib_nt.unit_name,
                  'quantity', pbib_nt.units,
                  'rate', pbib_nt.rate,
                  'total_ex_vat', pbib_nt.amount_ex_vat,
                  'total_vat', pbib_nt.amount_vat,
                  'total_inc_vat', pbib_nt.amount_inc_vat
                )
                order by coalesce(pbib_nt.line_kind,''), coalesce(pbib_nt.unit_name,''), coalesce(pbib_nt.rate,0)
              ),
              '[]'::jsonb
            )
            from public.pay_batch_candidates pbc_nt
            join public.pay_batch_items pbi_nt
              on pbi_nt.pay_batch_candidate_id = pbc_nt.id
            join public.pay_batch_item_breakdowns pbib_nt
              on pbib_nt.pay_batch_item_id = pbi_nt.id
            where pbc_nt.pay_batch_id = p_pay_batch_id
              and pbc_nt.candidate_id = cj.candidate_id
              and pbi_nt.is_voided = false
              and pbi_nt.timesheet_id is null
              and pbi_nt.item_type in ('LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','LOAN_PAYOUT')
          ), '[]'::jsonb)
        )
        order by cj.display_name nulls last, cj.tms_ref nulls last, cj.candidate_id
      ),
      '[]'::jsonb
    )
    into v_jobs_paye
    from cand_job cj;
  end if;

  -- ============================================================
  -- Candidate umbrella-copy jobs (recipient=candidate; umbrella items only)
  -- LOCKED RULE:
  --  - generated only if effective receive_when_umbrella_paid is true:
  --    overrides? candidate.remittance_receive_when_umbrella_paid : settings_defaults.remittance_receive_when_umbrella_paid
  --  - detailed_breakdown resolved from candidate overrides vs settings_defaults
  -- ============================================================
  if v_scope in ('ALL','UMBRELLA') then
    with umb_transfers as (
      select
        pbt.id as transfer_id,
        pbt.pay_batch_id,
        pbt.candidate_id,
        pbt.umbrella_id,
        pbt.pay_channel,
        pbt.amount,
        pbt.currency,
        pbt.status,
        pbt.payment_reference,
        pbt.completed_at_utc,
        pbt.rail_tx_id,
        pbt.rail_state,
        pbt.rail_meta_json,
        pbt.failed_reason,
        (
          upper(coalesce(pbt.rail_state,'')) = 'PAYROLL_TESTING'
          or (
            pbt.rail_meta_json is not null
            and (pbt.rail_meta_json ? 'simulated')
            and lower(btrim(coalesce(pbt.rail_meta_json->>'simulated',''))) in ('true','1','yes','y','on')
          )
        ) as is_simulated
      from public.pay_bank_transfers pbt
      where pbt.pay_batch_id = p_pay_batch_id
        and upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA'
        and upper(coalesce(pbt.status,'')) = 'COMPLETED'
        and pbt.umbrella_id is not null
        and pbt.candidate_id is not null
    ),
    cand_cfg as (
      select
        c.id as candidate_id,
        c.tms_ref,
        c.display_name,
        c.email,
        coalesce(c.remittance_overrides_enabled,false) as overrides_enabled,
        coalesce(c.remittances_detailed_breakdown,false) as detailed_enabled,
        coalesce(c.remittance_receive_when_umbrella_paid,false) as umb_copy_enabled
      from public.candidates c
      where c.id in (select distinct ut.candidate_id from umb_transfers ut)
    ),
    cand_effective as (
      select
        cc.*,
        (case when cc.overrides_enabled then cc.umb_copy_enabled else v_sd_remittance_receive_when_umbrella_paid end) as eff_copy,
        (case when cc.overrides_enabled then cc.detailed_enabled else v_sd_remittances_detailed_breakdown end) as eff_detailed
      from cand_cfg cc
    ),
    cand_allowed as (
      select *
      from cand_effective ce
      where ce.eff_copy = true
    ),
    cand_items as (
      select
        ca.candidate_id,
        pbi.id as pay_batch_item_id,
        pbi.item_type,
        pbi.description,
        pbi.timesheet_id,
        pbi.segment_key,
        pbi.source_ref,
        pbi.amount_ex_vat,
        pbi.amount_vat,
        pbi.amount_inc_vat,
        pbi.pay_channel
      from cand_allowed ca
      join public.pay_batch_candidates pbc
        on pbc.pay_batch_id = p_pay_batch_id
       and pbc.candidate_id = ca.candidate_id
      join public.pay_batch_items pbi
        on pbi.pay_batch_candidate_id = pbc.id
      left join public.v_timesheets_summary vs
        on vs.timesheet_id = pbi.timesheet_id
      where upper(coalesce(pbi.pay_channel,'')) = 'UMBRELLA'
        and pbi.item_type <> 'DEBT_CREATED'
        and coalesce(pbi.is_voided,false) = false
        and (
          case
            when upper(coalesce(vs.sheet_scope::text, v_missing_scope)) = 'DAILY' then
              (case
                 when lower(coalesce(v_remittance_includes_json->'daily'->'include_item_types'->>pbi.item_type, v_unknown_item_type_default_text)) in ('true','1','yes','y','on')
                 then true else false end)
            else
              (case
                 when lower(coalesce(v_remittance_includes_json->'weekly'->'include_item_types'->>pbi.item_type, v_unknown_item_type_default_text)) in ('true','1','yes','y','on')
                 then true else false end)
          end
        )
    ),
    ts_meta as (
      select
        ci.candidate_id,
        ts.timesheet_id,
        ts.week_ending_date,
        ts.reference_number,
        ts.contract_id,
        nullif(btrim(coalesce(ts.job_title_norm,'')),'') as job_title_norm,
        nullif(btrim(coalesce(ts.band,'')),'') as band_norm
      from cand_items ci
      join public.timesheets ts
        on ts.timesheet_id = ci.timesheet_id
       and ts.is_current = true
      group by ci.candidate_id, ts.timesheet_id, ts.week_ending_date, ts.reference_number, ts.contract_id, ts.job_title_norm, ts.band
    ),
    ts_enrich as (
      select
        tm.candidate_id,
        tm.timesheet_id,
        tm.week_ending_date,
        tm.reference_number,
        vs.client_id as client_id,
        vs.client_name as client_name,
        upper(coalesce(vs.sheet_scope::text, v_missing_scope)) as sheet_scope,
        nullif(btrim(coalesce(ct.role, tm.job_title_norm)),'') as job_title,
        nullif(btrim(coalesce(ct.band, tm.band_norm)),'') as band
      from ts_meta tm
      left join public.contracts ct
        on ct.id = tm.contract_id
      left join public.v_timesheets_summary vs
        on vs.timesheet_id = tm.timesheet_id
    ),
    ts_snap as (
      select
        tss.timesheet_id,
        tss.candidate_id,
        tss.base_snapshot_json,
        tss.target_snapshot_json
      from public.pay_batch_timesheet_snapshots tss
      where tss.pay_batch_id = p_pay_batch_id
        and upper(coalesce(tss.pay_channel,'')) = 'UMBRELLA'
    ),
    ts_class as (
      select
        te.candidate_id,
        te.timesheet_id,
        te.week_ending_date,
        te.reference_number,
        te.client_id,
        te.client_name,
        te.sheet_scope,
        te.job_title,
        te.band,
        tsn.base_snapshot_json,
        tsn.target_snapshot_json,
        case
          when tsn.target_snapshot_json is not null
           and jsonb_typeof(tsn.target_snapshot_json) = 'object'
           and jsonb_typeof(tsn.target_snapshot_json->'segments') = 'array'
           and exists (
             select 1
             from jsonb_array_elements(tsn.target_snapshot_json->'segments') s
             where s is not null
               and jsonb_typeof(s)='object'
               and nullif(btrim(coalesce(s->>'date','')),'') is not null
           )
          then 'SEGMENT'
          else 'AGGREGATE'
        end as timesheet_render_mode,
        case
          when tsn.base_snapshot_json is null then 'Standard'
          when tsn.base_snapshot_json = '{}'::jsonb then 'Standard'
          when (
            exists (
              select 1
              from jsonb_array_elements(coalesce(tsn.base_snapshot_json->'segments','[]'::jsonb)) s
              where s is not null and jsonb_typeof(s)='object'
                and round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) <> 0
            )
            or round(coalesce(nullif(tsn.base_snapshot_json->>'additional_pay_ex_vat','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),2) <> 0
            or round(coalesce(nullif(tsn.base_snapshot_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),2) <> 0
            or (
              jsonb_typeof(tsn.base_snapshot_json->'adjustments')='array'
              and jsonb_array_length(tsn.base_snapshot_json->'adjustments') > 0
            )
          )
          then 'Adjustment'
          else 'Standard'
        end as timesheet_type
      from ts_enrich te
      left join ts_snap tsn
        on tsn.timesheet_id = te.timesheet_id
       and tsn.candidate_id = te.candidate_id
    ),
    breakdown_raw as (
      select
        ci.candidate_id,
        ci.timesheet_id,
        pbib.line_kind,
        pbib.bucket_code,
        pbib.unit_name,
        pbib.units,
        pbib.rate,
        pbib.amount_ex_vat,
        pbib.amount_vat,
        pbib.amount_inc_vat
      from cand_items ci
      join public.pay_batch_item_breakdowns pbib
        on pbib.pay_batch_item_id = ci.pay_batch_item_id
      where ci.timesheet_id is not null
    ),
    ts_unit_rows as (
      select
        br.candidate_id,
        br.timesheet_id,
        br.line_kind,
        br.bucket_code,
        br.unit_name,
        br.rate,
        round(sum(coalesce(br.units,0)),2) as quantity,
        round(sum(coalesce(br.amount_ex_vat,0)),2) as total_ex_vat,
        round(sum(coalesce(br.amount_vat,0)),2) as total_vat,
        round(sum(coalesce(br.amount_inc_vat,0)),2) as total_inc_vat
      from breakdown_raw br
      group by br.candidate_id, br.timesheet_id, br.line_kind, br.bucket_code, br.unit_name, br.rate
      having
        round(sum(coalesce(br.units,0)),2) <> 0
        or round(sum(coalesce(br.amount_ex_vat,0)),2) <> 0
        or round(sum(coalesce(br.amount_inc_vat,0)),2) <> 0
    ),
    ts_sections as (
      select
        tc.candidate_id,
        tc.timesheet_id,
        tc.week_ending_date,
        tc.reference_number,
        tc.client_id,
        tc.client_name,
        tc.job_title,
        tc.band,
        tc.timesheet_render_mode,
        tc.timesheet_type,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by
              case upper(coalesce(r.bucket_code,''))
                when 'DAY' then 1
                when 'NIGHT' then 2
                when 'SAT' then 3
                when 'SUN' then 4
                when 'BH' then 5
                else 99
              end,
              coalesce(r.unit_name,'') asc,
              coalesce(r.rate,0) asc
          )
          filter (where upper(coalesce(r.line_kind,'')) = 'TS_BUCKET'),
          '[]'::jsonb
        ) as unit_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by coalesce(r.unit_name,'') asc, coalesce(r.rate,0) asc
          )
          filter (where upper(coalesce(r.line_kind,'')) = 'ADDITIONAL_UNIT'),
          '[]'::jsonb
        ) as additional_units_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat
            )
            order by coalesce(r.unit_name,'') asc
          )
          filter (where upper(coalesce(r.line_kind,'')) in ('EXPENSE','MILEAGE')),
          '[]'::jsonb
        ) as expenses_rows,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'unit', r.unit_name,
              'quantity', r.quantity,
              'rate', r.rate,
              'total_ex_vat', r.total_ex_vat,
              'total_vat', r.total_vat,
              'total_inc_vat', r.total_inc_vat,
              'line_kind', r.line_kind
            )
            order by coalesce(r.unit_name,'') asc
          )
          filter (where upper(coalesce(r.line_kind,'')) in ('ADJUSTMENT','CONVERSION_ADJ','LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','LOAN_PAYOUT','DEBT_CREATED')),
          '[]'::jsonb
        ) as other_rows,
        round(coalesce(sum(r.total_ex_vat),0),2) as totals_ex_vat,
        round(coalesce(sum(r.total_vat),0),2) as totals_vat,
        round(coalesce(sum(r.total_inc_vat),0),2) as totals_inc_vat,
        tc.base_snapshot_json,
        tc.target_snapshot_json
      from ts_class tc
      left join ts_unit_rows r
        on r.candidate_id = tc.candidate_id
       and r.timesheet_id = tc.timesheet_id
      group by
        tc.candidate_id,
        tc.timesheet_id,
        tc.week_ending_date,
        tc.reference_number,
        tc.client_id,
        tc.client_name,
        tc.job_title,
        tc.band,
        tc.timesheet_render_mode,
        tc.timesheet_type,
        tc.base_snapshot_json,
        tc.target_snapshot_json
    ),
    ts_schedule_rows_all as (
      select
        ts.candidate_id,
        ts.timesheet_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'segment_id', nullif(btrim(coalesce(s->>'segment_id','')),''),
              'date', nullif(btrim(coalesce(s->>'date','')),''),
              'start_utc', nullif(btrim(coalesce(s->>'start_utc','')),''),
              'end_utc', nullif(btrim(coalesce(s->>'end_utc','')),''),
              'break_mins', coalesce(nullif(s->>'break_mins','')::numeric,0),
              'breaks', coalesce(s->'breaks','[]'::jsonb)
            )
            order by
              nullif(btrim(coalesce(s->>'date','')),'') asc,
              nullif(btrim(coalesce(s->>'start_utc','')),'') asc,
              nullif(btrim(coalesce(s->>'segment_id','')),'') asc
          ),
          '[]'::jsonb
        ) as schedule_rows
      from ts_sections ts
      join lateral jsonb_array_elements(coalesce(ts.target_snapshot_json->'segments','[]'::jsonb)) s
        on ts.target_snapshot_json is not null
       and jsonb_typeof(ts.target_snapshot_json)='object'
      where ts.timesheet_render_mode = 'SEGMENT'
        and s is not null
        and jsonb_typeof(s)='object'
        and coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) = false
        and (
          coalesce(nullif(s->>'hours_day','')::numeric,0)
          + coalesce(nullif(s->>'hours_night','')::numeric,0)
          + coalesce(nullif(s->>'hours_sat','')::numeric,0)
          + coalesce(nullif(s->>'hours_sun','')::numeric,0)
          + coalesce(nullif(s->>'hours_bh','')::numeric,0)
        ) > 0
      group by ts.candidate_id, ts.timesheet_id
    ),
    ts_schedule_changes_all as (
      select
        ts.candidate_id,
        ts.timesheet_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'segment_id', seg_id,
              'date', coalesce(before_seg->>'date', after_seg->>'date'),
              'before', jsonb_build_object(
                'start_utc', before_seg->>'start_utc',
                'end_utc', before_seg->>'end_utc',
                'break_mins', coalesce(nullif(before_seg->>'break_mins','')::numeric,0),
                'breaks', coalesce(before_seg->'breaks','[]'::jsonb),
                'exclude_from_pay', coalesce(nullif(before_seg->>'exclude_from_pay','')::boolean,false)
              ),
              'after', jsonb_build_object(
                'start_utc', after_seg->>'start_utc',
                'end_utc', after_seg->>'end_utc',
                'break_mins', coalesce(nullif(after_seg->>'break_mins','')::numeric,0),
                'breaks', coalesce(after_seg->'breaks','[]'::jsonb),
                'exclude_from_pay', coalesce(nullif(after_seg->>'exclude_from_pay','')::boolean,false)
              )
            )
            order by
              coalesce(before_seg->>'date', after_seg->>'date') asc,
              coalesce(before_seg->>'start_utc', after_seg->>'start_utc') asc,
              seg_id asc
          ),
          '[]'::jsonb
        ) as schedule_changes
      from (
        select
          ts0.candidate_id,
          ts0.timesheet_id,
          nullif(btrim(coalesce(ids->>'segment_id','')),'') as seg_id,
          bseg.seg as before_seg,
          aseg.seg as after_seg
        from ts_sections ts0
        join lateral (
          select s
          from (
            select s0 as s from jsonb_array_elements(coalesce(ts0.base_snapshot_json->'segments','[]'::jsonb)) s0
            union all
            select s1 as s from jsonb_array_elements(coalesce(ts0.target_snapshot_json->'segments','[]'::jsonb)) s1
          ) u
          where u.s is not null and jsonb_typeof(u.s)='object'
        ) ids
          on ts0.timesheet_render_mode = 'SEGMENT'
         and ts0.timesheet_type = 'Adjustment'
        left join lateral (
          select s as seg
          from jsonb_array_elements(coalesce(ts0.base_snapshot_json->'segments','[]'::jsonb)) s
          where s is not null and jsonb_typeof(s)='object'
            and nullif(btrim(coalesce(s->>'segment_id','')),'') = nullif(btrim(coalesce(ids->>'segment_id','')),'')
          limit 1
        ) bseg on true
        left join lateral (
          select s as seg
          from jsonb_array_elements(coalesce(ts0.target_snapshot_json->'segments','[]'::jsonb)) s
          where s is not null and jsonb_typeof(s)='object'
            and nullif(btrim(coalesce(s->>'segment_id','')),'') = nullif(btrim(coalesce(ids->>'segment_id','')),'')
          limit 1
        ) aseg on true
      ) x
      join ts_sections ts
        on ts.candidate_id = x.candidate_id
       and ts.timesheet_id = x.timesheet_id
      where x.seg_id is not null
        and (
          coalesce(x.before_seg->>'start_utc','') <> coalesce(x.after_seg->>'start_utc','')
          or coalesce(x.before_seg->>'end_utc','') <> coalesce(x.after_seg->>'end_utc','')
          or coalesce(nullif(x.before_seg->>'break_mins','')::numeric,0) <> coalesce(nullif(x.after_seg->>'break_mins','')::numeric,0)
          or coalesce(x.before_seg->'breaks','[]'::jsonb) <> coalesce(x.after_seg->'breaks','[]'::jsonb)
          or coalesce(nullif(x.before_seg->>'exclude_from_pay','')::boolean,false) <> coalesce(nullif(x.after_seg->>'exclude_from_pay','')::boolean,false)
        )
      group by ts.candidate_id, ts.timesheet_id
    ),
    cand_timesheets as (
      select
        ts.candidate_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'timesheet_id', ts.timesheet_id::text,
              'week_ending_date', case when ts.week_ending_date is null then null else ts.week_ending_date::text end,
              'reference_number', ts.reference_number,
              'client_id', case when ts.client_id is null then null else ts.client_id::text end,
              'client_name', ts.client_name,
              'job_title', ts.job_title,
              'band', ts.band,
              'timesheet_render_mode', ts.timesheet_render_mode,
              'timesheet_type', ts.timesheet_type,
              'unit_rows', ts.unit_rows,
              'additional_units_rows', ts.additional_units_rows,
              'expenses_rows', ts.expenses_rows,
              'other_rows', ts.other_rows,
              'totals', jsonb_build_object(
                'total_ex_vat', ts.totals_ex_vat,
                'vat', ts.totals_vat,
                'total_inc_vat', ts.totals_inc_vat
              ),
              'schedule_rows', case
                when coalesce(ca.eff_detailed,false) = true and ts.timesheet_render_mode = 'SEGMENT'
                  then coalesce(sr.schedule_rows,'[]'::jsonb)
                else '[]'::jsonb
              end,
              'schedule_changes', case
                when coalesce(ca.eff_detailed,false) = true and ts.timesheet_render_mode = 'SEGMENT' and ts.timesheet_type = 'Adjustment'
                  then coalesce(sc.schedule_changes,'[]'::jsonb)
                else '[]'::jsonb
              end
            )
            order by ts.week_ending_date desc nulls last, ts.client_name nulls last, ts.reference_number nulls last, ts.timesheet_id
          ),
          '[]'::jsonb
        ) as timesheets_json
      from ts_sections ts
      join cand_allowed ca
        on ca.candidate_id = ts.candidate_id
      left join ts_schedule_rows_all sr
        on sr.candidate_id = ts.candidate_id
       and sr.timesheet_id = ts.timesheet_id
      left join ts_schedule_changes_all sc
        on sc.candidate_id = ts.candidate_id
       and sc.timesheet_id = ts.timesheet_id
      group by ts.candidate_id
    ),
    cand_job as (
      select
        ca.candidate_id,
        ca.tms_ref,
        ca.display_name,
        ca.email,
        ca.eff_detailed,
        bool_or(coalesce(ut.is_simulated,false)) as test_mode,
        round(coalesce(sum(ut.amount),0),2) as total_amount,
        coalesce(ct.timesheets_json,'[]'::jsonb) as timesheets_json,
        coalesce(jsonb_agg(
          jsonb_build_object(
            'transfer_id', ut.transfer_id::text,
            'completed_at_utc', case when ut.completed_at_utc is null then null else ut.completed_at_utc::text end,
            'transfer', jsonb_build_object(
              'pay_channel', ut.pay_channel,
              'amount', ut.amount,
              'currency', ut.currency,
              'status', ut.status,
              'payment_reference', ut.payment_reference,
              'rail_tx_id', ut.rail_tx_id,
              'rail_state', ut.rail_state,
              'rail_meta_json', ut.rail_meta_json,
              'failed_reason', ut.failed_reason,
              'is_simulated', coalesce(ut.is_simulated,false)
            )
          )
          order by ut.transfer_id
        ), '[]'::jsonb) as transfers_json
      from cand_allowed ca
      join umb_transfers ut
        on ut.candidate_id = ca.candidate_id
      left join cand_timesheets ct
        on ct.candidate_id = ca.candidate_id
      group by ca.candidate_id, ca.tms_ref, ca.display_name, ca.email, ca.eff_detailed, ct.timesheets_json
    )
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'job_kind', 'CANDIDATE_UMBRELLA_COPY_REMITTANCE',
          'pay_batch_id', v_batch.id::text,
          'scope', 'UMBRELLA',
          'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
          'bulk_reference', v_batch.bulk_reference,
          'test_mode', coalesce(cj.test_mode,false),
          'detailed_breakdown', coalesce(cj.eff_detailed,false),
          'recipient', jsonb_build_object(
            'entity_kind', 'CANDIDATE',
            'candidate_id', cj.candidate_id::text,
            'tms_ref', cj.tms_ref,
            'display_name', cj.display_name,
            'email', cj.email
          ),
          'summary', jsonb_build_object(
            'total_amount', cj.total_amount,
            'currency', 'GBP'
          ),
          'timesheets', coalesce(cj.timesheets_json,'[]'::jsonb),
          'transfers', coalesce(cj.transfers_json,'[]'::jsonb),
          'pay_totals', coalesce((
            select jsonb_build_object(
              'gross_preview', round(coalesce(pbc_tot.gross_preview,0),2),
              'overpayment_recovery_taken', round(coalesce(pbc_tot.overpayment_recovery_taken,0),2),
              'loan_repayment_taken', round(coalesce(pbc_tot.loan_repayment_taken,0),2),
              'final_paid', round(coalesce(pbc_tot.net_bank_amount,0),2)
            )
            from public.pay_batch_candidates pbc_tot
            where pbc_tot.pay_batch_id = p_pay_batch_id
              and pbc_tot.candidate_id = cj.candidate_id
            limit 1
          ), '{}'::jsonb),
          'non_timesheet_lines', coalesce((
            select coalesce(
              jsonb_agg(
                jsonb_build_object(
                  'line_kind', pbib_nt.line_kind,
                  'bucket_code', pbib_nt.bucket_code,
                  'unit', pbib_nt.unit_name,
                  'quantity', pbib_nt.units,
                  'rate', pbib_nt.rate,
                  'total_ex_vat', pbib_nt.amount_ex_vat,
                  'total_vat', pbib_nt.amount_vat,
                  'total_inc_vat', pbib_nt.amount_inc_vat
                )
                order by coalesce(pbib_nt.line_kind,''), coalesce(pbib_nt.unit_name,''), coalesce(pbib_nt.rate,0)
              ),
              '[]'::jsonb
            )
            from public.pay_batch_candidates pbc_nt
            join public.pay_batch_items pbi_nt
              on pbi_nt.pay_batch_candidate_id = pbc_nt.id
            join public.pay_batch_item_breakdowns pbib_nt
              on pbib_nt.pay_batch_item_id = pbi_nt.id
            where pbc_nt.pay_batch_id = p_pay_batch_id
              and pbc_nt.candidate_id = cj.candidate_id
              and pbi_nt.is_voided = false
              and pbi_nt.timesheet_id is null
              and pbi_nt.item_type in ('LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','LOAN_PAYOUT')
          ), '[]'::jsonb)
        )
        order by cj.display_name nulls last, cj.tms_ref nulls last, cj.candidate_id
      ),
      '[]'::jsonb
    )
    into v_jobs_cand_umb_copy
    from cand_job cj;
  end if;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch.id::text,
    'scope', v_scope,
    'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
    'bulk_reference', v_batch.bulk_reference,
    'remittance_header_message', v_remittance_header_message,
    'remittance_footer_message', v_remittance_footer_message,
    'jobs', (coalesce(v_jobs_umb,'[]'::jsonb) || coalesce(v_jobs_paye,'[]'::jsonb) || coalesce(v_jobs_cand_umb_copy,'[]'::jsonb))
  );
end;
$function$;







create or replace function public.pay_reconcile_external_payment(
  p_actor_user_id uuid,
  p_payload_json jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_pay_date date;
  v_note text;
  v_payment_reference text;

  v_settings record;
  v_batch_id uuid;

  v_timesheet_ids uuid[] := array[]::uuid[];
  v_dup jsonb := '[]'::jsonb;
  v_missing jsonb := '[]'::jsonb;

  v_now timestamptz := now();

  v_ts record;
  v_snapshot jsonb;
  v_signature text;

  v_timesheet_count int := 0;
  v_candidate_count int := 0;
begin
  if p_payload_json is null or jsonb_typeof(p_payload_json) <> 'object' then
    raise exception 'pay_reconcile_external_payment: payload_json must be an object';
  end if;

  if nullif(btrim(coalesce(p_payload_json->>'pay_date','')), '') is null then
    raise exception 'pay_reconcile_external_payment: payload.pay_date is required (YYYY-MM-DD)';
  end if;

  begin
    v_pay_date := (p_payload_json->>'pay_date')::date;
  exception when others then
    raise exception 'pay_reconcile_external_payment: payload.pay_date is not a valid date (YYYY-MM-DD)';
  end;

  v_note := nullif(btrim(coalesce(p_payload_json->>'note','')), '');

  v_payment_reference := nullif(
    btrim(
      coalesce(
        p_payload_json->>'payment_reference',
        p_payload_json->>'bulk_reference',
        p_payload_json->>'bank_reference',
        ''
      )
    ),
    ''
  );

  if v_payment_reference is null then
    raise exception 'pay_reconcile_external_payment: payload.payment_reference is required';
  end if;

  if jsonb_typeof(p_payload_json->'timesheet_ids') <> 'array' then
    raise exception 'pay_reconcile_external_payment: payload.timesheet_ids must be an array of UUID strings';
  end if;

  select coalesce(array_agg((x::text)::uuid), array[]::uuid[])
  into v_timesheet_ids
  from jsonb_array_elements_text(p_payload_json->'timesheet_ids') x;

  if array_length(v_timesheet_ids, 1) is null or array_length(v_timesheet_ids, 1) = 0 then
    raise exception 'pay_reconcile_external_payment: payload.timesheet_ids must not be empty';
  end if;

  -- Duplicates guard
  select coalesce(jsonb_agg(d.ts_id::text), '[]'::jsonb)
  into v_dup
  from (
    select t.ts_id
    from (
      select unnest(v_timesheet_ids) as ts_id
    ) t
    group by t.ts_id
    having count(*) > 1
  ) d;

  if jsonb_array_length(v_dup) > 0 then
    raise exception 'pay_reconcile_external_payment: duplicate timesheet_ids %', v_dup::text;
  end if;

  -- Validate settings exist (for snapshot fields required by pay_batches checks)
  select
    sd.banking_system,
    sd.external_paye_system
  into v_settings
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_settings.banking_system is null or v_settings.external_paye_system is null then
    raise exception 'pay_reconcile_external_payment: settings_defaults missing banking_system/external_paye_system';
  end if;

  -- Validate timesheets exist and are current
  select coalesce(jsonb_agg(m.ts_id::text), '[]'::jsonb)
  into v_missing
  from (
    select u.ts_id
    from (select unnest(v_timesheet_ids) as ts_id) u
    left join public.timesheets ts
      on ts.timesheet_id = u.ts_id
     and ts.is_current = true
    where ts.timesheet_id is null
  ) m;

  if jsonb_array_length(v_missing) > 0 then
    raise exception 'pay_reconcile_external_payment: timesheet(s) not found or not current %', v_missing::text;
  end if;

  -- Create a settled batch for audit/history linkage
  insert into public.pay_batches(
    pay_date,
    created_at_utc,
    created_by_user_id,
    status,
    banking_system_snapshot,
    external_paye_system_snapshot,
    rail_provider_snapshot,
    rail_env_snapshot,
    bulk_reference
  )
  values (
    v_pay_date,
    v_now,
    p_actor_user_id,
    'SETTLED',
    v_settings.banking_system,
    v_settings.external_paye_system,
    'EXTERNAL',
    'EXTERNAL',
    v_payment_reference
  )
  returning id into v_batch_id;

  -- Create pay_batch_candidates (for history drill-down)
  insert into public.pay_batch_candidates(
    pay_batch_id,
    candidate_id,
    candidate_tms_ref,
    candidate_display_name,
    settlement_status,
    settled_at_utc,
    settled_via,
    settled_note
  )
  select
    v_batch_id,
    c.id,
    c.tms_ref,
    c.display_name,
    'SETTLED',
    v_now,
    'EXTERNAL_RECONCILE',
    v_note
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  join public.candidates c
    on c.id = tf.candidate_id
  where tf.is_current = true
    and tf.timesheet_id = any(v_timesheet_ids)
  group by c.id, c.tms_ref, c.display_name;

  get diagnostics v_candidate_count = row_count;

  -- For each timesheet: build CURRENT snapshot and write to pay_state + history (+ pay_batch_timesheet_snapshots)
  for v_ts in
    select
      tf.timesheet_id,
      tf.candidate_id,
      upper(coalesce(tf.pay_method,'')) as pay_method,
      tf.total_pay_ex_vat,
      tf.expenses_pay_ex_vat,
      tf.travel_pay_ex_vat,
      tf.accommodation_pay_ex_vat,
      tf.other_pay_ex_vat,
      tf.mileage_pay_ex_vat,
      tf.invoice_breakdown_json,
      tf.additional_units_json,
      tf.hours_day,
      tf.hours_night,
      tf.hours_sat,
      tf.hours_sun,
      tf.hours_bh,
      tf.pay_day,
      tf.pay_night,
      tf.pay_sat,
      tf.pay_sun,
      tf.pay_bh,
      tf.mileage_units,
      tf.mileage_pay_rate,
      ts.reference_number
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    where tf.is_current = true
      and tf.timesheet_id = any(v_timesheet_ids)
  loop
    -- Build snapshot matching the NEW schema used by pay_create_draft_batch:
    -- - segments include schedule + hours_* when SEGMENTS mode
    -- - top-level hours_* and pay_* always included
    -- - additional_units_json included
    -- - mileage_units and mileage_pay_rate included
    v_snapshot := jsonb_build_object(
      'segments',
        case
          when v_ts.invoice_breakdown_json is not null
           and jsonb_typeof(v_ts.invoice_breakdown_json) = 'object'
           and upper(coalesce(v_ts.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
           and jsonb_typeof(v_ts.invoice_breakdown_json->'segments') = 'array'
          then (
            select coalesce(
              jsonb_agg(
                jsonb_build_object(
                  'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')), ''),
                  'date', nullif(btrim(coalesce(seg->>'date','')), ''),
                  'start_utc', nullif(btrim(coalesce(seg->>'start_utc','')), ''),
                  'end_utc', nullif(btrim(coalesce(seg->>'end_utc','')), ''),
                  'break_mins', coalesce(nullif(seg->>'break_mins','')::numeric,0),
                  'breaks', coalesce(seg->'breaks','[]'::jsonb),
                  'hours_day', coalesce(nullif(seg->>'hours_day','')::numeric,0),
                  'hours_night', coalesce(nullif(seg->>'hours_night','')::numeric,0),
                  'hours_sat', coalesce(nullif(seg->>'hours_sat','')::numeric,0),
                  'hours_sun', coalesce(nullif(seg->>'hours_sun','')::numeric,0),
                  'hours_bh', coalesce(nullif(seg->>'hours_bh','')::numeric,0),
                  'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
                  'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean,false),
                  'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')), '')
                )
                order by nullif(btrim(coalesce(seg->>'segment_id','')), '')
              ),
              '[]'::jsonb
            )
            from jsonb_array_elements(v_ts.invoice_breakdown_json->'segments') seg
            where seg is not null and jsonb_typeof(seg)='object'
          )
          else jsonb_build_array(
            jsonb_build_object(
              'segment_id', ('ts:' || v_ts.timesheet_id::text),
              'pay_amount', round(coalesce(v_ts.total_pay_ex_vat,0),2),
              'exclude_from_pay', false,
              'ref_num', nullif(btrim(coalesce(v_ts.reference_number,'')), '')
            )
          )
        end,
      'adjustments', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'id', a.id::text,
            'delta_pay_ex_vat', round(coalesce(a.delta_pay_ex_vat,0),2)
          )
          order by a.created_at nulls last, a.id
        )
        from public.ts_pay_adjustments a
        where a.as_advance = false
          and a.timesheet_id = v_ts.timesheet_id
      ), '[]'::jsonb),
      'additional_pay_ex_vat',
        case
          when v_ts.invoice_breakdown_json is not null
           and jsonb_typeof(v_ts.invoice_breakdown_json)='object'
           and upper(coalesce(v_ts.invoice_breakdown_json->>'mode',''))='SEGMENTS'
          then round(coalesce(nullif(v_ts.invoice_breakdown_json #>> '{additional,pay_ex_vat}','')::numeric,0),2)
          else 0::numeric
        end,
      'additional_units_json', coalesce(v_ts.additional_units_json, '{}'::jsonb),
      'hours_day', round(coalesce(nullif(v_ts.hours_day::text,'')::numeric,0),2),
      'hours_night', round(coalesce(nullif(v_ts.hours_night::text,'')::numeric,0),2),
      'hours_sat', round(coalesce(nullif(v_ts.hours_sat::text,'')::numeric,0),2),
      'hours_sun', round(coalesce(nullif(v_ts.hours_sun::text,'')::numeric,0),2),
      'hours_bh', round(coalesce(nullif(v_ts.hours_bh::text,'')::numeric,0),2),
      'pay_day', round(coalesce(nullif(v_ts.pay_day::text,'')::numeric,0),2),
      'pay_night', round(coalesce(nullif(v_ts.pay_night::text,'')::numeric,0),2),
      'pay_sat', round(coalesce(nullif(v_ts.pay_sat::text,'')::numeric,0),2),
      'pay_sun', round(coalesce(nullif(v_ts.pay_sun::text,'')::numeric,0),2),
      'pay_bh', round(coalesce(nullif(v_ts.pay_bh::text,'')::numeric,0),2),
      'mileage_units', round(coalesce(nullif(v_ts.mileage_units::text,'')::numeric,0),2),
      'mileage_pay_rate', case when v_ts.mileage_pay_rate is null then null else round(coalesce(nullif(v_ts.mileage_pay_rate::text,'')::numeric,0),2) end,
      'expenses', jsonb_build_object(
        'expenses_pay_ex_vat', round(coalesce(v_ts.expenses_pay_ex_vat,0),2),
        'travel_pay_ex_vat', round(coalesce(v_ts.travel_pay_ex_vat,0),2),
        'accommodation_pay_ex_vat', round(coalesce(v_ts.accommodation_pay_ex_vat,0),2),
        'other_pay_ex_vat', round(coalesce(v_ts.other_pay_ex_vat,0),2),
        'mileage_pay_ex_vat', round(coalesce(v_ts.mileage_pay_ex_vat,0),2)
      ),
      'reconciled_external', true,
      'reconciled_pay_batch_id', v_batch_id::text,
      'reconciled_at_utc', v_now::text,
      'external_payment_reference', v_payment_reference
    );

    v_signature := md5(v_snapshot::text);

    -- Optional but strong: write pay_batch_timesheet_snapshots for full batch artefact auditability
    insert into public.pay_batch_timesheet_snapshots(
      pay_batch_id,
      timesheet_id,
      candidate_id,
      pay_channel,
      base_snapshot_json,
      target_snapshot_json,
      signature
    )
    values (
      v_batch_id,
      v_ts.timesheet_id,
      v_ts.candidate_id,
      case when v_ts.pay_method = 'PAYE' then 'PAYE' else 'UMBRELLA' end,
      v_snapshot,
      v_snapshot,
      v_signature
    );

    insert into public.timesheet_pay_state(
      timesheet_id,
      last_settled_snapshot_json,
      last_settled_signature,
      last_settled_pay_batch_id,
      last_settled_at_utc
    )
    values (
      v_ts.timesheet_id,
      v_snapshot,
      v_signature,
      v_batch_id,
      v_now
    )
    on conflict (timesheet_id)
    do update set
      last_settled_snapshot_json = excluded.last_settled_snapshot_json,
      last_settled_signature = excluded.last_settled_signature,
      last_settled_pay_batch_id = excluded.last_settled_pay_batch_id,
      last_settled_at_utc = excluded.last_settled_at_utc;

    insert into public.timesheet_pay_state_history(
      timesheet_id,
      pay_batch_id,
      settled_at_utc,
      snapshot_json,
      signature
    )
    values (
      v_ts.timesheet_id,
      v_batch_id,
      v_now,
      v_snapshot,
      v_signature
    );

    insert into public.audit_events(
      actor_user_id,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason
    )
    values (
      p_actor_user_id,
      'timesheet',
      v_ts.timesheet_id::text,
      'PAY_EXTERNAL_RECONCILE',
      null,
      jsonb_build_object(
        'pay_batch_id', v_batch_id::text,
        'pay_date', v_pay_date::text,
        'timesheet_id', v_ts.timesheet_id::text,
        'candidate_id', v_ts.candidate_id::text,
        'pay_method', v_ts.pay_method,
        'payment_reference', v_payment_reference
      ),
      v_note
    );

    v_timesheet_count := v_timesheet_count + 1;
  end loop;

  -- Batch-level audit
  insert into public.audit_events(
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  )
  values (
    p_actor_user_id,
    'pay_batch',
    v_batch_id::text,
    'PAY_EXTERNAL_RECONCILE_BATCH',
    null,
    jsonb_build_object(
      'pay_batch_id', v_batch_id::text,
      'pay_date', v_pay_date::text,
      'payment_reference', v_payment_reference,
      'timesheet_ids', (select jsonb_agg(t::text) from unnest(v_timesheet_ids) t),
      'note', v_note
    ),
    v_note
  );

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch_id::text,
    'pay_date', v_pay_date::text,
    'payment_reference', v_payment_reference,
    'settled_timesheet_count', v_timesheet_count,
    'candidate_count', v_candidate_count
  );
end;
$$;


create or replace function public.pay_batches_claim_due_scheduled(
  p_limit int,
  p_now_utc timestamptz
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_limit int := greatest(1, least(coalesce(p_limit,50), 500));
  v_now timestamptz := now();
  v_cutoff timestamptz := coalesce(p_now_utc, v_now);

  v_claimed jsonb := '[]'::jsonb;
  v_claimed_count int := 0;
begin
  with claim as (
    select
      pb.id,
      pb.rail_provider_snapshot,
      pb.rail_env_snapshot,
      pb.schedule_kind,
      pb.scheduled_at_utc,
      pb.funding_account_ref
    from public.pay_batches pb
    where upper(coalesce(pb.status,'')) in ('AUTHORISED_FOR_PAYMENT')
      and pb.scheduled_at_utc is not null
      and pb.scheduled_at_utc <= v_cutoff
      and nullif(btrim(coalesce(pb.funding_account_ref,'')), '') is not null
    order by pb.scheduled_at_utc asc, pb.id asc
    for update skip locked
    limit v_limit
  ),
  upd as (
    update public.pay_batches pb2
    set
      status = 'EXECUTING',
      executing_started_at_utc = coalesce(pb2.executing_started_at_utc, v_now)
    from claim c
    where pb2.id = c.id
    returning
      pb2.id,
      pb2.rail_provider_snapshot,
      pb2.rail_env_snapshot,
      pb2.schedule_kind,
      pb2.scheduled_at_utc,
      pb2.funding_account_ref,
      pb2.executing_started_at_utc
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'pay_batch_id', u.id::text,
          'rail_provider_snapshot', u.rail_provider_snapshot,
          'rail_env_snapshot', u.rail_env_snapshot,
          'schedule_kind', u.schedule_kind,
          'scheduled_at_utc', u.scheduled_at_utc,
          'funding_account_ref', u.funding_account_ref,
          'executing_started_at_utc', u.executing_started_at_utc
        )
        order by u.scheduled_at_utc asc nulls last, u.id
      ),
      '[]'::jsonb
    ),
    count(*)::int
  into v_claimed, v_claimed_count
  from upd u;

  return jsonb_build_object(
    'ok', true,
    'server_utc', v_now,
    'cutoff_utc', v_cutoff,
    'limit', v_limit,
    'claimed_count', v_claimed_count,
    'claimed', v_claimed
  );
end;
$$;


create or replace function public.pay_bank_transfers_apply_rail_updates(
  p_pay_batch_id uuid,
  p_updates jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  v_updated_count int := 0;
  v_input_count int := 0;

  v_missing jsonb := '[]'::jsonb;
  v_duplicates jsonb := '[]'::jsonb;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_bank_transfers_apply_rail_updates: pay_batch_id is required';
  end if;

  if p_updates is null or jsonb_typeof(p_updates) <> 'array' then
    raise exception 'pay_bank_transfers_apply_rail_updates: updates must be a JSON array';
  end if;

  create temp table if not exists _tmp_pbt_updates (
    transfer_id uuid not null,
    status text not null,
    rail_tx_id text null,
    rail_state text null,
    rail_meta_json jsonb null,
    failed_reason text null,
    completed_at_utc timestamptz null
  ) on commit drop;

  truncate table _tmp_pbt_updates;

  insert into _tmp_pbt_updates(
    transfer_id,
    status,
    rail_tx_id,
    rail_state,
    rail_meta_json,
    failed_reason,
    completed_at_utc
  )
  select
    nullif(btrim(coalesce(e->>'transfer_id','')),'')::uuid as transfer_id,
    upper(btrim(coalesce(e->>'status',''))) as status,
    nullif(btrim(coalesce(e->>'rail_tx_id','')),'') as rail_tx_id,
    nullif(btrim(coalesce(e->>'rail_state','')),'') as rail_state,
    case
      when (e ? 'rail_meta_json') and jsonb_typeof(e->'rail_meta_json') in ('object','array','string','number','boolean','null')
        then e->'rail_meta_json'
      else null
    end as rail_meta_json,
    nullif(btrim(coalesce(e->>'failed_reason','')),'') as failed_reason,
    nullif(btrim(coalesce(e->>'completed_at_utc','')),'')::timestamptz as completed_at_utc
  from jsonb_array_elements(p_updates) e
  where e is not null and jsonb_typeof(e) = 'object';

  select count(*)::int
  into v_input_count
  from _tmp_pbt_updates t;

  if v_input_count = 0 then
    return jsonb_build_object(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'server_utc', v_now,
      'input_count', 0,
      'updated_count', 0,
      'missing_transfer_ids', '[]'::jsonb
    );
  end if;

  if exists (select 1 from _tmp_pbt_updates t where t.transfer_id is null limit 1) then
    raise exception 'pay_bank_transfers_apply_rail_updates: updates contains an invalid or missing transfer_id';
  end if;

  if exists (
    select 1
    from _tmp_pbt_updates t
    where t.status not in ('PENDING','COMPLETED','FAILED','BLOCKED')
    limit 1
  ) then
    raise exception 'pay_bank_transfers_apply_rail_updates: invalid status in updates (allowed: PENDING|COMPLETED|FAILED|BLOCKED)';
  end if;

  select coalesce(jsonb_agg(d.transfer_id::text order by d.transfer_id), '[]'::jsonb)
  into v_duplicates
  from (
    select t.transfer_id
    from _tmp_pbt_updates t
    group by t.transfer_id
    having count(*) > 1
  ) d;

  if jsonb_array_length(v_duplicates) > 0 then
    raise exception 'pay_bank_transfers_apply_rail_updates: duplicate transfer_id values %', v_duplicates::text;
  end if;

  select coalesce(
    jsonb_agg(t.transfer_id::text order by t.transfer_id),
    '[]'::jsonb
  )
  into v_missing
  from _tmp_pbt_updates t
  left join public.pay_bank_transfers pbt_chk
    on pbt_chk.id = t.transfer_id
   and pbt_chk.pay_batch_id = p_pay_batch_id
  where pbt_chk.id is null;

  update public.pay_bank_transfers pbt
  set
    status = t.status,
    rail_tx_id = coalesce(t.rail_tx_id, pbt.rail_tx_id),
    rail_state = coalesce(t.rail_state, pbt.rail_state),
    rail_meta_json = case
      when t.rail_meta_json is null then pbt.rail_meta_json
      when pbt.rail_meta_json is null then t.rail_meta_json
      else (pbt.rail_meta_json || t.rail_meta_json)
    end,
    completed_at_utc = case
      when t.status = 'COMPLETED' then coalesce(t.completed_at_utc, pbt.completed_at_utc, v_now)
      when t.status = 'FAILED' then coalesce(t.completed_at_utc, pbt.completed_at_utc)
      else pbt.completed_at_utc
    end,
    failed_reason = case
      when t.status = 'FAILED' then coalesce(nullif(btrim(coalesce(t.failed_reason,'')), ''), pbt.failed_reason, nullif(btrim(coalesce(t.rail_state,'')), ''))
      else pbt.failed_reason
    end
  from _tmp_pbt_updates t
  where pbt.id = t.transfer_id
    and pbt.pay_batch_id = p_pay_batch_id;

  get diagnostics v_updated_count = row_count;

  update public.pay_batches pb
  set last_status_checked_at_utc = v_now
  where pb.id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'server_utc', v_now,
    'input_count', v_input_count,
    'updated_count', v_updated_count,
    'missing_transfer_ids', v_missing
  );
end;
$$;


create or replace function public.pay_batch_auth_start(
  p_pay_batch_id uuid,
  p_schedule_kind text,
  p_scheduled_at_utc timestamptz,
  p_funding_account_ref text,
  p_warning_hours_json jsonb,
  p_actor_user_id uuid,
  p_actor_intent text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_kind text := upper(btrim(coalesce(p_schedule_kind,'')));
  v_intent text := upper(btrim(coalesce(p_actor_intent,'')));
  v_now timestamptz := now();

  v_user record;
  v_cfg record;

  v_req_qty int := 1;
  v_use_golden boolean := false;

  v_existing_id uuid := null;
  v_req_id uuid := null;

  v_batch record;

  v_fresh jsonb := null;
  v_is_stale boolean := false;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;

  v_batch_kind_fixed text := null;
  v_has_paye boolean := false;
  v_has_awaiting_net boolean := false;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_auth_start: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_auth_start: actor_user_id is required';
  end if;

  if v_kind not in ('IMMEDIATE','SCHEDULED') then
    raise exception 'pay_batch_auth_start: invalid schedule_kind (IMMEDIATE|SCHEDULED)';
  end if;

  v_use_golden := (v_intent = 'USE_GOLDEN_KEY');

  select
    tu.id,
    tu.is_active,
    tu.payment_authoriser,
    tu.payment_golden_key
  into v_user
  from public.tms_users tu
  where tu.id = p_actor_user_id
  limit 1;

  if v_user.id is null then
    raise exception 'pay_batch_auth_start: actor_user not found';
  end if;

  if coalesce(v_user.is_active,false) = false then
    raise exception 'pay_batch_auth_start: actor_user is not active';
  end if;

  if coalesce(v_user.payment_authoriser,false) = false then
    raise exception 'pay_batch_auth_start: actor_user must be a payment authoriser';
  end if;

  if v_use_golden = true and coalesce(v_user.payment_golden_key,false) = false then
    raise exception 'pay_batch_auth_start: actor_user does not have payment golden key';
  end if;

  select
    sd.payment_authoriser_quantity
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  v_req_qty := greatest(1, coalesce(v_cfg.payment_authoriser_quantity, 1));

  select
    pb.id,
    pb.status,
    pb.batch_kind_fixed
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch_auth_start: pay_batch not found';
  end if;

  v_batch_kind_fixed := upper(btrim(coalesce(v_batch.batch_kind_fixed,'')));

  v_fresh := public.pay_batch_validate_freshness(p_pay_batch_id, p_actor_user_id);
  v_is_stale := coalesce((v_fresh->>'is_stale')::boolean, false);
  v_stale_reasons := coalesce(v_fresh->'stale_reasons', '[]'::jsonb);

  if v_is_stale = true then
    select coalesce(jsonb_agg(x.elem), '[]'::jsonb)
      into v_diff_sample
    from (
      select elem
      from jsonb_array_elements(coalesce(v_fresh->'diff','[]'::jsonb)) as elem
      limit 50
    ) x;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_AUTH_START:STALE',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'stale_reasons', v_stale_reasons,
          'diff_sample', v_diff_sample
        ),
        'pay_batches',
        p_pay_batch_id::text,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_AUTH_START',
      'code', 'BATCH_STALE',
      'message', 'pay_batch_auth_start: batch is stale; regenerate draft before proceeding',
      'pay_batch_id', p_pay_batch_id::text,
      'stale_reasons', v_stale_reasons,
      'diff', v_diff_sample
    )::text;
  end if;

  if v_batch_kind_fixed <> 'LOANS' then
    select exists(
      select 1
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc
        on pbc.id = pbi.pay_batch_candidate_id
      where pbc.pay_batch_id = p_pay_batch_id
        and pbi.pay_channel = 'PAYE'
        and pbi.is_voided = false
    )
    into v_has_paye;

    select exists(
      select 1
      from public.pay_batch_candidates pbc2
      where pbc2.pay_batch_id = p_pay_batch_id
        and coalesce(pbc2.awaiting_net_amount,false) = true
    )
    into v_has_awaiting_net;

    if v_has_paye = true and v_has_awaiting_net = true then
      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_BATCH_AUTH_START:BLOCKED_AWAITING_NET',
          jsonb_build_object(
            'pay_batch_id', p_pay_batch_id::text,
            'has_paye', v_has_paye,
            'has_awaiting_net', v_has_awaiting_net
          ),
          'pay_batches',
          p_pay_batch_id::text,
          null,
          null,
          null,
          null
        );
      exception when others then
        null;
      end;

      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAYE_NET_REQUIRED',
        'message', 'pay_batch_auth_start: PAYE net amounts are required before authorisation can proceed',
        'pay_batch_id', p_pay_batch_id::text
      )::text;
    end if;
  end if;

  select
    pbar.id
  into v_existing_id
  from public.pay_batch_auth_requests pbar
  where pbar.pay_batch_id = p_pay_batch_id
    and pbar.state = 'AWAITING'
  limit 1;

  if v_existing_id is not null then
    raise exception 'pay_batch_auth_start: an active authorisation request already exists for this batch';
  end if;

  -- Validate schedule prerequisites and write schedule fields to pay_batches.
  -- We will override the batch status afterwards.
  perform public.pay_batch_schedule(
    p_pay_batch_id,
    v_kind,
    p_scheduled_at_utc,
    p_funding_account_ref,
    p_warning_hours_json,
    p_actor_user_id
  );

  select
    pb2.id,
    pb2.schedule_kind,
    pb2.scheduled_at_utc,
    pb2.funding_account_ref,
    pb2.funds_warning_hours_json
  into v_batch
  from public.pay_batches pb2
  where pb2.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch_auth_start: pay_batch not found after scheduling';
  end if;

  insert into public.pay_batch_auth_requests(
    pay_batch_id,
    requested_by_user_id,
    required_quantity,
    schedule_kind,
    scheduled_at_utc,
    funding_account_ref,
    funds_warning_hours_json,
    state,
    golden_key_used,
    golden_key_user_id,
    created_at_utc
  )
  values (
    p_pay_batch_id,
    p_actor_user_id,
    v_req_qty,
    v_batch.schedule_kind,
    v_batch.scheduled_at_utc,
    v_batch.funding_account_ref,
    v_batch.funds_warning_hours_json,
    'AWAITING',
    false,
    null,
    v_now
  )
  returning id into v_req_id;

  insert into public.pay_batch_auth_actions(
    auth_request_id,
    pay_batch_id,
    actor_user_id,
    action,
    action_at_utc,
    note
  )
  values (
    v_req_id,
    p_pay_batch_id,
    p_actor_user_id,
    case when v_use_golden then 'USE_GOLDEN_KEY' else 'AUTHORISE' end,
    v_now,
    null
  );

  if v_use_golden = true or v_req_qty <= 1 then
    update public.pay_batch_auth_requests pbar2
    set
      state = 'AUTHORISED',
      golden_key_used = v_use_golden,
      golden_key_user_id = case when v_use_golden then p_actor_user_id else null end,
      finalised_at_utc = v_now,
      finalised_by_user_id = p_actor_user_id
    where pbar2.id = v_req_id;

    update public.pay_batches pb3
    set status = 'AUTHORISED_FOR_PAYMENT'
    where pb3.id = p_pay_batch_id;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_AUTH_START:AUTHORISED',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'auth_request_id', v_req_id::text,
          'required_quantity', v_req_qty,
          'became_authorised', true,
          'golden_key_used', v_use_golden
        ),
        'pay_batches',
        p_pay_batch_id::text,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;

    return jsonb_build_object(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'status', (select pb4.status from public.pay_batches pb4 where pb4.id = p_pay_batch_id),
      'auth_request_id', v_req_id::text,
      'auth_state', 'AUTHORISED',
      'required_quantity', v_req_qty,
      'approved_count', 1,
      'became_authorised', true
    );
  else
    update public.pay_batches pb5
    set status = 'AWAITING_AUTHORISATION'
    where pb5.id = p_pay_batch_id;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_AUTH_START:AWAITING',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'auth_request_id', v_req_id::text,
          'required_quantity', v_req_qty,
          'became_authorised', false,
          'golden_key_used', v_use_golden
        ),
        'pay_batches',
        p_pay_batch_id::text,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;

    return jsonb_build_object(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'status', (select pb6.status from public.pay_batches pb6 where pb6.id = p_pay_batch_id),
      'auth_request_id', v_req_id::text,
      'auth_state', 'AWAITING',
      'required_quantity', v_req_qty,
      'approved_count', 1,
      'became_authorised', false
    );
  end if;
end;
$$;


create or replace function public.pay_batch_auth_apply_action(
  p_auth_request_id uuid,
  p_actor_user_id uuid,
  p_action text,
  p_note text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_action text := upper(btrim(coalesce(p_action,'')));

  v_req record;
  v_user record;

  v_inserted_id uuid := null;
  v_approved_count int := 0;

  v_became_authorised boolean := false;
  v_new_auth_state text := null;
  v_new_batch_status text := null;
begin
  if p_auth_request_id is null then
    raise exception 'pay_batch_auth_apply_action: auth_request_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_auth_apply_action: actor_user_id is required';
  end if;

  if v_action not in ('AUTHORISE','USE_GOLDEN_KEY','REJECT') then
    raise exception 'pay_batch_auth_apply_action: invalid action (AUTHORISE|USE_GOLDEN_KEY|REJECT)';
  end if;

  select
    pbar.id,
    pbar.pay_batch_id,
    pbar.state,
    pbar.required_quantity
  into v_req
  from public.pay_batch_auth_requests pbar
  where pbar.id = p_auth_request_id
  for update;

  if v_req.id is null then
    raise exception 'pay_batch_auth_apply_action: auth_request not found';
  end if;

  if v_req.state <> 'AWAITING' then
    raise exception 'pay_batch_auth_apply_action: auth_request must be AWAITING (current=%)', v_req.state;
  end if;

  select
    tu.id,
    tu.is_active,
    tu.payment_authoriser,
    tu.payment_golden_key
  into v_user
  from public.tms_users tu
  where tu.id = p_actor_user_id
  limit 1;

  if v_user.id is null then
    raise exception 'pay_batch_auth_apply_action: actor_user not found';
  end if;

  if coalesce(v_user.is_active,false) = false then
    raise exception 'pay_batch_auth_apply_action: actor_user is not active';
  end if;

  if coalesce(v_user.payment_authoriser,false) = false and coalesce(v_user.payment_golden_key,false) = false then
    raise exception 'pay_batch_auth_apply_action: actor_user is not an authoriser';
  end if;

  if v_action = 'USE_GOLDEN_KEY' and coalesce(v_user.payment_golden_key,false) = false then
    raise exception 'pay_batch_auth_apply_action: actor_user does not have payment golden key';
  end if;

  insert into public.pay_batch_auth_actions(
    auth_request_id,
    pay_batch_id,
    actor_user_id,
    action,
    action_at_utc,
    note
  )
  values (
    p_auth_request_id,
    v_req.pay_batch_id,
    p_actor_user_id,
    v_action,
    v_now,
    p_note
  )
  on conflict on constraint ux_pay_batch_auth_actions_one_per_user
  do nothing
  returning id into v_inserted_id;

  if v_inserted_id is null then
    raise exception 'pay_batch_auth_apply_action: actor_user has already acted on this request';
  end if;

  if v_action = 'REJECT' then
    update public.pay_batch_auth_requests pbar2
    set
      state = 'REJECTED',
      finalised_at_utc = v_now,
      finalised_by_user_id = p_actor_user_id,
      golden_key_used = false,
      golden_key_user_id = null
    where pbar2.id = p_auth_request_id;

    update public.pay_batch_auth_tokens pbat2
    set used_at_utc = v_now
    where pbat2.auth_request_id = p_auth_request_id
      and pbat2.used_at_utc is null;

    update public.pay_batches pb2
    set
      status = 'READY',
      schedule_kind = null,
      scheduled_at_utc = null,
      scheduled_by_user_id = null,
      funding_account_ref = null,
      funds_warning_hours_json = null
    where pb2.id = v_req.pay_batch_id;

    return jsonb_build_object(
      'ok', true,
      'pay_batch_id', v_req.pay_batch_id::text,
      'status', (select pb3.status from public.pay_batches pb3 where pb3.id = v_req.pay_batch_id),
      'auth_request_id', p_auth_request_id::text,
      'auth_state', 'REJECTED',
      'required_quantity', v_req.required_quantity,
      'approved_count', 0,
      'became_authorised', false
    );
  end if;

  select count(*)::int
  into v_approved_count
  from public.pay_batch_auth_actions pbaa0
  where pbaa0.auth_request_id = p_auth_request_id
    and pbaa0.action in ('AUTHORISE','USE_GOLDEN_KEY');

  if v_action = 'USE_GOLDEN_KEY' or v_approved_count >= greatest(1, coalesce(v_req.required_quantity,1)) then
    v_became_authorised := true;

    update public.pay_batch_auth_requests pbar3
    set
      state = 'AUTHORISED',
      finalised_at_utc = v_now,
      finalised_by_user_id = p_actor_user_id,
      golden_key_used = case when v_action = 'USE_GOLDEN_KEY' then true else coalesce(pbar3.golden_key_used,false) end,
      golden_key_user_id = case when v_action = 'USE_GOLDEN_KEY' then p_actor_user_id else pbar3.golden_key_user_id end
    where pbar3.id = p_auth_request_id;

    update public.pay_batches pb4
    set status = 'AUTHORISED_FOR_PAYMENT'
    where pb4.id = v_req.pay_batch_id;

    update public.pay_batch_auth_tokens pbat3
    set used_at_utc = v_now
    where pbat3.auth_request_id = p_auth_request_id
      and pbat3.used_at_utc is null;

    v_new_auth_state := 'AUTHORISED';
    v_new_batch_status := 'AUTHORISED_FOR_PAYMENT';
  else
    v_became_authorised := false;
    v_new_auth_state := 'AWAITING';

    update public.pay_batches pb5
    set status = 'AWAITING_AUTHORISATION'
    where pb5.id = v_req.pay_batch_id;

    v_new_batch_status := 'AWAITING_AUTHORISATION';
  end if;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_req.pay_batch_id::text,
    'status', (select pb6.status from public.pay_batches pb6 where pb6.id = v_req.pay_batch_id),
    'auth_request_id', p_auth_request_id::text,
    'auth_state', v_new_auth_state,
    'required_quantity', v_req.required_quantity,
    'approved_count', v_approved_count,
    'became_authorised', v_became_authorised
  );
end;
$$;


create or replace function public.pay_batch_auth_invites_upsert(
  p_auth_request_id uuid,
  p_actor_user_id uuid,
  p_target_user_ids uuid[] default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_req record;
  v_now timestamptz := now();
  v_expires timestamptz := (now() + interval '7 days');

  v_target_count int := 0;
  v_inserted_count int := 0;
  v_tokens jsonb := '[]'::jsonb;
begin
  if p_auth_request_id is null then
    raise exception 'pay_batch_auth_invites_upsert: auth_request_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_auth_invites_upsert: actor_user_id is required';
  end if;

  select
    pbar.id,
    pbar.pay_batch_id,
    pbar.state
  into v_req
  from public.pay_batch_auth_requests pbar
  where pbar.id = p_auth_request_id
  for update;

  if v_req.id is null then
    raise exception 'pay_batch_auth_invites_upsert: auth_request not found';
  end if;

  if v_req.state <> 'AWAITING' then
    raise exception 'pay_batch_auth_invites_upsert: auth_request must be AWAITING (current=%)', v_req.state;
  end if;

  with targets as (
    select
      tu.id as user_id
    from public.tms_users tu
    where tu.is_active = true
      and (coalesce(tu.payment_authoriser,false) = true or coalesce(tu.payment_golden_key,false) = true)
      and (
        p_target_user_ids is null
        or coalesce(array_length(p_target_user_ids,1),0) = 0
        or tu.id = any(p_target_user_ids)
      )
  )
  select count(*)::int
  into v_target_count
  from targets;

  if v_target_count = 0 then
    raise exception 'pay_batch_auth_invites_upsert: no eligible target users resolved';
  end if;

  with targets as (
    select
      tu.id as user_id
    from public.tms_users tu
    where tu.is_active = true
      and (coalesce(tu.payment_authoriser,false) = true or coalesce(tu.payment_golden_key,false) = true)
      and (
        p_target_user_ids is null
        or coalesce(array_length(p_target_user_ids,1),0) = 0
        or tu.id = any(p_target_user_ids)
      )
  )
  insert into public.pay_batch_auth_tokens(
    token,
    auth_request_id,
    target_user_id,
    expires_at_utc,
    used_at_utc,
    created_at_utc
  )
  select
    encode(gen_random_bytes(24), 'hex'),
    p_auth_request_id,
    t.user_id,
    v_expires,
    null,
    v_now
  from targets t
  on conflict on constraint ux_pay_batch_auth_tokens_one_per_target
  do nothing;

  get diagnostics v_inserted_count = row_count;

  with targets as (
    select
      tu.id as user_id
    from public.tms_users tu
    where tu.is_active = true
      and (coalesce(tu.payment_authoriser,false) = true or coalesce(tu.payment_golden_key,false) = true)
      and (
        p_target_user_ids is null
        or coalesce(array_length(p_target_user_ids,1),0) = 0
        or tu.id = any(p_target_user_ids)
      )
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'target_user_id', pbat.target_user_id::text,
        'token', pbat.token,
        'expires_at_utc', pbat.expires_at_utc,
        'used_at_utc', pbat.used_at_utc,
        'created_at_utc', pbat.created_at_utc
      )
      order by pbat.created_at_utc asc nulls last, pbat.token asc
    ),
    '[]'::jsonb
  )
  into v_tokens
  from public.pay_batch_auth_tokens pbat
  join targets t
    on t.user_id = pbat.target_user_id
  where pbat.auth_request_id = p_auth_request_id;

  return jsonb_build_object(
    'ok', true,
    'auth_request_id', p_auth_request_id::text,
    'pay_batch_id', v_req.pay_batch_id::text,
    'resolved_target_count', v_target_count,
    'inserted_count', v_inserted_count,
    'expires_at_utc', v_expires,
    'tokens', v_tokens
  );
end;
$$;

create or replace function public._bank_hash(
  p_sort_code text,
  p_account_number text,
  p_account_holder text
) returns text
language plpgsql
as $$
declare
  v_sort text;
  v_acct text;
  v_name text;
  v_raw  text;
begin
  v_sort := regexp_replace(coalesce(p_sort_code,''), '[^0-9]+', '', 'g');
  v_acct := regexp_replace(coalesce(p_account_number,''), '[^0-9]+', '', 'g');
  v_name := upper(regexp_replace(btrim(coalesce(p_account_holder,'')), '\s+', ' ', 'g'));

  if v_sort = '' or v_acct = '' then
    return null;
  end if;

  v_raw := v_sort || '|' || v_acct || '|' || v_name;

  -- md5() is built-in (no pgcrypto dependency) and returns a stable hex string.
  return md5(v_raw);
end $$;


CREATE OR REPLACE FUNCTION public.bank_readiness_lock(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_bank_details_hash text,
  p_lock_kind text DEFAULT 'READINESS'
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text;
  v_env text;
  v_entity_kind text;
  v_bank_hash text;
  v_lock_kind text;
  v_key text;

  v_h1 int;
  v_h2 int;
  v_lock_key bigint;
begin
  v_provider := upper(btrim(coalesce(p_provider,'')));
  v_env := upper(btrim(coalesce(p_env,'')));
  v_entity_kind := upper(btrim(coalesce(p_entity_kind,'')));
  v_bank_hash := btrim(coalesce(p_bank_details_hash,''));
  v_lock_kind := upper(btrim(coalesce(p_lock_kind,'READINESS')));

  if v_provider = '' then
    raise exception 'provider is required';
  end if;

  if v_env = '' then
    raise exception 'env is required';
  end if;

  if v_entity_kind = '' then
    raise exception 'entity_kind is required';
  end if;

  if p_entity_id is null then
    raise exception 'entity_id is required';
  end if;

  if v_bank_hash = '' then
    raise exception 'bank_details_hash is required';
  end if;

  if v_lock_kind = '' then
    raise exception 'lock_kind is required';
  end if;

  v_key := v_lock_kind
           || '|' || v_provider
           || '|' || v_env
           || '|' || v_entity_kind
           || '|' || p_entity_id::text
           || '|' || v_bank_hash;

  -- Build a stable 64-bit advisory lock key from two 32-bit hashes.
  v_h1 := hashtext(v_key);
  v_h2 := hashtext(v_key || '|2');

  v_lock_key :=
    (( (v_h1::bigint & 4294967295) << 32 )
      | (v_h2::bigint & 4294967295));

  perform pg_advisory_xact_lock(v_lock_key);

  return jsonb_build_object(
    'ok', true
  );
end;
$function$;

CREATE OR REPLACE FUNCTION public.pay_batch_validate_freshness(p_pay_batch_id uuid, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now_utc timestamptz := now();

  v_pay_date date;
  v_week_start date;
  v_batch_kind_fixed text;
  v_scope text;

  v_ts_ids uuid[] := array[]::uuid[];

  v_is_stale boolean := false;
  v_reasons text[] := array[]::text[];

  v_diffs jsonb := '[]'::jsonb;

  v_ts_changed_ct int := 0;
  v_key_diff_ct int := 0;
  v_ded_diff_ct int := 0;
  v_paye_net_diff_ct int := 0;

  v_diff_limit int := 500;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_validate_freshness: pay_batch_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception 'pay_batch_validate_freshness: actor_user_id is required';
  end if;

  select
    pb.pay_date,
    pb.batch_kind_fixed
  into
    v_pay_date,
    v_batch_kind_fixed
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  limit 1;

  if v_pay_date is null then
    raise exception 'pay_batch_validate_freshness: pay_batch_id not found (%).', p_pay_batch_id::text;
  end if;

  v_week_start := public._pay_week_start_monday(v_pay_date);

  select
    max(pbi.pay_channel)
  into
    v_scope
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbi.pay_channel in ('PAYE','UMBRELLA');

  v_scope := upper(btrim(coalesce(v_scope,'')));
  if v_scope not in ('PAYE','UMBRELLA') then
    v_scope := null;
  end if;

  select
    coalesce(
      (
        select array_agg(distinct t1.timesheet_id)
        from (
          select pbts.timesheet_id
          from public.pay_batch_timesheet_snapshots pbts
          where pbts.pay_batch_id = p_pay_batch_id
            and pbts.timesheet_id is not null

          union all

          select pbi2.timesheet_id
          from public.pay_batch_items pbi2
          join public.pay_batch_candidates pbc2
            on pbc2.id = pbi2.pay_batch_candidate_id
          where pbc2.pay_batch_id = p_pay_batch_id
            and pbi2.timesheet_id is not null
        ) t1
      ),
      array[]::uuid[]
    )
  into v_ts_ids;

  ---------------------------------------------------------------------------
  -- TIMESHEET_CHANGED: compare stored pbts.signature vs recomputed md5(current target_snapshot_json)
  ---------------------------------------------------------------------------
  with
  pbts as (
    select
      pbs.timesheet_id,
      pbs.signature as stored_signature
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.timesheet_id is not null
  ),
  tf0 as (
    select
      tf.timesheet_id,
      tf.candidate_id,
      tf.client_id,
      ts.contract_id,
      ts.reference_number,
      tf.invoice_breakdown_json,
      tf.hours_day,
      tf.hours_night,
      tf.hours_sat,
      tf.hours_sun,
      tf.hours_bh,
      tf.pay_day,
      tf.pay_night,
      tf.pay_sat,
      tf.pay_sun,
      tf.pay_bh,
      tf.additional_units_json,
      tf.mileage_units,
      tf.mileage_pay_rate,
      round(coalesce(tf.total_pay_ex_vat,0),2) as total_pay_ex_vat,
      round(coalesce(tf.expenses_pay_ex_vat,0),2) as expenses_pay_ex_vat,
      round(coalesce(tf.travel_pay_ex_vat,0),2) as travel_pay_ex_vat,
      round(coalesce(tf.accommodation_pay_ex_vat,0),2) as accommodation_pay_ex_vat,
      round(coalesce(tf.other_pay_ex_vat,0),2) as other_pay_ex_vat,
      round(coalesce(tf.mileage_pay_ex_vat,0),2) as mileage_pay_ex_vat
    from public.timesheets_financials tf
    join pbts x
      on x.timesheet_id = tf.timesheet_id
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    where tf.is_current = true
  ),
  cur0 as (
    select
      t.*,
      case
        when t.invoice_breakdown_json is not null
         and jsonb_typeof(t.invoice_breakdown_json)='object'
         and upper(coalesce(t.invoice_breakdown_json->>'mode',''))='SEGMENTS'
         and jsonb_typeof(t.invoice_breakdown_json->'segments')='array'
        then (
          select coalesce(
            jsonb_agg(
              jsonb_build_object(
                'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')),''),
                'date', nullif(btrim(coalesce(seg->>'date','')),''),
                'start_utc', nullif(btrim(coalesce(seg->>'start_utc','')),''),
                'end_utc', nullif(btrim(coalesce(seg->>'end_utc','')),''),
                'break_mins', coalesce(nullif(seg->>'break_mins','')::numeric,0),
                'breaks', coalesce(seg->'breaks','[]'::jsonb),
                'hours_day', coalesce(nullif(seg->>'hours_day','')::numeric,0),
                'hours_night', coalesce(nullif(seg->>'hours_night','')::numeric,0),
                'hours_sat', coalesce(nullif(seg->>'hours_sat','')::numeric,0),
                'hours_sun', coalesce(nullif(seg->>'hours_sun','')::numeric,0),
                'hours_bh', coalesce(nullif(seg->>'hours_bh','')::numeric,0),
                'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
                'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean,false),
                'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')),'')
              )
            ),
            '[]'::jsonb
          )
          from jsonb_array_elements(t.invoice_breakdown_json->'segments') seg
          where seg is not null and jsonb_typeof(seg)='object'
        )
        else jsonb_build_array(
          jsonb_build_object(
            'segment_id', ('ts:' || t.timesheet_id::text),
            'pay_amount', round(coalesce(t.total_pay_ex_vat,0),2),
            'exclude_from_pay', false,
            'ref_num', nullif(btrim(coalesce(t.reference_number,'')), '')
          )
        )
      end as cur_segments,
      case
        when t.invoice_breakdown_json is not null
         and jsonb_typeof(t.invoice_breakdown_json)='object'
         and upper(coalesce(t.invoice_breakdown_json->>'mode',''))='SEGMENTS'
        then round(coalesce(nullif(t.invoice_breakdown_json #>> '{additional,pay_ex_vat}','')::numeric,0),2)
        else 0::numeric
      end as cur_additional,
      coalesce(
        (
          select coalesce(
            jsonb_agg(
              jsonb_build_object(
                'id', a.id::text,
                'delta_pay_ex_vat', round(coalesce(a.delta_pay_ex_vat,0),2)
              )
              order by a.id
            ),
            '[]'::jsonb
          )
          from public.ts_pay_adjustments a
          where a.timesheet_id = t.timesheet_id
            and a.as_advance = false
        ),
        '[]'::jsonb
      ) as cur_adjs
    from tf0 t
  ),
  cur_sig as (
    select
      c.timesheet_id,
      md5(
        jsonb_build_object(
          'segments', coalesce(c.cur_segments, '[]'::jsonb),
          'additional_pay_ex_vat', round(coalesce(c.cur_additional,0),2),
          'additional_units_json', coalesce(c.additional_units_json, '{}'::jsonb),
          'hours_day', round(coalesce(c.hours_day,0),2),
          'hours_night', round(coalesce(c.hours_night,0),2),
          'hours_sat', round(coalesce(c.hours_sat,0),2),
          'hours_sun', round(coalesce(c.hours_sun,0),2),
          'hours_bh', round(coalesce(c.hours_bh,0),2),
          'pay_day', round(coalesce(c.pay_day,0),2),
          'pay_night', round(coalesce(c.pay_night,0),2),
          'pay_sat', round(coalesce(c.pay_sat,0),2),
          'pay_sun', round(coalesce(c.pay_sun,0),2),
          'pay_bh', round(coalesce(c.pay_bh,0),2),
          'mileage_units', round(coalesce(c.mileage_units,0),2),
          'mileage_pay_rate', c.mileage_pay_rate,
          'expenses', jsonb_build_object(
            'expenses_pay_ex_vat', round(coalesce(c.expenses_pay_ex_vat,0),2),
            'travel_pay_ex_vat', round(coalesce(c.travel_pay_ex_vat,0),2),
            'accommodation_pay_ex_vat', round(coalesce(c.accommodation_pay_ex_vat,0),2),
            'other_pay_ex_vat', round(coalesce(c.other_pay_ex_vat,0),2),
            'mileage_pay_ex_vat', round(coalesce(c.mileage_pay_ex_vat,0),2)
          ),
          'adjustments', coalesce(c.cur_adjs, '[]'::jsonb)
        )::text
      ) as current_signature
    from cur0 c
  ),
  diffs as (
    select
      p.timesheet_id,
      p.stored_signature,
      s.current_signature
    from pbts p
    join cur_sig s
      on s.timesheet_id = p.timesheet_id
    where coalesce(p.stored_signature,'') <> coalesce(s.current_signature,'')
  )
  select
    count(*)::int,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'timesheet_id', d.timesheet_id::text,
          'key_type', 'TS_SIGNATURE',
          'key_value', 'SIGNATURE',
          'expected', d.stored_signature,
          'actual', d.current_signature
        )
        order by d.timesheet_id::text
      ),
      '[]'::jsonb
    )
  into
    v_ts_changed_ct,
    v_diffs
  from diffs d;

  if v_ts_changed_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'TIMESHEET_CHANGED');
  end if;

  ---------------------------------------------------------------------------
  -- RESERVATION_CHANGED: compare this-batch item components vs (truth - baseline - reserved_other)
  -- Only for keys present in THIS batch.
  ---------------------------------------------------------------------------
  with
  inp as (
    select coalesce(
      (select array_agg(distinct x) from unnest(coalesce(v_ts_ids, array[]::uuid[])) as t(x) where x is not null),
      array[]::uuid[]
    ) as ts_ids
  ),
  this_items_raw as (
    select
      pbi.id as pay_batch_item_id,
      pbi.timesheet_id,
      pbi.item_type,
      pbi.segment_key,
      pbi.source_ref,
      coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0)::numeric as amount_ex_vat,
      coalesce(pbi.amount_inc_vat, pbi.amount_ex_vat, 0)::numeric as amount_inc_vat
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.timesheet_id is not null
      and pbi.is_voided = false
      and pbi.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
  ),
  this_snap as (
    select
      pbs.timesheet_id,
      pbs.target_snapshot_json
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
  ),
  this_seg_lookup as (
    select
      ti.timesheet_id,
      coalesce(
        nullif(btrim(coalesce(ti.segment_key,'')), ''),
        case
          when ti.source_ref is not null and btrim(ti.source_ref) like 'seg:%'
            then nullif(btrim(split_part(ti.source_ref,':',2)), '')
          else null
        end
      ) as seg_id
    from this_items_raw ti
    where ti.item_type = 'SEGMENT_DELTA'
  ),
  this_seg_date_map as (
    select
      sl.timesheet_id,
      sl.seg_id,
      nullif(btrim(coalesce(seg->>'date','')), '') as seg_date_raw
    from this_seg_lookup sl
    join this_snap sc
      on sc.timesheet_id = sl.timesheet_id
    join lateral jsonb_array_elements(coalesce(sc.target_snapshot_json->'segments','[]'::jsonb)) as seg on true
    where sl.seg_id is not null
      and seg is not null
      and jsonb_typeof(seg)='object'
      and nullif(btrim(coalesce(seg->>'segment_id','')), '') = sl.seg_id
  ),
  this_seg_date_final as (
    select
      sdm.timesheet_id,
      sdm.seg_id,
      case when sdm.seg_date_raw ~ '^\\d{4}-\\d{2}-\\d{2}$' then sdm.seg_date_raw else null end as seg_date
    from this_seg_date_map sdm
  ),
  this_components as (
    select
      ti.timesheet_id,
      case
        when ti.item_type = 'SEGMENT_DELTA'
          then case
                 when sdf.seg_date is not null then 'TS_DAY'
                 else 'TS_TOTAL'
               end
        when ti.item_type = 'MILEAGE_DELTA'
          then 'EXPENSE_CODE'
        when ti.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
          then case
                 when ti.source_ref is not null and (
                   btrim(ti.source_ref) like 'additional:%'
                   or btrim(ti.source_ref) like 'add:%'
                   or btrim(ti.source_ref) = 'additional'
                 )
                 then 'ADDITIONAL_CODE'
                 else 'EXPENSE_CODE'
               end
        else 'EXPENSE_CODE'
      end as key_type,
      case
        when ti.item_type = 'SEGMENT_DELTA'
          then coalesce(sdf.seg_date, 'TOTAL')
        when ti.item_type = 'MILEAGE_DELTA'
          then 'MILEAGE'
        when ti.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
          then case
                 when ti.source_ref is not null and (btrim(ti.source_ref) like 'additional:%' or btrim(ti.source_ref) like 'add:%')
                   then upper(nullif(btrim(split_part(ti.source_ref,':',2)), ''))
                 when ti.source_ref is not null and btrim(ti.source_ref) = 'additional'
                   then 'TOTAL'
                 when ti.source_ref is not null and btrim(ti.source_ref) <> ''
                   then upper(btrim(ti.source_ref))
                 else 'UNKNOWN'
               end
        else 'UNKNOWN'
      end as key_value,
      round(sum(coalesce(ti.amount_ex_vat,0)),2) as amount_ex_vat,
      round(sum(coalesce(ti.amount_inc_vat,0)),2) as amount_inc_vat
    from this_items_raw ti
    left join this_seg_date_final sdf
      on sdf.timesheet_id = ti.timesheet_id
     and sdf.seg_id = coalesce(
       nullif(btrim(coalesce(ti.segment_key,'')), ''),
       case
         when ti.source_ref is not null and btrim(ti.source_ref) like 'seg:%'
           then nullif(btrim(split_part(ti.source_ref,':',2)), '')
         else null
       end
     )
    group by
      ti.timesheet_id,
      case
        when ti.item_type = 'SEGMENT_DELTA'
          then case when sdf.seg_date is not null then 'TS_DAY' else 'TS_TOTAL' end
        when ti.item_type = 'MILEAGE_DELTA'
          then 'EXPENSE_CODE'
        when ti.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
          then case
                 when ti.source_ref is not null and (
                   btrim(ti.source_ref) like 'additional:%'
                   or btrim(ti.source_ref) like 'add:%'
                   or btrim(ti.source_ref) = 'additional'
                 )
                 then 'ADDITIONAL_CODE'
                 else 'EXPENSE_CODE'
               end
        else 'EXPENSE_CODE'
      end,
      case
        when ti.item_type = 'SEGMENT_DELTA'
          then coalesce(sdf.seg_date, 'TOTAL')
        when ti.item_type = 'MILEAGE_DELTA'
          then 'MILEAGE'
        when ti.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
          then case
                 when ti.source_ref is not null and (btrim(ti.source_ref) like 'additional:%' or btrim(ti.source_ref) like 'add:%')
                   then upper(nullif(btrim(split_part(ti.source_ref,':',2)), ''))
                 when ti.source_ref is not null and btrim(ti.source_ref) = 'additional'
                   then 'TOTAL'
                 when ti.source_ref is not null and btrim(ti.source_ref) <> ''
                   then upper(btrim(ti.source_ref))
                 else 'UNKNOWN'
               end
        else 'UNKNOWN'
      end
  ),
  other_active_items as (
    select
      pb_r.id as pay_batch_id,
      pbi.timesheet_id,
      pbi.item_type,
      pbi.segment_key,
      pbi.source_ref,
      coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0)::numeric as amount_ex_vat,
      coalesce(pbi.amount_inc_vat, pbi.amount_ex_vat, 0)::numeric as amount_inc_vat
    from inp i
    join public.pay_batch_items pbi
      on pbi.timesheet_id = any(i.ts_ids)
    join public.pay_batch_candidates pbc_r
      on pbc_r.id = pbi.pay_batch_candidate_id
    join public.pay_batches pb_r
      on pb_r.id = pbc_r.pay_batch_id
    where pbi.timesheet_id is not null
      and pb_r.id <> p_pay_batch_id
      and pbi.is_voided = false
      and pbi.pay_channel in ('PAYE','UMBRELLA')
      and upper(coalesce(pb_r.status,'')) in (
        'DRAFT',
        'DRAFT_CREATED',
        'READY',
        'WAITING_BANK_CONFIRM',
        'PARTIAL',
        'FAILED',
        'BLOCKED_FUNDS',
        'SCHEDULED',
        'EXECUTING',
        'AWAITING_AUTHORISATION',
        'AUTHORISED_FOR_PAYMENT'
      )
      and pbi.item_type not in (
        'DEBT_CREATED',
        'LOAN_REPAYMENT',
        'OVERPAYMENT_RECOVERY',
        'LOAN_PAYOUT'
      )
      and pbi.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
  ),
  snap_choice as (
    select
      ai.pay_batch_id,
      ai.timesheet_id,
      (
        select pbs1.target_snapshot_json
        from public.pay_batch_timesheet_snapshots pbs1
        where pbs1.pay_batch_id = ai.pay_batch_id
          and pbs1.timesheet_id = ai.timesheet_id
        order by pbs1.created_at_utc desc, pbs1.id desc
        limit 1
      ) as target_snapshot_json
    from (select distinct pay_batch_id, timesheet_id from other_active_items) ai
  ),
  seg_lookup as (
    select
      ai.pay_batch_id,
      ai.timesheet_id,
      coalesce(
        nullif(btrim(coalesce(ai.segment_key,'')), ''),
        case
          when ai.source_ref is not null and btrim(ai.source_ref) like 'seg:%'
            then nullif(btrim(split_part(ai.source_ref,':',2)), '')
          else null
        end
      ) as seg_id
    from other_active_items ai
    where ai.item_type = 'SEGMENT_DELTA'
  ),
  seg_date_map as (
    select
      sl.pay_batch_id,
      sl.timesheet_id,
      sl.seg_id,
      nullif(btrim(coalesce(seg->>'date','')), '') as seg_date_raw
    from seg_lookup sl
    join snap_choice sc
      on sc.pay_batch_id = sl.pay_batch_id
     and sc.timesheet_id = sl.timesheet_id
    join lateral jsonb_array_elements(coalesce(sc.target_snapshot_json->'segments','[]'::jsonb)) as seg on true
    where sl.seg_id is not null
      and seg is not null
      and jsonb_typeof(seg)='object'
      and nullif(btrim(coalesce(seg->>'segment_id','')), '') = sl.seg_id
  ),
  seg_date_final as (
    select
      sdm.pay_batch_id,
      sdm.timesheet_id,
      sdm.seg_id,
      case when sdm.seg_date_raw ~ '^\\d{4}-\\d{2}-\\d{2}$' then sdm.seg_date_raw else null end as seg_date
    from seg_date_map sdm
  ),
  reserved_other as (
    select
      ai.timesheet_id,
      case
        when ai.item_type = 'SEGMENT_DELTA'
          then case when sdf.seg_date is not null then 'TS_DAY' else 'TS_TOTAL' end
        when ai.item_type = 'MILEAGE_DELTA'
          then 'EXPENSE_CODE'
        when ai.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
          then case
                 when ai.source_ref is not null and (
                   btrim(ai.source_ref) like 'additional:%'
                   or btrim(ai.source_ref) like 'add:%'
                   or btrim(ai.source_ref) = 'additional'
                 )
                 then 'ADDITIONAL_CODE'
                 else 'EXPENSE_CODE'
               end
        else 'EXPENSE_CODE'
      end as key_type,
      case
        when ai.item_type = 'SEGMENT_DELTA'
          then coalesce(sdf.seg_date, 'TOTAL')
        when ai.item_type = 'MILEAGE_DELTA'
          then 'MILEAGE'
        when ai.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
          then case
                 when ai.source_ref is not null and (btrim(ai.source_ref) like 'additional:%' or btrim(ai.source_ref) like 'add:%')
                   then upper(nullif(btrim(split_part(ai.source_ref,':',2)), ''))
                 when ai.source_ref is not null and btrim(ai.source_ref) = 'additional'
                   then 'TOTAL'
                 when ai.source_ref is not null and btrim(ai.source_ref) <> ''
                   then upper(btrim(ai.source_ref))
                 else 'UNKNOWN'
               end
        else 'UNKNOWN'
      end as key_value,
      round(sum(coalesce(ai.amount_ex_vat,0)),2) as amount_ex_vat,
      round(sum(coalesce(ai.amount_inc_vat,0)),2) as amount_inc_vat
    from other_active_items ai
    left join seg_date_final sdf
      on sdf.pay_batch_id = ai.pay_batch_id
     and sdf.timesheet_id = ai.timesheet_id
     and sdf.seg_id = coalesce(
       nullif(btrim(coalesce(ai.segment_key,'')), ''),
       case
         when ai.source_ref is not null and btrim(ai.source_ref) like 'seg:%'
           then nullif(btrim(split_part(ai.source_ref,':',2)), '')
         else null
       end
     )
    group by
      ai.timesheet_id,
      case
        when ai.item_type = 'SEGMENT_DELTA'
          then case when sdf.seg_date is not null then 'TS_DAY' else 'TS_TOTAL' end
        when ai.item_type = 'MILEAGE_DELTA'
          then 'EXPENSE_CODE'
        when ai.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
          then case
                 when ai.source_ref is not null and (
                   btrim(ai.source_ref) like 'additional:%'
                   or btrim(ai.source_ref) like 'add:%'
                   or btrim(ai.source_ref) = 'additional'
                 )
                 then 'ADDITIONAL_CODE'
                 else 'EXPENSE_CODE'
               end
        else 'EXPENSE_CODE'
      end,
      case
        when ai.item_type = 'SEGMENT_DELTA'
          then coalesce(sdf.seg_date, 'TOTAL')
        when ai.item_type = 'MILEAGE_DELTA'
          then 'MILEAGE'
        when ai.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
          then case
                 when ai.source_ref is not null and (btrim(ai.source_ref) like 'additional:%' or btrim(ai.source_ref) like 'add:%')
                   then upper(nullif(btrim(split_part(ai.source_ref,':',2)), ''))
                 when ai.source_ref is not null and btrim(ai.source_ref) = 'additional'
                   then 'TOTAL'
                 when ai.source_ref is not null and btrim(ai.source_ref) <> ''
                   then upper(btrim(ai.source_ref))
                 else 'UNKNOWN'
               end
        else 'UNKNOWN'
      end
  ),
  tb as (
    select
      oc.timesheet_id,
      oc.key_type,
      oc.key_value,
      round(coalesce(oc.truth_ex_vat,0),2) as truth_ex_vat,
      round(coalesce(oc.baseline_ex_vat,0),2) as baseline_ex_vat
    from public._pay_outstanding_components((select ts_ids from inp)) oc
  ),
  joined as (
    select
      tc.timesheet_id,
      tc.key_type,
      tc.key_value,
      round(coalesce(tc.amount_ex_vat,0),2) as actual_ex_vat,
      round(
        coalesce(tb0.truth_ex_vat,0)
        - coalesce(tb0.baseline_ex_vat,0)
        - coalesce(ro.amount_ex_vat,0),
        2
      ) as expected_ex_vat
    from this_components tc
    left join tb tb0
      on tb0.timesheet_id = tc.timesheet_id
     and tb0.key_type = tc.key_type
     and tb0.key_value = tc.key_value
    left join reserved_other ro
      on ro.timesheet_id = tc.timesheet_id
     and ro.key_type = tc.key_type
     and ro.key_value = tc.key_value
  ),
  key_diffs as (
    select
      j.timesheet_id,
      j.key_type,
      j.key_value,
      j.expected_ex_vat,
      j.actual_ex_vat
    from joined j
    where round(coalesce(j.expected_ex_vat,0),2) <> round(coalesce(j.actual_ex_vat,0),2)
  )
  select
    count(*)::int,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'timesheet_id', kd.timesheet_id::text,
          'key_type', kd.key_type,
          'key_value', kd.key_value,
          'expected', kd.expected_ex_vat,
          'actual', kd.actual_ex_vat
        )
        order by kd.timesheet_id::text, kd.key_type, kd.key_value
      ),
      '[]'::jsonb
    )
  into
    v_key_diff_ct,
    v_diffs
  from (
    select * from key_diffs
    limit v_diff_limit
  ) kd;

  if v_key_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'RESERVATION_CHANGED');
  end if;

  ---------------------------------------------------------------------------
  -- DEDUCTION_CHANGED: recompute expected OVERPAYMENT_RECOVERY + LOAN_REPAYMENT and compare to this batch
  ---------------------------------------------------------------------------
  if v_scope in ('PAYE','UMBRELLA') then
    -- OVERPAYMENT_RECOVERY diffs (per advance_id)
    with
    actual_overpay as (
      select
        nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid as advance_id,
        round(sum(-coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0)),2)::numeric(12,2) as taken_ex
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc
        on pbc.id = pbi.pay_batch_candidate_id
      where pbc.pay_batch_id = p_pay_batch_id
        and pbi.is_voided = false
        and pbi.item_type = 'OVERPAYMENT_RECOVERY'
        and pbi.source_ref is not null
        and btrim(coalesce(pbi.source_ref,'')) like 'advance:%'
      group by nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid
    ),
    cand_scope as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        v_scope as pay_channel,
        case when v_scope = 'UMBRELLA' then c_sc.umbrella_id else null end as umbrella_id,
        pbc.awaiting_net_amount,
        pni.net_amount as paye_net_amount
      from public.pay_batch_candidates pbc
      join public.candidates c_sc
        on c_sc.id = pbc.candidate_id
      left join public.pay_batch_paye_net_inputs pni
        on pni.pay_batch_candidate_id = pbc.id
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    cand_earnings as (
      select
        cs.pay_batch_candidate_id,
        cs.candidate_id,
        cs.pay_channel,
        cs.umbrella_id,
        cs.awaiting_net_amount,
        greatest(
          coalesce(
            case
              when cs.pay_channel = 'PAYE' then cs.paye_net_amount
              else (
                select coalesce(sum(pbi2.amount_ex_vat), 0)
                from public.pay_batch_items pbi2
                where pbi2.pay_batch_candidate_id = cs.pay_batch_candidate_id
                  and pbi2.is_voided = false
                  and pbi2.amount_ex_vat > 0
                  and pbi2.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
              )
            end,
            0
          ),
          0
        )::numeric(12,2) as earnings_before_loan_ex
      from cand_scope cs
    ),
    overpay_advances as (
      select
        pa.id as advance_id,
        pa.candidate_id,
        pa.outstanding_amount::numeric(12,2) as outstanding_amount,
        pa.created_at
      from public.pay_advances pa
      where pa.advance_kind = 'OVERPAYMENT'::public.pay_advance_kind_enum
        and pa.status = 'ACTIVE'::public.pay_advance_status_enum
        and pa.outstanding_amount > 0
    ),
    cand_overpay as (
      select
        ce.pay_batch_candidate_id,
        ce.candidate_id,
        ce.earnings_before_loan_ex,
        round(coalesce(sum(oa.outstanding_amount), 0), 2)::numeric(12,2) as overpayment_outstanding_ex
      from cand_earnings ce
      left join overpay_advances oa
        on oa.candidate_id = ce.candidate_id
      where ce.awaiting_net_amount = false
      group by ce.pay_batch_candidate_id, ce.candidate_id, ce.earnings_before_loan_ex
    ),
    cand_recovery as (
      select
        co.pay_batch_candidate_id,
        co.candidate_id,
        round(least(co.overpayment_outstanding_ex, co.earnings_before_loan_ex), 2)::numeric(12,2) as recovery_total_ex
      from cand_overpay co
      where round(least(co.overpayment_outstanding_ex, co.earnings_before_loan_ex), 2) > 0
    ),
    alloc_base as (
      select
        cr.candidate_id,
        oa.advance_id,
        oa.outstanding_amount,
        oa.created_at,
        cr.recovery_total_ex,
        sum(oa.outstanding_amount) over (
          partition by cr.candidate_id
          order by oa.created_at, oa.advance_id
          rows between unbounded preceding and 1 preceding
        )::numeric(12,2) as cum_before_ex
      from cand_recovery cr
      join overpay_advances oa
        on oa.candidate_id = cr.candidate_id
    ),
    expected_overpay as (
      select
        ab.advance_id,
        round(
          least(
            ab.outstanding_amount,
            greatest(ab.recovery_total_ex - coalesce(ab.cum_before_ex, 0), 0)
          ),
          2
        )::numeric(12,2) as take_ex
      from alloc_base ab
      where round(
        least(
          ab.outstanding_amount,
          greatest(ab.recovery_total_ex - coalesce(ab.cum_before_ex, 0), 0)
        ),
        2
      ) > 0
    ),
    union_keys as (
      select eo.advance_id from expected_overpay eo
      union
      select ao.advance_id from actual_overpay ao
    ),
    overpay_diffs as (
      select
        uk.advance_id,
        round(coalesce(eo.take_ex,0),2)::numeric(12,2) as expected_ex,
        round(coalesce(ao.taken_ex,0),2)::numeric(12,2) as actual_ex
      from union_keys uk
      left join expected_overpay eo
        on eo.advance_id = uk.advance_id
      left join actual_overpay ao
        on ao.advance_id = uk.advance_id
      where round(coalesce(eo.take_ex,0),2) <> round(coalesce(ao.taken_ex,0),2)
    )
    select
      count(*)::int,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'timesheet_id', null,
            'key_type', 'OVERPAYMENT_RECOVERY',
            'key_value', od.advance_id::text,
            'expected', od.expected_ex,
            'actual', od.actual_ex
          )
          order by od.advance_id::text
        ),
        '[]'::jsonb
      )
    into
      v_ded_diff_ct,
      v_diffs
    from (
      select * from overpay_diffs
      limit v_diff_limit
    ) od;

    if v_ded_diff_ct > 0 then
      v_is_stale := true;
      v_reasons := array_append(v_reasons, 'DEDUCTION_CHANGED');
    end if;

    -- LOAN_REPAYMENT diffs (per loan_id for this week)
    with
    actual_loans as (
      select
        replace(pbi.source_ref, 'advance:', '')::uuid as loan_id,
        round(sum(-coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0)),2)::numeric(12,2) as taken_ex
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc
        on pbc.id = pbi.pay_batch_candidate_id
      where pbc.pay_batch_id = p_pay_batch_id
        and pbi.is_voided = false
        and pbi.item_type = 'LOAN_REPAYMENT'
        and pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
        and pbi.repayment_week_start = v_week_start
      group by replace(pbi.source_ref, 'advance:', '')::uuid
    ),
    cand_scope as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        v_scope as pay_channel,
        case when v_scope = 'UMBRELLA' then c_sc.umbrella_id else null end as umbrella_id,
        pbc.awaiting_net_amount,
        coalesce(pbc.overpayment_recovery_taken, 0)::numeric(12,2) as overpayment_recovery_taken_ex,
        pni.net_amount as paye_net_amount
      from public.pay_batch_candidates pbc
      join public.candidates c_sc
        on c_sc.id = pbc.candidate_id
      left join public.pay_batch_paye_net_inputs pni
        on pni.pay_batch_candidate_id = pbc.id
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    cand_earnings as (
      select
        cs.pay_batch_candidate_id,
        cs.candidate_id,
        cs.pay_channel,
        cs.umbrella_id,
        cs.awaiting_net_amount,
        cs.overpayment_recovery_taken_ex,
        greatest(
          coalesce(
            case
              when cs.pay_channel = 'PAYE' then cs.paye_net_amount
              else (
                select coalesce(sum(pbi2.amount_ex_vat), 0)
                from public.pay_batch_items pbi2
                where pbi2.pay_batch_candidate_id = cs.pay_batch_candidate_id
                  and pbi2.is_voided = false
                  and pbi2.amount_ex_vat > 0
                  and pbi2.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
              )
            end,
            0
          ),
          0
        )::numeric(12,2) as earnings_before_loan_ex
      from cand_scope cs
    ),
    cand_limits as (
      select
        ce.pay_batch_candidate_id,
        ce.candidate_id,
        ce.pay_channel,
        ce.umbrella_id,
        greatest(ce.earnings_before_loan_ex - ce.overpayment_recovery_taken_ex, 0)::numeric(12,2) as earnings_after_recovery_ex
      from cand_earnings ce
      where ce.awaiting_net_amount = false
    ),
    paid_wtd as (
      select
        pbc2.candidate_id,
        round(coalesce(sum(pbi2.amount_ex_vat), 0), 2)::numeric(12,2) as paid_wtd_before_ex
      from public.pay_batch_candidates pbc2
      join public.pay_batches pb2
        on pb2.id = pbc2.pay_batch_id
      join public.pay_batch_items pbi2
        on pbi2.pay_batch_candidate_id = pbc2.id
      where pb2.cancelled_at_utc is null
        and pb2.id <> p_pay_batch_id
        and coalesce(pb2.batch_kind_fixed, '') <> 'LOANS'
        and pb2.pay_date >= v_week_start
        and pb2.pay_date < (v_week_start + 7)
        and pbi2.is_voided = false
        and pbi2.item_type <> 'DEBT_CREATED'
      group by pbc2.candidate_id
    ),
    cand_with_floor as (
      select
        cl.pay_batch_candidate_id,
        cl.candidate_id,
        cl.pay_channel,
        cl.umbrella_id,
        cl.earnings_after_recovery_ex,
        coalesce(pw.paid_wtd_before_ex, 0)::numeric(12,2) as paid_wtd_before_ex,
        coalesce(c.min_take_home_wtd, 0)::numeric(12,2) as floor_ex,
        round(
          greatest(
            least(
              cl.earnings_after_recovery_ex,
              (coalesce(pw.paid_wtd_before_ex, 0) + cl.earnings_after_recovery_ex) - coalesce(c.min_take_home_wtd, 0)
            ),
            0
          ),
          2
        )::numeric(12,2) as max_loan_repayment_ex
      from cand_limits cl
      join public.candidates c
        on c.id = cl.candidate_id
      left join paid_wtd pw
        on pw.candidate_id = cl.candidate_id
      where cl.earnings_after_recovery_ex > 0
    ),
    loans as (
      select
        pa.id as loan_id,
        pa.candidate_id,
        pa.outstanding_amount::numeric(12,2) as outstanding_amount,
        pa.weekly_due::numeric(12,2) as weekly_due,
        pa.start_week_start,
        pa.created_at
      from public.pay_advances pa
      where pa.advance_kind = 'LOAN'::public.pay_advance_kind_enum
        and pa.payout_status = 'PAID'::public.pay_advance_payout_status_enum
        and pa.status = 'ACTIVE'::public.pay_advance_status_enum
        and pa.outstanding_amount > 0
        and pa.weekly_due is not null
        and pa.weekly_due > 0
        and (pa.start_week_start is null or pa.start_week_start <= v_week_start)
    ),
    loan_repaid_wtd as (
      select
        pbc2.candidate_id,
        replace(pbi2.source_ref, 'advance:', '')::uuid as loan_id,
        round(coalesce(sum(-pbi2.amount_ex_vat), 0), 2)::numeric(12,2) as repaid_wtd_ex
      from public.pay_batch_items pbi2
      join public.pay_batch_candidates pbc2
        on pbc2.id = pbi2.pay_batch_candidate_id
      join public.pay_batches pb2
        on pb2.id = pbc2.pay_batch_id
      where pbi2.item_type = 'LOAN_REPAYMENT'
        and pbi2.is_voided = false
        and pbi2.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
        and pbi2.repayment_week_start = v_week_start
        and pb2.cancelled_at_utc is null
        and pb2.id <> p_pay_batch_id
        and coalesce(pb2.batch_kind_fixed, '') <> 'LOANS'
      group by
        pbc2.candidate_id,
        replace(pbi2.source_ref, 'advance:', '')::uuid
    ),
    loan_due as (
      select
        cwf.pay_batch_candidate_id,
        cwf.candidate_id,
        cwf.pay_channel,
        cwf.umbrella_id,
        cwf.max_loan_repayment_ex,
        l.loan_id,
        l.outstanding_amount,
        l.weekly_due,
        l.start_week_start,
        l.created_at,
        least(l.weekly_due, l.outstanding_amount)::numeric(12,2) as due_this_week_ex,
        greatest(
          least(l.weekly_due, l.outstanding_amount) - coalesce(lrw.repaid_wtd_ex, 0),
          0
        )::numeric(12,2) as remaining_due_ex
      from cand_with_floor cwf
      join loans l
        on l.candidate_id = cwf.candidate_id
      left join loan_repaid_wtd lrw
        on lrw.candidate_id = cwf.candidate_id
       and lrw.loan_id = l.loan_id
      where cwf.max_loan_repayment_ex > 0
        and greatest(
          least(l.weekly_due, l.outstanding_amount) - coalesce(lrw.repaid_wtd_ex, 0),
          0
        ) > 0
    ),
    alloc_base as (
      select
        ld.candidate_id,
        ld.loan_id,
        ld.remaining_due_ex,
        ld.max_loan_repayment_ex,
        sum(ld.remaining_due_ex) over (
          partition by ld.candidate_id
          order by ld.start_week_start nulls first, ld.created_at, ld.loan_id
          rows between unbounded preceding and 1 preceding
        )::numeric(12,2) as cum_before_ex
      from loan_due ld
    ),
    expected_loans as (
      select
        ab.loan_id,
        round(
          least(
            ab.remaining_due_ex,
            greatest(ab.max_loan_repayment_ex - coalesce(ab.cum_before_ex, 0), 0)
          ),
          2
        )::numeric(12,2) as take_ex
      from alloc_base ab
      where round(
        least(
          ab.remaining_due_ex,
          greatest(ab.max_loan_repayment_ex - coalesce(ab.cum_before_ex, 0), 0)
        ),
        2
      ) > 0
    ),
    union_keys as (
      select el.loan_id from expected_loans el
      union
      select al.loan_id from actual_loans al
    ),
    loan_diffs as (
      select
        uk.loan_id,
        round(coalesce(el.take_ex,0),2)::numeric(12,2) as expected_ex,
        round(coalesce(al.taken_ex,0),2)::numeric(12,2) as actual_ex
      from union_keys uk
      left join expected_loans el
        on el.loan_id = uk.loan_id
      left join actual_loans al
        on al.loan_id = uk.loan_id
      where round(coalesce(el.take_ex,0),2) <> round(coalesce(al.taken_ex,0),2)
    )
    select
      count(*)::int,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'timesheet_id', null,
            'key_type', 'LOAN_REPAYMENT',
            'key_value', ld.loan_id::text,
            'expected', ld.expected_ex,
            'actual', ld.actual_ex
          )
          order by ld.loan_id::text
        ),
        '[]'::jsonb
      )
    into
      v_ded_diff_ct,
      v_diffs
    from (
      select * from loan_diffs
      limit v_diff_limit
    ) ld;

    if v_ded_diff_ct > 0 then
      v_is_stale := true;
      v_reasons := array_append(v_reasons, 'DEDUCTION_CHANGED');
    end if;
  end if;

  ---------------------------------------------------------------------------
  -- PAYE_NET_CHANGED: diff[] must include candidate net deltas (and net/awaiting drift)
  ---------------------------------------------------------------------------
  if v_scope = 'PAYE' and coalesce(v_batch_kind_fixed,'') <> 'LOANS' then
    with
    cand as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        coalesce(pbc.awaiting_net_amount,false) as awaiting_net_amount,
        round(coalesce(pbc.net_bank_amount,0),2)::numeric(12,2) as net_bank_amount_ex,
        round(coalesce(pbc.overpayment_recovery_taken,0),2)::numeric(12,2) as overpayment_recovery_taken_ex,
        round(coalesce(pbc.loan_repayment_taken,0),2)::numeric(12,2) as loan_repayment_taken_ex
      from public.pay_batch_candidates pbc
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    net_inp as (
      select
        c.pay_batch_candidate_id,
        pni.net_amount::numeric(12,2) as net_amount
      from cand c
      left join public.pay_batch_paye_net_inputs pni
        on pni.pay_batch_candidate_id = c.pay_batch_candidate_id
    ),
    ded_present as (
      select
        c.pay_batch_candidate_id,
        exists(
          select 1
          from public.pay_batch_items pbi
          where pbi.pay_batch_candidate_id = c.pay_batch_candidate_id
            and pbi.is_voided = false
            and pbi.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT')
        ) as has_deductions
      from cand c
    ),
    net_diffs as (
      select
        c.candidate_id,
        c.awaiting_net_amount,
        (ni.net_amount is not null) as has_net_input,
        dp.has_deductions,
        ni.net_amount::numeric(12,2) as expected_net,
        round(c.net_bank_amount_ex + c.overpayment_recovery_taken_ex + c.loan_repayment_taken_ex, 2)::numeric(12,2) as actual_net
      from cand c
      left join net_inp ni
        on ni.pay_batch_candidate_id = c.pay_batch_candidate_id
      join ded_present dp
        on dp.pay_batch_candidate_id = c.pay_batch_candidate_id
      where
            c.awaiting_net_amount <> (ni.net_amount is null)
         or ((ni.net_amount is null) and dp.has_deductions = true)
         or (ni.net_amount is not null
             and round(ni.net_amount,2) <> round(c.net_bank_amount_ex + c.overpayment_recovery_taken_ex + c.loan_repayment_taken_ex, 2))
    )
    select
      count(*)::int,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'timesheet_id', null,
            'key_type', 'PAYE_NET',
            'key_value', ('candidate:' || nd.candidate_id::text),
            'expected', nd.expected_net,
            'actual', nd.actual_net
          )
          order by nd.candidate_id::text
        ),
        '[]'::jsonb
      )
    into
      v_paye_net_diff_ct,
      v_diffs
    from (
      select * from net_diffs
      limit v_diff_limit
    ) nd;

    if v_paye_net_diff_ct > 0 then
      v_is_stale := true;
      v_reasons := array_append(v_reasons, 'PAYE_NET_CHANGED');
    end if;
  end if;

  ---------------------------------------------------------------------------
  -- Build final diff array: concatenate the captured diff sets (each stage overwrote v_diffs)
  -- So re-run as a single union here to provide a stable combined output.
  ---------------------------------------------------------------------------
  with
  diffs_all as (
    -- timesheet signature diffs
    select
      pbs.timesheet_id::text as timesheet_id,
      'TS_SIGNATURE'::text as key_type,
      'SIGNATURE'::text as key_value,
      pbs.signature::text as expected,
      cs.current_signature::text as actual,
      1 as ord
    from public.pay_batch_timesheet_snapshots pbs
    join (
      with
      pbts2 as (
        select
          pbs2.timesheet_id
        from public.pay_batch_timesheet_snapshots pbs2
        where pbs2.pay_batch_id = p_pay_batch_id
          and pbs2.timesheet_id is not null
      ),
      tf02 as (
        select
          tf.timesheet_id,
          ts.reference_number,
          tf.invoice_breakdown_json,
          tf.hours_day,
          tf.hours_night,
          tf.hours_sat,
          tf.hours_sun,
          tf.hours_bh,
          tf.pay_day,
          tf.pay_night,
          tf.pay_sat,
          tf.pay_sun,
          tf.pay_bh,
          tf.additional_units_json,
          tf.mileage_units,
          tf.mileage_pay_rate,
          round(coalesce(tf.total_pay_ex_vat,0),2) as total_pay_ex_vat,
          round(coalesce(tf.expenses_pay_ex_vat,0),2) as expenses_pay_ex_vat,
          round(coalesce(tf.travel_pay_ex_vat,0),2) as travel_pay_ex_vat,
          round(coalesce(tf.accommodation_pay_ex_vat,0),2) as accommodation_pay_ex_vat,
          round(coalesce(tf.other_pay_ex_vat,0),2) as other_pay_ex_vat,
          round(coalesce(tf.mileage_pay_ex_vat,0),2) as mileage_pay_ex_vat
        from public.timesheets_financials tf
        join pbts2 x
          on x.timesheet_id = tf.timesheet_id
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        where tf.is_current = true
      ),
      cur02 as (
        select
          t.*,
          case
            when t.invoice_breakdown_json is not null
             and jsonb_typeof(t.invoice_breakdown_json)='object'
             and upper(coalesce(t.invoice_breakdown_json->>'mode',''))='SEGMENTS'
             and jsonb_typeof(t.invoice_breakdown_json->'segments')='array'
            then (
              select coalesce(
                jsonb_agg(
                  jsonb_build_object(
                    'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')),''),
                    'date', nullif(btrim(coalesce(seg->>'date','')),''),
                    'start_utc', nullif(btrim(coalesce(seg->>'start_utc','')),''),
                    'end_utc', nullif(btrim(coalesce(seg->>'end_utc','')),''),
                    'break_mins', coalesce(nullif(seg->>'break_mins','')::numeric,0),
                    'breaks', coalesce(seg->'breaks','[]'::jsonb),
                    'hours_day', coalesce(nullif(seg->>'hours_day','')::numeric,0),
                    'hours_night', coalesce(nullif(seg->>'hours_night','')::numeric,0),
                    'hours_sat', coalesce(nullif(seg->>'hours_sat','')::numeric,0),
                    'hours_sun', coalesce(nullif(seg->>'hours_sun','')::numeric,0),
                    'hours_bh', coalesce(nullif(seg->>'hours_bh','')::numeric,0),
                    'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
                    'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean,false),
                    'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')),'')
                  )
                ),
                '[]'::jsonb
              )
              from jsonb_array_elements(t.invoice_breakdown_json->'segments') seg
              where seg is not null and jsonb_typeof(seg)='object'
            )
            else jsonb_build_array(
              jsonb_build_object(
                'segment_id', ('ts:' || t.timesheet_id::text),
                'pay_amount', round(coalesce(t.total_pay_ex_vat,0),2),
                'exclude_from_pay', false,
                'ref_num', nullif(btrim(coalesce(t.reference_number,'')), '')
              )
            )
          end as cur_segments,
          case
            when t.invoice_breakdown_json is not null
             and jsonb_typeof(t.invoice_breakdown_json)='object'
             and upper(coalesce(t.invoice_breakdown_json->>'mode',''))='SEGMENTS'
            then round(coalesce(nullif(t.invoice_breakdown_json #>> '{additional,pay_ex_vat}','')::numeric,0),2)
            else 0::numeric
          end as cur_additional,
          coalesce(
            (
              select coalesce(
                jsonb_agg(
                  jsonb_build_object(
                    'id', a.id::text,
                    'delta_pay_ex_vat', round(coalesce(a.delta_pay_ex_vat,0),2)
                  )
                  order by a.id
                ),
                '[]'::jsonb
              )
              from public.ts_pay_adjustments a
              where a.timesheet_id = t.timesheet_id
                and a.as_advance = false
            ),
            '[]'::jsonb
          ) as cur_adjs
        from tf02 t
      )
      select
        c.timesheet_id,
        md5(
          jsonb_build_object(
            'segments', coalesce(c.cur_segments, '[]'::jsonb),
            'additional_pay_ex_vat', round(coalesce(c.cur_additional,0),2),
            'additional_units_json', coalesce(c.additional_units_json, '{}'::jsonb),
            'hours_day', round(coalesce(c.hours_day,0),2),
            'hours_night', round(coalesce(c.hours_night,0),2),
            'hours_sat', round(coalesce(c.hours_sat,0),2),
            'hours_sun', round(coalesce(c.hours_sun,0),2),
            'hours_bh', round(coalesce(c.hours_bh,0),2),
            'pay_day', round(coalesce(c.pay_day,0),2),
            'pay_night', round(coalesce(c.pay_night,0),2),
            'pay_sat', round(coalesce(c.pay_sat,0),2),
            'pay_sun', round(coalesce(c.pay_sun,0),2),
            'pay_bh', round(coalesce(c.pay_bh,0),2),
            'mileage_units', round(coalesce(c.mileage_units,0),2),
            'mileage_pay_rate', c.mileage_pay_rate,
            'expenses', jsonb_build_object(
              'expenses_pay_ex_vat', round(coalesce(c.expenses_pay_ex_vat,0),2),
              'travel_pay_ex_vat', round(coalesce(c.travel_pay_ex_vat,0),2),
              'accommodation_pay_ex_vat', round(coalesce(c.accommodation_pay_ex_vat,0),2),
              'other_pay_ex_vat', round(coalesce(c.other_pay_ex_vat,0),2),
              'mileage_pay_ex_vat', round(coalesce(c.mileage_pay_ex_vat,0),2)
            ),
            'adjustments', coalesce(c.cur_adjs, '[]'::jsonb)
          )::text
        ) as current_signature
      from cur02 c
    ) cs
      on cs.timesheet_id = pbs.timesheet_id
    where pbs.pay_batch_id = p_pay_batch_id
      and coalesce(pbs.signature,'') <> coalesce(cs.current_signature,'')
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'timesheet_id', d.timesheet_id,
          'key_type', d.key_type,
          'key_value', d.key_value,
          'expected', d.expected,
          'actual', d.actual
        )
        order by d.ord, d.timesheet_id, d.key_type, d.key_value
      ),
      '[]'::jsonb
    )
  into v_diffs
  from (
    select * from diffs_all
    union all
    -- reservation/key diffs
    select
      (kd->>'timesheet_id') as timesheet_id,
      (kd->>'key_type') as key_type,
      (kd->>'key_value') as key_value,
      (kd->>'expected') as expected,
      (kd->>'actual') as actual,
      2 as ord
    from jsonb_array_elements(
      coalesce(
        (
          select coalesce(
            jsonb_agg(
              jsonb_build_object(
                'timesheet_id', x.timesheet_id::text,
                'key_type', x.key_type,
                'key_value', x.key_value,
                'expected', x.expected,
                'actual', x.actual
              )
              order by x.timesheet_id::text, x.key_type, x.key_value
            ),
            '[]'::jsonb
          )
          from (
            with
            inp2 as (
              select coalesce(
                (select array_agg(distinct x) from unnest(coalesce(v_ts_ids, array[]::uuid[])) as t(x) where x is not null),
                array[]::uuid[]
              ) as ts_ids
            ),
            tc2 as (
              select
                pbi3.timesheet_id,
                pbi3.item_type,
                pbi3.segment_key,
                pbi3.source_ref,
                coalesce(pbi3.amount_ex_vat, pbi3.amount_inc_vat, 0)::numeric as amount_ex_vat
              from public.pay_batch_items pbi3
              join public.pay_batch_candidates pbc3
                on pbc3.id = pbi3.pay_batch_candidate_id
              where pbc3.pay_batch_id = p_pay_batch_id
                and pbi3.timesheet_id is not null
                and pbi3.is_voided = false
                and pbi3.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
            ),
            snap2 as (
              select
                pbs2.timesheet_id,
                pbs2.target_snapshot_json
              from public.pay_batch_timesheet_snapshots pbs2
              where pbs2.pay_batch_id = p_pay_batch_id
            ),
            sl2 as (
              select
                t.timesheet_id,
                coalesce(
                  nullif(btrim(coalesce(t.segment_key,'')), ''),
                  case
                    when t.source_ref is not null and btrim(t.source_ref) like 'seg:%'
                      then nullif(btrim(split_part(t.source_ref,':',2)), '')
                    else null
                  end
                ) as seg_id
              from tc2 t
              where t.item_type = 'SEGMENT_DELTA'
            ),
            sdm2 as (
              select
                sl.timesheet_id,
                sl.seg_id,
                nullif(btrim(coalesce(seg->>'date','')), '') as seg_date_raw
              from sl2 sl
              join snap2 sc
                on sc.timesheet_id = sl.timesheet_id
              join lateral jsonb_array_elements(coalesce(sc.target_snapshot_json->'segments','[]'::jsonb)) as seg on true
              where sl.seg_id is not null
                and seg is not null
                and jsonb_typeof(seg)='object'
                and nullif(btrim(coalesce(seg->>'segment_id','')), '') = sl.seg_id
            ),
            sdf2 as (
              select
                sdm.timesheet_id,
                sdm.seg_id,
                case when sdm.seg_date_raw ~ '^\\d{4}-\\d{2}-\\d{2}$' then sdm.seg_date_raw else null end as seg_date
              from sdm2 sdm
            ),
            comp2 as (
              select
                t.timesheet_id,
                case
                  when t.item_type = 'SEGMENT_DELTA'
                    then case when sf.seg_date is not null then 'TS_DAY' else 'TS_TOTAL' end
                  when t.item_type = 'MILEAGE_DELTA'
                    then 'EXPENSE_CODE'
                  when t.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
                    then case
                           when t.source_ref is not null and (
                             btrim(t.source_ref) like 'additional:%'
                             or btrim(t.source_ref) like 'add:%'
                             or btrim(t.source_ref) = 'additional'
                           )
                           then 'ADDITIONAL_CODE'
                           else 'EXPENSE_CODE'
                         end
                  else 'EXPENSE_CODE'
                end as key_type,
                case
                  when t.item_type = 'SEGMENT_DELTA'
                    then coalesce(sf.seg_date, 'TOTAL')
                  when t.item_type = 'MILEAGE_DELTA'
                    then 'MILEAGE'
                  when t.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
                    then case
                           when t.source_ref is not null and (btrim(t.source_ref) like 'additional:%' or btrim(t.source_ref) like 'add:%')
                             then upper(nullif(btrim(split_part(t.source_ref,':',2)), ''))
                           when t.source_ref is not null and btrim(t.source_ref) = 'additional'
                             then 'TOTAL'
                           when t.source_ref is not null and btrim(t.source_ref) <> ''
                             then upper(btrim(t.source_ref))
                           else 'UNKNOWN'
                         end
                  else 'UNKNOWN'
                end as key_value,
                round(sum(coalesce(t.amount_ex_vat,0)),2) as actual,
                round(
                  coalesce(oc.truth_ex_vat,0)
                  - coalesce(oc.baseline_ex_vat,0)
                  - coalesce(ro.amount_ex_vat,0),
                  2
                ) as expected
              from tc2 t
              left join sdf2 sf
                on sf.timesheet_id = t.timesheet_id
               and sf.seg_id = coalesce(
                 nullif(btrim(coalesce(t.segment_key,'')), ''),
                 case
                   when t.source_ref is not null and btrim(t.source_ref) like 'seg:%'
                     then nullif(btrim(split_part(t.source_ref,':',2)), '')
                   else null
                 end
               )
              left join public._pay_outstanding_components((select ts_ids from inp2)) oc
                on oc.timesheet_id = t.timesheet_id
               and oc.key_type = (
                 case
                   when t.item_type = 'SEGMENT_DELTA'
                     then case when sf.seg_date is not null then 'TS_DAY' else 'TS_TOTAL' end
                   when t.item_type = 'MILEAGE_DELTA'
                     then 'EXPENSE_CODE'
                   when t.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
                     then case
                            when t.source_ref is not null and (
                              btrim(t.source_ref) like 'additional:%'
                              or btrim(t.source_ref) like 'add:%'
                              or btrim(t.source_ref) = 'additional'
                            )
                            then 'ADDITIONAL_CODE'
                            else 'EXPENSE_CODE'
                          end
                   else 'EXPENSE_CODE'
                 end
               )
               and oc.key_value = (
                 case
                   when t.item_type = 'SEGMENT_DELTA'
                     then coalesce(sf.seg_date, 'TOTAL')
                   when t.item_type = 'MILEAGE_DELTA'
                     then 'MILEAGE'
                   when t.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
                     then case
                            when t.source_ref is not null and (btrim(t.source_ref) like 'additional:%' or btrim(t.source_ref) like 'add:%')
                              then upper(nullif(btrim(split_part(t.source_ref,':',2)), ''))
                            when t.source_ref is not null and btrim(t.source_ref) = 'additional'
                              then 'TOTAL'
                            when t.source_ref is not null and btrim(t.source_ref) <> ''
                              then upper(btrim(t.source_ref))
                            else 'UNKNOWN'
                          end
                   else 'UNKNOWN'
                 end
               )
              left join (
                select
                  r.timesheet_id,
                  r.key_type,
                  r.key_value,
                  r.amount_ex_vat
                from (
                  select * from (select null::uuid as timesheet_id, null::text as key_type, null::text as key_value, 0::numeric as amount_ex_vat) z where false
                ) r
              ) ro
                on false
              group by
                t.timesheet_id,
                case
                  when t.item_type = 'SEGMENT_DELTA' then case when sf.seg_date is not null then 'TS_DAY' else 'TS_TOTAL' end
                  when t.item_type = 'MILEAGE_DELTA' then 'EXPENSE_CODE'
                  when t.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
                    then case
                           when t.source_ref is not null and (
                             btrim(t.source_ref) like 'additional:%'
                             or btrim(t.source_ref) like 'add:%'
                             or btrim(t.source_ref) = 'additional'
                           )
                           then 'ADDITIONAL_CODE'
                           else 'EXPENSE_CODE'
                         end
                  else 'EXPENSE_CODE'
                end,
                case
                  when t.item_type = 'SEGMENT_DELTA' then coalesce(sf.seg_date, 'TOTAL')
                  when t.item_type = 'MILEAGE_DELTA' then 'MILEAGE'
                  when t.item_type in ('EXPENSE_DELTA','ADJUSTMENT_DELTA')
                    then case
                           when t.source_ref is not null and (btrim(t.source_ref) like 'additional:%' or btrim(t.source_ref) like 'add:%')
                             then upper(nullif(btrim(split_part(t.source_ref,':',2)), ''))
                           when t.source_ref is not null and btrim(t.source_ref) = 'additional'
                             then 'TOTAL'
                           when t.source_ref is not null and btrim(t.source_ref) <> ''
                             then upper(btrim(t.source_ref))
                           else 'UNKNOWN'
                         end
                  else 'UNKNOWN'
                end,
                round(
                  coalesce(oc.truth_ex_vat,0)
                  - coalesce(oc.baseline_ex_vat,0)
                  - coalesce(ro.amount_ex_vat,0),
                  2
                )
            )
            select
              c2.timesheet_id,
              c2.key_type,
              c2.key_value,
              c2.expected,
              c2.actual
            from comp2 c2
            where round(coalesce(c2.expected,0),2) <> round(coalesce(c2.actual,0),2)
          ) x
        ),
        '[]'::jsonb
      )
    ) as kd
    union all
    -- DEDUCTION diffs: ADVANCE key (advance:<uuid>) for OVERPAYMENT_RECOVERY and LOAN_REPAYMENT
    select
      null::text as timesheet_id,
      'ADVANCE'::text as key_type,
      ('advance:' || dd.advance_id::text) as key_value,
      dd.expected_ex::text as expected,
      dd.actual_ex::text as actual,
      3 as ord
    from (
      (
        -- OVERPAYMENT_RECOVERY expected vs actual
        with
        actual_overpay as (
          select
            nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid as advance_id,
            round(sum(-coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0)),2)::numeric(12,2) as taken_ex
          from public.pay_batch_items pbi
          join public.pay_batch_candidates pbc
            on pbc.id = pbi.pay_batch_candidate_id
          where pbc.pay_batch_id = p_pay_batch_id
            and pbi.is_voided = false
            and pbi.item_type = 'OVERPAYMENT_RECOVERY'
            and pbi.source_ref is not null
            and btrim(coalesce(pbi.source_ref,'')) like 'advance:%'
          group by nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid
        ),
        cand_scope as (
          select
            pbc.id as pay_batch_candidate_id,
            pbc.candidate_id,
            coalesce(pbc.awaiting_net_amount,false) as awaiting_net_amount,
            pni.net_amount as paye_net_amount
          from public.pay_batch_candidates pbc
          left join public.pay_batch_paye_net_inputs pni
            on pni.pay_batch_candidate_id = pbc.id
          where pbc.pay_batch_id = p_pay_batch_id
        ),
        cand_earnings as (
          select
            cs.pay_batch_candidate_id,
            cs.candidate_id,
            cs.awaiting_net_amount,
            greatest(coalesce(cs.paye_net_amount,0),0)::numeric(12,2) as earnings_before_loan_ex
          from cand_scope cs
        ),
        overpay_advances as (
          select
            pa.id as advance_id,
            pa.candidate_id,
            pa.outstanding_amount::numeric(12,2) as outstanding_amount,
            pa.created_at
          from public.pay_advances pa
          where pa.advance_kind = 'OVERPAYMENT'::public.pay_advance_kind_enum
            and pa.status = 'ACTIVE'::public.pay_advance_status_enum
            and pa.outstanding_amount > 0
        ),
        cand_overpay as (
          select
            ce.candidate_id,
            ce.earnings_before_loan_ex,
            round(coalesce(sum(oa.outstanding_amount), 0), 2)::numeric(12,2) as overpayment_outstanding_ex
          from cand_earnings ce
          left join overpay_advances oa
            on oa.candidate_id = ce.candidate_id
          where ce.awaiting_net_amount = false
          group by ce.candidate_id, ce.earnings_before_loan_ex
        ),
        cand_recovery as (
          select
            co.candidate_id,
            round(least(co.overpayment_outstanding_ex, co.earnings_before_loan_ex), 2)::numeric(12,2) as recovery_total_ex
          from cand_overpay co
          where round(least(co.overpayment_outstanding_ex, co.earnings_before_loan_ex), 2) > 0
        ),
        alloc_base as (
          select
            cr.candidate_id,
            oa.advance_id,
            oa.outstanding_amount,
            oa.created_at,
            cr.recovery_total_ex,
            sum(oa.outstanding_amount) over (
              partition by cr.candidate_id
              order by oa.created_at, oa.advance_id
              rows between unbounded preceding and 1 preceding
            )::numeric(12,2) as cum_before_ex
          from cand_recovery cr
          join overpay_advances oa
            on oa.candidate_id = cr.candidate_id
        ),
        expected_overpay as (
          select
            ab.advance_id,
            round(
              least(
                ab.outstanding_amount,
                greatest(ab.recovery_total_ex - coalesce(ab.cum_before_ex, 0), 0)
              ),
              2
            )::numeric(12,2) as take_ex
          from alloc_base ab
          where round(
            least(
              ab.outstanding_amount,
              greatest(ab.recovery_total_ex - coalesce(ab.cum_before_ex, 0), 0)
            ),
            2
          ) > 0
        ),
        union_keys as (
          select eo.advance_id from expected_overpay eo
          union
          select ao.advance_id from actual_overpay ao
        ),
        overpay_diffs as (
          select
            uk.advance_id,
            round(coalesce(eo.take_ex,0),2)::numeric(12,2) as expected_ex,
            round(coalesce(ao.taken_ex,0),2)::numeric(12,2) as actual_ex
          from union_keys uk
          left join expected_overpay eo
            on eo.advance_id = uk.advance_id
          left join actual_overpay ao
            on ao.advance_id = uk.advance_id
          where round(coalesce(eo.take_ex,0),2) <> round(coalesce(ao.taken_ex,0),2)
        )
        select
          od.advance_id,
          od.expected_ex,
          od.actual_ex
        from overpay_diffs od
      )
      union all
      (
        -- LOAN_REPAYMENT expected vs actual (this week)
        with
        actual_loans as (
          select
            replace(pbi.source_ref, 'advance:', '')::uuid as loan_id,
            round(sum(-coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0)),2)::numeric(12,2) as taken_ex
          from public.pay_batch_items pbi
          join public.pay_batch_candidates pbc
            on pbc.id = pbi.pay_batch_candidate_id
          where pbc.pay_batch_id = p_pay_batch_id
            and pbi.is_voided = false
            and pbi.item_type = 'LOAN_REPAYMENT'
            and pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
            and pbi.repayment_week_start = v_week_start
          group by replace(pbi.source_ref, 'advance:', '')::uuid
        ),
        cand_scope as (
          select
            pbc.id as pay_batch_candidate_id,
            pbc.candidate_id,
            coalesce(pbc.awaiting_net_amount,false) as awaiting_net_amount,
            coalesce(pbc.overpayment_recovery_taken, 0)::numeric(12,2) as overpayment_recovery_taken_ex,
            pni.net_amount as paye_net_amount
          from public.pay_batch_candidates pbc
          left join public.pay_batch_paye_net_inputs pni
            on pni.pay_batch_candidate_id = pbc.id
          where pbc.pay_batch_id = p_pay_batch_id
        ),
        cand_earnings as (
          select
            cs.pay_batch_candidate_id,
            cs.candidate_id,
            cs.awaiting_net_amount,
            cs.overpayment_recovery_taken_ex,
            greatest(coalesce(cs.paye_net_amount,0),0)::numeric(12,2) as earnings_before_loan_ex
          from cand_scope cs
        ),
        cand_limits as (
          select
            ce.pay_batch_candidate_id,
            ce.candidate_id,
            greatest(ce.earnings_before_loan_ex - ce.overpayment_recovery_taken_ex, 0)::numeric(12,2) as earnings_after_recovery_ex
          from cand_earnings ce
          where ce.awaiting_net_amount = false
        ),
        paid_wtd as (
          select
            pbc2.candidate_id,
            round(coalesce(sum(pbi2.amount_ex_vat), 0), 2)::numeric(12,2) as paid_wtd_before_ex
          from public.pay_batch_candidates pbc2
          join public.pay_batches pb2
            on pb2.id = pbc2.pay_batch_id
          join public.pay_batch_items pbi2
            on pbi2.pay_batch_candidate_id = pbc2.id
          where pb2.cancelled_at_utc is null
            and pb2.id <> p_pay_batch_id
            and coalesce(pb2.batch_kind_fixed, '') <> 'LOANS'
            and pb2.pay_date >= v_week_start
            and pb2.pay_date < (v_week_start + 7)
            and pbi2.is_voided = false
            and pbi2.item_type <> 'DEBT_CREATED'
          group by pbc2.candidate_id
        ),
        cand_with_floor as (
          select
            cl.pay_batch_candidate_id,
            cl.candidate_id,
            cl.earnings_after_recovery_ex,
            coalesce(pw.paid_wtd_before_ex, 0)::numeric(12,2) as paid_wtd_before_ex,
            coalesce(c.min_take_home_wtd, 0)::numeric(12,2) as floor_ex,
            round(
              greatest(
                least(
                  cl.earnings_after_recovery_ex,
                  (coalesce(pw.paid_wtd_before_ex, 0) + cl.earnings_after_recovery_ex) - coalesce(c.min_take_home_wtd, 0)
                ),
                0
              ),
              2
            )::numeric(12,2) as max_loan_repayment_ex
          from cand_limits cl
          join public.candidates c
            on c.id = cl.candidate_id
          left join paid_wtd pw
            on pw.candidate_id = cl.candidate_id
          where cl.earnings_after_recovery_ex > 0
        ),
        loans as (
          select
            pa.id as loan_id,
            pa.candidate_id,
            pa.outstanding_amount::numeric(12,2) as outstanding_amount,
            pa.weekly_due::numeric(12,2) as weekly_due,
            pa.start_week_start,
            pa.created_at
          from public.pay_advances pa
          where pa.advance_kind = 'LOAN'::public.pay_advance_kind_enum
            and pa.payout_status = 'PAID'::public.pay_advance_payout_status_enum
            and pa.status = 'ACTIVE'::public.pay_advance_status_enum
            and pa.outstanding_amount > 0
            and pa.weekly_due is not null
            and pa.weekly_due > 0
            and (pa.start_week_start is null or pa.start_week_start <= v_week_start)
        ),
        loan_repaid_wtd as (
          select
            pbc2.candidate_id,
            replace(pbi2.source_ref, 'advance:', '')::uuid as loan_id,
            round(coalesce(sum(-pbi2.amount_ex_vat), 0), 2)::numeric(12,2) as repaid_wtd_ex
          from public.pay_batch_items pbi2
          join public.pay_batch_candidates pbc2
            on pbc2.id = pbi2.pay_batch_candidate_id
          join public.pay_batches pb2
            on pb2.id = pbc2.pay_batch_id
          where pbi2.item_type = 'LOAN_REPAYMENT'
            and pbi2.is_voided = false
            and pbi2.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
            and pbi2.repayment_week_start = v_week_start
            and pb2.cancelled_at_utc is null
            and pb2.id <> p_pay_batch_id
            and coalesce(pb2.batch_kind_fixed, '') <> 'LOANS'
          group by
            pbc2.candidate_id,
            replace(pbi2.source_ref, 'advance:', '')::uuid
        ),
        loan_due as (
          select
            cwf.candidate_id,
            cwf.max_loan_repayment_ex,
            l.loan_id,
            l.outstanding_amount,
            l.weekly_due,
            l.start_week_start,
            l.created_at,
            least(l.weekly_due, l.outstanding_amount)::numeric(12,2) as due_this_week_ex,
            greatest(
              least(l.weekly_due, l.outstanding_amount) - coalesce(lrw.repaid_wtd_ex, 0),
              0
            )::numeric(12,2) as remaining_due_ex
          from cand_with_floor cwf
          join loans l
            on l.candidate_id = cwf.candidate_id
          left join loan_repaid_wtd lrw
            on lrw.candidate_id = cwf.candidate_id
           and lrw.loan_id = l.loan_id
          where cwf.max_loan_repayment_ex > 0
            and greatest(
              least(l.weekly_due, l.outstanding_amount) - coalesce(lrw.repaid_wtd_ex, 0),
              0
            ) > 0
        ),
        alloc_base as (
          select
            ld.candidate_id,
            ld.loan_id,
            ld.remaining_due_ex,
            ld.max_loan_repayment_ex,
            sum(ld.remaining_due_ex) over (
              partition by ld.candidate_id
              order by ld.start_week_start nulls first, ld.created_at, ld.loan_id
              rows between unbounded preceding and 1 preceding
            )::numeric(12,2) as cum_before_ex
          from loan_due ld
        ),
        expected_loans as (
          select
            ab.loan_id,
            round(
              least(
                ab.remaining_due_ex,
                greatest(ab.max_loan_repayment_ex - coalesce(ab.cum_before_ex, 0), 0)
              ),
              2
            )::numeric(12,2) as take_ex
          from alloc_base ab
          where round(
            least(
              ab.remaining_due_ex,
              greatest(ab.max_loan_repayment_ex - coalesce(ab.cum_before_ex, 0), 0)
            ),
            2
          ) > 0
        ),
        union_keys as (
          select el.loan_id from expected_loans el
          union
          select al.loan_id from actual_loans al
        ),
        loan_diffs as (
          select
            uk.loan_id,
            round(coalesce(el.take_ex,0),2)::numeric(12,2) as expected_ex,
            round(coalesce(al.taken_ex,0),2)::numeric(12,2) as actual_ex
          from union_keys uk
          left join expected_loans el
            on el.loan_id = uk.loan_id
          left join actual_loans al
            on al.loan_id = uk.loan_id
          where round(coalesce(el.take_ex,0),2) <> round(coalesce(al.taken_ex,0),2)
        )
        select
          ld.loan_id as advance_id,
          ld.expected_ex,
          ld.actual_ex
        from loan_diffs ld
      )
      limit v_diff_limit
    ) dd
    where v_scope in ('PAYE','UMBRELLA')

    -- PAYE net diffs: candidate key (candidate:<uuid>) expected net input vs implied batch net
    union all
    select
      null::text as timesheet_id,
      'PAYE_NET'::text as key_type,
      ('candidate:' || nd.candidate_id::text) as key_value,
      nd.expected_net::text as expected,
      nd.actual_net::text as actual,
      4 as ord
    from (
      with
      cand as (
        select
          pbc.id as pay_batch_candidate_id,
          pbc.candidate_id,
          coalesce(pbc.awaiting_net_amount,false) as awaiting_net_amount,
          round(coalesce(pbc.net_bank_amount,0),2)::numeric(12,2) as net_bank_amount_ex,
          round(coalesce(pbc.overpayment_recovery_taken,0),2)::numeric(12,2) as overpayment_recovery_taken_ex,
          round(coalesce(pbc.loan_repayment_taken,0),2)::numeric(12,2) as loan_repayment_taken_ex
        from public.pay_batch_candidates pbc
        where pbc.pay_batch_id = p_pay_batch_id
      ),
      net_inp as (
        select
          c.pay_batch_candidate_id,
          pni.net_amount::numeric(12,2) as net_amount
        from cand c
        left join public.pay_batch_paye_net_inputs pni
          on pni.pay_batch_candidate_id = c.pay_batch_candidate_id
      ),
      ded_present as (
        select
          c.pay_batch_candidate_id,
          exists(
            select 1
            from public.pay_batch_items pbi
            where pbi.pay_batch_candidate_id = c.pay_batch_candidate_id
              and pbi.is_voided = false
              and pbi.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT')
          ) as has_deductions
        from cand c
      )
      select
        c.candidate_id,
        ni.net_amount as expected_net,
        round(c.net_bank_amount_ex + c.overpayment_recovery_taken_ex + c.loan_repayment_taken_ex, 2)::numeric(12,2) as actual_net,
        c.awaiting_net_amount,
        dp.has_deductions
      from cand c
      left join net_inp ni
        on ni.pay_batch_candidate_id = c.pay_batch_candidate_id
      join ded_present dp
        on dp.pay_batch_candidate_id = c.pay_batch_candidate_id
      where v_scope = 'PAYE'
        and coalesce(v_batch_kind_fixed,'') <> 'LOANS'
        and (
              c.awaiting_net_amount <> (ni.net_amount is null)
           or ((ni.net_amount is null) and dp.has_deductions = true)
           or (ni.net_amount is not null
               and round(ni.net_amount,2) <> round(c.net_bank_amount_ex + c.overpayment_recovery_taken_ex + c.loan_repayment_taken_ex, 2))
        )
      limit v_diff_limit
    ) nd

    -- INFO counts
    union all
    select
      null::text as timesheet_id,
      'INFO'::text as key_type,
      'COUNTS'::text as key_value,
      to_jsonb(jsonb_build_object(
        'ts_changed', v_ts_changed_ct,
        'key_diffs', v_key_diff_ct,
        'ded_diffs', v_ded_diff_ct,
        'paye_net_diffs', v_paye_net_diff_ct
      ))::text as expected,
      null::text as actual,
      99 as ord
  ) d;

  -- Deduplicate reasons
  if array_length(v_reasons,1) is not null then
    select coalesce(array_agg(distinct r order by r), array[]::text[])
    into v_reasons
    from unnest(v_reasons) r;
  end if;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_VALIDATE_FRESHNESS',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'pay_date', v_pay_date::text,
        'week_start', v_week_start::text,
        'batch_kind_fixed', coalesce(v_batch_kind_fixed, null),
        'scope', coalesce(v_scope, null),
        'is_stale', v_is_stale,
        'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
        'counts', jsonb_build_object(
          'timesheet_changed', v_ts_changed_ct,
          'stable_key_diffs', v_key_diff_ct,
          'deduction_diffs', v_ded_diff_ct,
          'paye_net_diffs', v_paye_net_diff_ct
        )
      ),
      'pay_batches',
      p_pay_batch_id::text
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'is_stale', v_is_stale,
    'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
    'diff', coalesce(v_diffs, '[]'::jsonb)
  );
end;
$function$;


create or replace function public.pay_batch_export_csv_rows(
  p_pay_batch_id uuid,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now_utc timestamptz := now();

  v_batch record;
  v_batch_kind text := null;
  v_channels text[] := null;

  v_is_cancelled boolean := false;

  v_fresh jsonb := null;
  v_is_stale boolean := false;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;

  v_rows jsonb := '[]'::jsonb;
  v_row_count int := 0;
  v_kind_counts jsonb := '{}'::jsonb;
begin
  if p_pay_batch_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_EXPORT_CSV_ROWS',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'message', 'pay_batch_export_csv_rows: pay_batch_id is required'
    )::text;
  end if;

  if p_actor_user_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_EXPORT_CSV_ROWS',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_batch_export_csv_rows: actor_user_id is required',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  select
    pb.id,
    pb.status,
    pb.pay_date,
    pb.cancelled_at_utc,
    pb.batch_kind_fixed
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  limit 1;

  if v_batch.id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_EXPORT_CSV_ROWS',
      'code', 'PAY_BATCH_NOT_FOUND',
      'message', 'pay_batch_export_csv_rows: pay_batch not found',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  v_is_cancelled := (v_batch.cancelled_at_utc is not null);

  -- Derived kind from batch items (PAYE/UMBRELLA/MIXED); fixed kind can override display (e.g. LOANS).
  select ch.channels
  into v_channels
  from (
    select
      array_agg(distinct upper(coalesce(pbi.pay_channel,'')) order by upper(coalesce(pbi.pay_channel,'')))
        filter (where upper(coalesce(pbi.pay_channel,'')) in ('PAYE','UMBRELLA')) as channels
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.item_type <> 'DEBT_CREATED'
      and pbi.is_voided = false
  ) ch;

  v_batch_kind := case
    when v_channels is null then null
    when array_position(v_channels,'PAYE') is not null and array_position(v_channels,'UMBRELLA') is not null then 'MIXED'
    when array_position(v_channels,'PAYE') is not null then 'PAYE'
    when array_position(v_channels,'UMBRELLA') is not null then 'UMBRELLA'
    else null
  end;

  if upper(coalesce(v_batch.batch_kind_fixed,'')) = 'LOANS' then
    v_batch_kind := 'LOANS';
  end if;

  -- Freshness interaction:
  -- - Non-cancelled batches: block export if stale.
  -- - Cancelled batches: export must still work; include stale summary in metadata.
  v_fresh := public.pay_batch_validate_freshness(p_pay_batch_id, p_actor_user_id);
  v_is_stale := coalesce((v_fresh->>'is_stale')::boolean, false);
  v_stale_reasons := coalesce(v_fresh->'stale_reasons', '[]'::jsonb);

  select coalesce(jsonb_agg(x.elem), '[]'::jsonb)
  into v_diff_sample
  from (
    select elem
    from jsonb_array_elements(coalesce(v_fresh->'diff','[]'::jsonb)) as elem
    limit 50
  ) x;

  if v_is_cancelled = false and v_is_stale = true then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_EXPORT_CSV_ROWS',
      'code', 'BATCH_STALE',
      'message', 'pay_batch_export_csv_rows: batch is stale; regenerate draft before exporting',
      'pay_batch_id', p_pay_batch_id::text,
      'stale_reasons', v_stale_reasons,
      'diff', v_diff_sample
    )::text;
  end if;

  create temp table if not exists _tmp_pay_export_rows (
    sort_surname text not null,
    sort_work_date date null,
    sort_timesheet_id uuid null,
    sort_line_kind text not null,
    sort_bucket_code text null,
    sort_unit_name text null,
    line_kind text not null,
    row_json jsonb not null
  ) on commit drop;

  truncate table _tmp_pay_export_rows;

  insert into _tmp_pay_export_rows(
    sort_surname,
    sort_work_date,
    sort_timesheet_id,
    sort_line_kind,
    sort_bucket_code,
    sort_unit_name,
    line_kind,
    row_json
  )
  with
  net_latest as (
    select
      pbc.id as pay_batch_candidate_id,
      (
        select pni.net_amount
        from public.pay_batch_paye_net_inputs pni
        where pni.pay_batch_candidate_id = pbc.id
        order by pni.imported_at_utc desc
        limit 1
      ) as paye_net_amount
    from public.pay_batch_candidates pbc
    where pbc.pay_batch_id = p_pay_batch_id
  ),
  base as (
    select
      pbc.candidate_id as candidate_id,
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_tms_ref as candidate_tms_ref_snap,
      pbc.candidate_display_name as candidate_display_name_snap,

      c.first_name as cand_first_name,
      c.last_name as cand_last_name,
      c.tms_ref as cand_tms_ref_live,

      pbi.id as pay_batch_item_id,
      pbi.item_type as item_type,
      pbi.timesheet_id as timesheet_id,
      pbi.segment_key as segment_key,
      pbi.source_ref as source_ref,
      pbi.pay_channel as pay_channel,
      pbi.repayment_week_start as repayment_week_start,

      pbib.line_kind as line_kind,
      pbib.bucket_code as bucket_code,
      pbib.unit_name as unit_name,
      pbib.units as units,
      pbib.rate as rate,
      pbib.amount_ex_vat as amount_ex_vat,
      pbib.amount_vat as amount_vat,
      pbib.amount_inc_vat as amount_inc_vat,

      ts.week_ending_date as week_ending_date,
      ts.reference_number as reference_number,
      cl.id as client_id,
      cl.name as client_name,

      nl.paye_net_amount as paye_net_amount,

      segd.work_date as seg_work_date
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
     and pbc.pay_batch_id = p_pay_batch_id
    join public.pay_batch_item_breakdowns pbib
      on pbib.pay_batch_item_id = pbi.id
    left join public.candidates c
      on c.id = pbc.candidate_id
    left join public.timesheets ts
      on ts.timesheet_id = pbi.timesheet_id
     and ts.is_current = true
    left join public.contracts ct
      on ct.id = ts.contract_id
    left join public.clients cl
      on cl.id = ct.client_id
    left join net_latest nl
      on nl.pay_batch_candidate_id = pbc.id
    left join public.pay_batch_timesheet_snapshots pbts
      on pbts.pay_batch_id = p_pay_batch_id
     and pbts.timesheet_id = pbi.timesheet_id
     and pbts.candidate_id = pbc.candidate_id
     and upper(coalesce(pbts.pay_channel,'')) = upper(coalesce(pbi.pay_channel,''))
    left join lateral (
      select
        nullif(btrim(coalesce(seg->>'date','')),'')::date as work_date
      from jsonb_array_elements(coalesce(pbts.target_snapshot_json->'segments','[]'::jsonb)) seg
      where pbi.item_type = 'SEGMENT_DELTA'
        and seg is not null
        and jsonb_typeof(seg) = 'object'
        and nullif(btrim(coalesce(seg->>'segment_id','')),'') = coalesce(
          nullif(btrim(coalesce(pbi.segment_key,'')), ''),
          case
            when pbi.source_ref is not null and btrim(coalesce(pbi.source_ref,'')) like 'seg:%'
              then nullif(btrim(split_part(pbi.source_ref,':',2)),'')
            else null
          end
        )
        and nullif(btrim(coalesce(seg->>'date','')),'') ~ '^\d{4}-\d{2}-\d{2}$'
      limit 1
    ) segd on true
    where pbi.item_type <> 'DEBT_CREATED'
      and pbi.is_voided = false
  ),
  rows0 as (
    select
      coalesce(
        nullif(btrim(coalesce(b.cand_last_name,'')), ''),
        nullif(btrim(regexp_replace(coalesce(b.candidate_display_name_snap,''), '^.*\s', '')), ''),
        nullif(btrim(coalesce(b.candidate_display_name_snap,'')), ''),
        b.candidate_id::text
      ) as sort_surname,

      coalesce(b.seg_work_date, b.week_ending_date) as sort_work_date,

      b.timesheet_id as sort_timesheet_id,

      upper(coalesce(b.line_kind,'')) as sort_line_kind,
      upper(nullif(btrim(coalesce(b.bucket_code,'')),'') ) as sort_bucket_code,
      nullif(btrim(coalesce(b.unit_name,'')),'') as sort_unit_name,

      upper(coalesce(b.line_kind,'')) as line_kind,

      jsonb_build_object(
        'candidate_id', b.candidate_id::text,
        'candidate_tms_ref', coalesce(
          nullif(btrim(coalesce(b.candidate_tms_ref_snap,'')), ''),
          nullif(btrim(coalesce(b.cand_tms_ref_live,'')), ''),
          null
        ),
        'candidate_first_name', nullif(btrim(coalesce(b.cand_first_name,'')), ''),
        'candidate_last_name', nullif(btrim(coalesce(b.cand_last_name,'')), ''),
        'candidate_display_name', nullif(btrim(coalesce(b.candidate_display_name_snap,'')), ''),

        'client_id', case when b.client_id is null then null else b.client_id::text end,
        'client_name', b.client_name,

        'timesheet_id', case when b.timesheet_id is null then null else b.timesheet_id::text end,
        'week_ending_date', case when b.week_ending_date is null then null else b.week_ending_date::text end,
        'work_date', case
          when b.timesheet_id is null then null
          when b.seg_work_date is not null then b.seg_work_date::text
          else case when b.week_ending_date is null then null else b.week_ending_date::text end
        end,
        'reference_number', b.reference_number,

        'pay_channel', upper(coalesce(b.pay_channel,'')),
        'item_type', b.item_type,

        'line_kind', b.line_kind,
        'bucket_code', b.bucket_code,

        'unit_name', b.unit_name,
        'units', b.units,
        'rate', b.rate,

        'amount_ex_vat', b.amount_ex_vat,
        'amount_vat', b.amount_vat,
        'amount_inc_vat', b.amount_inc_vat,

        'is_overpayment_recovery', (b.item_type = 'OVERPAYMENT_RECOVERY'),
        'is_loan_repayment', (b.item_type = 'LOAN_REPAYMENT'),
        'deduction_amount_ex_vat', case
          when b.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT') then round(abs(coalesce(b.amount_ex_vat,0)),2)
          else null
        end,

        'paye_net_amount', case
          when upper(coalesce(b.pay_channel,'')) = 'PAYE' then b.paye_net_amount
          else null
        end,

        'source_ref', b.source_ref,
        'repayment_week_start', case when b.repayment_week_start is null then null else b.repayment_week_start::text end
      ) as row_json
    from base b
  )
  select
    r.sort_surname,
    r.sort_work_date,
    r.sort_timesheet_id,
    r.sort_line_kind,
    r.sort_bucket_code,
    r.sort_unit_name,
    r.line_kind,
    r.row_json
  from rows0 r;

  select count(*)::int
  into v_row_count
  from _tmp_pay_export_rows;

  select coalesce(
    jsonb_object_agg(t.lk, t.ct),
    '{}'::jsonb
  )
  into v_kind_counts
  from (
    select
      coalesce(nullif(btrim(coalesce(per.line_kind,'')),'') , 'UNKNOWN') as lk,
      count(*)::int as ct
    from _tmp_pay_export_rows per
    group by coalesce(nullif(btrim(coalesce(per.line_kind,'')),'') , 'UNKNOWN')
  ) t;

  select coalesce(
    jsonb_agg(
      per.row_json
      order by
        per.sort_surname asc,
        per.sort_work_date asc nulls last,
        per.sort_timesheet_id asc nulls last,
        per.sort_line_kind asc,
        per.sort_bucket_code asc nulls last,
        per.sort_unit_name asc nulls last
    ),
    '[]'::jsonb
  )
  into v_rows
  from _tmp_pay_export_rows per;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_EXPORT_CSV_ROWS',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'row_count', v_row_count,
        'line_kind_counts', v_kind_counts,
        'is_cancelled', v_is_cancelled,
        'is_stale', v_is_stale,
        'stale_reasons', v_stale_reasons
      ),
      'pay_batches',
      p_pay_batch_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'ok', true,
    'batch', jsonb_build_object(
      'id', v_batch.id::text,
      'status', v_batch.status,
      'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
      'batch_kind', v_batch_kind,
      'batch_kind_fixed', case when v_batch.batch_kind_fixed is null then null else v_batch.batch_kind_fixed end,
      'is_cancelled', v_is_cancelled,
      'generated_at_utc', v_now_utc::text
    ),
    'freshness', jsonb_build_object(
      'is_stale', v_is_stale,
      'stale_reasons', v_stale_reasons,
      'diff_sample', v_diff_sample
    ),
    'summary', jsonb_build_object(
      'row_count', v_row_count,
      'line_kind_counts', v_kind_counts
    ),
    'rows', v_rows
  );
end;
$$;
create or replace function public.pay_loans_grant(
  p_candidate_id uuid,
  p_principal_amount numeric,
  p_weekly_due numeric,
  p_weeks_total int,
  p_start_week_start date,
  p_actor_user_id uuid,
  p_note text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now_utc timestamptz := now();
  v_pay_date date := (now() at time zone 'Europe/London')::date;

  v_settings record;
  v_candidate record;

  v_schedule_json jsonb := '[]'::jsonb;
  v_next_due date := null;

  v_advance_id uuid := null;
  v_pay_batch_id uuid := null;
  v_pay_batch_candidate_id uuid := null;
  v_pay_batch_item_id uuid := null;

  v_warnings jsonb := '[]'::jsonb;

  v_provider text := null;
  v_env text := null;
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;

  v_bnc_status text := null;
  v_bnc_has_override boolean := false;
  v_bpm_present boolean := false;
begin
  if p_candidate_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'CANDIDATE_ID_REQUIRED',
      'message', 'pay_loans_grant: candidate_id is required'
    )::text;
  end if;

  if p_actor_user_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_loans_grant: actor_user_id is required'
    )::text;
  end if;

  if p_principal_amount is null or round(p_principal_amount,2) <= 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'PRINCIPAL_INVALID',
      'message', 'pay_loans_grant: principal_amount must be > 0'
    )::text;
  end if;

  if p_weekly_due is null or round(p_weekly_due,2) <= 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'WEEKLY_DUE_INVALID',
      'message', 'pay_loans_grant: weekly_due must be > 0'
    )::text;
  end if;

  if p_weeks_total is null or p_weeks_total < 1 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'WEEKS_TOTAL_INVALID',
      'message', 'pay_loans_grant: weeks_total must be >= 1'
    )::text;
  end if;

  if p_start_week_start is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'START_WEEK_START_REQUIRED',
      'message', 'pay_loans_grant: start_week_start is required'
    )::text;
  end if;

  if public._pay_week_start_monday(p_start_week_start) <> p_start_week_start then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'START_WEEK_START_NOT_MONDAY',
      'message', 'pay_loans_grant: start_week_start must be a Monday (week start)',
      'start_week_start', p_start_week_start::text
    )::text;
  end if;

  select
    sd.banking_system,
    sd.external_paye_system,
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_name_check
  into v_settings
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_settings.banking_system is null or v_settings.external_paye_system is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'SETTINGS_DEFAULTS_MISSING',
      'message', 'pay_loans_grant: settings_defaults missing required banking defaults (id=1)'
    )::text;
  end if;

  select
    c.id,
    c.active,
    c.tms_ref,
    c.display_name,
    c.first_name,
    c.last_name,
    c.account_holder,
    c.sort_code,
    c.account_number,
    c.bank_details_hash
  into v_candidate
  from public.candidates c
  where c.id = p_candidate_id
  limit 1;

  if v_candidate.id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'CANDIDATE_NOT_FOUND',
      'message', 'pay_loans_grant: candidate not found',
      'candidate_id', p_candidate_id::text
    )::text;
  end if;

  if coalesce(v_candidate.active,false) = false then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_LOANS_GRANT',
      'code', 'CANDIDATE_INACTIVE',
      'message', 'pay_loans_grant: candidate is not active',
      'candidate_id', p_candidate_id::text
    )::text;
  end if;

  v_provider := upper(btrim(coalesce(v_settings.rail_provider_default,'CSV')));
  v_env := upper(btrim(coalesce(v_settings.rail_env_default,'PROD')));

  v_need_name_check := (coalesce(v_settings.rail_supports_name_check,false) = true) and (v_provider <> 'CSV');
  v_requires_payee_map := (v_provider <> 'CSV');

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'week_start', (p_start_week_start + (gs.i * 7))::date,
          'amount', round(
            -least(
              round(p_weekly_due,2),
              greatest(round(p_principal_amount,2) - (round(p_weekly_due,2) * gs.i), 0)
            ),
            2
          )
        )
        order by (p_start_week_start + (gs.i * 7))::date asc
      ),
      '[]'::jsonb
    )
  into v_schedule_json
  from generate_series(0, greatest(p_weeks_total,1) - 1) as gs(i);

  select min(x.week_start)
  into v_next_due
  from (
    select
      (p_start_week_start + (gs2.i * 7))::date as week_start,
      round(
        -least(
          round(p_weekly_due,2),
          greatest(round(p_principal_amount,2) - (round(p_weekly_due,2) * gs2.i), 0)
        ),
        2
      )::numeric as amt
    from generate_series(0, greatest(p_weeks_total,1) - 1) as gs2(i)
  ) x
  where x.amt < 0;

  insert into public.pay_advances(
    candidate_id,
    client_id,
    reason,
    original_amount,
    outstanding_amount,
    linked_shift_date,
    schedule_json,
    next_due_week_start,
    status,
    best_guess_hours,
    notes,
    created_at,
    created_by,
    updated_at,
    advance_kind,
    linked_timesheet_id,
    baseline_signature,
    payout_status,
    payout_pay_batch_id,
    payout_transfer_id,
    weekly_due,
    weeks_total,
    start_week_start
  )
  values (
    p_candidate_id,
    null::uuid,
    'LOAN'::public.pay_advance_reason_enum,
    round(p_principal_amount,2),
    round(p_principal_amount,2),
    null::date,
    coalesce(v_schedule_json,'[]'::jsonb),
    v_next_due,
    'ACTIVE'::public.pay_advance_status_enum,
    null::jsonb,
    nullif(btrim(coalesce(p_note,'')), ''),
    v_now_utc,
    p_actor_user_id,
    v_now_utc,
    'LOAN'::public.pay_advance_kind_enum,
    null::uuid,
    null::text,
    'PENDING'::public.pay_advance_payout_status_enum,
    null::uuid,
    null::uuid,
    round(p_weekly_due,2),
    p_weeks_total,
    p_start_week_start
  )
  returning id into v_advance_id;

  insert into public.pay_batches(
    pay_date,
    created_at_utc,
    created_by_user_id,
    status,
    banking_system_snapshot,
    external_paye_system_snapshot,
    rail_provider_snapshot,
    rail_env_snapshot,
    batch_kind_fixed
  )
  values (
    v_pay_date,
    v_now_utc,
    p_actor_user_id,
    'DRAFT',
    v_settings.banking_system,
    v_settings.external_paye_system,
    v_settings.rail_provider_default,
    v_settings.rail_env_default,
    'LOANS'
  )
  returning id into v_pay_batch_id;

  insert into public.pay_batch_candidates(
    pay_batch_id,
    candidate_id,
    candidate_tms_ref,
    candidate_display_name,
    paye_state,
    mismatch_settlement_choice,
    gross_preview,
    net_bank_amount,
    debt_created,
    loan_repayment_taken,
    overpayment_recovery_taken,
    awaiting_net_amount,
    updated_at
  )
  values (
    v_pay_batch_id,
    p_candidate_id,
    v_candidate.tms_ref,
    v_candidate.display_name,
    null,
    null,
    round(p_principal_amount,2),
    round(p_principal_amount,2),
    0,
    0,
    0,
    false,
    v_now_utc
  )
  returning id into v_pay_batch_candidate_id;

  insert into public.pay_batch_items(
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    pay_channel,
    umbrella_id,
    bank_reference,
    pay_bank_transfer_id,
    repayment_week_start,
    is_voided,
    is_mismatch,
    created_at,
    updated_at
  )
  values (
    v_pay_batch_candidate_id,
    'LOAN_PAYOUT',
    null::uuid,
    null::text,
    ('advance:' || v_advance_id::text),
    'Loan payout',
    round(p_principal_amount,2),
    0,
    round(p_principal_amount,2),
    'PAYE',
    null::uuid,
    null::text,
    null::uuid,
    null::date,
    false,
    false,
    v_now_utc,
    v_now_utc
  )
  returning id into v_pay_batch_item_id;

  insert into public.pay_batch_item_breakdowns(
    pay_batch_item_id,
    line_kind,
    bucket_code,
    unit_name,
    units,
    rate,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    meta_json
  )
  values (
    v_pay_batch_item_id,
    'LOAN_PAYOUT',
    null,
    'Loan payout',
    null::numeric,
    null::numeric,
    round(p_principal_amount,2),
    0,
    round(p_principal_amount,2),
    '{}'::jsonb
  );

  if v_candidate.bank_details_hash is null or btrim(coalesce(v_candidate.bank_details_hash,'')) = '' then
    v_warnings := v_warnings || jsonb_build_array(
      jsonb_build_object(
        'code', 'BLOCKED_BANK_DETAILS',
        'message', 'Candidate bank details are missing; batch can be created but will be blocked at prepare/schedule until bank details are present.',
        'candidate_id', p_candidate_id::text
      )
    );
  else
    if v_need_name_check = true then
      select
        coalesce(bnc.status, 'UNVERIFIED') as status,
        (bnc.override_reason is not null and bnc.override_hash = v_candidate.bank_details_hash) as has_override
      into
        v_bnc_status,
        v_bnc_has_override
      from public.bank_name_checks bnc
      where bnc.rail_provider = v_settings.rail_provider_default
        and bnc.rail_env = v_settings.rail_env_default
        and bnc.entity_kind = 'CANDIDATE'
        and bnc.entity_id = p_candidate_id
        and bnc.bank_details_hash = v_candidate.bank_details_hash
      limit 1;

      if coalesce(v_bnc_status,'UNVERIFIED') <> 'PASS' and coalesce(v_bnc_has_override,false) = false then
        v_warnings := v_warnings || jsonb_build_array(
          jsonb_build_object(
            'code', 'BLOCKED_NAME_CHECK',
            'message', 'Name check has not passed (or override missing) for candidate bank details; scheduling/execution may be blocked until resolved.',
            'candidate_id', p_candidate_id::text,
            'rail_provider', v_settings.rail_provider_default,
            'rail_env', v_settings.rail_env_default
          )
        );
      end if;
    end if;

    if v_requires_payee_map = true then
      select (bpm.payee_id is not null) as present
      into v_bpm_present
      from public.bank_payee_map bpm
      where bpm.rail_provider = v_settings.rail_provider_default
        and bpm.rail_env = v_settings.rail_env_default
        and bpm.entity_kind = 'CANDIDATE'
        and bpm.entity_id = p_candidate_id
        and bpm.bank_details_hash = v_candidate.bank_details_hash
      limit 1;

      if coalesce(v_bpm_present,false) = false then
        v_warnings := v_warnings || jsonb_build_array(
          jsonb_build_object(
            'code', 'BLOCKED_NO_PAYEE_MAP',
            'message', 'Payee map is missing for candidate bank details on this rail; scheduling/execution may be blocked until payee mapping exists.',
            'candidate_id', p_candidate_id::text,
            'rail_provider', v_settings.rail_provider_default,
            'rail_env', v_settings.rail_env_default
          )
        );
      end if;
    end if;
  end if;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_LOANS_GRANT',
      jsonb_build_object(
        'candidate_id', p_candidate_id::text,
        'advance_id', v_advance_id::text,
        'pay_batch_id', v_pay_batch_id::text,
        'principal_amount', round(p_principal_amount,2),
        'weekly_due', round(p_weekly_due,2),
        'weeks_total', p_weeks_total,
        'start_week_start', p_start_week_start::text,
        'pay_date', v_pay_date::text,
        'rail_provider', v_settings.rail_provider_default,
        'rail_env', v_settings.rail_env_default,
        'warnings', v_warnings
      ),
      'pay_batches',
      v_pay_batch_id::text
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_pay_batch_id::text,
    'advance_id', v_advance_id::text,
    'pay_date', v_pay_date::text,
    'batch_kind_fixed', 'LOANS',
    'warnings', v_warnings
  );
end;
$$;
