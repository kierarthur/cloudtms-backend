create or replace function private._invoice_reference_rows_batch(
  p_invoice_ids uuid[]
) returns table(
  invoice_id uuid,
  timesheet_id uuid,
  sheet_scope text,
  submission_mode text,
  ref_target text,
  segment_id text,
  day_ymd text,
  start_utc text,
  end_utc text,
  current_reference text,
  is_required boolean,
  row_key text
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with requested as materialized (
  select distinct x.invoice_id
  from unnest(coalesce(p_invoice_ids,array[]::uuid[])) x(invoice_id)
  where x.invoice_id is not null
),
invoice_timesheets as materialized (
  select l.invoice_id,l.timesheet_id,
    sum(case when upper(coalesce(l.meta_json->>'line_type','')) in(
      'HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY')
      then coalesce(l.total_charge_ex_vat,0) else 0 end) worked_charge_ex,
    sum(case when upper(coalesce(l.meta_json->>'line_type','')) in(
      'HOURS_DAILY','HOURS_WEEKLY')
      then coalesce(l.hours_day,0)+coalesce(l.hours_night,0)+coalesce(l.hours_sat,0)
        +coalesce(l.hours_sun,0)+coalesce(l.hours_bh,0) else 0 end) worked_hours
  from requested r
  join public.invoice_lines l on l.invoice_id=r.invoice_id
  where l.timesheet_id is not null
  group by l.invoice_id,l.timesheet_id
  having sum(case when upper(coalesce(l.meta_json->>'line_type','')) in(
      'HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY')
      then coalesce(l.total_charge_ex_vat,0) else 0 end)>0
    or sum(case when upper(coalesce(l.meta_json->>'line_type','')) in(
      'HOURS_DAILY','HOURS_WEEKLY')
      then coalesce(l.hours_day,0)+coalesce(l.hours_night,0)+coalesce(l.hours_sat,0)
        +coalesce(l.hours_sun,0)+coalesce(l.hours_bh,0) else 0 end)>0
),
base as materialized (
  select it.invoice_id,t.timesheet_id,t.sheet_scope::text sheet_scope,
    t.submission_mode::text submission_mode,t.reference_number,t.week_ending_date,
    t.worked_start_iso,t.worked_end_iso,t.scheduled_start_iso,t.scheduled_end_iso,
    t.actual_schedule_json,t.day_references_json,f.invoice_breakdown_json,
    coalesce(pc.require_reference_to_invoice,false)
      or coalesce(pc.reference_number_required_to_issue_invoice,false) is_required
  from invoice_timesheets it
  join public.timesheets t on t.timesheet_id=it.timesheet_id and t.is_current
  left join public.timesheets_financials f
    on f.timesheet_id=t.timesheet_id and f.is_current
  left join public.v_ts_invoice_precheck pc on pc.timesheet_id=t.timesheet_id
),
segment_rows as materialized (
  select b.invoice_id,b.timesheet_id,b.sheet_scope,b.submission_mode,'SEGMENT'::text ref_target,
    coalesce(nullif(btrim(s.value->>'segment_id'),''),
      case when nullif(btrim(s.value->>'start_utc'),'') is not null
         and nullif(btrim(s.value->>'end_utc'),'') is not null
        then 'SE:'||(s.value->>'start_utc')||'|'||(s.value->>'end_utc') end) segment_id,
    coalesce(nullif(btrim(s.value->>'date'),''),
      case when coalesce(s.value->>'start_utc','')~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
        then left(s.value->>'start_utc',10) end)
      day_ymd,
    nullif(btrim(s.value->>'start_utc'),'') start_utc,
    nullif(btrim(s.value->>'end_utc'),'') end_utc,
    nullif(btrim(s.value->>'ref_num'),'') current_reference,b.is_required
  from base b
  cross join lateral jsonb_array_elements(
    case when jsonb_typeof(b.invoice_breakdown_json->'segments')='array'
      then b.invoice_breakdown_json->'segments' else '[]'::jsonb end) s(value)
  where upper(coalesce(b.invoice_breakdown_json->>'mode',''))='SEGMENTS'
    and nullif(btrim(s.value->>'invoice_locked_invoice_id'),'')=b.invoice_id::text
    and(
      case when coalesce(s.value->>'charge_amount','')~
        '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
        then (s.value->>'charge_amount')::numeric else 0 end>0
      or
      (select sum(case when coalesce(v,'')~
          '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
          then v::numeric else 0 end)
       from (values(s.value->>'hours_day'),(s.value->>'hours_night'),
         (s.value->>'hours_sat'),(s.value->>'hours_sun'),
         (s.value->>'hours_bh')) h(v))>0
    )
),
manual_rows as materialized (
  select b.invoice_id,b.timesheet_id,b.sheet_scope,b.submission_mode,'SEGMENT'::text ref_target,
    'ts:'||b.timesheet_id::text||':'||(s.ordinality-1)::text segment_id,
    nullif(btrim(s.value->>'date'),'') day_ymd,
    nullif(btrim(s.value->>'start_utc'),'') start_utc,
    nullif(btrim(s.value->>'end_utc'),'') end_utc,
    nullif(btrim(s.value->>'ref_num'),'') current_reference,b.is_required
  from base b
  cross join lateral jsonb_array_elements(
    case when jsonb_typeof(b.actual_schedule_json)='array'
      then b.actual_schedule_json else '[]'::jsonb end)
    with ordinality s(value,ordinality)
  where upper(coalesce(b.invoice_breakdown_json->>'mode',''))<>'SEGMENTS'
    and b.sheet_scope='WEEKLY' and b.submission_mode='MANUAL'
    and nullif(btrim(s.value->>'start'),'') is not null
    and nullif(btrim(s.value->>'end'),'') is not null
),
weekly_rows as materialized (
  select b.invoice_id,b.timesheet_id,b.sheet_scope,b.submission_mode,'FREEFORM'::text ref_target,
    null::text segment_id,nullif(btrim(j.key),'') day_ymd,null::text start_utc,
    null::text end_utc,nullif(btrim(j.value),'') current_reference,b.is_required
  from base b
  cross join lateral jsonb_each_text(
    case when jsonb_typeof(b.day_references_json)='object'
      then b.day_references_json else '{}'::jsonb end) j(key,value)
  where upper(coalesce(b.invoice_breakdown_json->>'mode',''))<>'SEGMENTS'
    and b.sheet_scope='WEEKLY' and b.submission_mode<>'MANUAL'
    and left(coalesce(j.key,''),2)<>'__'
),
freeform_rows as materialized (
  select b.invoice_id,b.timesheet_id,b.sheet_scope,b.submission_mode,
    'FREEFORM'::text ref_target,
    'freeform:'||(f.ordinality-1)::text segment_id,
    null::text day_ymd,null::text start_utc,null::text end_utc,
    nullif(btrim(case
      when jsonb_typeof(f.value)='string' then f.value#>>'{}'
      when jsonb_typeof(f.value)='object' then coalesce(
        f.value->>'reference',f.value->>'ref_num',f.value->>'value')
    end),'') current_reference,b.is_required
  from base b
  cross join lateral jsonb_array_elements(
    case
      when jsonb_typeof(b.day_references_json)='array'
        then b.day_references_json
      when jsonb_typeof(b.day_references_json)='object'
        and jsonb_typeof(b.day_references_json->'__freeform_refs')='array'
        then b.day_references_json->'__freeform_refs'
      when jsonb_typeof(b.day_references_json)='object'
        and jsonb_typeof(b.day_references_json->'__freeform')='array'
        then b.day_references_json->'__freeform'
      when jsonb_typeof(b.day_references_json)='object'
        and jsonb_typeof(b.day_references_json->'__freeform_lines')='array'
        then b.day_references_json->'__freeform_lines'
      else '[]'::jsonb
    end
  ) with ordinality f(value,ordinality)
  where upper(coalesce(b.invoice_breakdown_json->>'mode',''))<>'SEGMENTS'
    and b.sheet_scope='WEEKLY' and b.submission_mode<>'MANUAL'
    and nullif(btrim(case
      when jsonb_typeof(f.value)='string' then f.value#>>'{}'
      when jsonb_typeof(f.value)='object' then coalesce(
        f.value->>'reference',f.value->>'ref_num',f.value->>'value')
    end),'') is not null
),
fallback_rows as materialized (
  select b.invoice_id,b.timesheet_id,b.sheet_scope,b.submission_mode,'TIMESHEET'::text ref_target,
    null::text segment_id,
    coalesce(
      (b.worked_start_iso at time zone 'Europe/London')::date::text,
      (b.scheduled_start_iso at time zone 'Europe/London')::date::text,
      b.week_ending_date::text) day_ymd,
    coalesce(to_jsonb(b.worked_start_iso)#>>'{}',
      to_jsonb(b.scheduled_start_iso)#>>'{}') start_utc,
    coalesce(to_jsonb(b.worked_end_iso)#>>'{}',
      to_jsonb(b.scheduled_end_iso)#>>'{}') end_utc,
    nullif(btrim(b.reference_number),'') current_reference,b.is_required
  from base b
  where upper(coalesce(b.invoice_breakdown_json->>'mode',''))<>'SEGMENTS'
    and not(b.sheet_scope='WEEKLY' and b.submission_mode='MANUAL'
      and jsonb_typeof(b.actual_schedule_json)='array')
    and not(b.sheet_scope='WEEKLY' and b.submission_mode<>'MANUAL'
      and jsonb_typeof(b.day_references_json) in('object','array'))
),
all_rows as (
  select * from segment_rows
  union all select * from manual_rows
  union all select * from weekly_rows
  union all select * from freeform_rows
  union all select * from fallback_rows
)
select a.invoice_id,a.timesheet_id,a.sheet_scope,a.submission_mode,a.ref_target,
  a.segment_id,a.day_ymd,a.start_utc,a.end_utc,a.current_reference,a.is_required,
  a.timesheet_id::text||'|'||coalesce(a.ref_target,'')||'|'||
    coalesce(a.segment_id,'')||'|'||coalesce(a.day_ymd,'')||'|'||
    coalesce(a.start_utc,'')||'|'||coalesce(a.end_utc,'') row_key
from all_rows a
order by a.invoice_id,a.timesheet_id,a.day_ymd nulls last,
  a.start_utc nulls last,a.segment_id nulls last;
$function$;

revoke all on function private._invoice_reference_rows_batch(uuid[])
  from public,anon,authenticated;
grant execute on function private._invoice_reference_rows_batch(uuid[])
  to service_role;
