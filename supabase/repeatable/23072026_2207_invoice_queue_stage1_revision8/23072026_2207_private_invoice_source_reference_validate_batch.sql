drop function if exists private._invoice_source_reference_validate_batch(jsonb);

create function private._invoice_source_reference_validate_batch(
  p_source_members jsonb
) returns table(
  source_member_key text,
  source_type text,
  source_id uuid,
  timesheet_id uuid,
  segment_id text,
  reference_ready boolean,
  blocker_code text,
  reference_source text,
  reference_hash text,
  current_revision text,
  detail_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with supplied as materialized (
  select e.ordinality::integer member_no,e.value member,
    upper(btrim(coalesce(e.value->>'source_type','TIMESHEET'))) source_type,
    case when coalesce(e.value->>'source_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(e.value->>'source_id')::uuid end source_id,
    case when coalesce(e.value->>'related_timesheet_id',
        e.value->>'timesheet_id',e.value->>'source_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then coalesce(e.value->>'related_timesheet_id',
        e.value->>'timesheet_id',e.value->>'source_id')::uuid end timesheet_id,
    nullif(btrim(coalesce(e.value->>'segment_id','')),'') segment_id,
    coalesce(nullif(btrim(e.value->>'source_member_key'),''),
      encode(digest(concat_ws('|',
        upper(btrim(coalesce(e.value->>'source_type','TIMESHEET'))),
        coalesce(e.value->>'related_timesheet_id',
          e.value->>'timesheet_id',e.value->>'source_id',''),
        coalesce(nullif(btrim(e.value->>'segment_id'),''),'WHOLE'),
        coalesce(e.value->>'target_invoice_week','')),'sha256'),'hex'))
      source_member_key,
    nullif(btrim(coalesce(e.value->>'row_revision',
      e.value->>'source_revision','')),'') expected_revision,
    nullif(btrim(e.value->>'target_invoice_week'),'')
      target_invoice_week,
    upper(coalesce(nullif(btrim(e.value->>'invoice_stream'),''),
      'NORMAL')) invoice_stream,
    upper(coalesce(nullif(btrim(e.value->>'consolidation_mode'),''),
      'NONE')) consolidation_mode
  from jsonb_array_elements(case
    when jsonb_typeof(p_source_members)='array' then p_source_members
    else '[]'::jsonb end) with ordinality e(value,ordinality)
  where jsonb_typeof(e.value)='object'
),
authoritative as materialized (
  select s.*,t.sheet_scope::text sheet_scope,
    t.submission_mode::text submission_mode,t.reference_number,
    t.day_references_json,t.actual_schedule_json,t.version,
    t.updated_at,t.is_current,t.revoked_at,
    f.id financial_id,f.updated_at financial_updated_at,
    f.timesheet_version financial_timesheet_version,f.is_current financial_is_current,
    f.invoice_breakdown_json,
    seg.segment_json,
    encode(digest(concat_ws('|',
      t.timesheet_id::text,f.id::text,f.timesheet_version::text,
      f.updated_at::text,t.version::text,t.updated_at::text,
      coalesce(f.invoice_breakdown_json::text,''),
      coalesce(s.segment_id,'WHOLE'),
      coalesce(seg.segment_json::text,''),
      coalesce(seg.segment_json->>'segment_id',''),
      coalesce(seg.segment_json->>'invoice_target_week_start',''),
      coalesce(seg.segment_json->>'invoice_locked_invoice_id',''),
      coalesce(seg.segment_json->>'ref_num',''),
      coalesce(seg.segment_json->>'charge_amount',''),
      coalesce(seg.segment_json->>'pay_amount',''),
      coalesce(s.target_invoice_week,''),
      s.invoice_stream,s.consolidation_mode),'sha256'),'hex')
      current_revision
  from supplied s
  left join public.timesheets t on t.timesheet_id=s.timesheet_id
  left join public.timesheets_financials f
    on f.timesheet_id=s.timesheet_id and f.is_current
  left join lateral (
    select x.value segment_json
    from jsonb_array_elements(case
      when jsonb_typeof(f.invoice_breakdown_json->'segments')='array'
        then f.invoice_breakdown_json->'segments'
      else '[]'::jsonb end) x(value)
    where s.segment_id is not null
      and x.value->>'segment_id'=s.segment_id
    order by x.value->>'segment_id'
    limit 1
  ) seg on true
),
schedule_stats as materialized (
  select a.member_no,
    count(*) filter(where jsonb_typeof(x.value)='object'
      and nullif(btrim(x.value->>'start'),'') is not null
      and nullif(btrim(x.value->>'end'),'') is not null) scheduled_count,
    count(*) filter(where jsonb_typeof(x.value)='object'
      and nullif(btrim(x.value->>'start'),'') is not null
      and nullif(btrim(x.value->>'end'),'') is not null
      and nullif(btrim(x.value->>'ref_num'),'') is not null) referenced_count,
    coalesce(jsonb_agg(jsonb_build_object(
      'start',x.value->>'start','end',x.value->>'end',
      'reference',nullif(btrim(x.value->>'ref_num'),''))
      order by x.ordinality) filter(where jsonb_typeof(x.value)='object'
        and nullif(btrim(x.value->>'start'),'') is not null
        and nullif(btrim(x.value->>'end'),'') is not null),'[]'::jsonb)
      schedule_references
  from authoritative a
  left join lateral jsonb_array_elements(case
    when jsonb_typeof(a.actual_schedule_json)='array' then a.actual_schedule_json
    else '[]'::jsonb end) with ordinality x(value,ordinality) on true
  group by a.member_no
),
day_stats as materialized (
  select a.member_no,
    count(*) filter(where left(coalesce(d.key,''),2)<>'__'
      and nullif(btrim(d.value),'') is not null) referenced_count,
    coalesce(jsonb_object_agg(d.key,d.value order by d.key)
      filter(where left(coalesce(d.key,''),2)<>'__'
        and nullif(btrim(d.value),'') is not null),'{}'::jsonb)
      day_references
  from authoritative a
  left join lateral jsonb_each_text(case
    when jsonb_typeof(a.day_references_json)='object' then a.day_references_json
    else '{}'::jsonb end) d(key,value) on true
  group by a.member_no
),
freeform_stats as materialized (
  select a.member_no,
    count(*) filter(where nullif(btrim(case
      when jsonb_typeof(f.value)='string' then f.value#>>'{}'
      when jsonb_typeof(f.value)='object' then coalesce(
        f.value->>'reference',f.value->>'ref_num',f.value->>'value')
      end),'') is not null) referenced_count,
    coalesce(jsonb_agg(nullif(btrim(case
      when jsonb_typeof(f.value)='string' then f.value#>>'{}'
      when jsonb_typeof(f.value)='object' then coalesce(
        f.value->>'reference',f.value->>'ref_num',f.value->>'value')
      end),'') order by f.ordinality)
      filter(where nullif(btrim(case
        when jsonb_typeof(f.value)='string' then f.value#>>'{}'
        when jsonb_typeof(f.value)='object' then coalesce(
          f.value->>'reference',f.value->>'ref_num',f.value->>'value')
        end),'') is not null),'[]'::jsonb) freeform_references
  from authoritative a
  left join lateral jsonb_array_elements(
    case
      when jsonb_typeof(a.day_references_json)='array'
        then a.day_references_json
      when jsonb_typeof(a.day_references_json)='object'
        and jsonb_typeof(a.day_references_json->'__freeform_refs')='array'
        then a.day_references_json->'__freeform_refs'
      when jsonb_typeof(a.day_references_json)='object'
        and jsonb_typeof(a.day_references_json->'__freeform')='array'
        then a.day_references_json->'__freeform'
      when jsonb_typeof(a.day_references_json)='object'
        and jsonb_typeof(a.day_references_json->'__freeform_lines')='array'
        then a.day_references_json->'__freeform_lines'
      else '[]'::jsonb
    end
  ) with ordinality f(value,ordinality) on true
  group by a.member_no
),
segment_stats as materialized (
  select a.member_no,
    count(s.value)::integer segment_count,
    max(nullif(btrim(s.value->>'ref_num'),'')) segment_reference,
    coalesce(jsonb_agg(jsonb_build_object(
      'segment_id',coalesce(nullif(btrim(s.value->>'segment_id'),''),
        case when nullif(btrim(s.value->>'start_utc'),'') is not null
          and nullif(btrim(s.value->>'end_utc'),'') is not null
          then 'SE:'||(s.value->>'start_utc')||'|'||
            (s.value->>'end_utc') end),
      'reference',nullif(btrim(s.value->>'ref_num'),''),
      'target_invoice_week',s.value->>'invoice_target_week_start')
      order by s.ordinality)
      filter(where s.value is not null),'[]'::jsonb) segment_references
  from authoritative a
  left join lateral jsonb_array_elements(
    case when jsonb_typeof(a.invoice_breakdown_json->'segments')='array'
      then a.invoice_breakdown_json->'segments' else '[]'::jsonb end)
    with ordinality s(value,ordinality)
    on coalesce(nullif(btrim(s.value->>'segment_id'),''),
      case when nullif(btrim(s.value->>'start_utc'),'') is not null
        and nullif(btrim(s.value->>'end_utc'),'') is not null
        then 'SE:'||(s.value->>'start_utc')||'|'||
          (s.value->>'end_utc') end)=a.segment_id
  group by a.member_no
),
evaluated as materialized (
  select a.*,coalesce(ss.scheduled_count,0) scheduled_count,
    coalesce(ss.referenced_count,0) schedule_reference_count,
    coalesce(ss.schedule_references,'[]'::jsonb) schedule_references,
    coalesce(ds.referenced_count,0) day_reference_count,
    coalesce(ds.day_references,'{}'::jsonb) day_references,
    coalesce(fs.referenced_count,0) freeform_reference_count,
    coalesce(fs.freeform_references,'[]'::jsonb) freeform_references,
    coalesce(sg.segment_count,0) segment_count,
    sg.segment_reference,
    coalesce(sg.segment_references,'[]'::jsonb) segment_references,
    case
      when a.timesheet_id is null or a.sheet_scope is null then false
      when a.segment_id is not null
        then coalesce(sg.segment_count,0)=1
          and nullif(btrim(coalesce(sg.segment_reference,'')),'') is not null
      when upper(coalesce(a.sheet_scope,''))='WEEKLY'
        and upper(coalesce(a.submission_mode,''))='MANUAL'
        then coalesce(ss.scheduled_count,0)>0
          and ss.scheduled_count=ss.referenced_count
      when upper(coalesce(a.sheet_scope,''))='WEEKLY'
        then nullif(btrim(coalesce(a.reference_number,'')),'') is not null
          or coalesce(ds.referenced_count,0)>0
          or coalesce(fs.referenced_count,0)>0
      else nullif(btrim(coalesce(a.reference_number,'')),'') is not null
    end ready,
    case
      when a.segment_id is not null then 'FINANCIAL_SEGMENT_REFERENCE'
      when upper(coalesce(a.sheet_scope,''))='WEEKLY'
        and upper(coalesce(a.submission_mode,''))='MANUAL'
        then 'WEEKLY_MANUAL_SCHEDULE'
      when upper(coalesce(a.sheet_scope,''))='WEEKLY'
        and nullif(btrim(coalesce(a.reference_number,'')),'') is not null
        then 'WEEKLY_REFERENCE'
      when upper(coalesce(a.sheet_scope,''))='WEEKLY'
        and coalesce(ds.referenced_count,0)>0 then 'DAY_REFERENCES'
      when upper(coalesce(a.sheet_scope,''))='WEEKLY'
        and coalesce(fs.referenced_count,0)>0 then 'FREEFORM_REFERENCES'
      else 'TIMESHEET_REFERENCE'
    end resolved_reference_source
  from authoritative a
  left join schedule_stats ss using(member_no)
  left join day_stats ds using(member_no)
  left join freeform_stats fs using(member_no)
  left join segment_stats sg using(member_no)
)
select e.source_member_key,e.source_type,e.source_id,e.timesheet_id,e.segment_id,
  e.ready
    and e.is_current is true and e.revoked_at is null
    and e.financial_id is not null and e.financial_is_current is true
    and(e.expected_revision is null or e.expected_revision=e.current_revision),
  case
    when e.source_id is null or e.timesheet_id is null then 'SOURCE_ID_INVALID'
    when e.sheet_scope is null then 'SOURCE_NOT_FOUND'
    when e.is_current is not true or e.revoked_at is not null
      then 'TIMESHEET_NOT_CURRENT'
    when e.financial_id is null or e.financial_is_current is not true
      then 'CURRENT_FINANCIALS_MISSING'
    when e.expected_revision is not null
      and e.expected_revision<>e.current_revision then 'SOURCE_REVISION_CHANGED'
    when e.segment_id is not null and e.segment_count<>1
      then 'SOURCE_SEGMENT_NOT_FOUND'
    when not e.ready then 'INVOICE_REFERENCE_REQUIRED'
  end blocker_code,
  e.resolved_reference_source,
  encode(digest(jsonb_build_object(
    'timesheet_id',e.timesheet_id,'segment_id',e.segment_id,
    'sheet_scope',e.sheet_scope,'submission_mode',e.submission_mode,
    'reference_number',nullif(btrim(coalesce(e.reference_number,'')),''),
    'segment_reference',e.segment_reference,
    'day_references',e.day_references,
    'freeform_references',e.freeform_references,
    'schedule_references',e.schedule_references)::text,'sha256'),'hex'),
  e.current_revision,
  jsonb_build_object(
    'source_member_key',e.source_member_key,
    'sheet_scope',e.sheet_scope,'submission_mode',e.submission_mode,
    'whole_reference_present',
      nullif(btrim(coalesce(e.reference_number,'')),'') is not null,
    'segment_reference',e.segment_reference,
    'segment_match_count',e.segment_count,
    'day_reference_count',e.day_reference_count,
    'freeform_reference_count',e.freeform_reference_count,
    'scheduled_segment_count',e.scheduled_count,
    'scheduled_reference_count',e.schedule_reference_count,
    'current_revision',e.current_revision,
    'expected_revision',e.expected_revision)
from evaluated e
order by e.member_no;
$function$;

revoke all on function private._invoice_source_reference_validate_batch(jsonb)
  from public,anon,authenticated;
grant execute on function private._invoice_source_reference_validate_batch(jsonb)
  to service_role;
