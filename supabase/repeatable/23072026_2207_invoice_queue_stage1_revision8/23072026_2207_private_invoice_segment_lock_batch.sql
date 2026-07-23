create or replace function private._invoice_segment_lock_batch(
  p_targets jsonb,
  p_now_utc timestamptz
) returns table(
  invoice_id uuid,
  timesheet_id uuid,
  success boolean,
  locked_segment_count integer,
  blocker_code text,
  detail_json jsonb
)
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
begin
  return query
  with raw as materialized (
    select x.ordinality,
      case when coalesce(x.value->>'invoice_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (x.value->>'invoice_id')::uuid end invoice_id,
      case when coalesce(x.value->>'timesheet_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (x.value->>'timesheet_id')::uuid end timesheet_id,
      nullif(x.value->>'expected_financial_revision','') expected_revision,
      array(select distinct btrim(s.value)
        from jsonb_array_elements_text(
          case when jsonb_typeof(x.value->'segment_ids')='array'
            then x.value->'segment_ids' else '[]'::jsonb end) s(value)
        where nullif(btrim(s.value),'') is not null) segment_ids
    from jsonb_array_elements(
      case when jsonb_typeof(p_targets)='array' then p_targets else '[]'::jsonb end)
      with ordinality x(value,ordinality)
  ),
  targets as materialized (
    select distinct on (r.invoice_id,r.timesheet_id) r.*,
      f.id financial_id,f.invoice_breakdown_json,
      encode(digest(jsonb_build_object(
        'financial_id',f.id,'timesheet_version',f.timesheet_version,
        'updated_at',f.updated_at,'basis',f.basis,
        'invoice_breakdown_json',f.invoice_breakdown_json)::text,'sha256'),'hex')
        current_revision
    from raw r
    left join public.timesheets_financials f
      on f.timesheet_id=r.timesheet_id and f.is_current
    where r.invoice_id is not null and r.timesheet_id is not null
    order by r.invoice_id,r.timesheet_id,r.ordinality
  ),
  segments as materialized (
    select t.*,s.ordinality segment_ordinality,
      s.value segment,
      coalesce(nullif(btrim(s.value->>'segment_id'),''),
        'SE:'||coalesce(s.value->>'start_utc','')||'|'||
          coalesce(s.value->>'end_utc','')) segment_key
    from targets t
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(t.invoice_breakdown_json->'segments')='array'
        then t.invoice_breakdown_json->'segments' else '[]'::jsonb end)
      with ordinality s(value,ordinality)
  ),
  validation as materialized (
    select t.invoice_id,t.timesheet_id,t.financial_id,t.invoice_breakdown_json,
      t.current_revision,t.expected_revision,t.segment_ids,
      count(s.segment_key) filter(where s.segment_key=any(t.segment_ids)) selected_count,
      array_agg(s.segment_key order by s.segment_ordinality)
        filter(where s.segment_key=any(t.segment_ids)
          and nullif(btrim(s.segment->>'invoice_locked_invoice_id'),'') is not null
          and s.segment->>'invoice_locked_invoice_id'<>t.invoice_id::text)
        conflicting_keys
    from targets t
    left join segments s on s.invoice_id=t.invoice_id
      and s.timesheet_id=t.timesheet_id
    group by t.invoice_id,t.timesheet_id,t.financial_id,t.invoice_breakdown_json,
      t.current_revision,t.expected_revision,t.segment_ids
  ),
  classified as materialized (
    select v.*,
      case
        when v.financial_id is null then 'CURRENT_FINANCIAL_NOT_FOUND'
        when cardinality(v.segment_ids)=0 then 'SEGMENT_IDS_REQUIRED'
        when v.expected_revision is not null
          and v.expected_revision<>v.current_revision then 'SOURCE_REVISION_CHANGED'
        when v.selected_count<>cardinality(v.segment_ids) then 'SEGMENT_NOT_FOUND'
        when cardinality(coalesce(v.conflicting_keys,array[]::text[]))>0
          then 'SEGMENT_ALREADY_LOCKED'
      end blocker
    from validation v
  ),
  rebuilt as materialized (
    select c.invoice_id,c.timesheet_id,c.financial_id,
      jsonb_set(c.invoice_breakdown_json,'{segments}',
        jsonb_agg(
          case when s.segment_key=any(c.segment_ids)
            then s.segment||jsonb_build_object(
              'invoice_locked_invoice_id',c.invoice_id::text,
              'invoice_locked_at_utc',
                coalesce(p_now_utc,now())::text)
            else s.segment end order by s.segment_ordinality),true) new_breakdown,
      cardinality(c.segment_ids) locked_count
    from classified c
    join segments s on s.invoice_id=c.invoice_id
      and s.timesheet_id=c.timesheet_id
    where c.blocker is null
    group by c.invoice_id,c.timesheet_id,c.financial_id,
      c.invoice_breakdown_json,c.segment_ids
  ),
  changed as materialized (
    update public.timesheets_financials f
    set invoice_breakdown_json=r.new_breakdown,
      locked_by_invoice_id=case
        when not exists(
          select 1
          from jsonb_array_elements(r.new_breakdown->'segments') s(value)
          where nullif(btrim(s.value->>'invoice_locked_invoice_id'),'') is null)
        then r.invoice_id else f.locked_by_invoice_id end,
      locked_at_utc=case
        when f.locked_at_utc is null then coalesce(p_now_utc,now())
        else f.locked_at_utc end,
      updated_at=coalesce(p_now_utc,now())
    from rebuilt r
    where f.id=r.financial_id
    returning r.invoice_id,r.timesheet_id,r.locked_count
  )
  select c.invoice_id,c.timesheet_id,(c.blocker is null and ch.invoice_id is not null),
    coalesce(ch.locked_count,0)::integer,c.blocker,
    jsonb_build_object(
      'expected_segment_ids',to_jsonb(c.segment_ids),
      'selected_segment_count',c.selected_count,
      'conflicting_segment_ids',
        to_jsonb(coalesce(c.conflicting_keys,array[]::text[])),
      'expected_financial_revision',c.expected_revision,
      'current_financial_revision',c.current_revision)
  from classified c
  left join changed ch on ch.invoice_id=c.invoice_id
    and ch.timesheet_id=c.timesheet_id;
end;
$function$;

revoke all on function private._invoice_segment_lock_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function private._invoice_segment_lock_batch(jsonb,timestamptz)
  to service_role;
