\set ON_ERROR_STOP on

select exists (
  select 1
  from public.invoice_operation_chunks
  where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT')
) as invoice_presentation_active_work
\gset

\if :invoice_presentation_active_work
do $deferred$
begin
  raise notice 'INVOICE_PRESENTATION_SNAPSHOT_DEFERRED_ACTIVE_WORK';
end;
$deferred$;
\else

create or replace function private._invoice_presentation_snapshot_batch(
    p_requests jsonb,
    p_now_utc timestamptz default null
)
returns table (
    request_key text,
    snapshot_schema_version text,
    presentation_model jsonb,
    timesheet_sources jsonb,
    supporting_sources jsonb,
    higher_rate_support jsonb,
    snapshot_json jsonb,
    snapshot_hash text,
    valid boolean,
    error_code text,
    error_detail jsonb
)
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
#variable_conflict use_column
declare
  v_now timestamptz := coalesce(p_now_utc, now());
begin
  if p_requests is null or jsonb_typeof(p_requests) is distinct from 'array' then
    raise exception using errcode='22023',
      message='p_requests must be a JSON array containing 1..100 requests';
  end if;

  if jsonb_array_length(p_requests) < 1
     or jsonb_array_length(p_requests) > 100 then
    raise exception using errcode='22023',
      message='p_requests must be a JSON array containing 1..100 requests';
  end if;

  return query
  with recursive raw_requests as materialized (
    select x.ordinality::integer request_no,
           x.value raw_request,
           nullif(btrim(coalesce(x.value->>'request_key','')),'') req_key,
           upper(nullif(btrim(coalesce(x.value->>'entity_type','')),'')) entity_type,
           case when coalesce(x.value->>'entity_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             then (x.value->>'entity_id')::uuid end entity_id,
           upper(nullif(btrim(coalesce(x.value->>'purpose','')),'')) purpose,
           coalesce(nullif(btrim(x.value->>'template_version'),''),'invoice-professional-v1') template_version,
           case when pg_input_is_valid(nullif(x.value->>'issue_at_utc',''),'timestamptz') then (x.value->>'issue_at_utc')::timestamptz end issue_at_utc,
           case when pg_input_is_valid(nullif(x.value->>'tax_point_utc',''),'timestamptz') then (x.value->>'tax_point_utc')::timestamptz end tax_point_utc,
           case when pg_input_is_valid(nullif(x.value->>'due_at_utc',''),'timestamptz') then (x.value->>'due_at_utc')::timestamptz end due_at_utc
    from jsonb_array_elements(p_requests) with ordinality x(value,ordinality)
  ),
  key_stats as materialized (
    select req_key,count(*)::integer key_count,min(request_no) first_request_no
    from raw_requests
    where req_key is not null
    group by req_key
  ),
  duplicate_key_results as materialized (
    select r.request_no,
           r.req_key request_key,
           'INVALID_PRESENTATION_REQUEST'::text snapshot_schema_version,
           '{}'::jsonb presentation_model,
           '[]'::jsonb timesheet_sources,
           '[]'::jsonb supporting_sources,
           jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1','rows','[]'::jsonb) higher_rate_support,
           jsonb_build_object('snapshot_schema_version','INVALID_PRESENTATION_REQUEST','request_no',r.request_no,'request_key',r.req_key) snapshot_json,
           encode(digest(jsonb_build_object('snapshot_schema_version','INVALID_PRESENTATION_REQUEST','request_no',r.request_no,'request_key',r.req_key)::text,'sha256'),'hex') snapshot_hash,
           false valid,
           'DUPLICATE_REQUEST_KEY'::text error_code,
           jsonb_build_object('request_no',r.request_no,'request_key',r.req_key,'duplicate_count',ks.key_count,'first_request_no',ks.first_request_no) error_detail
    from raw_requests r
    join key_stats ks on ks.req_key=r.req_key
    where ks.key_count > 1
  ),
  processable as materialized (
    select r.*
    from raw_requests r
    left join key_stats ks on ks.req_key=r.req_key
    where coalesce(ks.key_count,0) <= 1
  ),
  invalid_request_results as materialized (
    select p.request_no,
           coalesce(p.req_key,'__request_'||p.request_no::text) request_key,
           'INVALID_PRESENTATION_REQUEST'::text snapshot_schema_version,
           '{}'::jsonb presentation_model,
           '[]'::jsonb timesheet_sources,
           '[]'::jsonb supporting_sources,
           jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1','rows','[]'::jsonb) higher_rate_support,
           jsonb_build_object('snapshot_schema_version','INVALID_PRESENTATION_REQUEST','request_no',p.request_no,'request_key',p.req_key) snapshot_json,
           encode(digest(jsonb_build_object('snapshot_schema_version','INVALID_PRESENTATION_REQUEST','request_no',p.request_no,'request_key',p.req_key)::text,'sha256'),'hex') snapshot_hash,
           false valid,
           case
             when p.req_key is null then 'REQUEST_KEY_REQUIRED'
             when length(p.req_key) > 256 then 'REQUEST_KEY_TOO_LONG'
             when p.entity_type not in('INVOICE','TIMESHEET') then 'ENTITY_TYPE_UNSUPPORTED'
             when p.entity_id is null then 'ENTITY_ID_INVALID'
             when p.purpose not in('DRAFT_PREVIEW','FINAL_ISSUE','TIMESHEET') then 'PURPOSE_UNSUPPORTED'
             when p.purpose='FINAL_ISSUE' and (p.issue_at_utc is null or p.tax_point_utc is null or p.due_at_utc is null) then 'FINAL_ISSUE_DATES_REQUIRED'
             else 'PRESENTATION_REQUEST_INVALID' end error_code,
           jsonb_build_object('request_no',p.request_no,'entity_type',p.entity_type,'purpose',p.purpose) error_detail
    from processable p
    where p.req_key is null
       or length(p.req_key) > 256
       or p.entity_type not in('INVOICE','TIMESHEET')
       or p.entity_id is null
       or p.purpose not in('DRAFT_PREVIEW','FINAL_ISSUE','TIMESHEET')
       or (p.purpose='FINAL_ISSUE' and (p.issue_at_utc is null or p.tax_point_utc is null or p.due_at_utc is null))
  ),
  valid_requests as materialized (
    select p.*
    from processable p
    where p.req_key is not null and length(p.req_key) <= 256
      and p.entity_type in('INVOICE','TIMESHEET')
      and p.entity_id is not null
      and p.purpose in('DRAFT_PREVIEW','FINAL_ISSUE','TIMESHEET')
      and not (p.purpose='FINAL_ISSUE' and (p.issue_at_utc is null or p.tax_point_utc is null or p.due_at_utc is null))
  ),
  requested_invoices as materialized (
    select entity_id invoice_id from valid_requests where entity_type='INVOICE'
  ),
  reference_scope as materialized (
    select r.*
    from private._invoice_reference_rows_batch(
      coalesce((select array_agg(distinct invoice_id) from requested_invoices), array[]::uuid[])
    ) r
  ),
  invoice_line_base as materialized (
    select il.*,
           coalesce(nullif(btrim(vs.candidate_name),''),nullif(btrim(t.occupant_key_norm),'')) worker_name,
           row_number() over(partition by il.invoice_id order by il.created_at,il.id)::integer base_display_order,
           case when pg_input_is_valid(nullif(il.meta_json->>'quantity',''),'numeric') then nullif(il.meta_json->>'quantity','')::numeric end meta_quantity,
           case when pg_input_is_valid(nullif(il.meta_json->>'units',''),'numeric') then nullif(il.meta_json->>'units','')::numeric end meta_units,
           case when il.timesheet_id is null
             then coalesce(nullif(btrim(il.meta_json->>'reference'),''),
               nullif(btrim(il.meta_json->>'po_number'),''))
             else refs.reference end canonical_reference,
           case when il.timesheet_id is null then 'CUSTOM_LINE'
             else coalesce(refs.reference_source,'NONE') end reference_source,
           case when il.timesheet_id is null then 'CUSTOM_LINE'
             else coalesce(refs.reference_scope,'TIMESHEET') end reference_scope,
           coalesce(refs.reference_source_row_keys,'[]'::jsonb)
             reference_source_row_keys,
           coalesce(refs.reference_required,false) reference_required
    from public.invoice_lines il
    left join public.timesheets t on t.timesheet_id=il.timesheet_id and t.is_current
    left join public.v_timesheets_summary_base vs on vs.timesheet_id=il.timesheet_id
    left join lateral (
      select
        string_agg(x.current_reference,', ' order by x.first_row_key) reference,
        case when count(*)=1 then min(x.reference_source)
          else 'AGGREGATED_CANONICAL' end reference_source,
        case when count(*)=1 then min(x.reference_scope)
          else 'AGGREGATED_TIMESHEET' end reference_scope,
        jsonb_agg(x.row_key order by x.first_row_key) reference_source_row_keys,
        bool_or(x.is_required) reference_required
      from (
        select r.current_reference,min(r.row_key) first_row_key,
          min(r.row_key) row_key,
          min(case
            when r.ref_target='SEGMENT' then 'SEGMENT'
            when r.day_ymd is not null then 'DAY'
            else 'TIMESHEET' end) reference_source,
          min(case
            when r.ref_target='SEGMENT' then 'EXACT_SEGMENT'
            when r.day_ymd is not null then 'EXACT_DAY'
            else 'WHOLE_TIMESHEET' end) reference_scope,
          bool_or(r.is_required) is_required
        from reference_scope r
        where r.invoice_id=il.invoice_id and r.timesheet_id=il.timesheet_id
          and nullif(btrim(r.current_reference),'') is not null
        group by r.current_reference
      ) x
    ) refs on true
    where il.invoice_id in(select invoice_id from requested_invoices)
  ),
  invoice_line_component_raw as materialized (
    select b.invoice_id,b.id,b.created_at,
           (b.base_display_order*100+v.display_suffix)::integer base_display_order,
           coalesce(nullif(b.source_key,''),b.id::text)||':'||v.bucket row_key,
           v.label description,
           b.canonical_reference reference,b.reference_required,b.reference_scope,
           b.reference_source,b.reference_source_row_keys,
           'hours'::text unit,
           round(coalesce(v.hours,0),4) quantity_raw,
           round(coalesce(v.hours,0)*coalesce(v.rate,0),4) net_raw,
           round(coalesce(b.total_charge_ex_vat,0),2) line_net,
           round(coalesce(b.vat_amount,0),2) line_vat,
           coalesce(b.vat_rate_pct,0) vat_rate,
           b.source_key,b.id source_invoice_line_id,b.worker_name
    from invoice_line_base b
    cross join lateral (values
      ('DAY','Day',1,coalesce(b.hours_day,0),coalesce(b.charge_day,0)),
      ('NIGHT','Night',2,coalesce(b.hours_night,0),coalesce(b.charge_night,0)),
      ('SATURDAY','Saturday',3,coalesce(b.hours_sat,0),coalesce(b.charge_sat,0)),
      ('SUNDAY','Sunday',4,coalesce(b.hours_sun,0),coalesce(b.charge_sun,0)),
      ('BANK_HOLIDAY','Bank holiday',5,coalesce(b.hours_bh,0),coalesce(b.charge_bh,0))
    ) v(bucket,label,display_suffix,hours,rate)
    where coalesce(v.hours,0)<>0
    union all
    select b.invoice_id,b.id,b.created_at,(b.base_display_order*100+90)::integer,
           coalesce(nullif(b.source_key,''),b.id::text)||':'||lower(coalesce(nullif(b.meta_json->>'line_type',''),'residual')),
           case
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%MILEAGE%' then 'Mileage'
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%TRAVEL%' then 'Travel'
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%ACCOMMODATION%' then 'Accommodation'
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%HIGHER%' then 'Higher rate'
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%ADDITIONAL%' then 'Additional rate'
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%REVERS%' then 'Reversal'
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%CREDIT%' then 'Credit'
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%ADJUST%' then 'Adjustment'
             when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%EXPENSE%' then 'Other expenses'
             else coalesce(nullif(b.description,''),'Invoice line') end,
           b.canonical_reference,b.reference_required,b.reference_scope,
           b.reference_source,b.reference_source_row_keys,
           case when upper(coalesce(b.meta_json->>'line_type',b.source_key,'')) like '%MILEAGE%' then 'miles' else 'item' end,
           round(coalesce(b.meta_quantity,b.meta_units,1),4),
           round(coalesce(b.total_charge_ex_vat,0) - (
             coalesce(b.hours_day,0)*coalesce(b.charge_day,0)
             + coalesce(b.hours_night,0)*coalesce(b.charge_night,0)
             + coalesce(b.hours_sat,0)*coalesce(b.charge_sat,0)
             + coalesce(b.hours_sun,0)*coalesce(b.charge_sun,0)
             + coalesce(b.hours_bh,0)*coalesce(b.charge_bh,0)
           ),4),
           round(coalesce(b.total_charge_ex_vat,0),2),round(coalesce(b.vat_amount,0),2),coalesce(b.vat_rate_pct,0),b.source_key,b.id,b.worker_name
    from invoice_line_base b
    where abs(coalesce(b.total_charge_ex_vat,0) - (
      coalesce(b.hours_day,0)*coalesce(b.charge_day,0)
      + coalesce(b.hours_night,0)*coalesce(b.charge_night,0)
      + coalesce(b.hours_sat,0)*coalesce(b.charge_sat,0)
      + coalesce(b.hours_sun,0)*coalesce(b.charge_sun,0)
      + coalesce(b.hours_bh,0)*coalesce(b.charge_bh,0)
    )) > 0.004
       or not exists (
         select 1 from (values
           (coalesce(b.hours_day,0),coalesce(b.charge_day,0)),
           (coalesce(b.hours_night,0),coalesce(b.charge_night,0)),
           (coalesce(b.hours_sat,0),coalesce(b.charge_sat,0)),
           (coalesce(b.hours_sun,0),coalesce(b.charge_sun,0)),
           (coalesce(b.hours_bh,0),coalesce(b.charge_bh,0))
         ) nonzero(h,a) where coalesce(h,0)<>0 or coalesce(a,0)<>0)
  ),
  invoice_line_component_ranked as materialized (
    select r.*,
      row_number() over(partition by r.id order by r.base_display_order,r.row_key)::integer component_no,
      count(*) over(partition by r.id)::integer component_count,
      case when coalesce(r.line_net,0)<>0 then r.line_vat * r.net_raw / r.line_net
           else r.line_vat / nullif(count(*) over(partition by r.id),0) end raw_vat
    from invoice_line_component_raw r
  ),
  invoice_line_components as materialized (
    select q.invoice_id,q.id,q.created_at,q.base_display_order,q.row_key,q.description,
           q.reference,q.reference_required,q.reference_scope,q.reference_source,
           q.reference_source_row_keys,q.unit,
           q.quantity_raw quantity,
           case when q.quantity_raw<>0 then round(q.net_amount/q.quantity_raw,4) else q.net_amount end unit_price,
           q.net_amount,q.vat_rate,q.vat_amount,round(q.net_amount+q.vat_amount,2) gross_amount,
           q.source_key,q.source_invoice_line_id,q.worker_name
    from (
      select r.*,
        case when r.component_no=r.component_count
          then round(r.line_net - coalesce(sum(round(r.net_raw,2)) over(partition by r.id order by r.component_no rows between unbounded preceding and 1 preceding),0),2)
          else round(r.net_raw,2) end net_amount,
        case when r.component_no=r.component_count
          then round(r.line_vat - coalesce(sum(round(coalesce(r.raw_vat,0),2)) over(partition by r.id order by r.component_no rows between unbounded preceding and 1 preceding),0),2)
          else round(coalesce(r.raw_vat,0),2) end vat_amount
      from invoice_line_component_ranked r
    ) q
  ),
  invoice_line_rows as materialized (
    select c.invoice_id,
           jsonb_agg(jsonb_build_object(
             'row_key',c.row_key,
             'source_invoice_line_id',c.source_invoice_line_id,
             'source_key',coalesce(c.source_key,c.source_invoice_line_id::text),
             'description',c.description,
             'worker',c.worker_name,
             'reference',c.reference,
             'reference_required',c.reference_required,
             'reference_scope',c.reference_scope,
             'reference_source',c.reference_source,
             'reference_source_row_keys',c.reference_source_row_keys,
             'unit',c.unit,
             'quantity',c.quantity::text,
             'unit_price',c.unit_price::text,
             'net_amount',c.net_amount::text,
             'vat_rate',c.vat_rate::text,
             'vat_amount',c.vat_amount::text,
             'gross_amount',c.gross_amount::text,
             'display_order',c.base_display_order
           ) order by c.created_at,c.id,c.base_display_order) lines,
           round(sum(c.net_amount),2) line_net,
           round(sum(c.vat_amount),2) line_vat,
           round(sum(c.gross_amount),2) line_gross
    from invoice_line_components c
    group by c.invoice_id
  ),
  timesheet_targets as materialized (
    select distinct il.invoice_id,t.timesheet_id
    from requested_invoices ri
    join public.invoice_lines il on il.invoice_id=ri.invoice_id
      and il.timesheet_id is not null
    join public.timesheets t on t.timesheet_id=il.timesheet_id and t.is_current
    union
    select null::uuid,vr.entity_id
    from valid_requests vr
    where vr.entity_type='TIMESHEET'
  ),
  timesheet_base as materialized (
    select target.invoice_id,t.timesheet_id,t.document_revision,
      upper(coalesce(t.sheet_scope::text,'')) sheet_scope,
      upper(coalesce(t.submission_mode::text,'')) submission_mode,
      t.contract_id,t.week_ending_date,
      t.worked_start_iso,t.worked_end_iso,t.break_start_iso,t.break_end_iso,
      coalesce(t.break_minutes,0) break_minutes,t.actual_schedule_json,
      t.day_references_json,t.reference_number,t.additional_units_week,
      t.additional_units_per_day,t.authorised_at_server,t.auth_name,
      t.auth_job_title,t.r2_nurse_key,t.r2_auth_key,t.img_sha256_nurse,
      t.img_sha256_auth,t.qr_status::text qr_status,t.qr_token,
      t.qr_payload_json,t.qr_signed_hash,t.qr_signed_at_utc,
      coalesce(vs.candidate_id,tf.candidate_id,ct.candidate_id) candidate_id,
      coalesce(vs.client_id,tf.client_id,ct.client_id) client_id,
      coalesce(nullif(btrim(cand.first_name),''),
        split_part(coalesce(vs.candidate_name,t.occupant_key_norm,''),' ',1))
        candidate_first_name,
      coalesce(nullif(btrim(cand.last_name),''),
        nullif(btrim(regexp_replace(
          coalesce(vs.candidate_name,t.occupant_key_norm,''),
          '^[^ ]+[ ]*','','g')),'')) candidate_surname,
      coalesce(nullif(btrim(tf.role),''),nullif(btrim(t.job_title_norm),''),
        nullif(btrim(ct.role),'')) job_profile_title,
      coalesce(nullif(btrim(cl.name),''),nullif(btrim(vs.client_name),''),
        nullif(btrim(t.hospital_norm),'')) client_name,
      coalesce(nullif(btrim(t.hospital_norm),''),
        nullif(btrim(cl.name),''),nullif(btrim(vs.client_name),'')) hospital,
      concat_ws(' / ',nullif(btrim(t.hospital_norm),''),
        nullif(btrim(t.ward_norm),'')) site_ward,
      coalesce(nullif(btrim(tf.band),''),nullif(btrim(t.band),'')) band,
      coalesce(tf.invoice_breakdown_json,'{}'::jsonb) invoice_breakdown_json,
      coalesce(tf.additional_units_json,
        tf.invoice_breakdown_json#>'{additional,units}',
        '{}'::jsonb) additional_units_json,
      coalesce(ct.week_ending_weekday_snapshot,cs.week_ending_weekday,0)
        configured_week_ending_weekday,
      coalesce(pc.require_reference_to_invoice,false)
        or coalesce(pc.reference_number_required_to_issue_invoice,false)
        reference_required,
      sd.agency_name,
      case
        when nullif(coalesce(
          sd.invoice_document_presentation_json#>>'{branding,logo,r2_key}',
          sd.import_config_json#>>'{invoice_document_presentation,branding,logo,r2_key}'),'') is null
          then '{}'::jsonb
        else jsonb_build_object(
          'r2_key',coalesce(
            sd.invoice_document_presentation_json#>>'{branding,logo,r2_key}',
            sd.import_config_json#>>'{invoice_document_presentation,branding,logo,r2_key}'),
          'sha256',lower(coalesce(
            sd.invoice_document_presentation_json#>>'{branding,logo,sha256}',
            sd.import_config_json#>>'{invoice_document_presentation,branding,logo,sha256}')),
          'size_bytes',case when coalesce(
            sd.invoice_document_presentation_json#>>'{branding,logo,size_bytes}',
            sd.import_config_json#>>'{invoice_document_presentation,branding,logo,size_bytes}',
            '')~'^[1-9][0-9]{0,18}$' then coalesce(
              sd.invoice_document_presentation_json#>>'{branding,logo,size_bytes}',
              sd.import_config_json#>>'{invoice_document_presentation,branding,logo,size_bytes}')::bigint end,
          'media_type',lower(coalesce(
            sd.invoice_document_presentation_json#>>'{branding,logo,media_type}',
            sd.import_config_json#>>'{invoice_document_presentation,branding,logo,media_type}')))
      end logo_identity,
      coalesce(sd.timesheet_header_json,'{}'::jsonb) header_wording,
      coalesce(sd.timesheet_footer_json,'{}'::jsonb) footer_wording,
      coalesce(sd.temporary_worker_declaration_json,'{}'::jsonb)
        temporary_worker_declaration,
      coalesce(sd.client_declaration_json,'{}'::jsonb) client_declaration
    from timesheet_targets target
    join public.timesheets t on t.timesheet_id=target.timesheet_id and t.is_current
    left join public.v_timesheets_summary_base vs on vs.timesheet_id=t.timesheet_id
    left join public.contracts ct on ct.id=t.contract_id
    left join public.candidates cand on cand.id=coalesce(vs.candidate_id,ct.candidate_id)
    left join public.clients cl on cl.id=coalesce(vs.client_id,ct.client_id)
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=t.timesheet_id
    left join lateral (
      select tf0.*
      from public.timesheets_financials tf0
      where tf0.timesheet_id=t.timesheet_id and tf0.is_current
      order by tf0.computed_at_utc desc nulls last,tf0.created_at desc nulls last,
        tf0.updated_at desc nulls last,tf0.id desc
      limit 1
    ) tf on true
    left join lateral (
      select cs0.week_ending_weekday
      from public.client_settings cs0
      where cs0.client_id=coalesce(vs.client_id,tf.client_id,ct.client_id)
        and cs0.effective_from<=coalesce(
          (t.worked_start_iso at time zone 'Europe/London')::date,
          t.week_ending_date,v_now::date)
      order by cs0.effective_from desc
      limit 1
    ) cs on true
    cross join public.settings_defaults sd
    where sd.id=1
  ),
  timesheet_number_fnv(
    invoice_id,timesheet_id,source_text,character_no,fnv_hash
  ) as (
    select b.invoice_id,b.timesheet_id,b.timesheet_id::text,1::integer,
      2166136261::bigint
    from timesheet_base b
    union all
    select h.invoice_id,h.timesheet_id,h.source_text,h.character_no+1,
      mod(
        ((h.fnv_hash # ascii(substr(
          h.source_text,h.character_no,1))::bigint)*16777619::bigint),
        4294967296::bigint)
    from timesheet_number_fnv h
    where h.character_no<=length(h.source_text)
  ),
  timesheet_week_context as materialized (
    select b.*,
      coalesce(b.week_ending_date,
        (b.worked_start_iso at time zone 'Europe/London')::date+
          ((b.configured_week_ending_weekday-
            extract(dow from (b.worked_start_iso at time zone
              'Europe/London')::date)::integer+7)%7)) resolved_week_ending_date
    from timesheet_base b
  ),
  timesheet_schedule_source as materialized (
    select b.invoice_id,b.timesheet_id,s.ordinality::integer source_order,s.value row_json
    from timesheet_week_context b
    cross join lateral jsonb_array_elements(
      case
        when upper(coalesce(b.invoice_breakdown_json->>'mode',''))='SEGMENTS'
          and jsonb_typeof(b.invoice_breakdown_json->'segments')='array'
          then b.invoice_breakdown_json->'segments'
        when jsonb_typeof(b.actual_schedule_json)='array'
          then b.actual_schedule_json
        else '[]'::jsonb
      end) with ordinality s(value,ordinality)
    union all
    select b.invoice_id,b.timesheet_id,1,jsonb_build_object(
      'date',(b.worked_start_iso at time zone 'Europe/London')::date,
      'start_utc',b.worked_start_iso,'end_utc',b.worked_end_iso,
      'break_start',case when b.break_start_iso is not null
        then to_char(b.break_start_iso at time zone 'Europe/London','HH24:MI') end,
      'break_end',case when b.break_end_iso is not null
        then to_char(b.break_end_iso at time zone 'Europe/London','HH24:MI') end,
      'break_minutes',b.break_minutes,'ref_num',b.reference_number)
    from timesheet_week_context b
    where b.sheet_scope='DAILY'
      and not exists(
        select 1 from jsonb_array_elements(
          case
            when upper(coalesce(b.invoice_breakdown_json->>'mode',''))='SEGMENTS'
              and jsonb_typeof(b.invoice_breakdown_json->'segments')='array'
              then b.invoice_breakdown_json->'segments'
            when jsonb_typeof(b.actual_schedule_json)='array'
              then b.actual_schedule_json
            else '[]'::jsonb
          end) existing)
  ),
  timesheet_schedule_normalised as materialized (
    select s.*,b.resolved_week_ending_date,b.reference_required,
      coalesce(nullif(btrim(s.row_json->>'segment_id'),''),
        case when nullif(btrim(s.row_json->>'start_utc'),'') is not null
          and nullif(btrim(s.row_json->>'end_utc'),'') is not null
          then 'SE:'||(s.row_json->>'start_utc')||'|'||
            (s.row_json->>'end_utc') end,
        'ts:'||s.timesheet_id::text||':'||(s.source_order-1)::text) segment_id,
      coalesce(nullif(btrim(s.row_json->>'date'),''),
        nullif(btrim(s.row_json->>'day'),''),
        case when pg_input_is_valid(nullif(coalesce(
            s.row_json->>'worked_start',s.row_json->>'start_utc'),''),
            'timestamptz')
          then (coalesce(s.row_json->>'worked_start',
            s.row_json->>'start_utc')::timestamptz at time zone
              'Europe/London')::date::text end,
        case when b.sheet_scope='DAILY'
          then (b.worked_start_iso at time zone 'Europe/London')::date::text end)
        work_date,
      coalesce(nullif(btrim(s.row_json->>'worked_start'),''),
        nullif(btrim(s.row_json->>'start_utc'),''),
        nullif(btrim(s.row_json->>'start'),'')) worked_start,
      coalesce(nullif(btrim(s.row_json->>'worked_end'),''),
        nullif(btrim(s.row_json->>'end_utc'),''),
        nullif(btrim(s.row_json->>'end'),'')) worked_end,
      case
        when nullif(btrim(s.row_json->>'break_start'),'') is not null
          and nullif(btrim(s.row_json->>'break_end'),'') is not null
          then 'EXPLICIT_INTERVAL'
        when coalesce(s.row_json->>'break_minutes',
            s.row_json->>'break_mins','')~'^[0-9]{1,5}$'
          and coalesce(s.row_json->>'break_minutes',
            s.row_json->>'break_mins')::integer>0
          then 'MINUTES_ONLY'
        else 'NONE' end break_display_mode,
      case when coalesce(s.row_json->>'break_minutes',
          s.row_json->>'break_mins','')~'^[0-9]{1,5}$'
        then coalesce(s.row_json->>'break_minutes',
          s.row_json->>'break_mins')::integer else 0 end break_minutes,
      coalesce(nullif(btrim(s.row_json->>'ref_num'),''),
        nullif(btrim(s.row_json->>'reference'),'')) source_reference
    from timesheet_schedule_source s
    join timesheet_week_context b
      on b.invoice_id is not distinct from s.invoice_id
     and b.timesheet_id=s.timesheet_id
  ),
  timesheet_schedule_rows as materialized (
    select s.invoice_id,s.timesheet_id,s.source_order,s.segment_id,s.work_date,
      jsonb_build_object(
        'row_key',s.timesheet_id::text||'|SEGMENT|'||s.segment_id||'|'||
          coalesce(s.work_date,''),
        'display_order',row_number() over(
          partition by s.invoice_id,s.timesheet_id,s.work_date
          order by
            case when pg_input_is_valid(nullif(s.worked_start,''),'timestamptz')
              then s.worked_start::timestamptz end nulls last,
            s.worked_start,s.segment_id,s.source_order),
        'segment_id',s.segment_id,
        'date',s.work_date,
        'worked_start_utc',case when pg_input_is_valid(
          nullif(s.worked_start,''),'timestamptz')
          then s.worked_start::timestamptz end,
        'worked_end_utc',case when pg_input_is_valid(
          nullif(s.worked_end,''),'timestamptz')
          then s.worked_end::timestamptz end,
        'display_start_local',case
          when pg_input_is_valid(nullif(s.worked_start,''),'timestamptz')
            then to_char(s.worked_start::timestamptz at time zone
              'Europe/London','HH24:MI')
          when s.worked_start~'^[0-2][0-9]:[0-5][0-9]' then left(s.worked_start,5)
          end,
        'display_end_local',case
          when pg_input_is_valid(nullif(s.worked_end,''),'timestamptz')
            then to_char(s.worked_end::timestamptz at time zone
              'Europe/London','HH24:MI')
          when s.worked_end~'^[0-2][0-9]:[0-5][0-9]' then left(s.worked_end,5)
          end,
        'break_start_local',nullif(btrim(s.row_json->>'break_start'),''),
        'break_end_local',nullif(btrim(s.row_json->>'break_end'),''),
        'break_minutes',s.break_minutes,
        'break_display_mode',s.break_display_mode,
        'bucket_hours',jsonb_build_object(
          'DAY',coalesce(s.row_json->'hours_day','0'::jsonb),
          'NIGHT',coalesce(s.row_json->'hours_night','0'::jsonb),
          'SATURDAY',coalesce(s.row_json->'hours_sat','0'::jsonb),
          'SUNDAY',coalesce(s.row_json->'hours_sun','0'::jsonb),
          'BANK_HOLIDAY',coalesce(s.row_json->'hours_bh','0'::jsonb)),
        'paid_minutes',coalesce(
          case when (select sum(case when coalesce(v,'')~
              '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
              then v::numeric else 0 end)
            from(values(s.row_json->>'hours_day'),
              (s.row_json->>'hours_night'),(s.row_json->>'hours_sat'),
              (s.row_json->>'hours_sun'),(s.row_json->>'hours_bh')) h(v))>0
            then round(60*(select sum(case when coalesce(v,'')~
              '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
              then v::numeric else 0 end)
              from(values(s.row_json->>'hours_day'),
                (s.row_json->>'hours_night'),(s.row_json->>'hours_sat'),
                (s.row_json->>'hours_sun'),(s.row_json->>'hours_bh')) h(v)))::integer
          end,
          case when pg_input_is_valid(nullif(s.worked_start,''),'timestamptz')
              and pg_input_is_valid(nullif(s.worked_end,''),'timestamptz')
            then greatest(0,round(extract(epoch from(
              s.worked_end::timestamptz-s.worked_start::timestamptz))/60)
                -s.break_minutes)::integer end,
          case when coalesce(s.row_json->>'hours',
              s.row_json->>'units','')~
                '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
            then round(coalesce(s.row_json->>'hours',
              s.row_json->>'units')::numeric*60)::integer end),
        'band',b.band,
        'booking_reference',coalesce(
          (select rs.current_reference from reference_scope rs
            where rs.invoice_id=s.invoice_id
              and rs.timesheet_id=s.timesheet_id
              and rs.segment_id=s.segment_id
              and nullif(rs.current_reference,'') is not null
            order by rs.row_key limit 1),
          s.source_reference,
          (select rs.current_reference from reference_scope rs
            where rs.invoice_id=s.invoice_id
              and rs.timesheet_id=s.timesheet_id
              and rs.day_ymd=s.work_date
              and nullif(rs.current_reference,'') is not null
            order by rs.row_key limit 1),
          case when jsonb_typeof(b.day_references_json)='object'
            then coalesce(
              b.day_references_json#>>array[s.work_date,'reference'],
              b.day_references_json#>>array[s.work_date,'ref_num'],
              b.day_references_json->>s.work_date) end,
          nullif(b.reference_number,'')),
        'reference_required',b.reference_required,
        'reference_source',case
          when exists(select 1 from reference_scope rs
            where rs.invoice_id=s.invoice_id and rs.timesheet_id=s.timesheet_id
              and rs.segment_id=s.segment_id
              and nullif(rs.current_reference,'') is not null) then 'SEGMENT'
          when nullif(s.source_reference,'') is not null then 'SEGMENT_SOURCE'
          when exists(select 1 from reference_scope rs
            where rs.invoice_id=s.invoice_id and rs.timesheet_id=s.timesheet_id
              and rs.day_ymd=s.work_date
              and nullif(rs.current_reference,'') is not null) then 'DAY'
          when nullif(b.reference_number,'') is not null then 'TIMESHEET'
          else 'NONE' end,
        'reference_row_key',coalesce(
          (select rs.row_key from reference_scope rs
            where rs.invoice_id=s.invoice_id and rs.timesheet_id=s.timesheet_id
              and rs.segment_id=s.segment_id order by rs.row_key limit 1),
          s.timesheet_id::text||'|SEGMENT|'||s.segment_id||'|'||
            coalesce(s.work_date,'')))
        shift_model
    from timesheet_schedule_normalised s
    join timesheet_week_context b
      on b.invoice_id is not distinct from s.invoice_id
     and b.timesheet_id=s.timesheet_id
  ),
  timesheet_current_additional_rows as materialized (
    select b.invoice_id,b.timesheet_id,d.code,d.unit_date,d.quantity
    from timesheet_week_context b
    cross join lateral (
      select upper(day_unit.key) code,day_value.key unit_date,
        (day_value.value#>>'{}')::numeric quantity
      from jsonb_each(case
        when jsonb_typeof(b.additional_units_per_day)='object'
          then b.additional_units_per_day else '{}'::jsonb end) day_unit(key,value)
      cross join lateral jsonb_each(case
        when jsonb_typeof(day_unit.value)='object'
          then day_unit.value else '{}'::jsonb end) day_value(key,value)
      where coalesce(day_value.value#>>'{}','')~
          '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
        and (day_value.value#>>'{}')::numeric<>0
      union all
      select upper(week_unit.key),null::text,
        (week_unit.value#>>'{}')::numeric
      from jsonb_each(case
        when jsonb_typeof(b.additional_units_week)='object'
          then b.additional_units_week else '{}'::jsonb end) week_unit(key,value)
      where coalesce(week_unit.value#>>'{}','')~
          '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
        and (week_unit.value#>>'{}')::numeric<>0
        and not exists(
          select 1
          from jsonb_each(case
            when jsonb_typeof(b.additional_units_per_day)='object'
              then b.additional_units_per_day else '{}'::jsonb end) per_day(code,value)
          cross join lateral jsonb_each(case
            when jsonb_typeof(per_day.value)='object'
              then per_day.value else '{}'::jsonb end) per_value(day,value)
          where upper(per_day.code)=upper(week_unit.key)
            and coalesce(per_value.value#>>'{}','')~
              '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
            and (per_value.value#>>'{}')::numeric<>0)
    ) d
  ),
  timesheet_additional_rows as materialized (
    select b.invoice_id,b.timesheet_id,a.key code,
      coalesce(nullif(a.value->>'bucket_name',''),a.key) rate_type,
      nullif(d.key,'') unit_date,
      case
        when d.key is not null and coalesce(d.value#>>'{}','')~
          '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
          then(d.value#>>'{}')::numeric
        when d.key is null and coalesce(a.value->>'unit_count','')~
          '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
          then(a.value->>'unit_count')::numeric
      end quantity,
      coalesce(nullif(a.value->>'unit_name',''),'unit') unit_name,
      case when d.key is null then 'WEEKLY' else 'PER_DAY' end frequency,
      case when coalesce(a.value->>'configured_display_order',
          a.value->>'display_order','')~'^[0-9]{1,9}$'
        then coalesce(a.value->>'configured_display_order',
          a.value->>'display_order')::integer else 2147483647 end configured_order
    from timesheet_week_context b
    cross join lateral jsonb_each(case
      when jsonb_typeof(b.additional_units_json)='object'
        then b.additional_units_json else '{}'::jsonb end) a(key,value)
    left join lateral jsonb_each(case
      when jsonb_typeof(a.value->'days')='object' and a.value->'days'<>'{}'::jsonb
        then a.value->'days' else '{}'::jsonb end) d(key,value) on true
    where jsonb_typeof(a.value)='object'
      and case
        when d.key is not null and coalesce(d.value#>>'{}','')~
          '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
          then(d.value#>>'{}')::numeric<>0
        when d.key is null and coalesce(a.value->>'unit_count','')~
          '^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$'
          then(a.value->>'unit_count')::numeric<>0
        else false end
  ),
  timesheet_additional_staleness as materialized (
    select b.invoice_id,b.timesheet_id,
      exists(
        (select upper(c.code),c.unit_date,trim_scale(c.quantity)
         from timesheet_current_additional_rows c
         where c.invoice_id is not distinct from b.invoice_id
           and c.timesheet_id=b.timesheet_id
         except
         select upper(a.code),a.unit_date,trim_scale(a.quantity)
         from timesheet_additional_rows a
         where a.invoice_id is not distinct from b.invoice_id
           and a.timesheet_id=b.timesheet_id)
        union all
        (select upper(a.code),a.unit_date,trim_scale(a.quantity)
         from timesheet_additional_rows a
         where a.invoice_id is not distinct from b.invoice_id
           and a.timesheet_id=b.timesheet_id
         except
         select upper(c.code),c.unit_date,trim_scale(c.quantity)
         from timesheet_current_additional_rows c
         where c.invoice_id is not distinct from b.invoice_id
           and c.timesheet_id=b.timesheet_id)
      ) snapshot_stale
    from timesheet_week_context b
  ),
  timesheet_v2_models as materialized (
    select b.invoice_id,b.timesheet_id,
      jsonb_build_object(
        'schema_version','TIMESHEET_RENDER_MODEL_V2',
        'template_version','timesheet-professional-v2',
        'layout_contract_version','TIMESHEET_ONE_PAGE_LANDSCAPE_V2',
        'timesheet_id',b.timesheet_id,
        'document_revision',b.document_revision,
        'timesheet_number',coalesce((
          select lpad((h.fnv_hash%100000000)::text,8,'0')
          from timesheet_number_fnv h
          where h.invoice_id is not distinct from b.invoice_id
            and h.timesheet_id=b.timesheet_id
            and h.character_no=length(h.source_text)+1
          limit 1),'00000000'),
        'sheet_scope',b.sheet_scope,
        'form_variant',case
          when b.submission_mode='MANUAL' and upper(coalesce(b.qr_status,''))='PENDING'
            then 'QR_UNSIGNED'
          when b.submission_mode='ELECTRONIC'
            and b.authorised_at_server is not null
            and nullif(b.r2_nurse_key,'') is not null
            and nullif(b.r2_auth_key,'') is not null
            then 'ELECTRONIC_SIGNED'
          else 'ELECTRONIC_UNSIGNED' end,
        'submission_mode',b.submission_mode,
        'locale','en-GB','time_zone','Europe/London',
        'week_period',jsonb_build_object(
          'start_date',b.resolved_week_ending_date-6,
          'end_date',b.resolved_week_ending_date,
          'end_weekday_index',extract(dow from b.resolved_week_ending_date)::integer,
          'end_weekday_name',trim(to_char(b.resolved_week_ending_date,'Day')),
          'start_weekday_name',trim(to_char(b.resolved_week_ending_date-6,'Day')),
          'source',case when b.week_ending_date is not null
            then 'TIMESHEET_WEEK_ENDING_DATE'
            when b.contract_id is not null then 'CONTRACT_WEEK_ENDING_WEEKDAY_SNAPSHOT'
            else 'CLIENT_SETTING' end,
          'configured_week_ending_weekday',b.configured_week_ending_weekday,
          'days',(select jsonb_agg(jsonb_build_object(
            'row_key','day:'||d.day_date::text,
            'display_order',d.display_order,
            'date',d.day_date,
            'weekday_index',extract(dow from d.day_date)::integer,
            'weekday_name',trim(to_char(d.day_date,'Day')),
            'weekday_abbreviation',trim(to_char(d.day_date,'Dy')),
            'shift_lines',coalesce((select jsonb_agg(r.shift_model order by
              (r.shift_model->>'display_order')::integer,r.source_order)
              from timesheet_schedule_rows r
              where r.invoice_id is not distinct from b.invoice_id
                and r.timesheet_id=b.timesheet_id
                and r.work_date=d.day_date::text),'[]'::jsonb))
            order by d.display_order)
            from(select (b.resolved_week_ending_date-7+g)::date day_date,
                g::integer display_order
              from generate_series(1,7) g) d)),
        'worker',jsonb_build_object(
          'first_name',b.candidate_first_name,
          'surname',b.candidate_surname,
          'job_profile_title',b.job_profile_title),
        'client',jsonb_build_object(
          'name',b.client_name,'hospital',b.hospital,'site_ward',b.site_ward),
        'band',b.band,
        'branding',jsonb_build_object(
          'agency_name',coalesce(nullif(b.agency_name,''),'ARMS'),
          'logo',b.logo_identity),
        'wording',jsonb_build_object(
          'header',b.header_wording,'footer',b.footer_wording,
          'temporary_worker_declaration',b.temporary_worker_declaration,
          'client_declaration',b.client_declaration),
        'presentation_settings_hash',encode(digest(jsonb_build_object(
          'agency_name',b.agency_name,'logo',b.logo_identity,
          'header',b.header_wording,'footer',b.footer_wording,
          'temporary_worker_declaration',b.temporary_worker_declaration,
          'client_declaration',b.client_declaration)::text,'sha256'),'hex'),
        'additional_units_section',jsonb_build_object(
          'schema_version','TIMESHEET_ADDITIONAL_UNITS_V1',
          'visible',exists(select 1 from timesheet_additional_rows a
            where a.invoice_id is not distinct from b.invoice_id
              and a.timesheet_id=b.timesheet_id),
          'title','Additional rates / units',
          'column_labels',jsonb_build_object(
            'rate_type','Rate Type','date','Date',
            'quantity','Quantity','unit','Unit'),
          'minimum_blank_space_rows',1,
          'rows',coalesce((select jsonb_agg(jsonb_build_object(
            'row_key','additional:'||a.code||':'||coalesce(a.unit_date,'weekly'),
            'display_order',a.display_order,'code',a.code,
            'rate_type',a.rate_type,'date',a.unit_date,
            'quantity',trim_scale(a.quantity),'unit',a.unit_name,
            'frequency',a.frequency) order by a.display_order)
            from(select a0.*,row_number() over(order by a0.configured_order,
                a0.code,a0.unit_date nulls last)::integer display_order
              from timesheet_additional_rows a0
              where a0.invoice_id is not distinct from b.invoice_id
                and a0.timesheet_id=b.timesheet_id) a),'[]'::jsonb)),
        'additional_units_snapshot_stale',coalesce((
          select s.snapshot_stale
          from timesheet_additional_staleness s
          where s.invoice_id is not distinct from b.invoice_id
            and s.timesheet_id=b.timesheet_id),false),
        'totals',jsonb_build_object(
          'paid_minutes',coalesce((select sum(
            (r.shift_model->>'paid_minutes')::numeric)
            from timesheet_schedule_rows r
            where r.invoice_id is not distinct from b.invoice_id
              and r.timesheet_id=b.timesheet_id),0)),
        'authorisation',jsonb_build_object(
          'authorised',b.authorised_at_server is not null,
          'name',b.auth_name,'role',b.auth_job_title,
          'authorised_at_utc',b.authorised_at_server),
        'signatures',jsonb_build_object(
          'candidate',case when nullif(b.r2_nurse_key,'') is null then '{}'::jsonb
            else jsonb_build_object('r2_key',b.r2_nurse_key,
              'sha256',lower(b.img_sha256_nurse),'size_bytes',null,
              'media_type','image/png',
              'signed_date',b.authorised_at_server::date) end,
          'authoriser',case when nullif(b.r2_auth_key,'') is null then '{}'::jsonb
            else jsonb_build_object('r2_key',b.r2_auth_key,
              'sha256',lower(b.img_sha256_auth),'size_bytes',null,
              'media_type','image/png',
              'signed_date',b.authorised_at_server::date) end),
        'qr',jsonb_build_object(
          'required',b.submission_mode='MANUAL'
            and upper(coalesce(b.qr_status,''))='PENDING',
          'signed',b.qr_signed_hash is not null,
          'status',b.qr_status,'token',b.qr_token,
          'payload',case when jsonb_typeof(b.qr_payload_json)='object'
            then b.qr_payload_json else '{}'::jsonb end,
          'signed_hash',b.qr_signed_hash,'signed_at_utc',b.qr_signed_at_utc),
        'layout',jsonb_build_object(
          'one_page_required',true,
          'allowed_modes',jsonb_build_array('NORMAL','COMPACT','ULTRA'),
          'second_page_allowed',false,
          'minimum_font_size',5.5,
          'minimum_row_height_mm',3.45,
          'minimum_signature_height_mm',7,
          'minimum_additional_blank_rows',1),
        'week_period_hash',encode(digest(jsonb_build_object(
          'start_date',b.resolved_week_ending_date-6,
          'end_date',b.resolved_week_ending_date,
          'configured_week_ending_weekday',
            b.configured_week_ending_weekday)::text,'sha256'),'hex'),
        'reference_signature',encode(digest(coalesce((
          select string_agg((r.shift_model->>'reference_row_key')||'='||
            coalesce(r.shift_model->>'booking_reference',''),'||'
            order by r.work_date,(r.shift_model->>'display_order')::integer)
          from timesheet_schedule_rows r
          where r.invoice_id is not distinct from b.invoice_id
            and r.timesheet_id=b.timesheet_id),'') ,'sha256'),'hex'),
        'additional_units_hash',encode(digest(coalesce((
          select jsonb_agg(jsonb_build_object(
            'code',a.code,'date',a.unit_date,'quantity',a.quantity,
            'unit',a.unit_name) order by a.configured_order,a.code,
              a.unit_date nulls last)::text
          from timesheet_additional_rows a
          where a.invoice_id is not distinct from b.invoice_id
            and a.timesheet_id=b.timesheet_id),'[]'),'sha256'),'hex')
      ) render_model
    from timesheet_week_context b
  ),
  invoice_timesheet_rows as materialized (
    select il.invoice_id,t.timesheet_id,
      coalesce(v2.render_model,jsonb_build_object(
        'schema_version','TIMESHEET_RENDER_MODEL_V1',
        'timesheet_id',t.timesheet_id,
        'document_revision',t.document_revision,
        'candidate',jsonb_build_object('id',coalesce(vs.candidate_id::text,null),'name',coalesce(vs.candidate_name,t.occupant_key_norm)),
        'client',jsonb_build_object('id',coalesce(vs.client_id::text,null),'name',vs.client_name),
        'contract',jsonb_build_object('id',coalesce(t.contract_id::text,null),'reference',t.booking_id),
        'work',jsonb_build_object(
          'hospital',t.hospital_norm,
          'site',t.hospital_norm,
          'ward',t.ward_norm,
          'assignment',coalesce(
            nullif(btrim(t.job_title_norm),''),
            case when nullif(btrim(t.shift_label_norm),'') is not null
                   and length(t.shift_label_norm) <= 80
                   and t.shift_label_norm !~* '^weekly-correction-'
                   and t.shift_label_norm !~* '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
              then t.shift_label_norm end),
          'job_title',t.job_title_norm,
          'band',t.band,
          'shift_type',case when nullif(btrim(t.shift_label_norm),'') is not null
                              and length(t.shift_label_norm) <= 80
                              and t.shift_label_norm !~* '^weekly-correction-'
                              and t.shift_label_norm !~* '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
                         then t.shift_label_norm end),
        'week_ending_date',t.week_ending_date,
        'submission_mode',t.submission_mode::text,
        'sheet_scope',t.sheet_scope::text,
        'daily_schedule_rows',coalesce((
           select jsonb_agg(jsonb_build_object(
             'date',coalesce(s.value->>'date',s.value->>'day',to_char(t.worked_start_iso::date,'YYYY-MM-DD')),
             'scheduled_start',coalesce(s.value->>'scheduled_start',s.value->>'start',s.value->>'start_utc',t.scheduled_start_iso::text),
             'scheduled_end',coalesce(s.value->>'scheduled_end',s.value->>'end',s.value->>'end_utc',t.scheduled_end_iso::text),
             'worked_start',coalesce(s.value->>'worked_start',s.value->>'start',s.value->>'start_utc',t.worked_start_iso::text),
             'worked_end',coalesce(s.value->>'worked_end',s.value->>'end',s.value->>'end_utc',t.worked_end_iso::text),
             'break_start',coalesce(s.value->>'break_start',t.break_start_iso::text),
             'break_end',coalesce(s.value->>'break_end',t.break_end_iso::text),
             'break_minutes',case when coalesce(s.value->>'break_minutes',s.value->>'break_mins','') ~ '^[0-9]{1,5}$' then (coalesce(s.value->>'break_minutes',s.value->>'break_mins'))::integer else coalesce(t.break_minutes,0) end,
             'reference',coalesce(s.value->>'reference',s.value->>'ref_num',t.reference_number),
             'hours',coalesce(s.value->>'hours',s.value->>'units',case when t.worked_minutes is not null then round(t.worked_minutes::numeric/60,2)::text end),
             'units',coalesce(s.value->>'units',s.value->>'hours'),
             'display_order',s.ordinality)
             order by s.ordinality)
           from jsonb_array_elements(case when jsonb_typeof(t.actual_schedule_json)='array' then t.actual_schedule_json else '[]'::jsonb end) with ordinality s(value,ordinality)
           where upper(coalesce(t.sheet_scope::text,''))='DAILY'
        ), case when upper(coalesce(t.sheet_scope::text,''))='DAILY' then jsonb_build_array(jsonb_build_object(
             'date',t.worked_start_iso::date,'scheduled_start',t.scheduled_start_iso,'scheduled_end',t.scheduled_end_iso,
             'worked_start',t.worked_start_iso,'worked_end',t.worked_end_iso,
             'break_start',t.break_start_iso,'break_end',t.break_end_iso,'break_minutes',coalesce(t.break_minutes,0),
             'reference',t.reference_number,'hours',case when t.worked_minutes is not null then round(t.worked_minutes::numeric/60,2)::text end,'units',null,'display_order',1)) else '[]'::jsonb end),
        'weekly_schedule_rows',coalesce((
           select jsonb_agg(jsonb_build_object(
             'date',coalesce(s.value->>'date',s.value->>'day'),
             'scheduled_start',coalesce(s.value->>'scheduled_start',s.value->>'start',s.value->>'start_utc'),
             'scheduled_end',coalesce(s.value->>'scheduled_end',s.value->>'end',s.value->>'end_utc'),
             'worked_start',coalesce(s.value->>'worked_start',s.value->>'start',s.value->>'start_utc'),
             'worked_end',coalesce(s.value->>'worked_end',s.value->>'end',s.value->>'end_utc'),
             'break_start',s.value->>'break_start','break_end',s.value->>'break_end',
             'break_minutes',case when coalesce(s.value->>'break_minutes',s.value->>'break_mins','') ~ '^[0-9]{1,5}$' then (coalesce(s.value->>'break_minutes',s.value->>'break_mins'))::integer else 0 end,
             'reference',coalesce(s.value->>'reference',s.value->>'ref_num'),
             'hours',coalesce(
               s.value->>'hours',
               s.value->>'units',
               case when pg_input_is_valid(nullif(coalesce(s.value->>'worked_start',s.value->>'start',s.value->>'start_utc'),''),'timestamptz')
                          and pg_input_is_valid(nullif(coalesce(s.value->>'worked_end',s.value->>'end',s.value->>'end_utc'),''),'timestamptz')
                    then greatest(
                      0,
                      round((
                        extract(epoch from (
                          coalesce(s.value->>'worked_end',s.value->>'end',s.value->>'end_utc')::timestamptz
                          - coalesce(s.value->>'worked_start',s.value->>'start',s.value->>'start_utc')::timestamptz
                        )) / 60
                        - case when coalesce(s.value->>'break_minutes',s.value->>'break_mins','') ~ '^[0-9]{1,5}$'
                            then coalesce(s.value->>'break_minutes',s.value->>'break_mins')::numeric else 0 end
                      ) / 60,2)
                    )::text end),
             'units',coalesce(s.value->>'units',s.value->>'hours'),
             'display_order',s.ordinality)
             order by s.ordinality)
           from jsonb_array_elements(case when jsonb_typeof(t.actual_schedule_json)='array' then t.actual_schedule_json else '[]'::jsonb end) with ordinality s(value,ordinality)
           where upper(coalesce(t.sheet_scope::text,''))='WEEKLY'
        ),'[]'::jsonb),
        'references',jsonb_build_object(
          'whole',coalesce((select rs.current_reference from reference_scope rs where rs.invoice_id=il.invoice_id and rs.timesheet_id=t.timesheet_id and rs.segment_id is null and rs.day_ymd is null and nullif(rs.current_reference,'') is not null order by rs.row_key limit 1),nullif(t.reference_number,'')),
          'day',coalesce(
             (
               select jsonb_agg(
                 jsonb_build_object(
                   'day_key',ordered_reference.day_ymd,
                   'reference',ordered_reference.current_reference,
                   'display_order',ordered_reference.display_order
                 )
                 order by ordered_reference.display_order
               )
               from (
                 select
                   rs.day_ymd,
                   rs.current_reference,
                   row_number() over(order by rs.day_ymd,rs.row_key)::integer as display_order
                 from reference_scope rs
                 where rs.invoice_id=il.invoice_id
                   and rs.timesheet_id=t.timesheet_id
                   and rs.day_ymd is not null
                   and rs.segment_id is null
               ) ordered_reference
             ),
             (select jsonb_agg(jsonb_build_object('day_key',d.key,'reference',case when jsonb_typeof(d.value)='object' then coalesce(d.value->>'reference',d.value->>'ref_num',d.value->>'value') else trim(both '"' from d.value::text) end,'display_order',d.ordinality) order by d.ordinality) from jsonb_each(case when jsonb_typeof(t.day_references_json)='object' then t.day_references_json else '{}'::jsonb end) with ordinality d(key,value,ordinality)),
             '[]'::jsonb),
          'segment',coalesce(
            (
              select jsonb_agg(
                jsonb_build_object(
                  'segment_id',ordered_reference.segment_id,
                  'reference',ordered_reference.current_reference,
                  'display_order',ordered_reference.display_order
                )
                order by ordered_reference.display_order
              )
              from (
                select
                  rs.segment_id,
                  rs.current_reference,
                  row_number() over(order by rs.segment_id,rs.row_key)::integer as display_order
                from reference_scope rs
                where rs.invoice_id=il.invoice_id
                  and rs.timesheet_id=t.timesheet_id
                  and rs.segment_id is not null
              ) ordered_reference
            ),
            '[]'::jsonb
          )),
        'additional_units',coalesce((
          select jsonb_object_agg(
            au.key,
            case
              when jsonb_typeof(au.value)='number'
                then to_jsonb(trim_scale((au.value#>>'{}')::numeric)::text)
              else au.value
            end
            order by au.key
          )
          from jsonb_each(
            case when jsonb_typeof(t.additional_units_week)='object'
              then t.additional_units_week else '{}'::jsonb end
          ) au
        ),'{}'::jsonb),
        'authorisation',jsonb_build_object('authorised',t.authorised_at_server is not null,'name',t.auth_name,'role',t.auth_job_title,'authorised_at_utc',t.authorised_at_server),
        'signatures',jsonb_build_object(
          'candidate',case when nullif(t.r2_nurse_key,'') is null
            then jsonb_build_object('identity',coalesce(vs.candidate_name,t.occupant_key_norm),'role','Candidate / nurse')
            else jsonb_build_object('r2_key',t.r2_nurse_key,'sha256',t.img_sha256_nurse,'size_bytes',null,'media_type','image/png','identity',coalesce(vs.candidate_name,t.occupant_key_norm),'role','Candidate / nurse') end,
          'authoriser',case when nullif(t.r2_auth_key,'') is null
            then jsonb_build_object('identity',t.auth_name,'role',t.auth_job_title)
            else jsonb_build_object('r2_key',t.r2_auth_key,'sha256',t.img_sha256_auth,'size_bytes',null,'media_type','image/png','identity',t.auth_name,'role',t.auth_job_title) end),
        'qr',jsonb_build_object('required',t.qr_status is not null,'signed',t.qr_signed_hash is not null,'status',t.qr_status::text,'signed_hash',t.qr_signed_hash,'signed_at_utc',t.qr_signed_at_utc,'verification_summary',case when t.qr_signed_hash is not null then 'QR signature verified in frozen source' when t.qr_status is not null then t.qr_status::text else null end),
        'template_version','timesheet-professional-v1'
      )) render_model,
      t.submission_mode::text submission_mode,t.document_revision,t.manual_document_asset_id,
      coalesce(vs.client_is_nhsp,false) client_is_nhsp,
      coalesce(vs.client_no_timesheet_required,false) no_timesheet_required,
      coalesce(pc.effective_ts_attach_to_invoice,true) attach_timesheet
    from (
      select distinct il.invoice_id, il.timesheet_id
      from public.invoice_lines il
      where il.invoice_id in(select invoice_id from requested_invoices)
        and il.timesheet_id is not null
    ) il
    join public.timesheets t on t.timesheet_id=il.timesheet_id and t.is_current
    left join public.v_timesheets_summary_base vs on vs.timesheet_id=t.timesheet_id
    left join timesheet_v2_models v2
      on v2.invoice_id=il.invoice_id and v2.timesheet_id=t.timesheet_id
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=t.timesheet_id
  ),
  invoice_timesheets_agg as materialized (
    select invoice_id,jsonb_agg(jsonb_build_object(
      'timesheet_id',timesheet_id,'submission_mode',submission_mode,'document_revision',document_revision,
      'manual_document_asset_id',manual_document_asset_id,'client_is_nhsp',client_is_nhsp,
      'no_timesheet_required',no_timesheet_required,'attach_timesheet',attach_timesheet,
      'render_model',render_model) order by timesheet_id) timesheet_sources
    from invoice_timesheet_rows group by invoice_id
  ),
  support_agg as materialized (
    select source_group.invoice_id,
      jsonb_agg(jsonb_build_object(
        'source_system',source_group.source_system,
        'import_id',(
          select s.import_id
          from public.invoice_hr_source_rows s
          where s.invoice_id=source_group.invoice_id
            and upper(s.source_system)=upper(source_group.source_system)
          order by s.import_id
          limit 1
        ),
        'render_model',jsonb_build_object(
          'schema_version',case when upper(source_group.source_system)='NHSP'
            then 'NHSP_PRESENTATION_V1' else 'HEALTHROSTER_PRESENTATION_V2' end,
          'rows',coalesce((select jsonb_agg(
            case when upper(source_group.source_system)='NHSP' then jsonb_build_object(
              'worker',coalesce(
                r.value->>'worker',r.value->>'candidate',r.value->>'name',
                r.value->>'worker_name',r.value->>'staff_name'),
              'nhsp_shift_id',coalesce(
                r.value->>'nhsp_shift_id',r.value->>'shift_id',
                r.value->>'assignment_code',r.value->>'assignment'),
              'booking_reference',coalesce(
                r.value->>'booking_reference',r.value->>'reference',
                r.value->>'ref_num',r.value->>'assignment_code'),
              'site_ward',concat_ws(' / ',
                nullif(coalesce(
                  r.value->>'site',r.value->>'hospital',
                  r.value->>'trust',r.value->>'client'),''),
                nullif(r.value->>'ward','')),
              'shift_date',coalesce(
                r.value->>'shift_date',r.value->>'date',
                r.value->>'work_date',r.value->>'date_raw'),
              'shift_times',coalesce(
                r.value->>'shift_times',
                concat_ws(' - ',
                  nullif(coalesce(r.value->>'start',r.value->>'start_local'),''),
                  nullif(coalesce(r.value->>'end',r.value->>'end_local'),''))),
              'hours_units',coalesce(
                r.value->>'hours_units',r.value->>'hours',
                r.value->>'units',r.value->>'hours_worked',
                case
                  when coalesce(r.value->>'start_local','')
                         ~ '^([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$'
                   and coalesce(r.value->>'end_local','')
                         ~ '^([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$'
                   and coalesce(r.value->>'break_mins','0')
                         ~ '^[0-9]+([.][0-9]+)?$'
                  then trim(to_char(round((
                    extract(epoch from (
                      case
                        when (r.value->>'end_local')::time
                               >= (r.value->>'start_local')::time
                          then (r.value->>'end_local')::time
                               - (r.value->>'start_local')::time
                        else (r.value->>'end_local')::time
                               + interval '24 hours'
                               - (r.value->>'start_local')::time
                      end
                    )) / 3600
                    - (coalesce(r.value->>'break_mins','0')::numeric / 60)
                  )::numeric,2),'FM999999990.00'))
                end),
              'commission_amount',coalesce(
                r.value->>'commission_amount',r.value->>'commission',
                (select r.value->'raw_columns'->>((h.ordinality-1)::integer)
                 from jsonb_array_elements_text(
                   case when jsonb_typeof(s.header_columns)='array'
                     then s.header_columns else '[]'::jsonb end
                 ) with ordinality h(header_name,ordinality)
                 where lower(btrim(h.header_name))='commission'
                 order by h.ordinality
                 limit 1)),
              'total_amount',coalesce(
                r.value->>'total_amount',r.value->>'total_cost',
                (select r.value->'raw_columns'->>((h.ordinality-1)::integer)
                 from jsonb_array_elements_text(
                   case when jsonb_typeof(s.header_columns)='array'
                     then s.header_columns else '[]'::jsonb end
                 ) with ordinality h(header_name,ordinality)
                 where lower(btrim(h.header_name)) in ('total cost','total amount')
                 order by case when lower(btrim(h.header_name))='total cost'
                   then 0 else 1 end,h.ordinality
                 limit 1)),
              'source_identity',concat_ws(' · ',
                upper(coalesce(s.source_system,'NHSP')),
                'import row '||r.ordinality::text),
              'validation_state',coalesce(
                r.value->>'validation_state',r.value->>'status','Included'),
              'reversal_state',case
                when upper(coalesce(r.value->>'reversal_state',r.value->>'status','')) in ('REVERSED','REVERSAL')
                  or coalesce(
                    r.value->>'hours_units',r.value->>'hours',
                    r.value->>'units',r.value->>'hours_worked','') ~ '^-[0-9]'
                then 'REVERSED' else null end)
            else jsonb_build_object(
              'worker',coalesce(
                r.value->>'worker',r.value->>'candidate',r.value->>'name',
                r.value->>'worker_name',r.value->>'staff_name'),
              'assignment',coalesce(
                r.value->>'assignment',r.value->>'role',
                r.value->>'job_title',r.value->>'assignment_code'),
              'shift_date',coalesce(
                r.value->>'shift_date',r.value->>'date',
                r.value->>'work_date',r.value->>'date_raw'),
              'shift_times',coalesce(
                r.value->>'shift_times',
                concat_ws(' - ',
                  nullif(coalesce(r.value->>'start',r.value->>'start_local'),''),
                  nullif(coalesce(r.value->>'end',r.value->>'end_local'),''))),
              'site',coalesce(
                r.value->>'site',r.value->>'hospital',
                r.value->>'trust',r.value->>'client'),
              'ward',r.value->>'ward',
              'booking_reference',case
                when coalesce(hr_match.reference_match_count,0)=1
                  then hr_match.ref_num
                else null end,
              'units_hours',coalesce(
                r.value->>'units_hours',r.value->>'hours',
                r.value->>'units',r.value->>'hours_worked'),
              'validation_state',coalesce(
                r.value->>'validation_state',r.value->>'status','Included'),
              'source_identity',concat_ws(' · ',
                upper(coalesce(s.source_system,'HEALTHROSTER')),
                'import row '||r.ordinality::text),
              'reference_match_state',case
                when coalesce(hr_match.reference_match_count,0)>1
                  then 'AMBIGUOUS'
                when coalesce(hr_match.reference_match_count,0)=0
                  then 'MISSING'
                when hr_match.match_priority=1
                  then 'EXACT_IMPORT_EXTERNAL_ROW'
                when hr_match.match_priority=2
                  then 'EXACT_IMPORT_REQUEST_ID'
                else 'CONTROLLED_UNIQUE_FALLBACK' end,
              'reference_match_count',coalesce(hr_match.reference_match_count,0),
              'reference_source','HEALTHROSTER_REF_NUM',
              'external_row_key',coalesce(
                hr_match.external_row_key,
                r.value->>'external_row_key',r.value->>'row_key',
                r.value->>'request_id',r.value->>'requestId'),
              'source_record_id',coalesce(
                hr_match.shift_id::text,r.value->>'unique_id'),
              'reversal_state',case
                when upper(coalesce(r.value->>'reversal_state',r.value->>'status','')) in ('REVERSED','REVERSAL')
                  or coalesce(
                    r.value->>'units_hours',r.value->>'hours',
                    r.value->>'units',r.value->>'hours_worked','') ~ '^-[0-9]'
                then 'REVERSED' else null end) end
            order by
              coalesce(
                r.value->>'shift_date',r.value->>'date',
                r.value->>'work_date',r.value->>'date_raw',''),
              coalesce(
                r.value->>'start',r.value->>'worked_start',
                r.value->>'start_local',''),
              s.import_id,
              r.ordinality)
            from public.invoice_hr_source_rows s
            cross join lateral jsonb_array_elements(
              case when jsonb_typeof(s.rows_json)='array' then s.rows_json else '[]'::jsonb end
            ) with ordinality r(value,ordinality)
            left join lateral (
              select
                min(ranked.match_priority) match_priority,
                count(*)::integer reference_match_count,
                case when count(*)=1 then min(ranked.ref_num) end ref_num,
                case when count(*)=1 then min(ranked.external_row_key) end external_row_key,
                case when count(*)=1 then min(ranked.shift_id::text)::uuid end shift_id
              from (
                select candidates.*,
                  min(candidates.match_priority) over () winning_priority
                from (
                  select ns.ref_num,ns.external_row_key,ns.id shift_id,1 match_priority
                  from public.nhsp_shifts ns
                  where upper(ns.source_system::text)=upper(s.source_system)
                    and ns.latest_import_id=s.import_id
                    and nullif(btrim(coalesce(
                      r.value->>'external_row_key',r.value->>'row_key','')),'') is not null
                    and ns.external_row_key=coalesce(
                      r.value->>'external_row_key',r.value->>'row_key')
                  union all
                  select ns.ref_num,ns.external_row_key,ns.id shift_id,2 match_priority
                  from public.nhsp_shifts ns
                  where upper(ns.source_system::text)=upper(s.source_system)
                    and ns.latest_import_id=s.import_id
                    and nullif(btrim(coalesce(
                      r.value->>'request_id',r.value->>'requestId','')),'') is not null
                    and ns.hr_request_id=coalesce(
                      r.value->>'request_id',r.value->>'requestId')
                  union all
                  select ns.ref_num,ns.external_row_key,ns.id shift_id,3 match_priority
                  from public.nhsp_shifts ns
                  where upper(ns.source_system::text)=upper(s.source_system)
                    and coalesce(r.value->>'work_date',r.value->>'date','')
                          ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                    and ns.work_date=coalesce(
                      r.value->>'work_date',r.value->>'date')::date
                    and nullif(r.value->>'start_utc','') is not null
                    and nullif(r.value->>'end_utc','') is not null
                    and ns.start_utc=(r.value->>'start_utc')::timestamptz
                    and ns.end_utc=(r.value->>'end_utc')::timestamptz
                    and (
                      nullif(r.value->>'client_id','') is null
                      or ns.client_id=(r.value->>'client_id')::uuid
                    )
                    and lower(btrim(coalesce(ns.staff_name,'')))=
                        lower(btrim(coalesce(
                          r.value->>'staff_name',r.value->>'worker_name',
                          r.value->>'worker','')))
                    and lower(btrim(coalesce(ns.ward,'')))=
                        lower(btrim(coalesce(r.value->>'ward','')))
                ) candidates
              ) ranked
              where ranked.match_priority=ranked.winning_priority
            ) hr_match on upper(s.source_system)<>'NHSP'
            where s.invoice_id=source_group.invoice_id
              and upper(s.source_system)=upper(source_group.source_system)), '[]'::jsonb),
          'source_identity',jsonb_build_object(
            'source_system',source_group.source_system,
            'import_ids',coalesce((
              select jsonb_agg(to_jsonb(import_id) order by import_id)
              from (
                select distinct s.import_id
                from public.invoice_hr_source_rows s
                where s.invoice_id=source_group.invoice_id
                  and upper(s.source_system)=upper(source_group.source_system)
              ) imports
            ),'[]'::jsonb)))
       ) order by source_group.source_system) supporting_sources
    from (
      select distinct s.invoice_id,upper(s.source_system) source_system
      from public.invoice_hr_source_rows s
      where s.invoice_id in(select invoice_id from requested_invoices)
    ) source_group
    group by source_group.invoice_id
  ),
  supporting_manifest_agg as materialized (
    select il.invoice_id,
      jsonb_agg(distinct jsonb_build_object(
        'timesheet_id',e.timesheet_id,
        'evidence_id',e.id,
        'kind',upper(coalesce(e.kind,'OTHER')),
        'display_name',coalesce(e.display_name,e.kind,'Evidence'),
        'storage_key',e.storage_key,
        'asset_id',e.document_asset_id,
        'source_kind',a.source_kind,
        'source_revision',coalesce(a.source_revision,e.source_revision,encode(digest(concat_ws('|',e.id::text,e.storage_key,e.created_at::text),'sha256'),'hex')),
        'original_r2_key',coalesce(a.original_r2_key,e.storage_key),
        'asset_sha256',a.normalised_sha256,
        'asset_manifest_hash',a.normalised_manifest_hash,
        'asset_size_bytes',a.normalised_size_bytes,
        'asset_page_count',a.normalised_page_count
      )) supporting_manifest
    from public.invoice_lines il
    join public.timesheet_evidence e on e.timesheet_id=il.timesheet_id
    left join public.invoice_document_assets a on a.id=e.document_asset_id
    where il.invoice_id in(select invoice_id from requested_invoices)
      and il.timesheet_id is not null
      and nullif(coalesce(e.storage_key,''),'') is not null
      and upper(coalesce(e.kind,''))<>'TIMESHEET'
    group by il.invoice_id
  ),
  higher_rate_agg as materialized (
    select il.invoice_id,
      jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1','rows',
        coalesce(jsonb_agg(jsonb_build_object(
          'worker_source',coalesce(il.meta_json->>'worker',il.meta_json->>'candidate',il.booking_id),
          'shift_date',coalesce(il.meta_json->>'shift_date',il.meta_json->>'date'),
          'original_rate',il.meta_json->>'original_rate',
          'applied_rate',coalesce(il.meta_json->>'applied_rate',il.charge_day::text,il.charge_night::text,il.charge_sat::text,il.charge_sun::text,il.charge_bh::text),
          'units',coalesce(il.meta_json->>'units',il.meta_json->>'hours'),
          'display_amount',round(coalesce(il.total_charge_ex_vat,0),2)::text,
          'reason',coalesce(il.meta_json->>'reason',il.description),
          'approval_identity',il.meta_json->'approval_identity',
          'reference',coalesce(il.meta_json->>'reference',il.source_key)
        ) order by il.created_at,il.id) filter (where upper(coalesce(il.meta_json->>'line_type','') || ' ' || coalesce(il.description,'') || ' ' || coalesce(il.source_key,'')) like '%HIGHER%'), '[]'::jsonb)) higher_rate_support
    from public.invoice_lines il
    where il.invoice_id in(select invoice_id from requested_invoices)
    group by il.invoice_id
  ),
  invoice_models as materialized (
    select vr.request_no,vr.req_key request_key,
      case when vr.purpose='FINAL_ISSUE'
        then 'FINAL_ISSUE_PRESENTATION_SNAPSHOT_V5'
        else 'INVOICE_PRESENTATION_SNAPSHOT_V5' end::text snapshot_schema_version,
      i.id invoice_id,
      jsonb_build_object(
        'schema_version','INVOICE_RENDER_MODEL_V1',
        'purpose',vr.purpose,
        'document_type',case when i.type::text='CREDIT_NOTE' then 'CREDIT_NOTE'
          when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes') then 'SELF_BILL_INVOICE'
          else 'INVOICE' end,
        'is_draft',vr.purpose<>'FINAL_ISSUE',
        'invoice_id',i.id,
        'invoice_number',i.invoice_no,
        'issue_date',case when vr.purpose='FINAL_ISSUE' then vr.issue_at_utc else i.issued_at_utc end,
        'preview_date',case when vr.purpose='DRAFT_PREVIEW' then v_now end,
        'tax_point',case
          when vr.purpose='FINAL_ISSUE' then vr.tax_point_utc::text
          else coalesce(i.header_snapshot_json->>'tax_point_utc',i.issued_at_utc::text)
        end,
        'due_date',case when vr.purpose='FINAL_ISSUE' then vr.due_at_utc else i.due_at_utc end,
        'currency',coalesce(i.header_snapshot_json->>'currency','GBP'),
        'supplier',jsonb_build_object(
          'legal_name',coalesce(nullif(i.header_snapshot_json->>'agency_name',''),sd.agency_name),
          'trading_name',i.header_snapshot_json->>'agency_trading_name',
          'registered_address',coalesce((select jsonb_agg(btrim(line) order by ord) from regexp_split_to_table(coalesce(nullif(i.header_snapshot_json->>'registered_address',''),sd.registered_address,''), E'\r?\n') with ordinality a(line,ord) where btrim(line)<>''),'[]'::jsonb),
          'company_registration_number',coalesce(nullif(i.header_snapshot_json->>'company_reg_number',''),sd.company_reg_number),
          'vat_registration_number',coalesce(nullif(i.header_snapshot_json->>'vat_registration_number',''),sd.vat_registration_number),
          'contact_email',coalesce(i.header_snapshot_json->>'supplier_email',sd.finance_email,sd.system_email),
          'contact_phone',i.header_snapshot_json->>'supplier_phone'),
        'customer',jsonb_build_object(
          'legal_name',coalesce(nullif(i.header_snapshot_json->>'client_name',''),cl.name),
          'billing_address',coalesce((select jsonb_agg(btrim(line) order by ord) from regexp_split_to_table(coalesce(nullif(i.header_snapshot_json->>'client_invoice_address',''),cl.invoice_address,cl.client_address,''), E'\r?\n') with ordinality a(line,ord) where btrim(line)<>''),'[]'::jsonb),
          'account_reference',coalesce(i.header_snapshot_json->>'client_reference',cl.cli_ref)),
        'references',jsonb_build_object('purchase_order',coalesce(i.header_snapshot_json->>'po_reference',i.header_snapshot_json->>'po_number'),'client_reference',coalesce(i.header_snapshot_json->>'client_reference',cl.cli_ref),'work_location',i.header_snapshot_json->>'work_location'),
        'candidate_summary',i.header_snapshot_json->>'candidate_summary',
        'suppress_candidate_header',
          upper(coalesce(
            i.header_snapshot_json#>>'{meta,consolidation_mode}',
            i.header_snapshot_json#>>'{meta,invoice_consolidation_mode}',
            i.header_snapshot_json->>'consolidation_mode',
            i.header_snapshot_json->>'invoice_consolidation_mode','NONE'))
            in('BY_WEEK','ANY_WEEK')
          or exists(select 1 from public.invoice_hr_source_rows nhsp
            where nhsp.invoice_id=i.id and upper(nhsp.source_system)='NHSP')
          or (select count(distinct il.timesheet_id)
              from public.invoice_lines il
              where il.invoice_id=i.id and il.timesheet_id is not null)>1
          or (select count(distinct nullif(btrim(lb.worker_name),''))
              from invoice_line_base lb where lb.invoice_id=i.id)>1,
        'branding',jsonb_build_object(
          'logo',case
            when jsonb_typeof(i.header_snapshot_json->'agency_logo')='object'
              then jsonb_build_object(
                'r2_key',i.header_snapshot_json#>>'{agency_logo,r2_key}',
                'sha256',lower(i.header_snapshot_json#>>'{agency_logo,sha256}'),
                'size_bytes',case
                  when coalesce(i.header_snapshot_json#>>'{agency_logo,size_bytes}','') ~ '^[1-9][0-9]{0,18}$'
                    then (i.header_snapshot_json#>>'{agency_logo,size_bytes}')::bigint
                  else null end,
                'media_type',lower(i.header_snapshot_json#>>'{agency_logo,media_type}'))
            when jsonb_typeof(coalesce(
              sd.invoice_document_presentation_json#>'{branding,logo}',
              sd.import_config_json#>'{invoice_document_presentation,branding,logo}'
            ))='object'
              then jsonb_build_object(
                'r2_key',coalesce(
                  sd.invoice_document_presentation_json#>>'{branding,logo,r2_key}',
                  sd.import_config_json#>>'{invoice_document_presentation,branding,logo,r2_key}'),
                'sha256',lower(coalesce(
                  sd.invoice_document_presentation_json#>>'{branding,logo,sha256}',
                  sd.import_config_json#>>'{invoice_document_presentation,branding,logo,sha256}')),
                'size_bytes',case
                  when coalesce(
                    sd.invoice_document_presentation_json#>>'{branding,logo,size_bytes}',
                    sd.import_config_json#>>'{invoice_document_presentation,branding,logo,size_bytes}',
                    ''
                  ) ~ '^[1-9][0-9]{0,18}$'
                    then coalesce(
                      sd.invoice_document_presentation_json#>>'{branding,logo,size_bytes}',
                      sd.import_config_json#>>'{invoice_document_presentation,branding,logo,size_bytes}'
                    )::bigint
                  else null end,
                'media_type',lower(coalesce(
                  sd.invoice_document_presentation_json#>>'{branding,logo,media_type}',
                  sd.import_config_json#>>'{invoice_document_presentation,branding,logo,media_type}')))
            else '{}'::jsonb end),
        'lines',coalesce(l.lines,'[]'::jsonb),
        'vat_breakdown',coalesce((select jsonb_agg(jsonb_build_object(
          'rate',vb.vat_rate_pct::text,
          'net_amount',vb.net::text,
          'vat_amount',vb.vat::text,
          'gross_amount',vb.gross::text
        ) order by vb.vat_rate_pct) from (
          select coalesce(vat_rate_pct,0) vat_rate_pct,
            round(sum(coalesce(total_charge_ex_vat,0)),2) net,
            round(sum(coalesce(vat_amount,0)),2) vat,
            round(sum(coalesce(total_inc_vat,coalesce(total_charge_ex_vat,0)+coalesce(vat_amount,0))),2) gross
          from public.invoice_lines
          where invoice_id=i.id
          group by coalesce(vat_rate_pct,0)
        ) vb),'[]'::jsonb),
        'totals',jsonb_build_object(
          'net',round(coalesce(i.subtotal_ex_vat,0),2)::text,
          'vat',round(coalesce(i.vat_amount,0),2)::text,
          'gross',round(coalesce(i.total_inc_vat,0),2)::text,
          'amount_paid',coalesce((
            case when pg_input_is_valid(nullif(i.header_snapshot_json->>'amount_paid',''),'numeric')
              then (i.header_snapshot_json->>'amount_paid')::numeric end
          ),0)::text,
          'amount_credited',coalesce((
            case when pg_input_is_valid(nullif(i.header_snapshot_json->>'amount_credited',''),'numeric')
              then (i.header_snapshot_json->>'amount_credited')::numeric end
          ),0)::text,
          'amount_outstanding',coalesce((
            case when pg_input_is_valid(nullif(i.header_snapshot_json->>'amount_outstanding',''),'numeric')
              then (i.header_snapshot_json->>'amount_outstanding')::numeric end
          ),coalesce(i.total_inc_vat,0))::text),
        'payment',jsonb_build_object(
          'terms_days',coalesce(
            case when pg_input_is_valid(nullif(i.header_snapshot_json->>'payment_terms_days',''),'integer')
              then (i.header_snapshot_json->>'payment_terms_days')::integer end,
            cl.payment_terms_days,30),
          'terms_text',coalesce(
            nullif(i.header_snapshot_json->>'payment_terms_text',''),
            coalesce(
              case when pg_input_is_valid(nullif(i.header_snapshot_json->>'payment_terms_days',''),'integer')
                then (i.header_snapshot_json->>'payment_terms_days')::integer end,
              cl.payment_terms_days,
              30
            )::text || ' days from invoice date'
          ),
          'due_date_basis','ISSUE_DATE',
          'instructions',coalesce(
            i.header_snapshot_json->>'payment_instructions',
            sd.invoice_document_presentation_json->>'payment_instructions',
            sd.import_config_json#>>'{invoice_document_presentation,payment_instructions}'),
          'account_name',case when lower(coalesce(
            i.header_snapshot_json#>>'{meta,hide_bank_footer}',
            i.header_snapshot_json->>'hide_bank_footer',
            sd.invoice_document_presentation_json->>'hide_bank_footer_default',
            sd.import_config_json#>>'{invoice_document_presentation,hide_bank_footer_default}',
            'false')) in('true','t','1','yes') then null
            else coalesce(i.header_snapshot_json#>>'{bank,name}',i.header_snapshot_json->>'bank_name',sd.bank_name) end,
          'sort_code',case when lower(coalesce(
            i.header_snapshot_json#>>'{meta,hide_bank_footer}',
            i.header_snapshot_json->>'hide_bank_footer',
            sd.invoice_document_presentation_json->>'hide_bank_footer_default',
            sd.import_config_json#>>'{invoice_document_presentation,hide_bank_footer_default}',
            'false')) in('true','t','1','yes') then null
            else coalesce(i.header_snapshot_json#>>'{bank,sort_code}',i.header_snapshot_json->>'bank_sort_code',sd.bank_sort_code) end,
          'account_number',case when lower(coalesce(
            i.header_snapshot_json#>>'{meta,hide_bank_footer}',
            i.header_snapshot_json->>'hide_bank_footer',
            sd.invoice_document_presentation_json->>'hide_bank_footer_default',
            sd.import_config_json#>>'{invoice_document_presentation,hide_bank_footer_default}',
            'false')) in('true','t','1','yes') then null
            else coalesce(i.header_snapshot_json#>>'{bank,account_number}',i.header_snapshot_json->>'bank_account_number',sd.bank_account_number) end,
          'remittance_reference',i.invoice_no,
          'remittance_email',coalesce(i.header_snapshot_json->>'remittance_email',sd.finance_email),
          'hide_bank_footer',lower(coalesce(
            i.header_snapshot_json#>>'{meta,hide_bank_footer}',
            i.header_snapshot_json->>'hide_bank_footer',
            sd.invoice_document_presentation_json->>'hide_bank_footer_default',
            sd.import_config_json#>>'{invoice_document_presentation,hide_bank_footer_default}',
            'false')) in('true','t','1','yes')),
        'credit_note',jsonb_build_object('is_credit_note',i.type::text='CREDIT_NOTE','original_invoice_id',i.original_invoice_id,'original_invoice_number',coalesce(i.header_snapshot_json->>'original_invoice_number',orig.invoice_no),'original_invoice_date',coalesce(i.header_snapshot_json->>'original_invoice_date',orig.issued_at_utc::text),'reason',coalesce(i.header_snapshot_json->>'credit_reason',i.notes)),
        'self_bill',jsonb_build_object('is_self_bill',lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes'),'legal_wording',coalesce(i.header_snapshot_json->>'self_bill_wording',coalesce(sd.invoice_document_presentation_json->>'self_bill_legal_wording', sd.import_config_json#>>'{invoice_document_presentation,self_bill_legal_wording}'))),
        'legal_wording',coalesce(i.header_snapshot_json->'legal_wording',coalesce(sd.invoice_document_presentation_json->'legal_wording', sd.import_config_json#>'{invoice_document_presentation,legal_wording}'),'[]'::jsonb),
        'attachment_policy',coalesce(i.header_snapshot_json->'attach_policy',i.header_snapshot_json->'attachment_policy','{}'::jsonb),
        'template_version',vr.template_version,
        'locale',coalesce(i.header_snapshot_json->>'locale',sd.invoice_document_presentation_json->>'locale',sd.import_config_json#>>'{invoice_document_presentation,locale}','en-GB'),
        'page_geometry',coalesce(i.header_snapshot_json->>'page_geometry',sd.invoice_document_presentation_json->>'page_geometry',sd.import_config_json#>>'{invoice_document_presentation,page_geometry}','A4_PORTRAIT_210X297MM')) presentation_model,
      coalesce(ts.timesheet_sources,'[]'::jsonb) timesheet_sources,
      coalesce(sa.supporting_sources,'[]'::jsonb) supporting_sources,
      coalesce(hra.higher_rate_support,jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1','rows','[]'::jsonb)) higher_rate_support,
      coalesce(l.line_net,0) line_net,coalesce(l.line_vat,0) line_vat,coalesce(l.line_gross,0) line_gross,
      round(coalesce(i.subtotal_ex_vat,0),2) invoice_net,round(coalesce(i.vat_amount,0),2) invoice_vat,round(coalesce(i.total_inc_vat,0),2) invoice_gross
    from valid_requests vr
    join public.invoices i on vr.entity_type='INVOICE' and i.id=vr.entity_id
    left join public.invoices orig on orig.id=i.original_invoice_id
    join public.clients cl on cl.id=i.client_id
    cross join public.settings_defaults sd
    left join invoice_line_rows l on l.invoice_id=i.id
    left join invoice_timesheets_agg ts on ts.invoice_id=i.id
    left join support_agg sa on sa.invoice_id=i.id
    left join supporting_manifest_agg sma on sma.invoice_id=i.id
    left join higher_rate_agg hra on hra.invoice_id=i.id
    where sd.id=1
  ),
  timesheet_models as materialized (
    select vr.request_no,vr.req_key request_key,'TIMESHEET_PRESENTATION_SNAPSHOT_V5'::text snapshot_schema_version,
      coalesce(v2.render_model,jsonb_build_object(
        'schema_version','TIMESHEET_RENDER_MODEL_V1',
        'timesheet_id',t.timesheet_id,
        'document_revision',t.document_revision,
        'candidate',jsonb_build_object('id',coalesce(vs.candidate_id::text,null),'name',coalesce(vs.candidate_name,t.occupant_key_norm)),
        'client',jsonb_build_object('id',coalesce(vs.client_id::text,null),'name',vs.client_name),
        'contract',jsonb_build_object('id',coalesce(t.contract_id::text,null),'reference',t.booking_id),
        'work',jsonb_build_object(
          'hospital',t.hospital_norm,
          'site',t.hospital_norm,
          'ward',t.ward_norm,
          'assignment',coalesce(
            nullif(btrim(t.job_title_norm),''),
            case when nullif(btrim(t.shift_label_norm),'') is not null
                   and length(t.shift_label_norm) <= 80
                   and t.shift_label_norm !~* '^weekly-correction-'
                   and t.shift_label_norm !~* '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
              then t.shift_label_norm end),
          'job_title',t.job_title_norm,
          'band',t.band,
          'shift_type',case when nullif(btrim(t.shift_label_norm),'') is not null
                              and length(t.shift_label_norm) <= 80
                              and t.shift_label_norm !~* '^weekly-correction-'
                              and t.shift_label_norm !~* '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
                         then t.shift_label_norm end),
        'week_ending_date',t.week_ending_date,
        'submission_mode',t.submission_mode::text,
        'sheet_scope',t.sheet_scope::text,
        'daily_schedule_rows',coalesce((select jsonb_agg(jsonb_build_object('date',coalesce(s.value->>'date',s.value->>'day',to_char(t.worked_start_iso::date,'YYYY-MM-DD')),'scheduled_start',coalesce(s.value->>'scheduled_start',s.value->>'start',s.value->>'start_utc',t.scheduled_start_iso::text),'scheduled_end',coalesce(s.value->>'scheduled_end',s.value->>'end',s.value->>'end_utc',t.scheduled_end_iso::text),'worked_start',coalesce(s.value->>'worked_start',s.value->>'start',s.value->>'start_utc',t.worked_start_iso::text),'worked_end',coalesce(s.value->>'worked_end',s.value->>'end',s.value->>'end_utc',t.worked_end_iso::text),'break_start',coalesce(s.value->>'break_start',t.break_start_iso::text),'break_end',coalesce(s.value->>'break_end',t.break_end_iso::text),'break_minutes',case when coalesce(s.value->>'break_minutes',s.value->>'break_mins','') ~ '^[0-9]{1,5}$' then (coalesce(s.value->>'break_minutes',s.value->>'break_mins'))::integer else coalesce(t.break_minutes,0) end,'reference',coalesce(s.value->>'reference',s.value->>'ref_num',t.reference_number),'hours',coalesce(s.value->>'hours',s.value->>'units',case when t.worked_minutes is not null then round(t.worked_minutes::numeric/60,2)::text end),'units',coalesce(s.value->>'units',s.value->>'hours'),'display_order',s.ordinality) order by s.ordinality) from jsonb_array_elements(case when jsonb_typeof(t.actual_schedule_json)='array' then t.actual_schedule_json else '[]'::jsonb end) with ordinality s(value,ordinality) where upper(coalesce(t.sheet_scope::text,''))='DAILY'),case when upper(coalesce(t.sheet_scope::text,''))='DAILY' then jsonb_build_array(jsonb_build_object('date',t.worked_start_iso::date,'scheduled_start',t.scheduled_start_iso,'scheduled_end',t.scheduled_end_iso,'worked_start',t.worked_start_iso,'worked_end',t.worked_end_iso,'break_start',t.break_start_iso,'break_end',t.break_end_iso,'break_minutes',coalesce(t.break_minutes,0),'reference',t.reference_number,'hours',case when t.worked_minutes is not null then round(t.worked_minutes::numeric/60,2)::text end,'units',null,'display_order',1)) else '[]'::jsonb end),
        'weekly_schedule_rows',coalesce((select jsonb_agg(jsonb_build_object(
          'date',coalesce(s.value->>'date',s.value->>'day'),
          'scheduled_start',coalesce(s.value->>'scheduled_start',s.value->>'start',s.value->>'start_utc'),
          'scheduled_end',coalesce(s.value->>'scheduled_end',s.value->>'end',s.value->>'end_utc'),
          'worked_start',coalesce(s.value->>'worked_start',s.value->>'start',s.value->>'start_utc'),
          'worked_end',coalesce(s.value->>'worked_end',s.value->>'end',s.value->>'end_utc'),
          'break_start',s.value->>'break_start',
          'break_end',s.value->>'break_end',
          'break_minutes',case when coalesce(s.value->>'break_minutes',s.value->>'break_mins','') ~ '^[0-9]{1,5}$' then coalesce(s.value->>'break_minutes',s.value->>'break_mins')::integer else 0 end,
          'reference',coalesce(s.value->>'reference',s.value->>'ref_num'),
          'hours',coalesce(
            s.value->>'hours',
            s.value->>'units',
            case when pg_input_is_valid(nullif(coalesce(s.value->>'worked_start',s.value->>'start',s.value->>'start_utc'),''),'timestamptz')
                       and pg_input_is_valid(nullif(coalesce(s.value->>'worked_end',s.value->>'end',s.value->>'end_utc'),''),'timestamptz')
                 then greatest(
                   0,
                   round((
                     extract(epoch from (
                       coalesce(s.value->>'worked_end',s.value->>'end',s.value->>'end_utc')::timestamptz
                       - coalesce(s.value->>'worked_start',s.value->>'start',s.value->>'start_utc')::timestamptz
                     )) / 60
                     - case when coalesce(s.value->>'break_minutes',s.value->>'break_mins','') ~ '^[0-9]{1,5}$'
                         then coalesce(s.value->>'break_minutes',s.value->>'break_mins')::numeric else 0 end
                   ) / 60,2)
                 )::text end),
          'units',coalesce(s.value->>'units',s.value->>'hours'),
          'display_order',s.ordinality
        ) order by s.ordinality) from jsonb_array_elements(case when jsonb_typeof(t.actual_schedule_json)='array' then t.actual_schedule_json else '[]'::jsonb end) with ordinality s(value,ordinality) where upper(coalesce(t.sheet_scope::text,''))='WEEKLY'),'[]'::jsonb),
        'references',jsonb_build_object('whole',nullif(t.reference_number,''),'day',coalesce((select jsonb_agg(jsonb_build_object('day_key',d.key,'reference',case when jsonb_typeof(d.value)='object' then coalesce(d.value->>'reference',d.value->>'ref_num',d.value->>'value') else trim(both '"' from d.value::text) end,'display_order',d.ordinality) order by d.ordinality) from jsonb_each(case when jsonb_typeof(t.day_references_json)='object' then t.day_references_json else '{}'::jsonb end) with ordinality d(key,value,ordinality)), '[]'::jsonb),'segment','[]'::jsonb),
        'additional_units',coalesce((
          select jsonb_object_agg(
            au.key,
            case
              when jsonb_typeof(au.value)='number'
                then to_jsonb(trim_scale((au.value#>>'{}')::numeric)::text)
              else au.value
            end
            order by au.key
          )
          from jsonb_each(
            case when jsonb_typeof(t.additional_units_week)='object'
              then t.additional_units_week else '{}'::jsonb end
          ) au
        ),'{}'::jsonb),
        'authorisation',jsonb_build_object('authorised',t.authorised_at_server is not null,'name',t.auth_name,'role',t.auth_job_title,'authorised_at_utc',t.authorised_at_server),
        'signatures',jsonb_build_object(
          'candidate',case when nullif(t.r2_nurse_key,'') is null
            then jsonb_build_object('identity',coalesce(vs.candidate_name,t.occupant_key_norm),'role','Candidate / nurse')
            else jsonb_build_object('r2_key',t.r2_nurse_key,'sha256',t.img_sha256_nurse,'size_bytes',null,'media_type','image/png','identity',coalesce(vs.candidate_name,t.occupant_key_norm),'role','Candidate / nurse') end,
          'authoriser',case when nullif(t.r2_auth_key,'') is null
            then jsonb_build_object('identity',t.auth_name,'role',t.auth_job_title)
            else jsonb_build_object('r2_key',t.r2_auth_key,'sha256',t.img_sha256_auth,'size_bytes',null,'media_type','image/png','identity',t.auth_name,'role',t.auth_job_title) end),
        'qr',jsonb_build_object('required',t.qr_status is not null,'signed',t.qr_signed_hash is not null,'status',t.qr_status::text,'signed_hash',t.qr_signed_hash,'signed_at_utc',t.qr_signed_at_utc,'verification_summary',case when t.qr_signed_hash is not null then 'QR signature verified in frozen source' when t.qr_status is not null then t.qr_status::text else null end),
        'template_version','timesheet-professional-v1'
      )) presentation_model,
      '[]'::jsonb timesheet_sources,
      '[]'::jsonb supporting_sources,
      jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1','rows','[]'::jsonb) higher_rate_support
    from valid_requests vr
    join public.timesheets t on vr.entity_type='TIMESHEET' and t.timesheet_id=vr.entity_id and t.is_current
    left join public.v_timesheets_summary_base vs on vs.timesheet_id=t.timesheet_id
    left join timesheet_v2_models v2
      on v2.invoice_id is null and v2.timesheet_id=t.timesheet_id
  ),
  missing_entity_results as materialized (
    select vr.request_no,vr.req_key request_key,
      case when vr.entity_type='INVOICE' then 'INVOICE_PRESENTATION_SNAPSHOT_V5' else 'TIMESHEET_PRESENTATION_SNAPSHOT_V5' end snapshot_schema_version,
      '{}'::jsonb presentation_model,'[]'::jsonb timesheet_sources,'[]'::jsonb supporting_sources,
      jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1','rows','[]'::jsonb) higher_rate_support,
      jsonb_build_object('snapshot_schema_version','PRESENTATION_ENTITY_NOT_FOUND','request_key',vr.req_key,'entity_type',vr.entity_type,'entity_id',vr.entity_id) snapshot_json,
      encode(digest(jsonb_build_object('snapshot_schema_version','PRESENTATION_ENTITY_NOT_FOUND','request_key',vr.req_key,'entity_type',vr.entity_type,'entity_id',vr.entity_id)::text,'sha256'),'hex') snapshot_hash,
      false valid,
      'PRESENTATION_ENTITY_NOT_FOUND'::text error_code,
      jsonb_build_object('entity_type',vr.entity_type,'entity_id',vr.entity_id) error_detail
    from valid_requests vr
    where not exists(select 1 from invoice_models im where im.request_no=vr.request_no)
      and not exists(select 1 from timesheet_models tm where tm.request_no=vr.request_no)
  ),
  all_models as materialized (
    select request_no,request_key,snapshot_schema_version,presentation_model,timesheet_sources,supporting_sources,higher_rate_support,
      case
        when snapshot_schema_version like '%INVOICE%' and nullif(presentation_model->>'document_type','') is null then 'INVOICE_PRESENTATION_DOCUMENT_TYPE_REQUIRED'
        when snapshot_schema_version like '%INVOICE%' and nullif(presentation_model->>'invoice_number','') is null then 'INVOICE_PRESENTATION_INVOICE_NUMBER_REQUIRED'
        when snapshot_schema_version like '%INVOICE%' and nullif(presentation_model#>>'{supplier,legal_name}','') is null then 'INVOICE_PRESENTATION_SUPPLIER_REQUIRED'
        when snapshot_schema_version like '%INVOICE%' and jsonb_array_length(case when jsonb_typeof(presentation_model#>'{supplier,registered_address}')='array' then presentation_model#>'{supplier,registered_address}' else '[]'::jsonb end)=0 then 'INVOICE_PRESENTATION_SUPPLIER_ADDRESS_REQUIRED'
        when snapshot_schema_version like '%INVOICE%' and nullif(presentation_model#>>'{customer,legal_name}','') is null then 'INVOICE_PRESENTATION_CUSTOMER_REQUIRED'
        when snapshot_schema_version like '%INVOICE%' and jsonb_array_length(case when jsonb_typeof(presentation_model#>'{customer,billing_address}')='array' then presentation_model#>'{customer,billing_address}' else '[]'::jsonb end)=0 then 'INVOICE_PRESENTATION_CUSTOMER_ADDRESS_REQUIRED'
        when snapshot_schema_version='FINAL_ISSUE_PRESENTATION_SNAPSHOT_V5' and nullif(presentation_model->>'issue_date','') is null then 'INVOICE_PRESENTATION_ISSUE_DATE_REQUIRED'
        when snapshot_schema_version='FINAL_ISSUE_PRESENTATION_SNAPSHOT_V5' and nullif(presentation_model->>'tax_point','') is null then 'INVOICE_PRESENTATION_TAX_POINT_REQUIRED'
        when snapshot_schema_version='FINAL_ISSUE_PRESENTATION_SNAPSHOT_V5' and nullif(presentation_model->>'due_date','') is null then 'INVOICE_PRESENTATION_DUE_DATE_REQUIRED'
        when jsonb_array_length(coalesce(presentation_model->'lines','[]'::jsonb))=0
          and snapshot_schema_version like '%INVOICE%'
          and (coalesce(invoice_net,0)<>0 or coalesce(invoice_vat,0)<>0 or coalesce(invoice_gross,0)<>0)
          then 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING'
        when snapshot_schema_version like '%INVOICE%' and (line_net,line_vat,line_gross) is distinct from (invoice_net,invoice_vat,invoice_gross) then 'INVOICE_PRESENTATION_LINE_TOTAL_MISMATCH'
        when snapshot_schema_version like '%INVOICE%' and invoice_vat<>0 and nullif(presentation_model#>>'{supplier,vat_registration_number}','') is null then 'INVOICE_PRESENTATION_VAT_NUMBER_REQUIRED'
        when (presentation_model->>'document_type')='CREDIT_NOTE' and nullif(coalesce(presentation_model#>>'{credit_note,original_invoice_number}',presentation_model#>>'{credit_note,original_invoice_id}'),'') is null then 'CREDIT_NOTE_ORIGINAL_REQUIRED'
        when (presentation_model->>'document_type')='CREDIT_NOTE' and nullif(presentation_model#>>'{credit_note,reason}','') is null then 'CREDIT_NOTE_REASON_REQUIRED'
        when (presentation_model->>'document_type')='SELF_BILL_INVOICE' and nullif(presentation_model#>>'{self_bill,legal_wording}','') is null then 'SELF_BILL_WORDING_REQUIRED'
        else null end error_code
    from invoice_models
    union all
    select request_no,request_key,snapshot_schema_version,presentation_model,timesheet_sources,supporting_sources,higher_rate_support,
      case
        when presentation_model->>'schema_version'='TIMESHEET_RENDER_MODEL_V2'
          and nullif(presentation_model#>>'{week_period,end_date}','') is null
          then 'TIMESHEET_WEEK_ENDING_DATE_MISSING'
        when presentation_model->>'schema_version'='TIMESHEET_RENDER_MODEL_V2'
          and coalesce(presentation_model#>>'{week_period,configured_week_ending_weekday}','')~'^[0-6]$'
          and coalesce(presentation_model#>>'{week_period,end_weekday_index}','')~'^[0-6]$'
          and (presentation_model#>>'{week_period,configured_week_ending_weekday}')::integer
            <> (presentation_model#>>'{week_period,end_weekday_index}')::integer
          then 'TIMESHEET_WEEK_ENDING_WEEKDAY_MISMATCH'
        when presentation_model->>'schema_version'='TIMESHEET_RENDER_MODEL_V2'
          and jsonb_array_length(case
            when jsonb_typeof(presentation_model#>'{week_period,days}')='array'
              then presentation_model#>'{week_period,days}' else '[]'::jsonb end)<>7
          then 'TIMESHEET_WEEK_PERIOD_INVALID'
        when presentation_model->>'schema_version'='TIMESHEET_RENDER_MODEL_V2'
          and not exists(
            select 1
            from jsonb_array_elements(
              coalesce(presentation_model#>'{week_period,days}','[]'::jsonb)) d
            cross join lateral jsonb_array_elements(
              coalesce(d->'shift_lines','[]'::jsonb)) s)
          then 'TIMESHEET_PRESENTATION_SCHEDULE_MISSING'
        when presentation_model->>'schema_version'='TIMESHEET_RENDER_MODEL_V2'
          and exists(
            select 1
            from jsonb_array_elements(
              coalesce(presentation_model#>'{week_period,days}','[]'::jsonb)) d
            cross join lateral jsonb_array_elements(
              coalesce(d->'shift_lines','[]'::jsonb)) s
            where s->>'date'<>d->>'date')
          then 'TIMESHEET_SCHEDULE_DATE_OUTSIDE_PERIOD'
        when presentation_model->>'schema_version'='TIMESHEET_RENDER_MODEL_V2'
          and coalesce((presentation_model->>'additional_units_snapshot_stale')::boolean,false)
          then 'TIMESHEET_ADDITIONAL_UNITS_SNAPSHOT_STALE'
        when presentation_model->>'schema_version'='TIMESHEET_RENDER_MODEL_V2'
          and exists(
            select 1
            from jsonb_array_elements(coalesce(
              presentation_model#>'{additional_units_section,rows}',
              '[]'::jsonb)) a
            where nullif(a->>'date','') is not null
              and ((a->>'date')::date<
                (presentation_model#>>'{week_period,start_date}')::date
                or (a->>'date')::date>
                (presentation_model#>>'{week_period,end_date}')::date))
          then 'TIMESHEET_ADDITIONAL_UNIT_DATE_OUTSIDE_PERIOD'
        when presentation_model->>'schema_version'='TIMESHEET_RENDER_MODEL_V2'
          then null
        when jsonb_array_length(coalesce(
            presentation_model->'daily_schedule_rows','[]'::jsonb))=0
          and jsonb_array_length(coalesce(
            presentation_model->'weekly_schedule_rows','[]'::jsonb))=0
          then 'TIMESHEET_PRESENTATION_SCHEDULE_MISSING'
        else null end error_code
    from timesheet_models
  ),
  assembled as materialized (
    select m.*,
      encode(digest(m.presentation_model::text,'sha256'),'hex') presentation_model_hash,
      jsonb_build_object(
        'snapshot_schema_version',m.snapshot_schema_version,
        'request_key',m.request_key,
        'presentation_model',m.presentation_model,
        'presentation_model_hash',encode(digest(m.presentation_model::text,'sha256'),'hex'),
        'timesheet_sources',m.timesheet_sources,
        'supporting_sources',m.supporting_sources,
        'supporting_manifest',coalesce((select sma.supporting_manifest from supporting_manifest_agg sma where sma.invoice_id=(m.presentation_model->>'invoice_id')::uuid),'[]'::jsonb),
        'higher_rate_support',m.higher_rate_support
      ) snapshot_json
    from all_models m
  )
  select d.request_key,d.snapshot_schema_version,d.presentation_model,d.timesheet_sources,d.supporting_sources,d.higher_rate_support,d.snapshot_json,d.snapshot_hash,d.valid,d.error_code,d.error_detail
  from duplicate_key_results d
  union all
  select i.request_key,i.snapshot_schema_version,i.presentation_model,i.timesheet_sources,i.supporting_sources,i.higher_rate_support,i.snapshot_json,i.snapshot_hash,i.valid,i.error_code,i.error_detail
  from invalid_request_results i
  union all
  select m.request_key,m.snapshot_schema_version,m.presentation_model,m.timesheet_sources,m.supporting_sources,m.higher_rate_support,
         m.snapshot_json,encode(digest(m.snapshot_json::text,'sha256'),'hex'),m.error_code is null,
         m.error_code,
         case when m.error_code is null then '{}'::jsonb else jsonb_build_object('request_key',m.request_key,'code',m.error_code) end
  from assembled m
  union all
  select e.request_key,e.snapshot_schema_version,e.presentation_model,e.timesheet_sources,e.supporting_sources,e.higher_rate_support,e.snapshot_json,e.snapshot_hash,e.valid,e.error_code,e.error_detail
  from missing_entity_results e
  ;
end;
$function$;

revoke all on function private._invoice_presentation_snapshot_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function private._invoice_presentation_snapshot_batch(jsonb,timestamptz)
  to service_role;

\endif
