create or replace function private._invoice_batch_issue_source_rows_core_v2(
  p_allow_early boolean default false,
  p_limit integer default null,
  p_now_utc timestamptz default now(),
  p_invoice_ids uuid[] default null
) returns table(
  client_id uuid,
  client_name text,
  invoice_week_start date,
  week_ending_date date,
  invoice_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with
classified as materialized (
  select
    candidate.candidate_json,
    (candidate.candidate_json->>'invoice_id')::uuid invoice_id
  from private._invoice_batch_issue_classification_v2(
    p_allow_early,
    p_invoice_ids,
    p_now_utc
  ) candidate
  order by
    candidate.candidate_json->>'client_name',
    candidate.candidate_json->>'week_ending_date' desc,
    candidate.candidate_json->>'invoice_number',
    candidate.candidate_json->>'invoice_id'
  limit case
    when p_invoice_ids is not null then 250
    when p_limit is null then null
    else greatest(1,p_limit)
  end
),
base as materialized (
  select
    classified.*,
    invoice.header_snapshot_json,
    invoice.do_not_send,
    invoice.on_hold_reason,
    invoice.active_document_operation_id,
    invoice.last_document_error_json,
    case
      when pg_input_is_valid(
        coalesce(
          invoice.header_snapshot_json#>>
            '{meta,invoice_week_start}',
          ''
        ),
        'date'
      )
        then (
          invoice.header_snapshot_json#>>
            '{meta,invoice_week_start}'
        )::date
    end invoice_week_start
  from classified
  join public.invoices invoice on invoice.id=classified.invoice_id
),
source_timesheets as materialized (
  select distinct line.invoice_id,line.timesheet_id
  from public.invoice_lines line
  join base invoice on invoice.invoice_id=line.invoice_id
  where line.timesheet_id is not null
),
timesheet_support as materialized (
  select
    source.invoice_id,
    source.timesheet_id,
    timesheet.submission_mode,
    coalesce(precheck.effective_ts_attach_to_invoice,true)
      and not coalesce(summary.client_no_timesheet_required,false)
      and not coalesce(summary.client_is_nhsp,false) required,
    manual_asset.status manual_asset_state,
    coalesce(manual_asset.normalised_page_count,0)
      manual_asset_pages,
    version.status timesheet_document_state,
    coalesce(version.page_count,0) timesheet_document_pages,
    case
      when version.status<>'READY' then version.operation_id
    end active_timesheet_document_operation_id,
    upper(coalesce(timesheet.submission_mode::text,''))
      in('MANUAL','QR') is_manual
  from source_timesheets source
  left join public.timesheets timesheet
    on timesheet.timesheet_id=source.timesheet_id
   and timesheet.is_current
  left join public.v_ts_invoice_precheck precheck
    on precheck.timesheet_id=source.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=source.timesheet_id
  left join lateral (
    select evidence.document_asset_id
    from public.timesheet_evidence evidence
    left join public.invoice_document_assets candidate_asset
      on candidate_asset.id=evidence.document_asset_id
    where evidence.timesheet_id=timesheet.timesheet_id
      and upper(coalesce(evidence.kind,''))='TIMESHEET'
      and coalesce(evidence.processing_state,'')<>'SUPERSEDED'
    order by
      (
        evidence.document_asset_id=
          timesheet.manual_document_asset_id
      ) desc,
      (candidate_asset.status='READY') desc,
      evidence.created_at desc nulls last,
      evidence.id desc
    limit 1
  ) manual_source on true
  left join public.invoice_document_assets manual_asset
    on manual_asset.id=coalesce(
      timesheet.manual_document_asset_id,
      manual_source.document_asset_id
    )
  left join lateral (
    select candidate_version.*
    from public.invoice_document_versions candidate_version
    where candidate_version.entity_type='TIMESHEET'
      and candidate_version.entity_id=timesheet.timesheet_id
      and candidate_version.purpose='TIMESHEET'
      and candidate_version.source_revision=
        timesheet.document_revision::text
      and candidate_version.template_version=
        'timesheet-professional-v1'
      and candidate_version.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING','READY','FAILED',
        'SUPERSEDED','CANCELLED'
      )
    order by
      (candidate_version.status='READY') desc,
      (
        candidate_version.status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING'
        )
      ) desc,
      candidate_version.created_at_utc desc,
      candidate_version.id desc
    limit 1
  ) version on true
),
timesheet_support_agg as materialized (
  select
    invoice.invoice_id,
    count(*) filter(
      where support.timesheet_id is not null
        and support.required
        and support.is_manual
    )::integer manual_count,
    count(*) filter(
      where support.timesheet_id is not null
        and support.required
        and not support.is_manual
    )::integer electronic_count,
    count(*) filter(
      where support.timesheet_id is not null
        and support.required
        and coalesce(
          support.timesheet_document_state,
          'NOT_READY'
        )<>'READY'
    )::integer timesheet_not_ready_count,
    coalesce(sum(support.timesheet_document_pages)
      filter(where support.required),0)::integer timesheet_pages,
    coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',support.timesheet_id,
      'required',support.required,
      'submission_mode',coalesce(
        support.submission_mode::text,
        ''
      ),
      'manual_asset_state',support.manual_asset_state,
      'manual_asset_pages',support.manual_asset_pages,
      'timesheet_document_state',
        support.timesheet_document_state,
      'timesheet_document_pages',
        support.timesheet_document_pages,
      'active_timesheet_document_operation_id',
        support.active_timesheet_document_operation_id
    ) order by support.timesheet_id)
      filter(where support.timesheet_id is not null),
      '[]'::jsonb) timesheet_support_rows
  from base invoice
  left join timesheet_support support
    on support.invoice_id=invoice.invoice_id
  group by invoice.invoice_id
),
evidence_economics as materialized (
  select
    line.invoice_id,
    line.timesheet_id,
    bool_or(
      upper(coalesce(line.meta_json->>'line_type','')) in(
        'EXPENSE_MILEAGE','MILEAGE'
      )
      or coalesce(line.source_key,'') like '%:MILEAGE'
    ) mileage_required,
    bool_or(
      upper(coalesce(line.meta_json->>'line_type',''))
        like '%TRAVEL%'
    ) travel_required,
    bool_or(
      upper(coalesce(line.meta_json->>'line_type',''))
        like '%ACCOMMODATION%'
    ) accommodation_required,
    bool_or(
      upper(coalesce(line.meta_json->>'line_type',''))
        like 'EXPENSE_%'
      and upper(coalesce(line.meta_json->>'line_type',''))
        not in(
          'EXPENSE_MILEAGE','EXPENSE_TRAVEL',
          'EXPENSE_ACCOMMODATION'
        )
    ) general_expense_required
  from public.invoice_lines line
  join base invoice on invoice.invoice_id=line.invoice_id
  where line.timesheet_id is not null
  group by line.invoice_id,line.timesheet_id
),
evidence_rows as materialized (
  select distinct
    source.invoice_id,
    evidence.id evidence_id,
    evidence.timesheet_id,
    upper(coalesce(evidence.kind,'')) kind,
    evidence.document_asset_id,
    asset.status,
    coalesce(asset.normalised_page_count,0) pages,
    case
      when upper(coalesce(evidence.kind,''))='TIMESHEET'
        then coalesce(precheck.effective_ts_attach_to_invoice,true)
          and not coalesce(
            summary.client_no_timesheet_required,
            false
          )
          and not coalesce(summary.client_is_nhsp,false)
      when upper(coalesce(evidence.kind,''))='MILEAGE'
        then coalesce(economics.mileage_required,false)
      when upper(coalesce(evidence.kind,''))='TRAVEL'
        then coalesce(economics.travel_required,false)
      when upper(coalesce(evidence.kind,''))='ACCOMMODATION'
        then coalesce(economics.accommodation_required,false)
      when upper(coalesce(evidence.kind,'')) in(
        'OTHER','EXPENSE','EXPENSES'
      )
        then coalesce(
          economics.general_expense_required,
          false
        )
      else false
    end required
  from source_timesheets source
  join public.timesheet_evidence evidence
    on evidence.timesheet_id=source.timesheet_id
  left join public.v_ts_invoice_precheck precheck
    on precheck.timesheet_id=source.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=source.timesheet_id
  left join evidence_economics economics
    on economics.invoice_id=source.invoice_id
   and economics.timesheet_id=source.timesheet_id
  left join public.invoice_document_assets asset
    on asset.id=evidence.document_asset_id
),
evidence_agg as materialized (
  select
    invoice.invoice_id,
    count(evidence.evidence_id)
      filter(where evidence.required)::integer evidence_count,
    count(*) filter(
      where evidence.required
        and evidence.evidence_id is not null
        and evidence.document_asset_id is null
    )::integer unregistered_count,
    count(*) filter(
      where evidence.required
        and evidence.evidence_id is not null
        and evidence.document_asset_id is not null
        and coalesce(evidence.status,'DISCOVERED')
          not in(
            'READY','UNSUPPORTED','CORRUPT','MISSING','FAILED'
          )
    )::integer not_ready_count,
    count(*) filter(
      where evidence.required
        and evidence.status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED'
        )
    )::integer failed_count,
    coalesce(sum(evidence.pages)
      filter(where evidence.required),0)::integer evidence_pages
  from base invoice
  left join evidence_rows evidence
    on evidence.invoice_id=invoice.invoice_id
  group by invoice.invoice_id
),
hr_support as materialized (
  select
    invoice.invoice_id,
    count(source.source_system) filter(
      where upper(coalesce(source.source_system,''))
        ='HEALTHROSTER'
    )::integer healthroster_count,
    count(source.source_system) filter(
      where upper(coalesce(source.source_system,''))='NHSP'
    )::integer nhsp_count
  from base invoice
  left join public.invoice_hr_source_rows source
    on source.invoice_id=invoice.invoice_id
  group by invoice.invoice_id
),
line_flags as materialized (
  select
    invoice.invoice_id,
    count(line.id)::integer line_count,
    coalesce(bool_or(
      upper(coalesce(line.meta_json->>'line_type',''))
        like '%HIGHER_RATE%'
    ),false) higher_rate_required
  from base invoice
  left join public.invoice_lines line
    on line.invoice_id=invoice.invoice_id
  group by invoice.invoice_id
)
select
  (base.candidate_json->>'client_id')::uuid client_id,
  base.candidate_json->>'client_name' client_name,
  base.invoice_week_start,
  (base.candidate_json->>'week_ending_date')::date
    week_ending_date,
  jsonb_build_object(
    'invoice_id',base.invoice_id,
    'invoice_no',base.candidate_json->>'invoice_number',
    'status',base.candidate_json->>'invoice_status',
    'on_hold_reason',base.on_hold_reason,
    'subtotal_ex_vat',
      (base.candidate_json->>'total_ex_vat')::numeric,
    'vat_amount',
      (base.candidate_json->>'vat_amount')::numeric,
    'total_inc_vat',
      (base.candidate_json->>'total_inc_vat')::numeric,
    'currency',base.candidate_json->>'currency',
    'invoice_stream',base.candidate_json->>'invoice_stream',
    'is_self_bill',lower(coalesce(
      base.header_snapshot_json#>>'{meta,self_bill}',
      base.header_snapshot_json->>'self_bill',
      'false'
    )) in('true','t','1','yes'),
    'do_not_send',base.do_not_send,
    'document_revision',
      (base.candidate_json->>'document_revision')::bigint,
    'preview_document_state',
      base.candidate_json->>'preview_document_state',
    'stable_blocker_codes',
      base.candidate_json->'hard_blocker_codes',
    'document_dependency_codes',
      base.candidate_json->'document_dependency_codes',
    'delivery_blocker_codes',
      base.candidate_json->'delivery_blocker_codes',
    'can_issue_only',
      (base.candidate_json->>'can_issue_only')::boolean,
    'can_issue_and_deliver',
      (base.candidate_json->>'can_issue_and_deliver')::boolean,
    'validation_detail',
      base.candidate_json->'validation_detail',
    'estimated_supporting_page_count',
      evidence.evidence_pages
        +timesheet.timesheet_pages
        +hr.healthroster_count
        +hr.nhsp_count,
    'support_readiness',jsonb_build_object(
      'manual_timesheet_count',timesheet.manual_count,
      'electronic_timesheet_count',timesheet.electronic_count,
      'timesheet_not_ready_count',
        timesheet.timesheet_not_ready_count,
      'timesheets',timesheet.timesheet_support_rows,
      'evidence_count',evidence.evidence_count,
      'unregistered_asset_count',evidence.unregistered_count,
      'not_ready_asset_count',evidence.not_ready_count,
      'failed_asset_count',evidence.failed_count,
      'healthroster_count',hr.healthroster_count,
      'nhsp_count',hr.nhsp_count,
      'higher_rate_required',line.higher_rate_required
    ),
    'recipient_ready',not exists(
      select 1
      from jsonb_array_elements_text(
        coalesce(
          base.candidate_json->'delivery_blocker_codes',
          '[]'::jsonb
        )
      ) code(value)
      where code.value in(
        'MISSING_RECIPIENT','CONTRACT_MANUAL_EMAIL_MISSING',
        'CLIENT_MANUAL_EMAIL_MISSING',
        'CONTRACT_MANUAL_EMAIL_CONFLICT',
        'INVALID_TO_RECIPIENT','INVALID_CC_RECIPIENT',
        'INVALID_BCC_RECIPIENT'
      )
    ),
    'recipient',
      base.candidate_json#>'{_private,recipient}',
    'recipient_routing_warnings',
      base.candidate_json->'warning_codes',
    'active_issue_operation_id',
      base.candidate_json->>'active_issue_operation_id',
    'active_issue_operation',
      base.candidate_json->'active_issue_operation',
    'active_document_operation_id',
      base.active_document_operation_id,
    'last_issue_error',
      base.candidate_json->'last_issue_error',
    'last_document_error',
      base.last_document_error_json
  ) invoice_json
from base
join timesheet_support_agg timesheet
  on timesheet.invoice_id=base.invoice_id
join evidence_agg evidence on evidence.invoice_id=base.invoice_id
join hr_support hr on hr.invoice_id=base.invoice_id
join line_flags line on line.invoice_id=base.invoice_id
order by
  base.candidate_json->>'client_name' nulls last,
  base.candidate_json->>'week_ending_date' desc nulls last,
  base.candidate_json->>'invoice_number' nulls last,
  base.invoice_id;
$function$;

alter function private._invoice_batch_issue_source_rows_core_v2(
  boolean,integer,timestamptz,uuid[]
) owner to postgres;
revoke all on function private._invoice_batch_issue_source_rows_core_v2(
  boolean,integer,timestamptz,uuid[]
) from public,anon,authenticated;
grant execute on function private._invoice_batch_issue_source_rows_core_v2(
  boolean,integer,timestamptz,uuid[]
) to service_role;

create or replace function private._invoice_batch_issue_source_rows_v2(
  p_allow_early boolean default false,
  p_limit integer default null,
  p_now_utc timestamptz default now()
) returns table(
  client_id uuid,
  client_name text,
  invoice_week_start date,
  week_ending_date date,
  invoice_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
select
  source.client_id,
  source.client_name,
  source.invoice_week_start,
  source.week_ending_date,
  source.invoice_json
from private._invoice_batch_issue_source_rows_core_v2(
  p_allow_early,
  p_limit,
  p_now_utc,
  null::uuid[]
) source;
$function$;

alter function private._invoice_batch_issue_source_rows_v2(
  boolean,integer,timestamptz
) owner to postgres;
revoke all on function private._invoice_batch_issue_source_rows_v2(
  boolean,integer,timestamptz
) from public,anon,authenticated;
grant execute on function private._invoice_batch_issue_source_rows_v2(
  boolean,integer,timestamptz
) to service_role;
