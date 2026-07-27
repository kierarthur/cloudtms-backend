create or replace function private._invoice_batch_issue_classification_v2(
  p_allow_early boolean default false,
  p_invoice_ids uuid[] default null,
  p_now_utc timestamptz default now()
) returns table(
  selection_key text,
  candidate_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with
anchor as materialized (
  select (
    coalesce(p_now_utc,statement_timestamp())
    at time zone 'Europe/London'
  )::date today
),
base as materialized (
  select
    invoice.*,
    client.name client_name,
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
    end invoice_week_start,
    lower(coalesce(
      invoice.header_snapshot_json#>>'{meta,self_bill}',
      invoice.header_snapshot_json->>'self_bill',
      'false'
    )) in('true','t','1','yes') is_self_bill
  from public.invoices invoice
  join public.clients client on client.id=invoice.client_id
  where invoice.type::text='INVOICE'
    and invoice.status::text in('DRAFT','ON_HOLD')
    and (
      p_invoice_ids is null
      or invoice.id=any(p_invoice_ids)
    )
),
validation_requests as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key','candidate:'||invoice.id::text,
    'invoice_id',invoice.id,
    'expected_revision',invoice.document_revision,
    'allow_early',coalesce(p_allow_early,false),
    'deliver',true
  ) order by invoice.id),'[]'::jsonb) commands
  from base invoice
),
validations as materialized (
  select validation.*
  from validation_requests request
  cross join lateral private._invoice_issue_validate_batch(
    request.commands,
    (select today from anchor)
  ) validation
),
candidate_projection as materialized (
  select
    invoice.id invoice_id,
    coalesce(jsonb_agg(
      distinct to_jsonb(summary.candidate_id)
      order by to_jsonb(summary.candidate_id)
    )
      filter(where summary.candidate_id is not null),'[]'::jsonb)
      candidate_ids,
    coalesce(jsonb_agg(
      distinct to_jsonb(summary.candidate_name)
      order by to_jsonb(summary.candidate_name)
    )
      filter(where nullif(summary.candidate_name,'') is not null),
      '[]'::jsonb) candidate_names,
    coalesce(jsonb_agg(distinct to_jsonb(summary.week_ending_date)
      order by to_jsonb(summary.week_ending_date))
      filter(where summary.week_ending_date is not null),
      '[]'::jsonb) week_ending_dates,
    min(summary.week_ending_date) min_week_ending,
    max(summary.week_ending_date) max_week_ending
  from base invoice
  left join public.invoice_lines line on line.invoice_id=invoice.id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=line.timesheet_id
  group by invoice.id
),
active_issue as materialized (
  select distinct on(chunk.entity_id)
    chunk.entity_id invoice_id,
    chunk.operation_id,
    chunk.id chunk_id,
    chunk.status,
    chunk.phase,
    chunk.progress_json,
    chunk.error_json,
    operation.change_seq
  from public.invoice_operation_chunks chunk
  join public.invoice_operations operation
    on operation.id=chunk.operation_id
  join base invoice on invoice.id=chunk.entity_id
  where chunk.chunk_type='ISSUE_INVOICE'
    and chunk.entity_type='INVOICE'
    and chunk.status in(
      'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
    )
    and coalesce(
      chunk.payload_json->>'is_selection_expander',
      'false'
    )<>'true'
    and (not chunk.is_manifest_member or chunk.manifest_committed)
    and (
      not chunk.is_manifest_member
      or coalesce(chunk.entity_type,'')<>'OPERATION'
    )
  order by
    chunk.entity_id,
    chunk.updated_at_utc desc,
    chunk.id desc
),
classified as materialized (
  select
    invoice.*,
    invoice.invoice_week_start+6 week_ending_date,
    coalesce(validation.hard_blocker_codes,'[]'::jsonb)
      hard_blocker_codes,
    coalesce(validation.warning_codes,'[]'::jsonb) warning_codes,
    coalesce(validation.document_dependency_codes,'[]'::jsonb)
      document_dependency_codes,
    coalesce(validation.delivery_blocker_codes,'[]'::jsonb)
      delivery_blocker_codes,
    coalesce(validation.can_issue_only,false) can_issue_only,
    coalesce(validation.can_issue_and_deliver,false)
      can_issue_and_deliver,
    validation.detail_json validation_detail,
    validation.route_policy_result->'canonical_to' recipient,
    projection.candidate_ids,
    projection.candidate_names,
    projection.week_ending_dates,
    projection.min_week_ending,
    projection.max_week_ending,
    active.operation_id current_issue_operation_id,
    active.chunk_id current_issue_chunk_id,
    active.status current_issue_status,
    active.phase current_issue_phase,
    active.progress_json current_issue_progress,
    active.error_json current_issue_error,
    active.change_seq current_issue_change_seq,
    case
      when exists(
        select 1
        from public.invoice_document_versions version
        where version.entity_type='INVOICE'
          and version.entity_id=invoice.id
          and version.purpose='DRAFT_PREVIEW'
          and version.source_revision=
            invoice.document_revision::text
          and version.template_version='invoice-professional-v1'
          and version.status='READY'
          and version.r2_key is not null
          and version.sha256~'^[0-9a-f]{64}$'
          and coalesce(version.size_bytes,0)>0
          and coalesce(version.page_count,0)>0
      )
        then 'FRESH'
      when exists(
        select 1
        from public.invoice_document_versions version
        where version.entity_type='INVOICE'
          and version.entity_id=invoice.id
          and version.purpose='DRAFT_PREVIEW'
          and version.template_version='invoice-professional-v1'
          and version.status='READY'
          and version.r2_key is not null
          and version.sha256~'^[0-9a-f]{64}$'
          and coalesce(version.size_bytes,0)>0
          and coalesce(version.page_count,0)>0
      )
        then case
          when invoice.active_document_operation_id is not null
            then 'ACTIVE'
          when upper(coalesce(invoice.document_state,''))='FAILED'
            or invoice.last_document_error_json is not null
            then 'FAILED'
          else 'STALE'
        end
      else 'NEVER_GENERATED'
    end generated_state
  from base invoice
  left join validations validation
    on validation.request_key='candidate:'||invoice.id::text
   and validation.invoice_id=invoice.id
  join candidate_projection projection
    on projection.invoice_id=invoice.id
  left join active_issue active on active.invoice_id=invoice.id
),
candidate_state as materialized (
  select
    classified.*,
    coalesce(
      jsonb_array_length(classified.delivery_blocker_codes)>0
      and classified.can_issue_only
      and not classified.can_issue_and_deliver,
      false
    ) blocked_for_sending,
    (
      case
        when classified.generated_state='STALE'
          then jsonb_build_array('STALE')
        else '[]'::jsonb
      end
      ||case
        when classified.generated_state='FAILED'
          then jsonb_build_array('FAILED_RENDER')
        else '[]'::jsonb
      end
      ||case
        when classified.generated_state='ACTIVE'
          then jsonb_build_array('GENERATING')
        else '[]'::jsonb
      end
      ||classified.hard_blocker_codes
      ||classified.document_dependency_codes
    ) issue_blocker_codes,
    (
      case
        when jsonb_array_length(
          classified.delivery_blocker_codes
        )>0
         and classified.can_issue_only
         and not classified.can_issue_and_deliver
          then jsonb_build_array('BLOCKED_FOR_SENDING')
        else '[]'::jsonb
      end
      ||classified.warning_codes
    ) informational_codes
  from classified
),
final_rows as materialized (
  select
    state.*,
    (
      state.generated_state='FRESH'
      and state.can_issue_only
      and jsonb_array_length(state.hard_blocker_codes)=0
      and jsonb_array_length(state.document_dependency_codes)=0
      and state.current_issue_operation_id is null
    ) selectable,
    case
      when state.current_issue_operation_id is not null
        then 'IN_PROGRESS'
      when state.generated_state='STALE'
        then 'STALE'
      when state.generated_state='FAILED'
        then 'FAILED'
      when state.generated_state='ACTIVE'
        then 'IN_PROGRESS'
      when not state.can_issue_only
        or jsonb_array_length(state.hard_blocker_codes)>0
        or jsonb_array_length(
          state.document_dependency_codes
        )>0
        then 'BLOCKED'
      else 'READY'
    end row_status
  from candidate_state state
  where state.generated_state<>'NEVER_GENERATED'
    and state.status::text in('DRAFT','ON_HOLD')
    and nullif(state.invoice_no,'') is not null
)
select
  'invoice:'||invoice.id::text selection_key,
  jsonb_build_object(
    'selection_key','invoice:'||invoice.id::text,
    'invoice_id',invoice.id,
    'invoice_number',invoice.invoice_no,
    'source_revision',invoice.document_revision::text,
    'document_revision',invoice.document_revision,
    'client_id',invoice.client_id,
    'client_name',invoice.client_name,
    'candidate_ids',invoice.candidate_ids,
    'candidate_names',invoice.candidate_names,
    'candidate_display',case
      when jsonb_array_length(invoice.candidate_names)=1
        then invoice.candidate_names->>0
      when jsonb_array_length(invoice.candidate_names)>1
        then 'Multiple candidates ('||
          jsonb_array_length(invoice.candidate_names)::text||
          ')'
      else 'Unknown candidate'
    end,
    'week_ending_dates',case
      when jsonb_array_length(invoice.week_ending_dates)>0
        then invoice.week_ending_dates
      else jsonb_build_array(invoice.week_ending_date)
    end,
    'week_ending_date',coalesce(
      invoice.min_week_ending,
      invoice.week_ending_date
    ),
    'currency',coalesce(
      nullif(invoice.header_snapshot_json#>>'{meta,currency}',''),
      nullif(invoice.header_snapshot_json->>'currency',''),
      'GBP'
    ),
    'invoice_stream',upper(coalesce(
      nullif(
        invoice.header_snapshot_json#>>'{meta,invoice_stream}',
        ''
      ),
      nullif(invoice.header_snapshot_json->>'invoice_stream',''),
      case when invoice.is_self_bill then 'SELF_BILL' end,
      'NORMAL'
    )),
    'total_ex_vat',round(coalesce(invoice.subtotal_ex_vat,0),2),
    'vat_amount',round(coalesce(invoice.vat_amount,0),2),
    'total_inc_vat',round(coalesce(
      invoice.total_inc_vat,
      coalesce(invoice.subtotal_ex_vat,0)
        +coalesce(invoice.vat_amount,0)
    ),2),
    'preview_document_state',
      upper(coalesce(invoice.document_state,'')),
    'invoice_status',upper(coalesce(invoice.status::text,'')),
    'hard_blocker_codes',invoice.hard_blocker_codes,
    'document_dependency_codes',
      invoice.document_dependency_codes,
    'delivery_blocker_codes',invoice.delivery_blocker_codes,
    'warning_codes',invoice.warning_codes,
    'issue_blocker_codes',invoice.issue_blocker_codes,
    'informational_codes',invoice.informational_codes,
    'can_issue_only',invoice.can_issue_only,
    'can_issue_and_deliver',invoice.can_issue_and_deliver,
    'validation_detail',invoice.validation_detail,
    'support_readiness',null,
    'generated_state',invoice.generated_state,
    'blocked_for_sending',invoice.blocked_for_sending,
    'selectable',invoice.selectable,
    'row_status',invoice.row_status,
    'is_early',coalesce(
      invoice.max_week_ending,
      invoice.week_ending_date
    ) >= (select today from anchor),
    'active_issue_operation_id',invoice.current_issue_operation_id,
    'active_issue_operation',case
      when invoice.current_issue_operation_id is not null
        then jsonb_build_object(
          'id',invoice.current_issue_operation_id,
          'chunk_id',invoice.current_issue_chunk_id,
          'status',invoice.current_issue_status,
          'phase',invoice.current_issue_phase,
          'progress',invoice.current_issue_progress,
          'error',invoice.current_issue_error,
          'change_seq',invoice.current_issue_change_seq
        )
    end,
    'active_document_operation_id',
      invoice.active_document_operation_id,
    'last_issue_error',invoice.current_issue_error,
    'last_document_error',invoice.last_document_error_json,
    '_private',jsonb_build_object(
      'recipient',invoice.recipient,
      'routing_warnings',invoice.warning_codes
    )
  ) candidate_json
from final_rows invoice
order by invoice.client_name nulls last,
  invoice.week_ending_date desc nulls last,
  invoice.invoice_no nulls last,
  invoice.id;
$function$;

alter function private._invoice_batch_issue_classification_v2(
  boolean,uuid[],timestamptz
) owner to postgres;
revoke all on function private._invoice_batch_issue_classification_v2(
  boolean,uuid[],timestamptz
) from public,anon,authenticated;
grant execute on function private._invoice_batch_issue_classification_v2(
  boolean,uuid[],timestamptz
) to service_role;
