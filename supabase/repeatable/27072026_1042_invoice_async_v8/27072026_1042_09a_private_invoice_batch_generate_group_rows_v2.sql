create or replace function private._invoice_batch_generate_group_rows_v2(
  p_allow_early boolean default false,
  p_limit integer default null,
  p_scope_keys text[] default null,
  p_now_utc timestamptz default now()
) returns table(
  client_id uuid,
  client_name text,
  group_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with classified as materialized (
  select
    candidate.selection_key,
    candidate.candidate_json
  from private._invoice_batch_generate_classification_v2(
    p_allow_early,
    p_scope_keys,
    p_now_utc
  ) candidate
  where candidate.candidate_json->>'row_kind'='CREATE_INVOICE'
  order by
    candidate.candidate_json->>'week_ending_date',
    candidate.candidate_json->>'client_id',
    candidate.candidate_json->>'scope_key'
  limit case
    when p_scope_keys is not null then 250
    when p_limit is null then null
    else greatest(1,p_limit)
  end
)
select
  (classified.candidate_json->>'client_id')::uuid client_id,
  classified.candidate_json->>'client_name' client_name,
  jsonb_build_object(
    'group_key',classified.candidate_json->>'scope_key',
    'invoice_week_start',
      classified.candidate_json#>>'{_private,target_invoice_week}',
    'week_ending_date',
      classified.candidate_json->>'week_ending_date',
    'subtotal_ex_vat',
      (classified.candidate_json->>'total_ex_vat')::numeric,
    'vat_amount',
      (classified.candidate_json->>'vat_amount')::numeric,
    'total_inc_vat',
      (classified.candidate_json->>'total_inc_vat')::numeric,
    'total_hours',coalesce(
      (classified.candidate_json#>>'{_private,total_hours}')::numeric,
      0
    ),
    'stream',classified.candidate_json->>'invoice_stream',
    'consolidation_mode',
      classified.candidate_json#>>'{_private,consolidation_mode}',
    'canonical_source_ids',
      classified.candidate_json#>'{_private,canonical_source_ids}',
    'canonical_source_members',
      classified.candidate_json#>'{_private,canonical_source_members}',
    'canonical_source_revision',
      classified.candidate_json->>'source_revision',
    'blocker_code',
      classified.candidate_json->>'primary_blocker_code',
    'blocker_codes',
      classified.candidate_json->'action_blocker_codes',
    'blocker_detail',
      classified.candidate_json#>'{_private,blocker_detail}',
    'correction_validation',
      classified.candidate_json#>'{_private,correction_validation}',
    'document_dependencies',coalesce((
      select jsonb_agg(jsonb_build_object(
        'timesheet_id',timesheet.value->>'timesheet_id',
        'code','TIMESHEET_DOCUMENT_NOT_READY'
      ) order by timesheet.value->>'timesheet_id')
      from jsonb_array_elements(
        coalesce(
          classified.candidate_json#>'{_private,timesheets}',
          '[]'::jsonb
        )
      ) timesheet(value)
      where coalesce(
        (timesheet.value->>'timesheet_document_ready')::boolean,
        false
      ) is false
    ),'[]'::jsonb),
    'command_payload',
      classified.candidate_json->'command_payload',
    'timesheets',coalesce(
      classified.candidate_json#>'{_private,timesheets}',
      '[]'::jsonb
    ),
    'active_generation_operation_id',
      classified.candidate_json->>'active_operation_id',
    'active_generation_status',
      classified.candidate_json->>'active_operation_status',
    'active_generation_progress',
      classified.candidate_json#>'{_private,active_progress}',
    'last_generation_error',
      classified.candidate_json#>'{_private,active_error}',
    'retry_available',
      classified.candidate_json->>'active_operation_status'
        in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
  ) group_json
from classified;
$function$;

alter function private._invoice_batch_generate_group_rows_v2(
  boolean,integer,text[],timestamptz
) owner to postgres;
revoke all on function private._invoice_batch_generate_group_rows_v2(
  boolean,integer,text[],timestamptz
) from public,anon,authenticated;
grant execute on function private._invoice_batch_generate_group_rows_v2(
  boolean,integer,text[],timestamptz
) to service_role;
