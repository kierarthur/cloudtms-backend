-- Fulfil an existing DOCUMENT_INPUT only from its exact T0-only original
-- completed input inside a verified original FINAL_ISSUE graph.
create or replace function private.invoice_credit_original_input_receipt_v1(p_frozen_source jsonb)
returns table(result_json jsonb,page_count integer,size_bytes bigint)
language sql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
  select input.result_json,input.actual_page_count,input.actual_byte_count
  from public.invoice_operation_chunks input
  join public.invoice_document_versions original on original.id=input.document_version_id
  where input.id=case when pg_input_is_valid(p_frozen_source->>'original_document_input_id','uuid')
      then(p_frozen_source->>'original_document_input_id')::uuid end
    and original.id=case when pg_input_is_valid(p_frozen_source->>'input_document_version_id','uuid')
      then(p_frozen_source->>'input_document_version_id')::uuid end
    and original.entity_type='INVOICE' and original.purpose='FINAL_ISSUE'
    and original.status='READY' and original.verified_at_utc is not null
    and input.entity_type='TIMESHEET'
    and input.entity_id=case when pg_input_is_valid(p_frozen_source->>'timesheet_id','uuid')
      then(p_frozen_source->>'timesheet_id')::uuid end
    and input.chunk_type='DOCUMENT_INPUT' and input.payload_json->>'input_type'='ELECTRONIC_TIMESHEET'
    and input.status='COMPLETE' and input.actual_page_count>0 and input.actual_byte_count>0
    and input.result_json is not distinct from p_frozen_source->'original_document_input_result'
    and input.payload_json->>'source_revision' is not distinct from p_frozen_source->>'original_document_source_revision'
    and input.result_json->>'sha256'~'^[0-9a-f]{64}$'
    and nullif(input.result_json->>'r2_key','') is not null;
$function$;
