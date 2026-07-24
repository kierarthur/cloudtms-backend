-- CloudTMS invoice asynchronous processor contract V4.
-- TEST hard cutover: PDF, JPEG and static PNG only; verification is in-place.
begin;

do $block$
declare v_active_v3 bigint;
begin
  select count(*) into v_active_v3
  from public.invoice_operations o
  where o.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    and o.config_json#>>'{processor_policy,version}' = 'INVOICE_PROCESSOR_LIMITS_V3';
  if v_active_v3 <> 0 then
    raise exception using errcode='55000',
      message='INVOICE_PROCESSOR_V3_ACTIVE_OPERATIONS_PRESENT',
      detail=format('%s active V3 operation(s) must finish or be safely cancelled before V4',v_active_v3);
  end if;
end
$block$;

alter table public.invoice_operations drop constraint if exists invoice_operations_json_ck;
alter table public.invoice_operations add constraint invoice_operations_json_ck check (
  jsonb_typeof(input_json)='object' and jsonb_typeof(config_json)='object'
  and jsonb_typeof(progress_json)='object'
  and jsonb_typeof(config_json->'processor_policy')='object'
  and config_json#>>'{processor_policy,version}'='INVOICE_PROCESSOR_LIMITS_V4'
  and config_json#>>'{processor_policy,policy_version}'='INVOICE_PROCESSOR_LIMITS_V4'
  and jsonb_typeof(config_json#>'{processor_policy,context}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,result}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,asset}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,merge}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,attachment_index}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,verify}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,delivery}')='object'
  and config_json#>'{processor_policy,asset,allowed_media_types}'='["application/pdf","image/jpeg","image/png"]'::jsonb
  and config_json#>>'{processor_policy,merge,bin_packing_version}'='SEQUENTIAL_FIRST_FIT_V1'
  and config_json#>>'{processor_policy,asset,image_normalisation_profile}'='INVOICE_IMAGE_NORMALISATION_V1'
  and config_json#>>'{processor_policy,asset,pdf_normalisation_profile}'='INVOICE_PDF_NORMALISATION_V1'
  and config_json#>>'{processor_policy,verify,object_receipt_contract}'='ACTUAL_BYTES_OBJECT_RECEIPT_V3'
  and config_json#>>'{processor_policy,verify,logical_receipt_contract}'='LOGICAL_SOURCE_RECEIPT_V3'
  and config_json#>>'{processor_policy,verify,merge_receipt_contract}'='ACTUAL_BYTES_MERGE_RECEIPT_V3'
  and config_json#>>'{processor_policy,verify,document_root_receipt_contract}'='DOCUMENT_ROOT_RECEIPT_V3'
  and config_json#>>'{processor_policy,verify,ordered_input_hash_contract}'='ACTUAL_ORDERED_INPUT_V1'
  and config_json#>>'{processor_policy,verify,receipt_hash_algorithm}'='SHA256'
  and config_json#>'{processor_policy,delivery,allowed_policies}'='["ATTACH","SPLIT","SECURE_LINK"]'::jsonb
  and (config_json#>>'{processor_policy,verify,require_parse_success}')::boolean
  and (config_json#>>'{processor_policy,verify,require_ordered_input_receipts}')::boolean
  and (config_json#>>'{processor_policy,verify,require_logical_coverage_hash}')::boolean
  and (config_json#>>'{processor_policy,verify,require_physical_receipt_hash}')::boolean
  and (config_json#>>'{processor_policy,context,ASSET_INSPECT}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,context,ASSET_NORMALISE}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,context,SOURCE_RENDER}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,context,INVOICE_CORE_RENDER}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,context,PDF_MERGE}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,context,DOCUMENT_VERIFY}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,result,ASSET_INSPECT}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,result,ASSET_NORMALISE}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,result,SOURCE_RENDER}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,result,INVOICE_CORE_RENDER}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,result,PDF_MERGE}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,result,DOCUMENT_VERIFY}')::bigint between 1 and 16777216
  and (config_json#>>'{processor_policy,asset,max_pixels}')::bigint between 1 and 1000000000
  and (config_json#>>'{processor_policy,asset,max_decoded_bytes}')::bigint between 1 and 4294967296
  and (config_json#>>'{processor_policy,asset,max_source_bytes}')::bigint between 1 and 2147483648
  and (config_json#>>'{processor_policy,asset,max_pdf_part_pages}')::bigint between 1 and 10000
  and (config_json#>>'{processor_policy,asset,max_pdf_part_bytes}')::bigint between 1 and 1073741824
  and (config_json#>>'{processor_policy,asset,max_part_input_bytes}')::bigint between 1 and 2147483648
  and (config_json#>>'{processor_policy,asset,max_part_estimated_decoded_bytes}')::bigint between 1 and 4294967296
  and (config_json#>>'{processor_policy,merge,max_inputs}')::bigint between 1 and 1000
  and (config_json#>>'{processor_policy,merge,max_pages}')::bigint between 1 and 10000
  and (config_json#>>'{processor_policy,merge,max_input_bytes}')::bigint between 1 and 2147483648
  and (config_json#>>'{processor_policy,merge,max_estimated_decoded_bytes}')::bigint between 1 and 8589934592
  and (config_json#>>'{processor_policy,merge,max_levels}')::integer between 1 and 32
  and (config_json#>>'{processor_policy,attachment_index,max_render_passes}')::integer between 1 and 3
  and (config_json#>>'{processor_policy,verify,max_receipts}')::bigint between 1 and 10000
  and (config_json#>>'{processor_policy,delivery,max_attachments_per_message}')::bigint between 1 and 1000
  and (config_json#>>'{processor_policy,delivery,max_cumulative_attachment_bytes}')::bigint between 1 and 1073741824
  and (config_json#>>'{processor_policy,delivery,max_individual_attachment_bytes}')::bigint between 1 and 1073741824
  and (config_json#>>'{processor_policy,delivery,secure_link_threshold_bytes}')::bigint between 1 and 2147483648
  and (result_json is null or jsonb_typeof(result_json)='object')
  and (error_json is null or (jsonb_typeof(error_json)='object'
    and (not(error_json?'history') or (jsonb_typeof(error_json->'history')='array'
      and jsonb_array_length(error_json->'history')<=8))))
);
comment on constraint invoice_operations_json_ck on public.invoice_operations is
  'Requires the immutable CloudTMS invoice processor V4 policy and bounded JSON contracts.';
commit;

