create or replace function private._invoice_processor_limits()
returns jsonb
language sql
immutable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  select jsonb_build_object(
    'version','INVOICE_PROCESSOR_LIMITS_V4',
    'policy_version','INVOICE_PROCESSOR_LIMITS_V4',
    'context',jsonb_build_object(
      'ASSET_INSPECT',262144,
      'ASSET_NORMALISE',262144,
      'SOURCE_RENDER',524288,
      'INVOICE_CORE_RENDER',1048576,
      'PDF_MERGE',524288,
      'DOCUMENT_VERIFY',262144),
    'result',jsonb_build_object(
      'ASSET_INSPECT',524288,
      'ASSET_NORMALISE',524288,
      'SOURCE_RENDER',262144,
      'INVOICE_CORE_RENDER',262144,
      'PDF_MERGE',1048576,
      'DOCUMENT_VERIFY',524288),
    'asset',jsonb_build_object(
      'max_pixels',80000000,
      'max_decoded_bytes',536870912,
      'max_source_bytes',268435456,
      'allowed_media_types',jsonb_build_array(
        'application/pdf','image/jpeg','image/png'),
      'image_normalisation_profile','INVOICE_IMAGE_NORMALISATION_V1',
      'pdf_normalisation_profile','INVOICE_PDF_NORMALISATION_V1',
      'max_pdf_part_pages',100,
      'max_pdf_part_bytes',50331648,
      'max_part_input_bytes',67108864,
      'max_part_estimated_decoded_bytes',201326592),
    'merge',jsonb_build_object(
      'bin_packing_version','SEQUENTIAL_FIRST_FIT_V1',
      'max_inputs',24,
      'max_pages',2000,
      'max_input_bytes',536870912,
      'max_estimated_decoded_bytes',4294967296,
      'max_levels',16),
    'attachment_index',jsonb_build_object(
      'layout_version','BOUNDED_STABILISATION_V1',
      'renderer_version','INVOICE_ATTACHMENT_INDEX_RENDERER_V1',
      'template_version','invoice-attachment-index-v1',
      'css_font_identity','CLOUDTMS_INVOICE_CSS_FONT_V1',
      'page_geometry','A4_PORTRAIT_210X297MM',
      'locale_identity','en-GB_DDMMYYYY',
      'max_render_passes',3),
    'verify',jsonb_build_object(
      'object_receipt_contract','ACTUAL_BYTES_OBJECT_RECEIPT_V3',
      'logical_receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
      'merge_receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
      'document_root_receipt_contract','DOCUMENT_ROOT_RECEIPT_V3',
      'ordered_input_hash_contract','ACTUAL_ORDERED_INPUT_V1',
      'receipt_hash_algorithm','SHA256',
      'max_receipts',4096,
      'require_parse_success',true,
      'require_ordered_input_receipts',true,
      'require_logical_coverage_hash',true,
      'require_physical_receipt_hash',true),
    'delivery',jsonb_build_object(
      'max_attachments_per_message',30,
      'max_cumulative_attachment_bytes',20971520,
      'max_individual_attachment_bytes',10485760,
      'secure_link_threshold_bytes',20971520,
      'allowed_policies',jsonb_build_array('ATTACH','SPLIT','SECURE_LINK'))
  );
$function$;

revoke all on function private._invoice_processor_limits()
  from public,anon,authenticated;
grant execute on function private._invoice_processor_limits()
  to service_role;
