-- CloudTMS Invoice Operation Queue - Stage 1 schema migration
-- Generated 23/07/2026 Europe/London. TEST target only. Not executed.
begin;
create extension if not exists pgcrypto with schema extensions;
create schema if not exists private;
revoke all on schema private from public,anon,authenticated;
grant usage on schema private to postgres,service_role;

create sequence if not exists public.invoice_operation_change_seq as bigint;
revoke all on sequence public.invoice_operation_change_seq from public,anon,authenticated;
grant usage,select on sequence public.invoice_operation_change_seq to postgres,service_role;

create table public.invoice_operations(
 id uuid primary key default gen_random_uuid(),
 parent_operation_id uuid references public.invoice_operations(id) on delete set null,
 operation_type text not null,
 entity_type text,entity_id uuid,
 actor_user_id uuid references public.tms_users(id) on delete set null,
 idempotency_key text not null,status text not null default 'QUEUED',phase text not null default 'SUBMITTED',
 priority integer not null default 200,source_revision text,template_version text,
 input_json jsonb not null default '{}',config_json jsonb not null default '{}',
 progress_json jsonb not null default '{}',result_json jsonb,error_json jsonb,
 total_units integer not null default 0,completed_units integer not null default 0,
 failed_units integer not null default 0,chunk_count integer not null default 0,
 control_version bigint not null default 1,
 change_seq bigint not null default nextval('public.invoice_operation_change_seq'),
 requires_user_action boolean not null default false,
 created_at_utc timestamptz not null default now(),started_at_utc timestamptz,
 updated_at_utc timestamptz not null default now(),completed_at_utc timestamptz,failed_at_utc timestamptz,
 constraint invoice_operations_type_ck check(operation_type in(
  'GENERATE_INVOICES','BUILD_DOCUMENT','PREPARE_ASSET','ISSUE_INVOICES',
  'DELIVER_INVOICES','RECONCILE_INVOICE_WORK')),
 constraint invoice_operations_status_ck check(status in(
  'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE','FAILED',
  'DEAD_LETTER','CANCELLED','SUPERSEDED')),
 constraint invoice_operations_priority_ck check(priority between 0 and 2000),
 constraint invoice_operations_counts_ck check(total_units>=0 and completed_units>=0 and failed_units>=0
  and completed_units+failed_units<=total_units and chunk_count>=0 and control_version>0 and change_seq>0
  and(total_units>0 or(completed_units=0 and failed_units=0 and phase in('SUBMITTED','PLANNING','VALIDATING')))),
 constraint invoice_operations_json_ck check(jsonb_typeof(input_json)='object'
  and jsonb_typeof(config_json)='object' and jsonb_typeof(progress_json)='object'
  and jsonb_typeof(config_json->'processor_policy')='object'
  and config_json#>>'{processor_policy,version}'='INVOICE_PROCESSOR_LIMITS_V3'
  and config_json#>>'{processor_policy,policy_version}'='INVOICE_PROCESSOR_LIMITS_V3'
  and jsonb_typeof(config_json#>'{processor_policy,context}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,result}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,asset}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,merge}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,attachment_index}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,verify}')='object'
  and jsonb_typeof(config_json#>'{processor_policy,delivery}')='object'
  and config_json#>>'{processor_policy,merge,bin_packing_version}'
    ='SEQUENTIAL_FIRST_FIT_V1'
  and config_json#>>'{processor_policy,asset,image_normalisation_profile}'
    ='INVOICE_IMAGE_NORMALISATION_V1'
  and config_json#>>'{processor_policy,asset,pdf_normalisation_profile}'
    ='INVOICE_PDF_NORMALISATION_V1'
  and config_json#>'{processor_policy,asset,allowed_media_types}'=
    '["application/pdf","image/jpeg","image/png","image/webp","image/tiff","image/heic","image/heif"]'::jsonb
  and config_json#>>'{processor_policy,attachment_index,layout_version}'
    ='BOUNDED_STABILISATION_V1'
  and config_json#>>'{processor_policy,attachment_index,renderer_version}'
    ='INVOICE_ATTACHMENT_INDEX_RENDERER_V1'
  and config_json#>>'{processor_policy,attachment_index,template_version}'
    ='invoice-attachment-index-v1'
  and config_json#>>'{processor_policy,attachment_index,css_font_identity}'
    ='CLOUDTMS_INVOICE_CSS_FONT_V1'
  and config_json#>>'{processor_policy,attachment_index,page_geometry}'
    ='A4_PORTRAIT_210X297MM'
  and config_json#>>'{processor_policy,attachment_index,locale_identity}'
    ='en-GB_DDMMYYYY'
  and config_json#>>'{processor_policy,verify,receipt_schema_version}'
    ='INVOICE_MERGE_RECEIPT_V1'
  and config_json#>>'{processor_policy,verify,receipt_hash_algorithm}'='SHA256'
  and config_json#>'{processor_policy,delivery,allowed_policies}'=
    '["ATTACH","SPLIT","SECURE_LINK"]'::jsonb
  and jsonb_typeof(config_json#>'{processor_policy,context,ASSET_INSPECT}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,context,ASSET_NORMALISE}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,context,SOURCE_RENDER}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,context,INVOICE_CORE_RENDER}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,context,PDF_MERGE}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,context,DOCUMENT_VERIFY}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,result,ASSET_INSPECT}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,result,ASSET_NORMALISE}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,result,SOURCE_RENDER}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,result,INVOICE_CORE_RENDER}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,result,PDF_MERGE}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,result,DOCUMENT_VERIFY}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,asset,max_pixels}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,asset,max_decoded_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,asset,max_source_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,asset,max_pdf_part_pages}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,asset,max_pdf_part_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,asset,max_part_input_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,asset,max_part_estimated_decoded_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,merge,max_inputs}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,merge,max_pages}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,merge,max_input_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,merge,max_estimated_decoded_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,merge,max_levels}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,attachment_index,max_render_passes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,verify,max_receipts}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,delivery,max_attachments_per_message}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,delivery,max_cumulative_attachment_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,delivery,max_individual_attachment_bytes}')='number'
  and jsonb_typeof(config_json#>'{processor_policy,delivery,secure_link_threshold_bytes}')='number'
  and case when coalesce(config_json#>>'{processor_policy,context,ASSET_INSPECT}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,context,ASSET_INSPECT}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,context,ASSET_NORMALISE}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,context,ASSET_NORMALISE}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,context,SOURCE_RENDER}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,context,SOURCE_RENDER}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,context,INVOICE_CORE_RENDER}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,context,INVOICE_CORE_RENDER}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,context,PDF_MERGE}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,context,PDF_MERGE}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,context,DOCUMENT_VERIFY}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,context,DOCUMENT_VERIFY}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,result,ASSET_INSPECT}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,result,ASSET_INSPECT}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,result,ASSET_NORMALISE}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,result,ASSET_NORMALISE}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,result,SOURCE_RENDER}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,result,SOURCE_RENDER}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,result,INVOICE_CORE_RENDER}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,result,INVOICE_CORE_RENDER}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,result,PDF_MERGE}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,result,PDF_MERGE}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,result,DOCUMENT_VERIFY}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,result,DOCUMENT_VERIFY}')::bigint<=16777216
    else false end
  and case when coalesce(config_json#>>'{processor_policy,asset,max_pixels}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,asset,max_pixels}')::bigint<=1000000000
    else false end
  and case when coalesce(config_json#>>'{processor_policy,asset,max_decoded_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,asset,max_decoded_bytes}')::bigint<=4294967296
    else false end
  and case when coalesce(config_json#>>'{processor_policy,asset,max_source_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,asset,max_source_bytes}')::bigint<=2147483648
    else false end
  and case when coalesce(config_json#>>'{processor_policy,asset,max_pdf_part_pages}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,asset,max_pdf_part_pages}')::bigint<=10000
    else false end
  and case when coalesce(config_json#>>'{processor_policy,asset,max_pdf_part_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,asset,max_pdf_part_bytes}')::bigint<=1073741824
    else false end
  and case when coalesce(config_json#>>'{processor_policy,asset,max_part_input_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,asset,max_part_input_bytes}')::bigint<=2147483648
    else false end
  and case when coalesce(config_json#>>'{processor_policy,asset,max_part_estimated_decoded_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,asset,max_part_estimated_decoded_bytes}')::bigint<=4294967296
    else false end
  and case when coalesce(config_json#>>'{processor_policy,merge,max_inputs}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,merge,max_inputs}')::bigint<=1000
    else false end
  and case when coalesce(config_json#>>'{processor_policy,merge,max_pages}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,merge,max_pages}')::bigint<=10000
    else false end
  and case when coalesce(config_json#>>'{processor_policy,merge,max_input_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,merge,max_input_bytes}')::bigint<=2147483648
    else false end
  and case when coalesce(config_json#>>'{processor_policy,merge,max_estimated_decoded_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,merge,max_estimated_decoded_bytes}')::bigint<=8589934592
    else false end
  and case when coalesce(config_json#>>'{processor_policy,merge,max_levels}','')
      ~'^[1-9][0-9]{0,2}$'
    then(config_json#>>'{processor_policy,merge,max_levels}')::integer between 1 and 32
    else false end
  and case when coalesce(config_json#>>'{processor_policy,attachment_index,max_render_passes}','')
      ~'^[1-9][0-9]{0,2}$'
    then(config_json#>>'{processor_policy,attachment_index,max_render_passes}')::integer between 1 and 3
    else false end
  and case when coalesce(config_json#>>'{processor_policy,verify,max_receipts}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,verify,max_receipts}')::bigint<=10000
    else false end
  and jsonb_typeof(config_json#>'{processor_policy,verify,require_parse_success}')='boolean'
  and jsonb_typeof(config_json#>'{processor_policy,verify,require_ordered_input_receipts}')='boolean'
  and jsonb_typeof(config_json#>'{processor_policy,verify,require_logical_coverage_hash}')='boolean'
  and jsonb_typeof(config_json#>'{processor_policy,verify,require_physical_receipt_hash}')='boolean'
  and (config_json#>>'{processor_policy,verify,require_parse_success}')::boolean
  and (config_json#>>'{processor_policy,verify,require_ordered_input_receipts}')::boolean
  and (config_json#>>'{processor_policy,verify,require_logical_coverage_hash}')::boolean
  and (config_json#>>'{processor_policy,verify,require_physical_receipt_hash}')::boolean
  and case when coalesce(config_json#>>'{processor_policy,delivery,max_attachments_per_message}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,delivery,max_attachments_per_message}')::bigint<=1000
    else false end
  and case when coalesce(config_json#>>'{processor_policy,delivery,max_cumulative_attachment_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,delivery,max_cumulative_attachment_bytes}')::bigint<=1073741824
    else false end
  and case when coalesce(config_json#>>'{processor_policy,delivery,max_individual_attachment_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,delivery,max_individual_attachment_bytes}')::bigint<=1073741824
    else false end
  and case when coalesce(config_json#>>'{processor_policy,delivery,secure_link_threshold_bytes}','')
      ~'^[1-9][0-9]{0,15}$'
    then(config_json#>>'{processor_policy,delivery,secure_link_threshold_bytes}')::bigint<=2147483648
    else false end
  and(result_json is null or jsonb_typeof(result_json)='object')
  and(error_json is null or(jsonb_typeof(error_json)='object'
   and(not(error_json?'history') or(jsonb_typeof(error_json->'history')='array'
    and jsonb_array_length(error_json->'history')<=8)))))
);
create unique index uq_invoice_operations_active_idempotency
on public.invoice_operations(idempotency_key)
where status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');
create unique index uq_invoice_operations_delivery_identity
on public.invoice_operations(idempotency_key)
where operation_type='DELIVER_INVOICES'
  and status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE');
create index idx_invoice_operations_claim on public.invoice_operations(status,priority desc,created_at_utc,id);
create index idx_invoice_operations_entity_active on public.invoice_operations(entity_type,entity_id,operation_type,status)
 where status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');
create index idx_invoice_operations_entity_status on public.invoice_operations(entity_type,entity_id,status);
create index idx_invoice_operations_parent on public.invoice_operations(parent_operation_id);
create index idx_invoice_operations_type_status on public.invoice_operations(operation_type,status);
create index idx_invoice_operations_change_seq on public.invoice_operations(change_seq);
create index idx_invoice_operations_updated on public.invoice_operations(updated_at_utc desc);

create table public.invoice_document_assets(
 id uuid primary key default gen_random_uuid(),source_kind text not null,source_id uuid not null,
 source_revision text not null,original_r2_key text not null,original_filename text,
 declared_media_type text,detected_media_type text,original_sha256 text,original_size_bytes bigint,
 width_pixels integer,height_pixels integer,orientation_degrees integer,source_page_count integer,
 is_encrypted boolean,status text not null default 'DISCOVERED',
 normalised_manifest_json jsonb not null default '[]',normalised_manifest_hash text,
 normalised_r2_key text,normalised_sha256 text,
 normalised_size_bytes bigint,normalised_page_count integer,
 operation_id uuid references public.invoice_operations(id) on delete set null,
 error_json jsonb,created_at_utc timestamptz not null default now(),
 updated_at_utc timestamptz not null default now(),ready_at_utc timestamptz,
 constraint invoice_document_assets_status_ck check(status in(
  'DISCOVERED','INSPECTING','NORMALISING','READY','UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')),
 constraint invoice_document_assets_counts_ck check(
  (original_size_bytes is null or original_size_bytes>=0) and(width_pixels is null or width_pixels>0)
  and(height_pixels is null or height_pixels>0) and(source_page_count is null or source_page_count>=0)
  and(normalised_size_bytes is null or normalised_size_bytes>=0)
  and(normalised_page_count is null or normalised_page_count>=0)),
 constraint invoice_document_assets_rotation_ck check(orientation_degrees is null or orientation_degrees in(0,90,180,270)),
 constraint invoice_document_assets_manifest_array_ck check(jsonb_typeof(normalised_manifest_json)='array'),
 constraint invoice_document_assets_hash_format_ck check(
  (original_sha256 is null or original_sha256 ~ '^[0-9a-f]{64}$')
  and(normalised_sha256 is null or normalised_sha256 ~ '^[0-9a-f]{64}$')
  and(normalised_manifest_hash is null
    or normalised_manifest_hash ~ '^[0-9a-f]{64}$')),
 constraint invoice_document_assets_error_history_ck check(error_json is null or(jsonb_typeof(error_json)='object'
  and(not(error_json?'history') or(jsonb_typeof(error_json->'history')='array'
   and jsonb_array_length(error_json->'history')<=8)))),
 constraint invoice_document_assets_ready_ck check(status<>'READY' or(
  normalised_size_bytes>0 and normalised_page_count>0 and ready_at_utc is not null
  and(
    (normalised_r2_key is not null and normalised_sha256 is not null
      and jsonb_array_length(normalised_manifest_json)=0
      and normalised_manifest_hash is null)
    or
    (normalised_r2_key is null and normalised_sha256 is null
      and jsonb_array_length(normalised_manifest_json)>0
      and normalised_manifest_hash is not null)
  )))
);
create unique index uq_invoice_document_assets_source on public.invoice_document_assets(
 source_kind,source_id,source_revision,original_r2_key);
create index idx_invoice_document_assets_status_updated on public.invoice_document_assets(status,updated_at_utc);
create index idx_invoice_document_assets_original_hash on public.invoice_document_assets(original_sha256)
 where original_sha256 is not null;
create index idx_invoice_document_assets_source on public.invoice_document_assets(source_kind,source_id);
create index idx_invoice_document_assets_operation on public.invoice_document_assets(operation_id);

create table public.invoice_document_versions(
 id uuid primary key default gen_random_uuid(),entity_type text not null,entity_id uuid not null,
 purpose text not null,operation_id uuid not null references public.invoice_operations(id) on delete restrict,
 source_revision text not null,template_version text not null,status text not null default 'PLANNING',
 snapshot_json jsonb not null default '{}',snapshot_hash text not null,
 manifest_json jsonb not null default '[]',manifest_hash text not null,
 r2_key text,sha256 text,size_bytes bigint,expected_page_count integer,page_count integer,
 core_page_count integer,supporting_page_count integer,
 created_at_utc timestamptz not null default now(),ready_at_utc timestamptz,verified_at_utc timestamptz,
 superseded_at_utc timestamptz,error_json jsonb,
 constraint invoice_document_versions_purpose_ck check(purpose in('DRAFT_PREVIEW','FINAL_ISSUE','TIMESHEET')),
 constraint invoice_document_versions_status_ck check(status in(
  'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY','FAILED','SUPERSEDED','CANCELLED')),
 constraint invoice_document_versions_counts_ck check((size_bytes is null or size_bytes>=0)
  and(expected_page_count is null or expected_page_count>=0) and(page_count is null or page_count>=0)
  and(core_page_count is null or core_page_count>=0) and(supporting_page_count is null or supporting_page_count>=0)),
 constraint invoice_document_versions_manifest_array_ck check(jsonb_typeof(manifest_json)='array'),
 constraint invoice_document_versions_snapshot_object_ck check(jsonb_typeof(snapshot_json)='object'),
 constraint invoice_document_versions_hash_format_ck check(
  snapshot_hash ~ '^[0-9a-f]{64}$' and manifest_hash ~ '^[0-9a-f]{64}$'
  and(sha256 is null or sha256 ~ '^[0-9a-f]{64}$')),
 constraint invoice_document_versions_error_history_ck check(error_json is null or(jsonb_typeof(error_json)='object'
  and(not(error_json?'history') or(jsonb_typeof(error_json->'history')='array'
   and jsonb_array_length(error_json->'history')<=8)))),
 constraint invoice_document_versions_ready_ck check(status<>'READY' or(
  r2_key is not null and sha256 is not null and size_bytes>0 and page_count>0
  and ready_at_utc is not null and verified_at_utc is not null))
);
create unique index uq_invoice_document_versions_active_revision on public.invoice_document_versions(
 entity_type,entity_id,purpose,source_revision,template_version)
 where purpose in('DRAFT_PREVIEW','TIMESHEET')
   and status in('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY');
create unique index uq_invoice_document_versions_active_final_snapshot on public.invoice_document_versions(
 entity_type,entity_id,purpose,snapshot_hash,template_version)
 where purpose='FINAL_ISSUE'
   and status in('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY');
create index idx_invoice_document_versions_entity on public.invoice_document_versions(entity_type,entity_id,purpose,status);
create index idx_invoice_document_versions_operation_status on public.invoice_document_versions(operation_id,status);
create index idx_invoice_document_versions_status on public.invoice_document_versions(status,created_at_utc);

create table public.invoice_operation_chunks(
 id uuid primary key default gen_random_uuid(),
 operation_id uuid not null references public.invoice_operations(id) on delete cascade,
 chunk_type text not null,phase text not null,sequence_no integer not null,level_no integer not null default 0,
 work_key text not null,
 plan_generation integer not null default 1,
 replaced_by_chunk_id uuid,
 replacement_required boolean not null default false,
 entity_type text,entity_id uuid,
 document_version_id uuid references public.invoice_document_versions(id) on delete restrict,
 document_asset_id uuid references public.invoice_document_assets(id) on delete restrict,
 input_document_version_id uuid references public.invoice_document_versions(id) on delete restrict,
 status text not null default 'QUEUED',priority integer not null default 200,
 run_after_utc timestamptz not null default now(),payload_json jsonb not null default '{}',
 progress_json jsonb not null default '{}',result_json jsonb,error_json jsonb,
 expected_page_count integer,actual_page_count integer,expected_byte_count bigint,actual_byte_count bigint,
 attempt_count integer not null default 0,max_attempts integer not null default 5,
 lease_owner text,lease_token uuid,lease_expires_at_utc timestamptz,
 fence_token bigint not null default 0,operation_control_version bigint not null default 1,
 created_at_utc timestamptz not null default now(),started_at_utc timestamptz,
 updated_at_utc timestamptz not null default now(),completed_at_utc timestamptz,failed_at_utc timestamptz,
 constraint invoice_operation_chunks_type_ck check(chunk_type in(
  'GENERATION_GROUP','DOCUMENT_PLAN','DOCUMENT_INPUT','ASSET_INSPECT','ASSET_NORMALISE',
  'SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE','DOCUMENT_VERIFY','ISSUE_INVOICE',
  'DELIVERY_PREPARE','RECONCILE')),
 constraint invoice_operation_chunks_status_ck check(status in(
  'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE','FAILED',
  'DEAD_LETTER','CANCELLED','SUPERSEDED')),
 constraint invoice_operation_chunks_unique_slot unique(operation_id,chunk_type,level_no,sequence_no,work_key),
 constraint invoice_operation_chunks_work_key_ck check(work_key ~ '^[0-9a-f]{64}$'),
 constraint invoice_operation_chunks_generation_ck check(plan_generation>0),
 constraint invoice_operation_chunks_replacement_shape_ck check(
  replaced_by_chunk_id is distinct from id
  and(replaced_by_chunk_id is null or status='SUPERSEDED')
  and replacement_required=(replaced_by_chunk_id is not null)),
 constraint invoice_operation_chunks_replaced_by_fk foreign key(replaced_by_chunk_id)
  references public.invoice_operation_chunks(id) on delete restrict
  deferrable initially deferred,
 constraint invoice_operation_chunks_counts_ck check(sequence_no>=0 and level_no>=0 and priority between 0 and 2000
  and attempt_count>=0 and max_attempts>0 and fence_token>=0 and operation_control_version>0
  and(expected_page_count is null or expected_page_count>=0) and(actual_page_count is null or actual_page_count>=0)
  and(expected_byte_count is null or expected_byte_count>=0) and(actual_byte_count is null or actual_byte_count>=0)),
 constraint invoice_operation_chunks_json_ck check(jsonb_typeof(payload_json)='object'
  and jsonb_typeof(progress_json)='object' and(result_json is null or jsonb_typeof(result_json)='object')
  and(error_json is null or(jsonb_typeof(error_json)='object'
   and(not(error_json?'history') or(jsonb_typeof(error_json->'history')='array'
    and jsonb_array_length(error_json->'history')<=8))))),
 constraint invoice_operation_chunks_dependency_ck check(chunk_type<>'DOCUMENT_INPUT'
  or status in('WAITING','COMPLETE','BLOCKED','CANCELLED','SUPERSEDED')),
 constraint invoice_operation_chunks_running_lease_ck check(
  (status='RUNNING' and lease_owner is not null and lease_token is not null
   and lease_expires_at_utc is not null and started_at_utc is not null)
  or(status<>'RUNNING' and lease_owner is null and lease_token is null
   and lease_expires_at_utc is null))
);
create index idx_invoice_operation_chunks_claim on public.invoice_operation_chunks(
 status,priority desc,run_after_utc,created_at_utc,id)
 where chunk_type<>'DOCUMENT_INPUT' and status in('QUEUED','RETRY_WAIT','RUNNING');
create index idx_invoice_operation_chunks_operation_status on public.invoice_operation_chunks(operation_id,status);
create index idx_invoice_operation_chunks_replaced_by on public.invoice_operation_chunks(replaced_by_chunk_id)
 where replaced_by_chunk_id is not null;
create index idx_invoice_operation_chunks_slot_generation on public.invoice_operation_chunks(
 operation_id,chunk_type,level_no,sequence_no,plan_generation);
create index idx_invoice_operation_chunks_document_status on public.invoice_operation_chunks(document_version_id,status);
create index idx_invoice_operation_chunks_asset_status on public.invoice_operation_chunks(document_asset_id,status);
create index idx_invoice_operation_chunks_input_document_status on public.invoice_operation_chunks(input_document_version_id,status);
create index idx_invoice_operation_chunks_lease_expiry on public.invoice_operation_chunks(lease_expires_at_utc) where status='RUNNING';
create index idx_invoice_operation_chunks_dead_letter on public.invoice_operation_chunks(updated_at_utc) where status='DEAD_LETTER';
create index idx_invoice_operation_chunks_active on public.invoice_operation_chunks(operation_id,chunk_type,phase,status)
 where status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');
create index idx_invoice_operation_chunks_active_entity on public.invoice_operation_chunks(
 chunk_type,entity_type,entity_id,status,operation_id)
 where status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');
create unique index uq_invoice_operation_chunks_active_issue_invoice on public.invoice_operation_chunks(entity_id)
 where chunk_type='ISSUE_INVOICE' and entity_type='INVOICE'
  and status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

alter table public.invoices
 add column document_revision bigint not null default 1,
 add column document_state text not null default 'NOT_REQUESTED',
 add column preview_document_version_id uuid,
 add column issued_document_version_id uuid,
 add column active_document_operation_id uuid,
 add column issue_state text not null default 'NOT_STARTED',
 add column active_issue_operation_id uuid,
 add column last_document_error_json jsonb,
 add constraint invoices_document_state_ck check(document_state in('NOT_REQUESTED','QUEUED','STALE','PREPARING','READY','FAILED')),
 add constraint invoices_issue_state_ck check(issue_state in(
  'NOT_STARTED','VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE','ISSUED','BLOCKED','FAILED','CANCELLED','SUPERSEDED')),
 add constraint invoices_preview_document_version_fk foreign key(preview_document_version_id)
  references public.invoice_document_versions(id) on delete set null deferrable initially deferred,
 add constraint invoices_issued_document_version_fk foreign key(issued_document_version_id)
  references public.invoice_document_versions(id) on delete restrict deferrable initially deferred,
 add constraint invoices_active_document_operation_fk foreign key(active_document_operation_id)
  references public.invoice_operations(id) on delete set null deferrable initially deferred,
 add constraint invoices_active_issue_operation_fk foreign key(active_issue_operation_id)
  references public.invoice_operations(id) on delete set null deferrable initially deferred;

alter table public.timesheets
 add column document_revision bigint not null default 1,
 add column document_state text not null default 'NOT_REQUESTED',
 add column current_document_version_id uuid,
 add column active_document_operation_id uuid,
 add column manual_document_asset_id uuid,
 add column last_document_error_json jsonb,
 add constraint timesheets_document_state_ck check(document_state in('NOT_REQUESTED','QUEUED','STALE','PREPARING','READY','FAILED')),
 add constraint timesheets_current_document_version_fk foreign key(current_document_version_id)
  references public.invoice_document_versions(id) on delete set null deferrable initially deferred,
 add constraint timesheets_active_document_operation_fk foreign key(active_document_operation_id)
  references public.invoice_operations(id) on delete set null deferrable initially deferred,
 add constraint timesheets_manual_document_asset_fk foreign key(manual_document_asset_id)
  references public.invoice_document_assets(id) on delete set null deferrable initially deferred;

alter table public.timesheet_evidence
 add column document_asset_id uuid,
 add column source_revision text,
 add column processing_state text not null default 'DISCOVERED',
 add column processing_error_json jsonb,
 add constraint timesheet_evidence_asset_revision_ck check(
  document_asset_id is null or nullif(btrim(source_revision),'') is not null),
 add constraint timesheet_evidence_processing_state_ck check(processing_state in(
  'DISCOVERED','INSPECTING','NORMALISING','READY','UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')),
 add constraint timesheet_evidence_document_asset_fk foreign key(document_asset_id)
  references public.invoice_document_assets(id) on delete set null deferrable initially deferred;

alter table public.mail_outbox
 add column attachments_ready boolean not null default true,
 add column waiting_invoice_operation_id uuid,
 add column attachment_total_bytes bigint,
 add column attachment_delivery_policy text,
 add constraint mail_outbox_attachment_bytes_ck check(attachment_total_bytes is null or attachment_total_bytes>=0),
 add constraint mail_outbox_attachment_delivery_policy_ck check(
  attachment_delivery_policy is null or attachment_delivery_policy in('ATTACH','SPLIT','SECURE_LINK')),
 add constraint mail_outbox_waiting_invoice_operation_fk foreign key(waiting_invoice_operation_id)
  references public.invoice_operations(id) on delete set null deferrable initially deferred;

create index idx_invoices_active_document_operation on public.invoices(active_document_operation_id)
 where active_document_operation_id is not null;
create index idx_invoices_active_issue_operation on public.invoices(active_issue_operation_id)
 where active_issue_operation_id is not null;
create index idx_invoices_preview_document on public.invoices(preview_document_version_id)
 where preview_document_version_id is not null;
create index idx_invoices_issued_document on public.invoices(issued_document_version_id)
 where issued_document_version_id is not null;
create index idx_timesheets_active_document_operation on public.timesheets(active_document_operation_id)
 where active_document_operation_id is not null;
create index idx_timesheets_current_document on public.timesheets(current_document_version_id)
 where current_document_version_id is not null;
create index idx_timesheets_manual_document_asset on public.timesheets(manual_document_asset_id)
 where manual_document_asset_id is not null;
create index idx_timesheet_evidence_document_asset on public.timesheet_evidence(document_asset_id)
 where document_asset_id is not null;
create index idx_timesheet_evidence_source_revision on public.timesheet_evidence(timesheet_id,source_revision)
 where source_revision is not null;
create index idx_mail_outbox_invoice_attachment_ready on public.mail_outbox(
 status,attachments_ready,next_attempt_at_utc,created_at_utc)
 where status='QUEUED' and waiting_invoice_operation_id is null;
create unique index uq_mail_outbox_invoice_delivery_identity on public.mail_outbox(reference)
 where reference like 'INVOICE_DELIVERY_V1:%';

alter table public.invoice_operations enable row level security;
alter table public.invoice_operation_chunks enable row level security;
alter table public.invoice_document_versions enable row level security;
alter table public.invoice_document_assets enable row level security;
revoke all on public.invoice_operations,public.invoice_operation_chunks,
 public.invoice_document_versions,public.invoice_document_assets from public,anon,authenticated;
grant all on public.invoice_operations,public.invoice_operation_chunks,
 public.invoice_document_versions,public.invoice_document_assets to postgres,service_role;
comment on table public.invoice_operations is
 'Authoritative invoice workflow operations; JSON columns must not contain file bytes/base64.';
comment on table public.invoice_operation_chunks is
 'Bounded leased chunks with random lease, fence and parent-control ownership checks.';
comment on table public.invoice_document_versions is
 'Immutable version-addressed document metadata; READY requires final verification.';
comment on table public.invoice_document_assets is
 'Immutable original-upload registrations and reusable verified normalisations.';
commit;
