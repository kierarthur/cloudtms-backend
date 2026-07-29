create or replace function public.invoice_work_complete_batch(
  p_results jsonb,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_valid_results jsonb:='[]'::jsonb;
  v_rejections jsonb:='[]'::jsonb;
  v_result jsonb:='[]'::jsonb;
  v_ignored integer;
begin
  if p_results is null or jsonb_typeof(p_results)<>'array' then
    raise exception using errcode='22023',
      message='p_results must be an array containing 1..100 items';
  end if;

  if jsonb_array_length(p_results)<1
     or jsonb_array_length(p_results)>100 then
    raise exception using errcode='22023',
      message='p_results must be an array containing 1..100 items';
  end if;

  /*
   * Ownership and payload validity are classified per item.  Ownership
   * failures are returned without mutation.  A current claim with a malformed
   * processor result is transitioned to a permanent failure or bounded retry;
   * it is never left RUNNING.
   */
  with supplied as materialized (
    select x.ordinality::integer request_no,x.value raw_result,
      case when coalesce(x.value->>'chunk_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'chunk_id')::uuid end chunk_id,
      case when coalesce(x.value->>'lease_token','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'lease_token')::uuid end lease_token,
      case when coalesce(x.value->>'fence_token','') ~ '^[0-9]{1,18}$'
        then (x.value->>'fence_token')::bigint end fence_token,
      case when coalesce(x.value->>'operation_control_version','') ~ '^[0-9]{1,18}$'
        then (x.value->>'operation_control_version')::bigint end operation_control_version,
      upper(coalesce(x.value->>'outcome','')) outcome,
      coalesce(x.value->'result','{}'::jsonb) processor_result,
      coalesce(x.value->'error','{}'::jsonb) processor_error
    from jsonb_array_elements(p_results) with ordinality x(value,ordinality)
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      coalesce((
        select array_agg(distinct c.operation_id)
        from supplied s
        join public.invoice_operation_chunks c on c.id=s.chunk_id),
        array['00000000-0000-0000-0000-000000000000'::uuid]),
      null,null,10000) g
  ),
  inspected as materialized (
    select s.*,c.operation_id,c.chunk_type,c.phase,c.plan_generation,
      c.level_no,c.sequence_no,
      c.entity_type,c.entity_id,
      c.document_version_id,c.document_asset_id,c.input_document_version_id,
      c.payload_json,c.expected_page_count,c.expected_byte_count,c.attempt_count,c.max_attempts,
      c.status current_status,c.lease_token current_lease_token,
      c.fence_token current_fence_token,c.operation_control_version current_control_version,
      c.lease_expires_at_utc,o.status operation_status,
      o.control_version operation_current_control_version,
      o.config_json->'processor_policy' processor_policy,
      a.source_revision asset_source_revision,
      a.original_r2_key registered_original_r2_key,
      a.original_sha256 registered_original_sha256,
      a.original_size_bytes registered_original_size_bytes,
      dv.source_revision document_source_revision,dv.manifest_hash,dv.manifest_json,
      dv.snapshot_json,dv.snapshot_hash,
      case
        when s.chunk_id is null or s.lease_token is null
          or s.fence_token is null or s.operation_control_version is null then 'INVALID_COMPLETION'
        when c.id is null then 'CHUNK_NOT_FOUND'
        when exists(select 1 from current_graph g
          where g.operation_id=c.operation_id
            and g.replacement_chain_status='INVALID')
          then 'INVALID_REPLACEMENT_GRAPH'
        when not exists(select 1 from current_graph g
          where g.current_chunk_id=c.id
            and g.replacement_chain_status='VALID')
          then 'CHUNK_NOT_CURRENT'
        when c.status<>'RUNNING' then 'CHUNK_NOT_RUNNING'
        when c.lease_token is distinct from s.lease_token then 'LEASE_TOKEN_MISMATCH'
        when c.fence_token is distinct from s.fence_token then 'FENCE_TOKEN_MISMATCH'
        when c.operation_control_version is distinct from s.operation_control_version
          or o.control_version is distinct from s.operation_control_version then 'CONTROL_VERSION_MISMATCH'
        when c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now then 'LEASE_EXPIRED'
        when o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED')
          then 'OPERATION_TERMINAL'
      end ownership_error
    from supplied s
    left join public.invoice_operation_chunks c on c.id=s.chunk_id
    left join public.invoice_operations o on o.id=c.operation_id
    left join public.invoice_document_assets a on a.id=c.document_asset_id
    left join public.invoice_document_versions dv on dv.id=c.document_version_id
  ),
  identified as materialized (
    select i.*,
      case
        when i.chunk_type in('ASSET_NORMALISE','ASSET_INSPECT')
          then 'invoice-assets/'||i.document_asset_id||'/'||
            coalesce(i.asset_source_revision,'')||'/'||i.chunk_id||'/'||
            i.fence_token||'/'
        when i.chunk_type='SOURCE_RENDER'
          then 'invoice-documents/'||i.document_version_id||'/source/'||
            i.chunk_id||'/'||i.fence_token||'/'
        when i.chunk_type='INVOICE_CORE_RENDER'
          then 'invoice-documents/'||i.document_version_id||'/core/'||
            i.chunk_id||'/'||i.fence_token||'/'
        when i.chunk_type='PDF_MERGE'
          then 'invoice-documents/'||i.document_version_id||'/merge/'||
            i.level_no||'/'||i.sequence_no||'/'||i.chunk_id||'/'||
            i.fence_token||'/'
        when i.chunk_type='DOCUMENT_VERIFY' then null
      end expected_output_prefix,
      case when coalesce(
          i.processor_policy->'result'->>i.chunk_type,'')~'^[1-9][0-9]{0,9}$'
        then(i.processor_policy->'result'->>i.chunk_type)::integer
      end result_limit_bytes
    from inspected i
  ),
  classified as materialized (
    select i.*,
      case
        when i.outcome not in('SUCCESS','RETRY','BLOCKED','FAILED','SUPERSEDED','CANCELLED')
          then 'INVALID_OUTCOME'
        when coalesce(i.processor_policy->>'policy_version',i.processor_policy->>'version')<>'INVOICE_PROCESSOR_LIMITS_V4'
          or i.result_limit_bytes is null
          then 'PROCESSOR_POLICY_INVALID'
        when jsonb_typeof(i.processor_result)<>'object'
          or octet_length(i.processor_result::text)>i.result_limit_bytes
          or lower(i.processor_result::text) ~
            '"(base64|file_bytes|raw_bytes|processor_dump)"[[:space:]]*:'
          then 'INVALID_RESULT_PAYLOAD'
        when i.outcome='SUCCESS' and(
          coalesce(i.processor_result->>'chunk_id','')<>i.chunk_id::text
          or coalesce(i.processor_result->>'fence_token','')<>i.fence_token::text
          or upper(coalesce(i.processor_result->>'action',''))<>
            case i.chunk_type
              when 'ASSET_INSPECT' then 'ASSET_INSPECT'
              when 'ASSET_NORMALISE' then 'ASSET_NORMALISE'
              when 'SOURCE_RENDER' then 'SOURCE_RENDER'
              when 'INVOICE_CORE_RENDER' then 'INVOICE_CORE_RENDER'
              when 'PDF_MERGE' then 'PDF_MERGE'
              when 'DOCUMENT_VERIFY' then 'DOCUMENT_VERIFY'
              else upper(i.chunk_type) end
          or(i.document_asset_id is not null
            and coalesce(i.processor_result->>'document_asset_id','')
              <>i.document_asset_id::text)
          or(i.document_version_id is not null
            and coalesce(i.processor_result->>'document_version_id','')
              <>i.document_version_id::text)
          or coalesce(i.processor_result->>'plan_generation','')<>
            i.plan_generation::text
          or coalesce(i.processor_result->>'processor_policy_version','')<>
            coalesce(i.processor_policy->>'policy_version',i.processor_policy->>'version','')
          or(i.chunk_type<>'DOCUMENT_VERIFY' and
            coalesce(i.processor_result->>'output_prefix','')<>
              coalesce(i.expected_output_prefix,''))
          or(i.document_version_id is not null
            and nullif(i.document_source_revision,'') is not null
            and coalesce(i.processor_result->>'template_version','')<>
              coalesce((select v.template_version
                from public.invoice_document_versions v
                where v.id=i.document_version_id),''))
          or(i.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
            and coalesce(i.processor_result->>'render_kind','')<>
              coalesce(i.payload_json->>'render_kind',
                case when i.chunk_type='INVOICE_CORE_RENDER'
                  then 'INVOICE_CORE' end,''))
          or(i.chunk_type='PDF_MERGE'
            and coalesce(i.processor_result->>'ordered_input_hash','')<>
              coalesce(i.payload_json->>'ordered_input_hash',''))
        ) then 'PROCESSOR_RESULT_IDENTITY_MISMATCH'
        when i.outcome='SUCCESS'
          and i.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          and (
            /*
             * Render completion must prove the same frozen presentation model
             * identity that was planned and handed out by context.  For source
             * renders the source-model identity must come from the chunk
             * payload / expected_result_identity; we deliberately do not fall
             * back to the invoice-root model hash for source chunks.
             */
            nullif(coalesce(
              i.processor_result->>'presentation_model_schema_version',
              i.processor_result#>>'{render_identity,presentation_model_schema_version}',
              i.processor_result#>>'{identity,presentation_model_schema_version}',
              ''),'') is null
            or nullif(coalesce(
              i.processor_result->>'presentation_model_hash',
              i.processor_result#>>'{render_identity,presentation_model_hash}',
              i.processor_result#>>'{identity,presentation_model_hash}',
              ''),'') is null
            or nullif(coalesce(
              i.processor_result->>'snapshot_hash',
              i.processor_result#>>'{render_identity,snapshot_hash}',
              i.processor_result#>>'{identity,snapshot_hash}',
              ''),'') is null
            or nullif(coalesce(
              i.payload_json->>'presentation_model_schema_version',
              i.payload_json#>>'{expected_result_identity,presentation_model_schema_version}',
              case when i.chunk_type='INVOICE_CORE_RENDER'
                then i.snapshot_json#>>'{presentation_model,schema_version}' end,
              ''),'') is null
            or nullif(coalesce(
              i.payload_json->>'presentation_model_hash',
              i.payload_json#>>'{expected_result_identity,presentation_model_hash}',
              case when i.chunk_type='INVOICE_CORE_RENDER'
                then i.snapshot_json->>'presentation_model_hash' end,
              ''),'') is null
            or nullif(coalesce(
              i.payload_json->>'snapshot_hash',
              i.payload_json#>>'{expected_result_identity,snapshot_hash}',
              i.snapshot_hash,
              ''),'') is null
            or nullif(coalesce(
              i.processor_result->>'presentation_model_schema_version',
              i.processor_result#>>'{render_identity,presentation_model_schema_version}',
              i.processor_result#>>'{identity,presentation_model_schema_version}',
              ''),'') is distinct from nullif(coalesce(
              i.payload_json->>'presentation_model_schema_version',
              i.payload_json#>>'{expected_result_identity,presentation_model_schema_version}',
              case when i.chunk_type='INVOICE_CORE_RENDER'
                then i.snapshot_json#>>'{presentation_model,schema_version}' end,
              ''),'')
            or nullif(coalesce(
              i.processor_result->>'presentation_model_hash',
              i.processor_result#>>'{render_identity,presentation_model_hash}',
              i.processor_result#>>'{identity,presentation_model_hash}',
              ''),'') is distinct from nullif(coalesce(
              i.payload_json->>'presentation_model_hash',
              i.payload_json#>>'{expected_result_identity,presentation_model_hash}',
              case when i.chunk_type='INVOICE_CORE_RENDER'
                then i.snapshot_json->>'presentation_model_hash' end,
              ''),'')
            or nullif(coalesce(
              i.processor_result->>'snapshot_hash',
              i.processor_result#>>'{render_identity,snapshot_hash}',
              i.processor_result#>>'{identity,snapshot_hash}',
              ''),'') is distinct from nullif(coalesce(
              i.payload_json->>'snapshot_hash',
              i.payload_json#>>'{expected_result_identity,snapshot_hash}',
              i.snapshot_hash,
              ''),'')
          ) then 'RENDER_MODEL_IDENTITY_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) in('','unknown','application/octet-stream')
          then 'ASSET_MEDIA_TYPE_UNSUPPORTED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_kind',''))='empty'
          then 'ASSET_EMPTY'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_kind',''))='truncated'
          then 'ASSET_TRUNCATED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_kind',''))='missing'
          then 'MISSING_SOURCE'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and coalesce(i.processor_result->>'is_encrypted','false')='true'
          then 'ASSET_PDF_ENCRYPTED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) not in(
              'application/pdf','pdf','image/jpeg','jpeg','jpg','image/png','png')
          then 'ASSET_MEDIA_TYPE_UNSUPPORTED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and(coalesce(i.processor_result->>'original_size_bytes','') !~ '^[0-9]{1,18}$'
            or case when coalesce(i.processor_result->>'original_size_bytes','')
                ~ '^[0-9]{1,18}$'
              then(i.processor_result->>'original_size_bytes')::bigint
              else 0 end<=0
            or coalesce(i.processor_result->>'original_sha256','')
              !~ '^[0-9a-f]{64}$')
          then 'INVALID_INSPECTION_RESULT'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and(
            (i.processor_result->>'original_size_bytes')::bigint>
              (i.processor_policy#>>'{asset,max_source_bytes}')::bigint
            or not exists(
              select 1
              from jsonb_array_elements_text(
                i.processor_policy#>'{asset,allowed_media_types}') allowed(media_type)
              where allowed.media_type=lower(
                i.processor_result->>'detected_media_type')))
          then 'ASSET_SOURCE_SIZE_EXCEEDED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) in('application/pdf','pdf')
          and(coalesce(i.processor_result->>'page_count','') !~ '^[1-9][0-9]{0,8}$'
            or coalesce(i.processor_result->>'parse_verified','false')<>'true'
            or coalesce(i.processor_result->>'is_encrypted','false')<>'false')
          then 'INVALID_PDF_INSPECTION_RESULT'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) in(
              'image/jpeg','jpeg','jpg','image/png','png')
          and(coalesce(i.processor_result->>'width_pixels','') !~ '^[1-9][0-9]{0,8}$'
            or coalesce(i.processor_result->>'height_pixels','') !~ '^[1-9][0-9]{0,8}$'
            or coalesce(i.processor_result->>'estimated_decoded_bytes','')
              !~ '^[1-9][0-9]{0,17}$'
            or coalesce(i.processor_result->>'decode_verified','false')<>'true'
            or coalesce(i.processor_result->>'orientation_degrees','')
              !~ '^(0|90|180|270)$')
          then 'INVALID_IMAGE_INSPECTION_RESULT'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) in(
              'image/jpeg','jpeg','jpg','image/png','png')
          and(
            (i.processor_result->>'width_pixels')::bigint*
              (i.processor_result->>'height_pixels')::bigint>
                (i.processor_policy#>>'{asset,max_pixels}')::bigint
            or(i.processor_result->>'estimated_decoded_bytes')::bigint>
                (i.processor_policy#>>'{asset,max_decoded_bytes}')::bigint)
          then 'ASSET_DECODE_POLICY_EXCEEDED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and i.registered_original_sha256 is not null
          and i.processor_result->>'original_sha256'
            is distinct from i.registered_original_sha256
          then 'ORIGINAL_HASH_MISMATCH'
        when i.outcome='SUCCESS'
          and i.chunk_type in('ASSET_NORMALISE','SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE')
          and(coalesce(i.processor_result->>'r2_key','')=''
            or coalesce(i.processor_result->>'sha256','')
              !~ '^[0-9a-f]{64}$'
            or coalesce(i.processor_result->>'size_bytes','') !~ '^[0-9]{1,18}$'
            or case when coalesce(i.processor_result->>'size_bytes','')
                ~ '^[0-9]{1,18}$'
              then(i.processor_result->>'size_bytes')::bigint else 0 end<=0
            or coalesce(i.processor_result->>'page_count','') !~ '^[0-9]{1,9}$'
            or case when coalesce(i.processor_result->>'page_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'page_count')::integer else 0 end<=0
            or coalesce(i.processor_result->>'parse_verified','false')<>'true')
          then 'INVALID_ARTIFACT_RESULT'
        when i.outcome='SUCCESS'
          and i.chunk_type in('ASSET_NORMALISE','SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE')
          and left(i.processor_result->>'r2_key',length(
            case
              when i.chunk_type in('ASSET_NORMALISE','ASSET_INSPECT')
                then 'invoice-assets/'||i.document_asset_id||'/'||
                  coalesce(i.asset_source_revision,'')||'/'||i.chunk_id||'/'||
                  i.fence_token||'/'
              when i.chunk_type='SOURCE_RENDER'
                then 'invoice-documents/'||i.document_version_id||'/source/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              when i.chunk_type='INVOICE_CORE_RENDER'
                then 'invoice-documents/'||i.document_version_id||'/core/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              when i.chunk_type='PDF_MERGE'
                then 'invoice-documents/'||i.document_version_id||'/merge/'||
                  i.level_no||'/'||i.sequence_no||'/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              else 'invoice-documents/'||i.document_version_id||'/verify/'||
                i.chunk_id||'/'||i.fence_token||'/'
            end)) is distinct from
            case
              when i.chunk_type in('ASSET_NORMALISE','ASSET_INSPECT')
                then 'invoice-assets/'||i.document_asset_id||'/'||
                  coalesce(i.asset_source_revision,'')||'/'||i.chunk_id||'/'||
                  i.fence_token||'/'
              when i.chunk_type='SOURCE_RENDER'
                then 'invoice-documents/'||i.document_version_id||'/source/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              when i.chunk_type='INVOICE_CORE_RENDER'
                then 'invoice-documents/'||i.document_version_id||'/core/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              when i.chunk_type='PDF_MERGE'
                then 'invoice-documents/'||i.document_version_id||'/merge/'||
                  i.level_no||'/'||i.sequence_no||'/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              else 'invoice-documents/'||i.document_version_id||'/verify/'||
                i.chunk_id||'/'||i.fence_token||'/'
            end
          then 'OUTPUT_IDENTITY_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='PDF_MERGE'
          and(coalesce(i.processor_result->>'input_count','') !~ '^[0-9]{1,9}$'
            or case when coalesce(i.processor_result->>'input_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'input_count')::integer else -1 end<>
                jsonb_array_length(coalesce(i.payload_json->'inputs','[]'::jsonb))
            or coalesce(i.processor_result->>'ordered_input_hash','')<>
              coalesce(i.payload_json->>'ordered_input_hash',''))
          then 'MERGE_INPUT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='PDF_MERGE'
          and(
            jsonb_typeof(i.processor_result->'input_receipts')<>'array'
            or jsonb_array_length(i.processor_result->'input_receipts')<>
              jsonb_array_length(coalesce(i.payload_json->'inputs','[]'::jsonb))
            or exists(
              select 1
              from jsonb_array_elements(coalesce(
                i.payload_json->'inputs','[]'::jsonb))
                with ordinality expected(value,ordinality)
              full join jsonb_array_elements(coalesce(
                i.processor_result->'input_receipts','[]'::jsonb))
                with ordinality actual(value,ordinality)
                using(ordinality)
              where expected.value is null or actual.value is null
                or actual.value->>'input_chunk_id'
                  is distinct from expected.value->>'input_chunk_id'
                or actual.value->>'actual_input_order'
                  is distinct from expected.value->>'input_order'
                or actual.value->>'actual_input_order'
                  is distinct from actual.ordinality::text
                or actual.value->>'actual_r2_key'
                  is distinct from expected.value->>'r2_key'
                or actual.value->>'actual_sha256'
                  is distinct from expected.value->>'sha256'
                or actual.value->>'actual_page_count'
                  is distinct from expected.value->>'page_count'
                or actual.value->>'actual_size_bytes'
                  is distinct from expected.value->>'size_bytes'
                or(
                  expected.value?'expected_physical_receipt'
                  and(
                    actual.value->>'logical_source_key'
                      is distinct from expected.value->>'logical_source_key'
                    or actual.value->>'logical_manifest_ordinal'
                      is distinct from
                        expected.value->>'logical_manifest_ordinal'
                    or actual.value->>'physical_part_no'
                      is distinct from expected.value->>'physical_part_no'
                    or actual.value->>'actual_physical_receipt'
                      is distinct from
                        expected.value->>'expected_physical_receipt'
                    or actual.value->>'actual_physical_receipt'
                      is distinct from encode(digest(jsonb_build_object(
                        'receipt_contract','ACTUAL_BYTES_OBJECT_RECEIPT_V3',
                        'logical_source_key',
                          actual.value->>'logical_source_key',
                        'logical_manifest_ordinal',
                          case when coalesce(actual.value->>
                              'logical_manifest_ordinal','')~'^[0-9]{1,9}$'
                            then(actual.value->>
                              'logical_manifest_ordinal')::integer end,
                        'physical_part_no',
                          case when coalesce(actual.value->>
                              'physical_part_no','')~'^[1-9][0-9]{0,8}$'
                            then(actual.value->>
                              'physical_part_no')::integer end,
                        'object_key',actual.value->>'actual_r2_key',
                        'stored_sha256',actual.value->>'actual_sha256',
                        'expected_page_count',
                          case when coalesce(actual.value->>
                              'actual_page_count','')~'^[1-9][0-9]{0,8}$'
                            then(actual.value->>
                              'actual_page_count')::integer end,
                        'expected_byte_count',
                          case when coalesce(actual.value->>
                              'actual_size_bytes','')~'^[1-9][0-9]{0,17}$'
                            then(actual.value->>
                              'actual_size_bytes')::bigint end
                      )::text,'sha256'),'hex')))
                or(
                  expected.value?'child_merge_receipt_hash'
                  and(
                    actual.value->'actual_child_merge_receipt'
                      is distinct from expected.value->'child_merge_receipt'
                    or encode(digest(coalesce(actual.value->
                        'actual_child_merge_receipt','{}'::jsonb)::text,
                      'sha256'),'hex')
                        is distinct from
                          expected.value->>'child_merge_receipt_hash'
                    or actual.value->>'actual_child_merge_receipt_hash'
                      is distinct from
                        expected.value->>'child_merge_receipt_hash'))
            ))
          then 'MERGE_RECEIPT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='PDF_MERGE'
          and(
            jsonb_typeof(i.processor_result->'merge_receipt')<>'object'
            or coalesce(i.processor_result#>>
              '{merge_receipt,receipt_contract}','')<>
                'ACTUAL_BYTES_MERGE_RECEIPT_V3'
            or jsonb_typeof(i.processor_result#>
              '{merge_receipt,input_receipts}')<>'array'
            or i.processor_result#>'{merge_receipt,input_receipts}'
              is distinct from i.processor_result->'input_receipts'
            or nullif(i.processor_result#>>
              '{merge_receipt,processor_version}','') is null
            or coalesce(i.processor_result#>>
              '{merge_receipt,processor_policy_version}','')<>
                coalesce(i.processor_policy->>'policy_version',i.processor_policy->>'version','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,actual_ordered_input_hash}','')<>
                coalesce(i.payload_json->>'ordered_input_hash','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,output_sha256}','')<>
                coalesce(i.processor_result->>'sha256','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,output_page_count}','')<>
                coalesce(i.processor_result->>'page_count','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,actual_child_receipt_hash}','')<>
                coalesce(i.payload_json->>'expected_child_receipt_hash','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,actual_child_receipt_hash}','')<>
              coalesce((
                select encode(digest(string_agg(coalesce(
                  nullif(r.value->>'actual_physical_receipt',''),
                  encode(digest(coalesce(r.value->
                    'actual_child_merge_receipt','{}'::jsonb)::text,
                    'sha256'),'hex')),'||'
                  order by r.ordinality),'sha256'),'hex')
                from jsonb_array_elements(coalesce(
                  i.processor_result->'input_receipts','[]'::jsonb))
                  with ordinality r(value,ordinality)
              ),'')
            or i.processor_result#>'{merge_receipt,physical_receipts}'
                is distinct from
                  coalesce(i.payload_json->'expected_physical_receipts',
                    '[]'::jsonb)
            or coalesce(i.processor_result#>>
              '{merge_receipt,combined_logical_receipt_root}','')<>
                coalesce(i.payload_json->>'expected_logical_receipt_root','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,combined_physical_receipt_root}','')<>
                coalesce(i.payload_json->>'expected_physical_receipt_root','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,combined_physical_receipt_root}','')<>
              coalesce((
                select encode(digest(string_agg(
                  r.value->>'physical_receipt','||'
                  order by
                    case when coalesce(r.value->>
                        'logical_manifest_ordinal','')~'^[0-9]{1,9}$'
                      then(r.value->>
                        'logical_manifest_ordinal')::integer end,
                    case when coalesce(r.value->>
                        'physical_part_no','')~'^[1-9][0-9]{0,8}$'
                      then(r.value->>'physical_part_no')::integer end,
                    r.ordinality),'sha256'),'hex')
                from jsonb_array_elements(coalesce(i.processor_result#>
                  '{merge_receipt,physical_receipts}','[]'::jsonb))
                  with ordinality r(value,ordinality)
                where coalesce(r.value->>'physical_receipt','')
                  ~'^[0-9a-f]{64}$'
              ),'')
            or coalesce(i.processor_result#>>
              '{merge_receipt,combined_logical_receipt_root}','')<>
              coalesce((
                select encode(digest(string_agg(
                  logical.logical_receipt,'||'
                  order by logical.logical_manifest_ordinal,
                    logical.logical_source_key),'sha256'),'hex')
                from(
                  select r.value->>'logical_source_key'
                      logical_source_key,
                    min(case when coalesce(r.value->>
                        'logical_manifest_ordinal','')~'^[0-9]{1,9}$'
                      then(r.value->>
                        'logical_manifest_ordinal')::integer end)
                      logical_manifest_ordinal,
                    encode(digest(jsonb_build_object(
                      'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
                      'logical_source_key',
                        r.value->>'logical_source_key',
                      'logical_manifest_ordinal',
                        min(case when coalesce(r.value->>
                            'logical_manifest_ordinal','')
                              ~'^[0-9]{1,9}$'
                          then(r.value->>
                            'logical_manifest_ordinal')::integer end),
                      'ordered_physical_receipts',string_agg(
                        r.value->>'physical_receipt','||'
                        order by case when coalesce(r.value->>
                            'logical_manifest_ordinal','')
                              ~'^[0-9]{1,9}$'
                          then(r.value->>
                            'logical_manifest_ordinal')::integer end,
                          case when coalesce(r.value->>
                            'physical_part_no','')
                              ~'^[1-9][0-9]{0,8}$'
                          then(r.value->>
                            'physical_part_no')::integer end,
                          r.ordinality)
                    )::text,'sha256'),'hex') logical_receipt
                  from jsonb_array_elements(coalesce(
                    i.processor_result#>
                      '{merge_receipt,physical_receipts}',
                    '[]'::jsonb)) with ordinality r(value,ordinality)
                  where nullif(r.value->>
                      'logical_source_key','') is not null
                    and coalesce(r.value->>'physical_receipt','')
                      ~'^[0-9a-f]{64}$'
                  group by r.value->>'logical_source_key'
                ) logical
              ),'')
            or coalesce(i.processor_result#>>
              '{merge_receipt,plan_generation}','')<>
                i.plan_generation::text
          ) then 'MERGE_RECEIPT_CHAIN_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='PDF_MERGE'
          and(i.expected_page_count is null
            or case when coalesce(i.processor_result->>'page_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'page_count')::integer else -1 end<>
                i.expected_page_count)
          then 'MERGE_PAGE_COUNT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_NORMALISE'
          and(coalesce(i.processor_result->>'consumed_original_r2_key','')<>coalesce(i.registered_original_r2_key,'')
            or coalesce(i.processor_result->>'consumed_original_sha256','')<>coalesce(i.registered_original_sha256,'')
            or coalesce(i.processor_result->>'consumed_original_size_bytes','')<>coalesce(i.registered_original_size_bytes::text,''))
          then 'ASSET_SOURCE_IDENTITY_CHANGED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_NORMALISE'
          and(i.expected_page_count is null
            or case when coalesce(i.processor_result->>'page_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'page_count')::integer else -1 end<>
                i.expected_page_count)
          then 'ASSET_PART_PAGE_COUNT_MISMATCH'
        when i.outcome='SUCCESS'
          and i.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          and(
            lower(coalesce(i.processor_result->>'output_type','')) not in(
              'pdf','application/pdf')
            or coalesce(i.processor_result->>'template_version','')<>
              coalesce(i.payload_json->>'template_version','')
            or coalesce(i.processor_result->>'render_kind','')<>
              coalesce(i.payload_json->>'render_kind','')
            or coalesce(i.processor_result->>'document_version_id','')<>
              i.document_version_id::text)
          then 'RENDER_CONTRACT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='SOURCE_RENDER'
          and i.payload_json->>'render_kind'='ATTACHMENT_INDEX'
          and(
            coalesce(i.processor_result->>'layout_phase','')<>
              coalesce(i.payload_json->>'layout_phase','')
            or coalesce(i.processor_result->>'layout_pass','')<>
              coalesce(i.payload_json->>'layout_pass','')
            or coalesce(i.processor_result->>'layout_page_count','')<>
              coalesce(i.processor_result->>'page_count','')
            or nullif(i.processor_result->>'processor_version','') is null
            or coalesce(i.processor_result->>'processor_policy_version','')<>
              coalesce(i.processor_policy->>'policy_version',i.processor_policy->>'version','')
            or(i.payload_json->>'layout_phase'='FINAL'
              and(
                coalesce(i.processor_result->>'displayed_start_pages_hash','')<>
                  coalesce(i.payload_json->>'expected_start_pages_hash','')
                or jsonb_typeof(i.processor_result->'displayed_page_map')<>'array'
                or i.processor_result->'displayed_page_map'
                  is distinct from i.payload_json->'attachments'
                or encode(digest(coalesce(
                    i.processor_result->'displayed_page_map','[]'::jsonb)::text,
                    'sha256'),'hex')<>
                  coalesce(i.payload_json->>'expected_start_pages_hash','')
                or coalesce(i.processor_result->>'displayed_page_map_hash','')<>
                  coalesce(i.payload_json->>'expected_start_pages_hash','')
                or coalesce(i.processor_result->>'displayed_row_count','')<>
                  coalesce(i.payload_json->>'displayed_row_count','')
                or coalesce(i.processor_result->>'final_index_page_count','')<>
                  coalesce(i.processor_result->>'page_count','')
                or coalesce(i.processor_result->>'pagination_stream_hash','')<>
                  coalesce(i.payload_json->>'pagination_stream_hash','')
                or coalesce(i.processor_result->>'layout_identity_hash','')<>
                  encode(digest(coalesce(i.payload_json->'determinism',
                    '{}'::jsonb)::text,'sha256'),'hex')
                or coalesce(i.processor_result->>'displayed_rows_verified','false')
                  <>'true'))
          ) then 'ATTACHMENT_INDEX_CONTRACT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='DOCUMENT_VERIFY'
          and(coalesce(i.processor_result->>'manifest_coverage_verified','false')<>'true'
            or coalesce(i.processor_result->>'ordering_verified','false')<>'true'
            or coalesce(i.processor_result->>'manifest_hash','')<>i.manifest_hash
            or coalesce(i.processor_result->>'root_merge_receipt_hash','')<>
              coalesce(i.payload_json->>'final_merge_receipt_hash','')
            or encode(digest(coalesce(
              i.payload_json->'final_merge_receipt','{}'::jsonb)::text,
              'sha256'),'hex')<>
              coalesce(i.payload_json->>'final_merge_receipt_hash','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,receipt_contract}','')<>
                'ACTUAL_BYTES_MERGE_RECEIPT_V3'
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,combined_logical_receipt_root}','')<>
                coalesce(i.payload_json->>
                  'expected_logical_root_receipt','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,combined_physical_receipt_root}','')<>
                coalesce(i.payload_json->>
                  'expected_physical_root_receipt','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,actual_child_receipt_hash}','')<>
                coalesce(i.payload_json->>'expected_ordered_input_root','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,output_page_count}','')<>
                coalesce(i.payload_json->>'independent_expected_page_count','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,output_sha256}','')<>
                coalesce(i.payload_json->>'candidate_sha256','')
            or coalesce(i.processor_result->>
              'root_merge_receipt_identity','')<>
                coalesce(i.payload_json->>'root_merge_receipt_identity','')
            or coalesce(i.processor_result->>
              'actual_logical_root_receipt','')<>
                coalesce(i.payload_json->>
                  'expected_logical_root_receipt','')
            or coalesce(i.processor_result->>
              'actual_physical_root_receipt','')<>
                coalesce(i.payload_json->>
                  'expected_physical_root_receipt','')
            or coalesce(i.processor_result->>
              'actual_ordered_input_root','')<>
                coalesce(i.payload_json->>'expected_ordered_input_root','')
            or encode(digest(jsonb_build_object(
                'receipt_contract','DOCUMENT_ROOT_RECEIPT_V3',
                'logical_root',i.processor_result->>
                  'actual_logical_root_receipt',
                'physical_root',i.processor_result->>
                  'actual_physical_root_receipt',
                'ordered_input_root',i.processor_result->
                  'actual_ordered_input_root',
                'page_count',case when coalesce(
                    i.processor_result->>'actual_page_count','')~'^[1-9][0-9]{0,8}$'
                  then(i.processor_result->>'actual_page_count')::integer end,
                'output_sha256',i.processor_result->>'verified_candidate_sha256')::text,
              'sha256'),'hex')<>
                coalesce(i.payload_json->>'root_merge_receipt_identity','')
            or coalesce(i.processor_result->>
              'verified_candidate_sha256','')<>
                coalesce(i.payload_json->>'candidate_sha256','')
            or coalesce(i.processor_result->>'verified_candidate_size_bytes','')<>
                coalesce(i.payload_json->>'candidate_size_bytes','')
            or coalesce(i.processor_result->>'actual_page_count','')<>
                coalesce(i.expected_page_count::text,'')
            or coalesce(i.processor_result->>
              'verified_candidate_r2_key','')<>
                coalesce(i.payload_json->>'candidate_r2_key','')
            or coalesce(i.payload_json->>'resolved_input_coverage_hash','')<>
              coalesce(i.payload_json->>'expected_coverage_hash','')
            or coalesce(i.processor_result->>'assembled_input_coverage_hash','')<>
              coalesce(i.payload_json->>'expected_coverage_hash','')
            or coalesce(i.processor_result->>'assembled_input_count','')!~
              '^[0-9]{1,9}$'
            or case when coalesce(
                i.processor_result->>'assembled_input_count','')~'^[0-9]{1,9}$'
              then(i.processor_result->>'assembled_input_count')::integer end
                is distinct from case when coalesce(
                  i.payload_json->>'expected_input_count','')~'^[0-9]{1,9}$'
                  then(i.payload_json->>'expected_input_count')::integer end
            or coalesce(
                i.processor_result->>'assembled_physical_input_count','')
                !~ '^[0-9]{1,9}$'
            or jsonb_typeof(i.processor_result->'actual_input_receipts')<>'array'
            or case when coalesce(
                i.processor_result->>'assembled_physical_input_count','')
                  ~'^[0-9]{1,9}$'
              then(i.processor_result->>
                'assembled_physical_input_count')::integer end
                is distinct from case when coalesce(
                  i.payload_json->>'expected_physical_input_count','')
                    ~'^[0-9]{1,9}$'
                  then(i.payload_json->>
                    'expected_physical_input_count')::integer end
            or coalesce(
                i.processor_result->>'assembled_physical_input_hash','')<>
              coalesce(i.payload_json->>'expected_physical_input_hash','')
            or coalesce((
              select encode(digest(string_agg(concat_ws('|',
                receipt.value->>'logical_ordinal',
                receipt.value->>'physical_part_no',
                receipt.value->>'r2_key',
                receipt.value->>'sha256'),'||'
                 order by case when coalesce(
                     receipt.value->>'logical_ordinal','')~'^[0-9]{1,9}$'
                     then(receipt.value->>'logical_ordinal')::integer end,
                   case when coalesce(
                     receipt.value->>'physical_part_no','')
                       ~'^[1-9][0-9]{0,8}$'
                     then(receipt.value->>'physical_part_no')::integer end),
                'sha256'),'hex')
              from jsonb_array_elements(
                case when jsonb_typeof(
                    i.processor_result->'actual_input_receipts')='array'
                  then i.processor_result->'actual_input_receipts'
                  else '[]'::jsonb end) receipt(value)
              where coalesce(receipt.value->>'logical_ordinal','')~'^[0-9]{1,9}$'
                and coalesce(receipt.value->>'physical_part_no','')~'^[1-9][0-9]{0,8}$'
                and nullif(receipt.value->>'r2_key','') is not null
                and coalesce(receipt.value->>'sha256','')~'^[0-9a-f]{64}$'
            ),'')<>coalesce(i.payload_json->>'expected_physical_input_hash','')
            or case when coalesce(i.processor_result->>'page_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'page_count')::integer end
                is distinct from i.expected_page_count)
          then 'FINAL_VERIFICATION_MISMATCH'
        when i.outcome='SUCCESS'
          and coalesce(
            case when i.chunk_type='SOURCE_RENDER'
              then i.payload_json->>'source_revision' end,
            i.asset_source_revision,i.document_source_revision,
            i.payload_json->>'source_revision','')<>''
          and coalesce(i.processor_result->>'source_revision','')<>
            coalesce(
              case when i.chunk_type='SOURCE_RENDER'
                then i.payload_json->>'source_revision' end,
              i.asset_source_revision,i.document_source_revision,
              i.payload_json->>'source_revision','')
          then 'SOURCE_REVISION_MISMATCH'
      end result_error
    from identified i
  ),
  valid as materialized (
    select c.*,
      case
        when c.result_error is null then c.outcome
        when c.result_error in(
          'INVALID_OUTCOME','INVALID_RESULT_PAYLOAD','INVALID_INSPECTION_RESULT',
          'INVALID_ARTIFACT_RESULT') then 'RETRY'
        else 'FAILED'
      end effective_outcome,
      case when c.result_error is null then c.processor_error
        else jsonb_build_object('code',c.result_error,'retryable',
          c.result_error in(
            'INVALID_OUTCOME','INVALID_RESULT_PAYLOAD','INVALID_INSPECTION_RESULT',
            'INVALID_ARTIFACT_RESULT')) end effective_error
    from classified c where c.ownership_error is null
  )
  select
    coalesce((select jsonb_agg(jsonb_build_object(
      'request_no',v.request_no,'chunk_id',v.chunk_id,'operation_id',v.operation_id,
      'chunk_type',v.chunk_type,'phase',v.phase,'entity_type',v.entity_type,
      'entity_id',v.entity_id,'document_version_id',v.document_version_id,
      'document_asset_id',v.document_asset_id,
      'input_document_version_id',v.input_document_version_id,
      'payload_json',v.payload_json,'expected_page_count',v.expected_page_count,
      'expected_byte_count',v.expected_byte_count,'attempt_count',v.attempt_count,
      'max_attempts',v.max_attempts,'fence_token',v.fence_token,
      'processor_policy',v.processor_policy,
      'outcome',v.effective_outcome,'result',v.processor_result,'error',v.effective_error
    ) order by v.request_no) from valid v),'[]'::jsonb),
    coalesce((select jsonb_agg(jsonb_build_object(
      'request_no',c.request_no,'chunk_id',c.chunk_id,'status','REJECTED',
      'accepted',false,'code',c.ownership_error
    ) order by c.request_no) from classified c where c.ownership_error is not null),'[]'::jsonb)
  into v_valid_results,v_rejections;

  /* Successful inspection is explicit and always creates bounded next work. */
  with supplied as materialized (
    select x.value item,(x.value->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(v_valid_results) x(value)
    where x.value->>'outcome'='SUCCESS' and x.value->>'chunk_type'='ASSET_INSPECT'
  ),
  inspected_assets as materialized (
    update public.invoice_document_assets a
    set detected_media_type=case lower(coalesce(s.item#>>'{result,detected_media_type}',
          s.item#>>'{result,detected_kind}'))
          when 'pdf' then 'application/pdf'
          when 'jpeg' then 'image/jpeg' when 'jpg' then 'image/jpeg'
          when 'png' then 'image/png'
          else lower(coalesce(s.item#>>'{result,detected_media_type}',
            s.item#>>'{result,detected_kind}')) end,
        original_sha256=s.item#>>'{result,original_sha256}',
        original_size_bytes=(s.item#>>'{result,original_size_bytes}')::bigint,
        width_pixels=case when coalesce(s.item#>>'{result,width_pixels}','') ~ '^[0-9]{1,9}$'
          then(s.item#>>'{result,width_pixels}')::integer end,
        height_pixels=case when coalesce(s.item#>>'{result,height_pixels}','') ~ '^[0-9]{1,9}$'
          then(s.item#>>'{result,height_pixels}')::integer end,
        orientation_degrees=case
          when coalesce(s.item#>>'{result,orientation_degrees}','') ~ '^(0|90|180|270)$'
            then(s.item#>>'{result,orientation_degrees}')::integer
          else a.orientation_degrees end,
        source_page_count=case
          when coalesce(s.item#>>'{result,page_count}','') ~ '^[0-9]{1,9}$'
            then(s.item#>>'{result,page_count}')::integer
          when lower(coalesce(s.item#>>'{result,detected_media_type}',
            s.item#>>'{result,detected_kind}')) in(
              'image/jpeg','jpeg','jpg','image/png','png') then 1
          else null end,
        is_encrypted=false,status='NORMALISING',updated_at_utc=v_now,error_json=null
    from supplied s
    join public.invoice_operation_chunks c on c.id=s.chunk_id
    where a.id=c.document_asset_id
    returning a.*,c.operation_id work_operation_id,c.priority,
      s.item->'processor_policy' processor_policy,
      case when coalesce(s.item#>>'{result,estimated_decoded_bytes}','')
          ~ '^[0-9]{1,18}$'
        then(s.item#>>'{result,estimated_decoded_bytes}')::bigint else 0 end
        estimated_decoded_bytes,
      case when jsonb_typeof(s.item#>'{result,recommended_ranges}')='array'
        then s.item#>'{result,recommended_ranges}' else '[]'::jsonb end
        recommended_ranges
  ),
  adaptive_limits as materialized (
    select i.*,
      greatest(1,least(
        (i.processor_policy
          #>>'{asset,max_pdf_part_pages}')::integer,least(
        case when i.source_page_count>0
          then greatest(1,floor(
            (i.processor_policy
              #>>'{asset,max_part_input_bytes}')::numeric/
            greatest(i.original_size_bytes::numeric/i.source_page_count,1))::integer)
          else 1 end,
        case when i.estimated_decoded_bytes>0 and i.source_page_count>0
          then greatest(1,floor(
            (i.processor_policy
              #>>'{asset,max_part_estimated_decoded_bytes}')::numeric/
            greatest(i.estimated_decoded_bytes::numeric/i.source_page_count,1))::integer)
          else(i.processor_policy
            #>>'{asset,max_pdf_part_pages}')::integer end))) target_pages
    from inspected_assets i
  ),
  recommended_range_rows as materialized (
    select i.id,i.source_page_count,x.ordinality,
      case when coalesce(x.value->>'start','')~'^[0-9]{1,9}$'
        then(x.value->>'start')::integer end range_start,
      case when coalesce(x.value->>'end','')~'^[0-9]{1,9}$'
        then(x.value->>'end')::integer end range_end
    from adaptive_limits i
    cross join lateral jsonb_array_elements(i.recommended_ranges)
      with ordinality x(value,ordinality)
  ),
  recommended_range_checks as materialized (
    select i.id,
      jsonb_array_length(i.recommended_ranges)>0
      and count(r.ordinality)=jsonb_array_length(i.recommended_ranges)
      and bool_and(r.range_start is not null and r.range_end is not null
        and r.range_start between 1 and i.source_page_count
        and r.range_end between r.range_start and i.source_page_count)
      and min(r.range_start)=1 and max(r.range_end)=i.source_page_count
      and sum(r.range_end-r.range_start+1)=i.source_page_count
      and bool_and(coalesce(r.range_start=lagged.previous_end+1,
        r.range_start=1)) recommended_valid
    from adaptive_limits i
    left join lateral (
      select p.*,
        lag(p.range_end) over(order by p.range_start,p.range_end,p.ordinality)
          previous_end
      from recommended_range_rows p where p.id=i.id
    ) lagged on true
    left join recommended_range_rows r
      on r.id=lagged.id and r.ordinality=lagged.ordinality
    group by i.id,i.recommended_ranges,i.source_page_count
  ),
  ranges as materialized (
    select i.*,
      r.range_start,r.range_end,
      row_number() over(partition by i.id order by r.range_start)::integer-1 sequence_no
    from adaptive_limits i
    cross join lateral (
      select x.range_start,x.range_end
      from recommended_range_rows x
      join recommended_range_checks ck on ck.id=x.id
      where x.id=i.id and ck.recommended_valid
      union all
      select g,least(g+i.target_pages-1,i.source_page_count)
      from generate_series(1,i.source_page_count,i.target_pages) g
      where not coalesce((select ck.recommended_valid
        from recommended_range_checks ck where ck.id=i.id),false)
    ) r
  ),
  seeded as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_asset_id,status,priority,run_after_utc,payload_json,
      expected_page_count,expected_byte_count,operation_control_version,
      created_at_utc,updated_at_utc)
    select r.work_operation_id,'ASSET_NORMALISE','NORMALISE',
      encode(digest(concat_ws('|','ASSET_NORMALISE',r.id::text,
        r.source_revision,r.range_start::text,r.range_end::text,
        r.processor_policy->>'policy_version','1'),'sha256'),'hex'),
      r.sequence_no,
      'DOCUMENT_ASSET',r.id,r.id,'QUEUED',r.priority,v_now,
      jsonb_build_object(
        'page_range',jsonb_build_object('start',r.range_start,'end',r.range_end),
        'detected_media_type',r.detected_media_type,
        'source_revision',r.source_revision,
        'adaptive_limits',jsonb_build_object(
          'policy_version',r.processor_policy->>'policy_version',
          'max_part_pages',r.target_pages,
          'max_input_bytes',(r.processor_policy
            #>>'{asset,max_part_input_bytes}')::bigint,
          'max_estimated_decoded_bytes',(r.processor_policy
            #>>'{asset,max_part_estimated_decoded_bytes}')::bigint)),
      r.range_end-r.range_start+1,
      greatest(1,ceil(r.original_size_bytes::numeric*
        (r.range_end-r.range_start+1)/greatest(r.source_page_count,1)))::bigint,
      o.control_version,v_now,v_now
    from ranges r join public.invoice_operations o on o.id=r.work_operation_id
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing
    returning id
  ),
  completed_inspection as (
    update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',
        result_json=s.item->'result',error_json=null,
        completed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    from supplied s where c.id=s.chunk_id
    returning c.id
  )
  select count(*) into v_ignored from completed_inspection;

  /* All other validated successful artifacts become immutable chunk results. */
  with supplied as materialized (
    select x.value item,(x.value->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(v_valid_results) x(value)
    where x.value->>'outcome'='SUCCESS'
      and x.value->>'chunk_type'<>'ASSET_INSPECT'
  ),
  completed as (
    update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',result_json=s.item->'result',error_json=null,
        actual_page_count=(s.item#>>'{result,page_count}')::integer,
        actual_byte_count=(s.item#>>'{result,size_bytes}')::bigint,
        completed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    from supplied s where c.id=s.chunk_id
    returning c.id
  )
  select count(*) into v_ignored from completed;

  /*
   * The attachment index is stabilised within the frozen policy's bounded
   * deterministic pass budget.
   * A new pass is inserted before the prior pass is typed as replaced, so the
   * old pass remains current if creation of its replacement fails.
   */
  with index_results as materialized (
    select c.*,o.control_version,o.config_json->'processor_policy'
      processor_policy
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    join public.invoice_operations o on o.id=c.operation_id
    where x->>'outcome'='SUCCESS' and c.chunk_type='SOURCE_RENDER'
      and c.status='COMPLETE'
      and c.payload_json->>'render_kind'='ATTACHMENT_INDEX'
  ),
  next_pass_source as materialized (
    select r.*,
      (r.payload_json->>'layout_pass')::integer+1 next_pass,
      r.actual_page_count expected_index_page_count
    from index_results r
    where r.payload_json->>'layout_phase'='MEASURE'
       or(r.payload_json->>'layout_phase'='FINAL'
          and r.actual_page_count is distinct from
            (r.payload_json->>'expected_index_page_count')::integer
          and(r.payload_json->>'layout_pass')::integer<
            (r.processor_policy#>>
              '{attachment_index,max_render_passes}')::integer)
  ),
  final_attachments as materialized (
    select n.id prior_chunk_id,n.operation_id,n.document_version_id,
      n.sequence_no,n.priority,n.plan_generation,n.control_version,
      n.next_pass,n.expected_index_page_count,n.processor_policy,
      n.payload_json,calculated.attachments,
      calculated.source_display_count,calculated.matched_display_count,
      encode(digest(coalesce(n.payload_json->'pagination_stream',
        '[]'::jsonb)::text,'sha256'),'hex') pagination_stream_hash
    from next_pass_source n
    cross join lateral (
      select coalesce(jsonb_agg(
        jsonb_build_object(
          'row_id',coalesce(
            nullif(display_row.value->>'logical_source_key',''),
            nullif(display_row.value->>'row_id','')),
          'attachment_number',coalesce(
            (display_row.value->>'attachment_number')::integer,
            display_row.display_no::integer),
          'worker',coalesce(
            nullif(display_row.value->>'worker',''),
            nullif(display_row.value->>'source','')),
          'week_or_date',nullif(display_row.value->>'week_or_date',''),
          'document_type',coalesce(
            nullif(display_row.value->>'document_type',''),
            nullif(display_row.value->>'label',''),
            nullif(display_row.value->>'input_type','')),
          'evidence_description',coalesce(
            nullif(display_row.value->>'evidence_description',''),
            nullif(display_row.value->>'label','')),
          'reference',nullif(display_row.value->>'reference',''),
          'page_count',(display_row.value->>'page_count')::integer,
          'start_page',1+coalesce((
            select sum(case
              when preceding.value->>'input_type'='ATTACHMENT_INDEX'
                then n.expected_index_page_count
              else (preceding.value->>'page_count')::integer end)
            from jsonb_array_elements(
              case when jsonb_typeof(n.payload_json->'pagination_stream')='array'
                then n.payload_json->'pagination_stream'
                else '[]'::jsonb end) with ordinality
              preceding(value,stream_no)
            where preceding.stream_no<target.stream_no),0))
        order by target.stream_no,
          display_row.value->>'input_chunk_id'),'[]'::jsonb) attachments,
        jsonb_array_length(case
          when jsonb_typeof(n.payload_json->'display_rows')='array'
            then n.payload_json->'display_rows'
          when jsonb_typeof(n.payload_json->'attachments')='array'
            then n.payload_json->'attachments'
          else '[]'::jsonb end)::integer source_display_count,
        count(*)::integer matched_display_count
      from jsonb_array_elements(
        case when jsonb_typeof(n.payload_json->'display_rows')='array'
          then n.payload_json->'display_rows'
          when jsonb_typeof(n.payload_json->'attachments')='array'
          then n.payload_json->'attachments'
          else '[]'::jsonb end) with ordinality
        display_row(value,display_no)
      join lateral (
        select stream.stream_no
        from jsonb_array_elements(
          case when jsonb_typeof(n.payload_json->'pagination_stream')='array'
            then n.payload_json->'pagination_stream'
            else '[]'::jsonb end) with ordinality stream(value,stream_no)
        where stream.value->>'logical_source_key'=coalesce(
            display_row.value->>'logical_source_key',
            display_row.value->>'row_id')
          and coalesce(stream.value->>'is_displayed_attachment','false')='true'
        order by stream.stream_no
        limit 1
      ) target on true
    ) calculated
  ),
  replacement_index_passes as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,
      payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select f.operation_id,'SOURCE_RENDER','ATTACHMENT_INDEX_FINAL',
      encode(digest(concat_ws('|','ATTACHMENT_INDEX',
        f.document_version_id::text,f.sequence_no::text,
        f.payload_json->>'source_chunk_key','FINAL',f.next_pass::text,
        f.expected_index_page_count::text,
        encode(digest(f.attachments::text,'sha256'),'hex'),
        f.payload_json->>'template_version'),'sha256'),'hex'),
      f.plan_generation+1,f.sequence_no,'DOCUMENT',f.document_version_id,
      f.document_version_id,'QUEUED',f.priority,v_now,
      f.payload_json||jsonb_build_object(
        'layout_phase','FINAL','layout_pass',f.next_pass,
        'expected_index_page_count',f.expected_index_page_count,
        'attachments',f.attachments,
        'display_rows',f.attachments,
        'displayed_row_count',jsonb_array_length(f.attachments),
        'pagination_stream_hash',f.pagination_stream_hash,
        'expected_start_pages_hash',
          encode(digest(f.attachments::text,'sha256'),'hex'),
        'previous_layout_measurements',
          coalesce(f.payload_json->'previous_layout_measurements','[]'::jsonb)
          ||jsonb_build_array(jsonb_build_object(
            'pass',(f.payload_json->>'layout_pass')::integer,
            'phase',f.payload_json->>'layout_phase',
            'page_count',f.expected_index_page_count))),
      f.control_version,v_now,v_now
    from final_attachments f
    where f.source_display_count=f.matched_display_count
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
      do update set priority=greatest(
        public.invoice_operation_chunks.priority,excluded.priority),
        updated_at_utc=excluded.updated_at_utc
    returning id,operation_id,document_version_id,sequence_no,plan_generation
  ),
  linked_index_passes as (
    update public.invoice_operation_chunks old
    set status='SUPERSEDED',phase='SUPERSEDED',
      replaced_by_chunk_id=fresh.id,replacement_required=true,
      completed_at_utc=v_now,failed_at_utc=null,updated_at_utc=v_now,
      result_json=coalesce(old.result_json,'{}'::jsonb)||
        jsonb_build_object('replacement_chunk_id',fresh.id)
    from replacement_index_passes fresh
    where old.id in(
      select prior_chunk_id from final_attachments f
      where f.operation_id=fresh.operation_id
        and f.document_version_id=fresh.document_version_id
        and f.sequence_no=fresh.sequence_no
        and f.plan_generation+1=fresh.plan_generation)
    returning old.id
  ),
  unstable_passes as materialized (
    update public.invoice_operation_chunks c
    set status='FAILED',phase='FAILED',failed_at_utc=v_now,
      completed_at_utc=null,updated_at_utc=v_now,
      error_json=jsonb_build_object(
        'code',case
          when calc.prior_chunk_id is not null
            and calc.source_display_count<>calc.matched_display_count
            then 'ATTACHMENT_INDEX_PAGINATION_MAPPING_INVALID'
          else 'ATTACHMENT_INDEX_LAYOUT_UNSTABLE' end,
        'retryable',false,'max_layout_passes',
          (r.processor_policy#>>
            '{attachment_index,max_render_passes}')::integer,
        'expected_page_count',
          (c.payload_json->>'expected_index_page_count')::integer,
        'actual_page_count',c.actual_page_count,
        'measurements',
          coalesce(c.payload_json->'previous_layout_measurements','[]'::jsonb)
          ||jsonb_build_array(jsonb_build_object(
            'pass',(r.payload_json->>'layout_pass')::integer,
            'phase','FINAL','page_count',c.actual_page_count)))
    from index_results r
    left join final_attachments calc on calc.prior_chunk_id=r.id
    where c.id=r.id
      and(
        (calc.prior_chunk_id is not null
          and calc.source_display_count<>calc.matched_display_count)
        or(
          r.payload_json->>'layout_phase'='FINAL'
          and(r.payload_json->>'layout_pass')::integer=
            (r.processor_policy#>>
              '{attachment_index,max_render_passes}')::integer
          and r.actual_page_count is distinct from
            (r.payload_json->>'expected_index_page_count')::integer))
    returning c.id,c.operation_id,c.document_version_id,
      c.payload_json->>'source_chunk_key' source_chunk_key,c.error_json
  ),
  blocked_index_dependencies as (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
      completed_at_utc=null,updated_at_utc=v_now,
      error_json=u.error_json
    from unstable_passes u
    where d.operation_id=u.operation_id
      and d.document_version_id=u.document_version_id
      and d.chunk_type='DOCUMENT_INPUT' and d.status='WAITING'
      and d.payload_json->>'source_chunk_key'=u.source_chunk_key
    returning d.document_version_id
  ),
  failed_index_versions as (
    update public.invoice_document_versions v
    set status='FAILED',error_json=jsonb_build_object(
      'code','ATTACHMENT_INDEX_LAYOUT_UNSTABLE','retryable',false)
    where v.id in(select document_version_id
      from blocked_index_dependencies)
      and v.status not in('READY','SUPERSEDED','CANCELLED')
    returning v.id
  )
  select count(*) into v_ignored from linked_index_passes;

  /* Finalise an asset only after every adaptive output part is complete. */
  with affected as materialized (
    select distinct c.document_asset_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where x->>'outcome'='SUCCESS' and c.chunk_type='ASSET_NORMALISE'
  ),
  current_normalise as materialized (
    select c.*
    from private._invoice_current_chunks_batch(
      null,null,(select array_agg(document_asset_id) from affected),10000) g
    join public.invoice_operation_chunks c on c.id=g.current_chunk_id
    where g.replacement_chain_status='VALID'
      and c.chunk_type='ASSET_NORMALISE'
  ),
  ready as materialized (
    select a.id,
      jsonb_agg(jsonb_build_object(
        'sequence_no',c.sequence_no,'r2_key',c.result_json->>'r2_key',
        'sha256',c.result_json->>'sha256','size_bytes',c.actual_byte_count,
        'page_count',c.actual_page_count,'page_range',c.payload_json->'page_range')
        order by c.sequence_no) manifest,
      sum(c.actual_byte_count)::bigint bytes,
      sum(c.actual_page_count)::integer pages,
      min(c.result_json->>'sha256') filter(where c.sequence_no=0) single_hash,
      encode(digest(jsonb_agg(jsonb_build_object(
        'sequence_no',c.sequence_no,'r2_key',c.result_json->>'r2_key',
        'sha256',c.result_json->>'sha256','size_bytes',c.actual_byte_count,
        'page_count',c.actual_page_count,'page_range',c.payload_json->'page_range')
        order by c.sequence_no)::text,'sha256'),'hex') manifest_hash
    from affected x
    join public.invoice_document_assets a on a.id=x.document_asset_id
    join current_normalise c on c.document_asset_id=a.id
    group by a.id
    having count(*)>0 and bool_and(c.status='COMPLETE')
      and bool_and(c.actual_byte_count>0 and c.actual_page_count>0
        and coalesce(c.result_json->>'r2_key','')<>''
        and coalesce(c.result_json->>'sha256','')<>'')
      and min((c.payload_json#>>'{page_range,start}')::integer)=1
      and max((c.payload_json#>>'{page_range,end}')::integer)=a.source_page_count
      and sum((c.payload_json#>>'{page_range,end}')::integer
        -(c.payload_json#>>'{page_range,start}')::integer+1)=a.source_page_count
      and sum(c.actual_page_count)=a.source_page_count
      and not exists(
        select 1
        from (
          select
            (c2.payload_json#>>'{page_range,start}')::integer range_start,
            lag((c2.payload_json#>>'{page_range,end}')::integer)
              over(order by
                (c2.payload_json#>>'{page_range,start}')::integer,
                (c2.payload_json#>>'{page_range,end}')::integer,
                c2.sequence_no,c2.id) previous_end
          from current_normalise c2
          where c2.document_asset_id=a.id
            and coalesce(c2.payload_json#>>'{page_range,start}','')
              ~'^[0-9]{1,9}$'
            and coalesce(c2.payload_json#>>'{page_range,end}','')
              ~'^[0-9]{1,9}$'
        ) ordered_ranges
        where range_start<>coalesce(previous_end+1,1))
  ),
  updated_assets as materialized (
    update public.invoice_document_assets a
    set normalised_manifest_json=case when jsonb_array_length(r.manifest)=1
          then '[]'::jsonb else r.manifest end,
        normalised_r2_key=case when jsonb_array_length(r.manifest)=1
          then r.manifest->0->>'r2_key' else null end,
        normalised_sha256=case when jsonb_array_length(r.manifest)=1
          then r.single_hash else null end,
        normalised_manifest_hash=case when jsonb_array_length(r.manifest)>1
          then r.manifest_hash else null end,
        normalised_size_bytes=r.bytes,
        normalised_page_count=r.pages,status='READY',ready_at_utc=v_now,
        updated_at_utc=v_now,error_json=null
    from ready r where a.id=r.id
    returning a.*
  ),
  evidence_ready as (
    update public.timesheet_evidence e
    set document_asset_id=a.id,
        source_revision=a.source_revision,
        processing_state='READY',
        processing_error_json=null
    from updated_assets a
    where a.source_kind='TIMESHEET_EVIDENCE' and e.id=a.source_id
    returning e.id,e.timesheet_id,e.kind,e.document_asset_id
  ),
  manual_evidence_ready as (
    update public.timesheets t
    set manual_document_asset_id=e.document_asset_id,
        manual_pdf_r2_key=null,
        document_state='QUEUED',last_document_error_json=null,updated_at=v_now
    from evidence_ready e
    join updated_assets a on a.id=e.document_asset_id
    where upper(e.kind)='TIMESHEET' and t.timesheet_id=e.timesheet_id and t.is_current
    returning t.timesheet_id
  ),
  manual_asset_ready as (
    update public.timesheets t
    set manual_document_asset_id=a.id,
        manual_pdf_r2_key=null,
        document_state='QUEUED',last_document_error_json=null,updated_at=v_now
    from updated_assets a
    where a.source_kind='MANUAL_TIMESHEET'
      and t.timesheet_id=a.source_id and t.is_current
    returning t.timesheet_id
  ),
  manual_document_targets as materialized (
    select distinct t.timesheet_id,t.document_revision,a.operation_id
      asset_operation_id,asset_op.actor_user_id,
      asset_op.config_json->'processor_policy' processor_policy
    from updated_assets a
    join public.invoice_operations asset_op on asset_op.id=a.operation_id
    join public.timesheets t on t.is_current and(
      (a.source_kind='MANUAL_TIMESHEET' and t.timesheet_id=a.source_id)
      or(a.source_kind='TIMESHEET_EVIDENCE' and exists(
        select 1 from public.timesheet_evidence e
        where e.id=a.source_id and e.timesheet_id=t.timesheet_id
          and upper(coalesce(e.kind,''))='TIMESHEET')))
    where not exists(
      select 1 from public.invoice_document_versions v
      where v.entity_type='TIMESHEET' and v.entity_id=t.timesheet_id
        and v.purpose='TIMESHEET'
        and v.source_revision=t.document_revision::text
        and v.template_version='timesheet-professional-v1'
        and v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY'))
  ),
  inserted_manual_document_operations as materialized (
    insert into public.invoice_operations(
      parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,
      input_json,config_json,progress_json,total_units,chunk_count,
      control_version,change_seq,created_at_utc,updated_at_utc)
    select t.asset_operation_id,'BUILD_DOCUMENT','TIMESHEET',t.timesheet_id,
      t.actor_user_id,
      encode(digest(concat_ws('|','BUILD_DOCUMENT','TIMESHEET',
        t.timesheet_id::text,t.document_revision::text,
        'timesheet-professional-v1'),'sha256'),'hex'),
      'QUEUED','BUILD_MANIFEST',800,t.document_revision::text,
      'timesheet-professional-v1',
      jsonb_build_object('entity_type','TIMESHEET',
        'entity_id',t.timesheet_id,'purpose','TIMESHEET',
        'source_revision',t.document_revision::text,
        'template_version','timesheet-professional-v1',
        'reason','MANUAL_ASSET_READY'),
      jsonb_build_object('processor_policy',t.processor_policy),
      jsonb_build_object(
        'status_message','Manual timesheet document queued'),
      1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
    from manual_document_targets t
    on conflict do nothing
    returning *
  ),
  selected_manual_document_operations as materialized (
    select t.timesheet_id,t.document_revision,
      coalesce(created.id,existing.id) operation_id,
      coalesce(created.control_version,existing.control_version)
        control_version
    from manual_document_targets t
    left join inserted_manual_document_operations created
      on created.entity_id=t.timesheet_id
        and created.source_revision=t.document_revision::text
    left join lateral(
      select o.id,o.control_version
      from public.invoice_operations o
      where o.operation_type='BUILD_DOCUMENT'
        and o.entity_type='TIMESHEET' and o.entity_id=t.timesheet_id
        and o.source_revision=t.document_revision::text
        and o.template_version='timesheet-professional-v1'
        and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      order by o.created_at_utc desc,o.id desc limit 1
    ) existing on created.id is null
    where coalesce(created.id,existing.id) is not null
  ),
  inserted_manual_document_versions as materialized (
    insert into public.invoice_document_versions(
      entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,manifest_json,
      manifest_hash,created_at_utc)
    select 'TIMESHEET',s.timesheet_id,'TIMESHEET',s.operation_id,
      s.document_revision::text,'timesheet-professional-v1','PLANNING',
      '{}'::jsonb,encode(digest('{}','sha256'),'hex'),'[]'::jsonb,
      encode(digest('[]','sha256'),'hex'),v_now
    from selected_manual_document_operations s
    on conflict do nothing
    returning *
  ),
  selected_manual_document_versions as materialized (
    select s.*,coalesce(created.id,existing.id) document_version_id
    from selected_manual_document_operations s
    left join inserted_manual_document_versions created
      on created.operation_id=s.operation_id
    left join public.invoice_document_versions existing
      on created.id is null
      and existing.entity_type='TIMESHEET'
      and existing.entity_id=s.timesheet_id
      and existing.purpose='TIMESHEET'
      and existing.source_revision=s.document_revision::text
      and existing.template_version='timesheet-professional-v1'
      and existing.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING','READY')
  ),
  inserted_manual_document_plans as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,level_no,
      entity_type,entity_id,document_version_id,status,priority,
      run_after_utc,payload_json,operation_control_version,
      created_at_utc,updated_at_utc)
    select s.operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',
        s.document_version_id::text,s.document_revision::text,
        'timesheet-professional-v1','1'),'sha256'),'hex'),
      0,0,'TIMESHEET',s.timesheet_id,s.document_version_id,
      'QUEUED',800,v_now,
      jsonb_build_object('purpose','TIMESHEET',
        'source_revision',s.document_revision::text,
        'template_version','timesheet-professional-v1',
        'reason','MANUAL_ASSET_READY'),
      s.control_version,v_now,v_now
    from selected_manual_document_versions s
    where s.document_version_id is not null
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
      do nothing
    returning operation_id,document_version_id
  ),
  manual_document_pointer_updates as (
    update public.timesheets t
    set active_document_operation_id=s.operation_id,
      document_state=case when v.status='READY' then 'READY' else 'QUEUED' end,
      current_document_version_id=case when v.status='READY'
        then v.id else t.current_document_version_id end,
      updated_at=v_now
    from selected_manual_document_versions s
    join public.invoice_document_versions v
      on v.id=s.document_version_id
    where t.timesheet_id=s.timesheet_id and t.is_current
    returning t.timesheet_id
  ),
  released_dependencies as materialized (
    update public.invoice_operation_chunks d
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
        result_json=jsonb_build_object(
          'asset_id',a.id,'source_revision',a.source_revision,
          'normalised_r2_key',a.normalised_r2_key,
          'normalised_manifest',a.normalised_manifest_json,
          'parts',a.normalised_manifest_json,
          'sha256',a.normalised_sha256,
          'normalised_manifest_hash',a.normalised_manifest_hash,
          'size_bytes',a.normalised_size_bytes,
          'page_count',a.normalised_page_count),
        expected_page_count=a.normalised_page_count,
        actual_page_count=a.normalised_page_count,
        expected_byte_count=a.normalised_size_bytes,
        actual_byte_count=a.normalised_size_bytes,error_json=null
    from updated_assets a
    where d.chunk_type='DOCUMENT_INPUT' and d.document_asset_id=a.id
      and(
        d.status='WAITING'
        or(d.status='BLOCKED'
          and d.error_json->>'code'='DOCUMENT_DEPENDENCY_PERMANENT_FAILURE'))
    returning d.id,d.operation_id,d.document_version_id
  ),
  reset_document_versions as (
    update public.invoice_document_versions v
    set status='WAITING_FOR_INPUTS',error_json=null
    where v.id in(select document_version_id from released_dependencies)
      and v.status='FAILED'
      and v.error_json->>'code'='DOCUMENT_DEPENDENCY_PERMANENT_FAILURE'
    returning v.id
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',phase='WAIT_FOR_INPUTS',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN'
      and p.status in('QUEUED','WAITING','RETRY_WAIT','BLOCKED')
      and p.document_version_id in(
        select document_version_id from released_dependencies)
    returning p.id
  ),
  released_operation_updates as materialized (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(distinct d.operation_id)
        from released_dependencies d),array[]::uuid[]),
      v_now,true) r
  )
  select count(*) into v_ignored from released_dependencies;

  /*
   * A terminal set of normalisation parts that does not cover the registered
   * source exactly is a permanent asset failure.  It must never leave the
   * asset in NORMALISING with every worker chunk complete.
   */
  with affected as materialized (
    select distinct c.document_asset_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where x->>'outcome'='SUCCESS' and c.chunk_type='ASSET_NORMALISE'
  ),
  current_normalise as materialized (
    select c.*
    from private._invoice_current_chunks_batch(
      null,null,(select array_agg(document_asset_id) from affected),10000) g
    join public.invoice_operation_chunks c on c.id=g.current_chunk_id
    where g.replacement_chain_status='VALID'
      and c.chunk_type='ASSET_NORMALISE'
  ),
  coverage_failures as materialized (
    update public.invoice_document_assets a
    set status='FAILED',updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','ASSET_RANGE_COVERAGE_MISMATCH',
          'class','PERMANENT','retryable',false,
          'source_page_count',a.source_page_count)
    where a.id in(select document_asset_id from affected)
      and a.status='NORMALISING'
      and exists(
        select 1 from current_normalise n
        where n.document_asset_id=a.id)
      and not exists(
        select 1 from current_normalise n
        where n.document_asset_id=a.id and n.status<>'COMPLETE')
    returning a.*
  ),
  evidence_failed as (
    update public.timesheet_evidence e
    set processing_state='FAILED',processing_error_json=a.error_json
    from coverage_failures a
    where a.source_kind='TIMESHEET_EVIDENCE' and e.id=a.source_id
    returning e.id,e.timesheet_id,e.kind
  ),
  manual_failed as (
    update public.timesheets t
    set document_state='FAILED',last_document_error_json=a.error_json,
        updated_at=v_now
    from coverage_failures a
    where t.is_current and(
       (a.source_kind='MANUAL_TIMESHEET' and t.timesheet_id=a.source_id)
       or(a.source_kind='TIMESHEET_EVIDENCE'
          and exists(select 1 from evidence_failed e
            where e.id=a.source_id and upper(e.kind)='TIMESHEET'
              and e.timesheet_id=t.timesheet_id)))
    returning t.timesheet_id
  ),
  blocked_inputs as materialized (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        updated_at_utc=v_now,error_json=jsonb_build_object(
          'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
          'asset_error',a.error_json,'document_asset_id',a.id)
    from coverage_failures a
    where d.chunk_type='DOCUMENT_INPUT' and d.document_asset_id=a.id
      and d.status in('WAITING','BLOCKED')
    returning d.operation_id,d.document_version_id
  ),
  failed_versions as materialized (
    update public.invoice_document_versions v
    set status='FAILED',error_json=jsonb_build_object(
      'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
      'asset_code','ASSET_RANGE_COVERAGE_MISMATCH')
    where v.id in(select document_version_id from blocked_inputs)
      and v.status not in('READY','SUPERSEDED','CANCELLED')
    returning v.id
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',phase='WAIT_FOR_INPUTS',run_after_utc=v_now,
        updated_at_utc=v_now,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN'
      and p.document_version_id in(select id from failed_versions)
      and p.status in('WAITING','RETRY_WAIT','BLOCKED')
    returning p.id
  ),
  issue_block as (
    update public.invoice_operation_chunks i
    set status='BLOCKED',phase='WAIT_DOCUMENT',failed_at_utc=v_now,
        updated_at_utc=v_now,error_json=jsonb_build_object(
          'code','FINAL_DOCUMENT_PERMANENT_FAILURE',
          'document_version_id',v.id,
          'document_error','ASSET_RANGE_COVERAGE_MISMATCH')
    from failed_versions v
    where i.chunk_type='ISSUE_INVOICE' and i.phase='WAIT_DOCUMENT'
      and i.status in('WAITING','RETRY_WAIT')
      and coalesce(
        case when coalesce(i.payload_json->>'document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'document_version_id')::uuid end,
        case when coalesce(i.payload_json->>'final_document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'final_document_version_id')::uuid end)=v.id
    returning i.id
  )
  select count(*) into v_ignored from coverage_failures;

  /* Source/core render dependencies may also be shared across child operations. */
  with rendered as materialized (
    select c.*
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where x->>'outcome'='SUCCESS'
      and c.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
      and c.status='COMPLETE'
  ),
  released as materialized (
    update public.invoice_operation_chunks d
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
        result_json=r.result_json,actual_page_count=r.actual_page_count,
        actual_byte_count=r.actual_byte_count,error_json=null
    from rendered r
    where d.chunk_type='DOCUMENT_INPUT' and d.status='WAITING'
      and d.document_version_id=r.document_version_id
      and(
        d.payload_json->>'source_chunk_key'=r.payload_json->>'source_chunk_key'
        or(d.entity_type=r.entity_type and d.entity_id=r.entity_id
          and d.payload_json->>'input_type'=case
            when r.chunk_type='INVOICE_CORE_RENDER' then 'INVOICE_CORE'
            else r.payload_json->>'render_kind' end))
    returning d.operation_id,d.document_version_id
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',phase='WAIT_FOR_INPUTS',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN' and p.status in('WAITING','RETRY_WAIT')
      and p.document_version_id in(select document_version_id from released)
    returning p.id
  )
  select count(*) into v_ignored from released;

  /*
   * Verification is authoritative only when it agrees with the independently
   * frozen manifest hash, coverage hash, order and expected page count.
   */
  with verified as materialized (
    select c.id chunk_id,c.operation_id,c.document_version_id,x->'result' result
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    join public.invoice_document_versions v on v.id=c.document_version_id
    where x->>'outcome'='SUCCESS' and c.chunk_type='DOCUMENT_VERIFY'
      and c.status='COMPLETE'
      and x#>>'{result,parse_verified}'='true'
      and x#>>'{result,manifest_coverage_verified}'='true'
      and x#>>'{result,ordering_verified}'='true'
      and x#>>'{result,manifest_hash}'=v.manifest_hash
      and c.payload_json->>'resolved_input_coverage_hash'=
        c.payload_json->>'expected_coverage_hash'
      and x#>>'{result,assembled_input_coverage_hash}'=
        c.payload_json->>'expected_coverage_hash'
      and coalesce(x#>>'{result,assembled_input_count}','')~'^[0-9]{1,9}$'
      and coalesce(c.payload_json->>'expected_input_count','')~'^[0-9]{1,9}$'
      and(x#>>'{result,assembled_input_count}')::integer=
        (c.payload_json->>'expected_input_count')::integer
      and coalesce(x#>>'{result,assembled_physical_input_count}','')
        ~'^[0-9]{1,9}$'
      and coalesce(c.payload_json->>'expected_physical_input_count','')
        ~'^[0-9]{1,9}$'
      and(x#>>'{result,assembled_physical_input_count}')::integer=
        (c.payload_json->>'expected_physical_input_count')::integer
      and x#>>'{result,assembled_physical_input_hash}'=
        c.payload_json->>'expected_physical_input_hash'
      and x#>>'{result,actual_logical_root_receipt}'=
        c.payload_json->>'expected_logical_root_receipt'
      and x#>>'{result,actual_physical_root_receipt}'=
        c.payload_json->>'expected_physical_root_receipt'
      and x#>>'{result,actual_ordered_input_root}'=
        c.payload_json->>'expected_ordered_input_root'
      and x#>>'{result,root_merge_receipt_identity}'=
        c.payload_json->>'root_merge_receipt_identity'
      and x#>>'{result,verified_candidate_sha256}'=
        c.payload_json->>'candidate_sha256'
      and x#>>'{result,verified_candidate_r2_key}'=
        c.payload_json->>'candidate_r2_key'
      and x#>>'{result,verified_candidate_size_bytes}'=c.payload_json->>'candidate_size_bytes'
      and coalesce(x#>>'{result,actual_page_count}','')~'^[0-9]{1,9}$'
      and(x#>>'{result,actual_page_count}')::integer=c.expected_page_count
  ),
  ready_versions as materialized (
    update public.invoice_document_versions v
    set status='READY',r2_key=q.result->>'verified_candidate_r2_key',
        sha256=q.result->>'verified_candidate_sha256',
        size_bytes=(q.result->>'verified_candidate_size_bytes')::bigint,
        page_count=(q.result->>'actual_page_count')::integer,
        core_page_count=case when coalesce(q.result->>'core_page_count','') ~ '^[0-9]{1,9}$'
          then(q.result->>'core_page_count')::integer end,
        supporting_page_count=case
          when coalesce(q.result->>'supporting_page_count','') ~ '^[0-9]{1,9}$'
          then(q.result->>'supporting_page_count')::integer end,
        ready_at_utc=v_now,verified_at_utc=v_now,error_json=null
    from verified q where v.id=q.document_version_id
    returning v.*
  ),
  invoice_preview_ready as (
    update public.invoices i
    set preview_document_version_id=v.id,document_state='READY',
        invoice_pdf_r2_key=v.r2_key,invoice_pdf_generated_at_utc=v.verified_at_utc,
        active_document_operation_id=case when i.active_document_operation_id=v.operation_id
          then null else i.active_document_operation_id end,
        last_document_error_json=null,updated_at=v_now
    from ready_versions v
    where v.entity_type='INVOICE' and v.purpose='DRAFT_PREVIEW'
      and i.id=v.entity_id and i.document_revision::text=v.source_revision
    returning i.id
  ),
  timesheet_document_ready as (
    update public.timesheets t
    set current_document_version_id=v.id,document_state='READY',
        manual_pdf_r2_key=case when t.manual_document_asset_id is not null
          then v.r2_key else t.manual_pdf_r2_key end,
        active_document_operation_id=case when t.active_document_operation_id=v.operation_id
          then null else t.active_document_operation_id end,
        last_document_error_json=null,updated_at=v_now
    from ready_versions v
    where v.entity_type='TIMESHEET' and v.purpose='TIMESHEET'
      and t.timesheet_id=v.entity_id and t.document_revision::text=v.source_revision
    returning t.timesheet_id
  ),
  issue_wake as (
    update public.invoice_operation_chunks i
    set status='QUEUED',phase='FINALISE',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,error_json=null
    where i.chunk_type='ISSUE_INVOICE' and i.phase='WAIT_DOCUMENT'
      and i.status in('WAITING','RETRY_WAIT')
      and coalesce(
        case when coalesce(i.payload_json->>'document_version_id','') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'document_version_id')::uuid end,
        case when coalesce(i.payload_json->>'final_document_version_id','') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'final_document_version_id')::uuid end
      ) in(
        select id from ready_versions where purpose='FINAL_ISSUE')
    returning i.operation_id
  )
  select count(*) into v_ignored from ready_versions;

  /*
   * Non-success outcomes, and malformed current success results reclassified
   * above, receive one explicit transition with bounded error history.
   */
  with supplied as materialized (
    select x.value item,(x.value->>'chunk_id')::uuid chunk_id,
      upper(x.value->>'outcome') outcome,
      upper(coalesce(x.value#>>'{error,code}','PROCESSOR_FAILURE')) error_code,
      (lower(coalesce(x.value#>>'{error,transient}','false'))='true'
        or upper(coalesce(x.value#>>'{error,class}',''))='TRANSIENT'
        or upper(x.value->>'outcome')='RETRY') transient
    from jsonb_array_elements(v_valid_results) x(value)
    where x.value->>'outcome'<>'SUCCESS'
  ),
  transitioned as materialized (
    update public.invoice_operation_chunks c
    set status=case
          when s.outcome='CANCELLED' then 'CANCELLED'
          when s.outcome='SUPERSEDED' then 'SUPERSEDED'
          when s.outcome='BLOCKED' then 'BLOCKED'
          when s.transient and c.attempt_count<c.max_attempts then 'RETRY_WAIT'
          when s.transient then 'DEAD_LETTER'
          else 'FAILED' end,
        phase=case
          when s.outcome='CANCELLED' then 'CANCELLED'
          when s.outcome='SUPERSEDED' then 'SUPERSEDED'
          when s.outcome='BLOCKED' then 'BLOCKED'
          when s.transient and c.attempt_count<c.max_attempts then c.phase
          when s.transient then 'DEAD_LETTER'
          else 'FAILED' end,
        run_after_utc=case when s.transient and c.attempt_count<c.max_attempts
          then v_now+make_interval(secs=>
            least(1800,30*(2^least(greatest(c.attempt_count-1,0),6)))::integer
            +floor(random()*least(60,5*greatest(c.attempt_count,1)))::integer)
          else c.run_after_utc end,
        error_json=jsonb_build_object(
          'code',s.error_code,
          'class',case when s.transient then 'TRANSIENT' else 'PERMANENT' end,
          'retryable',s.transient and c.attempt_count<c.max_attempts,
          'details',s.item->'error',
          'history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                 with ordinality h(value,ordinality)
            where h.ordinality>greatest(
              jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now))),
        failed_at_utc=case
          when not(s.transient and c.attempt_count<c.max_attempts) then v_now else null end,
        completed_at_utc=case when s.outcome in('CANCELLED','SUPERSEDED') then v_now
          else c.completed_at_utc end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now
    from supplied s where c.id=s.chunk_id
    returning c.*
  ),
  asset_failures as materialized (
    update public.invoice_document_assets a
    set status=case upper(coalesce(t.error_json->>'code',''))
          when 'ASSET_MEDIA_TYPE_UNSUPPORTED' then 'UNSUPPORTED'
          when 'ASSET_PDF_ENCRYPTED' then 'UNSUPPORTED'
          when 'CORRUPT_PDF' then 'CORRUPT'
          when 'TRUNCATED_FILE' then 'CORRUPT'
          when 'EMPTY_FILE' then 'CORRUPT'
          when 'MISSING_SOURCE' then 'MISSING'
          else 'FAILED' end,
        error_json=t.error_json,updated_at_utc=v_now
    from transitioned t
    where a.id=t.document_asset_id
      and t.status in('FAILED','DEAD_LETTER','BLOCKED')
      and a.status not in('READY','SUPERSEDED')
    returning a.*
  ),
  evidence_failed as (
    update public.timesheet_evidence e
    set processing_state=a.status,processing_error_json=a.error_json
    from asset_failures a
    where a.source_kind='TIMESHEET_EVIDENCE' and e.id=a.source_id
    returning e.id,e.timesheet_id,e.kind,e.document_asset_id
  ),
  manual_evidence_failed as (
    update public.timesheets t
    set document_state='FAILED',last_document_error_json=a.error_json,updated_at=v_now
    from evidence_failed e
    join asset_failures a on a.id=e.document_asset_id
    where upper(e.kind)='TIMESHEET' and t.timesheet_id=e.timesheet_id and t.is_current
    returning t.timesheet_id
  ),
  manual_asset_failed as (
    update public.timesheets t
    set document_state='FAILED',last_document_error_json=a.error_json,
        updated_at=v_now
    from asset_failures a
    where a.source_kind='MANUAL_TIMESHEET'
      and t.timesheet_id=a.source_id and t.is_current
    returning t.timesheet_id
  ),
  blocked_dependencies as materialized (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
          'document_asset_id',a.id,'source_kind',a.source_kind,
          'source_id',a.source_id,'asset_error',a.error_json)
    from asset_failures a
    where d.chunk_type='DOCUMENT_INPUT' and d.document_asset_id=a.id
      and d.status='WAITING'
    returning d.id,d.operation_id,d.document_version_id,d.error_json
  ),
  failed_document_versions as (
    update public.invoice_document_versions v
    set status='FAILED',error_json=jsonb_build_object(
          'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
          'source_errors',coalesce((
            select jsonb_agg(d.error_json order by d.id)
            from blocked_dependencies d
            where d.document_version_id=v.id),'[]'::jsonb)),
        superseded_at_utc=null
    where v.id in(select document_version_id from blocked_dependencies)
      and v.status not in('READY','SUPERSEDED','CANCELLED')
    returning v.id,v.operation_id,v.entity_type,v.entity_id,v.purpose,v.error_json
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',phase='WAIT_FOR_INPUTS',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN'
      and p.document_version_id in(select document_version_id from blocked_dependencies)
      and p.status in('WAITING','RETRY_WAIT','BLOCKED')
    returning p.id
  ),
  issue_block as (
    update public.invoice_operation_chunks i
    set status='BLOCKED',phase='WAIT_DOCUMENT',failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','FINAL_DOCUMENT_PERMANENT_FAILURE',
          'document_version_id',v.id,'document_error',v.error_json)
    from failed_document_versions v
    where i.chunk_type='ISSUE_INVOICE' and i.phase='WAIT_DOCUMENT'
      and i.status in('WAITING','RETRY_WAIT')
      and coalesce(
        case when coalesce(i.payload_json->>'document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'document_version_id')::uuid end,
        case when coalesce(i.payload_json->>'final_document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'final_document_version_id')::uuid end)=v.id
    returning i.operation_id
  ),
  blocked_operation_updates as materialized (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(distinct affected.operation_id)
        from (
          select d.operation_id from blocked_dependencies d
          union
          select i.operation_id from issue_block i
        ) affected),array[]::uuid[]),
      v_now,true) r
  )
  select count(*) into v_ignored from transitioned;

  /* Merge completion or dependency change wakes the owning plan exactly once. */
  with affected_versions as materialized (
    select distinct c.document_version_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where c.document_version_id is not null
      and c.chunk_type in(
        'ASSET_NORMALISE','SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE','DOCUMENT_VERIFY')
      and c.status in('COMPLETE','FAILED','DEAD_LETTER','BLOCKED')
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN' and p.status in('WAITING','RETRY_WAIT')
      and p.document_version_id in(select document_version_id from affected_versions)
    returning p.id
  )
  select count(*) into v_ignored from plan_wake;

  with affected as materialized (
    select distinct c.operation_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    union
    select distinct d.operation_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks source
      on source.id=(x->>'chunk_id')::uuid
    join public.invoice_operation_chunks d
      on d.chunk_type='DOCUMENT_INPUT'
      and d.document_asset_id=source.document_asset_id
    where source.document_asset_id is not null
    union
    select distinct p.operation_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks source
      on source.id=(x->>'chunk_id')::uuid
    join public.invoice_operation_chunks p
      on p.chunk_type='DOCUMENT_PLAN'
      and p.document_version_id=source.document_version_id
    where source.document_version_id is not null
  ),
  operation_updates as (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(operation_id) from affected),
        array[]::uuid[]),v_now,true) r
  )
  select count(*) into v_ignored from operation_updates;

  with valid_items as (
    select x.value item from jsonb_array_elements(v_valid_results) x(value)
  ),
  results as (
    select (item->>'request_no')::integer request_no,jsonb_build_object(
      'chunk_id',c.id,'status',c.status,'phase',c.phase,'accepted',true,
      'run_after_utc',c.run_after_utc,'attempt_count',c.attempt_count,
      'error',c.error_json,'result',case when c.status='COMPLETE' then c.result_json end) result
    from valid_items v join public.invoice_operation_chunks c
      on c.id=(v.item->>'chunk_id')::uuid
    union all
    select (x.value->>'request_no')::integer request_no,x.value result
    from jsonb_array_elements(v_rejections) x(value)
  )
  select coalesce(jsonb_agg(result order by request_no),'[]'::jsonb)
  into v_result from results;

  return v_result;
end;
$function$;

revoke all on function public.invoice_work_complete_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function public.invoice_work_complete_batch(jsonb,timestamptz)
  to service_role;
