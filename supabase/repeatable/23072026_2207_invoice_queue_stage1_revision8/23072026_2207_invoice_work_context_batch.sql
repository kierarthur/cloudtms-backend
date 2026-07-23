create or replace function public.invoice_work_context_batch(
  p_claims jsonb,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb;
begin
  if jsonb_typeof(p_claims)<>'array'
     or jsonb_array_length(p_claims)<1
     or jsonb_array_length(p_claims)>100 then
    raise exception using errcode='22023',
      message='p_claims must be an array containing 1..100 claims';
  end if;

  with supplied as materialized (
    select x.ordinality::integer request_no,x.value raw_claim,
      case when coalesce(x.value->>'chunk_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'chunk_id')::uuid end chunk_id,
      case when coalesce(x.value->>'lease_token','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'lease_token')::uuid end lease_token,
      case when coalesce(x.value->>'fence_token','') ~ '^[0-9]{1,18}$'
        then (x.value->>'fence_token')::bigint end fence_token,
      case when coalesce(x.value->>'operation_control_version','') ~ '^[0-9]{1,18}$'
        then (x.value->>'operation_control_version')::bigint end operation_control_version
    from jsonb_array_elements(p_claims) with ordinality x(value,ordinality)
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
    select s.*,c.operation_id,c.chunk_type,c.phase,c.entity_type,c.entity_id,
      c.document_version_id,c.document_asset_id,c.input_document_version_id,
      c.payload_json,c.plan_generation,c.level_no,c.sequence_no,
      c.expected_page_count,c.expected_byte_count,
      c.lease_token current_lease_token,c.fence_token current_fence_token,
      c.operation_control_version current_control_version,
      c.lease_expires_at_utc,c.status current_status,
      o.control_version operation_current_control_version,o.status operation_status,
      o.config_json operation_config,
      jsonb_build_object(
        'chunk_id',v.chunk_id,
        'fence_token',v.fence_token,
        'plan_generation',v.plan_generation,
        'action',v.chunk_type,
        'document_version_id',v.document_version_id,
        'document_asset_id',v.document_asset_id,
        'source_revision',coalesce(v.asset_source_revision,
          v.document_source_revision,v.payload_json->>'source_revision'),
        'template_version',v.template_version,
        'processor_policy_version',v.processor_limits->>'version',
        'render_kind',v.payload_json->>'render_kind',
        'ordered_input_hash',v.payload_json->>'ordered_input_hash'
      ) || case
        when s.chunk_id is null or s.lease_token is null
          or s.fence_token is null or s.operation_control_version is null then 'INVALID_CLAIM'
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
  ),
  valid as materialized (
    select i.*,a.id asset_exists,a.source_kind,a.source_id,a.source_revision asset_source_revision,
      a.original_r2_key,a.original_filename,a.declared_media_type,a.detected_media_type,
      a.orientation_degrees,a.source_page_count,a.normalised_manifest_json,
      dv.id version_exists,dv.entity_type document_entity_type,dv.entity_id document_entity_id,
      dv.purpose,dv.source_revision document_source_revision,dv.template_version,
      dv.snapshot_json,dv.snapshot_hash,dv.manifest_json,dv.manifest_hash,
      input_v.status input_document_status,input_v.r2_key input_document_r2_key,
      input_v.sha256 input_document_sha256,input_v.size_bytes input_document_size_bytes,
      input_v.page_count input_document_page_count,
      i.operation_config->'processor_policy' processor_limits
    from inspected i
    left join public.invoice_document_assets a on a.id=i.document_asset_id
    left join public.invoice_document_versions dv on dv.id=i.document_version_id
    left join public.invoice_document_versions input_v on input_v.id=i.input_document_version_id
    where i.ownership_error is null
  ),
  source_models as materialized (
    select v.chunk_id,m.value manifest_item,
      case
        when m.value->>'input_type'='ELECTRONIC_TIMESHEET' then coalesce(
          (select x.value->'render_model'
           from jsonb_array_elements(
             case when jsonb_typeof(v.snapshot_json->'timesheet_sources')='array'
               then v.snapshot_json->'timesheet_sources' else '[]'::jsonb end)
             x(value)
           where x.value->>'timesheet_id'=m.value->>'source_entity_id'
           limit 1),
          case when v.document_entity_type='TIMESHEET'
            then v.snapshot_json end)
        when m.value->>'input_type' in('HEALTHROSTER_SUPPORT','NHSP_SUPPORT')
          then coalesce(
            (select x.value
             from jsonb_array_elements(
               case when jsonb_typeof(v.snapshot_json->'source_support')='array'
                 then v.snapshot_json->'source_support'
                 when jsonb_typeof(v.snapshot_json->'supporting_sources')='array'
                 then v.snapshot_json->'supporting_sources'
                 else '[]'::jsonb end) x(value)
             where x.value->>'import_id'=m.value->>'source_entity_id'
             limit 1),'{}'::jsonb)
        when m.value->>'input_type'='HIGHER_RATE_SUPPORT' then
          jsonb_build_object('lines',coalesce((
            select jsonb_agg(x.value order by x.ordinality)
            from jsonb_array_elements(
              case when jsonb_typeof(v.snapshot_json->'lines')='array'
                then v.snapshot_json->'lines' else '[]'::jsonb end)
              with ordinality x(value,ordinality)
            where upper(coalesce(x.value#>>'{business_meta,line_type}',''))
                like '%HIGHER%'
              or coalesce(x.value->'business_meta','{}'::jsonb)?'higher_rate'
          ),'[]'::jsonb))
        when m.value->>'input_type' in('ATTACHMENT_INDEX','SECTION_SEPARATOR')
          then jsonb_build_object(
            'display_label',m.value->>'display_label',
            'input_type',m.value->>'input_type',
            'manifest_ordinal',m.value->'ordinal')
        else coalesce(m.value->'frozen_model','{}'::jsonb)
      end frozen_model
    from valid v
    left join lateral jsonb_array_elements(
      case when jsonb_typeof(v.manifest_json)='array' then v.manifest_json else '[]'::jsonb end
    ) m(value) on
      case when coalesce(v.payload_json->>'manifest_ordinal','') ~ '^[0-9]{1,9}$'
        then (m.value->>'ordinal')::integer=(v.payload_json->>'manifest_ordinal')::integer
        else m.value->>'source_chunk_key'=v.payload_json->>'source_chunk_key' end
    where v.chunk_type='SOURCE_RENDER'
  ),
  projected as materialized (
    select v.request_no,v.chunk_id,v.operation_id,v.chunk_type,v.phase,
      v.entity_type,v.entity_id,v.document_version_id,v.document_asset_id,
      case
        when v.processor_limits is null
          or jsonb_typeof(v.processor_limits)<>'object'
          or nullif(v.processor_limits->>'policy_version','') is null
          then 'PROCESSOR_POLICY_MISSING'
        when v.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE') and v.asset_exists is null
          then 'ASSET_NOT_FOUND'
        when v.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE','DOCUMENT_VERIFY')
          and v.version_exists is null then 'DOCUMENT_VERSION_NOT_FOUND'
        when v.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          and(jsonb_typeof(v.snapshot_json)<>'object' or v.snapshot_json='{}'::jsonb)
          then 'FROZEN_SNAPSHOT_MISSING'
        when v.chunk_type='SOURCE_RENDER' and sm.manifest_item is null
          then 'SOURCE_MANIFEST_ITEM_MISSING'
        when v.chunk_type='SOURCE_RENDER'
          and coalesce(sm.frozen_model,'{}'::jsonb)='{}'::jsonb
          then 'SOURCE_FROZEN_MODEL_MISSING'
        when v.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE')
          and nullif(v.payload_json->>'source_revision','') is not null
          and v.payload_json->>'source_revision'<>v.asset_source_revision
          then 'SOURCE_REVISION_CHANGED'
        when v.chunk_type='SOURCE_RENDER'
          and nullif(v.payload_json->>'source_revision','') is not null
          and v.payload_json->>'source_revision'
            is distinct from sm.manifest_item->>'source_revision'
          then 'SOURCE_REVISION_CHANGED'
        when v.input_document_version_id is not null and v.input_document_status<>'READY'
          then 'INPUT_DOCUMENT_NOT_READY'
        when v.chunk_type='DOCUMENT_VERIFY'
          and(coalesce(v.payload_json->>'candidate_r2_key','')=''
            or coalesce(v.payload_json->>'candidate_sha256','')='')
          then 'FINAL_CANDIDATE_MISSING'
      end context_error,
      case
        when v.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE') then jsonb_build_object(
          'original_r2_key',v.original_r2_key,
          'original_filename',v.original_filename,
          'declared_media_type',v.declared_media_type,
          'detected_media_type',v.detected_media_type,
          'source_kind',v.source_kind,'source_id',v.source_id,
          'source_revision',v.asset_source_revision,
          'orientation_degrees',v.orientation_degrees,
          'source_page_count',v.source_page_count,
          'normalised_manifest',v.normalised_manifest_json,
          'page_range',v.payload_json->'page_range',
          'output_profile',coalesce(v.processor_limits->'asset','{}'::jsonb)
            ||jsonb_build_object(
              'mime_type','application/pdf','preserve_readability',true,
              'allow_upscale',false,'reject_encrypted',true),
          'immutable_destination_prefix','invoice-assets/'||v.document_asset_id||'/'||
            v.asset_source_revision||'/'||v.chunk_id||'/'||v.fence_token||'/')
        when v.chunk_type='SOURCE_RENDER' then jsonb_build_object(
          'render_kind',coalesce(sm.manifest_item->>'render_kind',
            sm.manifest_item->>'input_type',v.payload_json->>'render_kind'),
          'source_entity_type',sm.manifest_item->>'source_entity_type',
          'source_entity_id',sm.manifest_item->>'source_entity_id',
          'source_revision',sm.manifest_item->>'source_revision',
          'manifest_ordinal',sm.manifest_item->'ordinal',
          'frozen_presentation_model',coalesce(
            sm.frozen_model,'{}'::jsonb),
          'asset_dependencies',coalesce(sm.manifest_item->'asset_dependencies','[]'::jsonb),
          'attachment_index_layout',case
            when v.payload_json->>'render_kind'='ATTACHMENT_INDEX'
            then jsonb_build_object(
              'layout_phase',v.payload_json->>'layout_phase',
              'layout_pass',v.payload_json->'layout_pass',
              'max_layout_passes',v.payload_json->'max_layout_passes',
              'expected_index_page_count',
                v.payload_json->'expected_index_page_count',
               'core_page_count',v.payload_json->'core_page_count',
               'attachments',v.payload_json->'attachments',
               'display_rows',v.payload_json->'display_rows',
               'displayed_row_count',v.payload_json->'displayed_row_count',
               'pagination_stream',v.payload_json->'pagination_stream',
               'pagination_stream_hash',
                 v.payload_json->>'pagination_stream_hash',
               'expected_start_pages_hash',
                 v.payload_json->>'expected_start_pages_hash',
              'determinism',v.payload_json->'determinism',
              'prior_measurements',
                v.payload_json->'previous_layout_measurements')
            else null end,
          'template_version',v.template_version,
          'immutable_destination_prefix','invoice-documents/'||v.document_version_id||
            '/source/'||v.chunk_id||'/'||v.fence_token||'/')
        when v.chunk_type='INVOICE_CORE_RENDER' then jsonb_build_object(
          'render_kind','INVOICE_CORE',
          'document_entity_type',v.document_entity_type,
          'document_entity_id',v.document_entity_id,
          'purpose',v.purpose,
          'source_revision',v.document_source_revision,
          'frozen_invoice_snapshot',v.snapshot_json,
          'attachment_index',coalesce(v.snapshot_json->'attachment_index','[]'::jsonb),
          'template_version',v.template_version,
          'immutable_destination_prefix','invoice-documents/'||v.document_version_id||
            '/core/'||v.chunk_id||'/'||v.fence_token||'/')
        when v.chunk_type='PDF_MERGE' then jsonb_build_object(
          'ordered_inputs',v.payload_json->'inputs',
          'merge_level',v.level_no,'sequence_no',v.sequence_no,
          'plan_generation',v.plan_generation,
          'expected_page_count',v.expected_page_count,
          'expected_byte_count',v.expected_byte_count,
          'expected_ordered_input_hash',
            v.payload_json->>'ordered_input_hash',
          'expected_child_receipt_hash',
            v.payload_json->>'expected_child_receipt_hash',
          'expected_logical_receipt_root',
            v.payload_json->>'expected_logical_receipt_root',
          'expected_physical_receipt_root',
            v.payload_json->>'expected_physical_receipt_root',
          'expected_parent_receipt_hash',encode(digest(
            jsonb_build_object(
              'child_receipt_hash',
                v.payload_json->>'expected_child_receipt_hash',
              'logical_receipt_root',
                v.payload_json->>'expected_logical_receipt_root',
              'physical_receipt_root',
                v.payload_json->>'expected_physical_receipt_root',
              'expected_page_count',v.expected_page_count,
              'plan_generation',v.plan_generation)::text,
            'sha256'),'hex'),
          'required_receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
          'required_object_receipt_contract',
            'ACTUAL_BYTES_OBJECT_RECEIPT_V3',
          'required_logical_receipt_contract',
            'LOGICAL_SOURCE_RECEIPT_V3',
          'receipt_evidence_requirements',jsonb_build_object(
            'hash_actual_fetched_bytes',true,
            'hash_algorithm','SHA-256',
            'parse_each_pdf',true,
            'report_actual_page_count',true,
            'report_actual_byte_count',true,
            'preserve_actual_input_order',true,
            'include_processor_and_parser_versions',true),
          'processor_policy_version',v.processor_limits->>'version',
          'limits',coalesce(v.payload_json->'limits',
            v.processor_limits->'merge','{}'::jsonb),
          'immutable_destination_prefix','invoice-documents/'||v.document_version_id||
            '/merge/'||v.level_no||'/'||v.sequence_no||'/'||v.chunk_id||'/'||
            v.fence_token||'/')
        when v.chunk_type='DOCUMENT_VERIFY' then jsonb_build_object(
          'final_candidate_key',v.payload_json->>'candidate_r2_key',
          'final_candidate_sha256',v.payload_json->>'candidate_sha256',
          'final_candidate_size_bytes',v.payload_json->'candidate_size_bytes',
          'expected_manifest_hash',v.manifest_hash,
          'expected_coverage_hash',v.payload_json->>'expected_coverage_hash',
          'expected_physical_input_count',
            v.payload_json->'expected_physical_input_count',
          'expected_physical_input_hash',
            v.payload_json->>'expected_physical_input_hash',
          'expected_page_count',v.expected_page_count,
          'expected_input_count',v.payload_json->'expected_input_count',
          'expected_logical_input_hash',
            v.payload_json->>'expected_coverage_hash',
          'resolved_logical_input_hash',
            v.payload_json->>'resolved_input_coverage_hash',
          'expected_physical_input_count',
            v.payload_json->'expected_physical_input_count',
          'expected_physical_input_hash',
            v.payload_json->>'expected_physical_input_hash',
          'expected_logical_source_count',
            v.payload_json->'expected_logical_source_count',
          'expected_logical_root_receipt',
            v.payload_json->>'expected_logical_root_receipt',
          'expected_physical_root_receipt',
            v.payload_json->>'expected_physical_root_receipt',
          'expected_ordered_input_root',
            v.payload_json->>'expected_ordered_input_root',
          'root_merge_receipt_identity',
            v.payload_json->>'root_merge_receipt_identity',
          'receipt_contract',v.payload_json->>'receipt_contract',
          'receipt_evidence_requirements',jsonb_build_object(
            'reopen_final_candidate',true,
            'verify_actual_final_hash',true,
            'verify_actual_final_page_count',true,
            'verify_root_receipt_chain',true),
          'plan_generation',v.payload_json->'plan_generation',
          'final_merge_receipt',v.payload_json->'final_merge_receipt',
          'final_merge_receipt_hash',
            v.payload_json->>'final_merge_receipt_hash',
          'verification_policy',v.processor_limits->'verify',
          'document_version_id',v.document_version_id,
          'immutable_destination_prefix','invoice-documents/'||v.document_version_id||
            '/verify/'||v.chunk_id||'/'||v.fence_token||'/')
        else '{}'::jsonb
      end context
    from valid v left join source_models sm on sm.chunk_id=v.chunk_id
  ),
  sized as materialized (
    select p.*,
      octet_length(p.context::text) context_size_bytes,
      case when coalesce(p.context_error,'')='' and octet_length(p.context::text)>
          case when coalesce(v.processor_limits->'context'->>v.chunk_type,'')~
              '^[0-9]{1,9}$'
            then(v.processor_limits->'context'->>v.chunk_type)::integer
            else 0 end
        then 'CONTEXT_TOO_LARGE'
        else p.context_error end sized_context_error
    from projected p join valid v on v.chunk_id=p.chunk_id
  ),
  all_results as (
    select i.request_no,jsonb_build_object(
      'chunk_id',i.chunk_id,'status','REJECTED','accepted',false,
      'code',i.ownership_error) result
    from inspected i where i.ownership_error is not null
    union all
    select p.request_no,
      case when p.sized_context_error is null then jsonb_build_object(
        'chunk_id',p.chunk_id,'operation_id',p.operation_id,
        'chunk_type',p.chunk_type,'phase',p.phase,
        'entity_type',p.entity_type,'entity_id',p.entity_id,
        'document_version_id',p.document_version_id,
        'document_asset_id',p.document_asset_id,
        'status','OK','accepted',true,
        'expected_result_identity',jsonb_build_object(
          'chunk_id',p.chunk_id,'fence_token',v.fence_token,
          'plan_generation',v.plan_generation,
          'action',p.chunk_type,'document_version_id',p.document_version_id,
          'document_asset_id',p.document_asset_id,
          'source_revision',p.context->>'source_revision',
          'template_version',p.context->>'template_version',
          'processor_policy_version',
            p.context->>'processor_policy_version',
          'immutable_destination_prefix',
            p.context->>'immutable_destination_prefix',
          'render_kind',p.context->>'render_kind',
          'ordered_input_hash',p.context->>'ordered_input_hash'),
        'context',p.context)
      else jsonb_build_object(
        'chunk_id',p.chunk_id,'operation_id',p.operation_id,
        'chunk_type',p.chunk_type,'phase',p.phase,
        'status','CONTEXT_ERROR','accepted',true,
        'permanent',p.sized_context_error<>'CONTEXT_TOO_LARGE',
        'retryable',p.sized_context_error='CONTEXT_TOO_LARGE',
        'code',p.sized_context_error,
        'context_size_bytes',p.context_size_bytes)
      end result
    from sized p join valid v on v.chunk_id=p.chunk_id
  )
  select coalesce(jsonb_agg(result order by request_no),'[]'::jsonb)
  into v_result from all_results;

  return v_result;
end;
$function$;

revoke all on function public.invoice_work_context_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function public.invoice_work_context_batch(jsonb,timestamptz)
  to service_role;
