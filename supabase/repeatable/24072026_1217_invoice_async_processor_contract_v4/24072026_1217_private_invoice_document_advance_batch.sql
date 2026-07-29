create or replace function private._invoice_document_advance_batch(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_manifest_results jsonb := '[]'::jsonb;
  v_passthrough_results jsonb := '[]'::jsonb;
begin
  if p_claims is null or jsonb_typeof(p_claims) is distinct from 'array' then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;

  if jsonb_array_length(p_claims) < 1
     or jsonb_array_length(p_claims) > 100 then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;

  -- Non-BUILD_MANIFEST phases are unchanged downstream graph machinery.  The
  -- migration preserves the current implementation as
  -- private._invoice_document_advance_batch_v6_downstream.  BUILD_MANIFEST is
  -- implemented here without the obsolete V4 presentation snapshot materialisation.
  if exists (
    select 1 from jsonb_array_elements(p_claims) c
    where coalesce(c->>'phase','') <> 'BUILD_MANIFEST'
  ) then
    select private._invoice_document_advance_batch_v6_downstream(
      (select coalesce(jsonb_agg(c),'[]'::jsonb)
       from jsonb_array_elements(p_claims) c
       where coalesce(c->>'phase','') <> 'BUILD_MANIFEST'),
      v_now)
    into v_passthrough_results;
  end if;

  if not exists (
    select 1 from jsonb_array_elements(p_claims) c
    where coalesce(c->>'phase','') = 'BUILD_MANIFEST'
  ) then
    return coalesce(v_passthrough_results,'[]'::jsonb);
  end if;

  with claim_ids as materialized (
    select (c->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) c
    where coalesce(c->>'phase','')='BUILD_MANIFEST'
      and coalesce(c->>'chunk_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  base as materialized (
    select c.id,c.operation_id,c.entity_type,c.entity_id,c.document_version_id,c.priority,
           c.operation_control_version,c.payload_json,c.status,c.phase,c.plan_generation,
           o.source_revision,o.template_version,o.control_version,
           coalesce(c.payload_json->>'purpose',case when c.entity_type='TIMESHEET' then 'TIMESHEET' else 'DRAFT_PREVIEW' end) purpose
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
    where c.phase='BUILD_MANIFEST' and c.chunk_type='DOCUMENT_PLAN'
  ),
  version_base as materialized (
    select b.*,v.id version_id,v.snapshot_json existing_snapshot_json,v.snapshot_hash existing_snapshot_hash,
           v.template_version version_template_version
    from base b
    join public.invoice_document_versions v
      on v.id=b.document_version_id and v.operation_id=b.operation_id
     and v.entity_type=b.entity_type and v.entity_id=b.entity_id and v.purpose=b.purpose
  ),
  presentation_requests as materialized (
    select jsonb_agg(jsonb_build_object(
        'request_key',b.id::text,
        'entity_type',b.entity_type,
        'entity_id',b.entity_id,
        'purpose',b.purpose,
        'template_version',coalesce(b.version_template_version,b.template_version),
        'issue_at_utc',b.payload_json->>'frozen_issue_at_utc',
        'tax_point_utc',coalesce(b.payload_json->>'frozen_tax_point_utc',b.payload_json->>'frozen_issue_at_utc'),
        'due_at_utc',b.payload_json->>'frozen_due_at_utc') order by b.id) request_json
    from version_base b where b.purpose<>'FINAL_ISSUE'
    having count(*) > 0
  ),
  presentation_batch as materialized (
    select p.*
    from presentation_requests pr
    cross join lateral private._invoice_presentation_snapshot_batch(pr.request_json, v_now) p
  ),
  version_seed as materialized (
    select b.*,
      case when b.purpose='FINAL_ISSUE' then b.existing_snapshot_json else p.snapshot_json end snapshot_json_v5,
      case when b.purpose='FINAL_ISSUE' then b.existing_snapshot_hash else p.snapshot_hash end snapshot_hash_v5,
      case when b.purpose='FINAL_ISSUE' then true else coalesce(p.valid,false) end presentation_valid,
      p.error_code presentation_error_code,p.error_detail presentation_error_detail,
      coalesce(case when b.purpose='FINAL_ISSUE' then b.existing_snapshot_json#>>'{presentation_model,schema_version}' else p.presentation_model->>'schema_version' end,'') presentation_schema,
      coalesce(case when b.purpose='FINAL_ISSUE' then b.existing_snapshot_json->>'presentation_model_hash' else p.snapshot_json->>'presentation_model_hash' end,'') presentation_hash
    from version_base b
    left join presentation_batch p on p.request_key=b.id::text
  ),
  blocked as materialized (
    update public.invoice_operation_chunks c
       set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
           lease_owner=null,lease_token=null,lease_expires_at_utc=null,
           error_json=jsonb_build_object('code',coalesce(s.presentation_error_code,'DOCUMENT_PRESENTATION_INVALID'),
             'detail',coalesce(s.presentation_error_detail,'{}'::jsonb),
             'document_version_id',s.document_version_id),
           updated_at_utc=v_now
    from version_seed s
    where c.id=s.id and not s.presentation_valid
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  ),
  linked as materialized (
    select s.*
    from version_seed s
    where s.presentation_valid
  ),
  direct_timesheet as materialized (
    select l.id chunk_id,l.operation_id,l.document_version_id,
      upper(coalesce(t.submission_mode::text,'')) submission_mode,
      t.manual_document_asset_id,
      a.id asset_id,a.source_kind,a.source_id,a.source_revision,
      a.original_filename,a.normalised_page_count,a.status asset_status
    from linked l
    join public.timesheets t
      on l.entity_type='TIMESHEET'
      and t.timesheet_id=l.entity_id
      and t.is_current
    left join public.invoice_document_assets a
      on a.id=t.manual_document_asset_id
  ),
  blocked_direct_timesheet_source as materialized (
    update public.invoice_operation_chunks c
       set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
           lease_owner=null,lease_token=null,lease_expires_at_utc=null,
           error_json=jsonb_build_object(
             'code','MANUAL_TIMESHEET_ASSET_REQUIRED',
             'timesheet_id',c.entity_id,
             'submission_mode',dt.submission_mode),
           updated_at_utc=v_now
    from direct_timesheet dt
    where c.id=dt.chunk_id
      and dt.submission_mode in('MANUAL','QR')
      and dt.asset_id is null
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  ),
  invoice_ts as materialized (
    select l.id chunk_id,l.operation_id,l.document_version_id,
      x.value timesheet_source,
      case when coalesce(x.value->>'timesheet_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'timesheet_id')::uuid end timesheet_id,
      upper(coalesce(x.value->>'submission_mode','')) submission_mode,
      case when lower(coalesce(x.value->>'attach_timesheet','')) in('true','t','1','yes') then true when lower(coalesce(x.value->>'attach_timesheet','')) in('false','f','0','no') then false else true end attach_timesheet,
      case when lower(coalesce(x.value->>'client_is_nhsp','')) in('true','t','1','yes') then true else false end client_is_nhsp,
      case when lower(coalesce(x.value->>'no_timesheet_required','')) in('true','t','1','yes') then true else false end no_timesheet_required,
      x.value#>>'{render_model,document_revision}' document_revision,
      case when coalesce(x.value->>'manual_document_asset_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x.value->>'manual_document_asset_id')::uuid end manual_document_asset_id
    from linked l
    cross join lateral jsonb_array_elements(case when jsonb_typeof(l.snapshot_json_v5->'timesheet_sources')='array' then l.snapshot_json_v5->'timesheet_sources' else '[]'::jsonb end) x(value)
    where l.entity_type='INVOICE'
  ),
  support_sources as materialized (
    select l.id chunk_id,l.operation_id,l.document_version_id,x.ordinality::integer source_no,x.value support_source,
      upper(coalesce(x.value->>'source_system',x.value#>>'{render_model,source_identity,source_system}','HEALTHROSTER')) source_system,
      case when coalesce(x.value->>'import_id',x.value#>>'{render_model,source_identity,import_id}','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (coalesce(x.value->>'import_id',x.value#>>'{render_model,source_identity,import_id}'))::uuid end import_id
    from linked l
    cross join lateral jsonb_array_elements(case when jsonb_typeof(l.snapshot_json_v5->'supporting_sources')='array' then l.snapshot_json_v5->'supporting_sources' else '[]'::jsonb end) with ordinality x(value,ordinality)
    where l.entity_type='INVOICE'
  ),
  evidence_assets as materialized (
    select l.id chunk_id,l.operation_id,l.document_version_id,x.ordinality::integer evidence_no,x.value evidence,
      case when coalesce(x.value->>'asset_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x.value->>'asset_id')::uuid end asset_id,
      coalesce(x.value->>'source_revision',encode(digest(x.value::text,'sha256'),'hex')) source_revision,
      upper(coalesce(x.value->>'kind','EVIDENCE')) kind,
      coalesce(x.value->>'display_name',x.value->>'kind','Evidence') display_label,
      case when coalesce(x.value->>'evidence_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x.value->>'evidence_id')::uuid end evidence_id
    from linked l
    cross join lateral jsonb_array_elements(case when jsonb_typeof(l.snapshot_json_v5->'supporting_manifest')='array' then l.snapshot_json_v5->'supporting_manifest' else '[]'::jsonb end) with ordinality x(value,ordinality)
    where l.entity_type='INVOICE'
  ),
  manifest_items as materialized (
    select l.id chunk_id,l.document_version_id,0::integer ordinal,
      case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end input_type,
      l.entity_type source_entity_type,l.entity_id source_entity_id,
      l.source_revision source_revision,
      null::uuid document_asset_id,null::uuid input_document_version_id,
      case when l.entity_type='INVOICE' then 'Invoice core document' else 'Electronic timesheet' end display_label,
      0::integer expected_page_count,'CORE_RENDER'::text inclusion_reason,
      l.presentation_schema,l.presentation_hash
    from linked l
    left join direct_timesheet dt on dt.chunk_id=l.id
    where l.entity_type='INVOICE' or dt.submission_mode='ELECTRONIC'
    union all
    select dt.chunk_id,dt.document_version_id,0::integer,
      'ASSET',dt.source_kind,dt.source_id,dt.source_revision,
      dt.asset_id,null::uuid,
      coalesce(nullif(dt.original_filename,''),'Manual/evidence timesheet asset'),
      dt.normalised_page_count,'TIMESHEET_ASSET_POLICY',
      'ASSET_SOURCE_V1',
      encode(digest(concat_ws('|',dt.asset_id::text,dt.source_revision,
        dt.source_kind,dt.source_id::text),'sha256'),'hex')
    from direct_timesheet dt
    where dt.submission_mode in('MANUAL','QR') and dt.asset_id is not null
    union all
    select it.chunk_id,it.document_version_id,
      1000+row_number() over(partition by it.chunk_id order by it.timesheet_id)::integer,
      'ELECTRONIC_TIMESHEET','TIMESHEET',it.timesheet_id,
      coalesce(it.document_revision,encode(digest(it.timesheet_source::text,'sha256'),'hex')),
      null::uuid,null::uuid,'Electronic timesheet',null::integer,'TIMESHEET_POLICY',
      'TIMESHEET_RENDER_MODEL_V1',encode(digest((it.timesheet_source->'render_model')::text,'sha256'),'hex')
    from invoice_ts it
    where it.submission_mode='ELECTRONIC' and it.attach_timesheet and not it.client_is_nhsp and not it.no_timesheet_required and it.timesheet_id is not null
    union all
    select it.chunk_id,it.document_version_id,
      2000+row_number() over(partition by it.chunk_id order by it.timesheet_id)::integer,
      'ASSET','TIMESHEET',it.timesheet_id,
      coalesce(it.document_revision,encode(digest(it.timesheet_source::text,'sha256'),'hex')),
      it.manual_document_asset_id,null::uuid,'Manual/evidence timesheet asset',null::integer,'TIMESHEET_ASSET_POLICY',
      'ASSET_SOURCE_V1',encode(digest(coalesce(it.timesheet_source,'{}'::jsonb)::text,'sha256'),'hex')
    from invoice_ts it
    where it.attach_timesheet and it.manual_document_asset_id is not null
      and (it.submission_mode <> 'ELECTRONIC' or it.client_is_nhsp or it.no_timesheet_required)
    union all
    select ea.chunk_id,ea.document_version_id,2500+ea.evidence_no,
      'ASSET','TIMESHEET_EVIDENCE',coalesce(ea.evidence_id,ea.asset_id),
      ea.source_revision,ea.asset_id,null::uuid,ea.display_label,null::integer,'EVIDENCE_ASSET_POLICY',
      'ASSET_SOURCE_V1',encode(digest(coalesce(ea.evidence,'{}'::jsonb)::text,'sha256'),'hex')
    from evidence_assets ea where ea.asset_id is not null
    union all
    select ss.chunk_id,ss.document_version_id,3000+ss.source_no,
      case when ss.source_system='NHSP' then 'NHSP_SUPPORT' else 'HEALTHROSTER_SUPPORT' end,
      ss.source_system,ss.import_id,
      encode(digest(ss.support_source::text,'sha256'),'hex'),
      null::uuid,null::uuid,initcap(lower(ss.source_system))||' supporting report',null::integer,'FROZEN_SOURCE_SUPPORT',
      case when ss.source_system='NHSP' then 'NHSP_PRESENTATION_V1' else 'HEALTHROSTER_PRESENTATION_V1' end,
      encode(digest((ss.support_source->'render_model')::text,'sha256'),'hex')
    from support_sources ss where ss.import_id is not null
    union all
    select l.id,l.document_version_id,4000,
      'HIGHER_RATE_SUPPORT','INVOICE',l.entity_id,
      encode(digest(coalesce(l.snapshot_json_v5->'higher_rate_support','{}'::jsonb)::text,'sha256'),'hex'),
      null::uuid,null::uuid,'Higher-rate support',null::integer,'HIGHER_RATE_SUPPORT',
      'HIGHER_RATE_PRESENTATION_V1',encode(digest(coalesce(l.snapshot_json_v5->'higher_rate_support','{}'::jsonb)::text,'sha256'),'hex')
    from linked l
    where jsonb_array_length(case when jsonb_typeof(l.snapshot_json_v5#>'{higher_rate_support,rows}')='array' then l.snapshot_json_v5#>'{higher_rate_support,rows}' else '[]'::jsonb end)>0
    union all
    select l.id,l.document_version_id,9000,
      'ATTACHMENT_INDEX','DOCUMENT',l.document_version_id,
      encode(digest(concat_ws('|',l.document_version_id::text,l.snapshot_hash_v5,'ATTACHMENT_INDEX'),'sha256'),'hex'),
      null::uuid,null::uuid,'Attachment index',null::integer,'ATTACHMENT_INDEX',
      'ATTACHMENT_INDEX_PRESENTATION_V1',encode(digest(concat_ws('|',l.document_version_id::text,l.snapshot_hash_v5,'ATTACHMENT_INDEX'),'sha256'),'hex')
    from linked l
    where exists(
      select 1 from invoice_ts it
      where it.chunk_id=l.id
        and (
          (
            it.submission_mode='ELECTRONIC'
            and it.attach_timesheet
            and not it.client_is_nhsp
            and not it.no_timesheet_required
            and it.timesheet_id is not null
          )
          or (
            it.attach_timesheet
            and it.manual_document_asset_id is not null
            and (
              it.submission_mode<>'ELECTRONIC'
              or it.client_is_nhsp
              or it.no_timesheet_required
            )
          )
        )
    )
       or exists(
         select 1 from support_sources ss
         where ss.chunk_id=l.id and ss.import_id is not null
       )
       or exists(
         select 1 from evidence_assets ea
         where ea.chunk_id=l.id and ea.asset_id is not null
       )
       or jsonb_array_length(
         case
           when jsonb_typeof(l.snapshot_json_v5#>'{higher_rate_support,rows}')
               ='array'
             then l.snapshot_json_v5#>'{higher_rate_support,rows}'
           else '[]'::jsonb
         end
       )>0
  ),
  manifest_build as materialized (
    select m.chunk_id,m.document_version_id,
      jsonb_agg(jsonb_build_object(
        'ordinal',m.ordinal,'input_type',m.input_type,'render_kind',m.input_type,
        'source_entity_type',m.source_entity_type,'source_entity_id',m.source_entity_id,
        'source_revision',m.source_revision,'document_asset_id',m.document_asset_id,
        'input_document_version_id',m.input_document_version_id,'display_label',m.display_label,
        'expected_page_count',m.expected_page_count,'inclusion_reason',m.inclusion_reason,
        'presentation_model_schema_version',m.presentation_schema,
        'presentation_model_hash',m.presentation_hash,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,m.source_revision,m.presentation_hash),'sha256'),'hex'))
        order by m.ordinal,m.source_entity_id) manifest_json,
      encode(digest(jsonb_agg(jsonb_build_object('ordinal',m.ordinal,'input_type',m.input_type,'source_entity_type',m.source_entity_type,'source_entity_id',m.source_entity_id,'source_revision',m.source_revision,'presentation_model_hash',m.presentation_hash) order by m.ordinal,m.source_entity_id)::text,'sha256'),'hex') manifest_hash,
      sum(coalesce(m.expected_page_count,0))::integer expected_page_count
    from manifest_items m group by m.chunk_id,m.document_version_id
  ),
  update_manifests as materialized (
    update public.invoice_document_versions v
       set snapshot_json=l.snapshot_json_v5,
           snapshot_hash=l.snapshot_hash_v5,
           manifest_json=mb.manifest_json,
           manifest_hash=mb.manifest_hash,
           expected_page_count=mb.expected_page_count,
           status='WAITING_FOR_INPUTS',
           error_json=null
    from manifest_build mb
    join linked l
      on l.id=mb.chunk_id
     and l.version_id=mb.document_version_id
    where v.id=mb.document_version_id
    returning v.id
  ),
  core_chunks as materialized (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,
      case when l.entity_type='INVOICE' then 'INVOICE_CORE_RENDER' else 'SOURCE_RENDER' end,
      'RENDER',
      encode(digest(concat_ws('|',case when l.entity_type='INVOICE' then 'INVOICE_CORE_RENDER' else 'SOURCE_RENDER' end,l.document_version_id::text,'0',l.snapshot_hash_v5,l.presentation_hash,coalesce(l.template_version,''),'2'),'sha256'),'hex'),
      0,l.entity_type,l.entity_id,l.document_version_id,'QUEUED',l.priority,v_now,
      jsonb_build_object('render_kind',case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end,
        'template_version',coalesce(l.version_template_version,l.template_version),
        'source_revision',l.source_revision,
        'source_chunk_key',encode(digest(concat_ws('|',l.document_version_id::text,'0',case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end,l.entity_type,l.entity_id::text,l.source_revision,l.presentation_hash),'sha256'),'hex'),
        'presentation_model_schema_version',l.presentation_schema,
        'presentation_model_hash',l.presentation_hash,
        'snapshot_hash',l.snapshot_hash_v5),
      l.control_version,v_now,v_now
    from linked l
    left join direct_timesheet dt on dt.chunk_id=l.id
    where l.entity_type='INVOICE' or dt.submission_mode='ELECTRONIC'
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do update set
      priority=greatest(public.invoice_operation_chunks.priority,excluded.priority),
      payload_json=excluded.payload_json,
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id
  ),
  source_chunks as materialized (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,'SOURCE_RENDER','RENDER',
      encode(digest(concat_ws('|','SOURCE_RENDER',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_revision,m.presentation_hash,coalesce(l.template_version,''),'2'),'sha256'),'hex'),
      m.ordinal,m.source_entity_type,m.source_entity_id,l.document_version_id,'QUEUED',l.priority,v_now,
      jsonb_build_object('render_kind',m.input_type,'source_revision',m.source_revision,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,m.source_revision,m.presentation_hash),'sha256'),'hex'),
        'template_version',coalesce(l.version_template_version,l.template_version),
        'presentation_model_schema_version',m.presentation_schema,
        'presentation_model_hash',m.presentation_hash,
        'snapshot_hash',l.snapshot_hash_v5),
      l.control_version,v_now,v_now
    from linked l join manifest_items m on m.chunk_id=l.id
    where m.ordinal>0 and m.input_type not in('ASSET','ATTACHMENT_INDEX')
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do update set
      priority=greatest(public.invoice_operation_chunks.priority,excluded.priority),
      payload_json=excluded.payload_json,
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id
  ),
  input_chunks as materialized (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,document_asset_id,status,priority,run_after_utc,payload_json,
      result_json,error_json,expected_page_count,actual_page_count,expected_byte_count,actual_byte_count,
      operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,'DOCUMENT_INPUT','DEPENDENCY',
      encode(digest(concat_ws('|','DOCUMENT_INPUT',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_revision,coalesce(m.document_asset_id::text,m.input_document_version_id::text,m.source_entity_id::text),m.presentation_hash,'2'),'sha256'),'hex'),
      m.ordinal,m.source_entity_type,m.source_entity_id,l.document_version_id,m.document_asset_id,
      case
        when m.input_type='ASSET' and a.status='READY' then 'COMPLETE'
        when m.input_type='ASSET' and m.document_asset_id is null then 'BLOCKED'
        when m.input_type='ASSET' and a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED') then 'BLOCKED'
        when m.input_type='ASSET' then 'WAITING'
        else 'WAITING' end,
      l.priority,v_now,
      jsonb_build_object('ordinal',m.ordinal,'input_type',m.input_type,'display_label',m.display_label,
        'source_revision',m.source_revision,'source_entity_type',m.source_entity_type,'source_entity_id',m.source_entity_id,
        'presentation_model_schema_version',m.presentation_schema,'presentation_model_hash',m.presentation_hash,'snapshot_hash',l.snapshot_hash_v5,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,m.source_revision,m.presentation_hash),'sha256'),'hex')),
      case when m.input_type='ASSET' and a.status='READY' then jsonb_build_object(
        'r2_key',a.normalised_r2_key,'parts',a.normalised_manifest_json,
        'sha256',a.normalised_sha256,'normalised_manifest_hash',a.normalised_manifest_hash,
        'size_bytes',a.normalised_size_bytes,'page_count',a.normalised_page_count,
        'source_revision',m.source_revision,'document_asset_id',a.id) end,
      case
        when m.input_type='ASSET' and m.document_asset_id is null then jsonb_build_object('code','ASSET_NOT_REGISTERED','source_entity_id',m.source_entity_id,'source_revision',m.source_revision)
        when m.input_type='ASSET' and a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED') then jsonb_build_object('code','ASSET_DEPENDENCY_PERMANENT_FAILURE','asset_status',a.status,'document_asset_id',a.id,'source_entity_id',m.source_entity_id)
      end,
      m.expected_page_count,
      case when m.input_type='ASSET' and a.status='READY' then a.normalised_page_count end,
      case when m.input_type='ASSET' and a.status='READY' then a.normalised_size_bytes end,
      case when m.input_type='ASSET' and a.status='READY' then a.normalised_size_bytes end,
      l.control_version,v_now,v_now
    from linked l
    join manifest_items m on m.chunk_id=l.id
    left join public.invoice_document_assets a on a.id=m.document_asset_id
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do update set
      status=case when excluded.status in('COMPLETE','BLOCKED') then excluded.status else public.invoice_operation_chunks.status end,
      payload_json=excluded.payload_json,
      result_json=coalesce(excluded.result_json,public.invoice_operation_chunks.result_json),
      error_json=coalesce(excluded.error_json,public.invoice_operation_chunks.error_json),
      actual_page_count=coalesce(excluded.actual_page_count,public.invoice_operation_chunks.actual_page_count),
      actual_byte_count=coalesce(excluded.actual_byte_count,public.invoice_operation_chunks.actual_byte_count),
      updated_at_utc=excluded.updated_at_utc
      where public.invoice_operation_chunks.status not in('SUPERSEDED','CANCELLED')
    returning id,operation_id,status
  ),
  advanced as materialized (
    update public.invoice_operation_chunks c
       set document_version_id=l.document_version_id,
           phase='WAIT_FOR_INPUTS',
           status=case when exists(
             select 1 from direct_timesheet dt
             where dt.chunk_id=l.id
               and dt.submission_mode in('MANUAL','QR')
               and dt.asset_status='READY')
             then 'QUEUED' else 'WAITING' end,
           progress_json=jsonb_build_object(
             'status_message',case when exists(
               select 1 from direct_timesheet dt
               where dt.chunk_id=l.id
                 and dt.submission_mode in('MANUAL','QR')
                 and dt.asset_status='READY')
               then 'Document inputs ready'
               else 'Waiting for document inputs' end,
             'manifest_items',(select jsonb_array_length(v.manifest_json)
               from public.invoice_document_versions v
               where v.id=l.document_version_id)),
           lease_owner=null,lease_token=null,lease_expires_at_utc=null,
           updated_at_utc=v_now
    from linked l
    where c.id=l.id
      and not exists(
        select 1 from blocked_direct_timesheet_source blocked_source
        where blocked_source.id=l.id)
    returning c.id,c.operation_id,c.document_version_id,c.status,c.phase
  ),
  rollup_ops as materialized (
    select private._invoice_operation_rollup_batch(array_agg(distinct operation_id),v_now) ignored
    from (
      select operation_id from advanced
      union select operation_id from blocked
      union select operation_id from blocked_direct_timesheet_source
      union select operation_id from core_chunks
      union select operation_id from source_chunks
      union select operation_id from input_chunks
    ) u
  ),
  all_results as materialized (
    select jsonb_build_object('chunk_id',b.id,'operation_id',b.operation_id,'status',b.status,'phase',b.phase,'error',b.error_json) result from blocked b
    union all
    select jsonb_build_object('chunk_id',b.id,'operation_id',b.operation_id,'status',b.status,'phase',b.phase,'error',b.error_json) result from blocked_direct_timesheet_source b
    union all
    select jsonb_build_object('chunk_id',p.id,'operation_id',p.operation_id,'status',p.status,'phase',p.phase,'document_version_id',p.document_version_id) result from advanced p
  )
  select coalesce(jsonb_agg(result),'[]'::jsonb) into v_manifest_results from all_results;

  return coalesce(v_manifest_results,'[]'::jsonb) || coalesce(v_passthrough_results,'[]'::jsonb);
end;
$function$;

revoke all on function private._invoice_document_advance_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function private._invoice_document_advance_batch(jsonb,timestamptz)
  to service_role;
