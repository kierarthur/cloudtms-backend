create or replace function private._invoice_reconcile_advance_batch(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_result jsonb;
begin
  /*
   * Every reconciliation command is bounded either by explicit operation IDs
   * or by its time fence and max_rows.  It performs only indexed database
   * repair; it never reads R2, regenerates financial data, or renders files.
   */
  with reconcile_claims as materialized (
    select c.id reconcile_chunk_id,c.operation_id reconcile_operation_id,c.payload_json,
      case when jsonb_typeof(c.payload_json->'operation_ids')='array'
        then jsonb_array_length(c.payload_json->'operation_ids') else 0 end
        explicit_operation_count,
      least(500,greatest(1,case
        when coalesce(c.payload_json->>'max_rows','') ~ '^[0-9]{1,6}$'
          then (c.payload_json->>'max_rows')::integer else 100 end)) max_rows,
      v_now-make_interval(secs=>least(604800,greatest(30,case
        when coalesce(c.payload_json->>'older_than_seconds','') ~ '^[0-9]{1,6}$'
          then (c.payload_json->>'older_than_seconds')::integer
        else 300 end))) not_updated_after_utc
    from jsonb_array_elements(p_claims) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where c.chunk_type='RECONCILE' and c.status='RUNNING'
  ),
  explicit_scope as materialized (
    select rc.reconcile_chunk_id,(v.value)::uuid operation_id
    from reconcile_claims rc
    cross join lateral jsonb_array_elements_text(
      case when jsonb_typeof(rc.payload_json->'operation_ids')='array'
        then rc.payload_json->'operation_ids' else '[]'::jsonb end)
      with ordinality v(value,ordinality)
    where v.value ~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and v.ordinality<=500
  ),
  implicit_scope as materialized (
    select rc.reconcile_chunk_id,o.id operation_id
    from reconcile_claims rc
    cross join lateral (
      select candidate.id
      from public.invoice_operations candidate
      where not exists(
        select 1 from explicit_scope e where e.reconcile_chunk_id=rc.reconcile_chunk_id)
        and candidate.id<>rc.reconcile_operation_id
        and candidate.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        and candidate.updated_at_utc<=rc.not_updated_after_utc
      order by candidate.updated_at_utc,candidate.id
      limit rc.max_rows
    ) o
  ),
  scope as materialized (
    select distinct reconcile_chunk_id,operation_id from explicit_scope
    union
    select distinct reconcile_chunk_id,operation_id from implicit_scope
  ),
  scoped_operations as materialized (
    select distinct operation_id from scope
  ),
  scoped_operation_ids as materialized (
    select s.operation_id,
      row_number() over(order by s.operation_id) operation_no
    from scoped_operations s
  ),
  scoped_operation_pages as materialized (
    select ((s.operation_no-1)/500)::integer page_no,
      array_agg(s.operation_id order by s.operation_id) operation_ids
    from scoped_operation_ids s
    group by ((s.operation_no-1)/500)::integer
  ),
  initial_current_slots as materialized (
    select g.*
    from scoped_operation_pages p
    cross join lateral private._invoice_current_chunks_batch(
      p.operation_ids,null,null,10000) g
  ),
  initial_current_chunks as materialized (
    select g.*,c.payload_json,c.result_json,c.actual_page_count,
      c.actual_byte_count,c.updated_at_utc
    from initial_current_slots g
    left join public.invoice_operation_chunks c on c.id=g.current_chunk_id
  ),
  replacement_graph_validation as materialized (
    select g.operation_id,g.logical_slot_key,g.replacement_chain_error
    from initial_current_slots g
    where g.replacement_chain_status='INVALID'
  ),
  current_normalise_ranges as materialized (
    select c.operation_id,c.current_chunk_id,c.document_asset_id,
      c.current_status,c.sequence_no,c.payload_json,c.result_json,
      c.actual_page_count,c.actual_byte_count,
      case when coalesce(c.payload_json#>>'{page_range,start}','')
          ~'^[1-9][0-9]{0,8}$'
        then(c.payload_json#>>'{page_range,start}')::integer end range_start,
      case when coalesce(c.payload_json#>>'{page_range,end}','')
          ~'^[1-9][0-9]{0,8}$'
        then(c.payload_json#>>'{page_range,end}')::integer end range_end
    from initial_current_chunks c
    where c.replacement_chain_status='VALID'
      and c.chunk_type='ASSET_NORMALISE'
      and c.document_asset_id is not null
  ),
  ordered_normalise_ranges as materialized (
    select r.*,
      lag(r.range_end) over(partition by r.document_asset_id
        order by r.range_start,r.range_end,r.sequence_no,r.current_chunk_id)
        previous_end
    from current_normalise_ranges r
  ),
  asset_current_coverage as materialized (
    select a.id asset_id,a.status asset_status,a.source_page_count,
      count(r.current_chunk_id)::integer current_part_count,
      coalesce(bool_and(r.current_status='COMPLETE'),false) all_complete,
      coalesce(bool_and(
        r.range_start is not null and r.range_end is not null
        and r.range_start<=r.range_end
        and r.range_end<=a.source_page_count
        and r.range_start=coalesce(r.previous_end+1,1)
        and coalesce(r.actual_page_count,0)=r.range_end-r.range_start+1
        and coalesce(r.actual_byte_count,0)>0
        and coalesce(r.result_json->>'r2_key','')<>''
        and coalesce(r.result_json->>'sha256','')~'^[0-9a-f]{64}$'
      ),false)
      and min(r.range_start)=1
      and max(r.range_end)=a.source_page_count
      and sum(r.range_end-r.range_start+1)=a.source_page_count
        coverage_valid
    from public.invoice_document_assets a
    join ordered_normalise_ranges r on r.document_asset_id=a.id
    group by a.id,a.status,a.source_page_count
  ),
  invalid_current_assets as materialized (
    select c.*
    from asset_current_coverage c
    where c.current_part_count>0
      and(
        (c.asset_status='NORMALISING' and c.all_complete
          and not c.coverage_valid)
        or(c.asset_status='READY'
          and(not c.all_complete or not c.coverage_valid)))
  ),
  invalid_normalise_chunks as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',completed_at_utc=null,
        failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code',case when a.asset_status='READY'
            then 'HISTORICAL_NORMALISATION_CONTAMINATION'
            else 'ASSET_RANGE_COVERAGE_MISMATCH' end,
          'asset_id',a.asset_id,
          'source_page_count',a.source_page_count,
          'current_part_count',a.current_part_count,
          'reconciled_at_utc',v_now)
    from invalid_current_assets a
    where c.id in(
      select r.current_chunk_id from current_normalise_ranges r
      where r.document_asset_id=a.asset_id)
    returning c.id,c.operation_id,c.document_asset_id
  ),
  failed_current_range_assets as (
    update public.invoice_document_assets a
    set status='FAILED',updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','ASSET_RANGE_COVERAGE_MISMATCH',
          'class','PERMANENT','retryable',false,
          'source_page_count',a.source_page_count,
          'reconciled_at_utc',v_now)
    from invalid_current_assets x
    where a.id=x.asset_id and x.asset_status='NORMALISING'
    returning a.id,a.operation_id,a.source_kind,a.source_id,a.error_json
  ),
  invalid_asset_evidence as (
    update public.timesheet_evidence e
    set processing_state='FAILED',
        processing_error_json=jsonb_build_object(
          'code',case when x.asset_status='READY'
            then 'HISTORICAL_NORMALISATION_CONTAMINATION'
            else 'ASSET_RANGE_COVERAGE_MISMATCH' end,
          'asset_id',a.id,'reconciled_at_utc',v_now)
    from invalid_current_assets x
    join public.invoice_document_assets a on a.id=x.asset_id
    where a.source_kind='TIMESHEET_EVIDENCE' and e.id=a.source_id
    returning e.id,e.timesheet_id,e.kind,e.processing_error_json
  ),
  invalid_manual_sources as (
    update public.timesheets t
    set document_state='FAILED',
        last_document_error_json=e.processing_error_json,
        updated_at=v_now
    from invalid_asset_evidence e
    where upper(coalesce(e.kind,''))='TIMESHEET'
      and t.timesheet_id=e.timesheet_id and t.is_current
    returning t.timesheet_id
  ),
  invalid_running as (
    update public.invoice_operation_chunks c
    set status=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else 'RETRY_WAIT' end,
        phase=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else c.phase end,
        run_after_utc=case when c.attempt_count>=c.max_attempts then c.run_after_utc
          else v_now+interval '30 seconds' end,
        error_json=jsonb_build_object(
          'code',case
            when c.lease_token is null then 'RUNNING_WITHOUT_LEASE_TOKEN'
            when c.lease_owner is null then 'RUNNING_WITHOUT_LEASE_OWNER'
            when c.lease_expires_at_utc is null then 'RUNNING_WITHOUT_LEASE_EXPIRY'
            else 'LEASE_EXPIRED' end,
          'reconciled_at_utc',v_now,
          'history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                 with ordinality h(value,ordinality)
            where h.ordinality>greatest(
              jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now))),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        failed_at_utc=case when c.attempt_count>=c.max_attempts then v_now else null end,
        updated_at_utc=v_now
    where c.operation_id in(select operation_id from scoped_operations)
      and c.status='RUNNING'
      and(c.lease_token is null or c.lease_owner is null
        or c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now)
    returning c.id,c.operation_id
  ),
  cancelled_orphans as (
    update public.invoice_operation_chunks c
    set status=case when o.status='SUPERSEDED' then 'SUPERSEDED' else 'CANCELLED' end,
        phase=case when o.status='SUPERSEDED' then 'SUPERSEDED' else 'CANCELLED' end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        completed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object('code','PARENT_'||o.status,'reconciled_at_utc',v_now)
    from public.invoice_operations o
    where c.operation_id=o.id
      and o.id in(select operation_id from scoped_operations)
      and o.status in('CANCELLED','SUPERSEDED')
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.id,c.operation_id
  ),
  ready_asset_dependencies as (
    update public.invoice_operation_chunks d
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        result_json=jsonb_build_object(
          'asset_id',a.id,'source_revision',a.source_revision,
          'normalised_r2_key',a.normalised_r2_key,
          'normalised_manifest',a.normalised_manifest_json,
          'parts',a.normalised_manifest_json,
          'sha256',a.normalised_sha256,
          'normalised_manifest_hash',a.normalised_manifest_hash,
          'size_bytes',a.normalised_size_bytes,
          'page_count',a.normalised_page_count),
        expected_page_count=coalesce(a.normalised_page_count,d.expected_page_count),
        actual_page_count=a.normalised_page_count,
        expected_byte_count=coalesce(a.normalised_size_bytes,d.expected_byte_count),
        actual_byte_count=a.normalised_size_bytes,
        error_json=null
    from public.invoice_document_assets a
    where d.operation_id in(select operation_id from scoped_operations)
      and d.chunk_type='DOCUMENT_INPUT'
      and(
        d.status='WAITING'
        or(d.status='BLOCKED'
          and d.error_json->>'code'='DOCUMENT_DEPENDENCY_PERMANENT_FAILURE'))
      and d.document_asset_id=a.id and a.status='READY'
      and not exists(select 1 from invalid_current_assets x
        where x.asset_id=a.id)
      and a.normalised_size_bytes>0
      and a.normalised_page_count>0
      and((a.normalised_r2_key is not null
            and a.normalised_sha256 is not null
            and jsonb_array_length(a.normalised_manifest_json)=0
            and a.normalised_manifest_hash is null)
        or(a.normalised_r2_key is null
            and a.normalised_sha256 is null
            and jsonb_array_length(a.normalised_manifest_json)>0
            and a.normalised_manifest_hash is not null))
    returning d.id,d.operation_id,d.document_version_id
  ),
  ready_version_dependencies as (
    update public.invoice_operation_chunks d
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,
        updated_at_utc=v_now,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,
        result_json=jsonb_build_object(
          'document_version_id',v.id,'source_revision',v.source_revision,
          'r2_key',v.r2_key,'sha256',v.sha256,
          'size_bytes',v.size_bytes,'page_count',v.page_count),
        expected_page_count=coalesce(v.page_count,d.expected_page_count),
        actual_page_count=v.page_count,
        expected_byte_count=coalesce(v.size_bytes,d.expected_byte_count),
        actual_byte_count=v.size_bytes,error_json=null
    from public.invoice_document_versions v
    where d.operation_id in(select operation_id from scoped_operations)
      and d.chunk_type='DOCUMENT_INPUT'
      and(d.status='WAITING' or(d.status='BLOCKED'
        and d.error_json->>'code'='DOCUMENT_DEPENDENCY_PERMANENT_FAILURE'))
      and d.input_document_version_id=v.id and v.status='READY'
      and v.r2_key is not null and v.sha256 is not null
      and v.size_bytes>0 and v.page_count>0
    returning d.id,d.operation_id,d.document_version_id
  ),
  ready_dependencies as materialized (
    select * from ready_asset_dependencies
    union all
    select * from ready_version_dependencies
  ),
  reset_document_versions as (
    update public.invoice_document_versions v
    set status='WAITING_FOR_INPUTS',error_json=null
    where v.id in(select document_version_id from ready_dependencies)
      and v.status='FAILED'
      and v.error_json->>'code'='DOCUMENT_DEPENDENCY_PERMANENT_FAILURE'
    returning v.id
  ),
  failed_asset_dependencies as (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
          'asset_id',d.document_asset_id,
          'input_document_version_id',d.input_document_version_id,
          'source_entity_id',d.entity_id,'asset_error',
          coalesce(a.error_json,case when exists(
            select 1 from invalid_current_assets x where x.asset_id=a.id)
            then jsonb_build_object(
              'code','HISTORICAL_NORMALISATION_CONTAMINATION',
              'asset_id',a.id,'reconciled_at_utc',v_now) end))
    from public.invoice_document_assets a
    where d.operation_id in(select operation_id from scoped_operations)
      and d.chunk_type='DOCUMENT_INPUT'
      and d.status='WAITING'
      and d.document_asset_id=a.id
      and(
        a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        or exists(select 1 from invalid_current_assets x
          where x.asset_id=a.id))
    returning d.id,d.operation_id,d.document_version_id
  ),
  failed_version_dependencies as (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
          'input_document_version_id',d.input_document_version_id,
          'source_entity_id',d.entity_id,'document_error',v.error_json)
    from public.invoice_document_versions v
    where d.operation_id in(select operation_id from scoped_operations)
      and d.chunk_type='DOCUMENT_INPUT' and d.status='WAITING'
      and d.input_document_version_id=v.id
      and v.status in('FAILED','SUPERSEDED','CANCELLED')
    returning d.id,d.operation_id,d.document_version_id
  ),
  failed_dependencies as materialized (
    select * from failed_asset_dependencies
    union all
    select * from failed_version_dependencies
  ),
  affected_document_versions as materialized (
    select document_version_id from ready_dependencies
    union select document_version_id from failed_dependencies
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',
        phase=case when p.phase in('BUILD_MANIFEST','WAIT_FOR_INPUTS')
          then 'WAIT_FOR_INPUTS' else p.phase end,
        run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN'
      and p.document_version_id in(select document_version_id from affected_document_versions)
      and p.status in('WAITING','RETRY_WAIT','BLOCKED')
      and not exists(
        select 1 from public.invoice_operation_chunks d
        where d.document_version_id=p.document_version_id
          and d.chunk_type='DOCUMENT_INPUT' and d.status='BLOCKED')
    returning p.id,p.operation_id
  ),
  issue_wake as (
    update public.invoice_operation_chunks i
    set status=case when v.status='READY' then 'QUEUED' else 'BLOCKED' end,
        phase=case when v.status='READY' then 'FINALISE' else 'WAIT_DOCUMENT' end,
        run_after_utc=v_now,updated_at_utc=v_now,
        error_json=case when v.status='READY' then null else jsonb_build_object(
          'code','FINAL_DOCUMENT_PERMANENT_FAILURE',
          'document_version_id',v.id,'document_error',v.error_json) end,
        failed_at_utc=case when v.status='READY' then null else v_now end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    from public.invoice_document_versions v
    where i.operation_id in(select operation_id from scoped_operations)
      and i.chunk_type='ISSUE_INVOICE' and i.phase='WAIT_DOCUMENT'
      and i.status in('WAITING','RETRY_WAIT','BLOCKED')
      and coalesce(
        case when coalesce(i.payload_json->>'document_version_id','') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'document_version_id')::uuid end,
        case when coalesce(i.payload_json->>'final_document_version_id','') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'final_document_version_id')::uuid end)=v.id
      and v.status in('READY','FAILED','SUPERSEDED','CANCELLED')
    returning i.id,i.operation_id
  ),
  invoice_ready_pointers as (
    update public.invoices i
    set preview_document_version_id=case when v.purpose='DRAFT_PREVIEW' then v.id
          else i.preview_document_version_id end,
        document_state=case when v.purpose='DRAFT_PREVIEW' then 'READY' else i.document_state end,
        active_document_operation_id=case
          when i.active_document_operation_id=v.operation_id then null
          else i.active_document_operation_id end,
        invoice_pdf_r2_key=case when v.purpose='DRAFT_PREVIEW' then v.r2_key
          else i.invoice_pdf_r2_key end,
        invoice_pdf_generated_at_utc=case when v.purpose='DRAFT_PREVIEW' then v.verified_at_utc
          else i.invoice_pdf_generated_at_utc end,
        updated_at=v_now
    from public.invoice_document_versions v
    where v.entity_type='INVOICE' and v.purpose='DRAFT_PREVIEW' and v.status='READY'
      and v.operation_id in(select operation_id from scoped_operations)
      and i.id=v.entity_id and i.document_revision::text=v.source_revision
    returning i.id
  ),
  timesheet_ready_pointers as (
    update public.timesheets t
    set current_document_version_id=v.id,document_state='READY',
        active_document_operation_id=case when t.active_document_operation_id=v.operation_id then null
          else t.active_document_operation_id end,
        updated_at=v_now
    from public.invoice_document_versions v
    where v.entity_type='TIMESHEET' and v.purpose='TIMESHEET' and v.status='READY'
      and v.operation_id in(select operation_id from scoped_operations)
      and t.timesheet_id=v.entity_id and t.document_revision::text=v.source_revision
    returning t.timesheet_id
  ),
  stale_invoice_pointers as (
    update public.invoices i
    set active_document_operation_id=case when exists(
          select 1 from public.invoice_operations od
          where od.id=i.active_document_operation_id
            and od.id in(select operation_id from scoped_operations)
            and od.status in(
              'COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED'))
          then null else i.active_document_operation_id end,
        active_issue_operation_id=case when exists(
          select 1 from public.invoice_operations oi
          where oi.id=i.active_issue_operation_id
            and oi.id in(select operation_id from scoped_operations)
            and oi.status in(
              'COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED'))
          then null else i.active_issue_operation_id end,
        updated_at=v_now
    where exists(
      select 1 from public.invoice_operations o
      where o.id in(select operation_id from scoped_operations)
        and o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED')
        and o.id in(i.active_document_operation_id,i.active_issue_operation_id))
    returning i.id
  ),
  stale_timesheet_pointers as (
    update public.timesheets t
    set active_document_operation_id=null,updated_at=v_now
    from public.invoice_operations o
    where o.id=t.active_document_operation_id
      and o.id in(select operation_id from scoped_operations)
      and o.status in(
        'COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED')
    returning t.timesheet_id
  ),
  empty_operation_repair as (
    update public.invoice_operations o
    set status='FAILED',phase='FAILED',total_units=1,completed_units=0,
        failed_units=1,chunk_count=0,requires_user_action=true,
        failed_at_utc=coalesce(o.failed_at_utc,v_now),
        error_json=jsonb_build_object(
          'code','ACTIVE_OPERATION_WITHOUT_CHUNKS',
          'reconciled_at_utc',v_now),
        progress_json=coalesce(o.progress_json,'{}'::jsonb)||
          jsonb_build_object('total_units',1,'completed_units',0,
            'failed_units',1,'reconciled_at_utc',v_now),
        updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in(select operation_id from scoped_operations)
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      and not exists(select 1 from public.invoice_operation_chunks c
        where c.operation_id=o.id)
    returning o.id
  ),
  invalid_verify_completion as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        completed_at_utc=null,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','VERIFY_COMPLETED_WITHOUT_READY_DOCUMENT',
          'document_version_id',c.document_version_id,
          'reconciled_at_utc',v_now)
    where c.operation_id in(select operation_id from scoped_operations)
      and c.chunk_type='DOCUMENT_VERIFY' and c.status='COMPLETE'
      and not exists(
        select 1 from public.invoice_document_versions v
        where v.id=c.document_version_id and v.status='READY'
          and nullif(v.r2_key,'') is not null
          and nullif(v.sha256,'') is not null
          and coalesce(v.size_bytes,0)>0 and coalesce(v.page_count,0)>0)
    returning c.id,c.operation_id,c.document_version_id
  ),
  missing_manual_document_work as (
    update public.timesheets t
    set document_state='FAILED',
        last_document_error_json=jsonb_build_object(
          'code','READY_MANUAL_ASSET_WITHOUT_DOCUMENT_WORKFLOW',
          'reconciled_at_utc',v_now),
        updated_at=v_now
    where t.is_current and t.manual_document_asset_id is not null
      and exists(
        select 1
        from public.invoice_document_assets a
        where a.id=t.manual_document_asset_id and a.status='READY')
      and not exists(
        select 1
        from public.invoice_document_versions v
        where v.entity_type='TIMESHEET' and v.entity_id=t.timesheet_id
          and v.purpose='TIMESHEET'
          and v.source_revision=t.document_revision::text
          and v.status in(
            'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING',
            'VERIFYING','READY'))
      and (
        t.active_document_operation_id in(
          select operation_id from scoped_operations)
        or exists(
          select 1
          from public.timesheet_evidence e
          join public.invoice_document_assets a
            on a.id=e.document_asset_id and a.status='READY'
          where e.timesheet_id=t.timesheet_id
            and upper(coalesce(e.kind,''))='TIMESHEET'
            and a.operation_id in(
              select operation_id from scoped_operations)))
    returning t.timesheet_id
  ),
  invalid_policy_chunks as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',completed_at_utc=null,
        failed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','PROCESSOR_POLICY_INVALID',
          'policy_version',o.config_json#>>'{processor_policy,version}',
          'reconciled_at_utc',v_now)
    from public.invoice_operations o
    where c.operation_id=o.id
      and c.id in(select current_chunk_id from initial_current_chunks
        where current_chunk_id is not null
          and replacement_chain_status='VALID')
      and c.status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE')
      and(
        jsonb_typeof(o.config_json->'processor_policy')<>'object'
        or o.config_json#>>'{processor_policy,version}'
          is distinct from 'INVOICE_PROCESSOR_LIMITS_V4'
        or o.config_json#>>'{processor_policy,policy_version}'
          is distinct from 'INVOICE_PROCESSOR_LIMITS_V4'
        or jsonb_typeof(o.config_json#>'{processor_policy,context}')
          <>'object'
        or jsonb_typeof(o.config_json#>'{processor_policy,result}')
          <>'object'
        or jsonb_typeof(o.config_json#>
          '{processor_policy,delivery,allowed_policies}')<>'array'
        or o.config_json#>'{processor_policy,delivery,allowed_policies}'
          is distinct from '["ATTACH","SPLIT","SECURE_LINK"]'::jsonb
        or o.config_json#>'{processor_policy,asset,allowed_media_types}'
          is distinct from '["application/pdf","image/jpeg","image/png"]'::jsonb
        or o.config_json#>>'{processor_policy,verify,object_receipt_contract}'
          is distinct from 'ACTUAL_BYTES_OBJECT_RECEIPT_V3'
        or o.config_json#>>'{processor_policy,verify,logical_receipt_contract}'
          is distinct from 'LOGICAL_SOURCE_RECEIPT_V3'
        or o.config_json#>>'{processor_policy,verify,merge_receipt_contract}'
          is distinct from 'ACTUAL_BYTES_MERGE_RECEIPT_V3'
        or o.config_json#>>'{processor_policy,verify,document_root_receipt_contract}'
          is distinct from 'DOCUMENT_ROOT_RECEIPT_V3'
        or o.config_json#>>'{processor_policy,verify,ordered_input_hash_contract}'
          is distinct from 'ACTUAL_ORDERED_INPUT_V1'
        or(c.chunk_type='ASSET_NORMALISE' and exists(
          select 1 from public.invoice_document_assets a
          where a.id=c.document_asset_id
            and(a.original_sha256 is null or a.original_size_bytes is null)))
        or(c.chunk_type='DOCUMENT_VERIFY' and c.payload_json ? 'immutable_destination_prefix'))
    returning c.id,c.operation_id
  ),
  invalid_request_correlation as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',completed_at_utc=null,
        failed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','REQUEST_CORRELATION_INVALID',
          'chunk_id',c.id,
          'request_key',case
            when c.chunk_type='DELIVERY_PREPARE'
              then c.payload_json->>'request_key'
            else c.payload_json#>>'{frozen_delivery_route,request_key}' end,
          'reconciled_at_utc',v_now)
    where c.id in(select current_chunk_id from initial_current_chunks
        where current_chunk_id is not null
          and replacement_chain_status='VALID')
      and(
        (c.chunk_type='DELIVERY_PREPARE'
          and c.payload_json->>'request_key' is distinct from c.id::text)
        or(c.chunk_type='ISSUE_INVOICE'
          and jsonb_typeof(c.payload_json->'frozen_delivery_route')='object'
          and c.payload_json#>>'{frozen_delivery_route,request_key}'
            is distinct from c.id::text))
    returning c.id,c.operation_id
  ),
  delivery_route_request_batch as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key',c.id::text,
      'invoice_id',c.entity_id,
      'recipient_set',coalesce(
        c.payload_json#>'{routing_request,recipient_set}',
        '[]'::jsonb),
      'cc',coalesce(c.payload_json#>'{routing_request,cc}',
        '[]'::jsonb),
      'bcc',coalesce(c.payload_json#>'{routing_request,bcc}',
        '[]'::jsonb),
      'delivery_policy',coalesce(
        c.payload_json#>>'{routing_request,delivery_policy}',
        c.payload_json#>>'{frozen_delivery_route,delivery_policy}',
        'ATTACH'),
      'template_version',coalesce(
        c.payload_json#>>'{routing_request,template_version}',
        c.payload_json#>>'{frozen_delivery_route,template_version}',
        'invoice-delivery-v1'))
      order by c.id),'[]'::jsonb) requests
    from initial_current_chunks current
    join public.invoice_operation_chunks c
      on c.id=current.current_chunk_id
    where current.replacement_chain_status='VALID'
      and c.chunk_type='DELIVERY_PREPARE'
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      and c.entity_type='INVOICE' and c.entity_id is not null
  ),
  current_delivery_routes as materialized (
    select route.*
    from delivery_route_request_batch b
    cross join lateral private._invoice_delivery_routes_batch(
      b.requests,(v_now at time zone 'Europe/London')::date) route
  ),
  delivery_route_drift as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',completed_at_utc=null,
        failed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','DELIVERY_ROUTE_CHANGED',
          'frozen_route_policy_hash',
            c.payload_json#>>'{frozen_delivery_route,route_policy_hash}',
          'current_route_policy_hash',route.route_policy_hash,
          'frozen_route',jsonb_build_object(
            'to',c.payload_json#>'{frozen_delivery_route,to}',
            'cc',c.payload_json#>'{frozen_delivery_route,cc}',
            'bcc',c.payload_json#>'{frozen_delivery_route,bcc}',
            'delivery_suppressed',c.payload_json#>
              '{frozen_delivery_route,delivery_suppressed}'),
          'current_route',jsonb_build_object(
            'to',route.canonical_to,'cc',route.canonical_cc,
            'bcc',route.canonical_bcc,
            'delivery_suppressed',route.delivery_suppressed,
            'suppression_reason',route.suppression_reason),
          'reconciled_at_utc',v_now)
    from current_delivery_routes route
    where c.id=route.request_key::uuid
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      and c.payload_json#>>'{frozen_delivery_route,route_policy_hash}'
        is distinct from route.route_policy_hash
    returning c.id,c.operation_id
  ),
  invalid_pagination_contracts as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',completed_at_utc=null,
        failed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','ATTACHMENT_INDEX_PAGINATION_CONTRACT_INVALID',
          'reconciled_at_utc',v_now)
    where c.id in(select current_chunk_id from initial_current_chunks
        where current_chunk_id is not null
          and replacement_chain_status='VALID')
      and c.chunk_type='SOURCE_RENDER' and c.status='COMPLETE'
      and c.payload_json->>'render_kind'='ATTACHMENT_INDEX'
      and c.payload_json->>'layout_phase'='FINAL'
      and(
        jsonb_typeof(c.payload_json->'pagination_stream')<>'array'
        or coalesce(c.payload_json->>'pagination_stream_hash','')<>
          encode(digest(coalesce(c.payload_json->'pagination_stream',
            '[]'::jsonb)::text,'sha256'),'hex')
        or c.result_json->>'displayed_start_pages_hash'
          is distinct from c.payload_json->>'expected_start_pages_hash'
        or jsonb_typeof(c.result_json->'displayed_page_map')<>'array'
        or c.result_json->'displayed_page_map'
          is distinct from c.payload_json->'attachments'
        or encode(digest(coalesce(c.result_json->'displayed_page_map',
            '[]'::jsonb)::text,'sha256'),'hex')
          is distinct from c.payload_json->>'expected_start_pages_hash'
        or c.result_json->>'final_index_page_count'
          is distinct from c.result_json->>'page_count'
        or c.result_json->>'pagination_stream_hash'
          is distinct from c.payload_json->>'pagination_stream_hash'
        or c.result_json->>'layout_identity_hash'
          is distinct from encode(digest(coalesce(
            c.payload_json->'determinism','{}'::jsonb)::text,
            'sha256'),'hex'))
    returning c.id,c.operation_id
  ),
  invalid_root_receipts as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',completed_at_utc=null,
        failed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','ROOT_MERGE_RECEIPT_CONTRACT_INVALID',
          'reconciled_at_utc',v_now)
    where c.id in(select current_chunk_id from initial_current_chunks
        where current_chunk_id is not null
          and replacement_chain_status='VALID')
      and c.chunk_type='PDF_MERGE' and c.status='COMPLETE'
      and(
        c.result_json#>>'{merge_receipt,receipt_contract}'
          is distinct from 'ACTUAL_BYTES_MERGE_RECEIPT_V3'
        or c.result_json#>>
            '{merge_receipt,combined_logical_receipt_root}'
          is distinct from
            c.payload_json->>'expected_logical_receipt_root'
        or c.result_json#>>
            '{merge_receipt,combined_physical_receipt_root}'
          is distinct from
            c.payload_json->>'expected_physical_receipt_root'
        or c.result_json#>>'{merge_receipt,actual_child_receipt_hash}'
          is distinct from c.payload_json->>'expected_child_receipt_hash'
        or jsonb_typeof(c.result_json#>'{merge_receipt,physical_receipts}')
          <>'array'
        or c.result_json#>'{merge_receipt,physical_receipts}'
          is distinct from coalesce(c.payload_json->'expected_physical_receipts',
            '[]'::jsonb)
        or c.result_json#>>'{merge_receipt,combined_physical_receipt_root}'
          is distinct from coalesce((
            select encode(digest(string_agg(
              receipt.value->>'physical_receipt','||'
              order by receipt.ordinality),'sha256'),'hex')
            from jsonb_array_elements(coalesce(c.result_json#>
              '{merge_receipt,physical_receipts}','[]'::jsonb))
              with ordinality receipt(value,ordinality)),''))
    returning c.id,c.operation_id
  ),
  invalid_self_bill_suppression as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',completed_at_utc=null,
        failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','DELIVERY_SUPPRESSION_POLICY_INVALID',
          'detail','Raw self-bill state suppressed delivery without policy authority',
          'reconciled_at_utc',v_now)
    where c.id in(select current_chunk_id from initial_current_chunks
        where current_chunk_id is not null
          and replacement_chain_status='VALID')
      and c.chunk_type='DELIVERY_PREPARE' and c.status='COMPLETE'
      and lower(coalesce(c.result_json->>'delivery_skipped','false'))
        in('true','t','1','yes')
      and lower(coalesce(c.payload_json#>>
          '{frozen_delivery_route,self_bill}','false'))
        in('true','t','1','yes')
      and lower(coalesce(c.payload_json#>>
          '{frozen_delivery_route,delivery_suppressed}','false'))
        not in('true','t','1','yes')
    returning c.id,c.operation_id
  ),
  missing_delivery_children as (
    update public.invoice_operation_chunks c
    set status='QUEUED',phase='QUEUE_DELIVERY',completed_at_utc=null,
        failed_at_utc=null,run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','DELIVERY_DELEGATION_MISSING',
          'invoice_id',c.entity_id,'reconciled_at_utc',v_now)
    where c.id in(select current_chunk_id from initial_current_chunks
        where current_chunk_id is not null
          and replacement_chain_status='VALID'
          and chunk_type='ISSUE_INVOICE')
      and c.status='COMPLETE'
      and lower(coalesce(c.payload_json->>'deliver','false'))
        in('true','t','1','yes')
      and c.payload_json#>>'{frozen_delivery_route,request_key}'=c.id::text
      and not exists(
        select 1
        from public.invoice_operations d
        join public.invoice_operation_chunks dc on dc.operation_id=d.id
        where d.parent_operation_id=c.operation_id
          and d.operation_type='DELIVER_INVOICES'
          and dc.chunk_type='DELIVERY_PREPARE'
          and dc.entity_type='INVOICE' and dc.entity_id=c.entity_id
          and dc.document_version_id=c.document_version_id
          and dc.status not in('CANCELLED','SUPERSEDED'))
    returning c.id,c.operation_id
  ),
  scoped_mail_ids as materialized (
    select distinct x.value::uuid mail_id
    from initial_current_chunks c
    cross join lateral jsonb_array_elements_text(
      case when jsonb_typeof(c.result_json->'mail_outbox_ids')='array'
        then c.result_json->'mail_outbox_ids' else '[]'::jsonb end) x(value)
    where x.value~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    union
    select distinct(c.result_json->>'mail_outbox_id')::uuid
    from initial_current_chunks c
    where coalesce(c.result_json->>'mail_outbox_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  stuck_secure_mail as (
    update public.mail_outbox m
    set status='FAILED',failed_at=v_now,
        last_error='SECURE_LINK_DESCRIPTOR_MISMATCH',
        attempt_lease_token=null,attempt_leased_at_utc=null,
        attempt_lease_expires_at_utc=null
    where m.id in(select mail_id from scoped_mail_ids)
      and upper(coalesce(m.type,''))='INVOICE'
      and m.status='QUEUED'
      and upper(coalesce(m.attachment_delivery_policy,''))='SECURE_LINK'
      and(
        jsonb_typeof(m.attachments)<>'array'
        or jsonb_array_length(case when jsonb_typeof(m.attachments)='array'
          then m.attachments else '[]'::jsonb end)=0
        or exists(
          select 1
          from jsonb_array_elements(
            case when jsonb_typeof(m.attachments)='array'
              then m.attachments else '[]'::jsonb end) a
          where upper(coalesce(a->>'delivery_mode',''))<>'SECURE_LINK'
            or lower(coalesce(a->>'secure_link_required','false'))
              not in('true','t','1','yes')
            or nullif(a->>'r2_key','') is not null
            or nullif(a->>'sha256','') is null
            or coalesce(a->>'size_bytes','')!~'^[1-9][0-9]{0,17}$'
            or coalesce(a->>'page_count','')!~'^[1-9][0-9]{0,8}$'))
    returning m.id
  ),
  scoped_invoice_ids as materialized (
    select distinct c.entity_id invoice_id
    from initial_current_chunks c
    where c.entity_type='INVOICE' and c.entity_id is not null
    union
    select distinct(c.result_json->>'invoice_id')::uuid
    from initial_current_chunks c
    where coalesce(c.result_json->>'invoice_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  correction_reconcile_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','reconcile-correction:'||i.invoice_id::text,
      'scope_key',i.invoice_id::text,
      'invoice_id',i.invoice_id,
      'validation_purpose','RECONCILE') order by i.invoice_id),
      '[]'::jsonb) scopes
    from scoped_invoice_ids i
  ),
  correction_unit_anomalies as materialized (
    select validation.invoice_id,validation.timesheet_id,
      validation.blocker_code,validation.blocker_codes,
      validation.detail_json
    from correction_reconcile_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) validation
    where not validation.valid
  ),
  historical_summary_contamination as materialized (
    select distinct o.id operation_id
    from public.invoice_operations o
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(o.result_json->'issue_outcomes')='array'
        then o.result_json->'issue_outcomes'
        else '[]'::jsonb end) outcome(value)
    where o.id in(select operation_id from scoped_operations)
      and coalesce(outcome.value->>'chunk_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and not exists(
        select 1 from initial_current_chunks current
        where current.operation_id=o.id
          and current.current_chunk_id=
            (outcome.value->>'chunk_id')::uuid
          and current.replacement_chain_status='VALID')
  ),
  oversized_parent_progress as materialized (
    select distinct o.id operation_id
    from public.invoice_operations o
    where o.id in(select operation_id from scoped_operations)
      and(
        pg_column_size(coalesce(o.progress_json,'{}'::jsonb))>524288
        or exists(
          select 1
          from jsonb_array_elements(case when jsonb_typeof(
              o.progress_json#>'{child_operations,rows}')='array'
            then o.progress_json#>'{child_operations,rows}'
            else '[]'::jsonb end) child(value)
          where child.value ?| array['result','error','progress']))
  ),  current_chunks as materialized (
    select g.*
    from scoped_operation_pages p
    cross join lateral private._invoice_current_chunks_batch(
      p.operation_ids,null,null,10000) g
  ),
  aggregates as materialized (
    select c.operation_id,count(*)::integer total,
      count(*) filter(where c.current_status='COMPLETE')::integer completed,
      count(*) filter(where c.current_status in(
        'FAILED','DEAD_LETTER','BLOCKED')
        or c.replacement_chain_status='INVALID')::integer failed,
      bool_or(c.current_status in('QUEUED','RUNNING','WAITING','RETRY_WAIT'))
        filter(where c.replacement_chain_status='VALID') active,
      bool_or(c.current_status='DEAD_LETTER') dead,
      bool_or(c.current_status='BLOCKED'
        or c.replacement_chain_status='INVALID') blocked
    from current_chunks c
    group by c.operation_id
  ),
  parent_repair as (
    update public.invoice_operations o
    set total_units=a.total,chunk_count=a.total,completed_units=a.completed,failed_units=a.failed,
        status=case
          when a.dead and not a.active then 'DEAD_LETTER'
          when a.blocked and not a.active then 'BLOCKED'
          when not a.active and a.failed>0 then 'FAILED'
          when not a.active then 'COMPLETE'
          when a.active and not a.dead and not a.blocked
            and o.status in('FAILED','DEAD_LETTER','BLOCKED') then 'QUEUED'
          else o.status end,
        phase=case
          when not a.active and a.failed=0 then 'COMPLETE'
          when a.active and not a.dead and not a.blocked
            and o.status in('FAILED','DEAD_LETTER','BLOCKED') then 'WAIT_FOR_INPUTS'
          else o.phase end,
        requires_user_action=(a.dead or a.blocked),
        completed_at_utc=case when not a.active and a.failed=0
          then coalesce(o.completed_at_utc,v_now) else o.completed_at_utc end,
        failed_at_utc=case when not a.active and a.failed>0
          then coalesce(o.failed_at_utc,v_now)
          when a.active and not a.dead and not a.blocked then null
          else o.failed_at_utc end,
        progress_json=coalesce(o.progress_json,'{}'::jsonb)||jsonb_build_object(
          'total_units',a.total,'completed_units',a.completed,'failed_units',a.failed,
          'reconciled_at_utc',v_now),
        updated_at_utc=v_now,change_seq=nextval('public.invoice_operation_change_seq')
    from aggregates a
    where o.id=a.operation_id
      and o.status not in('CANCELLED','SUPERSEDED')
    returning o.id
  ),
  rollup_repair as materialized (
    select r.*
    from scoped_operation_pages p
    cross join lateral private._invoice_operation_rollup_batch(
      p.operation_ids,v_now,true) r
  ),
  repair_counts as materialized (
    select
      (select count(*) from invalid_normalise_chunks)
        invalid_normalise_chunks,
      (select count(*) from failed_current_range_assets)
        failed_current_range_assets,
      (select count(*) from invalid_asset_evidence)
        invalid_asset_evidence,
      (select count(*) from invalid_running) invalid_running,
      (select count(*) from cancelled_orphans) cancelled_orphans,
      (select count(*) from ready_dependencies) ready_dependencies,
      (select count(*) from failed_dependencies) failed_dependencies,
      (select count(*) from plan_wake) document_plans_woken,
      (select count(*) from issue_wake) issue_chunks_woken,
      (select count(*) from invoice_ready_pointers) invoice_pointers_repaired,
      (select count(*) from timesheet_ready_pointers) timesheet_pointers_repaired,
      (select count(*) from stale_invoice_pointers) stale_pointers_cleared,
      (select count(*) from stale_timesheet_pointers)
        stale_timesheet_pointers_cleared,
      (select count(*) from empty_operation_repair) empty_operations_failed,
      (select count(*) from replacement_graph_validation)
        invalid_replacement_chains,
      (select coalesce(jsonb_agg(jsonb_build_object(
          'operation_id',operation_id,
          'logical_slot_key',logical_slot_key,
          'error',replacement_chain_error)
          order by operation_id,logical_slot_key),'[]'::jsonb)
       from replacement_graph_validation)
        invalid_replacement_chain_details,
      (select count(*) from invalid_verify_completion)
        invalid_verify_completions,
      (select count(*) from missing_manual_document_work)
        missing_manual_document_workflows,
      (select count(*) from invalid_policy_chunks)
        invalid_processor_policy_chunks,
      (select count(*) from invalid_request_correlation)
        invalid_request_correlations,
      (select count(*) from delivery_route_drift)
        delivery_route_drift_blocked,
      (select count(*) from invalid_pagination_contracts)
        invalid_pagination_contracts_blocked,
      (select count(*) from invalid_root_receipts)
        invalid_root_receipts_blocked,
      (select count(*) from invalid_self_bill_suppression)
        invalid_self_bill_suppressions_blocked,
      (select count(*) from missing_delivery_children)
        missing_delivery_children_requeued,
      (select count(*) from stuck_secure_mail)
        secure_link_mail_failed,
      (select count(*) from correction_unit_anomalies)
        correction_unit_anomalies_reported,
      (select count(*) from historical_summary_contamination)
        historical_summary_contamination_repaired,
      (select count(*) from oversized_parent_progress)
        oversized_parent_progress_repaired,
      (select count(*) from parent_repair) parent_operations_repaired,
      (select count(*) from rollup_repair) operations_rolled_up
  ),
  completed_reconcile as (
    update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        result_json=jsonb_build_object(
          'reconciled_at_utc',v_now,
          'scope_operation_count',(select count(*) from scoped_operations),
          'request_scope_operation_count',(select count(*) from scope s
            where s.reconcile_chunk_id=c.id),
          'requested_explicit_operation_count',rc.explicit_operation_count,
          'scope_truncated',rc.explicit_operation_count>500,
          'repairs',to_jsonb(r)),
        error_json=null
    from repair_counts r
    cross join reconcile_claims rc
    where rc.reconcile_chunk_id=c.id
    returning c.id,c.operation_id,c.result_json
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'operation_id',operation_id,'status','COMPLETE',
    'phase','COMPLETE','result',result_json
  )),'[]'::jsonb) into v_result from completed_reconcile;

  return coalesce(v_result,'[]'::jsonb);
end;
$function$;

revoke all on function private._invoice_reconcile_advance_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function private._invoice_reconcile_advance_batch(jsonb,timestamptz)
  to service_role;
