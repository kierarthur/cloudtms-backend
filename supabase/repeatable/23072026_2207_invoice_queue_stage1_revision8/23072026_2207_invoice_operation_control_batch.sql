create or replace function public.invoice_operation_control_batch(
  p_actions jsonb,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_jwt_role text:=coalesce(
    nullif(current_setting('request.jwt.claim.role',true),''),
    auth.jwt()->>'role','');
  v_auth_user uuid:=auth.uid();
  v_result jsonb;
begin
  if jsonb_typeof(p_actions)<>'array'
     or jsonb_array_length(p_actions)<1
     or jsonb_array_length(p_actions)>100 then
    raise exception using errcode='22023',
      message='p_actions must be an array containing 1..100 actions';
  end if;

  if not exists(
       select 1 from public.tms_users u
       where u.id=p_actor_user_id and u.is_active and lower(u.role)='admin')
     or(v_jwt_role<>'service_role' and v_auth_user is distinct from p_actor_user_id) then
    raise exception using errcode='42501',message='Active administrator required';
  end if;

  with recursive raw_supplied as materialized (
    select x.ordinality::integer request_no,x.value raw_action,
      coalesce(x.value->>'operation_id','') operation_id_text,
      coalesce(x.value->>'run_after_utc','') run_after_text,
      coalesce(x.value->>'retry_chunk_id','') retry_chunk_id_text,
      x.value->'replacement' replacement_json,
      upper(coalesce(x.value->>'action','')) action,
      coalesce(x.value->>'priority','') priority_text,
      count(*) over(partition by x.value->>'operation_id') duplicate_count
    from jsonb_array_elements(p_actions) with ordinality x(value,ordinality)
  ),
  supplied as materialized (
    select r.request_no,r.raw_action,
      case when r.operation_id_text ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then r.operation_id_text::uuid end operation_id,
      r.action,
      greatest(0,least(case when r.priority_text ~ '^[0-9]{1,4}$'
        then r.priority_text::integer else 1000 end,2000)) requested_priority,
      case when r.run_after_text ~
          '^20[0-9]{2}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,6})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])$'
        and substring(r.run_after_text from 9 for 2)::integer<=case
          when substring(r.run_after_text from 6 for 2)::integer in(4,6,9,11) then 30
          when substring(r.run_after_text from 6 for 2)::integer=2
            then case when substring(r.run_after_text from 1 for 4)::integer%4=0
              then 29 else 28 end
          else 31 end
        then least(r.run_after_text::timestamptz,v_now+interval '30 days') end
        requested_run_after_utc,
      r.run_after_text<>'' run_after_supplied,
      case when r.retry_chunk_id_text ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then r.retry_chunk_id_text::uuid end retry_chunk_id,
      r.retry_chunk_id_text<>'' retry_chunk_supplied,
      jsonb_typeof(r.replacement_json)='object' replacement_requested,
      r.replacement_json->>'work_key' replacement_work_key,
      r.replacement_json->'payload_json' replacement_payload_json,
      r.duplicate_count
    from raw_supplied r
  ),
  operation_tree(request_no,root_operation_id,operation_id,depth,path)
  as materialized (
    select s.request_no,s.operation_id,o.id,0,array[o.id]::uuid[]
    from supplied s
    join public.invoice_operations o on o.id=s.operation_id
    union all
    select t.request_no,t.root_operation_id,child.id,t.depth+1,t.path||child.id
    from operation_tree t
    join public.invoice_operations child on child.parent_operation_id=t.operation_id
    where not child.id=any(t.path)
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      coalesce((select array_agg(distinct operation_id) from supplied
       where operation_id is not null),
       array['00000000-0000-0000-0000-000000000000'::uuid]),
      null,null,10000) g
  ),
  inspected as materialized (
    select s.*,o.status current_status,o.operation_type,o.control_version,
      o.priority current_priority,o.input_json,o.source_revision,
      o.manifest_committed,o.release_complete,
      case
        when s.operation_id is null then 'INVALID_OPERATION_ID'
        when s.action not in('RETRY','CANCEL','RESCHEDULE','RAISE_PRIORITY')
          then 'UNSUPPORTED_ACTION'
        when s.duplicate_count>1 then 'DUPLICATE_OPERATION_ACTION'
        when o.id is null then 'OPERATION_NOT_FOUND'
        when s.action='RESCHEDULE' and s.run_after_supplied
          and s.requested_run_after_utc is null then 'INVALID_RUN_AFTER_UTC'
        when s.action='CANCEL' and exists(
          select 1 from public.invoice_operation_chunks c
          join operation_tree tree
            on tree.request_no=s.request_no and tree.operation_id=c.operation_id
          join public.invoices i on i.id=c.entity_id
          where c.chunk_type='ISSUE_INVOICE'
            and c.entity_type='INVOICE' and i.status in('ISSUED','PAID'))
          then 'COMPLETED_LEGAL_ISSUE_CANNOT_BE_CANCELLED'
        when s.action='RETRY'
          and o.status not in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
          then 'OPERATION_NOT_RETRYABLE'
        when s.action='RETRY'
          and o.input_json->>'contract_version'
            ='INVOICE_BATCH_SELECTION_ROOT_V2'
          and o.error_json->>'code'
            ='BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
          then 'BATCH_FRESH_SELECTION_REQUIRED'
        when s.action='RETRY'
          and o.input_json->>'contract_version'
            ='INVOICE_BATCH_SELECTION_ROOT_V2'
          and not o.manifest_committed
          and o.source_revision is distinct from (
            private._invoice_candidate_snapshot_get_v2(
              o.input_json->>'action',
              v_now
            )->>'revision'
          )
          then 'BATCH_FRESH_SELECTION_REQUIRED'
        when s.action='RETRY'
          and s.retry_chunk_id is not null
          and exists (
            select 1
            from public.invoice_operation_chunks member
            where member.id=s.retry_chunk_id
              and member.is_manifest_member
              and not member.manifest_committed
          )
          then 'UNCOMMITTED_MANIFEST_CARRIER_NOT_RETRYABLE'
        when s.action='RETRY' and s.retry_chunk_supplied
          and s.retry_chunk_id is null then 'INVALID_RETRY_CHUNK_ID'
        when s.replacement_requested and v_jwt_role<>'service_role'
          then 'REPLACEMENT_SERVICE_ONLY'
        when s.replacement_requested and(
          s.action<>'RETRY' or s.retry_chunk_id is null)
          then 'REPLACEMENT_REQUIRES_TARGETED_RETRY'
        when s.replacement_requested
          and jsonb_typeof(s.replacement_payload_json)<>'object'
          then 'INVALID_REPLACEMENT_PAYLOAD'
        when s.replacement_requested
          and nullif(btrim(s.replacement_payload_json->>'source_revision'),'')
            is null
          then 'REPLACEMENT_SOURCE_REVISION_REQUIRED'
        when s.replacement_requested and exists(
          select 1
          from current_graph current
          join public.invoice_operation_chunks old
            on old.id=current.current_chunk_id
          left join public.invoice_document_versions dv
            on dv.id=old.document_version_id
          left join public.invoice_document_assets da
            on da.id=old.document_asset_id
          where current.current_chunk_id=s.retry_chunk_id
            and s.replacement_payload_json->>'source_revision'
              is distinct from coalesce(
                dv.source_revision,da.source_revision,
                old.payload_json->>'source_revision'))
          then 'REPLACEMENT_SOURCE_REVISION_CHANGED'
        when s.action='RETRY' and s.retry_chunk_id is not null and not exists(
          select 1 from current_graph retryable
          where retryable.current_chunk_id=s.retry_chunk_id
            and retryable.operation_id=o.id
            and retryable.replacement_chain_status='VALID'
            and retryable.current_status in(
              'FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT'))
          then 'RETRY_CHUNK_NOT_RETRYABLE'
        when s.action='RETRY' and exists(
          select 1 from current_graph invalid
          where invalid.operation_id=o.id
            and invalid.replacement_chain_status='INVALID')
          then 'INVALID_REPLACEMENT_GRAPH'
        when s.action='RESCHEDULE'
          and o.status not in('QUEUED','RETRY_WAIT','WAITING')
          then 'OPERATION_NOT_RESCHEDULABLE'
        when s.action='RAISE_PRIORITY'
          and o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED')
          then 'TERMINAL_OPERATION_PRIORITY_IMMUTABLE'
      end rejection_code
    from supplied s left join public.invoice_operations o on o.id=s.operation_id
  ),
  locked as materialized (
    select i.*
    from inspected i join public.invoice_operations o on o.id=i.operation_id
    where i.rejection_code is null
    order by o.id for update of o
  ),
  changed_operations as materialized (
    update public.invoice_operations o
    set
      control_version=case when l.action in('RETRY','CANCEL') then o.control_version+1
        else o.control_version end,
      status=case
        when l.action='CANCEL' then 'CANCELLED'
        when l.action='RETRY' then 'QUEUED'
        when l.action='RESCHEDULE' and o.status in('WAITING','RETRY_WAIT') then 'QUEUED'
        else o.status end,
      phase=case
        when l.action='CANCEL' then 'CANCELLED'
        when l.action='RETRY'
          and o.input_json->>'contract_version'
            ='INVOICE_BATCH_SELECTION_ROOT_V2'
          and not o.manifest_committed then 'BUILD_MANIFEST'
        when l.action='RETRY' then 'RETRY'
        else o.phase end,
      priority=case when l.action='RAISE_PRIORITY'
        then greatest(o.priority,l.requested_priority) else o.priority end,
      error_json=case when l.action='RETRY' then jsonb_build_object(
        'history',coalesce((
          select jsonb_agg(h.value order by h.ordinality)
          from jsonb_array_elements(coalesce(o.error_json->'history','[]'::jsonb))
               with ordinality h(value,ordinality)
          where h.ordinality>greatest(
            jsonb_array_length(coalesce(o.error_json->'history','[]'::jsonb))-6,0)
        ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
          'code',coalesce(o.error_json->>'code','UNKNOWN'),'at_utc',v_now)))
        else o.error_json end,
      requires_user_action=case when l.action='RETRY' then false else o.requires_user_action end,
      failed_at_utc=case when l.action='RETRY' then null else o.failed_at_utc end,
      completed_at_utc=case when l.action='RETRY' then null else o.completed_at_utc end,
      updated_at_utc=v_now,change_seq=nextval('public.invoice_operation_change_seq')
    from locked l where o.id=l.operation_id
    returning o.*,l.action,l.requested_priority,l.requested_run_after_utc,
      l.retry_chunk_id,l.replacement_requested,l.replacement_work_key,
      l.replacement_payload_json
  ),
  replacement_chunks as materialized (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,plan_generation,
      sequence_no,level_no,entity_type,entity_id,document_version_id,
      document_asset_id,input_document_version_id,status,priority,
      run_after_utc,payload_json,progress_json,expected_page_count,
      expected_byte_count,attempt_count,max_attempts,fence_token,
      operation_control_version,created_at_utc,updated_at_utc)
    select old.operation_id,old.chunk_type,
      case old.chunk_type
        when 'GENERATION_GROUP' then 'VALIDATE_SOURCES'
        when 'DOCUMENT_PLAN' then 'BUILD_MANIFEST'
        when 'ISSUE_INVOICE' then 'VALIDATE'
        when 'DELIVERY_PREPARE' then 'PREPARE'
        when 'RECONCILE' then 'RECONCILE'
        else old.phase end,
      encode(digest(concat_ws('|','REPLACEMENT',old.operation_id::text,
        old.chunk_type,old.level_no::text,old.sequence_no::text,
        coalesce(old.entity_type,'~'),coalesce(old.entity_id::text,'~'),
        coalesce(old.document_version_id::text,'~'),
        coalesce(old.document_asset_id::text,'~'),
        coalesce(old.input_document_version_id::text,'~'),
        (old.plan_generation+1)::text,changed.replacement_payload_json::text),
        'sha256'),'hex'),
      old.plan_generation+1,
      old.sequence_no,old.level_no,old.entity_type,old.entity_id,
      old.document_version_id,old.document_asset_id,
      old.input_document_version_id,
      case when old.chunk_type='DOCUMENT_INPUT' then 'WAITING'
        else 'QUEUED' end,
      old.priority,v_now,changed.replacement_payload_json,'{}'::jsonb,
      old.expected_page_count,old.expected_byte_count,0,old.max_attempts,0,
      changed.control_version,v_now,v_now
    from changed_operations changed
    join public.invoice_operation_chunks old
      on old.id=changed.retry_chunk_id
    join current_graph current
      on current.current_chunk_id=old.id
      and current.replacement_chain_status='VALID'
    where changed.action='RETRY' and changed.replacement_requested
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning *
  ),
  linked_replacements as materialized (
    update public.invoice_operation_chunks old
    set status='SUPERSEDED',phase='SUPERSEDED',
      replaced_by_chunk_id=fresh.id,replacement_required=true,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=v_now,failed_at_utc=null,updated_at_utc=v_now,
      error_json=jsonb_build_object(
        'code','CHANGED_INPUT_REPLACED',
        'replacement_chunk_id',fresh.id,
        'replacement_plan_generation',fresh.plan_generation)
    from replacement_chunks fresh
    join changed_operations changed
      on changed.id=fresh.operation_id
      and changed.retry_chunk_id is not null
    where old.id=changed.retry_chunk_id
      and fresh.operation_id=old.operation_id
      and fresh.chunk_type=old.chunk_type
      and fresh.level_no=old.level_no
      and fresh.sequence_no=old.sequence_no
      and fresh.entity_type is not distinct from old.entity_type
      and fresh.entity_id is not distinct from old.entity_id
      and fresh.document_version_id is not distinct from old.document_version_id
      and fresh.document_asset_id is not distinct from old.document_asset_id
      and fresh.input_document_version_id is not distinct from
        old.input_document_version_id
      and fresh.plan_generation>old.plan_generation
    returning old.id,old.operation_id
  ),
  cancelled_descendant_operations as materialized (
    update public.invoice_operations child
    set control_version=child.control_version+1,status='CANCELLED',phase='CANCELLED',
        completed_at_utc=v_now,failed_at_utc=null,updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    from changed_operations root
    join operation_tree tree
      on tree.root_operation_id=root.id and tree.operation_id<>root.id
    where root.action='CANCEL' and child.id=tree.operation_id
      and child.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning child.*
  ),
  changed_chunks as materialized (
    update public.invoice_operation_chunks c
    set status=case
          when o.action='CANCEL'
            and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
            then 'CANCELLED'
          when o.action='RETRY'
            and c.is_manifest_member
            and not c.manifest_committed
            then 'WAITING'
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.chunk_type='DOCUMENT_INPUT' then 'WAITING'
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then 'QUEUED'
          when o.action='RESCHEDULE' and c.status='RETRY_WAIT' then 'QUEUED'
          else c.status end,
        phase=case
          when o.action='CANCEL'
            and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
            then 'CANCELLED'
          when o.action='RETRY'
            and c.is_manifest_member
            and not c.manifest_committed
            then 'WAITING_MANIFEST_COMMIT'
          when o.action='RETRY'
            and coalesce(c.payload_json->>'is_selection_expander','false')
              in ('true','t','1','yes','on')
            and not c.manifest_committed
            then 'BUILD_MANIFEST'
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then case c.chunk_type
              when 'GENERATION_GROUP' then 'VALIDATE_SOURCES'
              when 'DOCUMENT_PLAN' then 'BUILD_MANIFEST'
              when 'DOCUMENT_INPUT' then c.phase
              when 'ISSUE_INVOICE' then 'VALIDATE'
              when 'DELIVERY_PREPARE' then 'PREPARE'
              when 'RECONCILE' then 'RECONCILE'
              else c.phase end
          else c.phase end,
        priority=case
          when o.action='RAISE_PRIORITY'
            and c.chunk_type<>'DOCUMENT_INPUT'
            and c.status in('QUEUED','RETRY_WAIT','WAITING')
            then greatest(c.priority,o.requested_priority)
          else c.priority end,
        run_after_utc=case
          when o.action='RESCHEDULE' and c.status in('QUEUED','RETRY_WAIT')
            then coalesce(o.requested_run_after_utc,v_now)
          when o.action='RETRY'
            and c.is_manifest_member
            and not c.manifest_committed
            then c.run_after_utc
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then v_now
          when o.action='RAISE_PRIORITY'
            and c.chunk_type<>'DOCUMENT_INPUT'
            and c.status in('QUEUED','RETRY_WAIT','WAITING')
            then least(c.run_after_utc,v_now)
          else c.run_after_utc end,
        attempt_count=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then 0 else c.attempt_count end,
        result_json=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then null else c.result_json end,
        actual_page_count=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then null else c.actual_page_count end,
        actual_byte_count=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then null else c.actual_byte_count end,
        error_json=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then jsonb_build_object(
              'history',coalesce((
                select jsonb_agg(h.value order by h.ordinality)
                from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                     with ordinality h(value,ordinality)
                where h.ordinality>greatest(
                  jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
              ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
                'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now)))
          else c.error_json end,
        failed_at_utc=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id) then null
          else c.failed_at_utc end,
        completed_at_utc=case
          when o.action='CANCEL'
            and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED') then v_now
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id) then null
          else c.completed_at_utc end,
        lease_owner=case when o.action in('RETRY','CANCEL') then null else c.lease_owner end,
        lease_token=case when o.action in('RETRY','CANCEL') then null else c.lease_token end,
        lease_expires_at_utc=case when o.action in('RETRY','CANCEL') then null
          else c.lease_expires_at_utc end,
        operation_control_version=o.control_version,
        updated_at_utc=v_now
    from changed_operations o
    where c.operation_id=o.id
      and c.id in(select current_chunk_id from current_graph
        where replacement_chain_status='VALID')
      and c.id not in(select id from linked_replacements)
      and(
        o.action in('RETRY','CANCEL')
        or(o.action='RESCHEDULE' and c.status in('QUEUED','RETRY_WAIT'))
        or(o.action='RAISE_PRIORITY' and c.chunk_type<>'DOCUMENT_INPUT'
          and c.status in('QUEUED','RETRY_WAIT','WAITING')))
    returning c.operation_id,c.id
  ),
  cancelled_descendant_chunks as materialized (
    update public.invoice_operation_chunks c
    set status='CANCELLED',phase='CANCELLED',completed_at_utc=v_now,
        failed_at_utc=null,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,
        operation_control_version=o.control_version,updated_at_utc=v_now
    from cancelled_descendant_operations o
    where c.operation_id=o.id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id,c.id,c.document_version_id,c.document_asset_id
  ),
  retry_roots as materialized (
    select c.*
    from changed_operations o
    join changed_chunks changed
      on changed.operation_id=o.id
    join public.invoice_operation_chunks c on c.id=changed.id
    where o.action='RETRY'
    union all
    select r.* from replacement_chunks r
  ),
  retried_assets as materialized (
    update public.invoice_document_assets a
    set status=case
          when r.chunk_type='ASSET_INSPECT' then 'DISCOVERED'
          else 'NORMALISING'
        end,
        normalised_manifest_json=case when r.chunk_type='ASSET_INSPECT'
          then '[]'::jsonb else a.normalised_manifest_json end,
        normalised_r2_key=null,normalised_sha256=null,
        normalised_manifest_hash=null,normalised_size_bytes=null,
        normalised_page_count=null,ready_at_utc=null,error_json=null,
        updated_at_utc=v_now
    from retry_roots r
    where r.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE')
      and a.id=r.document_asset_id and a.status<>'READY'
    returning a.id
  ),
  retry_document_scope as materialized (
    select distinct d.document_version_id,
      min(case
        when r.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          or r.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE') then 0
        when r.chunk_type='PDF_MERGE' then r.level_no
        when r.chunk_type='DOCUMENT_VERIFY' then 2147483647
        else 0 end) retry_level,
      bool_or(r.chunk_type in(
        'SOURCE_RENDER','INVOICE_CORE_RENDER','ASSET_INSPECT','ASSET_NORMALISE')) reset_inputs
    from retry_roots r
    join public.invoice_operation_chunks d
      on d.document_version_id=r.document_version_id
      or(r.document_asset_id is not null
        and d.chunk_type='DOCUMENT_INPUT'
        and d.document_asset_id=r.document_asset_id)
    where r.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER','ASSET_INSPECT',
      'ASSET_NORMALISE','PDF_MERGE','DOCUMENT_VERIFY')
      and d.document_version_id is not null
    group by d.document_version_id
  ),
  fenced_retry_document_operations as materialized (
    update public.invoice_operations o
    set control_version=o.control_version+1,updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in(
        select distinct v.operation_id
        from retry_document_scope scope
        join public.invoice_document_versions v on v.id=scope.document_version_id)
      and o.id not in(select id from changed_operations)
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning o.id,o.control_version
  ),
  fenced_retry_document_chunks as materialized (
    update public.invoice_operation_chunks c
    set operation_control_version=o.control_version,
        status=case when c.status='RUNNING' then 'RETRY_WAIT' else c.status end,
        run_after_utc=case when c.status='RUNNING' then v_now else c.run_after_utc end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=case when c.status='RUNNING' then jsonb_build_object(
          'code','UPSTREAM_RETRY_FENCED','at_utc',v_now) else c.error_json end,
        updated_at_utc=v_now
    from fenced_retry_document_operations o
    where c.operation_id=o.id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.id,c.operation_id
  ),
  retried_render_dependencies as (
    update public.invoice_operation_chunks d
    set status='WAITING',completed_at_utc=null,failed_at_utc=null,
        result_json=null,actual_page_count=null,actual_byte_count=null,
        error_json=null,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,updated_at_utc=v_now
    from retry_roots r
    where d.chunk_type='DOCUMENT_INPUT'
      and d.document_version_id=r.document_version_id
      and r.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
      and(
        d.payload_json->>'source_chunk_key'=r.payload_json->>'source_chunk_key'
        or(d.entity_type=r.entity_type and d.entity_id=r.entity_id
          and d.payload_json->>'input_type'=case
            when r.chunk_type='INVOICE_CORE_RENDER' then 'INVOICE_CORE'
            else r.payload_json->>'render_kind' end))
      and d.status in('COMPLETE','FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
    returning d.operation_id,d.document_version_id
  ),
  retried_asset_dependencies as (
    update public.invoice_operation_chunks d
    set status='WAITING',completed_at_utc=null,failed_at_utc=null,
        result_json=null,actual_page_count=null,actual_byte_count=null,
        error_json=null,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,updated_at_utc=v_now
    where d.chunk_type='DOCUMENT_INPUT'
      and d.document_asset_id in(select id from retried_assets)
      and d.status in('COMPLETE','FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
    returning d.operation_id,d.document_version_id
  ),
  invalidated_stale_document_chunks as (
    update public.invoice_operation_chunks stale
    set status='BLOCKED',phase='BLOCKED',
        result_json=null,actual_page_count=null,actual_byte_count=null,
        completed_at_utc=null,failed_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','UPSTREAM_RETRY_INVALIDATED',
          'requires_replacement',true,
          'invalidated_at_utc',v_now),
        updated_at_utc=v_now
    from retry_document_scope scope
    where stale.document_version_id=scope.document_version_id
      and(
        stale.chunk_type='DOCUMENT_VERIFY'
          and not exists(
            select 1 from retry_roots selected
            where selected.id=stale.id and selected.chunk_type='DOCUMENT_VERIFY')
        or stale.chunk_type='PDF_MERGE'
          and(
            (scope.reset_inputs and stale.level_no>=scope.retry_level)
            or(not scope.reset_inputs and stale.level_no>scope.retry_level)))
      and stale.status<>'SUPERSEDED'
    returning stale.operation_id,stale.document_version_id,stale.id
  ),
  retried_document_versions as (
    update public.invoice_document_versions v
    set status=case when scope.reset_inputs then 'WAITING_FOR_INPUTS'
          when scope.retry_level<2147483647 then 'ASSEMBLING'
          else 'VERIFYING' end,
        r2_key=null,sha256=null,size_bytes=null,page_count=null,
        ready_at_utc=null,verified_at_utc=null,error_json=null
    from retry_document_scope scope
    where v.id=scope.document_version_id
      and v.status not in('READY','SUPERSEDED','CANCELLED')
    returning v.id
  ),
  retried_document_plans as (
    update public.invoice_operation_chunks p
    set status='QUEUED',
        phase=case when scope.reset_inputs then 'WAIT_FOR_INPUTS' else 'PLAN_MERGES' end,
        run_after_utc=v_now,
        failed_at_utc=null,error_json=null,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,updated_at_utc=v_now
    from retry_document_scope scope
    where p.chunk_type='DOCUMENT_PLAN' and p.document_version_id=scope.document_version_id
      and p.status in('WAITING','RETRY_WAIT','BLOCKED','FAILED','DEAD_LETTER')
    returning p.id
  ),
  cancelled_document_versions as (
    update public.invoice_document_versions v
    set status='CANCELLED',error_json=jsonb_build_object(
          'code','OPERATION_CANCELLED','at_utc',v_now),
        superseded_at_utc=v_now
    where v.operation_id in(
        select id from changed_operations where action='CANCEL'
        union all select id from cancelled_descendant_operations)
      and v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING','FAILED')
    returning v.id,v.operation_id
  ),
  cancelled_operation_set as materialized (
    select id from changed_operations where action='CANCEL'
    union
    select id from cancelled_descendant_operations
  ),
  cancelled_invoice_pointers as (
    update public.invoices i
    set active_document_operation_id=case when i.active_document_operation_id=o.id
          then null else i.active_document_operation_id end,
        active_issue_operation_id=case when i.active_issue_operation_id=o.id
          then null else i.active_issue_operation_id end,
        document_state=case when i.active_document_operation_id=o.id
          and i.document_state in('QUEUED','PREPARING') then 'STALE' else i.document_state end,
        issue_state=case when i.active_issue_operation_id=o.id
          and i.issue_state not in('ISSUED') then 'CANCELLED' else i.issue_state end,
        updated_at=v_now
    from cancelled_operation_set cancelled
    join public.invoice_operations o on o.id=cancelled.id
    where i.active_document_operation_id=o.id or i.active_issue_operation_id=o.id
    returning i.id
  ),
  cancelled_timesheet_pointers as (
    update public.timesheets t
    set active_document_operation_id=null,
        document_state=case when t.document_state in('QUEUED','PREPARING')
          then 'STALE' else t.document_state end,
        updated_at=v_now
    from cancelled_operation_set cancelled
    join public.invoice_operations o on o.id=cancelled.id
    where t.active_document_operation_id=o.id
    returning t.timesheet_id
  ),
  affected_operation_ids as materialized (
    select id operation_id from changed_operations
    union select id from cancelled_descendant_operations
    union select id from fenced_retry_document_operations
    union select operation_id from retried_render_dependencies
    union select operation_id from retried_asset_dependencies
    union select operation_id from invalidated_stale_document_chunks
  ),
  recalculated_operations as materialized (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(operation_id) from affected_operation_ids),
        array[]::uuid[]),v_now,true) r
  ),
  results as (
    select i.request_no,jsonb_build_object(
      'operation_id',i.operation_id,'action',i.action,'accepted',false,
      'status',i.current_status,'control_version',i.control_version,
      'error',jsonb_build_object('code',i.rejection_code)) result
    from inspected i where i.rejection_code is not null
    union all
    select i.request_no,jsonb_build_object(
      'operation_id',changed.id,'action',changed.action,'accepted',true,
      'status',recalculated.status,'phase',recalculated.phase,
      'control_version',changed.control_version,
      'change_seq',recalculated.change_seq,'priority',changed.priority,
      'total_units',recalculated.total_units,
      'completed_units',recalculated.completed_units,
      'failed_units',recalculated.failed_units) result
    from inspected i
    join changed_operations changed on changed.id=i.operation_id
    join recalculated_operations recalculated
      on recalculated.operation_id=i.operation_id
  )
  select coalesce(jsonb_agg(result order by request_no),'[]'::jsonb)
  into v_result from results;

  return v_result;
end;
$function$;

revoke all on function public.invoice_operation_control_batch(jsonb,uuid,timestamptz)
  from public,anon;
grant execute on function public.invoice_operation_control_batch(jsonb,uuid,timestamptz)
  to authenticated,service_role;
