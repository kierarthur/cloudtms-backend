create or replace function public.invoice_work_claim_batch(
  p_chunk_types text[],
  p_worker_id text,
  p_limit integer default 25,
  p_lease_seconds integer default 60,
  p_now_utc timestamptz default now()
) returns table(
  chunk_id uuid, operation_id uuid, chunk_type text, phase text,
  entity_type text, entity_id uuid, document_version_id uuid, document_asset_id uuid,
  input_document_version_id uuid, payload_json jsonb, lease_token uuid,
  fence_token bigint, operation_control_version bigint, lease_expires_at_utc timestamptz,
  attempt_count integer, max_attempts integer, priority integer,
  expected_page_count integer, expected_byte_count bigint
)
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_limit integer := greatest(1,least(coalesce(p_limit,25),100));
  v_lease integer := greatest(15,least(coalesce(p_lease_seconds,60),600));
  v_types text[];
begin
  if coalesce(btrim(p_worker_id),'')='' then
    raise exception using errcode='22023',message='p_worker_id is required';
  end if;
  select coalesce(array_agg(distinct upper(btrim(x)) order by upper(btrim(x))),array[]::text[])
    into v_types from unnest(coalesce(p_chunk_types,array[]::text[])) x
    where upper(btrim(x)) in (
      'GENERATION_GROUP','DOCUMENT_PLAN','ASSET_INSPECT','ASSET_NORMALISE',
      'SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE','DOCUMENT_VERIFY',
      'ISSUE_INVOICE','DELIVERY_PREPARE','RECONCILE'
    );
  if cardinality(v_types)=0 then
    raise exception using errcode='22023',message='At least one runnable chunk type is required';
  end if;

  return query
  with invalid_running as (
    update public.invoice_operation_chunks c
    set status=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else 'RETRY_WAIT' end,
        phase=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else c.phase end,
        run_after_utc=case when c.attempt_count>=c.max_attempts then c.run_after_utc else v_now end,
        failed_at_utc=case when c.attempt_count>=c.max_attempts then v_now else null end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code',case
            when c.lease_token is null then 'RUNNING_WITHOUT_LEASE_TOKEN'
            when c.lease_owner is null then 'RUNNING_WITHOUT_LEASE_OWNER'
            when c.lease_expires_at_utc is null then 'RUNNING_WITHOUT_LEASE_EXPIRY'
            else 'LEASE_EXPIRED' end,
          'retryable',c.attempt_count<c.max_attempts,
          'history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                 with ordinality h(value,ordinality)
            where h.ordinality>greatest(
              jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now))),
        updated_at_utc=v_now
    from public.invoice_operations o
    where o.id=c.operation_id and c.chunk_type=any(v_types) and c.status='RUNNING'
      and(c.lease_token is null or c.lease_owner is null
        or c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now)
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
    returning c.id,c.operation_id,c.document_asset_id,c.document_version_id,c.status
  ),
  exhausted as (
    update public.invoice_operation_chunks c
    set status='DEAD_LETTER',failed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object('code','MAX_ATTEMPTS_EXHAUSTED','retryable',false,
          'attempt_count',c.attempt_count,'max_attempts',c.max_attempts)
    from public.invoice_operations o
    where o.id=c.operation_id
      and c.chunk_type=any(v_types)
      and c.status in ('QUEUED','RETRY_WAIT')
      and c.attempt_count>=c.max_attempts
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
    returning c.id,c.operation_id,c.document_asset_id,c.document_version_id
  ),
  all_exhausted as materialized (
    select e.id,e.operation_id,e.document_asset_id,e.document_version_id
    from exhausted e
    union all
    select r.id,r.operation_id,r.document_asset_id,r.document_version_id
    from invalid_running r where r.status='DEAD_LETTER'
  ),
  failed_assets as (
    update public.invoice_document_assets a
    set status='FAILED',updated_at_utc=v_now,
        error_json=jsonb_build_object('code','ASSET_WORK_DEAD_LETTER',
          'chunk_id',e.id,'retryable',false)
    from all_exhausted e
    where a.id=e.document_asset_id and a.status not in('READY','SUPERSEDED')
    returning a.id
  ),
  failed_versions as (
    update public.invoice_document_versions v
    set status='FAILED',
        error_json=jsonb_build_object('code','DOCUMENT_WORK_DEAD_LETTER',
          'retryable',false,'failed_at_utc',v_now)
    where v.id in(
        select e.document_version_id from all_exhausted e
        where e.document_version_id is not null)
      and v.status not in('READY','SUPERSEDED','CANCELLED','FAILED')
    returning v.id
  ),
  blocked_dependencies as (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object('code','DEPENDENCY_DEAD_LETTER',
          'document_asset_id',d.document_asset_id,
          'input_document_version_id',d.input_document_version_id)
    where d.chunk_type='DOCUMENT_INPUT' and d.status='WAITING'
      and(
        d.document_asset_id in(select id from failed_assets)
        or d.input_document_version_id in(select id from failed_versions))
    returning d.operation_id
  ),
  dead_ops as (
    update public.invoice_operations o
    set status=case when exists(
          select 1 from public.invoice_operation_chunks b
          where b.operation_id=o.id and b.status='BLOCKED') then 'BLOCKED' else 'DEAD_LETTER' end,
        phase=case when exists(
          select 1 from public.invoice_operation_chunks b
          where b.operation_id=o.id and b.status='BLOCKED') then 'BLOCKED' else 'DEAD_LETTER' end,
        requires_user_action=true,
        failed_at_utc=coalesce(o.failed_at_utc,v_now),updated_at_utc=v_now,
        error_json=jsonb_build_object('code','MAX_ATTEMPTS_EXHAUSTED','summary','One or more chunks exhausted retry attempts'),
        change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in(
        select e.operation_id from all_exhausted e
        union select b.operation_id from blocked_dependencies b)
    returning o.id
  ),
  dead_rollup as materialized (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(id) from dead_ops),array[]::uuid[]),
      v_now,true) r
  ),
  candidate_rows as materialized (
    select c.id,c.operation_id,
      (c.priority+least(100,floor(extract(epoch from
        (v_now-c.created_at_utc))/3600)::integer)) effective_priority,
      c.run_after_utc,c.created_at_utc
    from public.invoice_operation_chunks c
    join public.invoice_operations o on o.id=c.operation_id
    where c.chunk_type=any(v_types)
      and c.chunk_type<>'DOCUMENT_INPUT'
      and c.status in ('QUEUED','RETRY_WAIT')
      and c.run_after_utc<=v_now
      and c.attempt_count<c.max_attempts
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      and (
        not c.is_manifest_member
        or c.manifest_committed
      )
      and (
        o.entity_type is distinct from 'INVOICE_BATCH'
        or o.manifest_committed
        or coalesce(c.payload_json->>'is_selection_expander','false')
          in ('true','t','1','yes','on')
      )
      and(select count(*) from dead_rollup)>=0
    order by (c.priority + least(100,floor(extract(epoch from (v_now-c.created_at_utc))/3600)::integer)) desc,
      c.run_after_utc,c.created_at_utc,c.id
    limit least(500,v_limit*5)
  ),
  candidate_graph as materialized (
    select g.*
    from private._invoice_current_chunk_ids_v2(
      coalesce((select array_agg(distinct candidate.id)
        from candidate_rows candidate),
        array['00000000-0000-0000-0000-000000000000'::uuid]),
      500) g
  ),
  picked as materialized (
    select c.id
    from candidate_rows candidate
    join candidate_graph g on g.requested_chunk_id=candidate.id
      and g.current_chunk_id=candidate.id
      and g.replacement_chain_status='CURRENT'
    join public.invoice_operation_chunks c on c.id=candidate.id
    order by candidate.effective_priority desc,
      candidate.run_after_utc,candidate.created_at_utc,candidate.id
    for update of c skip locked
    limit v_limit
  ),
  claimed as (
    update public.invoice_operation_chunks c
    set status='RUNNING',lease_owner=p_worker_id,lease_token=gen_random_uuid(),
        lease_expires_at_utc=v_now+make_interval(secs=>v_lease),
        fence_token=c.fence_token+1,operation_control_version=o.control_version,
        attempt_count=c.attempt_count+1,started_at_utc=coalesce(c.started_at_utc,v_now),
        updated_at_utc=v_now,
        error_json=case
          when c.error_json is null then null
          else jsonb_build_object('history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(
              c.error_json->'history','[]'::jsonb))
              with ordinality h(value,ordinality)
            where h.ordinality>greatest(jsonb_array_length(coalesce(
              c.error_json->'history','[]'::jsonb))-7,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now)))
          end
    from picked p,public.invoice_operations o
    where c.id=p.id and o.id=c.operation_id
    returning c.*
  ),
  touched_ops as (
    update public.invoice_operations o
    set status='RUNNING',
        phase=coalesce((
          select c.phase from claimed c where c.operation_id=o.id
          order by c.priority desc,c.sequence_no,c.id limit 1),o.phase),
        started_at_utc=coalesce(o.started_at_utc,v_now),
        updated_at_utc=v_now,change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in (select distinct cl.operation_id from claimed cl)
      and o.status in ('QUEUED','WAITING','RETRY_WAIT','RUNNING')
    returning o.id
  )
  select c.id,c.operation_id,c.chunk_type,c.phase,c.entity_type,c.entity_id,
    c.document_version_id,c.document_asset_id,c.input_document_version_id,
    c.payload_json,c.lease_token,c.fence_token,c.operation_control_version,
    c.lease_expires_at_utc,c.attempt_count,c.max_attempts,c.priority,
    c.expected_page_count,c.expected_byte_count
  from claimed c
  order by c.priority desc,c.created_at_utc,c.id;
end;
$function$;

revoke all on function public.invoice_work_claim_batch(text[],text,integer,integer,timestamptz) from public,anon,authenticated;
grant execute on function public.invoice_work_claim_batch(text[],text,integer,integer,timestamptz) to service_role;
