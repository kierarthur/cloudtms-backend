create or replace function public.invoice_work_touch_batch(
  p_touches jsonb,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb;
begin
  if jsonb_typeof(p_touches)<>'array'
     or jsonb_array_length(p_touches)<1
     or jsonb_array_length(p_touches)>100 then
    raise exception using errcode='22023',
      message='p_touches must be an array containing 1..100 items';
  end if;

  with recursive supplied as materialized (
    select x.ordinality::integer request_no,x.value raw_touch,
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
      coalesce(x.value->'progress_patch','{}'::jsonb) patch,
      greatest(15,least(case
        when coalesce(x.value->>'lease_extension_seconds','') ~ '^[0-9]{1,4}$'
          then (x.value->>'lease_extension_seconds')::integer else 60 end,300)) extension_seconds
    from jsonb_array_elements(p_touches) with ordinality x(value,ordinality)
  ),
  ownership_graph as materialized (
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
    select s.*,c.operation_id,c.status current_status,c.lease_token current_lease_token,
      c.fence_token current_fence_token,c.operation_control_version current_control_version,
      c.lease_expires_at_utc,o.control_version operation_current_control_version,
      o.status operation_status,
      case
        when s.chunk_id is null or s.lease_token is null
          or s.fence_token is null or s.operation_control_version is null then 'INVALID_TOUCH'
        when jsonb_typeof(s.patch)<>'object' or octet_length(s.patch::text)>16384
          or lower(s.patch::text) ~
            '"(base64|file_bytes|raw_bytes|snapshot_json|manifest_json|ordered_inputs|processor_dump)"[[:space:]]*:'
          then 'INVALID_PROGRESS_PATCH'
        when c.id is null then 'CHUNK_NOT_FOUND'
        when exists(select 1 from ownership_graph g
          where g.operation_id=c.operation_id
            and g.replacement_chain_status='INVALID')
          then 'INVALID_REPLACEMENT_GRAPH'
        when not exists(select 1 from ownership_graph g
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
      end rejection_code
    from supplied s
    left join public.invoice_operation_chunks c on c.id=s.chunk_id
    left join public.invoice_operations o on o.id=c.operation_id
  ),
  eligible as materialized (
    select i.*,c.progress_json old_progress,
      jsonb_strip_nulls(i.patch) effective_patch,
      exists(
        select 1
        from unnest(array[
          'status_message','pages_complete','pages_total',
          'bytes_complete','bytes_total','merge_level',
          'parts_complete','parts_total','verification_phase','phase']) k
        where jsonb_strip_nulls(i.patch)?k
          and c.progress_json->k
            is distinct from jsonb_strip_nulls(i.patch)->k
      ) material_change
    from inspected i
    join public.invoice_operation_chunks c on c.id=i.chunk_id
    where i.rejection_code is null
  ),
  updated as materialized (
    update public.invoice_operation_chunks c
    set progress_json=coalesce(c.progress_json,'{}'::jsonb)||i.effective_patch,
        lease_expires_at_utc=greatest(
          c.lease_expires_at_utc,v_now+make_interval(secs=>i.extension_seconds)),
        updated_at_utc=v_now
    from eligible i
    where c.id=i.chunk_id
    returning c.id,c.operation_id,c.lease_expires_at_utc,c.progress_json,
      i.material_change
  ),
  affected as materialized (
    select distinct operation_id from updated
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      (select array_agg(operation_id) from affected),null,null,10000) g
    where g.replacement_chain_status='VALID'
  ),
  phase_scope as materialized (
    select distinct on(c.operation_id)
      c.operation_id,c.chunk_type,
      case when c.chunk_type='PDF_MERGE' then c.level_no end level_no
    from current_graph g
    join public.invoice_operation_chunks c on c.id=g.current_chunk_id
    where c.status in('RUNNING','QUEUED','RETRY_WAIT','WAITING')
    order by c.operation_id,
      case c.status when 'RUNNING' then 0 when 'QUEUED' then 1
        when 'RETRY_WAIT' then 2 else 3 end,
      c.level_no desc,c.priority desc,c.updated_at_utc desc,c.id
  ),
  grouped as materialized (
    select c.operation_id,count(*)::integer total,
      count(*) filter(where c.status='COMPLETE')::integer completed,
      count(*) filter(where c.status in('FAILED','DEAD_LETTER','BLOCKED'))::integer failed,
      max(c.updated_at_utc) latest_update,
      (array_agg(c.progress_json->>'status_message'
        order by(c.progress_json?'status_message') desc,c.updated_at_utc desc,c.id))[1] status_message,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'pages_complete','')~'^[0-9]+$'
        then(c.progress_json->>'pages_complete')::bigint else 0 end)
          pages_complete,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'pages_total','')~'^[0-9]+$'
        then(c.progress_json->>'pages_total')::bigint else 0 end)
          pages_total,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'bytes_complete','')~'^[0-9]+$'
        then(c.progress_json->>'bytes_complete')::bigint else 0 end)
          bytes_complete,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'bytes_total','')~'^[0-9]+$'
        then(c.progress_json->>'bytes_total')::bigint else 0 end)
          bytes_total,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'parts_complete','')~'^[0-9]+$'
        then(c.progress_json->>'parts_complete')::bigint else 0 end)
          parts_complete,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'parts_total','')~'^[0-9]+$'
        then(c.progress_json->>'parts_total')::bigint else 0 end)
          parts_total,
      max(case when coalesce(c.progress_json->>'merge_level','')~'^[0-9]+$'
        then(c.progress_json->>'merge_level')::integer end) merge_level,
      (array_agg(c.progress_json->>'verification_phase'
        order by(c.progress_json?'verification_phase') desc,
          c.updated_at_utc desc,c.id))[1] verification_phase,
      bool_or(u.material_change) material_change
    from current_graph current
    join public.invoice_operation_chunks c on c.id=current.current_chunk_id
    join phase_scope s on s.operation_id=c.operation_id
    left join updated u on u.id=c.id
    group by c.operation_id
  ),
  operation_progress as (
    update public.invoice_operations o
    set total_units=g.total,chunk_count=g.total,completed_units=g.completed,failed_units=g.failed,
        progress_json=coalesce(o.progress_json,'{}'::jsonb)||jsonb_build_object(
          'status_message',coalesce(g.status_message,o.progress_json->>'status_message'),
          'completed_units',g.completed,'failed_units',g.failed,
          'pages_complete',g.pages_complete,'pages_total',g.pages_total,
          'bytes_complete',g.bytes_complete,'bytes_total',g.bytes_total,
          'parts_complete',g.parts_complete,'parts_total',g.parts_total,
          'merge_level',g.merge_level,'verification_phase',g.verification_phase),
        updated_at_utc=v_now,
        change_seq=case when g.material_change
          then nextval('public.invoice_operation_change_seq') else o.change_seq end
    from grouped g where o.id=g.operation_id
    returning o.id
  ),
  ancestor_scope(id) as (
    select distinct o.parent_operation_id
    from public.invoice_operations o
    join grouped g on g.operation_id=o.id and g.material_change
    where o.parent_operation_id is not null
    union
    select distinct o.parent_operation_id
    from public.invoice_operations o
    join ancestor_scope s on s.id=o.id
    where o.parent_operation_id is not null
  ),
  ancestor_updates as (
    update public.invoice_operations o
    set updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in(select id from ancestor_scope)
      and o.status not in('CANCELLED','SUPERSEDED')
    returning o.id
  ),
  results as (
    select i.request_no,jsonb_build_object(
      'chunk_id',i.chunk_id,'status','REJECTED','accepted',false,'code',i.rejection_code) result
    from inspected i where i.rejection_code is not null
    union all
    select i.request_no,jsonb_build_object(
      'chunk_id',u.id,'status','TOUCHED','accepted',true,
      'lease_expires_at_utc',u.lease_expires_at_utc,'progress',u.progress_json) result
    from inspected i join updated u on u.id=i.chunk_id
  )
  select coalesce(jsonb_agg(result order by request_no),'[]'::jsonb)
  into v_result from results;

  return v_result;
end;
$function$;

revoke all on function public.invoice_work_touch_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function public.invoice_work_touch_batch(jsonb,timestamptz)
  to service_role;
