create or replace function public.invoice_operation_advance_batch(
  p_claims jsonb,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_valid jsonb := '[]'::jsonb;
  v_rejected jsonb := '[]'::jsonb;
  v_result jsonb := '[]'::jsonb;
  v_part jsonb := '[]'::jsonb;
  v_ignored integer;
begin
  if jsonb_typeof(p_claims)<>'array'
     or jsonb_array_length(p_claims)<1
     or jsonb_array_length(p_claims)>100 then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing between 1 and 100 claims';
  end if;

  /*
   * Ownership is classified per supplied item.  Bad input, a missing row, an
   * expired lease, or a stale fence is returned as a typed rejection and does
   * not roll back unrelated current claims.
   */
  with supplied as materialized (
    select
      x.ordinality::integer request_no,
      x.value raw_claim,
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
  inspected as materialized (
    select s.*,c.operation_id,c.chunk_type,c.phase,c.entity_type,c.entity_id,
      c.document_version_id,c.document_asset_id,c.payload_json,
      c.status current_status,c.lease_token current_lease_token,
      c.fence_token current_fence_token,
      c.operation_control_version current_control_version,
      c.lease_expires_at_utc,o.status operation_status,
      o.control_version operation_current_control_version,
      case
        when s.chunk_id is null or s.lease_token is null
          or s.fence_token is null or s.operation_control_version is null then 'INVALID_CLAIM'
        when c.id is null then 'CHUNK_NOT_FOUND'
        when c.status<>'RUNNING' then 'CHUNK_NOT_RUNNING'
        when c.lease_token is distinct from s.lease_token then 'LEASE_TOKEN_MISMATCH'
        when c.fence_token is distinct from s.fence_token then 'FENCE_TOKEN_MISMATCH'
        when c.operation_control_version is distinct from s.operation_control_version
          or o.control_version is distinct from s.operation_control_version then 'CONTROL_VERSION_MISMATCH'
        when c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now then 'LEASE_EXPIRED'
        when o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED') then 'OPERATION_TERMINAL'
      end rejection_code
    from supplied s
    left join public.invoice_operation_chunks c on c.id=s.chunk_id
    left join public.invoice_operations o on o.id=c.operation_id
  ),
  locked as materialized (
    select i.*
    from inspected i
    join public.invoice_operation_chunks c on c.id=i.chunk_id
    where i.rejection_code is null
    order by c.id
    for update of c
  )
  select
    coalesce((select jsonb_agg(jsonb_build_object(
      'chunk_id',l.chunk_id,'operation_id',l.operation_id,'chunk_type',l.chunk_type,
      'phase',l.phase,'entity_type',l.entity_type,'entity_id',l.entity_id,
      'document_version_id',l.document_version_id,'document_asset_id',l.document_asset_id,
      'payload_json',l.payload_json,'lease_token',l.lease_token,
      'fence_token',l.fence_token,
      'operation_control_version',l.operation_control_version
    ) order by l.request_no) from locked l),'[]'::jsonb),
    coalesce((select jsonb_agg(jsonb_build_object(
      'chunk_id',i.chunk_id,'status','REJECTED','phase','OWNERSHIP_REJECTED',
      'accepted',false,'code',i.rejection_code,'request_no',i.request_no
    ) order by i.request_no) from inspected i where i.rejection_code is not null),'[]'::jsonb)
  into v_valid,v_rejected;

  if jsonb_array_length(v_valid)>0 then
    v_part:=private._invoice_dispatch_advance_batch(v_valid,v_now);
    v_result:=v_result||coalesce(v_part,'[]'::jsonb);
  end if;

  /*
   * A private dispatcher must move every accepted claim out of RUNNING.  If
   * it did not, preserve the diagnostic and schedule a bounded retry instead
   * of clearing the lease and pretending the phase succeeded.
   */
  with valid_ids as (
    select (x->>'chunk_id')::uuid id
    from jsonb_array_elements(v_valid) x
  ),
  unhandled as (
    update public.invoice_operation_chunks c
    set status=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else 'RETRY_WAIT' end,
        phase=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else c.phase end,
        run_after_utc=case when c.attempt_count>=c.max_attempts then c.run_after_utc
          else v_now+make_interval(secs=>least(900,15*(2^least(c.attempt_count,6)))::integer)
             +(random()*10||' seconds')::interval end,
        error_json=jsonb_build_object(
          'code','UNHANDLED_PHASE_RESULT',
          'chunk_type',c.chunk_type,
          'phase',c.phase,
          'history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                 with ordinality h(value,ordinality)
            where h.ordinality>greatest(
              jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),
            'at_utc',v_now))),
        failed_at_utc=case when c.attempt_count>=c.max_attempts then v_now else null end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    where c.id in(select id from valid_ids) and c.status='RUNNING'
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  )
  select v_result||coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,'error',error_json
  )),'[]'::jsonb) into v_result from unhandled;

  /* Non-running rows are never allowed to retain a current lease. */
  with valid_ids as (
    select (x->>'chunk_id')::uuid id from jsonb_array_elements(v_valid) x
  ),
  released as (
    update public.invoice_operation_chunks c
    set lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now
    where c.id in(select id from valid_ids) and c.status<>'RUNNING'
    returning c.operation_id
  ),
  affected as (
    select distinct operation_id from released
  )
  select count(*) into v_ignored
  from private._invoice_operation_rollup_batch(
    coalesce((select array_agg(operation_id) from affected),array[]::uuid[]),
    v_now,true);

  return coalesce(v_result,'[]'::jsonb)||v_rejected;
end;
$function$;

revoke all on function public.invoice_operation_advance_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function public.invoice_operation_advance_batch(jsonb,timestamptz)
  to service_role;
