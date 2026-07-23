create or replace function private._invoice_dispatch_advance_batch(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_chunk_type text;
  v_group jsonb;
  v_part jsonb;
  v_result jsonb := '[]'::jsonb;
  v_error_code text;
  v_error_message text;
  v_error_detail text;
  v_error_hint text;
begin
  if jsonb_typeof(p_claims)<>'array'
     or jsonb_array_length(p_claims)>100 then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing at most 100 claims';
  end if;

  for v_chunk_type in
    select distinct x.value->>'chunk_type'
    from jsonb_array_elements(p_claims) x(value)
    where x.value->>'chunk_type' in(
      'GENERATION_GROUP','DOCUMENT_PLAN','ISSUE_INVOICE',
      'DELIVERY_PREPARE','RECONCILE')
  loop
    select jsonb_agg(x.value order by x.ordinality)
    into v_group
    from jsonb_array_elements(p_claims) with ordinality x(value,ordinality)
    where x.value->>'chunk_type'=v_chunk_type;

    /* One set-based call is made for each chunk type.  Expected business
       invalidity is returned by processors as typed rows. */
    begin
      case v_chunk_type
        when 'GENERATION_GROUP' then
          v_part:=private._invoice_generation_advance_batch(v_group,v_now);
        when 'DOCUMENT_PLAN' then
          v_part:=private._invoice_document_advance_batch(v_group,v_now);
        when 'ISSUE_INVOICE' then
          v_part:=private._invoice_issue_advance_batch(v_group,v_now);
        when 'DELIVERY_PREPARE' then
          v_part:=private._invoice_delivery_advance_batch(v_group,v_now);
        when 'RECONCILE' then
          v_part:=private._invoice_reconcile_advance_batch(v_group,v_now);
      end case;
      v_result:=v_result||coalesce(v_part,'[]'::jsonb);
    exception when others then
      get stacked diagnostics
        v_error_code=returned_sqlstate,
        v_error_message=message_text,
        v_error_detail=pg_exception_detail,
        v_error_hint=pg_exception_hint;

      with supplied as materialized (
        select (x.value->>'chunk_id')::uuid chunk_id,
          (x.value->>'lease_token')::uuid lease_token,
          (x.value->>'fence_token')::bigint fence_token
        from jsonb_array_elements(v_group) x(value)
      ),
      changed as (
        update public.invoice_operation_chunks c
        set status=case when c.attempt_count>=c.max_attempts
              then 'DEAD_LETTER' else 'RETRY_WAIT' end,
            phase=case when c.attempt_count>=c.max_attempts
              then 'DEAD_LETTER' else c.phase end,
            run_after_utc=case when c.attempt_count>=c.max_attempts
              then c.run_after_utc
              else v_now+make_interval(secs=>
                least(900,15*(2^least(c.attempt_count,6)))::integer)
                +make_interval(secs=>(random()*10)::integer) end,
            error_json=jsonb_build_object(
              'code','PHASE_PROCESSOR_EXCEPTION',
              'chunk_type',v_chunk_type,'sqlstate',v_error_code,
              'message',left(coalesce(v_error_message,'Processor exception'),500),
              'detail',nullif(left(coalesce(v_error_detail,''),500),''),
              'hint',nullif(left(coalesce(v_error_hint,''),300),''),
              'retryable',c.attempt_count<c.max_attempts,'at_utc',v_now,
              'history',coalesce((
                select jsonb_agg(h.value order by h.ordinality)
                from jsonb_array_elements(
                  coalesce(c.error_json->'history','[]'::jsonb))
                  with ordinality h(value,ordinality)
                where h.ordinality>greatest(jsonb_array_length(
                  coalesce(c.error_json->'history','[]'::jsonb))-6,0)
              ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
                'code',coalesce(c.error_json->>'code','UNKNOWN'),
                'at_utc',v_now))),
            failed_at_utc=case when c.attempt_count>=c.max_attempts
              then v_now else null end,
            lease_owner=null,lease_token=null,lease_expires_at_utc=null,
            updated_at_utc=v_now
        from supplied s
        where c.id=s.chunk_id and c.status='RUNNING'
          and c.lease_token=s.lease_token and c.fence_token=s.fence_token
        returning c.id,c.operation_id,c.status,c.phase,c.error_json
      )
      select v_result||coalesce(jsonb_agg(jsonb_build_object(
        'chunk_id',id,'operation_id',operation_id,'status',status,
        'phase',phase,'error',error_json)),'[]'::jsonb)
      into v_result
      from changed;
    end;
  end loop;

  return v_result;
end;
$function$;

revoke all on function private._invoice_dispatch_advance_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function private._invoice_dispatch_advance_batch(jsonb,timestamptz)
  to service_role;
