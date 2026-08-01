-- TEST rollback captured before restoring direct non-batch generation/issue dispatch.
begin;
CREATE OR REPLACE FUNCTION private._invoice_generation_advance_batch(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_manifest_claims jsonb;
  v_core_claims jsonb;
  v_rejected_claims jsonb;
  v_result jsonb := '[]'::jsonb;
begin
  if jsonb_typeof(coalesce(p_claims,'null'::jsonb)) is distinct from 'array'
     or jsonb_array_length(p_claims) < 1
     or jsonb_array_length(p_claims) > 100 then
    raise exception using
      errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;

  with claims as materialized (
    select value claim_json,ordinality
    from jsonb_array_elements(p_claims) with ordinality claim(value,ordinality)
  ),
  classified as materialized (
    select
      claim.*,
      exists (
        select 1
        from public.invoice_operation_chunks c
        join public.invoice_operations o on o.id=c.operation_id
        where c.id=case
          when pg_input_is_valid(coalesce(claim.claim_json->>'chunk_id',''),'uuid')
            then (claim.claim_json->>'chunk_id')::uuid
        end
          and c.chunk_type='GENERATION_GROUP'
          and o.entity_type='INVOICE_BATCH'
          and not c.is_manifest_member
          and c.phase in ('BUILD_MANIFEST','RELEASE_MANIFEST')
          and coalesce(c.payload_json->>'is_selection_expander','false')
            in ('true','t','1','yes','on')
      ) is_manifest,
      exists (
        select 1
        from public.invoice_operation_chunks c
        join public.invoice_operations o on o.id=c.operation_id
        where c.id=case
          when pg_input_is_valid(
            coalesce(claim.claim_json->>'chunk_id',''),
            'uuid'
          ) then (claim.claim_json->>'chunk_id')::uuid
        end
          and c.chunk_type='GENERATION_GROUP'
          and c.is_manifest_member
          and c.manifest_committed
          and o.manifest_committed
          and c.phase not in (
            'AWAITING_MANIFEST_COMMIT',
            'AWAITING_RELEASE'
          )
          and coalesce(
            c.payload_json->>'is_selection_expander',
            'false'
          ) not in ('true','t','1','yes','on')
      ) is_released
    from claims claim
  )
  select
    coalesce(jsonb_agg(claim_json order by ordinality)
      filter (where is_manifest),'[]'::jsonb),
    coalesce(jsonb_agg(claim_json order by ordinality)
      filter (where not is_manifest and is_released),'[]'::jsonb),
    coalesce(jsonb_agg(jsonb_build_object(
      'chunk_id',claim_json->>'chunk_id',
      'status','REJECTED',
      'phase','OWNERSHIP_REJECTED',
      'accepted',false,
      'code','MANIFEST_CLAIM_NOT_RELEASED'
    ) order by ordinality)
      filter (where not is_manifest and not is_released),'[]'::jsonb)
  into v_manifest_claims,v_core_claims,v_rejected_claims
  from classified;

  if jsonb_array_length(v_manifest_claims)>0 then
    v_result := v_result
      || private._invoice_batch_manifest_advance_v2(
        v_manifest_claims,
        'GENERATE',
        v_now
      );
  end if;

  if jsonb_array_length(v_core_claims)>0 then
    v_result := v_result
      || private._invoice_generation_advance_core_v8(
        v_core_claims,
        v_now
      );
  end if;

  return coalesce(v_result,'[]'::jsonb)
    || coalesce(v_rejected_claims,'[]'::jsonb);
end;
$function$;
alter function private._invoice_generation_advance_batch(jsonb,timestamp with time zone) owner to postgres;
revoke all on function private._invoice_generation_advance_batch(jsonb,timestamp with time zone) from public,anon,authenticated;
grant execute on function private._invoice_generation_advance_batch(jsonb,timestamp with time zone) to service_role;
CREATE OR REPLACE FUNCTION private._invoice_issue_advance_batch(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_manifest_claims jsonb;
  v_core_claims jsonb;
  v_rejected_claims jsonb;
  v_result jsonb := '[]'::jsonb;
begin
  if jsonb_typeof(coalesce(p_claims,'null'::jsonb)) is distinct from 'array'
     or jsonb_array_length(p_claims) < 1
     or jsonb_array_length(p_claims) > 100 then
    raise exception using
      errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;

  with claims as materialized (
    select value claim_json,ordinality
    from jsonb_array_elements(p_claims) with ordinality claim(value,ordinality)
  ),
  classified as materialized (
    select
      claim.*,
      exists (
        select 1
        from public.invoice_operation_chunks c
        join public.invoice_operations o on o.id=c.operation_id
        where c.id=case
          when pg_input_is_valid(coalesce(claim.claim_json->>'chunk_id',''),'uuid')
            then (claim.claim_json->>'chunk_id')::uuid
        end
          and c.chunk_type='ISSUE_INVOICE'
          and o.entity_type='INVOICE_BATCH'
          and not c.is_manifest_member
          and c.phase in ('BUILD_MANIFEST','RELEASE_MANIFEST')
          and coalesce(c.payload_json->>'is_selection_expander','false')
            in ('true','t','1','yes','on')
      ) is_manifest,
      exists (
        select 1
        from public.invoice_operation_chunks c
        join public.invoice_operations o on o.id=c.operation_id
        where c.id=case
          when pg_input_is_valid(
            coalesce(claim.claim_json->>'chunk_id',''),
            'uuid'
          ) then (claim.claim_json->>'chunk_id')::uuid
        end
          and c.chunk_type='ISSUE_INVOICE'
          and c.is_manifest_member
          and c.manifest_committed
          and o.manifest_committed
          and c.phase not in (
            'AWAITING_MANIFEST_COMMIT',
            'AWAITING_RELEASE'
          )
          and coalesce(
            c.payload_json->>'is_selection_expander',
            'false'
          ) not in ('true','t','1','yes','on')
      ) is_released
    from claims claim
  )
  select
    coalesce(jsonb_agg(claim_json order by ordinality)
      filter (where is_manifest),'[]'::jsonb),
    coalesce(jsonb_agg(claim_json order by ordinality)
      filter (where not is_manifest and is_released),'[]'::jsonb),
    coalesce(jsonb_agg(jsonb_build_object(
      'chunk_id',claim_json->>'chunk_id',
      'status','REJECTED',
      'phase','OWNERSHIP_REJECTED',
      'accepted',false,
      'code','MANIFEST_CLAIM_NOT_RELEASED'
    ) order by ordinality)
      filter (where not is_manifest and not is_released),'[]'::jsonb)
  into v_manifest_claims,v_core_claims,v_rejected_claims
  from classified;

  if jsonb_array_length(v_manifest_claims)>0 then
    v_result := v_result
      || private._invoice_batch_manifest_advance_v2(
        v_manifest_claims,
        'ISSUE',
        v_now
      );
  end if;

  if jsonb_array_length(v_core_claims)>0 then
    v_result := v_result
      || private._invoice_issue_advance_core_v8(
        v_core_claims,
        v_now
      );
  end if;

  return coalesce(v_result,'[]'::jsonb)
    || coalesce(v_rejected_claims,'[]'::jsonb);
end;
$function$;
alter function private._invoice_issue_advance_batch(jsonb,timestamp with time zone) owner to postgres;
revoke all on function private._invoice_issue_advance_batch(jsonb,timestamp with time zone) from public,anon,authenticated;
grant execute on function private._invoice_issue_advance_batch(jsonb,timestamp with time zone) to service_role;
commit;
