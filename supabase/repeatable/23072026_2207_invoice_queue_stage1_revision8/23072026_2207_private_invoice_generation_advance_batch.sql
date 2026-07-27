-- CloudTMS Invoice Async V8/V2 Generate worker authority.
create or replace function private._invoice_generation_advance_batch(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_manifest_claims jsonb;
  v_core_claims jsonb;
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
        where c.id=case
          when pg_input_is_valid(coalesce(claim.claim_json->>'chunk_id',''),'uuid')
            then (claim.claim_json->>'chunk_id')::uuid
        end
          and c.chunk_type='GENERATION_GROUP'
          and c.phase in ('BUILD_MANIFEST','RELEASE_MANIFEST')
          and coalesce(c.payload_json->>'is_selection_expander','false')
            in ('true','t','1','yes','on')
      ) is_manifest
    from claims claim
  )
  select
    coalesce(jsonb_agg(claim_json order by ordinality)
      filter (where is_manifest),'[]'::jsonb),
    coalesce(jsonb_agg(claim_json order by ordinality)
      filter (where not is_manifest),'[]'::jsonb)
  into v_manifest_claims,v_core_claims
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

  return coalesce(v_result,'[]'::jsonb);
end;
$function$;

alter function private._invoice_generation_advance_batch(
  jsonb,timestamptz
) owner to postgres;
revoke all on function private._invoice_generation_advance_batch(
  jsonb,timestamptz
) from public,anon,authenticated;
grant execute on function private._invoice_generation_advance_batch(
  jsonb,timestamptz
) to service_role;
