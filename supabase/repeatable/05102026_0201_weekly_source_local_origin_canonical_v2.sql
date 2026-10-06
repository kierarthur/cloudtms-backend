-- Repeatable CloudTMS function/view authority: weekly_source_local_origin_canonical_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Encoding only: this validates the CLOSED local variant, not its authority.
-- The actual receipt/generation/whole-vector/before/qualification verifier is
-- separately required at proposal/publication. No nullable-Final catch-all.
create or replace function private.weekly_source_local_origin_canonical_v2(p_origin jsonb)
returns jsonb language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_before jsonb;
  v_key text;
  v_keys text[];
  v_scalar_kind text;
begin
  perform private.weekly_source_publication_require_keys_v1(p_origin,
    array['origin_kind','publication_request_id','generation_id','request_sha256',
      'source_qualification_digest','policy_fingerprint','before_origin','before_inventory_digest'],
    'financial_request.source_revision.local');
  if p_origin->>'origin_kind' is distinct from 'PROTECTED_LOCAL_DECISION_V1' then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID' using errcode='22023',
      detail='{"field":"financial_request.source_revision.local.origin_kind","reason":"UNKNOWN_LOCAL_ORIGIN"}';
  end if;
  v_before:=p_origin->'before_origin';
  case v_before->>'kind'
    when 'INITIAL_AUTHORISED_TSFIN_V1' then
      v_keys:=array['kind','root_authorisation_id','financial_snapshot_id','authorisation_generation',
        'root_timesheet_id','root_version','authorised_row_signature','financial_snapshot_digest',
        'inventory_digest','entitlement_digest'];
    when 'COMMITTED_SOURCE_HEAD_V1' then
      v_keys:=array['kind','head_id','head_revision','decision_bundle_id','bundle_revision',
        'root_authorisation_id','authorisation_generation','root_timesheet_id','root_version',
        'source_generation_digest','inventory_digest','entitlement_digest'];
    else
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID' using errcode='22023',
        detail='{"field":"financial_request.source_revision.local.before_origin.kind","reason":"UNQUALIFIED_BEFORE_ORIGIN"}';
  end case;
  perform private.weekly_source_publication_require_keys_v1(v_before,v_keys,
    'financial_request.source_revision.local.before_origin');
  foreach v_key in array array['root_authorisation_id','root_timesheet_id'] loop
    perform private.weekly_source_publication_scalar_v1(v_before->v_key,'local.before.'||v_key,'UUID');
  end loop;
  foreach v_key in array array['inventory_digest','entitlement_digest'] loop
    perform private.weekly_source_publication_scalar_v1(v_before->v_key,'local.before.'||v_key,'HEX32');
  end loop;
  perform private.weekly_source_publication_scalar_v1(v_before->'authorisation_generation',
    'local.before.authorisation_generation','INT');
  if (v_before->>'authorisation_generation')::numeric<1
     or coalesce(v_before->>'root_version','') !~ '^[1-9][0-9]*$'
     or jsonb_typeof(v_before->'root_version') is distinct from 'string' then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID' using errcode='22023',
      detail='{"field":"financial_request.source_revision.local.before_origin","reason":"INVALID_BEFORE_VERSION"}';
  end if;
  if v_before->>'kind'='INITIAL_AUTHORISED_TSFIN_V1' then
    perform private.weekly_source_publication_scalar_v1(v_before->'financial_snapshot_id','local.before.financial_snapshot_id','UUID');
    perform private.weekly_source_publication_scalar_v1(v_before->'authorised_row_signature','local.before.authorised_row_signature','TEXT');
    perform private.weekly_source_publication_scalar_v1(v_before->'financial_snapshot_digest','local.before.financial_snapshot_digest','HEX32');
  else
    foreach v_key in array array['head_id','decision_bundle_id'] loop
      perform private.weekly_source_publication_scalar_v1(v_before->v_key,'local.before.'||v_key,'UUID');
    end loop;
    foreach v_key in array array['head_revision','bundle_revision'] loop
      if coalesce(v_before->>v_key,'') !~ '^[1-9][0-9]*$'
         or jsonb_typeof(v_before->v_key) is distinct from 'string' then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID' using errcode='22023',
          detail=jsonb_build_object('field','local.before.'||v_key,'reason','INVALID_BEFORE_VERSION')::text;
      end if;
    end loop;
    perform private.weekly_source_publication_scalar_v1(v_before->'source_generation_digest','local.before.source_generation_digest','HEX32');
    -- The Local-only witness is a bounded reference to the actual sealed HEAD.
    -- Its source-generation digest is the referenced HEAD's full-origin digest,
    -- not a hash of this flat witness. The factual context qualifies the actual
    -- complete I1 inventory before producing it; this codec grants no authority.
    -- Never copy prior Local/Final source JSON into each new Local origin.
  end if;
  -- A sealed witness is not silently rewritten. Genuine constructors already
  -- emit canonical UUIDs/lowercase hashes; require that exact form so
  -- canonical(canonical(input)) remains identical and valid on exact replay.
  foreach v_key in array array['root_authorisation_id','root_timesheet_id','financial_snapshot_id',
      'head_id','decision_bundle_id','inventory_digest','entitlement_digest',
      'financial_snapshot_digest','source_generation_digest'] loop
    if not (v_before ? v_key) then continue; end if;
    v_scalar_kind:=case when v_key like '%digest' then 'HEX32' else 'UUID' end;
    if private.weekly_source_publication_scalar_v1(v_before->v_key,
        'local.before.'||v_key,v_scalar_kind) is distinct from v_before->v_key then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID' using errcode='22023',
        detail=jsonb_build_object('field','local.before.'||v_key,'reason','NONCANONICAL_SEALED_ORIGIN')::text;
    end if;
  end loop;
  if p_origin->>'before_inventory_digest' is distinct from v_before->>'inventory_digest' then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID' using errcode='22023',
      detail='{"field":"local.before_inventory_digest","reason":"BEFORE_INVENTORY_MISMATCH"}';
  end if;
  return jsonb_build_object('origin_kind','PROTECTED_LOCAL_DECISION_V1',
    'publication_request_id',private.weekly_source_publication_scalar_v1(p_origin->'publication_request_id','local.publication_request_id','UUID'),
    'generation_id',private.weekly_source_publication_scalar_v1(p_origin->'generation_id','local.generation_id','UUID'),
    'request_sha256',private.weekly_source_publication_scalar_v1(p_origin->'request_sha256','local.request_sha256','HEX32'),
    'source_qualification_digest',private.weekly_source_publication_scalar_v1(p_origin->'source_qualification_digest','local.source_qualification_digest','HEX32'),
    'policy_fingerprint',private.weekly_source_publication_scalar_v1(p_origin->'policy_fingerprint','local.policy_fingerprint','HEX32'),
    'before_origin',v_before,
    'before_inventory_digest',private.weekly_source_publication_scalar_v1(p_origin->'before_inventory_digest','local.before_inventory_digest','HEX32'));
end;
$function$;
alter function private.weekly_source_local_origin_canonical_v2(jsonb) owner to postgres;
revoke all on function private.weekly_source_local_origin_canonical_v2(jsonb)
  from public,anon,authenticated,service_role;

commit;
