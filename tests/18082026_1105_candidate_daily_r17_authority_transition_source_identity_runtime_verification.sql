begin;

create or replace function pg_temp.r17_context()
returns jsonb language sql immutable as $function$
  select jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,
    'authority_mode_compatible',true,'transition_ready',true,
    'actor_user_id','00000000-0000-4000-8000-000000017090'
  )
$function$;

create or replace function pg_temp.r17_item(
  p_candidate_id uuid,
  p_identifier_hmac text default null,
  p_key_version jsonb default '1'::jsonb,
  p_disposition text default 'DRAINED'
)
returns jsonb language sql immutable as $function$
  select jsonb_strip_nulls(jsonb_build_object(
    'candidate_id',p_candidate_id,
    'expected_authority_mode','GOOGLE_PRIMARY',
    'expected_canonical_version',0,
    'expected_entitlement_enabled',false,
    'new_authority_mode','GOOGLE_PRIMARY',
    'entitlement_enabled',false,
    'in_flight_disposition',p_disposition,
    'source_link',case when p_identifier_hmac is null then null else jsonb_build_object(
      'identifier_hmac',p_identifier_hmac,
      'hmac_key_version',p_key_version,
      'state','PRIMARY'
    ) end
  ))
$function$;

insert into public.candidates(id,email,display_name,first_name,last_name,active)
select id,id::text||'@r17.invalid','R17 Authority Fixture','R17','Authority',true
from unnest(array[
  '00000000-0000-4000-8000-000000017001'::uuid,
  '00000000-0000-4000-8000-000000017002'::uuid,
  '00000000-0000-4000-8000-000000017003'::uuid,
  '00000000-0000-4000-8000-000000017004'::uuid,
  '00000000-0000-4000-8000-000000017005'::uuid,
  '00000000-0000-4000-8000-000000017006'::uuid,
  '00000000-0000-4000-8000-000000017007'::uuid
]) id;

insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode,canonical_version)
select 'TEST',id,'GOOGLE_PRIMARY',0
from unnest(array[
  '00000000-0000-4000-8000-000000017002'::uuid,
  '00000000-0000-4000-8000-000000017003'::uuid,
  '00000000-0000-4000-8000-000000017004'::uuid,
  '00000000-0000-4000-8000-000000017005'::uuid,
  '00000000-0000-4000-8000-000000017006'::uuid,
  '00000000-0000-4000-8000-000000017007'::uuid
]) id;

insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
select 'TEST',id,false,'R17 authority fixture',repeat('e',64)
from unnest(array[
  '00000000-0000-4000-8000-000000017002'::uuid,
  '00000000-0000-4000-8000-000000017003'::uuid,
  '00000000-0000-4000-8000-000000017004'::uuid,
  '00000000-0000-4000-8000-000000017005'::uuid,
  '00000000-0000-4000-8000-000000017006'::uuid,
  '00000000-0000-4000-8000-000000017007'::uuid
]) id;

insert into private.candidate_daily_source_links(
  environment,candidate_id,source_system,canonicalization_version,link_group_id,
  identifier_hmac,hmac_key_version,state,evidence_sha256
) values(
  'TEST','00000000-0000-4000-8000-000000017001','GOOGLE_CREDENTIALLY_PUBLIC_ID',
  'SOURCE_IDENTITY_V1','00000000-0000-4000-8000-000000017101',repeat('a',64),1,
  'PRIMARY',repeat('f',64)
);

do $test$
declare
  v_result jsonb;
  v_replay jsonb;
begin
  -- A source identity already owned by another Candidate is an indexed result,
  -- and exact replay returns the same durable receipt rather than aborting.
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.r17_context(),'00000000-0000-4000-8000-000000017201',
    'candidate-r17-single-conflict-0001',
    jsonb_build_array(pg_temp.r17_item(
      '00000000-0000-4000-8000-000000017002',repeat('a',64))),
    '00000000-0000-4000-8000-000000017091','R17 single conflict',repeat('1',64),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ'
  );
  if v_result#>>'{outcomes,0,status}'<>'REJECTED'
     or v_result#>>'{outcomes,0,error_code}'<>'IDENTITY_LINK_CONFLICT' then
    raise exception 'R17 single conflict was not contained: %',v_result;
  end if;
  v_replay:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.r17_context(),'00000000-0000-4000-8000-000000017201',
    'candidate-r17-single-conflict-0001',
    jsonb_build_array(pg_temp.r17_item(
      '00000000-0000-4000-8000-000000017002',repeat('a',64))),
    '00000000-0000-4000-8000-000000017091','R17 single conflict',repeat('1',64),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ'
  );
  if coalesce((v_replay->>'_idempotent_replay')::boolean,false) is not true
     or v_replay->>'batch_receipt_id'<>v_result->>'batch_receipt_id' then
    raise exception 'R17 conflict replay was not durable: %',v_replay;
  end if;

  -- A valid sibling before a conflict still commits and remains durable.
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.r17_context(),'00000000-0000-4000-8000-000000017202',
    'candidate-r17-valid-then-conflict-0001',
    jsonb_build_array(
      pg_temp.r17_item('00000000-0000-4000-8000-000000017003',repeat('b',64)),
      pg_temp.r17_item('00000000-0000-4000-8000-000000017004',repeat('a',64))
    ),
    '00000000-0000-4000-8000-000000017091','R17 mixed order A',repeat('2',64),
    '01K2ABCDEFGHJKMNPQRSTVWXY1'
  );
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED'
     or v_result#>>'{outcomes,1,error_code}'<>'IDENTITY_LINK_CONFLICT' then
    raise exception 'R17 valid-then-conflict batch was not isolated: %',v_result;
  end if;

  -- A conflict before a valid sibling cannot erase the valid outcome.
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.r17_context(),'00000000-0000-4000-8000-000000017203',
    'candidate-r17-conflict-then-valid-0001',
    jsonb_build_array(
      pg_temp.r17_item('00000000-0000-4000-8000-000000017004',repeat('a',64)),
      pg_temp.r17_item('00000000-0000-4000-8000-000000017005',repeat('c',64))
    ),
    '00000000-0000-4000-8000-000000017091','R17 mixed order B',repeat('3',64),
    '01K2ABCDEFGHJKMNPQRSTVWXY2'
  );
  if v_result#>>'{outcomes,0,error_code}'<>'IDENTITY_LINK_CONFLICT'
     or v_result#>>'{outcomes,1,status}'<>'COMMITTED' then
    raise exception 'R17 conflict-then-valid batch was not isolated: %',v_result;
  end if;

  -- Malformed identities bypass the prelock cast path and are rejected per item.
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.r17_context(),'00000000-0000-4000-8000-000000017204',
    'candidate-r17-malformed-link-0001',
    jsonb_build_array(pg_temp.r17_item(
      '00000000-0000-4000-8000-000000017006','not-a-hmac','2147483648'::jsonb)),
    '00000000-0000-4000-8000-000000017091','R17 malformed source',repeat('4',64),
    '01K2ABCDEFGHJKMNPQRSTVWXY3'
  );
  if v_result#>>'{outcomes,0,error_code}'<>'VALIDATION_FAILED' then
    raise exception 'R17 malformed source was not indexed validation: %',v_result;
  end if;

  -- Existing no-source, no-change transitions retain their prior contract.
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.r17_context(),'00000000-0000-4000-8000-000000017205',
    'candidate-r17-no-source-regression-0001',
    jsonb_build_array(pg_temp.r17_item(
      '00000000-0000-4000-8000-000000017007',null,'1'::jsonb,'NONE')),
    '00000000-0000-4000-8000-000000017091','R17 no-source regression',repeat('5',64),
    '01K2ABCDEFGHJKMNPQRSTVWXY4'
  );
  if v_result#>>'{outcomes,0,status}'<>'NO_CHANGE' then
    raise exception 'R17 no-source transition regressed: %',v_result;
  end if;
end
$test$;

do $acl$
begin
  if has_function_privilege('public','public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)','EXECUTE') then
    raise exception 'PUBLIC retained Candidate Daily authority-transition execute';
  end if;
  if exists(select 1 from pg_roles where rolname='anon') and
     has_function_privilege('anon','public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)','EXECUTE') then
    raise exception 'anon retained Candidate Daily authority-transition execute';
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated') and
     has_function_privilege('authenticated','public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)','EXECUTE') then
    raise exception 'authenticated retained Candidate Daily authority-transition execute';
  end if;
  if exists(select 1 from pg_roles where rolname='service_role') and not
     has_function_privilege('service_role','public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)','EXECUTE') then
    raise exception 'service_role lacks Candidate Daily authority-transition execute';
  end if;
end
$acl$;

rollback;
