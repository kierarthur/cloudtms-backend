-- MyTMS Google Candidate identity linking v2.
-- Google remains the CID1 generator. The agency data plane proves one exact
-- surname/email/mobile match and can attach that CID only to a blank or
-- already-identical Candidate key. No browser or Google script can call these
-- service-only functions directly.

create or replace function public.candidate_google_provisioning_match_v1(
  p_internal_context jsonb,
  p_candidate_code text,
  p_surname text,
  p_email text,
  p_mobile text,
  p_google_source_identity_hmac text,
  p_source_hmac_key_version integer,
  p_correlation_id text
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_candidate_code text:=pg_catalog.upper(pg_catalog.btrim(p_candidate_code));
  v_surname text:=pg_catalog.lower(pg_catalog.btrim(p_surname));
  v_email text:=private._candidate_normalize_email(p_email);
  v_mobile text:=private._candidate_google_mobile_normalize_v1(p_mobile);
  v_count integer:=0;
  v_candidate_id uuid;
  v_existing_code text;
  v_code_state text;
begin
  if p_internal_context is null
     or pg_catalog.jsonb_typeof(p_internal_context) is distinct from 'object'
     or p_internal_context->>'route_context_verified' is distinct from 'true'
     or p_internal_context->>'audience' is distinct from 'GOOGLE_PROVISIONING_MATCH'
     or p_internal_context->>'environment' not in ('TEST','LIVE')
     or v_candidate_code !~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
     or pg_catalog.char_length(v_surname) not between 1 and 200
     or pg_catalog.char_length(v_email) not between 3 and 320
     or pg_catalog.char_length(v_mobile) not between 7 and 32
     or p_google_source_identity_hmac !~ '^[a-f0-9]{64}$'
     or p_source_hmac_key_version not between 1 and 2147483647
     or pg_catalog.char_length(pg_catalog.btrim(p_correlation_id)) not between 1 and 200 then
    raise exception using errcode='22023',message='GOOGLE_PROVISIONING_MATCH_INVALID';
  end if;

  select pg_catalog.count(*)::integer,
         pg_catalog.min(c.id::text)::uuid,
         pg_catalog.min(pg_catalog.upper(pg_catalog.btrim(coalesce(c.key_norm,''))))
    into v_count,v_candidate_id,v_existing_code
  from public.candidates c
  where c.active is true
    and pg_catalog.lower(pg_catalog.btrim(coalesce(c.last_name,'')))=v_surname
    and private._candidate_normalize_email(c.email)=v_email
    and private._candidate_google_mobile_normalize_v1(c.phone)=v_mobile;

  if v_count=0 then
    return pg_catalog.jsonb_build_object('ok',true,'match_state','NO_MATCH');
  elsif v_count<>1 then
    return pg_catalog.jsonb_build_object('ok',true,'match_state','AMBIGUOUS');
  end if;

  v_code_state:=case
    when nullif(v_existing_code,'') is null then 'UNASSIGNED'
    when v_existing_code=v_candidate_code then 'SAME'
    else 'CONFLICT'
  end;
  if v_code_state='CONFLICT' then
    return pg_catalog.jsonb_build_object(
      'ok',true,'match_state','AMBIGUOUS','reason_code','CID_CONFLICT'
    );
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'match_state','EXACT','candidate_code_state',v_code_state,
    'local_candidate_id',v_candidate_id,
    'google_source_identity_hmac',p_google_source_identity_hmac,
    'source_hmac_key_version',p_source_hmac_key_version
  );
end;
$function$;

create or replace function public.candidate_google_provisioning_attach_v1(
  p_internal_context jsonb,
  p_local_candidate_id uuid,
  p_candidate_code text,
  p_surname text,
  p_email text,
  p_mobile text,
  p_google_source_identity_hmac text,
  p_source_hmac_key_version integer,
  p_correlation_id text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_code text:=pg_catalog.upper(pg_catalog.btrim(p_candidate_code));
  v_surname text:=pg_catalog.lower(pg_catalog.btrim(p_surname));
  v_email text:=private._candidate_normalize_email(p_email);
  v_mobile text:=private._candidate_google_mobile_normalize_v1(p_mobile);
  v_candidate public.candidates%rowtype;
  v_existing text;
begin
  if p_internal_context is null
     or pg_catalog.jsonb_typeof(p_internal_context) is distinct from 'object'
     or p_internal_context->>'route_context_verified' is distinct from 'true'
     or p_internal_context->>'audience' is distinct from 'GOOGLE_PROVISIONING_ATTACH'
     or p_internal_context->>'environment' not in ('TEST','LIVE')
     or p_local_candidate_id is null
     or v_code !~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
     or pg_catalog.char_length(v_surname) not between 1 and 200
     or pg_catalog.char_length(v_email) not between 3 and 320
     or pg_catalog.char_length(v_mobile) not between 7 and 32
     or p_google_source_identity_hmac !~ '^[a-f0-9]{64}$'
     or p_source_hmac_key_version not between 1 and 2147483647
     or pg_catalog.char_length(pg_catalog.btrim(p_correlation_id)) not between 1 and 200 then
    raise exception using errcode='22023',message='GOOGLE_PROVISIONING_ATTACH_INVALID';
  end if;

  select * into strict v_candidate
  from public.candidates c where c.id=p_local_candidate_id for update;
  if not v_candidate.active
     or pg_catalog.lower(pg_catalog.btrim(coalesce(v_candidate.last_name,'')))<>v_surname
     or private._candidate_normalize_email(v_candidate.email)<>v_email
     or private._candidate_google_mobile_normalize_v1(v_candidate.phone)<>v_mobile then
    raise exception using errcode='40001',message='GOOGLE_PROVISIONING_IDENTITY_CHANGED';
  end if;
  v_existing:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_candidate.key_norm,'')));
  if nullif(v_existing,'') is null then
    update public.candidates c
       set key_norm=v_code,updated_at=p_now_utc
     where c.id=p_local_candidate_id
       and nullif(pg_catalog.btrim(coalesce(c.key_norm,'')),'') is null;
    if not found then
      raise exception using errcode='40001',message='GOOGLE_PROVISIONING_CID_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'state','ATTACHED','local_candidate_id',p_local_candidate_id,
      'candidate_code',v_code,'idempotent_replay',false
    );
  elsif v_existing=v_code then
    return pg_catalog.jsonb_build_object(
      'ok',true,'state','UNCHANGED','local_candidate_id',p_local_candidate_id,
      'candidate_code',v_code,'idempotent_replay',true
    );
  end if;
  raise exception using errcode='40001',message='GOOGLE_PROVISIONING_CID_CONFLICT';
exception when no_data_found then
  raise exception using errcode='40001',message='GOOGLE_PROVISIONING_IDENTITY_CHANGED';
end;
$function$;

alter function public.candidate_google_provisioning_match_v1(
  jsonb,text,text,text,text,text,integer,text
) owner to postgres;
alter function public.candidate_google_provisioning_attach_v1(
  jsonb,uuid,text,text,text,text,text,integer,text,timestamptz
) owner to postgres;

revoke all on function public.candidate_google_provisioning_match_v1(
  jsonb,text,text,text,text,text,integer,text
) from public,anon,authenticated;
revoke all on function public.candidate_google_provisioning_attach_v1(
  jsonb,uuid,text,text,text,text,text,integer,text,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_google_provisioning_match_v1(
  jsonb,text,text,text,text,text,integer,text
) to service_role;
grant execute on function public.candidate_google_provisioning_attach_v1(
  jsonb,uuid,text,text,text,text,text,integer,text,timestamptz
) to service_role;
