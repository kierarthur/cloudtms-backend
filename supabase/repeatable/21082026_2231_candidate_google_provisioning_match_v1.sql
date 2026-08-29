-- Read-only exact Candidate proof for disabled-first MyTMS Google provisioning.
-- This function does not create a Candidate, link, membership, invitation or
-- source identity.  The control plane remains the reservation/commit authority.

create or replace function private._candidate_google_mobile_normalize_v1(p_mobile text)
returns text
language sql
immutable
parallel safe
set search_path=''
as $function$
  select case
    when v like '0044%' then substring(v from 3)
    when v like '0%' then '44'||substring(v from 2)
    else v
  end
  from (select pg_catalog.regexp_replace(coalesce(p_mobile,''),'[^0-9]','','g') v) normalized;
$function$;

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
         pg_catalog.min(c.id::text)::uuid
    into v_count,v_candidate_id
  from public.candidates c
  where c.active is true
    and pg_catalog.upper(pg_catalog.btrim(coalesce(c.key_norm,'')))=v_candidate_code
    and pg_catalog.lower(pg_catalog.btrim(coalesce(c.last_name,'')))=v_surname
    and private._candidate_normalize_email(c.email)=v_email
    and private._candidate_google_mobile_normalize_v1(c.phone)=v_mobile;

  if v_count=0 then
    return pg_catalog.jsonb_build_object('ok',true,'match_state','NO_MATCH');
  elsif v_count<>1 then
    return pg_catalog.jsonb_build_object('ok',true,'match_state','AMBIGUOUS');
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'match_state','EXACT',
    'local_candidate_id',v_candidate_id,
    'google_source_identity_hmac',p_google_source_identity_hmac,
    'source_hmac_key_version',p_source_hmac_key_version
  );
end;
$function$;

revoke all on function private._candidate_google_mobile_normalize_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_google_provisioning_match_v1(
  jsonb,text,text,text,text,text,integer,text
) from public,anon,authenticated;
grant execute on function public.candidate_google_provisioning_match_v1(
  jsonb,text,text,text,text,text,integer,text
) to service_role;
