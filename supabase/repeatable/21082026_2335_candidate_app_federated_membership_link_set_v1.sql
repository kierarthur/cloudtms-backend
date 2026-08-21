-- Service-only agency-local membership-link adoption for a centrally verified
-- route.  This creates no Candidate or business record and copies no password.

create or replace function public.candidate_app_federated_membership_link_set_v1(
  p_internal_context jsonb,
  p_environment text,
  p_global_account_identity_hmac bytea,
  p_membership_id uuid,
  p_membership_generation integer,
  p_candidate_id uuid,
  p_candidate_code text,
  p_target_state text,
  p_now_utc timestamptz
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_environment text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_environment,'')));
  v_target_state text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_target_state,'')));
  v_candidate public.candidates%rowtype;
  v_email text;
  v_account public.candidate_app_accounts%rowtype;
  v_link public.candidate_app_global_membership_links%rowtype;
begin
  if pg_catalog.jsonb_typeof(p_internal_context) is distinct from 'object'
     or p_internal_context->>'route_context_verified' is distinct from 'true'
     or p_internal_context->>'audience' is distinct from 'FEDERATED_MEMBERSHIP_LINK'
     or v_environment not in ('TEST','LIVE')
     or p_membership_id is null or p_candidate_id is null
     or coalesce(p_membership_generation,0)<1
     or coalesce(pg_catalog.octet_length(p_global_account_identity_hmac),0)<>32
     or v_target_state not in ('PENDING','ACTIVE','DISABLED','REVOKED')
     or p_now_utc is null
     or (p_candidate_code is not null
       and pg_catalog.upper(pg_catalog.btrim(p_candidate_code)) !~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$') then
    raise exception using errcode='22023',message='CANDIDATE_FEDERATED_LINK_INVALID';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('candidate-federated-membership:'||p_membership_id::text,0)
  );

  select * into strict v_candidate
  from public.candidates c
  where c.id=p_candidate_id and c.active is true
  for share;
  v_email:=pg_catalog.lower(pg_catalog.btrim(coalesce(v_candidate.email,'')));
  if v_email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
     or (p_candidate_code is not null
       and pg_catalog.upper(pg_catalog.btrim(coalesce(v_candidate.key_norm,'')))
         <>pg_catalog.upper(pg_catalog.btrim(p_candidate_code))) then
    raise exception using errcode='28000',message='CANDIDATE_FEDERATED_CANDIDATE_INVALID';
  end if;

  select * into v_link
  from public.candidate_app_global_membership_links l
  where l.membership_id=p_membership_id
  for update;
  if found then
    if v_link.global_account_identity_hmac<>p_global_account_identity_hmac
       or v_link.candidate_id<>p_candidate_id
       or p_membership_generation<v_link.membership_generation
       or (v_link.candidate_code is not null and p_candidate_code is not null
         and v_link.candidate_code<>pg_catalog.upper(pg_catalog.btrim(p_candidate_code))) then
      raise exception using errcode='28000',message='CANDIDATE_FEDERATED_MEMBERSHIP_STALE';
    end if;
    update public.candidate_app_global_membership_links
    set membership_generation=p_membership_generation,
        candidate_code=coalesce(
          candidate_code,pg_catalog.upper(pg_catalog.btrim(p_candidate_code))
        ),
        state=v_target_state,
        updated_at_utc=p_now_utc,
        revoked_at_utc=case when v_target_state='REVOKED' then p_now_utc else null end,
        revoke_reason=case when v_target_state='REVOKED' then 'CONTROL_PLANE_REVOKED' else null end
    where membership_id=p_membership_id
    returning * into v_link;
    if v_target_state in ('DISABLED','REVOKED') then
      update public.candidate_app_sessions
      set status='REVOKED',revoked_at_utc=p_now_utc,
          revoke_reason='FEDERATED_MEMBERSHIP_'||v_target_state,updated_at_utc=p_now_utc
      where auth_source='CONTROL_PLANE' and membership_id=p_membership_id and status='ACTIVE';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','UPDATED','state',v_link.state,
      'membership_generation',v_link.membership_generation,'internal_only',true
    );
  end if;

  if v_target_state in ('DISABLED','REVOKED') then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','UNCHANGED','state',v_target_state,
      'membership_generation',p_membership_generation,'internal_only',true
    );
  end if;

  select * into v_account
  from public.candidate_app_accounts a
  where a.environment=v_environment and a.email_normalized=v_email
  for update;
  if found then
    if v_account.status in ('LOCKED','DISABLED') then
      raise exception using errcode='28000',message='CANDIDATE_FEDERATED_ACCOUNT_REVIEW_REQUIRED';
    end if;
    if v_account.status='SETUP_REQUIRED' then
      update public.candidate_app_accounts
      set status='ACTIVE',updated_at_utc=p_now_utc
      where id=v_account.id
      returning * into v_account;
    end if;
  else
    insert into public.candidate_app_accounts(
      environment,email_normalized,status,created_at_utc,updated_at_utc
    ) values(v_environment,v_email,'ACTIVE',p_now_utc,p_now_utc)
    returning * into v_account;
  end if;

  insert into public.candidate_app_global_membership_links(
    membership_id,global_account_identity_hmac,account_id,candidate_id,candidate_code,
    membership_generation,state,linked_at_utc,updated_at_utc
  ) values(
    p_membership_id,p_global_account_identity_hmac,v_account.id,p_candidate_id,
    case when p_candidate_code is null then null else pg_catalog.upper(pg_catalog.btrim(p_candidate_code)) end,
    p_membership_generation,v_target_state,p_now_utc,p_now_utc
  ) returning * into v_link;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','LINKED','state',v_link.state,
    'membership_generation',v_link.membership_generation,'internal_only',true
  );
exception
  when no_data_found then
    raise exception using errcode='28000',message='CANDIDATE_FEDERATED_CANDIDATE_INVALID';
  when unique_violation then
    raise exception using errcode='28000',message='CANDIDATE_FEDERATED_LINK_CONFLICT';
end;
$function$;

revoke all on function public.candidate_app_federated_membership_link_set_v1(
  jsonb,text,bytea,uuid,integer,uuid,text,text,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_app_federated_membership_link_set_v1(
  jsonb,text,bytea,uuid,integer,uuid,text,text,timestamptz
) to service_role;

comment on function public.candidate_app_federated_membership_link_set_v1(
  jsonb,text,bytea,uuid,integer,uuid,text,text,timestamptz
) is 'Adopts or advances one exact centrally verified membership into the agency-local compatibility authority without copying a password or creating Candidate business data.';
