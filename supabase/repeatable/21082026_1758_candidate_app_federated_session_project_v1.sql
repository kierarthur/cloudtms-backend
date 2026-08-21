create or replace function public.candidate_app_federated_session_project_v1(
  p_environment text,
  p_global_account_identity_hmac bytea,
  p_global_session_identity_hmac bytea,
  p_membership_id uuid,
  p_membership_generation integer,
  p_candidate_id uuid,
  p_route_version integer,
  p_session_epoch bigint,
  p_now_utc timestamptz,
  p_expires_at_utc timestamptz
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_environment text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_environment,'')));
  v_link public.candidate_app_global_membership_links%rowtype;
  v_account public.candidate_app_accounts%rowtype;
  v_existing public.candidate_app_sessions%rowtype;
  v_session_id uuid:=pg_catalog.gen_random_uuid();
  v_expiry timestamptz;
begin
  if v_environment not in ('TEST','LIVE')
     or p_membership_id is null or p_candidate_id is null
     or p_membership_generation<1 or p_route_version<1 or p_session_epoch<1
     or coalesce(pg_catalog.octet_length(p_global_account_identity_hmac),0)<>32
     or coalesce(pg_catalog.octet_length(p_global_session_identity_hmac),0)<>32
     or p_now_utc is null or p_expires_at_utc is null
     or p_expires_at_utc<=p_now_utc
     or p_expires_at_utc>p_now_utc+interval '5 minutes' then
    raise exception 'CANDIDATE_FEDERATED_CONTEXT_INVALID' using errcode='22023';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('candidate-federated-membership:'||p_membership_id::text,0)
  );

  select * into strict v_link
  from public.candidate_app_global_membership_links
  where membership_id=p_membership_id
  for update;

  if v_link.state<>'ACTIVE'
     or v_link.global_account_identity_hmac<>p_global_account_identity_hmac
     or v_link.membership_generation<>p_membership_generation
     or v_link.candidate_id<>p_candidate_id then
    raise exception 'CANDIDATE_FEDERATED_MEMBERSHIP_STALE' using errcode='28000';
  end if;

  select * into strict v_account
  from public.candidate_app_accounts
  where id=v_link.account_id
  for share;
  if v_account.environment<>v_environment or v_account.status<>'ACTIVE'
     or not exists(
       select 1 from public.candidates c
       where c.id=v_link.candidate_id and c.active=true
         and pg_catalog.lower(pg_catalog.btrim(coalesce(c.email,'')))=v_account.email_normalized
     ) then
    raise exception 'CANDIDATE_AUTHORITY_REVOKED' using errcode='28000';
  end if;

  update public.candidate_app_sessions
  set status='REVOKED',revoked_at_utc=p_now_utc,revoke_reason='FEDERATED_CONTEXT_STALE',updated_at_utc=p_now_utc
  where auth_source='CONTROL_PLANE' and membership_id=p_membership_id and status='ACTIVE'
    and (membership_generation<>p_membership_generation
      or route_version<>p_route_version
      or session_epoch<>p_session_epoch
      or global_account_identity_hmac<>p_global_account_identity_hmac
      or global_session_identity_hmac<>p_global_session_identity_hmac
      or selected_candidate_id<>p_candidate_id
      or expires_at_utc<=p_now_utc);

  select * into v_existing
  from public.candidate_app_sessions
  where auth_source='CONTROL_PLANE' and membership_id=p_membership_id
    and membership_generation=p_membership_generation
    and route_version=p_route_version and session_epoch=p_session_epoch
    and global_account_identity_hmac=p_global_account_identity_hmac
    and global_session_identity_hmac=p_global_session_identity_hmac
    and selected_candidate_id=p_candidate_id and status='ACTIVE'
    and expires_at_utc>p_now_utc and absolute_expires_at_utc>p_now_utc
  for update;
  if found then
    update public.candidate_app_sessions
    set last_used_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_existing.id;
    return pg_catalog.jsonb_build_object(
      'ok',true,'idempotent_reuse',true,'session_id',v_existing.id,
      'account_id',v_existing.account_id,'selected_candidate_id',v_existing.selected_candidate_id,
      'environment',v_existing.environment,'rotation',v_existing.rotation,
      'issued_at_utc',v_existing.issued_at_utc,'expires_at_utc',v_existing.expires_at_utc,
      'absolute_expires_at_utc',v_existing.absolute_expires_at_utc,
      'membership_id',v_existing.membership_id,'membership_generation',v_existing.membership_generation,
      'route_version',v_existing.route_version,'session_epoch',v_existing.session_epoch,
      'internal_only',true
    );
  end if;

  v_expiry:=least(p_expires_at_utc,p_now_utc+interval '5 minutes');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    token_family_id,rotation,issued_at_utc,expires_at_utc,absolute_expires_at_utc,
    last_used_at_utc,auth_source,global_account_identity_hmac,global_session_identity_hmac,
    membership_id,membership_generation,route_version,session_epoch,created_at_utc,updated_at_utc
  ) values (
    v_session_id,v_link.account_id,v_environment,p_candidate_id,'ACTIVE',
    extensions.digest(pg_catalog.gen_random_uuid()::text,'sha256'),
    pg_catalog.gen_random_uuid(),0,p_now_utc,v_expiry,v_expiry,p_now_utc,'CONTROL_PLANE',
    p_global_account_identity_hmac,p_global_session_identity_hmac,p_membership_id,
    p_membership_generation,p_route_version,p_session_epoch,p_now_utc,p_now_utc
  ) returning * into v_existing;

  return pg_catalog.jsonb_build_object(
    'ok',true,'idempotent_reuse',false,'session_id',v_existing.id,
    'account_id',v_existing.account_id,'selected_candidate_id',v_existing.selected_candidate_id,
    'environment',v_existing.environment,'rotation',v_existing.rotation,
    'issued_at_utc',v_existing.issued_at_utc,'expires_at_utc',v_existing.expires_at_utc,
    'absolute_expires_at_utc',v_existing.absolute_expires_at_utc,
    'membership_id',v_existing.membership_id,'membership_generation',v_existing.membership_generation,
    'route_version',v_existing.route_version,'session_epoch',v_existing.session_epoch,
    'internal_only',true
  );
exception when no_data_found then
  raise exception 'CANDIDATE_FEDERATED_MEMBERSHIP_INVALID' using errcode='28000';
end;
$function$;

revoke all on function public.candidate_app_federated_session_project_v1(
  text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_app_federated_session_project_v1(
  text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz
) to service_role;

comment on function public.candidate_app_federated_session_project_v1(
  text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz
) is 'Projects a verified central route context into one short-lived agency-local Candidate session without issuing a browser refresh credential.';
