-- Rollback-contained first-use proof that two independent valid phone/login
-- families can coexist while stale and conflicting requests remain rejected
-- inside either exact family.

\set ON_ERROR_STOP on

begin;

do $verification$
declare
  v_now timestamptz:='2026-09-01 20:30:00+00'::timestamptz;
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_membership uuid:=pg_catalog.gen_random_uuid();
  v_account_hmac bytea:=pg_catalog.decode(repeat('1',64),'hex');
  v_legacy_session_hmac bytea:=pg_catalog.decode(repeat('2',64),'hex');
  v_family_a_hmac bytea:=pg_catalog.decode(repeat('3',64),'hex');
  v_family_a_old_session_hmac bytea:=pg_catalog.decode(repeat('4',64),'hex');
  v_family_a_new_session_hmac bytea:=pg_catalog.decode(repeat('5',64),'hex');
  v_family_a_conflict_hmac bytea:=pg_catalog.decode(repeat('6',64),'hex');
  v_family_b_hmac bytea:=pg_catalog.decode(repeat('7',64),'hex');
  v_family_b_session_hmac bytea:=pg_catalog.decode(repeat('8',64),'hex');
  v_context jsonb:=pg_catalog.jsonb_build_object(
    'route_context_verified',true,
    'audience','FEDERATED_MEMBERSHIP_LINK'
  );
  v_legacy jsonb;
  v_family_a_old jsonb;
  v_family_a_new jsonb;
  v_family_b jsonb;
  v_reused jsonb;
  v_rejected boolean:=false;
begin
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'federated-multi-device@example.invalid',true,'CID1-ABCDG');

  perform public.candidate_app_federated_membership_link_set_v1(
    v_context,'TEST',v_account_hmac,v_membership,1,v_candidate,
    'CID1-ABCDG','ACTIVE',v_now
  );

  -- A legacy projection is safely retired on first family-aware use.
  v_legacy:=public.candidate_app_federated_session_project_v1(
    'TEST',v_account_hmac,v_legacy_session_hmac,v_membership,1,v_candidate,2,1,
    v_now,v_now+interval '5 minutes'
  );
  v_family_a_old:=public.candidate_app_federated_session_project_v2(
    'TEST',v_account_hmac,v_family_a_old_session_hmac,v_membership,1,v_candidate,2,40,
    v_now+interval '1 second',v_now+interval '5 minutes 1 second',v_family_a_hmac
  );
  if (select status from public.candidate_app_sessions
      where id=(v_legacy->>'session_id')::uuid)<>'REVOKED' then
    raise exception using errcode='ZX999',message='FEDERATED_LEGACY_SESSION_NOT_RETIRED';
  end if;

  -- A separate phone begins at epoch one and remains concurrently active.
  v_family_b:=public.candidate_app_federated_session_project_v2(
    'TEST',v_account_hmac,v_family_b_session_hmac,v_membership,1,v_candidate,2,1,
    v_now+interval '2 seconds',v_now+interval '5 minutes 2 seconds',v_family_b_hmac
  );
  if (select count(*) from public.candidate_app_sessions
      where membership_id=v_membership and status='ACTIVE')<>2
     or not exists(
       select 1 from public.candidate_app_sessions
       where id=(v_family_a_old->>'session_id')::uuid and status='ACTIVE'
         and global_session_family_identity_hmac=v_family_a_hmac
     )
     or not exists(
       select 1 from public.candidate_app_sessions
       where id=(v_family_b->>'session_id')::uuid and status='ACTIVE'
         and session_epoch=1
         and global_session_family_identity_hmac=v_family_b_hmac
     ) then
    raise exception using errcode='ZX999',message='FEDERATED_INDEPENDENT_FAMILIES_DID_NOT_COEXIST';
  end if;

  -- Refreshing family A revokes only the older A row, never family B.
  v_family_a_new:=public.candidate_app_federated_session_project_v2(
    'TEST',v_account_hmac,v_family_a_new_session_hmac,v_membership,1,v_candidate,2,41,
    v_now+interval '3 seconds',v_now+interval '5 minutes 3 seconds',v_family_a_hmac
  );
  if (select status from public.candidate_app_sessions
      where id=(v_family_a_old->>'session_id')::uuid)<>'REVOKED'
     or (select status from public.candidate_app_sessions
         where id=(v_family_a_new->>'session_id')::uuid)<>'ACTIVE'
     or (select status from public.candidate_app_sessions
         where id=(v_family_b->>'session_id')::uuid)<>'ACTIVE'
     or (select count(*) from public.candidate_app_sessions
         where membership_id=v_membership and status='ACTIVE')<>2 then
    raise exception using errcode='ZX999',message='FEDERATED_FAMILY_REFRESH_CROSSED_DEVICE_BOUNDARY';
  end if;

  begin
    perform public.candidate_app_federated_session_project_v2(
      'TEST',v_account_hmac,v_family_a_old_session_hmac,v_membership,1,v_candidate,2,40,
      v_now+interval '4 seconds',v_now+interval '5 minutes 4 seconds',v_family_a_hmac
    );
  exception when sqlstate '28000' then
    if sqlerrm<>'CANDIDATE_FEDERATED_SESSION_STALE' then raise; end if;
    v_rejected:=true;
  end;
  if not v_rejected then
    raise exception using errcode='ZX999',message='FEDERATED_SAME_FAMILY_OLD_EPOCH_ACCEPTED';
  end if;

  v_rejected:=false;
  begin
    perform public.candidate_app_federated_session_project_v2(
      'TEST',v_account_hmac,v_family_a_conflict_hmac,v_membership,1,v_candidate,2,41,
      v_now+interval '5 seconds',v_now+interval '5 minutes 5 seconds',v_family_a_hmac
    );
  exception when sqlstate '28000' then
    if sqlerrm<>'CANDIDATE_FEDERATED_SESSION_STALE' then raise; end if;
    v_rejected:=true;
  end;
  if not v_rejected then
    raise exception using errcode='ZX999',message='FEDERATED_SAME_FAMILY_EPOCH_CONFLICT_ACCEPTED';
  end if;

  v_reused:=public.candidate_app_federated_session_project_v2(
    'TEST',v_account_hmac,v_family_a_new_session_hmac,v_membership,1,v_candidate,2,41,
    v_now+interval '6 seconds',v_now+interval '5 minutes 6 seconds',v_family_a_hmac
  );
  if v_reused->>'idempotent_reuse' is distinct from 'true'
     or (v_reused->>'session_id')::uuid<>(v_family_a_new->>'session_id')::uuid then
    raise exception using errcode='ZX999',message='FEDERATED_FAMILY_CURRENT_SESSION_REUSE_INVALID';
  end if;

  if not pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_app_federated_session_project_v2(text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz,bytea)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'public.candidate_app_federated_session_project_v2(text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz,bytea)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'public.candidate_app_federated_session_project_v2(text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz,bytea)',
       'EXECUTE'
     ) then
    raise exception using errcode='ZX999',message='FEDERATED_FAMILY_PROJECTION_ACL_INVALID';
  end if;

  if not exists(
    select 1
    from pg_catalog.pg_indexes
    where schemaname='public'
      and indexname='candidate_app_sessions_control_plane_family_epoch_uq'
      and indexdef ilike '%unique%global_session_family_identity_hmac%session_epoch%'
  ) then
    raise exception using errcode='ZX999',message='FEDERATED_FAMILY_UNIQUENESS_INDEX_MISSING';
  end if;
end;
$verification$;

rollback;

select pg_catalog.jsonb_build_object(
  'verification','PASS',
  'suite','candidate_federated_session_family_projection',
  'persistent_rows',0
) as candidate_federated_session_family_projection_proof;
