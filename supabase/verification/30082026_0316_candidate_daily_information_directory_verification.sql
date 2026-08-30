begin;

do $verification$
declare
  v_candidate uuid:=gen_random_uuid();
  v_context jsonb;
  v_current jsonb;
  v_saved jsonb;
  v_replay jsonb;
  v_candidate_read jsonb;
  v_version bigint;
  v_idempotency text:=gen_random_uuid()::text;
  v_actor_hmac text:=pg_catalog.encode(extensions.gen_random_bytes(32),'hex');
  v_home_version_before bigint;
  v_home_version_after bigint;
  v_hash text:=pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(gen_random_uuid()::text,'UTF8'),'sha256'),
    'hex'
  );
begin
  if pg_catalog.has_table_privilege('anon','public.candidate_daily_information_settings','SELECT')
     or pg_catalog.has_table_privilege('authenticated','public.candidate_daily_information_settings','SELECT')
     or pg_catalog.has_table_privilege('service_role','public.candidate_daily_information_settings','SELECT')
     or pg_catalog.has_table_privilege('anon','public.candidate_daily_information_versions','SELECT')
     or pg_catalog.has_table_privilege('authenticated','public.candidate_daily_information_versions','SELECT')
     or pg_catalog.has_table_privilege('service_role','public.candidate_daily_information_versions','SELECT')
  then
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: direct table access is open';
  end if;
  if pg_catalog.has_function_privilege(
       'anon','public.candidate_daily_information_settings_get_v1()','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','public.candidate_daily_information_settings_get_v1()','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'public.candidate_daily_information_candidate_v1(jsonb,text,timestamptz,text)',
       'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role','public.candidate_daily_information_settings_get_v1()','EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_daily_information_candidate_v1(jsonb,text,timestamptz,text)',
       'EXECUTE'
     )
  then
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: function ACL is wrong';
  end if;

  select candidate_home_announcement_version into v_home_version_before
  from public.settings_defaults where id=1;
  v_current:=public.candidate_daily_information_settings_get_v1();
  v_version:=(v_current->>'version')::bigint;
  if v_current->>'ok' is distinct from 'true'
     or pg_catalog.jsonb_typeof(v_current->'hospital_addresses') is distinct from 'array'
     or pg_catalog.jsonb_typeof(v_current->'accommodation_contacts') is distinct from 'array'
  then
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: initial settings read is invalid';
  end if;

  v_saved:=public.candidate_daily_information_settings_set_v1(
    v_version,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'hospital_name','  North General Hospital  ',
      'address',E'1 Health Street\r\nLondon\r\nN1 1AA',
      'telephone',' 020 7123 4567 ',
      'map_query',' North General Hospital, N1 1AA '
    )),
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'hospital_name','North General Hospital',
      'office_name',' Staff accommodation office ',
      'address','Residences Building, 2 Health Street',
      'telephone','020 7987 6543',
      'email',' HOUSING@EXAMPLE.INVALID ',
      'working_hours',E'Monday to Friday\r\n09:00–17:00'
    )),
    v_actor_hmac,v_idempotency,
    '2026-08-30 03:16:00+00'::timestamptz
  );
  if v_saved->>'ok' is distinct from 'true'
     or (v_saved->>'version')::bigint is distinct from v_version+1
     or v_saved#>>'{hospital_addresses,0,hospital_name}'
       is distinct from 'North General Hospital'
     or v_saved#>>'{hospital_addresses,0,address}'
       is distinct from E'1 Health Street\nLondon\nN1 1AA'
     or v_saved#>>'{accommodation_contacts,0,email}'
       is distinct from 'housing@example.invalid'
     or v_saved#>>'{accommodation_contacts,0,working_hours}'
       is distinct from E'Monday to Friday\n09:00–17:00'
  then
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: normalized save is wrong: %',v_saved;
  end if;

  v_replay:=public.candidate_daily_information_settings_set_v1(
    v_version,
    v_saved->'hospital_addresses',v_saved->'accommodation_contacts',
    v_actor_hmac,v_idempotency,
    '2026-08-30 03:17:00+00'::timestamptz
  );
  if v_replay->>'idempotent_replay' is distinct from 'true'
     or v_replay->>'version' is distinct from v_saved->>'version'
     or (select pg_catalog.count(*) from public.candidate_daily_information_versions
         where idempotency_key=v_idempotency)<>1
  then
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: idempotent replay is wrong';
  end if;

  begin
    perform public.candidate_daily_information_settings_set_v1(
      v_version,v_saved->'hospital_addresses',v_saved->'accommodation_contacts',
      pg_catalog.encode(extensions.gen_random_bytes(32),'hex'),v_idempotency,
      '2026-08-30 03:17:30+00'::timestamptz
    );
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: cross-actor replay was accepted';
  exception when sqlstate 'PT409' then
    null;
  end;

  begin
    perform public.candidate_daily_information_settings_set_v1(
      (v_saved->>'version')::bigint,
      pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'hospital_name','North General Hospital','address','1 Health Street'
        ),
        pg_catalog.jsonb_build_object(
          'hospital_name',' north general hospital ','address','Different address'
        )
      ),
      '[]'::jsonb,pg_catalog.encode(extensions.gen_random_bytes(32),'hex'),
      gen_random_uuid()::text,'2026-08-30 03:18:00+00'::timestamptz
    );
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: duplicate normalization was accepted';
  exception when sqlstate '22023' then
    null;
  end;

  begin
    perform public.candidate_daily_information_settings_set_v1(
      v_version,v_saved->'hospital_addresses',v_saved->'accommodation_contacts',
      pg_catalog.encode(extensions.gen_random_bytes(32),'hex'),gen_random_uuid()::text,
      '2026-08-30 03:18:00+00'::timestamptz
    );
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: stale version was accepted';
  exception when sqlstate 'PT409' then
    null;
  end;

  insert into public.candidates(id,email,display_name,active)
  values(
    v_candidate,
    'daily-information-'||v_candidate::text||'@example.invalid',
    'Daily information proof',true
  );
  insert into private.candidate_daily_entitlements(
    environment,candidate_id,enabled,reason,evidence_sha256
  ) values('TEST',v_candidate,true,'Daily information proof',v_hash);
  insert into private.candidate_daily_source_links(
    environment,candidate_id,source_system,canonicalization_version,link_group_id,
    identifier_hmac,hmac_key_version,state,evidence_sha256
  ) values(
    'TEST',v_candidate,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
    gen_random_uuid(),v_hash,1,'PRIMARY',v_hash
  );
  insert into private.candidate_daily_authority_scopes(
    environment,candidate_id,authority_mode,canonical_version,transition_in_progress
  ) values('TEST',v_candidate,'SUPABASE_PRIMARY',1,false);
  update public.settings_defaults
  set candidate_app_feature_flags_json=candidate_app_feature_flags_json
    ||pg_catalog.jsonb_build_object('candidate_daily_enabled',true)
  where id=1;
  v_context:=pg_catalog.jsonb_build_object(
    'policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate
  );

  v_candidate_read:=public.candidate_daily_information_candidate_v1(
    v_context,'hospital-addresses','2026-08-30 03:19:00+00'::timestamptz,
    '01K2ABCDEFGHJKMNPQRSTVWXYZ'
  );
  if v_candidate_read->>'kind' is distinct from 'hospital-addresses'
     or v_candidate_read->>'title' is distinct from 'Hospital addresses'
     or v_candidate_read->>'schema_version' is distinct from '1'
     or v_candidate_read#>>'{entries,0,hospital_name}'
       is distinct from 'North General Hospital'
     or v_candidate_read#>>'{appInfo,version}'
       is distinct from 'CLOUDTMS_DIRECTORY_V1'
  then
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: Candidate projection is wrong: %',
      v_candidate_read;
  end if;
  v_candidate_read:=public.candidate_daily_information_candidate_v1(
    v_context,'accommodation-contacts','2026-08-30 03:19:00+00'::timestamptz,
    '01K2ABCDEFGHJKMNPQRSTVWXYZ'
  );
  if v_candidate_read->>'kind' is distinct from 'accommodation-contacts'
     or v_candidate_read#>>'{entries,0,office_name}'
       is distinct from 'Staff accommodation office'
  then
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: accommodation projection is wrong';
  end if;

  select candidate_home_announcement_version into v_home_version_after
  from public.settings_defaults where id=1;
  if v_home_version_after is distinct from v_home_version_before then
    raise exception 'CANDIDATE_DAILY_INFORMATION_PROOF: unrelated MyTMS setting changed';
  end if;
end;
$verification$;

rollback;
