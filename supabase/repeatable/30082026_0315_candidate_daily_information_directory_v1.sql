begin;

create or replace function private._candidate_daily_information_text_v1(
  p_value text,
  p_min_length integer,
  p_max_length integer,
  p_allow_newlines boolean default false
)
returns text
language plpgsql
immutable
set search_path=''
as $function$
declare
  v_text text:=pg_catalog.btrim(
    pg_catalog.replace(
      pg_catalog.replace(coalesce(p_value,''),E'\r\n',E'\n'),
      E'\r',E'\n'
    )
  );
  v_control_check text;
begin
  if p_min_length<0 or p_max_length<p_min_length then
    raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
  end if;
  if not p_allow_newlines and pg_catalog.strpos(v_text,E'\n')>0 then
    raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
  end if;
  v_control_check:=pg_catalog.regexp_replace(v_text,E'[\n\t]','','g');
  if pg_catalog.char_length(v_text) not between p_min_length and p_max_length
     or pg_catalog.strpos(v_text,'<')>0
     or pg_catalog.strpos(v_text,'>')>0
     or v_control_check~'[[:cntrl:]]'
  then
    raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
  end if;
  return v_text;
end;
$function$;

create or replace function private._candidate_daily_information_normalize_v1(
  p_hospital_addresses jsonb,
  p_accommodation_contacts jsonb
)
returns jsonb
language plpgsql
immutable
set search_path=''
as $function$
declare
  v_item jsonb;
  v_output_hospitals jsonb:='[]'::jsonb;
  v_output_accommodation jsonb:='[]'::jsonb;
  v_hospital_name text;
  v_office_name text;
  v_address text;
  v_telephone text;
  v_map_query text;
  v_email text;
  v_working_hours text;
  v_seen text[]:='{}'::text[];
  v_key text;
begin
  if pg_catalog.jsonb_typeof(p_hospital_addresses)<>'array'
     or pg_catalog.jsonb_typeof(p_accommodation_contacts)<>'array'
     or pg_catalog.jsonb_array_length(p_hospital_addresses)>100
     or pg_catalog.jsonb_array_length(p_accommodation_contacts)>200
  then
    raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
  end if;

  for v_item in select value from pg_catalog.jsonb_array_elements(p_hospital_addresses)
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object'
       or not (v_item?'hospital_name') or not (v_item?'address')
       or exists (
         select 1 from pg_catalog.jsonb_object_keys(v_item) k
         where k not in ('hospital_name','address','telephone','map_query')
       )
       or pg_catalog.jsonb_typeof(v_item->'hospital_name')<>'string'
       or pg_catalog.jsonb_typeof(v_item->'address')<>'string'
       or (v_item?'telephone' and pg_catalog.jsonb_typeof(v_item->'telephone')<>'string')
       or (v_item?'map_query' and pg_catalog.jsonb_typeof(v_item->'map_query')<>'string')
    then
      raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
    end if;
    v_hospital_name:=private._candidate_daily_information_text_v1(
      v_item->>'hospital_name',1,160,false
    );
    v_address:=private._candidate_daily_information_text_v1(v_item->>'address',1,600,true);
    v_telephone:=private._candidate_daily_information_text_v1(
      coalesce(v_item->>'telephone',''),0,40,false
    );
    v_map_query:=private._candidate_daily_information_text_v1(
      coalesce(v_item->>'map_query',''),0,600,true
    );
    if v_telephone<>'' and v_telephone!~'^[0-9+(). /-]+$' then
      raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
    end if;
    v_key:=pg_catalog.lower(v_hospital_name);
    if v_key=any(v_seen) then
      raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_DUPLICATE';
    end if;
    v_seen:=pg_catalog.array_append(v_seen,v_key);
    v_output_hospitals:=v_output_hospitals||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'hospital_name',v_hospital_name,
        'address',v_address,
        'telephone',nullif(v_telephone,''),
        'map_query',nullif(v_map_query,'')
      ))
    );
  end loop;

  v_seen:='{}'::text[];
  for v_item in select value from pg_catalog.jsonb_array_elements(p_accommodation_contacts)
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object'
       or not (v_item?'hospital_name') or not (v_item?'office_name')
       or exists (
         select 1 from pg_catalog.jsonb_object_keys(v_item) k
         where k not in (
           'hospital_name','office_name','address','telephone','email','working_hours'
         )
       )
       or pg_catalog.jsonb_typeof(v_item->'hospital_name')<>'string'
       or pg_catalog.jsonb_typeof(v_item->'office_name')<>'string'
       or (v_item?'address' and pg_catalog.jsonb_typeof(v_item->'address')<>'string')
       or (v_item?'telephone' and pg_catalog.jsonb_typeof(v_item->'telephone')<>'string')
       or (v_item?'email' and pg_catalog.jsonb_typeof(v_item->'email')<>'string')
       or (v_item?'working_hours' and pg_catalog.jsonb_typeof(v_item->'working_hours')<>'string')
    then
      raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
    end if;
    v_hospital_name:=private._candidate_daily_information_text_v1(
      v_item->>'hospital_name',1,160,false
    );
    v_office_name:=private._candidate_daily_information_text_v1(
      v_item->>'office_name',1,160,false
    );
    v_address:=private._candidate_daily_information_text_v1(
      coalesce(v_item->>'address',''),0,600,true
    );
    v_telephone:=private._candidate_daily_information_text_v1(
      coalesce(v_item->>'telephone',''),0,40,false
    );
    v_email:=pg_catalog.lower(private._candidate_daily_information_text_v1(
      coalesce(v_item->>'email',''),0,254,false
    ));
    v_working_hours:=private._candidate_daily_information_text_v1(
      coalesce(v_item->>'working_hours',''),0,240,true
    );
    if v_telephone<>'' and v_telephone!~'^[0-9+(). /-]+$' then
      raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
    end if;
    if v_email<>'' and v_email!~'^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
      raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_INVALID';
    end if;
    v_key:=pg_catalog.lower(v_hospital_name)||E'\n'||pg_catalog.lower(v_office_name);
    if v_key=any(v_seen) then
      raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_DUPLICATE';
    end if;
    v_seen:=pg_catalog.array_append(v_seen,v_key);
    v_output_accommodation:=v_output_accommodation||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'hospital_name',v_hospital_name,
        'office_name',v_office_name,
        'address',nullif(v_address,''),
        'telephone',nullif(v_telephone,''),
        'email',nullif(v_email,''),
        'working_hours',nullif(v_working_hours,'')
      ))
    );
  end loop;

  return pg_catalog.jsonb_build_object(
    'accommodation_contacts',v_output_accommodation,
    'hospital_addresses',v_output_hospitals
  );
end;
$function$;

create or replace function public.candidate_daily_information_settings_get_v1()
returns jsonb
language sql
stable
security definer
set search_path=''
as $function$
  select pg_catalog.jsonb_build_object(
    'ok',true,
    'hospital_addresses',s.hospital_addresses,
    'accommodation_contacts',s.accommodation_contacts,
    'version',s.version,
    'semantic_sha256_hex',pg_catalog.encode(s.semantic_sha256,'hex'),
    'updated_at_utc',s.updated_at_utc
  )
  from public.candidate_daily_information_settings s
  where s.id=1
$function$;

create or replace function public.candidate_daily_information_settings_set_v1(
  p_expected_version bigint,
  p_hospital_addresses jsonb,
  p_accommodation_contacts jsonb,
  p_actor_identity_hmac_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_payload jsonb:=private._candidate_daily_information_normalize_v1(
    p_hospital_addresses,p_accommodation_contacts
  );
  v_hash bytea;
  v_actor bytea;
  v_existing public.candidate_daily_information_versions%rowtype;
  v_version bigint;
begin
  if p_expected_version is null or p_expected_version<1
     or coalesce(p_actor_identity_hmac_hex,'')!~'^[0-9a-f]{64}$'
     or pg_catalog.char_length(pg_catalog.btrim(coalesce(p_idempotency_key,'')))
       not between 16 and 200
     or p_now_utc is null
  then
    raise exception using errcode='22023',message='CANDIDATE_DAILY_INFORMATION_REQUEST_INVALID';
  end if;
  v_hash:=extensions.digest(pg_catalog.convert_to(v_payload::text,'UTF8'),'sha256');
  v_actor:=pg_catalog.decode(p_actor_identity_hmac_hex,'hex');

  select * into v_existing
  from public.candidate_daily_information_versions h
  where h.idempotency_key=p_idempotency_key;
  if found then
    if v_existing.semantic_sha256<>v_hash
       or v_existing.actor_identity_hmac<>v_actor then
      raise exception using errcode='PT409',message='CANDIDATE_DAILY_INFORMATION_IDEMPOTENCY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,
      'hospital_addresses',v_existing.payload->'hospital_addresses',
      'accommodation_contacts',v_existing.payload->'accommodation_contacts',
      'version',v_existing.version,
      'semantic_sha256_hex',pg_catalog.encode(v_existing.semantic_sha256,'hex'),
      'updated_at_utc',v_existing.recorded_at_utc,
      'idempotent_replay',true
    );
  end if;

  update public.candidate_daily_information_settings s set
    hospital_addresses=v_payload->'hospital_addresses',
    accommodation_contacts=v_payload->'accommodation_contacts',
    version=s.version+1,
    semantic_sha256=v_hash,
    updated_at_utc=p_now_utc,
    updated_by_hmac=v_actor
  where s.id=1 and s.version=p_expected_version
  returning s.version into v_version;
  if not found then
    raise exception using errcode='PT409',message='MYTMS_SETTINGS_VERSION_CONFLICT';
  end if;

  insert into public.candidate_daily_information_versions(
    version,payload,semantic_sha256,actor_identity_hmac,idempotency_key,
    reason_code,recorded_at_utc
  ) values (
    v_version,v_payload,v_hash,v_actor,p_idempotency_key,'OFFICE_SAVE',p_now_utc
  );

  return public.candidate_daily_information_settings_get_v1()
    ||pg_catalog.jsonb_build_object('idempotent_replay',false);
end;
$function$;

create or replace function public.candidate_daily_information_candidate_v1(
  p_internal_context jsonb,
  p_kind text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp(),
  p_correlation_id text default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_context jsonb;
  v_settings public.candidate_daily_information_settings%rowtype;
  v_kind text:=pg_catalog.lower(pg_catalog.btrim(coalesce(p_kind,'')));
begin
  v_context:=private._candidate_daily_context_v1(
    p_internal_context,'CANDIDATE_SURFACE',true
  );
  if p_now_utc is null
     or p_correlation_id!~'^[0-7][0-9A-HJKMNP-TV-Z]{25}$'
     or v_kind not in ('hospital-addresses','accommodation-contacts')
  then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select * into strict v_settings
  from public.candidate_daily_information_settings s
  where s.id=1;

  return pg_catalog.jsonb_build_object(
    'kind',v_kind,
    'title',case v_kind
      when 'hospital-addresses' then 'Hospital addresses'
      else 'Accommodation contacts'
    end,
    'schema_version',1,
    'entries',case v_kind
      when 'hospital-addresses' then v_settings.hospital_addresses
      else v_settings.accommodation_contacts
    end,
    'appInfo',pg_catalog.jsonb_build_object(
      'version','CLOUDTMS_DIRECTORY_V1',
      'buildTs',pg_catalog.to_char(
        v_settings.updated_at_utc at time zone 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
      )
    )
  );
end;
$function$;

alter function private._candidate_daily_information_text_v1(text,integer,integer,boolean)
  owner to postgres;
alter function private._candidate_daily_information_normalize_v1(jsonb,jsonb)
  owner to postgres;
alter function public.candidate_daily_information_settings_get_v1() owner to postgres;
alter function public.candidate_daily_information_settings_set_v1(
  bigint,jsonb,jsonb,text,text,timestamptz
) owner to postgres;
alter function public.candidate_daily_information_candidate_v1(jsonb,text,timestamptz,text)
  owner to postgres;

revoke all on function private._candidate_daily_information_text_v1(text,integer,integer,boolean)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_information_normalize_v1(jsonb,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_daily_information_settings_get_v1()
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_daily_information_settings_set_v1(
  bigint,jsonb,jsonb,text,text,timestamptz
) from public,anon,authenticated,service_role;
revoke all on function public.candidate_daily_information_candidate_v1(jsonb,text,timestamptz,text)
  from public,anon,authenticated,service_role;

grant execute on function public.candidate_daily_information_settings_get_v1()
  to service_role;
grant execute on function public.candidate_daily_information_settings_set_v1(
  bigint,jsonb,jsonb,text,text,timestamptz
) to service_role;
grant execute on function public.candidate_daily_information_candidate_v1(
  jsonb,text,timestamptz,text
) to service_role;

notify pgrst,'reload schema';

commit;
