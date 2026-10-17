-- Catalogue-only admission inside actual B. Never install or replace the existing key helper.
do $source_pure_preflight$
declare p pg_catalog.pg_proc%rowtype; n integer;
begin
 if current_database()<>'cloudtms_test_clone' or current_user<>session_user or current_user<>'dee50tht'
  or current_setting('transaction_read_only')<>'off'
  or current_setting('server_version_num')::integer not between 170000 and 179999 then
  raise exception 'SOURCE_PURE_NATIVE_CONTEXT_REFUSED' using errcode='55000';end if;
 select count(*) into n from pg_catalog.pg_proc p join pg_catalog.pg_namespace s on s.oid=p.pronamespace
  where s.nspname='private' and p.proname='weekly_source_pay_decision_native_status_child_v1';
 if n<>0 then raise exception 'SOURCE_PURE_NEW_NAME_ALREADY_PRESENT' using errcode='55000';end if;
 select count(*) into n from pg_catalog.pg_proc p join pg_catalog.pg_namespace s on s.oid=p.pronamespace
  where s.nspname='private' and p.proname='weekly_exceptional_json_keys_exact_v1';
 if n<>1 then raise exception 'SOURCE_PURE_DEPENDENCY_OVERLOAD_REFUSED' using errcode='55000';end if;
 select * into p from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure('private.weekly_exceptional_json_keys_exact_v1(jsonb,text[])');
 if not found then raise exception 'SOURCE_PURE_DEPENDENCY_MISSING' using errcode='55000';end if;
 if p.prokind<>'f' or p.pronargs<>2 or p.prorettype<>'boolean'::regtype or p.proretset
  or p.proargtypes<>'3802 1009'::oidvector or p.proargnames is distinct from array['p_value','p_allowed_keys']::text[]
  or (select lanname from pg_catalog.pg_language where oid=p.prolang)<>'sql'
  or p.provolatile<>'i' or p.prosecdef or pg_catalog.pg_get_userbyid(p.proowner)<>'dee50tht'
  or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
  or octet_length(pg_catalog.pg_get_functiondef(p.oid))<>429
  or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8')),'hex')<>'bb36d7e2d71abd7b9c0dfba29936d85edca54c8101591fd113e11d3ab7cf9189'
  or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>'c1e23ad8f8102e5b7b996bb448838441cb8920e23b64c69d46007d515b618063'
  or p.proacl is null or cardinality(p.proacl)<>1
  or (select count(*) from pg_catalog.aclexplode(p.proacl) a
   where a.grantee=p.proowner and a.grantor=p.proowner and a.privilege_type='EXECUTE' and not a.is_grantable)<>1
  or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
  or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
  or pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')
  or pg_catalog.has_schema_privilege('anon',p.pronamespace,'USAGE')
  or pg_catalog.has_schema_privilege('authenticated',p.pronamespace,'USAGE')
  or not pg_catalog.has_schema_privilege('service_role',p.pronamespace,'USAGE') then
  raise exception 'SOURCE_PURE_GENUINE_DEPENDENCY_CHANGED' using errcode='55000';end if;
end;$source_pure_preflight$;

create function private.weekly_source_pay_decision_native_status_child_v1(
 p_source jsonb,p_family uuid,p_run uuid,p_decision uuid,p_request uuid
) returns jsonb language plpgsql immutable security definer
set search_path=pg_catalog,private,public as $function$
declare s jsonb;r jsonb;n jsonb;b jsonb;a jsonb;k text;keys text[];
begin
 if p_source is null then
  if p_decision is not null or p_request is not null then
   raise exception 'SOURCE_NATIVE_OWNERSHIP_CONTRADICTION' using errcode='55000';
  end if;
  return null;
 end if;
 if jsonb_typeof(p_source) is distinct from 'object' then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if p_family is null or p_run is null then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if p_source->>'kind'='SOURCE_RESULT_REQUIRED' then
  keys:=array['kind','decision_id','request_id','reason'];
  if not private.weekly_exceptional_json_keys_exact_v1(p_source,keys) or not(p_source ?& keys)
   or jsonb_typeof(p_source->'decision_id') is distinct from 'string'
   or jsonb_typeof(p_source->'request_id') is distinct from 'string'
   or coalesce(p_source->>'decision_id','') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   or coalesce(p_source->>'request_id','') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   or (p_decision is not null and p_source->>'decision_id' is distinct from p_decision::text)
   or (p_request is not null and p_source->>'request_id' is distinct from p_request::text)
   or coalesce(p_source->>'reason','') not in('NOT_APPLIED_DEPENDENCY','APPLICATION_INCOMPLETE','NATIVE_RESULT_UNKNOWN') then
   raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';
  end if;
  return p_source;
 end if;
 keys:=array['kind','status'];
 if not private.weekly_exceptional_json_keys_exact_v1(p_source,keys) or not(p_source ?& keys)
  or p_source->>'kind' is distinct from 'NATIVE_STATUS' then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 s:=p_source->'status';keys:=array['ok','route','published','receipt'];
 if jsonb_typeof(s) is distinct from 'object' then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if not private.weekly_exceptional_json_keys_exact_v1(s,keys) or not(s ?& keys)
  or s->'ok' is distinct from 'true'::jsonb or coalesce(s->>'route','') not in('NEXT','SOURCE_LOCAL')
  or jsonb_typeof(s->'published') is distinct from 'boolean' or jsonb_typeof(s->'receipt') is distinct from 'object' then
  raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';
 end if;
 r:=s->'receipt';
 if r->>'family_id' is distinct from p_family::text or r->>'orchestration_run_id' is distinct from p_run::text
  or r->'idempotent_replay' is distinct from 'true'::jsonb then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if s->>'route'='NEXT' then
  keys:=array['ok','outcome','idempotent_replay','family_id','orchestration_run_id','approval_id','generation_id','work_id','revision_id','command_id',
   'agency_sequence','new_family_bound_version','approved_pay_ex_vat','state'];
  if not private.weekly_exceptional_json_keys_exact_v1(r,keys) or not(r ?& keys)
   or s->'published' is distinct from 'true'::jsonb or r->'ok' is distinct from 'true'::jsonb
   or r->>'outcome' is distinct from 'PUBLISHED_NEXT' or r->>'state' is distinct from 'NEXT_PUBLISHED'
   or jsonb_typeof(r->'approved_pay_ex_vat') is distinct from 'string'
   or coalesce(r->>'approved_pay_ex_vat','') !~ '^-?(0|[1-9][0-9]*)[.][0-9]{2}$' then
   raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';
  end if;
  foreach k in array array['approval_id','generation_id','work_id','revision_id','command_id'] loop
   if jsonb_typeof(r->k) is distinct from 'string' or coalesce(r->>k,'') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
  end loop;
  foreach k in array array['agency_sequence','new_family_bound_version'] loop
   if jsonb_typeof(r->k) is distinct from 'string' or coalesce(r->>k,'') !~ '^[1-9][0-9]{0,18}$' then
    raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
   if (r->>k)::numeric>9223372036854775807 then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
  end loop;
  return p_source;
 end if;
 keys:=array['authority','family_id','orchestration_run_id','publication_request_id','request_sha256','generation_id','state',
  'requires_first_authorisation','idempotent_replay','result','banking_receipt'];
 if not private.weekly_exceptional_json_keys_exact_v1(r,keys) or not(r ?& keys) or r->>'authority' is distinct from 'SOURCE_LOCAL'
  or coalesce(r->>'state','') not in('COMPLETE','PENDING_FREEZE') or jsonb_typeof(r->'requires_first_authorisation') is distinct from 'boolean'
  or jsonb_typeof(r->'request_sha256') is distinct from 'string' or coalesce(r->>'request_sha256','') !~ '^[0-9a-f]{64}$' then
  raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 foreach k in array array['publication_request_id','generation_id'] loop
  if jsonb_typeof(r->k) is distinct from 'string' or coalesce(r->>k,'') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
   raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 end loop;
 -- NativeLocalResult has its existing authority-specific native payload. Do
 -- not invent a closed replacement schema for that payload. The genuine saved
 -- native owner re-proves it; these are the unchanged normal Worker relations.
 n:=r->'result';b:=r->'banking_receipt';
 if jsonb_typeof(n) is distinct from 'object' or n->'ok' is distinct from 'true'::jsonb or n->'idempotent_replay' is distinct from 'true'::jsonb
  or n->>'family_id' is distinct from p_family::text or n->'generation_id' is distinct from r->'generation_id'
  or n->'publication_request_id' is distinct from r->'publication_request_id'
  or n->'requires_first_authorisation' is distinct from r->'requires_first_authorisation'
  or coalesce(n->>'authority','') not in('UNAUTHORISED_TSFIN','COMMON_ENTITLEMENT_HEAD')
  or (n->'requires_first_authorisation'='true'::jsonb) is distinct from (n->>'authority'='UNAUTHORISED_TSFIN')
  or (r->>'state'='PENDING_FREEZE' and (n->>'outcome' is distinct from 'SAVED_PENDING_FREEZE' or n->>'state' is distinct from 'PENDING' or r->'requires_first_authorisation'<>'false'::jsonb))
  or (r->>'state'='COMPLETE' and (n->>'outcome' is distinct from 'PUBLISHED' or n->>'state' is distinct from 'LIVE'))
  or s->'published' is distinct from to_jsonb(b is distinct from 'null'::jsonb)
  or ((r->'requires_first_authorisation'='true'::jsonb or r->>'state'='PENDING_FREEZE') and b is distinct from 'null'::jsonb) then
  raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if b='null'::jsonb then return p_source;end if;
 if jsonb_typeof(b) is distinct from 'object' then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 keys:=array['source_head_id','work_id','revision_id','command_id','agency_sequence','publication_status','application'];
 if not private.weekly_exceptional_json_keys_exact_v1(b,keys) or not(b ?& keys)
  or coalesce(b->>'publication_status','') not in('QUEUED','APPLYING','APPLIED') then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 foreach k in array array['source_head_id','work_id','revision_id','command_id'] loop
  if jsonb_typeof(b->k) is distinct from 'string' or coalesce(b->>k,'') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
   raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 end loop;
 if jsonb_typeof(b->'agency_sequence') is distinct from 'string' or coalesce(b->>'agency_sequence','') !~ '^[1-9][0-9]{0,18}$' then
  raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if (b->>'agency_sequence')::numeric>9223372036854775807 then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 a:=b->'application';keys:=array['ok','code','work_id','owner_epoch','current_revision_id','applied_revision_id','source_event_id','source_head_id',
  'financial_snapshot_id','physical_root_id','physical_root_version','revision_seal','source_inventory_digest','application_state','command_id','agency_sequence','financial_view_revision'];
 if jsonb_typeof(a) is distinct from 'object' then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if not private.weekly_exceptional_json_keys_exact_v1(a,keys) or not(a ?& keys)
  or jsonb_typeof(a->'ok') is distinct from 'boolean' or coalesce(a->>'code','') not in('OK','UNAVAILABLE','INCOMPATIBLE')
  or a->'ok' is distinct from to_jsonb(a->>'code'='OK')
  or (a->>'code'='OK' and coalesce(a->>'application_state','') not in('APPLIED','PENDING'))
  or (a->>'code'<>'OK' and a->>'application_state' is distinct from a->>'code')
  or a->'work_id' is distinct from b->'work_id' or a->'source_head_id' is distinct from b->'source_head_id'
  or a->'source_event_id' is distinct from b->'source_head_id' or a->'command_id' is distinct from b->'command_id'
  or a->'agency_sequence' is distinct from b->'agency_sequence' then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 foreach k in array array['current_revision_id','applied_revision_id','financial_snapshot_id','physical_root_id'] loop
  if a->k is distinct from 'null'::jsonb and (jsonb_typeof(a->k) is distinct from 'string'
   or coalesce(a->>k,'') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') then
   raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 end loop;
 foreach k in array array['owner_epoch','financial_view_revision'] loop
  if a->k is distinct from 'null'::jsonb then
   if jsonb_typeof(a->k) is distinct from 'string' or coalesce(a->>k,'') !~ '^(0|[1-9][0-9]{0,18})$'
    or (k='owner_epoch' and a->>k='0') then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
   if (a->>k)::numeric>9223372036854775807 then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
  end if;
 end loop;
 if a->'physical_root_version' is distinct from 'null'::jsonb then
  if jsonb_typeof(a->'physical_root_version') is distinct from 'number' then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
  -- Existing expressly safe native JSON integer ABI. Numeric 3 and 3.0 are
  -- the same integer; new bigint strings above remain lexically canonical.
  if (a->>'physical_root_version')::numeric<1 or (a->>'physical_root_version')::numeric>9007199254740991
   or trunc((a->>'physical_root_version')::numeric)<>(a->>'physical_root_version')::numeric then
   raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 end if;
 if a->'revision_seal' is distinct from 'null'::jsonb and (jsonb_typeof(a->'revision_seal') is distinct from 'string'
  or coalesce(a->>'revision_seal','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z$') then
  raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if a->'source_inventory_digest' is distinct from 'null'::jsonb and (jsonb_typeof(a->'source_inventory_digest') is distinct from 'string'
  or coalesce(a->>'source_inventory_digest','') !~ '^[0-9a-f]{64}$') then raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 if a->'ok'='true'::jsonb and (a->'current_revision_id' is distinct from b->'revision_id'
  or a->'owner_epoch'='null'::jsonb or a->'financial_view_revision'='null'::jsonb or a->'physical_root_id'='null'::jsonb
  or a->'physical_root_version'='null'::jsonb or a->'revision_seal'='null'::jsonb or a->'source_inventory_digest'='null'::jsonb
  or (a->>'application_state'='APPLIED' and (a->'applied_revision_id' is distinct from b->'revision_id' or b->>'publication_status'<>'APPLIED'))
  or (a->>'application_state'='PENDING' and (a->'applied_revision_id'=b->'revision_id' or b->>'publication_status'='APPLIED'))) then
  raise exception 'SOURCE_NATIVE_STATUS_INVALID' using errcode='55000';end if;
 return p_source;
end;
$function$;
revoke all on function private.weekly_source_pay_decision_native_status_child_v1(jsonb,uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;

-- Closed owner-only admission before the first call of the newly created pure child.
do $source_pure_postflight$
declare p pg_catalog.pg_proc%rowtype; n integer;
begin
 select count(*) into n from pg_catalog.pg_proc p join pg_catalog.pg_namespace s on s.oid=p.pronamespace
  where s.nspname='private' and p.proname='weekly_source_pay_decision_native_status_child_v1';
 if n<>1 then raise exception 'SOURCE_PURE_NEW_NAME_COUNT_REFUSED' using errcode='55000';end if;
 select * into p from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure('private.weekly_source_pay_decision_native_status_child_v1(jsonb,uuid,uuid,uuid,uuid)');
 if not found then raise exception 'SOURCE_PURE_NEW_SIGNATURE_MISSING' using errcode='55000';end if;
 if p.prokind<>'f' or p.pronargs<>5 or p.prorettype<>'jsonb'::regtype or p.proretset
  or p.proargtypes<>'3802 2950 2950 2950 2950'::oidvector
  or p.proargnames is distinct from array['p_source','p_family','p_run','p_decision','p_request']::text[]
  or (select lanname from pg_catalog.pg_language where oid=p.prolang)<>'plpgsql'
  or p.provolatile<>'i' or not p.prosecdef or pg_catalog.pg_get_userbyid(p.proowner)<>'dee50tht'
  or p.proconfig is distinct from array['search_path=pg_catalog, private, public']::text[]
  or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>'6798fc172f368d87abcfb950a60623b682df3cbce6667d54813da32ac77bb70e'
  or p.proacl is null or cardinality(p.proacl)<>1
  or (select count(*) from pg_catalog.aclexplode(p.proacl) a
   where a.grantee=p.proowner and a.grantor=p.proowner and a.privilege_type='EXECUTE' and not a.is_grantable)<>1
  or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
  or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
  or pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') then
  raise exception 'SOURCE_PURE_NEW_DEFINITION_OR_ACL_REFUSED' using errcode='55000';end if;
end;$source_pure_postflight$;

-- SYNTHETIC JSON boundary vectors only. No business fixtures or Banking success/economics.
do $source_pure_boundaries$
declare
 f constant uuid:='c9234701-4bed-40ae-a01d-795a27562823';
 r constant uuid:='0fe0a2df-4a2b-47d9-9bf4-13955806677f';
 d constant uuid:='44949c19-688b-454e-a58a-668a2f545521';
 q constant uuid:='bf2504ee-6851-44a3-a04c-3e47fd58e934';
 other_id constant uuid:='1f215dec-1164-4c76-bc41-4d0fd9b617ec';
 j jsonb; got jsonb; reason text; v record; refused boolean; positives integer:=0; negatives integer:=0;
begin
 got:=private.weekly_source_pay_decision_native_status_child_v1(null,f,r,null,null);
 if got is not null then raise exception 'SOURCE_PURE_NULL_RESULT_FAILED';end if;positives:=positives+1;
 foreach reason in array array['NOT_APPLIED_DEPENDENCY','APPLICATION_INCOMPLETE','NATIVE_RESULT_UNKNOWN'] loop
  j:=jsonb_build_object('kind','SOURCE_RESULT_REQUIRED','decision_id',d::text,'request_id',q::text,'reason',reason);
  got:=private.weekly_source_pay_decision_native_status_child_v1(j,f,r,d,q);
  if got is distinct from j then raise exception 'SOURCE_PURE_CLOSED_REQUIRED_ROUNDTRIP_FAILED';end if;positives:=positives+1;
 end loop;
 j:=jsonb_build_object('kind','SOURCE_RESULT_REQUIRED','decision_id',d::text,'request_id',q::text,'reason','NATIVE_RESULT_UNKNOWN');
 for v in select * from (values
  (null::jsonb,f,r,d,null::uuid,'SOURCE_NATIVE_OWNERSHIP_CONTRADICTION'),
  (null::jsonb,f,r,null::uuid,q,'SOURCE_NATIVE_OWNERSHIP_CONTRADICTION'),
  ('null'::jsonb,f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  ('[]'::jsonb,f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j-'request_id',f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j||'{"extra":true}'::jsonb,f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j,f,r,other_id,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j,f,r,d,other_id,'SOURCE_NATIVE_STATUS_INVALID'),
  (jsonb_set(j,'{reason}','"APPROVED"'::jsonb),f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j,null::uuid,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (jsonb_set(j,'{decision_id}','123'::jsonb),f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (jsonb_set(j,'{request_id}','"BF2504EE-6851-44A3-A04C-3E47FD58E934"'::jsonb),f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID')
 ) cases(source,family_id,run_id,decision_id,request_id,expected_message) loop
  refused:=false;
  begin
   perform private.weekly_source_pay_decision_native_status_child_v1(v.source,v.family_id,v.run_id,v.decision_id,v.request_id);
  exception when sqlstate '55000' then
   if sqlerrm is distinct from v.expected_message then raise exception 'SOURCE_PURE_WRONG_REFUSAL';end if;
   refused:=true;
  end;
  if not refused then raise exception 'SOURCE_PURE_MALFORMED_BOUNDARY_ACCEPTED';end if;
  negatives:=negatives+1;
 end loop;
 if positives<>4 or negatives<>12 then raise exception 'SOURCE_PURE_VECTOR_CENSUS_CHANGED';end if;
end;$source_pure_boundaries$;
select 'HR_SOURCE_STATUS_CHILD_PURE_PASS_4_12'::text as result;
