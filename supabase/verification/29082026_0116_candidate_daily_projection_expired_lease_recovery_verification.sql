begin;

do $verify$
declare
  v_definition text;
  v_identity regprocedure :=
    'public.candidate_daily_projection_claim_v1(jsonb,uuid,text,text,text,integer,integer,text)'::regprocedure;
begin
  select pg_get_functiondef(v_identity) into v_definition;
  if v_definition is null then
    raise exception 'PROJECTION_LEASE_RECOVERY_VERIFY: claim routine missing';
  end if;
  if position('o.state=''CLAIMED''' in v_definition)=0
     or position('lease_expires_at_utc<=now()' in v_definition)=0
     or position('FOR UPDATE SKIP LOCKED' in upper(v_definition))=0
     or position('LIMIT P_MAX_ITEMS' in upper(v_definition))=0
     or position('safe_error_code=''LEASE_EXPIRED''' in v_definition)=0
     or position('o.delivery_attempt_count+1' in v_definition)=0
     or position('lease_token=null' in v_definition)=0 then
    raise exception 'PROJECTION_LEASE_RECOVERY_VERIFY: expired-lease recovery incomplete';
  end if;
  if position('o.lease_expires_at_utc>now()' in v_definition)=0
     or position('message=''LEASE_CONFLICT''' in v_definition)=0
     or position('message=''LEASE_EXPIRED_STATUS_REQUIRED''' in v_definition)=0 then
    raise exception 'PROJECTION_LEASE_RECOVERY_VERIFY: active/replay lease guards missing';
  end if;
  if has_function_privilege('anon',v_identity,'EXECUTE')
     or has_function_privilege('authenticated',v_identity,'EXECUTE') then
    raise exception 'PROJECTION_LEASE_RECOVERY_VERIFY: browser execution exposed';
  end if;
  if not has_function_privilege('service_role',v_identity,'EXECUTE') then
    raise exception 'PROJECTION_LEASE_RECOVERY_VERIFY: service execution missing';
  end if;
  if lower(v_definition) like '%pg_catalog.coalesce(%'
     or lower(v_definition) like '%pg_catalog.nullif(%'
     or lower(v_definition) like '%pg_catalog.least(%'
     or lower(v_definition) like '%pg_catalog.greatest(%' then
    raise exception 'PROJECTION_LEASE_RECOVERY_VERIFY: illegal conditional qualification';
  end if;
end;
$verify$;

rollback;
