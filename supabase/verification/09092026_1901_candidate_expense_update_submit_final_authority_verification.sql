\set ON_ERROR_STOP on

begin;

do $verification$
declare
  v_proc pg_catalog.pg_proc%rowtype;
  v_owner text;
  v_config text[];
  v_definition_sha256 text;
begin
  select proc_row.*
  into v_proc
  from pg_catalog.pg_proc proc_row
  join pg_catalog.pg_namespace namespace_row
    on namespace_row.oid=proc_row.pronamespace
  where namespace_row.nspname='public'
    and proc_row.proname='candidate_expense_update_submit_atomic_v1'
    and pg_catalog.pg_get_function_identity_arguments(proc_row.oid)=
      'p_session_id uuid, p_environment text, p_workflow_id uuid, p_expected_generation integer, p_update_id uuid, p_payload jsonb, p_idempotency_key text, p_now_utc timestamp with time zone';

  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_SUBMIT_FINAL_AUTHORITY_MISSING';
  end if;

  v_owner:=pg_catalog.pg_get_userbyid(v_proc.proowner);

  select coalesce(pg_catalog.array_agg(config_value order by config_value),array[]::text[])
  into v_config
  from pg_catalog.unnest(coalesce(v_proc.proconfig,array[]::text[])) config_value;

  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.replace(
          pg_catalog.regexp_replace(
            pg_catalog.pg_get_functiondef(v_proc.oid),
            E'\n SET "plpgsql_check\\.[^"]+" TO ''[^'']*''',
            '',
            'g'
          ),
          E'\r\n',
          E'\n'
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  ) into v_definition_sha256;

  if v_definition_sha256<>'3be046c5da95b7d076cb8e4459abf46301a407579de30197f644394ae7938ea3' then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_SUBMIT_FINAL_AUTHORITY_HASH_MISMATCH: %',
      v_definition_sha256;
  end if;

  if v_proc.prosecdef is distinct from true
     or v_proc.provolatile<>'v'
     or v_proc.proparallel<>'u'
     or v_config is distinct from array['search_path=pg_catalog, public, private, pg_temp']::text[]
     or v_owner<>current_user then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_SUBMIT_FINAL_AUTHORITY_METADATA_MISMATCH';
  end if;

  if exists (
       select 1
       from pg_catalog.aclexplode(coalesce(
         v_proc.proacl,
         pg_catalog.acldefault('f'::"char",v_proc.proowner)
       )) expanded_acl
       where expanded_acl.grantee=0
         and expanded_acl.privilege_type='EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'public.candidate_expense_update_submit_atomic_v1(uuid,text,uuid,integer,uuid,jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'public.candidate_expense_update_submit_atomic_v1(uuid,text,uuid,integer,uuid,jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_expense_update_submit_atomic_v1(uuid,text,uuid,integer,uuid,jsonb,text,timestamptz)',
       'EXECUTE'
     ) then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_SUBMIT_FINAL_AUTHORITY_ACL_MISMATCH';
  end if;
end;
$verification$;

rollback;

\echo 'PASS: Candidate pending-expense submit final authority is NEW/UPGRADE identical and service-only.'
