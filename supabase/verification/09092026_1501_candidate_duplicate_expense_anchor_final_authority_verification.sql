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
  where namespace_row.nspname='private'
    and proc_row.proname='_expense_duplicate_review_v1'
    and pg_catalog.pg_get_function_identity_arguments(proc_row.oid)=
      'p_workflow_id uuid, p_required_categories text[]';

  if not found then
    raise exception 'CANDIDATE_DUPLICATE_EXPENSE_FINAL_AUTHORITY_MISSING';
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

  if v_definition_sha256<>'366fbaae56a5db9c628126f50ce078763f877c611dfae6cc26c587b565990ec8' then
    raise exception 'CANDIDATE_DUPLICATE_EXPENSE_FINAL_AUTHORITY_HASH_MISMATCH: %',
      v_definition_sha256;
  end if;

  if v_proc.prosecdef is distinct from true
     or v_proc.provolatile<>'v'
     or v_proc.proparallel<>'u'
     or v_config is distinct from array['search_path=pg_catalog, public, private, pg_temp']::text[]
     or v_owner<>current_user then
    raise exception 'CANDIDATE_DUPLICATE_EXPENSE_FINAL_AUTHORITY_METADATA_MISMATCH';
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
       'anon','private._expense_duplicate_review_v1(uuid,text[])','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','private._expense_duplicate_review_v1(uuid,text[])','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role','private._expense_duplicate_review_v1(uuid,text[])','EXECUTE'
     ) then
    raise exception 'CANDIDATE_DUPLICATE_EXPENSE_FINAL_AUTHORITY_ACL_MISMATCH';
  end if;
end;
$verification$;

rollback;

\echo 'PASS: Candidate duplicate-expense final authority is current, private and NEW/UPGRADE identical.'
