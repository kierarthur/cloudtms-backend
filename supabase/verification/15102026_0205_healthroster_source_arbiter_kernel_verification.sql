-- Mandatory constructor-only Source kernel owner/body/ACL/schema proof.
-- No application fixture or financial/worker function is executed.
begin;
do $source_kernel_verify$
declare e record;p record;role_name text;n integer;
begin
 for e in select * from(values
   ('private.timesheet_correction_a0_source_token_assert_v1(jsonb)','9c740017062e8b796b0e6c7f1c00c79fcf4108d68aa38d81ed2d4000c37ca000','i'),
   ('private.timesheet_correction_a0_fence_scalar_acquire_core_v1(jsonb,boolean)','ecc4320761f57d9c2e6e376121ab07cb5576c925cbaddba2072b1c8e548b6a31','v'),
   ('private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean)','a5ca75a91a3bd2725ff2f399f7432c7e5f0052bba07469affca2540686011b49','v'),
   ('private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean)','7b37741ba0377552a288ae700e95dde53b2f21ca7905bd81b4782071a6a7a2b8','v'),
   ('private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)','37eeb5ac3857fb815126e71b725eb264d20ec958ae7a4473248d1425c18abb1b','v')
 ) expected(signature,body_sha256,volatility) loop
  select f.* into p from pg_catalog.pg_proc f where f.oid=to_regprocedure(e.signature);
  if not found or p.proowner<>(select oid from pg_catalog.pg_roles where rolname=current_user)
   or not p.prosecdef or p.provolatile::text<>e.volatility
   or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
   or encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>e.body_sha256 then
    raise exception 'HEALTHROSTER_SOURCE_KERNEL_OWNER_BODY_PROFILE_INVALID' using errcode='55000';end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if has_function_privilege(role_name,p.oid,'EXECUTE') then
    raise exception 'HEALTHROSTER_SOURCE_KERNEL_PRIVATE_EXECUTE_EXPOSED' using errcode='42501';end if;
  end loop;
 end loop;
 select c.* into p from pg_catalog.pg_class c where c.oid=to_regclass('private.timesheet_correction_a0_transaction_fences');
 if not found or p.relkind<>'r' or not p.relrowsecurity or p.relforcerowsecurity
  or p.relowner<>(select oid from pg_catalog.pg_roles where rolname=current_user) then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_TABLE_OWNER_RLS_INVALID' using errcode='55000';end if;
 foreach role_name in array array['anon','authenticated','service_role'] loop
  if has_table_privilege(role_name,p.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_TABLE_PRIVILEGE_EXPOSED' using errcode='42501';end if;
 end loop;
 select count(*) into n from pg_catalog.pg_constraint c where c.conrelid=p.oid and c.contype='x'
  and c.conname in('timesheet_correction_a0_fences_uuid_excl','timesheet_correction_a0_fences_text_excl','timesheet_correction_a0_fences_coverage_excl');
 if n<>3 or not exists(select 1 from pg_catalog.pg_constraint c where c.conrelid=p.oid
  and c.conname='timesheet_correction_a0_fences_coverage_excl' and pg_get_constraintdef(c.oid) like '%retained WITH <>%')
  or to_regclass('private.timesheet_correction_a0_fences_coverage_retained_unique') is null then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_NATIVE_EXCLUSION_PROFILE_INVALID' using errcode='55000';end if;
 if not exists(select 1 from pg_catalog.pg_policy policy where policy.polrelid=p.oid
  and policy.polname='cloudtms_miget_service_owner_all' and policy.polcmd='*'
  and policy.polroles @> array[(select oid from pg_catalog.pg_roles where rolname=current_user),(select oid from pg_catalog.pg_roles where rolname='service_role')]
  and cardinality(policy.polroles)=2 and pg_get_expr(policy.polqual,policy.polrelid)='true'
  and pg_get_expr(policy.polwithcheck,policy.polrelid)='true') then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_PRIVATE_POLICY_INVALID' using errcode='55000';end if;
end $source_kernel_verify$;
rollback;
