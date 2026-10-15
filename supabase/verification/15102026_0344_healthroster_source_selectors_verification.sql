-- Data-free exact six-helper constructor profile. RAW trigger registration and real Source CAS are separately qualified.
begin;
do $selector_profile$
declare e record;p record;role_name text;
begin
 for e in select * from(values
('private.weekly_source_agency_source_dependencies_v1(uuid,uuid[])','028407bae355cf1c79de10e374b7232d012faf821c4dd6c37464a46cc5cb13dc','s','search_path=pg_catalog, public, private, pg_temp'),
('private.timesheet_correction_a0_source_footprint_v1(uuid,uuid[])','00b9edb762a81b2e1e782ca59339c8a2b00078504da3a48d550e92b5c7abfc73','s','search_path=pg_catalog, public, private, pg_temp'),
('private.timesheet_correction_a0_source_raw_tokens_v1(text,jsonb)','19599b34f465fdeb87f19b49e5af67abd0d533e1b4301b66d142d65d0beb2cb7','i','search_path=pg_catalog, pg_temp'),
('private.timesheet_correction_a0_source_revision_tokens_v1(text,jsonb)','9cc4a7069fc18a6d5ad3bc45f97fefa3174b454553cbff58d9c15143fb94fcfd','v','search_path=pg_catalog, pg_temp'),
('private.timesheet_correction_a0_source_policy_tokens_v1(text,jsonb)','6121e93116dc1ecf9936e6ea32fb30b7bc8123768745761b10e2b7e8f8d9f3ea','v','search_path=pg_catalog, pg_temp'),
('private.timesheet_correction_a0_source_reference_before_v1()','c49c00771146d2f366060bcc7c15e78c6ed17586bbccca5d94d0457614450c2d','v','search_path=pg_catalog, pg_temp')
 ) x(signature,body_sha256,volatility,configuration) loop
  select f.* into p from pg_proc f where f.oid=to_regprocedure(e.signature);
  if not found or p.proowner<>current_user::regrole::oid or not p.prosecdef or p.proleakproof
   or p.provolatile::text<>e.volatility or p.proconfig is distinct from array[e.configuration]
   or encode(sha256(convert_to(p.prosrc,'UTF8')),'hex')<>e.body_sha256 then
    raise exception 'HEALTHROSTER_SOURCE_SELECTOR_BODY_SECURITY_PROFILE_INVALID' using errcode='55000';end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if has_function_privilege(role_name,p.oid,'EXECUTE') then raise exception 'HEALTHROSTER_SOURCE_SELECTOR_PRIVATE_EXECUTE_EXPOSED' using errcode='42501';end if;
  end loop;
 end loop;
end $selector_profile$;
rollback;
