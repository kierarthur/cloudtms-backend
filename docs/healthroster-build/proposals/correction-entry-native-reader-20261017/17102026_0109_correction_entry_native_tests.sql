-- REQUIRED_NOT_RUN. For root's real PG17 rollback rehearsal after installing
-- the exact proposal in that same server-owned transaction. No business DML.
-- psql invocation supplies -v entry_fixture_json=<root's reviewed JSON array>.
-- The direct rollback MCP can replace only the INSERT's psql value with its
-- reviewed literal. Each input is a real safe fixture identity, never guessed.
-- Run in a repeatable-read rollback-owned transaction; never COMMIT this file.
\if :{?entry_fixture_json}
\else
\echo 'entry_fixture_json is required: no fixture selection or setup is implicit'
\quit 3
\endif
create temporary table entry_cases on commit drop as
 select * from jsonb_to_recordset(:'entry_fixture_json'::jsonb)
 as x(case_id text,category text,actor_user_id uuid,timesheet_id uuid,
 admitted boolean,current_timesheet_id uuid,placement_revision text,pending boolean);
do $tests$
declare c record; actual jsonb; replay jsonb; denied boolean; category text; signature text;
begin
 if current_setting('server_version_num')::integer/10000<>17 then raise exception 'PG17_REQUIRED';end if;
 if (select count(*) from entry_cases)<>14 or (select count(distinct case_id) from entry_cases)<>14
  or exists(select 1 from entry_cases where case_id is null or actor_user_id is null or timesheet_id is null
   or admitted is null or pending is null) then raise exception 'EXACT_14_REVIEWED_CASES_REQUIRED';end if;
 foreach category in array array['ADMIN_ORDINARY','AUTHORISER_OUTSIDE_SOURCE','GOLDEN_KEY_LEGACY',
  'AUTHORISER_EXPENSE_ONLY','MOVED_A_ALIAS_TO_B','NATIVE_ROTATION','CERTIFIED_PENDING',
  'INACTIVE_ACTOR','ORDINARY_ACTOR','DAILY_SUBJECT','UNKNOWN_SUBJECT','ARCHIVED_SUBJECT',
  'CROSS_CANDIDATE_NATIVE_CONFLICT','MALFORMED_NATIVE_DATES'] loop
  if (select count(*) from entry_cases e where e.category=category)<>1 then
   raise exception 'MISSING_OR_DUPLICATED_CASE_CATEGORY: %',category;end if;
 end loop;
 for c in select * from entry_cases order by case_id loop
  denied:=false;actual:=null;
  begin
   actual:=public.timesheet_correction_entry_v1(jsonb_build_object(
    'version','TIMESHEET_CORRECTION_ENTRY_REQUEST_V1','timesheet_id',c.timesheet_id,'actor_user_id',c.actor_user_id));
  exception when insufficient_privilege then
   if sqlerrm<>'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' then raise exception 'NONCLOSED_REFUSAL: %',c.case_id;end if;
   denied:=true;
  end;
  if denied=c.admitted then raise exception 'ENTRY_ADMISSION_MISMATCH: %',c.case_id;end if;
  if c.admitted then
   if actual is distinct from jsonb_build_object('version','TIMESHEET_CORRECTION_ENTRY_V1',
     'requested_timesheet_id',c.timesheet_id,'current_timesheet_id',c.current_timesheet_id,
     'placement_revision',c.placement_revision,'purpose','CONTRACT_CORRECTION','may_open_review',false,
     'disabled_reason',case when c.pending then 'CURRENT_ALLOCATION_PENDING' else 'REVIEW_UNAVAILABLE' end) then
    raise exception 'ENTRY_EXACT_PROJECTION_MISMATCH: %',c.case_id;end if;
   replay:=public.timesheet_correction_entry_v1(jsonb_build_object(
    'version','TIMESHEET_CORRECTION_ENTRY_REQUEST_V1','timesheet_id',c.timesheet_id,'actor_user_id',c.actor_user_id));
   if replay is distinct from actual then raise exception 'READ_REPLAY_CHANGED: %',c.case_id;end if;
  end if;
 end loop;
 -- Unknown browser keys are not ignored or echoed.
 select * into c from entry_cases where category='ADMIN_ORDINARY';
 denied:=false;
 begin
  perform public.timesheet_correction_entry_v1(jsonb_build_object('version','TIMESHEET_CORRECTION_ENTRY_REQUEST_V1',
   'timesheet_id',c.timesheet_id,'actor_user_id',c.actor_user_id,'client_id',c.timesheet_id));
 exception when insufficient_privilege then denied:=true;end;
 if not denied then raise exception 'EXTRA_REQUEST_KEY_ACCEPTED';end if;
 foreach signature in array array['private.timesheet_correction_entry_subject_v1(uuid,uuid)',
  'private.timesheet_correction_entry_v1(uuid,uuid)','public.timesheet_correction_entry_v1(jsonb)'] loop
  if not exists(select 1 from pg_proc p where p.oid=signature::regprocedure and p.prosecdef
   and p.provolatile='s' and p.proconfig=array['search_path=pg_catalog, public, private, pg_temp']) then
   raise exception 'ENTRY_SECURITY_CONFIG_MISMATCH';end if;
  if has_function_privilege('anon',signature,'EXECUTE') or has_function_privilege('authenticated',signature,'EXECUTE')
   or exists(select 1 from pg_proc p cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
    where p.oid=signature::regprocedure and a.grantee=0 and a.privilege_type='EXECUTE') then
   raise exception 'BROWSER_ENTRY_EXECUTE_EXPOSED';end if;
 end loop;
 if not has_function_privilege('service_role','public.timesheet_correction_entry_v1(jsonb)','EXECUTE')
  or has_function_privilege('service_role','private.timesheet_correction_entry_v1(uuid,uuid)','EXECUTE')
  or has_function_privilege('service_role','private.timesheet_correction_entry_subject_v1(uuid,uuid)','EXECUTE') then
  raise exception 'ENTRY_SERVICE_BOUNDARY_MISMATCH';end if;
end;
$tests$;
-- Bounded output: no actor, Timesheet, Client, evidence, money or raw response.
select count(*) as asserted_cases,'REQUIRED_NATIVE_PROGRAM_COMPLETED' as result from entry_cases;
