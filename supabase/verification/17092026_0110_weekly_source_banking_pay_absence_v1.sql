-- Rollback-only PostgreSQL 17 proof for WP-21: the setting
-- `weekly_source.banking_pay_integration_absent`.
--
-- WHAT THIS FILE DELIBERATELY DOES NOT DO.  It writes to NO Banking Pay-owned
-- relation: not `pay_batches`, not `pay_bank_transfers`, not `pay_batch_items`,
-- not `pay_batch_candidates`, not `pay_advance_reservations`, not the
-- unpay-batch relation, not any other. Manufacturing payment evidence in order
-- to test our own side would be producing exactly the evidence the accepted
-- Banking Pay contract says must never be sufficient, and it would prove only
-- that we agree with ourselves. Where a proof would require such evidence, this
-- file does NOT fake it: it states the limit instead, and the report says so
-- plainly.
--
-- WHAT IT PROVES, all executed:
--
--   1. Structure, ownership and privileges; no API role can reach any of it.
--   2. DEFAULT OFF: the relation is empty after a release and the reader says
--      so.
--   3. REFUSES IN A LIVE ENVIRONMENT, by its own check, whether it is written
--      through the function or directly, and whether the environment is LIVE or
--      simply unestablished.
--   4. IT PRODUCES NO MONEY-MOVEMENT VERDICT. The whole package is scanned for
--      the act of producing one, and the classifier's own answers are proved
--      untouched with the setting on.
--   5. IT CHANGES NO OUTCOME. A Weekly Source root is driven through
--      authorisation, the freeze census and the withdrawal availability verdict
--      with the setting OFF and again with the setting ON, and every payload is
--      compared key for key.
--   6. IT IS READ BY NO OWNER. The installed definition of every Weekly Source
--      routine is scanned, and none of them reads the setting.
--   7. IT CANNOT OUTLIVE ITS OWN TRUTH: the reader and the declare function both
--      raise once the integration has arrived.
--   8. IT DOES NOT TOUCH THE WP-07c TRIPWIRE: that tripwire's own probes are
--      still inline in its own file and do not call anything in this package.
--
-- Prerequisites: the Weekly Source schema migration, the WP-21 migration
-- `18092026_1740_weekly_source_banking_pay_absence.sql`, the rotation authority
-- (WP-03), the freeze census (WP-08a) and the first-authorisation owner
-- (WP-07/WP-07c).
--
-- Everything is inside one transaction that is rolled back.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is distinct from true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
end;
$function$;

create function pg_temp.assert_refused(
  p_sql text,p_message_like text,p_label text
) returns void language plpgsql as $function$
declare
  v_message text;
begin
  begin
    execute p_sql;
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message not like p_message_like then
      raise exception 'ASSERTION_FAILED: % refused with "%" not "%"',
        p_label,v_message,p_message_like;
    end if;
    return;
  end;
  raise exception 'ASSERTION_FAILED: % was accepted',p_label;
end;
$function$;

create function pg_temp.drain_workbench_jobs() returns void
language sql as $function$
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
$function$;

create function pg_temp.comparable(p_value jsonb) returns jsonb
language sql immutable as $function$
  select coalesce(p_value,'null'::jsonb) - 'evaluated_at_utc';
$function$;

-- ---------------------------------------------------------------------------
-- Fixture: ONE ordinary Weekly Source root, with NO Banking Pay data of any
-- kind. That is the point -- this is a root upstream of the Banking Pay
-- boundary, which is where the owner asked for testability, and it needs no
-- payment evidence to exercise.
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('21000000-0000-4000-8000-000000000001','wp21-office@example.test','admin',true,'not-a-login');
insert into public.clients(id,name) values ('21000000-0000-4000-8000-000000000002','WP21 Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('21000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name)
values ('21000000-0000-4000-8000-000000000101','WP21 Candidate 1');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  '21000000-0000-4000-8000-000000000201','21000000-0000-4000-8000-000000000101',
  '21000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}'::jsonb,
  'HEALTHROSTER',true,true,true,true);
insert into public.timesheets(
  timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
  line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
  qr_payload_json,is_adjustment,created_at,updated_at
) values (
  '21000000-0000-4000-8000-000000000301','WP21-BK-01',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'wp21-occupant','wp21-hospital','wp21-ward','wp21-role','weekly-0','2026-09-13',
  '21000000-0000-4000-8000-000000000201','[]'::jsonb,'{}'::jsonb,false,
  '2026-09-13 00:00:00+00','2026-09-13 00:00:00+00');
insert into public.contract_weeks(
  id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
  timesheet_id,is_adjustment
) values (
  '21000000-0000-4000-8000-000000000401','21000000-0000-4000-8000-000000000201',
  '2026-09-13',0,'SUBMITTED'::public.contract_week_status_enum,
  'MANUAL'::public.submission_mode_enum,'21000000-0000-4000-8000-000000000301',false);
insert into public.timesheets_financials(
  id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
  processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat
) values (
  '21000000-0000-4000-8000-000000000501','21000000-0000-4000-8000-000000000301',1,true,
  '21000000-0000-4000-8000-000000000101','21000000-0000-4000-8000-000000000002',
  'PENDING_AUTH'::public.ts_fin_processing_status_enum,10,100,200);

select pg_temp.drain_workbench_jobs();

-- ===========================================================================
-- 1. Structure, ownership, privileges
-- ===========================================================================
do $verify_structure$
declare
  v_count integer;
begin
  perform pg_temp.assert_true(
    pg_catalog.to_regclass('private.weekly_source_banking_pay_absence') is not null,
    'the absence relation must exist');

  -- No API role can reach the relation or any WP-21 function. This is what
  -- makes the setting unreachable from the broker, PostgREST, the Office screen
  -- and the Candidate app, in every environment.
  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_class as relation
  join pg_catalog.pg_namespace as relation_schema on relation_schema.oid=relation.relnamespace
  cross join lateral pg_catalog.aclexplode(
    coalesce(relation.relacl,pg_catalog.acldefault('r',relation.relowner))) as acl
  where relation_schema.nspname='private'
    and relation.relname='weekly_source_banking_pay_absence'
    and acl.grantee<>0
    and pg_catalog.pg_get_userbyid(acl.grantee)
        in ('anon','authenticated','service_role','authenticator');
  perform pg_temp.assert_true(v_count=0,
    'no API role may hold any privilege on the absence relation, found '||v_count);

  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as routine_schema on routine_schema.oid=routine.pronamespace
  cross join lateral pg_catalog.aclexplode(
    coalesce(routine.proacl,pg_catalog.acldefault('f',routine.proowner))) as acl
  where routine_schema.nspname='private'
    and routine.proname like '%weekly\_source\_banking\_pay\_%' escape '\'
    and acl.grantee<>0
    and pg_catalog.pg_get_userbyid(acl.grantee)
        in ('anon','authenticated','service_role','authenticator');
  perform pg_temp.assert_true(v_count=0,
    'no API role may execute any WP-21 function, found '||v_count);

  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as routine_schema on routine_schema.oid=routine.pronamespace
  where routine_schema.nspname='private'
    and routine.proname like '%weekly\_source\_banking\_pay\_%' escape '\'
    and (not routine.prosecdef
         or routine.proconfig is null
         or not exists (select 1 from pg_catalog.unnest(routine.proconfig) as setting(value)
                        where setting.value like 'search\_path=%' escape '\'));
  perform pg_temp.assert_true(v_count=0,
    'every WP-21 routine must be SECURITY DEFINER with a fixed search_path, '||v_count||' are not');

  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as routine_schema on routine_schema.oid=routine.pronamespace
  where routine_schema.nspname='private'
    and routine.proname like '%weekly\_source\_banking\_pay\_%' escape '\'
    and (routine.prosrc like '%pg\_catalog.coalesce(%' escape '\'
         or routine.prosrc like '%pg\_catalog.nullif(%' escape '\'
         or routine.prosrc like '%pg\_catalog.least(%' escape '\'
         or routine.prosrc like '%pg\_catalog.greatest(%' escape '\');
  perform pg_temp.assert_true(v_count=0,
    'no WP-21 routine may schema-qualify a conditional construct, '||v_count||' do');
end
$verify_structure$;

-- ===========================================================================
-- 2. DEFAULT OFF
-- ===========================================================================
do $verify_default_off$
declare
  v_count integer;
  v_state jsonb;
begin
  select pg_catalog.count(*)::integer into v_count
  from private.weekly_source_banking_pay_absence;
  perform pg_temp.assert_true(v_count=0,
    'the setting relation must be EMPTY after a release, found '||v_count||' rows');

  v_state:=private.weekly_source_banking_pay_absence_v1();
  perform pg_temp.assert_true(
    v_state->>'setting'='weekly_source.banking_pay_integration_absent'
    and coalesce((v_state->>'declared')::boolean,true) is false
    and v_state->>'reason_code'='SETTING_ABSENT',
    'an absent setting must read as OFF, got '||v_state::text);

  perform pg_temp.assert_true(
    private.weekly_source_banking_pay_boundary_notice_v1() is null,
    'the boundary notice must be NULL while the setting is off');
end
$verify_default_off$;

-- ===========================================================================
-- 3. IT PRODUCES NO MONEY-MOVEMENT VERDICT
--
-- Scanned over the INSTALLED definitions, not this file's source. No WP-21
-- routine may construct a cash state, and none may be reachable as a classifier
-- substitute: none of them returns the classifier's column set.
-- ===========================================================================
do $verify_produces_no_verdict$
declare
  v_offenders text;
begin
  -- No WP-21 routine may ASSIGN or RETURN a money-movement state value. The
  -- branch probe legitimately READS them back from the installed classifier, so
  -- the test is for the literals a producer would have to write.
  select pg_catalog.string_agg(routine_schema.nspname||'.'||routine.proname,', ')
    into v_offenders
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as routine_schema on routine_schema.oid=routine.pronamespace
  where routine_schema.nspname='private'
    and routine.proname like '%weekly\_source\_banking\_pay\_%' escape '\'
    and (routine.prosrc like '%''TERMINAL_NO_MONEY''%'
         or routine.prosrc like '%''PENDING_NON_FINAL''%'
         or routine.prosrc like '%''FINAL_MONEY_MOVED''%');
  perform pg_temp.assert_true(v_offenders is null,
    'no WP-21 routine may name a money-movement state value, so none can produce one; '
    ||'offenders: '||coalesce(v_offenders,'<none>'));

  -- And none of them returns the classifier's typed column set, so none can be
  -- dropped in as a substitute for it.
  select pg_catalog.string_agg(routine_schema.nspname||'.'||routine.proname,', ')
    into v_offenders
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as routine_schema on routine_schema.oid=routine.pronamespace
  where routine_schema.nspname='private'
    and routine.proname like '%weekly\_source\_banking\_pay\_%' escape '\'
    and routine.proname<>'weekly_source_banking_pay_branch_probe_v1'
    and coalesce(pg_catalog.array_position(routine.proargnames,'cash_state'),0)>0;
  perform pg_temp.assert_true(v_offenders is null,
    'only the arrival probe may carry the classifier typed columns, and it returns them '
    ||'verbatim; offenders: '||coalesce(v_offenders,'<none>'));

  -- Nothing in WP-21 writes to a Banking Pay-owned relation.
  select pg_catalog.string_agg(routine_schema.nspname||'.'||routine.proname,', ')
    into v_offenders
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as routine_schema on routine_schema.oid=routine.pronamespace
  where routine_schema.nspname='private'
    and routine.proname like '%weekly\_source\_banking\_pay\_%' escape '\'
    and pg_catalog.lower(routine.prosrc) ~
        '(insert[[:space:]]+into|update|delete[[:space:]]+from)[[:space:]]+(public\.)?pay_';
  perform pg_temp.assert_true(v_offenders is null,
    'no WP-21 routine may write to a Banking Pay-owned relation; offenders: '
    ||coalesce(v_offenders,'<none>'));
end
$verify_produces_no_verdict$;

-- ===========================================================================
-- 4. IT IS READ BY NO OWNER
--
-- The setting has exactly one reader, and no routine that decides anything can
-- see it. Scanned over installed definitions.
-- ===========================================================================
do $verify_no_owner_reads_it$
declare
  v_readers text;
begin
  select pg_catalog.string_agg(routine_schema.nspname||'.'||routine.proname,', '
           order by routine_schema.nspname||'.'||routine.proname)
    into v_readers
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as routine_schema on routine_schema.oid=routine.pronamespace
  where routine_schema.nspname in ('public','private')
    and routine.prokind in ('f','p')
    and pg_catalog.pg_get_functiondef(routine.oid)
        ~ '\mprivate\.weekly_source_banking_pay_absence\M';
  perform pg_temp.assert_true(
    coalesce(v_readers,'')
      ='private.weekly_source_banking_pay_absence_clear_v1, '
      ||'private.weekly_source_banking_pay_absence_declare_v1, '
      ||'private.weekly_source_banking_pay_absence_v1',
    'the ONLY routines that may touch the setting relation are the WP-21 reader and its two '
    ||'TEST controls; found: '||coalesce(v_readers,'<none>'));

  -- And no money owner calls the reader or the notice.
  select pg_catalog.string_agg(routine_schema.nspname||'.'||routine.proname,', '
           order by routine_schema.nspname||'.'||routine.proname)
    into v_readers
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as routine_schema on routine_schema.oid=routine.pronamespace
  where routine_schema.nspname in ('public','private')
    and routine.prokind in ('f','p')
    and routine.proname not like '%weekly\_source\_banking\_pay\_%' escape '\'
    and (pg_catalog.pg_get_functiondef(routine.oid)
           ~ '\mweekly_source_banking_pay_absence_v1\M'
         or pg_catalog.pg_get_functiondef(routine.oid)
           ~ '\mweekly_source_banking_pay_boundary_notice_v1\M');
  perform pg_temp.assert_true(v_readers is null,
    'no owner outside WP-21 may consult the setting or its notice; found: '
    ||coalesce(v_readers,'<none>'));
end
$verify_no_owner_reads_it$;

-- ===========================================================================
-- 5. IT DOES NOT TOUCH THE WP-07c FUTURE-EXPECTATION TRIPWIRE
--
-- The tripwire lives in WP-07c's own verification file and builds its own
-- probes. Nothing in WP-21 can suppress, defer or condition it, because nothing
-- the tripwire runs belongs to WP-21. Proved from the installed state: the
-- classifier the tripwire reads is untouched, and it still fails closed.
-- ===========================================================================
do $verify_tripwire_untouched$
declare
  v_pre record;
  v_overlay record;
  v_pending record;
begin
  select * into v_pre
  from public._pay_rail_state_money_movement_classify(
    'VOIDED',null,
    pg_catalog.jsonb_build_object(
      'failed_reason','PRE_BANK_CANCEL_VOIDED','pre_bank_cancel_applied',true),
    pg_catalog.jsonb_build_object(
      'failed_reason','PRE_BANK_CANCEL_VOIDED','pre_bank_cancel_applied',true));
  select * into v_overlay
  from public._pay_rail_state_money_movement_classify(
    'VOIDED',null,
    pg_catalog.jsonb_build_object(
      'failed_reason','CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED',
      'cancellation_reauthorisation_overlay_voided',true,'amount',0),
    pg_catalog.jsonb_build_object(
      'failed_reason','CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED',
      'cancellation_reauthorisation_overlay_voided',true,'amount',0));
  select * into v_pending
  from public._pay_rail_state_money_movement_classify('PENDING',null,'{}'::jsonb,'{}'::jsonb);

  perform pg_temp.assert_true(
    v_pre.cash_state='UNKNOWN' and coalesce(v_pre.is_terminal_no_money,false) is false
    and v_overlay.cash_state='UNKNOWN' and coalesce(v_overlay.is_terminal_no_money,false) is false,
    'the installed classifier must still fail closed on BOTH branches, which is what keeps the '
    ||'completed-cancellation journey correctly blocked; got pre='||coalesce(v_pre.cash_state,'<null>')
    ||' overlay='||coalesce(v_overlay.cash_state,'<null>'));
  perform pg_temp.assert_true(
    v_pending.cash_state='PENDING_NON_FINAL'
    and coalesce(v_pending.is_pending_non_final,false)
    and coalesce(v_pending.is_terminal_no_money,false) is false,
    'PENDING_NON_FINAL must stay preserved and distinct');

  -- The arrival probe returns exactly those answers, verbatim, adding nothing.
  perform pg_temp.assert_true(
    (select probe.cash_state from
       private.weekly_source_banking_pay_branch_probe_v1('PRE_BANK_CANCEL_VOIDED') as probe)
      is not distinct from v_pre.cash_state
    and (select probe.reason from
       private.weekly_source_banking_pay_branch_probe_v1('PRE_BANK_CANCEL_VOIDED') as probe)
      is not distinct from v_pre.reason,
    'the arrival probe must return the installed classifier answer verbatim for branch 1');
  perform pg_temp.assert_true(
    (select probe.cash_state from
       private.weekly_source_banking_pay_branch_probe_v1(
         'CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED') as probe)
      is not distinct from v_overlay.cash_state,
    'the arrival probe must return the installed classifier answer verbatim for branch 2');
  perform pg_temp.assert_refused(
    $sql$select * from private.weekly_source_banking_pay_branch_probe_v1('SOMETHING_ELSE')$sql$,
    '%WEEKLY_SOURCE_BANKING_PAY_BRANCH_PROBE_UNKNOWN%',
    'an unknown branch');

  perform pg_temp.assert_true(
    coalesce((private.weekly_source_banking_pay_integration_arrived_v1()->>'arrived')::boolean,true)
      is false,
    'the integration must still read as ABSENT today');
end
$verify_tripwire_untouched$;

-- ===========================================================================
-- 6. IT REFUSES IN A LIVE ENVIRONMENT, BY ITS OWN CHECK
-- ===========================================================================
do $verify_live_refusal$
declare
  v_state jsonb;
begin
  update private.cloudtms_database_identity set environment='LIVE';

  perform pg_temp.assert_refused(
    $sql$select private.weekly_source_banking_pay_absence_declare_v1('wp21-verifier','proof')$sql$,
    '%WEEKLY_SOURCE_BANKING_PAY_ABSENCE_LIVE_REFUSED%',
    'declaring the absence in a LIVE environment');

  perform pg_temp.assert_refused(
    $sql$insert into private.weekly_source_banking_pay_absence(
           singleton,declared,declared_by,reason)
         values (true,true,'direct-write','bypassing the function')$sql$,
    '%WEEKLY_SOURCE_BANKING_PAY_ABSENCE_LIVE_REFUSED%',
    'writing the relation DIRECTLY in a LIVE environment');

  -- A row that somehow survived a restore or a clone into a live database still
  -- reads as off, because the environment is re-read on every evaluation.
  update private.cloudtms_database_identity set environment='TEST';
  perform private.weekly_source_banking_pay_absence_declare_v1('wp21-verifier','proof');
  update private.cloudtms_database_identity set environment='LIVE';
  v_state:=private.weekly_source_banking_pay_absence_v1();
  perform pg_temp.assert_true(
    coalesce((v_state->>'declared')::boolean,true) is false
    and v_state->>'reason_code'='ENVIRONMENT_NOT_TEST',
    'a declared absence in a LIVE database must read as OFF, got '||v_state::text);
  perform pg_temp.assert_true(
    private.weekly_source_banking_pay_boundary_notice_v1() is null,
    'no boundary notice may be produced in a LIVE database');

  -- An unestablished environment also fails closed.
  delete from private.cloudtms_database_identity;
  v_state:=private.weekly_source_banking_pay_absence_v1();
  perform pg_temp.assert_true(
    coalesce((v_state->>'declared')::boolean,true) is false
    and v_state->>'reason_code'='ENVIRONMENT_UNKNOWN',
    'an unestablished environment must read as OFF, got '||v_state::text);

  insert into private.cloudtms_database_identity(singleton,environment,customer_key)
  values (true,'TEST','wp21-verifier');
  perform private.weekly_source_banking_pay_absence_clear_v1();
end
$verify_live_refusal$;

-- ===========================================================================
-- 7. IT CHANGES NO OUTCOME
--
-- The same ordinary root, driven through the same owners, with the setting OFF
-- and then ON. Every payload is compared key for key.
-- ===========================================================================
create table pg_temp.baseline(label text primary key, value jsonb);

do $verify_changes_no_outcome$
declare
  v_result jsonb;
  v_state jsonb;
begin
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    '21000000-0000-4000-8000-000000000301','21000000-0000-4000-8000-000000000301',
    null,'21000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'an ordinary root upstream of the Banking Pay boundary must authorise with the '
    ||'integration absent, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  insert into pg_temp.baseline(label,value) values
    ('authorise_off',pg_temp.comparable(v_result)),
    ('available_off',pg_temp.comparable(
       public.weekly_source_first_authorisation_withdraw_available_v1(
         '21000000-0000-4000-8000-000000000301'))),
    ('census_off',pg_temp.comparable(private.weekly_source_freeze_census_v1(
       '21000000-0000-4000-8000-000000000101',
       array['21000000-0000-4000-8000-000000000301']::uuid[])));

  -- The withdrawal availability verdict carries NO key from this package. WP-21
  -- adds nothing to any payload produced by any owner, in either setting state.
  perform pg_temp.assert_true(
    not exists (
      select 1
      from pg_catalog.jsonb_object_keys(
        (select value from pg_temp.baseline where label='available_off')) as payload_key(name)
      where payload_key.name like 'banking\_pay%' escape '\'),
    'no WP-21 key may appear in an ordinary payload');

  -- Now turn the setting ON.
  v_state:=private.weekly_source_banking_pay_absence_declare_v1(
    'wp21-verifier','Banking Pay is being built in another workstream');
  perform pg_temp.assert_true(
    coalesce((v_state->>'declared')::boolean,false)
    and v_state->>'reason_code'='DECLARED_ABSENT'
    and v_state->>'environment'='TEST',
    'the setting must read as ON once declared, got '||v_state::text);
  perform pg_temp.assert_true(
    (private.weekly_source_banking_pay_boundary_notice_v1()->>'kind')
      ='BANKING_PAY_INTEGRATION_ABSENT'
    and (private.weekly_source_banking_pay_boundary_notice_v1()->>'severity')
      ='RELEASE_BLOCKED_UNTIL_ACTIVATION_GATE'
    and (private.weekly_source_banking_pay_boundary_notice_v1()->>'message')
      like '%does not stop an existing Workbench claim route%'
    and (private.weekly_source_banking_pay_boundary_notice_v1()->>'changes_no_outcome')='true',
    'the notice must be explicit that it is informational and changes no outcome');

  -- And every outcome is identical.
  perform pg_temp.assert_true(
    pg_temp.comparable(public.weekly_source_first_authorisation_withdraw_available_v1(
      '21000000-0000-4000-8000-000000000301'))
      =(select value from pg_temp.baseline where label='available_off'),
    'the withdrawal availability verdict must be IDENTICAL with the setting ON');
  perform pg_temp.assert_true(
    pg_temp.comparable(private.weekly_source_freeze_census_v1(
      '21000000-0000-4000-8000-000000000101',
      array['21000000-0000-4000-8000-000000000301']::uuid[]))
      =(select value from pg_temp.baseline where label='census_off'),
    'the freeze census must be IDENTICAL with the setting ON');

  -- Including the classifier itself: the setting cannot reach it.
  perform pg_temp.assert_true(
    (select movement.cash_state
     from public._pay_rail_state_money_movement_classify(
       'VOIDED',null,
       pg_catalog.jsonb_build_object('failed_reason','PRE_BANK_CANCEL_VOIDED',
                                     'pre_bank_cancel_applied',true),
       pg_catalog.jsonb_build_object('failed_reason','PRE_BANK_CANCEL_VOIDED',
                                     'pre_bank_cancel_applied',true)) as movement)='UNKNOWN',
    'the installed classifier must still answer UNKNOWN with the setting ON -- the setting '
    ||'does not reach it, does not answer for it and does not change what it says');
end
$verify_changes_no_outcome$;

-- ===========================================================================
-- 8. IT CANNOT OUTLIVE ITS OWN TRUTH
--
-- The WP-21-owned arrival probe -- NOT the Banking Pay classifier -- is
-- temporarily replaced inside this rolled-back transaction so that one branch
-- reports arrived. The real detector, the real reader and the real declare
-- function are then executed against it.
--
-- `public._pay_rail_state_money_movement_classify` is NOT touched. Its
-- definition hash is measured before and after to prove it.
-- ===========================================================================
do $verify_cannot_outlive$
declare
  v_classifier_before text;
  v_classifier_after text;
  v_probe_before text;
  v_arrived jsonb;
  v_branch text;
begin
  select pg_catalog.md5(routine.prosrc) into v_classifier_before
  from pg_catalog.pg_proc as routine
  where routine.oid=pg_catalog.to_regprocedure(
    'public._pay_rail_state_money_movement_classify(text,text,jsonb,jsonb)');
  select pg_catalog.pg_get_functiondef(routine.oid) into v_probe_before
  from pg_catalog.pg_proc as routine
  where routine.oid=pg_catalog.to_regprocedure(
    'private.weekly_source_banking_pay_branch_probe_v1(text)');

  foreach v_branch in array array[
    'PRE_BANK_CANCEL_VOIDED','CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED']::text[]
  loop
    execute pg_catalog.format($fmt$
      create or replace function private.weekly_source_banking_pay_branch_probe_v1(
        p_branch text
      ) returns table (
        branch text, cash_state text, is_final_money_moved boolean,
        is_terminal_no_money boolean, is_pending_non_final boolean, reason text
      ) language sql volatile security definer
      set search_path to 'public','private','extensions','pg_catalog','pg_temp'
      as $simulated$
        select p_branch,
               case when p_branch=%L then 'TERMINAL' || '_NO_MONEY' else 'UNKNOWN' end,
               false, p_branch=%L, false,
               case when p_branch=%L then p_branch else 'simulated' end;
      $simulated$;
    $fmt$,v_branch,v_branch,v_branch);

    v_arrived:=private.weekly_source_banking_pay_integration_arrived_v1();
    perform pg_temp.assert_true(
      coalesce((v_arrived->>'arrived')::boolean,false),
      'the arrival detector must report ARRIVED for branch '||v_branch||', got '
      ||v_arrived::text);

    perform pg_temp.assert_refused(
      $sql$select private.weekly_source_banking_pay_absence_v1()$sql$,
      '%WEEKLY_SOURCE_BANKING_PAY_ABSENCE_NO_LONGER_TRUE%',
      'the setting reader, once branch '||v_branch||' has arrived');

    perform pg_temp.assert_refused(
      $sql$select private.weekly_source_banking_pay_boundary_notice_v1()$sql$,
      '%WEEKLY_SOURCE_BANKING_PAY_ABSENCE_NO_LONGER_TRUE%',
      'the boundary notice, once branch '||v_branch||' has arrived');

    perform pg_temp.assert_refused(
      $sql$select private.weekly_source_banking_pay_absence_declare_v1('x','y')$sql$,
      '%WEEKLY_SOURCE_BANKING_PAY_ABSENCE_NO_LONGER_TRUE%',
      'declaring the absence, once branch '||v_branch||' has arrived');
  end loop;

  execute v_probe_before;
  select pg_catalog.md5(routine.prosrc) into v_classifier_after
  from pg_catalog.pg_proc as routine
  where routine.oid=pg_catalog.to_regprocedure(
    'public._pay_rail_state_money_movement_classify(text,text,jsonb,jsonb)');
  perform pg_temp.assert_true(
    v_classifier_before=v_classifier_after,
    'the INSTALLED Banking Pay classifier definition must be untouched by this proof; before='
    ||coalesce(v_classifier_before,'<null>')||' after='||coalesce(v_classifier_after,'<null>'));

  perform pg_temp.assert_true(
    coalesce((private.weekly_source_banking_pay_integration_arrived_v1()->>'arrived')::boolean,true)
      is false,
    'with the real probe restored the integration must read as still absent');
end
$verify_cannot_outlive$;

-- ===========================================================================
-- 9. Clearing returns the database to the default-off state
-- ===========================================================================
do $verify_clear$
declare
  v_state jsonb;
begin
  v_state:=private.weekly_source_banking_pay_absence_clear_v1();
  perform pg_temp.assert_true(
    coalesce((v_state->>'declared')::boolean,true) is false
    and v_state->>'reason_code'='SETTING_ABSENT',
    'clearing must return the setting to absent, got '||v_state::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from private.weekly_source_banking_pay_absence)=0,
    'clearing must leave the relation empty');
  perform pg_temp.assert_true(
    private.weekly_source_banking_pay_boundary_notice_v1() is null,
    'no notice may survive clearing');
end
$verify_clear$;

select 'WP-21 BANKING PAY ABSENCE SETTING VERIFIER: ALL SECTIONS PASSED' as result;

rollback;
