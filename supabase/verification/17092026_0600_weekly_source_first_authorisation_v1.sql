-- Rollback-only PostgreSQL 17 proof for the Plan 6.2 Gate 3 first-authorisation
-- and withdrawal owners.
--
-- Covers, in order:
--   1. structure, ownership, security and privileges of every function the
--      package adds, plus the four illegal pg_catalog conditional prefixes;
--   2. interface I-6 and the Office entry point: the serial-gate mapping, the
--      stale refusal of proof/34 section 5 step 3 (ROT-001), generation 1 of
--      proof/34 section 4, and the already-authorised refusal;
--   3. UNA-001 and UNA-011: authorise, withdraw, re-authorise; family, physical
--      id and version identical, the withdrawn generation keeping its own
--      signature, generation N+1 appended;
--   4. UNA-017: one transaction token, one queued job, one complete-scope
--      result, the head cleared, and a forced invalidator failure rolling the
--      unauthorisation, the head clearing, the protected withdrawal and the
--      audit entry back together;
--   5. UNA-018: the exact replay returns the recorded result and calls nothing;
--      an old physical id of the family only locates the family;
--   6. UNA-004, UNA-005, UNA-006, UNA-007, UNA-008, UNA-009 and UNA-016: every
--      W1 to W9 refusal with its exact code and its permanent or temporary
--      nature;
--   7. UNA-002: the approved protected-hours decision marked withdrawn, never
--      deleted;
--   8. UNA-003: a Workbench result with no Draft is allowed;
--   9. UNA-010: a direct call while the control is unavailable is refused with
--      no lifecycle write and an audit row, and the availability function gives
--      the same verdict as the write path;
--  10. UNA-012: an ordinary, unmanaged Timesheet is refused by this owner and
--      the unchanged ordinary owner's definition is untouched;
--  11. HANDOVER 2 round 4 ruling 4 (an unmarked VOIDED transfer stays UNKNOWN
--      and can neither release nor permit withdrawal; a partial cancellation
--      left PENDING refuses) and ruling 5 item 5 (a reservation released with
--      reason WRITE_OFF is not evidence of a completed cancellation).
--
-- WP-07b added, closing the findings of the independent WP-07 review; items 12
-- and 13 were rewritten by WP-25 on 18 September 2026 because both described a
-- rule that no longer holds — see the note under item 19:
--  12. RULING A3 (section 11): a root that carries a COMMITTED entitlement head
--      IS withdrawable once the checks prove no payment work and no ambiguous
--      money effect has crossed the boundary, and the head is SUPERSEDED
--      atomically with the withdrawal, with an explicit withdrawal reason, an
--      immutable predecessor link and a durable receipt; reauthorisation creates
--      a new generation and never revives it, and a head that is already
--      superseded is history and refuses nothing.  Where a payment effect HAS
--      crossed, withdrawal is refused and no head is touched.  Decision D8's
--      same-statement head clearing is proved by EXECUTING the case ruling A3
--      restored, under the check constraint that makes a two-statement version
--      impossible.
--  13. the ordering rule of WP-03 handoff N16 (section 16): the two writes stay
--      in one transaction, with no EXCEPTION handler and no transaction control.
--      A managed-root guard trigger IS now attached to public.timesheets
--      (WP-24, `17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql`),
--      which is exactly the ruling OR-2 condition that makes the ordering
--      mandatory, so the withdrawal owner writes its withdrawal marks BEFORE it
--      calls public.timesheet_unauthorise_atomic.  Section 16 detects such a
--      trigger through the transitive closure of the routines it reaches — not
--      by a single-level name match, which missed this one — and then proves the
--      ordering is in force by execution, because the guard itself would refuse
--      the wrong order.
--  14. F2 (section 17): interface I-6's already-authorised test is family-wide,
--      so neither entry point can give a rotated family a second live
--      generation.
--  15. F3 and F4 (section 18): `ok` means the same thing everywhere in the
--      package, and the exact replay's declared dependence on audit retention
--      degrades to a refusal and never re-executes.
--  16. F5 (section 19): W4's WRITE_OFF refusal is declared at its own site as a
--      fail-closed addition stricter than the pack.
--
-- WP-25, 18 September 2026 — what changed and why.  Sections 11 and 16 of this
-- file each carried an assertion justified by a rule that had been overturned,
-- and this header stated both overturned rules as if they were the contract.
--
--   * Item 12 described WP-07b's permanent refusal.  HANDOVER 2 round-5 ruling
--     A3 REJECTED that as a product rule ("Option (a) is rejected as a permanent
--     product rule … the withdrawal owner must supersede the committed head
--     atomically with the withdrawal").  Section 11 had already been rewritten
--     to the ruling and proves it by execution; only the header and the D8
--     side-assertion still spoke the old rule.  The D8 side-assertion read the
--     owner's TEXT, and its stated justification was that ruling A3's case
--     "cannot be reached".  It can; section 11 reaches it.  Replaced with the
--     executed proof below.
--   * Item 13 and section 16 asserted that NO managed-root guard trigger exists
--     on the lifecycle relations.  One does, and has since WP-24 landed.  The
--     detector missed it because it matched the trigger function's own text for
--     `weekly_source_managed_root_guard`, and WP-24's trigger reaches that guard
--     one call further on.  Executed on a clean build from empty, the old query
--     returned NULL while `timesheets.weekly_source_managed_root_authorisation_
--     guard_bu` was attached — a tripwire reporting all clear on the condition it
--     exists to catch.  Replaced with a transitive-closure detector and an
--     assertion that states the ordering consequence and proves it by execution.
--
-- Nothing about the owners changed; both were already correct.  What changed is
-- that this file now asserts what they actually do.
--
-- Prerequisites: the Weekly Source schema migration, the ACL contract, the
-- rotation authority (WP-03, interface I-1), the freeze census (WP-08a,
-- interface I-2) and this package's repeatable.
--
-- Nothing here defines, wraps or re-creates a Banking Pay, Draft, execution,
-- cancellation, settlement, provider, recovery or remittance owner, and nothing
-- is written outside the rolled-back transaction.

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

-- The installed Workbench dirty trigger queues a job for every Candidate this
-- fixture touches, and the installed Candidate serial gate then reports
-- CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_CONTINUATION.  In production the Workbench
-- worker drains those jobs; inside one rolled-back transaction nothing does, so
-- the fixture drains them itself.  This is a fixture action on fixture rows: it
-- reproduces the worker's completion, changes no Banking Pay definition and is
-- never performed by an owner.
create function pg_temp.drain_workbench_jobs() returns void
language sql as $function$
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
$function$;

create function pg_temp.current_signature(p_timesheet_id uuid) returns text
language sql stable as $function$
  select nullif(pg_catalog.btrim(coalesce(
           signature->>'backend_row_signature',signature->>'row_signature','')),'')
  from public.timesheet_lifecycle_guard_signature_v1(
    p_timesheet_id,
    (select contract_week.id from public.contract_weeks contract_week
      where contract_week.timesheet_id=p_timesheet_id),
    false) as signature;
$function$;

create function pg_temp.seed_timesheet(
  p_timesheet_id uuid,p_booking_id text,p_version integer,p_is_current boolean,
  p_contract_id uuid
) returns uuid language sql as $function$
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
    line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
    qr_payload_json,is_adjustment,created_at,updated_at
  ) values (
    p_timesheet_id,p_booking_id,p_version,p_is_current,
    'RECEIVED'::public.timesheet_status_enum,
    'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
    'HOURS'::public.timesheet_line_type_enum,'wp07-occupant','wp07-hospital',
    'wp07-ward','wp07-role','weekly-0','2026-09-13',p_contract_id,
    '[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) returning timesheet_id;
$function$;

create function pg_temp.seed_week_and_financials(
  p_contract_week_id uuid,p_contract_id uuid,p_timesheet_id uuid,
  p_financials_id uuid,p_candidate_id uuid,p_client_id uuid,p_version integer
) returns void language sql as $function$
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    timesheet_id,is_adjustment
  ) values (
    p_contract_week_id,p_contract_id,'2026-09-13',0,
    'SUBMITTED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,p_timesheet_id,false);
  insert into public.timesheets_financials(
    id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat
  ) values (
    p_financials_id,p_timesheet_id,p_version,true,p_candidate_id,p_client_id,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,10,100,200);
$function$;

-- ---------------------------------------------------------------------------
-- Fixture world: one Client and ten Candidates, one Contract each, one Weekly
-- HOURS Timesheet family each.  Nothing here carries financial state until a
-- section adds it.
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('b7000000-0000-4000-8000-000000000001','wp07-office@example.test','admin',true,'not-a-login');
insert into public.tms_users(id,email,role,is_active,password_hash)
values ('b7000000-0000-4000-8000-00000000000e','wp07-inactive@example.test','admin',false,'not-a-login');
insert into public.clients(id,name) values ('b7000000-0000-4000-8000-000000000002','WP07 Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('b7000000-0000-4000-8000-000000000002',20,'2026-01-01');

do $seed_world$
declare
  v_index integer;
  v_candidate uuid;
  v_contract uuid;
begin
  for v_index in 1..10 loop
    v_candidate:=('b7000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_contract:=('b7000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    insert into public.candidates(id,display_name)
    values (v_candidate,'WP07 Candidate '||v_index);
    insert into public.contracts(
      id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
      weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
    ) values (
      v_contract,v_candidate,'b7000000-0000-4000-8000-000000000002',
      '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);
  end loop;
end
$seed_world$;

-- One family per Candidate.  Candidate 6 carries a rotated family (v1 demoted,
-- v2 current) so an old physical id can be presented.
do $seed_families$
declare
  v_index integer;
  v_timesheet uuid;
begin
  for v_index in 1..10 loop
    v_timesheet:=('b7000000-0000-4000-8000-0000000003'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    if v_index=6 then
      perform pg_temp.seed_timesheet(
        'b7000000-0000-4000-8000-000000000396','WP07-BK-06',1,false,
        'b7000000-0000-4000-8000-000000000206');
      perform pg_temp.seed_timesheet(
        v_timesheet,'WP07-BK-06',2,true,'b7000000-0000-4000-8000-000000000206');
      perform pg_temp.seed_week_and_financials(
        ('b7000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        ('b7000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_timesheet,
        ('b7000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        ('b7000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        'b7000000-0000-4000-8000-000000000002',2);
    else
      perform pg_temp.seed_timesheet(
        v_timesheet,'WP07-BK-'||pg_catalog.lpad(v_index::text,2,'0'),1,true,
        ('b7000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid);
      perform pg_temp.seed_week_and_financials(
        ('b7000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        ('b7000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_timesheet,
        ('b7000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        ('b7000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        'b7000000-0000-4000-8000-000000000002',1);
    end if;
  end loop;
end
$seed_families$;

select pg_temp.drain_workbench_jobs();

-- ---------------------------------------------------------------------------
-- 1. Structure, ownership, security, privileges
-- ---------------------------------------------------------------------------
do $verify_structure$
declare
  v_name text;
  v_definition text;
  v_prefix text;
begin
  foreach v_name in array array[
    'private.weekly_source_withdrawal_uuid_array_v1(jsonb)',
    'private.weekly_source_withdrawal_transfer_scope_v1(uuid,uuid[])',
    'private.weekly_source_first_authorisation_context_v1(uuid)',
    'private.weekly_source_first_authorisation_withdraw_checks_v1(jsonb,jsonb,uuid,text)',
    'private.weekly_source_first_authorisation_withdrawal_recorded_v1(uuid,text,uuid)',
    'private.weekly_source_withdrawal_canonical_request_v1(uuid,uuid,uuid,uuid,text,integer,uuid,uuid,uuid,integer,text,uuid,bigint)',
    'private.weekly_source_root_authorisation_signature_v1(uuid,uuid,text,integer,integer,text,text[])',
    'private.weekly_source_root_protected_decision_hashes_v1(uuid[])',
    'private.weekly_source_root_agency_id_v1(uuid[])',
    'private.weekly_source_invalidation_contract_assert_v1(uuid,uuid[],uuid,text,uuid[])',
    'private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb)',
    'private.weekly_source_first_authorisation_withdraw_protected_v1(uuid[],uuid,timestamptz)',
    'public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)',
    'public.weekly_source_first_authorisation_withdraw_available_v1(uuid)',
    'public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)',
    'public.weekly_source_first_authorisation_withdraw_request_v1(jsonb)'
  ] loop
    perform pg_temp.assert_true(
      pg_catalog.to_regprocedure(v_name) is not null,
      v_name||' must exist with exactly that signature');
    perform pg_temp.assert_true(
      (select proowner::regrole::text from pg_catalog.pg_proc
        where oid=pg_catalog.to_regprocedure(v_name))='postgres',
      v_name||' must be owned by postgres');
    -- Every function that reads a relation is SECURITY DEFINER.  The pure
    -- UUID extractor reads nothing, so it stays SECURITY INVOKER and IMMUTABLE,
    -- exactly like the census's own extractor.  WP-07c's canonical-request
    -- builder is the same shape: it reads no relation, takes every field as an
    -- argument and returns the object the installed digest encoder hashes, so
    -- it must be IMMUTABLE too or the digest could vary between calls.
    if v_name in (
         'private.weekly_source_withdrawal_uuid_array_v1(jsonb)',
         'private.weekly_source_withdrawal_canonical_request_v1(uuid,uuid,uuid,uuid,text,integer,uuid,uuid,uuid,integer,text,uuid,bigint)',
         'private.weekly_source_root_authorisation_signature_v1(uuid,uuid,text,integer,integer,text,text[])'
       ) then
      perform pg_temp.assert_true(
        (select provolatile from pg_catalog.pg_proc
          where oid=pg_catalog.to_regprocedure(v_name))='i'
        and not (select prosecdef from pg_catalog.pg_proc
                 where oid=pg_catalog.to_regprocedure(v_name)),
        v_name||' must be IMMUTABLE and SECURITY INVOKER');
    else
      perform pg_temp.assert_true(
        (select prosecdef from pg_catalog.pg_proc
          where oid=pg_catalog.to_regprocedure(v_name)),
        v_name||' must be SECURITY DEFINER');
    end if;
    perform pg_catalog.has_function_privilege('anon',pg_catalog.to_regprocedure(v_name),'EXECUTE');
    perform pg_temp.assert_true(
      not pg_catalog.has_function_privilege('anon',pg_catalog.to_regprocedure(v_name),'EXECUTE')
      and not pg_catalog.has_function_privilege(
        'authenticated',pg_catalog.to_regprocedure(v_name),'EXECUTE'),
      v_name||' must not be executable by a browser role');

    -- Workspace rule: COALESCE, NULLIF, LEAST and GREATEST are syntax, never
    -- pg_catalog functions.  A definition that carries one compiles and then
    -- fails with 42883 at first execution.
    v_definition:=pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(v_name));
    foreach v_prefix in array array[
      'pg_catalog.coalesce','pg_catalog.nullif','pg_catalog.least','pg_catalog.greatest'
    ] loop
      perform pg_temp.assert_true(
        pg_catalog.strpos(pg_catalog.lower(v_definition),v_prefix)=0,
        v_name||' must not use '||v_prefix);
    end loop;
  end loop;

  -- The three service entry points are callable by service_role only.
  foreach v_name in array array[
    'public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)',
    'public.weekly_source_first_authorisation_withdraw_available_v1(uuid)',
    'public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)',
    'public.weekly_source_first_authorisation_withdraw_request_v1(jsonb)'
  ] loop
    perform pg_temp.assert_true(
      pg_catalog.has_function_privilege(
        'service_role',pg_catalog.to_regprocedure(v_name),'EXECUTE'),
      v_name||' must be executable by service_role');
  end loop;

  -- The two read-only companions must be STABLE, so neither can write.
  perform pg_temp.assert_true(
    (select provolatile from pg_catalog.pg_proc
      where oid=pg_catalog.to_regprocedure(
        'public.weekly_source_first_authorisation_withdraw_available_v1(uuid)'))='s',
    'the availability function must be STABLE');
  perform pg_temp.assert_true(
    (select provolatile from pg_catalog.pg_proc
      where oid=pg_catalog.to_regprocedure(
        'private.weekly_source_first_authorisation_withdraw_checks_v1(jsonb,jsonb,uuid,text)'))='s',
    'the W1 to W11 checks must be STABLE');

  -- Interface I-6's fixed signature, so WP-02's coordinator cannot drift.
  perform pg_temp.assert_true(
    pg_catalog.to_regprocedure(
      'private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb)') is not null,
    'interface I-6 must keep its fixed signature');

  -- The unchanged owners this package only calls.
  perform pg_temp.assert_true(
    pg_catalog.to_regprocedure(
      'public.timesheet_unauthorise_atomic(uuid,uuid,uuid,timestamptz,text)') is not null
    and pg_catalog.to_regprocedure(
      'public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text)') is not null,
    'the two ordinary lifecycle owners must be present and unchanged');
end
$verify_structure$;

-- ---------------------------------------------------------------------------
-- 2. Interface I-6 and the Office first-authorisation entry point
--    (proof/34 section 5 steps 2 and 3; section 4's write set; ROT-001)
-- ---------------------------------------------------------------------------
do $verify_first_authorise$
declare
  v_result jsonb;
  v_lock jsonb;
  v_row public.weekly_source_root_authorisations%rowtype;
begin
  -- A browser role can never reach the owner.
  perform pg_catalog.set_config('request.jwt.claim.role','authenticated',true);
  perform pg_temp.assert_refused(
    $sql$select public.weekly_source_first_authorise_v1(
      'b7000000-0000-4000-8000-000000000301','b7000000-0000-4000-8000-000000000301',
      null,'b7000000-0000-4000-8000-000000000001')$sql$,
    'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED',
    'first authorisation by a browser role');
  perform pg_catalog.set_config('request.jwt.claim.role','service_role',true);

  -- ROT-001: the Timesheet rotated before first authorisation completes.  The
  -- older physical id is never authorised and nothing is written.
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000396','b7000000-0000-4000-8000-000000000396',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
    'ROT-001: an old physical id must be refused as stale, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations)=0,
    'ROT-001: the stale path must write no authorisation row');
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server is null from public.timesheets timesheet_row
      where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000396'),
    'ROT-001: the older version must stay unauthorised');
  perform pg_temp.drain_workbench_jobs();

  -- The same request against the canonical current version succeeds.
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000306','b7000000-0000-4000-8000-000000000306',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and v_result->>'gate'='GRANTED'
    and (v_result->>'authorisation_generation')::integer=1,
    'the canonical current version must authorise, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  -- proof/34 section 4's exact write set for generation 1.
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000301','b7000000-0000-4000-8000-000000000301',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false),
    'Candidate 1 must authorise, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  select * into v_row from public.weekly_source_root_authorisations
   where root_timesheet_id='b7000000-0000-4000-8000-000000000301';
  perform pg_temp.assert_true(
    v_row.family_booking_id='WP07-BK-01'
    and v_row.timesheet_version=1
    and v_row.authorisation_generation=1
    and v_row.current_entitlement_head_id is null
    and v_row.withdrawn_at_utc is null
    and v_row.authorised_by_user_id='b7000000-0000-4000-8000-000000000001'
    and v_row.authorised_row_signature
        =pg_temp.current_signature('b7000000-0000-4000-8000-000000000301'),
    'generation 1 must carry the family, the physical version, the actor and the row '
    ||'signature AT authorisation');

  -- The ordinary owner really ran: the Timesheet, its TSFIN and the Contract
  -- Week all moved, and Weekly Source wrote none of them.
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server is not null from public.timesheets timesheet_row
      where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000301')
    and (select financial_row.authorised_at_utc is not null
         from public.timesheets_financials financial_row
         where financial_row.timesheet_id='b7000000-0000-4000-8000-000000000301'
           and financial_row.is_current)
    and (select contract_week.status='AUTHORISED'::public.contract_week_status_enum
         from public.contract_weeks contract_week
         where contract_week.id='b7000000-0000-4000-8000-000000000401'),
    'the unchanged ordinary Authorise owner must have done the lifecycle work');

  -- A second authorisation of a live root is refused with no write.
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000301','b7000000-0000-4000-8000-000000000301',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_ALREADY_AUTHORISED',
    'a live root must not be authorised twice, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='b7000000-0000-4000-8000-000000000301')=1,
    'the refused second authorisation must write no second generation');
  perform pg_temp.drain_workbench_jobs();

  -- Interface I-6 called directly: the lock result is the only proof that the
  -- locks are held, so an absent or non-GRANTED one is refused before any write.
  v_result:=private.weekly_source_first_authorise_core_v1(
    'b7000000-0000-4000-8000-000000000302',null,
    'b7000000-0000-4000-8000-000000000001','{}'::jsonb);
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_FIRST_AUTHORISE_LOCK_RESULT_INVALID',
    'I-6 must refuse an invalid lock result, got '||v_result::text);

  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    'b7000000-0000-4000-8000-000000000102',
    array['b7000000-0000-4000-8000-000000000302']::uuid[],
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
    pg_catalog.gen_random_uuid(),'WP07-VERIFIER');
  perform pg_temp.assert_true(
    coalesce((v_lock->>'ok')::boolean,false) and v_lock->>'gate'='GRANTED',
    'interface I-1 must grant for Candidate 2, got '||v_lock::text);

  v_result:=private.weekly_source_first_authorise_core_v1(
    'b7000000-0000-4000-8000-000000000302',null,
    'b7000000-0000-4000-8000-00000000000e',v_lock);
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_FIRST_AUTHORISE_ACTOR_INVALID',
    'I-6 must refuse an inactive actor, got '||v_result::text);

  -- I-6 refuses a request whose lock result proves a non-canonical row.
  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    'b7000000-0000-4000-8000-000000000106',
    array['b7000000-0000-4000-8000-000000000396']::uuid[],
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
    pg_catalog.gen_random_uuid(),'WP07-VERIFIER');
  v_result:=private.weekly_source_first_authorise_core_v1(
    'b7000000-0000-4000-8000-000000000396',null,
    'b7000000-0000-4000-8000-000000000001',v_lock);
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
    'I-6 must refuse a non-canonical row from the lock result, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_first_authorise$;

-- ---------------------------------------------------------------------------
-- 3. UNA-001, UNA-011 and UNA-017: withdraw, then re-authorise
-- ---------------------------------------------------------------------------
do $verify_withdraw_happy$
declare
  v_available jsonb;
  v_result jsonb;
  v_generation1 public.weekly_source_root_authorisations%rowtype;
  v_generation2 public.weekly_source_root_authorisations%rowtype;
  v_signature text;
  v_items_before bigint;
  v_reservations_before bigint;
  v_transfers_before bigint;
  v_adjustments_before bigint;
  v_tokens integer;
  v_jobs integer;
begin
  select pg_catalog.count(*) into v_items_before from public.pay_batch_items;
  select pg_catalog.count(*) into v_reservations_before from public.pay_advance_reservations;
  select pg_catalog.count(*) into v_transfers_before from public.pay_bank_transfers;
  select pg_catalog.count(*) into v_adjustments_before from public.ts_pay_adjustments;

  select * into v_generation1 from public.weekly_source_root_authorisations
   where root_timesheet_id='b7000000-0000-4000-8000-000000000301';

  -- G3-4: the screens' verdict equals the write path's verdict.
  v_available:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000301');
  perform pg_temp.assert_true(
    coalesce((v_available->>'available')::boolean,false)
    and v_available->>'code' is null
    and pg_catalog.jsonb_array_length(v_available->'checks')=11,
    'UNA-001: the control must be available with all eleven checks recorded, got '
    ||v_available::text);

  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000301');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000301','b7000000-0000-4000-8000-000000000301',
    v_signature,'b7000000-0000-4000-8000-000000000001');

  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and coalesce((v_result->>'withdrawn')::boolean,false)
    and coalesce((v_result->>'replayed')::boolean,true) is false,
    'UNA-001: the withdrawal must succeed, got '||v_result::text);

  -- proof/36 section 5 step 1: the Timesheet is back to Awaiting authorisation.
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server is null
       and timesheet_row.revoked_reason='TIMESHEET_UNAUTHORISE'
     from public.timesheets timesheet_row
     where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000301')
    and (select financial_row.authorised_at_utc is null
         from public.timesheets_financials financial_row
         where financial_row.timesheet_id='b7000000-0000-4000-8000-000000000301'
           and financial_row.is_current)
    and (select contract_week.status='SUBMITTED'::public.contract_week_status_enum
         from public.contract_weeks contract_week
         where contract_week.id='b7000000-0000-4000-8000-000000000401'),
    'UNA-001: the unchanged owner must have returned the week to SUBMITTED');

  -- proof/36 section 5 step 2: no financial row of any kind is created.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.pay_batch_items)=v_items_before
    and (select pg_catalog.count(*) from public.pay_advance_reservations)=v_reservations_before
    and (select pg_catalog.count(*) from public.pay_bank_transfers)=v_transfers_before
    and (select pg_catalog.count(*) from public.ts_pay_adjustments)=v_adjustments_before,
    'UNA-001: the withdrawal must create no Banking Pay or adjustment row');

  -- proof/36 section 5 step 6: the live generation is marked withdrawn and the
  -- head pointer cleared; family, physical id, version and signature untouched.
  select * into v_generation2 from public.weekly_source_root_authorisations
   where id=v_generation1.id;
  perform pg_temp.assert_true(
    v_generation2.withdrawn_at_utc is not null
    and v_generation2.withdrawn_by_user_id='b7000000-0000-4000-8000-000000000001'
    and v_generation2.current_entitlement_head_id is null
    and v_generation2.root_timesheet_id=v_generation1.root_timesheet_id
    and v_generation2.family_booking_id=v_generation1.family_booking_id
    and v_generation2.timesheet_version=v_generation1.timesheet_version
    and v_generation2.authorised_row_signature=v_generation1.authorised_row_signature
    and v_generation2.authorisation_generation=1,
    'UNA-011: the withdrawn generation keeps its identity and gains only the marks');

  -- UNA-017: one token, one queued job, one complete-scope result.
  perform pg_temp.assert_true(
    (v_result#>>'{invalidation_contract,complete_scope_job_count}')::integer=1
    and pg_catalog.jsonb_array_length(v_result#>'{invalidation_contract,jobs}')=1
    and (v_result#>>'{invalidation,candidate_count}')::integer=1
    and (v_result->>'scope_change_tx_token')::uuid
        =(v_result#>>'{invalidation_contract,scope_change_tx_token}')::uuid,
    'UNA-017: exactly one token and one complete-scope job, got '
    ||(v_result->'invalidation_contract')::text);

  select pg_catalog.count(*)::integer into v_jobs
  from public.banking_pay_workbench_jobs
  where status in ('QUEUED','RUNNING')
    and candidate_id='b7000000-0000-4000-8000-000000000101';
  perform pg_temp.assert_true(
    v_jobs=1,
    'UNA-017: the ordinary Unauthorise trigger and the one aligned invalidation '
    ||'must coalesce into a single queued job, found '||v_jobs::text);

  select pg_catalog.count(*)::integer into v_tokens
  from public.banking_pay_scope_change_transactions
  where tx_token=(v_result->>'scope_change_tx_token')::uuid;
  perform pg_temp.assert_true(v_tokens=1,'UNA-017: exactly one scope-change token');

  -- proof/36 section 5 step 4: both events are visible in Audit.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text='b7000000-0000-4000-8000-000000000301'
        and action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN')=1,
    'UNA-001: the withdrawal must appear in Audit exactly once');

  perform pg_temp.drain_workbench_jobs();

  -- proof/36 section 5 step 8 and UNA-011: Office authorises the SAME Timesheet
  -- again; generation 2 is appended and generation 1 stays as history.
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000301','b7000000-0000-4000-8000-000000000301',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and (v_result->>'authorisation_generation')::integer=2,
    'UNA-011: re-authorisation must append generation 2, got '||v_result::text);

  select * into v_generation2 from public.weekly_source_root_authorisations
   where root_timesheet_id='b7000000-0000-4000-8000-000000000301'
     and authorisation_generation=2;
  perform pg_temp.assert_true(
    v_generation2.root_timesheet_id=v_generation1.root_timesheet_id
    and v_generation2.family_booking_id=v_generation1.family_booking_id
    and v_generation2.timesheet_version=v_generation1.timesheet_version
    and v_generation2.withdrawn_at_utc is null,
    'UNA-011: family, physical id and version are identical across the withdrawal');
  -- UNA-011: each generation records the row signature AT its own
  -- authorisation.  Inside one transaction the installed owners stamp
  -- `updated_at` from the transaction timestamp and the signature is a pure
  -- function of the row, so authorise → withdraw → re-authorise returns the
  -- identical value; the assertion is therefore that generation 2 equals the
  -- signature current at that moment, not that it differs from generation 1.
  perform pg_temp.assert_true(
    v_generation2.authorised_row_signature
      =pg_temp.current_signature('b7000000-0000-4000-8000-000000000301'),
    'UNA-011: the new generation records the row signature at ITS authorisation');
  perform pg_temp.assert_true(
    (select authorisation_row.authorised_row_signature
       from public.weekly_source_root_authorisations authorisation_row
      where authorisation_row.id=v_generation1.id)
    =v_generation1.authorised_row_signature,
    'UNA-011: the withdrawn generation keeps its original signature');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='b7000000-0000-4000-8000-000000000301')=2,
    'UNA-011: generation 1 survives as history');
  perform pg_temp.drain_workbench_jobs();
end
$verify_withdraw_happy$;

-- ---------------------------------------------------------------------------
-- Banking Pay evidence seeds for the refusal sections.
--
-- Under contract decision D2 these are fixtures in the EXISTING Banking Pay
-- evidence tables; Banking Pay's unfinished new logic is never exercised and no
-- result that depends on it is inferred.  The full real-owner library is
-- WP-16a's (tests/weekly-source/fixtures-banking); it needs several
-- transactions to drive the installed cancellation chain, which a rollback-only
-- verifier cannot provide, so the shapes W1 to W9 read are seeded here and the
-- end-to-end real-owner runs are recorded in the WP-07 report.
-- ---------------------------------------------------------------------------
create function pg_temp.seed_batch(
  p_batch_id uuid,p_status text,p_candidate_row_id uuid,p_candidate_id uuid,
  p_item_id uuid,p_timesheet_id uuid,p_is_voided boolean,
  p_execution_commit_state text default 'NOT_SUBMITTED',
  p_completed_at_utc timestamptz default null,
  p_cancelled_at_utc timestamptz default null,
  p_settlement_status text default null
) returns void language sql as $function$
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot,execution_commit_state,
    completed_at_utc,cancelled_at_utc,execution_committed_at_utc
  ) values (
    p_batch_id,'2026-09-18',p_status,'REVOLUT_API','SAGE','REVOLUT','SANDBOX',
    p_execution_commit_state,p_completed_at_utc,p_cancelled_at_utc,
    case when p_execution_commit_state='COMMITTED' then p_completed_at_utc end);
  insert into public.pay_batch_candidates(
    id,pay_batch_id,candidate_id,settlement_status,settled_at_utc
  ) values (
    p_candidate_row_id,p_batch_id,p_candidate_id,p_settlement_status,
    case when p_settlement_status='SETTLED' then p_completed_at_utc end);
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,pay_channel,timesheet_id,is_voided,
    amount_ex_vat,amount_inc_vat
  ) values (
    p_item_id,p_candidate_row_id,'TIMESHEET_PAYMENT','PAYE',p_timesheet_id,
    p_is_voided,100,100);
$function$;

-- ---------------------------------------------------------------------------
-- 4. UNA-004 and UNA-005 — a Banking Pay Draft, a reservation and a scheduled
--    batch: temporary refusals, nothing altered (proof/36 section 4 W3 to W5)
-- ---------------------------------------------------------------------------
do $verify_banking_active$
declare
  v_result jsonb;
  v_available jsonb;
  v_signature text;
  v_batch_before jsonb;
begin
  -- Candidate 2 is authorised first, while nothing blocks it.
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000302','b7000000-0000-4000-8000-000000000302',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 2 must authorise, got '||v_result::text);

  -- A live DRAFT batch holding a non-voided family item, with a RESERVED
  -- reservation on it.
  perform pg_temp.seed_batch(
    'b7000000-0000-4000-8000-000000000602','DRAFT',
    'b7000000-0000-4000-8000-000000000702','b7000000-0000-4000-8000-000000000102',
    'b7000000-0000-4000-8000-000000000802','b7000000-0000-4000-8000-000000000302',false);
  insert into public.pay_advances(
    id,candidate_id,client_id,reason,original_amount,outstanding_amount,case_type
  ) values (
    'b7000000-0000-4000-8000-000000000e02','b7000000-0000-4000-8000-000000000102',
    'b7000000-0000-4000-8000-000000000002','MANUAL_ADVANCE'::public.pay_advance_reason_enum,
    50,50,'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum);
  insert into public.pay_advance_reservations(
    id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
    reserved_amount,status
  ) values (
    'b7000000-0000-4000-8000-000000000902','b7000000-0000-4000-8000-000000000e02',
    'b7000000-0000-4000-8000-000000000602','b7000000-0000-4000-8000-000000000702',
    'b7000000-0000-4000-8000-000000000802',50,'RESERVED');
  perform pg_temp.drain_workbench_jobs();

  select pg_catalog.jsonb_build_object(
           'status',batch_row.status,'cancelled',batch_row.cancelled_at_utc,
           'items',(select pg_catalog.count(*) from public.pay_batch_items item_row
                    join public.pay_batch_candidates candidate_row
                      on candidate_row.id=item_row.pay_batch_candidate_id
                    where candidate_row.pay_batch_id=batch_row.id))
    into v_batch_before
  from public.pay_batches batch_row where batch_row.id='b7000000-0000-4000-8000-000000000602';

  v_available:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000302');
  perform pg_temp.assert_true(
    coalesce((v_available->>'available')::boolean,true) is false
    and v_available->>'code'='WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE'
    and v_available->>'refusal_nature'='TEMPORARY'
    and coalesce((v_available->>'retryable')::boolean,false),
    'UNA-004: the control must be unavailable and temporary, got '||v_available::text);

  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000302');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000302','b7000000-0000-4000-8000-000000000302',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE'
    and v_result->>'refusal_nature'='TEMPORARY',
    'UNA-004: the withdrawal must be refused temporarily, got '||v_result::text);

  -- W3, W4 and W5 must each report their own evidence.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from pg_catalog.jsonb_array_elements(v_result->'failed_checks') as failed(value)
      where failed.value->>'check' in ('W3','W4'))=2,
    'UNA-004: W3 and W4 must both fail, got '||(v_result->'failed_checks')::text);

  -- UNA-004: the Draft is untouched and the root stays authorised.
  perform pg_temp.assert_true(
    (select pg_catalog.jsonb_build_object(
              'status',batch_row.status,'cancelled',batch_row.cancelled_at_utc,
              'items',(select pg_catalog.count(*) from public.pay_batch_items item_row
                       join public.pay_batch_candidates candidate_row
                         on candidate_row.id=item_row.pay_batch_candidate_id
                       where candidate_row.pay_batch_id=batch_row.id))
       from public.pay_batches batch_row
      where batch_row.id='b7000000-0000-4000-8000-000000000602')=v_batch_before,
    'UNA-004: Weekly Source must not alter the Draft');
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server is not null from public.timesheets timesheet_row
      where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000302')
    and (select authorisation_row.withdrawn_at_utc is null
         from public.weekly_source_root_authorisations authorisation_row
         where authorisation_row.root_timesheet_id='b7000000-0000-4000-8000-000000000302'),
    'UNA-004: the root stays authorised after a refused withdrawal');

  -- UNA-010: the refused direct call leaves an audit row and no lifecycle write.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text='b7000000-0000-4000-8000-000000000302'
        and action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED')>=1,
    'UNA-010: a refused direct call must be audited');

  -- UNA-005: the same family with the batch moved to SCHEDULED and the item
  -- still live adds W5's own refusal.
  update public.pay_batches set status='SCHEDULED',schedule_kind='SCHEDULED',
         scheduled_at_utc=pg_catalog.clock_timestamp()
   where id='b7000000-0000-4000-8000-000000000602';
  v_available:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000302');
  perform pg_temp.assert_true(
    coalesce((v_available->>'available')::boolean,true) is false
    and (select pg_catalog.count(*)
           from pg_catalog.jsonb_array_elements(v_available->'failed_checks') as failed(value)
          where failed.value->>'check'='W5')=1,
    'UNA-005: a scheduled batch must fail W5, got '||(v_available->'failed_checks')::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_banking_active$;

-- ---------------------------------------------------------------------------
-- 5. UNA-006 — completed payment: permanently refused (W2)
-- ---------------------------------------------------------------------------
do $verify_paid$
declare
  v_result jsonb;
  v_signature text;
begin
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000303','b7000000-0000-4000-8000-000000000303',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 3 must authorise, got '||v_result::text);

  perform pg_temp.seed_batch(
    'b7000000-0000-4000-8000-000000000603','SETTLED',
    'b7000000-0000-4000-8000-000000000703','b7000000-0000-4000-8000-000000000103',
    'b7000000-0000-4000-8000-000000000803','b7000000-0000-4000-8000-000000000303',false,
    'COMMITTED',pg_catalog.clock_timestamp(),null,'SETTLED');
  -- The exact pair the settle rail writes: one history row and the per-batch
  -- frozen snapshot whose signature it copies (proof/32 section 5.2), so the
  -- census can class the item SETTLED_TERMINAL rather than CENSUS_ERROR.
  insert into public.pay_batch_timesheet_snapshots(
    pay_batch_id,timesheet_id,candidate_id,pay_channel,base_snapshot_json,
    target_snapshot_json,signature
  ) values (
    'b7000000-0000-4000-8000-000000000603','b7000000-0000-4000-8000-000000000303',
    'b7000000-0000-4000-8000-000000000103','PAYE','{}'::jsonb,'{}'::jsonb,
    'wp07-settled-signature');
  insert into public.timesheet_pay_state_history(
    timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
  ) values (
    'b7000000-0000-4000-8000-000000000303','b7000000-0000-4000-8000-000000000603',
    pg_catalog.clock_timestamp(),'{}'::jsonb,'wp07-settled-signature');
  insert into public.pay_bank_transfers(
    id,pay_batch_id,candidate_id,pay_channel,amount,status,rail_provider,rail_env,
    payee_entity_kind,completed_at_utc
  ) values (
    'b7000000-0000-4000-8000-000000000a03','b7000000-0000-4000-8000-000000000603',
    'b7000000-0000-4000-8000-000000000103','PAYE',100,'COMPLETED','REVOLUT','SANDBOX',
    'CANDIDATE',pg_catalog.clock_timestamp());
  update public.pay_batch_items set pay_bank_transfer_id='b7000000-0000-4000-8000-000000000a03'
   where id='b7000000-0000-4000-8000-000000000803';
  perform pg_temp.drain_workbench_jobs();

  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000303');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000303','b7000000-0000-4000-8000-000000000303',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_PAID'
    and v_result->>'refusal_nature'='PERMANENT'
    and coalesce((v_result->>'retryable')::boolean,true) is false,
    'UNA-006: a completed payment must refuse permanently, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from pg_catalog.jsonb_array_elements(v_result->'failed_checks') as failed(value)
      where failed.value->>'check'='W2'
        and failed.value->'reasons' @> '["SETTLEMENT_HISTORY_EXISTS"]'::jsonb
        and failed.value->'reasons' @> '["CANDIDATE_ROW_SETTLED"]'::jsonb
        and failed.value->'reasons' @> '["TRANSFER_FINAL_MONEY_MOVED"]'::jsonb)=1,
    'UNA-006: W2 must name the settlement history, the settled Candidate row and '
    ||'the final-money transfer, got '||(v_result->'failed_checks')::text);
  perform pg_temp.assert_true(
    (select authorisation_row.withdrawn_at_utc is null
       from public.weekly_source_root_authorisations authorisation_row
      where authorisation_row.root_timesheet_id='b7000000-0000-4000-8000-000000000303'),
    'UNA-006: nothing is withdrawn');
  perform pg_temp.drain_workbench_jobs();
end
$verify_paid$;

-- ---------------------------------------------------------------------------
-- 6. UNA-007 and UNA-008 — an invoice line on a DRAFT invoice, then an ISSUED
--    one: permanently refused (W7); and an active invoice operation (W8)
-- ---------------------------------------------------------------------------
do $verify_invoiced$
declare
  v_result jsonb;
  v_signature text;
begin
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000304','b7000000-0000-4000-8000-000000000304',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 4 must authorise, got '||v_result::text);

  insert into public.invoices(id,client_id,status)
  values ('b7000000-0000-4000-8000-000000000b04','b7000000-0000-4000-8000-000000000002',
          'DRAFT'::public.invoice_status_enum);
  insert into public.invoice_lines(id,invoice_id,timesheet_id,booking_id)
  values ('b7000000-0000-4000-8000-000000000c04','b7000000-0000-4000-8000-000000000b04',
          'b7000000-0000-4000-8000-000000000304','WP07-BK-04');
  perform pg_temp.drain_workbench_jobs();

  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000304');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000304','b7000000-0000-4000-8000-000000000304',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_INVOICED'
    and v_result->>'refusal_nature'='PERMANENT',
    'UNA-007: an unissued invoice line must refuse permanently, got '||v_result::text);

  update public.invoices set status='ISSUED'::public.invoice_status_enum,
         issued_at_utc=pg_catalog.clock_timestamp()
   where id='b7000000-0000-4000-8000-000000000b04';
  v_result:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000304');
  perform pg_temp.assert_true(
    coalesce((v_result->>'available')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_INVOICED',
    'UNA-008: an issued invoice must refuse permanently too, got '||v_result::text);

  -- W8 on its own: remove the line and leave a live invoice operation whose
  -- scope names the root.
  delete from public.invoice_lines where id='b7000000-0000-4000-8000-000000000c04';
  insert into public.invoice_operations(
    id,operation_type,entity_type,entity_id,idempotency_key,status,input_json
  ) values (
    'b7000000-0000-4000-8000-000000000d04','ISSUE_INVOICES','invoice',
    'b7000000-0000-4000-8000-000000000b04','wp07-op-04','RUNNING',
    pg_catalog.jsonb_build_object(
      'timesheet_id','b7000000-0000-4000-8000-000000000304'));
  v_result:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000304');
  perform pg_temp.assert_true(
    coalesce((v_result->>'available')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_INVOICE_OPERATION_ACTIVE'
    and v_result->>'refusal_nature'='TEMPORARY',
    'W8: a live invoice operation must refuse temporarily, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_invoiced$;

-- ---------------------------------------------------------------------------
-- The protected-hours chain, used by UNA-009 (a LATER approval) and UNA-002
-- (an approval that formed part of the first authorisation).
-- ---------------------------------------------------------------------------
insert into public.weekly_source_groups(
  id,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  'b7000000-0000-4000-8000-000000000f01','b7000000-0000-4000-8000-0000000000a1',
  'WP07_GROUP','WP07 Source Group','ROSTER',0,'12:00:00');
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc
) values (
  'b7000000-0000-4000-8000-000000000f02','b7000000-0000-4000-8000-000000000f01',
  '2026-09-13','2026-09-14 12:00:00+00');

create function pg_temp.seed_protected(
  p_suffix text,p_candidate_id uuid,p_contract_id uuid,p_root_timesheet_id uuid,
  p_approved_at_utc timestamptz
) returns uuid language plpgsql as $function$
declare
  v_event uuid:=('b7000000-0000-4000-8000-00000000'||p_suffix||'01')::uuid;
  v_family uuid:=('b7000000-0000-4000-8000-00000000'||p_suffix||'02')::uuid;
  v_approval uuid:=('b7000000-0000-4000-8000-00000000'||p_suffix||'03')::uuid;
  v_run uuid:=('b7000000-0000-4000-8000-00000000'||p_suffix||'04')::uuid;
begin
  insert into public.weekly_work_events(
    id,candidate_id,client_id,work_date,identity_kind,durable_identity_hash
  ) values (
    v_event,p_candidate_id,'b7000000-0000-4000-8000-000000000002','2026-09-10',
    'OFFICE_PROTECTED_SHIFT',pg_catalog.sha256(pg_catalog.convert_to(v_event::text,'UTF8')));
  insert into public.weekly_exceptional_pay_target_families(
    id,agency_id,candidate_id,contract_id,week_start_date,week_ending_date,
    root_timesheet_id,ownership_state,first_signed_evidence_fingerprint,
    current_lifecycle_state,creation_idempotency_key
  ) values (
    v_family,'b7000000-0000-4000-8000-0000000000a1',p_candidate_id,p_contract_id,
    '2026-09-07','2026-09-13',p_root_timesheet_id,'TARGET_MANAGED',
    pg_catalog.sha256(pg_catalog.convert_to(v_family::text,'UTF8')),
    'PROTECTED','wp07-family-'||p_suffix);
  insert into public.weekly_exceptional_orchestration_runs(
    id,family_id,request_kind,idempotency_key,requested_by_user_id,state,
    before_state_fingerprint
  ) values (
    v_run,v_family,'APPROVE','wp07-run-'||p_suffix,
    'b7000000-0000-4000-8000-000000000001','COMPLETE',
    pg_catalog.sha256(pg_catalog.convert_to(v_run::text,'UTF8')));
  insert into public.weekly_exceptional_payment_approvals(
    id,pay_target_family_id,work_event_id,candidate_id,client_id,contract_id,
    week_ending,protected_work_date,protected_start_at_local,protected_end_at_local,
    protected_break_minutes,contributing_issue_episode_ids_hash,
    signed_schedule_fact_hash,contract_rate_policy_source_fingerprint,
    approved_by_user_id,approval_reason,approved_at_utc,source_cycle_id,
    approved_target_pay_components_json,approved_target_gross,
    creation_orchestration_run_id,approval_hash,creation_idempotency_key
  ) values (
    v_approval,v_family,v_event,p_candidate_id,'b7000000-0000-4000-8000-000000000002',
    p_contract_id,'2026-09-13','2026-09-10','2026-09-10 08:00:00','2026-09-10 16:00:00',
    30,pg_catalog.sha256(pg_catalog.convert_to('episodes','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('schedule','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('policy','UTF8')),
    'b7000000-0000-4000-8000-000000000001','WP07 protected hours',p_approved_at_utc,
    'b7000000-0000-4000-8000-000000000f02','{}'::jsonb,100,
    v_run,
    pg_catalog.sha256(pg_catalog.convert_to(v_approval::text,'UTF8')),
    'wp07-approval-'||p_suffix);
  return v_approval;
end;
$function$;

-- ---------------------------------------------------------------------------
-- 7. UNA-009 — a later Weekly Source decision: permanently refused (W1)
-- ---------------------------------------------------------------------------
do $verify_later_decision$
declare
  v_result jsonb;
  v_signature text;
begin
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000305','b7000000-0000-4000-8000-000000000305',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 5 must authorise, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  -- A protected-hours decision approved AFTER the first authorisation.
  perform pg_temp.seed_protected(
    '05','b7000000-0000-4000-8000-000000000105','b7000000-0000-4000-8000-000000000205',
    'b7000000-0000-4000-8000-000000000305',
    -- Explicitly AFTER the first authorisation.  Inside one transaction every
    -- installed owner stamps transaction_timestamp(), so "later" has to be an
    -- explicit offset from that, not clock_timestamp().
    pg_catalog.transaction_timestamp()+'1 hour'::interval);
  perform pg_temp.drain_workbench_jobs();

  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000305');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000305','b7000000-0000-4000-8000-000000000305',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_LATER_DECISION_EXISTS'
    and v_result->>'refusal_nature'='PERMANENT'
    and (select pg_catalog.count(*)
           from pg_catalog.jsonb_array_elements(v_result->'failed_checks') as failed(value)
          where failed.value->>'check'='W1'
            and failed.value->'reasons' @> '["LATER_PROTECTED_HOURS_APPROVAL"]'::jsonb)=1,
    'UNA-009: a later decision must refuse permanently, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_later_decision$;

-- ---------------------------------------------------------------------------
-- 8. UNA-002 — protected/exceptional hours in the first authorisation:
--    the approval is marked withdrawn, never deleted (proof/36 section 5 step 7)
-- ---------------------------------------------------------------------------
do $verify_protected_withdrawn$
declare
  v_result jsonb;
  v_signature text;
  v_approval uuid;
  v_row public.weekly_exceptional_payment_approvals%rowtype;
begin
  perform pg_temp.drain_workbench_jobs();
  -- The approval is made first, then the Timesheet authorised, so the approval
  -- formed part of the first authorisation.
  v_approval:=pg_temp.seed_protected(
    '09','b7000000-0000-4000-8000-000000000109','b7000000-0000-4000-8000-000000000209',
    'b7000000-0000-4000-8000-000000000309',
    pg_catalog.transaction_timestamp()-'1 hour'::interval);
  perform pg_temp.drain_workbench_jobs();

  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000309','b7000000-0000-4000-8000-000000000309',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 9 must authorise, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000309');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000309','b7000000-0000-4000-8000-000000000309',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false),
    'UNA-002: the withdrawal must succeed with protected hours present, got '
    ||v_result::text);

  select * into v_row from public.weekly_exceptional_payment_approvals
   where id=v_approval;
  perform pg_temp.assert_true(
    v_row.id is not null
    and v_row.withdrawn_at_utc is not null
    and v_row.withdrawn_by_user_id='b7000000-0000-4000-8000-000000000001'
    and v_row.withdrawal_kind='FIRST_AUTHORISATION_WITHDRAWN'
    and v_row.approval_hash is not null
    and v_row.approved_at_utc is not null,
    'UNA-002: the approval must be marked withdrawn and never deleted');
  perform pg_temp.assert_true(
    (v_result#>'{protected_hours,approvals_withdrawn}') @> pg_catalog.to_jsonb(v_approval),
    'UNA-002: the result must name the withdrawn approval, got '
    ||(v_result->'protected_hours')::text);

  -- proof/36 section 5 step 7 also names a weekly_exceptional_pay_family_events
  -- row.  The installed state check admits only WAIT, ACCEPTED_SOURCE and
  -- NOT_WORKED, so the row is written only once the schema owner adds
  -- FIRST_AUTHORISATION_WITHDRAWN (handoff WP-07 N2).  Whichever is installed,
  -- the omission is reported rather than silent.
  perform pg_temp.assert_true(
    (v_result#>>'{protected_hours,family_event_state_admitted}')::boolean
    = exists (select 1 from pg_catalog.pg_constraint as constraint_row
              where constraint_row.conrelid
                    ='public.weekly_exceptional_pay_family_events'::pg_catalog.regclass
                and constraint_row.contype='c'
                and pg_catalog.pg_get_constraintdef(constraint_row.oid)
                    like '%FIRST_AUTHORISATION_WITHDRAWN%'),
    'UNA-002: the family-event capability must be reported from the installed constraint');
  perform pg_temp.assert_true(
    case when (v_result#>>'{protected_hours,family_event_state_admitted}')::boolean
         then pg_catalog.jsonb_array_length(v_result#>'{protected_hours,family_events_written}')=1
         else (v_result#>>'{protected_hours,family_event_omitted_reason}')
              ='WEEKLY_SOURCE_FAMILY_EVENT_STATE_NOT_ADMITTED' end,
    'UNA-002: the family event is either written or its omission is named, got '
    ||(v_result->'protected_hours')::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_protected_withdrawn$;

-- ---------------------------------------------------------------------------
-- 9. HANDOVER 2 round 4 ruling 4 — the VOIDED transfer, and ruling 5 item 5 —
--    a reservation released with reason WRITE_OFF
-- ---------------------------------------------------------------------------
do $verify_round4_rulings$
declare
  v_result jsonb;
  v_signature text;
  v_class record;
  v_pre record;
  v_overlay record;
  v_pending record;
  v_shape integer;
begin
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000307','b7000000-0000-4000-8000-000000000307',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 7 must authorise, got '||v_result::text);

  -- A completed whole-batch cancellation whose bound transfer the installed
  -- cancellation owner left as VOIDED.
  perform pg_temp.seed_batch(
    'b7000000-0000-4000-8000-000000000607','CANCELLED',
    'b7000000-0000-4000-8000-000000000707','b7000000-0000-4000-8000-000000000107',
    'b7000000-0000-4000-8000-000000000807','b7000000-0000-4000-8000-000000000307',true,
    'NOT_SUBMITTED',null,pg_catalog.clock_timestamp());
  insert into public.pay_bank_transfers(
    id,pay_batch_id,candidate_id,pay_channel,amount,status,rail_provider,rail_env,
    payee_entity_kind,failed_reason,rail_meta_json
  ) values (
    'b7000000-0000-4000-8000-000000000a07','b7000000-0000-4000-8000-000000000607',
    'b7000000-0000-4000-8000-000000000107','PAYE',0,'VOIDED','REVOLUT','SANDBOX',
    'CANDIDATE','PRE_BANK_CANCEL_VOIDED',
    pg_catalog.jsonb_build_object('pre_bank_cancel_applied',true));
  update public.pay_batch_items set pay_bank_transfer_id='b7000000-0000-4000-8000-000000000a07'
   where id='b7000000-0000-4000-8000-000000000807';
  perform pg_temp.drain_workbench_jobs();

  -- Ruling 4: Weekly Source adds NO private interpretation.  The installed
  -- classifier is asked, and it maps VOIDED to UNKNOWN today, whatever markers
  -- the cancellation owner left.
  select * into v_class
  from public._pay_rail_state_money_movement_classify(
    'VOIDED',null,
    pg_catalog.jsonb_build_object('pre_bank_cancel_applied',true),
    pg_catalog.jsonb_build_object('pre_bank_cancel_applied',true));
  perform pg_temp.assert_true(
    v_class.cash_state='UNKNOWN'
    and coalesce(v_class.is_terminal_no_money,false) is false
    and coalesce(v_class.is_final_money_moved,false) is false,
    'ruling 4: the INSTALLED classifier still maps VOIDED to UNKNOWN, got '
    ||v_class.cash_state);

  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000307');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000307','b7000000-0000-4000-8000-000000000307',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'refusal_nature' in ('TEMPORARY','INTEGRITY'),
    'ruling 4: an unmarked or ambiguous VOIDED transfer can never permit a '
    ||'withdrawal, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from pg_catalog.jsonb_array_elements(v_result->'failed_checks') as failed(value)
      where failed.value->>'check'='W2'
        and (failed.value->'reasons' @> '["TRANSFER_PENDING_OR_UNKNOWN"]'::jsonb
             or failed.value->'reasons'
                @> '["TERMINAL_NO_MONEY_TRANSFER_WITHOUT_BINDING_A_OR_B"]'::jsonb))=1,
    'ruling 4: W2 must name the unresolved transfer, got '
    ||(v_result->'failed_checks')::text);

  -- =====================================================================
  -- THE FUTURE-EXPECTATION TRIPWIRE.
  --
  -- This is built to FAIL the day the Banking Pay classifier ships, so that the
  -- deliberately blocked completed-cancellation change-of-mind journey cannot be
  -- forgotten.  A tripwire that does not fire is worse than none, so its shape
  -- is set by the Banking Pay workstream's own ACCEPTED contract
  -- (`…\plan6-pack-audit-20260916\BANKING_PAY_CLASSIFIER_ACCEPTANCE_R5.md`) and
  -- by round-5 Part F, not by what is convenient to assert.
  --
  -- THREE THINGS THAT CONTRACT FIXES, AND WHICH THIS BLOCK IS SHAPED TO:
  --
  --   1. The result is TYPED.  "Weekly Source and every other consumer must use
  --      the classifier's typed result.  They must not copy or independently
  --      interpret its marker rules."  So the tripwire reads the classifier's
  --      own typed output - `cash_state`, `is_terminal_no_money`,
  --      `is_pending_non_final`, `is_final_money_moved` and `reason` - and NOT a
  --      boolean, a bare string, or this package's own withdrawal verdict.
  --
  --   2. There are TWO separate reason-specific TERMINAL_NO_MONEY branches:
  --      `PRE_BANK_CANCEL_VOIDED` and
  --      `CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED`.  "Do not allow evidence
  --      from one branch to complete the other."  So the tripwire probes BOTH,
  --      independently, and fires if EITHER becomes terminal-no-money.  A
  --      classifier that shipped with only the second branch live would still
  --      unblock a journey, and a tripwire that watched only the first would
  --      stay silent through it.
  --
  --   3. `UNKNOWN` and `PENDING_NON_FINAL` are the states that KEEP the journey
  --      blocked.  "Any missing, mixed, contradictory or unregistered evidence
  --      returns UNKNOWN and fails closed"; "Preserve PENDING_NON_FINAL where
  --      payment resolution remains genuinely in flight."  So the tripwire must
  --      NOT fire on either, and that is asserted positively below rather than
  --      left to chance.
  --
  -- WHAT WHOEVER SEES THIS FAIL WILL BE LOOKING AT.  One of the two probes will
  -- report `is_terminal_no_money = true` with a `cash_state` of
  -- `TERMINAL_NO_MONEY` and a `reason` naming its branch, where today both
  -- report `cash_state = 'UNKNOWN'` with the reason "No explicit final-paid,
  -- terminal no-money, or pending non-final evidence was found."  The failure
  -- message prints both probes in full, so the reader can see which branch
  -- landed and whether the other is still closed.  That means: the Banking Pay
  -- classifier has shipped, the completed-cancellation change-of-mind journey is
  -- no longer blocked at the boundary, and Weekly Source's W2 and W6 handling of
  -- `is_terminal_no_money` must be re-proved against the real classifier -
  -- including that a `WRITE_OFF` release and latent `pay_unpay_batch` evidence
  -- still cannot satisfy either branch (round-5 A5, and the same requirement in
  -- the Banking Pay acceptance).  Nothing in Weekly Source should be changed
  -- before that re-proof; the block is correct until then.
  -- =====================================================================

  -- Shape guard first: if the classifier's typed result ever loses the columns
  -- this tripwire reads, the tripwire must fail LOUDLY rather than quietly test
  -- nothing.
  select pg_catalog.count(*)::integer into v_shape
  from pg_catalog.pg_proc as classifier
  cross join lateral pg_catalog.unnest(classifier.proargnames) as argument(name)
  where classifier.oid=pg_catalog.to_regprocedure(
          'public._pay_rail_state_money_movement_classify(text,text,jsonb,jsonb)')
    and argument.name in ('cash_state','is_terminal_no_money','is_pending_non_final',
                          'is_final_money_moved','reason');
  perform pg_temp.assert_true(v_shape=5,
    'TRIPWIRE SHAPE: the installed classifier must return a TYPED result '
    ||'carrying cash_state, is_terminal_no_money, is_pending_non_final, '
    ||'is_final_money_moved and reason; found '||v_shape||' of 5. The Banking '
    ||'Pay acceptance requires consumers to use the typed result, so a tripwire '
    ||'that cannot read it proves nothing');

  -- BRANCH 1 of 2: the pre-bank cancellation, with every marker the round-4
  -- rule and the Banking Pay acceptance name for it.
  select * into v_pre
  from public._pay_rail_state_money_movement_classify(
    'VOIDED',null,
    pg_catalog.jsonb_build_object(
      'failed_reason','PRE_BANK_CANCEL_VOIDED','pre_bank_cancel_applied',true),
    pg_catalog.jsonb_build_object(
      'failed_reason','PRE_BANK_CANCEL_VOIDED','pre_bank_cancel_applied',true));

  -- BRANCH 2 of 2: the correction reauthorisation overlay reset.  This branch
  -- did not exist when WP-07 wrote its tripwire, and a classifier shipping with
  -- only this one live would have gone unnoticed.
  select * into v_overlay
  from public._pay_rail_state_money_movement_classify(
    'VOIDED',null,
    pg_catalog.jsonb_build_object(
      'failed_reason','CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED',
      'cancellation_reauthorisation_overlay_voided',true,'amount',0),
    pg_catalog.jsonb_build_object(
      'failed_reason','CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED',
      'cancellation_reauthorisation_overlay_voided',true,'amount',0));

  perform pg_temp.assert_true(
    coalesce(v_pre.is_terminal_no_money,false) is false
    and coalesce(v_overlay.is_terminal_no_money,false) is false,
    'FUTURE-EXPECTATION TRIPWIRE FIRED: the Banking Pay money-movement '
    ||'classifier now returns TERMINAL_NO_MONEY for at least one of its two '
    ||'reason-specific branches, so the completed-cancellation change-of-mind '
    ||'journey is no longer blocked at the boundary and Weekly Source''s W2 and '
    ||'W6 handling must be re-proved against the real classifier (including '
    ||'that WRITE_OFF and latent pay_unpay_batch evidence still satisfy '
    ||'neither branch). PRE_BANK_CANCEL_VOIDED -> cash_state='
    ||coalesce(v_pre.cash_state,'<null>')||', terminal_no_money='
    ||coalesce(v_pre.is_terminal_no_money::text,'<null>')||', reason='
    ||coalesce(v_pre.reason,'<null>')
    ||'  |  CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED -> cash_state='
    ||coalesce(v_overlay.cash_state,'<null>')||', terminal_no_money='
    ||coalesce(v_overlay.is_terminal_no_money::text,'<null>')||', reason='
    ||coalesce(v_overlay.reason,'<null>'));

  -- Today both branches fail closed to UNKNOWN, which is what keeps the journey
  -- blocked. Asserted explicitly so the tripwire cannot be satisfied by a
  -- classifier that simply stopped answering.
  perform pg_temp.assert_true(
    v_pre.cash_state='UNKNOWN' and v_overlay.cash_state='UNKNOWN',
    'the tripwire must be watching a classifier that FAILS CLOSED on both '
    ||'branches today, got pre='||coalesce(v_pre.cash_state,'<null>')
    ||' overlay='||coalesce(v_overlay.cash_state,'<null>'));

  -- And the tripwire must NOT fire on PENDING_NON_FINAL, which the Banking Pay
  -- acceptance preserves for payment resolution that is genuinely still in
  -- flight. This assertion must keep holding AFTER the classifier ships: a
  -- pending transfer is not, and never becomes, terminal-no-money.
  select * into v_pending
  from public._pay_rail_state_money_movement_classify(
    'PENDING',null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    v_pending.cash_state='PENDING_NON_FINAL'
    and coalesce(v_pending.is_pending_non_final,false)
    and coalesce(v_pending.is_terminal_no_money,false) is false
    and coalesce(v_pending.is_final_money_moved,false) is false,
    'PENDING_NON_FINAL must be preserved and distinct from terminal-no-money, '
    ||'got '||coalesce(v_pending.cash_state,'<null>'));

  -- Separately from the tripwire, and NOT part of it: today's Weekly Source
  -- behaviour on the same fixture. The withdrawal refuses, as it must while the
  -- boundary says UNKNOWN.
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false,
    'ruling 4: a fully marked whole-transfer pre-bank cancellation is refused '
    ||'by Weekly Source while the classifier says UNKNOWN');

  -- A partial cancellation left PENDING also refuses.
  update public.pay_bank_transfers set status='PENDING',failed_reason=null,
         rail_meta_json='{}'::jsonb
   where id='b7000000-0000-4000-8000-000000000a07';
  v_result:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000307');
  perform pg_temp.assert_true(
    coalesce((v_result->>'available')::boolean,true) is false,
    'ruling 4: a partial cancellation left PENDING must refuse, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  -- Ruling 5 item 5: a reservation released with reason WRITE_OFF is not
  -- evidence of a completed cancellation.  Candidate 10, whose batch is
  -- otherwise a clean terminal cancellation.
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000310','b7000000-0000-4000-8000-000000000310',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 10 must authorise, got '||v_result::text);
  perform pg_temp.seed_batch(
    'b7000000-0000-4000-8000-000000000610','CANCELLED',
    'b7000000-0000-4000-8000-000000000710','b7000000-0000-4000-8000-000000000110',
    'b7000000-0000-4000-8000-000000000810','b7000000-0000-4000-8000-000000000310',true,
    'NOT_SUBMITTED',null,pg_catalog.clock_timestamp());
  insert into public.pay_advances(
    id,candidate_id,client_id,reason,original_amount,outstanding_amount,case_type
  ) values (
    'b7000000-0000-4000-8000-000000000e10','b7000000-0000-4000-8000-000000000110',
    'b7000000-0000-4000-8000-000000000002','MANUAL_ADVANCE'::public.pay_advance_reason_enum,
    50,50,'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum);
  insert into public.pay_advance_reservations(
    id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
    reserved_amount,status,released_at_utc,released_reason
  ) values (
    'b7000000-0000-4000-8000-000000000910','b7000000-0000-4000-8000-000000000e10',
    'b7000000-0000-4000-8000-000000000610','b7000000-0000-4000-8000-000000000710',
    'b7000000-0000-4000-8000-000000000810',50,'RELEASED',
    pg_catalog.clock_timestamp(),'WRITE_OFF');
  perform pg_temp.drain_workbench_jobs();

  v_result:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000310');
  perform pg_temp.assert_true(
    coalesce((v_result->>'available')::boolean,true) is false
    and (select pg_catalog.count(*)
           from pg_catalog.jsonb_array_elements(v_result->'failed_checks') as failed(value)
          where failed.value->>'check'='W4'
            and failed.value->'reasons'
                @> '["RESERVATION_RELEASED_BY_WRITE_OFF"]'::jsonb
            -- ROUND 5 SECTION A5: its own code and the REVIEW disposition, never
            -- a retry.  This fixture's census also errors, so the top-level code
            -- is W3's; the write-off's own top-level message is proved on a
            -- clean fixture in the WP-07c verifier
            -- (17092026_0610_weekly_source_withdrawal_supersession_v1.sql).
            and failed.value->>'code'='WEEKLY_SOURCE_UNAUTHORISE_WRITE_OFF_UNRESOLVED'
            and failed.value->>'nature'='INTEGRITY')=1
    and coalesce((v_result->>'retryable')::boolean,true) is false,
    'ruling 5 item 5 and round 5 section A5: a WRITE_OFF release is not '
    ||'completed-cancellation evidence, and carries its own review code, got '
    ||v_result::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_round4_rulings$;

-- ---------------------------------------------------------------------------
-- 10. UNA-012 — an ordinary, unmanaged Timesheet
-- ---------------------------------------------------------------------------
do $verify_ordinary_unchanged$
declare
  v_result jsonb;
  v_before text;
  v_after text;
begin
  perform pg_temp.drain_workbench_jobs();
  v_before:=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
    pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
      'public.timesheet_unauthorise_atomic(uuid,uuid,uuid,timestamptz,text)')),'UTF8')),'hex');

  -- Candidate 8 is authorised through the ORDINARY owner only, so Weekly Source
  -- never bound it.  The withdrawal owner refuses and the ordinary owner is the
  -- route (proof/36 section 3).
  perform public.timesheet_authorise_generic_atomic(
    'b7000000-0000-4000-8000-000000000308','b7000000-0000-4000-8000-000000000308',
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.drain_workbench_jobs();

  v_result:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000308');
  perform pg_temp.assert_true(
    coalesce((v_result->>'available')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT',
    'UNA-012: an unmanaged Timesheet is not this owner''s route, got '||v_result::text);

  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000308','b7000000-0000-4000-8000-000000000308',
    pg_temp.current_signature('b7000000-0000-4000-8000-000000000308'),
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT',
    'UNA-012: the write path refuses the same way, got '||v_result::text);

  -- The ordinary owner still does its ordinary job on that Timesheet, and its
  -- definition is byte-identical before and after.
  v_result:=public.timesheet_unauthorise_atomic(
    'b7000000-0000-4000-8000-000000000308','b7000000-0000-4000-8000-000000000308',
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false),
    'UNA-012: the unchanged ordinary owner must still unauthorise an ordinary '
    ||'Timesheet, got '||v_result::text);

  v_after:=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
    pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
      'public.timesheet_unauthorise_atomic(uuid,uuid,uuid,timestamptz,text)')),'UTF8')),'hex');
  perform pg_temp.assert_true(v_before=v_after,
    'UNA-012: the ordinary owner definition must be byte-identical');
  perform pg_temp.drain_workbench_jobs();
end
$verify_ordinary_unchanged$;

-- ---------------------------------------------------------------------------
-- 11. Decision D8 — the per-root authorisation relation's write rules
--     (WP-01c; proof/34 section 4; proof/36 section 5.6), and HANDOVER 2
--     ROUND 5 SECTION A3, which REPLACED WP-07b's finding-F1 fix: a committed
--     entitlement head no longer refuses the withdrawal permanently, it is
--     SUPERSEDED atomically with it, with an explicit withdrawal reason and an
--     immutable predecessor link, and reauthorisation never revives it
-- ---------------------------------------------------------------------------
do $verify_d8_rules$
declare
  v_result jsonb;
  v_signature text;
  v_live uuid;
  v_definition text;
  v_update text;
  v_audit_before bigint;
  v_jobs_before bigint;
  v_avail jsonb;
begin
  perform pg_temp.drain_workbench_jobs();

  -- Candidate 6's root is authorised on generation 1 by section 2.  Withdraw it
  -- FIRST, while the family carries no entitlement head at all: that is the
  -- only shape in which proof/36 section 5.6's head clearing is reachable at
  -- all once WP-07b's F1 fix is in, and it gives this section a genuinely
  -- withdrawn generation for the immutability rules below.
  select id into v_live from public.weekly_source_root_authorisations
   where root_timesheet_id='b7000000-0000-4000-8000-000000000306'
     and withdrawn_at_utc is null;
  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000306');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000306','b7000000-0000-4000-8000-000000000306',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and (v_result->>'entitlement_head_cleared') is null,
    'D8: a root with no head withdraws and reports no cleared head, got '
    ||v_result::text);
  perform pg_temp.assert_true(
    (select authorisation_row.withdrawn_at_utc is not null
       and authorisation_row.current_entitlement_head_id is null
     from public.weekly_source_root_authorisations authorisation_row
     where authorisation_row.id=v_live),
    'proof/36 section 5.6: the head pointer is null and the withdrawal marks are set');
  perform pg_temp.drain_workbench_jobs();

  -- ---------------------------------------------------------------------
  -- D8 — "the head pointer is cleared in the SAME statement as the withdrawal
  -- marks".  WP-25, 18 September 2026.
  --
  -- WHAT WAS HERE.  Two searches over `pg_get_functiondef` of the withdrawal
  -- owner — exactly one UPDATE of the relation, and that UPDATE sets all three
  -- columns — presented as the PROOF of the same-statement rule and justified in
  -- a comment by "WP-07b's F1 fix makes the reachable case impossible: W1
  -- refuses permanently whenever a head has been committed, so the owner can
  -- never be presented with a live generation that still carries a pointer."
  -- Round-5 ruling A3 rejected that rule.  The case IS reachable, this very
  -- section reaches it below, and a search over a routine's text is not evidence
  -- about what the routine does (Part 1 review rule 1).
  --
  -- The checks themselves were not wrong — they still catch a class of edit —
  -- and they are kept below, labelled STATIC.  What was wrong was the claim that
  -- they proved the rule, and the dead justification for making that claim.
  --
  -- WHAT IS ASSERTED NOW.  The property is enforced by a row CHECK, and a row
  -- CHECK is evaluated on the finished row at the end of each statement.  So:
  --
  --   * the constraint is read from the catalogue of the INSTALLED database, by
  --     its normalised definition, so widening or dropping it fails here;
  --   * a two-statement implementation is proved impossible by EXECUTING one —
  --     setting the withdrawal marks on a live generation that still carries a
  --     head pointer must raise 23514;
  --   * and the one-statement implementation is proved to exist by EXECUTING
  --     the withdrawal of a root that really does carry a COMMITTED_CURRENT
  --     head, further down this section, which can only succeed if the marks
  --     and the pointer clearing are one statement.
  --
  -- The two text searches are KEPT, because deleting them would remove coverage
  -- that execution does not replicate: "exactly ONE update of the relation"
  -- catches a second, unrelated write that no behavioural case would reach
  -- (Part 1 review rule 7 — do not weaken an existing test).  They are now
  -- labelled STATIC, which is what they are, and they are no longer the proof of
  -- anything the executed assertions below prove.  Both mutations were run:
  -- removing `current_entitlement_head_id=null` from the single UPDATE is caught
  -- by the static limb at definition time AND by the row CHECK at run time,
  -- 23514, on the generation that carries a pointer.
  -- ---------------------------------------------------------------------
  perform pg_temp.assert_true(
    exists (
      select 1 from pg_catalog.pg_constraint constraint_row
       where constraint_row.conrelid
               ='public.weekly_source_root_authorisations'::pg_catalog.regclass
         and constraint_row.contype='c'
         and pg_catalog.pg_get_constraintdef(constraint_row.oid)
             ='CHECK (((withdrawn_at_utc IS NULL) OR (current_entitlement_head_id IS NULL)))'),
    'D8: the installed relation must carry the row CHECK that makes a '
    ||'two-statement withdrawal impossible');

  -- STATIC, and declared so: a structural check on the installed definition, not
  -- evidence that any path works.
  v_definition:=pg_catalog.pg_get_functiondef(
    'public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)'::regprocedure);
  perform pg_temp.assert_true(
    (pg_catalog.length(v_definition)
     -pg_catalog.length(pg_catalog.replace(v_definition,
        'update public.weekly_source_root_authorisations','')))
    /pg_catalog.length('update public.weekly_source_root_authorisations')=1,
    'D8 (STATIC): the withdrawal owner must contain exactly ONE update of '
    ||'public.weekly_source_root_authorisations');
  v_update:=pg_catalog.substring(v_definition,
    'update public\.weekly_source_root_authorisations.*?;');
  perform pg_temp.assert_true(
    v_update like '%withdrawn_at_utc=%'
    and v_update like '%withdrawn_by_user_id=%'
    and v_update like '%current_entitlement_head_id=null%',
    'D8 (STATIC): that one UPDATE must set the two withdrawal columns AND clear '
    ||'the head pointer, got: '||coalesce(v_update,'<no match>'));

  -- Now re-authorise, and only then publish the committed decision bundle and
  -- head, exactly as the coordinator leaves them (24 section 4.5 step 5;
  -- proof/34 section 4's head-publication row).
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000306','b7000000-0000-4000-8000-000000000306',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and (v_result->>'authorisation_generation')::integer=2
    and v_result->>'family_booking_id'='WP07-BK-06'
    and (v_result->>'timesheet_version')::integer=2,
    'D8: re-authorisation appends generation 2 with the same identity, got '
    ||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state,committed_at_utc
  ) values (
    'b7000000-0000-4000-8000-000000000fb1',1,'b7000000-0000-4000-8000-0000000000a1',
    'b7000000-0000-4000-8000-000000000106','2026-09-13','SINGLE_ROOT','WP07-BK-06',
    'b7000000-0000-4000-8000-000000000306','b7000000-0000-4000-8000-000000000206',
    'b7000000-0000-4000-8000-000000000fd1','b7000000-0000-4000-8000-000000000001',
    'IMMEDIATE',pg_catalog.sha256(pg_catalog.convert_to('request','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('revision','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('choice','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('before','UTF8')),
    array['b7000000-0000-4000-8000-000000000fc1']::uuid[],'COMMITTED',
    pg_catalog.transaction_timestamp());
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    state,certified_zero,component_count,entitlement_digest,inventory_digest,
    source_generation_digest,decision_bundle_id,bundle_revision,decision_id,
    decided_by_user_id,committed_at_utc,publication_receipt_digest,scope_change_tx_token
  ) values (
    'b7000000-0000-4000-8000-000000000fc1','LOCKED_FINAL_SOURCE',
    'b7000000-0000-4000-8000-0000000000a1','b7000000-0000-4000-8000-000000000106',
    'b7000000-0000-4000-8000-000000000206','2026-09-13',
    'b7000000-0000-4000-8000-000000000306','WP07-BK-06',2,1,'COMMITTED_CURRENT',
    true,0,pg_catalog.sha256(pg_catalog.convert_to('entitlement','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('inventory','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('generation','UTF8')),
    'b7000000-0000-4000-8000-000000000fb1',1,'b7000000-0000-4000-8000-000000000fd1',
    'b7000000-0000-4000-8000-000000000001',pg_catalog.transaction_timestamp(),
    pg_catalog.sha256(pg_catalog.convert_to('receipt','UTF8')),
    'b7000000-0000-4000-8000-000000000fe1');

  -- A withdrawn generation can never be un-withdrawn and can never be given an
  -- entitlement head (proof/36 section 6's permanence rule; decision D8).
  -- Asserted on generation 1, which the withdrawal above really withdrew.
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$update public.weekly_source_root_authorisations
             set withdrawn_at_utc=null,withdrawn_by_user_id=null where id=%L$sql$,v_live),
    'WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_IMMUTABLE',
    'un-withdrawing a withdrawn generation');
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$update public.weekly_source_root_authorisations
             set current_entitlement_head_id='b7000000-0000-4000-8000-000000000fc1'
           where id=%L$sql$,v_live),
    'WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWN',
    'giving a withdrawn generation an entitlement head again');

  -- The live generation is generation 2, and the coordinator points it at the
  -- head exactly as it does in production.
  select id into v_live from public.weekly_source_root_authorisations
   where root_timesheet_id='b7000000-0000-4000-8000-000000000306'
     and withdrawn_at_utc is null;
  update public.weekly_source_root_authorisations
     set current_entitlement_head_id='b7000000-0000-4000-8000-000000000fc1'
   where id=v_live;

  -- The live-uniqueness index refuses a second live generation directly.
  perform pg_temp.assert_refused(
    $sql$insert into public.weekly_source_root_authorisations(
      root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
      authorised_row_signature,authorised_by_user_id
    ) values (
      'b7000000-0000-4000-8000-000000000306','WP07-BK-06',2,99,'x',
      'b7000000-0000-4000-8000-000000000001')$sql$,
    '%weekly_source_root_authorisations_live_uq%',
    'a second LIVE generation for one root');

  -- The identity guard refuses a stored family or version that is not the
  -- root's own.
  perform pg_temp.assert_refused(
    $sql$insert into public.weekly_source_root_authorisations(
      root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
      authorised_row_signature,authorised_by_user_id
    ) values (
      'b7000000-0000-4000-8000-000000000302','WP07-BK-WRONG',7,5,'x',
      'b7000000-0000-4000-8000-000000000001')$sql$,
    'WEEKLY_SOURCE_ROOT_AUTHORISATION_IDENTITY_MISMATCH',
    'a generation whose stored identity is not the root''s own');

  -- The withdrawal clears the head in the SAME statement: the check constraint
  -- is evaluated on the finished row, so a withdrawal that leaves the pointer
  -- set is impossible.
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$update public.weekly_source_root_authorisations
             set withdrawn_at_utc=pg_catalog.clock_timestamp(),
                 withdrawn_by_user_id='b7000000-0000-4000-8000-000000000001'
           where id=%L$sql$,v_live),
    '%weekly_source_root_authorisations_check%',
    'a withdrawal that leaves the entitlement head set');

  -- ---------------------------------------------------------------------
  -- HANDOVER 2 ROUND 5 SECTION A3 — THE RULING THAT REPLACED WP-07b's
  -- PERMANENT REFUSAL, PROVED END TO END.
  --
  -- The root now carries a COMMITTED_CURRENT entitlement head and a live
  -- generation that points at it, exactly as the publication coordinator
  -- leaves it.  Under WP-07b this was refused permanently.  Under ruling A3
  -- it must SUCCEED, and one transaction must produce all five effects:
  --
  --   1. the locks (the serial gate, the rotation set, FOR UPDATE on the live
  --      generation and on the head);
  --   2. the authorisation marked withdrawn;
  --   3. the committed head SUPERSEDED, with an explicit withdrawal reason and
  --      an immutable predecessor link;
  --   4. the aligned Candidate/canonical-root invalidation carrying the same
  --      transaction token and generation R25 requires;
  --   5. all of it committed together with ONE durable replay receipt.
  --
  -- The head here is CERTIFIED ZERO, which is the exact shape the WP-07 review
  -- proved pays nothing: before this package a withdrawal followed by a
  -- re-authorisation left that zero head current and the Workbench paid zero
  -- for an authorised 10-hour Timesheet.
  -- ---------------------------------------------------------------------
  select pg_catalog.count(*) into v_audit_before from public.audit_events
   where action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN'
     and object_id_text='b7000000-0000-4000-8000-000000000306';
  select pg_catalog.count(*) into v_jobs_before
    from public.banking_pay_workbench_jobs where status in ('QUEUED','RUNNING');

  -- The availability verdict says YES before the write path is called at all.
  v_avail:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000306');
  perform pg_temp.assert_true(
    coalesce((v_avail->>'available')::boolean,false)
    and coalesce((v_avail->>'ok')::boolean,false)
    and v_avail->>'code' is null
    and (v_avail#>>'{head_supersession,head_id}')='b7000000-0000-4000-8000-000000000fc1'
    and (v_avail#>>'{head_supersession,certified_zero}')='true',
    'A3: a committed certified-zero head must be withdrawable and the verdict '
    ||'must name the head it would supersede, got '||v_avail::text);

  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000306');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000306','b7000000-0000-4000-8000-000000000306',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and coalesce((v_result->>'withdrawn')::boolean,false)
    and coalesce((v_result->>'head_superseded')::boolean,false)
    and (v_result#>>'{head_supersession,head_id}')='b7000000-0000-4000-8000-000000000fc1'
    and (v_result#>>'{head_supersession,state_before}')='COMMITTED_CURRENT'
    and (v_result#>>'{head_supersession,state_after}')='SUPERSEDED'
    and (v_result#>>'{head_supersession,superseded_reason}')='FIRST_AUTHORISATION_WITHDRAWN'
    and (v_result->>'entitlement_head_cleared')='b7000000-0000-4000-8000-000000000fc1'
    and (v_result->>'withdrawal_receipt_id') is not null,
    'A3: the withdrawal must succeed and supersede the committed head, got '
    ||v_result::text);

  -- D8, EXECUTED (WP-25).  The generation this call just withdrew was carrying a
  -- head pointer when the call began: the coordinator pointed it at
  -- b7000000-…-000000000fc1 a few statements above, and the negatives at the top
  -- of this section proved that setting the withdrawal marks on such a row in a
  -- statement of its own raises 23514 on the row CHECK.  The call therefore
  -- cannot have succeeded unless the marks and the pointer clearing were ONE
  -- statement.  This is the case ruling A3 restored and WP-07b's removed comment
  -- claimed was unreachable; it is reached here, not argued from the owner's
  -- text.
  perform pg_temp.assert_true(
    (v_result->>'entitlement_head_cleared')='b7000000-0000-4000-8000-000000000fc1'
    and (select authorisation_row.withdrawn_at_utc is not null
           and authorisation_row.current_entitlement_head_id is null
         from public.weekly_source_root_authorisations authorisation_row
         where authorisation_row.id=v_live),
    'D8 executed: a generation that CARRIED a head pointer is withdrawn and the '
    ||'pointer cleared in one statement, which the row CHECK makes the only '
    ||'possible implementation');

  -- Step 2 executed: the authorisation is withdrawn and the Timesheet is back to
  -- Awaiting authorisation.
  perform pg_temp.assert_true(
    (select authorisation_row.withdrawn_at_utc is not null
       and authorisation_row.withdrawn_by_user_id='b7000000-0000-4000-8000-000000000001'
       and authorisation_row.current_entitlement_head_id is null
     from public.weekly_source_root_authorisations authorisation_row
     where authorisation_row.id=v_live)
    and (select timesheet_row.authorised_at_server is null
           from public.timesheets timesheet_row
          where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000306'),
    'A3 step 2: the authorisation must be marked withdrawn and the Timesheet '
    ||'unauthorised');

  -- Step 3 executed: the head is SUPERSEDED, carries the explicit reason and the
  -- withdrawal authority, carries NO successor head, and the receipt carries the
  -- immutable predecessor link back to it.
  perform pg_temp.assert_true(
    (select head_row.state='SUPERSEDED'
       and head_row.superseded_at_utc is not null
       and head_row.superseded_reason='FIRST_AUTHORISATION_WITHDRAWN'
       and head_row.superseded_by_head_id is null
       and head_row.superseded_by_withdrawal_id
           =(v_result->>'withdrawal_receipt_id')::uuid
     from public.weekly_source_entitlement_heads head_row
     where head_row.id='b7000000-0000-4000-8000-000000000fc1'),
    'A3 step 3: the head must be SUPERSEDED with the explicit withdrawal reason '
    ||'and the withdrawal authority, and no successor head');
  perform pg_temp.assert_true(
    (select receipt_row.predecessor_head_id='b7000000-0000-4000-8000-000000000fc1'
       and receipt_row.predecessor_head_revision=1
       and receipt_row.predecessor_head_state_before='COMMITTED_CURRENT'
       and receipt_row.predecessor_head_certified_zero=true
       and receipt_row.head_superseded=true
     from private.weekly_source_first_authorisation_withdrawal_receipts receipt_row
     where receipt_row.id=(v_result->>'withdrawal_receipt_id')::uuid),
    'A3 step 3: the receipt must carry the immutable predecessor link');

  -- The predecessor link really is immutable, and the head can never be revived.
  perform pg_temp.assert_refused(
    $sql$update public.weekly_source_entitlement_heads
            set state='COMMITTED_CURRENT',superseded_at_utc=null,
                superseded_reason=null,superseded_by_withdrawal_id=null
          where id='b7000000-0000-4000-8000-000000000fc1'$sql$,
    'WEEKLY_SOURCE_HEAD_WITHDRAWAL_SUPERSESSION_IMMUTABLE',
    'reviving a head that a withdrawal superseded');
  perform pg_temp.assert_refused(
    $sql$update public.weekly_source_entitlement_heads
            set superseded_by_withdrawal_id=null,superseded_reason=null
          where id='b7000000-0000-4000-8000-000000000fc1'$sql$,
    'WEEKLY_SOURCE_HEAD_WITHDRAWAL_SUPERSESSION_IMMUTABLE',
    'clearing the immutable predecessor link on a superseded head');
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$update private.weekly_source_first_authorisation_withdrawal_receipts
              set predecessor_head_id=null where id=%L$sql$,
      v_result->>'withdrawal_receipt_id'),
    'WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_IMMUTABLE',
    'changing a withdrawal receipt');
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$delete from private.weekly_source_first_authorisation_withdrawal_receipts
            where id=%L$sql$,
      v_result->>'withdrawal_receipt_id'),
    'WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_IMMUTABLE',
    'deleting a withdrawal receipt');

  -- Step 4 executed: exactly one aligned invalidation for the pair, carrying the
  -- one transaction token, and the R25 contract assert passed inside the owner.
  perform pg_temp.assert_true(
    (v_result->>'scope_change_tx_token') is not null
    and (v_result#>>'{invalidation,candidate_count}')='1'
    and coalesce((v_result#>>'{invalidation,job_inserted_count}')::integer,0)
        +coalesce((v_result#>>'{invalidation,job_coalesced_count}')::integer,0)>=1
    and coalesce((v_result#>>'{invalidation_contract,ok}')::boolean,false),
    'A3 step 4: one aligned invalidation with the one token, got '
    ||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(distinct job_row.scope_change_tx_token)
       from public.banking_pay_workbench_jobs job_row
      where job_row.status in ('QUEUED','RUNNING'))<=1,
    'A3 step 4: every job queued by this transaction carries ONE token');

  -- Step 5 executed: one receipt, and it carries the same token as the result
  -- and the same authorisation generation.
  -- Exactly one receipt for THIS withdrawal.  The root already carries one from
  -- the generation-1 withdrawal at the top of this section, and that is the
  -- point: one receipt per withdrawn generation, for ever, never one per root.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from private.weekly_source_first_authorisation_withdrawal_receipts receipt_row
      where receipt_row.root_timesheet_id='b7000000-0000-4000-8000-000000000306'
        and receipt_row.authorisation_generation
            =(v_result->>'authorisation_generation')::integer)=1
    and (select pg_catalog.count(*)
           from private.weekly_source_first_authorisation_withdrawal_receipts receipt_row
          where receipt_row.root_timesheet_id='b7000000-0000-4000-8000-000000000306')=2
    and (select pg_catalog.count(*)
           from private.weekly_source_first_authorisation_withdrawal_receipts receipt_row
          where receipt_row.root_timesheet_id='b7000000-0000-4000-8000-000000000306'
            and receipt_row.head_superseded)=1,
    'A3 step 5: exactly ONE durable replay receipt for this generation, and only '
    ||'the one that retired a head says so');
  perform pg_temp.assert_true(
    (select receipt_row.scope_change_tx_token=(v_result->>'scope_change_tx_token')::uuid
       and receipt_row.root_authorisation_id=v_live
       and receipt_row.authorisation_generation
           =(v_result->>'authorisation_generation')::integer
       and receipt_row.expected_row_signature=v_signature
       and pg_catalog.encode(receipt_row.request_digest,'hex')
           =(v_result->>'request_digest')
     from private.weekly_source_first_authorisation_withdrawal_receipts receipt_row
     where receipt_row.id=(v_result->>'withdrawal_receipt_id')::uuid),
    'A3 step 5: the receipt, the invalidation and the withdrawal share one token '
    ||'and one generation');

  -- THE WRONG-PAYMENT PATH IS CLOSED.  The Gate 4 selector resolves a head by
  -- physical root and COMMITTED_CURRENT state; after the withdrawal there is no
  -- such head for this root, so it cannot pay from the pre-withdrawal head.  The
  -- selector itself is untouched: this is the state it reads, not its code.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads head_row
      where head_row.root_timesheet_id='b7000000-0000-4000-8000-000000000306'
        and head_row.state='COMMITTED_CURRENT')=0,
    'A3: no committed current head may survive the withdrawal');

  -- The audit event is still written as well as the receipt.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN'
        and object_id_text='b7000000-0000-4000-8000-000000000306')=v_audit_before+1,
    'A3: the withdrawal is still recorded in Audit');

  -- REAUTHORISATION creates a NEW generation and NEVER revives the superseded
  -- head (ruling A3).
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000306','b7000000-0000-4000-8000-000000000306',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and (v_result->>'authorisation_generation')::integer=3,
    'A3: re-authorisation appends generation 3, got '||v_result::text);
  perform pg_temp.assert_true(
    (select head_row.state='SUPERSEDED'
       and head_row.superseded_reason='FIRST_AUTHORISATION_WITHDRAWN'
     from public.weekly_source_entitlement_heads head_row
     where head_row.id='b7000000-0000-4000-8000-000000000fc1')
    and (select authorisation_row.current_entitlement_head_id is null
           from public.weekly_source_root_authorisations authorisation_row
          where authorisation_row.root_timesheet_id
                ='b7000000-0000-4000-8000-000000000306'
            and authorisation_row.withdrawn_at_utc is null),
    'A3: re-authorisation must NEVER revive the superseded head and must point '
    ||'at no head at all');
  perform pg_temp.drain_workbench_jobs();

  -- A head that has been committed and then superseded is HISTORY and no longer
  -- refuses on its own: the root above is withdrawable again once it is
  -- re-authorised, which is precisely the change of mind ruling A3 restored.
  v_avail:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000306');
  perform pg_temp.assert_true(
    coalesce((v_avail->>'available')::boolean,false),
    'A3: a superseded head is history and must not refuse a later withdrawal, '
    ||'got '||v_avail::text);

  -- A STAGED head for the family, however, still refuses under W1.
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id
  ) values (
    'b7000000-0000-4000-8000-000000000fc2','LOCKED_FINAL_SOURCE',
    'b7000000-0000-4000-8000-0000000000a1','b7000000-0000-4000-8000-000000000106',
    'b7000000-0000-4000-8000-000000000206','2026-09-13',
    'b7000000-0000-4000-8000-000000000306','WP07-BK-06',2,2,
    'b7000000-0000-4000-8000-000000000fc1','STAGED',
    true,0,pg_catalog.sha256(pg_catalog.convert_to('entitlement2','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('inventory2','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('generation2','UTF8')),
    'b7000000-0000-4000-8000-000000000fb1',1,'b7000000-0000-4000-8000-000000000fd1',
    'b7000000-0000-4000-8000-000000000001');
  v_avail:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000306');
  perform pg_temp.assert_true(
    coalesce((v_avail->>'available')::boolean,true) is false
    and v_avail->>'code'='WEEKLY_SOURCE_UNAUTHORISE_LATER_DECISION_EXISTS'
    and (select pg_catalog.count(*)
           from pg_catalog.jsonb_array_elements(v_avail->'failed_checks') as failed(value)
          where failed.value->>'check'='W1'
            and failed.value->'reasons'
                @> '["ENTITLEMENT_HEAD_STAGED_FOR_THE_ROOT"]'::jsonb)=1,
    'A3: a publication in flight (a STAGED head) still refuses, got '
    ||v_avail::text);

  -- A head row is still never deleted.
  perform pg_temp.assert_refused(
    $sql$delete from public.weekly_source_entitlement_heads
          where id='b7000000-0000-4000-8000-000000000fc2'$sql$,
    'WEEKLY_SOURCE_IMMUTABLE_RECORD',
    'deleting an entitlement head row');

  -- Exactly TWO supersession authorities, never a third and never none.  Head
  -- revision 2 exists and is STAGED, so these are real rows being refused, not
  -- statements that matched nothing.
  perform pg_temp.assert_refused(
    $sql$update public.weekly_source_entitlement_heads
            set state='SUPERSEDED',superseded_at_utc=pg_catalog.clock_timestamp()
          where id='b7000000-0000-4000-8000-000000000fc2'$sql$,
    '%weekly_source_entitlement_heads_supersession_authority_check%',
    'superseding a head with no supersession authority');
  perform pg_temp.assert_refused(
    $sql$update public.weekly_source_entitlement_heads
            set state='SUPERSEDED',superseded_at_utc=pg_catalog.clock_timestamp(),
                superseded_reason='FIRST_AUTHORISATION_WITHDRAWN',
                superseded_by_head_id='b7000000-0000-4000-8000-000000000fc1'
          where id='b7000000-0000-4000-8000-000000000fc2'$sql$,
    '%weekly_source_entitlement_heads_supersession_authority_check%',
    'a publication supersession claiming the withdrawal reason');
  perform pg_temp.assert_refused(
    $sql$update public.weekly_source_entitlement_heads
            set superseded_reason='ENTITLEMENT_HEAD_PUBLICATION'
          where id='b7000000-0000-4000-8000-000000000fc2'$sql$,
    '%weekly_source_entitlement_heads_supersession_authority_check%',
    'a supersession reason on a head that is not superseded');

  perform pg_temp.drain_workbench_jobs();
end
$verify_d8_rules$;

-- ---------------------------------------------------------------------------
-- 12. UNA-018 — the exact replay, and an old physical id of the family
-- ---------------------------------------------------------------------------
do $verify_replay$
declare
  v_signature text;
  v_first jsonb;
  v_replay jsonb;
  v_audit_before bigint;
  v_jobs_before bigint;
  v_authorisations_before bigint;
  v_result jsonb;
begin
  perform pg_temp.drain_workbench_jobs();

  -- Candidate 1 is on generation 2 and clean; withdraw it.
  v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000301');
  v_first:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000301','b7000000-0000-4000-8000-000000000301',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_first->>'ok')::boolean,false),
    'UNA-018: the first withdrawal must succeed, got '||v_first::text);
  perform pg_temp.drain_workbench_jobs();

  select pg_catalog.count(*) into v_audit_before from public.audit_events;
  select pg_catalog.count(*) into v_jobs_before from public.banking_pay_workbench_jobs;
  select pg_catalog.count(*) into v_authorisations_before
    from public.weekly_source_root_authorisations;

  -- The exact replay returns the recorded result and calls nothing.
  v_replay:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000301','b7000000-0000-4000-8000-000000000301',
    v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_replay->>'replayed')::boolean,false)
    and coalesce((v_replay->>'ok')::boolean,false)
    and (v_replay->>'root_authorisation_id')=(v_first->>'root_authorisation_id')
    and (v_replay->>'scope_change_tx_token')=(v_first->>'scope_change_tx_token')
    and (v_replay->>'withdrawn_at_utc')=(v_first->>'withdrawn_at_utc'),
    'UNA-018: the exact replay must return the recorded result, got '||v_replay::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events)=v_audit_before
    and (select pg_catalog.count(*) from public.banking_pay_workbench_jobs)=v_jobs_before
    and (select pg_catalog.count(*) from public.weekly_source_root_authorisations)
        =v_authorisations_before,
    'UNA-018: the replay must write nothing at all');

  -- An old physical id of a rotated family only LOCATES the family: the
  -- decision is never received by or moved to it.  Candidate 6 is authorised on
  -- version 2; version 1 is history.
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000396','b7000000-0000-4000-8000-000000000396',
    pg_temp.current_signature('b7000000-0000-4000-8000-000000000306'),
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
    'UNA-018: an old physical id must be refused as an integrity failure, got '
    ||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='b7000000-0000-4000-8000-000000000396')=0,
    'UNA-018: no generation may ever be created on a historical physical id');
  -- The canonical root keeps exactly one live generation.  Its NUMBER is not
  -- asserted: section 11 now withdraws and re-authorises this root under ruling
  -- A3, so the live generation is 3, and the claim being made here is that the
  -- historical physical id took nothing from the canonical row.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.weekly_source_root_authorisations authorisation_row
      where authorisation_row.root_timesheet_id='b7000000-0000-4000-8000-000000000306'
        and authorisation_row.withdrawn_at_utc is null)=1,
    'UNA-018: the canonical root keeps its live generation');
  perform pg_temp.drain_workbench_jobs();
end
$verify_replay$;

-- ---------------------------------------------------------------------------
-- 13. UNA-017 second half — a forced failure after the unauthorisation rolls
--     the unauthorisation, the invalidation, the head clearing, the protected
--     withdrawal and the audit entry back together (proof/36 section 5 step 9)
-- ---------------------------------------------------------------------------
create function pg_temp.force_withdrawal_failure()
returns trigger language plpgsql as $function$
begin
  raise exception 'WP07_FORCED_FAILURE_AFTER_UNAUTHORISATION' using errcode='55000';
end;
$function$;

do $verify_forced_rollback$
declare
  v_signature text;
  v_authorised_before timestamptz;
  v_week_status text;
  v_jobs_before bigint;
  v_audit_before bigint;
  v_failed boolean:=false;
  v_message text;
  v_detail text;
  v_result jsonb;
begin
  -- A twelfth Candidate and a clean, head-free family of its own.  Since
  -- WP-07b's F1 fix a root that carries a committed entitlement head is refused
  -- permanently at the W1 check, which is BEFORE the first write, so the
  -- rollback case has to be proved on a root that really reaches the writes.
  insert into public.candidates(id,display_name)
  values ('b7000000-0000-4000-8000-000000000112','WP07 Candidate 12');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
  ) values (
    'b7000000-0000-4000-8000-000000000212','b7000000-0000-4000-8000-000000000112',
    'b7000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
    '{}'::jsonb,'HEALTHROSTER',true,true,true,true);
  perform pg_temp.seed_timesheet(
    'b7000000-0000-4000-8000-000000000312','WP07-BK-12',1,true,
    'b7000000-0000-4000-8000-000000000212');
  perform pg_temp.seed_week_and_financials(
    'b7000000-0000-4000-8000-000000000412','b7000000-0000-4000-8000-000000000212',
    'b7000000-0000-4000-8000-000000000312','b7000000-0000-4000-8000-000000000512',
    'b7000000-0000-4000-8000-000000000112','b7000000-0000-4000-8000-000000000002',1);
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    'b7000000-0000-4000-8000-000000000312','b7000000-0000-4000-8000-000000000312',
    null,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'UNA-017: the rollback fixture root must authorise, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  select timesheet_row.authorised_at_server into v_authorised_before
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000312';
  select contract_week.status::text into v_week_status
  from public.contract_weeks contract_week
  where contract_week.timesheet_id='b7000000-0000-4000-8000-000000000312';
  select pg_catalog.count(*) into v_jobs_before from public.banking_pay_workbench_jobs;
  select pg_catalog.count(*) into v_audit_before from public.audit_events
   where object_id_text='b7000000-0000-4000-8000-000000000312';

  perform pg_temp.assert_true(v_authorised_before is not null,
    'UNA-017: the root must be authorised before the forced failure');

  -- Fault injection at the first write AFTER the unauthorisation and the one
  -- aligned invalidation.  The installed invalidator's own ownership rule is
  -- not usable here: the owner refuses a foreign current TSFIN row before any
  -- write (proved below), which is stronger but does not exercise the rollback.
  execute $ddl$
    create trigger wp07_force_failure
      before update on public.weekly_source_root_authorisations
      for each row execute function pg_temp.force_withdrawal_failure()
  $ddl$;

  begin
    v_signature:=pg_temp.current_signature('b7000000-0000-4000-8000-000000000312');
    perform public.weekly_source_first_authorisation_withdraw_v1(
      'b7000000-0000-4000-8000-000000000312','b7000000-0000-4000-8000-000000000312',
      v_signature,'b7000000-0000-4000-8000-000000000001');
  exception when others then
    get stacked diagnostics v_message=message_text;
    v_failed:=v_message='WP07_FORCED_FAILURE_AFTER_UNAUTHORISATION';
    if not v_failed then
      get stacked diagnostics v_detail=pg_exception_detail;
      v_message:=v_message||' :: '||coalesce(v_detail,'');
    end if;
  end;

  execute 'drop trigger wp07_force_failure on public.weekly_source_root_authorisations';

  perform pg_temp.assert_true(v_failed,
    'UNA-017: the forced failure must propagate, got "'||coalesce(v_message,'<none>')||'"');

  -- Everything rolled back together.
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server from public.timesheets timesheet_row
      where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000312')
    is not distinct from v_authorised_before,
    'UNA-017: the unauthorisation must be rolled back');
  perform pg_temp.assert_true(
    (select contract_week.status::text from public.contract_weeks contract_week
      where contract_week.timesheet_id='b7000000-0000-4000-8000-000000000312')=v_week_status,
    'UNA-017: the Contract Week must be rolled back');
  perform pg_temp.assert_true(
    (select authorisation_row.withdrawn_at_utc is null
       from public.weekly_source_root_authorisations authorisation_row
      where authorisation_row.root_timesheet_id='b7000000-0000-4000-8000-000000000312'
        and authorisation_row.authorisation_generation=1),
    'UNA-017: the withdrawal marks must be rolled back');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_workbench_jobs)=v_jobs_before,
    'UNA-017: no dirty job may survive the rollback');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text='b7000000-0000-4000-8000-000000000312')=v_audit_before,
    'UNA-017: no audit entry may survive the rollback');
end
$verify_forced_rollback$;

-- ---------------------------------------------------------------------------
-- 14. Three-valued logic on the nullable Contract owner: the Candidate the
--     serial gate is pinned to fails closed (contracts.candidate_id is
--     nullable, and the installed invalidator refuses a pair whose Timesheet is
--     owned by another Candidate)
-- ---------------------------------------------------------------------------
do $verify_nullable_owner$
declare
  v_result jsonb;
  v_authorised_before timestamptz;
  v_owner uuid;
begin
  perform pg_temp.drain_workbench_jobs();

  -- The installed schema makes a SECOND current TSFIN row for one Timesheet
  -- impossible, so the invalidator's TIMESHEET_CANDIDATE_MISMATCH shape cannot
  -- be built on one root at all; recorded rather than assumed.
  perform pg_temp.assert_refused(
    $sql$insert into public.timesheets_financials(
      id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
      processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat
    ) values (
      'b7000000-0000-4000-8000-000000000596','b7000000-0000-4000-8000-000000000306',2,
      true,'b7000000-0000-4000-8000-000000000108','b7000000-0000-4000-8000-000000000002',
      'PENDING_AUTH'::public.ts_fin_processing_status_enum,1,1,1)$sql$,
    '%uq_tsfin_current%',
    'a second current TSFIN row for one Timesheet');

  -- The reachable case is the nullable Contract owner.  `is distinct from`
  -- rather than `<>` is what makes it refuse instead of silently passing.
  select timesheet_row.authorised_at_server into v_authorised_before
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000306';
  select contract_row.candidate_id into v_owner
  from public.contracts contract_row
  where contract_row.id='b7000000-0000-4000-8000-000000000206';

  update public.contracts set candidate_id=null
   where id='b7000000-0000-4000-8000-000000000206';
  perform pg_temp.drain_workbench_jobs();

  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'b7000000-0000-4000-8000-000000000306','b7000000-0000-4000-8000-000000000306',
    pg_temp.current_signature('b7000000-0000-4000-8000-000000000306'),
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
    and v_result->>'reason'='CANDIDATE_UNRESOLVED',
    'a null Contract owner must fail closed before any write, got '||v_result::text);
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server from public.timesheets timesheet_row
      where timesheet_row.timesheet_id='b7000000-0000-4000-8000-000000000306')
    is not distinct from v_authorised_before,
    'the refused call must write nothing');

  -- The availability function gives the same verdict as the write path.
  perform pg_temp.assert_true(
    (public.weekly_source_first_authorisation_withdraw_available_v1(
       'b7000000-0000-4000-8000-000000000306')->>'code')
    ='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
    'G3-4: the availability verdict must match the write path');

  update public.contracts set candidate_id=v_owner
   where id='b7000000-0000-4000-8000-000000000206';
  perform pg_temp.drain_workbench_jobs();
end
$verify_nullable_owner$;

-- ---------------------------------------------------------------------------
-- 15. UNA-003 — a Workbench result exists but no Draft does
-- ---------------------------------------------------------------------------
do $verify_workbench_result_no_draft$
declare
  v_scope_rows bigint;
begin
  select pg_catalog.count(*) into v_scope_rows
  from private.banking_pay_workbench_timesheet_scope_state
  where candidate_id='b7000000-0000-4000-8000-000000000101';
  perform pg_temp.assert_true(
    v_scope_rows>=1,
    'UNA-003: the Candidate must carry Workbench scope state from the earlier '
    ||'authorisation and withdrawal');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.pay_batch_items item_row
      where item_row.timesheet_id='b7000000-0000-4000-8000-000000000301')=0,
    'UNA-003: and no Draft item, which is the case the withdrawal already '
    ||'completed in section 3');
end
$verify_workbench_result_no_draft$;

-- ---------------------------------------------------------------------------
-- 16. THE ORDERING RULE (WP-03 handoff N16, carried into WP-07b)
--
--     Rule: the Timesheet must never be observable as UNAUTHORISED while its
--     Weekly Source authorisation record is still LIVE, because those are the
--     two facts the managed-root guard reads.  The withdrawal owner therefore
--     marks the record withdrawn before, or in the same statement as, anything
--     that unauthorises the Timesheet — unless the two writes are provably
--     unobservable apart, which is the case today.
--
--     WP-25, 18 September 2026.  This block used to say that the order
--     "unauthorise, then mark withdrawn" was safe because "NO trigger on
--     public.timesheets, public.timesheets_financials or public.contract_weeks
--     calls the managed-root guard", and (iii) below tested exactly that.
--     Both statements are now false, and were false before this edit:
--
--       * WP-24 attached `weekly_source_managed_root_authorisation_guard_bu` to
--         public.timesheets, which is precisely the ruling OR-2 condition this
--         section names;
--       * WP-07c moved the withdrawal marks BEFORE the call to
--         public.timesheet_unauthorise_atomic, which is the fix this section
--         demanded;
--       * and (iii)'s detector did not notice either, because it matched the
--         TRIGGER FUNCTION's own text for `weekly_source_managed_root_guard`
--         and WP-24's trigger reaches that guard one call further on.  Executed
--         on a clean build from empty, that query returned NULL while the
--         trigger was attached and firing.  A tripwire that reports all clear on
--         the condition it exists to catch is worse than no tripwire.
--
--     What (iii) asserts now: a transitive-closure detector, self-tested against
--     a deliberately broader probe so it can never again be narrower than the
--     class it claims to cover; and, when a guard trigger IS attached, the
--     ordering consequence proved BY EXECUTION — the fixture withdrawal below is
--     a managed root, so if the marks did not precede the unauthorise call the
--     guard would refuse it and this section would fail.
--
--     (i) and (ii) are unchanged: both writes in one transaction, no exception
--     handler, no transaction control, every row written under this
--     transaction's own id.  (i) is deliberately STATIC — the absence of an
--     exception handler is not an executable property — and is labelled so.
-- ---------------------------------------------------------------------------
do $verify_ordering_rule$
declare
  v_definition text;
  v_result jsonb;
  v_signature text;
  v_root uuid:='b7000000-0000-4000-8000-000000000313';
  v_xmin_timesheet text;
  v_xmin_financials text;
  v_xmin_week text;
  v_xmin_authorisation text;
  v_xmin_audit text;
  v_guard_triggers text;
  -- WP-25: the transitive closure of routines that reach the managed-root guard,
  -- the broader self-test probe, and the closure loop's pass counter.
  v_guard_closure oid[];
  v_guard_closure_new oid[];
  v_closure_pass integer;
  v_broad_triggers text;
  v_xid_before bigint;
  v_xid_after bigint;
begin
  v_definition:=pg_catalog.pg_get_functiondef(
    'public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)'::regprocedure);

  -- (i) one transaction, statically.  An EXCEPTION handler would put a write in
  -- a sub-transaction that can roll back on its own; transaction control or an
  -- out-of-transaction call would separate the two writes outright.
  perform pg_temp.assert_true(
    v_definition !~* '\mexception\s+when\m',
    'ordering rule: the withdrawal owner must contain no EXCEPTION handler, '
    ||'because that would place a write in a sub-transaction');
  perform pg_temp.assert_true(
    v_definition !~* '\m(commit|rollback|start\s+transaction|savepoint|dblink|dblink_exec|pg_background_launch)\m',
    'ordering rule: the withdrawal owner must contain no transaction control '
    ||'and no out-of-transaction call');
  perform pg_temp.assert_true(
    v_definition ~ 'ORDERING RULE',
    'ordering rule: the rule must be stated in plain English at the site');

  -- (ii) one transaction, EXECUTED.  Every row the sequence writes must carry
  -- the same xmin: the unauthorisation, the Contract Week, the withdrawal marks
  -- and the audit entry.  A future edit that commits between them, or wraps one
  -- in an exception block, changes one of these and this assertion fails.
  insert into public.candidates(id,display_name)
  values ('b7000000-0000-4000-8000-000000000113','WP07 Candidate 13');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
  ) values (
    'b7000000-0000-4000-8000-000000000213','b7000000-0000-4000-8000-000000000113',
    'b7000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
    '{}'::jsonb,'HEALTHROSTER',true,true,true,true);
  perform pg_temp.seed_timesheet(v_root,'WP07-BK-13',1,true,
    'b7000000-0000-4000-8000-000000000213');
  perform pg_temp.seed_week_and_financials(
    'b7000000-0000-4000-8000-000000000413','b7000000-0000-4000-8000-000000000213',
    v_root,'b7000000-0000-4000-8000-000000000513',
    'b7000000-0000-4000-8000-000000000113','b7000000-0000-4000-8000-000000000002',1);
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(v_root,v_root,null,
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'ordering rule: the fixture root must authorise, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
  v_signature:=pg_temp.current_signature(v_root);
  v_xid_before:=pg_catalog.pg_current_xact_id()::text::bigint;
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,v_signature,'b7000000-0000-4000-8000-000000000001');
  v_xid_after:=pg_catalog.pg_current_xact_id()::text::bigint;
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'ordering rule: the fixture withdrawal must succeed, got '||v_result::text);
  perform pg_temp.assert_true(
    v_xid_after=v_xid_before,
    'ordering rule: the withdrawal must not start a new top-level transaction');

  select timesheet_row.xmin::text into v_xmin_timesheet
    from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_root;
  select financial_row.xmin::text into v_xmin_financials
    from public.timesheets_financials financial_row
   where financial_row.timesheet_id=v_root and financial_row.is_current=true;
  select week_row.xmin::text into v_xmin_week
    from public.contract_weeks week_row where week_row.timesheet_id=v_root;
  select authorisation_row.xmin::text into v_xmin_authorisation
    from public.weekly_source_root_authorisations authorisation_row
   where authorisation_row.id=(v_result->>'root_authorisation_id')::uuid;
  select audit_row.xmin::text into v_xmin_audit
    from public.audit_events audit_row
   where audit_row.action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN'
     and audit_row.object_id_text=v_root::text;

  -- Every one of those rows must have been written inside THIS transaction, so
  -- every xmin is at or after this transaction's own top-level id.  The
  -- unchanged `public.timesheet_unauthorise_atomic` carries its own EXCEPTION
  -- handler, so its three lifecycle writes land in a SUB-transaction of ours and
  -- carry a later xid; that sub-transaction still rolls back with this one,
  -- which section 13 proves by forcing a failure after it.  This owner's own two
  -- writes — the withdrawal marks and the audit entry — must share one xid, and
  -- that xid must be the top-level one.
  perform pg_temp.assert_true(
    v_xmin_timesheet is not null
    and v_xmin_timesheet::bigint>=v_xid_before
    and v_xmin_financials::bigint>=v_xid_before
    and v_xmin_week::bigint>=v_xid_before
    and v_xmin_authorisation::bigint>=v_xid_before
    and v_xmin_audit::bigint>=v_xid_before,
    'ordering rule: every row the sequence writes must be written inside THIS '
    ||'transaction — xid='||v_xid_before::text
    ||' xmins were timesheet='||coalesce(v_xmin_timesheet,'<null>')
    ||' tsfin='||coalesce(v_xmin_financials,'<null>')
    ||' week='||coalesce(v_xmin_week,'<null>')
    ||' authorisation='||coalesce(v_xmin_authorisation,'<null>')
    ||' audit='||coalesce(v_xmin_audit,'<null>'));
  perform pg_temp.assert_true(
    v_xmin_authorisation=v_xmin_audit
    and v_xmin_authorisation::bigint=v_xid_before,
    'ordering rule: the withdrawal marks and the audit entry must be written by '
    ||'the withdrawal owner''s OWN frame, not a sub-transaction — authorisation='
    ||coalesce(v_xmin_authorisation,'<null>')||' audit='
    ||coalesce(v_xmin_audit,'<null>')||' top-level xid='||v_xid_before::text);

  -- (iii) the window test, EXECUTED (WP-25).  A concurrent session can never see
  -- the order, because PostgreSQL has no dirty read at any isolation level.  The
  -- only reader that can is an IN-TRANSACTION one: a trigger on a lifecycle
  -- relation that reaches the managed-root guard.
  --
  -- The detector walks the TRANSITIVE CLOSURE of the routines a trigger function
  -- reaches, because the single-level match this section used before missed
  -- WP-24's trigger, whose function calls
  -- `private.weekly_source_ordinary_authorisation_guard_v1`, which is what calls
  -- the guard.  `prokind='f'` is required: `pg_get_functiondef` raises on an
  -- aggregate, and both schemas contain some.
  select coalesce(pg_catalog.array_agg(seed.oid),array[]::oid[]) into v_guard_closure
  from (
    select routine.oid
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as routine_schema
      on routine_schema.oid=routine.pronamespace
    where routine_schema.nspname in ('public','private')
      and routine.prokind='f'
      and pg_catalog.pg_get_functiondef(routine.oid) ~* 'weekly_source_managed_root_guard'
  ) as seed;
  for v_closure_pass in 1..5 loop
    select coalesce(pg_catalog.array_agg(reached.oid),array[]::oid[]) into v_guard_closure_new
    from (
      select routine.oid
      from pg_catalog.pg_proc as routine
      join pg_catalog.pg_namespace as routine_schema
        on routine_schema.oid=routine.pronamespace
      where routine_schema.nspname in ('public','private')
        and routine.prokind='f'
        and not (routine.oid=any(v_guard_closure))
        and exists (
          select 1
          from pg_catalog.pg_proc as known
          join pg_catalog.pg_namespace as known_schema
            on known_schema.oid=known.pronamespace
          where known.oid=any(v_guard_closure)
            and pg_catalog.pg_get_functiondef(routine.oid)
                ~* ('\m'||known_schema.nspname||'\.'||known.proname||'\M'))
    ) as reached;
    exit when coalesce(pg_catalog.array_length(v_guard_closure_new,1),0)=0;
    v_guard_closure:=v_guard_closure||v_guard_closure_new;
  end loop;

  select pg_catalog.string_agg(
           trigger_class.relname||'.'||trigger_row.tgname,', ' order by trigger_row.tgname)
    into v_guard_triggers
  from pg_catalog.pg_trigger as trigger_row
  join pg_catalog.pg_class as trigger_class on trigger_class.oid=trigger_row.tgrelid
  join pg_catalog.pg_namespace as trigger_schema
    on trigger_schema.oid=trigger_class.relnamespace
  where trigger_row.tgisinternal is false
    and trigger_schema.nspname='public'
    and trigger_class.relname in ('timesheets','timesheets_financials','contract_weeks')
    and trigger_row.tgfoid=any(v_guard_closure);

  -- The detector's self-test.  A deliberately broader probe — any Weekly Source
  -- trigger at all on those three relations — must never see something the
  -- closure detector misses.  This is the assertion that would have caught the
  -- blind tripwire on the day WP-24 landed.
  select pg_catalog.string_agg(
           trigger_class.relname||'.'||trigger_row.tgname,', ' order by trigger_row.tgname)
    into v_broad_triggers
  from pg_catalog.pg_trigger as trigger_row
  join pg_catalog.pg_class as trigger_class on trigger_class.oid=trigger_row.tgrelid
  join pg_catalog.pg_namespace as trigger_schema
    on trigger_schema.oid=trigger_class.relnamespace
  join pg_catalog.pg_proc as trigger_function on trigger_function.oid=trigger_row.tgfoid
  where trigger_row.tgisinternal is false
    and trigger_schema.nspname='public'
    and trigger_class.relname in ('timesheets','timesheets_financials','contract_weeks')
    and trigger_function.prokind='f'
    and pg_catalog.pg_get_functiondef(trigger_function.oid) ~* 'weekly_source';
  perform pg_temp.assert_true(
    coalesce(v_broad_triggers,'')=coalesce(v_guard_triggers,''),
    'ordering rule: the guard-trigger detector is NARROWER than the class it '
    ||'claims to cover — the broad probe sees '||coalesce(v_broad_triggers,'<none>')
    ||' and the closure detector sees '||coalesce(v_guard_triggers,'<none>')
    ||'.  A tripwire that cannot see the condition it exists to catch must be '
    ||'widened, not trusted.');

  if v_guard_triggers is null then
    -- No in-transaction observer: the order inside the transaction is genuinely
    -- unobservable, and (i) and (ii) above are the whole proof.
    raise notice 'ORDERING RULE: no managed-root guard trigger is attached to a lifecycle relation, so the statement order inside the withdrawal transaction is unobservable and (i)+(ii) are the complete proof';
  else
    -- A guard trigger IS attached, so the order is observable IN TRANSACTION and
    -- the withdrawal marks MUST precede the call to
    -- public.timesheet_unauthorise_atomic.  That is proved by execution, not by
    -- reading the owner: the fixture root above is Weekly-Source-managed, the
    -- guard refuses an authorisation write on a managed root with no exemption
    -- for any caller, so the withdrawal could not have returned ok:true in the
    -- wrong order.  Put the marks back after the call and this assertion fails.
    --
    -- The guard's own behaviour — the refusal an ordinary Unauthorise receives,
    -- and the unchanged ordinary path — belongs to WP-24 and is proved in
    -- `supabase/verification/17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql`.
    -- It is deliberately not duplicated here.
    perform pg_temp.assert_true(
      coalesce((v_result->>'ok')::boolean,false)
      and coalesce((v_result->>'withdrawn')::boolean,false),
      'ordering rule / ruling OR-2: a managed-root guard trigger is attached ('
      ||v_guard_triggers||'), so the withdrawal marks must be written BEFORE the '
      ||'call to public.timesheet_unauthorise_atomic.  The fixture withdrawal of '
      ||'a managed root did not succeed, which is what the wrong order looks '
      ||'like: '||v_result::text);
    perform pg_temp.assert_true(
      (select authorisation_row.withdrawn_at_utc is not null
         from public.weekly_source_root_authorisations authorisation_row
        where authorisation_row.id=(v_result->>'root_authorisation_id')::uuid)
      and (select timesheet_row.authorised_at_server is null
             from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_root),
      'ordering rule: both writes landed — the generation is withdrawn and the '
      ||'Timesheet is unauthorised');
  end if;
  perform pg_temp.drain_workbench_jobs();
end
$verify_ordering_rule$;

-- ---------------------------------------------------------------------------
-- 17. WP-07b finding F2 — interface I-6's already-authorised test is
--     FAMILY-WIDE, so neither entry point can give a rotated family a second
--     live authorisation generation (proof/34 section 6)
-- ---------------------------------------------------------------------------
do $verify_i6_family_wide$
declare
  v_lock jsonb;
  v_core jsonb;
  v_office jsonb;
  v_current uuid:='b7000000-0000-4000-8000-000000000311';
  v_historical uuid:='b7000000-0000-4000-8000-000000000391';
  v_candidate uuid:='b7000000-0000-4000-8000-000000000111';
  v_contract uuid:='b7000000-0000-4000-8000-000000000211';
begin
  -- An eleventh Candidate with a family that rotated: v1 demoted, v2 current.
  insert into public.candidates(id,display_name)
  values (v_candidate,'WP07 Candidate 11');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
  ) values (
    v_contract,v_candidate,'b7000000-0000-4000-8000-000000000002',
    '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);
  perform pg_temp.seed_timesheet(v_historical,'WP07-BK-11',1,false,v_contract);
  perform pg_temp.seed_timesheet(v_current,'WP07-BK-11',2,true,v_contract);
  perform pg_temp.seed_week_and_financials(
    'b7000000-0000-4000-8000-000000000411',v_contract,v_current,
    'b7000000-0000-4000-8000-000000000511',v_candidate,
    'b7000000-0000-4000-8000-000000000002',2);
  perform pg_temp.drain_workbench_jobs();

  -- The state proof/34 section 6 calls an integrity failure: the live
  -- generation sits on a HISTORICAL member because the root rotated after the
  -- first authorisation.  WP-09's guard is not yet wired to all 28 entry
  -- points, so this shape is reachable and is planted directly.
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  ) values (v_historical,'WP07-BK-11',1,1,'planted-historical-signature',
            'b7000000-0000-4000-8000-000000000001');

  -- (a) The Office entry point reads the context first and already refused.
  v_office:=public.weekly_source_first_authorise_v1(v_current,v_current,null,
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_office->>'ok')::boolean,true) is false
    and v_office->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
    and v_office->>'reason'='LIVE_AUTHORISATION_NOT_ON_CANONICAL_ROW',
    'F2: the Office entry point must refuse a rotated family, got '||v_office::text);

  -- (b) WP-02's Gate 5 coordinator calls I-6 DIRECTLY with only an I-1 lock
  -- result and no context read.  Before this fix that path ACCEPTED, producing
  -- a second live generation in the family.
  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    v_candidate,array[v_current]::uuid[],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
    pg_catalog.gen_random_uuid(),'WP07B_F2');
  perform pg_temp.assert_true(
    coalesce((v_lock->>'ok')::boolean,false) and v_lock->>'gate'='GRANTED',
    'F2: the I-1 lock must be granted so the direct path is really exercised, '
    ||'got '||v_lock::text);
  v_core:=private.weekly_source_first_authorise_core_v1(
    v_current,null,'b7000000-0000-4000-8000-000000000001',v_lock);
  perform pg_temp.assert_true(
    coalesce((v_core->>'ok')::boolean,true) is false
    and v_core->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
    and v_core->>'reason'='LIVE_AUTHORISATION_NOT_ON_CANONICAL_ROW',
    'F2: interface I-6 must refuse a rotated family family-wide, got '
    ||v_core::text);

  -- Nothing was written by either entry point.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where pg_catalog.btrim(family_booking_id)='WP07-BK-11'
        and withdrawn_at_utc is null)=1,
    'F2: a rotated family can never gain a SECOND live generation');
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server is null
       from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_current),
    'F2: the refused call must not authorise the canonical row');

  -- With the family carrying no LIVE generation any more, the per-root refusal
  -- is unchanged: the canonical row authorises once, and a second direct call is
  -- refused WEEKLY_SOURCE_ROOT_ALREADY_AUTHORISED, not an integrity failure.
  -- The historical generation is marked withdrawn rather than deleted, because
  -- a generation can never be removed (decision D8).
  perform pg_temp.assert_refused(
    $sql$delete from public.weekly_source_root_authorisations
          where root_timesheet_id='b7000000-0000-4000-8000-000000000391'$sql$,
    'WEEKLY_SOURCE_IMMUTABLE_RECORD',
    'deleting an authorisation generation');
  update public.weekly_source_root_authorisations
     set withdrawn_at_utc=pg_catalog.clock_timestamp(),
         withdrawn_by_user_id='b7000000-0000-4000-8000-000000000001'
   where root_timesheet_id=v_historical;
  perform pg_temp.drain_workbench_jobs();
  v_office:=public.weekly_source_first_authorise_v1(v_current,v_current,null,
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_office->>'ok')::boolean,false)
    and (v_office->>'authorisation_generation')::integer=1,
    'F2: the canonical row must still authorise normally, got '||v_office::text);
  perform pg_temp.drain_workbench_jobs();
  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    v_candidate,array[v_current]::uuid[],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
    pg_catalog.gen_random_uuid(),'WP07B_F2');
  v_core:=private.weekly_source_first_authorise_core_v1(
    v_current,null,'b7000000-0000-4000-8000-000000000001',v_lock);
  perform pg_temp.assert_true(
    coalesce((v_core->>'ok')::boolean,true) is false
    and v_core->>'code'='WEEKLY_SOURCE_ROOT_ALREADY_AUTHORISED',
    'F2: the per-root refusal must be unchanged, got '||v_core::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_i6_family_wide$;

-- ---------------------------------------------------------------------------
-- 18. WP-07b findings F3 and F4 — one meaning of `ok` across the package, and
--     the declared dependence of the exact replay on audit retention
-- ---------------------------------------------------------------------------
do $verify_ok_meaning_and_replay$
declare
  v_avail jsonb;
  v_result jsonb;
  v_replay jsonb;
  v_signature text;
  v_root uuid:='b7000000-0000-4000-8000-000000000314';
  v_definition text;
  v_removed bigint;
begin
  insert into public.candidates(id,display_name)
  values ('b7000000-0000-4000-8000-000000000114','WP07 Candidate 14');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
  ) values (
    'b7000000-0000-4000-8000-000000000214','b7000000-0000-4000-8000-000000000114',
    'b7000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
    '{}'::jsonb,'HEALTHROSTER',true,true,true,true);
  perform pg_temp.seed_timesheet(v_root,'WP07-BK-14',1,true,
    'b7000000-0000-4000-8000-000000000214');
  perform pg_temp.seed_week_and_financials(
    'b7000000-0000-4000-8000-000000000414','b7000000-0000-4000-8000-000000000214',
    v_root,'b7000000-0000-4000-8000-000000000514',
    'b7000000-0000-4000-8000-000000000114','b7000000-0000-4000-8000-000000000002',1);
  perform pg_temp.drain_workbench_jobs();

  -- F3, meaning 1: an unmanaged root.  `ok` is false because the action is not
  -- permitted, and `available` says the same thing.
  v_avail:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'b7000000-0000-4000-8000-000000000308');
  perform pg_temp.assert_true(
    coalesce((v_avail->>'ok')::boolean,true) is false
    and coalesce((v_avail->>'available')::boolean,true) is false
    and v_avail->>'code'='WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT'
    and nullif(pg_catalog.btrim(coalesce(v_avail->>'refusal_message','')),'') is not null,
    'F3: the availability function must report ok=false when the action is not '
    ||'available, got '||v_avail::text);

  -- F3, meaning 2: an available root.  `ok` is true and equals `available`.
  v_result:=public.weekly_source_first_authorise_v1(v_root,v_root,null,
    'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'F3: the fixture root must authorise, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
  v_avail:=public.weekly_source_first_authorisation_withdraw_available_v1(v_root);
  perform pg_temp.assert_true(
    coalesce((v_avail->>'ok')::boolean,false)
    and coalesce((v_avail->>'available')::boolean,false)
    and (v_avail->>'code') is null
    and (v_avail->>'refusal_message') is null,
    'F3: an available root must report ok=true, available=true and no code, got '
    ||v_avail::text);

  -- F3: `ok` never disagrees with `available` in this function, whatever the
  -- verdict.  Asserted over every fixture root this verifier has touched.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.timesheets timesheet_row
       cross join lateral public.weekly_source_first_authorisation_withdraw_available_v1(
         timesheet_row.timesheet_id) as verdict
      where timesheet_row.booking_id like 'WP07-BK-%'
        and timesheet_row.is_current=true
        and coalesce((verdict->>'ok')::boolean,false)
            is distinct from coalesce((verdict->>'available')::boolean,false))=0,
    'F3: ok and available must agree for every root, always');

  -- F4 IS CLOSED BY ROUND 5 SECTION A3 STEP 5.  The exact replay used to read
  -- the recorded result out of the audit trail, so an audit-retention rule could
  -- silently turn a recorded success into a refusal; WP-07b declared that
  -- dependence rather than fixing it, because it owned no schema.  Ruling A3
  -- step 5 puts a durable replay receipt in the atomic write set, so the replay
  -- now reads the receipt.  This section proves the OLD dependence is gone by
  -- EXECUTION: the audit event is deleted exactly as a retention policy would,
  -- and the replay still returns the recorded result.
  v_signature:=pg_temp.current_signature(v_root);
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'F4: the withdrawal must succeed, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  v_replay:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_replay->>'replayed')::boolean,false)
    and coalesce((v_replay->>'ok')::boolean,false),
    'F4: the exact replay must return the recorded result, got '||v_replay::text);

  -- Remove the audit event, exactly as a retention policy would.
  delete from public.audit_events
   where action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN'
     and object_id_text=v_root::text;
  get diagnostics v_removed=row_count;
  perform pg_temp.assert_true(v_removed>=1,
    'F4: the fixture must actually remove the recorded event');

  v_replay:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,v_signature,'b7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_replay->>'ok')::boolean,false)
    and coalesce((v_replay->>'replayed')::boolean,false)
    and (v_replay->>'withdrawal_receipt_id') is not null,
    'F4 CLOSED: with the audit row gone the exact replay must STILL return the '
    ||'recorded result from the durable receipt, got '||v_replay::text);
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server is null
       from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_root)
    and (select pg_catalog.count(*) from public.weekly_source_root_authorisations
          where root_timesheet_id=v_root and withdrawn_at_utc is null)=0
    and (select pg_catalog.count(*)
           from private.weekly_source_first_authorisation_withdrawal_receipts
          where root_timesheet_id=v_root)=1,
    'F4: and a replay must never re-execute: one receipt, still withdrawn');
  perform pg_temp.drain_workbench_jobs();
end
$verify_ok_meaning_and_replay$;

-- ---------------------------------------------------------------------------
-- 19. WP-07b finding F5 — W4's WRITE_OFF refusal is STRICTER than proof/36
--     section 4 W4 and than round 4 ruling 5 item 5, and is now a declared
--     addition (D4) at its own site, with its justification
-- ---------------------------------------------------------------------------
do $verify_write_off_declared$
declare
  v_definition text;
  v_message text;
begin
  v_definition:=pg_catalog.pg_get_functiondef(
    'private.weekly_source_first_authorisation_withdraw_checks_v1(jsonb,jsonb,uuid,text)'::regprocedure);
  perform pg_temp.assert_true(
    v_definition ~ 'DECLARED ADDITION D4',
    'F5: the W4 WRITE_OFF rule must be declared as an addition at its own site');
  perform pg_temp.assert_true(
    v_definition ~ 'STRICTER than proof/36',
    'F5: and the declaration must say plainly that it is stricter than the pack');
  perform pg_temp.assert_true(
    v_definition ~ 'RESERVATION_RELEASED_BY_WRITE_OFF',
    'F5: the rule itself must still be present and fail-closed');
  -- The behaviour it declares is proved by section 9 against a real fixture.

  -- ROUND 5 SECTION A5 also fixes the WORDING.  The declaration must say so at
  -- the site, and the message itself is proved, on a clean fixture whose only
  -- failing check is the write-off, in
  -- `17092026_0610_weekly_source_withdrawal_supersession_v1.sql` section 8.
  perform pg_temp.assert_true(
    v_definition ~ 'ROUND 5 SECTION A5',
    'A5: the write-off refusal must cite the round-5 ruling that fixed its '
    ||'wording and its review disposition');
  v_message:=null;
end
$verify_write_off_declared$;

select 'WP-07 GATE 3 VERIFIER: ALL SECTIONS PASSED' as result;

rollback;
