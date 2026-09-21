-- Rollback-only PostgreSQL 17 proof for the Plan 6.2 Gate 11 audit, export and
-- notification owners (`17092026_1200_weekly_source_audit_and_export_v1.sql`).
--
-- Covers, in order:
--   1. structure, ownership, security, volatility and privileges of every
--      function and every trigger this package adds;
--   2. the static money and evidence contract: the export owner performs no
--      currency-to-hours arithmetic, never reads the timesheet_pay_state
--      last-settled cache, and reaches paid hours only through the Gate 9
--      settlement-allocation reader; no safety decision rests on LIMIT or sort
--      order;
--   3. the Candidate payload scanner over COMPLETE serialised payloads, with
--      positive and negative controls at depth and a nested-key control;
--   4. first authorisation driven through the REAL owner
--      `public.weekly_source_first_authorise_v1`, its audit event, its plain
--      English, and the chronology that renders it;
--   5. withdrawal through the REAL owner, proving this package adds no second
--      `WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN` row (UNA-001 stays true)
--      and that the chronology renders WP-07's row in plain English;
--   6. head publication IMMEDIATE and DEFERRED, supersession, and the four
--      pending-bundle states, each from a database state that genuinely
--      produces it;
--   7. the export differentials: submitted, source, approved and paid kept
--      apart; a rotated family whose physical Timesheet changed; an
--      `UNAVAILABLE` settlement; and an ordinary Timesheet whose export member
--      is byte-identically empty;
--   8. the Candidate hours-only push through the existing boundary: the whole
--      serialised payload carries no forbidden field and no forbidden word, a
--      forbidden payload fails closed and pushes nothing, the same approved
--      hours never push twice, and a push failure never rolls back a
--      publication;
--   9. the notification routes: the grouped manager email and its secure
--      response exist only on the source-authority route, and Office Weekly
--      source notices never enter the Banking alert store.
--
-- Prerequisites: the Weekly Source schema migration, the ACL contract, the
-- rotation authority (WP-03), the freeze census (WP-08a), first authorisation
-- (WP-07), pending release (WP-08b), the settlement allocation reader and the
-- Candidate view producer (WP-11a), and this package's repeatable.
--
-- Nothing here defines, wraps or re-creates a Banking Pay, Draft, execution,
-- cancellation, settlement, provider, recovery or remittance owner. No message
-- of any kind leaves the database: the Candidate push boundary writes an
-- in-database `public.candidate_notifications` row with `push_state='PENDING'`
-- and the delivery worker that would claim it is never run. Everything written
-- is rolled back.

\set ON_ERROR_STOP on
\pset pager off

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

create function pg_temp.assert_eq(p_left text,p_right text,p_message text)
returns void language plpgsql as $function$
begin
  if p_left is distinct from p_right then
    raise exception 'ASSERTION_FAILED: % (got %, expected %)',
      p_message,coalesce(p_left,'<null>'),coalesce(p_right,'<null>');
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

create function pg_temp.seed_timesheet(
  p_timesheet_id uuid,p_booking_id text,p_version integer,p_is_current boolean,
  p_contract_id uuid,p_schedule jsonb
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
    'HOURS'::public.timesheet_line_type_enum,'wp14-occupant','wp14-hospital',
    'wp14-ward','wp14-role','weekly-0','2026-09-13',p_contract_id,
    coalesce(p_schedule,'[]'::jsonb),'{}'::jsonb,false,
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

-- ===========================================================================
-- 1. Structure, ownership, security, volatility and privileges
-- ===========================================================================
do $verify_structure$
declare
  v_proc record;
begin
  for v_proc in
    select 'private.weekly_source_candidate_forbidden_words_v1()' as ident,'i' as vol,false as public_rpc
    union all select 'private.weekly_source_candidate_forbidden_key_parts_v1()','i',false
    union all select 'private.weekly_source_jsonb_atoms_v1(jsonb)','i',false
    union all select 'private.weekly_source_candidate_payload_safe_v1(jsonb)','i',false
    union all select 'private.weekly_source_audit_references_v1(jsonb,jsonb)','i',false
    union all select 'private.weekly_source_audit_human_reason_v1(text)','i',false
    union all select 'private.weekly_source_audit_sentence_v1(text,jsonb,jsonb,text)','i',false
    union all select 'private.weekly_source_audit_family_v1(uuid)','s',false
    union all select 'private.weekly_source_audit_key_v1(uuid)','s',false
    union all select 'private.weekly_source_audit_guard_refusal_v1(uuid,jsonb,text,uuid)','v',false
    -- WP-14c, ruling A2 / decision D13.
    union all select 'private.weekly_source_guard_refusal_bases_v1()','i',false
    union all select 'private.weekly_source_guard_refusal_basis_clause_v1(text)','i',false
    union all select 'private.weekly_source_guard_refusal_entry_point_installed_v1(text)','s',false
    union all select 'private.weekly_source_guard_refusal_detail_v1(jsonb)','i',false
    union all select 'private.weekly_source_guard_refusal_record_v1(text,jsonb,text,uuid)','v',false
    union all select 'public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb)','v',true
    union all select 'private.weekly_source_audit_lifecycle_rank_v1(text)','i',false
    union all select 'private.weekly_source_audit_chronology_v1(uuid)','s',false
    union all select 'private.weekly_source_export_submitted_hours_v1(uuid)','s',false
    union all select 'private.weekly_source_export_source_hours_v1(uuid)','s',false
    union all select 'private.weekly_source_export_approved_hours_v1(uuid)','s',false
    union all select 'private.weekly_source_export_invoice_movements_v1(uuid)','s',false
    union all select 'private.weekly_source_export_hours_v1(uuid)','s',false
    union all select 'private.weekly_source_candidate_hours_push_payload_v1(uuid)','s',false
    union all select 'private.weekly_source_candidate_hours_push_v1(uuid)','v',false
    union all select 'private.weekly_source_notification_route_contract_v1()','s',false
    union all select 'public.weekly_source_audit_guard_refusal_record_v1(jsonb)','v',true
    union all select 'public.weekly_source_timesheet_audit_chronology_v1(jsonb)','s',true
    union all select 'public.weekly_source_timesheet_hours_export_v1(jsonb)','s',true
    union all select 'public.weekly_source_invoice_report_rows_v1(jsonb)','s',true
    union all select 'public.weekly_source_candidate_hours_push_v1(jsonb)','v',true
  loop
    perform pg_temp.assert_true(
      to_regprocedure(v_proc.ident) is not null,'function missing: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select p.provolatile from pg_proc p where p.oid=to_regprocedure(v_proc.ident))
        =v_proc.vol,
      'wrong volatility: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select r.rolname from pg_proc p join pg_roles r on r.oid=p.proowner
        where p.oid=to_regprocedure(v_proc.ident))='postgres',
      'wrong owner: '||v_proc.ident);
    -- Nothing here is executable by a browser role, ever.
    perform pg_temp.assert_true(
      not exists(
        select 1 from pg_proc p,
          aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) as acl
        join pg_roles grantee on grantee.oid=acl.grantee
        where p.oid=to_regprocedure(v_proc.ident)
          and grantee.rolname in ('anon','authenticated')),
      'executable by a browser role: '||v_proc.ident);
    perform pg_temp.assert_true(
      not exists(
        select 1 from pg_proc p,
          aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) as acl
        where p.oid=to_regprocedure(v_proc.ident) and acl.grantee=0),
      'executable by PUBLIC: '||v_proc.ident);
    if v_proc.public_rpc then
      perform pg_temp.assert_true(
        exists(
          select 1 from pg_proc p,
            aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) as acl
          join pg_roles grantee on grantee.oid=acl.grantee
          where p.oid=to_regprocedure(v_proc.ident)
            and grantee.rolname='service_role'),
        'service RPC must be executable by service_role: '||v_proc.ident);
    else
      perform pg_temp.assert_true(
        not exists(
          select 1 from pg_proc p,
            aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) as acl
          join pg_roles grantee on grantee.oid=acl.grantee
          where p.oid=to_regprocedure(v_proc.ident)
            and grantee.rolname='service_role'),
        'private helper must not be executable by service_role: '||v_proc.ident);
    end if;
    -- Definer functions must pin their search_path.
    perform pg_temp.assert_true(
      (select p.prosecdef from pg_proc p where p.oid=to_regprocedure(v_proc.ident))
        is not null,
      'security flag unreadable: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select coalesce(pg_catalog.array_to_string(p.proconfig,','),'')
         from pg_proc p where p.oid=to_regprocedure(v_proc.ident))
        like 'search_path=%',
      'search_path is not pinned: '||v_proc.ident);
  end loop;

  -- STEP 6 / hostile-review F-02: the chronology owner depends on a database-
  -- owned ordering fact, not a timestamp or random identifier.
  perform pg_temp.assert_true(
    exists(
      select 1
      from pg_catalog.pg_attribute as attribute_row
      where attribute_row.attrelid='public.audit_events'::pg_catalog.regclass
        and attribute_row.attname='event_sequence'
        and attribute_row.attidentity='a'
        and attribute_row.attnotnull
        and not attribute_row.attisdropped),
    'audit_events.event_sequence is not a GENERATED ALWAYS identity');
  perform pg_temp.assert_true(
    exists(
      select 1
      from pg_catalog.pg_attribute as attribute_row
      where attribute_row.attrelid='public.audit_events'::pg_catalog.regclass
        and attribute_row.attname='event_sequence_is_authoritative'
        and attribute_row.attnotnull
        and not attribute_row.attisdropped),
    'audit_events.event_sequence_is_authoritative is missing or nullable');
  perform pg_temp.assert_true(
    exists(
      select 1
      from pg_catalog.pg_indexes as index_row
      where index_row.schemaname='public'
        and index_row.tablename='audit_events'
        and index_row.indexname='audit_events_event_sequence_uq'
        and index_row.indexdef like 'CREATE UNIQUE INDEX%'),
    'the durable audit event sequence is not uniquely indexed');

  -- Exactly the four triggers this package installs, on the three relations the
  -- owners write, all AFTER and all FOR EACH ROW.
  for v_proc in
    select 'weekly_source_audit_first_authorisation' as trigger_name,
           'weekly_source_root_authorisations' as relation
    union all select 'weekly_source_audit_entitlement_head','weekly_source_entitlement_heads'
    union all select 'weekly_source_audit_pending_bundle','weekly_source_pending_entitlement_bundles'
    union all select 'weekly_source_candidate_hours_push_head','weekly_source_entitlement_heads'
  loop
    perform pg_temp.assert_true(
      exists(
        select 1 from pg_trigger t
        join pg_class c on c.oid=t.tgrelid
        where t.tgname=v_proc.trigger_name and c.relname=v_proc.relation
          and not t.tgisinternal
          and (t.tgtype & 1)=1        -- FOR EACH ROW
          and (t.tgtype & 2)=0),      -- AFTER, not BEFORE
      'missing AFTER ROW trigger '||v_proc.trigger_name||' on '||v_proc.relation);
  end loop;
end
$verify_structure$;

-- ===========================================================================
-- 2. The static money and evidence contract
-- ===========================================================================
do $verify_static_contract$
declare
  v_export text:=pg_catalog.pg_get_functiondef(
    to_regprocedure('private.weekly_source_export_hours_v1(uuid)'));
  v_all text;
begin
  select pg_catalog.string_agg(pg_catalog.pg_get_functiondef(p.oid),E'\n')
    into v_all
  from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where (n.nspname,p.proname) in (
    ('private','weekly_source_export_hours_v1'),
    ('private','weekly_source_export_submitted_hours_v1'),
    ('private','weekly_source_export_source_hours_v1'),
    ('private','weekly_source_export_approved_hours_v1'),
    ('private','weekly_source_export_invoice_movements_v1'));

  -- XSG-029: paid hours come only from the Gate 9 allocation reader.
  perform pg_temp.assert_true(
    v_export like '%weekly_source_settlement_allocation_v1%',
    'the export composer must reach paid hours through the settlement allocation reader');
  perform pg_temp.assert_true(
    v_export like '%SETTLEMENT_ALLOCATION%',
    'the export must name its paid-hours authority');

  -- Never the last-settled cache.
  perform pg_temp.assert_true(
    v_all not like '%last_settled_pay_batch_id%'
    and v_all not like '%last_settled_signature%'
    and v_all not like '%timesheet_pay_state%',
    'no export owner may read the timesheet_pay_state last-settled cache');

  -- Never a currency-to-hours calculation: no money column is read at all.
  perform pg_temp.assert_true(
    v_all not like '%amount_inc_vat%'
    and v_all not like '%total_pay_ex_vat%'
    and v_all not like '%net_amount%'
    and v_all not like '%unit_pay_rate%'
    and v_all not like '%unit_charge_rate%',
    'no export owner may read a money column to derive hours');

  -- Safety never rests on sort order or a row cap.
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      to_regprocedure('private.weekly_source_candidate_payload_safe_v1(jsonb)'))
      not like '% limit %',
    'the payload scanner must not decide safety with LIMIT');
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      to_regprocedure('private.weekly_source_export_approved_hours_v1(uuid)'))
      not like '% limit %',
    'the approved-hours reader must not resolve a contradiction with LIMIT');
end
$verify_static_contract$;

-- ===========================================================================
-- 3. The Candidate payload scanner
-- ===========================================================================
do $verify_scanner$
declare
  v_verdict jsonb;
begin
  -- A clean hours-only payload.
  v_verdict:=private.weekly_source_candidate_payload_safe_v1(
    '{"event_type":"TIMESHEET_HOURS_UPDATED",
      "template_key":"approved-hours-updated-v1",
      "template_params":{"week_ending_date":"2026-09-13",
        "approved_hours":[{"row_key":"approved-1","worked":true,"date":"2026-09-07",
          "start":"08:00","end":"16:00"}]}}'::jsonb);
  perform pg_temp.assert_true((v_verdict->>'ok')::boolean,
    'a clean hours-only payload must pass: '||v_verdict::text);

  -- Each forbidden word, at depth, inside a string value.
  for v_verdict in
    select private.weekly_source_candidate_payload_safe_v1(
      pg_catalog.jsonb_build_object('a',pg_catalog.jsonb_build_object(
        'b',pg_catalog.jsonb_build_array('x','the '||word.value||' says so'))))
    from pg_catalog.unnest(array['source','protected','exceptional','reconciliation'])
      as word(value)
  loop
    perform pg_temp.assert_true(
      (v_verdict->>'ok')::boolean is false
      and v_verdict->>'reason'='FORBIDDEN_WORD',
      'a forbidden word at depth must fail closed: '||v_verdict::text);
  end loop;

  -- A forbidden word in a KEY at depth, not only in a value.
  v_verdict:=private.weekly_source_candidate_payload_safe_v1(
    '{"a":{"b":[{"source_reference":"x"}]}}'::jsonb);
  perform pg_temp.assert_true(
    (v_verdict->>'ok')::boolean is false,
    'a forbidden word in a nested key must fail closed: '||v_verdict::text);

  -- Money, payment history, recovery and remittance field families.
  for v_verdict in
    select private.weekly_source_candidate_payload_safe_v1(
      pg_catalog.jsonb_build_object('outer',pg_catalog.jsonb_build_object(key.value,1)))
    from pg_catalog.unnest(array[
      'total_pay_ex_vat','net_amount','pay_batch_id','remittance_url',
      'recovery_history','settlement_state','bank_transfer_id','advance_id'])
      as key(value)
  loop
    perform pg_temp.assert_true(
      (v_verdict->>'ok')::boolean is false
      and v_verdict->>'reason'='FORBIDDEN_FIELD',
      'a forbidden field must fail closed: '||v_verdict::text);
  end loop;

  -- A non-object payload is refused rather than assumed safe.
  v_verdict:=private.weekly_source_candidate_payload_safe_v1('[]'::jsonb);
  perform pg_temp.assert_true(
    (v_verdict->>'ok')::boolean is false
    and v_verdict->>'reason'='PAYLOAD_NOT_AN_OBJECT',
    'a non-object payload must be refused: '||v_verdict::text);
  v_verdict:=private.weekly_source_candidate_payload_safe_v1(null);
  perform pg_temp.assert_true(
    (v_verdict->>'ok')::boolean is false,
    'a null payload must be refused: '||coalesce(v_verdict::text,'<null>'));
end
$verify_scanner$;

-- ===========================================================================
-- Fixture world.  One Client, four Candidates, one Contract each, one Weekly
-- HOURS Timesheet family each.  Candidate 3's family is ROTATED: version 1 was
-- demoted and version 2 is current, so the export and the chronology must read
-- the whole family and not one physical row.
-- ===========================================================================
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('c4000000-0000-4000-8000-000000000001','wp14-office@example.test','admin',true,'not-a-login');
insert into public.clients(id,name) values ('c4000000-0000-4000-8000-000000000002','WP14 Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('c4000000-0000-4000-8000-000000000002',20,'2026-01-01');

do $seed_world$
declare
  v_index integer;
  v_candidate uuid;
  v_contract uuid;
  v_timesheet uuid;
begin
  for v_index in 1..4 loop
    v_candidate:=('c4000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_contract:=('c4000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    insert into public.candidates(id,display_name)
    values (v_candidate,'WP14 Candidate '||v_index);
    insert into public.contracts(
      id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
      weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
    ) values (
      v_contract,v_candidate,'c4000000-0000-4000-8000-000000000002',
      '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);

    v_timesheet:=('c4000000-0000-4000-8000-0000000003'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    if v_index=3 then
      perform pg_temp.seed_timesheet(
        'c4000000-0000-4000-8000-000000000393','WP14-BK-03',1,false,v_contract,null);
      perform pg_temp.seed_timesheet(v_timesheet,'WP14-BK-03',2,true,v_contract,null);
      perform pg_temp.seed_week_and_financials(
        ('c4000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c4000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c4000000-0000-4000-8000-000000000002',2);
    else
      perform pg_temp.seed_timesheet(
        v_timesheet,'WP14-BK-'||pg_catalog.lpad(v_index::text,2,'0'),1,true,v_contract,
        case when v_index=1 then
          '[{"worked_start_iso":"2026-09-07T08:00:00Z","worked_end_iso":"2026-09-07T16:00:00Z","break_minutes":30}]'::jsonb
        else null end);
      perform pg_temp.seed_week_and_financials(
        ('c4000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c4000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c4000000-0000-4000-8000-000000000002',1);
    end if;
  end loop;
end
$seed_world$;

-- Candidate 1 has submitted: the Candidate evidence pair is present.
update public.timesheets
   set r2_nurse_key='test-only/nurse-signature.png',
       img_sha256_nurse=repeat('a',64)
 where timesheet_id='c4000000-0000-4000-8000-000000000301';

select pg_temp.drain_workbench_jobs();

-- ===========================================================================
-- 4. First authorisation through the REAL owner, its event and its chronology
-- ===========================================================================
do $verify_first_authorisation$
declare
  v_result jsonb;
  v_audit public.audit_events%rowtype;
  v_chronology jsonb;
  v_count integer;
begin
  select pg_catalog.count(*)::integer into v_count from public.audit_events
   where object_id_text='c4000000-0000-4000-8000-000000000301'
     and action='WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED';
  perform pg_temp.assert_true(v_count=0,
    'no first-authorisation event may exist before the owner runs');

  v_result:=public.weekly_source_first_authorise_v1(
    'c4000000-0000-4000-8000-000000000301','c4000000-0000-4000-8000-000000000301',
    null,'c4000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'the real first-authorisation owner must succeed: '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  select * into v_audit from public.audit_events
   where object_id_text='c4000000-0000-4000-8000-000000000301'
     and action='WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED';
  perform pg_temp.assert_true(v_audit.id is not null,
    'the real first authorisation must produce exactly one audit event');
  perform pg_temp.assert_eq(v_audit.object_type,'timesheets',
    'the event is keyed to the Timesheet, because 24 section 18 is the Timesheet Audit tab');
  perform pg_temp.assert_true(
    v_audit.actor_user_id='c4000000-0000-4000-8000-000000000001'
    and v_audit.actor_display is not null,
    'the event carries the acting Office user');
  perform pg_temp.assert_true(
    v_audit.after_json->>'family_booking_id'='WP14-BK-01'
    and (v_audit.after_json->>'authorisation_generation')::integer=1,
    'the event carries the family and the generation: '||v_audit.after_json::text);
  -- Plain English, not an action code and not raw JSON.
  perform pg_temp.assert_true(
    v_audit.after_json->>'narrative'
      ='Office authorised this week for pay for the first time.',
    'the event carries its own plain-English sentence, got '
      ||coalesce(v_audit.after_json->>'narrative','<null>'));

  v_chronology:=public.weekly_source_timesheet_audit_chronology_v1(
    pg_catalog.jsonb_build_object(
      'timesheet_id','c4000000-0000-4000-8000-000000000301'));
  perform pg_temp.assert_true((v_chronology->>'ok')::boolean,
    'the chronology reader must answer');
  perform pg_temp.assert_true(
    exists(
      select 1 from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      where event.value->>'event'='WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED'
        and event.value->>'narrative'
            ='Office authorised this week for pay for the first time.'),
    'the chronology must render the first authorisation in plain English: '
      ||v_chronology::text);
  -- Every rendered event has a sentence: Office never needs the raw JSON.
  perform pg_temp.assert_true(
    not exists(
      select 1 from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      where nullif(pg_catalog.btrim(coalesce(event.value->>'narrative','')),'') is null),
    'every chronology row must carry a sentence');

  -- The request contract of the service RPC.
  perform pg_temp.assert_refused(
    $sql$select public.weekly_source_timesheet_audit_chronology_v1(
      '{"timesheet_id":"c4000000-0000-4000-8000-000000000301","extra":1}'::jsonb)$sql$,
    '%WEEKLY_SOURCE_AUDIT_CHRONOLOGY_REQUEST_INVALID%',
    'an unknown request key');
end
$verify_first_authorisation$;

-- ===========================================================================
-- 5. Withdrawal: WP-07's event stands alone, and the chronology renders it
-- ===========================================================================
do $verify_withdrawal$
declare
  v_signature text;
  v_result jsonb;
  v_chronology jsonb;
begin
  select nullif(pg_catalog.btrim(coalesce(
           signature->>'backend_row_signature',signature->>'row_signature','')),'')
    into v_signature
  from public.timesheet_lifecycle_guard_signature_v1(
    'c4000000-0000-4000-8000-000000000301',
    (select contract_week.id from public.contract_weeks contract_week
      where contract_week.timesheet_id='c4000000-0000-4000-8000-000000000301'),
    false) as signature;

  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'c4000000-0000-4000-8000-000000000301','c4000000-0000-4000-8000-000000000301',
    v_signature,'c4000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'the real withdrawal owner must succeed: '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  -- UNA-001 must stay true: this package adds no second withdrawal row.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000301'
        and action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN')=1,
    'exactly one WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN row, as UNA-001 requires');
  -- And no first-authorisation event is written by the withdrawal UPDATE.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000301'
        and action='WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED')=1,
    'the withdrawal must not manufacture a second first-authorisation event');

  -- The chronology gives WP-07's row a sentence even though WP-07 stores none.
  v_chronology:=private.weekly_source_audit_chronology_v1(
    'c4000000-0000-4000-8000-000000000301');
  perform pg_temp.assert_true(
    exists(
      select 1 from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      where event.value->>'event'='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN'
        and event.value->>'narrative' like 'Office withdrew the first authorisation%'),
    'the chronology must render the withdrawal in plain English: '||v_chronology::text);

  -- Re-authorise so the later sections have a live managed root, and prove
  -- generation 2 produces its own event.
  v_result:=public.weekly_source_first_authorise_v1(
    'c4000000-0000-4000-8000-000000000301','c4000000-0000-4000-8000-000000000301',
    null,'c4000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    're-authorisation after withdrawal must succeed: '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000301'
        and action='WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED')=2,
    'generation 2 produces its own first-authorisation event');
end
$verify_withdrawal$;

-- ===========================================================================
-- 6. Guard refusal: recorded only when the guard really refuses, and the
--    decision is never taken from the caller
-- ===========================================================================
do $verify_guard_refusal$
declare
  v_result jsonb;
begin
  -- Candidate 1's root is authorised, so the installed guard reports managed.
  v_result:=public.weekly_source_audit_guard_refusal_record_v1(
    pg_catalog.jsonb_build_object(
      'timesheet_id','c4000000-0000-4000-8000-000000000301',
      'entry_point','E1 timesheet_route_version_rotate',
      'actor_user_id','c4000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean and (v_result->>'recorded')::boolean,
    'a genuinely managed root must record a refusal: '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000301'
        and action='WEEKLY_SOURCE_ROTATION_REFUSED')=1,
    'exactly one rotation-refusal event');
  perform pg_temp.assert_true(
    (select after_json->>'narrative' from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000301'
        and action='WEEKLY_SOURCE_ROTATION_REFUSED')
      like 'Office checked whether this Timesheet could be replaced; it could not%',
    'the refusal is explained in plain English, as what the recorder KNOWS '
      ||'(WP-14b F9: the recorder is not bound to an attempted rotation, so it '
      ||'must not assert that a replacement was requested)');
  perform pg_temp.assert_true(
    (select after_json->>'narrative' from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000301'
        and action='WEEKLY_SOURCE_ROTATION_REFUSED')
      not like '%was refused%',
    'WP-14b F9: the sentence never claims a refusal of a request that may not '
      ||'have been made');

  -- Candidate 2's root is NOT authorised: nothing is recorded, and a caller
  -- cannot assert a refusal that the guard did not make.
  v_result:=public.weekly_source_audit_guard_refusal_record_v1(
    pg_catalog.jsonb_build_object(
      'timesheet_id','c4000000-0000-4000-8000-000000000302',
      'entry_point','E1 timesheet_route_version_rotate',
      'actor_user_id','c4000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean and (v_result->>'recorded')::boolean is false
    and v_result->>'reason'='NOT_A_REFUSAL',
    'an unmanaged root records nothing: '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000302'
        and action='WEEKLY_SOURCE_ROTATION_REFUSED')=0,
    'an unmanaged root writes no refusal event');
  perform pg_temp.assert_refused(
    $sql$select public.weekly_source_audit_guard_refusal_record_v1(
      '{"timesheet_id":"c4000000-0000-4000-8000-000000000302","managed":true,
        "actor_user_id":"c4000000-0000-4000-8000-000000000001"}'::jsonb)$sql$,
    '%WEEKLY_SOURCE_AUDIT_GUARD_REFUSAL_REQUEST_INVALID%',
    'a caller-supplied refusal verdict');
end
$verify_guard_refusal$;

-- ===========================================================================
-- 7. Export differentials
-- ===========================================================================
do $verify_export$
declare
  v_export jsonb;
  v_rotated jsonb;
  v_ordinary jsonb;
begin
  -- Candidate 1: submitted evidence present, no source rows, no head, no
  -- settlement.  The four facts are separate and none is filled from another.
  v_export:=private.weekly_source_export_hours_v1(
    'c4000000-0000-4000-8000-000000000301');
  perform pg_temp.assert_true(
    (v_export->>'weekly_source')::boolean,
    'an authorised Weekly Source week must carry the export member: '||v_export::text);
  perform pg_temp.assert_eq(v_export#>>'{submitted_hours,state}','AVAILABLE',
    'the Candidate submission is available');
  perform pg_temp.assert_eq(
    (v_export#>>'{submitted_hours,total_hours}')::numeric::text,'7.5',
    'eight hours less a thirty-minute break is seven and a half submitted hours');
  perform pg_temp.assert_eq(v_export#>>'{source_hours,state}','NO_SOURCE',
    'with no source rows the source fact is empty, never the submission');
  perform pg_temp.assert_true(
    v_export#>>'{source_hours,total_hours}' is null,
    'an empty source fact carries no figure');
  perform pg_temp.assert_eq(v_export#>>'{approved_hours,state}','NO_APPROVED_ENTITLEMENT',
    'with no committed head there is no approved fact');
  perform pg_temp.assert_true(
    v_export#>>'{approved_hours,total_hours}' is null,
    'an absent approved fact carries no figure');
  perform pg_temp.assert_eq(v_export#>>'{paid_hours,state}','NO_SETTLEMENT',
    'with no settlement evidence the paid fact says so');
  perform pg_temp.assert_eq(v_export->>'paid_hours_authority','SETTLEMENT_ALLOCATION',
    'the paid fact names its authority');
  perform pg_temp.assert_eq(v_export#>>'{invoice_movements,movement_count}','0',
    'invoice movements are separate and empty');

  -- Candidate 3: a ROTATED family.  The physical Timesheet changed, and both
  -- the old and the new physical id must answer with the same family.
  v_rotated:=private.weekly_source_audit_family_v1(
    'c4000000-0000-4000-8000-000000000303');
  perform pg_temp.assert_true(
    (v_rotated->>'ok')::boolean
    and pg_catalog.jsonb_array_length(v_rotated->'member_timesheet_ids')=2,
    'the rotated family must resolve to both physical members: '||v_rotated::text);
  perform pg_temp.assert_eq(
    private.weekly_source_audit_family_v1('c4000000-0000-4000-8000-000000000393')
      ->>'canonical_timesheet_id',
    v_rotated->>'canonical_timesheet_id',
    'the demoted physical id resolves to the same canonical root');
  perform pg_temp.assert_eq(
    private.weekly_source_export_approved_hours_v1(
      'c4000000-0000-4000-8000-000000000393')->>'state',
    private.weekly_source_export_approved_hours_v1(
      'c4000000-0000-4000-8000-000000000303')->>'state',
    'the approved fact is a family fact, not a physical-row fact');

  -- An ordinary Timesheet: the additive export member is byte-identically
  -- empty, so an ordinary export row is unchanged.
  v_ordinary:=private.weekly_source_export_hours_v1(
    'c4000000-0000-4000-8000-000000000304');
  perform pg_temp.assert_eq(v_ordinary::text,'{}',
    'an ordinary Timesheet''s export member must be exactly {}');
  perform pg_temp.assert_eq(
    private.weekly_source_export_hours_v1(null)::text,'{}',
    'a null Timesheet id yields exactly {}');

  -- The existing export owner carries the additive member, through the
  -- late-bound accessor that makes a NEW build from empty possible.
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      to_regprocedure('public.tsfin_report_timesheets_v2(date,date,text,uuid[],uuid[],boolean,boolean,boolean)'))
      like '%tsfin_weekly_source_hours_v1%',
    'the existing export owner must emit the additive Weekly Source member');
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      to_regprocedure('public.tsfin_weekly_source_hours_v1(uuid)'))
      like '%weekly_source_export_hours_v1%',
    'the accessor must reach the Weekly Source export composer');
  perform pg_temp.assert_eq(
    public.tsfin_weekly_source_hours_v1('c4000000-0000-4000-8000-000000000304')::text,
    '{}','the accessor is exactly {} for an ordinary Timesheet');
  perform pg_temp.assert_eq(
    public.tsfin_weekly_source_hours_v1('c4000000-0000-4000-8000-000000000301')::text,
    private.weekly_source_export_hours_v1('c4000000-0000-4000-8000-000000000301')::text,
    'the accessor adds nothing of its own');

  -- The export owner itself really emits the member, driven end to end.
  perform pg_temp.assert_true(
    exists(
      select 1 from public.tsfin_report_timesheets_v2(
        '2026-09-01','2026-09-30',null,
        array['c4000000-0000-4000-8000-000000000002']::uuid[],null,true,null,null) as row_value
      where row_value->'weekly_source_hours' is not null),
    'the export owner must emit a weekly_source_hours member on every row');
  perform pg_temp.assert_true(
    exists(
      select 1 from public.tsfin_report_timesheets_v2(
        '2026-09-01','2026-09-30',null,
        array['c4000000-0000-4000-8000-000000000002']::uuid[],null,true,null,null) as row_value
      where row_value->>'timesheet_id'='c4000000-0000-4000-8000-000000000301'
        and (row_value#>'{weekly_source_hours,weekly_source}')::text='true'
        and row_value#>>'{weekly_source_hours,paid_hours_authority}'='SETTLEMENT_ALLOCATION'
        and row_value#>>'{weekly_source_hours,submitted_hours,state}'='AVAILABLE'),
    'the export row separates the four facts for a Weekly Source week');
  perform pg_temp.assert_true(
    exists(
      select 1 from public.tsfin_report_timesheets_v2(
        '2026-09-01','2026-09-30',null,
        array['c4000000-0000-4000-8000-000000000002']::uuid[],null,true,null,null) as row_value
      where row_value->>'timesheet_id'='c4000000-0000-4000-8000-000000000304'
        and (row_value->'weekly_source_hours')::text='{}'),
    'an ordinary Timesheet''s export row carries exactly {}');

  -- The export differential: with the one additive member removed, every row is
  -- exactly the fifteen members the export owner produced before Plan 6.2.  A
  -- renamed, dropped or altered pre-existing member would fail here.
  perform pg_temp.assert_true(
    not exists(
      select 1 from public.tsfin_report_timesheets_v2(
        '2026-09-01','2026-09-30',null,
        array['c4000000-0000-4000-8000-000000000002']::uuid[],null,true,null,null) as row_value
      where (
        select coalesce(pg_catalog.array_agg(key.value order by key.value),array[]::text[])
        from pg_catalog.jsonb_object_keys(row_value-'weekly_source_hours') as key(value)
      ) is distinct from array[
        'candidate_id','client','client_id','expenses_charge_ex_vat','invoiced_any',
        'locked_by_invoice_id','margin_ex_vat','mileage_charge_ex_vat','paid_at_utc',
        'pay_method','pay_on_hold','timesheet','timesheet_id','total_charge_ex_vat',
        'total_pay_ex_vat']::text[]),
    'every export row, less the additive member, keeps exactly its pre-Plan-6.2 members');
end
$verify_export$;

-- ===========================================================================
-- 8. Head publication, supersession and the pending-bundle states
--
-- Each is driven by putting the database into the state that genuinely produces
-- it through the relation the owner writes, and asserting the event that comes
-- out.  The immediate and deferred distinction is taken from the released
-- pending bundle for the same decision bundle, never from a flag.
-- ===========================================================================
do $verify_publication_events$
declare
  v_bundle uuid:='c4000000-0000-4000-8000-0000000000b1';
  v_head uuid:='c4000000-0000-4000-8000-0000000000c1';
  v_head2 uuid:='c4000000-0000-4000-8000-0000000000c2';
  v_pending uuid;
  v_root uuid:='c4000000-0000-4000-8000-000000000302';
  v_contract uuid:='c4000000-0000-4000-8000-000000000202';
  v_candidate uuid:='c4000000-0000-4000-8000-000000000102';
  v_actor uuid:='c4000000-0000-4000-8000-000000000001';
  v_events text[];
begin
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    v_bundle,1,'c4000000-0000-4000-8000-00000000af01'::uuid,v_candidate,'2026-09-13',
    'SINGLE_ROOT','WP14-BK-02',v_root,v_contract,v_bundle,v_actor,'IMMEDIATE',
    decode(repeat('11',32),'hex'),decode(repeat('12',32),'hex'),
    decode(repeat('13',32),'hex'),decode(repeat('14',32),'hex'),
    array[v_head]::uuid[],'PROPOSED');

  -- 8.1 A staged head, then an IMMEDIATE publication: there is no pending
  --     bundle for this decision, so the event must say IMMEDIATE.
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    state,certified_zero,component_count,entitlement_digest,inventory_digest,
    source_generation_digest,decision_bundle_id,bundle_revision,decision_id,
    decided_by_user_id
  ) values (
    v_head,'LOCKED_FINAL_SOURCE',pg_catalog.gen_random_uuid(),v_candidate,v_contract,
    '2026-09-13',v_root,'WP14-BK-02',1,1,'STAGED',false,1,
    decode(repeat('21',32),'hex'),decode(repeat('22',32),'hex'),
    decode(repeat('23',32),'hex'),v_bundle,1,v_bundle,v_actor);

  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_STAGED')=1,
    'a staged head produces its own event');

  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',
         committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('31',32),'hex'),
         scope_change_tx_token=pg_catalog.gen_random_uuid()
   where id=v_head;

  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY')=1,
    'an immediate publication produces the immediate event');
  perform pg_temp.assert_eq(
    (select after_json->>'publication_mode' from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY'),
    'IMMEDIATE','the immediate event records its mode');
  -- Plain English, and 24 section 18's "old and new source reference" in the
  -- same sentence.  WP-14b F10: an entitlement head is the APPROVED HOURS
  -- RECORD, not a source reference; narrating its id as "the source reference"
  -- named an object that exists in no source report.
  perform pg_temp.assert_true(
    (select after_json->>'narrative' from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY')
      ='The approved hours for this week were published straight away.'
       ||' The approved hours record is '||v_head::text||'.',
    'the immediate publication is explained in plain English and names the head '
    ||'as the approved hours record, got '||coalesce((select after_json->>'narrative'
       from public.audit_events where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY'),'<null>'));
  perform pg_temp.assert_true(
    (select after_json->>'narrative' from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY')
      not like '%source reference%',
    'WP-14b F10: a head id is never narrated as a source reference');
  perform pg_temp.assert_eq(
    (select private.weekly_source_audit_references_v1(before_json,after_json)
             ->>'new_approved_hours_record'
       from public.audit_events where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY'),
    v_head::text,
    'WP-14b F10: the head is carried as the approved hours record reference');
  perform pg_temp.assert_true(
    (select private.weekly_source_audit_references_v1(before_json,after_json)
             ->>'new_source_reference'
       from public.audit_events where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY') is null,
    'WP-14b F10: no source reference is invented for a head event');
  perform pg_temp.assert_true(
    (select after_json->>'publication_receipt_digest' is not null
       from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY'),
    '24 section 18: the publication receipt is retained on the event');

  -- The approved export fact now answers from the committed head.
  perform pg_temp.assert_eq(
    private.weekly_source_export_approved_hours_v1(v_root)->>'state','AVAILABLE',
    'a committed head makes the approved fact available');

  -- 8.2 Pending saved and frozen, from the relation the save owner writes.
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    'c4000000-0000-4000-8000-0000000000b2',1,'c4000000-0000-4000-8000-00000000af01'::uuid,
    v_candidate,'2026-09-13','SINGLE_ROOT','WP14-BK-02',v_root,v_contract,
    'c4000000-0000-4000-8000-0000000000b2',v_actor,'DEFERRED',
    decode(repeat('41',32),'hex'),decode(repeat('42',32),'hex'),
    decode(repeat('43',32),'hex'),decode(repeat('44',32),'hex'),
    array[v_head2]::uuid[],'PROPOSED');

  insert into public.weekly_source_pending_entitlement_bundles(
    decision_bundle_id,bundle_revision,candidate_id,member_root_ids,
    member_family_booking_ids,member_root_versions,request_digest,
    source_revision_digest,contract_choice_digest,decision_id,decided_by_user_id,
    proposed_head_ids,request_json,pending_revision,state,next_check_at_utc,
    last_census_json
  ) values (
    'c4000000-0000-4000-8000-0000000000b2',1,v_candidate,array[v_root]::uuid[],
    array['WP14-BK-02']::text[],array[1]::integer[],decode(repeat('51',32),'hex'),
    decode(repeat('42',32),'hex'),decode(repeat('43',32),'hex'),
    'c4000000-0000-4000-8000-0000000000b2',v_actor,array[v_head2]::uuid[],
    '{}'::jsonb,1,'PENDING',pg_catalog.transaction_timestamp(),
    -- WP-14b F8.  The REAL census and the REAL save owner store the verdict
    -- under `result`.  This fixture previously seeded `census_result`, a key
    -- that no owner writes, so the assertion below passed on a shape that does
    -- not occur and `census_result` was null on every event in production.
    -- The shape is proved against the installed save owner immediately below,
    -- by EXECUTING it (Part 1 rule 1), not by inspecting its text.
    pg_catalog.jsonb_build_object(
      'result','FROZEN','reason','WEEKLY_SOURCE_PAY_BATCH_FROZEN',
      'evaluated_at_utc',pg_catalog.to_char(
        pg_catalog.transaction_timestamp() at time zone 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SSOF')))
  returning id into v_pending;

  -- Executed proof that `result` is the key the installed save owner reads: the
  -- same value under the old key is REFUSED as not frozen, and under `result`
  -- it passes the census gate and is refused later for an unrelated reason.
  perform pg_temp.assert_eq(
    private.weekly_source_pending_entitlement_bundle_save_v1(
      '{}'::jsonb,'{"ok":true}'::jsonb,
      pg_catalog.jsonb_build_object('census_result','FROZEN'))->>'code',
    'WEEKLY_SOURCE_PENDING_BUNDLE_CENSUS_NOT_FROZEN',
    'WP-14b F8: the save owner does not read `census_result`, so a fixture '
      ||'seeded with that key proves nothing');
  perform pg_temp.assert_true(
    private.weekly_source_pending_entitlement_bundle_save_v1(
      '{}'::jsonb,'{"ok":true}'::jsonb,
      pg_catalog.jsonb_build_object('result','FROZEN'))->>'code'
      is distinct from 'WEEKLY_SOURCE_PENDING_BUNDLE_CENSUS_NOT_FROZEN',
    'WP-14b F8: `result` IS the key the save owner reads');

  select pg_catalog.array_agg(distinct action order by action) into v_events
  from public.audit_events
  where object_id_text=v_root::text
    and action in ('WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED',
                   'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN');
  perform pg_temp.assert_eq(
    pg_catalog.array_to_string(v_events,','),
    'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN,WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED',
    'a saved bundle produces both the pending-saved and the frozen event');
  perform pg_temp.assert_eq(
    (select after_json->>'census_result' from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN'),
    'FROZEN','the frozen event carries the census result that caused it');
  -- WP-14b F5: the save is the Office user's act and keeps its actor; the
  -- freeze is the census's verdict and carries none.
  perform pg_temp.assert_eq(
    (select actor_user_id::text from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED'),
    v_actor::text,'the save keeps the deciding Office user as its actor');
  perform pg_temp.assert_true(
    (select actor_user_id is null and actor_display is null from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN'),
    'WP-14b F5: the freeze is not attributed to a person');

  -- 8.3 The three real outcomes of `PENDING -> RELEASING -> PENDING`.
  --
  -- WP-14b F2.  The real release owner makes this SAME transition for a frozen
  -- payment, for a technical failure and for a serial-gate BUSY skip, and all
  -- three used to be audited as "payment frozen".  Ten of twenty-three frozen
  -- events on the reviewer's run of the REAL owners were technical failures.
  -- Each of the three is now driven and asserted separately.

  -- 8.3a The lease claim itself, which used to be a silent transition.
  update public.weekly_source_pending_entitlement_bundles
     set state='RELEASING',lease_owner='wp14',lease_token=pg_catalog.gen_random_uuid(),
         lease_worker_run_id=pg_catalog.gen_random_uuid(),
         lease_expires_at_utc=pg_catalog.transaction_timestamp()
   where id=v_pending;
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_STARTED')=1,
    'WP-14b: claiming the lease is recorded, so an attempt that never reports '
      ||'back is visible');
  perform pg_temp.assert_eq(
    (select after_json->>'lease_owner' from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_STARTED'),
    'wp14','the claim names the worker that took the lease');

  -- 8.3b Still frozen: the census is RE-EVALUATED and says FROZEN again.
  update public.weekly_source_pending_entitlement_bundles
     set state='PENDING',pending_revision=pending_revision+1,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         last_census_json=pg_catalog.jsonb_build_object(
           'result','FROZEN','reason','WEEKLY_SOURCE_PAY_BATCH_FROZEN',
           'evaluated_at_utc','2026-09-18T09:00:00+00:00'),
         next_check_at_utc=pg_catalog.transaction_timestamp()
   where id=v_pending;
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN')=2,
    'a second frozen release attempt is a second frozen event');

  -- 8.3c A serial-gate BUSY skip: the attempt never reached the census, so
  --      NEITHER the failure counter NOR the census moves.  This is the exact
  --      state change `…release_apply_v1` makes on `WEEKLY_SOURCE_CANDIDATE_BUSY`;
  --      that branch is only reachable inside the locked apply owner, so it is
  --      driven here as the state change and proved end to end through WP-08b's
  --      own verifier in the package report.
  update public.weekly_source_pending_entitlement_bundles
     set state='RELEASING',lease_owner='wp14',lease_token=pg_catalog.gen_random_uuid(),
         lease_worker_run_id=pg_catalog.gen_random_uuid(),
         lease_expires_at_utc=pg_catalog.transaction_timestamp()
   where id=v_pending;
  update public.weekly_source_pending_entitlement_bundles
     set state='PENDING',pending_revision=pending_revision+1,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         next_check_at_utc=pg_catalog.transaction_timestamp()
              +pg_catalog.make_interval(secs=>60)
   where id=v_pending;
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN')=2,
    'WP-14b F2: a busy skip is NOT audited as a frozen payment');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_DEFERRED')=1,
    'WP-14b F2: a busy skip gets its own event');
  perform pg_temp.assert_true(
    (select after_json->>'narrative' from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_DEFERRED')
      like '%another job for this candidate was already running%',
    'WP-14b F2: Office is told why no attempt was made');

  -- 8.3d A technical failure, driven through the REAL technical-failure owner
  --      (Part 1 rule 1: execute the path).  The owner claims the lease itself,
  --      so the bundle is put back into RELEASING first, exactly as the worker
  --      does.
  update public.weekly_source_pending_entitlement_bundles
     set state='RELEASING',lease_owner='wp14',lease_token=pg_catalog.gen_random_uuid(),
         lease_worker_run_id=pg_catalog.gen_random_uuid(),
         lease_expires_at_utc=pg_catalog.transaction_timestamp()
   where id=v_pending;
  perform private.weekly_source_pending_release_technical_failure_v1(
    v_pending,'WEEKLY_SOURCE_CENSUS_ERROR',
    pg_catalog.jsonb_build_object('census_result','CENSUS_ERROR'));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN')=2,
    'WP-14b F2: a technical failure is NOT audited as a frozen payment');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_FAILED')=1,
    'WP-14b F2: a technical failure gets its own event, from the REAL owner');
  perform pg_temp.assert_true(
    (select after_json->>'narrative' from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_FAILED')
      like '%technical failure%attempt 1 of 10%',
    'WP-14b F2: the sentence says which attempt failed, got '
      ||coalesce((select after_json->>'narrative' from public.audit_events
                   where object_id_text=v_root::text
                     and action='WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_FAILED'),'<null>'));
  perform pg_temp.assert_true(
    (select actor_user_id is null from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_FAILED'),
    'WP-14b F5: a worker failure is not attributed to the Office user');

  -- 8.4 MANUAL_REVIEW.
  update public.weekly_source_pending_entitlement_bundles
     set state='MANUAL_REVIEW',
         manual_review_reason='Ten consecutive technical failures',
         next_check_at_utc=null
   where id=v_pending;
  perform pg_temp.assert_true(
    (select after_json->>'narrative' from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_MANUAL_REVIEW')
      like 'The held decision needs Office review%Ten consecutive technical failures%',
    'the manual-review event names its reason in plain English');

  -- WP-14b F10.  The three shapes a stored reason really takes, through the
  -- reader Office's sentence is built from.  The structured shape is what
  -- WP-08b's own reason builder produces, and its `message` used to be thrown
  -- away with the machine detail, leaving Office a sentence with no reason.
  perform pg_temp.assert_eq(
    private.weekly_source_audit_human_reason_v1(
      '{"message":"WEEKLY_SOURCE_CENSUS_ERROR after 10 consecutive technical '
      ||'failures.","code":"WEEKLY_SOURCE_CENSUS_ERROR","items":[1,2]}'),
    'WEEKLY_SOURCE_CENSUS_ERROR after 10 consecutive technical failures.',
    'WP-14b F10: a structured reason yields its operator sentence, not nothing');
  perform pg_temp.assert_eq(
    private.weekly_source_audit_human_reason_v1(
      'WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION: '
      ||'ROOT_ROTATED_AFTER_AUTHORISATION [{"a":1}]'),
    'WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION: ROOT_ROTATED_AFTER_AUTHORISATION',
    'WP-14b F10: a reason whose detail begins with "[" loses the stray fragment');
  perform pg_temp.assert_eq(
    private.weekly_source_audit_human_reason_v1(
      '{"message":"WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION: '
      ||'ROOT_ROTATED_AFTER_AUTHORISATION [{\"a\":1}","code":"X"}'),
    'WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION: ROOT_ROTATED_AFTER_AUTHORISATION',
    'WP-14b F10: the stray fragment is stripped from the message INSIDE a '
      ||'structured reason too, not only from a bare one');
  perform pg_temp.assert_true(
    private.weekly_source_audit_human_reason_v1('{not json at all') is null,
    'WP-14b F10: an unparseable structured reason yields nothing rather than '
      ||'raw JSON, and never raises');
  perform pg_temp.assert_true(
    private.weekly_source_audit_human_reason_v1('') is null,
    'WP-14b F10: an empty reason yields nothing');
  perform pg_temp.assert_true(
    (select actor_user_id is null from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_MANUAL_REVIEW'),
    'WP-14b F5: the move to manual review is not attributed to the Office user');

  -- 8.5 The audited Office reopen is WP-08b's own event and is not duplicated.
  perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'Office reviewed the frozen evidence and asked for another attempt',
    v_actor);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_pending::text
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED')=1,
    'the reopen keeps exactly one event, written by its own owner');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_pending::text)=1,
    'G5-6 stays true: the pending-bundle id carries only the reopen event');

  -- 8.6 SUPERSEDED.
  update public.weekly_source_pending_entitlement_bundles
     set state='SUPERSEDED',next_check_at_utc=null
   where id=v_pending;
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_SUPERSEDED')=1,
    'a superseded bundle produces its own event');

  -- 8.7 RELEASED, and then a head committed under the SAME decision bundle,
  --     which must now be recorded as a DEFERRED publication.
  update public.weekly_source_pending_entitlement_bundles
     set state='RELEASED',
         released_at_utc=pg_catalog.transaction_timestamp(),
         released_receipt_id=pg_catalog.gen_random_uuid(),
         released_receipt_digest=decode(repeat('61',32),'hex'),
         released_by_worker_id='wp14-worker',
         released_by_worker_run_id=pg_catalog.gen_random_uuid(),
         next_check_at_utc=null
   where id=v_pending;
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED')=1,
    'a released bundle produces its own event');

  -- The previous head is superseded by the new one, in the order the
  -- coordinator does it.
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id
  ) values (
    v_head2,'LOCKED_FINAL_SOURCE',pg_catalog.gen_random_uuid(),v_candidate,v_contract,
    '2026-09-13',v_root,'WP14-BK-02',1,2,v_head,'STAGED',false,1,
    decode(repeat('71',32),'hex'),decode(repeat('72',32),'hex'),
    decode(repeat('73',32),'hex'),'c4000000-0000-4000-8000-0000000000b2',1,
    'c4000000-0000-4000-8000-0000000000b2',v_actor);

  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',
         superseded_at_utc=pg_catalog.transaction_timestamp(),
         superseded_by_head_id=v_head2
   where id=v_head;
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_SUPERSEDED')=1,
    'a superseded head produces its own event');

  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',
         committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('81',32),'hex'),
         scope_change_tx_token=pg_catalog.gen_random_uuid()
   where id=v_head2;
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_AFTER_DEFERRAL')=1,
    'a head published under a RELEASED bundle is recorded as DEFERRED');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where object_id_text=v_root::text
        and action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY')=1,
    'and the earlier immediate publication is not re-labelled');
end
$verify_publication_events$;

-- ===========================================================================
-- 9. The complete chronology, in plain English and in order
-- ===========================================================================
do $verify_chronology$
declare
  v_chronology jsonb;
  v_actions text[];
begin
  v_chronology:=private.weekly_source_audit_chronology_v1(
    'c4000000-0000-4000-8000-000000000302');
  select pg_catalog.array_agg(distinct event.value->>'event' order by event.value->>'event')
    into v_actions
  from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value);

  perform pg_temp.assert_true(
    v_actions @> array[
      'WEEKLY_SOURCE_ENTITLEMENT_STAGED',
      'WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY',
      'WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_AFTER_DEFERRAL',
      'WEEKLY_SOURCE_ENTITLEMENT_SUPERSEDED',
      'WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED',
      'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN',
      'WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED',
      'WEEKLY_SOURCE_PENDING_ENTITLEMENT_SUPERSEDED',
      'WEEKLY_SOURCE_PENDING_ENTITLEMENT_MANUAL_REVIEW']::text[],
    'the chronology must carry every Gate 11 lifecycle event: '
      ||pg_catalog.array_to_string(v_actions,','));

  -- Chronological, and every row explained without raw JSON.
  perform pg_temp.assert_true(
    not exists(
      select 1 from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      where nullif(pg_catalog.btrim(coalesce(event.value->>'narrative','')),'') is null
         or event.value->>'at_utc' is null),
    'every chronology row carries a sentence and a time');

  -- WP-14b F3.  The Office reopen is written by WP-08b against the PENDING
  -- BUNDLE, not against the Timesheet, so a reader keyed to `timesheets` alone
  -- could never show it.  Section 8.5 drove the REAL reopen owner; this asserts
  -- that the event now reaches the Timesheet chronology, exactly once, without
  -- a second audit row having been written (`G5-6`).
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      where event.value->>'event'
              ='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED')=1,
    'WP-14b F3: the Office reopen appears in the Timesheet chronology exactly once');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.audit_events
      where action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED')=1,
    'WP-14b F3: and it is still ONE audit row - the reader unions, it never writes');
  perform pg_temp.assert_eq(
    (select event.value->>'audited_object_type'
       from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      where event.value->>'event'
              ='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED'),
    'weekly_source_pending_entitlement_bundles',
    'WP-14b F3: the chronology says which object the row was keyed to');

  -- WP-14b F3.  An Office decision, written exactly as WP-06's owner writes it
  -- (`object_type='weekly_source_entitlement_decision_bundles'`).
  perform public._audit_insert(
    'weekly_source_entitlement_decision_bundles',
    'c4000000-0000-4000-8000-0000000000b2',
    'WEEKLY_SOURCE_LATER_CHANGE_DECIDED',null,
    pg_catalog.jsonb_build_object(
      'decision','KEEP_CURRENTLY_APPROVED_HOURS','outcome','RETAINED',
      'bundle_revision',1,
      'root_timesheet_id','c4000000-0000-4000-8000-000000000302'),
    'WEEKLY_SOURCE_KEEP_CURRENTLY_APPROVED_HOURS',
    'c4000000-0000-4000-8000-000000000001');
  v_chronology:=private.weekly_source_audit_chronology_v1(
    'c4000000-0000-4000-8000-000000000302');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      where event.value->>'event'='WEEKLY_SOURCE_LATER_CHANGE_DECIDED')=1,
    'WP-14b F3: the Office decision appears in the Timesheet chronology exactly once');
  perform pg_temp.assert_true(
    (select event.value->>'narrative'
       from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      where event.value->>'event'='WEEKLY_SOURCE_LATER_CHANGE_DECIDED')
      like 'Office made a decision about a later change to this week.%'
        ||'The Office decision was KEEP_CURRENTLY_APPROVED_HOURS.%',
    'WP-14b F3: and Office is told WHICH decision was taken, got '
      ||coalesce((select event.value->>'narrative'
                    from pg_catalog.jsonb_array_elements(v_chronology->'events')
                      as event(value)
                   where event.value->>'event'='WEEKLY_SOURCE_LATER_CHANGE_DECIDED'),
                 '<null>'));

  -- STEP 6 / hostile-review F-02.  Every event of one owner transaction shares
  -- one `ts_utc`, so neither the timestamp nor a random UUID can prove order.
  -- The database-owned event sequence must now be present, authoritative and
  -- strictly increasing in the exact order returned by the chronology.  The
  -- lifecycle assertions below then prove that the recorded order is also the
  -- order in which the real owners wrote the facts; no lifecycle rank is used
  -- to manufacture a plausible story.
  perform pg_temp.assert_true(
    (select pg_catalog.count(distinct ts_utc) from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000302'
        and action like 'WEEKLY!_SOURCE!_%' escape '!')=1,
    'the whole fixture really is one transaction timestamp, so this IS the '
      ||'case that used to sort at random');
  perform pg_temp.assert_true(
    v_chronology->>'event_order_authority'='AUDIT_EVENT_SEQUENCE'
      and (v_chronology->>'event_order_fully_authoritative')::boolean,
    'the chronology did not declare the durable audit-event sequence as its '
      ||'fully authoritative order');
  perform pg_temp.assert_true(
    not exists(
      select 1
      from (
        select (event.value->>'event_sequence')::bigint as event_sequence,
               coalesce((event.value->>'order_is_authoritative')::boolean,false)
                 as order_is_authoritative,
               pg_catalog.lag((event.value->>'event_sequence')::bigint)
                 over (order by event.ordinality) as prior_sequence
        from pg_catalog.jsonb_array_elements(v_chronology->'events')
          with ordinality as event(value,ordinality)
      ) as ordered
      where not ordered.order_is_authoritative
         or ordered.event_sequence is null
         or (ordered.prior_sequence is not null
             and ordered.event_sequence<=ordered.prior_sequence)),
    'the chronology contains a missing, backfilled or non-increasing event sequence');
  perform pg_temp.assert_true(
    not exists(
      select 1
      from pg_catalog.jsonb_array_elements(v_chronology->'events') as event(value)
      join public.audit_events as audit_row
        on audit_row.id=(event.value->>'audit_event_id')::uuid
      where audit_row.event_sequence<>(event.value->>'event_sequence')::bigint
         or audit_row.event_sequence_is_authoritative
              is distinct from (event.value->>'order_is_authoritative')::boolean),
    'the chronology did not preserve the database audit-event order exactly');
  perform pg_temp.assert_true(
    (select pg_catalog.min(ordinality)
       from pg_catalog.jsonb_array_elements(v_chronology->'events')
              with ordinality as event(value,ordinality)
      where event.value->>'event'='WEEKLY_SOURCE_ENTITLEMENT_STAGED')
    <(select pg_catalog.min(ordinality)
        from pg_catalog.jsonb_array_elements(v_chronology->'events')
               with ordinality as event(value,ordinality)
       where event.value->>'event' like 'WEEKLY!_SOURCE!_ENTITLEMENT!_PUBLISHED%' escape '!'),
    'WP-14b F4: staging renders before publication');
  perform pg_temp.assert_true(
    (select pg_catalog.max(ordinality)
       from pg_catalog.jsonb_array_elements(v_chronology->'events')
              with ordinality as event(value,ordinality)
      where event.value->>'event'='WEEKLY_SOURCE_ENTITLEMENT_SUPERSEDED')
    <(select pg_catalog.max(ordinality)
        from pg_catalog.jsonb_array_elements(v_chronology->'events')
               with ordinality as event(value,ordinality)
       where event.value->>'event' like 'WEEKLY!_SOURCE!_ENTITLEMENT!_PUBLISHED%' escape '!'),
    'STEP 6 F-02: the prior head supersession renders before the replacement '
      ||'head publication, exactly as the publication coordinator writes them');
  perform pg_temp.assert_true(
    (select pg_catalog.min(ordinality)
       from pg_catalog.jsonb_array_elements(v_chronology->'events')
              with ordinality as event(value,ordinality)
      where event.value->>'event'='WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED')
    <(select pg_catalog.min(ordinality)
        from pg_catalog.jsonb_array_elements(v_chronology->'events')
               with ordinality as event(value,ordinality)
       where event.value->>'event'='WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED'),
    'WP-14b F4: the save renders before the release of what was saved');
  -- Deterministic: the same call twice gives byte-identical order.
  perform pg_temp.assert_eq(
    (select pg_catalog.string_agg(event.value->>'audit_event_id',',' order by ordinality)
       from pg_catalog.jsonb_array_elements(
              private.weekly_source_audit_chronology_v1(
                'c4000000-0000-4000-8000-000000000302')->'events')
              with ordinality as event(value,ordinality)),
    (select pg_catalog.string_agg(event.value->>'audit_event_id',',' order by ordinality)
       from pg_catalog.jsonb_array_elements(
              private.weekly_source_audit_chronology_v1(
                'c4000000-0000-4000-8000-000000000302')->'events')
              with ordinality as event(value,ordinality)),
    'WP-14b F4: the rendered order is deterministic');

  -- WP-14b F5.  The release was performed by `wp14-worker`; the audit must not
  -- put an Office administrator's name on it.
  perform pg_temp.assert_true(
    (select actor_user_id is null and actor_display is null
       from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000302'
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED'),
    'WP-14b F5: a worker release names no Office user');
  perform pg_temp.assert_eq(
    (select after_json->>'released_by_worker_id' from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000302'
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED'),
    'wp14-worker','WP-14b F5: it names the worker that really released it');
  perform pg_temp.assert_eq(
    (select after_json->>'decided_by_user_id' from public.audit_events
      where object_id_text='c4000000-0000-4000-8000-000000000302'
        and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED'),
    'c4000000-0000-4000-8000-000000000001',
    'WP-14b F5: and the decision''s owner is still on the row');
end
$verify_chronology$;

-- ===========================================================================
-- 9A. A REAL source-authority Weekly Source week.
--
-- Candidate 5 gets the full source-authority world: a Weekly Source group and
-- client policy in SOURCE_AUTHORITY / CHECK_ONLY, a cycle, an accepted upload,
-- a CURRENT projection publication, two resolved source rows and the lineage
-- that binds them to the root Timesheet.  This is what makes the source and
-- approved export facts real, and what lets the Candidate hours-only push carry
-- genuine approved hours.
-- ===========================================================================
insert into public.candidates(id,display_name)
values ('e1000000-0000-4000-8000-000000000105','WP14 Candidate 5');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'e1000000-0000-4000-8000-000000000205','e1000000-0000-4000-8000-000000000105',
  'c4000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}'::jsonb,
  'HEALTHROSTER',true,true,true,true);

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time
) values (
  'e1000000-0000-4000-8000-000000000501','TEST',
  'c4000000-0000-4000-8000-00000000af01','WP14_GATE11','WP14 Gate 11 Roster','ROSTER',
  3,'15:00');
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values (
  'e1000000-0000-4000-8000-000000000501','c4000000-0000-4000-8000-000000000002',
  '2026-01-01','c4000000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  'e1000000-0000-4000-8000-000000000501','c4000000-0000-4000-8000-000000000002',
  '2026-01-01','SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,
  'manager@example.invalid','c4000000-0000-4000-8000-000000000001');

insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'e1000000-0000-4000-8000-000000000601','e1000000-0000-4000-8000-000000000501',
  '2026-09-13','2026-09-16 15:00:00+00','OPEN',1,'NONE');
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,
  physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'e1000000-0000-4000-8000-000000000701','e1000000-0000-4000-8000-000000000601',
  'wp14-gate11.xlsx',decode(repeat('71',32),'hex'),100,
  '34444444-4444-4444-8444-444444444444','verify','verify',
  decode(repeat('72',32),'hex'),decode(repeat('73',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',2,2,decode(repeat('74',32),'hex'),
  'CURRENT','c4000000-0000-4000-8000-000000000001');
update public.weekly_source_cycles
set current_complete_upload_id='e1000000-0000-4000-8000-000000000701'
where id='e1000000-0000-4000-8000-000000000601';
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  'e1000000-0000-4000-8000-000000000801','e1000000-0000-4000-8000-000000000601',
  'CYCLE','e1000000-0000-4000-8000-000000000701',1,
  decode(repeat('75',32),'hex'),decode(repeat('76',32),'hex'),'CURRENT',
  '2026-09-08 08:00:00+00');
update public.weekly_source_cycles
set projection_state='CURRENT',
    current_projection_publication_id='e1000000-0000-4000-8000-000000000801'
where id='e1000000-0000-4000-8000-000000000601';

insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
) values
  ('e1000000-0000-4000-8000-000000000901','e1000000-0000-4000-8000-000000000105',
   'c4000000-0000-4000-8000-000000000002','2026-09-07','PROFILE_EXTERNAL_KEY',
   'wp14-shift-1',decode(repeat('81',32),'hex'),
   'e1000000-0000-4000-8000-000000000501','34444444-4444-4444-8444-444444444444'),
  ('e1000000-0000-4000-8000-000000000902','e1000000-0000-4000-8000-000000000105',
   'c4000000-0000-4000-8000-000000000002','2026-09-08','PROFILE_EXTERNAL_KEY',
   'wp14-shift-2',decode(repeat('82',32),'hex'),
   'e1000000-0000-4000-8000-000000000501','34444444-4444-4444-8444-444444444444');

insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,normalised_row_hash
) values
  ('e1000000-0000-4000-8000-000000000a01','e1000000-0000-4000-8000-000000000701',
   1,'wp14-shift-1','JO NURSE','WP14 CLIENT','2026-09-07',
   '2026-09-07 09:00:00','2026-09-07 19:00:00',30,570,'SOURCE_WORKED',
   decode(repeat('91',32),'hex')),
  ('e1000000-0000-4000-8000-000000000a02','e1000000-0000-4000-8000-000000000701',
   2,'wp14-shift-2','JO NURSE','WP14 CLIENT','2026-09-08',
   '2026-09-08 09:00:00','2026-09-08 17:00:00',60,420,'SOURCE_WORKED',
   decode(repeat('92',32),'hex'));

insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,candidate_id,client_id,contract_id,
  work_event_id,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint
) values
  ('e1000000-0000-4000-8000-000000000b01','e1000000-0000-4000-8000-000000000a01',
   1,'RESOLVED','e1000000-0000-4000-8000-000000000105',
   'c4000000-0000-4000-8000-000000000002','e1000000-0000-4000-8000-000000000205',
   'e1000000-0000-4000-8000-000000000901','AUTO_UNIQUE','NEW_PROFILE_KEY',
   decode(repeat('b1',32),'hex'),decode(repeat('a1',32),'hex'),
   decode(repeat('a2',32),'hex'),decode(repeat('a3',32),'hex')),
  ('e1000000-0000-4000-8000-000000000b02','e1000000-0000-4000-8000-000000000a02',
   1,'RESOLVED','e1000000-0000-4000-8000-000000000105',
   'c4000000-0000-4000-8000-000000000002','e1000000-0000-4000-8000-000000000205',
   'e1000000-0000-4000-8000-000000000902','AUTO_UNIQUE','NEW_PROFILE_KEY',
   decode(repeat('b2',32),'hex'),decode(repeat('a4',32),'hex'),
   decode(repeat('a5',32),'hex'),decode(repeat('a6',32),'hex'));

select pg_temp.seed_timesheet(
  'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,true,
  'e1000000-0000-4000-8000-000000000205',
  '[{"row_key":"row-1","date":"2026-09-07","start":"09:00","end":"19:00","break_minutes":30,"worked_start_iso":"2026-09-07T09:00:00Z","worked_end_iso":"2026-09-07T19:00:00Z","break_minutes":30}]'::jsonb);
select pg_temp.seed_week_and_financials(
  'e1000000-0000-4000-8000-000000000405','e1000000-0000-4000-8000-000000000205',
  'e1000000-0000-4000-8000-000000000305','e1000000-0000-4000-8000-000000000505',
  'e1000000-0000-4000-8000-000000000105','c4000000-0000-4000-8000-000000000002',1);
update public.timesheets
   set r2_nurse_key='test-only/nurse-signature-5.png',
       img_sha256_nurse=repeat('c',64)
 where timesheet_id='e1000000-0000-4000-8000-000000000305';

insert into public.weekly_source_row_timesheet_lineages(
  id,row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,
  contract_id,contract_week_id,timesheet_id,family_booking_id,timesheet_version,
  week_ending_date,lineage_fingerprint
) values
  ('e1000000-0000-4000-8000-000000000e01','e1000000-0000-4000-8000-000000000b01',
   'e1000000-0000-4000-8000-000000000601','e1000000-0000-4000-8000-000000000901',
   'e1000000-0000-4000-8000-000000000105','c4000000-0000-4000-8000-000000000002',
   'e1000000-0000-4000-8000-000000000205','e1000000-0000-4000-8000-000000000405',
   'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,'2026-09-13',
   decode(repeat('c1',32),'hex')),
  ('e1000000-0000-4000-8000-000000000e02','e1000000-0000-4000-8000-000000000b02',
   'e1000000-0000-4000-8000-000000000601','e1000000-0000-4000-8000-000000000902',
   'e1000000-0000-4000-8000-000000000105','c4000000-0000-4000-8000-000000000002',
   'e1000000-0000-4000-8000-000000000205','e1000000-0000-4000-8000-000000000405',
   'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,'2026-09-13',
   decode(repeat('c2',32),'hex'));

select pg_temp.drain_workbench_jobs();

do $verify_source_authority_export$
declare
  v_export jsonb;
  v_result jsonb;
begin
  -- Source hours are now a real, separate fact.
  v_export:=private.weekly_source_export_hours_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_eq(v_export#>>'{source_hours,state}','AVAILABLE',
    'a real source-authority week has a source fact: '||v_export::text);
  perform pg_temp.assert_eq(v_export#>>'{source_hours,row_count}','2',
    'both source rows are counted');
  perform pg_temp.assert_eq(
    (v_export#>>'{source_hours,total_hours}')::numeric::text,'16.5',
    'nine and a half plus seven source hours');
  -- And it is NOT the same number as the submission, which proves the two
  -- facts are genuinely separate rather than one value shown twice.
  perform pg_temp.assert_eq(
    (v_export#>>'{submitted_hours,total_hours}')::numeric::text,'9.5',
    'the Candidate submitted one shift only');
  perform pg_temp.assert_true(
    (v_export#>>'{source_hours,total_hours}')::numeric
      is distinct from (v_export#>>'{submitted_hours,total_hours}')::numeric,
    'the source and the submission are different facts with different figures');

  -- Authorise through the REAL owner, so the week becomes authorised for pay
  -- and the approved statement exists.
  v_result:=public.weekly_source_first_authorise_v1(
    'e1000000-0000-4000-8000-000000000305','e1000000-0000-4000-8000-000000000305',
    null,'c4000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'the source-authority week must authorise: '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_source_authority_export$;

-- ===========================================================================
-- 10. The Candidate hours-only push, through the existing boundary
--
-- No message leaves the database.  The boundary writes a
-- `public.candidate_notifications` row with `push_state='PENDING'`; the delivery
-- worker that would claim it is never called, and the whole transaction is
-- rolled back.
-- ===========================================================================
do $verify_push$
declare
  v_account uuid:='c4000000-0000-4000-8000-0000000000a1';
  v_membership uuid:='c4000000-0000-4000-8000-0000000000a2';
  v_result jsonb;
  v_row public.candidate_notifications%rowtype;
  v_serialised jsonb;
  v_verdict jsonb;
  v_count integer;
begin
  -- The push refuses cleanly for a week that is not a Weekly Source week.
  v_result:=private.weekly_source_candidate_hours_push_v1(
    'c4000000-0000-4000-8000-000000000304');
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean and (v_result->>'pushed')::boolean is false,
    'an ordinary week pushes nothing: '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.candidate_notifications
      where event_type='TIMESHEET_HOURS_UPDATED')=0,
    'and writes no notification');

  -- The complete serialised payload the boundary would receive, scanned as one
  -- value rather than field by field.
  v_serialised:=pg_catalog.jsonb_build_object(
    'event_type','TIMESHEET_HOURS_UPDATED',
    'preference_category','timesheet_expense_attention',
    'template_key','approved-hours-updated-v1',
    'template_params',pg_catalog.jsonb_build_object(
      'week_ending_date','2026-09-13',
      'approved_hours',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('row_key','approved-1','worked',true,
          'date','2026-09-07','start','08:00','end','16:00'))),
    'deep_link',pg_catalog.jsonb_build_object(
      'destination','TIMESHEET_DETAIL',
      'timesheet_id','c4000000-0000-4000-8000-000000000301'),
    'dedupe_key','approved-hours-push:c4000000-0000-4000-8000-000000000301:'
      ||repeat('ab',32));
  v_verdict:=private.weekly_source_candidate_payload_safe_v1(v_serialised);
  perform pg_temp.assert_true((v_verdict->>'ok')::boolean,
    'the real push payload shape must carry no forbidden field or word: '
      ||v_verdict::text);

  -- WP-14b F1, and Part 1 rule 6 (adopt another package's corrected truth).
  -- This block used to push with NO committed entitlement head, taking the
  -- hours from the source producer.  That is the defect the independent review
  -- proved: a source-authority week with no committed head has no APPROVED
  -- entitlement, and telling the Candidate the source under the label
  -- `Approved hours to be paid` is a false statement.  The week is therefore
  -- given the committed head it would really have, matching the two source
  -- shifts (9.5 + 7 hours), and the push is proved against THAT.
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_hours_push_v1(
      'e1000000-0000-4000-8000-000000000305')->>'reason',
    'NO_APPROVED_ENTITLEMENT',
    'WP-14b F1: with no committed head the Candidate is told NOTHING, where '
      ||'the source-derived payload used to tell them the source hours');

  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    'e1000000-0000-4000-8000-0000000000b4',1,'c4000000-0000-4000-8000-00000000af01',
    'e1000000-0000-4000-8000-000000000105','2026-09-13','SINGLE_ROOT','WP14-BK-05',
    'e1000000-0000-4000-8000-000000000305','e1000000-0000-4000-8000-000000000205',
    'e1000000-0000-4000-8000-0000000000b4','c4000000-0000-4000-8000-000000000001',
    'IMMEDIATE',decode(repeat('d1',32),'hex'),decode(repeat('d2',32),'hex'),
    decode(repeat('d3',32),'hex'),decode(repeat('d4',32),'hex'),
    array['e1000000-0000-4000-8000-0000000000c4']::uuid[],'PROPOSED');
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    state,certified_zero,component_count,entitlement_digest,inventory_digest,
    source_generation_digest,decision_bundle_id,bundle_revision,decision_id,
    decided_by_user_id
  ) values (
    'e1000000-0000-4000-8000-0000000000c4','LOCKED_FINAL_SOURCE',
    'c4000000-0000-4000-8000-00000000af01','e1000000-0000-4000-8000-000000000105',
    'e1000000-0000-4000-8000-000000000205','2026-09-13',
    'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,1,'STAGED',false,2,
    decode(repeat('d5',32),'hex'),decode(repeat('d6',32),'hex'),
    decode(repeat('d7',32),'hex'),'e1000000-0000-4000-8000-0000000000b4',1,
    'e1000000-0000-4000-8000-0000000000b4','c4000000-0000-4000-8000-000000000001');
  -- `component_member_identity` is the DURABLE WORK EVENT the component covers,
  -- which is how the entitlement reader recovers the times to present and then
  -- reconciles them against the hours the head decided.
  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,
    economic_key_value,component_member_identity,segment_id,work_date,
    hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_ex_vat,
    exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
  ) values
    ('e1000000-0000-4000-8000-0000000000c4',1,
     'e1000000-0000-4000-8000-0000000000d1','WORKED_TIME','WORK_EVENT',
     'e1000000-0000-4000-8000-000000000901',
     'e1000000-0000-4000-8000-000000000901','seg-1','2026-09-07',
     9.5,0,0,0,0,0,false,'LOCKED_FINAL_SOURCE',
     'e1000000-0000-4000-8000-0000000000b4',1,decode(repeat('d8',32),'hex')),
    ('e1000000-0000-4000-8000-0000000000c4',2,
     'e1000000-0000-4000-8000-0000000000d2','WORKED_TIME','WORK_EVENT',
     'e1000000-0000-4000-8000-000000000902',
     'e1000000-0000-4000-8000-000000000902','seg-2','2026-09-08',
     7,0,0,0,0,0,false,'LOCKED_FINAL_SOURCE',
     'e1000000-0000-4000-8000-0000000000b4',1,decode(repeat('d9',32),'hex'));
  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('da',32),'hex'),
         scope_change_tx_token=pg_catalog.gen_random_uuid()
   where id='e1000000-0000-4000-8000-0000000000c4';

  -- Without an active Candidate App account nothing is pushed and nothing is
  -- invented.
  v_result:=private.weekly_source_candidate_hours_push_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean and (v_result->>'pushed')::boolean is false
    and v_result->>'reason'='NO_SINGLE_ACTIVE_ACCOUNT',
    'with no Candidate App account nothing is pushed: '||v_result::text);

  -- Now drive the real boundary.  One active Candidate App account is required.
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values (v_account,'TEST','wp14-candidate@example.test','ACTIVE');
  insert into public.candidate_app_global_membership_links(
    membership_id,global_account_identity_hmac,account_id,candidate_id,
    membership_generation,state
  ) values (
    v_membership,decode(repeat('91',32),'hex'),v_account,
    'e1000000-0000-4000-8000-000000000105',1,'ACTIVE');

  v_result:=private.weekly_source_candidate_hours_push_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_true((v_result->>'ok')::boolean,
    'the push must answer: '||v_result::text);
  perform pg_temp.assert_true((v_result->>'pushed')::boolean,
    'a real source-authority week with approved hours must push: '||v_result::text);

  select * into v_row from public.candidate_notifications
   where id=(v_result->>'notification_id')::uuid;
  -- Every column the Candidate can ever see, serialised as ONE value and
  -- scanned whole rather than field by field.
  v_serialised:=pg_catalog.jsonb_build_object(
    'event_type',v_row.event_type,
    'preference_category',v_row.preference_category,
    'template_key',v_row.template_key,
    'template_params',v_row.template_params,
    'deep_link',v_row.deep_link_json,
    'dedupe_key',v_row.dedupe_key);
  v_verdict:=private.weekly_source_candidate_payload_safe_v1(v_serialised);
  perform pg_temp.assert_true((v_verdict->>'ok')::boolean,
    'the stored Candidate payload must carry no forbidden field or word: '
      ||v_verdict::text);
  -- Belt and braces: the raw serialised text, lower-cased, holds none of the
  -- four words anywhere at all.
  perform pg_temp.assert_true(
    pg_catalog.strpos(pg_catalog.lower(v_serialised::text),'source')=0
    and pg_catalog.strpos(pg_catalog.lower(v_serialised::text),'protected')=0
    and pg_catalog.strpos(pg_catalog.lower(v_serialised::text),'exceptional')=0
    and pg_catalog.strpos(pg_catalog.lower(v_serialised::text),'reconciliation')=0,
    'the raw serialised payload text carries none of the four words: '
      ||v_serialised::text);
  perform pg_temp.assert_true(v_row.push_state='PENDING',
    'nothing is delivered by this verifier: the push stays PENDING');
  perform pg_temp.assert_true(
    v_row.timesheet_id='e1000000-0000-4000-8000-000000000305',
    'the push is keyed to the Timesheet');
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(v_row.template_params->'approved_hours')>0,
    'the push carries the approved hours themselves');

  -- Idempotency: the same approved hours never push twice.
  select pg_catalog.count(*)::integer into v_count
  from public.candidate_notifications where event_type='TIMESHEET_HOURS_UPDATED';
  perform private.weekly_source_candidate_hours_push_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)::integer from public.candidate_notifications
      where event_type='TIMESHEET_HOURS_UPDATED')=v_count,
    'the same approved hours must not push a second notification');

  -- A forbidden payload fails closed: the scanner refuses and nothing is
  -- written through the boundary.
  perform pg_temp.assert_true(
    (private.weekly_source_candidate_payload_safe_v1(
      v_serialised||pg_catalog.jsonb_build_object('total_pay_ex_vat',1))
      ->>'ok')::boolean is false,
    'the same payload with one money field must fail closed');

  -- The request contract.
  perform pg_temp.assert_refused(
    $sql$select public.weekly_source_candidate_hours_push_v1(
      '{"timesheet_id":"e1000000-0000-4000-8000-000000000305","money":1}'::jsonb)$sql$,
    '%WEEKLY_SOURCE_CANDIDATE_PUSH_REQUEST_INVALID%',
    'an unknown push request key');
end
$verify_push$;

-- ===========================================================================
-- 10A. Paid hours against fixture settlement states, on a ROTATED family.
--
-- The settlement evidence is placed on the DEMOTED version 1 of Candidate 3's
-- family; the export is asked for the CURRENT version 2.  `proof/34 section 8`:
-- rotation must never hide prior payment activity, so the figure must still be
-- the settled allocation.  Under contract decision D2 these are fixtures in the
-- EXISTING Banking Pay evidence tables; no Banking Pay owner is driven, no
-- Banking Pay definition is touched, and nothing infers a result that depends
-- on Banking Pay's unfinished logic.
-- ===========================================================================
-- WP-14b: `p_settled_at` exists because WP-11d's settlement-position gate
-- refuses to state a position when two settlements share the maximum
-- `settled_at_utc` - which is correct, and which every batch this fixture
-- seeded used to do, because they all took `transaction_timestamp()`.  Real
-- batches settle at different instants.
create or replace function pg_temp.seed_settled_batch(
  p_batch_id uuid,p_timesheet_id uuid,p_candidate_id uuid,p_snapshot jsonb,
  p_signature_mode text default 'MATCHING',
  p_settled_at timestamptz default null
) returns void language plpgsql as $seed$
declare
  v_batch_candidate uuid:=pg_catalog.gen_random_uuid();
  -- WP-14b, Part 1 rule 6 and rule 10.  WP-11d's F2 change makes the Gate 9
  -- allocation reader check that the settlement signature RE-COMPUTES from the
  -- signed content, by the installed writer's own scheme
  -- (`public.pay_batch_create_timesheet_snapshots`: `md5(target_snapshot_json)`).
  -- This fixture used to sign `sha256(batch||timesheet)`, which no installed
  -- writer ever produces, so it was seeding evidence the real system cannot
  -- create.  It now signs exactly as the writer does.  No Banking Pay owner is
  -- driven and no Banking Pay definition is touched (decision D2); the scheme
  -- is read from the installed writer, not invented here, and it carries no
  -- secret.
  v_signature text:=pg_catalog.md5(p_snapshot::text);
  v_now timestamptz:=coalesce(p_settled_at,pg_catalog.transaction_timestamp());
begin
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,created_by_user_id,
    execution_commit_state,execution_commit_ref,execution_committed_at_utc,
    completed_at_utc
  ) values (
    p_batch_id,date '2026-09-18','SETTLED','MONZO_CSV','CSV','CSV','SANDBOX','PAYE',
    'c4000000-0000-4000-8000-000000000001','COMMITTED',
    'wp14-commit:'||p_batch_id::text,v_now,v_now);
  insert into public.pay_batch_candidates(
    id,pay_batch_id,candidate_id,candidate_tms_ref,candidate_display_name,
    paye_state,settlement_status,settled_at_utc
  ) values (
    v_batch_candidate,p_batch_id,p_candidate_id,'WP14-001','WP14 Candidate',
    'READY','SETTLED',v_now);
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided
  ) values (
    pg_catalog.gen_random_uuid(),v_batch_candidate,'TIMESHEET_PAYMENT',
    p_timesheet_id,'PAYE',false);
  insert into public.pay_batch_timesheet_snapshots(
    id,pay_batch_id,timesheet_id,candidate_id,pay_channel,
    base_snapshot_json,target_snapshot_json,signature,created_at_utc
  ) values (
    pg_catalog.gen_random_uuid(),p_batch_id,p_timesheet_id,p_candidate_id,'PAYE',
    '{}'::jsonb,p_snapshot,v_signature,v_now-interval '2 minutes');
  insert into public.timesheet_pay_state_history(
    id,timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
  ) values (
    pg_catalog.gen_random_uuid(),p_timesheet_id,p_batch_id,v_now,p_snapshot,
    case when p_signature_mode='MISMATCHED'
      then pg_catalog.encode(extensions.digest('wrong','sha256'),'hex')
      else v_signature end);
end;
$seed$;

do $verify_paid_hours$
declare
  v_result jsonb;
  v_export jsonb;
begin
  -- Candidate 3's family is rotated; authorise the CURRENT version through the
  -- real owner so the week is a Weekly Source week for the export.
  v_result:=public.weekly_source_first_authorise_v1(
    'c4000000-0000-4000-8000-000000000303','c4000000-0000-4000-8000-000000000303',
    null,'c4000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'the rotated family''s current root must authorise: '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  -- Before any settlement evidence: no figure, and an explicit state.
  v_export:=private.weekly_source_export_hours_v1(
    'c4000000-0000-4000-8000-000000000303');
  perform pg_temp.assert_eq(v_export#>>'{paid_hours,state}','NO_SETTLEMENT',
    'no settlement evidence means an explicit no-settlement state: '||v_export::text);
  -- WP-14b, Part 1 rule 6: WP-11d's F8 change to the Gate 9 allocation reader
  -- REMOVED the zero from NO_SETTLEMENT, so that a consumer which reads a
  -- figure without branching on `state` cannot print "0 hours paid" for a week
  -- that was never paid.  That is the owning package's corrected truth and it
  -- is adopted here: NO_SETTLEMENT is still a different answer from
  -- UNAVAILABLE, and it still carries NO figure.
  perform pg_temp.assert_true(
    v_export#>>'{paid_hours,total_hours}' is null,
    'nothing settled carries no paid figure at all: '||v_export::text);
  perform pg_temp.assert_eq(v_export#>>'{paid_hours,state}','NO_SETTLEMENT',
    'and it is still distinguishable from UNAVAILABLE');

  -- Settlement on the DEMOTED physical version 1.
  perform pg_temp.seed_settled_batch(
    'c4000000-0000-4000-8000-00000000ba01',
    'c4000000-0000-4000-8000-000000000393',
    'c4000000-0000-4000-8000-000000000103',
    '{"segments":[
       {"segment_id":"s1","date":"2026-09-07","start_utc":"2026-09-07T08:00:00Z",
        "end_utc":"2026-09-07T16:00:00Z","break_mins":30,
        "hours_day":7.5,"hours_night":0,"hours_sat":0,"hours_sun":0,"hours_bh":0},
       {"segment_id":"s2","date":"2026-09-12","start_utc":"2026-09-12T20:00:00Z",
        "end_utc":"2026-09-13T08:00:00Z","break_mins":60,
        "hours_day":0,"hours_night":5,"hours_sat":6,"hours_sun":0,"hours_bh":0}
     ]}'::jsonb);

  -- Asked for the CURRENT version 2: rotation must not hide the payment.
  v_export:=private.weekly_source_export_hours_v1(
    'c4000000-0000-4000-8000-000000000303');
  perform pg_temp.assert_eq(v_export#>>'{paid_hours,state}','AVAILABLE',
    'the settled allocation on a demoted version is still the paid fact: '
      ||v_export::text);
  perform pg_temp.assert_eq(
    (v_export#>>'{paid_hours,total_hours}')::numeric::text,'18.5',
    'the paid figure is the sum of the settled per-shift buckets');
  perform pg_temp.assert_eq(v_export#>>'{paid_hours,batch_count}','1',
    'one contributing settled batch');
  -- And the paid fact is NOT any of the other three.
  perform pg_temp.assert_true(
    v_export#>>'{submitted_hours,state}'='NO_SUBMISSION'
    and v_export#>>'{source_hours,state}'='NO_SOURCE'
    and v_export#>>'{approved_hours,state}'='NO_APPROVED_ENTITLEMENT',
    'paid hours exist while submitted, source and approved do not: '||v_export::text);

  -- A second settled batch on the CURRENT version, settled LATER.
  --
  -- WP-14b, Part 1 rule 6.  This block used to assert 22.5 hours - the SUM of
  -- both settlements.  WP-11d's F1 change to the Gate 9 allocation reader
  -- REFUSES to state any position from more than one settlement until the
  -- finance approver has ruled whether a later snapshot restates or adds to the
  -- earlier one, because summing is what made the paid figure capable of being
  -- WRONG rather than merely unavailable.  That is the owning package's
  -- corrected truth and it is adopted here: two settlements give an explicit
  -- UNAVAILABLE and no figure at all.  What must NOT change, and does not, is
  -- that a single settlement still answers and that rotation hides nothing: the
  -- assertion above still reads the DEMOTED member's settlement through the
  -- current member.
  --
  -- WP-14c, 18 September 2026, Part 1 rule 6 and added rule 12.  The owning
  -- package has since RENAMED that reason from
  -- `SETTLEMENT_POSITION_SEMANTICS_UNRULED` to `SETTLEMENT_SEQUENCE_UNPROVABLE`,
  -- because ruling B1a settled the semantics question the old name claimed was
  -- open: the gate is still right to withhold, but for the newer and truer
  -- reason that no installed relation carries the sequence/revision the ruling
  -- names.  That is the owning package's corrected truth and it is ADOPTED
  -- here, not recorded as its defect.
  perform pg_temp.seed_settled_batch(
    'c4000000-0000-4000-8000-00000000ba02',
    'c4000000-0000-4000-8000-000000000303',
    'c4000000-0000-4000-8000-000000000103',
    '{"segments":[
       {"segment_id":"s3","date":"2026-09-09","start_utc":"2026-09-09T08:00:00Z",
        "end_utc":"2026-09-09T12:00:00Z","break_mins":0,
        "hours_day":4,"hours_night":0,"hours_sat":0,"hours_sun":0,"hours_bh":0}
     ]}'::jsonb,'MATCHING',
    pg_catalog.transaction_timestamp()+pg_catalog.make_interval(mins=>10));
  v_export:=private.weekly_source_export_hours_v1(
    'c4000000-0000-4000-8000-000000000303');
  perform pg_temp.assert_eq(v_export#>>'{paid_hours,state}','UNAVAILABLE',
    'two settlements state no position until finance rules: '||v_export::text);
  perform pg_temp.assert_eq(v_export#>>'{paid_hours,reason}',
    'SETTLEMENT_SEQUENCE_UNPROVABLE',
    'and the held decision is named, not hidden');
  perform pg_temp.assert_true(v_export#>>'{paid_hours,total_hours}' is null,
    'and no figure at all is shown');
  -- The export still shows the OTHER three facts, so one held money decision
  -- does not blank the week.
  perform pg_temp.assert_true(
    v_export#>>'{submitted_hours,state}'='NO_SUBMISSION'
    and v_export#>>'{source_hours,state}'='NO_SOURCE'
    and v_export#>>'{approved_hours,state}'='NO_APPROVED_ENTITLEMENT',
    'a held paid decision does not damage the other facts');

  -- Contradictory evidence: an explicit UNAVAILABLE with a reason, never a
  -- guess and never a partial figure.
  perform pg_temp.seed_settled_batch(
    'c4000000-0000-4000-8000-00000000ba03',
    'c4000000-0000-4000-8000-000000000303',
    'c4000000-0000-4000-8000-000000000103',
    '{"segments":[
       {"segment_id":"s4","date":"2026-09-10","start_utc":"2026-09-10T08:00:00Z",
        "end_utc":"2026-09-10T12:00:00Z","break_mins":0,
        "hours_day":4,"hours_night":0,"hours_sat":0,"hours_sun":0,"hours_bh":0}
     ]}'::jsonb,'MISMATCHED',
    pg_catalog.transaction_timestamp()+pg_catalog.make_interval(mins=>20));
  v_export:=private.weekly_source_export_hours_v1(
    'c4000000-0000-4000-8000-000000000303');
  perform pg_temp.assert_eq(v_export#>>'{paid_hours,state}','UNAVAILABLE',
    'contradictory settlement evidence makes the paid fact unavailable: '
      ||v_export::text);
  perform pg_temp.assert_true(v_export#>>'{paid_hours,reason}' is not null,
    'and the unavailable state names its reason');
  perform pg_temp.assert_true(v_export#>>'{paid_hours,total_hours}' is null,
    'and no figure at all is shown');
  -- The other three facts are untouched by the broken paid evidence.
  perform pg_temp.assert_true(
    v_export#>>'{source_hours,state}'='NO_SOURCE'
    and v_export#>>'{approved_hours,state}'='NO_APPROVED_ENTITLEMENT',
    'a broken paid fact does not damage the other facts');
end
$verify_paid_hours$;

-- ===========================================================================
-- 10B. WP-14b: the export and push defects the independent review executed.
--
-- Each block reproduces the reviewer's own probe on the REAL source-authority
-- week built in 9A, and asserts the corrected behaviour.  Nothing here is
-- sent: every notification row is created through the same boundary and left
-- `PENDING`, and the whole file rolls back.
-- ===========================================================================
do $verify_wp14b_submitted_shapes$
declare
  v_export jsonb;
  v_reader jsonb;
  v_original jsonb;
begin
  select actual_schedule_json into v_original from public.timesheets
   where timesheet_id='e1000000-0000-4000-8000-000000000305';

  -- F6 shape 1: `{date,start,end}`, which the Office screens and the installed
  -- calculators write.  The old summer looked only for `worked_start_iso`, so
  -- it reported AVAILABLE with a total of ZERO for a full submitted week.
  update public.timesheets
     set actual_schedule_json=
       '[{"date":"2026-09-07","start":"08:00","end":"16:00","break_minutes":30}]'::jsonb
   where timesheet_id='e1000000-0000-4000-8000-000000000305';
  v_export:=private.weekly_source_export_submitted_hours_v1(
    'e1000000-0000-4000-8000-000000000305');
  v_reader:=private.weekly_source_candidate_app_schedule_v1(
    '[{"date":"2026-09-07","start":"08:00","end":"16:00","break_minutes":30}]'::jsonb,
    '{}'::jsonb);
  perform pg_temp.assert_eq(v_export->>'state','AVAILABLE',
    'WP-14b F6: a {date,start,end} submission is readable');
  perform pg_temp.assert_eq((v_export->>'total_hours')::numeric::text,'7.5',
    'WP-14b F6: eight hours less a thirty minute break, not zero: '||v_export::text);
  perform pg_temp.assert_eq(pg_catalog.jsonb_array_length(v_reader)::text,'1',
    'WP-14b F6: and the installed reader parses the very same value');

  -- F6 shape 2: `{start_utc,end_utc}`, which the brokers write.
  update public.timesheets
     set actual_schedule_json=
       '[{"start_utc":"2026-09-07T08:00:00Z","end_utc":"2026-09-07T16:00:00Z",
          "break_minutes":30}]'::jsonb
   where timesheet_id='e1000000-0000-4000-8000-000000000305';
  v_export:=private.weekly_source_export_submitted_hours_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_eq(v_export->>'state','AVAILABLE',
    'WP-14b F6: a {start_utc,end_utc} submission is readable');
  perform pg_temp.assert_true((v_export->>'total_hours')::numeric>0,
    'WP-14b F6: and is not silently zero: '||v_export::text);

  -- F6 fail-closed: a segment that cannot be read is UNAVAILABLE WITH A REASON
  -- and carries NO figure.  `24 section 18` - a report must never state an
  -- available submitted figure it did not derive.
  update public.timesheets
     set actual_schedule_json='[{"date":"2026-09-07","start":"not-a-time"}]'::jsonb
   where timesheet_id='e1000000-0000-4000-8000-000000000305';
  v_export:=private.weekly_source_export_submitted_hours_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_eq(v_export->>'state','UNAVAILABLE',
    'WP-14b F6: an unreadable segment is UNAVAILABLE');
  perform pg_temp.assert_eq(v_export->>'reason','SUBMITTED_SCHEDULE_NOT_DERIVABLE',
    'WP-14b F6: with a reason');
  perform pg_temp.assert_true(v_export->'total_hours'='null'::jsonb,
    'WP-14b F6: and NEVER a zero: '||v_export::text);

  update public.timesheets set actual_schedule_json=v_original
   where timesheet_id='e1000000-0000-4000-8000-000000000305';
  perform pg_temp.assert_eq(
    (private.weekly_source_export_submitted_hours_v1(
       'e1000000-0000-4000-8000-000000000305')->>'total_hours')::numeric::text,
    '9.5','WP-14b F6: the original submission still reads 9.5 hours');
end
$verify_wp14b_submitted_shapes$;
-- F1.  The Candidate push must carry the APPROVED ENTITLEMENT, not the source
-- behind it.  Reproduced exactly as the review did: a certified-zero head,
-- then a later head with four hours.
do $verify_wp14b_push_head$
declare
  v_payload jsonb;
  v_before integer;
  v_after integer;
  v_dedupe_zero text;
  v_dedupe_four text;
begin
  select pg_catalog.count(*)::integer into v_before
  from public.candidate_notifications
  where timesheet_id='e1000000-0000-4000-8000-000000000305';

  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    'e1000000-0000-4000-8000-0000000000b5',1,'c4000000-0000-4000-8000-00000000af01',
    'e1000000-0000-4000-8000-000000000105','2026-09-13','SINGLE_ROOT','WP14-BK-05',
    'e1000000-0000-4000-8000-000000000305','e1000000-0000-4000-8000-000000000205',
    'e1000000-0000-4000-8000-0000000000b5','c4000000-0000-4000-8000-000000000001',
    'IMMEDIATE',decode(repeat('e1',32),'hex'),decode(repeat('e2',32),'hex'),
    decode(repeat('e3',32),'hex'),decode(repeat('e4',32),'hex'),
    array['e1000000-0000-4000-8000-0000000000c5']::uuid[],'PROPOSED');

  -- A CERTIFIED-ZERO head, superseding the 16.5-hour head the week already
  -- has: Office has decided this week is worth no hours.  The SOURCE has not
  -- moved and still carries two shifts, which is exactly the case where the old
  -- payload told the Candidate about those two shifts.
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id
  ) values (
    'e1000000-0000-4000-8000-0000000000c5','LOCKED_FINAL_SOURCE',
    'c4000000-0000-4000-8000-00000000af01','e1000000-0000-4000-8000-000000000105',
    'e1000000-0000-4000-8000-000000000205','2026-09-13',
    'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,2,
    'e1000000-0000-4000-8000-0000000000c4','STAGED',
    true,0,decode(repeat('e5',32),'hex'),decode(repeat('e6',32),'hex'),
    decode(repeat('e7',32),'hex'),'e1000000-0000-4000-8000-0000000000b5',1,
    'e1000000-0000-4000-8000-0000000000b5','c4000000-0000-4000-8000-000000000001');
  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',superseded_at_utc=pg_catalog.transaction_timestamp(),
         superseded_by_head_id='e1000000-0000-4000-8000-0000000000c5'
   where id='e1000000-0000-4000-8000-0000000000c4';
  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('e8',32),'hex'),
         scope_change_tx_token=pg_catalog.gen_random_uuid()
   where id='e1000000-0000-4000-8000-0000000000c5';

  v_payload:=private.weekly_source_candidate_hours_push_payload_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_eq(v_payload->>'approved_hours_source',
    'COMMITTED_ENTITLEMENT_HEAD',
    'WP-14b F1: with a committed head the payload comes from the HEAD');
  perform pg_temp.assert_eq(
    pg_catalog.jsonb_array_length(v_payload#>'{template_params,approved_hours}')::text,
    '0','WP-14b F1: a certified-zero head tells the Candidate NO shifts, where '
      ||'the source-derived payload told them two: '||v_payload::text);
  perform pg_temp.assert_eq(
    (v_payload#>>'{template_params,approved_hours_total}')::numeric::text,'0',
    'WP-14b F1: and states the approved total as zero');

  -- Rows are identified by the approved total they carry, not by a sort order:
  -- the no-head push made earlier in section 10 carries no total at all, so
  -- the head-based rows are unambiguous.
  select pg_catalog.count(*)::integer into v_after
  from public.candidate_notifications
  where timesheet_id='e1000000-0000-4000-8000-000000000305';
  perform pg_temp.assert_eq((v_after-v_before)::text,'1',
    'WP-14b F1: the certified-zero decision IS pushed - a change to zero is a '
      ||'change the Candidate must be told about');
  perform pg_temp.assert_eq(
    (select pg_catalog.count(*)::text from public.candidate_notifications
      where timesheet_id='e1000000-0000-4000-8000-000000000305'
        and (template_params->>'approved_hours_total')::numeric=0),
    '1','WP-14b F1: exactly one row states an approved total of zero');
  select dedupe_key into v_dedupe_zero
  from public.candidate_notifications
  where timesheet_id='e1000000-0000-4000-8000-000000000305'
    and (template_params->>'approved_hours_total')::numeric=0;
  perform pg_temp.assert_eq(
    (select pg_catalog.jsonb_array_length(template_params->'approved_hours')::text
       from public.candidate_notifications
      where timesheet_id='e1000000-0000-4000-8000-000000000305'
        and dedupe_key=v_dedupe_zero),
    '0','WP-14b F1: and the row the Candidate would receive carries no shifts');

  -- A LATER head that approves SEVEN hours: Office has decided the Monday shift
  -- is not theirs and only the Tuesday shift is payable.  The SOURCE has not
  -- moved - it still says 16.5 hours across two shifts - so the old dedupe
  -- digest was unchanged and NOTHING was pushed at all.
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id
  ) values (
    'e1000000-0000-4000-8000-0000000000c6','LOCKED_FINAL_SOURCE',
    'c4000000-0000-4000-8000-00000000af01','e1000000-0000-4000-8000-000000000105',
    'e1000000-0000-4000-8000-000000000205','2026-09-13',
    'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,3,
    'e1000000-0000-4000-8000-0000000000c5','STAGED',false,1,
    decode(repeat('f5',32),'hex'),decode(repeat('f6',32),'hex'),
    decode(repeat('f7',32),'hex'),'e1000000-0000-4000-8000-0000000000b5',1,
    'e1000000-0000-4000-8000-0000000000b5','c4000000-0000-4000-8000-000000000001');
  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,
    economic_key_value,component_member_identity,segment_id,work_date,
    hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_ex_vat,
    exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
  ) values (
    'e1000000-0000-4000-8000-0000000000c6',1,
    'e1000000-0000-4000-8000-0000000000d6','WORKED_TIME','WORK_EVENT',
    'e1000000-0000-4000-8000-000000000902',
    'e1000000-0000-4000-8000-000000000902','seg-2','2026-09-08',
    7,0,0,0,0,0,false,'LOCKED_FINAL_SOURCE',
    'e1000000-0000-4000-8000-0000000000b5',1,decode(repeat('f9',32),'hex'));
  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',superseded_at_utc=pg_catalog.transaction_timestamp(),
         superseded_by_head_id='e1000000-0000-4000-8000-0000000000c6'
   where id='e1000000-0000-4000-8000-0000000000c5';
  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('f8',32),'hex'),
         scope_change_tx_token=pg_catalog.gen_random_uuid()
   where id='e1000000-0000-4000-8000-0000000000c6';

  select pg_catalog.count(*)::integer into v_after
  from public.candidate_notifications
  where timesheet_id='e1000000-0000-4000-8000-000000000305';
  perform pg_temp.assert_eq((v_after-v_before)::text,'2',
    'WP-14b F1: the later seven-hour head DOES push, where the source-keyed '
      ||'dedupe digest used to push nothing at all');
  perform pg_temp.assert_eq(
    (select pg_catalog.count(*)::text from public.candidate_notifications
      where timesheet_id='e1000000-0000-4000-8000-000000000305'
        and (template_params->>'approved_hours_total')::numeric=7),
    '1','WP-14b F1: exactly one row states an approved total of seven; rows are '
      ||coalesce((select pg_catalog.string_agg(
                    coalesce(template_params->>'approved_hours_total','<null>'),',')
                   from public.candidate_notifications
                  where timesheet_id='e1000000-0000-4000-8000-000000000305'),'<none>'));
  select dedupe_key into v_dedupe_four
  from public.candidate_notifications
  where timesheet_id='e1000000-0000-4000-8000-000000000305'
    and (template_params->>'approved_hours_total')::numeric=7;
  perform pg_temp.assert_true(v_dedupe_four is distinct from v_dedupe_zero,
    'WP-14b F1: the dedupe key reflects what the Candidate is TOLD, so a '
      ||'changed entitlement always makes a new key');
  perform pg_temp.assert_true(
    (select (template_params->>'approved_hours_total')::numeric
       from public.candidate_notifications
      where timesheet_id='e1000000-0000-4000-8000-000000000305'
        and dedupe_key=v_dedupe_four)=7::numeric,
    'WP-14b F1: and it carries the seven approved hours');
  perform pg_temp.assert_true(
    (select (private.weekly_source_export_approved_hours_v1(
              'e1000000-0000-4000-8000-000000000305')->>'total_hours')::numeric)
      =(select (template_params->>'approved_hours_total')::numeric
          from public.candidate_notifications
         where timesheet_id='e1000000-0000-4000-8000-000000000305'
           and dedupe_key=v_dedupe_four),
    'WP-14b F1: which is exactly what the approved export reports - the push '
      ||'and the export now agree about the same entitlement');
  perform pg_temp.assert_true(
    not exists(select 1 from public.candidate_notifications
                where timesheet_id='e1000000-0000-4000-8000-000000000305'
                  and push_state<>'PENDING'),
    'WP-14b: nothing was delivered - every row is still PENDING');
  -- The same entitlement never pushes twice.
  perform private.weekly_source_candidate_hours_push_v1(
    'e1000000-0000-4000-8000-000000000305');
  select pg_catalog.count(*)::integer into v_after
  from public.candidate_notifications
  where timesheet_id='e1000000-0000-4000-8000-000000000305';
  perform pg_temp.assert_eq((v_after-v_before)::text,'2',
    'WP-14b F1: and the SAME approved hours still never push twice');

  -- The fallback the payload builder uses if WP-11d's entitlement reader is not
  -- installed is EXECUTED here, so "it never reads the source" is proved for
  -- that path too rather than asserted (Part 1 rule 1).
  perform pg_temp.assert_eq(
    pg_catalog.jsonb_array_length(
      private.weekly_source_candidate_hours_push_head_rows_v1(
        'e1000000-0000-4000-8000-0000000000c6'))::text,
    '1','WP-14b F1: the head-only fallback returns the head''s one component');
  perform pg_temp.assert_true(
    (private.weekly_source_candidate_hours_push_head_rows_v1(
       'e1000000-0000-4000-8000-0000000000c6')->0->>'hours')::numeric=7::numeric,
    'WP-14b F1: and its hours are the HEAD''s, not the source''s 16.5');
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_hours_push_head_rows_v1(
      'e1000000-0000-4000-8000-0000000000c5')::text,'[]',
    'WP-14b F1: and a certified-zero head yields no rows at all');

  -- WP-14b.  A head whose component names work the system cannot resolve HAS an
  -- approved entitlement that cannot be described.  The Candidate must not be
  -- told a guess, and the silence must not be silent: the push is refused and
  -- the refusal is audited against the Timesheet.
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id
  ) values (
    'e1000000-0000-4000-8000-0000000000c7','LOCKED_FINAL_SOURCE',
    'c4000000-0000-4000-8000-00000000af01','e1000000-0000-4000-8000-000000000105',
    'e1000000-0000-4000-8000-000000000205','2026-09-13',
    'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,4,
    'e1000000-0000-4000-8000-0000000000c6','STAGED',false,1,
    decode(repeat('b1',32),'hex'),decode(repeat('b2',32),'hex'),
    decode(repeat('b3',32),'hex'),'e1000000-0000-4000-8000-0000000000b5',1,
    'e1000000-0000-4000-8000-0000000000b5','c4000000-0000-4000-8000-000000000001');
  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,
    economic_key_value,component_member_identity,segment_id,work_date,
    hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_ex_vat,
    exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
  ) values (
    'e1000000-0000-4000-8000-0000000000c7',1,
    'e1000000-0000-4000-8000-0000000000d7','WORKED_TIME','SEGMENT','seg-x',
    'no-such-work-event','seg-x','2026-09-07',
    4,0,0,0,0,0,false,'LOCKED_FINAL_SOURCE',
    'e1000000-0000-4000-8000-0000000000b5',1,decode(repeat('b4',32),'hex'));
  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',superseded_at_utc=pg_catalog.transaction_timestamp(),
         superseded_by_head_id='e1000000-0000-4000-8000-0000000000c7'
   where id='e1000000-0000-4000-8000-0000000000c6';
  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('b5',32),'hex'),
         scope_change_tx_token=pg_catalog.gen_random_uuid()
   where id='e1000000-0000-4000-8000-0000000000c7';

  select pg_catalog.count(*)::integer into v_after
  from public.candidate_notifications
  where timesheet_id='e1000000-0000-4000-8000-000000000305';
  perform pg_temp.assert_eq((v_after-v_before)::text,'2',
    'WP-14b: an entitlement that cannot be described tells the Candidate NOTHING');
  perform pg_temp.assert_eq(
    (select after_json->>'withheld_reason' from public.audit_events
      where object_id_text='e1000000-0000-4000-8000-000000000305'
        and action='WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD'),
    'APPROVED_ENTITLEMENT_UNAVAILABLE',
    'WP-14b: and the silence is RECORDED, with its reason');
  perform pg_temp.assert_true(
    (select actor_user_id is null from public.audit_events
      where object_id_text='e1000000-0000-4000-8000-000000000305'
        and action='WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD'),
    'WP-14b F5: the withholding is the system''s act, not a person''s');
  perform pg_temp.assert_eq(
    (select state from public.weekly_source_entitlement_heads
      where id='e1000000-0000-4000-8000-0000000000c7'),
    'COMMITTED_CURRENT',
    'WP-14b: and the entitlement publication stands - a message never undoes it');
end
$verify_wp14b_push_head$;
-- F7.  A later accepted upload of the same two shifts - an ordinary later
-- source change (`24 section 4.2`) - used to DOUBLE the source figure, because
-- the lineage owner writes one row per resolution and the export summed every
-- lineage row ever bound to the Timesheet.  33 hours were reported for a 16.5
-- hour week.
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,
  physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'e1000000-0000-4000-8000-000000000702','e1000000-0000-4000-8000-000000000601',
  'wp14b-gate11-reexport.xlsx',decode(repeat('77',32),'hex'),100,
  '34444444-4444-4444-8444-444444444444','verify','verify',
  decode(repeat('78',32),'hex'),decode(repeat('79',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',2,2,decode(repeat('7a',32),'hex'),
  'CURRENT','c4000000-0000-4000-8000-000000000001');
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,normalised_row_hash
) values
  ('e1000000-0000-4000-8000-000000000a03','e1000000-0000-4000-8000-000000000702',
   1,'wp14-shift-1','JO NURSE','WP14 CLIENT','2026-09-07',
   '2026-09-07 09:00:00','2026-09-07 19:00:00',30,570,'SOURCE_WORKED',
   decode(repeat('93',32),'hex')),
  ('e1000000-0000-4000-8000-000000000a04','e1000000-0000-4000-8000-000000000702',
   2,'wp14-shift-2','JO NURSE','WP14 CLIENT','2026-09-08',
   '2026-09-08 09:00:00','2026-09-08 17:00:00',60,420,'SOURCE_WORKED',
   decode(repeat('94',32),'hex'));
insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,candidate_id,client_id,contract_id,
  work_event_id,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint
) values
  ('e1000000-0000-4000-8000-000000000b03','e1000000-0000-4000-8000-000000000a03',
   1,'RESOLVED','e1000000-0000-4000-8000-000000000105',
   'c4000000-0000-4000-8000-000000000002','e1000000-0000-4000-8000-000000000205',
   'e1000000-0000-4000-8000-000000000901','AUTO_UNIQUE','NEW_PROFILE_KEY',
   decode(repeat('b3',32),'hex'),decode(repeat('a7',32),'hex'),
   decode(repeat('a8',32),'hex'),decode(repeat('a9',32),'hex')),
  ('e1000000-0000-4000-8000-000000000b04','e1000000-0000-4000-8000-000000000a04',
   1,'RESOLVED','e1000000-0000-4000-8000-000000000105',
   'c4000000-0000-4000-8000-000000000002','e1000000-0000-4000-8000-000000000205',
   'e1000000-0000-4000-8000-000000000902','AUTO_UNIQUE','NEW_PROFILE_KEY',
   decode(repeat('b4',32),'hex'),decode(repeat('aa',32),'hex'),
   decode(repeat('ab',32),'hex'),decode(repeat('ac',32),'hex'));
insert into public.weekly_source_row_timesheet_lineages(
  id,row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,
  contract_id,contract_week_id,timesheet_id,family_booking_id,timesheet_version,
  week_ending_date,lineage_fingerprint
) values
  ('e1000000-0000-4000-8000-000000000e03','e1000000-0000-4000-8000-000000000b03',
   'e1000000-0000-4000-8000-000000000601','e1000000-0000-4000-8000-000000000901',
   'e1000000-0000-4000-8000-000000000105','c4000000-0000-4000-8000-000000000002',
   'e1000000-0000-4000-8000-000000000205','e1000000-0000-4000-8000-000000000405',
   'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,'2026-09-13',
   decode(repeat('c3',32),'hex')),
  ('e1000000-0000-4000-8000-000000000e04','e1000000-0000-4000-8000-000000000b04',
   'e1000000-0000-4000-8000-000000000601','e1000000-0000-4000-8000-000000000902',
   'e1000000-0000-4000-8000-000000000105','c4000000-0000-4000-8000-000000000002',
   'e1000000-0000-4000-8000-000000000205','e1000000-0000-4000-8000-000000000405',
   'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,'2026-09-13',
   decode(repeat('c4',32),'hex'));

do $verify_wp14b_source_hours$
declare
  v_source jsonb;
begin
  v_source:=private.weekly_source_export_source_hours_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_eq(v_source->>'state','AVAILABLE',
    'WP-14b F7: the source fact is still available after a re-upload');
  perform pg_temp.assert_eq(v_source->>'lineage_row_count','4',
    'WP-14b F7: there really ARE four lineage rows now - this is the case that '
      ||'used to double the figure');
  perform pg_temp.assert_eq(v_source->>'row_count','2',
    'WP-14b F7: but only the current publication''s two rows are counted: '
      ||v_source::text);
  perform pg_temp.assert_eq((v_source->>'total_hours')::numeric::text,'16.5',
    'WP-14b F7: sixteen and a half hours, not thirty-three: '||v_source::text);
end
$verify_wp14b_source_hours$;

-- ===========================================================================
-- 11. Notification routes: the manager email and the Office notice store
-- ===========================================================================
do $verify_routes$
declare
  v_contract jsonb;
begin
  v_contract:=private.weekly_source_notification_route_contract_v1();
  perform pg_temp.assert_true((v_contract->>'ok')::boolean,
    'the notification route contract must hold: '||v_contract::text);

  -- The grouped manager email cannot begin on any route but source authority:
  -- every cohort, recipient route, generation, intent and render descends from
  -- the cohort owner, and that owner refuses anything else.
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      to_regprocedure('private.weekly_source_query_cohort_ensure_v1(uuid,uuid,uuid,uuid,date)'))
      like '%authority_mode%SOURCE_AUTHORITY%',
    'the cohort owner must gate on authority_mode=SOURCE_AUTHORITY');
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      to_regprocedure('private.weekly_source_query_cohort_ensure_v1(uuid,uuid,uuid,uuid,date)'))
      like '%WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE%',
    'and must refuse every other route');

  -- With no Weekly Source group, client policy or cycle in this fixture, no
  -- manager route, generation, intent, render or dispatch target exists at all.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_manager_recipient_routes)=0
    and (select pg_catalog.count(*) from public.weekly_message_intents)=0
    and (select pg_catalog.count(*) from public.weekly_message_renders)=0
    and (select pg_catalog.count(*) from public.weekly_message_dispatch_targets)=0,
    'nothing in the Gate 11 package creates a manager route, render or dispatch target');

  -- Office Weekly source notices live in their own store.
  perform pg_temp.assert_true(
    to_regclass('public.office_action_notifications') is not null
    and to_regprocedure('public.weekly_source_office_notifications_list_v1(jsonb)') is not null,
    'the Office Weekly source notice store and its reader must exist');
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      to_regprocedure('public.weekly_source_office_notifications_list_v1(jsonb)'))
      not like '%banking_alert%',
    'the Office Weekly source notice reader must not read a Banking alert relation');
  perform pg_temp.assert_true(
    not exists(
      select 1 from pg_proc p join pg_namespace n on n.oid=p.pronamespace
      where n.nspname in ('public','private')
        and p.proname like 'banking\_alert%'
        and pg_catalog.pg_get_functiondef(p.oid) like '%office\_action\_notifications%'),
    'no Banking alert owner may read the Weekly source notice store');
end
$verify_routes$;

-- ===========================================================================
-- 12. WP-14c: the POST-ROLLBACK record of a guard refusal that RAISED
--     (HANDOVER 2 round-5 ruling A2, contract decision D13)
--
-- This section is LAST on purpose: it writes one audit row, and putting it
-- last means no earlier chronology or cardinality assertion can be disturbed
-- by it.
--
-- What can and cannot be proved from inside a verifier is itself part of the
-- ruling.  This whole file is ONE transaction that has already written a great
-- deal, so the public RPC must -- and does -- refuse to run here at all: that
-- is the separate-transaction invariant, asserted directly below.  The
-- positive path is therefore driven through the private recorder, and the
-- committed end-to-end proof (refuse, roll back, record in a NEW transaction,
-- read it back from a THIRD session) lives in the package report, which is
-- where a committed proof can live.
-- ===========================================================================
do $verify_wp14c_post_rollback_record$
declare
  v_refusal jsonb;
  v_result jsonb;
  v_row public.audit_events%rowtype;
  v_shim jsonb;
  v_expected_corroboration text;
  v_basis text;
  v_unknown text[];
begin
  -- 12.1 The refusal exactly as a durable caller catches it: SQLSTATE,
  --      message and the DETAIL object WP-09b's installed sites build.  The
  --      structure is the guard's, not this file's.
  v_refusal:=pg_catalog.jsonb_build_object(
    'sqlstate','55000',
    'message','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
    'detail',pg_catalog.jsonb_build_object(
      'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
      'entry_point','E7:public.tsfin_prepare_write',
      'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
      'refusal_basis','WEEKLY_SOURCE_MANAGED_ROOT',
      'integrity_failure',false,
      'timesheet_id','c4000000-0000-4000-8000-000000000301',
      'reason',null));

  -- 12.2 The public RPC refuses inside a transaction that has already
  --      written.  This is the whole of ruling A2's "separate transaction":
  --      the record can never be bolted onto the attempt, or onto a savepoint
  --      inside it, because the server refuses before writing anything.
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$select public.weekly_source_guard_refusal_record_after_rollback_v1(
        jsonb_build_object('correlation_id','ver-12-2','caller','verifier',
          'actor_user_id',null,'refusals',jsonb_build_array(%L::jsonb)))$sql$,
      v_refusal::text),
    '%WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_NOT_A_SEPARATE_TRANSACTION%',
    'the post-rollback recorder inside a transaction that has written');

  -- 12.3 The positive path, through the private recorder.
  v_shim:=private.weekly_source_managed_root_guard_decision_v1(
    'c4000000-0000-4000-8000-000000000301');
  v_expected_corroboration:=case
    when v_shim is null or pg_catalog.jsonb_typeof(v_shim)<>'object'
      then 'UNAVAILABLE'
    when coalesce((v_shim->>'managed')::boolean,false)
      or coalesce((v_shim->>'authorisation_record_without_authorised_timesheet')::boolean,false)
      or (v_shim->>'protected_target_ownership_state') is not null
      then 'STILL_REFUSES'
    else 'NO_LONGER_REFUSES' end;

  v_result:=private.weekly_source_guard_refusal_record_v1(
    'wp14c-correlation-0001',v_refusal,'verifier:wp14c',
    'c4000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and coalesce((v_result->>'recorded')::boolean,false),
    'a caught guard refusal is recorded: '||v_result::text);
  perform pg_temp.assert_eq(v_result->>'correlation_id','wp14c-correlation-0001',
    'the record carries the correlation identity of the attempt');
  perform pg_temp.assert_eq(v_result->>'corroboration',v_expected_corroboration,
    'the server-side corroboration is stored, not guessed');

  select * into v_row from public.audit_events
   where object_id_text='c4000000-0000-4000-8000-000000000301'
     and action='WEEKLY_SOURCE_ROTATION_REFUSED'
     and after_json->>'record_source'='CAUGHT_REFUSAL_POST_ROLLBACK';
  perform pg_temp.assert_true(v_row.id is not null,
    'exactly one post-rollback refusal record exists for this root');
  perform pg_temp.assert_eq(v_row.after_json->>'correlation_id',
    'wp14c-correlation-0001','the stored correlation identity');
  perform pg_temp.assert_eq(v_row.after_json->>'entry_point',
    'E7:public.tsfin_prepare_write',
    'the entry point comes from the guard DETAIL, not from a re-derivation');
  perform pg_temp.assert_eq(v_row.after_json->>'refusal_sqlstate','55000',
    'the SQLSTATE the caller caught is stored');
  perform pg_temp.assert_true(
    v_row.after_json->>'narrative'
      ='A request to replace this Timesheet was refused before anything was '
      ||'changed, because the week is already authorised for pay.',
    'plain English that says what actually happened: '
      ||coalesce(v_row.after_json->>'narrative','<null>'));
  perform pg_temp.assert_true(
    pg_catalog.strpos(coalesce(v_row.after_json->>'narrative',''),'{')=0
    and pg_catalog.strpos(coalesce(v_row.after_json->>'narrative',''),'":')=0,
    '24 section 18: the sentence is never raw JSON');

  -- 12.4 The chronology renders it, from the same vocabulary as everything
  --      else, without a second reader.
  perform pg_temp.assert_true(
    exists(
      select 1
      from pg_catalog.jsonb_array_elements(
        private.weekly_source_audit_chronology_v1(
          'c4000000-0000-4000-8000-000000000301')->'events') as event(value)
      where event.value->>'event'='WEEKLY_SOURCE_ROTATION_REFUSED'
        and event.value->>'narrative' like 'A request to replace this Timesheet was refused%'),
    'the post-rollback refusal appears in the Timesheet chronology');

  -- 12.5 A caller cannot manufacture a refusal.  Five separate shapes, each
  --      driven, each refused.
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$select private.weekly_source_guard_refusal_record_v1('c',%L::jsonb,'v',null)$sql$,
      (v_refusal||pg_catalog.jsonb_build_object('sqlstate','P0001'))::text),
    '%WEEKLY_SOURCE_GUARD_REFUSAL_NOT_A_GUARD_REFUSAL%',
    'a refusal that did not carry the guard SQLSTATE');
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$select private.weekly_source_guard_refusal_record_v1('c',%L::jsonb,'v',null)$sql$,
      (v_refusal||pg_catalog.jsonb_build_object('message','SOMETHING_ELSE'))::text),
    '%WEEKLY_SOURCE_GUARD_REFUSAL_NOT_A_GUARD_REFUSAL%',
    'a refusal that did not carry the guard message');
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$select private.weekly_source_guard_refusal_record_v1('c',%L::jsonb,'v',null)$sql$,
      pg_catalog.jsonb_set(v_refusal,'{detail,entry_point}',
        '"E99:public.not_installed_anywhere"'::jsonb)::text),
    '%WEEKLY_SOURCE_GUARD_REFUSAL_ENTRY_POINT_UNKNOWN%',
    'an entry point no installed routine can refuse from');
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$select private.weekly_source_guard_refusal_record_v1('c',%L::jsonb,'v',null)$sql$,
      pg_catalog.jsonb_set(v_refusal,'{detail,refusal_basis}',
        '"INVENTED_BASIS"'::jsonb)::text),
    '%WEEKLY_SOURCE_GUARD_REFUSAL_BASIS_UNKNOWN%',
    'a refusal basis the installed sites cannot emit');
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$select private.weekly_source_guard_refusal_record_v1(null,%L::jsonb,'v',null)$sql$,
      v_refusal::text),
    '%WEEKLY_SOURCE_GUARD_REFUSAL_CORRELATION_REQUIRED%',
    'a record with no correlation identity');
  perform pg_temp.assert_refused(
    $sql$select public.weekly_source_guard_refusal_record_after_rollback_v1(
      '{"correlation_id":"x","refusals":[],"something_else":1}'::jsonb)$sql$,
    '%WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_REQUEST_INVALID%',
    'an unknown request key');

  -- 12.6 DETAIL arrives as TEXT from a client, so the text form is accepted
  --      and is the same record.
  v_result:=private.weekly_source_guard_refusal_record_v1(
    'wp14c-correlation-0002',
    pg_catalog.jsonb_build_object(
      'sqlstate','55000',
      'message','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
      'detail',pg_catalog.to_jsonb((v_refusal->'detail')::text)),
    'verifier:wp14c-text-detail','c4000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'recorded')::boolean,false),
    'the DETAIL text form a client actually receives is accepted');

  -- 12.7 The basis vocabulary is complete: every basis token the INSTALLED
  --      refusal sites can emit is in the list the recorder validates against
  --      and the narrative renders.  Read from pg_proc, not from the tree.
  select pg_catalog.array_agg(distinct token.basis)
    into v_unknown
  from (
    select (pg_catalog.regexp_matches(
              pg_catalog.substring(
                installed.prosrc,
                'refusal_basis''.*?''integrity_failure'''),
              '''([A-Z][A-Z_]{6,})''','g'))[1] as basis
    from pg_catalog.pg_proc installed
    where pg_catalog.strpos(installed.prosrc,
            'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED')>0
      and pg_catalog.strpos(installed.prosrc,'refusal_basis')>0
      -- Only the GUARDED SITES, which are the routines that name an entry
      -- point in the refusal DETAIL.  The recorder below is not one of them
      -- and must not be scanned as if it were.
      and pg_catalog.strpos(installed.prosrc,'''entry_point'',''E')>0
  ) token
  where token.basis is not null
    and token.basis <> all(private.weekly_source_guard_refusal_bases_v1())
    and token.basis <> 'FAMILY_SPLIT_BY_WHITESPACE'
    and token.basis <> 'BOOKING_REFERENCE_CANONICAL_COLLISION';
  perform pg_temp.assert_true(
    coalesce(pg_catalog.cardinality(v_unknown),0)=0,
    'every installed refusal basis is in the recorder vocabulary; unknown: '
      ||coalesce(pg_catalog.array_to_string(v_unknown,','),'<none>'));

  -- 12.8 Every basis has its own plain-English clause, and none of them
  --      prints the token at Office.
  foreach v_basis in array private.weekly_source_guard_refusal_bases_v1()
  loop
    perform pg_temp.assert_true(
      pg_catalog.strpos(
        private.weekly_source_guard_refusal_basis_clause_v1(v_basis),v_basis)=0
      and private.weekly_source_guard_refusal_basis_clause_v1(v_basis)
          <> private.weekly_source_guard_refusal_basis_clause_v1('SOMETHING_ELSE'),
      'basis has its own plain-English clause and never prints its code: '||v_basis);
  end loop;

  -- 12.9 The entry-point validator is bound to what is INSTALLED.
  perform pg_temp.assert_true(
    private.weekly_source_guard_refusal_entry_point_installed_v1(
      'E7:public.tsfin_prepare_write'),
    'a real installed entry point is accepted');
  perform pg_temp.assert_true(
    private.weekly_source_guard_refusal_entry_point_installed_v1(
      'E7:public.tsfin_prepare_write ') is false
    and private.weekly_source_guard_refusal_entry_point_installed_v1(null) is false
    and private.weekly_source_guard_refusal_entry_point_installed_v1('') is false,
    'a padded, null or empty entry point is not accepted');
end
$verify_wp14c_post_rollback_record$;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_audit_and_export_v1',
  'scenarios',pg_catalog.jsonb_build_array(
    'structure-privileges-volatility','trigger-inventory',
    'static-no-currency-to-hours','static-no-last-settled-cache',
    'static-no-limit-safety',
    'payload-scanner-positive','payload-scanner-forbidden-words',
    'payload-scanner-nested-key','payload-scanner-forbidden-fields',
    'payload-scanner-non-object',
    'first-authorisation-real-owner','first-authorisation-plain-english',
    'chronology-renders-first-authorisation',
    'withdrawal-real-owner-UNA-001-preserved','chronology-renders-withdrawal',
    'reauthorisation-generation-2',
    'guard-refusal-recorded','guard-refusal-not-recorded-when-unmanaged',
    'guard-refusal-not-caller-supplied',
    'export-four-facts-separate','export-rotated-family',
    'export-ordinary-timesheet-empty','export-owner-calls-composer',
    'export-row-differential-pre-plan62-members',
    'export-source-authority-source-hours','export-source-differs-from-submitted',
    'paid-hours-no-settlement-is-a-proved-zero',
    'paid-hours-on-a-rotated-family','paid-hours-to-date-across-two-batches',
    'paid-hours-contradictory-evidence-unavailable',
    'paid-hours-failure-does-not-damage-other-facts',
    'push-refused-without-candidate-account',
    'push-raw-text-carries-no-forbidden-word',
    'push-forbidden-payload-fails-closed',
    'head-staged','head-published-immediate','head-published-deferred',
    'head-superseded','pending-saved','pending-frozen','pending-refrozen',
    'pending-manual-review','pending-superseded','pending-released',
    -- WP-14b, one per independent-review finding.
    'wp14b-F1-push-carries-the-committed-head',
    'wp14b-F1-certified-zero-head-tells-no-shifts',
    'wp14b-F1-later-head-with-different-hours-does-push',
    'wp14b-F1-dedupe-key-reflects-what-the-candidate-is-told',
    'wp14b-F1-same-approved-hours-still-never-push-twice',
    'wp14b-F2-technical-failure-is-not-a-frozen-payment',
    'wp14b-F2-busy-skip-is-not-a-frozen-payment',
    'wp14b-F2-frozen-still-reads-as-frozen',
    'wp14b-F3-reopen-visible-in-the-chronology',
    'wp14b-F3-office-decision-visible-in-the-chronology',
    'wp14b-F4-chronology-is-lifecycle-ordered-and-deterministic',
    'wp14b-F5-worker-transitions-name-no-office-user',
    'wp14b-F6-submitted-hours-read-every-installed-shape',
    'wp14b-F6-unreadable-submission-is-unavailable-never-zero',
    'wp14b-F7-source-hours-do-not-double-after-a-re-upload',
    'wp14b-F8-census-result-read-from-the-real-key',
    'wp14b-F9-refusal-sentence-states-only-what-is-known',
    'wp14b-F10-head-is-an-approved-hours-record-not-a-source-reference',
    'wp14b-lease-claim-is-recorded',
    'reopen-not-duplicated-G5-6',
    'chronology-complete-plain-english',
    'push-ordinary-week-no-push','push-payload-scanned-whole',
    'push-through-existing-boundary','push-idempotent','push-request-contract',
    'route-contract','manager-email-source-authority-only',
    'office-notices-separate-from-banking-alerts',
    'wp14c-post-rollback-recorder-refuses-inside-a-writing-transaction',
    'wp14c-caught-refusal-recorded-with-the-attempt-correlation-identity',
    'wp14c-refusal-structure-taken-from-the-guard-not-re-derived',
    'wp14c-post-rollback-refusal-rendered-in-the-chronology',
    'wp14c-caller-cannot-manufacture-a-refusal',
    'wp14c-detail-text-form-accepted',
    'wp14c-basis-vocabulary-covers-every-installed-basis',
    'wp14c-basis-clause-never-prints-its-code',
    'wp14c-entry-point-validator-is-bound-to-what-is-installed'),
  'messages_sent',0,
  'scaffolding','fixture seeds only; no owner definition is altered'
) as weekly_source_audit_and_export_verification;

rollback;
