-- Rollback-only PostgreSQL 17.11 proof for the Weekly Source pending-publication
-- release owners (`17092026_0700_weekly_source_pending_entitlement_release_v1.sql`).
--
-- Authority: `P:\proof\32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md`
-- sections 2, 3, 7, 8, 10 and 11; `P:\24_…AUTHORITY.md` section 4.4; the
-- HANDOVER 2 round-4 rulings preserved at
-- `..\plan6-pack-audit-20260916\HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R4.md`
-- (rulings 3, 4 and 6 point 7).  Scenario ids below are the `R` numbers of
-- `proof/32` section 12.
--
-- Interfaces I-1 (WP-03), I-2 (WP-08a) and I-4 (WP-02) are used as installed.
-- Where one is absent the section that needs it says SKIPPED out loud rather
-- than passing silently.
--
-- Everything here runs inside one transaction that ends in `rollback`.

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

create function pg_temp.expect_failure(
  p_sql text,p_sqlstate text,p_message_fragment text,p_label text
) returns void language plpgsql as $function$
declare
  v_state text;
  v_message text;
begin
  begin
    execute p_sql;
  exception when others then
    get stacked diagnostics v_state=returned_sqlstate, v_message=message_text;
    if p_sqlstate is not null and v_state<>p_sqlstate then
      raise exception 'ASSERTION_FAILED: % expected SQLSTATE % but got % (%)',
        p_label,p_sqlstate,v_state,v_message;
    end if;
    if coalesce(p_message_fragment,'')<>''
       and pg_catalog.strpos(coalesce(v_message,''),p_message_fragment)=0 then
      raise exception 'ASSERTION_FAILED: % expected message containing "%" but got "%"',
        p_label,p_message_fragment,v_message;
    end if;
    return;
  end;
  raise exception 'ASSERTION_FAILED: % was expected to fail and did not',p_label;
end;
$function$;

-- ---------------------------------------------------------------------------
-- 1. The installed definitions, and proof/32 section 11 by search
-- ---------------------------------------------------------------------------
do $verify_release_definitions$
declare
  v_function record;
  v_definition text;
  v_all text:='';
begin
  for v_function in
    select * from (values
      ('private.weekly_source_pending_release_backoff_v1(integer)','i','f'),
      ('private.weekly_source_pending_release_lock_and_census_v1(uuid,uuid[],text[],integer[],uuid)','v','t'),
      ('private.weekly_source_pending_entitlement_bundle_save_v1(jsonb,jsonb,jsonb)','v','t'),
      ('private.weekly_source_pending_entitlement_release_claim_page_v1(text,uuid,integer,integer)','v','t'),
      ('private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid)','v','t'),
      ('private.weekly_source_pending_release_technical_failure_v1(uuid,text,jsonb,jsonb)','v','t'),
      ('private.weekly_source_pending_release_manual_review_v1(uuid,text,jsonb)','v','t'),
      ('private.weekly_source_pending_release_superseded_v1(uuid,text,jsonb)','v','t'),
      ('private.weekly_source_pending_entitlement_release_record_failure_v1(uuid,uuid,text,uuid,text,text)','v','t'),
      ('public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid)','v','t'),
      ('public.weekly_source_pending_entitlement_release_claim_page_v1(jsonb)','v','t'),
      ('public.weekly_source_pending_entitlement_release_apply_v1(jsonb)','v','t'),
      ('public.weekly_source_pending_entitlement_release_record_failure_v1(jsonb)','v','t')
    ) as expected(signature,volatility,security_definer)
  loop
    perform pg_temp.assert_true(
      pg_catalog.to_regprocedure(v_function.signature) is not null,
      'installed function missing: '||v_function.signature);
    perform pg_temp.assert_true(
      (select p.provolatile=v_function.volatility and p.prosecdef=(v_function.security_definer='t')
         from pg_catalog.pg_proc p
        where p.oid=pg_catalog.to_regprocedure(v_function.signature)),
      'volatility or security setting wrong on '||v_function.signature);
    perform pg_temp.assert_true(
      (select p.proowner=(current_user::pg_catalog.regrole)::oid
         from pg_catalog.pg_proc p
        where p.oid=pg_catalog.to_regprocedure(v_function.signature)),
      'unexpected owner on '||v_function.signature);
    v_definition:=pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(v_function.signature));
    -- Workspace AGENTS.md conditional-expression rule: COALESCE, NULLIF, LEAST
    -- and GREATEST are syntax constructs, not pg_catalog functions.  A
    -- schema-qualified call compiles and then fails 42883 at first execution.
    perform pg_temp.assert_true(
      v_definition !~* 'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(',
      'illegal schema-qualified conditional expression in '||v_function.signature);
    v_all:=v_all||v_definition;
  end loop;

  -- Only the four public transport wrappers may be executable by service_role;
  -- every private owner is revoked from every role.
  perform pg_temp.assert_true(
    not exists (
      select 1
        from pg_catalog.pg_proc p
       cross join lateral pg_catalog.aclexplode(
         coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) acl
       where p.oid in (
         pg_catalog.to_regprocedure('private.weekly_source_pending_entitlement_bundle_save_v1(jsonb,jsonb,jsonb)'),
         pg_catalog.to_regprocedure('private.weekly_source_pending_entitlement_release_claim_page_v1(text,uuid,integer,integer)'),
         pg_catalog.to_regprocedure('private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid)'))
         and acl.grantee<>p.proowner),
    'no private release owner may carry a grant to any role');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from pg_catalog.pg_proc p
      cross join lateral pg_catalog.aclexplode(
        coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) acl
      where p.oid=pg_catalog.to_regprocedure(
              'public.weekly_source_pending_entitlement_release_apply_v1(jsonb)')
        and acl.grantee<>p.proowner
        and acl.grantee='service_role'::pg_catalog.regrole::oid)=1,
    'the public apply wrapper must be executable by service_role and nothing else');

  -- Review finding F1, as a permanent ordering guard.  proof/32 section 7 says
  -- "under the locks, BEFORE ANY WRITE".  The under-lock lease re-verification
  -- must therefore appear in the apply owner's source BEFORE every step-3
  -- refusal transition; if a later edit moves it back down, this fails.  The
  -- ordering cannot be proved by a single-session behavioural test, so it is
  -- proved from the installed definition itself.
  v_definition:=pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
    'private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid)'));
  perform pg_temp.assert_true(
    pg_catalog.strpos(v_definition,'''stage'',''UNDER_LOCK''')>0
    and pg_catalog.strpos(v_definition,'''stage'',''UNDER_LOCK''')
        <pg_catalog.strpos(v_definition,'''SKIPPED_THIS_TICK''')
    and pg_catalog.strpos(v_definition,'''stage'',''UNDER_LOCK''')
        <pg_catalog.strpos(v_definition,'weekly_source_pending_release_manual_review_v1(')
    and pg_catalog.strpos(v_definition,'''stage'',''UNDER_LOCK''')
        -- WP-08c, HANDOVER 2 round-5 ruling B4.1.  The apply owner no longer
        -- calls the technical-failure owner directly: every refusal that is not
        -- already a named skip, supersession or manual review now goes through
        -- `…_refuse_v1`, which decides transient-or-permanent.  Adopting the
        -- new truth (Part 1 review rule 6): the ordering requirement is
        -- unchanged and is now asserted against the router's call site.
        <pg_catalog.strpos(v_definition,'weekly_source_pending_release_refuse_v1('),
    'F1: the under-lock lease re-check must precede every step-3 refusal write');
  perform pg_temp.assert_true(
    v_definition !~ 'weekly_source_pending_release_technical_failure_v1\(',
    'B4.1: the apply owner must not spend the retry budget directly; only the '
    ||'refusal disposition owner may route a refusal to the budget');
  -- Exactly two row locks are taken by the apply owner and no more: the pending
  -- bundle and the accepted decision bundle, in that order (proof/32 section 6
  -- step 4).  A third would be a new lock in the money path.
  perform pg_temp.assert_true(
    (pg_catalog.length(v_definition)
     -pg_catalog.length(pg_catalog.replace(v_definition,'for update;','')))
    /pg_catalog.length('for update;')=2
    and pg_catalog.strpos(v_definition,'weekly_source_pending_entitlement_bundles as pending_row')
        <pg_catalog.strpos(v_definition,'weekly_source_entitlement_decision_bundles as bundle_row'),
    'F1: the apply owner locks the pending bundle then the decision bundle, and nothing else');

  -- proof/32 section 11, by search over every installed definition of this
  -- package.  The ONE permitted Banking Pay call is the informational
  -- banking_pay_batch_signal_touch that 24 section 4.4 names.
  perform pg_temp.assert_true(
    v_all !~* '(insert|update|delete)\s+(into\s+)?(public\.)?(pay_batch|pay_bank|pay_advance|pay_payment|pay_settle|banking_pay_operations|timesheet_pay_state|pay_finance)',
    'section 11: the release owner must write no Banking Pay relation');
  perform pg_temp.assert_true(
    v_all !~* 'for\s+(update|no\s+key\s+update|share)[^;]*(pay_batch|pay_bank|pay_advance|banking_pay)',
    'section 11: the release owner must hold no lock on a Banking Pay table');
  perform pg_temp.assert_true(
    v_all !~* 'lifecycle_defer_summary_refresh|bpay_scope_invalidator_active',
    'section 11: the release owner must never set or read a Workbench session setting');
  perform pg_temp.assert_true(
    v_all !~* '(insert|update|delete)\s+(into\s+)?(public\.)?timesheets\b'
    and v_all !~* '(insert|update|delete)\s+(into\s+)?(public\.)?timesheets_financials\b',
    'section 11: the release owner must never mutate a public Timesheet or current TSFIN');
  perform pg_temp.assert_true(
    v_all !~* 'weekly_exceptional_c1_(source|component|staging|checkpoint)',
    'section 11: the release owner must read no C1 staging or checkpoint table');
  perform pg_temp.assert_true(
    v_all !~* '\bp_now_utc\b|\bp_actor_user_id\b\s+(uuid)?[^,)]*\)\s*returns[^;]*release',
    'section 11: the release path accepts no actor and no timestamp from the caller');
  -- The apply owner never reimplements the coordinator: it calls it once.
  v_definition:=pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
    'private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid)'));
  perform pg_temp.assert_true(
    (pg_catalog.length(v_definition)
     -pg_catalog.length(pg_catalog.replace(v_definition,'weekly_source_entitlement_publish_core_v1','')))
    /pg_catalog.length('weekly_source_entitlement_publish_core_v1')=1,
    'section 8: the apply owner must call the coordinator from exactly one place');
  perform pg_temp.assert_true(
    v_definition !~* 'weekly_source_entitlement_publish_immediate_v1',
    'the deferred path must not route through the immediate entry point');
  -- The pinned job type, and only that one, in the lock helper.
  v_definition:=pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
    'private.weekly_source_pending_release_lock_and_census_v1(uuid,uuid[],text[],integer[],uuid)'));
  perform pg_temp.assert_true(
    v_definition like '%WORKBENCH_CANDIDATE_PENDING_ENTITLEMENT_RELEASE%'
    and v_definition not like '%WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION%'
    and v_definition not like '%WORKBENCH_CANDIDATE_FIRST_AUTHORISATION%',
    'proof/32 section 6 step 1: the deferred path pins exactly one job type');

  -- Only the single informational Banking Pay signal, from I-5 and nowhere else.
  perform pg_temp.assert_true(
    (pg_catalog.length(v_all)
     -pg_catalog.length(pg_catalog.replace(v_all,'banking_pay_batch_signal_touch','')))
    /pg_catalog.length('banking_pay_batch_signal_touch')=1,
    '24 section 4.4: exactly one bounded stale-warning call site');
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
      'private.weekly_source_pending_entitlement_bundle_save_v1(jsonb,jsonb,jsonb)'))
      like '%banking_pay_batch_signal_touch%',
    '24 section 4.4: the stale warning belongs to the save owner');
end
$verify_release_definitions$;

-- ---------------------------------------------------------------------------
-- 2. proof/32 section 10 bounds, as arithmetic
-- ---------------------------------------------------------------------------
do $verify_release_bounds$
declare
  v_case record;
begin
  for v_case in
    select * from (values
      (1,interval '60 seconds'),
      (2,interval '120 seconds'),
      (3,interval '240 seconds'),
      (4,interval '480 seconds'),
      (5,interval '15 minutes'),
      (9,interval '15 minutes'),
      (10,interval '15 minutes')
    ) as expected(failure_count,backoff)
  loop
    perform pg_temp.assert_true(
      private.weekly_source_pending_release_backoff_v1(v_case.failure_count)=v_case.backoff,
      'backoff for failure '||v_case.failure_count::text||' must be '||v_case.backoff::text
        ||' but was '||private.weekly_source_pending_release_backoff_v1(v_case.failure_count)::text);
  end loop;
  perform pg_temp.assert_true(
    private.weekly_source_pending_release_backoff_v1(99)=interval '15 minutes',
    'the backoff is capped at 15 minutes however many failures there have been');
end
$verify_release_bounds$;

-- ---------------------------------------------------------------------------
-- 2b. Review finding F3 - the Office-visible reason keeps every item identifier
-- ---------------------------------------------------------------------------
-- HANDOVER 2 round-4 ruling 4 requires the exact census item identifiers to
-- reach the Office.  The old prose-plus-appended-detail form cut them off after
-- the fourth item at 1,000 characters, silently.  The reason is now a
-- STRUCTURED object with its own `items` array, `item_count` and an explicit
-- `items_truncated` flag, capped by DROPPING WHOLE ITEMS at a stated 8,000
-- characters so nothing is ever cut mid-identifier and no loss is ever silent.
do $verify_release_review_reason$
declare
  v_case record;
  v_detail jsonb;
  v_reason text;
  v_json jsonb;
begin
  for v_case in select * from (values (1),(2),(4),(5),(8),(40),(400)) as sizes(item_count) loop
    select pg_catalog.jsonb_build_object(
             'census_result','CENSUS_ERROR',
             'census_reason','WEEKLY_SOURCE_CENSUS_FROZEN',
             'census_error_items',pg_catalog.jsonb_agg(
               pg_catalog.jsonb_build_object(
                 'pay_batch_item_id',pg_catalog.md5('item:'||generated.n::text)::uuid,
                 'timesheet_id',pg_catalog.md5('ts:'||generated.n::text)::uuid,
                 'reason','WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE')))
      into v_detail
      from pg_catalog.generate_series(1,v_case.item_count) as generated(n);

    v_reason:=private.weekly_source_pending_release_review_reason_v1(
      'WEEKLY_SOURCE_CENSUS_ERROR','ten consecutive technical failures',v_detail,10);

    -- Always valid JSON, always within the stated cap.
    perform pg_temp.assert_true(
      pg_catalog.jsonb_typeof(v_reason::jsonb)='object'
      and pg_catalog.length(v_reason)<=8000,
      'F3: the reason must stay valid JSON within the 8000-character cap at '
        ||v_case.item_count::text||' items (length '||pg_catalog.length(v_reason)::text||')');
    v_json:=v_reason::jsonb;
    -- The TRUE count is always reported, listed or not.
    perform pg_temp.assert_true(
      (v_json->>'item_count')::integer=v_case.item_count,
      'F3: item_count must be the true count at '||v_case.item_count::text||' items');
    -- Nothing is ever dropped silently.
    perform pg_temp.assert_true(
      (v_json->>'items_truncated')::boolean
        =(pg_catalog.jsonb_array_length(v_json->'items')<v_case.item_count),
      'F3: items_truncated must say exactly whether items were dropped');
    -- WP-08c, HANDOVER 2 round-5 ruling B4.4.  `complete_detail_in` is now an
    -- object, because the complete list no longer survives only in a census
    -- blob: it is written losslessly to a bounded, pageable CHILD RELATION, and
    -- the reason must name both that relation and its reader so an Office
    -- screen can FIND the identifiers rather than scrape a string.  Adopting
    -- the new truth (Part 1 review rule 6).
    perform pg_temp.assert_true(
      v_json->'complete_detail_in'->>'relation'
        ='private.weekly_source_pending_release_review_items'
      and v_json->'complete_detail_in'->>'reader'
        ='public.weekly_source_pending_release_review_items_page_v1'
      and v_json->'complete_detail_in'->>'census_blob'
        ='public.weekly_source_pending_entitlement_bundles.last_census_json',
      'B4.4: the reason must name the child relation, its reader and the census blob');
    -- The five-item case is the one the review measured at 1,055 characters:
    -- every identifier must now be present.
    if v_case.item_count<=40 then
      perform pg_temp.assert_true(
        (v_json->>'items_truncated')::boolean is false
        and pg_catalog.jsonb_array_length(v_json->'items')=v_case.item_count,
        'F3: all '||v_case.item_count::text||' item identifiers must reach the Office');
      perform pg_temp.assert_true(
        v_reason like '%'||(pg_catalog.md5('item:'||v_case.item_count::text)::uuid)::text||'%',
        'F3: the LAST item identifier must be present at '
          ||v_case.item_count::text||' items');
    end if;
  end loop;

  -- A reason with no census items at all is still a well-formed object.
  v_reason:=private.weekly_source_pending_release_review_reason_v1(
    'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','a rotated root',
    pg_catalog.jsonb_build_object('reason','ROOT_NOT_CANONICAL'),null::integer);
  perform pg_temp.assert_true(
    (v_reason::jsonb->>'item_count')::integer=0
    and (v_reason::jsonb->>'items_truncated')::boolean is false
    and v_reason::jsonb->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
    'F3: a reason with no census items is still structured: '||v_reason);
end
$verify_release_review_reason$;

-- ---------------------------------------------------------------------------
-- 3. Minimum legal fixture
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,pg_catalog.decode(pg_catalog.repeat('01',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('02',32),'hex')
) on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash,display_name)
values ('d0000000-0000-4000-8000-000000000001','release@example.test','admin',true,
        'not-a-login','Release Office User');
insert into public.clients(id,name)
values ('d0000000-0000-4000-8000-000000000002','Release Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('d0000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name) values
 ('d0000000-0000-4000-8000-000000000003','Release Candidate'),
 ('d0000000-0000-4000-8000-000000000023','Release Other Candidate');
-- Two Contracts for one Candidate: WSREL-0001 is the A root and WSREL-0002 is
-- the B root of the bounded A/B bundle (24 section 4.5).
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values
 ('d0000000-0000-4000-8000-000000000004','d0000000-0000-4000-8000-000000000003',
  'd0000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}'::jsonb,
  'HEALTHROSTER',true,true,true,true),
 ('d0000000-0000-4000-8000-000000000014','d0000000-0000-4000-8000-000000000003',
  'd0000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}'::jsonb,
  'HEALTHROSTER',true,true,true,true);
insert into public.contract_weeks(id,contract_id,week_ending_date) values
 ('d0000000-0000-4000-8000-000000000005','d0000000-0000-4000-8000-000000000004','2026-03-08'),
 ('d0000000-0000-4000-8000-000000000015','d0000000-0000-4000-8000-000000000014','2026-03-08');

-- Three roots, one family each.  WSREL-0001 is the bundle root; WSREL-0002 is
-- the second root of the mixed A/B freeze; WSREL-0003 is the rotation case and
-- gains a second, current version part way through.
--
-- WP-25.  The two roots that get an authorisation record below carry
-- `authorised_at_server` from the start.  A live authorisation generation on a
-- Timesheet that is NOT authorised is not an inert fixture shortcut: round-5
-- ruling A4 calls it an integrity contradiction, WP-07c's W9 refuses it as
-- `ROOT_NOT_AUTHORISED`, and the installed managed-root guard trigger
-- `weekly_source_managed_root_authorisation_guard_bu` refuses every ordinary
-- authorisation write on it with refusal basis
-- `AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET`.  The pair is seeded here
-- in the shape production has it - authorised Timesheet, live generation -
-- because it cannot be repaired afterwards: that guard is a BEFORE UPDATE
-- trigger on exactly this column.  WSREL-0003 gets no authorisation record and
-- so stays unauthorised.
insert into public.timesheets(
  timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
  occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
  week_ending_date,contract_id,actual_schedule_json,qr_payload_json,is_adjustment,
  authorised_at_server,created_at,updated_at
) values
 ('d0000000-0000-4000-8000-000000000006','WSREL-0001',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'rel-occupant-a','rel-hospital','rel-ward','rel-role','weekly-0','2026-03-08',
  'd0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
  pg_catalog.statement_timestamp(),
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()),
 ('d0000000-0000-4000-8000-000000000016','WSREL-0002',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'rel-occupant-b','rel-hospital','rel-ward','rel-role','weekly-0','2026-03-08',
  'd0000000-0000-4000-8000-000000000014','[]'::jsonb,'{}'::jsonb,false,
  pg_catalog.statement_timestamp(),
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()),
 ('d0000000-0000-4000-8000-000000000026','WSREL-0003',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'rel-occupant-c','rel-hospital','rel-ward','rel-role','weekly-0','2026-03-08',
  'd0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
  null,
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());

insert into public.weekly_source_format_profiles(
  id,profile_code,version,final_authority_kind,container_kind,omission_meaning,
  row_finalisation_capability,worked_duration_authority,profile_json,profile_sha256
) values (
  'd0000000-0000-4000-8000-0000000000f1','RELEASE_PROOF',1,
  'GENERIC_COMPLETE_SNAPSHOT','XLSX','CANCEL_INSIDE_CONFIRMED_COVERAGE','NONE',
  'SOURCE_ACTUAL','{}'::jsonb,pg_catalog.decode(pg_catalog.repeat('a1',32),'hex'));
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  'd0000000-0000-4000-8000-0000000000f2','TEST','d0000000-0000-4000-8000-0000000000aa',
  'RELEASE_GROUP','Release Group','ROSTER',3,'15:00');
insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc)
values ('d0000000-0000-4000-8000-0000000000f3','d0000000-0000-4000-8000-0000000000f2',
        '2026-03-08',pg_catalog.clock_timestamp());
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
  coverage_proof_kind,physical_row_count,uploaded_by_user_id
) values (
  'd0000000-0000-4000-8000-0000000000f4','d0000000-0000-4000-8000-0000000000f3',
  'release.xlsx',pg_catalog.decode(pg_catalog.repeat('a2',32),'hex'),1024,
  'd0000000-0000-4000-8000-0000000000f1','p1','n1',
  pg_catalog.decode(pg_catalog.repeat('a3',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('a4',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,'d0000000-0000-4000-8000-000000000001');
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,source_candidate_identity,source_client_identity,
  work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,normalised_row_hash
) values
 ('d0000000-0000-4000-8000-0000000000f5','d0000000-0000-4000-8000-0000000000f4',1,
  'cand-1','client-1','2026-03-02','2026-03-02 08:00','2026-03-02 16:00',30,450,
  pg_catalog.decode(pg_catalog.repeat('a5',32),'hex'));
insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,durable_identity_hash,
  source_format_profile_id,profile_external_key
) values
 ('d0000000-0000-4000-8000-0000000000f6','d0000000-0000-4000-8000-000000000003',
  'd0000000-0000-4000-8000-000000000002','2026-03-02','PROFILE_EXTERNAL_KEY',
  pg_catalog.decode(pg_catalog.repeat('a6',32),'hex'),
  'd0000000-0000-4000-8000-0000000000f1','release-external-key-a');
insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint,work_event_id,candidate_id,client_id,
  contract_id,contract_selection_method,work_event_match_kind,work_event_match_fingerprint
) values
 ('d0000000-0000-4000-8000-0000000000f7','d0000000-0000-4000-8000-0000000000f5',1,'RESOLVED',
  pg_catalog.decode(pg_catalog.repeat('a7',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('a8',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('a9',32),'hex'),'d0000000-0000-4000-8000-0000000000f6',
  'd0000000-0000-4000-8000-000000000003','d0000000-0000-4000-8000-000000000002',
  'd0000000-0000-4000-8000-000000000004','AUTO_UNIQUE','NEW_PROFILE_KEY',
  pg_catalog.decode(pg_catalog.repeat('aa',32),'hex'));

insert into public.weekly_source_final_revisions(
  id,source_cycle_id,authority_scope_kind,revision_number,upload_id,
  coverage_start_local_date,coverage_end_local_date,coverage_timezone,reason,
  finalised_by_user_id,manifest_hash,policy_fingerprint,state
) values
 ('d0000000-0000-4000-8000-0000000000fa','d0000000-0000-4000-8000-0000000000f3','CYCLE',1,
  'd0000000-0000-4000-8000-0000000000f4','2026-03-02','2026-03-08','Europe/London',
  'INITIAL_FINALISATION','d0000000-0000-4000-8000-000000000001',
  pg_catalog.decode(pg_catalog.repeat('c1',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('c2',32),'hex'),'CURRENT');

insert into public.weekly_source_row_timesheet_lineages(
  row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,contract_id,
  contract_week_id,timesheet_id,family_booking_id,timesheet_version,
  week_ending_date,lineage_fingerprint
) values
 ('d0000000-0000-4000-8000-0000000000f7','d0000000-0000-4000-8000-0000000000f3',
  'd0000000-0000-4000-8000-0000000000f6','d0000000-0000-4000-8000-000000000003',
  'd0000000-0000-4000-8000-000000000002','d0000000-0000-4000-8000-000000000004',
  'd0000000-0000-4000-8000-000000000005','d0000000-0000-4000-8000-000000000006',
  'WSREL-0001',1,'2026-03-08',
  pg_catalog.decode(pg_catalog.repeat('d1',32),'hex'));

-- Decision D8 (WP-01c): the authorisation record and the current-head pointer
-- are per ROOT in public.weekly_source_root_authorisations, keyed on the
-- physical root_timesheet_id, with at most one live generation per root.
--
-- WP-25.  HANDOVER 2 round 5, Part E "Rotation readings", made the row carry
-- five bindings plus the canonical digest of them, and WP-07c's W9 refuses a
-- row whose `decision_digest` is NULL by name
-- (`ROOT_AUTHORISATION_DECISION_DIGEST_MISSING`).  Those three columns are
-- IMMUTABLE FACTS - they are not among the four lifecycle columns
-- `15092026_1534_weekly_source_acl_contract_v1.sql` registers for this
-- relation - so a row seeded without them can never be repaired by an UPDATE.
-- The bindings are therefore computed HERE, by the installed helpers that
-- `public.weekly_source_first_authorise_v1` itself calls, never by a
-- hand-written copy of them.  Where those helpers are not installed the rows
-- keep their old shape and the omission is announced rather than silent.
do $seed_root_authorisations$
declare
  v_bound boolean:=
    pg_catalog.to_regprocedure('private.weekly_source_root_authorisation_signature_v1'
      ||'(uuid,uuid,text,integer,integer,text,text[])') is not null
    and pg_catalog.to_regprocedure(
      'private.weekly_source_root_agency_id_v1(uuid[])') is not null
    and pg_catalog.to_regprocedure(
      'private.weekly_source_root_protected_decision_hashes_v1(uuid[])') is not null
    and pg_catalog.to_regprocedure(
      'private.weekly_source_publication_request_digest_v1(jsonb)') is not null;
begin
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id,
    agency_id,protected_decision_hashes,decision_digest)
  select
    seed.root,seed.booking,1,1,seed.signature,
    'd0000000-0000-4000-8000-000000000001'::uuid,
    case when v_bound
         then private.weekly_source_root_agency_id_v1(array[seed.root]) end,
    case when v_bound
         then private.weekly_source_root_protected_decision_hashes_v1(array[seed.root]) end,
    case when v_bound then private.weekly_source_publication_request_digest_v1(
      private.weekly_source_root_authorisation_signature_v1(
        private.weekly_source_root_agency_id_v1(array[seed.root]),
        seed.root,seed.booking,1,1,seed.signature,
        private.weekly_source_root_protected_decision_hashes_v1(array[seed.root]))) end
  from (values
    ('d0000000-0000-4000-8000-000000000006'::uuid,'WSREL-0001'::text,
     'signature-rel-a-generation-1'::text),
    ('d0000000-0000-4000-8000-000000000016'::uuid,'WSREL-0002'::text,
     'signature-rel-b-generation-1'::text)
  ) as seed(root,booking,signature);
  if not v_bound then
    raise notice 'NOT BOUND: the WP-07c signature helpers are not installed, so the seeded authorisation rows carry no round-5 bindings';
  end if;
end
$seed_root_authorisations$;

-- ---------------------------------------------------------------------------
-- 4. Request builders (interface I-3) and the decision bundle
-- ---------------------------------------------------------------------------
create function pg_temp.component(
  p_ordinal integer,p_id uuid,p_hours text,p_pay text
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'component_ordinal',p_ordinal,'component_id',p_id,'component_kind','WORKED_TIME',
    'economic_key_type','SEGMENT','economic_key_value','seg-'||p_ordinal::text,
    'component_member_identity','mem-'||p_ordinal::text,
    'segment_id',null,'segment_key',null,'segment_stable_key',null,
    'work_date','2026-03-02','reference_number',null,
    'hours_day',p_hours,'hours_night',null,'hours_sat',null,'hours_sun',null,'hours_bh',null,
    'additional_code_raw',null,'unit_count',null,'unit_pay_rate',null,'unit_charge_rate',null,
    'expense_code',null,'pay_ex_vat',p_pay,'charge_ex_vat',null,
    'exclude_from_pay',false,'origin','WEEKLY_SOURCE',
    'movement_id',null,'movement_group_id',null);
$function$;

create function pg_temp.request(
  p_bundle uuid,p_revision bigint,p_head uuid,p_decision uuid,p_components jsonb,
  p_root uuid default 'd0000000-0000-4000-8000-000000000006',
  p_booking text default 'WSREL-0001',
  p_version integer default 1
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'decision_bundle_id',p_bundle,'pending_bundle_id',null,'bundle_revision',p_revision,
    'candidate_id','d0000000-0000-4000-8000-000000000003',
    'member_root_ids',pg_catalog.jsonb_build_array(p_root),
    'member_family_booking_ids',pg_catalog.jsonb_build_array(p_booking),
    'member_root_versions',pg_catalog.jsonb_build_array(p_version),
    'head_ids',pg_catalog.jsonb_build_array(p_head),
    'decision_id',p_decision,'publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id','d0000000-0000-4000-8000-0000000000fa',
        'source_cycle_id','d0000000-0000-4000-8000-0000000000f3',
        'revision_number',1,
        'manifest_hash',pg_catalog.repeat('c1',32),
        'policy_fingerprint',pg_catalog.repeat('c2',32)),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id','d0000000-0000-4000-8000-000000000004',
          'week_ending_date','2026-03-08','selection_method','UNCHANGED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',pg_catalog.jsonb_array_length(p_components)=0,
          'component_count',pg_catalog.jsonb_array_length(p_components),
          'components',p_components))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','SINGLE_ROOT','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(null),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,'component_ids','[]'::jsonb,
          'inventory_digest',pg_catalog.repeat('00',32))),
      'moved_component_ids',pg_catalog.jsonb_build_array(),
      'target_root_authorisation',null,'whole_root_office_review',null));
$function$;

-- The bounded A/B request of 24 section 4.5: member 1 is A (WSREL-0001,
-- Contract …004) and member 2 is B (WSREL-0002, Contract …014).  Nothing moves
-- between them here, because R6 never reaches the coordinator: the census
-- freezes the bundle first.
create function pg_temp.request_ab(
  p_bundle uuid,p_head_a uuid,p_head_b uuid,p_decision uuid
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'decision_bundle_id',p_bundle,'pending_bundle_id',null,'bundle_revision',1,
    'candidate_id','d0000000-0000-4000-8000-000000000003',
    'member_root_ids',pg_catalog.jsonb_build_array(
      'd0000000-0000-4000-8000-000000000006','d0000000-0000-4000-8000-000000000016'),
    'member_family_booking_ids',pg_catalog.jsonb_build_array('WSREL-0001','WSREL-0002'),
    'member_root_versions',pg_catalog.jsonb_build_array(1,1),
    'head_ids',pg_catalog.jsonb_build_array(p_head_a,p_head_b),
    'decision_id',p_decision,'publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id','d0000000-0000-4000-8000-0000000000fa',
        'source_cycle_id','d0000000-0000-4000-8000-0000000000f3','revision_number',1,
        'manifest_hash',pg_catalog.repeat('c1',32),
        'policy_fingerprint',pg_catalog.repeat('c2',32)),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id','d0000000-0000-4000-8000-000000000004',
          'week_ending_date','2026-03-08','selection_method','UNCHANGED'),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'contract_id','d0000000-0000-4000-8000-000000000014',
          'week_ending_date','2026-03-08','selection_method','OFFICE_SELECTED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',true,'component_count',0,'components','[]'::jsonb),
        pg_catalog.jsonb_build_object('root_ordinal',2,'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',false,'component_count',1,
          'components',pg_catalog.jsonb_build_array(
            pg_temp.component(1,'c3c3c3c3-0000-4000-8000-000000000001','7.5','75.00'))))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','CROSS_CONTRACT_A_B','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(null,null),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'component_ids',pg_catalog.jsonb_build_array(
            'c3c3c3c3-0000-4000-8000-000000000001'),
          'inventory_digest',pg_catalog.repeat('00',32)),
        pg_catalog.jsonb_build_object('root_ordinal',2,'component_ids','[]'::jsonb,
          'inventory_digest',pg_catalog.repeat('00',32))),
      'moved_component_ids',pg_catalog.jsonb_build_array(
        'c3c3c3c3-0000-4000-8000-000000000001'),
      'target_root_authorisation',null,'whole_root_office_review',null));
$function$;

create function pg_temp.lock_result_ab() returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object('ok',true,'gate','GRANTED',
    'families',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'requested_timesheet_id','d0000000-0000-4000-8000-000000000006',
        'family_booking_id','WSREL-0001',
        'canonical_timesheet_id','d0000000-0000-4000-8000-000000000006',
        'canonical_version',1,'requested_is_canonical',true,'family_is_current',true,
        'member_timesheet_ids',pg_catalog.jsonb_build_array(
          'd0000000-0000-4000-8000-000000000006')),
      pg_catalog.jsonb_build_object(
        'requested_timesheet_id','d0000000-0000-4000-8000-000000000016',
        'family_booking_id','WSREL-0002',
        'canonical_timesheet_id','d0000000-0000-4000-8000-000000000016',
        'canonical_version',1,'requested_is_canonical',true,'family_is_current',true,
        'member_timesheet_ids',pg_catalog.jsonb_build_array(
          'd0000000-0000-4000-8000-000000000016'))));
$function$;

create function pg_temp.mk_bundle_ab(
  p_bundle uuid,p_heads uuid[],p_decision uuid,p_request jsonb
) returns void language plpgsql as $function$
declare
  v_canonical jsonb:=private.weekly_source_publication_request_canonical_v1(
    p_request,'IMMEDIATE',null::uuid);
begin
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    target_root_family_booking_id,target_root_timesheet_id,target_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
    contract_choice_digest,before_inventory_digest,proposed_head_ids,state
  ) values (
    p_bundle,1,'d0000000-0000-4000-8000-0000000000aa',
    'd0000000-0000-4000-8000-000000000003','2026-03-08','CROSS_CONTRACT_A_B',
    'WSREL-0001','d0000000-0000-4000-8000-000000000006','d0000000-0000-4000-8000-000000000004',
    'WSREL-0002','d0000000-0000-4000-8000-000000000016','d0000000-0000-4000-8000-000000000014',
    p_decision,'d0000000-0000-4000-8000-000000000001','DEFERRED',
    private.weekly_source_publication_request_digest_v1(v_canonical),
    private.weekly_source_publication_request_digest_v1(
      v_canonical->'financial_request'->'source_revision'),
    private.weekly_source_publication_request_digest_v1(
      v_canonical->'financial_request'->'contract_choices'),
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_before_inventory_v1(
        coalesce(p_request->'control','{}'::jsonb),2)),
    p_heads,'PROPOSED');
end;
$function$;

create function pg_temp.lock_result(
  p_root uuid default 'd0000000-0000-4000-8000-000000000006',
  p_booking text default 'WSREL-0001',
  p_version integer default 1
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object('ok',true,'gate','GRANTED',
    'families',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'requested_timesheet_id',p_root,'family_booking_id',p_booking,
        'canonical_timesheet_id',p_root,'canonical_version',p_version,
        'requested_is_canonical',true,'family_is_current',true,
        'member_timesheet_ids',pg_catalog.jsonb_build_array(p_root))));
$function$;

-- The accepted decision carries the four approval digests the coordinator
-- recomputes: the acceptance digest is the canonical request in IMMEDIATE mode
-- with no pending bundle, and the other three are mode-independent.  The
-- fixture derives them from the request with the one canonical encoder, exactly
-- as the proposal composer will.
create function pg_temp.mk_bundle(
  p_bundle uuid,p_revision bigint,p_heads uuid[],p_decision uuid,p_tag text,
  p_request jsonb,
  p_root uuid default 'd0000000-0000-4000-8000-000000000006',
  p_booking text default 'WSREL-0001'
) returns void language plpgsql as $function$
declare
  v_canonical jsonb:=private.weekly_source_publication_request_canonical_v1(
    p_request,'IMMEDIATE',null::uuid);
begin
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
    contract_choice_digest,before_inventory_digest,proposed_head_ids,state
  ) values (
    p_bundle,p_revision,'d0000000-0000-4000-8000-0000000000aa',
    'd0000000-0000-4000-8000-000000000003','2026-03-08','SINGLE_ROOT',
    p_booking,p_root,'d0000000-0000-4000-8000-000000000004',
    p_decision,'d0000000-0000-4000-8000-000000000001','DEFERRED',
    private.weekly_source_publication_request_digest_v1(v_canonical),
    private.weekly_source_publication_request_digest_v1(
      v_canonical->'financial_request'->'source_revision'),
    private.weekly_source_publication_request_digest_v1(
      v_canonical->'financial_request'->'contract_choices'),
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_before_inventory_v1(
        coalesce(p_request->'control','{}'::jsonb),pg_catalog.cardinality(p_heads))),
    p_heads,'PROPOSED');
end;
$function$;

-- A frozen root: one live DRAFT batch item on WSREL-0001 (census class ACTIVE,
-- predicate C1).  Nothing here is ever changed by the release owner, and the
-- final section proves it byte for byte.
insert into public.pay_batches(
  id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot)
values ('d0000000-0000-4000-8000-00000000ba01','2026-03-18','DRAFT','MONZO_CSV','SAGE');
insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id,settlement_status)
values ('d0000000-0000-4000-8000-00000000ba02','d0000000-0000-4000-8000-00000000ba01',
        'd0000000-0000-4000-8000-000000000003',null);
insert into public.pay_batch_items(
  id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,amount_inc_vat)
values ('d0000000-0000-4000-8000-00000000ba03','d0000000-0000-4000-8000-00000000ba02',
        'TIMESHEET_PAY','d0000000-0000-4000-8000-000000000006','PAYE',false,100.00);

-- ---------------------------------------------------------------------------
-- 5. The pending bundle's stored request, structurally (WP-08b_NEEDS N1 closed)
-- ---------------------------------------------------------------------------
-- `proof/32 §8`: the deferred release "calls the SAME atomic head-publication
-- coordinator used for immediate publication ... It does not reimplement it",
-- and the `§2` apply signature receives no request from its caller, so the
-- request has to live on the bundle.  This section is the structural half of
-- that: the column exists, with the right type, NOT NULL, carrying its shape
-- CHECK, and it is an IDENTITY column rather than a lifecycle one.
--
-- It is written the way WP-01a writes structure: the complete set of normalised
-- CHECK definitions on the relation is compared against an expected list, so
-- DROPPING, WIDENING or ADDING any one of them fails here even when no
-- behavioural negative happens to exercise it.
do $verify_release_request_column$
declare
  v_actual text[];
  v_expected text[]:=array[
    'CHECK (((state <> ''PENDING''::text) OR (next_check_at_utc IS NOT NULL)))',
    'CHECK (((state <> ''RELEASING''::text) OR ((lease_owner IS NOT NULL) AND (lease_token IS NOT NULL) AND (lease_worker_run_id IS NOT NULL) AND (lease_expires_at_utc IS NOT NULL))))',
    'CHECK (((state = ''MANUAL_REVIEW''::text) = (manual_review_reason IS NOT NULL)))',
    'CHECK (((state = ''RELEASED''::text) = ((released_at_utc IS NOT NULL) AND (released_receipt_id IS NOT NULL) AND (released_receipt_digest IS NOT NULL) AND (released_by_worker_id IS NOT NULL) AND (released_by_worker_run_id IS NOT NULL))))',
    'CHECK (((last_census_json IS NULL) OR (jsonb_typeof(last_census_json) = ''object''::text)))',
    'CHECK ((jsonb_typeof(request_json) = ''object''::text))',
    'CHECK ((octet_length(contract_choice_digest) = 32))',
    'CHECK (((released_receipt_digest IS NULL) OR (octet_length(released_receipt_digest) = 32)))',
    'CHECK ((octet_length(request_digest) = 32))',
    'CHECK ((octet_length(source_revision_digest) = 32))',
    'CHECK ((pending_revision >= 1))',
    'CHECK ((technical_failure_count >= 0))',
    'CHECK ((array_position(member_family_booking_ids, NULL::text) IS NULL))',
    'CHECK ((array_position(member_root_ids, NULL::uuid) IS NULL))',
    'CHECK ((array_position(member_root_versions, NULL::integer) IS NULL))',
    'CHECK ((array_position(proposed_head_ids, NULL::uuid) IS NULL))',
    'CHECK ((cardinality(member_root_ids) = cardinality(member_family_booking_ids)))',
    'CHECK ((cardinality(member_root_ids) = cardinality(member_root_versions)))',
    'CHECK ((cardinality(member_root_ids) >= 1))',
    'CHECK ((cardinality(proposed_head_ids) >= 1))',
    'CHECK (private.weekly_source_uuid_array_is_distinct_v1(member_root_ids))',
    'CHECK (private.weekly_source_uuid_array_is_distinct_v1(proposed_head_ids))',
    'CHECK ((state = ANY (ARRAY[''PENDING''::text, ''RELEASING''::text, ''RELEASED''::text, ''SUPERSEDED''::text, ''MANUAL_REVIEW''::text])))'
  ];
  v_missing text[];
  v_extra text[];
begin
  -- The column itself: present, jsonb, NOT NULL, no default.  A default would
  -- let a writer create a bundle with an empty request that no release could
  -- ever use, so its absence is asserted rather than assumed.
  perform pg_temp.assert_true(
    (select attribute_row.atttypid=pg_catalog.to_regtype('jsonb')
        and attribute_row.attnotnull
        and not attribute_row.atthasdef
       from pg_catalog.pg_attribute attribute_row
      where attribute_row.attrelid
              ='public.weekly_source_pending_entitlement_bundles'::pg_catalog.regclass
        and attribute_row.attname='request_json'
        and attribute_row.attnum>0
        and not attribute_row.attisdropped),
    'proof/32 section 8: request_json must exist as jsonb, NOT NULL and with no default');

  -- It is an IDENTITY column: it must NOT appear among the lifecycle columns the
  -- ACL contract lets the release owner move.  If it ever did, the fact guard
  -- would stop refusing a rewrite of the money request.  Read from the INSTALLED
  -- guard trigger rather than from the contract function, because the trigger is
  -- what actually refuses, and from the contract function as well when it is
  -- present.  A build whose ACL repeatable predates the reclassification says so
  -- out loud instead of passing silently.
  if exists (
    select 1 from pg_catalog.pg_trigger trigger_row
     where trigger_row.tgrelid
             ='public.weekly_source_pending_entitlement_bundles'::pg_catalog.regclass
       and trigger_row.tgname='weekly_source_immutable_fact_guard'
       and not trigger_row.tgisinternal)
  then
    perform pg_temp.assert_true(
      (select pg_catalog.pg_get_triggerdef(trigger_row.oid) not like '%request_json%'
          and pg_catalog.pg_get_triggerdef(trigger_row.oid) like '%''state''%'
         from pg_catalog.pg_trigger trigger_row
        where trigger_row.tgrelid
                ='public.weekly_source_pending_entitlement_bundles'::pg_catalog.regclass
          and trigger_row.tgname='weekly_source_immutable_fact_guard'
          and not trigger_row.tgisinternal),
      'the installed fact guard must exempt the lifecycle columns and NOT request_json');
  else
    raise notice 'NOT CHECKED: the ACL fact guard is not installed on the pending bundle in this database, so request_json''s identity classification was checked structurally only';
  end if;
  if pg_catalog.to_regprocedure(
       'private._weekly_source_acl_lifecycle_column_contract_v1()') is not null then
    perform pg_temp.assert_true(
      not exists (
        select 1 from private._weekly_source_acl_lifecycle_column_contract_v1() lifecycle
         where lifecycle.table_name='weekly_source_pending_entitlement_bundles'
           and lifecycle.column_name='request_json'),
      'request_json must be an identity column, never a registered lifecycle column');
  end if;

  -- The complete normalised CHECK set, compared both ways.
  select pg_catalog.array_agg(definition order by definition) into v_actual
    from (
      select pg_catalog.pg_get_constraintdef(constraint_row.oid) as definition
        from pg_catalog.pg_constraint constraint_row
       where constraint_row.conrelid
               ='public.weekly_source_pending_entitlement_bundles'::pg_catalog.regclass
         and constraint_row.contype='c'
    ) as constraint_set;
  select pg_catalog.array_agg(missing.definition order by missing.definition) into v_missing
    from (select pg_catalog.unnest(v_expected) except select pg_catalog.unnest(v_actual))
         as missing(definition);
  select pg_catalog.array_agg(extra.definition order by extra.definition) into v_extra
    from (select pg_catalog.unnest(v_actual) except select pg_catalog.unnest(v_expected))
         as extra(definition);
  perform pg_temp.assert_true(
    v_missing is null and v_extra is null,
    'the pending bundle CHECK set changed. missing='||coalesce(v_missing::text,'{}')
      ||' extra='||coalesce(v_extra::text,'{}'));

  -- The complete index set, the same way: the money rules on this relation are
  -- INDEXES, never CHECKs, so dropping or narrowing one has to fail here.  The
  -- partial unique index is the one that makes "exactly one live bundle per
  -- decision bundle" true, which is what I-5's supersession rests on.
  select pg_catalog.array_agg(definition order by definition) into v_actual
    from (
      select pg_catalog.pg_get_indexdef(index_row.indexrelid) as definition
        from pg_catalog.pg_index index_row
       where index_row.indrelid
               ='public.weekly_source_pending_entitlement_bundles'::pg_catalog.regclass
    ) as index_set;
  v_expected:=array[
    'CREATE INDEX weekly_source_pending_entitlement_bundles_claim_idx ON public.weekly_source_pending_entitlement_bundles USING btree (state, next_check_at_utc, id)',
    'CREATE INDEX weekly_source_pending_entitlement_bundles_lease_idx ON public.weekly_source_pending_entitlement_bundles USING btree (state, lease_expires_at_utc, id)',
    'CREATE UNIQUE INDEX weekly_source_pending_entitle_decision_bundle_id_bundle_rev_key ON public.weekly_source_pending_entitlement_bundles USING btree (decision_bundle_id, bundle_revision)',
    'CREATE UNIQUE INDEX weekly_source_pending_entitlement_bundles_live_uq ON public.weekly_source_pending_entitlement_bundles USING btree (decision_bundle_id) WHERE (state = ANY (ARRAY[''PENDING''::text, ''RELEASING''::text]))',
    'CREATE UNIQUE INDEX weekly_source_pending_entitlement_bundles_pkey ON public.weekly_source_pending_entitlement_bundles USING btree (id)',
    'CREATE UNIQUE INDEX weekly_source_pending_entitlement_bundles_request_digest_key ON public.weekly_source_pending_entitlement_bundles USING btree (request_digest)'
  ];
  select pg_catalog.array_agg(missing.definition order by missing.definition) into v_missing
    from (select pg_catalog.unnest(v_expected) except select pg_catalog.unnest(v_actual))
         as missing(definition);
  select pg_catalog.array_agg(extra.definition order by extra.definition) into v_extra
    from (select pg_catalog.unnest(v_actual) except select pg_catalog.unnest(v_expected))
         as extra(definition);
  perform pg_temp.assert_true(
    v_missing is null and v_extra is null,
    'the pending bundle INDEX set changed. missing='||coalesce(v_missing::text,'{}')
      ||' extra='||coalesce(v_extra::text,'{}'));

  -- Behavioural negatives for the new column, so the structural list is not the
  -- only thing standing between a widened schema and a bad row.
  perform pg_temp.expect_failure(
    $sql$insert into public.weekly_source_pending_entitlement_bundles(
      decision_bundle_id,bundle_revision,candidate_id,member_root_ids,
      member_family_booking_ids,member_root_versions,request_digest,
      source_revision_digest,contract_choice_digest,decision_id,decided_by_user_id,
      proposed_head_ids,request_json,pending_revision,state,next_check_at_utc
    ) values (
      'd0000000-0000-4000-8000-0000000000b1',1,'d0000000-0000-4000-8000-000000000003',
      array['d0000000-0000-4000-8000-000000000006']::uuid[],array['WSREL-0001']::text[],
      array[1]::integer[],pg_catalog.decode(pg_catalog.repeat('f1',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('f2',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('f3',32),'hex'),
      'd0000000-0000-4000-8000-0000000000d1','d0000000-0000-4000-8000-000000000001',
      array['d0000000-0000-4000-8000-0000000000c1']::uuid[],
      null::jsonb,1,'PENDING',pg_catalog.clock_timestamp());$sql$,
    '23502','','a pending bundle with no stored request is refused NOT NULL');
  perform pg_temp.expect_failure(
    $sql$insert into public.weekly_source_pending_entitlement_bundles(
      decision_bundle_id,bundle_revision,candidate_id,member_root_ids,
      member_family_booking_ids,member_root_versions,request_digest,
      source_revision_digest,contract_choice_digest,decision_id,decided_by_user_id,
      proposed_head_ids,request_json,pending_revision,state,next_check_at_utc
    ) values (
      'd0000000-0000-4000-8000-0000000000b1',1,'d0000000-0000-4000-8000-000000000003',
      array['d0000000-0000-4000-8000-000000000006']::uuid[],array['WSREL-0001']::text[],
      array[1]::integer[],pg_catalog.decode(pg_catalog.repeat('f4',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('f5',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('f6',32),'hex'),
      'd0000000-0000-4000-8000-0000000000d1','d0000000-0000-4000-8000-000000000001',
      array['d0000000-0000-4000-8000-0000000000c1']::uuid[],
      '[]'::jsonb,1,'PENDING',pg_catalog.clock_timestamp());$sql$,
    '23514','','a request stored as a JSON array rather than an object is refused');
end
$verify_release_request_column$;

-- ---------------------------------------------------------------------------
-- 6. Interface I-5 - the save
-- ---------------------------------------------------------------------------
do $verify_release_save$
declare
  v_request jsonb;
  v_result jsonb;
  v_again jsonb;
  v_row record;
  v_signal_count bigint;
begin
  v_request:=pg_temp.request(
    'd0000000-0000-4000-8000-0000000000b1',1,
    'd0000000-0000-4000-8000-0000000000c1','d0000000-0000-4000-8000-0000000000d1',
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00')));
  perform pg_temp.mk_bundle(
    'd0000000-0000-4000-8000-0000000000b1',1,
    array['d0000000-0000-4000-8000-0000000000c1']::uuid[],
    'd0000000-0000-4000-8000-0000000000d1','b1',v_request);

  -- A RELEASABLE or CENSUS_ERROR census is never parked as a pending decision.
  v_result:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request,pg_temp.lock_result(),
    pg_catalog.jsonb_build_object('result','RELEASABLE','items','[]'::jsonb));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PENDING_BUNDLE_CENSUS_NOT_FROZEN',
    'a RELEASABLE census must not be saved as pending: '||v_result::text);
  v_result:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request,pg_temp.lock_result(),
    pg_catalog.jsonb_build_object('result','CENSUS_ERROR','items','[]'::jsonb));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PENDING_BUNDLE_CENSUS_NOT_FROZEN',
    'a CENSUS_ERROR census must not be saved as pending: '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_pending_entitlement_bundles)=0,
    'neither refusal wrote a row');

  -- The real save, with the real census over the real frozen root.
  v_result:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request,pg_temp.lock_result(),
    private.weekly_source_freeze_census_v1(
      'd0000000-0000-4000-8000-000000000003',
      array['d0000000-0000-4000-8000-000000000006']::uuid[]));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean and (v_result->>'created')::boolean
    and v_result->>'state'='PENDING',
    'the save owner must create exactly one PENDING bundle: '||v_result::text);

  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where id=(v_result->>'pending_bundle_id')::uuid;
  perform pg_temp.assert_true(
    v_row.decided_by_user_id='d0000000-0000-4000-8000-000000000001',
    'proof/32 section 2: the actor is copied from the immutable accepted decision');
  perform pg_temp.assert_true(
    v_row.member_root_ids=array['d0000000-0000-4000-8000-000000000006']::uuid[]
    and v_row.member_family_booking_ids=array['WSREL-0001']::text[]
    and v_row.member_root_versions=array[1]::integer[]
    and v_row.proposed_head_ids=array['d0000000-0000-4000-8000-0000000000c1']::uuid[],
    'the stored member identity is the request''s, exactly');
  perform pg_temp.assert_true(
    v_row.technical_failure_count=0 and v_row.next_check_at_utc is not null
    and v_row.lease_owner is null and v_row.lease_token is null,
    'a new PENDING bundle carries no lease and a due time');
  -- The digest is the DEFERRED digest of this bundle's own id (I-3 section 0).
  perform pg_temp.assert_true(
    v_row.request_digest=private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(v_request,'DEFERRED',v_row.id)),
    'the stored digest is the DEFERRED digest of this pending bundle');
  perform pg_temp.assert_true(
    v_row.request_digest<>private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(v_request,'IMMEDIATE',null)),
    'the deferred digest differs from the immediate digest of the same decision');

  -- 24 section 4.4: the previous effective entitlement remains current and no
  -- head is written.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=0,
    '24 section 4.4: a frozen decision publishes no head');

  -- The one bounded, informational stale warning.
  select pg_catalog.count(*) into v_signal_count
    from public.banking_pay_batch_change_signals as signal_row
   where signal_row.pay_batch_id='d0000000-0000-4000-8000-00000000ba01';
  perform pg_temp.assert_true(
    v_signal_count=1 and (v_result->'stale_warning'->>'signalled_batch_count')::integer=1,
    '24 section 4.4: exactly one bounded stale warning to the affected active Draft');
  perform pg_temp.assert_true(
    (select batch_row.status='DRAFT' and batch_row.cancelled_at_utc is null
       from public.pay_batches batch_row
      where batch_row.id='d0000000-0000-4000-8000-00000000ba01')
    and (select item_row.is_voided is false and item_row.amount_inc_vat=100.00
           from public.pay_batch_items item_row
          where item_row.id='d0000000-0000-4000-8000-00000000ba03'),
    '24 section 4.4: the signal alters no frozen item, amount, reservation or payment state');

  -- Idempotent on the request digest.
  v_again:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request,pg_temp.lock_result(),
    private.weekly_source_freeze_census_v1(
      'd0000000-0000-4000-8000-000000000003',
      array['d0000000-0000-4000-8000-000000000006']::uuid[]));
  perform pg_temp.assert_true(
    (v_again->>'ok')::boolean and (v_again->>'created')::boolean is false
    and (v_again->>'replayed')::boolean
    and v_again->>'pending_bundle_id'=v_result->>'pending_bundle_id',
    'a second save of the same request returns the same bundle and creates nothing: '
      ||v_again::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_pending_entitlement_bundles)=1,
    'still exactly one pending bundle');

  -- A DIFFERENT request under the same decision bundle revision is refused and
  -- never overwrites the saved decision.  The four approval digests of the
  -- accepted decision catch it first, which is the earliest and strictest
  -- place it can be caught: any change inside the digest scope moves the
  -- acceptance digest.  (`WEEKLY_SOURCE_PENDING_BUNDLE_CONFLICT` remains as the
  -- backstop for a stored row whose digest no longer matches its own request.)
  v_again:=private.weekly_source_pending_entitlement_bundle_save_v1(
    pg_temp.request('d0000000-0000-4000-8000-0000000000b1',1,
      'd0000000-0000-4000-8000-0000000000c1','d0000000-0000-4000-8000-0000000000d1',
      pg_catalog.jsonb_build_array(
        pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','9.5','95.00'))),
    pg_temp.lock_result(),
    private.weekly_source_freeze_census_v1(
      'd0000000-0000-4000-8000-000000000003',
      array['d0000000-0000-4000-8000-000000000006']::uuid[]));
  perform pg_temp.assert_true(
    (v_again->>'ok')::boolean is false
    and v_again->>'code'='WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
    and v_again->'detail'->>'reason'='APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION'
    and (v_again->'detail'->>'request_digest_matches')::boolean is false,
    'a different request under the same revision is refused, never an overwrite: '
      ||v_again::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_pending_entitlement_bundles)=1
    and (select request_digest=v_row.request_digest
           from public.weekly_source_pending_entitlement_bundles where id=v_row.id),
    'the saved decision is untouched by the refused request');

  -- A change that lives only in the CONTROL scope moves no digest, so it is an
  -- exact replay of the same money (I-3 section 0, consequence 2).
  v_again:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request||pg_catalog.jsonb_build_object('control',
      (v_request->'control')||pg_catalog.jsonb_build_object('reason','SOMETHING_ELSE')),
    pg_temp.lock_result(),
    private.weekly_source_freeze_census_v1(
      'd0000000-0000-4000-8000-000000000003',
      array['d0000000-0000-4000-8000-000000000006']::uuid[]));
  perform pg_temp.assert_true(
    (v_again->>'ok')::boolean and (v_again->>'replayed')::boolean
    and v_again->>'pending_bundle_id'=v_result->>'pending_bundle_id',
    'a control-scope-only change is an exact replay of the same decision: '||v_again::text);
end
$verify_release_save$;

-- ---------------------------------------------------------------------------
-- 7. G5-4 - the claim page, its clamps and the expired-lease reclaim (R13, R27)
-- ---------------------------------------------------------------------------
do $verify_release_claim$
declare
  v_claim jsonb;
  v_pending uuid;
  v_row record;
  v_first_token uuid;
begin
  -- Named explicitly: more than one pending bundle exists by now, and an
  -- unordered `limit 1` would be a rule expressed through row order.
  select id into v_pending from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b1';

  -- Not yet due: the claim returns nothing.
  v_claim:=private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',60,10);
  perform pg_temp.assert_true(
    (v_claim->>'claimed_count')::integer=0,
    'a bundle whose next_check_at_utc is in the future is not claimed');

  -- R27: caller-supplied page 500 and lease 3600 are clamped INSIDE the
  -- function to 25 and 120; page 0 and lease 1 are clamped up to 1 and 30.
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second';
  v_claim:=private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',3600,500);
  perform pg_temp.assert_true(
    (v_claim->>'limit')::integer=25 and (v_claim->>'lease_seconds')::integer=120,
    'R27: page 500 and lease 3600 are clamped to 25 and 120 inside the function: '
      ||v_claim::text);
  perform pg_temp.assert_true(
    (v_claim->>'claimed_count')::integer=1,
    'the due bundle is claimed exactly once');
  v_first_token:=(v_claim->'bundles'->0->>'lease_token')::uuid;

  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    v_row.state='RELEASING' and v_row.lease_owner='weekly-source-release-worker'
    and v_row.lease_token=v_first_token
    and v_row.lease_worker_run_id='d0000000-0000-4000-8000-00000000cc01'
    and v_row.lease_expires_at_utc>pg_catalog.clock_timestamp(),
    'the claim leases the bundle to this worker and this Worker run');

  -- A second tick cannot claim it while the lease is live.
  v_claim:=private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker-2','d0000000-0000-4000-8000-00000000cc02',60,10);
  perform pg_temp.assert_true(
    (v_claim->>'claimed_count')::integer=0,
    'a live lease is not stolen by another worker');

  -- R13: an EXPIRED RELEASING lease is reclaimed by the next tick, with a new
  -- token and a new pending_revision, so the old worker can no longer apply.
  update public.weekly_source_pending_entitlement_bundles
     set lease_expires_at_utc=pg_catalog.clock_timestamp()-interval '1 second'
   where id=v_pending;
  v_claim:=private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker-2','d0000000-0000-4000-8000-00000000cc02',30,1);
  perform pg_temp.assert_true(
    (v_claim->>'claimed_count')::integer=1
    and (v_claim->'bundles'->0->>'lease_token')::uuid<>v_first_token
    and (v_claim->>'lease_seconds')::integer=30 and (v_claim->>'limit')::integer=1,
    'R13: an expired lease is reclaimed with a fresh token, and 30/1 are legal: '
      ||v_claim::text);

  -- R27 second half: the browser roles cannot reach any of it.
  perform pg_temp.assert_true(
    not pg_catalog.has_function_privilege('anon',
      pg_catalog.to_regprocedure('public.weekly_source_pending_entitlement_release_apply_v1(jsonb)'),'EXECUTE')
    and not pg_catalog.has_function_privilege('authenticated',
      pg_catalog.to_regprocedure('public.weekly_source_pending_entitlement_release_claim_page_v1(jsonb)'),'EXECUTE'),
    'R27: no browser role may execute a release owner');
  perform pg_temp.assert_true(
    not pg_catalog.has_table_privilege('anon',
      'public.weekly_source_pending_entitlement_bundles','SELECT')
    and not pg_catalog.has_table_privilege('authenticated',
      'public.weekly_source_pending_entitlement_bundles','SELECT'),
    'R27: no browser role may read the pending bundle relation directly');
end
$verify_release_claim$;

-- ---------------------------------------------------------------------------
-- 8. G5-5 - the lease checks (R32) and the FROZEN result (R3, R5)
-- ---------------------------------------------------------------------------
do $verify_release_apply_frozen$
declare
  v_pending uuid;
  v_row record;
  v_result jsonb;
  v_claim jsonb;
  v_tick integer;
  v_next timestamptz;
begin
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b1';
  v_pending:=v_row.id;

  -- R32: the right worker name and the right lease token, but the WRONG
  -- p_worker_run_id, is refused and writes nothing.
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,'d0000000-0000-4000-8000-00000000ccff');
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_RELEASE_LEASE_INVALID',
    'R32: a wrong Worker run id is refused WEEKLY_SOURCE_RELEASE_LEASE_INVALID: '
      ||v_result::text);
  perform pg_temp.assert_true(
    (select state='RELEASING' and technical_failure_count=0
       from public.weekly_source_pending_entitlement_bundles where id=v_pending),
    'R32: the refusal wrote nothing');

  -- A stale pending_revision is refused the same way.
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision-1,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_RELEASE_LEASE_INVALID',
    'a stale pending_revision is refused');

  -- R42, the BLOCKED half, at release level.  Inserting the Draft item above
  -- fired the installed Workbench dirty trigger, so a WORKBENCH_CANDIDATE_DIRTY_APPLY
  -- job is QUEUED for this Candidate and the installed serial gate reports
  -- BLOCKED.  proof/32 section 6 step 1: that is a RETRYABLE refusal with no
  -- write - skip this bundle for this tick and retry next tick.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_workbench_jobs
      where status in ('QUEUED','RUNNING'))>=1,
    'the installed dirty trigger queued a Workbench job, so the gate can be exercised');
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_CANDIDATE_BUSY'
    and (v_result->>'retryable')::boolean
    and v_result->>'outcome'='SKIPPED_THIS_TICK',
    'R42: a BLOCKED serial gate is a retryable refusal, not a failure: '||v_result::text);
  perform pg_temp.assert_true(
    (select state='PENDING' and technical_failure_count=0 and lease_token is null
       from public.weekly_source_pending_entitlement_bundles where id=v_pending),
    'R42: BLOCKED consumes no technical-failure budget and leaves the bundle reclaimable');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=0,
    'R42: a BLOCKED gate writes nothing');

  -- TEST SCAFFOLDING: retire the queued Candidate jobs so the gate can grant.
  -- This is the verifier arranging a state, not the release owner writing: the
  -- release owner never touches a Workbench job.
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');

  -- R3 and R5: the root is still frozen by a live Draft item.  FROZEN advances
  -- next_check_at_utc, keeps the old head current and NEVER touches the
  -- technical-failure counter - and no number of ticks ever releases it.
  -- WP-08c, HANDOVER 2 round-5 ruling B4.2, adopting the new truth (Part 1
  -- review rule 6).  Tick 1 is still the full claim/apply cycle and still ends
  -- FROZEN.  It now also leaves a WATCH signature, and from tick 2 onward the
  -- bounded watch inside the claim page re-proves the freeze WITHOUT claiming
  -- the bundle, so the apply owner is not called at all and the bundle performs
  -- no state transition.  The assertions below therefore check the same three
  -- properties - FROZEN, counter untouched, nothing released - through whichever
  -- path the tick actually took, and additionally prove that ticks 2 to 4 moved
  -- neither `state` nor `pending_revision`.
  for v_tick in 1..4 loop
    select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
    update public.weekly_source_pending_entitlement_bundles
       set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second'
     where id=v_pending;
    select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
    v_claim:=private.weekly_source_pending_entitlement_release_claim_page_v1(
      'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',60,25);
    if v_tick=1 then
      perform pg_temp.assert_true(
        (v_claim->>'claimed_count')::integer=1
        and (v_claim->'watch'->>'polled')::integer=0,
        'tick 1: the first attempt is a full claim, with nothing to watch yet: '
          ||v_claim::text);
      select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
      v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
        v_pending,v_row.pending_revision,v_row.request_digest,
        v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
      perform pg_temp.assert_true(
        (v_result->>'ok')::boolean and (v_result->>'released')::boolean is false
        and v_result->>'outcome'='FROZEN' and v_result->>'census_result'='FROZEN',
        'tick '||v_tick::text||': a frozen root must return FROZEN and release nothing: '
          ||(v_result-'census')::text);
    else
      perform pg_temp.assert_true(
        (v_claim->>'claimed_count')::integer=0
        and (v_claim->'watch'->>'polled')::integer=1
        and (v_claim->'watch'->>'unchanged_frozen')::integer=1
        and (v_claim->'watch'->>'escalated_to_claim')::integer=0,
        'tick '||v_tick::text||': B4.2 - an unchanged frozen bundle is watched, '
          ||'never re-claimed: '||v_claim::text);
      perform pg_temp.assert_true(
        (select pending_row.state='PENDING'
                and pending_row.pending_revision=v_row.pending_revision
           from public.weekly_source_pending_entitlement_bundles pending_row
          where pending_row.id=v_pending),
        'tick '||v_tick::text||': B4.2 - the watch performs no state transition');
    end if;
    select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
    perform pg_temp.assert_true(
      v_row.state='PENDING' and v_row.technical_failure_count=0
      and v_row.lease_token is null
      and v_row.next_check_at_utc>pg_catalog.clock_timestamp(),
      'tick '||v_tick::text||': FROZEN returns the bundle to PENDING with the counter '
        ||'untouched and a later due time');
    perform pg_temp.assert_true(
      v_row.last_census_json->>'result'='FROZEN'
      and (v_row.last_census_json->'watch'->>'observation_count')::bigint=v_tick,
      'tick '||v_tick::text||': the census that was run is recorded on the bundle, '
        ||'with the bounded observation counter at '||v_tick::text);
  end loop;

  -- R5: nothing was released by elapsed time, by attempt count or by inference.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=0
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts)=0,
    'R5: four ticks against a frozen root published no head and wrote no receipt');
end
$verify_release_apply_frozen$;

-- ---------------------------------------------------------------------------
-- 8b. R6 - a bounded A/B bundle, one root safe and one frozen
-- ---------------------------------------------------------------------------
-- proof/32 section 5.3: "Mixed A/B result (one root safe, the other frozen) ->
-- the whole bundle stays PENDING; neither root publishes."  WSREL-0002 carries
-- no Banking Pay evidence at all and is releasable on its own; WSREL-0001 is
-- still frozen by the live Draft item.  The census is run over EVERY physical
-- member of BOTH families at once, which is what makes the bundle indivisible.
do $verify_release_mixed_bundle$
declare
  v_request jsonb;
  v_saved jsonb;
  v_apply jsonb;
  v_row record;
  v_census_b jsonb;
  v_census_both jsonb;
  v_heads_before bigint;
begin
  -- Each member's own census, so "one safe, one frozen" is a measured fact.
  v_census_b:=private.weekly_source_freeze_census_v1(
    'd0000000-0000-4000-8000-000000000003',
    array['d0000000-0000-4000-8000-000000000016']::uuid[]);
  perform pg_temp.assert_true(v_census_b->>'result'='RELEASABLE',
    'R6: the B root alone must be releasable, got '||coalesce(v_census_b->>'result','<null>'));
  v_census_both:=private.weekly_source_freeze_census_v1(
    'd0000000-0000-4000-8000-000000000003',
    array['d0000000-0000-4000-8000-000000000006',
          'd0000000-0000-4000-8000-000000000016']::uuid[]);
  perform pg_temp.assert_true(v_census_both->>'result'='FROZEN',
    'R6: the two roots together must be FROZEN, got '
      ||coalesce(v_census_both->>'result','<null>'));

  v_request:=pg_temp.request_ab(
    'd0000000-0000-4000-8000-0000000000b6','d0000000-0000-4000-8000-0000000000c6',
    'd0000000-0000-4000-8000-0000000000c7','d0000000-0000-4000-8000-0000000000d6');
  perform pg_temp.mk_bundle_ab(
    'd0000000-0000-4000-8000-0000000000b6',
    array['d0000000-0000-4000-8000-0000000000c6',
          'd0000000-0000-4000-8000-0000000000c7']::uuid[],
    'd0000000-0000-4000-8000-0000000000d6',v_request);
  v_saved:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request,pg_temp.lock_result_ab(),v_census_both);
  perform pg_temp.assert_true(
    (v_saved->>'ok')::boolean and (v_saved->>'created')::boolean,
    'R6: the mixed bundle is saved as one PENDING bundle: '||v_saved::text);

  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second'
   where id=(v_saved->>'pending_bundle_id')::uuid;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc06',120,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where id=(v_saved->>'pending_bundle_id')::uuid;
  v_apply:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_row.id,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    (v_apply->>'released')::boolean is false and v_apply->>'outcome'='FROZEN',
    'R6: a mixed bundle stays PENDING and neither root publishes: '
      ||(v_apply-'census')::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'R6: neither root published a head');
  perform pg_temp.assert_true(
    (select state='PENDING' and technical_failure_count=0
       from public.weekly_source_pending_entitlement_bundles where id=v_row.id),
    'R6: the mixed bundle is retried, not failed');
  -- It is one bundle, not two: both roots and both heads are stored together.
  perform pg_temp.assert_true(
    (select pg_catalog.cardinality(member_root_ids)=2
       and pg_catalog.cardinality(proposed_head_ids)=2
       from public.weekly_source_pending_entitlement_bundles where id=v_row.id),
    'R6: the pending bundle carries both members and both proposed heads');
end
$verify_release_mixed_bundle$;

-- ---------------------------------------------------------------------------
-- 9. Round-4 ruling 3 - VOID_NOT_YET_PROVED is FROZEN and costs no budget,
--    and the transition to CENSUS_ERROR does cost one
-- ---------------------------------------------------------------------------
-- A voided item in a CANCELLED terminal batch that no binding proves.  A live
-- C4 correction operation for the batch makes it
-- `ACTIVE / WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED`; once the operation is
-- terminal the same item is `CENSUS_ERROR / WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE`.
do $verify_release_ruling3$
declare
  v_pending uuid;
  v_row record;
  v_result jsonb;
  v_claim jsonb;
  v_census jsonb;
  v_before integer;
begin
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b1';
  v_pending:=v_row.id;

  -- Retire the DRAFT freeze so the new evidence is the only thing left.
  update public.pay_batches set status='CANCELLED',cancelled_at_utc=pg_catalog.clock_timestamp()
   where id='d0000000-0000-4000-8000-00000000ba01';
  update public.pay_batch_items set is_voided=true
   where id='d0000000-0000-4000-8000-00000000ba03';
  -- A non-terminal correction request keeps Binding B from proving the void,
  -- and its status is not APPLIED, so Binding A has no core either.
  insert into public.pay_payment_correction_requests(
    id,pay_batch_id,correction_kind,status,required_quantity,approved_count,
    selection_json,selection_hash,plan_json,plan_hash,requested_by_user_id
  ) values (
    'd0000000-0000-4000-8000-0000000cc101','d0000000-0000-4000-8000-00000000ba01',
    'PRE_BANK_CANCEL','PLANNED',1,0,'{}'::jsonb,pg_catalog.repeat('a1',32),'{}'::jsonb,pg_catalog.repeat('a2',32),
    'd0000000-0000-4000-8000-000000000001');
  -- (a) a LIVE C4 correction operation for the batch.
  insert into public.banking_pay_operations(
    id,operation_type,status,phase,idempotency_key,pay_batch_id
  ) values (
    'd0000000-0000-4000-8000-0000000e0101','PAYMENT_CORRECTION','RUNNING','PROCESS_CHUNKS',
    'ws-release-proof-op-1','d0000000-0000-4000-8000-00000000ba01');

  v_census:=private.weekly_source_freeze_census_v1(
    'd0000000-0000-4000-8000-000000000003',
    array['d0000000-0000-4000-8000-000000000006']::uuid[]);
  perform pg_temp.assert_true(
    v_census->>'result'='FROZEN'
    and exists(select 1 from pg_catalog.jsonb_array_elements(v_census->'items') as item(value)
                where item.value->>'class'='ACTIVE'
                  and item.value->>'reason'='WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED'),
    'ruling 3: a void with a live C4 operation is ACTIVE/VOID_NOT_YET_PROVED and FROZEN: '
      ||(v_census-'proof'-'predicates')::text);

  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_before:=v_row.technical_failure_count;
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  -- WP-08c, HANDOVER 2 round-5 ruling B4.2, executed.  The bundle arrives here
  -- carrying the watch signature section 8 left on it, and the Banking Pay
  -- evidence has since CHANGED (a live C4 operation now makes the void
  -- unproved).  The watch must therefore notice, drop the marker and hand the
  -- bundle straight back to the claim in the same call - which is the whole
  -- reason the cheap poll is safe.
  v_claim:=private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',60,25);
  perform pg_temp.assert_true(
    (v_claim->'watch'->>'polled')::integer=1
    and (v_claim->'watch'->>'escalated_to_claim')::integer=1
    and (v_claim->'watch'->>'unchanged_frozen')::integer=0
    and (v_claim->>'claimed_count')::integer=1,
    'B4.2: a CHANGED census makes the watch escalate to a full attempt on the '
      ||'same tick: '||v_claim::text);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    v_result->>'outcome'='FROZEN' and (v_result->>'released')::boolean is false,
    'ruling 3: the bundle stays PENDING while the void is not yet proved');
  perform pg_temp.assert_true(
    (select technical_failure_count from public.weekly_source_pending_entitlement_bundles
      where id=v_pending)=v_before,
    'ruling 3: VOID_NOT_YET_PROVED is retried WITHOUT consuming the technical-failure budget');

  -- (b) the in-flight condition ends.  Both lease forms null, terminal status:
  -- the same item is now CENSUS_ERROR and the budget IS consumed.
  update public.banking_pay_operations
     set status='COMPLETE',phase='COMPLETE',runner_state='COMPLETE',
         lease_expires_at_utc=null,lock_expires_at_utc=null,
         completed_at_utc=pg_catalog.clock_timestamp()
   where id='d0000000-0000-4000-8000-0000000e0101';
  v_census:=private.weekly_source_freeze_census_v1(
    'd0000000-0000-4000-8000-000000000003',
    array['d0000000-0000-4000-8000-000000000006']::uuid[]);
  perform pg_temp.assert_true(
    v_census->>'result'='CENSUS_ERROR'
    and exists(select 1 from pg_catalog.jsonb_array_elements(v_census->'items') as item(value)
                where item.value->>'class'='CENSUS_ERROR'
                  and item.value->>'reason'='WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE'),
    'ruling 3: with no in-flight condition the same void is CENSUS_ERROR/VOID_UNBINDABLE: '
      ||(v_census-'proof'-'predicates')::text);

  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  -- WP-08c, HANDOVER 2 round-5 ruling B4.1, adopting the new truth (Part 1
  -- review rule 6).  This used to assert TECHNICAL_FAILURE with the counter at
  -- +1, because a census that cannot classify a Banking Pay item spent ten
  -- attempts and about ninety minutes of backoff before a human saw it.  B4.1
  -- reverses that: a CENSUS_ERROR is a fixed structural refusal, so it is now
  -- an IMMEDIATE Office manual review and it consumes NO budget at all.  What
  -- is unchanged, and still asserted, is that it releases nothing.
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and (v_result->>'released')::boolean is false
    and v_result->>'outcome'='MANUAL_REVIEW'
    and v_result->>'refusal_disposition'='PERMANENT'
    and v_result->>'code'='WEEKLY_SOURCE_CENSUS_ERROR',
    'B4.1: once nothing is in flight the census error goes STRAIGHT to Office '
      ||'manual review: '||(v_result-'detail')::text);
  perform pg_temp.assert_true(
    (select technical_failure_count from public.weekly_source_pending_entitlement_bundles
      where id=v_pending)=v_before,
    'B4.1: a permanent refusal spends none of the retry budget');
  perform pg_temp.assert_true(
    (v_result->'detail'->>'census_reason') is not null
    and pg_catalog.jsonb_array_length(v_result->'detail'->'census_error_items')>=1,
    'ruling 4: the failure detail names the census reason and the exact item ids');
  -- B4.4: the identifiers are in the child relation, not only in the reason.
  perform pg_temp.assert_true(
    (v_result->'review_items'->>'ok')::boolean
    and (v_result->'review_items'->>'rows_retained')::integer
        =(v_result->'review_items'->>'item_count')::integer
    and (v_result->'review_items'->>'item_count')::integer>=1,
    'B4.4: every census item identifier is retained in the child relation: '
      ||coalesce((v_result->'review_items')::text,'<null>'));
  perform pg_temp.assert_true(
    (select state='MANUAL_REVIEW' and manual_review_reason is not null
       from public.weekly_source_pending_entitlement_bundles where id=v_pending),
    'B4.1: the bundle is in MANUAL_REVIEW with an Office-visible reason');
end
$verify_release_ruling3$;

-- ---------------------------------------------------------------------------
-- 10. R14 - ten consecutive technical failures, MANUAL_REVIEW, audited reopen
-- ---------------------------------------------------------------------------
do $verify_release_manual_review$
declare
  v_pending uuid;
  v_row record;
  v_result jsonb;
  v_tick integer;
  v_audit_before bigint;
  v_audit_after bigint;
begin
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b1';
  v_pending:=v_row.id;

  -- WP-08c, HANDOVER 2 round-5 ruling B4.1, adopting the new truth (Part 1
  -- review rule 6).  Section 9 now leaves this bundle in MANUAL_REVIEW on its
  -- FIRST census error, so the ten-attempt budget can no longer be driven by a
  -- census error at all.  R14 is still a real requirement, so it is driven by
  -- the only thing that may now spend the budget: a rolled-back release
  -- transaction whose SQLSTATE PostgreSQL itself defines as transient
  -- (`40001 serialization_failure`).  The Office reopen puts the bundle back in
  -- play first, which is also the G5-6 precondition this section already owns.
  v_result:=public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'Office reviewed the census error and asked for another attempt',
    'd0000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean and v_result->>'state'='PENDING',
    'B4.1/G5-6: the Office reopen puts the permanently refused bundle back in play: '
      ||v_result::text);

  while (select technical_failure_count from public.weekly_source_pending_entitlement_bundles
          where id=v_pending)<10 loop
    update public.weekly_source_pending_entitlement_bundles
       set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
    perform private.weekly_source_pending_entitlement_release_claim_page_v1(
      'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',60,25);
    select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
    perform pg_temp.assert_true(
      v_row.state='RELEASING' and v_row.lease_token is not null,
      'R14: each attempt takes a real lease before its transaction rolls back');
    -- Ruling 6 point 7: the release transaction rolled back, so the Worker
    -- records the failure in a fresh transaction, reporting the SQLSTATE and
    -- the failure kind as facts and classifying neither.
    v_result:=public.weekly_source_pending_entitlement_release_record_failure_v1(
      pg_catalog.jsonb_build_object(
        'pending_bundle_id',v_pending,
        'lease_token',v_row.lease_token,
        'worker_id',v_row.lease_owner,
        'worker_run_id',v_row.lease_worker_run_id,
        'code','WEEKLY_SOURCE_RELEASE_APPLY_FAILED',
        'detail','WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK',
        'sqlstate','40001',
        'failure_kind','DATABASE_ERROR'));
    perform pg_temp.assert_true(
      (v_result->>'released')::boolean is false
      and (v_result->>'recorded')::boolean
      and (v_result->>'refusal_disposition')='TRANSIENT',
      'B4.1: a proved-transient SQLSTATE is the ONLY thing that spends the budget: '
        ||(v_result-'detail')::text);
  end loop;

  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    v_row.state='MANUAL_REVIEW' and v_row.technical_failure_count=10
    and v_row.manual_review_reason is not null
    and pg_catalog.jsonb_typeof(v_row.manual_review_reason::jsonb)='object'
    and v_row.manual_review_reason::jsonb->>'code'='WEEKLY_SOURCE_RELEASE_APPLY_FAILED'
    and (v_row.manual_review_reason::jsonb->>'consecutive_technical_failures')::integer=10
    -- Review finding F3: every enumerated identifier reaches the Office, and
    -- the count is the true one whether or not every item is listed.  A
    -- rolled-back release transaction carries no census of its own, so the
    -- truthful count here is zero and `items_truncated` must say false; the
    -- lossless identifiers for a CENSUS-driven review are proved in section 9
    -- and, at scale, in section 18.
    and (v_row.manual_review_reason::jsonb->>'items_truncated')::boolean is false
    and (v_row.manual_review_reason::jsonb->>'item_count')::integer
        =pg_catalog.jsonb_array_length(v_row.manual_review_reason::jsonb->'items'),
    'R14: ten consecutive TRANSIENT failures move the bundle to MANUAL_REVIEW with a '
      ||'clear, specific Office-visible reason (round-4 ruling 4; review F3; '
      ||'round-5 ruling B4.1): '||coalesce(v_row.manual_review_reason,'<null>'));
  perform pg_temp.assert_true(
    v_row.lease_token is null and v_row.lease_owner is null,
    'R14: a bundle in MANUAL_REVIEW carries no lease');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=0,
    'R14: the old head remains current and nothing was published');

  -- A bundle in MANUAL_REVIEW is not claimed again by any tick.
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 hour' where id=v_pending;
  perform pg_temp.assert_true(
    (private.weekly_source_pending_entitlement_release_claim_page_v1(
       'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',60,25)
     ->>'claimed_count')::integer=0,
    'R14: MANUAL_REVIEW is terminal for the Worker; only the audited Office action reopens it');

  -- G5-6: the audited reopen, and only from MANUAL_REVIEW.
  select pg_catalog.count(*) into v_audit_before from public.audit_events
   where object_id_text=v_pending::text;
  v_result:=public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'Office reviewed the frozen evidence and asked for another attempt',
    'd0000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean and v_result->>'state'='PENDING'
    and (v_result->>'released')::boolean is false,
    'G5-6: the reopen returns the bundle to PENDING and never releases: '||v_result::text);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    v_row.state='PENDING' and v_row.technical_failure_count=0
    and v_row.manual_review_reason is null,
    'G5-6: the reopen resets the counter and clears the reason');
  select pg_catalog.count(*) into v_audit_after from public.audit_events
   where object_id_text=v_pending::text
     and action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED';
  perform pg_temp.assert_true(
    v_audit_after=v_audit_before+1,
    'G5-6: exactly one audit row is written by the reopen');

  -- A second reopen from PENDING is refused; the reopen is not a general lever.
  v_result:=public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'again','d0000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PENDING_BUNDLE_NOT_IN_MANUAL_REVIEW',
    'G5-6: reopen applies only to MANUAL_REVIEW');
  perform pg_temp.expect_failure(
    $sql$select public.weekly_source_pending_entitlement_bundle_reopen_v1(
      '00000000-0000-4000-8000-000000000000','r','d0000000-0000-4000-8000-000000000099');$sql$,
    '42501','','G5-6: an unknown actor cannot reopen a bundle');
end
$verify_release_manual_review$;

-- ---------------------------------------------------------------------------
-- 11. R22, R23, R24 - a rotated or ambiguous stored root is MANUAL_REVIEW
-- ---------------------------------------------------------------------------
do $verify_release_rotation$
declare
  v_pending uuid;
  v_row record;
  v_result jsonb;
begin
  if pg_catalog.to_regprocedure(
       'private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)') is null then
    raise notice 'SKIPPED: interface I-1 is not installed; the rotation gate was not exercised';
    return;
  end if;
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b1';
  v_pending:=v_row.id;

  -- R24: an ambiguous family - here ZERO current rows.  (Two current rows are
  -- impossible: `timesheets_booking_id_current_uidx` is a partial unique index,
  -- so the ambiguity that can actually occur is the empty one.)
  update public.timesheets set is_current=false
   where timesheet_id='d0000000-0000-4000-8000-000000000006';
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false and v_result->>'outcome'='MANUAL_REVIEW',
    'R24: an ambiguous family is MANUAL_REVIEW, and nothing is written: '
      ||(v_result-'detail')::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=0,
    'R24: nothing was published');
  -- Office reviews it and asks for another attempt; the family is still broken
  -- when the next tick runs, which is the R22 case below.
  perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'Office asked for another attempt after the family was investigated',
    'd0000000-0000-4000-8000-000000000001');

  -- R22: the family rotates under the waiting bundle.  The stored physical id
  -- is no longer canonical.  The installed partial unique index allows exactly
  -- one current row per booking, and the old version is already stood down.
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
    week_ending_date,contract_id,actual_schedule_json,qr_payload_json,
    is_adjustment,created_at,updated_at
  ) values (
    'd0000000-0000-4000-8000-000000000046','WSREL-0001',2,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'rel-occupant-a','rel-hospital','rel-ward','rel-role','weekly-0','2026-03-08',
    'd0000000-0000-4000-8000-000000000004',
    '[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());

  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'outcome'='MANUAL_REVIEW',
    'R22: a rotated stored root is MANUAL_REVIEW, never a rebuild: '||(v_result-'detail')::text);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    v_row.state='MANUAL_REVIEW'
    and v_row.manual_review_reason::jsonb->>'code'='WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION',
    'R22: the Office-visible reason names the rotation');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=0
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts)=0,
    'R22: nothing was published and nothing was rebuilt');
  perform pg_temp.assert_true(
    (select version=1 and is_current is false
       from public.timesheets where timesheet_id='d0000000-0000-4000-8000-000000000006')
    and (select version=2 and is_current
           from public.timesheets where timesheet_id='d0000000-0000-4000-8000-000000000046'),
    'R22: the release owner rotated, replaced or rebuilt no public Timesheet');

  -- Put the family back so the release proof below runs on a clean root.
  update public.timesheets set is_current=false
   where timesheet_id='d0000000-0000-4000-8000-000000000046';
  update public.timesheets set is_current=true
   where timesheet_id='d0000000-0000-4000-8000-000000000006';
  delete from public.timesheets where timesheet_id='d0000000-0000-4000-8000-000000000046';
end
$verify_release_rotation$;

-- ---------------------------------------------------------------------------
-- 11a. HANDOVER 2 round-7 ruling A6 - THE COLLISION NAME, AND ITS LIMIT
-- ---------------------------------------------------------------------------
-- "§4.0 remains fail-closed for bound bundles.  Where the ambiguous family is
--  specifically the trim/whitespace-equivalent canonical booking-reference
--  split, the authoritative outcome is `BOOKING_REFERENCE_CANONICAL_COLLISION`
--  everywhere, including R24.  Other non-canonical shapes retain their own
--  precise reason; do not collapse every integrity failure into the collision
--  code."
--
-- Two limbs, both driven end to end through the real claim and apply owners, so
-- what is measured is the OFFICE-VISIBLE outcome and not an internal string:
--
--   * the trim/whitespace-equivalent split - a sibling row whose `btrim()`
--     matches but whose raw `booking_id` differs, which the installed partial
--     unique index on the RAW value permits - must now carry
--     `BOOKING_REFERENCE_CANONICAL_COLLISION`;
--   * the zero-current-row family, which is the shape R24 actually drives, must
--     KEEP `WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION` with its own
--     precise `CANONICAL_AMBIGUOUS` reason.  Collapsing it into the collision
--     code is the easy and wrong reading the approver forbade by name.
--
-- Measured on a build from empty before this change, BOTH produced
-- `WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION`, with the collision name
-- surviving only as a text suffix.
do $verify_a6_collision_name$
declare
  v_pending uuid;
  v_row record;
  v_result jsonb;
  v_heads bigint;
begin
  if pg_catalog.to_regprocedure(
       'private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)') is null then
    raise notice 'SKIPPED: interface I-1 is not installed; ruling A6 was not exercised';
    return;
  end if;
  select pg_catalog.count(*) into v_heads from public.weekly_source_entitlement_heads;
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b1';
  v_pending:=v_row.id;

  -- Limb 1: the trim/whitespace-equivalent canonical booking-reference split.
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
    week_ending_date,contract_id,actual_schedule_json,qr_payload_json,is_adjustment,
    created_at,updated_at
  ) values (
    'd0000000-0000-4000-8000-0000000000a6',' WSREL-0001',1,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'rel-occupant-split','rel-hospital','rel-ward','rel-role','weekly-0','2026-03-08',
    'd0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
  update public.weekly_source_pending_entitlement_bundles
     set state='PENDING',manual_review_reason=null,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,lease_expires_at_utc=null,
         next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second',
         pending_revision=pending_revision+1
   where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cca6',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'outcome'='MANUAL_REVIEW'
    and v_row.state='MANUAL_REVIEW'
    and (v_row.manual_review_reason::jsonb)->>'code'='BOOKING_REFERENCE_CANONICAL_COLLISION'
    and (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads,
    'A6: the trim/whitespace-equivalent canonical booking-reference split reaches the '
      ||'Office as BOOKING_REFERENCE_CANONICAL_COLLISION, and nothing is published: '
      ||coalesce(v_row.manual_review_reason,'<null>'));
  delete from public.timesheets where timesheet_id='d0000000-0000-4000-8000-0000000000a6';

  -- Limb 2: the OTHER non-canonical shape must keep its own precise reason.
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
  update public.timesheets set is_current=false
   where timesheet_id='d0000000-0000-4000-8000-000000000006';
  update public.weekly_source_pending_entitlement_bundles
     set state='PENDING',manual_review_reason=null,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,lease_expires_at_utc=null,
         next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second',
         pending_revision=pending_revision+1
   where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000ccb6',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'outcome'='MANUAL_REVIEW'
    and (v_row.manual_review_reason::jsonb)->>'code'
        ='WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION'
    and (v_row.manual_review_reason::jsonb)->>'code'
        <>'BOOKING_REFERENCE_CANONICAL_COLLISION'
    and pg_catalog.strpos(coalesce((v_row.manual_review_reason::jsonb)->>'message',''),
                          'CANONICAL_AMBIGUOUS')>0
    and (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads,
    'A6: a zero-or-many canonical-row family keeps its own precise reason and is NOT '
      ||'collapsed into the collision code: '||coalesce(v_row.manual_review_reason,'<null>'));
  update public.timesheets set is_current=true
   where timesheet_id='d0000000-0000-4000-8000-000000000006';

  -- Limb 3: the other named shapes keep their own reasons at the read-only
  -- resolver too, so the collision code has not become universal there either.
  perform pg_temp.assert_true(
    private.weekly_source_resolve_root_identity_v1(null)->>'reason'='ROOT_ID_REQUIRED',
    'A6: a null root keeps ROOT_ID_REQUIRED, not the collision code');
end
$verify_a6_collision_name$;

-- ---------------------------------------------------------------------------
-- 12. R1, R2 shape and R33 - the release, then the exact replay
-- ---------------------------------------------------------------------------
-- The frozen evidence is retired: the batch's only family item is voided and
-- its whole-batch cancellation is now proved by Binding B (CANCELLED with
-- cancelled_at_utc, every family item voided, every operation terminal, no
-- non-terminal correction request).  The census becomes RELEASABLE and the
-- SAME coordinator publishes.
do $verify_release_released$
declare
  v_guard_installed boolean;
  v_pending uuid;
  v_row record;
  v_result jsonb;
  v_again jsonb;
  v_census jsonb;
  v_receipt record;
begin
  if pg_catalog.to_regprocedure(
       'private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)') is null
     or pg_catalog.to_regprocedure(
       'private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)') is null then
    raise notice 'SKIPPED: interface I-1 or I-4 is not installed; the release was not exercised';
    return;
  end if;
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b1';
  v_pending:=v_row.id;

  delete from public.pay_payment_correction_requests
   where id='d0000000-0000-4000-8000-0000000cc101';
  v_census:=private.weekly_source_freeze_census_v1(
    'd0000000-0000-4000-8000-000000000003',
    array['d0000000-0000-4000-8000-000000000006']::uuid[]);
  perform pg_temp.assert_true(
    v_census->>'result'='RELEASABLE',
    'the whole-batch cancellation must now be RELEASABLE under Binding B: '
      ||(v_census-'proof'-'predicates')::text);

  -- Reopen the bundle through the audited Office action, exactly as Office
  -- would after reviewing the evidence.
  perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'Cancellation completed; release may be retried',
    'd0000000-0000-4000-8000-000000000001');
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc01',120,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;

  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean and (v_result->>'released')::boolean
    and (v_result->>'replayed')::boolean is false,
    'R1: a releasable bundle publishes through the SAME coordinator: '
      ||(v_result-'census'-'receipt')::text);

  -- The receipt is DEFERRED, carries both Worker fields, and its census and
  -- proof are the real ones (proof/32 section 9; H2-038).
  select * into v_receipt from private.weekly_source_entitlement_publication_receipts
   where pending_bundle_id=v_pending;
  perform pg_temp.assert_true(
    v_receipt.publication_mode='DEFERRED'
    and v_receipt.released_by_worker_id='weekly-source-release-worker'
    and v_receipt.released_by_worker_run_id='d0000000-0000-4000-8000-00000000cc01'
    and v_receipt.decided_by_user_id='d0000000-0000-4000-8000-000000000001',
    'H2-038: a DEFERRED receipt carries both Worker fields and the Office actor');
  perform pg_temp.assert_true(
    v_receipt.census_json->>'result'='RELEASABLE'
    and pg_catalog.jsonb_typeof(v_receipt.proof_json)='object'
    and v_receipt.proof_json ? 'cancellation' and v_receipt.proof_json ? 'settlement',
    'proof/32 section 9: the deferred receipt carries the complete census and every 5.1/5.2 tuple');
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(v_receipt.proof_json->'cancellation')>=1,
    'the cancellation proof tuples of the released items are recorded');

  -- The bundle's own release record: all five facts of section 8 step 5.
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    v_row.state='RELEASED' and v_row.released_at_utc is not null
    and v_row.released_receipt_id=v_receipt.id
    and v_row.released_receipt_digest=v_receipt.request_digest
    and v_row.released_by_worker_id='weekly-source-release-worker'
    and v_row.released_by_worker_run_id='d0000000-0000-4000-8000-00000000cc01',
    'proof/32 section 8 step 5: the bundle records the receipt, the time and both Worker fields');
  perform pg_temp.assert_true(
    v_row.request_digest=v_receipt.request_digest,
    'the pending bundle''s stored digest and the deferred receipt''s digest are one value');

  -- Exactly one head, committed current, one invalidation token, one receipt.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where state='COMMITTED_CURRENT')=1
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts)=1,
    'exactly one committed head and exactly one receipt');

  -- R33: the response is lost, the lease expires, the same request is applied
  -- again.  The replay check runs BEFORE the lease checks, so the committed
  -- receipt is returned and nothing is republished.
  update public.weekly_source_pending_entitlement_bundles
     set lease_owner='weekly-source-release-worker',
         lease_token='d0000000-0000-4000-8000-00000000cc09',
         lease_worker_run_id='d0000000-0000-4000-8000-00000000cc01',
         lease_expires_at_utc=pg_catalog.clock_timestamp()-interval '1 hour'
   where id=v_pending;
  v_again:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc09',
    'd0000000-0000-4000-8000-00000000cc01');
  perform pg_temp.assert_true(
    (v_again->>'ok')::boolean and (v_again->>'replayed')::boolean
    and (v_again->'receipt'->>'id')::uuid=v_receipt.id,
    'R33: the receipt is returned by the replay check BEFORE the expired-lease rejection: '
      ||(v_again-'receipt')::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=1
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts)=1,
    'R33: nothing was republished');

  -- The bundle's money identity is FROZEN, including the stored request.  With
  -- the ACL fact guard installed, no writer can move any field the replay check
  -- compares, so the replay-conflict branch is unreachable through the bundle -
  -- which is the stronger property and is asserted first.
  v_guard_installed:=exists (
    select 1 from pg_catalog.pg_trigger trigger_row
     where trigger_row.tgrelid
             ='public.weekly_source_pending_entitlement_bundles'::pg_catalog.regclass
       and trigger_row.tgname='weekly_source_immutable_fact_guard'
       and not trigger_row.tgisinternal);
  if v_guard_installed then
    perform pg_temp.expect_failure(
      pg_catalog.format($sql$update public.weekly_source_pending_entitlement_bundles
         set member_root_versions=array[99]::integer[] where id=%L;$sql$,v_pending),
      '55000','WEEKLY_SOURCE_IMMUTABLE_FACT',
      'the bundle''s member identity cannot be rewritten');
    perform pg_temp.expect_failure(
      pg_catalog.format($sql$update public.weekly_source_pending_entitlement_bundles
         set request_json='{"tampered":true}'::jsonb where id=%L;$sql$,v_pending),
      '55000','WEEKLY_SOURCE_IMMUTABLE_FACT',
      'WP-08b_NEEDS N1: the STORED REQUEST is an identity column and cannot be rewritten');
    perform pg_temp.expect_failure(
      pg_catalog.format($sql$delete from public.weekly_source_pending_entitlement_bundles
         where id=%L;$sql$,v_pending),
      '55000','WEEKLY_SOURCE_IMMUTABLE_RECORD',
      'a pending bundle row cannot be deleted');
    -- TEST SCAFFOLDING, rolled back with everything else: the guard is stood
    -- down for exactly one statement so the replay-conflict BRANCH is still
    -- executed rather than merely argued to be unreachable.
    execute 'alter table public.weekly_source_pending_entitlement_bundles
             disable trigger weekly_source_immutable_fact_guard';
  end if;

  update public.weekly_source_pending_entitlement_bundles
     set member_root_versions=array[99]::integer[] where id=v_pending;
  v_again:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc09',
    'd0000000-0000-4000-8000-00000000cc01');
  perform pg_temp.assert_true(
    (v_again->>'ok')::boolean is false
    and v_again->>'code'='WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT',
    'R10/R39: a digest hit whose immutable fields differ refuses the replay: '||v_again::text);
  update public.weekly_source_pending_entitlement_bundles
     set member_root_versions=array[1]::integer[] where id=v_pending;

  if v_guard_installed then
    execute 'alter table public.weekly_source_pending_entitlement_bundles
             enable trigger weekly_source_immutable_fact_guard';
  end if;

  -- The head this section published is the one ruling A3's supersession proof
  -- in section 12a below drives.  Prove here, while the publication is still in
  -- hand, that it is the shape that proof needs: one COMMITTED_CURRENT head for
  -- the root, carrying real money rather than a certified zero, and owned by
  -- the live authorisation generation.
  perform pg_temp.assert_true(
    (select head_row.state='COMMITTED_CURRENT'
       and head_row.certified_zero is false
       and head_row.component_count=1
       and head_row.root_timesheet_id='d0000000-0000-4000-8000-000000000006'
     from public.weekly_source_entitlement_heads head_row
     where head_row.id='d0000000-0000-4000-8000-0000000000c1')
    and (select authorisation_row.current_entitlement_head_id
                ='d0000000-0000-4000-8000-0000000000c1'
         from public.weekly_source_root_authorisations authorisation_row
         where authorisation_row.root_timesheet_id='d0000000-0000-4000-8000-000000000006'
           and authorisation_row.withdrawn_at_utc is null),
    'section 12a needs the coordinator to have left exactly one committed current '
      ||'head owned by the live generation');
end
$verify_release_released$;

-- ---------------------------------------------------------------------------
-- 12a. HANDOVER 2 round-5 ruling A3 - withdrawal after a COMMITTED entitlement
--      head.  Package WP-25.
-- ---------------------------------------------------------------------------
-- WHAT WAS HERE BEFORE, AND WHY IT IS GONE.
--
-- Until 18 September 2026 this position carried a WP-07b assertion which read
-- "withdrawal after a committed entitlement head is refused, PERMANENTLY, and
-- nothing about it is retryable", and which accepted the refusal code
-- `WEEKLY_SOURCE_UNAUTHORISE_ENTITLEMENT_HEAD_COMMITTED`.  Both are dead.
-- Round-5 ruling A3 REJECTED that rule in the approver's own words:
--
--   "OPTION (b) IS SELECTED WITH STRICT CONDITIONS.  Option (a) is rejected as
--    a permanent product rule ... A user must not become permanently unable to
--    withdraw an authorisation merely because its entitlement head has been
--    committed. ... the withdrawal owner must supersede the committed head
--    atomically with the withdrawal.  The withdrawal is permitted only when
--    authoritative checks prove that no payment work or ambiguous money effect
--    has crossed the boundary. ... If any payment is already in flight or any
--    effect is ambiguous, withdrawal is refused."
--
-- No installed owner produces `WEEKLY_SOURCE_UNAUTHORISE_ENTITLEMENT_HEAD_
-- COMMITTED` any more, so that limb of the old disjunction could never fire.
-- The assertion nevertheless passed, on the OTHER limb, for a reason that has
-- nothing to do with heads at all: this file's own fixture leaves a live
-- PENDING release bundle and a PROPOSED A/B decision bundle naming the root, so
-- W1 refuses for `PENDING_ENTITLEMENT_BUNDLE_NAMES_THE_ROOT` and
-- `ACCEPTED_OR_PROPOSED_LATER_DECISION`, and W9 refused as well because the
-- seeded authorisation row carried no round-5 decision digest.  It would have
-- gone on passing if supersession had been deleted outright (Gate 13 hostile
-- finance review, finding F4).
--
-- WHAT IS ASSERTED NOW.  Both limbs of the controlling rule, executed against
-- the head THIS FILE published a moment ago through the real coordinator, with
-- the real availability and withdrawal owners called and the relations read
-- back afterwards.  Nothing below inspects a routine's text.
--
--   LIMB 0  the refusal ruling A3 KEEPS: while a later decision is in flight,
--           W1 refuses permanently - and the reason it gives is the later
--           decision, never the head.
--   LIMB 1  a payment effect HAS crossed the boundary: refused permanently,
--           the committed head is still COMMITTED_CURRENT, the authorisation is
--           still live, and no withdrawal receipt exists.
--   LIMB 2  no payment effect has crossed: the withdrawal SUCCEEDS, the head is
--           SUPERSEDED in the same transaction with reason
--           `FIRST_AUTHORISATION_WITHDRAWN` and the immutable predecessor link
--           to the durable receipt, and no COMMITTED_CURRENT head survives for
--           the root - which is what stops the Gate 4 selector, unchanged,
--           paying the pre-withdrawal figure.
--
-- The one difference between LIMB 1 and LIMB 2 is one row of Banking Pay
-- settlement evidence.  That is deliberate: it is what makes this a test rather
-- than a demonstration.  If the money checks stopped working, LIMB 1 fails; if
-- the supersession stopped working, LIMB 2 fails.
--
-- SAVEPOINT.  Everything here runs inside `wp25_ruling_a3` and is rolled back
-- to it, so sections 13 onwards see the state section 12 left and no later
-- assertion in this file depends on anything done here.  The whole file is
-- still inside the outer transaction that ends in `rollback`.
-- ---------------------------------------------------------------------------
savepoint wp25_ruling_a3;

do $verify_ruling_a3_supersession$
declare
  v_root constant uuid:='d0000000-0000-4000-8000-000000000006';
  v_actor constant uuid:='d0000000-0000-4000-8000-000000000001';
  v_head constant uuid:='d0000000-0000-4000-8000-0000000000c1';
  v_pending uuid;
  v_authorisation uuid;
  v_signature text;
  v_avail jsonb;
  v_result jsonb;
  v_receipts_before bigint;
begin
  if pg_catalog.to_regprocedure(
       'public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)') is null
     or pg_catalog.to_regprocedure(
       'public.weekly_source_first_authorisation_withdraw_available_v1(uuid)') is null
     or pg_catalog.to_regprocedure(
       'private.weekly_source_pending_release_superseded_v1(uuid,text,jsonb)') is null then
    raise notice 'SKIPPED: the WP-07c withdrawal owner is not installed here; ruling A3 was not exercised';
    return;
  end if;

  select authorisation_row.id into v_authorisation
    from public.weekly_source_root_authorisations authorisation_row
   where authorisation_row.root_timesheet_id=v_root
     and authorisation_row.withdrawn_at_utc is null;
  perform pg_temp.assert_true(v_authorisation is not null,
    'A3: the root must carry exactly one live authorisation generation');

  -- ---- LIMB 0 -------------------------------------------------------------
  -- The refusal ruling A3 KEEPS.  Section 13's A/B decision is still PROPOSED
  -- and its release bundle is still PENDING, and a root named by a later
  -- decision is not withdrawable.  The assertion names the REASON, so a future
  -- change that refused for some other cause could not satisfy it.
  v_avail:=public.weekly_source_first_authorisation_withdraw_available_v1(v_root);
  perform pg_temp.assert_true(
    (v_avail->>'available')::boolean is false
    and exists (
      select 1 from pg_catalog.jsonb_array_elements(v_avail->'checks') as check_row(value)
       where check_row.value->>'check'='W1'
         and (check_row.value->>'passed')::boolean is false
         and check_row.value->>'nature'='PERMANENT'
         and check_row.value->>'code'='WEEKLY_SOURCE_UNAUTHORISE_LATER_DECISION_EXISTS'
         and check_row.value->'reasons' @> '["PENDING_ENTITLEMENT_BUNDLE_NAMES_THE_ROOT"]'::jsonb
         and check_row.value->'reasons' @> '["ACCEPTED_OR_PROPOSED_LATER_DECISION"]'::jsonb),
    'A3 limb 0: a root named by a live later decision is refused by W1, and the reason '
      ||'is the later decision: '||v_avail::text);
  -- And the head is NOT among the reasons W1 gives.  This is the assertion the
  -- old text got backwards.
  perform pg_temp.assert_true(
    not exists (
      select 1 from pg_catalog.jsonb_array_elements(v_avail->'checks') as check_row(value)
      cross join pg_catalog.jsonb_array_elements_text(
        coalesce(check_row.value->'reasons','[]'::jsonb)) as reason(value)
       where check_row.value->>'check'='W1'
         and reason.value like '%ENTITLEMENT_HEAD%'
         and reason.value<>'ENTITLEMENT_HEAD_STAGED_FOR_THE_ROOT')
    and v_avail::text not like '%WEEKLY_SOURCE_UNAUTHORISE_ENTITLEMENT_HEAD_COMMITTED%',
    'A3: a COMMITTED_CURRENT head is no longer a W1 refusal reason and the WP-07b '
      ||'refusal code is retired: '||v_avail::text);
  -- The verdict already names the head it WOULD supersede, decided under the
  -- same evaluation, which is how the Office screen offers the action.
  perform pg_temp.assert_true(
    (v_avail#>>'{head_supersession,head_id}')=v_head::text
    and (v_avail#>>'{head_supersession,state_before}')='COMMITTED_CURRENT'
    and (v_avail#>>'{head_supersession,certified_zero}')='false',
    'A3: the verdict must name the committed head it would supersede: '||v_avail::text);

  -- ---- the later decision is retired, through the installed owners ---------
  select bundle_row.id into v_pending
    from public.weekly_source_pending_entitlement_bundles bundle_row
   where bundle_row.decision_bundle_id='d0000000-0000-4000-8000-0000000000b6';
  perform private.weekly_source_pending_release_superseded_v1(
    v_pending,'WEEKLY_SOURCE_PENDING_BUNDLE_SUPERSEDED',
    pg_catalog.jsonb_build_object(
      'reason','SECTION_12A_FIXTURE_THE_OFFICE_ABANDONED_THE_A_B_DECISION'));
  update public.weekly_source_entitlement_decision_bundles
     set state='ABANDONED'
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b6';

  -- ---- the ordinary lifecycle state the release fixture never needed -------
  -- The root is authorised (the fixture seeds `authorised_at_server`, for the
  -- reason given there), but this file never needed the ordinary financial row
  -- or the contract-week link, and `public.timesheet_unauthorise_atomic` - which
  -- the withdrawal owner calls, unchanged and call-only - refuses with
  -- TARGET_NOT_FOUND / NO_TSFIN when there is no current TSFIN.  Seeded here, in
  -- the authorised shape that matches the Timesheet.
  update public.contract_weeks
     set timesheet_id=v_root,
         status='SUBMITTED'::public.contract_week_status_enum,
         submission_mode_snapshot='MANUAL'::public.submission_mode_enum
   where id='d0000000-0000-4000-8000-000000000005';
  -- `trg_tsfin_ai` may already have created a row for this Timesheet, so the
  -- row is brought to the authorised shape whichever way it got here.  This is
  -- an explicit existence test, never a `limit 1` (Part 1 review rule 5).
  if exists (select 1 from public.timesheets_financials financial_row
              where financial_row.timesheet_id=v_root) then
    update public.timesheets_financials
       set is_current=true,
           timesheet_version=1,
           candidate_id='d0000000-0000-4000-8000-000000000003',
           client_id='d0000000-0000-4000-8000-000000000002',
           processing_status='READY_FOR_HR'::public.ts_fin_processing_status_enum,
           authorised_at_utc=pg_catalog.clock_timestamp(),
           total_hours=9,total_pay_ex_vat=90,total_charge_ex_vat=180
     where timesheet_id=v_root;
  else
    insert into public.timesheets_financials(
      id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
      processing_status,authorised_at_utc,total_hours,total_pay_ex_vat,total_charge_ex_vat
    ) values (
      'd0000000-0000-4000-8000-0000000000e1',v_root,1,true,
      'd0000000-0000-4000-8000-000000000003','d0000000-0000-4000-8000-000000000002',
      'READY_FOR_HR'::public.ts_fin_processing_status_enum,pg_catalog.clock_timestamp(),
      9,90,180);
  end if;
  perform pg_temp.assert_true(
    (select timesheet_row.authorised_at_server is not null
       from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_root)
    and exists (select 1 from public.timesheets_financials financial_row
                 where financial_row.timesheet_id=v_root and financial_row.is_current),
    'A3: the root must be authorised and carry a current financial row before a '
      ||'withdrawal can be asked for');

  -- ---- LIMB 1 -------------------------------------------------------------
  -- A payment effect HAS crossed the boundary: the root's own financial row is
  -- marked PAID.  That is exactly what ruling A3 calls payment work across the
  -- boundary, and W2 reads it as a PERMANENT money refusal.  It is deliberately
  -- an effect that leaves the freeze census RELEASABLE, so the only thing
  -- standing between this root and a supersession is the money check itself.
  update public.timesheets_financials
     set paid_at_utc=pg_catalog.clock_timestamp()
   where timesheet_id=v_root;

  select pg_catalog.count(*) into v_receipts_before
    from private.weekly_source_first_authorisation_withdrawal_receipts;

  v_signature:=private.weekly_source_first_authorisation_context_v1(v_root)
                 ->>'current_row_signature';
  perform pg_temp.assert_true(v_signature is not null,
    'A3: the caller needs the root''s current row signature to ask for a withdrawal');

  v_avail:=public.weekly_source_first_authorisation_withdraw_available_v1(v_root);
  perform pg_temp.assert_true(
    (v_avail->>'available')::boolean is false
    and (v_avail->>'retryable')::boolean is false
    and v_avail->>'code'='WEEKLY_SOURCE_UNAUTHORISE_PAID'
    and exists (
      select 1 from pg_catalog.jsonb_array_elements(v_avail->'checks') as check_row(value)
       where check_row.value->>'check'='W2'
         and (check_row.value->>'passed')::boolean is false
         and check_row.value->>'nature'='PERMANENT'
         and check_row.value->'reasons' @> '["TIMESHEET_FINANCIALS_PAID"]'::jsonb)
    and v_avail->>'census_result'='RELEASABLE',
    'A3 limb 1: with the root paid, the verdict is a PERMANENT money refusal, and the '
      ||'freeze census is not what produced it: '||v_avail::text);

  -- The WRITE path gives the same answer, and writes nothing.  The Workbench
  -- queue is stood down first, exactly as WP-07c's own verifier does it: R25's
  -- invalidation contract identifies this operation's jobs as the ones that are
  -- QUEUED or RUNNING afterwards and were not before, so a queue still holding
  -- section 12's publication jobs would let this operation's invalidation
  -- coalesce into one of them and the contract would report NO_COMPLETE_SCOPE_JOB.
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,v_signature,v_actor);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and coalesce((v_result->>'withdrawn')::boolean,false) is false
    and coalesce((v_result->>'head_superseded')::boolean,false) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_PAID',
    'A3 limb 1: the write path refuses a root whose money has moved and supersedes '
      ||'nothing: '||v_result::text);
  perform pg_temp.assert_true(
    (select head_row.state='COMMITTED_CURRENT'
       and head_row.superseded_at_utc is null
       and head_row.superseded_reason is null
       and head_row.superseded_by_withdrawal_id is null
     from public.weekly_source_entitlement_heads head_row where head_row.id=v_head)
    and (select authorisation_row.withdrawn_at_utc is null
           and authorisation_row.current_entitlement_head_id=v_head
         from public.weekly_source_root_authorisations authorisation_row
         where authorisation_row.id=v_authorisation)
    and (select timesheet_row.authorised_at_server is not null
           from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_root)
    and (select pg_catalog.count(*)
           from private.weekly_source_first_authorisation_withdrawal_receipts)
        =v_receipts_before,
    'A3 limb 1: the committed head, the live generation, the authorisation on the '
      ||'Timesheet and the receipt relation are all exactly as they were');

  -- ---- LIMB 2 -------------------------------------------------------------
  -- The same root, the same head, the same call - with the payment effect
  -- removed and nothing else changed.  Ruling A3 requires this to SUCCEED and
  -- to retire the head.
  update public.timesheets_financials
     set paid_at_utc=null
   where timesheet_id=v_root;

  v_avail:=public.weekly_source_first_authorisation_withdraw_available_v1(v_root);
  perform pg_temp.assert_true(
    (v_avail->>'available')::boolean is true
    and (v_avail->>'ok')::boolean is true
    and v_avail->>'code' is null
    and (v_avail#>>'{head_supersession,head_id}')=v_head::text,
    'A3 limb 2: with no payment effect across the boundary a committed head must be '
      ||'withdrawable: '||v_avail::text);

  v_signature:=private.weekly_source_first_authorisation_context_v1(v_root)
                 ->>'current_row_signature';
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,v_signature,v_actor);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is true
    and (v_result->>'withdrawn')::boolean is true
    and (v_result->>'head_superseded')::boolean is true
    and (v_result#>>'{head_supersession,head_id}')=v_head::text
    and (v_result#>>'{head_supersession,state_before}')='COMMITTED_CURRENT'
    and (v_result#>>'{head_supersession,state_after}')='SUPERSEDED'
    and (v_result#>>'{head_supersession,superseded_reason}')='FIRST_AUTHORISATION_WITHDRAWN'
    and (v_result->>'entitlement_head_cleared')=v_head::text
    and (v_result->>'withdrawal_receipt_id') is not null,
    'A3 limb 2: the withdrawal must succeed and supersede the committed head '
      ||'atomically: '||v_result::text);

  -- Read back from the relations, not from the owner's own answer.
  perform pg_temp.assert_true(
    (select head_row.state='SUPERSEDED'
       and head_row.superseded_at_utc is not null
       and head_row.superseded_reason='FIRST_AUTHORISATION_WITHDRAWN'
       and head_row.superseded_by_head_id is null
       and head_row.superseded_by_withdrawal_id
           =(v_result->>'withdrawal_receipt_id')::uuid
     from public.weekly_source_entitlement_heads head_row where head_row.id=v_head),
    'A3 step 3: the head carries SUPERSEDED, the explicit withdrawal reason and the '
      ||'withdrawal authority, and no successor head');
  perform pg_temp.assert_true(
    (select receipt_row.predecessor_head_id=v_head
       and receipt_row.predecessor_head_state_before='COMMITTED_CURRENT'
       and receipt_row.head_superseded=true
       and receipt_row.root_authorisation_id=v_authorisation
     from private.weekly_source_first_authorisation_withdrawal_receipts receipt_row
     where receipt_row.id=(v_result->>'withdrawal_receipt_id')::uuid)
    and (select pg_catalog.count(*)
           from private.weekly_source_first_authorisation_withdrawal_receipts)
        =v_receipts_before+1,
    'A3 step 5: exactly one durable receipt, carrying the immutable predecessor link');
  perform pg_temp.assert_true(
    (select authorisation_row.withdrawn_at_utc is not null
       and authorisation_row.withdrawn_by_user_id=v_actor
       and authorisation_row.current_entitlement_head_id is null
     from public.weekly_source_root_authorisations authorisation_row
     where authorisation_row.id=v_authorisation)
    and (select timesheet_row.authorised_at_server is null
           from public.timesheets timesheet_row where timesheet_row.timesheet_id=v_root),
    'A3 step 2: the authorisation is withdrawn, its head pointer cleared, and the '
      ||'Timesheet is back to awaiting authorisation');

  -- THE WRONG-PAYMENT PATH.  The Gate 4 selector resolves a head by physical
  -- root and COMMITTED_CURRENT state; this is the state it would now read.  The
  -- selector itself is untouched by this package and by WP-07c.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads head_row
      where head_row.root_timesheet_id=v_root
        and head_row.state='COMMITTED_CURRENT')=0,
    'A3: no COMMITTED_CURRENT head may survive the withdrawal for this root');

  -- And nothing in Banking Pay moved to achieve any of it.
  perform pg_temp.assert_true(
    (select item_row.is_voided and item_row.amount_inc_vat=100.00
       from public.pay_batch_items item_row
      where item_row.id='d0000000-0000-4000-8000-00000000ba03')
    and (select pg_catalog.count(*) from public.timesheet_pay_state_history)=0
    and (select pg_catalog.count(*) from public.pay_bank_transfers)=0
    and (select financial_row.paid_at_utc is null
           from public.timesheets_financials financial_row
          where financial_row.timesheet_id=v_root),
    'A3: the withdrawal cancelled no money and reinterpreted no payment');
end
$verify_ruling_a3_supersession$;

rollback to savepoint wp25_ruling_a3;
release savepoint wp25_ruling_a3;

-- WP-01b's commit-time asserts, named explicitly.  Never SET CONSTRAINTS ALL
-- IMMEDIATE here: other deferred constraints in this database are not ours.
set constraints weekly_source_entitlement_head_inventory_assert,
                weekly_source_entitlement_head_receipt_assert,
                weekly_source_entitlement_head_component_inventory_assert immediate;
set constraints weekly_source_entitlement_head_inventory_assert,
                weekly_source_entitlement_head_receipt_assert,
                weekly_source_entitlement_head_component_inventory_assert deferred;

-- ---------------------------------------------------------------------------
-- 13. Round-4 ruling 6 point 7 - a rolled-back release transaction
-- ---------------------------------------------------------------------------
do $verify_release_rollback_record$
declare
  v_pending uuid;
  v_row record;
  v_result jsonb;
  v_request jsonb;
  v_before integer;
begin
  v_request:=pg_temp.request('d0000000-0000-4000-8000-0000000000b2',1,
    'd0000000-0000-4000-8000-0000000000c2','d0000000-0000-4000-8000-0000000000d2',
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c2c2c2c2-0000-4000-8000-000000000001','3.0','30.00')));
  perform pg_temp.mk_bundle(
    'd0000000-0000-4000-8000-0000000000b2',1,
    array['d0000000-0000-4000-8000-0000000000c2']::uuid[],
    'd0000000-0000-4000-8000-0000000000d2','b2',v_request);
  v_result:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request,
    pg_temp.lock_result(),
    pg_catalog.jsonb_build_object('result','FROZEN','items','[]'::jsonb,
      'class_counts',pg_catalog.jsonb_build_object('ACTIVE',1)));
  v_pending:=(v_result->>'pending_bundle_id')::uuid;

  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc03',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_before:=v_row.technical_failure_count;

  -- Another worker, another run, or another token cannot record a failure on
  -- someone else's bundle.
  v_result:=private.weekly_source_pending_entitlement_release_record_failure_v1(
    v_pending,v_row.lease_token,'someone-else',v_row.lease_worker_run_id,
    'WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK','55P03 lock timeout');
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_RELEASE_LEASE_INVALID',
    'ruling 6 point 7: only the lease holder may record its own rollback');

  -- The lease holder records it, even after the lease has expired, because the
  -- transaction that raised held a valid lease.
  update public.weekly_source_pending_entitlement_bundles
     set lease_expires_at_utc=pg_catalog.clock_timestamp()-interval '1 second'
   where id=v_pending;
  -- WP-08c, HANDOVER 2 round-5 ruling B4.1.  Round-4 ruling 6 point 7 names
  -- "retryable `55P03`" as the example, and `55P03 lock_not_available` is on the
  -- classifier's proved-transient list, so this case still takes the budget and
  -- still leaves the bundle PENDING.  The SQLSTATE now reaches the database as
  -- a FACT, in the fixed `SQLSTATE=xxxxx; KIND=xxxx;` form the public wrapper
  -- composes, and the database - not the Worker - decides what it means.
  v_result:=private.weekly_source_pending_entitlement_release_record_failure_v1(
    v_pending,v_row.lease_token,v_row.lease_owner,v_row.lease_worker_run_id,
    'WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK',
    'SQLSTATE=55P03; KIND=DATABASE_ERROR; lock timeout');
  perform pg_temp.assert_true(
    (v_result->>'recorded')::boolean
    and (v_result->>'technical_failure_count')::integer=v_before+1
    and v_result->>'state'='PENDING'
    and v_result->>'refusal_disposition'='TRANSIENT',
    'ruling 6 point 7: a rolled-back release transaction with a PROVED transient '
      ||'SQLSTATE is a technical failure on that bundle and leaves it PENDING or '
      ||'reclaimable, never half-released: '||v_result::text);
  perform pg_temp.assert_true(
    (select state='PENDING' and lease_token is null
       and next_check_at_utc>pg_catalog.clock_timestamp()
       from public.weekly_source_pending_entitlement_bundles where id=v_pending),
    'ruling 6 point 7: the bundle is reclaimable and backed off');

  -- B4.1, the reversal, executed on the same bundle: a rollback whose SQLSTATE
  -- can NEVER clear on its own - `23514 check_violation`, which is exactly what
  -- WP-08b's review reproduced in its F1 race - no longer spends nine more
  -- attempts and about ninety minutes. It reaches a human on this tick.
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc03',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_before:=v_row.technical_failure_count;
  v_result:=private.weekly_source_pending_entitlement_release_record_failure_v1(
    v_pending,v_row.lease_token,v_row.lease_owner,v_row.lease_worker_run_id,
    'WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK',
    'SQLSTATE=23514; KIND=DATABASE_ERROR; check constraint violated');
  perform pg_temp.assert_true(
    (v_result->>'released')::boolean is false
    and v_result->>'state'='MANUAL_REVIEW'
    and v_result->>'refusal_disposition'='PERMANENT'
    and (select technical_failure_count
           from public.weekly_source_pending_entitlement_bundles where id=v_pending)=v_before,
    'B4.1: a rollback with a SQLSTATE that cannot clear reaches Office manual review '
      ||'immediately and spends none of the budget: '||(v_result-'detail')::text);

  -- And the default for a rollback that carries NO SQLSTATE at all is the same:
  -- "the default for a refusal you cannot prove is transient must be immediate
  -- escalation, not the budget."
  perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'Office asked for another attempt after the constraint failure',
    'd0000000-0000-4000-8000-000000000001');
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc03',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=public.weekly_source_pending_entitlement_release_record_failure_v1(
    pg_catalog.jsonb_build_object(
      'pending_bundle_id',v_pending,'lease_token',v_row.lease_token,
      'worker_id',v_row.lease_owner,'worker_run_id',v_row.lease_worker_run_id,
      'code','WEEKLY_SOURCE_RELEASE_APPLY_FAILED',
      'detail','WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK'));
  perform pg_temp.assert_true(
    v_result->>'state'='MANUAL_REVIEW'
    and v_result->>'refusal_disposition'='PERMANENT'
    and (select technical_failure_count
           from public.weekly_source_pending_entitlement_bundles where id=v_pending)=0,
    'B4.1: an unproved rollback escalates immediately and spends no budget: '
      ||(v_result-'detail')::text);

  -- A Worker-observed TIMEOUT carries no SQLSTATE and IS transient (review F6):
  -- a slow-but-valid release must not be escalated to a human.
  perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'Office asked for another attempt after the unproved failure',
    'd0000000-0000-4000-8000-000000000001');
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc03',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=public.weekly_source_pending_entitlement_release_record_failure_v1(
    pg_catalog.jsonb_build_object(
      'pending_bundle_id',v_pending,'lease_token',v_row.lease_token,
      'worker_id',v_row.lease_owner,'worker_run_id',v_row.lease_worker_run_id,
      'code','WEEKLY_SOURCE_RELEASE_APPLY_FAILED',
      'detail','WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK',
      'failure_kind','TIMEOUT'));
  perform pg_temp.assert_true(
    v_result->>'state'='PENDING'
    and v_result->>'refusal_disposition'='TRANSIENT'
    and (select technical_failure_count
           from public.weekly_source_pending_entitlement_bundles where id=v_pending)=1,
    'B4.1/F6: a Worker-observed TIMEOUT is transient and takes the budget, not a human: '
      ||(v_result-'detail')::text);
end
$verify_release_rollback_record$;

-- ---------------------------------------------------------------------------
-- 13b. Decision D10 - the stored request and its digest, end to end
-- ---------------------------------------------------------------------------
-- D10 (WP-02_NEEDS N12): the migration does NOT bind
-- `weekly_source_pending_entitlement_bundles.request_json` to `request_digest`,
-- because the binding would need the canonical encoder, which lives in a
-- repeatable, and migrations are applied first.  The coordinator verifies the
-- pair instead, at save and at release, under the lock.  This section proves
-- WP-08b's half of that: I-5 stores the request it was given, UNMODIFIED, and
-- stores exactly the digest the coordinator will recompute.
--
-- It is driven through the REAL immediate entry point, so the coordinator's own
-- save-side check runs against WP-08b's row rather than being assumed.
do $verify_release_d10_pairing$
declare
  v_request jsonb;
  v_result jsonb;
  v_row record;
  v_heads_before bigint;
begin
  if pg_catalog.to_regprocedure(
       'private.weekly_source_entitlement_publish_immediate_v1(jsonb)') is null then
    raise notice 'SKIPPED: interface I-4 is not installed; D10 was not exercised end to end';
    return;
  end if;

  -- Freeze the B root with a live Draft item of its own.
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot)
  values ('d0000000-0000-4000-8000-00000000bb01','2026-03-25','DRAFT','MONZO_CSV','SAGE');
  insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id,settlement_status)
  values ('d0000000-0000-4000-8000-00000000bb02','d0000000-0000-4000-8000-00000000bb01',
          'd0000000-0000-4000-8000-000000000003',null);
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,amount_inc_vat)
  values ('d0000000-0000-4000-8000-00000000bb03','d0000000-0000-4000-8000-00000000bb02',
          'TIMESHEET_PAY','d0000000-0000-4000-8000-000000000016','PAYE',false,55.00);

  v_request:=pg_temp.request(
    'd0000000-0000-4000-8000-0000000000b7',1,
    'd0000000-0000-4000-8000-0000000000c8','d0000000-0000-4000-8000-0000000000d7',
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c8c8c8c8-0000-4000-8000-000000000001','6.0','60.00')),
    'd0000000-0000-4000-8000-000000000016','WSREL-0002',1);
  perform pg_temp.mk_bundle(
    'd0000000-0000-4000-8000-0000000000b7',1,
    array['d0000000-0000-4000-8000-0000000000c8']::uuid[],
    'd0000000-0000-4000-8000-0000000000d7','b7',v_request,
    'd0000000-0000-4000-8000-000000000016','WSREL-0002');

  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;

  v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  perform pg_temp.assert_true(
    (v_result->>'published')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE',
    'D10: a frozen root saves the decision through I-5 and publishes nothing: '
      ||(v_result-'census'-'lock_result')::text);
  perform pg_temp.assert_true(
    (v_result->'pending'->>'ok')::boolean and (v_result->'pending'->>'created')::boolean,
    'D10: the coordinator''s save-side check accepted WP-08b''s row: '
      ||coalesce((v_result->'pending')::text,'<null>'));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'D10: no head was written on the save path');

  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  -- (1) the request is stored UNMODIFIED.  I-5 normalises and enriches nothing;
  -- if it did, the coordinator's save-side check would raise.
  perform pg_temp.assert_true(
    v_row.request_json=v_request,
    'D10: the stored request must be byte-for-byte the request I-5 was handed');
  -- (2) the stored digest is the DEFERRED digest of the STORED request taken
  -- with this bundle's own id - exactly what the coordinator recomputes under
  -- the lock at release.
  perform pg_temp.assert_true(
    v_row.request_digest=private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(
        v_row.request_json,'DEFERRED',v_row.id)),
    'D10: the stored request and the stored digest must agree');
  -- (3) and it is NOT the acceptance digest the decision bundle carries, which
  -- is taken in IMMEDIATE mode with no pending bundle.  Two different values by
  -- design (I-3 section 5.1a).
  perform pg_temp.assert_true(
    v_row.request_digest<>(
      select bundle_row.request_digest
        from public.weekly_source_entitlement_decision_bundles bundle_row
       where bundle_row.decision_bundle_id='d0000000-0000-4000-8000-0000000000b7'
         and bundle_row.bundle_revision=1),
    'D10: the pending digest and the accepted decision''s acceptance digest differ by design');
end
$verify_release_d10_pairing$;

-- ---------------------------------------------------------------------------
-- 15. HANDOVER 2 round-5 ruling B4.2 - a long frozen run, MEASURED
-- ---------------------------------------------------------------------------
-- "Do not append an audit row for every frozen tick. Record state transitions
-- and maintain bounded current-state counters/timestamps. A repeated unchanged
-- frozen poll may update one bounded status record or operational metric, but
-- may not grow an audit table without limit."
--
-- This section does not assert the bound.  It RUNS a long frozen wait twice
-- over the same bundle and the same unchanged Banking Pay evidence, counting
-- every row that lands in `public.audit_events` across the whole run:
--
--   * run A is the behaviour this ruling reverses.  The watch marker is
--     stripped before each tick, so every tick is a full claim + apply cycle -
--     exactly what the shipped code did before WP-08c and exactly what it still
--     does for a first attempt.  The rows come from the installed Candidate
--     serial gate (`CANDIDATE_SERIAL_GATE_GRANTED`, which this package may not
--     change) and from WP-12's pending-bundle trigger;
--   * run B is the new behaviour.  Nothing is stripped, so the bounded watch
--     re-proves the freeze without a state transition.
--
-- The bundle is the one section 13 left PENDING on WSREL-0002, whose root is
-- frozen by the live DRAFT item `…bb03`.  The census is real on every tick.
do $verify_b42_audit_growth$
declare
  v_pending uuid;
  v_row record;
  v_claim jsonb;
  v_result jsonb;
  v_tick integer;
  v_ticks constant integer:=40;
  v_audit_start bigint;
  v_audit_a bigint;
  v_audit_b bigint;
  v_rows_a bigint;
  v_rows_b bigint;
  v_counter_before integer;
  v_heads_before bigint;
  v_receipts_before bigint;
begin
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  if not found then
    raise exception 'ASSERTION_FAILED: B4.2 needs section 13''s WSREL-0002 bundle';
  end if;
  v_pending:=v_row.id;
  if v_row.state<>'PENDING' then
    perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
      v_pending,'Office returned the bundle for the B4.2 measurement',
      'd0000000-0000-4000-8000-000000000001');
  end if;

  select pg_catalog.count(*) into v_heads_before
    from public.weekly_source_entitlement_heads;
  select pg_catalog.count(*) into v_receipts_before
    from private.weekly_source_entitlement_publication_receipts;

  -- B4.3 needs the counter to be NON-ZERO before the frozen run, otherwise
  -- "it neither incremented nor cleared it" is unmeasurable in one direction.
  -- One real transient failure through the real recorder puts it there.
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second',
         last_census_json=coalesce(last_census_json,'{}'::jsonb)-'watch'
   where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc07',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=public.weekly_source_pending_entitlement_release_record_failure_v1(
    pg_catalog.jsonb_build_object(
      'pending_bundle_id',v_pending,'lease_token',v_row.lease_token,
      'worker_id',v_row.lease_owner,'worker_run_id',v_row.lease_worker_run_id,
      'code','WEEKLY_SOURCE_RELEASE_APPLY_FAILED',
      'detail','WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK',
      'sqlstate','40001','failure_kind','DATABASE_ERROR'));
  perform pg_temp.assert_true(
    v_result->>'refusal_disposition'='TRANSIENT'
    and (select technical_failure_count
           from public.weekly_source_pending_entitlement_bundles where id=v_pending)>0,
    'B4.3 setup: one proved-transient failure leaves the counter non-zero: '
      ||(v_result-'detail')::text);

  -- One real full attempt next, so the freeze is proved under the locks and the
  -- watch signature exists.
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc07',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    v_result->>'outcome'='FROZEN' and (v_result->>'released')::boolean is false,
    'B4.2: the measurement starts from a REAL proved freeze: '||(v_result-'census')::text);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_counter_before:=v_row.technical_failure_count;

  -- ---- run A: the behaviour B4.2 reverses ---------------------------------
  select pg_catalog.count(*) into v_audit_start from public.audit_events;
  for v_tick in 1..v_ticks loop
    update public.weekly_source_pending_entitlement_bundles
       set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second',
           last_census_json=last_census_json-'watch'
     where id=v_pending;
    perform private.weekly_source_pending_entitlement_release_claim_page_v1(
      'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc07',60,25);
    select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
    perform pg_temp.assert_true(v_row.state='RELEASING',
      'B4.2 run A tick '||v_tick::text||': the old path claims the bundle every tick');
    v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
      v_pending,v_row.pending_revision,v_row.request_digest,
      v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
    perform pg_temp.assert_true(
      v_result->>'outcome'='FROZEN' and (v_result->>'released')::boolean is false,
      'B4.2 run A tick '||v_tick::text||': still frozen, still nothing released');
  end loop;
  select pg_catalog.count(*) into v_audit_a from public.audit_events;
  v_rows_a:=v_audit_a-v_audit_start;

  -- ---- run B: the shipped behaviour ---------------------------------------
  for v_tick in 1..v_ticks loop
    update public.weekly_source_pending_entitlement_bundles
       set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second'
     where id=v_pending;
    v_claim:=private.weekly_source_pending_entitlement_release_claim_page_v1(
      'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc07',60,25);
    perform pg_temp.assert_true(
      (v_claim->>'claimed_count')::integer=0
      and (v_claim->'watch'->>'unchanged_frozen')::integer=1,
      'B4.2 run B tick '||v_tick::text||': the unchanged freeze is watched, not claimed: '
        ||v_claim::text);
  end loop;
  select pg_catalog.count(*) into v_audit_b from public.audit_events;
  v_rows_b:=v_audit_b-v_audit_a;

  raise notice 'B4.2 MEASURED over % ticks each: run A (pre-ruling path) wrote % audit rows; run B (shipped watch) wrote % audit rows',
    v_ticks,v_rows_a,v_rows_b;

  -- The measurement, not an assertion about it: run A must grow with the run
  -- length and run B must not grow at all.
  perform pg_temp.assert_true(
    v_rows_a>=v_ticks,
    'B4.2: the pre-ruling path must be measured growing at least one audit row per '
      ||'tick, measured '||v_rows_a::text||' over '||v_ticks::text||' ticks');
  perform pg_temp.assert_true(
    v_rows_b=0,
    'B4.2: a repeated unchanged frozen poll must append NO audit row, measured '
      ||v_rows_b::text||' over '||v_ticks::text||' ticks');

  -- The bounded status record did move, and is bounded: one count and two
  -- timestamps on the bundle's own census column.
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    (v_row.last_census_json->'watch'->>'observation_count')::bigint>=v_ticks
    and (v_row.last_census_json->'watch'->>'first_observed_at_utc') is not null
    and (v_row.last_census_json->'watch'->>'last_observed_at_utc') is not null
    and pg_catalog.length((v_row.last_census_json->'watch')::text)<2000,
    'B4.2: the bounded current-state record carries the count and the timestamps and '
      ||'stays bounded: '||coalesce((v_row.last_census_json->'watch')::text,'<null>'));

  -- B4.3, measured across the same long run: eighty frozen observations moved
  -- the technical-failure counter neither up nor down.
  perform pg_temp.assert_true(
    v_row.technical_failure_count=v_counter_before,
    'B4.3: '||(2*v_ticks)::text||' frozen results neither incremented nor cleared the '
      ||'technical-failure counter (it was '||v_counter_before::text||', it is '
      ||v_row.technical_failure_count::text||')');
  perform pg_temp.assert_true(v_counter_before>0,
    'B4.3: the counter must be NON-ZERO for that measurement to mean anything');

  -- And nothing was released by elapsed time, by observation count or by
  -- inference across either run.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts)=v_receipts_before
    and (select state='PENDING'
           from public.weekly_source_pending_entitlement_bundles where id=v_pending),
    'B4.2/B4.3: '||(2*v_ticks)::text||' ticks against a frozen root published no head '
      ||'and wrote no receipt');
end
$verify_b42_audit_growth$;

-- ---------------------------------------------------------------------------
-- 16. HANDOVER 2 round-5 ruling B4.3 - when the counter may be reset
-- ---------------------------------------------------------------------------
-- "A frozen result does not reset the technical-failure counter. It neither
-- increments nor clears it. Reset is allowed only after positively proved
-- successful progress or a new superseding decision/generation."
--
-- Section 15 measured the frozen half over eighty observations.  This section
-- proves the other half: the ONLY writers that set the counter to zero are the
-- two that have positively proved progress or carry a superseding Office
-- decision, and no other path clears it.
do $verify_b43_counter_reset$
declare
  v_pending uuid;
  v_row record;
  v_before integer;
  v_reopen jsonb;
begin
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  v_pending:=v_row.id;
  v_before:=v_row.technical_failure_count;
  perform pg_temp.assert_true(v_before>0,'B4.3: the counter is non-zero before this section');

  -- Taking a fresh lease does not clear it either.
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second',
         last_census_json=last_census_json-'watch'
   where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc07',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    (select technical_failure_count from public.weekly_source_pending_entitlement_bundles
      where id=v_pending)=v_before,
    'B4.3: claiming a bundle does not clear the counter');

  -- =====================================================================
  -- HANDOVER 2 round-7 ruling A1 - WHICH KIND OF REOPEN, NOT MERELY THAT IT
  -- WAS ONE
  -- =====================================================================
  -- "An audited Office reopen may reset the counter only when it creates a new
  --  immutable superseding decision/generation with its own actor, reason and
  --  receipt.  A status-only reopen must not reset it.  Add this as the third
  --  named permitted cause beside the two proved RELEASED paths."
  --
  -- WHAT THIS REPLACED, AND WHY.  Until ruling A1 this section proved the rule
  -- by COUNTING THE STRING `technical_failure_count=0` in
  -- `pg_get_functiondef()` and asserting it appeared exactly three times.  That
  -- is a static assertion over source text (Part 1 executed-review rule 1), and
  -- it could not tell one kind of reopen from another: it passed unchanged
  -- while the audited reopen reset the counter with no generation and no
  -- receipt of its own, AND while a status-only `update … set state='PENDING',
  -- technical_failure_count=0` - no actor, no reason, no generation, no receipt
  -- - was accepted by the database outright.  Both were measured on a build
  -- from empty before this change.  A text count of three writers says nothing
  -- about a fourth writer that is not a function at all.
  --
  -- The replacement EXECUTES each of the three named causes and each of the
  -- refusals, against the installed relation guard.
  --
  -- Cause 3, the neighbouring case that must still succeed: the audited Office
  -- reopen, which now creates the superseding generation first.
  update public.weekly_source_pending_entitlement_bundles
     set state='MANUAL_REVIEW',manual_review_reason='A1 executed proof',
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         pending_revision=pending_revision+1
   where id=v_pending;
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_before:=v_row.technical_failure_count;
  perform pg_temp.assert_true(v_before>0,
    'A1: the counter is non-zero before the reopen limb');
  v_reopen:=public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'A1 executed proof: the Office asks for another attempt',
    'd0000000-0000-4000-8000-000000000001');
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    coalesce((v_reopen->>'ok')::boolean,false)
    and v_row.state='PENDING'
    and v_row.technical_failure_count=0
    -- and it is a SUPERSEDING reopen, not a status-only one: its own
    -- generation, its own predecessor link and its own receipt.
    and (v_reopen->>'reopen_generation')::bigint=v_row.pending_revision
    and (v_reopen->>'supersedes_pending_revision')::bigint=v_row.pending_revision-1
    and (v_reopen->>'reopen_receipt_id') is not null
    and v_reopen->>'counter_reset_cause'='SUPERSEDING_AUDITED_OFFICE_REOPEN'
    and (v_reopen->>'previous_technical_failure_count')::integer=v_before,
    'A1 cause 3: a superseding audited Office reopen still resets the counter, and says '
      ||'which generation it created: '||v_reopen::text);
  -- The receipt it created is a real, findable, attributable record carrying its
  -- own actor and its own reason, and there is exactly one of it.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.audit_events as audit_row
      where audit_row.id=(v_reopen->>'reopen_receipt_id')::uuid
        and audit_row.object_type='weekly_source_pending_entitlement_bundles'
        and audit_row.object_id_text=v_pending::text
        and audit_row.action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED'
        and audit_row.actor_user_id='d0000000-0000-4000-8000-000000000001'
        and pg_catalog.btrim(coalesce(audit_row.reason,''))<>''
        and audit_row.after_json->>'reopen_generation'
            =(v_reopen->>'reopen_generation')
        and audit_row.after_json->>'counter_reset_cause'
            ='SUPERSEDING_AUDITED_OFFICE_REOPEN')=1,
    'A1 cause 3: the reopen created exactly one immutable superseding generation receipt '
      ||'with its own actor and its own reason');

  -- THE CASE THAT MUST NOW BE REFUSED.  A STATUS-ONLY reopen: the same
  -- MANUAL_REVIEW -> PENDING transition, the same zeroing of the counter, but
  -- no superseding generation, no actor, no reason and no receipt.  Before
  -- ruling A1 this was accepted and took the counter 9 -> 0.
  update public.weekly_source_pending_entitlement_bundles
     set state='MANUAL_REVIEW',manual_review_reason='A1 executed proof, status-only limb',
         technical_failure_count=9,
         pending_revision=pending_revision+1
   where id=v_pending;
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set state='PENDING',manual_review_reason=null,
           technical_failure_count=0,
           next_check_at_utc=pg_catalog.clock_timestamp(),
           pending_revision=pending_revision+1
     where manual_review_reason='A1 executed proof, status-only limb';
  $sql$,'55000','WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED',
    'A1: a status-only reopen must not reset the technical-failure counter');
  perform pg_temp.assert_true(
    (select technical_failure_count=9 and state='MANUAL_REVIEW'
       from public.weekly_source_pending_entitlement_bundles where id=v_pending),
    'A1: the refused status-only reopen carried the failure count forward unchanged');

  -- A PARTIAL rewind is a reset this ruling does not name either, so the guard
  -- fires on any decrease rather than only on a write of literal zero.
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set technical_failure_count=1
     where manual_review_reason='A1 executed proof, status-only limb';
  $sql$,'55000','WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED',
    'A1: a partial rewind of the counter is refused as well as a zeroing');

  -- CAUSES 1 AND 2, the two proved RELEASED paths, and the limit on them: a
  -- RELEASED reset must name exactly one INSTALLED publication receipt.  A
  -- RELEASED-shaped write naming a receipt that does not exist is refused, so
  -- "state says RELEASED" is not by itself a licence to reset.
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set state='RELEASED',technical_failure_count=0,
           released_receipt_id='d0000000-0000-4000-8000-0000000000ff',
           released_receipt_digest=pg_catalog.decode(pg_catalog.repeat('5a',32),'hex'),
           released_by_worker_id='not-a-real-worker',
           released_by_worker_run_id='d0000000-0000-4000-8000-0000000000fe',
           released_at_utc=pg_catalog.clock_timestamp(),
           manual_review_reason=null,
           pending_revision=pending_revision+1
     where manual_review_reason='A1 executed proof, status-only limb';
  $sql$,'55000','WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED',
    'A1 causes 1 and 2: a RELEASED reset must name exactly one installed publication receipt');

  -- And the guard governs only a DECREASE: the technical-failure owner's own
  -- increment is untouched by it, which section 10 already exercises end to end.
  update public.weekly_source_pending_entitlement_bundles
     set technical_failure_count=technical_failure_count+1
   where id=v_pending;
  perform pg_temp.assert_true(
    (select technical_failure_count=10
       from public.weekly_source_pending_entitlement_bundles where id=v_pending),
    'A1: the guard governs a decrease only and never blocks an increment');

  -- Finally, the guard is INSTALLED and enabled on the relation, so this proof
  -- cannot silently become a proof about nothing.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from pg_catalog.pg_trigger trigger_row
      where trigger_row.tgrelid='public.weekly_source_pending_entitlement_bundles'::pg_catalog.regclass
        and trigger_row.tgname='weekly_source_pending_counter_reset_guard'
        and not trigger_row.tgisinternal
        and trigger_row.tgenabled='O')=1,
    'A1: the counter-reset guard is installed and enabled on the bundle relation');
end
$verify_b43_counter_reset$;

-- ---------------------------------------------------------------------------
-- 17. HANDOVER 2 round-5 ruling B4.1 - permanent refusals reach a human NOW
-- ---------------------------------------------------------------------------
-- "Every permanent refusal goes immediately to Office manual review... Only
-- genuinely transient technical errors consume the retry budget."  Ruling A1
-- control 5, delivered through WP-02b handoff N1, adds that the coordinator now
-- LABELS a conflicting replay and a tampered row, and that this owner must act
-- on the label rather than re-derive the decision.
--
-- Four distinct permanent codes are driven here, each through the real
-- disposition owner, and each asserted to reach MANUAL_REVIEW on the first
-- refusal with the retry budget untouched.  Two more are proved end to end
-- earlier in this file: `WEEKLY_SOURCE_CENSUS_ERROR` in section 9 and
-- `WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE` in section 11.
do $verify_b41_permanent_refusals$
declare
  v_pending uuid;
  v_row record;
  v_result jsonb;
  v_before integer;
  v_case record;
begin
  for v_case in
    select * from (values
      -- (a) the coordinator's own machine-readable disposition (A1 control 5 /
      --     WP-02b N1).  The code is deliberately one that appears in NO
      --     hand-kept list in this package, so only the LABEL can be deciding.
      ('WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT'::text,
       pg_catalog.jsonb_build_object(
         'detail',pg_catalog.jsonb_build_object(
           'reason','A_COMMITTED_RECEIPT_CARRIES_THIS_DIGEST_WITH_DIFFERENT_IMMUTABLE_FIELDS',
           'integrity_failure',true,'disposition','MANUAL_REVIEW'))),
      -- (b) the tampered stored row, same label, different code and reason.
      ('WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID'::text,
       pg_catalog.jsonb_build_object(
         'detail',pg_catalog.jsonb_build_object(
           'reason','THE_STORED_REQUEST_DOES_NOT_MATCH_ITS_STORED_DIGEST',
           'integrity_failure',true,'disposition','MANUAL_REVIEW'))),
      -- (c) an invariant failure that carries no label at all and was NOT in
      --     the old hand-kept escalation list: the serial-gate bypass.  Before
      --     this ruling it spent the whole budget.
      ('WEEKLY_SOURCE_SERIAL_GATE_BYPASSED'::text,
       pg_catalog.jsonb_build_object('reason','JOB_TYPE_NOT_PINNED','retryable',false)),
      -- (d) a refusal with NO disposition, NO retryability and NO SQLSTATE at
      --     all - the ruling's stated default.
      ('WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'::text,
       pg_catalog.jsonb_build_object('reason','APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION'))
    ) as cases(code,detail)
  loop
    select * into v_row from public.weekly_source_pending_entitlement_bundles
     where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
    v_pending:=v_row.id;
    if v_row.state<>'PENDING' then
      perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
        v_pending,'Office asked for another attempt','d0000000-0000-4000-8000-000000000001');
      select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
    end if;
    v_before:=v_row.technical_failure_count;

    v_result:=private.weekly_source_pending_release_refuse_v1(
      v_pending,v_case.code,v_case.detail);
    perform pg_temp.assert_true(
      (v_result->>'released')::boolean is false
      and v_result->>'outcome'='MANUAL_REVIEW'
      and v_result->>'state'='MANUAL_REVIEW'
      and v_result->>'refusal_disposition'='PERMANENT'
      and (select technical_failure_count
             from public.weekly_source_pending_entitlement_bundles where id=v_pending)=v_before,
      'B4.1: '||v_case.code||' must reach Office manual review on the FIRST refusal with '
        ||'the retry budget untouched: '||(v_result-'detail')::text);
    perform pg_temp.assert_true(
      (select manual_review_reason is not null
         and manual_review_reason::jsonb->>'code'=v_case.code
         from public.weekly_source_pending_entitlement_bundles where id=v_pending),
      'B4.1: the Office-visible reason names '||v_case.code);
  end loop;

  -- The negative that proves the label is what decided (a) and (b): the SAME
  -- code, with the label REMOVED and a proved-transient SQLSTATE reported,
  -- takes the budget instead.
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  v_pending:=v_row.id;
  perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
    v_pending,'Office asked for another attempt','d0000000-0000-4000-8000-000000000001');
  v_result:=private.weekly_source_pending_release_refuse_v1(
    v_pending,'WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT',
    pg_catalog.jsonb_build_object('reason','A_TRANSIENT_LOOKALIKE'),
    null::jsonb,'SQLSTATE=40001; KIND=DATABASE_ERROR');
  perform pg_temp.assert_true(
    v_result->>'refusal_disposition'='TRANSIENT'
    and v_result->>'state'='PENDING'
    and (select technical_failure_count
           from public.weekly_source_pending_entitlement_bundles where id=v_pending)=1,
    'B4.1: the same code with a PROVED transient SQLSTATE and no manual-review label '
      ||'takes the budget, which is what makes the disposition a decision and not a '
      ||'code list: '||(v_result-'detail')::text);

  -- The classifier itself, driven over its whole surface.  Executed, not read.
  perform pg_temp.assert_true(
    private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'40001')
    and private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'40P01')
    and private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'55P03')
    and private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'57014')
    and private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'08006')
    and private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'53200')
    and private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'KIND=TIMEOUT')
    and private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'KIND=NETWORK')
    and private.weekly_source_pending_release_transient_v1(
          'X',pg_catalog.jsonb_build_object('retryable',true),null),
    'B4.1: every proved-transient condition is classified transient');
  perform pg_temp.assert_true(
    not private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,null)
    and not private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'')
    and not private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'23514')
    and not private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'42883')
    and not private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'P0001')
    and not private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'XX000')
    and not private.weekly_source_pending_release_transient_v1('X','{}'::jsonb,'KIND=UNKNOWN')
    -- Part 1 review rule 4: a boolean read from JSON is three-valued.  Absent,
    -- JSON null, the STRING "true" and a number must all read as NOT transient.
    and not private.weekly_source_pending_release_transient_v1(
          'X',pg_catalog.jsonb_build_object('retryable',null),null)
    and not private.weekly_source_pending_release_transient_v1(
          'X','{"retryable":"true"}'::jsonb,null)
    and not private.weekly_source_pending_release_transient_v1(
          'X','{"retryable":1}'::jsonb,null)
    and not private.weekly_source_pending_release_transient_v1(
          'X',pg_catalog.jsonb_build_object('retryable',false),null),
    'B4.1: an unproved condition is PERMANENT, and a three-valued retryable flag never '
      ||'passes on absent, null, a string or a number');
end
$verify_b41_permanent_refusals$;

-- ---------------------------------------------------------------------------
-- 17a. HANDOVER 2 round-7 ruling A5 - FROZEN IS NOT A REFUSAL
-- ---------------------------------------------------------------------------
-- "`FROZEN / WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED` is not a refusal and
--  must never enter the permanence classifier.  It neither increments nor
--  resets the technical-failure counter, publishes a head, writes a completion
--  receipt nor causes immediate manual review.  The permanence classifier
--  begins only after a genuine refused attempt has been produced."
--
-- Section 9 and section 15 already prove the FROZEN PATH does the right thing.
-- This section proves the other half, which nothing proved before: that the
-- CLASSIFIER ITSELF refuses a frozen state.  Measured on a build from empty
-- before ruling A5 was implemented, the classifier accepted
-- `FROZEN / WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED`, returned
-- `refusal_disposition = PERMANENT` and moved the bundle to `MANUAL_REVIEW` on
-- that tick - so this was never a defect that "returns something harmless".
--
-- Both limbs are driven: the frozen state that must now be refused, and the
-- neighbouring genuine `CENSUS_ERROR` - carrying an ACTIVE
-- `WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED` item BESIDE a real census error -
-- which must still escalate immediately under B4.1.
do $verify_a5_frozen_is_not_a_refusal$
declare
  v_pending uuid;
  v_row record;
  v_before record;
  v_result jsonb;
  v_heads bigint;
  v_receipts bigint;
begin
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  v_pending:=v_row.id;
  update public.weekly_source_pending_entitlement_bundles
     set state='PENDING',manual_review_reason=null,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         next_check_at_utc=pg_catalog.clock_timestamp(),
         pending_revision=pending_revision+1
   where id=v_pending;
  select * into v_before from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  select pg_catalog.count(*) into v_heads from public.weekly_source_entitlement_heads;
  select pg_catalog.count(*) into v_receipts
    from private.weekly_source_entitlement_publication_receipts;

  -- Limb 1: the census verdict is FROZEN.  The classifier must refuse to
  -- classify it at all.
  perform pg_temp.expect_failure($sql$
    select private.weekly_source_pending_release_refuse_v1(
      (select id from public.weekly_source_pending_entitlement_bundles
        where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7'),
      'WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED',
      pg_catalog.jsonb_build_object('reason','WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED'),
      pg_catalog.jsonb_build_object('result','FROZEN',
        'reason','WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED'));
  $sql$,'55000','WEEKLY_SOURCE_FROZEN_STATE_IS_NOT_A_REFUSAL',
    'A5: a FROZEN census verdict must never enter the permanence classifier');

  -- Limb 1b: the frozen CODE alone, with no census supplied at all, is refused
  -- on the same ground.  A caller that forgets the census cannot smuggle a
  -- waiting state past the classifier.
  perform pg_temp.expect_failure($sql$
    select private.weekly_source_pending_release_refuse_v1(
      (select id from public.weekly_source_pending_entitlement_bundles
        where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7'),
      'WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED',
      '{}'::jsonb);
  $sql$,'55000','WEEKLY_SOURCE_FROZEN_STATE_IS_NOT_A_REFUSAL',
    'A5: the frozen refusal code alone is refused entry to the classifier too');

  -- Limb 1c: the ONLY writer that increments the counter carries the same
  -- guard, because ruling A5 says a frozen result "neither increments nor
  -- resets" it.
  perform pg_temp.expect_failure($sql$
    select private.weekly_source_pending_release_technical_failure_v1(
      (select id from public.weekly_source_pending_entitlement_bundles
        where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7'),
      'WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED',
      '{}'::jsonb,
      pg_catalog.jsonb_build_object('result','FROZEN'));
  $sql$,'55000','WEEKLY_SOURCE_FROZEN_STATE_IS_NOT_A_REFUSAL',
    'A5: a frozen result may not spend the technical-failure budget either');

  -- Nothing moved: no state transition, no counter movement, no head, no
  -- receipt.  Ruling A5 names all four.
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    v_row.state=v_before.state
    and v_row.technical_failure_count=v_before.technical_failure_count
    and v_row.pending_revision=v_before.pending_revision
    and v_row.manual_review_reason is not distinct from v_before.manual_review_reason
    and (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts)=v_receipts,
    'A5: a refused frozen state published no head, wrote no receipt, caused no manual '
      ||'review and neither incremented nor reset the counter');

  -- Limb 2, the neighbouring case that must STILL succeed.  A genuine refusal
  -- whose census verdict is CENSUS_ERROR still reaches the Office on this tick
  -- under B4.1 - even when it carries an ACTIVE VOID_NOT_YET_PROVED item beside
  -- the real error.  The guard must key on the VERDICT, never on the presence
  -- of that item.
  v_result:=private.weekly_source_pending_release_refuse_v1(
    v_pending,'WEEKLY_SOURCE_CENSUS_ERROR',
    pg_catalog.jsonb_build_object('reason','VOIDED_TERMINAL_UNBINDABLE',
                                  'integrity_failure',true),
    pg_catalog.jsonb_build_object(
      'result','CENSUS_ERROR','reason','WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE',
      'items',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('class','ACTIVE',
          'reason','WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED'),
        pg_catalog.jsonb_build_object('class','CENSUS_ERROR',
          'reason','WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE',
          'pay_batch_item_id','d0000000-0000-4000-8000-0000000000e1'))));
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  perform pg_temp.assert_true(
    v_result->>'refusal_disposition'='PERMANENT'
    and v_result->>'outcome'='MANUAL_REVIEW'
    and v_row.state='MANUAL_REVIEW'
    and v_row.technical_failure_count=v_before.technical_failure_count,
    'A5 limb 2: a genuine CENSUS_ERROR carrying a VOID_NOT_YET_PROVED item beside it '
      ||'still escalates immediately: '||v_result::text);
end
$verify_a5_frozen_is_not_a_refusal$;


-- ---------------------------------------------------------------------------
-- 18. HANDOVER 2 round-5 ruling B4.4 - a census far larger than the old cap
-- ---------------------------------------------------------------------------
-- "The Office reason's 1,000-character summary may remain bounded, but the exact
-- census item identifiers must be retained losslessly in bounded, pageable child
-- records and shown through the Office detail view. Never concatenate an
-- unbounded list into one field and never silently drop the fifth or later
-- identifier."
--
-- WP-08b's review measured the old defect at five items (1,055 characters) and
-- WP-08b's fix moved the cap to 8,000 characters, which still drops items at
-- scale.  This section builds a REAL census with sixty error items - twelve
-- times the count that broke the original cap - drives the real refusal path,
-- and then proves that every one of the sixty identifiers can be read back out
-- of the child relation, a bounded page at a time, with none dropped.
do $verify_b44_lossless_identifiers$
declare
  v_pending uuid;
  v_row record;
  v_result jsonb;
  v_census jsonb;
  v_items constant integer:=60;
  v_index integer;
  v_page jsonb;
  v_offset integer;
  v_pages integer:=0;
  v_seen uuid[]:=array[]::uuid[];
  v_expected uuid[];
  v_reason jsonb;
begin
  -- A cancelled Draft with sixty voided items and no bindable void operation:
  -- the census classes every one of them CENSUS_ERROR / VOID_UNBINDABLE, which
  -- is the same real shape section 9 proved with one item.
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,cancelled_at_utc)
  values ('d0000000-0000-4000-8000-00000000bc01','2026-04-01','CANCELLED','MONZO_CSV','SAGE',
          pg_catalog.clock_timestamp());
  insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id,settlement_status)
  values ('d0000000-0000-4000-8000-00000000bc02','d0000000-0000-4000-8000-00000000bc01',
          'd0000000-0000-4000-8000-000000000003',null);
  -- Exactly the section 9 shape that makes a void UNBINDABLE: a non-terminal
  -- correction request keeps Binding B from proving the void, and its status is
  -- not APPLIED, so Binding A has no core either.  There is no live C4
  -- operation, so the item is CENSUS_ERROR rather than ACTIVE.
  insert into public.pay_payment_correction_requests(
    id,pay_batch_id,correction_kind,status,required_quantity,approved_count,
    selection_json,selection_hash,plan_json,plan_hash,requested_by_user_id
  ) values (
    'd0000000-0000-4000-8000-0000000cc401','d0000000-0000-4000-8000-00000000bc01',
    'PRE_BANK_CANCEL','PLANNED',1,0,'{}'::jsonb,pg_catalog.repeat('b1',32),'{}'::jsonb,
    pg_catalog.repeat('b2',32),'d0000000-0000-4000-8000-000000000001');
  for v_index in 1..v_items loop
    insert into public.pay_batch_items(
      id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,amount_inc_vat)
    values (
      (pg_catalog.md5('b44:item:'||v_index::text)::uuid),
      'd0000000-0000-4000-8000-00000000bc02','TIMESHEET_PAY',
      'd0000000-0000-4000-8000-000000000016','PAYE',true,(10+v_index)::numeric);
  end loop;
  select coalesce(pg_catalog.array_agg((pg_catalog.md5('b44:item:'||ordinal::text)::uuid)
                                       order by ordinal),array[]::uuid[])
    into v_expected
    from pg_catalog.generate_series(1,v_items) as ordinal;

  -- Void the section 13 freeze so the census verdict is the sixty errors and
  -- not the live Draft item.
  update public.pay_batches set status='CANCELLED',cancelled_at_utc=pg_catalog.clock_timestamp()
   where id='d0000000-0000-4000-8000-00000000bb01';
  update public.pay_batch_items set is_voided=true
   where id='d0000000-0000-4000-8000-00000000bb03';

  v_census:=private.weekly_source_freeze_census_v1(
    'd0000000-0000-4000-8000-000000000003',
    array['d0000000-0000-4000-8000-000000000016']::uuid[]);
  perform pg_temp.assert_true(
    v_census->>'result'='CENSUS_ERROR'
    and (select pg_catalog.count(*)
           from pg_catalog.jsonb_array_elements(v_census->'items') as item(value)
          where item.value->>'class'='CENSUS_ERROR')>=v_items,
    'B4.4: the fixture must produce a REAL census with at least '||v_items::text
      ||' error items, got '||coalesce(v_census->>'result','<null>')||' with '
      ||(select pg_catalog.count(*)
           from pg_catalog.jsonb_array_elements(coalesce(v_census->'items','[]'::jsonb))
                as item(value)
          where item.value->>'class'='CENSUS_ERROR')::text);

  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  v_pending:=v_row.id;
  if v_row.state<>'PENDING' then
    perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
      v_pending,'Office asked for another attempt','d0000000-0000-4000-8000-000000000001');
  end if;
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second',
         last_census_json=coalesce(last_census_json,'{}'::jsonb)-'watch'
   where id=v_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc07',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    v_result->>'outcome'='MANUAL_REVIEW'
    and (v_result->>'released')::boolean is false,
    'B4.4: a sixty-item census error reaches Office manual review: '
      ||(v_result-'detail'-'census')::text);
  perform pg_temp.assert_true(
    (v_result->'review_items'->>'item_count')::integer>=v_items
    and (v_result->'review_items'->>'rows_retained')::integer
        =(v_result->'review_items'->>'item_count')::integer,
    'B4.4: every one of the '||v_items::text||' identifiers is RETAINED, not summarised: '
      ||coalesce((v_result->'review_items')::text,'<null>'));

  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_reason:=v_row.manual_review_reason::jsonb;
  -- The summary stays bounded and says so; it is no longer the carrier.
  perform pg_temp.assert_true(
    pg_catalog.length(v_row.manual_review_reason)<=8000
    and (v_reason->>'item_count')::integer>=v_items
    and v_reason->'complete_detail_in'->>'relation'
        ='private.weekly_source_pending_release_review_items',
    'B4.4: the Office summary stays bounded, reports the TRUE count and names the child '
      ||'relation');

  -- Now page the child records the way an Office detail view will, and collect
  -- every identifier.  The page size is clamped inside the function, so an
  -- unbounded page cannot be asked for.
  v_page:=private.weekly_source_pending_release_review_items_page_v1(
    v_pending,null::bigint,0,100000);
  perform pg_temp.assert_true(
    (v_page->>'limit')::integer=200,
    'B4.4: the page size is clamped inside the function, got '
      ||coalesce(v_page->>'limit','<null>'));

  v_offset:=0;
  loop
    v_page:=private.weekly_source_pending_release_review_items_page_v1(
      v_pending,null::bigint,v_offset,7);
    v_pages:=v_pages+1;
    perform pg_temp.assert_true((v_page->>'ok')::boolean,'B4.4: the pager must succeed');
    select v_seen||coalesce(pg_catalog.array_agg((item.value->>'pay_batch_item_id')::uuid
                                                 order by (item.value->>'item_ordinal')::integer),
                            array[]::uuid[])
      into v_seen
      from pg_catalog.jsonb_array_elements(v_page->'items') as item(value)
     where item.value->>'pay_batch_item_id' is not null;
    exit when (v_page->>'has_more')::boolean is not true;
    v_offset:=(v_page->>'next_offset')::integer;
    if v_pages>200 then
      raise exception 'ASSERTION_FAILED: B4.4 pager did not terminate';
    end if;
  end loop;

  perform pg_temp.assert_true(
    v_pages>=9,
    'B4.4: sixty identifiers at seven per page must take at least nine pages, took '
      ||v_pages::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from pg_catalog.unnest(v_expected) as expected(id)
      where not (expected.id=any(v_seen)))=0,
    'B4.4: EVERY one of the '||v_items::text||' exact identifiers must be retrievable '
      ||'through the paged child records, and none silently dropped');
  perform pg_catalog.count(*) from pg_catalog.unnest(v_seen);
  perform pg_temp.assert_true(
    (select pg_catalog.count(distinct seen.id) from pg_catalog.unnest(v_seen) as seen(id))
      >=v_items,
    'B4.4: paging returned every identifier exactly once, with no page boundary losing one');

  -- The child records are append-only evidence: they cannot be edited or
  -- deleted, and a later attempt writes its own generation beside them.
  perform pg_temp.expect_failure(
    $sql$update private.weekly_source_pending_release_review_items
            set item_reason='rewritten'$sql$,
    '55000','WEEKLY_SOURCE_PENDING_RELEASE_REVIEW_ITEM_IMMUTABLE',
    'B4.4: a review item is append-only evidence and cannot be rewritten');
  perform pg_temp.expect_failure(
    $sql$delete from private.weekly_source_pending_release_review_items$sql$,
    '55000','WEEKLY_SOURCE_PENDING_RELEASE_REVIEW_ITEM_IMMUTABLE',
    'B4.4: a review item cannot be deleted');
  perform pg_temp.assert_true(
    not pg_catalog.has_table_privilege('anon',
      'private.weekly_source_pending_release_review_items','SELECT')
    and not pg_catalog.has_table_privilege('authenticated',
      'private.weekly_source_pending_release_review_items','SELECT'),
    'B4.4: no browser role may read the child records');
end
$verify_b44_lossless_identifiers$;

-- ---------------------------------------------------------------------------
-- 19. The three guarantees the independent review confirmed, re-proved
-- ---------------------------------------------------------------------------
-- WP-08b's review answered three questions with "No".  They are the reason this
-- component is trusted unattended, and WP-08c must not have weakened any of
-- them.  Each is re-proved here against the CHANGED owners.
do $verify_b4_three_guarantees$
declare
  v_row record;
  v_pending uuid;
  v_result jsonb;
  v_heads bigint;
  v_receipts bigint;
  v_definitions text;
begin
  -- ---- 1. nothing can be released twice -----------------------------------
  -- The released bundle from section 12 is replayed with its committed digest
  -- after its lease is long gone: it returns the SAME receipt and publishes
  -- nothing new.
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where state='RELEASED' order by released_at_utc limit 1;
  if found then
    v_pending:=v_row.id;
    select pg_catalog.count(*) into v_heads from public.weekly_source_entitlement_heads;
    select pg_catalog.count(*) into v_receipts
      from private.weekly_source_entitlement_publication_receipts;
    v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
      v_pending,v_row.pending_revision,v_row.request_digest,
      'weekly-source-release-worker',pg_catalog.gen_random_uuid(),
      pg_catalog.gen_random_uuid());
    perform pg_temp.assert_true(
      (v_result->>'replayed')::boolean and (v_result->>'released')::boolean
      and (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads
      and (select pg_catalog.count(*)
             from private.weekly_source_entitlement_publication_receipts)=v_receipts,
      'GUARANTEE 1: a replay of a released bundle returns the committed receipt and '
        ||'publishes nothing a second time: '||(v_result-'receipt')::text);
    -- And a RELEASED bundle is never claimed again, by the watch or the claim.
    update public.weekly_source_pending_entitlement_bundles
       set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 hour' where id=v_pending;
    v_result:=private.weekly_source_pending_entitlement_release_claim_page_v1(
      'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc09',60,25);
    perform pg_temp.assert_true(
      not exists(select 1 from pg_catalog.jsonb_array_elements(v_result->'bundles')
                      as claimed(value)
                  where (claimed.value->>'pending_bundle_id')::uuid=v_pending)
      and not exists(select 1
                       from pg_catalog.jsonb_array_elements(v_result->'watch'->'bundles')
                            as watched(value)
                      where (watched.value->>'pending_bundle_id')::uuid=v_pending),
      'GUARANTEE 1: a RELEASED bundle is claimed by neither the claim page nor the new '
        ||'frozen watch: '||v_result::text);
  else
    raise notice 'NOT CHECKED: no RELEASED bundle exists in this run';
  end if;

  -- ---- 2. nothing frozen can be released ----------------------------------
  -- The new watch is the only code path WP-08c added that runs on a frozen
  -- bundle unattended, so the proof is twofold: the watch owner contains no
  -- publication call at all, and eighty measured frozen observations in
  -- section 15 released nothing.  The first half is proved here from the
  -- installed definition, because a negative cannot be executed.
  v_definitions:=pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
    'private.weekly_source_pending_release_watch_page_v1(text,uuid,integer)'));
  perform pg_temp.assert_true(
    v_definitions !~* 'weekly_source_entitlement_publish'
    and v_definitions !~* 'weekly_source_entitlement_heads'
    and v_definitions !~* 'weekly_source_entitlement_publication_receipts'
    and v_definitions !~* 'pay_workbench_scope_invalidate'
    and v_definitions !~* '\bstate=''RELEAS'
    and v_definitions !~* 'pay_batch|banking_pay|timesheets_financials',
    'GUARANTEE 2: the frozen watch contains no publication, no head, no receipt, no '
      ||'Workbench invalidation, no state promotion and no Banking Pay write');
  -- Executed: the watch owner, called directly on the frozen bundle, releases
  -- nothing and moves no state.
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  select pg_catalog.count(*) into v_heads from public.weekly_source_entitlement_heads;
  v_result:=private.weekly_source_pending_release_watch_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc09',25);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads
    and (select state from public.weekly_source_pending_entitlement_bundles where id=v_row.id)
        =v_row.state,
    'GUARANTEE 2: the frozen watch released nothing and promoted no state: '||v_result::text);
  -- And the coordinator is still reached from exactly one place, in DEFERRED
  -- mode, behind a RELEASABLE census.
  v_definitions:=pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
    'private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid)'));
  perform pg_temp.assert_true(
    (pg_catalog.length(v_definitions)
     -pg_catalog.length(pg_catalog.replace(v_definitions,'weekly_source_entitlement_publish_core_v1','')))
    /pg_catalog.length('weekly_source_entitlement_publish_core_v1')=1
    and pg_catalog.strpos(v_definitions,'v_census_result<>''RELEASABLE''')
        <pg_catalog.strpos(v_definitions,'weekly_source_entitlement_publish_core_v1'),
    'GUARANTEE 2: the one coordinator call site still sits AFTER the RELEASABLE gate');

  -- ---- 3. nothing can be released without a human decision behind it ------
  -- Every release still revalidates the accepted Office decision under the
  -- locks.  Executed: strip the decision and the release refuses.
  select * into v_row from public.weekly_source_pending_entitlement_bundles
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  v_pending:=v_row.id;
  if v_row.state<>'PENDING' then
    perform public.weekly_source_pending_entitlement_bundle_reopen_v1(
      v_pending,'Office asked for another attempt','d0000000-0000-4000-8000-000000000001');
  end if;
  update public.weekly_source_entitlement_decision_bundles
     set state='SUPERSEDED',superseded_at_utc=pg_catalog.clock_timestamp()
   where decision_bundle_id='d0000000-0000-4000-8000-0000000000b7';
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second',
         last_census_json=coalesce(last_census_json,'{}'::jsonb)-'watch'
   where id=v_pending;
  select pg_catalog.count(*) into v_heads from public.weekly_source_entitlement_heads;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source-release-worker','d0000000-0000-4000-8000-00000000cc09',60,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=v_pending;
  v_result:=private.weekly_source_pending_entitlement_release_apply_v1(
    v_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
  perform pg_temp.assert_true(
    (v_result->>'released')::boolean is false
    and v_result->>'outcome'='SUPERSEDED'
    and (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads,
    'GUARANTEE 3: a bundle whose accepted Office decision no longer stands is never '
      ||'released: '||(v_result-'detail')::text);
  -- The watch honours the same rule: it escalates rather than keep waiting on a
  -- decision that no longer stands, and it never decides anything itself.
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
      'private.weekly_source_pending_release_watch_page_v1(text,uuid,integer)'))
      like '%DECISION_NO_LONGER_STANDS%',
    'GUARANTEE 3: the frozen watch re-checks the accepted Office decision on every poll');
  -- And no release path anywhere in this package takes an actor or a timestamp
  -- from its caller.
  select pg_catalog.string_agg(pg_catalog.pg_get_functiondef(p.oid),E'\n' order by p.oid)
    into v_definitions
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
   where n.nspname in ('public','private')
     and p.proname like 'weekly\_source\_pending\_%' escape '\';
  perform pg_temp.assert_true(
    v_definitions !~* '\bp_now_utc\b|\bp_decided_by_user_id\b|\bp_authorised_by\b',
    'GUARANTEE 3: no release owner accepts an actor or a timestamp from its caller');
end
$verify_b4_three_guarantees$;

-- ---------------------------------------------------------------------------
-- 14. The Banking Pay evidence is byte for byte what it was (section 11)
-- ---------------------------------------------------------------------------
do $verify_release_banking_untouched$
begin
  perform pg_temp.assert_true(
    (select item_row.amount_inc_vat=100.00 and item_row.pay_channel='PAYE'
       and item_row.timesheet_id='d0000000-0000-4000-8000-000000000006'
       from public.pay_batch_items item_row
      where item_row.id='d0000000-0000-4000-8000-00000000ba03'),
    'section 11: the release owner changed no frozen Draft item, amount or channel');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.pay_advance_reservations reservation_row
      where reservation_row.pay_batch_id='d0000000-0000-4000-8000-00000000ba01'
         or reservation_row.pay_batch_item_id='d0000000-0000-4000-8000-00000000ba03')=0
    and (select pg_catalog.count(*) from public.pay_bank_transfers transfer_row
          where transfer_row.pay_batch_id='d0000000-0000-4000-8000-00000000ba01')=0,
    'section 11: the release owner created no reservation and no transfer for this family');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_batch_change_signals signal_row
      where signal_row.pay_batch_id='d0000000-0000-4000-8000-00000000ba01')=1,
    '24 section 4.4: the only Banking Pay row this package writes is one stale-warning signal');
end
$verify_release_banking_untouched$;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_pending_entitlement_release_v1',
  'scenarios',pg_catalog.jsonb_build_array(
    'request_json-structure','request_json-identity-not-lifecycle','request_json-negatives',
    'I-5-save','I-5-idempotent','I-5-approval-digest-refusal','I-5-control-scope-replay',
    'I-5-census-not-frozen','I-5-stale-warning',
    'R1','R3','R5','R6','R13','R14','R22','R23-in-fixtures','R24','R27','R32','R33',
    'R42-BLOCKED',
    'R10/R39-replay-conflict',
    'round4-ruling3-both-states','round4-ruling4-manual-review-reason',
    'round4-ruling6-point7-rollback-record','G5-6-audited-reopen',
    'D10-stored-request-and-digest-pairing',
    -- HANDOVER 2 round-5 ruling A3, package WP-25.  These three replace the
    -- retired 'WP-07b-no-withdrawal-after-publication' scenario, whose rule the
    -- approver reversed.
    'A3-later-decision-still-refuses-and-the-head-is-not-the-reason',
    'A3-payment-effect-across-the-boundary-refuses-and-supersedes-nothing',
    'A3-no-payment-effect-supersedes-the-committed-head-atomically',
    'F1-under-lock-lease-recheck-precedes-every-refusal-write',
    'F3-office-reason-keeps-every-item-identifier',
    'proof-32-section-11-by-search','proof-32-section-10-bounds',
    -- HANDOVER 2 round-5 ruling B4, package WP-08c.
    'B4.1-permanent-refusal-reaches-a-human-on-the-first-tick',
    'B4.1-four-distinct-permanent-codes',
    'B4.1-coordinator-disposition-label-honoured-A1-control-5',
    'B4.1-transient-sqlstate-and-timeout-still-spend-the-budget',
    'B4.1-ten-transient-failures-still-reach-manual-review',
    'B4.1-transient-classifier-driven-over-its-whole-surface',
    'B4.2-long-frozen-run-audit-rows-MEASURED-both-ways',
    'B4.2-watch-escalates-on-a-changed-census',
    'B4.2-bounded-current-state-counters-and-timestamps',
    'B4.3-frozen-neither-increments-nor-clears-the-counter',
    'B4.3-only-proved-progress-or-a-superseding-decision-resets-it',
    'B4.4-sixty-item-census-every-identifier-retained',
    'B4.4-identifiers-paged-out-of-the-child-relation',
    'B4.4-child-records-are-append-only-and-browser-invisible',
    -- HANDOVER 2 round-7 Part 5 rulings, all EXECUTED rather than text-searched.
    'R7-A1-superseding-audited-reopen-still-resets-with-its-own-generation-and-receipt',
    'R7-A1-status-only-reopen-REFUSED-at-the-relation',
    'R7-A1-partial-rewind-of-the-counter-REFUSED',
    'R7-A1-RELEASED-reset-must-name-one-installed-publication-receipt',
    'R7-A1-counter-reset-guard-installed-and-enabled',
    'R7-A5-frozen-verdict-never-enters-the-permanence-classifier',
    'R7-A5-frozen-never-spends-the-technical-failure-budget',
    'R7-A5-genuine-CENSUS_ERROR-beside-a-VOID_NOT_YET_PROVED-item-still-escalates',
    'R7-A6-trim-whitespace-split-is-BOOKING_REFERENCE_CANONICAL_COLLISION',
    'R7-A6-other-non-canonical-shapes-keep-their-own-precise-reason',
    'GUARANTEE-1-nothing-released-twice',
    'GUARANTEE-2-nothing-frozen-released',
    'GUARANTEE-3-nothing-released-without-a-human-decision'),
  'interfaces_used',pg_catalog.jsonb_build_object(
    'I-1','the installed WP-03 helper',
    'I-2','the installed WP-08a census',
    'I-4','the installed WP-02 coordinator, in DEFERRED mode'),
  'scaffolding','none'
) as weekly_source_pending_entitlement_release_verification;

rollback;
