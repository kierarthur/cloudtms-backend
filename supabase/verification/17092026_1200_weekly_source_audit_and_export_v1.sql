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
\ir support/06102026_1117_source_workbench_fixture_isolation.sql
-- Transaction-local fixture accounting; existing customer rows are not an empty-table precondition.
\ir support/22092026_1850_source_fixture_capture.sql
select pg_temp.ws_verify_watch('public.banking_pay_workbench_jobs'::regclass);
select pg_temp.ws_verify_watch('public.weekly_manager_recipient_routes'::regclass);
select pg_temp.ws_verify_watch('public.weekly_message_dispatch_targets'::regclass);
select pg_temp.ws_verify_watch('public.weekly_message_intents'::regclass);
select pg_temp.ws_verify_watch('public.weekly_message_renders'::regclass);

set local request.jwt.claim.role='service_role';
-- Normal request-end seam for pre-existing verifier facts; no old worker jobs run.
create temporary table bpspv_existing_setup_state(snapshot_json jsonb) on commit drop;
do $presentation_existing_request_boundary$
declare
 v_old_finalising text;v_old_scope_token text;
 v_jobs_before jsonb;v_jobs_after jsonb;v_bank_before jsonb;v_bank_after jsonb;
 v_economic_before jsonb;v_economic_after jsonb;v_relation text;v_hash text;
begin
 if exists(select 1 from public.banking_pay_workbench_sessions where status='OPEN' and discarded_at_utc is null)
   or exists(select 1 from public.banking_pay_workbench_session_scope where candidate_id='b8550000-0000-4000-8000-000000000003'::uuid)
   or exists(select 1 from public.banking_pay_workbench_jobs where status='RUNNING')
   or exists(select 1 from public.banking_pay_workbench_candidate_delta_projection_runs where status in ('RUNNING','PROCESSING','IN_PROGRESS')) then
   raise exception using errcode='P0001',message='PRESENTATION_EXISTING_ACTIVE_LANE_NOT_QUIET';
 end if;
 select coalesce(jsonb_agg(to_jsonb(j) order by j.id),'[]'::jsonb) into v_jobs_before from public.banking_pay_workbench_jobs j;
 v_bank_before:=pg_temp.ws_verify_workbench_fingerprint();
  v_economic_before:='{}'::jsonb;
  foreach v_relation in array array['public.timesheets','public.timesheets_financials','public.weekly_source_billing_movements','public.weekly_source_projection_publications','public.weekly_source_ordinary_pay_projection_receipts','public.weekly_source_root_authorisations','public.invoices','public.pay_advances','public.pay_finance_case_components','public.pay_batches','public.banking_pay_operations'] loop
    execute format('select md5(coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),''[]''::jsonb)::text) from %s x',v_relation) into v_hash;
    v_economic_before:=v_economic_before||jsonb_build_object(v_relation,v_hash);
  end loop;
  -- BEGIN V9 EXACT INSTALLED NAMED FINALIZER
  -- Same canonical cf8 baseline; root readback6538cf pins this unchanged owner.
  if (select count(*) from pg_catalog.pg_proc p
      join pg_catalog.pg_language l on l.oid=p.prolang
      where p.oid=to_regprocedure('public.pay_workbench_scope_change_finalize_trg_v1()')
        and p.proowner=current_user::regrole and p.prosecdef
        and p.prokind='f' and p.pronargs=0 and p.prorettype='pg_catalog.trigger'::regtype
        and p.provolatile='v' and p.proparallel='u' and l.lanname='plpgsql'
        and p.proconfig=array['search_path=public, pg_catalog']::text[]
        and md5(p.prosrc)='b1e01887b71866f545b62dd3b2cb658e'
        and md5(pg_catalog.pg_get_functiondef(p.oid))='03b772a328097a1206446fbd2038a296'
        and (select count(*) from pg_catalog.aclexplode(p.proacl) a)=2
        and not exists(select 1 from pg_catalog.aclexplode(p.proacl) a
          where a.privilege_type<>'EXECUTE' or a.is_grantable
            or a.grantor<>p.proowner
            or a.grantee not in(p.proowner,'service_role'::regrole))
        and has_function_privilege(current_user,p.oid,'EXECUTE')
        and has_function_privilege('service_role',p.oid,'EXECUTE')
        and not has_function_privilege('anon',p.oid,'EXECUTE')
        and not has_function_privilege('authenticated',p.oid,'EXECUTE'))<>1
     or (select count(*) from pg_catalog.pg_trigger t
       where t.tgrelid='public.banking_pay_scope_change_transactions'::regclass
         and t.tgname='trg_pay_workbench_scope_change_finalize_v1'
         and t.tgfoid=to_regprocedure('public.pay_workbench_scope_change_finalize_trg_v1()')
         and not t.tgisinternal and t.tgenabled='O' and t.tgtype=5
         and t.tgdeferrable and t.tginitdeferred and t.tgqual is null
         and t.tgnargs=0 and octet_length(t.tgargs)=0)<>1 then
    raise exception using errcode='P0001',message='PAID_FIXTURE_NAMED_FINALIZER_POSTURE_NOT_EXACT';
  end if;
  -- END V9 EXACT INSTALLED NAMED FINALIZER

  -- BEGIN V9 PREWORKER REAL REQUEST BOUNDARY
  -- Registered3108 models request COMMIT with this named real trigger only.
  -- These two transaction-local resets are not authorisation/payment gates.
  v_old_finalising:=current_setting('cloudtms.scope_generation_finalising',true);
  v_old_scope_token:=current_setting('cloudtms.banking_pay_scope_tx_token',true);
  if coalesce(v_old_finalising,'') not in('','false')
     or (coalesce(v_old_scope_token,'')<>'' and (
       v_old_scope_token !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or not exists(select 1 from public.banking_pay_scope_change_transactions s
         where s.tx_token::text=v_old_scope_token and s.state='PENDING'))) then
    raise exception using errcode='P0001',message='PAID_FIXTURE_PREWORKER_SCOPE_CONTEXT_NOT_EXACT';
  end if;
  execute 'SET CONSTRAINTS public.trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  perform set_config('cloudtms.scope_generation_finalising','false',true);
  perform set_config('cloudtms.banking_pay_scope_tx_token','',true);
  execute 'SET CONSTRAINTS public.trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  if current_setting('cloudtms.scope_generation_finalising',true)<>'false'
     or current_setting('cloudtms.banking_pay_scope_tx_token',true)<>''
     or (coalesce(v_old_scope_token,'')<>'' and not exists(
       select 1 from public.banking_pay_scope_change_transactions s
       where s.tx_token::text=v_old_scope_token and s.state in('FINALIZED','NOOP'))) then
    raise exception using errcode='P0001',message='PAID_FIXTURE_PREWORKER_SCOPE_NOT_FINALIZED';
  end if;
  -- END V9 PREWORKER REAL REQUEST BOUNDARY

 select coalesce(jsonb_agg(to_jsonb(j) order by j.id),'[]'::jsonb) into v_jobs_after from public.banking_pay_workbench_jobs j;
 v_bank_after:=pg_temp.ws_verify_workbench_fingerprint();
  v_economic_after:='{}'::jsonb;
  foreach v_relation in array array['public.timesheets','public.timesheets_financials','public.weekly_source_billing_movements','public.weekly_source_projection_publications','public.weekly_source_ordinary_pay_projection_receipts','public.weekly_source_root_authorisations','public.invoices','public.pay_advances','public.pay_finance_case_components','public.pay_batches','public.banking_pay_operations'] loop
    execute format('select md5(coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),''[]''::jsonb)::text) from %s x',v_relation) into v_hash;
    v_economic_after:=v_economic_after||jsonb_build_object(v_relation,v_hash);
  end loop;
 if v_economic_after is distinct from v_economic_before or v_bank_after is distinct from v_bank_before
   or (select coalesce(jsonb_agg(r->'id' order by r->>'id'),'[]'::jsonb) from jsonb_array_elements(v_jobs_before) r)
      is distinct from (select coalesce(jsonb_agg(r->'id' order by r->>'id'),'[]'::jsonb) from jsonb_array_elements(v_jobs_after) r)
   or exists(select 1 from jsonb_array_elements(v_jobs_before) old_row
      join public.banking_pay_workbench_jobs j on j.id=(old_row->>'id')::uuid
      where to_jsonb(j) is distinct from old_row and (
        (to_jsonb(j)-array['scope_change_generation','scope_change_tx_token','payload_json','updated_at_utc'])
          is distinct from (old_row-array['scope_change_generation','scope_change_tx_token','payload_json','updated_at_utc'])
        or jsonb_typeof(old_row->'payload_json') is distinct from 'object'
        or (j.payload_json-'scope_change_generation') is distinct from ((old_row->'payload_json')-'scope_change_generation')
        or j.scope_change_tx_token is not null
        or upper(btrim(coalesce(j.job_type,'')))='WORKBENCH_SCOPE_RECONCILE'
        or not exists(select 1 from public.banking_pay_scope_change_transactions s
          where s.tx_token::text=old_row->>'scope_change_tx_token' and s.state='FINALIZED'
            and s.allocated_generation>0
            and j.scope_change_generation=greatest(coalesce((old_row->>'scope_change_generation')::bigint,0),s.allocated_generation)
            and j.payload_json->'scope_change_generation'=to_jsonb(s.allocated_generation))
      )) then
   raise exception using errcode='P0001',message='PRESENTATION_EXISTING_REQUEST_BOUNDARY_NON_METADATA_DRIFT';
 end if;
 insert into pg_temp.bpspv_existing_setup_state values(jsonb_build_object('jobs',v_jobs_after,'bank',v_bank_after));
end $presentation_existing_request_boundary$;

-- BEGIN GENUINE CERTIFICATE CAPSULE
-- E-only genuine variants. Include inside reviewed root BEGIN/ROLLBACK only.
-- Requires the separately reviewed genuine capture proposal + actual due-worker setup.
-- Native NOT_RUN. No module switch, status stamp, arbitrary head/hash seed or external effect.
create function pg_temp.bpsx_assert(p_ok boolean,p_message text)
returns void language plpgsql as $f$
begin if p_ok is distinct from true then raise exception 'BPAY_NEXT_SOURCE_IMPORT_ASSERT: %',p_message; end if; end $f$;
do $bpsx_environment$ begin
 perform pg_temp.bpsx_assert(exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY'),
   'PRESENTATION_VARIANTS_INACTIVE_FINANCIAL_OWNER_REQUIRED');
 perform pg_temp.bpsx_assert(not exists(select 1 from public.candidates where id='b8560000-0000-4000-8000-000000000003')
   and not exists(select 1 from public.tms_users where id='b8560000-0000-4000-8000-000000000001'),
   'PRESENTATION_VARIANTS_EXACT_NAMESPACE_ABSENT');
end $bpsx_environment$;
insert into public.settings_defaults(id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256)
values(1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')) on conflict(id) do nothing;
insert into public.settings_finance_windows(id,date_from,erni_pct,vat_rate_pct,holiday_pay_pct)
select 'b8560000-0000-4000-8000-000000000080','2026-01-01',15,20,0
where not exists(select 1 from public.settings_finance_windows
  where '2026-09-16'::date between date_from and coalesce(date_to,'infinity'::date));
insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser)
values('b8560000-0000-4000-8000-000000000001','presentation-variants-owner@example.test','admin',true,'not-a-login',true);
insert into public.candidates(id,display_name)
values('b8560000-0000-4000-8000-000000000003','PRESENTATION variants Source worker');
insert into public.clients(id,name) values
 ('b8560000-0000-4000-8000-000000000002','NEXT imported Magnit-style roster'),
 ('b8560000-0000-4000-8000-000000000012','PRESENTATION variants NHSP Trust');
insert into public.client_settings(client_id,vat_rate_pct,effective_from,is_nhsp,requires_hr,no_timesheet_required,autoprocess_hr)
values
 ('b8560000-0000-4000-8000-000000000002',20,'2026-01-01',false,true,true,true),
 ('b8560000-0000-4000-8000-000000000012',20,'2026-01-01',true,false,false,false);
insert into public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp,week_ending_weekday_snapshot)
values
 ('b8560000-0000-4000-8000-000000000004','b8560000-0000-4000-8000-000000000003',
  'b8560000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
  '{"paye_day":20,"paye_night":20,"paye_sat":20,"paye_sun":20,"paye_bh":20,"umb_day":20,"umb_night":20,"umb_sat":20,"umb_sun":20,"umb_bh":20,"charge_day":40,"charge_night":40,"charge_sat":40,"charge_sun":40,"charge_bh":40}',
  'HEALTHROSTER',true,true,true,true,false,3),
 ('b8560000-0000-4000-8000-000000000014','b8560000-0000-4000-8000-000000000003',
  'b8560000-0000-4000-8000-000000000012','2026-01-01','2026-12-31','PAYE',
  '{"paye_day":20,"paye_night":20,"paye_sat":20,"paye_sun":20,"paye_bh":20,"umb_day":20,"umb_night":20,"umb_sat":20,"umb_sun":20,"umb_bh":20,"charge_day":40,"charge_night":40,"charge_sat":40,"charge_sun":40,"charge_bh":40}',
  null,true,false,false,false,false,0);
insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time,nhsp_report_heading_name)
values
 ('b8560000-0000-4000-8000-000000000005','TEST','b8560000-0000-4000-8000-000000000006',
  'BPAY_NEXT_IMPORT_ROSTER','NEXT imported roster','ROSTER',3,'15:00',null),
 ('b8560000-0000-4000-8000-000000000015','TEST','b8560000-0000-4000-8000-000000000006',
  'BPAY_NEXT_IMPORT_NHSP','NEXT imported NHSP','NHSP',3,'18:00','PRESENTATION variants NHSP Trust');
insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
values
 ('b8560000-0000-4000-8000-000000000005','b8560000-0000-4000-8000-000000000002','2026-01-01','b8560000-0000-4000-8000-000000000001'),
 ('b8560000-0000-4000-8000-000000000015','b8560000-0000-4000-8000-000000000012','2026-01-01','b8560000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,self_bill_correction_presentation,source_fixed_expenses_enabled,source_expense_vat_enabled,
  weekly_rate_classification_method,manager_queries_enabled,manager_query_recipient,created_by_user_id)
values
 ('b8560000-0000-4000-8000-000000000005','b8560000-0000-4000-8000-000000000002','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'FULL_REVERSAL_REPLACEMENT',true,false,'SPLIT_RATE_WINDOWS',true,'next-source-manager@example.test','b8560000-0000-4000-8000-000000000001'),
 ('b8560000-0000-4000-8000-000000000015','b8560000-0000-4000-8000-000000000012','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'FULL_REVERSAL_REPLACEMENT',false,false,'SPLIT_RATE_WINDOWS',true,'next-source-manager@example.test','b8560000-0000-4000-8000-000000000001');



-- BEGIN PAID ROOT OWNED SETUP FANOUT COMPLETION V1
-- E-only proposal: finish actual fixture-created LEGACY continuation work.
-- No manual job transition, new authority, financial patch or retry.
do $paid_root_owned_setup_fanout$
declare
  v_candidate constant uuid := 'b8560000-0000-4000-8000-000000000003';
  v_expected_scopes constant text[] := array[
    'CLIENT:b8560000-0000-4000-8000-000000000002',
    'CLIENT:b8560000-0000-4000-8000-000000000012',
    'CONTRACT:b8560000-0000-4000-8000-000000000004',
    'CONTRACT:b8560000-0000-4000-8000-000000000014'
  ];
  v_jobs uuid[];
  v_scopes text[];
  v_job_count integer;
  v_result jsonb;
  v_before jsonb := '{}'::jsonb;
  v_after jsonb := '{}'::jsonb;
  v_relation text;
  v_hash text;
  v_claim_now timestamptz;
  v_eligible uuid[];
  v_other_jobs jsonb;
  v_other_jobs_after jsonb;
  v_owned uuid[];
  v_ordinary uuid[];
  v_call integer;
  v_all_ids uuid[];
  v_owned_count integer;
  v_prior_jobs jsonb;
  v_bank_before jsonb;
  v_bank_after jsonb;
  v_expected_all_ids uuid[];
  -- BEGIN V9 NAMED BOUNDARY DECLARATIONS
  v_pending_count integer;
  v_old_finalising text;
  v_old_scope_token text;
  -- END V9 NAMED BOUNDARY DECLARATIONS
begin
  select s.snapshot_json->'jobs',s.snapshot_json->'bank' into strict v_prior_jobs,v_bank_before
    from pg_temp.bpspv_existing_setup_state s;
  if exists(select 1 from public.timesheets t join public.contracts c on c.id=t.contract_id
    where c.candidate_id=v_candidate)
    or exists(select 1 from public.timesheets_financials f where f.candidate_id=v_candidate) then
    raise exception using errcode='P0001',message='PRESENTATION_FACTS_ONLY_BEFORE_IMPORT_REQUIRED';
  end if;
  if (select active_owner from private.bpay_next_module_control where id=1)
       is distinct from 'LEGACY'
     or exists(select 1 from public.banking_pay_workbench_sessions where status='OPEN' and discarded_at_utc is null)
     or exists(select 1 from public.banking_pay_workbench_session_scope where candidate_id='b8550000-0000-4000-8000-000000000003'::uuid)
     or exists(select 1 from public.banking_pay_workbench_candidate_source_lines where candidate_id=v_candidate)
     or exists(select 1 from public.banking_pay_workbench_candidate_delta_projection_runs
       where status in ('RUNNING','PROCESSING','IN_PROGRESS'))
     or exists(select 1 from public.banking_pay_workbench_jobs where status='RUNNING') then
    raise exception using errcode='P0001',message='PAID_FIXTURE_FANOUT_NOT_QUIET';
  end if;

  -- Contract INSERTs populate candidate_ids; the preceding client_settings
  -- INSERT coalesces into the same CLIENT dedupe key through the real merger.
  select array_agg(j.id order by j.id),count(*)::integer,
         array_agg((j.payload_json->>'scope_kind')||':'||(j.payload_json->>'scope_id')
           order by (j.payload_json->>'scope_kind')||':'||(j.payload_json->>'scope_id'))
    into v_jobs,v_job_count,v_scopes
    from public.banking_pay_workbench_jobs j
   where j.status='QUEUED'
     and j.job_type='CONTRACT_CLIENT_DIRTY_FANOUT'
     and j.session_id is null and j.candidate_id is null
     and public._pay_workbench_candidate_serial_candidate_id(j.candidate_id,j.payload_json)=v_candidate
     and j.payload_json->'candidate_ids'=jsonb_build_array(v_candidate::text)
     and j.payload_json->>'queue_class'='DIRTY_TRIGGER_PRIORITY'
     and j.payload_json->>'trigger_table'='contracts'
     and j.payload_json->>'trigger_op'='INSERT'
     and j.payload_json->>'reason'='DIRTY_TRIGGER:CONTRACTS:INSERT'
     and j.payload_json->>'source_build_required'='true'
     and j.payload_json->>'fallback_reason'='CONTRACT_CLIENT_OR_CLIENT_SETTINGS_DIRTY'
     and j.dedupe_key='DIRTY_TRIGGER:CONTRACT_CLIENT_DIRTY_FANOUT:'
       ||(j.payload_json->>'scope_kind')||':'||(j.payload_json->>'scope_id');
  if v_job_count<>4 or v_scopes is distinct from v_expected_scopes then
    raise exception using errcode='P0001',message='PAID_FIXTURE_FANOUT_PROVENANCE_NOT_EXACT';
  end if;

  -- BEGIN V7 ALL TWELVE OWNED SETUP JOBS
  -- The initial empty job namespace is checked before the genuine fixture.
  -- Its four contract/client fanouts and eight Candidate dirty jobs are all
  -- setup work. The worker may interleave and genuinely requeue either type.
  select array_agg(j.id order by j.id) into v_ordinary
    from public.banking_pay_workbench_jobs j
   where j.status='QUEUED' and j.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
     and j.session_id is null and j.snapshot_run_id is null
     and j.candidate_id=v_candidate
     and j.payload_json->>'candidate_id'=v_candidate::text
     and j.payload_json->>'scope_kind'='CANDIDATE'
     and j.payload_json->>'scope_id'=v_candidate::text
     and j.payload_json->>'queue_class'='DIRTY_TRIGGER_PRIORITY'
     and j.payload_json->>'policy_x_authority_scope'='PRE_DRAFT_LIVE_TRUTH'
     and j.payload_json->>'policy_x_dirtying_only'='true'
     and j.payload_json->>'economic_truth_mutation_allowed'='false'
     and j.payload_json->>'trigger_table'='candidates'
     and j.payload_json->>'trigger_op'='INSERT'
     and j.payload_json->>'reason_latest'=
       'DIRTY_TRIGGER:'||upper(j.payload_json->>'trigger_table')||':'||(j.payload_json->>'trigger_op')
     and j.payload_json->'targeted_timesheet_ids'='[]'::jsonb
     and j.payload_json->'linked_timesheet_ids'='[]'::jsonb
     and not exists(
       select 1 from jsonb_array_elements_text(
         (j.payload_json->'targeted_timesheet_ids')||(j.payload_json->'linked_timesheet_ids')) target(id)
       where not exists(select 1 from public.timesheets t join public.contracts c on c.id=t.contract_id
         where t.timesheet_id::text=target.id and c.candidate_id=v_candidate))
     and j.dedupe_key='DIRTY_TRIGGER:WORKBENCH_CANDIDATE_DIRTY_APPLY:CANDIDATE:'
       ||v_candidate::text||':TIMESHEETS:'||
       case when jsonb_array_length(j.payload_json->'targeted_timesheet_ids')=0 then 'ALL'
         else (select string_agg(target.id,',' order by target.id)
           from jsonb_array_elements_text(j.payload_json->'targeted_timesheet_ids') target(id)) end;
  -- Enabled AFTER Candidate INSERT + the canonical ALL dedupe admit one
  -- facts-only owner, not the paid fixture's eight targeted Timesheet jobs.
  if cardinality(v_ordinary) is distinct from 1 then
    raise exception using errcode='P0001',message='PAID_FIXTURE_TWELVE_PROVENANCE_NOT_EXACT';
  end if;
  select array_agg(id order by id) into v_owned
    from unnest(v_jobs||v_ordinary) id;
  v_owned_count:=cardinality(v_jobs)+cardinality(v_ordinary);
  select array_agg(id order by id) into v_expected_all_ids from (
    select (r->>'id')::uuid as id from jsonb_array_elements(v_prior_jobs) r
    union all select unnest(v_owned)
  ) expected;
  select array_agg(j.id order by j.id) into v_all_ids
    from public.banking_pay_workbench_jobs j;
  v_claim_now:=clock_timestamp();
  if cardinality(v_owned)<>v_owned_count or v_all_ids is distinct from v_expected_all_ids
     or (select count(*) from public.banking_pay_workbench_jobs j
       where j.id=any(v_owned) and j.status='QUEUED' and j.run_at_utc<=v_claim_now)<>v_owned_count then
    raise exception using errcode='P0001',message='PAID_FIXTURE_TWELVE_DUE_NOT_EXACT';
  end if;
  select coalesce(jsonb_agg(to_jsonb(j) order by j.id),'[]'::jsonb) into v_other_jobs
    from public.banking_pay_workbench_jobs j where not(j.id=any(v_owned));
  -- END V7 ALL TWELVE OWNED SETUP JOBS

  if v_other_jobs is distinct from v_prior_jobs then
    raise exception using errcode='P0001',message='PRESENTATION_FACTS_CHANGED_EXISTING_JOB_ROWS';
  end if;
  v_bank_after:=pg_temp.ws_verify_workbench_fingerprint();
  if v_bank_after is distinct from v_bank_before then
    raise exception using errcode='P0001',message='PRESENTATION_EXISTING_BANK_ROW_DRIFT';
  end if;
  -- Retain complete fixture economic rows internally, never in output.
  -- This local finite fixture snapshot is not a production scan-cost claim.
  foreach v_relation in array array[
    'public.timesheets','public.timesheets_financials',
    'public.weekly_source_billing_movements',
    'public.weekly_source_projection_publications',
    'public.weekly_source_ordinary_pay_projection_receipts',
    'public.weekly_source_root_authorisations',
    'public.invoices','public.pay_advances','public.pay_finance_case_components',
    'public.pay_batches','public.banking_pay_operations'
  ] loop
    execute format('select md5(coalesce(jsonb_agg(to_jsonb(t) order by to_jsonb(t)::text),''[]''::jsonb)::text) from %s t',v_relation) into v_hash;
    v_before:=v_before||jsonb_build_object(v_relation,v_hash);
  end loop;

  -- BEGIN V9 EXACT INSTALLED NAMED FINALIZER
  -- Same canonical cf8 baseline; root readback6538cf pins this unchanged owner.
  if (select count(*) from pg_catalog.pg_proc p
      join pg_catalog.pg_language l on l.oid=p.prolang
      where p.oid=to_regprocedure('public.pay_workbench_scope_change_finalize_trg_v1()')
        and p.proowner=current_user::regrole and p.prosecdef
        and p.prokind='f' and p.pronargs=0 and p.prorettype='pg_catalog.trigger'::regtype
        and p.provolatile='v' and p.proparallel='u' and l.lanname='plpgsql'
        and p.proconfig=array['search_path=public, pg_catalog']::text[]
        and md5(p.prosrc)='b1e01887b71866f545b62dd3b2cb658e'
        and md5(pg_catalog.pg_get_functiondef(p.oid))='03b772a328097a1206446fbd2038a296'
        and (select count(*) from pg_catalog.aclexplode(p.proacl) a)=2
        and not exists(select 1 from pg_catalog.aclexplode(p.proacl) a
          where a.privilege_type<>'EXECUTE' or a.is_grantable
            or a.grantor<>p.proowner
            or a.grantee not in(p.proowner,'service_role'::regrole))
        and has_function_privilege(current_user,p.oid,'EXECUTE')
        and has_function_privilege('service_role',p.oid,'EXECUTE')
        and not has_function_privilege('anon',p.oid,'EXECUTE')
        and not has_function_privilege('authenticated',p.oid,'EXECUTE'))<>1
     or (select count(*) from pg_catalog.pg_trigger t
       where t.tgrelid='public.banking_pay_scope_change_transactions'::regclass
         and t.tgname='trg_pay_workbench_scope_change_finalize_v1'
         and t.tgfoid=to_regprocedure('public.pay_workbench_scope_change_finalize_trg_v1()')
         and not t.tgisinternal and t.tgenabled='O' and t.tgtype=5
         and t.tgdeferrable and t.tginitdeferred and t.tgqual is null
         and t.tgnargs=0 and octet_length(t.tgargs)=0)<>1 then
    raise exception using errcode='P0001',message='PAID_FIXTURE_NAMED_FINALIZER_POSTURE_NOT_EXACT';
  end if;
  -- END V9 EXACT INSTALLED NAMED FINALIZER

  -- BEGIN V9 PREWORKER REAL REQUEST BOUNDARY
  -- Registered3108 models request COMMIT with this named real trigger only.
  -- These two transaction-local resets are not authorisation/payment gates.
  v_old_finalising:=current_setting('cloudtms.scope_generation_finalising',true);
  v_old_scope_token:=current_setting('cloudtms.banking_pay_scope_tx_token',true);
  if coalesce(v_old_finalising,'') not in('','false')
     or (coalesce(v_old_scope_token,'')<>'' and (
       v_old_scope_token !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or not exists(select 1 from public.banking_pay_scope_change_transactions s
         where s.tx_token::text=v_old_scope_token and s.state='PENDING'))) then
    raise exception using errcode='P0001',message='PAID_FIXTURE_PREWORKER_SCOPE_CONTEXT_NOT_EXACT';
  end if;
  execute 'SET CONSTRAINTS public.trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  perform set_config('cloudtms.scope_generation_finalising','false',true);
  perform set_config('cloudtms.banking_pay_scope_tx_token','',true);
  execute 'SET CONSTRAINTS public.trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  if current_setting('cloudtms.scope_generation_finalising',true)<>'false'
     or current_setting('cloudtms.banking_pay_scope_tx_token',true)<>''
     or (coalesce(v_old_scope_token,'')<>'' and not exists(
       select 1 from public.banking_pay_scope_change_transactions s
       where s.tx_token::text=v_old_scope_token and s.state in('FINALIZED','NOOP'))) then
    raise exception using errcode='P0001',message='PAID_FIXTURE_PREWORKER_SCOPE_NOT_FINALIZED';
  end if;
  -- END V9 PREWORKER REAL REQUEST BOUNDARY

  -- BEGIN V7 BOUNDED GENUINE TWELVE JOB COMPLETION
  -- At most three calls, each capped at twelve, with the actual current clock.
  -- This is a finite fixture stop bound, not a product completion guarantee.
  for v_call in 1..3 loop
    exit when (select count(*) from public.banking_pay_workbench_jobs j
      where j.id=any(v_owned) and j.status='SUCCEEDED' and j.completed_at_utc is not null)=v_owned_count;
    v_claim_now:=clock_timestamp();
    v_result:=public.pay_workbench_dirty_apply_jobs_chunk(
      12,v_claim_now,null::uuid,v_candidate,'SOURCE_PAID_FIXTURE_SETUP_TWELVE',180
    );
    if v_result->>'ok' is distinct from 'true'
       or v_result->>'failed' is distinct from '0'
       or v_result->>'recovered_stale_count' is distinct from '0'
       or jsonb_typeof(v_result->'job_results') is distinct from 'array'
       or jsonb_typeof(v_result->'processed') is distinct from 'number'
       or jsonb_typeof(v_result->'succeeded') is distinct from 'number'
       or jsonb_typeof(v_result->'requeued') is distinct from 'number'
       or coalesce(v_result->>'processed','') !~ '^(?:[1-9]|1[0-2])$'
       or coalesce(v_result->>'succeeded','') !~ '^(?:[0-9]|1[0-2])$'
       or coalesce(v_result->>'requeued','') !~ '^(?:[0-9]|1[0-2])$'
       or jsonb_array_length(v_result->'job_results')<>(v_result->>'processed')::integer
       or (v_result->>'succeeded')::integer+(v_result->>'requeued')::integer<>(v_result->>'processed')::integer
       or exists(select 1 from jsonb_array_elements(v_result->'job_results') r
         where r->>'job_id' is null or not((r->>'job_id')::uuid=any(v_owned))
           or r->>'job_type' is distinct from (select j.job_type from public.banking_pay_workbench_jobs j
             where j.id=(r->>'job_id')::uuid)
           or coalesce(r->>'status','') not in ('SUCCEEDED','REQUEUED')
           or r#>>'{stage_result,ok}' is distinct from 'true'
           or coalesce(r#>>'{stage_result,candidate_serial_delayed}','false')<>'false'
           -- BEGIN V9 EXACT TERMINAL OR GENUINE COHORT PENDING
           or (r->>'job_type'='CONTRACT_CLIENT_DIRTY_FANOUT' and (
                r#>>'{stage_result,has_more}' is distinct from 'false'
             or r#>>'{stage_result,candidate_count}' is distinct from '1'
             or r#>>'{stage_result,affected_scope_count}' is distinct from '0'
             or r#>>'{stage_result,affected_session_count}' is distinct from '0'
             or r#>>'{stage_result,enqueued_count}' is distinct from '0'))
           or (r->>'job_type'='WORKBENCH_CANDIDATE_DIRTY_APPLY' and not coalesce((
                r#>>'{stage_result,candidate_id}'=v_candidate::text
             and r#>>'{stage_result,has_more}'='false'
             and r#>>'{stage_result,sessions_touched}'='0'
             and r#>>'{stage_result,jobs_queued}'='0'
             and r#>>'{stage_result,dirty_scope_count}'='0'
             and r#>>'{stage_result,dirty_source_line_count}'='0'
             and r#>>'{stage_result,dirty_line_count}'='0'
             and r#>>'{stage_result,dirty_preview_count}'='0'
           ) or (
               r->>'status'='REQUEUED'
               and r#>>'{stage_result,job_id}'=r->>'job_id'
               and r#>>'{stage_result,job_type}'='WORKBENCH_CANDIDATE_DIRTY_APPLY'
               and r#>>'{stage_result,candidate_id}'=v_candidate::text
               and r#>>'{stage_result,dirty_apply_cohort_action}' in
                 ('COHORT_REISSUED_PENDING_FINALIZATION','WAITING_FOR_COHORT_FINALIZATION')
               and r#>>'{stage_result,dirty_apply_cohort_authority_scope}'='CANDIDATE_FULL_LIVE'
               and jsonb_typeof(r#>'{stage_result,dirty_apply_cohort_member_count}')='number'
               and r#>>'{stage_result,dirty_apply_cohort_member_count}'='1'
               and r#>>'{stage_result,dirty_apply_cohort_excluded_request_owned_count}'='0'
               and r#>>'{stage_result,preinvalidated_scope_reissued}'='true'
               and r#>>'{stage_result,preinvalidated_scope_reissue_pending_finalization}'='true'
               and r#>>'{stage_result,has_more}'='true'
               and r#>>'{stage_result,more_due}'='true'
               and r#>>'{stage_result,rerun_required}'='true'
               and r#>>'{stage_result,dirty_apply_complete}'='false'
               and r#>>'{stage_result,dirty_apply_row_marking_applied}'='false'
               and r#>>'{stage_result,dirty_marking_skipped}'='true'
               and r#>>'{stage_result,session_progress_dirtying_skipped}'='true'
               and r#>>'{stage_result,policy_x_authority_scope}'='PRE_DRAFT_LIVE_TRUTH'
               and jsonb_typeof(r#>'{stage_result,effective_scope_change_generation}')='null'
               and (r#>>'{stage_result,made_progress}')=
                 (r#>>'{stage_result,dirty_apply_cohort_action}'='COHORT_REISSUED_PENDING_FINALIZATION')::text
               and not((r->'stage_result') ?| array['sessions_touched','jobs_queued',
                 'dirty_scope_count','dirty_source_line_count','dirty_line_count','dirty_preview_count'])
               and (r#>>'{stage_result,effective_scope_change_tx_token}') ~*
                 '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               and exists(select 1 from public.banking_pay_scope_change_transactions s
                 where s.tx_token::text=r#>>'{stage_result,effective_scope_change_tx_token}'
                   and s.state='PENDING')
           ),false))) then
           -- END V9 EXACT TERMINAL OR GENUINE COHORT PENDING
      raise exception using errcode='P0001',message='PAID_FIXTURE_TWELVE_PROCESSOR_NOT_EXACT';
    end if;
    -- BEGIN V9 PENDING COHORT REAL REQUEST BOUNDARY
    -- Only the positively qualified intermediate branch may require a flush.
    select count(*)::integer into v_pending_count
      from jsonb_array_elements(v_result->'job_results') r
      where r->>'job_type'='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        and r#>>'{stage_result,dirty_apply_cohort_action}' in
          ('COHORT_REISSUED_PENDING_FINALIZATION','WAITING_FOR_COHORT_FINALIZATION')
        and r#>>'{stage_result,has_more}'='true';
    if v_pending_count>0 then
      v_old_finalising:=current_setting('cloudtms.scope_generation_finalising',true);
      v_old_scope_token:=current_setting('cloudtms.banking_pay_scope_tx_token',true);
      if coalesce(v_old_finalising,'') not in('','false')
         or (coalesce(v_old_scope_token,'')<>'' and not exists(
           select 1 from jsonb_array_elements(v_result->'job_results') r
           where r#>>'{stage_result,effective_scope_change_tx_token}'=v_old_scope_token)) then
        raise exception using errcode='P0001',message='PAID_FIXTURE_PENDING_SCOPE_CONTEXT_NOT_EXACT';
      end if;
      execute 'SET CONSTRAINTS public.trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
      perform set_config('cloudtms.scope_generation_finalising','false',true);
      perform set_config('cloudtms.banking_pay_scope_tx_token','',true);
      execute 'SET CONSTRAINTS public.trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
      if current_setting('cloudtms.scope_generation_finalising',true)<>'false'
         or current_setting('cloudtms.banking_pay_scope_tx_token',true)<>''
         or exists(select 1 from jsonb_array_elements(v_result->'job_results') r
           where r#>>'{stage_result,dirty_apply_cohort_action}' in
             ('COHORT_REISSUED_PENDING_FINALIZATION','WAITING_FOR_COHORT_FINALIZATION')
             and not exists(select 1 from public.banking_pay_scope_change_transactions s
               where s.tx_token::text=r#>>'{stage_result,effective_scope_change_tx_token}'
                 and s.state in('FINALIZED','NOOP'))) then
        raise exception using errcode='P0001',message='PAID_FIXTURE_PENDING_SCOPE_NOT_FINALIZED';
      end if;
    end if;
    -- END V9 PENDING COHORT REAL REQUEST BOUNDARY
    select array_agg(j.id order by j.id) into v_all_ids
      from public.banking_pay_workbench_jobs j;
    select coalesce(jsonb_agg(to_jsonb(j) order by j.id),'[]'::jsonb) into v_other_jobs_after
      from public.banking_pay_workbench_jobs j where not(j.id=any(v_owned));
    v_bank_after:=pg_temp.ws_verify_workbench_fingerprint();
  if v_bank_after is distinct from v_bank_before then
    raise exception using errcode='P0001',message='PRESENTATION_EXISTING_BANK_ROW_DRIFT';
  end if;
    if v_all_ids is distinct from v_expected_all_ids or v_other_jobs_after is distinct from v_other_jobs
       or exists(select 1 from public.banking_pay_workbench_jobs j
         where j.id=any(v_owned) and j.status not in ('QUEUED','SUCCEEDED'))
       or exists(select 1 from public.banking_pay_workbench_sessions where status='OPEN' and discarded_at_utc is null)
       or exists(select 1 from public.banking_pay_workbench_session_scope where candidate_id='b8550000-0000-4000-8000-000000000003'::uuid)
       or exists(select 1 from public.banking_pay_workbench_candidate_source_lines where candidate_id=v_candidate) then
      raise exception using errcode='P0001',message='PAID_FIXTURE_TWELVE_CHILD_OR_NON_TARGET_DRIFT';
    end if;
    v_after:='{}'::jsonb;
    foreach v_relation in array array[
      'public.timesheets','public.timesheets_financials',
      'public.weekly_source_billing_movements',
      'public.weekly_source_projection_publications',
      'public.weekly_source_ordinary_pay_projection_receipts',
      'public.weekly_source_root_authorisations',
      'public.invoices','public.pay_advances','public.pay_finance_case_components',
      'public.pay_batches','public.banking_pay_operations'
    ] loop
      execute format('select md5(coalesce(jsonb_agg(to_jsonb(t) order by to_jsonb(t)::text),''[]''::jsonb)::text) from %s t',v_relation) into v_hash;
      v_after:=v_after||jsonb_build_object(v_relation,v_hash);
    end loop;
    if v_after is distinct from v_before then
      raise exception using errcode='P0001',message='PAID_FIXTURE_TWELVE_ECONOMIC_ROW_DRIFT';
    end if;
  end loop;
  if (select count(*) from public.banking_pay_workbench_jobs j
      where j.id=any(v_owned) and j.status='SUCCEEDED' and j.completed_at_utc is not null)<>v_owned_count
     or exists(select 1 from public.banking_pay_workbench_jobs j
       where j.id=any(v_owned) and j.status in ('QUEUED','RUNNING')) then
    raise exception using errcode='P0001',message='PAID_FIXTURE_TWELVE_NOT_TERMINAL_WITHIN_BOUND';
  end if;
  -- END V7 BOUNDED GENUINE TWELVE JOB COMPLETION

  -- BEGIN V7 NON-TARGET COMPLETE ROW READBACK
  select coalesce(jsonb_agg(to_jsonb(j) order by j.id),'[]'::jsonb) into v_other_jobs_after
    from public.banking_pay_workbench_jobs j where not(j.id=any(v_owned));
  v_bank_after:=pg_temp.ws_verify_workbench_fingerprint();
  if v_bank_after is distinct from v_bank_before then
    raise exception using errcode='P0001',message='PRESENTATION_EXISTING_BANK_ROW_DRIFT';
  end if;
  if v_other_jobs_after is distinct from v_other_jobs then
    raise exception using errcode='P0001',message='PAID_FIXTURE_OTHER_JOB_ROW_DRIFT';
  end if;
  -- END V7 NON-TARGET COMPLETE ROW READBACK

  foreach v_relation in array array[
    'public.timesheets','public.timesheets_financials',
    'public.weekly_source_billing_movements',
    'public.weekly_source_projection_publications',
    'public.weekly_source_ordinary_pay_projection_receipts',
    'public.weekly_source_root_authorisations',
    'public.invoices','public.pay_advances','public.pay_finance_case_components',
    'public.pay_batches','public.banking_pay_operations'
  ] loop
    execute format('select md5(coalesce(jsonb_agg(to_jsonb(t) order by to_jsonb(t)::text),''[]''::jsonb)::text) from %s t',v_relation) into v_hash;
    v_after:=v_after||jsonb_build_object(v_relation,v_hash);
  end loop;
  if v_after is distinct from v_before then
    raise exception using errcode='P0001',message='PAID_FIXTURE_FANOUT_ECONOMIC_ROW_DRIFT';
  end if;
end;
$paid_root_owned_setup_fanout$;
-- END PAID ROOT OWNED SETUP FANOUT COMPLETION V1

create function pg_temp.bpsx_import(p_nhsp boolean,p_week date,p_rows jsonb,p_key text,p_session uuid default null)
returns jsonb language plpgsql as $f$
declare
  v_actor constant uuid:='b8560000-0000-4000-8000-000000000001';
  v_candidate constant uuid:='b8560000-0000-4000-8000-000000000003';
  v_agency constant uuid:='b8560000-0000-4000-8000-000000000006';
  v_group uuid:=case when p_nhsp then 'b8560000-0000-4000-8000-000000000015'::uuid else 'b8560000-0000-4000-8000-000000000005'::uuid end;
  v_client uuid:=case when p_nhsp then 'b8560000-0000-4000-8000-000000000012'::uuid else 'b8560000-0000-4000-8000-000000000002'::uuid end;
  v_contract uuid:=case when p_nhsp then 'b8560000-0000-4000-8000-000000000014'::uuid else 'b8560000-0000-4000-8000-000000000004'::uuid end;
  v_cycle uuid;v_scope uuid;v_upload uuid;v_publication uuid;v_version bigint;v_result jsonb;v_request jsonb;v_stage_request jsonb;
  v_physical jsonb:='[{"source_row_ordinal":1,"classification":"HEADER","bounded_raw_cells_json":{"A":"NEXT Source native fixture"}}]';
  v_normal jsonb:='[]';v_money jsonb:='[]';v_expense jsonb:='[]';v_projection jsonb:='[]';
  v_row jsonb;v_source_row public.weekly_source_upload_rows%rowtype;v_resolution public.weekly_source_row_resolutions%rowtype;
  v_ord integer:=1;v_sign integer;v_minutes integer;v_break integer;v_pay bigint;v_charge bigint;v_commission bigint;
  v_state text;v_field record;v_economic jsonb;v_count integer:=jsonb_array_length(p_rows);
begin
  perform pg_temp.bpsx_assert(v_count between 0 and 4,'bounded two-shift import rows');
  insert into public.weekly_source_cycles(source_group_id,finalisation_week_ending,cutoff_at_utc,scope_client_id)
    values(v_group,p_week,case when p_nhsp then ((p_week-4)::date + time '18:00') at time zone 'Europe/London' else (p_week-4)::timestamp at time zone 'Europe/London' end,case when p_nhsp then null else v_client end)
    on conflict on constraint weekly_source_cycles_group_week_client_uq do nothing;
  select c.id into strict v_cycle from public.weekly_source_cycles c
    where c.source_group_id=v_group and c.finalisation_week_ending=p_week
      and c.scope_client_id is not distinct from case when p_nhsp then null else v_client end;
  if p_nhsp then
    insert into public.weekly_source_report_scopes(source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc)
      select v_cycle,'TEST',v_agency,v_group,v_client,c.cutoff_at_utc from public.weekly_source_cycles c where c.id=v_cycle
      on conflict(environment,agency_id,source_group_id,client_id,cutoff_at_utc) do nothing;
    select s.id into strict v_scope from public.weekly_source_report_scopes s where s.source_cycle_id=v_cycle and s.client_id=v_client;
  end if;
  v_request:=jsonb_build_object('actor_user_id',v_actor,'environment','TEST','agency_id',v_agency,
    'source_group_id',v_group,'source_cycle_id',v_cycle,'client_id',v_client,'report_scope_id',v_scope,
    'original_filename',p_key||case when p_nhsp then '.xlsx' else '.csv' end,
    'content_sha256',encode(private.weekly_source_sha256_jsonb_v1('NEXT_IMPORT_NATIVE_INPUT_V1',jsonb_build_object('key',p_key,'rows',p_rows)),'hex'),
    'byte_count',1024,'profile_code',case when p_nhsp then 'NHSP_FINAL_BACKING_V1' else 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' end,
    'profile_version',1,'parser_version','WEEKLY_SOURCE_STRICT_V1',
    'normaliser_version',case when p_nhsp then 'NHSP_BACKING_NORMALISER_V1' else 'ROSTER_WEEKLY_SUMMARY_NORMALISER_V1' end,
    'header_coordinate_map_json',case when p_nhsp then '{"Actual Start":"L","Actual End":"M","Actual Break":"N","Actual Total":"O","Commission":"P","Total Cost":"Q","FMC":"R"}'::jsonb
      else '{"Booking Start":"A","Booking End":"B","Total Hours":"C","Expenses":"D"}'::jsonb end,
    'purpose',case when p_session is null then 'ORDINARY' else 'FINAL_SOURCE_CORRECTION' end,'physical_row_count',v_count+1+case when p_nhsp then 1 else 0 end,
    'header_count',1,'trailer_count',case when p_nhsp then 1 else 0 end,'continuation_count',0,'accepted_count',v_count,
    'blocking_economic_duplicate_count',0,'malformed_count',0,'blocked_count',0,
    'coverage_proof_kind',case when p_nhsp then 'NHSP_TRUST_REPORT_SCOPE' else 'OFFICE_COMPLETE_EXPORT_ATTESTATION' end,
    'file_metadata_json',case when p_nhsp then jsonb_build_object('client_id',v_client,'nhsp_report_number',p_key,'nhsp_report_heading_name','PRESENTATION variants NHSP Trust') else jsonb_build_object('client_id',v_client) end,
    'parser_summary_json',jsonb_build_object('fatal_errors',0));
  if p_session is not null then
    v_request:=v_request||jsonb_build_object('correction_session_id',p_session);
    if p_nhsp then v_request:=jsonb_set(v_request,'{file_metadata_json,nhsp_report_number}',
      (select u.file_metadata_json->'nhsp_report_number' from public.weekly_final_source_correction_sessions s
       join public.weekly_source_final_revisions r on r.id=s.expected_current_final_revision_id
       join public.weekly_source_uploads u on u.id=r.upload_id where s.id=p_session));end if;
  end if;
  if v_count=0 then v_request:=jsonb_set(v_request,'{file_metadata_json}',
    (v_request->'file_metadata_json')||jsonb_build_object('explicit_empty_attestation',true));end if;
  if p_nhsp then
    v_request:=v_request||jsonb_build_object('workbook_part_and_sheet_fingerprint',repeat('7',64),
      'money_lexical_authority_version','XLSX_BINARY64_SAME_VALUE_PENCE_V1');
  else
    v_request:=v_request||jsonb_build_object('suggested_coverage_start_local_date',case when v_count>0 then '2026-08-31' else null end,
      'suggested_coverage_end_local_date',case when v_count>0 then '2026-08-31' else null end,'confirmed_coverage_start_local_date','2026-08-31',
      'confirmed_coverage_end_local_date','2026-08-31','coverage_timezone','Europe/London',
      'coverage_confirmation_version','OFFICE_COMPLETE_EXPORT_ATTESTATION_V1','coverage_state','COMPLETE');
  end if;
  v_stage_request:=v_request;
  v_result:=public.weekly_source_upload_stage_begin_atomic_v1(v_request);
  perform pg_temp.bpsx_assert(v_result->>'status'='STAGING','real stage begin');
  v_upload:=(v_result->>'logical_upload_id')::uuid;
  for v_row in select value from jsonb_array_elements(p_rows) loop
    v_ord:=v_ord+1;v_sign:=coalesce((v_row->>'sign')::integer,1);v_minutes:=(v_row->>'minutes')::integer;v_break:=(v_row->>'break')::integer;
    v_state:=case when v_minutes=0 then 'SOURCE_ABSENT_ZERO' else 'SOURCE_WORKED' end;
    v_pay:=v_sign*v_minutes*2000/60;v_charge:=v_sign*v_minutes*4000/60;v_commission:=v_sign*500;
    v_physical:=v_physical||jsonb_build_array(jsonb_build_object('source_row_ordinal',v_ord,
      'classification','ACCEPTED_SHIFT','bounded_raw_cells_json',jsonb_build_object('A',v_row->>'key')));
    v_normal:=v_normal||jsonb_build_array(jsonb_build_object('source_row_ordinal',v_ord,
      'external_source_key',v_row->>'key','source_candidate_identity','PRESENTATION variants Source worker',
      'source_client_identity',case when p_nhsp then 'PRESENTATION variants NHSP Trust' else 'NEXT imported Magnit-style roster' end,
      'work_date',v_row->>'date','start_at_local',case when v_minutes=0 and (v_row->>'expense')::bigint=0 then null else (v_row->>'date')||'T09:00:00' end,
      'end_at_local',case when v_minutes=0 and (v_row->>'expense')::bigint=0 then null else (v_row->>'date')||'T'||(v_row->>'end')||':00' end,
      'break_minutes',case when v_minutes=0 and (v_row->>'expense')::bigint=0 then null else v_break end,'actual_net_minutes',v_minutes,
      'row_finalisation_state',v_state,'role_band_source','BAND 5',
      'source_money_parse_state',case when p_nhsp then 'VALID' else 'NOT_APPLICABLE' end,
      'source_commission_pence',case when p_nhsp then v_commission else null end,
      'source_total_cost_pence',case when p_nhsp then v_charge-v_commission else null end,
      'source_shift_charge_pence',case when p_nhsp then v_charge else null end,
      'source_qualification_profile_version',case when p_nhsp then 'NHSP_TWO_COMPONENT_PENCE_V1' else null end,
      'source_expense_pence',case when p_nhsp then null else (v_row->>'expense')::bigint end,
      'source_expense_parse_state',case when p_nhsp then 'NOT_APPLICABLE' when (v_row->>'expense')::bigint=0 then 'OMITTED_ZERO' else 'VALID' end,
      'bounded_raw_columns_json',jsonb_build_object('Line ID',v_row->>'key','Total Hours',v_minutes::numeric/60)));
    if p_nhsp then
      for v_field in select * from (values('COMMISSION',15,'P',v_commission),
          ('TOTAL_COST',16,'Q',v_charge-v_commission),('FMC',17,'R',0::bigint)) f(kind,column_no,column_letter,pence) loop
        v_money:=v_money||jsonb_build_array(jsonb_build_object('source_row_ordinal',v_ord,
          'money_field_kind',v_field.kind,'source_column_index',v_field.column_no,'cell_coordinate',v_field.column_letter||v_ord::text,
          'source_kind','XLSX_NUMERIC_TOKEN','original_token',round(v_field.pence::numeric/100,2)::text,
          'decoded_token',round(v_field.pence::numeric/100,2)::text,'cell_type_marker','n','formula_present',false,
          'parse_state','VALID','parsed_pence',v_field.pence));
      end loop;
    else
      v_expense:=v_expense||jsonb_build_array(jsonb_build_object('source_row_ordinal',v_ord,
        'source_column_index',64,'cell_coordinate','BM'||v_ord::text,'source_kind','CSV_DECODED_TEXT',
        'original_token',case when (v_row->>'expense')::bigint=0 then '' else round((v_row->>'expense')::numeric/100,2)::text end,
        'decoded_token',case when (v_row->>'expense')::bigint=0 then '' else round((v_row->>'expense')::numeric/100,2)::text end,
        'cell_type_marker','CSV_FIELD','formula_present',false,'lexical_profile_version','SOURCE_FIXED_EXPENSE_PENCE_V1',
        'parse_state',case when (v_row->>'expense')::bigint=0 then 'OMITTED_ZERO' else 'VALID' end,'parsed_pence',(v_row->>'expense')::bigint));
    end if;
  end loop;
  if p_nhsp then v_physical:=v_physical||jsonb_build_array(jsonb_build_object('source_row_ordinal',v_count+2,
    'classification','TRAILER','bounded_raw_cells_json',jsonb_build_object('A','END')));end if;
  v_result:=public.weekly_source_upload_stage_rows_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'upload_id',v_upload,
    'physical_rows',v_physical,'normalised_rows',v_normal,'money_evidence',v_money,'expense_evidence',v_expense));
  perform pg_temp.bpsx_assert(v_result->>'status'='STAGING','real stage rows');
  v_result:=public.weekly_source_upload_seal_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'upload_id',v_upload));
  perform pg_temp.bpsx_assert(v_result->>'status'=case when p_session is null then 'CURRENT' else 'CORRECTION_READY' end,'real sealed upload: '||v_result::text);
  v_version:=case when p_session is null then (v_result->>'authority_scope_version')::bigint else coalesce((select s.version from public.weekly_source_report_scopes s where s.id=v_scope),(select c.version from public.weekly_source_cycles c where c.id=v_cycle)) end;
  v_result:=public.weekly_source_projection_begin_atomic_v1(jsonb_build_object('actor_user_id',v_actor,
    'upload_id',v_upload,'expected_authority_scope_version',v_version));
  v_publication:=(v_result->>'publication_id')::uuid;
  for v_source_row in select r.* from public.weekly_source_upload_rows r where r.upload_id=v_upload order by r.source_row_ordinal loop
    v_row:=p_rows->(v_source_row.source_row_ordinal-2);v_sign:=coalesce((v_row->>'sign')::integer,1);
    v_minutes:=v_source_row.actual_net_minutes;v_pay:=v_sign*v_minutes*2000/60;v_charge:=v_sign*v_minutes*4000/60;
    v_economic:=case when v_minutes=0 then null else jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1','calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
      'source_mode',case when p_nhsp then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end,'rate_method','SPLIT_RATE_WINDOWS',
      'sign',v_sign,'paid_minutes',v_minutes,'break_minutes',v_source_row.break_minutes,
      'bucket_minutes',jsonb_build_object('day',v_minutes,'night',0,'sat',0,'sun',0,'bh',0),
      'hours',jsonb_build_object('day',round(v_sign*v_minutes::numeric/60,2),'night',0,'sat',0,'sun',0,'bh',0),
      'pay_rates','{"day":20,"night":20,"sat":20,"sun":20,"bh":20}'::jsonb,
      'charge_rates','{"day":40,"night":40,"sat":40,"sun":40,"bh":40}'::jsonb,
      'total_pay_pence',v_pay::text,'calculated_charge_pence',v_charge::text) end;
    v_request:=jsonb_build_object('upload_row_id',v_source_row.id,'mapping_state','RESOLVED','candidate_id',v_candidate,
      'client_id',v_client,'contract_id',v_contract,'contract_selection_method','AUTO_UNIQUE','qualifying_contract_ids',jsonb_build_array(v_contract),
      'identity_kind',case when p_nhsp then 'SCHEDULE_TUPLE' else 'PROFILE_EXTERNAL_KEY' end,
      'profile_external_key',case when p_nhsp then null else v_source_row.external_source_key end,
      'prior_work_event_id',v_row->>'prior_event',
      'link_kind',case when v_minutes=0 then 'ZERO_SOURCE' when v_sign<0 then 'FULL_NEGATIVE_SOURCE' else 'POSITIVE_SOURCE' end,
      'economic_snapshot',v_economic);
    if p_nhsp then v_request:=v_request||jsonb_build_object('charge_check',jsonb_build_object(
      'row_sign_kind',case when v_sign<0 then 'FULL_NEGATIVE' else 'POSITIVE' end,
      'source_commission_pence',v_source_row.source_commission_pence::text,
      'source_total_cost_pence',v_source_row.source_total_cost_pence::text,'source_shift_charge_pence',v_source_row.source_shift_charge_pence::text,
      'calculated_segment_charge_pence',v_charge::text,'comparison_result','EXACT','comparison_reason_code','EXACT','phase_severity','NONE'));end if;
    v_projection:=v_projection||jsonb_build_array(jsonb_strip_nulls(v_request));
  end loop;
  v_result:=public.weekly_source_projection_rows_apply_atomic_v1(v_actor,v_publication,v_projection);
  perform pg_temp.bpsx_assert((v_result->>'applied_row_count')::integer=v_count,'real projection census');
  v_result:=public.weekly_source_projection_publish_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'publication_id',v_publication));
  perform pg_temp.bpsx_assert(v_result->>'status'=case when p_session is null then 'CURRENT' else 'CORRECTION_READY' end,'real published projection');
  if p_session is not null then return v_result||jsonb_build_object('cycle_id',v_cycle,'upload_id',v_upload,'publication_id',v_publication);end if;
  if not p_nhsp then
    v_result:=public.weekly_source_import_prepare_atomic_v1(jsonb_build_object('actor_user_id',v_actor,
      'upload_id',v_upload,'projection_publication_id',v_publication,'expected_authority_scope_version',v_version,
      'expected_row_manifest_hash',(select encode(u.row_manifest_hash,'hex') from public.weekly_source_uploads u where u.id=v_upload)));
    perform pg_temp.bpsx_assert(v_result->>'status'='PREPARED','real import preparation');
  end if;
  v_request:=jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle,
    'authority_scope_kind',case when p_nhsp then 'NHSP_REPORT_SCOPE' else 'CYCLE' end,'report_scope_id',v_scope,
    'upload_id',v_upload,'projection_publication_id',v_publication,'expected_authority_scope_version',v_version,
    'expected_row_manifest_hash',(select encode(u.row_manifest_hash,'hex') from public.weekly_source_uploads u where u.id=v_upload),
    'expected_comparison_manifest_hash',(select encode(p.comparison_manifest_hash,'hex') from public.weekly_source_projection_publications p where p.id=v_publication),
    'expected_issue_set_hash',(select encode(p.issue_set_hash,'hex') from public.weekly_source_projection_publications p where p.id=v_publication));
  v_result:=public.weekly_source_finalise_atomic_v1(v_request);
  perform pg_temp.bpsx_assert(v_result->>'final_revision_id' is not null,'real final revision');
  perform pg_temp.bpsx_assert((public.weekly_source_finalise_atomic_v1(v_request)->>'idempotent')::boolean,'exact real finalise replay');
  return v_result||jsonb_build_object('cycle_id',v_cycle,'upload_id',v_upload,'publication_id',v_publication,
    'stage_request',v_stage_request);
end $f$;

create function pg_temp.bpsx_service(p_context jsonb) returns jsonb language plpgsql as $f$
declare v_root public.timesheets%rowtype;v_contract public.contracts%rowtype;v_tsfin jsonb:='{}';
  v_key text;v_segment jsonb;v_policy jsonb;v_pay numeric;v_charge numeric;v_hours numeric;v_margin numeric;v_erni numeric;
begin
  select t.* into strict v_root from public.timesheets t where t.timesheet_id=(p_context->>'root_timesheet_id')::uuid;
  select c.* into strict v_contract from public.contracts c where c.id=v_root.contract_id;
  foreach v_key in array array['additional_charge_ex_vat','additional_margin_ex_vat','additional_pay_ex_vat','additional_units_json','band','basis','candidate_assignment','candidate_id',
    'charge_bh','charge_day','charge_night','charge_sat','charge_sun','client_id','expenses_charge_ex_vat','expenses_description','expenses_evidence_manifest',
    'expenses_evidence_r2_key','expenses_pay_ex_vat','hours_bh','hours_day','hours_night','hours_sat','hours_sun','invoice_breakdown_json','margin_ex_vat',
    'mileage_charge_ex_vat','mileage_charge_rate','mileage_evidence_manifest','mileage_evidence_r2_key','mileage_pay_ex_vat','mileage_pay_rate','mileage_units',
    'pay_bh','pay_day','pay_method','pay_night','pay_sat','pay_sun','policy_snapshot_json','processing_status','rate_source_refs_json','role','timesheet_id',
    'timesheet_version','total_charge_ex_vat','total_hours','total_pay_ex_vat'] loop v_tsfin:=v_tsfin||jsonb_build_object(v_key,null);end loop;
  v_policy:=((private._timesheet_settings_authority_frozen_v1(v_root.timesheet_id))->'values')-'resolved_at_utc';
  select coalesce(sum((s->>'pay_amount')::numeric),0),coalesce(sum((s->>'charge_amount')::numeric),0),coalesce(sum((s->>'hours_day')::numeric),0)
    into v_pay,v_charge,v_hours from jsonb_array_elements(p_context->'expected_segments') s;
  v_erni:=coalesce((v_policy->>'erni_pct')::numeric,0);
  v_erni:=case when v_erni>0 then 1+case when v_erni>1 then v_erni/100 else v_erni end else 1 end;
  v_margin:=round(round(v_charge,2)-(
    case when upper(coalesce(v_contract.pay_method_snapshot,''))='PAYE'
      and upper(coalesce(v_policy->>'apply_erni_to','PAYE_ONLY')) in ('ALL','PAYE_ONLY')
      then round(round(v_pay,2)*v_erni,2) else round(v_pay,2) end
  ),2);v_segment:=p_context->'expected_segments'->0;
  foreach v_key in array array['day','night','sat','sun','bh'] loop
    v_tsfin:=v_tsfin||jsonb_build_object('pay_'||v_key,v_segment#>array['weekly_source','pay_vector','rates',v_key],
      'charge_'||v_key,v_segment#>array['weekly_source','charge_vector','rates',v_key]);
  end loop;
  v_tsfin:=v_tsfin||jsonb_build_object('timesheet_id',v_root.timesheet_id,'timesheet_version',v_root.version,'candidate_id',v_contract.candidate_id,
    'client_id',v_contract.client_id,'role',v_contract.role,'band',v_contract.band,'pay_method',v_contract.pay_method_snapshot,'candidate_assignment','ASSIGNED',
    'processing_status','PENDING_AUTH','basis','NHSP','policy_snapshot_json',v_policy,'rate_source_refs_json',p_context->'expected_rate_source_refs',
    'hours_day',v_hours,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,'total_hours',v_hours,'total_pay_ex_vat',v_pay,'total_charge_ex_vat',v_charge,
    'margin_ex_vat',v_margin,'additional_units_json','{}'::jsonb,'additional_pay_ex_vat',0,'additional_charge_ex_vat',0,'additional_margin_ex_vat',0,
    'expenses_pay_ex_vat',0,'expenses_charge_ex_vat',0,'mileage_units',0,'mileage_pay_ex_vat',0,'mileage_charge_ex_vat',0,
    'invoice_breakdown_json',jsonb_build_object('mode','SEGMENTS','segments',p_context->'expected_segments',
      'additional',jsonb_build_object('units','{}'::jsonb,'pay_ex_vat',0,'charge_ex_vat',0,'margin_ex_vat',0),
      'totals',jsonb_build_object('total_pay_ex_vat',v_pay,'total_charge_ex_vat',v_charge,'margin_ex_vat',v_margin)));
  return jsonb_build_object('schema_version','WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1','calculator_owner','buildWeeklyScheduleSegmentsSnapshot',
    'source_actual_schedule_json',p_context->'expected_actual_schedule','tsfin_snapshot_json',v_tsfin);
end $f$;

create function pg_temp.bpsx_project_prior(p_revision uuid,p_root uuid,p_key text) returns void language plpgsql as $f$
declare v_r public.weekly_source_final_revisions%rowtype;v_manifest public.weekly_source_client_manifests%rowtype;
  v_segments jsonb;v_units jsonb;v_expenses jsonb;v_context jsonb;v_reply jsonb;
begin
  select r.* into strict v_r from public.weekly_source_final_revisions r where r.id=p_revision;
  select m.* into strict v_manifest from public.weekly_source_client_manifests m
    where m.final_revision_id=p_revision and m.client_id='b8560000-0000-4000-8000-000000000012';
  v_segments:=private.weekly_source_ordinary_projection_current_segments_v1(p_root,p_revision);
  v_units:=private.weekly_source_ordinary_projection_source_units_v1(p_revision,p_root);
  v_expenses:=private.weekly_source_ordinary_projection_current_expenses_v1(p_root,p_revision);
  perform pg_temp.bpsx_assert(jsonb_array_length(v_expenses)=0,'NHSP preceding projection has no imported expenses');
  v_context:=jsonb_build_object('root_timesheet_id',p_root,'expected_segments',v_segments,
    'expected_actual_schedule',private.weekly_source_ordinary_projection_actual_schedule_v1(v_segments),
    'expected_rate_source_refs',jsonb_build_object('schema_version','WEEKLY_SOURCE_FINAL_AUTHORITY_RATE_SOURCE_V1',
      'source_mode','NHSP_WEEKLY','root_timesheet_id',p_root,'final_revision_id',p_revision,
      'final_manifest_hash',encode(v_r.manifest_hash,'hex'),'final_policy_fingerprint',encode(v_r.policy_fingerprint,'hex'),
      'client_manifest_hash',encode(v_manifest.manifest_hash,'hex'),
      'source_unit_manifest_hash',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_ORDINARY_UNIT_MANIFEST_V1',v_units),'hex'),
      'source_expense_manifest_hash',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_ORDINARY_SOURCE_EXPENSE_MANIFEST_V1',v_expenses),'hex'),
      'active_segment_manifest_hash',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_ORDINARY_ACTIVE_SEGMENTS_V1',v_segments),'hex')));
  v_reply:=public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_REQUEST_V1',
    'actor_user_id','b8560000-0000-4000-8000-000000000001','final_revision_id',p_revision,
    'root_timesheet_id',p_root,'idempotency_key',p_key,'service_snapshot',pg_temp.bpsx_service(v_context)));
  perform pg_temp.bpsx_assert(v_reply->>'outcome' in ('PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED'),
    'genuine preceding projection receipt');
end $f$;
create function pg_temp.bpsx_decide(p_root uuid,p_final uuid,p_decision text,p_key text)
returns jsonb language plpgsql as $f$
declare
  v_actor constant uuid:='b8560000-0000-4000-8000-000000000001';
  v_bundle_id uuid:=gen_random_uuid();v_request jsonb;v_record jsonb;v_reply jsonb;
  v_components jsonb;v_inventory jsonb;v_public jsonb;v_head uuid;v_work uuid;v_revision uuid;
  v_component uuid;v_component_ids jsonb;v_expected jsonb;v_actual jsonb;
  v_head_row public.weekly_source_entitlement_heads%rowtype;
  v_reviewed public.weekly_source_entitlement_decision_bundles%rowtype;
begin
  perform pg_temp.bpsx_assert(p_decision in ('APPROVE_UPDATED_HOURS','KEEP_CURRENTLY_APPROVED_HOURS'),
    'only actual Office APP/KEEP');
  if p_decision='APPROVE_UPDATED_HOURS' then
    -- Use the proposal the actual preceding public projection owner recorded;
    -- do not create a replacement proposal or supply money to the APP caller.
    select b.* into strict v_reviewed from public.weekly_source_entitlement_decision_bundles b
      where b.decision_bundle_id=private.weekly_source_entitlement_derived_uuid_v1(
        'WEEKLY_SOURCE_DECISION_BUNDLE_V1',
        (select btrim(t.booking_id) from public.timesheets t where t.timesheet_id=p_root)||'|'||p_final::text)
        and b.bundle_revision=1 and b.state='PROPOSED' and b.bundle_kind='SINGLE_ROOT'
        and b.source_root_timesheet_id=p_root;
    v_bundle_id:=v_reviewed.decision_bundle_id;
    perform pg_temp.bpsx_assert((private.weekly_source_office_proposal_revision_v1(
      array[p_root],v_reviewed.source_revision_digest)->>'final_revision_id')::uuid=p_final,
      'exact current Final from the genuine recorded proposal');
  else
    v_inventory:=private.weekly_source_effective_inventory_v1(p_root);
    perform pg_temp.bpsx_assert(v_inventory->>'ok'='true'
      and v_inventory->>'authority'='HEAD','KEEP requires a genuine complete committed HEAD');
    select coalesce(jsonb_agg(c.value-'component_sha256' order by (c.value->>'component_ordinal')::integer),'[]'::jsonb)
      into v_components from jsonb_array_elements(v_inventory->'components') c(value);
    -- KEEP's genuine server proposal owner freezes the exact approved vector;
    -- the later public owner copies only the chosen detail of that prior HEAD.
    v_request:=private.weekly_source_entitlement_proposal_request_v1(p_root,p_final,
      'PROTECTED',v_bundle_id,1,gen_random_uuid(),gen_random_uuid(),v_components);
    v_record:=private.weekly_source_entitlement_proposal_record_v1(v_request,
      'b8560000-0000-4000-8000-000000000006','b8560000-0000-4000-8000-000000000014',
      (select week_ending_date from public.timesheets where timesheet_id=p_root),v_actor);
    perform pg_temp.bpsx_assert(v_record->>'ok'='true' and v_record->>'created'='true',
      'actual complete KEEP proposal constructor/record');
  end if;
  -- Fixture assertion derives the whole vector through the same existing pure
  -- Source owners, never through seeded/head caller fields. Zero and two-shift
  -- vectors are legitimate owner inputs, not exemptions from completeness.
  if p_decision='APPROVE_UPDATED_HOURS' then
    perform pg_temp.bpsx_assert(private.weekly_source_ordinary_projection_current_expenses_v1(p_root,p_final)='[]'::jsonb,
      'genuine variants contain no Source fixed expenses');
    v_components:=private.weekly_source_entitlement_components_v1(
      private.weekly_source_ordinary_projection_current_segments_v1(p_root,p_final),'[]'::jsonb);
    v_components:=private.weekly_source_retain_approved_additional_v2(p_root,v_components);
  end if;
  perform pg_temp.bpsx_assert(jsonb_array_length(v_components) between 0 and 2
    and not exists(select 1 from jsonb_array_elements(v_components) c(value)
      where c.value->>'component_kind' is distinct from 'WORKED_TIME'
        or c.value->'exclude_from_pay' is distinct from 'false'::jsonb),
    'complete zero-to-two payable worked components; no Additional or excluded variant');
  select coalesce(jsonb_agg(private.weekly_source_publication_component_canonical_v1(
    c.value,'presentation.complete_component') order by (c.value->>'component_ordinal')::integer),'[]'::jsonb)
    into v_expected from jsonb_array_elements(v_components) c(value);
  v_public:=jsonb_build_object('schema_version','WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1',
    'actor_user_id',v_actor,'bundle_revision',1,'decision',p_decision,
    'decision_bundle_id',v_bundle_id,'final_revision_id',p_final,
    'idempotency_key',p_key,'root_timesheet_id',p_root);
  v_reply:=public.weekly_source_later_change_decide_atomic_v1(v_public);
  perform pg_temp.bpsx_assert(v_reply->>'ok'='true' and v_reply->>'published'='true'
    and (v_reply->>'bundle_revision')::bigint=2
    and (v_reply->>'reviewed_bundle_revision')::bigint=1
    and v_reply->>'idempotent_replay'='false','genuine accepted successor publication, never a 100->100 substitute');
  select b.proposed_head_ids[1] into strict v_head
    from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=v_bundle_id and b.bundle_revision=2 and b.state='COMMITTED';
  select h.* into strict v_head_row from public.weekly_source_entitlement_heads h where h.id=v_head;
  perform pg_temp.bpsx_assert(v_head_row.state='COMMITTED_CURRENT'
    and v_head_row.component_count=jsonb_array_length(v_expected)
    and v_head_row.certified_zero=(jsonb_array_length(v_expected)=0)
    and v_head_row.root_timesheet_id=p_root
    and v_head_row.bundle_revision=2
    and exists(select 1 from public.weekly_source_root_authorisations a
      where a.root_timesheet_id=p_root and a.current_entitlement_head_id=v_head and a.withdrawn_at_utc is null),
    'actual head/auth pointer; complete zero-to-two-component zero-Additional profile');
  v_inventory:=private.weekly_source_effective_inventory_v1(p_root);
  select coalesce(jsonb_agg(c.value-'component_sha256' order by (c.value->>'component_ordinal')::integer),'[]'::jsonb)
    into v_actual from jsonb_array_elements(v_inventory->'components') c(value);
  select coalesce(jsonb_agg(c.value->'component_id' order by (c.value->>'component_ordinal')::integer),'[]'::jsonb)
    into v_component_ids from jsonb_array_elements(v_inventory->'components') c(value);
  perform pg_temp.bpsx_assert(v_inventory->>'ok'='true'
    and v_inventory#>>'{approval_basis,coverage_complete}'='true'
    and v_inventory#>>'{approval_basis,origin,kind}'='COMMITTED_SOURCE_HEAD_V1'
    and v_inventory->>'head_id'=v_head::text and v_actual=v_expected,
    'actual complete I1 certificate equals every canonical Source component, including exact zero');
  if jsonb_array_length(v_component_ids)=1 then v_component:=(v_component_ids->>0)::uuid; end if;
  if p_decision='APPROVE_UPDATED_HOURS' then
    perform pg_temp.bpsx_assert(exists(select 1 from private.weekly_source_accepted_publication_actions_v1 a
      where a.decision_bundle_id=v_bundle_id and a.bundle_revision=2 and a.planned_head_id=v_head
        and a.action='APPROVE_UPDATED_HOURS' and a.actor_user_id=v_actor
        and a.idempotency_key=p_key and a.final_revision_id=p_final
        and a.request_sha256=private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1',v_public-'idempotency_key')),
      'real I2 action written by public APP before chosen capture/publication');
  else
    perform pg_temp.bpsx_assert(not exists(select 1 from private.weekly_source_accepted_publication_actions_v1 a
      where a.decision_bundle_id=v_bundle_id),'KEEP gets no normal-Final APP exception');
  end if;
  return jsonb_build_object('request',v_public,'result',v_reply,'head_id',v_head,
    'component_id',v_component,'component_ids',v_component_ids);
end $f$;

create temporary table bpsx_observation(stage text primary key, value jsonb not null) on commit drop;
-- Proof observations are readbacks, not HEAD, authorisation or financial authority.
create function pg_temp.bpsx_observed(p_stage text) returns jsonb language sql as
$$select value from pg_temp.bpsx_observation where stage=p_stage$$;
do $bpsx_real_variants$
declare
 v_actor constant uuid:='b8560000-0000-4000-8000-000000000001';
 v_import jsonb;v_later jsonb;v_root uuid;v_event_1 uuid;v_event_2 uuid;v_final uuid;
 v_before jsonb;v_initial jsonb;v_answer jsonb;v_zero jsonb;v_four jsonb;v_keep jsonb;
 v_head jsonb;v_view jsonb;v_export jsonb;v_push jsonb;v_inventory jsonb;v_replay jsonb;
 v_details jsonb;v_before_keep jsonb;v_component uuid;v_first_payload jsonb;v_zero_payload jsonb;
 v_ui019 jsonb;v_ui020 jsonb;v_ui021 jsonb;v_scope_before jsonb;v_push_result jsonb;
 v_notification public.candidate_notifications%rowtype;v_notice_count integer;v_four_notice_count integer;
 v_no_account jsonb;v_multi_notification jsonb;v_zero_notification jsonb;v_four_notification jsonb;
 v_continuity_before jsonb;v_duplicate jsonb;v_lineage_rows integer;
begin
 -- All real finalisation cutoffs are before the task date; no clock/cutoff mutation.
 v_import:=pg_temp.bpsx_import(true,'2026-09-06',jsonb_build_array(
   jsonb_build_object('key','PVX-INITIAL-SHIFT-1','date','2026-09-01','end','19:00','minutes',570,'break',30,'expense',0,'sign',1),
   jsonb_build_object('key','PVX-INITIAL-SHIFT-2','date','2026-09-02','end','17:00','minutes',420,'break',60,'expense',0,'sign',1)),
   'PVX-GENUINE-INITIAL-TWO-SHIFTS');
 select l.timesheet_id,r.work_event_id into strict v_root,v_event_1
 from public.weekly_source_row_timesheet_lineages l
 join public.weekly_source_row_resolutions r on r.id=l.row_resolution_id
 join public.weekly_source_upload_rows u on u.id=r.upload_row_id
 where u.upload_id=(v_import->>'upload_id')::uuid and u.external_source_key='PVX-INITIAL-SHIFT-1';
 select r.work_event_id into strict v_event_2 from public.weekly_source_row_resolutions r
 join public.weekly_source_upload_rows u on u.id=r.upload_row_id
 join public.weekly_source_row_timesheet_lineages l on l.row_resolution_id=r.id
 where u.upload_id=(v_import->>'upload_id')::uuid and u.external_source_key='PVX-INITIAL-SHIFT-2'
   and l.timesheet_id=v_root;
 perform pg_temp.bpsx_assert(v_event_1<>v_event_2,'two genuine distinct work events on one real root');
 perform pg_temp.bpsx_project_prior((v_import->>'final_revision_id')::uuid,v_root,'PVX-FIRST-PROJECTION-330');
 perform pg_temp.bpsx_assert(public.weekly_source_first_authorise_v1(v_root,v_root,null,v_actor)->>'ok'='true',
   'genuine original two-shift Source first authorisation');
 select to_jsonb(f) into strict v_before from public.timesheets_financials f where f.timesheet_id=v_root and f.is_current;
 v_initial:=private.weekly_source_candidate_initial_approval_v2(v_root);
 v_view:=private.weekly_source_candidate_view_v1(v_root);
 v_export:=private.weekly_source_export_approved_hours_v1(v_root);
 perform pg_temp.bpsx_assert(v_initial->>'state'='AVAILABLE'
   and (v_initial->>'total_hours')::numeric=16.5 and jsonb_array_length(v_initial->'rows')=2
   and v_initial#>>'{rows,0,end}'='19:00' and v_initial#>>'{rows,1,end}'='17:00'
   and v_view->'approved_hours_to_be_paid'=v_initial->'rows'
   and v_export->>'state'='NO_APPROVED_ENTITLEMENT'
   and v_export->'total_hours'='null'::jsonb,
   'genuine two-shift initial certificate; HEAD-only export remains no-HEAD');
 insert into pg_temp.bpsx_observation values('initial',jsonb_build_object(
   'root_id',v_root,'candidate_id','b8560000-0000-4000-8000-000000000003',
   'entitlement',v_initial,'view',v_view,'export',v_export));
 -- A real report cancels both old positions and replaces them with 8h + 5.5h.
 v_later:=pg_temp.bpsx_import(true,'2026-09-13',jsonb_build_array(
   jsonb_build_object('key','PVX-N-9H5','date','2026-09-01','end','19:00','minutes',570,'break',30,'expense',0,'sign',-1,'prior_event',v_event_1),
   jsonb_build_object('key','PVX-P-8H','date','2026-09-01','end','17:30','minutes',480,'break',30,'expense',0,'sign',1,'prior_event',v_event_1),
   jsonb_build_object('key','PVX-N-7H','date','2026-09-02','end','17:00','minutes',420,'break',60,'expense',0,'sign',-1,'prior_event',v_event_2),
   jsonb_build_object('key','PVX-P-5H5','date','2026-09-02','end','15:30','minutes',330,'break',60,'expense',0,'sign',1,'prior_event',v_event_2)),
   'PVX-GENUINE-LATER-TWO-SHIFTS');
 v_final:=(v_later->>'final_revision_id')::uuid;
 perform pg_temp.bpsx_project_prior(v_final,v_root,'PVX-PROJECTION-270');
 v_answer:=pg_temp.bpsx_decide(v_root,v_final,'APPROVE_UPDATED_HOURS','PVX-APP-TWO-SHIFTS-13H5');
 v_head:=private.weekly_source_candidate_head_hours_v2(v_root);
 v_view:=private.weekly_source_candidate_view_v1(v_root);
 v_export:=private.weekly_source_export_approved_hours_v1(v_root);
 v_push:=private.weekly_source_candidate_hours_push_payload_v1(v_root);
 select coalesce(jsonb_agg(to_jsonb(d) order by d.component_id),'[]'::jsonb) into v_details
 from private.bpay_next_source_chosen_detail d where d.head_id=(v_answer->>'head_id')::uuid;
 perform pg_temp.bpsx_assert(v_head->>'state'='AVAILABLE' and (v_head->>'total_hours')::numeric=13.5
   and jsonb_array_length(v_head->'rows')=2 and v_head#>>'{rows,0,end}'='17:30'
   and v_head#>>'{rows,1,end}'='15:30' and jsonb_array_length(v_details)=2
   and not exists(select 1 from private.bpay_next_source_chosen_detail d
     join public.weekly_source_entitlement_head_components c on c.head_id=d.head_id and c.component_id=d.component_id
     where d.head_id=(v_answer->>'head_id')::uuid
       and (d.component_sha256<>c.component_sha256 or d.detail_sha256<>sha256(convert_to(d.detail_json::text,'UTF8'))
         or d.detail_json->>'work_event_id' not in (v_event_1::text,v_event_2::text)))
   and v_view->'approved_hours_to_be_paid'=v_head->'rows'
   and v_export->>'state'='AVAILABLE' and (v_export->>'total_hours')::numeric=13.5
   and v_push->>'ok'='true' and (v_push#>>'{template_params,approved_hours_total}')::numeric=13.5
   and v_push#>'{template_params,approved_hours}'=v_head->'rows',
   'genuine multi-shift APP exact chosen records/card/HEAD scalar/push');
 v_first_payload:=v_push;
 -- Same/different/absent Candidate TEST submissions are factual fixture inputs,
 -- never fabricated Source approval evidence. Subtransaction restores all side effects.
 select jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),
   'inventory',private.weekly_source_effective_inventory_v1(v_root)) into strict v_scope_before
 from public.timesheets t join public.timesheets_financials f on f.timesheet_id=t.timesheet_id and f.is_current
 where t.timesheet_id=v_root;
 begin
   update public.timesheets set r2_nurse_key='presentation-test-submission',
     img_sha256_nurse=repeat('51',32),actual_schedule_json=jsonb_build_array(
       jsonb_build_object('row_key','PVX-submitted-1','date','2026-09-01','start','09:00','end','17:30','break_minutes',30),
       jsonb_build_object('row_key','PVX-submitted-2','date','2026-09-02','start','09:00','end','15:30','break_minutes',60))
   where timesheet_id=v_root;
   v_ui019:=private.weekly_source_candidate_view_v1(v_root);
   update public.timesheets set actual_schedule_json=jsonb_set(actual_schedule_json,'{1,end}','"18:00"'::jsonb)
   where timesheet_id=v_root;
   v_ui020:=private.weekly_source_candidate_view_v1(v_root);
   update public.timesheets set r2_nurse_key=null,img_sha256_nurse=null,actual_schedule_json='[]'::jsonb
   where timesheet_id=v_root;
   v_ui021:=private.weekly_source_candidate_view_v1(v_root);
   raise exception using errcode='ZPVC1',message='PRESENTATION_TEST_SUBMISSION_ROLLBACK';
 exception when sqlstate 'ZPVC1' then null;
 end;
 perform pg_temp.bpsx_assert((select jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),
   'inventory',private.weekly_source_effective_inventory_v1(v_root))=v_scope_before
   from public.timesheets t join public.timesheets_financials f on f.timesheet_id=t.timesheet_id and f.is_current
   where t.timesheet_id=v_root),'factual Candidate submissions restore exact root/TSFIN/I1');
 perform pg_temp.bpsx_assert(v_ui019->'approved_hours_differ'='false'::jsonb
   and v_ui019->'approved_hours_to_be_paid'='[]'::jsonb
   and jsonb_array_length(v_ui020->'approved_hours_to_be_paid')=2
   and v_ui020#>>'{approved_hours_to_be_paid,1,end}'='15:30'
   and v_ui020#>>'{submitted_timesheet,1,end}'='18:00'
   and v_ui021->'submitted_timesheet'='[]'::jsonb
   and jsonb_array_length(v_ui021->'approved_hours_to_be_paid')=2,
   'actual UI019/UI020/UI021 certified two-shift owner outputs');
 insert into pg_temp.bpsx_observation values('ui019',v_ui019),('ui020',v_ui020),('ui021',v_ui021);
 v_no_account:=private.weekly_source_candidate_hours_push_v1(v_root);
 perform pg_temp.bpsx_assert(v_no_account->>'pushed'='false'
   and v_no_account->>'reason'='NO_SINGLE_ACTIVE_ACCOUNT','actual certified HEAD without account withholds');
 insert into public.candidate_app_accounts(id,environment,email_normalized,status)
 values('b8560000-0000-4000-8000-000000000091','TEST','presentation-variants-candidate@example.test','ACTIVE');
 insert into public.candidate_app_global_membership_links(membership_id,global_account_identity_hmac,
   account_id,candidate_id,membership_generation,state) values(
   'b8560000-0000-4000-8000-000000000092',decode(repeat('52',32),'hex'),
   'b8560000-0000-4000-8000-000000000091','b8560000-0000-4000-8000-000000000003',1,'ACTIVE');
 v_push_result:=private.weekly_source_candidate_hours_push_v1(v_root);
 select * into strict v_notification from public.candidate_notifications
 where id=(v_push_result->>'notification_id')::uuid;
 v_multi_notification:=to_jsonb(v_notification);
 select count(*) into v_notice_count from public.candidate_notifications where timesheet_id=v_root
   and event_type='TIMESHEET_HOURS_UPDATED';
 perform private.weekly_source_candidate_hours_push_v1(v_root);
 perform pg_temp.bpsx_assert(v_push_result->>'pushed'='true' and v_notification.push_state='PENDING'
   and v_notice_count=1 and (select count(*) from public.candidate_notifications
     where timesheet_id=v_root and event_type='TIMESHEET_HOURS_UPDATED')=v_notice_count,
   'actual stored multi-shift notification and literal dedupe without delivery');
 insert into pg_temp.bpsx_observation values('multi',jsonb_build_object(
   'root_id',v_root,'head_id',v_answer->'head_id','entitlement',v_head,'view',v_view,
   'export',v_export,'push',v_push,'no_account',v_no_account,
   'notification',v_multi_notification,'notification_count',v_notice_count));
 select jsonb_build_object('resolved_root_id',t.timesheet_id,'financial',to_jsonb(f),
   'inventory',private.weekly_source_effective_inventory_v1(v_root),
   'approved',private.weekly_source_candidate_head_hours_v2(v_root),
   'view',(private.weekly_source_candidate_view_v1(v_root) - array['request_id','scope_id','request_kind']),
   'detail',(select coalesce(jsonb_agg(to_jsonb(d) order by d.component_id),'[]'::jsonb)
     from private.bpay_next_source_chosen_detail d where d.head_id=(v_answer->>'head_id')::uuid))
   into strict v_continuity_before
 from public.timesheets t join public.timesheets_financials f on f.timesheet_id=t.timesheet_id and f.is_current
 where t.timesheet_id=v_root;
 -- Whole actual N8/N5.5 report, no replacement: empty live vector is certified zero.
 v_later:=pg_temp.bpsx_import(true,'2026-09-20',jsonb_build_array(
   jsonb_build_object('key','PVX-ZERO-N8','date','2026-09-01','end','17:30','minutes',480,'break',30,'expense',0,'sign',-1,'prior_event',v_event_1),
   jsonb_build_object('key','PVX-ZERO-N5H5','date','2026-09-02','end','15:30','minutes',330,'break',60,'expense',0,'sign',-1,'prior_event',v_event_2)),
   'PVX-GENUINE-FULL-NEGATIVE-ZERO');
 v_final:=(v_later->>'final_revision_id')::uuid;
 -- A genuine later upload contains the same durable work events again. BEFORE
 -- its Office decision, the existing complete approval remains the chosen fact.
 select count(*) into v_lineage_rows from public.weekly_source_row_resolutions r
 join public.weekly_source_upload_rows u on u.id=r.upload_row_id
 join public.weekly_source_row_timesheet_lineages l on l.row_resolution_id=r.id
 where r.work_event_id=v_event_1 and r.mapping_state='RESOLVED' and l.timesheet_id=v_root;
 perform pg_temp.bpsx_assert(v_lineage_rows>=3 and
   (select jsonb_build_object('resolved_root_id',t.timesheet_id,'financial',to_jsonb(f),
     'inventory',private.weekly_source_effective_inventory_v1(v_root),
     'approved',private.weekly_source_candidate_head_hours_v2(v_root),
     'view',(private.weekly_source_candidate_view_v1(v_root) - array['request_id','scope_id','request_kind']),
     'detail',(select coalesce(jsonb_agg(to_jsonb(d) order by d.component_id),'[]'::jsonb)
       from private.bpay_next_source_chosen_detail d where d.head_id=(v_answer->>'head_id')::uuid))=v_continuity_before
    from public.timesheets t join public.timesheets_financials f on f.timesheet_id=t.timesheet_id and f.is_current
    where t.timesheet_id=v_root),
   'genuine later report retains exact certified HEAD/card/root/current TSFIN/chosen detail before decision');
 -- BEGIN GENUINE LIVE REQUEST BINDING (no financial authority or request is seeded)
 v_view:=private.weekly_source_candidate_view_v1(v_root);
 perform pg_temp.bpsx_assert(
   jsonb_typeof(v_view->'request_id')='string'
   and jsonb_typeof(v_view->'scope_id')='string'
   and v_view->>'request_kind'='SUBMIT_TIMESHEET'
   and (select count(*)=1
     from public.weekly_timesheet_submission_request_memberships m
     join public.weekly_timesheet_submission_requests s on s.id=m.submission_request_id
     join public.weekly_candidate_outreach_generations g
       on g.candidate_cohort_id=s.candidate_cohort_id and g.source_cycle_id=s.source_cycle_id
       and g.candidate_id=s.candidate_id and g.generation_number=s.request_generation
     join public.weekly_candidate_cohorts cohort on cohort.id=s.candidate_cohort_id
       and cohort.current_submission_generation_id=g.id
     join public.timesheets root on root.timesheet_id=v_root
     join public.contracts contract on contract.id=root.contract_id
     join public.weekly_source_final_revisions final on final.id=v_final
       and final.source_cycle_id=s.source_cycle_id and final.upload_id=s.current_upload_id
     join public.weekly_source_report_scopes report on report.id=final.report_scope_id
       and report.source_cycle_id=final.source_cycle_id
       and report.current_final_revision_id=final.id
       and report.current_complete_upload_id=final.upload_id
       and report.current_projection_publication_id=s.current_projection_publication_id
     join public.weekly_source_groups source_group on source_group.id=report.source_group_id
     join public.weekly_source_projection_publications publication
       on publication.id=s.current_projection_publication_id
       and publication.upload_id=final.upload_id
       and publication.source_cycle_id=final.source_cycle_id and publication.state='CURRENT'
     join public.candidate_app_global_membership_links link
       on link.candidate_id=contract.candidate_id and link.state='ACTIVE'
     join public.candidate_app_accounts account on account.id=link.account_id
       and account.status='ACTIVE' and account.environment='TEST'
     where g.id=(v_view->>'request_id')::uuid and m.id=(v_view->>'scope_id')::uuid
       and g.request_kind='SUBMIT_TIMESHEET' and g.state='ACTIVE' and s.state='ACTIVE'
       and m.state='WAITING'
       and s.candidate_id=contract.candidate_id and g.client_id=contract.client_id
       and cohort.candidate_id=contract.candidate_id and cohort.source_cycle_id=final.source_cycle_id
       and cohort.client_id=contract.client_id
       and m.contract_id=contract.id and m.client_id=contract.client_id
       and m.week_ending=root.week_ending_date
       and report.client_id=contract.client_id
       and final.state='CURRENT' and final.authority_scope_kind='NHSP_REPORT_SCOPE'
       and final.source_cycle_id=(v_later->>'cycle_id')::uuid
       and final.upload_id=(v_later->>'upload_id')::uuid
       and publication.id=(v_later->>'publication_id')::uuid
       and s.environment='TEST' and report.environment=s.environment
       and source_group.environment=s.environment
       and s.agency_id=report.agency_id and source_group.agency_id=report.agency_id
       and s.membership_hash=g.membership_hash
       and m.expected_source_fingerprint=private.weekly_source_office_missing_scope_fingerprint_v1(
         publication.id,contract.candidate_id,contract.client_id,contract.id,root.week_ending_date)
       and link.membership_id='b8560000-0000-4000-8000-000000000092'::uuid
       and account.id='b8560000-0000-4000-8000-000000000091'::uuid
       and (select count(*) from public.candidate_app_global_membership_links active_link
         join public.candidate_app_accounts active_account on active_account.id=active_link.account_id
         where active_link.candidate_id=contract.candidate_id and active_link.state='ACTIVE'
           and active_account.status='ACTIVE')=1)
   and private.weekly_source_candidate_view_request_v1(
     private.weekly_source_candidate_week_context_v1(v_root))
     =jsonb_build_object('request_id',v_view->'request_id',
       'scope_id',v_view->'scope_id','request_kind',v_view->'request_kind'),
   'genuine later upload live SUBMIT_TIMESHEET request binds exact Final/scope/Candidate/account/membership');
 -- END GENUINE LIVE REQUEST BINDING
 insert into pg_temp.bpsx_observation values('later_upload_continuity',jsonb_build_object(
   'root_id',v_root,'head_id',v_answer->'head_id','durable_lineage_rows',v_lineage_rows,
   'entitlement',private.weekly_source_candidate_head_hours_v2(v_root)));
 perform pg_temp.bpsx_assert(private.weekly_source_ordinary_projection_current_segments_v1(v_root,v_final)='[]'::jsonb,
   'actual later full negatives leave zero complete Source worked vector');
 perform pg_temp.bpsx_project_prior(v_final,v_root,'PVX-PROJECTION-ZERO');
 v_zero:=pg_temp.bpsx_decide(v_root,v_final,'APPROVE_UPDATED_HOURS','PVX-APP-CERTIFIED-ZERO');
 v_head:=private.weekly_source_candidate_head_hours_v2(v_root);
 v_view:=private.weekly_source_candidate_view_v1(v_root);
 v_export:=private.weekly_source_export_approved_hours_v1(v_root);
 v_push:=private.weekly_source_candidate_hours_push_payload_v1(v_root);
 perform pg_temp.bpsx_assert(v_head->>'state'='AVAILABLE' and v_head->>'certified_zero'='true'
   and (v_head->>'total_hours')::numeric=0 and v_head->'rows'='[]'::jsonb
   and v_zero->'component_ids'='[]'::jsonb
   and not exists(select 1 from private.bpay_next_source_chosen_detail d where d.head_id=(v_zero->>'head_id')::uuid)
   and v_view->'approved_hours_to_be_paid'='[]'::jsonb
   and v_export->>'state'='AVAILABLE' and (v_export->>'total_hours')::numeric=0
   and v_push->>'ok'='true' and (v_push#>>'{template_params,approved_hours_total}')::numeric=0
   and v_push#>'{template_params,approved_hours}'='[]'::jsonb and v_push is distinct from v_first_payload,
   'genuine certified zero: no guessed source shifts; scalar zero; payload changes');
 v_zero_payload:=v_push;
 select to_jsonb(n) into strict v_zero_notification from public.candidate_notifications n
 where n.timesheet_id=v_root and n.event_type='TIMESHEET_HOURS_UPDATED'
   and (n.template_params->>'approved_hours_total')::numeric=0;
 perform pg_temp.bpsx_assert((select count(*) from public.candidate_notifications
   where timesheet_id=v_root and event_type='TIMESHEET_HOURS_UPDATED')=v_notice_count+1,
   'actual zero HEAD trigger adds exactly one changed-entitlement notice');
 insert into pg_temp.bpsx_observation values('zero',jsonb_build_object(
   'root_id',v_root,'head_id',v_zero->'head_id','entitlement',v_head,'view',v_view,
   'export',v_export,'push',v_push,'notification',v_zero_notification,'notification_count',v_notice_count+1));
 -- A later genuine positive on the same event supplies four hours, not a seeded HEAD.
 v_later:=pg_temp.bpsx_import(true,'2026-09-27',jsonb_build_array(
   jsonb_build_object('key','PVX-RESTORE-P4','date','2026-09-01','end','13:00','minutes',240,'break',0,'expense',0,'sign',1,'prior_event',v_event_1)),
   'PVX-GENUINE-RESTORE-FOUR-HOURS');
 v_final:=(v_later->>'final_revision_id')::uuid;
 perform pg_temp.bpsx_project_prior(v_final,v_root,'PVX-PROJECTION-80');
 v_four:=pg_temp.bpsx_decide(v_root,v_final,'APPROVE_UPDATED_HOURS','PVX-APP-FOUR-HOURS');
 v_component:=(v_four->>'component_id')::uuid;
 select coalesce(jsonb_agg(jsonb_build_object('component_id',d.component_id,'detail_json',d.detail_json,
   'detail_sha256',encode(d.detail_sha256,'hex'),'component_sha256',encode(d.component_sha256,'hex'))
   order by d.component_id),'[]'::jsonb) into v_before_keep
 from private.bpay_next_source_chosen_detail d where d.head_id=(v_four->>'head_id')::uuid;
 select to_jsonb(n) into strict v_four_notification from public.candidate_notifications n
 where n.timesheet_id=v_root and n.event_type='TIMESHEET_HOURS_UPDATED'
   and (n.template_params->>'approved_hours_total')::numeric=4;
 select count(*) into v_four_notice_count from public.candidate_notifications
 where timesheet_id=v_root and event_type='TIMESHEET_HOURS_UPDATED';
 perform pg_temp.bpsx_assert(v_four_notice_count=v_notice_count+2,
   'actual four-hour HEAD trigger adds exactly one new notice after zero');
 insert into pg_temp.bpsx_observation values('four',jsonb_build_object(
   'root_id',v_root,'head_id',v_four->'head_id',
   'entitlement',private.weekly_source_candidate_head_hours_v2(v_root),
   'view',private.weekly_source_candidate_view_v1(v_root),
   'export',private.weekly_source_export_approved_hours_v1(v_root),
   'push',private.weekly_source_candidate_hours_push_payload_v1(v_root),
   'notification',v_four_notification,'notification_count',v_four_notice_count));
 v_keep:=pg_temp.bpsx_decide(v_root,v_final,'KEEP_CURRENTLY_APPROVED_HOURS','PVX-KEEP-PROTECTED-FOUR');
 v_head:=private.weekly_source_candidate_head_hours_v2(v_root);
 v_view:=private.weekly_source_candidate_view_v1(v_root);
 v_export:=private.weekly_source_export_approved_hours_v1(v_root);
 v_push:=private.weekly_source_candidate_hours_push_payload_v1(v_root);
 select coalesce(jsonb_agg(jsonb_build_object('component_id',d.component_id,'detail_json',d.detail_json,
   'detail_sha256',encode(d.detail_sha256,'hex'),'component_sha256',encode(d.component_sha256,'hex'))
   order by d.component_id),'[]'::jsonb) into v_details
 from private.bpay_next_source_chosen_detail d where d.head_id=(v_keep->>'head_id')::uuid;
 perform pg_temp.bpsx_assert(v_head->>'state'='AVAILABLE' and (v_head->>'total_hours')::numeric=4
   and v_head#>>'{rows,0,end}'='13:00' and jsonb_array_length(v_head->'rows')=1
   and (v_keep->>'component_id')::uuid=v_component and v_before_keep=v_details
   and exists(select 1 from public.weekly_source_entitlement_heads h
     where h.id=(v_keep->>'head_id')::uuid and h.authority_kind='PROTECTED')
   and v_view->'approved_hours_to_be_paid'=v_head->'rows'
   and v_export->>'state'='AVAILABLE' and (v_export->>'total_hours')::numeric=4
   and v_push->>'ok'='true' and (v_push#>>'{template_params,approved_hours_total}')::numeric=4
   and v_push is distinct from v_zero_payload,
   'genuine APP4→KEEP PROTECTED preserves exact chosen clocks/hash/content');
 perform private.weekly_source_candidate_hours_push_v1(v_root);
 perform pg_temp.bpsx_assert((select count(*) from public.candidate_notifications
   where timesheet_id=v_root and event_type='TIMESHEET_HOURS_UPDATED')=v_four_notice_count,
   'KEEP same chosen hours and explicit retry add no notification');
 insert into pg_temp.bpsx_observation values('protected',jsonb_build_object(
   'root_id',v_root,'head_id',v_keep->'head_id','entitlement',v_head,'view',v_view,
   'export',v_export,'push',v_push,'notification',v_four_notification,'notification_count',v_four_notice_count));
 -- Identical NHSP reupload is admitted by the real owner as DUPLICATE, not as
 -- a fabricated second statement. Original live root/TF and immutable approval
 -- remain unchanged. This does not claim corrected-byte post-Final replacement.
 select jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),
   'inventory',private.weekly_source_effective_inventory_v1(v_root),
   'approved',private.weekly_source_candidate_head_hours_v2(v_root),
   'view',private.weekly_source_candidate_view_v1(v_root),
   'detail',(select coalesce(jsonb_agg(to_jsonb(d) order by d.component_id),'[]'::jsonb)
     from private.bpay_next_source_chosen_detail d where d.head_id=(v_keep->>'head_id')::uuid),
   'upload',(select to_jsonb(u) from public.weekly_source_uploads u where u.id=(v_later->>'upload_id')::uuid),
   'scope',(select to_jsonb(s) from public.weekly_source_report_scopes s where s.current_complete_upload_id=(v_later->>'upload_id')::uuid))
   into strict v_continuity_before
 from public.timesheets t join public.timesheets_financials f on f.timesheet_id=t.timesheet_id and f.is_current
 where t.timesheet_id=v_root;
 v_duplicate:=public.weekly_source_upload_stage_begin_atomic_v1(v_later->'stage_request');
 perform pg_temp.bpsx_assert(v_duplicate->>'status'='DUPLICATE'
   and v_duplicate->>'logical_upload_id'=v_later->>'upload_id'
   and v_duplicate->'current_pointer_moved'='false'::jsonb
   and (select jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),
     'inventory',private.weekly_source_effective_inventory_v1(v_root),
     'approved',private.weekly_source_candidate_head_hours_v2(v_root),
     'view',private.weekly_source_candidate_view_v1(v_root),
     'detail',(select coalesce(jsonb_agg(to_jsonb(d) order by d.component_id),'[]'::jsonb)
       from private.bpay_next_source_chosen_detail d where d.head_id=(v_keep->>'head_id')::uuid),
     'upload',(select to_jsonb(u) from public.weekly_source_uploads u where u.id=(v_later->>'upload_id')::uuid),
     'scope',(select to_jsonb(s) from public.weekly_source_report_scopes s where s.current_complete_upload_id=(v_later->>'upload_id')::uuid))=v_continuity_before
    from public.timesheets t join public.timesheets_financials f on f.timesheet_id=t.timesheet_id and f.is_current
    where t.timesheet_id=v_root)
   and (select count(*) from public.candidate_notifications where timesheet_id=v_root
     and event_type='TIMESHEET_HOURS_UPDATED')=v_four_notice_count,
   'real duplicate reupload retains exact current root/TF/HEAD/chosen rows/card and notification count');
 insert into pg_temp.bpsx_observation values('duplicate_reupload',jsonb_build_object(
   'root_id',v_root,'head_id',v_keep->'head_id','status',v_duplicate->'status',
   'entitlement',private.weekly_source_candidate_head_hours_v2(v_root)));
 v_inventory:=private.weekly_source_effective_inventory_v1(v_root);
 v_replay:=public.weekly_source_later_change_decide_atomic_v1(v_answer->'request');
 perform pg_temp.bpsx_assert(v_replay->>'idempotent_replay'='true'
   and v_inventory=private.weekly_source_effective_inventory_v1(v_root)
   and private.weekly_source_candidate_hours_push_payload_v1(v_root)=v_push
   and (select to_jsonb(f)=v_before from public.timesheets_financials f where f.timesheet_id=v_root and f.is_current),
   'literal historical APP replay leaves exact protected card/push/initial TSFIN unchanged');
 perform pg_temp.bpsx_assert(not exists(select 1 from private.bpay_next_work w where w.original_timesheet_id=v_root)
   and not exists(select 1 from private.bpay_next_job j where j.candidate_id='b8560000-0000-4000-8000-000000000003')
   and exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY'),
   'no NEXT activation/financial producer substituted for real presentation certificate');
 raise notice 'PRESENTATION_GENUINE_TWO_SHIFT_ZERO_PROTECTED_VARIANTS';
end $bpsx_real_variants$;

-- END GENUINE CERTIFICATE CAPSULE

create temporary table gate11_source_counter_baseline(
 rel oid primary key, writes bigint not null check(writes>=0)
) on commit drop;
insert into pg_temp.gate11_source_counter_baseline(rel,writes)
 select relation::regclass::oid,pg_temp.ws_verify_writes(relation::regclass)
 from (values
   ('public.weekly_manager_recipient_routes'),('public.weekly_message_intents'),
   ('public.weekly_message_renders'),('public.weekly_message_dispatch_targets')
 ) watched(relation);


do $original_fixture_modes$ begin
 if (select count(*) from (values
   ('weekly_source_entitlement_head_inventory_assert','private.weekly_source_entitlement_head_inventory_assert_v1()'),
   ('weekly_source_entitlement_head_receipt_assert','private.weekly_source_entitlement_head_receipt_assert_v1()')
 ) expected(name,signature)
 join pg_trigger t on t.tgname=expected.name
   and t.tgrelid='public.weekly_source_entitlement_heads'::regclass
 join pg_constraint c on c.oid=t.tgconstraint
 join pg_proc p on p.oid=t.tgfoid
 where t.tgfoid=to_regprocedure(expected.signature) and not t.tgisinternal
   and t.tgenabled='O' and t.tgtype=21 and t.tgdeferrable and t.tginitdeferred
   and t.tgnargs=0 and octet_length(t.tgargs)=0 and t.tgqual is null
   and t.tgattr::text='' and t.tgoldtable is null and t.tgnewtable is null
   and c.conname=t.tgname and c.connamespace='public'::regnamespace
   and c.conrelid=t.tgrelid and c.contype='t' and c.condeferrable and c.condeferred
   and p.proowner=current_user::regrole and p.prosecdef and p.prokind='f'
   and p.proconfig=array['search_path=pg_catalog, pg_temp']::text[]
   and not has_function_privilege('anon',p.oid,'EXECUTE')
   and not has_function_privilege('authenticated',p.oid,'EXECUTE')
   and not has_function_privilege('service_role',p.oid,'EXECUTE'))<>2 then
  raise exception 'EXACT_ORIGINAL_NEGATIVE_FIXTURE_CONSTRAINT_MODES_REQUIRED';
 end if;
end $original_fixture_modes$;
set constraints public.weekly_source_entitlement_head_inventory_assert deferred;
set constraints public.weekly_source_entitlement_head_receipt_assert deferred;


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
   where status in ('QUEUED','RUNNING')
     and id in(select (key->>0)::uuid from pg_temp.ws_verify_keys
       where rel='public.banking_pay_workbench_jobs'::regclass);
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
        where p.oid=to_regprocedure(v_proc.ident)) in ('postgres', current_user),
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
      where action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED'
        -- scoped to fixture rows (fixture ids and fixture pending bundles): hosted TEST holds real rows
        and (object_id_text like 'c4000000-0000-4000-8000-%'
          or object_id_text in (select bundle.id::text
            from public.weekly_source_pending_entitlement_bundles bundle
            where bundle.candidate_id::text like 'c4000000-0000-4000-8000-%')))=1,
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
      where event_type='TIMESHEET_HOURS_UPDATED'
        -- scoped to fixture rows: hosted TEST holds real rows
        and (timesheet_id::text like 'c4000000-0000-4000-8000-%'
          or timesheet_id::text like 'e1000000-0000-4000-8000-%'
          or candidate_id::text like 'c4000000-0000-4000-8000-%'
          or candidate_id::text like 'e1000000-0000-4000-8000-%'))=0,
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
  -- proved: this push's HEAD-only contract sends nothing without a committed
  -- head. This is not the initial-certificate Candidate reader's state, and
  -- source clocks cannot stand in for either certificate. The week is therefore
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

  -- The prior HEAD construction is deliberately uncertified, not a positive.
  v_result:=private.weekly_source_candidate_hours_push_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_true(v_result->>'pushed'='false'
    and v_result->>'reason'='APPROVED_ENTITLEMENT_UNAVAILABLE',
    'uncertified synthetic head cannot generate a Candidate notification');
  -- Genuine outputs and stored notification are actual observations from the
  -- real initial/two-shift/zero/APP4/KEEP capsule in this same outer transaction.
  v_result:=pg_temp.bpsx_observed('multi')->'no_account';
  perform pg_temp.assert_true(v_result->>'pushed'='false'
    and v_result->>'reason'='NO_SINGLE_ACTIVE_ACCOUNT',
    'a genuine certified head with no active account writes no push');
  v_result:=private.weekly_source_candidate_hours_push_v1(
    (pg_temp.bpsx_observed('protected')->>'root_id')::uuid);
  perform pg_temp.assert_true(v_result->>'ok'='true' and v_result->>'pushed'='true',
    'actual protected certificate answers through the actual push boundary');
  select * into strict v_row from public.candidate_notifications
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
    v_row.timesheet_id=(pg_temp.bpsx_observed('protected')->>'root_id')::uuid,
    'the push is keyed to the Timesheet');
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(v_row.template_params->'approved_hours')>0,
    'the push carries the approved hours themselves');

  -- Idempotency: the same approved hours never push twice.
  select pg_catalog.count(*)::integer into v_count
  from public.candidate_notifications where event_type='TIMESHEET_HOURS_UPDATED';
  perform private.weekly_source_candidate_hours_push_v1(
    (pg_temp.bpsx_observed('protected')->>'root_id')::uuid);
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
declare v_payload jsonb;v_zero jsonb;v_four jsonb;v_count integer;v_root uuid;
begin
 v_zero:=pg_temp.bpsx_observed('zero');v_four:=pg_temp.bpsx_observed('four');
 v_root:=(v_four->>'root_id')::uuid;v_payload:=v_zero->'push';
 perform pg_temp.assert_eq(v_payload->>'approved_hours_source','COMMITTED_ENTITLEMENT_HEAD',
   'WP-14b actual certified-zero comes from the complete HEAD');
 perform pg_temp.assert_eq(jsonb_array_length(v_payload#>'{template_params,approved_hours}')::text,'0',
   'WP-14b genuine certified zero sends no Source shifts');
 perform pg_temp.assert_true((v_payload#>>'{template_params,approved_hours_total}')::numeric=0,
   'WP-14b genuine approved total zero');
 perform pg_temp.assert_true((v_zero->>'notification_count')::integer
     -(pg_temp.bpsx_observed('multi')->>'notification_count')::integer=1
   and (v_zero#>>'{notification,template_params,approved_hours_total}')::numeric=0
   and jsonb_array_length(v_zero#>'{notification,template_params,approved_hours}')=0,
   'WP-14b actual zero publication trigger stores exactly one changed zero notice');
 perform pg_temp.assert_true((v_four->>'notification_count')::integer
     -(v_zero->>'notification_count')::integer=1
   and (v_four#>>'{notification,template_params,approved_hours_total}')::numeric=4,
   'WP-14b actual later four-hour publication stores exactly one changed notice');
 perform pg_temp.assert_true(v_zero#>>'{notification,dedupe_key}'
     is distinct from v_four#>>'{notification,dedupe_key}',
   'WP-14b dedupe follows approved payload, never old Source clocks');
 perform pg_temp.assert_true((v_four#>>'{export,total_hours}')::numeric
     =(v_four#>>'{notification,template_params,approved_hours_total}')::numeric,
   'WP-14b approved scalar export and actual stored push agree');
 perform pg_temp.assert_true(not exists(select 1 from public.candidate_notifications
     where timesheet_id=v_root and push_state<>'PENDING'),
   'WP-14b all actual notifications remain PENDING; no delivery');
 select count(*) into v_count from public.candidate_notifications
   where timesheet_id=v_root and event_type='TIMESHEET_HOURS_UPDATED';
 perform private.weekly_source_candidate_hours_push_v1(v_root);
 perform pg_temp.assert_true((select count(*) from public.candidate_notifications
   where timesheet_id=v_root and event_type='TIMESHEET_HOURS_UPDATED')=v_count,
   'WP-14b real KEEP/current literal retry cannot add a notice');
 perform pg_temp.assert_eq(jsonb_array_length(
   private.weekly_source_candidate_hours_push_head_rows_v1(
     (pg_temp.bpsx_observed('protected')->>'head_id')::uuid))::text,'1',
   'WP-14b head-only helper reads one actual protected component');
 perform pg_temp.assert_true((private.weekly_source_candidate_hours_push_head_rows_v1(
   (pg_temp.bpsx_observed('protected')->>'head_id')::uuid)->0->>'hours')::numeric=4,
   'WP-14b helper uses actual HEAD hours, not old Source 16.5');
 perform pg_temp.assert_eq(private.weekly_source_candidate_hours_push_head_rows_v1(
   (v_zero->>'head_id')::uuid)::text,'[]','WP-14b complete zero has no components');
end $verify_wp14b_push_head$;

-- Old unresolved work-event fixture remains an uncertified negative.
do $verify_wp14b_unresolved_head$
declare v_before integer;v_after integer;v_audit_before integer;v_audit_after integer;
 v_withheld jsonb;v_withheld_actor uuid;v_repeat jsonb;
begin
 select count(*) into v_before from public.candidate_notifications
 where timesheet_id='e1000000-0000-4000-8000-000000000305';
 select count(*) into v_audit_before from public.audit_events
 where object_id_text='e1000000-0000-4000-8000-000000000305'
   and action='WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD';
 perform pg_temp.assert_eq((select count(*) from public.audit_events
   where object_id_text='e1000000-0000-4000-8000-000000000305'
     and action='WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD'
     and after_json->>'head_id'='e1000000-0000-4000-8000-0000000000c7')::text,
   '0','WP-14b new negative head has no pre-existing withholding audit');
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
    'e1000000-0000-4000-8000-000000000305','WP14-BK-05',1,2,
    'e1000000-0000-4000-8000-0000000000c4','STAGED',false,1,
    decode(repeat('b1',32),'hex'),decode(repeat('b2',32),'hex'),
    decode(repeat('b3',32),'hex'),'e1000000-0000-4000-8000-0000000000b4',1,
    'e1000000-0000-4000-8000-0000000000b4','c4000000-0000-4000-8000-000000000001');
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
    'e1000000-0000-4000-8000-0000000000b4',1,decode(repeat('b4',32),'hex'));
  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',superseded_at_utc=pg_catalog.transaction_timestamp(),
         superseded_by_head_id='e1000000-0000-4000-8000-0000000000c7'
   where id='e1000000-0000-4000-8000-0000000000c4';
  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('b5',32),'hex'),
         scope_change_tx_token=pg_catalog.gen_random_uuid()
   where id='e1000000-0000-4000-8000-0000000000c7';

  select pg_catalog.count(*)::integer into v_after
  from public.candidate_notifications
  where timesheet_id='e1000000-0000-4000-8000-000000000305';
  perform pg_temp.assert_eq((v_after-v_before)::text,'0',
    'WP-14b: an entitlement that cannot be described tells the Candidate NOTHING');
  select count(*) into v_audit_after from public.audit_events
  where object_id_text='e1000000-0000-4000-8000-000000000305'
    and action='WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD';
  perform pg_temp.assert_eq((v_audit_after-v_audit_before)::text,'1',
    'WP-14b exact new negative-head commit produces one withholding audit');
  select after_json,actor_user_id into strict v_withheld,v_withheld_actor
  from public.audit_events
  where object_id_text='e1000000-0000-4000-8000-000000000305'
    and action='WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD'
    and after_json->>'head_id'='e1000000-0000-4000-8000-0000000000c7';
  perform pg_temp.assert_eq(v_withheld->>'withheld_reason',
    'APPROVED_ENTITLEMENT_UNAVAILABLE',
    'WP-14b: and the silence is RECORDED, with its exact new head reason');
  perform pg_temp.assert_true(v_withheld_actor is null
    and v_withheld->>'root_timesheet_id'='e1000000-0000-4000-8000-000000000305'
    and v_withheld->'sqlstate'='null'::jsonb,
    'WP-14b F5: exact root/head withholding is the system act, not a caught boundary error');
  v_repeat:=private.weekly_source_candidate_hours_push_v1(
    'e1000000-0000-4000-8000-000000000305');
  perform pg_temp.assert_true(v_repeat->>'pushed'='false'
    and v_repeat->>'reason'='APPROVED_ENTITLEMENT_UNAVAILABLE'
    and (select count(*) from public.candidate_notifications
      where timesheet_id='e1000000-0000-4000-8000-000000000305')=v_before
    and (select count(*) from public.audit_events
      where object_id_text='e1000000-0000-4000-8000-000000000305'
        and action='WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD')=v_audit_after,
    'WP-14b literal refused push retry adds neither notification nor commit audit');
  perform pg_temp.assert_true(exists(select 1 from public.weekly_source_entitlement_heads
    where id='e1000000-0000-4000-8000-0000000000c7'
      and prior_head_id='e1000000-0000-4000-8000-0000000000c4'
      and head_revision=2 and decision_bundle_id='e1000000-0000-4000-8000-0000000000b4')
    and exists(select 1 from public.weekly_source_entitlement_heads
      where id='e1000000-0000-4000-8000-0000000000c4' and state='SUPERSEDED'
        and superseded_by_head_id='e1000000-0000-4000-8000-0000000000c7'),
    'WP-14b exact retained malformed prior head/bundle and new revision lineage');
  perform pg_temp.assert_eq(
    (select state from public.weekly_source_entitlement_heads
      where id='e1000000-0000-4000-8000-0000000000c7'),
    'COMMITTED_CURRENT',
    'WP-14b: and the entitlement publication stands - a message never undoes it');
end
$verify_wp14b_unresolved_head$;
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

  -- The preceding genuine Source capsule owns its actual source-authority
  -- contacts. These original Gate11 commands must add ZERO further writes;
  -- every initial watcher and existing-row mutation guard remains active.
  perform pg_temp.assert_true(
    pg_temp.ws_verify_writes('public.weekly_manager_recipient_routes'::regclass)=
      (select writes from pg_temp.gate11_source_counter_baseline where rel='public.weekly_manager_recipient_routes'::regclass)
    and pg_temp.ws_verify_writes('public.weekly_message_intents'::regclass)=
      (select writes from pg_temp.gate11_source_counter_baseline where rel='public.weekly_message_intents'::regclass)
    and pg_temp.ws_verify_writes('public.weekly_message_renders'::regclass)=
      (select writes from pg_temp.gate11_source_counter_baseline where rel='public.weekly_message_renders'::regclass)
    and pg_temp.ws_verify_writes('public.weekly_message_dispatch_targets'::regclass)=
      (select writes from pg_temp.gate11_source_counter_baseline where rel='public.weekly_message_dispatch_targets'::regclass),
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
