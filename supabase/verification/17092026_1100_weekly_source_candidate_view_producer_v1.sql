-- PostgreSQL 17 rollback verification: weekly_source_candidate_view_producer_v1
--
-- Gate 9 items G9-6 (the MyTMS payload producer) and G9-4 (the Office
-- `action_state` Unauthorise verdict).
--
-- Proves:
--   * the produced payload is exactly the MyTMS contract's
--     `CandidateWeeklySourceView` (ten produced members, no others);
--   * `UI-019`, `UI-020` and `UI-021` payloads from database states that
--     genuinely produce them, including `NAI-MYT-001`'s rule that no approved
--     hours exist before Office authorisation;
--   * `NAI-MYT-002`'s expense modes;
--   * the payload carries no money and no internal vocabulary;
--   * an ordinary, non-Weekly-Source Timesheet detail is byte-identical before
--     and after the single additive call;
--   * `action_state` carries the withdrawal owner's own verdict, and reports an
--     explicit unavailable verdict when that owner is not installed.
--
-- Every fixture row is rolled back.

\set ON_ERROR_STOP on
\pset pager off

begin;

select pg_catalog.set_config('request.jwt.claim.role','service_role',true);
-- Normal request-end seam for pre-existing verifier facts; no old worker jobs run.
create temporary table bpspv_existing_setup_state(snapshot_json jsonb) on commit drop;
do $presentation_existing_request_boundary$
declare
 v_old_finalising text;v_old_scope_token text;
 v_jobs_before jsonb;v_jobs_after jsonb;v_bank_before jsonb;v_bank_after jsonb;
 v_economic_before jsonb;v_economic_after jsonb;v_relation text;v_hash text;
begin
 if exists(select 1 from public.banking_pay_workbench_sessions where status='OPEN' and discarded_at_utc is null)
   or exists(select 1 from public.banking_pay_workbench_jobs where status='RUNNING')
   or exists(select 1 from public.banking_pay_workbench_candidate_delta_projection_runs where status in ('RUNNING','PROCESSING','IN_PROGRESS')) then
   raise exception using errcode='P0001',message='PRESENTATION_EXISTING_ACTIVE_LANE_NOT_QUIET';
 end if;
 select coalesce(jsonb_agg(to_jsonb(j) order by j.id),'[]'::jsonb) into v_jobs_before from public.banking_pay_workbench_jobs j;
 select jsonb_build_object('sessions',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_sessions x),'scope',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_session_scope x),'source_lines',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_source_lines x),'delta_runs',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_delta_projection_runs x)) into v_bank_before;
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
 select jsonb_build_object('sessions',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_sessions x),'scope',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_session_scope x),'source_lines',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_source_lines x),'delta_runs',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_delta_projection_runs x)) into v_bank_after;
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
  select jsonb_build_object('sessions',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_sessions x),'scope',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_session_scope x),'source_lines',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_source_lines x),'delta_runs',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_delta_projection_runs x)) into v_bank_after;
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
    select jsonb_build_object('sessions',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_sessions x),'scope',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_session_scope x),'source_lines',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_source_lines x),'delta_runs',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_delta_projection_runs x)) into v_bank_after;
  if v_bank_after is distinct from v_bank_before then
    raise exception using errcode='P0001',message='PRESENTATION_EXISTING_BANK_ROW_DRIFT';
  end if;
    if v_all_ids is distinct from v_expected_all_ids or v_other_jobs_after is distinct from v_other_jobs
       or exists(select 1 from public.banking_pay_workbench_jobs j
         where j.id=any(v_owned) and j.status not in ('QUEUED','SUCCEEDED'))
       or exists(select 1 from public.banking_pay_workbench_sessions where status='OPEN' and discarded_at_utc is null)
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
  select jsonb_build_object('sessions',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_sessions x),'scope',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_session_scope x),'source_lines',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_source_lines x),'delta_runs',(select coalesce(jsonb_agg(to_jsonb(x) order by to_jsonb(x)::text),'[]'::jsonb) from public.banking_pay_workbench_candidate_delta_projection_runs x)) into v_bank_after;
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


create or replace function pg_temp.assert_true(p_ok boolean,p_message text)
returns void language plpgsql as $verify$
begin
  if p_ok is not true then raise exception 'VERIFY_FAILED: %',p_message; end if;
end;
$verify$;

create or replace function pg_temp.assert_eq(p_left text,p_right text,p_message text)
returns void language plpgsql as $verify$
begin
  if p_left is distinct from p_right then
    raise exception 'VERIFY_FAILED: % (got %, expected %)',
      p_message,coalesce(p_left,'<null>'),coalesce(p_right,'<null>');
  end if;
end;
$verify$;

-- ---------------------------------------------------------------------------
-- 1. Structure, privileges and volatility.
-- ---------------------------------------------------------------------------
do $structure$
declare
  v_proc record;
begin
  for v_proc in
    select unnest(array[
      'private.weekly_source_candidate_week_context_v1(uuid)',
      'private.weekly_source_candidate_view_request_v1(jsonb)',
      'private.weekly_source_candidate_approved_hours_v1(jsonb)',
      'private.weekly_source_candidate_hours_shape_v1(jsonb)',
      'private.weekly_source_candidate_hours_differ_v1(jsonb,jsonb)',
      'private.weekly_source_candidate_view_v1(uuid,timestamptz)',
      'private.weekly_source_candidate_view_merge_v1(uuid,timestamptz)',
      'private.weekly_source_office_authorisation_state_v1(uuid)',
      'private.weekly_source_office_unauthorise_action_state_v1(uuid)'
    ]::text[]) as ident
  loop
    perform pg_temp.assert_true(to_regprocedure(v_proc.ident) is not null,
      'function missing: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select p.provolatile from pg_proc p where p.oid=to_regprocedure(v_proc.ident))
        in ('i','s'),
      'producer must not be VOLATILE: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select r.rolname from pg_proc p join pg_roles r on r.oid=p.proowner
       where p.oid=to_regprocedure(v_proc.ident)) in ('postgres', current_user),
      'wrong owner: '||v_proc.ident);
    perform pg_temp.assert_true(
      not exists (
        select 1 from pg_proc p,
          aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) as acl
        left join pg_roles grantee on grantee.oid=acl.grantee
        where p.oid=to_regprocedure(v_proc.ident)
          and (acl.grantee=0
            or grantee.rolname in ('anon','authenticated','service_role'))),
      'private producer is executable by PUBLIC or a browser/service role: '||v_proc.ident);
  end loop;
end;
$structure$;

-- ---------------------------------------------------------------------------
-- 2. The producer chain reads no money and takes no lock.
--    (The Office bridge is checked separately: it is allowed to call the
--    withdrawal availability owner, which reads Banking Pay evidence itself.)
-- ---------------------------------------------------------------------------
do $prohibitions$
declare
  v_body text;
  v_term text;
begin
  v_body:=lower(
    pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_view_v1(uuid,timestamptz)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_week_context_v1(uuid)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_approved_hours_v1(jsonb)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_view_request_v1(jsonb)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_view_merge_v1(uuid,timestamptz)')));

  foreach v_term in array array[
    'pay_batch','pay_bank_transfer','timesheet_pay_state','timesheets_financials',
    'invoice','remittance','recovery','pay_advance','umbrella',
    'amount_ex_vat','amount_inc_vat','rates_json','pay_rate','charge_rate',
    'source_total_cost_pence','source_shift_charge_pence','source_commission_pence',
    'source_expense_pence',
    'for update','for share','pg_advisory',
    'insert into','update public.','delete from',
    'pg_catalog.coalesce(','pg_catalog.nullif(',
    'pg_catalog.least(','pg_catalog.greatest('
  ]::text[]
  loop
    perform pg_temp.assert_true(pg_catalog.strpos(v_body,v_term)=0,
      'MyTMS producer must not mention: '||v_term);
  end loop;
end;
$prohibitions$;

-- ---------------------------------------------------------------------------
-- 3. A real Weekly Source world.
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do nothing;

insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
values ('c1000000-0000-4000-8000-000000000001','mytms-producer@example.invalid',
  'admin','not-a-real-password','MyTMS producer verifier',true);

insert into public.clients(id,name,ts_queries_email)
values
  ('c2000000-0000-4000-8000-000000000001','Producer Source Trust','manager@example.invalid'),
  ('c2000000-0000-4000-8000-000000000002','Producer Ordinary Trust','manager@example.invalid');
insert into public.client_settings(
  id,client_id,effective_from,default_submission_mode,week_ending_weekday,
  hr_validation_required,autoprocess_hr,self_bill_no_invoices_sent,
  no_timesheet_required,requires_hr
) values
  ('c2100000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
   '2026-01-01','ELECTRONIC',0,true,false,false,false,true),
  ('c2100000-0000-4000-8000-000000000002','c2000000-0000-4000-8000-000000000002',
   '2026-01-01','ELECTRONIC',0,true,false,false,false,true);

insert into public.candidates(
  id,tms_ref,first_name,last_name,display_name,email,active,key_norm,opt_in_email
) values (
  'c3000000-0000-4000-8000-000000000001','MYT-90001','Jo','Nurse','Jo Nurse',
  'jo.producer@example.invalid',true,'JO-PRODUCER',true);

insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  week_ending_weekday_snapshot,default_submission_mode,role
) values
  ('c4000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
   0,'ELECTRONIC','RMN'),
  ('c4000000-0000-4000-8000-000000000002','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}',
   0,'ELECTRONIC','RMN');

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time
) values (
  'c5000000-0000-4000-8000-000000000001','TEST','c0000000-0000-4000-8000-000000000001',
  'MYTMS_PRODUCER_VERIFY','MyTMS producer verification Roster','ROSTER',3,'15:00');
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values (
  'c5000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
  '2026-01-01','c1000000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  'c5000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
  '2026-01-01','SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,
  'manager@example.invalid','c1000000-0000-4000-8000-000000000001');

insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'c6000000-0000-4000-8000-000000000001','c5000000-0000-4000-8000-000000000001',
  '2026-09-06','2026-09-09 15:00:00+00','OPEN',1,'NONE');
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,
  physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'c7000000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'mytms-producer-verify.xlsx',decode(repeat('71',32),'hex'),100,
  '34444444-4444-4444-8444-444444444444','verify','verify',
  decode(repeat('72',32),'hex'),decode(repeat('73',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',2,2,decode(repeat('74',32),'hex'),
  'CURRENT','c1000000-0000-4000-8000-000000000001');
update public.weekly_source_cycles
set current_complete_upload_id='c7000000-0000-4000-8000-000000000001'
where id='c6000000-0000-4000-8000-000000000001';
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  'c8000000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'CYCLE','c7000000-0000-4000-8000-000000000001',1,
  decode(repeat('75',32),'hex'),decode(repeat('76',32),'hex'),'CURRENT',
  '2026-09-01 08:00:00+00');
update public.weekly_source_cycles
set projection_state='CURRENT',
    current_projection_publication_id='c8000000-0000-4000-8000-000000000001'
where id='c6000000-0000-4000-8000-000000000001';

insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
) values
  ('c9000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','2026-09-01','PROFILE_EXTERNAL_KEY',
   'producer-shift-1',decode(repeat('81',32),'hex'),
   'c5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
  ('c9000000-0000-4000-8000-000000000002','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','2026-09-02','PROFILE_EXTERNAL_KEY',
   'producer-shift-2',decode(repeat('82',32),'hex'),
   'c5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444');

insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,normalised_row_hash
) values
  ('ca000000-0000-4000-8000-000000000001','c7000000-0000-4000-8000-000000000001',
   1,'producer-shift-1','JO NURSE','PRODUCER SOURCE TRUST','2026-09-01',
   '2026-09-01 09:00:00','2026-09-01 19:00:00',30,570,'SOURCE_WORKED',
   decode(repeat('91',32),'hex')),
  ('ca000000-0000-4000-8000-000000000002','c7000000-0000-4000-8000-000000000001',
   2,'producer-shift-2','JO NURSE','PRODUCER SOURCE TRUST','2026-09-02',
   '2026-09-02 09:00:00','2026-09-02 17:00:00',60,420,'SOURCE_WORKED',
   decode(repeat('92',32),'hex'));

insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,candidate_id,client_id,contract_id,
  work_event_id,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint
) values
  ('cb000000-0000-4000-8000-000000000001','ca000000-0000-4000-8000-000000000001',
   1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   'c9000000-0000-4000-8000-000000000001','AUTO_UNIQUE','NEW_PROFILE_KEY',
   decode(repeat('b1',32),'hex'),
   decode(repeat('a1',32),'hex'),decode(repeat('a2',32),'hex'),
   decode(repeat('a3',32),'hex')),
  ('cb000000-0000-4000-8000-000000000002','ca000000-0000-4000-8000-000000000002',
   1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   'c9000000-0000-4000-8000-000000000002','AUTO_UNIQUE','NEW_PROFILE_KEY',
   decode(repeat('b2',32),'hex'),
   decode(repeat('a4',32),'hex'),decode(repeat('a5',32),'hex'),
   decode(repeat('a6',32),'hex'));

-- The Weekly Source root: the Candidate submitted exactly the source hours.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,
  actual_schedule_json,additional_units_week,additional_units_per_day
) values (
  'cc000000-0000-4000-8000-000000000001','MYTMS-PRODUCER-SOURCE','jo-nurse',
  'producer-source-trust','ward-a','rmn','2026-09-01 08:00:00+00',
  '2026-09-02 17:00:00+00',90,1050,'2026-09-06','verify/jo.png',repeat('a',64),
  'c4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
  '[{"row_key":"row-1","date":"2026-09-01","start":"09:00","end":"19:00","break_minutes":30},
    {"row_key":"row-2","date":"2026-09-02","start":"09:00","end":"17:00","break_minutes":60}]'::jsonb,
  '{}'::jsonb,'{}'::jsonb);

-- The ordinary, non-Weekly-Source root used for the differential.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,
  actual_schedule_json,additional_units_week,additional_units_per_day
) values (
  'cc000000-0000-4000-8000-000000000002','MYTMS-PRODUCER-ORDINARY','jo-nurse',
  'producer-ordinary-trust','ward-b','rmn','2026-09-01 08:00:00+00',
  '2026-09-01 17:00:00+00',30,510,'2026-09-06','verify/jo2.png',repeat('b',64),
  'c4000000-0000-4000-8000-000000000002','WEEKLY','HOURS',
  '[{"row_key":"row-1","date":"2026-09-01","start":"09:00","end":"17:00","break_minutes":30}]'::jsonb,
  '{}'::jsonb,'{}'::jsonb);

insert into public.contract_weeks(
  id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
  day_entries_json,totals_json
) values
  ('cd000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   '2026-09-06','SUBMITTED','ELECTRONIC','cc000000-0000-4000-8000-000000000001',
   '[]'::jsonb,'{}'::jsonb),
  ('cd000000-0000-4000-8000-000000000002','c4000000-0000-4000-8000-000000000002',
   '2026-09-06','SUBMITTED','ELECTRONIC','cc000000-0000-4000-8000-000000000002',
   '[]'::jsonb,'{}'::jsonb);

-- The lineage rows are what bind the publication to this Timesheet, and are
-- what the Office presentation's publication lookup keys on.
insert into public.weekly_source_row_timesheet_lineages(
  id,row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,
  contract_id,contract_week_id,timesheet_id,family_booking_id,timesheet_version,
  week_ending_date,lineage_fingerprint
) values
  ('ce000000-0000-4000-8000-000000000001','cb000000-0000-4000-8000-000000000001',
   'c6000000-0000-4000-8000-000000000001','c9000000-0000-4000-8000-000000000001',
   'c3000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
   'c4000000-0000-4000-8000-000000000001','cd000000-0000-4000-8000-000000000001',
   'cc000000-0000-4000-8000-000000000001','MYTMS-PRODUCER-SOURCE',1,'2026-09-06',
   decode(repeat('c1',32),'hex')),
  ('ce000000-0000-4000-8000-000000000002','cb000000-0000-4000-8000-000000000002',
   'c6000000-0000-4000-8000-000000000001','c9000000-0000-4000-8000-000000000002',
   'c3000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
   'c4000000-0000-4000-8000-000000000001','cd000000-0000-4000-8000-000000000001',
   'cc000000-0000-4000-8000-000000000001','MYTMS-PRODUCER-SOURCE',1,'2026-09-06',
   decode(repeat('c2',32),'hex'));

-- ---------------------------------------------------------------------------
-- 3a. WP-11d F10: the committed entitlement head, which is the AUTHORITY for
--     approved hours.  Seeded exactly as the coordinator writes it - bundle,
--     STAGED head, components, then the commit update - so the shapes below are
--     the real relation contents, not a stub.
-- ---------------------------------------------------------------------------
create or replace function pg_temp.seed_committed_head(
  p_head uuid,
  p_bundle uuid,
  p_revision bigint,
  p_prior uuid,
  p_components jsonb,
  p_commit boolean default true,
  -- Declared component_count, overriding the number actually inserted.  The
  -- component rows are immutable once written (`WEEKLY_SOURCE_IMMUTABLE_RECORD`),
  -- so the count-mismatch shape can only be seeded, never produced by deletion.
  p_declared_count integer default null
) returns void
language plpgsql
as $seed_head$
declare
  v_component jsonb;
  v_ordinal integer:=0;
  v_count integer:=jsonb_array_length(coalesce(p_components,'[]'::jsonb));
begin
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    p_bundle,1,'c0000000-0000-4000-8000-000000000001',
    'c3000000-0000-4000-8000-000000000001','2026-09-06','SINGLE_ROOT',
    'MYTMS-PRODUCER-SOURCE','cc000000-0000-4000-8000-000000000001',
    'c4000000-0000-4000-8000-000000000001',p_bundle,
    'c1000000-0000-4000-8000-000000000001','IMMEDIATE',
    -- Unique per bundle: the relation enforces a unique request digest.
    sha256(convert_to('request:'||p_bundle::text,'UTF8')),
    sha256(convert_to('source:'||p_bundle::text,'UTF8')),
    sha256(convert_to('choice:'||p_bundle::text,'UTF8')),
    sha256(convert_to('before:'||p_bundle::text,'UTF8')),
    array[p_head]::uuid[],'PROPOSED');

  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id
  ) values (
    p_head,'LOCKED_FINAL_SOURCE','c0000000-0000-4000-8000-000000000001',
    'c3000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
    '2026-09-06','cc000000-0000-4000-8000-000000000001','MYTMS-PRODUCER-SOURCE',
    1,p_revision,p_prior,'STAGED',coalesce(p_declared_count,v_count)=0,
    coalesce(p_declared_count,v_count),
    decode(repeat('21',32),'hex'),decode(repeat('22',32),'hex'),
    decode(repeat('23',32),'hex'),p_bundle,1,p_bundle,
    'c1000000-0000-4000-8000-000000000001');

  for v_component in select value from jsonb_array_elements(coalesce(p_components,'[]'::jsonb))
  loop
    v_ordinal:=v_ordinal+1;
    insert into public.weekly_source_entitlement_head_components(
      id,head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,segment_id,segment_key,
      work_date,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      pay_ex_vat,exclude_from_pay,origin,decision_bundle_id,bundle_revision,
      component_sha256
    ) values (
      gen_random_uuid(),p_head,v_ordinal,
      (v_component->>'component_id')::uuid,
      coalesce(v_component->>'component_kind','WORKED_TIME'),'SEGMENT',
      'seg-'||v_ordinal::text,
      v_component->>'member_identity','seg-'||v_ordinal::text,
      v_component->>'member_identity',
      (v_component->>'work_date')::date,
      (v_component->>'hours_day')::numeric,
      coalesce((v_component->>'hours_night')::numeric,0),
      coalesce((v_component->>'hours_sat')::numeric,0),
      coalesce((v_component->>'hours_sun')::numeric,0),
      coalesce((v_component->>'hours_bh')::numeric,0),
      -- A money column the producer must never read.  It is seeded non-zero on
      -- purpose so that reading it would show up in the payload scan.
      123.45,coalesce((v_component->>'exclude_from_pay')::boolean,false),
      'WEEKLY_SOURCE',p_bundle,1,
      sha256(convert_to(p_head::text||v_ordinal::text,'UTF8')));
  end loop;

  if p_commit then
    perform pg_temp.commit_head(p_head);
  end if;
end;
$seed_head$;

create or replace function pg_temp.commit_head(p_head uuid)
returns void language plpgsql as $commit_head$
begin
  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',
         committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('41',32),'hex'),
         scope_change_tx_token=gen_random_uuid()
   where id=p_head;
end;
$commit_head$;

-- Supersede a committed head with a staged successor, in the order the
-- coordinator does it: the successor must exist before it can be pointed at.
create or replace function pg_temp.supersede_head(p_old uuid,p_new uuid)
returns void language plpgsql as $supersede_head$
begin
  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',
         superseded_at_utc=pg_catalog.transaction_timestamp(),
         superseded_by_head_id=p_new
   where id=p_old;
end;
$supersede_head$;

-- The two source shifts, as hour figures: 09:00-19:00 break 30 = 9.5 h, and
-- 09:00-17:00 break 60 = 7 h.  A head that approves exactly the source.
create or replace function pg_temp.head_matching_source()
returns jsonb language sql immutable as $$
  select jsonb_build_array(
    jsonb_build_object('component_id','d1000000-0000-4000-8000-000000000001',
      'member_identity','c9000000-0000-4000-8000-000000000001',
      'work_date','2026-09-01','hours_day',9.5),
    jsonb_build_object('component_id','d1000000-0000-4000-8000-000000000002',
      'member_identity','c9000000-0000-4000-8000-000000000002',
      'work_date','2026-09-02','hours_day',7));
$$;

-- ---------------------------------------------------------------------------
-- 4. NAI-MYT-001 first half: before Office authorisation there are no approved
--    hours, so the Candidate is shown nothing to be paid.
-- ---------------------------------------------------------------------------
do $before_auth$
declare
  v jsonb;
begin
  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(v is not null,'a Weekly Source week must produce a view');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'0',
    'NAI-MYT-001: no approved hours before Office authorisation');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','false',
    'NAI-MYT-001: nothing differs before Office authorisation');
  perform pg_temp.assert_eq(jsonb_array_length(v->'submitted_timesheet')::text,'2',
    'the Candidate submission is shown untouched');
end;
$before_auth$;

-- ---------------------------------------------------------------------------
-- 4.1 A SUBMIT_TIMESHEET request is addressed by the public outreach
--     generation id used in the notification/deep link, never by the private
--     submission-request row id.  The membership remains the scope id.
-- ---------------------------------------------------------------------------
savepoint before_submit_request_identity;

insert into public.weekly_route_activations(
  id,source_cycle_id,candidate_id,client_id,audience_route,route_mode,
  activated_by_user_id,activated_at_utc
) values (
  'c6100000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'c3000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
  'CANDIDATE','CANDIDATE_FIRST','c1000000-0000-4000-8000-000000000001',
  '2026-09-01 09:00:00+00'
);
insert into public.weekly_candidate_cohorts(
  id,source_cycle_id,candidate_id,client_id,manager_recipient_route_key
) values (
  'c6200000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'c3000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
  decode(repeat('61',32),'hex')
);
insert into public.weekly_candidate_outreach_generations(
  id,source_cycle_id,candidate_cohort_id,candidate_id,client_id,
  generation_number,activation_id,trigger_kind,request_kind,route_mode,
  started_at_utc,reminder_due_at_utc,deadline_at_utc,
  manual_reminder_available_at_utc,state,membership_hash
) values (
  'c6300000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'c6200000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
  'c2000000-0000-4000-8000-000000000001',1,
  'c6100000-0000-4000-8000-000000000001','OFFICE_ASK','SUBMIT_TIMESHEET',
  'CANDIDATE_FIRST','2026-09-01 09:00:00+00','2026-09-01 15:00:00+00',
  '2026-09-01 21:00:00+00','2026-09-01 10:00:00+00','ACTIVE',
  decode(repeat('62',32),'hex')
);
update public.weekly_candidate_cohorts
set current_generation_id='c6300000-0000-4000-8000-000000000001'
where id='c6200000-0000-4000-8000-000000000001';
insert into public.weekly_timesheet_submission_requests(
  id,environment,agency_id,source_cycle_id,candidate_id,candidate_cohort_id,
  request_generation,current_upload_id,current_projection_publication_id,state,
  started_at_utc,reminder_due_at_utc,deadline_at_utc,membership_hash
) values (
  'c6400000-0000-4000-8000-000000000001','TEST','c0000000-0000-4000-8000-000000000001',
  'c6000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
  'c6200000-0000-4000-8000-000000000001',1,
  'c7000000-0000-4000-8000-000000000001','c8000000-0000-4000-8000-000000000001',
  'ACTIVE','2026-09-01 09:00:00+00','2026-09-01 15:00:00+00',
  '2026-09-01 21:00:00+00',decode(repeat('63',32),'hex')
);
insert into public.weekly_timesheet_submission_request_memberships(
  id,submission_request_id,ordinal,week_ending,client_id,contract_id,
  expected_source_fingerprint,state
) values (
  'c6500000-0000-4000-8000-000000000001','c6400000-0000-4000-8000-000000000001',
  1,'2026-09-06','c2000000-0000-4000-8000-000000000001',
  'c4000000-0000-4000-8000-000000000001',decode(repeat('64',32),'hex'),'WAITING'
);

do $submit_request_identity$
declare
  v jsonb;
  v_contract_week_merge jsonb;
begin
  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v->>'request_kind','SUBMIT_TIMESHEET',
    'the candidate view exposes the submission request kind');
  perform pg_temp.assert_eq(v->>'request_id','c6300000-0000-4000-8000-000000000001',
    'request_id is the public outreach generation used by the deep link');
  perform pg_temp.assert_true(v->>'request_id' is distinct from
    'c6400000-0000-4000-8000-000000000001',
    'the private submission row id must never replace the public request id');
  perform pg_temp.assert_eq(v->>'scope_id','c6500000-0000-4000-8000-000000000001',
    'scope_id is the exact contract-week membership');

  -- The real first-submission shape has no Timesheet yet.  Removing only the
  -- Contract Week's Timesheet binding reproduces that state while retaining
  -- the same active server-owned request and scope identities.
  update public.contract_weeks
  set timesheet_id=null,status='OPEN'
  where id='cd000000-0000-4000-8000-000000000001';
  v_contract_week_merge:=
    private.weekly_source_candidate_contract_week_view_merge_v1(
      'cd000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(
    v_contract_week_merge#>>'{weekly_source_candidate_view,request_id}',
    'c6300000-0000-4000-8000-000000000001',
    'a first submission contract-week exposes the public request id');
  perform pg_temp.assert_eq(
    v_contract_week_merge#>>'{weekly_source_candidate_view,scope_id}',
    'c6500000-0000-4000-8000-000000000001',
    'a first submission contract-week exposes the exact membership scope');
  perform pg_temp.assert_eq(
    v_contract_week_merge#>>'{weekly_source_candidate_view,request_kind}',
    'SUBMIT_TIMESHEET',
    'only the server-owned first-submission request unlocks the week');
  perform pg_temp.assert_eq(
    v_contract_week_merge#>>'{weekly_source_candidate_view,expense_entry_mode}',
    'SEPARATE_TIMESHEET',
    'ordinary self-bill candidate expenses remain on a separate Timesheet');
end;
$submit_request_identity$;

rollback to savepoint before_submit_request_identity;

update public.timesheets set authorised_at_server=now()
where timesheet_id='cc000000-0000-4000-8000-000000000001';

-- ---------------------------------------------------------------------------
-- 4a. WP-11d F10.  The approved hours come from the COMMITTED ENTITLEMENT HEAD,
--     never from the source.
--
--     The independent review of WP-14 proved, on real data, that the previous
--     source-derived predicate told a Candidate about two shifts under a
--     CERTIFIED-ZERO head, and told them nothing at all when the approved hours
--     later changed, because the push deduplication key is a digest of this
--     payload and the unchanged source left it unchanged.  Each shape below is
--     executed, not inspected.
-- ---------------------------------------------------------------------------

-- 4a.1  Directly stamped authorisation without an initial certificate is
--       UNAVAILABLE, not proof that nothing was approved. The source rows
--       remain forbidden as guessed approved clocks. Genuine initial/APP/KEEP
--       certificate positives are supplied by the separate real owner prefix.
do $f10_no_head$
declare
  v jsonb;
  v_entitlement jsonb;
  v_source_rows integer;
begin
  select count(*)::integer into v_source_rows
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id
  where source_row.upload_id='c7000000-0000-4000-8000-000000000001'
    and resolution.mapping_state='RESOLVED';
  perform pg_temp.assert_eq(v_source_rows::text,'2',
    'the source rows the old predicate would have emitted are present');

  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'F10 direct authorisation stamp lacks a derivable initial certificate');
  perform pg_temp.assert_eq(v_entitlement->>'reason','INITIAL_APPROVED_HOURS_NOT_DERIVABLE',
    'F10 missing initial certificate is not a zero/no-approved assertion');

  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'0',
    'F10 the source rows must NOT be presented as approved hours');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','false',
    'F10 nothing is approved, so nothing differs');
end;
$f10_no_head$;

-- 4a.2  A CERTIFIED-ZERO head.  The Office has decided that nothing is approved.
--       The WP-14 review's shape A9: this previously pushed two shifts.
-- Complete zero comes only from actual N8/N5.5 and accepted APP, not a seeded HEAD.
do $f10_zero$
declare v jsonb;v_entitlement jsonb;
begin
 v_entitlement:=pg_temp.bpsx_observed('zero')->'entitlement';
 perform pg_temp.assert_eq(v_entitlement->>'state','AVAILABLE',
   'F10 genuine certified-zero head is a decided fact');
 perform pg_temp.assert_eq(v_entitlement->>'certified_zero','true','F10 certified zero');
 perform pg_temp.assert_true((v_entitlement->>'total_hours')::numeric=0,'F10 zero hours');
 v:=pg_temp.bpsx_observed('zero')->'view';
 perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'0',
   'F10/A9 genuine zero shows no guessed approved shifts');
end $f10_zero$;

-- 4a.3  A head that approves exactly the source, then a LATER head that approves
--       less.  The payload must CHANGE, because the push deduplication key is a
--       digest of it and a silent payload is why the WP-14 shape A9b pushed
--       nothing when the approved hours changed.
-- Actual two-shift APP followed by actual APP4/KEEP, observed at each owner boundary.
do $f10_change$
declare v_first jsonb;v_second jsonb;
begin
 v_first:=pg_temp.bpsx_observed('multi')#>'{entitlement,rows}';
 v_second:=pg_temp.bpsx_observed('protected')#>'{entitlement,rows}';
 perform pg_temp.assert_eq(jsonb_array_length(v_first)::text,'2','F10 full two-shift certificate');
 perform pg_temp.assert_eq(v_first#>>'{0,end}','17:30','F10 captured first shift clocks');
 perform pg_temp.assert_eq(jsonb_array_length(v_second)::text,'1','F10 actual later one-shift certificate');
 perform pg_temp.assert_true(v_first is distinct from v_second,
   'F10/A9b payload changes with the actual approved complete vector');
end $f10_change$;

-- 4a.4  Two committed heads for one family: contradictory evidence, reported and
--       never resolved by picking one.  The array-returning wrapper RAISES, so a
--       caller that can only carry an array cannot mistake it for "nothing is
--       approved".
--
--       HONEST NOTE ON REACHABILITY.  The schema currently makes this shape
--       unreachable through real data: the partial unique indexes
--       `..._committed_current_uq (btrim(root_family_booking_id))` and
--       `..._committed_root_uq (root_timesheet_id)` allow one committed head per
--       family, and the head root-identity trigger forces
--       `root_family_booking_id` to equal the Timesheet's own `booking_id`.  The
--       branch is therefore defence in depth.  It is nevertheless EXECUTED here
--       rather than merely inspected (Part 1, executed-review rule 1), by
--       dropping those two indexes inside this savepoint and rolling them back
--       with it.  That relaxation lives inside the verifier, so it runs
--       identically at release time; it is not a hand patch of one clone.
savepoint before_f10_two_heads;
do $f10_two_heads$
declare
  v_entitlement jsonb;
  v_raised boolean:=false;
begin
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000003a','d0000000-0000-4000-8000-0000000000bd',
    1,null,pg_temp.head_matching_source());

  drop index public.weekly_source_entitlement_heads_committed_current_uq;
  drop index public.weekly_source_entitlement_heads_committed_root_uq;

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
    r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,version,is_current,
    actual_schedule_json,additional_units_week,additional_units_per_day
  ) values (
    'cc000000-0000-4000-8000-00000000000f','MYTMS-PRODUCER-SOURCE','jo-nurse',
    'producer-source-trust','ward-a','rmn','2026-09-01 08:00:00+00',
    '2026-09-02 17:00:00+00',90,1050,'2026-09-06','verify/jo0f.png',repeat('f',64),
    'c4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',2,false,
    '[]'::jsonb,'{}'::jsonb,'{}'::jsonb);
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    'd0000000-0000-4000-8000-0000000000be',1,'c0000000-0000-4000-8000-000000000001',
    'c3000000-0000-4000-8000-000000000001','2026-09-06','SINGLE_ROOT',
    'MYTMS-PRODUCER-SOURCE','cc000000-0000-4000-8000-00000000000f',
    'c4000000-0000-4000-8000-000000000001','d0000000-0000-4000-8000-0000000000be',
    'c1000000-0000-4000-8000-000000000001','IMMEDIATE',
    sha256(convert_to('request:second-head','UTF8')),
    sha256(convert_to('source:second-head','UTF8')),
    sha256(convert_to('choice:second-head','UTF8')),
    sha256(convert_to('before:second-head','UTF8')),
    array['d0000000-0000-4000-8000-00000000004a']::uuid[],'PROPOSED');
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id,committed_at_utc,publication_receipt_digest,
    scope_change_tx_token
  ) values (
    'd0000000-0000-4000-8000-00000000004a','LOCKED_FINAL_SOURCE',
    'c0000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
    'c4000000-0000-4000-8000-000000000001','2026-09-06',
    'cc000000-0000-4000-8000-00000000000f','MYTMS-PRODUCER-SOURCE',2,2,
    'd0000000-0000-4000-8000-00000000003a','COMMITTED_CURRENT',true,0,
    decode(repeat('51',32),'hex'),decode(repeat('52',32),'hex'),
    decode(repeat('53',32),'hex'),'d0000000-0000-4000-8000-0000000000be',1,
    'd0000000-0000-4000-8000-0000000000be','c1000000-0000-4000-8000-000000000001',
    pg_catalog.transaction_timestamp(),decode(repeat('54',32),'hex'),
    gen_random_uuid());

  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'F10 two committed heads for one family is contradictory evidence');
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'MULTIPLE_COMMITTED_HEADS_FOR_FAMILY','F10 two heads reason');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'F10 two heads produce no rows at all');

  begin
    perform private.weekly_source_candidate_approved_hours_v1(
      private.weekly_source_candidate_week_context_v1(
        'cc000000-0000-4000-8000-000000000001'));
  exception when others then
    v_raised:=true;
  end;
  perform pg_temp.assert_true(v_raised,
    'F10 the array wrapper must RAISE on UNAVAILABLE, never return an empty '
    ||'array that looks like "nothing is approved"');
end;
$f10_two_heads$;
rollback to savepoint before_f10_two_heads;

-- 4a.5  A head whose declared component_count disagrees with what is stored.
savepoint before_f10_count;
do $f10_count$
declare
  v_entitlement jsonb;
begin
  -- Two components stored, three declared.  The component rows are immutable
  -- once written, so this is seeded rather than produced by a later deletion.
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000005a','d0000000-0000-4000-8000-0000000000bf',
    1,null,pg_temp.head_matching_source(),true,3);
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'reason','APPROVED_HEAD_DETAIL_NOT_DERIVABLE',
    'F10 a head whose stored components do not match its own count');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'F10 count mismatch produces no rows');
end;
$f10_count$;
rollback to savepoint before_f10_count;

-- 4a.6  A head component naming a work event that resolves to no times at all.
savepoint before_f10_times;
do $f10_times$
declare
  v_entitlement jsonb;
begin
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000006a','d0000000-0000-4000-8000-0000000000c0',
    1,null,jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-000000000003',
      'member_identity','not-a-resolvable-work-event',
      'work_date','2026-09-03','hours_day',5)));
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_HEAD_DETAIL_NOT_DERIVABLE',
    'F10 a component whose times cannot be recovered states no schedule');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'F10 unresolved times produce no rows');
end;
$f10_times$;
rollback to savepoint before_f10_times;

-- 4a.7  A head whose hours cannot be reconciled with the recovered times.  This
--       is the branch that stops the OPPOSITE error from F10: presenting clock
--       times that do not describe the hours the Office approved.
savepoint before_f10_reconcile;
do $f10_reconcile$
declare
  v_entitlement jsonb;
begin
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000007a','d0000000-0000-4000-8000-0000000000c1',
    1,null,jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-000000000004',
      'member_identity','c9000000-0000-4000-8000-000000000001',
      -- The head says 4 hours; the only times available for that work event are
      -- 09:00-19:00 break 30, which is 9.5 hours.
      'work_date','2026-09-01','hours_day',4)));
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_HEAD_DETAIL_NOT_DERIVABLE',
    'F10 times that do not describe the approved hours state no schedule');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'F10 irreconcilable hours produce no rows');
end;
$f10_reconcile$;
rollback to savepoint before_f10_reconcile;

-- 4a.8  The Office's OWN protected statement supplies the times when it exists,
--       and is preferred over the source row.  This is the real WP-14 A9b shape:
--       the Office took the shift over and reduced it to four hours, and the
--       head's buckets were computed from those times, so they reconcile.
-- Actual APP4 followed by KEEP PROTECTED with exact copied chosen-detail evidence.
-- Not a fabricated Local WAIT family/approval or a claim of that separate route.
-- Genuine Local no-Final/Draft75→HEAD65/cancel is separately retained in E170;
-- this certificate test neither replaces nor re-awards that native evidence.
do $f10_office$
declare v_entitlement jsonb;v jsonb;
begin
 v_entitlement:=pg_temp.bpsx_observed('protected')->'entitlement';
 v:=pg_temp.bpsx_observed('protected')->'view';
 perform pg_temp.assert_eq(v_entitlement->>'state','AVAILABLE','F10 real protected certificate');
 perform pg_temp.assert_true((v_entitlement->>'total_hours')::numeric=4,'F10 actual four-hour total');
 perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'1','F10 complete one row');
 perform pg_temp.assert_eq(v_entitlement#>>'{rows,0,end}','13:00','F10 exact frozen chosen end');
 perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'1',
   'F10 Candidate is shown the genuine approved shift');
 perform pg_temp.assert_eq(v->>'approved_hours_differ','true','F10 genuine no-submission differs');
 perform pg_temp.assert_true(lower(v::text) !~ '\m(pay|amount|vat|rate|charge|money|pence)\M',
   'F10 no money vocabulary while genuine approved rows are present');
 perform pg_temp.assert_true(v::text !~ '123\.45','F10 no component money leaked');
end $f10_office$;

-- ---------------------------------------------------------------------------
-- 5. Contract conformance and UI-019 (MYTMS_SAME).
--
--    WP-11d F10: UI-019 now needs a COMMITTED HEAD whose approved hours equal
--    the submission.  Without one there are no approved hours at all, which is
--    a different state (`NO_APPROVED_ENTITLEMENT`), not MYTMS_SAME.
-- ---------------------------------------------------------------------------
savepoint before_head_world;
select pg_temp.seed_committed_head(
  'd0000000-0000-4000-8000-00000000009a','d0000000-0000-4000-8000-0000000000c3',
  1,null,pg_temp.head_matching_source());

do $ui019$
declare
  v jsonb;
  v_keys text;
  v_hours jsonb;
begin
  v:=pg_temp.bpsx_observed('ui019');

  select string_agg(key,',' order by key) into v_keys
  from jsonb_object_keys(v) as k(key);
  perform pg_temp.assert_eq(v_keys,
    'approved_hours_differ,approved_hours_to_be_paid,expense_entry_mode,'
    ||'request_id,request_kind,scope_id,submitted_additional_units_per_day,'
    ||'submitted_additional_units_week,submitted_day_off_dates,submitted_timesheet',
    'CandidateWeeklySourceView members, exactly and only the ten produced');

  perform pg_temp.assert_eq(jsonb_typeof(v->'submitted_timesheet'),'array','submitted is an array');
  perform pg_temp.assert_eq(jsonb_typeof(v->'approved_hours_to_be_paid'),'array','approved is an array');
  perform pg_temp.assert_eq(jsonb_typeof(v->'approved_hours_differ'),'boolean','differ is a boolean');
  perform pg_temp.assert_eq(jsonb_typeof(v->'request_id'),'null','no live request');
  perform pg_temp.assert_eq(jsonb_typeof(v->'scope_id'),'null','no live scope');
  perform pg_temp.assert_eq(jsonb_typeof(v->'request_kind'),'null','no live request kind');

  -- CandidateWeeklySourceHours: exactly the seven required members.
  select value into v_hours from jsonb_array_elements(v->'submitted_timesheet') limit 1;
  select string_agg(key,',' order by key) into v_keys
  from jsonb_object_keys(v_hours) as k(key);
  perform pg_temp.assert_eq(v_keys,
    'additional_units,break_entry,date,end,row_key,start,worked',
    'CandidateWeeklySourceHours members');

  -- UI-019 MYTMS_SAME: the approved hours equal the submission, so no approved
  -- card is offered at all.
  perform pg_temp.assert_eq(v->>'approved_hours_differ','false','UI-019 no difference');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'0',
    'UI-019 no approved card');
  perform pg_temp.assert_eq(v->>'expense_entry_mode','SEPARATE_TIMESHEET',
    'NAI-MYT-002 ordinary source-authority expense mode');
end;
$ui019$;

-- ---------------------------------------------------------------------------
-- 6. The payload carries no money and no internal vocabulary.
-- ---------------------------------------------------------------------------
do $vocabulary$
declare
  v_text text;
  v_term text;
begin
  v_text:=lower(pg_temp.bpsx_observed('ui021')::text);
  -- Word-boundary matching: `SEPARATE_TIMESHEET` legitimately contains the
  -- letters of "rate", and the contract fixes that enum value, so a bare
  -- substring test would be wrong rather than strict.
  foreach v_term in array array[
    'source','protected','exceptional','reconciliation','reconcile',
    'remittance','invoice','recovery','payment','money','vat','rate','rates',
    'pence','charge','umbrella','batch','settled','settlement','paid'
  ]::text[]
  loop
    perform pg_temp.assert_true(v_text !~ ('\m'||v_term||'\M'),
      'MyTMS payload must never contain the term: '||v_term);
  end loop;
  -- And no money-shaped value anywhere.
  perform pg_temp.assert_true(v_text !~ '[£$€]','MyTMS payload must carry no currency symbol');
end;
$vocabulary$;

-- ---------------------------------------------------------------------------
-- 7. UI-020 (MYTMS_DIFFERENT): the approved hours differ from the submission.
-- ---------------------------------------------------------------------------
-- Source rows are immutable (`WEEKLY_SOURCE_IMMUTABLE_RECORD`), which is the
-- schema doing its job, so the difference is introduced on the Candidate's own
-- submission: the Candidate claims an hour more than the client system shows.
savepoint before_ui020;
update public.timesheets
set actual_schedule_json=
  '[{"row_key":"row-1","date":"2026-09-01","start":"09:00","end":"19:00","break_minutes":30},
    {"row_key":"row-2","date":"2026-09-02","start":"09:00","end":"18:00","break_minutes":60}]'::jsonb
where timesheet_id='cc000000-0000-4000-8000-000000000001';

do $ui020$
declare
  v jsonb;
begin
  v:=pg_temp.bpsx_observed('ui020');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','true','UI-020 difference detected');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'2',
    'UI-020 complete approved schedule');
  perform pg_temp.assert_eq(jsonb_array_length(v->'submitted_timesheet')::text,'2',
    'UI-020 the Candidate submission is still shown untouched');
  perform pg_temp.assert_eq(v#>>'{approved_hours_to_be_paid,1,end}','15:30',
    'UI-020 approved end time comes from the client system row, not the claim');
  perform pg_temp.assert_eq(v#>>'{submitted_timesheet,1,end}','18:00',
    'UI-020 the Candidate claim is shown exactly as submitted');
end;
$ui020$;
rollback to savepoint before_ui020;

-- ---------------------------------------------------------------------------
-- 8. UI-021 / NAI-MYT-001 second half: a source-authority week with no
--    Candidate submission, after Office authorisation.
--
--    The frozen contract has `additionalProperties: false` and no
--    no-submission member, so the state is expressed as an EMPTY
--    `submitted_timesheet` beside a non-empty `approved_hours_to_be_paid`.
--    That pair is unambiguous and the hours are never placed in
--    `submitted_timesheet`, so they can never be labelled as a submission.
-- ---------------------------------------------------------------------------
savepoint before_ui021;
update public.timesheets
set r2_nurse_key=null,img_sha256_nurse=null,actual_schedule_json='[]'::jsonb
where timesheet_id='cc000000-0000-4000-8000-000000000001';

do $ui021$
declare
  v jsonb;
begin
  v:=pg_temp.bpsx_observed('ui021');
  perform pg_temp.assert_eq(jsonb_array_length(v->'submitted_timesheet')::text,'0',
    'UI-021 the submitted fact stays empty and is never filled from the client system');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'2',
    'UI-021 approved hours only');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','true','UI-021 differs from nothing');
  perform pg_temp.assert_eq(jsonb_array_length(v->'submitted_additional_units_week')::text,'0',
    'UI-021 no submitted units');
  perform pg_temp.assert_eq(v#>>'{approved_hours_to_be_paid,0,worked}','true',
    'UI-021 approved rows carry hours');
  -- WP-11d: the whole-payload scan repeated in the UI-021 state, where approved
  -- rows are present.  The package's original scan only ever ran with an empty
  -- approved array.
  perform pg_temp.assert_true(
    lower(v::text) !~ ('\m(source|protected|exceptional|reconciliation|remittance'
      ||'|invoice|recovery|payment|money|vat|rate|rates|pence|charge|umbrella'
      ||'|batch|settled|settlement|paid)\M'),
    'UI-021 payload carries no internal vocabulary with approved rows present');
  perform pg_temp.assert_true(v::text !~ '123\.45',
    'UI-021 the head component money column is never read into the payload');
end;
$ui021$;
rollback to savepoint before_ui021;

-- ---------------------------------------------------------------------------
-- 9. NAI-MYT-002: the configured source-fixed expense mode hides Candidate
--    expense entry; every other source-authority week offers the separate
--    additional expense Timesheet.
-- ---------------------------------------------------------------------------
savepoint before_expense_mode;
update public.weekly_source_client_policies
set source_fixed_expenses_enabled=true
where source_group_id='c5000000-0000-4000-8000-000000000001'
  and client_id='c2000000-0000-4000-8000-000000000001';

do $expense_mode$
begin
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_view_v1(
      'cc000000-0000-4000-8000-000000000001')->>'expense_entry_mode',
    'NOT_AVAILABLE','NAI-MYT-002 source-fixed expense hides Candidate expense entry');
end;
$expense_mode$;
rollback to savepoint before_expense_mode;

-- ---------------------------------------------------------------------------
-- 9a. WP-11d F4: an integrity or configuration failure on a CONFIRMED Weekly
--     Source week is no longer indistinguishable from an ordinary week.
--
--     Before this fix the producer returned NULL for both, the detail RPC
--     omitted the member, and MyTMS silently rendered the ordinary Timesheet -
--     so a Candidate saw a blank week that should have shown hours, with no
--     signal anywhere.  The frozen contract has no error member, so the only
--     honest fail-closed route is to let the failure propagate.
-- ---------------------------------------------------------------------------
savepoint before_f4;
do $f4_policy$
declare
  v_raised boolean:=false;
  v_ordinary jsonb;
begin
  -- The effective-policy helper raises when the client policy row is gone.
  delete from public.weekly_source_client_policies
  where source_group_id='c5000000-0000-4000-8000-000000000001'
    and client_id='c2000000-0000-4000-8000-000000000001';
  begin
    perform private.weekly_source_candidate_view_v1(
      'cc000000-0000-4000-8000-000000000001');
  exception when others then
    v_raised:=true;
  end;
  perform pg_temp.assert_true(v_raised,
    'F4 a configuration failure on a confirmed Weekly Source week must not be '
    ||'swallowed into NULL');

  -- And an ordinary Timesheet is still NULL, not an error: the two states are
  -- now distinguishable, which is the whole point.
  v_ordinary:=private.weekly_source_candidate_view_v1(
    'cc000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_true(v_ordinary is null,
    'F4 an ordinary Timesheet still produces NULL');
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_view_merge_v1(
      'cc000000-0000-4000-8000-000000000002')::text,'{}',
    'F4 an ordinary Timesheet still merges nothing');
end;
$f4_policy$;
rollback to savepoint before_f4;

savepoint before_f4;
do $f4_entitlement$
declare
  v_entitlement jsonb;
  v_view jsonb;
  v_raised boolean:=false;
  v_wrapper_raised boolean:=false;
  v_message text;
begin
  -- WP-11e G3.  THIS ASSERTION CHANGED, AND THE CHANGE IS THE POINT.
  --
  -- It used to require that an UNAVAILABLE entitlement head PROPAGATE as an
  -- error through `weekly_source_candidate_view_v1`, and therefore through
  -- `public.candidate_app_timesheet_detail_v2`.  Executed against an ordinary
  -- source re-upload, that meant the worker could not open their own week at
  -- all - not the approved card, the WEEK - while their own submitted evidence
  -- was perfectly sound.  An unresolvable head is a statement about the
  -- OFFICE'S approval, not about the Candidate's submission.
  --
  -- It now degrades to a STATED unavailable result.  What the original
  -- assertion was defending is asserted here in full and separately: the
  -- resolver still states the reason, the payload never falls back to the
  -- source, and the array wrapper WP-14 uses still raises.
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-0000000000aa','d0000000-0000-4000-8000-0000000000c4',
    2,'d0000000-0000-4000-8000-00000000009a',
    jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-000000000006',
      'member_identity','still-not-a-work-event',
      'work_date','2026-09-03','hours_day',5)),false);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000009a',
    'd0000000-0000-4000-8000-0000000000aa');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-0000000000aa');

  -- 1. The reason is still STATED, in full, by the resolver the Office
  --    projection, the audit and WP-14 all read.
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'G3 the unresolvable head is still an explicit unavailable state');
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_HEAD_DETAIL_NOT_DERIVABLE',
    'G3 carrying its exact reason');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'G3 and no rows');

  -- 2. The view DOES NOT raise, and the week opens.
  begin
    v_view:=private.weekly_source_candidate_view_v1(
      'cc000000-0000-4000-8000-000000000001');
  exception when others then
    v_raised:=true;
    get stacked diagnostics v_message=message_text;
  end;
  perform pg_temp.assert_true(not v_raised,
    'G3 an unresolvable head must NOT stop the worker opening their week, got '
    ||coalesce(v_message,'<no message>'));
  perform pg_temp.assert_true(v_view is not null,
    'G3 the payload is produced, so NULL still means only "not a Weekly Source '
    ||'week"');
  perform pg_temp.assert_eq(
    jsonb_array_length(v_view->'submitted_timesheet')::text,'2',
    'G3 the Candidate still sees their OWN submitted evidence, which is sound');

  -- 3. And it still never falls back to the source (WP-11d F10).
  perform pg_temp.assert_eq(
    jsonb_array_length(v_view->'approved_hours_to_be_paid')::text,'0',
    'G3 no approved hours are shown, and in particular no source-derived ones');
  perform pg_temp.assert_eq(v_view->>'approved_hours_differ','false',
    'G3 nothing is presented as approved, so nothing differs');

  -- 4. The REAL RPC carries the week.  This is the assertion a worker cares
  --    about, driven through the real Candidate-app session in section 10a's
  --    world; here it is driven directly on the merge the RPC performs.
  perform pg_temp.assert_true(
    private.weekly_source_candidate_view_merge_v1(
      'cc000000-0000-4000-8000-000000000001') ? 'weekly_source_candidate_view',
    'G3 the additive call still returns the member rather than failing');

  -- 5. WP-14's array wrapper is UNCHANGED and still raises, so its push is
  --    still withheld on exactly this state.
  begin
    perform private.weekly_source_candidate_approved_hours_v1(
      private.weekly_source_candidate_week_context_v1(
        'cc000000-0000-4000-8000-000000000001'));
  exception when others then
    v_wrapper_raised:=true;
    get stacked diagnostics v_message=message_text;
  end;
  perform pg_temp.assert_true(v_wrapper_raised,
    'G3 the array wrapper WP-14 calls still raises on an unavailable head');
  perform pg_temp.assert_true(
    v_message like '%WEEKLY_SOURCE_APPROVED_ENTITLEMENT_UNAVAILABLE%'
    and v_message like '%APPROVED_HEAD_DETAIL_NOT_DERIVABLE%',
    'G3 and the raise still carries the exact reason');
end;
$f4_entitlement$;
rollback to savepoint before_f4;

-- ---------------------------------------------------------------------------
-- 9b. WP-11e G3: AN ORDINARY SOURCE RE-UPLOAD.
--
--     `weekly_source_uploads.state` has `SUPERSEDED` as a designed state and
--     the resolver has `EXACT_DURABLE_LINEAGE` precisely so that a corrected
--     re-upload of the same shift resolves to the SAME durable work event.
--     Before this fix, every approved component whose times come from the
--     source then had two matches, the head was `APPROVED_COMPONENT_TIMES_
--     UNRESOLVED`, and the real detail RPC RAISED: the worker could not open a
--     week whose head was sound and whose two source statements were identical.
--
--     The head here is the source-matching head seeded in section 4a.
-- ---------------------------------------------------------------------------
savepoint before_reupload;
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,
  physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'c7000000-0000-4000-8000-000000000002','c6000000-0000-4000-8000-000000000001',
  'mytms-producer-verify-corrected.xlsx',decode(repeat('61',32),'hex'),100,
  '34444444-4444-4444-8444-444444444444','verify','verify',
  decode(repeat('62',32),'hex'),decode(repeat('63',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',2,2,decode(repeat('64',32),'hex'),
  'CURRENT','c1000000-0000-4000-8000-000000000001');
update public.weekly_source_uploads set state='SUPERSEDED'
where id='c7000000-0000-4000-8000-000000000001';
update public.weekly_source_cycles
set current_complete_upload_id='c7000000-0000-4000-8000-000000000002'
where id='c6000000-0000-4000-8000-000000000001';

insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,normalised_row_hash
) values
  ('ca000000-0000-4000-8000-000000000011','c7000000-0000-4000-8000-000000000002',
   1,'producer-shift-1','JO NURSE','PRODUCER SOURCE TRUST','2026-09-01',
   '2026-09-01 09:00:00','2026-09-01 19:00:00',30,570,'SOURCE_WORKED',
   decode(repeat('93',32),'hex')),
  ('ca000000-0000-4000-8000-000000000012','c7000000-0000-4000-8000-000000000002',
   2,'producer-shift-2','JO NURSE','PRODUCER SOURCE TRUST','2026-09-02',
   '2026-09-02 09:00:00','2026-09-02 17:00:00',60,420,'SOURCE_WORKED',
   decode(repeat('94',32),'hex'));

insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,candidate_id,client_id,contract_id,
  work_event_id,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint
) values
  ('cb000000-0000-4000-8000-000000000011','ca000000-0000-4000-8000-000000000011',
   1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   'c9000000-0000-4000-8000-000000000001','AUTO_UNIQUE','EXACT_DURABLE_LINEAGE',
   decode(repeat('b3',32),'hex'),decode(repeat('a7',32),'hex'),
   decode(repeat('a8',32),'hex'),decode(repeat('a9',32),'hex')),
  ('cb000000-0000-4000-8000-000000000012','ca000000-0000-4000-8000-000000000012',
   1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   'c9000000-0000-4000-8000-000000000002','AUTO_UNIQUE','EXACT_DURABLE_LINEAGE',
   decode(repeat('b4',32),'hex'),decode(repeat('aa',32),'hex'),
   decode(repeat('ab',32),'hex'),decode(repeat('ac',32),'hex'));

do $reupload$
declare
  v_entitlement jsonb;
  v_view jsonb;
  v_rows integer;
begin
  select count(*)::integer into v_rows
  from public.weekly_source_row_resolutions as resolution
  join public.weekly_source_upload_rows as source_row
    on source_row.id=resolution.upload_row_id
  where resolution.work_event_id='c9000000-0000-4000-8000-000000000002'
    and resolution.mapping_state='RESOLVED';
  perform pg_temp.assert_eq(v_rows::text,'2',
    'G3 the shape really is two lineage-matched rows for one work event');

  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'G3 a re-upload cannot certify a synthetic head with no captured detail');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'G3 no uncertified approved shifts are guessed'
    );
  perform pg_temp.assert_eq(v_entitlement->>'reason','APPROVED_HEAD_DETAIL_NOT_DERIVABLE',
    'G3 no captured certificate can be inferred from a re-upload');

  v_view:=private.weekly_source_candidate_view_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(v_view is not null,
    'G3 the worker can open the week after a re-upload');
  perform pg_temp.assert_eq(
    jsonb_array_length(v_view->'submitted_timesheet')::text,'2',
    'G3 with their own submitted evidence intact');
end;
$reupload$;

-- A re-upload that states DIFFERENT times for the same work event is a real
-- contradiction and must STILL fail closed.  The distinct-tuple count is what
-- separates the two cases, so it is executed, not argued.
savepoint before_contradictory_reupload;
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,normalised_row_hash
) values (
  'ca000000-0000-4000-8000-000000000013','c7000000-0000-4000-8000-000000000002',
  3,'producer-shift-1b','JO NURSE','PRODUCER SOURCE TRUST','2026-09-01',
  '2026-09-01 07:00:00','2026-09-01 15:00:00',30,450,'SOURCE_WORKED',
  decode(repeat('95',32),'hex'));
insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,candidate_id,client_id,contract_id,
  work_event_id,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint
) values (
  'cb000000-0000-4000-8000-000000000013','ca000000-0000-4000-8000-000000000013',
  1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
  'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
  'c9000000-0000-4000-8000-000000000001','AUTO_UNIQUE','EXACT_DURABLE_LINEAGE',
  decode(repeat('b5',32),'hex'),decode(repeat('ad',32),'hex'),
  decode(repeat('ae',32),'hex'),decode(repeat('af',32),'hex'));
do $contradictory_reupload$
declare
  v_entitlement jsonb;
  v_view jsonb;
begin
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'G3 two DIFFERENT statements of the same shift are still a contradiction');
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_HEAD_DETAIL_NOT_DERIVABLE','G3 with the fail-closed reason');
  -- Fail-closed on the approved card, and still openable.
  v_view:=private.weekly_source_candidate_view_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(
    jsonb_array_length(v_view->'approved_hours_to_be_paid')::text,'0',
    'G3 a contradiction shows no approved hours and no source fallback');
end;
$contradictory_reupload$;
rollback to savepoint before_contradictory_reupload;

-- A SUPERSEDED upload's rows alone supply no times at all: the authority is the
-- cycle's current complete upload and nothing else.
savepoint before_only_superseded;
update public.weekly_source_cycles
set current_complete_upload_id=null
where id='c6000000-0000-4000-8000-000000000001';
do $no_authoritative_upload$
declare
  v_entitlement jsonb;
begin
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'G3 with no authoritative upload the head''s times cannot be recovered, and '
    ||'that fails closed rather than reading a superseded upload');
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_HEAD_DETAIL_NOT_DERIVABLE','G3 fail-closed reason');
end;
$no_authoritative_upload$;
rollback to savepoint before_only_superseded;
rollback to savepoint before_reupload;

-- ---------------------------------------------------------------------------
-- 9c. WP-11e G3: EVERY path that can raise through the real Candidate detail
--     RPC, enumerated and driven.  The rule the package now holds is:
--
--       * a failure to read the WEEK (client policy, family resolver) still
--         propagates - the caller cannot be handed a week that could not be
--         identified;
--       * a failure to resolve the OFFICE'S APPROVAL degrades to a stated
--         unavailable result, because the Candidate's own submitted evidence is
--         sound and they are entitled to see it.
-- ---------------------------------------------------------------------------
create or replace function pg_temp.view_outcome(p_timesheet uuid)
returns text
language plpgsql
as $outcome$
declare
  v jsonb;
begin
  v:=private.weekly_source_candidate_view_v1(p_timesheet);
  if v is null then return 'NULL'; end if;
  return 'PAYLOAD approved='
    ||jsonb_array_length(v->'approved_hours_to_be_paid')::text;
exception when others then
  -- `sqlstate` is a plpgsql special variable and must never be qualified.
  return 'RAISED '||sqlstate;
end;
$outcome$;

savepoint before_raise_census;
do $raise_census_heads$
declare
  v_head uuid;
  v_bundle uuid;
  v_revision bigint:=2;
begin
  -- (a) unresolvable component identity
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-0000000000e1','d0000000-0000-4000-8000-0000000000f1',
    v_revision,'d0000000-0000-4000-8000-00000000009a',
    jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-0000000000e1',
      'member_identity','no-such-work-event',
      'work_date','2026-09-03','hours_day',5)),false);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000009a',
    'd0000000-0000-4000-8000-0000000000e1');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-0000000000e1');
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001'),'PAYLOAD approved=0',
    'G3 APPROVED_HEAD_DETAIL_NOT_DERIVABLE degrades, it does not raise');
end;
$raise_census_heads$;
rollback to savepoint before_raise_census;

savepoint before_raise_census;
do $raise_census_reconcile$
begin
  -- (b) a head whose hours cannot be reconciled with the recovered times
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-0000000000e2','d0000000-0000-4000-8000-0000000000f2',
    2,'d0000000-0000-4000-8000-00000000009a',
    jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-0000000000e2',
      'member_identity','c9000000-0000-4000-8000-000000000001',
      'work_date','2026-09-01','hours_day',4)),false);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000009a',
    'd0000000-0000-4000-8000-0000000000e2');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-0000000000e2');
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001'),'PAYLOAD approved=0',
    'G3 APPROVED_HEAD_DETAIL_NOT_DERIVABLE degrades, it does not raise');
end;
$raise_census_reconcile$;
rollback to savepoint before_raise_census;

savepoint before_raise_census;
do $raise_census_count$
begin
  -- (c) a head whose declared component_count disagrees with what is stored
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-0000000000e3','d0000000-0000-4000-8000-0000000000f3',
    2,'d0000000-0000-4000-8000-00000000009a',pg_temp.head_matching_source(),
    false,5);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000009a',
    'd0000000-0000-4000-8000-0000000000e3');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-0000000000e3');
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001'),'PAYLOAD approved=0',
    'G3 APPROVED_HEAD_DETAIL_NOT_DERIVABLE degrades, it does not raise');
end;
$raise_census_count$;
rollback to savepoint before_raise_census;

savepoint before_raise_census;
do $raise_census_two_heads$
begin
  -- (d) two committed heads for one family.  The relaxation of the two partial
  --     unique indexes lives inside this savepoint, exactly as section 4a.4
  --     does it, so it runs identically at release time.
  drop index public.weekly_source_entitlement_heads_committed_current_uq;
  drop index public.weekly_source_entitlement_heads_committed_root_uq;
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
    r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,version,is_current,
    actual_schedule_json,additional_units_week,additional_units_per_day
  ) values (
    'cc000000-0000-4000-8000-0000000000e0','MYTMS-PRODUCER-SOURCE','jo-nurse',
    'producer-source-trust','ward-a','rmn','2026-09-01 08:00:00+00',
    '2026-09-02 17:00:00+00',90,1050,'2026-09-06','verify/joe0.png',repeat('e',64),
    'c4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',2,false,
    '[]'::jsonb,'{}'::jsonb,'{}'::jsonb);
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    'd0000000-0000-4000-8000-0000000000f4',1,'c0000000-0000-4000-8000-000000000001',
    'c3000000-0000-4000-8000-000000000001','2026-09-06','SINGLE_ROOT',
    'MYTMS-PRODUCER-SOURCE','cc000000-0000-4000-8000-0000000000e0',
    'c4000000-0000-4000-8000-000000000001','d0000000-0000-4000-8000-0000000000f4',
    'c1000000-0000-4000-8000-000000000001','IMMEDIATE',
    sha256(convert_to('request:g3-second-head','UTF8')),
    sha256(convert_to('source:g3-second-head','UTF8')),
    sha256(convert_to('choice:g3-second-head','UTF8')),
    sha256(convert_to('before:g3-second-head','UTF8')),
    array['d0000000-0000-4000-8000-0000000000e4']::uuid[],'PROPOSED');
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id,committed_at_utc,publication_receipt_digest,
    scope_change_tx_token
  ) values (
    'd0000000-0000-4000-8000-0000000000e4','LOCKED_FINAL_SOURCE',
    'c0000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
    'c4000000-0000-4000-8000-000000000001','2026-09-06',
    'cc000000-0000-4000-8000-0000000000e0','MYTMS-PRODUCER-SOURCE',2,2,
    'd0000000-0000-4000-8000-00000000009a','COMMITTED_CURRENT',true,0,
    decode(repeat('55',32),'hex'),decode(repeat('56',32),'hex'),
    decode(repeat('57',32),'hex'),'d0000000-0000-4000-8000-0000000000f4',1,
    'd0000000-0000-4000-8000-0000000000f4','c1000000-0000-4000-8000-000000000001',
    pg_catalog.transaction_timestamp(),decode(repeat('58',32),'hex'),
    gen_random_uuid());
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_approved_entitlement_v1(
      private.weekly_source_candidate_week_context_v1(
        'cc000000-0000-4000-8000-000000000001'))->>'reason',
    'MULTIPLE_COMMITTED_HEADS_FOR_FAMILY','G3 the shape really is two heads');
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001'),'PAYLOAD approved=0',
    'G3 MULTIPLE_COMMITTED_HEADS_FOR_FAMILY degrades, it does not raise');
end;
$raise_census_two_heads$;
rollback to savepoint before_raise_census;

savepoint before_raise_census;
do $raise_census_policy$
declare
  v_outcome text;
begin
  -- (e) THE ONE THAT MUST STILL RAISE.  A client policy that cannot be resolved
  --     means the WEEK cannot be read at all, not that an approval is
  --     unresolved, and the F4 ruling stands for it unchanged.
  delete from public.weekly_source_client_policies
  where source_group_id='c5000000-0000-4000-8000-000000000001'
    and client_id='c2000000-0000-4000-8000-000000000001';
  v_outcome:=pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(v_outcome like 'RAISED%',
    'G3 a configuration failure still propagates, got '||v_outcome);
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000002'),'NULL',
    'G3 and an ordinary Timesheet is still NULL, so the two remain '
    ||'distinguishable');
end;
$raise_census_policy$;
rollback to savepoint before_raise_census;

rollback to savepoint before_head_world;

-- ---------------------------------------------------------------------------
-- 10. The ordinary-Timesheet differential.
--
--     The single additive call site merges `private.…_view_merge_v1(...)` into
--     the Candidate detail result.  For every Timesheet that is not a Weekly
--     Source week that call returns `{}`, and `x || '{}'::jsonb = x` for every
--     jsonb object, so the ordinary payload is byte-identical to before the
--     edit.  Both halves are proved here.
-- ---------------------------------------------------------------------------
do $ordinary$
declare
  v_merge jsonb;
begin
  v_merge:=private.weekly_source_candidate_view_merge_v1('cc000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v_merge::text,'{}',
    'an ordinary Timesheet merges nothing into the Candidate detail');
  perform pg_temp.assert_true(
    private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000002') is null,
    'an ordinary Timesheet produces no Weekly Source view');

  -- The merge identity, on a realistic detail-shaped object.
  perform pg_temp.assert_true(
    ('{"ok":true,"timesheet":{"id":"x"},"hours":[1,2,3],"break_entry":null}'::jsonb
       ||'{}'::jsonb)::text
    ='{"ok":true,"timesheet":{"id":"x"},"hours":[1,2,3],"break_entry":null}'::jsonb::text,
    'merging an empty object is a no-op');

  -- A DAILY record never reaches the call site, and would produce nothing if it did.
  perform pg_temp.assert_true(
    private.weekly_source_candidate_view_merge_v1(
      '00000000-0000-4000-8000-0000000000ff')::text='{}',
    'an unknown Timesheet merges nothing');

  -- The single call site really is a single call, and it is on the weekly
  -- return path only, so the DAILY early return is untouched.
  perform pg_temp.assert_eq(
    (length(pg_get_functiondef(to_regprocedure(
       'public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)')))
     -length(replace(pg_get_functiondef(to_regprocedure(
       'public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)')),
       'weekly_source_candidate_view_merge_v1','')))::text,
    length('weekly_source_candidate_view_merge_v1')::text,
    'exactly one additive call in candidate_app_timesheet_detail_v2');
  perform pg_temp.assert_eq(
    (length(pg_get_functiondef(to_regprocedure(
       'public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)')))
     -length(replace(pg_get_functiondef(to_regprocedure(
       'public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)')),
       'weekly_source_candidate_contract_week_view_merge_v1','')))::text,
    length('weekly_source_candidate_contract_week_view_merge_v1')::text,
    'exactly one first-submission contract-week call in candidate_app_timesheet_detail_v2');
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_contract_week_view_merge_v1(
      'cd000000-0000-4000-8000-000000000002')::text,'{}',
    'an ordinary bound Contract Week receives no source-request unlock');
end;
$ordinary$;

-- ---------------------------------------------------------------------------
-- 10a. The real Candidate detail RPC carries it, through the single call site.
--
--      This is the end-to-end proof that the additive call actually runs inside
--      `public.candidate_app_timesheet_detail_v2` and that an ordinary Timesheet
--      comes back without the member at all.
-- ---------------------------------------------------------------------------
update public.settings_defaults
set candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  ||pg_catalog.jsonb_build_object('candidate_app_reads',true,'candidate_app_writes',true)
where id=1;

insert into public.candidate_app_accounts(
  id,environment,email_normalized,status,password_scheme,
  password_scheme_version,password_salt,password_digest,password_changed_at_utc
) values (
  'cf000000-0000-4000-8000-000000000001','TEST','jo.producer@example.invalid',
  'ACTIVE','PBKDF2-HMAC-SHA256',1,decode(repeat('91',16),'hex'),
  decode(repeat('92',32),'hex'),'2026-09-01 08:00:00+00');
insert into public.candidate_app_sessions(
  id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
  expires_at_utc,absolute_expires_at_utc
) values (
  'cf100000-0000-4000-8000-000000000001','cf000000-0000-4000-8000-000000000001',
  'TEST','c3000000-0000-4000-8000-000000000001','ACTIVE',
  extensions.digest('mytms-producer-session','sha256'),
  now()+interval '1 day',now()+interval '2 days');
insert into public.candidate_app_global_membership_links(
  membership_id,global_account_identity_hmac,account_id,candidate_id,
  candidate_code,membership_generation,state,linked_at_utc,updated_at_utc
) values (
  'cf200000-0000-4000-8000-000000000001',
  extensions.digest('mytms-producer-membership','sha256'),
  'cf000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
  'MYT-90001',1,'ACTIVE','2026-09-01 08:00:00+00','2026-09-01 08:00:00+00');

do $rpc$
declare
  v_source jsonb;
  v_ordinary jsonb;
begin
  v_source:=public.candidate_app_timesheet_detail_v2(
    'cf100000-0000-4000-8000-000000000001','TEST',
    'cc000000-0000-4000-8000-000000000001',null,null,now());
  perform pg_temp.assert_true(
    v_source ? 'weekly_source_candidate_view',
    'the Candidate detail RPC carries the Weekly Source view for a source week');
  perform pg_temp.assert_eq(
    jsonb_typeof(v_source->'weekly_source_candidate_view'),'object',
    'the member is the view object');
  perform pg_temp.assert_eq(
    (select string_agg(key,',' order by key)
     from jsonb_object_keys(v_source->'weekly_source_candidate_view') as k(key)),
    'approved_hours_differ,approved_hours_to_be_paid,expense_entry_mode,'
    ||'request_id,request_kind,scope_id,submitted_additional_units_per_day,'
    ||'submitted_additional_units_week,submitted_day_off_dates,submitted_timesheet',
    'the RPC emits the contract shape');

  v_ordinary:=public.candidate_app_timesheet_detail_v2(
    'cf100000-0000-4000-8000-000000000001','TEST',
    'cc000000-0000-4000-8000-000000000002',null,null,now());
  perform pg_temp.assert_true(
    not (v_ordinary ? 'weekly_source_candidate_view'),
    'an ordinary Timesheet payload does not carry the member at all');
end;
$rpc$;

-- ---------------------------------------------------------------------------
-- 11. G9-4: the Office `action_state` Unauthorise verdict.
-- ---------------------------------------------------------------------------
do $action_state$
declare
  v jsonb;
  v_keys text;
begin
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');

  select string_agg(key,',' order by key) into v_keys
  from jsonb_object_keys(v) as k(key);
  perform pg_temp.assert_eq(v_keys,'unauthorise,unauthorise_allowed',
    'action_state additions');

  select string_agg(key,',' order by key) into v_keys
  from jsonb_object_keys(v->'unauthorise') as k(key);
  -- WP-11d F7 adds `owner_sqlstate` and `owner_error`, so the verdict now has
  -- eleven members.  Both are always present and never null ('NONE' when the
  -- owner answered), so `jsonb_strip_nulls` at the Office call site still cannot
  -- remove one.  This is a behavioural difference recorded for the approver.
  perform pg_temp.assert_eq(v_keys,
    'authorisation_state,availability_source,available,owner_error,'
    ||'owner_sqlstate,permanent,reason,refusal_code,refusal_nature,retryable,'
    ||'withdrawn',
    'unauthorise verdict members');

  -- No member may be null: the Office projection strips nulls.
  perform pg_temp.assert_true(
    not exists(select 1 from jsonb_each(v->'unauthorise') as e(key,value)
               where jsonb_typeof(e.value)='null'),
    'no null member may reach jsonb_strip_nulls');

  -- The verdict is the withdrawal owner's own.  This root was authorised by a
  -- direct column update and carries no Weekly Source root authorisation, so
  -- the owner's own answer is NOT_MANAGED_ROOT and the control is refused.
  perform pg_temp.assert_eq(v->>'unauthorise_allowed','false',
    'an unmanaged root is not withdrawable through this owner');
  perform pg_temp.assert_eq(v#>>'{unauthorise,refusal_code}',
    'WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT',
    'the refusal code comes from the withdrawal availability owner');
  perform pg_temp.assert_eq(v#>>'{unauthorise,refusal_nature}','PERMANENT',
    'the refusal nature comes from the withdrawal availability owner');
  perform pg_temp.assert_eq(v#>>'{unauthorise,permanent}','true',
    'proof/36 section 6: a permanent refusal is reported as permanent');
  perform pg_temp.assert_eq(v#>>'{unauthorise,availability_source}',
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAW_AVAILABLE_V1',
    'the verdict came from the real availability owner');
  perform pg_temp.assert_eq(v#>>'{unauthorise,authorisation_state}','NEVER_AUTHORISED',
    'the lifecycle fact is reported beside the verdict');
  perform pg_temp.assert_eq(v#>>'{unauthorise,withdrawn}','false','not withdrawn');
end;
$action_state$;

-- The projection carries it.
do $projection$
declare
  v jsonb;
begin
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','c1000000-0000-4000-8000-000000000001',
    'timesheet_id','cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v->>'applicable','true','the presentation applies');
  perform pg_temp.assert_true(v#>'{action_state,unauthorise_allowed}' is not null,
    'action_state carries unauthorise_allowed');
  perform pg_temp.assert_true(v#>'{action_state,unauthorise,refusal_code}' is not null,
    'action_state carries the refusal kind');
  perform pg_temp.assert_true(v#>'{action_state,unauthorise,withdrawn}' is not null,
    'action_state carries the withdrawn state');
  -- The existing members are still there and unchanged in shape.
  perform pg_temp.assert_true(v#>'{action_state,authorise_allowed}' is not null,
    'the existing authorise_allowed member survives');
end;
$projection$;

-- A late-bound or failing availability owner is reported, never fatal.
savepoint before_owner_absent;
drop function public.weekly_source_first_authorisation_withdraw_available_v1(uuid);
do $owner_absent$
declare
  v jsonb;
begin
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v->>'unauthorise_allowed','false',
    'an absent availability owner never allows the control');
  perform pg_temp.assert_eq(v#>>'{unauthorise,availability_source}','OWNER_ABSENT',
    'an absent availability owner is named');
  perform pg_temp.assert_eq(v#>>'{unauthorise,refusal_code}',
    'WEEKLY_SOURCE_WITHDRAW_AVAILABILITY_UNAVAILABLE',
    'an absent availability owner yields an explicit refusal code');
  -- and the whole Office projection still answers.
  perform pg_temp.assert_eq(
    public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','c1000000-0000-4000-8000-000000000001',
      'timesheet_id','cc000000-0000-4000-8000-000000000001'))
    #>>'{action_state,unauthorise,availability_source}','OWNER_ABSENT',
    'the Office projection survives an absent availability owner');
end;
$owner_absent$;
rollback to savepoint before_owner_absent;

-- ---------------------------------------------------------------------------
-- 11a. WP-11d F3.  `available` read from the owner's JSON verdict is
--      THREE-VALUED, and Part 1 standing rule 4 requires absent, null AND
--      non-boolean to take the unsafe branch, with all four cases tested.
--      PostgreSQL accepts 'yes', 'on', '1' and 't' as boolean true, so the
--      previous bare cast granted the Unauthorise control on a string.
-- ---------------------------------------------------------------------------
do $f3_boolean_cast$
begin
  -- The positive control for the defect: PostgreSQL really does cast these.
  perform pg_temp.assert_eq(
    ('yes'::boolean)::text||','||('on'::boolean)::text||','||('1'::boolean)::text
      ||','||('t'::boolean)::text,
    'true,true,true,true',
    'PostgreSQL casts these strings to true, which is why a bare cast is unsafe');
end;
$f3_boolean_cast$;

savepoint before_f3;
create or replace function pg_temp.f3_case(p_verdict text)
returns text language plpgsql as $f3case$
declare
  v jsonb;
begin
  execute pg_catalog.format($stub$
    create or replace function public.weekly_source_first_authorisation_withdraw_available_v1(
      p_timesheet_id uuid) returns jsonb language sql stable as
    $b$ select %L::jsonb $b$;
  $stub$,p_verdict);
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  return (v->>'unauthorise_allowed')||'/'||(v#>>'{unauthorise,available}');
end;
$f3case$;

do $f3$
begin
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 absent available takes the unsafe branch');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":null,"code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 JSON-null available takes the unsafe branch');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":"yes","code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 a NON-BOOLEAN available takes the unsafe branch');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":1,"code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 a numeric available takes the unsafe branch');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":false,"code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 an explicit false is refused');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":true,"code":"NONE","refusal_nature":"NONE","retryable":true,"reason":"NONE"}'),
    'true/true','F3 a genuine boolean true still allows the control');
  -- `retryable` is the same three-valued JSON boolean.
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":true,"retryable":"yes","code":"NONE","refusal_nature":"NONE","reason":"NONE"}'),
    'true/true','F3 a non-boolean retryable does not break the verdict');
end;
$f3$;
rollback to savepoint before_f3;

-- ---------------------------------------------------------------------------
-- 11b. WP-11d F7.  An owner error keeps its cause.  The refusal code itself is
--      unchanged; the SQLSTATE and a bounded message are added so an Office user
--      and an operator are not left with "did not answer".
-- ---------------------------------------------------------------------------
savepoint before_f7;
do $f7$
declare
  v jsonb;
begin
  execute $stub$
    create or replace function public.weekly_source_first_authorisation_withdraw_available_v1(
      p_timesheet_id uuid) returns jsonb language plpgsql stable as
    $b$ begin raise exception using errcode='42501',
      message='WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED'; end; $b$;
  $stub$;
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v->>'unauthorise_allowed','false',
    'F7 a raising owner never allows the control');
  perform pg_temp.assert_eq(v#>>'{unauthorise,availability_source}','OWNER_ERROR',
    'F7 the failure is still named OWNER_ERROR');
  perform pg_temp.assert_eq(v#>>'{unauthorise,refusal_code}',
    'WEEKLY_SOURCE_WITHDRAW_AVAILABILITY_UNAVAILABLE',
    'F7 the refusal code is unchanged');
  perform pg_temp.assert_eq(v#>>'{unauthorise,owner_sqlstate}','42501',
    'F7 the dropped SQLSTATE is preserved');
  perform pg_temp.assert_eq(v#>>'{unauthorise,owner_error}',
    'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED','F7 the owner message is preserved');
  perform pg_temp.assert_true(
    length(v#>>'{unauthorise,owner_error}')<=200,'F7 the message is bounded');
end;
$f7$;
rollback to savepoint before_f7;

savepoint before_f7;
do $f7_bounded$
declare
  v jsonb;
begin
  execute $stub$
    create or replace function public.weekly_source_first_authorisation_withdraw_available_v1(
      p_timesheet_id uuid) returns jsonb language plpgsql stable as
    $b$ begin raise exception using errcode='XX000',
      message=repeat('E',5000); end; $b$;
  $stub$;
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(
    length(v#>>'{unauthorise,owner_error}')::text,'200',
    'F7 a long owner error cannot bloat the projection');
end;
$f7_bounded$;
rollback to savepoint before_f7;

-- A healthy owner carries the NONE placeholders, so the object still has every
-- member non-null and jsonb_strip_nulls at the call site cannot remove one.
do $f7_healthy$
declare
  v jsonb;
  v_nulls integer;
begin
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v#>>'{unauthorise,owner_sqlstate}','NONE',
    'a healthy owner reports no SQLSTATE');
  perform pg_temp.assert_eq(v#>>'{unauthorise,owner_error}','NONE',
    'a healthy owner reports no error');
  select count(*)::integer into v_nulls
  from jsonb_each(v->'unauthorise') as member(key,value)
  where jsonb_typeof(member.value)='null';
  perform pg_temp.assert_eq(v_nulls::text,'0',
    'every member of the verdict is non-null');
end;
$f7_healthy$;

rollback;
