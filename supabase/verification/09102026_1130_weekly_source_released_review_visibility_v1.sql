-- PostgreSQL 17 rollback verification: released checking-file review scopes.
-- Reuses the synthetic read-projection fixture identities, then proves the
-- outside-group contract, missing-timesheet and charge-warning stages.
-- No real candidate contract, source authority or financial data is changed.

\set ON_ERROR_STOP on
\pset pager off

begin isolation level repeatable read;
-- Acquire the shared change-notification counter lock before any snapshot.
-- All normal triggers still run; a concurrent Office save cannot invalidate
-- this rollback fixture's snapshot midway through its synthetic inserts.
-- Bound both acquisition and fixture duration; no runtime isolation changes.
set local lock_timeout='5s';
set local statement_timeout='45s';
lock table public.app_change_counters in share row exclusive mode;
\ir support/06102026_1818_source_verifier_snapshot_guard.sql
\ir support/06102026_1117_source_workbench_fixture_isolation.sql

\ir support/06102026_1410_source_full_row_fingerprints.sql

select pg_catalog.set_config('request.jwt.claim.role','service_role',true);

create or replace function pg_temp.assert_true(p_ok boolean,p_message text)
returns void language plpgsql as $verify$
begin
  if p_ok is not true then raise exception 'VERIFY_FAILED: %',p_message; end if;
end;
$verify$;

-- Coverage is recorded from actual owner results, never seeded expected phases.
create temporary table g9_certified_phase_coverage(ui_state text primary key);

savepoint g9_asserted_legacy_negative_fixture;

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')
) on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256,
  candidate_home_announcement_sha256=excluded.candidate_home_announcement_sha256;

insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
values ('d1000000-0000-4000-8000-000000000001','read-owner@example.invalid',
  'admin','not-a-real-password','Read owner verifier',true),
-- A SECOND Office user.  Interface I-3 section 5.4 lets the person who
-- authorises a brand-new Contract-to-Contract target root be someone other than
-- the person who took the decision, and that actor is carried in the request
-- but kept in no column and in no digest.  The Gate 9 cross-Contract fixture
-- below needs two distinct people to prove what happens when it is.
  ('d1000000-0000-4000-8000-000000000002','read-owner-two@example.invalid',
  'admin','not-a-real-password','Read owner verifier two',true);

insert into public.clients(id,name,ts_queries_email,vat_chargeable)
values
  ('d2000000-0000-4000-8000-000000000001','Workspace Trust','manager@example.invalid',true),
  ('d2000000-0000-4000-8000-000000000002','No-return Trust A',null,true),
  ('d2000000-0000-4000-8000-000000000003','No-return Trust B',null,true);
insert into public.client_settings(
  id,client_id,effective_from,hr_validation_required,autoprocess_hr,
  self_bill_no_invoices_sent,no_timesheet_required,requires_hr
) values (
  'd2100000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000001',
  '2026-01-01',true,true,true,true,true
);

insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
values
  ('d3000000-0000-4000-8000-000000000001','READ-001','Alex','Nurse','Alex Nurse','alex@example.invalid'),
  ('d3000000-0000-4000-8000-000000000002','READ-002','Robin','Nurse','Robin Nurse','robin@example.invalid');

insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr,
  overrideclientsettings,require_reference_to_pay
) values
  ('d4000000-0000-4000-8000-000000000001','d3000000-0000-4000-8000-000000000001',
   'd2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
   true,'HEALTHROSTER',true,true,true,false,false),
  ('d4000000-0000-4000-8000-000000000002','d3000000-0000-4000-8000-000000000002',
   'd2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
   true,'HEALTHROSTER',true,true,true,false,false);

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time,
  nhsp_report_heading_name
) values
  ('d5000000-0000-4000-8000-000000000001','TEST','d0000000-0000-4000-8000-000000000001',
   'READ_VERIFY_ROSTER','Read verification Roster','ROSTER',3,'15:00',null),
  ('d5000000-0000-4000-8000-000000000002','TEST','d0000000-0000-4000-8000-000000000001',
   'NO_RETURN_VERIFY','No-return verification','NHSP',3,'15:00','No-return verification');
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values
  ('d5000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000001','2026-01-01','d1000000-0000-4000-8000-000000000001'),
  ('d5000000-0000-4000-8000-000000000002','d2000000-0000-4000-8000-000000000002','2026-01-01','d1000000-0000-4000-8000-000000000001'),
  ('d5000000-0000-4000-8000-000000000002','d2000000-0000-4000-8000-000000000003','2026-01-01','d1000000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,
  candidate_queries_enabled,manager_queries_enabled,manager_query_recipient,created_by_user_id
) values (
  'd5000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000001','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,'Manager@Example.Invalid',
  'd1000000-0000-4000-8000-000000000001'
);

insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values
  ('d6000000-0000-4000-8000-000000000001','d5000000-0000-4000-8000-000000000001',
   '2026-09-06','2026-09-09 14:00:00+00','OPEN',1,'NONE'),
  ('d6000000-0000-4000-8000-000000000002','d5000000-0000-4000-8000-000000000002',
   '2026-09-06','2026-09-09 14:00:00+00','OPEN',1,'NONE'),
  ('d6000000-0000-4000-8000-000000000003','d5000000-0000-4000-8000-000000000001',
   '2026-08-30','2026-09-02 14:00:00+00','OPEN',1,'NONE');
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
  coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'd7000000-0000-4000-8000-000000000001','d6000000-0000-4000-8000-000000000001',
  'workspace.xlsx',decode(repeat('11',32),'hex'),100,
  '34444444-4444-4444-8444-444444444444','verify','verify',decode(repeat('12',32),'hex'),
  decode(repeat('13',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,
  decode(repeat('14',32),'hex'),'CURRENT','d1000000-0000-4000-8000-000000000001'
),(
  'd7000000-0000-4000-8000-000000000002','d6000000-0000-4000-8000-000000000003',
  'prior-workspace.xlsx',decode(repeat('61',32),'hex'),100,
  '34444444-4444-4444-8444-444444444444','verify','verify',decode(repeat('62',32),'hex'),
  decode(repeat('63',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,
  decode(repeat('64',32),'hex'),'SEALED','d1000000-0000-4000-8000-000000000001'
);
update public.weekly_source_cycles
set current_complete_upload_id='d7000000-0000-4000-8000-000000000001'
where id='d6000000-0000-4000-8000-000000000001';
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  'd8000000-0000-4000-8000-000000000001','d6000000-0000-4000-8000-000000000001','CYCLE',
  'd7000000-0000-4000-8000-000000000001',1,decode(repeat('15',32),'hex'),
  decode(repeat('16',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp()
);
update public.weekly_source_cycles
set projection_state='CURRENT',current_projection_publication_id='d8000000-0000-4000-8000-000000000001'
where id='d6000000-0000-4000-8000-000000000001';

insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
) values
  ('d9000000-0000-4000-8000-000000000001','d3000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000001',
   '2026-09-01','PROFILE_EXTERNAL_KEY','read-a-1',decode(repeat('21',32),'hex'),
   'd5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
  ('d9000000-0000-4000-8000-000000000002','d3000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000001',
   '2026-09-02','PROFILE_EXTERNAL_KEY','read-a-2',decode(repeat('22',32),'hex'),
   'd5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
  ('d9000000-0000-4000-8000-000000000003','d3000000-0000-4000-8000-000000000002','d2000000-0000-4000-8000-000000000001',
   '2026-09-03','PROFILE_EXTERNAL_KEY','read-r-1',decode(repeat('23',32),'hex'),
   'd5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444');

insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json
) values
  ('da000000-0000-4000-8000-000000000001','READ-A','alex-nurse','workspace-trust','ward-a','nurse',
   '2026-09-01 08:00:00+00','2026-09-02 18:00:00+00',30,1020,'2026-09-06',
   'verify/alex.png',repeat('a',64),'d4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
   '[{"date":"2026-09-01","start":"09:00","end":"18:00","break_minutes":30},{"date":"2026-09-02","start":"09:00","end":"18:00","break_minutes":30}]'),
  ('da000000-0000-4000-8000-000000000002','READ-R','robin-nurse','workspace-trust','ward-a','nurse',
   '2026-09-03 08:00:00+00','2026-09-03 18:00:00+00',30,570,'2026-09-06',
   'verify/robin.png',repeat('b',64),'d4000000-0000-4000-8000-000000000002','WEEKLY','HOURS',
   '[{"date":"2026-09-03","start":"09:00","end":"18:00","break_minutes":30}]');

insert into public.candidate_app_accounts(
  id,environment,email_normalized,status,notification_preferences_json
) values (
  'db000000-0000-4000-8000-000000000001','TEST','alex-app@example.invalid','ACTIVE',
  '{"push":true,"timesheet_expense_attention":true}'
);
insert into public.candidate_app_global_membership_links(
  membership_id,global_account_identity_hmac,account_id,candidate_id,membership_generation,state
) values (
  'db100000-0000-4000-8000-000000000001',decode(repeat('31',32),'hex'),
  'db000000-0000-4000-8000-000000000001','d3000000-0000-4000-8000-000000000001',1,'ACTIVE'
);

-- A real NHSP final-source row used to prove that the Office Finalise payload
-- contains the exact source hours and source pence the NHSP table displays.
-- It joins the existing NHSP group only after the no-return cycle above, so it
-- cannot change that cycle's attestation population.
insert into public.clients(id,name,ts_queries_email,vat_chargeable)
values ('d2000000-0000-4000-8000-000000000004','NHSP display Trust',null,true);
insert into public.client_settings(
  id,client_id,effective_from,hr_validation_required,autoprocess_hr,
  self_bill_no_invoices_sent,no_timesheet_required,requires_hr
) values (
  'd2100000-0000-4000-8000-000000000004','d2000000-0000-4000-8000-000000000004',
  '2026-01-01',false,false,true,false,false
);
insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
values ('d3000000-0000-4000-8000-000000000003','READ-003','Taylor','Nurse','Taylor Nurse','taylor@example.invalid');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr,
  overrideclientsettings,require_reference_to_pay
) values (
  'd4000000-0000-4000-8000-000000000003','d3000000-0000-4000-8000-000000000003',
  'd2000000-0000-4000-8000-000000000004','2026-01-01','2026-12-31','PAYE','{}',
  true,'NHSP',false,false,false,true,false
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,
  cutoff_local_time,nhsp_report_heading_name
) values (
  'd5000000-0000-4000-8000-000000000003','TEST','d0000000-0000-4000-8000-000000000002',
  'READ_VERIFY_NHSP','Read verification NHSP','NHSP',3,'15:00','NHSP display Trust'
);
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values (
  'd5000000-0000-4000-8000-000000000003','d2000000-0000-4000-8000-000000000004',
  '2026-09-07','d1000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,
  candidate_queries_enabled,manager_queries_enabled,created_by_user_id
) values (
  'd5000000-0000-4000-8000-000000000003','d2000000-0000-4000-8000-000000000004',
  '2026-09-07','SOURCE_AUTHORITY','CHECK_ONLY',true,false,false,
  'd1000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'd6000000-0000-4000-8000-000000000004','d5000000-0000-4000-8000-000000000003',
  '2026-09-13','2026-09-16 14:00:00+00','OPEN',1,'NONE'
);
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc,
  version,state,projection_state
) values (
  'dc000000-0000-4000-8000-000000000001','d6000000-0000-4000-8000-000000000004',
  'TEST','d0000000-0000-4000-8000-000000000002','d5000000-0000-4000-8000-000000000003',
  'd2000000-0000-4000-8000-000000000004','2026-09-16 14:00:00+00',1,'OPEN','NONE'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,header_coordinate_map_hash,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
  row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
) values (
  'd7000000-0000-4000-8000-000000000003','d6000000-0000-4000-8000-000000000004',
  'dc000000-0000-4000-8000-000000000001','nhsp-display.xlsx',decode(repeat('91',32),'hex'),100,
  '32222222-2222-4222-8222-222222222222','verify','verify',decode(repeat('92',32),'hex'),
  decode(repeat('93',32),'hex'),'NHSP_TRUST_REPORT_SCOPE',1,1,decode(repeat('94',32),'hex'),
  'CURRENT','d1000000-0000-4000-8000-000000000001','{"nhsp_report_number":"1741227"}'::jsonb
);
insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,
  durable_identity_hash,first_source_group_id,source_format_profile_id
) values (
  'd9000000-0000-4000-8000-000000000004','d3000000-0000-4000-8000-000000000003',
  'd2000000-0000-4000-8000-000000000004','2026-09-08','SCHEDULE_TUPLE',
  decode(repeat('95',32),'hex'),'d5000000-0000-4000-8000-000000000003',
  '32222222-2222-4222-8222-222222222222'
);
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,source_commission_pence,source_total_cost_pence,
  source_shift_charge_pence,source_money_parse_state,normalised_row_hash,bounded_raw_columns_json
) values (
  'dd000000-0000-4000-8000-000000000001','d7000000-0000-4000-8000-000000000003',1,
  'nhsp-display-shift','Taylor Nurse','NHSP display Trust','2026-09-08',
  '2026-09-08 09:00','2026-09-08 17:00',30,450,'SOURCE_WORKED',1000,9000,10000,
  'VALID',decode(repeat('96',32),'hex'),'{}'
);
insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,
  paid_minutes,rate_classifications_json,mapping_state,contract_selection_method,
  work_event_match_kind,work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_count,qualifying_contract_set_hash,source_row_fingerprint,
  contract_and_rate_fingerprint,effective_policy_fingerprint
) values (
  'de000000-0000-4000-8000-000000000001','dd000000-0000-4000-8000-000000000001',1,
  'd3000000-0000-4000-8000-000000000003','d2000000-0000-4000-8000-000000000004',
  'd4000000-0000-4000-8000-000000000003','d9000000-0000-4000-8000-000000000004',
  450,'{}','RESOLVED','AUTO_UNIQUE','NEW_SCHEDULE_TUPLE',decode(repeat('97',32),'hex'),
  decode(repeat('98',32),'hex'),1,decode(repeat('99',32),'hex'),decode(repeat('9a',32),'hex'),
  decode(repeat('9b',32),'hex'),decode(repeat('9c',32),'hex')
);
-- Before finalisation there is deliberately no immutable source-row lineage.
-- The candidate's signed current Contract-week Timesheet must nevertheless
-- suppress a false CANDIDATE_TIMESHEET_MISSING issue in the Office query view.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json
) values (
  'da000000-0000-4000-8000-000000000003','READ-N','taylor-nurse','nhsp-display-trust',
  'ward-n','nurse','2026-09-08 09:00:00+00','2026-09-08 17:00:00+00',30,450,
  '2026-09-13','verify/taylor.png',repeat('c',64),
  'd4000000-0000-4000-8000-000000000003','WEEKLY','HOURS',
  '[{"date":"2026-09-08","start":"09:00","end":"17:00","break_minutes":30}]'
);
insert into public.contract_weeks(
  id,contract_id,week_ending_date,additional_seq,status,timesheet_id
) values (
  'ca000000-0000-4000-8000-000000000003','d4000000-0000-4000-8000-000000000003',
  '2026-09-13',0,'SUBMITTED','da000000-0000-4000-8000-000000000003'
);
insert into public.weekly_source_charge_checks(
  id,upload_row_id,row_resolution_id,generation,row_sign_kind,
  source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
  calculated_segment_charge_pence,source_charge_difference_pence,
  comparison_profile_version,comparison_result,comparison_reason_code,phase_severity,
  charge_calculation_fingerprint
) values (
  'df000000-0000-4000-8000-000000000001','dd000000-0000-4000-8000-000000000001',
  'de000000-0000-4000-8000-000000000001',1,'POSITIVE',1000,9000,10000,10000,0,
  'NHSP_TWO_COMPONENT_PENCE_V1','EXACT','EXACT','NONE',decode(repeat('9d',32),'hex')
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,authority_scope_version,
  projection_generation,comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  'd8000000-0000-4000-8000-000000000003','d6000000-0000-4000-8000-000000000004',
  'NHSP_REPORT_SCOPE','dc000000-0000-4000-8000-000000000001',
  'd7000000-0000-4000-8000-000000000003',1,1,decode(repeat('9e',32),'hex'),
  decode(repeat('9f',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp()
);
update public.weekly_source_report_scopes
set current_complete_upload_id='d7000000-0000-4000-8000-000000000003',
    current_projection_publication_id='d8000000-0000-4000-8000-000000000003',
    projection_state='CURRENT'
where id='dc000000-0000-4000-8000-000000000001';


-- Repurpose only the synthetic NHSP fixture as a multi-client checking upload.
-- Matching a row client does not itself configure its source policy.
update public.client_settings set is_nhsp=true
where id='d2100000-0000-4000-8000-000000000004';
update public.weekly_source_uploads set report_scope_id=null,
  source_format_profile_id=(select id from public.weekly_source_format_profiles where profile_code='NHSP_PREFINAL_RELEASED_V1')
where id='d7000000-0000-4000-8000-000000000003';
update public.weekly_source_projection_publications
set authority_scope_kind='CYCLE',report_scope_id=null
where id='d8000000-0000-4000-8000-000000000003';
update public.weekly_source_cycles
set current_complete_upload_id='d7000000-0000-4000-8000-000000000003',
    current_projection_publication_id='d8000000-0000-4000-8000-000000000003',projection_state='CURRENT'
where id='d6000000-0000-4000-8000-000000000004';
delete from public.weekly_source_group_clients
where source_group_id='d5000000-0000-4000-8000-000000000003'
  and client_id='d2000000-0000-4000-8000-000000000004';

do $released_review$
declare
  request jsonb:=jsonb_build_object('actor_user_id','d1000000-0000-4000-8000-000000000001',
    'source_group_id','d5000000-0000-4000-8000-000000000003',
    'client_id','d2000000-0000-4000-8000-000000000004','tab','queries','section','checks');
  value jsonb; single jsonb; final_before jsonb; scopes_before jsonb;
begin
  final_before:=public.weekly_source_combined_finalise_workspace_v1(request-'tab'-'section');
  scopes_before:=public.weekly_source_workspace_scopes_v1(request-'tab'-'section');
  insert into public.weekly_source_row_resolutions
  select (jsonb_populate_record(null::public.weekly_source_row_resolutions,to_jsonb(r)||jsonb_build_object(
    'id','de000000-0000-4000-8000-000000000002','generation',2,
    'mapping_state','NO_ELIGIBLE_CONTRACT','blocker_code','NO_ELIGIBLE_CONTRACT',
    'contract_id',null,'qualifying_contract_count',0,'contract_selection_method',null,
    'work_event_id',null,'work_event_match_kind',null,'work_event_match_fingerprint',null))).*
  from public.weekly_source_row_resolutions r where id='de000000-0000-4000-8000-000000000001';
  value:=public.weekly_source_combined_review_workspace_v1(request);
  perform pg_temp.assert_true(value#>>'{rows,0,actions,0,label}'='Choose contract',
    'released outside-group missing contract disappeared from Office checks');
  perform pg_temp.assert_true(value->>'total_count'='1',
    'released outside-group contract check duplicated');
  perform pg_temp.assert_true((value#>>'{scope_options,0,review_only}')::boolean,
    'released review scope was represented as a report obligation');
  single:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('section','questions'));
  perform pg_temp.assert_true(single->>'total_count'='0',
    'unresolved contract incorrectly created an Hours question');

  perform pg_temp.assert_true(public.weekly_source_workspace_scopes_v1(request-'tab'-'section')=scopes_before,
    'review reads changed final-report obligations');
  perform pg_temp.assert_true(public.weekly_source_combined_finalise_workspace_v1(request-'tab'-'section')=final_before,
    'released review scope changed finalisation availability');
  update public.client_settings set is_nhsp=false
  where id='d2100000-0000-4000-8000-000000000004';
  value:=public.weekly_source_combined_review_workspace_v1(request);
  perform pg_temp.assert_true(value->>'total_count'='0',
    'non-NHSP outside-group client acquired released review authority');
  update public.client_settings set is_nhsp=true
  where id='d2100000-0000-4000-8000-000000000004';

  -- Once Office separately configures a valid source policy and contract,
  -- the existing hours and charge owners must surface the next checks. The
  -- review function itself must NOT add membership or manufacture a policy.
  insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
  values('d5000000-0000-4000-8000-000000000003','d2000000-0000-4000-8000-000000000004',
    '2026-09-07','d1000000-0000-4000-8000-000000000001');
  insert into public.weekly_source_row_resolutions
  select (jsonb_populate_record(null::public.weekly_source_row_resolutions,to_jsonb(r)||jsonb_build_object(
    'id','de000000-0000-4000-8000-000000000003','generation',3))).*
  from public.weekly_source_row_resolutions r where id='de000000-0000-4000-8000-000000000001';
  update public.timesheets set r2_nurse_key=null,img_sha256_nurse=null
  where timesheet_id='da000000-0000-4000-8000-000000000003';
  value:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('section','questions'));
  perform pg_temp.assert_true(value->>'total_count'='1'
    and value#>>'{rows,0,children,0,issue}'='Timesheet missing',
    'eligible resolved contract did not expose missing candidate hours');

  insert into public.weekly_source_charge_checks(
    id,upload_row_id,row_resolution_id,generation,row_sign_kind,
    source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
    calculated_segment_charge_pence,source_charge_difference_pence,
    comparison_profile_version,comparison_result,comparison_reason_code,phase_severity,
    charge_calculation_fingerprint
  ) values ('df000000-0000-4000-8000-000000000002','dd000000-0000-4000-8000-000000000001',
    'de000000-0000-4000-8000-000000000003',3,'POSITIVE',1000,9000,10000,12000,-2000,
    'NHSP_TWO_COMPONENT_PENCE_V1','MISMATCH','MISMATCH','PROVISIONAL_WARNING',decode(repeat('9d',32),'hex'));
  value:=public.weekly_source_combined_review_workspace_v1(request);
  perform pg_temp.assert_true(value->>'total_count'='1'
    and value#>>'{rows,0,actions,0,label}'='Open charge details',
    'resolved charge mismatch disappeared from Office checks');
  single:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('section','questions'));
  perform pg_temp.assert_true(single->>'total_count'='1',
    'charge warning hid the independent missing-timesheet question');

  update public.timesheets set r2_nurse_key='verify/taylor.png',img_sha256_nurse=repeat('c',64)
  where timesheet_id='da000000-0000-4000-8000-000000000003';
  value:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('section','questions'));
  perform pg_temp.assert_true(value->>'total_count'='0',
    'signed candidate hours created a false missing-timesheet question');
  value:=public.weekly_source_combined_review_workspace_v1(request);
  perform pg_temp.assert_true(value->>'total_count'='1',
    'signed matching hours incorrectly cleared a charge warning');

  perform pg_temp.assert_true(not has_function_privilege('authenticated',
    'public.weekly_source_combined_review_workspace_v1(jsonb)','EXECUTE'),
    'released review is exposed as a browser RPC');
end $released_review$;
rollback;
