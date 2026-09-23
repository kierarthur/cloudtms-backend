-- PostgreSQL 17 rollback verification: weekly_source_read_projections_v1
-- The fixture exercises paged Office reads, complete server-owned selections,
-- mixed Candidate availability, stale rollback, exact group acceptance and
-- no-shifts attestation, and the Gate 9 server-owned lifecycle phase: all 22
-- rows of the pack annex ui-lifecycle-state-matrix.csv, the proposal view and
-- UI-022, each from a database state that genuinely reaches it, plus the
-- fail-closed cases. Every data change is rolled back.

\set ON_ERROR_STOP on
\pset pager off

begin;

select pg_catalog.set_config('request.jwt.claim.role','service_role',true);

create or replace function pg_temp.assert_true(p_ok boolean,p_message text)
returns void language plpgsql as $verify$
begin
  if p_ok is not true then raise exception 'VERIFY_FAILED: %',p_message; end if;
end;
$verify$;

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

do $test$
declare
  v_sync jsonb;
  v_workspace jsonb;
  v_workspace_after_send jsonb;
  v_accept_workspace_all jsonb;
  v_workspace_after_ask jsonb;
  v_history jsonb;
  v_history_next jsonb;
  v_send jsonb;
  v_ask jsonb;
  v_accept jsonb;
  v_presentation jsonb;
  v_accept_request jsonb;
  v_accept_request_second jsonb;
  v_accept_request_multi jsonb;
  v_no_shifts jsonb;
  v_no_shifts_workspace jsonb;
  v_nhsp_workspace jsonb;
  v_original_ask_request jsonb;
  v_stale_rejected boolean:=false;
  v_invalid_rejected boolean:=false;
  v_no_shifts_stale boolean:=false;
  v_accept_stale boolean:=false;
  v_accept_extra_rejected boolean:=false;
  v_accept_empty_group_rejected boolean:=false;
begin
  v_sync:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000001',
    'issues',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'work_event_id','d9000000-0000-4000-8000-000000000001',
        'candidate_timesheet_id','da000000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('41',32),
        'contract_id','d4000000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-01 09:00','candidate_end_at_local','2026-09-01 18:00',
        'candidate_break_minutes',30,'system_start_at_local','2026-09-01 09:00',
        'system_end_at_local','2026-09-01 17:00','system_break_minutes',30
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','d9000000-0000-4000-8000-000000000002',
        'candidate_timesheet_id','da000000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('42',32),
        'contract_id','d4000000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_MISSING_OR_NOT_AUTHORISED','source_presence','ABSENT',
        'candidate_start_at_local','2026-09-02 09:00','candidate_end_at_local','2026-09-02 18:00',
        'candidate_break_minutes',30
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','d9000000-0000-4000-8000-000000000003',
        'candidate_timesheet_id','da000000-0000-4000-8000-000000000002',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('43',32),
        'contract_id','d4000000-0000-4000-8000-000000000002',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-03 09:00','candidate_end_at_local','2026-09-03 18:00',
        'candidate_break_minutes',30,'system_start_at_local','2026-09-03 09:00',
        'system_end_at_local','2026-09-03 17:00','system_break_minutes',30
      )
    )
  ));
  perform pg_temp.assert_true((v_sync->>'new_incidents')::integer=3,
    'three current incidents were not created');

  insert into public.weekly_timesheet_source_comparisons(
    id,source_cycle_id,upload_id,projection_publication_id,upload_row_id,
    timesheet_id,timesheet_revision,work_event_id,contract_id,work_date,
    comparison_state,candidate_start_at_local,candidate_end_at_local,
    candidate_break_minutes,source_start_at_local,source_end_at_local,
    source_break_minutes,total_break_minutes_match,source_reference_number,
    comparison_fingerprint
  ) values
    ('db200000-0000-4000-8000-000000000001','d6000000-0000-4000-8000-000000000001',
     'd7000000-0000-4000-8000-000000000001','d8000000-0000-4000-8000-000000000001',null,
     'da000000-0000-4000-8000-000000000001',1,'d9000000-0000-4000-8000-000000000001',
     'd4000000-0000-4000-8000-000000000001','2026-09-01','HOURS_MISMATCH',
     '2026-09-01 09:00','2026-09-01 18:00',30,'2026-09-01 09:00','2026-09-01 17:00',30,
     true,null,decode(repeat('51',32),'hex')),
    ('db200000-0000-4000-8000-000000000002','d6000000-0000-4000-8000-000000000001',
     'd7000000-0000-4000-8000-000000000001','d8000000-0000-4000-8000-000000000001',null,
     'da000000-0000-4000-8000-000000000001',1,'d9000000-0000-4000-8000-000000000002',
     'd4000000-0000-4000-8000-000000000001','2026-09-02','SOURCE_SHIFT_MISSING',
     '2026-09-02 09:00','2026-09-02 18:00',30,null,null,null,false,null,
     decode(repeat('52',32),'hex')),
    ('db200000-0000-4000-8000-000000000003','d6000000-0000-4000-8000-000000000001',
     'd7000000-0000-4000-8000-000000000001','d8000000-0000-4000-8000-000000000001',null,
     'da000000-0000-4000-8000-000000000002',1,'d9000000-0000-4000-8000-000000000003',
     'd4000000-0000-4000-8000-000000000002','2026-09-03','HOURS_MISMATCH',
     '2026-09-03 09:00','2026-09-03 18:00',30,'2026-09-03 09:00','2026-09-03 17:00',30,
     true,null,decode(repeat('53',32),'hex'));

  v_workspace:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','queries',
    'source_group_id','d5000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000001',
    'limit',1,'sort_key','candidate','sort_direction','asc'
  ));
  perform pg_temp.assert_true(v_workspace->>'contract'='WEEKLY_SOURCE_IMPORT_WORKSPACE_V1',
    'workspace contract is absent');
  perform pg_temp.assert_true((v_workspace#>>'{queries,total_count}')::integer=2
    and pg_catalog.jsonb_array_length(v_workspace#>'{queries,rows}')=1
    and (v_workspace#>>'{queries,has_more}')::boolean,
    'paged query response does not prove unloaded groups');
  perform pg_temp.assert_true((v_workspace#>>'{queries,bulk_actions,filtered_group_count}')::integer=2
    and (v_workspace#>>'{queries,bulk_actions,selection_complete}')::boolean,
    'complete server filter descriptor is absent');
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(v_workspace#>'{queries,rows,0,accept_system_hours_action,payload,selection,incident_ids}')=2
    and pg_catalog.jsonb_array_length(v_workspace#>'{queries,rows,0,accept_system_hours_action,payload,selection,group_selection_proofs}')=1
    and (v_workspace#>>'{queries,rows,0,accept_system_hours_action,enabled}')::boolean,
    'expanded group did not return every eligible shift');

  v_nhsp_workspace:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','finalise',
    'source_group_id','d5000000-0000-4000-8000-000000000003',
    'source_cycle_id','d6000000-0000-4000-8000-000000000004',
    'client_id','d2000000-0000-4000-8000-000000000004',
    'report_scope_id','dc000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000003'
  ));
  perform pg_temp.assert_true(
    v_nhsp_workspace#>>'{finalise,ready,rows,0,actual_hours}'='09:00-17:00 (30 min break)'
    and v_nhsp_workspace#>>'{finalise,ready,rows,0,movement}'='Positive'
    and v_nhsp_workspace#>>'{finalise,ready,rows,0,commission}'='£10.00'
    and v_nhsp_workspace#>>'{finalise,ready,rows,0,total_cost}'='£90.00'
    and v_nhsp_workspace#>>'{finalise,ready,rows,0,invoice_charge}'='£100.00',
    'NHSP Finalise row did not expose exact source hours and authoritative source pence');
  v_nhsp_workspace:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','finalise',
    'source_group_id','d5000000-0000-4000-8000-000000000003',
    'source_cycle_id','d6000000-0000-4000-8000-000000000004',
    'client_id','d2000000-0000-4000-8000-000000000004',
    'report_scope_id','dc000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000003',
    'sort_key','actual_hours','sort_direction','desc'
  ));
  perform pg_temp.assert_true(
    v_nhsp_workspace#>>'{finalise,ready,rows,0,actual_hours}'='09:00-17:00 (30 min break)',
    'NHSP Finalise did not accept and apply the approved Actual hours sort');

  v_nhsp_workspace:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','queries',
    'source_group_id','d5000000-0000-4000-8000-000000000003',
    'source_cycle_id','d6000000-0000-4000-8000-000000000004',
    'client_id','d2000000-0000-4000-8000-000000000004',
    'report_scope_id','dc000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000003'
  ));
  perform pg_temp.assert_true(
    (v_nhsp_workspace#>>'{queries,total_count}')::integer=0,
    'signed current Contract-week Timesheet was falsely reported missing before lineage creation');

  v_history:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','history',
    'source_group_id','d5000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000001',
    'cycle_filter','LAST_4_PAY_CYCLES','limit',1,'sort_key','when','sort_direction','desc'
  ));
  perform pg_temp.assert_true(v_history#>>'{history,cycle_filter}'='LAST_4_PAY_CYCLES'
    and pg_catalog.jsonb_array_length(v_history#>'{history,cycle_options}')=3
    and (v_history#>>'{history,total_count}')::integer=2
    and (v_history#>>'{history,has_more}')::boolean,
    'bounded History period filter or pagination is incomplete');
  v_history_next:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','history',
    'source_group_id','d5000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000001',
    'cycle_filter','LAST_4_PAY_CYCLES','cursor',v_history#>>'{history,next_cursor}',
    'limit',1,'sort_key','when','sort_direction','desc'
  ));
  perform pg_temp.assert_true(pg_catalog.jsonb_array_length(v_history_next#>'{history,rows}')=1
    and not (v_history_next#>>'{history,has_more}')::boolean,
    'History continuation did not return the unloaded event');

  v_presentation:=public.weekly_source_office_timesheet_presentation_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','d1000000-0000-4000-8000-000000000001',
      'timesheet_id','da000000-0000-4000-8000-000000000001'
    )
  );
  perform pg_temp.assert_true((v_presentation->>'applicable')::boolean
    and v_presentation->>'authority'='CLIENT_SYSTEM'
    and v_presentation#>>'{comparison,state}'='MISMATCH'
    and not (v_presentation#>>'{action_state,authorise_allowed}')::boolean,
    'shared Simple/Bulk presentation did not block an unprotected mismatch');
  perform pg_temp.assert_true(not (v_presentation::text ~* '(hourly.?rate|pay.?rate|charge.?rate|remittance)'),
    'presentation leaked a rate or remittance field');

  v_original_ask_request:=v_workspace#>'{queries,bulk_actions,ask_candidates,request}';
  v_ask:=public.weekly_source_office_bulk_query_action_atomic_v1(v_original_ask_request);
  perform pg_temp.assert_true((v_ask->>'selected_count')::integer=2
    and (v_ask->>'included_count')::integer=1
    and (v_ask->>'excluded_count')::integer=1
    and v_ask->>'status'='PARTLY_COMPLETE',
    'mixed Candidate availability did not preserve the eligible subset');
  perform pg_temp.assert_true(
    exists(select 1 from public.weekly_candidate_outreach_generations
      where candidate_id='d3000000-0000-4000-8000-000000000001')
    and not exists(select 1 from public.weekly_candidate_outreach_generations
      where candidate_id='d3000000-0000-4000-8000-000000000002')
    and exists(select 1 from pg_catalog.jsonb_array_elements(v_ask->'excluded') item
      where item->>'status'='UNAVAILABLE_NO_ACTIVE_APP_ACCOUNT'),
    'Candidate account preflight did not isolate the unavailable Candidate');

  begin
    perform public.weekly_source_office_bulk_query_action_atomic_v1(v_original_ask_request);
  exception when sqlstate '40001' then
    v_stale_rejected:=true;
  end;
  perform pg_temp.assert_true(v_stale_rejected,
    'stale workspace replay did not fail before further action');
  perform pg_temp.assert_true((select count(*) from public.weekly_candidate_outreach_generations)=1,
    'stale replay produced a partial Candidate action');

  v_workspace_after_ask:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','queries',
    'source_group_id','d5000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000001',
    'limit',1,'sort_key','candidate','sort_direction','asc'
  ));
  perform pg_temp.assert_true(v_workspace_after_ask#>>'{queries,rows,0,actions,1,label}'='Remind candidate'
    and not (v_workspace_after_ask#>>'{queries,rows,0,actions,1,enabled}')::boolean
    and nullif(v_workspace_after_ask#>>'{queries,rows,0,actions,1,reason}','') is not null,
    'Candidate reminder cooldown was not projected as a disabled plain action');
  update public.weekly_candidate_outreach_generations
  set manual_reminder_available_at_utc=pg_catalog.transaction_timestamp()-interval '1 second'
  where candidate_id='d3000000-0000-4000-8000-000000000001' and state='ACTIVE';
  v_workspace_after_ask:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','queries',
    'source_group_id','d5000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000001',
    'limit',1,'sort_key','candidate','sort_direction','asc'
  ));
  perform pg_temp.assert_true((v_workspace_after_ask#>>'{queries,rows,0,actions,1,enabled}')::boolean
    and (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(
      v_workspace_after_ask#>'{queries,rows,0,actions,1,payload}'))=2
    and nullif(v_workspace_after_ask#>>'{queries,rows,0,actions,1,payload,candidate_generation_id}','') is not null,
    'available Candidate reminder did not expose its exact two-key command payload');
  v_send:=public.weekly_source_office_bulk_query_action_atomic_v1(
    v_workspace_after_ask#>'{queries,bulk_actions,send_manager_now,request}'
  );
  perform pg_temp.assert_true((v_send->>'selected_count')::integer=2
    and (v_send->>'included_count')::integer=2
    and (select count(*) from public.weekly_manager_recipient_generations where state='ACTIVE')=1,
    'manager action did not expand the full unloaded filter into one recipient route');

  v_workspace_after_send:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','queries',
    'source_group_id','d5000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000001',
    'limit',1,'sort_key','candidate','sort_direction','asc'
  ));
  v_accept_workspace_all:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','queries',
    'source_group_id','d5000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000001',
    'projection_publication_id','d8000000-0000-4000-8000-000000000001',
    'limit',10,'sort_key','candidate','sort_direction','asc'
  ));
  select row_item.value#>'{accept_system_hours_action,payload}' into strict v_accept_request
  from pg_catalog.jsonb_array_elements(v_accept_workspace_all#>'{queries,rows}') row_item(value)
  where row_item.value->>'candidate'='Alex Nurse';
  select row_item.value#>'{accept_system_hours_action,payload}' into strict v_accept_request_second
  from pg_catalog.jsonb_array_elements(v_accept_workspace_all#>'{queries,rows}') row_item(value)
  where row_item.value->>'candidate'='Robin Nurse';
  begin
    perform public.weekly_source_office_bulk_query_action_atomic_v1(
      pg_catalog.jsonb_set(v_accept_request,'{selection,group_selection_proofs,0,selection_proof}',
        to_jsonb(repeat('0',64)),false)
    );
  exception when sqlstate '40001' then
    v_accept_stale:=true;
  end;
  perform pg_temp.assert_true(v_accept_stale
    and (select count(*) from public.weekly_discrepancy_incidents
      where candidate_id='d3000000-0000-4000-8000-000000000001' and state='OPEN')=2,
    'tampered complete-group proof did not roll back before acceptance');

  begin
    v_accept:=public.weekly_source_office_bulk_query_action_atomic_v1(
      pg_catalog.jsonb_set(
        v_accept_request,'{selection,incident_ids}',
        pg_catalog.jsonb_build_array(v_accept_request#>'{selection,incident_ids,0}'),false
      )
    );
    perform pg_temp.assert_true((v_accept->>'included_issue_count')::integer=1
      and (select count(*) from public.weekly_discrepancy_incidents
        where candidate_id='d3000000-0000-4000-8000-000000000001' and state='OPEN')=1,
      'one selected shift did not resolve independently');
    raise exception 'VERIFY_ACCEPT_INDIVIDUAL_ROLLBACK';
  exception when raise_exception then
    if sqlerrm<>'VERIFY_ACCEPT_INDIVIDUAL_ROLLBACK' then raise; end if;
  end;
  perform pg_temp.assert_true((select count(*) from public.weekly_discrepancy_incidents where state='OPEN')=3,
    'individual acceptance proof did not remain isolated for later checks');

  begin
    perform public.weekly_source_office_bulk_query_action_atomic_v1(
      pg_catalog.jsonb_set(
        v_accept_request,'{selection,incident_ids}',
        (v_accept_request#>'{selection,incident_ids}')
          ||pg_catalog.jsonb_build_array('ffffffff-ffff-4fff-8fff-ffffffffffff'),false
      )
    );
  exception when sqlstate '40001' then
    v_accept_extra_rejected:=true;
  end;
  perform pg_temp.assert_true(v_accept_extra_rejected
    and (select count(*) from public.weekly_discrepancy_incidents where state='OPEN')=3,
    'an incident outside the proved group was not rejected atomically');

  v_accept_request_multi:=pg_catalog.jsonb_set(
    pg_catalog.jsonb_set(
      pg_catalog.jsonb_set(
        v_accept_request,'{selection,group_keys}',
        (v_accept_request#>'{selection,group_keys}')
          ||(v_accept_request_second#>'{selection,group_keys}'),false
      ),'{selection,incident_ids}',
      (v_accept_request#>'{selection,incident_ids}')
        ||(v_accept_request_second#>'{selection,incident_ids}'),false
    ),'{selection,group_selection_proofs}',
    (v_accept_request#>'{selection,group_selection_proofs}')
      ||(v_accept_request_second#>'{selection,group_selection_proofs}'),false
  );
  begin
    perform public.weekly_source_office_bulk_query_action_atomic_v1(
      pg_catalog.jsonb_set(
        v_accept_request_multi,'{selection,incident_ids}',
        v_accept_request#>'{selection,incident_ids}',false
      )
    );
  exception when sqlstate '40001' then
    v_accept_empty_group_rejected:=true;
  end;
  perform pg_temp.assert_true(v_accept_empty_group_rejected
    and (select count(*) from public.weekly_discrepancy_incidents where state='OPEN')=3,
    'a selected group without a selected shift was not rejected atomically');

  begin
    v_accept:=public.weekly_source_office_bulk_query_action_atomic_v1(v_accept_request_multi);
    perform pg_temp.assert_true((v_accept->>'included_issue_count')::integer=3
      and (select count(*) from public.weekly_discrepancy_incidents where state='OPEN')=0,
      'the exact multi-group union did not resolve atomically');
    raise exception 'VERIFY_ACCEPT_MULTI_ROLLBACK';
  exception when raise_exception then
    if sqlerrm<>'VERIFY_ACCEPT_MULTI_ROLLBACK' then raise; end if;
  end;
  perform pg_temp.assert_true((select count(*) from public.weekly_discrepancy_incidents where state='OPEN')=3,
    'multi-group acceptance proof did not remain isolated for the final group check');

  v_accept:=public.weekly_source_office_bulk_query_action_atomic_v1(v_accept_request);
  perform pg_temp.assert_true((v_accept->>'included_issue_count')::integer=2
    and (select count(*) from public.weekly_discrepancy_incidents
      where candidate_id='d3000000-0000-4000-8000-000000000001' and state='OPEN')=0
    and (select count(*) from public.weekly_discrepancy_incidents
      where candidate_id='d3000000-0000-4000-8000-000000000002' and state='OPEN')=1,
    'inner group acceptance was incomplete or crossed the selected group');

  v_no_shifts_workspace:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','finalise',
    'source_group_id','d5000000-0000-4000-8000-000000000002',
    'source_cycle_id','d6000000-0000-4000-8000-000000000002'
  ));
  perform pg_temp.assert_true(
    v_no_shifts_workspace#>>'{profile,label}'='NHSP'
    and v_no_shifts_workspace#>>'{context,controls,0,label}'='Source'
    and v_no_shifts_workspace#>>'{context,controls,1,label}'='Trust'
    and v_no_shifts_workspace#>>'{context,controls,2,label}'='Report number'
    and v_no_shifts_workspace#>>'{context,controls,3,label}'='Cutoff'
    and pg_catalog.jsonb_array_length(v_no_shifts_workspace#>'{context,controls}')=4,
    'NHSP workspace did not return the locked Source, Trust, Report number and Cutoff context');
  perform pg_temp.assert_true(
    v_no_shifts_workspace#>>'{finalise,tracker,cycle_id}'='d6000000-0000-4000-8000-000000000002'
    and v_no_shifts_workspace#>>'{finalise,tracker,cycle_options,0,value}'='d6000000-0000-4000-8000-000000000002',
    'NHSP finalisation tracker did not expose its selected week as a filter');
  perform pg_temp.assert_true(
    v_no_shifts_workspace#>>'{finalise,tracker,rows,0,actions,0,command}'='NO_SHIFTS_TO_IMPORT'
    and nullif(v_no_shifts_workspace#>>'{finalise,tracker,rows,0,actions,0,context,trust}','') is not null
    and nullif(v_no_shifts_workspace#>>'{finalise,tracker,rows,0,actions,0,context,cutoff}','') is not null
    and (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(
      v_no_shifts_workspace#>'{finalise,tracker,rows,0,actions,0,payload}'))=6,
    'no-shifts tracker action mixed display context into or omitted an exact command payload');

  begin
    perform public.weekly_source_no_shifts_attest_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','d1000000-0000-4000-8000-000000000001',
      'source_cycle_id','d6000000-0000-4000-8000-000000000002',
      'source_group_id','d5000000-0000-4000-8000-000000000002',
      'client_id','d2000000-0000-4000-8000-000000000002',
      'expected_cycle_version',99,'attestation_text','No shifts to import'
    ));
  exception when sqlstate '40001' then
    v_no_shifts_stale:=true;
  end;
  perform pg_temp.assert_true(v_no_shifts_stale and not exists(
    select 1 from public.weekly_source_client_cycle_completions
    where source_cycle_id='d6000000-0000-4000-8000-000000000002'
  ),'stale no-shifts attestation wrote a partial completion');

  v_no_shifts:=public.weekly_source_no_shifts_attest_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000002',
    'source_group_id','d5000000-0000-4000-8000-000000000002',
    'client_id','d2000000-0000-4000-8000-000000000002',
    'expected_cycle_version',1,'attestation_text','No shifts to import'
  ));
  perform pg_temp.assert_true(v_no_shifts->>'cycle_state'='FINALISABLE',
    'first client attestation finalised an incomplete group');
  v_no_shifts:=public.weekly_source_no_shifts_attest_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000002',
    'source_group_id','d5000000-0000-4000-8000-000000000002',
    'client_id','d2000000-0000-4000-8000-000000000003',
    'expected_cycle_version',1,'attestation_text','No shifts to import'
  ));
  perform pg_temp.assert_true(v_no_shifts->>'cycle_state'='FINALISED'
    and (select state from public.weekly_source_cycles
      where id='d6000000-0000-4000-8000-000000000002')='FINALISED'
    and exists(
      select 1 from public.weekly_source_cycles next_cycle
      where next_cycle.source_group_id='d5000000-0000-4000-8000-000000000002'
        and next_cycle.finalisation_week_ending=
          (select finalisation_week_ending+7 from public.weekly_source_cycles
           where id='d6000000-0000-4000-8000-000000000002')
        and next_cycle.state='OPEN' and next_cycle.version=0
        and next_cycle.projection_state='NONE'
    ),
    'all-client completion did not finalise the group cycle');
  v_no_shifts:=public.weekly_source_no_shifts_attest_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'source_cycle_id','d6000000-0000-4000-8000-000000000002',
    'source_group_id','d5000000-0000-4000-8000-000000000002',
    'client_id','d2000000-0000-4000-8000-000000000003',
    'expected_cycle_version',1,'attestation_text','No shifts to import'
  ));
  perform pg_temp.assert_true((v_no_shifts->>'idempotent')::boolean,
    'exact no-shifts replay was not idempotent');

  begin
    perform public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','queries','unexpected',true
    ));
  exception when sqlstate '22023' then
    v_invalid_rejected:=true;
  end;
  perform pg_temp.assert_true(v_invalid_rejected,'unknown workspace request key was accepted');
end;
$test$;

do $security$
declare
  v_signature text;
  v_definition text;
begin
  foreach v_signature in array array[
    'public.weekly_source_office_workspace_v1(jsonb)',
    'public.weekly_source_office_timesheet_presentation_v1(jsonb)',
    'public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)',
    'public.weekly_source_no_shifts_attest_atomic_v1(jsonb)'
  ]::text[] loop
    perform pg_temp.assert_true(pg_catalog.has_function_privilege('service_role',v_signature,'EXECUTE'),
      v_signature||' is not service-role executable');
    perform pg_temp.assert_true(not pg_catalog.has_function_privilege('anon',v_signature,'EXECUTE')
      and not pg_catalog.has_function_privilege('authenticated',v_signature,'EXECUTE')
      and not exists(
        select 1
        from pg_catalog.pg_proc proc
        cross join lateral pg_catalog.aclexplode(
          coalesce(proc.proacl,pg_catalog.acldefault('f',proc.proowner))
        ) privilege
        where proc.oid=pg_catalog.to_regprocedure(v_signature)
          and privilege.grantee=0 and privilege.privilege_type='EXECUTE'
      ),
      v_signature||' is exposed beyond service_role');
  end loop;
  select pg_catalog.pg_get_functiondef(
    'public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)'::pg_catalog.regprocedure
  ) into v_definition;
  perform pg_temp.assert_true(v_definition !~* '(insert|update|delete)[[:space:]]+(into[[:space:]]+|from[[:space:]]+)?public\.(timesheets_financials|invoices|invoice_lines|pay_|banking_)',
    'bulk owner directly writes a finance, invoice or Banking relation');
  select pg_catalog.pg_get_functiondef(
    'public.weekly_source_no_shifts_attest_atomic_v1(jsonb)'::pg_catalog.regprocedure
  ) into v_definition;
  perform pg_temp.assert_true(v_definition !~* '(timesheets_financials|invoice_lines|pay_workbench|banking_pay)',
    'no-shifts owner references a protected financial path');
end;
$security$;



















-- ===========================================================================
-- GATE 9 (WP-11b) — the server-owned lifecycle phase, the proposal view and
-- `UI-022`.
--
-- Every one of the 22 rows of `P:\annexes\ui-lifecycle-state-matrix.csv` is
-- produced from a DATABASE STATE that genuinely reaches it and is then read
-- back out of the projection.  No row is asserted by seeding the projection's
-- own output, and no heading is asserted against a copy of itself: section
-- `G9-V1` pins all 22 heading strings against the matrix's own text first.
--
-- Banking Pay states are FIXTURES in the existing evidence tables, which is
-- exactly what contract decision D2 requires; no Banking Pay owner is called,
-- defined, wrapped or re-pointed anywhere in this file.
-- ===========================================================================

-- G9-V1.  The exact heading text of the lifecycle policy, character for
-- character, for all 22 rows, pinned against the matrix before anything else
-- runs.  `UI-022` carries U+00B7 between its two clauses.  Files 17 and 18 are
-- deliberately NOT followed: the deleted heading `Hours being authorised` must
-- appear nowhere (contract erratum E-4).
do $gate9_headings$
declare
  v_expected jsonb:=pg_catalog.jsonb_build_object(
    'UI-001','Hours to authorise',
    'UI-002','Hours to authorise',
    'UI-003','Hours to authorise',
    'UI-004','Hours to authorise',
    'UI-005','Approved hours',
    'UI-006','Approved hours',
    'UI-007','Hours paid',
    'UI-008','Currently approved hours',
    'UI-009','Hours paid to date',
    'UI-010','Hours paid to date',
    'UI-011','Approved hours',
    'UI-012','Current paid hours',
    'UI-013','Currently approved hours',
    'UI-014','Timesheet hours',
    'UI-015','Timesheet hours',
    'UI-019','Submitted Timesheet',
    'UI-020','Submitted Timesheet plus Approved hours to be paid',
    'UI-021','Timesheet (read-only)',
    'UI-022','Not authorised for pay '||pg_catalog.chr(183)||' invoiced from source'
  );
  v_policy jsonb:=private.weekly_source_office_lifecycle_policy_v1();
  v_key text;
  v_actual text;
begin
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(v_policy->'rows')=22,
    'the lifecycle policy does not carry all 22 matrix rows');
  for v_key in select pg_catalog.jsonb_object_keys(v_expected) loop
    select policy_row.value->>'heading' into v_actual
    from pg_catalog.jsonb_array_elements(v_policy->'rows') as policy_row(value)
    where policy_row.value->>'ui_state'=v_key;
    perform pg_temp.assert_true(v_actual is not distinct from (v_expected->>v_key),
      'heading text for '||v_key||' is not the lifecycle policy text: got '||coalesce(v_actual,'<null>'));
  end loop;
  -- The three rows whose heading is deliberately NOT this owner's.
  perform pg_temp.assert_true((
    select pg_catalog.count(*)::integer
    from pg_catalog.jsonb_array_elements(v_policy->'rows') as policy_row(value)
    where policy_row.value->>'ui_state' in ('UI-016','UI-017','UI-018')
      and policy_row.value->'heading'='null'::jsonb)=3,
    'UI-016, UI-017 and UI-018 must carry no Weekly Source heading');
  perform pg_temp.assert_true(v_policy::text !~* 'hours being authorised',
    'the deleted heading Hours being authorised is present (contract erratum E-4)');
  perform pg_temp.assert_true((
    select pg_catalog.count(distinct policy_row.value->>'ui_state')::integer
    from pg_catalog.jsonb_array_elements(v_policy->'rows') as policy_row(value))=22,
    'the lifecycle policy repeats a ui_state');
end;
$gate9_headings$;

-- ---------------------------------------------------------------------------
-- The Gate 9 world.  Four clients: one source authority, one Timesheet
-- authority, one source-fixed-expense, one in no Weekly Source group at all.
-- ---------------------------------------------------------------------------
insert into public.clients(id,name,ts_queries_email,vat_chargeable) values
  ('f2000000-0000-4000-8000-000000000001','Gate 9 Source Trust','g9@example.invalid',true),
  ('f2000000-0000-4000-8000-000000000002','Gate 9 Signed Trust','g9b@example.invalid',true),
  ('f2000000-0000-4000-8000-000000000003','Gate 9 Expense Trust','g9c@example.invalid',true),
  ('f2000000-0000-4000-8000-000000000004','Gate 9 Ordinary Trust','g9d@example.invalid',true);

insert into public.client_settings(
  id,client_id,effective_from,hr_validation_required,autoprocess_hr,
  self_bill_no_invoices_sent,no_timesheet_required,requires_hr
) values
  ('f2100000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-01-01',true,true,true,true,true),
  ('f2100000-0000-4000-8000-000000000002','f2000000-0000-4000-8000-000000000002','2026-01-01',true,true,false,false,true),
  ('f2100000-0000-4000-8000-000000000003','f2000000-0000-4000-8000-000000000003','2026-01-01',true,true,true,true,true),
  ('f2100000-0000-4000-8000-000000000004','f2000000-0000-4000-8000-000000000004','2026-01-01',true,true,true,true,true);

insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email) values
  ('f3000000-0000-4000-8000-000000000001','G9-001','Sam','Source','Sam Source','sam@example.invalid'),
  ('f3000000-0000-4000-8000-000000000002','G9-002','Sig','Signed','Sig Signed','sig@example.invalid'),
  ('f3000000-0000-4000-8000-000000000003','G9-003','Eve','Expense','Eve Expense','eve@example.invalid');

insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr,
  overrideclientsettings,require_reference_to_pay
) values
  ('f4000000-0000-4000-8000-000000000001','f3000000-0000-4000-8000-000000000001',
   'f2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
   true,'HEALTHROSTER',true,true,true,false,false),
  ('f4000000-0000-4000-8000-000000000002','f3000000-0000-4000-8000-000000000002',
   'f2000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}',
   true,'HEALTHROSTER',true,true,true,true,false),
  ('f4000000-0000-4000-8000-000000000003','f3000000-0000-4000-8000-000000000003',
   'f2000000-0000-4000-8000-000000000003','2026-01-01','2026-12-31','PAYE','{}',
   true,'HEALTHROSTER',true,true,true,false,false),
  ('f4000000-0000-4000-8000-000000000004','f3000000-0000-4000-8000-000000000003',
   'f2000000-0000-4000-8000-000000000004','2026-01-01','2026-12-31','PAYE','{}',
   true,'HEALTHROSTER',true,true,true,false,false),
  ('f4000000-0000-4000-8000-000000000005','f3000000-0000-4000-8000-000000000001',
   'f2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
   true,'HEALTHROSTER',true,true,true,false,false);

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values
  ('f5000000-0000-4000-8000-000000000001','TEST','d0000000-0000-4000-8000-000000000001',
   'G9_SOURCE','Gate 9 source authority','ROSTER',3,'15:00'),
  ('f5000000-0000-4000-8000-000000000002','TEST','d0000000-0000-4000-8000-000000000001',
   'G9_SIGNED','Gate 9 Timesheet authority','ROSTER',3,'15:00'),
  ('f5000000-0000-4000-8000-000000000003','TEST','d0000000-0000-4000-8000-000000000001',
   'G9_EXPENSE','Gate 9 source fixed expense','ROSTER',3,'15:00');
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values
  ('f5000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-01-01','d1000000-0000-4000-8000-000000000001'),
  ('f5000000-0000-4000-8000-000000000002','f2000000-0000-4000-8000-000000000002','2026-01-01','d1000000-0000-4000-8000-000000000001'),
  ('f5000000-0000-4000-8000-000000000003','f2000000-0000-4000-8000-000000000003','2026-01-01','d1000000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,
  source_fixed_expenses_enabled,candidate_queries_enabled,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values
  ('f5000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-01-01',
   'SOURCE_AUTHORITY','CHECK_ONLY',true,false,true,true,'g9m@example.invalid',
   'd1000000-0000-4000-8000-000000000001'),
  ('f5000000-0000-4000-8000-000000000002','f2000000-0000-4000-8000-000000000002','2026-01-01',
   'TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',false,false,true,true,'g9m@example.invalid',
   'd1000000-0000-4000-8000-000000000001'),
  ('f5000000-0000-4000-8000-000000000003','f2000000-0000-4000-8000-000000000003','2026-01-01',
   'SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,true,'g9m@example.invalid',
   'd1000000-0000-4000-8000-000000000001');

insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values
  ('f6000000-0000-4000-8000-000000000001','f5000000-0000-4000-8000-000000000001',
   '2026-09-06','2026-09-09 14:00:00+00','OPEN',1,'NONE'),
  ('f6000000-0000-4000-8000-000000000002','f5000000-0000-4000-8000-000000000002',
   '2026-09-06','2026-09-09 14:00:00+00','OPEN',1,'NONE'),
  ('f6000000-0000-4000-8000-000000000003','f5000000-0000-4000-8000-000000000003',
   '2026-09-06','2026-09-09 14:00:00+00','OPEN',1,'NONE');
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
  coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id
) values
  ('f7000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000001',
   'g9-source.xlsx',decode(repeat('71',32),'hex'),100,
   '34444444-4444-4444-8444-444444444444','verify','verify',decode(repeat('72',32),'hex'),
   decode(repeat('73',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,
   decode(repeat('74',32),'hex'),'CURRENT','d1000000-0000-4000-8000-000000000001'),
  ('f7000000-0000-4000-8000-000000000002','f6000000-0000-4000-8000-000000000002',
   'g9-signed.xlsx',decode(repeat('75',32),'hex'),100,
   '34444444-4444-4444-8444-444444444444','verify','verify',decode(repeat('76',32),'hex'),
   decode(repeat('77',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,
   decode(repeat('78',32),'hex'),'CURRENT','d1000000-0000-4000-8000-000000000001'),
  ('f7000000-0000-4000-8000-000000000003','f6000000-0000-4000-8000-000000000003',
   'g9-expense.xlsx',decode(repeat('79',32),'hex'),100,
   '34444444-4444-4444-8444-444444444444','verify','verify',decode(repeat('7a',32),'hex'),
   decode(repeat('7b',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,
   decode(repeat('7c',32),'hex'),'CURRENT','d1000000-0000-4000-8000-000000000001');
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values
  ('f8000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000001','CYCLE',
   'f7000000-0000-4000-8000-000000000001',1,decode(repeat('81',32),'hex'),
   decode(repeat('82',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp()),
  ('f8000000-0000-4000-8000-000000000002','f6000000-0000-4000-8000-000000000002','CYCLE',
   'f7000000-0000-4000-8000-000000000002',1,decode(repeat('83',32),'hex'),
   decode(repeat('84',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp()),
  ('f8000000-0000-4000-8000-000000000003','f6000000-0000-4000-8000-000000000003','CYCLE',
   'f7000000-0000-4000-8000-000000000003',1,decode(repeat('85',32),'hex'),
   decode(repeat('86',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp());
update public.weekly_source_cycles
set projection_state='CURRENT',
    current_complete_upload_id=('f7000000-0000-4000-8000-00000000000'||
      pg_catalog.right(id::text,1))::uuid,
    current_projection_publication_id=('f8000000-0000-4000-8000-00000000000'||
      pg_catalog.right(id::text,1))::uuid
where id in ('f6000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000002',
             'f6000000-0000-4000-8000-000000000003');


-- Timesheets, one per lifecycle scenario.  Each family has its own
-- `booking_id`, so the installed rotation resolver gives each one its own
-- family and no scenario can borrow another's evidence.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,r2_auth_key,img_sha256_auth,
  contract_id,sheet_scope,line_type,actual_schedule_json,authorised_at_server
)
select
  row_data.timesheet_id,row_data.booking_id,'g9-occupant','g9-hospital','g9-ward','nurse',
  (row_data.work_date||' 08:00:00+00')::timestamptz,(row_data.work_date||' 17:00:00+00')::timestamptz,
  30,510,row_data.week_ending,
  row_data.nurse_key,row_data.nurse_hash,row_data.auth_key,row_data.auth_hash,
  row_data.contract_id,row_data.scope::public.timesheet_scope_enum,'HOURS',row_data.schedule,row_data.authorised
from (values
  -- UI-001 exact match, never authorised
  ('fa000000-0000-4000-8000-000000000001'::uuid,'G9-T01','2026-09-01'::date,'2026-09-06'::date,
   'g9/t01.png',repeat('1',64),null::text,null::text,'f4000000-0000-4000-8000-000000000001'::uuid,'WEEKLY',
   '[{"date":"2026-09-01","start":"09:00","end":"17:00","break_minutes":30}]'::jsonb,null::timestamptz),
  -- UI-002 no Candidate submission at all
  ('fa000000-0000-4000-8000-000000000002'::uuid,'G9-T02','2026-09-08','2026-09-13',
   null,null,null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',null,null),
  -- UI-003 mismatch
  ('fa000000-0000-4000-8000-000000000003'::uuid,'G9-T03','2026-09-15','2026-09-20',
   'g9/t03.png',repeat('3',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-09-15","start":"09:00","end":"18:00","break_minutes":30}]',null),
  -- UI-004 Office alternate hours awaiting first authorisation
  ('fa000000-0000-4000-8000-000000000004'::uuid,'G9-T04','2026-09-22','2026-09-27',
   'g9/t04.png',repeat('4',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-09-22","start":"09:00","end":"18:00","break_minutes":30}]',null),
  -- UI-005 authorised, not paid
  ('fa000000-0000-4000-8000-000000000005'::uuid,'G9-T05','2026-09-29','2026-10-04',
   'g9/t05.png',repeat('5',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-09-29","start":"09:00","end":"17:00","break_minutes":30}]','2026-10-05 09:00:00+00'),
  -- UI-006 payment being processed
  ('fa000000-0000-4000-8000-000000000006'::uuid,'G9-T06','2026-10-06','2026-10-11',
   'g9/t06.png',repeat('6',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-10-06","start":"09:00","end":"17:00","break_minutes":30}]','2026-10-12 09:00:00+00'),
  -- UI-007 paid
  ('fa000000-0000-4000-8000-000000000007'::uuid,'G9-T07','2026-10-13','2026-10-18',
   'g9/t07.png',repeat('7',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-10-13","start":"09:00","end":"17:00","break_minutes":30}]','2026-10-19 09:00:00+00'),
  -- UI-008 later change pending, unpaid
  ('fa000000-0000-4000-8000-000000000008'::uuid,'G9-T08','2026-10-20','2026-10-25',
   'g9/t08.png',repeat('8',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-10-20","start":"09:00","end":"17:00","break_minutes":30}]','2026-10-26 09:00:00+00'),
  -- UI-009 later change pending, paid
  ('fa000000-0000-4000-8000-000000000009'::uuid,'G9-T09','2026-10-27','2026-11-01',
   'g9/t09.png',repeat('9',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-10-27","start":"09:00","end":"17:00","break_minutes":30}]','2026-11-02 09:00:00+00'),
  -- UI-010 approved change frozen behind a live payment
  ('fa000000-0000-4000-8000-00000000000a'::uuid,'G9-T10','2026-11-03','2026-11-08',
   'g9/t10.png',repeat('a',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-11-03","start":"09:00","end":"17:00","break_minutes":30}]','2026-11-09 09:00:00+00'),
  -- UI-011 later change published, not settled
  ('fa000000-0000-4000-8000-00000000000b'::uuid,'G9-T11','2026-11-10','2026-11-15',
   'g9/t11.png',repeat('b',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-11-10","start":"09:00","end":"17:00","break_minutes":30}]','2026-11-16 09:00:00+00'),
  -- UI-012 adjustment settled
  ('fa000000-0000-4000-8000-00000000000c'::uuid,'G9-T12','2026-11-17','2026-11-22',
   'g9/t12.png',repeat('c',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-11-17","start":"09:00","end":"17:00","break_minutes":30}]','2026-11-23 09:00:00+00'),
  -- UI-013 cross-Contract decision pending
  ('fa000000-0000-4000-8000-00000000000d'::uuid,'G9-T13','2026-11-24','2026-11-29',
   'g9/t13.png',repeat('d',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-11-24","start":"09:00","end":"17:00","break_minutes":30}]','2026-11-30 09:00:00+00'),
  -- UI-014 Timesheet authority, match, before first authorisation
  ('fa000000-0000-4000-8000-00000000000e'::uuid,'G9-T14','2026-09-01','2026-09-06',
   'g9/t14.png',repeat('e',64),'g9/t14a.png',repeat('e',64),'f4000000-0000-4000-8000-000000000002','WEEKLY',
   '[{"date":"2026-09-01","start":"09:00","end":"17:00","break_minutes":30}]',null),
  -- UI-015 Timesheet authority, mismatch
  ('fa000000-0000-4000-8000-00000000000f'::uuid,'G9-T15','2026-09-08','2026-09-13',
   'g9/t15.png',repeat('f',64),'g9/t15a.png',repeat('f',64),'f4000000-0000-4000-8000-000000000002','WEEKLY',
   '[{"date":"2026-09-08","start":"09:00","end":"18:00","break_minutes":30}]',null),
  -- UI-016 ordinary Weekly: the Client is in no Weekly Source group
  ('fa000000-0000-4000-8000-000000000010'::uuid,'G9-T16','2026-09-01','2026-09-06',
   'g9/t16.png',repeat('2',64),null,null,'f4000000-0000-4000-8000-000000000004','WEEKLY',
   '[{"date":"2026-09-01","start":"09:00","end":"17:00","break_minutes":30}]',null),
  -- UI-017 Daily
  ('fa000000-0000-4000-8000-000000000011'::uuid,'G9-T17','2026-09-01','2026-09-06',
   'g9/t17.png',repeat('b',64),null,null,'f4000000-0000-4000-8000-000000000004','DAILY',
   '[{"date":"2026-09-01","start":"09:00","end":"17:00","break_minutes":30}]',null),
  -- UI-018 source-fixed expense overlay
  ('fa000000-0000-4000-8000-000000000012'::uuid,'G9-T18','2026-09-01','2026-09-06',
   'g9/t18.png',repeat('c',64),null,null,'f4000000-0000-4000-8000-000000000003','WEEKLY',
   '[{"date":"2026-09-01","start":"09:00","end":"17:00","break_minutes":30}]',null),
  -- UI-022 first authorisation withdrawn while the week is invoiced from source
  ('fa000000-0000-4000-8000-000000000013'::uuid,'G9-T22','2026-12-01','2026-12-06',
   'g9/t22.png',repeat('d',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-12-01","start":"09:00","end":"17:00","break_minutes":30}]',null),
  -- UI-020 Candidate surface: approved hours differ from the submission
  ('fa000000-0000-4000-8000-000000000014'::uuid,'G9-T20','2026-12-08','2026-12-13',
   'g9/t20.png',repeat('e',64),null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',
   '[{"date":"2026-12-08","start":"09:00","end":"18:00","break_minutes":30}]','2026-12-14 09:00:00+00'),
  -- UI-021 Candidate surface: no submission, Office authorised from source
  ('fa000000-0000-4000-8000-000000000015'::uuid,'G9-T21','2026-12-15','2026-12-20',
   null,null,null,null,'f4000000-0000-4000-8000-000000000001','WEEKLY',null,'2026-12-21 09:00:00+00'),
  -- UI-019 Candidate surface: approved hours equal the submission
  ('fa000000-0000-4000-8000-000000000017'::uuid,'G9-T19','2026-09-15','2026-09-20',
   'g9/t19.png',repeat('a',64),'g9/t19a.png',repeat('a',64),'f4000000-0000-4000-8000-000000000002','WEEKLY',
   '[{"date":"2026-09-15","start":"09:00","end":"17:00","break_minutes":30}]','2026-09-21 09:00:00+00'),
  -- UI-013's B root: the new Contract the bundle would move the week onto
  -- provably blank: no submission and no schedule, which is what 24 section 4.5
  -- means by a genuinely new B root
  ('fa000000-0000-4000-8000-000000000016'::uuid,'G9-T13B','2026-11-24','2026-11-29',
   null,null,null,null,'f4000000-0000-4000-8000-000000000005','WEEKLY',null,null)
) as row_data(timesheet_id,booking_id,work_date,week_ending,nurse_key,nurse_hash,
              auth_key,auth_hash,contract_id,scope,schedule,authorised);

-- Work events and the source rows they came from.  Only the weeks that need a
-- latest-source schedule get rows; the rest legitimately have none.
insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
)
select row_data.event_id,row_data.candidate_id,row_data.client_id,row_data.work_date,
  'PROFILE_EXTERNAL_KEY',row_data.external_key,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5(row_data.external_key),64,'0'),'hex'),
  row_data.group_id,'34444444-4444-4444-8444-444444444444'
from (values
  ('f9000000-0000-4000-8000-000000000001'::uuid,'f3000000-0000-4000-8000-000000000001'::uuid,'f2000000-0000-4000-8000-000000000001'::uuid,'2026-09-01'::date,'g9-e01','f5000000-0000-4000-8000-000000000001'::uuid),
  ('f9000000-0000-4000-8000-000000000002'::uuid,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-09-08','g9-e02','f5000000-0000-4000-8000-000000000001'),
  ('f9000000-0000-4000-8000-000000000003'::uuid,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-09-15','g9-e03','f5000000-0000-4000-8000-000000000001'),
  ('f9000000-0000-4000-8000-000000000004'::uuid,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-09-22','g9-e04','f5000000-0000-4000-8000-000000000001'),
  ('f9000000-0000-4000-8000-000000000008'::uuid,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-10-20','g9-e08','f5000000-0000-4000-8000-000000000001'),
  ('f9000000-0000-4000-8000-000000000013'::uuid,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-12-01','g9-e13','f5000000-0000-4000-8000-000000000001'),
  ('f9000000-0000-4000-8000-000000000014'::uuid,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-12-08','g9-e14','f5000000-0000-4000-8000-000000000001'),
  ('f9000000-0000-4000-8000-000000000015'::uuid,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','2026-12-15','g9-e15','f5000000-0000-4000-8000-000000000001'),
  ('f9000000-0000-4000-8000-000000000018'::uuid,'f3000000-0000-4000-8000-000000000003','f2000000-0000-4000-8000-000000000003','2026-09-01','g9-e18','f5000000-0000-4000-8000-000000000003')
) as row_data(event_id,candidate_id,client_id,work_date,external_key,group_id);

insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,source_candidate_identity,source_client_identity,
  work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,
  normalised_row_hash,external_source_key
)
select row_data.row_id,row_data.upload_id,row_data.ordinal,'sam source','gate 9 trust',
  row_data.work_date,
  (row_data.work_date||' 09:00:00')::timestamp,(row_data.work_date||' 17:00:00')::timestamp,30,450,
  'SOURCE_WORKED',
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5(row_data.external_key),64,'0'),'hex'),
  row_data.external_key
from (values
  ('fb000000-0000-4000-8000-000000000001'::uuid,'f7000000-0000-4000-8000-000000000001'::uuid,1,'2026-09-01'::date,'g9-r01'),
  ('fb000000-0000-4000-8000-000000000002'::uuid,'f7000000-0000-4000-8000-000000000001',2,'2026-09-08','g9-r02'),
  ('fb000000-0000-4000-8000-000000000003'::uuid,'f7000000-0000-4000-8000-000000000001',3,'2026-09-15','g9-r03'),
  ('fb000000-0000-4000-8000-000000000004'::uuid,'f7000000-0000-4000-8000-000000000001',4,'2026-09-22','g9-r04'),
  ('fb000000-0000-4000-8000-000000000008'::uuid,'f7000000-0000-4000-8000-000000000001',8,'2026-10-20','g9-r08'),
  ('fb000000-0000-4000-8000-000000000013'::uuid,'f7000000-0000-4000-8000-000000000001',13,'2026-12-01','g9-r13'),
  ('fb000000-0000-4000-8000-000000000014'::uuid,'f7000000-0000-4000-8000-000000000001',14,'2026-12-08','g9-r14'),
  ('fb000000-0000-4000-8000-000000000015'::uuid,'f7000000-0000-4000-8000-000000000001',15,'2026-12-15','g9-r15'),
  ('fb000000-0000-4000-8000-000000000018'::uuid,'f7000000-0000-4000-8000-000000000003',1,'2026-09-01','g9-r18')
) as row_data(row_id,upload_id,ordinal,work_date,external_key);

insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,work_event_id,candidate_id,client_id,contract_id,
  contract_selection_method,work_event_match_kind,work_event_match_fingerprint,
  qualification_profile_fingerprint,qualifying_contract_count,qualifying_contract_set_hash,
  source_row_fingerprint
)
select row_data.resolution_id,row_data.row_id,1,'RESOLVED',row_data.event_id,
  row_data.candidate_id,row_data.client_id,row_data.contract_id,
  'AUTO_UNIQUE','NEW_PROFILE_KEY',
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('wem'||row_data.row_id::text),64,'0'),'hex'),
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('qual'||row_data.row_id::text),64,'0'),'hex'),1,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('set'||row_data.row_id::text),64,'0'),'hex'),
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('fp'||row_data.row_id::text),64,'0'),'hex')
from (values
  ('fc000000-0000-4000-8000-000000000001'::uuid,'fb000000-0000-4000-8000-000000000001'::uuid,'f9000000-0000-4000-8000-000000000001'::uuid,'f3000000-0000-4000-8000-000000000001'::uuid,'f2000000-0000-4000-8000-000000000001'::uuid,'f4000000-0000-4000-8000-000000000001'::uuid),
  ('fc000000-0000-4000-8000-000000000002'::uuid,'fb000000-0000-4000-8000-000000000002','f9000000-0000-4000-8000-000000000002','f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001'),
  ('fc000000-0000-4000-8000-000000000003'::uuid,'fb000000-0000-4000-8000-000000000003','f9000000-0000-4000-8000-000000000003','f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001'),
  ('fc000000-0000-4000-8000-000000000004'::uuid,'fb000000-0000-4000-8000-000000000004','f9000000-0000-4000-8000-000000000004','f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001'),
  ('fc000000-0000-4000-8000-000000000008'::uuid,'fb000000-0000-4000-8000-000000000008','f9000000-0000-4000-8000-000000000008','f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001'),
  ('fc000000-0000-4000-8000-000000000013'::uuid,'fb000000-0000-4000-8000-000000000013','f9000000-0000-4000-8000-000000000013','f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001'),
  ('fc000000-0000-4000-8000-000000000014'::uuid,'fb000000-0000-4000-8000-000000000014','f9000000-0000-4000-8000-000000000014','f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001'),
  ('fc000000-0000-4000-8000-000000000015'::uuid,'fb000000-0000-4000-8000-000000000015','f9000000-0000-4000-8000-000000000015','f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001'),
  ('fc000000-0000-4000-8000-000000000018'::uuid,'fb000000-0000-4000-8000-000000000018','f9000000-0000-4000-8000-000000000018','f3000000-0000-4000-8000-000000000003','f2000000-0000-4000-8000-000000000003','f4000000-0000-4000-8000-000000000003')
) as row_data(resolution_id,row_id,event_id,candidate_id,client_id,contract_id);

-- One comparison row per source-authority Timesheet, which is what links a
-- Timesheet to the CURRENT publication.  UI-015's row is the reference blocker.
insert into public.weekly_timesheet_source_comparisons(
  id,source_cycle_id,upload_id,projection_publication_id,timesheet_id,timesheet_revision,
  work_event_id,contract_id,work_date,comparison_state,candidate_break_minutes,
  total_break_minutes_match,comparison_fingerprint
)
select row_data.comparison_id,row_data.cycle_id,row_data.upload_id,row_data.publication_id,
  row_data.timesheet_id,1,row_data.event_id,row_data.contract_id,row_data.work_date,
  row_data.state,30,true,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('cmp'||row_data.comparison_id::text),64,'0'),'hex')
from (values
  ('fd000000-0000-4000-8000-000000000001'::uuid,'f6000000-0000-4000-8000-000000000001'::uuid,'f7000000-0000-4000-8000-000000000001'::uuid,'f8000000-0000-4000-8000-000000000001'::uuid,'fa000000-0000-4000-8000-000000000001'::uuid,'f9000000-0000-4000-8000-000000000001'::uuid,'f4000000-0000-4000-8000-000000000001'::uuid,'2026-09-01'::date,'SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000002'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000002','f9000000-0000-4000-8000-000000000002','f4000000-0000-4000-8000-000000000001','2026-09-08','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000003'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000003','f9000000-0000-4000-8000-000000000003','f4000000-0000-4000-8000-000000000001','2026-09-15','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000004'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000004','f9000000-0000-4000-8000-000000000004','f4000000-0000-4000-8000-000000000001','2026-09-22','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000005'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000005','f9000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-09-29','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000006'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000006','f9000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-10-06','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000007'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000007','f9000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-10-13','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000008'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000008','f9000000-0000-4000-8000-000000000008','f4000000-0000-4000-8000-000000000001','2026-10-20','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000009'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000009','f9000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-10-27','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-00000000000a'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-00000000000a','f9000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-11-03','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-00000000000b'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-00000000000b','f9000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-11-10','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-00000000000c'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-00000000000c','f9000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-11-17','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-00000000000d'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-00000000000d','f9000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-11-24','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-00000000000f'::uuid,'f6000000-0000-4000-8000-000000000002','f7000000-0000-4000-8000-000000000002','f8000000-0000-4000-8000-000000000002','fa000000-0000-4000-8000-00000000000f','f9000000-0000-4000-8000-000000000002','f4000000-0000-4000-8000-000000000002','2026-09-08','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000012'::uuid,'f6000000-0000-4000-8000-000000000003','f7000000-0000-4000-8000-000000000003','f8000000-0000-4000-8000-000000000003','fa000000-0000-4000-8000-000000000012','f9000000-0000-4000-8000-000000000018','f4000000-0000-4000-8000-000000000003','2026-09-01','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000013'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000013','f9000000-0000-4000-8000-000000000013','f4000000-0000-4000-8000-000000000001','2026-12-01','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000014'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000014','f9000000-0000-4000-8000-000000000014','f4000000-0000-4000-8000-000000000001','2026-12-08','SOURCE_SHIFT_MISSING'),
  ('fd000000-0000-4000-8000-000000000015'::uuid,'f6000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001','fa000000-0000-4000-8000-000000000015','f9000000-0000-4000-8000-000000000015','f4000000-0000-4000-8000-000000000001','2026-12-15','SOURCE_SHIFT_MISSING')
) as row_data(comparison_id,cycle_id,upload_id,publication_id,timesheet_id,event_id,contract_id,work_date,state);

-- UI-003: one OPEN discrepancy incident on the current publication.
insert into public.weekly_discrepancy_incidents(
  id,source_group_id,work_event_id,episode_number,candidate_id,client_id,source_cycle_id,
  current_comparison_revision_id,state,reconciliation_state,candidate_action_state,
  manager_potential_state,manager_action_state,waiting_source_state
) values (
  'fe000000-0000-4000-8000-000000000003','f5000000-0000-4000-8000-000000000001',
  'f9000000-0000-4000-8000-000000000003',1,'f3000000-0000-4000-8000-000000000001',
  'f2000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000001',
  null,'OPEN','UNRESOLVED','NOT_ASKED','NOT_REQUIRED','NOT_REQUIRED','NOT_WAITING');
insert into public.weekly_issue_comparison_revisions(
  id,incident_id,revision_number,projection_publication_id,comparison_upload_id,
  candidate_timesheet_id,issue_family,source_presence,material_comparison_fingerprint
) values (
  'ff000000-0000-4000-8000-000000000003','fe000000-0000-4000-8000-000000000003',1,
  'f8000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000001',
  'fa000000-0000-4000-8000-000000000003','SOURCE_HOURS_DIFFER','PRESENT',
  decode(repeat('a3',32),'hex'));
update public.weekly_discrepancy_incidents
set current_comparison_revision_id='ff000000-0000-4000-8000-000000000003'
where id='fe000000-0000-4000-8000-000000000003';


-- ---------------------------------------------------------------------------
-- Per-scenario state.  Everything below is written into the ordinary tables the
-- owners themselves write; nothing is injected into a projection result.
-- ---------------------------------------------------------------------------

-- Extra source rows for the weeks whose proposal needs a real finalised movement.
insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
)
select row_data.event_id,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
  row_data.work_date,'PROFILE_EXTERNAL_KEY',row_data.external_key,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5(row_data.external_key),64,'0'),'hex'),
  'f5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'
from (values
  ('f9000000-0000-4000-8000-000000000009'::uuid,'2026-10-27'::date,'g9-e09'),
  ('f9000000-0000-4000-8000-00000000000d'::uuid,'2026-11-24'::date,'g9-e0d')
) as row_data(event_id,work_date,external_key);

insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,source_candidate_identity,source_client_identity,
  work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,
  normalised_row_hash,external_source_key
)
select row_data.row_id,'f7000000-0000-4000-8000-000000000001',row_data.ordinal,'sam source','gate 9 trust',
  row_data.work_date,(row_data.work_date||' 09:00:00')::timestamp,
  (row_data.work_date||' 17:00:00')::timestamp,30,450,'SOURCE_WORKED',
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5(row_data.external_key),64,'0'),'hex'),
  row_data.external_key
from (values
  ('fb000000-0000-4000-8000-000000000009'::uuid,9,'2026-10-27'::date,'g9-r09'),
  ('fb000000-0000-4000-8000-00000000000d'::uuid,20,'2026-11-24'::date,'g9-r0d')
) as row_data(row_id,ordinal,work_date,external_key);

insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,work_event_id,candidate_id,client_id,contract_id,
  contract_selection_method,work_event_match_kind,work_event_match_fingerprint,
  qualification_profile_fingerprint,qualifying_contract_count,qualifying_contract_set_hash,
  source_row_fingerprint
)
select row_data.resolution_id,row_data.row_id,1,'RESOLVED',row_data.event_id,
  'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001','AUTO_UNIQUE','NEW_PROFILE_KEY',
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('wem'||row_data.row_id::text),64,'0'),'hex'),
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('qual'||row_data.row_id::text),64,'0'),'hex'),1,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('set'||row_data.row_id::text),64,'0'),'hex'),
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('fp'||row_data.row_id::text),64,'0'),'hex')
from (values
  ('fc000000-0000-4000-8000-000000000009'::uuid,'fb000000-0000-4000-8000-000000000009'::uuid,'f9000000-0000-4000-8000-000000000009'::uuid),
  ('fc000000-0000-4000-8000-00000000000d'::uuid,'fb000000-0000-4000-8000-00000000000d','f9000000-0000-4000-8000-00000000000d')
) as row_data(resolution_id,row_id,event_id);

-- One CURRENT final source revision for the Gate 9 source-authority cycle.
insert into public.weekly_source_final_revisions(
  id,source_cycle_id,authority_scope_kind,revision_number,upload_id,
  coverage_start_local_date,coverage_end_local_date,coverage_timezone,reason,
  finalised_by_user_id,finalised_at_utc,manifest_hash,policy_fingerprint,state
) values (
  'f1000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000001','CYCLE',1,
  'f7000000-0000-4000-8000-000000000001','2026-08-31','2026-12-31','Europe/London',
  'INITIAL_FINALISATION','d1000000-0000-4000-8000-000000000001','2026-09-10 10:00:00+00',
  decode(repeat('91',32),'hex'),decode(repeat('92',32),'hex'),'CURRENT');

-- One finalised source movement per proposal root, plus the one that puts the
-- UI-022 week on a self-bill.  The NHSP physical form is used because it needs
-- no state-transition chain and is exactly a finalised source line.
insert into public.weekly_source_billing_movements(
  id,nhsp_upload_row_id,final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,
  contract_id,work_event_id,movement_role,source_profile_kind,source_line_kind,
  source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,total_pay_ex_vat,
  calculated_comparison_charge_pence,source_validation_charge_pence,invoice_presentation_charge_pence,
  vat_rate_pct,vat_amount,total_inc_vat,price_check_result,mapping_rate_policy_fingerprint,
  invoice_timesheet_id,original_cycle_key,movement_economic_hash,placement_state
)
select row_data.movement_id,row_data.row_id,'f1000000-0000-4000-8000-000000000001',
  'f6000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001',
  row_data.event_id,'POSITIVE','NHSP_TRUST_BACKING_REPORT','NHSP_PHYSICAL_POSITIVE',
  -- `row_resolution_id` is where WP-06's segment builder reads the lineage from,
  -- and the two canonical vectors are the shape it reads hours out of, so these
  -- movements are composable rather than merely present.
  pg_catalog.jsonb_build_object('source_row_ordinal',1,'work_date',row_data.work_date,
    'row_resolution_id',row_data.resolution_id),
  pg_catalog.jsonb_build_object('row_sign',1,'total_pence',15000,
    'hours',pg_catalog.jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0)),
  pg_catalog.jsonb_build_object('row_sign',1,'total_pence',20000,
    'hours',pg_catalog.jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0)),
  150.00,20000,20000,20000,20.00,40.00,240.00,'EXACT',
  decode(repeat('93',32),'hex'),row_data.timesheet_id,'G9-CYCLE-1',
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('mv'||row_data.movement_id::text),64,'0'),'hex'),
  row_data.placement
from (values
  ('e0000000-0000-4000-8000-000000000008'::uuid,'fb000000-0000-4000-8000-000000000008'::uuid,'f9000000-0000-4000-8000-000000000008'::uuid,'fa000000-0000-4000-8000-000000000008'::uuid,'2026-10-20'::date,'UNPLACED','fc000000-0000-4000-8000-000000000008'::uuid),
  ('e0000000-0000-4000-8000-000000000009'::uuid,'fb000000-0000-4000-8000-000000000009','f9000000-0000-4000-8000-000000000009','fa000000-0000-4000-8000-000000000009','2026-10-27','UNPLACED','fc000000-0000-4000-8000-000000000009'),
  ('e0000000-0000-4000-8000-00000000000d'::uuid,'fb000000-0000-4000-8000-00000000000d','f9000000-0000-4000-8000-00000000000d','fa000000-0000-4000-8000-00000000000d','2026-11-24','UNPLACED','fc000000-0000-4000-8000-00000000000d'),
  ('e0000000-0000-4000-8000-000000000013'::uuid,'fb000000-0000-4000-8000-000000000013','f9000000-0000-4000-8000-000000000013','fa000000-0000-4000-8000-000000000013','2026-12-01','PLACED','fc000000-0000-4000-8000-000000000013')
) as row_data(movement_id,row_id,event_id,timesheet_id,work_date,placement,resolution_id);

-- The root authorisation record (decision D8), per root, exactly as the
-- first-authorisation owner writes it.  UI-022's row is the withdrawn one.
insert into public.weekly_source_root_authorisations(
  id,root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
  authorised_row_signature,authorised_by_user_id,authorised_at_utc,
  withdrawn_at_utc,withdrawn_by_user_id
)
select row_data.authorisation_id,row_data.timesheet_id,timesheet_row.booking_id,
  timesheet_row.version,1,'g9-signature-'||row_data.timesheet_id::text,
  'd1000000-0000-4000-8000-000000000001','2026-12-30 09:00:00+00',
  row_data.withdrawn_at,row_data.withdrawn_by
from (values
  ('e5000000-0000-4000-8000-000000000005'::uuid,'fa000000-0000-4000-8000-000000000005'::uuid,null::timestamptz,null::uuid),
  ('e5000000-0000-4000-8000-000000000006'::uuid,'fa000000-0000-4000-8000-000000000006',null,null),
  ('e5000000-0000-4000-8000-000000000007'::uuid,'fa000000-0000-4000-8000-000000000007',null,null),
  ('e5000000-0000-4000-8000-000000000008'::uuid,'fa000000-0000-4000-8000-000000000008',null,null),
  ('e5000000-0000-4000-8000-000000000009'::uuid,'fa000000-0000-4000-8000-000000000009',null,null),
  ('e5000000-0000-4000-8000-00000000000a'::uuid,'fa000000-0000-4000-8000-00000000000a',null,null),
  ('e5000000-0000-4000-8000-00000000000b'::uuid,'fa000000-0000-4000-8000-00000000000b',null,null),
  ('e5000000-0000-4000-8000-00000000000c'::uuid,'fa000000-0000-4000-8000-00000000000c',null,null),
  ('e5000000-0000-4000-8000-00000000000d'::uuid,'fa000000-0000-4000-8000-00000000000d',null,null),
  ('e5000000-0000-4000-8000-000000000017'::uuid,'fa000000-0000-4000-8000-000000000017',null,null),
  ('e5000000-0000-4000-8000-000000000014'::uuid,'fa000000-0000-4000-8000-000000000014',null,null),
  ('e5000000-0000-4000-8000-000000000015'::uuid,'fa000000-0000-4000-8000-000000000015',null,null),
  ('e5000000-0000-4000-8000-000000000013'::uuid,'fa000000-0000-4000-8000-000000000013',
   '2026-12-31 09:00:00+00'::timestamptz,'d1000000-0000-4000-8000-000000000001'::uuid)
) as row_data(authorisation_id,timesheet_id,withdrawn_at,withdrawn_by)
join public.timesheets timesheet_row on timesheet_row.timesheet_id=row_data.timesheet_id;


-- Decision bundles and entitlement heads.  `UI-011` needs a SECOND committed
-- head (a later change that has published), so its family carries a superseded
-- generation and a current one.
insert into public.weekly_source_entitlement_decision_bundles(
  decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
  source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
  target_root_family_booking_id,target_root_timesheet_id,target_contract_id,
  decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
  contract_choice_digest,before_inventory_digest,proposed_head_ids,state,committed_at_utc
) values
  ('e6000000-0000-4000-8000-000000000008',1,'d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','2026-10-25','SINGLE_ROOT','G9-T08',
   'fa000000-0000-4000-8000-000000000008','f4000000-0000-4000-8000-000000000001',
   null,null,null,
   'e7000000-0000-4000-8000-000000000008','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
   decode(repeat('b8',32),'hex'),decode(repeat('c8',32),'hex'),decode(repeat('d8',32),'hex'),
   decode(repeat('e8',32),'hex'),array['e8000000-0000-4000-8000-000000000008']::uuid[],
   'COMMITTED','2026-12-30 10:00:00+00'),
  ('e6000000-0000-4000-8000-00000000000b',1,'d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','2026-11-15','SINGLE_ROOT','G9-T11',
   'fa000000-0000-4000-8000-00000000000b','f4000000-0000-4000-8000-000000000001',
   null,null,null,
   'e7000000-0000-4000-8000-00000000000b','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
   decode(repeat('bb',32),'hex'),decode(repeat('cb',32),'hex'),decode(repeat('db',32),'hex'),
   decode(repeat('eb',32),'hex'),array['e8000000-0000-4000-8000-00000000000b']::uuid[],
   'COMMITTED','2026-12-30 10:00:00+00'),
  ('e6000000-0000-4000-8000-00000000001b',2,'d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','2026-11-15','SINGLE_ROOT','G9-T11',
   'fa000000-0000-4000-8000-00000000000b','f4000000-0000-4000-8000-000000000001',
   null,null,null,
   'e7000000-0000-4000-8000-00000000001b','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
   decode(repeat('ba',32),'hex'),decode(repeat('ca',32),'hex'),decode(repeat('da',32),'hex'),
   decode(repeat('ea',32),'hex'),array['e8000000-0000-4000-8000-00000000001b']::uuid[],
   'COMMITTED','2026-12-30 11:00:00+00'),
  -- UI-010: the bundle behind the saved, frozen decision.
  ('e6000000-0000-4000-8000-00000000000a',1,'d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','2026-11-08','SINGLE_ROOT','G9-T10',
   'fa000000-0000-4000-8000-00000000000a','f4000000-0000-4000-8000-000000000001',
   null,null,null,
   'e7000000-0000-4000-8000-00000000000a','d1000000-0000-4000-8000-000000000001','DEFERRED',
   decode(repeat('ab',32),'hex'),decode(repeat('ac',32),'hex'),decode(repeat('ad',32),'hex'),
   decode(repeat('ae',32),'hex'),array['e8000000-0000-4000-8000-00000000000a']::uuid[],
   'PROPOSED',null);

insert into public.weekly_source_entitlement_heads(
  id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,root_timesheet_id,
  root_family_booking_id,root_timesheet_version,head_revision,prior_head_id,state,
  certified_zero,component_count,entitlement_digest,inventory_digest,source_generation_digest,
  publication_receipt_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id,
  scope_change_tx_token,committed_at_utc
) values
  ('e8000000-0000-4000-8000-000000000008','LOCKED_FINAL_SOURCE','d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-10-25',
   'fa000000-0000-4000-8000-000000000008','G9-T08',1,1,null,'COMMITTED_CURRENT',
   false,1,decode(repeat('18',32),'hex'),decode(repeat('28',32),'hex'),decode(repeat('38',32),'hex'),
   decode(repeat('48',32),'hex'),'e6000000-0000-4000-8000-000000000008',1,
   'e7000000-0000-4000-8000-000000000008','d1000000-0000-4000-8000-000000000001',
   'e9000000-0000-4000-8000-000000000008','2026-12-30 10:00:00+00'),
  ('e8000000-0000-4000-8000-00000000000b','LOCKED_FINAL_SOURCE','d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-11-15',
   'fa000000-0000-4000-8000-00000000000b','G9-T11',1,1,null,'STAGED',
   true,0,decode(repeat('1b',32),'hex'),decode(repeat('2b',32),'hex'),decode(repeat('3b',32),'hex'),
   null,'e6000000-0000-4000-8000-00000000000b',1,
   'e7000000-0000-4000-8000-00000000000b','d1000000-0000-4000-8000-000000000001',null,null),
  ('e8000000-0000-4000-8000-00000000001b','LOCKED_FINAL_SOURCE','d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-11-15',
   'fa000000-0000-4000-8000-00000000000b','G9-T11',1,2,'e8000000-0000-4000-8000-00000000000b',
   'COMMITTED_CURRENT',true,0,decode(repeat('1a',32),'hex'),decode(repeat('2a',32),'hex'),
   decode(repeat('3a',32),'hex'),decode(repeat('4a',32),'hex'),
   'e6000000-0000-4000-8000-00000000001b',2,
   'e7000000-0000-4000-8000-00000000001b','d1000000-0000-4000-8000-000000000001',
   'e9000000-0000-4000-8000-00000000001b','2026-12-30 11:00:00+00');
update public.weekly_source_entitlement_heads
set state='SUPERSEDED',committed_at_utc='2026-12-30 10:00:00+00',
    publication_receipt_digest=decode(repeat('4b',32),'hex'),
    scope_change_tx_token='e9000000-0000-4000-8000-00000000000b',
    superseded_at_utc='2026-12-30 11:00:00+00',
    superseded_by_head_id='e8000000-0000-4000-8000-00000000001b'
where id='e8000000-0000-4000-8000-00000000000b';

insert into public.weekly_source_entitlement_head_components(
  head_id,component_ordinal,component_id,component_kind,economic_key_type,economic_key_value,
  component_member_identity,segment_id,segment_key,work_date,hours_day,pay_ex_vat,
  charge_ex_vat,exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
)
-- The component identity is the one WP-06's composer DERIVES from the work
-- identity, and the hours deliberately differ from the source, so the later
-- change is a real `changed` row rather than an add-and-remove pair.
select 'e8000000-0000-4000-8000-000000000008',1,
  private.weekly_source_entitlement_component_id_v1(
    'WORKED_TIME','SEGMENT',
    'weekly-source-event:f9000000-0000-4000-8000-000000000008',
    'f9000000-0000-4000-8000-000000000008'),
  'WORKED_TIME','SEGMENT','weekly-source-event:f9000000-0000-4000-8000-000000000008',
  'f9000000-0000-4000-8000-000000000008',
  'weekly-source-event:f9000000-0000-4000-8000-000000000008',
  'f9000000-0000-4000-8000-000000000008','2026-10-20',8.000000,160.00,210.00,false,
  'WEEKLY_SOURCE','e6000000-0000-4000-8000-000000000008',1,decode(repeat('58',32),'hex');

update public.weekly_source_root_authorisations
set current_entitlement_head_id='e8000000-0000-4000-8000-000000000008'
where root_timesheet_id='fa000000-0000-4000-8000-000000000008';
update public.weekly_source_root_authorisations
set current_entitlement_head_id='e8000000-0000-4000-8000-00000000001b'
where root_timesheet_id='fa000000-0000-4000-8000-00000000000b';



-- ---------------------------------------------------------------------------
-- Banking Pay evidence, as FIXTURES in the existing evidence tables (contract
-- decision D2).  No Banking Pay owner is called and none is defined here.
-- One live Draft for `UI-006`; settled evidence for `UI-007` and `UI-009`; two
-- settled batches over one root for `UI-012`.
-- ---------------------------------------------------------------------------
insert into public.pay_batches(
  id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
  rail_provider_snapshot,rail_env_snapshot,same_week_paye_override_used,
  execution_commit_state,execution_committed_at_utc,completed_at_utc
) values
  ('ec000000-0000-4000-8000-000000000006','2026-10-15','DRAFT','REVOLUT_CSV','CSV','CSV','PROD',false,
   'NOT_SUBMITTED',null,null),
  ('ec000000-0000-4000-8000-000000000007','2026-10-22','SETTLED','REVOLUT_CSV','CSV','CSV','PROD',false,
   'COMMITTED','2026-10-22 10:00:00+00','2026-10-22 11:00:00+00'),
  ('ec000000-0000-4000-8000-000000000009','2026-11-05','SETTLED','REVOLUT_CSV','CSV','CSV','PROD',false,
   'COMMITTED','2026-11-05 10:00:00+00','2026-11-05 11:00:00+00'),
  ('ec000000-0000-4000-8000-00000000000c','2026-11-26','SETTLED','REVOLUT_CSV','CSV','CSV','PROD',false,
   'COMMITTED','2026-11-26 10:00:00+00','2026-11-26 11:00:00+00'),
  ('ec000000-0000-4000-8000-00000000001c','2026-12-03','SETTLED','REVOLUT_CSV','CSV','CSV','PROD',false,
   'COMMITTED','2026-12-03 10:00:00+00','2026-12-03 11:00:00+00');

insert into public.pay_batch_candidates(
  id,pay_batch_id,candidate_id,settlement_status,settled_at_utc
) values
  ('ed000000-0000-4000-8000-000000000006','ec000000-0000-4000-8000-000000000006',
   'f3000000-0000-4000-8000-000000000001',null,null),
  ('ed000000-0000-4000-8000-000000000007','ec000000-0000-4000-8000-000000000007',
   'f3000000-0000-4000-8000-000000000001','SETTLED','2026-10-22 11:00:00+00'),
  ('ed000000-0000-4000-8000-000000000009','ec000000-0000-4000-8000-000000000009',
   'f3000000-0000-4000-8000-000000000001','SETTLED','2026-11-05 11:00:00+00'),
  ('ed000000-0000-4000-8000-00000000000c','ec000000-0000-4000-8000-00000000000c',
   'f3000000-0000-4000-8000-000000000001','SETTLED','2026-11-26 11:00:00+00'),
  ('ed000000-0000-4000-8000-00000000001c','ec000000-0000-4000-8000-00000000001c',
   'f3000000-0000-4000-8000-000000000001','SETTLED','2026-12-03 11:00:00+00');

-- The one live, non-voided item that makes `UI-006` a payment in flight.
insert into public.pay_batch_items(
  id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,amount_ex_vat,is_voided
) values (
  'ee000000-0000-4000-8000-000000000006','ed000000-0000-4000-8000-000000000006','TIMESHEET',
  'fa000000-0000-4000-8000-000000000006','PAYE',150.00,false);

insert into public.pay_batch_timesheet_snapshots(
  id,pay_batch_id,timesheet_id,candidate_id,pay_channel,base_snapshot_json,
  target_snapshot_json,signature,created_at_utc
)
select row_data.snapshot_id,row_data.batch_id,row_data.timesheet_id,
  'f3000000-0000-4000-8000-000000000001','PAYE',
  pg_catalog.jsonb_build_object('segments','[]'::jsonb),row_data.snapshot,
  -- The installed writer signs a snapshot as md5 of its own target snapshot
  -- text (pay_batch_create_timesheet_snapshots), and the Gate 9 settlement
  -- reader proves that binding, so the fixture signs the same way rather than
  -- carrying a placeholder.
  pg_catalog.md5(row_data.snapshot::text),row_data.created_at
from (values
  ('ef000000-0000-4000-8000-000000000007'::uuid,'ec000000-0000-4000-8000-000000000007'::uuid,
   'fa000000-0000-4000-8000-000000000007'::uuid,
   pg_catalog.jsonb_build_object('segments',pg_catalog.jsonb_build_array(
     pg_catalog.jsonb_build_object('segment_id','g9-seg-t07','date','2026-10-13',
       'start_utc','2026-10-13T09:00:00Z','end_utc','2026-10-13T17:00:00Z','break_mins',30,
       'hours_day','7.5','hours_night','0','hours_sat','0','hours_sun','0','hours_bh','0'))),
   'g9-sig-t07','2026-10-22 11:00:00+00'::timestamptz),
  ('ef000000-0000-4000-8000-000000000009'::uuid,'ec000000-0000-4000-8000-000000000009',
   'fa000000-0000-4000-8000-000000000009',
   pg_catalog.jsonb_build_object('segments',pg_catalog.jsonb_build_array(
     pg_catalog.jsonb_build_object('segment_id','g9-seg-t09','date','2026-10-27',
       'start_utc','2026-10-27T09:00:00Z','end_utc','2026-10-27T17:00:00Z','break_mins',30,
       'hours_day','7.5','hours_night','0','hours_sat','0','hours_sun','0','hours_bh','0'))),
   'g9-sig-t09','2026-11-05 11:00:00+00'),
  ('ef000000-0000-4000-8000-00000000000c'::uuid,'ec000000-0000-4000-8000-00000000000c',
   'fa000000-0000-4000-8000-00000000000c',
   pg_catalog.jsonb_build_object('segments',pg_catalog.jsonb_build_array(
     pg_catalog.jsonb_build_object('segment_id','g9-seg-t12','date','2026-11-17',
       'start_utc','2026-11-17T09:00:00Z','end_utc','2026-11-17T17:00:00Z','break_mins',30,
       'hours_day','7.5','hours_night','0','hours_sat','0','hours_sun','0','hours_bh','0'))),
   'g9-sig-t12a','2026-11-26 11:00:00+00'),
  -- the later adjustment, settled in a SECOND batch over the same root
  ('ef000000-0000-4000-8000-00000000001c'::uuid,'ec000000-0000-4000-8000-00000000001c',
   'fa000000-0000-4000-8000-00000000000c',
   pg_catalog.jsonb_build_object('segments',pg_catalog.jsonb_build_array(
     pg_catalog.jsonb_build_object('segment_id','g9-seg-t12b','date','2026-11-18',
       'start_utc','2026-11-18T09:00:00Z','end_utc','2026-11-18T12:00:00Z','break_mins',0,
       'hours_day','3.0','hours_night','0','hours_sat','0','hours_sun','0','hours_bh','0'))),
   'g9-sig-t12b','2026-12-03 11:00:00+00')
) as row_data(snapshot_id,batch_id,timesheet_id,snapshot,signature,created_at);

insert into public.timesheet_pay_state_history(
  id,timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
)
select row_data.history_id,snapshot_row.timesheet_id,snapshot_row.pay_batch_id,
  snapshot_row.created_at_utc,snapshot_row.target_snapshot_json,snapshot_row.signature
from (values
  ('f0100000-0000-4000-8000-000000000007'::uuid,'ef000000-0000-4000-8000-000000000007'::uuid),
  ('f0100000-0000-4000-8000-000000000009'::uuid,'ef000000-0000-4000-8000-000000000009'),
  ('f0100000-0000-4000-8000-00000000000c'::uuid,'ef000000-0000-4000-8000-00000000000c'),
  ('f0100000-0000-4000-8000-00000000001c'::uuid,'ef000000-0000-4000-8000-00000000001c')
) as row_data(history_id,snapshot_id)
join public.pay_batch_timesheet_snapshots snapshot_row on snapshot_row.id=row_data.snapshot_id;

-- ---------------------------------------------------------------------------
-- `UI-022`: the finalised source movement of the withdrawn week is on a
-- self-bill and stays there (DEC-061 Option A).  Nothing here is a pay
-- schedule; the projection returns it under `invoice_movement_history`.
-- ---------------------------------------------------------------------------
insert into public.weekly_source_client_manifests(
  id,final_revision_id,source_group_id,source_cycle_id,client_id,finalisation_week_ending,
  manifest_hash,movement_count,invoice_state
) values (
  'f0200000-0000-4000-8000-000000000001','f1000000-0000-4000-8000-000000000001',
  'f5000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000001',
  'f2000000-0000-4000-8000-000000000001','2026-09-06',decode(repeat('a1',32),'hex'),1,'READY');
insert into public.invoices(id,client_id,status,subtotal_ex_vat,vat_amount,total_inc_vat)
values ('f0300000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
  'DRAFT',200.00,40.00,240.00);
insert into public.invoice_lines(id,invoice_id,timesheet_id,hours_day,total_pay_ex_vat,
  total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat)
values ('f0400000-0000-4000-8000-000000000001','f0300000-0000-4000-8000-000000000001',
  'fa000000-0000-4000-8000-000000000013',7.50,150.00,200.00,50.00,20.00,40.00,240.00);
insert into public.weekly_source_invoice_presentation_lines(
  id,billing_movement_id,client_manifest_id,final_revision_id,original_finalisation_cycle_id,
  line_kind,origin_kind,client_id,candidate_id,contract_id,work_event_id,source_shift_group_id,
  candidate_display_snapshot,client_display_snapshot,work_date,start_at_local,end_at_local,
  break_minutes,description_snapshot,hours_day,total_pay_ex_vat,total_charge_ex_vat,
  calculated_comparison_charge_pence,source_validation_charge_pence,invoice_presentation_charge_pence,margin_ex_vat,
  vat_rate_pct,vat_amount,total_inc_vat,price_check_result,mapping_rate_policy_fingerprint,
  source_hash,amount_authority,presentation_hash
) values (
  'f0500000-0000-4000-8000-000000000001','e0000000-0000-4000-8000-000000000013',
  'f0200000-0000-4000-8000-000000000001','f1000000-0000-4000-8000-000000000001',
  'f6000000-0000-4000-8000-000000000001','SOURCE_ORDINARY','NHSP_PHYSICAL_ROW',
  'f2000000-0000-4000-8000-000000000001','f3000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001','f9000000-0000-4000-8000-000000000013',
  'f0600000-0000-4000-8000-000000000001','Sam Source','Gate 9 Source Trust','2026-12-01',
  '2026-12-01 09:00:00','2026-12-01 17:00:00',30,'Gate 9 source line',7.50,150.00,200.00,
  20000,20000,20000,50.00,20.00,40.00,240.00,'EXACT',decode(repeat('93',32),'hex'),
  decode(repeat('a5',32),'hex'),'VALIDATED_SOURCE_PENCE',decode(repeat('a6',32),'hex'));
insert into public.weekly_source_invoice_line_bindings(
  id,billing_movement_id,presentation_line_id,invoice_line_id,invoice_id,
  original_final_revision_id,original_cycle_id,client_id,manifest_hash,materialised_line_hash,
  binding_version,state
) values (
  'f0700000-0000-4000-8000-000000000001','e0000000-0000-4000-8000-000000000013',
  'f0500000-0000-4000-8000-000000000001','f0400000-0000-4000-8000-000000000001',
  'f0300000-0000-4000-8000-000000000001','f1000000-0000-4000-8000-000000000001',
  'f6000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
  decode(repeat('a1',32),'hex'),decode(repeat('a7',32),'hex'),1,'CURRENT');

-- `UI-004`: Office alternate hours awaiting the first authorisation.
insert into public.weekly_exceptional_pay_target_families(
  id,agency_id,candidate_id,contract_id,week_start_date,week_ending_date,root_timesheet_id,
  root_family_booking_id,ownership_state,first_signed_evidence_fingerprint,
  current_lifecycle_state,creation_idempotency_key
) values (
  'f0800000-0000-4000-8000-000000000004','d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001',
  '2026-09-21','2026-09-27','fa000000-0000-4000-8000-000000000004','G9-T04',
  'TARGET_MANAGED',decode(repeat('a8',32),'hex'),'PROTECTED','g9-family-t04');
insert into public.weekly_exceptional_orchestration_runs(
  id,family_id,request_kind,idempotency_key,requested_by_user_id,state,before_state_fingerprint
) values (
  'f0a00000-0000-4000-8000-000000000004','f0800000-0000-4000-8000-000000000004','APPROVE',
  'g9-run-t04','d1000000-0000-4000-8000-000000000001','COMPLETE',decode(repeat('b4',32),'hex'));
insert into public.weekly_exceptional_payment_approvals(
  id,pay_target_family_id,work_event_id,candidate_id,client_id,contract_id,week_ending,
  protected_work_date,protected_start_at_local,protected_end_at_local,protected_break_minutes,
  contributing_issue_episode_ids_hash,signed_schedule_fact_hash,
  contract_rate_policy_source_fingerprint,approved_by_user_id,approval_reason,
  source_cycle_id,approved_target_pay_components_json,approved_target_gross,
  creation_orchestration_run_id,approval_hash,creation_idempotency_key,evidence_timesheet_id
) values (
  'f0900000-0000-4000-8000-000000000004','f0800000-0000-4000-8000-000000000004',
  'f9000000-0000-4000-8000-000000000004','f3000000-0000-4000-8000-000000000001',
  'f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-09-27',
  '2026-09-22','2026-09-22 09:00:00','2026-09-22 18:00:00',30,
  decode(repeat('a9',32),'hex'),decode(repeat('aa',32),'hex'),decode(repeat('ab',32),'hex'),
  'd1000000-0000-4000-8000-000000000001','Office approved the submitted hours',
  'f6000000-0000-4000-8000-000000000001','{}'::jsonb,170.00,
  'f0a00000-0000-4000-8000-000000000004',decode(repeat('ac',32),'hex'),'g9-approval-t04',
  'fa000000-0000-4000-8000-000000000004');
insert into public.weekly_exceptional_pay_family_events(
  id,family_id,event_sequence,durable_work_event_id,evidence_approval_id,work_date,
  start_at_local,end_at_local,break_minutes,rate_classification_json,
  source_proposal_snapshot_json,source_proposal_hash,fixed_office_target_snapshot_json,
  fixed_office_target_hash,state,office_actor_user_id,office_reason,event_hash
) values (
  'f0b00000-0000-4000-8000-000000000004','f0800000-0000-4000-8000-000000000004',1,
  'f9000000-0000-4000-8000-000000000004','f0900000-0000-4000-8000-000000000004','2026-09-22',
  '2026-09-22 09:00:00','2026-09-22 18:00:00',30,'{}'::jsonb,'{}'::jsonb,
  decode(repeat('ad',32),'hex'),'{}'::jsonb,decode(repeat('ae',32),'hex'),'WAIT',
  'd1000000-0000-4000-8000-000000000001','Office approved the submitted hours',
  decode(repeat('af',32),'hex'));

-- ---------------------------------------------------------------------------
-- `UI-010`: the saved, frozen decision.  Its stored request is COMPLETE and its
-- stored digest is produced here by the one canonical encoder over that exact
-- request, so the proposal view can only display it by reproducing the digest.
-- Building it this way also proves the request shape is one the installed
-- canonicaliser accepts.
-- ---------------------------------------------------------------------------
do $gate9_pending$
declare
  v_pending_id constant uuid:='eb000000-0000-4000-8000-00000000000a';
  v_root constant uuid:='fa000000-0000-4000-8000-00000000000a';
  v_revision public.weekly_source_final_revisions%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_components jsonb;
  v_request jsonb;
  v_canonical jsonb;
  v_digest bytea;
begin
  select * into v_revision from public.weekly_source_final_revisions
  where id='f1000000-0000-4000-8000-000000000001';
  select * into v_timesheet from public.timesheets where timesheet_id=v_root;

  -- The complete entitlement vector, produced by WP-06's OWN composer from a
  -- segment the Office decision approved.  No hand-written component shape.
  v_components:=private.weekly_source_entitlement_components_v1(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'segment_id','g9-segment-t10','date','2026-11-03','ref_num','G9-REF-T10',
      'hours_day',8,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
      'pay_amount',160.00,'charge_amount',210.00,'exclude_from_pay',false,
      'weekly_source',pg_catalog.jsonb_build_object(
        'work_event_id','f9000000-0000-4000-8000-000000000001',
        'calculation_fingerprint','g9-calc-t10'))),
    '[]'::jsonb);

  v_request:=pg_catalog.jsonb_build_object(
    'decision_bundle_id','e6000000-0000-4000-8000-00000000000a',
    'pending_bundle_id',v_pending_id,
    'bundle_revision',1,
    'candidate_id','f3000000-0000-4000-8000-000000000001',
    'member_root_ids',pg_catalog.jsonb_build_array(v_root),
    'member_family_booking_ids',pg_catalog.jsonb_build_array(v_timesheet.booking_id),
    'member_root_versions',pg_catalog.jsonb_build_array(v_timesheet.version),
    'head_ids',pg_catalog.jsonb_build_array('e8000000-0000-4000-8000-00000000000a'),
    'decision_id','e7000000-0000-4000-8000-00000000000a',
    'publication_mode','DEFERRED',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id',v_revision.id,
        'source_cycle_id',v_revision.source_cycle_id,
        'revision_number',v_revision.revision_number,
        'manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
        'policy_fingerprint',pg_catalog.encode(v_revision.policy_fingerprint,'hex')),
      'contract_choices',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'root_ordinal',1,'contract_id','f4000000-0000-4000-8000-000000000001',
        'week_ending_date',v_timesheet.week_ending_date,'selection_method','UNCHANGED')),
      'member_entitlements',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'root_ordinal',1,'authority_kind','LOCKED_FINAL_SOURCE',
        'certified_zero',pg_catalog.jsonb_array_length(v_components)=0,
        'component_count',pg_catalog.jsonb_array_length(v_components),
        'components',v_components))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','SINGLE_ROOT','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(null),
      'before_positions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'root_ordinal',1,'component_ids','[]'::jsonb,
        'inventory_digest',pg_catalog.encode(
          private.weekly_source_publication_request_digest_v1(
            pg_catalog.jsonb_build_object('components','[]'::jsonb)),'hex'))),
      'moved_component_ids','[]'::jsonb,
      'target_root_authorisation',null,'whole_root_office_review',null));

  v_canonical:=private.weekly_source_publication_request_canonical_v1(
    v_request,'IMMEDIATE',null);
  v_digest:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_publication_request_canonical_v1(v_request,'DEFERRED',v_pending_id));

  insert into public.weekly_source_pending_entitlement_bundles(
    id,decision_bundle_id,bundle_revision,candidate_id,member_root_ids,
    member_family_booking_ids,member_root_versions,request_digest,source_revision_digest,
    contract_choice_digest,decision_id,decided_by_user_id,proposed_head_ids,request_json,
    pending_revision,state,next_check_at_utc
  ) values (
    v_pending_id,'e6000000-0000-4000-8000-00000000000a',1,
    'f3000000-0000-4000-8000-000000000001',array[v_root]::uuid[],
    array[v_timesheet.booking_id]::text[],array[v_timesheet.version]::integer[],
    v_digest,
    private.weekly_source_publication_request_digest_v1(
      v_canonical#>'{financial_request,source_revision}'),
    private.weekly_source_publication_request_digest_v1(
      v_canonical#>'{financial_request,contract_choices}'),
    'e7000000-0000-4000-8000-00000000000a','d1000000-0000-4000-8000-000000000001',
    array['e8000000-0000-4000-8000-00000000000a']::uuid[],v_request,1,'PENDING',
    '2026-12-31 12:00:00+00');
end;
$gate9_pending$;


-- ===========================================================================
-- The COMPOSING path, made real.
--
-- `UI-008` and `UI-009` carry a genuine later-source proposal: the finalised
-- source movement is given the lineage WP-06's snapshot builder needs, and the
-- proposal is then composed and recorded by WP-06's OWN owners
-- (`weekly_source_entitlement_proposal_request_v1` +
-- `…_proposal_record_v1`), so the request digest stored on the bundle is the
-- real one.  The Gate 9 proposal view therefore has to resolve the source
-- revision by digest AND reproduce that request digest before it will show
-- anything: the digest-proof rule does real work here rather than failing
-- closed.
-- ===========================================================================

-- The Contract week the source lineage binds to.
insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,status)
values
  ('f1100000-0000-4000-8000-000000000008','f4000000-0000-4000-8000-000000000001','2026-10-25',0,'SUBMITTED'),
  ('f1100000-0000-4000-8000-000000000009','f4000000-0000-4000-8000-000000000001','2026-11-01',0,'SUBMITTED');

-- The per-row economic snapshot the segment builder joins to.
insert into public.weekly_source_row_economic_snapshots(
  id,row_resolution_id,upload_row_id,generation,work_event_id,candidate_id,client_id,contract_id,
  calculator_version,source_mode,rate_method,row_sign,paid_minutes,break_minutes,
  minutes_day,minutes_night,minutes_sat,minutes_sun,minutes_bh,
  hours_day,hours_night,hours_sat,hours_sun,hours_bh,
  pay_day,pay_night,pay_sat,pay_sun,pay_bh,
  charge_day,charge_night,charge_sat,charge_sun,charge_bh,
  total_pay_pence,calculated_charge_pence,invoice_vat_chargeable,invoice_vat_rate_pct,
  source_expense_vat_enabled,canonical_result_json,contract_and_rate_fingerprint,
  effective_policy_fingerprint,invoice_vat_policy_fingerprint,calculation_fingerprint
)
select row_data.snapshot_id,row_data.resolution_id,row_data.upload_row_id,1,row_data.event_id,
  'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
  'HEALTHROSTER_WEEKLY','SPLIT_RATE_WINDOWS',1,450,30,450,0,0,0,0,
  7.50,0,0,0,0,20.00,22.00,24.00,26.00,28.00,30.00,32.00,34.00,36.00,38.00,
  15000,20000,true,20.00,false,'{}'::jsonb,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('crf'||row_data.snapshot_id::text),64,'0'),'hex'),
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('epf'||row_data.snapshot_id::text),64,'0'),'hex'),
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('ivf'||row_data.snapshot_id::text),64,'0'),'hex'),
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('cf'||row_data.snapshot_id::text),64,'0'),'hex')
from (values
  ('f1200000-0000-4000-8000-000000000008'::uuid,'fc000000-0000-4000-8000-000000000008'::uuid,'fb000000-0000-4000-8000-000000000008'::uuid,'f9000000-0000-4000-8000-000000000008'::uuid),
  ('f1200000-0000-4000-8000-000000000009'::uuid,'fc000000-0000-4000-8000-000000000009','fb000000-0000-4000-8000-000000000009','f9000000-0000-4000-8000-000000000009')
) as row_data(snapshot_id,resolution_id,upload_row_id,event_id);

-- The row-to-Timesheet lineage the segment builder requires for the root.
insert into public.weekly_source_row_timesheet_lineages(
  id,row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,contract_id,
  contract_week_id,timesheet_id,family_booking_id,timesheet_version,week_ending_date,
  lineage_fingerprint
)
select row_data.lineage_id,row_data.resolution_id,'f6000000-0000-4000-8000-000000000001',
  row_data.event_id,'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001',row_data.contract_week_id,row_data.timesheet_id,
  timesheet_row.booking_id,timesheet_row.version,timesheet_row.week_ending_date,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('lin'||row_data.lineage_id::text),64,'0'),'hex')
from (values
  ('f1300000-0000-4000-8000-000000000008'::uuid,'fc000000-0000-4000-8000-000000000008'::uuid,'f9000000-0000-4000-8000-000000000008'::uuid,'f1100000-0000-4000-8000-000000000008'::uuid,'fa000000-0000-4000-8000-000000000008'::uuid),
  ('f1300000-0000-4000-8000-000000000009'::uuid,'fc000000-0000-4000-8000-000000000009','f9000000-0000-4000-8000-000000000009','f1100000-0000-4000-8000-000000000009','fa000000-0000-4000-8000-000000000009')
) as row_data(lineage_id,resolution_id,event_id,contract_week_id,timesheet_id)
join public.timesheets timesheet_row on timesheet_row.timesheet_id=row_data.timesheet_id;

do $gate9_compose$
declare
  v_root uuid;
  v_components jsonb;
  v_request jsonb;
  v_recorded jsonb;
  v_bundle_id uuid;
  v_decision_id uuid;
  v_head_id uuid;
begin
  foreach v_root in array array[
    'fa000000-0000-4000-8000-000000000008'::uuid,
    'fa000000-0000-4000-8000-000000000009'::uuid
  ]::uuid[] loop
    v_components:=private.weekly_source_entitlement_components_v1(
      private.weekly_source_ordinary_projection_current_segments_v1(
        v_root,'f1000000-0000-4000-8000-000000000001'),
      private.weekly_source_ordinary_projection_current_expenses_v1(
        v_root,'f1000000-0000-4000-8000-000000000001'));
    perform pg_temp.assert_true(pg_catalog.jsonb_array_length(v_components)=1,
      'the composing path produced no component for '||v_root::text
      ||'; the later-source proposal would be certified zero and the proof empty');

    v_bundle_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_DECISION_BUNDLE_V1','G9-LATER|'||v_root::text);
    v_decision_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_DECISION_V1',v_bundle_id::text||'|1');
    v_head_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_PROPOSED_HEAD_V1',v_bundle_id::text||'|1|'||v_root::text);

    -- WP-06's builder.  It reads the Candidate from the CONTRACT (the fix that
    -- landed on 18 September); a null there fails closed before anything is
    -- written.
    v_request:=private.weekly_source_entitlement_proposal_request_v1(
      v_root,'f1000000-0000-4000-8000-000000000001','LOCKED_FINAL_SOURCE',
      v_bundle_id,1::bigint,v_head_id,v_decision_id,v_components);
    perform pg_temp.assert_true(
      v_request#>>'{candidate_id}'='f3000000-0000-4000-8000-000000000001',
      'WP-06 did not resolve the Candidate through the Contract');

    -- WP-06's recorder.  The digest it stores is the one the Gate 9 proposal
    -- view must reproduce.
    v_recorded:=private.weekly_source_entitlement_proposal_record_v1(
      v_request,'d0000000-0000-4000-8000-000000000001',
      'f4000000-0000-4000-8000-000000000001',
      (select week_ending_date from public.timesheets where timesheet_id=v_root),
      'd1000000-0000-4000-8000-000000000001');
    perform pg_temp.assert_true(coalesce((v_recorded->>'ok')::boolean,false)
      and coalesce((v_recorded->>'created')::boolean,false)
      and v_recorded->>'state'='PROPOSED',
      'WP-06 did not record a PROPOSED bundle for '||v_root::text
      ||': '||v_recorded::text);
  end loop;
end;
$gate9_compose$;

-- A fifth fail-closed family: ONE undecided proposal whose stored request digest
-- is not the digest of the request the server can rebuild.  This is the
-- contradictory-proposal case, now that the composing path works.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json,
  authorised_at_server
) values (
  'fa000000-0000-4000-8000-0000000000f6','G9-FC5','g9-occupant','g9-hospital','g9-ward','nurse',
  '2027-02-01 08:00:00+00','2027-02-01 17:00:00+00',30,510,'2027-02-07',
  'g9/fc5.png',repeat('7',64),'f4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
  '[{"date":"2027-02-01","start":"09:00","end":"17:00","break_minutes":30}]',
  '2026-12-30 09:00:00+00');
insert into public.weekly_timesheet_source_comparisons(
  id,source_cycle_id,upload_id,projection_publication_id,timesheet_id,timesheet_revision,
  work_event_id,contract_id,work_date,comparison_state,candidate_break_minutes,
  total_break_minutes_match,comparison_fingerprint
) values (
  'fd000000-0000-4000-8000-0000000000f6','f6000000-0000-4000-8000-000000000001',
  'f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001',
  'fa000000-0000-4000-8000-0000000000f6',1,'f9000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001','2027-02-01','SOURCE_SHIFT_MISSING',30,true,
  decode(repeat('f6',32),'hex'));
insert into public.weekly_source_root_authorisations(
  id,root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
  authorised_row_signature,authorised_by_user_id
) values (
  'e5000000-0000-4000-8000-0000000000f6','fa000000-0000-4000-8000-0000000000f6','G9-FC5',1,1,
  'g9-fc5','d1000000-0000-4000-8000-000000000001');
insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
) values (
  'f9000000-0000-4000-8000-0000000000f6','f3000000-0000-4000-8000-000000000001',
  'f2000000-0000-4000-8000-000000000001','2027-02-01','PROFILE_EXTERNAL_KEY','g9-e-fc5',
  decode(repeat('66',32),'hex'),'f5000000-0000-4000-8000-000000000001',
  '34444444-4444-4444-8444-444444444444');
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,source_candidate_identity,source_client_identity,
  work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,
  normalised_row_hash,external_source_key
) values (
  'fb000000-0000-4000-8000-0000000000f6','f7000000-0000-4000-8000-000000000001',60,
  'sam source','gate 9 trust','2027-02-01','2027-02-01 09:00:00','2027-02-01 17:00:00',30,450,
  'SOURCE_WORKED',decode(repeat('67',32),'hex'),'g9-r-fc5');
insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,work_event_id,candidate_id,client_id,contract_id,
  contract_selection_method,work_event_match_kind,work_event_match_fingerprint,
  qualification_profile_fingerprint,qualifying_contract_count,qualifying_contract_set_hash,
  source_row_fingerprint
) values (
  'fc000000-0000-4000-8000-0000000000f6','fb000000-0000-4000-8000-0000000000f6',1,'RESOLVED',
  'f9000000-0000-4000-8000-0000000000f6','f3000000-0000-4000-8000-000000000001',
  'f2000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001',
  'AUTO_UNIQUE','NEW_PROFILE_KEY',decode(repeat('68',32),'hex'),
  decode(repeat('69',32),'hex'),1,decode(repeat('6a',32),'hex'),decode(repeat('6b',32),'hex'));
insert into public.weekly_source_billing_movements(
  id,nhsp_upload_row_id,final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,
  contract_id,work_event_id,movement_role,source_profile_kind,source_line_kind,
  source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,total_pay_ex_vat,
  calculated_comparison_charge_pence,source_validation_charge_pence,invoice_presentation_charge_pence,
  vat_rate_pct,vat_amount,total_inc_vat,price_check_result,mapping_rate_policy_fingerprint,
  invoice_timesheet_id,original_cycle_key,movement_economic_hash,placement_state
) values (
  'e0000000-0000-4000-8000-0000000000f6','fb000000-0000-4000-8000-0000000000f6',
  'f1000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000001',
  'f2000000-0000-4000-8000-000000000001','f3000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001','f9000000-0000-4000-8000-0000000000f6',
  'POSITIVE','NHSP_TRUST_BACKING_REPORT','NHSP_PHYSICAL_POSITIVE',
  pg_catalog.jsonb_build_object('source_row_ordinal',1,'work_date','2027-02-01'),
  pg_catalog.jsonb_build_object('row_sign',1,'total_pence',15000,
    'hours',pg_catalog.jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0)),
  pg_catalog.jsonb_build_object('row_sign',1,'total_pence',20000,
    'hours',pg_catalog.jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0)),
  150.00,20000,20000,20000,20.00,40.00,240.00,'EXACT',decode(repeat('93',32),'hex'),
  'fa000000-0000-4000-8000-0000000000f6','G9-CYCLE-1',
  decode(repeat('f6',32),'hex'),'UNPLACED');
insert into public.weekly_source_entitlement_decision_bundles(
  decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
  source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
  decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
  contract_choice_digest,before_inventory_digest,proposed_head_ids,state
)
select 'e6000000-0000-4000-8000-0000000000f7',1,'d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','2027-02-07','SINGLE_ROOT','G9-FC5',
  'fa000000-0000-4000-8000-0000000000f6','f4000000-0000-4000-8000-000000000001',
  'e7000000-0000-4000-8000-0000000000f7','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
  -- the stored request digest is deliberately NOT the digest of any request the
  -- server can rebuild
  decode(repeat('59',32),'hex'),
  -- ...while the source-revision digest IS genuine, so the revision resolves and
  -- the failure is unambiguously the request digest
  private.weekly_source_publication_request_digest_v1(
    pg_catalog.jsonb_build_object(
      'final_revision_id',revision_row.id::text,
      'source_cycle_id',revision_row.source_cycle_id::text,
      'revision_number',revision_row.revision_number,
      'manifest_hash',pg_catalog.encode(revision_row.manifest_hash,'hex'),
      'policy_fingerprint',pg_catalog.encode(revision_row.policy_fingerprint,'hex'))),
  decode(repeat('5a',32),'hex'),decode(repeat('5b',32),'hex'),
  array['e8000000-0000-4000-8000-0000000000f7']::uuid[],'PROPOSED'
from public.weekly_source_final_revisions revision_row
where revision_row.id='f1000000-0000-4000-8000-000000000001';


-- ===========================================================================
-- The cross-Contract A-to-B shape, both halves.
--
-- `24 section 4.5` bounds an amendment bundle at two roots and makes it ONE
-- atomic decision.  Two states exist and they are different matrix rows:
--
--   * the decision is still to be made  -> `UI-013` CROSS_CONTRACT_PENDING;
--   * the decision has been made and saved while the root is frozen
--     -> `UI-010` LATER_CHANGE_APPROVED_FROZEN ("Decision saved; publication
--     pending", no new action).
--
-- Only the SECOND writes the complete two-root I-3 request down, because the
-- request is stored by the save-pending owner, which runs after Office decides.
-- So the two-root machinery is proved POSITIVELY on the saved decision, and
-- `UI-013` states everything that is statable without a two-root composer --
-- both roots' currently approved entitlements, from two independent I-7 reads,
-- and the complete bundle summary -- with the proposed half explicitly
-- unavailable by name.  See `WP-11b_NEEDS.md` N6 for what is missing.
-- ===========================================================================

-- The A root of the saved A-to-B decision, and its genuinely blank B root.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json,
  authorised_at_server
) values
  ('fa000000-0000-4000-8000-0000000000a1','G9-AB1','g9-occupant','g9-hospital','g9-ward','nurse',
   '2027-03-01 08:00:00+00','2027-03-01 17:00:00+00',30,510,'2027-03-07',
   'g9/ab1.png',repeat('a',64),'f4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
   '[{"date":"2027-03-01","start":"09:00","end":"17:00","break_minutes":30}]',
   '2026-12-30 09:00:00+00'),
  ('fa000000-0000-4000-8000-0000000000a2','G9-AB2','g9-occupant','g9-hospital','g9-ward','nurse',
   '2027-03-01 08:00:00+00','2027-03-01 17:00:00+00',30,510,'2027-03-07',
   'g9/ab2.png',repeat('b',64),'f4000000-0000-4000-8000-000000000005','WEEKLY','HOURS',
   '[{"date":"2027-03-01","start":"09:00","end":"17:00","break_minutes":30}]',null);
insert into public.weekly_timesheet_source_comparisons(
  id,source_cycle_id,upload_id,projection_publication_id,timesheet_id,timesheet_revision,
  work_event_id,contract_id,work_date,comparison_state,candidate_break_minutes,
  total_break_minutes_match,comparison_fingerprint
) values (
  'fd000000-0000-4000-8000-0000000000a1','f6000000-0000-4000-8000-000000000001',
  'f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001',
  'fa000000-0000-4000-8000-0000000000a1',1,'f9000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001','2027-03-01','SOURCE_SHIFT_MISSING',30,true,
  decode(repeat('a1',32),'hex'));
insert into public.weekly_source_root_authorisations(
  id,root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
  authorised_row_signature,authorised_by_user_id
) values (
  'e5000000-0000-4000-8000-0000000000a1','fa000000-0000-4000-8000-0000000000a1','G9-AB1',1,1,
  'g9-ab1','d1000000-0000-4000-8000-000000000001');

-- The A root's committed current head: the entitlement the saved decision is
-- about to move to B.
insert into public.weekly_source_entitlement_decision_bundles(
  decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
  source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
  decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
  contract_choice_digest,before_inventory_digest,proposed_head_ids,state,committed_at_utc
) values (
  'e6000000-0000-4000-8000-0000000000a0',1,'d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','2027-03-07','SINGLE_ROOT','G9-AB1',
  'fa000000-0000-4000-8000-0000000000a1','f4000000-0000-4000-8000-000000000001',
  'e7000000-0000-4000-8000-0000000000a0','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
  decode(repeat('a2',32),'hex'),decode(repeat('a3',32),'hex'),decode(repeat('a4',32),'hex'),
  decode(repeat('a5',32),'hex'),array['e8000000-0000-4000-8000-0000000000a0']::uuid[],
  'COMMITTED','2026-12-30 10:00:00+00');
insert into public.weekly_source_entitlement_heads(
  id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,root_timesheet_id,
  root_family_booking_id,root_timesheet_version,head_revision,prior_head_id,state,
  certified_zero,component_count,entitlement_digest,inventory_digest,source_generation_digest,
  publication_receipt_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id,
  scope_change_tx_token,committed_at_utc
) values (
  'e8000000-0000-4000-8000-0000000000a0','LOCKED_FINAL_SOURCE','d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2027-03-07',
  'fa000000-0000-4000-8000-0000000000a1','G9-AB1',1,1,null,'COMMITTED_CURRENT',
  false,1,decode(repeat('a6',32),'hex'),decode(repeat('a7',32),'hex'),decode(repeat('a8',32),'hex'),
  decode(repeat('a9',32),'hex'),'e6000000-0000-4000-8000-0000000000a0',1,
  'e7000000-0000-4000-8000-0000000000a0','d1000000-0000-4000-8000-000000000001',
  'e9000000-0000-4000-8000-0000000000a0','2026-12-30 10:00:00+00');
insert into public.weekly_source_entitlement_head_components(
  head_id,component_ordinal,component_id,component_kind,economic_key_type,economic_key_value,
  component_member_identity,segment_id,segment_key,work_date,hours_day,pay_ex_vat,
  charge_ex_vat,exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
)
select 'e8000000-0000-4000-8000-0000000000a0',1,
  private.weekly_source_entitlement_component_id_v1(
    'WORKED_TIME','SEGMENT','weekly-source-event:g9-ab-shift','g9-ab-shift'),
  'WORKED_TIME','SEGMENT','weekly-source-event:g9-ab-shift','g9-ab-shift',
  'weekly-source-event:g9-ab-shift','g9-ab-shift','2027-03-01',
  9.000000,180.00,240.00,false,'WEEKLY_SOURCE',
  'e6000000-0000-4000-8000-0000000000a0',1,decode(repeat('aa',32),'hex');
update public.weekly_source_root_authorisations
set current_entitlement_head_id='e8000000-0000-4000-8000-0000000000a0'
where root_timesheet_id='fa000000-0000-4000-8000-0000000000a1';

-- The SAVED two-root decision.  The request is complete, carries BOTH members,
-- and its digest is taken in exactly the mode and with exactly the pending id
-- the installed save-pending owner uses, so the proposal view can only display
-- it by reproducing that digest.
do $gate9_ab$
declare
  v_pending_id constant uuid:='eb000000-0000-4000-8000-0000000000a1';
  v_root_a constant uuid:='fa000000-0000-4000-8000-0000000000a1';
  v_root_b constant uuid:='fa000000-0000-4000-8000-0000000000a2';
  v_revision public.weekly_source_final_revisions%rowtype;
  v_components_b jsonb;
  v_request jsonb;
  v_canonical jsonb;
  v_digest bytea;
begin
  select * into v_revision from public.weekly_source_final_revisions
  where id='f1000000-0000-4000-8000-000000000001';

  -- The complete entitlement that MOVES to B, produced by WP-06's own composer
  -- from the same work identity the A head carries, so the component id is the
  -- same on both sides of the move -- which is exactly what `24 section 4.5`
  -- step 2 and interface I-7 guarantee 1 require.
  v_components_b:=private.weekly_source_entitlement_components_v1(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'segment_id','weekly-source-event:g9-ab-shift','date','2027-03-01',
      'ref_num','G9-REF-AB','hours_day',9,'hours_night',0,'hours_sat',0,'hours_sun',0,
      'hours_bh',0,'pay_amount',180.00,'charge_amount',240.00,'exclude_from_pay',false,
      'weekly_source',pg_catalog.jsonb_build_object(
        'work_event_id','g9-ab-shift','calculation_fingerprint','g9-calc-ab'))),
    '[]'::jsonb);

  v_request:=pg_catalog.jsonb_build_object(
    'decision_bundle_id','e6000000-0000-4000-8000-0000000000a1',
    'pending_bundle_id',v_pending_id,
    'bundle_revision',1,
    'candidate_id','f3000000-0000-4000-8000-000000000001',
    'member_root_ids',pg_catalog.jsonb_build_array(v_root_a,v_root_b),
    'member_family_booking_ids',pg_catalog.jsonb_build_array('G9-AB1','G9-AB2'),
    'member_root_versions',pg_catalog.jsonb_build_array(1,1),
    'head_ids',pg_catalog.jsonb_build_array(
      'e8000000-0000-4000-8000-0000000000a3','e8000000-0000-4000-8000-0000000000a4'),
    'decision_id','e7000000-0000-4000-8000-0000000000a1',
    'publication_mode','DEFERRED',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id',v_revision.id,
        'source_cycle_id',v_revision.source_cycle_id,
        'revision_number',v_revision.revision_number,
        'manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
        'policy_fingerprint',pg_catalog.encode(v_revision.policy_fingerprint,'hex')),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id','f4000000-0000-4000-8000-000000000001',
          'week_ending_date','2027-03-07','selection_method','UNCHANGED'),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'contract_id','f4000000-0000-4000-8000-000000000005',
          'week_ending_date','2027-03-07','selection_method','OFFICE_SELECTED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        -- A keeps nothing: the complete entitlement moves to B, which is the
        -- certified-zero A side of an A-to-B amendment.
        pg_catalog.jsonb_build_object('root_ordinal',1,'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',true,'component_count',0,'components','[]'::jsonb),
        pg_catalog.jsonb_build_object('root_ordinal',2,'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',pg_catalog.jsonb_array_length(v_components_b)=0,
          'component_count',pg_catalog.jsonb_array_length(v_components_b),
          'components',v_components_b))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','CROSS_CONTRACT_A_B','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(
        'e8000000-0000-4000-8000-0000000000a0',null),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'component_ids',(private.weekly_source_effective_inventory_v1(v_root_a)->'components'),
          'inventory_digest',
            private.weekly_source_effective_inventory_v1(v_root_a)->>'inventory_digest'),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'component_ids','[]'::jsonb,
          'inventory_digest',
            private.weekly_source_effective_inventory_v1(v_root_b)->>'inventory_digest')),
      'moved_component_ids',pg_catalog.jsonb_build_array(
        (v_components_b->0->>'component_id')),
      'target_root_authorisation',null,'whole_root_office_review',null));

  -- This A-to-B request is hand-built, because it models a decision already
  -- SAVED as frozen-pending rather than one being composed now.  It must still
  -- be a WHOLE move, or it would be a shape this release no longer permits and
  -- every assertion resting on it would pass for the wrong reason.  The
  -- definition is the composer's own: the source root retains nothing, so the
  -- moved set is exactly A's before-position.
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(v_request#>'{control,moved_component_ids}')
      =pg_catalog.jsonb_array_length(
        v_request#>'{control,before_positions,0,component_ids}')
    and (v_request#>'{financial_request,member_entitlements,0,certified_zero}')
        ='true'::jsonb
    and (v_request#>>'{financial_request,member_entitlements,0,component_count}')::integer=0,
    'the frozen-pending A-to-B fixture is not a WHOLE move, which this release '
    ||'no longer permits, so everything asserted from it would be vacuous');

  v_canonical:=private.weekly_source_publication_request_canonical_v1(
    v_request,'IMMEDIATE',null);
  v_digest:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_publication_request_canonical_v1(v_request,'DEFERRED',v_pending_id));

  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    target_root_family_booking_id,target_root_timesheet_id,target_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
    contract_choice_digest,before_inventory_digest,proposed_head_ids,state
  ) values (
    'e6000000-0000-4000-8000-0000000000a1',1,'d0000000-0000-4000-8000-000000000001',
    'f3000000-0000-4000-8000-000000000001','2027-03-07','CROSS_CONTRACT_A_B','G9-AB1',
    v_root_a,'f4000000-0000-4000-8000-000000000001',
    'G9-AB2',v_root_b,'f4000000-0000-4000-8000-000000000005',
    'e7000000-0000-4000-8000-0000000000a1','d1000000-0000-4000-8000-000000000001','DEFERRED',
    -- the acceptance digest, in IMMEDIATE mode with a null pending id (I-3 5.1a)
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(v_request,'IMMEDIATE',null)),
    -- The four approval digests are the CANONICAL ones the coordinator and the
    -- save-pending owner recompute (I-3 5.1a; WP-06c handoff N1).  A
    -- domain-separated sha256 of the raw sub-object is a different value and
    -- nothing would publish.
    private.weekly_source_publication_request_digest_v1(
      v_canonical#>'{financial_request,source_revision}'),
    private.weekly_source_publication_request_digest_v1(
      v_canonical#>'{financial_request,contract_choices}'),
    decode(repeat('ab',32),'hex'),
    array['e8000000-0000-4000-8000-0000000000a3',
          'e8000000-0000-4000-8000-0000000000a4']::uuid[],'PROPOSED');

  insert into public.weekly_source_pending_entitlement_bundles(
    id,decision_bundle_id,bundle_revision,candidate_id,member_root_ids,
    member_family_booking_ids,member_root_versions,request_digest,source_revision_digest,
    contract_choice_digest,decision_id,decided_by_user_id,proposed_head_ids,request_json,
    pending_revision,state,next_check_at_utc
  ) values (
    v_pending_id,'e6000000-0000-4000-8000-0000000000a1',1,
    'f3000000-0000-4000-8000-000000000001',
    array[v_root_a,v_root_b]::uuid[],array['G9-AB1','G9-AB2']::text[],
    array[1,1]::integer[],v_digest,
    -- The four approval digests are the CANONICAL ones the coordinator and the
    -- save-pending owner recompute (I-3 5.1a; WP-06c handoff N1).  A
    -- domain-separated sha256 of the raw sub-object is a different value and
    -- nothing would publish.
    private.weekly_source_publication_request_digest_v1(
      v_canonical#>'{financial_request,source_revision}'),
    private.weekly_source_publication_request_digest_v1(
      v_canonical#>'{financial_request,contract_choices}'),
    'e7000000-0000-4000-8000-0000000000a1','d1000000-0000-4000-8000-000000000001',
    array['e8000000-0000-4000-8000-0000000000a3',
          'e8000000-0000-4000-8000-0000000000a4']::uuid[],v_request,1,'PENDING',
    '2027-03-31 12:00:00+00');
end;
$gate9_ab$;

-- ---------------------------------------------------------------------------
-- `UI-013` itself: an UNDECIDED cross-Contract A-to-B proposal, composed and
-- recorded by WP-06's OWN two-root composer and recorder (WP-06c), so the
-- digest the bundle stores is the real one and the Gate 9 view can only display
-- the proposed entitlement by reproducing it from the bundle row alone.
--
-- The old root carries a committed head; the new root is provably blank, which
-- is what `24 section 4.5` expects of a B root.
-- ---------------------------------------------------------------------------
insert into public.weekly_source_entitlement_decision_bundles(
  decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
  source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
  decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
  contract_choice_digest,before_inventory_digest,proposed_head_ids,state,committed_at_utc
) values (
  'e6000000-0000-4000-8000-00000000001d',1,'d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','2026-11-29','SINGLE_ROOT','G9-T13',
  'fa000000-0000-4000-8000-00000000000d','f4000000-0000-4000-8000-000000000001',
  'e7000000-0000-4000-8000-00000000001d','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
  decode(repeat('ac',32),'hex'),decode(repeat('ad',32),'hex'),decode(repeat('ae',32),'hex'),
  decode(repeat('af',32),'hex'),array['e8000000-0000-4000-8000-00000000001d']::uuid[],
  'COMMITTED','2026-12-30 10:00:00+00');
insert into public.weekly_source_entitlement_heads(
  id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,root_timesheet_id,
  root_family_booking_id,root_timesheet_version,head_revision,prior_head_id,state,
  certified_zero,component_count,entitlement_digest,inventory_digest,source_generation_digest,
  publication_receipt_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id,
  scope_change_tx_token,committed_at_utc
) values (
  'e8000000-0000-4000-8000-00000000001d','LOCKED_FINAL_SOURCE','d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2026-11-29',
  'fa000000-0000-4000-8000-00000000000d','G9-T13',1,1,null,'COMMITTED_CURRENT',
  false,1,decode(repeat('b1',32),'hex'),decode(repeat('b2',32),'hex'),decode(repeat('b3',32),'hex'),
  decode(repeat('b4',32),'hex'),'e6000000-0000-4000-8000-00000000001d',1,
  'e7000000-0000-4000-8000-00000000001d','d1000000-0000-4000-8000-000000000001',
  'e9000000-0000-4000-8000-00000000001d','2026-12-30 10:00:00+00');
insert into public.weekly_source_entitlement_head_components(
  head_id,component_ordinal,component_id,component_kind,economic_key_type,economic_key_value,
  component_member_identity,segment_id,segment_key,work_date,hours_day,pay_ex_vat,
  charge_ex_vat,exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
)
select 'e8000000-0000-4000-8000-00000000001d',1,
  private.weekly_source_entitlement_component_id_v1(
    'WORKED_TIME','SEGMENT','weekly-source-event:g9-t13-shift','g9-t13-shift'),
  'WORKED_TIME','SEGMENT','weekly-source-event:g9-t13-shift','g9-t13-shift',
  'weekly-source-event:g9-t13-shift','g9-t13-shift','2026-11-24',
  11.000000,220.00,290.00,false,'WEEKLY_SOURCE',
  'e6000000-0000-4000-8000-00000000001d',1,decode(repeat('b5',32),'hex');
update public.weekly_source_root_authorisations
set current_entitlement_head_id='e8000000-0000-4000-8000-00000000001d'
where root_timesheet_id='fa000000-0000-4000-8000-00000000000d';

do $gate9_ui013$
declare
  v_root_a constant uuid:='fa000000-0000-4000-8000-00000000000d';
  v_root_b constant uuid:='fa000000-0000-4000-8000-000000000016';
  v_inventory jsonb;
  v_moved uuid[];
  v_request jsonb;
  v_recorded jsonb;
  v_bundle_id uuid;
  v_decision_id uuid;
  v_head_a uuid;
  v_head_b uuid;
begin
  -- The WHOLE entitlement moves, so the move set is exactly interface I-7's
  -- component ids for the old root -- which is the only move set a reader can
  -- derive from what is persisted (WP-06c handoff N3).
  v_inventory:=private.weekly_source_effective_inventory_v1(v_root_a);
  perform pg_temp.assert_true(coalesce((v_inventory->>'ok')::boolean,false)
    and v_inventory->>'authority'='HEAD',
    'the UI-013 old root must carry a committed head for the move to be a move');
  select pg_catalog.array_agg((component.value->>'component_id')::uuid
           order by component.value->>'component_id')
    into v_moved
  from pg_catalog.jsonb_array_elements(v_inventory->'components') as component(value);

  v_bundle_id:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_DECISION_BUNDLE_V1','G9-AB-UNDECIDED|'||v_root_a::text);
  v_decision_id:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_DECISION_V1',v_bundle_id::text||'|1');
  v_head_a:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_PROPOSED_HEAD_V1',v_bundle_id::text||'|1|'||v_root_a::text);
  v_head_b:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_PROPOSED_HEAD_V1',v_bundle_id::text||'|1|'||v_root_b::text);

  -- WP-06's two-root composer.
  v_request:=private.weekly_source_entitlement_proposal_cross_contract_request_v1(
    v_root_a,v_root_b,'f1000000-0000-4000-8000-000000000001',
    v_bundle_id,1::bigint,v_head_a,v_head_b,v_decision_id,v_moved,
    'LOCKED_FINAL_SOURCE','LOCKED_FINAL_SOURCE','UNCHANGED','OFFICE_SELECTED',
    'd1000000-0000-4000-8000-000000000001',null,null);
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(v_request->'member_root_ids')=2
    and v_request#>>'{control,bundle_kind}'='CROSS_CONTRACT_A_B',
    'WP-06 did not compose a two-root cross-Contract request');

  -- WP-06's recorder, which now writes the bundle kind and the three target
  -- columns.  The digest it stores is the one the Gate 9 view must reproduce.
  v_recorded:=private.weekly_source_entitlement_proposal_record_v1(
    v_request,'d0000000-0000-4000-8000-000000000001',
    'f4000000-0000-4000-8000-000000000001','2026-11-29',
    'd1000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_recorded->>'ok')::boolean,false)
    and coalesce((v_recorded->>'created')::boolean,false)
    and v_recorded->>'state'='PROPOSED',
    'WP-06 did not record the undecided cross-Contract bundle: '||v_recorded::text);
  perform pg_temp.assert_true((
    select bundle_row.bundle_kind='CROSS_CONTRACT_A_B'
       and bundle_row.target_root_timesheet_id=v_root_b
       and bundle_row.target_contract_id='f4000000-0000-4000-8000-000000000005'
      from public.weekly_source_entitlement_decision_bundles bundle_row
     where bundle_row.decision_bundle_id=v_bundle_id and bundle_row.bundle_revision=1),
    'the recorded bundle did not carry the cross-Contract identity');
end;
$gate9_ui013$;

-- ---------------------------------------------------------------------------
-- The PARTIAL move, and the screen for a cross-Contract bundle that cannot be
-- rebuilt.  Two separate things are proved on this one old root, which holds
-- TWO components:
--
--   1. Moving only ONE of them is REFUSED.  The finance approver has ruled
--      partial Contract-to-Contract moves out of scope for this release, and
--      WP-06c refuses one at the composer, the recorder and the coordinator,
--      before anything is written.  This verifier proves the gate this file
--      actually reaches -- the composer -- by name, and proves that nothing was
--      written.  It deliberately does NOT hand-author a request to reach the
--      other two gates: a second way to produce an entitlement request is the
--      one thing this programme must never have, and those gates are WP-06c's
--      to prove.
--
--   2. The screen for an unrebuildable cross-Contract bundle is still proved,
--      because one cause of unrebuildability survives the partial-move ruling:
--      a STALE decision.  Only `financial_request` is canonicalised into the
--      request digest -- `control` is deliberately outside it (I-3 section 0)
--      -- so neither the move set nor the target root's authorisation actor can
--      make a stored decision disagree with its rebuild.  What can is the
--      position: when a Contract no longer holds the entitlement the decision
--      was taken against, the rebuilt request differs and any figure shown
--      would be one the decision was never taken against.  So the WHOLE move
--      below is composed and recorded, and then the old Contract's entitlement
--      moves on.  The screen must refuse the proposed half by name and still
--      show everything that is statable.
--
--      Both of those properties are asserted here rather than assumed, and the
--      first of them -- that the authorising actor is NOT a cause -- is pinned
--      explicitly, because it is the thing a reader of this file would most
--      naturally get wrong.
-- ---------------------------------------------------------------------------
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json,
  authorised_at_server
) values
  ('fa000000-0000-4000-8000-0000000000b1','G9-PM1','g9-occupant','g9-hospital','g9-ward','nurse',
   '2027-04-05 08:00:00+00','2027-04-05 17:00:00+00',30,510,'2027-04-11',
   'g9/pm1.png',repeat('c',64),'f4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
   '[{"date":"2027-04-05","start":"09:00","end":"17:00","break_minutes":30}]',
   '2026-12-30 09:00:00+00'),
  ('fa000000-0000-4000-8000-0000000000b2','G9-PM2','g9-occupant','g9-hospital','g9-ward','nurse',
   '2027-04-05 08:00:00+00','2027-04-05 17:00:00+00',30,510,'2027-04-11',
   null,null,'f4000000-0000-4000-8000-000000000005','WEEKLY','HOURS',null,null);
insert into public.weekly_timesheet_source_comparisons(
  id,source_cycle_id,upload_id,projection_publication_id,timesheet_id,timesheet_revision,
  work_event_id,contract_id,work_date,comparison_state,candidate_break_minutes,
  total_break_minutes_match,comparison_fingerprint
) values (
  'fd000000-0000-4000-8000-0000000000b1','f6000000-0000-4000-8000-000000000001',
  'f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001',
  'fa000000-0000-4000-8000-0000000000b1',1,'f9000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001','2027-04-05','SOURCE_SHIFT_MISSING',30,true,
  decode(repeat('b1',32),'hex'));
insert into public.weekly_source_root_authorisations(
  id,root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
  authorised_row_signature,authorised_by_user_id
) values (
  'e5000000-0000-4000-8000-0000000000b1','fa000000-0000-4000-8000-0000000000b1','G9-PM1',1,1,
  'g9-pm1','d1000000-0000-4000-8000-000000000001');
insert into public.weekly_source_entitlement_decision_bundles(
  decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
  source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
  decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
  contract_choice_digest,before_inventory_digest,proposed_head_ids,state,committed_at_utc
) values (
  'e6000000-0000-4000-8000-0000000000b0',1,'d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','2027-04-11','SINGLE_ROOT','G9-PM1',
  'fa000000-0000-4000-8000-0000000000b1','f4000000-0000-4000-8000-000000000001',
  'e7000000-0000-4000-8000-0000000000b0','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
  decode(repeat('b6',32),'hex'),decode(repeat('b7',32),'hex'),decode(repeat('b8',32),'hex'),
  decode(repeat('b9',32),'hex'),array['e8000000-0000-4000-8000-0000000000b0']::uuid[],
  'COMMITTED','2026-12-30 10:00:00+00');
insert into public.weekly_source_entitlement_heads(
  id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,root_timesheet_id,
  root_family_booking_id,root_timesheet_version,head_revision,prior_head_id,state,
  certified_zero,component_count,entitlement_digest,inventory_digest,source_generation_digest,
  publication_receipt_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id,
  scope_change_tx_token,committed_at_utc
) values (
  'e8000000-0000-4000-8000-0000000000b0','LOCKED_FINAL_SOURCE','d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001','2027-04-11',
  'fa000000-0000-4000-8000-0000000000b1','G9-PM1',1,1,null,'COMMITTED_CURRENT',
  false,2,decode(repeat('ba',32),'hex'),decode(repeat('bb',32),'hex'),decode(repeat('bc',32),'hex'),
  decode(repeat('bd',32),'hex'),'e6000000-0000-4000-8000-0000000000b0',1,
  'e7000000-0000-4000-8000-0000000000b0','d1000000-0000-4000-8000-000000000001',
  'e9000000-0000-4000-8000-0000000000b0','2026-12-30 10:00:00+00');
insert into public.weekly_source_entitlement_head_components(
  head_id,component_ordinal,component_id,component_kind,economic_key_type,economic_key_value,
  component_member_identity,segment_id,segment_key,work_date,hours_day,pay_ex_vat,
  charge_ex_vat,exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
)
select 'e8000000-0000-4000-8000-0000000000b0',shift.ordinal,
  private.weekly_source_entitlement_component_id_v1(
    'WORKED_TIME','SEGMENT','weekly-source-event:'||shift.key,shift.key),
  'WORKED_TIME','SEGMENT','weekly-source-event:'||shift.key,shift.key,
  'weekly-source-event:'||shift.key,shift.key,shift.work_date,
  shift.hours,shift.pay,shift.charge,false,'WEEKLY_SOURCE',
  'e6000000-0000-4000-8000-0000000000b0',1,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('pm'||shift.key),64,'0'),'hex')
from (values
  (1,'g9-pm-shift-a','2027-04-05'::date,6.000000,120.00,160.00),
  (2,'g9-pm-shift-b','2027-04-06'::date,5.000000,100.00,140.00)
) as shift(ordinal,key,work_date,hours,pay,charge);
update public.weekly_source_root_authorisations
set current_entitlement_head_id='e8000000-0000-4000-8000-0000000000b0'
where root_timesheet_id='fa000000-0000-4000-8000-0000000000b1';

-- The partial-move root needs the source revision to be DISCOVERABLE, so that
-- the refusal below is unambiguously about the move set and not about the
-- revision.  A projection receipt is the ordinary artefact that records which
-- final revision a root was projected from.
insert into public.weekly_source_ordinary_pay_projection_receipts(
  id,final_revision_id,source_cycle_id,client_id,root_timesheet_id,source_profile_kind,
  source_mode,outcome,source_unit_count,source_unit_outcomes_json,source_unit_manifest_hash,
  source_expense_manifest_hash,final_manifest_hash,final_policy_fingerprint,
  service_snapshot_hash,server_calculation_fingerprint,root_before_hash,root_after_hash,
  idempotency_key,request_hash,receipt_hash,actor_user_id
) values (
  'f1400000-0000-4000-8000-0000000000b1','f1000000-0000-4000-8000-000000000001',
  'f6000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
  'fa000000-0000-4000-8000-0000000000b1','HEALTHROSTER_ACTUAL_ROWS','HEALTHROSTER_WEEKLY',
  'PROPOSED',0,'[]'::jsonb,decode(repeat('c1',32),'hex'),decode(repeat('c2',32),'hex'),
  decode(repeat('c3',32),'hex'),decode(repeat('c4',32),'hex'),decode(repeat('c5',32),'hex'),
  decode(repeat('c6',32),'hex'),decode(repeat('c7',32),'hex'),decode(repeat('c7',32),'hex'),
  'g9-pm-idempotency-key',decode(repeat('c9',32),'hex'),decode(repeat('ca',32),'hex'),
  'd1000000-0000-4000-8000-000000000001');

do $gate9_partial$
declare
  v_root_a constant uuid:='fa000000-0000-4000-8000-0000000000b1';
  v_root_b constant uuid:='fa000000-0000-4000-8000-0000000000b2';
  v_partial uuid[];
  v_whole uuid[];
  v_request jsonb;
  v_recorded jsonb;
  v_bundle_id uuid;
  v_decision_id uuid;
  v_head_a uuid;
  v_head_b uuid;
  v_bundles_before integer;
  v_receipts_before integer;
  v_heads_before integer;
  v_components_before integer;
  v_refusal text:=null;
  v_detail text:=null;
begin
  v_bundle_id:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_DECISION_BUNDLE_V1','G9-AB-PARTIAL|'||v_root_a::text);
  v_decision_id:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_DECISION_V1',v_bundle_id::text||'|1');
  v_head_a:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_PROPOSED_HEAD_V1',v_bundle_id::text||'|1|'||v_root_a::text);
  v_head_b:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_PROPOSED_HEAD_V1',v_bundle_id::text||'|1|'||v_root_b::text);

  -- ======================================================================
  -- 1.  A PARTIAL move is REFUSED, by name, before anything is written.
  -- ======================================================================
  -- ONE of the old root's two components would move.  This is exactly the shape
  -- the finance approver ruled out, so it is exactly the shape that must be
  -- proved to refuse.
  select pg_catalog.array_agg(component.component_id)
    into v_partial
  from public.weekly_source_entitlement_head_components component
  where component.head_id='e8000000-0000-4000-8000-0000000000b0'
    and component.component_ordinal=1;
  perform pg_temp.assert_true(pg_catalog.cardinality(v_partial)=1,
    'the partial fixture must move exactly one of the old root''s two components');

  select pg_catalog.count(*)::integer into v_bundles_before
  from public.weekly_source_entitlement_decision_bundles;
  select pg_catalog.count(*)::integer into v_heads_before
  from public.weekly_source_entitlement_heads;
  select pg_catalog.count(*)::integer into v_components_before
  from public.weekly_source_entitlement_head_components;
  select pg_catalog.count(*)::integer into v_receipts_before
  from public.weekly_source_ordinary_pay_projection_receipts;

  begin
    v_request:=private.weekly_source_entitlement_proposal_cross_contract_request_v1(
      v_root_a,v_root_b,'f1000000-0000-4000-8000-000000000001',
      v_bundle_id,1::bigint,v_head_a,v_head_b,v_decision_id,v_partial,
      'LOCKED_FINAL_SOURCE','LOCKED_FINAL_SOURCE','UNCHANGED','OFFICE_SELECTED',
      'd1000000-0000-4000-8000-000000000001',null,null);
  exception when others then
    get stacked diagnostics v_detail=pg_exception_detail;
    v_refusal:=sqlerrm;
  end;
  perform pg_temp.assert_true(
    v_refusal='WEEKLY_SOURCE_PROPOSAL_PARTIAL_MOVE_UNSUPPORTED',
    'the composer did not refuse a partial move by name; it returned: '
    ||coalesce(v_refusal,'a composed request'));
  perform pg_temp.assert_true(
    (v_detail::jsonb)->>'code'='WEEKLY_SOURCE_PROPOSAL_PARTIAL_MOVE_UNSUPPORTED'
    and (v_detail::jsonb)->>'reason'
        ='ONLY_A_WHOLE_ENTITLEMENT_MOVE_IS_SUPPORTED_IN_THIS_RELEASE'
    and ((v_detail::jsonb)->>'source_before_component_count')::integer=2
    and ((v_detail::jsonb)->>'moved_component_count')::integer=1
    and ((v_detail::jsonb)->>'would_remain_on_the_source')::integer=1,
    'the partial-move refusal did not carry its reason and its counts: '
    ||coalesce(v_detail,'<no detail>'));
  -- The refusal must be readable by an Office user, not only by a machine.
  perform pg_temp.assert_true(
    pg_catalog.char_length(coalesce((v_detail::jsonb)->>'message',''))>80
    and (v_detail::jsonb)->>'message' ~* 'not supported in this release'
    and (v_detail::jsonb)->>'message' ~* 'nothing has been proposed',
    'the partial-move refusal did not explain itself in plain English: '
    ||coalesce((v_detail::jsonb)->>'message','<none>'));

  -- ...and NOTHING was written.  Not the bundle, not the decision's proposed
  -- heads, not a component, not a receipt.
  perform pg_temp.assert_true(
    not exists(select 1 from public.weekly_source_entitlement_decision_bundles bundle_row
                where bundle_row.decision_bundle_id=v_bundle_id)
    and not exists(select 1 from public.weekly_source_entitlement_heads head_row
                where head_row.id in (v_head_a,v_head_b))
    and (select pg_catalog.count(*)::integer
           from public.weekly_source_entitlement_decision_bundles)=v_bundles_before
    and (select pg_catalog.count(*)::integer
           from public.weekly_source_entitlement_heads)=v_heads_before
    and (select pg_catalog.count(*)::integer
           from public.weekly_source_entitlement_head_components)=v_components_before
    and (select pg_catalog.count(*)::integer
           from public.weekly_source_ordinary_pay_projection_receipts)=v_receipts_before,
    'the refused partial move still wrote something');
  -- The old root keeps its whole entitlement: nothing moved, nothing was split.
  perform pg_temp.assert_true((
    select head_row.component_count=2 and head_row.state='COMMITTED_CURRENT'
      from public.weekly_source_entitlement_heads head_row
     where head_row.id='e8000000-0000-4000-8000-0000000000b0'),
    'the refused partial move disturbed the old root''s committed head');

  -- ======================================================================
  -- 2.  A WHOLE move, recorded, and then made STALE -- which is the only way a
  --     stored cross-Contract decision can fail to rebuild once partial moves
  --     are refused at composition.
  -- ======================================================================
  -- The move set is the old root's complete I-7 inventory: what this release
  -- supports, and what a reader derives.
  select pg_catalog.array_agg((component.value->>'component_id')::uuid
           order by component.value->>'component_id')
    into v_whole
  from pg_catalog.jsonb_array_elements(
    private.weekly_source_effective_inventory_v1(v_root_a)->'components')
    as component(value);
  perform pg_temp.assert_true(pg_catalog.cardinality(v_whole)=2,
    'the whole move must carry both of the old root''s components');

  -- Recorded with the SECOND Office user authorising the brand-new target root
  -- while the FIRST takes the decision.
  v_request:=private.weekly_source_entitlement_proposal_cross_contract_request_v1(
    v_root_a,v_root_b,'f1000000-0000-4000-8000-000000000001',
    v_bundle_id,1::bigint,v_head_a,v_head_b,v_decision_id,v_whole,
    'LOCKED_FINAL_SOURCE','LOCKED_FINAL_SOURCE','UNCHANGED','OFFICE_SELECTED',
    'd1000000-0000-4000-8000-000000000002',null,null);
  perform pg_temp.assert_true(
    v_request#>>'{control,target_root_authorisation,actor_user_id}'
      ='d1000000-0000-4000-8000-000000000002',
    'the composer did not carry the target root authorisation actor');

  -- PINNED, because it is the thing most easily got wrong: `control` is OUTSIDE
  -- the request digest (I-3 section 0).  Composing the same move with a
  -- DIFFERENT authorising actor gives the SAME digest, so neither that actor nor
  -- `control.moved_component_ids` can ever be the reason a decision fails to
  -- rebuild.  Only the financial position can.
  perform pg_temp.assert_true(
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(v_request,'IMMEDIATE',null))
    =private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(
        private.weekly_source_entitlement_proposal_cross_contract_request_v1(
          v_root_a,v_root_b,'f1000000-0000-4000-8000-000000000001',
          v_bundle_id,1::bigint,v_head_a,v_head_b,v_decision_id,v_whole,
          'LOCKED_FINAL_SOURCE','LOCKED_FINAL_SOURCE','UNCHANGED','OFFICE_SELECTED',
          'd1000000-0000-4000-8000-000000000001',null,null),
        'IMMEDIATE',null)),
    'the target root authorisation actor changed the request digest, so CONTROL '
    ||'scope is inside it after all and I-3 section 0 no longer holds');

  v_recorded:=private.weekly_source_entitlement_proposal_record_v1(
    v_request,'d0000000-0000-4000-8000-000000000001',
    'f4000000-0000-4000-8000-000000000001','2027-04-11',
    'd1000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_recorded->>'ok')::boolean,false)
    and coalesce((v_recorded->>'created')::boolean,false)
    and v_recorded->>'state'='PROPOSED',
    'the whole-move bundle was not recorded: '||v_recorded::text);

  -- As recorded, the decision IS rebuildable and the screen shows the proposed
  -- entitlement.  Proving that first is what stops the refusal below from
  -- passing for the wrong reason.
  perform pg_temp.assert_true((
    select (proposal.value->>'request_digest_verified')::boolean
       and proposal.value->>'state'='PROPOSED_CROSS_CONTRACT'
      from (select public.weekly_source_office_timesheet_presentation_v1(
              pg_catalog.jsonb_build_object(
                'actor_user_id','d1000000-0000-4000-8000-000000000001',
                'timesheet_id',v_root_a))->'proposal') as proposal(value)),
    'the whole-move bundle did not display before the position was moved on, so '
    ||'the refusal proved afterwards would be proving nothing');

  -- Now the POSITION moves on, while the accepted decision stays exactly as it
  -- was.  The new Contract root is authorised in its own right and acquires its
  -- own entitlement AFTER the cross-Contract decision was accepted -- an
  -- ordinary race between two Office acts on the same Candidate week.  The
  -- decision is now stale: it was taken against a new Contract holding nothing,
  -- and that Contract now holds something.
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
    contract_choice_digest,before_inventory_digest,proposed_head_ids,state,committed_at_utc
  ) values (
    'e6000000-0000-4000-8000-0000000000b4',1,'d0000000-0000-4000-8000-000000000001',
    'f3000000-0000-4000-8000-000000000001','2027-04-11','SINGLE_ROOT','G9-PM2',
    'fa000000-0000-4000-8000-0000000000b2','f4000000-0000-4000-8000-000000000005',
    'e7000000-0000-4000-8000-0000000000b4','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
    decode(repeat('d1',32),'hex'),decode(repeat('d2',32),'hex'),decode(repeat('d3',32),'hex'),
    decode(repeat('d4',32),'hex'),array['e8000000-0000-4000-8000-0000000000b4']::uuid[],
    'COMMITTED','2026-12-31 09:00:00+00');
  insert into public.weekly_source_root_authorisations(
    id,root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  ) values (
    'e5000000-0000-4000-8000-0000000000b2','fa000000-0000-4000-8000-0000000000b2','G9-PM2',1,1,
    'g9-pm2','d1000000-0000-4000-8000-000000000001');
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,root_timesheet_id,
    root_family_booking_id,root_timesheet_version,head_revision,prior_head_id,state,
    certified_zero,component_count,entitlement_digest,inventory_digest,source_generation_digest,
    publication_receipt_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id,
    scope_change_tx_token,committed_at_utc
  ) values (
    'e8000000-0000-4000-8000-0000000000b4','LOCKED_FINAL_SOURCE',
    'd0000000-0000-4000-8000-000000000001','f3000000-0000-4000-8000-000000000001',
    'f4000000-0000-4000-8000-000000000005','2027-04-11',
    'fa000000-0000-4000-8000-0000000000b2','G9-PM2',1,1,null,'COMMITTED_CURRENT',
    false,1,decode(repeat('d5',32),'hex'),decode(repeat('d6',32),'hex'),
    decode(repeat('d7',32),'hex'),decode(repeat('d8',32),'hex'),
    'e6000000-0000-4000-8000-0000000000b4',1,'e7000000-0000-4000-8000-0000000000b4',
    'd1000000-0000-4000-8000-000000000001','e9000000-0000-4000-8000-0000000000b4',
    '2026-12-31 09:00:00+00');
  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,economic_key_value,
    component_member_identity,segment_id,segment_key,work_date,hours_day,pay_ex_vat,
    charge_ex_vat,exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
  )
  select 'e8000000-0000-4000-8000-0000000000b4',1,
    private.weekly_source_entitlement_component_id_v1(
      'WORKED_TIME','SEGMENT','weekly-source-event:g9-pm-shift-c','g9-pm-shift-c'),
    'WORKED_TIME','SEGMENT','weekly-source-event:g9-pm-shift-c','g9-pm-shift-c',
    'weekly-source-event:g9-pm-shift-c','g9-pm-shift-c','2027-04-07'::date,
    4.000000,80.00,110.00,false,'WEEKLY_SOURCE',
    'e6000000-0000-4000-8000-0000000000b4',1,
    pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('pmc'),64,'0'),'hex');
  update public.weekly_source_root_authorisations
  set current_entitlement_head_id='e8000000-0000-4000-8000-0000000000b4'
  where root_timesheet_id='fa000000-0000-4000-8000-0000000000b2';

  -- The accepted decision is untouched, so the refusal below is about the
  -- position and nothing else.
  perform pg_temp.assert_true((
    select bundle_row.state='PROPOSED'
       and bundle_row.decided_by_user_id='d1000000-0000-4000-8000-000000000001'
       and bundle_row.bundle_kind='CROSS_CONTRACT_A_B'
      from public.weekly_source_entitlement_decision_bundles bundle_row
     where bundle_row.decision_bundle_id=v_bundle_id and bundle_row.bundle_revision=1),
    'the stale-position fixture disturbed the accepted decision instead of the '
    ||'position');
  -- The old Contract still holds exactly what it held: only the NEW Contract
  -- moved, which is what makes the accepted decision stale.
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(
      private.weekly_source_effective_inventory_v1(v_root_a)->'components')=2
    and pg_catalog.jsonb_array_length(
      private.weekly_source_effective_inventory_v1(v_root_b)->'components')=1,
    'the new Contract''s current entitlement did not move on');
end;
$gate9_partial$;

-- ---------------------------------------------------------------------------
-- The Candidate surface reads APPROVED hours from the committed entitlement
-- head, not from the source rows (WP-11d F10), so the two Candidate rows that
-- show approved hours need one.  The head's times are presented from the
-- resolved source row for the same durable work event, which is exactly how the
-- producer resolves them.
-- ---------------------------------------------------------------------------
insert into public.weekly_source_entitlement_decision_bundles(
  decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
  source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
  decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
  contract_choice_digest,before_inventory_digest,proposed_head_ids,state,committed_at_utc
)
select row_data.bundle_id,1,'d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001',timesheet_row.week_ending_date,'SINGLE_ROOT',
  timesheet_row.booking_id,row_data.timesheet_id,'f4000000-0000-4000-8000-000000000001',
  row_data.decision_id,'d1000000-0000-4000-8000-000000000001','IMMEDIATE',
  row_data.d1,row_data.d2,row_data.d3,row_data.d4,
  array[row_data.head_id]::uuid[],'COMMITTED','2026-12-30 10:00:00+00'
from (values
  ('e6000000-0000-4000-8000-000000000020'::uuid,'fa000000-0000-4000-8000-000000000014'::uuid,
   'e7000000-0000-4000-8000-000000000020'::uuid,'e8000000-0000-4000-8000-000000000020'::uuid,
   decode(repeat('20',32),'hex'),decode(repeat('21',32),'hex'),
   decode(repeat('22',32),'hex'),decode(repeat('23',32),'hex')),
  ('e6000000-0000-4000-8000-000000000021'::uuid,'fa000000-0000-4000-8000-000000000015',
   'e7000000-0000-4000-8000-000000000021','e8000000-0000-4000-8000-000000000021',
   decode(repeat('24',32),'hex'),decode(repeat('25',32),'hex'),
   decode(repeat('26',32),'hex'),decode(repeat('27',32),'hex'))
) as row_data(bundle_id,timesheet_id,decision_id,head_id,d1,d2,d3,d4)
join public.timesheets timesheet_row on timesheet_row.timesheet_id=row_data.timesheet_id;

insert into public.weekly_source_entitlement_heads(
  id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,root_timesheet_id,
  root_family_booking_id,root_timesheet_version,head_revision,prior_head_id,state,
  certified_zero,component_count,entitlement_digest,inventory_digest,source_generation_digest,
  publication_receipt_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id,
  scope_change_tx_token,committed_at_utc
)
select row_data.head_id,'LOCKED_FINAL_SOURCE','d0000000-0000-4000-8000-000000000001',
  'f3000000-0000-4000-8000-000000000001','f4000000-0000-4000-8000-000000000001',
  timesheet_row.week_ending_date,row_data.timesheet_id,timesheet_row.booking_id,
  timesheet_row.version,1,null,'COMMITTED_CURRENT',false,1,
  row_data.d1,row_data.d2,row_data.d3,row_data.d4,
  row_data.bundle_id,1,row_data.decision_id,'d1000000-0000-4000-8000-000000000001',
  row_data.token,'2026-12-30 10:00:00+00'
from (values
  ('e8000000-0000-4000-8000-000000000020'::uuid,'fa000000-0000-4000-8000-000000000014'::uuid,
   'e6000000-0000-4000-8000-000000000020'::uuid,'e7000000-0000-4000-8000-000000000020'::uuid,
   'e9000000-0000-4000-8000-000000000020'::uuid,
   decode(repeat('28',32),'hex'),decode(repeat('29',32),'hex'),
   decode(repeat('2a',32),'hex'),decode(repeat('2b',32),'hex')),
  ('e8000000-0000-4000-8000-000000000021'::uuid,'fa000000-0000-4000-8000-000000000015',
   'e6000000-0000-4000-8000-000000000021','e7000000-0000-4000-8000-000000000021',
   'e9000000-0000-4000-8000-000000000021',
   decode(repeat('2c',32),'hex'),decode(repeat('2d',32),'hex'),
   decode(repeat('2e',32),'hex'),decode(repeat('2f',32),'hex'))
) as row_data(head_id,timesheet_id,bundle_id,decision_id,token,d1,d2,d3,d4)
join public.timesheets timesheet_row on timesheet_row.timesheet_id=row_data.timesheet_id;

insert into public.weekly_source_entitlement_head_components(
  head_id,component_ordinal,component_id,component_kind,economic_key_type,economic_key_value,
  component_member_identity,segment_id,segment_key,work_date,hours_day,pay_ex_vat,
  charge_ex_vat,exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256
)
select row_data.head_id,1,
  private.weekly_source_entitlement_component_id_v1(
    'WORKED_TIME','SEGMENT','weekly-source-event:'||row_data.event_id::text,
    row_data.event_id::text),
  'WORKED_TIME','SEGMENT','weekly-source-event:'||row_data.event_id::text,
  row_data.event_id::text,'weekly-source-event:'||row_data.event_id::text,
  row_data.event_id::text,row_data.work_date,
  7.500000,150.00,200.00,false,'WEEKLY_SOURCE',
  row_data.bundle_id,1,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('cand'||row_data.head_id::text),64,'0'),'hex')
from (values
  ('e8000000-0000-4000-8000-000000000020'::uuid,'f9000000-0000-4000-8000-000000000014'::uuid,
   '2026-12-08'::date,'e6000000-0000-4000-8000-000000000020'::uuid),
  ('e8000000-0000-4000-8000-000000000021'::uuid,'f9000000-0000-4000-8000-000000000015',
   '2026-12-15','e6000000-0000-4000-8000-000000000021')
) as row_data(head_id,event_id,work_date,bundle_id);

update public.weekly_source_root_authorisations
set current_entitlement_head_id='e8000000-0000-4000-8000-000000000020'
where root_timesheet_id='fa000000-0000-4000-8000-000000000014';
update public.weekly_source_root_authorisations
set current_entitlement_head_id='e8000000-0000-4000-8000-000000000021'
where root_timesheet_id='fa000000-0000-4000-8000-000000000015';


-- ===========================================================================
-- G9-V2.  All 22 rows, read back OUT OF THE PROJECTION for a database state
-- that genuinely reaches each one.  Nothing here seeds a projection result.
-- ===========================================================================
do $gate9_rows$
declare
  v_case record;
  v_result jsonb;
  v_lifecycle jsonb;
  v_expected_heading text;
  v_covered text[]:='{}'::text[];
begin
  for v_case in
    select * from (values
      -- (expected ui_state, Timesheet, surface, expected overlay)
      ('UI-001','fa000000-0000-4000-8000-000000000001'::uuid,'OFFICE',null::text),
      ('UI-002','fa000000-0000-4000-8000-000000000002'::uuid,'OFFICE',null),
      ('UI-003','fa000000-0000-4000-8000-000000000003'::uuid,'OFFICE',null),
      ('UI-004','fa000000-0000-4000-8000-000000000004'::uuid,'OFFICE',null),
      ('UI-005','fa000000-0000-4000-8000-000000000005'::uuid,'OFFICE',null),
      ('UI-006','fa000000-0000-4000-8000-000000000006'::uuid,'OFFICE',null),
      ('UI-007','fa000000-0000-4000-8000-000000000007'::uuid,'OFFICE',null),
      ('UI-008','fa000000-0000-4000-8000-000000000008'::uuid,'OFFICE',null),
      ('UI-009','fa000000-0000-4000-8000-000000000009'::uuid,'OFFICE',null),
      ('UI-010','fa000000-0000-4000-8000-00000000000a'::uuid,'OFFICE',null),
      ('UI-011','fa000000-0000-4000-8000-00000000000b'::uuid,'OFFICE',null),
      ('UI-012','fa000000-0000-4000-8000-00000000000c'::uuid,'OFFICE',null),
      ('UI-013','fa000000-0000-4000-8000-00000000000d'::uuid,'OFFICE',null),
      ('UI-014','fa000000-0000-4000-8000-00000000000e'::uuid,'OFFICE',null),
      ('UI-015','fa000000-0000-4000-8000-00000000000f'::uuid,'OFFICE',null),
      ('UI-016','fa000000-0000-4000-8000-000000000010'::uuid,'OFFICE',null),
      ('UI-017','fa000000-0000-4000-8000-000000000011'::uuid,'OFFICE',null),
      -- UI-018 is an OVERLAY on the phase the week is really in.
      ('UI-001','fa000000-0000-4000-8000-000000000012'::uuid,'OFFICE','UI-018'),
      ('UI-022','fa000000-0000-4000-8000-000000000013'::uuid,'OFFICE',null),
      ('UI-019','fa000000-0000-4000-8000-000000000017'::uuid,'CANDIDATE',null),
      ('UI-020','fa000000-0000-4000-8000-000000000014'::uuid,'CANDIDATE',null),
      ('UI-021','fa000000-0000-4000-8000-000000000015'::uuid,'CANDIDATE',null)
    ) as t(expected_ui_state,timesheet_id,surface,expected_overlay)
  loop
    v_result:=public.weekly_source_office_timesheet_presentation_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id','d1000000-0000-4000-8000-000000000001',
        'timesheet_id',v_case.timesheet_id));
    v_lifecycle:=case when v_case.surface='CANDIDATE'
      then v_result->'candidate_lifecycle' else v_result->'lifecycle' end;

    perform pg_temp.assert_true(v_lifecycle is not null,
      v_case.expected_ui_state||': the projection returned no lifecycle object');
    perform pg_temp.assert_true(coalesce((v_lifecycle->>'ok')::boolean,false),
      v_case.expected_ui_state||': the lifecycle is not ok: '||coalesce(v_lifecycle->>'errors','<null>'));
    perform pg_temp.assert_true(v_lifecycle->>'ui_state'=v_case.expected_ui_state,
      v_case.expected_ui_state||': the server resolved '||coalesce(v_lifecycle->>'ui_state','<null>')
      ||' for '||v_case.timesheet_id::text);

    -- The heading is the lifecycle policy's own text, and the policy text was
    -- pinned against the matrix in G9-V1 before any of this ran.
    select policy_row.value->>'heading' into v_expected_heading
    from pg_catalog.jsonb_array_elements(
      private.weekly_source_office_lifecycle_policy_v1()->'rows') as policy_row(value)
    where policy_row.value->>'ui_state'=v_case.expected_ui_state;
    perform pg_temp.assert_true(
      coalesce(v_lifecycle->>'heading','<none>')=coalesce(v_expected_heading,'<none>'),
      v_case.expected_ui_state||': heading was '||coalesce(v_lifecycle->>'heading','<none>'));
    perform pg_temp.assert_true(
      v_lifecycle->'permitted_actions' is not null
      and pg_catalog.jsonb_typeof(v_lifecycle->'permitted_actions')='array',
      v_case.expected_ui_state||': permitted actions are not server-owned');

    if v_case.expected_overlay is not null then
      perform pg_temp.assert_true(
        v_lifecycle->'overlay_states' @> pg_catalog.to_jsonb(array[v_case.expected_overlay]),
        v_case.expected_ui_state||': overlay '||v_case.expected_overlay||' is absent');
      v_covered:=v_covered||v_case.expected_overlay;
    end if;
    v_covered:=v_covered||v_case.expected_ui_state;
  end loop;

  perform pg_temp.assert_true(
    (select pg_catalog.count(distinct element)::integer
     from pg_catalog.unnest(v_covered) as element)=22,
    'the 22 matrix rows were not all produced; produced '||
    (select pg_catalog.count(distinct element)::text
     from pg_catalog.unnest(v_covered) as element));
end;
$gate9_rows$;

-- ===========================================================================
-- G9-V3.  The named per-row facts the matrix requires, beyond the heading.
-- ===========================================================================
do $gate9_facts$
declare
  v jsonb;
  l jsonb;
  s jsonb;
begin
  -- UI-016 and UI-017: the Weekly Source component must not mount and this
  -- owner supplies no heading at all.
  foreach v in array array[
    public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','d1000000-0000-4000-8000-000000000001',
      'timesheet_id','fa000000-0000-4000-8000-000000000010')),
    public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','d1000000-0000-4000-8000-000000000001',
      'timesheet_id','fa000000-0000-4000-8000-000000000011'))
  ]::jsonb[] loop
    perform pg_temp.assert_true((v->>'applicable')::boolean is false,
      'a bypass row claimed the Weekly Source component applies');
    perform pg_temp.assert_true(v#>'{lifecycle,heading}'='null'::jsonb
      and v#>>'{lifecycle,heading_source}'='LEGACY_OWNER',
      'a bypass row was given a Weekly Source heading');
  end loop;

  -- UI-007: Hours paid comes ONLY from the immutable settlement allocation.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-000000000007'));
  s:=v#>'{lifecycle,schedules,paid_to_date}';
  perform pg_temp.assert_true((s->>'available')::boolean
    and s->>'source'='WEEKLY_SOURCE_SETTLEMENT_ALLOCATION'
    and (s->>'row_count')::integer=1
    and (s->>'total_hours')::numeric=7.5,
    'UI-007 did not take Hours paid from the settlement allocation');
  perform pg_temp.assert_true(
    v#>'{lifecycle,schedules,current_paid}'=v#>'{lifecycle,schedules,paid_to_date}',
    'Hours paid to date and current paid hours are not the one immutable fact');
  perform pg_temp.assert_true(
    (v#>>'{lifecycle,schedules,hours_to_authorise,available}')::boolean is false
    and v#>>'{lifecycle,schedules,hours_to_authorise,reason}'='NOT_IN_A_FIRST_AUTHORISATION_PHASE',
    'a paid week still offered an hours-to-authorise schedule');

  -- UI-012: the cumulative allocation spans BOTH settled batches.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-00000000000c'));
  s:=v#>'{lifecycle,schedules,current_paid}';
  -- The settlement-position gate WITHHOLDS the paid figure for a root that
  -- settled more than once.  The PHASE is still decidable -- two settled batches
  -- is a count of evidence rows, not a figure -- so UI-012 resolves with its own
  -- heading and the paid schedule says, by name, that the figure is withheld.
  --
  -- The assertion is on the CLASS, not on the reason literal.  Pinning the
  -- literal is the trap WP-11e walked into and out of: the reason this gate
  -- returns has already changed once (SETTLEMENT_POSITION_SEMANTICS_UNRULED ->
  -- SETTLEMENT_SEQUENCE_UNPROVABLE) and the superseded literal is retained only
  -- as a compatibility mapping.  A verifier pinned to the dying literal would
  -- keep passing on a stale build and start failing on a correct one, which is
  -- the worst shape this kind of assertion can take.  So: the reason must be
  -- present, it must be one the READER ITSELF classes as a withheld position,
  -- and it must carry a sentence an Office user can read.
  perform pg_temp.assert_true((s->>'available')::boolean is false
    and s->'rows'='[]'::jsonb
    and s->>'unavailable_class'='POSITION_WITHHELD'
    and pg_catalog.char_length(coalesce(s->>'reason',''))>0
    and private.weekly_source_settlement_reason_class_v1(s->>'reason')
          ->>'unavailable_class'='POSITION_WITHHELD'
    and pg_catalog.char_length(coalesce(s->>'reason_detail',''))>20,
    'UI-012 stated a paid figure the settlement-position gate withholds: '||s::text);
  perform pg_temp.assert_true((v#>>'{lifecycle,settlement,settlement_count}')::integer=2
    and (v#>>'{lifecycle,settlement,batch_count}')::integer=2,
    'UI-012 did not see two settlements over one root');
  perform pg_temp.assert_true(v#>'{lifecycle,errors}'='[]'::jsonb,
    'a WITHHELD paid position was reported as a damaged projection');

  -- UI-006: payment in flight, and NO paid figure is produced.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-000000000006'));
  perform pg_temp.assert_true(v#>>'{lifecycle,payment,state}'='IN_FLIGHT'
    and (v#>>'{lifecycle,payment,live_item_count}')::integer=1,
    'UI-006 did not see the live Banking Pay item');
  perform pg_temp.assert_true(
    (v#>>'{lifecycle,schedules,paid_to_date,available}')::boolean is false
    and v#>>'{lifecycle,schedules,paid_to_date,reason}'='NO_SETTLEMENT',
    'UI-006 produced a paid figure while payment was still in flight');
  perform pg_temp.assert_true(
    (v#>>'{lifecycle,schedules,processing,available}')::boolean,
    'UI-006 did not carry the schedule whose payment is being processed');

  -- UI-010: the complete proposed entitlement, beside the currently approved
  -- one, with the pending change and the decision payload.  Displayed ONLY
  -- because the stored request digest was reproduced.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-00000000000a'));
  l:=v->'proposal';
  perform pg_temp.assert_true((l->>'present')::boolean and l->>'state'='FROZEN_PENDING'
    and (l->>'request_digest_verified')::boolean,
    'UI-010 did not prove the saved decision against its stored digest');
  perform pg_temp.assert_true((l#>>'{proposed,available}')::boolean
    and (l#>>'{proposed,row_count}')::integer=1
    and l#>>'{proposed,source}'='WEEKLY_SOURCE_ENTITLEMENT_COMPOSER'
    and (l#>>'{proposed,rows,0,total_hours}')::numeric=8,
    'UI-010 did not show the complete proposed entitlement from the composer');
  perform pg_temp.assert_true((l#>>'{currently_approved,available}')::boolean,
    'UI-010 did not show the currently approved entitlement beside it');
  perform pg_temp.assert_true((l#>>'{change,added_count}')::integer=1
    and (l#>>'{change,removed_count}')::integer=0,
    'UI-010 did not state the pending change');
  perform pg_temp.assert_true(
    l#>>'{decision_reason,source_change,final_revision_id}'='f1000000-0000-4000-8000-000000000001'
    and l#>>'{decision_reason,source_change,reason}'='INITIAL_FINALISATION',
    'UI-010 did not state the decision reason');
  perform pg_temp.assert_true(
    l#>'{decision,actions}' @> '[{"action":"APPROVE_UPDATED_HOURS"}]'::jsonb
    and l#>'{decision,actions}' @> '[{"action":"KEEP_CURRENTLY_APPROVED_HOURS"}]'::jsonb
    and l#>>'{decision,command_payload,final_revision_id}'='f1000000-0000-4000-8000-000000000001',
    'UI-010 did not offer the two Office decisions with a server-resolved revision');
  perform pg_temp.assert_true(not (l#>'{proposed}')::text ~* ('(pay_ex_vat|charge_ex_vat|'||pg_catalog.chr(163)||')'),
    'the proposal leaked money into an hours schedule');

  -- UI-008: the COMPOSING path, positively.  The bundle was composed and
  -- recorded by WP-06's own owners, so the proposal is displayed only because
  -- the Gate 9 view resolved the source revision by digest AND reproduced the
  -- request digest the recorder stored.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-000000000008'));
  l:=v->'proposal';
  perform pg_temp.assert_true((l->>'present')::boolean and l->>'state'='PROPOSED'
    and (l->>'request_digest_verified')::boolean,
    'UI-008 did not prove the composed proposal against its stored digest: '||l::text);
  perform pg_temp.assert_true(
    l->>'final_revision_id'='f1000000-0000-4000-8000-000000000001',
    'UI-008 did not resolve the source revision by digest');
  perform pg_temp.assert_true((l#>>'{proposed,available}')::boolean
    and (l#>>'{proposed,row_count}')::integer=1
    and l#>>'{proposed,source}'='WEEKLY_SOURCE_ENTITLEMENT_COMPOSER'
    and (l#>>'{proposed,rows,0,total_hours}')::numeric=7.5,
    'UI-008 did not show the complete proposed entitlement from the composer');
  perform pg_temp.assert_true((l#>>'{currently_approved,available}')::boolean
    and (l#>>'{currently_approved,row_count}')::integer=1
    and l#>>'{currently_approved,source}'='EFFECTIVE_INVENTORY_HEAD'
    and (l#>>'{currently_approved,rows,0,total_hours}')::numeric=8,
    'UI-008 did not show the currently approved entitlement beside the proposal');
  -- The pending change is a real CHANGED row on one component identity, not an
  -- add-and-remove pair, because the composer and the head agree on identity.
  perform pg_temp.assert_true((l#>>'{change,changed_count}')::integer=1
    and (l#>>'{change,added_count}')::integer=0
    and (l#>>'{change,removed_count}')::integer=0
    and (l#>>'{change,changed,0,currently_approved_total_hours}')::numeric=8
    and (l#>>'{change,changed,0,proposed_total_hours}')::numeric=7.5,
    'UI-008 did not state the pending change as a change: '||(l->'change')::text);
  perform pg_temp.assert_true(
    l#>>'{decision_reason,source_change,final_revision_id}'='f1000000-0000-4000-8000-000000000001'
    and l#>>'{decision_reason,source_change,reason}'='INITIAL_FINALISATION'
    and l#>>'{decision_reason,current_position,authority}'='HEAD'
    and l#>>'{decision_reason,current_position,authority_kind}'='LOCKED_FINAL_SOURCE',
    'UI-008 did not state the decision reason on the composing path');
  perform pg_temp.assert_true(
    l#>'{decision,actions}' @> '[{"action":"APPROVE_UPDATED_HOURS"}]'::jsonb
    and l#>'{decision,actions}' @> '[{"action":"KEEP_CURRENTLY_APPROVED_HOURS"}]'::jsonb
    and l#>>'{decision,command_payload,root_timesheet_id}'='fa000000-0000-4000-8000-000000000008'
    and l#>>'{decision,command_payload,final_revision_id}'='f1000000-0000-4000-8000-000000000001'
    and (l#>>'{decision,command_payload,bundle_revision}')::bigint=1,
    'UI-008 did not offer a complete two-decision command payload');
  perform pg_temp.assert_true(not (l#>'{proposed}')::text ~* ('(pay_ex_vat|charge_ex_vat|'||pg_catalog.chr(163)||')'),
    'the composed proposal leaked money into an hours schedule');
  -- UI-009 is the same proposal on a SETTLED root, which is the only difference
  -- between the two rows.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-000000000009'));
  perform pg_temp.assert_true(v#>>'{proposal,state}'='PROPOSED'
    and (v#>>'{proposal,request_digest_verified}')::boolean
    and (v#>>'{lifecycle,schedules,paid_to_date,available}')::boolean,
    'UI-009 did not carry a proved proposal beside a settled allocation');

  -- The SAVED two-root A-to-B decision.  This is the positive proof of the
  -- whole cross-Contract machinery: a complete two-member I-3 request, read out
  -- of the one artefact that stores it, displayed ONLY because its digest was
  -- reproduced in the same mode and with the same pending id the installed
  -- save-pending owner used.  A keeps nothing, B receives the complete
  -- entitlement, and the component identity is the SAME on both sides of the
  -- move, which is what makes it a move rather than a delete and an add.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-0000000000a1'));
  l:=v->'proposal';
  perform pg_temp.assert_true(v#>>'{lifecycle,ui_state}'='UI-010',
    'a DECIDED and frozen A-to-B decision is UI-010, not UI-013: got '
    ||coalesce(v#>>'{lifecycle,ui_state}','<null>'));
  perform pg_temp.assert_true(l->>'state'='FROZEN_PENDING'
    and (l->>'request_digest_verified')::boolean
    and (l->>'member_count')::integer=2
    and (l->>'primary_root_ordinal')::integer=1,
    'the saved two-root decision was not proved against its stored digest: '||l::text);
  perform pg_temp.assert_true(
    l#>>'{cross_contract,source_root_timesheet_id}'='fa000000-0000-4000-8000-0000000000a1'
    and l#>>'{cross_contract,target_root_timesheet_id}'='fa000000-0000-4000-8000-0000000000a2'
    and l#>>'{cross_contract,source_contract_id}'='f4000000-0000-4000-8000-000000000001'
    and l#>>'{cross_contract,target_contract_id}'='f4000000-0000-4000-8000-000000000005'
    and l#>>'{cross_contract,atomic}'
        ='Both roots are decided by one action; neither publishes alone.',
    'the saved two-root decision did not state both roots and both Contracts');
  -- Member 1, the old root: keeps nothing, and its current entitlement is its
  -- committed head.
  perform pg_temp.assert_true((l#>>'{members,0,root_ordinal}')::integer=1
    and l#>>'{members,0,root_timesheet_id}'='fa000000-0000-4000-8000-0000000000a1'
    and (l#>>'{members,0,is_requested_root}')::boolean
    and (l#>>'{members,0,proposed,available}')::boolean
    and (l#>>'{members,0,proposed,row_count}')::integer=0
    and (l#>>'{members,0,proposed_certified_zero}')::boolean
    and (l#>>'{members,0,currently_approved,row_count}')::integer=1
    and l#>>'{members,0,currently_approved_authority}'='HEAD'
    and (l#>>'{members,0,currently_approved,rows,0,total_hours}')::numeric=9,
    'the A member of the saved decision is wrong: '||(l->'members'->0)::text);
  -- Member 2, the new root: receives the complete entitlement from WP-06's
  -- composer, and is provably blank today.
  perform pg_temp.assert_true((l#>>'{members,1,root_ordinal}')::integer=2
    and l#>>'{members,1,root_timesheet_id}'='fa000000-0000-4000-8000-0000000000a2'
    and (l#>>'{members,1,is_requested_root}')::boolean is false
    and (l#>>'{members,1,proposed,available}')::boolean
    and (l#>>'{members,1,proposed,row_count}')::integer=1
    and l#>>'{members,1,proposed,source}'='WEEKLY_SOURCE_ENTITLEMENT_COMPOSER'
    and (l#>>'{members,1,proposed,rows,0,total_hours}')::numeric=9
    and (l#>>'{members,1,currently_approved,row_count}')::integer=0
    and l#>>'{members,1,currently_approved_authority}'='TSFIN',
    'the B member of the saved decision is wrong: '||(l->'members'->1)::text);
  -- The SAME component identity on both sides of the move.
  perform pg_temp.assert_true(
    l#>>'{members,1,proposed,rows,0,component_id}'
    =(select component.component_id::text
        from public.weekly_source_entitlement_head_components component
       where component.head_id='e8000000-0000-4000-8000-0000000000a0'),
    'the moved component changed identity between Contract A and Contract B');

  -- UI-013: an UNDECIDED cross-Contract A-to-B decision, now stated in full.
  -- The bundle was composed and recorded by WP-06's own two-root composer and
  -- recorder, so the proposal is displayed ONLY because this view rebuilt the
  -- two-root request from the bundle row and interface I-7 alone and reproduced
  -- the digest the recorder stored.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-00000000000d'));
  l:=v->'proposal';
  perform pg_temp.assert_true(v#>>'{lifecycle,ui_state}'='UI-013'
    and v#>>'{lifecycle,heading}'='Currently approved hours',
    'UI-013 did not resolve with its own heading');
  perform pg_temp.assert_true(l->>'bundle_kind'='CROSS_CONTRACT_A_B'
    and (l->>'present')::boolean
    and l->>'state'='PROPOSED_CROSS_CONTRACT'
    and (l->>'request_digest_verified')::boolean
    and (l->>'member_count')::integer=2,
    'UI-013 did not prove the two-root proposal against its stored digest: '||l::text);
  perform pg_temp.assert_true(
    l->>'final_revision_id'='f1000000-0000-4000-8000-000000000001',
    'UI-013 did not resolve the source revision by digest');
  -- The entitlement LEAVES Contract A and arrives COMPLETE at Contract B.
  perform pg_temp.assert_true(
    l#>>'{members,0,root_timesheet_id}'='fa000000-0000-4000-8000-00000000000d'
    and l#>>'{members,0,contract_id}'='f4000000-0000-4000-8000-000000000001'
    and (l#>>'{members,0,is_requested_root}')::boolean
    and (l#>>'{members,0,proposed,available}')::boolean
    and (l#>>'{members,0,proposed,row_count}')::integer=0
    and (l#>>'{members,0,proposed_certified_zero}')::boolean
    and (l#>>'{members,0,currently_approved,row_count}')::integer=1
    and l#>>'{members,0,currently_approved_authority}'='HEAD'
    and (l#>>'{members,0,currently_approved,rows,0,total_hours}')::numeric=11,
    'UI-013: the old root did not give up its complete entitlement: '
    ||(l->'members'->0)::text);
  perform pg_temp.assert_true(
    l#>>'{members,1,root_timesheet_id}'='fa000000-0000-4000-8000-000000000016'
    and l#>>'{members,1,contract_id}'='f4000000-0000-4000-8000-000000000005'
    and (l#>>'{members,1,is_requested_root}')::boolean is false
    and (l#>>'{members,1,proposed,available}')::boolean
    and (l#>>'{members,1,proposed,row_count}')::integer=1
    and l#>>'{members,1,proposed,source}'='WEEKLY_SOURCE_ENTITLEMENT_COMPOSER'
    and (l#>>'{members,1,proposed,rows,0,total_hours}')::numeric=11
    and (l#>>'{members,1,currently_approved,available}')::boolean
    and (l#>>'{members,1,currently_approved,row_count}')::integer=0
    and l#>>'{members,1,currently_approved_authority}'='TSFIN',
    'UI-013: the new root did not receive the complete entitlement: '
    ||(l->'members'->1)::text);
  -- The moved component keeps the SAME identifier on BOTH sides of the move,
  -- which is what makes it a move rather than a delete and an add
  -- (`24 section 4.5` step 2; interface I-7 guarantee 1).
  perform pg_temp.assert_true(
    l#>>'{members,1,proposed,rows,0,component_id}'
    =l#>>'{members,0,currently_approved,rows,0,component_id}',
    'UI-013: the moved component changed identity between Contract A and Contract B');
  perform pg_temp.assert_true(
    l#>>'{cross_contract,source_contract_id}'='f4000000-0000-4000-8000-000000000001'
    and l#>>'{cross_contract,target_contract_id}'='f4000000-0000-4000-8000-000000000005'
    and l#>>'{cross_contract,atomic}'
        ='Both roots are decided by one action; neither publishes alone.',
    'UI-013 did not state both Contracts and the atomic rule');
  -- The decision value is now non-null, so the Office buttons appear on their
  -- own: the frontend renders only from this value and the wire shape is
  -- unchanged.
  perform pg_temp.assert_true(l->'decision'<>'null'::jsonb
    and l#>'{decision,actions}' @> '[{"action":"APPROVE_UPDATED_HOURS"}]'::jsonb
    and l#>'{decision,actions}' @> '[{"action":"KEEP_CURRENTLY_APPROVED_HOURS"}]'::jsonb
    and l#>>'{decision,command_payload,root_timesheet_id}'='fa000000-0000-4000-8000-00000000000d'
    and l#>>'{decision,command_payload,final_revision_id}'='f1000000-0000-4000-8000-000000000001',
    'UI-013 did not offer the two Office decisions: '||(l->'decision')::text);
  perform pg_temp.assert_true(
    v#>'{lifecycle,permitted_actions}' @> '["APPROVE_UPDATED_HOURS"]'::jsonb
    and v#>'{lifecycle,permitted_actions}' @> '["KEEP_CURRENTLY_APPROVED_HOURS"]'::jsonb,
    'UI-013 did not permit the two Office decisions');
  perform pg_temp.assert_true(
    not (l#>'{members}')::text ~* ('(pay_ex_vat|charge_ex_vat|'||pg_catalog.chr(163)||')'),
    'UI-013 leaked money into an hours schedule');

  -- A cross-Contract bundle whose request cannot be rebuilt: the proposed half
  -- is refused BY NAME on the screen, and ONLY the proposed half.  Everything
  -- that IS statable is still stated.  (The old root here was also the subject
  -- of the partial-move refusal above; this bundle is the WHOLE move that was
  -- accepted afterwards, and it became unrebuildable when the NEW Contract root
  -- was authorised in its own right and acquired its own entitlement -- the
  -- decision is stale, and the verifier proved the same bundle DID display
  -- before that happened.)
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-0000000000b1'));
  l:=v->'proposal';
  perform pg_temp.assert_true(v#>>'{lifecycle,ui_state}'='UI-013'
    and v#>>'{lifecycle,heading}'='Currently approved hours'
    and (v#>>'{lifecycle,ok}')::boolean
    and v#>'{lifecycle,errors}'='[]'::jsonb,
    'an unrebuildable cross-Contract proposal turned the whole row into an error');
  perform pg_temp.assert_true(l->>'state'='UNAVAILABLE'
    and l->>'reason'='PROPOSAL_CROSS_CONTRACT_MOVE_SET_NOT_RECOVERABLE'
    and l->'decision'='null'::jsonb
    and (l#>>'{members,0,proposed,available}')::boolean is false
    and l#>>'{members,0,proposed,reason}'='PROPOSAL_CROSS_CONTRACT_MOVE_SET_NOT_RECOVERABLE'
    and (l#>>'{members,1,proposed,available}')::boolean is false,
    'an unrebuildable proposal did not fail closed by name: '||l::text);
  -- The reason is readable, and it no longer blames a PARTIAL move: a partial
  -- move cannot reach this screen, because it is refused when it is composed.
  perform pg_temp.assert_true(
    pg_catalog.char_length(coalesce(l->>'detail',''))>80
    and l->>'detail' ~* 'refused when the decision is composed'
    and l->>'detail' ~* 'stale'
    and l->>'detail' !~* 'either this is a PARTIAL move',
    'the refusal did not say why, so the screen would read as a loading failure: '
    ||coalesce(l->>'detail','<none>'));
  -- ...and the bundle summary is still stated, because it is statable.
  perform pg_temp.assert_true((l->>'member_count')::integer=2
    and (l#>>'{members,0,currently_approved,available}')::boolean
    and (l#>>'{members,0,currently_approved,row_count}')::integer=2
    and (l#>>'{members,1,currently_approved,available}')::boolean
    and (l#>>'{members,1,currently_approved,row_count}')::integer=1,
    'an unrebuildable proposal lost the old-root and new-root bundle summary');
  -- Only the proposed half is withheld.  Nothing else on the screen is: the
  -- old Contract's currently approved position is the one it holds NOW, stated
  -- in full, which is exactly what the Office user needs in order to see that
  -- the decision has gone stale.
  perform pg_temp.assert_true(
    (v#>>'{lifecycle,schedules,currently_approved,available}')::boolean
    and (v#>>'{lifecycle,schedules,currently_approved,row_count}')::integer=2
    and v#>'{lifecycle,schedules,currently_approved,reason}'='null'::jsonb,
    'an unrebuildable proposal withheld more of the screen than the proposed half');

  -- UI-022: DEC-061 Option A.  The invoice movement history is SEPARATE and is
  -- labelled as never being Candidate paid hours.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-000000000013'));
  perform pg_temp.assert_true(v#>>'{lifecycle,authorisation_state}'='WITHDRAWN',
    'UI-022 was not read as a withdrawn first authorisation');
  perform pg_temp.assert_true(
    (v#>>'{invoice_movement_history,invoiced_from_source}')::boolean
    and (v#>>'{invoice_movement_history,bound_line_count}')::integer=1,
    'UI-022 did not prove the finalised source movement is still on the self-bill');
  perform pg_temp.assert_true(v->'invoice_movement_history' is not null
    and v#>'{lifecycle,schedules,paid_to_date,rows}'='[]'::jsonb,
    'UI-022 mixed invoice movements into a pay schedule');
  perform pg_temp.assert_true(
    v#>>'{invoice_movement_history,movements,0,note}'='Invoice movement history. Never Candidate paid hours.',
    'the invoice movement history is not labelled');

  -- UI-021: the Candidate surface, hours only.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-000000000015'));
  perform pg_temp.assert_true((v#>>'{candidate_lifecycle,submitted_row_count}')::integer=0
    and (v#>>'{candidate_lifecycle,approved_hours_row_count}')::integer>0,
    'UI-021 is not a no-submission week with approved hours');
  -- UI-019 and UI-020 differ by exactly the server-owned difference flag.
  perform pg_temp.assert_true(
    (public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','d1000000-0000-4000-8000-000000000001',
      'timesheet_id','fa000000-0000-4000-8000-000000000017'))
      #>>'{candidate_lifecycle,approved_hours_differ}')::boolean is false
    and (public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','d1000000-0000-4000-8000-000000000001',
      'timesheet_id','fa000000-0000-4000-8000-000000000014'))
      #>>'{candidate_lifecycle,approved_hours_differ}')::boolean,
    'the Candidate surface did not separate UI-019 from UI-020 on the server');
end;
$gate9_facts$;

-- ===========================================================================
-- G9-V4.  Fail closed.  A malformed or contradictory projection returns an
-- explicit error state with a reason, and never a phase.
--
-- Each contradiction gets its OWN family, because the Weekly Source immutable
-- record guard forbids deleting an authorisation record, and because a proof
-- that has to tidy up after itself is a proof whose order matters.
-- ===========================================================================
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json,
  is_current,version,authorised_at_server
)
select row_data.timesheet_id,row_data.booking_id,'g9-occupant','g9-hospital','g9-ward','nurse',
  (row_data.work_date||' 08:00:00+00')::timestamptz,(row_data.work_date||' 17:00:00+00')::timestamptz,
  30,510,row_data.week_ending,'g9/fc.png',repeat('7',64),
  'f4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
  ('[{"date":"'||row_data.work_date||'","start":"09:00","end":"17:00","break_minutes":30}]')::jsonb,
  row_data.is_current,row_data.version,'2026-12-30 09:00:00+00'
from (values
  -- two live authorisation generations on ONE family
  ('fa000000-0000-4000-8000-0000000000f1'::uuid,'G9-FC1','2027-01-04'::date,'2027-01-10'::date,true,1),
  ('fa000000-0000-4000-8000-0000000000f2'::uuid,'G9-FC1','2027-01-04','2027-01-10',false,2),
  -- settlement evidence whose signature does not match its snapshot
  ('fa000000-0000-4000-8000-0000000000f3'::uuid,'G9-FC2','2027-01-11','2027-01-17',true,1),
  -- settled money on a root that was never authorised
  ('fa000000-0000-4000-8000-0000000000f4'::uuid,'G9-FC3','2027-01-18','2027-01-24',true,1),
  -- two DIFFERENT undecided decisions on one root
  ('fa000000-0000-4000-8000-0000000000f5'::uuid,'G9-FC4','2027-01-25','2027-01-31',true,1)
) as row_data(timesheet_id,booking_id,work_date,week_ending,is_current,version);

insert into public.weekly_timesheet_source_comparisons(
  id,source_cycle_id,upload_id,projection_publication_id,timesheet_id,timesheet_revision,
  work_event_id,contract_id,work_date,comparison_state,candidate_break_minutes,
  total_break_minutes_match,comparison_fingerprint
)
select row_data.comparison_id,'f6000000-0000-4000-8000-000000000001',
  'f7000000-0000-4000-8000-000000000001','f8000000-0000-4000-8000-000000000001',
  row_data.timesheet_id,1,'f9000000-0000-4000-8000-000000000001',
  'f4000000-0000-4000-8000-000000000001',row_data.work_date,'SOURCE_SHIFT_MISSING',30,true,
  pg_catalog.decode(pg_catalog.lpad(pg_catalog.md5('fc'||row_data.comparison_id::text),64,'0'),'hex')
from (values
  ('fd000000-0000-4000-8000-0000000000f1'::uuid,'fa000000-0000-4000-8000-0000000000f1'::uuid,'2027-01-04'::date),
  ('fd000000-0000-4000-8000-0000000000f3'::uuid,'fa000000-0000-4000-8000-0000000000f3','2027-01-11'),
  ('fd000000-0000-4000-8000-0000000000f4'::uuid,'fa000000-0000-4000-8000-0000000000f4','2027-01-18'),
  ('fd000000-0000-4000-8000-0000000000f5'::uuid,'fa000000-0000-4000-8000-0000000000f5','2027-01-25')
) as row_data(comparison_id,timesheet_id,work_date);

insert into public.weekly_source_root_authorisations(
  id,root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
  authorised_row_signature,authorised_by_user_id
) values
  ('e5000000-0000-4000-8000-0000000000f1','fa000000-0000-4000-8000-0000000000f1','G9-FC1',1,1,
   'g9-fc1-a','d1000000-0000-4000-8000-000000000001'),
  ('e5000000-0000-4000-8000-0000000000f2','fa000000-0000-4000-8000-0000000000f2','G9-FC1',2,1,
   'g9-fc1-b','d1000000-0000-4000-8000-000000000001'),
  ('e5000000-0000-4000-8000-0000000000f3','fa000000-0000-4000-8000-0000000000f3','G9-FC2',1,1,
   'g9-fc2','d1000000-0000-4000-8000-000000000001'),
  ('e5000000-0000-4000-8000-0000000000f5','fa000000-0000-4000-8000-0000000000f5','G9-FC4',1,1,
   'g9-fc4','d1000000-0000-4000-8000-000000000001');

insert into public.pay_batches(
  id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
  rail_provider_snapshot,rail_env_snapshot,same_week_paye_override_used,
  execution_commit_state,execution_committed_at_utc,completed_at_utc
) values
  ('ec000000-0000-4000-8000-0000000000f3','2027-01-21','SETTLED','REVOLUT_CSV','CSV','CSV','PROD',
   false,'COMMITTED','2027-01-21 10:00:00+00','2027-01-21 11:00:00+00'),
  ('ec000000-0000-4000-8000-0000000000f4','2027-01-28','SETTLED','REVOLUT_CSV','CSV','CSV','PROD',
   false,'COMMITTED','2027-01-28 10:00:00+00','2027-01-28 11:00:00+00');
insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id,settlement_status,settled_at_utc)
values
  ('ed000000-0000-4000-8000-0000000000f3','ec000000-0000-4000-8000-0000000000f3',
   'f3000000-0000-4000-8000-000000000001','SETTLED','2027-01-21 11:00:00+00'),
  ('ed000000-0000-4000-8000-0000000000f4','ec000000-0000-4000-8000-0000000000f4',
   'f3000000-0000-4000-8000-000000000001','SETTLED','2027-01-28 11:00:00+00');
insert into public.pay_batch_timesheet_snapshots(
  id,pay_batch_id,timesheet_id,candidate_id,pay_channel,base_snapshot_json,target_snapshot_json,
  signature,created_at_utc
) values
  ('ef000000-0000-4000-8000-0000000000f3','ec000000-0000-4000-8000-0000000000f3',
   'fa000000-0000-4000-8000-0000000000f3','f3000000-0000-4000-8000-000000000001','PAYE',
   pg_catalog.jsonb_build_object('segments','[]'::jsonb),
   pg_catalog.jsonb_build_object('segments',pg_catalog.jsonb_build_array(
     pg_catalog.jsonb_build_object('segment_id','g9-seg-fc2','date','2027-01-11',
       'break_mins',30,'hours_day','7.5','hours_night','0','hours_sat','0',
       'hours_sun','0','hours_bh','0'))),
   'g9-sig-fc2','2027-01-21 11:00:00+00'),
  ('ef000000-0000-4000-8000-0000000000f4','ec000000-0000-4000-8000-0000000000f4',
   'fa000000-0000-4000-8000-0000000000f4','f3000000-0000-4000-8000-000000000001','PAYE',
   pg_catalog.jsonb_build_object('segments','[]'::jsonb),
   pg_catalog.jsonb_build_object('segments',pg_catalog.jsonb_build_array(
     pg_catalog.jsonb_build_object('segment_id','g9-seg-fc3','date','2027-01-18',
       'break_mins',30,'hours_day','7.5','hours_night','0','hours_sat','0',
       'hours_sun','0','hours_bh','0'))),
   'g9-sig-fc3','2027-01-28 11:00:00+00');
update public.pay_batch_timesheet_snapshots
set signature=pg_catalog.md5(target_snapshot_json::text)
where id in ('ef000000-0000-4000-8000-0000000000f3',
             'ef000000-0000-4000-8000-0000000000f4');

insert into public.timesheet_pay_state_history(
  id,timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
) values
  -- the signature deliberately does NOT match the snapshot's
  ('f0100000-0000-4000-8000-0000000000f3','fa000000-0000-4000-8000-0000000000f3',
   'ec000000-0000-4000-8000-0000000000f3','2027-01-21 11:00:00+00',
   (select target_snapshot_json from public.pay_batch_timesheet_snapshots
     where id='ef000000-0000-4000-8000-0000000000f3'),'g9-sig-fc2-TAMPERED'),
  ('f0100000-0000-4000-8000-0000000000f4','fa000000-0000-4000-8000-0000000000f4',
   'ec000000-0000-4000-8000-0000000000f4','2027-01-28 11:00:00+00',
   (select target_snapshot_json from public.pay_batch_timesheet_snapshots
     where id='ef000000-0000-4000-8000-0000000000f4'),
   (select signature from public.pay_batch_timesheet_snapshots
     where id='ef000000-0000-4000-8000-0000000000f4'));

insert into public.weekly_source_entitlement_decision_bundles(
  decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
  source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
  decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
  contract_choice_digest,before_inventory_digest,proposed_head_ids,state
) values
  ('e6000000-0000-4000-8000-0000000000f5',1,'d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','2027-01-31','SINGLE_ROOT','G9-FC4',
   'fa000000-0000-4000-8000-0000000000f5','f4000000-0000-4000-8000-000000000001',
   'e7000000-0000-4000-8000-0000000000f5','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
   decode(repeat('51',32),'hex'),decode(repeat('52',32),'hex'),decode(repeat('53',32),'hex'),
   decode(repeat('54',32),'hex'),array['e8000000-0000-4000-8000-0000000000f5']::uuid[],'PROPOSED'),
  ('e6000000-0000-4000-8000-0000000000f6',1,'d0000000-0000-4000-8000-000000000001',
   'f3000000-0000-4000-8000-000000000001','2027-01-31','SINGLE_ROOT','G9-FC4',
   'fa000000-0000-4000-8000-0000000000f5','f4000000-0000-4000-8000-000000000001',
   'e7000000-0000-4000-8000-0000000000f6','d1000000-0000-4000-8000-000000000001','IMMEDIATE',
   decode(repeat('55',32),'hex'),decode(repeat('56',32),'hex'),decode(repeat('57',32),'hex'),
   decode(repeat('58',32),'hex'),array['e8000000-0000-4000-8000-0000000000f6']::uuid[],'PROPOSED');

do $gate9_failclosed$
declare
  v jsonb;
  l jsonb;
begin
  -- (a) Two live authorisation generations on one family.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-0000000000f1'));
  l:=v->'lifecycle';
  perform pg_temp.assert_true((l->>'ok')::boolean is false
    and l->>'ui_state' is null and l->'heading'='null'::jsonb
    and l->'errors' @> '[{"code":"MULTIPLE_LIVE_ROOT_AUTHORISATIONS"}]'::jsonb,
    'two live authorisations on one family did not fail closed: '||l::text);
  perform pg_temp.assert_true(l->'permitted_actions'='[]'::jsonb,
    'a contradictory projection still offered an action');

  -- (b) Contradictory settlement evidence: a signature that does not match the
  -- snapshot's.  No paid figure is produced, not even a partial one.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-0000000000f3'));
  l:=v->'lifecycle';
  perform pg_temp.assert_true((l->>'ok')::boolean is false
    and l->'errors' @> '[{"code":"SETTLEMENT_ALLOCATION_UNAVAILABLE"}]'::jsonb
    and (l#>>'{settlement,reason}')='SETTLEMENT_SNAPSHOT_CONFLICT',
    'tampered settlement evidence did not fail closed: '||l::text);
  perform pg_temp.assert_true(
    (l#>>'{schedules,paid_to_date,available}') is null
      or (l#>>'{schedules,paid_to_date,available}')::boolean is false,
    'a paid figure survived contradictory settlement evidence');

  -- (c) Settled money on a root that was never authorised.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-0000000000f4'));
  perform pg_temp.assert_true((v#>>'{lifecycle,ok}')::boolean is false
    and v#>'{lifecycle,errors}' @> '[{"code":"SETTLEMENT_WITHOUT_AUTHORISATION"}]'::jsonb,
    'settlement without authorisation did not fail closed: '||(v->'lifecycle')::text);

  -- (d) Two DIFFERENT undecided decisions on one root.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-0000000000f5'));
  perform pg_temp.assert_true(v#>>'{proposal,state}'='UNAVAILABLE'
    and v#>>'{proposal,reason}'='MULTIPLE_PROPOSED_BUNDLES'
    and (v#>>'{proposal,present}')::boolean is false,
    'two undecided proposals on one root did not fail closed');

  -- (e) An undecided SINGLE-ROOT proposal whose stored request digest is not the
  -- digest of the request the server rebuilds.  Its source-revision digest IS
  -- genuine, so the revision resolves and the failure is unambiguously the
  -- request digest.  The proposal is shown as unavailable with that reason and
  -- NEVER as an unproved schedule, while the PHASE is still resolved because a
  -- proposal does exist.
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-0000000000f6'));
  perform pg_temp.assert_true(v#>>'{proposal,state}'='UNAVAILABLE'
    and (v#>>'{proposal,present}')::boolean
    and v#>>'{proposal,reason}'='PROPOSAL_REQUEST_DIGEST_MISMATCH'
    and (v#>>'{proposal,proposed,available}')::boolean is false
    and v#>'{proposal,decision}'='null'::jsonb,
    'a contradictory proposal was offered as a decision surface: '||(v->'proposal')::text);
  perform pg_temp.assert_true(v#>>'{lifecycle,ui_state}'='UI-008',
    'a contradictory proposal lost the phase as well as the content');
  -- The source revision IS resolved by digest, not by sort order.
  perform pg_temp.assert_true(
    v#>>'{proposal,final_revision_id}'='f1000000-0000-4000-8000-000000000001',
    'the proposal source revision was not identified by digest');
end;
$gate9_failclosed$;

-- ===========================================================================
-- G9-V5.  The browser is given no reason to infer anything, and the workspace
-- serves the whole server-owned vocabulary.
-- ===========================================================================
do $gate9_contract$
declare
  v jsonb;
  v_definition text;
begin
  v:=public.weekly_source_office_workspace_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','imports',
    'source_group_id','f5000000-0000-4000-8000-000000000001',
    'source_cycle_id','f6000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_true(
    pg_catalog.jsonb_array_length(v#>'{lifecycle_policy,rows}')=22
    and (v#>>'{lifecycle_policy,browser_may_infer_phase}')::boolean is false
    and v#>>'{lifecycle_policy,per_timesheet_owner}'
        ='public.weekly_source_office_timesheet_presentation_v1',
    'the workspace does not serve the complete lifecycle vocabulary');
  perform pg_temp.assert_true(v::text !~* 'hours being authorised',
    'the workspace carries the deleted heading Hours being authorised');

  -- The resolver never reads a Banking Pay cache or derives hours from money.
  select pg_catalog.pg_get_functiondef(
    'private.weekly_source_office_lifecycle_phase_v1(uuid,jsonb)'::pg_catalog.regprocedure)
    into v_definition;
  perform pg_temp.assert_true(v_definition !~* 'timesheet_pay_state[^_]'
    and v_definition !~* 'amount_ex_vat|amount_inc_vat|net_bank_amount|gross_preview',
    'the lifecycle resolver reads a money or last-settled-cache column');
  perform pg_temp.assert_true(v_definition !~* '(insert|update|delete)[[:space:]]+(into[[:space:]]+|from[[:space:]]+)?(public|private)\.',
    'the lifecycle resolver writes');

  select pg_catalog.pg_get_functiondef(
    'private.weekly_source_office_payment_progress_v1(uuid[])'::pg_catalog.regprocedure)
    into v_definition;
  perform pg_temp.assert_true(v_definition !~* '[[:space:]]not[[:space:]]+in[[:space:]]*\([[:space:]]*select',
    'the payment reader uses NOT IN over a subquery, which is null-unsafe');
  perform pg_temp.assert_true(v_definition ~* 'is not true',
    'the payment reader does not treat a null is_voided as not voided');

  -- Every private owner this package adds is owner-only.
  foreach v_definition in array array[
    'private.weekly_source_office_lifecycle_policy_v1()',
    'private.weekly_source_office_lifecycle_row_v1(text)',
    'private.weekly_source_office_lifecycle_result_v1(text,jsonb,jsonb,text)',
    'private.weekly_source_office_schedule_absent_v1(text)',
    'private.weekly_source_office_schedule_from_rows_v1(jsonb,text)',
    'private.weekly_source_office_schedule_from_components_v1(jsonb,text)',
    'private.weekly_source_office_schedule_from_allocation_v1(jsonb,text)',
    'private.weekly_source_office_schedule_from_actual_v1(jsonb)',
    'private.weekly_source_office_payment_progress_v1(uuid[])',
    'private.weekly_source_office_invoice_movements_v1(uuid[])',
    'private.weekly_source_office_proposal_revision_v1(uuid[],bytea)',
    'private.weekly_source_office_proposal_view_v1(uuid,uuid[])',
    'private.weekly_source_office_lifecycle_phase_v1(uuid,jsonb)',
    'private.weekly_source_office_candidate_phase_v1(uuid,timestamptz)'
  ]::text[] loop
    perform pg_temp.assert_true(
      (select proowner::regrole::text from pg_catalog.pg_proc
        where oid=pg_catalog.to_regprocedure(v_definition))='postgres',
      v_definition||' is not owned by postgres');
    perform pg_temp.assert_true(
      not pg_catalog.has_function_privilege('anon',v_definition,'EXECUTE')
      and not pg_catalog.has_function_privilege('authenticated',v_definition,'EXECUTE')
      and not pg_catalog.has_function_privilege('service_role',v_definition,'EXECUTE'),
      v_definition||' is executable by a non-owner role');
  end loop;
end;
$gate9_contract$;

-- ===========================================================================
-- G9-V6.  The settlement reader's unavailable contract, consumed rather than
-- re-derived, and the two rows that depend on it proved in BOTH states of the
-- fail-closed gate: withheld today, and narrowed the moment the finance
-- approver rules.  Neither state should need re-testing when the ruling lands.
-- ===========================================================================
do $gate9_unavailable_contract$
declare
  v jsonb;
  s jsonb;
  a jsonb;
begin
  -- The class is present and NULL on the two stated states, so a consumer can
  -- read it unconditionally (WP-11d D4).
  a:=private.weekly_source_settlement_allocation_v1('fa000000-0000-4000-8000-000000000007');
  perform pg_temp.assert_true(a->>'state'='AVAILABLE'
    and a ? 'unavailable_class' and a->'unavailable_class'='null'::jsonb,
    'the reader did not carry a null unavailable_class on AVAILABLE');
  a:=private.weekly_source_settlement_allocation_v1('fa000000-0000-4000-8000-000000000005');
  perform pg_temp.assert_true(a->>'state'='NO_SETTLEMENT'
    and a ? 'unavailable_class' and a->'unavailable_class'='null'::jsonb,
    'the reader did not carry a null unavailable_class on NO_SETTLEMENT');
  -- A week that has not been paid has NO paid figure; it does not have zero.
  perform pg_temp.assert_true(
    not (a ? 'total_hours') and not (a ? 'hours_by_bucket') and not (a ? 'shifts'),
    'NO_SETTLEMENT carried a numeric member, which reads as a paid zero');

  -- POSITION_WITHHELD: the phase resolves, the heading stands, and no numeric
  -- member is offered anywhere.
  a:=private.weekly_source_settlement_allocation_v1('fa000000-0000-4000-8000-00000000000c');
  -- Again: the CLASS is the contract, the reason literal is not.  What is
  -- asserted is that the reader's own classifier agrees with the class it
  -- published, so the two can never disagree whatever the literal becomes.
  perform pg_temp.assert_true(a->>'unavailable_class'='POSITION_WITHHELD'
    and private.weekly_source_settlement_reason_class_v1(a->>'reason')
          ->>'unavailable_class'='POSITION_WITHHELD'
    and pg_catalog.char_length(coalesce(a->>'reason_detail',''))>20,
    'the withheld position did not carry its class and plain-English detail');
  perform pg_temp.assert_true((a->>'settlement_count')::integer=2
    and (a->>'batch_count')::integer=2,
    'the withheld position did not carry the counts the phase is resolved from');

  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-00000000000c'));
  perform pg_temp.assert_true((v#>>'{lifecycle,ok}')::boolean
    and v#>'{lifecycle,errors}'='[]'::jsonb
    and v#>>'{lifecycle,ui_state}'='UI-012'
    and v#>>'{lifecycle,heading}'='Current paid hours',
    'a withheld position was treated as a damaged projection: '||(v->'lifecycle')::text);
  foreach s in array array[
    v#>'{lifecycle,schedules,paid_to_date}',
    v#>'{lifecycle,schedules,current_paid}'
  ]::jsonb[] loop
    perform pg_temp.assert_true((s->>'available')::boolean is false
      and s->>'unavailable_class'='POSITION_WITHHELD'
      and private.weekly_source_settlement_reason_class_v1(s->>'reason')
            ->>'unavailable_class'='POSITION_WITHHELD'
      and pg_catalog.char_length(coalesce(s->>'reason_detail',''))>20,
      'the withheld paid schedule did not carry the class and the detail');
    -- The counts are evidence, not a figure, and are the only numbers here.
    perform pg_temp.assert_true((s->>'settlement_count')::integer=2
      and (s->>'batch_count')::integer=2,
      'the withheld paid schedule lost the evidence counts');
    perform pg_temp.assert_true(
      not (s ? 'total_hours') and not (s ? 'hours_by_bucket')
      and s->'rows'='[]'::jsonb,
      'the withheld paid schedule offered a numeric member: '||s::text);
  end loop;

  -- EVIDENCE_DAMAGED stays a contradiction.
  a:=private.weekly_source_settlement_allocation_v1('fa000000-0000-4000-8000-0000000000f3');
  perform pg_temp.assert_true(a->>'unavailable_class'='EVIDENCE_DAMAGED'
    and a->>'reason'='SETTLEMENT_SNAPSHOT_CONFLICT'
    and pg_catalog.char_length(coalesce(a->>'reason_detail',''))>20,
    'damaged evidence did not carry its class and detail');
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-0000000000f3'));
  perform pg_temp.assert_true((v#>>'{lifecycle,ok}')::boolean is false
    and v#>>'{lifecycle,ui_state}' is null
    and v#>'{lifecycle,heading}'='null'::jsonb
    and v#>'{lifecycle,errors}' @> '[{"code":"SETTLEMENT_ALLOCATION_UNAVAILABLE"}]'::jsonb
    and v#>'{lifecycle,errors}' @> '[{"unavailable_class":"EVIDENCE_DAMAGED"}]'::jsonb,
    'damaged evidence stopped being a contradiction: '||(v->'lifecycle')::text);
  -- ...and the contradiction carries the reader's own sentence, not a generic one.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)::integer
       from pg_catalog.jsonb_array_elements(v#>'{lifecycle,errors}') as element(value)
      where element.value->>'code'='SETTLEMENT_ALLOCATION_UNAVAILABLE'
        and element.value->>'detail'
            =(private.weekly_source_settlement_allocation_v1(
                'fa000000-0000-4000-8000-0000000000f3')->>'reason_detail'))=1,
    'the contradiction did not carry the reader''s own plain-English detail');

  -- A single-settlement root is unaffected by the gate and states its figure,
  -- with the provenance of the settlement that restated it.
  s:=(public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-000000000007')))
    #>'{lifecycle,schedules,paid_to_date}';
  perform pg_temp.assert_true((s->>'available')::boolean
    and (s->>'total_hours')::numeric=7.5
    and s->>'position_basis'='LATEST_RESTATEMENT',
    'a single-settlement root did not state its restated figure');
end;
$gate9_unavailable_contract$;

-- ---------------------------------------------------------------------------
-- G9-V6a.  The SAME rows with the position gate OPENED.
--
-- This section is deliberately narrower than it used to be, and the reason is
-- worth stating because it is the same mistake twice over.
--
-- It used to perform "the two-line deletion WP-11d prescribed" and then assert
-- that the week read 3.0.  Both halves have since been overtaken:
--
--   * WP-11e says in terms that the two-line deletion is WRONG -- it leaves a
--     function with no RETURN and raises 2F005 on every multi-settlement root.
--     Opening the gate is a one-line substitution of `return null` for the final
--     return, TOGETHER WITH re-pointing the tie test and the reader's position
--     pick at an installed settlement sequence column.  No such column exists on
--     any installed evidence relation.
--   * The finance approver HAS now ruled (HANDOVER 2 response R5, ruling B1a),
--     and the withholding is the RULED behaviour, not a placeholder.  What is
--     still open is the restatement UNIT -- ROOT versus ROOT_AND_SHIFT (WP-11e
--     G8) -- and the two disagree on a real figure.
--
-- So asserting a specific opened-gate figure would be asserting one side of a
-- question the approver has not answered, on a recipe its owner has withdrawn.
-- This section therefore opens the gate in the only way that is well defined
-- today -- the fall-through made unconditional, the tie test kept exactly as
-- shipped -- and asserts only what is invariant under BOTH units: the phase and
-- heading stand, a figure appears, it is the LATEST RESTATEMENT and not a sum
-- across settlements, the evidence counts survive, every other row is
-- unaffected, and the tie rule still fires.  The figure itself is left to the
-- package that owns it.  Then the shipped gate is rolled back in.
-- ---------------------------------------------------------------------------
savepoint gate9_before_narrowing;

create or replace function private.weekly_source_settlement_position_gate_v1(
  p_settlements jsonb
) returns text
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_count integer;
  v_at_max integer;
begin
  v_count:=pg_catalog.jsonb_array_length(coalesce(p_settlements,'[]'::jsonb));
  if v_count<=1 then
    return null;
  end if;
  select pg_catalog.count(*)::integer
    into v_at_max
  from pg_catalog.jsonb_array_elements(p_settlements) as element(value)
  where (element.value->>'settled_at_utc')::timestamptz
      = (select pg_catalog.max((inner_element.value->>'settled_at_utc')::timestamptz)
         from pg_catalog.jsonb_array_elements(p_settlements) as inner_element(value));
  if v_at_max>1 then
    -- The tie test is kept EXACTLY as shipped: a tie is an integrity fault,
    -- independent of the ruling, and must survive any opening of the gate.
    return 'SETTLEMENT_ORDER_AMBIGUOUS';
  end if;
  -- The one-line substitution: the final return is made unconditional.  This is
  -- a complete function with a RETURN on every path, which the deletion WP-11d
  -- prescribed would not have been.
  return null;
end;
$function$;

do $gate9_narrowed$
declare
  v jsonb;
  s jsonb;
begin
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-00000000000c'));
  perform pg_temp.assert_true((v#>>'{lifecycle,ok}')::boolean
    and v#>>'{lifecycle,ui_state}'='UI-012'
    and v#>>'{lifecycle,heading}'='Current paid hours',
    'UI-012 did not keep its phase and heading under the opened gate');
  s:=v#>'{lifecycle,schedules,current_paid}';
  -- The RESTATEMENT model, asserted against the unit the SERVER declares rather
  -- than against a number this verifier chose.
  --
  -- The fixture's two settlements restate DIFFERENT shifts -- 7.5 on one and 3.0
  -- on another -- so the two units genuinely disagree, and there is no number
  -- that is right under both:
  --
  --   ROOT_AND_SHIFT  each shift's position is the latest settlement that
  --                   restated THAT shift, so both shifts stand: two rows, 10.5.
  --   ROOT            the latest settlement's whole snapshot IS the position, so
  --                   a shift absent from it is no longer paid: one row, 3.0.
  --
  -- WP-11e G8 says the approver must confirm which is intended and names
  -- `weekly_source_settlement_position_unit_v1` as the one place that choice
  -- lives.  So this reads that function and asserts the figure agrees with it.
  -- The check stays numeric and stays strict, and it cannot pre-judge the
  -- ruling: if the unit changes, this assertion follows it, and if the reader
  -- ever disagrees with its own declared unit, this fails.
  perform pg_temp.assert_true((s->>'available')::boolean
    and s->>'unavailable_class' is null
    and s->>'position_basis'='LATEST_RESTATEMENT'
    and s ? 'total_hours',
    'the opened gate did not give a latest-RESTATEMENT position: '||s::text);
  perform pg_temp.assert_true(
    case private.weekly_source_settlement_position_unit_v1()
      when 'ROOT_AND_SHIFT' then
        (s->>'total_hours')::numeric=10.5 and (s->>'row_count')::integer=2
      when 'ROOT' then
        (s->>'total_hours')::numeric=3.0 and (s->>'row_count')::integer=1
      else false
    end,
    'the opened figure did not agree with the restatement unit the server '
    ||'declares ('||coalesce(private.weekly_source_settlement_position_unit_v1(),
                            '<null>')||'): '||s::text);
  -- And under EITHER unit it is a restatement, never an accumulation: the two
  -- settlements are not added to each other for any shift.  Each row's hours
  -- come from exactly one settlement.
  perform pg_temp.assert_true(
    not exists(
      select 1
      from pg_catalog.jsonb_array_elements(coalesce(s->'rows','[]'::jsonb)) as row_element(value)
      where (row_element.value->>'total_hours')::numeric not in (7.5,3.0)),
    'a paid row carried hours that are not one settlement''s restated position, '
    ||'which means settlements were accumulated: '||s::text);
  perform pg_temp.assert_true(
    v#>'{lifecycle,schedules,paid_to_date}'=v#>'{lifecycle,schedules,current_paid}',
    'the two paid schedules diverged under the opened gate');
  perform pg_temp.assert_true((s->>'settlement_count')::integer=2
    and (s->>'batch_count')::integer=2,
    'the opened figure lost the evidence counts');
  -- Every other row is unchanged by the narrowing.
  perform pg_temp.assert_true((
    select v2#>>'{lifecycle,ui_state}'
    from (select public.weekly_source_office_timesheet_presentation_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id','d1000000-0000-4000-8000-000000000001',
        'timesheet_id','fa000000-0000-4000-8000-000000000007')) v2) probe)='UI-007',
    'UI-007 changed under the opened gate');
  -- The tie rule must SURVIVE the opening: it is an integrity fault independent
  -- of the ruling, and WP-11e keeps it for exactly that reason.
  perform pg_temp.assert_true(
    private.weekly_source_settlement_position_gate_v1(
      pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('settled_at_utc','2026-11-26T11:00:00+00:00'),
        pg_catalog.jsonb_build_object('settled_at_utc','2026-11-26T11:00:00+00:00')))
    ='SETTLEMENT_ORDER_AMBIGUOUS',
    'the tie rule did not survive the opening');
end;
$gate9_narrowed$;

rollback to savepoint gate9_before_narrowing;

do $gate9_gate_restored$
declare
  v jsonb;
begin
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','d1000000-0000-4000-8000-000000000001',
    'timesheet_id','fa000000-0000-4000-8000-00000000000c'));
  perform pg_temp.assert_true(
    (v#>>'{lifecycle,schedules,current_paid,available}')::boolean is false
    and v#>>'{lifecycle,schedules,current_paid,unavailable_class}'='POSITION_WITHHELD',
    'the shipped fail-closed gate was not restored after the opened-gate proof');
end;
$gate9_gate_restored$;

rollback;
