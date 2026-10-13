-- WP-F1 rollback-only fixtures and real settings-owner admission proofs.
-- Root supplies the exact fixture manifest/vacancy evidence before invocation.
-- No finalisation, authorisation, payment, invoice, publication or comms owner.
\set ON_ERROR_STOP on
\pset pager off
begin;
set local lock_timeout='5s';
set local statement_timeout='60s';
select set_config('request.jwt.claim.role','service_role',true);
create or replace function pg_temp.f1_assert(p_ok boolean,p_name text)
returns void language plpgsql as $f$
begin if p_ok is not true then raise exception 'F1_VERIFY_FAILED: %',p_name; end if; end;
$f$;
create or replace function pg_temp.f1_refuses(p_sql text,p_code text)
returns void language plpgsql as $f$
begin
  begin execute p_sql;
  exception when others then
    if sqlerrm=p_code then return; end if;
    raise exception 'F1_WRONG_REFUSAL: expected %, received %',p_code,sqlerrm;
  end;
  raise exception 'F1_REFUSAL_MISSING: %',p_code;
end;
$f$;

-- Generated owner IDs are anchored by these exact synthetic actors/groups.
-- Guard UUID prefixes AND the unique text keys so unrelated rows are untouched.
select pg_temp.f1_assert(not exists(select 1 from public.tms_users where id::text like 'f7110000-%' or email like 'f1-%@example.invalid')
  and not exists(select 1 from public.clients where id::text like 'f7110000-%' or name like 'F1 fixture %')
  and not exists(select 1 from public.client_settings where id::text like 'f7110000-%')
  and not exists(select 1 from public.candidates where id::text like 'f7110000-%' or tms_ref like 'F1-VERIFY-%')
  and not exists(select 1 from public.contracts where id::text like 'f7110000-%')
  and not exists(select 1 from public.timesheets where timesheet_id::text like 'f7110000-%' or booking_id like 'F1-VERIFY-%')
  and not exists(select 1 from public.weekly_source_groups where id::text like 'f7110000-%' or code like 'F1_VERIFY_%')
  and not exists(select 1 from public.weekly_source_group_clients where id::text like 'f7110000-%')
  and not exists(select 1 from public.weekly_source_client_policies where id::text like 'f7110000-%')
  and not exists(select 1 from public.weekly_source_cycles where id::text like 'f7110000-%')
  and not exists(select 1 from public.weekly_source_uploads where id::text like 'f7110000-%' or original_filename like 'f1-verify-%')
  and not exists(select 1 from public.weekly_source_final_revisions where id::text like 'f7110000-%')
  and not exists(select 1 from public.weekly_source_rule_sets where code like 'F1_VERIFY_%' or created_by_user_id::text like 'f7110000-%')
  and not exists(select 1 from public.weekly_source_rule_set_versions where created_by_user_id::text like 'f7110000-%')
  and not exists(select 1 from public.weekly_source_group_rule_assignments where created_by_user_id::text like 'f7110000-%'), 'exact synthetic fixture vacancy');

insert into public.tms_users(id,email,role,password_hash,display_name,is_active,payment_authoriser,payment_golden_key) values
  ('f7110000-0000-4000-8000-000000000001','f1-admin@example.invalid','admin','not-a-real-password','F1 admin',true,false,false),
  ('f7110000-0000-4000-8000-000000000002','f1-authoriser@example.invalid','user','not-a-real-password','F1 authoriser',true,true,false),
  ('f7110000-0000-4000-8000-000000000003','f1-inactive@example.invalid','admin','not-a-real-password','F1 inactive',false,false,false);
insert into public.clients(id,name,ts_queries_email,vat_chargeable) values
  ('f7110000-0000-4000-8000-000000000010','F1 fixture self bill','f1-manager@example.invalid',true),
  ('f7110000-0000-4000-8000-000000000011','F1 fixture agency','f1-manager@example.invalid',true),
  ('f7110000-0000-4000-8000-000000000012','F1 fixture signed','f1-manager@example.invalid',true),
  ('f7110000-0000-4000-8000-000000000013','F1 fixture NHSP','f1-manager@example.invalid',true);
insert into public.client_settings(id,client_id,effective_from,hr_validation_required,autoprocess_hr,self_bill_no_invoices_sent,no_timesheet_required,requires_hr,is_nhsp) values
  ('f7110000-0000-4000-8000-000000000020','f7110000-0000-4000-8000-000000000010',current_date-60,true,true,true,true,true,false),
  ('f7110000-0000-4000-8000-000000000021','f7110000-0000-4000-8000-000000000011',current_date-60,true,true,false,false,true,false),
  ('f7110000-0000-4000-8000-000000000022','f7110000-0000-4000-8000-000000000012',current_date-60,true,true,false,false,true,false),
  ('f7110000-0000-4000-8000-000000000023','f7110000-0000-4000-8000-000000000013',current_date-60,false,false,true,false,false,true);
insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email) values
  ('f7110000-0000-4000-8000-000000000030','F1-VERIFY-AGENCY','F1','Agency','F1 Agency','f1-candidate-agency@example.invalid'),
  ('f7110000-0000-4000-8000-000000000031','F1-VERIFY-SIGNED','F1','Signed','F1 Signed','f1-candidate-signed@example.invalid');
insert into public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp,overrideclientsettings,require_reference_to_pay) values
  ('f7110000-0000-4000-8000-000000000040','f7110000-0000-4000-8000-000000000030','f7110000-0000-4000-8000-000000000011',current_date-30,current_date+180,'PAYE','{}',false,'HEALTHROSTER',false,true,true,false,false,false),
  ('f7110000-0000-4000-8000-000000000041','f7110000-0000-4000-8000-000000000031','f7110000-0000-4000-8000-000000000012',current_date-30,current_date+180,'PAYE','{}',false,'HEALTHROSTER',false,true,true,false,false,false);
insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time,nhsp_report_heading_name) values
  ('f7110000-0000-4000-8000-000000000050','TEST','f7110000-0000-4000-8000-000000000099','F1_VERIFY_SELF','F1 self bill','ROSTER',3,'23:59',null),
  ('f7110000-0000-4000-8000-000000000051','TEST','f7110000-0000-4000-8000-000000000099','F1_VERIFY_AGENCY','F1 agency','ROSTER',3,'23:59',null),
  ('f7110000-0000-4000-8000-000000000052','TEST','f7110000-0000-4000-8000-000000000099','F1_VERIFY_SIGNED','F1 signed','ROSTER',3,'23:59',null),
  ('f7110000-0000-4000-8000-000000000053','TEST','f7110000-0000-4000-8000-000000000099','F1_VERIFY_NHSP','F1 NHSP','NHSP',3,'23:59','F1 fixture NHSP');
insert into public.weekly_source_group_clients(id,source_group_id,client_id,valid_from,created_by_user_id) values
  ('f7110000-0000-4000-8000-000000000060','f7110000-0000-4000-8000-000000000050','f7110000-0000-4000-8000-000000000010',current_date-30,'f7110000-0000-4000-8000-000000000001'),
  ('f7110000-0000-4000-8000-000000000061','f7110000-0000-4000-8000-000000000051','f7110000-0000-4000-8000-000000000011',current_date-30,'f7110000-0000-4000-8000-000000000001'),
  ('f7110000-0000-4000-8000-000000000062','f7110000-0000-4000-8000-000000000052','f7110000-0000-4000-8000-000000000012',current_date-30,'f7110000-0000-4000-8000-000000000001'),
  ('f7110000-0000-4000-8000-000000000063','f7110000-0000-4000-8000-000000000053','f7110000-0000-4000-8000-000000000013',current_date-30,'f7110000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(id,source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,created_by_user_id) values
  ('f7110000-0000-4000-8000-000000000070','f7110000-0000-4000-8000-000000000050','f7110000-0000-4000-8000-000000000010',current_date-30,'SOURCE_AUTHORITY','CHECK_ONLY',true,false,false,'f7110000-0000-4000-8000-000000000001'),
  ('f7110000-0000-4000-8000-000000000071','f7110000-0000-4000-8000-000000000051','f7110000-0000-4000-8000-000000000011',current_date-30,'TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',false,false,false,'f7110000-0000-4000-8000-000000000001'),
  ('f7110000-0000-4000-8000-000000000072','f7110000-0000-4000-8000-000000000052','f7110000-0000-4000-8000-000000000012',current_date-30,'TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',false,false,false,'f7110000-0000-4000-8000-000000000001'),
  ('f7110000-0000-4000-8000-000000000073','f7110000-0000-4000-8000-000000000053','f7110000-0000-4000-8000-000000000013',current_date-30,'SOURCE_AUTHORITY','CHECK_ONLY',true,false,false,'f7110000-0000-4000-8000-000000000001');

create temporary table f1_owner_proof(name text primary key,value jsonb);
do $verify$
declare
  v_base jsonb:=jsonb_build_object('actor_user_id','f7110000-0000-4000-8000-000000000001','agency_id','f7110000-0000-4000-8000-000000000099','environment','TEST');
  v_week date:=((transaction_timestamp() at time zone 'Europe/London')::date+21);
  v_save jsonb; v_save_request jsonb; v_saved_two jsonb; v_assign jsonb; v_assign_request jsonb; v_agency_save jsonb; v_agency_assign jsonb; v_signed_assign jsonb;
  v_cycle uuid:='f7110000-0000-4000-8000-000000000080';
  v_before_policy jsonb; v_shape jsonb; v_receipt jsonb; v_request jsonb; v_latest private.cloudtms_database_releases%rowtype;
  v_signature text; v_role text; v_enabled boolean;
  v_profile uuid; v_off_revision uuid:='f7110000-0000-4000-8000-000000000085';
begin
  v_week:=v_week+(7-extract(dow from v_week)::integer)%7;
  -- A later Contract outside a finite membership must not participate in this
  -- former group's rules. No old policy is invented for that later date.
  update public.weekly_source_group_clients set valid_to=v_week where id='f7110000-0000-4000-8000-000000000060';
  insert into public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,self_bill,weekly_timesheet_source,
    no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp,overrideclientsettings,require_reference_to_pay)
    values('f7110000-0000-4000-8000-000000000042','f7110000-0000-4000-8000-000000000030','f7110000-0000-4000-8000-000000000010',v_week+8,v_week+30,
      'PAYE','{}',false,'HEALTHROSTER',false,true,true,false,false,false);
  insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc) values
    (v_cycle,'f7110000-0000-4000-8000-000000000050',v_week,v_week+interval '3 days'),
    ('f7110000-0000-4000-8000-000000000081','f7110000-0000-4000-8000-000000000051',v_week,v_week+interval '3 days'),
    ('f7110000-0000-4000-8000-000000000082','f7110000-0000-4000-8000-000000000052',v_week,v_week+interval '3 days');
  v_before_policy:=private._weekly_source_effective_policy_v1('f7110000-0000-4000-8000-000000000010',null,v_week);
  v_save_request:=v_base||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000050','request_id','f7110000-0000-4000-8000-000000000100',
    'rule_set',jsonb_build_object('id',null,'code','F1_VERIFY_RULES','display_name','F1 verified rule set','expected_version',0),
    'values',jsonb_build_object('checking_profile_kind','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1'));
  v_save:=public.weekly_source_rule_set_version_save_atomic_v1(v_save_request);
  perform pg_temp.f1_assert((public.weekly_source_rule_set_version_save_atomic_v1(v_save_request)->>'replayed')::boolean,'exact save replay');
  perform pg_temp.f1_refuses(format('select public.weekly_source_rule_set_version_save_atomic_v1(%L::jsonb)',v_save_request||jsonb_build_object('values',jsonb_build_object('checking_profile_kind','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1'))),'WEEKLY_SOURCE_RULE_REQUEST_REUSED');
  perform pg_temp.f1_refuses(format('select public.weekly_source_rule_set_version_save_atomic_v1(%L::jsonb)',v_save_request||jsonb_build_object('actor_user_id','f7110000-0000-4000-8000-000000000002')),'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED');
  perform pg_temp.f1_refuses(format('select public.weekly_source_rule_sets_get_v1(%L::jsonb)',v_base||jsonb_build_object('actor_user_id','f7110000-0000-4000-8000-000000000003')),'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED');
  perform pg_temp.f1_refuses(format('update public.weekly_source_rule_set_versions set values_json=values_json where id=%L::uuid',v_save->>'rule_set_version_id'),'WEEKLY_SOURCE_IMMUTABLE_RECORD');
  perform pg_temp.f1_refuses(format('delete from public.weekly_source_rule_set_versions where id=%L::uuid',v_save->>'rule_set_version_id'),'WEEKLY_SOURCE_IMMUTABLE_RECORD');
  perform pg_temp.f1_assert(exists(select 1 from pg_catalog.pg_trigger where tgrelid='public.weekly_source_rule_set_versions'::regclass
    and not tgisinternal and tgname='weekly_source_immutable_truncate_guard'
    and tgfoid='private._weekly_source_immutable_record_guard_v1()'::regprocedure and (tgtype::integer&32)=32),'central TRUNCATE guard installed');
  perform pg_temp.f1_refuses(format('select public.weekly_source_rule_set_version_save_atomic_v1(%L::jsonb)',v_save_request||jsonb_build_object('request_id','f7110000-0000-4000-8000-000000000101','rule_set',jsonb_build_object('id',v_save->>'rule_set_id','code','F1_VERIFY_RULES','display_name','F1 stale','expected_version',99))),'WEEKLY_SOURCE_RULE_SET_STALE');
  v_saved_two:=public.weekly_source_rule_set_version_save_atomic_v1(v_save_request||jsonb_build_object('request_id','f7110000-0000-4000-8000-000000000102',
    'rule_set',jsonb_build_object('id',v_save->>'rule_set_id','code','F1_VERIFY_RULES','display_name','F1 verified rule set','expected_version',1),
    'values',jsonb_build_object('checking_profile_kind','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1')));
  perform pg_temp.f1_assert((v_saved_two->>'version_number')::integer=2 and v_saved_two->>'rule_set_version_id'<>v_save->>'rule_set_version_id','new immutable version');
  v_assign_request:=v_base||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000050','rule_set_version_id',v_save->>'rule_set_version_id',
    'effective_from_week_ending',v_week,'effective_to_week_ending',null,'expected_group_version',1,'request_id','f7110000-0000-4000-8000-000000000110');
  v_assign:=public.weekly_source_group_rule_assign_atomic_v1(v_assign_request);
  perform pg_temp.f1_assert((public.weekly_source_group_rule_assign_atomic_v1(v_assign_request)->>'replayed')::boolean,'exact assignment replay');
  perform pg_temp.f1_assert(not (v_assign->>'feature_enabled')::boolean and private.weekly_source_effective_rule_set_v1('f7110000-0000-4000-8000-000000000010',v_cycle) is null,'assignment remains OFF');
  perform pg_temp.f1_assert(v_before_policy=private._weekly_source_effective_policy_v1('f7110000-0000-4000-8000-000000000010',null,v_week),'off save/assignment exact effective policy/hash unchanged');
  perform pg_temp.f1_assert(not private.weekly_source_has_self_bill_rules_v1('f7110000-0000-4000-8000-000000000010',v_cycle),'off rule predicate false');
  perform pg_temp.f1_assert(not private.weekly_source_is_import_authoritative_v1('f7110000-0000-4000-8000-000000000013',null,v_week),'NHSP excluded');
  perform pg_temp.f1_assert(not private.weekly_source_is_import_authoritative_v1('f7110000-0000-4000-8000-000000000098',null,v_week),'ordinary/unassigned client false');
  perform pg_temp.f1_assert(not private.weekly_source_is_import_authoritative_v1('f7110000-0000-4000-8000-000000000010','f7110000-0000-4000-8000-000000000042',v_week+8),'Contract after membership end is outside group');
  perform pg_temp.f1_refuses(format('select public.weekly_source_group_rule_assign_atomic_v1(%L::jsonb)',v_assign_request||jsonb_build_object('request_id','f7110000-0000-4000-8000-000000000111')),'WEEKLY_SOURCE_RULE_GROUP_STALE');
  perform pg_temp.f1_refuses(format('select public.weekly_source_group_rule_assign_atomic_v1(%L::jsonb)',v_assign_request||jsonb_build_object('request_id','f7110000-0000-4000-8000-000000000112','expected_group_version',2)),'WEEKLY_SOURCE_RULE_ASSIGNMENT_OVERLAP');
  perform pg_temp.f1_refuses(format('select public.weekly_source_group_rule_assign_atomic_v1(%L::jsonb)',v_assign_request||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000053','request_id','f7110000-0000-4000-8000-000000000113')),'WEEKLY_SOURCE_RULE_GROUP_NOT_APPLICABLE');
  perform pg_temp.f1_refuses(format('select public.weekly_source_group_rule_assign_atomic_v1(%L::jsonb)',v_assign_request||jsonb_build_object('agency_id','f7110000-0000-4000-8000-000000000098','request_id','f7110000-0000-4000-8000-000000000114')),'WEEKLY_SOURCE_RULE_GROUP_NOT_APPLICABLE');
  perform pg_temp.f1_refuses(format('select public.weekly_source_rule_set_version_save_atomic_v1(%L::jsonb)',v_save_request||jsonb_build_object('request_id','f7110000-0000-4000-8000-000000000115','values',jsonb_build_object('checking_profile_kind','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','earliest_final_weekday',3,'earliest_final_local_time','23:58'))),'WEEKLY_SOURCE_RULE_R9');
  perform pg_temp.f1_refuses(format('select private._weekly_source_rule_values_v1(%L::jsonb)',jsonb_build_object('checking_profile_kind','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','invoice_sender','AGENCY_INVOICE','lookback_warning','OFF')),'WEEKLY_SOURCE_RULE_R13');
  -- Clearing applies only to a later unfinalised range, clips the old open range
  -- and cannot turn any group on. NULL remains a legacy financial path.
  v_receipt:=public.weekly_source_group_rule_assign_atomic_v1(v_assign_request||jsonb_build_object('rule_set_version_id',null,'effective_from_week_ending',v_week+7,'expected_group_version',2,'request_id','f7110000-0000-4000-8000-000000000116'));
  perform pg_temp.f1_assert(v_receipt->'rule_set_version_id'='null'::jsonb and not (v_receipt->>'feature_enabled')::boolean,'dated NULL clearing');
  perform pg_temp.f1_assert((select effective_to_week_ending=v_week from public.weekly_source_group_rule_assignments where id=(v_assign->>'assignment_id')::uuid),'open previous range clipped');
  -- Final rule witnesses cannot be backfilled, and any final revision for any
  -- member locks its cycle out of future assignment changes.
  select id into strict v_profile from public.weekly_source_format_profiles
    where profile_code='HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1' and active order by version desc limit 1;
  insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc)
    values('f7110000-0000-4000-8000-000000000084','f7110000-0000-4000-8000-000000000050',v_week+14,v_week+interval '17 days');
  insert into public.weekly_source_uploads(id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id)
    values('f7110000-0000-4000-8000-000000000086','f7110000-0000-4000-8000-000000000084','f1-verify-off.xlsx',decode(repeat('31',32),'hex'),100,v_profile,
      'verify','verify',decode(repeat('32',32),'hex'),decode(repeat('33',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,decode(repeat('34',32),'hex'),'SEALED','f7110000-0000-4000-8000-000000000001');
  insert into public.weekly_source_final_revisions(id,source_cycle_id,authority_scope_kind,revision_number,upload_id,coverage_start_local_date,coverage_end_local_date,
    coverage_timezone,reason,finalised_by_user_id,manifest_hash,policy_fingerprint,state)
    values(v_off_revision,'f7110000-0000-4000-8000-000000000084','CYCLE',1,'f7110000-0000-4000-8000-000000000086',v_week+8,v_week+14,
      'Europe/London','INITIAL_FINALISATION','f7110000-0000-4000-8000-000000000001',decode(repeat('35',32),'hex'),decode(repeat('36',32),'hex'),'CURRENT');
  perform pg_temp.f1_assert((select rule_set_version_id is null from public.weekly_source_final_revisions where id=v_off_revision),'OFF insert preserves NULL witness');
  perform pg_temp.f1_refuses(format('update public.weekly_source_final_revisions set rule_set_version_id=%L::uuid where id=%L::uuid',v_save->>'rule_set_version_id',v_off_revision),'WEEKLY_SOURCE_FINAL_RULE_VERSION_IMMUTABLE');
  perform pg_temp.f1_refuses(format('select public.weekly_source_group_rule_assign_atomic_v1(%L::jsonb)',v_assign_request||jsonb_build_object('effective_from_week_ending',v_week+14,
    'expected_group_version',3,'request_id','f7110000-0000-4000-8000-000000000117')),'WEEKLY_SOURCE_RULE_FINALISED_WEEK_REFUSED');
  -- Agency choice is saved under the real old TIMESHEET policy while OFF.
  v_before_policy:=private._weekly_source_effective_policy_v1('f7110000-0000-4000-8000-000000000011','f7110000-0000-4000-8000-000000000040',v_week);
  v_agency_save:=public.weekly_source_rule_set_version_save_atomic_v1(v_base||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000051',
    'request_id','f7110000-0000-4000-8000-000000000120','rule_set',jsonb_build_object('id',null,'code','F1_VERIFY_AGENCY_RULES','display_name','F1 agency rules','expected_version',0),
    'values',jsonb_build_object('checking_profile_kind','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','invoice_sender','AGENCY_INVOICE','attach_healthroster_section',false)));
  v_agency_assign:=public.weekly_source_group_rule_assign_atomic_v1(v_assign_request||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000051',
    'rule_set_version_id',v_agency_save->>'rule_set_version_id','request_id','f7110000-0000-4000-8000-000000000121'));
  perform pg_temp.f1_assert(v_before_policy=private._weekly_source_effective_policy_v1('f7110000-0000-4000-8000-000000000011','f7110000-0000-4000-8000-000000000040',v_week),'agency OFF stored/effective policy unchanged');
  v_signed_assign:=public.weekly_source_group_rule_assign_atomic_v1(v_assign_request||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000052',
    'rule_set_version_id',null,'request_id','f7110000-0000-4000-8000-000000000122'));
  insert into public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,contract_id,week_ending_date,line_type,submission_mode,sheet_scope,worked_start_iso,worked_end_iso) values
    ('f7110000-0000-4000-8000-000000000090','F1-VERIFY-AGENCY','f1-agency','f1-fixture-agency','ward','nurse','f7110000-0000-4000-8000-000000000040',v_week,'HOURS','MANUAL','WEEKLY',(v_week-3)+time '08:00',(v_week-3)+time '16:00'),
    ('f7110000-0000-4000-8000-000000000091','F1-VERIFY-SIGNED','f1-signed','f1-fixture-signed','ward','nurse','f7110000-0000-4000-8000-000000000041',v_week,'HOURS','MANUAL','WEEKLY',(v_week-3)+time '08:00',(v_week-3)+time '16:00'),
    ('f7110000-0000-4000-8000-000000000092','F1-VERIFY-DAILY','f1-signed','f1-fixture-signed','ward','nurse',null,v_week,'HOURS','MANUAL','DAILY',(v_week-3)+time '08:00',(v_week-3)+time '16:00');
  -- Native MANUAL carrier is legal without synthetic signatures, PDFs, READY
  -- or manager approval marks. The predicate proves scope only. Existing
  -- 03092026_1642 settings-authority verification uses this same legal carrier.
  insert into public.timesheets_financials(timesheet_id,timesheet_version,candidate_id,client_id,total_hours,processing_status) values
    ('f7110000-0000-4000-8000-000000000092',1,'f7110000-0000-4000-8000-000000000031','f7110000-0000-4000-8000-000000000012',8,'UNPROCESSED');
  perform pg_temp.f1_assert(not private.weekly_source_agency_invoice_check_only_v1('f7110000-0000-4000-8000-000000000090')
    and not private.weekly_source_signed_check_only_v1('f7110000-0000-4000-8000-000000000091'),'all new physical Timesheet gates OFF');
  select * into v_latest from private.cloudtms_database_releases order by started_at_utc desc,release_id desc limit 1;
  v_request:=v_base||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000051','assignment_id',v_agency_assign->>'assignment_id',
    'rule_set_version_id',v_agency_save->>'rule_set_version_id','effective_from_week_ending',v_week,'expected_assignment_version',1,
    'feature_enabled',true,'feature_purpose','RULE_SET','release_id',coalesce(v_latest.release_id,'F1-missing-release'),'reason','F1 rollback fixture only','request_id','f7110000-0000-4000-8000-000000000130');
  perform pg_temp.f1_refuses(format('select public.weekly_source_group_rule_feature_set_atomic_v1(%L::jsonb)',v_request||jsonb_build_object('feature_enabled','true')),'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID');
  perform pg_temp.f1_refuses(format('select public.weekly_source_group_rule_feature_set_atomic_v1(%L::jsonb)',v_request||jsonb_build_object('release_id','F1-not-latest-release')),'WEEKLY_SOURCE_RULE_VERIFIED_RELEASE_REQUIRED');
  if v_latest.status='VERIFIED' and v_latest.completed_at_utc is not null and v_latest.installed_contract_sha256=v_latest.repository_contract_sha256 then
    v_shape:=private._weekly_source_settings_client_shape_v1('f7110000-0000-4000-8000-000000000099','TEST','f7110000-0000-4000-8000-000000000011',v_week-6);
    v_request:=v_request||jsonb_build_object('client_settings_versions',jsonb_build_array(jsonb_build_object('client_id','f7110000-0000-4000-8000-000000000011','expected_settings_version',v_shape->>'settings_version')));
    v_receipt:=public.weekly_source_group_rule_feature_set_atomic_v1(v_request);
    perform pg_temp.f1_assert((v_receipt->>'feature_enabled')::boolean and jsonb_array_length(v_receipt->'client_changes')=1,'atomic agency enable with before/after receipt');
    perform pg_temp.f1_assert((public.weekly_source_group_rule_feature_set_atomic_v1(v_request)->>'replayed')::boolean,'exact activation replay');
    perform pg_temp.f1_assert(private.weekly_source_agency_invoice_check_only_v1('f7110000-0000-4000-8000-000000000090'),'actual enabled unsigned agency Timesheet');
    perform pg_temp.f1_assert(private.weekly_source_has_self_bill_rules_v1('f7110000-0000-4000-8000-000000000011','f7110000-0000-4000-8000-000000000081'),'literal nonNULL rules predicate includes agency');
    perform pg_temp.f1_assert(not private.weekly_source_signed_check_only_v1('f7110000-0000-4000-8000-000000000090'),'agency is not signed route');
    v_shape:=private._weekly_source_settings_client_shape_v1('f7110000-0000-4000-8000-000000000099','TEST','f7110000-0000-4000-8000-000000000011',v_week-6);
    v_request:=v_request||jsonb_build_object('feature_enabled',false,'expected_assignment_version',2,'request_id','f7110000-0000-4000-8000-000000000131',
      'client_settings_versions',jsonb_build_array(jsonb_build_object('client_id','f7110000-0000-4000-8000-000000000011','expected_settings_version',v_shape->>'settings_version')));
    v_receipt:=public.weekly_source_group_rule_feature_set_atomic_v1(v_request);
    perform pg_temp.f1_assert(not private.weekly_source_agency_invoice_check_only_v1('f7110000-0000-4000-8000-000000000090')
      and v_receipt->'client_changes'->0->'restored_values'->>'authority_mode'='TIMESHEET_AUTHORITY','atomic disable restores old authority');
    v_request:=v_base||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000052','assignment_id',v_signed_assign->>'assignment_id',
      'rule_set_version_id',null,'effective_from_week_ending',v_week,'expected_assignment_version',1,'feature_enabled',true,'feature_purpose','SIGNED_CHECK_ONLY',
      'release_id',v_latest.release_id,'reason','F1 signed rollback fixture only','request_id','f7110000-0000-4000-8000-000000000132');
    v_receipt:=public.weekly_source_group_rule_feature_set_atomic_v1(v_request);
    perform pg_temp.f1_assert(private.weekly_source_signed_check_only_v1('f7110000-0000-4000-8000-000000000091'),'actual current signed-group Weekly scope');
    perform pg_temp.f1_assert(private.weekly_source_signed_check_only_v1('f7110000-0000-4000-8000-000000000092'),'actual nullable-Contract Daily scope from current TSFIN client');
    perform pg_temp.f1_assert(private.weekly_source_effective_rule_set_v1('f7110000-0000-4000-8000-000000000012','f7110000-0000-4000-8000-000000000082') is null,'enabled NULL signed choice retains old rule path');
    v_request:=v_base||jsonb_build_object('source_group_id','f7110000-0000-4000-8000-000000000050','assignment_id',v_assign->>'assignment_id',
      'rule_set_version_id',v_save->>'rule_set_version_id','effective_from_week_ending',v_week,'expected_assignment_version',2,'feature_enabled',true,'feature_purpose','RULE_SET',
      'release_id',v_latest.release_id,'reason','F1 frozen witness rollback fixture only','request_id','f7110000-0000-4000-8000-000000000133');
    v_receipt:=public.weekly_source_group_rule_feature_set_atomic_v1(v_request);
    insert into public.weekly_source_uploads(id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
      parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id)
      values('f7110000-0000-4000-8000-000000000087',v_cycle,'f1-verify-enabled.xlsx',decode(repeat('41',32),'hex'),100,v_profile,
        'verify','verify',decode(repeat('42',32),'hex'),decode(repeat('43',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,decode(repeat('44',32),'hex'),'SEALED','f7110000-0000-4000-8000-000000000001');
    insert into public.weekly_source_final_revisions(id,source_cycle_id,authority_scope_kind,revision_number,upload_id,coverage_start_local_date,coverage_end_local_date,
      coverage_timezone,reason,finalised_by_user_id,manifest_hash,policy_fingerprint,state)
      values('f7110000-0000-4000-8000-000000000088',v_cycle,'CYCLE',1,'f7110000-0000-4000-8000-000000000087',v_week-6,v_week,
        'Europe/London','INITIAL_FINALISATION','f7110000-0000-4000-8000-000000000001',decode(repeat('45',32),'hex'),decode(repeat('46',32),'hex'),'CURRENT');
    perform pg_temp.f1_assert((select rule_set_version_id=(v_save->>'rule_set_version_id')::uuid from public.weekly_source_final_revisions
      where id='f7110000-0000-4000-8000-000000000088'),'enabled revision INSERT freezes exact immutable version');
    insert into f1_owner_proof values('activation',jsonb_build_object('positive_exercised',true,'real_latest_release_id',v_latest.release_id));
  else
    perform pg_temp.f1_refuses(format('select public.weekly_source_group_rule_feature_set_atomic_v1(%L::jsonb)',v_request),'WEEKLY_SOURCE_RULE_VERIFIED_RELEASE_REQUIRED');
    insert into f1_owner_proof values('activation',jsonb_build_object('positive_exercised',false,'held_by_real_release_gate',true));
  end if;
  perform pg_temp.f1_assert(not private.weekly_source_signed_check_only_v1('f7110000-0000-4000-8000-000000000098')
    and not private.weekly_source_agency_invoice_check_only_v1('f7110000-0000-4000-8000-000000000098'),'unknown physical Timesheets false');
  foreach v_signature in array array['private._weekly_source_rule_values_v1(jsonb)','private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean)',
    'private.weekly_source_is_import_authoritative_v1(uuid,uuid,date)','private.weekly_source_effective_rule_set_v1(uuid,uuid)','private.weekly_source_has_self_bill_rules_v1(uuid,uuid)',
    'private.weekly_source_agency_invoice_check_only_v1(uuid)','private.weekly_source_signed_check_only_v1(uuid)','private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date)',
    'private.weekly_source_final_rule_version_guard_v1()'] loop
    perform pg_temp.f1_assert(to_regprocedure(v_signature) is not null,'private helper exists');
    foreach v_role in array array['anon','authenticated','service_role'] loop
      perform pg_temp.f1_assert(not has_function_privilege(v_role,v_signature,'EXECUTE'),'private helper effective API EXECUTE denied');
    end loop;
  end loop;
  foreach v_signature in array array['public.weekly_source_rule_sets_get_v1(jsonb)','public.weekly_source_rule_set_version_save_atomic_v1(jsonb)',
    'public.weekly_source_group_rule_assign_atomic_v1(jsonb)','public.weekly_source_group_rule_feature_set_atomic_v1(jsonb)'] loop
    perform pg_temp.f1_assert(has_function_privilege('service_role',v_signature,'EXECUTE')
      and not has_function_privilege('anon',v_signature,'EXECUTE') and not has_function_privilege('authenticated',v_signature,'EXECUTE'),'service-only RPC grants');
  end loop;
  insert into f1_owner_proof values('catalogue',public.weekly_source_rule_sets_get_v1(v_base));
end;
$verify$;
select jsonb_build_object('contract','WP_F1_ROLLBACK_PROOF_V1','settings_owners_exercised',true,
  'activation',(select value from f1_owner_proof where name='activation'),
  'rule_set_count',(select jsonb_array_length(value->'rule_sets') from f1_owner_proof where name='catalogue'),
  'assignment_count',(select jsonb_array_length(value->'assignments') from f1_owner_proof where name='catalogue'));
rollback;
