-- PostgreSQL 17 rollback proof: weekly_source_candidate_app_contract_v1
--
-- Proves the service-only MyTMS projection, partial draft, closed final-submit
-- contract, exact replay receipts, source-absent zero-hours answer, and the
-- single transaction spanning the established Candidate workflow and Weekly
-- source response owners.  All fixture rows and injected failure objects are
-- rolled back.

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

create or replace function pg_temp.assert_no_financial_keys(p_value jsonb)
returns void language plpgsql as $verify$
declare
  v_key text;
  v_child jsonb;
begin
  if pg_catalog.jsonb_typeof(p_value)='object' then
    for v_key,v_child in select key,value from pg_catalog.jsonb_each(p_value)
    loop
      if pg_catalog.lower(v_key) in (
        'pay','pay_gross','gross_pay','charge','charge_gross','invoice','invoice_id',
        'remittance','banking','banking_pay','expense','expenses','amount','vat'
      ) then
        raise exception 'VERIFY_FAILED: financial key leaked: %',v_key;
      end if;
      perform pg_temp.assert_no_financial_keys(v_child);
    end loop;
  elsif pg_catalog.jsonb_typeof(p_value)='array' then
    for v_child in select value from pg_catalog.jsonb_array_elements(p_value)
    loop
      perform pg_temp.assert_no_financial_keys(v_child);
    end loop;
  end if;
end;
$verify$;

-- The isolated Plan 6 test database does not install this unrelated legacy
-- audit helper.  Supply the exact call contract transaction-locally.
create or replace function public._audit_insert(
  p_object_type text,p_object_id_text text,p_action text,p_before_json jsonb,
  p_after_json jsonb,p_reason text,p_actor_user_id uuid
) returns void language plpgsql as $audit$
begin
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,reason
  ) values (
    p_actor_user_id,p_object_type,p_object_id_text,p_action,
    p_before_json,p_after_json,p_reason
  );
end;
$audit$;

-- Same reason as the audit helper above: the isolated Plan 6 database carries
-- the latest workflow owner but not this unchanged prerequisite from its base
-- repeatable.  Reinstall the exact helper inside this outer rollback.
create or replace function private._candidate_office_service_context_valid_v1(
  p_environment text,p_actor_user_id uuid,p_action text
) returns boolean
language plpgsql
stable
security definer
set search_path=pg_catalog,public,private,pg_temp
as $context$
declare
  v_context jsonb;
begin
  begin
    v_context:=nullif(pg_catalog.current_setting(
      'cloudtms.office_candidate_context',true
    ),'')::jsonb;
  exception when others then
    return false;
  end;
  return coalesce(v_context->>'contract_version','')='CANDIDATE_OFFICE_SERVICE_CONTEXT_V1'
    and (p_environment is null
      or v_context->>'environment'=private._candidate_assert_environment(p_environment))
    and (p_actor_user_id is null
      or v_context->>'actor_user_id'=p_actor_user_id::text)
    and (
      v_context->>'action'=pg_catalog.upper(pg_catalog.btrim(coalesce(p_action,'')))
      or (
        v_context->>'permission'='reject_submission'
        and v_context->>'action'='REJECT_EXPENSE_CATEGORY'
        and pg_catalog.upper(pg_catalog.btrim(coalesce(p_action,''))) in (
          'WORKER_SUBMIT','PAPER_PREPARE','PAPER_MANIFEST_PROMOTE'
        )
      )
    );
end;
$context$;

select pg_temp.assert_true(
  pg_catalog.to_regprocedure(
    'public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamp with time zone)'
  ) is not null,
  'request projection RPC signature is absent'
);
select pg_temp.assert_true(
  pg_catalog.to_regprocedure(
    'public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone)'
  ) is not null,
  'partial draft RPC signature is absent'
);
select pg_temp.assert_true(
  pg_catalog.to_regprocedure(
    'public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone)'
  ) is not null,
  'atomic final-submit RPC signature is absent'
);
select pg_temp.assert_true(
  pg_catalog.to_regprocedure(
    'public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamp with time zone)'
  ) is not null,
  'source-authoritative Candidate evidence owner is absent'
);

do $catalog$
declare
  v_signature text;
begin
  foreach v_signature in array array[
    'public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamp with time zone)',
    'public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone)',
    'public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone)',
    'public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamp with time zone)'
  ] loop
    if not pg_catalog.has_function_privilege('service_role',v_signature,'EXECUTE')
       or pg_catalog.has_function_privilege('anon',v_signature,'EXECUTE')
       or pg_catalog.has_function_privilege('authenticated',v_signature,'EXECUTE') then
      raise exception 'VERIFY_FAILED: RPC ACL is not service-only: %',v_signature;
    end if;
  end loop;
  if not exists(
    select 1 from pg_catalog.pg_proc procedure_row
    join pg_catalog.pg_namespace namespace_row
      on namespace_row.oid=procedure_row.pronamespace
    where namespace_row.nspname='public'
      and procedure_row.proname in (
        'weekly_source_candidate_app_request_get_v1',
        'weekly_source_candidate_app_draft_save_atomic_v1',
        'weekly_source_candidate_app_submit_atomic_v1',
        'weekly_source_candidate_check_materialise_atomic_v1'
      )
      and procedure_row.proconfig @> array['search_path=public, private, pg_catalog, pg_temp']
  ) then
    raise exception 'VERIFY_FAILED: fixed search_path is absent';
  end if;
  if not exists(
    select 1 from pg_catalog.pg_class relation_row
    join pg_catalog.pg_namespace namespace_row on namespace_row.oid=relation_row.relnamespace
    where namespace_row.nspname='public'
      and relation_row.relname='weekly_candidate_app_mutation_receipts'
      and relation_row.relrowsecurity and relation_row.relforcerowsecurity
  ) or pg_catalog.has_table_privilege('anon','public.weekly_candidate_app_mutation_receipts','SELECT')
     or pg_catalog.has_table_privilege('authenticated','public.weekly_candidate_app_mutation_receipts','SELECT') then
    raise exception 'VERIFY_FAILED: mutation receipt table is not forced-RLS/browser-denied';
  end if;
  if pg_catalog.has_function_privilege(
       'service_role',
       'private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone)',
       'EXECUTE'
     ) then
    raise exception 'VERIFY_FAILED: private Candidate comparison owner is callable';
  end if;
end;
$catalog$;

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
)
values (
  1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')
) on conflict (id) do nothing;
update public.settings_defaults
set candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  ||pg_catalog.jsonb_build_object('candidate_app_reads',true,'candidate_app_writes',true)
where id=1;

insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
values (
  'fa100000-0000-4000-8000-000000000001',
  'candidate-weekly-app-office@example.invalid','admin','not-a-real-password',
  'Candidate weekly app verifier',true
);
insert into public.clients(id,name,ts_queries_email)
values (
  'fa200000-0000-4000-8000-000000000001',
  'Candidate App Test Trust','manager@example.invalid'
);
insert into public.client_settings(
  id,client_id,effective_from,default_submission_mode,week_ending_weekday,
  hr_validation_required,autoprocess_hr,self_bill_no_invoices_sent,
  no_timesheet_required,requires_hr,is_nhsp
) values (
  'fa210000-0000-4000-8000-000000000001',
  'fa200000-0000-4000-8000-000000000001','2026-01-01','ELECTRONIC',0,
  true,false,false,false,true,false
);
insert into public.candidates(
  id,tms_ref,first_name,last_name,display_name,email,active,key_norm,opt_in_email
) values (
  'fa300000-0000-4000-8000-000000000001','CCR-90001','Casey','Nurse',
  'Casey Nurse','casey.weekly.app@example.invalid',true,'CASEY-WEEKLY-APP',true
);
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  week_ending_weekday_snapshot,default_submission_mode,role
) values (
  'fa400000-0000-4000-8000-000000000001',
  'fa300000-0000-4000-8000-000000000001',
  'fa200000-0000-4000-8000-000000000001',
  '2026-01-01','2026-12-31','PAYE','{}',0,'ELECTRONIC','RMN'
);

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time
) values (
  'fa500000-0000-4000-8000-000000000001','TEST',
  'fa000000-0000-4000-8000-000000000001',
  'CANDIDATE_APP_VERIFY','Candidate App verification Roster','ROSTER',3,'15:00'
);
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values (
  'fa500000-0000-4000-8000-000000000001',
  'fa200000-0000-4000-8000-000000000001','2026-01-01',
  'fa100000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  'fa500000-0000-4000-8000-000000000001',
  'fa200000-0000-4000-8000-000000000001','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,'manager@example.invalid',
  'fa100000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,
  projection_state
) values (
  'fa600000-0000-4000-8000-000000000001',
  'fa500000-0000-4000-8000-000000000001',
  '2026-09-06','2026-09-09 15:00:00+00','OPEN',1,'NONE'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,
  physical_row_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'fa700000-0000-4000-8000-000000000001',
  'fa600000-0000-4000-8000-000000000001','candidate-app-verify.xlsx',
  decode(repeat('71',32),'hex'),100,'34444444-4444-4444-8444-444444444444',
  'verify','verify',decode(repeat('72',32),'hex'),decode(repeat('73',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,decode(repeat('74',32),'hex'),
  'CURRENT','fa100000-0000-4000-8000-000000000001'
);
update public.weekly_source_cycles
set current_complete_upload_id='fa700000-0000-4000-8000-000000000001'
where id='fa600000-0000-4000-8000-000000000001';
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  'fa800000-0000-4000-8000-000000000001',
  'fa600000-0000-4000-8000-000000000001','CYCLE',
  'fa700000-0000-4000-8000-000000000001',1,
  decode(repeat('75',32),'hex'),decode(repeat('76',32),'hex'),'CURRENT',
  '2026-09-01 08:00:00+00'
);
update public.weekly_source_cycles
set projection_state='CURRENT',
    current_projection_publication_id='fa800000-0000-4000-8000-000000000001'
where id='fa600000-0000-4000-8000-000000000001';

insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
) values
  ('fa900000-0000-4000-8000-000000000001',
   'fa300000-0000-4000-8000-000000000001',
   'fa200000-0000-4000-8000-000000000001','2026-09-01',
   'PROFILE_EXTERNAL_KEY','candidate-app-shift-1',decode(repeat('81',32),'hex'),
   'fa500000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
  ('fa900000-0000-4000-8000-000000000002',
   'fa300000-0000-4000-8000-000000000001',
   'fa200000-0000-4000-8000-000000000001','2026-09-02',
   'PROFILE_EXTERNAL_KEY','candidate-app-shift-2',decode(repeat('82',32),'hex'),
   'fa500000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
  ('fa900000-0000-4000-8000-000000000003',
   'fa300000-0000-4000-8000-000000000001',
   'fa200000-0000-4000-8000-000000000001','2026-09-03',
   'PROFILE_EXTERNAL_KEY','candidate-app-shift-3',decode(repeat('83',32),'hex'),
   'fa500000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444');

insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
  job_title_norm,worked_start_iso,worked_end_iso,break_minutes,worked_minutes,
  week_ending_date,r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,
  line_type,actual_schedule_json,additional_units_week,
  additional_units_per_day
) values (
  'faa00000-0000-4000-8000-000000000001','CANDIDATE-APP-VERIFY',
  'casey-nurse','candidate-app-test-trust','ward-a','rmn',
  '2026-09-01 08:00:00+00','2026-09-03 19:00:00+00',90,1530,
  '2026-09-06','verify/candidate.png',repeat('a',64),
  'fa400000-0000-4000-8000-000000000001','WEEKLY','HOURS',
  '[
    {"row_key":"row-1","date":"2026-09-01","start":"09:00","end":"19:00","break_minutes":30},
    {"row_key":"row-2","date":"2026-09-02","start":"20:00","end":"08:00","break_minutes":60},
    {"row_key":"row-3","date":"2026-09-03","start":"09:00","end":"17:00","break_minutes":0}
  ]'::jsonb,'{}'::jsonb,'{}'::jsonb
);
insert into public.contract_weeks(
  id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
  day_entries_json,totals_json
) values (
  'fab00000-0000-4000-8000-000000000001',
  'fa400000-0000-4000-8000-000000000001','2026-09-06','SUBMITTED',
  'ELECTRONIC','faa00000-0000-4000-8000-000000000001',
  '[]'::jsonb,'{}'::jsonb
);
insert into public.candidate_app_accounts(
  id,environment,email_normalized,status,password_scheme,
  password_scheme_version,password_salt,password_digest,password_changed_at_utc
) values (
  'fac00000-0000-4000-8000-000000000001','TEST',
  'casey.weekly.app@example.invalid','ACTIVE','PBKDF2-HMAC-SHA256',1,
  decode(repeat('91',16),'hex'),decode(repeat('92',32),'hex'),
  '2026-09-01 08:00:00+00'
);
insert into public.candidate_app_sessions(
  id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
  expires_at_utc,absolute_expires_at_utc
) values (
  'fad00000-0000-4000-8000-000000000001',
  'fac00000-0000-4000-8000-000000000001','TEST',
  'fa300000-0000-4000-8000-000000000001','ACTIVE',
  extensions.digest('candidate-weekly-app-session','sha256'),
  '2026-09-30 00:00:00+00','2026-10-01 00:00:00+00'
);
insert into public.candidate_app_global_membership_links(
  membership_id,global_account_identity_hmac,account_id,candidate_id,
  candidate_code,membership_generation,state,linked_at_utc,updated_at_utc
) values (
  'faff0000-0000-4000-8000-000000000001',
  extensions.digest('candidate-weekly-app-membership','sha256'),
  'fac00000-0000-4000-8000-000000000001',
  'fa300000-0000-4000-8000-000000000001','CCR-90001',1,'ACTIVE',
  '2026-09-01 08:00:00+00','2026-09-01 08:00:00+00'
);
create temp table candidate_app_workbench_jobs_before as
select workbench_job.*
from public.banking_pay_workbench_jobs workbench_job
where workbench_job.candidate_id='fa300000-0000-4000-8000-000000000001';

do $runtime$
declare
  v_now timestamptz:='2026-09-02 10:00:00+00';
  v_sync jsonb;
  v_ask jsonb;
  v_generation uuid;
  v_request jsonb;
  v_scope jsonb;
  v_issue_1 jsonb;
  v_issue_2 jsonb;
  v_issue_3 jsonb;
  v_partial_body jsonb;
  v_partial jsonb;
  v_replay jsonb;
  v_final_body jsonb;
  v_final jsonb;
  v_material_request jsonb;
  v_material_replay jsonb;
  v_workflow_create jsonb;
  v_workflow uuid:='fae00000-0000-4000-8000-000000000001';
  v_signature uuid:='faf00000-0000-4000-8000-000000000001';
  v_injected boolean:=false;
  v_rejected boolean:=false;
  v_conflict boolean:=false;
  v_blocked boolean:=false;
  v_block_error text;
begin
  v_sync:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','fa100000-0000-4000-8000-000000000001',
    'source_cycle_id','fa600000-0000-4000-8000-000000000001',
    'projection_publication_id','fa800000-0000-4000-8000-000000000001',
    'issues',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'work_event_id','fa900000-0000-4000-8000-000000000001',
        'candidate_timesheet_id','faa00000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('a1',32),
        'contract_id','fa400000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-01 09:00',
        'candidate_end_at_local','2026-09-01 19:00','candidate_break_minutes',30,
        'system_start_at_local','2026-09-01 09:00',
        'system_end_at_local','2026-09-01 17:00','system_break_minutes',30
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','fa900000-0000-4000-8000-000000000002',
        'candidate_timesheet_id','faa00000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('a2',32),
        'contract_id','fa400000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_MISSING_OR_NOT_AUTHORISED','source_presence','ABSENT',
        'candidate_start_at_local','2026-09-02 20:00',
        'candidate_end_at_local','2026-09-03 08:00','candidate_break_minutes',60
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','fa900000-0000-4000-8000-000000000003',
        'candidate_timesheet_id','faa00000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('a3',32),
        'contract_id','fa400000-0000-4000-8000-000000000001',
        'issue_family','HEALTHROSTER_NOT_FINALISED','source_presence','UNFINALISED',
        'candidate_start_at_local','2026-09-03 09:00',
        'candidate_end_at_local','2026-09-03 17:00','candidate_break_minutes',0
      )
    )
  ));
  perform pg_temp.assert_true((v_sync->>'new_incidents')::integer=3,
    'three comparison issues were not created');

  v_ask:=public.weekly_source_query_ask_candidate_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','fa100000-0000-4000-8000-000000000001',
      'source_cycle_id','fa600000-0000-4000-8000-000000000001',
      'projection_publication_id','fa800000-0000-4000-8000-000000000001',
      'candidate_id','fa300000-0000-4000-8000-000000000001',
      'client_id','fa200000-0000-4000-8000-000000000001',
      'incident_ids',(
        select pg_catalog.jsonb_agg(id order by id)
        from public.weekly_discrepancy_incidents
        where source_cycle_id='fa600000-0000-4000-8000-000000000001'
      )
    )
  );
  v_generation:=(v_ask->>'candidate_generation_id')::uuid;

  -- Switch the fixture to a valid Roster source-authority route only after the
  -- server has created the request. This keeps notification setup independent
  -- while proving the Candidate workflow admission against the real route.
  update public.client_settings
  set autoprocess_hr=true,no_timesheet_required=true,requires_hr=false
  where id='fa210000-0000-4000-8000-000000000001';

  v_request:=public.weekly_source_candidate_app_request_get_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_now
  );
  perform private.weekly_source_candidate_app_assert_request_shape_v1(v_request);
  perform pg_temp.assert_no_financial_keys(v_request);
  perform pg_temp.assert_true(
    (select pg_catalog.array_agg(key order by key)
     from pg_catalog.jsonb_object_keys(v_request) key)
    =array['earliest_outstanding_scope_id','ok','request_fingerprint','request_id',
           'request_version','scopes','state'],
    'request projection has an open or missing top-level field'
  );
  v_scope:=v_request#>'{scopes,0}';
  perform pg_temp.assert_true(
    v_scope->>'request_kind'='CHECK_HOURS'
    and pg_catalog.jsonb_array_length(v_scope->'issues')=3
    and pg_catalog.jsonb_array_length(v_scope->'submitted_timesheet')=3,
    'request projection did not preserve the exact week and three issues'
  );
  select value into strict v_issue_1 from pg_catalog.jsonb_array_elements(v_scope->'issues')
  where value->>'date'='2026-09-01';
  select value into strict v_issue_2 from pg_catalog.jsonb_array_elements(v_scope->'issues')
  where value->>'date'='2026-09-02';
  select value into strict v_issue_3 from pg_catalog.jsonb_array_elements(v_scope->'issues')
  where value->>'date'='2026-09-03';
  perform pg_temp.assert_true(
    v_issue_2->>'issue_family'='SOURCE_ABSENT'
    and v_issue_2#>>'{system_hours,worked}'='false'
    and v_issue_3->>'issue_family'='HEALTHROSTER_NOT_FINALISED',
    'source-absent/unfinalised facts were not projected distinctly'
  );

  -- HEALTHROSTER_NOT_FINALISED is not an authoritative zero-hours answer.
  begin
    perform private.weekly_source_candidate_app_responses_v1(
      v_scope,pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'issue_id',v_issue_3->>'issue_id',
        'issue_fingerprint',v_issue_3->>'issue_fingerprint',
        'answer_code','MY_HOURS_WRONG_SYSTEM_CORRECT'
      )),false
    );
  exception when sqlstate '22023' then
    v_rejected:=sqlerrm='WEEKLY_SOURCE_CANDIDATE_SYSTEM_HOURS_UNAVAILABLE';
  end;
  perform pg_temp.assert_true(v_rejected,
    'unfinalised system-correct answer was not rejected closed');

  -- Save only one of three open answers.  This is intentionally partial.
  v_partial_body:=pg_catalog.jsonb_build_object(
    'request_version',v_request->'request_version',
    'request_fingerprint',v_request->>'request_fingerprint',
    'scope_id',v_scope->>'scope_id','scope_version',v_scope->'scope_version',
    'scope_fingerprint',v_scope->>'scope_fingerprint',
    'responses',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'issue_id',v_issue_1->>'issue_id',
      'issue_fingerprint',v_issue_1->>'issue_fingerprint',
      'answer_code','MY_HOURS_CORRECT'
    )),
    'idempotency_key','fb000000-0000-4000-8000-000000000001'
  );
  v_partial:=public.weekly_source_candidate_app_draft_save_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,
    v_partial_body,v_now
  );
  perform pg_temp.assert_true(
    v_partial#>>'{scopes,0,completion_state}'='OUTSTANDING'
    and (select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(
      v_partial#>'{scopes,0,issues}'
    ) issue where issue->'saved_draft_response'<>'null'::jsonb)=1,
    'partial draft did not preserve exactly one answer'
  );
  -- Exact replay is checked before the mutable ACTIVE-state guard.
  update public.weekly_candidate_outreach_generations
  set state='COMPLETE' where id=v_generation;
  v_replay:=public.weekly_source_candidate_app_draft_save_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,
    v_partial_body,v_now+interval '1 minute'
  );
  perform pg_temp.assert_true(v_replay=v_partial,
    'draft exact replay did not return the stored response before mutable guards');
  update public.weekly_candidate_outreach_generations
  set state='ACTIVE' where id=v_generation;

  -- Changed payload under the same key must conflict.
  begin
    perform public.weekly_source_candidate_app_draft_save_atomic_v1(
      'fad00000-0000-4000-8000-000000000001','TEST',v_generation,
      pg_catalog.jsonb_set(v_partial_body,'{responses}','[]'::jsonb),v_now
    );
  exception when unique_violation then
    v_conflict:=sqlerrm='WEEKLY_SOURCE_CANDIDATE_REPLAY_CONFLICT';
  end;
  perform pg_temp.assert_true(v_conflict,'changed draft replay did not conflict');

  -- Final submit requires every current open issue, even though draft save did not.
  v_request:=v_partial;
  v_scope:=v_request#>'{scopes,0}';
  select value into strict v_issue_1 from pg_catalog.jsonb_array_elements(v_scope->'issues')
  where value->>'date'='2026-09-01';
  select value into strict v_issue_2 from pg_catalog.jsonb_array_elements(v_scope->'issues')
  where value->>'date'='2026-09-02';
  select value into strict v_issue_3 from pg_catalog.jsonb_array_elements(v_scope->'issues')
  where value->>'date'='2026-09-03';
  begin
    perform private.weekly_source_candidate_app_responses_v1(
      v_scope,pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'issue_id',v_issue_1->>'issue_id',
          'issue_fingerprint',v_issue_1->>'issue_fingerprint',
          'answer_code','MY_HOURS_CORRECT'
        ),
        pg_catalog.jsonb_build_object(
          'issue_id',v_issue_2->>'issue_id',
          'issue_fingerprint',v_issue_2->>'issue_fingerprint',
          'answer_code','MY_HOURS_WRONG_SYSTEM_CORRECT'
        )
      ),true
    );
    raise exception 'VERIFY_FAILED: incomplete final response unexpectedly passed';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CANDIDATE_ALL_OPEN_ISSUES_REQUIRED' then raise; end if;
  end;

  v_workflow_create:=public.candidate_workflow_transition_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_workflow,'CREATE',null,
    pg_catalog.jsonb_build_object(
      'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
      'contract_id','fa400000-0000-4000-8000-000000000001',
      'contract_week_id','fab00000-0000-4000-8000-000000000001',
      'week_ending_date','2026-09-06',
      'target_timesheet_id','faa00000-0000-4000-8000-000000000001',
      'input_snapshot','{}'::jsonb
    ),'candidate-weekly-app-workflow',v_now
  );
  perform pg_temp.assert_true(
    v_workflow_create->>'workflow_id'=v_workflow::text
    and v_workflow_create->>'state'='WORKER_DRAFT'
    and (v_workflow_create->>'generation')::integer=1,
    'active CHECK_HOURS request did not admit the real Candidate workflow create path'
  );
  update public.client_settings
  set autoprocess_hr=false,no_timesheet_required=false,requires_hr=true
  where id='fa210000-0000-4000-8000-000000000001';
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    document_role,state,storage_key,media_type,byte_size,source_content_sha256,
    immutable_at_utc,required,review_ordinal,review_render_state,
    final_signed_render_state,created_at_utc
  ) values (
    v_signature,v_workflow,1,1,'faa00000-0000-4000-8000-000000000001',
    'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE','IMMUTABLE',
    'verify/candidate-signature.png','image/png',256,decode(repeat('b1',32),'hex'),
    v_now,false,null,'NOT_REQUIRED','NOT_REQUIRED',v_now
  );

  v_final_body:=pg_catalog.jsonb_build_object(
    'submission_kind','WHOLE_WEEK_REVISION',
    'request_version',v_request->'request_version',
    'request_fingerprint',v_request->>'request_fingerprint',
    'scope_id',v_scope->>'scope_id','scope_version',v_scope->'scope_version',
    'scope_fingerprint',v_scope->>'scope_fingerprint',
    'workflow_id',v_workflow,'generation',1,
    'candidate_signature_component_id',v_signature,
    'candidate_signed_at_utc',v_now,
    'immutable_submission',pg_catalog.jsonb_build_object(
      'actual_schedule_json',pg_catalog.jsonb_build_array(
        v_scope#>'{submitted_timesheet,0}',v_scope#>'{submitted_timesheet,2}'
      ),
      'additional_units_week','{}'::jsonb,
      'additional_units_per_day','{}'::jsonb,
      'timesheet_patch_json',pg_catalog.jsonb_build_object(
        'actual_schedule_json',pg_catalog.jsonb_build_array(
          v_scope#>'{submitted_timesheet,0}',v_scope#>'{submitted_timesheet,2}'
        ),
        'additional_units_week','{}'::jsonb,
        'additional_units_per_day','{}'::jsonb
      )
    ),
    'responses',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'issue_id',v_issue_1->>'issue_id',
        'issue_fingerprint',v_issue_1->>'issue_fingerprint',
        'answer_code','MY_HOURS_CORRECT'
      ),
      pg_catalog.jsonb_build_object(
        'issue_id',v_issue_2->>'issue_id',
        'issue_fingerprint',v_issue_2->>'issue_fingerprint',
        'answer_code','MY_HOURS_WRONG_SYSTEM_CORRECT'
      ),
      pg_catalog.jsonb_build_object(
        'issue_id',v_issue_3->>'issue_id',
        'issue_fingerprint',v_issue_3->>'issue_fingerprint',
        'answer_code','MY_HOURS_CORRECT'
      )
    ),
    'idempotency_key','fb000000-0000-4000-8000-000000000002'
  );

  -- A Candidate must not be able to save against a Timesheet revision that
  -- changed after the exact request/scope projection they reviewed.  The
  -- exception block rolls the injected version change back before continuing.
  v_rejected:=false;
  begin
    update public.timesheets
    set version=version+1,updated_at=v_now+interval '1 second'
    where timesheet_id='faa00000-0000-4000-8000-000000000001';
    perform public.weekly_source_candidate_app_submit_atomic_v1(
      'fad00000-0000-4000-8000-000000000001','TEST',v_generation,
      v_final_body,v_now
    );
  exception when sqlstate '40001' then
    v_rejected:=sqlerrm in (
      'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE',
      'WEEKLY_SOURCE_CANDIDATE_SCOPE_STALE'
    );
  end;
  perform pg_temp.assert_true(v_rejected,
    'stale Candidate Timesheet revision was accepted');

  -- Inject a response-owner failure after the workflow owner runs.  The caught
  -- failure must leave neither half committed.
  create or replace function pg_temp.fail_weekly_candidate_response_insert()
  returns trigger language plpgsql as $failure$
  begin
    raise exception 'WEEKLY_CANDIDATE_APP_VERIFY_INJECTED' using errcode='Z2203';
  end;
  $failure$;
  create trigger weekly_candidate_app_verify_failure
  before insert on public.weekly_candidate_response_draft_items
  for each row execute function pg_temp.fail_weekly_candidate_response_insert();
  begin
    perform public.weekly_source_candidate_app_submit_atomic_v1(
      'fad00000-0000-4000-8000-000000000001','TEST',v_generation,
      v_final_body,v_now
    );
  exception when sqlstate 'Z2203' then
    v_injected:=sqlerrm='WEEKLY_CANDIDATE_APP_VERIFY_INJECTED';
  end;
  perform pg_temp.assert_true(v_injected,'injected downstream failure was not reached');
  perform pg_temp.assert_true(exists(
    select 1 from public.candidate_submission_workflows
    where id=v_workflow and state='WORKER_DRAFT' and generation=1
  ),'workflow mutation survived downstream response-owner failure');
  perform pg_temp.assert_true(not exists(
    select 1 from public.weekly_candidate_app_mutation_receipts
    where idempotency_key='fb000000-0000-4000-8000-000000000002'
  ),'failed atomic submit wrote a receipt');
  drop trigger weekly_candidate_app_verify_failure
    on public.weekly_candidate_response_draft_items;

  v_final:=public.weekly_source_candidate_app_submit_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,
    v_final_body,v_now
  );
  perform pg_temp.assert_true(
    v_final->>'ok'='true' and v_final->>'idempotent_replay'='false'
    and (v_final->>'processed_scope_id')::uuid=(v_scope->>'scope_id')::uuid
    and (v_final->>'workflow_id')::uuid=v_workflow
    and (v_final->>'generation')::integer=2,
    'final-submit receipt does not match the app contract'
  );
  v_material_request:=pg_catalog.jsonb_build_object(
    'account_id','fac00000-0000-4000-8000-000000000001',
    'candidate_id','fa300000-0000-4000-8000-000000000001',
    'candidate_generation_id',v_generation,
    'projection_publication_id','fa800000-0000-4000-8000-000000000001',
    'request_version',(v_final_body->>'request_version')::integer,
    'request_fingerprint',v_final_body->>'request_fingerprint',
    'scope_id',(v_final_body->>'scope_id')::uuid,
    'scope_version',(v_final_body->>'scope_version')::integer,
    'scope_fingerprint',v_final_body->>'scope_fingerprint',
    'request_kind','CHECK_HOURS','workflow_id',v_workflow,
    'expected_workflow_generation',1,
    'candidate_signature_component_id',v_signature,
    'candidate_signed_at_utc',v_now,
    'immutable_submission',v_final_body->'immutable_submission',
    'request_idempotency_key','fb000000-0000-4000-8000-000000000002'
  );
  v_material_replay:=public.weekly_source_candidate_check_materialise_atomic_v1(
    v_material_request,v_now+interval '30 seconds'
  );
  perform pg_temp.assert_true(
    v_material_replay->>'idempotent_replay'='true'
    and (v_material_replay->>'workflow_id')::uuid=v_workflow
    and (v_material_replay->>'timesheet_id')::uuid=
      'faa00000-0000-4000-8000-000000000001',
    'service-owned materialisation exact replay was not stable'
  );
  v_conflict:=false;
  begin
    perform public.weekly_source_candidate_check_materialise_atomic_v1(
      pg_catalog.jsonb_set(
        v_material_request,'{candidate_signed_at_utc}',
        pg_catalog.to_jsonb(v_now+interval '1 second'),false
      ),v_now+interval '30 seconds'
    );
  exception when unique_violation then
    v_conflict:=sqlerrm='WEEKLY_SOURCE_CANDIDATE_MATERIALISATION_REPLAY_CONFLICT';
  end;
  perform pg_temp.assert_true(v_conflict,
    'materialisation idempotency key accepted a different immutable request');
  perform pg_temp.assert_true(
    (select pg_catalog.array_agg(key order by key)
     from pg_catalog.jsonb_object_keys(v_final) key)
    =array['generation','idempotent_replay','next_outstanding_scope_id','ok',
           'processed_scope_id','request','workflow_id'],
    'final response has an open or missing top-level field'
  );
  perform pg_temp.assert_no_financial_keys(v_final);
  perform pg_temp.assert_true(exists(
    select 1 from public.candidate_submission_workflows
    where id=v_workflow and generation=2
      and state='WORKER_SUBMITTED'
      and manager_signature_component_id is null
      and manager_approved_at_utc is null
      and canonical_save_financials_id is null
  ),'source-authoritative Candidate evidence owner did not accept the signed week');
  perform pg_temp.assert_true(exists(
    select 1 from public.timesheets timesheet
    where timesheet.timesheet_id='faa00000-0000-4000-8000-000000000001'
      and timesheet.authorised_at_server is null
      and timesheet.submission_mode='MANUAL'
      and timesheet.candidate_workflow_id=v_workflow
      and timesheet.candidate_workflow_generation=2
      and timesheet.r2_nurse_key='verify/candidate-signature.png'
      and timesheet.img_sha256_nurse=repeat('b1',32)
      and pg_catalog.jsonb_array_length(timesheet.actual_schedule_json)=2
  ),'ordinary unapproved Candidate comparison evidence was not saved exactly');
  perform pg_temp.assert_true(exists(
    select 1 from public.contract_weeks week_row
    where week_row.id='fab00000-0000-4000-8000-000000000001'
      and week_row.timesheet_id='faa00000-0000-4000-8000-000000000001'
      and week_row.submission_mode_snapshot='ELECTRONIC'
  ),'Candidate-submitted comparison evidence did not retain the electronic route');
  perform pg_temp.assert_true(exists(
    select 1 from public.candidate_submission_components component
    where component.workflow_id=v_workflow and component.workflow_generation=2
      and component.component_kind='CANDIDATE_SIGNATURE'
      and component.document_role='CANDIDATE_SIGNATURE'
      and component.state='IMMUTABLE'
      and component.source_component_id=v_signature
      and component.source_content_sha256=decode(repeat('b1',32),'hex')
  ),'immutable Candidate signature was not bound to the saved evidence generation');
  perform pg_temp.assert_true(exists(
    select 1 from public.candidate_submission_workflows workflow_row
    join public.candidate_submission_components component
      on component.id=workflow_row.candidate_signature_component_id
    where workflow_row.id=v_workflow and workflow_row.generation=2
      and workflow_row.candidate_signature_sha256=component.source_content_sha256
      and component.timesheet_id='faa00000-0000-4000-8000-000000000001'
  ),'saved workflow did not retain the exact immutable Candidate signature hash');
  perform pg_temp.assert_true(not exists(
    select 1 from public.candidate_submission_components component
    where component.workflow_id=v_workflow and component.workflow_generation=2
      and (component.document_role='MANAGER_SIGNATURE'
           or component.review_render_state='PENDING'
           or component.final_signed_render_state='PENDING')
  ),'source-authoritative Candidate submission created manager-review evidence');
  perform pg_temp.assert_true(not exists(
    select 1 from public.timesheets_financials financial
    where financial.timesheet_id='faa00000-0000-4000-8000-000000000001'
  ),'Candidate comparison evidence created TSFIN/payment economics');
  perform pg_temp.assert_true(not exists(
    select 1 from public.invoice_lines invoice_line
    where invoice_line.timesheet_id='faa00000-0000-4000-8000-000000000001'
  ),'Candidate comparison evidence created invoice state');
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    where incident.work_event_id='fa900000-0000-4000-8000-000000000002'
      and comparison.source_presence='ABSENT'
      and incident.state='RESOLVED'
  ),'source-absent system-correct answer was not stored as zero hours');

  -- The exact final replay returns the stored outcome before the now-complete
  -- outreach/workflow mutable guards.  Reusing the key across a mutation kind
  -- or request identity remains a hard conflict.
  v_replay:=public.weekly_source_candidate_app_submit_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,
    v_final_body,v_now+interval '1 minute'
  );
  perform pg_temp.assert_true(
    v_replay->>'idempotent_replay'='true'
    and (v_replay-'idempotent_replay')=(v_final-'idempotent_replay'),
    'final exact replay was not receipt-first/exact'
  );
  v_conflict:=false;
  begin
    perform private.weekly_source_candidate_app_receipt_v1(
      'fa300000-0000-4000-8000-000000000001',v_generation,'DRAFT_SAVE',
      'fb000000-0000-4000-8000-000000000002',
      decode(repeat('00',32),'hex')
    );
  exception when unique_violation then
    v_conflict:=sqlerrm='WEEKLY_SOURCE_CANDIDATE_REPLAY_CONFLICT';
  end;
  perform pg_temp.assert_true(v_conflict,'cross-kind receipt reuse did not conflict');
  v_conflict:=false;
  begin
    perform private.weekly_source_candidate_app_receipt_v1(
      'fa300000-0000-4000-8000-000000000099',v_generation,'FINAL_SUBMIT',
      'fb000000-0000-4000-8000-000000000002',
      (select request_hash from public.weekly_candidate_app_mutation_receipts
       where idempotency_key='fb000000-0000-4000-8000-000000000002')
    );
  exception when unique_violation then
    v_conflict:=sqlerrm='WEEKLY_SOURCE_CANDIDATE_REPLAY_CONFLICT';
  end;
  perform pg_temp.assert_true(v_conflict,'cross-candidate receipt reuse did not conflict');
  v_conflict:=false;
  begin
    perform private.weekly_source_candidate_app_receipt_v1(
      'fa300000-0000-4000-8000-000000000001',
      'fa600000-0000-4000-8000-000000000099','FINAL_SUBMIT',
      'fb000000-0000-4000-8000-000000000002',
      (select request_hash from public.weekly_candidate_app_mutation_receipts
       where idempotency_key='fb000000-0000-4000-8000-000000000002')
    );
  exception when unique_violation then
    v_conflict:=sqlerrm='WEEKLY_SOURCE_CANDIDATE_REPLAY_CONFLICT';
  end;
  perform pg_temp.assert_true(v_conflict,'cross-generation receipt reuse did not conflict');

  -- A late Timesheet request spans three earlier complete weeks so the same
  -- transaction proves exact match, hours mismatch and a Candidate-only shift
  -- that is missing from the current complete source.
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,
    source_candidate_identity,source_client_identity,work_date,
    start_at_local,end_at_local,break_minutes,actual_net_minutes,
    row_finalisation_state,normalised_row_hash
  ) values
    ('fc000000-0000-4000-8000-000000000001',
     'fa700000-0000-4000-8000-000000000001',1,'late-source-exact',
     'CCR-90001','Candidate App Test Trust','2026-08-10',
     '2026-08-10 09:00','2026-08-10 17:00',30,450,'SOURCE_WORKED',
     decode(repeat('c1',32),'hex')),
    ('fc000000-0000-4000-8000-000000000002',
     'fa700000-0000-4000-8000-000000000001',2,'late-source-mismatch',
     'CCR-90001','Candidate App Test Trust','2026-08-17',
     '2026-08-17 09:00','2026-08-17 17:00',30,450,'SOURCE_WORKED',
     decode(repeat('c2',32),'hex')),
    ('fc000000-0000-4000-8000-000000000003',
     'fa700000-0000-4000-8000-000000000001',3,'late-source-missing',
     'CCR-90001','Candidate App Test Trust','2026-08-24',
     '2026-08-24 09:00','2026-08-24 17:00',30,450,'SOURCE_WORKED',
     decode(repeat('c3',32),'hex'));
  insert into public.weekly_work_events(
    id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
    durable_identity_hash,first_source_group_id,source_format_profile_id
  ) values
    ('fc100000-0000-4000-8000-000000000001',
     'fa300000-0000-4000-8000-000000000001',
     'fa200000-0000-4000-8000-000000000001','2026-08-10',
     'PROFILE_EXTERNAL_KEY','late-source-exact',decode(repeat('d1',32),'hex'),
     'fa500000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
    ('fc100000-0000-4000-8000-000000000002',
     'fa300000-0000-4000-8000-000000000001',
     'fa200000-0000-4000-8000-000000000001','2026-08-17',
     'PROFILE_EXTERNAL_KEY','late-source-mismatch',decode(repeat('d2',32),'hex'),
     'fa500000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
    ('fc100000-0000-4000-8000-000000000003',
     'fa300000-0000-4000-8000-000000000001',
     'fa200000-0000-4000-8000-000000000001','2026-08-24',
     'PROFILE_EXTERNAL_KEY','late-source-missing',decode(repeat('d3',32),'hex'),
     'fa500000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444');
  insert into public.weekly_source_row_resolutions(
    id,upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,
    paid_minutes,rate_classifications_json,mapping_state,contract_selection_method,
    work_event_match_kind,work_event_match_fingerprint,
    qualification_profile_fingerprint,qualifying_contract_count,
    qualifying_contract_set_hash,source_row_fingerprint,
    contract_and_rate_fingerprint,effective_policy_fingerprint
  ) values
    ('fc200000-0000-4000-8000-000000000001',
     'fc000000-0000-4000-8000-000000000001',1,
     'fa300000-0000-4000-8000-000000000001',
     'fa200000-0000-4000-8000-000000000001',
     'fa400000-0000-4000-8000-000000000001',
     'fc100000-0000-4000-8000-000000000001',450,'{}','RESOLVED','AUTO_UNIQUE',
     'NEW_PROFILE_KEY',decode(repeat('e1',32),'hex'),decode(repeat('e2',32),'hex'),
     1,decode(repeat('e3',32),'hex'),decode(repeat('c1',32),'hex'),
     decode(repeat('e4',32),'hex'),decode(repeat('e5',32),'hex')),
    ('fc200000-0000-4000-8000-000000000002',
     'fc000000-0000-4000-8000-000000000002',1,
     'fa300000-0000-4000-8000-000000000001',
     'fa200000-0000-4000-8000-000000000001',
     'fa400000-0000-4000-8000-000000000001',
     'fc100000-0000-4000-8000-000000000002',450,'{}','RESOLVED','AUTO_UNIQUE',
     'NEW_PROFILE_KEY',decode(repeat('e6',32),'hex'),decode(repeat('e7',32),'hex'),
     1,decode(repeat('e8',32),'hex'),decode(repeat('c2',32),'hex'),
     decode(repeat('e9',32),'hex'),decode(repeat('ea',32),'hex')),
    ('fc200000-0000-4000-8000-000000000003',
     'fc000000-0000-4000-8000-000000000003',1,
     'fa300000-0000-4000-8000-000000000001',
     'fa200000-0000-4000-8000-000000000001',
     'fa400000-0000-4000-8000-000000000001',
     'fc100000-0000-4000-8000-000000000003',450,'{}','RESOLVED','AUTO_UNIQUE',
     'NEW_PROFILE_KEY',decode(repeat('eb',32),'hex'),decode(repeat('ec',32),'hex'),
     1,decode(repeat('ed',32),'hex'),decode(repeat('c3',32),'hex'),
     decode(repeat('ee',32),'hex'),decode(repeat('ef',32),'hex'));
  insert into public.weekly_work_event_source_links(
    id,work_event_id,upload_row_id,row_resolution_id,link_kind,link_hash
  ) values
    ('fc300000-0000-4000-8000-000000000001',
     'fc100000-0000-4000-8000-000000000001',
     'fc000000-0000-4000-8000-000000000001',
     'fc200000-0000-4000-8000-000000000001','PROVISIONAL_SOURCE',
     decode(repeat('f1',32),'hex')),
    ('fc300000-0000-4000-8000-000000000002',
     'fc100000-0000-4000-8000-000000000002',
     'fc000000-0000-4000-8000-000000000002',
     'fc200000-0000-4000-8000-000000000002','PROVISIONAL_SOURCE',
     decode(repeat('f2',32),'hex')),
    ('fc300000-0000-4000-8000-000000000003',
     'fc100000-0000-4000-8000-000000000003',
     'fc000000-0000-4000-8000-000000000003',
     'fc200000-0000-4000-8000-000000000003','FULL_NEGATIVE_SOURCE',
     decode(repeat('f3',32),'hex'));
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,
    day_entries_json,totals_json
  ) values
    ('fc400000-0000-4000-8000-000000000001',
     'fa400000-0000-4000-8000-000000000001','2026-08-16','OPEN','MANUAL','[]','{}'),
    ('fc400000-0000-4000-8000-000000000002',
     'fa400000-0000-4000-8000-000000000001','2026-08-23','OPEN','MANUAL','[]','{}'),
    ('fc400000-0000-4000-8000-000000000003',
     'fa400000-0000-4000-8000-000000000001','2026-08-30','OPEN','MANUAL','[]','{}');

  v_ask:=public.weekly_source_timesheet_submission_request_start_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','fa100000-0000-4000-8000-000000000001',
      'source_cycle_id','fa600000-0000-4000-8000-000000000001',
      'projection_publication_id','fa800000-0000-4000-8000-000000000001',
      'candidate_id','fa300000-0000-4000-8000-000000000001',
      'scopes',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'week_ending','2026-08-16','client_id','fa200000-0000-4000-8000-000000000001',
          'contract_id','fa400000-0000-4000-8000-000000000001',
          'expected_source_fingerprint',repeat('11',32)
        ),
        pg_catalog.jsonb_build_object(
          'week_ending','2026-08-23','client_id','fa200000-0000-4000-8000-000000000001',
          'contract_id','fa400000-0000-4000-8000-000000000001',
          'expected_source_fingerprint',repeat('22',32)
        ),
        pg_catalog.jsonb_build_object(
          'week_ending','2026-08-30','client_id','fa200000-0000-4000-8000-000000000001',
          'contract_id','fa400000-0000-4000-8000-000000000001',
          'expected_source_fingerprint',repeat('33',32)
        )
      )
    )
  );
  v_generation:=(v_ask->>'candidate_generation_id')::uuid;
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,
    day_entries_json,totals_json
  ) values (
    'fc400000-0000-4000-8000-000000000004',
    'fa400000-0000-4000-8000-000000000001','2026-08-09','OPEN','MANUAL','[]','{}'
  );
  update public.client_settings
  set autoprocess_hr=true,no_timesheet_required=true,requires_hr=false
  where id='fa210000-0000-4000-8000-000000000001';

  -- Import-authoritative hours are not a general editor.  The fourth week is
  -- deliberately outside the exact three-scope server request and must remain
  -- closed even though the same Candidate and Contract have an active request.
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      'fad00000-0000-4000-8000-000000000001','TEST',
      'fc500000-0000-4000-8000-000000000010','CREATE',null,
      pg_catalog.jsonb_build_object(
        'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
        'contract_id','fa400000-0000-4000-8000-000000000001',
        'contract_week_id','fc400000-0000-4000-8000-000000000004',
        'week_ending_date','2026-08-09','input_snapshot','{}'::jsonb
      ),'weekly-source-no-request-must-fail',v_now
    );
  exception when sqlstate '55000' then
    v_block_error:=sqlerrm;
    v_blocked:=sqlerrm='CANDIDATE_RECORD_VIEW_ONLY';
  end;
  perform pg_temp.assert_true(v_blocked,
    'import-authoritative Candidate hours opened outside the server-owned request; result='
      ||coalesce(v_block_error,'NO_ERROR'));
  v_workflow_create:=public.candidate_workflow_transition_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',
    'fc500000-0000-4000-8000-000000000001','CREATE',null,
    pg_catalog.jsonb_build_object(
      'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
      'contract_id','fa400000-0000-4000-8000-000000000001',
      'contract_week_id','fc400000-0000-4000-8000-000000000001',
      'week_ending_date','2026-08-16','input_snapshot','{}'::jsonb
    ),'late-submit-exact',v_now
  );
  perform pg_temp.assert_true(v_workflow_create->>'state'='WORKER_DRAFT',
    'first SUBMIT_TIMESHEET scope did not admit the real workflow create path');
  v_workflow_create:=public.candidate_workflow_transition_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',
    'fc500000-0000-4000-8000-000000000002','CREATE',null,
    pg_catalog.jsonb_build_object(
      'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
      'contract_id','fa400000-0000-4000-8000-000000000001',
      'contract_week_id','fc400000-0000-4000-8000-000000000002',
      'week_ending_date','2026-08-23','input_snapshot','{}'::jsonb
    ),'late-submit-mismatch',v_now
  );
  perform pg_temp.assert_true(v_workflow_create->>'state'='WORKER_DRAFT',
    'second SUBMIT_TIMESHEET scope did not admit the real workflow create path');
  v_workflow_create:=public.candidate_workflow_transition_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',
    'fc500000-0000-4000-8000-000000000003','CREATE',null,
    pg_catalog.jsonb_build_object(
      'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
      'contract_id','fa400000-0000-4000-8000-000000000001',
      'contract_week_id','fc400000-0000-4000-8000-000000000003',
      'week_ending_date','2026-08-30','input_snapshot','{}'::jsonb
    ),'late-submit-missing',v_now
  );
  perform pg_temp.assert_true(v_workflow_create->>'state'='WORKER_DRAFT',
    'third SUBMIT_TIMESHEET scope did not admit the real workflow create path');
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,component_kind,document_role,
    state,storage_key,media_type,byte_size,source_content_sha256,immutable_at_utc,
    required,review_render_state,final_signed_render_state,created_at_utc
  ) values
    ('fc600000-0000-4000-8000-000000000001','fc500000-0000-4000-8000-000000000001',
     1,1,'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE','IMMUTABLE',
     'verify/late-exact.png','image/png',256,decode(repeat('41',32),'hex'),v_now,
     false,'NOT_REQUIRED','NOT_REQUIRED',v_now),
    ('fc600000-0000-4000-8000-000000000002','fc500000-0000-4000-8000-000000000002',
     1,1,'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE','IMMUTABLE',
     'verify/late-mismatch.png','image/png',256,decode(repeat('42',32),'hex'),v_now,
     false,'NOT_REQUIRED','NOT_REQUIRED',v_now),
    ('fc600000-0000-4000-8000-000000000003','fc500000-0000-4000-8000-000000000003',
     1,1,'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE','IMMUTABLE',
     'verify/late-missing.png','image/png',256,decode(repeat('43',32),'hex'),v_now,
     false,'NOT_REQUIRED','NOT_REQUIRED',v_now);

  -- Exact current source comparison auto-completes without a manager path.
  v_request:=public.weekly_source_candidate_app_request_get_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_now
  );
  select value into strict v_scope from pg_catalog.jsonb_array_elements(v_request->'scopes')
  where value->>'week_ending_date'='2026-08-16';
  v_final_body:=pg_catalog.jsonb_build_object(
    'submission_kind','WHOLE_WEEK_REVISION','request_version',v_request->'request_version',
    'request_fingerprint',v_request->>'request_fingerprint','scope_id',v_scope->>'scope_id',
    'scope_version',v_scope->'scope_version','scope_fingerprint',v_scope->>'scope_fingerprint',
    'workflow_id','fc500000-0000-4000-8000-000000000001','generation',1,
    'candidate_signature_component_id','fc600000-0000-4000-8000-000000000001',
    'candidate_signed_at_utc',v_now,'immutable_submission',pg_catalog.jsonb_build_object(
      'actual_schedule_json',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'row_key','late-exact','date','2026-08-10','start','09:00','end','17:00',
        'break_minutes',30
      )),'additional_units_week','{}'::jsonb,'additional_units_per_day','{}'::jsonb
    ),'responses','[]'::jsonb,
    'idempotency_key','fc700000-0000-4000-8000-000000000001'
  );
  update public.weekly_timesheet_submission_request_memberships
  set expected_source_fingerprint=decode(repeat('99',32),'hex')
  where id=(v_scope->>'scope_id')::uuid;
  v_rejected:=false;
  begin
    perform public.weekly_source_candidate_app_submit_atomic_v1(
      'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_final_body,v_now
    );
  exception when sqlstate '40001' then
    v_rejected:=sqlerrm in (
      'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE','WEEKLY_SOURCE_CANDIDATE_SCOPE_STALE'
    );
  end;
  perform pg_temp.assert_true(v_rejected,'stale late-submit source scope was accepted');
  update public.weekly_timesheet_submission_request_memberships
  set expected_source_fingerprint=decode(repeat('11',32),'hex')
  where id=(v_scope->>'scope_id')::uuid;
  v_final:=public.weekly_source_candidate_app_submit_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_final_body,v_now
  );
  perform pg_temp.assert_true(
    v_final->>'generation'='2'
    and v_final#>>'{request,scopes,0,completion_state}'='COMPLETE'
    and exists(
      select 1 from public.weekly_timesheet_submission_request_memberships membership
      where membership.id=(v_scope->>'scope_id')::uuid
        and membership.state='SUBMITTED_MATCHED'
    )
    and not exists(
      select 1 from public.weekly_discrepancy_incidents incident
      join public.weekly_work_events event on event.id=incident.work_event_id
      where incident.source_cycle_id='fa600000-0000-4000-8000-000000000001'
        and event.work_date between '2026-08-10' and '2026-08-16'
    ),'exact late Timesheet did not auto-complete without an issue'
  );
  v_replay:=public.weekly_source_candidate_app_submit_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_final_body,
    v_now+interval '1 minute'
  );
  perform pg_temp.assert_true(v_replay->>'idempotent_replay'='true',
    'late Timesheet exact replay was not receipt-first');

  -- The next scope differs from source and enters the ordinary manager route.
  v_request:=public.weekly_source_candidate_app_request_get_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_now
  );
  select value into strict v_scope from pg_catalog.jsonb_array_elements(v_request->'scopes')
  where value->>'week_ending_date'='2026-08-23';
  v_final_body:=pg_catalog.jsonb_build_object(
    'submission_kind','WHOLE_WEEK_REVISION','request_version',v_request->'request_version',
    'request_fingerprint',v_request->>'request_fingerprint','scope_id',v_scope->>'scope_id',
    'scope_version',v_scope->'scope_version','scope_fingerprint',v_scope->>'scope_fingerprint',
    'workflow_id','fc500000-0000-4000-8000-000000000002','generation',1,
    'candidate_signature_component_id','fc600000-0000-4000-8000-000000000002',
    'candidate_signed_at_utc',v_now,'immutable_submission',pg_catalog.jsonb_build_object(
      'actual_schedule_json',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'row_key','late-mismatch','date','2026-08-17','start','09:00','end','18:00',
        'break_minutes',30
      )),'additional_units_week','{}'::jsonb,'additional_units_per_day','{}'::jsonb
    ),'responses','[]'::jsonb,
    'idempotency_key','fc700000-0000-4000-8000-000000000002'
  );
  v_final:=public.weekly_source_candidate_app_submit_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_final_body,v_now
  );
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_timesheet_submission_request_memberships membership
    where membership.id=(v_scope->>'scope_id')::uuid
      and membership.state='SUBMITTED_WITH_ISSUES'
  ) and exists(
    select 1 from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    where incident.work_event_id='fc100000-0000-4000-8000-000000000002'
      and incident.state='OPEN' and incident.candidate_action_state='NOT_REQUIRED'
      and incident.manager_action_state='DUE'
      and comparison.issue_family='SOURCE_HOURS_DIFFER'
      and comparison.candidate_end_at_local='2026-08-17 18:00'
      and comparison.system_end_at_local='2026-08-17 17:00'
  ),'mismatching late Timesheet did not enter the manager issue lifecycle');

  -- A complete submitted week may contain a worked shift absent from source.
  v_request:=public.weekly_source_candidate_app_request_get_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_now
  );
  select value into strict v_scope from pg_catalog.jsonb_array_elements(v_request->'scopes')
  where value->>'week_ending_date'='2026-08-30';
  v_final_body:=pg_catalog.jsonb_build_object(
    'submission_kind','WHOLE_WEEK_REVISION','request_version',v_request->'request_version',
    'request_fingerprint',v_request->>'request_fingerprint','scope_id',v_scope->>'scope_id',
    'scope_version',v_scope->'scope_version','scope_fingerprint',v_scope->>'scope_fingerprint',
    'workflow_id','fc500000-0000-4000-8000-000000000003','generation',1,
    'candidate_signature_component_id','fc600000-0000-4000-8000-000000000003',
    'candidate_signed_at_utc',v_now,'immutable_submission',pg_catalog.jsonb_build_object(
      'actual_schedule_json',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'row_key','late-present','date','2026-08-24','start','09:00','end','17:00',
          'break_minutes',30
        ),
        pg_catalog.jsonb_build_object(
          'row_key','late-absent','date','2026-08-25','start','09:00','end','17:00',
          'break_minutes',30
        )
      ),'additional_units_week','{}'::jsonb,'additional_units_per_day','{}'::jsonb
    ),'responses','[]'::jsonb,
    'idempotency_key','fc700000-0000-4000-8000-000000000003'
  );
  v_final:=public.weekly_source_candidate_app_submit_atomic_v1(
    'fad00000-0000-4000-8000-000000000001','TEST',v_generation,v_final_body,v_now
  );
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events event on event.id=incident.work_event_id
    where incident.source_cycle_id='fa600000-0000-4000-8000-000000000001'
      and event.work_date='2026-08-25'
      and comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
      and comparison.source_presence='ABSENT'
      and comparison.source_row_id is null
  ),'Candidate-only late shift did not create a source-missing incident');
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    where incident.work_event_id='fc100000-0000-4000-8000-000000000003'
      and comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
      and comparison.source_presence='ABSENT'
      and comparison.source_row_id='fc000000-0000-4000-8000-000000000003'
  ),'full-negative source movement was treated as worked Candidate hours');
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_timesheet_submission_requests submission
    where submission.candidate_id='fa300000-0000-4000-8000-000000000001'
      and submission.source_cycle_id='fa600000-0000-4000-8000-000000000001'
      and submission.state='COMPLETE'
  ) and exists(
    select 1 from public.weekly_candidate_outreach_generations generation
    where generation.id=v_generation and generation.state='COMPLETE'
  ),'multi-scope late Timesheet request did not close after all saved scopes');
  perform pg_temp.assert_true(not exists(
    select 1 from public.timesheets_financials financial
    where financial.timesheet_id in (
      select week_row.timesheet_id from public.contract_weeks week_row
      where week_row.id in (
        'fc400000-0000-4000-8000-000000000001',
        'fc400000-0000-4000-8000-000000000002',
        'fc400000-0000-4000-8000-000000000003'
      )
    )
  ),'late Candidate evidence created TSFIN/payment economics');
  perform pg_temp.assert_true(not exists(
    select 1
    from public.weekly_timesheet_submission_request_memberships membership
    join public.weekly_timesheet_submission_requests submission
      on submission.id=membership.submission_request_id
    where submission.candidate_id='fa300000-0000-4000-8000-000000000001'
      and submission.source_cycle_id='fa600000-0000-4000-8000-000000000001'
      and membership.state in ('SUBMITTED_MATCHED','SUBMITTED_WITH_ISSUES')
      and membership.submitted_timesheet_hash is distinct from
        private.weekly_source_query_candidate_timesheet_hash_v1(
          membership.submitted_timesheet_id
        )
  ),'late completion did not persist the exact saved Timesheet hash');
  perform pg_temp.assert_true(not exists(
    select 1
    from public.contract_weeks week_row
    join public.timesheets timesheet
      on timesheet.timesheet_id=week_row.timesheet_id
    where week_row.id in (
      'fc400000-0000-4000-8000-000000000001',
      'fc400000-0000-4000-8000-000000000002',
      'fc400000-0000-4000-8000-000000000003'
    ) and (
      timesheet.authorised_at_server is not null
      or timesheet.submission_mode<>'MANUAL'
      or timesheet.sheet_scope<>'WEEKLY'
      or timesheet.line_type<>'HOURS'
      or week_row.submission_mode_snapshot<>'ELECTRONIC'
    )
  ),'late Candidate evidence escaped the ordinary unapproved Weekly route');
  perform pg_temp.assert_true(not exists(
    select 1 from public.invoice_lines invoice_line
    where invoice_line.timesheet_id in (
      select week_row.timesheet_id from public.contract_weeks week_row
      where week_row.id in (
        'fc400000-0000-4000-8000-000000000001',
        'fc400000-0000-4000-8000-000000000002',
        'fc400000-0000-4000-8000-000000000003'
      )
    )
  ),'late Candidate evidence created invoice lines');
  perform pg_temp.assert_true(not exists(
    select 1 from public.timesheet_pay_state pay_state
    where pay_state.timesheet_id in (
      select week_row.timesheet_id from public.contract_weeks week_row
      where week_row.id in (
        'fc400000-0000-4000-8000-000000000001',
        'fc400000-0000-4000-8000-000000000002',
        'fc400000-0000-4000-8000-000000000003'
      )
    )
  ) and not exists(
    select 1 from public.pay_batch_items batch_item
    where batch_item.timesheet_id in (
      select week_row.timesheet_id from public.contract_weeks week_row
      where week_row.id in (
        'fc400000-0000-4000-8000-000000000001',
        'fc400000-0000-4000-8000-000000000002',
        'fc400000-0000-4000-8000-000000000003'
      )
    )
  ) and not exists(
    select 1 from public.pay_advances finance_case
    where finance_case.linked_timesheet_id in (
      select week_row.timesheet_id from public.contract_weeks week_row
      where week_row.id in (
        'fc400000-0000-4000-8000-000000000001',
        'fc400000-0000-4000-8000-000000000002',
        'fc400000-0000-4000-8000-000000000003'
      )
    )
  ) and not exists(
    select 1 from public.banking_pay_workbench_candidate_line_work line_work
    where line_work.timesheet_id in (
      select week_row.timesheet_id from public.contract_weeks week_row
      where week_row.id in (
        'fc400000-0000-4000-8000-000000000001',
        'fc400000-0000-4000-8000-000000000002',
        'fc400000-0000-4000-8000-000000000003'
      )
    )
  ) and not exists(
    (select pg_catalog.to_jsonb(workbench_job)
     from public.banking_pay_workbench_jobs workbench_job
     where workbench_job.candidate_id='fa300000-0000-4000-8000-000000000001'
     except
     select pg_catalog.to_jsonb(boundary_row)
     from candidate_app_workbench_jobs_before boundary_row)
    union all
    (select pg_catalog.to_jsonb(boundary_row)
     from candidate_app_workbench_jobs_before boundary_row
     except
     select pg_catalog.to_jsonb(workbench_job)
     from public.banking_pay_workbench_jobs workbench_job
     where workbench_job.candidate_id='fa300000-0000-4000-8000-000000000001')
  ),'late Candidate evidence touched Banking Pay or Workbench state');

  raise notice 'WEEKLY_SOURCE_CANDIDATE_APP_CONTRACT_V1: PASS';
end;
$runtime$;

rollback;

select case when not exists(
  select 1 from public.weekly_candidate_app_mutation_receipts
  where idempotency_key in (
    'fb000000-0000-4000-8000-000000000001',
    'fb000000-0000-4000-8000-000000000002'
  )
) and not exists(
  select 1 from public.candidates
  where id='fa300000-0000-4000-8000-000000000001'
) then 'WEEKLY_SOURCE_CANDIDATE_APP_CONTRACT_V1_ROLLBACK: PASS'
else 'WEEKLY_SOURCE_CANDIDATE_APP_CONTRACT_V1_ROLLBACK: FAIL' end as rollback_result;
