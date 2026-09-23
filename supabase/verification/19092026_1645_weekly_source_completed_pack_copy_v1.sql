\set ON_ERROR_STOP on

begin;

create or replace function pg_temp.assert_true(p_value boolean,p_message text)
returns void language plpgsql as $assert$
begin
  if p_value is distinct from true then
    raise exception 'VERIFY_FAILED: %',p_message;
  end if;
end;
$assert$;

select pg_temp.assert_true(
  pg_catalog.to_regprocedure(
    'public.weekly_source_completed_pack_copy_due_list_v1(jsonb)'
  ) is not null,
  'due-list RPC is absent'
);
select pg_temp.assert_true(
  pg_catalog.to_regprocedure(
    'public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)'
  ) is not null,
  'commit RPC is absent'
);
select pg_temp.assert_true(
  not exists(
    select 1
    from pg_catalog.unnest(array[
      'public.weekly_source_completed_pack_copy_status_sync_v1(jsonb)',
      'public.weekly_source_completed_pack_copy_due_list_v1(jsonb)',
      'public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)'
    ]) signature
    where not pg_catalog.has_function_privilege('service_role',signature,'EXECUTE')
       or pg_catalog.has_function_privilege('anon',signature,'EXECUTE')
       or pg_catalog.has_function_privilege('authenticated',signature,'EXECUTE')
  ),
  'a completed-pack RPC is not service-only'
);
select pg_temp.assert_true(
  not pg_catalog.has_function_privilege(
    'service_role','private._weekly_source_completed_pack_copy_eligibility_v1(uuid)','EXECUTE'
  ) and not pg_catalog.has_function_privilege(
    'anon','private._weekly_source_completed_pack_copy_eligibility_v1(uuid)','EXECUTE'
  ) and not pg_catalog.has_function_privilege(
    'authenticated','private._weekly_source_completed_pack_copy_eligibility_v1(uuid)','EXECUTE'
  ),
  'private eligibility owner is externally callable'
);
select pg_temp.assert_true(
  not exists(
    select 1
    from pg_catalog.unnest(array[
      'private._weekly_source_completed_pack_copy_eligibility_v1(uuid)',
      'public.weekly_source_completed_pack_copy_status_sync_v1(jsonb)',
      'public.weekly_source_completed_pack_copy_due_list_v1(jsonb)',
      'public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)'
    ]) signature
    join pg_catalog.pg_proc routine
      on routine.oid=pg_catalog.to_regprocedure(signature)
    where routine.proowner<>(current_user::pg_catalog.regrole)::oid
       or not routine.prosecdef
       or not coalesce(routine.proconfig,'{}'::text[])
              @> array['search_path=pg_catalog, pg_temp']::text[]
  ),
  'completed-pack owner, definer or fixed search-path contract is unsafe'
);

insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
values (
  'e5010000-0000-4000-8000-000000000001',
  'completed-pack-verifier@example.invalid','admin','not-a-real-password',
  'Completed pack verifier',true
);

insert into public.clients(id,name,ts_queries_email) values
  ('e5020000-0000-4000-8000-000000000001','Source Authority Trust','queries@example.invalid'),
  ('e5020000-0000-4000-8000-000000000002','Timesheet Authority Trust','queries2@example.invalid');
insert into public.client_settings(client_id,effective_from,vat_rate_pct) values
  ('e5020000-0000-4000-8000-000000000001','2026-01-01',20),
  ('e5020000-0000-4000-8000-000000000002','2026-01-01',20);
insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email,key_norm)
values
  ('e5030000-0000-4000-8000-000000000001','CP-1','Casey','Source','Casey Source','casey@example.invalid','CASEY-SOURCE'),
  ('e5030000-0000-4000-8000-000000000002','CP-2','Alex','Evidence','Alex Evidence','alex@example.invalid','ALEX-EVIDENCE');

insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  week_ending_weekday_snapshot,default_submission_mode,role,self_bill,
  weekly_timesheet_source,no_timesheet_required,autoprocess_hr
) values
  ('e5040000-0000-4000-8000-000000000001','e5030000-0000-4000-8000-000000000001',
   'e5020000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',0,
   'ELECTRONIC','Nurse',true,'HEALTHROSTER',true,true),
  ('e5040000-0000-4000-8000-000000000002','e5030000-0000-4000-8000-000000000002',
   'e5020000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}',0,
   'ELECTRONIC','Nurse',false,'HEALTHROSTER',false,false);

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time
) values
  ('e5050000-0000-4000-8000-000000000001','TEST','e5000000-0000-4000-8000-000000000001',
   'PACK_SOURCE','Pack Source','ROSTER',3,'15:00'),
  ('e5050000-0000-4000-8000-000000000002','TEST','e5000000-0000-4000-8000-000000000001',
   'PACK_EVIDENCE','Pack Evidence','ROSTER',3,'15:00');
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values
  ('e5050000-0000-4000-8000-000000000001','e5020000-0000-4000-8000-000000000001',
   '2026-01-01','e5010000-0000-4000-8000-000000000001'),
  ('e5050000-0000-4000-8000-000000000002','e5020000-0000-4000-8000-000000000002',
   '2026-01-01','e5010000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,
  completed_pack_copy_enabled,completed_pack_recipient,created_by_user_id
) values
  ('e5050000-0000-4000-8000-000000000001','e5020000-0000-4000-8000-000000000001',
   '2026-01-01','SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,true,
   'copy-source@example.invalid','e5010000-0000-4000-8000-000000000001'),
  ('e5050000-0000-4000-8000-000000000002','e5020000-0000-4000-8000-000000000002',
   '2026-01-01','TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',false,false,true,true,
   'copy-evidence@example.invalid','e5010000-0000-4000-8000-000000000001');

insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  contract_id,submission_mode,sheet_scope,line_type,actual_schedule_json
) values
  ('e5060000-0000-4000-8000-000000000001','PACK-FAMILY-SOURCE','casey-source',
   'source-authority-trust','ward-a','nurse','2026-09-07 09:00+00','2026-09-07 17:00+00',30,450,
   '2026-09-13','e5040000-0000-4000-8000-000000000001','MANUAL','WEEKLY','HOURS',
   '[{"date":"2026-09-07","start":"09:00","end":"17:00","break_minutes":30}]'),
  ('e5060000-0000-4000-8000-000000000002','PACK-FAMILY-EVIDENCE','alex-evidence',
   'timesheet-authority-trust','ward-b','nurse','2026-09-08 09:00+00','2026-09-08 17:00+00',30,450,
   '2026-09-13','e5040000-0000-4000-8000-000000000002','MANUAL','WEEKLY','HOURS',
   '[{"date":"2026-09-08","start":"09:00","end":"17:00","break_minutes":30}]');
insert into public.contract_weeks(
  id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
  day_entries_json,totals_json
) values
  ('e5070000-0000-4000-8000-000000000001','e5040000-0000-4000-8000-000000000001',
   '2026-09-13','SUBMITTED','ELECTRONIC','e5060000-0000-4000-8000-000000000001','[]','{}'),
  ('e5070000-0000-4000-8000-000000000002','e5040000-0000-4000-8000-000000000002',
   '2026-09-13','SUBMITTED','ELECTRONIC','e5060000-0000-4000-8000-000000000002','[]','{}');
insert into public.candidate_app_accounts(id,environment,email_normalized,status) values
  ('e5080000-0000-4000-8000-000000000001','TEST','casey@example.invalid','ACTIVE'),
  ('e5080000-0000-4000-8000-000000000002','TEST','alex@example.invalid','ACTIVE');

insert into public.candidate_submission_workflows(
  id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
  contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
  idempotency_key,worker_submitted_at_utc,immutable_submission_json,
  immutable_submission_sha256,candidate_signed_at_utc
) values
  ('e5090000-0000-4000-8000-000000000001','TEST','e5080000-0000-4000-8000-000000000001',
   'e5030000-0000-4000-8000-000000000001','CONTRACT_HOURS','WEEKLY','ELECTRONIC',
   'WORKER_SUBMITTED',1,'e5040000-0000-4000-8000-000000000001',
   'e5070000-0000-4000-8000-000000000001','e5060000-0000-4000-8000-000000000001',
   'e5060000-0000-4000-8000-000000000001','2026-09-13','pack-source-1',
   '2026-09-10 10:00+00','{}',decode(repeat('11',32),'hex'),'2026-09-10 10:00+00'),
  ('e5090000-0000-4000-8000-000000000002','TEST','e5080000-0000-4000-8000-000000000002',
   'e5030000-0000-4000-8000-000000000002','CONTRACT_COMBINED','WEEKLY','PHONE',
   'FINALISED',2,'e5040000-0000-4000-8000-000000000002',
   'e5070000-0000-4000-8000-000000000002','e5060000-0000-4000-8000-000000000002',
   'e5060000-0000-4000-8000-000000000002','2026-09-13','pack-evidence-1',
   '2026-09-10 10:00+00','{}',decode(repeat('21',32),'hex'),'2026-09-10 10:00+00');

-- A genuine ordinary weekly workflow must not abort the source-only sweep.
-- It intentionally has no Weekly Source group membership or client policy.
insert into public.clients(id,name) values
  ('e5020000-0000-4000-8000-000000000003','Ordinary Weekly Client');
insert into public.client_settings(client_id,effective_from,vat_rate_pct) values
  ('e5020000-0000-4000-8000-000000000003','2026-01-01',20);
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  week_ending_weekday_snapshot,default_submission_mode,role,self_bill,
  no_timesheet_required,autoprocess_hr
) values (
  'e5040000-0000-4000-8000-000000000003','e5030000-0000-4000-8000-000000000001',
  'e5020000-0000-4000-8000-000000000003','2026-01-01','2026-12-31','PAYE','{}',0,
  'ELECTRONIC','Nurse',false,false,false
);
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  contract_id,submission_mode,sheet_scope,line_type,actual_schedule_json
) values (
  'e5060000-0000-4000-8000-000000000004','PACK-FAMILY-ORDINARY','casey-source',
  'ordinary-weekly-client','ward-c','nurse','2026-09-14 09:00+00','2026-09-14 17:00+00',30,450,
  '2026-09-20','e5040000-0000-4000-8000-000000000003','MANUAL','WEEKLY','HOURS',
  '[{"date":"2026-09-14","start":"09:00","end":"17:00","break_minutes":30}]'
);
insert into public.contract_weeks(
  id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
  day_entries_json,totals_json
) values (
  'e5070000-0000-4000-8000-000000000003','e5040000-0000-4000-8000-000000000003',
  '2026-09-20','SUBMITTED','ELECTRONIC','e5060000-0000-4000-8000-000000000004','[]','{}'
);
insert into public.candidate_submission_workflows(
  id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
  contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,idempotency_key,
  worker_submitted_at_utc,immutable_submission_json,immutable_submission_sha256,
  candidate_signed_at_utc
) values (
  'e5090000-0000-4000-8000-000000000003','TEST','e5080000-0000-4000-8000-000000000001',
  'e5030000-0000-4000-8000-000000000001','CONTRACT_HOURS','WEEKLY','ELECTRONIC',
  'WORKER_SUBMITTED',1,'e5040000-0000-4000-8000-000000000003',
  'e5070000-0000-4000-8000-000000000003',
  'e5060000-0000-4000-8000-000000000004','e5060000-0000-4000-8000-000000000004',
  '2026-09-20','pack-ordinary-1','2026-09-17 10:00+00','{}',
  decode(repeat('31',32),'hex'),'2026-09-17 10:00+00'
);
select pg_temp.assert_true(
  private._weekly_source_completed_pack_copy_eligibility_v1(
    'e5090000-0000-4000-8000-000000000003'
  ) is null,
  'ordinary weekly workflow entered the source-only completed-pack copy route'
);

insert into public.candidate_submission_components(
  id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
  document_role,state,storage_key,media_type,byte_size,source_content_sha256,
  immutable_at_utc,review_ordinal,required,review_render_state,final_signed_render_state
) values
  ('e5100000-0000-4000-8000-000000000001','e5090000-0000-4000-8000-000000000001',1,1,
   'e5060000-0000-4000-8000-000000000001','CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE',
   'IMMUTABLE','signatures/source.png','image/png',100,decode(repeat('12',32),'hex'),
   '2026-09-10 10:00+00',null,false,'NOT_REQUIRED','NOT_REQUIRED'),
  ('e5100000-0000-4000-8000-000000000002','e5090000-0000-4000-8000-000000000002',1,1,
   'e5060000-0000-4000-8000-000000000002','CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE',
   'IMMUTABLE','signatures/evidence-candidate.png','image/png',100,decode(repeat('22',32),'hex'),
   '2026-09-10 10:00+00',null,false,'NOT_REQUIRED','NOT_REQUIRED'),
  ('e5100000-0000-4000-8000-000000000003','e5090000-0000-4000-8000-000000000002',1,2,
   'e5060000-0000-4000-8000-000000000002','HOURS_TIMESHEET',
   'ELECTRONIC_TIMESHEET_MANAGER_REVIEW','IMMUTABLE',null,null,null,null,
   '2026-09-10 10:00+00',1,true,'PENDING','PENDING');
update public.candidate_submission_components
set review_ordinal=1,review_render_state='READY',final_signed_render_state='READY',
    review_storage_key='review/evidence.pdf',review_content_sha256=decode(repeat('23',32),'hex'),
    review_media_type='application/pdf',review_byte_size=900,review_page_count=1,
    review_render_input_sha256=decode(repeat('24',32),'hex'),
    review_renderer_contract_version='CANDIDATE_REVIEW_DOCUMENTS_V1',
    review_renderer_receipt_json='{}',review_generated_at_utc='2026-09-10 10:01+00',
    final_signed_storage_key='final/evidence.pdf',final_signed_content_sha256=decode(repeat('25',32),'hex'),
    final_signed_media_type='application/pdf',final_signed_byte_size=1000,final_signed_page_count=1,
    final_signed_render_input_sha256=decode(repeat('24',32),'hex'),
    final_signed_renderer_contract_version='CANDIDATE_REVIEW_DOCUMENTS_V1',
    final_signed_renderer_receipt_json='{}',final_signed_generated_at_utc='2026-09-10 10:02+00'
where id='e5100000-0000-4000-8000-000000000003';

-- Ordinary receipt evidence remains part of the final signed evidence pack.
-- This is a second required component of the same completed workflow; it does
-- not create, approve, reject or move an expense.
insert into public.candidate_submission_components(
  id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
  expense_category,document_role,state,storage_key,media_type,byte_size,
  source_content_sha256,immutable_at_utc,review_ordinal,required,
  review_render_state,final_signed_render_state
) values (
  'e5100000-0000-4000-8000-000000000005','e5090000-0000-4000-8000-000000000002',1,4,
  'e5060000-0000-4000-8000-000000000002','EXPENSE_EVIDENCE','TRAVEL',
  'SOURCE_EVIDENCE','IMMUTABLE','evidence/travel-receipt.pdf','application/pdf',600,
  decode(repeat('28',32),'hex'),'2026-09-10 10:00+00',2,true,'PENDING','PENDING'
);
update public.candidate_submission_components
set review_render_state='READY',final_signed_render_state='READY',
    review_storage_key='review/travel-receipt.pdf',
    review_content_sha256=decode(repeat('29',32),'hex'),
    review_media_type='application/pdf',review_byte_size=600,review_page_count=1,
    review_render_input_sha256=decode(repeat('2a',32),'hex'),
    review_renderer_contract_version='CANDIDATE_REVIEW_DOCUMENTS_V1',
    review_renderer_receipt_json='{}',review_generated_at_utc='2026-09-10 10:01+00',
    final_signed_storage_key='final/travel-receipt.pdf',
    final_signed_content_sha256=decode(repeat('2b',32),'hex'),
    final_signed_media_type='application/pdf',final_signed_byte_size=600,
    final_signed_page_count=1,
    final_signed_render_input_sha256=decode(repeat('2a',32),'hex'),
    final_signed_renderer_contract_version='CANDIDATE_REVIEW_DOCUMENTS_V1',
    final_signed_renderer_receipt_json='{}',
    final_signed_generated_at_utc='2026-09-10 10:02+00'
where id='e5100000-0000-4000-8000-000000000005';

insert into public.candidate_approval_requests(
  id,workflow_id,workflow_generation,method,state,manager_name,manager_position,
  approved_at_utc,idempotency_key,review_manifest_sha256,required_component_ids,
  required_component_manifest_json
) values (
  'e5110000-0000-4000-8000-000000000001','e5090000-0000-4000-8000-000000000002',1,
  'PHONE','APPROVED','Morgan Manager','Manager','2026-09-10 10:02+00','pack-approval-1',
  decode(repeat('26',32),'hex'),array['e5100000-0000-4000-8000-000000000003'::uuid],
  '[{"component_id":"e5100000-0000-4000-8000-000000000003"}]'
);
update public.candidate_approval_requests
set required_component_ids=array[
      'e5100000-0000-4000-8000-000000000003'::uuid,
      'e5100000-0000-4000-8000-000000000005'::uuid
    ],
    required_component_manifest_json='[
      {"component_id":"e5100000-0000-4000-8000-000000000003"},
      {"component_id":"e5100000-0000-4000-8000-000000000005"}
    ]'::jsonb
where id='e5110000-0000-4000-8000-000000000001';
insert into public.candidate_submission_components(
  id,workflow_id,workflow_generation,component_no,component_kind,document_role,state,
  approval_request_id,storage_key,media_type,byte_size,source_content_sha256,
  immutable_at_utc,required,review_render_state,final_signed_render_state,
  manager_signature_capture_method
) values (
  'e5100000-0000-4000-8000-000000000004','e5090000-0000-4000-8000-000000000002',1,3,
  'MANAGER_SIGNATURE','MANAGER_SIGNATURE','IMMUTABLE','e5110000-0000-4000-8000-000000000001',
  'signatures/evidence-manager.png','image/png',100,decode(repeat('27',32),'hex'),
  '2026-09-10 10:02+00',false,'NOT_REQUIRED','NOT_REQUIRED','DRAW'
);
update public.candidate_approval_requests
set signature_component_id='e5100000-0000-4000-8000-000000000004'
where id='e5110000-0000-4000-8000-000000000001';
update public.candidate_submission_workflows
set candidate_signature_component_id=case id
      when 'e5090000-0000-4000-8000-000000000001' then 'e5100000-0000-4000-8000-000000000001'::uuid
      else 'e5100000-0000-4000-8000-000000000002'::uuid end,
    candidate_signature_sha256=case id
      when 'e5090000-0000-4000-8000-000000000001' then decode(repeat('12',32),'hex')
      else decode(repeat('22',32),'hex') end
where id in ('e5090000-0000-4000-8000-000000000001','e5090000-0000-4000-8000-000000000002');
update public.candidate_submission_workflows
set manager_name='Morgan Manager',manager_position='Manager',
    manager_signature_component_id='e5100000-0000-4000-8000-000000000004',
    manager_signature_sha256=decode(repeat('27',32),'hex'),
    manager_approved_at_utc='2026-09-10 10:02+00',finalised_at_utc='2026-09-10 10:03+00'
where id='e5090000-0000-4000-8000-000000000002';

-- Effective setting hierarchy and readiness negatives. These are isolated
-- policy/readiness changes inside this rollback-only verification.
do $policy_and_readiness$
declare
  v_policy jsonb;
  v_due jsonb;
begin
  v_policy:=private._weekly_source_effective_policy_v1(
    'e5020000-0000-4000-8000-000000000001',
    'e5040000-0000-4000-8000-000000000001','2026-09-13'
  );
  if (v_policy->>'completed_pack_copy_enabled')::boolean is not true
     or v_policy->>'completed_pack_recipient'<>'copy-source@example.invalid' then
    raise exception 'VERIFY_FAILED: Client-on completed-copy setting was not effective';
  end if;

  insert into public.weekly_source_contract_policies(
    contract_id,effective_from,completed_pack_copy_enabled_override,
    completed_pack_recipient_override,created_by_user_id
  ) values (
    'e5040000-0000-4000-8000-000000000001','2026-01-01',false,null,
    'e5010000-0000-4000-8000-000000000001'
  );
  v_policy:=private._weekly_source_effective_policy_v1(
    'e5020000-0000-4000-8000-000000000001',
    'e5040000-0000-4000-8000-000000000001','2026-09-13'
  );
  if (v_policy->>'completed_pack_copy_enabled')::boolean is not false
     or v_policy->>'completed_pack_recipient' is not null then
    raise exception 'VERIFY_FAILED: Contract-off override did not suppress the copy';
  end if;

  update public.weekly_source_client_policies
  set completed_pack_copy_enabled=false,completed_pack_recipient=null
  where client_id='e5020000-0000-4000-8000-000000000001';
  update public.weekly_source_contract_policies
  set completed_pack_copy_enabled_override=true,
      completed_pack_recipient_override='contract-copy@example.invalid'
  where contract_id='e5040000-0000-4000-8000-000000000001';
  v_policy:=private._weekly_source_effective_policy_v1(
    'e5020000-0000-4000-8000-000000000001',
    'e5040000-0000-4000-8000-000000000001','2026-09-13'
  );
  if (v_policy->>'completed_pack_copy_enabled')::boolean is not true
     or v_policy->>'completed_pack_recipient'<>'contract-copy@example.invalid' then
    raise exception 'VERIFY_FAILED: Contract-on/recipient override did not win';
  end if;

  update public.weekly_source_client_policies
  set completed_pack_copy_enabled=true,
      completed_pack_recipient='copy-source@example.invalid'
  where client_id='e5020000-0000-4000-8000-000000000001';
  update public.weekly_source_contract_policies
  set completed_pack_copy_enabled_override=null,
      completed_pack_recipient_override=null
  where contract_id='e5040000-0000-4000-8000-000000000001';

  update public.candidate_submission_workflows
  set candidate_signed_at_utc=null,candidate_signature_component_id=null,
      candidate_signature_sha256=null
  where id='e5090000-0000-4000-8000-000000000001';
  v_due:=public.weekly_source_completed_pack_copy_due_list_v1('{"limit":20}');
  if (v_due->>'count')::integer<>1
     or v_due->'items'->0->>'document_mode'<>'INVOICE_EVIDENCE_REQUIRED' then
    raise exception 'VERIFY_FAILED: incomplete Candidate signature was eligible';
  end if;
  update public.candidate_submission_workflows
  set candidate_signed_at_utc='2026-09-10 10:00+00',
      candidate_signature_component_id='e5100000-0000-4000-8000-000000000001',
      candidate_signature_sha256=decode(repeat('12',32),'hex')
  where id='e5090000-0000-4000-8000-000000000001';

  update public.candidate_submission_workflows
  set manager_name=null,manager_position=null,manager_approved_at_utc=null,
      manager_signature_component_id=null,manager_signature_sha256=null
  where id='e5090000-0000-4000-8000-000000000002';
  v_due:=public.weekly_source_completed_pack_copy_due_list_v1('{"limit":20}');
  if (v_due->>'count')::integer<>1
     or v_due->'items'->0->>'document_mode'<>'CHECK_ONLY' then
    raise exception 'VERIFY_FAILED: incomplete manager approval was eligible';
  end if;
  update public.candidate_submission_workflows
  set manager_name='Morgan Manager',manager_position='Manager',
      manager_approved_at_utc='2026-09-10 10:02+00',
      manager_signature_component_id='e5100000-0000-4000-8000-000000000004',
      manager_signature_sha256=decode(repeat('27',32),'hex')
  where id='e5090000-0000-4000-8000-000000000002';

  update public.weekly_source_client_policies
  set document_mode='IMPORT_ONLY'
  where client_id='e5020000-0000-4000-8000-000000000001';
  v_due:=public.weekly_source_completed_pack_copy_due_list_v1('{"limit":20}');
  if (v_due->>'count')::integer<>1
     or v_due->'items'->0->>'document_mode'<>'INVOICE_EVIDENCE_REQUIRED' then
    raise exception 'VERIFY_FAILED: IMPORT_ONLY produced a completed-pack copy';
  end if;
  update public.weekly_source_client_policies
  set document_mode='CHECK_ONLY'
  where client_id='e5020000-0000-4000-8000-000000000001';
end;
$policy_and_readiness$;

create temp table pack_finance_before as
select
  (select count(*) from public.timesheets_financials
   where timesheet_id in ('e5060000-0000-4000-8000-000000000001','e5060000-0000-4000-8000-000000000002')) as financials,
  (select count(*) from public.invoice_lines
   where timesheet_id in ('e5060000-0000-4000-8000-000000000001','e5060000-0000-4000-8000-000000000002')) as invoices,
  (select count(*) from public.pay_batch_items
   where timesheet_id in ('e5060000-0000-4000-8000-000000000001','e5060000-0000-4000-8000-000000000002')) as pay_items;

do $prove$
declare
  v_due jsonb;
  v_item jsonb;
  v_result jsonb;
  v_count integer;
begin
  v_due:=public.weekly_source_completed_pack_copy_due_list_v1('{"limit":20}');
  if (v_due->>'count')::integer<>2 then
    raise exception 'VERIFY_FAILED: both completed document modes were not due: %',v_due;
  end if;
  if not exists(select 1 from jsonb_array_elements(v_due->'items') item
    where item->>'document_mode'='CHECK_ONLY'
      and item->>'recipient'='copy-source@example.invalid') then
    raise exception 'VERIFY_FAILED: CHECK_ONLY completed pack was not eligible';
  end if;
  if not exists(select 1 from jsonb_array_elements(v_due->'items') item
    where item->>'document_mode'='INVOICE_EVIDENCE_REQUIRED'
      and jsonb_array_length(item->'components')=2
      and item->'components'->0->>'storage_key'='final/evidence.pdf'
      and item->'components'->1->>'storage_key'='final/travel-receipt.pdf'
      and item->'components'->1->>'expense_category'='TRAVEL') then
    raise exception 'VERIFY_FAILED: final signed evidence pack was not eligible';
  end if;

  for v_item in select value from jsonb_array_elements(v_due->'items') loop
    v_result:=public.weekly_source_completed_pack_copy_commit_atomic_v1(
      jsonb_build_object(
        'workflow_id',v_item->>'workflow_id',
        'render_input_sha256',v_item->>'render_input_sha256',
        'storage_key','weekly-source/test/completed/'||(v_item->>'workflow_id')||'.pdf',
        'final_document_sha256',case when v_item->>'document_mode'='CHECK_ONLY'
          then repeat('31',32) else repeat('32',32) end,
        'filename','Completed_Timesheet_2026-09-13.pdf',
        'media_type','application/pdf','byte_size',1200,'page_count',1,
        'content_policy_version','WEEKLY_COMPLETED_PACK_COPY_CONTENT_V1'
      )
    );
    if not coalesce((v_result->>'ok')::boolean,false)
       or coalesce((v_result->>'idempotent_replay')::boolean,true) then
      raise exception 'VERIFY_FAILED: first completed-pack commit failed: %',v_result;
    end if;
  end loop;

  select count(*) into v_count from public.weekly_completed_pack_copy_events;
  if v_count<>2 then raise exception 'VERIFY_FAILED: completion event count is not two'; end if;
  select count(*) into v_count from public.mail_outbox
  where payment_scope_json->>'completed_pack_copy_authority'='WEEKLY_COMPLETED_PACK_COPY_V1';
  if v_count<>2 then raise exception 'VERIFY_FAILED: completed pack outbox count is not two'; end if;
  if exists(
    select 1 from public.mail_outbox
    where payment_scope_json->>'completed_pack_copy_authority'='WEEKLY_COMPLETED_PACK_COPY_V1'
      and (body_html ~* 'href=|https?://' or body_text ~* 'https?://')
  ) then raise exception 'VERIFY_FAILED: informational email contains a link'; end if;

  v_item:=(select value from jsonb_array_elements(v_due->'items') value
    where value->>'document_mode'='CHECK_ONLY');
  v_result:=public.weekly_source_completed_pack_copy_commit_atomic_v1(
    jsonb_build_object(
      'workflow_id',v_item->>'workflow_id','render_input_sha256',v_item->>'render_input_sha256',
      'storage_key','different-key-is-ignored-on-replay.pdf',
      'final_document_sha256',repeat('ff',32),'filename','replay.pdf',
      'media_type','application/pdf','byte_size',1,'page_count',1,
      'content_policy_version','WEEKLY_COMPLETED_PACK_COPY_CONTENT_V1'
    )
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'VERIFY_FAILED: exact completion generation did not replay';
  end if;
end;
$prove$;

-- The generic mail drainer must claim the real informational copy without
-- opening the guarded Candidate manager/paper or Banking Pay mail routes.
-- A changed attachment hash must remain unclaimable.
do $claim_guard$
declare
  v_outbox_id uuid;
  v_claimed integer;
begin
  select outbox.id into strict v_outbox_id
  from public.mail_outbox outbox
  where outbox.payment_scope_json->>'completed_pack_copy_authority'
    ='WEEKLY_COMPLETED_PACK_COPY_V1'
    and outbox.context_id='e5060000-0000-4000-8000-000000000001';

  update public.mail_outbox
  set attachments=jsonb_set(attachments,'{0,sha256}',to_jsonb(repeat('f',64)))
  where id=v_outbox_id;
  select count(*) into v_claimed
  from public.email_outbox_claim_ready_batch(10,'weekly-copy-forged-proof',5) claimed
  where claimed.id=v_outbox_id;
  if v_claimed<>0 then
    raise exception 'VERIFY_FAILED: forged completed-copy attachment was claimable';
  end if;

  update public.mail_outbox
  set attachments=jsonb_set(
    attachments,'{0,sha256}',to_jsonb(payment_scope_json->>'final_document_sha256')
  )
  where id=v_outbox_id;
  select count(*) into v_claimed
  from public.email_outbox_claim_ready_batch(10,'weekly-copy-valid-proof',5) claimed
  where claimed.id=v_outbox_id;
  if v_claimed<>1 then
    raise exception 'VERIFY_FAILED: valid completed-copy message was not claimable';
  end if;
end;
$claim_guard$;

-- A later policy edit cannot redirect the immutable event or its already
-- queued command. A genuinely re-signed generation is nevertheless a new
-- completion and becomes due exactly once.
update public.weekly_source_client_policies
set completed_pack_recipient='later-change@example.invalid'
where client_id='e5020000-0000-4000-8000-000000000001';
select pg_temp.assert_true(
  exists(
    select 1 from public.weekly_completed_pack_copy_events event
    where event.timesheet_id='e5060000-0000-4000-8000-000000000001'
      and event.recipient_snapshot='copy-source@example.invalid'
  ) and exists(
    select 1 from public.mail_outbox outbox
    where outbox.payment_scope_json->>'completed_pack_copy_authority'='WEEKLY_COMPLETED_PACK_COPY_V1'
      and outbox.context_id='e5060000-0000-4000-8000-000000000001'
      and outbox."to"='copy-source@example.invalid'
  ),
  'later settings change redirected an existing completion event'
);
insert into public.candidate_submission_components(
  id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
  document_role,state,storage_key,media_type,byte_size,source_content_sha256,
  immutable_at_utc,required,review_render_state,final_signed_render_state
) values (
  'e5100000-0000-4000-8000-000000000006','e5090000-0000-4000-8000-000000000001',2,1,
  'e5060000-0000-4000-8000-000000000001','CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE',
  'IMMUTABLE','signatures/source-v2.png','image/png',100,decode(repeat('41',32),'hex'),
  '2026-09-11 10:00+00',false,'NOT_REQUIRED','NOT_REQUIRED'
);
update public.candidate_submission_workflows
set generation=2,candidate_signature_component_id='e5100000-0000-4000-8000-000000000006',
    candidate_signature_sha256=decode(repeat('41',32),'hex'),
    candidate_signed_at_utc='2026-09-11 10:00+00',updated_at_utc='2026-09-11 10:00+00'
where id='e5090000-0000-4000-8000-000000000001';
select pg_temp.assert_true(
  (public.weekly_source_completed_pack_copy_due_list_v1('{"limit":20}')->>'count')::integer=1,
  'a genuinely re-signed completion generation was not due exactly once'
);
update public.candidate_submission_workflows
set generation=1,candidate_signature_component_id='e5100000-0000-4000-8000-000000000001',
    candidate_signature_sha256=decode(repeat('12',32),'hex'),
    candidate_signed_at_utc='2026-09-10 10:00+00',updated_at_utc='2026-09-10 10:00+00'
where id='e5090000-0000-4000-8000-000000000001';
update public.weekly_source_client_policies
set completed_pack_recipient='copy-source@example.invalid'
where client_id='e5020000-0000-4000-8000-000000000001';

-- A physical Timesheet rotation cannot create a second copy for the same
-- immutable completion generation.
update public.timesheets set is_current=false
where timesheet_id='e5060000-0000-4000-8000-000000000001';
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  contract_id,submission_mode,sheet_scope,line_type,actual_schedule_json,version
) select
  'e5060000-0000-4000-8000-000000000003',booking_id,occupant_key_norm,
  hospital_norm,ward_norm,job_title_norm,worked_start_iso,worked_end_iso,
  break_minutes,worked_minutes,week_ending_date,contract_id,submission_mode,
  sheet_scope,line_type,actual_schedule_json,2
from public.timesheets where timesheet_id='e5060000-0000-4000-8000-000000000001';
select pg_temp.assert_true(
  (public.weekly_source_completed_pack_copy_due_list_v1('{"limit":20}')->>'count')::integer=0,
  'Timesheet family rotation created a duplicate informational copy'
);

update public.mail_outbox set status='SENT',sent_at=clock_timestamp()
where payment_scope_json->>'completed_pack_copy_authority'='WEEKLY_COMPLETED_PACK_COPY_V1';
select public.weekly_source_completed_pack_copy_status_sync_v1('{"limit":20}');
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_completed_pack_copy_events where state<>'SENT'),
  'sent outbox state did not synchronise to completion events'
);

select pg_temp.assert_true(
  (select row(financials,invoices,pay_items) from pack_finance_before)
  =row(
    (select count(*) from public.timesheets_financials where timesheet_id in (
      'e5060000-0000-4000-8000-000000000001','e5060000-0000-4000-8000-000000000002')),
    (select count(*) from public.invoice_lines where timesheet_id in (
      'e5060000-0000-4000-8000-000000000001','e5060000-0000-4000-8000-000000000002')),
    (select count(*) from public.pay_batch_items where timesheet_id in (
      'e5060000-0000-4000-8000-000000000001','e5060000-0000-4000-8000-000000000002'))
  ),
  'informational copy changed financial, invoice or payment rows'
);

rollback;
