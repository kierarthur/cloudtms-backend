-- PostgreSQL 17 rollback verification: weekly_source_query_delivery_v1
-- This file creates only transaction-local fixture data and always rolls it back.

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

-- Every accepted Candidate outreach now owns a durable in-app notification.
-- Give each transaction-local Candidate fixture exactly one active app account
-- and membership so this verifier exercises that fail-closed invariant rather
-- than bypassing it.
create or replace function pg_temp.attach_candidate_app_fixture_v1()
returns trigger language plpgsql as $fixture$
declare
  v_account_id uuid:=pg_catalog.gen_random_uuid();
begin
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,notification_preferences_json
  ) values (
    v_account_id,'TEST',new.id::text||'@weekly-query.test.invalid','ACTIVE',
    '{"push":true,"timesheet_expense_attention":true}'::jsonb
  );
  insert into public.candidate_app_global_membership_links(
    membership_id,global_account_identity_hmac,account_id,candidate_id,
    membership_generation,state
  ) values (
    pg_catalog.gen_random_uuid(),
    extensions.digest(pg_catalog.convert_to('weekly-query:'||new.id::text,'UTF8'),'sha256'),
    v_account_id,new.id,1,'ACTIVE'
  );
  return new;
end;
$fixture$;

create trigger weekly_query_verifier_candidate_app_fixture
after insert on public.candidates
for each row execute function pg_temp.attach_candidate_app_fixture_v1();

-- The transport now operates per immutable provider target.  This helper is
-- intentionally transaction-local and lets the older business-policy checks
-- below exercise the new register -> claim -> SUBMISSION_STARTED path without
-- reintroducing the retired command-level transport RPCs.
create or replace function pg_temp.start_one_delivery_target_v1(p_request jsonb)
returns jsonb language plpgsql as $transport$
declare
  v_command_id uuid:=(p_request->>'dispatch_command_id')::uuid;
  v_worker text:=p_request->>'worker_id';
  v_command public.weekly_message_dispatch_commands%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_render public.weekly_message_renders%rowtype;
  v_route public.weekly_manager_recipient_routes%rowtype;
  v_target public.weekly_message_dispatch_targets%rowtype;
  v_claim jsonb;
  v_claimed jsonb;
  v_snapshot_id uuid;
  v_external_id uuid;
  v_fingerprint text;
  v_snapshot_hash text;
  v_provider text;
  v_safe jsonb;
begin
  select * into strict v_command from public.weekly_message_dispatch_commands
  where id=v_command_id;
  if v_command.state='RETIRED' then
    return pg_catalog.jsonb_build_object('ok',false,'reason','REVIEW_CHANGED');
  end if;
  select * into strict v_intent from public.weekly_message_intents
  where id=v_command.message_intent_id;
  select * into strict v_render from public.weekly_message_renders
  where id=v_command.message_render_id;
  select * into v_target from public.weekly_message_dispatch_targets
  where dispatch_command_id=v_command_id order by target_ordinal limit 1;
  if not found then
    if v_intent.audience_kind='MANAGER' then
      select * into strict v_route from public.weekly_manager_recipient_routes
      where id=v_command.recipient_route_id;
      v_snapshot_id:=null;
      v_external_id:=v_route.id;
      v_fingerprint:=pg_catalog.encode(v_route.normalised_recipient_hash,'hex');
      v_snapshot_hash:=pg_catalog.encode(v_render.rendered_content_hash,'hex');
      v_provider:='POWER_AUTOMATE';
      v_safe:=pg_catalog.jsonb_build_object('recipient_route_id',v_route.id);
    else
      v_snapshot_id:=v_command.id;
      v_external_id:=v_command.id;
      v_fingerprint:=coalesce(p_request->>'keyed_target_fingerprint',repeat('91',32));
      v_snapshot_hash:=pg_catalog.encode(v_render.rendered_content_hash,'hex');
      v_provider:='FCM';
      v_safe:=pg_catalog.jsonb_build_object(
        'control_plane_snapshot_id',v_snapshot_id,
        'snapshot_device_id',v_external_id,
        'provider',v_provider,
        'target_revision_hash',v_snapshot_hash
      );
    end if;
    perform public.weekly_source_message_targets_register_atomic_v1(
      pg_catalog.jsonb_build_object(
        'dispatch_command_id',v_command.id,
        'lease_token',p_request->>'lease_token','worker_id',v_worker,
        'control_plane_snapshot_id',v_snapshot_id,'suppression_reason',null,
        'targets',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'external_target_id',v_external_id,'target_fingerprint',v_fingerprint,
          'target_snapshot_hash',v_snapshot_hash,'target_version',1,
          'provider',v_provider,'safe_target_snapshot',v_safe
        ))
      )
    );
  end if;
  select * into strict v_target from public.weekly_message_dispatch_targets
  where dispatch_command_id=v_command_id order by target_ordinal limit 1;
  if v_target.state='SUBMISSION_STARTED' then
    return public.weekly_source_message_dispatch_target_start_atomic_v1(
      pg_catalog.jsonb_build_object(
        'dispatch_target_id',v_target.id,
        'lease_token',coalesce(v_target.lease_token,pg_catalog.gen_random_uuid()),
        'worker_id',coalesce(v_target.lease_owner,v_worker)
      )
    );
  end if;
  v_claim:=public.weekly_source_message_dispatch_target_claim_v1(
    pg_catalog.jsonb_build_object('worker_id',v_worker,'limit',100,'lease_seconds',60)
  );
  select value into strict v_claimed
  from pg_catalog.jsonb_array_elements(v_claim->'targets')
  where value->>'dispatch_command_id'=v_command_id::text;
  return public.weekly_source_message_dispatch_target_start_atomic_v1(
    pg_catalog.jsonb_build_object(
      'dispatch_target_id',v_claimed->>'dispatch_target_id',
      'lease_token',v_claimed->>'lease_token','worker_id',v_worker
    )
  );
end;
$transport$;

create or replace function pg_temp.finish_one_delivery_target_v1(p_request jsonb)
returns jsonb language plpgsql as $transport$
begin
  return public.weekly_source_message_dispatch_target_result_atomic_v1(
    pg_catalog.jsonb_build_object(
      'provider_attempt_id',p_request->>'provider_attempt_id',
      'outcome',p_request->>'outcome',
      'provider_message_id',p_request->>'provider_message_id',
      'bounded_provider_receipt',case
        when nullif(p_request->>'provider_message_id','') is null then '{}'::jsonb
        else pg_catalog.jsonb_build_object(
          'provider_status',202,
          'provider_request_id',p_request->>'provider_message_id'
        ) end,
      'bounded_error',case
        when nullif(p_request#>>'{error,code}','') is null then '{}'::jsonb
        else pg_catalog.jsonb_build_object(
          'error_code',pg_catalog.upper(p_request#>>'{error,code}')
        ) end
    )
  );
end;
$transport$;

-- The isolated Plan 6 schema clone intentionally omits this legacy repeatable;
-- provide its exact call contract transaction-locally and prove rollback below.
create or replace function public._audit_insert(
  p_object_type text,p_object_id_text text,p_action text,p_before_json jsonb,
  p_after_json jsonb,p_reason text,p_actor_user_id uuid
) returns void language plpgsql as $audit$
begin
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,reason
  ) values (
    p_actor_user_id,p_object_type,p_object_id_text,p_action,p_before_json,p_after_json,p_reason
  );
end;
$audit$;

select pg_temp.assert_true(exists(
  select 1 from public.weekly_source_global_settings
  where singleton
    and candidate_reminder_after=interval '6 hours'
    and candidate_response_deadline_after=interval '12 hours'
    and manager_partial_digest_after=interval '6 hours'
    and manager_secure_link_lifetime=interval '7 days'
), 'default 6h candidate reminder, 12h deadline, 6h manager tranche and 7d secure-link lifetime are not installed');

update public.weekly_source_global_settings
set candidate_reminder_after=interval '4 hours',
    candidate_response_deadline_after=interval '10 hours',
    manager_partial_digest_after=interval '3 hours',
    manager_manual_send_cooldown=interval '5 minutes',
    candidate_manual_reminder_cooldown=interval '60 minutes',
    manager_secure_link_lifetime=interval '5 days',version=version+1;

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')
) on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256,
  candidate_home_announcement_sha256=excluded.candidate_home_announcement_sha256;

insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
values ('e1000000-0000-4000-8000-000000000001','weekly-query-office@example.invalid','admin','not-a-real-password','Query verifier',true);

insert into public.clients(id,name,ts_queries_email)
values ('e2000000-0000-4000-8000-000000000001','North Test Trust','manager@example.invalid');
insert into public.client_settings(
  id,client_id,effective_from,hr_validation_required,autoprocess_hr,
  self_bill_no_invoices_sent,no_timesheet_required,requires_hr
) values (
  'e2100000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000001',
  '2026-01-01',true,true,true,true,true
);

insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
values
  ('e3000000-0000-4000-8000-000000000001','CCR-90001','Alex','Nurse','Alex Nurse','alex@example.invalid'),
  ('e3000000-0000-4000-8000-000000000002','CCR-90002','Robin','Nurse','Robin Nurse','robin@example.invalid');

insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr
) values
  ('e4000000-0000-4000-8000-000000000001','e3000000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000001',
   '2026-01-01','2026-12-31','PAYE','{}',true,'HEALTHROSTER',true,true,true),
  ('e4000000-0000-4000-8000-000000000002','e3000000-0000-4000-8000-000000000002','e2000000-0000-4000-8000-000000000001',
   '2026-01-01','2026-12-31','PAYE','{}',true,'HEALTHROSTER',true,true,true);

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  'e5000000-0000-4000-8000-000000000001','TEST','e0000000-0000-4000-8000-000000000001',
  'QUERY_VERIFY_ROSTER','Query verification Roster','ROSTER',3,'15:00'
);
insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
values ('e5000000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000001','2026-01-01','e1000000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,
  candidate_queries_enabled,manager_queries_enabled,manager_query_recipient,created_by_user_id
) values (
  'e5000000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000001','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,'Manager@Example.Invalid',
  'e1000000-0000-4000-8000-000000000001'
);

insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'e6000000-0000-4000-8000-000000000001','e5000000-0000-4000-8000-000000000001',
  '2026-09-06','2026-09-09 15:00:00+00','OPEN',1,'NONE'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
  coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'e7000000-0000-4000-8000-000000000001','e6000000-0000-4000-8000-000000000001','query-v1.xlsx',
  decode(repeat('11',32),'hex'),100,'34444444-4444-4444-8444-444444444444','verify','verify',
  decode(repeat('12',32),'hex'),decode(repeat('13',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',
  0,decode(repeat('14',32),'hex'),'CURRENT','e1000000-0000-4000-8000-000000000001'
);
update public.weekly_source_cycles
set current_complete_upload_id='e7000000-0000-4000-8000-000000000001'
where id='e6000000-0000-4000-8000-000000000001';
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  'e8000000-0000-4000-8000-000000000001','e6000000-0000-4000-8000-000000000001','CYCLE',
  'e7000000-0000-4000-8000-000000000001',1,decode(repeat('15',32),'hex'),decode(repeat('16',32),'hex'),
  'CURRENT',pg_catalog.transaction_timestamp()
);
update public.weekly_source_cycles set projection_state='CURRENT',
  current_projection_publication_id='e8000000-0000-4000-8000-000000000001'
where id='e6000000-0000-4000-8000-000000000001';

insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
) values
  ('e9000000-0000-4000-8000-000000000001','e3000000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000001',
   '2026-09-01','PROFILE_EXTERNAL_KEY','verify-shift-1',decode(repeat('21',32),'hex'),
   'e5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
  ('e9000000-0000-4000-8000-000000000002','e3000000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000001',
   '2026-09-02','PROFILE_EXTERNAL_KEY','verify-shift-2',decode(repeat('22',32),'hex'),
   'e5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444');

insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json
) values (
  'ea000000-0000-4000-8000-000000000001','VERIFY-TS-1','alex-nurse','north-test-trust','ward-a','nurse',
  '2026-09-01 08:00:00+00','2026-09-01 18:00:00+00',30,570,'2026-09-06',
  'verify/nurse.png',repeat('a',64),'e4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
  '[{"date":"2026-09-01","start":"09:00","end":"19:00","break_minutes":30}]'
);

do $test$
declare
  v_result jsonb;
  v_incident_1 uuid;
  v_incident_2 uuid;
  v_incident_3 uuid;
  v_generation_1 uuid;
  v_generation_2 uuid;
  v_candidate_page jsonb;
  v_candidate_response jsonb;
  v_manager_intent uuid;
  v_render_input jsonb;
  v_route_preparation jsonb;
  v_stage jsonb;
  v_claim jsonb;
  v_start jsonb;
  v_review jsonb;
  v_manager_response jsonb;
  v_manual jsonb;
  v_submission jsonb;
  v_submission_generation uuid;
  v_submission_membership uuid;
  v_second_manager_intent uuid;
  v_second_manager_response jsonb;
  v_review_item jsonb;
  v_v3_sync_request jsonb;
  v_candidate_generation_before uuid;
  v_candidate_generation_after uuid;
  v_candidate_generation_count_before integer;
  v_candidate_generation_count_after integer;
  v_manager_generation_before uuid;
  v_manager_generation_after uuid;
  v_dispatch_command uuid;
  v_provider_attempt uuid;
  v_provider_key text;
  v_started timestamptz;
  v_cooldown_rejected boolean:=false;
  v_candidate_stale_rejected boolean:=false;
  v_manager_old_link_rejected boolean:=false;
  v_candidate_toggle_rejected boolean:=false;
  v_manager_toggle_rejected boolean:=false;
  v_timesheet_authority_rejected boolean:=false;
  v_zero_length_rejected boolean:=false;
begin
  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000001',
    'issues',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'work_event_id','e9000000-0000-4000-8000-000000000001',
      'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
      'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('31',32),
      'contract_id','e4000000-0000-4000-8000-000000000001',
      'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
      'candidate_start_at_local','2026-09-01 09:00','candidate_end_at_local','2026-09-01 19:00',
      'candidate_break_minutes',30,'system_start_at_local','2026-09-01 09:00',
      'system_end_at_local','2026-09-01 17:00','system_break_minutes',30
    ))
  ));
  perform pg_temp.assert_true((v_result->>'new_incidents')::integer=1,'initial issue was not created');
  select id into strict v_incident_1 from public.weekly_discrepancy_incidents
  where work_event_id='e9000000-0000-4000-8000-000000000001' and state='OPEN';

  v_result:=public.weekly_source_query_ask_candidate_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000001',
    'candidate_id','e3000000-0000-4000-8000-000000000001',
    'client_id','e2000000-0000-4000-8000-000000000001',
    'incident_ids',pg_catalog.jsonb_build_array(v_incident_1)
  ));
  v_generation_1:=(v_result->>'candidate_generation_id')::uuid;
  select started_at_utc into v_started from public.weekly_candidate_outreach_generations where id=v_generation_1;
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_candidate_outreach_generations
    where id=v_generation_1 and reminder_due_at_utc=started_at_utc+interval '4 hours'
      and deadline_at_utc=started_at_utc+interval '10 hours'
  ),'configured 4h/10h candidate clocks were not used');

  -- Install a newer complete publication containing the same fact. It must not resend.
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
    coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id
  ) values (
    'e7000000-0000-4000-8000-000000000002','e6000000-0000-4000-8000-000000000001','query-v2.xlsx',
    decode(repeat('41',32),'hex'),100,'34444444-4444-4444-8444-444444444444','verify','verify',
    decode(repeat('42',32),'hex'),decode(repeat('43',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',
    0,decode(repeat('44',32),'hex'),'SEALED','e1000000-0000-4000-8000-000000000001'
  );
  update public.weekly_source_uploads set state='SUPERSEDED' where id='e7000000-0000-4000-8000-000000000001';
  update public.weekly_source_uploads set state='CURRENT' where id='e7000000-0000-4000-8000-000000000002';
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state,published_at_utc
  ) values (
    'e8000000-0000-4000-8000-000000000002','e6000000-0000-4000-8000-000000000001','CYCLE',
    'e7000000-0000-4000-8000-000000000002',2,decode(repeat('45',32),'hex'),decode(repeat('46',32),'hex'),
    'BUILDING',pg_catalog.transaction_timestamp()
  );
  update public.weekly_source_projection_publications set state='STALE' where id='e8000000-0000-4000-8000-000000000001';
  update public.weekly_source_projection_publications set state='CURRENT' where id='e8000000-0000-4000-8000-000000000002';
  update public.weekly_source_cycles set version=2,
    current_complete_upload_id='e7000000-0000-4000-8000-000000000002',
    current_projection_publication_id='e8000000-0000-4000-8000-000000000002'
  where id='e6000000-0000-4000-8000-000000000001';

  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000002',
    'issues',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'work_event_id','e9000000-0000-4000-8000-000000000001',
      'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
      'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('31',32),
      'contract_id','e4000000-0000-4000-8000-000000000001',
      'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
      'candidate_start_at_local','2026-09-01 09:00','candidate_end_at_local','2026-09-01 19:00',
      'candidate_break_minutes',30,'system_start_at_local','2026-09-01 09:00',
      'system_end_at_local','2026-09-01 17:00','system_break_minutes',30
    ))
  ));
  perform pg_temp.assert_true((v_result->>'unchanged_incidents')::integer=1,'unchanged re-import changed the incident');
  perform pg_temp.assert_true((select count(*) from public.weekly_candidate_outreach_generations)=1,
    'unchanged re-import resent candidate outreach');

  -- A genuinely new issue resets the active cohort and includes every open issue.
  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000002',
    'issues',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000001',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('31',32),
        'contract_id','e4000000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-01 09:00','candidate_end_at_local','2026-09-01 19:00',
        'candidate_break_minutes',30,'system_start_at_local','2026-09-01 09:00',
        'system_end_at_local','2026-09-01 17:00','system_break_minutes',30
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000002',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('32',32),
        'contract_id','e4000000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_MISSING_OR_NOT_AUTHORISED','source_presence','ABSENT',
        'candidate_start_at_local','2026-09-02 20:00','candidate_end_at_local','2026-09-03 08:00',
        'candidate_break_minutes',60
      )
    )
  ));
  perform pg_temp.assert_true((v_result->>'new_incidents')::integer=1,'new issue was not created');
  select id into strict v_incident_2 from public.weekly_discrepancy_incidents
  where work_event_id='e9000000-0000-4000-8000-000000000002' and state='OPEN';
  select id into strict v_generation_2 from public.weekly_candidate_outreach_generations
  where state='ACTIVE';
  perform pg_temp.assert_true(v_generation_2<>v_generation_1,'new issue did not reset the generation');
  perform pg_temp.assert_true((select count(*) from public.weekly_candidate_outreach_memberships
    where candidate_generation_id=v_generation_2 and state='ACTIONABLE')=2,
    'new generation did not include all open issues');

  v_candidate_page:=public.weekly_source_candidate_query_get_v1(pg_catalog.jsonb_build_object(
    'candidate_id','e3000000-0000-4000-8000-000000000001',
    'candidate_generation_id',v_generation_2,
    'projection_publication_id','e8000000-0000-4000-8000-000000000002'
  ));
  v_candidate_response:=pg_catalog.jsonb_build_object(
    'candidate_id','e3000000-0000-4000-8000-000000000001',
    'candidate_generation_id',v_generation_2,
    'projection_publication_id','e8000000-0000-4000-8000-000000000002',
    'request_idempotency_key','eb000000-0000-4000-8000-000000000001',
    'responses',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'incident_id',v_candidate_page->'items'->0->>'incident_id',
      'expected_comparison_fingerprint',v_candidate_page->'items'->0->>'current_fact_version',
      'expected_timesheet_hash',v_candidate_page->'items'->0->>'expected_timesheet_hash',
      'choice','CANDIDATE_CORRECT'
    ))
  );
  v_result:=public.weekly_source_candidate_response_submit_atomic_v1(v_candidate_response);
  perform pg_temp.assert_true((v_result->>'answered_count')::integer=1,'candidate answer not accepted');
  perform pg_temp.assert_true(exists(select 1 from public.office_action_notifications
    where issue_id=(v_candidate_page->'items'->0->>'incident_id')::uuid
      and event_kind='WEEKLY_CANDIDATE_SOURCE_DISPUTED'),
    'candidate source-dispute Office alert missing');
  v_result:=public.weekly_source_candidate_response_submit_atomic_v1(v_candidate_response);
  perform pg_temp.assert_true((v_result->>'replay')::boolean,
    'candidate exact replay was not receipt-first');
  begin
    perform public.weekly_source_candidate_response_submit_atomic_v1(
      pg_catalog.jsonb_set(
        v_candidate_response,'{request_idempotency_key}',
        '"eb000000-0000-4000-8000-000000000002"'::jsonb
      )
    );
  exception when sqlstate '40001' then
    v_candidate_stale_rejected:=sqlerrm='WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE';
  end;
  perform pg_temp.assert_true(v_candidate_stale_rejected,
    'candidate response with a new key bypassed stale membership rejection');

  -- At the configured manager partial time, only the answered shift becomes one digest.
  perform public.weekly_source_query_scheduler_tick_v1(pg_catalog.jsonb_build_object(
    'now_utc',v_started+interval '3 hours','limit',100
  ));
  select id into strict v_manager_intent from public.weekly_message_intents
  where audience_kind='MANAGER' and tranche_kind='MANAGER_T6_RESPONDED' and state='DUE';
  perform public.weekly_source_query_scheduler_tick_v1(pg_catalog.jsonb_build_object(
    'now_utc',v_started+interval '11 hours','limit',100
  ));
  perform pg_temp.assert_true(not exists(
    select 1 from public.weekly_message_intents
    where audience_kind='MANAGER' and tranche_kind='MANAGER_T12_REMAINDER'
  ) and exists(
    select 1 from public.weekly_manager_cohort_due_events
    where event_kind='T12_REMAINDER' and state='PENDING'
  ),'later manager tranche duplicated rows while an earlier digest was pending');
  v_render_input:=public.weekly_source_message_render_input_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_manager_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000002'
  ));
  perform pg_temp.assert_true((v_render_input->>'shift_count')::integer=1,'partial manager digest was not one shift');
  v_route_preparation:=public.weekly_source_manager_route_prepare_atomic_v1(
    pg_catalog.jsonb_build_object(
      'message_intent_id',v_manager_intent,
      'projection_publication_id','e8000000-0000-4000-8000-000000000002'
    )
  );
  perform pg_temp.assert_true(
    (v_route_preparation->>'expires_at_utc')::timestamptz
      -(v_route_preparation->>'issued_at_utc')::timestamptz=interval '5 days',
    'configured manager secure-link lifetime was not frozen into the route preparation'
  );
  v_stage:=public.weekly_source_message_render_stage_atomic_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_manager_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000002',
    'membership_hash',v_render_input->>'membership_hash','policy_version','1.8.0',
    'renderer_version','1.4.0','structure_version','1.0.0',
    'subject_text','Timesheet queries requiring your review - 1 shift',
    'html_body','<html><body><table><tr><td>Alex Nurse</td><td>Tue 1 Sep 2026</td><td>09:00-19:00 (30 min break)</td><td>09:00-17:00 (30 min break)</td></tr></table><a href="https://example.invalid/review">Review all queries</a></body></html>',
    'plain_body','Alex Nurse - Tue 1 Sep 2026 - 09:00-19:00 (30 min break) - 09:00-17:00 (30 min break). Review all queries.',
    'credential_hash',repeat('51',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000001',
    'agency_receipt_id','ed000000-0000-4000-8000-000000000001',
    'data_plane_identity','agency-test','route_version','v1','credential_version','v1'
    ,'manager_route_preparation_id',v_route_preparation->>'manager_route_preparation_id'
  ));
  v_claim:=public.weekly_source_message_dispatch_claim_v1(pg_catalog.jsonb_build_object(
    'worker_id','query-verifier','limit',1,'lease_seconds',60
  ));
  perform pg_temp.assert_true((v_claim->>'claimed_count')::integer=1,'manager dispatch not claimed');
  v_start:=pg_temp.start_one_delivery_target_v1(pg_catalog.jsonb_build_object(
    'dispatch_command_id',v_claim->'commands'->0->>'dispatch_command_id',
    'lease_token',v_claim->'commands'->0->>'lease_token','worker_id','query-verifier',
    'channel','EMAIL','target_kind','MANAGER_ADDRESS','keyed_target_fingerprint',repeat('52',32)
  ));
  perform pg_temp.finish_one_delivery_target_v1(pg_catalog.jsonb_build_object(
    'provider_attempt_id',v_start->>'provider_attempt_id','outcome','ACCEPTED',
    'provider_message_id','verify-message-1','receipt',pg_catalog.jsonb_build_object('accepted',true)
  ));
  perform pg_temp.assert_true(exists(select 1 from public.weekly_discrepancy_incidents
    where id=(v_candidate_page->'items'->0->>'incident_id')::uuid and manager_action_state='SENT'),
    'Manager informed was marked before or not at provider acceptance');

  v_review:=public.weekly_source_manager_review_get_v1(pg_catalog.jsonb_build_object(
    'credential_hash',repeat('51',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000001'
  ));
  perform pg_temp.assert_true((v_review->>'remaining_count')::integer=1,'manager review did not expose one authorised row');
  v_manager_response:=pg_catalog.jsonb_build_object(
    'credential_hash',repeat('51',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000001',
    'request_idempotency_key','ee000000-0000-4000-8000-000000000001',
    'review_batch_version',v_review->>'review_batch_version',
    'response_fingerprint',v_review->>'response_fingerprint',
    'responses',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'review_item_id',v_review->'items'->0->>'review_item_id',
      'incident_episode',v_review->'items'->0->>'incident_episode',
      'current_fact_version',v_review->'items'->0->>'current_fact_version',
      'response_kind','SYSTEM_CORRECT'
    ))
  );
  v_result:=public.weekly_source_manager_review_respond_atomic_v1(v_manager_response);
  perform pg_temp.assert_true((v_result->>'answered_count')::integer=1,'manager response not accepted');
  perform pg_temp.assert_true(exists(select 1 from public.office_action_notifications
    where issue_id=(v_candidate_page->'items'->0->>'incident_id')::uuid
      and event_kind='WEEKLY_MANAGER_SYSTEM_CONFIRMED'),
    'manager confirmation Office alert missing');
  v_result:=public.weekly_source_manager_review_respond_atomic_v1(v_manager_response);
  perform pg_temp.assert_true((v_result->>'replay')::boolean,'manager exact replay was not receipt-first');
  v_review:=public.weekly_source_manager_review_get_v1(pg_catalog.jsonb_build_object(
    'credential_hash',repeat('51',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000001'
  ));
  perform pg_temp.assert_true((v_review->>'remaining_count')::integer=0,'answered manager row reappeared');

  -- At the configured candidate deadline, the still-unanswered Timesheet row
  -- becomes the one manager remainder; the earlier answered row is not repeated.
  perform public.weekly_source_query_scheduler_tick_v1(pg_catalog.jsonb_build_object(
    'now_utc',v_started+interval '11 hours','limit',100
  ));
  select id into strict v_manager_intent from public.weekly_message_intents
  where audience_kind='MANAGER' and tranche_kind='MANAGER_T12_REMAINDER' and state='DUE';
  v_render_input:=public.weekly_source_message_render_input_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_manager_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000002'
  ));
  perform pg_temp.assert_true((v_render_input->>'shift_count')::integer=1
    and v_render_input->'rows'->0->>'issueId'=v_incident_2::text,
    'candidate-deadline manager remainder repeated or omitted the wrong row');

  -- Manual manager send is recipient-wide and the second acceptance is blocked for five minutes.
  v_manual:=public.weekly_source_query_send_manager_now_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000002',
    'incident_ids',pg_catalog.jsonb_build_array(v_incident_2)
  ));
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_route_activations
    where source_cycle_id='e6000000-0000-4000-8000-000000000001'
      and candidate_id='e3000000-0000-4000-8000-000000000001'
      and client_id='e2000000-0000-4000-8000-000000000001'
      and audience_route='CANDIDATE' and route_mode='MANAGER_DIRECT'
  ) and exists(
    select 1 from public.weekly_candidate_outreach_generations
    where id=v_generation_2 and state='CANCELLED'
  ) and not exists(
    select 1 from public.weekly_message_intents
    where candidate_generation_id=v_generation_2 and state in ('DUE','RENDERED')
  ) and exists(
    select 1 from public.weekly_discrepancy_incidents
    where id=v_incident_2 and candidate_action_state='NOT_REQUIRED'
  ),'Office manager-direct choice did not stop unattempted candidate outreach');
  perform pg_temp.assert_true(not exists(
    select 1
    from public.weekly_manager_cohort_due_events due_event
    join public.weekly_message_intents intent
      on due_event.id=any(intent.sorted_due_event_ids)
    join public.weekly_candidate_cohorts cohort on cohort.id=due_event.candidate_cohort_id
    where intent.id=(v_manual->>'message_intent_id')::uuid
      and (cohort.candidate_id,cohort.client_id)<>(
        'e3000000-0000-4000-8000-000000000001'::uuid,
        'e2000000-0000-4000-8000-000000000001'::uuid
      )
  ),'manual manager digest included a cohort outside the Office selection');
  begin
    perform public.weekly_source_query_send_manager_now_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','e1000000-0000-4000-8000-000000000001',
      'source_cycle_id','e6000000-0000-4000-8000-000000000001',
      'projection_publication_id','e8000000-0000-4000-8000-000000000002',
      'incident_ids',pg_catalog.jsonb_build_array(v_incident_2)
    ));
  exception when sqlstate '55000' then
    v_cooldown_rejected:=sqlerrm='WEEKLY_SOURCE_MANAGER_SEND_COOLDOWN';
  end;
  perform pg_temp.assert_true(v_cooldown_rejected,'five-minute manager send cooldown not enforced');

  -- Missing Timesheet uses the submit route and never creates a manager activation/email.
  v_submission:=public.weekly_source_timesheet_submission_request_start_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000002',
    'candidate_id','e3000000-0000-4000-8000-000000000002',
    'scopes',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'week_ending','2026-09-06','client_id','e2000000-0000-4000-8000-000000000001',
      'contract_id','e4000000-0000-4000-8000-000000000002',
      'expected_source_fingerprint',repeat('61',32)
    ))
  ));
  v_submission_generation:=(v_submission->>'candidate_generation_id')::uuid;
  perform pg_temp.assert_true(not exists(select 1 from public.weekly_route_activations
    where candidate_id='e3000000-0000-4000-8000-000000000002' and audience_route='MANAGER'),
    'missing Timesheet incorrectly enabled manager send');
  perform public.weekly_source_query_scheduler_tick_v1(pg_catalog.jsonb_build_object(
    'now_utc',(select started_at_utc+interval '11 hours' from public.weekly_candidate_outreach_generations
               where id=v_submission_generation),'limit',100
  ));
  perform pg_temp.assert_true(exists(select 1 from public.weekly_timesheet_submission_requests
    where id=(v_submission->>'submission_request_id')::uuid and state='OVERDUE'),
    'missing Timesheet request did not become overdue at its configured deadline');
  v_result:=public.weekly_source_candidate_query_get_v1(pg_catalog.jsonb_build_object(
    'candidate_id','e3000000-0000-4000-8000-000000000002',
    'candidate_generation_id',v_submission_generation,
    'projection_publication_id','e8000000-0000-4000-8000-000000000002'
  ));
  perform pg_temp.assert_true(v_result->>'request_kind'='SUBMIT_TIMESHEET','missing Timesheet CTA is not the submit route');

  -- A submitted Timesheet is compared first; its issue must not replace the submit route
  -- with an hours-check route before completion activates the manager directly.
  insert into public.weekly_work_events(
    id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
    durable_identity_hash,first_source_group_id,source_format_profile_id
  ) values (
    'e9000000-0000-4000-8000-000000000003','e3000000-0000-4000-8000-000000000002',
    'e2000000-0000-4000-8000-000000000001','2026-09-03','PROFILE_EXTERNAL_KEY',
    'verify-shift-3',decode(repeat('23',32),'hex'),
    'e5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'
  );
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
    r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json
  ) values (
    'ea000000-0000-4000-8000-000000000002','VERIFY-TS-2','robin-nurse',
    'north-test-trust','ward-b','nurse','2026-09-03 08:00:00+00',
    '2026-09-03 17:00:00+00',30,510,'2026-09-06','verify/nurse-2.png',repeat('b',64),
    'e4000000-0000-4000-8000-000000000002','WEEKLY','HOURS',
    '[{"date":"2026-09-03","start":"08:00","end":"17:00","break_minutes":30}]'
  );
  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000002',
    'issues',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000002',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('32',32),
        'contract_id','e4000000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_MISSING_OR_NOT_AUTHORISED','source_presence','ABSENT',
        'candidate_start_at_local','2026-09-02 20:00','candidate_end_at_local','2026-09-03 08:00',
        'candidate_break_minutes',60
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000003',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000002',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('62',32),
        'contract_id','e4000000-0000-4000-8000-000000000002',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-03 08:00','candidate_end_at_local','2026-09-03 17:00',
        'candidate_break_minutes',30,'system_start_at_local','2026-09-03 08:00',
        'system_end_at_local','2026-09-03 16:00','system_break_minutes',30
      )
    )
  ));
  perform pg_temp.assert_true((v_result->>'new_incidents')::integer=1,
    'submitted Timesheet issue was not synchronised');
  select id into strict v_incident_3 from public.weekly_discrepancy_incidents
  where work_event_id='e9000000-0000-4000-8000-000000000003' and state='OPEN';
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_candidate_outreach_generations
    where id=v_submission_generation and state='ACTIVE' and request_kind='SUBMIT_TIMESHEET'
  ) and not exists(
    select 1 from public.weekly_candidate_outreach_generations
    where candidate_id='e3000000-0000-4000-8000-000000000002'
      and request_kind='CHECK_HOURS'
  ),'issue comparison incorrectly replaced the missing-Timesheet submit route');

  select id into strict v_submission_membership
  from public.weekly_timesheet_submission_request_memberships
  where submission_request_id=(v_submission->>'submission_request_id')::uuid
    and client_id='e2000000-0000-4000-8000-000000000001'
    and contract_id='e4000000-0000-4000-8000-000000000002';
  v_result:=public.weekly_source_timesheet_submission_complete_atomic_v1(
    pg_catalog.jsonb_build_object(
      'candidate_id','e3000000-0000-4000-8000-000000000002',
      'submission_request_id',v_submission->>'submission_request_id',
      'projection_publication_id','e8000000-0000-4000-8000-000000000002',
      'completions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'membership_id',v_submission_membership,
        'timesheet_id','ea000000-0000-4000-8000-000000000002',
        'timesheet_revision',1,
        'timesheet_hash',pg_catalog.encode(
          private.weekly_source_query_candidate_timesheet_hash_v1(
            'ea000000-0000-4000-8000-000000000002'
          ),'hex'
        ),
        'outcome','ISSUES'
      ))
    )
  );
  perform pg_temp.assert_true((v_result->>'issue_scope_count')::integer=1
    and pg_catalog.jsonb_array_length(v_result->'manager_message_intent_ids')=1,
    'submitted Timesheet issue did not activate one manager digest');
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_route_activations
    where source_cycle_id='e6000000-0000-4000-8000-000000000001'
      and candidate_id='e3000000-0000-4000-8000-000000000002'
      and audience_route='MANAGER' and route_mode='MANAGER_DIRECT'
  ),'missing-Timesheet issue did not activate the manager-direct route');
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_discrepancy_incidents
    where id=v_incident_3 and candidate_action_state='NOT_REQUIRED'
      and manager_action_state='DUE'
  ),'candidate was incorrectly asked to check an issue found after submission');

  -- A single manager address receives one digest for all applicable candidates.
  v_second_manager_intent:=(v_result->'manager_message_intent_ids'->>0)::uuid;
  v_render_input:=public.weekly_source_message_render_input_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_second_manager_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000002'
  ));
  perform pg_temp.assert_true((v_render_input->>'shift_count')::integer=2
    and (v_render_input->>'candidate_count')::integer=2,
    'manager digest was not grouped across both candidates at one address');
  v_route_preparation:=public.weekly_source_manager_route_prepare_atomic_v1(
    pg_catalog.jsonb_build_object(
      'message_intent_id',v_second_manager_intent,
      'projection_publication_id','e8000000-0000-4000-8000-000000000002'
    )
  );
  v_stage:=public.weekly_source_message_render_stage_atomic_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_second_manager_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000002',
    'membership_hash',v_render_input->>'membership_hash','policy_version','1.8.0',
    'renderer_version','1.4.0','structure_version','1.0.0',
    'subject_text','Timesheet queries requiring your review - 2 shifts',
    'html_body','<html><body><table><tr><td>Alex Nurse</td><td>Wed 2 Sep 2026</td><td>20:00-08:00 (60 min break)</td></tr><tr><td>Robin Nurse</td><td>Thu 3 Sep 2026</td><td>08:00-17:00 (30 min break)</td><td>08:00-16:00 (30 min break)</td></tr></table><a href="https://example.invalid/review-2">Review all queries</a></body></html>',
    'plain_body','Alex Nurse - Wed 2 Sep 2026 - 20:00-08:00 (60 min break). Robin Nurse - Thu 3 Sep 2026 - 08:00-17:00 (30 min break) - 08:00-16:00 (30 min break). Review all queries.',
    'credential_hash',repeat('71',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000002',
    'agency_receipt_id','ed000000-0000-4000-8000-000000000002',
    'data_plane_identity','agency-test','route_version','v1','credential_version','v1'
    ,'manager_route_preparation_id',v_route_preparation->>'manager_route_preparation_id'
  ));
  v_claim:=public.weekly_source_message_dispatch_claim_v1(pg_catalog.jsonb_build_object(
    'worker_id','query-verifier-2','limit',1,'lease_seconds',60
  ));
  perform pg_temp.assert_true(v_claim->'commands'->0->>'dispatch_command_id'=v_stage->>'dispatch_command_id',
    'two-candidate manager digest dispatch was not claimed');
  v_start:=pg_temp.start_one_delivery_target_v1(pg_catalog.jsonb_build_object(
    'dispatch_command_id',v_claim->'commands'->0->>'dispatch_command_id',
    'lease_token',v_claim->'commands'->0->>'lease_token','worker_id','query-verifier-2',
    'channel','EMAIL','target_kind','MANAGER_ADDRESS','keyed_target_fingerprint',repeat('72',32)
  ));
  perform pg_temp.finish_one_delivery_target_v1(pg_catalog.jsonb_build_object(
    'provider_attempt_id',v_start->>'provider_attempt_id','outcome','ACCEPTED',
    'provider_message_id','verify-message-2','receipt',pg_catalog.jsonb_build_object('accepted',true)
  ));
  v_review:=public.weekly_source_manager_review_get_v1(pg_catalog.jsonb_build_object(
    'credential_hash',repeat('71',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000002'
  ));
  perform pg_temp.assert_true((v_review->>'remaining_count')::integer=2,
    'two-candidate manager review did not expose both rows');
  select value into strict v_review_item
  from pg_catalog.jsonb_array_elements(v_review->'items')
  where value->>'review_item_id'=(
    select item.id::text from public.weekly_manager_review_items item
    where item.review_batch_id=(v_review->>'review_batch_id')::uuid
      and item.incident_id=v_incident_2
  );
  v_second_manager_response:=pg_catalog.jsonb_build_object(
    'credential_hash',repeat('71',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000002',
    'request_idempotency_key','ee000000-0000-4000-8000-000000000002',
    'review_batch_version',v_review->>'review_batch_version',
    'response_fingerprint',v_review->>'response_fingerprint',
    'responses',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'review_item_id',v_review_item->>'review_item_id',
      'incident_episode',v_review_item->>'incident_episode',
      'current_fact_version',v_review_item->>'current_fact_version',
      'response_kind','MANAGER_CORRECTED_SOURCE',
      'intended_start','20:00',
      'intended_end','08:00',
      'intended_break_minutes',60
    ))
  );
  v_result:=public.weekly_source_manager_review_respond_atomic_v1(v_second_manager_response);
  perform pg_temp.assert_true((v_result->>'answered_count')::integer=1
    and (v_result->>'remaining_count')::integer=1,
    'manager could not answer only one row and leave one for later');
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_discrepancy_incidents
    where id=v_incident_2 and manager_action_state='RESPONDED'
      and waiting_source_state='WAITING_REIMPORT'
      and reconciliation_state='WAITING_FOR_SOURCE'
  ),'manager correction did not wait for a new system-hours check');
  -- 04A section 4: the corrected response "records the manager's intended
  -- start, end and break".  Section 6 then expects Office to compare that
  -- statement with the later accepted source, so the Office alert must actually
  -- carry the figures the manager gave; a recorded value no surface reads
  -- cannot be compared with anything (WP-42 finding F5).  Section 2's
  -- prohibited values stay prohibited: the fan-out owner refuses any pay,
  -- charge, rate, margin, VAT, invoice or banking key.
  perform pg_temp.assert_true(exists(
    select 1 from public.office_action_notifications
    where issue_id=v_incident_2 and event_kind='WEEKLY_MANAGER_SOURCE_CORRECTED'
      and payload_json ? 'manager_intended_start'
      and payload_json ? 'manager_intended_end'
      and (payload_json->>'manager_intended_break_minutes')::integer=60
      and not payload_json ?| array['pay','pay_rate','charge','charge_rate',
                                    'margin','vat','invoice','banking']
  ),'manager corrected-hours Office alert missing, or missing the hours the manager gave');
  v_review:=public.weekly_source_manager_review_get_v1(pg_catalog.jsonb_build_object(
    'credential_hash',repeat('71',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000002'
  ));
  perform pg_temp.assert_true((v_review->>'remaining_count')::integer=1
    and v_review->'items'->0->>'candidate_name'='Robin Nurse',
    'answered manager row did not disappear while the remaining row stayed available');

  -- WP-42 finding F7.  `14 section 5.3.4`: "If start equals end, the row blocks
  -- because neither accepted profile supplies an explicit 24-hour marker."
  -- `03 section 19A` step 1 gives the money owner the same rule - reject "an
  -- unsupported equal-clock 24-hour interpretation" - and `03` again at the
  -- HealthRoster/NHSP admission rule: "only an end earlier than start means
  -- overnight".  A manager correction is a statement about what an accepted
  -- source row should say, so it obeys the same rule: an equal start and end is
  -- refused outright, never rolled to the next calendar day.  This is now
  -- visible: since WP-45 fixed F5 the same three figures ride the Office notice
  -- as plain text, so a zero-length correction would have been shown to a person
  -- as a full day.
  select value into strict v_review_item
  from pg_catalog.jsonb_array_elements(v_review->'items')
  where value->>'candidate_name'='Robin Nurse';
  begin
    perform public.weekly_source_manager_review_respond_atomic_v1(
      pg_catalog.jsonb_build_object(
        'credential_hash',repeat('71',32),
        'control_plane_ticket_id','ec000000-0000-4000-8000-000000000002',
        'request_idempotency_key','ee000000-0000-4000-8000-000000000049',
        'review_batch_version',v_review->>'review_batch_version',
        'response_fingerprint',v_review->>'response_fingerprint',
        'responses',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'review_item_id',v_review_item->>'review_item_id',
          'incident_episode',v_review_item->>'incident_episode',
          'current_fact_version',v_review_item->>'current_fact_version',
          'response_kind','MANAGER_CORRECTED_SOURCE',
          'intended_start','09:00',
          'intended_end','09:00',
          'intended_break_minutes',60
        ))
      )
    );
  exception when sqlstate '22023' then
    v_zero_length_rejected:=sqlerrm='WEEKLY_SOURCE_MANAGER_INTENDED_HOURS_INVALID';
  end;
  perform pg_temp.assert_true(v_zero_length_rejected,
    'a manager correction whose start equals its end was not refused');
  perform pg_temp.assert_true((select item.response_state
      from public.weekly_manager_review_items item
      where item.id=(v_review_item->>'review_item_id')::uuid)='UNANSWERED',
    'the refused zero-length correction still answered the review item');
  perform pg_temp.assert_true(not exists(
    select 1 from public.weekly_manager_review_items item
    where item.intended_end_at_local
            =item.intended_start_at_local+pg_catalog.make_interval(days=>1)
       or item.intended_end_at_local=item.intended_start_at_local
  ),'a manager review item recorded a zero-length or 24-hour intended shift');
  perform pg_temp.assert_true(not exists(
    select 1 from public.office_action_notifications
    where event_kind='WEEKLY_MANAGER_SOURCE_CORRECTED'
      and payload_json->>'manager_intended_start'
            =payload_json->>'manager_intended_end'
  ),'an Office notice carried a zero-length manager correction');
  -- The genuine overnight correction answered above is the sentinel that must
  -- NOT change: 20:00 to 08:00 still belongs to the following calendar day, and
  -- the Office notice still carries that exact next-day figure.  It is twelve
  -- hours, and it must stay twelve hours.
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_manager_review_items item
    where item.incident_id=v_incident_2
      and item.response_kind='MANAGER_CORRECTED_SOURCE'
      and item.intended_start_at_local='2026-09-02 20:00:00'::timestamp
      and item.intended_end_at_local='2026-09-03 08:00:00'::timestamp
      and item.intended_break_minutes=60
  ),'the genuine overnight manager correction no longer rolls to the next day');
  perform pg_temp.assert_true(exists(
    select 1 from public.office_action_notifications
    where issue_id=v_incident_2 and event_kind='WEEKLY_MANAGER_SOURCE_CORRECTED'
      and payload_json->>'manager_intended_start'='2026-09-02 20:00'
      and payload_json->>'manager_intended_end'='2026-09-03 08:00'
  ),'the Office notice no longer carries the exact overnight correction figures');

  -- Publish changed system hours. This is a genuinely new incident revision: it
  -- restarts both channels and includes every still-open issue in the new digest.
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
    coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id
  ) values (
    'e7000000-0000-4000-8000-000000000003','e6000000-0000-4000-8000-000000000001','query-v3.xlsx',
    decode(repeat('81',32),'hex'),100,'34444444-4444-4444-8444-444444444444','verify','verify',
    decode(repeat('82',32),'hex'),decode(repeat('83',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',
    0,decode(repeat('84',32),'hex'),'SEALED','e1000000-0000-4000-8000-000000000001'
  );
  update public.weekly_source_uploads set state='SUPERSEDED'
  where id='e7000000-0000-4000-8000-000000000002';
  update public.weekly_source_uploads set state='CURRENT'
  where id='e7000000-0000-4000-8000-000000000003';
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state,published_at_utc
  ) values (
    'e8000000-0000-4000-8000-000000000003','e6000000-0000-4000-8000-000000000001','CYCLE',
    'e7000000-0000-4000-8000-000000000003',3,decode(repeat('85',32),'hex'),decode(repeat('86',32),'hex'),
    'BUILDING',pg_catalog.transaction_timestamp()
  );
  update public.weekly_source_projection_publications set state='STALE'
  where id='e8000000-0000-4000-8000-000000000002';
  update public.weekly_source_projection_publications set state='CURRENT'
  where id='e8000000-0000-4000-8000-000000000003';
  update public.weekly_source_cycles set version=3,
    current_complete_upload_id='e7000000-0000-4000-8000-000000000003',
    current_projection_publication_id='e8000000-0000-4000-8000-000000000003'
  where id='e6000000-0000-4000-8000-000000000001';

  select pg_catalog.count(*) into v_candidate_generation_count_before
  from public.weekly_candidate_outreach_generations
  where candidate_id='e3000000-0000-4000-8000-000000000001';
  select current_generation_id into strict v_manager_generation_before
  from public.weekly_manager_recipient_routes where current_generation_id is not null;
  v_v3_sync_request:=pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000003',
    'issues',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000002',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('32',32),
        'contract_id','e4000000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-02 20:00','candidate_end_at_local','2026-09-03 08:00',
        'candidate_break_minutes',60,'system_start_at_local','2026-09-02 20:00',
        'system_end_at_local','2026-09-03 07:00','system_break_minutes',60
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000003',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000002',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('62',32),
        'contract_id','e4000000-0000-4000-8000-000000000002',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-03 08:00','candidate_end_at_local','2026-09-03 17:00',
        'candidate_break_minutes',30,'system_start_at_local','2026-09-03 08:00',
        'system_end_at_local','2026-09-03 16:00','system_break_minutes',30
      )
    )
  );
  v_result:=public.weekly_source_query_sync_atomic_v1(v_v3_sync_request);
  perform pg_temp.assert_true((v_result->>'changed_comparisons')::integer=1
    and (v_result->>'unchanged_incidents')::integer=1,
    'changed system hours were not distinguished from the unchanged issue');
  select pg_catalog.count(*) into v_candidate_generation_count_after
  from public.weekly_candidate_outreach_generations
  where candidate_id='e3000000-0000-4000-8000-000000000001';
  select current_generation_id into strict v_manager_generation_after
  from public.weekly_manager_recipient_routes where current_generation_id is not null;
  perform pg_temp.assert_true(v_candidate_generation_count_after=v_candidate_generation_count_before
    and not exists(
      select 1 from public.weekly_candidate_outreach_generations
      where candidate_id='e3000000-0000-4000-8000-000000000001' and state='ACTIVE'
    ) and exists(
      select 1 from public.weekly_route_activations
      where source_cycle_id='e6000000-0000-4000-8000-000000000001'
        and candidate_id='e3000000-0000-4000-8000-000000000001'
        and audience_route='CANDIDATE' and route_mode='MANAGER_DIRECT'
    ) and v_manager_generation_after<>v_manager_generation_before,
    'changed issue did not preserve manager-direct while resetting manager outreach');
  perform pg_temp.assert_true((select episode_number from public.weekly_discrepancy_incidents
    where id=v_incident_2)=2,'changed issue did not increment its episode');
  perform pg_temp.assert_true((select count(*) from public.weekly_manager_recipient_memberships
    where recipient_generation_id=v_manager_generation_after)=2,
    'new manager generation did not include every unresolved issue');
  -- 04A section 8: "A later cohort merely becoming due is not a revocation
  -- event for an earlier accepted batch", and section 1: a later generation
  -- "cannot broaden, alter, supersede or revoke an already accepted earlier
  -- batch merely by becoming due."  The accepted batch, its receipt and the
  -- manager's saved answer therefore survive this rotation, and the link keeps
  -- showing the batch's own remaining membership.
  --
  -- This assertion previously required the opposite -- batch and receipt
  -- REVOKED -- and passed.  Corrected by WP-45 against the sealed pack, which
  -- outranks both the contract and the shipped assertion (WP-42 finding F1).
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_manager_review_batches
    where id=(v_stage->>'review_batch_id')::uuid and state='ACTIVE'
  ) and exists(
    select 1 from public.weekly_manager_route_receipts
    where review_batch_id=(v_stage->>'review_batch_id')::uuid and state='ACTIVE'
  ),'a later cohort revoked an earlier accepted manager batch');
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_manager_review_items item
    where item.review_batch_id=(v_stage->>'review_batch_id')::uuid
      and item.incident_id=v_incident_2 and item.response_state='ANSWERED'
  ),'the manager answer was destroyed by a later cohort');
  v_review:=public.weekly_source_manager_review_get_v1(pg_catalog.jsonb_build_object(
    'credential_hash',repeat('71',32),
    'control_plane_ticket_id','ec000000-0000-4000-8000-000000000002'
  ));
  perform pg_temp.assert_true((v_review->>'remaining_count')::integer=1,
    'the accepted manager link stopped working when a later cohort became due');
  -- 04A section 6.1 "never resends a row already in an accepted batch" and
  -- section 6.5 "No row is sent twice": the row still unanswered in that
  -- accepted batch must not reappear in the new generation's sendable set,
  -- while the row whose episode genuinely changed may.
  perform pg_temp.assert_true(
    private.weekly_source_query_manager_row_owned_v1(
      (select recipient_route_id from public.weekly_manager_recipient_generations
       where id=v_manager_generation_after),
      v_incident_1,
      (select episode_number from public.weekly_discrepancy_incidents where id=v_incident_1)
    )
    and not private.weekly_source_query_manager_row_owned_v1(
      (select recipient_route_id from public.weekly_manager_recipient_generations
       where id=v_manager_generation_after),
      v_incident_2,
      (select episode_number from public.weekly_discrepancy_incidents where id=v_incident_2)
    ),
    'accepted-batch ownership did not survive the generation rotation, or blocked a new episode');
  v_result:=public.weekly_source_manager_review_respond_atomic_v1(v_second_manager_response);
  perform pg_temp.assert_true((v_result->>'replay')::boolean,
    'exact manager replay did not survive a superseded generation and newer facts');
  -- 04A section 8: an already answered or later-episode membership is stale, and
  -- "the server writes nothing for the submitted subset".
  begin
    perform public.weekly_source_manager_review_respond_atomic_v1(
      pg_catalog.jsonb_set(
        v_second_manager_response,'{request_idempotency_key}',
        '"ee000000-0000-4000-8000-000000000003"'::jsonb
      )
    );
  exception when sqlstate '40001' then
    v_manager_old_link_rejected:=sqlerrm='WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_STALE';
  end;
  perform pg_temp.assert_true(v_manager_old_link_rejected,
    'a stale answer was accepted a second time through a still-live manager link');
  v_result:=public.weekly_source_query_sync_atomic_v1(v_v3_sync_request);
  perform pg_temp.assert_true((v_result->>'changed_comparisons')::integer=0
    and (v_result->>'unchanged_incidents')::integer=2,
    'unchanged repeat of the newest facts created another incident revision');
  perform pg_temp.assert_true(v_candidate_generation_count_after=(
    select pg_catalog.count(*) from public.weekly_candidate_outreach_generations
    where candidate_id='e3000000-0000-4000-8000-000000000001'
  ) and v_manager_generation_after=(
    select current_generation_id from public.weekly_manager_recipient_routes
    where current_generation_id is not null
  ),'unchanged re-import resent an outreach generation');
end;
$test$;

do $delivery$
declare
  v_generation_id uuid;
  v_intent_id uuid;
  v_input jsonb;
  v_stage jsonb;
  v_claim jsonb;
  v_register jsonb;
  v_start jsonb;
  v_replay jsonb;
  v_request jsonb;
  v_render_request jsonb;
  v_command_id uuid;
  v_attempt_id uuid;
  v_provider_key text;
  v_first_retry_key text;
  v_retry_intent uuid;
  v_retry_command uuid;
  v_target_one jsonb;
  v_target_two jsonb;
  v_target_two_id uuid;
  v_ambiguous_target_id uuid;
  v_invalid_target_id uuid;
  v_render_conflict boolean:=false;
begin
  insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
  values (
    'e3000000-0000-4000-8000-000000000003','CCR-90003','Taylor','Nurse',
    'Taylor Nurse','taylor@example.invalid'
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr
  ) values (
    'e4000000-0000-4000-8000-000000000003','e3000000-0000-4000-8000-000000000003',
    'e2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
    true,'HEALTHROSTER',true,true,true
  );
  v_request:=public.weekly_source_timesheet_submission_request_start_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','e1000000-0000-4000-8000-000000000001',
      'source_cycle_id','e6000000-0000-4000-8000-000000000001',
      'projection_publication_id','e8000000-0000-4000-8000-000000000003',
      'candidate_id','e3000000-0000-4000-8000-000000000003',
      'scopes',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'week_ending','2026-09-06','client_id','e2000000-0000-4000-8000-000000000001',
        'contract_id','e4000000-0000-4000-8000-000000000003',
        'expected_source_fingerprint',repeat('92',32)
      ))
    )
  );
  v_generation_id:=(v_request->>'candidate_generation_id')::uuid;
  v_intent_id:=(v_request->>'message_intent_id')::uuid;
  v_input:=public.weekly_source_message_render_input_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_intent_id,
    'projection_publication_id','e8000000-0000-4000-8000-000000000003'
  ));
  v_render_request:=pg_catalog.jsonb_build_object(
    'message_intent_id',v_intent_id,
    'projection_publication_id','e8000000-0000-4000-8000-000000000003',
    'membership_hash',v_input->>'membership_hash','policy_version','1.7.0',
    'renderer_version','1.3.0','plain_body','Please submit your Timesheet.'
  );
  v_stage:=public.weekly_source_message_render_stage_atomic_v1(v_render_request);
  v_command_id:=(v_stage->>'dispatch_command_id')::uuid;
  v_replay:=public.weekly_source_message_render_stage_atomic_v1(v_render_request);
  perform pg_temp.assert_true((v_replay->>'replay')::boolean
    and v_replay->>'dispatch_command_id'=v_command_id::text,
    'exact rendered-message replay did not return the original outbox command');
  begin
    perform public.weekly_source_message_render_stage_atomic_v1(
      pg_catalog.jsonb_set(
        v_render_request,'{plain_body}','"Please submit this Timesheet now."'::jsonb
      )
    );
  exception when sqlstate '23505' then
    v_render_conflict:=sqlerrm='WEEKLY_SOURCE_MESSAGE_RENDER_REPLAY_CONFLICT';
  end;
  perform pg_temp.assert_true(v_render_conflict,
    'conflicting rendered-message replay was silently accepted');

  -- Snapshot two devices under one immutable command.  Acceptance by the
  -- first device must not close or skip the second device.
  v_claim:=public.weekly_source_message_dispatch_claim_v1(pg_catalog.jsonb_build_object(
    'worker_id','multi-device-command','limit',1,'lease_seconds',60
  ));
  perform pg_temp.assert_true(v_claim->'commands'->0->>'dispatch_command_id'=v_command_id::text,
    'new dispatch command was not leased');
  v_register:=public.weekly_source_message_targets_register_atomic_v1(
    pg_catalog.jsonb_build_object(
      'dispatch_command_id',v_command_id,
      'lease_token',v_claim->'commands'->0->>'lease_token',
      'worker_id','multi-device-command',
      'control_plane_snapshot_id','f1000000-0000-4000-8000-000000000001',
      'suppression_reason',null,
      'targets',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'external_target_id','f2000000-0000-4000-8000-000000000001',
          'target_fingerprint',repeat('a1',32),'target_snapshot_hash',repeat('b1',32),
          'target_version',1,'provider','APNS',
          'safe_target_snapshot',pg_catalog.jsonb_build_object(
            'control_plane_snapshot_id','f1000000-0000-4000-8000-000000000001',
            'snapshot_device_id','f2000000-0000-4000-8000-000000000001',
            'provider','APNS','target_revision_hash',repeat('b1',32)
          )
        ),
        pg_catalog.jsonb_build_object(
          'external_target_id','f2000000-0000-4000-8000-000000000002',
          'target_fingerprint',repeat('a2',32),'target_snapshot_hash',repeat('b2',32),
          'target_version',1,'provider','FCM',
          'safe_target_snapshot',pg_catalog.jsonb_build_object(
            'control_plane_snapshot_id','f1000000-0000-4000-8000-000000000001',
            'snapshot_device_id','f2000000-0000-4000-8000-000000000002',
            'provider','FCM','target_revision_hash',repeat('b2',32)
          )
        )
      )
    )
  );
  perform pg_temp.assert_true((v_register->>'target_count')::integer=2,
    'multi-device snapshot did not create two independent targets');
  v_claim:=public.weekly_source_message_dispatch_target_claim_v1(pg_catalog.jsonb_build_object(
    'worker_id','multi-device-targets','limit',10,'lease_seconds',60
  ));
  select value into strict v_target_one from pg_catalog.jsonb_array_elements(v_claim->'targets')
  where value->>'external_target_id'='f2000000-0000-4000-8000-000000000001';
  select value into strict v_target_two from pg_catalog.jsonb_array_elements(v_claim->'targets')
  where value->>'external_target_id'='f2000000-0000-4000-8000-000000000002';
  v_target_two_id:=(v_target_two->>'dispatch_target_id')::uuid;
  v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(
    pg_catalog.jsonb_build_object(
      'dispatch_target_id',v_target_one->>'dispatch_target_id',
      'lease_token',v_target_one->>'lease_token','worker_id','multi-device-targets'
    )
  );
  v_attempt_id:=(v_start->>'provider_attempt_id')::uuid;
  v_provider_key:=v_start->>'provider_idempotency_key';
  v_replay:=public.weekly_source_message_dispatch_target_start_atomic_v1(
    pg_catalog.jsonb_build_object(
      'dispatch_target_id',v_target_one->>'dispatch_target_id',
      'lease_token',v_target_one->>'lease_token','worker_id','multi-device-targets'
    )
  );
  perform pg_temp.assert_true((v_replay->>'replay')::boolean
    and (v_replay->>'provider_attempt_id')::uuid=v_attempt_id
    and v_replay->>'provider_idempotency_key'=v_provider_key,
    'duplicate Queue delivery did not reuse the exact unfinished provider attempt');
  perform public.weekly_source_message_dispatch_target_result_atomic_v1(
    pg_catalog.jsonb_build_object(
      'provider_attempt_id',v_attempt_id,'outcome','ACCEPTED',
      'provider_message_id','verify-device-one',
      'bounded_provider_receipt',pg_catalog.jsonb_build_object(
        'provider_status',200,'provider_request_id','verify-device-one'
      ),'bounded_error','{}'::jsonb
    )
  );
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_message_dispatch_commands
    where id=v_command_id and target_count=2 and terminal_target_count=1
      and accepted_target_count=1 and target_set_state='PREPARED'
  ) and exists(
    select 1 from public.weekly_message_dispatch_targets
    where id=v_target_two_id and state='LEASED'
  ),'first accepted device closed or skipped the second device');
  v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(
    pg_catalog.jsonb_build_object(
      'dispatch_target_id',v_target_two->>'dispatch_target_id',
      'lease_token',v_target_two->>'lease_token','worker_id','multi-device-targets'
    )
  );
  v_first_retry_key:=v_start->>'provider_idempotency_key';
  perform public.weekly_source_message_dispatch_target_result_atomic_v1(
    pg_catalog.jsonb_build_object(
      'provider_attempt_id',v_start->>'provider_attempt_id','outcome','TRANSIENT_FAILURE',
      'provider_message_id',null,'bounded_provider_receipt','{}'::jsonb,
      'bounded_error',pg_catalog.jsonb_build_object('error_code','PROVIDER_TEMPORARY')
    )
  );
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_message_dispatch_targets
    where id=v_target_two_id and state='TRANSIENT_FAILURE'
      and next_attempt_at_utc>pg_catalog.transaction_timestamp()
  ),'transient device failure did not enter bounded backoff');
  update public.weekly_message_dispatch_targets
  set next_attempt_at_utc=pg_catalog.transaction_timestamp()-interval '1 second'
  where id=v_target_two_id;
  v_claim:=public.weekly_source_message_dispatch_target_claim_v1(pg_catalog.jsonb_build_object(
    'worker_id','multi-device-retry','limit',10,'lease_seconds',60
  ));
  select value into strict v_target_two from pg_catalog.jsonb_array_elements(v_claim->'targets')
  where value->>'dispatch_target_id'=v_target_two_id::text;
  v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(
    pg_catalog.jsonb_build_object(
      'dispatch_target_id',v_target_two->>'dispatch_target_id',
      'lease_token',v_target_two->>'lease_token','worker_id','multi-device-retry'
    )
  );
  perform pg_temp.assert_true(v_start->>'provider_idempotency_key'<>v_first_retry_key,
    'bounded retry reused a completed provider-attempt identity');
  v_attempt_id:=(v_start->>'provider_attempt_id')::uuid;
  perform public.weekly_source_message_dispatch_target_result_atomic_v1(
    pg_catalog.jsonb_build_object(
      'provider_attempt_id',v_attempt_id,'outcome','ACCEPTED',
      'provider_message_id','verify-device-two',
      'bounded_provider_receipt',pg_catalog.jsonb_build_object(
        'provider_status',200,'provider_request_id','verify-device-two'
      ),'bounded_error','{}'::jsonb
    )
  );
  v_replay:=public.weekly_source_message_dispatch_target_result_atomic_v1(
    pg_catalog.jsonb_build_object(
      'provider_attempt_id',v_attempt_id,'outcome','ACCEPTED',
      'provider_message_id','verify-device-two',
      'bounded_provider_receipt',pg_catalog.jsonb_build_object(
        'provider_status',200,'provider_request_id','verify-device-two'
      ),'bounded_error','{}'::jsonb
    )
  );
  perform pg_temp.assert_true((v_replay->>'replay')::boolean and exists(
    select 1 from public.weekly_message_dispatch_commands command
    join public.weekly_message_intents intent on intent.id=command.message_intent_id
    where command.id=v_command_id and command.state='ACCEPTED'
      and command.target_set_state='TERMINAL' and command.target_count=2
      and command.terminal_target_count=2 and command.accepted_target_count=2
      and intent.state='DISPATCHED'
  ),'all-device completion or duplicate result replay was not deterministic');

  -- An expired lease after SUBMISSION_STARTED is an ambiguous provider
  -- outcome.  It must never be guessed or submitted again.
  update public.weekly_candidate_outreach_generations
  set manual_reminder_available_at_utc=pg_catalog.transaction_timestamp()-interval '1 second'
  where id=v_generation_id;
  v_request:=public.weekly_source_candidate_reminder_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'candidate_generation_id',v_generation_id,
    'projection_publication_id','e8000000-0000-4000-8000-000000000003'
  ));
  v_retry_intent:=(v_request->>'message_intent_id')::uuid;
  v_input:=public.weekly_source_message_render_input_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_retry_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000003'
  ));
  v_stage:=public.weekly_source_message_render_stage_atomic_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_retry_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000003',
    'membership_hash',v_input->>'membership_hash','policy_version','1.7.0',
    'renderer_version','1.3.0','plain_body','Please submit your Timesheet.'
  ));
  v_retry_command:=(v_stage->>'dispatch_command_id')::uuid;
  v_claim:=public.weekly_source_message_dispatch_claim_v1(pg_catalog.jsonb_build_object(
    'worker_id','ambiguous-verifier','limit',1,'lease_seconds',60
  ));
  v_start:=pg_temp.start_one_delivery_target_v1(
    pg_catalog.jsonb_build_object(
      'dispatch_command_id',v_retry_command,
      'lease_token',v_claim->'commands'->0->>'lease_token','worker_id','ambiguous-verifier',
      'channel','PUSH','target_kind','CANDIDATE_DEVICE',
      'keyed_target_fingerprint',repeat('93',32)
    )
  );
  select id into strict v_ambiguous_target_id
  from public.weekly_message_dispatch_targets where dispatch_command_id=v_retry_command;
  update public.weekly_message_dispatch_targets
  set lease_expires_at_utc=pg_catalog.transaction_timestamp()-interval '1 second'
  where id=v_ambiguous_target_id;
  perform public.weekly_source_message_dispatch_target_claim_v1(
    pg_catalog.jsonb_build_object('worker_id','ambiguity-sweeper','limit',100,'lease_seconds',60)
  );
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_message_dispatch_targets
    where id=v_ambiguous_target_id and state='AMBIGUOUS'
  ) and exists(
    select 1 from public.weekly_message_delivery_failures
    where dispatch_target_id=v_ambiguous_target_id
      and failure_class='PROVIDER_AMBIGUOUS'
      and safe_failure_code='SUBMISSION_LEASE_EXPIRED'
  ),'expired provider submission was guessed or omitted from the failure ledger');

  -- A definite invalid token is terminal for only that exact snapshot target
  -- and records that target retirement still needs attention when the provider
  -- authority could not confirm it.
  update public.weekly_candidate_outreach_generations
  set manual_reminder_available_at_utc=pg_catalog.transaction_timestamp()-interval '1 second'
  where id=v_generation_id;
  v_request:=public.weekly_source_candidate_reminder_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'candidate_generation_id',v_generation_id,
    'projection_publication_id','e8000000-0000-4000-8000-000000000003'
  ));
  v_retry_intent:=(v_request->>'message_intent_id')::uuid;
  v_input:=public.weekly_source_message_render_input_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_retry_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000003'
  ));
  v_stage:=public.weekly_source_message_render_stage_atomic_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_retry_intent,
    'projection_publication_id','e8000000-0000-4000-8000-000000000003',
    'membership_hash',v_input->>'membership_hash','policy_version','1.7.0',
    'renderer_version','1.3.0','plain_body','Please submit your Timesheet.'
  ));
  v_retry_command:=(v_stage->>'dispatch_command_id')::uuid;
  v_claim:=public.weekly_source_message_dispatch_claim_v1(pg_catalog.jsonb_build_object(
    'worker_id','invalid-target-verifier','limit',1,'lease_seconds',60
  ));
  v_start:=pg_temp.start_one_delivery_target_v1(pg_catalog.jsonb_build_object(
    'dispatch_command_id',v_retry_command,
    'lease_token',v_claim->'commands'->0->>'lease_token',
    'worker_id','invalid-target-verifier','channel','PUSH',
    'target_kind','CANDIDATE_DEVICE','keyed_target_fingerprint',repeat('94',32)
  ));
  select id into strict v_invalid_target_id
  from public.weekly_message_dispatch_targets where dispatch_command_id=v_retry_command;
  perform public.weekly_source_message_dispatch_target_result_atomic_v1(
    pg_catalog.jsonb_build_object(
      'provider_attempt_id',v_start->>'provider_attempt_id',
      'outcome','DEFINITELY_REJECTED','provider_message_id',null,
      'bounded_provider_receipt',pg_catalog.jsonb_build_object('provider_status',410),
      'bounded_error',pg_catalog.jsonb_build_object(
        'error_code','INVALID_TARGET','provider_status',410
      )
    )
  );
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_message_dispatch_targets
    where id=v_invalid_target_id and state='DEFINITELY_REJECTED'
  ) and exists(
    select 1 from public.weekly_message_delivery_failures
    where dispatch_target_id=v_invalid_target_id and failure_class='INVALID_TARGET'
  ) and exists(
    select 1 from public.weekly_message_delivery_failures
    where dispatch_target_id=v_invalid_target_id and failure_class='TARGET_RETIREMENT_FAILED'
  ),'definite invalid target was not isolated and recorded');
end;
$delivery$;

do $candidate_choices$
declare
  v_sync jsonb;
  v_ask jsonb;
  v_page jsonb;
  v_response jsonb;
  v_result jsonb;
  v_incident_neither uuid;
  v_incident_candidate_wrong uuid;
begin
  insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
  values (
    'e3000000-0000-4000-8000-000000000004','CCR-90004','Morgan','Nurse',
    'Morgan Nurse','morgan@example.invalid'
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr
  ) values (
    'e4000000-0000-4000-8000-000000000004','e3000000-0000-4000-8000-000000000004',
    'e2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
    true,'HEALTHROSTER',true,true,true
  );
  insert into public.weekly_work_events(
    id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
    durable_identity_hash,first_source_group_id,source_format_profile_id
  ) values
    ('e9000000-0000-4000-8000-000000000004','e3000000-0000-4000-8000-000000000004',
     'e2000000-0000-4000-8000-000000000001','2026-09-04','PROFILE_EXTERNAL_KEY',
     'verify-shift-4',decode(repeat('24',32),'hex'),
     'e5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
    ('e9000000-0000-4000-8000-000000000005','e3000000-0000-4000-8000-000000000004',
     'e2000000-0000-4000-8000-000000000001','2026-09-05','PROFILE_EXTERNAL_KEY',
     'verify-shift-5',decode(repeat('25',32),'hex'),
     'e5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444');
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
    r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json
  ) values (
    'ea000000-0000-4000-8000-000000000004','VERIFY-TS-4','morgan-nurse',
    'north-test-trust','ward-c','nurse','2026-09-04 08:00:00+00',
    '2026-09-05 17:00:00+00',60,960,'2026-09-06','verify/nurse-4.png',repeat('d',64),
    'e4000000-0000-4000-8000-000000000004','WEEKLY','HOURS',
    '[{"date":"2026-09-04","start":"08:00","end":"17:00","break_minutes":30},{"date":"2026-09-05","start":"09:00","end":"17:00","break_minutes":30}]'
  );
  v_sync:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000003',
    'issues',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000002',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('32',32),
        'contract_id','e4000000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-02 20:00','candidate_end_at_local','2026-09-03 08:00',
        'candidate_break_minutes',60,'system_start_at_local','2026-09-02 20:00',
        'system_end_at_local','2026-09-03 07:00','system_break_minutes',60
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000003',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000002',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('62',32),
        'contract_id','e4000000-0000-4000-8000-000000000002',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-03 08:00','candidate_end_at_local','2026-09-03 17:00',
        'candidate_break_minutes',30,'system_start_at_local','2026-09-03 08:00',
        'system_end_at_local','2026-09-03 16:00','system_break_minutes',30
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000004',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000004',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('63',32),
        'contract_id','e4000000-0000-4000-8000-000000000004',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-04 08:00','candidate_end_at_local','2026-09-04 17:00',
        'candidate_break_minutes',30,'system_start_at_local','2026-09-04 08:00',
        'system_end_at_local','2026-09-04 16:00','system_break_minutes',30
      ),
      pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000005',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000004',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('64',32),
        'contract_id','e4000000-0000-4000-8000-000000000004',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-05 09:00','candidate_end_at_local','2026-09-05 17:00',
        'candidate_break_minutes',30,'system_start_at_local','2026-09-05 10:00',
        'system_end_at_local','2026-09-05 17:00','system_break_minutes',30
      )
    )
  ));
  perform pg_temp.assert_true((v_sync->>'new_incidents')::integer=2,
    'candidate-choice verification issues were not created');
  select id into strict v_incident_neither from public.weekly_discrepancy_incidents
  where work_event_id='e9000000-0000-4000-8000-000000000004' and state='OPEN';
  select id into strict v_incident_candidate_wrong from public.weekly_discrepancy_incidents
  where work_event_id='e9000000-0000-4000-8000-000000000005' and state='OPEN';
  v_ask:=public.weekly_source_query_ask_candidate_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000003',
    'candidate_id','e3000000-0000-4000-8000-000000000004',
    'client_id','e2000000-0000-4000-8000-000000000001',
    'incident_ids',pg_catalog.jsonb_build_array(v_incident_neither,v_incident_candidate_wrong)
  ));
  v_page:=public.weekly_source_candidate_query_get_v1(pg_catalog.jsonb_build_object(
    'candidate_id','e3000000-0000-4000-8000-000000000004',
    'candidate_generation_id',v_ask->>'candidate_generation_id',
    'projection_publication_id','e8000000-0000-4000-8000-000000000003'
  ));
  select pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'incident_id',item->>'incident_id',
      'expected_comparison_fingerprint',item->>'current_fact_version',
      'expected_timesheet_hash',item->>'expected_timesheet_hash',
      'choice',case when item->>'incident_id'=v_incident_neither::text
        then 'NEITHER_CORRECT' else 'CANDIDATE_WRONG' end,
      'corrected_start_at_local',case when item->>'incident_id'=v_incident_neither::text
        then '2026-09-04 09:00' else '2026-09-05 10:00' end,
      'corrected_end_at_local',case when item->>'incident_id'=v_incident_neither::text
        then '2026-09-04 18:00' else '2026-09-05 16:00' end,
      'corrected_break_minutes',30
    ) order by item->>'incident_id'
  ) into v_response
  from pg_catalog.jsonb_array_elements(v_page->'items') item;
  v_result:=public.weekly_source_candidate_response_submit_atomic_v1(
    pg_catalog.jsonb_build_object(
      'candidate_id','e3000000-0000-4000-8000-000000000004',
      'candidate_generation_id',v_ask->>'candidate_generation_id',
      'projection_publication_id','e8000000-0000-4000-8000-000000000003',
      'request_idempotency_key','eb000000-0000-4000-8000-000000000004',
      'responses',v_response
    )
  );
  perform pg_temp.assert_true((v_result->>'answered_count')::integer=2
    and (v_result->>'resolved_count')::integer=1,
    'candidate response choices did not produce their separate outcomes');
  perform pg_temp.assert_true(exists(
    select 1 from public.office_action_notifications
    where issue_id=v_incident_neither and event_kind='WEEKLY_CANDIDATE_SOURCE_DISPUTED'
  ) and not exists(
    select 1 from public.office_action_notifications
    where issue_id=v_incident_candidate_wrong and event_kind='WEEKLY_CANDIDATE_SOURCE_DISPUTED'
  ),'both-hours-wrong Office alert was not limited to the required choice');
  perform pg_temp.assert_true(exists(
    select 1 from public.weekly_discrepancy_incidents
    where id=v_incident_neither and state='OPEN' and manager_action_state='DUE'
  ) and exists(
    select 1 from public.weekly_discrepancy_incidents
    where id=v_incident_candidate_wrong and state='RESOLVED'
      and resolution_kind='CANDIDATE_CORRECTED'
  ),'candidate choice state transitions are incorrect');
end;
$candidate_choices$;

do $policy_boundaries$
declare
  v_candidate_disabled boolean:=false;
  v_manager_disabled boolean:=false;
  v_timesheet_authority boolean:=false;
  v_incident_2 uuid;
  v_incident_3 uuid;
  v_result jsonb;
begin
  select id into strict v_incident_2 from public.weekly_discrepancy_incidents
  where work_event_id='e9000000-0000-4000-8000-000000000002' and state='OPEN';
  select id into strict v_incident_3 from public.weekly_discrepancy_incidents
  where work_event_id='e9000000-0000-4000-8000-000000000003' and state='OPEN';

  update public.weekly_source_client_policies
  set candidate_queries_enabled=false,manager_queries_enabled=true
  where source_group_id='e5000000-0000-4000-8000-000000000001'
    and client_id='e2000000-0000-4000-8000-000000000001';
  begin
    perform public.weekly_source_query_ask_candidate_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','e1000000-0000-4000-8000-000000000001',
      'source_cycle_id','e6000000-0000-4000-8000-000000000001',
      'projection_publication_id','e8000000-0000-4000-8000-000000000003',
      'candidate_id','e3000000-0000-4000-8000-000000000001',
      'client_id','e2000000-0000-4000-8000-000000000001',
      'incident_ids',pg_catalog.jsonb_build_array(v_incident_2)
    ));
  exception when sqlstate '55000' then
    v_candidate_disabled:=sqlerrm='WEEKLY_SOURCE_CANDIDATE_QUERIES_DISABLED';
  end;
  perform pg_temp.assert_true(v_candidate_disabled,
    'candidate contact toggle did not disable only the candidate route');

  update public.weekly_source_client_policies
  set candidate_queries_enabled=true,manager_queries_enabled=false
  where source_group_id='e5000000-0000-4000-8000-000000000001'
    and client_id='e2000000-0000-4000-8000-000000000001';
  begin
    perform public.weekly_source_query_send_manager_now_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','e1000000-0000-4000-8000-000000000001',
      'source_cycle_id','e6000000-0000-4000-8000-000000000001',
      'projection_publication_id','e8000000-0000-4000-8000-000000000003',
      'incident_ids',pg_catalog.jsonb_build_array(v_incident_3)
    ));
  exception when sqlstate '55000' then
    v_manager_disabled:=sqlerrm='WEEKLY_SOURCE_MANAGER_QUERIES_DISABLED';
  end;
  perform pg_temp.assert_true(v_manager_disabled,
    'manager contact toggle did not disable only the manager route');

  -- The secure source-query page is unavailable to the manager-signed
  -- Timesheet-authority route, which retains its existing email journey.
  update public.weekly_source_client_policies
  set candidate_queries_enabled=true,manager_queries_enabled=true,
      authority_mode='TIMESHEET_AUTHORITY',document_mode='INVOICE_EVIDENCE_REQUIRED',
      self_bill_enabled=false
  where source_group_id='e5000000-0000-4000-8000-000000000001'
    and client_id='e2000000-0000-4000-8000-000000000001';
  begin
    perform public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','e1000000-0000-4000-8000-000000000001',
      'source_cycle_id','e6000000-0000-4000-8000-000000000001',
      'projection_publication_id','e8000000-0000-4000-8000-000000000003',
      'issues',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'work_event_id','e9000000-0000-4000-8000-000000000002',
        'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
        'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('32',32),
        'contract_id','e4000000-0000-4000-8000-000000000001',
        'issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
        'candidate_start_at_local','2026-09-02 20:00','candidate_end_at_local','2026-09-03 08:00',
        'candidate_break_minutes',60,'system_start_at_local','2026-09-02 20:00',
        'system_end_at_local','2026-09-03 07:00','system_break_minutes',60
      ))
    ));
  exception when sqlstate '55000' then
    v_timesheet_authority:=sqlerrm='WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE';
  end;
  perform pg_temp.assert_true(v_timesheet_authority,
    'Timesheet-authority route entered the secure source-query lifecycle');
  update public.weekly_source_client_policies
  set authority_mode='SOURCE_AUTHORITY',document_mode='CHECK_ONLY',self_bill_enabled=true,
      candidate_queries_enabled=true,manager_queries_enabled=true
  where source_group_id='e5000000-0000-4000-8000-000000000001'
    and client_id='e2000000-0000-4000-8000-000000000001';
end;
$policy_boundaries$;

-- An unresolved shift is a durable incident, not a one-week record. A later
-- current publication for the same source group may append facts to that
-- incident while its first-cycle audit origin remains unchanged.
do $cross_cycle_carry$
declare
  v_incident_id uuid;
  v_issue jsonb;
  v_changed_issue jsonb;
  v_result jsonb;
  v_original_comparison_id uuid;
  v_generation_before uuid;
  v_generation_after uuid;
  v_generation_count integer;
  v_render_input jsonb;
  v_route_preparation jsonb;
  v_stage jsonb;
  v_claim jsonb;
  v_claimed jsonb;
  v_start jsonb;
  v_older_publication_rejected boolean:=false;
begin
  select incident.id,incident.current_comparison_revision_id
  into strict v_incident_id,v_original_comparison_id
  from public.weekly_discrepancy_incidents incident
  where incident.work_event_id='e9000000-0000-4000-8000-000000000002'
    and incident.state='OPEN';

  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (
    'e6000000-0000-4000-8000-000000000002','e5000000-0000-4000-8000-000000000001',
    '2026-09-13','2026-09-16 15:00:00+00','OPEN',1,'NONE'
  );
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
    coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id
  ) values (
    'e7000000-0000-4000-8000-000000000004','e6000000-0000-4000-8000-000000000002',
    'query-next-cycle.xlsx',decode(repeat('91',32),'hex'),100,
    '34444444-4444-4444-8444-444444444444','verify','verify',decode(repeat('92',32),'hex'),
    decode(repeat('93',32),'hex'),'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,
    decode(repeat('94',32),'hex'),'CURRENT','e1000000-0000-4000-8000-000000000001'
  );
  update public.weekly_source_cycles
  set current_complete_upload_id='e7000000-0000-4000-8000-000000000004'
  where id='e6000000-0000-4000-8000-000000000002';
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state,published_at_utc
  ) values (
    'e8000000-0000-4000-8000-000000000004','e6000000-0000-4000-8000-000000000002',
    'CYCLE','e7000000-0000-4000-8000-000000000004',1,decode(repeat('95',32),'hex'),
    decode(repeat('96',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp()
  );
  update public.weekly_source_cycles
  set projection_state='CURRENT',
      current_projection_publication_id='e8000000-0000-4000-8000-000000000004'
  where id='e6000000-0000-4000-8000-000000000002';

  begin
    perform public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','e1000000-0000-4000-8000-000000000001',
      'source_cycle_id','e6000000-0000-4000-8000-000000000002',
      'projection_publication_id','e8000000-0000-4000-8000-000000000003',
      'issues','[]'::jsonb
    ));
  exception when sqlstate '55000' then
    v_older_publication_rejected:=sqlerrm='WEEKLY_SOURCE_QUERY_PUBLICATION_SCOPE_INVALID';
  end;
  perform pg_temp.assert_true(v_older_publication_rejected,
    'a later-cycle operation accepted an older publication');

  select pg_catalog.jsonb_build_object(
    'work_event_id',incident.work_event_id,
    'candidate_timesheet_id',comparison.candidate_timesheet_id,
    'candidate_timesheet_revision',comparison.candidate_timesheet_revision,
    'candidate_shift_fingerprint',pg_catalog.encode(comparison.candidate_shift_fingerprint,'hex'),
    'contract_id',comparison.contract_id,'issue_family',comparison.issue_family,
    'source_presence',comparison.source_presence,
    'candidate_start_at_local',comparison.candidate_start_at_local,
    'candidate_end_at_local',comparison.candidate_end_at_local,
    'candidate_break_minutes',comparison.candidate_break_minutes,
    'system_start_at_local',comparison.system_start_at_local,
    'system_end_at_local',comparison.system_end_at_local,
    'system_break_minutes',comparison.system_break_minutes
  ) into strict v_issue
  from public.weekly_discrepancy_incidents incident
  join public.weekly_issue_comparison_revisions comparison
    on comparison.id=incident.current_comparison_revision_id
  where incident.id=v_incident_id;

  select current_generation_id into strict v_generation_before
  from public.weekly_manager_recipient_routes
  where source_cycle_id='e6000000-0000-4000-8000-000000000001'
    and current_generation_id is not null;
  select pg_catalog.count(*) into v_generation_count
  from public.weekly_manager_recipient_generations
  where recipient_route_id=(
    select id from public.weekly_manager_recipient_routes
    where source_cycle_id='e6000000-0000-4000-8000-000000000001'
      and current_generation_id is not null
  );

  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000002',
    'projection_publication_id','e8000000-0000-4000-8000-000000000004',
    'issues',pg_catalog.jsonb_build_array(v_issue)
  ));
  perform pg_temp.assert_true(
    (v_result->>'unchanged_incidents')::integer=1
    and (v_result->>'changed_comparisons')::integer=0
    and (select source_cycle_id from public.weekly_discrepancy_incidents
         where id=v_incident_id)='e6000000-0000-4000-8000-000000000001'
    and (select current_comparison_revision_id from public.weekly_discrepancy_incidents
         where id=v_incident_id)=v_original_comparison_id,
    'unchanged later-cycle facts replaced the incident origin or comparison'
  );
  perform pg_temp.assert_true(v_generation_count=(
    select pg_catalog.count(*)
    from public.weekly_manager_recipient_generations
    where recipient_route_id=(
      select id from public.weekly_manager_recipient_routes
      where source_cycle_id='e6000000-0000-4000-8000-000000000001'
        and current_generation_id is not null
    )
  ),'unchanged later-cycle facts resent manager outreach');

  v_changed_issue:=v_issue||pg_catalog.jsonb_build_object(
    'system_end_at_local','2026-09-03 06:30'
  );
  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000002',
    'projection_publication_id','e8000000-0000-4000-8000-000000000004',
    'issues',pg_catalog.jsonb_build_array(v_changed_issue)
  ));
  select current_generation_id into strict v_generation_after
  from public.weekly_manager_recipient_routes
  where source_cycle_id='e6000000-0000-4000-8000-000000000001'
    and current_generation_id is not null;
  perform pg_temp.assert_true(
    (v_result->>'changed_comparisons')::integer=1
    and (select source_cycle_id from public.weekly_discrepancy_incidents
         where id=v_incident_id)='e6000000-0000-4000-8000-000000000001'
    and (select comparison.projection_publication_id
         from public.weekly_discrepancy_incidents incident
         join public.weekly_issue_comparison_revisions comparison
           on comparison.id=incident.current_comparison_revision_id
         where incident.id=v_incident_id)='e8000000-0000-4000-8000-000000000004'
    and v_generation_after<>v_generation_before,
    'later-cycle changed facts did not append to and restart the durable incident'
  );
  perform public.weekly_source_query_scheduler_tick_v1(pg_catalog.jsonb_build_object(
    'now_utc',pg_catalog.transaction_timestamp()+interval '1 minute','limit',100
  ));
  select public.weekly_source_message_render_input_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',intent.id,
    'projection_publication_id','e8000000-0000-4000-8000-000000000004'
  )) into strict v_render_input
  from public.weekly_message_intents intent
  where intent.recipient_generation_id=v_generation_after and intent.state='DUE'
  order by intent.created_at_utc desc,intent.id desc limit 1;
  perform pg_temp.assert_true((v_render_input->>'shift_count')::integer>=1,
    'later current publication could not render the restarted original-cycle route');
  v_route_preparation:=public.weekly_source_manager_route_prepare_atomic_v1(
    pg_catalog.jsonb_build_object(
      'message_intent_id',(
        select intent.id from public.weekly_message_intents intent
        where intent.recipient_generation_id=v_generation_after and intent.state='DUE'
        order by intent.created_at_utc desc,intent.id desc limit 1
      ),
      'projection_publication_id','e8000000-0000-4000-8000-000000000004'
    )
  );
  v_stage:=public.weekly_source_message_render_stage_atomic_v1(
    pg_catalog.jsonb_build_object(
      'message_intent_id',(
        select intent.id from public.weekly_message_intents intent
        where intent.recipient_generation_id=v_generation_after and intent.state='DUE'
        order by intent.created_at_utc desc,intent.id desc limit 1
      ),
      'projection_publication_id','e8000000-0000-4000-8000-000000000004',
      'membership_hash',v_render_input->>'membership_hash',
      'policy_version','1.8.0','renderer_version','1.4.0','structure_version','1.0.0',
      'subject_text',pg_catalog.format(
        'Timesheet queries requiring your review - %s %s',
        v_render_input->>'shift_count',
        case when (v_render_input->>'shift_count')::integer=1 then 'shift' else 'shifts' end
      ),
      'html_body','<p>Please review the listed hours.</p>',
      'plain_body','Please review the listed hours.',
      'credential_hash',repeat('97',32),
      'control_plane_ticket_id','ec100000-0000-4000-8000-000000000001',
      'agency_receipt_id','ed100000-0000-4000-8000-000000000001',
      'data_plane_identity','query-cross-cycle','route_version','1','credential_version','1'
      ,'manager_route_preparation_id',v_route_preparation->>'manager_route_preparation_id'
    )
  );
  v_claim:=public.weekly_source_message_dispatch_claim_v1(pg_catalog.jsonb_build_object(
    'worker_id','cross-cycle-verifier','limit',100,'lease_seconds',30
  ));
  select value into strict v_claimed
  from pg_catalog.jsonb_array_elements(v_claim->'commands')
  where value->>'dispatch_command_id'=v_stage->>'dispatch_command_id';

  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000002',
    'projection_publication_id','e8000000-0000-4000-8000-000000000004',
    'issues',pg_catalog.jsonb_build_array(v_changed_issue)
  ));
  perform pg_temp.assert_true(
    (v_result->>'unchanged_incidents')::integer=1
    and v_generation_after=(
      select current_generation_id from public.weekly_manager_recipient_routes
      where source_cycle_id='e6000000-0000-4000-8000-000000000001'
        and current_generation_id is not null
    ),'repeated later-cycle facts resent the restarted route');

  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000002',
    'projection_publication_id','e8000000-0000-4000-8000-000000000004',
    'issues','[]'::jsonb
  ));
  perform pg_temp.assert_true(
    (v_result->>'resolved_incidents')::integer>=1
    and exists(
      select 1 from public.weekly_discrepancy_incidents
      where id=v_incident_id and state='RESOLVED' and resolution_kind='SOURCE_MATCHED'
        and source_cycle_id='e6000000-0000-4000-8000-000000000001'
    ),'later complete HealthRoster coverage did not resolve the durable incident');
  v_start:=pg_temp.start_one_delivery_target_v1(
    pg_catalog.jsonb_build_object(
      'dispatch_command_id',v_stage->>'dispatch_command_id',
      'lease_token',v_claimed->>'lease_token','worker_id','cross-cycle-verifier',
      'channel','EMAIL','target_kind','MANAGER_ADDRESS',
      'keyed_target_fingerprint',repeat('98',32)
    )
  );
  perform pg_temp.assert_true(
    not (v_start->>'ok')::boolean and v_start->>'reason'='REVIEW_CHANGED'
    and exists(
      select 1 from public.weekly_message_dispatch_commands
      where id=(v_stage->>'dispatch_command_id')::uuid and state='RETIRED'
    ) and exists(
      select 1 from public.weekly_manager_review_batches
      where id=(v_stage->>'review_batch_id')::uuid and state='REVOKED'
    ),'a resolved shift remained dispatchable in a stale manager email');
end;
$cross_cycle_carry$;

-- A final NHSP backing report is an exact billing statement, but omission is
-- not proof that an open worker claim was corrected. Keep the incident open
-- on omission and resolve it only when a later positive row actually matches.
do $nhsp_no_inference$
declare
  v_incident_id uuid;
  v_result jsonb;
begin
  insert into public.clients(id,name,ts_queries_email)
  values ('f2000000-0000-4000-8000-000000000001','NHSP Query Trust','nhsp-manager@example.invalid');
  insert into public.client_settings(
    id,client_id,effective_from,hr_validation_required,autoprocess_hr,
    self_bill_no_invoices_sent,no_timesheet_required,requires_hr
  ) values (
    'f2100000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
    '2026-01-01',true,true,true,true,true
  );
  insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
  values (
    'f3000000-0000-4000-8000-000000000001','CCR-99001','Taylor','Nurse',
    'Taylor Nurse','taylor@example.invalid'
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp
  ) values (
    'f4000000-0000-4000-8000-000000000001','f3000000-0000-4000-8000-000000000001',
    'f2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
    true,null,false,false,false,true
  );
  insert into public.weekly_source_groups(
    id,environment,agency_id,code,display_name,source_family,cutoff_weekday,
    cutoff_local_time,nhsp_report_heading_name
  ) values (
    'f5000000-0000-4000-8000-000000000001','TEST','f0000000-0000-4000-8000-000000000001',
    'QUERY_VERIFY_NHSP','NHSP query verification','NHSP',3,'15:00','NHSP Query Trust'
  );
  insert into public.weekly_source_group_clients(
    source_group_id,client_id,valid_from,created_by_user_id
  ) values (
    'f5000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
    '2026-01-01','e1000000-0000-4000-8000-000000000001'
  );
  insert into public.weekly_source_client_policies(
    source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,
    candidate_queries_enabled,manager_queries_enabled,manager_query_recipient,created_by_user_id
  ) values (
    'f5000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
    '2026-01-01','SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,
    'nhsp-manager@example.invalid','e1000000-0000-4000-8000-000000000001'
  );
  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (
    'f6000000-0000-4000-8000-000000000001','f5000000-0000-4000-8000-000000000001',
    '2026-09-06','2026-09-09 15:00:00+00','OPEN',1,'NONE'
  );
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
    coverage_proof_kind,physical_row_count,row_manifest_hash,state,uploaded_by_user_id
  ) values (
    'f7000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000001',
    'nhsp-prefinal.xlsx',decode(repeat('a1',32),'hex'),100,
    '31111111-1111-4111-8111-111111111111','verify','verify',decode(repeat('a2',32),'hex'),
    decode(repeat('a3',32),'hex'),'OFFICE_COMPLETE_EXPORT_ATTESTATION',0,
    decode(repeat('a4',32),'hex'),'CURRENT','e1000000-0000-4000-8000-000000000001'
  );
  update public.weekly_source_cycles
  set current_complete_upload_id='f7000000-0000-4000-8000-000000000001'
  where id='f6000000-0000-4000-8000-000000000001';
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state,published_at_utc
  ) values (
    'f8000000-0000-4000-8000-000000000001','f6000000-0000-4000-8000-000000000001',
    'CYCLE','f7000000-0000-4000-8000-000000000001',1,decode(repeat('a5',32),'hex'),
    decode(repeat('a6',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp()
  );
  update public.weekly_source_cycles
  set projection_state='CURRENT',
      current_projection_publication_id='f8000000-0000-4000-8000-000000000001'
  where id='f6000000-0000-4000-8000-000000000001';
  insert into public.weekly_work_events(
    id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
    durable_identity_hash,first_source_group_id,source_format_profile_id
  ) values (
    'f9000000-0000-4000-8000-000000000001','f3000000-0000-4000-8000-000000000001',
    'f2000000-0000-4000-8000-000000000001','2026-09-02','PROFILE_EXTERNAL_KEY',
    'nhsp-query-shift-1',decode(repeat('a7',32),'hex'),
    'f5000000-0000-4000-8000-000000000001','31111111-1111-4111-8111-111111111111'
  );
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
    r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,actual_schedule_json
  ) values (
    'fa000000-0000-4000-8000-000000000001','VERIFY-NHSP-TS-1','taylor-nurse',
    'nhsp-query-trust','ward-n','nurse','2026-09-02 09:00:00+00','2026-09-02 17:00:00+00',
    30,450,'2026-09-06','verify/nhsp-nurse.png',repeat('b1',32),
    'f4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
    '[{"date":"2026-09-02","start":"09:00","end":"17:00","break_minutes":30}]'
  );
  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','f6000000-0000-4000-8000-000000000001',
    'projection_publication_id','f8000000-0000-4000-8000-000000000001',
    'issues',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'work_event_id','f9000000-0000-4000-8000-000000000001',
      'candidate_timesheet_id','fa000000-0000-4000-8000-000000000001',
      'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('b2',32),
      'contract_id','f4000000-0000-4000-8000-000000000001',
      'issue_family','SOURCE_MISSING_OR_NOT_AUTHORISED','source_presence','ABSENT',
      'candidate_start_at_local','2026-09-02 09:00','candidate_end_at_local','2026-09-02 17:00',
      'candidate_break_minutes',30
    ))
  ));
  select id into strict v_incident_id
  from public.weekly_discrepancy_incidents
  where work_event_id='f9000000-0000-4000-8000-000000000001' and state='OPEN';

  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (
    'f6000000-0000-4000-8000-000000000002','f5000000-0000-4000-8000-000000000001',
    '2026-09-13','2026-09-16 15:00:00+00','OPEN',0,'NONE'
  );
  insert into public.weekly_source_report_scopes(
    id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc,
    version,state,projection_state
  ) values (
    'fb000000-0000-4000-8000-000000000002','f6000000-0000-4000-8000-000000000002',
    'TEST','f0000000-0000-4000-8000-000000000001','f5000000-0000-4000-8000-000000000001',
    'f2000000-0000-4000-8000-000000000001','2026-09-16 15:00:00+00',1,'OPEN','NONE'
  );
  insert into public.weekly_source_uploads(
    id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,header_coordinate_map_hash,
    declared_scope_fingerprint,coverage_proof_kind,physical_row_count,row_manifest_hash,
    state,uploaded_by_user_id
  ) values (
    'f7000000-0000-4000-8000-000000000002','f6000000-0000-4000-8000-000000000002',
    'fb000000-0000-4000-8000-000000000002','nhsp-final-empty.xlsx',decode(repeat('b3',32),'hex'),
    100,'32222222-2222-4222-8222-222222222222','verify','verify',decode(repeat('b4',32),'hex'),
    decode(repeat('b5',32),'hex'),'NHSP_TRUST_REPORT_SCOPE',0,decode(repeat('b6',32),'hex'),
    'CURRENT','e1000000-0000-4000-8000-000000000001'
  );
  update public.weekly_source_report_scopes
  set current_complete_upload_id='f7000000-0000-4000-8000-000000000002'
  where id='fb000000-0000-4000-8000-000000000002';
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state,published_at_utc
  ) values (
    'f8000000-0000-4000-8000-000000000002','f6000000-0000-4000-8000-000000000002',
    'NHSP_REPORT_SCOPE','fb000000-0000-4000-8000-000000000002',
    'f7000000-0000-4000-8000-000000000002',1,decode(repeat('b7',32),'hex'),
    decode(repeat('b8',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp()
  );
  update public.weekly_source_report_scopes
  set projection_state='CURRENT',
      current_projection_publication_id='f8000000-0000-4000-8000-000000000002'
  where id='fb000000-0000-4000-8000-000000000002';
  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','f6000000-0000-4000-8000-000000000002',
    'projection_publication_id','f8000000-0000-4000-8000-000000000002',
    'issues','[]'::jsonb
  ));
  perform pg_temp.assert_true(
    (v_result->>'resolved_incidents')::integer=0
    and exists(
      select 1 from public.weekly_discrepancy_incidents
      where id=v_incident_id and state='OPEN'
        and source_cycle_id='f6000000-0000-4000-8000-000000000001'
    ),'NHSP final-report omission incorrectly resolved the worker claim');

  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (
    'f6000000-0000-4000-8000-000000000003','f5000000-0000-4000-8000-000000000001',
    '2026-09-20','2026-09-23 15:00:00+00','OPEN',0,'NONE'
  );
  insert into public.weekly_source_report_scopes(
    id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc,
    version,state,projection_state
  ) values (
    'fb000000-0000-4000-8000-000000000003','f6000000-0000-4000-8000-000000000003',
    'TEST','f0000000-0000-4000-8000-000000000001','f5000000-0000-4000-8000-000000000001',
    'f2000000-0000-4000-8000-000000000001','2026-09-23 15:00:00+00',1,'OPEN','NONE'
  );
  insert into public.weekly_source_uploads(
    id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,header_coordinate_map_hash,
    declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
    row_manifest_hash,state,uploaded_by_user_id
  ) values (
    'f7000000-0000-4000-8000-000000000003','f6000000-0000-4000-8000-000000000003',
    'fb000000-0000-4000-8000-000000000003','nhsp-final-matched.xlsx',decode(repeat('b9',32),'hex'),
    100,'32222222-2222-4222-8222-222222222222','verify','verify',decode(repeat('ba',32),'hex'),
    decode(repeat('bb',32),'hex'),'NHSP_TRUST_REPORT_SCOPE',1,1,decode(repeat('bc',32),'hex'),
    'CURRENT','e1000000-0000-4000-8000-000000000001'
  );
  update public.weekly_source_report_scopes
  set current_complete_upload_id='f7000000-0000-4000-8000-000000000003'
  where id='fb000000-0000-4000-8000-000000000003';
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
    source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
    actual_net_minutes,row_finalisation_state,source_commission_pence,source_total_cost_pence,
    source_shift_charge_pence,source_money_parse_state,normalised_row_hash,bounded_raw_columns_json
  ) values (
    'fc000000-0000-4000-8000-000000000001','f7000000-0000-4000-8000-000000000003',1,
    'nhsp-query-shift-1','Taylor Nurse','NHSP Query Trust','2026-09-02',
    '2026-09-02 09:00','2026-09-02 17:00',30,450,'SOURCE_WORKED',1000,9000,10000,
    'VALID',decode(repeat('bd',32),'hex'),'{}'
  );
  insert into public.weekly_source_row_resolutions(
    id,upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,
    paid_minutes,rate_classifications_json,mapping_state,contract_selection_method,
    work_event_match_kind,work_event_match_fingerprint,qualification_profile_fingerprint,
    qualifying_contract_count,qualifying_contract_set_hash,source_row_fingerprint,
    contract_and_rate_fingerprint,effective_policy_fingerprint
  ) values (
    'fd000000-0000-4000-8000-000000000001','fc000000-0000-4000-8000-000000000001',1,
    'f3000000-0000-4000-8000-000000000001','f2000000-0000-4000-8000-000000000001',
    'f4000000-0000-4000-8000-000000000001','f9000000-0000-4000-8000-000000000001',
    450,'{}','RESOLVED','AUTO_UNIQUE','REUSED_PROFILE_KEY',decode(repeat('be',32),'hex'),
    decode(repeat('bf',32),'hex'),1,decode(repeat('c1',32),'hex'),decode(repeat('c2',32),'hex'),
    decode(repeat('c3',32),'hex'),decode(repeat('c4',32),'hex')
  );
  insert into public.weekly_work_event_source_links(
    id,work_event_id,upload_row_id,row_resolution_id,link_kind,link_hash
  ) values (
    'fe000000-0000-4000-8000-000000000001','f9000000-0000-4000-8000-000000000001',
    'fc000000-0000-4000-8000-000000000001','fd000000-0000-4000-8000-000000000001',
    'POSITIVE_SOURCE',decode(repeat('c5',32),'hex')
  );
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state,published_at_utc
  ) values (
    'f8000000-0000-4000-8000-000000000003','f6000000-0000-4000-8000-000000000003',
    'NHSP_REPORT_SCOPE','fb000000-0000-4000-8000-000000000003',
    'f7000000-0000-4000-8000-000000000003',1,decode(repeat('c6',32),'hex'),
    decode(repeat('c7',32),'hex'),'CURRENT',pg_catalog.transaction_timestamp()
  );
  update public.weekly_source_report_scopes
  set projection_state='CURRENT',
      current_projection_publication_id='f8000000-0000-4000-8000-000000000003'
  where id='fb000000-0000-4000-8000-000000000003';
  v_result:=public.weekly_source_query_sync_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','f6000000-0000-4000-8000-000000000003',
    'projection_publication_id','f8000000-0000-4000-8000-000000000003',
    'issues','[]'::jsonb
  ));
  perform pg_temp.assert_true(
    (v_result->>'resolved_incidents')::integer=1
    and exists(
      select 1 from public.weekly_discrepancy_incidents
      where id=v_incident_id and state='RESOLVED' and resolution_kind='SOURCE_MATCHED'
        and source_cycle_id='f6000000-0000-4000-8000-000000000001'
    ),'later matching NHSP positive row did not resolve the durable worker claim');
end;
$nhsp_no_inference$;

do $acl_and_write_boundary$
declare
  v_public_names constant text[]:=array[
    'weekly_source_candidate_query_get_v1',
    'weekly_source_message_render_input_v1',
    'weekly_source_timesheet_submission_complete_atomic_v1',
    'weekly_source_query_accept_system_hours_atomic_v1',
    'weekly_source_office_notifications_list_v1',
    'weekly_source_office_notification_ack_atomic_v1',
    'weekly_source_manager_review_get_v1',
    'weekly_source_manager_review_respond_atomic_v1',
    'weekly_source_message_dispatch_claim_v1',
    'weekly_source_message_dispatch_submission_start_atomic_v1',
    'weekly_source_message_dispatch_result_atomic_v1',
    'weekly_source_manager_route_prepare_atomic_v1',
    'weekly_source_message_render_stage_atomic_v1',
    'weekly_source_query_scheduler_tick_v1',
    'weekly_source_candidate_response_submit_atomic_v1',
    'weekly_source_query_send_manager_now_atomic_v1',
    'weekly_source_candidate_reminder_atomic_v1',
    'weekly_source_query_ask_candidate_atomic_v1',
    'weekly_source_timesheet_submission_request_start_atomic_v1',
    'weekly_source_query_sync_atomic_v1'
  ];
  v_private_names constant text[]:=array[
    'weekly_source_query_require_service_v1',
    'weekly_source_query_manager_generation_v1',
    'weekly_source_query_manager_intent_v1',
    'weekly_source_query_restart_activated_cohort_v1',
    'weekly_source_query_cohort_ensure_v1',
    'weekly_source_query_candidate_intent_v1',
    'weekly_source_query_candidate_generation_v1',
    'weekly_source_query_hex32_v1',
    'weekly_source_query_ascii_fold_v1',
    'weekly_source_query_normalise_recipient_v1',
    'weekly_source_query_current_publication_v1',
    'weekly_source_query_response_fingerprint_v1',
    'weekly_source_query_candidate_timesheet_hash_v1',
    'weekly_source_query_incident_policy_v1',
    'weekly_source_query_notice_fanout_v1',
    'weekly_source_query_comparison_fingerprint_v1'
  ];
  v_tables constant text[]:=array[
    'weekly_discrepancy_incidents','weekly_issue_comparison_revisions',
    'weekly_route_activations','weekly_candidate_cohorts',
    'weekly_candidate_outreach_generations','weekly_candidate_outreach_memberships',
    'weekly_timesheet_submission_requests','weekly_timesheet_submission_request_memberships',
    'weekly_candidate_response_drafts','weekly_candidate_response_draft_items',
    'weekly_discrepancy_events','weekly_manager_recipient_routes',
    'weekly_manager_recipient_generations','weekly_manager_recipient_memberships',
    'weekly_manager_cohort_due_events','weekly_message_intents','weekly_message_renders',
    'weekly_message_dispatch_commands','weekly_message_provider_attempts',
    'weekly_manager_review_batches','weekly_manager_review_items',
    'weekly_manager_route_receipts','weekly_manager_route_preparations',
    'office_action_notifications'
  ];
  v_name text;
  v_signature text;
  v_oid oid;
begin
  foreach v_name in array v_public_names loop
    v_signature:=pg_catalog.format('public.%I(jsonb)',v_name);
    perform pg_temp.assert_true(pg_catalog.to_regprocedure(v_signature) is not null,
      'approved service RPC missing: '||v_signature);
    perform pg_temp.assert_true(
      pg_catalog.has_function_privilege('service_role',v_signature,'EXECUTE'),
      'service_role cannot execute approved RPC: '||v_signature
    );
    perform pg_temp.assert_true(
      not pg_catalog.has_function_privilege('anon',v_signature,'EXECUTE')
      and not pg_catalog.has_function_privilege('authenticated',v_signature,'EXECUTE'),
      'browser role can execute broker-only RPC: '||v_signature
    );
  end loop;
  foreach v_name in array v_private_names loop
    select procedure.oid,
           pg_catalog.format('%I.%I(%s)',namespace.nspname,procedure.proname,
             pg_catalog.pg_get_function_identity_arguments(procedure.oid))
    into strict v_oid,v_signature
    from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace on namespace.oid=procedure.pronamespace
    where namespace.nspname='private' and procedure.proname=v_name;
    perform pg_temp.assert_true(
      not pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE')
      and not pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
      and not pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE'),
      'private query helper is directly API-callable: '||v_signature
    );
  end loop;
  foreach v_name in array v_tables loop
    perform pg_temp.assert_true(
      not pg_catalog.has_table_privilege('anon',pg_catalog.format('public.%I',v_name),'SELECT')
      and not pg_catalog.has_table_privilege('anon',pg_catalog.format('public.%I',v_name),'INSERT')
      and not pg_catalog.has_table_privilege('anon',pg_catalog.format('public.%I',v_name),'UPDATE')
      and not pg_catalog.has_table_privilege('anon',pg_catalog.format('public.%I',v_name),'DELETE')
      and not pg_catalog.has_table_privilege('authenticated',pg_catalog.format('public.%I',v_name),'SELECT')
      and not pg_catalog.has_table_privilege('authenticated',pg_catalog.format('public.%I',v_name),'INSERT')
      and not pg_catalog.has_table_privilege('authenticated',pg_catalog.format('public.%I',v_name),'UPDATE')
      and not pg_catalog.has_table_privilege('authenticated',pg_catalog.format('public.%I',v_name),'DELETE'),
      'browser role has direct query-lifecycle table access: public.'||v_name
    );
  end loop;
  perform pg_temp.assert_true(not exists(
    select 1
    from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace on namespace.oid=procedure.pronamespace
    cross join lateral pg_catalog.regexp_matches(
      pg_catalog.pg_get_functiondef(procedure.oid),
      '(insert[[:space:]]+into|update|delete[[:space:]]+from)[[:space:]]+public[.]([a-zA-Z0-9_]+)',
      'gi'
    ) write_target
    where (
      (namespace.nspname='public' and procedure.proname=any(v_public_names))
      or (namespace.nspname='private' and procedure.proname=any(v_private_names))
    ) and write_target[2] not like 'weekly_%'
      and write_target[2]<>'office_action_notifications'
  ),'query lifecycle writes outside its own tables');
  perform pg_temp.assert_true(not exists(
    select 1
    from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace on namespace.oid=procedure.pronamespace
    where (
      (namespace.nspname='public' and procedure.proname=any(v_public_names))
      or (namespace.nspname='private' and procedure.proname=any(v_private_names))
    )
      and pg_catalog.pg_get_functiondef(procedure.oid)
        ~* '(http_post|http_get|net\\.|dblink|lo_export|copy[[:space:]])'
  ),'query/delivery SQL contains an external provider call');
end;
$acl_and_write_boundary$;

select pg_temp.assert_true(
  pg_catalog.has_function_privilege('service_role','public.weekly_source_query_sync_atomic_v1(jsonb)','EXECUTE'),
  'service_role lacks approved query RPC'
);
select pg_temp.assert_true(
  not pg_catalog.has_function_privilege('anon','public.weekly_source_query_sync_atomic_v1(jsonb)','EXECUTE')
  and not pg_catalog.has_function_privilege('authenticated','public.weekly_source_manager_review_get_v1(jsonb)','EXECUTE'),
  'browser roles can execute broker-only RPCs'
);
select pg_temp.assert_true(
  not pg_catalog.has_function_privilege('service_role','private.weekly_source_query_notice_fanout_v1(uuid,uuid,text,jsonb)','EXECUTE'),
  'private helper is directly API-callable'
);
select pg_temp.assert_true(not exists(
  select 1 from public.weekly_message_renders
  where (coalesce(subject_text,'')||' '||coalesce(html_body,'')||' '||plain_body)
    ~* '(^|[^[:alnum:]_])(pay|charge|rate|vat|invoice|banking|import|source|fingerprint|generation|incident|projection)([^[:alnum:]_]|$)'
),'candidate or manager content contains financial or technical terminology');
select pg_temp.assert_true(
  pg_catalog.pg_get_functiondef(
    'public.weekly_source_candidate_query_get_v1(jsonb)'::pg_catalog.regprocedure
  ) like '%Missing or not yet authorised%'
  and pg_catalog.pg_get_functiondef(
    'public.weekly_source_manager_review_get_v1(jsonb)'::pg_catalog.regprocedure
  ) like '%I have added and/or authorised the shift%'
  and pg_catalog.pg_get_functiondef(
    'public.weekly_source_message_render_input_v1(jsonb)'::pg_catalog.regprocedure
  ) like '%NHSP_ABSENT%',
  'NHSP missing-or-not-yet-authorised wording policy is absent'
);

-- The earlier query-delivery repeatable is also replayed during upgrades.
-- It must not replace the later target-delivery claim contract with its
-- pre-target version or manager email dispatch will remain leased forever.
select pg_temp.assert_true(
  pg_catalog.pg_get_functiondef(
    'public.weekly_source_message_dispatch_claim_v1(jsonb)'::pg_catalog.regprocedure
  ) like '%manager_recipient_route_id%'
  and pg_catalog.pg_get_functiondef(
    'public.weekly_source_message_dispatch_claim_v1(jsonb)'::pg_catalog.regprocedure
  ) like '%manager_recipient_fingerprint%'
  and pg_catalog.pg_get_functiondef(
    'public.weekly_source_message_dispatch_claim_v1(jsonb)'::pg_catalog.regprocedure
  ) like '%command.target_set_state%'
  and pg_catalog.pg_get_functiondef(
    'public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'::pg_catalog.regprocedure
  ) like '%WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED%'
  and pg_catalog.pg_get_functiondef(
    'public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'::pg_catalog.regprocedure
  ) like '%WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED%'
  ,'manager target-delivery claim contract was overwritten'
);

select 'weekly_source_query_delivery_v1: rollback verification passed' as result;

rollback;

do $fresh$
begin
  if exists(select 1 from public.weekly_source_groups
            where id='e5000000-0000-4000-8000-000000000001') then
    raise exception 'VERIFY_FAILED: rollback did not remove verification fixtures';
  end if;
end;
$fresh$;
select 'weekly_source_query_delivery_v1: fresh rollback absence passed' as result;
