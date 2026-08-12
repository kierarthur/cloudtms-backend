-- Candidate App disposable-local semantic verification.
-- Prerequisite: install the compile fixture, all three migrations and all repeatables
-- into an empty local PostgreSQL database. Every fixture write is rolled back.

begin;

-- This file runs only in a disposable database.  Production defaults remain
-- false; semantic tests enable each independently gated Candidate App path.
update public.settings_defaults
set candidate_app_feature_flags_json=jsonb_build_object(
  'candidate_account_registration',true,
  'candidate_app_reads',true,
  'candidate_app_writes',true,
  'candidate_record_role_capabilities',true,
  'candidate_expense_atomic_placement',true,
  'candidate_expense_invoice_routing_v1',true,
  'candidate_manager_approval',true,
  'candidate_paper_qr',true,
  'candidate_notifications',true,
  'candidate_daily_finalisation',true,
  'candidate_settings',true
)
where id=1;

do $policy_and_evidence$
declare
  v_client uuid:='11111111-1111-1111-1111-111111111111';
  v_candidate_one uuid:='22222222-2222-2222-2222-222222222221';
  v_candidate_two uuid:='22222222-2222-2222-2222-222222222222';
  v_contract uuid:='33333333-3333-3333-3333-333333333333';
  v_user uuid:='44444444-4444-4444-4444-444444444444';
  v_timesheet uuid:='55555555-5555-5555-5555-555555555555';
  v_contract_week uuid:='55555555-5555-5555-5555-555555555556';
  v_account uuid:='66666666-6666-6666-6666-666666666666';
  v_workflow uuid:='77777777-7777-7777-7777-777777777777';
  v_component uuid:='88888888-8888-8888-8888-888888888888';
  v_policy jsonb;
  v_eligibility jsonb;
  v_schedule jsonb;
  v_mode public.submission_mode_enum;
  v_week_end date;
begin
  insert into public.tms_users(id) values(v_user);
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate_one,'candidate@example.test',true,null);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,std_schedule_json,default_submission_mode
  ) values(
    v_contract,v_candidate_one,v_client,current_date-7,current_date+7,0,
    '{"mon":{"start":"09:00","end":"17:00","break_minutes":30}}'::jsonb,'ELECTRONIC'
  );

  v_policy:=private._candidate_policy_resolve_v1(v_client,v_contract,current_date);
  if (v_policy->>'candidate_electronic_auto_authorise')::boolean<>false
     or v_policy->>'candidate_electronic_auto_authorise_source'<>'GLOBAL' then
    raise exception 'global policy failed: %',v_policy;
  end if;

  insert into public.client_settings(
    id,client_id,effective_from,updated_at,default_submission_mode,
    candidate_electronic_auto_authorise
  ) values(gen_random_uuid(),v_client,current_date-1,now(),'ELECTRONIC',true);

  update public.client_settings set default_submission_mode='MANUAL' where client_id=v_client;
  v_mode:=private._candidate_submission_mode_v1(v_client,v_contract,current_date);
  if v_mode<>'MANUAL' then
    raise exception 'client submission mode precedence failed: %',v_mode;
  end if;
  update public.contracts set overrideclientsettings=true where id=v_contract;
  v_mode:=private._candidate_submission_mode_v1(v_client,v_contract,current_date);
  if v_mode<>'ELECTRONIC' then
    raise exception 'contract submission mode override failed: %',v_mode;
  end if;
  update public.contracts set overrideclientsettings=false where id=v_contract;

  v_week_end:=current_date+mod(0-extract(dow from current_date)::integer+7,7);
  v_schedule:=private._candidate_week_schedule_from_template_v1(
    (select std_schedule_json from public.contracts where id=v_contract),
    v_week_end,current_date-7,current_date+7
  );
  if jsonb_array_length(v_schedule)<>1
     or v_schedule->0->>'date' is null
     or (v_schedule->0->>'expected_minutes')::integer<>450 then
    raise exception 'dated week schedule generation failed: %',v_schedule;
  end if;

  v_policy:=private._candidate_policy_resolve_v1(v_client,v_contract,current_date);
  if (v_policy->>'candidate_electronic_auto_authorise')::boolean<>true
     or v_policy->>'candidate_electronic_auto_authorise_source'<>'CLIENT' then
    raise exception 'client policy failed: %',v_policy;
  end if;

  update public.contracts
  set candidate_electronic_auto_authorise_override=false
  where id=v_contract;
  v_policy:=private._candidate_policy_resolve_v1(v_client,v_contract,current_date);
  if (v_policy->>'candidate_electronic_auto_authorise')::boolean<>false
     or v_policy->>'candidate_electronic_auto_authorise_source'<>'CONTRACT' then
    raise exception 'contract policy failed: %',v_policy;
  end if;

  v_eligibility:=private._candidate_email_eligibility_v1('TEST',' Candidate@Example.Test ');
  if (v_eligibility->>'eligible')::boolean<>true
     or (v_eligibility->>'match_count')::integer<>1
     or (v_eligibility->>'selection_required')::boolean<>false then
    raise exception 'single TEST eligibility failed: %',v_eligibility;
  end if;

  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate_two,'CANDIDATE@example.test',true,null);
  v_eligibility:=private._candidate_email_eligibility_v1('TEST','candidate@example.test');
  if (v_eligibility->>'eligible')::boolean<>true
     or (v_eligibility->>'match_count')::integer<>2
     or (v_eligibility->>'selection_required')::boolean<>true then
    raise exception 'duplicate TEST eligibility failed: %',v_eligibility;
  end if;

  update public.settings_defaults set candidate_app_environment='LIVE' where id=1;
  v_eligibility:=private._candidate_email_eligibility_v1('LIVE','candidate@example.test');
  if (v_eligibility->>'eligible')::boolean<>false
     or v_eligibility->>'reason_code'<>'LIVE_DUPLICATE_ACTIVE_EMAIL' then
    raise exception 'duplicate LIVE eligibility failed: %',v_eligibility;
  end if;
  update public.settings_defaults set candidate_app_environment='TEST' where id=1;

  insert into public.timesheets(timesheet_id,submission_mode,contract_id,week_ending_date)
  values(v_timesheet,'MANUAL',v_contract,v_week_end);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(v_contract_week,v_contract,v_week_end,0,'OPEN','MANUAL',v_timesheet);
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,password_scheme,password_scheme_version,
    password_salt,password_digest,password_changed_at_utc
  ) values(
    v_account,'TEST','candidate@example.test','ACTIVE','PBKDF2-HMAC-SHA256',1,
    decode(repeat('01',16),'hex'),decode(repeat('02',32),'hex'),now()
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate_one,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'CREATED',1,v_contract,v_contract_week,v_timesheet,v_timesheet,v_week_end,
    '{}','{}','semantic-workflow'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    expense_category,document_role,state,storage_key,media_type,byte_size,
    source_content_sha256,immutable_at_utc
  ) values(
    v_component,v_workflow,1,1,v_timesheet,'EXPENSE_EVIDENCE','OTHER','SOURCE_EVIDENCE',
    'IMMUTABLE','semantic/one','image/jpeg',10,decode(repeat('aa',32),'hex'),now()
  );

  begin
    update public.candidate_submission_components
    set storage_key='semantic/changed'
    where id=v_component;
    raise exception 'immutable component update unexpectedly succeeded';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_COMPONENT_IMMUTABLE' then raise; end if;
  end;

  begin
    insert into public.candidate_submission_components(
      workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
      expense_category,document_role,state,storage_key,media_type,byte_size,
      source_content_sha256,immutable_at_utc
    ) values(
      v_workflow,1,2,v_timesheet,'EXPENSE_EVIDENCE','OTHER','SOURCE_EVIDENCE','IMMUTABLE',
      'semantic/two','image/jpeg',10,decode(repeat('aa',32),'hex'),now()
    );
    raise exception 'duplicate digest unexpectedly succeeded';
  exception when unique_violation then null;
  end;

  insert into public.timesheet_evidence(
    timesheet_id,kind,display_name,storage_key,processing_state
  ) values(v_timesheet,'TIMESHEET','one','semantic/timesheet-one','READY');
  begin
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,processing_state
    ) values(v_timesheet,'TIMESHEET','two','semantic/timesheet-two','READY');
    raise exception 'second active TIMESHEET unexpectedly succeeded';
  exception when unique_violation then null;
  end;
end;
$policy_and_evidence$;

do $authentication$
declare
  v_candidate uuid:='aaaaaaaa-0000-0000-0000-000000000001';
  v_session uuid:='aaaaaaaa-0000-0000-0000-000000000002';
  v_next_session uuid:='aaaaaaaa-0000-0000-0000-000000000003';
  v_token bytea:=decode(repeat('11',32),'hex');
  v_wrong bytea:=decode(repeat('12',32),'hex');
  v_response jsonb;
  v_challenge uuid;
  v_account uuid;
  v_attempts integer;
  v_state text;
begin
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'auth@example.test',true,null);

  v_response:=public.candidate_auth_challenge_transition_v1(
    'START','TEST','auth@example.test','ACTIVATE',null,v_token,'auth-start-v1',now(),1
  );
  if coalesce((v_response->>'ok')::boolean,false)=false
     or coalesce((v_response->>'deliver_email')::boolean,false)=false then
    raise exception 'challenge START failed: %',v_response;
  end if;
  v_challenge:=(v_response->>'challenge_id')::uuid;

  v_response:=public.candidate_auth_challenge_transition_v1(
    'START','TEST','auth@example.test','ACTIVATE',null,v_token,'auth-start-v1',now(),1
  );
  if coalesce((v_response->>'idempotent_replay')::boolean,false)=false
     or coalesce((v_response->>'deliver_email')::boolean,false)=false then
    raise exception 'challenge replay failed: %',v_response;
  end if;

  v_response:=public.candidate_auth_challenge_transition_v1(
    'VERIFY','TEST','auth@example.test','ACTIVATE',v_challenge,v_wrong,'auth-verify-wrong-v1',now()
  );
  if coalesce((v_response->>'ok')::boolean,true)=true then
    raise exception 'wrong challenge accepted: %',v_response;
  end if;
  select attempt_count,state into v_attempts,v_state
  from public.candidate_auth_challenges where id=v_challenge;
  if v_attempts<>1 or v_state<>'PENDING' then
    raise exception 'failed challenge attempt was not durable: %, %',v_attempts,v_state;
  end if;

  v_response:=public.candidate_auth_challenge_transition_v1(
    'VERIFY','TEST','auth@example.test','ACTIVATE',v_challenge,v_token,'auth-verify-good-v1',now()
  );
  if v_response->>'state'<>'VERIFIED' then
    raise exception 'challenge VERIFY failed: %',v_response;
  end if;

  v_response:=public.candidate_auth_account_transition_v1(
    'ACTIVATE_PASSWORD','TEST',null,null,v_session,v_candidate,
    jsonb_build_object(
      'challenge_id',v_challenge,'password_scheme','PBKDF2-HMAC-SHA256',
      'password_scheme_version',1,'password_salt_hex',repeat('21',16),
      'password_digest_hex',repeat('22',32),
      'password_params',jsonb_build_object('iterations',600000),
      'refresh_token_hash_hex',repeat('31',32),
      'expires_at_utc',now()+interval '30 days',
      'absolute_expires_at_utc',now()+interval '90 days','platform','TEST',
      'idempotency_request_sha256',repeat('41',32),
      'idempotency_key_version',1
    ),
    'auth-activate-v1',now()
  );
  v_account:=(v_response->>'account_id')::uuid;
  if v_account is null or v_response->>'status'<>'ACTIVE'
     or (v_response->>'session_id')::uuid<>v_session then
    raise exception 'account activation failed: %',v_response;
  end if;
  select state into v_state from public.candidate_auth_challenges where id=v_challenge;
  if v_state<>'CONSUMED' then raise exception 'challenge consumption failed: %',v_state; end if;

  v_response:=public.candidate_auth_account_transition_v1(
    'REFRESH_SESSION','TEST',v_account,null,v_session,null,
    jsonb_build_object(
      'presented_refresh_token_hash_hex',repeat('31',32),
      'new_refresh_token_hash_hex',repeat('32',32),
      'new_session_id',v_next_session,
      'idempotency_request_sha256',repeat('42',32),
      'idempotency_key_version',1
    ),
    'auth-refresh-v1',now()
  );
  if (v_response->>'session_id')::uuid<>v_next_session
     or (v_response->>'rotation')::integer<>1 then
    raise exception 'refresh rotation failed: %',v_response;
  end if;

  -- Lost-response replay must return the same successor before the rotated-token
  -- theft path is evaluated, even if generated session/hash inputs differ.
  v_response:=public.candidate_auth_account_transition_v1(
    'REFRESH_SESSION','TEST',v_account,null,v_session,null,
    jsonb_build_object(
      'presented_refresh_token_hash_hex',repeat('31',32),
      'new_refresh_token_hash_hex',repeat('39',32),
      'new_session_id','aaaaaaaa-0000-4000-8000-000000000009',
      'idempotency_request_sha256',repeat('42',32),
      'idempotency_key_version',1
    ),
    'auth-refresh-v1',now()
  );
  if (v_response->>'session_id')::uuid<>v_next_session
     or coalesce((v_response->>'idempotent_replay')::boolean,false)=false then
    raise exception 'refresh lost-response replay failed: %',v_response;
  end if;

  v_response:=public.candidate_auth_account_transition_v1(
    'REFRESH_SESSION','TEST',v_account,null,v_session,null,
    jsonb_build_object(
      'presented_refresh_token_hash_hex',repeat('31',32),
      'new_refresh_token_hash_hex',repeat('33',32),
      'new_session_id','aaaaaaaa-0000-0000-0000-000000000004',
      'idempotency_request_sha256',repeat('43',32),
      'idempotency_key_version',1
    ),
    'auth-refresh-reuse-v1',now()
  );
  if coalesce((v_response->>'ok')::boolean,true)=true
     or v_response->>'error_code'<>'CANDIDATE_REFRESH_TOKEN_REUSE' then
    raise exception 'refresh reuse handling failed: %',v_response;
  end if;
  select status into v_state from public.candidate_app_sessions where id=v_next_session;
  if v_state<>'REVOKED' then
    raise exception 'refresh family revocation was not durable: %',v_state;
  end if;
end;
$authentication$;

do $workflow_finalisation$
declare
  v_client uuid:='bbbbbbbb-0000-0000-0000-000000000001';
  v_candidate uuid:='bbbbbbbb-0000-0000-0000-000000000002';
  v_contract uuid:='bbbbbbbb-0000-0000-0000-000000000003';
  v_week uuid:='bbbbbbbb-0000-0000-0000-000000000004';
  v_timesheet uuid:='bbbbbbbb-0000-0000-0000-000000000005';
  v_user uuid:='bbbbbbbb-0000-0000-0000-000000000006';
  v_account uuid:='bbbbbbbb-0000-0000-0000-000000000007';
  v_session uuid:='bbbbbbbb-0000-0000-0000-000000000008';
  v_workflow uuid:='bbbbbbbb-0000-0000-0000-000000000009';
  v_candidate_signature uuid;
  v_review_component uuid;
  v_manager_signature uuid;
  v_component uuid;
  v_response jsonb;
  v_state text;
  v_row_signature text;
  v_render_input_hash text;
  v_manifest_hash text;
  v_approval_request uuid;
  v_approved_at timestamptz;
begin
  insert into public.tms_users(id) values(v_user);
  update public.settings_defaults
  set candidate_app_system_actor_user_id=v_user
  where id=1;
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'workflow@example.test',true,null);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(timesheet_id,submission_mode,r2_nurse_key,r2_auth_key)
  values(v_timesheet,'ELECTRONIC','existing/candidate-signature','existing/manager-signature');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'OPEN','ELECTRONIC',v_timesheet);
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,password_scheme,password_scheme_version,
    password_salt,password_digest,password_changed_at_utc
  ) values(
    v_account,'TEST','workflow@example.test','ACTIVE','PBKDF2-HMAC-SHA256',1,
    decode(repeat('41',16),'hex'),decode(repeat('42',32),'hex'),now()
  );
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('43',32),'hex'),
    now()+interval '30 days',now()+interval '90 days'
  );

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_week,
      'target_timesheet_id',v_timesheet,'week_ending_date',current_date,
      'input_snapshot','{}'::jsonb
    ),
    'workflow-create-v1',now()
  );
  if v_response->>'state'<>'WORKER_DRAFT' or (v_response->>'generation')::integer<>1 then
    raise exception 'workflow create failed: %',v_response;
  end if;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,
    jsonb_build_object(
      'component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE',
      'storage_key','workflow/candidate-signature.png','media_type','image/png','byte_size',128
    ),
    'workflow-candidate-signature-v1',now()
  );
  v_candidate_signature:=(v_response->>'component_id')::uuid;
  if v_response->>'storage_key'<>'workflow/candidate-signature.png'
     or v_response->>'media_type'<>'image/png'
     or (v_response->>'byte_size')::bigint<>128
     or v_response->>'component_kind'<>'CANDIDATE_SIGNATURE'
     or v_response->>'document_role'<>'CANDIDATE_SIGNATURE' then
    raise exception 'component prepare did not return its authoritative upload contract: %',v_response;
  end if;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,
    jsonb_build_object(
      'component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE',
      'storage_key','workflow/ignored-retry-key.png','media_type','image/png','byte_size',128
    ),
    'workflow-candidate-signature-v1',now()
  );
  if not coalesce((v_response->>'idempotent_replay')::boolean,false)
     or (v_response->>'component_id')::uuid<>v_candidate_signature
     or v_response->>'storage_key'<>'workflow/candidate-signature.png' then
    raise exception 'component prepare replay did not preserve the original object identity: %',v_response;
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,
      jsonb_build_object(
        'component_id',v_candidate_signature,
        'source_content_sha256_hex',repeat('44',32),
        'verified_byte_size',128,'verified_media_type','image/png'
      ),'workflow-candidate-signature-v1',now()
    );
    raise exception 'different action reused a durable mutation key';
  exception when others then
    if sqlerrm<>'CANDIDATE_IDEMPOTENCY_CONFLICT' then raise; end if;
  end;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,
      jsonb_build_object(
        'component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE',
        'storage_key','workflow/ignored-conflict-key.jpg','media_type','image/jpeg','byte_size',128
      ),
      'workflow-candidate-signature-v1',now()
    );
    raise exception 'component prepare replay unexpectedly accepted conflicting media';
  exception when others then
    if sqlerrm<>'CANDIDATE_IDEMPOTENCY_CONFLICT' then raise; end if;
  end;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,
    jsonb_build_object(
      'component_id',v_candidate_signature,'source_content_sha256_hex',repeat('44',32),
      'verified_byte_size',128,'verified_media_type','image/png'
    ),
    'workflow-candidate-signature-complete-v1',now()
  );

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'WORKER_SUBMIT',1,
    jsonb_build_object(
      'candidate_signature_component_id',v_candidate_signature,
      'candidate_signed_at_utc',now(),
      'renderer_contract_version','TIMESHEET_OFFICIAL_PDF_V1',
      'immutable_submission',jsonb_build_object(
        'timesheet_patch_json','{}'::jsonb,
        'contract_week_patch_json','{}'::jsonb,
        'canonical_tsfin_snapshot','{}'::jsonb
      )
    ),
    'workflow-submit-v1',now()
  );
  if v_response->>'state'<>'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'
     or (v_response->>'generation')::integer<>2 then
    raise exception 'worker submit failed: %',v_response;
  end if;
  v_review_component:=(v_response->>'review_document_component_id')::uuid;
  v_render_input_hash:=private._candidate_component_render_contract_v1(
    v_workflow,2,v_review_component,'REVIEW'
  )->>'render_input_sha256';

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'SELECT_PHONE_APPROVAL',2,'{}'::jsonb,
      'workflow-phone-before-review-v1',now()
    );
    raise exception 'phone review unexpectedly began before official document registration';
  exception when sqlstate '55000' then
    if sqlerrm<>'MANAGER_REVIEW_DOCUMENT_NOT_READY' then raise; end if;
  end;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'REGISTER_MANAGER_REVIEW_DOCUMENT',2,
    jsonb_build_object(
      'component_id',v_review_component,
      'storage_key','workflow/review/timesheet.pdf',
      'content_sha256_hex',repeat('45',32),
      'render_input_sha256_hex',v_render_input_hash,
      'media_type','application/pdf','byte_size',2048,'page_count',1,
      'renderer_contract_version','TIMESHEET_OFFICIAL_PDF_V1',
      'renderer_receipt',jsonb_build_object(
        'form_variant','ELECTRONIC_MANAGER_REVIEW','workflow_id',v_workflow,
        'workflow_generation',2,'component_id',v_review_component,
        'component_kind','HOURS_TIMESHEET','document_role','ELECTRONIC_TIMESHEET_MANAGER_REVIEW',
        'review_ordinal',1,'scope','WEEKLY','page_count',1,
        'render_input_sha256',v_render_input_hash,
        'candidate_signature_embedded',true,'manager_signature_embedded',false,
        'manager_approval_date_embedded',false
      )
    ),
    'workflow-register-review-v1',now()
  );
  if v_response->>'state'<>'READY_FOR_MANAGER_APPROVAL'
     or coalesce((v_response->>'review_document_ready')::boolean,false)=false then
    raise exception 'manager review document registration failed: %',v_response;
  end if;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'SELECT_PHONE_APPROVAL',2,
    jsonb_build_object(
      'approval_token_hash_hex',encode(extensions.digest(v_workflow::text||':phone','sha256'),'hex'),
      'expires_at_utc',now()+interval '30 minutes','handoff_token_key_version',1,
      'public_broker_binding',jsonb_build_object(
        'contract_version','CANDIDATE_PUBLIC_PHONE_BINDING_V1',
        'public_session_binding_sha256',repeat('ab',32),
        'device_binding_sha256',repeat('cd',32)
      ),'broker_handoff_key_version',1
    ),
    'workflow-select-phone-v1',now()
  );
  v_manifest_hash:=v_response->>'review_manifest_sha256';
  v_approval_request:=(v_response->>'approval_request_id')::uuid;
  if v_response->>'state'<>'AWAITING_MANAGER_APPROVAL'
     or nullif(v_response->>'issued_at_utc','') is null
     or (v_response->>'issued_at_utc')::timestamptz>now() then
    raise exception 'phone approval selection failed: %',v_response;
  end if;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'BEGIN_MANAGER_REVIEW',2,'{}'::jsonb,
    'workflow-begin-review-v1',now()
  );
  if v_response->>'manifest_sha256'<>v_manifest_hash
     or (v_response->>'page_count')::integer<>1 then
    raise exception 'begin manager review failed: %',v_response;
  end if;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'RECORD_REVIEW_PROGRESS',2,
    jsonb_build_object(
      'manifest_sha256_hex',v_manifest_hash,'component_id',v_review_component,
      'component_sha256_hex',repeat('45',32),
      'viewed_receipt',jsonb_build_object('viewed',true)
    ),
    'workflow-review-progress-v1',now()
  );
  if (v_response->>'reviewed_count')::integer<>1 then
    raise exception 'review progress failed: %',v_response;
  end if;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',2,
    jsonb_build_object(
      'component_kind','MANAGER_SIGNATURE','document_role','MANAGER_SIGNATURE',
      'approval_request_id',v_approval_request,
      'storage_key','workflow/signature','media_type','image/png','byte_size',128
    ),
    'workflow-signature-v1',now()
  );
  v_manager_signature:=(v_response->>'component_id')::uuid;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',2,
    jsonb_build_object(
      'component_id',v_manager_signature,'source_content_sha256_hex',repeat('46',32),
      'verified_byte_size',128,'verified_media_type','image/png'
    ),
    'workflow-signature-complete-v1',now()
  );
  if v_response->>'state'<>'IMMUTABLE' then
    raise exception 'component complete failed: %',v_response;
  end if;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'PHONE_APPROVE',2,
    jsonb_build_object(
      'manifest_sha256_hex',v_manifest_hash,'signature_component_id',v_manager_signature,
      'manager_name','Test Manager','manager_position','Manager'
    ),
    'workflow-phone-approve-v1',now()
  );
  if v_response->>'state'<>'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT'
     or (v_response->>'generation')::integer<>2 then
    raise exception 'phone approval failed: %',v_response;
  end if;
  v_approved_at:=(v_response->>'approved_at_utc')::timestamptz;

  begin
    perform public.candidate_submission_finalize_atomic_v1(
      v_session,'TEST',v_workflow,2,null,'workflow-finalise-too-early-v1',now()
    );
    raise exception 'finalise unexpectedly succeeded before final signed document';
  exception when sqlstate '55000' then
    if sqlerrm<>'FINAL_SIGNED_DOCUMENT_NOT_READY' then raise; end if;
  end;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'REGISTER_FINAL_SIGNED_DOCUMENT',2,
    jsonb_build_object(
      'component_id',v_review_component,
      'storage_key','workflow/final/timesheet.pdf',
      'content_sha256_hex',repeat('47',32),
      'render_input_sha256_hex',v_render_input_hash,
      'media_type','application/pdf','byte_size',2304,'page_count',1,
      'renderer_contract_version','TIMESHEET_OFFICIAL_PDF_V1',
      'renderer_receipt',jsonb_build_object(
        'form_variant','ELECTRONIC_SIGNED','workflow_id',v_workflow,
        'workflow_generation',2,'component_id',v_review_component,
        'component_kind','HOURS_TIMESHEET','document_role','ELECTRONIC_TIMESHEET_MANAGER_REVIEW',
        'review_ordinal',1,'scope','WEEKLY','page_count',1,
        'render_input_sha256',v_render_input_hash,
        'candidate_signature_embedded',true,'manager_signature_embedded',true,
        'manager_approval_date_embedded',true,
        'candidate_signature_sha256',repeat('44',32),
        'manager_signature_sha256',repeat('46',32),
        'manager_name','Test Manager','manager_position','Manager',
        'manager_approved_at_utc',v_approved_at
      )
    ),
    'workflow-register-final-v1',now()
  );
  if v_response->>'state'<>'READY_TO_FINALISE'
     or coalesce((v_response->>'all_final_signed_documents_ready')::boolean,false)=false then
    raise exception 'final signed document registration failed: %',v_response;
  end if;

  v_row_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_timesheet,v_week,false
  )->>'row_signature';
  update public.candidate_app_sessions
  set revoked_at_utc=now(), updated_at_utc=now()
  where id=v_session;
  v_response:=public.candidate_submission_finalize_atomic_v1(
    null,'TEST',v_workflow,2,v_row_signature,'workflow-finalise-v1',now(),
    jsonb_build_object('service_finalisation',jsonb_build_object(
      'contract_version','CANDIDATE_MANAGER_FINALISATION_V1',
      'workflow_generation',2,
      'approval_method','PHONE',
      'approval_request_id',v_approval_request,
      'review_manifest_sha256_hex',v_manifest_hash
    ))
  );
  if v_response->>'state'<>'FINALISED'
     or coalesce((v_response->>'auto_authorised')::boolean,true)=true then
    raise exception 'finalise failed: %',v_response;
  end if;
  select state into v_state
  from public.candidate_submission_workflows
  where id=v_workflow;
  if v_state<>'FINALISED' then
    raise exception 'workflow state not finalised: %',v_state;
  end if;
  if not exists(
    select 1 from public.timesheets t
    where t.timesheet_id=v_timesheet and t.submission_mode='ELECTRONIC'
      and t.r2_nurse_key='workflow/candidate-signature.png'
      and t.r2_auth_key='workflow/signature'
      and t.auth_name='Test Manager' and t.auth_job_title='Manager'
      and t.candidate_manager_approved_at_utc is not null
      and t.authorised_at_server is null
  ) then
    raise exception 'canonical electronic signature pair/manager approval was not materialised';
  end if;
  if not exists(
    select 1 from public.timesheet_evidence e
    where e.timesheet_id=v_timesheet and e.kind='TIMESHEET'
      and e.document_role='SIGNED_TIMESHEET'
      and e.storage_key='workflow/final/timesheet.pdf'
      and e.candidate_component_id=v_review_component
  ) then
    raise exception 'canonical final signed official timesheet evidence was not materialised';
  end if;
  if exists(
    select 1 from public.timesheet_evidence e
    where e.timesheet_id=v_timesheet and e.storage_key='workflow/review/timesheet.pdf'
  ) then
    raise exception 'manager-review artefact incorrectly consumed canonical evidence';
  end if;
  update public.candidate_approval_requests
  set state='SUPERSEDED',superseded_at_utc=now(),updated_at_utc=now()
  where id=v_approval_request;
  v_response:=public.candidate_submission_finalize_atomic_v1(
    null,'TEST',v_workflow,2,null,'workflow-finalise-v1',now(),
    jsonb_build_object('service_finalisation',jsonb_build_object(
      'contract_version','CANDIDATE_MANAGER_FINALISATION_V1',
      'workflow_generation',2,
      'replay_key_probe_only',true
    ))
  );
  if v_response->>'state'<>'FINALISED'
     or not coalesce((v_response->>'idempotent_replay')::boolean,false) then
    raise exception 'finalisation completion receipt was not replayable after approval history moved: %',v_response;
  end if;
end;
$workflow_finalisation$;

do $daily_manager_review$
declare
  v_client uuid:='eeeeeeee-1000-0000-0000-000000000001';
  v_candidate uuid:='eeeeeeee-1000-0000-0000-000000000002';
  v_contract uuid:='eeeeeeee-1000-0000-0000-000000000003';
  v_timesheet uuid:='eeeeeeee-1000-0000-0000-000000000004';
  v_account uuid:='eeeeeeee-1000-0000-0000-000000000005';
  v_session uuid:='eeeeeeee-1000-0000-0000-000000000006';
  v_workflow uuid:='eeeeeeee-1000-0000-0000-000000000007';
  v_candidate_signature uuid;
  v_review_component uuid;
  v_render_input_hash text;
  v_response jsonb;
begin
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'daily-workflow@example.test',true,'GCK-DAILY-TEST');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,sheet_scope,submission_mode,booking_id,worked_start_iso,week_ending_date,
    candidate_submission_route_intent
  ) values(v_timesheet,v_contract,'DAILY','MANUAL','DAILY-BOOKING-TEST',
    current_date::timestamptz+interval '9 hours',current_date,'ELECTRONIC');
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,worked_start_iso
  ) values(v_timesheet,v_candidate,v_client,true,current_date::timestamptz+interval '9 hours');
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,password_scheme,password_scheme_version,
    password_salt,password_digest,password_changed_at_utc
  ) values(
    v_account,'TEST','daily-workflow@example.test','ACTIVE','PBKDF2-HMAC-SHA256',1,
    decode(repeat('51',16),'hex'),decode(repeat('52',32),'hex'),now()
  );
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('53',32),'hex'),
    now()+interval '30 days',now()+interval '90 days'
  );

  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','DAILY','scope','DAILY','route','PHONE',
      'contract_id',v_contract,'target_timesheet_id',v_timesheet,
      'work_date',current_date,'input_snapshot','{}'::jsonb
    ),'daily-workflow-create-v1',now()
  );
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,
    jsonb_build_object(
      'component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE',
      'storage_key','daily/candidate-signature.png','media_type','image/png','byte_size',128
    ),'daily-candidate-signature-v1',now()
  );
  v_candidate_signature:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,
    jsonb_build_object(
      'component_id',v_candidate_signature,'source_content_sha256_hex',repeat('54',32),
      'verified_byte_size',128,'verified_media_type','image/png'
    ),'daily-candidate-signature-complete-v1',now()
  );
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'WORKER_SUBMIT',1,
    jsonb_build_object(
      'candidate_signature_component_id',v_candidate_signature,
      'candidate_signed_at_utc',now(),
      'renderer_contract_version','TIMESHEET_OFFICIAL_PDF_V1',
      'immutable_submission',jsonb_build_object(
        'booking_id','DAILY-BOOKING-TEST','work_date',current_date,
        'actual_start','09:00','actual_end','17:00',
        'break_start','12:00','break_end','13:00'
      )
    ),'daily-worker-submit-v1',now()
  );
  v_review_component:=(v_response->>'review_document_component_id')::uuid;
  v_render_input_hash:=private._candidate_component_render_contract_v1(
    v_workflow,2,v_review_component,'REVIEW'
  )->>'render_input_sha256';
  if v_response#>>'{render_contract,scope}'<>'DAILY' then
    raise exception 'DAILY render contract scope failed: %',v_response;
  end if;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'REGISTER_MANAGER_REVIEW_DOCUMENT',2,
    jsonb_build_object(
      'component_id',v_review_component,'storage_key','daily/review/timesheet.pdf',
      'content_sha256_hex',repeat('55',32),'render_input_sha256_hex',v_render_input_hash,
      'media_type','application/pdf','byte_size',2048,'page_count',1,
      'renderer_contract_version','TIMESHEET_OFFICIAL_PDF_V1',
      'renderer_receipt',jsonb_build_object(
        'form_variant','ELECTRONIC_MANAGER_REVIEW','workflow_id',v_workflow,
        'workflow_generation',2,'component_id',v_review_component,
        'component_kind','HOURS_TIMESHEET','document_role','ELECTRONIC_TIMESHEET_MANAGER_REVIEW',
        'review_ordinal',1,'scope','DAILY','page_count',1,
        'render_input_sha256',v_render_input_hash,
        'candidate_signature_embedded',true,'manager_signature_embedded',false,
        'manager_approval_date_embedded',false
      )
    ),'daily-register-review-v1',now()
  );
  if v_response->>'state'<>'READY_FOR_MANAGER_APPROVAL'
     or not exists(
       select 1 from public.candidate_submission_components c
       where c.id=v_review_component and c.workflow_generation=2
         and c.review_render_state='READY' and c.review_page_count=1
         and c.review_renderer_receipt_json->>'scope'='DAILY'
         and (c.review_renderer_receipt_json->>'candidate_signature_embedded')::boolean=true
         and (c.review_renderer_receipt_json->>'manager_signature_embedded')::boolean=false
     ) then
    raise exception 'DAILY manager-review document registration failed: %',v_response;
  end if;
  if exists(
    select 1 from public.timesheet_evidence where timesheet_id=v_timesheet
  ) then
    raise exception 'DAILY review artefact incorrectly became canonical evidence';
  end if;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'SELECT_PHONE_APPROVAL',2,
    jsonb_build_object(
      'approval_token_hash_hex',encode(extensions.digest(v_workflow::text||':daily-phone','sha256'),'hex'),
      'expires_at_utc',now()+interval '30 minutes','handoff_token_key_version',1,
      'public_broker_binding',jsonb_build_object(
        'contract_version','CANDIDATE_PUBLIC_PHONE_BINDING_V1',
        'public_session_binding_sha256',repeat('ab',32),
        'device_binding_sha256',repeat('cd',32)
      ),'broker_handoff_key_version',1
    ),
    'daily-select-phone-v1',now()
  );
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'AMEND',2,
    jsonb_build_object('input_snapshot',jsonb_build_object('amended',true)),
    'daily-amend-v2',now()
  );
  if v_response->>'state'<>'WORKER_DRAFT' or (v_response->>'generation')::integer<>3 then
    raise exception 'DAILY amendment did not return a new draft generation: %',v_response;
  end if;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',3,
    jsonb_build_object(
      'component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE',
      'storage_key','daily/candidate-signature-amended.png','media_type','image/png','byte_size',128
    ),'daily-candidate-signature-amended-v1',now()
  );
  v_candidate_signature:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',3,
    jsonb_build_object(
      'component_id',v_candidate_signature,'source_content_sha256_hex',repeat('56',32),
      'verified_byte_size',128,'verified_media_type','image/png'
    ),'daily-candidate-signature-amended-complete-v1',now()
  );
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'WORKER_SUBMIT',3,
    jsonb_build_object(
      'candidate_signature_component_id',v_candidate_signature,
      'candidate_signed_at_utc',now(),
      'renderer_contract_version','TIMESHEET_OFFICIAL_PDF_V1',
      'immutable_submission',jsonb_build_object(
        'booking_id','DAILY-BOOKING-TEST','work_date',current_date,
        'actual_start','09:00','actual_end','18:00',
        'break_start','12:00','break_end','13:00'
      )
    ),'daily-worker-amend-v3',now()
  );
  if v_response->>'state'<>'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'
     or (v_response->>'generation')::integer<>4
     or v_response#>>'{render_contract,render_input_sha256}'=v_render_input_hash
     or not exists(
       select 1 from public.candidate_submission_components
       where id=v_review_component and state='SUPERSEDED'
         and review_render_state='SUPERSEDED'
     )
     or exists(
       select 1 from public.candidate_approval_requests
       where workflow_id=v_workflow and workflow_generation=2 and state='PENDING'
     ) then
    raise exception 'DAILY amendment did not supersede old document/request generation: %',v_response;
  end if;
end;
$daily_manager_review$;

do $expense_carrier$
declare
  v_client uuid:='dddddddd-0000-0000-0000-000000000001';
  v_candidate uuid:='dddddddd-0000-0000-0000-000000000002';
  v_contract uuid:='dddddddd-0000-0000-0000-000000000003';
  v_week uuid:='dddddddd-0000-0000-0000-000000000004';
  v_timesheet uuid:='dddddddd-0000-0000-0000-000000000005';
  v_fin uuid:='dddddddd-0000-0000-0000-000000000006';
  v_response jsonb;
  v_carrier public.contract_weeks%rowtype;
begin
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active)
  values(v_candidate,'carrier@example.test',true);
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_expenses_require_separate_timesheet
  ) values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,line_type,submission_mode,r2_nurse_key,r2_auth_key
  ) values(
    v_timesheet,'HOURS','ELECTRONIC','existing/candidate-signature','existing/manager-signature'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id,enforce_day_partition,
    allowed_days_mask,split_boundary_date,split_group_key
  ) values(
    v_week,v_contract,current_date,0,'OPEN','ELECTRONIC',v_timesheet,
    true,'MON,TUE',current_date-3,'carrier-split'
  );
  insert into public.timesheets_financials(
    id,timesheet_id,candidate_id,client_id,total_hours
  ) values(v_fin,v_timesheet,v_candidate,v_client,8);

  v_response:=public.expense_carrier_resolve_or_create_atomic_v1(
    v_candidate,'TEST',v_timesheet,null,'carrier-create-v1',now()
  );
  if v_response->>'placement'<>'CREATE_CARRIER' then
    raise exception 'expense carrier creation failed: %',v_response;
  end if;
  select * into v_carrier from public.contract_weeks
  where id=(v_response->>'target_contract_week_id')::uuid;
  if not found
     or v_carrier.status<>'OPEN'
     or v_carrier.submission_mode_snapshot<>'MANUAL'
     or not v_carrier.is_adjustment
     or not v_carrier.enforce_day_partition
     or v_carrier.allowed_days_mask<>'MON,TUE'
     or v_carrier.split_boundary_date<>current_date-3
     or v_carrier.split_group_key<>'carrier-split'
     or v_carrier.totals_json->'hours' is null
     or v_carrier.totals_json->'expenses_draft' is null then
    raise exception 'expense carrier did not preserve office adjustment semantics: %',to_jsonb(v_carrier);
  end if;
end;
$expense_carrier$;

do $expense_application$
declare
  v_client uuid:='cccccccc-0000-0000-0000-000000000001';
  v_candidate uuid:='cccccccc-0000-0000-0000-000000000002';
  v_contract uuid:='cccccccc-0000-0000-0000-000000000003';
  v_week uuid:='cccccccc-0000-0000-0000-000000000004';
  v_timesheet uuid:='cccccccc-0000-0000-0000-000000000005';
  v_user uuid:='cccccccc-0000-0000-0000-000000000006';
  v_account uuid:='cccccccc-0000-0000-0000-000000000007';
  v_workflow uuid:='cccccccc-0000-0000-0000-000000000008';
  v_component uuid:='cccccccc-0000-0000-0000-000000000009';
  v_response jsonb;
  v_row_signature text;
begin
  insert into public.tms_users(id) values(v_user);
  update public.settings_defaults
  set candidate_app_system_actor_user_id=v_user
  where id=1;
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active)
  values(v_candidate,'expense@example.test',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(timesheet_id,line_type,submission_mode)
  values(v_timesheet,'EXPENSES','MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,1,'OPEN','MANUAL',v_timesheet);
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(v_account,'TEST','expense@example.test','SETUP_REQUIRED');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,target_timesheet_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'CREATED',1,v_contract,v_week,v_timesheet,current_date,'{}','{}','expense-workflow'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    expense_category,document_role,state,storage_key,media_type,byte_size,
    source_content_sha256,immutable_at_utc
  ) values(
    v_component,v_workflow,1,1,v_timesheet,'EXPENSE_EVIDENCE','OTHER','SOURCE_EVIDENCE',
    'IMMUTABLE','expense/evidence','image/jpeg',100,decode(repeat('cc',32),'hex'),now()
  );

  v_row_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_timesheet,v_week,false
  )->>'row_signature';
  begin
    perform public.timesheet_expense_apply_atomic_v1(
      v_candidate,'TEST',v_timesheet,v_workflow,1,v_row_signature,
      jsonb_build_object(
        'contract_week_id',v_week,
        'canonical_tsfin_snapshot',jsonb_build_object(
          'candidate_id',v_candidate,'client_id',v_client,
          'other_pay_ex_vat',10,'other_charge_ex_vat',12
        ),
        'timesheet_patch_json','{}'::jsonb,
        'contract_week_patch_json','{}'::jsonb,
        'evidence_display_name','Receipt'
      ),
      array[v_component],'expense-apply-too-early-v1',now()
    );
    raise exception 'expense application unexpectedly committed before manager approval/finalisation';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_EXPENSE_APPLY_FINALISE_ONLY' then raise; end if;
  end;
  if exists(
    select 1 from public.timesheet_evidence
    where candidate_component_id=v_component and timesheet_id=v_timesheet
  ) then
    raise exception 'premature expense application materialised evidence';
  end if;
end;
$expense_application$;

do $expense_invoice_routing$
declare
  v_client uuid:='eeeeeeee-0000-0000-0000-000000000001';
  v_candidate uuid:='eeeeeeee-0000-0000-0000-000000000002';
  v_contract uuid:='eeeeeeee-0000-0000-0000-000000000003';
  v_timesheet uuid:='eeeeeeee-0000-0000-0000-000000000004';
  v_invoice uuid:='eeeeeeee-0000-0000-0000-000000000005';
  v_group jsonb;
  v_route jsonb;
begin
  insert into public.clients(id,name,primary_invoice_email)
  values(v_client,'Invoice test client','primary@example.test');
  insert into public.candidates(id,email,active)
  values(v_candidate,'invoice-candidate@example.test',true);
  insert into public.client_settings(
    id,client_id,effective_from,invoice_consolidation_mode,
    candidate_expense_invoice_email,self_bill_no_invoices_sent
  ) values(
    gen_random_uuid(),v_client,current_date-30,'BY_WEEK',
    'expenses@example.test',true
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,self_bill,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,true,
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,is_current,updated_at
  ) values(v_timesheet,v_contract,current_date,'HOURS','MANUAL',true,now());
  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,timesheet_id
  ) values(v_contract,current_date,1,'AUTHORISED',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,is_current,is_stale,
    processing_status,basis,total_hours,hours_day,additional_pay_ex_vat,
    additional_charge_ex_vat,other_pay_ex_vat,other_charge_ex_vat
  ) values(
    v_timesheet,1,v_candidate,v_client,true,false,'READY_FOR_INVOICE','MANUAL',
    0,0,0,0,-10,-12
  );

  select to_jsonb(g) into v_group
  from private._invoice_generation_resolve_command_groups(
    jsonb_build_array(jsonb_build_object(
      'command_type','GENERATE_EXPENSES',
      'source_ids',jsonb_build_array(v_timesheet)
    )),null,now()
  ) g
  limit 1;
  if v_group->>'invoice_stream'<>'EXPENSE'
     or v_group->>'blocker_code' is not null
     or (v_group->>'self_bill')::boolean<>true then
    raise exception 'expense-only invoice classification failed: %',v_group;
  end if;

  insert into public.invoices(
    id,client_id,type,status,header_snapshot_json,do_not_send
  ) values(
    v_invoice,v_client,'INVOICE','DRAFT',
    jsonb_build_object(
      'invoice_stream','EXPENSE',
      'invoice_week_start',current_date-6,
      'meta',jsonb_build_object('self_bill',true,'invoice_week_start',current_date-6)
    ),false
  );
  insert into public.invoice_lines(invoice_id,timesheet_id)
  values(v_invoice,v_timesheet);

  select to_jsonb(r) into v_route
  from private._invoice_delivery_routes_batch(
    jsonb_build_array(jsonb_build_object(
      'request_key','expense-route-v1','invoice_id',v_invoice
    )),current_date
  ) r
  limit 1;
  if v_route->>'route_source'<>'EXPENSE_INVOICE_EMAIL'
     or coalesce((v_route->>'delivery_suppressed')::boolean,true)=true
     or v_route->'canonical_to'<>jsonb_build_array('expenses@example.test')
     or v_route->'blocker_codes'<>'[]'::jsonb then
    raise exception 'expense invoice delivery routing failed: %',v_route;
  end if;

  update public.client_settings
  set candidate_expense_invoice_email=null
  where client_id=v_client;
  select to_jsonb(g) into v_group
  from private._invoice_generation_resolve_command_groups(
    jsonb_build_array(jsonb_build_object(
      'command_type','GENERATE_EXPENSES',
      'source_ids',jsonb_build_array(v_timesheet)
    )),null,now()
  ) g
  limit 1;
  if v_group->>'blocker_code'<>'EXPENSE_INVOICE_EMAIL_REQUIRED' then
    raise exception 'missing expense email did not block grouping: %',v_group;
  end if;
end;
$expense_invoice_routing$;

rollback;
