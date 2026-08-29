-- Candidate App authority-boundary runtime verification.
-- Run only in a disposable PostgreSQL database after the full prospective bundle.
-- Every fixture write is rolled back.

begin;

update public.settings_defaults
set candidate_app_feature_flags_json=jsonb_build_object(
  'candidate_app_reads',true,
  'candidate_app_writes',true,
  'candidate_record_role_capabilities',true,
  'candidate_expense_atomic_placement',true,
  'candidate_manager_approval',true,
  'candidate_daily_finalisation',true
)
where id=1;

do $identity_and_authorised_lock$
declare
  v_now timestamptz:=date_trunc('second',now());
  v_client_one uuid:='94000000-0000-0000-0000-000000000001';
  v_candidate_one uuid:='94000000-0000-0000-0000-000000000002';
  v_contract_one uuid:='94000000-0000-0000-0000-000000000003';
  v_week_one uuid:='94000000-0000-0000-0000-000000000004';
  v_timesheet_one uuid:='94000000-0000-0000-0000-000000000005';
  v_authorised_carrier_week uuid:='94000000-0000-0000-0000-000000000006';
  v_authorised_carrier_timesheet uuid:='94000000-0000-0000-0000-000000000007';
  v_account_one uuid:='94000000-0000-0000-0000-000000000008';
  v_session_one uuid:='94000000-0000-0000-0000-000000000009';
  v_client_two uuid:='94000000-0000-0000-0000-000000000011';
  v_candidate_two uuid:='94000000-0000-0000-0000-000000000012';
  v_contract_two uuid:='94000000-0000-0000-0000-000000000013';
  v_week_two uuid:='94000000-0000-0000-0000-000000000014';
  v_timesheet_two uuid:='94000000-0000-0000-0000-000000000015';
  v_daily_two uuid:='94000000-0000-0000-0000-000000000016';
  v_capabilities jsonb;
  v_placement jsonb;
begin
  insert into public.clients(id) values(v_client_one),(v_client_two);
  insert into public.candidates(id,email,active,key_norm) values
    (v_candidate_one,'authority-one@example.test',true,'AUTHORITY-ONE'),
    (v_candidate_two,'authority-two@example.test',true,'AUTHORITY-TWO');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_expenses_require_separate_timesheet,allow_daily_manager_authorise_on_phone
  ) values
    (gen_random_uuid(),v_client_one,current_date-1,'ELECTRONIC',true,true),
    (gen_random_uuid(),v_client_two,current_date-1,'ELECTRONIC',true,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode
  ) values
    (v_contract_one,v_candidate_one,v_client_one,current_date-60,current_date+60,
      extract(dow from current_date)::integer,'ELECTRONIC'),
    (v_contract_two,v_candidate_two,v_client_two,current_date-60,current_date+60,
      extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,sheet_scope,
    booking_id,worked_start_iso,r2_nurse_key,r2_auth_key
  ) values
    (v_timesheet_one,v_contract_one,current_date,'HOURS','ELECTRONIC','WEEKLY',null,null,'fixture/candidate-one','fixture/manager-one'),
    (v_authorised_carrier_timesheet,v_contract_one,current_date,'EXPENSES','ELECTRONIC','WEEKLY',null,null,'fixture/candidate-carrier','fixture/manager-carrier'),
    (v_timesheet_two,v_contract_two,current_date,'HOURS','ELECTRONIC','WEEKLY',null,null,'fixture/candidate-two','fixture/manager-two'),
    (v_daily_two,v_contract_two,current_date,'HOURS','MANUAL','DAILY','BOOKED-SHIFT-TWO',v_now,null,null);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values
    (v_week_one,v_contract_one,current_date,0,'OPEN','ELECTRONIC',v_timesheet_one),
    (v_authorised_carrier_week,v_contract_one,current_date,1,'AUTHORISED','ELECTRONIC',v_authorised_carrier_timesheet),
    (v_week_two,v_contract_two,current_date,0,'OPEN','ELECTRONIC',v_timesheet_two);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status,authorised_at_utc
  ) values
    (v_timesheet_one,v_candidate_one,v_client_one,8,'UNPROCESSED',v_now),
    (v_authorised_carrier_timesheet,v_candidate_one,v_client_one,0,'UNPROCESSED',v_now),
    (v_timesheet_two,v_candidate_two,v_client_two,8,'UNPROCESSED',null),
    (v_daily_two,v_candidate_two,v_client_two,8,'UNPROCESSED',null);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account_one,'TEST','authority-one@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session_one,v_account_one,'TEST',v_candidate_one,'ACTIVE',decode(repeat('a1',32),'hex'),
    v_now+interval '30 days',v_now+interval '90 days'
  );

  v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet_one,v_week_one,'{}'::jsonb);
  if coalesce((v_capabilities->>'candidate_mutation_locked')::boolean,false)=false
     or coalesce((v_capabilities->>'can_edit_hours')::boolean,true)=true
     or coalesce((v_capabilities->>'can_edit_expenses')::boolean,false)=false
     or coalesce((v_capabilities->>'can_attach_timesheet')::boolean,true)=true
     or coalesce((v_capabilities->>'can_process')::boolean,true)=true then
    raise exception 'authorised record did not preserve locked hours plus separate expense entry: %',v_capabilities;
  end if;
  v_placement:=public.expense_placement_resolve_v1(
    v_candidate_one,'TEST',v_timesheet_one,v_week_one,'{}'::jsonb,v_now
  );
  if v_placement->>'placement'<>'CREATE_CARRIER'
     or (v_placement->>'target_timesheet_id')::uuid=v_authorised_carrier_timesheet then
    raise exception 'authorised anchor/carrier was reused for candidate mutation: %',v_placement;
  end if;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract_one,'contract_week_id',v_week_one,
        'target_timesheet_id',v_timesheet_one,'week_ending_date',current_date
      ),'authority:authorised-hours-mutation',v_now
    );
    raise exception 'authorised hours record accepted a candidate mutation workflow';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_RECORD_MUTATION_LOCKED' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_HOURS','scope','DAILY','route','ELECTRONIC',
        'contract_id',v_contract_one,'contract_week_id',v_week_one,
        'target_timesheet_id',v_timesheet_one,'week_ending_date',current_date
      ),'authority:weekly-kind-daily-scope',v_now
    );
    raise exception 'CONTRACT workflow accepted DAILY scope';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_CONTRACT_WORKFLOW_IDENTITY_REQUIRED' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','DAILY','scope','WEEKLY','route','PHONE',
        'target_timesheet_id',v_daily_two,'work_date',current_date
      ),'authority:daily-kind-weekly-scope',v_now
    );
    raise exception 'DAILY workflow accepted WEEKLY scope';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_DAILY_IDENTITY_INVALID' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','DAILY','scope','DAILY','route','PAPER',
        'target_timesheet_id',v_daily_two,'work_date',current_date
      ),'authority:daily-paper-route',v_now
    );
    raise exception 'DAILY workflow accepted PAPER route';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_DAILY_IDENTITY_INVALID' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','PHONE',
        'contract_id',v_contract_one,'contract_week_id',v_week_one,
        'target_timesheet_id',v_timesheet_one,'week_ending_date',current_date
      ),'authority:contract-phone-route',v_now
    );
    raise exception 'contract workflow accepted a manager-method route as its submission route';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_CONTRACT_WORKFLOW_IDENTITY_REQUIRED' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract_one,'contract_week_id',v_week_one,
        'anchor_timesheet_id',v_timesheet_two,'week_ending_date',current_date
      ),'authority:foreign-anchor',v_now
    );
    raise exception 'anchor from another contract/week was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract_two,'contract_week_id',v_week_two,
        'target_timesheet_id',v_timesheet_two,'week_ending_date',current_date
      ),'authority:cross-candidate-contract',v_now
    );
    raise exception 'cross-candidate contract identity was accepted';
  exception when sqlstate 'P0002' then
    if sqlerrm<>'CANDIDATE_WORKFLOW_CONTRACT_NOT_FOUND' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract_one,'contract_week_id',v_week_one,
        'target_timesheet_id',v_timesheet_one,'week_ending_date',current_date+7
      ),'authority:wrong-week-date',v_now
    );
    raise exception 'caller-supplied wrong week date was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_WORKFLOW_WEEK_MISMATCH' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract_one,'contract_week_id',v_week_one,
        'anchor_timesheet_id',v_timesheet_one,'target_timesheet_id',v_timesheet_one,
        'week_ending_date',current_date
      ),'authority:caller-expense-target',v_now
    );
    raise exception 'caller-selected expense target was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_EXPENSE_TARGET_SERVER_RESOLVED' then raise; end if;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST',gen_random_uuid(),'CREATE',1,
      jsonb_build_object(
        'workflow_kind','DAILY','scope','DAILY','route','PHONE',
        'target_timesheet_id',v_daily_two,'work_date',current_date
      ),'authority:cross-candidate-daily',v_now
    );
    raise exception 'cross-candidate DAILY shift was accepted';
  exception when sqlstate 'P0002' then
    if sqlerrm<>'CANDIDATE_DAILY_SHIFT_NOT_FOUND' then raise; end if;
  end;

  perform public.candidate_workflow_transition_atomic_v1(
    v_session_one,'TEST','94000000-0000-0000-0000-000000000017','CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract_one,'contract_week_id',v_week_one,
      'anchor_timesheet_id',v_timesheet_one,'week_ending_date',current_date
    ),'authority:first-expense-claim',v_now
  );
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session_one,'TEST','94000000-0000-0000-0000-000000000018','CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract_one,'contract_week_id',v_week_one,
        'anchor_timesheet_id',v_timesheet_one,'week_ending_date',current_date+7
      ),'authority:second-claim-false-date',v_now
    );
    raise exception 'second expense claim bypassed the gate with a false week date';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_WORKFLOW_WEEK_MISMATCH' then raise; end if;
  end;
end;
$identity_and_authorised_lock$;

do $manager_request_binding$
declare
  v_now timestamptz:=date_trunc('second',now());
  v_client uuid:='94100000-0000-0000-0000-000000000001';
  v_candidate uuid:='94100000-0000-0000-0000-000000000002';
  v_contract uuid:='94100000-0000-0000-0000-000000000003';
  v_week uuid:='94100000-0000-0000-0000-000000000004';
  v_timesheet uuid:='94100000-0000-0000-0000-000000000005';
  v_account uuid:='94100000-0000-0000-0000-000000000006';
  v_workflow uuid:='94100000-0000-0000-0000-000000000007';
  v_request uuid:='94100000-0000-0000-0000-000000000008';
  v_required uuid:='94100000-0000-0000-0000-000000000009';
  v_token_hex text:=repeat('b1',32);
  v_response jsonb;
  v_signature uuid;
  v_invalid_state text;
  v_phone_request uuid:='94100000-0000-0000-0000-000000000010';
  v_phone_signature uuid:='94100000-0000-0000-0000-000000000011';
begin
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'manager-binding@example.test',true,null);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.timesheets(timesheet_id,contract_id,week_ending_date,submission_mode)
  values(v_timesheet,v_contract,current_date,'MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'OPEN','ELECTRONIC',v_timesheet);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','manager-binding@example.test','ACTIVE');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key,review_manifest_sha256
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','ELECTRONIC',
    'AWAITING_MANAGER_APPROVAL',1,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    '{}','{}','manager-binding-workflow',decode(repeat('b2',32),'hex')
  );
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,method,state,manager_email_normalized,
    token_hash,expires_at_utc,review_manifest_sha256,required_component_ids,
    required_component_manifest_json,idempotency_key
  ) values(
    v_request,v_workflow,1,'EMAIL','PENDING','manager@example.test',
    decode(v_token_hex,'hex'),v_now-interval '1 second',decode(repeat('b2',32),'hex'),
    array[v_required],jsonb_build_array(jsonb_build_object('component_id',v_required)),
    'manager-binding-request'
  );

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_workflow,'COMPONENT_PREPARE',1,
      jsonb_build_object(
        'approval_token_hash_hex',v_token_hex,
        'component_kind','MANAGER_SIGNATURE','document_role','MANAGER_SIGNATURE',
        'storage_key','manager-binding/expired.png','media_type','image/png','byte_size',128
      ),'manager-binding:expired-prepare',v_now
    );
    raise exception 'expired manager request prepared a signature';
  exception when sqlstate '28000' then
    if sqlerrm<>'MANAGER_APPROVAL_REQUEST_NOT_READY' then raise; end if;
  end;

  foreach v_invalid_state in array array['REFUSED','CANCELLED','SUPERSEDED'] loop
    update public.candidate_approval_requests
    set state=v_invalid_state,
        refused_at_utc=case when v_invalid_state='REFUSED' then v_now else null end,
        cancelled_at_utc=case when v_invalid_state='CANCELLED' then v_now else null end,
        superseded_at_utc=case when v_invalid_state='SUPERSEDED' then v_now else null end,
        expires_at_utc=v_now+interval '1 hour'
    where id=v_request;
    begin
      perform public.candidate_workflow_transition_atomic_v1(
        null,'TEST',v_workflow,'COMPONENT_PREPARE',1,
        jsonb_build_object(
          'approval_token_hash_hex',v_token_hex,
          'component_kind','MANAGER_SIGNATURE','document_role','MANAGER_SIGNATURE',
          'storage_key','manager-binding/'||lower(v_invalid_state)||'.png',
          'media_type','image/png','byte_size',128
        ),'manager-binding:'||lower(v_invalid_state)||'-prepare',v_now
      );
      raise exception '% manager request prepared a signature',v_invalid_state;
    exception when sqlstate '28000' then
      if sqlerrm<>'MANAGER_APPROVAL_REQUEST_NOT_READY' then raise; end if;
    end;
  end loop;

  update public.candidate_approval_requests
  set state='PENDING',refused_at_utc=null,cancelled_at_utc=null,superseded_at_utc=null,
      expires_at_utc=v_now+interval '1 hour'
  where id=v_request;
  update public.candidate_submission_workflows set generation=2 where id=v_workflow;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_workflow,'COMPONENT_PREPARE',2,
      jsonb_build_object(
        'approval_token_hash_hex',v_token_hex,
        'component_kind','MANAGER_SIGNATURE','document_role','MANAGER_SIGNATURE',
        'storage_key','manager-binding/old-generation.png','media_type','image/png','byte_size',128
      ),'manager-binding:old-generation-prepare',v_now
    );
    raise exception 'old-generation manager request prepared a signature';
  exception when sqlstate '28000' then
    if sqlerrm<>'MANAGER_APPROVAL_REQUEST_NOT_READY' then raise; end if;
  end;
  update public.candidate_submission_workflows set generation=1 where id=v_workflow;

  update public.candidate_approval_requests
  set expires_at_utc=v_now+interval '1 hour'
  where id=v_request;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'COMPONENT_PREPARE',1,
    jsonb_build_object(
      'approval_token_hash_hex',v_token_hex,
      'component_kind','MANAGER_SIGNATURE','document_role','MANAGER_SIGNATURE',
      'storage_key','manager-binding/live.png','media_type','image/png','byte_size',128,
      'manager_signature_capture_method','DRAW',
      'expected_source_content_sha256_hex',repeat('b3',32)
    ),'manager-binding:live-prepare',v_now
  );
  v_signature:=(v_response->>'component_id')::uuid;
  if (select approval_request_id from public.candidate_submission_components where id=v_signature)
     is distinct from v_request then
    raise exception 'manager signature was not bound to the exact approval request';
  end if;

  update public.candidate_approval_requests
  set state='SUPERSEDED',superseded_at_utc=v_now
  where id=v_request;
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,method,state,review_manifest_sha256,
    required_component_ids,required_component_manifest_json,idempotency_key
  ) values(
    v_phone_request,v_workflow,1,'PHONE','PENDING',decode(repeat('b2',32),'hex'),
    array[v_required],jsonb_build_array(jsonb_build_object('component_id',v_required)),
    'manager-binding-phone-request'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,approval_request_id,
    component_kind,document_role,state,storage_key,media_type,byte_size,
    required,review_render_state,final_signed_render_state
  ) values(
    v_phone_signature,v_workflow,1,99,v_phone_request,'MANAGER_SIGNATURE','MANAGER_SIGNATURE',
    'PENDING','manager-binding/phone.png','image/png',128,false,'NOT_REQUIRED','NOT_REQUIRED'
  );
  update public.candidate_approval_requests
  set state='SUPERSEDED',superseded_at_utc=v_now where id=v_phone_request;
  update public.candidate_approval_requests
  set state='PENDING',superseded_at_utc=null where id=v_request;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_workflow,'COMPONENT_COMPLETE',1,
      jsonb_build_object(
        'approval_token_hash_hex',v_token_hex,'component_id',v_phone_signature,
        'source_content_sha256_hex',repeat('b4',32),
        'verified_byte_size',128,'verified_media_type','image/png'
      ),'manager-binding:email-cannot-complete-phone',v_now
    );
    raise exception 'email request completed a phone-request signature';
  exception when sqlstate '28000' then
    if sqlerrm<>'MANAGER_APPROVAL_REQUEST_NOT_READY' then raise; end if;
  end;

  update public.candidate_approval_requests
  set state='SUPERSEDED',superseded_at_utc=v_now
  where id=v_request;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_workflow,'COMPONENT_COMPLETE',1,
      jsonb_build_object(
        'approval_token_hash_hex',v_token_hex,'component_id',v_signature,
        'source_content_sha256_hex',repeat('b3',32),
        'verified_byte_size',128,'verified_media_type','image/png'
      ),'manager-binding:superseded-complete',v_now
    );
    raise exception 'superseded manager request completed a signature';
  exception when sqlstate '28000' then
    if sqlerrm<>'MANAGER_APPROVAL_REQUEST_NOT_READY' then raise; end if;
  end;
end;
$manager_request_binding$;

do $stable_pagination_and_hidden_carrier$
declare
  v_now timestamptz:=date_trunc('second',now());
  v_client uuid:='94200000-0000-0000-0000-000000000001';
  v_candidate uuid:='94200000-0000-0000-0000-000000000002';
  v_contract uuid:='94200000-0000-0000-0000-000000000003';
  v_account uuid:='94200000-0000-0000-0000-000000000004';
  v_session uuid:='94200000-0000-0000-0000-000000000005';
  v_page_one jsonb;
  v_page_two jsonb;
  v_page_three jsonb;
  v_cursor text;
  v_cursor_two text;
  v_ids uuid[];
begin
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'pagination@example.test',true,null);
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,
    candidate_expenses_require_separate_timesheet
  ) values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',
    extract(dow from current_date)::integer,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-90,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','pagination@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('c1',32),'hex'),
    v_now+interval '30 days',v_now+interval '90 days');

  for day_offset in 0..5 loop
    insert into public.timesheets(
      timesheet_id,contract_id,week_ending_date,line_type,submission_mode
    ) values(
      ('94200000-0000-0000-0000-'||lpad((100+day_offset)::text,12,'0'))::uuid,
      v_contract,current_date-(day_offset*7),'HOURS','MANUAL'
    );
    insert into public.contract_weeks(
      id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
    ) values(
      ('94200000-0000-0000-0000-'||lpad((200+day_offset)::text,12,'0'))::uuid,
      v_contract,current_date-(day_offset*7),0,'OPEN','ELECTRONIC',
      ('94200000-0000-0000-0000-'||lpad((100+day_offset)::text,12,'0'))::uuid
    );
    insert into public.timesheets_financials(
      timesheet_id,candidate_id,client_id,total_hours,processing_status,paid_at_utc
    ) values(
      ('94200000-0000-0000-0000-'||lpad((100+day_offset)::text,12,'0'))::uuid,
      v_candidate,v_client,8,'UNPROCESSED',
      case when day_offset in (1,3) then v_now else null end
    );
  end loop;

  -- A base-sequence expense-only carrier is never a visible candidate row.
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode
  ) values('94200000-0000-0000-0000-000000000300',v_contract,current_date-42,'EXPENSES','MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values('94200000-0000-0000-0000-000000000301',v_contract,current_date-42,0,
    'OPEN','ELECTRONIC','94200000-0000-0000-0000-000000000300');
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,other_pay_ex_vat,processing_status
  ) values('94200000-0000-0000-0000-000000000300',v_candidate,v_client,0,10,'UNPROCESSED');

  v_page_one:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,2,v_now);
  v_cursor:=v_page_one->>'next_cursor';
  if jsonb_array_length(v_page_one->'items')<>2 or v_cursor not like 'v2|CURRENT|%'
     or exists(select 1 from jsonb_array_elements(v_page_one->'items') item
       where item->>'timesheet_id'='94200000-0000-0000-0000-000000000300') then
    raise exception 'first stable page or hidden base carrier failed: %',v_page_one;
  end if;
  v_page_two:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',v_cursor,2,v_now);
  v_cursor_two:=v_page_two->>'next_cursor';
  v_page_three:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',v_cursor_two,2,v_now);
  select array_agg(distinct item_id order by item_id) into v_ids
  from (
    select (item->>'contract_week_id')::uuid as item_id from jsonb_array_elements(v_page_one->'items') item
    union all
    select (item->>'contract_week_id')::uuid as item_id from jsonb_array_elements(v_page_two->'items') item
    union all
    select (item->>'contract_week_id')::uuid as item_id from jsonb_array_elements(v_page_three->'items') item
  ) all_pages;
  if cardinality(v_ids)<>6 or jsonb_array_length(v_page_two->'items')<>2
     or jsonb_array_length(v_page_three->'items')<>2
     or v_page_three->>'next_cursor' is not null then
    raise exception 'cursor pagination skipped or duplicated rows: page1 %, page2 %, page3 %',
      v_page_one,v_page_two,v_page_three;
  end if;
  if not exists(
    select 1 from jsonb_array_elements(v_page_one->'readiness_conflicts') conflict
    where conflict->>'code'='EXPENSE_DISPLAY_ANCHOR_NOT_FOUND'
  ) then
    raise exception 'hidden base expense carrier did not produce a readiness conflict: %',v_page_one;
  end if;
  begin
    perform public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT','v0|1|2026-01-01|'||gen_random_uuid(),2,v_now);
    raise exception 'unsupported cursor version was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_CURSOR_INVALID' then raise; end if;
  end;
end;
$stable_pagination_and_hidden_carrier$;

rollback;
