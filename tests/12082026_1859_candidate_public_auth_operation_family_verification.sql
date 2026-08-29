-- Candidate public-auth operation-family durable receipt and key-scope verification.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_app_writes',true,
  'candidate_manager_approval',true,
  'candidate_account_registration',true
)
where id=1;

do $candidate_public_auth_operation_family$
declare
  v_now timestamptz:='2026-08-12 18:59:00+00';
  v_client uuid:='ca590000-0000-4000-8000-000000000001';
  v_candidate uuid:='ca590000-0000-4000-8000-000000000002';
  v_contract uuid:='ca590000-0000-4000-8000-000000000003';
  v_timesheet uuid:='ca590000-0000-4000-8000-000000000004';
  v_week uuid:='ca590000-0000-4000-8000-000000000005';
  v_account uuid:='ca590000-0000-4000-8000-000000000006';
  v_workflow_a uuid:='ca590000-0000-4000-8000-000000000007';
  v_workflow_b uuid:='ca590000-0000-4000-8000-000000000008';
  v_result jsonb;
  v_failed boolean;
begin
  insert into public.clients(id,name) values(v_client,'Candidate public auth operation-family client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'candidate-public-auth-operation@example.test',true,'CANDIDATE-PUBLIC-AUTH-OPERATION');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,2,'ELECTRONIC');
  insert into public.timesheets(timesheet_id,contract_id,week_ending_date,line_type,submission_mode)
  values(v_timesheet,v_contract,current_date,'HOURS','MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','candidate-public-auth-operation@example.test','ACTIVE');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,idempotency_key
  ) values
  (v_workflow_a,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PHONE',
    'AWAITING_MANAGER_APPROVAL',2,v_contract,v_week,v_timesheet,v_timesheet,current_date,'public-auth-workflow-a'),
  (v_workflow_b,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PHONE',
    'AWAITING_MANAGER_APPROVAL',2,v_contract,v_week,v_timesheet,v_timesheet,current_date,'public-auth-workflow-b');

  perform private._candidate_workflow_mutation_receipt_v1(
    v_workflow_a,'shared-public-operation-key',repeat('11',32),'SELECT_PHONE_APPROVAL',
    'CANDIDATE_CLIENT','ACCOUNT:'||v_account::text,
    jsonb_build_object('ok',true,'workflow_id',v_workflow_a,'generation',2),v_now
  );
  v_result:=private._candidate_workflow_mutation_receipt_v1(
    v_workflow_a,'shared-public-operation-key',repeat('11',32),'SELECT_PHONE_APPROVAL',
    'CANDIDATE_CLIENT','ACCOUNT:'||v_account::text,null,v_now+interval '1 second'
  );
  if not coalesce((v_result->>'found')::boolean,false)
     or v_result#>>'{response,workflow_id}'<>v_workflow_a::text then
    raise exception 'exact public operation receipt replay failed: %',v_result;
  end if;

  v_failed:=false;
  begin
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow_b,'shared-public-operation-key',repeat('22',32),'SELECT_PHONE_APPROVAL',
      'CANDIDATE_CLIENT','ACCOUNT:'||v_account::text,null,v_now+interval '2 seconds'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'same environment/key accepted a different workflow identity';
  end if;

  v_failed:=false;
  begin
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow_a,'shared-public-operation-key',repeat('33',32),'SELECT_PHONE_APPROVAL',
      'CANDIDATE_CLIENT','ACCOUNT:'||v_account::text,null,v_now+interval '3 seconds'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'same workflow/key accepted changed semantic binding';
  end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',v_account,'candidate-public-auth-operation@example.test',
    'ca590000-0000-4000-8000-000000000009',v_candidate,jsonb_build_object(
      'login_failed',true,
      'idempotency_request_sha256',repeat('44',32),
      'idempotency_key_version',1,
      'public_credential_versions',jsonb_build_object(
        'contract_version','CANDIDATE_PUBLIC_CREDENTIAL_VERSIONS_V1',
        'access_key_version',2,'refresh_key_version',3,'public_session_key_version',4
      )
    ),'public-login-versioned-receipt',v_now
  );
  if v_result->>'error_code'<>'CANDIDATE_LOGIN_INVALID' then
    raise exception 'versioned login failure receipt setup failed: %',v_result;
  end if;
  v_result:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',v_account,'candidate-public-auth-operation@example.test',
    'ca590000-0000-4000-8000-000000000009',v_candidate,jsonb_build_object(
      'replay_probe_only',true,
      'idempotency_request_sha256',repeat('44',32),
      'idempotency_key_version',1
    ),'public-login-versioned-receipt',v_now+interval '1 minute'
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or v_result#>>'{public_credential_versions,access_key_version}'<>'2'
     or v_result#>>'{public_credential_versions,refresh_key_version}'<>'3'
     or v_result#>>'{public_credential_versions,public_session_key_version}'<>'4' then
    raise exception 'public credential versions were not frozen in the durable auth result: %',v_result;
  end if;
end;
$candidate_public_auth_operation_family$;

rollback;
