-- Candidate execution-boundary closure: one PAPER executor, advancing backoff,
-- shared failure truth and durable Office retry results.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_app_writes',true,'candidate_paper_qr',true
)
where id=1;

do $candidate_execution_boundary$
declare
  v_now timestamptz:=clock_timestamp();
  v_actor uuid:='ce592200-0000-4000-8000-000000000001';
  v_client uuid:='ce592200-0000-4000-8000-000000000002';
  v_candidate uuid:='ce592200-0000-4000-8000-000000000003';
  v_contract uuid:='ce592200-0000-4000-8000-000000000004';
  v_timesheet uuid:='ce592200-0000-4000-8000-000000000005';
  v_week uuid:='ce592200-0000-4000-8000-000000000006';
  v_account uuid:='ce592200-0000-4000-8000-000000000007';
  v_paper uuid:='ce592200-0000-4000-8000-000000000008';
  v_mail uuid:='ce592200-0000-4000-8000-000000000010';
  v_attempt_one text:=repeat('91',32);
  v_attempt_two text:=repeat('92',32);
  v_attempt_three text:=repeat('93',32);
  v_attempt_four text:=repeat('94',32);
  v_operation_one text:='paper-operation-boundary-1';
  v_operation_two text:='paper-operation-boundary-2';
  v_operation_three text:='paper-operation-boundary-3';
  v_operation_four text:='paper-operation-boundary-4';
  v_manifest jsonb:=jsonb_build_object('pages',jsonb_build_array(
    jsonb_build_object('page_key','hours:1','component_kind','HOURS_TIMESHEET')
  ));
  v_manifest_hash text;
  v_result jsonb;
  v_readiness jsonb;
  v_failed boolean;
begin
  insert into public.clients(id,name) values(v_client,'Candidate execution boundary client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'candidate-execution-boundary@example.test',true,'CANDIDATE-EXECUTION-BOUNDARY');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,2,'ELECTRONIC');
  insert into public.timesheets(timesheet_id,contract_id,week_ending_date,line_type,submission_mode)
  values(v_timesheet,v_contract,current_date,'HOURS','MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status
  ) values(v_timesheet,v_candidate,v_client,8,'UNPROCESSED');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','candidate-execution-boundary@example.test','ACTIVE');

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,paper_return_manifest_json,paper_return_manifest_sha256
  ) values(
    v_paper,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','PAPER','AWAITING_PAPER_RETURN',1,
    v_contract,v_week,v_timesheet,null,current_date,'candidate-paper-boundary-creation',
    v_manifest,private._candidate_sha256_jsonb_v1(v_manifest)
  );
  v_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_manifest),'hex');
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json,attachments
  ) values(
    v_mail,'TIMESHEET_QR','candidate-execution-boundary@example.test','Paper pack','Preparing.',
    'QUEUED','timesheets',v_timesheet,'CANDIDATE-EXECUTION-BOUNDARY-PAPER',jsonb_build_object(
      'candidate_mail_authority','CANDIDATE_PAPER_V1','candidate_workflow_id',v_paper,
      'candidate_workflow_generation',1,'paper_return_manifest_sha256',v_manifest_hash,
      'candidate_paper_pack_ready',false,'candidate_paper_pack_retryable',false,
      'candidate_paper_pack_attempt_count',0,'mail_held_until_pdf_rendered',true,
      'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING'
    ),'[]'::jsonb
  );

  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
      'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_one,
      'paper_pack_operation_id',v_operation_one
    ),v_operation_one,v_now
  );
  if v_result->>'paper_pack_attempt_state'<>'CLAIMED'
     or not coalesce((v_result->>'claim_acquired_new')::boolean,false)
     or v_result->>'paper_pack_operation_id'<>v_operation_one then
    raise exception 'fresh PAPER operation did not acquire the exact executor lease: %',v_result;
  end if;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
      'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_one,
      'paper_pack_operation_id',v_operation_one
    ),v_operation_one,v_now+interval '1 second'
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or coalesce((v_result->>'claim_acquired_new')::boolean,true) then
    raise exception 'exact PAPER claim retry incorrectly re-acquired executor authority: %',v_result;
  end if;

  v_failed:=false;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
        'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
        'paper_return_manifest_sha256',v_manifest_hash,
        'paper_pack_attempt_token',v_attempt_two,
        'paper_pack_operation_id',v_operation_two
      ),v_operation_two,v_now+interval '2 seconds'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_PAPER_PACK_ATTEMPT_IN_PROGRESS' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'a second PAPER operation acquired a live executor lease';
  end if;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_MARK_FAILURE',1,jsonb_build_object(
      'service_paper_pack_failure',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_one,
      'paper_pack_operation_id',v_operation_one,
      'error_code','CANDIDATE_PAPER_R2_WRITE_TRANSIENT'
    ),v_operation_one||':failure',v_now+interval '3 seconds'
  );
  if v_result->>'paper_pack_state'<>'FAILED_RETRYABLE'
     or (v_result->>'paper_pack_attempt_count')::integer<>1
     or v_result->>'paper_pack_operation_id'<>v_operation_one then
    raise exception 'first PAPER failure did not own count/backoff/result: %',v_result;
  end if;

  v_failed:=false;
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'WORKFLOW_ACTION_EXECUTE',v_actor,'TEST',jsonb_build_object(
        'workflow_id',v_paper,'generation',1,
        'workflow_action','PAPER_PACK_ATTEMPT_CLAIM',
        'idempotency_key','office-paper-attempt-before-due',
        'payload',jsonb_build_object(
          'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
          'paper_return_manifest_sha256',v_manifest_hash,
          'paper_pack_attempt_token',repeat('9a',32),
          'paper_pack_operation_id','office-paper-operation-before-due'
        )
      ),v_now+interval '4 seconds'
    );
  exception when sqlstate '55000' then
    v_failed:=position('CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'Office PAPER attempt bypassed the durable retry backoff';
  end if;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
      'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_two,
      'paper_pack_operation_id',v_operation_two
    ),v_operation_two,v_now+interval '2 minutes'
  );
  if (v_result->>'paper_pack_attempt_count')::integer<>2
     or v_result->>'paper_pack_operation_id'<>v_operation_two then
    raise exception 'second PAPER attempt did not advance the durable count: %',v_result;
  end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_MARK_FAILURE',1,jsonb_build_object(
      'service_paper_pack_failure',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_two,
      'paper_pack_operation_id',v_operation_two,
      'error_code','CANDIDATE_PAPER_SOURCE_READ_TRANSIENT'
    ),v_operation_two||':failure',v_now+interval '2 minutes 1 second'
  );
  if (v_result->>'next_retry_at_utc')::timestamptz
       is distinct from v_now+interval '7 minutes 1 second' then
    raise exception 'second PAPER failure did not advance to five-minute backoff: %',v_result;
  end if;
  v_readiness:=private._candidate_paper_pack_readiness_v1(v_paper,1);
  if v_readiness->>'state'<>'BACKOFF'
     or v_readiness->>'failure_scope'<>'OUTBOX'
     or (v_readiness->>'attempt_count')::integer<>2
     or v_readiness->>'operation_id'<>v_operation_two then
    raise exception 'Candidate shared PAPER read did not expose durable failure truth: %',v_readiness;
  end if;

  -- A worker crash after claim leaves the exact lease in place. Once that
  -- lease expires, a new attempt key and token must recover the operation.
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
      'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_three,
      'paper_pack_operation_id',v_operation_three
    ),v_operation_three,v_now+interval '8 minutes'
  );
  if not coalesce((v_result->>'claim_acquired_new')::boolean,false)
     or (v_result->>'paper_pack_attempt_count')::integer<>3 then
    raise exception 'third PAPER attempt did not acquire its crash-recovery lease: %',v_result;
  end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
      'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_four,
      'paper_pack_operation_id',v_operation_four
    ),v_operation_four,v_now+interval '18 minutes 1 second'
  );
  if not coalesce((v_result->>'claim_acquired_new')::boolean,false)
     or (v_result->>'paper_pack_attempt_count')::integer<>4
     or v_result->>'paper_pack_operation_id'<>v_operation_four then
    raise exception 'expired PAPER executor lease did not recover with a new attempt: %',v_result;
  end if;

  -- The shared reader must also expose a failure which happened before an
  -- outbox row could be created. Reuse the same workflow after removing the
  -- synthetic outbox so this fixture does not create a second active claim.
  delete from public.mail_outbox where id=v_mail;
  update public.candidate_submission_workflows
  set last_mutation_response_json=jsonb_build_object(
      'ok',true,'failure_scope','WORKFLOW','paper_pack_state','FAILED_RETRYABLE',
      'retryable',true,'failure_code','CANDIDATE_PAPER_DOCUMENT_PENDING_TIMEOUT',
      'paper_pack_operation_id','paper-document-pending-operation'
    )
  where id=v_paper;
  v_readiness:=private._candidate_paper_pack_readiness_v1(v_paper,1);
  if v_readiness->>'state'<>'PREPARING'
     or coalesce((v_readiness->>'retryable')::boolean,false) then
    raise exception 'document-pending timeout incorrectly exposed a pack retry: %',v_readiness;
  end if;
  update public.candidate_submission_workflows
  set last_mutation_response_json=jsonb_build_object(
      'ok',true,'failure_scope','WORKFLOW','paper_pack_state','FAILED_TERMINAL',
      'failure_code','CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED',
      'paper_pack_operation_id','paper-pre-outbox-operation'
    )
  where id=v_paper;
  v_readiness:=private._candidate_paper_pack_readiness_v1(v_paper,1);
  if v_readiness->>'state'<>'FAILED_TERMINAL'
     or v_readiness->>'failure_scope'<>'WORKFLOW'
     or v_readiness->>'operation_id'<>'paper-pre-outbox-operation' then
    raise exception 'pre-outbox workflow failure remained invisible: %',v_readiness;
  end if;

  v_result:=public.cloudtms_office_candidate_adapter_v1(
    'PAPER_RETRY_REPLAY',v_actor,'TEST',jsonb_build_object(
      'workflow_id',v_paper,'generation',1,'idempotency_key','office-paper-retry-operation'
    ),v_now
  );
  if coalesce((v_result->>'found')::boolean,true) then
    raise exception 'unknown Office retry operation incorrectly replayed: %',v_result;
  end if;
  v_result:=public.cloudtms_office_candidate_adapter_v1(
    'PAPER_RETRY_RECORD',v_actor,'TEST',jsonb_build_object(
      'workflow_id',v_paper,'generation',1,'idempotency_key','office-paper-retry-operation',
      'http_status',503,'result',jsonb_build_object(
        'ok',false,'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
        'idempotency_key','office-paper-retry-operation','workflow_id',v_paper,
        'generation',1,'paper_pack_state','FAILED_RETRYABLE','retryable',true,
        'error_code','CANDIDATE_PAPER_SOURCE_READ_TRANSIENT',
        'next_retry_at_utc',v_now+interval '5 minutes','idempotent_replay',false
      )
    ),v_now
  );
  v_result:=public.cloudtms_office_candidate_adapter_v1(
    'PAPER_RETRY_REPLAY',v_actor,'TEST',jsonb_build_object(
      'workflow_id',v_paper,'generation',1,'idempotency_key','office-paper-retry-operation'
    ),v_now+interval '1 minute'
  );
  if not coalesce((v_result->>'found')::boolean,false)
     or not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or v_result#>>'{result,paper_pack_state}'<>'FAILED_RETRYABLE'
     or (v_result->>'http_status')::integer<>503 then
    raise exception 'Office retry UUID did not retain its complete durable result: %',v_result;
  end if;
end;
$candidate_execution_boundary$;

rollback;
