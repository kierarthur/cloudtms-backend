-- Candidate semantic replay and PAPER execution authority closure.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_app_writes',true,'candidate_paper_qr',true
)
where id=1;

do $candidate_execution_closure$
declare
  v_now timestamptz:='2026-08-12 00:59:00+00';
  v_actor uuid:='ce590000-0000-4000-8000-000000000001';
  v_client uuid:='ce590000-0000-4000-8000-000000000002';
  v_candidate uuid:='ce590000-0000-4000-8000-000000000003';
  v_contract uuid:='ce590000-0000-4000-8000-000000000004';
  v_timesheet uuid:='ce590000-0000-4000-8000-000000000005';
  v_week uuid:='ce590000-0000-4000-8000-000000000006';
  v_account uuid:='ce590000-0000-4000-8000-000000000007';
  v_finalised uuid:='ce590000-0000-4000-8000-000000000008';
  v_paper uuid:='ce590000-0000-4000-8000-000000000009';
  v_mail uuid:='ce590000-0000-4000-8000-000000000010';
  v_finalise_key text:='candidate-finalisation-replay-closure-v2';
  v_reject_key text:='candidate-rejection-replay-closure-v2';
  v_attempt_token text:=repeat('8b',32);
  v_manifest jsonb:=jsonb_build_object('pages',jsonb_build_array(
    jsonb_build_object('page_key','hours:1','component_kind','HOURS_TIMESHEET')
  ));
  v_manifest_hash text;
  v_service jsonb;
  v_hash text;
  v_result jsonb;
  v_failed boolean;
begin
  insert into public.clients(id,name) values(v_client,'Candidate execution closure client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'candidate-execution-closure@example.test',true,'CANDIDATE-EXECUTION-CLOSURE');
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
  values(v_account,'TEST','candidate-execution-closure@example.test','ACTIVE');

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,idempotency_key
  ) values(
    v_finalised,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER','FINALISED',2,
    v_contract,v_week,v_timesheet,v_timesheet,current_date,'candidate-finalised-creation'
  );
  v_service:=jsonb_build_object(
    'contract_version','CANDIDATE_MANAGER_FINALISATION_V1',
    'approval_request_id',null,'approval_method','PAPER','workflow_generation',1,
    'review_manifest_sha256_hex','','actor_user_id',null
  );
  v_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_FINALISATION_MUTATION_REQUEST_V2',
    'workflow_id',v_finalised,'action','RETRY_FINALISATION','expected_generation',1,
    'service_finalisation',v_service,'channel','SERVICE','actor_identity','SERVICE'
  )::text,'UTF8'),'sha256'),'hex');
  perform private._candidate_workflow_mutation_receipt_v1(
    v_finalised,v_finalise_key,v_hash,'RETRY_FINALISATION','SERVICE','SERVICE',
    jsonb_build_object('ok',true,'workflow_id',v_finalised,'state','FINALISED','generation',2),v_now
  );
  v_result:=public.candidate_submission_finalize_atomic_v1(
    null,'TEST',v_finalised,1,null,v_finalise_key,v_now+interval '1 minute',
    jsonb_build_object('service_finalisation',v_service||jsonb_build_object('replay_probe_only',true))
  );
  if v_result->>'state'<>'FINALISED'
     or not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'finalisation did not replay before current-generation/state validation: %',v_result;
  end if;

  v_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_REJECTION_REQUEST_V2','environment','TEST',
    'actor_user_id',v_actor,'timesheet_id',v_timesheet,'expected_timesheet_id',v_timesheet,
    'expected_row_signature','ROW-SIGNATURE-V1','reason','Candidate supplied the wrong week.'
  )::text,'UTF8'),'sha256'),'hex');
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,reason,correlation_id,ts_utc
  ) values(
    v_actor,'candidate_submission_rejection_receipt',v_timesheet::text,
    'CANDIDATE_SUBMISSION_REJECTION_RECEIPT',jsonb_build_object(
      'request_sha256',v_hash,'contract_version','CANDIDATE_REJECTION_REQUEST_V2'
    ),jsonb_build_object('ok',true,'timesheet_id',v_timesheet),
    'Candidate supplied the wrong week.',v_reject_key,v_now
  );
  v_result:=public.candidate_submission_reject_atomic_v1(
    v_actor,'TEST',v_timesheet,v_timesheet,'ROW-SIGNATURE-V1',
    'Candidate supplied the wrong week.',v_reject_key,v_now+interval '1 minute'
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'canonical rejection did not return its exact durable replay: %',v_result;
  end if;
  v_failed:=false;
  begin
    perform public.candidate_submission_reject_atomic_v1(
      v_actor,'TEST',v_timesheet,v_timesheet,'ROW-SIGNATURE-V1',
      'A materially different rejection reason.',v_reject_key,v_now+interval '2 minutes'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'canonical rejection accepted a different request under the same key';
  end if;

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,paper_return_manifest_json,paper_return_manifest_sha256
  ) values(
    v_paper,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','PAPER','AWAITING_PAPER_RETURN',1,
    v_contract,v_week,v_timesheet,null,current_date,'candidate-paper-creation',
    v_manifest,private._candidate_sha256_jsonb_v1(v_manifest)
  );
  v_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_manifest),'hex');
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json,attachments
  ) values(
    v_mail,'TIMESHEET_QR','candidate-execution-closure@example.test','Paper pack','Preparing.',
    'QUEUED','timesheets',v_timesheet,'CANDIDATE-EXECUTION-CLOSURE-PAPER',jsonb_build_object(
      'candidate_mail_authority','CANDIDATE_PAPER_V1','candidate_workflow_id',v_paper,
      'candidate_workflow_generation',1,'paper_return_manifest_sha256',v_manifest_hash,
      'candidate_paper_pack_ready',false,'candidate_paper_pack_retryable',true,
      'candidate_paper_pack_failure_class','RETRYABLE',
      'candidate_paper_pack_next_retry_at_utc',v_now-interval '1 minute',
      'candidate_paper_pack_attempt_count',0,'mail_held_until_pdf_rendered',true,
      'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING'
    ),'[]'::jsonb
  );
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
      'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_token
    ),'candidate-paper-attempt-1',v_now
  );
  if v_result->>'paper_pack_attempt_state'<>'CLAIMED'
     or (v_result->>'paper_pack_attempt_count')::integer<>1 then
    raise exception 'PAPER execution did not acquire its exact database attempt lease: %',v_result;
  end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_MARK_FAILURE',1,jsonb_build_object(
      'service_paper_pack_failure',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'paper_pack_attempt_token',v_attempt_token,
      'error_code','CANDIDATE_PAPER_R2_WRITE_TRANSIENT'
    ),'candidate-paper-attempt-1-failure',v_now+interval '1 second'
  );
  if v_result->>'paper_pack_state'<>'FAILED_RETRYABLE'
     or nullif(v_result->>'next_retry_at_utc','') is null
     or nullif((select payment_scope_json->>'candidate_paper_pack_attempt_token'
                from public.mail_outbox where id=v_mail),'') is not null then
    raise exception 'retryable PAPER failure did not close the lease and persist backoff: %',v_result;
  end if;
  v_failed:=false;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
        'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
        'paper_return_manifest_sha256',v_manifest_hash,
        'paper_pack_attempt_token',repeat('8c',32)
      ),'candidate-paper-attempt-before-due',v_now+interval '2 seconds'
    );
  exception when sqlstate '55000' then
    v_failed:=position('CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'scheduler PAPER attempt ignored the durable retry backoff';
  end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_paper,'PAPER_PACK_MARK_FAILURE',1,jsonb_build_object(
      'service_paper_pack_failure',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'error_code','CANDIDATE_PAPER_DOCUMENT_FAILED'
    ),'candidate-paper-terminal-failure',v_now+interval '3 seconds'
  );
  if v_result->>'paper_pack_state'<>'FAILED_TERMINAL' then
    raise exception 'terminal PAPER failure was not persisted: %',v_result;
  end if;
  v_failed:=false;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_paper,'PAPER_PACK_ATTEMPT_CLAIM',1,jsonb_build_object(
        'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
        'paper_return_manifest_sha256',v_manifest_hash,
        'paper_pack_attempt_token',repeat('8d',32)
      ),'candidate-paper-attempt-after-terminal',v_now+interval '4 seconds'
    );
  exception when sqlstate '55000' then
    v_failed:=position('CANDIDATE_PAPER_PACK_FAILED_TERMINAL' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'terminal PAPER failure remained executable';
  end if;
end;
$candidate_execution_closure$;

rollback;
