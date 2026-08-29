\set ON_ERROR_STOP on
begin;

do $verify_definitions$
declare
  v_qr text := pg_get_functiondef(
    'public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz)'::regprocedure);
  v_complete text := pg_get_functiondef(
    'public.invoice_work_complete_batch(jsonb,timestamptz)'::regprocedure);
  v_claim text := pg_get_functiondef(
    'public.email_outbox_claim_ready_batch(integer,text,integer)'::regprocedure);
begin
  if position('CANDIDATE_PAPER_PACK_PENDING' in v_qr) = 0
     or position('candidate_paper_pack_ready' in v_qr) = 0
     or position('CANDIDATE_PAPER_WORKFLOW_CONFLICT' in v_qr) = 0 then
    raise exception 'Candidate PAPER identity/hold is not owned by QR enqueue';
  end if;
  if position('candidate_workflow_id' in v_complete) = 0 then
    raise exception 'Ordinary QR completion does not exclude Candidate-bound mail';
  end if;
  if position('candidate_complete_pack_storage_key' in v_claim) = 0
     or position('candidate_paper_pack_ready' in v_claim) = 0 then
    raise exception 'Mail claim does not prove the complete Candidate pack';
  end if;
end;
$verify_definitions$;

do $verify_claim_runtime$
declare
  v_candidate_id constant uuid := '10000000-0000-4000-8000-000000000001';
  v_ordinary_id constant uuid := '10000000-0000-4000-8000-000000000002';
  v_workflow_id constant uuid := '10000000-0000-4000-8000-000000000003';
  v_timesheet_id constant uuid := '10000000-0000-4000-8000-000000000004';
  v_partial_id constant uuid := '10000000-0000-4000-8000-000000000005';
  v_person_id constant uuid := '10000000-0000-4000-8000-000000000006';
  v_client_id constant uuid := '10000000-0000-4000-8000-000000000007';
  v_contract_id constant uuid := '10000000-0000-4000-8000-000000000008';
  v_week_id constant uuid := '10000000-0000-4000-8000-000000000009';
  v_account_id constant uuid := '10000000-0000-4000-8000-000000000010';
  v_manifest constant text := repeat('a',64);
  v_pack_hash constant text := repeat('b',64);
  v_claimed uuid[];
begin
  insert into public.candidates(id,email,active)
  values(v_person_id,'candidate-paper@example.invalid',true);
  insert into public.clients(id,name) values(v_client_id,'Candidate paper test client');
  insert into public.contracts(id,candidate_id,client_id,start_date,end_date)
  values(v_contract_id,v_person_id,v_client_id,'2026-01-01','2026-12-31');
  insert into public.timesheets(
    timesheet_id,submission_mode,sheet_scope,contract_id,booking_id,week_ending_date
  ) values(v_timesheet_id,'MANUAL','WEEKLY',v_contract_id,'PAPER-CLAIM-TEST','2026-08-16');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week_id,v_contract_id,'2026-08-16','SUBMITTED','MANUAL',v_timesheet_id);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account_id,'TEST','candidate-paper@example.invalid','ACTIVE');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,paper_return_manifest_json,paper_return_manifest_sha256
  ) values(
    v_workflow_id,'TEST',v_account_id,v_person_id,'CONTRACT_HOURS','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',2,v_contract_id,v_week_id,v_timesheet_id,v_timesheet_id,
    '2026-08-16','candidate-paper-claim-test',
    jsonb_build_object('workflow_id',v_workflow_id,'workflow_generation',2,
      'pages',jsonb_build_array(jsonb_build_object(
        'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))),
    decode(v_manifest,'hex')
  );

  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,context_kind,context_id,
    scheduled_for_utc,next_attempt_at_utc,payment_scope_json
  ) values (
    v_candidate_id,'TIMESHEET_QR','candidate-paper@example.invalid','Candidate paper pack',
    '[]'::jsonb,'QUEUED',clock_timestamp()-interval '2 minutes','timesheets',v_timesheet_id,
    clock_timestamp()-interval '1 minute',clock_timestamp()-interval '1 minute',
    jsonb_build_object(
      'candidate_workflow_id',v_workflow_id,
      'candidate_workflow_generation',2,
      'paper_return_manifest_sha256',v_manifest,
      'candidate_paper_pack_ready',false,
      'mail_held_until_pdf_rendered',true,
      'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING'
    )
  );

  select coalesce(array_agg(claimed.id),'{}'::uuid[]) into v_claimed
  from public.email_outbox_claim_ready_batch(10,'candidate-held-check',5) claimed;
  if cardinality(v_claimed) <> 0 then
    raise exception 'Held Candidate PAPER mail became claimable: %',v_claimed;
  end if;

  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,context_kind,context_id,
    scheduled_for_utc,next_attempt_at_utc,payment_scope_json
  ) values (
    v_ordinary_id,'TIMESHEET_QR','ordinary-qr@example.invalid','Ordinary QR pack',
    '[]'::jsonb,'QUEUED',clock_timestamp()-interval '1 minute','timesheets',gen_random_uuid(),
    clock_timestamp()-interval '1 minute',clock_timestamp()-interval '1 minute','{}'::jsonb
  );

  select coalesce(array_agg(claimed.id),'{}'::uuid[]) into v_claimed
  from public.email_outbox_claim_ready_batch(10,'ordinary-qr-check',5) claimed;
  if v_claimed is distinct from array[v_ordinary_id] then
    raise exception 'Ordinary non-Candidate QR claim regression: %',v_claimed;
  end if;

  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,context_kind,context_id,
    scheduled_for_utc,next_attempt_at_utc,payment_scope_json
  ) values (
    v_partial_id,'TIMESHEET_QR','partial-candidate@example.invalid','Partial Candidate binding',
    '[]'::jsonb,'QUEUED',clock_timestamp()-interval '1 minute','timesheets',gen_random_uuid(),
    clock_timestamp()-interval '1 minute',clock_timestamp()-interval '1 minute',
    jsonb_build_object('candidate_paper_pack_ready',false)
  );

  select coalesce(array_agg(claimed.id),'{}'::uuid[]) into v_claimed
  from public.email_outbox_claim_ready_batch(10,'candidate-partial-check',5) claimed;
  if cardinality(v_claimed) <> 0 then
    raise exception 'A partial Candidate PAPER marker fell through to ordinary QR claim: %',v_claimed;
  end if;

  update public.mail_outbox
  set attachments=jsonb_build_array(jsonb_build_object(
        'r2_key','candidate-app/test/complete-pack.pdf',
        'filename','Timesheet.pdf',
        'content_type','application/pdf',
        'sha256',v_pack_hash,
        'size_bytes',500,
        'page_count',3,
        'candidate_workflow_id',v_workflow_id,
        'candidate_workflow_generation',2,
        'paper_return_manifest_sha256',v_manifest)),
      payment_scope_json=payment_scope_json||jsonb_build_object(
        'candidate_paper_pack_ready',true,
        'mail_held_until_pdf_rendered',false,
        'mail_hold_reason',null,
        'candidate_complete_pack_storage_key','candidate-app/test/complete-pack.pdf',
        'candidate_complete_pack_sha256',v_pack_hash,
        'candidate_complete_pack_size_bytes',500,
        'candidate_complete_pack_page_count',3,
        'candidate_complete_pack_media_type','application/pdf'),
      scheduled_for_utc=clock_timestamp()-interval '1 second',
      next_attempt_at_utc=clock_timestamp()-interval '1 second'
  where id=v_candidate_id;

  select coalesce(array_agg(claimed.id),'{}'::uuid[]) into v_claimed
  from public.email_outbox_claim_ready_batch(10,'candidate-ready-check',5) claimed;
  if v_claimed is distinct from array[v_candidate_id] then
    raise exception 'Exact complete Candidate pack was not claimable: %',v_claimed;
  end if;
end;
$verify_claim_runtime$;

rollback;
