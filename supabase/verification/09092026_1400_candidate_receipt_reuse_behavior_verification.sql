\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for exact Candidate receipt reuse.
-- A physical receipt becomes reusable only when its exact previous use has
-- ended and no current component or materialised Timesheet evidence still
-- uses it.  This fixture creates only TEST identities and leaves no rows.

begin;

update public.settings_defaults
set candidate_app_environment='TEST',
    candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
      ||'{"candidate_app_reads":true,"candidate_app_writes":true}'::jsonb
where id=1;

create function pg_temp.candidate_receipt_prepare(
  p_session_id uuid,
  p_workflow_id uuid,
  p_generation integer,
  p_source_component_id uuid,
  p_expense_category text,
  p_idempotency_key text,
  p_now_utc timestamptz
)
returns jsonb
language sql
as $function$
  select public.candidate_component_prepare_atomic_v1(
    p_session_id,
    'TEST',
    p_workflow_id,
    p_generation,
    jsonb_build_object(
      'component_kind','EXPENSE_EVIDENCE',
      'document_role','SOURCE_EVIDENCE',
      'expense_category',p_expense_category,
      'source_component_id',p_source_component_id,
      'storage_key','verification/receipt-reuse/reupload/'
        ||pg_catalog.encode(extensions.digest(
          pg_catalog.convert_to(p_idempotency_key,'UTF8'),'sha256'
        ),'hex')||'.jpg',
      'media_type','image/jpeg',
      'byte_size',321,
      'source_content_sha256_hex',(
        select pg_catalog.encode(component.source_content_sha256,'hex')
        from public.candidate_submission_components component
        where component.id=p_source_component_id
      ),
      'expected_source_content_sha256_hex',(
        select pg_catalog.encode(component.source_content_sha256,'hex')
        from public.candidate_submission_components component
        where component.id=p_source_component_id
      )
    ),
    p_idempotency_key,
    p_now_utc
  );
$function$;

create function pg_temp.expect_candidate_receipt_live_block(
  p_session_id uuid,
  p_workflow_id uuid,
  p_generation integer,
  p_source_component_id uuid,
  p_expense_category text,
  p_idempotency_key text,
  p_now_utc timestamptz
)
returns void
language plpgsql
as $function$
begin
  begin
    perform pg_temp.candidate_receipt_prepare(
      p_session_id,p_workflow_id,p_generation,p_source_component_id,
      p_expense_category,p_idempotency_key,p_now_utc
    );
    raise exception 'Live Candidate receipt was unexpectedly reusable';
  exception when unique_violation then
    if sqlerrm<>'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED' then
      raise;
    end if;
  end;
end;
$function$;

create function pg_temp.expect_candidate_receipt_identity_block(
  p_session_id uuid,
  p_workflow_id uuid,
  p_generation integer,
  p_source_component_id uuid,
  p_expense_category text,
  p_media_type text,
  p_byte_size bigint,
  p_source_digest_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz
)
returns void
language plpgsql
as $function$
begin
  begin
    perform public.candidate_component_prepare_atomic_v1(
      p_session_id,
      'TEST',
      p_workflow_id,
      p_generation,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category',p_expense_category,
        'source_component_id',p_source_component_id,
        'storage_key','verification/receipt-reuse/reupload/'
          ||pg_catalog.encode(extensions.digest(
            pg_catalog.convert_to(p_idempotency_key,'UTF8'),'sha256'
          ),'hex')||'.jpg',
        'media_type',p_media_type,
        'byte_size',p_byte_size,
        'source_content_sha256_hex',p_source_digest_hex,
        'expected_source_content_sha256_hex',p_source_digest_hex
      ),
      p_idempotency_key,
      p_now_utc
    );
    raise exception 'Mismatched Candidate receipt identity was unexpectedly reusable';
  exception when invalid_authorization_specification then
    if sqlerrm<>'CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED' then
      raise;
    end if;
  end;
end;
$function$;

create function pg_temp.candidate_receipt_complete(
  p_session_id uuid,
  p_workflow_id uuid,
  p_generation integer,
  p_component_id uuid,
  p_source_digest_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz
)
returns jsonb
language sql
as $function$
  select public.candidate_workflow_transition_atomic_v1(
    p_session_id,
    'TEST',
    p_workflow_id,
    'COMPONENT_COMPLETE',
    p_generation,
    jsonb_build_object(
      'component_id',p_component_id,
      'source_content_sha256_hex',p_source_digest_hex,
      'verified_media_type','image/jpeg',
      'verified_byte_size',321
    ),
    p_idempotency_key,
    p_now_utc
  );
$function$;

do $verification$
declare
  v_now timestamptz:=pg_catalog.transaction_timestamp();
  v_client uuid:=pg_catalog.gen_random_uuid();
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_contract uuid:=pg_catalog.gen_random_uuid();
  v_contract_week uuid:=pg_catalog.gen_random_uuid();
  v_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_daily_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_account uuid:=pg_catalog.gen_random_uuid();
  v_session uuid:=pg_catalog.gen_random_uuid();
  v_target_workflow uuid:=pg_catalog.gen_random_uuid();
  v_source_workflow uuid:=pg_catalog.gen_random_uuid();
  v_daily_workflow uuid:=pg_catalog.gen_random_uuid();
  v_source_root uuid:=pg_catalog.gen_random_uuid();
  v_approved_sibling_root uuid:=pg_catalog.gen_random_uuid();
  v_source_expense uuid:=pg_catalog.gen_random_uuid();
  v_approved_sibling_expense uuid:=pg_catalog.gen_random_uuid();
  v_missing_workflow uuid:=pg_catalog.gen_random_uuid();
  v_missing_root uuid:=pg_catalog.gen_random_uuid();
  v_terminal_workflow uuid;
  v_terminal_root uuid;
  v_same_workflow_earlier_root uuid:=pg_catalog.gen_random_uuid();
  v_same_workflow_current_root uuid:=pg_catalog.gen_random_uuid();
  v_terminal_state text;
  v_result jsonb;
  v_replay jsonb;
  v_component_id uuid;
  v_direct_component_id uuid;
  v_sibling_before jsonb;
  v_direct_digest_hex text:=pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to('direct receipt exact bytes','UTF8'),'sha256'
  ),'hex');
  v_direct_storage_key text:='verification/receipt-reuse/direct-root.jpg';
  v_source_digest_hex text;
  v_source_storage_key text;
  v_recovery_storage_key text;
  v_lineage_plan json;
  v_constraint_name text;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate receipt reuse verification client');

  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(
    pg_catalog.gen_random_uuid(),v_client,current_date-7,'ELECTRONIC',
    extract(dow from current_date)::integer
  );

  insert into public.candidates(id,email,active,key_norm,display_name)
  values(
    v_candidate,
    'receipt-reuse-'||v_candidate::text||'@example.test',
    true,
    'GCK-RECEIPT-REUSE-'||pg_catalog.replace(v_candidate::text,'-',''),
    'Receipt Reuse Verification Candidate'
  );

  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC'
  );

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,sheet_scope,line_type,
    submission_mode,r2_nurse_key,r2_auth_key,is_current
  ) values(
    v_timesheet,'RECEIPT-REUSE-'||pg_catalog.replace(v_timesheet::text,'-',''),
    'GCK-RECEIPT-REUSE-'||pg_catalog.replace(v_candidate::text,'-',''),
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE',
    v_contract,current_date,'WEEKLY','HOURS','ELECTRONIC',
    'verification/receipt-reuse/candidate','verification/receipt-reuse/manager',
    false
  );

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,week_ending_date,sheet_scope,line_type,submission_mode,
    is_current
  ) values(
    v_daily_timesheet,
    'RECEIPT-REUSE-DAILY-'||pg_catalog.replace(v_daily_timesheet::text,'-',''),
    'GCK-RECEIPT-REUSE-'||pg_catalog.replace(v_candidate::text,'-',''),
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE',
    current_date,'DAILY','HOURS','MANUAL',false
  );

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(
    v_contract_week,v_contract,current_date,919,'OPEN','ELECTRONIC',v_timesheet
  );

  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(
    v_account,'TEST','receipt-reuse-'||v_candidate::text||'@example.test','ACTIVE'
  );

  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(pg_catalog.convert_to(v_session::text,'UTF8'),'sha256'),
    v_now+interval '30 days',v_now+interval '90 days'
  );

  -- The target is the Candidate's open worked-Timesheet workflow.  The
  -- source is a combined hours-and-expense workflow for the same Contract
  -- and week.  That is a valid mixed-record shape and does not weaken the
  -- one-active-expense-workflow constraint merely for this verification.
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,policy_snapshot_json,
    input_snapshot_json,idempotency_key
  ) values(
    v_target_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY',
    'ELECTRONIC','WORKER_DRAFT',1,v_contract,v_contract_week,v_timesheet,
    v_timesheet,current_date,'{}','{}','receipt-reuse:target'
  ),(
    v_source_workflow,'TEST',v_account,v_candidate,'CONTRACT_COMBINED','WEEKLY',
    'ELECTRONIC','WORKER_DRAFT',1,v_contract,v_contract_week,v_timesheet,
    v_timesheet,current_date,'{}','{}','receipt-reuse:source'
  );

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,anchor_timesheet_id,target_timesheet_id,work_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key
  ) values(
    v_daily_workflow,'TEST',v_account,v_candidate,'DAILY','DAILY','PHONE',
    'WORKER_DRAFT',1,v_daily_timesheet,v_daily_timesheet,current_date,
    '{}','{}','receipt-reuse:daily-target'
  );

  -- Daily Timesheets remain hours-only even if a crafted client calls the
  -- generic component endpoint directly.
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_daily_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','OTHER',
        'media_type','image/jpeg',
        'byte_size',321
      ),
      'receipt-reuse:block-daily-expense',v_now
    );
    raise exception 'Daily Timesheet unexpectedly accepted expense evidence';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_COMPONENT_TYPE_INVALID' then
      raise;
    end if;
  end;

  -- First-use receipt uploads are exact before any R2 completion.  Missing or
  -- unbound digests fail closed, while a lost prepare response returns the
  -- one original PENDING owner and its original storage key.
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','TRAVEL',
        'storage_key','verification/receipt-reuse/direct-missing-digest.jpg',
        'media_type','image/jpeg','byte_size',321
      ),
      'receipt-reuse:direct-missing-digest',v_now
    );
    raise exception 'Direct receipt without an expected digest was accepted';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_COMPONENT_DIGEST_INVALID' then
      raise;
    end if;
  end;
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','TRAVEL',
        'storage_key','verification/receipt-reuse/direct-unbound-source.jpg',
        'media_type','image/jpeg','byte_size',321,
        'source_content_sha256_hex',v_direct_digest_hex,
        'expected_source_content_sha256_hex',v_direct_digest_hex
      ),
      'receipt-reuse:direct-unbound-source',v_now
    );
    raise exception 'Direct receipt accepted a source digest without its source';
  exception when invalid_authorization_specification then
    if sqlerrm<>'CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED' then
      raise;
    end if;
  end;
  v_result:=public.candidate_component_prepare_atomic_v1(
    v_session,'TEST',v_target_workflow,1,
    jsonb_build_object(
      'component_kind','EXPENSE_EVIDENCE',
      'document_role','SOURCE_EVIDENCE',
      'expense_category','TRAVEL',
      'storage_key',v_direct_storage_key,
      'media_type','image/jpeg','byte_size',321,
      'expected_source_content_sha256_hex',v_direct_digest_hex
    ),
    'receipt-reuse:direct-exact',v_now
  );
  v_direct_component_id:=nullif(v_result->>'component_id','')::uuid;
  if v_direct_component_id is null
     or v_result->>'state' is distinct from 'PENDING'
     or (select source_component_id from public.candidate_submission_components
         where id=v_direct_component_id) is not null
     or (select source_content_sha256 from public.candidate_submission_components
         where id=v_direct_component_id) is not null
     or (select expected_source_content_sha256 from public.candidate_submission_components
         where id=v_direct_component_id) is distinct from decode(v_direct_digest_hex,'hex') then
    raise exception 'Direct receipt was not reserved with exact expected bytes: %',v_result;
  end if;
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','TRAVEL',
        'storage_key','verification/receipt-reuse/direct-competing-key.jpg',
        'media_type','image/jpeg','byte_size',321,
        'expected_source_content_sha256_hex',v_direct_digest_hex
      ),
      'receipt-reuse:direct-competing-key',v_now+interval '0.01 seconds'
    );
    raise exception 'A second direct PREPARE reserved the same receipt bytes';
  exception when unique_violation then
    if sqlerrm<>'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED' then
      raise;
    end if;
  end;
  if exists(
    select 1
    from public.candidate_submission_components component
    where component.upload_idempotency_key='receipt-reuse:direct-competing-key'
  ) or exists(
    select 1
    from public.audit_events event_row
    where event_row.object_type='candidate_workflow_mutation_receipt'
      and event_row.object_id_text=v_target_workflow::text
      and event_row.correlation_id='receipt-reuse:direct-competing-key'
  ) then
    raise exception 'The rejected direct PREPARE left an upload admission behind';
  end if;
  delete from public.audit_events
  where object_type='candidate_workflow_mutation_receipt'
    and object_id_text=v_target_workflow::text
    and correlation_id='receipt-reuse:direct-exact';
  v_replay:=public.candidate_component_prepare_atomic_v1(
    v_session,'TEST',v_target_workflow,1,
    jsonb_build_object(
      'component_kind','EXPENSE_EVIDENCE',
      'document_role','SOURCE_EVIDENCE',
      'expense_category','TRAVEL',
      'storage_key','verification/receipt-reuse/direct-lost-response-key.jpg',
      'media_type','image/jpeg','byte_size',321,
      'expected_source_content_sha256_hex',v_direct_digest_hex
    ),
    'receipt-reuse:direct-exact',v_now+interval '0.1 seconds'
  );
  if not coalesce((v_replay->>'idempotent_replay')::boolean,false)
     or nullif(v_replay->>'component_id','')::uuid is distinct from v_direct_component_id
     or v_replay->>'storage_key' is distinct from v_direct_storage_key
     or (select count(*) from public.candidate_submission_components component
         where component.workflow_id=v_target_workflow
           and component.upload_idempotency_key='receipt-reuse:direct-exact')<>1 then
    raise exception 'Direct receipt lost-response replay created a second owner: %',v_replay;
  end if;
  delete from public.audit_events
  where object_type='candidate_workflow_mutation_receipt'
    and object_id_text=v_target_workflow::text
    and correlation_id='receipt-reuse:direct-exact';
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','TRAVEL',
        'storage_key',v_direct_storage_key,
        'media_type','image/jpeg','byte_size',321,
        'expected_source_content_sha256_hex',pg_catalog.repeat('0',64)
      ),
      'receipt-reuse:direct-exact',v_now+interval '0.2 seconds'
    );
    raise exception 'Direct receipt replay accepted different expected bytes';
  exception when unique_violation then
    if sqlerrm<>'CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT' then
      raise;
    end if;
  end;
  v_replay:=pg_temp.candidate_receipt_complete(
    v_session,v_target_workflow,1,v_direct_component_id,v_direct_digest_hex,
    'receipt-reuse:direct-complete',v_now+interval '0.3 seconds'
  );
  if (select state from public.candidate_submission_components
      where id=v_direct_component_id) is distinct from 'IMMUTABLE'
     or (select source_content_sha256 from public.candidate_submission_components
         where id=v_direct_component_id) is distinct from decode(v_direct_digest_hex,'hex')
     or (select expected_source_content_sha256 from public.candidate_submission_components
         where id=v_direct_component_id) is distinct from decode(v_direct_digest_hex,'hex') then
    raise exception 'Direct receipt did not complete against its expected bytes: %',v_replay;
  end if;
  update public.candidate_submission_components
  set state='SUPERSEDED',superseded_at_utc=v_now
  where id=v_direct_component_id;
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','TRAVEL',
        'storage_key','verification/receipt-reuse/direct-ended-root.jpg',
        'media_type','image/jpeg','byte_size',321,
        'expected_source_content_sha256_hex',v_direct_digest_hex
      ),
      'receipt-reuse:direct-ended-root',v_now+interval '0.4 seconds'
    );
    raise exception 'Direct PREPARE bypassed an existing completed receipt root';
  exception when unique_violation then
    if sqlerrm<>'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED' then
      raise;
    end if;
  end;
  if exists(
    select 1
    from public.candidate_submission_components component
    where component.upload_idempotency_key='receipt-reuse:direct-ended-root'
  ) then
    raise exception 'The rejected completed-root PREPARE left an upload admission behind';
  end if;

  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,expense_category,document_role,state,storage_key,
    media_type,byte_size,source_content_sha256,immutable_at_utc,
    final_signed_storage_key,final_signed_content_sha256,
    final_signed_media_type,final_signed_byte_size,final_signed_page_count,
    final_signed_render_input_sha256,final_signed_renderer_contract_version,
    final_signed_renderer_receipt_json,final_signed_generated_at_utc,
    final_signed_render_state
  ) values(
    v_source_root,v_source_workflow,1,1,v_timesheet,'EXPENSE_EVIDENCE','OTHER',
    'SOURCE_EVIDENCE','IMMUTABLE',
    'verification/receipt-reuse/'||v_source_root::text||'.jpg','image/jpeg',321,
    extensions.digest(pg_catalog.convert_to(v_source_root::text,'UTF8'),'sha256'),v_now,
    'verification/receipt-reuse/final-'||v_source_root::text||'.pdf',
    extensions.digest(
      pg_catalog.convert_to('final-'||v_source_root::text,'UTF8'),'sha256'
    ),'application/pdf',1024,1,
    extensions.digest(
      pg_catalog.convert_to('render-'||v_source_root::text,'UTF8'),'sha256'
    ),'EXPENSE_EVIDENCE_PDF_V1','{}',v_now,'READY'
  );

  insert into public.candidate_expense_components(
    expense_component_id,workflow_id,workflow_generation,expense_category,
    owning_timesheet_id,amount,mileage_units,lifecycle_state,
    manager_approval_state,agency_authorisation_state
  ) values(
    v_source_expense,v_source_workflow,1,'OTHER',v_timesheet,5.67,0,
    'DRAFT','NOT_REQUESTED','NOT_AUTHORISED'
  );

  select pg_catalog.encode(source_content_sha256,'hex'),storage_key
  into strict v_source_digest_hex,v_source_storage_key
  from public.candidate_submission_components
  where id=v_source_root;

  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','OTHER',
        'source_component_id',v_source_root,
        'storage_key','verification/receipt-reuse/reupload/missing-expected.jpg',
        'media_type','image/jpeg',
        'byte_size',321,
        'source_content_sha256_hex',v_source_digest_hex
      ),
      'receipt-reuse:block-missing-expected',v_now
    );
    raise exception 'Receipt recovery without an expected digest was accepted';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_COMPONENT_DIGEST_INVALID' then
      raise;
    end if;
  end;
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','OTHER',
        'source_component_id',v_source_root,
        'media_type','image/jpeg',
        'byte_size',321,
        'source_content_sha256_hex',v_source_digest_hex,
        'expected_source_content_sha256_hex',v_source_digest_hex
      ),
      'receipt-reuse:block-missing-storage',v_now
    );
    raise exception 'Receipt recovery without a fresh storage key was accepted';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_COMPONENT_STORAGE_KEY_INVALID' then
      raise;
    end if;
  end;

  -- The database itself, not only the calling service, binds a reuse request
  -- to the exact immutable bytes selected by the Candidate.
  perform pg_temp.expect_candidate_receipt_identity_block(
    v_session,v_target_workflow,1,v_source_root,'OTHER','image/png',321,
    v_source_digest_hex,'receipt-reuse:block-wrong-media',v_now
  );
  perform pg_temp.expect_candidate_receipt_identity_block(
    v_session,v_target_workflow,1,v_source_root,'OTHER','image/jpeg',322,
    v_source_digest_hex,'receipt-reuse:block-wrong-size',v_now
  );
  perform pg_temp.expect_candidate_receipt_identity_block(
    v_session,v_target_workflow,1,v_source_root,'OTHER','image/jpeg',321,
    pg_catalog.repeat('0',64),'receipt-reuse:block-wrong-digest',v_now
  );
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','OTHER',
        'source_component_id',v_source_root,
        'storage_key','verification/receipt-reuse/reupload/wrong-expected.jpg',
        'media_type','image/jpeg',
        'byte_size',321,
        'source_content_sha256_hex',v_source_digest_hex,
        'expected_source_content_sha256_hex',pg_catalog.repeat('0',64)
      ),
      'receipt-reuse:block-wrong-expected',v_now
    );
    raise exception 'Mismatched expected Candidate receipt digest was accepted';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_COMPONENT_DIGEST_MISMATCH' then
      raise;
    end if;
  end;

  -- A receipt in each live business state remains unavailable.
  perform pg_temp.expect_candidate_receipt_live_block(
    v_session,v_target_workflow,1,v_source_root,'OTHER',
    'receipt-reuse:block-draft',v_now
  );

  update public.candidate_submission_workflows
  set state='WORKER_SUBMITTED',worker_submitted_at_utc=v_now,updated_at_utc=v_now
  where id=v_source_workflow;
  update public.candidate_expense_components
  set lifecycle_state='SUBMITTED',manager_approval_state='PENDING',
      submitted_at_utc=v_now,updated_at_utc=v_now
  where expense_component_id=v_source_expense;
  perform pg_temp.expect_candidate_receipt_live_block(
    v_session,v_target_workflow,1,v_source_root,'OTHER',
    'receipt-reuse:block-submitted',v_now+interval '1 second'
  );

  update public.candidate_submission_workflows
  set state='FINALISED',manager_approved_at_utc=v_now,finalised_at_utc=v_now,
      updated_at_utc=v_now
  where id=v_source_workflow;
  update public.candidate_expense_components
  set lifecycle_state='MANAGER_APPROVED',manager_approval_state='APPROVED',
      manager_approved_at_utc=v_now,updated_at_utc=v_now
  where expense_component_id=v_source_expense;
  perform pg_temp.expect_candidate_receipt_live_block(
    v_session,v_target_workflow,1,v_source_root,'OTHER',
    'receipt-reuse:block-approved',v_now+interval '2 seconds'
  );

  -- Ending only Other releases that exact receipt.  The Accommodation
  -- sibling remains approved and bit-for-bit unchanged.
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,expense_category,document_role,state,storage_key,
    media_type,byte_size,source_content_sha256,immutable_at_utc,
    manager_approved_at_utc
  ) values(
    v_approved_sibling_root,v_source_workflow,1,2,v_timesheet,
    'EXPENSE_EVIDENCE','ACCOMMODATION','SOURCE_EVIDENCE','IMMUTABLE',
    'verification/receipt-reuse/'||v_approved_sibling_root::text||'.jpg',
    'image/jpeg',654,
    extensions.digest(
      pg_catalog.convert_to(v_approved_sibling_root::text,'UTF8'),'sha256'
    ),v_now,v_now
  );
  insert into public.candidate_expense_components(
    expense_component_id,workflow_id,workflow_generation,expense_category,
    owning_timesheet_id,amount,mileage_units,lifecycle_state,
    manager_approval_state,agency_authorisation_state,submitted_at_utc,
    manager_approved_at_utc
  ) values(
    v_approved_sibling_expense,v_source_workflow,1,'ACCOMMODATION',v_timesheet,
    25,0,'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',v_now,v_now
  );
  update public.candidate_expense_components
  set lifecycle_state='WITHDRAWN',removed_at_utc=v_now,updated_at_utc=v_now
  where expense_component_id=v_source_expense;
  select pg_catalog.to_jsonb(expense_row) into strict v_sibling_before
  from public.candidate_expense_components expense_row
  where expense_row.expense_component_id=v_approved_sibling_expense;

  v_result:=pg_temp.candidate_receipt_prepare(
    v_session,v_target_workflow,1,v_source_root,'OTHER',
    'receipt-reuse:ended-category',v_now+interval '3 seconds'
  );
  v_component_id:=nullif(v_result->>'component_id','')::uuid;
  select storage_key into strict v_recovery_storage_key
  from public.candidate_submission_components
  where id=v_component_id;
  if coalesce((v_result->>'ok')::boolean,false)=false
     or v_component_id is null
     or (select source_component_id
         from public.candidate_submission_components
         where id=v_component_id) is distinct from v_source_root
     or (select state from public.candidate_submission_components
         where id=v_component_id) is distinct from 'PENDING'
     or (select source_content_sha256 from public.candidate_submission_components
         where id=v_component_id) is not null
     or (select expected_source_content_sha256
         from public.candidate_submission_components
         where id=v_component_id) is distinct from decode(v_source_digest_hex,'hex')
     or v_recovery_storage_key is not distinct from v_source_storage_key
     or nullif(btrim(v_recovery_storage_key),'') is null
     or (select manager_approved_at_utc
         from public.candidate_submission_components
         where id=v_component_id) is not null
     or (select pg_catalog.to_jsonb(expense_row)
         from public.candidate_expense_components expense_row
         where expense_row.expense_component_id=v_approved_sibling_expense)
        is distinct from v_sibling_before then
    raise exception 'Ended category reuse changed the approved sibling: %',v_result;
  end if;

  begin
    update public.candidate_submission_components
    set expected_source_content_sha256=extensions.digest(
      pg_catalog.convert_to('different expected receipt bytes','UTF8'),'sha256'
    )
    where id=v_component_id;
    raise exception 'Pending receipt recovery identity was mutable';
  exception when object_not_in_prerequisite_state then
    if sqlerrm<>'CANDIDATE_COMPONENT_IMMUTABLE' then
      raise;
    end if;
  end;

  begin
    insert into public.candidate_submission_components(
      workflow_id,workflow_generation,component_no,timesheet_id,
      component_kind,expense_category,document_role,state,source_component_id,
      storage_key,media_type,byte_size,upload_idempotency_key,
      expected_source_content_sha256,created_at_utc
    ) values(
      v_target_workflow,1,9999,v_timesheet,'EXPENSE_EVIDENCE','OTHER',
      'SOURCE_EVIDENCE','PENDING',v_source_root,v_recovery_storage_key,
      'image/jpeg',321,'receipt-reuse:duplicate-physical-owner',
      decode(v_source_digest_hex,'hex'),v_now
    );
    raise exception 'A second physical owner accepted the recovery storage key';
  exception when unique_violation then
    get stacked diagnostics v_constraint_name=constraint_name;
    if v_constraint_name<>'candidate_submission_components_physical_storage_key_uq' then
      raise;
    end if;
  end;

  begin
    perform pg_temp.candidate_receipt_complete(
      v_session,v_target_workflow,1,v_component_id,pg_catalog.repeat('0',64),
      'receipt-reuse:complete-wrong-digest',v_now+interval '3.5 seconds'
    );
    raise exception 'Receipt recovery completed with different bytes';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_COMPONENT_DIGEST_MISMATCH' then
      raise;
    end if;
  end;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_target_workflow,'COMPONENT_COMPLETE',1,
      jsonb_build_object(
        'component_id',v_component_id,
        'source_content_sha256_hex',v_source_digest_hex,
        'verified_media_type','image/jpeg',
        'verified_byte_size',322
      ),
      'receipt-reuse:complete-wrong-size',v_now+interval '3.51 seconds'
    );
    raise exception 'Receipt recovery completed with changed size metadata';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_COMPONENT_MEDIA_INVALID' then
      raise;
    end if;
  end;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_target_workflow,'COMPONENT_COMPLETE',1,
      jsonb_build_object(
        'component_id',v_component_id,
        'source_content_sha256_hex',v_source_digest_hex,
        'verified_media_type','image/png',
        'verified_byte_size',321
      ),
      'receipt-reuse:complete-wrong-media',v_now+interval '3.52 seconds'
    );
    raise exception 'Receipt recovery completed with changed media metadata';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_COMPONENT_MEDIA_INVALID' then
      raise;
    end if;
  end;
  v_replay:=pg_temp.candidate_receipt_complete(
    v_session,v_target_workflow,1,v_component_id,v_source_digest_hex,
    'receipt-reuse:complete-ended-category',v_now+interval '3.6 seconds'
  );
  if coalesce((v_replay->>'ok')::boolean,false)=false
     or (select state from public.candidate_submission_components
         where id=v_component_id) is distinct from 'IMMUTABLE'
     or (select source_content_sha256
         from public.candidate_submission_components
         where id=v_component_id) is distinct from decode(v_source_digest_hex,'hex')
     or (select expected_source_content_sha256
         from public.candidate_submission_components
         where id=v_component_id) is distinct from decode(v_source_digest_hex,'hex')
     or (select storage_key from public.candidate_submission_components
         where id=v_component_id) is distinct from v_recovery_storage_key then
    raise exception 'Exact receipt re-upload did not complete immutably: %',v_replay;
  end if;

  -- Older releases could leave the prepared component without its audit
  -- receipt.  Exact recovery accepts only the same source identity and digest.
  delete from public.audit_events
  where object_type='candidate_workflow_mutation_receipt'
    and object_id_text=v_target_workflow::text
    and correlation_id='receipt-reuse:ended-category';
  v_replay:=pg_temp.candidate_receipt_prepare(
    v_session,v_target_workflow,1,v_source_root,'OTHER',
    'receipt-reuse:ended-category',v_now+interval '4 seconds'
  );
  if not coalesce((v_replay->>'idempotent_replay')::boolean,false)
     or nullif(v_replay->>'component_id','')::uuid is distinct from v_component_id then
    raise exception 'Receipt reuse legacy recovery was not exact: %, %',
      v_result,v_replay;
  end if;
  delete from public.audit_events
  where object_type='candidate_workflow_mutation_receipt'
    and object_id_text=v_target_workflow::text
    and correlation_id='receipt-reuse:ended-category';
  begin
    perform public.candidate_component_prepare_atomic_v1(
      v_session,'TEST',v_target_workflow,1,
      jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE',
        'document_role','SOURCE_EVIDENCE',
        'expense_category','OTHER',
        'source_component_id',v_source_root,
        'storage_key',v_recovery_storage_key,
        'media_type','image/jpeg',
        'byte_size',321,
        'source_content_sha256_hex',pg_catalog.repeat('0',64),
        'expected_source_content_sha256_hex',pg_catalog.repeat('0',64)
      ),
      'receipt-reuse:ended-category',v_now+interval '4 seconds'
    );
    raise exception 'Legacy recovery accepted a changed receipt digest';
  exception when unique_violation then
    if sqlerrm<>'CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT' then
      raise;
    end if;
  end;

  -- A lost-response replay returns the same physical recovery component and
  -- creates no duplicate reservation or upload owner.
  v_replay:=pg_temp.candidate_receipt_prepare(
    v_session,v_target_workflow,1,v_source_root,'OTHER',
    'receipt-reuse:ended-category',v_now+interval '4 seconds'
  );
  if not coalesce((v_replay->>'idempotent_replay')::boolean,false)
     or nullif(v_replay->>'component_id','')::uuid is distinct from v_component_id
     or v_replay->>'storage_key' is distinct from v_recovery_storage_key
     or v_replay->>'component_kind' is distinct from 'EXPENSE_EVIDENCE'
     or v_replay->>'document_role' is distinct from 'SOURCE_EVIDENCE'
     or v_replay->>'expense_category' is distinct from 'OTHER'
     or (select count(*) from public.candidate_submission_components component
         where component.workflow_id=v_target_workflow
           and component.upload_idempotency_key='receipt-reuse:ended-category')<>1 then
    raise exception 'Receipt reuse replay was not exactly idempotent: %, %',
      v_result,v_replay;
  end if;
  update public.candidate_submission_components
  set state='SUPERSEDED',superseded_at_utc=v_now
  where id=v_component_id;

  -- Materialised Timesheet evidence is a second current-use authority.  It
  -- fails closed even though the category ledger says the old claim ended.
  perform pg_catalog.set_config(
    'cloudtms.candidate_expense_summary_suppressed','true',true
  );
  insert into public.timesheet_evidence(
    timesheet_id,kind,display_name,storage_key,processing_state,document_role,
    candidate_component_id
  ) values(
    v_timesheet,'OTHER','Receipt reuse verification',
    'verification/receipt-reuse/final-'||v_source_root::text||'.pdf',
    'READY','SOURCE_EVIDENCE',v_source_root
  );
  perform pg_temp.expect_candidate_receipt_live_block(
    v_session,v_target_workflow,1,v_source_root,'OTHER',
    'receipt-reuse:block-materialised',v_now+interval '5 seconds'
  );
  update public.timesheet_evidence
  set processing_state='SUPERSEDED'
  where candidate_component_id=v_source_root;
  perform pg_catalog.set_config(
    'cloudtms.candidate_expense_summary_suppressed','false',true
  );

  -- More than 100 ended lineage rows must not cause a false duplicate or a
  -- false release.  This guards against restoring the former bounded scan.
  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,expense_category,document_role,state,source_component_id,
    storage_key,media_type,byte_size,source_content_sha256,immutable_at_utc,
    superseded_at_utc
  )
  select
    v_source_workflow,1,100+series.value,v_timesheet,
    'EXPENSE_EVIDENCE','OTHER','SOURCE_EVIDENCE','SUPERSEDED',v_source_root,
    'verification/receipt-reuse/history-'||series.value::text||'.jpg',
    'image/jpeg',321,
    extensions.digest(
      pg_catalog.convert_to('history-'||series.value::text,'UTF8'),'sha256'
    ),v_now,v_now
  from pg_catalog.generate_series(1,150) as series(value);
  perform pg_catalog.set_config('enable_seqscan','off',true);
  execute $explain$
    explain (format json)
    select 1
    from public.candidate_submission_components component
    where component.source_component_id=$1
      and component.workflow_id=$2
      and component.workflow_generation=1
      and component.expense_category='OTHER'
      and component.state<>'ABANDONED'
  $explain$ into v_lineage_plan using v_source_root,v_source_workflow;
  perform pg_catalog.set_config('enable_seqscan','on',true);
  if pg_catalog.strpos(
       pg_catalog.lower(v_lineage_plan::text),
       'candidate_submission_components_source_lineage_idx'
     )=0 then
    raise exception 'Candidate receipt lineage index does not support its exact lookup';
  end if;
  v_result:=pg_temp.candidate_receipt_prepare(
    v_session,v_target_workflow,1,v_source_root,'OTHER',
    'receipt-reuse:over-100-history',v_now+interval '6 seconds'
  );
  v_component_id:=nullif(v_result->>'component_id','')::uuid;
  if v_component_id is null then
    raise exception 'Ended receipt with more than 100 history rows was not reusable';
  end if;
  update public.candidate_submission_components
  set state='SUPERSEDED',superseded_at_utc=v_now
  where id=v_component_id;

  -- A legacy current workflow with no category ledger fails closed.  The
  -- ended dependent row supplies an ended fact so this specifically proves
  -- the missing-ledger current-use fallback rather than the earlier gate.
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,policy_snapshot_json,
    input_snapshot_json,idempotency_key
  ) values(
    v_missing_workflow,'TEST',v_account,v_candidate,'CONTRACT_COMBINED','WEEKLY',
    'ELECTRONIC','FINALISED',1,v_contract,v_contract_week,v_timesheet,
    v_timesheet,current_date,'{}','{}','receipt-reuse:missing-ledger-source'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,expense_category,document_role,state,storage_key,
    media_type,byte_size,source_content_sha256,immutable_at_utc
  ) values(
    v_missing_root,v_missing_workflow,1,1,v_timesheet,
    'EXPENSE_EVIDENCE','TRAVEL','SOURCE_EVIDENCE','IMMUTABLE',
    'verification/receipt-reuse/'||v_missing_root::text||'.jpg','image/jpeg',321,
    extensions.digest(pg_catalog.convert_to(v_missing_root::text,'UTF8'),'sha256'),v_now
  );
  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,expense_category,document_role,state,source_component_id,
    storage_key,media_type,byte_size,source_content_sha256,immutable_at_utc,
    superseded_at_utc
  ) values(
    v_missing_workflow,1,2,v_timesheet,'EXPENSE_EVIDENCE','TRAVEL',
    'SOURCE_EVIDENCE','SUPERSEDED',v_missing_root,
    'verification/receipt-reuse/missing-ledger-ended.jpg','image/jpeg',321,
    extensions.digest(pg_catalog.convert_to('missing-ledger-ended','UTF8'),'sha256'),
    v_now,v_now
  );
  perform pg_temp.expect_candidate_receipt_live_block(
    v_session,v_target_workflow,1,v_missing_root,'TRAVEL',
    'receipt-reuse:block-missing-ledger',v_now+interval '7 seconds'
  );

  -- Every whole-workflow terminal state releases an otherwise-unused root.
  foreach v_terminal_state in array array[
    'CANCELLED','REJECTED','REFUSED','EXPIRED','SUPERSEDED'
  ] loop
    v_terminal_workflow:=pg_catalog.gen_random_uuid();
    v_terminal_root:=pg_catalog.gen_random_uuid();
    insert into public.candidate_submission_workflows(
      id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
      generation,contract_id,contract_week_id,anchor_timesheet_id,
      target_timesheet_id,week_ending_date,policy_snapshot_json,
      input_snapshot_json,idempotency_key,cancelled_at_utc
    ) values(
      v_terminal_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE',
      'WEEKLY','ELECTRONIC',v_terminal_state,1,v_contract,v_contract_week,
      v_timesheet,v_timesheet,current_date,'{}','{}',
      'receipt-reuse:terminal:'||pg_catalog.lower(v_terminal_state),
      case when v_terminal_state='CANCELLED' then v_now else null end
    );
    insert into public.candidate_submission_components(
      id,workflow_id,workflow_generation,component_no,timesheet_id,
      component_kind,expense_category,document_role,state,storage_key,
      media_type,byte_size,source_content_sha256,immutable_at_utc
    ) values(
      v_terminal_root,v_terminal_workflow,1,1,v_timesheet,
      'EXPENSE_EVIDENCE','MILEAGE','SOURCE_EVIDENCE',
      case when v_terminal_state='CANCELLED' then 'ABANDONED' else 'IMMUTABLE' end,
      'verification/receipt-reuse/'||v_terminal_root::text||'.jpg',
      'image/jpeg',321,
      extensions.digest(
        pg_catalog.convert_to(v_terminal_root::text,'UTF8'),'sha256'
      ),v_now
    );
    v_result:=pg_temp.candidate_receipt_prepare(
      v_session,v_target_workflow,1,v_terminal_root,'MILEAGE',
      'receipt-reuse:terminal-result:'||pg_catalog.lower(v_terminal_state),
      v_now+interval '8 seconds'
    );
    v_component_id:=nullif(v_result->>'component_id','')::uuid;
    if v_component_id is null
       or (select source_component_id from public.candidate_submission_components
           where id=v_component_id) is distinct from v_terminal_root then
      raise exception 'Terminal workflow did not release receipt: %, %',
        v_terminal_state,v_result;
    end if;
    update public.candidate_submission_components
    set state='SUPERSEDED',superseded_at_utc=v_now
    where id=v_component_id;
  end loop;

  -- Carry-forward inside one workflow is generation-aware.  An immutable
  -- root from an earlier generation is allowed; a root created in the
  -- current generation cannot be selected as historical evidence.
  update public.candidate_submission_workflows
  set generation=3,updated_at_utc=v_now
  where id=v_target_workflow;
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,expense_category,document_role,state,storage_key,
    media_type,byte_size,source_content_sha256,immutable_at_utc
  ) values(
    v_same_workflow_earlier_root,v_target_workflow,2,1,v_timesheet,
    'EXPENSE_EVIDENCE','ACCOMMODATION','SOURCE_EVIDENCE','IMMUTABLE',
    'verification/receipt-reuse/'||v_same_workflow_earlier_root::text||'.jpg',
    'image/jpeg',321,
    extensions.digest(
      pg_catalog.convert_to(v_same_workflow_earlier_root::text,'UTF8'),'sha256'
    ),v_now
  );
  v_result:=pg_temp.candidate_receipt_prepare(
    v_session,v_target_workflow,3,v_same_workflow_earlier_root,'ACCOMMODATION',
    'receipt-reuse:same-workflow-earlier',v_now+interval '9 seconds'
  );
  v_component_id:=nullif(v_result->>'component_id','')::uuid;
  if v_component_id is null
     or (select source_component_id from public.candidate_submission_components
         where id=v_component_id) is distinct from v_same_workflow_earlier_root then
    raise exception 'Earlier same-workflow generation was not carried forward: %',v_result;
  end if;

  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,expense_category,document_role,state,storage_key,
    media_type,byte_size,source_content_sha256,immutable_at_utc
  ) values(
    v_same_workflow_current_root,v_target_workflow,3,99,v_timesheet,
    'EXPENSE_EVIDENCE','TRAVEL','SOURCE_EVIDENCE','IMMUTABLE',
    'verification/receipt-reuse/'||v_same_workflow_current_root::text||'.jpg',
    'image/jpeg',321,
    extensions.digest(
      pg_catalog.convert_to(v_same_workflow_current_root::text,'UTF8'),'sha256'
    ),v_now
  );
  begin
    perform pg_temp.candidate_receipt_prepare(
      v_session,v_target_workflow,3,v_same_workflow_current_root,'TRAVEL',
      'receipt-reuse:block-current-generation',v_now+interval '10 seconds'
    );
    raise exception 'Current-generation source was unexpectedly reusable';
  exception when invalid_authorization_specification then
    if sqlerrm<>'CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED' then
      raise;
    end if;
  end;
end;
$verification$;

rollback;
