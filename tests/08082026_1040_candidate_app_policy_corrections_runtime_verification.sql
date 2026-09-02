-- Targeted executable verification for the 8 August Candidate App policy corrections.
-- Run only after the complete bundle has been installed in a disposable PostgreSQL database.
-- All fixture writes and canonical mutations are rolled back.

begin;

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

-- The compact compile fixture intentionally omits this existing TEST authority.
-- The real installed function remains authoritative outside this rolled-back test.
create or replace function public._import_review_assert_actor_v1(p_actor_user_id uuid)
returns void language plpgsql as $function$
begin
  if p_actor_user_id is null then
    raise exception 'ACTOR_REQUIRED' using errcode='42501';
  end if;
end;
$function$;

create or replace function public._ctms_assert_import_correction_settings_write_v1(
  p_is_nhsp boolean,
  p_requires_hr boolean,
  p_no_timesheet_required boolean,
  p_reversal_complete public.correction_financials_date_basis_enum,
  p_reversal_replacement public.correction_financials_date_basis_enum
)
returns void language plpgsql as $function$
begin
  return;
end;
$function$;

do $policy_and_claim_gate$
declare
  v_client uuid:='a1000000-0000-0000-0000-000000000001';
  v_candidate uuid:='a1000000-0000-0000-0000-000000000002';
  v_contract uuid:='a1000000-0000-0000-0000-000000000003';
  v_week uuid:='a1000000-0000-0000-0000-000000000004';
  v_timesheet uuid:='a1000000-0000-0000-0000-000000000005';
  v_actor uuid:='a1000000-0000-0000-0000-000000000006';
  v_account uuid:='a1000000-0000-0000-0000-000000000007';
  v_session uuid:='a1000000-0000-0000-0000-000000000008';
  v_prior_workflow uuid:='a1000000-0000-0000-0000-000000000009';
  v_new_workflow uuid:='a1000000-0000-0000-0000-00000000000a';
  v_policy jsonb;
  v_placement jsonb;
  v_response jsonb;
  v_settings_updated_at timestamptz;
begin
  insert into public.tms_users(id) values(v_actor);
  begin
    perform public.client_create_with_settings_v1(
      'a1000000-0000-0000-0000-0000000000b0'::uuid,
      jsonb_build_object('name','Invalid free manager email client'),v_actor,
      jsonb_build_object(
        'candidate_manager_approval_policy_json',jsonb_build_object(
          'approved_emails',jsonb_build_array(),
          'approved_domains',jsonb_build_array(),
          'allow_free_business_email',true
        )
      ),now());
    raise exception 'client create enabled free manager email without a barred-domain policy';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_BARRED_MANAGER_DOMAIN_POLICY_REQUIRED' then raise; end if;
  end;
  begin
    perform public.client_create_with_settings_v1(
      'a1000000-0000-0000-0000-0000000000b1'::uuid,
      jsonb_build_object('name','Invalid import separation client'),v_actor,
      jsonb_build_object(
        'is_nhsp',true,
        'candidate_expenses_require_separate_timesheet',false,
        'candidate_expense_invoice_email','expenses@example.test'
      ),now());
    raise exception 'import client create accepted disabled candidate expense separation';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_IMPORT_EXPENSE_SEPARATION_REQUIRED' then raise; end if;
  end;
  begin
    perform public.client_create_with_settings_v1(
      'a1000000-0000-0000-0000-0000000000b2'::uuid,
      jsonb_build_object('name','Invalid import email client'),v_actor,
      jsonb_build_object(
        'is_nhsp',true,
        'candidate_expenses_require_separate_timesheet',true
      ),now());
    raise exception 'import client create accepted a missing Expense Invoice Email';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_IMPORT_EXPENSE_EMAIL_REQUIRED' then raise; end if;
  end;
  insert into public.clients(id,name) values(v_client,'Import policy runtime client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'policy-runtime@example.test',true,'GCK-POLICY-RUNTIME');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,is_nhsp,
    candidate_expenses_require_separate_timesheet,candidate_expense_invoice_email
  ) values(
    gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',true,true,
    'expenses@example.test'
  ) returning updated_at into v_settings_updated_at;
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,weekly_timesheet_source,
    candidate_expenses_require_separate_timesheet_override
  ) values(
    v_contract,v_candidate,v_client,current_date-60,current_date+60,
    extract(dow from current_date)::integer,'ELECTRONIC','NHSP',false
  );
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,additional_units_week
  ) values(v_timesheet,v_contract,current_date,'HOURS','MANUAL','{"ON_CALL":1}'::jsonb);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'OPEN','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status
  ) values(v_timesheet,v_candidate,v_client,0,'UNPROCESSED');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','policy-runtime@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('a1',32),'hex'),
    now()+interval '30 days',now()+interval '90 days');

  v_policy:=private._candidate_policy_resolve_v1(v_client,v_contract,current_date);
  if coalesce((v_policy->>'import_expense_separation_mandatory')::boolean,false)=false
     or coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false)=false
     or v_policy->>'expenses_require_separate_timesheet_source'<>'IMPORT_MANDATORY'
     or coalesce((v_policy->>'expense_invoice_email_ready')::boolean,false)=false then
    raise exception 'mandatory import expense policy did not override the contract false value: %',v_policy;
  end if;

  v_placement:=public.expense_placement_resolve_v1(
    v_candidate,'TEST',v_timesheet,v_week,'{}'::jsonb,now());
  if v_placement#>>'{capabilities,record_role}'<>'IMPORT_HOURS'
     or coalesce((v_placement#>>'{capabilities,requires_carrier}')::boolean,false)=false
     or v_placement->>'placement'='BLOCKED' then
    raise exception 'import/additional-units-only carrier resolution was incorrect: %',v_placement;
  end if;

  begin
    perform public.client_update_with_settings_v1(
      v_client,1,v_settings_updated_at,'{}'::jsonb,
      jsonb_build_object('candidate_expenses_require_separate_timesheet',false),
      v_actor,repeat('S',16));
    raise exception 'import client accepted disabled candidate expense separation';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_IMPORT_EXPENSE_SEPARATION_REQUIRED' then raise; end if;
  end;
  begin
    perform public.client_update_with_settings_v1(
      v_client,1,v_settings_updated_at,'{}'::jsonb,
      jsonb_build_object('candidate_expense_invoice_email',null),
      v_actor,repeat('E',16));
    raise exception 'import client accepted a missing Expense Invoice Email';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_IMPORT_EXPENSE_EMAIL_REQUIRED' then raise; end if;
  end;
  begin
    perform public.client_update_with_settings_v1(
      v_client,1,v_settings_updated_at,'{}'::jsonb,
      jsonb_build_object(
        'candidate_manager_approval_policy_json',jsonb_build_object(
          'approved_emails',jsonb_build_array(),
          'approved_domains',jsonb_build_array(),
          'allow_free_business_email',true
        )
      ),v_actor,repeat('B',16));
    raise exception 'client update enabled free manager email without a barred-domain policy';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_BARRED_MANAGER_DOMAIN_POLICY_REQUIRED' then raise; end if;
  end;

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key,created_at_utc,updated_at_utc
  ) values(
    v_prior_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'FINALISED',2,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    v_policy,'{}'::jsonb,'prior-finalised',now(),now()
  );
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_new_workflow,'CREATE',1,jsonb_build_object(
        'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract,'contract_week_id',v_week,
        'anchor_timesheet_id',v_timesheet,
        'week_ending_date',current_date
      ),'claim-before-authorised',now());
    raise exception 'second expense claim was accepted before the first was authorised';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE' then raise; end if;
  end;
  update public.timesheets_financials set authorised_at_utc=now()
  where timesheet_id=v_timesheet and is_current=true;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_new_workflow,'CREATE',1,jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_week,
      'anchor_timesheet_id',v_timesheet,
      'week_ending_date',current_date
    ),'claim-after-authorised',now());
  if v_response->>'state'<>'WORKER_DRAFT' then
    raise exception 'second expense claim did not open after authorisation: %',v_response;
  end if;

  -- An invoiced/paid hours record stays immutable, but it remains a truthful
  -- worked-week anchor for a later, separate expense-only carrier.
  update public.contract_weeks set status='INVOICED' where id=v_week;
  update public.timesheets_financials set paid_at_utc=now()
  where timesheet_id=v_timesheet and is_current=true;
  v_placement:=public.expense_placement_resolve_v1(
    v_candidate,'TEST',v_timesheet,v_week,'{}'::jsonb,now());
  if v_placement->>'placement'<>'CREATE_CARRIER'
     or coalesce((v_placement#>>'{capabilities,protected}')::boolean,false)=false
     or coalesce((v_placement#>>'{capabilities,candidate_expenses_allowed}')::boolean,false)=false
     or coalesce((v_placement#>>'{capabilities,can_edit_expenses}')::boolean,false)=false
     or coalesce((v_placement#>>'{capabilities,requires_carrier}')::boolean,false)=false then
    raise exception 'protected import hours did not remain a separate-expense anchor: %',v_placement;
  end if;

  begin
    insert into public.candidate_submission_components(
      workflow_id,workflow_generation,component_no,component_kind,expense_category,
      document_role,state,created_at_utc
    ) values(v_new_workflow,1,900,'EXPENSE_EVIDENCE',null,'SOURCE_EVIDENCE','PENDING',now());
    raise exception 'schema accepted a null category for expense evidence';
  exception when check_violation then null;
  end;
  begin
    insert into public.candidate_submission_components(
      workflow_id,workflow_generation,component_no,component_kind,expense_category,
      document_role,state,created_at_utc
    ) values(v_new_workflow,1,901,'MILEAGE_FORM','TRAVEL','MILEAGE_CLAIM_FORM','PENDING',now());
    raise exception 'schema accepted a non-mileage category for a mileage form';
  exception when check_violation then null;
  end;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_new_workflow,'COMPONENT_PREPARE',1,jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE','document_role','SOURCE_EVIDENCE',
        'storage_key','policy/null-category.png','media_type','image/png','byte_size',128
      ),'null-category',now());
    raise exception 'uncategorised expense evidence was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_COMPONENT_TYPE_INVALID' then raise; end if;
  end;
end;
$policy_and_claim_gate$;

do $paper_complete_pack$
declare
  v_client uuid:='a2000000-0000-0000-0000-000000000001';
  v_candidate uuid:='a2000000-0000-0000-0000-000000000002';
  v_contract uuid:='a2000000-0000-0000-0000-000000000003';
  v_week uuid:='a2000000-0000-0000-0000-000000000004';
  v_timesheet uuid:='a2000000-0000-0000-0000-000000000005';
  v_actor uuid:='a2000000-0000-0000-0000-000000000006';
  v_account uuid:='a2000000-0000-0000-0000-000000000007';
  v_session uuid:='a2000000-0000-0000-0000-000000000008';
  v_workflow uuid:='a2000000-0000-0000-0000-000000000009';
  v_source_component uuid;
  v_unsafe_component uuid;
  v_return_component uuid;
  v_response jsonb;
  v_page jsonb;
  v_manifest jsonb;
  v_manifest_hash text;
  v_service jsonb;
  v_mail uuid;
  v_row_signature text;
  v_failed boolean:=false;
  v_counter integer:=0;
begin
  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;
  insert into public.clients(id,name) values(v_client,'Paper complete-pack runtime client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'paper-runtime@example.test',true,'GCK-PAPER-RUNTIME');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_expenses_require_separate_timesheet,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',false,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-60,current_date+60,
    extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    qr_status,qr_token,actual_schedule_json
  ) values(
    v_timesheet,v_contract,current_date,'HOURS','MANUAL',
    'PENDING','paper-complete-pack-runtime-token',
    jsonb_build_array(jsonb_build_object(
      'date',current_date,'start','09:00','end','17:30','break_minutes',30
    ))
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'OPEN','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status
  ) values(v_timesheet,v_candidate,v_client,8,'UNPROCESSED');
  insert into public.invoice_document_versions(
    entity_type,entity_id,purpose,source_revision,template_version,status,
    r2_key,sha256,size_bytes,page_count
  ) values(
    'TIMESHEET',v_timesheet,'TIMESHEET','1','timesheet-professional-v2','READY',
    'candidate-app/test/paper-base.pdf',repeat('f',64),1024,1
  );
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','paper-runtime@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('a2',32),'hex'),
    now()+interval '30 days',now()+interval '90 days');

  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CREATE',1,jsonb_build_object(
      'workflow_kind','CONTRACT_COMBINED','scope','WEEKLY','route','PAPER',
      'contract_id',v_contract,'contract_week_id',v_week,
      'anchor_timesheet_id',v_timesheet,'target_timesheet_id',v_timesheet,
      'week_ending_date',current_date
    ),'paper:create',now());
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,jsonb_build_object(
      'component_kind','EXPENSE_EVIDENCE','expense_category','OTHER',
      'document_role','SOURCE_EVIDENCE','storage_key','paper/source/other.png',
      'media_type','image/png','byte_size',1024
    ),'paper:evidence:prepare',now());
  v_source_component:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,jsonb_build_object(
      'component_id',v_source_component,'source_content_sha256_hex',repeat('a3',32),
      'verified_byte_size',1024,'verified_media_type','image/png'
    ),'paper:evidence:complete',now());
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,jsonb_build_object(
      'component_kind','EXPENSE_EVIDENCE','expense_category','TRAVEL',
      'document_role','SOURCE_EVIDENCE','storage_key','paper/source/unsafe.png',
      'media_type','image/png','byte_size',128
    ),'paper:unsafe:prepare',now());
  v_unsafe_component:=(v_response->>'component_id')::uuid;
  update public.candidate_submission_components
  set state='SUPERSEDED',superseded_at_utc=now()
  where id=v_unsafe_component;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,jsonb_build_object(
      'component_kind','EXPENSE_EVIDENCE','expense_category','TRAVEL',
      'document_role','SOURCE_EVIDENCE','storage_key','paper/source/unsafe.png',
      'media_type','image/png','byte_size',128
    ),'paper:unsafe:prepare',now());
  if not coalesce((v_response->>'idempotent_replay')::boolean,false)
     or (v_response->>'component_id')::uuid<>v_unsafe_component then
    raise exception 'superseded component exact replay did not preserve its durable receipt: %',v_response;
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,jsonb_build_object(
        'component_id',v_unsafe_component,'source_content_sha256_hex',repeat('a5',32),
        'verified_byte_size',128,'verified_media_type','image/png'
      ),'paper:unsafe:complete',now());
    raise exception 'superseded component was revived by completion';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_COMPONENT_COMPLETE_STATE_CONFLICT' then raise; end if;
  end;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'WORKER_SUBMIT',1,jsonb_build_object(
      'approval_route','PAPER',
      'immutable_submission',jsonb_build_object(
        'hours_submission',jsonb_build_object(
          'canonical_tsfin_snapshot',jsonb_build_object(
            'candidate_id',v_candidate,'client_id',v_client,'total_hours',8,
            'hours_day',8,'total_pay_ex_vat',80,'total_charge_ex_vat',100,'margin_ex_vat',20
          ),
          'timesheet_patch_json',jsonb_build_object(
            'actual_schedule_json',jsonb_build_array(jsonb_build_object(
              'date',current_date,'start','09:00','end','17:30','break_minutes',30
            ))
          ),
          'contract_week_patch_json','{}'::jsonb
        ),
        'expense_submission',jsonb_build_object(
          'canonical_tsfin_snapshot',jsonb_build_object(
            'candidate_id',v_candidate,'client_id',v_client,
            'other_pay_ex_vat',10,'other_charge_ex_vat',12,
            'total_pay_ex_vat',90,'total_charge_ex_vat',112,'margin_ex_vat',22
          ),
          'timesheet_patch_json','{}'::jsonb,'contract_week_patch_json','{}'::jsonb,
          'evidence_display_name','Returned paper expense page'
        )
      )
    ),'paper:submit',now());
  if v_response->>'state'<>'WORKER_SUBMITTED' or v_response->>'generation'<>'2' then
    raise exception 'paper worker submission did not freeze generation two: %',v_response;
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'COMPONENT_PREPARE',2,jsonb_build_object(
        'component_kind','EXPENSE_EVIDENCE','expense_category','OTHER',
        'document_role','SOURCE_EVIDENCE','storage_key','paper/source/other.png',
        'media_type','image/png','byte_size',1024
      ),'paper:evidence:prepare',now());
    raise exception 'cross-generation mutation key reuse unexpectedly issued a contract';
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_IDEMPOTENCY_CONFLICT' then raise; end if;
  end;

  update public.timesheets set actual_schedule_json='[]'::jsonb where timesheet_id=v_timesheet;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'PAPER_PREPARE',2,'{}'::jsonb,
      'paper:prepare:queue-failure',now());
    raise exception 'paper preparation was accepted when the canonical QR queue rejected the timesheet';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_PAPER_PACK_QUEUE_FAILED' then raise; end if;
  end;
  if (select state from public.candidate_submission_workflows where id=v_workflow)<>'WORKER_SUBMITTED'
     or exists(
       select 1 from public.mail_outbox
       where payment_scope_json->>'candidate_workflow_id'=v_workflow::text
     ) then
    raise exception 'canonical QR queue failure did not roll back PAPER preparation atomically';
  end if;
  update public.timesheets
  set actual_schedule_json=jsonb_build_array(jsonb_build_object(
    'date',current_date,'start','09:00','end','17:30','break_minutes',30
  ))
  where timesheet_id=v_timesheet;

  update public.candidates set email=null where id=v_candidate;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'PAPER_PREPARE',2,'{}'::jsonb,
      'paper:prepare:missing-email',now());
    raise exception 'paper preparation was accepted without a candidate email';
  exception
    when sqlstate '28000' then
      if sqlerrm<>'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' then raise; end if;
    when sqlstate '55000' then
      if sqlerrm<>'CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE' then raise; end if;
  end;
  if (select state from public.candidate_submission_workflows where id=v_workflow)<>'WORKER_SUBMITTED'
     or exists(
       select 1 from public.mail_outbox
       where payment_scope_json->>'candidate_workflow_id'=v_workflow::text
     ) then
    raise exception 'missing-email PAPER preparation did not roll back atomically';
  end if;

  update public.candidates
  set email='paper-runtime@example.test',opt_in_email=false
  where id=v_candidate;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'PAPER_PREPARE',2,'{}'::jsonb,
      'paper:prepare:email-opted-out',now());
    raise exception 'paper preparation was accepted after email opt-out';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE' then raise; end if;
  end;
  if (select state from public.candidate_submission_workflows where id=v_workflow)<>'WORKER_SUBMITTED'
     or exists(
       select 1 from public.mail_outbox
       where payment_scope_json->>'candidate_workflow_id'=v_workflow::text
     ) then
    raise exception 'email-opt-out PAPER preparation did not roll back atomically';
  end if;
  update public.candidates set opt_in_email=true where id=v_candidate;

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'PAPER_PREPARE',2,'{}'::jsonb,'paper:prepare',now());
  select paper_return_manifest_json into v_manifest
  from public.candidate_submission_workflows where id=v_workflow;
  v_mail:=(v_response->'paper_pack'->>'mail_outbox_id')::uuid;
  select encode(paper_return_manifest_sha256,'hex') into v_manifest_hash
  from public.candidate_submission_workflows where id=v_workflow;
  if v_response->>'state'<>'AWAITING_PAPER_RETURN'
     or jsonb_array_length(v_manifest->'pages')<>3
     or not coalesce((v_response->'paper_pack'->>'queued')::boolean,false)
     or not coalesce((v_response->'paper_pack'->>'recipient_available')::boolean,false)
     or nullif(v_response->'paper_pack'->>'mail_outbox_id','') is null then
    raise exception 'paper manifest was not the complete three-page pack: %',v_response;
  end if;
  if not exists(
    select 1 from public.mail_outbox mail
    where mail.id=v_mail and mail.type='TIMESHEET_QR'
      and mail.context_kind='timesheets' and mail.context_id=v_timesheet
      and mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and mail.payment_scope_json->>'candidate_workflow_id'=v_workflow::text
      and mail.payment_scope_json->>'candidate_workflow_generation'='2'
      and mail.payment_scope_json->>'paper_return_manifest_sha256'=v_manifest_hash
  ) then
    raise exception 'canonical PAPER enqueue omitted the closed Office/Candidate authority identity';
  end if;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'PAPER_PACK_MARK_FAILURE',2,jsonb_build_object(
      'service_paper_pack_failure',true,
      'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hash,
      'error_code','CANDIDATE_PAPER_PACK_ASSEMBLY_TRANSIENT'
    ),'paper:pack:failure:retryable',now()
  );
  if v_response->>'paper_pack_state'<>'FAILED_RETRYABLE'
     or not coalesce((v_response->>'retryable')::boolean,false)
     or not exists(
       select 1 from public.mail_outbox mail where mail.id=v_mail
         and lower(coalesce(mail.payment_scope_json->>'candidate_paper_pack_retryable','false'))
           in ('true','t','1','yes')
     ) then
    raise exception 'production PAPER failure owner did not create a retryable receipt: %',v_response;
  end if;

  v_page:=(v_manifest->'pages')->0;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',2,jsonb_build_object(
      'component_kind','SIGNED_RETURN','document_role','SIGNED_RETURN',
      'paper_return_page_key',v_page->>'page_key',
      'storage_key','paper/returned/first.pdf','media_type','application/pdf','byte_size',512
    ),'paper:return:first:prepare',now());
  v_return_component:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',2,jsonb_build_object(
      'component_id',v_return_component,'source_content_sha256_hex',repeat('a4',32),
      'verified_byte_size',512,'verified_media_type','application/pdf'
    ),'paper:return:first:complete',now());
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'PAPER_RETURN',2,'{}'::jsonb,'paper:return:incomplete',now());
    raise exception 'one returned page was accepted as a complete paper pack';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_PAPER_RETURN_INCOMPLETE' then raise; end if;
  end;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'COMPONENT_PREPARE',2,jsonb_build_object(
        'component_kind','SIGNED_RETURN','document_role','SIGNED_RETURN',
        'paper_return_page_key',v_page->>'page_key',
        'storage_key','paper/returned/duplicate.pdf','media_type','application/pdf','byte_size',512
      ),'paper:return:duplicate:prepare',now());
    raise exception 'duplicate returned paper page was accepted';
  exception when sqlstate '23505' then
    if sqlerrm<>'CANDIDATE_PAPER_RETURN_PAGE_DUPLICATE' then raise; end if;
  end;

  for v_page in select value from jsonb_array_elements(v_manifest->'pages') with ordinality p(value,ordinality)
    where ordinality>1 order by ordinality
  loop
    v_counter:=v_counter+1;
    v_response:=public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'COMPONENT_PREPARE',2,jsonb_build_object(
        'component_kind','SIGNED_RETURN','document_role','SIGNED_RETURN',
        'paper_return_page_key',v_page->>'page_key',
        'storage_key','paper/returned/'||v_counter::text||'.pdf',
        'media_type','application/pdf','byte_size',512
      ),'paper:return:'||v_counter::text||':prepare',now());
    v_return_component:=(v_response->>'component_id')::uuid;
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',2,jsonb_build_object(
        'component_id',v_return_component,
        'source_content_sha256_hex',encode(extensions.digest(
          convert_to('paper-return-'||v_counter::text,'UTF8'),'sha256'),'hex'),
        'verified_byte_size',512,'verified_media_type','application/pdf'
      ),'paper:return:'||v_counter::text||':complete',now());
  end loop;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'PAPER_RETURN',2,'{}'::jsonb,'paper:return:complete',now());
  if v_response->>'state'<>'RECEIVED' then
    raise exception 'complete paper pack did not become received: %',v_response;
  end if;

  v_row_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet,v_week,false)->>'row_signature';
  v_service:=jsonb_build_object(
    'contract_version','CANDIDATE_MANAGER_FINALISATION_V1',
    'workflow_generation',2,
    'approval_method','PAPER',
    'approval_request_id',null,
    'approval_request_generation',null,
    'review_manifest_sha256_hex','',
    'paper_return_manifest_sha256_hex',v_manifest_hash,
    'finalisation_identity',jsonb_build_object(
      'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
      'workflow_id',v_workflow,
      'workflow_generation',2,
      'approval_method','PAPER',
      'approval_request_id',null,
      'approval_request_generation',null,
      'review_manifest_sha256_hex',null,
      'paper_return_manifest_sha256_hex',v_manifest_hash
    )
  );
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'FINALISE_EXECUTE',v_actor,'TEST',jsonb_build_object(
        'workflow_id',v_workflow,
        'generation',2,
        'expected_row_signature',repeat('0',64),
        'idempotency_key','paper:finalise:initial',
        'daily_materialisation_json',jsonb_build_object('service_finalisation',v_service)
      ),now()
    );
    raise exception 'stale initial PAPER finalisation unexpectedly succeeded';
  exception when others then
    if sqlerrm='ROW_SIGNATURE_MISMATCH' then
      v_failed:=true;
    else
      raise;
    end if;
  end;
  if not v_failed
     or (select state from public.candidate_submission_workflows where id=v_workflow)<>'RECEIVED'
     or exists(
       select 1 from public.audit_events ae
       where ae.object_type='candidate_workflow_finalisation_completion'
         and ae.object_id_text=v_workflow::text
     ) then
    raise exception 'failed initial PAPER finalisation left lifecycle or completion residue';
  end if;
  v_response:=public.cloudtms_office_candidate_adapter_v1(
    'FINALISE_EXECUTE',v_actor,'TEST',jsonb_build_object(
      'workflow_id',v_workflow,
      'generation',2,
      'expected_row_signature',v_row_signature,
      'idempotency_key','paper:finalise:retry',
      'daily_materialisation_json',jsonb_build_object('service_finalisation',v_service)
    ),now()
  );
  if v_response->>'state'<>'FINALISED'
     or not exists(select 1 from public.timesheets
       where timesheet_id=v_timesheet and submission_mode='MANUAL'
         and r2_nurse_key is null and r2_auth_key is null)
     or not exists(select 1 from public.timesheets_financials
       where timesheet_id=v_timesheet and is_current and total_hours=8 and other_pay_ex_vat=10)
     or (select count(*) from public.timesheet_evidence where timesheet_id=v_timesheet)<>3
     or not exists(select 1 from public.timesheet_evidence
       where timesheet_id=v_timesheet and kind='TIMESHEET' and document_role='SIGNED_TIMESHEET') then
    raise exception 'complete paper pack did not materialise atomically: %',v_response;
  end if;
  v_response:=public.cloudtms_office_candidate_adapter_v1(
    'FINALISE_REPLAY_LOOKUP',v_actor,'TEST',jsonb_build_object(
      'workflow_id',v_workflow,
      'generation',2,
      'expected_row_signature',repeat('0',64),
      'idempotency_key','paper:finalise:initial',
      'daily_materialisation_json',jsonb_build_object('service_finalisation',v_service)
    ),now()
  );
  if v_response->>'state'<>'FINALISED'
     or v_response->>'generation'<>'3'
     or not coalesce((v_response->>'idempotent_replay')::boolean,false) then
    raise exception 'failed trigger key did not find the later canonical PAPER completion: %',v_response;
  end if;
end;
$paper_complete_pack$;

do $dated_read_boundaries$
declare
  v_client uuid:='a3000000-0000-0000-0000-000000000001';
  v_candidate uuid:='a3000000-0000-0000-0000-000000000002';
  v_contract uuid:='a3000000-0000-0000-0000-000000000003';
  v_account uuid:='a3000000-0000-0000-0000-000000000004';
  v_session uuid:='a3000000-0000-0000-0000-000000000005';
  v_weekday integer:=extract(dow from current_date)::integer;
  v_current_week date:=current_date;
  v_response jsonb;
  v_options jsonb;
  v_week_date date;
  v_week_id uuid;
  v_timesheet_id uuid;
  v_index integer:=0;
begin
  insert into public.clients(id,name) values(v_client,'Dated reads runtime client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'dated-reads@example.test',true,'GCK-DATED-READS');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,
    candidate_paper_submission_enabled
  ) values
    (gen_random_uuid(),v_client,current_date-200,'MANUAL',v_weekday,false),
    (gen_random_uuid(),v_client,current_date+7,'ELECTRONIC',v_weekday,false);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,overrideclientsettings
  ) values(v_contract,v_candidate,v_client,current_date-200,current_date+35,
    v_weekday,'MANUAL',false);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','dated-reads@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('a5',32),'hex'),
    now()+interval '30 days',now()+interval '90 days');

  foreach v_week_date in array array[
    v_current_week-49,
    v_current_week-56,
    v_current_week-100
  ] loop
    v_index:=v_index+1;
    v_week_id:=('a3000000-0000-0000-0000-'||lpad((100+v_index)::text,12,'0'))::uuid;
    v_timesheet_id:=('a3000000-0000-0000-0000-'||lpad((200+v_index)::text,12,'0'))::uuid;
    insert into public.timesheets(
      timesheet_id,contract_id,week_ending_date,line_type,submission_mode
    ) values(v_timesheet_id,v_contract,v_week_date,'HOURS','MANUAL');
    insert into public.contract_weeks(
      id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
    ) values(v_week_id,v_contract,v_week_date,'OPEN','MANUAL',v_timesheet_id);
    insert into public.timesheets_financials(
      timesheet_id,candidate_id,client_id,total_hours,processing_status,paid_at_utc
    ) values(
      v_timesheet_id,v_candidate,v_client,8,'READY_FOR_INVOICE',
      case when v_week_date=v_current_week-100 then null else now() end
    );
  end loop;

  v_response:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,50,now());
  if not exists(
      select 1 from jsonb_array_elements(v_response->'items') item
      where (item->>'week_ending_date')::date=v_current_week-49
    )
     or not exists(
      select 1 from jsonb_array_elements(v_response->'items') item
      where (item->>'week_ending_date')::date=v_current_week-56
    )
     or not exists(
      select 1 from jsonb_array_elements(v_response->'items') item
      where (item->>'week_ending_date')::date=v_current_week-100
        and coalesce((item->>'paid')::boolean,false)=false
    ) then
    raise exception 'Current recent-paid or age-unbounded unpaid membership was incorrect: %',v_response;
  end if;

  v_response:=public.candidate_missing_week_options_v1(
    v_session,'TEST',v_contract,current_date,current_date+21,now());
  v_options:=v_response->'options';
  if exists(
      select 1 from jsonb_array_elements(v_options) option_row
      where (option_row->>'week_ending_date')::date=current_date
    )
     or not exists(
      select 1 from jsonb_array_elements(v_options) option_row
      where (option_row->>'week_ending_date')::date=current_date+7
        and option_row->>'submission_mode'='ELECTRONIC'
    ) then
    raise exception 'missing-week policy was not resolved for each generated week: %',v_response;
  end if;
end;
$dated_read_boundaries$;

rollback;
