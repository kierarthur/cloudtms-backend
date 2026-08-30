-- Rollback-contained first-use proof for QR/printed evidence manifest labels.
-- Installs no authority and leaves no fixture rows after completion.

\set ON_ERROR_STOP on

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
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(
    v_actor,
    'qr-evidence-label-runtime@example.invalid',
    'UNUSABLE_VERIFICATION_ONLY',
    'admin',
    true
  );
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
  perform public.candidate_paper_manifest_v2_promote_v1(
    v_session,'TEST',v_workflow,2,
    v_response->>'paper_return_manifest_sha256',now()
  );
  select paper_return_manifest_json into v_manifest
  from public.candidate_submission_workflows where id=v_workflow;
  v_mail:=(v_response->'paper_pack'->>'mail_outbox_id')::uuid;
  select encode(paper_return_manifest_sha256,'hex') into v_manifest_hash
  from public.candidate_submission_workflows where id=v_workflow;
  if v_response->>'state'<>'AWAITING_PAPER_RETURN'
     or jsonb_array_length(v_manifest->'pages')<>3
     or v_manifest->>'manifest_version'<>'2'
     or v_manifest->>'qr_contract_version'<>'CANDIDATE_PAPER_PAGE_QR_V2'
     or v_manifest#>>'{pages,1,display_name}'<>'Expense summary'
     or v_manifest#>>'{pages,2,display_name}'<>'Other 1'
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
  if v_response->>'state'<>'FINALISED' then
    raise exception 'complete paper pack did not finalise: %',v_response;
  end if;
  if not exists(select 1 from public.timesheets
    where timesheet_id=v_timesheet and submission_mode='MANUAL'
      and r2_nurse_key is null and r2_auth_key is null) then
    raise exception 'PAPER finalisation did not preserve the expected unsigned MANUAL Timesheet';
  end if;
  if (select count(*) from public.timesheet_evidence where timesheet_id=v_timesheet)<>3 then
    raise exception 'PAPER finalisation did not attach exactly three evidence rows: %',
      (select jsonb_agg(jsonb_build_object(
        'kind',kind,'document_role',document_role,'display_name',display_name,
        'candidate_component_id',candidate_component_id
      ) order by created_at_utc,id) from public.timesheet_evidence where timesheet_id=v_timesheet);
  end if;
  if not exists(select 1 from public.timesheet_evidence
    where timesheet_id=v_timesheet and kind='TIMESHEET' and document_role='SIGNED_TIMESHEET') then
    raise exception 'PAPER finalisation did not attach the signed Timesheet evidence';
  end if;
  if not exists(
       select 1
       from public.timesheet_evidence evidence
       join public.candidate_submission_components component
         on component.id=evidence.candidate_component_id
       join lateral jsonb_array_elements(v_manifest->'pages') manifest_page
         on manifest_page->>'page_key'=component.paper_return_page_key
       where evidence.timesheet_id=v_timesheet
         and manifest_page->>'component_kind'='EXPENSE_SUMMARY'
         and manifest_page->>'display_name'='Expense summary'
         and evidence.display_name=manifest_page->>'display_name'
     ) then
    raise exception 'PAPER finalisation did not attach the Expense summary manifest label: %',
      (select jsonb_agg(jsonb_build_object(
        'display_name',evidence.display_name,
        'candidate_component_id',evidence.candidate_component_id,
        'component_kind',component.component_kind,
        'paper_return_page_key',component.paper_return_page_key,
        'matched_manifest_page',manifest_page.page
      ))
       from public.timesheet_evidence evidence
       left join public.candidate_submission_components component
         on component.id=evidence.candidate_component_id
       left join lateral (
         select page
         from jsonb_array_elements(v_manifest->'pages') page
         where page->>'page_key'=component.paper_return_page_key
       ) manifest_page on true
       where evidence.timesheet_id=v_timesheet);
  end if;
  if not exists(
       select 1
       from public.timesheet_evidence evidence
       join public.candidate_submission_components component
         on component.id=evidence.candidate_component_id
       join lateral jsonb_array_elements(v_manifest->'pages') manifest_page
         on manifest_page->>'page_key'=component.paper_return_page_key
       where evidence.timesheet_id=v_timesheet
         and manifest_page->>'component_kind'='EXPENSE_EVIDENCE'
         and manifest_page->>'display_name'='Other 1'
         and evidence.display_name=manifest_page->>'display_name'
     ) then
    raise exception 'PAPER finalisation did not attach the Other 1 manifest label: %',
      (select jsonb_agg(jsonb_build_object('display_name',display_name,'candidate_component_id',candidate_component_id))
       from public.timesheet_evidence where timesheet_id=v_timesheet);
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
select 'PASS'::text as candidate_paper_evidence_manifest_label_runtime_verification;

rollback;
