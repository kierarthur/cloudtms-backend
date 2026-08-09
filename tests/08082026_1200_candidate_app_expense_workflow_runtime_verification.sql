-- Candidate App combined and standalone-expense executable verification.
-- Run only in a disposable database after the complete install bundle.
-- Every fixture write, temporary helper and canonical mutation is rolled back.

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

create function pg_temp.candidate_register_all_review_components(
  p_workflow_id uuid,
  p_generation integer,
  p_key_prefix text,
  p_now_utc timestamptz
)
returns void
language plpgsql
as $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_contract jsonb;
  v_receipt jsonb;
  v_response jsonb;
begin
  for v_component in
    select * from public.candidate_submission_components
    where workflow_id=p_workflow_id and workflow_generation=p_generation
      and required=true and state<>'SUPERSEDED'
    order by review_ordinal,id
  loop
    v_contract:=private._candidate_component_render_contract_v1(
      p_workflow_id,p_generation,v_component.id,'REVIEW');
    v_receipt:=jsonb_build_object(
      'form_variant',v_contract->>'form_variant',
      'workflow_id',p_workflow_id,
      'workflow_generation',p_generation,
      'component_id',v_component.id,
      'component_kind',v_component.component_kind,
      'document_role',v_component.document_role,
      'review_ordinal',v_component.review_ordinal,
      'scope',v_contract->>'scope',
      'page_count',1,
      'render_input_sha256',v_contract->>'render_input_sha256',
      'candidate_signature_embedded',(v_contract->>'candidate_signature_embedded')::boolean,
      'manager_signature_embedded',false,
      'manager_approval_date_embedded',false
    );
    v_response:=public.candidate_workflow_transition_atomic_v1(
      null,'TEST',p_workflow_id,'REGISTER_REVIEW_COMPONENT',p_generation,
      jsonb_build_object(
        'component_id',v_component.id,
        'storage_key',p_key_prefix||'/review/'||v_component.id::text||'.pdf',
        'content_sha256_hex',encode(extensions.digest(
          convert_to(p_key_prefix||':review:'||v_component.id::text,'UTF8'),'sha256'),'hex'),
        'render_input_sha256_hex',v_contract->>'render_input_sha256',
        'media_type','application/pdf','byte_size',2048,'page_count',1,
        'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
        'renderer_receipt',v_receipt
      ),p_key_prefix||':register-review:'||v_component.id::text,p_now_utc
    );
    if coalesce((v_response->>'review_document_ready')::boolean,false)=false then
      raise exception 'review component did not become ready: %',v_response;
    end if;
  end loop;
  if (select state from public.candidate_submission_workflows where id=p_workflow_id)
     <>'READY_FOR_MANAGER_APPROVAL' then
    raise exception 'workflow did not become review-ready';
  end if;
end;
$function$;

create function pg_temp.candidate_phone_approve_all(
  p_session_id uuid,
  p_workflow_id uuid,
  p_generation integer,
  p_key_prefix text,
  p_signature_hash_seed text,
  p_now_utc timestamptz
)
returns jsonb
language plpgsql
as $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_response jsonb;
  v_manifest_hash text;
  v_approval_request_id uuid;
  v_signature_component_id uuid;
begin
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'SELECT_PHONE_APPROVAL',p_generation,
    jsonb_build_object(
      'approval_token_hash_hex',encode(extensions.digest(p_workflow_id::text||':'||p_key_prefix||':phone','sha256'),'hex'),
      'expires_at_utc',p_now_utc+interval '30 minutes'
    ),p_key_prefix||':phone-select',p_now_utc);
  v_manifest_hash:=v_response->>'review_manifest_sha256';
  v_approval_request_id:=(v_response->>'approval_request_id')::uuid;
  if v_response->>'state'<>'AWAITING_MANAGER_APPROVAL' or v_manifest_hash is null then
    raise exception 'phone approval request failed: %',v_response;
  end if;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'BEGIN_MANAGER_REVIEW',p_generation,
    '{}'::jsonb,p_key_prefix||':begin-review',p_now_utc);
  if v_response->>'manifest_sha256'<>v_manifest_hash then
    raise exception 'manager manifest changed: %',v_response;
  end if;
  for v_component in
    select * from public.candidate_submission_components
    where workflow_id=p_workflow_id and workflow_generation=p_generation
      and required=true and state<>'SUPERSEDED'
    order by review_ordinal,id
  loop
    perform public.candidate_workflow_transition_atomic_v1(
      p_session_id,'TEST',p_workflow_id,'RECORD_REVIEW_PROGRESS',p_generation,
      jsonb_build_object(
        'manifest_sha256_hex',v_manifest_hash,
        'component_id',v_component.id,
        'component_sha256_hex',encode(v_component.review_content_sha256,'hex'),
        'viewed_receipt',jsonb_build_object('viewed',true,'component_id',v_component.id)
      ),p_key_prefix||':reviewed:'||v_component.id::text,p_now_utc);
  end loop;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'COMPONENT_PREPARE',p_generation,
    jsonb_build_object(
      'component_kind','MANAGER_SIGNATURE','document_role','MANAGER_SIGNATURE',
      'approval_request_id',v_approval_request_id,
      'storage_key',p_key_prefix||'/manager-signature.png',
      'media_type','image/png','byte_size',256
    ),p_key_prefix||':manager-signature-prepare',p_now_utc);
  v_signature_component_id:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'COMPONENT_COMPLETE',p_generation,
    jsonb_build_object(
      'component_id',v_signature_component_id,
      'source_content_sha256_hex',encode(extensions.digest(
        convert_to(p_signature_hash_seed,'UTF8'),'sha256'),'hex'),
      'verified_byte_size',256,'verified_media_type','image/png'
    ),p_key_prefix||':manager-signature-complete',p_now_utc);
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'PHONE_APPROVE',p_generation,
    jsonb_build_object(
      'manifest_sha256_hex',v_manifest_hash,
      'signature_component_id',v_signature_component_id,
      'manager_name','Runtime Manager','manager_position','Service Manager'
    ),p_key_prefix||':phone-approve',p_now_utc);
  if v_response->>'state'<>'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT' then
    raise exception 'manager approval failed: %',v_response;
  end if;
  return v_response;
end;
$function$;

create function pg_temp.candidate_register_all_final_components(
  p_workflow_id uuid,
  p_generation integer,
  p_key_prefix text,
  p_now_utc timestamptz
)
returns void
language plpgsql
as $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_contract jsonb;
  v_receipt jsonb;
  v_response jsonb;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id;
  for v_component in
    select * from public.candidate_submission_components
    where workflow_id=p_workflow_id and workflow_generation=p_generation
      and required=true and state<>'SUPERSEDED'
    order by review_ordinal,id
  loop
    v_contract:=private._candidate_component_render_contract_v1(
      p_workflow_id,p_generation,v_component.id,'FINAL');
    v_receipt:=jsonb_strip_nulls(jsonb_build_object(
      'form_variant',v_contract->>'form_variant',
      'workflow_id',p_workflow_id,
      'workflow_generation',p_generation,
      'component_id',v_component.id,
      'component_kind',v_component.component_kind,
      'document_role',v_component.document_role,
      'review_ordinal',v_component.review_ordinal,
      'scope',v_contract->>'scope','page_count',1,
      'render_input_sha256',v_contract->>'render_input_sha256',
      'candidate_signature_embedded',(v_contract->>'candidate_signature_embedded')::boolean,
      'manager_signature_embedded',true,'manager_approval_date_embedded',true,
      'candidate_signature_sha256',case when v_component.component_kind='HOURS_TIMESHEET'
        then encode(v_workflow.candidate_signature_sha256,'hex') end,
      'manager_signature_sha256',encode(v_workflow.manager_signature_sha256,'hex'),
      'manager_name',v_workflow.manager_name,'manager_position',v_workflow.manager_position,
      'manager_approved_at_utc',v_workflow.manager_approved_at_utc
    ));
    v_response:=public.candidate_workflow_transition_atomic_v1(
      null,'TEST',p_workflow_id,'REGISTER_FINAL_SIGNED_DOCUMENT',p_generation,
      jsonb_build_object(
        'component_id',v_component.id,
        'storage_key',p_key_prefix||'/final/'||v_component.id::text||'.pdf',
        'content_sha256_hex',encode(extensions.digest(
          convert_to(p_key_prefix||':final:'||v_component.id::text,'UTF8'),'sha256'),'hex'),
        'render_input_sha256_hex',v_contract->>'render_input_sha256',
        'media_type','application/pdf','byte_size',2304,'page_count',1,
        'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
        'renderer_receipt',v_receipt
      ),p_key_prefix||':register-final:'||v_component.id::text,p_now_utc);
  end loop;
  if (select state from public.candidate_submission_workflows where id=p_workflow_id)
     <>'READY_TO_FINALISE' then
    raise exception 'workflow did not become finalisation-ready';
  end if;
end;
$function$;

do $standalone_expense$
declare
  v_client uuid:='91000000-0000-0000-0000-000000000001';
  v_candidate uuid:='91000000-0000-0000-0000-000000000002';
  v_contract uuid:='91000000-0000-0000-0000-000000000003';
  v_week uuid:='91000000-0000-0000-0000-000000000004';
  v_anchor_timesheet uuid:='91000000-0000-0000-0000-000000000005';
  v_actor uuid:='91000000-0000-0000-0000-000000000006';
  v_account uuid:='91000000-0000-0000-0000-000000000007';
  v_session uuid:='91000000-0000-0000-0000-000000000008';
  v_workflow uuid:='91000000-0000-0000-0000-000000000009';
  v_source_component uuid;
  v_response jsonb;
  v_row_signature text;
  v_target_timesheet uuid;
begin
  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'standalone-expense@example.test',true,'GCK-STANDALONE');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_expenses_require_separate_timesheet
  ) values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    r2_nurse_key,r2_auth_key
  ) values(
    v_anchor_timesheet,v_contract,current_date,'HOURS','ELECTRONIC',
    'existing/candidate-signature','existing/manager-signature'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'OPEN','ELECTRONIC',v_anchor_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status
  ) values(v_anchor_timesheet,v_candidate,v_client,8,'UNPROCESSED');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','standalone-expense@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('91',32),'hex'),
    now()+interval '30 days',now()+interval '90 days');

  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CREATE',1,jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_week,
      'anchor_timesheet_id',v_anchor_timesheet,
      'week_ending_date',current_date
    ),'standalone:create',now());
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,jsonb_build_object(
      'component_kind','EXPENSE_EVIDENCE','expense_category','OTHER',
      'document_role','SOURCE_EVIDENCE','storage_key','standalone/source/receipt.png',
      'media_type','image/png','byte_size',1024
    ),'standalone:evidence:prepare',now());
  v_source_component:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,jsonb_build_object(
      'component_id',v_source_component,'source_content_sha256_hex',repeat('92',32),
      'verified_byte_size',1024,'verified_media_type','image/png'
    ),'standalone:evidence:complete',now());
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'WORKER_SUBMIT',1,jsonb_build_object(
      'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
      'immutable_submission',jsonb_build_object(
        'canonical_tsfin_snapshot',jsonb_build_object(
          'candidate_id',v_candidate,'client_id',v_client,
          'other_pay_ex_vat',10,'other_charge_ex_vat',12,
          'total_pay_ex_vat',10,'total_charge_ex_vat',12,'margin_ex_vat',2
        ),
        'timesheet_patch_json',jsonb_build_object('line_type','EXPENSES'),
        'contract_week_patch_json','{}'::jsonb,
        'evidence_display_name','Standalone expense evidence'
      )
    ),'standalone:submit',now());
  if v_response->>'state'<>'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'
     or (select candidate_signature_component_id from public.candidate_submission_workflows where id=v_workflow) is not null
     or exists(select 1 from public.candidate_submission_components
       where workflow_id=v_workflow and workflow_generation=2 and component_kind='HOURS_TIMESHEET')
     or (select count(*) from public.candidate_submission_components
       where workflow_id=v_workflow and workflow_generation=2 and required)=0 then
    raise exception 'standalone expense incorrectly required a worker signature/hours page: %',v_response;
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'SELECT_PHONE_APPROVAL',2,'{}'::jsonb,
      'standalone:too-early',now());
    raise exception 'manager approval began before all expense review pages were ready';
  exception when sqlstate '55000' then
    if sqlerrm<>'MANAGER_REVIEW_DOCUMENT_NOT_READY' then raise; end if;
  end;
  perform pg_temp.candidate_register_all_review_components(v_workflow,2,'standalone',now());
  v_response:=pg_temp.candidate_phone_approve_all(
    v_session,v_workflow,2,'standalone','standalone-manager-signature',now());
  v_row_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_anchor_timesheet,v_week,false)->>'row_signature';
  begin
    perform public.candidate_submission_finalize_atomic_v1(
      v_session,'TEST',v_workflow,2,v_row_signature,'standalone:too-early-finalise',now());
    raise exception 'standalone expense finalised before every signed derivative was ready';
  exception when sqlstate '55000' then
    if sqlerrm<>'FINAL_SIGNED_DOCUMENT_NOT_READY' then raise; end if;
  end;
  if (select state from public.candidate_submission_workflows where id=v_workflow)
     <>'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT' then
    raise exception 'manager approval was not retained after final-render failure';
  end if;
  perform pg_temp.candidate_register_all_final_components(v_workflow,2,'standalone',
    (v_response->>'approved_at_utc')::timestamptz);
  if exists(select 1 from public.candidate_submission_components
    where workflow_id=v_workflow and workflow_generation=2 and required
      and (review_render_state<>'READY' or final_signed_render_state<>'READY'
        or review_render_input_sha256 is distinct from final_signed_render_input_sha256
        or review_content_sha256=final_signed_content_sha256)) then
    raise exception 'standalone expense review/final page equality contract failed';
  end if;
  v_row_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_anchor_timesheet,v_week,false)->>'row_signature';
  v_response:=public.candidate_submission_finalize_atomic_v1(
    v_session,'TEST',v_workflow,2,v_row_signature,'standalone:finalise',now());
  v_target_timesheet:=(v_response->>'timesheet_id')::uuid;
  if v_response->>'state'<>'FINALISED' or v_target_timesheet is null
     or v_target_timesheet=v_anchor_timesheet then
    raise exception 'standalone expense did not finalise onto a separate carrier: %',v_response;
  end if;
  if (select other_pay_ex_vat from public.timesheets_financials
      where timesheet_id=v_anchor_timesheet and is_current)=10 then
    raise exception 'standalone expense changed the original hours record';
  end if;
  if not exists(select 1 from public.timesheets_financials
      where timesheet_id=v_target_timesheet and is_current and other_pay_ex_vat=10)
     or exists(select 1 from public.timesheet_evidence
      where timesheet_id=v_target_timesheet and kind='TIMESHEET')
     or (select count(*) from public.timesheet_evidence
      where timesheet_id=v_target_timesheet)<>2 then
    raise exception 'standalone expense carrier/evidence materialisation failed' using
      detail=jsonb_build_object(
        'target_timesheet_id',v_target_timesheet,
        'financials',(select to_jsonb(f) from public.timesheets_financials f
          where f.timesheet_id=v_target_timesheet and f.is_current limit 1),
        'evidence',(select coalesce(jsonb_agg(to_jsonb(e)),'[]'::jsonb)
          from public.timesheet_evidence e where e.timesheet_id=v_target_timesheet)
      )::text;
  end if;
end;
$standalone_expense$;

do $combined_submission$
declare
  v_client uuid:='92000000-0000-0000-0000-000000000001';
  v_candidate uuid:='92000000-0000-0000-0000-000000000002';
  v_contract uuid:='92000000-0000-0000-0000-000000000003';
  v_week uuid:='92000000-0000-0000-0000-000000000004';
  v_timesheet uuid:='92000000-0000-0000-0000-000000000005';
  v_actor uuid:='92000000-0000-0000-0000-000000000006';
  v_account uuid:='92000000-0000-0000-0000-000000000007';
  v_session uuid:='92000000-0000-0000-0000-000000000008';
  v_workflow uuid:='92000000-0000-0000-0000-000000000009';
  v_signature uuid;
  v_fresh_signature uuid;
  v_evidence uuid;
  v_response jsonb;
  v_row_signature text;
  v_amended_submission jsonb;
  v_old_request uuid;
begin
  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;
  insert into public.clients(id) values(v_client);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'combined@example.test',true,'GCK-COMBINED');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_expenses_require_separate_timesheet
  ) values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',false);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    r2_nurse_key,r2_auth_key
  ) values(
    v_timesheet,v_contract,current_date,'HOURS','ELECTRONIC',
    'existing/candidate-signature','existing/manager-signature'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'OPEN','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status
  ) values(v_timesheet,v_candidate,v_client,0,'UNPROCESSED');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','combined@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('93',32),'hex'),
    now()+interval '30 days',now()+interval '90 days');

  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CREATE',1,jsonb_build_object(
      'workflow_kind','CONTRACT_COMBINED','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_week,
      'anchor_timesheet_id',v_timesheet,'target_timesheet_id',v_timesheet,
      'week_ending_date',current_date
    ),'combined:create',now());
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,jsonb_build_object(
      'component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE',
      'storage_key','combined/source/candidate-signature.png','media_type','image/png','byte_size',512
    ),'combined:signature:prepare',now());
  v_signature:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,jsonb_build_object(
      'component_id',v_signature,'source_content_sha256_hex',repeat('94',32),
      'verified_byte_size',512,'verified_media_type','image/png'
    ),'combined:signature:complete',now());
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',1,jsonb_build_object(
      'component_kind','EXPENSE_EVIDENCE','expense_category','OTHER',
      'document_role','SOURCE_EVIDENCE','storage_key','combined/source/receipt.png',
      'media_type','image/png','byte_size',1024
    ),'combined:evidence:prepare',now());
  v_evidence:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,jsonb_build_object(
      'component_id',v_evidence,'source_content_sha256_hex',repeat('95',32),
      'verified_byte_size',1024,'verified_media_type','image/png'
    ),'combined:evidence:complete',now());
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'WORKER_SUBMIT',1,jsonb_build_object(
      'candidate_signature_component_id',v_signature,'candidate_signed_at_utc',now(),
      'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
      'immutable_submission',jsonb_build_object(
        'hours_submission',jsonb_build_object(
          'canonical_tsfin_snapshot',jsonb_build_object(
            'candidate_id',v_candidate,'client_id',v_client,'total_hours',8,
            'hours_day',8,'total_pay_ex_vat',80,'total_charge_ex_vat',100,'margin_ex_vat',20
          ),
          'timesheet_patch_json',jsonb_build_object(
            'actual_schedule_json',jsonb_build_array(jsonb_build_object(
              'date',current_date,'start','09:00','end','17:30','break_minutes',30
            )),
            'additional_units_week',jsonb_build_object('ON_CALL',1)
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
          'evidence_display_name','Combined claim evidence'
        )
      )
    ),'combined:submit',now());
  if (select count(*) from public.candidate_submission_components
      where workflow_id=v_workflow and workflow_generation=2 and required)<>3
     or not exists(select 1 from public.candidate_submission_components
      where workflow_id=v_workflow and workflow_generation=2
        and component_kind='HOURS_TIMESHEET' and review_ordinal=1)
     or not exists(select 1 from public.candidate_submission_components
      where workflow_id=v_workflow and workflow_generation=2
        and component_kind='EXPENSE_SUMMARY' and review_ordinal=2)
     or not exists(select 1 from public.candidate_submission_components
      where workflow_id=v_workflow and workflow_generation=2
        and component_kind='EXPENSE_EVIDENCE' and review_ordinal=3
        and source_component_id=v_evidence) then
    raise exception 'combined required-page manifest/order was incorrect: %',v_response;
  end if;
  perform pg_temp.candidate_register_all_review_components(v_workflow,2,'combined',now());

  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'SELECT_PHONE_APPROVAL',2,
    jsonb_build_object(
      'approval_token_hash_hex',encode(extensions.digest(v_workflow::text||':first-phone','sha256'),'hex'),
      'expires_at_utc',now()+interval '30 minutes'
    ),
    'combined:first-phone-select',now());
  select id into v_old_request from public.candidate_approval_requests
  where workflow_id=v_workflow and workflow_generation=2 and state='PENDING';
  v_amended_submission:=jsonb_set(
    (select immutable_submission_json from public.candidate_submission_workflows where id=v_workflow),
    '{hours_submission,timesheet_patch_json,additional_units_week,ON_CALL}','2'::jsonb,true
  );
  select id into v_signature from public.candidate_submission_components
  where workflow_id=v_workflow and workflow_generation=2
    and component_kind='CANDIDATE_SIGNATURE' and state='IMMUTABLE';
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'AMEND',2,
    jsonb_build_object('input_snapshot',v_amended_submission),
    'combined:amend-to-draft',now());
  if v_response->>'state'<>'WORKER_DRAFT' or v_response->>'generation'<>'3' then
    raise exception 'combined amendment did not create a new draft: %',v_response;
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'WORKER_SUBMIT',3,jsonb_build_object(
        'candidate_signature_component_id',v_signature,'candidate_signed_at_utc',now(),
        'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
        'immutable_submission',v_amended_submission
      ),'combined:amend-with-stale-signature',now());
    raise exception 'changed hours accepted a stale candidate signature';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_SIGNATURE_REQUIRED_AFTER_AMENDMENT' then raise; end if;
  end;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_PREPARE',3,jsonb_build_object(
      'component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE',
      'storage_key','combined/source/candidate-signature-amended.png',
      'media_type','image/png','byte_size',512
    ),'combined:amended-signature:prepare',now());
  v_fresh_signature:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',3,jsonb_build_object(
      'component_id',v_fresh_signature,'source_content_sha256_hex',repeat('96',32),
      'verified_byte_size',512,'verified_media_type','image/png'
    ),'combined:amended-signature:complete',now());
  v_response:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'WORKER_SUBMIT',3,jsonb_build_object(
      'candidate_signature_component_id',v_fresh_signature,'candidate_signed_at_utc',now(),
      'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
      'immutable_submission',v_amended_submission
    ),'combined:amend',now());
  if v_response->>'generation'<>'4'
     or (select state from public.candidate_approval_requests where id=v_old_request)<>'SUPERSEDED'
     or exists(select 1 from public.candidate_submission_components
       where workflow_id=v_workflow and workflow_generation=2 and required and state<>'SUPERSEDED')
     or (select count(*) from public.candidate_submission_components
       where workflow_id=v_workflow and workflow_generation=4 and required)<>3
     or not exists(select 1 from public.candidate_submission_components
       where workflow_id=v_workflow and workflow_generation=4
         and component_kind='EXPENSE_EVIDENCE' and source_component_id=v_evidence) then
    raise exception 'combined amendment did not carry immutable evidence or supersede stale authority: %',v_response;
  end if;
  perform pg_temp.candidate_register_all_review_components(v_workflow,4,'combined-amended',now());
  v_response:=pg_temp.candidate_phone_approve_all(
    v_session,v_workflow,4,'combined-amended','combined-manager-signature',now());
  perform pg_temp.candidate_register_all_final_components(v_workflow,4,'combined-amended',
    (v_response->>'approved_at_utc')::timestamptz);
  v_row_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet,v_week,false)->>'row_signature';
  v_response:=public.candidate_submission_finalize_atomic_v1(
    v_session,'TEST',v_workflow,4,v_row_signature,'combined:finalise',now());
  if v_response->>'state'<>'FINALISED' or (v_response->>'timesheet_id')::uuid<>v_timesheet
     or not exists(select 1 from public.timesheets_financials
       where timesheet_id=v_timesheet and is_current and total_hours=8 and other_pay_ex_vat=10)
     or not exists(select 1 from public.timesheets
       where timesheet_id=v_timesheet and submission_mode='ELECTRONIC'
         and r2_nurse_key is not null and r2_auth_key is not null)
     or (select count(*) from public.timesheet_evidence where timesheet_id=v_timesheet)<>3
     or not exists(select 1 from public.timesheet_evidence
       where timesheet_id=v_timesheet and kind='TIMESHEET' and document_role='SIGNED_TIMESHEET') then
    raise exception 'combined hours/expense atomic finalisation failed: %',v_response;
  end if;
end;
$combined_submission$;

rollback;
