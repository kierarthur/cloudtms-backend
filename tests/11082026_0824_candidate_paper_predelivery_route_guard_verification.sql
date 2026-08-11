\set ON_ERROR_STOP on

-- Candidate PAPER pre-delivery route guard. Disposable PostgreSQL only;
-- every synthetic row and feature change is rolled back.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
      'candidate_app_writes',true,
      'candidate_paper_qr',true,
      'candidate_notifications',true,
      'candidate_route_confirmation',true
    ),
    candidate_app_environment='TEST'
where id=1;

create or replace function pg_temp.verify_predelivery_paper_route_guard(
  p_expense_state text
)
returns void
language plpgsql
as $function$
declare
  v_now timestamptz:=clock_timestamp();
  v_actor uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_hours_workflow uuid:=gen_random_uuid();
  v_expense_workflow uuid:=gen_random_uuid();
  v_hours_mail uuid:=gen_random_uuid();
  v_current_token text:='predelivery-hours-token-'||gen_random_uuid()::text;
  v_hours_manifest jsonb;
  v_hours_manifest_hash text;
  v_source_context jsonb;
  v_context jsonb;
  v_result jsonb;
  v_manual uuid;
  v_action text;
begin
  if p_expense_state not in ('WORKER_DRAFT','WORKER_SUBMITTED') then
    raise exception 'Invalid pre-delivery route fixture state';
  end if;
  v_hours_manifest:=jsonb_build_object(
    'workflow_id',v_hours_workflow,'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  );
  v_hours_manifest_hash:=encode(
    private._candidate_sha256_jsonb_v1(v_hours_manifest),'hex'
  );

  insert into public.tms_users(id) values(v_actor);
  insert into public.candidates(id,email,active)
  values(v_candidate,'predelivery-route-'||replace(v_candidate::text,'-','')
    ||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'PAPER pre-delivery route client '||left(v_client::text,8));
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,current_date-1,'MANUAL',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode,overrideclientsettings
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'MANUAL',true);
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,qr_status,qr_token,qr_payload_json,qr_generated_at,
    qr_last_sent_hash,qr_last_sent_at_utc
  ) values(v_timesheet,v_contract,
    'PAPER-PREDELIVERY-ROUTE-'||replace(v_timesheet::text,'-',''),
    current_date,'HOURS','WEEKLY','MANUAL','PENDING',v_current_token,'{}',v_now,
    'predelivery-hours-pack-sent',v_now);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,basis,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','predelivery-route-'||replace(v_candidate::text,'-','')
    ||'@example.test','ACTIVE');

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,finalised_at_utc,
    paper_return_manifest_json,paper_return_manifest_sha256,renderer_contract_version
  ) values(
    v_hours_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    'FINALISED',2,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'predelivery-hours:'||v_hours_workflow::text,v_now,
    v_hours_manifest,decode(v_hours_manifest_hash,'hex'),
    'CANDIDATE_REVIEW_DOCUMENTS_V1'
  ),(
    v_expense_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','PAPER',
    p_expense_state,1,v_contract,v_week,v_timesheet,null,current_date,
    'predelivery-expense:'||v_expense_workflow::text,null,
    null,null,'CANDIDATE_REVIEW_DOCUMENTS_V1'
  );
  update public.timesheets set
    candidate_workflow_id=v_hours_workflow,candidate_workflow_generation=2
  where timesheet_id=v_timesheet;

  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json
  ) values(
    v_hours_mail,'TIMESHEET_QR','predelivery-hours@example.test',
    'Historical hours pack','Historical hours pack',
    jsonb_build_array(jsonb_build_object(
      'r2_key','candidate-app/test/predelivery/hours.pdf')),
    'SENT',v_now,'timesheets',v_timesheet,v_now,v_now,
    'predelivery-hours-mail:'||v_hours_workflow::text,
    jsonb_build_object(
      'candidate_workflow_id',v_hours_workflow,
      'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_hours_manifest_hash,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(
        convert_to(v_current_token,'UTF8'),'sha256'),'hex')
    )
  );

  v_source_context:=private._candidate_paper_source_workflow_context_v1(v_timesheet);
  if v_source_context->>'selected_workflow_id'<>v_hours_workflow::text
     or v_source_context->>'current_token_owner_workflow_id'<>v_hours_workflow::text
     or (v_source_context->>'affected_nonterminal_workflow_count')::integer<>1
     or not coalesce((v_source_context->>'identity_conflict')::boolean,false)
     or v_source_context->>'conflict_reason'<>
       'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT'
     or not exists(
       select 1
       from jsonb_array_elements(
         v_source_context->'affected_nonterminal_workflows'
       ) affected
       where affected->>'workflow_id'=v_expense_workflow::text
         and affected->>'state'=p_expense_state
     ) then
    raise exception 'Pre-delivery workflow was not represented as a route conflict: %',
      v_source_context;
  end if;

  -- Only actions that retain/reissue QR remain blocked.  A switch to MANUAL is
  -- allowed after the office explicitly confirms removal of the incomplete
  -- standalone expense claim.
  foreach v_action in array array[
    'INVALIDATE_QR','REISSUE_QR'
  ] loop
    v_context:=public.timesheet_route_version_preview_v1(v_timesheet,v_action);
    if v_context->>'warning_code'<>'ROUTE_CHANGE_WORKFLOW_CONFLICT'
       or v_context->>'block_reason'<>
         'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT'
       or coalesce((v_context->>'permitted_action')::boolean,false)
       or (v_context->>'paper_source_affected_nonterminal_workflow_count')::integer<>1
       or nullif(v_context->>'paper_workflow_id','') is not null then
      raise exception 'Source-rotating action did not fail closed for % / %: %',
        p_expense_state,v_action,v_context;
    end if;
  end loop;

  foreach v_action in array array['CONVERT_QR_TO_MANUAL','DISABLE_QR'] loop
    v_context:=public.timesheet_route_version_preview_v1(v_timesheet,v_action);
    if v_context->>'warning_code'<>
         'CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVE_CONFIRM'
       or not coalesce((v_context->>'permitted_action')::boolean,false)
       or not coalesce(
         (v_context->>'incomplete_expense_claim_removal_required')::boolean,false
       )
       or v_context->>'incomplete_expense_workflow_id'<>v_expense_workflow::text then
      raise exception 'MANUAL action did not request incomplete claim removal for % / %: %',
        p_expense_state,v_action,v_context;
    end if;
  end loop;

  v_context:=public.timesheet_route_version_preview_v1(v_timesheet,'CONVERT_QR_TO_MANUAL');
  v_result:=public.timesheet_route_version_confirmed_v1(
    v_timesheet,v_timesheet,v_context->>'row_signature',
    v_context->>'context_sha256','CONVERT_QR_TO_MANUAL',v_actor,
    'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',null,
    'predelivery-route-confirm:'||v_expense_workflow::text,false,v_now
  );
  v_manual:=nullif(v_result->>'new_timesheet_id','')::uuid;

  if v_manual is null
     or (select state from public.candidate_submission_workflows
         where id=v_hours_workflow)<>'FINALISED'
     or (select state from public.candidate_submission_workflows
         where id=v_expense_workflow)<>'SUPERSEDED'
     or (select anchor_timesheet_id from public.candidate_submission_workflows
         where id=v_expense_workflow)<>v_timesheet
     or (select qr_token from public.timesheets where timesheet_id=v_timesheet) is not null
     or (select is_current from public.timesheets where timesheet_id=v_timesheet)
     or not (select is_current from public.timesheets where timesheet_id=v_manual)
     or (select count(*) from public.timesheets
          where booking_id=(select booking_id from public.timesheets
            where timesheet_id=v_timesheet))<>2
     or (select status from public.mail_outbox where id=v_hours_mail)<>'SENT'
     or coalesce((select
        (payment_scope_json->>'candidate_paper_generation_retired')::boolean
        from public.mail_outbox where id=v_hours_mail),false)
     or not coalesce(
       (v_result#>>'{workflow_retirement,primary_workflow_retirement,paper_delivery_retirement,qr_invalidation_proven}')::boolean,
       false
     )
     or not coalesce(
       (v_result#>>'{workflow_retirement,incomplete_expense_claim_removed}')::boolean,false
     ) then
    raise exception 'Confirmed QR incomplete-claim removal was incomplete: %',v_result;
  end if;
end;
$function$;

select pg_temp.verify_predelivery_paper_route_guard('WORKER_DRAFT');
select pg_temp.verify_predelivery_paper_route_guard('WORKER_SUBMITTED');

rollback;
