\set ON_ERROR_STOP on

-- Present in TEST; the compact local compile fixture keeps a shorter enum.
alter type public.ts_fin_reason_enum add value if not exists 'REVOKED';

begin;

-- The compact Candidate compile fixture omits two legacy financial audit
-- columns that the installed QR reset authority clears. Add them only inside
-- this rolled-back runtime fixture so the rejection path matches TEST.
alter table public.timesheets_financials
  add column if not exists paid_by_user_id uuid,
  add column if not exists payment_reference text;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
      'candidate_app_reads',true,
      'candidate_app_writes',true,
      'candidate_notifications',true,
      'candidate_paper_qr',true
    ),
    candidate_app_environment='TEST'
where id=1;

create or replace function pg_temp.verify_finalised_paper_rejection(
  p_separate_expense boolean,
  p_mail_status text,
  p_active_lease boolean,
  p_expect_block boolean
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
  v_target_timesheet uuid;
  v_target_week uuid;
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_mail uuid:=gen_random_uuid();
  v_ready_notification uuid:=gen_random_uuid();
  v_booking text:='FINALISED-PAPER-'||replace(gen_random_uuid()::text,'-','');
  v_target_booking text;
  v_qr_token text:='candidate-paper-token-'||gen_random_uuid()::text;
  v_signature text;
  v_result jsonb;
  v_replay jsonb;
  v_new_timesheet uuid;
  v_page jsonb;
  v_card jsonb;
  v_blocked boolean:=false;
begin
  if p_mail_status not in ('QUEUED','SENT') then
    raise exception 'Unsupported finalised PAPER test mail status: %',p_mail_status;
  end if;
  if p_active_lease and p_mail_status='SENT' then
    raise exception 'SENT fixture cannot own an active provider lease';
  end if;
  if p_expect_block is distinct from p_active_lease then
    raise exception 'Expected block must match active provider lease fixture';
  end if;
  v_target_timesheet:=v_timesheet;
  v_target_week:=v_week;
  v_target_booking:=v_booking;

  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;
  insert into public.candidates(id,email,active)
  values(v_candidate,'paper-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'Finalised PAPER rejection client '||left(v_client::text,8));
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,current_date-1,'MANUAL',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'MANUAL'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,qr_status,qr_token,qr_payload_json,qr_generated_at
  ) values(
    v_timesheet,v_contract,v_booking,current_date,'HOURS','WEEKLY','MANUAL',
    'PENDING',v_qr_token,jsonb_build_object('workflow_id',v_workflow),v_now
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'UNPROCESSED',8);
  if p_separate_expense then
    v_target_timesheet:=gen_random_uuid();
    v_target_week:=gen_random_uuid();
    v_target_booking:='FINALISED-PAPER-EXPENSE-'||replace(gen_random_uuid()::text,'-','');
    insert into public.timesheets(
      timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
      submission_mode,is_adjustment,parent_timesheet_id
    ) values(
      v_target_timesheet,v_contract,v_target_booking,current_date,
      'EXPENSES','WEEKLY','MANUAL',true,v_timesheet
    );
    insert into public.contract_weeks(
      id,contract_id,week_ending_date,additional_seq,status,
      submission_mode_snapshot,timesheet_id,is_adjustment
    ) values(
      v_target_week,v_contract,current_date,1,'SUBMITTED','MANUAL',
      v_target_timesheet,true
    );
    insert into public.timesheets_financials(
      timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours,
      expenses_pay_ex_vat,expenses_charge_ex_vat,other_pay_ex_vat,other_charge_ex_vat
    ) values(
      v_target_timesheet,v_candidate,v_client,true,'UNPROCESSED',0,25,30,25,30
    );
  end if;
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','paper-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(convert_to(v_session::text,'UTF8'),'sha256'),
    v_now+interval '1 day',v_now+interval '7 days'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,finalised_at_utc,
    paper_return_manifest_json,paper_return_manifest_sha256,renderer_contract_version
  ) values(
    v_workflow,'TEST',v_account,v_candidate,
    case when p_separate_expense then 'CONTRACT_EXPENSE' else 'CONTRACT_HOURS' end,
    'WEEKLY','PAPER','FINALISED',2,
    v_contract,v_target_week,v_timesheet,v_target_timesheet,current_date,
    'finalised-paper:'||v_workflow::text,v_now,
    jsonb_build_object('workflow_id',v_workflow,'workflow_generation',1,
      'pages',jsonb_build_array(jsonb_build_object(
        'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))),
    private._candidate_sha256_jsonb_v1(jsonb_build_object(
      'workflow_id',v_workflow,'workflow_generation',1,
      'pages',jsonb_build_array(jsonb_build_object(
        'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET')))),
    'CANDIDATE_REVIEW_DOCUMENTS_V1'
  );
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json,
    attempt_lease_token,attempt_leased_at_utc,attempt_lease_expires_at_utc
  ) values(
    v_mail,'TIMESHEET_QR','paper@example.test','Candidate PAPER pack','Pack ready',
    jsonb_build_array(jsonb_build_object('r2_key','candidate-app/test/finalised-paper/pack.pdf')),
    p_mail_status::public.mail_status_enum,v_now,'timesheets',v_timesheet,v_now,v_now,
    'finalised-paper-mail:'||v_workflow::text,
    jsonb_build_object(
      'candidate_workflow_id',v_workflow,
      'candidate_workflow_generation',1,
      'candidate_paper_pack_ready',true,
      'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(convert_to(v_qr_token,'UTF8'),'sha256'),'hex')
    ),
    case when p_active_lease then 'active-provider-lease' else null end,
    case when p_active_lease then v_now else null end,
    case when p_active_lease then v_now+interval '5 minutes' else null end
  );
  insert into public.candidate_notifications(
    id,account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
    template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
  ) values(
    v_ready_notification,v_account,v_candidate,v_workflow,v_timesheet,
    'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1',
    jsonb_build_object('workflow_generation',1),
    jsonb_build_object('type','paper_pack','workflow_id',v_workflow,'generation',1),
    'UNREAD','PENDING',
    'CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow::text||':1:finalised-paper',v_now
  );

  v_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_target_timesheet,v_target_week,false
  )->>'row_signature';
  begin
    v_result:=public.candidate_submission_reject_atomic_v1(
      v_actor,'TEST',v_target_timesheet,v_target_timesheet,v_signature,
      'Finalised PAPER submission rejected',
      'finalised-paper-reject:'||v_workflow::text,v_now
    );
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS' then raise; end if;
    v_blocked:=true;
  end;

  if p_expect_block then
    if not v_blocked then
      raise exception 'Finalised PAPER rejection ignored active provider lease';
    end if;
    if (select state from public.candidate_submission_workflows where id=v_workflow)<>'FINALISED'
       or (select generation from public.candidate_submission_workflows where id=v_workflow)<>2
       or (select timesheet_id from public.contract_weeks where id=v_target_week)<>v_target_timesheet
       or (select count(*) from public.timesheets where booking_id=v_target_booking)<>1
       or (select qr_token from public.timesheets where timesheet_id=v_timesheet)<>v_qr_token then
      raise exception 'Blocked finalised PAPER rejection partially mutated lifecycle';
    end if;
    if (select state from public.candidate_notifications where id=v_ready_notification)<>'UNREAD'
       or (select push_state from public.candidate_notifications where id=v_ready_notification)<>'PENDING'
       or coalesce((select (deep_link_json->>'obsolete')::boolean
                    from public.candidate_notifications where id=v_ready_notification),false) then
      raise exception 'Blocked finalised PAPER rejection altered readiness notification';
    end if;
    return;
  end if;

  if v_blocked then
    raise exception 'Unleased finalised PAPER rejection was unexpectedly blocked';
  end if;
  v_new_timesheet:=nullif(v_result->>'timesheet_id','')::uuid;
  if v_new_timesheet is null or v_new_timesheet=v_target_timesheet then
    raise exception 'Finalised PAPER rejection did not rotate current timesheet: %',v_result;
  end if;
  if (select state from public.candidate_submission_workflows where id=v_workflow)<>'REJECTED'
     or (select generation from public.candidate_submission_workflows where id=v_workflow)<>3 then
    raise exception 'Finalised PAPER workflow was not rejected exactly once';
  end if;
  if p_mail_status='SENT' then
    if (select status from public.mail_outbox where id=v_mail)<>'SENT'
       or coalesce((select (payment_scope_json->>'candidate_paper_generation_retired')::boolean
                    from public.mail_outbox where id=v_mail),false) then
      raise exception 'SENT finalised PAPER mail was not preserved as immutable history';
    end if;
  else
    if not coalesce((select (payment_scope_json->>'candidate_paper_generation_retired')::boolean
                     from public.mail_outbox where id=v_mail),false)
       or (select attachments from public.mail_outbox where id=v_mail)<>'[]'::jsonb
       or (select scheduled_for_utc from public.mail_outbox where id=v_mail)<>'infinity'::timestamptz
       or (select next_attempt_at_utc from public.mail_outbox where id=v_mail)<>'infinity'::timestamptz then
      raise exception 'Unsent finalised PAPER mail generation was not made inert';
    end if;
  end if;
  if (select state from public.candidate_notifications where id=v_ready_notification)<>'DISMISSED'
     or (select push_state from public.candidate_notifications where id=v_ready_notification)<>'SKIPPED'
     or not coalesce((select (deep_link_json->>'obsolete')::boolean
                      from public.candidate_notifications where id=v_ready_notification),false) then
    raise exception 'Finalised PAPER readiness notification remained current-looking';
  end if;
  if (select count(*) from public.candidate_notifications
      where workflow_id=v_workflow and event_type='OFFICE_REJECTED')<>1 then
    raise exception 'Finalised PAPER rejection did not create exactly one OFFICE_REJECTED notification';
  end if;
  if (select qr_token from public.timesheets where timesheet_id=v_timesheet) is not null then
    raise exception 'Frozen PAPER delivery QR source token remained active';
  end if;
  if p_separate_expense
     and (select timesheet_id from public.contract_weeks where id=v_week)<>v_timesheet then
    raise exception 'Separate-expense PAPER rejection rotated the hours anchor economics';
  end if;

  v_page:=public.candidate_app_timesheet_page_v1(v_session,'TEST',null,50,v_now);
  select item.value into v_card
  from jsonb_array_elements(v_page->'items') item(value)
  where item.value->>'timesheet_id'=(case when p_separate_expense
    then v_timesheet else v_new_timesheet end)::text
  limit 1;
  if v_card->>'candidate_status_code'<>'REJECTED'
     or v_card#>>'{rejection,required_action}'<>(case when p_separate_expense
       then 'RESUBMIT_EXPENSE_CLAIM' else 'RESUBMIT_TIMESHEET' end) then
    raise exception 'Finalised PAPER rejection was not projected on replacement card: %',v_card;
  end if;

  v_replay:=public.candidate_submission_reject_atomic_v1(
    v_actor,'TEST',v_target_timesheet,v_target_timesheet,v_signature,
    'Finalised PAPER submission rejected',
    'finalised-paper-reject:'||v_workflow::text,v_now
  );
  if not coalesce((v_replay->>'idempotent_replay')::boolean,false)
     or (select count(*) from public.candidate_notifications
         where workflow_id=v_workflow and event_type='OFFICE_REJECTED')<>1
     or (select count(*) from public.timesheets where booking_id=v_target_booking)<>2 then
    raise exception 'Finalised PAPER rejection replay was not idempotent: %',v_replay;
  end if;
end;
$function$;

select pg_temp.verify_finalised_paper_rejection(false,'QUEUED',false,false);
select pg_temp.verify_finalised_paper_rejection(false,'QUEUED',true,true);
select pg_temp.verify_finalised_paper_rejection(false,'SENT',false,false);
select pg_temp.verify_finalised_paper_rejection(true,'QUEUED',false,false);
select pg_temp.verify_finalised_paper_rejection(true,'QUEUED',true,true);
select pg_temp.verify_finalised_paper_rejection(true,'SENT',false,false);

rollback;
