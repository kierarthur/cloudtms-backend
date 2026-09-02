\set ON_ERROR_STOP on

-- Exact legacy one-page PAPER cancellation compatibility. Every fixture row
-- and setting change is rollback-only; no hosted or user data is used.
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

create or replace function pg_temp.verify_legacy_orphan_cancel(
  p_manifest_version text,
  p_matching_receipt_token boolean,
  p_expect_success boolean,
  p_expect_compatibility boolean
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
  v_session uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_mail uuid:=gen_random_uuid();
  v_second_mail uuid:=gen_random_uuid();
  v_manifest jsonb;
  v_manifest_hash text;
  v_source_token text:='legacy-orphan-current-'||gen_random_uuid()::text;
  v_receipt_token text;
  v_booking_id text:='LEGACY-ORPHAN-'||replace(gen_random_uuid()::text,'-','');
  v_result jsonb;
  v_failed boolean:=false;
  v_failed_message text;
  v_current_count integer;
  v_financial_count integer;
begin
  v_receipt_token:=case when p_matching_receipt_token
    then v_source_token
    else 'legacy-orphan-receipt-'||gen_random_uuid()::text
  end;
  v_manifest:=jsonb_build_object(
    'workflow_id',v_workflow,
    'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'
    ))
  );
  if nullif(btrim(coalesce(p_manifest_version,'')),'') is not null then
    v_manifest:=v_manifest||jsonb_build_object('manifest_version',p_manifest_version);
  end if;
  v_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_manifest),'hex');

  if exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='tms_users'
      and column_name='password_hash'
  ) then
    execute $sql$
      insert into public.tms_users(id,email,password_hash,role,is_active)
      values($1,$2,'UNUSABLE_VERIFICATION_ONLY','admin',true)
    $sql$ using v_actor,
      'legacy-orphan-actor-'||replace(v_actor::text,'-','')||'@example.test';
  else
    insert into public.tms_users(id,email,is_active)
    values(
      v_actor,
      'legacy-orphan-actor-'||replace(v_actor::text,'-','')||'@example.test',
      true
    );
  end if;
  update public.settings_defaults
  set candidate_app_system_actor_user_id=v_actor
  where id=1;
  insert into public.candidates(id,email,active)
  values(
    v_candidate,
    'legacy-orphan-'||replace(v_candidate::text,'-','')||'@example.test',
    true
  );
  insert into public.clients(id,name)
  values(v_client,'Legacy orphan client '||left(v_client::text,8));
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,current_date-1,'MANUAL',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,pay_method_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'PAYE','MANUAL'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,occupant_key_norm,hospital_norm,
    ward_norm,job_title_norm,week_ending_date,line_type,sheet_scope,
    submission_mode,qr_status,qr_token,qr_payload_json,qr_generated_at,document_state
  ) values(
    v_timesheet,v_contract,v_booking_id,
    'GCK-LEGACY-'||replace(v_candidate::text,'-',''),
    'LEGACY ORPHAN HOSPITAL','LEGACY ORPHAN WARD','LEGACY ORPHAN ROLE',
    current_date,'HOURS','WEEKLY',
    'MANUAL','PENDING',v_source_token,
    jsonb_build_object('v',1,'tok',v_source_token),v_now,'READY'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,is_current,basis,
    processing_status,total_hours
  ) values(
    v_timesheet,1,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8
  );
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(
    v_account,'TEST',
    'legacy-orphan-'||replace(v_candidate::text,'-','')||'@example.test',
    'ACTIVE'
  );
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(convert_to(v_session::text,'UTF8'),'sha256'),
    v_now+interval '1 day',v_now+interval '7 days'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,idempotency_key,
    paper_return_manifest_json,paper_return_manifest_sha256,
    renderer_contract_version
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',1,v_contract,v_week,v_timesheet,v_timesheet,
    current_date,'legacy-orphan-workflow:'||v_workflow::text,
    v_manifest,decode(v_manifest_hash,'hex'),'CANDIDATE_REVIEW_DOCUMENTS_V1'
  );
  update public.timesheets
  set candidate_workflow_id=v_workflow,candidate_workflow_generation=1
  where timesheet_id=v_timesheet;
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    sent_at,context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json
  ) values(
    v_mail,'TIMESHEET_QR','legacy-orphan@example.test','Legacy signed pack',
    'Historical one-page pack',
    jsonb_build_array(jsonb_build_object(
      'r2_key','candidate-app/test/legacy-orphan/pack.pdf'
    )),
    'SENT',v_now,v_now,'timesheets',v_timesheet,v_now,v_now,
    'legacy-orphan-mail:'||v_workflow::text,
    jsonb_build_object(
      'candidate_mail_authority','CANDIDATE_PAPER_V1',
      'candidate_workflow_id',v_workflow,
      'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_manifest_hash,
      'candidate_paper_pack_ready',true,
      'mail_held_until_pdf_rendered',false,
      'candidate_paper_generation_retired',false,
      'qr_token_hash',encode(extensions.digest(
        convert_to(v_receipt_token,'UTF8'),'sha256'
      ),'hex')
    )
  );

  -- Modern manifest V2 may be delivered again without changing its immutable
  -- workflow generation. The newest delivery has a different legacy source
  -- token receipt; cancellation must retire the whole generation while proving
  -- the current source token belongs to one of the exact receipts.
  if p_manifest_version='2' and p_matching_receipt_token then
    insert into public.mail_outbox(
      id,type,"to",subject,body_text,attachments,status,created_at_utc,
      sent_at,context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
      deterministic_outbox_key,payment_scope_json
    ) values(
      v_second_mail,'TIMESHEET_QR','legacy-orphan@example.test','Redelivered signed pack',
      'Modern page-manifest redelivery',
      jsonb_build_array(jsonb_build_object(
        'r2_key','candidate-app/test/legacy-orphan/pack-redelivery.pdf'
      )),
      'SENT',v_now+interval '1 second',v_now+interval '1 second',
      'timesheets',v_timesheet,v_now,v_now,
      'legacy-orphan-mail-redelivery:'||v_workflow::text,
      jsonb_build_object(
        'candidate_mail_authority','CANDIDATE_PAPER_V1',
        'candidate_workflow_id',v_workflow,
        'candidate_workflow_generation',1,
        'paper_return_manifest_sha256',v_manifest_hash,
        'candidate_paper_manifest_version',2,
        'candidate_paper_pack_ready',true,
        'mail_held_until_pdf_rendered',false,
        'candidate_paper_generation_retired',false,
        'qr_token_hash',encode(extensions.digest(
          convert_to('modern-redelivery-'||v_second_mail::text,'UTF8'),'sha256'
        ),'hex')
      )
    );
  end if;

  begin
    v_result:=public.candidate_workflow_cancel_atomic_v2(
      v_session,'TEST',v_workflow,1,
      jsonb_build_object('reason_note','Retire the historical one-page pack.'),
      'legacy-orphan-cancel:'||v_workflow::text,v_now
    );
  exception when sqlstate '40001' then
    v_failed:=true;
    v_failed_message:=sqlerrm;
  end;

  if p_expect_success then
    if v_failed
       or v_result->>'state'<>'CANCELLED'
       or (select state from public.candidate_submission_workflows
           where id=v_workflow)<>'CANCELLED'
       or (select generation from public.candidate_submission_workflows
           where id=v_workflow)<>2
       or (select qr_token from public.timesheets
           where timesheet_id=v_timesheet) is not null
       or (select status from public.mail_outbox where id=v_mail)<>'SENT'
       or (p_manifest_version='2' and p_matching_receipt_token
           and (select status from public.mail_outbox where id=v_second_mail)<>'SENT')
       or coalesce((select (payment_scope_json->>'candidate_paper_generation_retired')::boolean
                    from public.mail_outbox where id=v_mail),false) then
      raise exception 'Exact legacy one-page orphan cancellation failed: result=%, failure=%',
        v_result,v_failed_message;
    end if;

    select count(*)::integer into v_current_count
    from public.timesheets current_timesheet
    where current_timesheet.booking_id=v_booking_id
      and current_timesheet.is_current=true
      and current_timesheet.archived_at_utc is null;
    select count(*)::integer into v_financial_count
    from public.timesheets_financials financial_row
    where financial_row.timesheet_id in (
      select family_timesheet.timesheet_id
      from public.timesheets family_timesheet
      where family_timesheet.booking_id=v_booking_id
    );
    if v_current_count<>0 or v_financial_count<>1
       or (select status from public.contract_weeks where id=v_week)<>'OPEN'
       or (select timesheet_id from public.contract_weeks where id=v_week) is not null then
      raise exception 'Cancellation did not return the Weekly claim to its normal open Contract Week with retained history: current=%, financial=%',
        v_current_count,v_financial_count;
    end if;
    if p_expect_compatibility and not exists(
         select 1 from public.audit_events event_row
         where event_row.object_type='candidate_submission_workflow'
           and event_row.object_id_text=v_workflow::text
           and event_row.action='CANDIDATE_LEGACY_PAPER_ORPHAN_PREPARED_FOR_CANCELLATION'
       ) then
      raise exception 'Legacy compatibility preparation was not audited';
    elsif not p_expect_compatibility and exists(
         select 1 from public.audit_events event_row
         where event_row.object_type='candidate_submission_workflow'
           and event_row.object_id_text=v_workflow::text
           and event_row.action='CANDIDATE_LEGACY_PAPER_ORPHAN_PREPARED_FOR_CANCELLATION'
       ) then
      raise exception 'Normal modern cancellation incorrectly used legacy compatibility';
    end if;
  else
    if not v_failed
       or v_failed_message<>'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
       or (select state from public.candidate_submission_workflows
           where id=v_workflow)<>'AWAITING_PAPER_RETURN'
       or (select qr_token from public.timesheets
           where timesheet_id=v_timesheet)<>v_source_token
       or exists(
         select 1 from public.audit_events event_row
         where event_row.object_type='candidate_submission_workflow'
           and event_row.object_id_text=v_workflow::text
           and event_row.action='CANDIDATE_LEGACY_PAPER_ORPHAN_PREPARED_FOR_CANCELLATION'
       ) then
      raise exception 'Modern or ambiguous pack did not fail closed without mutation: failure=%',
        v_failed_message;
    end if;
  end if;
end;
$function$;

-- The exact pre-v2 one-page mismatch is retired through the ordinary owner.
select pg_temp.verify_legacy_orphan_cancel(null,false,true,true);

-- A modern page-manifest mismatch remains closed; it cannot use compatibility.
select pg_temp.verify_legacy_orphan_cancel('2',false,false,false);

-- A normal modern page-manifest pack with a current receipt cancels through the
-- unchanged owner and never enters the legacy compatibility branch.
select pg_temp.verify_legacy_orphan_cancel('2',true,true,false);

do $verify_acl$
begin
  if pg_catalog.has_function_privilege(
       'service_role',
       'private._candidate_legacy_paper_orphan_prepare_v1(text,uuid,integer,timestamptz)',
       'EXECUTE'
     ) then
    raise exception 'Private legacy compatibility helper became directly executable';
  end if;
  if not pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_workflow_cancel_atomic_v2(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     ) or pg_catalog.has_function_privilege(
       'anon',
       'public.candidate_workflow_cancel_atomic_v2(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     ) or pg_catalog.has_function_privilege(
       'authenticated',
       'public.candidate_workflow_cancel_atomic_v2(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     ) then
    raise exception 'Legacy cancellation wrapper ACL contract changed';
  end if;
end;
$verify_acl$;

rollback;
