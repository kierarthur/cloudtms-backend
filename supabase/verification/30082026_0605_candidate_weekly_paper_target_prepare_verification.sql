\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for the weekly PAPER/QR adapter.  A
-- submitted Candidate workflow may have no Timesheet after withdrawal.  The
-- adapter must create exactly one unapproved current Timesheet/TSFIN through
-- the existing weekly canonical writer, link the workflow, replay safely, and
-- remain inaccessible to browser roles.
begin;

do $candidate_weekly_paper_target_prepare_verification$
declare
  v_now timestamptz := '2026-08-30 06:05:00+00';
  v_week_ending_date date := ('2026-08-30 06:05:00+00'::timestamptz at time zone 'UTC')::date;
  v_work_date date := (('2026-08-30 06:05:00+00'::timestamptz at time zone 'UTC')::date-6);
  v_client uuid := gen_random_uuid();
  v_candidate uuid := gen_random_uuid();
  v_contract uuid := gen_random_uuid();
  v_week uuid := gen_random_uuid();
  v_account uuid := gen_random_uuid();
  v_session uuid := gen_random_uuid();
  v_workflow uuid := gen_random_uuid();
  v_timesheet uuid := gen_random_uuid();
  v_email text;
  v_schedule jsonb;
  v_create jsonb;
  v_patch jsonb;
  v_week_patch jsonb;
  v_snapshot jsonb;
  v_submission jsonb;
  v_first jsonb;
  v_replay jsonb;
  v_paper jsonb;
  v_paper_replay jsonb;
  v_promote jsonb;
  v_manifest jsonb;
  v_manifest_hex text;
  v_page jsonb;
  v_qr jsonb;
  v_proof jsonb;
  v_prepare jsonb;
  v_pack jsonb;
  v_pack_replay jsonb;
  v_verified_pages jsonb;
  v_component uuid;
  v_component_key uuid := gen_random_uuid();
  v_pack_key uuid := gen_random_uuid();
  v_bad_pack_key uuid := gen_random_uuid();
  v_content_hash text;
  v_bad_proof_failed boolean := false;
  v_mileage_source_component uuid := gen_random_uuid();
  v_mileage_component_no integer;
  v_mileage_source_hash text;
  v_mileage_manifest jsonb;
  v_mileage_manifest_hex text;
  v_mileage_page jsonb;
  v_hours_page jsonb;
  v_mileage_qr jsonb;
  v_pack_qr jsonb;
  v_mileage_proof jsonb;
  v_pack_proof jsonb;
  v_wrong_mileage_failed boolean := false;
  v_mileage_on_hours_failed boolean := false;
  v_plan_chunk uuid;
  v_plan_document_version uuid;
  v_plan_result jsonb;
  v_manual_operation uuid := gen_random_uuid();
  v_manual_document_version uuid := gen_random_uuid();
  v_manual_plan_chunk uuid := gen_random_uuid();
  v_manual_revision bigint;
  v_manual_result jsonb;
  v_stale_failed boolean := false;
begin
  v_email := 'paper-target-'||replace(v_candidate::text,'-','')||'@example.test';
  v_schedule := jsonb_build_array(jsonb_build_object(
    'date',v_work_date,'day','MON','start','09:00','end','17:00',
    'break_minutes',0,'hours',8
  ));
  v_create := jsonb_build_object(
    'timesheet_id',v_timesheet,
    'booking_id','CANDIDATE-WEEKLY-'||replace(v_week::text,'-',''),
    'contract_id',v_contract,
    'week_ending_date',v_week_ending_date,
    'occupant_key_norm','PAPER-TARGET-'||replace(v_candidate::text,'-',''),
    'hospital_norm','Paper target verification client',
    'ward_norm','',
    'job_title_norm','RMN',
    'shift_label_norm','weekly',
    'status','RECEIVED',
    'submission_mode','MANUAL',
    'line_type','HOURS',
    'sheet_scope','WEEKLY',
    'actual_schedule_json',v_schedule,
    'additional_units_week','{}'::jsonb,
    'additional_units_per_day','{}'::jsonb,
    'candidate_hint_text',jsonb_build_object('display_name','Paper Target Candidate')
  );
  v_patch := jsonb_build_object(
    'actual_schedule_json',v_schedule,
    'additional_units_week','{}'::jsonb,
    'additional_units_per_day','{}'::jsonb
  );
  v_week_patch := jsonb_build_object(
    'totals_json',jsonb_build_object('total_hours',8,'actual_schedule_json',v_schedule),
    'planned_schedule_json',v_schedule
  );
  v_snapshot := jsonb_build_object(
    'timesheet_id',v_timesheet,
    'timesheet_version',1,
    'candidate_id',v_candidate,
    'client_id',v_client,
    'basis','SELF_REPORTED',
    'candidate_assignment','ASSIGNED',
    'processing_status','PENDING_AUTH',
    'total_hours',8,
    'hours_day',8,
    'hours_night',0,
    'hours_sat',0,
    'hours_sun',0,
    'hours_bh',0,
    'invoice_breakdown_json','{}'::jsonb,
    'additional_units_json','{}'::jsonb,
    'mileage_units',0,
    'mileage_pay_ex_vat',0,
    'mileage_charge_ex_vat',0,
    'expenses_pay_ex_vat',0,
    'expenses_charge_ex_vat',0,
    'travel_pay_ex_vat',0,
    'travel_charge_ex_vat',0,
    'accommodation_pay_ex_vat',0,
    'accommodation_charge_ex_vat',0,
    'other_pay_ex_vat',0,
    'other_charge_ex_vat',0
  );
  v_submission := jsonb_build_object(
    'contract_version','CANDIDATE_WEEKLY_CANONICAL_AUTHORITY_V1',
    'timesheet_create_json',v_create,
    'timesheet_patch_json',v_patch,
    'contract_week_patch_json',v_week_patch,
    'canonical_tsfin_snapshot',v_snapshot
  );

  update public.settings_defaults
  set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
    'candidate_app_reads',true,
    'candidate_app_writes',true,
    'candidate_record_role_capabilities',true,
    'candidate_paper_qr',true
  )
  where id=1;

  insert into public.clients(id,name)
  values(v_client,'Paper target verification client');
  insert into public.candidates(id,email,display_name,active,key_norm,opt_in_email)
  values(v_candidate,v_email,'Paper Target Candidate',true,
    'PAPER-TARGET-'||replace(v_candidate::text,'-',''),true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot,role,
    candidate_paper_submission_enabled_override
  ) values(
    v_contract,v_candidate,v_client,v_week_ending_date-30,v_week_ending_date+30,
    extract(dow from v_week_ending_date)::integer,
    'ELECTRONIC','PAYE','RMN',true
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
    day_entries_json,totals_json
  ) values(
    v_week,v_contract,v_week_ending_date,'SUBMITTED','ELECTRONIC',null,
    v_schedule,jsonb_build_object('total_hours',8,'actual_schedule_json',v_schedule)
  );
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,password_scheme,password_scheme_version,
    password_salt,password_digest,password_changed_at_utc
  ) values(
    v_account,'TEST',v_email,'ACTIVE','PBKDF2-HMAC-SHA256',1,
    decode(repeat('41',16),'hex'),decode(repeat('42',32),'hex'),v_now
  );
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(v_session::text,'sha256'),v_now+interval '1 hour',v_now+interval '2 hours'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,review_manifest_sha256,
    immutable_submission_json,immutable_submission_sha256
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','ELECTRONIC',
    'READY_FOR_MANAGER_APPROVAL',1,v_contract,v_week,null,null,v_week_ending_date,
    'paper-target-verification-'||v_workflow::text,
    extensions.digest('paper-target-verification-'||v_workflow::text,'sha256'),
    v_submission,private._candidate_sha256_jsonb_v1(v_submission)
  );
  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,component_kind,document_role,state,
    required,review_ordinal,review_render_state,final_signed_render_state
  ) values(
    v_workflow,1,1,'HOURS_TIMESHEET','ELECTRONIC_TIMESHEET_MANAGER_REVIEW','PENDING',
    true,1,'PENDING','PENDING'
  );

  v_first:=public.candidate_weekly_paper_target_prepare_v1(
    v_session,'TEST',v_workflow,1,v_now
  );
  v_replay:=public.candidate_weekly_paper_target_prepare_v1(
    v_session,'TEST',v_workflow,1,v_now+interval '1 second'
  );

  if v_first->>'timesheet_id' is distinct from v_timesheet::text
     or v_first->>'created' is distinct from 'true'
     or v_replay->>'timesheet_id' is distinct from v_timesheet::text
     or v_replay->>'idempotent_replay' is distinct from 'true'
     or (select timesheet_id from public.contract_weeks where id=v_week) is distinct from v_timesheet
     or (select target_timesheet_id from public.candidate_submission_workflows where id=v_workflow) is distinct from v_timesheet
     or (select timesheet_id from public.candidate_submission_components where workflow_id=v_workflow and component_no=1) is distinct from v_timesheet
     or (select count(*) from public.timesheets where contract_id=v_contract and week_ending_date=v_week_ending_date and is_current)<>1
     or not exists(
       select 1 from public.timesheets
       where timesheet_id=v_timesheet and is_current and archived_at_utc is null
         and sheet_scope='WEEKLY' and submission_mode='MANUAL'
         and authorised_at_server is null
     )
     or (select count(*) from public.timesheets_financials where timesheet_id=v_timesheet and is_current)<>1
     or not exists(
       select 1 from public.timesheets_financials
       where timesheet_id=v_timesheet and is_current
         and authorised_at_utc is null and paid_at_utc is null
         and locked_by_invoice_id is null
     ) then
    raise exception 'CANDIDATE_WEEKLY_PAPER_TARGET_FIRST_USE_FAILED: %, %',v_first,v_replay;
  end if;

  begin
    perform public.candidate_weekly_paper_target_prepare_v1(
      v_session,'TEST',v_workflow,2,v_now+interval '2 seconds'
    );
  exception when sqlstate '40001' then
    v_stale_failed:=true;
  end;
  if not v_stale_failed then
    raise exception 'CANDIDATE_WEEKLY_PAPER_TARGET_STALE_GENERATION_ACCEPTED';
  end if;

  -- The real app offers Printed Documents after the electronic review pack is
  -- ready.  Target preparation above must preserve that state, then the atomic
  -- adapter enters the existing PAPER authority without creating another
  -- Timesheet, workflow or outbox item.
  if (select state from public.candidate_submission_workflows where id=v_workflow)
       is distinct from 'READY_FOR_MANAGER_APPROVAL'
     or (select route from public.candidate_submission_workflows where id=v_workflow)
       is distinct from 'ELECTRONIC' then
    raise exception 'CANDIDATE_WEEKLY_PAPER_TARGET_READY_STATE_NOT_PRESERVED';
  end if;
  v_paper:=public.candidate_weekly_paper_prepare_atomic_v1(
    v_session,'TEST',v_workflow,'PAPER_PREPARE',1,'{}'::jsonb,
    'paper-ready-verification-'||v_workflow::text,v_now+interval '3 seconds'
  );
  v_paper_replay:=public.candidate_weekly_paper_prepare_atomic_v1(
    v_session,'TEST',v_workflow,'PAPER_PREPARE',1,'{}'::jsonb,
    'paper-ready-verification-'||v_workflow::text,v_now+interval '4 seconds'
  );
  if v_paper->>'state' is distinct from 'AWAITING_PAPER_RETURN'
     or v_paper_replay->>'state' is distinct from 'AWAITING_PAPER_RETURN'
     or (select state from public.candidate_submission_workflows where id=v_workflow)
          is distinct from 'AWAITING_PAPER_RETURN'
     or (select route from public.candidate_submission_workflows where id=v_workflow)
          is distinct from 'PAPER'
     or (select count(*) from public.timesheets where contract_id=v_contract and week_ending_date=v_week_ending_date and is_current)<>1
     or (select count(*) from public.mail_outbox
         where type='TIMESHEET_QR'
           and payment_scope_json->>'candidate_workflow_id'=v_workflow::text)<>1
     or (select processing_status::text
         from public.timesheets_financials
         where timesheet_id=v_timesheet and is_current)
          is distinct from 'AWAITING_MANUAL_SIGNATURE'
     or not exists(
       select 1
       from public.timesheets current_timesheet
       join public.invoice_document_versions current_document
         on current_document.id=current_timesheet.current_document_version_id
       where current_timesheet.timesheet_id=v_timesheet
         and current_timesheet.is_current
         and current_timesheet.document_state='QUEUED'
         and current_document.source_revision=current_timesheet.document_revision::text
         and current_document.entity_id=current_timesheet.timesheet_id
     ) then
    raise exception 'CANDIDATE_WEEKLY_PAPER_READY_FIRST_USE_FAILED: %, %',v_paper,v_paper_replay;
  end if;

  -- The Candidate PAPER route stores the Timesheet as MANUAL so the eventual
  -- returned page is handled by the established signed-asset authority.  Its
  -- initial frozen QR_UNSIGNED pack is nevertheless a server-rendered official
  -- form and must not be rejected as a missing uploaded manual asset.
  if (select count(*)
      from public.invoice_operation_chunks c
      where c.entity_type='TIMESHEET'
        and c.entity_id=v_timesheet
        and c.chunk_type='DOCUMENT_PLAN'
        and c.phase='BUILD_MANIFEST')<>1 then
    raise exception 'CANDIDATE_WEEKLY_PAPER_DOCUMENT_PLAN_MULTIPLICITY_INVALID';
  end if;
  select c.id,c.document_version_id
    into v_plan_chunk,v_plan_document_version
  from public.invoice_operation_chunks c
  where c.entity_type='TIMESHEET'
    and c.entity_id=v_timesheet
    and c.chunk_type='DOCUMENT_PLAN'
    and c.phase='BUILD_MANIFEST';

  v_plan_result:=private._invoice_document_advance_batch(
    jsonb_build_array(jsonb_build_object(
      'chunk_id',v_plan_chunk,'phase','BUILD_MANIFEST')),
    v_now+interval '5 seconds'
  );
  if v_plan_result->0->>'status' is distinct from 'QUEUED'
     or v_plan_result->0->>'phase' is distinct from 'WAIT_FOR_INPUTS'
     or (select status::text from public.invoice_operation_chunks
         where id=v_plan_chunk) is distinct from 'QUEUED'
     or (select phase from public.invoice_operation_chunks
         where id=v_plan_chunk) is distinct from 'WAIT_FOR_INPUTS'
     or (select status::text from public.invoice_document_versions
         where id=v_plan_document_version) is distinct from 'WAITING_FOR_INPUTS'
     or (select jsonb_array_length(manifest_json)
         from public.invoice_document_versions
         where id=v_plan_document_version)<>1
     or (select manifest_json->0->>'input_type'
         from public.invoice_document_versions
         where id=v_plan_document_version) is distinct from 'ELECTRONIC_TIMESHEET'
     or (select count(*) from public.invoice_operation_chunks
         where operation_id=(select operation_id from public.invoice_operation_chunks
                             where id=v_plan_chunk)
           and chunk_type='SOURCE_RENDER' and phase='RENDER' and status='QUEUED')<>1
     or exists(
       select 1 from public.invoice_operation_chunks
       where operation_id=(select operation_id from public.invoice_operation_chunks
                           where id=v_plan_chunk)
         and error_json->>'code'='MANUAL_TIMESHEET_ASSET_REQUIRED'
     ) then
    raise exception 'CANDIDATE_WEEKLY_QR_UNSIGNED_DOCUMENT_PLAN_FAILED: %',v_plan_result;
  end if;

  -- New packs are promoted while the email is still held.  The returned
  -- physical JPEG is first staged outside PostgreSQL, but no returned page is
  -- accepted here until the exact full manifest is supplied in one call.
  v_promote:=public.candidate_paper_manifest_v2_promote_v1(
    v_session,'TEST',v_workflow,1,
    v_paper->>'paper_return_manifest_sha256',v_now+interval '6 seconds'
  );
  select paper_return_manifest_json,
         encode(paper_return_manifest_sha256,'hex')
    into v_manifest,v_manifest_hex
  from public.candidate_submission_workflows
  where id=v_workflow;
  if v_promote->>'manifest_version' is distinct from '2'
     or v_manifest->>'qr_contract_version' is distinct from
       'CANDIDATE_PAPER_PAGE_QR_V2'
     or jsonb_array_length(v_manifest->'pages')<>1 then
    raise exception 'CANDIDATE_PAPER_MANIFEST_V2_PROMOTION_FAILED: %',v_promote;
  end if;

  v_page:=v_manifest->'pages'->0;
  v_qr:=jsonb_build_object(
    'v',2,
    'w',v_workflow::text,
    't',v_timesheet::text,
    'g',1,
    'm',v_manifest_hex,
    'o',(v_page->>'ordinal')::integer,
    'p',v_page->>'page_key_sha256_16',
    'k',v_page->>'page_kind_code',
    'c',coalesce(v_page->>'category_code',''),
    'n',(v_page->>'category_occurrence')::integer
  );
  v_proof:=public.candidate_paper_return_proof_validate_v2(
    v_session,'TEST',v_workflow,1,v_manifest_hex,v_page->>'page_key',v_qr,
    v_now+interval '7 seconds'
  );
  v_content_hash:=encode(extensions.digest(
    'candidate-paper-pack-v2-first-use','sha256'
  ),'hex');
  v_prepare:=public.candidate_component_prepare_atomic_v1(
    v_session,'TEST',v_workflow,1,jsonb_build_object(
      'component_kind','SIGNED_RETURN',
      'document_role','SIGNED_RETURN',
      'paper_return_page_key',v_page->>'page_key',
      'storage_key','candidate-paper-pack-v2/verification.jpg',
      'media_type','image/jpeg',
      'byte_size',4096
    ),v_component_key::text,v_now+interval '8 seconds'
  );
  v_component:=(v_prepare->>'component_id')::uuid;
  v_verified_pages:=jsonb_build_array(jsonb_build_object(
    'ordinal',(v_page->>'ordinal')::integer,
    'page_key',v_page->>'page_key',
    'component_id',v_component,
    'source_content_sha256',v_content_hash,
    'byte_size',4096,
    'media_type','image/jpeg',
    'image_width',1600,
    'image_height',1200,
    'manifest_sha256',v_manifest_hex,
    'qr_payload',v_qr,
    'proof_receipt_sha256',v_proof->>'proof_receipt_sha256',
    'qr_payload_sha256',v_proof->>'qr_payload_sha256'
  ));

  -- A syntactically valid but false proof must roll back the component
  -- completion performed earlier in the same function invocation.
  begin
    perform public.candidate_paper_return_pack_complete_v2(
      v_session,'TEST',v_workflow,1,
      jsonb_set(v_verified_pages,'{0,proof_receipt_sha256}',
        to_jsonb(repeat('0',64)),false),
      v_bad_pack_key::text,v_now+interval '9 seconds'
    );
  exception when sqlstate '40001' then
    v_bad_proof_failed:=true;
  end;
  if not v_bad_proof_failed
     or (select state from public.candidate_submission_components
         where id=v_component) is distinct from 'PENDING'
     or (select state from public.candidate_submission_workflows
         where id=v_workflow) is distinct from 'AWAITING_PAPER_RETURN' then
    raise exception 'CANDIDATE_PAPER_PACK_PARTIAL_ACCEPTANCE_NOT_ROLLED_BACK';
  end if;

  v_pack:=public.candidate_paper_return_pack_complete_v2(
    v_session,'TEST',v_workflow,1,v_verified_pages,
    v_pack_key::text,v_now+interval '10 seconds'
  );
  if v_pack->>'state' is distinct from 'RECEIVED'
     or v_pack->>'paper_return_pack_verified' is distinct from 'true'
     or v_pack->>'paper_return_page_count' is distinct from '1'
     or (select state from public.candidate_submission_components
         where id=v_component) is distinct from 'IMMUTABLE'
     or (select paper_return_proof_receipt_sha256 is not null
         from public.candidate_submission_components
         where id=v_component) is distinct from true
     or (select state from public.candidate_submission_workflows
         where id=v_workflow) is distinct from 'RECEIVED' then
    raise exception 'CANDIDATE_PAPER_PACK_ATOMIC_FIRST_USE_FAILED: %',v_pack;
  end if;

  v_pack_replay:=public.candidate_paper_return_pack_complete_v2(
    v_session,'TEST',v_workflow,1,v_verified_pages,
    v_pack_key::text,v_now+interval '11 seconds'
  );
  if v_pack_replay->>'state' is distinct from 'RECEIVED'
     or v_pack_replay->>'idempotent_replay' is distinct from 'true'
     or v_pack_replay->>'paper_return_pack_verified' is distinct from 'true'
     or v_pack_replay->>'paper_return_page_count' is distinct from '1'
     or (select count(*) from public.candidate_submission_components
         where workflow_id=v_workflow and component_kind='SIGNED_RETURN')<>1
     or (select count(*) from public.audit_events
         where object_type='candidate_workflow_mutation_receipt'
           and object_id_text=v_workflow::text
           and correlation_id=v_pack_key::text)<>1 then
    raise exception 'CANDIDATE_PAPER_PACK_IDEMPOTENT_REPLAY_FAILED: %',v_pack_replay;
  end if;

  -- Mileage is photographed as unsigned evidence before the worker chooses a
  -- manager route.  PAPER then carries each photograph into its own current
  -- pack page and gives that page the same unique return-QR contract as every
  -- other page.  Prove the exact Mileage pack-page QR is accepted, a stale
  -- manifest is rejected, and a Mileage page identity cannot satisfy Hours.
  v_mileage_source_hash:=encode(extensions.digest(
    'candidate-paper-mileage-source-first-use','sha256'
  ),'hex');
  select coalesce(max(component_no),0)+1 into v_mileage_component_no
  from public.candidate_submission_components
  where workflow_id=v_workflow and workflow_generation=1;
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,expense_category,document_role,state,required,
    storage_key,media_type,byte_size,source_content_sha256,immutable_at_utc
  ) values(
    v_mileage_source_component,v_workflow,1,v_mileage_component_no,v_timesheet,
    'MILEAGE_FORM','MILEAGE','MILEAGE_CLAIM_FORM','IMMUTABLE',false,
    'candidate-paper-mileage-source/verification.jpg','image/jpeg',4096,
    decode(v_mileage_source_hash,'hex'),v_now+interval '12 seconds'
  );
  v_mileage_page:=jsonb_build_object(
    'ordinal',1,
    'page_key','paper-v2:2:mileage-source',
    'page_key_sha256_16',substring(encode(extensions.digest(
      'paper-v2:2:mileage-source','sha256'
    ),'hex') from 1 for 16),
    'page_kind_code','M',
    'category_code','M',
    'category_occurrence',1,
    'component_kind','MILEAGE_FORM',
    'expense_category','MILEAGE',
    'source_component_id',v_mileage_source_component,
    'source_content_sha256',v_mileage_source_hash
  );
  v_hours_page:=jsonb_build_object(
    'ordinal',2,
    'page_key','paper-v2:2:hours',
    'page_key_sha256_16',substring(encode(extensions.digest(
      'paper-v2:2:hours','sha256'
    ),'hex') from 1 for 16),
    'page_kind_code','T',
    'category_code','',
    'category_occurrence',1,
    'component_kind','HOURS_TIMESHEET',
    'expense_category',null
  );
  v_mileage_manifest:=jsonb_build_object(
    'manifest_version',2,
    'qr_contract_version','CANDIDATE_PAPER_PAGE_QR_V2',
    'workflow_id',v_workflow,
    'generation',2,
    'pages',jsonb_build_array(v_mileage_page,v_hours_page)
  );
  v_mileage_manifest_hex:=encode(
    private._candidate_sha256_jsonb_v1(v_mileage_manifest),'hex'
  );
  update public.candidate_submission_workflows
     set generation=2,
         route='PAPER',
         state='AWAITING_PAPER_RETURN',
         paper_return_manifest_json=v_mileage_manifest,
         paper_return_manifest_sha256=decode(v_mileage_manifest_hex,'hex')
   where id=v_workflow;

  v_mileage_qr:=jsonb_build_object(
    'v',2,
    'w',v_workflow::text,
    't',v_timesheet::text,
    'g',2,
    'm',v_mileage_manifest_hex,
    'o',1,
    'p',v_mileage_page->>'page_key_sha256_16',
    'k','M',
    'c','M',
    'n',1
  );
  v_mileage_proof:=public.candidate_paper_return_proof_validate_v2(
    v_session,'TEST',v_workflow,2,v_mileage_manifest_hex,
    v_mileage_page->>'page_key',v_mileage_qr,v_now+interval '13 seconds'
  );
  if v_mileage_proof->>'ok' is distinct from 'true'
     or v_mileage_proof->>'qr_identity_kind' is distinct from 'PACK_PAGE'
     or v_mileage_proof->>'page_component_kind' is distinct from 'MILEAGE_FORM' then
    raise exception 'CANDIDATE_PAPER_MILEAGE_PACK_QR_FIRST_USE_FAILED: %',
      v_mileage_proof;
  end if;

  begin
    perform public.candidate_paper_return_proof_validate_v2(
      v_session,'TEST',v_workflow,2,v_mileage_manifest_hex,
      v_mileage_page->>'page_key',
      jsonb_set(v_mileage_qr,'{m}',to_jsonb(repeat('f',64)),false),
      v_now+interval '14 seconds'
    );
  exception when sqlstate '28000' then
    v_wrong_mileage_failed:=true;
  end;
  if not v_wrong_mileage_failed then
    raise exception 'CANDIDATE_PAPER_STALE_MILEAGE_PACK_QR_ACCEPTED';
  end if;

  begin
    perform public.candidate_paper_return_proof_validate_v2(
      v_session,'TEST',v_workflow,2,v_mileage_manifest_hex,
      v_hours_page->>'page_key',v_mileage_qr,v_now+interval '15 seconds'
    );
  exception when sqlstate '28000' then
    v_mileage_on_hours_failed:=true;
  end;
  if not v_mileage_on_hours_failed then
    raise exception 'CANDIDATE_PAPER_MILEAGE_PACK_QR_ACCEPTED_FOR_HOURS';
  end if;

  v_pack_qr:=jsonb_build_object(
    'v',2,
    'w',v_workflow::text,
    't',v_timesheet::text,
    'g',2,
    'm',v_mileage_manifest_hex,
    'o',2,
    'p',v_hours_page->>'page_key_sha256_16',
    'k','T',
    'c','',
    'n',1
  );
  v_pack_proof:=public.candidate_paper_return_proof_validate_v2(
    v_session,'TEST',v_workflow,2,v_mileage_manifest_hex,
    v_hours_page->>'page_key',v_pack_qr,v_now+interval '16 seconds'
  );
  if v_pack_proof->>'ok' is distinct from 'true'
     or v_pack_proof->>'qr_identity_kind' is distinct from 'PACK_PAGE'
     or v_pack_proof->>'page_component_kind' is distinct from 'HOURS_TIMESHEET' then
    raise exception 'CANDIDATE_PAPER_CURRENT_PACK_QR_REGRESSION: %',v_pack_proof;
  end if;

  -- Preserve the opposite fail-closed rule: an ordinary MANUAL Timesheet with
  -- no registered source asset is still blocked.  This second planner is a
  -- rollback-only synthetic operation over the same fixture after QR has been
  -- cancelled, so it cannot be mistaken for the approved QR_UNSIGNED form.
  update public.timesheets
     set qr_status='CANCELLED',qr_token=null,qr_payload_json='{}'::jsonb,
         current_document_version_id=null,active_document_operation_id=null,
         document_state='STALE',document_revision=document_revision+1,
         updated_at=v_now+interval '6 seconds'
   where timesheet_id=v_timesheet and is_current
   returning document_revision into v_manual_revision;

  insert into public.invoice_operations(
    id,operation_type,entity_type,entity_id,idempotency_key,status,phase,
    priority,source_revision,template_version,input_json,config_json,
    progress_json,total_units,chunk_count,control_version,change_seq,
    created_at_utc,updated_at_utc
  ) values(
    v_manual_operation,'BUILD_DOCUMENT','TIMESHEET',v_timesheet,
    'paper-manual-negative-'||v_manual_operation::text,'QUEUED','BUILD_MANIFEST',
    550,v_manual_revision::text,'timesheet-professional-v2','{}'::jsonb,
    jsonb_build_object('processor_policy',private._invoice_processor_limits()),
    '{}'::jsonb,1,1,1,nextval('public.invoice_operation_change_seq'),
    v_now+interval '6 seconds',v_now+interval '6 seconds'
  );
  insert into public.invoice_document_versions(
    id,entity_type,entity_id,purpose,operation_id,source_revision,
    template_version,status,snapshot_json,snapshot_hash,manifest_json,
    manifest_hash,created_at_utc
  ) values(
    v_manual_document_version,'TIMESHEET',v_timesheet,'TIMESHEET',
    v_manual_operation,v_manual_revision::text,'timesheet-professional-v2',
    'PLANNING','{}'::jsonb,encode(extensions.digest('{}','sha256'),'hex'),
    '[]'::jsonb,encode(extensions.digest('[]','sha256'),'hex'),
    v_now+interval '6 seconds'
  );
  insert into public.invoice_operation_chunks(
    id,operation_id,chunk_type,phase,work_key,sequence_no,entity_type,
    entity_id,document_version_id,status,priority,run_after_utc,payload_json,
    operation_control_version,created_at_utc,updated_at_utc
  ) values(
    v_manual_plan_chunk,v_manual_operation,'DOCUMENT_PLAN','BUILD_MANIFEST',
    encode(extensions.digest('paper-manual-negative-'||v_manual_operation::text,
      'sha256'),'hex'),0,'TIMESHEET',v_timesheet,v_manual_document_version,
    'QUEUED',550,v_now+interval '6 seconds','{}'::jsonb,1,
    v_now+interval '6 seconds',v_now+interval '6 seconds'
  );
  v_manual_result:=private._invoice_document_advance_batch(
    jsonb_build_array(jsonb_build_object(
      'chunk_id',v_manual_plan_chunk,'phase','BUILD_MANIFEST')),
    v_now+interval '7 seconds'
  );
  if v_manual_result->0->>'status' is distinct from 'BLOCKED'
     or v_manual_result->0#>>'{error,code}' is distinct from
       'MANUAL_TIMESHEET_ASSET_REQUIRED'
     or (select status::text from public.invoice_operation_chunks
         where id=v_manual_plan_chunk) is distinct from 'BLOCKED' then
    raise exception 'ORDINARY_MANUAL_MISSING_ASSET_NOT_BLOCKED: %',v_manual_result;
  end if;

  if pg_catalog.has_function_privilege('anon',
       'public.candidate_weekly_paper_target_prepare_v1(uuid,text,uuid,integer,timestamp with time zone)',
       'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',
       'public.candidate_weekly_paper_target_prepare_v1(uuid,text,uuid,integer,timestamp with time zone)',
       'EXECUTE')
     or not pg_catalog.has_function_privilege('service_role',
       'public.candidate_weekly_paper_target_prepare_v1(uuid,text,uuid,integer,timestamp with time zone)',
       'EXECUTE') then
    raise exception 'CANDIDATE_WEEKLY_PAPER_TARGET_ACL_INVALID';
  end if;
  if pg_catalog.has_function_privilege('anon',
       'public.candidate_weekly_paper_prepare_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)',
       'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',
       'public.candidate_weekly_paper_prepare_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)',
       'EXECUTE')
     or not pg_catalog.has_function_privilege('service_role',
       'public.candidate_weekly_paper_prepare_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)',
       'EXECUTE') then
    raise exception 'CANDIDATE_WEEKLY_PAPER_READY_ACL_INVALID';
  end if;
  if pg_catalog.has_function_privilege('anon',
       'public.candidate_paper_return_pack_complete_v2(uuid,text,uuid,integer,jsonb,text,timestamp with time zone)',
       'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',
       'public.candidate_paper_return_pack_complete_v2(uuid,text,uuid,integer,jsonb,text,timestamp with time zone)',
       'EXECUTE')
     or not pg_catalog.has_function_privilege('service_role',
       'public.candidate_paper_return_pack_complete_v2(uuid,text,uuid,integer,jsonb,text,timestamp with time zone)',
       'EXECUTE') then
    raise exception 'CANDIDATE_PAPER_PACK_ACL_INVALID';
  end if;
end;
$candidate_weekly_paper_target_prepare_verification$;

rollback;
