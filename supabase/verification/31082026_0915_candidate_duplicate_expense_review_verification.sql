\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for Candidate duplicate-expense review.
-- The official expense summary never counts as a real Other expense.  An
-- individual Office user may authorise only after explicit review; bulk and
-- automatic routes remain blocked.
begin;

-- Candidate runtime CI deliberately creates a compact Contract fixture.  The
-- production bulk authoriser reads this established Office flag even though
-- the duplicate-review case leaves it false.  Add the missing fixture column
-- only inside this rollback-contained transaction.
do $contract_fixture$
begin
  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='contracts' and column_name='requires_hr'
  ) then
    alter table public.contracts add column requires_hr boolean not null default false;
  end if;
end;
$contract_fixture$;

-- The Candidate runtime matrix deliberately installs a reduced CloudTMS
-- catalogue.  Its authorisation owner still emits the normal optional
-- diagnostic event, so provide a rollback-contained no-op only when that
-- unrelated diagnostic helper is absent from the reduced fixture.
do $diagnostic_fixture$
begin
  if to_regprocedure('public._temp_diag_log(text,text,text,jsonb)') is null then
    execute $sql$
      create function public._temp_diag_log(
        p_action text,
        p_object_type text,
        p_object_id_text text default null,
        p_payload_json jsonb default '{}'::jsonb
      ) returns void
      language plpgsql
      as $body$
      begin
        return;
      end;
      $body$
    $sql$;
  end if;
end;
$diagnostic_fixture$;

-- The reduced Candidate runtime catalogue also omits the established
-- lifecycle-signature reader used by the Office bulk-authorisation owner.
-- Supply a deterministic rollback-only signature so this verifier exercises
-- the real authorisation routines without replacing their production owner.
do $lifecycle_signature_fixture$
begin
  if to_regprocedure('public.timesheet_lifecycle_signature_v1(uuid,uuid,boolean)') is null then
    execute $sql$
      create function public.timesheet_lifecycle_signature_v1(
        p_timesheet_id uuid,
        p_contract_week_id uuid default null,
        p_include_finance boolean default false
      ) returns jsonb
      language sql
      stable
      as $body$
        select jsonb_build_object(
          'ok', true,
          'backend_row_signature',
          'verification:' || coalesce(p_timesheet_id::text, '') || ':' ||
            coalesce(p_contract_week_id::text, '') || ':' ||
            coalesce(p_include_finance::text, 'false')
        )
      $body$
    $sql$;
  end if;
end;
$lifecycle_signature_fixture$;

-- The reduced Candidate catalogue does not install the very large Office
-- Timesheet-list projection.  Supply its exact return contract from the
-- fixture's real Timesheet, Contract Week and financial rows so the genuine
-- bulk-authorise owner below can classify the duplicate-review record.
do $lightweight_rows_fixture$
begin
  if to_regprocedure('public.timesheet_summary_lightweight_rows_v1(jsonb)') is null then
    execute $sql$
      create function public.timesheet_summary_lightweight_rows_v1(
        p_filters jsonb default '{}'::jsonb
      ) returns table(
        timesheet_id uuid,contract_week_id uuid,contract_id uuid,candidate_id uuid,
        candidate_name text,candidate_display_name text,client_id uuid,client_name text,
        booking_id text,occupant_key_norm text,hospital_norm text,candidate_hint_text jsonb,
        week_ending_date date,work_date date,sheet_scope text,submission_mode text,
        submission_mode_snapshot text,basis text,route_type text,route_display text,
        route_family text,route_subfamily text,underlying_channel_family text,
        summary_stage text,tools_stage text,processing_status text,
        processing_status_display text,authorised_at_utc timestamptz,
        authorised_at_server timestamptz,processed_at_utc timestamptz,
        is_authorised boolean,total_hours numeric,total_pay_ex_vat numeric,
        total_charge_ex_vat numeric,margin_ex_vat numeric,net_delta_ex_vat numeric,
        paid_at_utc timestamptz,pay_icon_code text,pay_status_code text,
        pay_paid_at_utc timestamptz,invoice_is_paid boolean,invoice_issue_stage text,
        invoice_segment_stage text,invoice_segments_total integer,
        invoice_segments_locked integer,invoice_segments_unlocked integer,
        issue_codes text[],validation_status text,validation_summary text,
        hr_crosscheck_status text,hr_crosscheck_issues text[],qr_status text,
        is_qr boolean,is_adjusted boolean,needs_attention boolean,
        has_rate_issue boolean,has_pay_channel_issue boolean,
        client_no_timesheet_required boolean,client_autoprocess_hr boolean,
        client_is_nhsp boolean,has_any_evidence boolean,
        attached_evidence_count integer,primary_artifact_storage_key text,
        primary_artifact_display_name text,primary_artifact_preview_mode text
      ) language sql stable as $body$
        select
          timesheet.timesheet_id,week.id,timesheet.contract_id,financial.candidate_id,
          coalesce(candidate.display_name,candidate.email),candidate.display_name,
          financial.client_id,client.name,timesheet.booking_id,
          timesheet.occupant_key_norm,timesheet.hospital_norm,
          coalesce(timesheet.candidate_hint_text,'{}'::jsonb),
          timesheet.week_ending_date,null::date,timesheet.sheet_scope::text,
          timesheet.submission_mode::text,week.submission_mode_snapshot::text,
          coalesce(financial.basis::text,'MANUAL'),'WEEKLY_MANUAL','Manual',
          'MANUAL','MANUAL_NON_QR','MANUAL_NON_QR','PROCESSED','PROCESSED',
          financial.processing_status::text,financial.processing_status::text,
          financial.authorised_at_utc,timesheet.authorised_at_server,
          null::timestamptz,
          (financial.authorised_at_utc is not null
            or timesheet.authorised_at_server is not null),
          financial.total_hours,financial.total_pay_ex_vat,
          financial.total_charge_ex_vat,financial.margin_ex_vat,
          financial.total_charge_ex_vat-financial.total_pay_ex_vat,
          financial.paid_at_utc,'UNPAID','UNPAID',financial.paid_at_utc,
          false,'NOT_ISSUED','UNLOCKED',0,0,0,array[]::text[],'','',
          '',array[]::text[],timesheet.qr_status::text,false,
          coalesce(timesheet.is_adjustment,false),false,false,false,
          false,false,false,false,0,null::text,null::text,null::text
        from public.timesheets timesheet
        join public.timesheets_financials financial
          on financial.timesheet_id=timesheet.timesheet_id
         and financial.is_current=true
        left join public.contract_weeks week
          on week.timesheet_id=timesheet.timesheet_id
        left join public.candidates candidate on candidate.id=financial.candidate_id
        left join public.clients client on client.id=financial.client_id
        where timesheet.is_current=true and timesheet.archived_at_utc is null
          and (
            nullif(p_filters->>'candidate_id','') is null
            or financial.candidate_id=(p_filters->>'candidate_id')::uuid
          )
          and (
            nullif(p_filters->>'week_ending_date','') is null
            or timesheet.week_ending_date=(p_filters->>'week_ending_date')::date
          )
      $body$
    $sql$;
  end if;
end;
$lightweight_rows_fixture$;

-- Keep the compact proof on the bulk-authorise decision owner.  Its canonical
-- status-patch reader belongs to the much larger Office list catalogue, so
-- replace only that read helper inside this rollback transaction with stable
-- signatures for the exact row keys supplied by the genuine dataset owner.
create or replace function public.bulk_timesheet_row_patch_v1(
  p_filters jsonb default '{}'::jsonb
) returns table(row_json jsonb)
language sql stable
as $fixture$
  select jsonb_build_object(
    'row_key',row_key.value,
    'row_signature',md5(row_key.value),
    'backend_row_signature',md5(row_key.value),
    'mutation_row_signature',md5(row_key.value),
    'has_retained_financial_history',false
  )
  from jsonb_array_elements_text(coalesce(p_filters->'row_keys','[]'::jsonb))
    row_key(value)
$fixture$;

-- The same reduced catalogue omits the optional HR-validation history read by
-- the Office authorisation owners.  The duplicate-expense fixture has no HR
-- validation, so an empty rollback-only relation preserves that truthful
-- state while allowing the unchanged production query to run.
do $validation_fixture$
begin
  if to_regclass('public.timesheet_validations') is null then
    create table public.timesheet_validations (
      id uuid not null,
      timesheet_id uuid,
      status public.validation_status_enum not null,
      reason_code text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      pre_validated boolean not null default false
    );
  end if;
end;
$validation_fixture$;

do $verification$
declare
  v_actor uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_prior_contract uuid:=gen_random_uuid();
  v_current_contract uuid:=gen_random_uuid();
  v_prior_week uuid:=gen_random_uuid();
  v_current_week uuid:=gen_random_uuid();
  v_prior_timesheet uuid:=gen_random_uuid();
  v_current_timesheet uuid:=gen_random_uuid();
  v_prior_workflow uuid:=gen_random_uuid();
  v_current_workflow uuid:=gen_random_uuid();
  v_week date:=date '2026-08-30';
  v_review jsonb;
  v_bulk jsonb;
  v_bulk_dataset jsonb;
  v_bulk_dataset_row jsonb;
  v_authorised jsonb;
  v_finalisation_codes jsonb;
  v_error text;
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema='public' and table_name='tms_users' and column_name='password_hash'
  ) then
    execute $sql$
      insert into public.tms_users(id,email,password_hash,role,is_active)
      values($1,$2,'UNUSABLE_VERIFICATION_ONLY','admin',true)
    $sql$ using v_actor,'duplicate-expense-actor-'||v_actor::text||'@example.test';
  else
    insert into public.tms_users(id,email,is_active)
    values(v_actor,'duplicate-expense-actor-'||v_actor::text||'@example.test',true);
  end if;
  insert into public.clients(id,name) values(v_client,'Duplicate Expense Verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(gen_random_uuid(),v_client,v_week-30,'ELECTRONIC',0);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'duplicate-expense-'||v_candidate::text||'@example.test',true,
    'GCK-DUPLICATE-'||replace(v_candidate::text,'-',''));
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','duplicate-expense-'||v_candidate::text||'@example.test','ACTIVE');

  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values
    (v_prior_contract,v_candidate,v_client,v_week-30,v_week+30,'PAYE',0,'ELECTRONIC'),
    (v_current_contract,v_candidate,v_client,v_week-30,v_week+30,'PAYE',0,'ELECTRONIC');

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    contract_id,week_ending_date,sheet_scope,line_type,submission_mode
  ) values
    (v_prior_timesheet,'DUPLICATE-PRIOR-'||v_prior_timesheet::text,
      'GCK-DUPLICATE-'||replace(v_candidate::text,'-',''),'HOSPITAL','WARD','NURSE',
      v_prior_contract,v_week,'WEEKLY','HOURS','MANUAL'),
    (v_current_timesheet,'DUPLICATE-CURRENT-'||v_current_timesheet::text,
      'GCK-DUPLICATE-'||replace(v_candidate::text,'-',''),'HOSPITAL','WARD','NURSE',
      v_current_contract,v_week,'WEEKLY','HOURS','MANUAL');

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values
    (v_prior_week,v_prior_contract,v_week,'OPEN','MANUAL',v_prior_timesheet),
    (v_current_week,v_current_contract,v_week,'OPEN','MANUAL',v_current_timesheet);

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,processing_status,
    mileage_units,mileage_pay_ex_vat,mileage_charge_ex_vat
  ) values
    (v_prior_timesheet,1,v_candidate,v_client,8,'READY_FOR_INVOICE',12,6,6),
    (v_current_timesheet,1,v_candidate,v_client,8,'PENDING_AUTH',4,2,2);

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,issue_codes,idempotency_key,worker_submitted_at_utc
  ) values
    (v_prior_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
      'FINALISED',1,v_prior_contract,v_prior_week,v_prior_timesheet,v_prior_timesheet,
      v_week,'[]','duplicate-expense-prior',now()-interval '1 day'),
    (v_current_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
      'MANAGER_APPROVED',1,v_current_contract,v_current_week,v_current_timesheet,v_current_timesheet,
      v_week,'[]','duplicate-expense-current',now());

  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    expense_category,document_role,state,storage_key,media_type,byte_size,
    source_content_sha256,upload_idempotency_key,immutable_at_utc
  ) values
    (v_prior_workflow,1,1,v_prior_timesheet,'MILEAGE_FORM','MILEAGE','MILEAGE_CLAIM_FORM',
      'IMMUTABLE','duplicate-expense/mileage.jpg','image/jpeg',1024,
      decode(repeat('a1',32),'hex'),'duplicate-expense-mileage',now()),
    (v_prior_workflow,1,2,v_prior_timesheet,'EXPENSE_SUMMARY',null,
      'EXPENSE_MILEAGE_APPROVAL_SUMMARY','IMMUTABLE',null,null,null,null,
      'duplicate-expense-summary',now());

  v_review:=private._expense_duplicate_review_v1(
    v_current_workflow,array['MILEAGE','OTHER']::text[]
  );
  if coalesce((v_review->>'required')::boolean,false) is distinct from true
     or v_review->'categories' is distinct from '["MILEAGE"]'::jsonb
     or nullif(v_review->>'confirmation_digest','') is null then
    raise exception 'Duplicate review did not isolate Mileage from the official summary: %',v_review;
  end if;

  v_finalisation_codes:=private._candidate_finalisation_issue_codes_v1(
    jsonb_build_array(
      'DUPLICATE_EXPENSE_REVIEW','DUPLICATE_EXPENSE_MILEAGE',
      'DUPLICATE_EXPENSE_NOT_A_REAL_CATEGORY','STALE_POLICY_WARNING'
    ),
    jsonb_build_array('HEALTHROSTER_VALIDATION_REQUIRED')
  );
  if v_finalisation_codes is distinct from jsonb_build_array(
      'DUPLICATE_EXPENSE_MILEAGE','DUPLICATE_EXPENSE_REVIEW',
      'HEALTHROSTER_VALIDATION_REQUIRED'
    ) then
    raise exception 'Finalisation did not preserve the confirmed duplicate review exactly: %',
      v_finalisation_codes;
  end if;

  update public.candidate_submission_workflows
  set issue_codes='[]'::jsonb,state='FINALISED',generation=generation+1,
      finalised_at_utc=now()
  where id=v_current_workflow;

  v_review:=private._timesheet_duplicate_expense_review_v1(v_current_timesheet);
  if coalesce((v_review->>'required')::boolean,false) is distinct from true
     or v_review->'categories' is distinct from '["MILEAGE"]'::jsonb then
    raise exception 'Finalised claim did not recover its earlier confirmed duplicate review: %',
      v_review;
  end if;
  v_review:=private._timesheet_duplicate_expense_review_v1(v_prior_timesheet);
  if coalesce((v_review->>'required')::boolean,false) then
    raise exception 'A later claim incorrectly back-labelled the first claim as a duplicate: %',
      v_review;
  end if;

  v_bulk_dataset:=public.bulk_authorise_dataset_v1(jsonb_build_object(
    'candidate_id',v_candidate,
    'week_ending_date',v_week,
    'classification','TIMESHEETS'
  ));
  select dataset_row.value
    into v_bulk_dataset_row
  from jsonb_array_elements(coalesce(v_bulk_dataset->'rows','[]'::jsonb)) dataset_row(value)
  where dataset_row.value->>'timesheet_id'=v_current_timesheet::text
  limit 1;
  if v_bulk_dataset_row is null
     or v_bulk_dataset_row->>'bulk_authorise_section' is distinct from 'processed_review_required'
     or v_bulk_dataset_row->>'bulk_authorise_block_code' is distinct from 'DUPLICATE_EXPENSE_REVIEW_REQUIRED'
     or coalesce((v_bulk_dataset_row->>'can_bulk_authorise')::boolean,true)
     or not coalesce(v_bulk_dataset_row->'issue_codes','[]'::jsonb) ? 'DUPLICATE_EXPENSE_REVIEW' then
    raise exception 'Bulk dataset did not present the recovered duplicate review: %',
      v_bulk_dataset_row;
  end if;

  begin
    perform public.timesheet_authorise_generic_atomic(
      v_current_timesheet,v_current_timesheet,v_actor,now(),null
    );
    raise exception 'Generic authorisation unexpectedly bypassed duplicate review';
  exception when sqlstate 'PT409' then
    get stacked diagnostics v_error=message_text;
    if v_error<>'DUPLICATE_EXPENSE_REVIEW_REQUIRED' then raise; end if;
  end;

  v_bulk:=public.timesheet_authorise_bulk_atomic(
    jsonb_build_array(jsonb_build_object(
      'timesheet_id',v_current_timesheet,
      'expected_timesheet_id',v_current_timesheet
    )),v_actor,now()
  );
  if v_bulk#>>'{results,0,error_code}' is distinct from 'DUPLICATE_EXPENSE_REVIEW_REQUIRED'
     or coalesce((v_bulk->>'success_count')::integer,-1)<>0 then
    raise exception 'Bulk authorisation did not exclude duplicate expense review: %',v_bulk;
  end if;

  v_authorised:=public.timesheet_authorise_reviewed_atomic(
    v_current_timesheet,v_current_timesheet,v_actor,now(),null,true
  );
  if coalesce((v_authorised->>'ok')::boolean,false) is distinct from true
     or (select authorised_at_server is null from public.timesheets
         where timesheet_id=v_current_timesheet)
     or (select authorised_at_utc is null from public.timesheets_financials
         where timesheet_id=v_current_timesheet and is_current=true) then
    raise exception 'Reviewed individual authorisation did not complete: %',v_authorised;
  end if;
end;
$verification$;

rollback;
