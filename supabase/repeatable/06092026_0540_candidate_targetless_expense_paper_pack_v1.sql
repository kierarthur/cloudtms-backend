-- A later standalone expense claim can legitimately choose PAPER after the
-- worked Timesheet has already been manager-approved.  That approved row is
-- an immutable display/ownership anchor, not a fresh unsigned QR Timesheet.
-- Create only the held complete-pack mail authority here.  The expense claim
-- remains target-less until its returned pages are accepted and finalisation
-- creates the separate expense carrier.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_targetless_expense_paper_pack_enqueue_v1(
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_anchor public.timesheets%rowtype;
  v_reserved_week public.contract_weeks%rowtype;
  v_candidate public.candidates%rowtype;
  v_manifest_sha256 text;
  v_recipient_namespace text;
  v_reference text;
  v_scope jsonb;
  v_mail public.mail_outbox%rowtype;
  v_mail_count integer;
  v_conflict_count integer;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_workflow_id is null
     or coalesce(p_expected_generation,0)<1
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
  end if;

  select workflow_row.* into v_workflow
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=p_workflow_id
    and workflow_row.environment=v_environment
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation is distinct from p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.workflow_kind<>'CONTRACT_EXPENSE'
     or v_workflow.scope<>'WEEKLY'
     or v_workflow.route<>'PAPER'
     or v_workflow.state<>'AWAITING_PAPER_RETURN'
     or v_workflow.target_timesheet_id is not null
     or v_workflow.anchor_timesheet_id is null
     or v_workflow.contract_week_id is null
     or v_workflow.immutable_submission_sha256 is null
     or v_workflow.paper_return_manifest_sha256 is null
     or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256 then
    raise exception 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID' using errcode='40001';
  end if;
  v_manifest_sha256:=encode(v_workflow.paper_return_manifest_sha256,'hex');

  select timesheet_row.* into v_anchor
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id=v_workflow.anchor_timesheet_id
    and timesheet_row.is_current=true
    and timesheet_row.archived_at_utc is null
  for share;
  if not found
     or v_anchor.contract_id is distinct from v_workflow.contract_id
     or v_anchor.week_ending_date is distinct from v_workflow.week_ending_date
     or v_anchor.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001';
  end if;

  select week_row.* into v_reserved_week
  from public.contract_weeks week_row
  where week_row.id=v_workflow.contract_week_id
  for share;
  if not found
     or v_reserved_week.contract_id is distinct from v_workflow.contract_id
     or v_reserved_week.week_ending_date is distinct from v_workflow.week_ending_date
     or v_reserved_week.timesheet_id is not null then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_INCONSISTENT' using errcode='40001';
  end if;

  if not exists(
    select 1 from public.contracts contract_row
    where contract_row.id=v_workflow.contract_id
      and contract_row.candidate_id=v_workflow.candidate_id
  ) then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
  end if;

  select candidate_row.* into v_candidate
  from public.candidates candidate_row
  where candidate_row.id=v_workflow.candidate_id;
  if not found then
    raise exception 'CANDIDATE_NOT_FOUND' using errcode='P0002';
  end if;
  if nullif(btrim(coalesce(v_candidate.email,'')),'') is null then
    raise exception 'CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE' using errcode='55000';
  end if;
  if not coalesce(v_candidate.opt_in_email,true) then
    raise exception 'CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE' using errcode='55000';
  end if;

  select count(*)::integer into v_conflict_count
  from public.candidate_submission_workflows other_workflow
  where other_workflow.id<>v_workflow.id
    and other_workflow.environment=v_environment
    and other_workflow.route='PAPER'
    and other_workflow.state='AWAITING_PAPER_RETURN'
    and (
      other_workflow.target_timesheet_id=v_anchor.timesheet_id
      or other_workflow.anchor_timesheet_id=v_anchor.timesheet_id
    );
  if v_conflict_count<>0 then
    raise exception 'CANDIDATE_PAPER_WORKFLOW_CONFLICT' using errcode='40001';
  end if;

  v_recipient_namespace:=md5(lower(btrim(v_candidate.email)));
  v_reference:='candidate_expense_paper_send:'||v_workflow.id::text
    ||':g'||v_workflow.generation::text
    ||':manifest:'||v_manifest_sha256
    ||':anchor:'||v_anchor.timesheet_id::text
    ||':recipient:'||v_recipient_namespace;
  perform pg_advisory_xact_lock(hashtextextended(v_reference,0));

  v_scope:=jsonb_build_object(
    'job_kind','CANDIDATE_EXPENSE_PAPER_PACK',
    'candidate_mail_authority','CANDIDATE_PAPER_V1',
    'candidate_workflow_id',v_workflow.id,
    'candidate_workflow_generation',v_workflow.generation,
    'paper_return_manifest_sha256',v_manifest_sha256,
    'candidate_paper_pack_ready',false,
    'candidate_paper_pack_retryable',false,
    'candidate_paper_pack_failure_class',null,
    'candidate_paper_pack_failure_code',null,
    'candidate_paper_pack_failure_contract_version',null,
    'candidate_paper_pack_failed_at_utc',null,
    'candidate_paper_pack_preparation_started_at_utc',p_now_utc,
    'candidate_paper_pack_preparation_deadline_at_utc',p_now_utc+interval '15 minutes',
    'candidate_paper_pack_attempt_count',0,
    'candidate_paper_pack_attempt_token',null,
    'candidate_paper_pack_attempt_expires_at_utc',null,
    'candidate_paper_pack_next_retry_at_utc',null,
    'candidate_paper_generation_retired',false,
    'mail_held_until_pdf_rendered',true,
    'mail_delayed_for_pdf_render',true,
    'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING',
    'source_authority','WORKFLOW_IMMUTABLE_SUBMISSION',
    'source_authority_sha256',encode(v_workflow.immutable_submission_sha256,'hex'),
    'current_timesheet_id',v_anchor.timesheet_id,
    'current_version',v_anchor.version,
    'idempotency_key',v_reference,
    'client_idempotency_key',p_idempotency_key
  );

  select count(*)::integer into v_mail_count
  from public.mail_outbox mail_row
  where mail_row.type='TIMESHEET_QR'
    and mail_row.context_kind='timesheets'
    and mail_row.context_id=v_anchor.timesheet_id
    and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and mail_row.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
    and lower(coalesce(mail_row.payment_scope_json->>'paper_return_manifest_sha256',''))
          =v_manifest_sha256;
  if v_mail_count>1 then
    raise exception 'CANDIDATE_PAPER_OUTBOX_CONFLICT' using errcode='40001';
  end if;
  if v_mail_count=1 then
    select mail_row.* into v_mail
    from public.mail_outbox mail_row
    where mail_row.type='TIMESHEET_QR'
      and mail_row.context_kind='timesheets'
      and mail_row.context_id=v_anchor.timesheet_id
      and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and mail_row.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(mail_row.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_manifest_sha256
    for update;
    if v_mail.status='FAILED'
       or nullif(btrim(coalesce(v_mail.attempt_lease_token,'')),'') is not null
       or jsonb_typeof(v_mail.attachments)<>'array'
       or jsonb_array_length(v_mail.attachments)<>0
       or lower(coalesce(v_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            not in ('false','f','0','no')
       or lower(coalesce(v_mail.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
            not in ('true','t','1','yes')
       or v_mail.payment_scope_json->>'mail_hold_reason'<>'CANDIDATE_PAPER_PACK_PENDING' then
      raise exception 'CANDIDATE_PAPER_OUTBOX_CONFLICT' using errcode='40001';
    end if;
  else
    insert into public.mail_outbox(
      type,"to",cc,bcc,reply_to,importance,email_type,subject,body_html,body_text,
      attachments,status,last_error,created_at_utc,sent_at,created_by,reference,
      recipient_kind,recipient_id,context_kind,context_id,mailshot_run_id,
      document_template_id,provider_status,delivered_at,read_at,scheduled_for_utc,
      next_attempt_at_utc,payment_scope_json
    ) values(
      'TIMESHEET_QR',btrim(v_candidate.email),null,null,null,'Normal','html',
      'Printed expense claim – week ending '||v_workflow.week_ending_date::text,
      '<p>Please print every page of the attached expense claim, ask the manager to sign each page, and return a photograph of every signed page in MyTMS.</p>',
      'Please print every page of the attached expense claim, ask the manager to sign each page, and return a photograph of every signed page in MyTMS.',
      '[]'::jsonb,'QUEUED'::public.mail_status_enum,null,p_now_utc,null,null,v_reference,
      'candidate',v_workflow.candidate_id,'timesheets',v_anchor.timesheet_id,null,
      null,null,null,null,timestamptz '9999-12-31 00:00:00+00',
      timestamptz '9999-12-31 00:00:00+00',v_scope
    ) returning * into v_mail;
  end if;

  return jsonb_build_object(
    'ok',true,
    'queued',true,
    'operation','candidate_targetless_expense_paper_pack_enqueue',
    'mail_outbox_id',v_mail.id,
    'current_timesheet_id',v_anchor.timesheet_id,
    'current_version',v_anchor.version,
    'recipient_available',true,
    'send_state','CANDIDATE_PAPER_PACK_MAIL_HELD',
    'document_state','WORKFLOW_SOURCE_READY',
    'document_operation_id',null,
    'document_version_id',null,
    'document_version_status','WORKFLOW_SOURCE_READY'
  );
end;
$function$;

alter function public.candidate_targetless_expense_paper_pack_enqueue_v1(
  text,uuid,integer,text,timestamptz
) owner to postgres;

revoke all on function public.candidate_targetless_expense_paper_pack_enqueue_v1(
  text,uuid,integer,text,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_targetless_expense_paper_pack_enqueue_v1(
  text,uuid,integer,text,timestamptz
) to service_role;

comment on function public.candidate_targetless_expense_paper_pack_enqueue_v1(
  text,uuid,integer,text,timestamptz
) is 'Creates the held complete-pack mail authority for a target-less standalone PAPER expense without changing or reissuing its already-approved worked Timesheet anchor.';

notify pgrst, 'reload schema';

commit;
