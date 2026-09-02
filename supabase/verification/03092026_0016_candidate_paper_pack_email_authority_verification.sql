\set ON_ERROR_STOP on

do $verification$
declare
  v_definition text;
begin
  select pg_get_functiondef(
    'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,bigint,jsonb,text,timestamptz)'::regprocedure
  ) into v_definition;

  if v_definition !~* 'candidate_mail_authority[^\n]+CANDIDATE_PAPER_V1[^\n]+CANDIDATE_PAPER_PACK_EMAIL_V1'
  then
    raise exception 'CANDIDATE_PAPER_PACK_EMAIL_PROVIDER_AUTHORITY_MISSING';
  end if;

  if exists(
    select 1
    from public.mail_outbox mail
    where mail.type='TIMESHEET_QR'
      and mail.context_kind='timesheets'
      and mail.reference like 'candidate-paper-pack-email:%'
      and mail.deterministic_outbox_key like 'CANDIDATE\_PAPER\_PACK\_EMAIL:%' escape '\'
      and mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and lower(coalesce(mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in ('true','t','1','yes')
      and lower(coalesce(mail.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
            in ('false','f','0','no')
      and nullif(btrim(coalesce(mail.payment_scope_json->>'mail_hold_reason','')),'') is null
      and jsonb_typeof(mail.attachments)='array'
      and jsonb_array_length(mail.attachments)=1
  ) then
    raise exception 'CANDIDATE_PAPER_PACK_EMAIL_AUTHORITY_REPAIR_INCOMPLETE';
  end if;
end;
$verification$;
