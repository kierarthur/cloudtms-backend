-- One-time repair for Candidate on-demand PAPER pack email rows created while
-- the sender incorrectly labelled them as the one canonical pack-delivery row.
-- The deterministic key/reference pair proves that these are explicit
-- candidate requests; canonical pack rows use a different key and reference.

\set ON_ERROR_STOP on

begin;

update public.mail_outbox mail
set payment_scope_json=jsonb_set(
  mail.payment_scope_json,
  '{candidate_mail_authority}',
  '"CANDIDATE_PAPER_PACK_EMAIL_V1"'::jsonb,
  false
)
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
  and jsonb_array_length(mail.attachments)=1;

do $verification$
begin
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

commit;
