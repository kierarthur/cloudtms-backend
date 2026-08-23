-- Upgrade only the untouched stock initial manager-email wording. Agency
-- customisations are preserved. Candidate identity in the final CTA is added
-- by the service renderer and is therefore not an editable template token.

\set ON_ERROR_STOP on

begin;

do $migration$
declare
  v_settings public.settings_defaults%rowtype;
  v_templates jsonb;
  v_version bigint;
  v_hash bytea;
  v_changed boolean:=false;
  v_body constant text:='The below candidate has submitted a timesheet or expenses for approval. You can approve or refuse the complete submission using the secure link below.';
  v_html constant text:='<p>The below candidate has submitted a timesheet or expenses for approval. You can approve or refuse the complete submission using the secure link below.</p>';
begin
  select * into strict v_settings from public.settings_defaults where id=1 for update;
  v_templates:=v_settings.candidate_manager_email_templates_json;

  if v_templates#>>'{TIMESHEET,INITIAL,body_text}'=
      'Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.' then
    v_templates:=jsonb_set(v_templates,'{TIMESHEET,INITIAL,body_text}',to_jsonb(v_body),false);
    v_templates:=jsonb_set(v_templates,'{TIMESHEET,INITIAL,body_html}',to_jsonb(v_html),false);
    v_changed:=true;
  end if;
  if v_templates#>>'{EXPENSE_CLAIM,INITIAL,body_text}'=
      'Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.' then
    v_templates:=jsonb_set(v_templates,'{EXPENSE_CLAIM,INITIAL,body_text}',to_jsonb(v_body),false);
    v_templates:=jsonb_set(v_templates,'{EXPENSE_CLAIM,INITIAL,body_html}',to_jsonb(v_html),false);
    v_changed:=true;
  end if;

  if v_changed then
    v_version:=v_settings.candidate_manager_email_templates_version+1;
    v_hash:=extensions.digest(pg_catalog.convert_to(v_templates::text,'UTF8'),'sha256');
    update public.settings_defaults set
      candidate_manager_email_templates_json=v_templates,
      candidate_manager_email_templates_version=v_version,
      candidate_manager_email_templates_sha256=v_hash,
      candidate_manager_email_templates_updated_at_utc=pg_catalog.transaction_timestamp(),
      candidate_manager_email_templates_updated_by_hmac=null
    where id=1;
    insert into public.candidate_manager_email_template_versions(
      version,templates_json,sanitizer_policy_version,semantic_sha256,
      actor_identity_hmac,idempotency_key,reason_code,recorded_at_utc
    ) values (
      v_version,v_templates,v_settings.candidate_manager_email_sanitizer_policy_version,v_hash,
      null,'manager-email-candidate-identity-defaults-20260823','OFFICE_SAVE',
      pg_catalog.transaction_timestamp()
    );
  end if;
end
$migration$;

commit;
