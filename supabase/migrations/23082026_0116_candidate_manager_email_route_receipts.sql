-- Additive agency-side manager EMAIL route receipt, template and signature
-- evidence authority. No Candidate business row or feature flag is activated.

create table public.candidate_manager_email_route_receipts (
  route_receipt_id uuid primary key default pg_catalog.gen_random_uuid(),
  workflow_id uuid not null references public.candidate_submission_workflows(id) on delete restrict,
  approval_request_id uuid not null references public.candidate_approval_requests(id) on delete restrict,
  request_generation integer not null check (request_generation >= 1),
  credential_generation integer not null check (credential_generation >= 1),
  mail_kind text not null check (mail_kind in ('INITIAL','REMINDER','RENEWAL','WITHDRAWAL','CANCELLATION')),
  manager_token_hash_snapshot bytea not null check (pg_catalog.octet_length(manager_token_hash_snapshot)=32),
  manager_route_ticket_id uuid not null,
  route_revision bigint not null check (route_revision >= 1),
  registration_receipt_sha256 bytea not null check (pg_catalog.octet_length(registration_receipt_sha256)=32),
  route_semantic_sha256 bytea not null check (pg_catalog.octet_length(route_semantic_sha256)=32),
  idempotency_key text not null check (pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200),
  state text not null default 'REGISTERED' check (state in ('REGISTERED','CURRENT','RETIRED','FAILED')),
  failure_code text check (failure_code is null or failure_code ~ '^[A-Z][A-Z0-9_]{2,99}$'),
  registered_at_utc timestamptz not null,
  current_at_utc timestamptz,
  retired_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (approval_request_id, credential_generation),
  unique (workflow_id, idempotency_key),
  check ((state='REGISTERED' and current_at_utc is null and retired_at_utc is null)
    or (state='CURRENT' and current_at_utc is not null and retired_at_utc is null)
    or (state in ('RETIRED','FAILED') and retired_at_utc is not null))
);
alter table public.candidate_manager_email_route_receipts owner to postgres;

create unique index candidate_manager_email_route_one_current_uq
  on public.candidate_manager_email_route_receipts(approval_request_id) where state='CURRENT';
create index candidate_manager_email_route_workflow_idx
  on public.candidate_manager_email_route_receipts(workflow_id,state,request_generation,credential_generation);
create index candidate_manager_email_route_ticket_idx
  on public.candidate_manager_email_route_receipts(manager_route_ticket_id,route_revision);

alter table public.candidate_approval_requests
  add column current_manager_route_receipt_id uuid
    references public.candidate_manager_email_route_receipts(route_receipt_id) on delete restrict;
create unique index candidate_approval_requests_current_route_receipt_uq
  on public.candidate_approval_requests(current_manager_route_receipt_id)
  where current_manager_route_receipt_id is not null;

alter table public.candidate_submission_components
  add column manager_signature_capture_method text
    check (manager_signature_capture_method is null or manager_signature_capture_method in ('DRAW','UPLOAD')),
  add column expected_source_content_sha256 bytea
    check (expected_source_content_sha256 is null or pg_catalog.octet_length(expected_source_content_sha256)=32),
  add column validated_image_width integer check (validated_image_width is null or validated_image_width between 1 and 10000),
  add column validated_image_height integer check (validated_image_height is null or validated_image_height between 1 and 10000);

alter table public.settings_defaults
  add column candidate_manager_email_templates_json jsonb not null default
  '{
    "schema_version":"CANDIDATE_MANAGER_EMAIL_TEMPLATES_V1",
    "TIMESHEET":{
      "INITIAL":{"subject":"Timesheet approval required","body_text":"Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.","body_html":"<p>Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.</p>","button_text":"Review and approve","include_link":true},
      "REMINDER":{"subject":"Reminder: timesheet approval required","body_text":"This is a reminder to review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.","body_html":"<p>This is a reminder to review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.</p>","button_text":"Review and approve","include_link":true},
      "RENEWAL":{"subject":"New timesheet approval link","body_text":"A new secure link has been issued because the previous link expired. Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.","body_html":"<p>A new secure link has been issued because the previous link expired. Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.</p>","button_text":"Review and approve","include_link":true},
      "WITHDRAWAL":{"subject":"Timesheet approval request withdrawn","body_text":"The approval request for this timesheet has been withdrawn. The previous secure link is no longer valid. No further action is required.","body_html":"<p>The approval request for this timesheet has been withdrawn. The previous secure link is no longer valid. No further action is required.</p>","button_text":null,"include_link":false},
      "CANCELLATION":{"subject":"Timesheet approval request cancelled","body_text":"The approval request for this timesheet has been cancelled. The previous secure link is no longer valid. No further action is required.","body_html":"<p>The approval request for this timesheet has been cancelled. The previous secure link is no longer valid. No further action is required.</p>","button_text":null,"include_link":false}
    },
    "EXPENSE_CLAIM":{
      "INITIAL":{"subject":"Expense approval required","body_text":"Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.","body_html":"<p>Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.</p>","button_text":"Review and approve","include_link":true},
      "REMINDER":{"subject":"Reminder: expense approval required","body_text":"This is a reminder to review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.","body_html":"<p>This is a reminder to review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.</p>","button_text":"Review and approve","include_link":true},
      "RENEWAL":{"subject":"New expense approval link","body_text":"A new secure link has been issued because the previous link expired. Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.","body_html":"<p>A new secure link has been issued because the previous link expired. Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.</p>","button_text":"Review and approve","include_link":true},
      "WITHDRAWAL":{"subject":"Expense approval request withdrawn","body_text":"The approval request for this expense claim has been withdrawn. The previous secure link is no longer valid. No further action is required.","body_html":"<p>The approval request for this expense claim has been withdrawn. The previous secure link is no longer valid. No further action is required.</p>","button_text":null,"include_link":false},
      "CANCELLATION":{"subject":"Expense approval request cancelled","body_text":"The approval request for this expense claim has been cancelled. The previous secure link is no longer valid. No further action is required.","body_html":"<p>The approval request for this expense claim has been cancelled. The previous secure link is no longer valid. No further action is required.</p>","button_text":null,"include_link":false}
    }
  }'::jsonb,
  add column candidate_manager_email_templates_version bigint not null default 1 check (candidate_manager_email_templates_version>=1),
  add column candidate_manager_email_sanitizer_policy_version text not null default 'MANAGER_EMAIL_SAFE_HTML_V1',
  add column candidate_manager_email_templates_sha256 bytea,
  add column candidate_manager_email_templates_updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  add column candidate_manager_email_templates_updated_by_hmac bytea
    check (candidate_manager_email_templates_updated_by_hmac is null or pg_catalog.octet_length(candidate_manager_email_templates_updated_by_hmac)=32),
  add constraint settings_defaults_candidate_manager_email_templates_ck check (
    pg_catalog.jsonb_typeof(candidate_manager_email_templates_json)='object'
    and candidate_manager_email_templates_json->>'schema_version'='CANDIDATE_MANAGER_EMAIL_TEMPLATES_V1'
    and candidate_manager_email_templates_json ? 'TIMESHEET'
    and candidate_manager_email_templates_json ? 'EXPENSE_CLAIM'
  );

update public.settings_defaults
set candidate_manager_email_templates_sha256=extensions.digest(
  pg_catalog.convert_to(candidate_manager_email_templates_json::text,'UTF8'),'sha256'
)
where candidate_manager_email_templates_sha256 is null;

alter table public.settings_defaults
  alter column candidate_manager_email_templates_sha256 set not null,
  add constraint settings_defaults_candidate_manager_email_templates_hash_ck
    check (pg_catalog.octet_length(candidate_manager_email_templates_sha256)=32);

create table public.candidate_manager_email_template_versions (
  template_version_id uuid primary key default pg_catalog.gen_random_uuid(),
  version bigint not null unique check (version >= 1),
  templates_json jsonb not null,
  sanitizer_policy_version text not null,
  semantic_sha256 bytea not null check (pg_catalog.octet_length(semantic_sha256)=32),
  actor_identity_hmac bytea check (actor_identity_hmac is null or pg_catalog.octet_length(actor_identity_hmac)=32),
  idempotency_key text not null unique check (pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200),
  reason_code text not null check (reason_code in ('BOOTSTRAP','OFFICE_SAVE','OFFICE_RESET')),
  recorded_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);
alter table public.candidate_manager_email_template_versions owner to postgres;

insert into public.candidate_manager_email_template_versions(
  version,templates_json,sanitizer_policy_version,semantic_sha256,
  actor_identity_hmac,idempotency_key,reason_code,recorded_at_utc
)
select candidate_manager_email_templates_version,candidate_manager_email_templates_json,
  candidate_manager_email_sanitizer_policy_version,candidate_manager_email_templates_sha256,
  null,'manager-email-template-bootstrap-v1','BOOTSTRAP',pg_catalog.transaction_timestamp()
from public.settings_defaults where id=1;

alter table public.candidate_manager_email_route_receipts enable row level security;
alter table public.candidate_manager_email_route_receipts force row level security;
alter table public.candidate_manager_email_template_versions enable row level security;
alter table public.candidate_manager_email_template_versions force row level security;
revoke all on public.candidate_manager_email_route_receipts from public,anon,authenticated;
revoke all on public.candidate_manager_email_route_receipts from service_role;
revoke all on public.candidate_manager_email_template_versions from public,anon,authenticated;
revoke all on public.candidate_manager_email_template_versions from service_role;

do $verification$
begin
  if exists(select 1 from public.candidate_manager_email_route_receipts)
     or (select pg_catalog.count(*) from public.candidate_approval_requests where current_manager_route_receipt_id is not null)<>0
     or exists(select 1 from public.candidate_submission_components where manager_signature_capture_method is not null)
  then raise exception using errcode='check_violation',message='CANDIDATE_MANAGER_EMAIL_AUTHORITY_INSTALL_NOT_INERT'; end if;
end
$verification$;
