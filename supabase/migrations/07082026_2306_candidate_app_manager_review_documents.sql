-- Candidate App official manager-review/final-timesheet document contract.
--
-- This additive migration implements the controlling Manager-Review Official
-- Timesheet Render Addendum v1.0 without adding an eighth Candidate App table.
-- Review documents remain private workflow artefacts.  They are not canonical
-- timesheet evidence and cannot consume the active TIMESHEET evidence slot.

alter table public.candidate_submission_workflows
  alter column state set default 'WORKER_DRAFT',
  add column if not exists immutable_submission_json jsonb,
  add column if not exists immutable_submission_sha256 bytea,
  add column if not exists policy_snapshot_sha256 bytea,
  add column if not exists candidate_signature_component_id uuid,
  add column if not exists candidate_signature_sha256 bytea,
  add column if not exists candidate_signed_at_utc timestamptz,
  add column if not exists review_manifest_json jsonb,
  add column if not exists review_manifest_sha256 bytea,
  add column if not exists paper_return_manifest_json jsonb,
  add column if not exists paper_return_manifest_sha256 bytea,
  add column if not exists renderer_contract_version text,
  add column if not exists manager_name text,
  add column if not exists manager_position text,
  add column if not exists manager_signature_component_id uuid,
  add column if not exists manager_signature_sha256 bytea,
  add column if not exists manager_approved_at_utc timestamptz,
  add column if not exists daily_context_sha256 bytea,
  add column if not exists canonical_financial_sha256 bytea,
  add column if not exists canonical_save_input_sha256 bytea,
  add column if not exists canonical_save_row_signature text,
  add column if not exists canonical_save_financials_id uuid references public.timesheets_financials(id) on delete restrict,
  add column if not exists canonical_save_receipt_json jsonb,
  add column if not exists canonical_saved_at_utc timestamptz;

alter table public.candidate_submission_components
  add column if not exists approval_request_id uuid,
  add column if not exists required boolean not null default false,
  add column if not exists review_ordinal integer,
  add column if not exists review_storage_key text,
  add column if not exists review_content_sha256 bytea,
  add column if not exists review_media_type text,
  add column if not exists review_byte_size bigint,
  add column if not exists review_page_count integer,
  add column if not exists review_render_input_sha256 bytea,
  add column if not exists review_renderer_contract_version text,
  add column if not exists review_renderer_receipt_json jsonb,
  add column if not exists review_generated_at_utc timestamptz,
  add column if not exists review_render_state text not null default 'NOT_REQUIRED',
  add column if not exists final_signed_storage_key text,
  add column if not exists final_signed_content_sha256 bytea,
  add column if not exists final_signed_media_type text,
  add column if not exists final_signed_byte_size bigint,
  add column if not exists final_signed_page_count integer,
  add column if not exists final_signed_render_input_sha256 bytea,
  add column if not exists final_signed_renderer_contract_version text,
  add column if not exists final_signed_renderer_receipt_json jsonb,
  add column if not exists final_signed_generated_at_utc timestamptz,
  add column if not exists final_signed_render_state text not null default 'NOT_REQUIRED',
  add column if not exists paper_return_page_key text;

alter table public.candidate_approval_requests
  add column if not exists request_generation integer not null default 1,
  add column if not exists review_manifest_sha256 bytea,
  add column if not exists required_component_ids uuid[] not null default '{}'::uuid[],
  add column if not exists required_component_manifest_json jsonb not null default '[]'::jsonb,
  add column if not exists manager_review_timesheet_component_id uuid,
  add column if not exists manager_review_timesheet_sha256 bytea,
  add column if not exists review_progress_json jsonb not null default '{}'::jsonb,
  add column if not exists review_started_at_utc timestamptz,
  add column if not exists review_completed_at_utc timestamptz;

alter table public.timesheets
  add column if not exists candidate_workflow_id uuid,
  add column if not exists candidate_workflow_generation integer,
  add column if not exists candidate_manager_approved_at_utc timestamptz;

do $migration$
begin
  alter table public.candidate_submission_workflows
    drop constraint if exists candidate_submission_workflows_state_ck;
  alter table public.candidate_submission_workflows
    add constraint candidate_submission_workflows_state_ck check (state in (
      'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
      'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
      'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
      'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
      'AWAITING_PAPER_RETURN','RECEIVED','FINALISED','REFUSED','REJECTED',
      'CANCELLED','EXPIRED','SUPERSEDED'
    ));

  alter table public.candidate_submission_components
    drop constraint if exists candidate_submission_components_kind_ck;
  alter table public.candidate_submission_components
    add constraint candidate_submission_components_kind_ck check (component_kind in (
      'HOURS_TIMESHEET','EXPENSE_SUMMARY','MILEAGE_FORM','EXPENSE_EVIDENCE',
      'SIGNED_RETURN','MANAGER_SIGNATURE','CANDIDATE_SIGNATURE','PAPER_DOCUMENT'
    ));

  alter table public.candidate_submission_components
    drop constraint if exists candidate_submission_components_role_ck;
  alter table public.candidate_submission_components
    add constraint candidate_submission_components_role_ck check (document_role in (
      'SOURCE_EVIDENCE','MILEAGE_CLAIM_FORM','EXPENSE_MILEAGE_APPROVAL_SUMMARY',
      'SIGNED_TIMESHEET','MANAGER_SIGNATURE','CANDIDATE_SIGNATURE',
      'ELECTRONIC_TIMESHEET_MANAGER_REVIEW','SIGNED_RETURN'
    ));

  if not exists (
    select 1 from pg_constraint
    where conrelid='public.candidate_submission_components'::regclass
      and conname='candidate_submission_components_approval_request_fk'
  ) then
    alter table public.candidate_submission_components
      add constraint candidate_submission_components_approval_request_fk
      foreign key (approval_request_id)
      references public.candidate_approval_requests(id) on delete restrict;
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid='public.candidate_submission_workflows'::regclass
      and conname='candidate_submission_workflows_candidate_signature_fk'
  ) then
    alter table public.candidate_submission_workflows
      add constraint candidate_submission_workflows_candidate_signature_fk
      foreign key (candidate_signature_component_id)
      references public.candidate_submission_components(id) on delete restrict;
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid='public.candidate_submission_workflows'::regclass
      and conname='candidate_submission_workflows_manager_signature_fk'
  ) then
    alter table public.candidate_submission_workflows
      add constraint candidate_submission_workflows_manager_signature_fk
      foreign key (manager_signature_component_id)
      references public.candidate_submission_components(id) on delete restrict;
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid='public.candidate_approval_requests'::regclass
      and conname='candidate_approval_requests_review_timesheet_fk'
  ) then
    alter table public.candidate_approval_requests
      add constraint candidate_approval_requests_review_timesheet_fk
      foreign key (manager_review_timesheet_component_id)
      references public.candidate_submission_components(id) on delete restrict;
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid='public.timesheets'::regclass
      and conname='timesheets_candidate_workflow_fk'
  ) then
    alter table public.timesheets
      add constraint timesheets_candidate_workflow_fk
      foreign key (candidate_workflow_id)
      references public.candidate_submission_workflows(id) on delete restrict;
  end if;
end;
$migration$;

alter table public.timesheets
  add constraint timesheets_candidate_workflow_generation_ck check (
    candidate_workflow_generation is null or candidate_workflow_generation > 0
  ),
  add constraint timesheets_candidate_workflow_pair_ck check (
    (candidate_workflow_id is null and candidate_workflow_generation is null)
    or (candidate_workflow_id is not null and candidate_workflow_generation is not null)
  );

alter table public.candidate_submission_workflows
  add constraint candidate_submission_workflows_immutable_submission_ck check (
    immutable_submission_json is null or jsonb_typeof(immutable_submission_json)='object'
  ),
  add constraint candidate_submission_workflows_hashes_ck check (
    (immutable_submission_sha256 is null or octet_length(immutable_submission_sha256)=32)
    and (policy_snapshot_sha256 is null or octet_length(policy_snapshot_sha256)=32)
    and (candidate_signature_sha256 is null or octet_length(candidate_signature_sha256)=32)
    and (review_manifest_sha256 is null or octet_length(review_manifest_sha256)=32)
    and (paper_return_manifest_sha256 is null or octet_length(paper_return_manifest_sha256)=32)
    and (manager_signature_sha256 is null or octet_length(manager_signature_sha256)=32)
    and (daily_context_sha256 is null or octet_length(daily_context_sha256)=32)
    and (canonical_financial_sha256 is null or octet_length(canonical_financial_sha256)=32)
    and (canonical_save_input_sha256 is null or octet_length(canonical_save_input_sha256)=32)
  ),
  add constraint candidate_submission_workflows_canonical_save_ck check (
    (canonical_saved_at_utc is null and canonical_save_input_sha256 is null
      and canonical_save_row_signature is null and canonical_save_financials_id is null
      and canonical_save_receipt_json is null)
    or (canonical_saved_at_utc is not null and canonical_save_input_sha256 is not null
      and nullif(btrim(canonical_save_row_signature),'') is not null
      and canonical_save_financials_id is not null
      and jsonb_typeof(canonical_save_receipt_json)='object')
  ),
  add constraint candidate_submission_workflows_paper_manifest_ck check (
    (paper_return_manifest_json is null and paper_return_manifest_sha256 is null)
    or (
      jsonb_typeof(paper_return_manifest_json)='object'
      and jsonb_typeof(paper_return_manifest_json->'pages')='array'
      and jsonb_array_length(paper_return_manifest_json->'pages')>0
      and paper_return_manifest_sha256 is not null
    )
  ),
  add constraint candidate_submission_workflows_worker_signature_ck check (
    candidate_signature_component_id is null
    or (candidate_signature_sha256 is not null and candidate_signed_at_utc is not null)
  ),
  add constraint candidate_submission_workflows_manager_signature_ck check (
    manager_signature_component_id is null
    or (
      manager_signature_sha256 is not null
      and nullif(btrim(manager_name),'') is not null
      and nullif(btrim(manager_position),'') is not null
      and manager_approved_at_utc is not null
    )
  );

alter table public.candidate_submission_components
  add constraint candidate_submission_components_approval_request_role_ck check (
    (component_kind='MANAGER_SIGNATURE' and approval_request_id is not null)
    or (component_kind<>'MANAGER_SIGNATURE' and approval_request_id is null)
  ),
  add constraint candidate_submission_components_review_ordinal_ck check (
    review_ordinal is null or review_ordinal > 0
  ),
  add constraint candidate_submission_components_review_state_ck check (
    review_render_state in ('NOT_REQUIRED','PENDING','READY','FAILED','SUPERSEDED')
  ),
  add constraint candidate_submission_components_final_state_ck check (
    final_signed_render_state in ('NOT_REQUIRED','PENDING','READY','FAILED','SUPERSEDED')
  ),
  add constraint candidate_submission_components_required_render_ck check (
    required=false
    or (
      review_ordinal is not null
      and review_render_state<>'NOT_REQUIRED'
      and final_signed_render_state<>'NOT_REQUIRED'
    )
  ),
  add constraint candidate_submission_components_review_hash_ck check (
    (review_content_sha256 is null or octet_length(review_content_sha256)=32)
    and (review_render_input_sha256 is null or octet_length(review_render_input_sha256)=32)
  ),
  add constraint candidate_submission_components_final_hash_ck check (
    (final_signed_content_sha256 is null or octet_length(final_signed_content_sha256)=32)
    and (final_signed_render_input_sha256 is null or octet_length(final_signed_render_input_sha256)=32)
  ),
  add constraint candidate_submission_components_review_size_ck check (
    review_byte_size is null or review_byte_size between 1 and 52428800
  ),
  add constraint candidate_submission_components_final_size_ck check (
    final_signed_byte_size is null or final_signed_byte_size between 1 and 52428800
  ),
  add constraint candidate_submission_components_hours_role_ck check (
    component_kind<>'HOURS_TIMESHEET'
    or (
      document_role='ELECTRONIC_TIMESHEET_MANAGER_REVIEW'
      and required=true
      and review_ordinal is not null
      and review_render_state<>'NOT_REQUIRED'
      and final_signed_render_state<>'NOT_REQUIRED'
    )
  ),
  add constraint candidate_submission_components_review_ready_ck check (
    review_render_state<>'READY'
    or (
      review_storage_key is not null
      and review_content_sha256 is not null
      and (
        review_media_type='application/pdf'
        or review_media_type like 'image/%'
      )
      and review_byte_size is not null
      and review_page_count=1
      and review_render_input_sha256 is not null
      and nullif(btrim(review_renderer_contract_version),'') is not null
      and jsonb_typeof(review_renderer_receipt_json)='object'
      and review_generated_at_utc is not null
    )
  ),
  add constraint candidate_submission_components_final_ready_ck check (
    final_signed_render_state<>'READY'
    or (
      final_signed_storage_key is not null
      and final_signed_content_sha256 is not null
      and final_signed_media_type='application/pdf'
      and final_signed_byte_size is not null
      and final_signed_page_count=1
      and final_signed_render_input_sha256 is not null
      and nullif(btrim(final_signed_renderer_contract_version),'') is not null
      and jsonb_typeof(final_signed_renderer_receipt_json)='object'
      and final_signed_generated_at_utc is not null
    )
  ),
  add constraint candidate_submission_components_render_input_match_ck check (
    review_render_state<>'READY'
    or final_signed_render_state<>'READY'
    or review_render_input_sha256=final_signed_render_input_sha256
  ),
  add constraint candidate_submission_components_paper_page_ck check (
    (component_kind='SIGNED_RETURN' and nullif(btrim(paper_return_page_key),'') is not null)
    or (component_kind<>'SIGNED_RETURN' and paper_return_page_key is null)
  );

alter table public.candidate_approval_requests
  add constraint candidate_approval_requests_request_generation_ck check (request_generation > 0),
  add constraint candidate_approval_requests_review_manifest_hash_ck check (
    review_manifest_sha256 is null or octet_length(review_manifest_sha256)=32
  ),
  add constraint candidate_approval_requests_review_timesheet_hash_ck check (
    manager_review_timesheet_sha256 is null or octet_length(manager_review_timesheet_sha256)=32
  ),
  add constraint candidate_approval_requests_required_manifest_ck check (
    jsonb_typeof(required_component_manifest_json)='array'
    and jsonb_typeof(review_progress_json)='object'
  ),
  add constraint candidate_approval_requests_live_binding_ck check (
    state not in ('PENDING','APPROVED')
    or (
      review_manifest_sha256 is not null
      and cardinality(required_component_ids)>0
      and jsonb_array_length(required_component_manifest_json)>0
    )
  );

drop index if exists public.candidate_submission_workflows_one_active_expense_uq;
create unique index candidate_submission_workflows_one_active_expense_uq
  on public.candidate_submission_workflows(candidate_id,contract_id,week_ending_date)
  where workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
    and state in (
      'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
      'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
      'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
      'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
      'AWAITING_PAPER_RETURN','RECEIVED'
    );

create unique index if not exists candidate_submission_components_hours_review_uq
  on public.candidate_submission_components(workflow_id,workflow_generation)
  where component_kind='HOURS_TIMESHEET' and state<>'SUPERSEDED';

create unique index if not exists candidate_submission_components_required_ordinal_uq
  on public.candidate_submission_components(workflow_id,workflow_generation,review_ordinal)
  where required=true and review_ordinal is not null and state<>'SUPERSEDED';

create unique index if not exists candidate_submission_components_review_storage_uq
  on public.candidate_submission_components(review_storage_key)
  where review_storage_key is not null;

create unique index if not exists candidate_submission_components_final_storage_uq
  on public.candidate_submission_components(final_signed_storage_key)
  where final_signed_storage_key is not null;

create unique index if not exists candidate_submission_components_paper_return_page_uq
  on public.candidate_submission_components(workflow_id,workflow_generation,paper_return_page_key)
  where component_kind='SIGNED_RETURN' and state<>'SUPERSEDED';

create unique index if not exists candidate_submission_components_live_manager_signature_uq
  on public.candidate_submission_components(approval_request_id)
  where component_kind='MANAGER_SIGNATURE'
    and state<>'SUPERSEDED'
    and approval_request_id is not null;

create index if not exists candidate_submission_components_approval_request_idx
  on public.candidate_submission_components(approval_request_id)
  where approval_request_id is not null;

create index if not exists candidate_submission_workflows_candidate_signature_idx
  on public.candidate_submission_workflows(candidate_signature_component_id)
  where candidate_signature_component_id is not null;

create index if not exists candidate_submission_workflows_manager_signature_idx
  on public.candidate_submission_workflows(manager_signature_component_id)
  where manager_signature_component_id is not null;

create index if not exists candidate_approval_requests_review_timesheet_idx
  on public.candidate_approval_requests(manager_review_timesheet_component_id)
  where manager_review_timesheet_component_id is not null;

create index if not exists timesheets_candidate_workflow_idx
  on public.timesheets(candidate_workflow_id,candidate_workflow_generation)
  where candidate_workflow_id is not null;

-- The existing office additional-DAILY route may be retried after a Worker or
-- network failure. One operation key can own only one additional row for its
-- parent, so a retry reuses/recalculates that row instead of creating another.
create unique index if not exists timesheets_manual_adjustment_idempotency_uq
  on public.timesheets(parent_timesheet_id,idempotency_key)
  where adjustment_origin='MANUAL_ADJUSTMENT'
    and parent_timesheet_id is not null
    and nullif(btrim(idempotency_key),'') is not null;

comment on column public.candidate_submission_workflows.immutable_submission_json is
  'Frozen worker-submitted business content used by both official review and final electronic timesheet renders.';
comment on column public.candidate_submission_workflows.daily_context_sha256 is
  'SHA-256 of the server-authoritative locked DAILY role, band, client, work-date, route, policy and pre-save lifecycle context.';
comment on column public.candidate_submission_workflows.canonical_financial_sha256 is
  'SHA-256 of the final canonical TSFIN business/economic content. Used only as exact-content lineage proof; it does not calculate economics.';
comment on column public.candidate_submission_workflows.review_manifest_sha256 is
  'SHA-256 of the exact ordered required review manifest for the current workflow generation.';
comment on column public.candidate_submission_workflows.paper_return_manifest_sha256 is
  'SHA-256 of the exact generation-bound paper/QR page set that must be returned before the workflow can be received.';
comment on column public.candidate_submission_components.review_storage_key is
  'Private pre-approval review artefact. It is never canonical timesheet evidence and may represent an official timesheet or one expense page.';
comment on column public.candidate_submission_components.final_signed_storage_key is
  'Private CloudTMS-generated final manager-signed derivative for this exact reviewed page.';
comment on column public.candidate_submission_components.paper_return_page_key is
  'Exact generation-bound page key from the frozen paper-return manifest. Required only for SIGNED_RETURN components.';
comment on column public.candidate_submission_components.approval_request_id is
  'Exact live manager approval request that authorised creation of this manager-signature component.';
comment on column public.candidate_approval_requests.required_component_ids is
  'Exact ordered required review component IDs bound to this approval request and workflow generation.';
comment on column public.timesheets.candidate_manager_approved_at_utc is
  'Manager approval time for a Candidate App electronic submission. This is distinct from office authorisation in authorised_at_server.';
