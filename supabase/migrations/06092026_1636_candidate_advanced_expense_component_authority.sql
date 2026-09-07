-- One-time CloudTMS schema/data migration: candidate_advanced_expense_component_authority
--
-- A Candidate expense category is an independently addressable business
-- component.  Receipt/document rows remain immutable evidence; they are not a
-- safe category identity because one category may contain several receipts.
-- This ledger adds that missing stable identity without changing financial,
-- invoicing, settlement or Banking Pay authority.

\set ON_ERROR_STOP on

begin;

create table public.candidate_expense_components (
  expense_component_id uuid primary key default pg_catalog.gen_random_uuid(),
  workflow_id uuid not null
    references public.candidate_submission_workflows(id) on delete restrict,
  workflow_generation integer not null check (workflow_generation >= 1),
  component_generation integer not null default 1 check (component_generation >= 1),
  expense_category text not null
    check (expense_category in ('MILEAGE','TRAVEL','ACCOMMODATION','OTHER')),
  owning_timesheet_id uuid
    references public.timesheets(timesheet_id) on delete set null,
  amount numeric not null default 0 check (amount >= 0),
  mileage_units numeric not null default 0 check (mileage_units >= 0),
  lifecycle_state text not null check (lifecycle_state in (
    'DRAFT','SUBMITTED','MANAGER_APPROVED','MANAGER_REFUSED','OFFICE_REJECTED',
    'WITHDRAWN','CANCELLED','SUPERSEDED'
  )),
  manager_approval_state text not null check (manager_approval_state in (
    'NOT_REQUESTED','PENDING','APPROVED','REFUSED'
  )),
  agency_authorisation_state text not null default 'NOT_AUTHORISED'
    check (agency_authorisation_state in ('NOT_AUTHORISED','AUTHORISED','INVOICED','PAID')),
  approval_request_id uuid
    references public.candidate_approval_requests(id) on delete restrict,
  submitted_at_utc timestamptz,
  manager_approved_at_utc timestamptz,
  refusal_kind text check (refusal_kind is null or refusal_kind in ('MANAGER_REFUSAL','AGENCY_REJECTION')),
  refusal_reason text check (refusal_reason is null or pg_catalog.char_length(refusal_reason) between 1 and 1000),
  refused_at_utc timestamptz,
  removed_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (workflow_id,expense_category),
  check ((expense_category='MILEAGE') or mileage_units=0),
  check ((lifecycle_state in ('MANAGER_REFUSED','OFFICE_REJECTED'))=(refusal_kind is not null)),
  check ((lifecycle_state='MANAGER_REFUSED')=(refusal_kind='MANAGER_REFUSAL')),
  check ((lifecycle_state='OFFICE_REJECTED')=(refusal_kind='AGENCY_REJECTION')),
  check ((lifecycle_state in ('MANAGER_REFUSED','OFFICE_REJECTED'))=
    (refusal_reason is not null and refused_at_utc is not null)),
  check (refusal_kind is distinct from 'MANAGER_REFUSAL' or approval_request_id is not null),
  check ((lifecycle_state in ('OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'))=(removed_at_utc is not null))
);
alter table public.candidate_expense_components owner to postgres;

create index candidate_expense_components_timesheet_state_idx
  on public.candidate_expense_components(owning_timesheet_id,lifecycle_state,expense_category);
create index candidate_expense_components_workflow_state_idx
  on public.candidate_expense_components(workflow_id,lifecycle_state,expense_category);
create index candidate_expense_components_approval_idx
  on public.candidate_expense_components(approval_request_id)
  where approval_request_id is not null;

create table public.candidate_expense_component_events (
  event_id uuid primary key default pg_catalog.gen_random_uuid(),
  expense_component_id uuid not null
    references public.candidate_expense_components(expense_component_id) on delete restrict,
  workflow_id uuid not null
    references public.candidate_submission_workflows(id) on delete restrict,
  component_generation integer not null check (component_generation >= 1),
  event_type text not null check (event_type in (
    'CREATED','SUBMITTED','MANAGER_APPROVED','MANAGER_REFUSED','OFFICE_REJECTED',
    'REPLACED','WITHDRAWN','CANCELLED','SUPERSEDED','AUTHORISED','INVOICED','PAID'
    ,'AGENCY_PROTECTION_CLEARED','OWNER_ROTATED'
  )),
  actor_kind text not null check (actor_kind in ('SYSTEM','CANDIDATE','MANAGER','OFFICE')),
  actor_id uuid,
  reason text check (reason is null or pg_catalog.char_length(reason) between 1 and 1000),
  before_state_json jsonb not null default '{}'::jsonb
    check (pg_catalog.jsonb_typeof(before_state_json)='object'),
  after_state_json jsonb not null default '{}'::jsonb
    check (pg_catalog.jsonb_typeof(after_state_json)='object'),
  idempotency_key text not null
    check (pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200),
  occurred_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (expense_component_id,idempotency_key)
);
alter table public.candidate_expense_component_events owner to postgres;
create index candidate_expense_component_events_component_idx
  on public.candidate_expense_component_events(expense_component_id,occurred_at_utc,event_id);

create table public.candidate_pending_expense_updates (
  update_id uuid primary key default pg_catalog.gen_random_uuid(),
  workflow_id uuid not null
    references public.candidate_submission_workflows(id) on delete restrict,
  approval_request_id uuid
    references public.candidate_approval_requests(id) on delete restrict,
  from_workflow_generation integer not null check (from_workflow_generation >= 1),
  current_workflow_generation integer not null check (current_workflow_generation >= 1),
  update_plan_json jsonb not null
    check (
      pg_catalog.jsonb_typeof(update_plan_json)='array'
      and pg_catalog.jsonb_array_length(update_plan_json) between 1 and 4
    ),
  state text not null check (state in ('EDITING','RENDERING','COMMITTED','ABORTED','FAILED')),
  update_mode text not null default 'PENDING_MANAGER'
    check (update_mode in ('PENDING_MANAGER','PAPER_REPLACEMENT')),
  actor_kind text not null default 'CANDIDATE'
    check (actor_kind in ('CANDIDATE','OFFICE')),
  actor_id uuid,
  terminal_lifecycle_state text
    check (terminal_lifecycle_state is null or terminal_lifecycle_state in ('WITHDRAWN','OFFICE_REJECTED')),
  reason_note text check (reason_note is null or pg_catalog.char_length(reason_note) between 1 and 1000),
  operation_id uuid,
  begin_request_sha256 bytea
    check (begin_request_sha256 is null or pg_catalog.octet_length(begin_request_sha256)=32),
  begin_result_json jsonb
    check (begin_result_json is null or pg_catalog.jsonb_typeof(begin_result_json)='object'),
  submit_idempotency_key text
    check (submit_idempotency_key is null or pg_catalog.char_length(pg_catalog.btrim(submit_idempotency_key)) between 1 and 200),
  submit_request_sha256 bytea
    check (submit_request_sha256 is null or pg_catalog.octet_length(submit_request_sha256)=32),
  submit_result_json jsonb
    check (submit_result_json is null or pg_catalog.jsonb_typeof(submit_result_json)='object'),
  rebind_idempotency_key text
    check (rebind_idempotency_key is null or pg_catalog.char_length(pg_catalog.btrim(rebind_idempotency_key)) between 1 and 200),
  rebind_request_sha256 bytea
    check (rebind_request_sha256 is null or pg_catalog.octet_length(rebind_request_sha256)=32),
  rebind_result_json jsonb
    check (rebind_result_json is null or pg_catalog.jsonb_typeof(rebind_result_json)='object'),
  abort_idempotency_key text
    check (abort_idempotency_key is null or pg_catalog.char_length(pg_catalog.btrim(abort_idempotency_key)) between 1 and 200),
  abort_request_sha256 bytea
    check (abort_request_sha256 is null or pg_catalog.octet_length(abort_request_sha256)=32),
  abort_result_json jsonb
    check (abort_result_json is null or pg_catalog.jsonb_typeof(abort_result_json)='object'),
  prior_workflow_state text not null,
  prior_workflow_snapshot_json jsonb not null
    check (pg_catalog.jsonb_typeof(prior_workflow_snapshot_json)='object'),
  prior_immutable_submission_json jsonb not null
    check (pg_catalog.jsonb_typeof(prior_immutable_submission_json)='object'),
  prior_immutable_submission_sha256 bytea not null
    check (pg_catalog.octet_length(prior_immutable_submission_sha256)=32),
  prior_review_manifest_json jsonb not null
    check (pg_catalog.jsonb_typeof(prior_review_manifest_json)='object'),
  prior_review_manifest_sha256 bytea not null
    check (pg_catalog.octet_length(prior_review_manifest_sha256)=32),
  prior_paper_source_timesheet_id uuid
    references public.timesheets(timesheet_id) on delete set null,
  prior_paper_source_timesheet_id_snapshot uuid,
  prior_paper_source_snapshot_json jsonb
    check (prior_paper_source_snapshot_json is null
      or pg_catalog.jsonb_typeof(prior_paper_source_snapshot_json)='object'),
  prior_paper_source_snapshot_sha256 bytea
    check (prior_paper_source_snapshot_sha256 is null
      or pg_catalog.octet_length(prior_paper_source_snapshot_sha256)=32),
  failure_code text check (failure_code is null or failure_code ~ '^[A-Z][A-Z0-9_]{2,99}$'),
  idempotency_key text not null
    check (pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200),
  started_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  completed_at_utc timestamptz,
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (workflow_id,idempotency_key),
  check (expires_at_utc>started_at_utc),
  check ((submit_idempotency_key is null)=(submit_request_sha256 is null)),
  check ((rebind_idempotency_key is null)=(rebind_request_sha256 is null)),
  check ((rebind_idempotency_key is null)=(rebind_result_json is null)),
  check ((begin_request_sha256 is null)=(begin_result_json is null)),
  check ((abort_idempotency_key is null)=(abort_request_sha256 is null)),
  check ((abort_idempotency_key is null)=(abort_result_json is null)),
  check ((update_mode='PAPER_REPLACEMENT')=
    (prior_paper_source_timesheet_id_snapshot is not null
      and prior_paper_source_snapshot_json is not null
      and prior_paper_source_snapshot_sha256 is not null)),
  check (update_mode<>'PAPER_REPLACEMENT'
    or state in ('COMMITTED','ABORTED','FAILED')
    or prior_paper_source_timesheet_id is not null),
  check ((state in ('COMMITTED','ABORTED','FAILED'))=(completed_at_utc is not null))
);
alter table public.candidate_pending_expense_updates owner to postgres;
create unique index candidate_pending_expense_updates_one_active_uq
  on public.candidate_pending_expense_updates(workflow_id)
  where state in ('EDITING','RENDERING');

create table public.candidate_expense_summary_refreshes (
  refresh_id uuid primary key default pg_catalog.gen_random_uuid(),
  -- Keep a render-attempt tombstone after an empty carrier is deleted. A
  -- Worker may already have written the immutable PDF and still needs this
  -- row to durably enqueue exact R2 cleanup.
  timesheet_id uuid
    references public.timesheets(timesheet_id) on delete set null,
  timesheet_id_snapshot uuid not null,
  summary_generation integer not null check (summary_generation >= 1),
  state text not null check (state in ('PENDING','RENDERING','READY','REMOVED','FAILED','SUPERSEDED')),
  totals_json jsonb not null check (pg_catalog.jsonb_typeof(totals_json)='object'),
  totals_sha256 bytea not null check (pg_catalog.octet_length(totals_sha256)=32),
  source_financials_id uuid,
  summary_storage_key text,
  summary_sha256 bytea check (summary_sha256 is null or pg_catalog.octet_length(summary_sha256)=32),
  failure_code text check (failure_code is null or failure_code ~ '^[A-Z][A-Z0-9_]{2,99}$'),
  attempt_count integer not null default 0 check (attempt_count between 0 and 5),
  claim_token uuid,
  attempt_storage_key text,
  claimed_at_utc timestamptz,
  lease_expires_at_utc timestamptz,
  idempotency_key text not null
    check (pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200),
  requested_at_utc timestamptz not null,
  completed_at_utc timestamptz,
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (timesheet_id_snapshot,summary_generation),
  unique (timesheet_id_snapshot,idempotency_key),
  check ((state in ('READY','REMOVED','FAILED','SUPERSEDED'))=(completed_at_utc is not null)),
  check (state<>'RENDERING' or (claim_token is not null and claimed_at_utc is not null and lease_expires_at_utc is not null)),
  check ((state='READY')=(summary_storage_key is not null and summary_sha256 is not null))
);
alter table public.candidate_expense_summary_refreshes owner to postgres;
create unique index candidate_expense_summary_refreshes_one_pending_uq
  on public.candidate_expense_summary_refreshes(timesheet_id) where state in ('PENDING','RENDERING');

-- Every user-visible mutation has one durable, exact replay receipt.  The
-- request digest prevents reuse of an idempotency key for different work;
-- result_json is written only in the same commit as the authoritative change.
create table public.candidate_expense_operations (
  operation_id uuid primary key default pg_catalog.gen_random_uuid(),
  environment text not null check (environment in ('TEST','LIVE')),
  account_id uuid references public.candidate_app_accounts(id) on delete restrict,
  candidate_id uuid references public.candidates(id) on delete restrict,
  actor_kind text not null check (actor_kind in ('CANDIDATE','OFFICE','SYSTEM')),
  actor_id uuid,
  action_code text not null check (action_code in (
    'REMOVE_EXPENSE','WITHDRAW_EXPENSE','CANCEL_EXPENSE',
    'WITHDRAW_ENTIRE_CLAIM','CANCEL_ENTIRE_CLAIM',
    'REJECT_EXPENSE_CATEGORY','RESUBMIT_EXPENSE_CATEGORY','CREATE_UPDATED_DOCUMENTS'
  )),
  workflow_id uuid references public.candidate_submission_workflows(id) on delete restrict,
  timesheet_id uuid references public.timesheets(timesheet_id) on delete set null,
  expense_component_id uuid references public.candidate_expense_components(expense_component_id) on delete restrict,
  request_sha256 bytea not null check (pg_catalog.octet_length(request_sha256)=32),
  idempotency_key text not null check (pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200),
  state text not null check (state in ('PREPARING','RENDERING','COMMITTED','ABORTED','FAILED')),
  progress_json jsonb check (progress_json is null or pg_catalog.jsonb_typeof(progress_json)='object'),
  result_json jsonb check (result_json is null or pg_catalog.jsonb_typeof(result_json)='object'),
  failure_code text check (failure_code is null or failure_code ~ '^[A-Z][A-Z0-9_]{2,99}$'),
  created_at_utc timestamptz not null,
  updated_at_utc timestamptz not null,
  completed_at_utc timestamptz,
  unique nulls not distinct (environment,actor_kind,actor_id,idempotency_key),
  check ((state in ('COMMITTED','ABORTED','FAILED'))=(completed_at_utc is not null)),
  check (state<>'COMMITTED' or result_json is not null),
  check (result_json is null or state in ('COMMITTED','ABORTED','FAILED'))
);
alter table public.candidate_expense_operations owner to postgres;
create index candidate_expense_operations_workflow_idx
  on public.candidate_expense_operations(workflow_id,created_at_utc,operation_id);
alter table public.candidate_pending_expense_updates
  add constraint candidate_pending_expense_updates_operation_fk
  foreign key(operation_id) references public.candidate_expense_operations(operation_id)
  on delete restrict;

alter table public.candidate_expense_components enable row level security;
alter table public.candidate_expense_components force row level security;
alter table public.candidate_expense_component_events enable row level security;
alter table public.candidate_expense_component_events force row level security;
alter table public.candidate_pending_expense_updates enable row level security;
alter table public.candidate_pending_expense_updates force row level security;
alter table public.candidate_expense_summary_refreshes enable row level security;
alter table public.candidate_expense_summary_refreshes force row level security;
alter table public.candidate_expense_operations enable row level security;
alter table public.candidate_expense_operations force row level security;

do $service_owner_policies$
declare v_table text;
begin
  foreach v_table in array array[
    'candidate_expense_components','candidate_expense_component_events',
    'candidate_pending_expense_updates','candidate_expense_summary_refreshes',
    'candidate_expense_operations'
  ] loop
    execute pg_catalog.format(
      'create policy cloudtms_miget_service_owner_all on public.%I for all to %I, service_role using (true) with check (true)',
      v_table,current_user
    );
  end loop;
end;
$service_owner_policies$;

revoke all on public.candidate_expense_components from public,anon,authenticated,service_role;
revoke all on public.candidate_expense_component_events from public,anon,authenticated,service_role;
revoke all on public.candidate_pending_expense_updates from public,anon,authenticated,service_role;
revoke all on public.candidate_expense_summary_refreshes from public,anon,authenticated,service_role;
revoke all on public.candidate_expense_operations from public,anon,authenticated,service_role;

-- Existing completed and in-flight expense workflows receive one stable row
-- per category.  Values are taken from their frozen Candidate submission,
-- while the owning Timesheet is used only for the current agency protection
-- state.  No economic value is recalculated or changed by this backfill.
with workflow_claims as (
  select workflow.*,
    coalesce(
      workflow.immutable_submission_json#>'{expense_submission,canonical_tsfin_snapshot}',
      workflow.immutable_submission_json->'expense_submission',
      workflow.immutable_submission_json#>'{expense_claim,canonical_tsfin_snapshot}',
      workflow.immutable_submission_json->'expense_claim',
      '{}'::jsonb
    ) as claim,
    case when workflow.state='FINALISED' then greatest(workflow.generation-1,1)
      else workflow.generation end as document_generation
  from public.candidate_submission_workflows workflow
  where workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
    and workflow.immutable_submission_json is not null
), category_values as (
  select workflow_claims.*,
    category.expense_category,
    case category.expense_category
      when 'MILEAGE' then coalesce(nullif(claim->>'mileage_pay_ex_vat','')::numeric,0)
      when 'TRAVEL' then coalesce(nullif(claim->>'travel_pay_ex_vat','')::numeric,0)
      when 'ACCOMMODATION' then coalesce(nullif(claim->>'accommodation_pay_ex_vat','')::numeric,0)
      else coalesce(nullif(claim->>'other_pay_ex_vat','')::numeric,0)
    end as amount,
    case when category.expense_category='MILEAGE'
      then coalesce(nullif(claim->>'mileage_units','')::numeric,0) else 0 end as mileage_units
  from workflow_claims
  cross join (values ('MILEAGE'),('TRAVEL'),('ACCOMMODATION'),('OTHER')) category(expense_category)
), backfill as (
  select category_values.*,
    approval.id as approval_request_id,
    approval.state as approval_state,
    approval.approved_at_utc,
    approval.refused_at_utc,
    approval.refusal_reason,
    financial.authorised_at_utc,
    financial.locked_by_invoice_id,
    financial.paid_at_utc,
    timesheet.authorised_at_server,
    timesheet.status::text as timesheet_status,
    exists (
      select 1 from public.candidate_submission_components evidence
      where evidence.workflow_id=category_values.id
        and evidence.workflow_generation=category_values.document_generation
        and evidence.expense_category=category_values.expense_category
        and evidence.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
        and evidence.state not in ('SUPERSEDED','REJECTED','ABANDONED')
    ) as has_evidence
  from category_values
  left join lateral (
    select request.* from public.candidate_approval_requests request
    where request.workflow_id=category_values.id
      and request.workflow_generation=category_values.document_generation
    order by request.request_generation desc,request.updated_at_utc desc,request.id desc
    limit 1
  ) approval on true
  left join lateral (
    select row.* from public.timesheets_financials row
    where row.timesheet_id=category_values.target_timesheet_id and row.is_current
    order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc limit 1
  ) financial on true
  left join public.timesheets timesheet
    on timesheet.timesheet_id=category_values.target_timesheet_id and timesheet.is_current
)
insert into public.candidate_expense_components(
  workflow_id,workflow_generation,expense_category,owning_timesheet_id,amount,mileage_units,
  lifecycle_state,manager_approval_state,agency_authorisation_state,approval_request_id,
  submitted_at_utc,manager_approved_at_utc,refusal_kind,refusal_reason,refused_at_utc,
  removed_at_utc,created_at_utc,updated_at_utc
)
select id,document_generation,expense_category,target_timesheet_id,amount,mileage_units,
  case
    when state='REFUSED' then 'MANAGER_REFUSED'
    when state='REJECTED' then 'OFFICE_REJECTED'
    when state='CANCELLED' then 'CANCELLED'
    when state in ('EXPIRED','SUPERSEDED') then 'SUPERSEDED'
    when state in ('MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE','FINALISED','RECEIVED')
      then 'MANAGER_APPROVED'
    when state in ('CREATED','WORKER_DRAFT') then 'DRAFT'
    else 'SUBMITTED'
  end,
  case
    when state='REFUSED' then 'REFUSED'
    when approved_at_utc is not null or state in ('MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE','FINALISED','RECEIVED') then 'APPROVED'
    when approval_state='PENDING' then 'PENDING'
    else 'NOT_REQUESTED'
  end,
  case
    when paid_at_utc is not null or upper(coalesce(timesheet_status,''))='PAID' then 'PAID'
    when locked_by_invoice_id is not null or upper(coalesce(timesheet_status,''))='INVOICED' then 'INVOICED'
    when authorised_at_utc is not null or authorised_at_server is not null
      or upper(coalesce(timesheet_status,'')) in ('AUTHORISED','AUTHORIZED') then 'AUTHORISED'
    else 'NOT_AUTHORISED'
  end,
  approval_request_id,worker_submitted_at_utc,approved_at_utc,
  case when state='REFUSED' then 'MANAGER_REFUSAL'
    when state='REJECTED' then 'AGENCY_REJECTION' else null end,
  case when state in ('REFUSED','REJECTED') then coalesce(refusal_reason,rejection_reason,'No reason recorded') else null end,
  case when state in ('REFUSED','REJECTED') then coalesce(refused_at_utc,updated_at_utc) else null end,
  case when state in ('REJECTED','CANCELLED','EXPIRED','SUPERSEDED') then updated_at_utc else null end,
  created_at_utc,updated_at_utc
from backfill
where amount>0 or mileage_units>0 or has_evidence;

insert into public.candidate_expense_component_events(
  expense_component_id,workflow_id,component_generation,event_type,actor_kind,
  before_state_json,after_state_json,idempotency_key,occurred_at_utc
)
select component.expense_component_id,component.workflow_id,component.component_generation,
  case component.lifecycle_state
    when 'MANAGER_APPROVED' then 'MANAGER_APPROVED'
    when 'MANAGER_REFUSED' then 'MANAGER_REFUSED'
    when 'OFFICE_REJECTED' then 'OFFICE_REJECTED'
    when 'WITHDRAWN' then 'WITHDRAWN'
    when 'CANCELLED' then 'CANCELLED'
    when 'SUPERSEDED' then 'SUPERSEDED'
    when 'SUBMITTED' then 'SUBMITTED'
    else 'CREATED'
  end,
  'SYSTEM','{}'::jsonb,to_jsonb(component),
  'advanced-expense-component-backfill:'||component.expense_component_id::text,
  component.updated_at_utc
from public.candidate_expense_components component;

do $verification$
begin
  if exists (
    select 1 from public.candidate_expense_components component
    left join public.candidate_submission_workflows workflow on workflow.id=component.workflow_id
    where workflow.id is null or workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
  ) then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_BACKFILL_INVALID' using errcode='23514';
  end if;
end
$verification$;

commit;
