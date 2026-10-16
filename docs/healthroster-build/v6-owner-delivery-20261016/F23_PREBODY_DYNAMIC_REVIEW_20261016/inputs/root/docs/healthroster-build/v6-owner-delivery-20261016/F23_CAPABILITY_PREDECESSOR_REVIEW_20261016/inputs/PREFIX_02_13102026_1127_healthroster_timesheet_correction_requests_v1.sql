-- One-time CloudTMS schema/data migration: healthroster_timesheet_correction_requests_v1
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- SEND_5 approves this exact request store because cycle/cohort-bound submission
-- requests and workflow-bound approvals cannot represent a pre-workflow P0.
-- It contains no parallel financial, query, signature or schedule authority.
create table private.timesheet_correction_requests (
  id uuid primary key default gen_random_uuid(),
  correction_id text not null unique,
  origin text not null check(origin in('AGENCY_INVOICE_CORRECTION','SIGNED_TIMESHEET_CORRECTION')),
  state text not null default 'WAITING' check(state in('WAITING','APPLIED','CANCELLED')),
  state_revision bigint not null default 1 check(state_revision>=1),
  proposal_revision bigint not null default 1 check(proposal_revision=1),
  root_timesheet_id uuid not null references public.timesheets(timesheet_id),
  original_booking_id text not null,
  reversed_timesheet_id uuid not null references public.timesheets(timesheet_id),
  reversed_version integer not null check(reversed_version>=1),
  reversed_financial_id uuid,
  candidate_id uuid not null references public.candidates(id),
  client_id uuid not null references public.clients(id),
  contract_id uuid references public.contracts(id),
  work_date date not null,
  week_ending_date date not null,
  replacement_timesheet_id uuid not null unique,
  replacement_booking_id text not null unique,
  replacement_week_id uuid,
  reversal_timesheet_id uuid unique,
  reversal_booking_id text unique,
  reversal_week_id uuid,
  request_key text not null unique check(length(request_key) between 1 and 200),
  request_fingerprint text not null check(request_fingerprint~'^[0-9a-f]{64}$'),
  target_revision text not null check(target_revision~'^[0-9a-f]{64}$'),
  reason text not null check(length(btrim(reason)) between 1 and 2000),
  frozen_original_json jsonb not null check(jsonb_typeof(frozen_original_json)='object'),
  proposal_json jsonb not null check(jsonb_typeof(proposal_json)='object'),
  original_invoice_id uuid references public.invoices(id),
  return_invoice_id uuid references public.invoices(id),
  workflow_id uuid,
  workflow_generation integer,
  terminal_receipt_json jsonb,
  created_by_user_id uuid not null references public.tms_users(id),
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  applied_at_utc timestamptz,
  cancelled_at_utc timestamptz,
  check(replacement_booking_id<>original_booking_id),
  check(replacement_timesheet_id<>reversed_timesheet_id and replacement_timesheet_id<>root_timesheet_id),
  check(reversal_timesheet_id is null or reversal_timesheet_id<>replacement_timesheet_id),
  check((state='WAITING' and terminal_receipt_json is null and applied_at_utc is null and cancelled_at_utc is null)
    or (state='APPLIED' and jsonb_typeof(terminal_receipt_json)='object' and applied_at_utc is not null
      and cancelled_at_utc is null and reversal_timesheet_id is not null and reversal_booking_id is not null)
    or (state='CANCELLED' and jsonb_typeof(terminal_receipt_json)='object' and cancelled_at_utc is not null
      and applied_at_utc is null and reversal_timesheet_id is null)),
  check((workflow_id is null and workflow_generation is null)
    or (workflow_id is not null and workflow_generation>=1))
);
create unique index timesheet_correction_request_waiting_root_v1
  on private.timesheet_correction_requests(root_timesheet_id) where state='WAITING';
create index timesheet_correction_request_reversed_v1
  on private.timesheet_correction_requests(reversed_timesheet_id,state);
alter table private.timesheet_correction_requests owner to postgres;
alter table private.timesheet_correction_requests enable row level security;
alter table private.timesheet_correction_requests force row level security;
create policy cloudtms_miget_service_owner_all on private.timesheet_correction_requests
  for all to current_user,service_role using(true) with check(true);
revoke all on private.timesheet_correction_requests from public,anon,authenticated,service_role;
grant all on private.timesheet_correction_requests to postgres;
comment on table private.timesheet_correction_requests is
  'One immutable original/proposal and exact physical P0 request; service RPCs delegate existing lifecycle/signature/document authorities.';

commit;
