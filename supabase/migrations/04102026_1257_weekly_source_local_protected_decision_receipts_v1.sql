-- Source-owned approval receipts, NOT a Banking Pay ledger or a C1 receipt.
-- The temporary write capability is sealed to this database transaction, one
-- unauthorised root, its previous financial row and the exact approved snapshot.

\set ON_ERROR_STOP on

begin;

create table private.weekly_source_local_protected_decision_receipts (
  publication_request_id uuid primary key references public.weekly_exceptional_c1_publication_requests(id),
  generation_id uuid not null unique references public.weekly_exceptional_pay_generations(id),
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id),
  root_timesheet_id uuid not null references public.timesheets(timesheet_id),
  actor_user_id uuid not null references public.tms_users(id),
  request_sha256 bytea not null check (octet_length(request_sha256)=32),
  idempotency_key text not null unique,
  preparing_transaction_id xid8 not null,
  prior_financial_id uuid references public.timesheets_financials(id),
  approved_snapshot_json jsonb not null check (jsonb_typeof(approved_snapshot_json)='object'),
  state text not null check (state in ('PREPARING','COMPLETE','PENDING_FREEZE')),
  result_json jsonb not null default '{}'::jsonb,
  created_at_utc timestamptz not null default transaction_timestamp(),
  completed_at_utc timestamptz,
  check ((state='PREPARING')=(completed_at_utc is null))
);
alter table private.weekly_source_local_protected_decision_receipts owner to postgres;
revoke all on private.weekly_source_local_protected_decision_receipts from public,anon,authenticated,service_role;
comment on table private.weekly_source_local_protected_decision_receipts is
  'CloudTMS protected decision audit/replay receipt. Before first Authorise the existing mutable TSFIN owner writes the sealed approved snapshot; after Authorise the existing common-head publisher owns entitlement. No residual, Draft, payment or invoice ledger. PREPARING is usable only in its own transaction.';

commit;
