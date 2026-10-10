-- Notification-only receipts. No timesheet/query/audit or provider data is removed.
create table private.candidate_notification_operations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  account_id uuid not null references public.candidate_app_accounts(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  idempotency_key text not null check (pg_catalog.length(idempotency_key) between 1 and 200),
  action text not null check (action in ('SNAPSHOT','MARK_READ','DELETE','UNDO')),
  request_json jsonb not null check (pg_catalog.jsonb_typeof(request_json)='object'),
  notification_ids uuid[] not null default '{}',
  result_json jsonb not null check (pg_catalog.jsonb_typeof(result_json)='object'),
  created_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  unique(account_id,candidate_id,idempotency_key)
);
create table private.candidate_notification_deleted_items (
  operation_id uuid not null references private.candidate_notification_operations(id) on delete restrict,
  notification_id uuid not null references public.candidate_notifications(id) on delete cascade,
  prior_state text not null check (prior_state in ('UNREAD','READ')),
  prior_read_at_utc timestamptz,
  primary key(operation_id,notification_id)
);
create table private.candidate_notification_dismissal_owners (
  notification_id uuid primary key references public.candidate_notifications(id) on delete cascade,
  operation_id uuid not null references private.candidate_notification_operations(id) on delete restrict,
  dismissed_at_utc timestamptz not null
);
alter table private.candidate_notification_operations owner to postgres;
alter table private.candidate_notification_deleted_items owner to postgres;
alter table private.candidate_notification_dismissal_owners owner to postgres;
revoke all on private.candidate_notification_operations, private.candidate_notification_deleted_items,
  private.candidate_notification_dismissal_owners from public,anon,authenticated,service_role;
alter table private.candidate_notification_operations enable row level security;
alter table private.candidate_notification_operations force row level security;
create policy candidate_notification_operations_owner on private.candidate_notification_operations to CURRENT_USER using (true) with check (true);
alter table private.candidate_notification_deleted_items enable row level security;
alter table private.candidate_notification_deleted_items force row level security;
create policy candidate_notification_deleted_items_owner on private.candidate_notification_deleted_items to CURRENT_USER using (true) with check (true);
alter table private.candidate_notification_dismissal_owners enable row level security;
alter table private.candidate_notification_dismissal_owners force row level security;
create policy candidate_notification_dismissal_owners_owner on private.candidate_notification_dismissal_owners to CURRENT_USER using (true) with check (true);
create index candidate_notifications_inbox_scope_idx on public.candidate_notifications
  (account_id,candidate_id,created_at_utc desc,id desc) where state in ('UNREAD','READ');
create index candidate_notification_deleted_items_notification_idx
  on private.candidate_notification_deleted_items(notification_id);
