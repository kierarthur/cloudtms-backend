-- Immutable evidence at the actual approved one-WORK preparation pin.
-- No money, Source inventory, current-state backfill, reader or activation.
-- Accepted QueryV3 6FCCC7CD... keeps the helper/table/fixed mutex names V2.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_run_work_query_pin_v2 (
  run_work_id uuid not null,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  work_id uuid not null,
  captured_revision_id uuid not null,
  prepare_job_id uuid not null references private.bpay_next_job(id) on delete restrict,
  module_epoch bigint not null check (module_epoch>0),
  gate_version text not null check (gate_version='PAY_QUERY_STATE_V3'),
  disposition text not null check (disposition in ('CLEAR','BLOCKED','UNAVAILABLE')),
  scope jsonb,
  query_state_sha256 bytea,
  captured_at_utc timestamptz not null default pg_catalog.clock_timestamp()
    check (pg_catalog.isfinite(captured_at_utc)),
  primary key (run_work_id),
  foreign key (run_work_id,run_worker_id,work_id,captured_revision_id)
    references private.bpay_next_run_work(id,run_worker_id,work_id,captured_revision_id) on delete restrict,
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  constraint bpay_next_query_pin_disposition_ck check (
    (disposition in ('CLEAR','BLOCKED') and scope is not null
      and query_state_sha256 is not null and pg_catalog.octet_length(query_state_sha256)=32)
    or (disposition='UNAVAILABLE' and query_state_sha256 is null)
  ),
  -- All nine ScopeV2 identities are bounded by their actual parent types:
  -- UUID text, booking<=200 chars, finite YYYY-MM-DD and positive int64 text.
  -- Missing keys/JSON null/extra keys cannot pass via CHECK's NULL semantics.
  constraint bpay_next_query_pin_scope_ck check (scope is null or (
    pg_catalog.jsonb_typeof(scope)='object'
    and scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
      'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
    and scope - array['root_timesheet_id','family_booking_id','target_family_id',
      'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
    and pg_catalog.jsonb_typeof(scope->'root_timesheet_id')='string'
    and scope->>'root_timesheet_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and pg_catalog.jsonb_typeof(scope->'family_booking_id')='string'
    and pg_catalog.char_length(scope->>'family_booking_id') between 1 and 200
    and pg_catalog.btrim(scope->>'family_booking_id')<>''
    and pg_catalog.jsonb_typeof(scope->'candidate_id')='string'
    and scope->>'candidate_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and pg_catalog.jsonb_typeof(scope->'client_id')='string'
    and scope->>'client_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and pg_catalog.jsonb_typeof(scope->'contract_id')='string'
    and scope->>'contract_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and pg_catalog.jsonb_typeof(scope->'week_ending_date')='string'
    and scope->>'week_ending_date' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    and pg_catalog.jsonb_typeof(scope->'root_version')='string'
    and scope->>'root_version' ~ '^[1-9][0-9]{0,18}$'
    and (scope->>'root_version')::numeric<=9223372036854775807
    and ((scope->'target_family_id'='null'::jsonb and scope->'family_bound_version'='null'::jsonb)
      or (pg_catalog.jsonb_typeof(scope->'target_family_id')='string'
        and scope->>'target_family_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        and pg_catalog.jsonb_typeof(scope->'family_bound_version')='string'
        and scope->>'family_bound_version' ~ '^[1-9][0-9]{0,18}$'
        and (scope->>'family_bound_version')::numeric<=9223372036854775807))
  ) is true)
);

-- PK already covers the captured run_work composite FK; these two indexes
-- cover the remaining actual parent FKs without an operation/history scan.
create index bpay_next_query_pin_worker_idx
  on private.bpay_next_run_work_query_pin_v2(run_worker_id,candidate_id,run_work_id);
create index bpay_next_query_pin_prepare_job_idx
  on private.bpay_next_run_work_query_pin_v2(prepare_job_id,run_work_id);

alter table private.bpay_next_run_work_query_pin_v2 owner to postgres;
alter table private.bpay_next_run_work_query_pin_v2 enable row level security;
revoke all on private.bpay_next_run_work_query_pin_v2
  from public,anon,authenticated,service_role;

comment on table private.bpay_next_run_work_query_pin_v2 is
  'One immutable factual query disposition captured atomically with actual run_work and cursor. Missing row is unavailable, never live reconstruction. No automatic Draft invalidation or financial authority.';

commit;
