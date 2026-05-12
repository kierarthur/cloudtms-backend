begin;

select pg_advisory_xact_lock(hashtext('banking_pay_operation_config:draft_candidate_scope_defaults:v1'));

with expected as (
  select *
  from (
    values
      ('DRAFT_CREATE', 'INSERT_CANDIDATES',            'CANDIDATE_SCOPE', 100, 1, 500, 15000, 60),
      ('DRAFT_CREATE', 'INSERT_ITEMS',                 'CANDIDATE_SCOPE', 100, 1, 500, 15000, 60),
      ('DRAFT_CREATE', 'APPLY_FINANCE_ADJUSTMENTS',    'CANDIDATE_SCOPE', 100, 1, 500, 15000, 60),
      ('DRAFT_CREATE', 'FINALISE_RESERVATIONS',        'CANDIDATE_SCOPE', 100, 1, 500, 15000, 60),
      ('DRAFT_CREATE', 'POPULATE_CANDIDATE_SUMMARIES', 'CANDIDATE_SCOPE', 100, 1, 500, 15000, 60),
      ('DRAFT_CREATE', 'CREATE_TIMESHEET_SNAPSHOTS',   'CANDIDATE_SCOPE', 100, 1, 500, 15000, 60),
      ('DRAFT_CREATE', 'BUILD_ITEM_BREAKDOWNS',        'CANDIDATE_SCOPE', 100, 1, 500, 15000, 60)
  ) as v(
    operation_type,
    phase,
    chunk_type,
    default_chunk_size,
    min_chunk_size,
    max_chunk_size,
    max_advance_ms,
    lock_seconds
  )
),
updated as (
  update public.banking_pay_operation_config c
  set
    default_chunk_size = e.default_chunk_size,
    min_chunk_size = e.min_chunk_size,
    max_chunk_size = e.max_chunk_size,
    max_advance_ms = e.max_advance_ms,
    lock_seconds = e.lock_seconds,
    enabled = true,
    updated_at_utc = now()
  from expected e
  where c.operation_type = e.operation_type
    and c.phase = e.phase
    and c.chunk_type = e.chunk_type
  returning c.operation_type, c.phase, c.chunk_type
)
insert into public.banking_pay_operation_config (
  id,
  operation_type,
  phase,
  chunk_type,
  default_chunk_size,
  min_chunk_size,
  max_chunk_size,
  max_advance_ms,
  lock_seconds,
  enabled,
  updated_at_utc,
  updated_by
)
select
  gen_random_uuid(),
  e.operation_type,
  e.phase,
  e.chunk_type,
  e.default_chunk_size,
  e.min_chunk_size,
  e.max_chunk_size,
  e.max_advance_ms,
  e.lock_seconds,
  true,
  now(),
  null
from expected e
where not exists (
  select 1
  from public.banking_pay_operation_config c
  where c.operation_type = e.operation_type
    and c.phase = e.phase
    and c.chunk_type = e.chunk_type
);

commit;
