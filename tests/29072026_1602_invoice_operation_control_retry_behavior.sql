\set ON_ERROR_STOP on

begin;

create role anon;
create role authenticated;
create role service_role;

create schema auth;
create schema extensions;
create schema private;
create extension pgcrypto with schema extensions;

create sequence public.invoice_operation_change_seq as bigint;

create table public.tms_users (
  id uuid primary key,
  is_active boolean not null,
  role text not null
);

create table public.invoice_operations (
  id uuid primary key default gen_random_uuid(),
  parent_operation_id uuid,
  operation_type text not null,
  entity_type text,
  entity_id uuid,
  actor_user_id uuid,
  idempotency_key text not null,
  status text not null,
  phase text not null,
  priority integer not null default 200,
  source_revision text,
  template_version text,
  input_json jsonb not null default '{}'::jsonb,
  config_json jsonb not null default '{}'::jsonb,
  progress_json jsonb not null default '{}'::jsonb,
  result_json jsonb,
  error_json jsonb,
  total_units integer not null default 0,
  completed_units integer not null default 0,
  failed_units integer not null default 0,
  chunk_count integer not null default 0,
  control_version bigint not null default 1,
  change_seq bigint not null default nextval('public.invoice_operation_change_seq'),
  requires_user_action boolean not null default false,
  created_at_utc timestamptz not null default now(),
  started_at_utc timestamptz,
  updated_at_utc timestamptz not null default now(),
  completed_at_utc timestamptz,
  failed_at_utc timestamptz,
  manifest_generation integer not null default 1,
  manifest_committed boolean not null default false,
  release_complete boolean not null default false,
  result_page_revision bigint not null default 1
);

create table public.invoice_document_assets (
  id uuid primary key default gen_random_uuid(),
  status text not null,
  source_revision text,
  normalised_manifest_json jsonb not null default '[]'::jsonb,
  normalised_manifest_hash text,
  normalised_r2_key text,
  normalised_sha256 text,
  normalised_size_bytes bigint,
  normalised_page_count integer,
  ready_at_utc timestamptz,
  error_json jsonb,
  updated_at_utc timestamptz not null default now()
);

create table public.invoice_document_versions (
  id uuid primary key default gen_random_uuid(),
  operation_id uuid not null,
  entity_type text,
  entity_id uuid,
  purpose text,
  source_revision text,
  template_version text,
  status text not null,
  r2_key text,
  sha256 text,
  size_bytes bigint,
  page_count integer,
  ready_at_utc timestamptz,
  verified_at_utc timestamptz,
  superseded_at_utc timestamptz,
  error_json jsonb
);

create table public.invoice_operation_chunks (
  id uuid primary key default gen_random_uuid(),
  operation_id uuid not null,
  chunk_type text not null,
  phase text not null,
  sequence_no integer not null,
  level_no integer not null default 0,
  work_key text not null,
  plan_generation integer not null default 1,
  replaced_by_chunk_id uuid,
  replacement_required boolean not null default false,
  entity_type text,
  entity_id uuid,
  document_version_id uuid,
  document_asset_id uuid,
  input_document_version_id uuid,
  status text not null,
  priority integer not null default 200,
  run_after_utc timestamptz not null default now(),
  payload_json jsonb not null default '{}'::jsonb,
  progress_json jsonb not null default '{}'::jsonb,
  result_json jsonb,
  error_json jsonb,
  expected_page_count integer,
  actual_page_count integer,
  expected_byte_count bigint,
  actual_byte_count bigint,
  attempt_count integer not null default 0,
  max_attempts integer not null default 5,
  lease_owner text,
  lease_token uuid,
  lease_expires_at_utc timestamptz,
  fence_token bigint not null default 0,
  operation_control_version bigint not null default 1,
  created_at_utc timestamptz not null default now(),
  started_at_utc timestamptz,
  updated_at_utc timestamptz not null default now(),
  completed_at_utc timestamptz,
  failed_at_utc timestamptz,
  manifest_generation integer not null default 1,
  is_manifest_member boolean not null default false,
  manifest_committed boolean not null default false,
  result_visible boolean not null default true,
  selection_key text,
  result_category text,
  constraint invoice_operation_chunks_unique_slot
    unique(operation_id,chunk_type,level_no,sequence_no,work_key)
);

create table public.invoices (
  id uuid primary key,
  status text,
  active_document_operation_id uuid,
  active_issue_operation_id uuid,
  document_state text,
  issue_state text,
  updated_at timestamptz
);

create table public.timesheets (
  timesheet_id uuid primary key,
  active_document_operation_id uuid,
  document_state text,
  updated_at timestamptz
);

create function auth.uid()
returns uuid
language sql
stable
as $$
  select nullif(current_setting('test.actor_user_id', true), '')::uuid;
$$;

create function auth.jwt()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'role',
    coalesce(
      nullif(current_setting('request.jwt.claim.role', true), ''),
      'service_role'
    )
  );
$$;

\ir ../supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_01_private_invoice_batch_canonical_text_v2.sql
\ir ../supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_02_private_invoice_batch_hash_v2.sql

create function private._invoice_candidate_snapshot_get_v2(
  p_action text,
  p_now_utc timestamptz
) returns jsonb
language sql
stable
as $$
  select jsonb_build_object('revision', 'behavior-fixture');
$$;

create function private._invoice_processor_limits()
returns jsonb
language sql
stable
as $$
  select '{}'::jsonb;
$$;

create function private._invoice_operation_rollup_batch(
  p_operation_ids uuid[],
  p_now_utc timestamptz,
  p_propagate_ancestors boolean
) returns table(
  operation_id uuid,
  status text,
  phase text,
  total_units integer,
  completed_units integer,
  failed_units integer,
  blocked_required_count integer,
  requires_user_action boolean,
  change_seq bigint
)
language sql
stable
as $$
  select
    operation.id,
    operation.status,
    operation.phase,
    operation.total_units,
    operation.completed_units,
    operation.failed_units,
    0,
    operation.requires_user_action,
    operation.change_seq
  from public.invoice_operations operation
  where operation.id = any(coalesce(p_operation_ids, array[]::uuid[]))
  order by operation.id;
$$;

\ir ../supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_control_batch.sql

create function pg_temp.assert_true(
  p_condition boolean,
  p_message text
) returns void
language plpgsql
as $$
begin
  if coalesce(p_condition, false) is not true then
    raise exception 'ASSERTION_FAILED: %', p_message;
  end if;
end;
$$;

create function pg_temp.control_envelope(
  p_actor_user_id uuid,
  p_request_token text,
  p_actions jsonb
) returns jsonb
language sql
as $$
  select jsonb_build_object(
    'contract_version', 'INVOICE_OPERATION_CONTROL_V2',
    'request_token', p_request_token,
    'request_hash', private._invoice_batch_hash_v2(jsonb_build_object(
      'contract_version', 'INVOICE_OPERATION_CONTROL_V2',
      'request_token', p_request_token,
      'actor_user_id', p_actor_user_id,
      'actions', p_actions
    )),
    'actions', p_actions
  );
$$;

set local request.jwt.claim.role = 'service_role';
set local test.actor_user_id = '00000000-0000-4000-8000-000000000001';

insert into public.tms_users(id, is_active, role)
values ('00000000-0000-4000-8000-000000000001', true, 'admin');

do $behavior$
declare
  v_actor constant uuid := '00000000-0000-4000-8000-000000000001';
  v_result jsonb;
  v_actions jsonb;
  v_rejected_result jsonb;
  v_control_version bigint;
  v_target_status text;
  v_target_attempt integer;
begin
  -- Accepted: WAITING + user action + current failed leaf.
  insert into public.invoice_operations(
    id, operation_type, entity_type, actor_user_id, idempotency_key,
    status, phase, total_units, failed_units, chunk_count,
    control_version, requires_user_action
  ) values (
    '00000000-0000-4000-8000-000000000101',
    'BUILD_DOCUMENT', 'INVOICE', v_actor, 'fixture-operation-101',
    'WAITING', 'WAIT_FOR_INPUTS', 2, 1, 2, 1, true
  );
  insert into public.invoice_operation_chunks(
    id, operation_id, chunk_type, phase, sequence_no, work_key,
    status, attempt_count, error_json
  ) values
  (
    '00000000-0000-4000-8000-000000000201',
    '00000000-0000-4000-8000-000000000101',
    'SOURCE_RENDER', 'FAILED', 1, 'fixture-target-201',
    'FAILED', 4, '{"code":"PROCESSOR_RESULT_IDENTITY_MISMATCH"}'
  ),
  (
    '00000000-0000-4000-8000-000000000202',
    '00000000-0000-4000-8000-000000000101',
    'DOCUMENT_INPUT', 'DEPENDENCY', 2, 'fixture-healthy-202',
    'WAITING', 0, null
  );

  v_actions := jsonb_build_array(jsonb_build_object(
    'operation_id', '00000000-0000-4000-8000-000000000101',
    'action', 'RETRY',
    'retry_chunk_id', '00000000-0000-4000-8000-000000000201'
  ));
  v_result := public.invoice_operation_control_batch(
    pg_temp.control_envelope(v_actor, 'accepted-target-token', v_actions),
    v_actor,
    '2026-07-29T16:00:00Z'
  );
  perform pg_temp.assert_true(
    v_result->0->>'accepted' = 'true',
    'qualified WAITING operation was not accepted'
  );
  select control_version into v_control_version
  from public.invoice_operations
  where id = '00000000-0000-4000-8000-000000000101';
  perform pg_temp.assert_true(
    v_control_version = 2,
    'control version did not increment exactly once'
  );
  select status, attempt_count into v_target_status, v_target_attempt
  from public.invoice_operation_chunks
  where id = '00000000-0000-4000-8000-000000000201';
  perform pg_temp.assert_true(
    v_target_status = 'QUEUED' and v_target_attempt = 0,
    'targeted current failed leaf was not reset'
  );
  perform pg_temp.assert_true(
    (select status = 'WAITING' and attempt_count = 0
     from public.invoice_operation_chunks
     where id = '00000000-0000-4000-8000-000000000202'),
    'targeted retry changed an unrelated current leaf'
  );

  -- Same-token replay returns the receipt and cannot repeat the mutation.
  v_result := public.invoice_operation_control_batch(
    pg_temp.control_envelope(v_actor, 'accepted-target-token', v_actions),
    v_actor,
    '2026-07-29T16:01:00Z'
  );
  perform pg_temp.assert_true(
    v_result->0->>'accepted' = 'true',
    'same-token replay did not return the durable accepted result'
  );
  perform pg_temp.assert_true(
    (select control_version = 2
     from public.invoice_operations
     where id = '00000000-0000-4000-8000-000000000101'),
    'same-token replay repeated the operation mutation'
  );

  -- Rejected: WAITING without user action.
  insert into public.invoice_operations(
    id, operation_type, actor_user_id, idempotency_key, status, phase,
    total_units, failed_units, chunk_count, control_version,
    requires_user_action
  ) values (
    '00000000-0000-4000-8000-000000000102',
    'BUILD_DOCUMENT', v_actor, 'fixture-operation-102', 'WAITING',
    'WAIT_FOR_INPUTS', 1, 1, 1, 1, false
  );
  insert into public.invoice_operation_chunks(
    id, operation_id, chunk_type, phase, sequence_no, work_key,
    status, attempt_count
  ) values (
    '00000000-0000-4000-8000-000000000203',
    '00000000-0000-4000-8000-000000000102',
    'SOURCE_RENDER', 'FAILED', 1, 'fixture-target-203', 'FAILED', 2
  );
  v_actions := jsonb_build_array(jsonb_build_object(
    'operation_id', '00000000-0000-4000-8000-000000000102',
    'action', 'RETRY',
    'retry_chunk_id', '00000000-0000-4000-8000-000000000203'
  ));
  v_result := public.invoice_operation_control_batch(
    pg_temp.control_envelope(v_actor, 'no-user-action-token', v_actions),
    v_actor,
    '2026-07-29T16:02:00Z'
  );
  perform pg_temp.assert_true(
    v_result->0->>'accepted' = 'false'
      and v_result->0->'error'->>'code' = 'OPERATION_NOT_RETRYABLE',
    'WAITING operation without user action was not rejected'
  );

  -- Rejected: WAITING with user action but no retryable leaf.
  insert into public.invoice_operations(
    id, operation_type, actor_user_id, idempotency_key, status, phase,
    total_units, chunk_count, control_version, requires_user_action
  ) values (
    '00000000-0000-4000-8000-000000000103',
    'BUILD_DOCUMENT', v_actor, 'fixture-operation-103', 'WAITING',
    'WAIT_FOR_INPUTS', 1, 1, 1, true
  );
  insert into public.invoice_operation_chunks(
    id, operation_id, chunk_type, phase, sequence_no, work_key, status
  ) values (
    '00000000-0000-4000-8000-000000000204',
    '00000000-0000-4000-8000-000000000103',
    'DOCUMENT_INPUT', 'DEPENDENCY', 1, 'fixture-waiting-204', 'WAITING'
  );
  v_actions := jsonb_build_array(jsonb_build_object(
    'operation_id', '00000000-0000-4000-8000-000000000103',
    'action', 'RETRY'
  ));
  v_result := public.invoice_operation_control_batch(
    pg_temp.control_envelope(v_actor, 'no-retryable-leaf-token', v_actions),
    v_actor,
    '2026-07-29T16:03:00Z'
  );
  perform pg_temp.assert_true(
    v_result->0->>'accepted' = 'false'
      and v_result->0->'error'->>'code' = 'OPERATION_NOT_RETRYABLE',
    'WAITING operation without a retryable leaf was not rejected'
  );

  -- Rejected: requested chunk is replaced and therefore is not current.
  insert into public.invoice_operations(
    id, operation_type, actor_user_id, idempotency_key, status, phase,
    total_units, failed_units, chunk_count, control_version,
    requires_user_action
  ) values (
    '00000000-0000-4000-8000-000000000104',
    'BUILD_DOCUMENT', v_actor, 'fixture-operation-104', 'WAITING',
    'WAIT_FOR_INPUTS', 2, 2, 2, 1, true
  );
  insert into public.invoice_operation_chunks(
    id, operation_id, chunk_type, phase, sequence_no, work_key,
    status, attempt_count
  ) values (
    '00000000-0000-4000-8000-000000000206',
    '00000000-0000-4000-8000-000000000104',
    'SOURCE_RENDER', 'FAILED', 2, 'fixture-current-206', 'FAILED', 1
  );
  insert into public.invoice_operation_chunks(
    id, operation_id, chunk_type, phase, sequence_no, work_key,
    status, attempt_count, replaced_by_chunk_id, replacement_required
  ) values (
    '00000000-0000-4000-8000-000000000205',
    '00000000-0000-4000-8000-000000000104',
    'SOURCE_RENDER', 'SUPERSEDED', 1, 'fixture-replaced-205',
    'FAILED', 1, '00000000-0000-4000-8000-000000000206', true
  );
  v_actions := jsonb_build_array(jsonb_build_object(
    'operation_id', '00000000-0000-4000-8000-000000000104',
    'action', 'RETRY',
    'retry_chunk_id', '00000000-0000-4000-8000-000000000205'
  ));
  v_result := public.invoice_operation_control_batch(
    pg_temp.control_envelope(v_actor, 'replaced-target-token', v_actions),
    v_actor,
    '2026-07-29T16:04:00Z'
  );
  perform pg_temp.assert_true(
    v_result->0->>'accepted' = 'false',
    'replaced non-current chunk was accepted'
  );
  perform pg_temp.assert_true(
    (select control_version = 1
     from public.invoice_operations
     where id = '00000000-0000-4000-8000-000000000104'),
    'replaced-target rejection mutated the operation'
  );

  -- A healthy unrelated WAITING operation stays unchanged.
  insert into public.invoice_operations(
    id, operation_type, actor_user_id, idempotency_key, status, phase,
    total_units, chunk_count, control_version, requires_user_action
  ) values (
    '00000000-0000-4000-8000-000000000105',
    'BUILD_DOCUMENT', v_actor, 'fixture-operation-105', 'WAITING',
    'WAIT_FOR_INPUTS', 1, 1, 7, false
  );
  insert into public.invoice_operation_chunks(
    id, operation_id, chunk_type, phase, sequence_no, work_key, status
  ) values (
    '00000000-0000-4000-8000-000000000207',
    '00000000-0000-4000-8000-000000000105',
    'DOCUMENT_INPUT', 'DEPENDENCY', 1, 'fixture-healthy-207', 'WAITING'
  );
  perform pg_temp.assert_true(
    (select status = 'WAITING' and control_version = 7
     from public.invoice_operations
     where id = '00000000-0000-4000-8000-000000000105'),
    'unrelated healthy WAITING operation changed'
  );

  -- A rejected receipt remains immutable; a fresh token can later retry.
  insert into public.invoice_operations(
    id, operation_type, actor_user_id, idempotency_key, status, phase,
    total_units, failed_units, chunk_count, control_version,
    requires_user_action
  ) values (
    '00000000-0000-4000-8000-000000000106',
    'BUILD_DOCUMENT', v_actor, 'fixture-operation-106', 'WAITING',
    'WAIT_FOR_INPUTS', 1, 1, 1, 1, false
  );
  insert into public.invoice_operation_chunks(
    id, operation_id, chunk_type, phase, sequence_no, work_key,
    status, attempt_count
  ) values (
    '00000000-0000-4000-8000-000000000208',
    '00000000-0000-4000-8000-000000000106',
    'SOURCE_RENDER', 'FAILED', 1, 'fixture-target-208', 'FAILED', 3
  );
  v_actions := jsonb_build_array(jsonb_build_object(
    'operation_id', '00000000-0000-4000-8000-000000000106',
    'action', 'RETRY',
    'retry_chunk_id', '00000000-0000-4000-8000-000000000208'
  ));
  v_rejected_result := public.invoice_operation_control_batch(
    pg_temp.control_envelope(v_actor, 'previous-rejected-token', v_actions),
    v_actor,
    '2026-07-29T16:05:00Z'
  );
  perform pg_temp.assert_true(
    v_rejected_result->0->>'accepted' = 'false',
    'fixture rejected receipt was not created'
  );
  update public.invoice_operations
  set requires_user_action = true
  where id = '00000000-0000-4000-8000-000000000106';

  v_result := public.invoice_operation_control_batch(
    pg_temp.control_envelope(v_actor, 'previous-rejected-token', v_actions),
    v_actor,
    '2026-07-29T16:06:00Z'
  );
  perform pg_temp.assert_true(
    v_result = v_rejected_result,
    'same rejected token did not replay its immutable rejection'
  );
  perform pg_temp.assert_true(
    (select control_version = 1
     from public.invoice_operations
     where id = '00000000-0000-4000-8000-000000000106'),
    'rejected-token replay mutated the operation'
  );

  v_result := public.invoice_operation_control_batch(
    pg_temp.control_envelope(v_actor, 'fresh-after-rejection-token', v_actions),
    v_actor,
    '2026-07-29T16:07:00Z'
  );
  perform pg_temp.assert_true(
    v_result->0->>'accepted' = 'true',
    'fresh token after a rejected receipt did not perform the corrected retry'
  );
  perform pg_temp.assert_true(
    (select control_version = 2
     from public.invoice_operations
     where id = '00000000-0000-4000-8000-000000000106'),
    'fresh-token retry did not increment control version once'
  );

  perform pg_temp.assert_true(
    (select status = 'WAITING' and control_version = 7
     from public.invoice_operations
     where id = '00000000-0000-4000-8000-000000000105'),
    'unrelated healthy WAITING operation changed during other retries'
  );
end;
$behavior$;

rollback;
