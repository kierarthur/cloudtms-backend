-- Rollback-only proof for the durable final-source -> ordinary Timesheet/TSFIN
-- orchestration checkpoint.  Final source is created by the existing verifier;
-- this file proves checkpoint/recovery and receipt-led completion without
-- creating an alternate pay, Workbench, Draft or invoice route.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';
delete from public.settings_defaults where id=1;
\set weekly_source_verification_outer_transaction true
\ir 15092026_1534_weekly_source_finalisation_v1.sql

create temp table finalisation_pay_boundary_before as
select
  (select pg_catalog.count(*) from public.invoice_lines) as invoice_line_count,
  (select pg_catalog.count(*) from public.pay_batches) as pay_batch_count,
  (select pg_catalog.count(*) from public.pay_batch_items) as pay_batch_item_count;

create temp table finalisation_pay_subject as
select revision.id as final_revision_id,
       revision.source_cycle_id,
       movement.invoice_timesheet_id as root_timesheet_id
from public.weekly_source_final_revisions revision
join public.weekly_source_billing_movements movement
  on movement.final_revision_id=revision.id
where revision.state='CURRENT'
  and revision.reason='INITIAL_FINALISATION'
  and movement.invoice_timesheet_id is not null
  and not exists(
    select 1
    from public.weekly_source_ordinary_pay_projection_receipts receipt
    where receipt.final_revision_id=revision.id
      and receipt.root_timesheet_id=movement.invoice_timesheet_id
  )
  and exists(
    select 1
    from public.weekly_source_state_transitions transition_row
    join public.weekly_source_billing_movements transition_movement
      on transition_movement.transition_id=transition_row.id
     and transition_movement.invoice_timesheet_id=movement.invoice_timesheet_id
    where transition_row.final_revision_id=revision.id
      and transition_row.ordinary_source_entitlement_projection_state='PENDING'
  )
order by revision.finalised_at_utc,revision.id,movement.invoice_timesheet_id
limit 1;

select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from finalisation_pay_subject),
  'finalisation verifier did not provide one unprojected source root'
);

create temp table finalisation_pay_open as
select public.weekly_source_finalisation_pay_open_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_OPEN_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'final_revision_id',subject.final_revision_id
  )
) as result
from finalisation_pay_subject subject;

select pg_temp.assert_true(
  (select result->>'state'='READY'
          and (result->>'task_count')::integer>0
          and (result->>'terminal_task_count')::integer=0
          and not (result->>'idempotent_replay')::boolean
   from finalisation_pay_open),
  'final source did not create one durable READY pay-projection manifest'
);

create temp table finalisation_pay_task as
select (opened.result->>'run_id')::uuid as run_id,
       task.value->>'task_id' as task_id,
       (task.value->>'version')::bigint as task_version,
       task.value->>'prepared_context_hash' as context_hash,
       task.value->>'projection_idempotency_key' as projection_idempotency_key,
       (task.value->>'root_timesheet_id')::uuid as root_timesheet_id
from finalisation_pay_open opened
cross join lateral pg_catalog.jsonb_array_elements(opened.result->'tasks') task(value)
join finalisation_pay_subject subject
  on (task.value->>'root_timesheet_id')::uuid=subject.root_timesheet_id;

create temp table finalisation_pay_start_one as
select public.weekly_source_finalisation_pay_task_start_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'run_id',task.run_id,'task_id',task.task_id,
    'expected_task_version',task.task_version,
    'expected_context_hash',task.context_hash
  )
) as result
from finalisation_pay_task task;

select pg_temp.assert_true(
  (select result->>'status'='SUBMISSION_STARTED'
          and (result#>>'{task,attempt_count}')::integer=1
          and result#>>'{projection_request,idempotency_key}'=
              (select projection_idempotency_key from finalisation_pay_task)
   from finalisation_pay_start_one),
  'first projection submission was not durably checkpointed'
);

-- Replaying START cannot submit again: it returns recovery-required and keeps
-- the same attempt count until an explicit recovery decision is recorded.
select pg_temp.assert_true(
  (select replay->>'status'='RECOVERY_REQUIRED'
          and (replay#>>'{task,attempt_count}')::integer=1
   from finalisation_pay_task task
   cross join lateral public.weekly_source_finalisation_pay_task_start_atomic_v1(
     pg_catalog.jsonb_build_object(
       'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_V1',
       'actor_user_id','a0000000-0000-4000-8000-000000000001',
       'run_id',task.run_id,'task_id',task.task_id,
       'expected_task_version',(select (result#>>'{task,version}')::bigint
                                from finalisation_pay_start_one),
       'expected_context_hash',task.context_hash
     )
   ) replay),
  'a repeated START was permitted to become a hidden retry'
);

create temp table finalisation_pay_unknown as
select public.weekly_source_finalisation_pay_task_unknown_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'run_id',task.run_id,'task_id',task.task_id,
    'expected_task_version',(select (result#>>'{task,version}')::bigint
                             from finalisation_pay_start_one),
    'error_code','VERIFIER_RESPONSE_UNKNOWN'
  )
) as result
from finalisation_pay_task task;

select pg_temp.assert_true(
  (select result->>'state'='RECOVERY_REQUIRED'
          and result#>>'{task,state}'='RECOVERY_REQUIRED'
          and (result#>>'{task,attempt_count}')::integer=1
   from finalisation_pay_unknown),
  'unknown result was not durably stopped for recovery'
);

create temp table finalisation_pay_recover_no_retry as
select public.weekly_source_finalisation_pay_task_recover_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_RECOVER_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'run_id',task.run_id,'task_id',task.task_id,
    'expected_task_version',(select (result#>>'{task,version}')::bigint
                             from finalisation_pay_unknown),
    'confirm_retry',false
  )
) as result
from finalisation_pay_task task;

select pg_temp.assert_true(
  (select result->>'state'='RECOVERY_REQUIRED'
          and not (result->>'retry_permitted')::boolean
          and exists(
            select 1 from pg_catalog.jsonb_array_elements(result->'tasks') item(value)
            where item.value->>'task_id'=(select task_id from finalisation_pay_task)
              and (item.value->>'attempt_count')::integer=1
          )
   from finalisation_pay_recover_no_retry),
  'receipt-first recovery permitted an unconfirmed retry'
);

create temp table finalisation_pay_rearm as
select public.weekly_source_finalisation_pay_task_recover_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_RECOVER_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'run_id',task.run_id,'task_id',task.task_id,
    'expected_task_version',(
      select (task_item.value->>'version')::bigint
      from finalisation_pay_recover_no_retry recovered
      cross join lateral pg_catalog.jsonb_array_elements(recovered.result->'tasks') task_item(value)
      where task_item.value->>'task_id'=task.task_id
    ),
    'confirm_retry',true
  )
) as result
from finalisation_pay_task task;

select pg_temp.assert_true(
  (select result->>'state'='READY'
          and (result->>'retry_permitted')::boolean
          and result#>>'{task,state}'='READY'
   from finalisation_pay_rearm),
  'explicit recovery confirmation did not re-arm exactly one READY item'
);

create temp table finalisation_pay_start_two as
select public.weekly_source_finalisation_pay_task_start_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'run_id',task.run_id,'task_id',task.task_id,
    'expected_task_version',(select (result#>>'{task,version}')::bigint
                             from finalisation_pay_rearm),
    'expected_context_hash',task.context_hash
  )
) as result
from finalisation_pay_task task;

select pg_temp.assert_true(
  (select result->>'status'='SUBMISSION_STARTED'
          and (result#>>'{task,attempt_count}')::integer=2
          and result#>>'{projection_request,idempotency_key}'=
              (select projection_idempotency_key from finalisation_pay_task)
   from finalisation_pay_start_two),
  'explicit retry did not retain the original projection idempotency key'
);

-- The ordinary projection owner persists the receipt in production.  Insert a
-- correctly shaped refusal receipt here so FINISH can prove that it trusts only
-- independently persisted evidence and never a Worker-supplied outcome.
-- Gate 2 / S9: there is no REFUSED_LOCKED receipt any more.  The state this
-- section used to prove does not exist.  What must still be proved is that a
-- receipt-led finish checkpoints the TERMINAL state the projection really
-- returns for an already-authorised root -- PROPOSED -- and that the run
-- completes rather than stranding at ACTION_REQUIRED with nothing to consume it.
create temp table finalisation_pay_proposed_receipt as
with facts as (
  select subject.final_revision_id,subject.source_cycle_id,subject.root_timesheet_id,
         movement.actual_client_id as client_id,
         movement.source_profile_kind,
         case when movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
              then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end as source_mode,
         revision.manifest_hash,revision.policy_fingerprint,
         private.weekly_source_ordinary_projection_source_units_v1(
           subject.final_revision_id,subject.root_timesheet_id
         ) as source_units,
         private.weekly_source_ordinary_projection_current_expenses_v1(
           subject.root_timesheet_id,subject.final_revision_id
         ) as source_expenses,
         private.weekly_source_ordinary_projection_root_hash_v1(
           subject.root_timesheet_id
         ) as root_hash,
         task.projection_idempotency_key
  from finalisation_pay_subject subject
  join public.weekly_source_final_revisions revision
    on revision.id=subject.final_revision_id
  join public.weekly_source_billing_movements movement
    on movement.final_revision_id=subject.final_revision_id
   and movement.invoice_timesheet_id=subject.root_timesheet_id
  join finalisation_pay_task task on task.root_timesheet_id=subject.root_timesheet_id
  order by movement.id
  limit 1
)
select receipt.*
from facts
cross join lateral private.weekly_source_ordinary_projection_receipt_insert_v1(
  facts.final_revision_id,facts.source_cycle_id,facts.client_id,
  facts.root_timesheet_id,null::uuid,facts.source_profile_kind,facts.source_mode,
  'PROPOSED',facts.source_units,
  private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_UNIT_MANIFEST_V1',facts.source_units
  ),facts.source_expenses,
  private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_SOURCE_EXPENSE_MANIFEST_V1',facts.source_expenses
  ),facts.manifest_hash,facts.policy_fingerprint,
  private.weekly_source_sha256_jsonb_v1('VERIFIER_SERVICE_SNAPSHOT','{}'::jsonb),
  private.weekly_source_sha256_jsonb_v1('VERIFIER_SERVER_CALCULATION','{}'::jsonb),
  facts.root_hash,facts.root_hash,facts.projection_idempotency_key,
  private.weekly_source_sha256_jsonb_v1(
    'VERIFIER_FINALISATION_PAY_REQUEST',pg_catalog.to_jsonb(facts.projection_idempotency_key)
  ),'a0000000-0000-4000-8000-000000000001'
) as receipt;

create temp table finalisation_pay_finished as
select public.weekly_source_finalisation_pay_task_finish_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_TASK_FINISH_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'run_id',task.run_id,'task_id',task.task_id,
    'expected_task_version',(select (result#>>'{task,version}')::bigint
                             from finalisation_pay_start_two),
    'projection_receipt_id',receipt.id,
    'projection_receipt_hash',pg_catalog.encode(receipt.receipt_hash,'hex')
  )
) as result
from finalisation_pay_task task
cross join finalisation_pay_proposed_receipt receipt;

select pg_temp.assert_true(
  (select (result->>'action_required_task_count')::integer=0
          and exists(
            select 1 from pg_catalog.jsonb_array_elements(result->'tasks') item(value)
            where item.value->>'task_id'=(select task_id from finalisation_pay_task)
              and item.value->>'state'='PROPOSED'
              and coalesce((item.value->>'action_required')::boolean,false) is false
          )
   from finalisation_pay_finished)
  and exists(
    select 1
    from public.weekly_source_state_transitions transition_row
    join finalisation_pay_subject subject
      on subject.final_revision_id=transition_row.final_revision_id
    join public.weekly_source_billing_movements movement
      on movement.transition_id=transition_row.id
     and movement.invoice_timesheet_id=subject.root_timesheet_id
    where transition_row.ordinary_source_entitlement_projection_state='PUBLISHED'
  ),
  'a receipt-led PROPOSED finish must checkpoint a terminal, non-action-required task'
);

select pg_temp.assert_true(
  (select invoice_line_count=(select pg_catalog.count(*) from public.invoice_lines)
          and pay_batch_count=(select pg_catalog.count(*) from public.pay_batches)
          and pay_batch_item_count=(select pg_catalog.count(*) from public.pay_batch_items)
   from finalisation_pay_boundary_before),
  'finalisation-pay checkpoints changed invoice or Banking Pay-owned rows'
);

select pg_temp.assert_true(
  pg_catalog.has_function_privilege(
    'service_role','public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)','EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'service_role','public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb)','EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'service_role','public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb)','EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'service_role','public.weekly_source_finalisation_pay_task_unknown_atomic_v1(jsonb)','EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'service_role','public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb)','EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon','public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)','EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated','public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)','EXECUTE'
  ),
  'finalisation-pay checkpoint RPCs are not service-only'
);

rollback;
