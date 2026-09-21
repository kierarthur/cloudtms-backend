-- Rollback-only PostgreSQL 17 proof for protected-hours action orchestration.
-- It exercises exact request replay, stale-write refusal, WAIT's no-economic-
-- change lifecycle and the service-only RPC boundary.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is distinct from true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
end;
$function$;

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')
) on conflict (id) do nothing;
insert into public.tms_users(
  id,email,role,is_active,password_hash,payment_authoriser
) values (
  'f1000000-0000-4000-8000-000000000001','protected-action@example.test',
  'admin',true,'not-a-login',true
);
insert into public.clients(id,name)
values ('f1000000-0000-4000-8000-000000000002','Protected Action Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('f1000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name)
values ('f1000000-0000-4000-8000-000000000003','Protected Action Candidate');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'f1000000-0000-4000-8000-000000000004',
  'f1000000-0000-4000-8000-000000000003',
  'f1000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE',
  '{"pay":{"day":10,"night":10,"sat":10,"sun":10,"bh":10},"charge":{"day":20,"night":20,"sat":20,"sun":20,"bh":20}}'::jsonb,
  'HEALTHROSTER',true,true,true,true
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time
) values (
  'f1000000-0000-4000-8000-000000000005','TEST',
  'f1000000-0000-4000-8000-000000000006','PROTECTED_ACTION',
  'Protected Action','ROSTER',3,'15:00'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  'f1000000-0000-4000-8000-000000000007',
  'f1000000-0000-4000-8000-000000000005',
  'f1000000-0000-4000-8000-000000000002','2026-01-01',
  'f1000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,self_bill_correction_presentation,
  source_fixed_expenses_enabled,source_expense_vat_enabled,
  weekly_rate_classification_method,created_by_user_id
) values (
  'f1000000-0000-4000-8000-000000000008',
  'f1000000-0000-4000-8000-000000000005',
  'f1000000-0000-4000-8000-000000000002','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'FULL_REVERSAL_REPLACEMENT',
  false,false,'SPLIT_RATE_WINDOWS',
  'f1000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,
  projection_state
) values (
  'f1000000-0000-4000-8000-000000000009',
  'f1000000-0000-4000-8000-000000000005','2026-09-20',
  '2026-09-16T14:00:00Z','OPEN',1,'NONE'
);

create temp table prepared_family as
select public.weekly_exceptional_pay_prepare_family_v1(
  pg_catalog.jsonb_build_object(
    'actor_user_id','f1000000-0000-4000-8000-000000000001',
    'source_cycle_id','f1000000-0000-4000-8000-000000000009',
    'candidate_id','f1000000-0000-4000-8000-000000000003',
    'client_id','f1000000-0000-4000-8000-000000000002',
    'contract_id','f1000000-0000-4000-8000-000000000004',
    'week_ending_date','2026-09-13','work_date','2026-09-07',
    'start_at_local','2026-09-07 09:00:00',
    'end_at_local','2026-09-07 17:00:00','break_minutes',30,
    'evidence_timesheet_id',null,
    'reason','Candidate reports this shift was worked.',
    'idempotency_key','protected-action-family-0001'
  )
) as result;

select pg_temp.assert_true(
  (select result->>'request_kind'='APPROVE'
          and result->>'run_state'='RUNNING' from prepared_family)
  and (select request_fingerprint is not null
       from public.weekly_exceptional_orchestration_runs
       where id=(select (result->>'orchestration_run_id')::uuid from prepared_family)),
  'initial approval preparation must seal its exact request fingerprint'
);

insert into public.weekly_exceptional_payment_approvals(
  id,pay_target_family_id,work_event_id,candidate_id,client_id,contract_id,
  week_ending,protected_work_date,protected_start_at_local,
  protected_end_at_local,protected_break_minutes,
  contributing_issue_episode_ids,contributing_issue_episode_ids_hash,
  signed_schedule_fact_hash,contract_rate_policy_source_fingerprint,
  approved_by_user_id,approval_reason,source_cycle_id,
  approved_target_pay_components_json,approved_target_gross,
  creation_orchestration_run_id,approval_hash,creation_idempotency_key
) select
  'f1000000-0000-4000-8000-00000000000a',
  (result->>'family_id')::uuid,(result->>'work_event_id')::uuid,
  'f1000000-0000-4000-8000-000000000003',
  'f1000000-0000-4000-8000-000000000002',
  'f1000000-0000-4000-8000-000000000004','2026-09-13','2026-09-07',
  '2026-09-07 09:00:00','2026-09-07 17:00:00',30,'{}'::uuid[],
  pg_catalog.decode(pg_catalog.repeat('11',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('12',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('13',32),'hex'),
  'f1000000-0000-4000-8000-000000000001','Initial protected hours.',
  'f1000000-0000-4000-8000-000000000009','{}'::jsonb,75,
  (result->>'orchestration_run_id')::uuid,
  pg_catalog.decode(pg_catalog.repeat('14',32),'hex'),'protected-action-approval-0001'
from prepared_family;

insert into public.weekly_exceptional_pay_generations(
  id,family_id,generation_number,request_idempotency_key,reason,
  complete_prior_vector_json,complete_prior_vector_hash,
  complete_next_vector_json,complete_next_vector_hash,
  fixed_target_source_state_fingerprint,lifecycle_state,published_at_utc,result_hash
) select
  'f1000000-0000-4000-8000-00000000000b',(result->>'family_id')::uuid,1,
  'protected-action-generation-0001','INITIAL_APPROVAL',
  '{"components":[]}'::jsonb,pg_catalog.decode(pg_catalog.repeat('21',32),'hex'),
  '{"schema_version":"WEEKLY_PROTECTED_TARGET_VECTOR_V1","components":[],"component_count":0,"is_zero_entitlement":true}'::jsonb,
  pg_catalog.decode(pg_catalog.repeat('22',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('23',32),'hex'),'PUBLISHED',
  pg_catalog.statement_timestamp(),pg_catalog.decode(pg_catalog.repeat('24',32),'hex')
from prepared_family;

insert into public.weekly_exceptional_pay_family_events(
  id,family_id,event_sequence,durable_work_event_id,evidence_approval_id,
  work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,
  source_proposal_snapshot_json,source_proposal_hash,
  fixed_office_target_snapshot_json,fixed_office_target_hash,state,
  office_actor_user_id,office_reason,event_hash
) select
  'f1000000-0000-4000-8000-00000000000c',(result->>'family_id')::uuid,1,
  (result->>'work_event_id')::uuid,'f1000000-0000-4000-8000-00000000000a',
  '2026-09-07','2026-09-07 09:00:00','2026-09-07 17:00:00',30,
  '{"server":"weekly-calculator"}'::jsonb,
  '{"source_present":false}'::jsonb,pg_catalog.decode(pg_catalog.repeat('31',32),'hex'),
  '{"work_date":"2026-09-07","start_at_local":"2026-09-07 09:00:00","end_at_local":"2026-09-07 17:00:00","break_minutes":30}'::jsonb,
  pg_catalog.decode(pg_catalog.repeat('32',32),'hex'),'WAIT',
  'f1000000-0000-4000-8000-000000000001','Initial protected hours.',
  pg_catalog.decode(pg_catalog.repeat('33',32),'hex')
from prepared_family;

insert into public.weekly_exceptional_pay_target_events(
  id,family_id,approval_id,event_sequence,fixed_target_component_snapshot,
  current_source_proposal_snapshot,complete_prior_family_vector_fingerprint,
  complete_next_family_vector_fingerprint,reason,resulting_lifecycle_state,
  financial_generation_id,actor_user_id,event_hash,idempotency_key
) select
  'f1000000-0000-4000-8000-00000000000d',(result->>'family_id')::uuid,
  'f1000000-0000-4000-8000-00000000000a',1,'{}'::jsonb,
  '{"source_present":false}'::jsonb,pg_catalog.decode(pg_catalog.repeat('21',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('22',32),'hex'),'INITIAL_APPROVAL',
  'WAITING_SOURCE','f1000000-0000-4000-8000-00000000000b',
  'f1000000-0000-4000-8000-000000000001',
  pg_catalog.decode(pg_catalog.repeat('34',32),'hex'),'protected-action-target-0001'
from prepared_family;

update public.weekly_exceptional_pay_target_families family
set current_generation_id='f1000000-0000-4000-8000-00000000000b',
    current_generation_number=1,
    current_complete_target_vector_hash=pg_catalog.decode(pg_catalog.repeat('22',32),'hex'),
    current_source_proposal_hash=pg_catalog.decode(pg_catalog.repeat('31',32),'hex'),
    current_lifecycle_state='WAITING_SOURCE',c1_publication_state='LIVE',
    current_component_count=0
where family.id=(select (result->>'family_id')::uuid from prepared_family);
update public.weekly_exceptional_orchestration_runs run
set state='COMPLETE',after_state_fingerprint=pg_catalog.decode(pg_catalog.repeat('24',32),'hex'),
    completed_at_utc=pg_catalog.statement_timestamp()
where run.id=(select (result->>'orchestration_run_id')::uuid from prepared_family);

create temp table before_wait as
select
  (select count(*) from public.timesheets_financials) as financials,
  (select count(*) from public.weekly_exceptional_pay_generations) as generations,
  (select count(*) from public.weekly_exceptional_c1_publication_requests) as publications,
  (select count(*) from public.invoice_lines) as invoice_lines,
  (select count(*) from public.pay_batches) as pay_batches;

create temp table amend_result as
select public.weekly_exceptional_pay_prepare_action_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_ACTION_PREPARE_V1',
    'actor_user_id','f1000000-0000-4000-8000-000000000001',
    'family_id',(result->>'family_id')::uuid,
    'source_cycle_id','f1000000-0000-4000-8000-000000000009',
    'work_event_id',(result->>'work_event_id')::uuid,
    'action','AMEND','expected_family_bound_version','1',
    'protected_schedule',pg_catalog.jsonb_build_object(
      'work_date','2026-09-07','start_at_local','2026-09-07 08:00:00',
      'end_at_local','2026-09-07 18:00:00','break_minutes',60
    ),
    'reason','Office amended the protected hours.',
    'idempotency_key','protected-action-amend-0001'
  )
) as result from prepared_family;
select pg_temp.assert_true(
  (select result->>'request_kind'='AMEND'
          and result#>>'{protected_schedule,start_at_local}'='2026-09-07 08:00:00'
          and not (result->>'idempotent_replay')::boolean from amend_result),
  'AMEND must seal the Office schedule against the existing family'
);
select pg_temp.assert_true(
  (select not (public.weekly_exceptional_pay_action_publication_status_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_PROTECTED_ACTION_PUBLICATION_STATUS_V1',
      'actor_user_id','f1000000-0000-4000-8000-000000000001',
      'family_id',(result->>'family_id')::uuid,
      'orchestration_run_id',(result->>'orchestration_run_id')::uuid
    ))#>>'{staged}')::boolean from amend_result),
  'an unstaged run must never invent a C1 publication identity'
);

do $idempotency_collision$
declare v_family uuid; v_event uuid;
begin
  select (result->>'family_id')::uuid,(result->>'work_event_id')::uuid
    into v_family,v_event from prepared_family;
  begin
    perform public.weekly_exceptional_pay_prepare_action_v1(
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_PROTECTED_ACTION_PREPARE_V1',
        'actor_user_id','f1000000-0000-4000-8000-000000000001',
        'family_id',v_family,'source_cycle_id','f1000000-0000-4000-8000-000000000009',
        'work_event_id',v_event,'action','AMEND','expected_family_bound_version','1',
        'protected_schedule',pg_catalog.jsonb_build_object(
          'work_date','2026-09-07','start_at_local','2026-09-07 08:00:00',
          'end_at_local','2026-09-07 18:00:00','break_minutes',60
        ),'reason','Different bytes must collide.',
        'idempotency_key','protected-action-amend-0001'
      )
    );
    raise exception 'IDEMPOTENCY_COLLISION_WAS_ACCEPTED';
  exception when unique_violation then
    if sqlerrm<>'WEEKLY_PROTECTED_ACTION_IDEMPOTENCY_COLLISION' then raise; end if;
  end;
end;
$idempotency_collision$;

do $all_follow_up_actions_prepare$
declare
  v_family uuid;
  v_event uuid;
  v_action text;
  v_result jsonb;
begin
  select (result->>'family_id')::uuid,(result->>'work_event_id')::uuid
    into v_family,v_event from prepared_family;
  foreach v_action in array array['WITHDRAW','RECONCILE','RECORD_NOT_WORKED'] loop
    v_result:=public.weekly_exceptional_pay_prepare_action_v1(
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_PROTECTED_ACTION_PREPARE_V1',
        'actor_user_id','f1000000-0000-4000-8000-000000000001',
        'family_id',v_family,'source_cycle_id','f1000000-0000-4000-8000-000000000009',
        'work_event_id',v_event,'action',v_action,'expected_family_bound_version','1',
        'protected_schedule',null,'reason','Prepare '||v_action||' without client economics.',
        'idempotency_key','protected-action-'||pg_catalog.lower(v_action)||'-0001'
      )
    );
    perform pg_temp.assert_true(
      v_result->>'request_kind'=v_action
        and v_result->'protected_schedule'=pg_catalog.jsonb_build_object(
          'work_date','2026-09-07','start_at_local','2026-09-07T09:00:00',
          'end_at_local','2026-09-07T17:00:00','break_minutes',30
        ),
      v_action||' must prepare against the current server-owned protected schedule'
    );
  end loop;
end;
$all_follow_up_actions_prepare$;

create temp table wait_prepared as
select public.weekly_exceptional_pay_prepare_action_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_ACTION_PREPARE_V1',
    'actor_user_id','f1000000-0000-4000-8000-000000000001',
    'family_id',(result->>'family_id')::uuid,
    'source_cycle_id','f1000000-0000-4000-8000-000000000009',
    'work_event_id',(result->>'work_event_id')::uuid,
    'action','WAIT','expected_family_bound_version','1',
    'protected_schedule',null,'reason','Wait for corrected source.',
    'idempotency_key','protected-action-wait-0001'
  )
) as result from prepared_family;

create temp table wait_context as
select public.weekly_exceptional_pay_action_context_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_ACTION_CONTEXT_V1',
    'actor_user_id','f1000000-0000-4000-8000-000000000001',
    'family_id',(result->>'family_id')::uuid,
    'orchestration_run_id',(result->>'orchestration_run_id')::uuid,
    'source_cycle_id','f1000000-0000-4000-8000-000000000009',
    'work_event_id',(result->>'work_event_id')::uuid,
    'protected_schedule',result->'protected_schedule',
    'evidence_timesheet_id',null
  )
) as result from wait_prepared;
select pg_temp.assert_true(
  (select result->>'action'='WAIT'
          and result->>'current_target_vector_sha256'=pg_catalog.repeat('22',32)
          and not (result#>>'{source_proposal,source_present}')::boolean
          and result#>>'{protected_schedule,start_at_local}'='2026-09-07T09:00:00'
   from wait_context),
  'WAIT context must use the current protected schedule and server-observed source absence'
);

create temp table wait_result as
select public.weekly_exceptional_pay_wait_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_WAIT_V1',
    'actor_user_id','f1000000-0000-4000-8000-000000000001',
    'family_id',context.result->>'family_id',
    'orchestration_run_id',context.result->>'orchestration_run_id',
    'source_cycle_id',context.result->>'source_cycle_id',
    'work_event_id',context.result->>'work_event_id',
    'expected_family_bound_version',context.result->>'family_bound_version',
    'expected_target_vector_sha256',context.result->>'current_target_vector_sha256',
    'source_proposal',context.result->'source_proposal',
    'protected_schedule',context.result->'protected_schedule',
    'reason','Wait for corrected source.',
    'idempotency_key','protected-action-wait-0001:wait'
  )
) as result from wait_context context;
select pg_temp.assert_true(
  (select result->>'outcome'='WAITING_FOR_SOURCE'
          and not (result->>'idempotent_replay')::boolean from wait_result)
  and (select bound_version=2 and current_generation_number=1
       from public.weekly_exceptional_pay_target_families
       where id=(select (result->>'family_id')::uuid from prepared_family))
  and (select financials=(select count(*) from public.timesheets_financials)
          and generations=(select count(*) from public.weekly_exceptional_pay_generations)
          and publications=(select count(*) from public.weekly_exceptional_c1_publication_requests)
          and invoice_lines=(select count(*) from public.invoice_lines)
          and pay_batches=(select count(*) from public.pay_batches) from before_wait),
  'WAIT must change only protected audit/lifecycle state and no financial, invoice or Banking row'
);

create temp table wait_prepare_replay as
select public.weekly_exceptional_pay_prepare_action_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_ACTION_PREPARE_V1',
    'actor_user_id','f1000000-0000-4000-8000-000000000001',
    'family_id',(result->>'family_id')::uuid,
    'source_cycle_id','f1000000-0000-4000-8000-000000000009',
    'work_event_id',(result->>'work_event_id')::uuid,
    'action','WAIT','expected_family_bound_version','1',
    'protected_schedule',null,'reason','Wait for corrected source.',
    'idempotency_key','protected-action-wait-0001'
  )
) as result from prepared_family;
select pg_temp.assert_true(
  (select (result->>'idempotent_replay')::boolean and result->>'run_state'='COMPLETE'
   from wait_prepare_replay),
  'an exact completed replay must survive the family version advance'
);
select pg_temp.assert_true(
  (select (public.weekly_exceptional_pay_wait_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_PROTECTED_WAIT_V1',
      'actor_user_id','f1000000-0000-4000-8000-000000000001',
      'family_id',context.result->>'family_id',
      'orchestration_run_id',context.result->>'orchestration_run_id',
      'source_cycle_id',context.result->>'source_cycle_id',
      'work_event_id',context.result->>'work_event_id',
      'expected_family_bound_version',context.result->>'family_bound_version',
      'expected_target_vector_sha256',context.result->>'current_target_vector_sha256',
      'source_proposal',context.result->'source_proposal',
      'protected_schedule',context.result->'protected_schedule',
      'reason','Wait for corrected source.',
      'idempotency_key','protected-action-wait-0001:wait'
    )
  )->>'idempotent_replay')::boolean from wait_context context),
  'WAIT application must be idempotent after completion'
);

do $stale_refused$
declare v_family uuid; v_event uuid;
begin
  select (result->>'family_id')::uuid,(result->>'work_event_id')::uuid
    into v_family,v_event from prepared_family;
  begin
    perform public.weekly_exceptional_pay_prepare_action_v1(
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_PROTECTED_ACTION_PREPARE_V1',
        'actor_user_id','f1000000-0000-4000-8000-000000000001',
        'family_id',v_family,'source_cycle_id','f1000000-0000-4000-8000-000000000009',
        'work_event_id',v_event,'action','WITHDRAW','expected_family_bound_version','1',
        'protected_schedule',null,'reason','Stale action must fail.',
        'idempotency_key','protected-action-stale-0001'
      )
    );
    raise exception 'STALE_ACTION_WAS_ACCEPTED';
  exception when serialization_failure then
    if sqlerrm<>'WEEKLY_PROTECTED_ACTION_STALE' then raise; end if;
  end;
end;
$stale_refused$;

select pg_temp.assert_true(
  has_function_privilege('service_role',
    'public.weekly_exceptional_pay_prepare_action_v1(jsonb)','EXECUTE')
  and has_function_privilege('service_role',
    'public.weekly_exceptional_pay_action_context_v1(jsonb)','EXECUTE')
  and has_function_privilege('service_role',
    'public.weekly_exceptional_pay_action_publication_status_v1(jsonb)','EXECUTE')
  and has_function_privilege('service_role',
    'public.weekly_exceptional_pay_wait_atomic_v1(jsonb)','EXECUTE')
  and not has_function_privilege('anon',
    'public.weekly_exceptional_pay_prepare_action_v1(jsonb)','EXECUTE')
  and not has_function_privilege('authenticated',
    'public.weekly_exceptional_pay_prepare_action_v1(jsonb)','EXECUTE')
  and not has_function_privilege('anon',
    'public.weekly_exceptional_pay_action_context_v1(jsonb)','EXECUTE')
  and not has_function_privilege('authenticated',
    'public.weekly_exceptional_pay_action_context_v1(jsonb)','EXECUTE')
  and not has_function_privilege('anon',
    'public.weekly_exceptional_pay_action_publication_status_v1(jsonb)','EXECUTE')
  and not has_function_privilege('authenticated',
    'public.weekly_exceptional_pay_action_publication_status_v1(jsonb)','EXECUTE')
  and not has_function_privilege('anon',
    'public.weekly_exceptional_pay_wait_atomic_v1(jsonb)','EXECUTE')
  and not has_function_privilege('authenticated',
    'public.weekly_exceptional_pay_wait_atomic_v1(jsonb)','EXECUTE'),
  'every protected action RPC must remain service-only'
);


-- ===========================================================================
-- WP-57: NHSP row order must not decide the protected-shift proposal
-- (WP-52 handoff N1, pack 14 s4.2.5).
--
-- EXECUTED before the fix, on a build from empty: the same corrected shift -
-- 8 h, then a full reversal of it plus a re-issued 9 h line - answered
-- `source_present=true, 540 approved minutes` on one run of this fixture and
-- `source_present=false, 0` on the next, because the ranking that decided it
-- fell through `movement.created_at_utc` (identical for every movement of one
-- finalisation) to `movement.id`, a random UUID.  With `source_present=false`
-- the RECORD_NOT_WORKED guard stopped guarding, so Office could record a
-- worked and re-issued shift as not worked while the Client stood invoiced
-- GBP 180.00.  Split across reports it was deterministically wrong.
--
-- These cases drive the REAL owners - weekly_source_projection_rows_apply_
-- atomic_v1 with the broker's SCHEDULE_TUPLE identity, weekly_source_finalise_
-- atomic_v1, weekly_exceptional_pay_prepare_family_v1,
-- weekly_exceptional_pay_prepare_action_v1 and
-- weekly_exceptional_pay_action_context_v1 - and assert that EVERY permutation
-- of the same facts produces the SAME answer, that the two sentinels which
-- should differ do differ, and that the RECORD_NOT_WORKED guard follows the
-- source rather than the row order.
--
-- Its own source group, Trust and policy: these cases add several finalised
-- cycles, and a later final cycle inside a shared scope changes what other
-- verifiers are allowed to do.
-- ===========================================================================

insert into public.clients(id,name)
values ('57000000-0000-4000-8000-000000000002','WP57 NHSP Trust');
insert into public.client_settings(
  client_id,vat_rate_pct,effective_from,is_nhsp,autoprocess_hr,requires_hr,
  no_timesheet_required
) values (
  '57000000-0000-4000-8000-000000000002',20,'2026-01-01',true,false,false,false
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,
  cutoff_local_time,nhsp_report_heading_name
) values (
  '57000000-0000-4000-8000-000000000005','TEST','57000000-0000-4000-8000-000000000006',
  'WP57_NHSP','WP57 NHSP','NHSP',3,'15:00','WP57 NHSP Trust'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  '57000000-0000-4000-8000-000000000007','57000000-0000-4000-8000-000000000005',
  '57000000-0000-4000-8000-000000000002','2026-01-01',
  'f1000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,self_bill_correction_presentation,
  weekly_rate_classification_method,created_by_user_id
) values (
  '57000000-0000-4000-8000-000000000012','57000000-0000-4000-8000-000000000005',
  '57000000-0000-4000-8000-000000000002','2026-01-01','SOURCE_AUTHORITY','CHECK_ONLY',
  true,'FULL_REVERSAL_REPLACEMENT','SPLIT_RATE_WINDOWS',
  'f1000000-0000-4000-8000-000000000001'
);

create function pg_temp.wp57_econ(p_net integer,p_break integer,p_sign integer)
returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
    'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1','source_mode','NHSP_WEEKLY',
    'rate_method','SPLIT_RATE_WINDOWS','sign',p_sign,
    'paid_minutes',p_net,'break_minutes',p_break,
    'bucket_minutes',pg_catalog.jsonb_build_object('day',p_net,'night',0,'sat',0,'sun',0,'bh',0),
    'hours',pg_catalog.jsonb_build_object(
      'day',pg_catalog.round((p_net::numeric/60)*p_sign,2),'night',0,'sat',0,'sun',0,'bh',0),
    'pay_rates',pg_catalog.jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
    'charge_rates',pg_catalog.jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
    'total_pay_pence',((pg_catalog.round(pg_catalog.round(
        pg_catalog.abs(pg_catalog.round((p_net::numeric/60)*p_sign,2))*10,2)*100,0)::bigint)*p_sign)::text,
    'calculated_charge_pence',((pg_catalog.round(pg_catalog.round(
        pg_catalog.abs(pg_catalog.round((p_net::numeric/60)*p_sign,2))*20,2)*100,0)::bigint)*p_sign)::text
  );
$function$;

create function pg_temp.wp57_candidate(p_code text) returns jsonb language plpgsql as $function$
declare v_cand uuid:=pg_catalog.gen_random_uuid(); v_con uuid:=pg_catalog.gen_random_uuid();
begin
  insert into public.candidates(id,display_name) values (v_cand,'WP57 '||p_code);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp
  ) values (
    v_con,v_cand,'57000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
    '{}'::jsonb,null,true,false,false,false,false
  );
  return pg_catalog.jsonb_build_object('candidate_id',v_cand,'contract_id',v_con,'code',p_code);
end; $function$;

create function pg_temp.wp57_scope(p_code text,p_week_ending date,p_cutoff timestamptz,
  p_rows integer) returns jsonb language plpgsql as $function$
declare v_cycle uuid:=pg_catalog.gen_random_uuid(); v_scope uuid:=pg_catalog.gen_random_uuid();
        v_upload uuid:=pg_catalog.gen_random_uuid(); v_pub uuid:=pg_catalog.gen_random_uuid();
        v_actor constant uuid:='f1000000-0000-4000-8000-000000000001';
begin
  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (v_cycle,'57000000-0000-4000-8000-000000000005',p_week_ending,p_cutoff,'OPEN',1,
    'REBUILDING');
  insert into public.weekly_source_report_scopes(
    id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc,
    version,state,projection_state
  ) values (v_scope,v_cycle,'TEST','57000000-0000-4000-8000-000000000006',
    '57000000-0000-4000-8000-000000000005','57000000-0000-4000-8000-000000000002',p_cutoff,
    1,'OPEN','REBUILDING');
  insert into public.weekly_source_uploads(
    id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,
    workbook_part_and_sheet_fingerprint,header_coordinate_map_json,header_coordinate_map_hash,
    money_lexical_authority_version,declared_scope_fingerprint,coverage_proof_kind,
    physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id,
    file_metadata_json
  ) values (
    v_upload,v_cycle,v_scope,'wp57-'||p_code||'.xlsx',
    private.weekly_source_sha256_jsonb_v1('WP57_CONTENT',pg_catalog.to_jsonb(p_code)),200,
    '32222222-2222-4222-8222-222222222222','WP57_PARSER_V1','NHSP_BACKING_NORMALISER_V1',
    private.weekly_source_sha256_jsonb_v1('WP57_WORKBOOK',pg_catalog.to_jsonb(p_code)),'{}'::jsonb,
    private.weekly_source_sha256_jsonb_v1('WP57_HEADERS',pg_catalog.to_jsonb(p_code)),
    'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
    private.weekly_source_sha256_jsonb_v1('WP57_SCOPE',pg_catalog.to_jsonb(p_code)),
    'NHSP_TRUST_REPORT_SCOPE',p_rows,p_rows,
    private.weekly_source_sha256_jsonb_v1('WP57_ROWS',pg_catalog.to_jsonb(p_code)),
    'CURRENT',v_actor,
    pg_catalog.jsonb_build_object(
      'nhsp_report_number','BR-57-'||p_code,'nhsp_report_heading_name','WP57 NHSP Trust')
  );
  update public.weekly_source_report_scopes
  set current_complete_upload_id=v_upload where id=v_scope;
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
    authority_scope_version,comparison_manifest_hash,issue_set_hash,state
  ) values (v_pub,v_cycle,'NHSP_REPORT_SCOPE',v_scope,v_upload,1,
    private.weekly_source_sha256_jsonb_v1('WP57_CMP',pg_catalog.to_jsonb(p_code)),
    private.weekly_source_sha256_jsonb_v1('WP57_ISS',pg_catalog.to_jsonb(p_code)),'BUILDING');
  return pg_catalog.jsonb_build_object(
    'cycle_id',v_cycle,'scope_id',v_scope,'upload_id',v_upload,'pub_id',v_pub,'code',p_code);
end; $function$;

create function pg_temp.wp57_row(p_scope jsonb,p_ordinal integer,p_reference text,
  p_candidate jsonb,p_date date,p_start time,p_end time,p_charge bigint)
returns uuid language plpgsql as $function$
declare v_row uuid:=pg_catalog.gen_random_uuid();
        v_net integer:=(pg_catalog.date_part('epoch',p_end-p_start)/60)::integer;
        v_commission bigint:=case when p_charge<0 then -500 else 500 end;
begin
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
    source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
    actual_net_minutes,row_finalisation_state,role_band_source,source_commission_pence,
    source_total_cost_pence,source_shift_charge_pence,source_money_parse_state,
    source_qualification_profile_version,source_expense_parse_state,normalised_row_hash
  ) values (
    v_row,(p_scope->>'upload_id')::uuid,p_ordinal,p_reference,
    'WP57 '||(p_candidate->>'code'),'WP57 NHSP Trust',
    p_date,(p_date::text||' '||p_start::text)::timestamp,
    (p_date::text||' '||p_end::text)::timestamp,0,v_net,'SOURCE_WORKED','BAND 5',
    v_commission,p_charge-v_commission,p_charge,'VALID',
    'NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
    private.weekly_source_sha256_jsonb_v1('WP57_ROW',pg_catalog.jsonb_build_object(
      's',p_scope->>'code','o',p_ordinal,'r',p_reference))
  );
  return v_row;
end; $function$;

create function pg_temp.wp57_entry(p_candidate jsonb,p_row uuid,p_charge bigint,p_net integer)
returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'upload_row_id',p_row,'mapping_state','RESOLVED',
    'candidate_id',(p_candidate->>'candidate_id')::uuid,
    'client_id','57000000-0000-4000-8000-000000000002',
    'contract_id',(p_candidate->>'contract_id')::uuid,
    'contract_selection_method','AUTO_UNIQUE',
    'qualifying_contract_ids',pg_catalog.jsonb_build_array((p_candidate->>'contract_id')::uuid),
    -- 24 s9: the Reference Number is never the durable work identity.  This is
    -- the identity the broker sends for every NHSP row.
    'identity_kind','SCHEDULE_TUPLE',
    'link_kind',case when p_charge<0 then 'FULL_NEGATIVE_SOURCE' else 'POSITIVE_SOURCE' end,
    'economic_snapshot',pg_temp.wp57_econ(p_net,0,case when p_charge<0 then -1 else 1 end),
    'charge_check',pg_catalog.jsonb_build_object(
      'row_sign_kind',case when p_charge<0 then 'FULL_NEGATIVE' else 'POSITIVE' end,
      'source_commission_pence',(case when p_charge<0 then -500 else 500 end)::text,
      'source_total_cost_pence',(p_charge-case when p_charge<0 then -500 else 500 end)::text,
      'source_shift_charge_pence',p_charge::text,
      'calculated_segment_charge_pence',
        (pg_temp.wp57_econ(p_net,0,case when p_charge<0 then -1 else 1 end)
          ->>'calculated_charge_pence'),
      'comparison_result','EXACT','comparison_reason_code','EXACT','phase_severity','NONE')
  );
$function$;

-- One report: stage its rows in the exact physical order given, publish and
-- finalise, all through the real owners.
create function pg_temp.wp57_report(p_code text,p_candidate jsonb,p_week_ending date,
  p_cutoff timestamptz,p_rows jsonb) returns jsonb language plpgsql as $function$
declare v_scope jsonb; v_entries jsonb:='[]'::jsonb; v_row jsonb; v_row_id uuid;
        v_net integer; v_index integer;
begin
  v_scope:=pg_temp.wp57_scope(p_code,p_week_ending,p_cutoff,
    pg_catalog.jsonb_array_length(p_rows));
  for v_index in 0..pg_catalog.jsonb_array_length(p_rows)-1 loop
    v_row:=p_rows->v_index;
    v_net:=(pg_catalog.date_part('epoch',
      (v_row->>'end')::time-(v_row->>'start')::time)/60)::integer;
    v_row_id:=pg_temp.wp57_row(v_scope,v_index+1,p_code||'-R'||v_index,p_candidate,
      (v_row->>'date')::date,(v_row->>'start')::time,(v_row->>'end')::time,
      (v_row->>'charge')::bigint);
    v_entries:=v_entries||pg_catalog.jsonb_build_array(
      pg_temp.wp57_entry(p_candidate,v_row_id,(v_row->>'charge')::bigint,v_net));
  end loop;
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    'f1000000-0000-4000-8000-000000000001',(v_scope->>'pub_id')::uuid,v_entries);
  update public.weekly_source_projection_publications
  set state='CURRENT',published_at_utc=pg_catalog.clock_timestamp()
  where id=(v_scope->>'pub_id')::uuid;
  update public.weekly_source_report_scopes
  set projection_state='CURRENT',current_projection_publication_id=(v_scope->>'pub_id')::uuid
  where id=(v_scope->>'scope_id')::uuid;
  perform public.weekly_source_finalise_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','f1000000-0000-4000-8000-000000000001',
    'source_cycle_id',(v_scope->>'cycle_id')::uuid,'authority_scope_kind','NHSP_REPORT_SCOPE',
    'report_scope_id',(v_scope->>'scope_id')::uuid,'upload_id',(v_scope->>'upload_id')::uuid,
    'projection_publication_id',(v_scope->>'pub_id')::uuid,'expected_authority_scope_version',1,
    'expected_row_manifest_hash',(select pg_catalog.encode(upload_row.row_manifest_hash,'hex')
      from public.weekly_source_uploads upload_row
      where upload_row.id=(v_scope->>'upload_id')::uuid),
    'expected_comparison_manifest_hash',
      (select pg_catalog.encode(publication.comparison_manifest_hash,'hex')
       from public.weekly_source_projection_publications publication
       where publication.id=(v_scope->>'pub_id')::uuid),
    'expected_issue_set_hash',(select pg_catalog.encode(publication.issue_set_hash,'hex')
      from public.weekly_source_projection_publications publication
      where publication.id=(v_scope->>'pub_id')::uuid)
  ));
  return v_scope;
end; $function$;

-- The Client's net invoiced position for this Candidate.
create function pg_temp.wp57_invoiced(p_candidate jsonb) returns numeric
language sql stable as $function$
  select coalesce(pg_catalog.sum(movement.invoice_presentation_charge_pence),0)::numeric/100
  from public.weekly_source_billing_movements movement
  where movement.candidate_id=(p_candidate->>'candidate_id')::uuid;
$function$;

-- Open a protected family against the NHSP work event.  The approval,
-- generation and family-event rows are SEEDED in the same shape this file
-- already seeds them above for the HealthRoster fixture; everything MEASURED
-- comes from the real owners.
create function pg_temp.wp57_family(p_candidate jsonb,p_cycle uuid,p_event uuid,p_tag text)
returns uuid language plpgsql as $function$
declare
  v_actor constant uuid:='f1000000-0000-4000-8000-000000000001';
  v_family jsonb; v_family_id uuid;
  v_approval uuid:=pg_catalog.gen_random_uuid();
  v_generation uuid:=pg_catalog.gen_random_uuid();
  v_h11 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_11',pg_catalog.to_jsonb(p_tag));
  v_h12 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_12',pg_catalog.to_jsonb(p_tag));
  v_h13 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_13',pg_catalog.to_jsonb(p_tag));
  v_h14 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_14',pg_catalog.to_jsonb(p_tag));
  v_h21 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_21',pg_catalog.to_jsonb(p_tag));
  v_h22 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_22',pg_catalog.to_jsonb(p_tag));
  v_h23 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_23',pg_catalog.to_jsonb(p_tag));
  v_h24 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_24',pg_catalog.to_jsonb(p_tag));
  v_h31 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_31',pg_catalog.to_jsonb(p_tag));
  v_h32 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_32',pg_catalog.to_jsonb(p_tag));
  v_h33 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_33',pg_catalog.to_jsonb(p_tag));
  v_h34 bytea:=private.weekly_source_sha256_jsonb_v1('WP57_SEED_34',pg_catalog.to_jsonb(p_tag));
begin
  v_family:=public.weekly_exceptional_pay_prepare_family_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'source_cycle_id',p_cycle,
    'candidate_id',(p_candidate->>'candidate_id')::uuid,
    'client_id','57000000-0000-4000-8000-000000000002',
    'contract_id',(p_candidate->>'contract_id')::uuid,
    'week_ending_date','2026-09-13','work_event_id',p_event,'work_date','2026-09-07',
    'start_at_local','2026-09-07 09:00:00','end_at_local','2026-09-07 18:00:00',
    'break_minutes',0,'evidence_timesheet_id',null,
    'reason','Candidate reports this shift was worked.',
    'idempotency_key','wp57-protected-family-key-'||p_tag
  ));
  v_family_id:=(v_family->>'family_id')::uuid;

  insert into public.weekly_exceptional_payment_approvals(
    id,pay_target_family_id,work_event_id,candidate_id,client_id,contract_id,
    week_ending,protected_work_date,protected_start_at_local,protected_end_at_local,
    protected_break_minutes,contributing_issue_episode_ids,
    contributing_issue_episode_ids_hash,signed_schedule_fact_hash,
    contract_rate_policy_source_fingerprint,approved_by_user_id,approval_reason,
    source_cycle_id,approved_target_pay_components_json,approved_target_gross,
    creation_orchestration_run_id,approval_hash,creation_idempotency_key
  ) values (
    v_approval,v_family_id,(v_family->>'work_event_id')::uuid,
    (p_candidate->>'candidate_id')::uuid,'57000000-0000-4000-8000-000000000002',
    (p_candidate->>'contract_id')::uuid,'2026-09-13','2026-09-07',
    '2026-09-07 09:00:00','2026-09-07 18:00:00',0,'{}'::uuid[],
    v_h11,v_h12,v_h13,v_actor,'Initial protected hours.',
    p_cycle,'{}'::jsonb,90,(v_family->>'orchestration_run_id')::uuid,v_h14,
    'wp57-approval-'||p_tag
  );
  insert into public.weekly_exceptional_pay_generations(
    id,family_id,generation_number,request_idempotency_key,reason,
    complete_prior_vector_json,complete_prior_vector_hash,complete_next_vector_json,
    complete_next_vector_hash,fixed_target_source_state_fingerprint,lifecycle_state,
    published_at_utc,result_hash
  ) values (
    v_generation,v_family_id,1,'wp57-generation-'||p_tag,'INITIAL_APPROVAL',
    '{"components":[]}'::jsonb,v_h21,
    '{"schema_version":"WEEKLY_PROTECTED_TARGET_VECTOR_V1","components":[],"component_count":0,"is_zero_entitlement":true}'::jsonb,
    v_h22,v_h23,'PUBLISHED',pg_catalog.statement_timestamp(),v_h24
  );
  insert into public.weekly_exceptional_pay_family_events(
    id,family_id,event_sequence,durable_work_event_id,evidence_approval_id,work_date,
    start_at_local,end_at_local,break_minutes,rate_classification_json,
    source_proposal_snapshot_json,source_proposal_hash,fixed_office_target_snapshot_json,
    fixed_office_target_hash,state,office_actor_user_id,office_reason,event_hash
  ) values (
    pg_catalog.gen_random_uuid(),v_family_id,1,(v_family->>'work_event_id')::uuid,v_approval,
    '2026-09-07','2026-09-07 09:00:00','2026-09-07 18:00:00',0,
    '{"server":"weekly-calculator"}'::jsonb,'{"source_present":false}'::jsonb,v_h31,
    '{"work_date":"2026-09-07","start_at_local":"2026-09-07 09:00:00","end_at_local":"2026-09-07 18:00:00","break_minutes":0}'::jsonb,
    v_h32,'WAIT',v_actor,'Initial protected hours.',v_h33
  );
  insert into public.weekly_exceptional_pay_target_events(
    id,family_id,approval_id,event_sequence,fixed_target_component_snapshot,
    current_source_proposal_snapshot,complete_prior_family_vector_fingerprint,
    complete_next_family_vector_fingerprint,reason,resulting_lifecycle_state,
    financial_generation_id,actor_user_id,event_hash,idempotency_key
  ) values (
    pg_catalog.gen_random_uuid(),v_family_id,v_approval,1,'{}'::jsonb,
    '{"source_present":false}'::jsonb,v_h21,v_h22,'INITIAL_APPROVAL','WAITING_SOURCE',
    v_generation,v_actor,v_h34,'wp57-target-'||p_tag
  );
  update public.weekly_exceptional_pay_target_families
  set current_generation_id=v_generation,current_generation_number=1,
      current_complete_target_vector_hash=v_h22,current_source_proposal_hash=v_h31,
      current_lifecycle_state='WAITING_SOURCE',c1_publication_state='LIVE',
      current_component_count=0
  where id=v_family_id;
  update public.weekly_exceptional_orchestration_runs
  set state='COMPLETE',after_state_fingerprint=v_h24,
      completed_at_utc=pg_catalog.statement_timestamp()
  where id=(v_family->>'orchestration_run_id')::uuid;
  return v_family_id;
end; $function$;

create function pg_temp.wp57_context(p_family uuid,p_cycle uuid,p_event uuid,p_tag text,
  p_action text) returns jsonb language plpgsql as $function$
declare v_actor constant uuid:='f1000000-0000-4000-8000-000000000001'; v_run jsonb;
begin
  v_run:=public.weekly_exceptional_pay_prepare_action_v1(pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_ACTION_PREPARE_V1','actor_user_id',v_actor,
    'family_id',p_family,'source_cycle_id',p_cycle,'work_event_id',p_event,
    'action',p_action,
    'expected_family_bound_version',(select family.bound_version::text
      from public.weekly_exceptional_pay_target_families family where family.id=p_family),
    'protected_schedule',null,'reason','WP57 '||p_action||' '||p_tag,
    'idempotency_key','wp57-protected-action-key-'||p_tag||'-'||pg_catalog.lower(p_action)
  ));
  return public.weekly_exceptional_pay_action_context_v1(pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_ACTION_CONTEXT_V1','actor_user_id',v_actor,
    'family_id',p_family,'orchestration_run_id',(v_run->>'orchestration_run_id')::uuid,
    'source_cycle_id',p_cycle,'work_event_id',p_event,
    'protected_schedule',v_run->'protected_schedule','evidence_timesheet_id',null
  ));
end; $function$;

-- The proposal owner's answer for the selected shift, as one comparable value:
-- is the source present, how many minutes does it approve, and what does the
-- client-source evidence row carry for the same work event.
create function pg_temp.wp57_position(p_ctx jsonb) returns text
language sql immutable as $function$
  select pg_catalog.format('present=%s|source_minutes=%s|approved_minutes=%s',
    p_ctx#>>'{source_proposal,source_present}',
    p_ctx#>>'{source_proposal,source_minutes}',
    coalesce((select evidence.value->>'approved_minutes'
      from pg_catalog.jsonb_array_elements(p_ctx->'client_sources') evidence(value)
      where evidence.value->>'external_identity'=p_ctx->>'work_event_id'),'<absent>'));
$function$;

-- Build one whole case: its reports in the given order, then the protected
-- family and the proposal context, then the RECORD_NOT_WORKED verdict.
create function pg_temp.wp57_case(p_tag text,p_reports jsonb,p_first_week date)
returns jsonb language plpgsql as $function$
declare
  v_candidate jsonb; v_scope jsonb; v_cycle uuid; v_event uuid; v_events integer;
  v_family uuid; v_ctx jsonb; v_guard text; v_index integer;
  v_week date:=p_first_week;
  -- distinct, increasing cutoffs per case and per report; no two cycles share one
  v_cutoff timestamptz:='2026-08-02T10:00:00Z'::timestamptz
    +pg_catalog.make_interval(mins=>(p_first_week-'2029-01-01'::date));
begin
  v_candidate:=pg_temp.wp57_candidate(p_tag);
  for v_index in 0..pg_catalog.jsonb_array_length(p_reports)-1 loop
    v_week:=v_week+7;
    v_cutoff:=v_cutoff+pg_catalog.make_interval(mins=>1);
    v_scope:=pg_temp.wp57_report(p_tag||'r'||v_index,v_candidate,v_week,v_cutoff,
      p_reports->v_index);
    v_cycle:=(v_scope->>'cycle_id')::uuid;
  end loop;
  select pg_catalog.count(distinct movement.work_event_id)::integer into v_events
  from public.weekly_source_billing_movements movement
  where movement.candidate_id=(v_candidate->>'candidate_id')::uuid;
  -- Standing rule 5: no `limit`.  The work event is taken only after the
  -- exact cardinality proof on the line above, and PostgreSQL has no
  -- min(uuid), so the text form is aggregated and cast back (AGENTS.md).
  perform pg_temp.assert_true(v_events=1,
    'case '||p_tag||' must resolve to exactly one work event, got '||v_events);
  select pg_catalog.min(movement.work_event_id::text)::uuid into v_event
  from public.weekly_source_billing_movements movement
  where movement.candidate_id=(v_candidate->>'candidate_id')::uuid;

  v_family:=pg_temp.wp57_family(v_candidate,v_cycle,v_event,p_tag);
  v_ctx:=pg_temp.wp57_context(v_family,v_cycle,v_event,p_tag,'WAIT');
  begin
    perform pg_temp.wp57_context(v_family,v_cycle,v_event,p_tag,'RECORD_NOT_WORKED');
    v_guard:='ACCEPTED';
  exception when others then
    v_guard:=case when sqlerrm='WEEKLY_PROTECTED_NOT_WORKED_SOURCE_PRESENT'
      then 'REFUSED' else 'ERROR '||sqlstate||' '||sqlerrm end;
  end;
  return pg_catalog.jsonb_build_object(
    'tag',p_tag,'work_events',v_events,'position',pg_temp.wp57_position(v_ctx),
    'invoiced',pg_temp.wp57_invoiced(v_candidate),'not_worked_guard',v_guard);
end; $function$;

do $wp57_nhsp_protected_row_order$
declare
  v_pos8 constant jsonb:='{"date":"2026-09-07","start":"09:00","end":"17:00","charge":16000}';
  v_pos9 constant jsonb:='{"date":"2026-09-07","start":"09:00","end":"18:00","charge":18000}';
  v_neg8 constant jsonb:='{"date":"2026-09-07","start":"09:00","end":"17:00","charge":-16000}';
  v_live constant text:='present=true|source_minutes=540|approved_minutes=540';
  p1 jsonb; p2 jsonb; p3 jsonb; p4 jsonb; s1 jsonb; s2 jsonb;
begin
  -- P1 / P2.  A prior report establishes 8 h.  The correction arrives as ONE
  -- report carrying the full reversal and the 9 h re-issue, listed in OPPOSITE
  -- physical order.  14 s4.2.5.
  p1:=pg_temp.wp57_case('p1',pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_array(v_pos8),
    pg_catalog.jsonb_build_array(v_neg8,v_pos9)),'2029-01-06');
  p2:=pg_temp.wp57_case('p2',pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_array(v_pos8),
    pg_catalog.jsonb_build_array(v_pos9,v_neg8)),'2029-02-03');

  perform pg_temp.assert_true(
    (p1->>'work_events')::integer=1 and (p2->>'work_events')::integer=1,
    'a reversal and its re-issue must resolve to one work event in either row order');
  perform pg_temp.assert_true(p1->>'position'=v_live,
    'reversal listed FIRST must show the live 9 h source position, got '||(p1->>'position'));
  perform pg_temp.assert_true(p2->>'position'=v_live,
    'reversal listed SECOND must show the live 9 h source position, got '||(p2->>'position'));
  perform pg_temp.assert_true(p1->>'position'=p2->>'position',
    'the protected-shift source position must not depend on the physical row order: '
      ||(p1->>'position')||' vs '||(p2->>'position'));
  perform pg_temp.assert_true(
    (p1->>'invoiced')::numeric=180.00 and (p2->>'invoiced')::numeric=180.00,
    'both row orders must invoice the Client GBP 180.00, got '
      ||(p1->>'invoiced')||' and '||(p2->>'invoiced'));

  -- P3 / P4.  The same correction split across reports, with the reversal of
  -- the ORIGINAL line arriving last and first.
  p3:=pg_temp.wp57_case('p3',pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_array(v_pos8),
    pg_catalog.jsonb_build_array(v_pos9),
    pg_catalog.jsonb_build_array(v_neg8)),'2029-03-03');
  p4:=pg_temp.wp57_case('p4',pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_array(v_pos8),
    pg_catalog.jsonb_build_array(v_neg8),
    pg_catalog.jsonb_build_array(v_pos9)),'2029-04-07');

  perform pg_temp.assert_true(p3->>'position'=v_live,
    'a later reversal of the ORIGINAL 8 h line must not wipe the re-issued 9 h source '
      ||'position, got '||(p3->>'position'));
  perform pg_temp.assert_true(p4->>'position'=v_live,
    'the reversal arriving before the re-issue must show the live 9 h source position, got '
      ||(p4->>'position'));
  perform pg_temp.assert_true(
    p3->>'position'=p1->>'position' and p4->>'position'=p1->>'position',
    'splitting the same correction across reports must not change the source position: '
      ||(p3->>'position')||' / '||(p4->>'position')||' vs '||(p1->>'position'));
  perform pg_temp.assert_true(
    (p3->>'invoiced')::numeric=180.00 and (p4->>'invoiced')::numeric=180.00,
    'both split orders must invoice the Client GBP 180.00, got '
      ||(p3->>'invoiced')||' and '||(p4->>'invoiced'));

  -- SENTINELS that must DIFFER, or the permutation table proves nothing.
  s1:=pg_temp.wp57_case('s1',pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_array(v_pos8),
    pg_catalog.jsonb_build_array(v_neg8)),'2029-05-05');
  s2:=pg_temp.wp57_case('s2',pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_array(v_pos8)),'2029-06-02');

  perform pg_temp.assert_true(
    s1->>'position'='present=false|source_minutes=0|approved_minutes=0',
    'a reversal with no re-issue must leave no live source position, got '
      ||(s1->>'position'));
  perform pg_temp.assert_true((s1->>'invoiced')::numeric=0.00,
    'a reversal with no re-issue must leave the Client invoiced GBP 0.00, got '
      ||(s1->>'invoiced'));
  perform pg_temp.assert_true(s1->>'position'<>p1->>'position',
    'the reversed-and-never-re-issued sentinel must differ from the re-issued cases, '
      ||'or this proves nothing');
  perform pg_temp.assert_true(
    s2->>'position'='present=true|source_minutes=480|approved_minutes=480'
      and (s2->>'invoiced')::numeric=160.00,
    'an uncorrected 8 h shift must still show 480 approved minutes and GBP 160.00, got '
      ||(s2->>'position')||' / '||(s2->>'invoiced'));
  perform pg_temp.assert_true(s2->>'position'<>p1->>'position',
    'the uncorrected 8 h sentinel must differ from the corrected 9 h cases');

  -- The guard the defect disabled.  RECORD_NOT_WORKED must follow the source,
  -- never the row order: refused wherever a live source position exists,
  -- accepted only where the source genuinely says nothing.
  perform pg_temp.assert_true(
    p1->>'not_worked_guard'='REFUSED' and p2->>'not_worked_guard'='REFUSED'
      and p3->>'not_worked_guard'='REFUSED' and p4->>'not_worked_guard'='REFUSED'
      and s2->>'not_worked_guard'='REFUSED',
    'RECORD_NOT_WORKED must be refused for every permutation of a worked, re-issued '
      ||'shift, got '||(p1->>'not_worked_guard')||'/'||(p2->>'not_worked_guard')||'/'
      ||(p3->>'not_worked_guard')||'/'||(p4->>'not_worked_guard')||'/'
      ||(s2->>'not_worked_guard'));
  perform pg_temp.assert_true(s1->>'not_worked_guard'='ACCEPTED',
    'RECORD_NOT_WORKED must still be permitted where the source has reversed the shift '
      ||'and never re-issued it, got '||(s1->>'not_worked_guard'));

  -- STATIC assertion, labelled as such: the installed owner must still read
  -- the single live-position owner.  A later edit cannot quietly reintroduce a
  -- private ranking without this failing.
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      'public.weekly_exceptional_pay_action_context_v1(jsonb)'::regprocedure
    ) like '%weekly_source_ordinary_projection_active_movements_v1%',
    'STATIC: the protected-shift proposal owner must read '
      ||'private.weekly_source_ordinary_projection_active_movements_v1');
end
$wp57_nhsp_protected_row_order$;

rollback;
