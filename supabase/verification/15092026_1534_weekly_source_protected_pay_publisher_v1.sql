-- Rollback-only PostgreSQL 17 proof for the protected-pay preparation owner.
-- Prerequisites: Plan 6 schema, classifiers, settings/policy and this repeatable.

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
  'e1000000-0000-4000-8000-000000000001','protected-owner@example.test',
  'admin',true,'not-a-login',true
);
insert into public.tms_users(
  id,email,role,is_active,password_hash,payment_authoriser
) values (
  'e1000000-0000-4000-8000-000000000002','ordinary-admin@example.test',
  'admin',true,'not-a-login',false
);
insert into public.clients(id,name)
values ('e1000000-0000-4000-8000-000000000003','Protected Roster Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('e1000000-0000-4000-8000-000000000003',20,'2026-01-01');
insert into public.candidates(id,display_name)
values (
  'e1000000-0000-4000-8000-000000000004','Protected Candidate'
);
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'e1000000-0000-4000-8000-000000000005',
  'e1000000-0000-4000-8000-000000000004',
  'e1000000-0000-4000-8000-000000000003',
  '2026-01-01','2026-12-31','PAYE',
  '{"pay":{"day":10,"night":10,"sat":10,"sun":10,"bh":10},"charge":{"day":20,"night":20,"sat":20,"sun":20,"bh":20}}'::jsonb,
  'HEALTHROSTER',true,true,true,true
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time
) values (
  'e1000000-0000-4000-8000-000000000006','TEST',
  'e1000000-0000-4000-8000-000000000007','PROTECTED_ROSTER',
  'Protected Roster','ROSTER',3,'15:00'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  'e1000000-0000-4000-8000-000000000008',
  'e1000000-0000-4000-8000-000000000006',
  'e1000000-0000-4000-8000-000000000003','2026-01-01',
  'e1000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,self_bill_correction_presentation,
  source_fixed_expenses_enabled,source_expense_vat_enabled,
  weekly_rate_classification_method,created_by_user_id
) values (
  'e1000000-0000-4000-8000-000000000009',
  'e1000000-0000-4000-8000-000000000006',
  'e1000000-0000-4000-8000-000000000003','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'FULL_REVERSAL_REPLACEMENT',
  false,false,'SPLIT_RATE_WINDOWS',
  'e1000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,
  projection_state
) values (
  'e1000000-0000-4000-8000-00000000000a',
  'e1000000-0000-4000-8000-000000000006','2026-09-20',
  '2026-09-16T14:00:00Z','OPEN',1,'NONE'
);

create temp table before_finance as
select
  (select count(*) from public.timesheets_financials) as financials,
  (select count(*) from public.pay_batches) as pay_batches;

create temp table first_result as
select public.weekly_exceptional_pay_prepare_family_v1(
  pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e1000000-0000-4000-8000-00000000000a',
    'candidate_id','e1000000-0000-4000-8000-000000000004',
    'client_id','e1000000-0000-4000-8000-000000000003',
    'contract_id','e1000000-0000-4000-8000-000000000005',
    'week_ending_date','2026-09-13','work_date','2026-09-07',
    'start_at_local','2026-09-07 09:00:00',
    'end_at_local','2026-09-07 17:00:00','break_minutes',30,
    'reason','Candidate reports this shift was worked.',
    'idempotency_key','protected-family-test-0001'
  )
) as result;

select pg_temp.assert_true(
  (select (result->>'created_root')::boolean
            and (result->>'created_family')::boolean
            and (result->>'created_work_event')::boolean
            and (result->>'requires_zero_financial')::boolean
            and result->>'source_mode'='HEALTHROSTER_WEEKLY'
          from first_result),
  'source-absent protection must create one ordinary root, family and work event'
);
select pg_temp.assert_true(
  (select result->>'week_ending_date'='2026-09-13'
          and result->>'source_cycle_id'='e1000000-0000-4000-8000-00000000000a'
   from first_result),
  'a later source cycle must be allowed to protect an unresolved historic work week'
);
select pg_temp.assert_true(
  (select count(*)=1 from public.weekly_exceptional_pay_target_families
   where ownership_state='TARGET_MANAGED'
     and current_lifecycle_state='PENDING_APPROVAL'
     and current_generation_id is null),
  'preparation must establish one target-managed pending family without publishing entitlement'
);
select pg_temp.assert_true(
  (select count(*)=1 from public.contract_weeks
   where contract_id='e1000000-0000-4000-8000-000000000005'
     and week_ending_date='2026-09-13' and additional_seq=0
     and timesheet_id is not null),
  'preparation must use the ordinary base Contract Week'
);
select pg_temp.assert_true(
  (select count(*)=1 from public.timesheets
   where timesheet_id=(select (result->>'root_timesheet_id')::uuid from first_result)
     and sheet_scope='WEEKLY' and line_type='HOURS' and is_current
     and not is_adjustment and actual_schedule_json='[]'::jsonb),
  'the sole public root must remain an ordinary empty Weekly Timesheet'
);
select pg_temp.assert_true(
  (select count(*)=1 from public.weekly_work_events
   where identity_kind='OFFICE_PROTECTED_SHIFT'
     and candidate_id='e1000000-0000-4000-8000-000000000004'
     and client_id='e1000000-0000-4000-8000-000000000003'
     and work_date='2026-09-07'),
  'a source-absent protected shift must have one durable Office work event'
);
select pg_temp.assert_true(
  (select count(*)=1 from public.weekly_exceptional_orchestration_runs
   where request_kind='APPROVE' and state='RUNNING'),
  'preparation must create one service-owned orchestration run'
);
select pg_temp.assert_true(
  (select financials=(select count(*) from public.timesheets_financials)
          and pay_batches=(select count(*) from public.pay_batches)
   from before_finance),
  'family preparation must not create a financial snapshot, pay batch or Banking Pay row'
);

create temp table replay_result as
select public.weekly_exceptional_pay_prepare_family_v1(
  pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e1000000-0000-4000-8000-00000000000a',
    'candidate_id','e1000000-0000-4000-8000-000000000004',
    'client_id','e1000000-0000-4000-8000-000000000003',
    'contract_id','e1000000-0000-4000-8000-000000000005',
    'week_ending_date','2026-09-13','work_date','2026-09-07',
    'start_at_local','2026-09-07 09:00:00',
    'end_at_local','2026-09-07 17:00:00','break_minutes',30,
    'reason','Candidate reports this shift was worked.',
    'idempotency_key','protected-family-test-0001'
  )
) as result;
select pg_temp.assert_true(
  (select (result->>'idempotent_replay')::boolean
          and not (result->>'created_root')::boolean
          and not (result->>'created_family')::boolean
          and not (result->>'created_work_event')::boolean
   from replay_result),
  'an exact retry must return the original identities without duplicate writes'
);
select pg_temp.assert_true(
  (select count(*)=1 from public.weekly_exceptional_pay_target_families)
  and (select count(*)=1 from public.weekly_exceptional_orchestration_runs)
  and (select count(*)=1 from public.weekly_work_events),
  'an exact retry must leave every identity cardinality at one'
);

create temp table additional_result as
select public.weekly_exceptional_pay_prepare_family_v1(
  pg_catalog.jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e1000000-0000-4000-8000-00000000000a',
    'candidate_id','e1000000-0000-4000-8000-000000000004',
    'client_id','e1000000-0000-4000-8000-000000000003',
    'contract_id','e1000000-0000-4000-8000-000000000005',
    'week_ending_date','2026-09-13','work_date','2026-09-08',
    'start_at_local','2026-09-08 09:00:00',
    'end_at_local','2026-09-08 13:00:00','break_minutes',0,
    'reason','Office is protecting another shift in the same week.',
    'idempotency_key','protected-family-test-0002'
  )
) as result;
select pg_temp.assert_true(
  (select result->>'request_kind'='AMEND'
          and not (result->>'created_family')::boolean
          and (result->>'created_work_event')::boolean
   from additional_result)
  and (select count(*)=1 from public.weekly_exceptional_pay_target_families)
  and (select count(*)=2 from public.weekly_exceptional_orchestration_runs)
  and (select count(*)=2 from public.weekly_work_events),
  'a later protected shift must reuse the same complete weekly family'
);

do $non_authoriser_refused$
begin
  begin
    perform public.weekly_exceptional_pay_prepare_family_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id','e1000000-0000-4000-8000-000000000002',
        'source_cycle_id','e1000000-0000-4000-8000-00000000000a',
        'candidate_id','e1000000-0000-4000-8000-000000000004',
        'client_id','e1000000-0000-4000-8000-000000000003',
        'contract_id','e1000000-0000-4000-8000-000000000005',
        'week_ending_date','2026-09-13','work_date','2026-09-07',
        'start_at_local','2026-09-07 10:00:00',
        'end_at_local','2026-09-07 18:00:00','break_minutes',30,
        'reason','Must be refused.','idempotency_key','protected-family-test-0003'
      )
    );
    raise exception 'NON_AUTHORISER_WAS_ACCEPTED';
  exception when insufficient_privilege then
    if sqlerrm<>'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' then raise; end if;
  end;
end;
$non_authoriser_refused$;

select pg_temp.assert_true(
  not has_function_privilege('anon',
    'public.weekly_exceptional_pay_prepare_family_v1(jsonb)','EXECUTE')
  and not has_function_privilege('authenticated',
    'public.weekly_exceptional_pay_prepare_family_v1(jsonb)','EXECUTE')
  and has_function_privilege('service_role',
    'public.weekly_exceptional_pay_prepare_family_v1(jsonb)','EXECUTE'),
  'the preparation owner must remain service-only'
);

rollback;
