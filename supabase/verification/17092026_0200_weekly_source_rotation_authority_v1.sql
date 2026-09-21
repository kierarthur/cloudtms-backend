-- Rollback-only PostgreSQL 17 proof for the Plan 6.2 Gate 6 rotation authority.
--
-- Covers, in order:
--   1. the Candidate serial gate mapping (proof/32 section 6 step 1; R42
--      GRANTED and BYPASSED; BLOCKED needs two sessions and is proved by the
--      two-session harness recorded in the WP-03 report);
--   2. interface I-1 resolution facts for a canonical and a non-canonical
--      request, in input order, with the complete member set;
--   3. every I-1 integrity failure: no roots, unknown root, a blank booking
--      identity the installed resolver silently drops (WP-08a review F1), a
--      whitespace-split family (WP-08a review F3), a root belonging to another
--      Candidate, and a family with no current row (ROT-009);
--   4. the real lineage owner under decision D8: the binding record and its
--      idempotent replay, that a bound but never-authorised root is NOT
--      managed, the guard against a real public.weekly_source_root_authorisations
--      row, the withdrawn and contradictory cases, a rotation of an authorised
--      root through the REAL installed rotation owner, the F5 refusal and the
--      proof/34 section 6 integrity failure (G6-12) from both asserts;
--   5. the managed-root guard (G6-10, proof/34 section 3 "Guard owner"):
--      managed, not managed, withdrawn-is-not-managed, and the fail-closed
--      direction for an unresolvable family;
--   6. owner, security and ACL for every function this package adds.
--
-- Prerequisites: supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql,
-- supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql,
-- supabase/repeatable/17092026_0200_weekly_source_rotation_authority_v1.sql,
-- supabase/repeatable/15092026_1534_weekly_source_timesheet_lineage_v1.sql and
-- supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql.
--
-- Nothing here calls a Banking Pay, Draft, execution, cancellation, settlement,
-- provider, recovery or remittance owner, and nothing writes outside the
-- rolled-back transaction.

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

-- Run a statement and require the exact SQLSTATE and message text.
create function pg_temp.assert_refused(
  p_sql text,p_message_like text,p_label text
) returns void language plpgsql as $function$
declare
  v_message text;
begin
  begin
    execute p_sql;
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message not like p_message_like then
      raise exception 'ASSERTION_FAILED: % refused with "%" not "%"',
        p_label,v_message,p_message_like;
    end if;
    return;
  end;
  raise exception 'ASSERTION_FAILED: % was accepted',p_label;
end;
$function$;

-- ---------------------------------------------------------------------------
-- Fixture: one agency, one Client, two Candidates and their Contracts.
-- ---------------------------------------------------------------------------

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')
) on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256,
  candidate_home_announcement_sha256=excluded.candidate_home_announcement_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('a3000000-0000-4000-8000-000000000001','plan62-rotation@example.test','admin',true,'not-a-login');
insert into public.clients(id,name)
values ('a3000000-0000-4000-8000-000000000002','Plan 6.2 Rotation Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('a3000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name)
values ('a3000000-0000-4000-8000-000000000003','Plan 6.2 Rotation Candidate');
insert into public.candidates(id,display_name)
values ('a3000000-0000-4000-8000-00000000000f','Plan 6.2 Other Candidate');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'a3000000-0000-4000-8000-000000000004',
  'a3000000-0000-4000-8000-000000000003',
  'a3000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true
);
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'a3000000-0000-4000-8000-000000000010',
  'a3000000-0000-4000-8000-00000000000f',
  'a3000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true
);

-- A helper that builds an ordinary Weekly HOURS Timesheet row directly. These
-- rows are identity fixtures for the resolver; no Weekly Source owner created
-- them and none of them carries financial state.
create function pg_temp.seed_timesheet(
  p_timesheet_id uuid,p_booking_id text,p_version integer,p_is_current boolean,
  p_contract_id uuid
) returns uuid language sql as $function$
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
    line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
    qr_payload_json,is_adjustment,created_at,updated_at
  ) values (
    p_timesheet_id,p_booking_id,p_version,p_is_current,
    'RECEIVED'::public.timesheet_status_enum,
    'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
    'HOURS'::public.timesheet_line_type_enum,'rotation-occupant','rotation-hospital',
    'rotation-ward','rotation-role','weekly-0','2026-09-13',p_contract_id,
    '[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) returning timesheet_id;
$function$;

-- Family ONE: two versions, version 2 current. The ordinary rotated family.
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000101','WP03-BK-ONE',1,false,
  'a3000000-0000-4000-8000-000000000004');
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000102','WP03-BK-ONE',2,true,
  'a3000000-0000-4000-8000-000000000004');

-- Family PAD: one current version whose booking id carries surrounding
-- whitespace and has no trimmed sibling. This is the R37 shape: it must resolve
-- and lock normally (trimmed key first, then the raw key because it differs).
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000103','  WP03-BK-PAD  ',1,true,
  'a3000000-0000-4000-8000-000000000004');

-- Family BLANK: an existing Timesheet whose booking identity is whitespace
-- only. The installed resolver returns NO row for it (WP-08a review F1).
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000104','   ',1,true,
  'a3000000-0000-4000-8000-000000000004');

-- Family SPLIT: two raw booking values with the same trimmed value, each
-- current in its own resolver family (WP-08a review F3).
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000105','WP03-BK-SPLIT',1,true,
  'a3000000-0000-4000-8000-000000000004');
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000106',' WP03-BK-SPLIT',1,true,
  'a3000000-0000-4000-8000-000000000004');

-- Family NOCURRENT: every version demoted (ROT-009).
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000107','WP03-BK-NOCURRENT',1,false,
  'a3000000-0000-4000-8000-000000000004');
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000108','WP03-BK-NOCURRENT',2,false,
  'a3000000-0000-4000-8000-000000000004');

-- Family OTHER: belongs to the second Candidate.
select pg_temp.seed_timesheet(
  'a3000000-0000-4000-8000-000000000109','WP03-BK-OTHER',1,true,
  'a3000000-0000-4000-8000-000000000010');

-- ---------------------------------------------------------------------------
-- 1. Candidate serial gate mapping (proof/32 section 6 step 1; R42)
-- ---------------------------------------------------------------------------

-- The installed unique indexes make two current rows, and a duplicated version,
-- structurally impossible inside ONE raw booking family. Recorded here so the
-- ROT-009 coverage below is read correctly: the reachable ambiguity is the
-- whitespace split, not a duplicate current row.
select pg_temp.assert_true(
  exists(select 1 from pg_catalog.pg_indexes
         where schemaname='public' and tablename='timesheets'
           and indexname='timesheets_booking_id_current_uidx')
  and exists(select 1 from pg_catalog.pg_indexes
             where schemaname='public' and tablename='timesheets'
               and indexname='timesheets_booking_id_version_uidx'),
  'the installed one-current-row and unique-version indexes must still exist'
);

-- R42, BLOCKED. Creating the Candidate's Contract above fired the installed
-- Workbench dirty trigger, which queued a real CONTRACT_CLIENT_DIRTY_FANOUT job
-- for this Candidate. The installed gate therefore blocks, and the Weekly Source
-- helper must map that to a RETRYABLE WEEKLY_SOURCE_CANDIDATE_BUSY with no
-- write, exactly as proof/32 section 6 step 1 requires.
select pg_temp.assert_true(
  (select count(*)>0
   from public.banking_pay_workbench_jobs job
   where pg_catalog.upper(pg_catalog.btrim(coalesce(job.status,'')))in('QUEUED','RUNNING')
     and job.job_type='CONTRACT_CLIENT_DIRTY_FANOUT'),
  'the installed Workbench trigger must have queued a Candidate job for the fixture'
);

select pg_temp.assert_true(
  (select gate->>'code'='WEEKLY_SOURCE_CANDIDATE_BUSY'
      and gate->>'gate'='BLOCKED'
      and (gate->>'retryable')::boolean
      and (gate->>'ok')::boolean is false
   from (select private.weekly_source_candidate_serial_gate_v1(
     'a3000000-0000-4000-8000-000000000003',
     'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
     'a3000000-0000-4000-8000-0000000000a0','proof') as gate) probe),
  'R42: a genuinely busy Candidate must be a retryable WEEKLY_SOURCE_CANDIDATE_BUSY'
);

select pg_temp.assert_true(
  (select (result->>'ok')::boolean is false
      and result->>'code'='WEEKLY_SOURCE_CANDIDATE_BUSY'
      and result->'families' is null
   from (select private.weekly_source_lock_and_resolve_families_v1(
     'a3000000-0000-4000-8000-000000000003',
     array['a3000000-0000-4000-8000-000000000102']::uuid[],
     'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
     'a3000000-0000-4000-8000-0000000000a0','proof') as result) probe),
  'I-1 must stop at the gate and resolve nothing when the Candidate is busy'
);

-- Fixture-only, inside this rolled-back transaction: clear the queued fanout
-- jobs the fixture inserts produced so the remaining proofs see a clear
-- Candidate. No Banking Pay owner is called and no Banking Pay logic changes.
delete from public.banking_pay_workbench_jobs
where pg_catalog.upper(pg_catalog.btrim(coalesce(status,'')))in('QUEUED','RUNNING');

select pg_temp.assert_true(
  (private.weekly_source_candidate_serial_gate_v1(
     'a3000000-0000-4000-8000-000000000003',
     'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
     'a3000000-0000-4000-8000-0000000000a1','proof')->>'gate')='GRANTED'
  and (private.weekly_source_candidate_serial_gate_v1(
     'a3000000-0000-4000-8000-000000000003',
     'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
     'a3000000-0000-4000-8000-0000000000a2','proof')->>'gate')='GRANTED'
  and (private.weekly_source_candidate_serial_gate_v1(
     'a3000000-0000-4000-8000-000000000003',
     'WORKBENCH_CANDIDATE_PENDING_ENTITLEMENT_RELEASE',
     'a3000000-0000-4000-8000-0000000000a3','proof')->>'gate')='GRANTED'
  and (private.weekly_source_candidate_serial_gate_v1(
     'a3000000-0000-4000-8000-000000000003',
     'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_WITHDRAWAL',
     'a3000000-0000-4000-8000-0000000000a4','proof')->>'gate')='GRANTED',
  'R42: each of the four pinned Weekly Source job types must be GRANTED'
);

select pg_temp.assert_true(
  (select gate->>'code'='WEEKLY_SOURCE_SERIAL_GATE_BYPASSED'
     and gate->>'reason'='JOB_TYPE_NOT_PINNED'
     and (gate->>'retryable')::boolean is false
     and (gate->>'ok')::boolean is false
   from (select private.weekly_source_candidate_serial_gate_v1(
     'a3000000-0000-4000-8000-000000000003','WORKBENCH_CANDIDATE_DIRTY_APPLY',
     'a3000000-0000-4000-8000-0000000000a5','proof') as gate) probe),
  'R42: an unpinned job type must be refused as BYPASSED and never retryable'
);

select pg_temp.assert_true(
  (select gate->>'code'='WEEKLY_SOURCE_SERIAL_GATE_BYPASSED'
     and gate->>'reason'='CANDIDATE_REQUIRED'
   from (select private.weekly_source_candidate_serial_gate_v1(
     null,'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
     'a3000000-0000-4000-8000-0000000000a6','proof') as gate) probe),
  'R42: a null Candidate must be refused as BYPASSED, never silently allowed'
);

-- The installed helper itself would BYPASS the unpinned type, which is exactly
-- why the Weekly Source helper refuses it before consulting the gate.
select pg_temp.assert_true(
  (public._pay_workbench_candidate_serial_try_gate(
     p_job_id:='a3000000-0000-4000-8000-0000000000a7',
     p_candidate_id:='a3000000-0000-4000-8000-000000000003',
     p_job_type:='WORKBENCH_SESSION_SCOPE_SEED',
     p_payload_json:='{}'::jsonb,
     p_reason:='proof')->>'candidate_serial_gate_decision')='BYPASSED',
  'the installed gate returns BYPASSED for a job type outside its Candidate rule'
);

-- ---------------------------------------------------------------------------
-- 2. Interface I-1 resolution facts
-- ---------------------------------------------------------------------------

create temp table wp03_lock_one as
select private.weekly_source_lock_and_resolve_families_v1(
  'a3000000-0000-4000-8000-000000000003',
  array['a3000000-0000-4000-8000-000000000102']::uuid[],
  'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
  'a3000000-0000-4000-8000-0000000000b1','proof'
) as result;

select pg_temp.assert_true(
  (select (result->>'ok')::boolean
      and result->>'gate'='GRANTED'
      and pg_catalog.jsonb_array_length(result->'families')=1
      and result#>>'{families,0,requested_timesheet_id}'='a3000000-0000-4000-8000-000000000102'
      and result#>>'{families,0,family_booking_id}'='WP03-BK-ONE'
      and result#>>'{families,0,canonical_timesheet_id}'='a3000000-0000-4000-8000-000000000102'
      and (result#>>'{families,0,canonical_version}')::integer=2
      and (result#>>'{families,0,requested_is_canonical}')::boolean
      and (result#>>'{families,0,family_is_current}')::boolean
      and pg_catalog.jsonb_array_length(result#>'{families,0,member_timesheet_ids}')=2
   from wp03_lock_one),
  'I-1 must return the canonical id, its version and the complete member set'
);

-- The helper reports facts; it never decides between "stale, rebuild" and
-- "integrity failure" for a non-canonical request.
select pg_temp.assert_true(
  (select (result->>'ok')::boolean
      and (result#>>'{families,0,requested_is_canonical}')::boolean is false
      and (result#>>'{families,0,family_is_current}')::boolean is false
      and result#>>'{families,0,canonical_timesheet_id}'='a3000000-0000-4000-8000-000000000102'
      and (result#>>'{families,0,canonical_version}')::integer=2
   from (select private.weekly_source_lock_and_resolve_families_v1(
     'a3000000-0000-4000-8000-000000000003',
     array['a3000000-0000-4000-8000-000000000101']::uuid[],
     'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
     'a3000000-0000-4000-8000-0000000000b2','proof') as result) probe),
  'I-1 must report a non-canonical request as a fact, with ok true'
);

-- Input order is preserved, and a whitespace-padded booking with no trimmed
-- sibling resolves and locks normally (R37 shape, single session).
select pg_temp.assert_true(
  (select (result->>'ok')::boolean
      and pg_catalog.jsonb_array_length(result->'families')=2
      and result#>>'{families,0,requested_timesheet_id}'='a3000000-0000-4000-8000-000000000103'
      and result#>>'{families,0,family_booking_id}'='  WP03-BK-PAD  '
      and result#>>'{families,1,requested_timesheet_id}'='a3000000-0000-4000-8000-000000000102'
   from (select private.weekly_source_lock_and_resolve_families_v1(
     'a3000000-0000-4000-8000-000000000003',
     array['a3000000-0000-4000-8000-000000000103',
           'a3000000-0000-4000-8000-000000000102']::uuid[],
     'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
     'a3000000-0000-4000-8000-0000000000b3','proof') as result) probe),
  'I-1 must keep input order and resolve a whitespace-padded booking family'
);

-- ---------------------------------------------------------------------------
-- 3. Every I-1 integrity failure
-- ---------------------------------------------------------------------------

create function pg_temp.lock_reason(p_ids uuid[],p_candidate_id uuid)
returns text language sql as $function$
  select coalesce(
    private.weekly_source_lock_and_resolve_families_v1(
      p_candidate_id,p_ids,'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
      'a3000000-0000-4000-8000-0000000000c0','proof'
    )->>'reason',
    'NO_REASON'
  );
$function$;

create function pg_temp.lock_code(p_ids uuid[],p_candidate_id uuid)
returns text language sql as $function$
  select coalesce(
    private.weekly_source_lock_and_resolve_families_v1(
      p_candidate_id,p_ids,'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
      'a3000000-0000-4000-8000-0000000000c0','proof'
    )->>'code',
    'NO_CODE'
  );
$function$;

select pg_temp.assert_true(
  pg_temp.lock_reason(array[]::uuid[],'a3000000-0000-4000-8000-000000000003')
    ='NO_REQUESTED_ROOTS'
  and pg_temp.lock_reason(null,'a3000000-0000-4000-8000-000000000003')
    ='NO_REQUESTED_ROOTS'
  and pg_temp.lock_code(array[]::uuid[],'a3000000-0000-4000-8000-000000000003')
    ='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
  'I-1 must refuse a null or empty root array'
);

select pg_temp.assert_true(
  pg_temp.lock_reason(
    array['a3000000-0000-4000-8000-0000000000ff']::uuid[],
    'a3000000-0000-4000-8000-000000000003')='BOOKING_IDENTITY_MISSING',
  'I-1 must refuse a root id that names no Timesheet'
);

-- WP-08a review F1: the installed resolver returns no row at all for this one,
-- so a guard that inspects only returned rows would never fire.
select pg_temp.assert_true(
  (select count(*)=0 from public._pay_timesheet_rotation_scope(
     array['a3000000-0000-4000-8000-000000000104']::uuid[])),
  'F1 precondition: the installed resolver drops a blank-booking Timesheet'
);
select pg_temp.assert_true(
  pg_temp.lock_reason(
    array['a3000000-0000-4000-8000-000000000104']::uuid[],
    'a3000000-0000-4000-8000-000000000003')='BOOKING_IDENTITY_MISSING',
  'F1: a blank booking identity on an existing Timesheet must fail closed'
);
select pg_temp.assert_true(
  pg_temp.lock_reason(
    array['a3000000-0000-4000-8000-000000000102',
          'a3000000-0000-4000-8000-000000000104']::uuid[],
    'a3000000-0000-4000-8000-000000000003')='BOOKING_IDENTITY_MISSING',
  'F1: one dropped member must refuse the whole request, never return ok'
);

-- WP-08a review F3: two raw booking values sharing one trimmed value.
select pg_temp.assert_true(
  pg_temp.lock_reason(
    array['a3000000-0000-4000-8000-000000000105']::uuid[],
    'a3000000-0000-4000-8000-000000000003')='BOOKING_REFERENCE_CANONICAL_COLLISION'
  and pg_temp.lock_reason(
    array['a3000000-0000-4000-8000-000000000106']::uuid[],
    'a3000000-0000-4000-8000-000000000003')='BOOKING_REFERENCE_CANONICAL_COLLISION',
  'F3: a whitespace-split family is ambiguous and must fail closed either way'
);

select pg_temp.assert_true(
  pg_temp.lock_reason(
    array['a3000000-0000-4000-8000-000000000109']::uuid[],
    'a3000000-0000-4000-8000-000000000003')='ROOT_CANDIDATE_MISMATCH',
  'I-1 must refuse a root that belongs to another Candidate'
);

-- ROT-009: a family with no current row at all.
select pg_temp.assert_true(
  pg_temp.lock_reason(
    array['a3000000-0000-4000-8000-000000000107']::uuid[],
    'a3000000-0000-4000-8000-000000000003')='CANONICAL_AMBIGUOUS',
  'ROT-009: a family with zero current rows must fail closed'
);

-- The read-only resolver reaches the same verdicts without taking a lock.
select pg_temp.assert_true(
  (private.weekly_source_resolve_root_identity_v1(
     'a3000000-0000-4000-8000-000000000104')->>'reason')='BOOKING_IDENTITY_MISSING'
  and (private.weekly_source_resolve_root_identity_v1(
     'a3000000-0000-4000-8000-000000000105')->>'reason')='BOOKING_REFERENCE_CANONICAL_COLLISION'
  and (private.weekly_source_resolve_root_identity_v1(
     'a3000000-0000-4000-8000-000000000107')->>'reason')='CANONICAL_AMBIGUOUS'
  and (private.weekly_source_resolve_root_identity_v1(null)->>'reason')='ROOT_ID_REQUIRED'
  and (private.weekly_source_resolve_root_identity_v1(
     'a3000000-0000-4000-8000-000000000102')->>'ok')::boolean,
  'the read-only resolver must reach the same verdicts as I-1'
);

-- ---------------------------------------------------------------------------
-- 5. The managed-root guard, on families with no Weekly Source binding
-- ---------------------------------------------------------------------------

select pg_temp.assert_true(
  (select (guard->>'ok')::boolean
      and (guard->>'managed')::boolean is false
      and guard->>'family_booking_id'='WP03-BK-ONE'
      and (guard->>'canonical_version')::integer=2
      and guard->>'refusal_code' is null
      and guard->'authorisation'='null'::jsonb
   from (select private.weekly_source_managed_root_guard_v1(
     'a3000000-0000-4000-8000-000000000102') as guard) probe),
  'an unauthorised ordinary family must not be managed'
);

select pg_temp.assert_true(
  (select (guard->>'ok')::boolean is false
      and (guard->>'managed')::boolean
      and guard->>'refusal_code'='WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
      and guard->>'reason'='BOOKING_IDENTITY_MISSING'
   from (select private.weekly_source_managed_root_guard_v1(
     'a3000000-0000-4000-8000-000000000104') as guard) probe),
  'ROT-009/ROT-012: an unresolvable family must fail closed in the guard'
);

select pg_temp.assert_true(
  (select (guard->>'managed')::boolean
      and guard->>'reason'='BOOKING_REFERENCE_CANONICAL_COLLISION'
   from (select private.weekly_source_managed_root_guard_v1(
     'a3000000-0000-4000-8000-000000000105') as guard) probe)
  and (select (guard->>'managed')::boolean
      and guard->>'reason'='CANONICAL_AMBIGUOUS'
   from (select private.weekly_source_managed_root_guard_v1(
     'a3000000-0000-4000-8000-000000000107') as guard) probe),
  'ROT-012: an ambiguous family must fail closed in the guard'
);

-- ---------------------------------------------------------------------------
-- 4. The real lineage owner, on a real published projection
-- ---------------------------------------------------------------------------

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  'a3000000-0000-4000-8000-000000000005','TEST',
  'a3000000-0000-4000-8000-000000000006','PLAN62_ROTATION','Plan 6.2 Rotation','ROSTER',3,'15:00'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  'a3000000-0000-4000-8000-000000000007',
  'a3000000-0000-4000-8000-000000000005',
  'a3000000-0000-4000-8000-000000000002','2026-01-01',
  'a3000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,weekly_rate_classification_method,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  'a3000000-0000-4000-8000-000000000012',
  'a3000000-0000-4000-8000-000000000005',
  'a3000000-0000-4000-8000-000000000002','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'SPLIT_RATE_WINDOWS',true,
  'manager@example.test','a3000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'a3000000-0000-4000-8000-000000000008',
  'a3000000-0000-4000-8000-000000000005','2026-09-13','2026-09-09T14:00:00Z',
  'OPEN',1,'REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_json,header_coordinate_map_hash,
  declared_scope_fingerprint,suggested_coverage_start_local_date,suggested_coverage_end_local_date,
  confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,coverage_timezone,
  coverage_confirmation_version,coverage_confirmed_by_user_id,coverage_confirmed_at_utc,
  coverage_shrink_acknowledged,coverage_state,coverage_proof_kind,physical_row_count,
  accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'a3000000-0000-4000-8000-000000000009',
  'a3000000-0000-4000-8000-000000000008','rotation.csv',decode(repeat('01',32),'hex'),100,
  '35555555-5555-4555-8555-555555555555','PARSER_V1','NORMALISER_V1','{}',decode(repeat('02',32),'hex'),
  decode(repeat('03',32),'hex'),'2026-09-07','2026-09-09','2026-09-07','2026-09-09','Europe/London',
  'COMPLETE_EXPORT_V1','a3000000-0000-4000-8000-000000000001',clock_timestamp(),false,'COMPLETE',
  'OFFICE_COMPLETE_EXPORT_ATTESTATION',3,3,decode(repeat('04',32),'hex'),'CURRENT',
  'a3000000-0000-4000-8000-000000000001'
);
update public.weekly_source_cycles
set current_complete_upload_id='a3000000-0000-4000-8000-000000000009'
where id='a3000000-0000-4000-8000-000000000008';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,source_expense_pence,
  source_expense_parse_state,normalised_row_hash
) values
(
  'a3000000-0000-4000-8000-000000000021',
  'a3000000-0000-4000-8000-000000000009',1,'rot-1','Plan 6.2 Rotation Candidate',
  'Plan 6.2 Rotation Client','2026-09-07','2026-09-07 09:00','2026-09-07 17:00',30,450,
  'NOT_APPLICABLE',0,'OMITTED_ZERO',decode(repeat('05',32),'hex')
),(
  'a3000000-0000-4000-8000-000000000022',
  'a3000000-0000-4000-8000-000000000009',2,'rot-2','Plan 6.2 Rotation Candidate',
  'Plan 6.2 Rotation Client','2026-09-08','2026-09-08 09:00','2026-09-08 17:00',30,450,
  'NOT_APPLICABLE',0,'OMITTED_ZERO',decode(repeat('0a',32),'hex')
),(
  'a3000000-0000-4000-8000-000000000023',
  'a3000000-0000-4000-8000-000000000009',3,'rot-3','Plan 6.2 Rotation Candidate',
  'Plan 6.2 Rotation Client','2026-09-09','2026-09-09 09:00','2026-09-09 17:00',30,450,
  'NOT_APPLICABLE',0,'OMITTED_ZERO',decode(repeat('0b',32),'hex')
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state
) values (
  'a3000000-0000-4000-8000-000000000011',
  'a3000000-0000-4000-8000-000000000008','CYCLE',
  'a3000000-0000-4000-8000-000000000009',1,
  decode(repeat('06',32),'hex'),decode(repeat('07',32),'hex'),'BUILDING'
);

select public.weekly_source_projection_rows_apply_atomic_v1(
  'a3000000-0000-4000-8000-000000000001',
  'a3000000-0000-4000-8000-000000000011',
  jsonb_build_array(
    jsonb_build_object(
      'upload_row_id','a3000000-0000-4000-8000-000000000021',
      'mapping_state','RESOLVED',
      'candidate_id','a3000000-0000-4000-8000-000000000003',
      'client_id','a3000000-0000-4000-8000-000000000002',
      'contract_id','a3000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',jsonb_build_array('a3000000-0000-4000-8000-000000000004'),
      'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key','rot-1',
      'link_kind','POSITIVE_SOURCE',
      'economic_snapshot',jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','HEALTHROSTER_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
        'sign',1,'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',jsonb_build_object('day',450,'night',0,'sat',0,'sun',0,'bh',0),
        'hours',jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0),
        'pay_rates',jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
        'charge_rates',jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
        'total_pay_pence','7500','calculated_charge_pence','15000'
      )
    ),
    jsonb_build_object(
      'upload_row_id','a3000000-0000-4000-8000-000000000022',
      'mapping_state','RESOLVED',
      'candidate_id','a3000000-0000-4000-8000-000000000003',
      'client_id','a3000000-0000-4000-8000-000000000002',
      'contract_id','a3000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',jsonb_build_array('a3000000-0000-4000-8000-000000000004'),
      'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key','rot-2',
      'link_kind','POSITIVE_SOURCE',
      'economic_snapshot',jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','HEALTHROSTER_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
        'sign',1,'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',jsonb_build_object('day',450,'night',0,'sat',0,'sun',0,'bh',0),
        'hours',jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0),
        'pay_rates',jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
        'charge_rates',jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
        'total_pay_pence','7500','calculated_charge_pence','15000'
      )
    ),
    jsonb_build_object(
      'upload_row_id','a3000000-0000-4000-8000-000000000023',
      'mapping_state','RESOLVED',
      'candidate_id','a3000000-0000-4000-8000-000000000003',
      'client_id','a3000000-0000-4000-8000-000000000002',
      'contract_id','a3000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',jsonb_build_array('a3000000-0000-4000-8000-000000000004'),
      'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key','rot-3',
      'link_kind','POSITIVE_SOURCE',
      'economic_snapshot',jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','HEALTHROSTER_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
        'sign',1,'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',jsonb_build_object('day',450,'night',0,'sat',0,'sun',0,'bh',0),
        'hours',jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0),
        'pay_rates',jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
        'charge_rates',jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
        'total_pay_pence','7500','calculated_charge_pence','15000'
      )
    )
  )
);

update public.weekly_source_projection_publications
set state='CURRENT',published_at_utc=clock_timestamp()
where id='a3000000-0000-4000-8000-000000000011';
update public.weekly_source_cycles
set projection_state='CURRENT',
    current_projection_publication_id='a3000000-0000-4000-8000-000000000011'
where id='a3000000-0000-4000-8000-000000000008';

-- Binding of the source root.  Decision D8: this is a BINDING, not an
-- authorisation (review F14: the previous label was wrong).  The Timesheet is
-- still RECEIVED and nothing has authorised it.
select public.weekly_source_timesheet_lineage_ensure_atomic_v1(
  (select id from public.weekly_source_row_resolutions
   where upload_row_id='a3000000-0000-4000-8000-000000000021'),
  'a3000000-0000-4000-8000-000000000001'
);

create temp table wp03_root as
select lineage.*
from public.weekly_source_row_timesheet_lineages lineage
where lineage.row_resolution_id=(
  select id from public.weekly_source_row_resolutions
  where upload_row_id='a3000000-0000-4000-8000-000000000021'
);

select pg_temp.assert_true(
  (select count(*)=1 from wp03_root)
  and (select lineage.family_booking_id=timesheet.booking_id
         and lineage.timesheet_version=timesheet.version
         and timesheet.is_current
         and timesheet.status='RECEIVED'::public.timesheet_status_enum
         and timesheet.authorised_at_server is null
       from wp03_root lineage
       join public.timesheets timesheet
         on timesheet.timesheet_id=lineage.timesheet_id),
  'D8: the binding records the binding-time family identity and version only'
);

-- D8: the ensure owner writes no authorisation record of any kind.
select pg_temp.assert_true(
  (select count(*)=0 from public.weekly_source_root_authorisations),
  'D8/F3: the ensure owner must write no root authorisation'
);

-- Review F3: a bound but never-authorised root is NOT managed, so first
-- authorisation is never blocked.
select pg_temp.assert_true(
  (select (guard->>'ok')::boolean
      and (guard->>'managed')::boolean is false
      and guard->>'refusal_code' is null
      and (guard->>'timesheet_currently_authorised')::boolean is false
      and guard->'authorisation'='null'::jsonb
   from (select private.weekly_source_managed_root_guard_v1(
     (select timesheet_id from wp03_root)) as guard) probe),
  'F3: a bound, never-authorised root must not be managed'
);

-- Idempotent replay: the same call adds no binding.
select public.weekly_source_timesheet_lineage_ensure_atomic_v1(
  (select id from public.weekly_source_row_resolutions
   where upload_row_id='a3000000-0000-4000-8000-000000000021'),
  'a3000000-0000-4000-8000-000000000001'
);
select pg_temp.assert_true(
  (select count(*)=1 from public.weekly_source_row_timesheet_lineages lineage
   where lineage.row_resolution_id=(select row_resolution_id from wp03_root)),
  'D8: an exact replay must not add a second binding'
);

-- The second worked row in the same Contract week reuses the same root.
select public.weekly_source_timesheet_lineage_ensure_atomic_v1(
  (select id from public.weekly_source_row_resolutions
   where upload_row_id='a3000000-0000-4000-8000-000000000022'),
  'a3000000-0000-4000-8000-000000000001'
);
select pg_temp.assert_true(
  (select count(distinct lineage.timesheet_id)=1
     and count(*)=2
   from public.weekly_source_row_timesheet_lineages lineage),
  'the two bound worked rows must share the one ordinary base Weekly Timesheet'
);

-- ---------------------------------------------------------------------------
-- 4a-bis. Review G3: a PERMITTED rotation before first authorisation is the
-- proof/34 section 5 step 3 STALE refusal, never the section 6 integrity
-- failure, and it must leave the source row rebuildable.
-- ---------------------------------------------------------------------------
--
-- At this point the root is BOUND and has no authorisation record at all. The
-- whole block runs in a plpgsql subtransaction that is unwound at the end, so
-- the rotation it performs does not leak into the cases below.
do $g3_stale_before_authorisation$
declare
  v_result jsonb;
  v_message text;
  v_detail text;
  v_seen text;
begin
  begin
    if (select count(*) from public.weekly_source_root_authorisations)<>0 then
      raise exception 'ASSERTION_FAILED: G3 needs a root with no authorisation record';
    end if;

    v_result:=private._timesheet_route_version_legacy_v1(
      (select timesheet_id from wp03_root),(select timesheet_id from wp03_root),
      'ALLOW_QR_AGAIN','a3000000-0000-4000-8000-000000000001',true
    );
    if nullif(v_result->>'new_timesheet_id','') is null then
      raise exception 'ASSERTION_FAILED: G3 fixture rotation did not rotate: %',v_result;
    end if;

    -- The installed owner re-points contract_weeks.timesheet_id, so the ensure
    -- owner now discovers the NEW root while the immutable binding still names
    -- the old one.
    begin
      perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
        (select row_resolution_id from wp03_root),
        'a3000000-0000-4000-8000-000000000001'
      );
      v_seen:='ACCEPTED';
    exception when others then
      get stacked diagnostics v_message=message_text,v_detail=pg_exception_detail;
      v_seen:=v_message;
    end;

    if v_seen is distinct from 'WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION' then
      raise exception 'ASSERTION_FAILED: G3 expected the section 5 step 3 stale refusal, got %',
        v_seen;
    end if;
    if coalesce(v_detail,'') not like '%REBUILD_RESOLUTION_AT_A_NEW_GENERATION%' then
      raise exception 'ASSERTION_FAILED: G3 refusal must say how to recover, got %',v_detail;
    end if;

    raise exception 'WP03_G3_UNWIND';
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message is distinct from 'WP03_G3_UNWIND' then
      raise;
    end if;
  end;
end;
$g3_stale_before_authorisation$;

-- Review G8: when the authorisation relation cannot be read at all, the ensure
-- owner must fail CLOSED, the same direction as the guard, not bind.
do $g8_relation_absent$
declare
  v_message text;
  v_seen text;
begin
  begin
    alter table public.weekly_source_root_authorisations
      rename to weekly_source_root_authorisations_wp03_g8;
    begin
      perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
        (select id from public.weekly_source_row_resolutions
         where upload_row_id='a3000000-0000-4000-8000-000000000023'),
        'a3000000-0000-4000-8000-000000000001'
      );
      v_seen:='ACCEPTED';
    exception when others then
      get stacked diagnostics v_message=message_text;
      v_seen:=v_message;
    end;
    if v_seen is distinct from 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' then
      raise exception 'ASSERTION_FAILED: G8 the ensure owner must fail closed when the authorisation relation is absent, got %',
        v_seen;
    end if;
    raise exception 'WP03_G8_UNWIND';
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message is distinct from 'WP03_G8_UNWIND' then
      raise;
    end if;
  end;
end;
$g8_relation_absent$;

-- ---------------------------------------------------------------------------
-- 4b. The guard against a real root authorisation (decision D8)
-- ---------------------------------------------------------------------------

-- Stand in for interface I-6 (WP-07): the first-authorisation owner authorises
-- the Timesheet through the ordinary owner and then writes the root
-- authorisation.  WP-03 owns neither, so the fixture below performs the two
-- effects directly and says so; it proves the GUARD's rule, not I-6.
-- WP-24 (Gate 13 finding F1).  THIS WRITE IS PERMITTED, AND THAT IS THE POINT.
-- `wp03_root` is Weekly-Source BOUND here - the lineage ensure owner bound it
-- above - but it carries NO live authorisation generation yet, so it is not
-- MANAGED.  Ruling B3 requires that such a root keep exactly the behaviour it
-- had before this feature existed, and it does: the authorisation-boundary
-- guard WP-24 attached to `public.timesheets` does not refuse this statement.
-- The assertion below is the executed evidence for that, and it is the
-- counter-case to a guard that is merely too broad: WP-24 refuses a MANAGED
-- root's authorisation changing outside the accepted owner, and nothing else.
do $wp24_bound_but_not_managed$
begin
  if private.weekly_source_guard_flag_v1(
       private.weekly_source_ordinary_authorisation_guard_v1(
         (select timesheet_id from wp03_root),
         (select family_booking_id from wp03_root)),'refuse',true) is not false then
    raise exception 'ASSERTION_FAILED: ruling B3 - a BOUND but not yet MANAGED root must keep its ordinary authorisation behaviour';
  end if;
end;
$wp24_bound_but_not_managed$;

update public.timesheets
set authorised_at_server=pg_catalog.clock_timestamp()
where timesheet_id=(select timesheet_id from wp03_root);

insert into public.weekly_source_root_authorisations(
  root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
  authorised_row_signature,authorised_by_user_id
)
select
  lineage.timesheet_id,lineage.family_booking_id,lineage.timesheet_version,1,
  coalesce(
    public.timesheet_lifecycle_guard_signature_v1(
      lineage.timesheet_id,lineage.contract_week_id,false)->>'backend_row_signature',
    public.timesheet_lifecycle_guard_signature_v1(
      lineage.timesheet_id,lineage.contract_week_id,false)->>'row_signature'),
  'a3000000-0000-4000-8000-000000000001'
from wp03_root lineage;

select pg_temp.assert_true(
  (select (guard->>'ok')::boolean
      and (guard->>'managed')::boolean
      and guard->>'refusal_code'='WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
      and (guard->>'timesheet_currently_authorised')::boolean
      and (guard#>>'{authorisation,root_timesheet_id}')
          =(select timesheet_id::text from wp03_root)
   from (select private.weekly_source_managed_root_guard_v1(
     (select timesheet_id from wp03_root)) as guard) probe),
  'D8: a live root authorisation on an authorised canonical root is managed'
);

-- Decision D8 as the owner ruled on 18 September 2026 (review G5): managed is
-- the CONJUNCTION, so a live record whose Timesheet is not currently authorised
-- is managed = FALSE, and the contradiction is reported as EVIDENCE
-- (authorisation_record_without_authorised_timesheet), never as managedness.
--
-- The ordering rule this exposes belongs to the withdrawal owner: it must mark
-- the record withdrawn BEFORE, or in the same statement as, anything that
-- unauthorises the Timesheet, so the pair is never observable in this order.
-- WP-24, 18 September 2026 (Gate 13 finding F1).  THE ASSERTION BELOW IS
-- UNCHANGED; only the way its state is reached has changed, and it had to.
--
-- This block used to clear `authorised_at_server` on `wp03_root` - a root that
-- carries a LIVE authorisation generation - and set it back afterwards.  That
-- write IS finding F1: it is what leaves a committed entitlement head current
-- on a root the Office has unauthorised, and the reviewer measured GBP 90.00 /
-- 9 h being paid against GBP 140.00 / 14 h authorised because of it.  HANDOVER
-- 2 round-5 Part D rules OR-2 CONFIRMED, "Guard every entry point", and WP-24's
-- `weekly_source_managed_root_authorisation_guard_bu` trigger on
-- `public.timesheets` now refuses it - including when it is a raw table write
-- like this one, which is the whole point of putting the guard in the database
-- rather than in a caller.
--
-- The state D8 is about - a LIVE record on a Timesheet that is NOT authorised -
-- is therefore built here on its own never-authorised root instead, by writing
-- the record directly.  That is the same technique this file already uses and
-- declares above at the "stand in for interface I-6" fixture.  `wp03_root` is
-- not touched, so its authorisation-generation sequence, which `:949` and
-- `:953` assert exactly, is unaffected.
do $contradictory$
declare
  v_guard jsonb;
  v_root uuid:='a3000000-0000-4000-8000-0000000001d8';
begin
  perform pg_temp.seed_timesheet(
    v_root,'WP03-BK-D8-CONTRADICTION',1,true,
    'a3000000-0000-4000-8000-000000000004');

  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  )
  select timesheet.timesheet_id,timesheet.booking_id,timesheet.version,1,
    'wp03-d8-contradiction-signature','a3000000-0000-4000-8000-000000000001'
  from public.timesheets timesheet where timesheet.timesheet_id=v_root;

  v_guard:=private.weekly_source_managed_root_guard_v1(v_root);
  if coalesce((v_guard->>'managed')::boolean,true) is not false
     or coalesce((v_guard->>'ok')::boolean,false) is not true
     or coalesce(
          (v_guard->>'authorisation_record_without_authorised_timesheet')::boolean,
          false) is not true
     or v_guard->>'refusal_code' is not null then
    raise exception 'ASSERTION_FAILED: D8 makes a record on an unauthorised Timesheet managed=false with the contradiction as evidence: %',
      v_guard;
  end if;

  -- WP-24 adds one assertion rather than removing one: the contradiction this
  -- block names is now also the state the authorisation-boundary guard refuses,
  -- so an ordinary Authorise can never revive a stale head from it.
  if private.weekly_source_guard_flag_v1(
       private.weekly_source_ordinary_authorisation_guard_v1(
         v_root,'WP03-BK-D8-CONTRADICTION'),'refuse',false) is not true then
    raise exception 'ASSERTION_FAILED: the authorisation-boundary guard must refuse the D8 contradiction state';
  end if;
end;
$contradictory$;

-- F7: the same record covers a protected-pay root; there is no special case and
-- the guard consults no protected-pay state to decide.
select pg_temp.assert_true(
  (select guard->>'protected_target_ownership_state' is null
      and (guard->>'managed')::boolean
   from (select private.weekly_source_managed_root_guard_v1(
     (select timesheet_id from wp03_root)) as guard) probe),
  'F7: the managed decision is the root authorisation, never protected-pay state'
);

-- A withdrawn root authorisation leaves the root unmanaged, so re-authorisation
-- is never blocked; and re-authorisation appends generation 2, which makes the
-- root managed again with generation 1 kept as history.  WP-01c's
-- weekly_source_root_authorisation_withdrawal_once trigger makes a withdrawal
-- permanent (review F12), so the only way back is the new generation.
do $withdrawn$
declare
  v_guard jsonb;
  v_root uuid:=(select timesheet_id from wp03_root);
begin
  update public.weekly_source_root_authorisations
  set withdrawn_at_utc=pg_catalog.clock_timestamp(),
      withdrawn_by_user_id='a3000000-0000-4000-8000-000000000001'
  where root_timesheet_id=v_root;
  v_guard:=private.weekly_source_managed_root_guard_v1(v_root);
  if coalesce((v_guard->>'managed')::boolean,true) is not false
     or coalesce((v_guard->>'ok')::boolean,false) is not true then
    raise exception 'ASSERTION_FAILED: a withdrawn root must not be managed: %',v_guard;
  end if;

  -- F12, proved here because WP-03's guard depends on it: the withdrawal cannot
  -- be cleared, so a withdrawn generation can never be brought back to life.
  begin
    update public.weekly_source_root_authorisations
    set withdrawn_at_utc=null,withdrawn_by_user_id=null
    where root_timesheet_id=v_root;
    raise exception 'ASSERTION_FAILED: a withdrawal was cleared';
  exception when sqlstate '55000' then
    null;
  end;

  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  )
  select
    timesheet.timesheet_id,timesheet.booking_id,timesheet.version,2,
    'wp03-regeneration-signature','a3000000-0000-4000-8000-000000000001'
  from public.timesheets timesheet where timesheet.timesheet_id=v_root;

  v_guard:=private.weekly_source_managed_root_guard_v1(v_root);
  if coalesce((v_guard->>'managed')::boolean,false) is not true
     or (v_guard#>>'{authorisation,authorisation_generation}')::integer<>2 then
    raise exception 'ASSERTION_FAILED: re-authorisation must make the root managed again: %',
      v_guard;
  end if;
  if (select count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id=v_root)<>2 then
    raise exception 'ASSERTION_FAILED: generation 1 must stay as history';
  end if;
end;
$withdrawn$;

-- Review G2: when a finalisation has published the complete sorted family lock
-- set it took up front, the lineage ensure owner must refuse retryably for any
-- root that set does not contain, rather than lock a new family out of that
-- sorted order. Proved here at the ensure owner; the finalisation side, where
-- the set is recomputed under the locks and a growth refuses 40001, is proved
-- in tests/weekly-source/wp03-rotation-authority-races.mjs.
do $g2_root_set_fence$
declare
  v_message text;
  v_sqlstate text;
  v_seen text;
begin
  begin
    perform pg_catalog.set_config(
      'cloudtms.weekly_source_finalisation_root_set',
      'a3000000-0000-4000-8000-000000000102',true);
    begin
      perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
        (select row_resolution_id from wp03_root),
        'a3000000-0000-4000-8000-000000000001'
      );
      v_seen:='ACCEPTED';
    exception when others then
      get stacked diagnostics v_message=message_text,v_sqlstate=returned_sqlstate;
      v_seen:=v_message||'/'||v_sqlstate;
    end;
    if v_seen is distinct from
       'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK/40001' then
      raise exception 'ASSERTION_FAILED: G2 the ensure owner must refuse a root outside the published set, got %',
        v_seen;
    end if;
    perform pg_catalog.set_config(
      'cloudtms.weekly_source_finalisation_root_set','',true);
    raise exception 'WP03_G2_UNWIND';
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message is distinct from 'WP03_G2_UNWIND' then
      raise;
    end if;
  end;
  perform pg_catalog.set_config(
    'cloudtms.weekly_source_finalisation_root_set','',true);
end;
$g2_root_set_fence$;

-- Review G6: safety is never expressed through a LIMIT.  Two live records for
-- one canonical root must reach the guard's fail-closed branch, not hand it an
-- arbitrary generation.  The live-generation partial unique index is what
-- normally prevents this, so the case is reached by dropping that index inside
-- a plpgsql subtransaction that is unwound immediately afterwards.
do $g6_two_live_records$
declare
  v_state jsonb;
  v_guard jsonb;
  v_message text;
begin
  begin
    drop index public.weekly_source_root_authorisations_live_uq;
    insert into public.weekly_source_root_authorisations(
      root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
      authorised_row_signature,authorised_by_user_id
    )
    select timesheet.timesheet_id,timesheet.booking_id,timesheet.version,99,
      'wp03-g6-second-live-record','a3000000-0000-4000-8000-000000000001'
    from public.timesheets timesheet
    where timesheet.timesheet_id=(select timesheet_id from wp03_root);

    v_state:=private.weekly_source_root_authorisation_state_v1(
      (select timesheet_id from wp03_root),
      array[(select timesheet_id from wp03_root)]::uuid[]
    );
    if v_state->>'reason' is distinct from 'ROOT_AUTHORISATION_CARDINALITY'
       or v_state->'authorisation' is distinct from 'null'::jsonb then
      raise exception 'ASSERTION_FAILED: G6 the state reader must refuse two live records, got %',
        v_state;
    end if;

    v_guard:=private.weekly_source_managed_root_guard_v1(
      (select timesheet_id from wp03_root));
    if coalesce((v_guard->>'managed')::boolean,false) is not true
       or coalesce((v_guard->>'ok')::boolean,true) is not false
       or v_guard->>'reason' is distinct from 'ROOT_AUTHORISATION_CARDINALITY'
       or v_guard->>'refusal_code'
          is distinct from 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED' then
      raise exception 'ASSERTION_FAILED: G6 the guard must fail closed on two live records, got %',
        v_guard;
    end if;

    raise exception 'WP03_G6_UNWIND';
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message is distinct from 'WP03_G6_UNWIND' then
      raise;
    end if;
  end;
end;
$g6_two_live_records$;


-- ---------------------------------------------------------------------------
-- 4c. Rotation of an authorised root (ROT-004, F5, G6-12)
-- ---------------------------------------------------------------------------

-- A REAL installed rotation owner, not a hand imitation (review F14).  It
-- re-points contract_weeks.timesheet_id, which a hand rotation does not, so
-- this is what production actually does.
--
-- Review G4: this case must reach the state where a live authorisation record
-- sits on a rotated family, which is exactly the state WP-09's guard refuses at
-- the PUBLIC dispatcher public.timesheet_route_version_rotate.  The dispatcher
-- therefore cannot be used to construct it, and the guard must not be weakened
-- or the record withdrawn to get past it, because the record is the state these
-- assertions test.  The installed private body that the dispatcher itself calls
-- when the candidate_route_confirmation flag is off carries no guard and is
-- still a real installed owner, so this case drives that body directly. The
-- guarded dispatcher path is proved separately, in the two-session driver
-- tests/weekly-source/wp03-rotation-authority-races.mjs.
-- WP-24, 18 September 2026 (Gate 13 finding F1).  EVERY ASSERTION AFTER THIS
-- BLOCK IS UNCHANGED, and so is the end state they test: a LIVE authorisation
-- record sitting on a historical family member.
--
-- The rotation owner needs the root unauthorised, and the fixture used to reach
-- that by clearing `authorised_at_server` on a root that carries a LIVE
-- generation, then setting it back.  Both of those writes are finding F1
-- itself, and WP-24's `weekly_source_managed_root_authorisation_guard_bu`
-- trigger on `public.timesheets` now refuses them under ruled OR-2.
--
-- So the live generation is withdrawn for the duration of the two
-- authorisation-boundary writes and a fresh live generation is written back
-- onto the OLD physical id immediately afterwards, inside the same block.  This
-- is NOT the weakening the note above forbids: that note forbids withdrawing
-- the record to get the ROTATION past the dispatcher's guard, which would
-- destroy the state under test.  Here the record is live again before the
-- block ends and before any assertion runs, so what the assertions see is
-- identical.  The generation number is not asserted after this point - `:949`
-- and `:953` are the only generation assertions and both run earlier - and the
-- two unscoped `count(*)` assertions over the relation are at `:692` and
-- `:751`, both before any record exists.
do $real_rotation$
declare
  v_result jsonb;
  v_root uuid:=(select timesheet_id from wp03_root);
begin
  update public.weekly_source_root_authorisations
     set withdrawn_at_utc=pg_catalog.clock_timestamp(),
         withdrawn_by_user_id='a3000000-0000-4000-8000-000000000001',
         current_entitlement_head_id=null
   where root_timesheet_id=v_root
     and withdrawn_at_utc is null;

  update public.timesheets set authorised_at_server=null
  where timesheet_id=v_root;
  v_result:=private._timesheet_route_version_legacy_v1(
    v_root,v_root,'ALLOW_QR_AGAIN','a3000000-0000-4000-8000-000000000001',true
  );
  if nullif(v_result->>'new_timesheet_id','') is null then
    raise exception 'ASSERTION_FAILED: the installed rotation owner did not rotate: %',
      v_result;
  end if;
  create temp table wp03_rotated as select (v_result->>'new_timesheet_id')::uuid as timesheet_id;
  update public.timesheets set authorised_at_server=pg_catalog.clock_timestamp()
  where timesheet_id=v_root;

  -- The live record is put back on the OLD physical id, which is what makes the
  -- family "a live authorisation on a historical member" for every assertion
  -- below.
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  )
  select timesheet.timesheet_id,timesheet.booking_id,timesheet.version,3,
    'wp03-post-rotation-signature','a3000000-0000-4000-8000-000000000001'
  from public.timesheets timesheet where timesheet.timesheet_id=v_root;
end;
$real_rotation$;

-- ROT-004 / proof/34 section 6: the live authorisation now sits on a historical
-- member, so the family still refuses, from either physical id, and says why.
select pg_temp.assert_true(
  (select (guard->>'managed')::boolean
      and (guard->>'ok')::boolean is false
      and guard->>'reason'='AUTHORISED_ROOT_NOT_CANONICAL'
      and guard->>'refusal_code'='WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
   from (select private.weekly_source_managed_root_guard_v1(
     (select timesheet_id from wp03_rotated)) as guard) probe)
  and (select (guard->>'managed')::boolean
      and guard->>'reason'='AUTHORISED_ROOT_NOT_CANONICAL'
   from (select private.weekly_source_managed_root_guard_v1(
     (select timesheet_id from wp03_root)) as guard) probe),
  'ROT-004/F5: a rotated authorised family stays managed and fails closed either way'
);

-- Review F5: the ensure owner must refuse to bind a NEW source row to the new
-- physical Timesheet while a live authorisation stays on the old one.
select pg_temp.assert_refused(
  pg_catalog.format(
    'select public.weekly_source_timesheet_lineage_ensure_atomic_v1(%L,%L)',
    (select id from public.weekly_source_row_resolutions
     where upload_row_id='a3000000-0000-4000-8000-000000000023'),
    'a3000000-0000-4000-8000-000000000001'),
  '%WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE%',
  'F5: binding a new row while the authorisation sits on the old physical root'
);

-- proof/34 section 6 (G6-12): both lineage asserts report the rotation as an
-- integrity failure, never a stale rebuild.
select pg_temp.assert_refused(
  pg_catalog.format(
    'select private.weekly_source_timesheet_lineage_assert_v1(%L)',
    (select row_resolution_id from wp03_root)),
  '%WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE%',
  'G6-12: the lineage assert on a rotated bound root'
);
select pg_temp.assert_refused(
  pg_catalog.format(
    'select private.weekly_source_finalisation_lineage_assert_v1(%L,%L)',
    (select row_resolution_id from wp03_root),
    'a3000000-0000-4000-8000-000000000008'),
  '%WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE%',
  'G6-12: the finalisation lineage assert on a rotated bound root'
);

select pg_temp.assert_true(
  (select (resolved->>'ok')::boolean
      and resolved->>'canonical_timesheet_id'=(select timesheet_id::text from wp03_rotated)
      and (resolved->>'requested_is_canonical')::boolean is false
   from (select private.weekly_source_resolve_root_identity_v1(
     (select timesheet_id from wp03_root)) as resolved) probe),
  'G6-12: the stored root must resolve as no longer canonical'
);


-- Review G9 and WP-06 review F5: after WP-06's change S8 nothing may be keyed
-- on the physical root id alone, and the guard's evidence must fail CLOSED,
-- because a permissive failure inside a rotation guard lets a rotation through
-- that should have been refused.
select pg_temp.assert_true(
  (select (guard->>'weekly_source_bound')::boolean
   from (select private.weekly_source_managed_root_guard_v1(
     (select timesheet_id from wp03_rotated)) as guard) probe),
  'G9: the new physical id of a rotated Weekly Source family is still bound, because the probe keys on the family, not the physical root id'
);

-- HANDOVER 2 round-8 correction C5 (finance approver, 18 September 2026)
-- SUPERSEDES the assertion that previously stood here, which required
-- 'a3000000-0000-4000-8000-0000000000ff' -- an identifier that matches NO
-- Timesheet, NO binding, NO authorisation record and NO protected target
-- family -- to report bound and fail closed.  The ruling is explicit: "For an
-- identifier that matches no managed, bound or protected root, preserve the
-- former typed not-found/no-op outcome; it is not a rotation collision.  Fail
-- closed only where a relevant managed/bound/protected identity exists but its
-- family cannot be safely resolved."  The assertion is therefore replaced by
-- its corrected form, and by a NEW positive fail-closed assertion below so the
-- narrowing cannot silently become a hole.  Nothing is weakened: the refusal
-- this file proves is now proved on the case the ruling keeps.
--
-- The predicate is the installed narrowed one, copied verbatim from the call
-- sites (08082026_2035_timesheet_route_version_rotate.sql:1132-1136), and is
-- evaluated here rather than described.
create function pg_temp.installed_refusal_predicate(p_guard jsonb)
returns boolean language sql immutable as $function$
  select
    (coalesce((p_guard->>'managed')::boolean,true)
     and (coalesce((p_guard->>'ok')::boolean,true)
          or coalesce((p_guard->>'weekly_source_bound')::boolean,true)))
    or coalesce(
         (p_guard->>'authorisation_record_without_authorised_timesheet')::boolean,false)
    or p_guard->>'protected_target_ownership_state' is not null;
$function$;

select pg_temp.assert_true(
  (select (guard->>'managed')::boolean is false
      and (guard->>'weekly_source_bound')::boolean is false
      and guard->>'refusal_code' is null
      and guard->>'reason'='ROOT_NOT_FOUND'
      and (guard->>'authorisation_record_without_authorised_timesheet')::boolean is false
      and guard->>'protected_target_ownership_state' is null
      and pg_temp.installed_refusal_predicate(guard) is false
   from (select private.weekly_source_managed_root_guard_v1(
     'a3000000-0000-4000-8000-0000000000ff') as guard) probe),
  'C5: an identifier that matches no managed, bound or protected root is not a rotation collision, and the installed predicate does not refuse it'
);

-- C5, the NULL identifier: the same "matches nothing" case, so the same
-- preserved typed outcome.  The owner's own "timesheet_id is required" answers.
select pg_temp.assert_true(
  (select (guard->>'managed')::boolean is false
      and (guard->>'weekly_source_bound')::boolean is false
      and guard->>'refusal_code' is null
      and guard->>'reason'='ROOT_ID_REQUIRED'
      and pg_temp.installed_refusal_predicate(guard) is false
   from (select private.weekly_source_managed_root_guard_v1(null::uuid) as guard) probe),
  'C5: a NULL identifier matches no managed, bound or protected root and keeps its typed outcome'
);

-- C5, the half the ruling KEEPS, proved positively rather than assumed: a root
-- for which a relevant BOUND identity genuinely exists, but whose family cannot
-- be safely resolved, still fails closed.  Built with the technique WP-09b used
-- (report section 6.5): a whitespace-variant sibling of the bound family's own
-- booking reference makes it a trim-equivalent canonical collision.  The
-- sibling is removed again inside the same block, so every later assertion in
-- this file sees the state it saw before.
do $c5_fail_closed$
declare
  v_bound_timesheet_id uuid;
  v_bound_booking_id text;
  v_sibling_id uuid:='a3000000-0000-4000-8000-0000000000fe';
  v_guard jsonb;
  v_guard_before jsonb;
  v_before_bound boolean;
  v_job_ids uuid[];
begin
  -- Fixture-only bookkeeping, inside this rolled-back transaction: remember the
  -- Workbench jobs that already exist so the ones this fixture's insert queues
  -- can be removed again, and no later proof sees a Candidate this block made
  -- busy.  No Banking Pay owner is called and no Banking Pay logic changes.
  select pg_catalog.array_agg(job.id) into v_job_ids
  from public.banking_pay_workbench_jobs job;

  select timesheet_id into v_bound_timesheet_id from wp03_rotated;
  select booking_id into v_bound_booking_id
  from public.timesheets where timesheet_id=v_bound_timesheet_id;

  v_guard_before:=private.weekly_source_managed_root_guard_v1(v_bound_timesheet_id);
  v_guard:=v_guard_before;
  v_before_bound:=(v_guard->>'weekly_source_bound')::boolean;
  if v_before_bound is not true then
    raise exception 'ASSERTION_FAILED: C5 setup: the family under test must be bound before the collision is created';
  end if;

  perform pg_temp.seed_timesheet(
    v_sibling_id,' '||v_bound_booking_id,1,true,
    'a3000000-0000-4000-8000-000000000004');

  v_guard:=private.weekly_source_managed_root_guard_v1(v_bound_timesheet_id);
  if not (
       (v_guard->>'weekly_source_bound')::boolean
       and (v_guard->>'managed')::boolean
       and (v_guard->>'ok')::boolean is false
       and v_guard->>'reason' in ('FAMILY_SPLIT_BY_WHITESPACE',
                                  'BOOKING_REFERENCE_CANONICAL_COLLISION')
       and pg_temp.installed_refusal_predicate(v_guard)
     ) then
    raise exception 'ASSERTION_FAILED: C5: a relevant BOUND identity whose family cannot be safely resolved must still fail closed; guard=%',v_guard;
  end if;

  -- The same kept fail-closed case through the definer-rights shim, which is
  -- all entry point E7 can see.
  v_guard:=private.weekly_source_managed_root_guard_decision_v1(v_bound_timesheet_id);
  if not (
       (v_guard->>'weekly_source_bound')::boolean
       and (v_guard->>'managed')::boolean
       and (v_guard->>'ok')::boolean is false
       and pg_temp.installed_refusal_predicate(v_guard)
     ) then
    raise exception 'ASSERTION_FAILED: C5: the kept fail-closed case must also refuse through the shim; decision=%',v_guard;
  end if;

  delete from public.timesheets where timesheet_id=v_sibling_id;
  delete from public.banking_pay_workbench_jobs job
  where job.id<>all(coalesce(v_job_ids,array[]::uuid[]));

  v_guard:=private.weekly_source_managed_root_guard_v1(v_bound_timesheet_id);
  if v_guard is distinct from v_guard_before then
    raise exception 'ASSERTION_FAILED: C5: the collision fixture must leave the family exactly as it found it; before=% after=%',
      v_guard_before,v_guard;
  end if;
  raise notice 'PASS C5: matches-nothing permitted, bound-but-unresolvable still refused';
end;
$c5_fail_closed$;

-- S8 makes the protected-pay family unique on btrim(root_family_booking_id), so
-- two rows for one family cannot exist; the guard's AMBIGUOUS branch is defence
-- in depth. The index that guarantees it is asserted here rather than assumed.
select pg_temp.assert_true(
  exists(select 1 from pg_catalog.pg_indexes
         where schemaname='public'
           and tablename='weekly_exceptional_pay_target_families'
           and indexdef ilike '%btrim%root_family_booking_id%'
           and indexdef ilike 'CREATE UNIQUE INDEX%'),
  'G9: S8 keys the protected-pay family on btrim(root_family_booking_id), uniquely'
);


-- ---------------------------------------------------------------------------
-- 5b. WP-09b handoff N1: the decision shim carries the narrowed predicate's
--     inputs, so entry point E7 decides exactly as every definer-rights site
-- ---------------------------------------------------------------------------
--
-- HANDOVER 2 round-5 ruling B3 narrows the refusal to roots that are
-- Weekly-Source managed or bound, or that carry protected pay evidence, and is
-- explicit that an unrelated UNBOUND ordinary family must not acquire a new
-- refusal merely because this feature was installed.  The predicate WP-09b
-- installed at all 34 call sites is reproduced here verbatim and evaluated over
-- the SHIM's output, which is all entry point E7 can see.
create function pg_temp.wp09b_refuses(p_decision jsonb)
returns boolean language sql immutable as $function$
  select
    (coalesce((p_decision->>'managed')::boolean,true)
     and (coalesce((p_decision->>'ok')::boolean,true)
          or coalesce((p_decision->>'weekly_source_bound')::boolean,true)))
    or coalesce(
         (p_decision->>'authorisation_record_without_authorised_timesheet')::boolean,false)
    or p_decision->>'protected_target_ownership_state' is not null;
$function$;

-- The shim must still disclose only the decision: inputs, never identities.
select pg_temp.assert_true(
  (select decision ?& array['ok','managed','refusal_code','reason','timesheet_id',
                            'weekly_source_bound',
                            'authorisation_record_without_authorised_timesheet',
                            'protected_target_ownership_state']
      and (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(decision))=8
      and not (decision ? 'member_timesheet_ids')
      and not (decision ? 'canonical_timesheet_id')
      and not (decision ? 'family_booking_id')
      and not (decision ? 'canonical_version')
      and not (decision ? 'authorisation')
   from (select private.weekly_source_managed_root_guard_decision_v1(
     'a3000000-0000-4000-8000-000000000102') as decision) probe),
  'N1: the shim returns the decision inputs and no identity of any kind'
);

-- The three malformed UNBOUND ordinary shapes WP-09b measured. Each of them
-- reached E7 (prepare, revoke, and E25 which calls E7) as a refusal only
-- because the shim omitted weekly_source_bound: nine outcomes, one field.
do $n1_nine_shapes$
declare
  v_shape record;
  v_decision jsonb;
  v_guard jsonb;
begin
  for v_shape in
    select * from (values
      ('BLANK','a3000000-0000-4000-8000-000000000104'::uuid,'BOOKING_IDENTITY_MISSING'),
      ('SPLIT','a3000000-0000-4000-8000-000000000105'::uuid,'BOOKING_REFERENCE_CANONICAL_COLLISION'),
      ('NOCURRENT','a3000000-0000-4000-8000-000000000107'::uuid,'CANONICAL_AMBIGUOUS')
    ) as shape(name,timesheet_id,expected_reason)
  loop
    v_guard:=private.weekly_source_managed_root_guard_v1(v_shape.timesheet_id);
    v_decision:=private.weekly_source_managed_root_guard_decision_v1(v_shape.timesheet_id);

    if v_decision->>'reason' is distinct from v_shape.expected_reason then
      raise exception 'ASSERTION_FAILED: N1 % expected reason %, got %',
        v_shape.name,v_shape.expected_reason,v_decision->>'reason';
    end if;
    -- The shim must now agree with the full guard on every decision input.
    if (v_decision->>'weekly_source_bound')::boolean
       is distinct from (v_guard->>'weekly_source_bound')::boolean then
      raise exception 'ASSERTION_FAILED: N1 % shim and guard disagree on weekly_source_bound: % vs %',
        v_shape.name,v_decision->>'weekly_source_bound',v_guard->>'weekly_source_bound';
    end if;
    -- Unbound ordinary family: the ruling says leave it alone.
    if coalesce((v_decision->>'weekly_source_bound')::boolean,true) is not false then
      raise exception 'ASSERTION_FAILED: N1 % is an unbound ordinary family and must report weekly_source_bound=false: %',
        v_shape.name,v_decision;
    end if;
    if pg_temp.wp09b_refuses(v_decision) then
      raise exception 'ASSERTION_FAILED: N1 % must now PROCEED at E7 under ruling B3, got a refusal: %',
        v_shape.name,v_decision;
    end if;
    -- And the same shape must still refuse at every definer-rights site only
    -- when it is bound, which the next block proves.
  end loop;
end;
$n1_nine_shapes$;

-- The load-bearing half: the same malformed shape, on a family Weekly Source
-- HAS bound, must still refuse.  A trim-equivalent sibling is added to the
-- bound family inside a subtransaction that is unwound afterwards.
do $n1_bound_still_refuses$
declare
  v_decision jsonb;
  v_message text;
begin
  begin
    insert into public.timesheets(
      timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
      line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
      shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
      qr_payload_json,is_adjustment,created_at,updated_at
    )
    select 'a3000000-0000-4000-8000-0000000006a1',' '||timesheet.booking_id,1,false,
      'RECEIVED'::public.timesheet_status_enum,
      'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
      'HOURS'::public.timesheet_line_type_enum,'n1','n1','n1','n1','weekly-0',
      timesheet.week_ending_date,timesheet.contract_id,'[]'::jsonb,'{}'::jsonb,false,
      pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
    from public.timesheets timesheet
    where timesheet.timesheet_id=(select timesheet_id from wp03_root);

    v_decision:=private.weekly_source_managed_root_guard_decision_v1(
      (select timesheet_id from wp03_root));
    if coalesce((v_decision->>'weekly_source_bound')::boolean,false) is not true then
      raise exception 'ASSERTION_FAILED: N1 a BOUND family split by a trim-equivalent sibling must report weekly_source_bound=true: %',
        v_decision;
    end if;
    if not pg_temp.wp09b_refuses(v_decision) then
      raise exception 'ASSERTION_FAILED: N1 a BOUND malformed family must still refuse at E7: %',
        v_decision;
    end if;
    raise exception 'WP03_N1_UNWIND';
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message is distinct from 'WP03_N1_UNWIND' then
      raise;
    end if;
  end;
end;
$n1_bound_still_refuses$;

-- A root carrying a live authorisation record still refuses through the shim,
-- and an ordinary unbound family that resolves cleanly still proceeds.
select pg_temp.assert_true(
  pg_temp.wp09b_refuses(private.weekly_source_managed_root_guard_decision_v1(
    (select timesheet_id from wp03_root))),
  'N1: a managed root still refuses at E7 through the shim'
);
select pg_temp.assert_true(
  not pg_temp.wp09b_refuses(private.weekly_source_managed_root_guard_decision_v1(
    'a3000000-0000-4000-8000-000000000102')),
  'N1: an unrelated unbound ordinary family acquires no refusal from E7'
);

-- weekly_source_bound is load-bearing now, so its direction is asserted at the
-- shim, not only at the guard (review G9, WP-06 review F5).  HANDOVER 2
-- round-8 correction C5 SUPERSEDES the two assertions that stood here, which
-- required an unknown id and a null id to fail closed through the shim.  Both
-- are "matches nothing" cases, so both now carry the preserved typed outcome
-- through to entry point E7, which is the only site that reads the shim.  The
-- fail-closed direction that survives the ruling -- a relevant bound identity
-- whose family cannot be safely resolved -- is asserted through the shim in
-- the same block that asserts it at the guard (section 5, $c5_fail_closed$).
select pg_temp.assert_true(
  (select (decision->>'weekly_source_bound')::boolean is false
      and (decision->>'managed')::boolean is false
      and decision->>'refusal_code' is null
      and not pg_temp.wp09b_refuses(decision)
   from (select private.weekly_source_managed_root_guard_decision_v1(
     'a3000000-0000-4000-8000-0000000000ff') as decision) probe),
  'C5: an id that matches no managed, bound or protected root is not refused through the shim either'
);
select pg_temp.assert_true(
  (select (decision->>'weekly_source_bound')::boolean is false
      and (decision->>'managed')::boolean is false
      and decision->>'refusal_code' is null
      and not pg_temp.wp09b_refuses(decision)
   from (select private.weekly_source_managed_root_guard_decision_v1(null) as decision) probe),
  'C5: a null id is not refused through the shim either'
);

-- ---------------------------------------------------------------------------
-- 6. Owner, security and ACL
-- ---------------------------------------------------------------------------

select pg_temp.assert_true(
  (select count(*)=8 from pg_catalog.pg_proc proc
   join pg_catalog.pg_namespace space on space.oid=proc.pronamespace
   where space.nspname='private'
     and proc.proname in (
       'weekly_source_candidate_serial_gate_v1',
       'weekly_source_lock_family_rows_v1',
       'weekly_source_lock_and_resolve_families_v1',
       'weekly_source_resolve_root_identity_v1',
       'weekly_source_root_integrity_assert_v1',
       'weekly_source_root_authorisation_state_v1',
       'weekly_source_managed_root_guard_v1',
       'weekly_source_managed_root_guard_decision_v1'
     )
     and proc.prosecdef
     and pg_catalog.pg_get_userbyid(proc.proowner)='postgres'
     and proc.proconfig @> array['search_path=public, private, pg_catalog, pg_temp']),
  'all six rotation-authority functions must be SECURITY DEFINER, postgres-owned, fixed search_path'
);

select pg_temp.assert_true(
  (select bool_and(
     not pg_catalog.has_function_privilege('anon',proc.oid,'EXECUTE')
     and not pg_catalog.has_function_privilege('authenticated',proc.oid,'EXECUTE')
     and not pg_catalog.has_function_privilege('service_role',proc.oid,'EXECUTE'))
   from pg_catalog.pg_proc proc
   join pg_catalog.pg_namespace space on space.oid=proc.pronamespace
   where space.nspname='private'
     and proc.proname like 'weekly_source_%'
     and proc.proname in (
       'weekly_source_candidate_serial_gate_v1',
       'weekly_source_lock_family_rows_v1',
       'weekly_source_lock_and_resolve_families_v1',
       'weekly_source_resolve_root_identity_v1',
       'weekly_source_root_integrity_assert_v1',
       'weekly_source_root_authorisation_state_v1',
       'weekly_source_managed_root_guard_v1'
     )),
  'ROT-012: no browser or service role may execute the rotation-authority owners'
);


-- Review F8: the one deliberate exception to owner-only, and its exact
-- boundary.  Entry point E7 is SECURITY INVOKER and executable by
-- `service_role`, so a guard call inside it needs a definer-rights shim.
select pg_temp.assert_true(
  (select pg_catalog.has_function_privilege('service_role',proc.oid,'EXECUTE')
      and not pg_catalog.has_function_privilege('anon',proc.oid,'EXECUTE')
      and not pg_catalog.has_function_privilege('authenticated',proc.oid,'EXECUTE')
      and not pg_catalog.has_function_privilege('public',proc.oid,'EXECUTE')
      and proc.prosecdef
      and pg_catalog.pg_get_userbyid(proc.proowner)='postgres'
   from pg_catalog.pg_proc proc
   join pg_catalog.pg_namespace space on space.oid=proc.pronamespace
   where space.nspname='private'
     and proc.proname='weekly_source_managed_root_guard_decision_v1'),
  'F8: only the decision shim is executable by service_role, never by a browser role'
);

-- The shim must leak nothing beyond the refusal decision and its inputs.
-- WP-09b handoff N1 added three decision INPUTS to the five original keys; the
-- shape is asserted exactly in section 5b, so this check covers disclosure only.
select pg_temp.assert_true(
  (select decision ?& array['ok','managed','refusal_code','reason','timesheet_id']
      and (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(decision))=8
      and not (decision ? 'member_timesheet_ids')
      and not (decision ? 'canonical_timesheet_id')
      and not (decision ? 'family_booking_id')
      and not (decision ? 'authorisation')
      and not (decision ? 'canonical_version')
   from (select private.weekly_source_managed_root_guard_decision_v1(
     'a3000000-0000-4000-8000-000000000102') as decision) probe),
  'F8: the decision shim discloses only the decision and its inputs'
);

-- The shim agrees with the guard on the decision itself.
select pg_temp.assert_true(
  (select (decision->>'managed')::boolean
          is not distinct from (guard->>'managed')::boolean
      and decision->>'refusal_code' is not distinct from guard->>'refusal_code'
   from (select
      private.weekly_source_managed_root_guard_decision_v1(
        (select timesheet_id from wp03_root)) as decision,
      private.weekly_source_managed_root_guard_v1(
        (select timesheet_id from wp03_root)) as guard) probe),
  'F8: the decision shim never disagrees with the guard'
);

-- Entry point E7 really is SECURITY INVOKER and service_role-executable, which
-- is why the shim exists; the guard itself stays unreachable from that role.
select pg_temp.assert_true(
  (select pg_catalog.count(*)=2
      and pg_catalog.bool_and(
            not proc.prosecdef
            and pg_catalog.has_function_privilege('service_role',proc.oid,'EXECUTE'))
   from pg_catalog.pg_proc proc
   join pg_catalog.pg_namespace space on space.oid=proc.pronamespace
   where space.nspname='public'
     and proc.proname in ('tsfin_prepare_write','tsfin_mark_revoked')),
  'F8: entry point E7 really is SECURITY INVOKER and service_role-executable'
);

do $f8_invoker$
declare
  v_decision jsonb;
  v_refused boolean:=false;
begin
  set local role service_role;
  -- The guard itself must stay out of reach.
  begin
    perform private.weekly_source_managed_root_guard_v1(
      'a3000000-0000-4000-8000-000000000102');
  exception when insufficient_privilege then
    v_refused:=true;
  end;
  -- The shim must work for exactly the role E7 runs as.
  v_decision:=private.weekly_source_managed_root_guard_decision_v1(
    'a3000000-0000-4000-8000-000000000102');
  reset role;
  if not v_refused then
    raise exception 'ASSERTION_FAILED: service_role could execute the guard directly';
  end if;
  if v_decision is null or not (v_decision ? 'managed') then
    raise exception 'ASSERTION_FAILED: service_role could not execute the decision shim: %',
      v_decision;
  end if;
end;
$f8_invoker$;

-- The interface I-1 signature three other packages code against is exact.
select pg_temp.assert_true(
  (select count(*)=1 from pg_catalog.pg_proc proc
   join pg_catalog.pg_namespace space on space.oid=proc.pronamespace
   where space.nspname='private'
     and proc.proname='weekly_source_lock_and_resolve_families_v1'
     and pg_catalog.pg_get_function_identity_arguments(proc.oid)
         ='p_candidate_id uuid, p_requested_timesheet_ids uuid[], p_job_type text, p_job_id uuid, p_reason text'),
  'I-1: the fixed signature must be exactly as the interface states'
);

select jsonb_build_object(
  'ok',true,'verification','weekly_source_rotation_authority_v1',
  'lineage_generations',(
    select pg_catalog.count(*) from public.weekly_source_row_timesheet_lineages),
  'timesheet_families',(
    select pg_catalog.count(distinct pg_catalog.btrim(booking_id)) from public.timesheets),
  'banking_pay_rows_written',(
    select pg_catalog.count(*) from public.pay_batch_items)
);

\if :{?weekly_source_verification_outer_transaction}
\else
rollback;
\endif
