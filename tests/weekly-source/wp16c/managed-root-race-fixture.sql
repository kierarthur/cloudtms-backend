-- Weekly Source Plan 6.2 — Gate 12 (WP-16c). COMMITTED fixture for the UNA-014
-- concurrency proofs.
--
-- `UNA-014` races withdrawal against a Workbench source build and Draft
-- preparation for the same Candidate, and against a concurrent rotation attempt
-- on the same family, in both orders. A race needs two sessions that can see
-- each other's committed state, so this fixture commits. It is only ever applied
-- to a disposable local clone created for the run and dropped after it.
--
-- Roots produced:
--   RACE-A  `d6000000-…-000000000301`, booking `WP16C-RACE-A`, AUTHORISED.
--           A plain family; the serial-gate and Draft races.
--   RACE-B  `d6000000-…-000000000302` (version 2, current) with
--           `d6000000-…-000000000392` (version 1, demoted), booking
--           `'  WP16C-RACE-B  '` — whitespace-padded on purpose, which is the
--           `R37`/`ROT-012` lock shape (trimmed key, then raw key). AUTHORISED.
--   RACE-C  `d6000000-…-000000000303` / `…000000000393`, booking
--           `'  WP16C-RACE-C  '` — padded and rotated, NOT authorised, for
--           `ROT-002`, where the rotation commits first.
--   RACE-D  `d6000000-…-000000000304` / `…000000000394`, booking
--           `'  WP16C-RACE-D  '` — padded and rotated, NOT authorised, for
--           `ROT-003`, where the first authorisation commits first.
--
-- Nothing here defines, wraps or re-creates a Banking Pay owner.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('d6000000-0000-4000-8000-000000000001','wp16c-race@example.test','admin',true,'not-a-login')
on conflict (id) do nothing;
insert into public.clients(id,name)
values ('d6000000-0000-4000-8000-000000000002','WP16C Race Client')
on conflict (id) do nothing;
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('d6000000-0000-4000-8000-000000000002',20,'2026-01-01')
on conflict do nothing;

create or replace function pg_temp.seed_timesheet(
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
    'HOURS'::public.timesheet_line_type_enum,'wp16c-occupant','wp16c-hospital',
    'wp16c-ward','wp16c-role','weekly-0','2026-09-13',p_contract_id,
    '[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) returning timesheet_id;
$function$;

create or replace function pg_temp.seed_week_and_financials(
  p_contract_week_id uuid,p_contract_id uuid,p_timesheet_id uuid,
  p_financials_id uuid,p_candidate_id uuid,p_client_id uuid,p_version integer
) returns void language sql as $function$
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    timesheet_id,is_adjustment
  ) values (
    p_contract_week_id,p_contract_id,'2026-09-13',0,
    'SUBMITTED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,p_timesheet_id,false);
  insert into public.timesheets_financials(
    id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat
  ) values (
    p_financials_id,p_timesheet_id,p_version,true,p_candidate_id,p_client_id,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,10,100,200);
$function$;

do $seed_race_world$
declare
  v_index integer;
  v_candidate uuid;
  v_contract uuid;
  v_timesheet uuid;
begin
  for v_index in 1..4 loop
    v_candidate:=('d6000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_contract:=('d6000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_timesheet:=('d6000000-0000-4000-8000-0000000003'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    insert into public.candidates(id,display_name)
    values (v_candidate,'WP16C Race Candidate '||v_index);
    insert into public.contracts(
      id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
      weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
    ) values (
      v_contract,v_candidate,'d6000000-0000-4000-8000-000000000002',
      '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);
    if v_index in (2,3,4) then
      -- Whitespace-padded rotated families: version 1 demoted, version 2 current.
      -- Candidate 2's is authorised below; Candidates 3 and 4's are deliberately
      -- left UNAUTHORISED, because ROT-002 and ROT-003 race a rotation against a
      -- FIRST authorisation and so must start before one exists.
      perform pg_temp.seed_timesheet(
        ('d6000000-0000-4000-8000-0000000003'||pg_catalog.lpad((90+v_index)::text,2,'0'))::uuid,
        '  WP16C-RACE-'||pg_catalog.chr(64+v_index)||'  ',1,false,v_contract);
      perform pg_temp.seed_timesheet(
        v_timesheet,'  WP16C-RACE-'||pg_catalog.chr(64+v_index)||'  ',2,true,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('d6000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('d6000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'d6000000-0000-4000-8000-000000000002',2);
    else
      perform pg_temp.seed_timesheet(v_timesheet,'WP16C-RACE-A',1,true,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('d6000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('d6000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'d6000000-0000-4000-8000-000000000002',1);
    end if;
  end loop;
end
$seed_race_world$;

-- Drain the Workbench jobs the installed dirty trigger queued for the fixture
-- Candidates, or the installed serial gate reports
-- CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_CONTINUATION and every gated owner refuses
-- WEEKLY_SOURCE_CANDIDATE_BUSY before the race has begun (WP-07 finding F1).
update public.banking_pay_workbench_jobs
   set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
 where status in ('QUEUED','RUNNING');

-- Both roots reach `managed` through the real installed first-authorisation
-- owner, never by writing `weekly_source_root_authorisations` by hand.
do $authorise_race_roots$
declare
  v_result jsonb;
begin
  v_result:=public.weekly_source_first_authorise_v1(
    'd6000000-0000-4000-8000-000000000301','d6000000-0000-4000-8000-000000000301',
    null,'d6000000-0000-4000-8000-000000000001');
  if coalesce((v_result->>'ok')::boolean,false) is not true then
    raise exception 'WP16C_RACE_FIXTURE_AUTHORISE_A_FAILED: %',v_result::text;
  end if;
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');

  v_result:=public.weekly_source_first_authorise_v1(
    'd6000000-0000-4000-8000-000000000302','d6000000-0000-4000-8000-000000000302',
    null,'d6000000-0000-4000-8000-000000000001');
  if coalesce((v_result->>'ok')::boolean,false) is not true then
    raise exception 'WP16C_RACE_FIXTURE_AUTHORISE_B_FAILED: %',v_result::text;
  end if;
  -- RACE-C and RACE-D are deliberately NOT authorised here.
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
end
$authorise_race_roots$;

-- A Draft holding a live item for Candidate 1 is seeded by the FIXTURE, not by
-- an owner: Create Draft is on contract section 2's do-not-touch list and
-- decision D2 puts Banking Pay's new logic out of scope, so the Draft is a
-- fixture in the existing evidence tables, exactly as WP-16a's library does.
-- It is deliberately NOT attached to the race roots: race (b) attaches it
-- inside the race, so the "Draft exists first" and "withdrawal first" orders
-- can both be driven.
select 'WP16C_RACE_FIXTURE_READY' as result,
       (select count(*) from public.weekly_source_root_authorisations
         where withdrawn_at_utc is null
           and root_timesheet_id in ('d6000000-0000-4000-8000-000000000301',
                                     'd6000000-0000-4000-8000-000000000302')) as live_generations,
       (select count(*) from public.weekly_source_root_authorisations
         where root_timesheet_id in ('d6000000-0000-4000-8000-000000000303',
                                     'd6000000-0000-4000-8000-000000000304')) as unauthorised_race_roots;

commit;
