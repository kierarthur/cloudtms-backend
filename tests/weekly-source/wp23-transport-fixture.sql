-- Weekly Source Plan 6.2 — WP-23 transport proof fixture.
--
-- WP-19b's code-completeness sweep found eleven new Weekly Source service RPCs
-- with no caller outside the database.  WP-23 wires them.  An unreachable owner
-- and a wired one look identical in a unit test, so every WP-23 claim is made by
-- driving the REAL route through `dispatchWeeklySourceRequest` against a real
-- PostgreSQL 17.11 build and then measuring the database.  This file is the
-- committed world those drives run against.
--
-- It is COMMITTED, not rolled back, because the drives are several separate
-- transactions (one per route call, exactly as PostgREST runs them) and the
-- post-rollback guard-refusal recorder is only meaningful across transactions.
-- It therefore runs only against a disposable local clone named by WP-23.
--
-- The world shape is WP-16C's (`tests/weekly-source/wp16c/una-proofs.sql`), so
-- that the same owners are driven against the same kind of ground the executed
-- Gate 12 suites already use.  Identifiers are WP-23's own `c9…` block so no
-- other package's fixture can collide with it.
--
-- Nothing here defines, wraps or re-creates a Banking Pay, Draft, execution,
-- cancellation, settlement, provider, recovery or remittance owner.
-- `set constraints all immediate` is never used.

\set ON_ERROR_STOP on

begin;

-- The installed Workbench dirty trigger queues a job for every Candidate this
-- fixture touches and the installed Candidate serial gate then reports
-- CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_CONTINUATION, so every gated Weekly Source
-- owner would refuse WEEKLY_SOURCE_CANDIDATE_BUSY.  The fixture reproduces the
-- Workbench worker's completion on its own fixture rows.  It changes no Banking
-- Pay definition and is never performed by an owner.
create or replace function public.wp23_drain_workbench_jobs() returns void
language sql as $function$
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
$function$;

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('c9000000-0000-4000-8000-000000000001','wp23-office@example.test','admin',true,'not-a-login')
on conflict (id) do nothing;

-- Client A is inside a Weekly Source group; Client B is not.  Both are needed:
-- the Office Authorise route must send A's week to the first-authorisation
-- wrapper and must leave B's ordinary Weekly week on the ordinary owner.
insert into public.clients(id,name)
values ('c9000000-0000-4000-8000-000000000002','WP23 Weekly Source Client'),
       ('c9000000-0000-4000-8000-000000000003','WP23 Ordinary Client')
on conflict (id) do nothing;

insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('c9000000-0000-4000-8000-000000000002',20,'2026-01-01'),
       ('c9000000-0000-4000-8000-000000000003',20,'2026-01-01')
on conflict do nothing;

-- The Weekly Source binding: an active group, a membership covering the week
-- ending date, and the one client policy the effective-policy owner requires.
-- `TIMESHEET_AUTHORITY` is chosen so the presentation owner does not refuse with
-- its own SOURCE_CHECK_IN_PROGRESS before applicability can be read; the
-- source-authority branch is exercised separately by the Gate 9 verifier.
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,timezone,
  cutoff_weekday,cutoff_local_time,active
) values (
  'c9000000-0000-4000-8000-000000000601','TEST',
  'c9000000-0000-4000-8000-0000000006ff','WP23_GROUP','WP23 Source Group','ROSTER',
  'Europe/London',2,'12:00:00',true
) on conflict (id) do nothing;

insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,valid_to
) values (
  'c9000000-0000-4000-8000-000000000602',
  'c9000000-0000-4000-8000-000000000601',
  'c9000000-0000-4000-8000-000000000002','2026-01-01',null
) on conflict (id) do nothing;

insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,effective_to,
  authority_mode,document_mode,self_bill_enabled,created_by_user_id
) values (
  'c9000000-0000-4000-8000-000000000603',
  'c9000000-0000-4000-8000-000000000601',
  'c9000000-0000-4000-8000-000000000002','2026-01-01',null,
  'TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',false,'c9000000-0000-4000-8000-000000000001'
) on conflict (id) do nothing;

do $seed_world$
declare
  v_index integer;
  v_candidate uuid;
  v_contract uuid;
  v_timesheet uuid;
begin
  for v_index in 1..3 loop
    v_candidate:=('c9000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_contract:=('c9000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_timesheet:=('c9000000-0000-4000-8000-0000000003'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;

    insert into public.candidates(id,display_name)
    values (v_candidate,'WP23 Candidate '||v_index);

    insert into public.contracts(
      id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
      weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
    ) values (
      v_contract,v_candidate,
      -- Candidates 1 and 2 belong to the Weekly-Source-bound Client; Candidate 3
      -- is the ordinary control.
      (case when v_index=3 then 'c9000000-0000-4000-8000-000000000003'
            else 'c9000000-0000-4000-8000-000000000002' end)::uuid,
      '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);

    insert into public.timesheets(
      timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
      line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
      shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
      qr_payload_json,is_adjustment,created_at,updated_at
    ) values (
      v_timesheet,'WP23-BK-'||pg_catalog.lpad(v_index::text,2,'0'),1,true,
      'RECEIVED'::public.timesheet_status_enum,
      'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
      'HOURS'::public.timesheet_line_type_enum,'wp23-occupant','wp23-hospital',
      'wp23-ward','wp23-role','weekly-0','2026-09-13',v_contract,
      '[]'::jsonb,'{}'::jsonb,false,
      pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());

    insert into public.contract_weeks(
      id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
      timesheet_id,is_adjustment
    ) values (
      ('c9000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
      v_contract,'2026-09-13',0,
      'SUBMITTED'::public.contract_week_status_enum,
      'MANUAL'::public.submission_mode_enum,v_timesheet,false);

    insert into public.timesheets_financials(
      id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
      processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat
    ) values (
      ('c9000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
      v_timesheet,1,true,v_candidate,
      (case when v_index=3 then 'c9000000-0000-4000-8000-000000000003'
            else 'c9000000-0000-4000-8000-000000000002' end)::uuid,
      'PENDING_AUTH'::public.ts_fin_processing_status_enum,10,100,200);
  end loop;
end
$seed_world$;

-- A bundle already in MANUAL_REVIEW, so the Office reopen (G5-6) has something
-- real to return to the queue.  The decision bundle is the parent the pending
-- bundle's foreign key requires; neither row is produced by an owner here,
-- because the point of the proof is the ROUTE, not the coordinator that
-- normally writes them.
insert into public.weekly_source_entitlement_decision_bundles(
  decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
  bundle_kind,source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
  decision_id,decided_by_user_id,publication_mode,
  request_digest,source_revision_digest,contract_choice_digest,before_inventory_digest,
  proposed_head_ids,state,committed_at_utc
) values (
  'c9000000-0000-4000-8000-000000000801',1,'c9000000-0000-4000-8000-0000000006ff',
  'c9000000-0000-4000-8000-000000000101','2026-09-13',
  'SINGLE_ROOT','WP23-BK-01','c9000000-0000-4000-8000-000000000301',
  'c9000000-0000-4000-8000-000000000201',
  'c9000000-0000-4000-8000-000000000802','c9000000-0000-4000-8000-000000000001','DEFERRED',
  decode(repeat('11',32),'hex'),decode(repeat('12',32),'hex'),
  decode(repeat('13',32),'hex'),decode(repeat('14',32),'hex'),
  array['c9000000-0000-4000-8000-000000000803']::uuid[],'COMMITTED',pg_catalog.statement_timestamp()
) on conflict do nothing;

insert into public.weekly_source_pending_entitlement_bundles(
  id,decision_bundle_id,bundle_revision,candidate_id,
  member_root_ids,member_family_booking_ids,member_root_versions,
  request_digest,source_revision_digest,contract_choice_digest,
  decision_id,decided_by_user_id,proposed_head_ids,request_json,
  pending_revision,state,technical_failure_count,manual_review_reason
) values (
  'c9000000-0000-4000-8000-000000000804','c9000000-0000-4000-8000-000000000801',1,
  'c9000000-0000-4000-8000-000000000101',
  array['c9000000-0000-4000-8000-000000000301']::uuid[],
  array['WP23-BK-01']::text[],array[1]::integer[],
  decode(repeat('11',32),'hex'),decode(repeat('12',32),'hex'),decode(repeat('13',32),'hex'),
  'c9000000-0000-4000-8000-000000000802','c9000000-0000-4000-8000-000000000001',
  array['c9000000-0000-4000-8000-000000000803']::uuid[],
  '{"wp23_fixture":true}'::jsonb,
  3,'MANUAL_REVIEW',7,'WP23 fixture: escalated for Office review.'
) on conflict do nothing;

select public.wp23_drain_workbench_jobs();

commit;
