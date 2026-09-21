-- Rollback-only PostgreSQL 17.11 proof for the Weekly Source read-only Banking
-- Pay freeze census (`private.weekly_source_freeze_census_v1`).
--
-- Authority: `P:\proof\32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md`
-- sections 4.0 to 4.3, 5.1 to 5.4 and the verbatim HANDOVER 2 rulings in
-- section 14.  Scenario ids below are the `R` numbers of section 12.
--
-- PROVISIONAL-FIXTURE: the WP-16a Banking Pay evidence library
-- (`tests/weekly-source/fixtures-banking/`) did not exist when this verifier was
-- written, so every Banking Pay state below is seeded directly here in the exact
-- shape the installed writers produce.  Every such scenario is marked
-- `PROVISIONAL-FIXTURE` in its comment.
--
-- The WP-16a library has since been delivered (29 states) and every scenario was
-- re-run against it read-only, item by item, in a disposable clone.  The marker
-- now means "this file seeds its own evidence", not "unproved": the following
-- are additionally corroborated by the real installed owners or by WP-16a's
-- named seeds, with the classes this census assigns recorded in
-- `plan6-2-implementation\reports\WP-08a_REPORT.md`, "Review response":
--
--   Binding A terminal (`VOIDED_TERMINAL/A`), Binding B (`VOIDED_TERMINAL/B`),
--   the mid-flight cancellation (`ACTIVE` / `VOID_NOT_YET_PROVED`, open ruling
--   OR-8), `R20`, `R34` (2 history conflicts), `R35` (3 snapshot conflicts),
--   `R31` (`VOID_UNBINDABLE`), `R43` (both halves), `R2`, `R4`, `R16`, `R28`.
--
-- Still seeded here only, because no real-owner state reaches them:
-- `R30`/Binding C (the installed writers void recovery items with a NULL
-- `timesheet_id`, so `§4.2` never enumerates them - open ruling OR-10), `R21`,
-- `R44`'s matched Umbrella case, and the review scenarios F1, F3, F4, F6, F7.
--
-- The census is read-only: this file also proves that its installed definition
-- takes no row lock on a Banking Pay table, writes nothing, never reads the
-- non-authoritative `timesheet_pay_state` cache and never reads current
-- Candidate Umbrella data.

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

-- The static contract assertions below inspect executable code only: SQL line
-- comments are stripped first, so a comment that quotes a forbidden construct
-- (the open-ruling blocks do quote them) cannot make the assertion lie.
create function pg_temp.census_code() returns text
language sql stable as $function$
  select pg_catalog.regexp_replace(
    pg_catalog.pg_get_functiondef(
      'private.weekly_source_freeze_census_v1(uuid,uuid[])'::pg_catalog.regprocedure),
    '--[^' || pg_catalog.chr(10) || ']*','','g');
$function$;

create function pg_temp.census(p_members uuid[]) returns jsonb
language sql stable as $function$
  select private.weekly_source_freeze_census_v1(
    'aa000000-0000-4000-8000-00000000000a'::uuid,p_members);
$function$;

create function pg_temp.census_family(p_booking text) returns jsonb
language sql stable as $function$
  select private.weekly_source_freeze_census_v1(
    'aa000000-0000-4000-8000-00000000000a'::uuid,
    (select coalesce(pg_catalog.array_agg(family_row.timesheet_id),array[]::uuid[])
     from public.timesheets family_row where family_row.booking_id=p_booking));
$function$;

create function pg_temp.item_class(p_census jsonb,p_item uuid) returns text
language sql immutable as $function$
  select census_item->>'class'
  from pg_catalog.jsonb_array_elements(p_census->'items') as census_item
  where census_item->>'pay_batch_item_id'=p_item::text;
$function$;

create function pg_temp.item_binding(p_census jsonb,p_item uuid) returns text
language sql immutable as $function$
  select census_item->>'binding'
  from pg_catalog.jsonb_array_elements(p_census->'items') as census_item
  where census_item->>'pay_batch_item_id'=p_item::text;
$function$;

create function pg_temp.has_predicate(p_census jsonb,p_predicate text) returns boolean
language sql immutable as $function$
  select exists (
    select 1 from pg_catalog.jsonb_array_elements(p_census->'predicates') as predicate_row
    where predicate_row->>'predicate'=p_predicate);
$function$;

create function pg_temp.has_error(p_census jsonb,p_code text) returns boolean
language sql immutable as $function$
  select exists (
    select 1 from pg_catalog.jsonb_array_elements(p_census->'errors') as error_row
    where error_row->>'code'=p_code);
$function$;

create function pg_temp.mk_timesheet(
  p_id uuid,p_booking text,p_version integer,p_is_current boolean,p_contract uuid
) returns uuid language plpgsql as $function$
begin
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,week_ending_date,contract_id,sheet_scope,line_type,
    version,is_current,status
  ) values (
    p_id,p_booking,'occ-norm','hosp-norm','ward-norm','nurse-norm',
    date '2026-09-13',p_contract,'WEEKLY','HOURS',p_version,p_is_current,'RECEIVED'
  );
  return p_id;
end;
$function$;

create function pg_temp.mk_batch(
  p_id uuid,p_status text,p_commit_state text,
  p_cancelled timestamptz,p_completed timestamptz,p_schedule_kind text
) returns uuid language plpgsql as $function$
begin
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    execution_commit_state,execution_committed_at_utc,execution_commit_ref,
    cancelled_at_utc,completed_at_utc,schedule_kind,scheduled_at_utc
  ) values (
    p_id,date '2026-09-18',p_status,'MONZO_CSV','SAGE',p_commit_state,
    case when p_commit_state='COMMITTED' then timestamptz '2026-09-18T10:00:00Z' end,
    case when p_commit_state='COMMITTED' then 'commit-'||p_id::text end,
    p_cancelled,p_completed,p_schedule_kind,
    case when p_schedule_kind='SCHEDULED' then timestamptz '2026-09-18T09:00:00Z' end
  );
  return p_id;
end;
$function$;

create function pg_temp.mk_candidate_row(
  p_id uuid,p_batch uuid,p_candidate uuid,p_settlement text,p_settled boolean
) returns uuid language plpgsql as $function$
begin
  insert into public.pay_batch_candidates(
    id,pay_batch_id,candidate_id,settlement_status,settled_at_utc
  ) values (
    p_id,p_batch,p_candidate,p_settlement,
    case when p_settled then timestamptz '2026-09-18T10:05:00Z' end
  );
  return p_id;
end;
$function$;

create function pg_temp.mk_item(
  p_id uuid,p_candidate_row uuid,p_timesheet uuid,p_voided boolean,
  p_channel text,p_umbrella uuid,p_transfer uuid,p_reservation uuid
) returns uuid language plpgsql as $function$
begin
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,umbrella_id,
    is_voided,pay_bank_transfer_id,reservation_id,amount_inc_vat
  ) values (
    p_id,p_candidate_row,'TIMESHEET_PAY',p_timesheet,p_channel,p_umbrella,
    p_voided,p_transfer,p_reservation,100.00
  );
  return p_id;
end;
$function$;

create function pg_temp.mk_transfer(
  p_id uuid,p_batch uuid,p_candidate uuid,p_umbrella uuid,p_channel text,
  p_status text,p_rail_state text
) returns uuid language plpgsql as $function$
begin
  insert into public.pay_bank_transfers(
    id,pay_batch_id,candidate_id,umbrella_id,pay_channel,amount,status,rail_state,
    transfer_group_key
  ) values (
    p_id,p_batch,p_candidate,p_umbrella,p_channel,100.00,p_status,p_rail_state,
    'grp-'||p_id::text
  );
  return p_id;
end;
$function$;

create function pg_temp.mk_operation(
  p_id uuid,p_batch uuid,p_type text,p_status text,p_phase text,
  p_lease timestamptz,p_lock timestamptz,p_freeze text,p_input jsonb
) returns uuid language plpgsql as $function$
begin
  insert into public.banking_pay_operations(
    id,operation_type,status,phase,pay_batch_id,idempotency_key,
    lease_expires_at_utc,lock_expires_at_utc,scope_freeze_status,input_json
  ) values (
    p_id,p_type,p_status,p_phase,p_batch,'idem-'||p_id::text,
    p_lease,p_lock,coalesce(p_freeze,'NONE'),coalesce(p_input,'{}'::jsonb)
  );
  return p_id;
end;
$function$;

-- The settle rail's own snapshot and history shape
-- (`04082026_1211_pay_settle_rail.sql` snapshot selection and history insert).
create function pg_temp.mk_settled_proof(
  p_batch uuid,p_timesheet uuid,p_signature text,p_channel text
) returns void language plpgsql as $function$
begin
  insert into public.pay_batch_timesheet_snapshots(
    pay_batch_id,timesheet_id,candidate_id,pay_channel,
    base_snapshot_json,target_snapshot_json,signature
  ) values (
    p_batch,p_timesheet,'aa000000-0000-4000-8000-00000000000a',p_channel,
    '{"base":true}'::jsonb,'{"target":true}'::jsonb,p_signature
  );
  insert into public.timesheet_pay_state_history(
    timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
  ) values (
    p_timesheet,p_batch,timestamptz '2026-09-18T10:05:00Z',
    '{"target":true}'::jsonb,p_signature
  );
end;
$function$;

-- Binding A evidence exactly as the installed correction owners record it:
-- the request, its candidate membership row, an APPLIED work item whose
-- `result_json.changed_scope_json.changed_pay_batch_item_ids` names the item,
-- the durable `pay_payment_correction_items` row, and the terminal
-- `PAYMENT_CORRECTION` operation at `COMPLETE`/`COMPLETE`.
create function pg_temp.mk_binding_a(
  p_request uuid,p_batch uuid,p_candidate_row uuid,p_item uuid,
  p_request_status text,p_work_item uuid,p_operation uuid
) returns void language plpgsql as $function$
begin
  insert into public.pay_payment_correction_requests(
    id,pay_batch_id,correction_kind,status,selection_hash,plan_hash
  ) values (p_request,p_batch,'PRE_BANK_CANCEL',p_request_status,
            'sel-'||p_request::text,'plan-'||p_request::text)
  on conflict (id) do nothing;

  insert into public.pay_payment_correction_request_candidates(
    correction_request_id,selection_ordinal,pay_batch_candidate_id,
    candidate_scope_hash,active_item_count,source_row_count,active_amount,
    pay_batch_item_ids,eligibility_code_at_plan
  ) values (p_request,1,p_candidate_row,encode(sha256(convert_to(p_candidate_row::text,'UTF8')),'hex'),1,1,100.00,
            array[p_item],'ELIGIBLE')
  on conflict do nothing;

  insert into public.pay_payment_correction_work_items(
    id,correction_request_id,pay_batch_id,pay_batch_candidate_id,work_kind,
    selection_json,selection_hash,status,result_json
  ) values (
    p_work_item,p_request,p_batch,p_candidate_row,'PRE_BANK_CANCEL',
    pg_catalog.jsonb_build_object('expected_pay_batch_item_ids',
      pg_catalog.jsonb_build_array(p_item::text)),
    encode(sha256(convert_to(p_candidate_row::text,'UTF8')),'hex'),'APPLIED',
    pg_catalog.jsonb_build_object(
      'ok',true,'status','APPLIED',
      'changed_scope_json',pg_catalog.jsonb_build_object(
        'pay_batch_id',p_batch::text,
        'correction_request_id',p_request::text,
        'change_kind','PRE_BANK_CANCEL',
        'changed_pay_batch_item_ids',
          pg_catalog.jsonb_build_array(p_item::text)))
  );

  insert into public.pay_payment_correction_items(
    correction_request_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
    correction_item_kind,status
  ) values (p_request,p_batch,p_candidate_row,p_item,'PRE_BANK_CANCEL','APPLIED');

  perform pg_temp.mk_operation(
    p_operation,p_batch,'PAYMENT_CORRECTION','COMPLETE','COMPLETE',
    null,null,'NONE',
    pg_catalog.jsonb_build_object('correction_request_id',p_request::text));
end;
$function$;

-- ----------------------------------------------------------------- base seed
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do nothing;

insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser)
values ('aa000000-0000-4000-8000-000000000001','census-owner@example.test',
        'admin',true,'not-a-login',true);
insert into public.clients(id,name)
values ('aa000000-0000-4000-8000-000000000002','Census Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('aa000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.umbrellas(id,name,vat_chargeable,enabled)
values ('aa000000-0000-4000-8000-00000000000e','Census Umbrella',true,true);
insert into public.umbrellas(id,name,vat_chargeable,enabled)
values ('aa000000-0000-4000-8000-00000000000f','Census Umbrella Two',true,true);
insert into public.candidates(id,display_name,umbrella_id)
values ('aa000000-0000-4000-8000-00000000000a','Census Candidate A',
        'aa000000-0000-4000-8000-00000000000e');
insert into public.candidates(id,display_name)
values ('aa000000-0000-4000-8000-00000000000b','Census Candidate B');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'aa000000-0000-4000-8000-00000000000c','aa000000-0000-4000-8000-00000000000a',
  'aa000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
  '{"pay":{"day":10,"night":10,"sat":10,"sun":10,"bh":10},"charge":{"day":20,"night":20,"sat":20,"sun":20,"bh":20}}'::jsonb,
  'HEALTHROSTER',true,true,true,true
);
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'aa000000-0000-4000-8000-00000000000d','aa000000-0000-4000-8000-00000000000b',
  'aa000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
  '{"pay":{"day":10,"night":10,"sat":10,"sun":10,"bh":10},"charge":{"day":20,"night":20,"sat":20,"sun":20,"bh":20}}'::jsonb,
  'HEALTHROSTER',true,true,true,true
);
insert into public.pay_advances(
  id,candidate_id,reason,original_amount,outstanding_amount,schedule_json,status,
  case_type,oneoff_bank_details_required
) values (
  'aa000000-0000-4000-8000-000000000010','aa000000-0000-4000-8000-00000000000b',
  'MANUAL_ADVANCE',100.00,100.00,'[]'::jsonb,'ACTIVE','PAYMENT_ADVANCE',false
);

-- ========================================================== static contract
select pg_temp.assert_true(
  (select p.provolatile='s' and p.prosecdef
     and p.proconfig @> array['search_path=public, private, extensions, pg_catalog, pg_temp']
   from pg_catalog.pg_proc p
   join pg_catalog.pg_namespace n on n.oid=p.pronamespace
   where n.nspname='private' and p.proname='weekly_source_freeze_census_v1'),
  'the census must be STABLE, SECURITY DEFINER with a fixed search_path'
);
select pg_temp.assert_true(
  not has_function_privilege('anon',
        'private.weekly_source_freeze_census_v1(uuid,uuid[])','EXECUTE')
  and not has_function_privilege('authenticated',
        'private.weekly_source_freeze_census_v1(uuid,uuid[])','EXECUTE')
  and not has_function_privilege('service_role',
        'private.weekly_source_freeze_census_v1(uuid,uuid[])','EXECUTE'),
  'the census must remain a private owner-only helper'
);
select pg_temp.assert_true(
  (select pg_catalog.lower(pg_temp.census_code())
          !~ 'for +update|for +share|for +no +key +update|for +key +share'),
  'the census must take no row lock on any Banking Pay table'
);
select pg_temp.assert_true(
  (select pg_catalog.lower(pg_temp.census_code())
          !~ 'insert +into|update +public|update +private|delete +from|truncate|lock +table|pg_advisory'),
  'the census must write nothing and take no advisory lock'
);
-- R28 and section 5.2: the one-row-per-Timesheet settled cache is never used.
select pg_temp.assert_true(
  (select pg_catalog.strpos(pg_catalog.lower(pg_temp.census_code()),
          'timesheet_pay_state ')=0
      and pg_catalog.strpos(pg_catalog.lower(pg_temp.census_code()),
          'last_settled_signature')=0),
  'the non-authoritative timesheet_pay_state cache must never be consulted'
);
-- R44 and section 14.1: current Candidate data is never consulted.
select pg_temp.assert_true(
  (select pg_catalog.strpos(pg_catalog.lower(pg_temp.census_code()),
          'public.candidates')=0
      and pg_catalog.strpos(pg_catalog.lower(pg_temp.census_code()),
          'public.umbrellas')=0),
  'the Umbrella rule must read only frozen Draft item evidence'
);
-- Transfers are classified only by the installed adapter.
select pg_temp.assert_true(
  (select pg_catalog.lower(pg_temp.census_code())
          like '%public._pay_rail_state_money_movement_classify%'),
  'transfers must be classified only by the installed rail-state adapter'
);
select pg_temp.assert_true(
  (select pg_temp.census_code()
          !~* 'pg_catalog\.(coalesce|nullif|least|greatest)[[:space:]]*\('
      and pg_temp.census_code()
          !~* '(^|[^[:alnum:]_])(min|max)[[:space:]]*\([[:space:]]*[^)]*uuid'),
  'the census must avoid the prohibited PostgreSQL constructs'
);

-- =============================================== R29 Binding B, whole-batch
-- PROVISIONAL-FIXTURE (WP-16a `pay_batch_abort_failed_draft_create_partial`).
select pg_temp.mk_timesheet('00000029-0000-4000-8000-000000000001','BK-R29',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000029-0000-4000-8000-000000000002','CANCELLED','NOT_SUBMITTED',
                        timestamptz '2026-09-18T11:00:00Z',null,null);
select pg_temp.mk_candidate_row('00000029-0000-4000-8000-000000000003',
        '00000029-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000029-0000-4000-8000-000000000004',
        '00000029-0000-4000-8000-000000000003','00000029-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_operation('00000029-0000-4000-8000-000000000005',
        '00000029-0000-4000-8000-000000000002','DRAFT_CREATE','COMPLETE','COMPLETE',
        null,null,'NONE',null);

create temp table r29 as select pg_temp.census_family('BK-R29') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r29)
  and (select pg_temp.item_class(census,'00000029-0000-4000-8000-000000000004')
       ='VOIDED_TERMINAL' from r29)
  and (select pg_temp.item_binding(census,'00000029-0000-4000-8000-000000000004')='B'
       from r29),
  'R29: an aborted Draft with every family item voided releases under Binding B'
);

-- =============================================== R30 Binding C, then settled
-- PROVISIONAL-FIXTURE (WP-16a `pay_set_paye_net_manual` void inside a Draft).
select pg_temp.mk_timesheet('00000030-0000-4000-8000-000000000001','BK-R30',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000030-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000030-0000-4000-8000-000000000003',
        '00000030-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000030-0000-4000-8000-000000000006',
        '00000030-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000030-0000-4000-8000-000000000004',
        '00000030-0000-4000-8000-000000000003','00000030-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_item('00000030-0000-4000-8000-000000000005',
        '00000030-0000-4000-8000-000000000003','00000030-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000030-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000030-0000-4000-8000-000000000002',
        '00000030-0000-4000-8000-000000000001','sig-r30','PAYE');
-- Open ruling OR-10: the durable artefact the installed `pay_set_paye_net_manual`
-- leaves on the item it voids - its own reservation RELEASED with
-- `released_reason = 'PAYE_NET_REPROJECTION'`.
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status,released_at_utc,released_reason
) values ('00000030-0000-4000-8000-000000000007','aa000000-0000-4000-8000-000000000010',
          '00000030-0000-4000-8000-000000000002','00000030-0000-4000-8000-000000000003',
          '00000030-0000-4000-8000-000000000004',25.00,'RELEASED',
          timestamptz '2026-09-18T09:30:00Z','PAYE_NET_REPROJECTION');

create temp table r30 as select pg_temp.census_family('BK-R30') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r30)
  and (select pg_temp.item_class(census,'00000030-0000-4000-8000-000000000004')
       ='VOIDED_TERMINAL' from r30)
  and (select pg_temp.item_binding(census,'00000030-0000-4000-8000-000000000004')='C'
       from r30)
  and (select pg_temp.item_class(census,'00000030-0000-4000-8000-000000000005')
       ='SETTLED_TERMINAL' from r30),
  'R30: Binding C proves the superseded void once the batch has settled'
);

-- R30 second half: while the Draft is alive the root stays FROZEN.
select pg_temp.mk_timesheet('00000130-0000-4000-8000-000000000001','BK-R30-DRAFT',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000130-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000130-0000-4000-8000-000000000003',
        '00000130-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000130-0000-4000-8000-000000000004',
        '00000130-0000-4000-8000-000000000003','00000130-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status,released_at_utc,released_reason
) values ('00000130-0000-4000-8000-000000000005','aa000000-0000-4000-8000-000000000010',
          '00000130-0000-4000-8000-000000000002','00000130-0000-4000-8000-000000000003',
          '00000130-0000-4000-8000-000000000004',25.00,'RELEASED',
          timestamptz '2026-09-18T09:30:00Z','PAYE_NET_REPROJECTION');

create temp table r30draft as select pg_temp.census_family('BK-R30-DRAFT') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r30draft)
  and (select pg_temp.item_class(census,'00000130-0000-4000-8000-000000000004')='ACTIVE'
       from r30draft),
  'R30: while the Draft is alive the Binding C void is not proved and the root is FROZEN'
);

-- ============================================= R31 unbindable void, terminal
-- PROVISIONAL-FIXTURE (synthetic writer; census file 01 section 4.3).
select pg_temp.mk_timesheet('00000031-0000-4000-8000-000000000001','BK-R31',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000031-0000-4000-8000-000000000002','CANCELLED','NOT_SUBMITTED',
                        timestamptz '2026-09-18T11:00:00Z',null,null);
select pg_temp.mk_candidate_row('00000031-0000-4000-8000-000000000003',
        '00000031-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000031-0000-4000-8000-000000000004',
        '00000031-0000-4000-8000-000000000003','00000031-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
insert into public.pay_payment_correction_requests(
  id,pay_batch_id,correction_kind,status,selection_hash,plan_hash
) values ('00000031-0000-4000-8000-000000000005',
          '00000031-0000-4000-8000-000000000002','PRE_BANK_CANCEL','PROCESSING',
          'sel-r31','plan-r31');

create temp table r31 as select pg_temp.census_family('BK-R31') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from r31)
  and (select pg_temp.item_class(census,'00000031-0000-4000-8000-000000000004')
       ='CENSUS_ERROR' from r31),
  'R31: a voided item in a terminal batch that fits no binding is CENSUS_ERROR, never a silent FROZEN'
);

-- ============================================ R3 partial cancellation remainder
-- PROVISIONAL-FIXTURE (WP-16a partial cancellation with a DRAFT remainder).
select pg_temp.mk_timesheet('00000003-0000-4000-8000-000000000001','BK-R3',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000003-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000003-0000-4000-8000-000000000003',
        '00000003-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000003-0000-4000-8000-000000000004',
        '00000003-0000-4000-8000-000000000003','00000003-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_item('00000003-0000-4000-8000-000000000005',
        '00000003-0000-4000-8000-000000000003','00000003-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
select pg_temp.mk_binding_a('00000003-0000-4000-8000-000000000006',
        '00000003-0000-4000-8000-000000000002','00000003-0000-4000-8000-000000000003',
        '00000003-0000-4000-8000-000000000004','APPLIED',
        '00000003-0000-4000-8000-000000000007','00000003-0000-4000-8000-000000000008');

create temp table r3 as select pg_temp.census_family('BK-R3') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r3)
  and (select pg_temp.item_class(census,'00000003-0000-4000-8000-000000000004')
       ='VOIDED_TERMINAL' from r3)
  and (select pg_temp.item_binding(census,'00000003-0000-4000-8000-000000000004')='A'
       from r3)
  and (select pg_temp.item_class(census,'00000003-0000-4000-8000-000000000005')='ACTIVE'
       from r3)
  and (select pg_temp.has_predicate(census,'C6') from r3),
  'R3: a DRAFT remainder holding a live family item keeps the root FROZEN (C6)'
);

-- ==================================================== R4 partial settlement
-- PROVISIONAL-FIXTURE (WP-16a partial settlement).
select pg_temp.mk_timesheet('00000004-0000-4000-8000-000000000001','BK-R4',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_timesheet('00000004-0000-4000-8000-00000000000b','BK-R4',2,false,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000004-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000004-0000-4000-8000-000000000003',
        '00000004-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000004-0000-4000-8000-000000000004',
        '00000004-0000-4000-8000-000000000003','00000004-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
select pg_temp.mk_item('00000004-0000-4000-8000-000000000005',
        '00000004-0000-4000-8000-000000000003','00000004-0000-4000-8000-00000000000b',
        false,'PAYE',null,null,null);
select pg_temp.mk_settled_proof('00000004-0000-4000-8000-000000000002',
        '00000004-0000-4000-8000-000000000001','sig-r4','PAYE');

create temp table r4 as select pg_temp.census_family('BK-R4') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r4)
  and (select pg_temp.item_class(census,'00000004-0000-4000-8000-000000000004')
       ='SETTLED_TERMINAL' from r4)
  and (select pg_temp.item_class(census,'00000004-0000-4000-8000-000000000005')='ACTIVE'
       from r4),
  'R4: partial settlement keeps the root FROZEN'
);

-- ==================================== R5 provider-unknown / PENDING_NON_FINAL
-- PROVISIONAL-FIXTURE (WP-16a provider-unknown and PENDING_NON_FINAL transfers).
select pg_temp.mk_timesheet('00000005-0000-4000-8000-000000000001','BK-R5',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000005-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000005-0000-4000-8000-000000000003',
        '00000005-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000005-0000-4000-8000-000000000006',
        '00000005-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','PROCESSING',null);
select pg_temp.mk_item('00000005-0000-4000-8000-000000000004',
        '00000005-0000-4000-8000-000000000003','00000005-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000005-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000005-0000-4000-8000-000000000002',
        '00000005-0000-4000-8000-000000000001','sig-r5','PAYE');

create temp table r5 as select pg_temp.census_family('BK-R5') as first_census,
                                pg_temp.census_family('BK-R5') as second_census;
select pg_temp.assert_true(
  (select first_census->>'result'='FROZEN' and second_census->>'result'='FROZEN' from r5)
  and (select pg_temp.has_predicate(first_census,'C5') from r5)
  and (select pg_temp.item_class(first_census,'00000005-0000-4000-8000-000000000004')
       ='ACTIVE' from r5),
  'R5: a PENDING_NON_FINAL transfer keeps the root FROZEN on every repeated census'
);

-- ====================================== R16 SETTLED batch still SCHEDULED
-- PROVISIONAL-FIXTURE (WP-16a `SETTLED` batch retaining `schedule_kind`).
select pg_temp.mk_timesheet('00000016-0000-4000-8000-000000000001','BK-R16',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000016-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z','SCHEDULED');
select pg_temp.mk_candidate_row('00000016-0000-4000-8000-000000000003',
        '00000016-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000016-0000-4000-8000-000000000006',
        '00000016-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000016-0000-4000-8000-000000000004',
        '00000016-0000-4000-8000-000000000003','00000016-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000016-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000016-0000-4000-8000-000000000002',
        '00000016-0000-4000-8000-000000000001','sig-r16','PAYE');

create temp table r16 as select pg_temp.census_family('BK-R16') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r16)
  and (select pg_temp.item_class(census,'00000016-0000-4000-8000-000000000004')
       ='SETTLED_TERMINAL' from r16),
  'R16: schedule_kind SCHEDULED retained on a SETTLED batch must not freeze the root'
);

-- ============================ R17 COMPLETE operation retaining FROZEN scope
-- PROVISIONAL-FIXTURE (WP-16a retained `scope_freeze_status = FROZEN`).
select pg_temp.mk_timesheet('00000017-0000-4000-8000-000000000001','BK-R17',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000017-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000017-0000-4000-8000-000000000003',
        '00000017-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000017-0000-4000-8000-000000000006',
        '00000017-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000017-0000-4000-8000-000000000004',
        '00000017-0000-4000-8000-000000000003','00000017-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000017-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000017-0000-4000-8000-000000000002',
        '00000017-0000-4000-8000-000000000001','sig-r17','PAYE');
select pg_temp.mk_operation('00000017-0000-4000-8000-000000000007',
        '00000017-0000-4000-8000-000000000002','PAYMENT_EXECUTE','COMPLETE','COMPLETE',
        null,null,'FROZEN',null);

create temp table r17 as select pg_temp.census_family('BK-R17') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r17)
  and not (select pg_temp.has_predicate(census,'C4') from r17),
  'R17: a COMPLETE operation still carrying scope_freeze_status FROZEN is history'
);

-- ============================================ R36 future legacy lock_expires
-- PROVISIONAL-FIXTURE (WP-16a terminal operation with a future legacy lock).
select pg_temp.mk_timesheet('00000036-0000-4000-8000-000000000001','BK-R36',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000036-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000036-0000-4000-8000-000000000003',
        '00000036-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000036-0000-4000-8000-000000000006',
        '00000036-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000036-0000-4000-8000-000000000004',
        '00000036-0000-4000-8000-000000000003','00000036-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000036-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000036-0000-4000-8000-000000000002',
        '00000036-0000-4000-8000-000000000001','sig-r36','PAYE');
select pg_temp.mk_operation('00000036-0000-4000-8000-000000000007',
        '00000036-0000-4000-8000-000000000002','PAYMENT_EXECUTE','COMPLETE','COMPLETE',
        null,pg_catalog.clock_timestamp()+interval '1 hour','NONE',null);

create temp table r36 as select pg_temp.census_family('BK-R36') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r36)
  and (select pg_temp.has_predicate(census,'C4') from r36)
  and (select pg_temp.item_class(census,'00000036-0000-4000-8000-000000000004')='ACTIVE'
       from r36),
  'R36: a terminal operation whose legacy lock_expires_at_utc is in the future is live (C4)'
);

-- ============================== R18 ambiguous RETURNED / COMMITTED transfers
-- PROVISIONAL-FIXTURE (WP-16a RETURNED and REVERSED transfers).
select pg_temp.mk_timesheet('00000018-0000-4000-8000-000000000001','BK-R18A',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000018-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000018-0000-4000-8000-000000000003',
        '00000018-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000018-0000-4000-8000-000000000006',
        '00000018-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','RETURNED',null);
select pg_temp.mk_item('00000018-0000-4000-8000-000000000004',
        '00000018-0000-4000-8000-000000000003','00000018-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000018-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000018-0000-4000-8000-000000000002',
        '00000018-0000-4000-8000-000000000001','sig-r18a','PAYE');

select pg_temp.mk_timesheet('00000118-0000-4000-8000-000000000001','BK-R18B',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000118-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000118-0000-4000-8000-000000000003',
        '00000118-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000118-0000-4000-8000-000000000006',
        '00000118-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','UNKNOWN','EXECUTED');
select pg_temp.mk_item('00000118-0000-4000-8000-000000000004',
        '00000118-0000-4000-8000-000000000003','00000118-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000118-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000118-0000-4000-8000-000000000002',
        '00000118-0000-4000-8000-000000000001','sig-r18b','PAYE');

create temp table r18 as select pg_temp.census_family('BK-R18A') as returned_census,
                                pg_temp.census_family('BK-R18B') as executed_census;
select pg_temp.assert_true(
  (select returned_census->>'result'='FROZEN' and executed_census->>'result'='FROZEN'
   from r18)
  and (select pg_temp.has_predicate(returned_census,'C5')
       and pg_temp.has_predicate(executed_census,'C5') from r18),
  'R18: RETURNED and bare EXECUTED provider evidence is ambiguous and stays FROZEN'
);

-- =============================== R19 / R38 APPLIED_WITH_BLOCKERS blockers
-- PROVISIONAL-FIXTURE (WP-16a APPLIED_WITH_BLOCKERS inside and outside the family).
select pg_temp.mk_timesheet('00000019-0000-4000-8000-000000000001','BK-R19A',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_timesheet('00000019-0000-4000-8000-00000000000b','BK-R19-OUT',1,true,
                            'aa000000-0000-4000-8000-00000000000d');
select pg_temp.mk_batch('00000019-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000019-0000-4000-8000-000000000003',
        '00000019-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_candidate_row('00000019-0000-4000-8000-00000000000c',
        '00000019-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',null,false);
select pg_temp.mk_item('00000019-0000-4000-8000-000000000004',
        '00000019-0000-4000-8000-000000000003','00000019-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_item('00000019-0000-4000-8000-00000000000d',
        '00000019-0000-4000-8000-00000000000c','00000019-0000-4000-8000-00000000000b',
        false,'PAYE',null,null,null);
select pg_temp.mk_binding_a('00000019-0000-4000-8000-000000000006',
        '00000019-0000-4000-8000-000000000002','00000019-0000-4000-8000-000000000003',
        '00000019-0000-4000-8000-000000000004','APPLIED_WITH_BLOCKERS',
        '00000019-0000-4000-8000-000000000007','00000019-0000-4000-8000-000000000008');
-- the blocker names an item outside F(root)
insert into public.pay_payment_correction_work_items(
  id,correction_request_id,pay_batch_id,pay_batch_candidate_id,work_kind,
  selection_json,selection_hash,status
) values (
  '00000019-0000-4000-8000-000000000009','00000019-0000-4000-8000-000000000006',
  '00000019-0000-4000-8000-000000000002','00000019-0000-4000-8000-00000000000c',
  'PRE_BANK_CANCEL',
  pg_catalog.jsonb_build_object('expected_pay_batch_item_ids',
    pg_catalog.jsonb_build_array('00000019-0000-4000-8000-00000000000d')),
  'scope-blocker-out','BLOCKED'
);

create temp table r19a as select pg_temp.census_family('BK-R19A') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r19a)
  and (select pg_temp.item_class(census,'00000019-0000-4000-8000-000000000004')
       ='VOIDED_TERMINAL' from r19a)
  and (select pg_temp.item_binding(census,'00000019-0000-4000-8000-000000000004')='A'
       from r19a),
  'R19: APPLIED_WITH_BLOCKERS with every blocker outside the family releases under Binding A'
);

-- R19 second half: one blocker names a family item.
select pg_temp.mk_timesheet('00000119-0000-4000-8000-000000000001','BK-R19B',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000119-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000119-0000-4000-8000-000000000003',
        '00000119-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000119-0000-4000-8000-000000000004',
        '00000119-0000-4000-8000-000000000003','00000119-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_item('00000119-0000-4000-8000-000000000005',
        '00000119-0000-4000-8000-000000000003','00000119-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_binding_a('00000119-0000-4000-8000-000000000006',
        '00000119-0000-4000-8000-000000000002','00000119-0000-4000-8000-000000000003',
        '00000119-0000-4000-8000-000000000004','APPLIED_WITH_BLOCKERS',
        '00000119-0000-4000-8000-000000000007','00000119-0000-4000-8000-000000000008');
insert into public.pay_payment_correction_work_items(
  id,correction_request_id,pay_batch_id,pay_batch_candidate_id,work_kind,
  selection_json,selection_hash,status
) values (
  '00000119-0000-4000-8000-000000000009','00000119-0000-4000-8000-000000000006',
  '00000119-0000-4000-8000-000000000002','00000119-0000-4000-8000-000000000003',
  'PRE_BANK_CANCEL',
  pg_catalog.jsonb_build_object('expected_pay_batch_item_ids',
    pg_catalog.jsonb_build_array('00000119-0000-4000-8000-000000000005')),
  'scope-blocker-in','BLOCKED'
);

create temp table r19b as select pg_temp.census_family('BK-R19B') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r19b)
  and (select pg_temp.item_class(census,'00000119-0000-4000-8000-000000000004')='ACTIVE'
       from r19b),
  'R19: a blocker naming a family item keeps the root FROZEN, never CENSUS_ERROR'
);

-- R38: the blocker carries no exact item identity.
select pg_temp.mk_timesheet('00000038-0000-4000-8000-000000000001','BK-R38',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000038-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000038-0000-4000-8000-000000000003',
        '00000038-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000038-0000-4000-8000-000000000004',
        '00000038-0000-4000-8000-000000000003','00000038-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_binding_a('00000038-0000-4000-8000-000000000006',
        '00000038-0000-4000-8000-000000000002','00000038-0000-4000-8000-000000000003',
        '00000038-0000-4000-8000-000000000004','APPLIED_WITH_BLOCKERS',
        '00000038-0000-4000-8000-000000000007','00000038-0000-4000-8000-000000000008');
insert into public.pay_payment_correction_work_items(
  id,correction_request_id,pay_batch_id,pay_batch_candidate_id,work_kind,
  selection_json,selection_hash,status,result_json
) values (
  '00000038-0000-4000-8000-000000000009','00000038-0000-4000-8000-000000000006',
  '00000038-0000-4000-8000-000000000002',null,'PRE_BANK_CANCEL','{}'::jsonb,
  'scope-blocker-anon','BLOCKED',
  pg_catalog.jsonb_build_object('ok',false,'status','BLOCKED',
    'blocker',pg_catalog.jsonb_build_object('code','SOURCE_SCOPE_CHANGED',
      'message','Candidate scope changed'))
);

create temp table r38 as select pg_temp.census_family('BK-R38') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r38)
  and (select pg_temp.item_class(census,'00000038-0000-4000-8000-000000000004')='ACTIVE'
       from r38),
  'R38: a blocker with no exact item identity keeps the root FROZEN'
);

-- ====================================== R20 / R34 settlement-history conflict
-- PROVISIONAL-FIXTURE (WP-16a two settlement-history rows).
select pg_temp.mk_timesheet('00000020-0000-4000-8000-000000000001','BK-R20',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000020-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000020-0000-4000-8000-000000000003',
        '00000020-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000020-0000-4000-8000-000000000004',
        '00000020-0000-4000-8000-000000000003','00000020-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
select pg_temp.mk_settled_proof('00000020-0000-4000-8000-000000000002',
        '00000020-0000-4000-8000-000000000001','sig-r20','PAYE');
insert into public.timesheet_pay_state_history(
  timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
) values ('00000020-0000-4000-8000-000000000001','00000020-0000-4000-8000-000000000002',
          timestamptz '2026-09-18T10:06:00Z','{"target":true}'::jsonb,'sig-r20-other');

create temp table r20 as select pg_temp.census_family('BK-R20') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from r20)
  and (select pg_temp.item_class(census,'00000020-0000-4000-8000-000000000004')
       ='CENSUS_ERROR' from r20)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'='SETTLEMENT_HISTORY_CONFLICT') from r20),
  'R20: two settlement-history rows with different signatures are SETTLEMENT_HISTORY_CONFLICT'
);

select pg_temp.mk_timesheet('00000034-0000-4000-8000-000000000001','BK-R34',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000034-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000034-0000-4000-8000-000000000003',
        '00000034-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000034-0000-4000-8000-000000000004',
        '00000034-0000-4000-8000-000000000003','00000034-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
select pg_temp.mk_settled_proof('00000034-0000-4000-8000-000000000002',
        '00000034-0000-4000-8000-000000000001','sig-r34','PAYE');
insert into public.timesheet_pay_state_history(
  timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
) values ('00000034-0000-4000-8000-000000000001','00000034-0000-4000-8000-000000000002',
          timestamptz '2026-09-18T10:05:00Z','{"target":true}'::jsonb,'sig-r34');

create temp table r34 as select pg_temp.census_family('BK-R34') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from r34)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'='SETTLEMENT_HISTORY_CONFLICT') from r34),
  'R34: two identical settlement-history rows still fail closed'
);

-- ========================================= R35 settlement-snapshot conflicts
-- PROVISIONAL-FIXTURE (WP-16a missing, empty and conflicting snapshots).
-- (a) history present, snapshot missing
select pg_temp.mk_timesheet('00000035-0000-4000-8000-000000000001','BK-R35A',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000035-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000035-0000-4000-8000-000000000003',
        '00000035-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000035-0000-4000-8000-000000000004',
        '00000035-0000-4000-8000-000000000003','00000035-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
insert into public.timesheet_pay_state_history(
  timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
) values ('00000035-0000-4000-8000-000000000001','00000035-0000-4000-8000-000000000002',
          timestamptz '2026-09-18T10:05:00Z','{"target":true}'::jsonb,'sig-r35a');

-- (b) chosen snapshot has an empty signature
select pg_temp.mk_timesheet('00000135-0000-4000-8000-000000000001','BK-R35B',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000135-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000135-0000-4000-8000-000000000003',
        '00000135-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000135-0000-4000-8000-000000000004',
        '00000135-0000-4000-8000-000000000003','00000135-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
select pg_temp.mk_settled_proof('00000135-0000-4000-8000-000000000002',
        '00000135-0000-4000-8000-000000000001','','PAYE');

-- (c) a second snapshot row with a different signature
select pg_temp.mk_timesheet('00000235-0000-4000-8000-000000000001','BK-R35C',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000235-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000235-0000-4000-8000-000000000003',
        '00000235-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000235-0000-4000-8000-000000000004',
        '00000235-0000-4000-8000-000000000003','00000235-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
select pg_temp.mk_settled_proof('00000235-0000-4000-8000-000000000002',
        '00000235-0000-4000-8000-000000000001','sig-r35c','PAYE');
insert into public.pay_batch_timesheet_snapshots(
  pay_batch_id,timesheet_id,candidate_id,pay_channel,
  base_snapshot_json,target_snapshot_json,signature
) values ('00000235-0000-4000-8000-000000000002','00000235-0000-4000-8000-000000000001',
          'aa000000-0000-4000-8000-00000000000a','UMBRELLA',
          '{"base":true}'::jsonb,'{"target":true}'::jsonb,'sig-r35c-other');

-- (d) history signature does not equal the chosen snapshot signature
select pg_temp.mk_timesheet('00000335-0000-4000-8000-000000000001','BK-R35D',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000335-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000335-0000-4000-8000-000000000003',
        '00000335-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000335-0000-4000-8000-000000000004',
        '00000335-0000-4000-8000-000000000003','00000335-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
insert into public.pay_batch_timesheet_snapshots(
  pay_batch_id,timesheet_id,candidate_id,pay_channel,
  base_snapshot_json,target_snapshot_json,signature
) values ('00000335-0000-4000-8000-000000000002','00000335-0000-4000-8000-000000000001',
          'aa000000-0000-4000-8000-00000000000a','PAYE',
          '{"base":true}'::jsonb,'{"target":true}'::jsonb,'sig-r35d-snapshot');
insert into public.timesheet_pay_state_history(
  timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
) values ('00000335-0000-4000-8000-000000000001','00000335-0000-4000-8000-000000000002',
          timestamptz '2026-09-18T10:05:00Z','{"target":true}'::jsonb,'sig-r35d-history');

create temp table r35 as
select pg_temp.census_family('BK-R35A') as missing_census,
       pg_temp.census_family('BK-R35B') as empty_census,
       pg_temp.census_family('BK-R35C') as conflicting_census,
       pg_temp.census_family('BK-R35D') as mismatch_census;
select pg_temp.assert_true(
  (select missing_census->>'result'='CENSUS_ERROR'
      and empty_census->>'result'='CENSUS_ERROR'
      and conflicting_census->>'result'='CENSUS_ERROR'
      and mismatch_census->>'result'='CENSUS_ERROR' from r35)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(missing_census->'items') as census_item
         where census_item->>'reason'='SETTLEMENT_SNAPSHOT_CONFLICT') from r35)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(mismatch_census->'items') as census_item
         where census_item->>'reason'='SETTLEMENT_SNAPSHOT_CONFLICT') from r35),
  'R35: a missing, empty, conflicting or mismatched settlement snapshot is SETTLEMENT_SNAPSHOT_CONFLICT'
);

-- ======================== R21 evidence on an earlier version of the family
-- PROVISIONAL-FIXTURE (WP-16a rotated family with an older version).
select pg_temp.mk_timesheet('00000021-0000-4000-8000-000000000001','BK-R21',1,false,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_timesheet('00000021-0000-4000-8000-00000000000a','BK-R21',2,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000021-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000021-0000-4000-8000-000000000003',
        '00000021-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000021-0000-4000-8000-000000000006',
        '00000021-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000021-0000-4000-8000-000000000004',
        '00000021-0000-4000-8000-000000000003','00000021-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000021-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000021-0000-4000-8000-000000000002',
        '00000021-0000-4000-8000-000000000001','sig-r21','PAYE');

-- the caller supplies only the canonical current row; the census resolves the family
create temp table r21 as select pg_temp.census(
  array['00000021-0000-4000-8000-00000000000a'::uuid]) as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r21)
  and (select pg_temp.item_class(census,'00000021-0000-4000-8000-000000000004')
       ='SETTLED_TERMINAL' from r21)
  and (select (census->'expanded_member_timesheet_ids') ?
       '00000021-0000-4000-8000-000000000001' from r21),
  'R21: payment evidence on an earlier family version is enumerated and proved'
);

-- ========================================== R28 root settled in two batches
-- PROVISIONAL-FIXTURE (WP-16a root settled in two batches).
select pg_temp.mk_timesheet('00000028-0000-4000-8000-000000000001','BK-R28',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000028-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_batch('00000028-0000-4000-8000-00000000000a','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-25T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000028-0000-4000-8000-000000000003',
        '00000028-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_candidate_row('00000028-0000-4000-8000-00000000000b',
        '00000028-0000-4000-8000-00000000000a','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000028-0000-4000-8000-000000000006',
        '00000028-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_transfer('00000028-0000-4000-8000-00000000000c',
        '00000028-0000-4000-8000-00000000000a','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000028-0000-4000-8000-000000000004',
        '00000028-0000-4000-8000-000000000003','00000028-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000028-0000-4000-8000-000000000006',null);
select pg_temp.mk_item('00000028-0000-4000-8000-00000000000d',
        '00000028-0000-4000-8000-00000000000b','00000028-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000028-0000-4000-8000-00000000000c',null);
select pg_temp.mk_settled_proof('00000028-0000-4000-8000-000000000002',
        '00000028-0000-4000-8000-000000000001','sig-r28-first','PAYE');
select pg_temp.mk_settled_proof('00000028-0000-4000-8000-00000000000a',
        '00000028-0000-4000-8000-000000000001','sig-r28-second','PAYE');

create temp table r28 as select pg_temp.census_family('BK-R28') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r28)
  and (select (census->'class_counts'->>'SETTLED_TERMINAL')::integer=2 from r28)
  and (select pg_catalog.jsonb_array_length(census->'proof')=2 from r28),
  'R28: a root settled in two batches proves each (timesheet, batch) pair on its own signature'
);

-- ================================= R41 one Candidate cancelled out of a Draft
-- PROVISIONAL-FIXTURE (WP-16a Binding A with a live and a settled remainder).
select pg_temp.mk_timesheet('00000041-0000-4000-8000-000000000001','BK-R41A',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_timesheet('00000041-0000-4000-8000-00000000000b','BK-R41-OTHER',1,true,
                            'aa000000-0000-4000-8000-00000000000d');
select pg_temp.mk_batch('00000041-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000041-0000-4000-8000-000000000003',
        '00000041-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_candidate_row('00000041-0000-4000-8000-00000000000c',
        '00000041-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',null,false);
select pg_temp.mk_item('00000041-0000-4000-8000-000000000004',
        '00000041-0000-4000-8000-000000000003','00000041-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_transfer('00000041-0000-4000-8000-00000000000e',
        '00000041-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',
        null,'PAYE','PROCESSING',null);
select pg_temp.mk_item('00000041-0000-4000-8000-00000000000d',
        '00000041-0000-4000-8000-00000000000c','00000041-0000-4000-8000-00000000000b',
        false,'PAYE',null,'00000041-0000-4000-8000-00000000000e',null);
-- Review finding F4: predicate C2 is now the literal `proof/32 §4.3` wording, so
-- another Candidate's RESERVED reservation in this batch WOULD freeze this root.
-- `R41` names only the other Candidates' transfers, so this fixture carries only
-- those; the literal C2 behaviour is pinned by its own scenario below.
select pg_temp.mk_binding_a('00000041-0000-4000-8000-000000000006',
        '00000041-0000-4000-8000-000000000002','00000041-0000-4000-8000-000000000003',
        '00000041-0000-4000-8000-000000000004','APPLIED',
        '00000041-0000-4000-8000-000000000007','00000041-0000-4000-8000-000000000008');

create temp table r41a as select pg_temp.census_family('BK-R41A') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r41a)
  and (select pg_temp.item_binding(census,'00000041-0000-4000-8000-000000000004')='A'
       from r41a)
  and not (select pg_temp.has_predicate(census,'C5') from r41a),
  'R41: another Candidate''s live transfer in the same Draft is outside the family transfer scope'
);

select pg_temp.mk_timesheet('00000141-0000-4000-8000-000000000001','BK-R41B',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_timesheet('00000141-0000-4000-8000-00000000000b','BK-R41B-OTHER',1,true,
                            'aa000000-0000-4000-8000-00000000000d');
select pg_temp.mk_batch('00000141-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000141-0000-4000-8000-000000000003',
        '00000141-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_candidate_row('00000141-0000-4000-8000-00000000000c',
        '00000141-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',
        'SETTLED',true);
select pg_temp.mk_item('00000141-0000-4000-8000-000000000004',
        '00000141-0000-4000-8000-000000000003','00000141-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_transfer('00000141-0000-4000-8000-00000000000e',
        '00000141-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',
        null,'PAYE','FAILED',null);
select pg_temp.mk_item('00000141-0000-4000-8000-00000000000d',
        '00000141-0000-4000-8000-00000000000c','00000141-0000-4000-8000-00000000000b',
        false,'PAYE',null,'00000141-0000-4000-8000-00000000000e',null);
select pg_temp.mk_binding_a('00000141-0000-4000-8000-000000000006',
        '00000141-0000-4000-8000-000000000002','00000141-0000-4000-8000-000000000003',
        '00000141-0000-4000-8000-000000000004','APPLIED',
        '00000141-0000-4000-8000-000000000007','00000141-0000-4000-8000-000000000008');

create temp table r41b as select pg_temp.census_family('BK-R41B') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r41b)
  and (select pg_temp.item_binding(census,'00000141-0000-4000-8000-000000000004')='A'
       from r41b),
  'R41: the same voided item still releases after the remainder batch has settled'
);

-- ================================ R43 batch completed with failed payments
-- PROVISIONAL-FIXTURE (WP-16a `FAILED` batch with mixed per-Candidate outcomes).
select pg_temp.mk_timesheet('00000043-0000-4000-8000-000000000001','BK-R43A',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000043-0000-4000-8000-000000000002','FAILED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000043-0000-4000-8000-000000000003',
        '00000043-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_candidate_row('00000043-0000-4000-8000-00000000000c',
        '00000043-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',
        'FAILED',false);
select pg_temp.mk_transfer('00000043-0000-4000-8000-000000000006',
        '00000043-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_transfer('00000043-0000-4000-8000-00000000000e',
        '00000043-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',
        null,'PAYE','FAILED',null);
select pg_temp.mk_item('00000043-0000-4000-8000-000000000004',
        '00000043-0000-4000-8000-000000000003','00000043-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000043-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000043-0000-4000-8000-000000000002',
        '00000043-0000-4000-8000-000000000001','sig-r43','PAYE');

create temp table r43a as select pg_temp.census_family('BK-R43A') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r43a)
  and (select pg_temp.item_class(census,'00000043-0000-4000-8000-000000000004')
       ='SETTLED_TERMINAL' from r43a),
  'R43: a completed-with-failures FAILED batch is terminal and the settled Candidate releases'
);

-- R43 second half: this Candidate's own row failed.
select pg_temp.mk_timesheet('00000143-0000-4000-8000-000000000001','BK-R43B',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000143-0000-4000-8000-000000000002','FAILED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000143-0000-4000-8000-000000000003',
        '00000143-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'FAILED',false);
select pg_temp.mk_item('00000143-0000-4000-8000-000000000004',
        '00000143-0000-4000-8000-000000000003','00000143-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);

create temp table r43b as select pg_temp.census_family('BK-R43B') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r43b)
  and (select pg_temp.item_class(census,'00000143-0000-4000-8000-000000000004')='ACTIVE'
       from r43b),
  'R43: a failed Candidate in a terminal batch is ACTIVE and FROZEN, never CENSUS_ERROR'
);

-- =================================== R44 batch-level Umbrella transfer rule
-- PROVISIONAL-FIXTURE (WP-16a Umbrella transfers with a null candidate_id).
-- (a) matched through the frozen item Umbrella and pay channel
select pg_temp.mk_timesheet('00000044-0000-4000-8000-000000000001','BK-R44A',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000044-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000044-0000-4000-8000-000000000003',
        '00000044-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000044-0000-4000-8000-000000000004',
        '00000044-0000-4000-8000-000000000003','00000044-0000-4000-8000-000000000001',
        false,'UMBRELLA','aa000000-0000-4000-8000-00000000000e',null,null);
select pg_temp.mk_settled_proof('00000044-0000-4000-8000-000000000002',
        '00000044-0000-4000-8000-000000000001','sig-r44a','UMBRELLA');
select pg_temp.mk_transfer('00000044-0000-4000-8000-000000000006',
        '00000044-0000-4000-8000-000000000002',null,
        'aa000000-0000-4000-8000-00000000000e','UMBRELLA','PROCESSING',null);

create temp table r44a as select pg_temp.census_family('BK-R44A') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r44a)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'predicates') as predicate_row
         where predicate_row->>'predicate'='C5'
           and predicate_row->'detail'->>'scope_kind'='BATCH_UMBRELLA'
           and predicate_row->'detail'->>'pay_bank_transfer_id'
               ='00000044-0000-4000-8000-000000000006') from r44a),
  'R44: a matched null-Candidate Umbrella transfer is a conservative freeze signal'
);

-- (b) contradictory frozen item evidence
select pg_temp.mk_timesheet('00000144-0000-4000-8000-000000000001','BK-R44B',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000144-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000144-0000-4000-8000-000000000003',
        '00000144-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000144-0000-4000-8000-000000000004',
        '00000144-0000-4000-8000-000000000003','00000144-0000-4000-8000-000000000001',
        false,'PAYE','aa000000-0000-4000-8000-00000000000e',null,null);
select pg_temp.mk_settled_proof('00000144-0000-4000-8000-000000000002',
        '00000144-0000-4000-8000-000000000001','sig-r44b','PAYE');
select pg_temp.mk_transfer('00000144-0000-4000-8000-000000000006',
        '00000144-0000-4000-8000-000000000002',null,
        'aa000000-0000-4000-8000-00000000000e','UMBRELLA','COMPLETED',null);

-- (c) missing frozen item evidence
select pg_temp.mk_timesheet('00000244-0000-4000-8000-000000000001','BK-R44C',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000244-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000244-0000-4000-8000-000000000003',
        '00000244-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000244-0000-4000-8000-000000000004',
        '00000244-0000-4000-8000-000000000003','00000244-0000-4000-8000-000000000001',
        false,'UMBRELLA',null,null,null);
select pg_temp.mk_settled_proof('00000244-0000-4000-8000-000000000002',
        '00000244-0000-4000-8000-000000000001','sig-r44c','UMBRELLA');
select pg_temp.mk_transfer('00000244-0000-4000-8000-000000000006',
        '00000244-0000-4000-8000-000000000002',null,
        'aa000000-0000-4000-8000-00000000000e','UMBRELLA','COMPLETED',null);

create temp table r44bc as select pg_temp.census_family('BK-R44B') as contradictory_census,
                                  pg_temp.census_family('BK-R44C') as missing_census;
select pg_temp.assert_true(
  (select contradictory_census->>'result'='CENSUS_ERROR' from r44bc)
  and (select pg_temp.has_error(contradictory_census,
        'WEEKLY_SOURCE_CENSUS_UMBRELLA_EVIDENCE_CONTRADICTORY') from r44bc)
  and (select missing_census->>'result'='CENSUS_ERROR' from r44bc)
  and (select pg_temp.has_error(missing_census,
        'WEEKLY_SOURCE_CENSUS_UMBRELLA_EVIDENCE_MISSING') from r44bc),
  'R44: missing or contradictory frozen item evidence is CENSUS_ERROR, never a fallback'
);

-- (d) the Candidate's current Umbrella is changed afterwards: nothing changes.
update public.candidates set umbrella_id='aa000000-0000-4000-8000-00000000000f'
where id='aa000000-0000-4000-8000-00000000000a';
create temp table r44d as select pg_temp.census_family('BK-R44A') as census;
select pg_temp.assert_true(
  -- `family_split_scan` is a per-call diagnostic whose `max_rows_examined`
  -- counts the sibling rows in the member's Contract-week, and this verifier
  -- keeps adding fixtures to the same Contract-week between the two censuses,
  -- so it is excluded along with the clock.  Everything that decides money -
  -- result, items, proof, predicates, errors - must be identical.
  (select (r44d.census-'evaluated_at_utc'-'family_split_scan')
          =(r44a.census-'evaluated_at_utc'-'family_split_scan')
   from r44d,r44a),
  'R44: changing the Candidate''s current Umbrella cannot change the census'
);

-- ================================ section 4.2 class 1 and C2 completeness
-- (a) a family with no Banking Pay evidence at all releases.
select pg_temp.mk_timesheet('00000050-0000-4000-8000-000000000001','BK-NOEVIDENCE',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
create temp table no_evidence as select pg_temp.census_family('BK-NOEVIDENCE') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from no_evidence)
  and (select pg_catalog.jsonb_array_length(census->'items')=0 from no_evidence),
  'a family carrying no Banking Pay evidence is releasable'
);

-- (b) a foreign candidate row is a missing/foreign join.
select pg_temp.mk_timesheet('00000051-0000-4000-8000-000000000001','BK-FOREIGN',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000051-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000051-0000-4000-8000-000000000003',
        '00000051-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',
        'SETTLED',true);
select pg_temp.mk_item('00000051-0000-4000-8000-000000000004',
        '00000051-0000-4000-8000-000000000003','00000051-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
create temp table foreign_join as select pg_temp.census_family('BK-FOREIGN') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from foreign_join)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'='WEEKLY_SOURCE_CENSUS_ITEM_JOIN_INVALID')
       from foreign_join),
  'a candidate row belonging to another Candidate is a foreign join and fails closed'
);

-- (c) a non-voided item inside a CANCELLED batch contradicts the cancellation owner.
select pg_temp.mk_timesheet('00000052-0000-4000-8000-000000000001','BK-LIVEINCANCELLED',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000052-0000-4000-8000-000000000002','CANCELLED','NOT_SUBMITTED',
                        timestamptz '2026-09-18T11:00:00Z',null,null);
select pg_temp.mk_candidate_row('00000052-0000-4000-8000-000000000003',
        '00000052-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000052-0000-4000-8000-000000000004',
        '00000052-0000-4000-8000-000000000003','00000052-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
create temp table live_in_cancelled as
  select pg_temp.census_family('BK-LIVEINCANCELLED') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from live_in_cancelled)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'
               ='WEEKLY_SOURCE_CENSUS_ACTIVE_ITEM_IN_CANCELLED_BATCH')
       from live_in_cancelled),
  'a non-voided item in a CANCELLED batch is CENSUS_ERROR'
);

-- (d) C2: an active reservation on the family's own item freezes the root.
select pg_temp.mk_timesheet('00000053-0000-4000-8000-000000000001','BK-C2',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000053-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000053-0000-4000-8000-000000000003',
        '00000053-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000053-0000-4000-8000-000000000004',
        '00000053-0000-4000-8000-000000000003','00000053-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
select pg_temp.mk_settled_proof('00000053-0000-4000-8000-000000000002',
        '00000053-0000-4000-8000-000000000001','sig-c2','PAYE');
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status
) values ('00000053-0000-4000-8000-000000000005','aa000000-0000-4000-8000-000000000010',
          '00000053-0000-4000-8000-000000000002','00000053-0000-4000-8000-000000000003',
          '00000053-0000-4000-8000-000000000004',25.00,'COMMITTED');
create temp table c2_case as select pg_temp.census_family('BK-C2') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from c2_case)
  and (select pg_temp.has_predicate(census,'C2') from c2_case)
  and (select pg_temp.item_class(census,'00000053-0000-4000-8000-000000000004')='ACTIVE'
       from c2_case),
  'C2: a RESERVED or COMMITTED reservation on a family item keeps the root FROZEN'
);

-- ============================ review findings F1, F3, F4, F6, F7, F9, OR-10
-- Every scenario below was added in answer to the independent review.

-- F1(a). A member Timesheet that EXISTS but whose booking identity is blank is
-- dropped by the installed resolver, which returns no row at all for it.  The
-- reviewer's probe: a two-root bundle whose second root holds a live DRAFT item.
select pg_temp.mk_timesheet('00000060-0000-4000-8000-000000000001','BK-F1-CLEAN',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000060-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000060-0000-4000-8000-000000000003',
        '00000060-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000060-0000-4000-8000-000000000006',
        '00000060-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000060-0000-4000-8000-000000000004',
        '00000060-0000-4000-8000-000000000003','00000060-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000060-0000-4000-8000-000000000006',null);
select pg_temp.mk_settled_proof('00000060-0000-4000-8000-000000000002',
        '00000060-0000-4000-8000-000000000001','sig-f1','PAYE');
select pg_temp.mk_timesheet('00000060-0000-4000-8000-00000000000a','   ',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000060-0000-4000-8000-00000000000b','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000060-0000-4000-8000-00000000000c',
        '00000060-0000-4000-8000-00000000000b','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000060-0000-4000-8000-00000000000d',
        '00000060-0000-4000-8000-00000000000c','00000060-0000-4000-8000-00000000000a',
        false,'PAYE',null,null,null);

select pg_temp.assert_true(
  (select pg_catalog.count(*)=0
   from public._pay_timesheet_rotation_scope(
          array['00000060-0000-4000-8000-00000000000a'::uuid])),
  'F1 premise: the installed resolver returns no row for a blank booking identity'
);
create temp table f1 as select pg_temp.census(array[
  '00000060-0000-4000-8000-000000000001'::uuid,
  '00000060-0000-4000-8000-00000000000a'::uuid]) as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from f1)
  and (select pg_temp.has_error(census,'WEEKLY_SOURCE_CENSUS_MEMBER_NOT_RESOLVED')
       from f1)
  and (select pg_temp.has_error(census,'WEEKLY_SOURCE_CENSUS_BOOKING_IDENTITY_BLANK')
       from f1),
  'F1: a member the resolver drops can never leave the bundle RELEASABLE'
);
create temp table f1b as select pg_temp.census(
  array['00000060-0000-4000-8000-00000000000a'::uuid]) as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from f1b),
  'F1: the same holds when the blank-booking member is supplied alone'
);

-- F3. A sibling whose booking identity differs only by surrounding whitespace is
-- a different family to the installed resolver, so the family is ambiguous.
select pg_temp.mk_timesheet('00000061-0000-4000-8000-000000000001','BK-F3',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_timesheet('00000061-0000-4000-8000-00000000000a',' BK-F3',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000061-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000061-0000-4000-8000-000000000003',
        '00000061-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000061-0000-4000-8000-000000000004',
        '00000061-0000-4000-8000-000000000003','00000061-0000-4000-8000-00000000000a',
        false,'PAYE',null,null,null);
create temp table f3 as select pg_temp.census(
  array['00000061-0000-4000-8000-000000000001'::uuid]) as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from f3)
  and (select pg_temp.has_error(census,'WEEKLY_SOURCE_CENSUS_FAMILY_BOOKING_SPLIT')
       from f3),
  'F3: a whitespace-padded sibling booking identity makes the family ambiguous'
);

-- F6 / F1. A member Timesheet whose Contract belongs to another Candidate, in a
-- family that carries no Banking Pay evidence at all.
select pg_temp.mk_timesheet('00000062-0000-4000-8000-000000000001','BK-F6',1,true,
                            'aa000000-0000-4000-8000-00000000000d');
create temp table f6 as select pg_temp.census(
  array['00000062-0000-4000-8000-000000000001'::uuid]) as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from f6)
  and (select pg_temp.has_error(census,'WEEKLY_SOURCE_CENSUS_MEMBER_CANDIDATE_MISMATCH')
       from f6),
  'F6: a member belonging to another Candidate fails closed even with zero items'
);

-- G3. The second ownership source: a member whose Contract carries no Candidate
-- at all, but whose current `timesheets_financials` row names another Candidate.
-- The Contract chain cannot resolve it; the financial row can, and it fails closed.
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'aa000000-0000-4000-8000-000000000011',null,
  'aa000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
  '{"pay":{"day":10,"night":10,"sat":10,"sun":10,"bh":10},"charge":{"day":20,"night":20,"sat":20,"sun":20,"bh":20}}'::jsonb,
  'HEALTHROSTER',true,true,true,true
);
select pg_temp.mk_timesheet('00000069-0000-4000-8000-000000000001','BK-G3',1,true,
                            'aa000000-0000-4000-8000-000000000011');
-- The installed `trg_tsfin_ai` trigger does not create a row for a bare seeded
-- Timesheet, so the current financial row is seeded here in the shape the
-- installed writers produce.
insert into public.timesheets_financials(
  timesheet_id,timesheet_version,candidate_id,is_current
) values ('00000069-0000-4000-8000-000000000001',1,
          'aa000000-0000-4000-8000-00000000000b',true);
create temp table g3 as select pg_temp.census_family('BK-G3') as census;
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.timesheets_financials
   where timesheet_id='00000069-0000-4000-8000-000000000001'
     and candidate_id='aa000000-0000-4000-8000-00000000000b' and is_current),
  'G3 premise: the Timesheet has a current financial row naming the other Candidate'
);
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from g3)
  and (select pg_temp.has_error(census,'WEEKLY_SOURCE_CENSUS_MEMBER_CANDIDATE_MISMATCH')
       from g3),
  'G3: the current financial row is a second authoritative Candidate link and fails closed'
);

-- F4. Predicate C2 at its literal `proof/32 §4.3` width: a RESERVED reservation
-- that reaches the family only through the batch freezes the root.
select pg_temp.mk_timesheet('00000063-0000-4000-8000-000000000001','BK-F4',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_timesheet('00000063-0000-4000-8000-00000000000a','BK-F4-OTHER',1,true,
                            'aa000000-0000-4000-8000-00000000000d');
select pg_temp.mk_batch('00000063-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000063-0000-4000-8000-000000000003',
        '00000063-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_candidate_row('00000063-0000-4000-8000-00000000000c',
        '00000063-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',null,false);
select pg_temp.mk_item('00000063-0000-4000-8000-000000000004',
        '00000063-0000-4000-8000-000000000003','00000063-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_item('00000063-0000-4000-8000-00000000000d',
        '00000063-0000-4000-8000-00000000000c','00000063-0000-4000-8000-00000000000a',
        false,'PAYE',null,null,null);
select pg_temp.mk_binding_a('00000063-0000-4000-8000-000000000006',
        '00000063-0000-4000-8000-000000000002','00000063-0000-4000-8000-000000000003',
        '00000063-0000-4000-8000-000000000004','APPLIED',
        '00000063-0000-4000-8000-000000000007','00000063-0000-4000-8000-000000000008');
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status
) values ('00000063-0000-4000-8000-00000000000f','aa000000-0000-4000-8000-000000000010',
          '00000063-0000-4000-8000-000000000002','00000063-0000-4000-8000-00000000000c',
          '00000063-0000-4000-8000-00000000000d',50.00,'RESERVED');
create temp table f4 as select pg_temp.census_family('BK-F4') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from f4)
  and (select pg_temp.has_predicate(census,'C2') from f4),
  'F4: literal C2 - a RESERVED reservation anywhere in a family batch freezes the root'
);

-- OR-10 (F2). `R31` in the two containers the delivered verifier did not cover:
-- an uncorroborated voided item in a terminal SETTLED batch, and in a
-- completed-FAILED COMMITTED batch.  Under the literal Binding C both would be
-- VOIDED_TERMINAL/C and the bundle would publish.
select pg_temp.mk_timesheet('00000064-0000-4000-8000-000000000001','BK-OR10-SETTLED',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000064-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000064-0000-4000-8000-000000000003',
        '00000064-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000064-0000-4000-8000-000000000004',
        '00000064-0000-4000-8000-000000000003','00000064-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
create temp table or10_settled as
  select pg_temp.census_family('BK-OR10-SETTLED') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from or10_settled)
  and (select pg_temp.item_class(census,'00000064-0000-4000-8000-000000000004')
       ='CENSUS_ERROR' from or10_settled)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'='WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE')
       from or10_settled),
  'OR-10: an uncorroborated void in a SETTLED committed batch is CENSUS_ERROR, not Binding C'
);

select pg_temp.mk_timesheet('00000065-0000-4000-8000-000000000001','BK-OR10-FAILED',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000065-0000-4000-8000-000000000002','FAILED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000065-0000-4000-8000-000000000003',
        '00000065-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000065-0000-4000-8000-000000000004',
        '00000065-0000-4000-8000-000000000003','00000065-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
create temp table or10_failed as
  select pg_temp.census_family('BK-OR10-FAILED') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from or10_failed),
  'OR-10: the same holds in a completed-with-failures FAILED committed batch'
);

-- OR-8 (F9). The delivered verifier had no scenario that distinguished the two
-- readings of an unbindable void whose evidence is still in flight.  This one
-- pins the implemented reading: FROZEN, never CENSUS_ERROR.
select pg_temp.mk_timesheet('00000066-0000-4000-8000-000000000001','BK-OR8',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000066-0000-4000-8000-000000000002','CANCELLED','NOT_SUBMITTED',
                        timestamptz '2026-09-18T11:00:00Z',null,null);
select pg_temp.mk_candidate_row('00000066-0000-4000-8000-000000000003',
        '00000066-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000066-0000-4000-8000-000000000004',
        '00000066-0000-4000-8000-000000000003','00000066-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_operation('00000066-0000-4000-8000-000000000005',
        '00000066-0000-4000-8000-000000000002','PAYMENT_CORRECTION','RUNNING',
        'REFRESH_WORKBENCH',null,null,'NONE',null);
create temp table or8 as select pg_temp.census_family('BK-OR8') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from or8)
  and (select pg_temp.item_class(census,'00000066-0000-4000-8000-000000000004')='ACTIVE'
       from or8)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'='WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED')
       from or8)
  and (select pg_temp.has_predicate(census,'C4') from or8),
  'OR-8: a mid-flight cancellation is FROZEN and self-healing, never CENSUS_ERROR'
);

-- F7 / G1. Two terminal correction requests name the same item. A second, clean
-- request must NOT rescue an item that another terminal request disqualifies:
-- `proof/32 §5.1` Binding A and `R38` say a blocker naming a family item, or one
-- with no exact item identity, leaves the root FROZEN, and producing a cleaner
-- request elsewhere does not make that blocker go away.
select pg_temp.mk_timesheet('00000067-0000-4000-8000-000000000001','BK-F7',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_timesheet('00000067-0000-4000-8000-00000000000a','BK-F7-OTHER',1,true,
                            'aa000000-0000-4000-8000-00000000000d');
select pg_temp.mk_batch('00000067-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000067-0000-4000-8000-000000000003',
        '00000067-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_candidate_row('00000067-0000-4000-8000-00000000000c',
        '00000067-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000b',null,false);
select pg_temp.mk_item('00000067-0000-4000-8000-000000000004',
        '00000067-0000-4000-8000-000000000003','00000067-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_item('00000067-0000-4000-8000-00000000000d',
        '00000067-0000-4000-8000-00000000000c','00000067-0000-4000-8000-00000000000a',
        false,'PAYE',null,null,null);
-- the LOWER request id carries a blocker naming the family item itself
select pg_temp.mk_binding_a('00000067-0000-4000-8000-000000000005',
        '00000067-0000-4000-8000-000000000002','00000067-0000-4000-8000-000000000003',
        '00000067-0000-4000-8000-000000000004','APPLIED_WITH_BLOCKERS',
        '00000067-0000-4000-8000-000000000007','00000067-0000-4000-8000-000000000008');
insert into public.pay_payment_correction_work_items(
  id,correction_request_id,pay_batch_id,pay_batch_candidate_id,work_kind,
  selection_json,selection_hash,status
) values (
  '00000067-0000-4000-8000-000000000009','00000067-0000-4000-8000-000000000005',
  '00000067-0000-4000-8000-000000000002','00000067-0000-4000-8000-000000000003',
  'PRE_BANK_CANCEL',
  pg_catalog.jsonb_build_object('expected_pay_batch_item_ids',
    pg_catalog.jsonb_build_array('00000067-0000-4000-8000-000000000004')),
  'scope-f7-in','BLOCKED'
);
-- The HIGHER request id is clean.  Built without a second
-- `pay_payment_correction_items` row, because the installed
-- `pay_payment_correction_items_applied_item_kind_uidx` permits only one APPLIED
-- row per (item, kind); the work item's own
-- `result_json.changed_scope_json.changed_pay_batch_item_ids` is the other shape
-- the installed apply owners write and is sufficient on its own.
insert into public.pay_payment_correction_requests(
  id,pay_batch_id,correction_kind,status,selection_hash,plan_hash
) values ('00000067-0000-4000-8000-00000000000e',
          '00000067-0000-4000-8000-000000000002','PRE_BANK_CANCEL','APPLIED',
          'sel-f7-clean','plan-f7-clean');
insert into public.pay_payment_correction_request_candidates(
  correction_request_id,selection_ordinal,pay_batch_candidate_id,
  candidate_scope_hash,active_item_count,source_row_count,active_amount,
  pay_batch_item_ids,eligibility_code_at_plan
) values ('00000067-0000-4000-8000-00000000000e',1,
          '00000067-0000-4000-8000-000000000003',
          encode(sha256(convert_to('f7-clean','UTF8')),'hex'),1,1,100.00,
          array['00000067-0000-4000-8000-000000000004'::uuid],'ELIGIBLE');
insert into public.pay_payment_correction_work_items(
  id,correction_request_id,pay_batch_id,pay_batch_candidate_id,work_kind,
  selection_json,selection_hash,status,result_json
) values (
  '00000067-0000-4000-8000-00000000000f','00000067-0000-4000-8000-00000000000e',
  '00000067-0000-4000-8000-000000000002','00000067-0000-4000-8000-000000000003',
  'PRE_BANK_CANCEL',
  pg_catalog.jsonb_build_object('expected_pay_batch_item_ids',
    pg_catalog.jsonb_build_array('00000067-0000-4000-8000-000000000004')),
  encode(sha256(convert_to('f7-clean','UTF8')),'hex'),'APPLIED',
  pg_catalog.jsonb_build_object('ok',true,'status','APPLIED',
    'changed_scope_json',pg_catalog.jsonb_build_object(
      'pay_batch_id','00000067-0000-4000-8000-000000000002',
      'correction_request_id','00000067-0000-4000-8000-00000000000e',
      'change_kind','PRE_BANK_CANCEL',
      'changed_pay_batch_item_ids',
        pg_catalog.jsonb_build_array('00000067-0000-4000-8000-000000000004')))
);
select pg_temp.mk_operation('00000067-0000-4000-8000-000000000010',
        '00000067-0000-4000-8000-000000000002','PAYMENT_CORRECTION','COMPLETE','COMPLETE',
        null,null,'NONE',
        pg_catalog.jsonb_build_object('correction_request_id',
          '00000067-0000-4000-8000-00000000000e'));
create temp table f7 as select pg_temp.census_family('BK-F7') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from f7)
  and (select pg_temp.item_class(census,'00000067-0000-4000-8000-000000000004')
       ='ACTIVE' from f7)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'
               ='WEEKLY_SOURCE_CENSUS_APPLIED_WITH_BLOCKERS_UNPROVED') from f7),
  'G1: a clean second correction request must not rescue a disqualified item'
);

-- G1, the reviewer's probe C2: the disqualifying blocker carries NO exact item
-- identity, and a clean higher-id request is added afterwards.
select pg_temp.mk_timesheet('00000068-0000-4000-8000-000000000001','BK-G1',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000068-0000-4000-8000-000000000002','CANCELLED','NOT_SUBMITTED',
                        timestamptz '2026-09-18T11:00:00Z',null,null);
select pg_temp.mk_candidate_row('00000068-0000-4000-8000-000000000003',
        '00000068-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000068-0000-4000-8000-000000000004',
        '00000068-0000-4000-8000-000000000003','00000068-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_binding_a('00000068-0000-4000-8000-000000000005',
        '00000068-0000-4000-8000-000000000002','00000068-0000-4000-8000-000000000003',
        '00000068-0000-4000-8000-000000000004','APPLIED_WITH_BLOCKERS',
        '00000068-0000-4000-8000-000000000007','00000068-0000-4000-8000-000000000008');
insert into public.pay_payment_correction_work_items(
  id,correction_request_id,pay_batch_id,pay_batch_candidate_id,work_kind,
  selection_json,selection_hash,status
) values (
  '00000068-0000-4000-8000-000000000009','00000068-0000-4000-8000-000000000005',
  '00000068-0000-4000-8000-000000000002',null,'PRE_BANK_CANCEL',
  pg_catalog.jsonb_build_object('expected_pay_batch_item_ids','[]'::jsonb),
  'scope-g1-anon','BLOCKED'
);
create temp table g1a as select pg_temp.census_family('BK-G1') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from g1a)
  and (select pg_temp.item_class(census,'00000068-0000-4000-8000-000000000004')='ACTIVE'
       from g1a),
  'G1: an un-identified blocker leaves the root FROZEN before the clean request is added'
);
-- now add the clean, higher-id request; nothing else changes
insert into public.pay_payment_correction_requests(
  id,pay_batch_id,correction_kind,status,selection_hash,plan_hash
) values ('00000068-0000-4000-8000-0000000000f1',
          '00000068-0000-4000-8000-000000000002','NO_MONEY_UNWIND','APPLIED',
          'sel-g1-clean','plan-g1-clean');
insert into public.pay_payment_correction_request_candidates(
  correction_request_id,selection_ordinal,pay_batch_candidate_id,
  candidate_scope_hash,active_item_count,source_row_count,active_amount,
  pay_batch_item_ids,eligibility_code_at_plan
) values ('00000068-0000-4000-8000-0000000000f1',1,
          '00000068-0000-4000-8000-000000000003',
          encode(sha256(convert_to('g1-clean','UTF8')),'hex'),1,1,100.00,
          array['00000068-0000-4000-8000-000000000004'::uuid],'ELIGIBLE');
insert into public.pay_payment_correction_work_items(
  id,correction_request_id,pay_batch_id,pay_batch_candidate_id,work_kind,
  selection_json,selection_hash,status,result_json
) values (
  '00000068-0000-4000-8000-0000000000f2','00000068-0000-4000-8000-0000000000f1',
  '00000068-0000-4000-8000-000000000002','00000068-0000-4000-8000-000000000003',
  'NO_MONEY_UNWIND','{}'::jsonb,
  encode(sha256(convert_to('g1-clean','UTF8')),'hex'),'APPLIED',
  pg_catalog.jsonb_build_object('changed_scope_json',
    pg_catalog.jsonb_build_object('changed_pay_batch_item_ids',
      pg_catalog.jsonb_build_array('00000068-0000-4000-8000-000000000004')))
);
select pg_temp.mk_operation('00000068-0000-4000-8000-0000000000f3',
        '00000068-0000-4000-8000-000000000002','PAYMENT_CORRECTION','COMPLETE','COMPLETE',
        null,null,'NONE',
        pg_catalog.jsonb_build_object('correction_request_id',
          '00000068-0000-4000-8000-0000000000f1'));
create temp table g1b as select pg_temp.census_family('BK-G1') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from g1b)
  and (select pg_temp.item_class(census,'00000068-0000-4000-8000-000000000004')='ACTIVE'
       from g1b),
  'G1: adding a clean terminal request must not flip the same root to RELEASABLE'
);

-- ==================== HANDOVER 2 round-4 rulings (17 September 2026) =======
-- The preserved response is
-- `plan6-pack-audit-20260916\HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R4.md`
-- and it outranks `proof/32` revision 4 wherever it amends it.

-- Ruling 1 (OR-10). The positive artefact is required, and it is enough.
-- The negative halves are already asserted by `R31`, `OR-10 SETTLED` and
-- `OR-10 FAILED` above; this is the positive path, and a second item in the
-- same batch proves the artefact is read per item and not per batch.
select pg_temp.mk_timesheet('00000070-0000-4000-8000-000000000001','BK-R4-C',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000070-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000070-0000-4000-8000-000000000003',
        '00000070-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000070-0000-4000-8000-000000000004',
        '00000070-0000-4000-8000-000000000003','00000070-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_item('00000070-0000-4000-8000-000000000005',
        '00000070-0000-4000-8000-000000000003','00000070-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status,released_at_utc,released_reason
) values ('00000070-0000-4000-8000-000000000006','aa000000-0000-4000-8000-000000000010',
          '00000070-0000-4000-8000-000000000002','00000070-0000-4000-8000-000000000003',
          '00000070-0000-4000-8000-000000000004',25.00,'RELEASED',
          timestamptz '2026-09-18T09:30:00Z','PAYE_NET_REPROJECTION');
create temp table r4c as select pg_temp.census_family('BK-R4-C') as census;
select pg_temp.assert_true(
  (select pg_temp.item_class(census,'00000070-0000-4000-8000-000000000004')
   ='VOIDED_TERMINAL' from r4c)
  and (select pg_temp.item_binding(census,'00000070-0000-4000-8000-000000000004')='C'
       from r4c)
  and (select pg_temp.item_class(census,'00000070-0000-4000-8000-000000000005')
       ='CENSUS_ERROR' from r4c)
  and (select census->>'result'='CENSUS_ERROR' from r4c),
  'Ruling 1: the item-linked reprojection artefact proves only the item it is linked to'
);

-- Ruling 2 (OR-9). A born-voided row that has been given a Timesheet identity
-- is enumerable, and it is CENSUS_ERROR - never Binding C. The structural
-- exclusion `NON_ENUMERATED_BORN_VOIDED_TEMPLATE` is registered by WP-18; this
-- is the runtime half.
select pg_temp.mk_timesheet('00000071-0000-4000-8000-000000000001','BK-R4-BORN',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000071-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000071-0000-4000-8000-000000000003',
        '00000071-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
-- the dormant recovery-template shape of `pay_batch_apply_finance_adjustments`
-- (`true as is_voided`), but given the family's Timesheet identity
insert into public.pay_batch_items(
  id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,amount_inc_vat
) values ('00000071-0000-4000-8000-000000000004',
          '00000071-0000-4000-8000-000000000003','OVERPAYMENT_RECOVERY',
          '00000071-0000-4000-8000-000000000001','PAYE',true,0.00);
create temp table r4born as select pg_temp.census_family('BK-R4-BORN') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from r4born)
  and (select pg_temp.item_class(census,'00000071-0000-4000-8000-000000000004')
       ='CENSUS_ERROR' from r4born)
  and (select pg_temp.item_binding(census,'00000071-0000-4000-8000-000000000004')
       is null from r4born)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'='WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE')
       from r4born),
  'Ruling 2: an enumerated born-voided row is CENSUS_ERROR, never Binding C'
);

-- Ruling 5 (OR-3 item 5). A reservation released with reason WRITE_OFF is not
-- positive reservation evidence for any binding. The same fixture proves it in
-- three places: Binding A's common condition, Binding C's artefact test, and
-- the section 5.2 settlement condition.
select pg_temp.mk_timesheet('00000072-0000-4000-8000-000000000001','BK-R4-WO',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000072-0000-4000-8000-000000000002','CANCELLED','NOT_SUBMITTED',
                        timestamptz '2026-09-18T11:00:00Z',null,null);
select pg_temp.mk_candidate_row('00000072-0000-4000-8000-000000000003',
        '00000072-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000072-0000-4000-8000-000000000004',
        '00000072-0000-4000-8000-000000000003','00000072-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_binding_a('00000072-0000-4000-8000-000000000005',
        '00000072-0000-4000-8000-000000000002','00000072-0000-4000-8000-000000000003',
        '00000072-0000-4000-8000-000000000004','APPLIED',
        '00000072-0000-4000-8000-000000000006','00000072-0000-4000-8000-000000000007');
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status,released_at_utc,released_reason
) values ('00000072-0000-4000-8000-000000000008','aa000000-0000-4000-8000-000000000010',
          '00000072-0000-4000-8000-000000000002','00000072-0000-4000-8000-000000000003',
          '00000072-0000-4000-8000-000000000004',25.00,'RELEASED',
          timestamptz '2026-09-18T09:30:00Z','WRITE_OFF');
create temp table r4wo as select pg_temp.census_family('BK-R4-WO') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from r4wo)
  and (select pg_temp.item_class(census,'00000072-0000-4000-8000-000000000004')
       ='CENSUS_ERROR' from r4wo)
  and (select pg_temp.item_binding(census,'00000072-0000-4000-8000-000000000004')
       is null from r4wo)
  and not (select pg_temp.has_predicate(census,'C2') from r4wo),
  'Ruling 5: a WRITE_OFF release does not satisfy Binding A''s reservation evidence, and C2 cannot see it either'
);
-- and the same reservation does not rescue a settlement proof
select pg_temp.mk_timesheet('00000172-0000-4000-8000-000000000001','BK-R4-WO2',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000172-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000172-0000-4000-8000-000000000003',
        '00000172-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000172-0000-4000-8000-000000000005',
        '00000172-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000172-0000-4000-8000-000000000004',
        '00000172-0000-4000-8000-000000000003','00000172-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000172-0000-4000-8000-000000000005',null);
select pg_temp.mk_settled_proof('00000172-0000-4000-8000-000000000002',
        '00000172-0000-4000-8000-000000000001','sig-r4wo2','PAYE');
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status,released_at_utc,released_reason
) values ('00000172-0000-4000-8000-000000000006','aa000000-0000-4000-8000-000000000010',
          '00000172-0000-4000-8000-000000000002','00000172-0000-4000-8000-000000000003',
          '00000172-0000-4000-8000-000000000004',25.00,'RELEASED',
          timestamptz '2026-09-18T09:30:00Z','WRITE_OFF');
create temp table r4wo2 as select pg_temp.census_family('BK-R4-WO2') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r4wo2)
  and (select pg_temp.item_class(census,'00000172-0000-4000-8000-000000000004')='ACTIVE'
       from r4wo2),
  'Ruling 5: a WRITE_OFF release does not satisfy the section 5.2 settlement reservation condition'
);

-- =====================================================================
-- HANDOVER 2 round-7 ruling A4 - THE WIDER READING, IN EVERY SPELLING
-- =====================================================================
-- "APPROVED.  The wider conservative interpretation is required.  `WRITE_OFF`
--  must not satisfy the common condition, a binding's positive reservation
--  evidence or §5.2 settlement evidence.  It is neither positive release
--  evidence nor a freeze by itself."
--
-- The two cases above prove the EXACT literal `'WRITE_OFF'`.  Ruling A4 also
-- requires every place a write-off could still satisfy one of the three to be
-- closed, and one was open: `pay_advance_reservations.released_reason` is plain
-- `text` with no check constraint, and the census compared the raw value while
-- the withdrawal owner in `17092026_0600_…` already normalised it with
-- `upper(btrim(...))`.  The two authorities disagreed about what a write-off is
-- and the census was the permissive one.
--
-- Measured on a build from empty before the fix, and both are
-- release-direction:
--
--   * reason `' write_off '` on a Binding A void  -> VOIDED_TERMINAL / A,
--     family RELEASABLE  (the §5.1 common condition was satisfied);
--   * reason `'Write_Off'` on a settled item      -> SETTLED_TERMINAL,
--     family RELEASABLE  (§5.2 settlement evidence was satisfied).
--
-- Both limbs are driven here, plus the neighbouring case that must STILL
-- succeed, so the narrowing cannot be mistaken for "no reservation release is
-- ever evidence".
select pg_temp.mk_timesheet('00000f36-0000-4000-8000-000000000001','BK-A4-WOV',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000f36-0000-4000-8000-000000000002','CANCELLED','NOT_SUBMITTED',
                        timestamptz '2026-09-18T11:00:00Z',null,null);
select pg_temp.mk_candidate_row('00000f36-0000-4000-8000-000000000003',
        '00000f36-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_item('00000f36-0000-4000-8000-000000000004',
        '00000f36-0000-4000-8000-000000000003','00000f36-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
select pg_temp.mk_binding_a('00000f36-0000-4000-8000-000000000005',
        '00000f36-0000-4000-8000-000000000002','00000f36-0000-4000-8000-000000000003',
        '00000f36-0000-4000-8000-000000000004','APPLIED',
        '00000f36-0000-4000-8000-000000000006','00000f36-0000-4000-8000-000000000007');
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status,released_at_utc,released_reason
) values ('00000f36-0000-4000-8000-000000000008','aa000000-0000-4000-8000-000000000010',
          '00000f36-0000-4000-8000-000000000002','00000f36-0000-4000-8000-000000000003',
          '00000f36-0000-4000-8000-000000000004',25.00,'RELEASED',
          timestamptz '2026-09-18T09:30:00Z',' write_off ');
create temp table a4wov as select pg_temp.census_family('BK-A4-WOV') as census;
select pg_temp.assert_true(
  (select census->>'result'<>'RELEASABLE' from a4wov)
  and (select pg_temp.item_class(census,'00000f36-0000-4000-8000-000000000004')
       <>'VOIDED_TERMINAL' from a4wov)
  and (select pg_temp.item_binding(census,'00000f36-0000-4000-8000-000000000004')
       is null from a4wov),
  'A4: a write-off stored as '' write_off '' must not satisfy the section 5.1 common '
  ||'condition, so the family cannot be RELEASABLE'
);

select pg_temp.mk_timesheet('00001f36-0000-4000-8000-000000000001','BK-A4-WOV2',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00001f36-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00001f36-0000-4000-8000-000000000003',
        '00001f36-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00001f36-0000-4000-8000-000000000005',
        '00001f36-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00001f36-0000-4000-8000-000000000004',
        '00001f36-0000-4000-8000-000000000003','00001f36-0000-4000-8000-000000000001',
        false,'PAYE',null,'00001f36-0000-4000-8000-000000000005',null);
select pg_temp.mk_settled_proof('00001f36-0000-4000-8000-000000000002',
        '00001f36-0000-4000-8000-000000000001','sig-a4wov2','PAYE');
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status,released_at_utc,released_reason
) values ('00001f36-0000-4000-8000-000000000006','aa000000-0000-4000-8000-000000000010',
          '00001f36-0000-4000-8000-000000000002','00001f36-0000-4000-8000-000000000003',
          '00001f36-0000-4000-8000-000000000004',25.00,'RELEASED',
          timestamptz '2026-09-18T09:30:00Z','Write_Off');
create temp table a4wov2 as select pg_temp.census_family('BK-A4-WOV2') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from a4wov2)
  and (select pg_temp.item_class(census,'00001f36-0000-4000-8000-000000000004')='ACTIVE'
       from a4wov2),
  'A4: a write-off stored as ''Write_Off'' must not satisfy the section 5.2 settlement '
  ||'evidence either'
);

-- The neighbouring case that must STILL succeed: a genuine PAYE-net
-- reprojection release is positive Binding C evidence and still releases.  The
-- narrowing above is about the write-off reason only, not about reservation
-- releases in general.
select pg_temp.mk_timesheet('00002f36-0000-4000-8000-000000000001','BK-A4-OK',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00002f36-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00002f36-0000-4000-8000-000000000003',
        '00002f36-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00002f36-0000-4000-8000-000000000004',
        '00002f36-0000-4000-8000-000000000003','00002f36-0000-4000-8000-000000000001',
        true,'PAYE',null,null,null);
insert into public.pay_advance_reservations(
  id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
  reserved_amount,status,released_at_utc,released_reason
) values ('00002f36-0000-4000-8000-000000000006','aa000000-0000-4000-8000-000000000010',
          '00002f36-0000-4000-8000-000000000002','00002f36-0000-4000-8000-000000000003',
          '00002f36-0000-4000-8000-000000000004',25.00,'RELEASED',
          timestamptz '2026-09-18T09:30:00Z','PAYE_NET_REPROJECTION');
create temp table a4ok as select pg_temp.census_family('BK-A4-OK') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from a4ok)
  and (select pg_temp.item_class(census,'00002f36-0000-4000-8000-000000000004')
       ='VOIDED_TERMINAL' from a4ok)
  and (select pg_temp.item_binding(census,'00002f36-0000-4000-8000-000000000004')='C'
       from a4ok),
  'A4: the neighbouring case still succeeds - a PAYE_NET_REPROJECTION release is still '
  ||'positive Binding C evidence and the family still releases'
);

-- And a write-off is still NOT A FREEZE BY ITSELF: predicate C2 tests
-- RESERVED/COMMITTED, and a written-off row is RELEASED, so it neither proves
-- nor blocks.  Both write-off families above are checked, in both spellings.
select pg_temp.assert_true(
  not (select pg_temp.has_predicate(census,'C2') from a4wov)
  and not (select pg_temp.has_predicate(census,'C2') from a4wov2)
  and not (select pg_temp.has_predicate(census,'C2') from r4wo),
  'A4: a write-off is neither positive release evidence nor a freeze by itself - C2 '
  ||'never sees it, in any spelling'
);

-- Ruling 4 (OR-11) acceptance scenario, the parts that do not need the future
-- Banking Pay classifier.
--
-- Part (b): a partial cancellation left PENDING stays frozen.
select pg_temp.mk_timesheet('00000073-0000-4000-8000-000000000001','BK-R4-PEND',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000073-0000-4000-8000-000000000002','DRAFT','NOT_SUBMITTED',
                        null,null,null);
select pg_temp.mk_candidate_row('00000073-0000-4000-8000-000000000003',
        '00000073-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
select pg_temp.mk_transfer('00000073-0000-4000-8000-000000000005',
        '00000073-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','PENDING',null);
select pg_temp.mk_item('00000073-0000-4000-8000-000000000004',
        '00000073-0000-4000-8000-000000000003','00000073-0000-4000-8000-000000000001',
        true,'PAYE',null,'00000073-0000-4000-8000-000000000005',null);
select pg_temp.mk_binding_a('00000073-0000-4000-8000-000000000006',
        '00000073-0000-4000-8000-000000000002','00000073-0000-4000-8000-000000000003',
        '00000073-0000-4000-8000-000000000004','APPLIED',
        '00000073-0000-4000-8000-000000000007','00000073-0000-4000-8000-000000000008');
create temp table r4pend as select pg_temp.census_family('BK-R4-PEND') as census;
select pg_temp.assert_true(
  (select census->>'result'='FROZEN' from r4pend)
  and (select pg_temp.item_class(census,'00000073-0000-4000-8000-000000000004')='ACTIVE'
       from r4pend)
  and (select pg_temp.has_predicate(census,'C5') from r4pend),
  'Ruling 4(b): a partial cancellation left PENDING stays frozen'
);

-- Part (c): an unmarked, malformed or contradictory `VOIDED` transfer stays
-- UNKNOWN and cannot release. Asserted against the installed classifier itself,
-- so the census is proved to add no private interpretation of `VOIDED`.
select pg_temp.assert_true(
  (select movement.cash_state='UNKNOWN'
      and not movement.is_terminal_no_money
   from public._pay_rail_state_money_movement_classify(
          'VOIDED',null,'{}'::jsonb,'{}'::jsonb) as movement)
  and (select movement.cash_state='UNKNOWN'
       from public._pay_rail_state_money_movement_classify(
              'VOIDED',null,
              pg_catalog.jsonb_build_object('pre_bank_cancel_applied',true),
              pg_catalog.jsonb_build_object('pre_bank_cancel_applied',true)) as movement)
  and (select movement.cash_state='UNKNOWN'
       from public._pay_rail_state_money_movement_classify(
              'VOIDED',null,
              pg_catalog.jsonb_build_object('pre_bank_cancel_applied','not-a-boolean'),
              pg_catalog.jsonb_build_object('pre_bank_cancel_applied','not-a-boolean')) as movement),
  'Ruling 4(c): every VOIDED shape stays UNKNOWN under the installed classifier'
);
select pg_temp.mk_timesheet('00000074-0000-4000-8000-000000000001','BK-R4-VOIDED',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000074-0000-4000-8000-000000000002','CANCELLED','NOT_SUBMITTED',
                        timestamptz '2026-09-18T11:00:00Z',null,null);
select pg_temp.mk_candidate_row('00000074-0000-4000-8000-000000000003',
        '00000074-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',null,false);
insert into public.pay_bank_transfers(
  id,pay_batch_id,candidate_id,pay_channel,amount,status,failed_reason,
  rail_meta_json,transfer_group_key
) values ('00000074-0000-4000-8000-000000000005','00000074-0000-4000-8000-000000000002',
          'aa000000-0000-4000-8000-00000000000a','PAYE',0.00,'VOIDED',
          'PRE_BANK_CANCEL_VOIDED',
          pg_catalog.jsonb_build_object('pre_bank_cancel_applied',true),
          'grp-r4-voided');
select pg_temp.mk_item('00000074-0000-4000-8000-000000000004',
        '00000074-0000-4000-8000-000000000003','00000074-0000-4000-8000-000000000001',
        true,'PAYE',null,'00000074-0000-4000-8000-000000000005',null);
select pg_temp.mk_binding_a('00000074-0000-4000-8000-000000000006',
        '00000074-0000-4000-8000-000000000002','00000074-0000-4000-8000-000000000003',
        '00000074-0000-4000-8000-000000000004','APPLIED',
        '00000074-0000-4000-8000-000000000007','00000074-0000-4000-8000-000000000008');
create temp table r4voided as select pg_temp.census_family('BK-R4-VOIDED') as census;
-- PART (a) — EXPECTED TO CHANGE WHEN BANKING PAY SHIPS THE CLASSIFIER.
-- Ruling 4 assigns to Banking Pay one authoritative, versioned rule under which
-- `status = 'VOIDED'` + `failed_reason = 'PRE_BANK_CANCEL_VOIDED'` +
-- `rail_meta_json.pre_bank_cancel_applied = true` + a consistent registered
-- pre-bank-cancellation work identity is terminal-no-money. The fixture below is
-- exactly that shape. Until that classifier ships, Weekly Source must keep the
-- fail-closed result and add no private interpretation, so the assertion pins
-- TODAY's outcome. When Banking Pay lands the rule this assertion will fail, and
-- that failure is the signal to flip it to `VOIDED_TERMINAL/A` / `RELEASABLE`.
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from r4voided)
  and (select pg_temp.item_class(census,'00000074-0000-4000-8000-000000000004')
       ='CENSUS_ERROR' from r4voided)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'='WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE')
       from r4voided),
  'Ruling 4(a) PENDING BANKING PAY CLASSIFIER: a fully marked cancellation-owned VOIDED transfer is still UNKNOWN today, so the root fails closed'
);

-- Part (d): replay changes nothing.
create temp table r4replay as select pg_temp.census_family('BK-R4-VOIDED') as census;
select pg_temp.assert_true(
  (select (r4replay.census-'evaluated_at_utc'-'family_split_scan')
          =(r4voided.census-'evaluated_at_utc'-'family_split_scan')
   from r4replay,r4voided),
  'Ruling 4(d): replaying the census changes nothing'
);

-- Extra disposition: the bounded family-split read reports a positive
-- completion proof, and exceeding the bound is its own error, never a silent
-- pass.
select pg_temp.assert_true(
  (select (census->'family_split_scan'->>'completed')::boolean
      and (census->'family_split_scan'->>'bound')::integer=256
      and (census->'family_split_scan'->>'members_probed')::integer>=1
   from r4replay),
  'Bounded read: the family-split probe reports a positive completion proof'
);

-- Extra disposition: exceeding the fixed bound is its own error, never a silent
-- pass. A Contract-week is filled past `v_split_scan_bound` (256) and the census
-- must refuse rather than return an answer it could not complete.
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'aa000000-0000-4000-8000-000000000012','aa000000-0000-4000-8000-00000000000a',
  'aa000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE',
  '{"pay":{"day":10,"night":10,"sat":10,"sun":10,"bh":10},"charge":{"day":20,"night":20,"sat":20,"sun":20,"bh":20}}'::jsonb,
  'HEALTHROSTER',true,true,true,true
);
select pg_temp.mk_timesheet('00000076-0000-4000-8000-000000000001','BK-R4-BOUND',1,true,
                            'aa000000-0000-4000-8000-000000000012');
insert into public.timesheets(
  booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  week_ending_date,contract_id,sheet_scope,line_type,version,is_current,status
)
select 'BK-R4-BOUND-FILL-'||filler, 'occ-norm','hosp-norm','ward-norm','nurse-norm',
       date '2026-09-13','aa000000-0000-4000-8000-000000000012','WEEKLY','HOURS',
       1,true,'RECEIVED'
from pg_catalog.generate_series(1,300) as filler;
create temp table r4bound as select pg_temp.census_family('BK-R4-BOUND') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from r4bound)
  and (select pg_temp.has_error(census,
        'WEEKLY_SOURCE_CENSUS_FAMILY_SPLIT_SCAN_BOUND_EXCEEDED') from r4bound)
  and not (select (census->'family_split_scan'->>'completed')::boolean from r4bound)
  and (select (census->'family_split_scan'->>'max_rows_examined')::integer=257
       from r4bound),
  'Bounded read: exceeding the fixed bound is CENSUS_ERROR with its own reason code, never a silent pass'
);

-- Extra disposition: the settlement snapshot selection reproduces the installed
-- settle rail's own selection exactly, including its candidate filter, on a
-- family that carries three snapshot rows for one Timesheet - two for this
-- Candidate on different pay channels and one belonging to another Candidate.
select pg_temp.mk_timesheet('00000075-0000-4000-8000-000000000001','BK-R4-SNAP',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000075-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000075-0000-4000-8000-000000000003',
        '00000075-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_transfer('00000075-0000-4000-8000-000000000005',
        '00000075-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        null,'PAYE','COMPLETED',null);
select pg_temp.mk_item('00000075-0000-4000-8000-000000000004',
        '00000075-0000-4000-8000-000000000003','00000075-0000-4000-8000-000000000001',
        false,'PAYE',null,'00000075-0000-4000-8000-000000000005',null);
-- the other Candidate's snapshot, which the settle rail's candidate filter and
-- therefore this census both exclude
insert into public.pay_batch_timesheet_snapshots(
  pay_batch_id,timesheet_id,candidate_id,pay_channel,
  base_snapshot_json,target_snapshot_json,signature,created_at_utc
) values ('00000075-0000-4000-8000-000000000002','00000075-0000-4000-8000-000000000001',
          'aa000000-0000-4000-8000-00000000000b','UMBRELLA',
          '{"base":true}'::jsonb,'{"target":true}'::jsonb,'sig-foreign',
          timestamptz '2026-09-18T09:59:59Z');
select pg_temp.mk_settled_proof('00000075-0000-4000-8000-000000000002',
        '00000075-0000-4000-8000-000000000001','sig-r4snap','PAYE');
create temp table r4snap as select pg_temp.census_family('BK-R4-SNAP') as census;
select pg_temp.assert_true(
  (select census->>'result'='RELEASABLE' from r4snap)
  and (select exists (
     select 1 from pg_catalog.jsonb_array_elements(census->'proof') as proof_row
     where proof_row->>'section'='5.2'
       and proof_row->>'pay_batch_timesheet_snapshot_id'=(
         select chosen.id::text
         from (
           select distinct on (pbs.timesheet_id) pbs.id
           from public.pay_batch_timesheet_snapshots pbs
           where pbs.pay_batch_id='00000075-0000-4000-8000-000000000002'
             and pbs.candidate_id in ('aa000000-0000-4000-8000-00000000000a')
           order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id
         ) as chosen))
   from r4snap),
  'Snapshot selection: the census chooses exactly the row the settle rail''s own selector returns'
);
-- and it fails closed when the only snapshot belongs to another Candidate
select pg_temp.mk_timesheet('00000175-0000-4000-8000-000000000001','BK-R4-SNAP2',1,true,
                            'aa000000-0000-4000-8000-00000000000c');
select pg_temp.mk_batch('00000175-0000-4000-8000-000000000002','SETTLED','COMMITTED',
                        null,timestamptz '2026-09-18T10:00:00Z',null);
select pg_temp.mk_candidate_row('00000175-0000-4000-8000-000000000003',
        '00000175-0000-4000-8000-000000000002','aa000000-0000-4000-8000-00000000000a',
        'SETTLED',true);
select pg_temp.mk_item('00000175-0000-4000-8000-000000000004',
        '00000175-0000-4000-8000-000000000003','00000175-0000-4000-8000-000000000001',
        false,'PAYE',null,null,null);
insert into public.pay_batch_timesheet_snapshots(
  pay_batch_id,timesheet_id,candidate_id,pay_channel,
  base_snapshot_json,target_snapshot_json,signature
) values ('00000175-0000-4000-8000-000000000002','00000175-0000-4000-8000-000000000001',
          'aa000000-0000-4000-8000-00000000000b','PAYE',
          '{"base":true}'::jsonb,'{"target":true}'::jsonb,'sig-foreign-only');
insert into public.timesheet_pay_state_history(
  timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
) values ('00000175-0000-4000-8000-000000000001','00000175-0000-4000-8000-000000000002',
          timestamptz '2026-09-18T10:05:00Z','{"target":true}'::jsonb,'sig-foreign-only');
create temp table r4snap2 as select pg_temp.census_family('BK-R4-SNAP2') as census;
select pg_temp.assert_true(
  (select census->>'result'='CENSUS_ERROR' from r4snap2)
  and (select exists (
         select 1 from pg_catalog.jsonb_array_elements(census->'items') as census_item
         where census_item->>'reason'='SETTLEMENT_SNAPSHOT_CONFLICT') from r4snap2),
  'Snapshot selection: a snapshot belonging to another Candidate fails closed'
);

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_freeze_census_v1',
  'scenarios',pg_catalog.jsonb_build_array(
    'R3','R4','R5','R16','R17','R18','R19','R20','R21','R28','R29','R30','R31',
    'R34','R35','R36','R38','R41','R43','R44'),
  'review_findings',pg_catalog.jsonb_build_array(
    'F1','F3','F4','F6','F7','F9','G1','G2','G3','G5'),
  'handover2_round4_rulings',pg_catalog.jsonb_build_array(
    'ruling-1-binding-c','ruling-2-born-voided','ruling-3-mid-flight',
    'ruling-4-voided-transfer','ruling-5-write-off',
    'extra-correction-request-severity','extra-bounded-family-split',
    'extra-snapshot-candidate-predicate'),
  'fixture_source',
    'self-contained; the WP-16a real-owner cross-checks are in the WP-08a report',
  'row_locks_taken',0,
  'writes_performed_by_census',0
) as result;

rollback;
