-- Weekly Source Plan 6.2 — Gate 12 executed proof suite: UNA (part 1 of 2).
-- WP-16c. Rollback-contained, PostgreSQL 17.11.
--
-- This file carries the single-session half of the `UNA-001..UNA-019` suite.
-- The multi-session half (`UNA-014`) and the multi-transaction fixture half
-- (`UNA-015`, `UNA-019`) are in `una-suite.mjs`, because neither can be
-- expressed inside one rolled-back transaction.
--
-- WHAT THIS FILE IS NOT.  It is not a second copy of
-- `supabase/verification/17092026_0600_weekly_source_first_authorisation_v1.sql`.
-- That verifier already drives `UNA-001..UNA-012`, `UNA-016`, `UNA-017` and
-- `UNA-018` through the real installed owners, and `una-suite.mjs` executes it
-- as step one of the suite so its result is obtained, not assumed (Part 1
-- rule 6: never claim a verifier result you did not get).  This file adds the
-- ground no verifier reaches, plus one independent end-to-end lifecycle so the
-- suite has a proof of its own that does not depend on another package's file.
--
-- EVERY ASSERTION HERE EXECUTES THE PATH (Part 1 rule 1).  Nothing is decided by
-- reading `pg_get_functiondef`.  Where a fact can only be established
-- statically, the row is emitted with evidence `STATIC` and the suite reports it
-- as such.
--
-- Output contract, one line per assertion:
--   WS16C_PROOF|<control id>|PASS|FAIL|SKIP|<EXECUTED|STATIC|ENVIRONMENT>|<detail>
--
-- A section that raises unexpectedly records FAIL for its ids and lets the rest
-- of the file run, so one defect does not hide the other eighteen results.
--
-- Nothing here defines, wraps or re-creates a Banking Pay, Draft, execution,
-- cancellation, settlement, provider, recovery or remittance owner. Nothing is
-- written outside the rolled-back transaction. `set constraints all immediate`
-- is never used: it fires Banking Pay deferred finalisation.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create temporary table wp16c_results(
  ordinal serial primary key,
  proof_id text not null,
  result text not null,
  evidence text not null,
  detail text not null
) on commit drop;

create function pg_temp.proof(
  p_proof_id text, p_result text, p_evidence text, p_detail text
) returns void language sql as $function$
  insert into wp16c_results(proof_id,result,evidence,detail)
  values (p_proof_id,p_result,p_evidence,pg_catalog.left(pg_catalog.replace(p_detail,'|','/'),400));
$function$;

create function pg_temp.check(
  p_proof_id text, p_condition boolean, p_detail text, p_evidence text default 'EXECUTED'
) returns boolean language plpgsql as $function$
begin
  perform pg_temp.proof(
    p_proof_id,
    case when p_condition is distinct from true then 'FAIL' else 'PASS' end,
    p_evidence,
    p_detail);
  return coalesce(p_condition,false);
end;
$function$;

-- The installed Workbench dirty trigger queues a job for every Candidate this
-- fixture touches and the installed Candidate serial gate then reports
-- CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_CONTINUATION, so every gated Weekly Source
-- owner would refuse WEEKLY_SOURCE_CANDIDATE_BUSY and only the BLOCKED branch
-- would ever be proved (WP-07 finding F1, addressed to WP-16c by name).  The
-- fixture reproduces the Workbench worker's completion on its own fixture rows.
-- It changes no Banking Pay definition and is never performed by an owner.
create function pg_temp.drain_workbench_jobs() returns void
language sql as $function$
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
$function$;

create function pg_temp.current_signature(p_timesheet_id uuid) returns text
language sql stable as $function$
  select nullif(pg_catalog.btrim(coalesce(
           signature->>'backend_row_signature',signature->>'row_signature','')),'')
  from public.timesheet_lifecycle_guard_signature_v1(
    p_timesheet_id,
    (select contract_week.id from public.contract_weeks contract_week
      where contract_week.timesheet_id=p_timesheet_id),
    false) as signature;
$function$;

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
    'HOURS'::public.timesheet_line_type_enum,'wp16c-occupant','wp16c-hospital',
    'wp16c-ward','wp16c-role','weekly-0','2026-09-13',p_contract_id,
    '[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) returning timesheet_id;
$function$;

create function pg_temp.seed_week_and_financials(
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

-- ---------------------------------------------------------------------------
-- Fixture world.  One Client, six Candidates, one Contract and one Weekly HOURS
-- Timesheet family each.  Candidate 5 carries a rotated family so an old
-- physical id can be presented.  Nothing carries financial state until a
-- section adds it.
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('c6000000-0000-4000-8000-000000000001','wp16c-office@example.test','admin',true,'not-a-login');
insert into public.tms_users(id,email,role,is_active,password_hash)
values ('c6000000-0000-4000-8000-00000000000e','wp16c-inactive@example.test','admin',false,'not-a-login');
insert into public.clients(id,name) values ('c6000000-0000-4000-8000-000000000002','WP16C Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('c6000000-0000-4000-8000-000000000002',20,'2026-01-01');

do $seed_world$
declare
  v_index integer;
  v_candidate uuid;
  v_contract uuid;
  v_timesheet uuid;
begin
  for v_index in 1..6 loop
    v_candidate:=('c6000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_contract:=('c6000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_timesheet:=('c6000000-0000-4000-8000-0000000003'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    insert into public.candidates(id,display_name)
    values (v_candidate,'WP16C Candidate '||v_index);
    insert into public.contracts(
      id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
      weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
    ) values (
      v_contract,v_candidate,'c6000000-0000-4000-8000-000000000002',
      '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);
    if v_index=5 then
      perform pg_temp.seed_timesheet(
        'c6000000-0000-4000-8000-000000000395','WP16C-BK-05',1,false,v_contract);
      perform pg_temp.seed_timesheet(v_timesheet,'WP16C-BK-05',2,true,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('c6000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c6000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c6000000-0000-4000-8000-000000000002',2);
    else
      perform pg_temp.seed_timesheet(
        v_timesheet,'WP16C-BK-'||pg_catalog.lpad(v_index::text,2,'0'),1,true,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('c6000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c6000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c6000000-0000-4000-8000-000000000002',1);
    end if;
  end loop;
end
$seed_world$;

select pg_temp.drain_workbench_jobs();

-- ===========================================================================
-- 1. UNA-001 and UNA-011 — the whole lifecycle through the real owners.
--    Authorise, read availability, withdraw, prove the Timesheet is back to
--    awaiting authorisation with its identity intact, then authorise the SAME
--    Timesheet again and prove generation 2 is appended beside generation 1.
-- ===========================================================================
do $una_001$
declare
  v_ts uuid:='c6000000-0000-4000-8000-000000000301';
  v_actor uuid:='c6000000-0000-4000-8000-000000000001';
  v_authorise jsonb;
  v_available jsonb;
  v_withdraw jsonb;
  v_reauthorise jsonb;
  v_signature text;
  v_generation_rows integer;
  v_live_rows integer;
  v_authorised_after timestamptz;
  v_tsfin_rows integer;
  v_audit_auth integer;
  v_audit_withdrawn integer;
  v_identity record;
begin
  perform pg_temp.drain_workbench_jobs();
  v_authorise:=public.weekly_source_first_authorise_v1(v_ts,v_ts,null,v_actor);
  perform pg_temp.drain_workbench_jobs();
  if not pg_temp.check('UNA-001',coalesce((v_authorise->>'ok')::boolean,false),
    'first authorisation through public.weekly_source_first_authorise_v1 returned ok=true, generation '
    ||coalesce(v_authorise->>'authorisation_generation','?')) then
    return;
  end if;

  v_available:=public.weekly_source_first_authorisation_withdraw_available_v1(v_ts);
  perform pg_temp.check('UNA-001',coalesce((v_available->>'available')::boolean,false),
    'availability owner reports available=true with code '||coalesce(v_available->>'code','NONE')
    ||' for a clean first authorisation');

  v_signature:=pg_temp.current_signature(v_ts);
  v_withdraw:=public.weekly_source_first_authorisation_withdraw_v1(v_ts,v_ts,v_signature,v_actor);
  perform pg_temp.drain_workbench_jobs();
  if not pg_temp.check('UNA-001',coalesce((v_withdraw->>'withdrawn')::boolean,false),
    'withdrawal returned withdrawn=true, replayed='||coalesce(v_withdraw->>'replayed','?')) then
    return;
  end if;

  select root.authorised_at_server into v_authorised_after
  from public.timesheets root where root.timesheet_id=v_ts;
  perform pg_temp.check('UNA-001',v_authorised_after is null,
    'the Timesheet is back to awaiting authorisation (authorised_at_server is null)');

  perform pg_temp.check('UNA-001',
    (v_withdraw->>'timesheet_id')::uuid=v_ts,
    'the withdrawal reports the same timesheet_id it was given, so the row is not replaced');

  select count(*) into v_tsfin_rows from public.timesheets_financials financial
  where financial.timesheet_id=v_ts;
  perform pg_temp.check('UNA-001',v_tsfin_rows>0,
    'submitted and source evidence is preserved: '||v_tsfin_rows||' timesheets_financials row(s) survive');

  perform pg_temp.check('UNA-001',
    not exists (select 1 from public.pay_batch_items item where item.timesheet_id=v_ts)
    and not exists (select 1 from public.ts_pay_adjustments adjustment where adjustment.timesheet_id=v_ts),
    'no Banking Pay item and no pay adjustment exists, so no under/over-payment or recovery was created');

  select count(*) into v_audit_auth from public.audit_events event
  where event.object_type='timesheets' and event.object_id_text=v_ts::text
    and event.action='WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED';
  select count(*) into v_audit_withdrawn from public.audit_events event
  where event.object_type='timesheets' and event.object_id_text=v_ts::text
    and event.action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN';
  perform pg_temp.check('UNA-001',v_audit_auth>=1 and v_audit_withdrawn=1,
    'Audit carries the authorisation ('||v_audit_auth||') and exactly one withdrawal ('||v_audit_withdrawn||')');

  -- UNA-001 last clause and UNA-011: Office authorises the same Timesheet again.
  v_reauthorise:=public.weekly_source_first_authorise_v1(v_ts,v_ts,null,v_actor);
  perform pg_temp.drain_workbench_jobs();
  perform pg_temp.check('UNA-001',coalesce((v_reauthorise->>'ok')::boolean,false),
    'Office authorises the same Timesheet again: ok='||coalesce(v_reauthorise->>'ok','?'));

  select count(*) into v_generation_rows
  from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_ts;
  select count(*) into v_live_rows
  from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_ts and generation.withdrawn_at_utc is null;
  perform pg_temp.check('UNA-011',v_generation_rows=2 and v_live_rows=1,
    're-authorisation appends a generation: '||v_generation_rows||' rows, '||v_live_rows||' live');

  select generation.family_booking_id,generation.timesheet_version,
         generation.authorisation_generation
    into v_identity
  from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_ts and generation.withdrawn_at_utc is null;
  perform pg_temp.check('UNA-011',
    v_identity.authorisation_generation=2
    and v_identity.family_booking_id=(v_withdraw->>'family_booking_id')
    and v_identity.timesheet_version=(v_withdraw->>'timesheet_version')::integer,
    'timesheet_id, family ('||v_identity.family_booking_id||'), version ('
    ||v_identity.timesheet_version||') unchanged across withdrawal and re-authorisation; generation 2');

  perform pg_temp.check('UNA-011',
    (select count(distinct scope.canonical_timesheet_id) from public._pay_timesheet_rotation_scope(array[v_ts]) scope)=1
    and (select pg_catalog.min(scope.canonical_timesheet_id::text) from public._pay_timesheet_rotation_scope(array[v_ts]) scope)::uuid=v_ts,
    'public._pay_timesheet_rotation_scope resolves the same canonical row after the round trip');
exception when others then
  perform pg_temp.proof('UNA-001','FAIL','EXECUTED','section raised: '||sqlerrm);
  perform pg_temp.proof('UNA-011','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$una_001$;

-- ===========================================================================
-- 2. UNA-010 — the control is unavailable and a direct server call is made.
--    Refused, nothing written, and a refusal audit row exists.  The role guard
--    is exercised in the same section.
-- ===========================================================================
do $una_010$
declare
  v_ts uuid:='c6000000-0000-4000-8000-000000000302';
  v_actor uuid:='c6000000-0000-4000-8000-000000000001';
  v_inactive uuid:='c6000000-0000-4000-8000-00000000000e';
  v_authorise jsonb;
  v_available jsonb;
  v_result jsonb;
  v_signature text;
  v_refusal_rows integer;
  v_authorised_before timestamptz;
  v_authorised_after timestamptz;
  v_live_before integer;
  v_live_after integer;
  v_role_refused boolean:=false;
begin
  perform pg_temp.drain_workbench_jobs();
  v_authorise:=public.weekly_source_first_authorise_v1(v_ts,v_ts,null,v_actor);
  perform pg_temp.drain_workbench_jobs();

  -- Make the control unavailable through a real invoice line on the root: W7 is
  -- the permanent refusal proof/36 section 6 says removes the control entirely.
  insert into public.invoices(id,client_id,status,invoice_no)
  values ('c6000000-0000-4000-8000-000000000a02','c6000000-0000-4000-8000-000000000002',
          'DRAFT'::public.invoice_status_enum,'WP16C-INV-A02')
  on conflict (id) do nothing;
  insert into public.invoice_lines(id,invoice_id,timesheet_id,booking_id,description)
  values ('c6000000-0000-4000-8000-000000000b02','c6000000-0000-4000-8000-000000000a02',v_ts,
          'WP16C-BK-02','WP16C invoice line');

  v_available:=public.weekly_source_first_authorisation_withdraw_available_v1(v_ts);
  perform pg_temp.check('UNA-010',coalesce((v_available->>'available')::boolean,true)=false,
    'the control is unavailable: available='||coalesce(v_available->>'available','?')
    ||', code '||coalesce(v_available->>'code','NONE'));

  select root.authorised_at_server into v_authorised_before
  from public.timesheets root where root.timesheet_id=v_ts;
  select count(*) into v_live_before from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_ts and generation.withdrawn_at_utc is null;

  v_signature:=pg_temp.current_signature(v_ts);
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(v_ts,v_ts,v_signature,v_actor);

  select root.authorised_at_server into v_authorised_after
  from public.timesheets root where root.timesheet_id=v_ts;
  select count(*) into v_live_after from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_ts and generation.withdrawn_at_utc is null;

  perform pg_temp.check('UNA-010',
    coalesce((v_result->>'ok')::boolean,true)=false
    and v_authorised_before is not distinct from v_authorised_after
    and v_live_before=v_live_after,
    'the direct call is refused with code '||coalesce(v_result->>'code','NONE')
    ||' and no lifecycle write happened');

  select count(*) into v_refusal_rows from public.audit_events event
  where event.object_type='timesheets' and event.object_id_text in (v_ts::text)
    and event.action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED';
  perform pg_temp.check('UNA-010',v_refusal_rows>=1,
    'the refusal wrote '||v_refusal_rows||' audit row(s), as UNA-010 requires');

  -- The same entry point refuses an inactive actor without touching anything.
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(v_ts,v_ts,v_signature,v_inactive);
  perform pg_temp.check('UNA-010',
    coalesce(v_result->>'code','')='WEEKLY_SOURCE_UNAUTHORISE_ACTOR_INVALID',
    'an inactive actor is refused: '||coalesce(v_result->>'code','NONE'));

  begin
    perform pg_catalog.set_config('request.jwt.claim.role','authenticated',true);
    perform public.weekly_source_first_authorisation_withdraw_v1(v_ts,v_ts,v_signature,v_actor);
  exception when others then
    v_role_refused:=sqlerrm like '%WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED%';
  end;
  perform pg_catalog.set_config('request.jwt.claim.role','service_role',true);
  perform pg_temp.check('UNA-010',v_role_refused,
    'a non-service caller is refused WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED by the entry point itself');
exception when others then
  perform pg_catalog.set_config('request.jwt.claim.role','service_role',true);
  perform pg_temp.proof('UNA-010','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$una_010$;

-- ===========================================================================
-- 3. UNA-013 — DEC-061 Option A.  A week already carried on a finalised source
--    self-bill.  Whatever the withdrawal owner decides, the invoice must not
--    move: lines, bindings, totals and revision byte-identical, and the
--    finalised movement still on the exact self-bill.
-- ===========================================================================
do $una_013$
declare
  v_ts uuid:='c6000000-0000-4000-8000-000000000303';
  v_actor uuid:='c6000000-0000-4000-8000-000000000001';
  v_invoice uuid:='c6000000-0000-4000-8000-000000000a03';
  v_authorise jsonb;
  v_result jsonb;
  v_signature text;
  v_before text;
  v_after text;
  v_office jsonb;
begin
  perform pg_temp.drain_workbench_jobs();
  v_authorise:=public.weekly_source_first_authorise_v1(v_ts,v_ts,null,v_actor);
  perform pg_temp.drain_workbench_jobs();
  if not coalesce((v_authorise->>'ok')::boolean,false) then
    perform pg_temp.proof('UNA-013','FAIL','EXECUTED',
      'could not reach an authorised root: '||coalesce(v_authorise->>'code','NONE'));
    return;
  end if;

  insert into public.invoices(id,client_id,status,invoice_no,subtotal_ex_vat,vat_amount,total_inc_vat,issued_at_utc)
  values (v_invoice,'c6000000-0000-4000-8000-000000000002',
          'ISSUED'::public.invoice_status_enum,'WP16C-INV-A03',200,40,240,pg_catalog.clock_timestamp());
  insert into public.invoice_lines(
    id,invoice_id,timesheet_id,booking_id,description,total_pay_ex_vat,total_charge_ex_vat,total_inc_vat
  ) values (
    'c6000000-0000-4000-8000-000000000b03',v_invoice,v_ts,'WP16C-BK-03',
    'WP16C source self-bill line',100,200,240);

  select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
      coalesce(pg_catalog.string_agg(row_text,'|' order by row_text),''),'UTF8')),'hex')
    into v_before
  from (
    select line.id::text||':'||line.invoice_id::text||':'||line.timesheet_id::text||':'
           ||line.total_charge_ex_vat::text||':'||line.total_inc_vat::text as row_text
    from public.invoice_lines line where line.timesheet_id=v_ts
    union all
    select 'invoice:'||invoice.id::text||':'||invoice.status::text||':'
           ||coalesce(invoice.subtotal_ex_vat,0)::text||':'||coalesce(invoice.total_inc_vat,0)::text
           ||':rev'||invoice.document_revision::text
    from public.invoices invoice where invoice.id=v_invoice
    union all
    select 'binding:'||binding.id::text||':'||binding.state
    from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_id=v_invoice
  ) as invoice_state;

  v_signature:=pg_temp.current_signature(v_ts);
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(v_ts,v_ts,v_signature,v_actor);

  select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
      coalesce(pg_catalog.string_agg(row_text,'|' order by row_text),''),'UTF8')),'hex')
    into v_after
  from (
    select line.id::text||':'||line.invoice_id::text||':'||line.timesheet_id::text||':'
           ||line.total_charge_ex_vat::text||':'||line.total_inc_vat::text as row_text
    from public.invoice_lines line where line.timesheet_id=v_ts
    union all
    select 'invoice:'||invoice.id::text||':'||invoice.status::text||':'
           ||coalesce(invoice.subtotal_ex_vat,0)::text||':'||coalesce(invoice.total_inc_vat,0)::text
           ||':rev'||invoice.document_revision::text
    from public.invoices invoice where invoice.id=v_invoice
    union all
    select 'binding:'||binding.id::text||':'||binding.state
    from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_id=v_invoice
  ) as invoice_state;

  perform pg_temp.check('UNA-013',v_before=v_after,
    'DEC-061 Option A: invoice lines, bindings, totals and status are byte-identical across the '
    ||'withdrawal attempt (outcome code '||coalesce(v_result->>'code','ALLOWED')||')');

  v_office:=private.weekly_source_office_unauthorise_action_state_v1(v_ts);
  perform pg_temp.check('UNA-013',
    coalesce((v_office#>>'{unauthorise,available}')::boolean,true)=false
    and coalesce(v_office#>>'{unauthorise,permanent}','')='true',
    'the Office action state reports the control removed permanently while the week is invoiced: '
    ||coalesce(v_office#>>'{unauthorise,refusal_code}','NONE'));
exception when others then
  perform pg_temp.proof('UNA-013','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$una_013$;

-- ===========================================================================
-- 4. UNA-012 — an ordinary, non-Weekly-Source-managed Timesheet.  The Weekly
--    Source owner refuses it, and the ordinary owner's own behaviour is
--    unchanged: it is driven here, on a root this project never managed.
-- ===========================================================================
do $una_012$
declare
  v_ts uuid:='c6000000-0000-4000-8000-000000000304';
  v_actor uuid:='c6000000-0000-4000-8000-000000000001';
  v_result jsonb;
  v_ordinary jsonb;
  v_signature text;
  v_authorised timestamptz;
begin
  -- Authorise through the ORDINARY owner, so no Weekly Source generation exists.
  v_ordinary:=public.timesheet_authorise_generic_atomic(
    p_timesheet_id=>v_ts,p_expected_timesheet_id=>v_ts,p_actor_user_id=>v_actor);
  perform pg_temp.drain_workbench_jobs();
  perform pg_temp.check('UNA-012',coalesce((v_ordinary->>'ok')::boolean,false),
    'the ordinary authorise owner accepted an unmanaged root: ok='||coalesce(v_ordinary->>'ok','?'));

  v_signature:=pg_temp.current_signature(v_ts);
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(v_ts,v_ts,v_signature,v_actor);
  perform pg_temp.check('UNA-012',
    coalesce(v_result->>'code','')='WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT',
    'the Weekly Source withdrawal owner refuses an unmanaged root: '
    ||coalesce(v_result->>'code','NONE'));

  -- The ordinary unauthorise owner still works, unchanged, on the same root.
  v_ordinary:=public.timesheet_unauthorise_atomic(
    p_timesheet_id=>v_ts,p_expected_timesheet_id=>v_ts,p_actor_user_id=>v_actor);
  perform pg_temp.drain_workbench_jobs();
  select root.authorised_at_server into v_authorised
  from public.timesheets root where root.timesheet_id=v_ts;
  perform pg_temp.check('UNA-012',
    coalesce((v_ordinary->>'ok')::boolean,false) and v_authorised is null,
    'public.timesheet_unauthorise_atomic behaves unchanged on an ordinary root: ok='
    ||coalesce(v_ordinary->>'ok','?')||', authorised_at_server cleared');
exception when others then
  perform pg_temp.proof('UNA-012','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$una_012$;

-- ===========================================================================
-- 5. UNA-018 — the replay half, on a rotated family.  An exact replay returns
--    the recorded result without calling the unauthorise owner again, and an
--    old physical id of the family only locates the family: the decision is
--    never received by or moved to it.
-- ===========================================================================
do $una_018$
declare
  v_current uuid:='c6000000-0000-4000-8000-000000000305';
  v_historic uuid:='c6000000-0000-4000-8000-000000000395';
  v_actor uuid:='c6000000-0000-4000-8000-000000000001';
  v_authorise jsonb;
  v_withdraw jsonb;
  v_replay jsonb;
  v_old jsonb;
  v_signature text;
  v_generations_on_historic integer;
  v_withdrawn_rows_before integer;
  v_withdrawn_rows_after integer;
begin
  perform pg_temp.drain_workbench_jobs();
  v_authorise:=public.weekly_source_first_authorise_v1(v_current,v_current,null,v_actor);
  perform pg_temp.drain_workbench_jobs();
  if not coalesce((v_authorise->>'ok')::boolean,false) then
    perform pg_temp.proof('UNA-018','FAIL','EXECUTED',
      'could not authorise the rotated family''s canonical row: '||coalesce(v_authorise->>'code','NONE'));
    return;
  end if;

  v_signature:=pg_temp.current_signature(v_current);
  v_withdraw:=public.weekly_source_first_authorisation_withdraw_v1(
    v_current,v_current,v_signature,v_actor);
  perform pg_temp.drain_workbench_jobs();
  if not coalesce((v_withdraw->>'withdrawn')::boolean,false) then
    perform pg_temp.proof('UNA-018','FAIL','EXECUTED',
      'withdrawal refused unexpectedly: '||coalesce(v_withdraw->>'code','NONE'));
    return;
  end if;

  select count(*) into v_withdrawn_rows_before
  from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_current;

  v_replay:=public.weekly_source_first_authorisation_withdraw_v1(
    v_current,v_current,v_signature,v_actor);

  select count(*) into v_withdrawn_rows_after
  from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_current;

  perform pg_temp.check('UNA-018',
    coalesce((v_replay->>'replayed')::boolean,false)
    and (v_replay->>'root_authorisation_id')=(v_withdraw->>'root_authorisation_id')
    and v_withdrawn_rows_before=v_withdrawn_rows_after,
    'the exact replay returns the recorded result (replayed=true, same generation id) and writes nothing');

  v_old:=public.weekly_source_first_authorisation_withdraw_v1(
    v_historic,v_historic,v_signature,v_actor);
  select count(*) into v_generations_on_historic
  from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_historic;
  perform pg_temp.check('UNA-018',
    coalesce((v_old->>'ok')::boolean,true)=false and v_generations_on_historic=0,
    'an old physical id of the family is refused with '||coalesce(v_old->>'code','NONE')
    ||' and no generation is ever created on it');
exception when others then
  perform pg_temp.proof('UNA-018','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$una_018$;

-- ===========================================================================
-- Results
-- ===========================================================================
select 'WS16C_PROOF|'||result_row.proof_id||'|'||result_row.result||'|'
       ||result_row.evidence||'|'||result_row.detail as line
from wp16c_results as result_row
order by result_row.ordinal;

rollback;
