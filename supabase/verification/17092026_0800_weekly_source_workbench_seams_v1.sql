-- Rollback-only PostgreSQL 17.11 proof for the two Weekly Source Plan 6.2
-- Workbench seams (Gate 4):
--
--   private.pay_workbench_unit_economic_occurrence_page_v1   (27 section 5.1)
--   private.pay_workbench_timesheet_input_fingerprint_v1     (27 section 5.2)
--
-- Authority: `P:\27_WORKBENCH_COMPATIBILITY_AND_BANKING_PAY_HANDOFF.md`
-- sections 2, 5.1, 5.2 and 6, word for word; `P:\26_…LEDGER.md` Gate 4;
-- `P:\24_…AUTHORITY.md` section 4.3 (line 141 and line 145);
-- `P:\03_IMPLEMENTATION_SPECIFICATION.md` WB-008, WB-009, WB-013;
-- `H2-004`, `H2-025`; `SPI-081` to `SPI-085`.
--
-- The NO-HEAD byte-identity differential cannot live in this file: it needs the
-- pre-change and post-change definitions of the same function over the same
-- data, which is two definitions and therefore two transactions.  It is run
-- separately over a clone populated from `tests/weekly-source/fixtures-banking`
-- and recorded in `plan6-2-implementation\reports\WP-10_REPORT.md` section 3.
-- What this file proves about the no-head case is that the ordinary TSFIN
-- occurrences and the ordinary adjustments are all still composed, and that the
-- fingerprint carries no entitlement key at all.
--
-- Banking Pay: nothing here calls, defines, wraps or infers any Banking Pay
-- owner (contract decision D2).  The Workbench build set-up shape is the
-- repository's own established one for exercising this selector,
-- `tests\13082026_1942_banking_pay_james_rate_authority_runtime_verification.sql`
-- lines 154-256 (session, economic build, sealed build scope, UNIT_PROJECTION
-- fact).  Head rows are a NAMED SEED written in the order WP-01b fixed
-- (`reports\WP-01a_REPORT.md`, "Review response (WP-01b)" section 0): bundle,
-- head, components, receipt, activate.  Section 3 then makes WP-01a's three
-- DEFERRABLE INITIALLY DEFERRED constraint triggers immediate **by name** -
-- weekly_source_entitlement_head_inventory_assert,
-- weekly_source_entitlement_head_component_inventory_assert and
-- weekly_source_entitlement_head_receipt_assert - so they fire against the seed
-- and prove it has the shape the real coordinator produces.
--
-- `set constraints all immediate` and `set constraints all deferred` are
-- FORBIDDEN in this file and appear nowhere in it.  `all immediate` would fire
-- every deferrable constraint with pending events in the transaction, including
-- the Banking Pay finalisation trigger
-- trg_pay_workbench_scope_change_finalize_v1 on
-- public.banking_pay_scope_change_transactions; `all deferred` would defer three
-- initially-immediate Banking Pay Workbench foreign keys for the rest of the
-- transaction.  A verifier fires its own assertions and nothing else.  The full
-- reasoning is at the first site, in section 3.
--
-- Everything here runs inside one transaction that ends in `rollback`.

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

create function pg_temp.assert_eq(p_actual anyelement,p_expected anyelement,p_message text)
returns void language plpgsql as $function$
begin
  if p_actual is distinct from p_expected then
    raise exception 'ASSERTION_FAILED: % (actual=% expected=%)',
      p_message,coalesce(p_actual::text,'<null>'),coalesce(p_expected::text,'<null>');
  end if;
end;
$function$;

-- Runs one statement, requires it to fail, and requires the message to contain
-- the given fragment.  A statement that SUCCEEDS is a failure.
create function pg_temp.expect_failure(p_sql text,p_message_fragment text,p_label text)
returns void language plpgsql as $function$
declare
  v_message text;
begin
  begin
    execute p_sql;
  exception when others then
    get stacked diagnostics v_message=message_text;
    if p_message_fragment is not null and pg_catalog.strpos(v_message,p_message_fragment)=0 then
      raise exception 'ASSERTION_FAILED: % expected message containing %, got %',
        p_label,p_message_fragment,v_message;
    end if;
    return;
  end;
  raise exception 'ASSERTION_FAILED: % succeeded but had to fail closed',p_label;
end;
$function$;

-- ======================================================================= --
-- 0. Preflight: both seams are installed with the exact signatures the
--    Workbench calls, and the Gate 1 head relations exist.
-- ======================================================================= --
do $verify_preflight$
begin
  perform pg_temp.assert_true(
    to_regprocedure('private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer)')
      is not null,'selector signature unchanged');
  perform pg_temp.assert_true(
    to_regprocedure('private.pay_workbench_timesheet_input_fingerprint_v1(uuid,uuid,uuid[])')
      is not null,'fingerprint signature unchanged');
  perform pg_temp.assert_true(
    to_regclass('public.weekly_source_entitlement_heads') is not null
    and to_regclass('public.weekly_source_entitlement_head_components') is not null,
    'Gate 1 head relations present (WB-012 release ordering)');

  -- 27 section 5.2 and 24 line 145: the fingerprint must not scan or
  -- concatenate an unbounded component population.  Static proof: it never
  -- names the component relation at all.
  perform pg_temp.assert_eq(
    pg_catalog.strpos(pg_catalog.pg_get_functiondef(
      'private.pay_workbench_timesheet_input_fingerprint_v1(uuid,uuid,uuid[])'::regprocedure),
      'weekly_source_entitlement_head_components'),
    0,'fingerprint never references the head component relation');

  -- 27 section 6: the selector still owns exactly three fact families and the
  -- two branches this project does not touch are still there.
  perform pg_temp.assert_true(
    pg_catalog.strpos(pg_catalog.pg_get_functiondef(
      'private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer)'::regprocedure),
      'FROZEN_SETTLED_COMPONENT')>0
    and pg_catalog.strpos(pg_catalog.pg_get_functiondef(
      'private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer)'::regprocedure),
      'PAY_STATE_FALLBACK')>0,
    'the other two selector branches are still present');
end;
$verify_preflight$;

-- ======================================================================= --
-- 1. A rollback-only world: one authorised root with a rich current TSFIN
--    (two dated segments, two additional units, two source expenses), three
--    ordinary non-advance ts_pay_adjustments, one as_advance adjustment that
--    must NEVER be emitted, and one sealed Workbench unit projection over it.
-- ======================================================================= --
create table pg_temp.world(k text primary key,v uuid);
create table pg_temp.worldt(k text primary key,v text);

do $verify_world$
declare
  v_agency uuid:='17092026-0800-4000-8000-000000000000';
  v_actor uuid:='17092026-0800-4000-8000-000000000001';
  v_client uuid:='17092026-0800-4000-8000-000000000002';
  v_contract uuid:='17092026-0800-4000-8000-000000000003';
  v_candidate uuid:='17092026-0800-4000-8000-000000000004';
  v_timesheet uuid:='17092026-0800-4000-8000-000000000005';
  v_snapshot uuid:='17092026-0800-4000-8000-000000000006';
  v_session uuid:='17092026-0800-4000-8000-000000000007';
  v_build uuid:='17092026-0800-4000-8000-000000000008';
  v_booking text:='WP10-SEAM-0001';
  v_unit text;
  v_now timestamptz:=clock_timestamp();
  v_financial uuid;
begin
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(v_actor,'wp10-seams-'||replace(v_actor::text,'-','')||'@example.invalid',
    'UNUSABLE_ROLLBACK_VERIFIER','admin',true);

  insert into public.clients(id,name) values(v_client,'WP10 seam client');

  -- The installed _timesheet_settings_authority_before_v1 trigger resolves
  -- contract settings on insert, which needs a client_settings row.
  insert into public.client_settings(client_id) values(v_client);

  insert into public.contracts(id,client_id,start_date,end_date,pay_method_snapshot)
  values(v_contract,v_client,date '2026-01-01',date '2026-12-31','PAYE');

  insert into public.candidates(id,display_name,tms_ref,pay_method)
  values(v_candidate,'WP10 seam candidate',
    'WP10-'||replace(v_candidate::text,'-',''),'PAYE');

  insert into public.settings_finance_windows(
    date_from,date_to,vat_rate_pct,erni_pct,holiday_pay_pct)
  select date '2026-01-01',date '2026-12-31',20,15.05,12.07
  where not exists(select 1 from public.settings_finance_windows w
    where date '2026-03-20' between w.date_from and coalesce(w.date_to,'infinity'::date));

  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,authorised_at_server,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date,
    sheet_scope,contract_id)
  values(v_timesheet,v_booking,1,true,timestamptz '2026-03-16 09:00:00+00',
    'wp10-occupant','wp10-hospital','wp10-ward','wp10-job',date '2026-03-15',
    'WEEKLY',v_contract);

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,is_current,candidate_id,client_id,pay_method,
    computed_at_utc,total_pay_ex_vat,total_charge_ex_vat,
    hours_day,hours_night,pay_day,pay_night,charge_day,charge_night,
    travel_pay_ex_vat,travel_charge_ex_vat,other_pay_ex_vat,other_charge_ex_vat,
    additional_units_json,invoice_breakdown_json)
  values(v_timesheet,1,true,v_candidate,v_client,'PAYE',v_now,80.00,110.00,
    8,2,8,8,11,11,12.50,12.50,3.00,3.00,
    jsonb_build_object(
      'ONCALL',jsonb_build_object('unit_count',2,'pay_rate',5,'charge_rate',7),
      'SLEEPIN',jsonb_build_object('unit_count',1,'pay_rate',15,'charge_rate',21)),
    jsonb_build_object('mode','SEGMENTS','segments',jsonb_build_array(
      jsonb_build_object('segment_id','wp10-seg-1','segment_key','wp10-seg-1',
        'segment_stable_key','wp10-seg-1','date','2026-03-09',
        'hours_day',5,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
        'pay_amount',40.00,'charge_amount',55.00,'exclude_from_pay',false),
      jsonb_build_object('segment_id','wp10-seg-2','segment_key','wp10-seg-2',
        'segment_stable_key','wp10-seg-2','date','2026-03-10',
        'hours_day',3,'hours_night',2,'hours_sat',0,'hours_sun',0,'hours_bh',0,
        'pay_amount',40.00,'charge_amount',55.00,'exclude_from_pay',false))))
  returning id into v_financial;

  -- SPI-081/082/083/085: two unpaid and one already-paid ordinary non-advance
  -- adjustment, plus one as_advance adjustment the selector must never emit.
  insert into public.ts_pay_adjustments(
    id,timesheet_id,candidate_id,client_id,week_ending_date,delta_pay_ex_vat,
    reason,as_advance,paid_at_utc)
  values
    ('17092026-0800-4000-8000-000000000101',v_timesheet,v_candidate,v_client,
      date '2026-03-15',12.34,'WP10 unpaid adjustment',false,null),
    ('17092026-0800-4000-8000-000000000102',v_timesheet,v_candidate,v_client,
      date '2026-03-15',-5.00,'WP10 PAID adjustment (SPI-083)',false,
      timestamptz '2026-03-18 12:00:00+00'),
    ('17092026-0800-4000-8000-000000000103',v_timesheet,v_candidate,v_client,
      date '2026-03-15',7.50,'WP10 second unpaid adjustment',false,null),
    ('17092026-0800-4000-8000-000000000104',v_timesheet,v_candidate,v_client,
      date '2026-03-15',99.99,'WP10 ADVANCE - never emitted',true,null);

  insert into public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active)
  values(v_snapshot,date '2026-03-20',date '2026-03-15',date '2026-03-09',
    date '2026-01-01',date '2026-03-15','OPEN',false);

  insert into public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version)
  values(v_session,v_actor,date '2026-03-20',date '2026-03-15',
    'WP10_SEAMS:'||v_session::text,v_snapshot,'OPEN',1);

  v_unit:='UNIT:'||lower(v_timesheet::text);

  insert into private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,
    source_build_run_id,source_job_id,captured_candidate_generation,
    source_change_seq,status,private_stage,
    seed_scope_count,seed_scope_digest,seed_scope_sealed_at_utc,
    scope_count,dependency_node_count,dependency_edge_count,tagged_edge_count,
    row_seal_count,last_stable_ordinal,scope_cursor_json,closure_cursor_json,
    dependency_edge_stream_complete,dependency_edge_stream_digest,
    edge_tag_stream_complete,edge_tag_digest,unit_digest,scope_digest,
    dependency_digest,sealed_fingerprint_digest,dependency_closure_sealed_at_utc,
    obsolete_at_utc,created_at_utc,updated_at_utc)
  values(v_build,v_candidate,v_session,1,v_snapshot,
    gen_random_uuid(),null,0,0,'OBSOLETE','WORKSPACE_FACT',
    1,md5('WP10_SEED'),v_now,1,1,0,0,1,1,'{"terminal":true}'::jsonb,
    '{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb,
    true,md5(''),true,md5(''),md5('WP10_UNIT'),md5('WP10_SCOPE'),
    md5('WP10_DEPENDENCY'),md5('WP10_FINGERPRINT'),v_now,v_now,v_now,v_now);

  insert into private.banking_pay_workbench_economic_build_scope(
    build_id,timesheet_id,candidate_id,root_timesheet_id,stable_ordinal,
    dependency_unit_anchor_timesheet_id,dependency_unit_key,dependency_unit_digest,
    captured_input_fingerprint,closure_status,seal_prepared_at_utc,
    completed_fact_families,fact_row_count,fact_digest,
    seed_reasons,dependency_reasons,captured_dirty_generation,required_fact_families)
  values(v_build,v_timesheet,v_candidate,v_timesheet,1,
    v_timesheet,v_unit,md5('WP10_UNIT_DIGEST'),md5('WP10_INPUT_FP'),'SEALED',v_now,
    array['LIVE_ENTITLEMENT_INPUT'],1,md5('WP10_FACTS'),
    array['WP10_FIXTURE'],array[]::text[],0,array['LIVE_ENTITLEMENT_INPUT']);

  insert into private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
    truth_ex_vat,financial_digest,source_ordinal)
  values(v_build,'LIVE_ENTITLEMENT_INPUT','wp10-unit-projection',
    v_candidate,v_timesheet,array[v_timesheet],v_unit,'UNIT_PROJECTION',
    v_timesheet,'TS_TOTAL','TOTAL',0,md5('WP10_UNIT_PROJECTION'),1);

  insert into private.banking_pay_workbench_timesheet_scope_state(
    timesheet_id,candidate_id,dirty_generation,economic_state,last_dirty_reason)
  values(v_timesheet,v_candidate,1,'DIRTY','WP10_FIXTURE')
  on conflict do nothing;

  insert into pg_temp.world(k,v) values
    ('agency',v_agency),('actor',v_actor),('client',v_client),('contract',v_contract),
    ('candidate',v_candidate),('timesheet',v_timesheet),('session',v_session),
    ('build',v_build),('financial',v_financial);
  insert into pg_temp.worldt(k,v) values('booking',v_booking),('unit',v_unit);
end;
$verify_world$;

-- One paged walk of the selector, exactly as a caller walks it.
create function pg_temp.walk()
returns table(source_key text,source_relation text,source_id uuid,
  economic_key_type text,economic_key_value text,truth_ex_vat numeric,
  source_payload_json jsonb,financial_digest text,resolution_failure text)
language plpgsql stable as $walk$
declare
  v_build uuid:=(select v from pg_temp.world where k='build');
  v_ts uuid:=(select v from pg_temp.world where k='timesheet');
  v_unit text:=(select v from pg_temp.worldt where k='unit');
  v_cursor text:=null;
  v_count integer;
  v_page integer:=0;
  r record;
begin
  loop
    v_page:=v_page+1; v_count:=0;
    for r in select * from private.pay_workbench_unit_economic_occurrence_page_v1(
      v_build,v_unit,'LIVE_ENTITLEMENT_INPUT',v_ts,v_cursor,25)
    loop
      v_count:=v_count+1;
      source_key:=r.source_key; source_relation:=r.source_relation; source_id:=r.source_id;
      economic_key_type:=r.economic_key_type; economic_key_value:=r.economic_key_value;
      truth_ex_vat:=r.truth_ex_vat; source_payload_json:=r.source_payload_json;
      financial_digest:=r.financial_digest; resolution_failure:=r.resolution_failure;
      v_cursor:=r.source_key;
      return next;
    end loop;
    exit when v_count<25 or v_page>20;
  end loop;
end;
$walk$;

-- Every ordinary non-advance adjustment occurrence, as one canonical text, so
-- "emitted exactly once and unchanged" is a single comparison.
create function pg_temp.adjustment_signature()
returns text language sql stable as $sig$
  select coalesce(string_agg(
    w.source_key||'~'||w.source_relation||'~'||w.source_id::text||'~'
    ||coalesce(w.economic_key_type,'')||'~'||coalesce(w.economic_key_value,'')||'~'
    ||w.truth_ex_vat::text||'~'||w.financial_digest,'|' order by w.source_key),'<none>')
  from pg_temp.walk() w
  where w.source_relation='ts_pay_adjustments';
$sig$;

-- WP-10c.  The same page walk with a caller-chosen page size, so paging can be
-- proved exact after ruling B2's suppression rather than assumed.  The selector
-- returns up to p_limit+1 rows per call (one look-ahead parent), so a page that
-- comes back short is the last one.
create function pg_temp.walk_n(p_limit integer)
returns table(source_key text,source_relation text,source_id uuid,
  economic_key_type text,economic_key_value text,truth_ex_vat numeric,
  source_payload_json jsonb,financial_digest text,resolution_failure text)
language plpgsql stable as $walkn$
declare
  v_build uuid:=(select v from pg_temp.world where k='build');
  v_ts uuid:=(select v from pg_temp.world where k='timesheet');
  v_unit text:=(select v from pg_temp.worldt where k='unit');
  v_cursor text:=null;
  v_count integer;
  v_page integer:=0;
  r record;
begin
  loop
    v_page:=v_page+1; v_count:=0;
    for r in select * from private.pay_workbench_unit_economic_occurrence_page_v1(
      v_build,v_unit,'LIVE_ENTITLEMENT_INPUT',v_ts,v_cursor,p_limit)
    loop
      v_count:=v_count+1;
      source_key:=r.source_key; source_relation:=r.source_relation; source_id:=r.source_id;
      economic_key_type:=r.economic_key_type; economic_key_value:=r.economic_key_value;
      truth_ex_vat:=r.truth_ex_vat; source_payload_json:=r.source_payload_json;
      financial_digest:=r.financial_digest; resolution_failure:=r.resolution_failure;
      v_cursor:=r.source_key;
      return next;
    end loop;
    exit when v_count<p_limit or v_page>400;
  end loop;
end;
$walkn$;

-- WP-10c.  EVERY occurrence, not only the adjustments, as one canonical text.
-- The no-head case is compared with this so "an ordinary Timesheet is untouched
-- and reaches none of the head refusals" is one executed comparison.
create function pg_temp.full_signature()
returns text language sql stable as $fullsig$
  select coalesce(string_agg(
    w.source_key||'~'||w.source_relation||'~'||coalesce(w.source_id::text,'')||'~'
    ||coalesce(w.economic_key_type,'')||'~'||coalesce(w.economic_key_value,'')||'~'
    ||coalesce(w.truth_ex_vat::text,'')||'~'||coalesce(w.financial_digest,'')||'~'
    ||coalesce(w.resolution_failure,''),'|' order by w.source_key),'<none>')
  from pg_temp.walk() w;
$fullsig$;

create function pg_temp.fingerprint_json()
returns jsonb language sql stable as $fp$
  select fp.revision_json
  from private.pay_workbench_timesheet_input_fingerprint_v1(
    (select v from pg_temp.world where k='build'),
    (select v from pg_temp.world where k='candidate'),
    array[(select v from pg_temp.world where k='timesheet')]) fp;
$fp$;

-- The named head seed, in WP-01b's fixed write order.
create function pg_temp.seed_head(
  p_head_id uuid,p_bundle_id uuid,p_components jsonb,p_state text default 'STAGED')
returns uuid language plpgsql as $seed$
declare
  v_ts uuid:=(select v from pg_temp.world where k='timesheet');
  v_booking text:=(select v from pg_temp.worldt where k='booking');
  v_candidate uuid:=(select v from pg_temp.world where k='candidate');
  v_contract uuid:=(select v from pg_temp.world where k='contract');
  v_agency uuid:=(select v from pg_temp.world where k='agency');
  v_actor uuid:=(select v from pg_temp.world where k='actor');
  v_count integer:=jsonb_array_length(p_components);
  v_digest bytea:=sha256(convert_to('WP10_REQUEST:'||p_head_id::text,'UTF8'));
  v_token uuid:=gen_random_uuid();
  v_now timestamptz:=clock_timestamp();
  v_component jsonb;
  v_i integer;
  v_revision bigint;
  v_prior uuid;
begin
  -- weekly_source_entitlement_heads_family_revision_uq: head_revision is
  -- monotonic per family, and check((head_revision=1)=(prior_head_id is null)).
  select coalesce(pg_catalog.max(head_row.head_revision),0)+1 into v_revision
  from public.weekly_source_entitlement_heads head_row
  where pg_catalog.btrim(head_row.root_family_booking_id)=pg_catalog.btrim(v_booking);
  if v_revision>1 then
    select pg_catalog.min(head_row.id::text)::uuid into v_prior
    from public.weekly_source_entitlement_heads head_row
    where pg_catalog.btrim(head_row.root_family_booking_id)=pg_catalog.btrim(v_booking)
      and head_row.head_revision=v_revision-1;
  end if;

  -- 1. the decision bundle revision
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state,committed_at_utc)
  values(p_bundle_id,1,v_agency,v_candidate,date '2026-03-15',
    'SINGLE_ROOT',v_booking,v_ts,v_contract,gen_random_uuid(),v_actor,'IMMEDIATE',
    v_digest,sha256('src'::bytea),sha256('contract'::bytea),sha256('before'::bytea),
    array[p_head_id],'COMMITTED',v_now);

  -- 2. the head
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,inventory_digest,
    source_generation_digest,publication_receipt_digest,decision_bundle_id,
    bundle_revision,decision_id,decided_by_user_id,scope_change_tx_token,
    committed_at_utc)
  values(p_head_id,'LOCKED_FINAL_SOURCE',v_agency,v_candidate,v_contract,
    date '2026-03-15',v_ts,v_booking,1,v_revision,
    v_prior,p_state,v_count=0,v_count,sha256('ent'::bytea),sha256('inv'::bytea),
    sha256('srcgen'::bytea),
    case when p_state='COMMITTED_CURRENT' then v_digest end,p_bundle_id,1,
    gen_random_uuid(),v_actor,
    case when p_state='COMMITTED_CURRENT' then v_token end,
    case when p_state='COMMITTED_CURRENT' then v_now end);

  -- 3. the components, tagged with the same bundle pair
  for v_i in 0..greatest(v_count-1,0) loop
    exit when v_count=0;
    v_component:=p_components->v_i;
    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,segment_id,segment_key,
      segment_stable_key,work_date,reference_number,hours_day,hours_night,
      hours_sat,hours_sun,hours_bh,additional_code_raw,unit_count,unit_pay_rate,
      unit_charge_rate,expense_code,pay_ex_vat,charge_ex_vat,exclude_from_pay,
      origin,decision_bundle_id,bundle_revision,component_sha256)
    values(p_head_id,v_i+1,(v_component->>'component_id')::uuid,
      v_component->>'component_kind',v_component->>'economic_key_type',
      v_component->>'economic_key_value',v_component->>'component_member_identity',
      v_component->>'segment_id',v_component->>'segment_key',
      v_component->>'segment_stable_key',(v_component->>'work_date')::date,
      v_component->>'reference_number',(v_component->>'hours_day')::numeric,
      (v_component->>'hours_night')::numeric,(v_component->>'hours_sat')::numeric,
      (v_component->>'hours_sun')::numeric,(v_component->>'hours_bh')::numeric,
      v_component->>'additional_code_raw',(v_component->>'unit_count')::numeric,
      (v_component->>'unit_pay_rate')::numeric,(v_component->>'unit_charge_rate')::numeric,
      v_component->>'expense_code',(v_component->>'pay_ex_vat')::numeric,
      (v_component->>'charge_ex_vat')::numeric,
      coalesce((v_component->>'exclude_from_pay')::boolean,false),
      'WEEKLY_SOURCE',p_bundle_id,1,
      sha256(convert_to(v_component::text,'UTF8')));
  end loop;

  -- 4. the immutable publication receipt
  insert into private.weekly_source_entitlement_publication_receipts(
    decision_bundle_id,bundle_revision,request_digest,publication_mode,
    candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,
    head_ids,scope_change_tx_token,decision_id,decided_by_user_id,
    census_json,proof_json)
  values(p_bundle_id,1,v_digest,'IMMEDIATE',v_candidate,array[v_ts],
    array[v_booking],array[1],array[p_head_id],v_token,gen_random_uuid(),v_actor,
    '{}'::jsonb,'{}'::jsonb);

  return p_head_id;
end;
$seed$;

-- WP-01b section 0 step 5: supersede the outgoing head, THEN activate the
-- incoming one.  The two committed-current indexes are bare unique indexes and
-- cannot be deferred, and the superseded_by_head_id foreign key means the
-- incoming head must already exist as STAGED before the outgoing one can point
-- at it.  So: seed STAGED, supersede, activate.
create function pg_temp.promote(p_head_id uuid)
returns void language plpgsql as $promote$
declare
  v_outgoing uuid;
begin
  select id into v_outgoing from public.weekly_source_entitlement_heads
  where state='COMMITTED_CURRENT'
    and root_timesheet_id=(select v from pg_temp.world where k='timesheet');

  if v_outgoing is not null then
    update public.weekly_source_entitlement_heads
       set state='SUPERSEDED',superseded_at_utc=clock_timestamp(),
           superseded_by_head_id=p_head_id
     where id=v_outgoing;
  end if;

  update public.weekly_source_entitlement_heads head_row
     set state='COMMITTED_CURRENT',committed_at_utc=clock_timestamp(),
         publication_receipt_digest=receipt_row.request_digest,
         scope_change_tx_token=receipt_row.scope_change_tx_token
    from private.weekly_source_entitlement_publication_receipts receipt_row
   where head_row.id=p_head_id
     and receipt_row.decision_bundle_id=head_row.decision_bundle_id
     and receipt_row.bundle_revision=head_row.bundle_revision;
end;
$promote$;

-- ======================================================================= --
-- 2. NO HEAD.  27 section 5.1 first bullet, and the adjustment rule.
-- ======================================================================= --
create table pg_temp.baseline(k text primary key,v text);

do $verify_no_head$
declare
  v_segments integer;
  v_additional integer;
  v_expenses integer;
  v_adjustments integer;
  v_advance integer;
  v_fp jsonb;
begin
  select count(*) filter (where w.source_payload_json->>'source_kind'='SEGMENT'),
         count(*) filter (where w.source_payload_json->>'source_kind'='ADDITIONAL'),
         count(*) filter (where w.source_payload_json->>'source_kind'='EXPENSE'),
         count(*) filter (where w.source_relation='ts_pay_adjustments'),
         count(*) filter (where w.source_id='17092026-0800-4000-8000-000000000104')
    into v_segments,v_additional,v_expenses,v_adjustments,v_advance
  from pg_temp.walk() w;

  perform pg_temp.assert_eq(v_segments,2,'no head: both TSFIN segments composed');
  perform pg_temp.assert_eq(v_additional,2,'no head: both TSFIN additional units composed');
  perform pg_temp.assert_eq(v_expenses,2,'no head: both TSFIN source expenses composed');
  perform pg_temp.assert_eq(v_adjustments,3,
    'no head: all three ordinary non-advance adjustments composed exactly once');
  perform pg_temp.assert_eq(v_advance,0,
    'no head: the as_advance adjustment is never emitted (WB-013)');
  perform pg_temp.assert_true(
    (select bool_and(w.source_relation='timesheets_financials')
     from pg_temp.walk() w where w.source_payload_json->>'source_kind'<>'ADJUSTMENT'),
    'no head: every base-authority occurrence is TSFIN-derived');

  insert into pg_temp.baseline(k,v) values('adjustments',pg_temp.adjustment_signature());
  -- WP-10c: the complete ordinary occurrence set, every field, kept so section 8
  -- can prove an ordinary Timesheet is byte-identical after all of WP-10c's new
  -- code exists and after every guard section 6 removes.
  insert into pg_temp.baseline(k,v) values('no_head_full',pg_temp.full_signature());
  -- WP-10c: and the same set walked one page at a time, to fix the baseline for
  -- the paging-exactness assertions.
  insert into pg_temp.baseline(k,v)
  values('no_head_pages_1',(select coalesce(string_agg(w.source_key,'|' order by w.source_key),'<none>')
                            from pg_temp.walk_n(1) w));

  -- 27 section 5.2: with no head the ordinary fingerprint remains equivalent.
  -- Here it is stronger than equivalent: no entitlement key exists at all.
  v_fp:=pg_temp.fingerprint_json();
  perform pg_temp.assert_eq(
    (select count(*)::integer from jsonb_object_keys(v_fp) key_name
     where key_name like 'entitlement%'),0,
    'no head: the fingerprint carries no entitlement key');
  insert into pg_temp.baseline(k,v) values('fingerprint_no_head',v_fp::text);
end;
$verify_no_head$;

-- ======================================================================= --
-- 3. ONE VALID COMMITTED HEAD.  27 section 5.1 second and third bullets;
--    H2-004; H2-025; SPI-081, SPI-082, SPI-083, SPI-085.
--    The head is four components: two worked-time, one addition, one expense -
--    deliberately DIFFERENT from the TSFIN content, so "replaced, not merged"
--    is visible.
-- ======================================================================= --
create table pg_temp.head_ids(k text primary key,v uuid);

do $verify_one_head$
declare
  v_head uuid:='17092026-0800-4000-8000-000000000201';
  v_bundle uuid:='17092026-0800-4000-8000-000000000301';
  v_tsfin integer;
  v_head_rows integer;
  v_total numeric;
  v_adjustments integer;
begin
  perform pg_temp.seed_head(v_head,v_bundle,jsonb_build_array(
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000401',
      'component_kind','WORKED_TIME','economic_key_type','TS_DAY',
      'economic_key_value','2026-03-09','component_member_identity','wp10-head-seg-1',
      'segment_id','wp10-head-seg-1','segment_key','wp10-head-seg-1',
      'segment_stable_key','wp10-head-seg-1','work_date','2026-03-09',
      'hours_day',6,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
      'pay_ex_vat',48.00,'charge_ex_vat',66.00,'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000402',
      'component_kind','WORKED_TIME','economic_key_type','TS_DAY',
      'economic_key_value','2026-03-10','component_member_identity','wp10-head-seg-2',
      'segment_id','wp10-head-seg-2','segment_key','wp10-head-seg-2',
      'segment_stable_key','wp10-head-seg-2','work_date','2026-03-10',
      'hours_day',2,'hours_night',1,'hours_sat',0,'hours_sun',0,'hours_bh',0,
      'pay_ex_vat',24.00,'charge_ex_vat',33.00,'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000403',
      'component_kind','ADDITIONAL_UNIT','economic_key_type','ADDITIONAL_CODE',
      'economic_key_value','ONCALL','component_member_identity','additional:ONCALL',
      'additional_code_raw','oncall','unit_count',3,'unit_pay_rate',5,
      'unit_charge_rate',7,'pay_ex_vat',15.00,'charge_ex_vat',21.00,
      'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000404',
      'component_kind','EXPENSE','economic_key_type','EXPENSE_CODE',
      'economic_key_value','TRAVEL','component_member_identity','expense:TRAVEL',
      'expense_code','travel','pay_ex_vat',20.00,'charge_ex_vat',20.00,
      'exclude_from_pay',false)));
  perform pg_temp.promote(v_head);

  -- WP-01a's DEFERRABLE INITIALLY DEFERRED asserts fire here, against the seed,
  -- so the seed is proved to have the shape the real coordinator writes.
  --
  -- NAMED, NEVER `all`.  `set constraints all immediate` would fire every other
  -- deferrable constraint that has pending events in this transaction, and this
  -- database carries 22 deferrable constraints - among them the Banking Pay
  -- DEFERRABLE INITIALLY DEFERRED finalisation trigger
  -- trg_pay_workbench_scope_change_finalize_v1 on
  -- public.banking_pay_scope_change_transactions, which takes FOR UPDATE locks
  -- and drives queue and counter finalisation.  `set constraints all deferred`
  -- is the mirror hazard: it would additionally make three initially-IMMEDIATE
  -- Banking Pay Workbench foreign keys (bpay_wb_economic_builds_source_job_fk,
  -- bpay_wb_scope_registry_current_build_fk,
  -- bpay_wb_timesheet_scope_state_current_build_fk) deferred for the rest of
  -- the transaction.  A verifier must fire nothing but its own assertions, so
  -- both statements name exactly WP-01a's three constraint triggers and nothing
  -- else.  The three are listed once here and repeated at each later site.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  -- back to WP-01a's own mode for those three only: SET CONSTRAINTS is
  -- transaction-scoped, and the next seed writes its head before its components.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;

  select count(*) filter (where w.source_relation='timesheets_financials'),
         count(*) filter (where w.source_relation='weekly_source_entitlement_head_components'),
         count(*) filter (where w.source_relation='ts_pay_adjustments')
    into v_tsfin,v_head_rows,v_adjustments
  from pg_temp.walk() w;

  perform pg_temp.assert_eq(v_tsfin,0,
    'one head: NO TSFIN-derived occurrence survives (H2-004 "never both")');
  perform pg_temp.assert_eq(v_head_rows,4,
    'one head: every one of the four head components is composed exactly once');
  perform pg_temp.assert_eq(v_adjustments,3,
    'one head: the three ordinary adjustments are still composed exactly once (SPI-081)');
  perform pg_temp.assert_eq(pg_temp.adjustment_signature(),
    (select v from pg_temp.baseline where k='adjustments'),
    'one head: each adjustment occurrence is byte-identical to the no-head case '
    ||'(H2-025 "composed exactly once outside that replacement")');
  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk() w
     where w.source_id='17092026-0800-4000-8000-000000000104'),0,
    'one head: the as_advance adjustment is still never emitted');

  -- The head is the money, the TSFIN is not.
  select sum(w.truth_ex_vat) into v_total
  from pg_temp.walk() w
  where w.source_relation='weekly_source_entitlement_head_components';
  perform pg_temp.assert_eq(v_total,107.00::numeric,
    'one head: the emitted base authority is the HEAD total, not the TSFIN total');

  -- No head component was resolved through a TSFIN row identity.
  perform pg_temp.assert_true(
    (select bool_and(w.source_id in (
        select c.id from public.weekly_source_entitlement_head_components c
        where c.head_id=v_head))
     from pg_temp.walk() w
     where w.source_relation='weekly_source_entitlement_head_components'),
    'one head: each emitted occurrence names its own head component row');

  -- Every emitted head occurrence carries the fixed-size head identity, and the
  -- rate authority completed on the root's unchanged rate card.
  perform pg_temp.assert_true(
    (select bool_and((w.source_payload_json#>>'{entitlement_head,head_id}')=v_head::text)
     from pg_temp.walk() w
     where w.source_relation='weekly_source_entitlement_head_components'),
    'one head: head identity evidence present on every head occurrence');
  perform pg_temp.assert_true(
    (select bool_and(w.resolution_failure is null)
     from pg_temp.walk() w),
    'one head: no occurrence carries a resolution failure');

  -- Adjustments are never copied into a head: the relation has no adjustment
  -- column and the selector never emits an adjustment from a head component.
  perform pg_temp.assert_eq(
    (select count(*)::integer from information_schema.columns
     where table_schema='public'
       and table_name='weekly_source_entitlement_head_components'
       and column_name in ('adjustment_id','ts_pay_adjustment_id')),0,
    'one head: the component relation carries no adjustment identity (WB-007)');

  insert into pg_temp.head_ids(k,v) values('first',v_head);
  insert into pg_temp.baseline(k,v) values('fingerprint_one_head',pg_temp.fingerprint_json()::text);
end;
$verify_one_head$;

-- ======================================================================= --
-- 4. The fingerprint with a head (27 section 5.2; 24 line 145).
-- ======================================================================= --
do $verify_fingerprint$
declare
  v_fp jsonb:=(select v::jsonb from pg_temp.baseline where k='fingerprint_one_head');
  v_head uuid:=(select v from pg_temp.head_ids where k='first');
  v_next uuid:='17092026-0800-4000-8000-000000000204';
  v_bundle uuid:='17092026-0800-4000-8000-000000000304';
  v_before text;
  v_after text;
begin
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_count','1','fingerprint: head count');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_authority_kind','LOCKED_FINAL_SOURCE',
    'fingerprint: authority kind');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_id',v_head::text,'fingerprint: head id');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_revision','1','fingerprint: monotonic revision');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_component_count','4',
    'fingerprint: component count');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_certified_zero','false',
    'fingerprint: certified-zero flag');
  perform pg_temp.assert_eq(v_fp->>'entitlement_digest',encode(sha256('ent'::bytea),'hex'),
    'fingerprint: entitlement digest');
  perform pg_temp.assert_eq(v_fp->>'entitlement_inventory_digest',encode(sha256('inv'::bytea),'hex'),
    'fingerprint: inventory digest');
  perform pg_temp.assert_eq(v_fp->>'entitlement_source_generation_digest',
    encode(sha256('srcgen'::bytea),'hex'),'fingerprint: source-generation digest');
  perform pg_temp.assert_true(v_fp->>'entitlement_receipt_digest' is not null,
    'fingerprint: receipt digest');

  -- Exactly the nine facts of 27 section 5.2 plus the ambiguity counter, and
  -- every one of them a fixed-size scalar: no list, array or concatenation of
  -- components (24 line 145).
  perform pg_temp.assert_eq(
    (select count(*)::integer from jsonb_object_keys(v_fp) key_name
     where key_name like 'entitlement%'),10,
    'fingerprint: exactly the nine 27 section 5.2 facts plus entitlement_head_count');
  perform pg_temp.assert_true(
    (select bool_and(jsonb_typeof(v_fp->key_name) in ('string','number','boolean'))
     from jsonb_object_keys(v_fp) key_name where key_name like 'entitlement%'),
    'fingerprint: every added head fact is a fixed-size scalar');

  -- It moves when a head fact moves.  A head scalar is immutable under
  -- WP-01a's _weekly_source_immutable_fact_guard_v1, which this verifier never
  -- disables, so the only way a head fact changes is the coordinator's own way:
  -- supersede this head and activate the next revision.  That is what is proved.
  v_before:=pg_temp.fingerprint_json()::text;
  perform pg_temp.assert_true(
    v_before is distinct from (select v from pg_temp.baseline where k='fingerprint_no_head'),
    'fingerprint: a head moves the fingerprint away from the no-head value');

  perform pg_temp.seed_head(v_next,v_bundle,jsonb_build_array(
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000406',
      'component_kind','WORKED_TIME','economic_key_type','TS_DAY',
      'economic_key_value','2026-03-09','component_member_identity','wp10-rev2-seg',
      'segment_id','wp10-rev2-seg','segment_key','wp10-rev2-seg',
      'segment_stable_key','wp10-rev2-seg','work_date','2026-03-09',
      'hours_day',4,'pay_ex_vat',32.00,'charge_ex_vat',44.00,'exclude_from_pay',false)));
  perform pg_temp.promote(v_next);
  -- Named, never `all` - see the note at the first site in section 3.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  -- back to WP-01a's own mode for those three only: the next seed writes its
  -- head before its components.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;

  v_after:=pg_temp.fingerprint_json()::text;
  perform pg_temp.assert_true(v_before is distinct from v_after,
    'fingerprint: publishing the next head revision moves the fingerprint');
  perform pg_temp.assert_eq((v_after::jsonb)->>'entitlement_head_id',v_next::text,
    'fingerprint: it now reports the new head');
  perform pg_temp.assert_eq((v_after::jsonb)->>'entitlement_head_component_count','1',
    'fingerprint: it now reports the new component count');

  -- ONLY when a head fact changes.  The component population is not an input at
  -- all: the definition never names the component relation, so no plan of it
  -- can reach one, however many components any head holds.  Re-proved here on
  -- the installed definition, after the head change above.
  perform pg_temp.assert_eq(
    pg_catalog.strpos(pg_catalog.pg_get_functiondef(
      'private.pay_workbench_timesheet_input_fingerprint_v1(uuid,uuid,uuid[])'::regprocedure),
      'weekly_source_entitlement_head_components'),
    0,'fingerprint: the component relation is never named, so it is never scanned');
  -- And the fingerprint still returns exactly one row per requested Timesheet.
  perform pg_temp.assert_eq(
    (select count(*)::integer from private.pay_workbench_timesheet_input_fingerprint_v1(
      (select v from pg_temp.world where k='build'),
      (select v from pg_temp.world where k='candidate'),
      array[(select v from pg_temp.world where k='timesheet')])),
    1,'fingerprint: exactly one row per requested Timesheet');

  -- The selector followed the head change with it: one component, three
  -- adjustments, and no TSFIN-derived occurrence.
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,
    'selector: the new head revision is the one selected');
  perform pg_temp.assert_eq(pg_temp.adjustment_signature(),
    (select v from pg_temp.baseline where k='adjustments'),
    'selector: the adjustments are still byte-identical across a head revision');

  insert into pg_temp.head_ids(k,v) values('second',v_next);
end;
$verify_fingerprint$;

-- ======================================================================= --
-- 5. CERTIFIED ZERO.  27 section 5.1 fourth bullet; 03 WB-009; SPI-084.
-- ======================================================================= --
do $verify_certified_zero$
declare
  v_head uuid:=(select id from public.weekly_source_entitlement_heads where state='COMMITTED_CURRENT');
  v_zero uuid:='17092026-0800-4000-8000-000000000202';
  v_bundle uuid:='17092026-0800-4000-8000-000000000302';
  v_fp jsonb;
begin
  -- Supersede, then activate: the committed-current indexes are bare unique
  -- indexes and cannot be deferred (WP-01b section 0 step 5).
  perform pg_temp.seed_head(v_zero,v_bundle,'[]'::jsonb);
  perform pg_temp.promote(v_zero);
  -- Named, never `all` - see the note at the first site in section 3.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  -- back to WP-01a's own mode for those three only: the next seed writes its
  -- head before its components.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;

  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk() w
     where w.source_relation<>'ts_pay_adjustments'),0,
    'certified zero: no positive TSFIN-derived economic occurrence (WB-009)');
  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk() w
     where w.source_relation='ts_pay_adjustments'),3,
    'certified zero: the root''s ordinary adjustments are still emitted');
  perform pg_temp.assert_eq(pg_temp.adjustment_signature(),
    (select v from pg_temp.baseline where k='adjustments'),
    'certified zero: each adjustment occurrence is byte-identical to the no-head case (SPI-084)');

  v_fp:=pg_temp.fingerprint_json();
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_certified_zero','true',
    'certified zero: the fingerprint says so');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_component_count','0',
    'certified zero: component count is zero');
  perform pg_temp.assert_true(
    v_fp::text is distinct from (select v from pg_temp.baseline where k='fingerprint_no_head'),
    'certified zero is NOT the same input state as no head');
end;
$verify_certified_zero$;

-- ======================================================================= --
-- 5A. HANDOVER 2 round-5 rulings B2 and B1 (WP-10c).
--
--     B2: zero-valued head ADDITIONS and EXPENSES are suppressed as monetary
--     facts, aligned with the ordinary path.  Worked time is not suppressed,
--     because the ordinary segment path does not suppress it either.
--
--     B1: certified zero still emits no monetary fact row, and the no-row
--     outcome must stay distinguishable from missing/incomplete source evidence
--     and from an expected-nonzero head that produces no fact row.
--
--     Every assertion here EXECUTES the selector.  None inspects its text.
--     All WP-01a guards are still in place throughout this section; section 6
--     is the only place anything is removed.
-- ======================================================================= --
do $verify_b2_suppression$
declare
  v_mixed uuid:='17092026-0800-4000-8000-000000000205';
  v_bundle uuid:='17092026-0800-4000-8000-000000000305';
  v_emitted integer;
  v_total numeric;
  v_keys text;
  v_fp jsonb;
  v_line record;
begin
  -- Six components, deliberately covering every arm of the B2 rule:
  --   1 WORKED_TIME     48.00  emitted
  --   2 ADDITIONAL_UNIT 15.00  emitted
  --   3 ADDITIONAL_UNIT  0.00  SUPPRESSED - the ordinary additions arm of
  --                            raw_rows drops a zero addition
  --   4 EXPENSE         20.00  emitted
  --   5 EXPENSE          0.00  SUPPRESSED - expense_rows drops a zero expense
  --   6 WORKED_TIME      0.00  EMITTED - segment_rows has no zero filter, so
  --                            suppressing this one would NOT be alignment
  perform pg_temp.seed_head(v_mixed,v_bundle,jsonb_build_array(
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000501',
      'component_kind','WORKED_TIME','economic_key_type','TS_DAY',
      'economic_key_value','2026-03-09','component_member_identity','wp10c-seg-1',
      'segment_id','wp10c-seg-1','segment_key','wp10c-seg-1',
      'segment_stable_key','wp10c-seg-1','work_date','2026-03-09',
      'hours_day',6,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
      'pay_ex_vat',48.00,'charge_ex_vat',66.00,'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000502',
      'component_kind','ADDITIONAL_UNIT','economic_key_type','ADDITIONAL_CODE',
      'economic_key_value','ONCALL','component_member_identity','additional:ONCALL',
      'additional_code_raw','oncall','unit_count',3,'unit_pay_rate',5,
      'unit_charge_rate',7,'pay_ex_vat',15.00,'charge_ex_vat',21.00,
      'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000503',
      'component_kind','ADDITIONAL_UNIT','economic_key_type','ADDITIONAL_CODE',
      'economic_key_value','ZEROADD','component_member_identity','additional:ZEROADD',
      'additional_code_raw','zeroadd','unit_count',0,'unit_pay_rate',0,
      'unit_charge_rate',0,'pay_ex_vat',0.00,'charge_ex_vat',0.00,
      'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000504',
      'component_kind','EXPENSE','economic_key_type','EXPENSE_CODE',
      'economic_key_value','TRAVEL','component_member_identity','expense:TRAVEL',
      'expense_code','travel','pay_ex_vat',20.00,'charge_ex_vat',20.00,
      'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000505',
      'component_kind','EXPENSE','economic_key_type','EXPENSE_CODE',
      'economic_key_value','ACCOMMODATION','component_member_identity','expense:ACCOMMODATION',
      'expense_code','accommodation','pay_ex_vat',0.00,'charge_ex_vat',0.00,
      'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000506',
      'component_kind','WORKED_TIME','economic_key_type','TS_DAY',
      'economic_key_value','2026-03-11','component_member_identity','wp10c-seg-zero',
      'segment_id','wp10c-seg-zero','segment_key','wp10c-seg-zero',
      'segment_stable_key','wp10c-seg-zero','work_date','2026-03-11',
      'hours_day',0,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
      'pay_ex_vat',0.00,'charge_ex_vat',0.00,'exclude_from_pay',false)));
  perform pg_temp.promote(v_mixed);
  -- Named, never `all` - see the note at the first site in section 3.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;

  -- 5A.1  The two zero-valued monetary kinds produce NO monetary fact.
  select count(*)::integer into v_emitted from pg_temp.walk() w
  where w.source_relation='weekly_source_entitlement_head_components';
  perform pg_temp.assert_eq(v_emitted,4,
    'B2: 6 components declared, 4 emitted - the zero addition and the zero '
    ||'expense produce no monetary fact');

  select coalesce(string_agg(w.source_key,'|' order by w.source_key),'<none>')
    into v_keys from pg_temp.walk() w
  where w.source_relation='weekly_source_entitlement_head_components';
  perform pg_temp.assert_eq(pg_catalog.strpos(v_keys,':HEADCOMP:000000000003'),0,
    'B2: the zero-valued ADDITIONAL_UNIT (ordinal 3) is absent from the fact stream');
  perform pg_temp.assert_eq(pg_catalog.strpos(v_keys,':HEADCOMP:000000000005'),0,
    'B2: the zero-valued EXPENSE (ordinal 5) is absent from the fact stream');
  perform pg_temp.assert_true(pg_catalog.strpos(v_keys,':HEADCOMP:000000000006')>0,
    'B2: the zero-valued WORKED_TIME (ordinal 6) IS still emitted - the ordinary '
    ||'segment path has no zero filter, so suppressing it would not be alignment');
  perform pg_temp.assert_true(pg_catalog.strpos(v_keys,':HEADCOMP:000000000001')>0
    and pg_catalog.strpos(v_keys,':HEADCOMP:000000000002')>0
    and pg_catalog.strpos(v_keys,':HEADCOMP:000000000004')>0,
    'B2: every nonzero component is still emitted');

  select sum(w.truth_ex_vat) into v_total from pg_temp.walk() w
  where w.source_relation='weekly_source_entitlement_head_components';
  perform pg_temp.assert_eq(v_total,83.00::numeric,
    'B2: the emitted base authority is 48.00+15.00+20.00+0.00, and no money '
    ||'moved because the suppressed rows were worth nothing');
  perform pg_temp.assert_true(
    (select bool_and(w.resolution_failure is null) from pg_temp.walk() w),
    'B2: nothing the suppression left behind carries a resolution failure - in '
    ||'particular the zero-valued WORKED_TIME occurrence resolves cleanly');

  -- The ordinary adjustments are untouched by any of it.
  perform pg_temp.assert_eq(pg_temp.adjustment_signature(),
    (select v from pg_temp.baseline where k='adjustments'),
    'B2: each ordinary adjustment occurrence is still byte-identical to the '
    ||'no-head case');
  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk() w
     where w.source_relation='timesheets_financials'),0,
    'B2: suppressing head components does not re-open the TSFIN gates');

  -- The evidence transcript this section asserts on, printed so a reader of the
  -- run log sees the money table and not only the word "pass".
  for v_line in
    select '  ordinal '||pg_catalog.lpad(c.component_ordinal::text,2)
        ||'  '||pg_catalog.rpad(c.component_kind,16)
        ||'  declared pay '||pg_catalog.lpad(c.pay_ex_vat::text,7)
        ||'  ->  '||case when w.source_key is null then 'SUPPRESSED, no monetary fact'
             else 'emitted, truth '||w.truth_ex_vat::text end as line
    from public.weekly_source_entitlement_head_components c
    left join pg_temp.walk() w
      on w.source_key='10:'||(select v from pg_temp.world where k='timesheet')::text
         ||':HEADCOMP:'||pg_catalog.lpad(c.component_ordinal::text,12,'0')
    where c.head_id=v_mixed
    order by c.component_ordinal
  loop
    raise notice 'R5-B2 %',v_line.line;
  end loop;

  -- 5A.2  The identity of the suppressed components is NOT lost.  Ruling B2:
  -- "Preserve any required zero identity in the evidence seam/receipt, not as a
  -- new Workbench occurrence."  The evidence seam is the fingerprint; it still
  -- reports the COMPLETE inventory of six, not the four that carry money.
  v_fp:=pg_temp.fingerprint_json();
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_id',v_mixed::text,
    'B2 evidence seam: the fingerprint names the head');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_component_count','6',
    'B2 evidence seam: the fingerprint still publishes all SIX components, '
    ||'including the two suppressed as monetary facts');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_certified_zero','false',
    'B2 evidence seam: this head is not certified zero');
  perform pg_temp.assert_true(
    (v_fp->>'entitlement_inventory_digest') is not null
    and (v_fp->>'entitlement_digest') is not null,
    'B2 evidence seam: the sealed inventory and entitlement digests are published');
  -- and the receipt: the second half of ruling B2's "evidence seam/receipt".
  perform pg_temp.assert_eq(
    (select count(*)::integer
     from private.weekly_source_entitlement_publication_receipts receipt_row
     where v_mixed=any(receipt_row.head_ids)),1,
    'B2 receipt: exactly one immutable publication receipt names this head');

  -- 5A.3  Paging is still exact with suppression in place.  The suppression is
  -- applied INSIDE head_component_rows, before its bounded prefetch, so the
  -- prefetch window always holds emittable rows and the cursor always advances.
  -- Walked one row at a time, two at a time and twenty-five at a time, the
  -- result must be the same set in the same order with no duplicate and no gap.
  perform pg_temp.assert_eq(
    (select string_agg(w.source_key,'|' order by w.source_key) from pg_temp.walk_n(1) w),
    (select string_agg(w.source_key,'|' order by w.source_key) from pg_temp.walk() w),
    'B2 paging: one row per page gives exactly the 25-row-page result');
  perform pg_temp.assert_eq(
    (select string_agg(w.source_key,'|' order by w.source_key) from pg_temp.walk_n(2) w),
    (select string_agg(w.source_key,'|' order by w.source_key) from pg_temp.walk() w),
    'B2 paging: two rows per page gives exactly the 25-row-page result');
  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk_n(1) w),
    (select count(distinct w.source_key)::integer from pg_temp.walk_n(1) w),
    'B2 paging: no occurrence is returned twice when paging one row at a time');
  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk_n(1) w
     where w.source_relation='weekly_source_entitlement_head_components'),4,
    'B2 paging: every emittable head component is reachable one page at a time, '
    ||'so a suppressed component can never strand the ones after it');

  -- 5A.4  A head-supplied rate is still impossible.  The rate authority
  -- document on every emitted head occurrence is still built from the ROOT's
  -- own current TSFIN row - 27 section 6 holds "financial source authority and
  -- certified preview" unchanged - so the head supplied hours, units and
  -- amounts and no rate, exactly as before WP-10c.
  perform pg_temp.assert_true(
    (select bool_and((w.source_payload_json#>>'{rate_authority,rate_authority_version}')='1')
     from pg_temp.walk() w
     where w.source_relation='weekly_source_entitlement_head_components'),
    'B2: every emitted head occurrence still carries the unchanged rate '
    ||'authority document');
  perform pg_temp.assert_true(
    (select bool_and((w.source_payload_json#>>'{rate_authority,source,financial_row_id}')
       =(select v::text from pg_temp.world where k='financial'))
     from pg_temp.walk() w
     where w.source_relation='weekly_source_entitlement_head_components'),
    'B2: the rate card behind every head occurrence is still the ROOT''s own '
    ||'current TSFIN row, so a head-supplied rate remains impossible');
  perform pg_temp.assert_eq(
    (select count(*)::integer from information_schema.columns
     where table_schema='public'
       and table_name='weekly_source_entitlement_head_components'
       and column_name in ('pay_rate','charge_rate','rate_card_id','hourly_rate')),0,
    'B2: the head component relation still carries no rate-card identity at all');
end;
$verify_b2_suppression$;

do $verify_b1_three_outcomes$
declare
  v_zero2 uuid:='17092026-0800-4000-8000-000000000206';
  v_bundle2 uuid:='17092026-0800-4000-8000-000000000306';
  v_allzero uuid:='17092026-0800-4000-8000-000000000207';
  v_bundle3 uuid:='17092026-0800-4000-8000-000000000307';
  v_good uuid:='17092026-0800-4000-8000-000000000208';
  v_bundle4 uuid:='17092026-0800-4000-8000-000000000308';
  v_fp jsonb;
  v_message text;
begin
  -- ---- OUTCOME 1: certified zero.  No monetary fact row, legitimately. ----
  perform pg_temp.seed_head(v_zero2,v_bundle2,'[]'::jsonb);
  perform pg_temp.promote(v_zero2);
  -- Named, never `all` - see the note at the first site in section 3.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;

  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk() w
     where w.source_relation<>'ts_pay_adjustments'),0,
    'B1 outcome 1: certified zero emits no monetary fact row, and does not raise');
  v_fp:=pg_temp.fingerprint_json();
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_certified_zero','true',
    'B1 outcome 1: the evidence seam says certified zero');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_component_count','0',
    'B1 outcome 1: the evidence seam says zero components');
  perform pg_temp.assert_eq(v_fp->>'entitlement_head_id',v_zero2::text,
    'B1 outcome 1: the evidence seam names the head, so the no-row outcome is '
    ||'not an absent-head ambiguity (03 WB-009)');
  perform pg_temp.assert_true(
    (v_fp->>'entitlement_head_revision') is not null
    and (v_fp->>'entitlement_receipt_digest') is not null
    and (v_fp->>'entitlement_source_generation_digest') is not null,
    'B1 outcome 1: revision and all required digests reach the evidence seam');
  perform pg_temp.assert_eq(
    (select count(*)::integer
     from private.weekly_source_entitlement_publication_receipts receipt_row
     where v_zero2=any(receipt_row.head_ids)),1,
    'B1 outcome 1: the zero identity also reaches the immutable receipt');
  perform pg_temp.assert_true(
    v_fp::text is distinct from (select v from pg_temp.baseline where k='fingerprint_no_head'),
    'B1 outcome 1: certified zero is distinguishable from an ordinary root with '
    ||'no head at all');

  -- ---- OUTCOME 3: expected nonzero, no fact row.  INTEGRITY FAILURE. ----
  -- Two components, neither certified zero nor worth anything: after ruling
  -- B2's suppression this head would contribute nothing to the fact stream
  -- while its component_count and inventory digest still assert a live
  -- entitlement.  It must refuse, by a name that cannot read as certified zero.
  perform pg_temp.seed_head(v_allzero,v_bundle3,jsonb_build_array(
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000507',
      'component_kind','ADDITIONAL_UNIT','economic_key_type','ADDITIONAL_CODE',
      'economic_key_value','ZEROADD','component_member_identity','additional:ZEROADD',
      'additional_code_raw','zeroadd','unit_count',0,'unit_pay_rate',0,
      'unit_charge_rate',0,'pay_ex_vat',0.00,'charge_ex_vat',0.00,
      'exclude_from_pay',false),
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000508',
      'component_kind','EXPENSE','economic_key_type','EXPENSE_CODE',
      'economic_key_value','ACCOMMODATION','component_member_identity','expense:ACCOMMODATION',
      'expense_code','accommodation','pay_ex_vat',0.00,'charge_ex_vat',0.00,
      'exclude_from_pay',false)));
  perform pg_temp.promote(v_allzero);
  -- Named, never `all` - see the note at the first site in section 3.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;

  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_EXPECTED_NONZERO_NO_FACT',
    'B1 outcome 3: an expected-nonzero head that would produce no fact row');

  -- And it is NOT the certified-zero outcome: it raises instead of returning,
  -- and its name carries no CERTIFIED_ZERO token for a consumer to match on.
  begin
    perform count(*) from pg_temp.walk();
    raise exception 'ASSERTION_FAILED: B1 outcome 3 returned rows instead of raising';
  exception when sqlstate '23514' then
    get stacked diagnostics v_message=message_text;
  end;
  perform pg_temp.assert_eq(pg_catalog.strpos(v_message,'CERTIFIED_ZERO'),0,
    'B1 outcome 3: the integrity failure cannot be mistaken for certified zero - '
    ||'its code does not contain the CERTIFIED_ZERO token');
  perform pg_temp.assert_true(
    pg_catalog.strpos(v_message,'PAY_WORKBENCH_ENTITLEMENT_HEAD_EXPECTED_NONZERO_NO_FACT')>0,
    'B1 outcome 3: the refusal is raised by its own name');

  -- ---- OUTCOME 2: missing or incomplete source evidence. ----
  -- The evidence outcomes cannot be built here, because they need WP-01a's
  -- immutability guard and its table CHECKs gone and this section deliberately
  -- runs with every guard in place (WEEKLY_SOURCE_IMMUTABLE_FACT refuses the
  -- write, which is itself worth recording: a head is immutable while it is
  -- current).  They are proved in section 6 instead, each by name:
  -- 6a STAGED, 6b AMBIGUOUS, 6c STALE, 6d INCOMPLETE - including the exact pair
  -- to this case, a head claiming certified zero while still holding a
  -- component - 6e/6f MALFORMED and 6g/6h EVIDENCE_INCOMPLETE.  What is proved
  -- HERE is that outcome 3 is none of them and carries its own name.

  -- ---- restore a good current head for the sections that follow ----
  perform pg_temp.seed_head(v_good,v_bundle4,jsonb_build_array(
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000509',
      'component_kind','WORKED_TIME','economic_key_type','TS_DAY',
      'economic_key_value','2026-03-09','component_member_identity','wp10c-good',
      'segment_id','wp10c-good','segment_key','wp10c-good',
      'segment_stable_key','wp10c-good','work_date','2026-03-09',
      'hours_day',6,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
      'pay_ex_vat',48.00,'charge_ex_vat',66.00,'exclude_from_pay',false)));
  perform pg_temp.promote(v_good);
  -- Named, never `all` - see the note at the first site in section 3.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,
    'B1: after the integrity failure a good head composes normally again');
  perform pg_temp.assert_eq(pg_temp.adjustment_signature(),
    (select v from pg_temp.baseline where k='adjustments'),
    'B1: the ordinary adjustments survived all three outcomes unchanged');
end;
$verify_b1_three_outcomes$;

-- ======================================================================= --
-- 6. FAIL CLOSED.  27 section 5.1 fifth bullet and file 26 Gate 4
--    ("multiple, staged or broken heads fail closed"); WB-008's first three
--    limbs (complete components, revision, receipt).
--
--    Most of these states cannot exist while WP-01a's own guards stand, so the
--    exact guards each case would otherwise be stopped by are removed FIRST,
--    in one statement group, before any row in this section is written.  That
--    ordering is required: PostgreSQL refuses ALTER TABLE and CREATE INDEX on a
--    relation that has pending deferred trigger events, and every seed above
--    leaves some.  Removing the guards is the point of the section: it proves
--    the selector refuses on its own account rather than relying on the schema.
--    Sections 0 to 5 ran with every guard in place.  Every change here is
--    inside the rollback, and nothing Banking-Pay-owned or Workbench-owned is
--    touched.  Each case is undone so the next starts from the proved-good
--    state.
-- ======================================================================= --

-- Flush the pending DEFERRABLE INITIALLY DEFERRED asserts from section 5, then
-- put the mode back: the next seed writes its head before its components.
-- Named, never `all` - see the note at the first site in section 3.
set constraints
  public.weekly_source_entitlement_head_inventory_assert,
  public.weekly_source_entitlement_head_component_inventory_assert,
  public.weekly_source_entitlement_head_receipt_assert immediate;
set constraints
  public.weekly_source_entitlement_head_inventory_assert,
  public.weekly_source_entitlement_head_component_inventory_assert,
  public.weekly_source_entitlement_head_receipt_assert deferred;

-- The guards this section has to defeat, all removed before any write.
drop index public.weekly_source_entitlement_heads_committed_current_uq;
drop index public.weekly_source_entitlement_heads_committed_root_uq;
alter table public.weekly_source_entitlement_heads
  disable trigger weekly_source_immutable_fact_guard;
alter table public.weekly_source_entitlement_heads
  disable trigger weekly_source_entitlement_head_root_identity;
alter table public.weekly_source_entitlement_head_components
  disable trigger weekly_source_immutable_record_guard;
-- WP-10c, for 6g.  The three evidence digests are NOT NULL in WP-01a's schema,
-- so the only way to prove the selector refuses a head with incomplete evidence
-- on its OWN account - rather than inheriting the property from a Weekly Source
-- relation another workstream is still shaping - is to remove the NOT NULLs
-- first.  The octet_length CHECKs beside them pass on NULL and are left alone.
alter table public.weekly_source_entitlement_heads
  alter column entitlement_digest drop not null;
alter table public.weekly_source_entitlement_heads
  alter column inventory_digest drop not null;
alter table public.weekly_source_entitlement_heads
  alter column source_generation_digest drop not null;
-- WP-10c, for section 8.  prior_head_id and superseded_by_head_id are both
-- ON DELETE RESTRICT self-foreign-keys pointing in opposite directions along
-- the head chain, so no delete order exists while both stand and the chain
-- cannot be torn down to restore an ordinary Timesheet.  They are dropped HERE,
-- at the one point in the file where the relation has no pending deferred
-- trigger events (the flush immediately above), because PostgreSQL refuses
-- ALTER TABLE on a relation that has any.  Nothing between here and section 8
-- depends on them.
alter table public.weekly_source_entitlement_heads
  drop constraint weekly_source_entitlement_heads_prior_fk;
alter table public.weekly_source_entitlement_heads
  drop constraint weekly_source_entitlement_heads_superseded_by_fk;

do $verify_fail_closed_setup$
declare
  v_dropped integer:=0;
  r record;
begin
  -- Every table CHECK that would stop section 6e from building a committed head
  -- with no receipt digest or no scope-change token.  There is more than one.
  for r in
    select con.conname
    from pg_catalog.pg_constraint con
    where con.conrelid='public.weekly_source_entitlement_heads'::regclass
      and con.contype='c'
      and (pg_catalog.strpos(pg_catalog.pg_get_constraintdef(con.oid),
             'publication_receipt_digest')>0
        or pg_catalog.strpos(pg_catalog.pg_get_constraintdef(con.oid),
             'scope_change_tx_token')>0)
  loop
    execute format('alter table public.weekly_source_entitlement_heads drop constraint %I',
      r.conname);
    v_dropped:=v_dropped+1;
  end loop;
  perform pg_temp.assert_true(v_dropped>0,
    'the receipt and token CHECKs existed to be dropped');
end;
$verify_fail_closed_setup$;

do $verify_fail_closed$
declare
  v_zero uuid:=(select id from public.weekly_source_entitlement_heads
                where state='COMMITTED_CURRENT' limit 1);
  v_ts uuid:=(select v from pg_temp.world where k='timesheet');
  v_booking text:=(select v from pg_temp.worldt where k='booking');
  v_second uuid:='17092026-0800-4000-8000-000000000203';
  v_bundle uuid:='17092026-0800-4000-8000-000000000303';
begin
  -- 6a STAGED.  A staged head with no committed head refuses rather than
  -- falling back to TSFIN.  The staged head is seeded first, because the
  -- outgoing head's superseded_by_head_id foreign key has to point at it.
  perform pg_temp.seed_head(v_second,v_bundle,jsonb_build_array(
    jsonb_build_object('component_id','17092026-0800-4000-8000-000000000405',
      'component_kind','WORKED_TIME','economic_key_type','TS_DAY',
      'economic_key_value','2026-03-09','component_member_identity','wp10-staged',
      'segment_id','wp10-staged','segment_key','wp10-staged',
      'segment_stable_key','wp10-staged','work_date','2026-03-09',
      'hours_day',1,'pay_ex_vat',8.00,'charge_ex_vat',11.00,'exclude_from_pay',false)),
    'STAGED');
  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',superseded_at_utc=clock_timestamp(),
         superseded_by_head_id=v_second
   where id=v_zero;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_STAGED','6a staged head');

  -- Activate it and prove the good path again.
  perform pg_temp.promote(v_second);
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,
    '6a activated: one head component plus the three adjustments');

  -- 6b AMBIGUOUS.  24 section 4.3 allows at most one committed current head per
  -- root across both authority kinds.  Both partial unique indexes have to be
  -- gone for two committed heads to exist at all, which is exactly the state
  -- this refusal is for.
  update public.weekly_source_entitlement_heads head_row
     set state='COMMITTED_CURRENT',superseded_at_utc=null,superseded_by_head_id=null,
         committed_at_utc=clock_timestamp(),
         publication_receipt_digest=receipt_row.request_digest,
         scope_change_tx_token=receipt_row.scope_change_tx_token
    from private.weekly_source_entitlement_publication_receipts receipt_row
   where head_row.id=v_zero
     and receipt_row.decision_bundle_id=head_row.decision_bundle_id
     and receipt_row.bundle_revision=head_row.bundle_revision;
  perform pg_temp.assert_eq(
    (select count(*)::integer from public.weekly_source_entitlement_heads
     where root_timesheet_id=v_ts and state='COMMITTED_CURRENT'),2,
    '6b two committed heads really exist');
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_AMBIGUOUS','6b two committed heads');
  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',superseded_at_utc=clock_timestamp(),
         superseded_by_head_id=v_second
   where id=v_zero;
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,'6b undone');

  -- 6c STALE (proof/34 sections 4 and 6).  The head names one exact physical
  -- root version.  The drift is created on the HEAD, not on public.timesheets,
  -- so no Workbench trigger, invalidator or scope-change token is involved.
  update public.weekly_source_entitlement_heads
     set root_timesheet_version=99 where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_STALE','6c root version drifted under the head');
  update public.weekly_source_entitlement_heads
     set root_timesheet_version=1 where id=v_second;
  update public.weekly_source_entitlement_heads
     set root_family_booking_id=v_booking||'-REPOINTED' where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_STALE','6c booking identity drifted under the head');
  update public.weekly_source_entitlement_heads
     set root_family_booking_id=v_booking where id=v_second;
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,'6c undone');

  -- 6d INCOMPLETE (WB-008 "complete components").  component_count no longer
  -- equals the real inventory.  WP-01a's deferred assert would catch this at
  -- commit; the selector must not wait for a commit that may never come.
  update public.weekly_source_entitlement_heads set component_count=2 where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_INCOMPLETE','6d declared inventory too large');
  update public.weekly_source_entitlement_heads set component_count=0,certified_zero=true
   where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_INCOMPLETE',
    '6d a "certified zero" head that still holds a component');
  update public.weekly_source_entitlement_heads set component_count=1,certified_zero=false
   where id=v_second;
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,'6d undone');

  -- 6e MALFORMED (WB-008 "receipt" and "revision").  Each limb is also a table
  -- CHECK; the receipt CHECK was dropped above, which proves the selector
  -- refuses even if the schema guard were lost.
  update public.weekly_source_entitlement_heads
     set publication_receipt_digest=null where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_MALFORMED','6e committed head with no receipt digest');
  update public.weekly_source_entitlement_heads head_row
     set publication_receipt_digest=receipt_row.request_digest
    from private.weekly_source_entitlement_publication_receipts receipt_row
   where head_row.id=v_second
     and receipt_row.decision_bundle_id=head_row.decision_bundle_id
     and receipt_row.bundle_revision=head_row.bundle_revision;

  update public.weekly_source_entitlement_heads
     set scope_change_tx_token=null where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_MALFORMED',
    '6e committed head with no scope-change token (24 4.5 step 6 unproved)');
  update public.weekly_source_entitlement_heads head_row
     set scope_change_tx_token=receipt_row.scope_change_tx_token
    from private.weekly_source_entitlement_publication_receipts receipt_row
   where head_row.id=v_second
     and receipt_row.decision_bundle_id=head_row.decision_bundle_id
     and receipt_row.bundle_revision=head_row.bundle_revision;
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,'6e undone');

  -- 6f MALFORMED, component limb.  An adjustment must never appear in a head
  -- (WB-007, H2-025), and a code-bearing component with no code cannot be
  -- composed at all.  component_kind is free text, so these need no CHECK gone.
  update public.weekly_source_entitlement_head_components
     set component_kind='ADJUSTMENT' where head_id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_COMPONENT_UNSUPPORTED',
    '6f an adjustment copied into a head');
  update public.weekly_source_entitlement_head_components
     set component_kind='EXPENSE',expense_code=null where head_id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_COMPONENT_UNSUPPORTED',
    '6f an expense component with no expense code');
  update public.weekly_source_entitlement_head_components
     set component_kind='ADDITIONAL_UNIT',additional_code_raw=null where head_id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_COMPONENT_UNSUPPORTED',
    '6f an additional component with no additional code');
  update public.weekly_source_entitlement_head_components
     set component_kind='WORKED_TIME',additional_code_raw=null,expense_code=null
   where head_id=v_second;
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,'6f undone');

  -- 6g EVIDENCE INCOMPLETE (WP-10c, HANDOVER 2 round-5 ruling B1: "The no-row
  -- outcome must remain distinguishable from missing or incomplete source
  -- evidence").  A committed head whose sealed evidence is not complete cannot
  -- publish the identity that makes a no-row outcome safe, so it refuses by its
  -- own name rather than producing an absent-head ambiguity (03 WB-009).  The
  -- three NOT NULLs were removed above, so this proves the SELECTOR's refusal.
  update public.weekly_source_entitlement_heads
     set entitlement_digest=null where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_EVIDENCE_INCOMPLETE',
    '6g committed head with no entitlement digest');
  update public.weekly_source_entitlement_heads
     set entitlement_digest=sha256('ent'::bytea) where id=v_second;

  update public.weekly_source_entitlement_heads
     set inventory_digest=null where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_EVIDENCE_INCOMPLETE',
    '6g committed head with no inventory digest');
  update public.weekly_source_entitlement_heads
     set inventory_digest=sha256('inv'::bytea) where id=v_second;

  update public.weekly_source_entitlement_heads
     set source_generation_digest=null where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_EVIDENCE_INCOMPLETE',
    '6g committed head with no source-generation digest (WB-008 final-source '
    ||'lineage limb)');
  update public.weekly_source_entitlement_heads
     set source_generation_digest=sha256('srcgen'::bytea) where id=v_second;
  perform pg_temp.assert_eq((select count(*)::integer from pg_temp.walk()),4,'6g undone');

  -- 6h A CERTIFIED-ZERO head with incomplete evidence is the case ruling B1
  -- exists for: it would otherwise return no row and no identity, which is
  -- exactly "certified zero" and "nothing was published here" collapsing into
  -- the same observation.  It refuses.
  delete from public.weekly_source_entitlement_head_components where head_id=v_second;
  update public.weekly_source_entitlement_heads
     set component_count=0,certified_zero=true,inventory_digest=null where id=v_second;
  perform pg_temp.expect_failure(
    'select count(*) from pg_temp.walk()',
    'PAY_WORKBENCH_ENTITLEMENT_HEAD_EVIDENCE_INCOMPLETE',
    '6h a certified-zero head with no inventory digest is missing evidence, '
    ||'not a safe no-row outcome');
  update public.weekly_source_entitlement_heads
     set inventory_digest=sha256('inv'::bytea) where id=v_second;
  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk() w
     where w.source_relation<>'ts_pay_adjustments'),0,
    '6h once the evidence is complete the same certified-zero head is the '
    ||'legitimate no-row outcome again');

  -- Every refusal above left the ordinary adjustments untouched once the head
  -- was restored: the selector never half-emits and never drops an adjustment
  -- because a head was broken.
  perform pg_temp.assert_eq(pg_temp.adjustment_signature(),
    (select v from pg_temp.baseline where k='adjustments'),
    '6 the ordinary adjustments survive every fail-closed case unchanged');

  -- Contract decision D8: the per-root authorisation record is NOT an input to
  -- this seam.  Proved statically on the installed definition, so a later
  -- change to that relation can never break the Workbench selector.
  perform pg_temp.assert_eq(
    pg_catalog.strpos(pg_catalog.pg_get_functiondef(
      'private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer)'::regprocedure),
      'weekly_source_root_authorisations'),
    0,'D8: the selector never reads the per-root authorisation relation');
  perform pg_temp.assert_eq(
    pg_catalog.strpos(pg_catalog.pg_get_functiondef(
      'private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer)'::regprocedure),
      'weekly_source_row_timesheet_lineages'),
    0,'D8: the selector never reads the per-source-row lineage relation');
end;
$verify_fail_closed$;

-- ======================================================================= --
-- 7. The other two fact families are unaffected by a head (27 section 6).
-- ======================================================================= --
do $verify_other_families$
declare
  v_build uuid:=(select v from pg_temp.world where k='build');
  v_ts uuid:=(select v from pg_temp.world where k='timesheet');
  v_unit text:=(select v from pg_temp.worldt where k='unit');
  v_frozen integer;
  v_fallback integer;
begin
  select count(*) into v_frozen from private.pay_workbench_unit_economic_occurrence_page_v1(
    v_build,v_unit,'FROZEN_SETTLED_COMPONENT',v_ts,null,25);
  select count(*) into v_fallback from private.pay_workbench_unit_economic_occurrence_page_v1(
    v_build,v_unit,'PAY_STATE_FALLBACK',v_ts,null,25);
  perform pg_temp.assert_eq(v_frozen,0,
    'FROZEN_SETTLED_COMPONENT still answers and is unaffected by the head');
  perform pg_temp.assert_eq(v_fallback,0,
    'PAY_STATE_FALLBACK still answers and is unaffected by the head');

  -- The two refusals the function already had are unchanged.
  perform pg_temp.expect_failure(
    format('select count(*) from private.pay_workbench_unit_economic_occurrence_page_v1(%L,%L,%L,%L,null,25)',
      v_build,v_unit,'NOT_A_FAMILY',v_ts),
    'PAY_WORKBENCH_BUILD_CURSOR_INVALID','pre-existing cursor refusal unchanged');
  perform pg_temp.expect_failure(
    format('select count(*) from private.pay_workbench_unit_economic_occurrence_page_v1(%L,%L,%L,%L,null,25)',
      v_build,v_unit,'LIVE_ENTITLEMENT_INPUT',gen_random_uuid()),
    'PAY_WORKBENCH_UNIT_PROJECTION_INCOMPLETE','pre-existing projection refusal unchanged');
end;
$verify_other_families$;

-- ======================================================================= --
-- 8. THE ORDINARY TIMESHEET, with the whole of WP-10c's new code present
--    (WP-10c).  Every head is removed from the root and the complete
--    occurrence set is compared, field by field, with the one section 2
--    captured before any head existed.  That is the executed proof that none
--    of the EIGHT head refusals - including the two WP-10c adds - is reachable
--    on an ordinary Timesheet, and that ruling B2's suppression predicate
--    cannot touch an ordinary occurrence.
--
--    It runs last, after section 6 has removed guards, so it is also a proof
--    that an ordinary Timesheet is unaffected by every state section 6 built.
-- ======================================================================= --
do $verify_ordinary_timesheet$
declare
  v_ts uuid:=(select v from pg_temp.world where k='timesheet');
  v_heads integer;
  v_fp jsonb;
begin
  -- The head rows are simply removed.  WP-01a's three DEFERRABLE INITIALLY
  -- DEFERRED asserts are NOT forced immediate here, and must not be: the
  -- self-referencing links have to be cleared before the rows can go, which
  -- leaves each head momentarily inconsistent with an inventory it is about to
  -- lose, and the whole teardown is inside the rollback.  Every assertion this
  -- section makes is about the SELECTOR's output on a root with no head.
  delete from public.weekly_source_entitlement_head_components
  where head_id in (select id from public.weekly_source_entitlement_heads
                    where root_timesheet_id=v_ts);
  delete from public.weekly_source_entitlement_heads where root_timesheet_id=v_ts;

  select count(*)::integer into v_heads from public.weekly_source_entitlement_heads
  where root_timesheet_id=v_ts;
  perform pg_temp.assert_eq(v_heads,0,'8: the root carries no head of any state');

  perform pg_temp.assert_eq(pg_temp.full_signature(),
    (select v from pg_temp.baseline where k='no_head_full'),
    '8: an ordinary Timesheet produces EXACTLY the occurrence set it produced '
    ||'before any head existed - every field, every digest, byte for byte');
  perform pg_temp.assert_eq(
    (select coalesce(string_agg(w.source_key,'|' order by w.source_key),'<none>')
     from pg_temp.walk_n(1) w),
    (select v from pg_temp.baseline where k='no_head_pages_1'),
    '8: and it pages identically one row at a time');
  perform pg_temp.assert_eq(
    (select count(*)::integer from pg_temp.walk() w
     where w.source_relation='weekly_source_entitlement_head_components'),0,
    '8: no head occurrence can appear on a root with no head');

  v_fp:=pg_temp.fingerprint_json();
  perform pg_temp.assert_eq(
    (select count(*)::integer from jsonb_object_keys(v_fp) key_name
     where key_name like 'entitlement%'),0,
    '8: the fingerprint carries no entitlement key again');

  -- All eight head refusals exist in the installed definition and not one of
  -- them fired above.  The list is asserted so that a future package removing
  -- one has to face this file.
  perform pg_temp.assert_eq(
    (select count(*)::integer from unnest(array[
       'PAY_WORKBENCH_ENTITLEMENT_HEAD_AMBIGUOUS',
       'PAY_WORKBENCH_ENTITLEMENT_HEAD_STAGED',
       'PAY_WORKBENCH_ENTITLEMENT_HEAD_MALFORMED',
       'PAY_WORKBENCH_ENTITLEMENT_HEAD_EVIDENCE_INCOMPLETE',
       'PAY_WORKBENCH_ENTITLEMENT_HEAD_STALE',
       'PAY_WORKBENCH_ENTITLEMENT_HEAD_INCOMPLETE',
       'PAY_WORKBENCH_ENTITLEMENT_HEAD_COMPONENT_UNSUPPORTED',
       'PAY_WORKBENCH_ENTITLEMENT_HEAD_EXPECTED_NONZERO_NO_FACT']::text[]) code
     where pg_catalog.strpos(pg_catalog.pg_get_functiondef(
       'private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer)'::regprocedure),
       code)>0),8,
    '8: all eight head refusals are present in the installed definition, and '
    ||'none of them fired on the ordinary Timesheet above');
end;
$verify_ordinary_timesheet$;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_workbench_seams_v1',
  'seams',pg_catalog.jsonb_build_array(
    'private.pay_workbench_unit_economic_occurrence_page_v1',
    'private.pay_workbench_timesheet_input_fingerprint_v1'),
  'scenarios',pg_catalog.jsonb_build_array(
    'no-head ordinary composition','one-valid-head replacement','H2-004','H2-025',
    'SPI-081','SPI-082','SPI-083','SPI-084','SPI-085',
    'certified-zero WB-009',
    'R5-B2 zero head addition suppressed','R5-B2 zero head expense suppressed',
    'R5-B2 zero head worked time still emitted (ordinary-path alignment)',
    'R5-B2 zero identity preserved in the evidence seam and the receipt',
    'R5-B2 paging exact at page sizes 1, 2 and 25 after suppression',
    'R5-B1 outcome 1: certified zero, no row, identity published',
    'R5-B1 outcome 2: missing/incomplete evidence refuses by name',
    'R5-B1 outcome 3: expected nonzero with no fact row refuses by name',
    'R5-B1 the integrity failure carries no CERTIFIED_ZERO token',
    'fail-closed EVIDENCE_INCOMPLETE (three digests, and certified zero)',
    'ordinary Timesheet byte-identical with all eight head refusals present',
    'fail-closed STAGED','fail-closed AMBIGUOUS',
    'fail-closed STALE (version and re-pointed booking id)',
    'fail-closed INCOMPLETE (over-declared, and certified zero holding a component)',
    'fail-closed MALFORMED (no receipt digest, no scope-change token)',
    'fail-closed COMPONENT_UNSUPPORTED (adjustment in a head; missing code)',
    'fingerprint nine facts','fingerprint moves on the next head revision',
    'fingerprint never scans the component population',
    'D8: the selector reads neither Weekly Source authorisation relation',
    'other two fact families unaffected','pre-existing refusals unchanged'),
  'banking_pay_definitions_changed',0,
  'workbench_definitions_changed',2
) as result;

rollback;
