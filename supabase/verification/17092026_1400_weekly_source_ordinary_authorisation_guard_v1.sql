-- Rollback-only PostgreSQL 17 proof for package WP-24: the ordinary Authorise
-- and Unauthorise owners may not move a Weekly-Source-managed root's
-- authorisation, and an ordinary non-managed root is completely unaffected.
--
-- Controlling authority, read word for word and not paraphrased here:
--   * `proof/36 section 3`: the withdrawal owner "is the **only** route by which
--     a Weekly-Source-managed root is unauthorised", and "a direct call while
--     the control is unavailable is refused with no write" (UNA-010, contract
--     G3-4);
--   * HANDOVER 2 round-5 Part D, ruling OR-2 CONFIRMED: "Guard every entry
--     point … If the guard attaches to Timesheet, financial or contract-week
--     writes, the withdrawal owner obeys the same canonical lock order as Part
--     A3";
--   * HANDOVER 2 round-5 Part B3 (contract section 1.1 SD-2): the refusal is
--     narrowed to managed, bound or protected roots; an unrelated unbound
--     ordinary family must not acquire a new refusal.
--
-- What this file proves, all by EXECUTION and never by inspecting a definition:
--
--   1. structure and ACL of the guard trigger and its two owners;
--   2. the three-valued JSON boolean reader, all five cases;
--   3. Gate 13 finding F1 itself: the ordinary Unauthorise owner called
--      directly on a managed root with a committed entitlement head is REFUSED,
--      and a complete write fingerprint is unchanged;
--   4. the same for the Bulk Unauthorise owner;
--   5. the withdrawal owner still completes end to end on that root after the
--      ordering change, the head is superseded, and the ordinary re-authorise
--      that follows it succeeds;
--   6. first authorisation of a fresh managed root is not blocked;
--   7. an ordinary NON-managed root authorises, unauthorises and re-authorises
--      exactly as before, and the money the Gate 4 Workbench selector produces
--      for it follows the Office.
--
-- Nothing here defines, wraps or re-creates a Banking Pay, Draft, execution,
-- cancellation, settlement, provider, recovery or remittance owner, and nothing
-- is written outside the rolled-back transaction.

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

create function pg_temp.drain_jobs() returns void language sql as $function$
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

-- A complete "nothing was written" fingerprint of every relation a refused
-- ordinary Authorise or Unauthorise could touch.  A refusing case takes it
-- before and after and compares the WHOLE object, so a write nobody thought to
-- assert on still fails this test.
create function pg_temp.write_fingerprint() returns jsonb
language sql stable as $function$
  select pg_catalog.jsonb_build_object(
    'authorised_timesheets',(select pg_catalog.count(*) from public.timesheets
                               where authorised_at_server is not null),
    'revoked_timesheets',(select pg_catalog.count(*) from public.timesheets
                            where revoked_at is not null),
    'tsfin_authorised',(select pg_catalog.count(*) from public.timesheets_financials
                          where authorised_at_utc is not null),
    'tsfin_status',(select pg_catalog.jsonb_object_agg(processing_status,tally)
                      from (select processing_status::text as processing_status,
                                   pg_catalog.count(*) as tally
                              from public.timesheets_financials
                             group by 1) as grouped),
    'contract_week_status',(select pg_catalog.jsonb_object_agg(status,tally)
                              from (select status::text as status,
                                           pg_catalog.count(*) as tally
                                      from public.contract_weeks
                                     group by 1) as grouped),
    'live_authorisations',(select pg_catalog.count(*)
                             from public.weekly_source_root_authorisations
                            where withdrawn_at_utc is null),
    'withdrawn_authorisations',(select pg_catalog.count(*)
                                  from public.weekly_source_root_authorisations
                                 where withdrawn_at_utc is not null),
    'heads_committed_current',(select pg_catalog.count(*)
                                 from public.weekly_source_entitlement_heads
                                where state='COMMITTED_CURRENT'),
    'heads_superseded',(select pg_catalog.count(*)
                          from public.weekly_source_entitlement_heads
                         where state='SUPERSEDED'),
    'head_components',(select pg_catalog.count(*)
                         from public.weekly_source_entitlement_head_components),
    'workbench_jobs',(select pg_catalog.count(*) from public.banking_pay_workbench_jobs),
    'pay_batch_items',(select pg_catalog.count(*) from public.pay_batch_items),
    'pay_state_history',(select pg_catalog.count(*) from public.timesheet_pay_state_history),
    'ts_pay_adjustments',(select pg_catalog.count(*) from public.ts_pay_adjustments),
    'audit_events',(select pg_catalog.count(*) from public.audit_events));
$function$;

-- ---------------------------------------------------------------------------
-- Fixture world.  Three Weekly HOURS Timesheet families: one that becomes a
-- managed root with a committed entitlement head, one that becomes a managed
-- root and is then withdrawn, and one that is never Weekly Source at all.
-- Every one of them carries the Workbench UNIT_PROJECTION the Gate 4 selector
-- requires, so the money figures below come from the real selector.
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256)
values(1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values('c2400000-0000-4000-8000-000000000001','wp24-office@example.invalid','admin',true,
  'UNUSABLE_ROLLBACK_VERIFIER');
insert into public.clients(id,name) values('c2400000-0000-4000-8000-000000000002','WP24 Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values('c2400000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.settings_finance_windows(date_from,date_to,vat_rate_pct,erni_pct,holiday_pay_pct)
select '2026-01-01','2026-12-31',20,15.05,12.07
where not exists(select 1 from public.settings_finance_windows window_row
  where date '2026-09-13' between window_row.date_from
    and coalesce(window_row.date_to,'infinity'::date));

create function pg_temp.seed_root(
  p_tag text,p_root uuid,p_candidate uuid,p_contract uuid,p_week uuid,
  p_financial uuid,p_session uuid,p_build uuid,p_snapshot uuid,p_booking text
) returns void language plpgsql as $function$
declare
  v_now timestamptz:=pg_catalog.clock_timestamp();
begin
  insert into public.candidates(id,display_name,tms_ref,pay_method)
  values(p_candidate,'WP24 '||p_tag,
    'WP24-'||pg_catalog.replace(p_candidate::text,'-',''),'PAYE');

  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr)
  values(p_contract,p_candidate,'c2400000-0000-4000-8000-000000000002',
    '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);

  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
    line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
    qr_payload_json,is_adjustment,created_at,updated_at)
  values(p_root,p_booking,1,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'wp24-occ-'||p_tag,'wp24-hosp','wp24-ward','wp24-role','weekly-0',
    '2026-09-13',p_contract,'[]'::jsonb,'{}'::jsonb,false,v_now,v_now);

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    timesheet_id,is_adjustment)
  values(p_week,p_contract,'2026-09-13',0,
    'SUBMITTED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,p_root,false);

  -- 9 hours at GBP 10/hour pay and GBP 20/hour charge.
  insert into public.timesheets_financials(
    id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,pay_method,
    processing_status,computed_at_utc,total_hours,total_pay_ex_vat,total_charge_ex_vat,
    hours_day,pay_day,charge_day,invoice_breakdown_json)
  values(p_financial,p_root,1,true,p_candidate,
    'c2400000-0000-4000-8000-000000000002','PAYE',
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,v_now,
    9,90.00,180.00,9,90.00,180.00,
    pg_catalog.jsonb_build_object('mode','SEGMENTS','segments',
      pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'segment_id','wp24-'||p_tag||'-seg-1','segment_key','wp24-'||p_tag||'-seg-1',
          'segment_stable_key','wp24-'||p_tag||'-seg-1','date','2026-09-08',
          'hours_day',9,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
          'pay_amount',90.00,'charge_amount',180.00,'exclude_from_pay',false))));

  insert into public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active)
  values(p_snapshot,'2026-09-18','2026-09-13','2026-09-07','2026-01-01','2026-09-13',
    'OPEN',false);

  insert into public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version)
  values(p_session,'c2400000-0000-4000-8000-000000000001','2026-09-18','2026-09-13',
    'WP24:'||p_session::text,p_snapshot,'OPEN',1);

  insert into private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,
    source_build_run_id,source_job_id,captured_candidate_generation,
    source_change_seq,status,private_stage,seed_scope_count,seed_scope_digest,
    seed_scope_sealed_at_utc,scope_count,dependency_node_count,dependency_edge_count,
    tagged_edge_count,row_seal_count,last_stable_ordinal,scope_cursor_json,
    closure_cursor_json,dependency_edge_stream_complete,dependency_edge_stream_digest,
    edge_tag_stream_complete,edge_tag_digest,unit_digest,scope_digest,
    dependency_digest,sealed_fingerprint_digest,dependency_closure_sealed_at_utc,
    obsolete_at_utc,created_at_utc,updated_at_utc)
  values(p_build,p_candidate,p_session,1,p_snapshot,
    pg_catalog.gen_random_uuid(),null,0,0,'OBSOLETE','WORKSPACE_FACT',
    1,pg_catalog.md5('WP24_SEED'),v_now,1,1,0,0,1,1,'{"terminal":true}'::jsonb,
    '{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb,
    true,pg_catalog.md5(''),true,pg_catalog.md5(''),pg_catalog.md5('WP24_UNIT'),
    pg_catalog.md5('WP24_SCOPE'),pg_catalog.md5('WP24_DEP'),pg_catalog.md5('WP24_FP'),
    v_now,v_now,v_now,v_now);

  insert into private.banking_pay_workbench_economic_build_scope(
    build_id,timesheet_id,candidate_id,root_timesheet_id,stable_ordinal,
    dependency_unit_anchor_timesheet_id,dependency_unit_key,dependency_unit_digest,
    captured_input_fingerprint,closure_status,seal_prepared_at_utc,
    completed_fact_families,fact_row_count,fact_digest,seed_reasons,
    dependency_reasons,captured_dirty_generation,required_fact_families)
  values(p_build,p_root,p_candidate,p_root,1,p_root,
    'UNIT:'||pg_catalog.lower(p_root::text),pg_catalog.md5('WP24_UNIT_DIGEST'),
    pg_catalog.md5('WP24_INPUT_FP'),'SEALED',v_now,
    array['LIVE_ENTITLEMENT_INPUT'],1,pg_catalog.md5('WP24_FACTS'),
    array['WP24_FIXTURE'],array[]::text[],0,array['LIVE_ENTITLEMENT_INPUT']);

  insert into private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
    truth_ex_vat,financial_digest,source_ordinal)
  values(p_build,'LIVE_ENTITLEMENT_INPUT','wp24-unit-projection-'||p_tag,
    p_candidate,p_root,array[p_root],'UNIT:'||pg_catalog.lower(p_root::text),
    'UNIT_PROJECTION',p_root,'TS_TOTAL','TOTAL',0,
    pg_catalog.md5('WP24_UNIT_PROJECTION_'||p_tag),1);

  insert into private.banking_pay_workbench_timesheet_scope_state(
    timesheet_id,candidate_id,dirty_generation,economic_state,last_dirty_reason)
  values(p_root,p_candidate,1,'DIRTY','WP24_FIXTURE')
  on conflict do nothing;
end;
$function$;

create function pg_temp.publish_head_9h90(
  p_head uuid,p_bundle uuid,p_decision uuid,p_receipt uuid,p_token uuid,
  p_candidate uuid,p_contract uuid,p_root uuid,p_booking text,p_tag text
) returns void language sql as $function$
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state,committed_at_utc)
  values(p_bundle,1,'c2400000-0000-4000-8000-0000000000a1',p_candidate,'2026-09-13',
    'SINGLE_ROOT',p_booking,p_root,p_contract,p_decision,
    'c2400000-0000-4000-8000-000000000001','IMMEDIATE',
    pg_catalog.sha256(pg_catalog.convert_to('request-'||p_head::text,'UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('revision','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('choice','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('before','UTF8')),
    array[p_head]::uuid[],'COMMITTED',pg_catalog.transaction_timestamp());

  insert into private.weekly_source_entitlement_publication_receipts(
    id,decision_bundle_id,bundle_revision,request_digest,publication_mode,
    candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,
    head_ids,scope_change_tx_token,decision_id,decided_by_user_id,
    census_json,proof_json)
  values(p_receipt,p_bundle,1,
    pg_catalog.sha256(pg_catalog.convert_to('receipt-'||p_head::text,'UTF8')),
    'IMMEDIATE',p_candidate,array[p_root]::uuid[],array[p_booking]::text[],
    array[1]::integer[],array[p_head]::uuid[],p_token,p_decision,
    'c2400000-0000-4000-8000-000000000001','{}'::jsonb,'{}'::jsonb);

  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    state,certified_zero,component_count,entitlement_digest,inventory_digest,
    source_generation_digest,decision_bundle_id,bundle_revision,decision_id,
    decided_by_user_id,committed_at_utc,publication_receipt_digest,
    scope_change_tx_token)
  values(p_head,'LOCKED_FINAL_SOURCE','c2400000-0000-4000-8000-0000000000a1',
    p_candidate,p_contract,'2026-09-13',p_root,p_booking,1,1,
    'COMMITTED_CURRENT',false,1,
    pg_catalog.sha256(pg_catalog.convert_to('entitlement-'||p_head::text,'UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('inventory-'||p_head::text,'UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('generation-'||p_head::text,'UTF8')),
    p_bundle,1,p_decision,'c2400000-0000-4000-8000-000000000001',
    pg_catalog.transaction_timestamp(),
    pg_catalog.sha256(pg_catalog.convert_to('receipt-'||p_head::text,'UTF8')),
    p_token);

  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,
    economic_key_value,component_member_identity,segment_id,segment_key,
    segment_stable_key,work_date,hours_day,pay_ex_vat,charge_ex_vat,
    exclude_from_pay,origin,decision_bundle_id,bundle_revision,component_sha256)
  values(p_head,1,pg_catalog.gen_random_uuid(),'WORKED_TIME','TS_DAY','2026-09-08',
    'wp24-'||p_tag||'-seg-1','wp24-'||p_tag||'-seg-1','wp24-'||p_tag||'-seg-1',
    'wp24-'||p_tag||'-seg-1','2026-09-08',9,90.00,180.00,false,'SOURCE',p_bundle,1,
    pg_catalog.sha256(pg_catalog.convert_to('component-'||p_head::text,'UTF8')));

  update public.weekly_source_root_authorisations
     set current_entitlement_head_id=p_head
   where root_timesheet_id=p_root and withdrawn_at_utc is null;
$function$;

-- One paged walk of the REAL Gate 4 selector, exactly as a caller walks it.
create function pg_temp.workbench_money(p_build uuid,p_root uuid) returns text
language plpgsql stable as $function$
declare
  v_unit text:='UNIT:'||pg_catalog.lower(p_root::text);
  v_cursor text:=null; v_count integer; v_page integer:=0; r record;
  v_money numeric:=0; v_hours numeric:=0; v_relation text:=null;
begin
  loop
    v_page:=v_page+1; v_count:=0;
    for r in select * from private.pay_workbench_unit_economic_occurrence_page_v1(
      p_build,v_unit,'LIVE_ENTITLEMENT_INPUT',p_root,v_cursor,25)
    loop
      v_count:=v_count+1;
      if r.source_relation in ('weekly_source_entitlement_head_components',
                               'timesheets_financials') then
        v_relation:=r.source_relation;
        v_money:=v_money+coalesce(r.truth_ex_vat,0);
        v_hours:=v_hours
          +coalesce((r.source_payload_json->'segment'->>'hours_day')::numeric,0)
          +coalesce((r.source_payload_json->'segment'->>'hours_night')::numeric,0)
          +coalesce((r.source_payload_json->'segment'->>'hours_sat')::numeric,0)
          +coalesce((r.source_payload_json->'segment'->>'hours_sun')::numeric,0)
          +coalesce((r.source_payload_json->'segment'->>'hours_bh')::numeric,0);
      end if;
      v_cursor:=r.source_key;
    end loop;
    exit when v_count<25 or v_page>20;
  end loop;
  return coalesce(v_relation,'<none>')||' GBP '
    ||pg_catalog.to_char(v_money,'FM999999990.00')||' / '
    ||pg_catalog.to_char(v_hours,'FM999999990.0')||' h';
end;
$function$;

create function pg_temp.office_edit_to_14h140(p_root uuid,p_tag text) returns void
language sql as $function$
  update public.timesheets_financials
     set total_hours=14,total_pay_ex_vat=140.00,total_charge_ex_vat=280.00,
         hours_day=14,pay_day=140.00,charge_day=280.00,
         invoice_breakdown_json=pg_catalog.jsonb_build_object('mode','SEGMENTS','segments',
           pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
             'segment_id','wp24-'||p_tag||'-seg-1','segment_key','wp24-'||p_tag||'-seg-1',
             'segment_stable_key','wp24-'||p_tag||'-seg-1','date','2026-09-08',
             'hours_day',14,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
             'pay_amount',140.00,'charge_amount',280.00,'exclude_from_pay',false)))
   where timesheet_id=p_root and is_current;
$function$;

-- ---------------------------------------------------------------------------
-- 1. Structure and ACL
-- ---------------------------------------------------------------------------
do $verify_structure$
declare
  v_trigger record;
begin
  perform pg_temp.assert_true(
    pg_catalog.to_regprocedure(
      'private.weekly_source_ordinary_authorisation_guard_v1(uuid,text)') is not null,
    'the guard decision owner must exist');
  perform pg_temp.assert_true(
    pg_catalog.to_regprocedure(
      'private.weekly_source_guard_flag_v1(jsonb,text,boolean)') is not null,
    'the three-valued JSON boolean reader must exist');

  select t.tgname,t.tgtype,t.tgenabled,pg_catalog.pg_get_triggerdef(t.oid,true) as definition
    into v_trigger
  from pg_catalog.pg_trigger t
  where t.tgrelid=pg_catalog.to_regclass('public.timesheets')
    and t.tgname='weekly_source_managed_root_authorisation_guard_bu';
  perform pg_temp.assert_true(v_trigger.tgname is not null,
    'the authorisation-boundary guard trigger must be attached to public.timesheets');
  -- BEFORE (2) | ROW (1) | UPDATE (16)
  perform pg_temp.assert_true((v_trigger.tgtype::integer & 2)=2,
    'the guard trigger must be BEFORE, so it refuses before the owner writes');
  perform pg_temp.assert_true((v_trigger.tgtype::integer & 1)=1,
    'the guard trigger must be FOR EACH ROW');
  perform pg_temp.assert_true((v_trigger.tgtype::integer & 16)=16,
    'the guard trigger must fire on UPDATE');
  perform pg_temp.assert_true(v_trigger.tgenabled='O',
    'the guard trigger must be enabled in origin mode');
  perform pg_temp.assert_true(
    v_trigger.definition like '%authorised_at_server%',
    'the guard trigger must be armed on the authorisation boundary column only');

  -- Owner-only, exactly as the Weekly Source ACL contract requires of every
  -- weekly_source_% routine that is not a service RPC.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from pg_catalog.pg_proc p
      where p.oid in (
              pg_catalog.to_regprocedure('private.weekly_source_ordinary_authorisation_guard_v1(uuid,text)'),
              pg_catalog.to_regprocedure('private.weekly_source_ordinary_authorisation_guard_tg_v1()'),
              pg_catalog.to_regprocedure('private.weekly_source_guard_flag_v1(jsonb,text,boolean)'))
        and p.proowner::regrole::text='postgres')=3,
    'all three WP-24 routines must be owned by postgres');
  perform pg_temp.assert_true(
    not pg_catalog.has_function_privilege('anon',
      pg_catalog.to_regprocedure('private.weekly_source_ordinary_authorisation_guard_v1(uuid,text)'),'EXECUTE')
    and not pg_catalog.has_function_privilege('authenticated',
      pg_catalog.to_regprocedure('private.weekly_source_ordinary_authorisation_guard_v1(uuid,text)'),'EXECUTE')
    and not pg_catalog.has_function_privilege('service_role',
      pg_catalog.to_regprocedure('private.weekly_source_ordinary_authorisation_guard_v1(uuid,text)'),'EXECUTE'),
    'the guard decision owner must be owner-only');
  perform pg_temp.assert_true(
    (select prosecdef from pg_catalog.pg_proc
      where oid=pg_catalog.to_regprocedure(
        'private.weekly_source_ordinary_authorisation_guard_tg_v1()')),
    'the trigger body must be SECURITY DEFINER, or it could not read the '
    ||'owner-only rotation guard');
end
$verify_structure$;

-- ---------------------------------------------------------------------------
-- 2. The three-valued JSON boolean reader, EXECUTED on all five cases
--    (Part 1 addendum rule 4)
-- ---------------------------------------------------------------------------
do $verify_flag$
begin
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1('{"a":true}'::jsonb,'a',false) is true,
    'an explicit JSON true reads as true');
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1('{"a":false}'::jsonb,'a',true) is false,
    'an explicit JSON false reads as false even when the unsafe value is true');
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1('{"b":true}'::jsonb,'a',true) is true
    and private.weekly_source_guard_flag_v1('{"b":true}'::jsonb,'a',false) is false,
    'an ABSENT key takes the stated unsafe value, never NULL');
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1('{"a":null}'::jsonb,'a',true) is true
    and private.weekly_source_guard_flag_v1('{"a":null}'::jsonb,'a',false) is false,
    'a JSON null takes the stated unsafe value, never NULL');
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1('{"a":"true"}'::jsonb,'a',false) is false
    and private.weekly_source_guard_flag_v1('{"a":1}'::jsonb,'a',true) is true,
    'a NON-boolean takes the stated unsafe value and never raises 22P02 from '
    ||'inside a guard');
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1(null::jsonb,'a',true) is true,
    'a null decision object takes the stated unsafe value');
end
$verify_flag$;

-- ---------------------------------------------------------------------------
-- 3 and 4.  GATE 13 FINDING F1 ITSELF.
--    The ordinary Unauthorise owner and the Bulk Unauthorise owner, called
--    directly on a managed root carrying a committed entitlement head, are both
--    REFUSED, and a complete write fingerprint is unchanged (UNA-010).
-- ---------------------------------------------------------------------------
do $verify_f1$
declare
  v_root uuid:='c2400000-0000-4000-8000-000000000301';
  v_candidate uuid:='c2400000-0000-4000-8000-000000000101';
  v_contract uuid:='c2400000-0000-4000-8000-000000000201';
  v_build uuid:='c2400000-0000-4000-8000-000000000801';
  v_first jsonb;
  v_before jsonb;
  v_money_before text;
begin
  perform pg_temp.seed_root('managed',v_root,v_candidate,v_contract,
    'c2400000-0000-4000-8000-000000000401','c2400000-0000-4000-8000-000000000501',
    'c2400000-0000-4000-8000-000000000701',v_build,
    'c2400000-0000-4000-8000-000000000901','WP24-MANAGED-01');
  perform pg_temp.drain_jobs();

  -- Requirement 6: first authorisation of a fresh root is NOT blocked by the
  -- new guard.  It runs the ordinary Authorise owner inside itself, and at that
  -- moment there is no live generation, so the state test permits it.
  v_first:=public.weekly_source_first_authorise_v1(
    v_root,v_root,null,'c2400000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_first->>'ok')::boolean,false),
    'first authorisation must still succeed through the guard, got '||v_first::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id=v_root and withdrawn_at_utc is null)=1,
    'first authorisation must leave exactly one live generation');

  perform pg_temp.publish_head_9h90(
    'c2400000-0000-4000-8000-000000000a01','c2400000-0000-4000-8000-000000000b01',
    'c2400000-0000-4000-8000-000000000c01','c2400000-0000-4000-8000-000000000d01',
    'c2400000-0000-4000-8000-000000000e01',v_candidate,v_contract,v_root,
    'WP24-MANAGED-01','managed');
  perform pg_temp.drain_jobs();

  v_money_before:=pg_temp.workbench_money(v_build,v_root);
  perform pg_temp.assert_true(
    v_money_before='weekly_source_entitlement_head_components GBP 90.00 / 9.0 h',
    'the published head must be what the Workbench is fed, got '||v_money_before);

  -- The guard decision itself, executed rather than inspected.
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1(
      private.weekly_source_ordinary_authorisation_guard_v1(v_root,'WP24-MANAGED-01'),
      'refuse',false),
    'the state test must refuse for an authorised managed root');

  v_before:=pg_temp.write_fingerprint();

  -- UNA-010, the real negative test: the ordinary owner, called directly.
  perform pg_temp.assert_refused(
    pg_catalog.format(
      'select public.timesheet_unauthorise_atomic(%L::uuid,%L::uuid,%L::uuid)',
      v_root,v_root,'c2400000-0000-4000-8000-000000000001'),
    '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%',
    'F1: the ordinary Unauthorise owner on a managed root');
  perform pg_temp.assert_true(pg_temp.write_fingerprint()=v_before,
    'UNA-010: the refused ordinary Unauthorise must write NOTHING');

  -- The Bulk sibling, the second owner the Office route calls.
  perform pg_temp.assert_refused(
    pg_catalog.format(
      'select public.timesheet_unauthorise_bulk_atomic(%L::jsonb,%L::uuid)',
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'timesheet_id',v_root,'expected_timesheet_id',v_root))::text,
      'c2400000-0000-4000-8000-000000000001'),
    '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%',
    'F1: the Bulk Unauthorise owner on a managed root');
  perform pg_temp.assert_true(pg_temp.write_fingerprint()=v_before,
    'UNA-010: the refused Bulk Unauthorise must write NOTHING');

  -- And the state the reviewer measured is now unreachable: the root is still
  -- authorised, the head is still the one that was published, and the money the
  -- Workbench is fed has not moved.
  perform pg_temp.assert_true(
    (select authorised_at_server is not null from public.timesheets
      where timesheet_id=v_root),
    'the managed root must still be authorised after both refusals');
  perform pg_temp.assert_true(
    pg_temp.workbench_money(v_build,v_root)=v_money_before,
    'the Workbench figure must be unchanged after both refusals');
end
$verify_f1$;

-- ---------------------------------------------------------------------------
-- 5.  The withdrawal owner still completes end to end after the ordering
--     change, and the ordinary re-authorise that follows it succeeds.
-- ---------------------------------------------------------------------------
do $verify_withdrawal$
declare
  v_root uuid:='c2400000-0000-4000-8000-000000000303';
  v_candidate uuid:='c2400000-0000-4000-8000-000000000103';
  v_contract uuid:='c2400000-0000-4000-8000-000000000203';
  v_build uuid:='c2400000-0000-4000-8000-000000000803';
  v_first jsonb;
  v_withdraw jsonb;
  v_money text;
begin
  perform pg_temp.seed_root('withdrawn',v_root,v_candidate,v_contract,
    'c2400000-0000-4000-8000-000000000403','c2400000-0000-4000-8000-000000000503',
    'c2400000-0000-4000-8000-000000000703',v_build,
    'c2400000-0000-4000-8000-000000000903','WP24-MANAGED-03');
  perform pg_temp.drain_jobs();

  v_first:=public.weekly_source_first_authorise_v1(
    v_root,v_root,null,'c2400000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_first->>'ok')::boolean,false),
    'fixture: first authorisation, got '||v_first::text);
  perform pg_temp.publish_head_9h90(
    'c2400000-0000-4000-8000-000000000a03','c2400000-0000-4000-8000-000000000b03',
    'c2400000-0000-4000-8000-000000000c03','c2400000-0000-4000-8000-000000000d03',
    'c2400000-0000-4000-8000-000000000e03',v_candidate,v_contract,v_root,
    'WP24-MANAGED-03','withdrawn');
  perform pg_temp.drain_jobs();

  -- THE ACCEPTED ROUTE.  This is the half of WP-24 that would break first if
  -- the ordering change in 17092026_0600 were wrong or absent: the withdrawal
  -- owner calls the same ordinary Unauthorise owner the guard refuses above.
  v_withdraw:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,pg_temp.current_signature(v_root),
    'c2400000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_withdraw->>'ok')::boolean,false),
    'the withdrawal owner must still complete through the guard, got '
    ||v_withdraw::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id=v_root and withdrawn_at_utc is null)=0,
    'the withdrawal must leave no live generation');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where root_timesheet_id=v_root and state='SUPERSEDED'
        and superseded_by_withdrawal_id is not null)=1,
    'ruling A3 step 3: the committed head must be superseded with its '
    ||'withdrawal link');
  perform pg_temp.assert_true(
    (select authorised_at_server is null from public.timesheets
      where timesheet_id=v_root),
    'the withdrawal must leave the root unauthorised');
  perform pg_temp.drain_jobs();

  -- SD-1: "Reauthorisation creates a new generation/head and never revives the
  -- superseded head."  The ordinary Authorise owner is permitted here, because
  -- the generation is withdrawn: the state test, not the caller, decides.
  perform pg_temp.office_edit_to_14h140(v_root,'withdrawn');
  perform pg_temp.assert_true(
    coalesce((public.timesheet_authorise_generic_atomic(
      v_root,v_root,'c2400000-0000-4000-8000-000000000001')->>'ok')::boolean,false),
    'an ordinary re-authorise after a proper withdrawal must be permitted');
  perform pg_temp.drain_jobs();

  v_money:=pg_temp.workbench_money(v_build,v_root);
  perform pg_temp.assert_true(
    v_money='timesheets_financials GBP 140.00 / 14.0 h',
    'after a proper withdrawal the superseded head must NOT be revived and the '
    ||'Workbench must follow the Office, got '||v_money);
end
$verify_withdrawal$;

-- ---------------------------------------------------------------------------
-- 7.  THE REGRESSION CONTROL.  An ordinary NON-managed root - no Weekly Source
--     authorisation, no entitlement head, no binding of any kind - authorises,
--     unauthorises and re-authorises exactly as it did before this feature
--     existed, and its money follows the Office (ruling B3).
-- ---------------------------------------------------------------------------
do $verify_ordinary$
declare
  v_root uuid:='c2400000-0000-4000-8000-000000000302';
  v_candidate uuid:='c2400000-0000-4000-8000-000000000102';
  v_contract uuid:='c2400000-0000-4000-8000-000000000202';
  v_build uuid:='c2400000-0000-4000-8000-000000000802';
  v_money text;
begin
  perform pg_temp.seed_root('ordinary',v_root,v_candidate,v_contract,
    'c2400000-0000-4000-8000-000000000402','c2400000-0000-4000-8000-000000000502',
    'c2400000-0000-4000-8000-000000000702',v_build,
    'c2400000-0000-4000-8000-000000000902','WP24-ORDINARY-01');
  perform pg_temp.drain_jobs();

  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1(
      private.weekly_source_ordinary_authorisation_guard_v1(v_root,'WP24-ORDINARY-01'),
      'refuse',true) is false,
    'ruling B3: an unbound ordinary family must NOT acquire a refusal');

  perform pg_temp.assert_true(
    coalesce((public.timesheet_authorise_generic_atomic(
      v_root,v_root,'c2400000-0000-4000-8000-000000000001')->>'ok')::boolean,false),
    'an ordinary root must still authorise');
  perform pg_temp.drain_jobs();
  v_money:=pg_temp.workbench_money(v_build,v_root);
  perform pg_temp.assert_true(v_money='timesheets_financials GBP 90.00 / 9.0 h',
    'an ordinary root is paid from its own TSFIN, got '||v_money);

  perform pg_temp.assert_true(
    coalesce((public.timesheet_unauthorise_atomic(
      v_root,v_root,'c2400000-0000-4000-8000-000000000001')->>'ok')::boolean,false),
    'an ordinary root must still UNAUTHORISE: this is the regression that would '
    ||'be worse than the defect');
  perform pg_temp.drain_jobs();
  perform pg_temp.assert_true(
    (select authorised_at_server is null from public.timesheets
      where timesheet_id=v_root),
    'the ordinary unauthorise must actually have happened');

  perform pg_temp.office_edit_to_14h140(v_root,'ordinary');
  perform pg_temp.assert_true(
    coalesce((public.timesheet_authorise_generic_atomic(
      v_root,v_root,'c2400000-0000-4000-8000-000000000001')->>'ok')::boolean,false),
    'an ordinary root must still RE-AUTHORISE');
  perform pg_temp.drain_jobs();

  v_money:=pg_temp.workbench_money(v_build,v_root);
  perform pg_temp.assert_true(v_money='timesheets_financials GBP 140.00 / 14.0 h',
    'an ordinary root''s money must follow the Office exactly as before, got '
    ||v_money);

  -- And the Bulk owner is unchanged for it too.
  perform pg_temp.assert_true(
    coalesce((public.timesheet_unauthorise_bulk_atomic(
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'timesheet_id',v_root,'expected_timesheet_id',v_root)),
      'c2400000-0000-4000-8000-000000000001')->>'all_success')::boolean,false),
    'the Bulk Unauthorise owner must still succeed for an ordinary root');
end
$verify_ordinary$;

-- ---------------------------------------------------------------------------
-- 8.  THE OWNER, NOT THE ROUTE.  `runCorrectionPairLifecycleIfApplicable`
--     (`broker/src/index.js:95093`) is reached from BOTH the /unauthorise route
--     (`:77771`) and the Authorise route (`:169086`), and when it sees a
--     correction pair it posts straight to `public.timesheet_authorise_bulk_atomic`
--     / `public.timesheet_unauthorise_bulk_atomic` at `:95120` with no Weekly
--     Source check of any kind.  A broker-level branch would not cover it.
--     Every call below is made by SQL, directly to the owner, with no route in
--     front of it, so what is proved is a property of the OWNER.
--
--     The state used here is finding F1's own end state - a live authorisation
--     generation on a Timesheet that is not authorised - reached without any
--     guarded write, by binding a never-authorised root.  It is the state an
--     ordinary re-authorise would use to revive a stale head.
-- ---------------------------------------------------------------------------
do $verify_owner_not_route$
declare
  v_root uuid:='c2400000-0000-4000-8000-000000000304';
  v_before jsonb;
  v_decision jsonb;
begin
  perform pg_temp.seed_root('contradiction',v_root,
    'c2400000-0000-4000-8000-000000000104','c2400000-0000-4000-8000-000000000204',
    'c2400000-0000-4000-8000-000000000404','c2400000-0000-4000-8000-000000000504',
    'c2400000-0000-4000-8000-000000000704','c2400000-0000-4000-8000-000000000804',
    'c2400000-0000-4000-8000-000000000904','WP24-CONTRADICTION-01');
  perform pg_temp.drain_jobs();

  -- A live generation on a root that was never authorised.  No guarded write is
  -- used to reach it.
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id,agency_id)
  values(v_root,'WP24-CONTRADICTION-01',1,1,'wp24-contradiction-signature',
    'c2400000-0000-4000-8000-000000000001','c2400000-0000-4000-8000-0000000000a1');

  v_decision:=private.weekly_source_ordinary_authorisation_guard_v1(
    v_root,'WP24-CONTRADICTION-01');
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1(v_decision,'refuse',false),
    'the contradiction state must refuse');
  perform pg_temp.assert_true(
    v_decision->>'refusal_basis'='AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET',
    'the refusal must be named for what it is, got '||coalesce(v_decision::text,'<null>'));
  -- D8: this state is NOT managed, so a guard keyed on managedness alone would
  -- have permitted the revival.  The contradiction limb is what stops it.
  perform pg_temp.assert_true(
    private.weekly_source_guard_flag_v1(v_decision,'managed',true) is false,
    'D8: a live record on an unauthorised Timesheet is managed = false');

  v_before:=pg_temp.write_fingerprint();

  perform pg_temp.assert_refused(
    pg_catalog.format(
      'select public.timesheet_authorise_generic_atomic(%L::uuid,%L::uuid,%L::uuid)',
      v_root,v_root,'c2400000-0000-4000-8000-000000000001'),
    '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%',
    'the ordinary Authorise owner on the contradiction state');
  perform pg_temp.assert_true(pg_temp.write_fingerprint()=v_before,
    'the refused ordinary Authorise must write NOTHING');

  -- The owner the correction-pair bypass at broker/src/index.js:95120 posts to.
  perform pg_temp.assert_refused(
    pg_catalog.format(
      'select public.timesheet_authorise_bulk_atomic(%L::jsonb,%L::uuid)',
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'timesheet_id',v_root,'expected_timesheet_id',v_root))::text,
      'c2400000-0000-4000-8000-000000000001'),
    '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%',
    'the Bulk Authorise owner on the contradiction state');
  perform pg_temp.assert_true(pg_temp.write_fingerprint()=v_before,
    'the refused Bulk Authorise must write NOTHING');

  perform pg_temp.assert_true(
    (select authorised_at_server is null from public.timesheets
      where timesheet_id=v_root),
    'the contradiction root must still be unauthorised after both refusals');
end
$verify_owner_not_route$;

select pg_catalog.jsonb_build_object(
  'package','WP-24',
  'finding','GATE13_F1',
  'bulk_authorise_owner_direct_call','REFUSED_WITH_ZERO_WRITES',
  'guard_trigger','weekly_source_managed_root_authorisation_guard_bu',
  'managed_root_ordinary_unauthorise','REFUSED_WITH_ZERO_WRITES',
  'managed_root_bulk_unauthorise','REFUSED_WITH_ZERO_WRITES',
  'withdrawal_owner','COMPLETES_AND_SUPERSEDES',
  'ordinary_root','UNCHANGED') as result;

rollback;
