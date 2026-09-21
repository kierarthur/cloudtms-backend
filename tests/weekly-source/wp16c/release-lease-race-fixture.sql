-- Weekly Source Plan 6.2 — Gate 12 (WP-16c). COMMITTED fixture for the release-
-- lease two-session case, adopted from WP-08b.
--
-- PROVENANCE.  The body of this file is WP-08b`s own `race_setup.sql`, which it
-- ran from its session scratchpad as executed evidence for review finding F1 and
-- then handed over: "the two-session race is executed evidence re-run here, not
-- a standing test. A standing two-session case belongs with the suites that own
-- the multi-connection adapter" (`WP-08b_REPORT.md` section C2). This gives it a
-- committed home, in the same style as WP-03`s `wp03-rotation-authority-races.mjs`.
--
-- WHAT THE CASE PROVES.  Two sessions hold the SAME release lease. The first
-- releases; the second must be refused BY THE CODE, UNDER THE LOCK, BEFORE ANY
-- WRITE — not merely stopped afterwards by a schema constraint. Before WP-08b`s
-- fix the reviewer measured session 2 reaching the BUSY branch and being stopped
-- by the relation`s check constraint with a raised 23514. The runtime half is
-- `release-suite.mjs`; the structural half (the ordering inside the installed
-- definition, and exactly two row locks) is WP-08b`s own verifier.
--
-- Requires WP-16a`s fixture library to be installed first:
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f <fixtures-banking>/install.sql
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f <fixtures-banking>/build-all.sql
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f release-lease-race-fixture.sql
--
-- Nothing here defines, wraps or re-creates a Banking Pay owner.

\set ON_ERROR_STOP on
begin;
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $f$
begin
  if p_condition is distinct from true then raise exception 'ASSERTION_FAILED: %',p_message; end if;
end;$f$;


-- TEST SCAFFOLDING: retire the Workbench jobs the fixture build queued so the
-- installed Candidate serial gate can GRANT.  The release owner never touches a
-- Workbench job.
update public.banking_pay_workbench_jobs
   set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
 where status in ('QUEUED','RUNNING');

-- Weekly Source world over the fixture roots.
do $world$
declare
  j jsonb:=ws_banking_fixture.base_world_identities_v1();
  v_actor uuid:=(j->>'actor_user_id')::uuid;
  v_client uuid:=(j->>'client_id')::uuid;
begin
  insert into public.client_settings(client_id,vat_rate_pct,effective_from)
  values (v_client,20,'2026-01-01') on conflict do nothing;

  insert into public.weekly_source_format_profiles(
    id,profile_code,version,final_authority_kind,container_kind,omission_meaning,
    row_finalisation_capability,worked_duration_authority,profile_json,profile_sha256)
  values ('ee000000-0000-4000-8000-0000000000f1','WP08B_E2E',1,
    'GENERIC_COMPLETE_SNAPSHOT','XLSX','CANCEL_INSIDE_CONFIRMED_COVERAGE','NONE',
    'SOURCE_ACTUAL','{}'::jsonb,pg_catalog.decode(pg_catalog.repeat('a1',32),'hex'));
  insert into public.weekly_source_groups(
    id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time)
  values ('ee000000-0000-4000-8000-0000000000f2','TEST','ee000000-0000-4000-8000-0000000000aa',
    'WP08B_E2E','WP08B e2e','ROSTER',3,'15:00');
  insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc)
  values ('ee000000-0000-4000-8000-0000000000f3','ee000000-0000-4000-8000-0000000000f2',
          '2026-03-15',pg_catalog.clock_timestamp());
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
    coverage_proof_kind,physical_row_count,uploaded_by_user_id)
  values ('ee000000-0000-4000-8000-0000000000f4','ee000000-0000-4000-8000-0000000000f3',
    'e2e.xlsx',pg_catalog.decode(pg_catalog.repeat('a2',32),'hex'),1024,
    'ee000000-0000-4000-8000-0000000000f1','p1','n1',
    pg_catalog.decode(pg_catalog.repeat('a3',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('a4',32),'hex'),
    'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,v_actor);
  insert into public.weekly_source_final_revisions(
    id,source_cycle_id,authority_scope_kind,revision_number,upload_id,
    coverage_start_local_date,coverage_end_local_date,coverage_timezone,reason,
    finalised_by_user_id,manifest_hash,policy_fingerprint,state)
  values ('ee000000-0000-4000-8000-0000000000fa','ee000000-0000-4000-8000-0000000000f3','CYCLE',1,
    'ee000000-0000-4000-8000-0000000000f4','2026-03-09','2026-03-15','Europe/London',
    'INITIAL_FINALISATION',v_actor,
    pg_catalog.decode(pg_catalog.repeat('c1',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('c2',32),'hex'),'CURRENT');
end$world$;

create function pg_temp.root_contract(p_tag text) returns uuid
language sql immutable as $f$
  select pg_catalog.md5('wp08b:contract:'||p_tag)::uuid;
$f$;

-- One Contract, contract week, upload row, work event, row resolution, lineage
-- binding and ROOT AUTHORISATION (decision D8) per root.
--
-- TEST SCAFFOLDING, stated in the open: the WP-16a fixture Contract is shared
-- by seven Candidates and carries no candidate_id, while interface I-1 requires
-- every requested root to belong to the pinned Candidate through
-- contracts.candidate_id.  Each root used here therefore gets its own Contract
-- for the same Client and the same dates, and only that root is re-pointed.  No
-- Banking Pay evidence row is created, changed or deleted.
create function pg_temp.mk_root(
  p_tag text,p_root uuid,p_booking text,p_candidate uuid,p_ordinal integer
) returns void language plpgsql as $f$
declare
  j jsonb:=ws_banking_fixture.base_world_identities_v1();
  v_client uuid:=(j->>'client_id')::uuid;
  v_contract uuid:=pg_temp.root_contract(p_tag);
  v_week uuid:=pg_catalog.md5('wp08b:week:'||p_tag)::uuid;
  v_row uuid:=pg_catalog.md5('wp08b:row:'||p_tag)::uuid;
  v_event uuid:=pg_catalog.md5('wp08b:event:'||p_tag)::uuid;
  v_res uuid:=pg_catalog.md5('wp08b:res:'||p_tag)::uuid;
begin
  insert into public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot)
  values (v_contract,p_candidate,v_client,date '2026-01-01',date '2026-12-31','PAYE');
  update public.timesheets set contract_id=v_contract where timesheet_id=p_root;
  insert into public.contract_weeks(id,contract_id,week_ending_date)
  values (v_week,v_contract,'2026-03-15');

  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,source_candidate_identity,source_client_identity,
    work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,normalised_row_hash)
  values (v_row,'ee000000-0000-4000-8000-0000000000f4',p_ordinal,'cand-'||p_tag,'client-1',
    '2026-03-10','2026-03-10 08:00','2026-03-10 16:00',30,450,
    pg_catalog.sha256(pg_catalog.convert_to('row:'||p_tag,'UTF8')));
  insert into public.weekly_work_events(
    id,candidate_id,client_id,work_date,identity_kind,durable_identity_hash,
    source_format_profile_id,profile_external_key)
  values (v_event,p_candidate,v_client,'2026-03-10','PROFILE_EXTERNAL_KEY',
    pg_catalog.sha256(pg_catalog.convert_to('event:'||p_tag,'UTF8')),
    'ee000000-0000-4000-8000-0000000000f1','wp08b-'||p_tag);
  insert into public.weekly_source_row_resolutions(
    id,upload_row_id,generation,mapping_state,qualification_profile_fingerprint,
    qualifying_contract_set_hash,source_row_fingerprint,work_event_id,candidate_id,client_id,
    contract_id,contract_selection_method,work_event_match_kind,work_event_match_fingerprint)
  values (v_res,v_row,1,'RESOLVED',
    pg_catalog.sha256(pg_catalog.convert_to('qpf:'||p_tag,'UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('qcs:'||p_tag,'UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('srf:'||p_tag,'UTF8')),
    v_event,p_candidate,v_client,v_contract,'AUTO_UNIQUE','NEW_PROFILE_KEY',
    pg_catalog.sha256(pg_catalog.convert_to('wem:'||p_tag,'UTF8')));
  insert into public.weekly_source_row_timesheet_lineages(
    row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,contract_id,
    contract_week_id,timesheet_id,family_booking_id,timesheet_version,
    week_ending_date,lineage_fingerprint)
  values (v_res,'ee000000-0000-4000-8000-0000000000f3',v_event,p_candidate,v_client,v_contract,
    v_week,p_root,p_booking,1,'2026-03-15',
    pg_catalog.sha256(pg_catalog.convert_to('lin:'||p_tag,'UTF8')));
  -- Decision D8: the authorisation record and the current-head pointer are per
  -- ROOT, keyed on the physical root_timesheet_id.
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id)
  values (p_root,p_booking,1,1,'sig-'||p_tag,(j->>'actor_user_id')::uuid);
  -- TEST SCAFFOLDING: re-pointing the root fired the installed Workbench dirty
  -- trigger, so the Candidate serial gate would report BLOCKED.  The verifier
  -- proves that refusal deliberately; here the queued job is retired so the
  -- release path itself can be exercised.  The release owner never touches a
  -- Workbench job.
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
end;$f$;

create function pg_temp.component(p_ordinal integer,p_id uuid,p_hours text,p_pay text)
returns jsonb language sql immutable as $f$
  select pg_catalog.jsonb_build_object(
    'component_ordinal',p_ordinal,'component_id',p_id,'component_kind','WORKED_TIME',
    'economic_key_type','SEGMENT','economic_key_value','seg-'||p_ordinal::text,
    'component_member_identity','mem-'||p_ordinal::text,
    'segment_id',null,'segment_key',null,'segment_stable_key',null,
    'work_date','2026-03-10','reference_number',null,
    'hours_day',p_hours,'hours_night',null,'hours_sat',null,'hours_sun',null,'hours_bh',null,
    'additional_code_raw',null,'unit_count',null,'unit_pay_rate',null,'unit_charge_rate',null,
    'expense_code',null,'pay_ex_vat',p_pay,'charge_ex_vat',null,
    'exclude_from_pay',false,'origin','WEEKLY_SOURCE','movement_id',null,'movement_group_id',null);
$f$;

create function pg_temp.request(
  p_bundle uuid,p_head uuid,p_decision uuid,p_root uuid,p_booking text,p_candidate uuid,p_tag text
) returns jsonb language sql stable as $f$
  select pg_catalog.jsonb_build_object(
    'decision_bundle_id',p_bundle,'pending_bundle_id',null,'bundle_revision',1,
    'candidate_id',p_candidate,
    'member_root_ids',pg_catalog.jsonb_build_array(p_root),
    'member_family_booking_ids',pg_catalog.jsonb_build_array(p_booking),
    'member_root_versions',pg_catalog.jsonb_build_array(1),
    'head_ids',pg_catalog.jsonb_build_array(p_head),
    'decision_id',p_decision,'publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id','ee000000-0000-4000-8000-0000000000fa',
        'source_cycle_id','ee000000-0000-4000-8000-0000000000f3','revision_number',1,
        'manifest_hash',pg_catalog.repeat('c1',32),'policy_fingerprint',pg_catalog.repeat('c2',32)),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id',pg_temp.root_contract(p_tag),
          'week_ending_date','2026-03-15','selection_method','UNCHANGED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',false,'component_count',1,
          'components',pg_catalog.jsonb_build_array(
            pg_temp.component(1,pg_catalog.md5('wp08b:comp:'||p_head::text)::uuid,'7.5','75.00'))))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','SINGLE_ROOT','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(null),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,'component_ids','[]'::jsonb,
          'inventory_digest',pg_catalog.repeat('00',32))),
      'moved_component_ids',pg_catalog.jsonb_build_array(),
      'target_root_authorisation',null,'whole_root_office_review',null));
$f$;

create function pg_temp.mk_bundle(
  p_bundle uuid,p_head uuid,p_decision uuid,p_root uuid,p_booking text,p_candidate uuid,p_tag text,
  p_request jsonb
) returns void language plpgsql as $f$
declare
  j jsonb:=ws_banking_fixture.base_world_identities_v1();
  v_canonical jsonb:=private.weekly_source_publication_request_canonical_v1(
    p_request,'IMMEDIATE',null::uuid);
begin
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
    contract_choice_digest,before_inventory_digest,proposed_head_ids,state)
  values (p_bundle,1,'ee000000-0000-4000-8000-0000000000aa',p_candidate,'2026-03-15','SINGLE_ROOT',
    p_booking,p_root,pg_temp.root_contract(p_tag),
    p_decision,(j->>'actor_user_id')::uuid,'DEFERRED',
    private.weekly_source_publication_request_digest_v1(v_canonical),
    private.weekly_source_publication_request_digest_v1(
      v_canonical->'financial_request'->'source_revision'),
    private.weekly_source_publication_request_digest_v1(
      v_canonical->'financial_request'->'contract_choices'),
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_before_inventory_v1(
        coalesce(p_request->'control','{}'::jsonb),1)),
    array[p_head]::uuid[],'PROPOSED');
end;$f$;

create function pg_temp.claim_and_apply(p_pending uuid) returns jsonb
language plpgsql as $f$
declare v_row record;
begin
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second' where id=p_pending;
  perform private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source:wp08b-e2e','ee000000-0000-4000-8000-00000000cc01',120,25);
  select * into v_row from public.weekly_source_pending_entitlement_bundles where id=p_pending;
  if v_row.state<>'RELEASING' then
    return pg_catalog.jsonb_build_object('claimed',false,'state',v_row.state);
  end if;
  return private.weekly_source_pending_entitlement_release_apply_v1(
    p_pending,v_row.pending_revision,v_row.request_digest,
    v_row.lease_owner,v_row.lease_token,v_row.lease_worker_run_id);
end;$f$;

create function pg_temp.release_once(
  p_tag text,p_bundle uuid,p_head uuid,p_decision uuid,
  p_root uuid,p_booking text,p_candidate uuid,p_expect_census text
) returns jsonb language plpgsql as $f$
declare
  v_request jsonb;
  v_saved jsonb;
  v_pending uuid;
  v_apply jsonb;
  v_census jsonb;
  v_members uuid[];
begin
  v_request:=pg_temp.request(p_bundle,p_head,p_decision,p_root,p_booking,p_candidate,p_tag);
  perform pg_temp.mk_bundle(p_bundle,p_head,p_decision,p_root,p_booking,p_candidate,p_tag,v_request);

  -- The census at SAVE time is hand-built FROZEN: the Office decision arrived
  -- while the root was in flight.  The census at RELEASE time is the REAL
  -- installed one over the real WP-16a evidence.
  v_saved:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request,
    pg_catalog.jsonb_build_object('ok',true,'gate','GRANTED','families',
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'requested_timesheet_id',p_root,'family_booking_id',p_booking,
        'canonical_timesheet_id',p_root,'canonical_version',1,
        'requested_is_canonical',true,'family_is_current',true,
        'member_timesheet_ids',pg_catalog.jsonb_build_array(p_root)))),
    pg_catalog.jsonb_build_object('result','FROZEN','items','[]'::jsonb));
  perform pg_temp.assert_true((v_saved->>'ok')::boolean,p_tag||': save: '||v_saved::text);
  v_pending:=(v_saved->>'pending_bundle_id')::uuid;

  select coalesce(pg_catalog.array_agg(distinct s.family_timesheet_id),array[]::uuid[])
    into v_members from public._pay_timesheet_rotation_scope(array[p_root]) s;
  v_census:=private.weekly_source_freeze_census_v1(p_candidate,v_members);
  perform pg_temp.assert_true(v_census->>'result'=p_expect_census,
    p_tag||': expected census '||p_expect_census||' but got '||(v_census->>'result')
      ||' reason '||coalesce(v_census->>'reason','<null>'));

  v_apply:=pg_temp.claim_and_apply(v_pending);
  return pg_catalog.jsonb_build_object(
    'tag',p_tag,'pending_bundle_id',v_pending,
    'census_result',v_census->>'result',
    'apply',v_apply-'census'-'receipt'-'lock_result'-'heads',
    'receipt_id',v_apply->'receipt'->>'id',
    'receipt_mode',v_apply->'receipt'->>'publication_mode',
    'receipt_worker',v_apply->'receipt'->>'released_by_worker_id',
    'proof_sections',pg_catalog.jsonb_build_object(
      'cancellation',pg_catalog.jsonb_array_length(
        coalesce(v_apply->'receipt'->'proof_json'->'cancellation','[]'::jsonb)),
      'settlement',pg_catalog.jsonb_array_length(
        coalesce(v_apply->'receipt'->'proof_json'->'settlement','[]'::jsonb))),
    'bundle_state',(select state from public.weekly_source_pending_entitlement_bundles
                     where id=v_pending));
end;$f$;

do $race$
declare
  j jsonb:=ws_banking_fixture.base_world_identities_v1();
  v_request jsonb;
  v_saved jsonb;
  v_claim jsonb;
begin
  perform pg_temp.mk_root('R1',(j->>'timesheet_f_v1')::uuid,'ws-fixture-booking-f',
    (j->>'candidate_e_id')::uuid,101);
  v_request:=pg_temp.request('ee000000-0000-4000-8000-0000000000b1',
    'ee000000-0000-4000-8000-0000000000c1','ee000000-0000-4000-8000-0000000000d1',
    (j->>'timesheet_f_v1')::uuid,'ws-fixture-booking-f',(j->>'candidate_e_id')::uuid,'R1');
  perform pg_temp.mk_bundle('ee000000-0000-4000-8000-0000000000b1',
    'ee000000-0000-4000-8000-0000000000c1','ee000000-0000-4000-8000-0000000000d1',
    (j->>'timesheet_f_v1')::uuid,'ws-fixture-booking-f',(j->>'candidate_e_id')::uuid,'R1',v_request);
  v_saved:=private.weekly_source_pending_entitlement_bundle_save_v1(
    v_request,
    pg_catalog.jsonb_build_object('ok',true,'gate','GRANTED','families',
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'requested_timesheet_id',(j->>'timesheet_f_v1')::uuid,
        'family_booking_id','ws-fixture-booking-f',
        'canonical_timesheet_id',(j->>'timesheet_f_v1')::uuid,'canonical_version',1,
        'requested_is_canonical',true,'family_is_current',true,
        'member_timesheet_ids',pg_catalog.jsonb_build_array((j->>'timesheet_f_v1')::uuid)))),
    pg_catalog.jsonb_build_object('result','FROZEN','items','[]'::jsonb));
  raise notice 'SAVED %',v_saved::text;
  update public.weekly_source_pending_entitlement_bundles
     set next_check_at_utc=pg_catalog.clock_timestamp()-interval '1 second';
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
  v_claim:=private.weekly_source_pending_entitlement_release_claim_page_v1(
    'weekly-source:wp08b-race','ee000000-0000-4000-8000-00000000cc01',120,25);
  raise notice 'CLAIM %',v_claim::text;
end$race$;
commit;

-- Published so the suite reads the identity back instead of hard-coding it.
select 'WP16C_LEASE_RACE_READY' as result,
       bundle_row.id::text as pending_bundle_id,
       bundle_row.pending_revision::text as pending_revision,
       pg_catalog.encode(bundle_row.request_digest,'hex') as request_digest_hex,
       bundle_row.lease_token::text as lease_token,
       bundle_row.lease_owner as lease_owner,
       bundle_row.lease_worker_run_id::text as lease_worker_run_id,
       bundle_row.state as state
from public.weekly_source_pending_entitlement_bundles as bundle_row
order by bundle_row.created_at_utc
limit 1;
