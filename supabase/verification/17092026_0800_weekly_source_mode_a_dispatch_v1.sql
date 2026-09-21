-- Verification: weekly_source_mode_a_dispatch_v1  (Plan 6.2 Gate 8; XSG-009)
--
-- Drives the real owners end to end in one rolled-back transaction:
--   public.weekly_source_projection_rows_apply_atomic_v1
--   public.weekly_source_mode_a_dispatch_atomic_v1
--   public.hr_weekly_validation_preview            (the established engine)
--   public.hr_weekly_apply_transactional           (the established owner)
--   public.weekly_source_mode_a_reference_apply_atomic_v1
--
-- Proves, against pack `24 §14` and `25 §9`:
--   * a signed-Timesheet-authority upload is dispatched to the established
--     validation-only route and never to source finalisation;
--   * exact match writes the reference through the established owner;
--   * a mismatch holds the week, produces the existing manager correction
--     email action and writes no reference;
--   * unsigned / unsubmitted evidence does not participate;
--   * the Candidate is never queried and no secure manager link is created;
--   * no source manifest, self-bill, cutoff, protected pay or billing movement
--     is produced anywhere on the Mode A route;
--   * Daily is untouched.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do nothing;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('73000000-0000-4000-8000-000000000001','plan62-mode-a@example.test','admin',true,'not-a-login');

-- One complete Mode A scope.  `p_second_end` decides whether the second day
-- matches the signed Timesheet; `p_with_timesheet` decides whether the signed
-- Candidate-and-manager evidence exists at all.
create function pg_temp.mode_a_scope(
  p_code text,
  p_second_end time,
  p_with_timesheet boolean
) returns jsonb language plpgsql as $scope$
declare
  v_client uuid:=pg_catalog.gen_random_uuid();
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_contract uuid:=pg_catalog.gen_random_uuid();
  v_group uuid:=pg_catalog.gen_random_uuid();
  v_agency uuid:=pg_catalog.gen_random_uuid();
  v_cycle uuid:=pg_catalog.gen_random_uuid();
  v_upload uuid:=pg_catalog.gen_random_uuid();
  v_row_one uuid:=pg_catalog.gen_random_uuid();
  v_row_two uuid:=pg_catalog.gen_random_uuid();
  v_publication uuid:=pg_catalog.gen_random_uuid();
  v_contract_week uuid:=pg_catalog.gen_random_uuid();
  v_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_actor constant uuid:='73000000-0000-4000-8000-000000000001';
begin
  insert into public.clients(id,name) values (v_client,'Mode A Trust '||p_code);
  insert into public.client_settings(
    client_id,vat_rate_pct,week_ending_weekday,autoprocess_hr,requires_hr,
    no_timesheet_required,pay_reference_required,effective_from
  ) values (v_client,20,0,true,true,false,false,'2026-01-01');
  insert into public.candidates(id,display_name,first_name,last_name,active)
  values (v_candidate,'Mode A '||p_code,'Mode A',p_code,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,
    week_ending_weekday_snapshot,band
  ) values (
    v_contract,v_candidate,v_client,'2026-01-01','2026-12-31','PAYE','{}'::jsonb,
    'HEALTHROSTER',false,false,true,true,0,'Band 5'
  );
  -- The verified band/role authority the installed catalogue requires.
  insert into public.assignment_band_mappings(
    system_type,incoming_code,band_match_pattern,active,candidate_id,client_id,target_contract_id
  ) values ('HR_WEEKLY','Band 5 Nurse','band 5',true,v_candidate,v_client,v_contract);

  insert into public.weekly_source_groups(
    id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
  ) values (
    v_group,'TEST',v_agency,'PLAN62_MODEA_'||pg_catalog.upper(p_code),
    'Plan 6.2 Mode A '||p_code,'ROSTER',3,'15:00'
  );
  insert into public.weekly_source_group_clients(
    source_group_id,client_id,valid_from,created_by_user_id
  ) values (v_group,v_client,'2026-01-01',v_actor);
  insert into public.weekly_source_client_policies(
    source_group_id,client_id,effective_from,authority_mode,document_mode,
    self_bill_enabled,weekly_rate_classification_method,manager_queries_enabled,
    manager_query_recipient,created_by_user_id
  ) values (
    v_group,v_client,'2026-01-01','TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',
    false,'SPLIT_RATE_WINDOWS',true,'manager@example.test',v_actor
  );
  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (v_cycle,v_group,'2026-09-13','2026-09-09T14:00:00Z','OPEN',1,'REBUILDING');
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_json,header_coordinate_map_hash,
    declared_scope_fingerprint,suggested_coverage_start_local_date,suggested_coverage_end_local_date,
    confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,coverage_timezone,
    coverage_confirmation_version,coverage_confirmed_by_user_id,coverage_confirmed_at_utc,
    coverage_shrink_acknowledged,coverage_state,coverage_proof_kind,physical_row_count,
    accepted_count,row_manifest_hash,state,uploaded_by_user_id
  ) values (
    v_upload,v_cycle,'mode-a-'||p_code||'.csv',
    private.weekly_source_sha256_jsonb_v1('MODE_A_UPLOAD',pg_catalog.to_jsonb(p_code)),100,
    '35555555-5555-4555-8555-555555555555','PARSER_V1','NORMALISER_V1','{}',
    private.weekly_source_sha256_jsonb_v1('MODE_A_HEADER',pg_catalog.to_jsonb(p_code)),
    private.weekly_source_sha256_jsonb_v1('MODE_A_SCOPE',pg_catalog.to_jsonb(p_code)),
    '2026-09-07','2026-09-08','2026-09-07','2026-09-08','Europe/London','COMPLETE_EXPORT_V1',
    v_actor,pg_catalog.clock_timestamp(),false,'COMPLETE',
    'OFFICE_COMPLETE_EXPORT_ATTESTATION',2,2,
    private.weekly_source_sha256_jsonb_v1('MODE_A_MANIFEST',pg_catalog.to_jsonb(p_code)),
    'CURRENT',v_actor
  );
  update public.weekly_source_cycles set current_complete_upload_id=v_upload where id=v_cycle;
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
    source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
    actual_net_minutes,row_finalisation_state,role_band_source,source_expense_pence,
    source_expense_parse_state,normalised_row_hash
  ) values
  (v_row_one,v_upload,1,'REF-'||p_code||'-1','Mode A '||p_code,'Mode A Trust '||p_code,
   '2026-09-07','2026-09-07 09:00','2026-09-07 17:00',30,450,'NOT_APPLICABLE','Band 5 Nurse',0,
   'OMITTED_ZERO',private.weekly_source_sha256_jsonb_v1('MODE_A_ROW1',pg_catalog.to_jsonb(p_code))),
  (v_row_two,v_upload,2,'REF-'||p_code||'-2','Mode A '||p_code,'Mode A Trust '||p_code,
   '2026-09-08','2026-09-08 09:00',('2026-09-08 '||p_second_end::text)::timestamp,30,
   (pg_catalog.date_part('epoch',p_second_end-time '09:00')/60)::integer-30,
   'NOT_APPLICABLE','Band 5 Nurse',0,'OMITTED_ZERO',
   private.weekly_source_sha256_jsonb_v1('MODE_A_ROW2',pg_catalog.to_jsonb(p_code)));
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state
  ) values (
    v_publication,v_cycle,'CYCLE',v_upload,1,
    private.weekly_source_sha256_jsonb_v1('MODE_A_CMP',pg_catalog.to_jsonb(p_code)),
    private.weekly_source_sha256_jsonb_v1('MODE_A_ISS',pg_catalog.to_jsonb(p_code)),'BUILDING'
  );

  perform public.weekly_source_projection_rows_apply_atomic_v1(
    v_actor,v_publication,
    pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'upload_row_id',v_row_one,'mapping_state','RESOLVED','candidate_id',v_candidate,
        'client_id',v_client,'contract_id',v_contract,'contract_selection_method','AUTO_UNIQUE',
        'qualifying_contract_ids',pg_catalog.jsonb_build_array(v_contract),
        'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key','REF-'||p_code||'-1',
        'link_kind','TIMESHEET_EVIDENCE'
      ),
      pg_catalog.jsonb_build_object(
        'upload_row_id',v_row_two,'mapping_state','RESOLVED','candidate_id',v_candidate,
        'client_id',v_client,'contract_id',v_contract,'contract_selection_method','AUTO_UNIQUE',
        'qualifying_contract_ids',pg_catalog.jsonb_build_array(v_contract),
        'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key','REF-'||p_code||'-2',
        'link_kind','TIMESHEET_EVIDENCE'
      )
    )
  );
  update public.weekly_source_projection_publications
  set state='CURRENT',published_at_utc=pg_catalog.clock_timestamp() where id=v_publication;
  update public.weekly_source_cycles
  set projection_state='CURRENT',current_projection_publication_id=v_publication where id=v_cycle;

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    day_entries_json,totals_json
  ) values (v_contract_week,v_contract,'2026-09-13',0,'OPEN','ELECTRONIC','[]'::jsonb,'{}'::jsonb);

  if p_with_timesheet then
    -- Named seed: a complete, signed Candidate-and-manager Weekly Timesheet.
    -- In production the Candidate app creates this; Weekly Source never does.
    -- The signature columns satisfy chk_ts_signatures_for_electronic, which is
    -- the installed proof that both signatures are present.
    insert into public.timesheets(
      timesheet_id,contract_id,week_ending_date,sheet_scope,line_type,submission_mode,
      status,version,is_current,actual_schedule_json,booking_id,
      occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
      r2_nurse_key,r2_auth_key,img_sha256_nurse,img_sha256_auth
    ) values (
      v_timesheet,v_contract,'2026-09-13','WEEKLY','HOURS','ELECTRONIC','STORED',1,true,
      '[{"date":"2026-09-07","start":"09:00","end":"17:00","break_minutes":30},
        {"date":"2026-09-08","start":"09:00","end":"17:00","break_minutes":30}]'::jsonb,
      'MODEA-'||p_code,'mode a '||p_code,'mode a trust','ward','nurse',
      'test-only/nurse.png','test-only/manager.png',repeat('a',64),repeat('b',64)
    );
    update public.contract_weeks set timesheet_id=v_timesheet,status='SUBMITTED'
    where id=v_contract_week;
  end if;

  return pg_catalog.jsonb_build_object(
    'client_id',v_client,'candidate_id',v_candidate,'contract_id',v_contract,
    'cycle_id',v_cycle,'upload_id',v_upload,'publication_id',v_publication,
    'timesheet_id',case when p_with_timesheet then v_timesheet end,
    'row_one',v_row_one,'row_two',v_row_two
  );
end;
$scope$;

do $mode_a_verification$
declare
  v_actor constant uuid:='73000000-0000-4000-8000-000000000001';
  v_clean jsonb;
  v_held jsonb;
  v_unsigned jsonb;
  v_dispatch jsonb;
  v_apply jsonb;
  v_count integer;
  v_state text;
begin
  -- ── 1. Complete exact coverage ───────────────────────────────────────────
  v_clean:=pg_temp.mode_a_scope('CLEAN',time '17:00',true);
  v_dispatch:=public.weekly_source_mode_a_dispatch_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'publication_id',(v_clean->>'publication_id')::uuid
  ));
  if coalesce((v_dispatch->>'dispatched')::boolean,false) is not true
     or (v_dispatch->>'timesheet_authority_rows')::integer<>2 then
    raise exception 'MODE_A_CLEAN_DISPATCH_DID_NOT_RUN: %',v_dispatch::text;
  end if;
  -- The established owner's comparison, recorded by production code.  Both
  -- days match on times and on equal break duration, so both are EXACT_MATCH
  -- and both carry the source reference.
  select count(*)::integer into v_count
  from public.weekly_timesheet_source_comparisons comparison
  where comparison.projection_publication_id=(v_clean->>'publication_id')::uuid
    and comparison.comparison_state='EXACT_MATCH'
    and comparison.source_reference_number is not null
    and comparison.total_break_minutes_match;
  if v_count<>2 then
    raise exception 'MODE_A_CLEAN_EXACT_MATCH_COMPARISONS_NOT_WRITTEN: %',v_count;
  end if;
  if exists(
    select 1 from public.weekly_timesheet_source_comparisons comparison
    where comparison.projection_publication_id=(v_clean->>'publication_id')::uuid
      and comparison.comparison_state<>'EXACT_MATCH'
  ) then
    raise exception 'MODE_A_CLEAN_UNEXPECTED_COMPARISON_STATE';
  end if;

  v_apply:=public.weekly_source_mode_a_reference_apply_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'publication_id',(v_clean->>'publication_id')::uuid
  ));
  select operation.state into v_state
  from public.weekly_timesheet_reference_apply_operations operation
  where operation.projection_publication_id=(v_clean->>'publication_id')::uuid;
  if v_state is distinct from 'APPLIED' then
    raise exception 'MODE_A_CLEAN_REFERENCE_APPLY_DID_NOT_APPLY: % / %',v_state,v_apply::text;
  end if;
  -- `25 §9`: "A matching source row writes the reference".  The established
  -- owner writes it; this verifier only proves the result.
  select count(*)::integer into v_count
  from public.nhsp_shifts shift
  where shift.timesheet_id=(v_clean->>'timesheet_id')::uuid
    and nullif(pg_catalog.btrim(coalesce(shift.ref_num,'')),'') is not null;
  if v_count<>2 then
    raise exception 'MODE_A_CLEAN_REFERENCE_NOT_WRITTEN_BY_ESTABLISHED_OWNER: %',v_count;
  end if;
  if not exists(
    select 1 from public.weekly_timesheet_reference_apply_items item
    join public.weekly_timesheet_reference_apply_operations operation
      on operation.id=item.operation_id
    where operation.projection_publication_id=(v_clean->>'publication_id')::uuid
      and item.applied_at_utc is not null
  ) then
    raise exception 'MODE_A_CLEAN_REFERENCE_APPLY_ITEMS_NOT_RECORDED';
  end if;
  -- `25 §9`: complete coverage may auto-authorise only through the existing
  -- setting.  The decision is the established resolver's and is recorded, not
  -- taken here.
  if not exists(
    select 1 from public.weekly_timesheet_reference_apply_operations operation
    where operation.projection_publication_id=(v_clean->>'publication_id')::uuid
      and operation.auto_authorise_requested
      and operation.auto_authorise_applied
  ) then
    raise exception 'MODE_A_CLEAN_AUTO_AUTHORISATION_NOT_RECORDED';
  end if;

  -- ── 2. One mismatched day holds the week ─────────────────────────────────
  v_held:=pg_temp.mode_a_scope('HELD',time '18:00',true);
  v_dispatch:=public.weekly_source_mode_a_dispatch_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'publication_id',(v_held->>'publication_id')::uuid
  ));
  if coalesce((v_dispatch->>'dispatched')::boolean,false) is not true then
    raise exception 'MODE_A_HELD_DISPATCH_DID_NOT_RUN';
  end if;
  if not exists(
    select 1 from public.weekly_timesheet_source_comparisons comparison
    where comparison.projection_publication_id=(v_held->>'publication_id')::uuid
      and comparison.comparison_state='HOURS_MISMATCH' and comparison.work_date='2026-09-08'
  ) or not exists(
    select 1 from public.weekly_timesheet_source_comparisons comparison
    where comparison.projection_publication_id=(v_held->>'publication_id')::uuid
      and comparison.comparison_state='EXACT_MATCH' and comparison.work_date='2026-09-07'
  ) then
    raise exception 'MODE_A_HELD_COMPARISON_STATES_INVALID';
  end if;
  -- `24 §14`: "a mismatch uses the existing manager correction email journey".
  -- The established catalogue raises its own EMAIL_ISSUE action; Weekly Source
  -- creates no route of its own.
  if not exists(
    select 1 from public.import_review_decisions decision
    join public.hr_imports import_row on import_row.id=decision.import_id
    where import_row.client_id=(v_held->>'client_id')::uuid
      and decision.is_current and decision.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
  ) then
    raise exception 'MODE_A_HELD_MANAGER_CORRECTION_EMAIL_ACTION_ABSENT';
  end if;
  v_apply:=public.weekly_source_mode_a_reference_apply_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'publication_id',(v_held->>'publication_id')::uuid
  ));
  select operation.state into v_state
  from public.weekly_timesheet_reference_apply_operations operation
  where operation.projection_publication_id=(v_held->>'publication_id')::uuid;
  if v_state is distinct from 'REFUSED' then
    raise exception 'MODE_A_HELD_REFERENCE_APPLY_WAS_NOT_REFUSED: % / %',v_state,v_apply::text;
  end if;
  if exists(
    select 1 from public.nhsp_shifts shift
    where shift.timesheet_id=(v_held->>'timesheet_id')::uuid
      and nullif(pg_catalog.btrim(coalesce(shift.ref_num,'')),'') is not null
  ) then
    raise exception 'MODE_A_HELD_REFERENCE_WAS_WRITTEN_DESPITE_A_MISMATCH';
  end if;

  -- ── 3. Unsigned / unsubmitted evidence does not participate ──────────────
  v_unsigned:=pg_temp.mode_a_scope('UNSIGNED',time '17:00',false);
  v_dispatch:=public.weekly_source_mode_a_dispatch_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'publication_id',(v_unsigned->>'publication_id')::uuid
  ));
  if exists(
    select 1 from public.weekly_timesheet_source_comparisons comparison
    where comparison.projection_publication_id=(v_unsigned->>'publication_id')::uuid
  ) then
    raise exception 'MODE_A_UNSIGNED_EVIDENCE_PRODUCED_A_COMPARISON';
  end if;
  if not exists(
    select 1 from public.import_review_decisions decision
    join public.hr_imports import_row on import_row.id=decision.import_id
    where import_row.client_id=(v_unsigned->>'client_id')::uuid
      and decision.is_current and decision.blocking
      and decision.summary_json->>'reason_code'='WEEKLY_TIMESHEET_NOT_SUBMITTED'
  ) then
    raise exception 'MODE_A_UNSIGNED_EVIDENCE_WAS_NOT_HELD_BY_THE_ESTABLISHED_OWNER';
  end if;

  -- ── 4. Boundaries ────────────────────────────────────────────────────────
  -- No source-authority artefact anywhere on the Mode A route.
  if exists(select 1 from public.weekly_source_billing_movements)
     or exists(select 1 from public.weekly_source_final_revisions)
     or exists(select 1 from public.weekly_source_charge_checks)
     or exists(select 1 from public.weekly_source_row_economic_snapshots)
     or exists(select 1 from public.weekly_exceptional_pay_target_families) then
    raise exception 'MODE_A_CROSSED_INTO_THE_SOURCE_AUTHORITY_ROUTE';
  end if;
  -- The Candidate is never queried, and no secure manager link is created.
  if exists(select 1 from public.weekly_discrepancy_incidents)
     or exists(select 1 from public.weekly_discrepancy_events)
     or exists(select 1 from public.weekly_message_delivery_failures) then
    raise exception 'MODE_A_CREATED_A_CANDIDATE_OR_SECURE_QUERY';
  end if;
  -- Daily is untouched: every import this route created is HR_WEEKLY.
  if exists(
    select 1 from public.hr_imports import_row
    where import_row.parser_version='WEEKLY_SOURCE_MODE_A_BRIDGE_V1'
      and (import_row.import_scope is distinct from 'HR_WEEKLY'
        or import_row.source_system<>'HEALTHROSTER'::public.hr_source_enum)
  ) then
    raise exception 'MODE_A_BRIDGE_CREATED_A_NON_WEEKLY_IMPORT';
  end if;
  if exists(
    select 1 from public.import_review_decisions decision
    join public.hr_imports import_row on import_row.id=decision.import_id
    where import_row.parser_version='WEEKLY_SOURCE_MODE_A_BRIDGE_V1'
      and decision.summary_json->>'source_route' like '%DAILY%'
  ) then
    raise exception 'MODE_A_BRIDGE_PRODUCED_A_DAILY_ACTION';
  end if;
  -- Every Mode A authority resolution defaults `Reference required before pay`
  -- to false and is client-level (`25 §9`).
  if exists(
    select 1 from public.weekly_timesheet_authority_resolutions authority
    where authority.require_reference_to_pay is not false
  ) then
    raise exception 'MODE_A_REFERENCE_REQUIRED_BEFORE_PAY_DID_NOT_DEFAULT_FALSE';
  end if;
end;
$mode_a_verification$;

select pg_catalog.jsonb_build_object(
  'ok',true,'verification','weekly_source_mode_a_dispatch_v1',
  'comparisons',(select count(*) from public.weekly_timesheet_source_comparisons),
  'authority_resolutions',(select count(*) from public.weekly_timesheet_authority_resolutions),
  'reference_apply_operations',(
    select pg_catalog.jsonb_object_agg(state,count)
    from (
      select state,count(*)::integer count
      from public.weekly_timesheet_reference_apply_operations group by state
    ) states
  ),
  'reference_apply_items',(select count(*) from public.weekly_timesheet_reference_apply_items),
  'bridged_imports',(
    select count(*) from public.hr_imports
    where parser_version='WEEKLY_SOURCE_MODE_A_BRIDGE_V1'
  ),
  'financial_writes',(select count(*) from public.weekly_source_billing_movements)
);

rollback;
