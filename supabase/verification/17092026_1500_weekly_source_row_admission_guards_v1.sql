-- Rollback-only proof for weekly_source_row_admission_guards_v1 (WP-54).
--
-- Two pack rules that nothing enforced before this package, each proved TWICE:
-- once as a direct call on the guard, and once END TO END through the real
-- owner `public.weekly_source_upload_seal_atomic_v1`, so that no assertion here
-- rests on reading a function's text.
--
--   NHSP-BR-006 (14 section 4.1.3): "The service refuses a cutoff earlier than
--   any row's real `Actual` finishing instant, including an overnight finish."
--
--   03 section 7: "Overlapping candidate work events are checked before
--   finalisation.  The system does not assume two overlapping records are valid
--   because their references differ." -- with 14 section 4.2.8 (the row
--   "blocks finalisation") and 14 section 4.1.7 ("Finalisation is all or
--   nothing.  A blocker cannot be removed, unticked or ignored"), which is why
--   the whole report is refused rather than the offending row dropped.
--
-- Every case that MUST still pass is asserted as well as every case that must
-- refuse, because a guard that refuses too much is as wrong as one that refuses
-- too little: an abutting split shift, two different people on the same
-- interval, and -- the one that matters for NHSP -- a full reversal together
-- with the re-issue it overlaps (14 section 4.2.4, a negative line "is the full
-- reversal supplied by NHSP"), all still seal.
--
-- Prerequisites: Plan 6 schema, private classifiers, settings/profile registry,
-- upload/publication repeatable and this package's repeatable.
-- Creates no Timesheet, financial, invoice, Workbench or Banking Pay record.

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

create function pg_temp.guard_begin(
  p_report_number text,
  p_accepted integer
) returns jsonb language plpgsql as $function$
begin
  return public.weekly_source_upload_stage_begin_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','95400000-0000-4000-8000-000000000001',
      'environment','TEST',
      'agency_id','95400000-0000-4000-8000-000000000002',
      'source_group_id','95400000-0000-4000-8000-000000000030',
      'source_cycle_id','95400000-0000-4000-8000-000000000031',
      'report_scope_id','95400000-0000-4000-8000-000000000050',
      'client_id','95400000-0000-4000-8000-000000000040',
      'original_filename','nhsp-'||p_report_number||'.xlsx',
      'content_sha256',pg_catalog.encode(
        pg_catalog.sha256(pg_catalog.convert_to(p_report_number,'UTF8')),'hex'),
      'byte_count',2048,
      'profile_code','NHSP_FINAL_BACKING_V1',
      'profile_version',1,
      'parser_version','WEEKLY_SOURCE_STRICT_V1',
      'normaliser_version','NHSP_BACKING_NORMALISER_V1',
      'workbook_part_and_sheet_fingerprint',repeat('1',64),
      'header_coordinate_map_json',pg_catalog.jsonb_build_object(
        'Actual Start','L','Actual End','M','Actual Break','N','Actual Total','O',
        'Commission','P','Total Cost','Q','FMC','R'
      ),
      'money_lexical_authority_version','XLSX_BINARY64_SAME_VALUE_PENCE_V1',
      'purpose','ORDINARY',
      'coverage_proof_kind','NHSP_TRUST_REPORT_SCOPE',
      'physical_row_count',p_accepted+2,
      'header_count',1,
      'trailer_count',1,
      'continuation_count',0,
      'accepted_count',p_accepted,
      'blocking_economic_duplicate_count',0,
      'malformed_count',0,
      'blocked_count',0,
      'file_metadata_json',pg_catalog.jsonb_build_object(
        'nhsp_report_number',p_report_number,
        'nhsp_report_heading_name','Guard Trust'
      ),
      'parser_summary_json',pg_catalog.jsonb_build_object('fatal_errors',0)
    )
  );
end;
$function$;

-- p_rows: [{candidate, work_date, start, end, break, net, commission, total_cost}]
create function pg_temp.guard_stage(
  p_upload_id uuid,p_report_number text,p_rows jsonb
) returns jsonb language plpgsql as $function$
declare
  v_n integer:=pg_catalog.jsonb_array_length(p_rows);
  v_physical jsonb:=pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'source_row_ordinal',1,'classification','HEADER',
    'bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','Agency')));
  v_normalised jsonb:='[]'::jsonb;
  v_money jsonb:='[]'::jsonb;
  v_row jsonb;
  v_ord integer;
  i integer;
begin
  for i in 0..v_n-1 loop
    v_row:=p_rows->i;
    v_ord:=i+2;
    v_physical:=v_physical||pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_ord,'classification','ACCEPTED_SHIFT',
      'bounded_raw_cells_json',pg_catalog.jsonb_build_object(
        'A',v_row->>'candidate','row',v_ord::text));
    v_normalised:=v_normalised||pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_ord,
      'external_source_key','NHSP-'||p_report_number||'-'||v_ord::text,
      'source_candidate_identity',v_row->>'candidate',
      'source_client_identity','Guard Trust',
      'work_date',v_row->>'work_date',
      'start_at_local',v_row->>'start',
      'end_at_local',v_row->>'end',
      'break_minutes',(v_row->>'break')::integer,
      'actual_net_minutes',(v_row->>'net')::integer,
      'row_finalisation_state','SOURCE_WORKED',
      'role_band_source','BAND 5',
      'source_commission_pence',(v_row->>'commission')::bigint,
      'source_total_cost_pence',(v_row->>'total_cost')::bigint,
      'source_shift_charge_pence',
        (v_row->>'commission')::bigint+(v_row->>'total_cost')::bigint,
      'source_money_parse_state','VALID',
      'source_qualification_profile_version','NHSP_TWO_COMPONENT_PENCE_V1',
      'source_expense_parse_state','NOT_APPLICABLE',
      'bounded_raw_columns_json',pg_catalog.jsonb_build_object(
        'Actual Start',v_row->>'start'));
    v_money:=v_money
      ||pg_catalog.jsonb_build_object(
          'source_row_ordinal',v_ord,'money_field_kind','COMMISSION',
          'source_column_index',15,'cell_coordinate','P'||v_ord::text,
          'source_kind','XLSX_NUMERIC_TOKEN',
          'original_token',v_row->>'commission','decoded_token',v_row->>'commission',
          'cell_type_marker','n','formula_present',false,'parse_state','VALID',
          'parsed_pence',(v_row->>'commission')::bigint)
      ||pg_catalog.jsonb_build_object(
          'source_row_ordinal',v_ord,'money_field_kind','TOTAL_COST',
          'source_column_index',16,'cell_coordinate','Q'||v_ord::text,
          'source_kind','XLSX_NUMERIC_TOKEN',
          'original_token',v_row->>'total_cost','decoded_token',v_row->>'total_cost',
          'cell_type_marker','n','formula_present',false,'parse_state','VALID',
          'parsed_pence',(v_row->>'total_cost')::bigint)
      ||pg_catalog.jsonb_build_object(
          'source_row_ordinal',v_ord,'money_field_kind','FMC',
          'source_column_index',17,'cell_coordinate','R'||v_ord::text,
          'source_kind','XLSX_NUMERIC_TOKEN',
          'original_token','0','decoded_token','0.00',
          'cell_type_marker','n','formula_present',false,'parse_state','VALID',
          'parsed_pence',0);
  end loop;
  v_physical:=v_physical||pg_catalog.jsonb_build_object(
    'source_row_ordinal',v_n+2,'classification','TRAILER',
    'bounded_raw_cells_json',pg_catalog.jsonb_build_object('Q','0.00'));
  return public.weekly_source_upload_stage_rows_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','95400000-0000-4000-8000-000000000001',
      'upload_id',p_upload_id,
      'physical_rows',v_physical,
      'normalised_rows',v_normalised,
      'money_evidence',v_money,
      'expense_evidence','[]'::jsonb));
end;
$function$;

-- Drives the REAL seal and returns {status, reason_code, upload_id}.
create function pg_temp.guard_seal(
  p_report_number text,p_rows jsonb
) returns jsonb language plpgsql as $function$
declare
  v_upload uuid;
  v_seal jsonb;
begin
  v_upload:=(pg_temp.guard_begin(
    p_report_number,pg_catalog.jsonb_array_length(p_rows))->>'logical_upload_id')::uuid;
  perform pg_temp.guard_stage(v_upload,p_report_number,p_rows);
  v_seal:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','95400000-0000-4000-8000-000000000001','upload_id',v_upload));
  return v_seal||pg_catalog.jsonb_build_object('probe_upload_id',v_upload);
end;
$function$;

create function pg_temp.guard_row(
  p_candidate text,p_date text,p_start text,p_end text,
  p_break integer,p_net integer,p_commission bigint,p_total_cost bigint
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'candidate',p_candidate,'work_date',p_date,'start',p_start,'end',p_end,
    'break',p_break,'net',p_net,'commission',p_commission,'total_cost',p_total_cost);
$function$;

insert into public.tms_users(
  id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key
) values (
  '95400000-0000-4000-8000-000000000001',
  'weekly-row-admission-guards-verification@example.invalid','admin',true,
  'not-a-login',true,false
);
insert into public.clients(id,cli_ref,name) values
  ('95400000-0000-4000-8000-000000000040','CLI-95401','Guard NHSP Trust');
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,timezone,
  cutoff_weekday,cutoff_local_time,nhsp_report_heading_name
) values (
  '95400000-0000-4000-8000-000000000030','TEST',
  '95400000-0000-4000-8000-000000000002','NHSP_GUARD','NHSP Guard',
  'NHSP','Europe/London',3,'15:00','Guard Trust'
);
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values (
  '95400000-0000-4000-8000-000000000030','95400000-0000-4000-8000-000000000040',
  '2026-01-01','95400000-0000-4000-8000-000000000001'
);
-- The cutoff occurrence: Wednesday 16 September 2026 at 15:00 Europe/London,
-- which is 14:00:00+00 because 16 September is inside British Summer Time.
-- Every case below is stated against that one instant.
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc
) values (
  '95400000-0000-4000-8000-000000000031','95400000-0000-4000-8000-000000000030',
  '2026-09-20','2026-09-16 14:00:00+00'
);
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc
) values (
  '95400000-0000-4000-8000-000000000050','95400000-0000-4000-8000-000000000031','TEST',
  '95400000-0000-4000-8000-000000000002','95400000-0000-4000-8000-000000000030',
  '95400000-0000-4000-8000-000000000040','2026-09-16 14:00:00+00'
);

do $row_admission_guards$
declare
  v_result jsonb;
  v_raised text;
  v_upload uuid;
begin
  -- =======================================================================
  -- Part 1: the guards themselves, called directly.
  -- =======================================================================

  -- An empty upload breaches neither rule.
  v_result:=pg_temp.guard_seal('BRG-001',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T17:00:00',30,450,500,15500)));
  perform pg_temp.assert_true(v_result->>'status'='CURRENT',
    'a compliant NHSP backing report must still seal');
  v_upload:=(v_result->>'probe_upload_id')::uuid;

  -- Directly: the same rows against a cutoff BEFORE their finish.
  begin
    perform private.weekly_source_cutoff_admission_assert_v1(
      v_upload,'NHSP_FINAL_BACKING_V1','2026-09-14 15:00:00+00');
    v_raised:='NONE';
  exception when others then
    v_raised:=sqlerrm;
  end;
  perform pg_temp.assert_true(v_raised='WEEKLY_SOURCE_NHSP_CUTOFF_BEFORE_ACTUAL_FINISH',
    'the cutoff guard must refuse a cutoff earlier than a row Actual finish, got '||v_raised);

  -- Directly: EXACTLY equal is admitted -- the cutoff is not EARLIER.
  -- 2026-09-14 17:00 Europe/London is 16:00:00+00 (British Summer Time).
  begin
    perform private.weekly_source_cutoff_admission_assert_v1(
      v_upload,'NHSP_FINAL_BACKING_V1','2026-09-14 16:00:00+00');
    v_raised:='NONE';
  exception when others then
    v_raised:=sqlerrm;
  end;
  perform pg_temp.assert_true(v_raised='NONE',
    'a cutoff exactly equal to the Actual finish must be admitted, got '||v_raised);

  -- Directly: one second earlier refuses.  This is the British Summer Time
  -- boundary case as well: read as a naive UTC instant the row would finish at
  -- 17:00Z and pass, so a guard that skipped the zone conversion would not
  -- refuse here.
  begin
    perform private.weekly_source_cutoff_admission_assert_v1(
      v_upload,'NHSP_FINAL_BACKING_V1','2026-09-14 15:59:59+00');
    v_raised:='NONE';
  exception when others then
    v_raised:=sqlerrm;
  end;
  perform pg_temp.assert_true(v_raised='WEEKLY_SOURCE_NHSP_CUTOFF_BEFORE_ACTUAL_FINISH',
    'one second before the Actual finish must refuse, got '||v_raised);

  -- Directly: the rule is NHSP final only; another profile returns.
  begin
    perform private.weekly_source_cutoff_admission_assert_v1(
      v_upload,'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','2026-09-01 00:00:00+00');
    v_raised:='NONE';
  exception when others then
    v_raised:=sqlerrm;
  end;
  perform pg_temp.assert_true(v_raised='NONE',
    'the cutoff rule must not fire outside NHSP_FINAL_BACKING_V1, got '||v_raised);

  -- Directly: fail closed on a missing cutoff occurrence.
  begin
    perform private.weekly_source_cutoff_admission_assert_v1(
      v_upload,'NHSP_FINAL_BACKING_V1',null);
    v_raised:='NONE';
  exception when others then
    v_raised:=sqlerrm;
  end;
  perform pg_temp.assert_true(v_raised='WEEKLY_SOURCE_NHSP_CUTOFF_OCCURRENCE_REQUIRED',
    'a missing cutoff occurrence must fail closed, got '||v_raised);

  -- Directly: a null upload id is a caller error, not a silent pass.
  begin
    perform private.weekly_source_overlap_admission_assert_v1(null);
    v_raised:='NONE';
  exception when others then
    v_raised:=sqlerrm;
  end;
  perform pg_temp.assert_true(v_raised='WEEKLY_SOURCE_OVERLAP_ASSERT_INPUT_INVALID',
    'the overlap guard must refuse a null upload id, got '||v_raised);

  -- Directly: the sealed single-row report has no overlap.
  begin
    perform private.weekly_source_overlap_admission_assert_v1(v_upload);
    v_raised:='NONE';
  exception when others then
    v_raised:=sqlerrm;
  end;
  perform pg_temp.assert_true(v_raised='NONE',
    'a one-row report must not be reported as overlapping, got '||v_raised);

  -- =======================================================================
  -- Part 2: end to end through the real seal.
  -- =======================================================================

  -- NHSP-BR-006: a row worked two days AFTER the confirmed cutoff.  Before this
  -- package this report sealed CURRENT.
  v_result:=pg_temp.guard_seal('BRG-002',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-18',
      '2026-09-18T09:00:00','2026-09-18T17:00:00',30,450,500,15500)));
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
    and v_result->>'reason_code'='WEEKLY_SOURCE_NHSP_CUTOFF_BEFORE_ACTUAL_FINISH',
    'the seal must refuse a row finishing after the cutoff, got '
    ||coalesce(v_result->>'status','?')||'/'||coalesce(v_result->>'reason_code','?'));
  perform pg_temp.assert_true(
    (select state from public.weekly_source_uploads
     where id=(v_result->>'probe_upload_id')::uuid)='REJECTED',
    'a refused report must not remain staging or become current');

  -- "including an overnight finish": 15 September 22:00 to 16 September 15:00
  -- Europe/London finishes at exactly 14:00:00+00, the cutoff instant itself,
  -- so it is admitted.
  v_result:=pg_temp.guard_seal('BRG-003',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-15',
      '2026-09-15T22:00:00','2026-09-16T15:00:00',60,960,500,15500)));
  perform pg_temp.assert_true(v_result->>'status'='CURRENT',
    'an overnight shift finishing exactly at the cutoff must seal, got '
    ||coalesce(v_result->>'status','?'));

  -- The same overnight shift one minute later must refuse.  These two cases
  -- differ by one minute and nothing else.
  v_result:=pg_temp.guard_seal('BRG-004',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-15',
      '2026-09-15T22:00:00','2026-09-16T15:01:00',60,961,500,15500)));
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
    and v_result->>'reason_code'='WEEKLY_SOURCE_NHSP_CUTOFF_BEFORE_ACTUAL_FINISH',
    'an overnight shift finishing one minute after the cutoff must refuse, got '
    ||coalesce(v_result->>'status','?')||'/'||coalesce(v_result->>'reason_code','?'));

  -- 03 section 7: two POSITIVE lines, one Candidate, one date, overlapping.
  -- Before this package this report sealed and carried GBP 340.00 of invoice
  -- authority for one shift.
  v_result:=pg_temp.guard_seal('BRG-005',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T17:00:00',0,480,500,15500),
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T18:00:00',0,540,500,17500)));
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
    and v_result->>'reason_code'='WEEKLY_SOURCE_OVERLAPPING_WORKED_ROWS',
    'the seal must refuse two overlapping positive lines for one person, got '
    ||coalesce(v_result->>'status','?')||'/'||coalesce(v_result->>'reason_code','?'));

  -- A £0 source charge does not mean no work.  It is the explicit rate-card
  -- warning journey and must still participate in duplicate/overlap admission.
  v_result:=pg_temp.guard_seal('BRG-011',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate Zero','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T17:00:00',0,480,0,0),
    pg_temp.guard_row('Guard Candidate Zero','2026-09-14',
      '2026-09-14T12:00:00','2026-09-14T18:00:00',0,360,0,0)));
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
    and v_result->>'reason_code'='WEEKLY_SOURCE_OVERLAPPING_WORKED_ROWS',
    'two overlapping £0 worked rows must refuse before Office rate acceptance, got '
    ||coalesce(v_result->>'status','?')||'/'||coalesce(v_result->>'reason_code','?'));

  -- Must still seal: an abutting split shift.  Half-open intervals.
  v_result:=pg_temp.guard_seal('BRG-006',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T13:00:00',0,240,250,7750),
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T13:00:00','2026-09-14T18:00:00',0,300,250,9750)));
  perform pg_temp.assert_true(v_result->>'status'='CURRENT',
    'an abutting split shift must still seal, got '||coalesce(v_result->>'status','?'));

  -- Must still seal: 14 section 4.2.4 -- a full reversal and the re-issue it
  -- overlaps.  This is the ordinary NHSP correction shape and the single most
  -- important thing this guard must NOT break.
  v_result:=pg_temp.guard_seal('BRG-007',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T17:00:00',0,480,-500,-15500),
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T18:00:00',0,540,500,17500)));
  perform pg_temp.assert_true(v_result->>'status'='CURRENT',
    'a full reversal and its overlapping re-issue must still seal, got '
    ||coalesce(v_result->>'status','?'));

  -- Must still seal: two DIFFERENT people on the same interval.
  v_result:=pg_temp.guard_seal('BRG-008',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T17:00:00',0,480,500,15500),
    pg_temp.guard_row('Guard Candidate Two','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T17:00:00',0,480,500,15500)));
  perform pg_temp.assert_true(v_result->>'status'='CURRENT',
    'two different people on one interval must still seal, got '
    ||coalesce(v_result->>'status','?'));

  -- Must refuse: an overlap of one minute, and an overlap that spans midnight
  -- into the next calendar date, so the rule is not secretly keyed on
  -- work_date.
  v_result:=pg_temp.guard_seal('BRG-009',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T09:00:00','2026-09-14T13:01:00',0,241,250,7750),
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T13:00:00','2026-09-14T18:00:00',0,300,250,9750)));
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
    and v_result->>'reason_code'='WEEKLY_SOURCE_OVERLAPPING_WORKED_ROWS',
    'a one-minute overlap must refuse, got '||coalesce(v_result->>'status','?'));

  v_result:=pg_temp.guard_seal('BRG-010',pg_catalog.jsonb_build_array(
    pg_temp.guard_row('Guard Candidate','2026-09-13',
      '2026-09-13T20:00:00','2026-09-14T04:00:00',0,480,250,7750),
    pg_temp.guard_row('Guard Candidate','2026-09-14',
      '2026-09-14T03:00:00','2026-09-14T11:00:00',0,480,250,9750)));
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
    and v_result->>'reason_code'='WEEKLY_SOURCE_OVERLAPPING_WORKED_ROWS',
    'an overlap across two calendar dates must refuse, got '
    ||coalesce(v_result->>'status','?')||'/'||coalesce(v_result->>'reason_code','?'));

  -- The guards decide nothing else: no Timesheet, financial, invoice or
  -- Banking Pay row exists after all of the above.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_billing_movements)=0
    and (select pg_catalog.count(*) from public.weekly_source_final_revisions)=0,
    'the admission guards must create no financial record');
end;
$row_admission_guards$;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_row_admission_guards_v1',
  'rules',pg_catalog.jsonb_build_array('NHSP-BR-006','03 section 7 / 14 section 4.2.8'),
  'sealed_reports',(select pg_catalog.count(*) from public.weekly_source_uploads
                    where state='CURRENT' or state='SUPERSEDED'),
  'refused_reports',(select pg_catalog.count(*) from public.weekly_source_uploads
                     where state='REJECTED'),
  'financial_records',(select pg_catalog.count(*) from public.weekly_source_billing_movements)
);

rollback;
