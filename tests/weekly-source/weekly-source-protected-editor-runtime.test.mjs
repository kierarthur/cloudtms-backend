import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

test('protected editor retains immutable history and matches changed hours only for the same candidate and date', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const fixture = readFileSync('supabase/verification/15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'utf8');
  const marker = 'create temp table before_wait as';
  assert.equal(fixture.split(marker).length, 2);
  const sql = fixture.slice(0, fixture.indexOf(marker)) + `
do $proof$
declare context jsonb; before_runs bigint;
begin
  select count(*) into before_runs from public.weekly_exceptional_orchestration_runs;
  select public.weekly_source_protected_editor_context_v1(jsonb_build_object(
    'actor_user_id','f1000000-0000-4000-8000-000000000001',
    'client_id','f1000000-0000-4000-8000-000000000002',
    'candidate_id','f1000000-0000-4000-8000-000000000003',
    'work_date','2026-09-07','work_event_id',result->>'work_event_id'))
    into context from prepared_family;
  perform pg_temp.assert_true(jsonb_array_length(context->'history')=1,'one real approval event');
  perform pg_temp.assert_true(context#>>'{history,0,reason}'='Initial protected hours.','stored reason retained');
  perform pg_temp.assert_true(context#>'{history,0,before}'='null'::jsonb,'initial approval has no invented predecessor');
  perform pg_temp.assert_true(context#>>'{history,0,after,break_minutes}'='30','stored break retained');
  perform pg_temp.assert_true(context#>>'{history,0,at}' is not null,'real event time');
  perform pg_temp.assert_true((select count(*)=before_runs from public.weekly_exceptional_orchestration_runs),'history read creates no action');
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
    header_coordinate_map_hash,money_lexical_authority_version,declared_scope_fingerprint,
    coverage_proof_kind,physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id
  ) values (
    'f2000000-0000-4000-8000-000000000001','f1000000-0000-4000-8000-000000000009',
    'protected-match-proof.xlsx',decode(repeat('41',32),'hex'),200,
    '33333333-3333-4333-8333-333333333333','PROOF','PROOF',decode(repeat('42',32),'hex'),'{}',
    decode(repeat('43',32),'hex'),'NOT_APPLICABLE',decode(repeat('44',32),'hex'),
      'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',2,2,decode(repeat('45',32),'hex'),'CURRENT',
    'f1000000-0000-4000-8000-000000000001'
  );
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,source_client_identity,
    work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,normalised_row_hash
  ) values (
    'f2000000-0000-4000-8000-000000000002','f2000000-0000-4000-8000-000000000001',1,
    'future-roster-reference','Protected Action Candidate','Protected Action Client','2026-09-07',
    '2026-09-07 09:00','2026-09-07 16:00',15,405,'SOURCE_WORKED',decode(repeat('46',32),'hex')
  );
  context:=private.weekly_source_protected_match_candidates_v1(
    'f2000000-0000-4000-8000-000000000002','f1000000-0000-4000-8000-000000000003',
    'f1000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_true(jsonb_array_length(context)=1,'different source hours retain the same protected work candidate');
  perform pg_temp.assert_true((context#>>'{0,schedule_compatible}')::boolean,'overlapping changed schedule remains eligible');
  perform pg_temp.assert_true(context#>>'{0,break_minutes}'='30','matching does not overwrite the approved break with imported 15 minutes');
  perform pg_temp.assert_true(jsonb_array_length(private.weekly_source_protected_match_candidates_v1(
    'f2000000-0000-4000-8000-000000000002','f1000000-0000-4000-8000-000000000001',
    'f1000000-0000-4000-8000-000000000002'))=0,'another candidate cannot match');
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,source_client_identity,
    work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,normalised_row_hash
  ) values (
    'f2000000-0000-4000-8000-000000000003','f2000000-0000-4000-8000-000000000001',2,
    'other-day-reference','Protected Action Candidate','Protected Action Client','2026-09-08',
    '2026-09-08 09:00','2026-09-08 16:00',15,405,'SOURCE_WORKED',decode(repeat('47',32),'hex')
  );
  perform pg_temp.assert_true(jsonb_array_length(private.weekly_source_protected_match_candidates_v1(
    'f2000000-0000-4000-8000-000000000003','f1000000-0000-4000-8000-000000000003',
    'f1000000-0000-4000-8000-000000000002'))=0,'another date cannot match');
end;
$proof$;
rollback;`;
  const result = spawnSync(process.env.CLOUDTMS_TEST_PSQL || 'psql', ['-X','-h','127.0.0.1',
    '-p',process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test',
    '-v','ON_ERROR_STOP=1'], { input: sql, encoding: 'utf8', env: { ...process.env, PGOPTIONS: '-c jit=off' } });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
});

test('protected editor: no import, no timesheet, contracts, scoped cycle and permissions', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const port = process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT;
  assert.match(port, /^\d+$/);
  const fixture = readFileSync('supabase/verification/15092026_1534_weekly_source_protected_pay_publisher_v1.sql', 'utf8');
  const marker = 'insert into public.weekly_source_cycles(';
  assert.equal(fixture.split(marker).length, 2, 'fixture seed boundary must be unique');
  const seed = fixture.slice(0, fixture.indexOf(marker));
  const sql = seed + `
do $proof$
declare
  request jsonb := jsonb_build_object(
    'actor_user_id','e1000000-0000-4000-8000-000000000001',
    'client_id','e1000000-0000-4000-8000-000000000003',
    'candidate_id','e1000000-0000-4000-8000-000000000004',
    'work_date','2026-09-07');
  context jsonb;
  prepared jsonb;
  other_cycle jsonb;
  cycle_count integer;
  scopes jsonb;
  scope_page jsonb;
begin
  context := public.weekly_source_protected_editor_context_v1(request);
  perform pg_temp.assert_true(context->>'source_cycle_id' is null,'read requires no cycle');
  perform pg_temp.assert_true(jsonb_array_length(context->'contracts')=1,'one qualified contract');
  perform pg_temp.assert_true(context#>>'{contracts,0,week_ending_date}'='2026-09-13','contract week ending');
  perform pg_temp.assert_true(not exists(select 1 from public.contract_weeks where contract_id='e1000000-0000-4000-8000-000000000005'),'read creates no contract week');
  prepared := public.weekly_source_protected_editor_prepare_v1(request);
  perform pg_temp.assert_true(prepared->>'source_cycle_id' is not null,'no-import context allocated');
  perform pg_temp.assert_true(exists(select 1 from public.weekly_source_cycles where id=(prepared->>'source_cycle_id')::uuid and scope_client_id='e1000000-0000-4000-8000-000000000003'),'roster cycle belongs to selected client');
  select count(*) into cycle_count from public.weekly_source_cycles where source_group_id='e1000000-0000-4000-8000-000000000006';
  perform public.weekly_source_protected_editor_prepare_v1(request);
  perform pg_temp.assert_true((select count(*)=cycle_count from public.weekly_source_cycles where source_group_id='e1000000-0000-4000-8000-000000000006'),'repeat preparation creates no extra cycles');
  perform pg_temp.assert_true(not exists(select 1 from public.timesheets where contract_id='e1000000-0000-4000-8000-000000000005'),'preparation creates no timesheet');
  insert into public.clients(id,name) values('e2000000-0000-4000-8000-000000000003','Independent second client');
  insert into public.weekly_source_group_clients(id,source_group_id,client_id,valid_from,created_by_user_id)
    values('e2000000-0000-4000-8000-000000000008','e1000000-0000-4000-8000-000000000006',
      'e2000000-0000-4000-8000-000000000003','2026-01-01','e1000000-0000-4000-8000-000000000001');
  insert into public.weekly_source_client_policies(id,source_group_id,client_id,effective_from,authority_mode,document_mode,
    self_bill_enabled,self_bill_correction_presentation,source_fixed_expenses_enabled,source_expense_vat_enabled,
    weekly_rate_classification_method,created_by_user_id)
    select 'e2000000-0000-4000-8000-000000000009',source_group_id,'e2000000-0000-4000-8000-000000000003',
      effective_from,authority_mode,document_mode,self_bill_enabled,self_bill_correction_presentation,
      source_fixed_expenses_enabled,source_expense_vat_enabled,weekly_rate_classification_method,created_by_user_id
    from public.weekly_source_client_policies where id='e1000000-0000-4000-8000-000000000009';
  other_cycle:=public.weekly_source_client_cycle_resolve_atomic_v1(jsonb_build_object(
    'actor_user_id',request->>'actor_user_id','source_cycle_id',prepared->>'source_cycle_id',
    'client_id','e2000000-0000-4000-8000-000000000003'));
  perform pg_temp.assert_true(other_cycle->>'source_cycle_id'<>prepared->>'source_cycle_id',
    'two source-authority clients must not share the source publication pointer');
  perform pg_temp.assert_true((select a.finalisation_week_ending=b.finalisation_week_ending
    from public.weekly_source_cycles a,public.weekly_source_cycles b
    where a.id=(prepared->>'source_cycle_id')::uuid and b.id=(other_cycle->>'source_cycle_id')::uuid),
    'independent clients retain the same chosen finalisation period');
  perform pg_temp.assert_true(public.weekly_source_client_cycle_resolve_atomic_v1(jsonb_build_object(
    'actor_user_id',request->>'actor_user_id','source_cycle_id',prepared->>'source_cycle_id',
    'client_id','e2000000-0000-4000-8000-000000000003'))=other_cycle,'second client selection reuses its own cycle');
  scopes:=public.weekly_source_workspace_scopes_v1(jsonb_build_object(
    'actor_user_id',request->>'actor_user_id','source_group_id','e1000000-0000-4000-8000-000000000006','limit',1));
  perform pg_temp.assert_true((scopes->>'total_count')::integer=2,'all clients appear exactly once, not duplicated by unscoped calendar cycle');
  perform pg_temp.assert_true((scopes->>'has_more')::boolean,'scope enumeration does not silently cap its result');
  scope_page:=public.weekly_source_workspace_scopes_v1(jsonb_build_object(
    'actor_user_id',request->>'actor_user_id','source_group_id','e1000000-0000-4000-8000-000000000006',
    'limit',1,'cursor',scopes->>'next_cursor'));
  perform pg_temp.assert_true(scope_page#>>'{rows,0,client_id}'<>scopes#>>'{rows,0,client_id}','next page returns the other client');
  perform pg_temp.assert_true(not (scope_page->>'has_more')::boolean,'scope enumeration reaches EOF');
  perform pg_temp.assert_true((scopes->>'reports_awaiting_finalisation')::integer=0,'no imported report is not a prepared finalisation');
  perform pg_temp.assert_true(jsonb_array_length(scopes->'this_week_cutoffs')=1,'current cutoff is separately available');
  scope_page:=public.weekly_source_combined_finalise_workspace_v1(jsonb_build_object(
    'actor_user_id',request->>'actor_user_id','source_group_id','e1000000-0000-4000-8000-000000000006'));
  perform pg_temp.assert_true(scope_page#>>'{counts,ready}'='0' and jsonb_array_length(scope_page->'scopes')=0,
    'no finalisation file means nothing enters combined finalisation');
  begin
    perform public.weekly_source_workspace_scopes_v1(jsonb_build_object(
      'actor_user_id',request->>'actor_user_id','client_id',request->>'client_id',
      'source_group_id','e1000000-0000-4000-8000-000000000006','cursor',scopes->>'next_cursor'));
    raise exception 'cross-filter cursor accepted';
  exception when sqlstate '40001' then null; end;
  perform pg_temp.assert_true(not has_function_privilege('authenticated','public.weekly_source_workspace_scopes_v1(jsonb)','EXECUTE'),'scope reader browser denied');
  insert into public.weekly_work_events(candidate_id,client_id,work_date,identity_kind,durable_identity_hash,first_source_group_id)
    values('e1000000-0000-4000-8000-000000000004','e1000000-0000-4000-8000-000000000003','2026-09-07',
      'OFFICE_PROTECTED_SHIFT',decode(repeat('42',32),'hex'),'e1000000-0000-4000-8000-000000000006');
  context:=public.weekly_source_protected_editor_context_v1(request);
  perform pg_temp.assert_true(jsonb_array_length(context->'events')=1,'unpublished protected identity is visible');
  perform pg_temp.assert_true(context->>'work_event_id' is null,'no arbitrary automatic identity choice');
  context:=public.weekly_source_protected_editor_context_v1(request||jsonb_build_object('work_event_id',context#>>'{events,0,work_event_id}'));
  perform pg_temp.assert_true(context->>'work_event_id' is not null,'explicit identity selected');
  perform pg_temp.assert_true(context->>'evidence_timesheet_id' is null and (context->>'signed_evidence_available')::boolean=false,'unsigned identity never invents signed evidence');
  begin
    perform public.weekly_exceptional_pay_prepare_family_v1(request||jsonb_build_object(
      'source_cycle_id',prepared->>'source_cycle_id','contract_id','e1000000-0000-4000-8000-000000000005',
      'week_ending_date','2026-09-13','start_at_local','2026-09-07 09:00:00','end_at_local','2026-09-07 16:00:00',
      'break_minutes',15,'reason','duplicate must be refused','idempotency_key','protected-editor-duplicate-0001'));
    raise exception 'duplicate identity accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_PROTECTED_EXISTING_SHIFT_SELECTION_REQUIRED' then raise; end if;
  end;
  begin
    perform public.weekly_source_protected_editor_context_v1(request||jsonb_build_object('actor_user_id','e1000000-0000-4000-8000-000000000002'));
    raise exception 'non-authoriser accepted';
  exception when insufficient_privilege then null; end;
  begin
    perform public.weekly_source_protected_editor_context_v1(request||jsonb_build_object('pay',100));
    raise exception 'money accepted';
  exception when invalid_parameter_value then null; end;
  perform pg_temp.assert_true(not has_function_privilege('authenticated','public.weekly_source_protected_editor_prepare_v1(jsonb)','EXECUTE'),'browser denied');
end;
$proof$;
rollback;
`;
  const result = spawnSync(process.env.CLOUDTMS_TEST_PSQL || 'psql', ['-X','-h','127.0.0.1','-p',port,
    '-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  { input: sql, encoding: 'utf8', env: { ...process.env, PGOPTIONS: '-c jit=off' } });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
});
