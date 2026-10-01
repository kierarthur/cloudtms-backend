import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

function runProof(body) {
  const fixture=readFileSync('supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql','utf8');
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL || 'psql',['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    {input:fixture.slice(0,fixture.indexOf('do $test$'))+body+'\nrollback;',encoding:'utf8',env:{...process.env,PGOPTIONS:'-c jit=off'}});
  assert.equal(result.status,0,result.stderr || result.error?.message);
}

test('incremental query owner retains the final per-target transport closure', () => {
  const source=readFileSync('supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql','utf8');
  assert(source.lastIndexOf('\\ir 15092026_2311_weekly_source_delivery_targets_v1.sql')>
    source.lastIndexOf('create or replace function public.weekly_source_message_dispatch_submission_start_atomic_v1'));
});

test('missing timesheet starts automatically and equivalent new upload preserves request and clocks', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => runProof(`
insert into public.weekly_work_events
select (jsonb_populate_record(null::public.weekly_work_events,to_jsonb(event)||jsonb_build_object(
  'id','e9000000-0000-4000-8000-000000000003','candidate_id','e3000000-0000-4000-8000-000000000002',
  'profile_external_key','missing-shift','durable_identity_hash','\\x'||repeat('81',32)))).*
from public.weekly_work_events event where event.id='e9000000-0000-4000-8000-000000000001';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,source_client_identity,
  work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,
  normalised_row_hash,bounded_raw_columns_json
) values ('ec000000-0000-4000-8000-000000000001','e7000000-0000-4000-8000-000000000001',1,
  'missing-shift','Robin Nurse','North Test Trust','2026-09-01','2026-09-01 09:00','2026-09-01 17:00',30,450,
  'SOURCE_WORKED',decode(repeat('82',32),'hex'),'{}');
insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,paid_minutes,
  rate_classifications_json,mapping_state,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,qualifying_contract_count,
  qualifying_contract_set_hash,source_row_fingerprint,contract_and_rate_fingerprint,effective_policy_fingerprint
) values ('ed000000-0000-4000-8000-000000000001','ec000000-0000-4000-8000-000000000001',1,
  'e3000000-0000-4000-8000-000000000002','e2000000-0000-4000-8000-000000000001',
  'e4000000-0000-4000-8000-000000000002','e9000000-0000-4000-8000-000000000003',450,'{}','RESOLVED',
  'AUTO_UNIQUE','REUSED_PROFILE_KEY',decode(repeat('83',32),'hex'),decode(repeat('84',32),'hex'),1,
  decode(repeat('85',32),'hex'),decode(repeat('86',32),'hex'),decode(repeat('87',32),'hex'),decode(repeat('88',32),'hex'));
do $proof$
declare original_request public.weekly_timesheet_submission_requests%rowtype; initial_count bigint;
begin
  update public.weekly_source_client_policies set candidate_queries_enabled=false
  where source_group_id='e5000000-0000-4000-8000-000000000001';
  perform private.weekly_source_import_outreach_v1('e8000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(not exists(select 1 from public.weekly_timesheet_submission_requests
    where source_cycle_id='e6000000-0000-4000-8000-000000000001'),'disabled candidate setting prevents automatic request');
  update public.weekly_source_client_policies set candidate_queries_enabled=true
  where source_group_id='e5000000-0000-4000-8000-000000000001';
  perform private.weekly_source_import_outreach_v1('e8000000-0000-4000-8000-000000000001');
  select request.* into strict original_request from public.weekly_timesheet_submission_requests request
  where request.source_cycle_id='e6000000-0000-4000-8000-000000000001' and request.state='ACTIVE';
  select count(*) into initial_count from public.weekly_message_intents;
  perform pg_temp.assert_true(initial_count>0,'missing timesheet automatically creates initial intent');
  update public.weekly_source_uploads set state='SUPERSEDED' where id='e7000000-0000-4000-8000-000000000001';
  insert into public.weekly_source_uploads
  select (jsonb_populate_record(null::public.weekly_source_uploads,to_jsonb(upload)||jsonb_build_object(
    'id','e7000000-0000-4000-8000-000000000002','state','CURRENT','content_sha256','\\x'||repeat('91',32)))).*
  from public.weekly_source_uploads upload where upload.id='e7000000-0000-4000-8000-000000000001';
  update public.weekly_source_projection_publications set state='STALE' where id='e8000000-0000-4000-8000-000000000001';
  insert into public.weekly_source_projection_publications
  select (jsonb_populate_record(null::public.weekly_source_projection_publications,to_jsonb(publication)||jsonb_build_object(
    'id','e8000000-0000-4000-8000-000000000002','upload_id','e7000000-0000-4000-8000-000000000002','state','CURRENT'))).*
  from public.weekly_source_projection_publications publication where publication.id='e8000000-0000-4000-8000-000000000001';
  insert into public.weekly_source_upload_rows
  select (jsonb_populate_record(null::public.weekly_source_upload_rows,to_jsonb(row)||jsonb_build_object(
    'id','ec000000-0000-4000-8000-000000000002','upload_id','e7000000-0000-4000-8000-000000000002'))).*
  from public.weekly_source_upload_rows row where row.id='ec000000-0000-4000-8000-000000000001';
  insert into public.weekly_source_row_resolutions
  select (jsonb_populate_record(null::public.weekly_source_row_resolutions,to_jsonb(resolution)||jsonb_build_object(
    'id','ed000000-0000-4000-8000-000000000002','upload_row_id','ec000000-0000-4000-8000-000000000002'))).*
  from public.weekly_source_row_resolutions resolution where resolution.id='ed000000-0000-4000-8000-000000000001';
  update public.weekly_source_cycles set current_complete_upload_id='e7000000-0000-4000-8000-000000000002',
    current_projection_publication_id='e8000000-0000-4000-8000-000000000002'
  where id='e6000000-0000-4000-8000-000000000001';
  perform private.weekly_source_import_outreach_v1('e8000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_true((select count(*)=initial_count from public.weekly_message_intents),
    'new upload, publication and resolution IDs with same hours send nothing new');
  perform pg_temp.assert_true(exists(select 1 from public.weekly_timesheet_submission_requests request
    where request.id=original_request.id and request.state='ACTIVE'
      and request.deadline_at_utc=original_request.deadline_at_utc
      and request.membership_hash=original_request.membership_hash),'original request, immutable membership and clocks survive');
  update public.weekly_source_uploads set state='SUPERSEDED' where id='e7000000-0000-4000-8000-000000000002';
  insert into public.weekly_source_uploads
  select (jsonb_populate_record(null::public.weekly_source_uploads,to_jsonb(upload)||jsonb_build_object(
    'id','e7000000-0000-4000-8000-000000000003','state','CURRENT','content_sha256','\\x'||repeat('92',32)))).*
  from public.weekly_source_uploads upload where upload.id='e7000000-0000-4000-8000-000000000002';
  update public.weekly_source_projection_publications set state='STALE' where id='e8000000-0000-4000-8000-000000000002';
  insert into public.weekly_source_projection_publications
  select (jsonb_populate_record(null::public.weekly_source_projection_publications,to_jsonb(publication)||jsonb_build_object(
    'id','e8000000-0000-4000-8000-000000000003','upload_id','e7000000-0000-4000-8000-000000000003','state','CURRENT'))).*
  from public.weekly_source_projection_publications publication where publication.id='e8000000-0000-4000-8000-000000000002';
  insert into public.weekly_source_upload_rows
  select (jsonb_populate_record(null::public.weekly_source_upload_rows,to_jsonb(row)||jsonb_build_object(
    'id','ec000000-0000-4000-8000-000000000003','upload_id','e7000000-0000-4000-8000-000000000003',
    'break_minutes',45,'actual_net_minutes',435))).*
  from public.weekly_source_upload_rows row where row.id='ec000000-0000-4000-8000-000000000002';
  insert into public.weekly_source_row_resolutions
  select (jsonb_populate_record(null::public.weekly_source_row_resolutions,to_jsonb(resolution)||jsonb_build_object(
    'id','ed000000-0000-4000-8000-000000000003','upload_row_id','ec000000-0000-4000-8000-000000000003'))).*
  from public.weekly_source_row_resolutions resolution where resolution.id='ed000000-0000-4000-8000-000000000002';
  update public.weekly_source_cycles set current_complete_upload_id='e7000000-0000-4000-8000-000000000003',
    current_projection_publication_id='e8000000-0000-4000-8000-000000000003'
  where id='e6000000-0000-4000-8000-000000000001';
  perform private.weekly_source_import_outreach_v1('e8000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_true(exists(select 1 from public.weekly_timesheet_submission_requests
    where id=original_request.id and state='SUPERSEDED'),'material break change supersedes the old request');
  perform pg_temp.assert_true((select count(*)=initial_count+1 from public.weekly_message_intents),
    'material break change creates exactly one new initial intent');
  perform private.weekly_source_import_outreach_v1('e8000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_true((select count(*)=initial_count+1 from public.weekly_message_intents),
    'rechecking changed facts again does not send twice');
  perform pg_temp.assert_true(not has_function_privilege('service_role',
    'private.weekly_source_submission_request_start_v1(jsonb,boolean)','EXECUTE'),
    'automatic request owner is not a new caller-controlled privilege bypass');
end;
$proof$;
`));

test('accepted source comparison starts candidate contact without Office ASK and replay sends nothing', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const fixture = readFileSync('supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql', 'utf8');
  const sql = fixture.slice(0, fixture.indexOf('do $test$')) + `
do $proof$
declare
  request jsonb:=jsonb_build_object('actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000001');
  issue jsonb:=jsonb_build_object('work_event_id','e9000000-0000-4000-8000-000000000001',
    'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
    'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('31',32),
    'contract_id','e4000000-0000-4000-8000-000000000001','issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
    'candidate_start_at_local','2026-09-01 09:00','candidate_end_at_local','2026-09-01 19:00','candidate_break_minutes',30,
    'system_start_at_local','2026-09-01 09:00','system_end_at_local','2026-09-01 17:00','system_break_minutes',30);
  initial_count bigint; generation uuid; deadline timestamptz; result jsonb;
begin
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(issue)));
  select id,deadline_at_utc into strict generation,deadline
  from public.weekly_candidate_outreach_generations
  where source_cycle_id=(request->>'source_cycle_id')::uuid and request_kind='CHECK_HOURS' and state='ACTIVE';
  perform pg_temp.assert_true(exists(select 1 from public.weekly_message_intents
    where candidate_generation_id=generation and tranche_kind='CANDIDATE_INITIAL'),'first mismatch automatically creates initial candidate intent');
  perform pg_temp.assert_true(not exists(select 1 from public.weekly_message_intents
    where source_cycle_id=(request->>'source_cycle_id')::uuid and audience_kind='MANAGER'),
    'normal first contact must not email manager immediately');
  select count(*) into initial_count from public.weekly_message_intents;
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(issue)));
  perform private.weekly_source_import_outreach_v1((request->>'projection_publication_id')::uuid);
  perform pg_temp.assert_true((select count(*)=initial_count from public.weekly_message_intents),'unchanged replay adds no candidate or manager intent');
  perform pg_temp.assert_true(exists(select 1 from public.weekly_candidate_outreach_generations
    where id=generation and state='ACTIVE' and deadline_at_utc=deadline),'unchanged replay retains original generation and clock');
end;
$proof$;
rollback;`;
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL || 'psql', ['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  {input:sql,encoding:'utf8',env:{...process.env,PGOPTIONS:'-c jit=off'}});
  assert.equal(result.status,0,result.stderr || result.error?.message);
});

test('automatic mismatch contact preserves explicit manager direct and disabled candidate policy', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => runProof(`
do $proof$
declare
  request jsonb:=jsonb_build_object('actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000001');
  issue jsonb:=jsonb_build_object('work_event_id','e9000000-0000-4000-8000-000000000001',
    'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
    'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('31',32),
    'contract_id','e4000000-0000-4000-8000-000000000001','issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
    'candidate_start_at_local','2026-09-01 09:00','candidate_end_at_local','2026-09-01 19:00','candidate_break_minutes',30,
    'system_start_at_local','2026-09-01 09:00','system_end_at_local','2026-09-01 17:00','system_break_minutes',30);
begin
  update public.weekly_source_client_policies set candidate_queries_enabled=false
  where source_group_id='e5000000-0000-4000-8000-000000000001';
  perform public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(issue)));
  perform pg_temp.assert_true(not exists(select 1 from public.weekly_message_intents
    where source_cycle_id=(request->>'source_cycle_id')::uuid),'disabled candidate policy does not start contact');
  update public.weekly_source_client_policies set candidate_queries_enabled=true
  where source_group_id='e5000000-0000-4000-8000-000000000001';
  insert into public.weekly_route_activations(source_cycle_id,candidate_id,client_id,audience_route,
    route_mode,activated_by_user_id,activated_at_utc)
  select (request->>'source_cycle_id')::uuid,'e3000000-0000-4000-8000-000000000001',
    'e2000000-0000-4000-8000-000000000001',audience,'MANAGER_DIRECT',
    (request->>'actor_user_id')::uuid,transaction_timestamp()
  from unnest(array['CANDIDATE','MANAGER']) audience;
  perform private.weekly_source_import_outreach_v1((request->>'projection_publication_id')::uuid);
  perform pg_temp.assert_true(not exists(select 1 from public.weekly_message_intents
    where source_cycle_id=(request->>'source_cycle_id')::uuid and audience_kind='CANDIDATE'),
    'automatic import must not overwrite deliberate manager direct with candidate first');
  perform pg_temp.assert_true((select count(*)=2 from public.weekly_route_activations
    where source_cycle_id=(request->>'source_cycle_id')::uuid and route_mode='MANAGER_DIRECT'
      and activated_by_user_id=(request->>'actor_user_id')::uuid),'direct route and provenance remain unchanged');
end;
$proof$;
`));
