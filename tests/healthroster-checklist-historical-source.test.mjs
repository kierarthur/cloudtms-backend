import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {scanManagedSql,exactAtomicBody} from '../scripts/cloudtms-managed-release-sql.mjs';
const read=name=>fs.readFileSync(new URL('../supabase/repeatable/healthroster_wp_u3_source_proposals/'+name,import.meta.url),'utf8');
test('historical setup manifest is service-only bounded keyset output and delegates genuine submission without GET writes or calendar lookback',()=>{
  const source=read('wp_u3_historical_submission_manifest.sqlinc');
  assert.equal(scanManagedSql(exactAtomicBody(source)).statements.length,7);
  for(const body of source.split('$function$').filter((_,i)=>i%2===1)){
    const tokens=scanManagedSql(body).statements.flatMap(s=>s.tokens);
    for(const forbidden of ['INSERT','UPDATE','DELETE','TRUNCATE','LOCK','NOTIFY','EXECUTE','CALL'])assert.ok(!tokens.includes(forbidden),forbidden);
    assert.doesNotMatch(body,/pg_advisory|for\s+(?:update|share)|create\s+(?:temp|table)|interval\s+'\d+\s+(?:day|week|month)|generate_series|uuid_generate/i);
  }
  for(const proof of ['private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id)',
    'private.weekly_source_signed_submission_read_context_v1(p_timesheet_id,source.id)',
    "if witness->'actual_submitted' is distinct from 'true'::jsonb then return null",'SIGNED_SCOPE_AMBIGUOUS',
    "own_week>(calendar->>'latest_ended_week')::date",'WEEKLY_SOURCE_SUBMISSION_CYCLE_CLIENT_WEEK_UNPROVED',
    'to_jsonb(m)','to_jsonb(p)','to_jsonb(a)',"'source_group',to_jsonb(g)","'actor',to_jsonb(actor)",
    "token->>'read_revision' is distinct from revision", "r->>'timesheet_id'=token->>'after'",
    "order by (r->>'timesheet_id')::uuid limit page_limit+1",'page_limit not between 1 and 100',
    "actor.role::text is distinct from 'admin'",'grant execute on function public.weekly_source_checklist_submission_manifest_v1(jsonb) to service_role'])
    assert.ok(source.includes(proof),proof);
});
test('historical preparation rechecks exact genuine admission and witness around the one established per-Timesheet lifecycle owner',()=>{
  const source=read('wp_u3_historical_submission_prepare.sqlinc');
  assert.equal(scanManagedSql(exactAtomicBody(source)).statements.length,4);
  assert.equal((source.match(/result:=private\.weekly_source_checklist_submission_cycle_prepare_v1\(/g)||[]).length,1);
  assert.doesNotMatch(source,/\b(?:insert|update|delete|for\s+update|pg_advisory_xact_lock)\b|_daily_timesheet|candidate_workflow_transition|timesheet_authorise|bpay_/i);
  for(const hash of ['submission_witness_sha256','admission_sha256']){
    assert.ok(source.includes(`jsonb_typeof(p_request->'${hash}') is distinct from 'string'`));
    assert.ok(source.includes(`descriptor->>'${hash}' is distinct from expected_`));
    assert.ok(source.includes(`after_descriptor->>'${hash}' is distinct from expected_`));
  }
  assert.ok(source.indexOf('descriptor:=')<source.indexOf('result:=private.'));
  assert.ok(source.indexOf('after_descriptor:=')>source.indexOf('result:=private.'));
  assert.ok(source.includes('to_jsonb(actor) is distinct from expected_actor'));
  const owner=read('wp_u3_submission_cycle_prepare.sqlinc');
  assert.ok(owner.indexOf('weekly_source_pay_query_admit_v2')<owner.indexOf('weekly_source_lock_family_rows_v1'));
  assert.ok(owner.indexOf('weekly_source_lock_family_rows_v1')<owner.indexOf('pg_advisory_xact_lock'));
  assert.ok(owner.includes('on conflict on constraint weekly_source_cycles_group_week_client_uq do nothing'));
  assert.doesNotMatch(owner,/insert into public\.weekly_source_cycles\([^)]*created_at/);
});
test('historical service RPCs are absent from actual Checklist/read callers and incomplete caller/native gates stay explicit',()=>{
  for(const file of ['wp_u3_native_checklist.sqlinc','wp_u3_client_week_context.sqlinc','wp_u3_native_page.sqlinc'])
    assert.doesNotMatch(read(file),/weekly_source_checklist_submission_manifest_v1|weekly_source_checklist_historical_prepare_atomic_v1/);
  const document=fs.readFileSync(new URL('../docs/healthroster-build/proposals/checklist-native-20261014/HISTORICAL_PREPARATION_WIP.md',import.meta.url),'utf8');
  for(const gate of ['CALLER_NOT_BUILT','NATIVE_SQL_NOT_EXECUTED','FEATURE_ON_HELD','I1/F1','separate top-level RPC transactions'])assert.ok(document.includes(gate),gate);
});
