import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),lane=path.resolve(here,'../../../..');
const root=path.resolve(lane,'../healthroster-v2-backend-13102026');
const read=file=>fs.readFileSync(path.join(here,file),'utf8');
const trace=JSON.parse(read('EXACT_REHEARSAL_TRACE.json')),hunks=JSON.parse(read('FORWARD_HUNKS.json'));
function reverse(text,file){for(const h of hunks.filter(h=>h.file===file).reverse()){assert.equal(text.split(h.after).length,2);text=text.replace(h.after,()=>h.before);}return text;}
function activeMeta(sql,exists=false){let active=true;const stack=[];let emitted='';
  for(const line of sql.split('\n')){const text=line.trim();
    if(text==='\\if :{?weekly_source_verification_outer_transaction}'){stack.push({parent:active,condition:exists});active=active&&exists;continue;}
    if(text==='\\else'){const frame=stack.at(-1);if(frame)active=frame.parent&&!frame.condition;continue;}
    if(text==='\\endif'){const frame=stack.pop();if(frame)active=frame.parent;continue;}
    if(!active)continue;
    if(/^\\set weekly_source_verification_outer_transaction\b/.test(text))exists=true;
    if(text==='\\unset weekly_source_verification_outer_transaction')exists=false;
    emitted+=line+'\n';
  }return {exists,emitted};}
test('exact interrupted547 receipt and every current controlling input remain byte pinned',()=>{
  assert.equal(trace.commit,'547c74da5f0a05b09297c042bc7f3cc0c0105b85');assert.equal(trace.affected_errors.length,18);
  for(const p of JSON.parse(read('CONTROLLING_INPUTS.json')).inputs){const bytes=fs.readFileSync(path.join(root,p.path));assert.equal(bytes.length,p.bytes,p.path);assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),p.sha256,p.path);}
  assert.ok(trace.original_fixture_commit_pins.every(p=>p.matches_current_disk));
  for(const error of JSON.parse(read('ERROR_CLASSIFICATION.json')).errors)
    assert.ok(error.depends_on_statements.every(index=>index<error.statement),'a cascade cannot depend on a later statement');
});
test('only two actually included prior units set the leaked variable; no prior unset exists',()=>{
  const prior=trace.psql_variable_trace.slice(0,trace.psql_variable_trace.findIndex(s=>s.file.includes('17092026_0100')));
  assert.deepEqual(prior.filter(s=>s.meta.startsWith('\\set ')).map(s=>s.file),[
    'supabase/verification/15092026_1534_weekly_source_invoice_batch_integration_v1.sql',
    'supabase/verification/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql']);
  assert.equal(prior.some(s=>s.meta.startsWith('\\unset ')),false);
  const target=trace.psql_variable_trace.find(s=>s.file.includes('17092026_0100'));assert.equal(target.exists_before,true);
});
test('actual rendered target skips fixture capture under leaked outer mode, standalone conditionals remain intact',()=>{
  const plan=JSON.parse(fs.readFileSync(path.join(root,'docs/healthroster-build/rehearsals/13102026/FULL_REHEARSAL_EXACT_SOURCE_PLAN.json'),'utf8'));
  const sql=plan.verifierUnits.find(x=>x.file.includes('17092026_0100')).sql;
  assert.equal(activeMeta(sql,true).emitted.includes('create temporary table if not exists ws_verify_keys'),false);
  assert.equal(activeMeta(sql,false).emitted.includes('create temporary table if not exists ws_verify_keys'),true);
  assert.equal(activeMeta('\\unset weekly_source_verification_outer_transaction\n'+sql,true).emitted.includes('create temporary table if not exists ws_verify_keys'),true);
  assert.equal(read('01_unchanged_entitlement_schema.sqlinc'),fs.readFileSync(path.join(root,'supabase/verification/17092026_0100_weekly_source_entitlement_schema_v1.sql'),'utf8'));
});
test('full guarded runner changes only per-unit psql flag scope and retains archival no-execution gate',()=>{
  const file='02_archived_runner_variable_scope_successor.mjs',text=read(file);
  assert.equal(reverse(text,file),fs.readFileSync(path.join(root,'docs/healthroster-build/rehearsals/13102026/run_full_upgrade_rehearsal.mjs'),'utf8'));
  assert.match(text,/throw new Error\('ARCHIVED_REHEARSAL_PAUSED_REBASE_AND_SEND7_TURN_REQUIRED'\)/);
  assert.equal((text.match(/\\\\unset weekly_source_verification_outer_transaction/g)??[]).length,2);
  assert.ok(text.includes("append(u.sql,u)"));assert.ok(text.includes("'rollback to savepoint hr_verifier_'")||text.includes("append('rollback to savepoint hr_verifier_"));
});
test('all complete fixture successors reverse exactly to current historical files',()=>{
  for(const [file,input] of [['03_complete_first_authorisation_fixture.sqlinc','17092026_0600_weekly_source_first_authorisation_v1.sql'],
    ['04_complete_withdrawal_fixture.sqlinc','17092026_0610_weekly_source_withdrawal_supersession_v1.sql'],
    ['05_complete_workbench_fixture.sqlinc','17092026_0800_weekly_source_workbench_seams_v1.sql']])
    assert.equal(reverse(read(file),file),fs.readFileSync(path.join(root,'supabase/verification',input),'utf8'));
});
test('D8 receipt matches the existing exact bundle/head/digest/token/root/version without manager or transport evidence',()=>{
  const text=read('03_complete_first_authorisation_fixture.sqlinc'),start=text.indexOf('-- Named minimum schema receipt'),end=text.indexOf('  -- A withdrawn generation',start),block=text.slice(start,end);
  assert.equal((block.match(/weekly_source_entitlement_publication_receipts/g)??[]).length,1);
  for(const key of ['member_root_ids','member_family_booking_ids','member_root_versions','head_ids','scope_change_tx_token','decision_bundle_id','bundle_revision'])assert.ok(block.includes(key));
  for(const id of ['000000000fb1','000000000fc1','000000000fe1','000000000306'])assert.ok(block.includes(id));
  assert.ok(block.includes("array[2]::integer[]"));assert.equal((block.match(/convert_to\('receipt','UTF8'\)/g)??[]).length,2);
  assert.doesNotMatch(block,/manager_approved|signature_manifest|mail_outbox|next_revision|pay_run|receipt_state|paid_at/);
});
test('named construction checks still force complete inventory/receipt and restore their own schema timing',()=>{
  for(const file of ['04_complete_withdrawal_fixture.sqlinc','05_complete_workbench_fixture.sqlinc']){
    const changed=hunks.find(h=>h.file===file);assert.ok(changed.after.includes('public.weekly_source_entitlement_head_inventory_assert'));
    assert.ok(changed.after.includes('public.weekly_source_entitlement_head_component_inventory_assert'));
    assert.ok(changed.after.includes('public.weekly_source_entitlement_head_receipt_assert'));
    assert.ok(changed.after.indexOf(' deferred;')<changed.after.indexOf('insert into public.weekly_source_entitlement_decision_bundles'));
    assert.ok(changed.after.lastIndexOf(' immediate;')>changed.after.indexOf('weekly_source_entitlement_head_components'));
    assert.ok(changed.after.lastIndexOf(' deferred;')>changed.after.lastIndexOf(' immediate;'));
    assert.doesNotMatch(changed.after,/set constraints all|disable trigger|drop constraint/i);
  }
});
test('ordering proof retains top-level equality and every written-row fence, binding marks/audit to actual caller frame',()=>{
  const text=read('03_complete_first_authorisation_fixture.sqlinc'),start=text.indexOf('do $verify_ordering_rule$'),end=text.indexOf('$verify_ordering_rule$;',start),block=text.slice(start,end);
  assert.match(block,/returning xmin::text into v_owner_frame_xid/);
  assert.ok(block.includes('v_xid_after=v_xid_before'));assert.ok(block.includes('v_xmin_authorisation=v_xmin_audit'));
  assert.ok(block.includes('and v_xmin_authorisation=v_owner_frame_xid'));
  for(const key of ['timesheet','financials','week','authorisation','audit'])assert.ok(block.includes(`v_xmin_${key}::bigint>=v_xid_before`),key);
  assert.doesNotMatch(block,/and v_xmin_authorisation::bigint=v_xid_before/);
});
