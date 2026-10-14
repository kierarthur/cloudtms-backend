import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { hash, lexical, calls, closureCalls, assertRoot, here, argumentTypes } from './read-source.mjs';
const lane=path.resolve(here,'../../../..');
const root=path.resolve(lane,'../healthroster-v2-backend-13102026');
const inputs=JSON.parse(fs.readFileSync(path.join(here,'CONTROLLING_INPUTS.json')));
const receipt=JSON.parse(fs.readFileSync(path.join(here,'INTEGRATION_RECEIPT_PROPOSAL.json')));
const safety=JSON.parse(fs.readFileSync(path.join(here,'VERIFIER_SAFETY_REREVIEW.json')));
const census=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_CENSUS.json')));
const read=(p,r=here)=>fs.readFileSync(path.join(r,p),'utf8').replaceAll('\r\n','\n');
const official=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const additions=receipt.privateSignatures;
const addValues=additions.map(s=>`    ('${s}'),`).join('\n')+'\n';

test('every current controlling input and old safety review remains exact',()=>{
 assertRoot(root,inputs.sourceCommit);
 assert.equal(inputs.sourceCommit,'b9e2cbfcdf061a4e8fe07044e658c2daf043a362');
 for(const f of [...inputs.sourceInputs,...safety.oldReviewInputs])
  assert.equal(hash(fs.readFileSync(path.join(root,f.path))),f.sha256,f.path);
 assert.equal(census.nativePinComparison.length,20);
 assert.ok(census.nativePinComparison.every(p=>p.unchanged));
});
test('all four frozen packets and each artifact remain untouched',()=>{
 for(const f of inputs.frozenPackets){
  assert.equal(hash(fs.readFileSync(path.join(lane,f.path))),f.sha256);
  const m=JSON.parse(read(f.path,lane));
  for(const a of m.files??m.artifacts){
   const p=a.path.startsWith('docs/')||a.path.startsWith('tests/')?a.path:path.posix.join(path.posix.dirname(f.path),a.path);
   assert.equal(hash(fs.readFileSync(path.join(lane,p))),a.sha256,p);
  }
 }
});
test('both complete central ACL successors reverse to every controlling native SQL byte',()=>{
 for(const [p,i] of [['12_complete_private_registry_union.sqlinc',1],['13_complete_final_acl_union.sqlinc',0]]){
  const next=read(p),before=official.exactAtomicBody(read(receipt.centralACL.guardedInputs[i].path,root)).trimEnd()+'\n';
  assert.equal(next.split(addValues).length,2);
  assert.equal(next.replace(addValues,''),before);
  assert.equal(official.scanManagedSql(next).meta.length,0);
  assert.ok(!official.scanManagedSql(next).statements.some(s=>['BEGIN','COMMIT','ROLLBACK'].includes(s.tokens[0])));
 }
});
test('independent full verifier changes only eleven private expected signatures',()=>{
 const next=read('14_complete_acl_verifier_union.sqlinc');
 const addition=additions.map(s=>`    '${s}',`).join('\n')+'\n';
 assert.equal(next.split(addition).length,2);
 assert.equal(next.replace(addition,''),read(receipt.centralACL.verifierInput.path,root));
 assert.equal(new Set(additions).size,11);
 assert.deepEqual(receipt.counts,{baselineTables:105,baselinePrivate:221,baselineService:142,
  automaticOnlyTables:105,automaticOnlyPrivate:232,automaticOnlyService:142});
});
test('final private ACL revokes all eleven from every caller and never grants them',()=>{
 const sql=read('15_final_automatic_private_acl.sqlinc');
 for(const s of additions){assert.ok(sql.includes(`alter function ${s} owner to postgres;`));
  assert.ok(sql.includes(`revoke all on function ${s} from public,anon,authenticated,service_role;`));}
 assert.ok(!/\bgrant\b/i.test(lexical(sql).code));
 const registry=read('12_complete_private_registry_union.sqlinc');
 const body=registry.match(/create or replace function private\._weekly_source_acl_private_helper_contract_v1\([\s\S]*?\$function\$;/)[0];
 const sigs=[...body.matchAll(/'((?:private|public)\.[^']+\([^']*\))'/g)].map(m=>m[1]);
 assert.equal(sigs.length,232);assert.equal(new Set(sigs).size,232);
});
test('single proposed release expands to one transaction through the official packaging scanner',()=>{
 const source=read('16_proposed_root.sqlinc');
 const frozen=path.join(lane,'docs/healthroster-build/proposals/automatic-native-delta');
 const forward=path.join(lane,'docs/healthroster-build/proposals/automatic-native-delta-forward-review');
 const expanded=source.replace(/^\\ir\s+[^\n]*\/([^\/\n]+)$/gm,(_,filename)=>{
  const r=filename==='08_complete_automatic_reconcile_delta_forward.sqlinc'?forward:
   /^(?:12|13|15)_/.test(filename)?here:frozen;
  return read(filename,r);
 });
 assert.ok(!expanded.includes('\\ir'));
 assert.doesNotThrow(()=>official.exactAtomicBody(expanded));
 assert.equal(official.scanManagedSql(expanded).statements.filter(s=>s.tokens[0]==='BEGIN').length,1);
 assert.equal(official.scanManagedSql(expanded).statements.filter(s=>s.tokens[0]==='COMMIT').length,1);
 assert.ok(!source.includes('10_complete_stage_request')&&!source.includes('11_complete_automatic_reconcile'));
});
test('actual repaired Report/M1 Workers, FIN overloads and A0 writers remain independently pinned',()=>{
 const p=inputs.preservedFacts.map(f=>f.path);
 for(const name of ['13102026_2110_healthroster_report_native_fin_worker_v1.sql',
  '13102026_1544_weekly_source_self_bill_report_scope_v1.sql','13102026_1561_weekly_source_magnit_contract_price_v1.sql',
  'upload-staging-adapter.mjs','upload-publication-owner.mjs','report-upload-provenance.mjs','13102026_1127_approval.sqlinc'])
  assert.ok(p.some(s=>s.endsWith(name)),name);
 const owners=inputs.keyOwners.map(o=>o.name);
 assert.ok(owners.includes('private.timesheet_correction_native_original_reversal_v1'));
 assert.ok(owners.includes('public.tsfin_write_current_snapshot_single_bounded'));
 assert.ok(owners.includes('public.tsfin_write_snapshots_and_complete'));
 assert.equal(receipt.ReportM1.FINDispatcherChanged,false);assert.equal(receipt.ReportM1.ABQualified,false);
});
test('missing capability remains a strict current-root gate, never a ready value',()=>{
 for(const name of ['private.weekly_source_bulk_source_cas_v1','private.weekly_source_bulk_producer_ready_v1',
  'private.weekly_source_automatic_reconcile_producer_ready_v1']){
  const owner=census.nativeFinalOwners.find(o=>o.name===name);assert.ok(owner);assert.deepEqual(owner.owners,[]);
 }
 const source27=read('supabase/repeatable/13102026_1438_weekly_source_protection_queries/27_automatic_reconcile.sqlinc',root);
 assert.ok(source27.includes("v_ready is distinct from true"));
 assert.ok(source27.includes("execute 'select private.weekly_source_automatic_reconcile_producer_ready_v1($1)'"));
 assert.deepEqual(receipt.newPublicSignatures,[]);assert.deepEqual(receipt.newServiceSignatures,[]);
});
test('safety preserves all old exclusions and treats null Report only as missing-authority proof',()=>{
 assert.deepEqual(safety.oldCountsPreserved,{verifiers:168,eligible:152,excluded:16});
 assert.equal(safety.oldExclusionsChanged,false);assert.equal(safety.verifiers.length,4);
 assert.ok(safety.verifiers.every(v=>!v.executed&&!v.requiresGenuineApplyingRelease&&!v.containsActualQueueProcessorCall));
 const sql=read(safety.verifiers[3].path,root);
 assert.ok(sql.includes('pay_authority_v1(null)'));assert.ok(sql.includes('pay_witness_v1(null)'));
 assert.ok(!calls(sql).direct.includes('private.weekly_source_finalise_engine_v1'));
 const authority=read('supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/02_pay_authority.sqlinc',root);
 assert.ok(authority.indexOf("if not found or b.state is distinct from 'CURRENT' then return null; end if;")
  <authority.indexOf('if not exists(select 1 from public.weekly_source_upload_rows'));
});
test('fixture drain keys and source corruption teardown remain bounded, with no queue processor',()=>{
 const first=read(safety.verifiers[0].path,root),withdraw=read(safety.verifiers[1].path,root),seams=read(safety.verifiers[2].path,root);
 assert.ok(first.includes("where rel='public.banking_pay_workbench_jobs'::regclass"));
 assert.ok(withdraw.includes("pg_catalog.strpos(dedupe_key,'c7000000-0000-4000-8000-')>0"));
 assert.ok(seams.includes('delete from public.weekly_source_entitlement_heads where root_timesheet_id=v_ts;'));
 assert.ok(seams.includes("con.conrelid='public.weekly_source_entitlement_heads'::regclass"));
 for(const v of safety.verifiers.slice(0,3)){
  assert.ok(v.fixturePrefix);assert.ok(!/\bset\s+constraints\s+all\b/i.test(lexical(read(v.path,root)).code));
  const graph=census.verifiers.find(c=>c.path===v.path).conservativeCalleeClosure;
  assert.ok(!graph.owners.some(o=>/dirty_apply_jobs_chunk|outbox_claim_ready|dispatch_(?:commands|targets)_claim|pending_entitlement_release_claim/.test(o.name)));
 }
});
test('safety scanner distinguishes inspected routine text from executed dynamic network SQL and follows native triggers',()=>{
 assert.deepEqual(calls("select pg_get_functiondef('public.payment_execute(uuid)'::regprocedure);").direct,[]);
 assert.deepEqual(calls("execute 'select net.http_post($1)' using p; ").dynamicCalls,['net.http_post']);
 const inv={relations:new Set(['public.fixture']),functions:new Map([['private.guard/0',
  {key:'private.guard/0',name:'private.guard',body:'perform net.http_post(1);',...calls('perform net.http_post(1);')}]]),
  triggers:new Map([['t',{name:'t',relation:'public.fixture',callee:'private.guard'}]])};
 const graph=closureCalls(inv,[],['public.fixture']);
 assert.ok(graph.unresolved.includes('net.http_post'));assert.ok(graph.edges.some(e=>e.kind==='DML_TRIGGER_CONSERVATIVE'));
});
test('native overload inventory retains typed same-arity signatures and nested defaults',()=>{
 assert.deepEqual(argumentTypes('p_id uuid, p_fields jsonb default jsonb_build_object(\'x\',1)'),['uuid','jsonb']);
 assert.deepEqual(argumentTypes('p_id text, p_fields jsonb'),['text','jsonb']);
 assert.deepEqual(argumentTypes('uuid, timestamp without time zone, OUT result jsonb'),['uuid','timestamp without time zone']);
 assert.equal(census.nativeFinalOwners.find(o=>o.name==='private.weekly_source_finalise_engine_v1').owners.length,2);
});
test('native I1 tuple and independently retained stable key are preserved rather than C1 transport identity',()=>{
 const tuple=receipt.nativeI1Identity.tuple;
 assert.deepEqual(tuple,['component_kind','economic_key_type','economic_key_value','component_member_identity']);
 const raw=read('supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',root);
 assert.ok(raw.includes("'WEEKLY_SOURCE_ENTITLEMENT_COMPONENT_V1'"));
 assert.ok(raw.includes("segment.value#>>'{weekly_source,calculation_fingerprint}' as segment_stable_key"));
 const next=read('../automatic-native-delta-forward-review/08_complete_automatic_reconcile_delta_forward.sqlinc');
 assert.ok(next.includes("'segment_stable_key'"));
 const producer=read('../automatic-native-delta/07_staged_entitlement_components.sqlinc');
 assert.ok(producer.includes('private.weekly_source_entitlement_components_v1(v_segments,v_expenses)'));
 assert.ok(producer.includes('private.weekly_source_initial_additional_components_v2('));
 assert.ok(!producer.includes('timesheet_correction_native_original_reversal_v1'));
});
