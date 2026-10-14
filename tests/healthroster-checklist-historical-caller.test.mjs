import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import os from 'node:os';import path from 'node:path';import {createHash} from 'node:crypto';
import {prepareHistoricalChecklistMetadata} from '../scripts/healthroster-prepare-historical-checklist.mjs';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,actor=id(1),A='a'.repeat(64),B='b'.repeat(64);
const manifestRpc='weekly_source_checklist_submission_manifest_v1',prepareRpc='weekly_source_checklist_historical_prepare_atomic_v1';
const copy=value=>structuredClone(value),sha=file=>createHash('sha256').update(fs.readFileSync(file)).digest('hex');
function fixture(n=10,legacy=false){return {timesheet_id:id(n),timesheet_version:1,source_group_id:id(2),client_id:id(3),week_ending:'2026-10-11',
  native_cycle_week_ending:'2026-10-11',source_cycle_id:null,submission_witness_sha256:A,admission_sha256:B,
  witness_identity:legacy?{authority:'VERIFIED_LEGACY_SOURCE_DOCUMENT',workflow_id:null,workflow_generation:null,hours_component_id:null,
    candidate_signature_component_id:null,document_version_id:id(n+100)}:{authority:'CANDIDATE_IMMUTABLE_HOURS',workflow_id:id(n+100),
    workflow_generation:2,hours_component_id:id(n+200),candidate_signature_component_id:id(n+300),document_version_id:null}};}
function page(rows,{missing=rows.length,eligible=missing,more=false,cursor=null,revision=A}={}){return {ok:true,contract:'WEEKLY_SOURCE_SUBMISSION_MANIFEST_V1',
  actor_user_id:actor,permission_view:'ADMIN',binding:B,read_revision:revision,eligible_count:eligible,missing_count:missing,rows:copy(rows),has_more:more,next_cursor:cursor};}
function receipt(row){return {ok:true,contract:'WEEKLY_SOURCE_HISTORICAL_SUBMISSION_PREPARE_V1',actor_user_id:actor,timesheet_id:row.timesheet_id,
  metadata_only:true,descriptor:{...copy(row),source_cycle_id:id(900)}};}
function context(t){
  const base=fs.mkdtempSync(path.join(os.tmpdir(),'healthroster-historical-source-'));
  t.after(()=>{
    const resolved=fs.realpathSync(base),parent=fs.realpathSync(os.tmpdir());
    assert.equal(path.dirname(resolved),parent);assert.ok(path.basename(resolved).startsWith('healthroster-historical-source-'));
    fs.rmSync(resolved,{recursive:true});
  });return {actorUserId:actor,evidenceDirectory:base,runId:id(800),directory:path.join(base,id(800))};
}
test('actual historical caller persists complete native-shaped witness manifest before separate sequential per-Timesheet calls and fresh verification',async t=>{
  const ctx=context(t),rows=[fixture(10),fixture(11,true),fixture(12)],calls=[];let read=0,concurrent=0;
  const rpc=async(name,request)=>{
    calls.push({name,request:copy(request)});assert.equal(++concurrent,1);
    try{
      if(name===manifestRpc){
        read++;if(read===1)return page(rows.slice(0,2),{missing:3,more:true,cursor:'native-cursor'});
        if(read===2){assert.equal(request.p_request.cursor,'native-cursor');return page(rows.slice(2),{missing:3});}
        return page([],{eligible:3});
      }
      assert.equal(name,prepareRpc);assert.equal(read,2);
      const saved=JSON.parse(fs.readFileSync(path.join(ctx.directory,'manifest.json'),'utf8'));
      assert.deepEqual(saved.rows,rows);const expected=rows.find(row=>row.timesheet_id===request.p_request.timesheet_id);
      assert.deepEqual(request,{p_request:{actor_user_id:actor,timesheet_id:expected.timesheet_id,submission_witness_sha256:A,admission_sha256:B}});
      await Promise.resolve();return receipt(expected);
    }finally{concurrent--;}
  };
  const result=await prepareHistoricalChecklistMetadata({...ctx,rpc});
  assert.equal(result.status,'METADATA_PREPARED');assert.equal(result.ok,true);assert.equal(result.qualification,false);assert.equal(result.confirmed_count,3);
  assert.deepEqual(calls.map(call=>call.name),[manifestRpc,manifestRpc,prepareRpc,prepareRpc,prepareRpc,manifestRpc]);
  assert.equal(result.manifest.sha256,sha(result.manifest.path));
  for(const row of result.receipts)assert.equal(row.evidence.sha256,sha(row.evidence.path));
  assert.equal(JSON.parse(fs.readFileSync(path.join(ctx.directory,'result.json'),'utf8')).remaining_count,0);
});
test('stale, foreign, coerced hash, duplicate/order/cursor, capacity and incomplete manifests refuse before ANY writer',async t=>{
  const variants=[
    pages=>{pages[1].read_revision=B;},pages=>{pages[1].actor_user_id=id(9);},pages=>{pages[1].binding=A;},
    pages=>{pages[1].rows[0].submission_witness_sha256=[A];},pages=>{pages[1].rows[0].admission_sha256=[B];},
    pages=>{pages[1].rows[0]=copy(pages[0].rows[0]);},pages=>{pages[1].has_more=true;pages[1].next_cursor='native-cursor';},
    pages=>{pages[1].rows=[];},pages=>{pages[1].rows[0].week_ending='2026-02-30';},
    pages=>{pages[0].missing_count=10001;pages[0].eligible_count=10001;},pages=>{pages[1].rows[0].source_cycle_id=id(900);},
    pages=>{pages[1].rows[0].native_cycle_week_ending='2026-10-12';}
  ];
  for(const mutate of variants){
    const ctx=context(t),pages=[page([fixture(10)],{missing:2,more:true,cursor:'native-cursor'}),page([fixture(11)],{missing:2})];mutate(pages);let read=0,write=0;
    const rpc=async name=>{if(name===manifestRpc)return pages[read++];write++;return null;};
    await assert.rejects(()=>prepareHistoricalChecklistMetadata({...ctx,rpc}));assert.equal(write,0);
    assert.equal(fs.existsSync(path.join(ctx.directory,'manifest.json')),false);
  }
});
test('later unknown transaction retains first confirmed receipt, stops without retry, and fresh replay never prepares already present cycle again',async t=>{
  const ctx=context(t),rows=[fixture(10),fixture(11),fixture(12)],attempts=[];
  const rpc=async(name,request)=>{
    if(name===manifestRpc)return page(rows);
    const ts=request.p_request.timesheet_id;attempts.push(ts);if(ts===rows[1].timesheet_id)throw Error('transport ended after possible commit');return receipt(rows[0]);
  };
  const result=await prepareHistoricalChecklistMetadata({...ctx,rpc});
  assert.equal(result.status,'STOPPED_UNCONFIRMED_TRANSACTION');assert.equal(result.confirmed_count,1);assert.equal(result.ok,false);
  assert.deepEqual(attempts,[rows[0].timesheet_id,rows[1].timesheet_id]);assert.equal(result.receipts[1].outcome,'UNCONFIRMED');
  assert.equal(JSON.parse(fs.readFileSync(path.join(ctx.directory,'result.json'),'utf8')).confirmed_count,1);
  let read=0;const replayAttempts=[];
  const replay=await prepareHistoricalChecklistMetadata({...ctx,runId:id(801),rpc:async(name,request)=>{
    if(name===manifestRpc)return ++read===1?page(rows.slice(2),{eligible:3}):page([],{eligible:3});
    replayAttempts.push(request.p_request.timesheet_id);return receipt(rows[2]);
  }});
  assert.equal(replay.ok,true);assert.deepEqual(replayAttempts,[rows[2].timesheet_id]);assert.equal(replay.qualification,false);
});
test('already present or vacant native census uses no metadata writer and retains native count with qualification false',async t=>{
  for(const eligible of [0,3]){
    const ctx=context(t),calls=[];const result=await prepareHistoricalChecklistMetadata({...ctx,rpc:async name=>{
      calls.push(name);return page([],{eligible});
    }});
    assert.equal(result.ok,true);assert.equal(result.eligible_count,eligible);assert.equal(result.qualification,false);assert.equal(result.confirmed_count,0);
    assert.deepEqual(calls,[manifestRpc,manifestRpc]);
  }
});
test('foreign/changed or array-hash preparation receipts stay UNCONFIRMED and cannot erase a possible native metadata commit',async t=>{
  for(const mutate of [r=>{r.actor_user_id=id(9);},r=>{r.timesheet_id=id(9);},r=>{r.metadata_only=false;},r=>{r.descriptor.client_id=id(9);},
    r=>{r.descriptor.submission_witness_sha256=[A];},r=>{r.descriptor.admission_sha256=[B];},r=>{r.descriptor.source_cycle_id=null;}]){
    const ctx=context(t),expected=fixture(),observed=receipt(expected);mutate(observed);let writes=0;
    const result=await prepareHistoricalChecklistMetadata({...ctx,rpc:async name=>name===manifestRpc?page([expected]):(writes++,observed)});
    assert.equal(result.status,'STOPPED_UNCONFIRMED_TRANSACTION');assert.equal(writes,1);assert.equal(result.confirmed_count,0);
    assert.equal(result.receipts[0].receipt,null);assert.equal(result.qualification,false);
  }
});
test('nonactual/planned/draft/expense/P0 shaped witness frames are not a replacement for the actual native admission owner',async t=>{
  for(const mutate of [r=>{r.witness_identity.authority='PLANNED';},r=>{r.witness_identity.authority='LOCAL_DRAFT';},
    r=>{r.witness_identity.authority='EXPENSE_ONLY';},r=>{r.witness_identity.authority='WAITING_P0_ONLY';},
    r=>{r.witness_identity.workflow_id=null;},r=>{r.witness_identity.workflow_generation=[2];},r=>{r.actual_submitted=false;}]){
    const ctx=context(t),expected=fixture();mutate(expected);let writes=0;
    await assert.rejects(()=>prepareHistoricalChecklistMetadata({...ctx,rpc:async name=>name===manifestRpc?page([expected]):(writes++,null)}));
    assert.equal(writes,0);
  }
});
test('fresh verification new missing row or unavailable native owner preserves confirmed preparation without readiness/qualification claim',async t=>{
  for(const unavailable of [false,true]){
    const ctx=context(t),expected=fixture();let reads=0;
    const result=await prepareHistoricalChecklistMetadata({...ctx,rpc:async name=>{
      if(name===prepareRpc)return receipt(expected);
      if(++reads===1)return page([expected]);if(unavailable)throw Error('PGRST202');return page([fixture(11)]);
    }});
    assert.equal(result.confirmed_count,1);assert.equal(result.ok,false);assert.equal(result.qualification,false);
    assert.equal(result.status,unavailable?'PREPARED_VERIFICATION_UNAVAILABLE':'MISSING_BOOKKEEPING_REMAINS');
  }
});
test('immutable evidence refusals happen before writes or stop truthfully after an actual confirmed metadata receipt',async t=>{
  const ctx=context(t);fs.mkdirSync(ctx.directory);let calls=0;
  await assert.rejects(()=>prepareHistoricalChecklistMetadata({...ctx,rpc:async()=>{calls++;}}));assert.equal(calls,0);
  const fresh=context(t),expected=fixture();let writes=0;
  const result=await prepareHistoricalChecklistMetadata({...fresh,rpc:async name=>{
    if(name===manifestRpc)return page([expected]);writes++;
    fs.writeFileSync(path.join(fresh.directory,`receipt-${expected.timesheet_id}.json`),'preserved pre-existing bytes');return receipt(expected);
  }});
  assert.equal(writes,1);assert.equal(result.confirmed_count,1);assert.equal(result.status,'STOPPED_EVIDENCE_PERSISTENCE_FAILED');
  assert.equal(result.receipts[0].outcome,'CONFIRMED');assert.equal(result.receipts[0].evidence,null);assert.equal(result.ok,false);
  assert.equal(fs.readFileSync(path.join(fresh.directory,`receipt-${expected.timesheet_id}.json`),'utf8'),'preserved pre-existing bytes');
});
