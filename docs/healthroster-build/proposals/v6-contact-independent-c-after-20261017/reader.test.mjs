import test from 'node:test';
import {EventEmitter} from 'node:events';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {BoundedMemoryCapture,LIMITS,createFixedMetadataReader} from './fixed-metadata-reader.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:16384,backendPid:42,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1]};
const turn={identity,signal:new AbortController().signal,assertHeld:()=>{throw Error('TEST_MUST_NOT_CONNECT');},runOwnedChild:()=>{throw Error('TEST_MUST_NOT_CONNECT');}};
const capture=text=>{const c=new BoundedMemoryCapture();c.stdout(Buffer.from(text));return c;};
test('split UTF8 bytes round-trip without per-chunk decoding loss',()=>{
 const c=new BoundedMemoryCapture(),b=Buffer.from('{"pid":1}\n{"value":"ðŸ’·Ã©"}\n');
 for(const byte of b)c.stdout(Buffer.from([byte]));
 assert.equal(c.finish().snapshot.value,'ðŸ’·Ã©');
});
test('only two complete JSON object frames accepted',()=>{
 for(const text of ['{}\n','{}\n{}\n{}\n','NOTICE\n{}\n','[]\n{}\n','{}\n{"x":\n'])assert.throws(()=>capture(text).finish(),/^Error: LOCAL_METADATA_/);
});
test('invalid UTF8 is refused before parsing',()=>{const c=new BoundedMemoryCapture();c.stdout(Buffer.from([0xff]));assert.throws(()=>c.finish(),/LOCAL_METADATA_UTF8_REFUSED/);});
test('byte cap refuses before retaining excess, no large allocation needed',()=>{
 const c=new BoundedMemoryCapture();c.bytes=LIMITS.stdoutBytes;c.stdout(Buffer.from('X'));assert.equal(c.failed,'LOCAL_METADATA_STDOUT_BOUND');assert.equal(c.chunks.length,0);
 c.stdout(Buffer.alloc(1));assert.equal(c.chunks.length,0);assert.throws(()=>c.finish(),/LOCAL_METADATA_STDOUT_BOUND/);
});
test('stderr is discarded and any diagnostic holds',()=>{const c=new BoundedMemoryCapture();c.stderr(Buffer.from('secret or clinical diagnostic'));assert.equal(c.failed,'LOCAL_METADATA_DIAGNOSTIC_REFUSED');assert.equal(c.chunks.length,0);assert.throws(()=>c.finish(),/LOCAL_METADATA_DIAGNOSTIC_REFUSED/);});
test('JSON parse failure exposes only a closed code',()=>{try{capture('{}\n{"sensitive":"broken}\n').finish();assert.fail();}catch(e){assert.equal(e.message,'LOCAL_METADATA_JSON_REFUSED');assert.ok(!e.stack.includes('sensitive'));}});
test('finite original canonical program unchanged, once, READ ONLY and no data fixture',async()=>{
 const reader=await createFixedMetadataReader({root,turn,caseId:'contact20-lock1'}),program=reader.inspectSource();
 assert.equal(program.input.split(program.canonicalSql).length,2);
 assert.ok(program.input.startsWith('begin isolation level repeatable read read only;\n'));
 assert.ok(program.input.endsWith('\nrollback;\n'));
 assert.equal(program.relations.length,8);
 assert.ok(program.canonicalSql.includes("'backgroundRows'"));assert.ok(program.canonicalSql.includes('backgroundWriterRoutines'));
 const {restorationSnapshotSql}=await import(pathToFileURL(path.join(root,'scripts/healthroster-install1-full-rehearsal.mjs')).href);
 const prior=JSON.parse(fs.readFileSync(path.join(here,'../v6-contact-six-lock-contention-20261016/PLAN.json'),'utf8'));
 assert.equal(program.canonicalSql,restorationSnapshotSql({identity,marker:prior.marker,relations:program.relations,
  exportContractSql:fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'),observeLiveBusiness:true}));
 const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
 assert.deepEqual(scanManagedSql(program.input).statements.map(s=>program.input.slice(s.start,s.end).trim().match(/^[a-z]+/i)[0].toLowerCase()),['begin','do','select','set','with','rollback']);
});
test('no generic SQL or route/cap replacement API exists',()=>{
 const text=fs.readFileSync(new URL('./fixed-metadata-reader.mjs',import.meta.url),'utf8');
 assert.ok(!text.includes('verification_sql'));assert.ok(!text.includes('miget_db_transaction_rollback('));
 assert.ok(!text.includes('writeFile'));assert.ok(!text.includes('console.'));assert.ok(!text.includes('turn.turnConnection.query'));
 assert.ok(text.includes("child.kill('SIGTERM')"));assert.ok(text.includes("child.kill('SIGKILL')"));
 assert.ok(!text.includes('pg_cancel_backend'));assert.ok(!text.includes('pg_terminate_backend'));
});
test('saved shared helper caps and full authority are unchanged',()=>{
 const shared=fs.readFileSync(path.join(root,'scripts/healthroster-rehearsal-turn.mjs'),'utf8');
 assert.ok(shared.includes('line.length>1000000||p.lines.length>20'));assert.ok(shared.includes('probeMs=10000'));assert.ok(shared.includes('statement_timeout=5s'));
 const live=fs.readFileSync(path.join(root,'scripts/healthroster-live-test-restoration.mjs'),'utf8');
 assert.ok(live.includes('const originalKeys=new Map(backgroundRelations.map(relation=>[relation,new Set(completeRows(before,relation).keys())]));'));
 assert.ok(live.includes('for(const relation of backgroundRelations)completeRows(after,relation);'));assert.ok(live.includes('const qualifiedFrames=new WeakMap();'));
});

test('AFTER requires exactly a complete third control frame; BEFORE forbids it',()=>{
 const text='{"readerPid":204}\n{"complete":true}\n{"backendPid":204,"connectedFresh":true}\n';
 const c=new BoundedMemoryCapture('AFTER');for(const byte of Buffer.from(text))c.stdout(Buffer.from([byte]));
 const p=c.finish();assert.equal(p.fixtureProof.backendPid,204);assert.equal(p.transport.frameCount,3);
 assert.equal(p.transport.fixtureProofSha256.length,64);assert.throws(()=>capture(text).finish(),/FRAME_COUNT/);
 for(const t of ['{}\n{}\n','{}\n{}\n{}\n{}\n','{}\n{}\n{"private":"broken}\n','{}\n{}\n[]\n']){
  const c=new BoundedMemoryCapture('AFTER');c.stdout(t);assert.throws(()=>c.finish(),/^Error: LOCAL_METADATA_/);
 }
});

test('third control frame has independent small bound within unchanged64MiB total',()=>{
 const c=new BoundedMemoryCapture('AFTER');c.stdout('{}\n{}\n'+JSON.stringify({x:'X'.repeat(16384)})+'\n');
 assert.throws(()=>c.finish(),/LOCAL_METADATA_CONTROL_FRAME_BOUND/);
 assert.equal(LIMITS.stdoutBytes,67108864);assert.equal(LIMITS.childMilliseconds,70000);
});

test('28MB synthetic canonical snapshot remains complete in memory with third frame',()=>{
 const c=new BoundedMemoryCapture('AFTER'),snapshot=JSON.stringify({synthetic:'X'.repeat(28*1024*1024)});
 for(const text of ['{}\n',snapshot,'\n',JSON.stringify({backendPid:204}),'\n'])c.stdout(Buffer.from(text));
 const p=c.finish();assert.equal(p.snapshot.synthetic.length,28*1024*1024);assert.equal(p.transport.snapshotUtf8Bytes,Buffer.byteLength(snapshot));
 assert.equal(p.transport.frameCount,3);assert.equal(c.chunks.length,0);
});

test('C-after SQL is exact canonical snapshot followed once by exact original fixture proof and rollback',async()=>{
 const reader=await createFixedMetadataReader({root,turn,caseId:'contact20-lock1'});
 const {source}=await import('./input-builder.mjs'),loaded=await source(root);
 const transaction={database:'cloudtms_test_clone',backendPid:202,xid:'105',epochZero:true};
 const observations=[{unit:0,...transaction,rows:[]}],before={backgroundRows:Object.fromEntries(loaded.live.backgroundRelations.map(r=>[r,[]]))};
 const p=reader.inspectSource({transaction,observations,before}),proof=loaded.live.freshFixtureProofSql(transaction,observations,before);
 assert.equal(p.after.fixtureSql,proof);assert.equal(p.after.input,p.input.slice(0,-'rollback;\n'.length)+proof+'\nrollback;\n');
 assert.equal(p.after.input.split(p.canonicalSql).length,2);assert.equal(p.after.input.split(proof).length,2);
 assert.ok(p.after.input.indexOf(proof)>p.after.input.indexOf(p.canonicalSql));
 assert.deepEqual(loaded.sql.scanManagedSql(p.after.input).statements.map(s=>p.after.input.slice(s.start,s.end).trim().match(/^[a-z]+/i)[0].toLowerCase()),['begin','do','select','set','with','select','rollback']);
 const wrong=structuredClone(observations);wrong[0].rows.push({relation:loaded.live.backgroundRelations[0],keyHash:'a'.repeat(64)});
 assert.throws(()=>reader.inspectSource({transaction,observations:wrong,before}),/LOCAL_METADATA_EMPTY_NATIVE_B_REQUIRED/);
});

// Exercise the exact private child function in an offline VM with fake OS events.
// Production retains no caller-supplied SQL/spawn/connection callback.
function childHarness({phase='AFTER',startThrows=false,postHeldFails=false}={}){
 const text=fs.readFileSync(new URL('./fixed-metadata-reader.mjs',import.meta.url),'utf8');
 const body=text.slice(text.indexOf('async function runFixedChild('),text.indexOf('\nexport async function createFixedMetadataReader'));
 const child=new EventEmitter();child.stdout=new EventEmitter();child.stderr=new EventEmitter();child.stdin=new EventEmitter();
 const killed=[],timers=[],controller=new AbortController();let written,held=0;
 child.kill=signal=>{killed.push(signal);return true;};child.stdin.end=input=>{written=input;};
 const schedule=(callback,ms)=>{const t={callback,ms,cleared:false};timers.push(t);return t;};
 const clear=t=>{if(t)t.cleared=true;};
 const spawn=(exe,args,options)=>{assert.equal(exe,'fixed-psql');assert.equal(options.windowsHide,true);assert.equal(options.stdio.join(','),'pipe,pipe,pipe');if(startThrows)throw Error('private-start-error');return child;};
 const fn=new Function('spawn','check','LIMITS','BoundedMemoryCapture','setTimeout','clearTimeout','psql','return '+body)(spawn,(ok,code)=>{if(!ok)throw Error(code);},LIMITS,BoundedMemoryCapture,schedule,clear,'fixed-psql');
 const owned={signal:controller.signal,assertHeld:async()=>{held++;if(postHeldFails&&held===2)throw Error('private-lock-error');}};
 const promise=fn({input:'FIXED_PROGRAM',env:{},turn:owned,phase});
 return {promise,child,killed,timers,controller,written:()=>written,held:()=>held,tick:ms=>{const t=timers.find(t=>t.ms===ms&&!t.cleared);assert.ok(t);t.callback();}};
}

test('exact fixed child closes before accepting complete three-frame result and rechecks turn',async()=>{
 const h=childHarness();await Promise.resolve();h.child.stdout.emit('data',Buffer.from('{}\n{}\n{}\n'));h.child.emit('close',0,null);
 const p=await h.promise;assert.equal(p.childClosed,true);assert.equal(p.transport.frameCount,3);assert.equal(h.held(),2);assert.equal(h.written(),'FIXED_PROGRAM');
 assert.ok(h.timers.every(t=>t.cleared));assert.deepEqual(h.killed,[]);
});

test('child stderr, nonzero exit, truncated frame and lost lock all close as closed failures',async()=>{
 for(const kind of ['stderr','exit','truncated','lost','postHeld']){
  const h=childHarness({postHeldFails:kind==='postHeld'}),rejected=assert.rejects(h.promise,/^Error: LOCAL_METADATA_[A-Z_]+$/);await Promise.resolve();
  if(kind==='stderr')h.child.stderr.emit('data',Buffer.from('private server diagnostic'));
  if(kind==='lost')h.controller.abort();
  h.child.stdout.emit('data',Buffer.from(kind==='truncated'?'{}\n{}\n{"private":"broken}\n':'{}\n{}\n{}\n'));
  h.child.emit('close',kind==='exit'?1:0,null);await rejected;
  if(['stderr','lost'].includes(kind))assert.deepEqual(h.killed,['SIGTERM']);
 }
});

test('own-child deadline escalates only its child and unconfirmed close cannot pass',async()=>{
 const h=childHarness(),rejected=assert.rejects(h.promise,/LOCAL_METADATA_CHILD_CLOSE_UNCONFIRMED/);await Promise.resolve();
 h.tick(70000);assert.deepEqual(h.killed,['SIGTERM']);h.tick(2000);assert.deepEqual(h.killed,['SIGTERM','SIGKILL']);h.tick(5000);await rejected;
 const source=fs.readFileSync(new URL('./fixed-metadata-reader.mjs',import.meta.url),'utf8');assert.ok(!source.includes('pg_terminate_backend'));assert.ok(!source.includes('pg_cancel_backend'));
});

test('spawn and stdin failure have bounded closed errors and no payload disclosure',async()=>{
 const a=childHarness({startThrows:true});await assert.rejects(a.promise,/^Error: LOCAL_METADATA_START_FAILED$/);
 const b=childHarness(),rejected=assert.rejects(b.promise,/^Error: LOCAL_METADATA_INPUT_FAILED$/);await Promise.resolve();b.child.stdin.emit('error',Error('private stdin error'));b.child.emit('close',1,null);await rejected;
});
