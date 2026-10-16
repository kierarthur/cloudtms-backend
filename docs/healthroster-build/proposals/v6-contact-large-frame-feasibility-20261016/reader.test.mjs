import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {BoundedMemoryCapture,LIMITS,createFixedMetadataReader} from './fixed-metadata-reader.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:16384,backendPid:42,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1]};
const turn={identity,signal:new AbortController().signal,assertHeld:()=>{throw Error('TEST_MUST_NOT_CONNECT');},runOwnedChild:()=>{throw Error('TEST_MUST_NOT_CONNECT');}};
const capture=text=>{const c=new BoundedMemoryCapture();c.stdout(Buffer.from(text));return c;};
test('split UTF8 bytes round-trip without per-chunk decoding loss',()=>{
 const c=new BoundedMemoryCapture(),b=Buffer.from('{"pid":1}\n{"value":"💷é"}\n');
 for(const byte of b)c.stdout(Buffer.from([byte]));
 assert.equal(c.finish().snapshot.value,'💷é');
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
test('40-column capacity does not qualify actual full response',()=>{
 const b=JSON.parse(fs.readFileSync(new URL('./INCIDENT_AND_BUDGET.json',import.meta.url),'utf8'));
 assert.equal(b.hypotheticalFortyColumnGrid.rowsPerSnapshot,88);assert.equal(b.hypotheticalFortyColumnGrid.minimumDuplicateJsonPayloadAsciiBytes,111845646);
 assert.equal(b.before.lastEof,false);assert.equal(b.after.lastEof,false);assert.equal(b.accepted,false);
});
