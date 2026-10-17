// SYNTHETIC_SOURCE_BOUNDARY_TESTS_ONLY. No executable/process, database or API call.
import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import path from'node:path';import vm from'node:vm';import{fileURLToPath,pathToFileURL}from'node:url';import{EventEmitter}from'node:events';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';
import{SqlstateStderrCapture,diagnosticContext,diagnosticError,preserveMetadataDiagnostic,metadataDiagnosticFor}from'./current/metadata-diagnostic.mjs';
import{BoundedMemoryCapture,LIMITS}from'./current/fixed-metadata-reader.mjs';
import{createFixedMetadataReader}from'./current/fixed-metadata-reader.mjs';
import{ROOT}from'./current/source.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex'),raw=fs.readFileSync(here+'/current/fixed-metadata-reader.mjs','utf8');
const check=(v,e)=>{if(!v)throw Error(e);};
const states=['55000','42P01','42703','57014','XX000','08006'];
function parse(parts,closed=true){const p=new SqlstateStderrCapture();for(const b of parts)p.write(b);return p.finish({closed});}
test('anchored exact ERROR/FATAL frame survives every byte boundary and one-byte chunks',()=>{
 let vectors=0;
 for(const state of states)for(const head of ['ERROR:  ','FATAL:  ','psql:<stdin>:1: ERROR:  ','psql:<stdin>:2147483647: FATAL:  '])for(const tail of ['','\n','\r\n']){
  const b=Buffer.from(head+state+tail);for(let i=0;i<=b.length;i++){assert.equal(parse([b.subarray(0,i),b.subarray(i)]),state);vectors++;}
  assert.equal(parse([...b].map(n=>Buffer.from([n]))),state);vectors++;
 }
 const expected=states.length*['ERROR:  ','FATAL:  ','psql:<stdin>:1: ERROR:  ','psql:<stdin>:2147483647: FATAL:  '].reduce((n,head)=>n+['','\n','\r\n'].reduce((m,tail)=>m+Buffer.byteLength(head+'XXXXX'+tail)+2,0),0);assert.equal(vectors,expected);assert.ok(vectors>1500);
});
test('canaries, NOTICE, SQLSTATE suffix/prefix, multiple frames and malformed UTF8 remain generic',()=>{
 const canary='PRIVATE_CREDENTIAL_QUERY_CANARY_7890';
 for(const s of [canary,'NOTICE:  00000\n','NOTICE:  00000\nERROR:  55000\n','ERROR:  55000\nNOTICE:  00000\n',
  'prefix ERROR:  55000\n','ERROR:  55000 '+canary+'\n','ERROR:  550000\n','ERROR:  55p01\n','ERROR:  55000\nERROR:  42703\n',
  'ERROR:  55000\n\n','ERROR:  5500','ERROR:  55000\r','psql:/secret/query.sql:1: ERROR:  55000\n','psql:<stdin>:0: ERROR:  55000\n']){
  const b=Buffer.from(s);for(let i=0;i<=b.length;i++)assert.equal(parse([b.subarray(0,i),b.subarray(i)]),null);
 }
 for(const b of [Buffer.from([255]),Buffer.concat([Buffer.from('ERROR:  55000'),Buffer.from([0xff,10])]),Buffer.from('ERROR:  55000\u0000\n')])assert.equal(parse([b]),null);
 const p=new SqlstateStderrCapture();p.write(Buffer.from(canary));assert.equal(JSON.stringify(p),'{}');assert.equal(p.finish({closed:true}),null);assert.equal(JSON.stringify(p),'{}');
});
test('unclosed/truncated child and parser reuse cannot qualify an SQLSTATE',()=>{
 assert.equal(parse(['ERROR:  55000\n'],false),null);const p=new SqlstateStderrCapture();p.write('ERROR:  55000\n');assert.equal(p.finish({closed:true}),'55000');assert.equal(p.finish({closed:true}),null);p.write('ERROR:  42703\n');assert.equal(p.finish({closed:true}),null);
});
test('16KiB cap and every stderr retain refusal while dropping captured stdout maps',()=>{
 const c=new BoundedMemoryCapture('BEFORE');c.stdout(Buffer.from('RAW_MAP_CANARY'));
 for(const b of Buffer.from('psql:<stdin>:123: ERROR:  42P01\n'))c.stderr(Buffer.from([b]));
 assert.equal(c.failed,'LOCAL_METADATA_DIAGNOSTIC_REFUSED');assert.equal(c.chunks.length,0);assert.throws(()=>c.finish(),/LOCAL_METADATA_DIAGNOSTIC_REFUSED/);assert.equal(c.diagnosticSqlstate(true),'42P01');assert.doesNotMatch(JSON.stringify(c),/RAW_MAP_CANARY|ERROR|psql|42P01/);
 const over=new BoundedMemoryCapture();over.stderr(Buffer.alloc(16*1024+1,65));assert.equal(over.failed,'LOCAL_METADATA_STDERR_BOUND');assert.equal(over.diagnosticSqlstate(true),null);assert.equal(over.chunks.length,0);
 const split=new BoundedMemoryCapture();split.stderr(Buffer.from('ERROR:  55000\n'));split.stderr(Buffer.alloc(16*1024,65));assert.equal(split.failed,'LOCAL_METADATA_STDERR_BOUND');assert.equal(split.diagnosticSqlstate(true),null);
});
test('closed diagnostic output contains phase/time/optional state only and rejects duck typing',()=>{
 const c=diagnosticContext('BEFORE');c.spawnReturnedAt=new Date().toISOString();c.firstStderrAt=new Date().toISOString();c.rawStderr='SECRET_CANARY';
 const e=diagnosticError('LOCAL_METADATA_DIAGNOSTIC_REFUSED',c,'55000'),record=metadataDiagnosticFor(e);
 assert.deepEqual(Object.keys(record).sort(),['elapsedMilliseconds','finishedAt','firstStderrAt','phase','spawnReturnedAt','sqlstate','startedAt','version']);
 assert.equal(record.sqlstate,'55000');assert.ok(record.elapsedMilliseconds>=0);assert.doesNotMatch(JSON.stringify(record),/SECRET_CANARY|rawStderr|query|password/);
 const wrapped=preserveMetadataDiagnostic(e,Error('LOCAL_METADATA_DIAGNOSTIC_REFUSED'));assert.deepEqual(metadataDiagnosticFor(wrapped),record);
 assert.equal(metadataDiagnosticFor({metadataDiagnostic:record}),null);assert.equal(metadataDiagnosticFor(Error('ERROR:  55000')),null);assert.equal(metadataDiagnosticFor(diagnosticError('LOCAL_METADATA_DIAGNOSTIC_REFUSED',{...c,startedAt:'2026-99-99T00:00:00.000Z'})),null);
});
// Execute the exact private runFixedChild function with an in-memory EventEmitter spawn.
// No OS process is created; these are transport event regressions, not native evidence.
async function fakeChildRead({chunks=[],stdout=null,code=1,phase='BEFORE'}){
 const start=raw.indexOf('async function runFixedChild('),end=raw.indexOf('\nexport async function createFixedMetadataReader',start),fnSource=raw.slice(start,end);
 const child=new EventEmitter();child.stdout=new EventEmitter();child.stderr=new EventEmitter();child.stdin=new EventEmitter();let kills=0,spawnCount=0;
 child.kill=()=>{kills++;return true;};child.stdin.end=()=>queueMicrotask(()=>{if(stdout)child.stdout.emit('data',stdout);for(const chunk of chunks)child.stderr.emit('data',chunk);child.emit('close',code,kills?'SIGTERM':null);});
 const fn=new vm.Script('('+fnSource+')').runInNewContext({spawn:()=>{spawnCount++;return child;},psql:'DISABLED_NO_PROCESS',BoundedMemoryCapture,LIMITS,check,diagnosticContext,diagnosticError,setTimeout,clearTimeout,Date});
 let error;try{await fn({input:'SYNTHETIC_NOT_SQL',env:{},turn:{assertHeld:async()=>{},signal:new AbortController().signal},phase});}catch(e){error=e;}
 assert.equal(spawnCount,1);return{error,kills,record:metadataDiagnosticFor(error)};
}
test('actual capture callback consumes split chunks after first immediate kill and persists only safe state',async()=>{
 const b=Buffer.from('psql:<stdin>:123: ERROR:  55000\n');for(let i=1;i<b.length;i++){
  const r=await fakeChildRead({chunks:[b.subarray(0,i),b.subarray(i)]});assert.equal(r.error.message,'LOCAL_METADATA_DIAGNOSTIC_REFUSED');assert.equal(r.kills,1);assert.equal(r.record.sqlstate,'55000');assert.equal(r.record.phase,'BEFORE');assert.ok(r.record.firstStderrAt);assert.doesNotMatch(JSON.stringify(r.record),/psql|ERROR|SYNTHETIC_NOT_SQL/);
 }
});
test('actual child event path refuses NOTICE/unsupported stderr and keeps UTF8 stdout guard',async()=>{
 for(const chunks of [[Buffer.from('NOTICE:  00000\n')],[Buffer.from('ERROR:  55000 SECRET_CANARY\n')],[Buffer.from('ERROR:  55000\n'),Buffer.from('NOTICE:  00000\n')]]){
  const r=await fakeChildRead({chunks,phase:'AFTER'});assert.equal(r.error.message,'LOCAL_METADATA_DIAGNOSTIC_REFUSED');assert.equal(r.record.sqlstate,null);assert.equal(r.record.phase,'AFTER');assert.equal(r.kills,1);assert.doesNotMatch(JSON.stringify(r.record),/SECRET_CANARY|NOTICE/);
 }
 const capture=new BoundedMemoryCapture();capture.stdout(Buffer.from([255]));assert.throws(()=>capture.finish(),/LOCAL_METADATA_UTF8_REFUSED/);
 const u=await fakeChildRead({stdout:Buffer.from([255]),code:0});assert.equal(u.error.message,'LOCAL_METADATA_READ_REFUSED');assert.equal(u.record.sqlstate,null);
});
test('all mounted original parents reverse; canonical SQL/input, controls and programmes remain exact',async()=>{
 const p=JSON.parse(fs.readFileSync(here+'/PARENT_REVERSES.json')),selection=JSON.parse(fs.readFileSync(here+'/PAYLOAD_SELECTION.json'));
 const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',ROOT,...args],{maxBuffer:32e6});let unchanged=0;
 for(const r of selection.files){const b=fs.readFileSync(here+'/'+r.payload);assert.equal(sha(b),r.sha256);if(r.parentSha256===null){assert.equal(path.basename(r.target),'metadata-diagnostic.mjs');continue;}
  const before=git('show',p.parentCommit+':'+r.target);assert.equal(sha(before),r.parentSha256);const edit=p.changed.find(e=>r.target===p.parentPrefix+'/'+e.file);if(edit){assert.equal(edit.beforeRaw,before.toString());assert.equal(edit.afterRaw,b.toString());let current=b.toString();for(const h of [...edit.hunks].reverse())current=current.replace(h.after,()=>h.before);assert.equal(current,before.toString());}else{assert.deepEqual(b,before);unchanged++;}
 }
 assert.equal(unchanged,40);for(const name of ['control.mjs','timing.mjs','PROGRAMMES.json','OFFLINE_INPUTS.json','native-boundary.mjs','input-builder.mjs','source.mjs','SOURCE_PINS.json','qt-assertions.sql'])assert.deepEqual(fs.readFileSync(here+'/current/'+name),git('show',p.parentCommit+':'+p.parentPrefix+'/'+name));
 const old=await import(pathToFileURL(ROOT+'/'+p.parentPrefix+'/fixed-metadata-reader.mjs').href),turn={identity:{physicalDatabase:'cloudtms_test_clone',databaseOid:17,backendPid:101,turnId:'hr_turn_'+'1'.repeat(32),key:[20261013,1]},signal:new AbortController().signal,assertHeld:async()=>{throw Error('NO_CONNECTION_ALLOWED');},runOwnedChild:()=>{throw Error('NO_CONNECTION_ALLOWED');}};
 for(const caseId of ['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1']){const a=await createFixedMetadataReader({root:ROOT,turn,caseId}),b=await old.createFixedMetadataReader({root:ROOT,turn,caseId});assert.deepEqual(a.inspectSource(),b.inspectSource());}
 assert.deepEqual(LIMITS,{stdoutBytes:67108864,stderrBytes:16384,connectSeconds:10,statementMilliseconds:45000,lockMilliseconds:2000,childMilliseconds:70000,terminateGraceMilliseconds:2000,closeGraceMilliseconds:3000});
});
