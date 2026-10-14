import test from 'node:test';import assert from 'node:assert/strict';import {EventEmitter} from 'node:events';
import {createRehearsalStreamObserver} from '../scripts/healthroster-rehearsal-stream-observer.mjs';
import {runOwnedTurnChild} from '../scripts/healthroster-turn-control.mjs';
const units=[{index:0,file:'supabase/verification/native.sql'}],lineMap=[{startLine:10,endLine:20,file:units[0].file,kind:'verifier',statement:1,object:'private.actual_owner'}];
const make=()=>{const records=[];return {records,observer:createRehearsalStreamObserver({units,lineMap,allowedCodes:new Set(['ACTUAL_SOURCE_CODE']),onRecord:r=>records.push(r)})};};
test('fragmented CRLF control records preserve exact source unit and partial native transaction identity',()=>{
 const {records,observer}=make();const text='HR_FILE_BEGIN 0\r\nHR_REHEARSAL_TRANSACTION_JSON '+JSON.stringify({database:'cloudtms_test_clone',backendPid:91,xid:'123',epochZero:true})+'\nHR_FILE_END 0\n';
 for(const byte of Buffer.from(text))observer.onStdout(Buffer.from([byte]));
 assert.deepEqual(records.map(r=>r.kind),['UNIT_PROGRESS','PARTIAL_TRANSACTION_IDENTITY','UNIT_PROGRESS']);assert.equal(records[1].backendPid,91);assert.equal(records[1].qualified,false);assert.equal(records[2].event,'END');
 assert.equal(observer.summary().sqlQualified,false);assert.equal(observer.summary().restorationQualified,false);
});
test('source-bound closed diagnostics survive fragmented stderr without arbitrary primary/detail/context text',()=>{
 const {records,observer}=make();const text='psql:<stdin>:15: ERROR: 55000: ACTUAL_SOURCE_CODE confidential suffix\nDETAIL: confidential detail\nCONTEXT: private SQL\npsql:<stdin>:16: ERROR: 23505: SECRET_UPPERCASE_PAYLOAD\n';
 for(let i=0;i<text.length;i+=7)observer.onStderr(Buffer.from(text.slice(i,i+7)));
 assert.equal(records.length,2);assert.equal(records[0].error,'ACTUAL_SOURCE_CODE');assert.equal(records[0].object,'private.actual_owner');assert.equal(records[1].error,'DATABASE_DIAGNOSTIC_WITHHELD');assert.equal(records[0].errorCollectionComplete,false);
 assert.doesNotMatch(JSON.stringify(records),/confidential|private SQL|SECRET_UPPERCASE_PAYLOAD/);
});
test('unmapped fatal and psql client failures retain closed failure identity without connection or primary text',()=>{
 const {records,observer}=make();observer.onStderr(Buffer.from('psql: error: connection to private-host failed\nFATAL: 57P01: confidential shutdown payload\n'));
 assert.equal(records.length,2);assert.equal(records[1].sqlstate,'57P01');assert.ok(records.every(r=>r.error==='DATABASE_DIAGNOSTIC_LOCATION_UNPROVED'));assert.doesNotMatch(JSON.stringify(records),/private-host|confidential/);
});
test('raw rows, fixtures, SQL, catalogue and long-line suffixes cannot become durable records',()=>{
 const {records,observer}=make();observer.onStdout(Buffer.from('private row\nHR_FIXTURE_OBSERVATION_JSON {"private":"row"}\n'+'x'.repeat(5000)));
 observer.onStdout(Buffer.from('HR_FILE_BEGIN 0\nHR_FILE_END 0\n'));
 assert.equal(records.length,1);assert.equal(records[0].event,'END');assert.equal(observer.summary().stdout.overlongLines,1);assert.doesNotMatch(JSON.stringify(records),/private row|HR_FIXTURE/);
});
test('unknown units, invalid/extra transaction fields, other databases and unsafe epochs do not produce witnesses',()=>{
 const {records,observer}=make();observer.onStdout(Buffer.from('HR_FILE_BEGIN 99\nHR_REHEARSAL_TRANSACTION_JSON broken\n'));
 for(const delta of [{database:'other'},{backendPid:0},{xid:'4294967296'},{epochZero:false},{extra:'private'}])observer.onStdout(Buffer.from('HR_REHEARSAL_TRANSACTION_JSON '+JSON.stringify({database:'cloudtms_test_clone',backendPid:91,xid:'123',epochZero:true,...delta})+'\n'));
 assert.deepEqual(records,[]);
});
test('unterminated and overlong stderr cannot leak bytes or masquerade as complete diagnostics',()=>{
 const {records,observer}=make();observer.onStderr(Buffer.from('DETAIL: '+'x'.repeat(5000)));observer.onStdout(Buffer.from('HR_FILE_BEGIN '));
 assert.deepEqual(records,[]);assert.deepEqual(observer.summary().stderr,{incompleteLine:true,overlongLines:1});assert.equal(observer.summary().stdout.incompleteLine,true);
});
test('closed records already persisted remain after the exact owned child loses its turn',async()=>{
 const {records,observer}=make(),controller=new AbortController(),child=new EventEmitter();
 child.stdout=new EventEmitter();child.stderr=new EventEmitter();child.kill=signal=>setImmediate(()=>child.emit('close',null,signal));
 const task=runOwnedTurnChild({file:'fake',signal:controller.signal,assertHeld:async()=>{},spawnChild:()=>child,options:{onStdout:observer.onStdout,onStderr:observer.onStderr}});
 await new Promise(resolve=>setImmediate(resolve));child.stdout.emit('data',Buffer.from('HR_FILE_BEGIN 0\n'));child.stderr.emit('data',Buffer.from('psql:<stdin>:15: ERROR: 55000: ACTUAL_SOURCE_CODE private text\n'));controller.abort(Error('TURN_CONNECTION_CLOSED'));
 await assert.rejects(task,/TURN_CONNECTION_CLOSED/);assert.equal(records.length,2);assert.equal(records[1].errorCollectionComplete,false);assert.equal(observer.summary().sqlQualified,false);
});
test('persistence failure cancels only the owned rollback child and fails without exposing callback text',async()=>{
 const controller=new AbortController(),child=new EventEmitter(),kills=[];child.stdout=new EventEmitter();child.stderr=new EventEmitter();child.kill=signal=>{kills.push(signal);setImmediate(()=>child.emit('close',null,signal));};
 const observer=createRehearsalStreamObserver({units,lineMap,allowedCodes:new Set(),onRecord(){throw Error('private write error');}});
 const task=runOwnedTurnChild({file:'fake',signal:controller.signal,assertHeld:async()=>{},spawnChild:()=>child,options:{onStdout:observer.onStdout}});
 await new Promise(resolve=>setImmediate(resolve));child.stdout.emit('data',Buffer.from('HR_FILE_BEGIN 0\n'));await assert.rejects(task,/OWNED_CHILD_OBSERVER_FAILED/);assert.deepEqual(kills,['SIGTERM']);
});
