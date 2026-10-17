// Exact private source, synthetic EventEmitter children and virtual timers only.
// No OS process, socket, SQL execution, shared turn or native timing is produced.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import assert from 'node:assert/strict';import {EventEmitter} from 'node:events';import {fileURLToPath} from 'node:url';
import {BoundedMemoryCapture,LIMITS} from './frozen/current/fixed-metadata-reader.mjs';
import {diagnosticContext,diagnosticError,metadataDiagnosticFor} from './frozen/current/metadata-diagnostic.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),raw=fs.readFileSync(here+'/frozen/current/fixed-metadata-reader.mjs','utf8'),start=raw.indexOf('async function runFixedChild('),end=raw.indexOf('\nexport async function createFixedMetadataReader',start),source=raw.slice(start,end),cases=[];
async function exercise({label,chunks=[],close=true,code=1,phase='BEFORE',preHeldFailure=false,spawnFailure=false,abort=false,deadline=false,expectedCode,expectedState=null,expectedKills=[]}){
 const child=new EventEmitter();child.stdout=new EventEmitter();child.stderr=new EventEmitter();child.stdin=new EventEmitter();const kills=[],timers=new Map(),controller=new AbortController();let seq=0,spawns=0;
 const later=(fn,ms)=>{const id=++seq;timers.set(id,{fn,ms,clear:false});return id;},clear=id=>{if(timers.has(id))timers.get(id).clear=true;},fire=ms=>{const t=[...timers.values()].find(t=>t.ms===ms&&!t.clear);assert(t);t.fn();};
 child.kill=signal=>{kills.push(signal);return true;};child.stdin.end=()=>queueMicrotask(()=>{child.stdout.emit('data',Buffer.from('SYNTHETIC_MAP_CANARY'));for(const b of chunks)child.stderr.emit('data',Buffer.from(b));if(abort)controller.abort(Error('SYNTHETIC_TURN_LOST'));if(deadline)fire(LIMITS.childMilliseconds);if(close)child.emit('close',code,kills.length?'SIGTERM':null);else{fire(LIMITS.terminateGraceMilliseconds);fire(LIMITS.terminateGraceMilliseconds+LIMITS.closeGraceMilliseconds);}});
 const fn=new vm.Script('('+source+')').runInNewContext({spawn:()=>{spawns++;if(spawnFailure)throw Error('SECRET_NATIVE_PATH_CANARY');return child;},psql:'DISABLED_NO_OS_PROCESS',BoundedMemoryCapture,LIMITS,check:(v,e)=>{if(!v)throw Error(e);},diagnosticContext,diagnosticError,setTimeout:later,clearTimeout:clear,Date});
 let error;try{await fn({input:'SYNTHETIC_NOT_SQL',env:{},turn:{assertHeld:async()=>{if(preHeldFailure)throw Error('SECRET_AUTH_CANARY');},signal:controller.signal},phase});}catch(e){error=e;}
 assert(error);assert.equal(error.message,expectedCode);const record=metadataDiagnosticFor(error);assert(record);assert.equal(record.sqlstate,expectedState);assert.equal(record.phase,phase);assert.deepEqual(kills,expectedKills);assert.doesNotMatch(JSON.stringify(record),/SECRET|CANARY|SYNTHETIC|SELECT|DISABLED/);assert.deepEqual(Object.keys(record).sort(),['elapsedMilliseconds','finishedAt','firstStderrAt','phase','spawnReturnedAt','sqlstate','startedAt','version']);
 if(preHeldFailure||spawnFailure){assert.equal(record.spawnReturnedAt,null);assert.equal(record.firstStderrAt,null);}else assert(record.spawnReturnedAt);
 for(const t of timers.values())assert(t.clear);cases.push({label,expectedCode,expectedState,phase,spawnCalls:spawns,killSignals:kills,closedShapeOnly:true,syntheticTimersNotNativeTiming:true});
}
await exercise({label:'complete SQLSTATE without child close remains null after TERM/KILL bound',chunks:['ERROR:  55000\n'],close:false,expectedCode:'LOCAL_METADATA_CHILD_CLOSE_UNCONFIRMED',expectedKills:['SIGTERM','SIGKILL']});
await exercise({label:'zero exit never clears stderr refusal',chunks:['ERROR:  42P01\n'],code:0,expectedCode:'LOCAL_METADATA_DIAGNOSTIC_REFUSED',expectedState:'42P01',expectedKills:['SIGTERM']});
await exercise({label:'AFTER partial frame is null',chunks:['FATAL:  0800'],phase:'AFTER',expectedCode:'LOCAL_METADATA_DIAGNOSTIC_REFUSED',expectedKills:['SIGTERM']});
await exercise({label:'earlier valid frame plus trailing line stays null',chunks:['ERROR:  55000\n','PRIVATE_CANARY'],expectedCode:'LOCAL_METADATA_DIAGNOSTIC_REFUSED',expectedKills:['SIGTERM']});
await exercise({label:'silent nonzero exit gets no guessed state',expectedCode:'LOCAL_METADATA_CHILD_FAILED'});
await exercise({label:'spawn refusal has no connection claim or raw cause',spawnFailure:true,expectedCode:'LOCAL_METADATA_START_FAILED'});
await exercise({label:'turn loss before spawn has no connection claim or raw cause',preHeldFailure:true,expectedCode:'LOCAL_METADATA_TURN_LOST'});
await exercise({label:'owned abort stops then closes without guessed state',abort:true,expectedCode:'LOCAL_METADATA_TURN_LOST',expectedKills:['SIGTERM']});
await exercise({label:'virtual child deadline stops and closes without inferred SQLSTATE',deadline:true,expectedCode:'LOCAL_METADATA_CHILD_DEADLINE',expectedKills:['SIGTERM']});
fs.writeFileSync(here+'/INDEPENDENT_EVENT_RESULTS.json',JSON.stringify({version:'CONTACT_DIAGNOSTIC_INDEPENDENT_PRIVATE_EVENT_VECTORS_V1',tests:cases.length,cases,native:'NOT_RUN',osProcesses:0,rootWrites:0},null,2)+'\n');console.log(JSON.stringify({independentEvents:cases.length,native:'NOT_RUN'}));
