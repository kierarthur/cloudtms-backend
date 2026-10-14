import test from'node:test';import assert from'node:assert/strict';import{EventEmitter}from'node:events';
import{createRehearsalResourceGuard,assertRehearsalResourceSample,createMigetRehearsalResourceReader}from'../scripts/healthroster-rehearsal-resource-guard.mjs';
import{runOwnedTurnChild}from'../scripts/healthroster-turn-control.mjs';
import{renderFullRehearsalUnits}from'../scripts/healthroster-full-rehearsal-protocol.mjs';
import{createHash}from'node:crypto';
const metrics=['miget_instance_memory_used_bytes','miget_instance_resource_limit','miget_instance_restarts_total','miget_instance_oom_events_total','miget_volume_used_bytes','miget_volume_size_bytes'];
const sample=()=>{const evaluatedAt=Date.now()/1000;return{service:'01a02f5a-2bee-7db2-910d-a7e71f11ba0a',pod:'postgres-service-p0qoe-1',namespace:'migetuq4',observedAt:new Date(evaluatedAt*1000).toISOString(),metricTimes:Object.fromEntries(metrics.map(name=>[name,{evaluatedAt,sampledAt:evaluatedAt-5}])),memoryUsed:1000000000,memoryLimit:4294967296,volumeUsed:5000000000,volumeLimit:52804530176,restarts:2,oomEvents:0};};
const vector=()=>{const s=sample(),values=[s.memoryUsed,s.memoryLimit,s.restarts,s.oomEvents,s.volumeUsed,s.volumeLimit];return metrics.flatMap((name,index)=>{const labels={addon:'postgres-service-p0qoe',namespace:'migetuq4',...(name.includes('volume')?{volume:'postgres-service-p0qoe-1'}:{instance:'postgres-service-p0qoe-1'}),...(index===1?{resource:'memory'}:{})};const times=s.metricTimes[name];return[{metric:{...labels,__name__:name},value:[times.evaluatedAt,String(values[index])]},{metric:{...labels,source_metric:name},value:[times.evaluatedAt,String(times.sampledAt)]}];});};
const vectorReader=result=>createMigetRehearsalResourceReader({headers:{authorization:'Bearer synthetic'},fetcher:async()=>({ok:true,json:async()=>({status:'success',data:{resultType:'vector',result}})})});
test('resource observations close exact TEST identity, scalar fields and native4GiB limit',()=>{
 assert.equal(assertRehearsalResourceSample(sample()).memoryLimit,4294967296);
 for(const alter of [s=>s.pod='other',s=>s.service='other',s=>s.namespace='other',s=>s.memoryUsed='1000',s=>s.memoryLimit=8589934592,s=>s.observedAt=['2026-10-14'],s=>s.rawQuery='private']){const value=sample();alter(value);assert.throws(()=>assertRehearsalResourceSample(value));}
});
test('preflight resource ceiling stops before any child is admitted and cannot be raised by caller',async()=>{
 const guard=createRehearsalResourceGuard({readSample:async()=>({...sample(),memoryUsed:3221225472})});await assert.rejects(guard.prepare(),/MEMORY_HEADROOM_STOP/);
 assert.throws(()=>createRehearsalResourceGuard({readSample:async()=>sample(),memoryStopBytes:4294967296}));assert.throws(()=>guard.start(()=>{}),/NOT_PREPARED/);
});
for(const[code,change]of [['REHEARSAL_MEMORY_HEADROOM_STOP',s=>s.memoryUsed=3221225472],['REHEARSAL_DISK_HEADROOM_STOP',s=>s.volumeUsed=s.volumeLimit-100],['REHEARSAL_TEST_RESTART_OR_OOM_OBSERVED',s=>s.restarts=3],['REHEARSAL_TEST_RESTART_OR_OOM_OBSERVED',s=>s.oomEvents=1]])test('periodic observation cancels only admitted owned child: '+code,async()=>{
 let poll,reads=0,cancellations=0;const guard=createRehearsalResourceGuard({readSample:async()=>{const value=sample();if(reads++)change(value);return value;},schedule:callback=>{poll=callback;return{};},unschedule:()=>{}});
 await guard.prepare();guard.start(error=>{assert.equal(error.code,code);cancellations++;});poll();const result=await guard.stop();assert.equal(result.code,code);assert.equal(cancellations,1);poll();assert.equal(cancellations,1);
});
test('observation or durable journal failure fails closed with no automatic retry',async()=>{
 let poll,reads=0,code;const guard=createRehearsalResourceGuard({readSample:async()=>{reads++;if(reads>1)throw Error('synthetic private diagnostic');return sample();},schedule:callback=>{poll=callback;return{};},unschedule:()=>{}});
 await guard.prepare();guard.start(error=>{code=error.code;});poll();await guard.stop();assert.equal(code,'REHEARSAL_RESOURCE_OBSERVATION_FAILED');assert.equal(reads,2);
 const journal=createRehearsalResourceGuard({readSample:async()=>sample(),onSample:()=>{throw Error('journal unavailable');}});await assert.rejects(journal.prepare(),/journal unavailable/);
});
test('native resource guard kills exact spawned rollback child without aborting shared turn or another process',async()=>{
 const controller=new AbortController(),owned=new EventEmitter(),kills=[];owned.stdout=new EventEmitter();owned.stderr=new EventEmitter();owned.kill=signal=>{kills.push(signal);setImmediate(()=>owned.emit('close',null,signal));};
 let cancel;const guard={start:callback=>{cancel=callback;},stop:async()=>({failed:true,code:'REHEARSAL_MEMORY_HEADROOM_STOP'})};
 const pending=runOwnedTurnChild({file:'controlled-child',signal:controller.signal,assertHeld:async()=>{},spawnChild:()=>owned,resourceGuard:guard});await new Promise(resolve=>setImmediate(resolve));cancel({code:'REHEARSAL_MEMORY_HEADROOM_STOP'});
 await assert.rejects(pending,/MEMORY_HEADROOM_STOP/);assert.deepEqual(kills,['SIGTERM']);assert.equal(controller.signal.aborted,false);
 await assert.rejects(runOwnedTurnChild({file:'durable-release',cancelOnLoss:false,signal:controller.signal,assertHeld:async()=>{},spawnChild:()=>assert.fail('release cannot be spawned'),resourceGuard:guard}),/ONLY_FOR_OWN_ROLLBACK_CHILD/);
});
test('Miget reader is a single exact addon GET; foreign or duplicate resource rows and429 refuse',async()=>{
 let calls=0;const fetcher=async(url,options)=>{calls++;assert.equal(options.method,undefined);assert.match(url,/query=/);assert.ok(new URL(url).searchParams.get('query').split(' or on(__name__,source_metric,instance,volume,resource,namespace) ').every(part=>part.includes('{addon="postgres-service-p0qoe"')));assert.match(new URL(url).searchParams.get('query'),/label_replace\(timestamp\(/);return{ok:false,status:429};};
 const read=createMigetRehearsalResourceReader({fetcher,headers:{authorization:'Bearer synthetic'}});await assert.rejects(read(),/HTTP_429/);assert.equal(calls,1);
 const denied=createMigetRehearsalResourceReader({headers:{authorization:'Bearer synthetic'},fetcher:async()=>({ok:true,json:async()=>({status:'success',data:{resultType:'vector',result:[]}})})});await assert.rejects(denied(),/READ_AMBIGUOUS/);
});
test('native metric-name identity is retained in union and complete actual-labelled scalar frame decodes',async()=>{
 const result=vector();
 const read=createMigetRehearsalResourceReader({headers:{authorization:'Bearer synthetic'},fetcher:async url=>{assert.match(new URL(url).searchParams.get('query'),/or on\(__name__,source_metric,instance,volume,resource,namespace\)/);return{ok:true,json:async()=>({status:'success',data:{resultType:'vector',result}})};}});
 const frame=await read();assert.equal(frame.memoryUsed,1000000000);assert.equal(frame.restarts,2);assert.equal(frame.oomEvents,0);assert.equal(frame.volumeLimit,52804530176);
});
test('plan disposal and bounded operator memory preserve entire verifier body, force-generic regression and outer transaction',()=>{
 const sql='set local plan_cache_mode=force_generic_plan;\nselect 1;',hash=createHash('sha256').update(sql).digest('hex');
 const result=renderFullRehearsalUnits([{file:'supabase/verification/example.sql',kind:'verifier',sql,sqlHash:hash}],{outerIsolation:'REPEATABLE READ'});
 assert.ok(result.sql.includes(sql));assert.match(result.sql,/discard plans;/);assert.doesNotMatch(result.sql,/discard (?:all|temp|sequences)/i);
 for(const statement of ["set local work_mem='4MB';",'set local hash_mem_multiplier=1;','set local max_parallel_workers_per_gather=0;',"set local client_connection_check_interval='1s';"]){assert.ok(result.sql.includes(statement));assert.ok(result.sql.indexOf(statement)<result.sql.indexOf('savepoint hr_verifier_0;'));}
 assert.match(result.sql,/rollback to savepoint hr_verifier_0;/);assert.equal(result.units[0].sqlHash,hash);
});

test('old evaluation and old native sample refuse even when receipt is locally fresh',async()=>{
 for(const stale of ['both','sample']){const rows=vector();for(const row of rows){if(stale==='both')row.value[0]-=3600;if(row.metric.source_metric)row.value[1]=String(Number(row.value[1])-3600);}await assert.rejects(vectorReader(rows)(),/SAMPLE_STALE/);}
 const missing=vector().filter(row=>!row.metric.source_metric);await assert.rejects(vectorReader(missing)(),/READ_AMBIGUOUS/);
 const invalid=vector();invalid[0].value[0]='now';await assert.rejects(vectorReader(invalid)(),/TIME_INVALID/);
 const own=sample();own.observedAt=new Date(Date.now()+1000).toISOString();assert.throws(()=>assertRehearsalResourceSample(own),/TIME_INVALID/);
});
test('unknown numeric provider values cannot become zero for any metric',async()=>{
 for(const name of metrics)for(const invalid of ['',false,true,' ',[],['0'],'NaN','Infinity']){const rows=vector();rows.find(row=>row.metric.__name__===name).value[1]=invalid;await assert.rejects(vectorReader(rows)(),/VALUE_INVALID/);}
 const actual=await vectorReader(vector())();assert.equal(actual.oomEvents,0);
});
test('a stale low memory baseline cannot admit a child and current high memory cancels once',async()=>{
 const rows=vector();for(const row of rows){row.value[0]-=3600;if(row.metric.source_metric)row.value[1]=String(Number(row.value[1])-3600);}
 const denied=createRehearsalResourceGuard({readSample:vectorReader(rows)});await assert.rejects(denied.prepare(),/SAMPLE_STALE/);assert.throws(()=>denied.start(()=>assert.fail('no child')),/NOT_PREPARED/);
 let reads=0,poll,cancelled=0;const guard=createRehearsalResourceGuard({readSample:async()=>({...sample(),memoryUsed:reads++?3221225472:1000000000}),schedule:callback=>{poll=callback;return{};},unschedule:()=>{}});await guard.prepare();guard.start(()=>cancelled++);poll();assert.equal((await guard.stop()).code,'REHEARSAL_MEMORY_HEADROOM_STOP');assert.equal(cancelled,1);
});

test('memory and slower exporter age budgets are finite and checked independently',()=>{
 const slow=sample();for(const name of metrics.slice(2))slow.metricTimes[name].sampledAt-=60;assert.doesNotThrow(()=>assertRehearsalResourceSample(slow));
 const memory=sample();memory.metricTimes[metrics[0]].sampledAt-=30;assert.throws(()=>assertRehearsalResourceSample(memory),/SAMPLE_STALE/);
 const expired=sample();expired.metricTimes[metrics[3]].sampledAt-=90;assert.throws(()=>assertRehearsalResourceSample(expired),/SAMPLE_STALE/);
 const future=sample();future.metricTimes[metrics[0]].sampledAt+=20;assert.throws(()=>assertRehearsalResourceSample(future),/SAMPLE_STALE/);
});
