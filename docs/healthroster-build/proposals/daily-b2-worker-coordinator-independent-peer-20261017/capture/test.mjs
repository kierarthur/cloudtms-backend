// Source-only synthetic regressions. Never native/HTTP/browser/pipe/real-Git/credential proof.
import test from 'node:test';import assert from 'node:assert/strict';import {EventEmitter} from 'node:events';import fs from 'node:fs';
import {fixture,fixtureS0,envelope,c,n,b2,core,source,NOW,here,OP,BK,at,ACTOR} from './test-support.mjs';
import {rootMachine,runAdapter,runBookkeeper} from './offline-root.mjs';import {submitWorker,qualifyWorker} from './offline-worker.mjs';
import {waitCurrentResume,parentCurrentBridge} from './payload/report/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/current-ipc.mjs';
const stages=['S1','S1a','S2','S3','S4'];
function afterCapture(f,m){
 const aux=m.out+'/'+f.stage+'_AUX_AFTER_NATIVE.json';if(m.files.has(aux)){const result=JSON.parse(m.get(aux));for(const s of [result.statements[1],result.rollbackVerification.statements[0]])Object.values(s.rows[0])[0].observedAt=new Date().toISOString();m.put(aux,b2.jsonBytes(result));}
 const built=JSON.parse(m.get(m.out+'/'+f.stage+'_AFTER_INPUT.json')),result=envelope(built,f.after,[0,0]);for(const s of [result.statements[1],result.rollbackVerification.statements[0]])s.rows[0].fixture_read.observedAt=new Date().toISOString();m.save(f.stage+'_AFTER_NATIVE.json',result);
}
for(const stage of stages)test(stage+': complete actual submit → CURRENT root → guarded onePOST → five-record actual adapter → AFTER root → worker qualify → PUBLISH',async()=>{
 const f=fixture(stage),m=rootMachine(f),b1=Buffer.from(m.get(m.out+'/'+stage+'_BEFORE_NATIVE.json')),entry=Buffer.from(m.get(m.out+'/'+stage+'_ENTRY_BINDING.json'));
 const sent=await submitWorker(f,m);assert.equal(sent.result.status,'PASS');assert.equal(sent.forwarded,1);assert.equal(sent.rootError,undefined);
 const events=sent.events,held=events.findIndex(x=>x[0]==='held'&&x[1].endsWith('before-post-forward')),consumed=events.findIndex(x=>x[0]==='open'&&x[1]===stage+'_CURRENT_CONSUMPTION.json'),forward=events.findIndex(x=>x[0]==='forward'),attempt=events.findIndex(x=>x[0]==='open'&&x[1]===stage+'_ATTEMPT.json');assert(attempt>=0&&held>attempt&&consumed>held&&forward>consumed);
 const privateAttempt=m.get(m.out+'/'+stage+'_ATTEMPT.json'),consumption=JSON.parse(m.get(m.out+'/'+stage+'_CURRENT_CONSUMPTION.json'));assert.equal(consumption.attemptBindingSha256,c.sha(privateAttempt));assert.equal(consumption.boundary,'ELIGIBILITY_ONLY_BEFORE_ORIGINAL_ROUTE_FETCH');
 afterCapture(f,m);const adopted=await runAdapter(f,m);assert.equal(adopted.files.length,5);assert(m.get(m.ROOT+'/'+f.plan.currentConsumptionEvidence).equals(m.get(m.out+'/'+stage+'_CURRENT_CONSUMPTION.json')));
 await runBookkeeper(f,m,'AFTER');const q=await qualifyWorker(f,m);assert.equal(q.result.status,'PASS');assert.equal(q.journal.currentBefore.currentEvidenceSha256,c.sha(m.get(m.ROOT+'/'+f.plan.currentReadEvidence)));
 m.save(stage+'_COORDINATOR_SUMMARY.json',{stage,operatorSourceCommit:source,status:'GENUINE_STAGE_NATIVE_QUALIFIED_OWN_TURN_RELEASED',oneBusinessRequestQualified:true,ownedDirectory:m.out});const published=await runBookkeeper(f,m,'PUBLISH');assert.equal(published.status,'ROOT_GENUINE_'+stage+'_PUBLISH_COMMITTED');
 const prefix=JSON.parse(m.get(m.ROOT+'/'+f.plan.completedEvidence));assert.equal(prefix.currentBefore.nonce,consumption.nonce);assert.deepEqual(Array.from(prefix.qualified),Array.from([...f.plan.qualifiedPrefix,stage]));assert(b1.equals(m.get(m.out+'/'+stage+'_BEFORE_NATIVE.json')));assert(entry.equals(m.get(m.out+'/'+stage+'_ENTRY_BINDING.json')));
});
test('B1 entry retains30s: old baseline cannot create setup, while submit requires fresh mandatory B2',async()=>{
 const f=fixture(),m=rootMachine(f);const e=await qualifyWorker(f,m,{phase:'entry'});assert.equal(e.result.status,'HOLD');assert.equal(e.result.code,'FIXTURE_FRESH_NATIVE_CLOCK_REQUIRED');assert(!m.files.has(m.out+'/S1_SETUP_ENTRY.md'));
 for(const stage of stages){const f=fixture(stage);assert.throws(()=>n.readEnvelope(JSON.parse(f.beforeBytes),f.beforeBuilt.receipt,{now:NOW,maxAgeMs:30000}));assert.doesNotThrow(()=>n.readEnvelope(JSON.parse(f.beforeBytes),f.beforeBuilt.receipt,{now:NOW,maxAgeMs:180000}));}
});
for(const mode of ['reused-b1','changed-scope','old-effect-source','wrong-resume','duplicate-resume','age-during-held','age-during-consumption-fsync','held-loss'])test('actual submit refusal: '+mode,async()=>{
 const f=fixture(),m=rootMachine(f),r=await submitWorker(f,m,{mode});assert.equal(r.result.status,'HOLD');assert.equal(r.forwarded,0);assert(!m.files.has(m.out+'/S1_QUALIFIED.json'));
 if(['age-during-held','age-during-consumption-fsync','held-loss'].includes(mode))assert(m.files.has(m.out+'/S1_ATTEMPT.json'));
 if(mode==='age-during-consumption-fsync')assert(m.files.has(m.out+'/S1_CURRENT_CONSUMPTION.json'));
});
test('lost actual POST reply never manufactures success or clears one-shot attempt',async()=>{
 const f=fixture(),m=rootMachine(f),r=await submitWorker(f,m,{mode:'lost-reply'});assert.equal(r.forwarded,1);assert.equal(r.result.status,'HOLD');assert(m.files.has(m.out+'/S1_ATTEMPT.json'));assert(m.files.has(m.out+'/S1_RECONCILE_INPUT.json'));assert(!m.files.has(m.out+'/S1_QUALIFIED.json'));
});
test('same original SQL/verification/limits and fullseven identity; only new outer labels differ',()=>{
 for(const stage of stages){const f=fixture(stage);assert.equal(f.pending.built.input.sql,f.beforeBuilt.input.sql);assert.equal(f.pending.built.input.verification_sql,f.beforeBuilt.input.verification_sql);assert.equal(f.pending.built.receipt.phase,'BEFORE');assert.deepEqual(f.pending.built.receipt.identity,f.beforeBuilt.receipt.identity);const copy=structuredClone(f.pending.built);copy.input.label=f.beforeBuilt.input.label;copy.receipt.label=f.beforeBuilt.receipt.label;assert.deepEqual(copy,f.beforeBuilt);assert.notEqual(f.pending.built.receipt.label,f.beforeBuilt.receipt.label);}
 assert.equal(n.scopes.length,11);assert.equal(n.routineSelection.length,55);
});
test('root accepts causal nonce ordering without assuming host/native clock alignment',()=>{
 const f=fixture(),r=structuredClone(f.record),native=JSON.parse(r.nativeResultText);for(const s of [native.statements[1],native.rollbackVerification.statements[0]])s.rows[0].fixture_read.observedAt=at(-8000);r.nativeResultText=JSON.stringify(native);assert.doesNotThrow(()=>b2.validateCurrentEvidence(r,{...f.baseline,expectedBinding:f.pending.binding,now:NOW}));
});
for(const key of n.scopes)test('B2 refuses equal-count changed full scope digest: '+key,()=>{
 const f=fixture(),r=structuredClone(f.record),native=JSON.parse(r.nativeResultText);for(const s of [native.statements[1],native.rollbackVerification.statements[0]])s.rows[0].fixture_read.scope[key].sha256=c.sha('changed-'+key);r.nativeResultText=JSON.stringify(native);assert.throws(()=>b2.validateCurrentEvidence(r,{...f.baseline,now:NOW}));
});
for(const area of ['facts','ids','catalogueSha256','actor','turn','source','entry','effect','label','sql','fresh','mainClock','freshClock','unknownReadyKey'])test('closed/current substitution refusal: '+area,()=>{
 const f=fixture(),r=structuredClone(f.record),native=JSON.parse(r.nativeResultText),frames=[native.statements[1].rows[0].fixture_read,native.rollbackVerification.statements[0].rows[0].fixture_read];
 if(area==='facts')for(const v of frames)v.facts.rate=!v.facts.rate;
 if(area==='ids')for(const v of frames)v.ids.settings=['e7d84371-0000-0000-0000-000000000099'];
 if(area==='catalogueSha256')for(const v of frames)v.catalogueSha256=c.sha('changed');
 if(area==='actor')r.binding.actorBindingSha256=c.sha('other');if(area==='turn')r.binding.identity.task='DAILY_GENUINE_S2_STAGED_ONLY';if(area==='source')r.binding.operatorSourceCommit='f'.repeat(40);if(area==='entry')r.binding.entrySha256=c.sha('other');if(area==='effect')r.binding.effectAdmissionSha256=c.sha('other');
 if(area==='label')native.label=f.beforeBuilt.receipt.label;if(area==='sql'){const built=JSON.parse(r.inputText);built.input.sql+='select 1;';r.inputText=JSON.stringify(built)+'\n';}
 if(area==='fresh')native.rollbackVerification.connectedFresh=false;if(area==='mainClock')frames[0].observedAt=at(-31000);if(area==='freshClock')frames[1].observedAt=at(-31000);
 if(area==='unknownReadyKey'){const ready=structuredClone(f.pending.ready);ready.binding.identity.actorId=ACTOR;assert.throws(()=>b2.assertCurrentReady(ready));return;}
 r.nativeResultText=JSON.stringify(native);assert.throws(()=>b2.validateCurrentEvidence(r,{...f.baseline,now:NOW}));
});
test('AFTER allows historical B2 but requires actual consumption-time currentness, not current30 after65sPOST',()=>{
 const f=fixture();assert.doesNotThrow(()=>b2.validateCurrentAfter({...f.baseline,currentEvidenceBytes:f.currentEvidenceBytes,consumptionBytes:f.consumptionBytes,response:{...f.response,finishedAt:at(65000)},now:NOW+66000}));
 const c1=JSON.parse(f.consumptionBytes);c1.checkedAt=at(40000);assert.throws(()=>b2.validateCurrentAfter({...f.baseline,currentEvidenceBytes:f.currentEvidenceBytes,consumptionBytes:b2.jsonBytes(c1),response:{...f.response,finishedAt:at(65000)},now:NOW+66000}));
});
test('db533 fixed age modes reject arbitrary widening, coercion, nonfinite and omitted-null bounds',()=>{
 const f=fixture();for(const bound of [30000,180000])assert.doesNotThrow(()=>b2.validateCurrentEvidence(f.record,{...f.baseline,now:NOW,currentMaxAgeMs:bound}));
 for(const bound of [0,29999,30001,179999,180001,Infinity,NaN,'30000','180000',null,false])assert.throws(()=>b2.validateCurrentEvidence(f.record,{...f.baseline,now:NOW,currentMaxAgeMs:bound}));
});
test('db533 qualified prefix requires every original typed seven-field turn, exact stage and fixed evidence paths',()=>{
 for(const stage of stages){const f=fixture(stage),v=b2.validateCurrentAfter({...f.baseline,currentEvidenceBytes:f.currentEvidenceBytes,consumptionBytes:f.consumptionBytes,response:f.response,now:NOW});assert.doesNotThrow(()=>b2.assertCurrentPrefix(v,stage));
  const reject=fn=>{const x=structuredClone(v);fn(x);assert.throws(()=>b2.assertCurrentPrefix(x,stage));};
  for(const key of Object.keys(v.identity))reject(x=>delete x.identity[key]);
  for(const [key,bad] of Object.entries({physicalDatabase:'uofvkfi5',databaseOid:'1',backendPid:0,turnId:'hr_turn_bad',key:[20261013,2],takenAt:'yesterday',task:'DAILY_GENUINE_S0_STAGED_ONLY'}))reject(x=>{x.identity[key]=bad;});
  reject(x=>{x.identity.extra=true;});reject(x=>{x.stage='S0';});reject(x=>{x.currentReadEvidence+='x';});reject(x=>{x.currentConsumptionEvidence+='x';});
 }
});
for(const where of ['adapter','after','qualify','publish'])test('mandatory B2 cannot be omitted/substituted at '+where,async()=>{
 const f=fixture(),m=rootMachine(f);await runAdapter(f,m);
 if(where==='adapter'){m.files.delete(m.ROOT+'/'+f.plan.currentReadEvidence);await assert.rejects(runAdapter(f,m));return;}
 if(where==='after'){m.files.delete(m.ROOT+'/'+f.plan.currentConsumptionEvidence);await assert.rejects(runBookkeeper(f,m,'AFTER'));return;}
 await runBookkeeper(f,m,'AFTER');
 if(where==='qualify'){m.put(m.ROOT+'/'+f.plan.currentConsumptionEvidence,Buffer.from('{}'));assert.equal((await qualifyWorker(f,m)).result.status,'HOLD');return;}
 const q=await qualifyWorker(f,m);assert.equal(q.result.status,'PASS');m.save('S1_COORDINATOR_SUMMARY.json',{stage:'S1',operatorSourceCommit:source,status:'GENUINE_STAGE_NATIVE_QUALIFIED_OWN_TURN_RELEASED',oneBusinessRequestQualified:true,ownedDirectory:m.out});m.put(m.ROOT+'/'+f.plan.currentReadEvidence,Buffer.from('{}'));await assert.rejects(runBookkeeper(f,m,'PUBLISH'));
});
test('ordered existing IPC/Controls waits before announcing; duplicate/foreign resume and lost child fail',async()=>{
 const f=fixture(),child=new EventEmitter(),controller=new AbortController(),messages=[],held=[],controls={failure:null,pending:null,assertActive(){if(this.failure)throw this.failure;},wait(command){assert.equal(command,'CURRENT_NATIVE_SAVED');assert(!this.pending);return new Promise((a,b)=>{this.pending={resolve:a,reject:b};});},fail(code){this.failure=Error(code);this.pending?.reject(this.failure);this.pending=null;}};
 child.send=(m,cb)=>{messages.push(m);cb?.();};const bridge=parentCurrentBridge({stage:'S1',source,signal:controller.signal,controls,assertHeld:async p=>held.push(p),announce:ready=>{assert(controls.pending);assert.equal(ready.type,'CURRENT_NATIVE_READY');controls.pending.resolve();controls.pending=null;}});bridge.attach(child);child.emit('message',f.pending.ready);await new Promise(r=>setImmediate(r));bridge.close();assert.doesNotThrow(()=>bridge.assertCompleted());assert.equal(messages[0].nonce,f.pending.binding.nonce);assert.equal(held.length,2);
 let violated=0;const channel=new EventEmitter();channel.send=()=>{};const wait=waitCurrentResume(channel,f.pending.ready,{signal:controller.signal,onViolation:()=>violated++});channel.emit('message',messages[0]);const got=await wait;channel.emit('message',messages[0]);assert.equal(violated,1);got.dispose();
 const dead=new EventEmitter();dead.send=()=>{};const pending=waitCurrentResume(dead,f.pending.ready,{signal:controller.signal,onViolation:()=>violated++});dead.emit('disconnect');await assert.rejects(pending);
});
test('unchanged S0 submit/qualify/root PUBLISH has no mandatory CURRENT and no business POST',async()=>{
 const f=fixtureS0(),m=rootMachine(f);m.save('PRIVATE_BINDING.json',{identity:f.identity,actorId:ACTOR});for(const suffix of ['RESPONSE.json','AFTER_INPUT.json','AFTER_NATIVE.json','CURRENT_INPUT.json','CURRENT_BINDING.json','CURRENT_CONSUMPTION.json'])m.files.delete(m.out+'/S0_'+suffix);
 const sent=await qualifyWorker(f,m,{phase:'submit'});assert.equal(sent.result.status,'PASS');assert.equal(sent.result.code,'FIXTURE_READ_ONLY_S0_AWAITING_NATIVE_AFTER');assert(!m.files.has(m.out+'/S0_CURRENT_INPUT.json'));
 afterCapture(f,m);const b=JSON.parse(m.get(m.out+'/S0_BEFORE_NATIVE.json')),a=JSON.parse(m.get(m.out+'/S0_AFTER_NATIVE.json')),response=JSON.parse(m.get(m.out+'/S0_RESPONSE.json'));
 const proof=(phase,result,bytes)=>({stage:'S0',phase,operatorSourceCommit:source,actualFrame:result.rollbackVerification.statements[0].rows[0].fixture_read,nativeResultSha256:c.sha(bytes),responseProjectionSha256:c.sha(JSON.stringify(response.projection)),accepted:true,rolledBack:true,connectedFresh:true});
 m.put(m.ROOT+'/'+f.plan.beforeEvidence,b2.jsonBytes(proof('BEFORE',b,m.get(m.out+'/S0_BEFORE_NATIVE.json'))));m.put(m.ROOT+'/'+f.plan.afterEvidence,b2.jsonBytes(proof('AFTER',a,m.get(m.out+'/S0_AFTER_NATIVE.json'))));for(const path of f.plan.afterReviewedEvidence.slice(1))m.put(m.ROOT+'/'+path,b2.jsonBytes({synthetic:true}));
 m.put(m.ROOT+'/'+core.publicBase+'DAILY_GENUINE_S0_AFTER_EFFECT_QUALIFICATION_20261017.json',b2.jsonBytes({version:'DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1',stage:'S0',beforeEnvelopeSha256:c.sha(f.beforeBytes),afterEnvelopeSha256:c.sha(m.get(m.out+'/S0_AFTER_NATIVE.json')),responseProjectionSha256:c.sha(JSON.stringify(response.projection)),canonicalAfterEvidence:f.plan.afterEvidence,exactApprovedChangesEvidence:f.plan.afterReviewedEvidence[1],backgroundAttributionEvidence:f.plan.afterReviewedEvidence[2]}));
 const q=await qualifyWorker(f,m);assert.equal(q.result.status,'PASS');assert.equal(q.journal.currentBefore,undefined);m.save('S0_COORDINATOR_SUMMARY.json',{stage:'S0',operatorSourceCommit:source,status:'S0_NATIVE_QUALIFIED_OWN_TURN_RELEASED',businessRequests:0,ownedDirectory:m.out});assert.equal((await runBookkeeper(f,m,'PUBLISH')).status,'ROOT_GENUINE_S0_PUBLISH_COMMITTED');
});
