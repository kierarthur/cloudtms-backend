from capture import HERE,OP,S0,ONE,BOOK,P,put,dump,sha
import json,difflib
edits=[]
S1REC=P+'daily-s1-actual-effect-recorder-20261017/recorder.mjs'
S1CLOCK=P+'daily-s1-after-clock-adapter-20261017/clock-forward.mjs'
S1RUN=P+'daily-s1-after-clock-adapter-20261017/run.mjs'
OTHERREC=P+'daily-original-stages-after-response-clock-forward-20261017/recorder.mjs'
OTHERRUN=P+'daily-original-stages-root-effect-adapter-20261017/run.mjs'
OTHERVAL=P+'daily-original-stages-root-effect-adapter-20261017/validate.mjs'
EFFECT=P+'daily-b2-current-effect-20261017/'
def edit(owner,file,before,after):
 p=HERE/'payload'/owner/file;b=p.read_bytes();s=b.decode();assert s.count(before)==1,(file,before[:100]);n=s.replace(before,after,1).encode();p.write_bytes(n)
 edits.append(dict(owner=owner,file=file,before=before,after=after))
def reset(owner,file):put('payload/'+owner+'/'+file,(HERE/'parents'/owner/file).read_bytes())
def main():
 targets=[('report',OP+'worker.mjs'),('report',OP+'stages.mjs'),('report',S0+'phase-parent.mjs'),('report',ONE+'coordinate.mjs'),('report',ONE+'stage.mjs'),('report',ONE+'progress.mjs'),('report',BOOK+'core.mjs'),('report',BOOK+'bookkeeper.mjs'),('root',P+'daily-genuine-root-stage-bookkeeper-20261017.mjs'),*[('root',f) for f in [S1REC,S1RUN,OTHERREC,OTHERRUN,OTHERVAL]]]
 for owner,file in targets:reset(owner,file)
 for f in ['current-read.mjs','current-ipc.mjs']:put('payload/report/'+OP+f,(HERE/'runtime'/f).read_bytes())
 put('payload/root/'+EFFECT+'current-effect.mjs',(HERE/'runtime/current-effect.mjs').read_bytes())
 w=OP+'worker.mjs'
 edit('report',w,"import fs from 'node:fs';", "import crypto from 'node:crypto';\nimport {buildCurrent,currentFiles,validateCurrentEvidence,currentAttempt,makeConsumption,validateCurrentAfter} from './current-read.mjs';\nimport {waitCurrentResume} from './current-ipc.mjs';\nimport fs from 'node:fs';")
 edit('report',w,"let browser,closed=true,status='HOLD',code='FIXTURE_PHASE_HELD',activeNetwork,expectedWrites=0;", "let browser,closed=true,status='HOLD',code='FIXTURE_PHASE_HELD',activeNetwork,expectedWrites=0,current,currentResumeDispose,attemptBinding;")
 edit('report',w,"p==='BEFORE'&&phase==='qualify'?180000:30000", "p==='BEFORE'&&(phase==='qualify'||phase==='submit'&&stage!=='S0')?180000:30000")
 edit('report',w," const openBrowser=async()=>{", """ const readCurrent=()=>{
   check(current,'FIXTURE_CURRENT_READ_REQUIRED');const bytes=committedRootFile(current.evidenceFile);
   check(!current.evidenceSha256||current.evidenceSha256===sha(bytes),'FIXTURE_CURRENT_COMMITTED_EVIDENCE_CHANGED');
   const v=validateCurrentEvidence(JSON.parse(bytes),{...current.baseline,expectedBinding:current.binding,now:Date.now(),currentMaxAgeMs:30000});
   check(v.inputSha256===current.inputSha256,'FIXTURE_CURRENT_READY_INPUT_BYTES_CHANGED');current.evidenceSha256=sha(bytes);return {bytes,v};
  };
  const openBrowser=async()=>{""")
 edit('report',w,"try{assertActive();await turn.assertHeld('daily-genuine-'+stage+'-before-post-forward');assertActive();}catch{network.refuseTransport();return r.abort();}","""try{
      assertActive();await turn.assertHeld('daily-genuine-'+stage+'-before-post-forward');assertActive();
      check(attemptBinding&&current,'FIXTURE_CURRENT_PERSISTED_ATTEMPT_REQUIRED');readCurrent();
      // Durable eligibility evidence, never a claim that fetch forwarded or succeeded.
      writeClosed(out,currentFiles(stage).consumption,makeConsumption(current.binding,current.evidenceSha256,Date.now()));
      readCurrent();assertActive(); // Recheck both native clocks AFTER fsync and every awaited held/source callback.
     }catch{network.refuseTransport();return r.abort();}""")
 edit('report',w,"const b=privateBinding(),{page,network}=await openBrowser();await currentAdmission(page,b.actorId);network.assertClean(0);\n    // Recheck native freshness and exact committed entry AFTER real current actor admission.\n    load('BEFORE');await turn.assertHeld('daily-genuine-'+stage+'-before-attempt');assertActive();\n    persistAttempt(out,stage,{entrySha256:entrySha,beforeEnvelopeSha256:sha(before.bytes)});network.arm();expectedWrites=1;", """const b=privateBinding(),{page,network}=await openBrowser();await currentAdmission(page,b.actorId);network.assertClean(0);
     const admittedAt=new Date().toISOString();
     await turn.assertHeld('daily-genuine-'+stage+'-before-current-input');assertActive();
     const baseline={beforeBuilt:before.built,beforeInputBytes:read(preparedFile('BEFORE')),beforeBytes:before.bytes,entry,entrySha256:entrySha};
     const pending=buildCurrent({stage,identity:turn.identity,actorId:b.actorId,generated:beforeState.generated,...baseline,admittedAt,readyAt:new Date().toISOString(),nonce:crypto.randomBytes(16).toString('hex')});
     writeClosed(out,currentFiles(stage).input,pending.built);writeClosed(out,currentFiles(stage).binding,pending.binding);
     const resumed=await waitCurrentResume(process,pending.ready,{signal:AbortSignal.any([turn.signal,abort.signal]),onViolation:onAbort});currentResumeDispose=resumed.dispose;
     current={binding:pending.binding,baseline,evidenceFile:resumed.evidenceFile,inputSha256:pending.ready.inputSha256};readCurrent();network.assertClean(0);
     await turn.assertHeld('daily-genuine-'+stage+'-before-attempt');assertActive();readCurrent();
     attemptBinding=currentAttempt(current.binding,current.evidenceSha256);persistAttempt(out,stage,attemptBinding);network.arm();expectedWrites=1;""")
 edit('report',w,"assertActive();saveQualification(out,stage,before.frame,after.frame,response.projection,beforeState);status='PASS';", """let currentBefore=null;
    if(stage!=='S0'){
     const {entry,effectFile}=JSON.parse(read(out+'/'+stage+'_ENTRY_BINDING.json')),effectBytes=committedRootFile(effectFile);
     const entrySha256=assertSetupEntry(committedRootFile('docs/healthroster-build/TEST_SETUP_MANIFEST.md').toString(),entry,stage,before.bytes,before.built.receipt,effectBytes);
     const f=currentFiles(stage),currentEvidenceBytes=committedRootFile(f.evidence),consumptionBytes=committedRootFile(f.consumptionEvidence);
     currentBefore=validateCurrentAfter({currentEvidenceBytes,consumptionBytes,beforeBuilt:before.built,beforeInputBytes:read(preparedFile('BEFORE')),beforeBytes:before.bytes,entry,entrySha256,response});
     check(afterEffect.currentReadEvidence===f.evidence&&afterEffect.currentConsumptionEvidence===f.consumptionEvidence&&afterEffect.currentEvidenceSha256===sha(currentEvidenceBytes)&&afterEffect.currentConsumptionSha256===sha(consumptionBytes),'FIXTURE_CURRENT_AFTER_EFFECT_BINDING_REQUIRED');
    }
    assertActive();saveQualification(out,stage,before.frame,after.frame,response.projection,beforeState,currentBefore);status='PASS';""")
 edit('report',w,"clearTimeout(timer);process.removeListener('message',onMessage);", "clearTimeout(timer);currentResumeDispose?.();process.removeListener('message',onMessage);")
 s=OP+'stages.mjs'
 edit('report',s,"import {scopes} from './native-read.mjs';", "import {scopes} from './native-read.mjs';\nimport {assertCurrentPrefix} from './current-read.mjs';")
 edit('report',s,"check(Array.isArray(j.qualified)&&j.qualified.every((s,i)=>s===STAGES[i]),'FIXTURE_QUALIFIED_PREFIX_REQUIRED');return j;", "check(Array.isArray(j.qualified)&&j.qualified.every((s,i)=>s===STAGES[i]),'FIXTURE_QUALIFIED_PREFIX_REQUIRED');if(j.qualified.length>1)assertCurrentPrefix(j.currentBefore,j.qualified.at(-1));return j;")
 edit('report',s,"export function saveQualification(directory,stage,before,after,projection,previous){", "export function saveQualification(directory,stage,before,after,projection,previous,currentBefore=null){\n if(stage!=='S0')assertCurrentPrefix(currentBefore,stage);")
 edit('report',s,"{stage,after,projection});", "{stage,after,projection,...stage==='S0'?{}:{currentBefore}});")
 edit('report',s,"const value={qualified,after,generated:{...previous.generated,...projection}};", "const value={qualified,after,generated:{...previous.generated,...projection},...stage==='S0'?{}:{currentBefore}};")
 edit('report',S0+'phase-parent.mjs',"runOwnedTurnChild,treeFactory=ownedWorker}","runOwnedTurnChild,treeFactory=ownedWorker,currentNative=null}")
 edit('report',S0+'phase-parent.mjs',"const tree=treeFactory();let execution;", "const tree=treeFactory();let execution;const spawnOwned=(...args)=>{const child=tree.spawn(...args);currentNative?.attach(child);return child;};")
 edit('report',S0+'phase-parent.mjs',"spawnChild:tree.spawn,resourceGuard:tree", "spawnChild:spawnOwned,resourceGuard:tree")
 edit('report',S0+'phase-parent.mjs',"finally{await tree.confirm();}","finally{await tree.confirm();currentNative?.close();}")
 edit('report',S0+'phase-parent.mjs',"return {...r,ownedDirectory:out", "if(r.status==='PASS')currentNative?.assertCompleted();\n  return {...r,ownedDirectory:out")
 edit('report',ONE+'stage.mjs',"runOwnedTurnChild,treeFactory}){", "runOwnedTurnChild,treeFactory,currentNative}){")
 edit('report',ONE+'stage.mjs',"runOwnedTurnChild,treeFactory});", "runOwnedTurnChild,treeFactory,currentNative});")
 edit('report',ONE+'coordinate.mjs',"import {source} from './source.mjs';", "import {parentCurrentBridge} from '../daily-health-genuine-staged-operator-20261017/current-ipc.mjs';\nimport {source} from './source.mjs';")
 edit('report',ONE+'coordinate.mjs',"const report=status=>console.log(JSON.stringify({status,stage,", "const report=(status,current=null)=>console.log(JSON.stringify({status,...current?{current}:{},stage,")
 edit('report',ONE+'coordinate.mjs',"phase:async phase=>{const r=await runPhase({phase,stage,out,", "phase:async phase=>{const currentNative=phase==='submit'?parentCurrentBridge({stage,source:sourceCommit,controls,signal,assertHeld:turn.assertHeld,announce:ready=>report('CURRENT_NATIVE_READY',ready)}):null;const r=await runPhase({phase,stage,out,currentNative,")
 edit('report',ONE+'progress.mjs',"'SETUP_ENTRY_READY','AFTER_READY'", "'SETUP_ENTRY_READY','CURRENT_NATIVE_READY','AFTER_READY'")
 edit('report',ONE+'progress.mjs',"import fs from 'node:fs';", "import {assertCurrentReady} from '../daily-health-genuine-staged-operator-20261017/current-read.mjs';\nimport fs from 'node:fs';")
 edit('report',ONE+'progress.mjs',"check(ready.has(r.status)&&Object.keys(r).sort().join(',')===", "if(r.status==='CURRENT_NATIVE_READY'){assertCurrentReady(r.current,{stage,source:sourceCommit});}\n  check(ready.has(r.status)&&Object.keys(r).filter(k=>r.status==='CURRENT_NATIVE_READY'?k!=='current':true).sort().join(',')===")
 edit('report',ONE+'progress.mjs',"remainingWorkMs:found.remainingWorkMs}:null;", "remainingWorkMs:found.remainingWorkMs,...found.status==='CURRENT_NATIVE_READY'?{current:found.current}:{}}:null;")
 # Core and both actual/full root caller postimages are coupled below, no transport is added.
 for owner,b in [('report',BOOK+'bookkeeper.mjs'),('root',P+'daily-genuine-root-stage-bookkeeper-20261017.mjs')]:
  edit(owner,b,"['BEFORE','SETUP','AFTER','PUBLISH'].includes(phase)","['BEFORE','SETUP','CURRENT','AFTER','PUBLISH'].includes(phase)")
  edit(owner,b,"stage+'_COORDINATOR_SUMMARY.json']);", "stage+'_COORDINATOR_SUMMARY.json',stage+'_CURRENT_INPUT.json',stage+'_CURRENT_BINDING.json',stage+'_CURRENT_NATIVE.json',stage+'_CURRENT_TIMING.json',stage+'_CURRENT_CONSUMPTION.json']);")
  edit(owner,b,"owners={...contract,...native,...stagesOwner};", "currentOwner=await import(pathToFileURL(report+'/'+operator+'/current-read.mjs').href),owners={...contract,...native,...stagesOwner,...currentOwner};")
  edit(owner,b,"bytes:Buffer.from(JSON.stringify(value,null,2)+'\\n')", "bytes:Buffer.isBuffer(value)?Buffer.from(value):Buffer.from(JSON.stringify(value,null,2)+'\\n')")
  edit(owner,b,"}else if(phase==='AFTER'){", """}else if(phase==='CURRENT'){
  const b=get('BEFORE'),entry=JSON.parse(read(stage+'_ENTRY_BINDING.json')).entry,effectBytes=readPublic(effectFile);
  const entrySha256=owners.assertSetupEntry(readPublic('docs/healthroster-build/TEST_SETUP_MANIFEST.md').toString(),entry,stage,b.bytes,b.built.receipt,effectBytes);
  must(entry.sourceCommit===source,'BOOKKEEPER_CURRENT_SOURCE_REQUIRED');
  const record=owners.makeCurrentEvidence({binding:JSON.parse(read(stage+'_CURRENT_BINDING.json')),builtBytes:read(stage+'_CURRENT_INPUT.json'),nativeBytes:read(stage+'_CURRENT_NATIVE.json'),timing:JSON.parse(read(stage+'_CURRENT_TIMING.json')),beforeBuilt:b.built,beforeInputBytes:read(stage+'_BEFORE_INPUT.json'),beforeBytes:b.bytes,entry,entrySha256,now:Date.now()});
  write(plan.currentReadEvidence,record);
 }else if(phase==='AFTER'){""")
  edit(owner,b,"write(plan.afterEvidence,afterProof({", """const entry=JSON.parse(read(stage+'_ENTRY_BINDING.json')).entry,effectBytes=readPublic(effectFile);
  const entrySha256=owners.assertSetupEntry(readPublic('docs/healthroster-build/TEST_SETUP_MANIFEST.md').toString(),entry,stage,b.bytes,b.built.receipt,effectBytes);
  const currentEvidenceBytes=readPublic(plan.currentReadEvidence),consumptionBytes=readPublic(plan.currentConsumptionEvidence);must(consumptionBytes.equals(read(stage+'_CURRENT_CONSUMPTION.json')),'BOOKKEEPER_COMMITTED_CONSUMPTION_BYTES_REQUIRED');
  write(plan.afterEvidence,afterProof({entry,entrySha256,beforeInputBytes:read(stage+'_BEFORE_INPUT.json'),currentEvidenceBytes,consumptionBytes,""")
  edit(owner,b,"write(plan.completedEvidence,publishPrefix({", """const currentPublish=stage==='S0'?{}:(()=>{const data=get('BEFORE'),entry=JSON.parse(read(stage+'_ENTRY_BINDING.json')).entry,effectBytes=readPublic(effectFile);return {beforeBuilt:data.built,beforeInputBytes:read(stage+'_BEFORE_INPUT.json'),entry,entrySha256:owners.assertSetupEntry(readPublic('docs/healthroster-build/TEST_SETUP_MANIFEST.md').toString(),entry,stage,data.bytes,data.built.receipt,effectBytes)};})();
  write(plan.completedEvidence,publishPrefix({...currentPublish,currentEvidenceBytes:stage==='S0'?null:readPublic(plan.currentReadEvidence),consumptionBytes:stage==='S0'?null:readPublic(plan.currentConsumptionEvidence),""")
  expected=[S1REC,S1CLOCK,S1RUN,OTHERREC,OTHERRUN,OTHERVAL,EFFECT+'current-effect.mjs']
  edit(owner,b,"if(phase==='BEFORE'){", """if(phase==='CURRENT'){
  const expectedEffectFiles="""+json.dumps(expected,separators=(',',':'))+""",effectCommit=env.HR_ROOT_HELPER_COMMIT;
  must(/^[a-f0-9]{40}$/.test(effectCommit??''),'BOOKKEEPER_B2_EFFECT_SOURCE_COMMIT_REQUIRED');git(root,'merge-base','--is-ancestor',effectCommit,rootHead);
  const file='"""+EFFECT+"""SOURCE_SELECTION.json',selectionBytes=readPublic(file),selection=JSON.parse(selectionBytes);
  must(selectionBytes.equals(git(root,'show',effectCommit+':'+file))&&Object.keys(selection).sort().join(',')==='files,version'&&selection.version==='DAILY_B2_EXACT_EFFECT_SOURCE_V1'&&JSON.stringify(selection.files.map(r=>r.file))===JSON.stringify(expectedEffectFiles),'BOOKKEEPER_B2_EFFECT_SELECTION_REQUIRED');
  for(const r of selection.files){const b=readPublic(r.file);must(Object.keys(r).sort().join(',')==='bytes,file,sha256'&&Number.isSafeInteger(r.bytes)&&r.bytes>0&&/^[a-f0-9]{64}$/.test(r.sha256)&&b.length===r.bytes&&sha(b)===r.sha256&&b.equals(git(root,'show',effectCommit+':'+r.file)),'BOOKKEEPER_B2_EFFECT_SOURCE_CHANGED');}
 }
 if(phase==='BEFORE'){""")
 core=BOOK+'core.mjs'
 edit('report',core,"must(keys(plan,['version'", "must(keys(plan,[...stage==='S0'?[]:['currentReadEvidence','currentConsumptionEvidence'],'version'")
 edit('report',core,"plan.version==='DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V1'", "plan.version===(stage==='S0'?'DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V1':'DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V2')")
 edit('report',core,"const prefix=stages.slice(0,stages.indexOf(stage));", "if(stage!=='S0')must(plan.currentReadEvidence===publicBase+'DAILY_GENUINE_'+stage+'_CURRENT_READ_20261017.json'&&plan.currentConsumptionEvidence===publicBase+'DAILY_GENUINE_'+stage+'_CURRENT_CONSUMPTION_20261017.json','BOOKKEEPER_EXACT_CURRENT_PATHS_REQUIRED');\n const prefix=stages.slice(0,stages.indexOf(stage));")
 edit('report',core,"return {qualified:plan.qualifiedPrefix,after:prior.afterFrame};", "if(prior.stage!=='S0')must(prior.currentBefore?.version==='DAILY_GENUINE_B2_CONSUMED_BINDING_V1'&&prior.currentBefore.stage===prior.stage&&prior.currentBefore.operatorSourceCommit===plan.operatorSourceCommit,'BOOKKEEPER_CURRENT_PREFIX_REQUIRED');\n return {qualified:plan.qualifiedPrefix,after:prior.afterFrame};")
 edit('report',core,"afterEffect,readPublic,now},o){", "afterEffect,readPublic,now,entry,entrySha256,beforeInputBytes,currentEvidenceBytes,consumptionBytes},o){")
 edit('report',core,"const refs=['canonicalAfterEvidence'", """const currentBefore=o.validateCurrentAfter({currentEvidenceBytes,consumptionBytes,beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,response,now});
 must(afterEffect.currentReadEvidence===plan.currentReadEvidence&&afterEffect.currentConsumptionEvidence===plan.currentConsumptionEvidence&&afterEffect.currentEvidenceSha256===o.sha(currentEvidenceBytes)&&afterEffect.currentConsumptionSha256===o.sha(consumptionBytes),'BOOKKEEPER_CURRENT_AFTER_EFFECT_REQUIRED');
 const refs=['canonicalAfterEvidence'""")
 edit('report',core,"originalChildQualification:'PENDING_ORIGINAL_QUALIFY',", "originalChildQualification:'PENDING_ORIGINAL_QUALIFY',currentBefore,")
 edit('report',core,"beforeBytes,afterBytes,response},o){", "beforeBytes,afterBytes,response,currentEvidenceBytes,consumptionBytes,beforeBuilt,beforeInputBytes,entry,entrySha256},o){")
 edit('report',core,"return {version:'DAILY_GENUINE_PUBLIC_COMPLETED_PREFIX_V1'", """if(stage!=='S0'){
  o.assertCurrentPrefix(after.currentBefore,stage);
  const currentBefore=o.validateCurrentAfter({currentEvidenceBytes,consumptionBytes,beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,response,now:Date.parse(after.validatedAt)});
  must(after.currentBefore.operatorSourceCommit===source&&same(currentBefore,after.currentBefore),'BOOKKEEPER_CURRENT_PUBLISH_BINDING_REQUIRED');
 }
 return {version:'DAILY_GENUINE_PUBLIC_COMPLETED_PREFIX_V1',...stage==='S0'?{}:{currentBefore:after.currentBefore}""")
 # Upgrade the actual effect composers/callers, so no old four-record path is admitted before POST.
 wrapper="import {bindAfterCurrent} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/"+EFFECT+"current-effect.mjs';\n"
 edit('root',S1REC,"import{createHash}from'node:crypto';",wrapper+"import{createHash}from'node:crypto';")
 edit('root',S1REC,"'auxAfterBytes','now']", "'auxAfterBytes','now','beforeInputBytes','consumptionBytes']")
 edit('root',S1REC,"const nextPlan={...plan", "const current=bindAfterCurrent({stage:'S1',plan,...a});\n const nextPlan={...plan")
 edit('root',S1REC,"effect={version:'DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1'", "effect={...current.afterFields,version:'DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1'")
 edit('root',S1REC,"bytes:json(effect)},{file:planFile", "bytes:json(effect)},current.consumptionRecord,{file:planFile")
 edit('root',S1REC,"return afterProof({stage:'S1'", "const current=bindAfterCurrent({stage:'S1',plan:p.plan,...a});\n return afterProof({...current.afterArgs,stage:'S1'")
 edit('root',S1REC,"{readEnvelope,assertBefore,assertAfter,sha});", "{readEnvelope,assertBefore,assertAfter,sha,...current.afterOwners});")
 edit('root',OTHERREC,"import {createHash} from 'node:crypto';", wrapper+"import {createHash} from 'node:crypto';")
 edit('root',OTHERREC,"exact(a,[...fields,...auxiliary?auxFields:[]])", "exact(a,[...fields,'beforeInputBytes','consumptionBytes',...auxiliary?auxFields:[]])")
 edit('root',OTHERREC,"const effect={version:'DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1'", "const current=bindAfterCurrent({plan,...a});\n const effect={...current.afterFields,version:'DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1'")
 edit('root',OTHERREC,"bytes:json(effect)},{file:a.planFile", "bytes:json(effect)},current.consumptionRecord,{file:a.planFile")
 edit('root',OTHERREC,"size===4&&records.every", "size===5&&records.every")
 edit('root',OTHERREC,"'FOUR_DISTINCT_PUBLIC_RECORD_PATHS_REQUIRED'", "'FIVE_DISTINCT_PUBLIC_RECORD_PATHS_REQUIRED'")
 edit('root',OTHERVAL,"import {createHash} from 'node:crypto';", wrapper+"import {createHash} from 'node:crypto';")
 edit('root',OTHERVAL,"return afterProof({stage:a.stage", "const current=bindAfterCurrent({plan,...a});\n return afterProof({...current.afterArgs,stage:a.stage")
 edit('root',OTHERVAL,"{readEnvelope,assertBefore,assertAfter,sha});", "{readEnvelope,assertBefore,assertAfter,sha,...current.afterOwners});")
 for f,stage in [(S1RUN,'S1'),(OTHERRUN,None)]:
  token="'S1_RESPONSE.json'" if stage else "a.stage+'_RESPONSE.json'"
  edit('root',f,token+",...auxiliary" if not stage else token+",'S1_AUX_BEFORE_INPUT.json'",token+",a.stage+'_CURRENT_CONSUMPTION.json',...auxiliary" if not stage else token+",'S1_CURRENT_CONSUMPTION.json','S1_AUX_BEFORE_INPUT.json'")
  prefix="a.stage" if not stage else "'S1'"
  old="beforeBuilt:JSON.parse(read("+prefix+"+'_BEFORE_INPUT.json'))" if not stage else "beforeBuilt:JSON.parse(read('S1_BEFORE_INPUT.json'))"
  new=old+",beforeInputBytes:read("+prefix+"+'_BEFORE_INPUT.json'),consumptionBytes:read("+prefix+"+'_CURRENT_CONSUMPTION.json')"
  edit('root',f,old,new)
  edit('root',f,"files.length===4&&new Set(files).size===4", "files.length===5&&new Set(files).size===5")
  edit('root',f,"'EXACT_FOUR_PUBLIC_RECORDS_REQUIRED'", "'EXACT_FIVE_PUBLIC_RECORDS_REQUIRED'")
 effectRows=[]
 for f in [S1REC,S1CLOCK,S1RUN,OTHERREC,OTHERRUN,OTHERVAL,EFFECT+'current-effect.mjs']:
  b=(HERE/'payload/root'/f).read_bytes();effectRows.append(dict(file=f,bytes=len(b),sha256=sha(b)))
 dump('payload/root/'+EFFECT+'SOURCE_SELECTION.json',dict(version='DAILY_B2_EXACT_EFFECT_SOURCE_V1',files=effectRows))
 dump('EDITS.json',dict(version='B2_LITERAL_COMPLETE_PARENT_REVERSALS_V1',hunks=edits))
 rows=[]
 for owner,file in targets:
  a=(HERE/'parents'/owner/file).read_bytes();b=(HERE/'payload'/owner/file).read_bytes()
  rows.append(dict(owner=owner,file=file,beforeBytes=len(a),beforeSha256=sha(a),bytes=len(b),sha256=sha(b)))
  put('diff/'+owner+'/'+file+'.diff',''.join(difflib.unified_diff(a.decode().splitlines(True),b.decode().splitlines(True),fromfile=file,tofile=file)).encode())
 for f in ['current-read.mjs','current-ipc.mjs']:
  b=(HERE/'payload/report'/OP/f).read_bytes();rows.append(dict(owner='report',file=OP+f,parent='ABSENT',bytes=len(b),sha256=sha(b)))
 for f in [EFFECT+'current-effect.mjs',EFFECT+'SOURCE_SELECTION.json']:
  b=(HERE/'payload/root'/f).read_bytes();rows.append(dict(owner='root',file=f,parent='ABSENT',bytes=len(b),sha256=sha(b)))
 dump('SELECTION.json',dict(version='B2_UNMOUNTED_SOURCE_SELECTION_V1',targets=rows,sourceAdmission='HELD_REQUIRES_REAL_SUCCESSOR_RESEAL_NOT_FAKE_COMMIT'))
 print(json.dumps(dict(targets=len(rows),hunks=len(edits))))
if __name__=='__main__':main()
