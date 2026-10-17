// The only process permitted to load the approved private browser state.
// No dotenv, credentials, auth login/refresh, screenshots, tracing, videos or raw logs.
import crypto from 'node:crypto';
import {buildCurrent,currentFiles,validateCurrentEvidence,currentAttempt,makeConsumption,validateCurrentAfter} from './current-read.mjs';
import {waitCurrentResume} from './current-ipc.mjs';
import fs from 'node:fs';import path from 'node:path';import {pathToFileURL,fileURLToPath} from 'node:url';
import {ROOT,FRONT,BACK,PRIVATE_STATE,PW,STAGES,route,request,check,sha,qualifyResponse,entryText} from './contract.mjs';
import {here,sourcePreflight,assertReviewedExecutionSource,ownedDirectory,physical,committedRootFile,git} from './source.mjs';
import {networkGuard,guardedRoute,currentAdmission} from './network.mjs';
import {buildReadInput,readEnvelope} from './native-read.mjs';
import {assertBefore,assertAfter,assertSetupEntry,assertEffectAdmission,acquireSession,readJournal,persistAttempt,writeClosed,saveQualification} from './stages.mjs';
const read=file=>{const s=fs.lstatSync(file);check(s.isFile()&&!s.isSymbolicLink()&&s.size<=2e6,'FIXTURE_SMALL_PRIVATE_INPUT_REQUIRED');return fs.readFileSync(file);};
export async function perform(options,{browserFactory=null}={}){
 const out=ownedDirectory(process.env.HEALTH_GENUINE_OWNED_OUT);check(physical(process.cwd())===out&&typeof process.send==='function','FIXTURE_CHILD_ONLY');
 sourcePreflight();const phase=options['--phase'],stage=options['--stage'];
 check(STAGES.includes(stage)&&['bind','prepare','entry','submit','qualify','reconcile'].includes(phase)
  &&new RegExp('^CLOSED_'+phase+'_'+stage+'_[a-f0-9]{16}\\.json$').test(options.resultName),'FIXTURE_CLOSED_PHASE_REQUIRED');
 const control=await import(pathToFileURL(ROOT+'/scripts/healthroster-turn-control.mjs').href);
 const turn=await control.connectTurnControl();let release;
 try{await turn.assertHeld('daily-genuine-'+stage+'-'+phase+'-begin');release=acquireSession(out);}catch(error){turn.close();throw error;}
 let browser,closed=true,status='HOLD',code='FIXTURE_PHASE_HELD',activeNetwork,expectedWrites=0,current,currentResumeDispose,attemptBinding;
 const abort=new AbortController(),deadline=Date.now()+110000;let timer;
 const assertActive=()=>check(!abort.signal.aborted&&Date.now()<deadline,'FIXTURE_BUDGET_HELD');
 const onAbort=()=>{abort.abort();void browser?.close().catch(()=>{});};
 process.on('message',onMessage);function onMessage(m){if(m==='ABORT')onAbort();}
 timer=setTimeout(onAbort,110000);turn.signal.addEventListener('abort',onAbort,{once:true});
 const bindingFile=out+'/PRIVATE_BINDING.json',nativeFile=p=>out+'/'+stage+'_'+p+'_NATIVE.json',preparedFile=p=>out+'/'+stage+'_'+p+'_INPUT.json';
 const prepared=p=>JSON.parse(read(preparedFile(p)));
 const load=p=>{const built=prepared(p),bytes=read(nativeFile(p));check(JSON.stringify(built.receipt.identity)===JSON.stringify(turn.identity),'FIXTURE_CURRENT_ORIGINAL_TURN_REQUIRED');return {built,bytes,frame:readEnvelope(JSON.parse(bytes),built.receipt,{maxAgeMs:p==='BEFORE'&&(phase==='qualify'||phase==='submit'&&stage!=='S0')?180000:30000})};};
 let beforeState;
 const privateBinding=()=>JSON.parse(read(bindingFile));
 const generate=(p,binding,generated=beforeState.generated)=>{
  const b=buildReadInput({stage,phase:p,identity:turn.identity,actorId:binding.actorId,
   settingsId:generated.settingsId??null,rateId:generated.rateId??null,timesheetId:generated.timesheetId??null});
  writeClosed(out,stage+'_'+p+'_INPUT.json',b);return b;
 };
 const readCurrent=()=>{
   check(current,'FIXTURE_CURRENT_READ_REQUIRED');const bytes=committedRootFile(current.evidenceFile);
   check(!current.evidenceSha256||current.evidenceSha256===sha(bytes),'FIXTURE_CURRENT_COMMITTED_EVIDENCE_CHANGED');
   const v=validateCurrentEvidence(JSON.parse(bytes),{...current.baseline,expectedBinding:current.binding,now:Date.now(),currentMaxAgeMs:30000});
   check(v.inputSha256===current.inputSha256,'FIXTURE_CURRENT_READY_INPUT_BYTES_CHANGED');current.evidenceSha256=sha(bytes);return {bytes,v};
  };
  const openBrowser=async()=>{
  assertActive();closed=false;
  const {chromium}=browserFactory??await import(PW),executable=physical(chromium.executablePath());
  check(executable.startsWith('D:/Development/Caches/ms-playwright/'),'FIXTURE_EXISTING_D_BROWSER_REQUIRED');
  browser=await chromium.launch({headless:true,executablePath:executable,timeout:15000,downloadsPath:out+'/downloads',
   args:['--disable-logging','--disable-crash-reporter','--disable-breakpad']});
  const context=await browser.newContext({baseURL:FRONT,storageState:PRIVATE_STATE,ignoreHTTPSErrors:false,serviceWorkers:'block',acceptDownloads:false});
  const network=networkGuard(stage);activeNetwork=network;
  await context.route('**/*',async r=>{
   // Original live A control is rechecked immediately before the single business POST can be forwarded.
   if(r.request().method()==='POST'&&r.request().url()===BACK+route[stage]){
    try{
      assertActive();await turn.assertHeld('daily-genuine-'+stage+'-before-post-forward');assertActive();
      check(attemptBinding&&current,'FIXTURE_CURRENT_PERSISTED_ATTEMPT_REQUIRED');readCurrent();
      // Durable eligibility evidence, never a claim that fetch forwarded or succeeded.
      writeClosed(out,currentFiles(stage).consumption,makeConsumption(current.binding,current.evidenceSha256,Date.now()));
      readCurrent();assertActive(); // Recheck both native clocks AFTER fsync and every awaited held/source callback.
     }catch{network.refuseTransport();return r.abort();}
   }
   return guardedRoute(r,network);
  });await context.routeWebSocket('**/*',s=>s.close());
  const page=await context.newPage();page.setDefaultTimeout(15000);page.setDefaultNavigationTimeout(20000);
  await page.goto('/');await page.waitForFunction(()=>typeof authFetch==='function',null,{timeout:20000});
  check(context.pages().length===1&&!abort.signal.aborted,'FIXTURE_PAGE_OR_BUDGET_HELD');
  return {page,network,context};
 };
 try{
  beforeState=readJournal(out);
  if(phase==='bind'){
   check(stage==='S0'&&!fs.existsSync(bindingFile),'FIXTURE_FRESH_BINDING_REQUIRED');
   const identity=turn.identity;control.assertTurnIdentity(identity);
   const {page,network}=await openBrowser(),admitted=await currentAdmission(page);network.assertClean(0);
   writeClosed(out,'PRIVATE_BINDING.json',{identity,actorId:admitted.actorId});
   generate('BEFORE',{identity,actorId:admitted.actorId});code='FIXTURE_BINDING_PREPARED';status='PASS';
  }else if(phase==='prepare'){
   const b=privateBinding();check(!fs.existsSync(out+'/'+stage+'_ATTEMPT.json'),'FIXTURE_STAGE_ALREADY_ATTEMPTED');
   check(STAGES[beforeState.qualified.length]===stage,'FIXTURE_STAGE_ORDER_REFUSED');generate('BEFORE',b);code='FIXTURE_NATIVE_READ_PREPARED';status='PASS';
  }else if(phase==='entry'){
   const before=load('BEFORE');assertBefore(stage,before.frame,beforeState);
   const effectFile=options['--effect-file'],effectBytes=committedRootFile(effectFile),effect=JSON.parse(effectBytes);
   const sourceCommit=git(here,'rev-parse','HEAD').toString().trim();
   const entry={sourceCommit,beforeSqlSha256:before.built.receipt.sqlSha256,verificationSqlSha256:before.built.receipt.verificationSqlSha256,beforeEnvelopeSha256:sha(before.bytes),effectAdmissionSha256:sha(effectBytes)};
   assertEffectAdmission(effect,stage,before.frame,entry);
   for(const k of ['nativeOwnerEvidence','schemaEffectEvidence','canonicalBeforeEvidence','reviewRecord'])committedRootFile(effect[k]);
   writeClosed(out,stage+'_ENTRY_BINDING.json',{entry,effectFile});
   fs.writeFileSync(out+'/'+stage+'_SETUP_ENTRY.md',entryText(stage,entry),{flag:'wx',mode:0o600});code='FIXTURE_COMMIT_EXACT_SETUP_ENTRY_NEXT';status='PASS';
  }else if(phase==='submit'){
   const {entry,effectFile}=JSON.parse(read(out+'/'+stage+'_ENTRY_BINDING.json'));
   const before=load('BEFORE');assertBefore(stage,before.frame,beforeState);
   const effects=committedRootFile(effectFile);assertEffectAdmission(JSON.parse(effects),stage,before.frame,entry);
   const manifest=committedRootFile('docs/healthroster-build/TEST_SETUP_MANIFEST.md').toString();
   const entrySha=assertSetupEntry(manifest,entry,stage,before.bytes,before.built.receipt,effects);
   assertReviewedExecutionSource(entry.sourceCommit);
   if(stage==='S0'){
    assertActive();
    persistAttempt(out,stage,{entrySha256:entrySha,beforeEnvelopeSha256:sha(before.bytes)});
    writeClosed(out,stage+'_RESPONSE.json',{kind:'NO_BUSINESS_REQUEST',projection:{},finishedAt:new Date().toISOString(),browserClosed:true});
    generate('AFTER',privateBinding());code='FIXTURE_READ_ONLY_S0_AWAITING_NATIVE_AFTER';status='PASS';
   }else{
    const b=privateBinding(),{page,network}=await openBrowser();await currentAdmission(page,b.actorId);network.assertClean(0);
     const admittedAt=new Date().toISOString();
     await turn.assertHeld('daily-genuine-'+stage+'-before-current-input');assertActive();
     const baseline={beforeBuilt:before.built,beforeInputBytes:read(preparedFile('BEFORE')),beforeBytes:before.bytes,entry,entrySha256:entrySha};
     const pending=buildCurrent({stage,identity:turn.identity,actorId:b.actorId,generated:beforeState.generated,...baseline,admittedAt,readyAt:new Date().toISOString(),nonce:crypto.randomBytes(16).toString('hex')});
     writeClosed(out,currentFiles(stage).input,pending.built);writeClosed(out,currentFiles(stage).binding,pending.binding);
     const resumed=await waitCurrentResume(process,pending.ready,{signal:AbortSignal.any([turn.signal,abort.signal]),onViolation:onAbort});currentResumeDispose=resumed.dispose;
     current={binding:pending.binding,baseline,evidenceFile:resumed.evidenceFile,inputSha256:pending.ready.inputSha256};readCurrent();network.assertClean(0);
     await turn.assertHeld('daily-genuine-'+stage+'-before-attempt');assertActive();readCurrent();
     attemptBinding=currentAttempt(current.binding,current.evidenceSha256);persistAttempt(out,stage,attemptBinding);network.arm();expectedWrites=1;
    let responseStatus=null,projection=null,responseCode='FIXTURE_RESPONSE_RECONCILIATION_REQUIRED';
    try{
     const result=await page.evaluate(async({url,body})=>{
      const r=await authFetch(url,{method:'POST',headers:{'content-type':'application/json'},body,auth401Mode:'return',signal:AbortSignal.timeout(65000)});
      return {status:r.status,body:await r.json().catch(()=>null)};
     },{url:BACK+route[stage],body:JSON.stringify(request(stage))});
     responseStatus=result.status;projection=qualifyResponse(stage,result.status,result.body);responseCode='FIXTURE_RESPONSE_AWAITING_NATIVE_READBACK';
    }catch{/* Uncertain outcomes never clear the durable attempt or retry the request. */}
    await browser.close();browser=null;closed=true;
    try{network.assertClean(1);}catch{projection=null;responseCode='FIXTURE_NETWORK_BOUNDARY_HELD';}
    writeClosed(out,stage+'_RESPONSE.json',{kind:'REAL_HTTP_RESULT',status:responseStatus,projection,code:responseCode,finishedAt:new Date().toISOString(),browserClosed:true});
    if(projection){generate('AFTER',b,{...beforeState.generated,...projection});status='PASS';code='FIXTURE_AWAITING_NATIVE_AFTER';}
    else{generate('RECONCILE',b);code='FIXTURE_UNCERTAIN_NATIVE_RECONCILIATION_REQUIRED';}
   }
  }else if(phase==='qualify'){
   const before=load('BEFORE'),after=load('AFTER'),response=JSON.parse(read(out+'/'+stage+'_RESPONSE.json'));
   check(JSON.stringify(before.built.receipt.identity)===JSON.stringify(after.built.receipt.identity),'FIXTURE_SAME_OWNED_TURN_REQUIRED');
   check(response.browserClosed===true&&(stage==='S0'?response.kind==='NO_BUSINESS_REQUEST':response.kind==='REAL_HTTP_RESULT'&&response.status===200&&response.projection!==null),'FIXTURE_REAL_QUALIFIED_RESPONSE_REQUIRED');
   check(Date.parse(after.frame.observedAt)>=Date.parse(response.finishedAt),'FIXTURE_ACTUAL_AFTER_ORDER_REQUIRED');
   assertBefore(stage,before.frame,beforeState);assertAfter(stage,before.frame,after.frame,response.projection);
   // Complete approved native effect attribution is ROOT-owned, separate from this scoped aggregate.
   // The actual installed FKs/callbacks and all approved side effects must be qualified; a 500 is never "no mutation".
   const afterEffectFile=options['--effect-file'],afterEffect=JSON.parse(committedRootFile(afterEffectFile));
   check(afterEffect.version==='DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1'&&afterEffect.stage===stage
    &&afterEffect.beforeEnvelopeSha256===sha(before.bytes)&&afterEffect.afterEnvelopeSha256===sha(after.bytes)
    &&afterEffect.responseProjectionSha256===sha(JSON.stringify(response.projection)),'FIXTURE_ROOT_COMPLETE_AFTER_EFFECT_REQUIRED');
   for(const k of ['canonicalAfterEvidence','exactApprovedChangesEvidence','backgroundAttributionEvidence'])committedRootFile(afterEffect[k]);
   let currentBefore=null;
    if(stage!=='S0'){
     const {entry,effectFile}=JSON.parse(read(out+'/'+stage+'_ENTRY_BINDING.json')),effectBytes=committedRootFile(effectFile);
     const entrySha256=assertSetupEntry(committedRootFile('docs/healthroster-build/TEST_SETUP_MANIFEST.md').toString(),entry,stage,before.bytes,before.built.receipt,effectBytes);
     const f=currentFiles(stage),currentEvidenceBytes=committedRootFile(f.evidence),consumptionBytes=committedRootFile(f.consumptionEvidence);
     currentBefore=validateCurrentAfter({currentEvidenceBytes,consumptionBytes,beforeBuilt:before.built,beforeInputBytes:read(preparedFile('BEFORE')),beforeBytes:before.bytes,entry,entrySha256,response});
     check(afterEffect.currentReadEvidence===f.evidence&&afterEffect.currentConsumptionEvidence===f.consumptionEvidence&&afterEffect.currentEvidenceSha256===sha(currentEvidenceBytes)&&afterEffect.currentConsumptionSha256===sha(consumptionBytes),'FIXTURE_CURRENT_AFTER_EFFECT_BINDING_REQUIRED');
    }
    assertActive();saveQualification(out,stage,before.frame,after.frame,response.projection,beforeState,currentBefore);status='PASS';code='FIXTURE_STAGE_NATIVE_QUALIFIED';
  }else if(phase==='reconcile'){
   check(fs.existsSync(out+'/'+stage+'_ATTEMPT.json')&&!fs.existsSync(out+'/'+stage+'_QUALIFIED.json'),'FIXTURE_UNRESOLVED_ATTEMPT_REQUIRED');
   if(!fs.existsSync(preparedFile('RECONCILE'))){generate('RECONCILE',privateBinding());code='FIXTURE_RECONCILIATION_INPUT_PREPARED_STILL_HELD';}
   else{load('RECONCILE');code='FIXTURE_RECONCILIATION_RECORDED_STILL_HELD';}
  }else throw Error('FIXTURE_PHASE_REQUIRED');
 }catch(error){code=/^FIXTURE_[A-Z0-9_]+$/.test(error?.message??'')?error.message:'FIXTURE_PHASE_HELD';}
 finally{
  clearTimeout(timer);currentResumeDispose?.();process.removeListener('message',onMessage);
  if(browser){let closeTimer;try{await Promise.race([browser.close(),new Promise((_,reject)=>{closeTimer=setTimeout(()=>reject(Error('CLOSE')),8000);})]);closed=true;}catch{closed=false;status='HOLD';code='FIXTURE_BROWSER_CLOSE_HELD';}finally{clearTimeout(closeTimer);}}
  if(activeNetwork){try{activeNetwork.assertClean(expectedWrites);}catch{status='HOLD';code='FIXTURE_NETWORK_BOUNDARY_HELD';}}
  try{await turn.assertHeld('daily-genuine-'+stage+'-'+phase+'-end');}catch{status='HOLD';code='FIXTURE_ORIGINAL_TURN_LOST';}
  if(abort.signal.aborted||Date.now()>=deadline){status='HOLD';code='FIXTURE_BUDGET_HELD';}
  turn.signal.removeEventListener('abort',onAbort);turn.close();
  // A failed/unknown close does not release the cross-process boundary.
  if(closed)release();
  writeClosed(out,options.resultName,{status,code,phase,stage,browserClosed:closed});
 }
 return {status,code,phase,stage,browserClosed:closed};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const result=await perform(JSON.parse(process.argv[2]));process.exitCode=result.status==='PASS'?0:1;}catch{process.exitCode=1;}finally{process.disconnect?.();}
}
