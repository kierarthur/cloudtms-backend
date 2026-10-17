// Runs the complete actual perform()/journal bodies against memory only. No production seam.
import fs from 'node:fs';import vm from 'node:vm';import path from 'node:path';import crypto from 'node:crypto';import {EventEmitter} from 'node:events';import {pathToFileURL} from 'node:url';
import {here,OP,c,n,b2,core,moduleVM,NOW,at,envelope,ACTOR,RATE,SETTINGS,TS} from './test-support.mjs';import * as network from './payload/report/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/network.mjs';import {waitCurrentResume} from './payload/report/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/current-ipc.mjs';import {runBookkeeper} from './offline-root.mjs';import {setSyntheticTurn} from './turn-test-namespace.mjs';
export async function qualifyWorker(f,m,{mode='good',phase='qualify'}={}){
 let handles=new Map(),serial=0;const events=m.events,norm=p=>p.replaceAll('\\','/');
 const fakefs={...m.memoryFS,openSync:(p,flags)=>{p=norm(p);if(flags!=='wx'||m.files.has(p))throw Error('SYNTHETIC_EEXIST');m.put(p,Buffer.alloc(0));handles.set(++serial,p);events.push(['open',path.basename(p)]);return serial;},writeSync:(fd,s)=>{const p=handles.get(fd);m.put(p,Buffer.concat([m.get(p),Buffer.from(s)]));},fsyncSync:()=>{},closeSync:fd=>handles.delete(fd),unlinkSync:p=>m.files.delete(norm(p)),renameSync:(a,b)=>{m.put(norm(b),m.get(a));m.files.delete(norm(a));}};
 let currentBefore=null;
 if(f.plan.qualifiedPrefix.length>1){currentBefore=b2.validateCurrentAfter({currentEvidenceBytes:f.currentEvidenceBytes,consumptionBytes:f.consumptionBytes,...f.baseline,response:f.response,now:Date.now()});currentBefore=structuredClone(currentBefore);currentBefore.stage=f.plan.qualifiedPrefix.at(-1);currentBefore.identity.task='DAILY_GENUINE_'+currentBefore.stage+'_STAGED_ONLY';Object.assign(currentBefore,{currentReadEvidence:b2.currentFiles(currentBefore.stage).evidence,currentConsumptionEvidence:b2.currentFiles(currentBefore.stage).consumptionEvidence});}
 m.save('QUALIFIED.json',{qualified:f.plan.qualifiedPrefix,after:f.stage==='S0'?null:{...f.before,stage:f.plan.qualifiedPrefix.at(-1),phase:'AFTER'},generated:f.generated,...f.plan.qualifiedPrefix.length>1?{currentBefore}:{}});
 const journals=moduleVM('report',OP+'stages.mjs',{...c,...n,...b2,fs:fakefs,path},['assertBefore','assertAfter','assertSetupEntry','assertEffectAdmission','acquireSession','readJournal','persistAttempt','writeClosed','saveQualification']);
 let messageHandler;const proc={env:{HEALTH_GENUINE_OWNED_OUT:m.out},cwd:()=>m.out,send(){},on:(name,fn)=>{if(name==='message')messageHandler=fn;},removeListener:(name,fn)=>{if(name==='message'&&messageHandler===fn)messageHandler=null;}};
 const turn={identity:f.identity,signal:new AbortController().signal,assertHeld:async phase=>{events.push(['held',phase]);if(mode==='lost-end'&&phase.endsWith('-end'))throw Error('SYNTHETIC_LOSS');},close:()=>events.push(['turnClosed'])};setSyntheticTurn(turn);
 const code=fs.readFileSync(here+'/payload/report/'+OP+'worker.mjs','utf8'),start=code.indexOf('export async function perform('),end=code.indexOf('\nif(process.argv[1]'),body=code.slice(start,end).replace(/^export /,'');
 const context=vm.createContext({...c,...n,...b2,...network,...journals,fs:fakefs,path,pathToFileURL,process:proc,Buffer,Date,AbortController,AbortSignal,setTimeout,clearTimeout,here:here+'/payload/report/'+OP,sourcePreflight:()=>events.push(['sourcePreflight']),assertReviewedExecutionSource:()=>events.push(['reviewedExecutionSource']),ownedDirectory:p=>p,physical:norm,git:()=>Buffer.from(f.entry.sourceCommit),read:p=>m.get(p),committedRootFile:file=>m.get(m.ROOT+'/'+file)});
 const script=new vm.Script('('+body+')',{importModuleDynamically:async url=>{if(url!==pathToFileURL(c.ROOT+'/scripts/healthroster-turn-control.mjs').href)throw Error('SYNTHETIC_IMPORT');return import('./turn-test-namespace.mjs');}});
 const result=await script.runInContext(context)({'--phase':phase,'--stage':f.stage,'--effect-file':core.publicBase+'DAILY_GENUINE_'+f.stage+(phase==='entry'?'_EFFECT_ADMISSION_20261017.json':'_AFTER_EFFECT_QUALIFICATION_20261017.json'),resultName:'CLOSED_'+phase+'_'+f.stage+'_'+'e'.repeat(16)+'.json'});
 return {result,journal:JSON.parse(m.get(m.out+'/QUALIFIED.json'))};
}
export async function submitWorker(f,m,{mode='good'}={}){
 let clockAdvance=0,handles=new Map(),serial=0,handler,forwarded=0,rootError;
 class Clock extends Date{constructor(...a){super(...a.length?a:[Date.now()+clockAdvance]);}static now(){return Date.now()+clockAdvance;}}
 const norm=p=>p.replaceAll('\\','/'),events=m.events;
 const fakefs={...m.memoryFS,openSync:(p,flags)=>{p=norm(p);if(flags!=='wx'||m.files.has(p))throw Error('SYNTHETIC_EEXIST');m.put(p,Buffer.alloc(0));handles.set(++serial,p);events.push(['open',path.basename(p)]);return serial;},writeSync:(fd,s)=>{const p=handles.get(fd);m.put(p,Buffer.concat([m.get(p),Buffer.from(s)]));},fsyncSync:fd=>{if(mode==='age-during-consumption-fsync'&&handles.get(fd)?.endsWith('_CURRENT_CONSUMPTION.json'))clockAdvance+=31000;},closeSync:fd=>handles.delete(fd),unlinkSync:p=>m.files.delete(norm(p)),renameSync:(a,b)=>{m.put(norm(b),m.get(a));m.files.delete(norm(a));}};
 const seed=b2.validateCurrentAfter({currentEvidenceBytes:f.currentEvidenceBytes,consumptionBytes:f.consumptionBytes,...f.baseline,response:f.response,now:Date.now()});
 if(f.plan.qualifiedPrefix.length>1){seed.stage=f.plan.qualifiedPrefix.at(-1);seed.identity.task='DAILY_GENUINE_'+seed.stage+'_STAGED_ONLY';seed.currentReadEvidence=b2.currentFiles(seed.stage).evidence;seed.currentConsumptionEvidence=b2.currentFiles(seed.stage).consumptionEvidence;}
 m.save('QUALIFIED.json',{qualified:f.plan.qualifiedPrefix,after:f.before,generated:f.generated,...f.plan.qualifiedPrefix.length>1?{currentBefore:seed}:{}});m.save('PRIVATE_BINDING.json',{identity:f.identity,actorId:ACTOR});
 m.files.delete(m.ROOT+'/'+f.plan.currentReadEvidence);m.files.delete(m.out+'/'+f.stage+'_CURRENT_INPUT.json');m.files.delete(m.out+'/'+f.stage+'_CURRENT_BINDING.json');m.files.delete(m.out+'/'+f.stage+'_CURRENT_CONSUMPTION.json');
 for(const suffix of ['RESPONSE.json','AFTER_INPUT.json','AFTER_NATIVE.json'])m.files.delete(m.out+'/'+f.stage+'_'+suffix);
 const proc=new EventEmitter();Object.assign(proc,{env:{HEALTH_GENUINE_OWNED_OUT:m.out},cwd:()=>m.out});
 proc.send=(ready,callback)=>{
  events.push(['ready',ready]);callback?.();
  void (async()=>{
   const built=JSON.parse(m.get(m.out+'/'+b2.currentFiles(f.stage).input));let result=envelope(built,f.before,[0,0]);
   for(const frame of [result.statements[1].rows[0].fixture_read,result.rollbackVerification.statements[0].rows[0].fixture_read])frame.observedAt=new Date().toISOString();
   if(mode==='reused-b1')result=JSON.parse(f.beforeBytes);
   if(mode==='changed-scope')for(const frame of [result.statements[1].rows[0].fixture_read,result.rollbackVerification.statements[0].rows[0].fixture_read])frame.scope.outbox.sha256=c.sha('foreign-change');
   m.save(f.stage+'_CURRENT_NATIVE.json',result);m.save(f.stage+'_CURRENT_TIMING.json',{startedAt:new Date().toISOString(),finishedAt:new Date().toISOString()});
   if(mode==='old-effect-source')m.put(m.ROOT+'/'+P_EFFECT+'current-effect.mjs',Buffer.from('SYNTHETIC_OLD_ADAPTER'));
   await runBookkeeper(f,m,'CURRENT');
   const resume={type:'CURRENT_NATIVE_SAVED',nonce:ready.binding.nonce,evidenceFile:b2.currentFiles(f.stage).evidence,bindingSha256:ready.bindingSha256};if(mode==='wrong-resume')resume.nonce='0'.repeat(32);
   proc.emit('message',resume);if(mode==='duplicate-resume')proc.emit('message',resume);
  })().catch(e=>{rootError=e;proc.emit('message','ABORT');});
 };
 const P_EFFECT='docs/healthroster-build/proposals/daily-b2-current-effect-20261017/';
 const journals=moduleVM('report',OP+'stages.mjs',{...c,...n,...b2,fs:fakefs,path},['assertBefore','assertAfter','assertSetupEntry','assertEffectAdmission','acquireSession','readJournal','persistAttempt','writeClosed','saveQualification']);
 const responseBody=()=>{
  const body=c.request(f.stage);if(f.stage==='S1')return {ok:true,replay:false,client:{...body},client_settings:{...body.client_settings,id:SETTINGS,client_id:c.CLIENT,effective_from:'1900-01-01'},policy:{healthroster:{},nhsp:{}}};
  if(f.stage==='S1a')return {hospital:{...body,client_id:c.CLIENT}};if(f.stage==='S2')return {candidate:{...body,tms_ref:'SYNTHETIC'},tsfin:Object.fromEntries(['enqueued','ran_now','picked','ok','fail','auto_assigned_timesheets'].map(k=>[k,0]))};if(f.stage==='S3')return {rate:{...body,id:RATE,disabled_at_utc:null}};return {timesheet_id:TS,current_timesheet_id:TS,created_unprocessed:true};
 };
 const deliver=async(url,method,body)=>{let answer,aborted=false;await handler({request:()=>({url:()=>url,method:()=>method,postData:()=>body,resourceType:()=>'fetch'}),abort:async()=>{aborted=true;},fulfill:async x=>{answer=x.response;},fetch:async()=>{
  if(method==='POST'){forwarded++;events.push(['forward']);if(mode==='lost-reply')throw Error('SYNTHETIC_LOST_REPLY');}
  const value=method==='POST'?responseBody():url.endsWith('/api/me')?{user:{id:ACTOR,roles:['admin']}}:{contract:'WEEKLY_SOURCE_IMPORT_WORKSPACE_V1',queries:{rows:[]}};
  return {status:()=>200,json:async()=>value,dispose:async()=>{}};
 }});if(aborted||!answer)throw Error('SYNTHETIC_ABORTED');return {status:answer.status(),json:answer.json};};
 const page={setDefaultTimeout(){},setDefaultNavigationTimeout(){},goto:async()=>{},waitForFunction:async()=>{},evaluate:async(fn,args)=>new vm.Script('('+fn.toString()+')').runInNewContext({authFetch:async(url,init={})=>deliver(url,init.method??'GET',init.body??null),AbortSignal})(args)};
 const browserFactory={chromium:{executablePath:()=> 'D:/Development/Caches/ms-playwright/SYNTHETIC_ONLY.exe',launch:async()=>({newContext:async()=>({route:async(_glob,fn)=>{handler=fn;},routeWebSocket:async()=>{},newPage:async()=>page,pages:()=>[page]}),close:async()=>events.push(['browserClosed'])})}};
 const turn={identity:f.identity,signal:new AbortController().signal,assertHeld:async phase=>{events.push(['held',phase]);if(phase.endsWith('before-post-forward')){if(mode==='age-during-held')clockAdvance+=31000;if(mode==='held-loss')throw Error('SYNTHETIC_LOSS');}},close:()=>events.push(['turnClosed'])};setSyntheticTurn(turn);
 const code=fs.readFileSync(here+'/payload/report/'+OP+'worker.mjs','utf8'),start=code.indexOf('export async function perform('),end=code.indexOf('\nif(process.argv[1]'),body=code.slice(start,end).replace(/^export /,'');
 const context=vm.createContext({...c,...n,...b2,...network,...journals,waitCurrentResume,crypto,fs:fakefs,path,pathToFileURL,process:proc,Buffer,Date:Clock,AbortController,AbortSignal,setTimeout,clearTimeout,here:here+'/payload/report/'+OP,sourcePreflight:()=>events.push(['sourcePreflight']),assertReviewedExecutionSource:()=>events.push(['reviewedExecutionSource']),ownedDirectory:p=>p,physical:norm,git:()=>Buffer.from(f.entry.sourceCommit),read:p=>m.get(p),committedRootFile:file=>m.get(m.ROOT+'/'+file)});
 const script=new vm.Script('('+body+')',{importModuleDynamically:async()=>import('./turn-test-namespace.mjs')});
 const result=await script.runInContext(context)({'--phase':'submit','--stage':f.stage,resultName:'CLOSED_submit_'+f.stage+'_'+'d'.repeat(16)+'.json'},{browserFactory});
 return {result,forwarded,rootError,events};
}
