import './source-overlay.mjs';
// Actual read-only source admission only. Never opens a turn, env, psql or DB.
import fs from'node:fs';import assert from'node:assert/strict';import cp from'node:child_process';import{syncBuiltinESMExports}from'node:module';import{pathToFileURL,fileURLToPath}from'node:url';import{createHash}from'node:crypto';
const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',ADAPTER='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017',here=new URL('.',import.meta.url),original=cp.execFileSync,sha=b=>createHash('sha256').update(b).digest('hex');let active=null;
const gitArgs=['-c','gc.auto=0','-c','maintenance.auto=false','-C',ROOT],head=()=>original('git',[...gitArgs,'rev-parse','HEAD'],{encoding:'utf8'}).trim(),startHead=head(),sourceBytes=fs.readFileSync(ROOT+'/'+ADAPTER+'/source.mjs');
assert(fs.realpathSync.native(process.cwd()).replaceAll('\\','/').startsWith('D:/Development/GitHub/'));assert(!fs.existsSync(new URL('./POSTIMAGE_MEASUREMENTS.json',here)),'FROZEN_MEASUREMENT');
cp.execFileSync=function(file,args,options){const t=performance.now();try{return original(file,args,options);}finally{if(active){assert.equal(file,'git','SOURCE_ONLY_GIT_PROCESS_REQUIRED');const command=args.find((v,i)=>i>0&&['rev-parse','show','diff','merge-base','cat-file'].includes(v));assert(command,'CLOSED_SOURCE_GIT_COMMAND_REQUIRED');active.calls.push({command,milliseconds:performance.now()-t});}}};syncBuiltinESMExports();
const phases=[];
async function measure(name,fn){active={calls:[]};const t=performance.now();try{await fn();const elapsed=performance.now()-t,calls=active.calls;phases.push({name,elapsedMilliseconds:elapsed,gitCalls:calls.length,gitMilliseconds:calls.reduce((n,x)=>n+x.milliseconds,0),gitCommands:Object.fromEntries([...new Set(calls.map(x=>x.command))].map(k=>[k,calls.filter(x=>x.command===k).length]))});}finally{active=null;}}
try{const s=await import(pathToFileURL(ROOT+'/'+ADAPTER+'/source.mjs')),b=await import(pathToFileURL(ROOT+'/'+ADAPTER+'/input-builder.mjs')),identity={physicalDatabase:'cloudtms_test_clone',databaseOid:17,backendPid:101,turnId:'hr_turn_'+'1'.repeat(32),key:[20261013,1]},caseId='CONTACT_CONSTRUCTORS_V1';
 await measure('assertCommittedAdapter',()=>s.assertCommittedAdapter());
 await measure('assertManifestedFixtureEntry',()=>s.assertManifestedFixtureEntry(ROOT));
 await measure('sourceCold',()=>s.source(ROOT,caseId));
 await measure('sourceWarm',()=>s.source(ROOT,caseId));
 await measure('actualBeforePhaseWarm',async()=>{s.assertCommittedAdapter();await s.source(ROOT,caseId);});
 await measure('buildInputWarm',()=>b.buildInput({root:ROOT,identity,caseId}));
 assert.equal(head(),startHead,'ROOT_HEAD_MOVED_DURING_MEASUREMENT');assert(sourceBytes.equals(fs.readFileSync(ROOT+'/'+ADAPTER+'/source.mjs')));
 const result={version:'CONTACT_POSTIMAGE_SOURCE_ADMISSION_OFFLINE_MEASUREMENTS_V1',measuredAt:new Date().toISOString(),root:ROOT,head:startHead,sourceSha256:sha(sourceBytes),executedSourcePostimageSha256:sha(fs.readFileSync(new URL('./candidate/docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017/source.mjs',here))),actualPostimageAdmission:'NOT_RUN_UNMOUNTED_OFFLINE_MODULE_OVERLAY_ONLY',caseId,identity:'SYNTHETIC_SOURCE_ONLY_NO_NATIVE',phases,wallTimesAreLocalMeasurementsNotNativeCause:true,native:'NOT_RUN',queriesExecuted:0,credentialsLoaded:false,rawGitOutputRecorded:false,turnsOpened:0,statementOrWorkBudgetsChanged:false};fs.writeFileSync(new URL('./POSTIMAGE_MEASUREMENTS.json',here),JSON.stringify(result,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify(result));
}finally{cp.execFileSync=original;syncBuiltinESMExports();}
