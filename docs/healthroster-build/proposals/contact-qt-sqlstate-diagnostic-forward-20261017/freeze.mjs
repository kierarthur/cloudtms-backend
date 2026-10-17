// Own source-only freeze. Never starts the native holder, psql, API or connector.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{execFileSync,spawnSync}from'node:child_process';import{createHash}from'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const sha=b=>createHash('sha256').update(b).digest('hex'),check=(v,e)=>{if(!v)throw Error(e);};
check(fs.realpathSync.native(here).replaceAll('\\','/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'),'OWN_PHYSICAL_D_ONLY');
if(process.argv.includes('--verify')){const m=JSON.parse(fs.readFileSync(here+'/MANIFEST.json'));for(const r of m.files){const b=fs.readFileSync(here+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256,'FROZEN_BYTES:'+r.file);}console.log(JSON.stringify({files:m.files.length,rawExact:true,native:'NOT_RUN'}));}
else{
 check(!fs.existsSync(here+'/MANIFEST.json'),'FROZEN_HISTORY');
 const run=(exe,args)=>{const r=spawnSync(exe,args,{cwd:here,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},encoding:'utf8',windowsHide:true});check(r.status===0,r.stderr||r.stdout);return r.stdout+r.stderr;};
 const extra=run(process.execPath,['--test','--test-reporter=tap',here+'/test.mjs']),original=run(process.execPath,['--test','--test-reporter=tap',here+'/current/test.mjs']);
 check(/# tests 8\r?\n# suites 0\r?\n# pass 8\r?\n# fail 0/.test(extra)&&/# tests 16\r?\n# suites 0\r?\n# pass 16\r?\n# fail 0/.test(original),'ALL_TWENTY_FOUR_SOURCE_TESTS_REQUIRED');
 fs.writeFileSync(here+'/TEST_RESULTS.txt',extra+'\n'+original);
 for(const dir of [here,here+'/current'])for(const file of fs.readdirSync(dir).filter(n=>n.endsWith('.mjs')))run(process.execPath,['--check',dir+'/'+file]);
 const r1Path=root+'/.codex-tmp/contact-qt-positive-native-20261017-constructors-r1/HOLD.json',r1=fs.readFileSync(r1Path),hold=JSON.parse(r1);
 check(hold.code==='LOCAL_METADATA_DIAGNOSTIC_REFUSED'&&hold.deadlineObserved===false&&hold.localTiming.sequenceStartedAt==='2026-10-17T08:57:32.383Z'&&hold.elapsedMilliseconds===49881&&hold.nativeClockFrames.length===0,'EXACT_ORIGINAL_R1_HOLD_REQUIRED');
 fs.writeFileSync(here+'/R1_HOLD_COPY.json',r1);
 fs.writeFileSync(here+'/R1_PROVENANCE.json',JSON.stringify({source:r1Path,sha256:sha(r1),savedRawCopy:true,gitBackingNotAsserted:true,cause:'UNKNOWN_STDERR_DISCARDED',rootReportedScope:'C_BEFORE_STOPPED_BEFORE_READY_INPUT_MCP_B_MARKER_DDL_FIXTURES',rootReportedOwnUnlockAndCloseAt:'2026-10-17T08:58:22.259Z',rootReportedProved:false,notIndependentlyNativeObservedHere:true,nativeTrialByThisAuthor:'NOT_RUN'},null,2)+'\n');
 const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:32e6}),head=git('rev-parse','HEAD').toString().trim(),pins=JSON.parse(fs.readFileSync(here+'/current/SOURCE_PINS.json'));
 for(const r of pins.rootInputs){const b=fs.readFileSync(root+'/'+r.file);check(sha(b)===r.sha256&&b.equals(git('show',head+':'+r.file)),'CURRENT_ROOT_PIN_CHANGED:'+r.file);}
 const states=['55000','42P01','42703','57014','XX000','08006'],heads=['ERROR:  ','FATAL:  ','psql:<stdin>:1: ERROR:  ','psql:<stdin>:2147483647: FATAL:  '],tails=['','\n','\r\n'];
 const byteBoundaryVectors=states.length*heads.reduce((n,h)=>n+tails.reduce((m,t)=>m+Buffer.byteLength(h+'XXXXX'+t)+2,0),0);
 fs.writeFileSync(here+'/SOURCE_REVIEW.json',JSON.stringify({version:'CONTACT_QT_CLOSED_SQLSTATE_SOURCE_REVIEW_V1',parentCommit:'2b0fcee4eeecb9ce3adb61ef3c25d02c0e5a948c',currentRoot:head,rootPins:pins.rootInputs.length,sourceTests:24,
  byteBoundaryVectors,actualSyntheticChildSplitCases:Buffer.byteLength('psql:<stdin>:123: ERROR:  55000\n')-1,selectedFiles:46,changedExisting:5,unchangedExisting:40,addedModules:1,
  nativeSqlBytesUnchanged:true,canonicalQueryBytesUnchanged:true,originalWeakMapUnchanged:true,statementMilliseconds:45000,childMilliseconds:70000,workMilliseconds:120000,wholeMilliseconds:180000,
  originalR1Cause:'UNKNOWN',nativeCompilation:'NOT_PROVED',nativeExecution:'NOT_RUN',rootEdits:0,runtimeTargets:[]},null,2)+'\n');
 const files=[];const visit=(dir,prefix='')=>{for(const e of fs.readdirSync(dir,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){if(e.name==='__pycache__')continue;const file=prefix+e.name;if(e.isDirectory())visit(dir+'/'+e.name,file+'/');else if(file!=='MANIFEST.json'){const b=fs.readFileSync(dir+'/'+e.name);files.push({file,bytes:b.length,sha256:sha(b)});}}};visit(here);
 fs.writeFileSync(here+'/MANIFEST.json',JSON.stringify({version:'CONTACT_QT_SQLSTATE_DIAGNOSTIC_FORWARD_SOURCE_ONLY_FREEZE_V1',parentCommit:'2b0fcee4eeecb9ce3adb61ef3c25d02c0e5a948c',sourceTests:24,native:'NOT_RUN',runtimeTargets:[],files},null,2)+'\n');
 console.log(JSON.stringify({files:files.length,manifestSha256:sha(fs.readFileSync(here+'/MANIFEST.json')),sourceTests:24,byteBoundaryVectors,native:'NOT_RUN'}));
}
