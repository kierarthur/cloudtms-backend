// Own source-only freeze. It does not invoke hold-case or any database tool.
import fs from'node:fs';import path from'node:path';import {fileURLToPath}from'node:url';import {spawnSync,execFileSync}from'node:child_process';import {sha}from'./build-program.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),own='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026',root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const check=(v,e)=>{if(!v)throw Error(e);};check(fs.realpathSync.native(here).replaceAll('\\','/').startsWith(own+'/'),'OWN_PHYSICAL_D_ONLY');
if(process.argv.includes('--verify')){
 const m=JSON.parse(fs.readFileSync(here+'/MANIFEST.json'));for(const r of m.files){const b=fs.readFileSync(here+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256,r.file);}console.log(JSON.stringify({frozenFiles:m.files.length,rawExact:true,native:'NOT_RUN'}));
}else{
 check(!fs.existsSync(here+'/MANIFEST.json'),'FROZEN_HISTORY');
 const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));
 const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{maxBuffer:24e6});
 const head=git('rev-parse','HEAD').toString().trim();
 const source=pins.rootInputs.map(r=>{const saved=fs.readFileSync(root+'/'+r.file);check(saved.length===r.bytes&&sha(saved)===r.sha256,r.file);check(saved.equals(git('show',pins.rootCommit+':'+r.file))&&saved.equals(git('show',head+':'+r.file)),r.file);return{file:r.file,sha256:r.sha256,bytes:r.bytes,parentAndCurrentCommittedRawEqual:true,savedEqualsCommitted:true};});
 fs.writeFileSync(here+'/ROOT_SOURCE_BINDING.json',JSON.stringify({sourceParent:pins.rootCommit,currentRootCheckpoint:head,rootPhysical:fs.realpathSync.native(root),source,rootExistingFilesEdited:0,native:'NOT_RUN'},null,2)+'\n');
 const run=(file,args=[])=>{const r=spawnSync(file,args,{cwd:own,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},encoding:'utf8',windowsHide:true});check(r.status===0,r.stderr||r.stdout);return r.stdout+r.stderr;};
 run(process.execPath,[here+'/prepare-offline.mjs']);
 const grammar=run('python',[here+'/grammar-and-bindings.py']);
 const tests=run(process.execPath,[here+'/test.mjs']);
 for(const f of fs.readdirSync(here).filter(x=>x.endsWith('.mjs')))run(process.execPath,['--check',here+'/'+f]);
 fs.writeFileSync(here+'/TEST_RESULTS.txt',tests);fs.writeFileSync(here+'/GRAMMAR_RUN.txt',grammar);
 const files=[];const visit=(dir,prefix='')=>{for(const e of fs.readdirSync(dir,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){if(e.name==='__pycache__')continue;const file=prefix+e.name;if(e.isDirectory())visit(dir+'/'+e.name,file+'/');else if(file!=='MANIFEST.json'){const b=fs.readFileSync(dir+'/'+e.name);files.push({file,bytes:b.length,sha256:sha(b)});}}};visit(here);
 fs.writeFileSync(here+'/MANIFEST.json',JSON.stringify({version:'CONTACT_QT_POSITIVE_SOURCE_ONLY_FREEZE_V1',sourceParent:pins.rootCommit,sourceTests:16,parsedNativeInputPlBodies:21,native:'NOT_RUN',runtimeTargets:[],files},null,2)+'\n');
 console.log(JSON.stringify({files:files.length,manifestSha256:sha(fs.readFileSync(here+'/MANIFEST.json')),sourceTests:16,native:'NOT_RUN'}));
}
