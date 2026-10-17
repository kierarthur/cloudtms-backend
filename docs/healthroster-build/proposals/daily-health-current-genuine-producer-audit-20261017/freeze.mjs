// Finite source-only audit freeze. No API/native/connector entry point exists.
import fs from'node:fs';import path from'node:path';import{spawnSync}from'node:child_process';
import{here,sha,assertCurrentSource,deriveFixture,census}from'./source.mjs';
const check=(v,e)=>{if(!v)throw Error(e);};
check(fs.realpathSync.native(here).replaceAll('\\','/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'),'OWN_PHYSICAL_D_ONLY');
if(process.argv.includes('--verify')){const m=JSON.parse(fs.readFileSync(here+'/MANIFEST.json'));for(const r of m.files){const b=fs.readFileSync(here+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256,'FROZEN_BYTES:'+r.file);}console.log(JSON.stringify({files:m.files.length,rawExact:true,native:'NOT_RUN'}));}
else{
 check(!fs.existsSync(here+'/MANIFEST.json'),'FROZEN_HISTORY');const binding=assertCurrentSource();
 fs.writeFileSync(here+'/CURRENT_ROOT_BINDING.json',JSON.stringify(binding,null,2)+'\n');
 fs.writeFileSync(here+'/DERIVED_FIXTURE.json',JSON.stringify(await deriveFixture(),null,2)+'\n');
 const run=(exe,args)=>{const r=spawnSync(exe,args,{cwd:here,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},encoding:'utf8',windowsHide:true});check(r.status===0,r.stderr||r.stdout);return r.stdout+r.stderr;};
 const tests=run(process.execPath,['--test','--test-reporter=tap',here+'/test.mjs']);check(/# tests 11\r?\n# suites 0\r?\n# pass 11\r?\n# fail 0/.test(tests),'ELEVEN_SOURCE_TESTS_REQUIRED');fs.writeFileSync(here+'/TEST_RESULTS.txt',tests);
 for(const name of fs.readdirSync(here).filter(n=>n.endsWith('.mjs')))run(process.execPath,['--check',here+'/'+name]);
 const files=[];const visit=(dir,prefix='')=>{for(const e of fs.readdirSync(dir,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){if(e.name==='__pycache__')continue;const file=prefix+e.name;if(e.isDirectory())visit(dir+'/'+e.name,file+'/');else if(file!=='MANIFEST.json'){const b=fs.readFileSync(dir+'/'+e.name);files.push({file,bytes:b.length,sha256:sha(b)});}}};visit(here);
 fs.writeFileSync(here+'/MANIFEST.json',JSON.stringify({version:'DAILY_GENUINE_CURRENT_PRODUCER_AUDIT_SOURCE_ONLY_FREEZE_V1',sourceParent:census.commit,currentSourceCheckpoint:binding.currentCheckpoint,sourceTests:11,native:'NOT_RUN',runtimeTargets:[],bankingBodiesSelected:0,files},null,2)+'\n');
 console.log(JSON.stringify({files:files.length,manifestSha256:sha(fs.readFileSync(here+'/MANIFEST.json')),sourceTests:11,native:'NOT_RUN'}));
}
