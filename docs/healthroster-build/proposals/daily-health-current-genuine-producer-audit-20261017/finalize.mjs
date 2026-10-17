// Append-only correction to the initial local source freeze. Never invokes native/HTTP.
import fs from'node:fs';import{spawnSync}from'node:child_process';
import{here,census,sha,assertCurrentSource}from'./source.mjs';
const check=(v,e)=>{if(!v)throw Error(e);};
const run=(exe,args)=>{const r=spawnSync(exe,args,{cwd:here,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},encoding:'utf8',windowsHide:true});check(r.status===0,r.stderr||r.stdout);return r.stdout+r.stderr;};
if(process.argv.includes('--verify')){const m=JSON.parse(fs.readFileSync(here+'/FINAL_MANIFEST.json'));for(const r of m.files){const b=fs.readFileSync(here+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256,'FINAL_FROZEN_BYTES:'+r.file);}console.log(JSON.stringify({files:m.files.length,rawExact:true,native:'NOT_RUN'}));}
else{
 check(!fs.existsSync(here+'/FINAL_MANIFEST.json'),'FROZEN_HISTORY');
 run(process.execPath,[here+'/freeze.mjs','--verify']);const binding=assertCurrentSource();
 const before=fs.readFileSync(here+'/MANIFEST.json');check(sha(before)==='39a2de2fab40738c2ceb589491e4e06bc8212f5b4e3e42d1dd9667ee9bdfb500','EXACT_FIRST_LOCAL_FREEZE_REQUIRED');
 const contract=JSON.parse(fs.readFileSync(census.root+'/supabase/release/current-contract.json'));
 const source=fs.readFileSync(here+'/reference/current/22092026_1226_client_initial_settings_baseline_v1.sql','utf8');
 const wanted=['private._candidate_feature_enabled_current_v1','private._candidate_normalize_manager_policy_v1','public._ctms_assert_import_correction_settings_write_v1','public._inv_write_audit','public.import_auto_authorise_policy_resolve_v1'];
 const calls=[...source.matchAll(/\b((?:private|public)\.[a-z_0-9]+)\(/g)].map(m=>({name:m[1],line:source.slice(0,m.index).split('\n').length})).filter(r=>r.name!=='public.client_create_with_settings_v1');
 check(calls.length===6&&new Set(calls.map(c=>c.name)).size===5&&calls.every(c=>wanted.includes(c.name)),'EXACT_FIVE_DIRECT_ROUTINES_SIX_CALLS_REQUIRED');
 const directRoutines=wanted.map(name=>{const expected=contract.routines.filter(r=>r.schema+'.'+r.identity.split('(')[0]===name);check(expected.length===1,'EXACT_BASELINE_ROUTINE_REQUIRED');return{name,baselineMetadata:expected[0],bodyInspected:false,nativeBinding:'NOT_OBSERVED_HERE'};});
 const ast=JSON.parse(run('python',[here+'/client-return-ast.py']).trim());
 run(process.execPath,['--check',here+'/finalize.mjs']);
 fs.writeFileSync(here+'/FINAL_SOURCE_REVIEW.json',JSON.stringify({version:'DAILY_GENUINE_CURRENT_PRODUCER_AUDIT_FINAL_SOURCE_REVIEW_V1',firstLocalManifestSha256:sha(before),firstFreezeFilesUnchanged:41,
  correction:'Four pre-RETURN direct custom routines omitted the fifth genuine policy resolver in the unchanged actual RETURN. Complete source retained all six calls; this append corrects only the source census.',
  currentSource:binding,customDirectCallSites:calls,distinctCustomDirectRoutines:directRoutines,sourceAst:ast,sourceTests:11,
  futureRootActor:'BIND_SAFE_ROOT_OBSERVATION_PRIVATELY',vacancy:'NOT_BOUND',workerDeployedSource:'NOT_PROVED_HERE',native:'NOT_RUN',nativeBusinessRowsRead:0,businessRowsCreated:0,runtimeTargets:[],bankingBodiesSelected:0},null,2)+'\n');
 const files=[];const visit=(dir,prefix='')=>{for(const e of fs.readdirSync(dir,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){if(e.name==='__pycache__')continue;const file=prefix+e.name;if(e.isDirectory())visit(dir+'/'+e.name,file+'/');else if(file!=='FINAL_MANIFEST.json'){const b=fs.readFileSync(dir+'/'+e.name);files.push({file,bytes:b.length,sha256:sha(b)});}}};visit(here);
 fs.writeFileSync(here+'/FINAL_MANIFEST.json',JSON.stringify({version:'DAILY_GENUINE_CURRENT_PRODUCER_AUDIT_FINAL_SOURCE_FREEZE_V1',firstLocalManifestSha256:sha(before),sourceParent:census.commit,currentCheckpoint:binding.currentCheckpoint,native:'NOT_RUN',runtimeTargets:[],files},null,2)+'\n');
 console.log(JSON.stringify({files:files.length,finalManifestSha256:sha(fs.readFileSync(here+'/FINAL_MANIFEST.json')),sourceTests:11,native:'NOT_RUN'}));
}
