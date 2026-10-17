// Own proposal source freeze/integrity only. No API, database or native runner.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {execFileSync,spawnSync} from 'node:child_process';import {createHash} from 'node:crypto';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url));
const own='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026';
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const sha=b=>createHash('sha256').update(b).digest('hex');
assert(fs.realpathSync.native(here).replaceAll('\\','/').startsWith(own+'/'));
const manifestPath=here+'/MANIFEST.json';
if(process.argv.includes('--verify')){
  const manifest=JSON.parse(fs.readFileSync(manifestPath));
  for(const f of manifest.files){const b=fs.readFileSync(here+'/'+f.path);assert.equal(b.length,f.bytes,f.path);assert.equal(sha(b),f.sha256,f.path);}
  console.log(JSON.stringify({files:manifest.files.length,allFrozenBytesExact:true,native:'NOT_RUN'}));
}else{
  assert(!fs.existsSync(manifestPath),'Frozen history must not be rewritten');
  const census=JSON.parse(fs.readFileSync(here+'/INITIAL_CENSUS.json'));
  const checkpoint='f9d6d77d521c027bbab88f12af6f4e5c9f83f16e';
  const files=[...new Set(census.selected.map(x=>x.declaredPhysical).concat([
    'broker/src/index.js','docs/bpay-next/final-definer-closures.json','supabase/release/current-release.json',
    'supabase/release/current-contract.json','scripts/cloudtms-db-release-lib.mjs',
    'scripts/cloudtms-managed-release-sql.mjs','scripts/bpay-next-final-definers.mjs']))];
  const git=(commit,file)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',commit+':'+file],{maxBuffer:24e6});
  const sourcePins=files.map(file=>{const original=git(census.commit,file),current=git(checkpoint,file);assert(original.equals(current),'Source changed across pinned checkpoints: '+file);assert(current.equals(fs.readFileSync(root+'/'+file)),file);return{file,bytes:current.length,sha256:sha(current),rawUnchangedAcrossPinnedCommits:true,savedEqualsCommitted:true};});
  const authority={version:'S5_SOURCE_AUTHORITY_V1',ownPhysical:fs.realpathSync.native(own),rootPhysical:fs.realpathSync.native(root),
    sourceCommit:census.commit,rootDefinitionObservationCommit:checkpoint,sourcePins,
    noRuntimeTargets:true,parentBodiesReplaced:0,parentInversesRequired:0,
    lexicalCensusCaveat:'INITIAL_CENSUS.namedCalls is a lexical qualified-token census, not an executable call graph; public.audit_events is the INSERT relation in _temp_diag_log. SOURCE_PROOF supplies the scoped statement/control-flow inspection.',
    native:'NOT_RUN',databaseConnections:0,networkCalls:0,bankBodiesSelected:0};
  fs.writeFileSync(here+'/SOURCE_AUTHORITY.json',JSON.stringify(authority,null,2)+'\n');
  const env={...process.env,PYTHONDONTWRITEBYTECODE:'1'};
  const proof=spawnSync('python',[here+'/source-proof.py'],{cwd:own,env,encoding:'utf8'});assert.equal(proof.status,0,proof.stderr);
  const tests=spawnSync('python',[here+'/test-source-proof.py'],{cwd:own,env,encoding:'utf8'});assert.equal(tests.status,0,tests.stderr);
  fs.writeFileSync(here+'/SOURCE_TEST_RESULTS.txt',tests.stdout+tests.stderr);
  const inventory=[];
  function visit(dir,prefix=''){for(const entry of fs.readdirSync(dir,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){if(entry.name==='__pycache__')continue;const rel=prefix+entry.name;if(entry.isDirectory())visit(dir+'/'+entry.name,rel+'/');else if(rel!=='MANIFEST.json'){const b=fs.readFileSync(dir+'/'+entry.name);inventory.push({path:rel,bytes:b.length,sha256:sha(b)});}}}
  visit(here);
  const manifest={version:'S5_FALSE_ARM_SOURCE_ONLY_FREEZE_V1',sourceCommit:census.commit,definitionObservationCommit:checkpoint,
    scope:'EXACT_OFFICE_MANUAL_DAILY_SOURCE_FEASIBILITY_ONLY',native:'NOT_RUN',runtimeTargets:[],files:inventory};
  fs.writeFileSync(manifestPath,JSON.stringify(manifest,null,2)+'\n');
  console.log(JSON.stringify({files:inventory.length,manifestSha256:sha(fs.readFileSync(manifestPath)),sourceTests:12,native:'NOT_RUN'}));
}
