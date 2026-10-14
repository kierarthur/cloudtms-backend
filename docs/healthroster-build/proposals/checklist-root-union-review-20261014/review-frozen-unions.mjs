// Independent source/caller review. Reads exact immutable Git objects only.
// Does not run preparation generators, alter another lane, or execute SQL/UI.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import test from 'node:test';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const office=path.join(path.dirname(root),'healthroster-v2-office-13102026');
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const commits=['58a2c5026c6f727c1112e8cdbe01df5ebf823caf','de90ab2134aca782a0f4b56154585fad65a81f12'];
const git=(commit,file,repo=root)=>execFileSync('git',['-c','maintenance.auto=false','show',commit+':'+file],{cwd:repo,maxBuffer:24e6});
const snapshots=new Map(),pins=[];
for(const commit of commits){const files=execFileSync('git',['diff-tree','--no-commit-id','--name-only','-r',commit],{cwd:root,encoding:'utf8'}).trim().split('\n');
  for(const file of files){const raw=git(commit,file);snapshots.set(path.join(root,file),raw);pins.push({commit,path:file,bytes:raw.length,sha256:hash(raw)});}}
const composeFile='scripts/healthroster-owner-source-compose.mjs',composeBytes=git(commits[0],composeFile);
snapshots.set(path.join(root,composeFile),composeBytes);
const compose=await import('data:text/javascript;base64,'+composeBytes.toString('base64'));
const officeMain=git('2d15d121ac888a2e2a076dc7a2c4be85f9958556','js/main.js',office);
assert.equal(hash(officeMain),'d86f2ef841f53243997c7f5bb73b913a4a04a2c563378ab47e58d1a5ee82c847');
const frozenFs={...fs,readFileSync(file,encoding){const key=path.resolve(String(file)),raw=snapshots.get(key);assert.ok(raw,'Only frozen Git-source reads allowed: '+key);return encoding?raw.toString(encoding):Buffer.from(raw);}};
const frozenExec=(command,args,options)=>{assert.equal(command,'git');assert.equal(options.cwd,office);
  assert.equal(args.at(-1),'2d15d121ac888a2e2a076dc7a2c4be85f9958556:js/main.js');return Buffer.from(officeMain);};
const tests=[['58a2c5026c6f727c1112e8cdbe01df5ebf823caf','tests/healthroster-owner-source-compose.test.mjs'],
  ['58a2c5026c6f727c1112e8cdbe01df5ebf823caf','tests/healthroster-office-workspace-union.test.mjs'],
  ['de90ab2134aca782a0f4b56154585fad65a81f12','tests/healthroster-office-paid-native-caller-union.test.mjs']];
const adaptations=[];
for(const [commit,file]of tests){const raw=git(commit,file).toString();let source=raw.replace(/^import .*;\r?\n/gm,'');
  const before="const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');";
  const compact="const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');";
  if(source.includes(before))source=source.replace(before,'const repo='+JSON.stringify(root)+';');
  else if(source.includes(compact))source=source.replace(compact,'const repo='+JSON.stringify(root)+';');
  assert.ok(!source.includes('import.meta'));
  adaptations.push({commit,file,original_sha256:hash(raw),harness_source_sha256:hash(source),
    changes:'ESM imports injected as exact native bindings; repository locator fixed to virtual immutable Git source view. Test assertions and business/host modules unchanged.'});
  const context=vm.createContext({fs:frozenFs,path,vm,crypto,execFileSync:frozenExec,fileURLToPath,test,assert,
    composeOwnerSource:compose.composeOwnerSource,sourceSha256:compose.sourceSha256,Buffer,URL,URLSearchParams,TextEncoder,TextDecoder,console,structuredClone});
  new vm.Script(source,{filename:commit+':'+file}).runInContext(context);
}
const result={status:'BOUNDED_INDEPENDENT_SOURCE_AND_IN_MEMORY_CALLER_REVIEW',source_only:true,native:false,SQL_executed:false,
  generators_executed:false,another_lane_edited:false,commits,source_files:pins.length,pins,adaptations,
  office_main:{commit:'2d15d121ac888a2e2a076dc7a2c4be85f9958556',path:'js/main.js',sha256:hash(officeMain)}};
fs.writeFileSync(path.join(here,'FROZEN_REVIEW_INPUT_PINS.json'),JSON.stringify(result,null,2)+'\n');
