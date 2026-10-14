// Independent source review of immutable Git objects. No SQL or generator runs.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import test from 'node:test';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const commit='62b72fa4a3b23a9c1936127184b40d0e2a56366d';
const base='docs/healthroster-build/proposals/a0-source-selector-domain-construction-20261014/';
const sha=raw=>crypto.createHash('sha256').update(raw).digest('hex');
const git=(rev,file)=>execFileSync('git',['-c','maintenance.auto=false','cat-file','blob',rev+':'+file],{cwd:repo,maxBuffer:5e6});
const files=execFileSync('git',['diff-tree','--no-commit-id','--name-only','-r',commit],{cwd:repo,encoding:'utf8'}).trim().split('\n');
const blobs=new Map(),pins=[];
function capture(rev,file){const raw=git(rev,file);blobs.set(path.join(repo,file),raw);pins.push({commit:rev,file,bytes:raw.length,sha256:sha(raw)});return raw;}
for(const file of files)capture(commit,file);
const scannerFile='scripts/cloudtms-managed-release-sql.mjs',scannerBytes=capture(commit,scannerFile);
const {scanManagedSql}=await import('data:text/javascript;base64,'+scannerBytes.toString('base64'));
const reverse=JSON.parse(blobs.get(path.join(repo,base+'COMPLETE_CURRENT_REVERSES.json')));
const originalGit=new Map();for(const r of reverse.records)originalGit.set(r.source_commit+':'+r.source_path,capture(r.source_commit,r.source_path));
const local=p=>p instanceof URL?fileURLToPath(p):String(p);
const frozenFs={readFileSync(p,encoding){const raw=blobs.get(path.resolve(local(p)));assert.ok(raw,'Unpinned file read: '+local(p));return encoding?raw.toString(encoding):Buffer.from(raw);},
  readdirSync(p){assert.equal(path.resolve(local(p)),path.join(repo,base,'forward'));return files.filter(f=>f.startsWith(base+'forward/')).map(f=>path.basename(f));}};
const frozenExec=(command,args)=>{assert.equal(command,'git');assert.equal(args[0],'cat-file');assert.equal(args[1],'blob');const raw=originalGit.get(args[2]);assert.ok(raw,'Unpinned Git input');return Buffer.from(raw);};
const testFile='tests/healthroster-a0-source-selector-domain.test.mjs',raw=blobs.get(path.join(repo,testFile)).toString();
let source=raw.replace(/^import .*;\r?\n/gm,'');
source=source.replace('import.meta.url',JSON.stringify(new URL('file:///'+path.join(repo,testFile).replaceAll('\\','/')).href));
assert.ok(!source.includes('import.meta'));
new vm.Script(source,{filename:commit+':'+testFile}).runInContext(vm.createContext({fs:frozenFs,execFileSync:frozenExec,scanManagedSql,
  test,assert,createHash:crypto.createHash,URL,Buffer,console}));
const lexical=files.filter(f=>f.startsWith(base+'forward/')).map(file=>{const raw=blobs.get(path.join(repo,file)),parsed=scanManagedSql(raw.toString());
  assert.equal(parsed.meta.length,0);return {file,statements:parsed.statements.length,sha256:sha(raw)};});
assert.equal(lexical.reduce((n,x)=>n+x.statements,0),31);
const manifests=JSON.parse(blobs.get(path.join(repo,'docs/healthroster-build/root-a0-source-selector-domain-source-20261014/SHA256_MANIFEST.json')));
for(const m of manifests){const raw=blobs.get(path.join(repo,'docs/healthroster-build/root-a0-source-selector-domain-source-20261014/'+m.file));assert.equal(raw.length,m.bytes);assert.equal(sha(raw),m.sha256);}
const evidence={status:'BOUNDED_INDEPENDENT_SOURCE_REVIEW',commit,SQL_executed:false,native:false,generators_executed:false,another_lane_edited:false,
  pins,lexical,adaptation:{testFile,original_sha256:sha(raw),harness_source_sha256:sha(source),
    changes:'ESM imports bound to pinned scanner and built-ins; import.meta.url fixed to immutable virtual Git source. All nine test bodies/assertions unchanged.'}};
fs.writeFileSync(path.join(here,'FROZEN_REVIEW_INPUT_PINS.json'),JSON.stringify(evidence,null,2)+'\n');
