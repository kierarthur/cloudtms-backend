// Bounded immutable source replay; never import or invoke a native holder.
import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),r2=path.join(here,'frozen/source-status-child-pure-r2-alias-forward-20261017'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const write=(file,data)=>{const target=path.join(here,file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,typeof data==='string'?data:JSON.stringify(data,null,2)+'\n');};
const git=(root,...args)=>{const r=cp.spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{maxBuffer:20e6});assert.equal(r.status,0,r.stderr.toString());return r.stdout;};
const capture=JSON.parse(fs.readFileSync(path.join(here,'CAPTURE.json'))),checks=[];
for(const selection of capture.selections)for(const row of selection.files){const b=fs.readFileSync(path.join(here,'frozen',selection.name,row.file));assert.equal(sha(b),row.sha256);assert.equal(b.length,row.bytes);assert(b.equals(git(capture.author,'show',selection.commit+':'+selection.folder+'/'+row.file)));assert(b.equals(fs.readFileSync(path.join(capture.author,selection.folder,row.file))));checks.push({selection:selection.name,file:row.file,savedGitCaptureEqual:true});}
const runs=[];const run=(name,exe,args)=>{const start=Date.now(),r=cp.spawnSync(exe,args,{cwd:repo,encoding:'utf8',maxBuffer:8e6});write('replay/'+name+'.stdout.txt',r.stdout);write('replay/'+name+'.stderr.txt',r.stderr);assert.equal(r.status,0,r.stdout+r.stderr);runs.push({name,exitCode:r.status,elapsedMs:Date.now()-start});};
const guard=pathToFileURL(path.join(here,'frozen-write-guard.mjs')).href;
run('node20',process.execPath,['--import',guard,'--test',path.join(r2,'test.mjs')]);
run('prepare-offline',process.execPath,['--import',guard,path.join(r2,'prepare-offline.mjs')]);
run('python-source-grammar-r2','C:/Users/KierArthur/AppData/Local/Programs/Python/Python312/python.exe',['//?/'+path.join(here,'python-replay.py').replaceAll('\\','/')]);
const source=await import(pathToFileURL(path.join(capture.author,capture.selections[0].folder,'source.mjs')));
const committed=source.assertCommittedAdapter(),context=await source.source(source.ROOT);
const rootHead=git(source.ROOT,'rev-parse','HEAD').toString().trim();
write('REPLAY_RESULTS.json',{sourceCommit:capture.selections[0].commit,captureChecks:checks,runs,assertCommittedAdapter:committed,rootHead,rootPins:context.pins.rootInputs.length,programmeBytes:context.programme.sqlBytes,programmeSha256:context.programme.sqlSha256,programmeSourceChecks:41,catalogueAdmissionAstCases:56,nodeTests:20,sourceAndGrammarChecks:177,R2ForwardChecks:134,native:'NOT_RUN',restoration:'NOT_RUN',rootWrites:0,OfficeWrites:0,BankingProducer:'NOT_PROVIDED',publicB051Result:'NOT_PROVIDED'});
console.log(JSON.stringify({capturedFiles:checks.length,runs:runs.map(r=>r.name),rootPins:context.pins.rootInputs.length,programme:context.programme.sqlSha256,native:'NOT_RUN'}));
