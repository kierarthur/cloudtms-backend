// Independent raw F23 census. Preliminary until Money's corrected frozen selection.
// Only exact selected Health SQL is read; seven excluded Banking bodies are absent.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const money='D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026';
const functional='9c791fa971c97a198c09c4e3579c01b589486c14',metadata='b9c1d0bad2e52ce95f7aa6b8e7f98ecbbd1fc832';
const finalMode=process.argv.includes('--final'),finalCommit='a7e4522cb87ae6961cb40efcb83e4e69b12fbd6c';
const packagePath='docs/healthroster-build/proposals/same-separate-health-current-successor-20261017';
const forwardPath='docs/healthroster-build/proposals/same-separate-daily-parent-forward-20261017';
const counterPath='docs/healthroster-build/proposals/same-separate-counter-forward-20261017';
const gatePath='scripts/bpay-next-source-gates.mjs',gateSha='2798cc6178aca812b82194659a6618bd94f12fdb404621783b5822b53a6a965f';
const sha=value=>createHash('sha256').update(value).digest('hex'),canon=value=>value.toString('utf8').replaceAll('\r\n','\n');
const git=(repo,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',repo,...args],{windowsHide:true,maxBuffer:12*1024*1024});
for(const folder of [here,root,money,process.cwd()])assert.match(fs.realpathSync.native(folder).replaceAll('\\','/'),/^D:\/Development\/GitHub(?:\/|$)/);
assert.equal(fs.realpathSync.native(root).replaceAll('\\','/'),root);
assert.equal(sha(fs.readFileSync(path.join(root,gatePath))),gateSha);
const committed=(commit,file,hash)=>{
 const bytes=git(money,['show',commit+':'+file]);if(hash)assert.equal(sha(bytes),hash,file);
 assert.ok(bytes.equals(fs.readFileSync(path.join(money,file))),'SAVED_COMMITTED_EQUAL '+file);return bytes;
};
const selectionCommit=finalMode?finalCommit:metadata,selectionPath=(finalMode?counterPath:forwardPath)+'/COMPOSED_SELECTION.json';
const selectionBytes=committed(selectionCommit,selectionPath,finalMode?'8b2c91e61a262f592d7ecae6b437f23d7277a5e565c7e4cc70076c0e21719c74':'0adf915b6d71f190267740c3356729c4710927962bf78c9dda16c070a6044a74');
const selection=JSON.parse(selectionBytes);assert.equal(selection.files.length,finalMode?84:72);assert.equal(selection.selectedHealthDefinitions,61);
if(finalMode)assert.equal(selection.featureDistinctHealthOwners,62);
const heldNames=new Set(selection.dependenciesHeld.map(r=>path.posix.basename(r.path)));assert.equal(heldNames.size,7);
assert.ok(selection.dependenciesHeld.every(r=>r.bodyRead===false));
const policy=JSON.parse(committed(metadata,forwardPath+'/SOURCE_POLICY_PROVENANCE.json'));
const actualPolicy=fs.readFileSync(policy.source);assert.equal(sha(actualPolicy),policy.sha256);
const lines=actualPolicy.toString('utf8').split(/\r?\n/);
for(const clause of policy.clauses)assert.equal(lines.slice(clause.firstLine-1,clause.lastLine).join('\n'),clause.text);
const {specialConstructViolations}=await import(pathToFileURL(path.join(root,gatePath)).href);
const files=[];
for(const pin of selection.files){
 if(!pin.target.startsWith('supabase/')||! /\.sql(?:inc)?$/i.test(pin.target))continue;
 assert.ok(!heldNames.has(path.posix.basename(pin.target)),'BANKING_BODY_EXCLUDED_BEFORE_READ');
 const sourceCommit=pin.sourceCommit??finalCommit,sourcePath=pin.sourceCommit?pin.sourcePath:counterPath+'/'+pin.sourcePath;
 assert.ok(sourceCommit===functional||finalMode&&sourceCommit===finalCommit);
 if(sourceCommit===functional)assert.equal(sourcePath,packagePath+'/payload/'+pin.target);
 const raw=committed(sourceCommit,sourcePath,pin.sha256);assert.equal(raw.length,pin.bytes);
 const source=canon(raw),points=new Map();
 for(const hazard of specialConstructViolations(source,pin.target)){
  const old=points.get(hazard.key);points.set(hazard.key,{...hazard,count:(old?.count??0)+1});
 }
 files.push({file:pin.target,sourceCommit,sourcePath,rawSha256:sha(raw),sha256:sha(source),points:[...points.values()]});
}
const counts={files:files.length,rawOccurrences:0,uniquePoints:0,kinds:{}};
for(const file of files)for(const point of file.points){counts.rawOccurrences+=point.count;counts.uniquePoints++;
 const kind=point.hazard.startsWith('ROWTYPE_SELECT:')?'ROWTYPE_SELECT':point.hazard;counts.kinds[kind]=(counts.kinds[kind]??0)+point.count;}
const data={version:finalMode?'SAME_SEPARATE_FINAL_RAW_F23_CENSUS_V1':'SAME_SEPARATE_PRELIMINARY_RAW_F23_CENSUS_V1',
 status:finalMode?'EXACT_FINAL_A7E452_SOURCE_CENSUS_NOT_NATIVE':'PRELIMINARY_AWAIT_CORRECTED_MONEY_FREEZE',functional,metadata,
 ...(finalMode?{finalCommit}:{}),selectionCommit,selectionPath,
 rootHead:git(root,['rev-parse','HEAD']).toString('utf8').trim(),gate:{path:gatePath,sha256:gateSha},selectionSha256:sha(selectionBytes),
 originalSourceAuthority:{path:policy.source,sha256:policy.sha256,clauses:policy.clauses.map(({firstLine,lastLine})=>({firstLine,lastLine}))},
 counts,files,native:'NOT_RUN',catalogueDependentBodies:'FOUR_NOT_PROVED',bankingBodiesRead:0};
fs.writeFileSync(path.join(here,finalMode?'CENSUS.json':'PRELIMINARY_CENSUS.json'),JSON.stringify(data,null,2)+'\n');
console.log(JSON.stringify({status:data.status,counts}));
const bySignature=new Map();
for(const file of files)for(const p of file.points){if(p.hazard.startsWith('ROWTYPE_SELECT:'))continue;
 const[sig]=JSON.parse(p.key),key=sig+'|'+p.hazard;bySignature.set(key,(bySignature.get(key)??0)+p.count);}
console.log(JSON.stringify([...bySignature].map(([signatureAndHazard,count])=>({signatureAndHazard,count}))));
