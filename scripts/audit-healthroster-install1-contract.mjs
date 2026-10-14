import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';
import {repoRoot,canonicalContractHash} from './cloudtms-db-release-lib.mjs';import {digest} from './cloudtms-managed-release-sql.mjs';
const base='07e66fb2b2742fbe634a41582f2ad3e004eed3b9',file='supabase/release/current-contract.json';
const before=JSON.parse(execFileSync('git',['show',`${base}:${file}`],{cwd:repoRoot,encoding:'utf8',maxBuffer:32000000}));
const after=JSON.parse(fs.readFileSync(path.join(repoRoot,file),'utf8'));
const changes=[];
for(const key of new Set([...Object.keys(before),...Object.keys(after)])){
 if(key==='routines')continue;
 if(JSON.stringify(before[key])!==JSON.stringify(after[key]))changes.push({kind:'outside-routine-contract',section:key});
}
const identity=row=>row.schema+'.'+row.identity;
const prior=new Map(before.routines.map(row=>[identity(row),row])),current=new Map(after.routines.map(row=>[identity(row),row]));
if(prior.size!==before.routines.length||current.size!==after.routines.length)throw Error('CONTRACT_ROUTINE_IDENTITY_DUPLICATE');
const allowed=new Set(JSON.parse(fs.readFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/PP_SOURCE_COMPOSITION_RECEIPT.json'),'utf8')).rows
 .map(row=>row.signature.slice(0,row.signature.indexOf('('))));
allowed.add('private._weekly_source_acl_service_rpc_contract_v1');
for(const [key,row] of prior){
 const next=current.get(key);
 if(!next)changes.push({kind:'removed-routine',identity:key});
 else if(JSON.stringify(row)!==JSON.stringify(next))changes.push({kind:'changed-routine',identity:key,
  name:row.schema+'.'+row.identity.slice(0,row.identity.indexOf('(')),
  fields:[...new Set([...Object.keys(row),...Object.keys(next)])].filter(field=>JSON.stringify(row[field])!==JSON.stringify(next[field])),before:row,after:next});
}
for(const [key,row] of current)if(!prior.has(key))changes.push({kind:'added-routine',identity:key,
 name:row.schema+'.'+row.identity.slice(0,row.identity.indexOf('(')),after:row});
const bad=changes.filter(row=>!['changed-routine','added-routine'].includes(row.kind)||!allowed.has(row.name));
const additions=changes.filter(row=>row.kind==='added-routine');
const exactExpected=additions.length===1&&additions[0].name==='private.weekly_source_pay_actor_allowed_v1'
 &&changes.length===17&&changes.filter(row=>row.kind==='changed-routine').length===16;
const report={status:bad.length===0&&exactExpected?'EXACT_PP_CONTRACT_DELTA_REVIEWED':'CONTRACT_DELTA_HELD',base,
 beforeHash:canonicalContractHash(before),afterHash:canonicalContractHash(after),
 sourceCommit:execFileSync('git',['rev-parse','HEAD'],{cwd:repoRoot,encoding:'utf8'}).trim(),
 routineCounts:{before:prior.size,after:current.size},changes,unexpected:bad,exactExpected,
 unchangedSections:Object.keys(before).filter(key=>key!=='routines'&&JSON.stringify(before[key])===JSON.stringify(after[key])),
 qualification:'Data-free clean local PostgreSQL17 schema contract only; no hosted release, full verifier/native financial/API claim.'};
fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/LOCAL_PP_CONTRACT_DELTA_REVIEW.json'),JSON.stringify(report,null,2)+'\n');
console.log(JSON.stringify({status:report.status,beforeHash:report.beforeHash,afterHash:report.afterHash,routineCounts:report.routineCounts,
 changes:changes.map(({kind,identity,fields})=>({kind,identity,fields})),unexpected:bad.map(row=>({kind:row.kind,identity:row.identity,section:row.section}))}));
if(report.status!=='EXACT_PP_CONTRACT_DELTA_REVIEWED')process.exitCode=1;
