import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),old=path.resolve(out,'../strict-qualification-first-use-inventory-forward');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const from=" const rows=bytes.toString('utf8').split('\\0').filter(Boolean).map(line=>{";
const to=" if(!Buffer.isBuffer(bytes))fail('QUALIFICATION_GIT_TREE_INVALID');\n let decoded;\n try{decoded=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes);}\n catch{fail('QUALIFICATION_GIT_PATH_UTF8_INVALID');}\n if(!Buffer.from(decoded,'utf8').equals(bytes))fail('QUALIFICATION_GIT_PATH_UTF8_INVALID');\n const records=decoded.split('\\0');\n if(records.pop()!==''||records.some(record=>record.length===0))fail('QUALIFICATION_GIT_TREE_INVALID');\n const rows=records.map(line=>{";
const returnFrom="  return {mode:match[1],type:match[2],blob:match[3],path:match[4]};\n }).filter(row=>selected(row.path)).sort(pathCompare);";
const returnTo="  // All raw records are admitted or refused; none is filtered away.\n  // Fatal UTF8 plus roundtrip keeps the original path bytes authoritative.\n  if(!selected(match[4])||Buffer.from(match[4],'utf8').toString('utf8')!==match[4])\n   fail('QUALIFICATION_GIT_PATH_UNSUPPORTED');\n  return {mode:match[1],type:match[2],blob:match[3],path:match[4]};\n }).sort(pathCompare);";
const deltas=[];
for(const name of ['source-equivalence.mjs','weekly-source-protected-execution-tree.mjs']){
 const before=fs.readFileSync(path.join(old,name),'utf8');for(const anchor of [from,returnFrom])assert.equal(before.split(anchor).length-1,1);
 const after=before.replace(from,()=>to).replace(returnFrom,()=>returnTo);
 assert.equal(after.replace(to,()=>from).replace(returnTo,()=>returnFrom),before);
 fs.writeFileSync(path.join(out,name),after);deltas.push({file:name,predecessor_sha256:sha(Buffer.from(before)),successor_sha256:sha(Buffer.from(after)),reverse_matches_entire_predecessor:true});
}
fs.writeFileSync(path.join(out,'SOURCE_DELTAS.json'),JSON.stringify({schema_version:'STRICT_QUALIFICATION_FATAL_GIT_PATH_FORWARD_V1',source_only:true,deltas,new_signatures:0,new_grants:0,
 root_source_commit:'cea15289e0283e3383e71f0acb8b18ce184647c2',root_private_preserved:232,planned_complete_private_union:258,actual_native_acceptance:false,actual_activation:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,full_reverse_proofs:2,fatal_utf8:true,raw_record_filtering:false}));
