import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(out,'../protected-source-qualification-operational-gate'),sha=x=>createHash('sha256').update(x).digest('hex');
const proofs=[];
function emit(file,before,changes){let value=before;for(const h of changes){assert.equal(value.split(h.before).length-1,1);value=value.replace(h.before,()=>h.after);}
 let reverse=value;for(const h of [...changes].reverse()){assert.equal(reverse.split(h.after).length-1,1);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);
 fs.writeFileSync(path.join(out,file),value);proofs.push({file,input_sha256:sha(before),output_sha256:sha(value),reverse_hunk_matches_entire_input_definition:true,
  changes:changes.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after)}))});}
const sql=fs.readFileSync(path.join(out,'../strict-qualification-first-use-inventory-forward/OPERATIONAL_CERTIFICATE.sqlinc'),'utf8');
emit('SAME_INSTALLED_OPERATIONAL_CERTIFICATE.sqlinc',sql,[{before:"if executed.release_id=r.release_id or executed.status<>'VERIFIED'",after:"if executed.status<>'VERIFIED'"}]);
const js=fs.readFileSync(path.join(prior,'operational-evidence.mjs'),'utf8');
emit('same-installed-operational-evidence.mjs',js,[
 {before:"from './source-equivalence.mjs'",after:"from '../strict-qualification-git-path-parser-forward/source-equivalence.mjs'"},
 {before:'if(nativeContext.release_id===context.release_id||nativeContext.contract_sha256!==context.contract_sha256',
  after:'if((nativeContext.release_id===context.release_id&&nativeContext.git_commit!==context.git_commit)||nativeContext.contract_sha256!==context.contract_sha256'},
 {before:'if(native.release_id===q.release_id||native.contract_sha256!==q.contract_sha256',
  after:'if((native.release_id===q.release_id&&native.git_commit!==q.git_commit)||native.contract_sha256!==q.contract_sha256'}]);
fs.writeFileSync(path.join(out,'SAME_INSTALLED_CERTIFICATE_REVERSE.json'),JSON.stringify({source_only:true,
 no_release_identity_relabelled:true,no_prerequisite_or_partial_schema_accepted:true,no_remote_gate_removed:true,no_actual_acceptance:true,proofs},null,2)+'\n');
console.log(JSON.stringify({source_only:true,full_source_reverse_proofs:proofs.length,actual_acceptance:false}));
