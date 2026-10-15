import fs from 'node:fs';
import cp from 'node:child_process';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
const [root,out]=process.argv.slice(2);
const commit='ecbd37def2626b109c5cf609835e682667e96e3c';
const base='docs/healthroster-build/proposals/agency-source-phase-active-capture-forward-20261015/';
const sha=b=>createHash('sha256').update(b).digest('hex');
const blob=(rev,p)=>cp.execFileSync('git',['cat-file','blob',rev+':'+p],{cwd:root,maxBuffer:8e6});
const saved=p=>fs.readFileSync(path.join(root,base,p));
function seal(){const raw=blob(commit,base+'REVIEW_MANIFEST.json'),m=JSON.parse(raw);for(const e of [...m.entries,{path:'REVIEW_MANIFEST.json'}]){const g=blob(commit,base+e.path);assert.deepEqual(g,saved(e.path));if(e.sha256){assert.equal(g.length,e.bytes);assert.equal(sha(g),e.sha256);}}return{artifacts:m.entries.length+1,manifest_sha256:sha(raw)};}
fs.mkdirSync(out,{recursive:true});const before=seal();
const reverse=JSON.parse(blob(commit,base+'COMPLETE_SOURCE_PHASE_REVERSES.json'));
const owners=reverse.owners??reverse.entries??reverse.reverses;
assert(Array.isArray(owners),'actual reverse ledger array required');
let hunks=0;
for(const entry of owners){let s=blob(commit,base+entry.output).toString();assert.equal(sha(s),entry.after_sha256);for(const h of [...entry.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);hunks++;}const rev=entry.output.startsWith('92_')?'7f2ce2fb1e33aa7cd77d294e464e5cd98aa7f2a3':'b9100ea9d9b8f84098c593d5c6b80ead373f1e2b';assert.deepEqual(Buffer.from(s),blob(rev,entry.input));assert.equal(sha(s),entry.before_sha256);}
assert.equal(owners.length,5);assert.equal(hunks,16);
const suites=[[base+'source-phase-admission.test.mjs',20],['docs/healthroster-build/proposals/agency-business-entry-inline-source-forward-20261014/construction/business-inline.test.mjs',58]];
const tests=[];
for(const [file,count] of suites){const r=cp.spawnSync(process.execPath,['--test','--test-reporter=tap',file],{cwd:root,encoding:'utf8',maxBuffer:8e6});const b=Buffer.from(r.stdout+r.stderr);assert.equal(r.status,0);assert.match(b.toString(),new RegExp('# pass '+count+'\\b'));assert.match(b.toString(),/# fail 0\b/);const name=path.basename(file)+'.tap.txt';fs.writeFileSync(path.join(out,name),b);tests.push({file,count,output_sha256:sha(b)});}
const checks={source_only:true,native:false,mount_ready:false,commit,before,after:seal(),whole_owner_reverses:owners.length,unique_hunks:hunks,tests,confirmed_native_rotation_field:'booking_id',comparison_and_lineage_update_counterexamples:'withdrawn: actual immutable append-only contract',remaining_implementation:['precise cross-group policy-date membership and actual resolved-policy transitive parents','raw/nested trigger registration and complete current owner integration'],unrun:['PostgreSQL current compiled roles/contract/registry','actual Candidate/I1/Source/order/concurrency/hidden-RR/native phase and complete financial acceptance']};
fs.writeFileSync(path.join(out,'CHECKS.json'),JSON.stringify(checks,null,2)+'\n');console.log(JSON.stringify(checks));
