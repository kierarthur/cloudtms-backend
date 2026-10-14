import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync,spawnSync} from 'node:child_process';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const current='c432594d9203421f009d3c7f21780f8c7df4f24e',before=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',`${current}:broker/src/index.js`],{cwd:own,maxBuffer:25000000});
const scratch=path.join(own,'.codex-tmp','a0-inherited-whitespace-current-index.js');assert.ok(path.resolve(scratch).startsWith(path.resolve(own)+path.sep));fs.mkdirSync(path.dirname(scratch),{recursive:true});fs.writeFileSync(scratch,before);
const lines=s=>s.match(/[^\n]*\n|[^\n]+$/g)||[],original=lines(before.toString());const files=[];
try{for(const folder of ['install2-a0-runtime-route-wiring-20261014','install2-a0-runtime-refusal-successor-20261014']){
 const file=`docs/healthroster-build/proposals/${folder}/forward/broker/src/index.js`,full=path.join(own,file),after=fs.readFileSync(full),next=lines(after.toString());
 const diff=spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','diff','--no-index','--no-ext-diff','--unified=0','--',scratch,full],{cwd:own,encoding:'utf8',maxBuffer:1000000});assert.ok([0,1].includes(diff.status));
 const hunks=[...diff.stdout.matchAll(/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/gm)].map(m=>({old_start:Number(m[1]),old_count:Number(m[2]??1),new_start:Number(m[3]),new_count:Number(m[4]??1)}));
 const proof=[];for(let i=0;i<next.length;i++){
  if(!/[\t ]+(?:\r?\n)?$/.test(next[i]))continue;const line=i+1;let offset=0;
  for(const h of hunks){assert.ok(!(line>=h.new_start&&line<h.new_start+h.new_count),'Changed line has whitespace');if(line>=h.new_start+h.new_count)offset+=h.new_count-h.old_count;}
  const oldLine=line-offset;assert.equal(next[i],original[oldLine-1]);proof.push({new_line:line,current_parent_line:oldLine,line_sha256:sha(next[i]),exact_current_parent_bytes:true});
 }
 files.push({path:file,sha256:sha(after),inherited_findings:proof.length,introduced_findings:0,proof});
}}finally{fs.unlinkSync(scratch);}
const result={schema_version:'A0_PUBLICATION_INHERITED_WHITESPACE_SOURCE_PROOF_V1',input_commit:current,current_index_sha256:sha(before),
 git_added_full_artifact_whitespace_check_pass:false,all_reported_whitespace_inherited:true,introduced_findings:0,files,source_only:true,native_calls:0};
fs.writeFileSync(path.join(here,'INHERITED_WHITESPACE_PROOF.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({files:files.length,findings:files.reduce((n,f)=>n+f.inherited_findings,0),introduced:0,git_full_added_copy_whitespace_pass:false}));
