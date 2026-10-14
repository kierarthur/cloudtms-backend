import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {spawnSync,execFileSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:own,encoding:null,maxBuffer:64*1024*1024});
const pins=JSON.parse(fs.readFileSync(path.join(dir,'THREE_WAY_INPUTS.json'))),proofs=[];
const lines=s=>s.match(/[^\n]*\n|[^\n]+$/g)??[];
for(const row of pins.files){const before=git(['show',pins.current_commit+':'+row.file]),after=fs.readFileSync(path.join(dir,row.output));
 if(sha(before)!==row.current_sha256||sha(after)!==row.joined_sha256)throw Error('SOURCE_CHANGED '+row.file);
 const input=path.join(dir,'.merge-inputs','reverse_before'),output=path.join(dir,'.merge-inputs','reverse_after');fs.writeFileSync(input,before);fs.writeFileSync(output,after);
 const d=spawnSync('git',['diff','--no-index','--no-ext-diff','--unified=0','--',input,output],{cwd:own,encoding:'utf8',maxBuffer:64*1024*1024});
 if(![0,1].includes(d.status))throw Error('DIFF_FAILED');
 const old=lines(before.toString()),next=lines(after.toString()),hunks=[];
 for(const m of d.stdout.matchAll(/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/gm)){
  const oldCount=Number(m[2]??1),newCount=Number(m[4]??1),oldStart=Number(m[1])-(oldCount?1:0),newStart=Number(m[3])-(newCount?1:0);
  const b=old.slice(oldStart,oldStart+oldCount).join(''),a=next.slice(newStart,newStart+newCount).join('');
  hunks.push({old_line_start:oldStart,old_line_count:oldCount,new_line_start:newStart,new_line_count:newCount,before:b,after:a,before_sha256:sha(b),after_sha256:sha(a)});
 }
 let restored=[...next],reapplied=[...old];
 for(const h of [...hunks].reverse()){restored.splice(h.new_line_start,h.new_line_count,...lines(h.before));reapplied.splice(h.old_line_start,h.old_line_count,...lines(h.after));}
 if(!Buffer.from(restored.join('')).equals(before)||!Buffer.from(reapplied.join('')).equals(after))throw Error('ENTIRE_REVERSE_FAILED '+row.file);
 proofs.push({file:row.file,input_commit:pins.current_commit,input_sha256:sha(before),output_sha256:sha(after),hunk_count:hunks.length,hunks,
  complete_reverse_matches_current_git:true,complete_forward_matches_saved:true});
}
fs.writeFileSync(path.join(dir,'FULL_RUNTIME_REVERSE.json'),JSON.stringify({schema_version:'INSTALL2_SHARED_RUNTIME_ENTIRE_SOURCE_REVERSE_V1',
 input_commit:pins.current_commit,common_commit:pins.common_commit,broad_commit:pins.broad_commit,files:proofs,no_native_execution:true},null,2)+'\n');
console.log(JSON.stringify(proofs.map(p=>({file:p.file,hunks:p.hunk_count,sha256:p.output_sha256,full_reverse:true}))));
