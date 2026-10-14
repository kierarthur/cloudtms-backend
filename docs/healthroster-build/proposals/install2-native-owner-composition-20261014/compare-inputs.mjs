// Read the independently pinned complete inputs; write review drafts only.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
const folder=path.dirname(fileURLToPath(import.meta.url));
const receipt=JSON.parse(fs.readFileSync(path.join(folder,'INPUTS.json')));
const rows=[];
for(const row of receipt.owners){
 const files=Object.fromEntries(['actual','broad','common'].map(label=>[label,path.join(folder,row.slug+'.'+label+'.sqlinc')]));
 for(const [label,file]of Object.entries(files))assert.equal(createHash('sha256').update(fs.readFileSync(file,'utf8').slice(0,-1)).digest('hex'),row[label].sha256);
 const merge=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','merge-file','-p','--diff3','-L','CURRENT_BANKING_WP_PP','-L','COMMON_NATIVE_OWNER','-L','BROAD_HEALTHROSTER',files.actual,files.common,files.broad],{encoding:'utf8',windowsHide:true,maxBuffer:8_000_000});
 assert.ok(merge.status!==null&&merge.status>=0&&merge.status<128,'MERGE_TOOL_FAILED');
 fs.writeFileSync(path.join(folder,row.slug+'.draft.txt'),merge.stdout);
 for(const label of ['actual','broad']){const diff=spawnSync('git',['diff','--no-index','--no-ext-diff','--',files.common,files[label]],{encoding:'utf8',windowsHide:true,maxBuffer:8_000_000});assert.ok(diff.status===0||diff.status===1);fs.writeFileSync(path.join(folder,row.slug+'.'+label+'.diff'),diff.stdout);}
 rows.push({name:row.name,conflictBlocks:(merge.stdout.match(/^<<<<<<< /gm)??[]).length,mergeExit:merge.status,reviewOnly:true});
}
fs.writeFileSync(path.join(folder,'MERGE_REVIEW.json'),JSON.stringify({sourceOnly:true,status:'DRAFT_NOT_MOUNTED',rows},null,2)+'\n');
console.log(JSON.stringify(rows));
