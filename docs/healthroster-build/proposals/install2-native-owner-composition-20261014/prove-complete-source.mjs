// Whole-source reversible transformations are source review evidence only.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const folder=path.dirname(fileURLToPath(import.meta.url));
const decisions=JSON.parse(fs.readFileSync(path.join(folder,'COMPLETE_OWNER_DECISIONS.json')));
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const lines=source=>source.match(/[^\n]*\n|[^\n]+$/g)??[];
const result=[];
for(const row of decisions.owners){
 const slug=row.file.replace('.complete.sqlinc','');
 const beforePath=path.join(folder,slug+'.actual.sqlinc'),afterPath=path.join(folder,row.file);
 const before=fs.readFileSync(beforePath,'utf8'),after=fs.readFileSync(afterPath,'utf8');
 assert.equal(hash(after),row.completeFileSha256);
 const diff=spawnSync('git',['diff','--no-index','--no-ext-diff','--unified=0','--',beforePath,afterPath],{encoding:'utf8',windowsHide:true,maxBuffer:8_000_000});
 assert.ok(diff.status===0||diff.status===1);
 const records=diff.stdout.split('\n'),hunks=[];let active=null;
 for(const line of records){const header=line.match(/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/);
  if(header){active={oldLine:Number(header[1]),oldCount:Number(header[2]??1),newLine:Number(header[3]),newCount:Number(header[4]??1),before:'',after:''};hunks.push(active);continue;}
  if(!active)continue;
  if(line.startsWith('-'))active.before+=line.slice(1)+'\n';
  else if(line.startsWith('+'))active.after+=line.slice(1)+'\n';
  else if(line==='\\ No newline at end of file')throw Error('FULL_SOURCE_TRAILING_NEWLINE_REQUIRED');
 }
 const apply=(source,reverse)=>{const vector=lines(source);
  for(const h of [...hunks].reverse()){
   const count=reverse?h.newCount:h.oldCount,start=(reverse?h.newLine:h.oldLine)-(count?1:0);
   const expected=reverse?h.after:h.before,replacement=reverse?h.before:h.after;
   assert.equal(vector.slice(start,start+count).join(''),expected,row.name+' EXACT_HUNK_BYTES');
   vector.splice(start,count,...lines(replacement));
  }
  return vector.join('');
 };
 assert.equal(apply(before,false),after,row.name+' WHOLE_SOURCE_FORWARD');
 assert.equal(apply(after,true),before,row.name+' WHOLE_SOURCE_REVERSE');
 result.push({...row,beforeFile:slug+'.actual.sqlinc',beforeFileSha256:hash(before),hunks,completeForwardMatches:true,completeReverseMatches:true});
}
fs.writeFileSync(path.join(folder,'FULL_NATIVE_OWNER_REVERSE.json'),JSON.stringify({sourceOnly:true,status:'WHOLE_SOURCE_TRANSFORMATIONS_PASS_NATIVE_UNPROVED',owners:result},null,2)+'\n');
console.log(JSON.stringify({sourceOnly:true,owners:result.length,hunks:result.reduce((n,row)=>n+row.hunks.length,0),allCompleteForwardAndReverse:true}));
