// Forward packaging repair: retain every unchanged published Worker byte.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {repoRoot} from './cloudtms-db-release-lib.mjs';
import {canonical,digest} from './cloudtms-managed-release-sql.mjs';
const base='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';
const git=args=>execFileSync('git',args,{cwd:repoRoot,encoding:'utf8',maxBuffer:64000000,windowsHide:true});
const receipt=JSON.parse(fs.readFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/PP_SOURCE_COMPOSITION_RECEIPT.json'),'utf8'));
const records=[];
for(const file of ['broker/src/index.js','broker/src/weekly-source/routes.js']){
 const before=git(['show',`${base}:${file}`]),current=fs.readFileSync(path.join(repoRoot,file),'utf8');
 if(digest(canonical(current))!==receipt.edits.find(row=>row.file===file)?.afterSha256)throw Error('OWN_SOURCE_COMPOSITION_DRIFT');
 const diff=canonical(git(['diff','--ignore-space-at-eol','--unified=3',base,'--',file]));
 const hunks=diff.split(/(?=^@@ )/m).slice(1),lines=before.split('\n');
 for(const hunk of hunks.toReversed()){
  const header=hunk.match(/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/);
  if(!header)throw Error('WORKER_PATCH_HEADER_INVALID');
  const start=Number(header[1])-1,count=header[2]===undefined?1:Number(header[2]);
  const events=hunk.slice(hunk.indexOf('\n')+1).split('\n').filter(line=>/^[ +\-]/.test(line));
  const old=lines.slice(start,start+count),newLines=[];let cursor=0;
  const carriage=old[0]?.endsWith('\r')?'\r':'';
  for(const event of events){
   if(event[0]==='+')newLines.push(event.slice(1)+carriage);
   else{
    if(old[cursor]?.replace(/\r$/,'')!==event.slice(1))throw Error('WORKER_ORIGINAL_CONTEXT_DRIFT');
    if(event[0]===' ')newLines.push(old[cursor]);
    cursor++;
   }
  }
  if(cursor!==count)throw Error('WORKER_PATCH_LINE_CENSUS_DRIFT');
  lines.splice(start,count,...newLines);
 }
 const repaired=lines.join('\n');
 if(canonical(repaired)!==canonical(current))throw Error('WORKER_SEMANTIC_BYTES_CHANGED');
 fs.writeFileSync(path.join(repoRoot,file),repaired);
 records.push({file,baseRawSha256:digest(before),canonicalCompositionSha256:digest(canonical(current)),
  rawComposedSha256:digest(repaired),hunks:hunks.length,
  proof:'Every unchanged context and untouched line retains exact predecessor bytes; canonical composition is identical.'});
}
fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/PP_WORKER_BYTE_PRESERVATION.json'),JSON.stringify({base,status:'SOURCE_ONLY_CANONICAL_COMPOSITION_UNCHANGED',records},null,2)+'\n');
console.log(JSON.stringify({status:'WORKER_UNCHANGED_BYTES_RETAINED',records}));
