// Source-only, complete bounded reverse to frozen b6d4. No SQL/client/turn call.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {here,sha,requireThat} from './source.mjs';
const parent='b6d4dc6bdd1267bc871c92fb3e5210c2eea63a07';
const relative='docs/healthroster-build/proposals/daily-health-pure-rollback-adapter-20261017';
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',here,...args],{windowsHide:true,maxBuffer:2*1024*1024});
requireThat(/^D:[\\/]Development[\\/]GitHub[\\/]/.test(fs.realpathSync.native(here)),'FORWARD_D_PHYSICAL_SOURCE_REQUIRED');
const parentFiles=git(['ls-tree','--full-tree','--name-only',parent+':'+relative]).toString('utf8').trim().split(/\r?\n/);
requireThat(parentFiles.length===20,'FORWARD_WHOLE_PARENT_SET_REQUIRED');
const files=parentFiles.map(file=>{
 const before=git(['show',parent+':'+relative+'/'+file]),after=fs.readFileSync(path.join(here,file));
 const row={file,beforeSha256:sha(before),afterSha256:sha(after),unchanged:before.equals(after)};
 if(!row.unchanged){
  const a=before.toString('utf8').split(/(?<=\n)/),b=after.toString('utf8').split(/(?<=\n)/);let prefix=0,suffix=0;
  while(prefix<a.length&&prefix<b.length&&a[prefix]===b[prefix])prefix++;
  while(suffix<a.length-prefix&&suffix<b.length-prefix&&a[a.length-1-suffix]===b[b.length-1-suffix])suffix++;
  const startByte=Buffer.byteLength(a.slice(0,prefix).join('')),oldText=a.slice(prefix,a.length-suffix).join(''),newText=b.slice(prefix,b.length-suffix).join('');
  row.hunk={startByte,before:oldText,after:newText};
  // Byte-bounded splices are literal: SQL $&, $', and backticks have no special semantics.
  const forward=Buffer.concat([before.subarray(0,startByte),Buffer.from(newText),before.subarray(startByte+Buffer.byteLength(oldText))]);
  const reverse=Buffer.concat([after.subarray(0,startByte),Buffer.from(oldText),after.subarray(startByte+Buffer.byteLength(newText))]);
  requireThat(forward.equals(after)&&reverse.equals(before),'FORWARD_COMPLETE_LITERAL_REVERSE_REQUIRED');
 }
 return {...row,wholeParentReverse:true};
});
for(const file of ['SOURCE_PINS.json','source.mjs','fixed-metadata-reader.mjs','control.mjs','SOURCE_TRANSFERS.json'])requireThat(files.find(r=>r.file===file)?.unchanged,'FORWARD_PROTECTED_PREDECESSOR_CHANGED');
const oldManifest=JSON.parse(git(['show',parent+':'+relative+'/SOURCE_MANIFEST.json']).toString('utf8')),
 newManifest=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'),'utf8'));
requireThat(oldManifest.sourcePinsSha256===newManifest.sourcePinsSha256,'FORWARD_ROOT_PINS_CHANGED');
for(const row of newManifest.files)requireThat(row.sha256===sha(fs.readFileSync(path.join(here,row.file))),'FORWARD_EXECUTABLE_MANIFEST_STALE');
const oldExamples=JSON.parse(git(['show',parent+':'+relative+'/OFFLINE_INPUTS.json']).toString('utf8')),
 newExamples=JSON.parse(fs.readFileSync(path.join(here,'OFFLINE_INPUTS.json'),'utf8'));
for(const name of ['ORIGINAL_PROGRAMME','SYNTHETIC_B','SYNTHETIC_C_BEFORE','SYNTHETIC_C_AFTER']){
 const a=oldExamples.examples.find(r=>r.name===name),b=newExamples.examples.find(r=>r.name===name);
 requireThat(a.sql===b.sql&&a.sha256===b.sha256,'FORWARD_UNCHANGED_SQL_SOURCE_REQUIRED');
}
const evidence={version:'DAILY_PURE_POOLING_FORWARD_COMPLETE_REVERSE_V1',parent,files,
 predecessorSourcePinsSha256:oldManifest.sourcePinsSha256,sourcePinsSha256:newManifest.sourcePinsSha256,
 predecessorExecutableManifestSha256:sha(git(['show',parent+':'+relative+'/SOURCE_MANIFEST.json'])),
 executableManifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),
 programmeAndBAndCanonicalBeforeAfterSql:'BYTE_UNCHANGED',
 executableChange:'Permit a fresh MCP client to reuse B/closed-C-before physical pooled backend; retain distinct independent C-after and genuine original qualifier; expose actual read-only state in small verification proof',
 addedEvidenceFiles:['forward-review.mjs','FORWARD_REVERSE.json'],native:'NOT_RUN',dbConnections:0};
const encoded=JSON.stringify(evidence,null,2)+'\n';
if(process.argv[2]==='--write')fs.writeFileSync(path.join(here,'FORWARD_REVERSE.json'),encoded);
else requireThat(fs.readFileSync(path.join(here,'FORWARD_REVERSE.json'),'utf8')===encoded,'FORWARD_REVIEW_EVIDENCE_STALE');
console.log(JSON.stringify({status:'WHOLE_B6D4_PARENT_REVERSE_PASS',parent,parentFiles:files.length,changed:files.filter(r=>!r.unchanged).length,
 sourcePinsUnchanged:true,pureProgrammeAndBAndCanonicalSqlUnchanged:true,executableManifestSha256:evidence.executableManifestSha256,
 evidenceSha256:sha(encoded),native:'NOT_RUN'}));
