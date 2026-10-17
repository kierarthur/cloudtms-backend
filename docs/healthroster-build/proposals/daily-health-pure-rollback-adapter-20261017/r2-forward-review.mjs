// Complete source-only reverse to a20a. Does not call a client, tool or turn.
import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';import {here,ROOT,sha,requireThat} from './source.mjs';
const parent='a20a074114ed84173a56cf34c39e97eb8777101b',relative='docs/healthroster-build/proposals/daily-health-pure-rollback-adapter-20261017';
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',here,...args],{windowsHide:true,maxBuffer:2*1024*1024});
requireThat(/^D:[\\/]Development[\\/]GitHub[\\/]/.test(fs.realpathSync.native(here)),'R2_D_PHYSICAL_SOURCE_REQUIRED');
const parentFiles=git(['ls-tree','--full-tree','--name-only',parent+':'+relative]).toString('utf8').trim().split(/\r?\n/);
requireThat(parentFiles.length===22,'R2_WHOLE_PARENT_SET_REQUIRED');
const files=parentFiles.map(file=>{
 const before=git(['show',parent+':'+relative+'/'+file]),after=fs.readFileSync(path.join(here,file));
 const row={file,beforeSha256:sha(before),afterSha256:sha(after),unchanged:before.equals(after)};
 if(!row.unchanged){
  const a=before.toString('utf8').split(/(?<=\n)/),b=after.toString('utf8').split(/(?<=\n)/);let prefix=0,suffix=0;
  while(prefix<a.length&&prefix<b.length&&a[prefix]===b[prefix])prefix++;
  while(suffix<a.length-prefix&&suffix<b.length-prefix&&a[a.length-1-suffix]===b[b.length-1-suffix])suffix++;
  const startByte=Buffer.byteLength(a.slice(0,prefix).join('')),oldText=a.slice(prefix,a.length-suffix).join(''),newText=b.slice(prefix,b.length-suffix).join('');
  row.hunk={startByte,before:oldText,after:newText};
  requireThat(Buffer.concat([before.subarray(0,startByte),Buffer.from(newText),before.subarray(startByte+Buffer.byteLength(oldText))]).equals(after)
   &&Buffer.concat([after.subarray(0,startByte),Buffer.from(oldText),after.subarray(startByte+Buffer.byteLength(newText))]).equals(before),'R2_COMPLETE_LITERAL_REVERSE_REQUIRED');
 }
 return {...row,wholeParentReverse:true};
});
for(const file of ['SOURCE_PINS.json','source.mjs','fixed-metadata-reader.mjs','FORWARD_REVERSE.json','forward-review.mjs'])requireThat(files.find(r=>r.file===file)?.unchanged,'R2_PROTECTED_PREDECESSOR_CHANGED');
const readOld=file=>JSON.parse(git(['show',parent+':'+relative+'/'+file]).toString('utf8')),readNew=file=>JSON.parse(fs.readFileSync(path.join(here,file),'utf8'));
const oldManifest=readOld('SOURCE_MANIFEST.json'),newManifest=readNew('SOURCE_MANIFEST.json');
requireThat(oldManifest.sourcePinsSha256===newManifest.sourcePinsSha256,'R2_ROOT_PINS_CHANGED');
for(const f of newManifest.files)requireThat(f.sha256===sha(fs.readFileSync(path.join(here,f.file))),'R2_EXECUTABLE_MANIFEST_STALE');
const oldExamples=readOld('OFFLINE_INPUTS.json'),newExamples=readNew('OFFLINE_INPUTS.json'),example=(list,name)=>list.examples.find(r=>r.name===name).sql;
for(const name of ['ORIGINAL_PROGRAMME','SYNTHETIC_C_BEFORE','SYNTHETIC_C_AFTER'])requireThat(example(oldExamples,name)===example(newExamples,name),'R2_ORIGINAL_PROGRAMME_OR_CANONICAL_QUERY_CHANGED');
const {scanManagedSql}=await import(pathToFileURL(path.join(ROOT,'scripts/cloudtms-managed-release-sql.mjs')).href);
const oldB=example(oldExamples,'SYNTHETIC_B'),newB=example(newExamples,'SYNTHETIC_B'),parsed=scanManagedSql(newB),clockRows=parsed.statements
 .map(s=>parsed.source.slice(s.start,s.end).trim()).filter(s=>s.endsWith('as hr_guard_clock;'));
requireThat(clockRows.length===2,'R2_EXACT_TWO_NATIVE_CLOCK_FRAMES_REQUIRED');
let revertedB=newB;
for(const clock of clockRows){requireThat(revertedB.split(clock+'\n').length===2,'R2_CLOCK_REVERSE_ANCHOR');revertedB=revertedB.replace(clock+'\n',()=> '');}
const completionClocks=",\n  'transactionStartedAt',transaction_timestamp(),'statementStartedAt',statement_timestamp(),'observedAt',clock_timestamp()";
requireThat(revertedB.split(completionClocks).length===2,'R2_COMPLETION_CLOCK_ANCHOR');
revertedB=revertedB.replace(completionClocks,()=> '');requireThat(revertedB===oldB,'R2_ENTIRE_B_ONLY_DIAGNOSTIC_CLOCK_DELTA');
const verificationClocks=",\n 'transactionStartedAt',transaction_timestamp(),'statementStartedAt',statement_timestamp(),'observedAt',clock_timestamp()";
const fresh=example(newExamples,'SYNTHETIC_MCP_VERIFICATION');requireThat(fresh.split(verificationClocks).length===2,'R2_FRESH_CLOCK_ANCHOR');
requireThat(fresh.replace(verificationClocks,()=> '')===example(oldExamples,'SYNTHETIC_MCP_VERIFICATION'),'R2_FRESH_ONLY_DIAGNOSTIC_CLOCK_DELTA');
const genuine=readNew('GENUINE_R2_COMMAND_EVIDENCE.json');requireThat(sha(fs.readFileSync(genuine.sourceResultPath))===genuine.sourceResultSha256,'R2_GENUINE_EVIDENCE_CHANGED');
requireThat(genuine.accepted===false&&genuine.rolledBack===true&&genuine.failure.statement===5&&genuine.nativeProgrammeRan===false,'R2_NATIVE_HOLD_PRESERVED');
const data={version:'DAILY_PURE_R2_RESPONSE_FORWARD_COMPLETE_REVERSE_V1',parent,files,
 sourcePinsSha256:newManifest.sourcePinsSha256,predecessorExecutableManifestSha256:sha(git(['show',parent+':'+relative+'/SOURCE_MANIFEST.json'])),
 executableManifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),
 programmeAndOriginalGuard:'BYTE_UNCHANGED',wholeBReverse:'EXACT_AFTER_REMOVING_ONLY_TWO_GUARD_CLOCK_SELECTS_AND_COMPLETION_CLOCK_FIELDS',
 canonicalBeforeAndAfterSql:'BYTE_UNCHANGED',verificationReverse:'EXACT_AFTER_REMOVING_ONLY_ACTUAL_CLOCK_FIELDS',
 observedNativeR2:{status:'HOLD_NOT_QUALIFIED',sourceResultSha256:genuine.sourceResultSha256,programme:'NOT_RUN'},
 responseABI:{SET:'GENUINE_R2_OBSERVED_NULL_COUNT_EMPTY_FIELDS_ROWS',SELECT:'STRICT_INTEGER1_ONE_MATCHING_FIELD_ROW',
  DO_CREATE_RESET:'EXACT_CLOSED_SOURCE_INDEX_COMMAND_NULL_EMPTY_EXPECTATION_NATIVE_NOT_PROVED'},
 budgets:{workMilliseconds:120000,wholeMilliseconds:180000},
 newFiles:['GENUINE_R2_COMMAND_EVIDENCE.json','PG_RESULT_PARSER_EVIDENCE.json','timing.mjs','r2-forward-review.mjs','R2_FORWARD_REVERSE.json'],
 currentForwardNative:'NOT_RUN',dbConnections:0};
const encoded=JSON.stringify(data,null,2)+'\n';
if(process.argv[2]==='--write')fs.writeFileSync(path.join(here,'R2_FORWARD_REVERSE.json'),encoded);
else requireThat(fs.readFileSync(path.join(here,'R2_FORWARD_REVERSE.json'),'utf8')===encoded,'R2_REVERSE_EVIDENCE_STALE');
console.log(JSON.stringify({status:'WHOLE_A20_PARENT_REVERSE_PASS',parent,parentFiles:files.length,changed:files.filter(r=>!r.unchanged).length,
 programmeAndOriginalGuardUnchanged:true,canonicalQueriesUnchanged:true,sourcePinsSha256:newManifest.sourcePinsSha256,
 executableManifestSha256:data.executableManifestSha256,evidenceSha256:sha(encoded),nativeR2:'HOLD_NOT_QUALIFIED',currentForwardNative:'NOT_RUN'}));
