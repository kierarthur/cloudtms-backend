// Offline source/render/inverse proof only. No transport or database is opened.
import fs from'node:fs';import path from'node:path';import {fileURLToPath,pathToFileURL}from'node:url';import {buildProgram,CASES,sha}from'./build-program.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
if(fs.existsSync(here+'/MANIFEST.json'))throw Error('FROZEN_HISTORY');
const {exactAtomicBody,scanManagedSql}=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs').href),pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));
const plans={};for(const caseId of CASES)plans[caseId]=await buildProgram({root,caseId});
fs.writeFileSync(here+'/PROGRAMMES.json',JSON.stringify(plans,null,2)+'\n');
const inverses=[];
for(const plan of Object.values(plans))for(const transfer of plan.transfers){
 if(!transfer.onlyOuterClientTransactionFramingRemoved)continue;
 const pin=pins.rootInputs.find(r=>r.file===transfer.file),raw=fs.readFileSync(here+'/'+pin.copy,'utf8'),body=exactAtomicBody(raw),scan=scanManagedSql(raw);
 if(scan.source!==raw)throw Error('RAW_SOURCE_MUST_ALREADY_BE_CANONICAL');
 const removals=[...scan.meta,scan.statements[0],scan.statements.at(-1)].sort((a,b)=>a.start-b.start);let removedBytes=0;
 const framing=removals.map(s=>{const row={sourceStart:s.start,sourceEnd:s.end,bodyOffset:s.start-removedBytes,raw:raw.slice(s.start,s.end)};removedBytes+=s.end-s.start;return row;});
 let independentlyRemoved=raw;for(const s of [...removals].reverse())independentlyRemoved=independentlyRemoved.slice(0,s.start)+independentlyRemoved.slice(s.end);
 if(independentlyRemoved!==body||framing.map(x=>x.raw.trim()).join('|')!=='\\set ON_ERROR_STOP on|begin;|commit;')throw Error('EXACT_THREE_PACKAGING_SPANS_REQUIRED');
 const mapped=body.replace(/owner to postgres/gi,()=> 'OWNER TO CURRENT_USER');
 const executable=transfer.executableSha256===sha(body)?body:mapped;
 if(sha(executable)!==transfer.executableSha256||plan.sql.split(executable).length!==2)throw Error('COMPLETE_RETAINED_ATOMIC_BODY_REQUIRED');
 let restored=executable;
 const edits=[];let delta=0;
 if(executable!==body)for(const match of body.matchAll(/owner to postgres/gi)){const after='OWNER TO CURRENT_USER';edits.push({before:match[0],after,offset:match.index+delta});delta+=after.length-match[0].length;}
 for(const edit of [...edits].reverse()){if(restored.slice(edit.offset,edit.offset+edit.after.length)!==edit.after)throw Error('LITERAL_OWNER_REVERSE_RANGE_REQUIRED');restored=restored.slice(0,edit.offset)+edit.before+restored.slice(edit.offset+edit.after.length);}
 let full=restored;for(const row of [...framing].reverse())full=full.slice(0,row.bodyOffset)+row.raw+full.slice(row.bodyOffset);
 if(full!==raw)throw Error('WHOLE_RAW_ORIGINAL_PARENT_REVERSE_REQUIRED');
 inverses.push({caseId:plan.caseId,file:transfer.file,beforeRaw:raw,selectedExecutable:executable,originalPackagingSpans:framing,
  ownerEdits:edits,wholeOriginalSha256:sha(raw),executableSha256:sha(executable),
  wholeParentRawReverse:true,everyOriginalAssertionRetained:true});
}
fs.writeFileSync(here+'/PROGRAMME_PARENT_REVERSES.json',JSON.stringify({version:'CONTACT_QT_FOUR_ORIGINAL_ATOMIC_PARENT_REVERSES_V1',inverses,runtimeFilesEdited:0,originalSchemaHistoryEdited:false},null,2)+'\n');
const {buildInput}=await import('./input-builder.mjs'),offline={};
// This identity is exclusively a synthetic source render, never native evidence.
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:17,backendPid:101,turnId:'hr_turn_'+'1'.repeat(32),key:[20261013,1]};
for(const caseId of CASES)offline[caseId]={testData:'SYNTHETIC_SOURCE_RENDER_ONLY_NATIVE_NOT_RUN',...await buildInput({root,identity,caseId})};
fs.writeFileSync(here+'/OFFLINE_INPUTS.json',JSON.stringify(offline,null,2)+'\n');
console.log(JSON.stringify({programmes:CASES.length,completeParentInverses:inverses.length,native:'NOT_RUN'}));
