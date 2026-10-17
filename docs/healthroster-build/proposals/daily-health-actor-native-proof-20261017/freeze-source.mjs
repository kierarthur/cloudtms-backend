// Source freeze only. No database, credential, turn, holder or tool invocation.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';import {createHash} from 'node:crypto';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',parent='b5809c06b2e6c6e8ad4c671a8bbee647cac44729',old='docs/healthroster-build/proposals/daily-health-pure-rollback-adapter-20261017';
const sha=x=>createHash('sha256').update(x).digest('hex'),git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,maxBuffer:10*1024*1024});
assert.match(fs.realpathSync.native(here).replaceAll('\\','/'),/^D:\/Development\/GitHub\/\.codex-worktrees\/healthroster-checklist-native-14102026\//);
const sourcePins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));sourcePins.businessSnapshotScope=['private.candidate_daily_batch_receipts','public.tms_users','public.ts_financials_outbox'];sourcePins.businessProgrammeReadsAndWrites=['public.tms_users'];
sourcePins.catalogueAndLedger='WHOLE_UNCHANGED_ORIGINAL_CANONICAL_QUERY';sourcePins.native='NOT_RUN';
for(const row of sourcePins.rootInputs){const b=fs.readFileSync(root+'/'+row.file);assert.equal(sha(b),row.sha256);assert.equal(b.length,row.bytes);assert(b.equals(git(['show',sourcePins.rootCommit+':'+row.file])));}
const forward=JSON.parse(fs.readFileSync(here+'/ROOT_SOURCE_FORWARD.json'));assert.equal(forward.runtimeUnchangedSuccessor.currentRoot,sourcePins.rootCommit);for(const row of sourcePins.rootInputs)assert(git(['show',forward.currentRoot+':'+row.file]).equals(git(['show',sourcePins.rootCommit+':'+row.file])));assert.equal(forward.changed.length,1);assert.equal(forward.unchanged.length,48);
for(const row of forward.changed){const before=fs.readFileSync(here+'/'+row.fullParent),after=fs.readFileSync(here+'/'+row.fullSuccessor);assert.equal(sha(before),row.beforeSha256);assert.equal(sha(after),row.afterSha256);assert(before.equals(git(['show',forward.priorRoot+':'+row.file])));assert(after.equals(git(['show',forward.currentRoot+':'+row.file])));let reversed=after.toString();for(const h of row.hunks){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,before.toString());}
for(const row of forward.unchanged){const prior=git(['show',forward.priorRoot+':'+row.file]),current=git(['show',forward.currentRoot+':'+row.file]);assert(prior.equals(current));assert.equal(sha(prior),row.sha256);}
fs.writeFileSync(here+'/SOURCE_PINS.json',JSON.stringify(sourcePins,null,2)+'\n');
const files=['source.mjs','input-builder.mjs','fixed-metadata-reader.mjs','control.mjs','timing.mjs','result-validator.mjs','hold-pure.mjs','prepare-offline.mjs','grammar.py'];
const transfers=[],diffs=[];
for(const name of files){const target=name==='hold-pure.mjs'?'hold-actor.mjs':name,before=fs.readFileSync(here+'/reference/pure/'+name),after=fs.readFileSync(here+'/'+target);assert(before.equals(git(['show',parent+':'+old+'/'+name])));
 const a=after.toString(),b=before.toString();assert.equal(a.split(a).length,2);assert.equal(a.replace(a,()=>b),b);
 transfers.push({file:target,parentCommit:parent,parentPath:old+'/'+name,completeParent:'reference/pure/'+name,inputSha256:sha(before),outputSha256:sha(after),fullLiteralReverse:true});
 try{execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','diff','--no-index','--no-ext-diff','--src-prefix=parent/','--dst-prefix=actor/','--',here+'/reference/pure/'+name,here+'/'+target],{windowsHide:true,maxBuffer:5*1024*1024});}
 catch(e){assert.equal(e.status,1);diffs.push(e.stdout.toString());}
}
fs.writeFileSync(here+'/SOURCE_TRANSFERS.json',JSON.stringify({version:'DAILY_ACTOR_FULL_PARENT_TRANSFERS_V1',parentCommit:parent,files:transfers,
 unchangedOriginalWeakMap:true,fullCanonicalCatalogueLedgerPreserved:true,completeUsersAndBothBackgroundRelations:true,budgetsChanged:false,originalPureHistoryRetained:true,native:'NOT_RUN'},null,2)+'\n');
fs.writeFileSync(here+'/ADAPTER_DIFF.patch',diffs.join('\n'));
const executable=['source.mjs','build-program.mjs','input-builder.mjs','fixed-metadata-reader.mjs','control.mjs','result-validator.mjs','timing.mjs','hold-actor.mjs','SETUP_ENTRY.md','NATIVE_DEPENDENCY_DIAG_R1.json'];
const manifest={version:'DAILY_HEALTH_ACTOR_ADAPTER_EXECUTABLE_V1',sourcePinsSha256:sha(fs.readFileSync(here+'/SOURCE_PINS.json')),files:executable.map(file=>({file,sha256:sha(fs.readFileSync(here+'/'+file))})),native:'NOT_RUN'};
fs.writeFileSync(here+'/SOURCE_MANIFEST.json',JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({sourceFiles:sourcePins.rootInputs.length,transfers:transfers.length,executable:executable.length,sourcePinsSha256:manifest.sourcePinsSha256,native:'NOT_RUN'}));
