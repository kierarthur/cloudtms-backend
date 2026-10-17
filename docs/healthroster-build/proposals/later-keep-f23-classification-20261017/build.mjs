// Build finite proof data from exact mounted Health source and frozen owners.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{createHash}from'node:crypto';import{fileURLToPath,pathToFileURL}from'node:url';import{execFileSync}from'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..'),root=path.resolve(own,'../healthroster-install2-14102026'),money=path.resolve(own,'../healthroster-v2-money-build-13102026');
for(const p of[own,root,money])assert(fs.realpathSync(p).replaceAll('\\','/').startsWith('D:/Development/GitHub/'));
const canon=s=>s.toString().replaceAll('\r\n','\n'),sha=s=>createHash('sha256').update(s).digest('hex'),read=p=>canon(fs.readFileSync(root+'/'+p)),write=(p,s)=>{fs.mkdirSync(path.dirname(here+'/'+p),{recursive:true});fs.writeFileSync(here+'/'+p,s);};
const blob=(commit,p)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+p],{cwd:money,windowsHide:true,maxBuffer:40*1024*1024});
const {scanManagedSql}=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs')),{routineDefinitions}=await import(pathToFileURL(root+'/scripts/bpay-next-final-definers.mjs'));
const statements=s=>{const x=scanManagedSql(s);return x.statements.map(q=>x.source.slice(q.start,q.end).trim());};
const complete=(s,signature)=>{const a=statements(s).filter(q=>routineDefinitions(q,'owner').includes(signature));assert.equal(a.length,1,signature);return a[0];};
const reverse=(s,hunks)=>{for(const h of [...hunks].reverse()){const a=canon(h.after),b=canon(h.before);assert.equal(s.split(a).length,2,'LITERAL_REVERSE_ANCHOR');s=s.replace(a,()=>b);}return s;};
const prefix='docs/healthroster-build/proposals/',later='source-later-accept-current-successor-20261017',keep='kept-current-public-read-successor-20261017',lCommit='b6922e27c18312cd4f285e31c6ad99489612d577',kCommit='eb0aa9633fa284c088da6e7c12196974619cd48b';
const inputs=[],frozen=(commit,file)=>{const raw=blob(commit,file);inputs.push({commit,file,sha256:sha(raw),bytes:raw.length});return canon(raw);};
const census=JSON.parse(fs.readFileSync(here+'/CENSUS.json')),old=JSON.parse(read(prefix+'kept-entry-f23-classification-20261017/CLASSIFICATION.json'));
const schema=JSON.parse(read(old.schema.path)),relations={};
for(const f of census.files)for(const p of f.points){if(!p.hazard.startsWith('ROWTYPE_SELECT:'))continue;const name=p.hazard.split(':')[2].replaceAll(' ','').toLowerCase();if(relations[name])continue;const r=schema.relations.find(x=>x.schema+'.'+x.name===name);if(r)relations[name]={kind:r.kind,columns:r.columns};else{const prior=old.schema.relations[name];assert(prior?.ddl,'SOURCE_CATALOGUE_RELATION:'+name);assert(statements(read(prior.ddlPath)).includes(prior.ddl));relations[name]=prior;}}
// audit_events.after_json is the actual JSONB source of the new replay object.
for(const name of['public.audit_events'])if(!relations[name]){const r=schema.relations.find(x=>x.schema+'.'+x.name===name);assert(r);relations[name]={kind:r.kind,columns:r.columns};}
const ownerReverses=[];
for(const[packet,commit,dir]of[[later,lCommit,'17102026_0141_weekly_source_later_accept_health'],[keep,kCommit,'17102026_0209_weekly_source_keep_current_public_read']]){
 const edits=JSON.parse(frozen(commit,prefix+packet+'/CURRENT_OWNER_EDITS.json'));
 for(const e of edits.owners){const file='supabase/repeatable/'+dir+'/'+e.leaf,after=complete(read(file),e.signature),prior=complete(frozen(commit,prefix+packet+'/current/'+e.leaf),e.signature);assert.equal(reverse(after,e.hunks),prior);let currentFile=e.provenance.physical??null;
  if(currentFile)assert.equal(complete(read(currentFile),e.signature),prior,'ACTUAL_CURRENT_PARENT:'+currentFile);
  else{const parentCommit=e.provenance.commit,parentPath=e.provenance.virtualParent.startsWith('forward/')?prefix+later+'/'+e.provenance.virtualParent:prefix+e.provenance.virtualParent;assert.equal(complete(frozen(parentCommit,parentPath),e.signature),prior,'FROZEN_VIRTUAL_PARENT');}
  const saved='inputs/owners/'+String(ownerReverses.length+1).padStart(2,'0')+'.sqlinc';write(saved,prior+'\n');ownerReverses.push({file,signature:e.signature,hunks:e.hunks,currentFile,priorFile:prefix+'later-keep-f23-classification-20261017/'+saved,priorStatementSha256:sha(prior),successorStatementSha256:sha(after),provenance:e.provenance});
 }
}
const l=JSON.parse(frozen(lCommit,prefix+later+'/CLASSIFIER_EDITS.json')),k=JSON.parse(frozen(kCommit,prefix+keep+'/CLASSIFIER_EDITS.json'));
const entry=read(l.currentInputTarget),laterSource=read(l.target),keepSource=read(k.target),missingSource=reverse(laterSource,l.hunks);assert.equal(reverse(missingSource,l.parentCompositionHunks),entry);assert.equal(reverse(keepSource,k.hunks),laterSource);
assert.equal(reverse(entry,old.classifier.entryHunks),read(old.classifier.kept));assert.equal(reverse(read(old.classifier.kept),old.classifier.keptHunks),read(old.classifier.prior));
const classifier={prior:old.classifier.prior,kept:old.classifier.kept,entry:l.currentInputTarget,later:l.target,keep:k.target,keptHunks:old.classifier.keptHunks,entryHunks:old.classifier.entryHunks,missingHunks:l.parentCompositionHunks,laterHunks:l.hunks,keepHunks:k.hunks};
const dependencyPaths=[old.schema.path,'scripts/healthroster-kept-entry-type-review.mjs',prefix+'kept-entry-f23-classification-20261017/CLASSIFICATION.json',classifier.prior,classifier.kept,classifier.entry,...ownerReverses.filter(x=>x.currentFile).map(x=>x.currentFile)];
const dependencies=[...new Set(dependencyPaths)].map(p=>({path:p,sha256:sha(read(p))}));
const integrationCommit='ef64f43909db9811efaf1021d19f2fddc4bd44e6';assert.equal(execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','rev-parse','HEAD'],{cwd:root,encoding:'utf8',windowsHide:true}).trim(),integrationCommit);
const value={version:'HEALTH_LATER_KEEP_FINITE_TYPE_PROPOSAL_V1',qualification:'SOURCE_TYPES_AND_CONSTRUCTION_ONLY',native:'NOT_RUN',integrationCommit,baseCommit:census.baseCommit,mountReceipt:census.receipt,mountReceiptSha256:census.receiptSha256,counts:census.counts,files:census.files,schema:{path:old.schema.path,relations},ownerReverses,classifier,dependencies,frozenInputs:inputs,originalKeptEntryReviewSha256:'6309ea0cadd0d2cd3e98537c145cd8bb49aef984d74bbfe0f11075f54982a874',newRuntimeSql:false};
write('CLASSIFICATION.json',JSON.stringify(value,null,2)+'\n');console.log(JSON.stringify({counts:value.counts,relations:Object.keys(relations).length,ownerReverses:ownerReverses.length,dependencies:dependencies.length,reviewSha256:sha(fs.readFileSync(here+'/CLASSIFICATION.json'))}));
