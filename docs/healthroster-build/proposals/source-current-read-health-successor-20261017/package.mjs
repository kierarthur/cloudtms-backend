import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';
import {here,repo,parent,sha,get,write} from './build.mjs';import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const edits=JSON.parse(fs.readFileSync(path.join(here,'CURRENT_OWNER_EDITS.json'))),read=p=>fs.readFileSync(path.join(here,p),'utf8');
let names;if(fs.existsSync(path.join(here,'MOUNT_FILENAMES.json')))names=JSON.parse(read('MOUNT_FILENAMES.json'));else{
 const output=execFileSync(process.execPath,['scripts/cloudtms-db-release.mjs','new','--kind=repeatable','--name=weekly_source_current_read_health_v1'],{cwd:repo,encoding:'utf8',windowsHide:true});
 const relative=output.match(/^Created (supabase\/[^\r\n]+)$/m)?.[1];assert(relative&&/^supabase\/repeatable\/17102026_\d{4}_/.test(relative));assert(relative>'supabase/repeatable/17102026_0335_weekly_source_paid_match_correction_receipt_v1.sql');
 const source=path.resolve(repo,relative),destination=path.resolve(here,'payload',relative);assert(source.startsWith(repo+path.sep)&&destination.startsWith(here+path.sep));
 const scaffold=fs.readFileSync(source);fs.mkdirSync(path.dirname(destination),{recursive:true});fs.renameSync(source,destination);
 const stamp=path.posix.basename(relative).slice(0,13);names={repeatable:relative,verification:'supabase/verification/'+stamp+'_source_current_read_security.sql',createdAtUtc:new Date().toISOString(),command:'node scripts/cloudtms-db-release.mjs new --kind=repeatable --name=weekly_source_current_read_health_v1',scaffoldSha256:sha(scaffold)};write('MOUNT_FILENAMES.json',JSON.stringify(names,null,2)+'\n');
}
const root=names.repeatable,folder=root.replace(/_v1[.]sql$/,''),leaves=[],routines=[];
for(const [dir,paths]of [['authored',['01_native_status_child.sqlinc','02_current_read_access.sqlinc']],['forward/native',['03_complete_native_gate.sqlinc','04_complete_internal_source_read.sqlinc','05_complete_internal_service_read.sqlinc','06_complete_public_source_accept_read.sqlinc','98_complete_current_classification.sqlinc']]])for(const leaf of paths){
 const raw=read(dir+'/'+leaf),events=functionEvents(raw).events.filter(e=>e.kind==='CREATE');assert(events.length>0);for(const e of events){assert(e.id.startsWith('private.weekly_source_pay_decision_')||e.id.startsWith('public.weekly_source_pay_decision_')||e.id.startsWith('private._weekly_source_'));routines.push({signature:e.id,physical:folder+'/'+leaf,definitionSha256:sha(e.definition),header:e.header});}
 write('payload/'+folder+'/'+leaf,raw);leaves.push(leaf);
}
assert.equal(routines.length,12);assert.equal(new Set(routines.map(r=>r.signature)).size,12);
let acl='';for(const r of routines)acl+=`alter function ${r.signature} owner to postgres;\nrevoke all on function ${r.signature} from public,anon,authenticated,service_role;\n`+(r.signature.startsWith('public.')?`grant execute on function ${r.signature} to service_role;\n`:'');
write('payload/'+folder+'/99_acl.sqlinc',acl+"notify pgrst,'reload schema';\n");leaves.push('99_acl.sqlinc');
write('payload/'+root,'\\set ON_ERROR_STOP on\nbegin;\n'+leaves.map(l=>'\\ir '+path.posix.basename(folder)+'/'+l).join('\n')+'\ncommit;\n');
for(const f of edits.files.filter(f=>f.target.startsWith('broker/')&&f.savedWholeOutput))write('payload/'+f.target,read('forward/'+f.target));
write('INDEX_PARENT_EDITS.json',JSON.stringify(edits.files.find(f=>f.target==='broker/src/index.js'),null,2)+'\n');
const sameCommit='a7e4522cb87ae6961cb40efcb83e4e69b12fbd6c',samePrefix='docs/healthroster-build/proposals/same-separate-counter-forward-20261017/',same=JSON.parse(get(sameCommit,samePrefix+'COMPOSED_SELECTION.json')),rf=same.files.find(f=>f.target==='docs/bpay-next/final-definer-closures.json'),input=get(rf.sourceCommit??sameCommit,rf.sourceCommit?rf.sourcePath:samePrefix+rf.sourcePath),registry=JSON.parse(input),registryChanges=[];
for(const r of routines){const at=registry.chains.findIndex(c=>c.signature===r.signature),before=at<0?null:structuredClone(registry.chains[at]);let after;
 if(before)after={...before,finalRoot:root,finalPhysical:r.physical,allowedPhysical:[...new Set([...before.allowedPhysical,r.physical])],predecessors:[...new Set([...before.predecessors,before.finalRoot])],finalClosures:[...new Set([...before.finalClosures,root])],healthSourceReadReview:'docs/healthroster-build/proposals/source-current-read-health-successor-20261017/INTEGRATION.md'};
 else{assert(edits.newPrivate.includes(r.signature));after={signature:r.signature,finalRoot:root,finalPhysical:r.physical,allowedPhysical:[r.physical],predecessors:[],finalClosures:[root],owner:'Health Source current-read and exact native child consumer',healthSourceReadReview:'docs/healthroster-build/proposals/source-current-read-health-successor-20261017/INTEGRATION.md'};}
 if(at<0)registry.chains.push(after);else registry.chains[at]=after;registryChanges.push({signature:r.signature,before,after});
}
const serial=o=>JSON.stringify(o,null,2).replace(/[^\x00-\x7f]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0'))+'\n',reverse=structuredClone(registry);
for(const e of [...registryChanges].reverse()){const at=reverse.chains.findIndex(c=>c.signature===e.signature);assert(at>=0);if(e.before)reverse.chains[at]=e.before;else reverse.chains.splice(at,1);}assert.equal(serial(reverse),input);
write('current/final-definer-closures.json',input);write('payload/docs/bpay-next/final-definer-closures.json',serial(registry));write('REGISTRY_EDITS.json',JSON.stringify({virtualParent:{commit:sameCommit,path:samePrefix+rf.sourcePath,sha256:sha(input)},inputSha256:sha(input),outputSha256:sha(serial(registry)),changes:registryChanges,fullRawReverse:true,broaderCurrentParentReconciliation:'REQUIRED_BEFORE_FREEZE'},null,2)+'\n');
write('EXPECTED_ACL_DELTA.json',JSON.stringify({purpose:'Independent literal expected sets, never derived from runtime classifiers at verification time.',newRoutineSignatures:edits.newPrivate,newServiceSignatures:[],privateHelpers:[
 'private.weekly_source_pay_decision_native_normal_gate_v1(uuid,uuid,uuid,text)',
 'private.weekly_source_pay_decision_native_status_child_v1(jsonb,uuid,uuid,uuid,uuid)',
 'private.weekly_source_pay_decision_read_access_v1(uuid,uuid,uuid)',
 'private.weekly_source_pay_decision_read_source_v1(uuid,uuid,uuid)'],serviceRpcs:[
 'public.weekly_source_pay_decision_accept_v1(jsonb)','public.weekly_source_pay_decision_read_source_v1(jsonb)'],newRelations:[],newLifecycleColumns:[],broaderPreExistingSourceInventory:'SEPARATE_REQUIRED_IMPLEMENTATION_OBLIGATION',native:'NOT_RUN'},null,2)+'\n');
write('ROUTINE_INVENTORY.json',JSON.stringify({routines,newPrivate:2,newService:0,classificationPrivateDelta:4,classificationServiceDelta:2,ACL:'NATIVE_NOT_RUN',relationsChanged:0},null,2)+'\n');
console.log(JSON.stringify({root,routines:routines.length,newPrivate:2,registryForwards:registryChanges.length,rootEdits:0,native:'NOT_RUN'}));
