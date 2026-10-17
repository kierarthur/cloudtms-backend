// Finite, reviewable managed-source payload. No I2 writes or DB operations.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {here,repo,root,checkpoint,blob,sha,apply,reverse,splice} from './build.mjs';

const read=p=>fs.readFileSync(path.join(here,p),'utf8');
const write=(p,s)=>{fs.mkdirSync(path.dirname(path.join(here,p)),{recursive:true});fs.writeFileSync(path.join(here,p),s);};
const namesPath=path.join(here,'MOUNT_FILENAMES.json');
let names;
if(fs.existsSync(namesPath))names=JSON.parse(fs.readFileSync(namesPath));
else{
  names={};
  for(const [key,kind,name] of [
    ['schema','migration','weekly_source_kept_10_decision_schema'],
    ['guards','migration','weekly_source_kept_20_audit_guards'],
    ['repeatable','repeatable','weekly_source_kept_pay_rekey_v1'],
  ]){
    const result=execFileSync(process.execPath,['scripts/cloudtms-db-release.mjs','new','--kind='+kind,'--name='+name],{cwd:repo,encoding:'utf8',windowsHide:true});
    const file=result.match(/^Created (supabase\/[^\r\n]+)$/m)?.[1];
    if(!file||!/^supabase\/(?:migrations|repeatable)\/17102026_\d{4}_/.test(file))throw Error('FRESH_17_OCTOBER_SCAFFOLD_REQUIRED');
    const source=path.join(repo,file),target=path.join(here,'payload',file);
    assert(source.startsWith(repo+path.sep)&&target.startsWith(here+path.sep));
    fs.mkdirSync(path.dirname(target),{recursive:true});fs.renameSync(source,target);names[key]=file;
  }
  assert(names.schema<names.guards);write('MOUNT_FILENAMES.json',JSON.stringify(names,null,2)+'\n');
}
const folder=names.repeatable.replace(/_v1\.sql$/,'');
const support=folder.split('/').at(-1);
const wrap=sql=>'\\set ON_ERROR_STOP on\nbegin;\n'+sql+'\ncommit;\n';
write('payload/'+names.schema,wrap(read('forward/00_original_decision_schema.sqlinc')));
write('payload/'+names.guards,wrap(read('forward/13_one_time_audit_guards.sqlinc')));
const logical=JSON.parse(read('CLASSIFICATION_DELTA.json'));
const forward=JSON.parse(read('BUILD_RESULT.json')).outputs.map(r=>r.path).filter(p=>!['forward/00_original_decision_schema.sqlinc','forward/13_one_time_audit_guards.sqlinc'].includes(p));
for(const p of forward)write('payload/'+folder+'/'+path.basename(p),read(p));

// Preserve the exact complete current classifier/ACL owner and alter only the
// new literal identities and its explicit one-relation count. No wildcard grant.
const classifier='supabase/repeatable/14102026_0709_healthroster_current_literal_classification/01_complete_current_literal_classification.sqlinc';
const before=blob(checkpoint,classifier).toString(),classifierHunks=[];
const add=(a,b,purpose)=>classifierHunks.push({before:a,after:b,purpose});
add("    ('weekly_source_self_bill_report_line_bindings', 'STATEFUL_SERVER_OWNED'),","    ('weekly_source_kept_as_paid_decisions', 'IMMUTABLE_APPEND_ONLY'),\n    ('weekly_source_self_bill_report_line_bindings', 'STATEFUL_SERVER_OWNED'),",'The one original immutable decision relation joins the current exact table contract.');
const newPrivate=logical.uniqueFunctions.filter(f=>f.newDeclaration&&!f.serviceExecute).map(f=>f.signature).sort();
add("    ('private.weekly_source_invoice_adjustment_authority_v1(uuid,uuid[])'),",newPrivate.map(sig=>"    ('"+sig+"'),").join('\n')+"\n    ('private.weekly_source_invoice_adjustment_authority_v1(uuid,uuid[])'),",'Only six new private implementation helpers; service/browser execution remains denied.');
add("    ('public.weekly_source_upload_context_v1(jsonb)'),","    ('public.weekly_source_keep_as_paid_decide_atomic_v1(jsonb)'),\n    ('public.weekly_source_upload_context_v1(jsonb)'),",'One deliberately granted service RPC; no direct browser or relation access.');
for(const count of ['v_contract_count<>105','v_distinct_contract_count<>105','exactly 105 unique relations.'])add(count,count.replace('105','106'),'Exact current 105 relation contract plus one planned immutable relation.');
const after=apply(before,classifierHunks);assert.equal(reverse(after,classifierHunks),before);
const classifierTarget=folder+'/98_complete_current_classification.sqlinc';write('payload/'+classifierTarget,after);
write('CLASSIFIER_EDITS.json',JSON.stringify({checkpoint,target:classifier,successorTarget:classifierTarget,inputSha256:sha(before),outputSha256:sha(after),hunks:classifierHunks,completeReverseByteEquality:true},null,2)+'\n');

const runtime=JSON.parse(read('PARENT_EDITS.json'));
write('payload/'+runtime.target,read('PROPOSED_ROUTES.js'));
write('payload/'+runtime.module.target,read(runtime.module.path));
const includes=forward.filter(p=>!p.endsWith('/99_exact_function_acl.sqlinc')).sort().map(p=>support+'/'+path.basename(p));
// The complete current ACL gate runs after every replacement. Exact new ACLs
// reassert after it, so this changed recursive root is independently closed.
includes.push(support+'/98_complete_current_classification.sqlinc',support+'/99_exact_function_acl.sqlinc');
write('payload/'+names.repeatable,wrap(includes.map(p=>'\\ir '+p).join('\n')));

const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const registryPath='docs/bpay-next/final-definer-closures.json',registryBefore=blob(checkpoint,registryPath).toString(),registry=JSON.parse(registryBefore),registryEdits=[];
const definitionOrder=[];
for(const include of includes){const physical='supabase/repeatable/'+include,source=read('payload/'+physical);for(const signature of routineDefinitions(source,physical))definitionOrder.push({signature,physical});}
const final=new Map(definitionOrder.map(x=>[x.signature,x]));
for(const {signature,physical} of final.values()){
  const at=registry.chains.findIndex(c=>c.signature===signature),prior=at<0?null:structuredClone(registry.chains[at]);
  const original=logical.existingCurrentOwners.find(c=>c.signature===signature);
  const next=prior?{...prior,finalRoot:names.repeatable,finalPhysical:physical,
    allowedPhysical:[...new Set([...prior.allowedPhysical,physical])],
    predecessors:[...new Set([...(prior.predecessors??[]),prior.finalRoot])],
    finalClosures:[...new Set([...(prior.finalClosures??[]),names.repeatable])]}:{signature,finalRoot:names.repeatable,finalPhysical:physical,
    allowedPhysical:[...new Set([...(original?[original.predecessorPhysical]:[]),physical])],
    predecessors:original?[original.predecessorRoot]:[],finalClosures:[names.repeatable],owner:'HealthRoster kept-as-paid/rekey exact source contribution'};
  next.healthKeptPaySourceReview='docs/healthroster-build/proposals/kept-pay-rekey-current-root-successor-20261016/INTEGRATION.md';
  if(at<0)registry.chains.push(next);else registry.chains[at]=next;
  registryEdits.push({signature,before:prior,after:next});
}
// Reverse JSON changes in place, then prove the original serialized bytes.
const reversed=structuredClone(registry);
for(const change of [...registryEdits].reverse()){const at=reversed.chains.findIndex(c=>c.signature===change.signature);if(change.before===null)reversed.chains.splice(at,1);else reversed.chains[at]=change.before;}
assert.equal(JSON.stringify(reversed,null,2)+'\n',registryBefore);
const registryAfter=JSON.stringify(registry,null,2)+'\n';write('payload/'+registryPath,registryAfter);
write('REGISTRY_EDITS.json',JSON.stringify({checkpoint,target:registryPath,inputSha256:sha(registryBefore),outputSha256:sha(registryAfter),changes:registryEdits,completeReverseByteEquality:true},null,2)+'\n');

// Grammar scanners inspect full complete source, never execute it.
const files=[];
function walk(dir){for(const entry of fs.readdirSync(dir,{withFileTypes:true})){const p=path.join(dir,entry.name);if(entry.isDirectory())walk(p);else{const content=fs.readFileSync(p),target=path.relative(path.join(here,'payload'),p).replaceAll('\\','/');if(target.endsWith('.sql')||target.endsWith('.sqlinc'))scanManagedSql(content.toString());files.push({target,sha256:sha(content),bytes:content.length});}}}
walk(path.join(here,'payload'));files.sort((a,b)=>a.target.localeCompare(b.target));
write('PAYLOAD_MANIFEST.json',JSON.stringify({checkpoint,files,managedRoots:names,completeCurrentOwnerReversals:5,completeParentReverse:true,classifierReverse:true,registryReverse:true,native:'NOT_RUN',catalogue:'NOT_RUN',API:'NOT_RUN',races:'NOT_RUN',contractSeal:'NOT_RUN',managedMount:'NOT_RUN',dbConnections:0,rootEdits:0},null,2)+'\n');
console.log(JSON.stringify({files:files.length,managedRoots:names,registryChanges:registryEdits.length,newPrivate:newPrivate.length,fullReversals:true,native:'NOT_RUN'}));
