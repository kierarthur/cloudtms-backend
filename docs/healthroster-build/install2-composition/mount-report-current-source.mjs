// One-time source composition. No native function, SQL connection or seal builder is invoked.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../../../scripts/cloudtms-managed-release-sql.mjs';
import {declarationIdentity,stripSqlComments} from '../proposals/strict-qualification-install1-forward/native-source-identity.mjs';
const input='c432594d9203421f009d3c7f21780f8c7df4f24e', commit='52adf06e10fcefdbf43f53dfcc911deedcf2b1f0';
const sha=b=>createHash('sha256').update(b).digest('hex');
const object=(c,f)=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show',c+':'+f],{maxBuffer:64*1024*1024,stdio:['ignore','pipe','pipe']});
assert.equal(execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),input);
const base='docs/healthroster-build/proposals/';
const packets=[
 ['install2-shared-runtime-forward-20261014','df1431cae0169faf922e209e677d5a09d753742426597c9e24beaa352c1a7a50',35],
 ['install2-report-canonical-integration-20261014','6b16dda5df21ea961086c2336ada3e6337f482d244f0137acd1a6142d82eec75',28],
 ['install2-report-canonical-seal-successor-20261014','db40c675781b557f8954bdb102735203587320f6769f870908e58527f3592247',10],
 ['install2-report-native-btrim-successor-20261014','5286c539a3d0bf1df3ac233a3daf1d2945381b7a7dad83a71467ac5eed9136d9',9]
];
const prepared=[];
const prepare=(file,bytes,sourceFile,expected=sha(bytes),derivative=null)=>{
 assert.ok(!file.split('/').includes('..'));assert.equal(sha(bytes),expected);
 assert.ok(!fs.existsSync(file),'NEW_TARGET_EXISTS '+file);
 prepared.push({file,bytes,sha256:expected,sourceCommit:commit,sourceFile,derivative});
};
for(const [name,expected,count] of packets){
 const dir=base+name+'/',raw=object(commit,dir+'REVIEW_MANIFEST.json');
 assert.equal(sha(raw),expected);const manifest=JSON.parse(raw);assert.equal(manifest.artifacts.length+1,count);
 for(const row of manifest.artifacts){const bytes=object(commit,dir+row.path);assert.equal(bytes.length,row.bytes);prepare(dir+row.path,bytes,dir+row.path,row.sha256);}
 prepare(dir+'REVIEW_MANIFEST.json',raw,dir+'REVIEW_MANIFEST.json',expected);
}
const canonical=base+packets[1][0]+'/',reverse=JSON.parse(object(commit,canonical+'CURRENT_NATIVE_REVERSE.json'));
let hunkCount=0;
for(const owner of reverse.owners){
 const before=fs.readFileSync(owner.input),saved=object(commit,canonical+'forward/'+owner.output);
 assert.equal(sha(before),owner.input_sha256);assert.equal(sha(saved),owner.output_sha256);
 let composed=before.toString('utf8');for(const h of owner.hunks){assert.equal(composed.split(h.old).length,2);composed=composed.replace(h.old,()=>h.new);hunkCount++;}
 assert.ok(Buffer.from(composed).equals(saved));
 for(const h of [...owner.hunks].reverse()){assert.equal(composed.split(h.new).length,2);composed=composed.replace(h.new,()=>h.old);}
 assert.ok(Buffer.from(composed).equals(before));
}
assert.equal(reverse.owners.length,5);assert.equal(hunkCount,32);
const order=JSON.parse(object(commit,canonical+'INTEGRATION_ORDER.json'));
const oldStem='14102026_0440_healthroster_report_native_integration';
const parts=Object.fromEntries(new Intl.DateTimeFormat('en-GB',{timeZone:'Europe/London',day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date()).filter(x=>x.type!=='literal').map(x=>[x.type,x.value]));
const stem=parts.day+parts.month+parts.year+'_'+parts.hour+parts.minute+'_healthroster_report_current_native_composition';
assert.ok(stem>'14102026_0519_healthroster_a0_current_native_composition');
const rootFile='supabase/repeatable/'+stem+'_v1.sql',definitions=new Map();
for(const rel of order.literal_relative_include_order){
 assert.ok(rel.startsWith(oldStem+'/'));assert.ok(!rel.includes('..'));
 const origin=canonical+'forward/supabase/repeatable/'+rel,bytes=object(commit,origin);
 prepare('supabase/repeatable/'+stem+'/'+path.posix.basename(rel),bytes,origin);
 const source=bytes.toString('utf8');
 for(const unit of scanManagedSql(source).statements){const text=stripSqlComments(source.slice(unit.start,unit.end));const head=text.match(/^\s*create\s+(?:or\s+replace\s+)?function\s+((?:private|public)\.\w+)\s*\(([\s\S]*?)\)\s*returns\b/i);if(head){const sig=declarationIdentity(head[1],head[2]).semantic_signature;assert.ok(!definitions.has(sig));definitions.set(sig,origin);}}
}
assert.equal(definitions.size,12);
const service=new Set(['public.weekly_source_group_rule_feature_set_atomic_v1(jsonb)','public.weekly_source_invoice_admit_atomic_v1(jsonb)','private._invoice_presentation_snapshot_batch(jsonb,timestamp with time zone)']);
const snapshotInput='supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/11_complete_native_snapshot_assembler.sqlinc';
assert.match(fs.readFileSync(snapshotInput,'utf8'),/grant execute on function private\._invoice_presentation_snapshot_batch\(jsonb,timestamptz\)\s+to service_role;/);
for(const s of service)assert.ok(definitions.has(s),'SERVICE_CLASSIFICATION_SIGNATURE_MISSING '+s);
const acl=[...definitions.keys()].map(sig=>{
 assert.match(sig,/^(?:private|public)\.[a-z0-9_]+\([a-z0-9_ ,\[\]]*\)$/);
 assert.ok(!sig.startsWith('public.')||service.has(sig));
 return 'alter function '+sig+' owner to postgres;\nrevoke all on function '+sig+' from public,anon,authenticated,service_role;\n'+(service.has(sig)?'grant execute on function '+sig+' to service_role;\n':'');
}).join('');
const originalRoot=object(commit,canonical+'forward/'+order.canonical_repeatable).toString('utf8');
assert.equal([...originalRoot.matchAll(/^\\ir /gm)].length,10);
let root=originalRoot.replaceAll(oldStem,stem);
const anchor="notify pgrst, 'reload schema';";assert.equal(root.split(anchor).length,2);
root=root.replace(anchor,'-- Reassert every actual definition in this atomic stage; preserve existing service snapshot authority.\n'+acl+anchor);
prepare(rootFile,Buffer.from(root),canonical+'forward/'+order.canonical_repeatable,sha(Buffer.from(root)),{includeStemOnly:{before:oldStem,after:stem},stageAclAdded:12,existingServiceGrants:3});
const verifier=object(commit,canonical+'forward/'+order.data_free_verifier.path);
prepare('supabase/verification/'+stem+'_declarations.sql',verifier,canonical+'forward/'+order.data_free_verifier.path);
const dependencies=JSON.parse(object(commit,canonical+'SCRIPT_AND_RECEIPT_DEPENDENCY_PINS.json'));
for(const row of dependencies.preserved_root_scripts)assert.equal(sha(fs.readFileSync(row.path)),row.sha256);
assert.equal(sha(fs.readFileSync(dependencies.preserved_renderer.path)),dependencies.preserved_renderer.sha256);
for(const row of dependencies.corrected_source_files){
 const origin=row.destination.endsWith('native-lock-observation.mjs')?base+packets[3][0]+'/forward/'+row.destination:canonical+'forward/'+row.destination;
 const bytes=object(commit,origin),expected=row.destination.endsWith('native-lock-observation.mjs')?'34a0093ee48aa74a949ed7600ee187913f30e8d7a4d1591167e3863db20f2c77':row.sha256;
 prepare(row.destination,bytes,origin,expected);
}
const builderOrigin=base+packets[2][0]+'/build-native-seal.mjs',builder=object(commit,builderOrigin);
assert.equal(sha(builder),'cbb5c29aea680fbd83fab583c871350b7913ed0df8bd9bade1487b99931e53de');
const oldImport="from '../install2-report-canonical-integration-20261014/forward/scripts/weekly-source-report-fin-native-mapper.mjs';",newImport="from './weekly-source-report-fin-native-mapper.mjs';";
let rebound=builder.toString('utf8');assert.equal(rebound.split(oldImport).length,2);rebound=rebound.replace(oldImport,newImport);
assert.ok(Buffer.from(rebound.replace(newImport,oldImport)).equals(builder));
prepare('scripts/weekly-source-report-fin-native-seal-build.mjs',Buffer.from(rebound),builderOrigin,sha(Buffer.from(rebound)),{hunks:[{old:oldImport,new:newImport}],fullReverse:true});
for(const row of prepared){fs.mkdirSync(path.dirname(row.file),{recursive:true});fs.writeFileSync(row.file,row.bytes);}
const receipt={sourceOnly:true,status:'MOUNTED_SOURCE_FINAL_REGISTRY_CONTRACT_NATIVE_AND_ACCEPTANCE_HELD',rootInputCommit:input,publication:commit,packets:packets.map(([name,manifestSha256,count])=>({name,manifestSha256,count})),rootFile,generatedAtUtc:new Date().toISOString(),completeOwnerHunks:hunkCount,completeOwnerInputsVerifiedAgainstSavedRoot:true,definitions:[...definitions.keys()].map(signature=>({signature,serviceExecute:service.has(signature)})),stageDefinitions:12,newOwnerPrivateHelpers:7,existingServiceGrants:3,allStageAclReasserted:true,standaloneRollbackVerifierExcludedFromInstall:true,nativeSeal:null,brokerAndManagedOwnersUnchanged:true,files:prepared.map(({bytes,...row})=>row),noDatabaseNativeBuilderOrRuntimeInvocation:true};
fs.writeFileSync('docs/healthroster-build/install2-composition/REPORT_CURRENT_SOURCE_COMPOSITION_14102026.json',JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({rootFile,sourceFiles:prepared.length,wholeOwnerHunks:hunkCount,definitions:12,existingServiceGrants:3,nativeSeal:null}));
