import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),repo=path.resolve(here,'../../../..');
const root=path.resolve(repo,'../healthroster-install2-14102026'),money=path.resolve(repo,'../healthroster-v2-money-build-13102026');
const pins=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_PINS.json'))),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=p=>fs.readFileSync(path.join(root,p),'utf8');
const git=(commit,file)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+file],{cwd:repo,maxBuffer:16000000});
const original='84738fe4057d9d693ccd723eb1835106f1091f57',prefix='docs/healthroster-build/proposals/';
const r=prefix+'source-current-read-health-successor-20261017/',c=prefix+'source-complete-classification-current-successor-20261017/',f=prefix+'source-read-identity-portability-forward-20261017/';
const json=p=>JSON.parse(read(p)),selected=p=>pins.selectedTargets[p]?read(pins.selectedTargets[p].path):git(original,p).toString();
const checks=[],check=(name,fn)=>{fn();checks.push(name);};
const actualInputs=new Map();
function actual(file){
 if(actualInputs.has(file))return actualInputs.get(file);
 const bytes=git(original,file),saved=sha(fs.readFileSync(path.join(root,file)));
 // This programme can also be replayed after root mounts the exact composition.
 // Historical parent bytes stay Git-bound; only the explicitly selected final
 // payload is an alternative for a current saved metadata file.
 assert(saved===sha(bytes)||(pins.selectedTargets[file]&&saved===pins.selectedTargets[file].sha256),file);
 actualInputs.set(file,bytes.toString());return bytes.toString();
}
for(const pin of pins.files)check('frozen-saved-Git-root-evidence:'+pin.path,()=>{
 const a=fs.readFileSync(path.join(root,pin.path)),b=fs.readFileSync(path.join(money,pin.path));
 assert.equal(a.length,pin.bytes);assert.equal(sha(a),pin.sha256);assert.deepEqual(a,b);assert.deepEqual(a,git(pin.commit,pin.path));
});
const indexPath='docs/healthroster-build/proposals/a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const index=await import(pathToFileURL(path.join(root,indexPath)).href);
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
actual(indexPath);actual('scripts/cloudtms-managed-release-sql.mjs');
const roles=json(c+'APPROVED_ROLE_LITERALS.json').roles;
const acl=selected('supabase/repeatable/17102026_0741_weekly_source_complete_classification/99_exact_acl.sqlinc');
const aclStatements=scanManagedSql(acl).statements.map(s=>acl.slice(s.start,s.end).trim());
assert.equal(roles.length,81);assert.equal(new Set(roles.map(v=>v.signature)).size,81);
for(const row of roles)check('actual-current-header-and-explicit-ACL:'+row.signature,()=>{
 const current=selected(row.current.physical);assert.equal(sha(current),row.current.wholeSha256);
 const defs=index.functionEvents(current).events.filter(e=>e.kind==='CREATE'&&e.id===row.signature);
 assert.equal(defs.length,1);assert.equal(sha(defs[0].definition),row.current.definitionSha256);assert.equal(defs[0].header,row.current.header);
 for(const evidence of row.explicitSourceIntent){
  const source=actual(evidence.path);assert.equal(sha(source),evidence.sha256);
  const statements=scanManagedSql(source).statements.map(s=>source.slice(s.start,s.end).trim());
  assert(statements.includes(evidence.sql),row.signature+': explicit original ACL statement absent');
 }
 const revoke=`revoke all on function ${row.signature} from public,anon,authenticated,service_role;`;
 const grant=`grant execute on function ${row.signature} to service_role;`;
 assert(aclStatements.includes(`alter function ${row.signature} owner to postgres;`));
 assert(aclStatements.includes(revoke));assert.equal(aclStatements.includes(grant),row.serviceExecute);
 assert.equal(row.serviceExecute,row.explicitSourceIntent.some(e=>/^grant execute/i.test(e.sql)));
});
check('no-extra-grant-or-unclassified-function-in-exact81-ACL',()=>{
 const expected=new Set(roles.flatMap(v=>[`alter function ${v.signature} owner to postgres;`,
  `revoke all on function ${v.signature} from public,anon,authenticated,service_role;`,
  ...(v.serviceExecute?[`grant execute on function ${v.signature} to service_role;`]:[])]));
 const classifiers=index.functionEvents(selected('supabase/repeatable/17102026_0741_weekly_source_complete_classification/01_complete_current_classification.sqlinc')).events.filter(v=>v.kind==='CREATE');
 assert.equal(classifiers.length,6);
 for(const row of classifiers){expected.add(`alter function ${row.id} owner to postgres;`);
  expected.add(`revoke all on function ${row.id} from public,anon,authenticated,service_role;`);}
 expected.add("notify pgrst,'reload schema';");
 assert.equal(aclStatements.length,expected.size);for(const sql of aclStatements)assert(expected.has(sql));
 assert.equal(roles.filter(v=>v.serviceExecute).length,22);
 assert.equal(roles.find(v=>v.signature==='private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb)').serviceExecute,true);
 const immutable=roles.filter(v=>!v.current.securityDefiner);assert.equal(immutable.length,2);
 for(const row of immutable){assert.equal(row.current.volatility,'i');assert.equal(row.current.searchPath,'pg_catalog');}
});
check('case-insensitive-actual-root-inventory-and-final426-1095',()=>{
 const paths=execFileSync('git',['ls-tree','-r','--name-only',original,'--','supabase/migrations','supabase/repeatable'],{cwd:repo}).toString().split(/\r?\n/);
 // Actual sqlFiles includes tracked nested .sql, not only top-level roots.
 const count=kind=>paths.filter(p=>new RegExp('^supabase/'+kind+'/.+[.]sql$','i').test(p)).length;
 assert.equal(count('migrations'),425);assert.equal(count('repeatable'),1093);
 const combined=new Set(paths);for(const p of Object.keys(pins.selectedTargets))combined.add(p);
 assert.equal([...combined].filter(p=>/^supabase\/migrations\/.+[.]sql$/i.test(p)).length,426);
 assert.equal([...combined].filter(p=>/^supabase\/repeatable\/.+[.]sql$/i.test(p)).length,1095);
 assert(paths.some(p=>/^supabase\/migrations\/.*[.]SQL$/.test(p)),'original uppercase SQL migrations retained');
});
check('all425-lock-rows-and-metadata-preserved-exactly-one-addition',()=>{
 const before=JSON.parse(actual('supabase/release/migration-lock.json')),after=JSON.parse(selected('supabase/release/migration-lock.json'));
 assert.equal(before.migrations.length,425);assert.equal(after.migrations.length,426);
 const old=new Map(before.migrations.map(v=>[v.path,v]));
 for(const row of after.migrations.filter(v=>old.has(v.path)))assert.deepEqual(row,old.get(row.path));
 const extra=after.migrations.filter(v=>!old.has(v.path));assert.equal(extra.length,1);
 assert.equal(extra[0].path,'supabase/migrations/17102026_0749_source_current_read_private_admission.sql');
 assert.equal(extra[0].sha256,sha(selected(extra[0].path)));
 after.migrations=after.migrations.filter(v=>old.has(v.path));assert.deepEqual(after,before);
});
check('complete-registry-chain-composition-and-all-six-Daily-chains',()=>{
 const p='docs/bpay-next/final-definer-closures.json',base=JSON.parse(actual(p));let current=structuredClone(base);
 for(const packet of[r,c]){
  const next=JSON.parse(read(packet+'payload/'+p)),edits=json(packet+'REGISTRY_EDITS.json');
  for(const change of edits.changes){
   const at=current.chains.findIndex(v=>v.signature===change.signature);
   if(change.before===null){assert.equal(at,-1);current.chains.push(change.after);}
   else{assert(at>=0);assert.deepEqual(current.chains[at],change.before);current.chains[at]=change.after;}
  }
  assert.deepEqual(new Map(current.chains.map(v=>[v.signature,v])),new Map(next.chains.map(v=>[v.signature,v])));
  assert.deepEqual({...current,chains:[]},{...next,chains:[]});current=next;
 }
 assert.deepEqual(current,JSON.parse(selected(p)));
 const daily=base.chains.filter(v=>v.finalRoot==='supabase/repeatable/17102026_0248_nhsp_v6_daily_health_support_v1.sql');
 assert.equal(daily.length,6);for(const row of daily)assert.deepEqual(current.chains.find(v=>v.signature===row.signature),row);
});
check('guard-exact-two-catalogue-names-all-overloads-before-repeatables',()=>{
 const guard=selected('supabase/migrations/17102026_0749_source_current_read_private_admission.sql');
 const strings=[...guard.matchAll(/'((?:''|[^'])*)'/g)].map(v=>v[1]);
 assert.deepEqual(strings.filter(v=>v.startsWith('weekly_source_')),
  ['weekly_source_pay_decision_native_status_child_v1','weekly_source_pay_decision_read_access_v1']);
 assert(guard.includes("where n.nspname='private' and p.proname=routine_name"));
 assert(!/proargtypes|oidvectortypes|to_regprocedure|create\s+(?:or\s+replace\s+)?function/i.test(guard));
 const order=json(f+'NAMESPACE_ADMISSION.json').managedApplyOrder,source=actual(order.path);
 assert.equal(sha(source),order.sha256);assert(source.includes(order.literal));
 assert(order.literal.indexOf('for(const unit of migrationUnits)await applyUnit(unit);')<order.literal.indexOf('await preapply('));
});
check('public-read-arm-cannot-return-internal-or-accepted-receipt',()=>{
 const s=selected('supabase/repeatable/17102026_0722_weekly_source_current_read_health/06_complete_public_source_accept_read.sqlinc');
 const arm=s.slice(s.indexOf(" if kind='READ_RESULT' then"),s.indexOf(' fingerprint:='));
 assert(arm.includes("raise exception 'SOURCE_PAY_DECISION_READ_PROJECTION_DEPENDENCY_REQUIRED' using errcode='55000'"));
 assert(!/\breturn\b/i.test(index.stripSqlComments(arm)));
 assert(arm.includes('private.weekly_source_pay_decision_read_access_v1(j.decision_id,j.request_id,actor)'));
});
fs.writeFileSync(path.join(here,'INDEPENDENT_SOURCE_RESULTS.json'),JSON.stringify({passed:checks.length,failed:0,checks,
 actualInputPins:[...actualInputs].map(([file,source])=>({path:file,commit:original,sha256:sha(source)})),
 finalInventory:{migrations:426,repeatables:1095},roles:81,serviceRoles:22,immutableInvokers:2,
 fullGlobalLoader:'ROOT_OWNED_NOT_RERUN_TO_AVOID_BANK_BODIES',native:'NOT_RUN',dbConnections:0},null,2)+'\n');
console.log(JSON.stringify({passed:checks.length,roles:81,finalInventory:[426,1095],native:'NOT_RUN'}));
