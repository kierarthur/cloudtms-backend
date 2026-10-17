// Independent source review only. No SQL execution or foreign worktree writes.
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..');
const physical=path.resolve(own,'../healthroster-checklist-native-14102026'),root=path.resolve(own,'../healthroster-install2-14102026');
const pin='82b14dc385319ebe3d741bb2586c9b6ed172914c',base='acee28f5b4d862bdf7e4425a65566dd2d060be35',purePin='728a2c32e624fed2db577b7deba70600d27885b1';
const proposal='docs/healthroster-build/proposals/daily-health-support-managed-root-20261017',pure='docs/healthroster-build/proposals/daily-health-pure-native-proof-20261017';
for(const p of [own,physical,root])assert(fs.realpathSync.native(p).replaceAll('\\','/').startsWith('D:/Development/GitHub/'));
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=(cwd,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd,windowsHide:true,maxBuffer:128*1024*1024});
const blob=(commit,file)=>git(root,['show',commit+':'+file]);
const get=(file)=>fs.readFileSync(path.join(physical,proposal,file));
const json=file=>JSON.parse(get(file));
const manifest=json('PAYLOAD_MANIFEST.json'),guards=json('SOURCE_GUARDS.json'),contract=json('CATALOGUE_EXPECTATIONS.json');
const checks=[],pins=[];
function test(name,fn){fn();checks.push(name);}
function extract(text,name){const prefix='create or replace function '+name+'(',start=text.indexOf(prefix);assert(start>=0&&text.indexOf(prefix,start+1)===-1,name);const body=text.indexOf('as $function$',start),end=text.indexOf('$function$;',body+13);assert(body>start&&end>body);return text.slice(start,end+'$function$;'.length);}
test('25 frozen Physical evidence files and all seven selected targets equal the actual mounted bytes',()=>{
 const files=git(physical,['ls-tree','-r','--name-only',pin,'--',proposal]).toString().trim().split('\n');assert.equal(files.length,25);
 for(const file of files){const b=blob(pin,file);assert.deepEqual(fs.readFileSync(path.join(physical,file)),b);assert.deepEqual(fs.readFileSync(path.join(root,file)),b);pins.push({commit:pin,file,sha256:sha(b),bytes:b.length});}
 assert.equal(manifest.checkpoint,base);assert.equal(manifest.files.length,7);
 for(const row of manifest.files){const b=get(row.source);assert.equal(sha(b),row.sha256);assert.equal(b.length,row.bytes);assert.deepEqual(fs.readFileSync(path.join(root,row.target)),b);}
});
test('both whole metadata parents reverse exactly to acee28 and every prior registry/mandatory array is preserved',()=>{
 const edits=json('METADATA_EDITS.json');assert.equal(edits.edits.length,2);
 for(const row of edits.edits){const original=blob(base,row.target);let text=get('payload/'+row.target).toString();assert.equal(sha(text),row.outputSha256);for(const h of [...row.hunks].reverse()){assert.equal(text.split(h.after).length,2);text=text.replace(h.after,()=>h.before);}assert.deepEqual(Buffer.from(text),original);assert.equal(sha(original),row.inputSha256);}
 const registry=JSON.parse(fs.readFileSync(path.join(root,'docs/bpay-next/final-definer-closures.json'))),prior=JSON.parse(blob(base,'docs/bpay-next/final-definer-closures.json'));
 assert.deepEqual(registry.chains.slice(0,-6),prior.chains);assert.deepEqual({...registry,chains:prior.chains},prior);
 assert.deepEqual(registry.chains.slice(-6).map(r=>r.signature),manifest.signatures);
 for(const row of registry.chains.slice(-6)){assert.equal(row.finalRoot,manifest.managedRoot);assert.equal(row.finalPhysical,manifest.files[2].target);assert.deepEqual(row.allowedPhysical,[row.finalPhysical]);assert.deepEqual(row.predecessors,[]);assert.deepEqual(row.finalClosures,[manifest.managedRoot]);}
 const release=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json'))),old=JSON.parse(blob(base,'supabase/release/current-release.json'));
 for(const key of ['verificationFiles','newVerificationFiles']){assert.deepEqual(release[key].slice(0,-1),old[key]);assert.equal(release[key].at(-1),manifest.independentVerification);assert.equal(release[key].filter(x=>x===manifest.independentVerification).length,1);}
 assert.equal(old.verificationFiles.length,178);assert.equal(old.newVerificationFiles.length,179);
 assert.deepEqual({...release,verificationFiles:old.verificationFiles,newVerificationFiles:old.newVerificationFiles},old);
});
const helpers=get('payload/'+manifest.files[2].target).toString(),originalFile=guards.helperSource.file;
test('six whole declarations are byte-identical e938 and original twelve evidence files were copied exactly',()=>{
 const old=blob(manifest.e938,originalFile).toString();assert.equal(sha(old),guards.helperSource.rawSha256);
 const names=[...helpers.matchAll(/^create or replace function ([^(]+)\(/gm)].map(m=>m[1]);assert.equal(names.length,6);assert.deepEqual(names,manifest.signatures.map(x=>x.split('(')[0]));
 for(const name of names)assert.equal(extract(helpers,name),extract(old,name));
 assert(!/create or replace function (?:public\.|private\.bpay_next_)/i.test(helpers));
 const prefix=path.posix.dirname(originalFile),files=git(physical,['ls-tree','-r','--name-only',manifest.e938,'--',prefix]).toString().trim().split('\n');assert.equal(files.length,12);
 for(const file of files)assert.deepEqual(fs.readFileSync(path.join(root,file)),blob(manifest.e938,file));
});
test('one-time collision guard is separate; rerunnable root installs exact six owner-only ACLs without grants',()=>{
 const admission=get('payload/'+manifest.oneTimeAdmission).toString(),repeat=get('payload/'+manifest.managedRoot).toString(),acl=get('payload/'+manifest.files[3].target).toString();
 assert.deepEqual([...admission.matchAll(/'(private\.[^']+\([^']*\))'/g)].map(m=>m[1]),manifest.signatures);
 assert(admission.includes('pg_catalog.to_regprocedure(signature) is not null'));assert(admission.includes("errcode='55000'"));assert(!/\b(?:create|alter|insert|update|delete|grant|revoke)\b/i.test(admission.replace(/^--.*$/gm,'')));
 assert(!repeat.includes('admission'));assert.equal((repeat.match(/^\\ir /gm)||[]).length,2);
 for(const s of manifest.signatures){assert.equal(acl.split('alter function '+s+' owner to postgres;').length,2);assert.equal(acl.split('revoke all on function '+s+' from public,anon,authenticated,service_role;').length,2);}
 assert(!/\bgrant\b/i.test(acl));assert.equal((acl.match(/^alter function /gm)||[]).length,6);assert.equal((acl.match(/^revoke all /gm)||[]).length,6);
 const reverse=get('reverse/complete_additive_reverse.sqlinc').toString();assert.deepEqual([...reverse.matchAll(/^drop function ([^;]+);/gm)].map(m=>m[1]),[...manifest.signatures].reverse());
});
test('independent mandatory catalogue verifier checks exact types/volatility/search paths/owner and direct plus effective ACL',()=>{
 const text=get('payload/'+manifest.independentVerification).toString();assert.equal(contract.routines.length,6);
 assert.deepEqual(contract.routines.map(r=>r.signature),manifest.signatures);
 for(const r of contract.routines)assert(text.includes(`('${r.signature}','${r.result}','${r.volatility}','${r.searchPath}')`));
 for(const token of ["actual.nspname<>'private'","actual.lanname<>'plpgsql'","actual.prokind<>'f'",'not actual.prosecdef','actual.proretset','actual.prorettype<>pg_catalog.to_regtype','actual.provolatile::text<>','actual.proowner<>(current_user::regrole)::oid','pg_catalog.unnest(actual.proconfig)','pg_catalog.aclexplode','permission.grantee<>actual.proowner',"array['anon','authenticated','service_role']",'pg_catalog.has_function_privilege'])assert(text.includes(token),token);
 assert(!/perform private\.daily_rate_correction_health_/.test(text));assert(text.trimEnd().endsWith('rollback;'));
});
test('all ten native owner pages, ten final-owner chains, three policy files and the actual F23 source remain exact',()=>{
 assert.equal(guards.files.length,10);assert.equal(guards.finalOwners.length,10);assert.equal(guards.policyFiles.length,3);
 for(const row of guards.files){const b=fs.readFileSync(path.join(root,row.file));assert.equal(sha(b),row.rawSha256);assert.deepEqual(b,blob(base,row.file));}
 const registry=JSON.parse(fs.readFileSync(path.join(root,'docs/bpay-next/final-definer-closures.json')));
 for(const row of guards.finalOwners){const found=registry.chains.find(x=>x.signature===row.signature);assert(found);assert.equal(found.finalRoot,row.finalRoot);assert.equal(found.finalPhysical,row.finalPhysical);}
 for(const row of guards.policyFiles)assert.equal(sha(fs.readFileSync(path.join(guards.policyRoot,row.file),'utf8').replaceAll('\r\n','\n')),row.canonicalSha256);
 assert.equal(sha(fs.readFileSync(path.join(root,guards.gateSource.file))),guards.gateSource.rawSha256);
});
test('managed admission appends only its exact migration lock entry and leaves all 423 old entries unchanged',()=>{
 const file='supabase/release/migration-lock.json',old=JSON.parse(blob(purePin,file)),now=JSON.parse(fs.readFileSync(path.join(root,file)));
 assert.equal(old.migrations.length,423);assert.equal(now.migrations.length,424);
 const added=now.migrations.filter(x=>!old.migrations.some(y=>x.path===y.path));assert.equal(added.length,1);assert.equal(added[0].path,manifest.oneTimeAdmission);assert.equal(added[0].sha256,manifest.files[0].sha256);
 for(const row of old.migrations)assert.deepEqual(now.migrations.find(x=>x.path===row.path),row);
 assert.deepEqual({...now,migrations:old.migrations},old);
});
const gate=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));
test('actual current F23 write/type/special-construct gates accept five new SQL inputs with zero allowances',()=>{
 for(const row of manifest.files.filter(r=>/\.sql(?:inc)?$/i.test(r.target))){const text=get(row.source).toString();assert.equal(gate.specialConstructViolations(text,row.target).length,0);gate.assertSpecialConstructs(text,row.target);gate.assertSqlWriteBoundaries(text,row.target);gate.assertNextExactNumericTypes(text,row.target);}
});
// Execute the exact frozen source loader with a capability-limited filesystem.
// The only would-be receipt write is captured in memory; original test.mjs is
// intentionally not run because its pre-mount parent assertions no longer apply.
const captures=[],loaderFile=path.join(physical,proposal,'source-check.mjs');
const readonlyFs=new Proxy({}, {get(_t,k){if(['readFileSync','realpathSync'].includes(k))return fs[k];if(k==='writeFileSync')return(file,data)=>{assert.equal(path.resolve(file),path.join(physical,proposal,'SOURCE_LOADER_RESULTS.json'));captures.push(Buffer.from(data));};throw Error('UNAPPROVED_FS_CAPABILITY:'+String(k));}});
const readonlyChild={execFileSync(command,args,options){assert.equal(command,'git');assert(args.includes('show')||args.includes('ls-tree')||args.includes('cat-file'));return execFileSync(command,args,options);}};
const context=vm.createContext({Buffer,console:{log(){}}});
const module=new vm.SourceTextModule(fs.readFileSync(loaderFile,'utf8'),{context,identifier:pathToFileURL(loaderFile).href,initializeImportMeta(meta){meta.url=pathToFileURL(loaderFile).href;}});
await module.link(async spec=>{assert(['node:fs','node:path','node:vm','node:crypto','node:assert/strict','node:child_process','node:url'].includes(spec));let ns=await import(spec);if(spec==='node:fs')ns={...ns,default:readonlyFs};if(spec==='node:child_process')ns={...ns,execFileSync:readonlyChild.execFileSync};return new vm.SyntheticModule(Object.keys(ns),function(){for(const [k,v]of Object.entries(ns))this.setExport(k,v);},{context});});
await module.evaluate();
test('exact frozen loader executes actual committed inventory/final-owner tools read-only with matching captured receipt',()=>{
 assert.equal(captures.length,1);assert.deepEqual(captures[0],get('SOURCE_LOADER_RESULTS.json'));const r=JSON.parse(captures[0]);assert.deepEqual(r.candidate,{migrations:424,repeatables:1091});assert.equal(r.verified.signatures,3001);assert.equal(r.verified.registered,1607);assert.equal(r.verified.roots,1091);
});
const pureManifest=JSON.parse(blob(purePin,pure+'/SOURCE_MANIFEST.json'));
test('pure proof frozen source, five exact declarations and unchanged native assertion program retain their pins',()=>{
 const files=git(root,['ls-tree','-r','--name-only',purePin,'--',pure]).toString().trim().split('\n');
 for(const file of files){const b=blob(purePin,file);assert.deepEqual(fs.readFileSync(path.join(root,file)),b);pins.push({commit:purePin,file,sha256:sha(b),bytes:b.length});}
 for(const row of pureManifest.rootInputs){const b=fs.readFileSync(path.join(root,row.file));assert.equal(sha(b),row.fileSha256);assert.deepEqual(b,blob(base,row.file));const selected=Buffer.from(extract(b.toString(),row.name));assert.equal(selected.length,row.definitionBytes);assert.equal(sha(selected),row.definitionSha256);}
 const helper=fs.readFileSync(path.join(root,pure,pureManifest.helper.file));assert.equal(sha(helper),pureManifest.helper.sha256);assert.equal(helper.length,pureManifest.helper.bytes);assert.equal(helper.toString(),extract(helpers,pureManifest.helper.name));
 const assertions=fs.readFileSync(path.join(root,pure,pureManifest.program.file));assert.equal(sha(assertions),pureManifest.program.sha256);assert.equal(assertions.length,pureManifest.program.bytes);assert.deepEqual(assertions,blob(manifest.e938,'docs/healthroster-build/proposals/daily-health-composition-20261017/17102026_0126_daily_health_reason_program.sql'));
});
const {buildPureProgram}=await import(pathToFileURL(path.join(root,pure,'build-program.mjs'))),program=buildPureProgram({root});
test('pure builder returns only the exact five-definition 8029-byte closure and refuses foreign roots',()=>{
 assert.equal(program.sqlBytes,8029);assert.equal(program.definitionCount,5);assert.equal(program.positiveUnicodeVectors,6);assert.equal(program.malformedStoredReasonRefusals,8);assert.equal(program.businessFixtureRows,0);
 assert.throws(()=>buildPureProgram({root:own}),/REGISTERED_ROOT_REQUIRED/);assert.throws(()=>buildPureProgram({root:'relative'}),/ABSOLUTE_ROOT_REQUIRED/);
 assert(!/\b(?:insert|update|delete|merge|execute|nextval|setval)\b/i.test(program.sql));assert.equal(program.native,'REQUIRED_NOT_RUN');assert.equal(program.restoration,'REQUIRED_NOT_RUN');
});
fs.writeFileSync(path.join(here,'REVIEW_RESULTS.json'),JSON.stringify({scope:'INDEPENDENT_SOURCE_ONLY',physicalCommit:pin,metadataParent:base,pureCommit:purePin,passed:checks.length,failed:0,checks,actualCommittedLoader:JSON.parse(captures[0]),pure:{...program,sql:undefined},foreignWrites:0,databaseConnections:0,native:'NOT_RUN',release:'NOT_RUN'},null,2)+'\n');
fs.writeFileSync(path.join(here,'SOURCE_PINS.json'),JSON.stringify({physicalCommit:pin,metadataParent:base,pureCommit:purePin,files:pins},null,2)+'\n');
console.log(JSON.stringify({passed:checks.length,failed:0,payloads:7,exactHelpers:6,mandatoryPrefixPreserved:true,pureBytes:program.sqlBytes,native:'NOT_RUN'}));
