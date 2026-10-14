// Source integration of separately reviewed immutable packets; no SQL execution.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../scripts/cloudtms-managed-release-sql.mjs';
const folder=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(folder,'../../..');
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:300_000_000});
const object=(commit,file)=>git(['show',commit+':'+file]);
const publication='d6ffd1fd971664f7fb2a7ecc65bad86fff328262',money='d4b62cb15159d0afb02cfebf8c1e034c68c26550';
const packet='docs/healthroster-build/proposals/original-prerequisites-install1-forward/';
assert.equal(git(['rev-parse','--abbrev-ref','HEAD']).toString().trim(),'codex/healthroster-install2-14102026');
assert.equal(git(['rev-parse','HEAD']).toString().trim(),'a63455e878391cb3ee9732165e58c0db517f48a7');
const manifestBytes=object(publication,packet+'REVIEW_MANIFEST.json'),manifest=JSON.parse(manifestBytes);
assert.equal(hash(manifestBytes),'5bf0c84bfac484bd7c46014c768b208dcc6baa4834e074bebabde989e5fff084');
assert.equal(manifest.files.length,73);assert.equal(manifest.exact_current_root_commit,'a63455e878391cb3ee9732165e58c0db517f48a7');
const prepared=[];
const prepare=(file,bytes,expectedHash,sourceCommit,sourceFile)=>{
 assert.ok(/^(?:supabase|docs\/healthroster-build)\/[A-Za-z0-9_./-]+$/.test(file)&&!file.split('/').includes('..'),'SOURCE_PATH_REFUSED');
 const target=path.join(root,file);assert.ok(!fs.existsSync(target),'NEW_SOURCE_TARGET_EXISTS '+file);assert.equal(hash(bytes),expectedHash,file);
 if(/\.(?:sql|sqlinc)$/.test(file))assert.ok(!bytes.includes(13),'SQL_LF_REQUIRED '+file);
 prepared.push({file,bytes,sha256:expectedHash,sourceCommit,sourceFile});
};
for(const row of manifest.files){const bytes=object(publication,row.file);assert.equal(bytes.length,row.bytes);prepare(row.file,bytes,row.sha256,publication,row.file);}
prepare(packet+'REVIEW_MANIFEST.json',manifestBytes,hash(manifestBytes),publication,packet+'REVIEW_MANIFEST.json');
const checks=object(publication,packet+'SOURCE_CHECK_RESULT.json');
assert.equal(hash(checks),'df3bb84f2a105e582aa76e93c05c0b59b4f40b68a3461c6fcc95b4af34c3010b');
if(!prepared.some(row=>row.file===packet+'SOURCE_CHECK_RESULT.json'))prepare(packet+'SOURCE_CHECK_RESULT.json',checks,hash(checks),publication,packet+'SOURCE_CHECK_RESULT.json');
const sourceMain='supabase/support/healthroster_original_prerequisites_install1_forward/ORIGINAL_PREREQUISITES_SOURCE_FORWARD.sqlinc';
// Keep the frozen support packet intact; canonical executable includes stay
// within the existing release compiler's supabase/repeatable confinement.
const originalCanonical='supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/';
for(const row of [...prepared].filter(row=>row.file.startsWith('supabase/support/healthroster_original_prerequisites_install1_forward/')&&!row.file.endsWith('/CAP_CONSTRAINT_ONE_TIME_MIGRATION.sqlinc'))){
 prepare(originalCanonical+path.posix.basename(row.file),row.bytes,row.sha256,row.sourceCommit,row.sourceFile);
}
const main=object(publication,sourceMain);assert.equal(hash(main),'1ec71ddb192128b78a02e8ea11a7d5afd2797116e2d954b7cb7a03b9b2be74bc');
const mainParsed=scanManagedSql(main.toString('utf8'));assert.equal(mainParsed.statements.length,0);assert.equal(mainParsed.meta.length,51);
for(const include of mainParsed.meta)assert.match(include.text,/^\\ir [A-Za-z0-9_]+\.sqlinc$/);
const definitionRecord=JSON.parse(object(publication,packet+'COMPLETE_FINAL_DEFINITIONS.json'));
const definitions=definitionRecord.comparison_to_actual37;
assert.equal(definitions.length,definitionRecord.final_distinct_semantic_declarations);
assert.equal(definitions.length,38); // Original37 plus the exact retained lineage owner.
const signaturePattern=/^(?:private|public)\.[a-z0-9_]+\([a-z0-9_ ,\[\]]*\)$/;
const exactAcl=signatures=>signatures.map(signature=>{assert.match(signature,signaturePattern);
 return `alter function ${signature} owner to postgres;\nrevoke all on function ${signature} from public,anon,authenticated,service_role;\n`
  +(signature.startsWith('public.')?`grant execute on function ${signature} to service_role;\n`:'');
}).join('');
const nativeAcl=exactAcl(definitions.map(row=>row.semantic_signature));
const migrationFile='supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql';
const constraintFile='supabase/support/healthroster_original_prerequisites_install1_forward/CAP_CONSTRAINT_ONE_TIME_MIGRATION.sqlinc';
const constraints=object(publication,constraintFile);
const migration=Buffer.from('-- One-time reviewed capability CHECK extension; source only until protected release.\nbegin;\n'+constraints.toString('utf8')+'\ncommit;\n');
prepare(migrationFile,migration,hash(migration),publication,constraintFile);
const repeatableFile='supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites_v1.sql';
const repeatable=Buffer.from('-- Complete reviewed original qualification prerequisites.\n-- Final current-native0420, Money0430, Report and canonical registry/seal gates remain required.\nbegin;\n\\ir 14102026_0410_healthroster_protected_source_prerequisites/ORIGINAL_PREREQUISITES_SOURCE_FORWARD.sqlinc\n'+nativeAcl+"notify pgrst, 'reload schema';\ncommit;\n");
prepare(repeatableFile,repeatable,hash(repeatable),publication,sourceMain);
// Mount the already source-accepted complete Money interfaces after root0420.
const moneyPrefix='docs/healthroster-build/proposals/strict-qualification-native-interfaces/';
const order=JSON.parse(object(money,moneyPrefix+'INTEGRATION_ORDER.json')).sql_complete_successors_in_final_order;
assert.equal(order.length,31);
const moneyReviewPrefix='docs/healthroster-build/proposals/strict-qualification-install1-forward/';
const interfaceProof=JSON.parse(object(publication,moneyReviewPrefix+'MONEY_INTERFACE_INSTALL1_BINDINGS.json'));
assert.equal(interfaceProof.complete_interfaces,47);assert.equal(interfaceProof.installed_existing,0);
const moneyLeaves=[];
for(const [i,input]of order.entries()){
 const bytes=object(money,moneyPrefix+input);
 const file='supabase/support/healthroster_money_native_interfaces/'+String(i+1).padStart(2,'0')+'_'+input;
 prepare(file,bytes,hash(bytes),money,moneyPrefix+input);
 const canonical='supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/'+path.posix.basename(file);
 prepare(canonical,bytes,hash(bytes),money,moneyPrefix+input);moneyLeaves.push({file:canonical,sha256:hash(bytes)});
}
const moneyRootFile='supabase/repeatable/14102026_0430_healthroster_money_native_interfaces_v1.sql';
const moneySource=Buffer.from('-- Complete corrected Money interfaces, preserving current Bank/WP owners.\n-- No qualification or activation follows from source presence.\nbegin;\n'+moneyLeaves.map(row=>'\\ir 14102026_0430_healthroster_money_native_interfaces/'+path.posix.basename(row.file)).join('\n')+'\n'
 +exactAcl(interfaceProof.interfaceProof.map(row=>row.signature))+"notify pgrst, 'reload schema';\ncommit;\n");
prepare(moneyRootFile,moneySource,hash(moneySource),money,moneyPrefix+'INTEGRATION_ORDER.json');
// Prove the existing root0420 C1 leaf has not been displaced in the saved tree.
const finalC1='supabase/repeatable/14102026_0420_healthroster_current_native_composition/weekly_exceptional_pay_stage_c1_request_v1.complete.sqlinc';
assert.equal(hash(fs.readFileSync(path.join(root,finalC1))),'532146598b453a2e0e253ccf0df2c29fa66977f63e644e8003d71e77e594c6f5');
for(const row of prepared){fs.mkdirSync(path.dirname(path.join(root,row.file)),{recursive:true});fs.writeFileSync(path.join(root,row.file),row.bytes);}
const receipt={sourceOnly:true,status:'MOUNTED_SOURCE_CANONICAL_NATIVE_AND_RELEASE_GATES_HELD',rootInputCommit:'a63455e878391cb3ee9732165e58c0db517f48a7',prerequisitePublication:publication,moneyPublication:money,prerequisiteManifestSha256:hash(manifestBytes),sourceChecksSha256:hash(checks),originalComponents:51,originalFinalDefinitions:definitions.length,moneyComponents:31,moneyInterfaces:47,allFormerlyAbsent83AlreadyPresentAtInput:true,oneTimeConstraintsOnlyInMigration:true,
 immediatePrivateAndPublicAcl:true,finalCanonicalRegistryAndCatalogueUnsealed:true,sequence:[migrationFile,repeatableFile,'supabase/repeatable/14102026_0420_healthroster_current_native_composition_v1.sql',moneyRootFile],files:prepared.map(({bytes,...row})=>row),noRuntimeSidecarOrOldManagedCallerMounted:true,noDatabaseOrRuntimeAction:true};
fs.writeFileSync(path.join(folder,'QUALIFICATION_FOUNDATIONS_COMPOSITION_14102026.json'),JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({status:receipt.status,sourceFiles:prepared.length,originalComponents:51,moneyComponents:31,immediateAcl:true,sequence:receipt.sequence}));
