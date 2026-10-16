// Source-only assembly. Reads the integrator worktree; writes only this proposal.
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
import assert from 'node:assert/strict';

const here=path.dirname(fileURLToPath(import.meta.url));
const repo=path.resolve(here,'../../../..');
const root=path.resolve(process.argv[2]??'');
assert(process.argv[2],'READ_ONLY_INTEGRATOR_ROOT_REQUIRED');
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const sha=value=>createHash('sha256').update(value).digest('hex');
const lf=value=>value.toString('utf8').replaceAll('\r\n','\n');
const git=(cwd,args)=>{
 const result=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd,maxBuffer:30_000_000,windowsHide:true});
 assert.equal(result.status,0,result.stderr.toString());return result.stdout;
};
const selectionPath='docs/healthroster-build/proposals/v6-invoice-final-mount-20261016/FINAL_MOUNT_SELECTION.json';
const selectionBytes=fs.readFileSync(path.join(repo,selectionPath));
assert.equal(sha(selectionBytes),'42fe73e938a141588fa60d629353dc2706069a7491cfb31570ea67626fbbc7a4');
const selection=JSON.parse(selectionBytes);
const selectionManifestPath=selectionPath.replace('FINAL_MOUNT_SELECTION','SOURCE_MANIFEST');
assert.equal(sha(fs.readFileSync(path.join(repo,selectionManifestPath))),'d0a6cf0fc76c9c96476c5364f07479b695389d4f71b514a27553e6c00c5df58c');
function readSelected(row){
 const bytes=git(repo,['show',`${row.sourceCommit}:${row.path}`]);
 assert.equal(sha(bytes),row.rawSha256,row.path+' Git object');
 assert.equal(sha(lf(bytes)),row.canonicalLfSha256,row.path+' LF');
 assert.equal(sha(fs.readFileSync(path.join(repo,row.path))),row.currentSavedDiskRawSha256,row.path+' saved disk');
 assert.equal(row.currentSavedDiskEqualsSelectedGitObject,true,row.path);
 return lf(bytes);
}
function statements(source,file){
 const parsed=scanManagedSql(source);assert.equal(parsed.meta.length,0,file+' must contain SQL only');
 return parsed.statements.map(row=>{
  assert.equal(row.unterminated,undefined,file+' unterminated statement');
  const sql=source.slice(row.start,row.end).trim();
  return {sql,sha256:sha(sql),signatures:routineDefinitions(sql,file),start:row.start,end:row.end};
 });
}
const schema=selection.selected.find(row=>row.path.endsWith('/001_billing_schema.sqlinc'));
const schemaSource=readSelected(schema);
const rootSchemaPath='supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql';
const rootSchema=lf(fs.readFileSync(path.join(root,rootSchemaPath)));
assert(rootSchema.includes(schemaSource.trim()),'EXACT_EXISTING_ROOT01_SCHEMA_REQUIRED');
const schemaStatements=statements(schemaSource,schema.path);
assert.equal(schemaStatements.length,16);assert.equal(schemaStatements.filter(row=>/^create table/i.test(row.sql)).length,6);

const native=selection.retainedNativeParentNonFunctionDependencies[0];
assert.equal(native.canonicalLfSha256,'e0f16651f794d8e68ebeb7ae8074302b815cacc92f062d3117838fe9226a2a70');
const nativeSource=readSelected(native),nativeStatements=statements(nativeSource,native.path);
assert.equal(nativeStatements.length,4);
assert.match(nativeStatements[0].sql,/^drop function if exists private\._invoice_generation_vat_policy_batch\(jsonb\);$/i);
assert(!/\bcascade\b/i.test(nativeStatements[0].sql));
assert.equal(nativeStatements[1].signatures[0],'private._invoice_generation_vat_policy_batch(jsonb)');
assert.match(nativeStatements[2].sql,/^revoke all on function[\s\S]+from public,anon,authenticated;$/i);
assert.match(nativeStatements[3].sql,/^grant execute on function[\s\S]+to service_role;$/i);

const order=selection.selected.filter(row=>row!==schema&&!row.path.endsWith('/099_new_private_acl.sqlinc'));
order.push(selection.selected.find(row=>row.path.endsWith('/099_new_private_acl.sqlinc')));
const rows=[],functions=[],effects=[];
const chunks=['-- HealthRoster V6 Invoice finite source assembly; proposal only.\n-- Root allocates managed migrations/repeatables and closure registration.\n-- Prerequisites: exact Root01 schema, complete Physical/final Candidate and Daily04b/c/d.\n-- Native/API/ACL/contention/financial proof: NOT_RUN.\n'];
for(const row of order){
 const source=readSelected(row),parsed=statements(source,row.path);
 const actualFunctions=parsed.flatMap(item=>item.signatures.map(signature=>({signature,statementSha256:item.sha256})));
 assert.deepEqual(actualFunctions,row.functions,row.path+' complete function identity');
 assert.deepEqual(parsed.filter(item=>!item.signatures.length).map(item=>({statementSha256:item.sha256,statement:item.sql})),row.nonFunctionEffects,row.path+' exact effects');
 let payload=source;
 if(row.path.endsWith('/033c_native_vat_policy.sqlinc')){
  assert.equal(parsed.length,1);assert.match(source,/^create function /i);
  // Preserve the exact native input/output ABI, volatility, security and GUCs.
  const header=text=>text.slice(0,text.indexOf('as $function$')).trim();
  assert.equal(header(parsed[0].sql),header(nativeStatements[1].sql),'VAT_NATIVE_ABI_MUST_MATCH');
  const originalJoin='on t.timesheet_id=s.timesheet_id and t.is_current';
  const eligibleJoin='on t.timesheet_id=s.timesheet_id and (t.is_current or private.timesheet_contract_source_original_eligible_v1(t.timesheet_id))';
  assert.equal(nativeStatements[1].sql.split(originalJoin).length,2,'EXACT_NATIVE_VAT_JOIN_REQUIRED');
  assert.equal(nativeStatements[1].sql.replace(originalJoin,eligibleJoin),parsed[0].sql,'ONLY_SELECTED_VAT_DELTA_REQUIRED');
  payload=nativeSource.slice(0,nativeStatements[1].start)+source.trimEnd()+nativeSource.slice(nativeStatements[1].end);
 }
 const payloadStatements=statements(payload,row.path);
 const firstByte=Buffer.byteLength(chunks.join(''));
 const marker=`\n-- BEGIN selected ${row.sourceCommit}:${row.path}\n-- RawSHA256 ${row.rawSha256}; CanonicalLFSHA256 ${row.canonicalLfSha256}\n`;
 const rendered=marker+payload.trimEnd()+`\n-- END selected ${row.path}\n`;
 chunks.push(rendered);
 rows.push({sourceCommit:row.sourceCommit,path:row.path,rawSha256:row.rawSha256,canonicalLfSha256:row.canonicalLfSha256,
  assemblyPayloadSha256:sha(payload.trimEnd()+'\n'),byteStart:firstByte,byteEnd:firstByte+Buffer.byteLength(rendered),
  functionCount:actualFunctions.length,effectCount:payloadStatements.filter(item=>!item.signatures.length).length,
  modification:payload===source?'NONE_COMPLETE_LF_PAYLOAD':'NATIVE_DROP_AND_ACL_AROUND_UNCHANGED_COMPLETE_CREATE'});
 for(const item of payloadStatements){
  if(item.signatures.length)functions.push(...item.signatures.map(signature=>({signature,statementSha256:item.sha256,source:row.path})));
  else effects.push({statementSha256:item.sha256,statement:item.sql,source:row.path,
   provenance:row.path.endsWith('/033c_native_vat_policy.sqlinc')?native.path:row.path});
 }
}
assert.equal(rows.length,66);assert.equal(functions.length,94);assert.equal(new Set(functions.map(row=>row.signature)).size,94);
assert.equal(effects.length,165);assert.equal(rows.at(-1).path.split('/').at(-1),'099_new_private_acl.sqlinc');
const joined=chunks.join('');assert(!joined.includes('\r'),'ASSEMBLY_MUST_BE_LF');
assert(!/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i.test(joined),'ILLEGAL_QUALIFIED_CONDITIONAL');
assert.equal(statements(joined,'JOINED_INVOICE_SOURCE.sqlinc').length,259);
const vatFunction=functions.find(row=>row.signature==='private._invoice_generation_vat_policy_batch(jsonb)');
const dependencyFileHits=[];
for(const file of git(repo,['ls-files','--','supabase/repeatable']).toString().split(/\r?\n/).filter(file=>file.endsWith('.sql'))){
 const text=lf(fs.readFileSync(path.join(repo,file)));
 if(file!==native.path&&text.includes('_invoice_generation_vat_policy_batch('))dependencyFileHits.push(file);
}
const manifest={version:'HEALTH_INVOICE_FINITE_SOURCE_ASSEMBLY_V1',qualification:'SOURCE_ONLY_NOT_NATIVE_ACCEPTANCE',
 selection:{path:selectionPath,sha256:sha(selectionBytes),manifestPath:selectionManifestPath,manifestSha256:sha(fs.readFileSync(path.join(repo,selectionManifestPath)))},
 sourceHead:git(repo,['rev-parse','HEAD']).toString().trim(),
 counts:{selectedSourceFiles:67,joinedSourcePayloads:66,completeUniqueFunctions:94,selectedNonFunctionStatements:178,
  exactExistingSchemaStatementsSuppressed:16,nativeParentStatementsAdded:3,retainedNonFunctionStatements:165,totalJoinedStatements:259},
 schemaSuppression:{sourceCommit:schema.sourceCommit,path:schema.path,rawSha256:schema.rawSha256,canonicalLfSha256:schema.canonicalLfSha256,
  reason:'Exact complete LF source occurs contiguously in canonical Root01; no schema/ACL/RLS/index recreation.',
  existingRootCommit:git(root,['rev-parse','HEAD']).toString().trim(),existingRootPath:rootSchemaPath,existingRootCanonicalLfSha256:sha(rootSchema),
  statements:schemaStatements.map(row=>({statementSha256:row.sha256,statement:row.sql}))},
 vat:{signature:vatFunction.signature,selectedCompleteCreateStatementSha256:vatFunction.statementSha256,
  nativeParentCommit:native.sourceCommit,nativeParentPath:native.path,nativeParentRawSha256:native.rawSha256,nativeParentCanonicalLfSha256:native.canonicalLfSha256,
  strategy:'EXACT_NATIVE_NO_CASCADE_DROP_THEN_COMPLETE_SELECTED_CREATE_THEN_EXACT_NATIVE_REVOKE_GRANT',
  abi:'Identical named jsonb input, twelve named TABLE outputs, SQL STABLE SECURITY DEFINER and search_path; selected body retained byte-for-byte after canonical LF.',
  completeBodyDelta:'Exactly one native timesheets join predicate expands t.is_current to (t.is_current OR timesheet_contract_source_original_eligible_v1(t.timesheet_id)); every other native CREATE byte is identical after LF normalization.',
  rootRequirement:'Mount only in one transaction after source_original eligibility helper and before final generation/callers and final099 ACL. Do not separately append CREATE-only033c.',
  dependencyEvidence:{qualification:'LITERAL_SOURCE_FILE_HITS_ONLY_NOT_POSTGRES_CATALOGUE',files:dependencyFileHits,
   nativeDependencyDropProof:'NOT_RUN; exact no-CASCADE DROP must pass clean PostgreSQL17 and rollback rehearsal. No dependency deletion or CASCADE authorised.'}},
 ordering:{preserved:'Selected file order except exact existing schema suppression and final099 moved after all selected helpers/native Expense owners.',finalAclPath:rows.at(-1).path},
 physicalPrerequisites:selection.mandatoryPhysicalDependencies,pendingPrerequisites:selection.additionalPendingDependencies,
 integrationBoundary:{rootAllocatesManagedAuthority:true,physicalFinalWorkflowOwner:selection.physicalOwner,
  dailyContractFamilyDependencies:'Root-owned complete Daily04b/04c/04d must precede their SQL callers; f017001 alone does not supply Daily Contract family.',
  sourceDutySeam:'Selected021a currently calls current-pay query scope; root must mount the exact coordinated immutable original-A Invoice-duty successor before current-pay refusal is activated.',
  invoiceEditContext:selection.noDefinitionOfInvoiceEditContext,bankingNativePublisher:'HELD; this assembly creates no Bank ABI or native financial producer.'},
 payloads:rows,functions,nonFunctionEffects:effects,
 output:{path:'JOINED_INVOICE_SOURCE.sqlinc',sha256:sha(joined),bytes:Buffer.byteLength(joined)},
 native:{postgres17:'NOT_RUN',rollback:'NOT_RUN',api:'NOT_RUN',acl:'NOT_RUN',concurrency:'NOT_RUN',financial:'NOT_RUN'},
 sourcePreflight:{dbCheck:'BASELINE_FAIL_UNRECORDED_MIGRATION',detail:'supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql'}};
fs.writeFileSync(path.join(here,'JOINED_INVOICE_SOURCE.sqlinc'),joined);
fs.writeFileSync(path.join(here,'ASSEMBLY_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
const artifactFiles=['ASSEMBLY_MANIFEST.json','JOINED_INVOICE_SOURCE.sqlinc','build-assembly.mjs','verify-assembly.test.mjs','README.md'];
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'HEALTH_SOURCE_QUALIFICATION_MANIFEST_V1',
 qualification:'SOURCE_ONLY_NOT_NATIVE_ACCEPTANCE',native:'NOT_RUN',
 files:Object.fromEntries(artifactFiles.map(file=>[file,sha(fs.readFileSync(path.join(here,file)))]))},null,2)+'\n');
console.log(JSON.stringify({functions:functions.length,effects:effects.length,statements:259,bytes:Buffer.byteLength(joined),sha256:sha(joined)}));
