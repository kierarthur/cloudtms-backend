// Independent read-only review of the frozen Source proposal. Writes this review only.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {spawnSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const sourceRepo=path.resolve(repo,'../healthroster-v2-money-build-13102026'),root=path.resolve(repo,'../healthroster-install2-14102026');
const commit='9ac637ef3bdc50ad1b82b616283cb226a7bbb69b',rootCommit='efd18231c06df14ed91c7f6b137ddb6fec7e2df2';
const base='docs/healthroster-build/proposals/v6-source-managed-assembly-20261016/';
const sha=value=>createHash('sha256').update(value).digest('hex'),lf=value=>value.toString('utf8').replaceAll('\r\n','\n');
function git(args){const r=spawnSync('git',['show',...args],{cwd:repo,maxBuffer:20_000_000,windowsHide:true});assert.equal(r.status,0,r.stderr.toString());return r.stdout;}
const blob=(ref,file)=>git([`${ref}:${file}`]),read=file=>blob(commit,base+file),json=file=>JSON.parse(read(file));
const {scanManagedSql,readManagedClosure,exactAtomicBody}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const m=json('ASSEMBLY_MANIFEST.json'),audit=JSON.parse(blob(m.selectionAudit.commit,m.selectionAudit.path));
assert.equal(sha(blob(m.selectionAudit.commit,m.selectionAudit.path)),m.selectionAudit.sha256);
const sourceSeal=json('SOURCE_MANIFEST.json');for(const row of sourceSeal.files){const bytes=read(row.path);
 assert.equal(sha(bytes),row.sha256);assert.equal(bytes.length,row.bytes);
 assert.equal(sha(fs.readFileSync(path.join(sourceRepo,base,row.path))),sha(bytes),'SAVED_PROPOSAL_DRIFT '+row.path);
}
function defs(text,file){return scanManagedSql(text).statements.flatMap(row=>{const sql=text.slice(row.start,row.end).trim();
 return routineDefinitions(sql,file).map(signature=>({signature,sql,statementSha256:sha(sql),file,start:row.start}));});}
const actualDefinitions=[],actualStatements=[],output=[];
for(const row of m.output){const bytes=read('managed/'+row.path),text=lf(bytes);assert.equal(sha(bytes),row.sha256);assert.equal(bytes.length,row.bytes);
 output.push({path:row.path,sha256:sha(bytes),bytes:bytes.length});
 if(row.path.endsWith('.sqlinc'))for(const statement of scanManagedSql(text).statements){const sql=text.slice(statement.start,statement.end).trim();
  actualStatements.push({path:row.path,sql,sha256:sha(sql)});actualDefinitions.push(...defs(sql,row.path));
 }
}
const originalDefinitions=[],removed=[],payloadProofs=[];
for(const selection of audit.ordered_selections.filter(row=>row.package==='source'||row.package==='source_pending')){
 const original=blob(selection.commit,selection.path);assert.equal(sha(original),selection.whole_file_sha256);
 const emitted=m.payloads.find(row=>row.commit===selection.commit&&row.path===selection.path);assert(emitted);
 if(selection.selection==='EXCLUDE_BANKING_OWNED_WHOLE_FILE'){assert.equal(emitted.emitted,false);payloadProofs.push({path:selection.path,selection:selection.selection,wholeSha256:sha(original),emitted:false});continue;}
 let selected=original;if(selection.selection==='EXACT_HEALTH_PREFIX_ONLY_BANK_BODY_EXCLUDED'){
  assert.equal(selection.byte_end_exclusive,14177);selected=original.subarray(selection.byte_start,selection.byte_end_exclusive);
  assert.equal(sha(selected),selection.selected_sha256);assert.equal(sha(original.subarray(selection.byte_end_exclusive)),selection.excluded_suffix_sha256);
 }
 const text=lf(selected),parsed=scanManagedSql(text);assert.equal(parsed.meta.length,0);assert(parsed.statements.every(row=>!row.unterminated));
 originalDefinitions.push(...defs(text,selection.path).map(row=>({...row,stage:selection.package==='source'?'BASE':'PENDING_CURRENT'})));
 let retained=text;const suppress=[];
 for(const row of parsed.statements){const sql=text.slice(row.start,row.end).trim();
  const isSetup=selection.path.includes('/forward/')&&(/^create\s+(constraint\s+)?trigger\b/i.test(sql)||/^do\s+\$guards\$/i.test(sql));
  if(isSetup){suppress.push(row);removed.push({file:selection.path,sql,sha256:sha(sql)});continue;}
  if(!routineDefinitions(sql,selection.path).length){
   const outputText=lf(read('managed/'+emitted.managedPath));assert(outputText.includes(sql),'NONFUNCTION_EFFECT_LOST '+selection.path);
  }
 }
 for(const row of suppress.sort((a,b)=>b.start-a.start))retained=retained.slice(0,row.start)+retained.slice(row.end);
 const outputText=lf(read('managed/'+emitted.managedPath));assert(outputText.includes(retained),'COMPLETE_PAYLOAD_LOST '+selection.path);
 payloadProofs.push({path:selection.path,sourceCommit:selection.commit,selection:selection.selection,selectedSha256:sha(text),retainedSha256:sha(retained),suppressedSetupStatements:suppress.length,allRetainedBytesContiguous:true});
}
const duty=m.payloads.find(row=>row.package==='invoice_duty'),dutyText=lf(blob(duty.commit,duty.path));
assert.equal(lf(read('managed/'+duty.managedPath)),dutyText);originalDefinitions.push(...defs(dutyText,duty.path).map(row=>({...row,stage:'INVOICE_DUTY'})));
assert.equal(removed.length,5);assert.equal(originalDefinitions.length,83);assert.equal(actualDefinitions.length,83);
for(const [index,original]of originalDefinitions.entries()){
 assert.equal(actualDefinitions[index].signature,original.signature);assert.equal(actualDefinitions[index].sql,original.sql,'COMPLETE_FUNCTION_REVERSE '+original.signature);
}
const final=new Map(actualDefinitions.map(row=>[row.signature,row]));assert.equal(final.size,81);
for(const row of actualDefinitions)assert(!/^(?:private|public)\.bpay_/.test(row.signature));
const superseded=[...new Set(actualDefinitions.map(row=>row.signature))].filter(signature=>actualDefinitions.filter(row=>row.signature===signature).length>1);
const triggerText=lf(read('effects/01_health_trigger_exact_replay.sqlinc'));
assert.equal(triggerText,lf(read('managed/supabase/repeatable/16102026_1100_nhsp_v6_source_pay_authority/02_exact_health_trigger_effects.sqlinc')));
assert(!/\b(drop\s+trigger|create\s+or\s+replace\s+trigger|enable\s+trigger)\b/i.test(triggerText));
assert.equal((triggerText.match(/^lock table .* in share row exclusive mode;/gm)??[]).length,15);
const schemaGuard=lf(read('guards/06_incident_resolution_predecessor.sqlinc'));
const currentContract=JSON.parse(blob(rootCommit,'supabase/release/current-contract.json'));
const incident=currentContract.relations.find(row=>row.schema==='public'&&row.name==='weekly_discrepancy_incidents');
const check=incident.constraints.find(row=>row.name==='weekly_discrepancy_incidents_resolution_kind_check');assert(schemaGuard.includes(check.definition));
const incidentSchema=m.payloads.find(row=>row.path.endsWith('/one-time/06_native_source_contact_resolution.sqlinc'));
const nativeIncident=lf(blob(incidentSchema.commit,incidentSchema.path)),migration=lf(read('managed/'+incidentSchema.managedPath));
assert(migration.indexOf(schemaGuard)<migration.indexOf(nativeIncident));assert(migration.includes(nativeIncident));
const originalCheckDdl=lf(blob(m.guards.incident.nativeMigrationCommit,m.guards.incident.nativeMigrationPath));
const oldValues=[...originalCheckDdl.match(/resolution_kind text check \(resolution_kind is null or resolution_kind in \(([\s\S]*?)\)\)/)[1].matchAll(/'([^']+)'/g)].map(row=>row[1]);
const newValues=[...nativeIncident.slice(nativeIncident.indexOf('check(resolution_kind')).matchAll(/'([^']+)'/g)].map(row=>row[1]);assert.deepEqual(newValues,[...oldValues,'SOURCE_PAY_DECISION']);
const acl=new Map();
const signatureFromAcl=text=>routineDefinitions('create function '+text+' returns void language sql as $$select null$$;','ACL')[0];
for(const item of actualStatements){let match;
 if((match=item.sql.match(/^revoke\s+(?:all|execute)\s+on\s+function\s+([\s\S]+?)\s+from\s+([\s\S]+?);$/i))){
  const signature=signatureFromAcl(match[1]);if(!acl.has(signature))acl.set(signature,{revokes:[],grants:[],owners:[]});acl.get(signature).revokes.push(match[2].toLowerCase().replaceAll(' ',''));
 }else if((match=item.sql.match(/^grant\s+execute\s+on\s+function\s+([\s\S]+?)\s+to\s+([\s\S]+?);$/i))){
  const signature=signatureFromAcl(match[1]);if(!acl.has(signature))acl.set(signature,{revokes:[],grants:[],owners:[]});acl.get(signature).grants.push(match[2].toLowerCase().replaceAll(' ',''));
 }else if((match=item.sql.match(/^alter\s+function\s+([\s\S]+?)\s+owner\s+to\s+([\s\S]+?);$/i))){
  const signature=signatureFromAcl(match[1]);if(!acl.has(signature))acl.set(signature,{revokes:[],grants:[],owners:[]});acl.get(signature).owners.push(match[2]);
 }
}
const security=[...final].map(([signature,row])=>{const tag=row.sql.match(/\bas\s+(\$[A-Za-z_]*\$|\$\$)/i);assert(tag,signature);const header=row.sql.slice(0,tag.index);
 assert(/set\s+search_path\s*(?:=|to)/i.test(header),signature+' search_path');return {signature,header,securityDefiner:/\bsecurity\s+definer\b/i.test(header),language:/\blanguage\s+sql\b/i.test(header)?'SQL':'PLPGSQL',acl:acl.get(signature)??null};});
const firstDefinitionByName=new Map();actualDefinitions.forEach((row,index)=>{const name=row.signature.split('(')[0];if(!firstDefinitionByName.has(name))firstDefinitionByName.set(name,index);});
const sqlFunctions=[];
for(const [index,row]of actualDefinitions.entries()){
 const header=row.sql.slice(0,row.sql.search(/\bas\s+\$/i));if(!/\blanguage\s+sql\b/i.test(header))continue;
 const calls=[...new Set([...row.sql.matchAll(/\b(private|public)\.([a-z_][a-z0-9_]*)\s*\(/gi)].map(match=>match[1].toLowerCase()+'.'+match[2].toLowerCase()))].filter(name=>name!==row.signature.split('(')[0]);
 const proposalCalls=calls.filter(name=>firstDefinitionByName.has(name));for(const name of proposalCalls)assert(firstDefinitionByName.get(name)<index,'ACTUAL_SQL_CREATE_ORDER '+row.signature+' -> '+name);
 sqlFunctions.push({signature:row.signature,sourceFile:originalDefinitions[index].file,sql:row.sql,proposalCalls,externalCalls:calls.filter(name=>!firstDefinitionByName.has(name)),sourceOrder:index});
}
const sourceSqlMetadata=m.releaseHold.sqlLanguageCreationDependencies.filter(row=>row.package==='source');
const nonSourceSqlMetadata=m.releaseHold.sqlLanguageCreationDependencies.filter(row=>row.package!=='source');
const rootInvoicePath='supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc';
const rootInvoice=lf(blob(rootCommit,rootInvoicePath));
const nativeDuty=defs(rootInvoice,rootInvoicePath).find(row=>row.signature==='private.weekly_source_invoice_duty_v1(uuid)');
const successorDuty=final.get('private.weekly_source_invoice_duty_v1(uuid)');
assert.equal(successorDuty.sql.split('private.weekly_source_invoice_duty_scope_v1').length,2);
assert.equal(successorDuty.sql.replace('private.weekly_source_invoice_duty_scope_v1','private.weekly_source_pay_query_scope_v1'),nativeDuty.sql);
const dutyContract=currentContract.routines.find(row=>row.schema==='private'&&row.identity.startsWith('weekly_source_invoice_duty_v1('));
assert.equal(dutyContract.owner,'postgres');assert.deepEqual(dutyContract.acl,[{grantee:'postgres',grantable:false,privilege:'EXECUTE'}]);
const dutyAclInventoryPath='supabase/repeatable/13102026_1436_weekly_source_invoice_money_acl_inventory_v1.sql';
assert(lf(blob(rootCommit,dutyAclInventoryPath)).includes("('private.weekly_source_invoice_duty_v1(uuid)')"));
const dutyProof={rootCommit,rootInvoicePath,currentStatementSha256:nativeDuty.statementSha256,successorStatementSha256:successorDuty.statementSha256,
 completeOneCallReverseEqualsCurrentInvoice:true,inheritedCurrentPrivateContract:dutyContract,dutyAclInventoryPath,
 qualification:'CREATE OR REPLACE retains existing ACL/owner; full0955 precedes1120. Effective final role verification remains required.'};
const report={version:'SOURCE_MANAGED_INDEPENDENT_INTERIM_REVIEW_V1',qualification:'SOURCE_ONLY_NOT_NATIVE_NOT_FINAL_PART_B',proposalCommit:commit,rootCheckpoint:rootCommit,
 sourceSealSha256:sha(read('SOURCE_MANIFEST.json')),proposalManifestSha256:sha(read('ASSEMBLY_MANIFEST.json')),
 counts:{managedFiles:output.length,completeDeclarations:actualDefinitions.length,uniqueFinalFunctions:final.size,originalSourceRecords:52,selectedHealthBaseFunctions:76,pendingFunctions:6,dutyFunctions:1,setupStatementsReplaced:removed.length,triggerDefinitions:15},
 originalCompleteFunctionsAllEqual:true,allOtherSelectedEffectsRetained:true,removedSetup:removed.map(({sql,...row})=>row),supersededSignatures:superseded,
 payloadProofs,functions:originalDefinitions.map(({sql,start,...row})=>row),security,
 triggerGuard:{path:m.guards.triggers.path,sha256:sha(triggerText),locks:15,noDropReplaceEnable:true,pg17Sources:['https://www.postgresql.org/docs/17/catalog-pg-trigger.html','https://www.postgresql.org/docs/17/catalog-pg-constraint.html','https://raw.githubusercontent.com/postgres/postgres/REL_17_STABLE/src/backend/commands/trigger.c'],native:'NOT_RUN'},
 incidentPredecessor:{currentRootConstraint:check,guardSha256:sha(schemaGuard),guardBeforeUnchangedBody:true,oldValues,newValues,native:'NOT_RUN'},
 actualSqlFunctions:sqlFunctions,sourceCreationMetadata:sourceSqlMetadata,invoiceDutyCompleteReverse:dutyProof,
 extraneousContactCreationMetadata:nonSourceSqlMetadata,
 bankingHold:{active:m.releaseHold.active,foreignKeys:m.releaseHold.creationTimeForeignKeys,rowtypes:m.releaseHold.creationTimeRowtypes,externalDependencies:m.releaseHold.externalDependenciesVerbatim,readResult:m.releaseHold.readResult},
 outputs:output,native:{database:false,lock:false,docker:false,compile:'NOT_RUN',acl:'NOT_RUN',triggerReplay:'NOT_RUN',concurrency:'NOT_RUN',api:'NOT_RUN',partB:'NOT_RUN'}};
fs.writeFileSync(path.join(here,'REVIEW_EVIDENCE.json'),JSON.stringify(report,null,2)+'\n');
const files=['REVIEW.md','audit.mjs','guard-ast-review.py','REVIEW_EVIDENCE.json','GUARD_AST_REVIEW.json'].map(file=>{
 const bytes=fs.readFileSync(path.join(here,file));assert(!bytes.includes(Buffer.from('\r\n')));return {path:file,sha256:sha(bytes),bytes:bytes.length};
});
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'SOURCE_MANAGED_INDEPENDENT_INTERIM_REVIEW_PINS_V1',
 proposalCommit:commit,rootCheckpoint:rootCommit,files,excludesSelf:true,status:'SOURCE_ONLY_NOT_FINAL_PART_B',
 verdict:'No source blocker found in exact15guards/1105guard/83complete selections; seven Contact-only metadata records require separation before Source-only dependency reconciliation.',
 native:'NOT_RUN',databaseConnections:0,rootEdits:false,producerEdits:false,bankingEdits:false},null,2)+'\n');
console.log(JSON.stringify({counts:report.counts,sqlFunctions:sqlFunctions.map(row=>({signature:row.signature,externalCalls:row.externalCalls})),security:{definer:security.filter(row=>row.securityDefiner).length,invoker:security.filter(row=>!row.securityDefiner).map(row=>row.signature),missingAcl:security.filter(row=>!row.acl).map(row=>row.signature)},extraneousContactCreationMetadata:nonSourceSqlMetadata.length,native:'NOT_RUN'}));
